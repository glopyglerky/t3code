"""Reuse completed fork qualification, never an unqualified push predecessor.

Missing history, expired artifacts, and network errors select no baseline. The
caller must then run full fork qualification. Artifact JSON is data, never code.
"""

from __future__ import annotations

import io
import json
from pathlib import Path
import re
import subprocess
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener
import zipfile
import zlib


SHA = re.compile(r"[0-9a-f]{40}\Z")
DIGEST = re.compile(r"[0-9a-f]{64}\Z")
REPOSITORY = re.compile(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+\Z")
WORKFLOW = ".github/workflows/hybrid.yml"
WORKFLOW_NAME = "Rust port qualification"
GATES = ("native", "adapters", "application")
MAX_BYTES = 1024 * 1024
MAX_CANDIDATES = 12
TIME_BUDGET = 30


class HistoryError(Exception):
    pass


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def _bounded_read(response) -> bytes:
    raw = response.read(MAX_BYTES + 1)
    if len(raw) > MAX_BYTES:
        raise HistoryError("Qualification response exceeds its bound.")
    return raw


def _download_origin(url: str) -> bool:
    parsed = urlsplit(url)
    host = parsed.hostname or ""
    return (
        parsed.scheme == "https" and parsed.port in (None, 443)
        and parsed.username is None and parsed.password is None and not parsed.fragment
        and (host == "github.com" or host.endswith((".github.com", ".githubusercontent.com",
                                                  ".blob.core.windows.net")))
    )


class GitHub:
    def __init__(self, repository: str, token: str | None):
        if not REPOSITORY.fullmatch(repository):
            raise HistoryError("Invalid qualification repository.")
        self.base = f"https://api.github.com/repos/{repository}"
        self.token = token
        self.opener = build_opener(NoRedirect)
        self.deadline = time.monotonic() + TIME_BUDGET

    def _request(self, url: str, authenticated: bool = True) -> bytes:
        headers = {"User-Agent": "t3code-ls-ci-history"}
        if authenticated:
            if not url.startswith(self.base + "/"):
                raise HistoryError("Credentialed request left its repository.")
            headers.update({"Accept": "application/vnd.github+json",
                            "X-GitHub-Api-Version": "2022-11-28"})
            if self.token:
                headers["Authorization"] = f"Bearer {self.token}"
        remaining = self.deadline - time.monotonic()
        if remaining <= 0:
            raise HistoryError("Qualification lookup exceeded its time budget.")
        with self.opener.open(Request(url, headers=headers), timeout=min(10, remaining)) as response:
            return _bounded_read(response)

    def json(self, path: str):
        return json.loads(self._request(self.base + path))

    def artifact(self, identifier: int) -> bytes:
        url = f"{self.base}/actions/artifacts/{identifier}/zip"
        authenticated = True
        for _ in range(4):
            try:
                return self._request(url, authenticated)
            except HTTPError as error:
                location = error.headers.get("Location")
                if error.code not in (301, 302, 303, 307, 308) or not location:
                    raise
                if not _download_origin(location):
                    raise HistoryError("Unexpected qualification download origin.") from error
                # Never carry the GitHub token into a signed storage URL.
                url, authenticated = location, False
        raise HistoryError("Qualification download redirected too many times.")


def read_receipt(archive: bytes) -> dict:
    if len(archive) > MAX_BYTES:
        raise HistoryError("Qualification archive exceeds its bound.")
    with zipfile.ZipFile(io.BytesIO(archive)) as bundle:
        files = bundle.infolist()
        if len(files) != 1 or files[0].filename != "ci-qualification.json":
            raise HistoryError("Qualification archive must contain exactly its receipt.")
        info = files[0]
        if info.file_size > MAX_BYTES or info.flag_bits & 1 or info.is_dir():
            raise HistoryError("Unsupported qualification receipt archive.")
        with bundle.open(info) as source:
            raw = _bounded_read(source)
    value = json.loads(raw)
    if not isinstance(value, dict):
        raise HistoryError("Qualification receipt is not an object.")
    return value


def valid_receipt(receipt: object, commit: str, tree: str, policy_digest: str) -> bool:
    if not isinstance(receipt, dict):
        return False
    if (type(receipt.get("schema_version")) is not int or receipt["schema_version"] != 1
            or receipt.get("status") != "passed"
            or receipt.get("source_commit") != commit or receipt.get("source_tree") != tree
            or receipt.get("policy_digest") != policy_digest):
        return False
    plan = receipt.get("plan")
    results = receipt.get("gate_results")
    if not isinstance(plan, dict) or plan.get("head") != commit or not isinstance(results, dict):
        return False
    gates = plan.get("gates")
    if not isinstance(gates, dict) or set(gates) != set(GATES) or set(results) != set(GATES):
        return False
    platform_result = receipt.get("database_platform_result")
    if platform_result not in (("success",) if gates["native"] else ("success", "skipped")):
        return False
    return all(type(gates[gate]) is bool and
               results[gate] in (("success",) if gates[gate] else ("success", "skipped"))
               for gate in GATES)


def eligible_run(run: object, repository: str, current_run_id: str) -> bool:
    if not isinstance(run, dict):
        return False
    identifier = run.get("id")
    path = run.get("path")
    if not isinstance(path, str):
        return False
    workflow, separator, reference = path.partition("@")
    return (
        type(identifier) is int and identifier > 0 and str(identifier) != str(current_run_id)
        and workflow == WORKFLOW and (not separator or bool(reference))
        and run.get("name") == WORKFLOW_NAME
        and run.get("event") in ("push", "workflow_dispatch")
        and run.get("status") == "completed" and run.get("conclusion") == "success"
        and isinstance(run.get("head_sha"), str) and bool(SHA.fullmatch(run["head_sha"]))
        and isinstance(run.get("head_repository"), dict)
        and run["head_repository"].get("full_name") == repository
    )


def _git(root: Path, *args: str, timeout: float = 10) -> str:
    return subprocess.run(["git", "--no-replace-objects", "-C", str(root), *args],
                          check=True, capture_output=True, text=True, timeout=timeout).stdout.strip()


def find_qualification(root: Path, repository: str, head: str, policy_digest: str,
                       current_run_id: str, token: str | None) -> dict | None:
    """Return a verified identical commit or successful ancestor, else None.

    Inspect at most one bounded page and twelve eligible artifacts. Reused
    receipts only suppress another run of this exact source and CI policy. A
    verified ancestor, rather than event.before, anchors incremental selection.
    """
    if not SHA.fullmatch(head) or not DIGEST.fullmatch(policy_digest):
        return None
    deadline = time.monotonic() + TIME_BUDGET
    try:
        client = GitHub(repository, token)
        response = client.json("/actions/workflows/hybrid.yml/runs?status=success&per_page=50")
        runs = response.get("workflow_runs") if isinstance(response, dict) else None
        if not isinstance(runs, list):
            return None
        eligible = [run for run in runs if eligible_run(run, repository, current_run_id)]
        # An exact-head receipt wins even if an ancestor completed more recently.
        eligible.sort(key=lambda run: run["head_sha"] != head)
        for run in eligible[:MAX_CANDIDATES]:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return None
            commit = run["head_sha"]
            try:
                _git(root, "merge-base", "--is-ancestor", commit, head, timeout=min(10, remaining))
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return None
                tree = _git(root, "rev-parse", f"{commit}^{{tree}}", timeout=min(10, remaining))
                if not SHA.fullmatch(tree):
                    continue
                response = client.json(f"/actions/runs/{run['id']}/artifacts?name=ci-qualification&per_page=10")
                artifacts = response.get("artifacts") if isinstance(response, dict) else None
                if not isinstance(artifacts, list):
                    continue
                matching = [artifact for artifact in artifacts if isinstance(artifact, dict)
                            and artifact.get("name") == "ci-qualification"
                            and artifact.get("expired") is False
                            and type(artifact.get("id")) is int and artifact["id"] > 0
                            and type(artifact.get("size_in_bytes")) is int
                            and 0 < artifact["size_in_bytes"] <= MAX_BYTES]
                if len(matching) != 1:
                    continue
                receipt = read_receipt(client.artifact(matching[0]["id"]))
                if valid_receipt(receipt, commit, tree, policy_digest):
                    return {"commit": commit, "run_id": run["id"], "reused": commit == head,
                            "receipt": receipt}
            except HTTPError as error:
                if error.code != 404:
                    return None
            except (URLError, TimeoutError):
                return None
            except (HistoryError, OSError, ValueError,
                    subprocess.SubprocessError, zipfile.BadZipFile, zlib.error, RuntimeError):
                # An unverifiable run cannot become an incremental baseline.
                continue
    except (HistoryError, HTTPError, URLError, TimeoutError, OSError, ValueError):
        return None
    return None
