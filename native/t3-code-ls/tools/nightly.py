#!/usr/bin/env python3
"""Pin published T3 nightlies and report source changes without modifying the checkout."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
from typing import Callable
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener

REPOSITORY = "pingdotgg/t3code"
REPOSITORY_ID = 1153130349
API = f"https://api.github.com/repos/{REPOSITORY}"
REMOTE = f"https://github.com/{REPOSITORY}.git"
SHA = re.compile(r"[0-9a-f]{40}\Z")
NIGHTLY = re.compile(r"(?:nightly-)?v[0-9]+\.[0-9]+\.[0-9]+-nightly\.[0-9]{8}\.[0-9]+\Z")
SCOPES = {
    "server": ("apps/server/",),
    "contracts": ("packages/contracts/",),
    "client_runtime": ("packages/client-runtime/",),
    "web": ("apps/web/",),
    "desktop": ("apps/desktop/",),
    "mobile": ("apps/mobile/",),
    "connect": (
        "infra/relay/", "apps/server/src/cloud/", "apps/server/src/relay/",
        "apps/server/src/cli/connect", "packages/client-runtime/src/relay/",
        "packages/client-runtime/src/connection/", "packages/shared/src/relay",
        "packages/shared/src/connectAuth", "packages/contracts/src/relay",
    ),
    "providers": ("packages/provider-", "packages/effect-acp/", "packages/effect-codex-app-server/"),
    "shared": ("packages/shared/",),
}


class TrackingError(Exception):
    pass


def require_sha(value: object) -> str:
    if not isinstance(value, str) or not SHA.fullmatch(value):
        raise TrackingError("Expected a complete, lowercase Git commit/object SHA.")
    return value


def published_time(value: object) -> datetime:
    if not isinstance(value, str):
        raise TrackingError("A published nightly has no valid publication timestamp.")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise TrackingError("A published nightly has an invalid publication timestamp.") from error
    if parsed.tzinfo is None:
        raise TrackingError("Publication timestamps must include their timezone.")
    return parsed.astimezone(timezone.utc)


def release_metadata(release: dict) -> dict:
    tag = release.get("tag_name")
    if not isinstance(tag, str) or not NIGHTLY.fullmatch(tag):
        raise TrackingError("Unexpected nightly tag format; update the tracker deliberately.")
    identifier = release.get("id")
    if type(identifier) is not int or identifier < 1:
        raise TrackingError("A published nightly has no valid release ID.")
    timestamp = published_time(release.get("published_at"))
    return {
        "repository": REPOSITORY,
        "channel": "nightly",
        "release_id": identifier,
        "tag": tag,
        "published_at": timestamp.isoformat().replace("+00:00", "Z"),
        "release_url": f"https://github.com/{REPOSITORY}/releases/tag/{tag}",
    }


def latest_nightly(releases: list) -> dict:
    candidates = []
    seen = {}
    identifiers = {}
    for release in releases:
        if not isinstance(release, dict):
            raise TrackingError("GitHub returned a malformed release entry.")
        tag = release.get("tag_name")
        if not isinstance(tag, str):
            raise TrackingError("GitHub returned a release without its tag.")
        if "nightly" not in tag.lower():
            continue
        if type(release.get("draft")) is not bool:
            raise TrackingError("GitHub did not identify whether a nightly is a draft.")
        if release["draft"]:
            continue
        # Match upstream's publication ordering, not tag date or target_commitish.
        metadata = release_metadata(release)
        old = seen.get(metadata["tag"])
        if old is not None and old != metadata:
            raise TrackingError("A nightly tag appeared with conflicting release metadata.")
        old_identifier = identifiers.get(metadata["release_id"])
        if old_identifier is not None and old_identifier != metadata:
            raise TrackingError("A release ID appeared with conflicting nightly metadata.")
        seen[metadata["tag"]] = metadata
        identifiers[metadata["release_id"]] = metadata
        candidates.append(metadata)
    if not candidates:
        raise TrackingError("No published nightly release was found.")
    return max(candidates, key=lambda item: (published_time(item["published_at"]), item["release_id"]))


def safe_api_url(url: str) -> str:
    parsed = urlsplit(url)
    if parsed.scheme != "https" or parsed.netloc != "api.github.com" or parsed.fragment:
        raise TrackingError("Refusing a GitHub API URL outside the expected HTTPS origin.")
    if not parsed.path.startswith((f"/repos/{REPOSITORY}/", f"/repositories/{REPOSITORY_ID}/")):
        raise TrackingError("Refusing an API URL outside the upstream repository.")
    return url


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise TrackingError("GitHub redirected an API request; resolve the repository change manually.")


class GitHub:
    def __init__(self, token: str | None = None):
        self.token = token
        self.opener = build_opener(NoRedirect)

    def get(self, url: str) -> tuple[object, dict]:
        headers = {
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "t3-code-ls-nightly-tracker",
        }
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        try:
            with self.opener.open(Request(safe_api_url(url), headers=headers), timeout=30) as response:
                raw = response.read(32 * 1024 * 1024 + 1)
                if len(raw) > 32 * 1024 * 1024:
                    raise TrackingError("GitHub response exceeded the safety limit; no partial result was accepted.")
                return json.loads(raw), dict(response.headers)
        except HTTPError as error:
            raise TrackingError(f"GitHub API request failed with HTTP {error.code}.") from error
        except (URLError, TimeoutError, UnicodeError, json.JSONDecodeError) as error:
            raise TrackingError(f"GitHub API response could not be read: {type(error).__name__}.") from error


def next_page(headers: dict) -> str | None:
    link = next((value for key, value in headers.items() if key.lower() == "link"), None)
    if not link:
        return None
    matches = re.findall(r'<([^>]+)>;\s*rel="([^"]+)"', link)
    if not matches:
        raise TrackingError("Malformed GitHub pagination header.")
    following = [url for url, relation in matches if "next" in relation.split()]
    if len(following) > 1:
        raise TrackingError("Ambiguous GitHub pagination header.")
    return safe_api_url(following[0]) if following else None


def all_releases(get: Callable, max_pages: int = 1000) -> list:
    url = f"{API}/releases?per_page=100"
    visited = set()
    releases = []
    while url is not None:
        if url in visited:
            raise TrackingError("GitHub pagination repeated a page.")
        if len(visited) >= max_pages:
            raise TrackingError("Release pagination limit reached; latest nightly is unknown.")
        visited.add(url)
        page, headers = get(url)
        if not isinstance(page, list):
            raise TrackingError("GitHub returned a non-list release page.")
        releases.extend(page)
        url = next_page(headers)
    return releases


def resolve_tag(get: Callable, tag: str) -> str:
    if not NIGHTLY.fullmatch(tag):
        raise TrackingError("Invalid nightly tag.")
    ref, _ = get(f"{API}/git/ref/tags/{quote(tag, safe='')}")
    if not isinstance(ref, dict) or ref.get("ref") != f"refs/tags/{tag}":
        raise TrackingError("GitHub returned the wrong tag reference.")
    obj = ref.get("object")
    seen = set()
    for _ in range(16):
        if not isinstance(obj, dict):
            raise TrackingError("Tag reference has no object.")
        sha = require_sha(obj.get("sha"))
        if obj.get("type") == "commit":
            return sha
        if obj.get("type") != "tag" or sha in seen:
            raise TrackingError("Tag points to an unsupported object or a tag cycle.")
        seen.add(sha)
        annotated, _ = get(f"{API}/git/tags/{sha}")
        if not isinstance(annotated, dict) or annotated.get("sha") != sha:
            raise TrackingError("GitHub returned the wrong annotated tag object.")
        obj = annotated.get("object")
    raise TrackingError("Annotated tag nesting exceeded the supported depth.")


def git(root: Path, *args: str) -> bytes:
    try:
        return subprocess.run(
            ["git", "-C", str(root), *args], check=True,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        ).stdout
    except (OSError, subprocess.CalledProcessError) as error:
        # Git stderr can contain remote credentials or local paths. Keep errors bounded.
        raise TrackingError(f"git {args[0]} failed.") from error


def ensure_commit(root: Path, commit: str, fetch: bool = False) -> None:
    require_sha(commit)
    try:
        kind = git(root, "cat-file", "-t", commit).strip()
    except TrackingError:
        if not fetch:
            raise TrackingError(f"Commit {commit} is missing locally; rerun check with --fetch.")
        git(root, "-c", "core.hooksPath=/dev/null", "fetch", "--no-tags", "--no-write-fetch-head", REMOTE, commit)
        kind = git(root, "cat-file", "-t", commit).strip()
    if kind != b"commit":
        raise TrackingError("Pinned object is not a commit.")


def read_tree(root: Path, commit: str) -> dict[str, dict]:
    require_sha(commit)
    result = {}
    for record in git(root, "ls-tree", "-r", "-z", commit).split(b"\0"):
        if not record:
            continue
        try:
            metadata, raw_path = record.split(b"\t", 1)
            mode, kind, oid = metadata.decode("ascii").split(" ")
            path = raw_path.decode("utf-8")
        except (ValueError, UnicodeError) as error:
            raise TrackingError("The upstream tree contains an unsupported path or tree entry.") from error
        require_sha(oid)
        if kind not in ("blob", "commit") or mode not in ("100644", "100755", "120000", "160000"):
            raise TrackingError("The upstream tree contains an unsupported object mode.")
        if path in result or path.startswith("/") or any(p in (".", "..", "") for p in path.split("/")):
            raise TrackingError("The upstream tree contains an unsafe or duplicate path.")
        result[path] = {"mode": mode, "kind": kind, "oid": oid}
    return result


def surfaces_for(path: str) -> list[str]:
    return [name for name, prefixes in SCOPES.items() if path.startswith(prefixes)]


def inventory(tree: dict) -> dict:
    result = {}
    for surface, prefixes in SCOPES.items():
        rows = [[path, item["mode"], item["kind"], item["oid"]]
                for path, item in sorted(tree.items()) if path.startswith(prefixes)]
        if not rows:
            raise TrackingError(f"Required source surface is absent: {surface}.")
        payload = json.dumps(rows, ensure_ascii=True, separators=(",", ":")).encode("ascii")
        result[surface] = {
            "path_prefixes": list(prefixes), "entries": len(rows),
            "inventory_sha256": hashlib.sha256(payload).hexdigest(),
        }
    return result


def manifest(root: Path, metadata: dict, commit: str) -> dict:
    tree = read_tree(root, commit)
    return {
        "schema_version": 1,
        "upstream": {**metadata, "commit": require_sha(commit),
                     "root_tree": git(root, "rev-parse", f"{commit}^{{tree}}").decode().strip()},
        "qualification": {
            "status": "unqualified",
            "last_qualified_commit": None,
            "evidence": [],
            "meaning": "Source baseline only. Pinning or updating this file does not establish Lust, transport, UI, mobile, or feature parity.",
        },
        "inventory_format": "SHA-256 of ASCII compact JSON arrays [path,mode,kind,Git-object-id], sorted by path; source objects come from the pinned commit, never the working tree.",
        "source_surfaces": inventory(tree),
    }


def read_lock(path: Path) -> dict:
    try:
        value = json.loads(path.read_text())
    except (OSError, ValueError) as error:
        raise TrackingError("Could not read the upstream lock.") from error
    if not isinstance(value, dict) or value.get("schema_version") != 1:
        raise TrackingError("Unsupported upstream lock schema.")
    upstream = value.get("upstream")
    if not isinstance(upstream, dict) or upstream.get("repository") != REPOSITORY or upstream.get("channel") != "nightly":
        raise TrackingError("Upstream lock identifies a different repository or channel.")
    require_sha(upstream.get("commit"))
    require_sha(upstream.get("root_tree"))
    release_metadata({"id": upstream.get("release_id"), "tag_name": upstream.get("tag"),
                      "published_at": upstream.get("published_at")})
    if not isinstance(value.get("qualification"), dict):
        raise TrackingError("Upstream lock has no qualification state.")
    return value


def report(root: Path, lock: dict, candidate: dict) -> dict:
    base = lock["upstream"]["commit"]
    head = candidate["upstream"]["commit"]
    before = read_tree(root, base)
    after = read_tree(root, head)
    if inventory(before) != lock.get("source_surfaces"):
        raise TrackingError("Locked source inventory does not match its pinned commit.")
    actual_tree = git(root, "rev-parse", f"{base}^{{tree}}").decode().strip()
    if lock["upstream"]["root_tree"] != actual_tree:
        raise TrackingError("Locked root tree does not match its pinned commit.")
    changes = []
    for path in sorted(before.keys() | after.keys()):
        old, new = before.get(path), after.get(path)
        if old != new:
            changes.append({"path": path, "status": "added" if old is None else "removed" if new is None else "modified",
                            "surfaces": surfaces_for(path), "before": old, "after": new})
    updated = lock["upstream"] != candidate["upstream"]
    return {
        "schema_version": 1,
        "state": "update_available" if updated else "current",
        "baseline": lock["upstream"],
        "candidate": candidate,
        "qualification": lock["qualification"],
        "requires_review": updated,
        "complete_tree_comparison": True,
        "changed_file_count": len(changes),
        "changed_surface_counts": {name: sum(name in item["surfaces"] for item in changes) for name in SCOPES},
        "changes": changes,
        "next_step": "Inspect the candidate source diff and port affected behavior; run compatibility tests before separately advancing the qualified revision. This command never merges, changes branches, edits the lock, or qualifies a port.",
    }


def write_json(path: Path, value: dict) -> bool:
    encoded = json.dumps(value, indent=2, ensure_ascii=True) + "\n"
    if path.exists() and path.read_text() == encoded:
        return False
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", dir=path.parent, prefix=f".{path.name}.", delete=False) as out:
            temporary = out.name
            out.write(encoded)
            out.flush()
            os.fsync(out.fileno())
        os.replace(temporary, path)
    finally:
        if temporary is not None and os.path.exists(temporary):
            os.unlink(temporary)
    return True


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    snapshot = commands.add_parser("snapshot", help="Record an explicitly resolved nightly as an unqualified source baseline.")
    snapshot.add_argument("--repo-root", type=Path, required=True)
    snapshot.add_argument("--commit", required=True)
    snapshot.add_argument("--tag", required=True)
    snapshot.add_argument("--release-id", required=True, type=int)
    snapshot.add_argument("--published-at", required=True)
    snapshot.add_argument("--output", type=Path, required=True)
    check = commands.add_parser("check", help="Report the newest published nightly; leave worktree, branches, and lock unchanged.")
    check.add_argument("--repo-root", type=Path, required=True)
    check.add_argument("--lock", type=Path, required=True)
    check.add_argument("--output", type=Path, required=True)
    check.add_argument("--fetch", action="store_true", help="Fetch missing immutable upstream commit objects, without updating branches.")
    args = parser.parse_args(argv)
    try:
        if args.command == "snapshot":
            ensure_commit(args.repo_root, args.commit)
            metadata = release_metadata({"id": args.release_id, "tag_name": args.tag, "published_at": args.published_at})
            result = manifest(args.repo_root, metadata, args.commit)
        else:
            if args.output.resolve() == args.lock.resolve():
                raise TrackingError("A check report cannot overwrite the source lock.")
            lock = read_lock(args.lock)
            client = GitHub(os.environ.get("GITHUB_TOKEN"))
            metadata = latest_nightly(all_releases(client.get))
            if published_time(metadata["published_at"]) < published_time(lock["upstream"]["published_at"]):
                raise TrackingError("The newest published nightly predates the lock; investigate removed or changed releases.")
            commit = resolve_tag(client.get, metadata["tag"])
            if metadata["tag"] == lock["upstream"]["tag"] and commit != lock["upstream"]["commit"]:
                raise TrackingError("The pinned nightly tag moved to another commit; investigate before updating.")
            ensure_commit(args.repo_root, lock["upstream"]["commit"], args.fetch)
            ensure_commit(args.repo_root, commit, args.fetch)
            result = report(args.repo_root, lock, manifest(args.repo_root, metadata, commit))
        changed = write_json(args.output, result)
        print(json.dumps({"state": result.get("state", "source_snapshot"), "output_changed": changed,
                          "commit": result.get("candidate", result)["upstream"]["commit"],
                          "changed_file_count": result.get("changed_file_count")}))
        return 0
    except (TrackingError, OSError) as error:
        print(f"nightly tracking failed: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
