#!/usr/bin/env python3
"""Report upstream issue/PR activity without changing upstream or qualifying code.

Only GitHub GET endpoints are used. Issue events preserve close/reopen transitions
between polls. PR files are cached by head/base revision; comments and patch bodies
are never retained. The state cursor is a successful scan's start time, with an
overlap on the next scan. This is an incremental monitor, not an audit log or a
complete historical backfill. Unmerged proposals are always advisory.
"""

from __future__ import annotations

import argparse
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import sys
import tempfile
from typing import Callable
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, urlencode, urlsplit
from urllib.request import Request, build_opener

from nightly import (
    API, REPOSITORY, REPOSITORY_ID, SCOPES, NoRedirect, TrackingError,
    next_page, require_sha, safe_api_url, surfaces_for,
)

API_VERSION = "2026-03-10"
MAX_RESPONSE = 32 * 1024 * 1024
MAX_STATE = 64 * 1024 * 1024
RETENTION_DAYS = 30
NUMBER = re.compile(r"[1-9][0-9]*\Z")
SOURCE_HINTS = {
    "server": r"\b(server|backend)\b",
    "contracts": r"\b(contract|contracts|schema|schemas|rpc)\b",
    "client_runtime": r"\b(client.runtime|connection.supervisor)\b",
    "web": r"\b(web|browser|frontend)\b",
    "desktop": r"\b(desktop|electron)\b",
    "mobile": r"\b(mobile|ios|android|expo)\b",
    "connect": r"\b(t3.connect|relay|cloudflared|iroh|pairing|remote.access)\b",
    "providers": r"\b(provider|codex|claude|acp)\b",
}


def timestamp(value: object) -> datetime:
    if not isinstance(value, str):
        raise TrackingError("Missing or invalid activity timestamp.")
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise TrackingError("Invalid activity timestamp.") from error
    if result.tzinfo is None:
        raise TrackingError("Activity timestamps must include their timezone.")
    return result.astimezone(timezone.utc)


def iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def integer(value: object, field: str, minimum: int = 0) -> int:
    if type(value) is not int or value < minimum:
        raise TrackingError(f"Invalid {field} in GitHub response or saved state.")
    return value


def text(value: object, field: str) -> str:
    if not isinstance(value, str):
        raise TrackingError(f"Missing {field} in GitHub response or saved state.")
    return value


def boolean(value: object, field: str) -> bool:
    if type(value) is not bool:
        raise TrackingError(f"Invalid {field} in GitHub response or saved state.")
    return value


def object_value(value: object, field: str) -> dict:
    if not isinstance(value, dict):
        raise TrackingError(f"Invalid {field} object in GitHub response or saved state.")
    return value


class GitHub:
    def __init__(self, token: str | None = None):
        self.token = token
        self.opener = build_opener(NoRedirect)
        self.requests = 0

    def get(self, url: str) -> tuple[object, dict]:
        headers = {
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": API_VERSION,
            "User-Agent": "t3-code-ls-upstream-activity",
        }
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        self.requests += 1
        try:
            request = Request(safe_api_url(url), headers=headers, method="GET")
            with self.opener.open(request, timeout=30) as response:
                raw = response.read(MAX_RESPONSE + 1)
                if len(raw) > MAX_RESPONSE:
                    raise TrackingError("GitHub response exceeded the limit; no partial scan was accepted.")
                return json.loads(raw), dict(response.headers)
        except HTTPError as error:
            limited = error.code == 429 or error.headers.get("X-RateLimit-Remaining") == "0"
            reason = "rate limited" if limited else f"HTTP {error.code}"
            raise TrackingError(f"GitHub activity request failed ({reason}); state is unchanged.") from error
        except (URLError, TimeoutError, UnicodeError, ValueError) as error:
            raise TrackingError(f"GitHub activity response could not be read ({type(error).__name__}).") from error


def api_path(url: str) -> str:
    parsed = urlsplit(safe_api_url(url))
    for prefix in (f"/repos/{REPOSITORY}", f"/repositories/{REPOSITORY_ID}"):
        if parsed.path.startswith(prefix + "/"):
            return parsed.path[len(prefix):]
    raise TrackingError("Unexpected activity pagination path.")


def pages(get: Callable, url: str, *, key: str | None = None,
          limit: int = 10000, max_pages: int = 1000, identity: str = "id") -> list[dict]:
    """Follow every Link page, rejecting changed filters, duplicate rows and truncation."""
    path = api_path(url)
    filters = {name: values for name, values in parse_qs(urlsplit(url).query).items()
               if name != "page"}
    visited: set[str] = set()
    seen: set[object] = set()
    result: list[dict] = []
    total = None
    while url is not None:
        if url in visited:
            raise TrackingError("Activity pagination repeated a page.")
        if len(visited) >= max_pages:
            raise TrackingError("Activity pagination limit reached; state is unchanged.")
        if api_path(url) != path:
            raise TrackingError("Activity pagination changed its resource.")
        query = {name: values for name, values in parse_qs(urlsplit(url).query).items()
                 if name != "page"}
        if query != filters:
            raise TrackingError("Activity pagination changed its selection filters.")
        visited.add(url)
        payload, headers = get(url)
        if key is not None:
            payload = object_value(payload, path)
            count = integer(payload.get("total_count"), "pagination total")
            if total is not None and total != count:
                raise TrackingError("GitHub changed a paginated total during the scan; retry.")
            total = count
            rows = payload.get(key)
        else:
            rows = payload
        if not isinstance(rows, list):
            raise TrackingError("GitHub returned a non-list activity page.")
        for row in rows:
            row = object_value(row, "activity row")
            identifier = row.get(identity)
            if isinstance(identifier, bool) or not isinstance(identifier, (str, int)):
                raise TrackingError("Activity row lacks its pagination identity.")
            if identifier in seen:
                raise TrackingError("Duplicate activity row across pages; pagination may have moved. Retry.")
            seen.add(identifier)
            result.append(row)
            if len(result) > limit:
                raise TrackingError("Activity item limit reached; narrow the initial window or increase the explicit limit.")
        url = next_page(headers)
    if total is not None and len(result) != total:
        raise TrackingError("GitHub returned an incomplete paginated collection.")
    return result


def issue_query(since: datetime) -> str:
    return f"{API}/issues?" + urlencode({
        "state": "all", "sort": "updated", "direction": "asc",
        "since": iso(since), "per_page": 100,
    })


def hints(item: dict) -> list[str]:
    body = "" if item.get("body") is None else item["body"]
    if not isinstance(body, str):
        raise TrackingError("Issue body has an unsupported type.")
    labels = item.get("labels", [])
    words = " ".join([text(item.get("title"), "issue title"), body,
                      *[text(label.get("name"), "label") for label in labels]])
    matched = {name for name, pattern in SOURCE_HINTS.items() if re.search(pattern, words, re.I)}
    matched.update(name for name, prefixes in SCOPES.items() if any(prefix in words for prefix in prefixes))
    return sorted(matched)


def issue_snapshot(item: dict, *, is_pr: bool | None = None) -> dict:
    number = integer(item.get("number"), "issue number", 1)
    state = item.get("state")
    if state not in ("open", "closed"):
        raise TrackingError("Unknown GitHub issue state.")
    created, updated = timestamp(item.get("created_at")), timestamp(item.get("updated_at"))
    if updated < created:
        raise TrackingError("Issue update predates its creation.")
    labels = item.get("labels")
    if not isinstance(labels, list) or any(not isinstance(label, dict) for label in labels):
        raise TrackingError("Invalid issue labels.")
    body = "" if item.get("body") is None else item["body"]
    if not isinstance(body, str):
        raise TrackingError("Invalid issue body.")
    if is_pr is None:
        is_pr = isinstance(item.get("pull_request"), dict)
    closed = item.get("closed_at")
    return {
        "number": number, "kind": "pull_request" if is_pr else "issue",
        "url": f"https://github.com/{REPOSITORY}/{'pull' if is_pr else 'issues'}/{number}",
        "title": text(item.get("title"), "issue title"), "state": state,
        "created_at": iso(created), "updated_at": iso(updated),
        "closed_at": iso(timestamp(closed)) if closed is not None else None,
        "labels": sorted({text(label.get("name"), "label") for label in labels}),
        "comments": integer(item.get("comments"), "comment count"),
        "state_reason": item.get("state_reason"),
        "body_sha256": hashlib.sha256(body.encode("utf-8")).hexdigest(),
        "surfaces": hints(item), "surface_basis": "unverified_title_label_body_hints",
    }


def pr_metadata(item: dict, number: int) -> dict:
    if integer(item.get("number"), "PR number", 1) != number:
        raise TrackingError("GitHub returned a different pull request.")
    head, base = object_value(item.get("head"), "PR head"), object_value(item.get("base"), "PR base")
    merged = boolean(item.get("merged"), "merged status")
    merged_at = item.get("merged_at")
    if merged != (merged_at is not None):
        raise TrackingError("GitHub returned inconsistent merge metadata.")
    mergeable = item.get("mergeable")
    if mergeable is not None and type(mergeable) is not bool:
        raise TrackingError("Invalid mergeability signal.")
    return {
        "draft": boolean(item.get("draft"), "draft status"),
        "merged": merged, "merged_at": iso(timestamp(merged_at)) if merged else None,
        "head_sha": require_sha(head.get("sha")), "base_sha": require_sha(base.get("sha")),
        "base_ref": text(base.get("ref"), "base ref"),
        "mergeable": mergeable, "mergeable_state": item.get("mergeable_state"),
        "requested_reviewers": sorted(text(user.get("login"), "reviewer")
                                      for user in item.get("requested_reviewers", [])),
        "requested_teams": sorted(text(team.get("slug"), "review team")
                                  for team in item.get("requested_teams", [])),
        "advisory_only": True,
    }


def pr_files(get: Callable, number: int, pr: dict, changed_files: int, old: dict, max_pages: int) -> dict:
    if changed_files > 3000:
        raise TrackingError("PR file list exceeds GitHub's 3000-file cap; complete surface coverage is unavailable.")
    version = {key: pr[key] for key in ("head_sha", "base_sha", "base_ref")}
    cached = old.get("pull_request", {}).get("files", {})
    if cached.get("revision") == version and cached.get("count") == changed_files:
        return cached
    rows = pages(get, f"{API}/pulls/{number}/files?per_page=100", limit=3000,
                 identity="filename", max_pages=max_pages)
    if len(rows) != changed_files:
        raise TrackingError("PR files changed or were truncated during the scan; retry.")
    paths = set()
    for row in rows:
        paths.add(text(row.get("filename"), "changed filename"))
        if row.get("previous_filename") is not None:
            paths.add(text(row["previous_filename"], "previous filename"))
    return {
        "revision": version, "count": changed_files, "paths": sorted(paths),
        "surfaces": sorted({surface for path in paths for surface in surfaces_for(path)}),
    }


def reviews(get: Callable, number: int, head: str, max_pages: int) -> dict:
    rows = pages(get, f"{API}/pulls/{number}/reviews?per_page=100", max_pages=max_pages)
    latest = {}
    for row in sorted(rows, key=lambda row: integer(row.get("id"), "review ID", 1)):
        state = row.get("state")
        if state not in ("APPROVED", "CHANGES_REQUESTED", "COMMENTED", "DISMISSED", "PENDING"):
            raise TrackingError("Unknown GitHub review state.")
        # A comment-only review does not erase an existing approval or change request.
        if state in ("COMMENTED", "PENDING"):
            continue
        user = object_value(row.get("user"), "review user")
        login = text(user.get("login"), "reviewer login")
        commit = require_sha(row.get("commit_id"))
        latest[login] = {
            "reviewer": login, "state": state, "commit_sha": commit,
            "on_current_head": commit == head,
            "submitted_at": iso(timestamp(row.get("submitted_at"))),
        }
    return {
        "review_count": len(rows), "latest_decisions": [latest[key] for key in sorted(latest)],
        "required_review_policy_evaluated": False,
    }


def ci(get: Callable, head: str, max_pages: int) -> dict:
    runs = pages(get, f"{API}/commits/{head}/check-runs?filter=latest&per_page=100",
                 key="check_runs", max_pages=max_pages)
    statuses = pages(get, f"{API}/commits/{head}/status?per_page=100",
                     key="statuses", identity="context", max_pages=max_pages)
    checks = []
    for run in runs:
        if require_sha(run.get("head_sha")) != head:
            raise TrackingError("A check run belongs to another commit.")
        checks.append({
            "id": integer(run.get("id"), "check-run ID", 1),
            "name": text(run.get("name"), "check name"),
            "status": text(run.get("status"), "check status"),
            "conclusion": run.get("conclusion"),
            "url": run.get("html_url"),
        })
    contexts = [{"context": text(row.get("context"), "status context"),
                 "state": text(row.get("state"), "commit status"),
                 "updated_at": iso(timestamp(row.get("updated_at"))),
                 "url": row.get("target_url")} for row in statuses]
    return {
        "scope": "head_commit_only", "head_sha": head,
        "check_runs": sorted(checks, key=lambda row: row["id"]),
        "commit_statuses": sorted(contexts, key=lambda row: row["context"]),
        "checks_reported": len(checks), "status_contexts_reported": len(contexts),
        "required_check_policy_evaluated": False,
    }


def issue_events(get: Callable, number: int, since: datetime, through: datetime,
                 seen: dict, max_pages: int) -> list[dict]:
    rows = pages(get, f"{API}/issues/{number}/events?per_page=100", max_pages=max_pages)
    result = []
    for row in rows:
        created = timestamp(row.get("created_at"))
        identifier = str(integer(row.get("id"), "event ID", 1))
        if since <= created <= through and identifier not in seen:
            event = {"id": identifier, "event": text(row.get("event"), "event type"),
                     "created_at": iso(created)}
            if row.get("label") is not None:
                event["label"] = text(object_value(row["label"], "event label").get("name"), "label")
            if isinstance(row.get("actor"), dict):
                event["actor"] = row["actor"].get("login")
            result.append(event)
    return sorted(result, key=lambda row: (row["created_at"], int(row["id"])))


def changed_fields(old: dict, new: dict) -> list[str]:
    changed = [key for key in new if key != "pull_request" and old.get(key) != new[key]]
    if "pull_request" in new:
        previous = old.get("pull_request", {})
        changed.extend(f"pull_request.{key}" for key, value in new["pull_request"].items()
                       if previous.get(key) != value)
    return sorted(changed)


def change_record(old: dict, new: dict, events: list[dict], since: datetime) -> dict | None:
    fields = changed_fields(old, new)
    if not fields and not events:
        return None
    actions = {event["event"] for event in events}
    if not old and timestamp(new["created_at"]) >= since:
        actions.add("opened")
    if old and old["state"] != new["state"]:
        actions.add("reopened" if new["state"] == "open" else "closed")
    pr = new.get("pull_request", {})
    if not old and new["state"] == "closed" and new["closed_at"] is not None and timestamp(new["closed_at"]) >= since:
        actions.add("closed")
    if pr.get("merged") and ((not old and timestamp(pr["merged_at"]) >= since)
                             or (old and not old.get("pull_request", {}).get("merged"))):
        actions.add("merged")
    if fields and (old or not actions):
        actions.add("updated")
    return {**{key: value for key, value in new.items() if key != "body_sha256"},
            "actions": sorted(actions), "changed_fields": fields, "events": events,
            "labels_added": sorted(set(new["labels"]) - set(old.get("labels", []))),
            "labels_removed": sorted(set(old.get("labels", [])) - set(new["labels"]))}


def read_state(path: Path) -> dict | None:
    if not path.exists():
        return None
    if path.is_symlink():
        raise TrackingError("The activity state must not be a symbolic link.")
    try:
        with path.open("rb") as source:
            raw = source.read(MAX_STATE + 1)
        if len(raw) > MAX_STATE:
            raise TrackingError("Activity state exceeded the supported size.")
        value = json.loads(raw)
    except (OSError, ValueError) as error:
        raise TrackingError("Could not read the existing activity state; it was not replaced.") from error
    value = object_value(value, "saved state")
    if value.get("schema_version") != 1 or value.get("repository") != REPOSITORY:
        raise TrackingError("Activity state belongs to an unsupported schema or another repository.")
    timestamp(value.get("cursor"))
    bootstrap = object_value(value.get("bootstrap"), "saved bootstrap window")
    timestamp(bootstrap.get("from"))
    timestamp(bootstrap.get("through"))
    items = object_value(value.get("items"), "saved items")
    for number, item in items.items():
        if not NUMBER.fullmatch(number) or not isinstance(item, dict) or item.get("number") != int(number):
            raise TrackingError("Invalid item identity in saved state.")
        if item.get("kind") not in ("issue", "pull_request") or item.get("state") not in ("open", "closed"):
            raise TrackingError("Invalid item kind/state in saved state.")
        timestamp(item.get("updated_at"))
        timestamp(item.get("created_at"))
        if not isinstance(item.get("labels"), list) or any(not isinstance(label, str) for label in item["labels"]):
            raise TrackingError("Invalid labels in saved state.")
        if item["kind"] == "pull_request":
            pr = object_value(item.get("pull_request"), "saved PR metadata")
            require_sha(pr.get("head_sha"))
            require_sha(pr.get("base_sha"))
            boolean(pr.get("draft"), "saved draft status")
            boolean(pr.get("merged"), "saved merged status")
            if "files" in pr:
                files = object_value(pr["files"], "saved PR file cache")
                object_value(files.get("revision"), "saved file revision")
                integer(files.get("count"), "saved file count")
                for field in ("paths", "surfaces"):
                    if not isinstance(files.get(field), list) or any(not isinstance(entry, str) for entry in files[field]):
                        raise TrackingError("Invalid saved PR file inventory.")
    events = object_value(value.get("seen_events"), "saved event IDs")
    for identifier, created in events.items():
        if not NUMBER.fullmatch(identifier):
            raise TrackingError("Invalid event identity in saved state.")
        timestamp(created)
    return value


def scan(get: Callable, state: dict | None, *, through: datetime, lookback_hours: int | None,
         max_items: int, overlap_minutes: int = 60, details: str = "prs", max_pages: int = 1000) -> tuple[dict, dict]:
    if not 1 <= max_items <= 100000 or not 1 <= max_pages <= 10000:
        raise TrackingError("Activity count/page limits must be explicit positive supported bounds.")
    if not 1 <= overlap_minutes <= 1440:
        raise TrackingError("The activity overlap must be between 1 and 1440 minutes.")
    if details not in ("prs", "none"):
        raise TrackingError("Unknown activity detail mode.")
    through = timestamp(iso(through))
    overlap = timedelta(minutes=overlap_minutes)
    if state is None:
        if lookback_hours is None or not 1 <= lookback_hours <= 720:
            raise TrackingError("First run requires --lookback-hours between 1 and 720, plus --max-items.")
        since = through - timedelta(hours=lookback_hours)
        old_items, seen = {}, {}
    else:
        cursor = timestamp(state["cursor"])
        if cursor > through:
            raise TrackingError("Saved activity cursor is in the future; check the clock before retrying.")
        since = cursor - overlap
        old_items, seen = state["items"], state["seen_events"]
    rows = pages(get, issue_query(since), identity="number", limit=max_items, max_pages=max_pages)
    discovered = {str(integer(row.get("number"), "issue number", 1)): row for row in rows}
    tracked_open = {number for number, item in old_items.items()
                    if details == "prs" and item["kind"] == "pull_request" and item["state"] == "open"}
    numbers = set(discovered) | tracked_open
    if len(numbers) > max_items:
        raise TrackingError("Candidates plus tracked open PRs exceed --max-items; no cursor was advanced.")
    items, new_seen = dict(old_items), dict(seen)
    changes, deferred = [], []
    for number_text in sorted(numbers, key=int):
        number, old = int(number_text), old_items.get(number_text, {})
        item = discovered.get(number_text)
        is_pr = number_text in tracked_open or isinstance((item or {}).get("pull_request"), dict)
        if item is not None and timestamp(item.get("updated_at")) > through:
            deferred.append(number)
            continue
        if is_pr:
            item, _ = get(f"{API}/pulls/{number}")
            item = object_value(item, "pull request")
            if timestamp(item.get("updated_at")) > through:
                deferred.append(number)
                continue
        if item is None:
            raise TrackingError("An activity candidate has no snapshot.")
        snapshot = issue_snapshot(item, is_pr=is_pr)
        if is_pr:
            pr = pr_metadata(item, number)
            if details == "prs":
                pr["files"] = pr_files(get, number, pr, integer(item.get("changed_files"), "changed-file count"), old, max_pages)
                pr["reviews"] = reviews(get, number, pr["head_sha"], max_pages)
                pr["ci"] = ci(get, pr["head_sha"], max_pages)
                confirmed, _ = get(f"{API}/pulls/{number}")
                confirmed = object_value(confirmed, "PR after detail scan")
                current = pr_metadata(confirmed, number)
                if any(current[key] != pr[key] for key in ("head_sha", "base_sha", "base_ref")) or confirmed.get("changed_files") != item["changed_files"]:
                    raise TrackingError("PR revisions changed during detail collection; retry before recording source areas.")
                snapshot["surfaces"] = pr["files"]["surfaces"]
                snapshot["surface_basis"] = "pull_request_file_paths"
            snapshot["pull_request"] = pr
        fetch_events = number_text in discovered or old.get("updated_at") != snapshot["updated_at"]
        events = issue_events(get, number, since, through, seen, max_pages) if fetch_events else []
        for event in events:
            new_seen[event["id"]] = event["created_at"]
        record = change_record(old, snapshot, events, since)
        if record is not None:
            changes.append(record)
        items[number_text] = snapshot
    retention = through - timedelta(days=RETENTION_DAYS)
    items = {number: item for number, item in items.items()
             if item["state"] == "open" or timestamp(item["updated_at"]) >= retention}
    new_seen = {identifier: created for identifier, created in new_seen.items()
                if timestamp(created) >= through - overlap}
    changes.sort(key=lambda item: (item["updated_at"], item["number"]), reverse=True)
    report = {
        "schema_version": 1, "repository": REPOSITORY, "complete": True, "advisory_only": True,
        "window": {"from": iso(since), "through": iso(through),
                   "overlap_seconds": int(overlap.total_seconds()), "initial_scan": state is None},
        "counts": {"candidates": len(rows), "changes": len(changes),
                   "pull_requests": sum(row["kind"] == "pull_request" for row in changes),
                   "issues": sum(row["kind"] == "issue" for row in changes),
                   "refreshed_pull_requests": len(tracked_open - set(discovered))},
        "changes": changes, "deferred_after_window": sorted(deferred), "details": details,
        "coverage": {"issue_and_pr_metadata": True, "issue_events": True,
                     "pr_paths_reviews_head_ci": details == "prs", "complete_history": False},
        "limits": {"max_items": max_items, "max_pages_per_collection": max_pages},
        "limitations": [
            "A new or reset state (including lost CI cache) starts only the explicit lookback window, not complete historical coverage. Recover an archived state to preserve an earlier cursor.",
            "GitHub listings are mutable and eventually consistent; overlap reduces gaps but is not an audit-log guarantee.",
            "With details=prs, known open PRs refresh even without an issue update; details=none tracks changed issue/PR metadata and events only, so CI-only changes require enrichment followup.",
            "Older unobserved open PRs require a larger initial window or a separate targeted followup.",
            "Review and CI signals are observations, not evaluation of branch protection, required approvals, or merge readiness.",
            "CI covers the current PR head only; merge-queue or synthetic merge-commit checks may differ.",
            "GitHub's check-run endpoint reports the most recent 1000 check suites for a head; it is not full historical CI coverage.",
            "Issue source areas are unverified text hints; PR areas use changed paths when detail collection is enabled.",
            "Proposals never change the nightly pin, merge code, or establish port compatibility.",
        ],
    }
    new_state = {
        "schema_version": 1, "repository": REPOSITORY, "cursor": iso(through),
        "bootstrap": state["bootstrap"] if state else {
            "from": iso(since), "through": iso(through), "lookback_hours": lookback_hours, "max_items": max_items,
        },
        "items": items, "seen_events": new_seen,
    }
    return report, new_state


@contextmanager
def state_lock(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    lock = path.with_name(path.name + ".lock")
    try:
        descriptor = os.open(lock, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    except FileExistsError as error:
        raise TrackingError("Another activity run holds the state lock. If a run crashed, verify it has stopped before removing the lock.") from error
    try:
        with os.fdopen(descriptor, "w") as output:
            output.write(f"{os.getpid()}\n")
        yield
    finally:
        try:
            lock.unlink()
        except OSError:
            print("Activity state lock cleanup failed; verify the completed process before removing the lock.", file=sys.stderr)


def stage_json(path: Path, value: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = (json.dumps(value, indent=2, ensure_ascii=True) + "\n").encode("ascii")
    if len(payload) > MAX_STATE:
        raise TrackingError("Activity output exceeded the supported size; existing state is unchanged.")
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(dir=path.parent, prefix=f".{path.name}.", delete=False) as output:
            temporary = Path(output.name)
            output.write(payload)
            output.flush()
            os.fsync(output.fileno())
        return temporary
    except BaseException:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
        raise


def commit_files(state_path: Path, report_path: Path, report: dict, state: dict) -> None:
    """Persist the complete report first; the final atomic replace is the cursor commit."""
    staged = []
    try:
        report_temp = stage_json(report_path, report)
        staged.append(report_temp)
        state_temp = stage_json(state_path, state)
        staged.append(state_temp)
        os.replace(report_temp, report_path)
        os.replace(state_temp, state_path)
    finally:
        for path in staged:
            path.unlink(missing_ok=True)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--lookback-hours", type=int, help="Required for a new state; bounds its initial activity window (1–720 hours).")
    parser.add_argument("--max-items", type=int, required=True, help="Abort rather than truncate when candidates plus tracked open PRs exceed this count.")
    parser.add_argument("--overlap-minutes", type=int, default=60)
    parser.add_argument("--max-pages", type=int, default=1000, help="Abort if any collection exceeds this many pages.")
    parser.add_argument("--details", choices=("prs", "none"), default="prs", help="prs includes PR paths/reviews/head CI; none explicitly requests metadata and issue events only.")
    args = parser.parse_args(argv)
    try:
        state_path, report_path = args.state.absolute(), args.output.absolute()
        if state_path.resolve() == report_path.resolve() or report_path.resolve() == state_path.with_name(state_path.name + ".lock").resolve():
            raise TrackingError("Activity state, lock, and report must be different files.")
        if state_path.is_symlink() or report_path.is_symlink():
            raise TrackingError("Activity state and report must not be symbolic links.")
        with state_lock(state_path):
            state = read_state(state_path)
            client = GitHub(os.environ.get("GITHUB_TOKEN"))
            report, updated = scan(client.get, state, through=datetime.now(timezone.utc),
                                   lookback_hours=args.lookback_hours, max_items=args.max_items,
                                   overlap_minutes=args.overlap_minutes, details=args.details, max_pages=args.max_pages)
            report["counts"]["api_requests"] = client.requests
            commit_files(state_path, report_path, report, updated)
        print(json.dumps({"state": "activity_reported", "cursor": updated["cursor"],
                          "changes": report["counts"]["changes"], "output": str(report_path)}))
        return 0
    except (TrackingError, OSError) as error:
        print(f"upstream activity tracking failed: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
