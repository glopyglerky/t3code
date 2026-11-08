"""Offline behavior tests for bounded, durable upstream activity monitoring."""

import copy
from datetime import timedelta
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import activity

NOW = activity.timestamp("2026-11-08T12:00:00Z")
HEAD, BASE = "a" * 40, "b" * 40


def issue(number=1, **overrides):
    return {
        "id": number * 10, "number": number, "title": "Mobile reconnect problem",
        "state": "open", "created_at": "2026-11-08T10:00:00Z",
        "updated_at": "2026-11-08T11:00:00Z", "closed_at": None,
        "labels": [{"name": "bug"}], "body": "Related to apps/mobile/ and T3 Connect.",
        "comments": 0, **overrides,
    }


def pull(number=2, **overrides):
    values = dict(pull_request={"url": f"{activity.API}/pulls/{number}"},
                  merged=False, merged_at=None, draft=False, mergeable=None,
                  mergeable_state="unknown", head={"sha": HEAD},
                  base={"sha": BASE, "ref": "main"}, requested_reviewers=[],
                  requested_teams=[], changed_files=1)
    values.update(overrides)
    return issue(number, **values)


def event(identifier=100, kind="labeled", created="2026-11-08T11:30:00Z", **overrides):
    return {"id": identifier, "event": kind, "created_at": created,
            "actor": {"login": "maintainer"}, **overrides}


class API:
    def __init__(self, fixtures):
        self.fixtures, self.calls = fixtures, []

    def __call__(self, url):
        self.calls.append(url)
        value = self.fixtures[url]
        if isinstance(value, Exception):
            raise value
        return copy.deepcopy(value)


def fixtures(rows, *, since=NOW - timedelta(hours=24)):
    result = {activity.issue_query(since): (rows, {})}
    for row in rows:
        number = row["number"]
        result[f"{activity.API}/issues/{number}/events?per_page=100"] = ([], {})
        if isinstance(row.get("pull_request"), dict):
            result[f"{activity.API}/pulls/{number}"] = (row, {})
    return result


def enriched(number=2, *, conclusion=None):
    return {
        f"{activity.API}/pulls/{number}/files?per_page=100": ([{
            "filename": "packages/contracts/src/auth.ts",
            "previous_filename": "apps/mobile/old-auth.ts", "status": "renamed",
            "patch": "NEVER RETAIN THIS PATCH BODY",
        }], {}),
        f"{activity.API}/pulls/{number}/reviews?per_page=100": ([{
            "id": 50, "user": {"login": "alice"}, "state": "APPROVED",
            "commit_id": HEAD, "submitted_at": "2026-11-08T11:00:00Z",
        }], {}),
        f"{activity.API}/commits/{HEAD}/check-runs?filter=latest&per_page=100": ({
            "total_count": 1, "check_runs": [{
                "id": 70, "head_sha": HEAD, "name": "test", "status": "completed" if conclusion else "in_progress",
                "conclusion": conclusion, "html_url": f"https://github.com/{activity.REPOSITORY}/actions/runs/70",
            }],
        }, {}),
        f"{activity.API}/commits/{HEAD}/status?per_page=100": ({"total_count": 0, "statuses": []}, {}),
    }


def run_scan(get, state=None, **kwargs):
    options = {"through": NOW, "lookback_hours": 24, "max_items": 500, "details": "none"}
    options.update(kwargs)
    return activity.scan(get, state, **options)


class ActivityPagination(unittest.TestCase):
    def test_every_page_including_empty_intermediate_page_is_followed(self):
        first = activity.issue_query(NOW - timedelta(hours=24))
        second = first + "&page=2"
        third = second.replace("page=2", "page=3").replace(
            f"/repos/{activity.REPOSITORY}", f"/repositories/{activity.REPOSITORY_ID}")
        data = fixtures([issue(), issue(3)])
        data[first] = ([issue()], {"Link": f'<{second}>; rel="next", <{third}>; rel="last"'})
        data[second] = ([], {"Link": f'<{third}>; rel="next"'})
        data[third] = ([issue(3)], {})
        report, state = run_scan(API(data))
        self.assertEqual({row["number"] for row in report["changes"]}, {1, 3})
        self.assertEqual(state["cursor"], activity.iso(NOW))
        self.assertTrue(report["complete"])

    def test_page_cycles_count_limits_duplicate_rows_and_changed_filters_fail_closed(self):
        first = activity.issue_query(NOW - timedelta(hours=24))
        variants = [
            {first: ([issue()], {"Link": f'<{first}>; rel="next"'})},
            {first: ([issue(), issue(3)], {})},
            {first: ([issue(), issue()], {})},
            {first: ([], {"Link": f'<{first.replace("state=all", "state=open")}&page=2>; rel="next"'})},
            {first: ([], {"Link": '<https://evil.example/collect>; rel="next"'})},
        ]
        for data in variants:
            with self.subTest(data=data), self.assertRaises(activity.TrackingError):
                run_scan(API(data), max_items=1)

    def test_nested_collection_total_mismatch_and_file_cap_do_not_claim_complete(self):
        url = f"{activity.API}/commits/{HEAD}/check-runs?filter=latest&per_page=100"
        with self.assertRaisesRegex(activity.TrackingError, "incomplete"):
            activity.pages(API({url: ({"total_count": 2, "check_runs": [{"id": 1}]}, {})}), url, key="check_runs")
        with self.assertRaisesRegex(activity.TrackingError, "3000"):
            activity.pr_files(API({}), 2, {"head_sha": HEAD, "base_sha": BASE, "base_ref": "main"}, 3001, {}, 10)


class ActivityChanges(unittest.TestCase):
    def test_new_issue_opened_labels_and_source_hints_are_advisory(self):
        data = fixtures([issue()])
        data[f"{activity.API}/issues/1/events?per_page=100"] = ([event(label={"name": "bug"})], {})
        report, _ = run_scan(API(data))
        changed = report["changes"][0]
        self.assertIn("opened", changed["actions"])
        self.assertIn("labeled", changed["actions"])
        self.assertEqual(changed["labels_added"], ["bug"])
        self.assertIn("mobile", changed["surfaces"])
        self.assertIn("unverified", changed["surface_basis"])
        self.assertTrue(report["advisory_only"])
        self.assertNotIn("body_sha256", changed)

    def test_close_and_reopen_between_polls_are_both_reported_despite_same_final_state(self):
        _, previous = run_scan(API(fixtures([issue()])))
        current = issue(updated_at="2026-11-08T13:30:00Z")
        data = fixtures([current], since=NOW - timedelta(hours=1))
        data[f"{activity.API}/issues/1/events?per_page=100"] = ([
            event(101, "closed", "2026-11-08T13:10:00Z"),
            event(102, "reopened", "2026-11-08T13:20:00Z"),
        ], {})
        report, _ = run_scan(API(data), previous, through=NOW + timedelta(hours=2))
        self.assertEqual(report["changes"][0]["state"], "open")
        self.assertTrue({"closed", "reopened"}.issubset(report["changes"][0]["actions"]))

    def test_overlap_deduplicates_seen_events_but_catches_late_visible_events(self):
        data = fixtures([issue()])
        events_url = f"{activity.API}/issues/1/events?per_page=100"
        data[events_url] = ([event()], {})
        _, previous = run_scan(API(data))
        next_data = fixtures([issue()], since=NOW - timedelta(hours=1))
        next_data[events_url] = ([event()], {})
        report, _ = run_scan(API(next_data), previous, through=NOW + timedelta(minutes=30))
        self.assertEqual(report["changes"], [])
        next_data[events_url] = ([event(), event(101, "unlabeled", "2026-11-08T11:45:00Z")], {})
        report, _ = run_scan(API(next_data), previous, through=NOW + timedelta(minutes=30))
        self.assertEqual(report["changes"][0]["actions"], ["unlabeled"])
        self.assertEqual(report["changes"][0]["changed_fields"], [])

    def test_snapshot_after_scan_start_is_deferred_without_advancing_past_it(self):
        data = fixtures([issue(updated_at="2026-11-08T12:01:00Z")])
        api = API(data)
        report, state = run_scan(api)
        self.assertEqual(report["deferred_after_window"], [1])
        self.assertEqual(report["changes"], [])
        self.assertEqual(state["items"], {})
        self.assertEqual(state["cursor"], "2026-11-08T12:00:00Z")
        self.assertEqual(len(api.calls), 1)

    def test_merged_pr_and_metadata_mode_skip_files_reviews_and_ci(self):
        merged = pull(state="closed", closed_at="2026-11-08T11:00:00Z",
                      merged=True, merged_at="2026-11-08T11:00:00Z")
        data = fixtures([merged])
        api = API(data)
        report, _ = run_scan(api)
        self.assertIn("merged", report["changes"][0]["actions"])
        self.assertTrue(report["changes"][0]["pull_request"]["advisory_only"])
        self.assertFalse(any(path.endswith(("/files?per_page=100", "/reviews?per_page=100")) or "/commits/" in path for path in api.calls))

    def test_old_merge_with_new_label_activity_is_not_announced_as_a_new_merge(self):
        old = pull(created_at="2026-10-01T10:00:00Z", state="closed", merged=True,
                   merged_at="2026-10-02T10:00:00Z", closed_at="2026-10-02T10:00:00Z")
        report, _ = run_scan(API(fixtures([old])))
        self.assertEqual(report["changes"][0]["actions"], ["updated"])

    def test_metadata_mode_does_not_refresh_accumulated_unchanged_open_prs(self):
        _, previous = run_scan(API(fixtures([pull()])))
        saved = previous["items"].pop("2")
        for number in range(1, 1002):
            previous["items"][str(number)] = {**saved, "number": number}
        api = API(fixtures([], since=NOW - timedelta(hours=1)))
        report, _ = run_scan(api, previous, through=NOW + timedelta(hours=2), max_items=1)
        self.assertEqual(report["counts"]["refreshed_pull_requests"], 0)
        self.assertEqual(report["changes"], [])
        self.assertEqual(len(api.calls), 1)

    def test_known_open_pr_ci_refreshes_without_issue_update_and_paths_are_reused(self):
        original = pull()
        data = fixtures([original]) | enriched()
        first, state = run_scan(API(data), details="prs")
        changed = first["changes"][0]
        self.assertEqual(changed["surface_basis"], "pull_request_file_paths")
        self.assertEqual(changed["surfaces"], ["contracts", "mobile"])
        self.assertNotIn("NEVER RETAIN", json.dumps(state))
        later = fixtures([], since=NOW - timedelta(hours=1)) | enriched(conclusion="success")
        later[f"{activity.API}/pulls/2"] = (original, {})
        reviews_url = f"{activity.API}/pulls/2/reviews?per_page=100"
        later[reviews_url][0].append({"id": 51, "user": {"login": "alice"}, "state": "COMMENTED"})
        api = API(later)
        report, _ = run_scan(api, state, through=NOW + timedelta(hours=2), details="prs")
        self.assertEqual(report["counts"]["refreshed_pull_requests"], 1)
        self.assertIn("pull_request.ci", report["changes"][0]["changed_fields"])
        review = report["changes"][0]["pull_request"]["reviews"]["latest_decisions"][0]
        self.assertEqual(review["state"], "APPROVED")
        self.assertFalse(any("/files?" in path for path in api.calls))
        self.assertFalse(any("/comments" in path or path.endswith(".patch") for path in api.calls))

    def test_missing_initial_window_or_future_cursor_fails_before_any_api_call(self):
        api = API({})
        with self.assertRaisesRegex(activity.TrackingError, "First run"):
            run_scan(api, lookback_hours=None)
        previous = {"cursor": "2026-11-09T00:00:00Z"}
        with self.assertRaisesRegex(activity.TrackingError, "future"):
            run_scan(api, previous)
        self.assertEqual(api.calls, [])


class ActivityPersistence(unittest.TestCase):
    def test_partial_api_failure_preserves_prior_state_and_report_byte_for_byte(self):
        report, state = run_scan(API(fixtures([issue()])))
        with tempfile.TemporaryDirectory() as directory:
            state_path, output_path = Path(directory) / "state.json", Path(directory) / "report.json"
            activity.commit_files(state_path, output_path, report, state)
            before_state, before_report = state_path.read_bytes(), output_path.read_bytes()
            calls = []

            def partial_failure(url):
                calls.append(url)
                if len(calls) == 1:
                    return [issue()], {"Link": f'<{url}&page=2>; rel="next"'}
                raise activity.TrackingError("page two failed")

            with patch.object(activity.GitHub, "get", side_effect=partial_failure), \
                    patch("sys.stderr", new_callable=io.StringIO):
                code = activity.main(["--state", str(state_path), "--output", str(output_path), "--max-items", "500"])
            self.assertEqual(code, 1)
            self.assertEqual(state_path.read_bytes(), before_state)
            self.assertEqual(output_path.read_bytes(), before_report)
            self.assertEqual(len(calls), 2)
            self.assertFalse(state_path.with_name("state.json.lock").exists())

    def test_failed_report_commit_cannot_advance_state_and_staged_files_are_cleaned(self):
        with tempfile.TemporaryDirectory() as directory:
            state_path, report_path = Path(directory) / "state.json", Path(directory) / "report.json"
            state_path.write_bytes(b"original user state")
            report_path.write_bytes(b"previous report")
            with patch.object(activity.os, "replace", side_effect=OSError("disk failure")):
                with self.assertRaises(OSError):
                    activity.commit_files(state_path, report_path, {"complete": True}, {"cursor": "new"})
            self.assertEqual(state_path.read_bytes(), b"original user state")
            self.assertEqual(report_path.read_bytes(), b"previous report")
            self.assertEqual(sorted(path.name for path in Path(directory).iterdir()), ["report.json", "state.json"])

    def test_state_commit_failure_leaves_complete_report_and_old_cursor_for_safe_retry(self):
        with tempfile.TemporaryDirectory() as directory:
            state_path, report_path = Path(directory) / "state.json", Path(directory) / "report.json"
            state_path.write_bytes(b"old cursor")
            replace = activity.os.replace

            def fail_state(source, destination):
                if destination == state_path:
                    raise OSError("state directory unavailable")
                replace(source, destination)

            with patch.object(activity.os, "replace", side_effect=fail_state), self.assertRaises(OSError):
                activity.commit_files(state_path, report_path, {"complete": True}, {"cursor": "new"})
            self.assertEqual(state_path.read_bytes(), b"old cursor")
            self.assertEqual(json.loads(report_path.read_text()), {"complete": True})

    def test_corrupt_or_wrong_repository_state_is_not_reset_and_lock_blocks_concurrent_run(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.json"
            for data in [b"invalid json", json.dumps({"schema_version": 1, "repository": "other/repo"}).encode()]:
                path.write_bytes(data)
                with self.assertRaises(activity.TrackingError):
                    activity.read_state(path)
                self.assertEqual(path.read_bytes(), data)
            with activity.state_lock(path):
                with self.assertRaisesRegex(activity.TrackingError, "holds the state lock"):
                    with activity.state_lock(path):
                        self.fail("concurrent writer entered")

    def test_report_cannot_alias_or_overwrite_the_state(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.json"
            path.write_bytes(b"user state")
            with patch("sys.stderr", new_callable=io.StringIO):
                code = activity.main(["--state", str(path), "--output", str(path), "--max-items", "500", "--lookback-hours", "24"])
            self.assertEqual(code, 1)
            self.assertEqual(path.read_bytes(), b"user state")


if __name__ == "__main__":
    unittest.main()
