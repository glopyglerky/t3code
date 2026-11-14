"""Offline tests for qualification provenance and incremental CI baselines."""

import copy
import io
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import Mock, patch
from urllib.error import HTTPError, URLError
import zipfile

import ci_history
import ci_performance
from test_ci_performance import encoded_report
import hashlib


REPO = "glopyglerky/t3code-ls"
POLICY = "a" * 64


def receipt(commit="b" * 40, tree="c" * 40, gates=None, identifier=10, attempt=1):
    gates = gates or {name: True for name in ci_history.GATES}
    plan = {"head": commit, "source_tree": tree, "policy_digest": POLICY,
            "upstream_commit": "d" * 40, "upstream_tree": "e" * 40}
    measured, raw = encoded_report(plan, str(identifier))
    evidence = ci_performance.validate_report(measured, raw, plan, str(identifier), hashlib.sha256(raw).hexdigest())
    return {
        "schema_version": 1, "source_commit": commit, "source_tree": tree,
        "run_id": identifier, "run_attempt": attempt, "performance_run_attempt": 1,
        "performance_evidence": evidence if gates["performance"] else None,
        "policy_digest": POLICY, "status": "passed",
        "plan": {**plan, "gates": gates},
        "gate_results": {name: "success" if required else "skipped"
                         for name, required in gates.items()},
    }


def run(commit, identifier=10, **updates):
    value = {
        "id": identifier, "run_attempt": 1, "path": ci_history.WORKFLOW, "name": ci_history.WORKFLOW_NAME,
        "event": "push", "status": "completed", "conclusion": "success",
        "head_sha": commit, "head_repository": {"full_name": REPO},
    }
    value.update(updates)
    return value


def archive(value, name="ci-qualification.json"):
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", zipfile.ZIP_DEFLATED) as bundle:
        bundle.writestr(name, json.dumps(value))
    return output.getvalue()


class ReceiptValidation(unittest.TestCase):
    def test_every_required_gate_must_pass_on_the_recorded_source_and_policy(self):
        original = receipt()
        self.assertTrue(ci_history.valid_receipt(original, "b" * 40, "c" * 40, POLICY))
        mutations = [
            lambda value: value.update(schema_version=True),
            lambda value: value.update(status="running"),
            lambda value: value.update(source_commit="d" * 40),
            lambda value: value.update(source_tree="d" * 40),
            lambda value: value.update(policy_digest="d" * 64),
            lambda value: value["plan"].update(head="d" * 40),
            lambda value: value["plan"]["gates"].update(native="false"),
            lambda value: value["plan"]["gates"].pop("adapters"),
            lambda value: value["gate_results"].update(native="skipped"),
            lambda value: value["gate_results"].update(application="failure"),
            lambda value: value["gate_results"].update(extra="success"),
            lambda value: value.update(performance_evidence=[]),
            lambda value: value.update(run_attempt=0),
            lambda value: value.update(performance_run_attempt=2),
        ]
        for mutate in mutations:
            value = copy.deepcopy(original)
            mutate(value)
            self.assertFalse(ci_history.valid_receipt(value, "b" * 40, "c" * 40, POLICY))
        optional = receipt(gates={"native": True, "adapters": False, "application": False, "performance": False})
        self.assertTrue(ci_history.valid_receipt(optional, "b" * 40, "c" * 40, POLICY))
        optional["gate_results"]["application"] = "cancelled"
        self.assertFalse(ci_history.valid_receipt(optional, "b" * 40, "c" * 40, POLICY))

    def test_only_completed_same_repository_workflow_runs_are_eligible(self):
        original = run("b" * 40)
        self.assertTrue(ci_history.eligible_run(original, REPO, "99"))
        for update in [
            {"id": 99}, {"id": True}, {"name": "Unrelated workflow"},
            {"path": ".github/workflows/other.yml"}, {"event": "pull_request"},
            {"status": "in_progress"}, {"conclusion": "failure"},
            {"head_sha": "main"}, {"head_repository": {"full_name": "someone/fork"}},
            {"run_attempt": None}, {"run_attempt": True}, {"run_attempt": 0},
        ]:
            self.assertFalse(ci_history.eligible_run({**original, **update}, REPO, "99"))

    def test_receipt_archive_is_small_and_cannot_hide_extra_files_or_paths(self):
        original = receipt()
        self.assertEqual(ci_history.read_receipt(archive(original)), original)
        for payload in [archive(original, "../ci-qualification.json"),
                        archive([original]), b"a" * (ci_history.MAX_BYTES + 1)]:
            with self.assertRaises((ci_history.HistoryError, zipfile.BadZipFile)):
                ci_history.read_receipt(payload)
        output = io.BytesIO()
        with zipfile.ZipFile(output, "w", zipfile.ZIP_DEFLATED) as bundle:
            bundle.writestr("ci-qualification.json", json.dumps(original))
            bundle.writestr("extra.json", "{}")
        with self.assertRaises(ci_history.HistoryError):
            ci_history.read_receipt(output.getvalue())
        oversized = archive({"padding": "x" * (ci_history.MAX_BYTES + 1)})
        with self.assertRaises(ci_history.HistoryError):
            ci_history.read_receipt(oversized)

    def test_workflow_path_accepts_optional_api_ref_without_accepting_other_workflows(self):
        for path in [ci_history.WORKFLOW, ci_history.WORKFLOW + "@refs/heads/main",
                     ci_history.WORKFLOW + "@main"]:
            self.assertTrue(ci_history.eligible_run(run("b" * 40, path=path), REPO, "99"))
        for path in [ci_history.WORKFLOW + "@", ".github/workflows/other.yml@refs/heads/main",
                     ci_history.WORKFLOW + ".backup@main", None]:
            self.assertFalse(ci_history.eligible_run(run("b" * 40, path=path), REPO, "99"))

    def test_download_redirect_never_forwards_the_github_token(self):
        client = ci_history.GitHub(REPO, "private-token")
        redirected = "https://example.blob.core.windows.net/receipt.zip?signature=example"
        opener = Mock()
        opener.open.side_effect = [
            HTTPError(client.base, 302, "Found", {"Location": redirected}, None),
            io.BytesIO(b"archive"),
        ]
        client.opener = opener
        self.assertEqual(client.artifact(123), b"archive")
        requests = [call.args[0] for call in opener.open.call_args_list]
        self.assertEqual(requests[0].get_header("Authorization"), "Bearer private-token")
        self.assertIsNone(requests[1].get_header("Authorization"))
        self.assertEqual(requests[1].full_url, redirected)

    def test_unexpected_redirect_and_oversized_response_fail_closed(self):
        client = ci_history.GitHub(REPO, "private-token")
        client.opener = Mock()
        client.opener.open.side_effect = HTTPError(
            client.base, 302, "Found", {"Location": "http://localhost/private"}, None)
        with self.assertRaises(ci_history.HistoryError):
            client.artifact(123)
        self.assertEqual(client.opener.open.call_count, 1)
        client.opener.open.side_effect = None
        client.opener.open.return_value = io.BytesIO(b"x" * (ci_history.MAX_BYTES + 1))
        with self.assertRaises(ci_history.HistoryError):
            client.json("/actions/runs")


class QualifiedHistory(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.git("init", "--quiet")
        self.git("config", "user.email", "test@example.invalid")
        self.git("config", "user.name", "Offline test")
        (self.root / "code.rs").write_text("working Rust\n")
        self.base = self.commit("qualified code")
        (self.root / "code.rs").write_text("broken Rust\n")
        self.failed = self.commit("unqualified Rust change")
        (self.root / "README.md").write_text("documentation only\n")
        self.head = self.commit("docs after failed code")

    def git(self, *args):
        return subprocess.check_output(["git", "-C", str(self.root), *args],
                                       stderr=subprocess.DEVNULL).decode().strip()

    def commit(self, message):
        self.git("add", "--all")
        self.git("commit", "--quiet", "-m", message)
        return self.git("rev-parse", "HEAD")

    def client(self, runs, receipts, artifact_overrides=None):
        client = Mock()
        overrides = artifact_overrides or {}

        def get(path):
            if path == "/actions/workflows/hybrid.yml/runs?status=success&per_page=50":
                return {"workflow_runs": runs}
            identifier = int(path.split("/")[3])
            if path == f"/actions/runs/{identifier}":
                return next((entry for entry in runs if entry["id"] == identifier), {})
            attempt = receipts.get(identifier, {}).get("run_attempt", 1)
            entry = {"id": identifier, "name": f"ci-qualification-{attempt}", "expired": False,
                     "size_in_bytes": 1024, **overrides.get(identifier, {})}
            return {"artifacts": [entry] if identifier in receipts else []}

        client.json.side_effect = get
        client.artifact.side_effect = lambda identifier: archive(receipts[identifier])
        return client

    def record(self, commit, identifier=10, attempt=1):
        return receipt(commit, self.git("rev-parse", f"{commit}^{{tree}}"), identifier=identifier, attempt=attempt)

    def find(self, client):
        with patch("ci_history.GitHub", return_value=client):
            return ci_history.find_qualification(self.root, REPO, self.head, POLICY, "99", "token")

    def test_failed_code_then_documentation_uses_prior_qualified_ancestor(self):
        client = self.client([run(self.failed, 11, conclusion="failure"), run(self.base, 10)],
                             {10: self.record(self.base)})
        result = self.find(client)
        self.assertEqual(result["commit"], self.base)
        self.assertFalse(result["reused"])
        self.assertIn("code.rs", self.git("diff", "--name-only", result["commit"], self.head))

    def test_consumer_revalidates_only_the_exact_planned_successful_run(self):
        client = self.client([run(self.head, 11), run(self.base, 10)],
                             {10: self.record(self.base), 11: self.record(self.head, 11)})
        with patch("ci_history.GitHub", return_value=client):
            result = ci_history.find_qualification(
                self.root, REPO, self.head, POLICY, "99", "token",
                expected_run_id=10, expected_commit=self.base, fetch_missing=True)
        self.assertEqual(result["commit"], self.base)
        self.assertEqual(result["run_id"], 10)
        client.json.assert_any_call("/actions/runs/10")
        self.assertNotIn("/actions/workflows/hybrid.yml/runs?status=success&per_page=50",
                         [call.args[0] for call in client.json.call_args_list])

    def test_forged_or_failed_planned_run_is_not_fetched_or_reused(self):
        for selected in (run(self.base, 10, conclusion="failure"), run(self.failed, 10),
                         run(self.base, 11), run(self.base, 10, head_repository={"full_name": "other/repo"})):
            client = self.client([selected], {10: self.record(self.base)})
            with self.subTest(selected=selected), patch("ci_history.GitHub", return_value=client), \
                    patch("ci_history._git") as git_call:
                result = ci_history.find_qualification(
                    self.root, REPO, self.head, POLICY, "99", "token",
                    expected_run_id=10, expected_commit=self.base, fetch_missing=True)
                self.assertIsNone(result)
                git_call.assert_not_called()
                client.artifact.assert_not_called()

    def test_only_validated_missing_commit_is_fetched_by_immutable_sha(self):
        client = self.client([run(self.base, 10)], {10: self.record(self.base)})
        original = ci_history._git
        calls = []
        def missing_once(root, *args, **kwargs):
            calls.append(args)
            if args[0] == "cat-file":
                raise subprocess.CalledProcessError(1, ["git", *args])
            if args[0] == "fetch":
                return ""
            return original(root, *args, **kwargs)
        with patch("ci_history.GitHub", return_value=client), patch("ci_history._git", missing_once):
            result = ci_history.find_qualification(
                self.root, REPO, self.head, POLICY, "99", "token",
                expected_run_id=10, expected_commit=self.base, fetch_missing=True)
        self.assertEqual(result["commit"], self.base)
        self.assertEqual([args for args in calls if args[0] == "fetch"],
                         [("fetch", "--no-tags", "--no-write-fetch-head",
                           "https://github.com/glopyglerky/t3code-ls.git", self.base)])

    def test_exact_commit_reuse_wins_over_a_more_recent_ancestor_run(self):
        client = self.client([run(self.base, 12), run(self.head, 10)],
                             {12: self.record(self.base, 12), 10: self.record(self.head)})
        result = self.find(client)
        self.assertEqual(result["commit"], self.head)
        self.assertTrue(result["reused"])
        self.assertEqual(result["run_id"], 10)

    def test_unverified_success_expired_receipt_and_policy_drift_cannot_be_baselines(self):
        wrong = self.record(self.base)
        wrong["policy_digest"] = "d" * 64
        for client in [
            self.client([run(self.base)], {}),
            self.client([run(self.base)], {10: self.record(self.base)}, {10: {"expired": True}}),
            self.client([run(self.base)], {10: wrong}),
        ]:
            self.assertIsNone(self.find(client))

    def test_rerun_history_uses_the_current_receipt_and_retains_its_successful_measurement_attempt(self):
        current = self.record(self.head, attempt=2)
        runs = [run(self.head, run_attempt=2)]
        client = self.client(runs, {10: current})
        result = self.find(client)
        self.assertEqual(result["receipt"]["run_attempt"], 2)
        self.assertEqual(result["receipt"]["performance_evidence"]["ci_run_attempt"], "1")
        client.json.assert_any_call("/actions/runs/10/artifacts?name=ci-qualification-2&per_page=10")
        self.assertIsNone(self.find(self.client(runs, {10: current}, {10: {"name": "ci-qualification-1"}})))
        self.assertIsNone(self.find(self.client(runs, {10: self.record(self.head)})))
        wrong_run = {**current, "run_id": 11}
        self.assertIsNone(self.find(self.client(runs, {10: wrong_run})))

    def test_unrelated_success_and_missing_git_objects_cannot_be_baselines(self):
        self.git("switch", "--quiet", "--orphan", "unrelated")
        (self.root / "other.rs").write_text("unrelated source\n")
        unrelated = self.commit("unrelated qualified branch")
        client = self.client([run(unrelated, 10), run("d" * 40, 11)],
                             {10: self.record(unrelated), 11: receipt("d" * 40)})
        self.assertIsNone(self.find(client))
        client.artifact.assert_not_called()

    def test_network_failure_or_malformed_archive_selects_full_qualification(self):
        client = self.client([run(self.base)], {10: self.record(self.base)})
        client.json.side_effect = URLError("offline")
        self.assertIsNone(self.find(client))
        client = self.client([run(self.base)], {10: self.record(self.base)})
        client.artifact.side_effect = None
        client.artifact.return_value = b"invalid ZIP"
        self.assertIsNone(self.find(client))


if __name__ == "__main__":
    unittest.main()
