"""Evidence issuance tests using GitHub job results and actual Git checkouts."""

import copy
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

import ci_history
import ci_receipt


HEAD = "a" * 40
TREE = "b" * 40
POLICY = "c" * 64
SCRIPT = Path(ci_receipt.__file__).resolve()


def plan(mode="full-fork"):
    selected = mode == "full-fork"
    return {
        "schema_version": 1, "head": HEAD, "source_tree": TREE, "policy_digest": POLICY,
        "mode": mode, "base": None if selected else HEAD, "base_run_id": None if selected else 100,
        "reused_run_id": 100 if mode == "reused" else None,
        "gates": {gate: selected for gate in ci_history.GATES},
        "rust_packages": ["t3code-core"] if selected else [],
        "adapter_ports": ["project-commands"] if selected else [], "stage_native": selected,
        "component_tests": [],
        "changed_paths": ["apps/web/src/App.tsx"], "reasons": ["Example selection"],
    }


def needs(value):
    return {"plan": {"result": "success", "outputs": {}},
            "database_platforms": {"result": "success" if value["gates"]["native"] else "skipped"},
            **{gate: {"result": "success" if selected else "skipped", "outputs": {}}
               for gate, selected in value["gates"].items()}}


class ReceiptIssuance(unittest.TestCase):
    def test_successful_full_run_issues_a_compact_compatible_receipt(self):
        value = plan()
        value["changed_paths"] = ["large unrelated plan metadata " * 50000]
        statuses = needs(value)
        statuses["native"]["future_metadata"] = {"trace": "optional"}
        result = ci_receipt.make_receipt(value, statuses, HEAD, "101")
        self.assertTrue(ci_history.valid_receipt(result, HEAD, TREE, POLICY))
        self.assertEqual(result["run_id"], 101)
        self.assertEqual(result["database_platform_result"], "success")
        self.assertEqual(result["plan"]["adapter_ports"], ["project-commands"])
        self.assertNotIn("changed_paths", result["plan"])
        self.assertNotIn("reasons", result["plan"])
        self.assertNotIn("future_metadata", json.dumps(result))
        self.assertLess(len(json.dumps(result)), 2048)

    def test_affected_native_and_documentation_runs_accept_only_their_required_results(self):
        for native in (True, False):
            with self.subTest(native=native):
                value = plan("affected-fork")
                value["gates"]["native"] = native
                value["rust_packages"] = ["t3code-core"] if native else []
                result = ci_receipt.make_receipt(value, needs(value), HEAD)
                self.assertTrue(ci_history.valid_receipt(result, HEAD, TREE, POLICY))
                self.assertEqual(result["gate_results"]["application"], "skipped")
        value = plan("affected-fork")
        statuses = needs(value)
        statuses["native"]["result"] = "success"
        self.assertEqual(ci_receipt.make_receipt(value, statuses, HEAD)["status"], "passed")

    def test_exact_source_reuse_records_the_prior_qualified_run(self):
        value = plan("reused")
        result = ci_receipt.make_receipt(value, needs(value), HEAD, "101")
        self.assertEqual(result["plan"]["reused_run_id"], 100)
        self.assertTrue(ci_history.valid_receipt(result, HEAD, TREE, POLICY))
        for changes in [{"base": "d" * 40}, {"base_run_id": 99}, {"reused_run_id": None}]:
            with self.subTest(changes=changes), self.assertRaises(ci_receipt.ReceiptError):
                ci_receipt.make_receipt({**value, **changes}, needs(value), HEAD)
        with self.assertRaises(ci_receipt.ReceiptError):
            ci_receipt.make_receipt(value, needs(value), HEAD, "100")

    def test_missing_failed_cancelled_and_skipped_required_jobs_do_not_issue_evidence(self):
        value = plan()
        for job in ("plan", "database_platforms", *ci_history.GATES):
            for status in ("failure", "cancelled", "skipped", "queued", None):
                with self.subTest(job=job, status=status):
                    statuses = needs(value)
                    statuses[job]["result"] = status
                    with self.assertRaises(ci_receipt.ReceiptError):
                        ci_receipt.make_receipt(value, statuses, HEAD)
            statuses = needs(value)
            del statuses[job]
            with self.assertRaises(ci_receipt.ReceiptError):
                ci_receipt.make_receipt(value, statuses, HEAD)
        statuses = needs(value)
        statuses["unknown"] = {"result": "success"}
        with self.assertRaises(ci_receipt.ReceiptError):
            ci_receipt.make_receipt(value, statuses, HEAD)
        value = plan("reused")
        statuses = needs(value)
        statuses["native"]["result"] = "failure"
        with self.assertRaises(ci_receipt.ReceiptError):
            ci_receipt.make_receipt(value, statuses, HEAD)

    def test_inconsistent_native_binaries_or_selected_adapters_cannot_pass(self):
        mutations = [
            lambda value: value.update(stage_native=False),
            lambda value: value.update(stage_native="true"),
            lambda value: value.update(rust_packages=[]),
            lambda value: value.update(adapter_ports=[]),
            lambda value: value["gates"].update(native=False),
            lambda value: value["gates"].update(adapters=False),
            lambda value: value.update(component_tests=["../unknown.mjs"]),
            lambda value: value.update(component_tests=["tools/check-policy.mjs"] * 2),
        ]
        for mutate in mutations:
            value = plan()
            mutate(value)
            with self.assertRaises(ci_receipt.ReceiptError):
                ci_receipt.make_receipt(value, needs(value), HEAD)
        value = plan("affected-fork")
        value["gates"]["application"] = True
        value["stage_native"] = True
        with self.assertRaises(ci_receipt.ReceiptError):
            ci_receipt.make_receipt(value, needs(value), HEAD)

    def test_malformed_identity_policy_and_plan_fields_cannot_issue_evidence(self):
        mutations = [
            lambda value: value.update(schema_version=True),
            lambda value: value.update(head="d" * 40),
            lambda value: value.update(source_tree="main"),
            lambda value: value.update(policy_digest="d" * 40),
            lambda value: value.update(mode="trusted"),
            lambda value: value.update(base_run_id=True),
            lambda value: value.pop("reused_run_id"),
            lambda value: value["gates"].update(native="false"),
            lambda value: value["gates"].update(extra=True),
            lambda value: value.update(rust_packages=["--all"]),
            lambda value: value.update(adapter_ports=["project-commands", "project-commands"]),
        ]
        for mutate in mutations:
            value = plan()
            statuses = needs(value)
            mutate(value)
            with self.assertRaises(ci_receipt.ReceiptError):
                ci_receipt.make_receipt(value, statuses, HEAD)
        for value in ([], None, "not a plan"):
            with self.assertRaises(ci_receipt.ReceiptError):
                ci_receipt.make_receipt(value, needs(plan()), HEAD)


class ReceiptCommand(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.git("init", "--quiet")
        self.git("config", "user.email", "test@example.invalid")
        self.git("config", "user.name", "Offline test")
        (self.root / "source.rs").write_text("fn main() {}\n")
        self.git("add", "source.rs")
        self.git("commit", "--quiet", "-m", "qualified source")
        self.value = plan()
        self.value.update(head=self.git("rev-parse", "HEAD"), source_tree=self.git("rev-parse", "HEAD^{tree}"))
        self.input = self.root / "plan.json"
        self.output = self.root / "proof" / "ci-qualification.json"

    def git(self, *args):
        return subprocess.check_output(["git", "-C", str(self.root), *args],
                                       stderr=subprocess.DEVNULL, text=True).strip()

    def command(self, statuses=None, **environment):
        self.input.write_text(json.dumps(self.value))
        env = {**os.environ, "CI_HEAD": self.value["head"], "CI_RUN_ID": "101",
               "CI_NEEDS": json.dumps(needs(self.value) if statuses is None else statuses), **environment}
        return subprocess.run([sys.executable, str(SCRIPT), "--plan", str(self.input),
                               "--output", str(self.output)], cwd=self.root, env=env,
                              capture_output=True, text=True, timeout=10)

    def test_command_writes_receipt_for_successful_exact_checkout(self):
        result = self.command()
        self.assertEqual(result.returncode, 0, result.stderr)
        output = json.loads(self.output.read_text())
        self.assertTrue(ci_history.valid_receipt(output, self.value["head"], self.value["source_tree"], POLICY))

    def test_failure_never_creates_or_overwrites_a_receipt(self):
        statuses = needs(self.value)
        statuses["application"]["result"] = "skipped"
        result = self.command(statuses)
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(self.output.exists())
        self.output.parent.mkdir()
        self.output.write_text("existing evidence\n")
        result = self.command(statuses)
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(self.output.read_text(), "existing evidence\n")

    def test_modified_or_staged_source_cannot_receive_a_commit_receipt(self):
        (self.root / "source.rs").write_text("unqualified source change\n")
        for staged in (False, True):
            if staged:
                self.git("add", "source.rs")
            result = self.command()
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("tracked source changes", result.stderr)
            self.assertFalse(self.output.exists())

    def test_wrong_checkout_tree_and_malformed_environment_fail_closed(self):
        original = copy.deepcopy(self.value)
        for changes in [{"head": "d" * 40}, {"source_tree": "d" * 40}]:
            self.value = {**original, **changes}
            result = self.command()
            self.assertNotEqual(result.returncode, 0)
            self.assertFalse(self.output.exists())
        self.value = original
        for environment in [{"CI_NEEDS": "{"}, {"CI_NEEDS": "[]"}, {"CI_HEAD": ""}, {"CI_RUN_ID": "oops"}]:
            result = self.command(**environment)
            self.assertNotEqual(result.returncode, 0)
            self.assertFalse(self.output.exists())


if __name__ == "__main__":
    unittest.main()
