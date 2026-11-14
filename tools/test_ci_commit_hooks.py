"""Exercise the real Git hook boundary in owned disposable repositories."""

import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

import nightly
from ci_plan import policy_digest
from ci_commit_hooks import HookError, digest, git, restore, verify_hooks


HOOK = '''#!/usr/bin/env python3
import os,pathlib,subprocess,sys,time
root=pathlib.Path.cwd()
mode=os.environ.get("FIXTURE_HOOK", "pass")
print("normal pre-commit hook", flush=True)
if mode == "inherited":
 selected=subprocess.check_output(["git","diff","--cached","--name-only"],text=True).splitlines()
 print("selected paths: "+repr(selected),flush=True)
 if "candidate.txt" in selected and "inherited finding" in (root/"candidate.txt").read_text():
  print("inherited source is still invalid",file=sys.stderr,flush=True)
  sys.exit(7)
if mode == "plan-change":
 pathlib.Path(os.environ["FIXTURE_PLAN"]).write_text("{}")
if mode == "fail":
 print("deliberate unused-code finding",file=sys.stderr,flush=True)
 sys.exit(7)
if mode in ("staged-change", "unstaged-change"):
 (root/"candidate.txt").write_text("formatter changed source\\n")
 if mode == "staged-change": subprocess.run(["git","add","candidate.txt"],check=True)
if mode == "new-file":
 (root/"new-source.txt").write_text("new source\\n")
 sys.exit(9)
if mode in ("sleep", "orphan"):
 child=subprocess.Popen([sys.executable,"-c","import signal,time; signal.signal(signal.SIGTERM,signal.SIG_IGN); time.sleep(120)"])
 pathlib.Path(os.environ["FIXTURE_CHILD_PID"]).write_text(str(child.pid))
 if mode == "sleep": time.sleep(120)
sys.exit(subprocess.run([str(root/"node_modules/.bin/vp"),"run","knip:check"]).returncode)
'''


class HostedCommitHooks(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="t3-hosted-hooks-")
        self.addCleanup(self.temporary.cleanup)
        base = Path(self.temporary.name)
        self.root = base / "checkout"
        self.runner = base / "runner"
        self.home = base / "home"
        for path in (self.root, self.runner, self.home):
            path.mkdir()
        self.env = {key: val for key, val in os.environ.items()
                    if not key.startswith("GIT_") and key not in ("HUSKY", "VP_GIT_HOOKS", "VITE_GIT_HOOKS")}
        self.env.update(GITHUB_ACTIONS="true", CI="true", GITHUB_WORKSPACE=str(self.root),
                        RUNNER_TEMP=str(self.runner), GITHUB_RUN_ID="123", GITHUB_RUN_ATTEMPT="2",
                        GITHUB_REPOSITORY="glopyglerky/t3code-ls",
                        RUNNER_ENVIRONMENT="github-hosted",
                        HOME=str(self.home), XDG_CONFIG_HOME=str(self.home / ".config"),
                        FIXTURE_CHILD_PID=str(self.runner / "owned-child.pid"))
        self.git("init", "-q")
        self.git("config", "user.name", "Hook fixture")
        self.git("config", "user.email", "hook-fixture@example.invalid")
        (self.root / ".gitignore").write_text("node_modules/\n.vite-hooks/_/\n")
        (self.root / ".vite-hooks").mkdir()
        (self.root / ".vite-hooks/pre-commit").write_text(HOOK)
        (self.root / "package.json").write_text(json.dumps({"scripts": {
            "knip:check": "vp run knip:check:all",
            "knip:check:files": "knip --include files,dependencies --no-config-hints",
            "knip:check:exports": "knip --workspace apps/server --exports --no-config-hints",
        }}))
        (self.root / "vite.config.ts").write_text("// Controlled hook task fixture.\n")
        (self.root / "upstream-only.ts").write_text("// Unchanged trusted upstream source.\n")
        self.git("add", ".")
        self.git("commit", "-qm", "fixture base")
        self.parent = self.git("rev-parse", "HEAD")
        self.upstream = self.parent
        self.upstream_tree = self.git("rev-parse", "HEAD^{tree}")
        (self.root / "upstream.lock.json").write_text(json.dumps({"upstream": {
            "repository": "pingdotgg/t3code", "commit": self.upstream, "root_tree": self.upstream_tree,
        }}))
        (self.root / "candidate.txt").write_text("candidate source\n")
        self.expected_candidate = "candidate source\n"
        self.git("add", ".")
        self.git("commit", "-qm", "fixture candidate")
        self.head = self.git("rev-parse", "HEAD")
        self.tree = self.git("rev-parse", "HEAD^{tree}")
        self.git("checkout", "--detach", "-q", self.head)
        self.env["GITHUB_SHA"] = self.head
        installed = self.root / ".vite-hooks/_"
        installed.mkdir()
        entry = installed / "pre-commit"
        entry.write_text('#!/bin/sh\nexec python3 "$GITHUB_WORKSPACE/.vite-hooks/pre-commit"\n')
        entry.chmod(0o755)
        self.git("config", "core.hooksPath", ".vite-hooks/_")
        binary = self.root / "node_modules/.bin/vp"
        binary.parent.mkdir(parents=True)
        binary.write_text('#!/bin/sh\n[ "$1" = run ] && [ "$2" = knip:check ] || exit 90\nprintf "full files/dependencies scan\\nfull exports scan\\n"\nexit "${FIXTURE_KNIP_EXIT:-0}"\n')
        binary.chmod(0o755)
        self.output = self.runner / "proof.json"
        self.plan = self.runner / "ci-plan.json"
        self.env["FIXTURE_PLAN"] = str(self.plan)
        self.write_plan()

    def write_plan(self, **changes):
        value = {
            "schema_version": 1, "head": self.head, "source_tree": self.tree,
            "policy_digest": policy_digest(nightly.read_tree(self.root, self.head)),
            "upstream_commit": self.upstream, "upstream_tree": self.upstream_tree,
            "base": None, "base_run_id": None, "ci_repository": self.env["GITHUB_REPOSITORY"],
            "ci_run_id": "123", "ci_run_attempt": "1", **changes,
        }
        self.plan.write_text(json.dumps(value))

    def commit_candidate(self, message):
        self.git("add", "--all")
        self.git("-c", "core.hooksPath=/dev/null", "commit", "-qm", message)
        self.parent = self.git("rev-parse", "HEAD^1")
        self.head = self.git("rev-parse", "HEAD")
        self.tree = self.git("rev-parse", "HEAD^{tree}")
        self.env["GITHUB_SHA"] = self.head
        self.write_plan()

    def git(self, *args):
        return subprocess.run(["git", "-C", str(self.root), *args], env=self.env,
                              check=True, capture_output=True, text=True).stdout.strip()

    def run_hook(self, **changes):
        return verify_hooks(self.root, self.output, dict(self.env, **changes), self.plan)

    def assert_restored(self):
        self.assertEqual(self.git("rev-parse", "HEAD"), self.head)
        self.assertEqual(self.git("write-tree"), self.tree)
        self.assertEqual(self.git("status", "--porcelain", "--untracked-files=all"), "")
        self.assertEqual((self.root / "candidate.txt").read_text(), self.expected_candidate)
        self.assertTrue((self.root / "node_modules/.bin/vp").is_file())
        self.assertEqual(subprocess.run(["git", "-C", str(self.root), "symbolic-ref", "-q", "HEAD"],
                                       capture_output=True).returncode, 1)

    def test_normal_hook_and_both_full_scans_preserve_exact_candidate(self):
        proof = self.run_hook()
        self.assertEqual(proof["status"], "passed")
        self.assertEqual(proof["source_commit"], self.head)
        self.assertEqual(proof["source_tree"], self.tree)
        self.assertEqual(proof["first_parent"], self.parent)
        self.assertEqual(proof["changed_paths"], ["candidate.txt", "upstream.lock.json"])
        self.assertEqual(proof["hook_base"], self.upstream)
        self.assertEqual(proof["hook_base_reason"], "all-fork-differences-from-trusted-upstream")
        self.assertEqual(proof["ci_plan"]["producing_run_attempt"], "1")
        self.assertNotIn("upstream-only.ts", proof["changed_paths"])
        self.assertEqual(proof["hook_result"], {"exit_code": 0, "timed_out": False})
        log = self.output.with_suffix(".stdout.log").read_text() + self.output.with_suffix(".stderr.log").read_text()
        self.assertIn("normal pre-commit hook", log)
        self.assertIn("full files/dependencies scan", log)
        self.assertIn("full exports scan", log)
        self.assert_restored()

    def test_hook_failure_retains_diagnostic_and_restores_before_return(self):
        with self.assertRaisesRegex(HookError, "status 7"):
            self.run_hook(FIXTURE_HOOK="fail")
        proof = json.loads(self.output.read_text())
        self.assertEqual(proof["status"], "failed")
        self.assertIn("deliberate unused-code finding", self.output.with_suffix(".stderr.log").read_text())
        self.assert_restored()

    def test_unrelated_followup_still_checks_inherited_failed_fork_source(self):
        self.expected_candidate = "inherited finding\n"
        (self.root / "candidate.txt").write_text(self.expected_candidate)
        self.commit_candidate("unqualified source finding")
        (self.root / "followup.md").write_text("unrelated documentation\n")
        self.commit_candidate("later documentation does not qualify source")
        self.assertEqual(self.git("diff", "--name-only", self.parent, self.head), "followup.md")
        with self.assertRaisesRegex(HookError, "status 7"):
            self.run_hook(FIXTURE_HOOK="inherited")
        proof = json.loads(self.output.read_text())
        self.assertEqual(proof["first_parent"], self.parent)
        self.assertNotEqual(proof["first_parent"], proof["hook_base"])
        self.assertIn("candidate.txt", proof["changed_paths"])
        self.assertNotIn("upstream-only.ts", proof["changed_paths"])
        self.assertIn("inherited source is still invalid", self.output.with_suffix(".stderr.log").read_text())
        self.assert_restored()

    def test_verified_qualified_ancestor_is_revalidated_before_selecting_its_delta(self):
        base = self.head
        (self.root / "followup.md").write_text("later fork edit\n")
        self.commit_candidate("followup after qualified code")
        self.write_plan(base=base, base_run_id=10)
        record = {"commit": base, "run_id": 10, "receipt": {"source_commit": base}}
        with patch("ci_commit_hooks.find_qualification", return_value=record) as verify:
            proof = self.run_hook()
        self.assertEqual(proof["hook_base"], base)
        self.assertEqual(proof["hook_base_reason"], "verified-qualified-ancestor")
        self.assertEqual(proof["changed_paths"], ["followup.md"])
        self.assertEqual(verify.call_args.kwargs,
                         {"expected_run_id": 10, "expected_commit": base, "fetch_missing": True})
        self.assert_restored()

    def test_stale_wrong_run_wrong_policy_and_forged_base_refuse_before_hook(self):
        mutations = [
            ({"head": "a" * 40}, "exact candidate"),
            ({"source_tree": "a" * 40}, "exact candidate"),
            ({"ci_repository": "someone/else"}, "current repository"),
            ({"ci_run_id": "124"}, "this run"),
            ({"ci_run_attempt": "3"}, "producing attempt"),
            ({"ci_run_attempt": "0"}, "producing attempt"),
            ({"policy_digest": "a" * 64}, "stale policy"),
            ({"upstream_commit": "a" * 40}, "upstream reference"),
            ({"base": None, "base_run_id": 10}, "absent qualified baseline"),
            ({"base": "main", "base_run_id": 10}, "immutable 40-hex"),
        ]
        for changes, message in mutations:
            with self.subTest(changes=changes):
                self.write_plan(**changes)
                with self.assertRaisesRegex(HookError, message):
                    self.run_hook()
                self.assertFalse(self.output.exists())
                self.assert_restored()
        self.write_plan(base=self.parent, base_run_id=10)
        with patch("ci_commit_hooks.find_qualification", return_value=None):
            with self.assertRaisesRegex(HookError, "could not be revalidated"):
                self.run_hook()
        self.assert_restored()

    def test_nonancestor_qualified_base_cannot_hide_candidate_changes(self):
        unrelated = self.git("commit-tree", self.tree, "-m", "unrelated source history")
        self.write_plan(base=unrelated, base_run_id=10)
        record = {"commit": unrelated, "run_id": 10, "receipt": {"source_commit": unrelated}}
        with patch("ci_commit_hooks.find_qualification", return_value=record):
            with self.assertRaisesRegex(HookError, "must be an ancestor"):
                self.run_hook()
        self.assertFalse(self.output.exists())
        self.assert_restored()

    def test_plan_mutation_after_hook_is_rejected_with_exact_restoration(self):
        with self.assertRaisesRegex(HookError, "plan changed"):
            self.run_hook(FIXTURE_HOOK="plan-change")
        self.assertEqual(json.loads(self.output.read_text())["status"], "failed")
        self.assert_restored()

    def test_full_knip_failure_cannot_become_a_pass(self):
        with self.assertRaisesRegex(HookError, "hook failed with status 5"):
            self.run_hook(FIXTURE_KNIP_EXIT="5")
        proof = json.loads(self.output.read_text())
        self.assertEqual(proof["status"], "failed")
        self.assertEqual(proof["hook_result"]["exit_code"], 5)
        self.assert_restored()

    def test_formatter_staging_a_changed_tree_is_rejected_and_restored(self):
        with self.assertRaisesRegex(HookError, "changed the candidate source"):
            self.run_hook(FIXTURE_HOOK="staged-change")
        proof = json.loads(self.output.read_text())
        self.assertNotEqual(proof["observed_index_tree"], self.tree)
        self.assert_restored()

    def test_unstaged_formatter_change_is_rejected_and_restored(self):
        with self.assertRaisesRegex(HookError, "changed the candidate source"):
            self.run_hook(FIXTURE_HOOK="unstaged-change")
        self.assertEqual(json.loads(self.output.read_text())["unstaged_changes"], ["candidate.txt"])
        self.assert_restored()

    def test_new_untracked_source_is_recorded_then_removed_without_touching_dependencies(self):
        with self.assertRaisesRegex(HookError, "status 9"):
            self.run_hook(FIXTURE_HOOK="new-file")
        self.assertEqual(json.loads(self.output.read_text())["new_source_files"], ["new-source.txt"])
        self.assertFalse((self.root / "new-source.txt").exists())
        self.assert_restored()

    def test_timeout_stops_owned_descendant_before_restoring_source(self):
        with patch("ci_commit_hooks.TERMINATION_GRACE_SECONDS", 0.2):
            with self.assertRaisesRegex(HookError, "timed out"):
                verify_hooks(self.root, self.output, dict(self.env, FIXTURE_HOOK="sleep"),
                             self.plan, timeout_seconds=0.5)
        proof = json.loads(self.output.read_text())
        self.assertTrue(proof["hook_result"]["timed_out"])
        child_pid = int((self.runner / "owned-child.pid").read_text())
        proc = Path(f"/proc/{child_pid}/stat")
        if proc.exists():
            self.assertEqual(proc.read_text().split(")", 1)[1].split()[0], "Z")
        self.assert_restored()

    def test_successful_exit_with_background_work_is_rejected_and_group_stopped(self):
        with self.assertRaisesRegex(HookError, "process group running"):
            self.run_hook(FIXTURE_HOOK="orphan")
        proof = json.loads(self.output.read_text())
        self.assertEqual(proof["hook_result"]["exit_code"], 0)
        self.assertTrue(proof["hook_result"]["leftover_processes"])
        child_pid = int((self.runner / "owned-child.pid").read_text())
        proc = Path(f"/proc/{child_pid}/stat")
        if proc.exists():
            self.assertEqual(proc.read_text().split(")", 1)[1].split()[0], "Z")
        self.assert_restored()

    def test_untracked_cleanup_error_does_not_skip_head_and_index_restoration(self):
        original = Path.unlink
        def refuse_created(path, *args, **kwargs):
            if path == self.root / "new-source.txt":
                raise PermissionError("owned fixture cleanup denied")
            return original(path, *args, **kwargs)
        with patch.object(Path, "unlink", refuse_created):
            with self.assertRaisesRegex(HookError, "cleanup denied"):
                self.run_hook(FIXTURE_HOOK="new-file")
        proof = json.loads(self.output.read_text())
        self.assertEqual(proof["status"], "failed")
        self.assertIn("cleanup denied", proof["restoration_error"])
        self.assertEqual(self.git("rev-parse", "HEAD"), self.head)
        self.assertEqual(self.git("write-tree"), self.tree)
        self.assertEqual((self.root / "candidate.txt").read_text(), "candidate source\n")
        self.assertTrue((self.root / "new-source.txt").exists())

    def test_log_collection_error_cannot_skip_exact_checkout_restoration(self):
        def unreadable_log(path):
            if path.name.endswith(".stdout.log"):
                raise OSError("owned log became unreadable")
            return digest(path)
        with patch("ci_commit_hooks.digest", unreadable_log):
            with self.assertRaisesRegex(HookError, "log became unreadable"):
                self.run_hook()
        proof = json.loads(self.output.read_text())
        self.assertEqual(proof["status"], "failed")
        self.assertIn("log became unreadable", proof["evidence_error"])
        self.assert_restored()

    def test_cleanup_interruption_still_attempts_the_remaining_git_restoration(self):
        for phase in ("ls-files", "read-tree"):
            with self.subTest(phase=phase):
                self.git("update-ref", "--no-deref", "HEAD", self.parent)
                interrupted = False
                def interrupt_once(root, *args, **kwargs):
                    nonlocal interrupted
                    if not interrupted and args[0] == phase:
                        interrupted = True
                        raise HookError("one-shot cleanup interruption")
                    return git(root, *args, **kwargs)
                with patch("ci_commit_hooks.git", interrupt_once):
                    with self.assertRaisesRegex(HookError, "one-shot cleanup interruption"):
                        restore(self.root, self.head, self.tree)
                self.assertTrue(interrupted)
                self.assert_restored()

    def test_cancellation_closes_group_and_restores_detached_candidate(self):
        script = Path(__file__).with_name("ci_commit_hooks.py")
        child = subprocess.Popen([sys.executable, str(script), "--output", str(self.output),
                                  "--plan", str(self.plan)],
                                 cwd=self.root, env=dict(self.env, FIXTURE_HOOK="sleep"),
                                 stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        try:
            marker = self.runner / "owned-child.pid"
            deadline = time.monotonic() + 5
            while not marker.exists() and child.poll() is None and time.monotonic() < deadline:
                time.sleep(0.02)
            self.assertTrue(marker.exists())
            child.send_signal(signal.SIGTERM)
            stdout, stderr = child.communicate(timeout=15)
            self.assertEqual(child.returncode, 1, (stdout, stderr))
            proof = json.loads(self.output.read_text())
            self.assertEqual(proof["status"], "failed")
            self.assertIn("signal 15", proof["error"])
            self.assert_restored()
        finally:
            if child.poll() is None:
                child.kill()
                child.wait()

    def test_non_actions_dirty_attached_wrong_sha_and_disabled_hooks_refuse_without_mutation(self):
        for changes, message in (({"CI": "false"}, "disposable GitHub"),
                                 ({"RUNNER_ENVIRONMENT": "self-hosted"}, "GitHub-hosted"),
                                 ({"GITHUB_SHA": "0" * 40}, "exact Actions candidate"),
                                 ({"HUSKY": "0"}, "must not be disabled"),
                                 ({"VP_GIT_HOOKS": "0"}, "must not be disabled"),
                                 ({"VITE_GIT_HOOKS": "0"}, "must not be disabled")):
            with self.subTest(changes=changes), self.assertRaisesRegex(HookError, message):
                self.run_hook(**changes)
            self.assert_restored()
        self.git("switch", "-c", "developer-branch")
        with self.assertRaisesRegex(HookError, "detached HEAD"):
            self.run_hook()
        self.assertEqual(self.git("symbolic-ref", "--short", "HEAD"), "developer-branch")
        self.git("checkout", "--detach", "-q", self.head)
        (self.root / "developer.txt").write_text("keep me\n")
        with self.assertRaisesRegex(HookError, "must be clean"):
            self.run_hook()
        self.assertEqual((self.root / "developer.txt").read_text(), "keep me\n")
        self.assertEqual(self.git("rev-parse", "HEAD"), self.head)

    def test_missing_hook_external_initializer_and_reused_evidence_refuse(self):
        entry = self.root / ".vite-hooks/_/pre-commit"
        data = entry.read_bytes()
        entry.unlink()
        with self.assertRaisesRegex(HookError, "hook is missing"):
            self.run_hook()
        entry.write_bytes(data)
        entry.chmod(0o755)
        init = self.home / ".config/husky/init.sh"
        init.parent.mkdir(parents=True)
        init.write_text("HUSKY=0\n")
        with self.assertRaisesRegex(HookError, "external hook initialization"):
            self.run_hook()
        init.unlink()
        self.run_hook()
        with self.assertRaisesRegex(HookError, "must be new"):
            self.run_hook()
        self.assert_restored()


if __name__ == "__main__":
    unittest.main()
