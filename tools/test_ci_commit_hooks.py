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

from ci_commit_hooks import HookError, digest, git, restore, verify_hooks


HOOK = '''#!/usr/bin/env python3
import os,pathlib,subprocess,sys,time
root=pathlib.Path.cwd()
mode=os.environ.get("FIXTURE_HOOK", "pass")
print("normal pre-commit hook", flush=True)
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
        self.git("add", ".")
        self.git("commit", "-qm", "fixture base")
        self.parent = self.git("rev-parse", "HEAD")
        (self.root / "candidate.txt").write_text("candidate source\n")
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

    def git(self, *args):
        return subprocess.run(["git", "-C", str(self.root), *args], env=self.env,
                              check=True, capture_output=True, text=True).stdout.strip()

    def run_hook(self, **changes):
        return verify_hooks(self.root, self.output, dict(self.env, **changes))

    def assert_restored(self):
        self.assertEqual(self.git("rev-parse", "HEAD"), self.head)
        self.assertEqual(self.git("write-tree"), self.tree)
        self.assertEqual(self.git("status", "--porcelain", "--untracked-files=all"), "")
        self.assertEqual((self.root / "candidate.txt").read_text(), "candidate source\n")
        self.assertTrue((self.root / "node_modules/.bin/vp").is_file())
        self.assertEqual(subprocess.run(["git", "-C", str(self.root), "symbolic-ref", "-q", "HEAD"],
                                       capture_output=True).returncode, 1)

    def test_normal_hook_and_both_full_scans_preserve_exact_candidate(self):
        proof = self.run_hook()
        self.assertEqual(proof["status"], "passed")
        self.assertEqual(proof["source_commit"], self.head)
        self.assertEqual(proof["source_tree"], self.tree)
        self.assertEqual(proof["first_parent"], self.parent)
        self.assertEqual(proof["changed_paths"], ["candidate.txt"])
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
                verify_hooks(self.root, self.output, dict(self.env, FIXTURE_HOOK="sleep"), timeout_seconds=0.5)
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
        child = subprocess.Popen([sys.executable, str(script), "--output", str(self.output)],
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
