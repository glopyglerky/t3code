"""Check byte preservation and failure behavior for upstream README synchronization."""

import importlib.util
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location("sync_readme", Path(__file__).with_name("sync-readme.py"))
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def git(repo, *arguments):
    return subprocess.check_output(["git", "-C", str(repo), *arguments], stderr=subprocess.DEVNULL).strip()


class ReadmeSynchronization(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.repo = Path(self.temporary.name)
        git(self.repo, "init", "--quiet")
        git(self.repo, "config", "user.name", "README test")
        git(self.repo, "config", "user.email", "readme-test@example.invalid")
        self.upstream_readme = self.repo / "README.md"
        self.readme = self.repo / "native/t3-code-ls/upstream-README.md"
        self.lock_path = self.repo / "native/t3-code-ls/upstream.lock.json"
        self.lock_path.parent.mkdir(parents=True)

    def pin(self, source):
        self.upstream_readme.write_bytes(source)
        git(self.repo, "add", "README.md")
        git(self.repo, "commit", "--quiet", "-m", "upstream fixture")
        self.commit = git(self.repo, "rev-parse", "HEAD").decode()
        self.tree = git(self.repo, "rev-parse", "HEAD^{tree}").decode()
        self.lock = {"upstream": {"repository": "pingdotgg/t3code", "commit": self.commit,
                                  "root_tree": self.tree}}
        self.write_lock()
        self.prefix = b"# Fork heading\r\n\r\n" + MODULE.BEGIN
        self.suffix = MODULE.END + b"\n\nFork additions, unchanged.\r\n"
        self.readme.write_bytes(self.prefix + source + self.suffix)

    def write_lock(self):
        self.lock_path.write_text(json.dumps(self.lock))

    def test_restores_exact_bytes_without_altering_fork_content_and_is_idempotent(self):
        for source in (b"# Upstream\r\n\r\nOriginal.\r\n", b"no final newline", b"\xef\xbb\xbf# Upstream\n", b""):
            with self.subTest(source=source):
                self.pin(source)
                self.readme.chmod(0o640)
                edited = self.prefix + b"changed upstream text\n" + self.suffix
                self.readme.write_bytes(edited)
                with self.assertRaisesRegex(MODULE.ReadmeError, "differs"):
                    MODULE.sync_readme(self.repo)
                self.assertEqual(self.readme.read_bytes(), edited)
                result = MODULE.sync_readme(self.repo, write=True)
                self.assertEqual(result["status"], "updated")
                self.assertEqual(self.upstream_readme.read_bytes(), source)
                self.assertEqual(self.readme.read_bytes(), self.prefix + source + self.suffix)
                self.assertEqual(self.readme.stat().st_mode & 0o777, 0o640)
                before = self.readme.stat().st_mtime_ns
                self.assertEqual(MODULE.sync_readme(self.repo, write=True)["status"], "matched")
                self.assertEqual(self.readme.stat().st_mtime_ns, before)

    def test_rejects_ambiguous_or_reversed_boundaries_without_writing(self):
        self.pin(b"upstream")
        valid = self.readme.read_bytes()
        documents = [b"unmarked", valid + MODULE.BEGIN, valid + MODULE.END,
                     MODULE.END + b"text" + MODULE.BEGIN]
        for document in documents:
            with self.subTest(document=document):
                self.readme.write_bytes(document)
                with self.assertRaises(MODULE.ReadmeError):
                    MODULE.sync_readme(self.repo, write=True)
                self.assertEqual(self.readme.read_bytes(), document)

    def test_source_is_the_exact_commit_even_with_local_replacements(self):
        source = b"original pinned source\n"
        self.pin(source)
        pinned_document = self.readme.read_bytes()
        self.upstream_readme.write_bytes(b"new upstream source\n")
        git(self.repo, "add", "README.md")
        git(self.repo, "commit", "--quiet", "-m", "different source")
        new_commit = git(self.repo, "rev-parse", "HEAD").decode()
        git(self.repo, "replace", self.commit, new_commit)
        self.readme.write_bytes(pinned_document)
        self.assertEqual(MODULE.source_readme(self.repo), (self.commit, source))
        self.assertEqual(MODULE.sync_readme(self.repo)["status"], "matched")

    def test_rejects_mutable_missing_and_inconsistent_source_pins(self):
        self.pin(b"upstream\n")
        original = self.readme.read_bytes()
        cases = [dict(commit="HEAD"), dict(commit="a" * 40), dict(root_tree="b" * 40),
                 dict(commit=self.tree), dict(repository="another/repository")]
        baseline = self.lock["upstream"].copy()
        for case in cases:
            with self.subTest(case=case):
                self.lock["upstream"] = baseline | case
                self.write_lock()
                with self.assertRaises(MODULE.ReadmeError):
                    MODULE.sync_readme(self.repo, write=True)
                self.assertEqual(self.readme.read_bytes(), original)

    def test_never_overwrites_a_symlink_or_an_observed_concurrent_edit(self):
        self.pin(b"upstream\n")
        original = self.readme.read_bytes()
        target = self.repo / "other.md"
        target.write_bytes(original)
        self.readme.unlink()
        self.readme.symlink_to(target)
        with self.assertRaisesRegex(MODULE.ReadmeError, "regular file"):
            MODULE.sync_readme(self.repo, write=True)
        self.assertEqual(target.read_bytes(), original)
        self.readme.unlink()
        self.readme.write_bytes(self.prefix + b"stale source" + self.suffix)
        concurrent_edit = original + b"Another writer's additions.\n"
        with patch.object(MODULE.os, "fsync", side_effect=lambda _: self.readme.write_bytes(concurrent_edit)):
            with self.assertRaisesRegex(MODULE.ReadmeError, "changed during"):
                MODULE.sync_readme(self.repo, write=True)
        self.assertEqual(self.readme.read_bytes(), concurrent_edit)
        self.assertEqual(list(self.repo.glob(".readme-*")), [])


if __name__ == "__main__":
    unittest.main()
