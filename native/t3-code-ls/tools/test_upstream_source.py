"""The differential reference must remain the pinned upstream Git object."""

import json
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


class UpstreamSourceTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.git("init", "--quiet")
        self.git("config", "user.name", "Reference test")
        self.git("config", "user.email", "reference@example.invalid")
        self.source = self.root / "packages/contracts/src/auth.ts"
        self.source.parent.mkdir(parents=True)
        self.source.write_text('export const value = "upstream";\n')
        self.git("add", ".")
        self.git("commit", "--quiet", "-m", "upstream")
        self.commit = self.git("rev-parse", "HEAD")
        self.tree = self.git("rev-parse", "HEAD^{tree}")
        self.native = self.root / "native/t3-code-ls"
        (self.native / "tools").mkdir(parents=True)
        self.helper = self.native / "tools/upstream-source.mjs"
        shutil.copyfile(Path(__file__).with_name("upstream-source.mjs"), self.helper)
        self.write_lock(self.commit, self.tree)

    def git(self, *arguments):
        return subprocess.check_output(
            ["git", "-C", str(self.root), *arguments], text=True,
            stderr=subprocess.DEVNULL,
        ).strip()

    def write_lock(self, commit, tree):
        (self.native / "upstream.lock.json").write_text(json.dumps({
            "upstream": {
                "repository": "pingdotgg/t3code", "commit": commit, "root_tree": tree,
            },
        }))

    def load(self, path="packages/contracts/src/auth.ts"):
        code = (
            f"import {{ upstreamSource }} from {json.dumps(self.helper.as_uri())};"
            f"process.stdout.write(upstreamSource({json.dumps(path)}));"
        )
        return subprocess.run(
            ["node", "--input-type=module", "-e", code],
            capture_output=True, text=True, timeout=30,
        )

    def test_worktree_edits_and_git_replacements_cannot_change_the_reference(self):
        self.source.write_text('export const value = "fork";\n')
        result = self.load()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, 'export const value = "upstream";\n')
        self.git("add", "packages")
        self.git("commit", "--quiet", "-m", "fork")
        replacement = self.git("rev-parse", "HEAD")
        self.git("replace", self.commit, replacement)
        result = self.load()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, 'export const value = "upstream";\n')

    def test_mutable_missing_and_inconsistent_pins_fail(self):
        for commit, tree in [("HEAD", self.tree), ("a" * 40, self.tree),
                             (self.commit, "b" * 40)]:
            with self.subTest(commit=commit, tree=tree):
                self.write_lock(commit, tree)
                self.assertNotEqual(self.load().returncode, 0)

    def test_paths_cannot_escape_the_pinned_source_tree(self):
        for path in ["../auth.ts", "/auth.ts", "HEAD:auth.ts", "--help"]:
            with self.subTest(path=path):
                self.assertNotEqual(self.load(path).returncode, 0)


if __name__ == "__main__":
    unittest.main()
