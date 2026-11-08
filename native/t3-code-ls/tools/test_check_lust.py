"""Fail-closed checks for the compiler proof receipt; these do not claim Lust parity."""
import importlib.util
import json
from pathlib import Path
import tempfile
import sys
import unittest
from unittest.mock import patch

sys.dont_write_bytecode = True

SPEC = importlib.util.spec_from_file_location("check_lust", Path(__file__).with_name("check-lust.py"))
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class ProofGateTests(unittest.TestCase):
    def test_only_complete_nonempty_passing_run_counts_as_proof(self):
        valid = dict(kind="summary", build="ok", total=2, passed=2,
                     failed=0, timed_out=0, ignored=0, filtered_out=0)
        self.assertEqual(MODULE.test_summary(json.dumps(valid)), valid)
        changes = [dict(total=0, passed=0), dict(passed=1, failed=1),
                   dict(passed=1, timed_out=1), dict(passed=1, ignored=1),
                   dict(filtered_out=1), dict(build="failed")]
        for change in changes:
            with self.subTest(change=change), self.assertRaises(RuntimeError):
                MODULE.test_summary(json.dumps(valid | change))
        with self.assertRaises(RuntimeError):
            MODULE.test_summary("")
        with self.assertRaises(RuntimeError):
            MODULE.test_summary(json.dumps(valid) + "\n" + json.dumps(valid))

    def test_mismatched_revision_and_dirty_source_are_rejected_before_build(self):
        with patch.object(MODULE, "git", return_value="other"):
            with self.assertRaisesRegex(RuntimeError, "revision mismatch"):
                MODULE.verify_source(Path("/compiler"), "pinned")
        with patch.object(MODULE, "git", side_effect=["pinned", " M src/main.rs"]):
            with self.assertRaisesRegex(RuntimeError, "local changes"):
                MODULE.verify_source(Path("/compiler"), "pinned")

    def test_failed_receipt_replaces_stale_success(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "receipt.json"
            MODULE.write_receipt(path, dict(status="passed"))
            MODULE.write_receipt(path, dict(status="failed", error="compiler exited 1"))
            self.assertEqual(json.loads(path.read_text())["status"], "failed")


if __name__ == "__main__":
    unittest.main()
