"""Main stays behind a fresh-head lease and verified exact-source qualification."""

import copy
from unittest.mock import Mock, patch
import unittest

import ci_plan
import ci_promotion
import ci_receipt
from test_ci_performance import encoded_report
import test_ci_plan as planner_fixtures
from test_ci_receipt import needs


class PromotionVerification(unittest.TestCase):
    def setUp(self):
        self.fixture = planner_fixtures.GitFixtureTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.tearDown)
        self.root, self.base = self.fixture.root, self.fixture.base
        self.fixture.write("crates/core/src/project_commands.rs", "pub fn qualified_change() {}\n")
        self.head = self.fixture.commit("measured candidate")
        self.plan = ci_plan.make_plan(self.root, self.head, None)
        self.receipt = ci_receipt.make_receipt(self.plan, needs(self.plan), self.head, "101", encoded_report(self.plan))
        self.qualified = {"commit": self.head, "run_id": 101, "reused": True, "receipt": self.receipt}
        self.client = Mock()
        self.client.json.return_value = self.reference(self.base)

    @staticmethod
    def reference(sha):
        return {"ref": "refs/heads/main", "object": {"type": "commit", "sha": sha}}

    def verify(self, qualification=None):
        with patch("ci_promotion.GitHub", return_value=self.client), patch(
                "ci_promotion.find_qualification", return_value=self.qualified if qualification is None else qualification):
            return ci_promotion.verify_promotion(self.root, "glopyglerky/t3code-ls", self.head, None, self.base)

    def test_verified_candidate_emits_only_a_nonforced_compare_and_set_request(self):
        result = self.verify()
        self.assertEqual(result["update_ref"], {
            "repository_full_name": "glopyglerky/t3code-ls", "branch_name": "main",
            "sha": self.head, "expected_sha": self.base, "force": False,
        })
        self.assertEqual(self.client.json.call_count, 2)

    def test_missing_stale_or_failed_evidence_cannot_promote(self):
        for value in [{}, {**self.qualified, "commit": self.base},
                      {**self.qualified, "reused": False}]:
            with self.subTest(value=value), self.assertRaises(ci_promotion.PromotionError):
                self.verify(value)
        value = copy.deepcopy(self.qualified)
        value["receipt"]["performance_evidence"] = None
        with self.assertRaises(ci_promotion.PromotionError):
            self.verify(value)

    def test_moved_main_and_untracked_source_require_reverification(self):
        self.client.json.side_effect = [self.reference(self.base), self.reference(self.head)]
        with self.assertRaisesRegex(ci_promotion.PromotionError, "advanced"):
            self.verify()
        self.client.json.side_effect = None
        self.fixture.write("unqualified.rs", "pub fn extra_module() {}\n")
        with self.assertRaisesRegex(ci_promotion.PromotionError, "untracked"):
            self.verify()


if __name__ == "__main__":
    unittest.main()
