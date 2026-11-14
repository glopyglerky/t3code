"""Artifact wiring must survive both all-job and failed-job reruns."""

from pathlib import Path
import re
import unittest


ROOT = Path(__file__).resolve().parent.parent


def artifact_names(workflow: str, action: str) -> list[str]:
    names = []
    for step in re.split(r"(?m)^      - ", workflow):
        if f"uses: actions/{action}-artifact@" not in step:
            continue
        match = re.search(r"(?m)^          name: (.+)$", step)
        if match is None:
            raise AssertionError(f"{action} step has no exact artifact name")
        names.append(match[1])
    return names


def resolve(name: str, attempt: int, plan: int, native: int, performance: int) -> str:
    values = {
        "github.run_attempt": str(attempt),
        "steps.plan.outputs.artifact_name": f"ci-plan-{attempt}",
        "steps.scope.outputs.native_artifact": f"hybrid-native-binaries-{attempt}",
        "needs.plan.outputs.plan_artifact": f"ci-plan-{plan}",
        "needs.native.outputs.native_artifact": f"hybrid-native-binaries-{native}",
        "needs.performance.outputs.proof_run_attempt": str(performance),
    }
    return re.sub(r"\$\{\{ ([^}]+) \}\}", lambda match: values[match[1]], name)


class WorkflowArtifactWiring(unittest.TestCase):
    def setUp(self):
        self.hybrid = (ROOT / ".github/workflows/hybrid.yml").read_text()
        self.native = (ROOT / ".github/workflows/native.yml").read_text()

    def test_rerun_all_keeps_every_prior_plan_binary_and_failure_artifact(self):
        uploads = artifact_names(self.hybrid, "upload") + artifact_names(self.native, "upload")
        self.assertEqual(len(uploads), 8)
        attempts = [{resolve(name, attempt, attempt, attempt, attempt) for name in uploads}
                    for attempt in (1, 2)]
        self.assertEqual(len(attempts[0]), len(uploads))
        self.assertTrue(attempts[0].isdisjoint(attempts[1]))
        self.assertNotRegex(self.hybrid + self.native, r"overwrite:\s*true")

    def test_rerun_failed_consumers_select_the_successful_producer_artifacts(self):
        downloads = artifact_names(self.hybrid, "download")
        names = [resolve(name, 2, 1, 1, 1) for name in downloads]
        self.assertEqual(names.count("hybrid-native-binaries-1"), 3)
        self.assertEqual(names.count("ci-plan-1"), 2)
        self.assertEqual(names.count("hybrid-performance-proof-1"), 1)
        self.assertEqual(len(names), 6)
        names = [resolve(name, 2, 1, 1, 2) for name in downloads]
        self.assertIn("hybrid-performance-proof-2", names)
        self.assertNotIn("hybrid-native-binaries-2", names)

    def test_producer_outputs_are_exported_through_the_reusable_workflow(self):
        self.assertIn('echo "artifact_name=ci-plan-$GITHUB_RUN_ATTEMPT" >> "$GITHUB_OUTPUT"', self.hybrid)
        self.assertIn("plan_artifact: ${{ steps.plan.outputs.artifact_name }}", self.hybrid)
        self.assertIn('echo "native_artifact=hybrid-native-binaries-$GITHUB_RUN_ATTEMPT" >> "$GITHUB_OUTPUT"', self.native)
        self.assertIn("native_artifact: ${{ steps.scope.outputs.native_artifact }}", self.native)
        self.assertIn("value: ${{ jobs.qualify.outputs.native_artifact }}", self.native)
        self.assertIn("proof_run_attempt: ${{ steps.proof.outputs.proof_run_attempt }}", self.hybrid)


if __name__ == "__main__":
    unittest.main()
