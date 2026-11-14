"""Reject stale or incomplete measurements without trusting an aggregate verdict."""

import copy
import hashlib
import json
import unittest

import ci_performance


def report(plan, run_id="101"):
    scenarios = []
    for spec in ci_performance.CONTRACT["scenarios"]:
        scenarios.append({
            "id": spec["id"], "phase": spec["phase"], "metric": spec["metric"],
            "status": "passed", "ratio": .8,
            "confidence_interval": [.8, .8],
            "upstream": {"median_ms": 1, "p95_ms": 1, "max_ms": 1,
                         "samples": 8 if spec["phase"] == "cold" else 96,
                         "sequential_requests_per_second": 1000},
            "native": {"median_ms": .8, "p95_ms": .8, "max_ms": .8,
                       "samples": 8 if spec["phase"] == "cold" else 96,
                       "sequential_requests_per_second": 1250},
            "pairs": [{"pair": number, "order": ci_performance.expected_orders(8, ci_performance.CONTRACT["seed"])[number],
                       "upstream_ms": 1, "native_ms": .8, "ratio": .8} for number in range(8)],
        })
    operations = [row["id"] for row in ci_performance.CONTRACT["scenarios"] if row["metric"] == "median"]
    initial = {
        "sidebar-shell": {"threads": [{"id": f"thread:{number}"} for number in range(224)], "archivedThreads": []},
        "history-window": {"projection": {"thread": {"id": "thread:performance:long"},
                                          "turnItems": [{"id": "item"}], "runs": [{"id": "run"}],
                                          "nodes": [{"id": "node"}]}},
        "message-search": {"matches": [{"id": "message"}]},
        "accent-search": {"matches": [{"id": "accent", "text": "école"}]},
        "visit-commit": {"id": "thread:performance:write"},
    }
    digests = {name: hashlib.sha256(ci_performance.canonical(value).encode()).hexdigest()
               for name, value in initial.items()}
    validation = {"initial": initial, "committed": {"id": "thread:performance:long"},
                  "reopened": {"id": "thread:performance:long"}, "rollback": "passed",
                  "cardinality": ci_performance.validation_cardinality(initial)}
    runs = [{"backend": backend, "purpose": "validation", "pair": None, "order": None, "result": validation}
            for backend in ("upstream", "native")]
    for pair, order in enumerate(ci_performance.expected_orders(8, ci_performance.CONTRACT["seed"])):
        for backend in order:
            duration = 1 if backend == "upstream" else .8
            runs.append({"backend": backend, "purpose": "measurement", "pair": pair, "order": order,
                         "result": {"samples": {name: [duration] * 12 for name in operations},
                                    "cold_first_operations_ms": {name: duration / 10 for name in operations},
                                    "service_open_ms": duration / 2, "cold_open_ms": duration,
                                    "digests": digests, "committed_writes": 16}})
    dependencies = [{"importer": "apps/server", "name": name, "version": "4.1.3",
                     "package_json_sha256": "5" * 64,
                     "installed_path": "node_modules/.pnpm/locked/node_modules/" + name}
                    for name in ("effect", "@effect/platform-node")]
    resolution = [[item[field] for field in ("importer", "name", "version", "package_json_sha256", "installed_path")]
                  for item in dependencies]
    resolution_hash = hashlib.sha256(json.dumps(resolution, ensure_ascii=False, separators=(",", ":")).encode()).hexdigest()
    return {
        "schema_version": 1, "suite": "persistence-api", "status": "passed", "mode": "qualify",
        "source_commit": plan["head"], "source_tree": plan["source_tree"],
        "source_tree_dirty": False, "source_tree_dirty_after": False, "qualifies": True,
        "policy_digest": plan["policy_digest"],
        "upstream_commit": plan["upstream_commit"], "upstream_tree": plan["upstream_tree"],
        "ci_run_id": run_id, "ci_run_attempt": "1", "native_build_run_attempt": "1",
        "compiler_route": "bluefe-rustc-via-lustr", "build_profile": "release",
        "toolchain_lock_sha256": "d" * 64, "native_manifest_sha256": "e" * 64,
        "baseline": {"commit": plan["upstream_commit"], "tree": plan["upstream_tree"],
                     "archive_sha256": "1" * 64, "dependency_lock_sha256": "2" * 64,
                     "fork_dependency_lock_sha256": "3" * 64, "sqlite_wrapper_sha256": "4" * 64,
                     "runtime_provenance": {"status": "verified", "issues": [], "package_manager": "pnpm@12.14.0",
                                            "installed_lock_sha256": "6" * 64, "modules_metadata_sha256": "7" * 64,
                                            "node_executable_sha256": "8" * 64,
                                            "external_resolution_sha256": resolution_hash, "per_workspace_resolution": True,
                                            "installed_lock_matches_application_lock": True,
                                            "external_packages_within_virtual_store": True},
                     "external_dependencies": dependencies,
                     "resolved_runtime": [{"importer": item["importer"], "name": item["name"],
                                           "expected_version": item["version"], "actual_version": item["version"],
                                           "package_json_sha256": item["package_json_sha256"],
                                           "installed_path": item["installed_path"]} for item in dependencies]},
        "executable_sha256": "f" * 64, "configuration": {
            **{key: value for key, value in ci_performance.CONTRACT.items() if key not in ("schema_version", "suite", "scenarios")},
            "phase": "all",
        },
        "correctness": {"status": "passed", "checks": ["equal outputs", "rollback", "reopen"], "result_digests": digests},
        "scenarios": scenarios, "runs": runs,
    }


def encoded_report(plan, run_id="101"):
    value = report(plan, run_id)
    raw = json.dumps(value).encode()
    return value, raw


class PerformanceEvidence(unittest.TestCase):
    def setUp(self):
        self.plan = {"head": "a" * 40, "source_tree": "b" * 40, "policy_digest": "c" * 64,
                     "upstream_commit": "d" * 40, "upstream_tree": "e" * 40}

    def validate(self, value):
        raw = json.dumps(value).encode()
        return ci_performance.validate_report(value, raw, self.plan, "101", hashlib.sha256(raw).hexdigest())

    def test_all_workloads_retain_their_ratios_in_compact_receipt(self):
        value = report(self.plan)
        result = self.validate(value)
        self.assertEqual([row["ratio"] for row in result["scenario_results"]], [.8] * len(ci_performance.CONTRACT["scenarios"]))
        self.assertTrue(ci_performance.valid_summary(result, self.plan["head"], self.plan["source_tree"], self.plan["policy_digest"]))
        self.assertNotIn("pairs", json.dumps(result))

    def test_stale_wrong_route_or_nonqualifying_results_cannot_pass(self):
        for field, replacement in [
            ("source_commit", "f" * 40), ("source_tree", "f" * 40), ("policy_digest", "a" * 64),
            ("upstream_commit", "a" * 40), ("upstream_tree", "a" * 40), ("ci_run_id", "100"), ("ci_run_attempt", "2"),
            ("source_tree_dirty", True), ("build_profile", "debug"), ("compiler_route", "native-lust"),
            ("source_tree_dirty_after", True), ("qualifies", False),
            ("mode", "diagnostic"), ("status", "inconclusive"), ("status", "regressed"),
        ]:
            with self.subTest(field=field, replacement=replacement), self.assertRaises(ValueError):
                self.validate({**report(self.plan), field: replacement})

    def test_one_bad_or_missing_case_is_not_hidden_by_the_passed_summary(self):
        mutations = [
            lambda value: value["scenarios"][0].update(status="regressed"),
            lambda value: value["scenarios"][0].update(confidence_interval=[.9, 1.01]),
            lambda value: value["scenarios"][0].update(ratio=float("nan")),
            lambda value: value["scenarios"][0]["pairs"].pop(),
            lambda value: value["scenarios"][0]["pairs"][0].update(native_ms=0),
            lambda value: value["scenarios"][0]["pairs"][0].update(pair=1),
            lambda value: value["scenarios"][0]["pairs"][0].update(ratio=.5),
            lambda value: value["scenarios"][0]["pairs"][0].update(order=["wrong"]),
            lambda value: value["scenarios"][0].update(confidence_interval=[.5, .6]),
            lambda value: value["scenarios"][1].update(metric="elapsed"),
            lambda value: value["configuration"].update(seed=123),
            lambda value: value["configuration"]["fixture"].update(messages=1),
            lambda value: value["scenarios"][0]["native"].pop("p95_ms"),
            lambda value: value["scenarios"].pop(),
            lambda value: value.update(scenarios=[]),
            lambda value: value.update(runs=[]),
            lambda value: value["runs"].pop(),
            lambda value: value["runs"][2]["result"]["samples"]["sidebar-shell"].pop(),
            lambda value: value["runs"][2]["result"]["samples"]["sidebar-shell"].__setitem__(0, 100),
            lambda value: value["runs"][2]["result"].update(cold_open_ms=50),
            lambda value: value["runs"][2]["result"].update(committed_writes=15),
            lambda value: value["runs"][0]["result"]["cardinality"].update(history_nodes=100),
            lambda value: value["runs"][0]["result"]["initial"]["message-search"].update(matches=[]),
            lambda value: value["runs"][0]["result"]["initial"]["accent-search"]["matches"][0].update(text="different"),
            lambda value: value["runs"][0]["result"]["initial"]["sidebar-shell"].update(threads=[]),
            lambda value: value["baseline"].update(commit="f" * 40),
            lambda value: value["baseline"].update(resolved_runtime=[]),
            lambda value: value["baseline"]["resolved_runtime"][0].update(actual_version="0.0.0"),
            lambda value: value["configuration"].update(pairs=7),
            lambda value: value["configuration"].update(samples=1),
            lambda value: value["correctness"].update(status="failed"),
            lambda value: value["correctness"].update(checks=[]),
        ]
        for mutation in mutations:
            value = copy.deepcopy(report(self.plan))
            mutation(value)
            with self.assertRaises(ValueError):
                self.validate(value)

    def test_artifact_hash_is_bound_to_this_job_output(self):
        value, raw = encoded_report(self.plan)
        for digest in (None, "", "a" * 64):
            with self.assertRaises(ValueError):
                ci_performance.validate_report(value, raw, self.plan, "101", digest)

    def test_unverified_installed_dependencies_cannot_qualify_from_matching_direct_versions(self):
        for field, replacement in [
            ("status", "diagnostic-unverified"), ("issues", ["external dependency outside pnpm"]),
            ("package_manager", "npm@12"), ("package_manager", "pnpm@12.13.0"),
            ("installed_lock_matches_application_lock", False),
            ("external_packages_within_virtual_store", False),
            ("per_workspace_resolution", False), ("external_resolution_sha256", None),
            ("installed_lock_sha256", None), ("modules_metadata_sha256", ""),
            ("node_executable_sha256", "not-a-digest"),
        ]:
            value = report(self.plan)
            value["baseline"]["runtime_provenance"][field] = replacement
            with self.subTest(field=field, replacement=replacement), self.assertRaises(ValueError):
                self.validate(value)
        for replacement in (None, [], {}):
            value = report(self.plan)
            value["baseline"]["runtime_provenance"] = replacement
            with self.subTest(replacement=replacement), self.assertRaises(ValueError):
                self.validate(value)

    def test_retained_dependency_resolutions_bind_the_runtime_provenance(self):
        for mutation in (
            lambda value: value["baseline"].update(external_dependencies=[]),
            lambda value: value["baseline"]["external_dependencies"].append(value["baseline"]["external_dependencies"][0]),
            lambda value: value["baseline"]["external_dependencies"][0].update(version="0.0.0"),
            lambda value: value["baseline"]["external_dependencies"][0].update(installed_path="outside/node_modules/effect"),
            lambda value: value["baseline"]["external_dependencies"][0].update(installed_path="node_modules/.pnpm/../../outside"),
            lambda value: value["baseline"]["resolved_runtime"][0].update(installed_path="node_modules/.pnpm/different"),
            lambda value: value["baseline"]["resolved_runtime"][0].update(package_json_sha256="b" * 64),
        ):
            value = report(self.plan)
            mutation(value)
            with self.assertRaises(ValueError):
                self.validate(value)

    def test_canonical_digest_uses_the_workers_json_number_and_property_order(self):
        value = {"2": 1.0, "10": 1e-7, "1": 1e-6, "é": -0.0, "a": 1e21}
        self.assertEqual(ci_performance.canonical(value),
                         '{"1":0.000001,"2":1,"10":1e-7,"a":1e+21,"é":0}')


if __name__ == "__main__":
    unittest.main()
