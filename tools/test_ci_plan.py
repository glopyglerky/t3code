"""Regression coverage for qualification selection, without building upstream."""

from __future__ import annotations

import copy
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import ci_plan


NAMES = {
    "crates/core": "t3-code-ls-core",
    "crates/storage": "t3-code-ls-storage",
    "crates/transport": "t3-code-ls-transport",
    "crates/app": "t3-code-ls-app",
    "tools": "t3code-ls-tools",
}
REVERSE = {
    "t3-code-ls-core": {"t3-code-ls-storage", "t3-code-ls-transport", "t3-code-ls-app"},
    "t3-code-ls-storage": {"t3-code-ls-app"},
    "t3-code-ls-transport": set(),
    "t3-code-ls-app": set(),
    "t3code-ls-tools": set(),
}


def port(identifier, rust_path, adapter=None, crates=None, test=None):
    return {
        "id": identifier,
        "rust_paths": [rust_path],
        "rust_crates": crates or ["crates/core"],
        "adapter_paths": [adapter] if adapter else [],
        "acceptance_tests": [test] if test else [],
        "upstream_sources": [],
        "retired_paths": [],
    }


MAPPING = {"ports": [
    port("project-commands", "crates/core/src/project_commands.rs",
         "apps/server/src/orchestration-v2/NativeProjectCommands.ts",
         test="tools/check-project-adapter.mjs"),
    port("command-and-dispatch-policy", "crates/core/src/command_policy.rs",
         "apps/server/src/orchestration-v2/NativeCommandPolicy.ts",
         test="tools/check-policy-adapter.mjs"),
    port("redb-persistence", "crates/storage/src/lib.rs", crates=["crates/core", "crates/storage"]),
    port("iroh-transport", "crates/transport/src/bridge.rs", crates=["crates/core", "crates/transport"]),
    port("native-api-component", "crates/app/src/http.rs", crates=["crates/core", "crates/storage", "crates/app"]),
]}
MAPPING["ports"][0]["acceptance_tests"].append("tools/check-project-commands.mjs")
MAPPING["ports"][3]["acceptance_tests"].append("tools/check-connection.mjs")
COMPONENT_MANIFEST = {
    "tools/check-project-commands.mjs": {
        "package": "t3-code-ls-core", "executable": "examples/project_commands_matrix",
        "extra_arguments": [], "proof_argument": False,
    },
    "tools/check-connection.mjs": {
        "package": "t3-code-ls-core", "executable": "examples/connection_matrix",
        "extra_arguments": [], "proof_argument": False,
    },
}


class SelectionTests(unittest.TestCase):
    def select(self, paths, trusted=(), mapping=None, old=None, full_reason=None, manifest=None):
        return ci_plan.select_checks(
            paths, copy.deepcopy(mapping or MAPPING), copy.deepcopy(old or MAPPING),
            NAMES, REVERSE, set(trusted), full_reason,
            copy.deepcopy(COMPONENT_MANIFEST if manifest is None else manifest),
        )

    def assert_light(self, result):
        self.assertEqual(result["mode"], "affected-fork")
        self.assertEqual(result["gates"], {"native": False, "adapters": False, "application": False})
        self.assertEqual(result["rust_packages"], [])
        self.assertEqual(result["component_tests"], [])
        self.assertFalse(result["stage_native"])
        self.assertFalse(result["upstream_tests"])

    def test_documentation_and_standalone_reporting_changes_skip_compilers_and_application(self):
        self.assert_light(self.select(["docs/porting/strategy.md", "README.md"]))
        self.assert_light(self.select(["tools/plan-porting.py", "tools/test_nightly.py"]))

    def test_rust_tools_change_runs_tools_and_source_guards_only(self):
        result = self.select(["tools/src/migration.rs"])
        self.assertEqual(result["mode"], "affected-fork")
        self.assertEqual(result["rust_packages"], ["t3code-ls-tools"])
        self.assertEqual(result["gates"], {"native": True, "adapters": False, "application": False})
        self.assertTrue(result["source_guards"])
        self.assertFalse(result["stage_native"])

    def test_storage_candidate_tests_reverse_consumer_without_production_application(self):
        result = self.select(["crates/storage/src/lib.rs"])
        self.assertEqual(result["mode"], "affected-fork")
        self.assertEqual(set(result["rust_packages"]), {"t3-code-ls-storage", "t3-code-ls-app", "t3code-ls-tools"})
        self.assertEqual(result["adapter_ports"], [])
        self.assertFalse(result["gates"]["application"])
        self.assertFalse(result["stage_native"])

    def test_transport_candidate_does_not_build_unrelated_storage_or_application(self):
        result = self.select(["crates/transport/src/bridge.rs"])
        self.assertEqual(set(result["rust_packages"]), {"t3-code-ls-transport", "t3code-ls-tools"})
        self.assertFalse(result["gates"]["application"])
        self.assertEqual(result["adapter_ports"], [])

    def test_owned_core_module_selects_its_port_and_all_rust_reverse_consumers(self):
        result = self.select(["crates/core/src/project_commands.rs"])
        self.assertEqual(result["mode"], "affected-fork")
        self.assertEqual(set(result["rust_packages"]), set(NAMES.values()))
        self.assertEqual(result["adapter_ports"], ["project-commands"])
        self.assertEqual(set(result["affected_ports"]), {
            "project-commands", "redb-persistence", "iroh-transport", "native-api-component",
        })
        self.assertTrue(result["gates"]["application"])
        self.assertTrue(result["stage_native"])

    def test_shared_core_decoder_fans_out_to_every_dependent_port(self):
        result = self.select(["crates/core/src/json.rs"])
        self.assertEqual(result["mode"], "affected-fork")
        self.assertEqual(set(result["affected_ports"]), {entry["id"] for entry in MAPPING["ports"]})
        self.assertEqual(result["adapter_ports"], ["command-and-dispatch-policy", "project-commands"])
        self.assertTrue(result["gates"]["application"])

    def test_port_test_changes_select_its_adapter_without_browser_flow(self):
        result = self.select(["tools/check-project-adapter.mjs"])
        self.assertEqual(result["adapter_ports"], ["project-commands"])
        self.assertEqual(set(result["rust_packages"]), {"t3-code-ls-core", "t3code-ls-tools"})
        self.assertEqual(result["component_tests"], [])
        self.assertTrue(result["stage_native"])
        self.assertFalse(result["gates"]["application"])

    def test_changed_project_matrix_is_run_beside_its_adapter_without_browser_flow(self):
        path = "tools/check-project-commands.mjs"
        result = self.select([path])
        self.assertEqual(result["component_tests"], [path])
        self.assertEqual(result["adapter_ports"], ["project-commands"])
        self.assertEqual(set(result["rust_packages"]), {"t3-code-ls-core", "t3code-ls-tools"})
        self.assertFalse(result["gates"]["application"])

    def test_changed_candidate_connection_script_gets_an_executable_gate(self):
        path = "tools/check-connection.mjs"
        result = self.select([path])
        self.assertEqual(result["component_tests"], [path])
        self.assertEqual(result["adapter_ports"], [])
        self.assertEqual(set(result["rust_packages"]), {
            "t3-code-ls-core", "t3-code-ls-transport", "t3code-ls-tools",
        })
        self.assertEqual(result["gates"], {"native": True, "adapters": False, "application": False})
        self.assertFalse(result["stage_native"])

    def test_unknown_mapped_acceptance_tests_cannot_silently_pass_any_plan(self):
        mapping = copy.deepcopy(MAPPING)
        path = "tools/check-new-acceptance.mjs"
        mapping["ports"][0]["acceptance_tests"].append(path)
        for changed in [[], ["docs/notes.md"], [path]]:
            with self.subTest(changed=changed):
                with self.assertRaisesRegex(ValueError, "needs an execution mapping.*check-new-acceptance"):
                    self.select(changed, mapping=mapping)

    def test_component_execution_mapping_must_name_a_known_rust_package(self):
        manifest = copy.deepcopy(COMPONENT_MANIFEST)
        manifest["tools/check-connection.mjs"]["package"] = "unknown-package"
        with self.assertRaisesRegex(ValueError, "unknown Rust package execution mapping"):
            self.select(["tools/check-connection.mjs"], manifest=manifest)

    def test_component_test_selection_is_exposed_as_json_github_output(self):
        result = self.select(["tools/check-connection.mjs"])
        with tempfile.TemporaryDirectory(prefix="t3-ci-output-") as temporary:
            root = Path(temporary)
            ci_plan.write_plan(result, root / "plan.json", root / "github-output")
            values = dict(line.split("=", 1) for line in (root / "github-output").read_text().splitlines())
        self.assertEqual(json.loads(values["component_tests"]), ["tools/check-connection.mjs"])

    def test_upstream_provenance_change_runs_source_guards_without_retesting_the_application(self):
        for path in ["upstream.lock.json", "upstream-README.md"]:
            with self.subTest(path=path):
                result = self.select([path])
                self.assertEqual(result["mode"], "affected-fork")
                self.assertEqual(result["rust_packages"], ["t3code-ls-tools"])
                self.assertTrue(result["source_guards"])
                self.assertFalse(result["stage_native"])
                self.assertFalse(result["gates"]["application"])

    def test_acceptance_receipts_in_current_or_previous_mapping_are_source_guard_inputs(self):
        path = "docs/qualification/project-linux.json"
        recorded = copy.deepcopy(MAPPING)
        recorded["ports"][0]["platform_routes"] = [{
            "acceptance_evidence": {"linux-x64": path},
        }]
        for current, old in [(recorded, MAPPING), (MAPPING, recorded)]:
            with self.subTest(current_has_receipt=current is recorded):
                result = self.select([path], mapping=current, old=old)
                self.assertEqual(result["mode"], "affected-fork")
                self.assertEqual(result["rust_packages"], ["t3code-ls-tools"])
                self.assertTrue(result["source_guards"])
                self.assertFalse(result["stage_native"])
                self.assertEqual(result["gates"], {"native": True, "adapters": False, "application": False})

    def test_adapter_typecheck_configuration_is_not_swallowed_by_tools_cargo_member(self):
        result = self.select(["tools/project-tsconfig.json"])
        self.assertEqual(set(result["adapter_ports"]), ci_plan.PRODUCTION_PORTS)
        self.assertTrue(result["stage_native"])
        self.assertFalse(result["gates"]["application"])

    def test_rust_test_only_change_does_not_change_production_execution(self):
        result = self.select(["crates/core/tests/command_policy.rs"])
        self.assertEqual(result["mode"], "affected-fork")
        self.assertEqual(set(result["rust_packages"]), {"t3-code-ls-core", "t3code-ls-tools"})
        self.assertEqual(result["adapter_ports"], [])
        self.assertFalse(result["gates"]["application"])

    def test_shared_metadata_ci_and_locks_require_full_fork_qualification(self):
        for path in ["port-map.json", "Cargo.lock", "Cargo.toml", "pnpm-lock.yaml",
                     "toolchain.lock.json", "tools/ci_plan.py", ".github/workflows/hybrid.yml",
                     "tools/nightly.py", "tools/ci_receipt.py", "tools/test-rust.py",
                     "tools/acceptance-checks.json", "tools/check-acceptance.mjs",
                     ".cargo/config.toml"]:
            with self.subTest(path=path):
                result = self.select([path])
                self.assertEqual(result["mode"], "full-fork")
                self.assertEqual(set(result["rust_packages"]), set(NAMES.values()))
                self.assertTrue(all(result["gates"].values()))
                self.assertFalse(result["upstream_tests"])

    def test_unknown_qualification_helpers_cannot_escape_in_the_reporting_directory(self):
        for path in ["tools/upstream-source.mjs", "tools/new-oracle.py", "tools/oracles/new.ts"]:
            with self.subTest(path=path):
                result = self.select([path])
                self.assertEqual(result["mode"], "full-fork")
                self.assertTrue(all(result["gates"].values()))

    def test_tools_rust_inputs_are_scoped_without_ignoring_rust_fixtures(self):
        for path in ["tools/src/main.rs", "tools/tests/guard.rs", "tools/benches/check.rs",
                     "tools/fixtures/recovery.json", "tools/Cargo.toml", "tools/build.rs"]:
            with self.subTest(path=path):
                result = self.select([path])
                self.assertEqual(result["mode"], "affected-fork")
                self.assertEqual(result["rust_packages"], ["t3code-ls-tools"])
                self.assertFalse(result["gates"]["application"])

    def test_unmapped_application_change_or_deletion_fails_to_full_fork(self):
        result = self.select(["apps/server/src/deleted-unmapped-service.ts"])
        self.assertEqual(result["mode"], "full-fork")
        self.assertTrue(result["gates"]["application"])

    def test_pristine_upstream_implementation_needs_no_requalification(self):
        path = "apps/web/src/TrustedUpstreamView.tsx"
        result = self.select([path], trusted=[path])
        self.assert_light(result)
        self.assertEqual(result["trusted_upstream_paths"], [path])

    def test_trusted_source_reason_is_aggregated_for_large_upstream_imports(self):
        paths = [f"apps/web/src/view-{index}.tsx" for index in range(500)]
        result = self.select(paths, trusted=paths)
        self.assert_light(result)
        self.assertEqual(len(result["reasons"]), 1)
        self.assertIn("500 changed files", result["reasons"][0])

    def test_trusted_contract_changes_still_require_review_of_rust_consumers(self):
        path = "packages/contracts/src/orchestrationV2.ts"
        result = self.select([path], trusted=[path])
        self.assertEqual(set(result["affected_ports"]), {entry["id"] for entry in MAPPING["ports"]})
        self.assertEqual(set(result["rust_packages"]), set(NAMES.values()))
        self.assertEqual(result["adapter_ports"], ["command-and-dispatch-policy", "project-commands"])
        self.assertTrue(result["gates"]["application"])
        self.assertFalse(result["upstream_tests"])

    def test_trusted_candidate_source_runs_owning_rust_crates_and_their_consumers(self):
        path = "apps/server/src/orchestration-v2/EventSink.ts"
        mapping = copy.deepcopy(MAPPING)
        mapping["ports"][2]["upstream_sources"] = [{"path": path}]
        result = self.select([path], trusted=[path], mapping=mapping, old=mapping)
        self.assertEqual(result["mode"], "affected-fork")
        self.assertEqual(set(result["rust_packages"]), set(NAMES.values()))
        self.assertEqual(set(result["affected_ports"]), {
            "redb-persistence", "iroh-transport", "native-api-component",
        })
        self.assertEqual(result["adapter_ports"], [])
        self.assertFalse(result["gates"]["application"])
        self.assertFalse(result["stage_native"])

    def test_reverse_dependency_owned_by_production_is_qualified_without_other_shared_crate_ports(self):
        mapping = copy.deepcopy(MAPPING)
        mapping["ports"].append(port("guest-share-decisions", "crates/app/src/guest.rs", crates=["crates/app"]))
        result = self.select(["crates/storage/src/lib.rs"], mapping=mapping, old=mapping)
        self.assertEqual(set(result["affected_ports"]), {
            "redb-persistence", "native-api-component", "guest-share-decisions",
        })
        self.assertEqual(result["adapter_ports"], ["guest-share-decisions"])
        self.assertTrue(result["gates"]["application"])

    def test_unknown_or_removed_rust_owners_require_full_qualification(self):
        path = "apps/server/src/orchestration-v2/RemovedSource.ts"
        for crates in [[], ["crates/removed"]]:
            with self.subTest(crates=crates):
                old = copy.deepcopy(MAPPING)
                old["ports"][2]["upstream_sources"] = [{"path": path}]
                old["ports"][2]["rust_crates"] = crates
                result = self.select([path], trusted=[path], old=old)
                self.assertEqual(result["mode"], "full-fork")
                self.assertTrue(all(result["gates"].values()))

    def test_removed_mapping_cannot_erase_previous_port_ownership(self):
        old = copy.deepcopy(MAPPING)
        old_path = "apps/server/src/orchestration-v2/OldProjectAdapter.ts"
        old["ports"][0]["adapter_paths"].append(old_path)
        result = self.select([old_path], old=old)
        self.assertEqual(result["adapter_ports"], ["project-commands"])
        self.assertTrue(result["gates"]["application"])

    def test_explicit_full_reason_overrides_an_empty_diff(self):
        result = self.select([], full_reason="Requested full fork qualification")
        self.assertEqual(result["mode"], "full-fork")
        self.assertTrue(all(result["gates"].values()))
        self.assertEqual(result["component_tests"], sorted(COMPONENT_MANIFEST))

    def test_dependency_closure_handles_transitive_edges_and_cycles(self):
        graph = {"core": {"storage"}, "storage": {"app"}, "app": {"storage"}, "tools": set()}
        self.assertEqual(ci_plan.closure({"core"}, graph), {"core", "storage", "app"})
        self.assertEqual(ci_plan.closure({"tools"}, graph), {"tools"})


class GitFixtureTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix="t3-ci-plan-")
        self.root = Path(self.directory.name)
        self.run_git("init", "--quiet")
        self.write("Cargo.toml", '[workspace]\nmembers = ["crates/core", "crates/storage", "crates/transport", "crates/app", "tools"]\n'
                   '[workspace.dependencies]\nshared-core = { package = "t3-code-ls-core", path = "crates/core" }\n')
        for member, name in NAMES.items():
            self.write(f"{member}/Cargo.toml", f'[package]\nname = "{name}"\nversion = "0.1.0"\n')
        self.write("crates/storage/Cargo.toml", '[package]\nname = "t3-code-ls-storage"\nversion = "0.1.0"\n'
                   '[target.\'cfg(unix)\'.dependencies]\nshared-core = { workspace = true }\n')
        self.write("crates/transport/Cargo.toml", '[package]\nname = "t3-code-ls-transport"\nversion = "0.1.0"\n'
                   '[dev-dependencies]\ncore-test = { package = "t3-code-ls-core", path = "../core" }\n')
        self.write("crates/app/Cargo.toml", '[package]\nname = "t3-code-ls-app"\nversion = "0.1.0"\n'
                   '[build-dependencies]\nstore = { package = "t3-code-ls-storage", path = "../storage" }\n')
        self.write("crates/core/src/project_commands.rs", "pub fn original() {}\n")
        self.write("apps/web/src/TrustedUpstreamView.tsx", "export const label = 'upstream';\n")
        self.write("apps/server/src/Orchestrator.ts", "export const backend = 'upstream';\n")
        self.write("apps/server/package.json", '{"name":"upstream-server"}\n')
        self.write("port-map.json", json.dumps(MAPPING))
        self.write("tools/acceptance-checks.json", json.dumps(COMPONENT_MANIFEST))
        self.upstream = self.commit("upstream fixture")
        self.write("upstream.lock.json", json.dumps({"upstream": {"commit": self.upstream}}))
        self.base = self.commit("qualified fork fixture")

    def tearDown(self):
        self.directory.cleanup()

    def run_git(self, *args):
        return subprocess.run(["git", "-C", str(self.root), *args], check=True,
                              capture_output=True, text=True).stdout.strip()

    def write(self, path, content):
        target = self.root / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content)

    def commit(self, message):
        self.run_git("add", "--all")
        self.run_git("-c", "user.name=CI planner fixture", "-c", "user.email=ci-fixture@example.invalid",
                     "-c", "commit.gpgSign=false", "commit", "--quiet", "-m", message)
        return self.run_git("rev-parse", "HEAD")

    def qualification(self):
        # Production callers receive this only from ci_history's verified receipt lookup.
        return {"commit": self.base, "run_id": 123, "reused": False}

    def test_cargo_graph_reads_target_build_dev_and_workspace_path_dependencies(self):
        names, reverse = ci_plan.cargo_graph(self.root, self.base)
        self.assertEqual(names, NAMES)
        self.assertEqual(reverse["t3-code-ls-core"], {"t3-code-ls-storage", "t3-code-ls-transport"})
        self.assertEqual(reverse["t3-code-ls-storage"], {"t3-code-ls-app"})
        self.assertEqual(ci_plan.closure({"t3-code-ls-core"}, reverse), set(NAMES.values()) - {"t3code-ls-tools"})

    def test_unknown_local_cargo_dependency_cannot_silently_drop_a_consumer(self):
        manifest = self.root / "crates/app/Cargo.toml"
        manifest.write_text(manifest.read_text() + '\n[dependencies]\nmissing = { path = "../missing" }\n')
        head = self.commit("unmapped local dependency")
        with self.assertRaisesRegex(ValueError, "Unmapped local Cargo dependency"):
            ci_plan.cargo_graph(self.root, head)

    def test_unqualified_push_predecessor_does_not_hide_earlier_rust_changes(self):
        self.write("crates/core/src/project_commands.rs", "pub fn changed() {}\n")
        unqualified = self.commit("Rust work with no passing qualification")
        self.write("docs/notes.md", "Documentation-only follow-up.\n")
        head = self.commit("later documentation")
        with patch.dict(os.environ, {"GITHUB_EVENT_BEFORE": unqualified}):
            result = ci_plan.make_plan(self.root, head, self.qualification())
        self.assertEqual(result["base"], self.base)
        self.assertIn("crates/core/src/project_commands.rs", result["changed_paths"])
        self.assertIn("docs/notes.md", result["changed_paths"])
        self.assertEqual(result["adapter_ports"], ["project-commands"])
        self.assertTrue(result["gates"]["application"])

    def test_missing_verified_ancestor_requires_full_fork_qualification(self):
        self.write("docs/notes.md", "Documentation-only follow-up.\n")
        head = self.commit("docs without a qualification receipt")
        result = ci_plan.make_plan(self.root, head, None)
        self.assertEqual(result["mode"], "full-fork")
        self.assertIsNone(result["base"])
        self.assertTrue(all(result["gates"].values()))
        self.assertEqual(result["component_tests"], sorted(COMPONENT_MANIFEST))

    def test_recorded_acceptance_receipt_edits_and_deletions_run_source_guards(self):
        path = "docs/qualification/project-linux.json"
        mapping = copy.deepcopy(MAPPING)
        mapping["ports"][0]["platform_routes"] = [{
            "acceptance_evidence": {"linux-x64": path},
        }]
        self.write("port-map.json", json.dumps(mapping))
        self.write(path, '{"status":"accepted"}\n')
        self.base = self.commit("qualified acceptance receipt fixture")
        for deleted in [False, True]:
            with self.subTest(deleted=deleted):
                if deleted:
                    (self.root / path).unlink()
                else:
                    self.write(path, '{"status":"stale"}\n')
                head = self.commit("change acceptance receipt")
                result = ci_plan.make_plan(self.root, head, self.qualification())
                self.assertEqual(result["changed_paths"], [path])
                self.assertEqual(result["mode"], "affected-fork")
                self.assertEqual(result["rust_packages"], ["t3code-ls-tools"])
                self.assertTrue(result["source_guards"])
                self.assertFalse(result["stage_native"])
                self.assertFalse(result["gates"]["application"])

    def test_deleted_upstream_file_is_not_misclassified_as_trusted_source(self):
        path = "apps/web/src/TrustedUpstreamView.tsx"
        (self.root / path).unlink()
        head = self.commit("unaccounted source deletion")
        result = ci_plan.make_plan(self.root, head, self.qualification())
        self.assertIn(path, result["changed_paths"])
        self.assertNotIn(path, result["trusted_upstream_paths"])
        self.assertEqual(result["mode"], "full-fork")

    def test_restoring_fork_modified_source_to_upstream_cannot_remove_wiring_without_qualification(self):
        for path, wiring in [
            ("apps/server/src/Orchestrator.ts", "export const backend = 'rust integration';\n"),
            ("apps/server/package.json", '{"name":"fork-server","rustIntegration":true}\n'),
        ]:
            with self.subTest(path=path):
                self.write(path, wiring)
                self.base = self.commit("qualified fork wiring")
                self.run_git("checkout", self.upstream, "--", path)
                head = self.commit("replace fork wiring with upstream")
                result = ci_plan.make_plan(self.root, head, self.qualification())
                self.assertEqual(result["changed_paths"], [path])
                self.assertNotIn(path, result["trusted_upstream_paths"])
                self.assertEqual(result["mode"], "full-fork")
                self.assertTrue(all(result["gates"].values()))

    def test_true_upstream_source_advance_and_addition_skip_unrelated_integration_checks(self):
        existing = "apps/web/src/TrustedUpstreamView.tsx"
        added = "apps/web/src/NewUpstreamView.tsx"
        self.run_git("checkout", "--detach", self.upstream)
        self.write(existing, "export const label = 'upstream advance';\n")
        self.write(added, "export const label = 'upstream addition';\n")
        upstream = self.commit("trusted upstream advances")
        self.run_git("checkout", "--detach", self.base)
        self.run_git("checkout", upstream, "--", existing, added)
        self.write("upstream.lock.json", json.dumps({"upstream": {"commit": upstream}}))
        head = self.commit("mechanical upstream import")
        result = ci_plan.make_plan(self.root, head, self.qualification())
        self.assertEqual(set(result["trusted_upstream_paths"]), {existing, added})
        self.assertEqual(result["mode"], "affected-fork")
        self.assertEqual(result["rust_packages"], ["t3code-ls-tools"])
        self.assertFalse(result["gates"]["adapters"])
        self.assertFalse(result["gates"]["application"])

    def test_restoring_a_fork_deleted_upstream_path_is_not_a_trusted_addition(self):
        path = "apps/web/src/TrustedUpstreamView.tsx"
        (self.root / path).unlink()
        self.base = self.commit("qualified fork source removal")
        self.run_git("checkout", self.upstream, "--", path)
        head = self.commit("restore removed upstream source")
        result = ci_plan.make_plan(self.root, head, self.qualification())
        self.assertNotIn(path, result["trusted_upstream_paths"])
        self.assertEqual(result["mode"], "full-fork")

    def test_unavailable_qualified_base_upstream_objects_require_full_qualification(self):
        self.write("upstream.lock.json", json.dumps({"upstream": {"commit": "a" * 40}}))
        self.base = self.commit("qualification with unavailable old upstream source")
        self.write("upstream.lock.json", json.dumps({"upstream": {"commit": self.upstream}}))
        head = self.commit("restore an available upstream pin")
        result = ci_plan.make_plan(self.root, head, self.qualification())
        self.assertEqual(result["trusted_upstream_paths"], [])
        self.assertEqual(result["mode"], "full-fork")
        self.assertTrue(all(result["gates"].values()))

    def test_exact_source_qualification_can_still_be_reused(self):
        qualification = {**self.qualification(), "reused": True}
        result = ci_plan.make_plan(self.root, self.base, qualification)
        self.assertEqual(result["mode"], "reused")
        self.assertEqual(result["changed_paths"], [])
        self.assertFalse(any(result["gates"].values()))
        self.assertEqual(result["component_tests"], [])
        forced = ci_plan.make_plan(self.root, self.base, qualification, force_full=True)
        self.assertEqual(forced["mode"], "full-fork")
        self.assertTrue(all(forced["gates"].values()))
        self.assertEqual(forced["component_tests"], sorted(COMPONENT_MANIFEST))

    def test_uncommitted_worktree_changes_do_not_affect_a_pinned_plan(self):
        self.write("docs/notes.md", "Recorded documentation change.\n")
        head = self.commit("documentation")
        self.write("crates/core/src/project_commands.rs", "uncommitted and unqualified\n")
        result = ci_plan.make_plan(self.root, head, self.qualification())
        self.assertEqual(result["changed_paths"], ["docs/notes.md"])
        self.assertFalse(any(result["gates"].values()))


if __name__ == "__main__":
    unittest.main()
