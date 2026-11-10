#!/usr/bin/env python3
"""Select fork qualification from a verified ancestor; upstream is a trusted oracle."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tomllib

import nightly


PRODUCTION_PORTS = {
    "command-and-dispatch-policy", "project-commands", "guest-share-decisions",
    "provider-recovery", "provider-maintenance", "codex-structured-generation",
    "codex-process-runtime",
}
HYBRID_ACCEPTANCE_TESTS = {
    "tools/check-policy-adapter.mjs": "command-and-dispatch-policy",
    "tools/check-dispatch.mjs": "command-and-dispatch-policy",
    "tools/check-project-adapter.mjs": "project-commands",
    "tools/check-guest-share-adapter.mjs": "guest-share-decisions",
    "tools/check-recovery-adapter.mjs": "provider-recovery",
    "tools/check-maintenance-adapter.mjs": "provider-maintenance",
    "tools/check-generation-adapter.mjs": "codex-structured-generation",
    "tools/check-provider-adapter.mjs": "codex-process-runtime",
}
POLICY_FILES = {
    "tools/ci_plan.py", "tools/ci_history.py", "tools/hybrid-checks.mjs",
    "tools/check-hybrid.mjs", "tools/build-native.mjs", "tools/native-artifacts.mjs",
    "tools/install-toolchain.mjs", "tools/install-server-tests.mjs",
    "tools/fetch-migration-sources.mjs", "tools/nightly.py",
    "tools/ci_receipt.py", "tools/test-rust.py",
    "tools/acceptance-checks.json", "tools/check-acceptance.mjs",
}
REPORTING_TOOLS = {"tools/plan-porting.py", "tools/sync-readme.py", "tools/verify-upstream.py"}
GLOBAL_INPUTS = {
    "Cargo.toml", "Cargo.lock", "rust-toolchain.toml", "toolchain.lock.json",
    "lust-toolchain.json", "package.json", "pnpm-lock.yaml", "pnpm-workspace.yaml",
    ".pnpmfile.cjs", "tsconfig.base.json", "tsconfig.effect.json", "vite.config.ts",
    "port-map.json",
}
INTEGRATION_ROOTS = ("apps/", "packages/", "native/", "infra/", "scripts/", "patches/")


def git(root: Path, *args: str) -> bytes:
    return subprocess.run(["git", "--no-replace-objects", "-C", str(root), *args],
                          check=True, capture_output=True, timeout=60).stdout


def read_json(root: Path, revision: str, path: str) -> dict:
    return json.loads(git(root, "show", f"{revision}:{path}"))


def is_policy(path: str) -> bool:
    return path in POLICY_FILES or path.startswith(".github/workflows/")


def policy_digest(tree: dict) -> str:
    entries = [[path, tree[path]] for path in sorted(tree) if is_policy(path)]
    return hashlib.sha256(json.dumps(entries, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def cargo_graph(root: Path, revision: str) -> tuple[dict, dict]:
    """Read the actual workspace and every local dependency, including target sections."""
    workspace = tomllib.loads(git(root, "show", f"{revision}:Cargo.toml").decode())
    members = workspace["workspace"]["members"]
    if not isinstance(members, list) or not members:
        raise ValueError("Cargo workspace has no explicit members")
    manifests = {}
    for member in members:
        nightly.require_relative_path(member)
        if "*" in member:
            raise ValueError("CI requires explicit workspace members")
        manifests[member] = tomllib.loads(git(root, "show", f"{revision}:{member}/Cargo.toml").decode())
    names = {member: manifest["package"]["name"] for member, manifest in manifests.items()}
    if len(set(names.values())) != len(names):
        raise ValueError("Duplicate Cargo package names")
    reverse = {name: set() for name in names.values()}

    def sections(value):
        for key, child in value.items():
            if key in ("dependencies", "dev-dependencies", "build-dependencies"):
                yield child
            elif isinstance(child, dict):
                yield from sections(child)

    workspace_dependencies = workspace["workspace"].get("dependencies", {})
    for member, manifest in manifests.items():
        for dependencies in sections(manifest):
            for dependency, spec in dependencies.items():
                if not isinstance(spec, dict):
                    continue
                location = member
                if spec.get("workspace"):
                    spec = workspace_dependencies.get(dependency)
                    location = "."
                    if spec is None:
                        raise ValueError(f"Unresolved workspace dependency: {dependency}")
                    if not isinstance(spec, dict):
                        continue
                if "path" not in spec:
                    continue
                resolved = (root / location / spec["path"]).resolve().relative_to(root.resolve()).as_posix()
                if resolved not in names:
                    raise ValueError(f"Unmapped local Cargo dependency: {resolved}")
                reverse[names[resolved]].add(names[member])
    return names, reverse


def closure(selected: set[str], reverse: dict) -> set[str]:
    result = set(selected)
    pending = list(selected)
    while pending:
        for consumer in reverse[pending.pop()]:
            if consumer not in result:
                result.add(consumer)
                pending.append(consumer)
    return result


def is_test(path: str) -> bool:
    return "/tests/" in path or "/testkit/" in path or ".test." in path or path.startswith("tools/test_")


def rust_members(path: str, names: dict) -> set[str]:
    """The tools directory contains standalone scripts as well as its Rust crate."""
    return {
        member for member in names
        if path.startswith(member + "/") and (
            member != "tools"
            or path in {"tools/Cargo.toml", "tools/build.rs"}
            or path.startswith(("tools/src/", "tools/tests/", "tools/benches/", "tools/fixtures/"))
        )
    }


def is_reporting_tool(path: str) -> bool:
    name = path.removeprefix("tools/")
    return path in REPORTING_TOOLS or (
        path.startswith("tools/") and "/" not in name
        and name.startswith("test_") and name.endswith(".py")
    )


def provenance_only_mapping_change(mapping: dict, old_mapping: dict) -> bool:
    """Ignore only revision bookkeeping; check-source still validates the full map."""
    def without_provenance(value):
        if (not isinstance(value, dict) or type(value.get("schema_version")) is not int
                or value["schema_version"] != 1):
            return None
        try:
            nightly.require_sha(value["integrated_upstream_commit"])
            history, ports = value["source_history"], value["ports"]
            if not isinstance(history, list) or not history or not isinstance(ports, list) or not ports:
                return None
            for revision in history:
                nightly.require_sha(revision)
            ids, retained_ports = set(), []
            for port in ports:
                if not isinstance(port, dict):
                    return None
                identifier = port.get("id")
                if not isinstance(identifier, str) or not identifier or identifier in ids:
                    return None
                ids.add(identifier)
                nightly.require_sha(port["reviewed_upstream_commit"])
                retained_ports.append({key: child for key, child in port.items()
                                       if key != "reviewed_upstream_commit"})
            retained = {key: child for key, child in value.items()
                        if key not in {"integrated_upstream_commit", "source_history", "ports"}}
            retained["ports"] = retained_ports
            # JSON comparison preserves value types, unlike Python's True == 1.
            return json.dumps(retained, sort_keys=True, separators=(",", ":"), allow_nan=False)
        except (KeyError, TypeError, ValueError, nightly.TrackingError):
            return None

    current, previous = without_provenance(mapping), without_provenance(old_mapping)
    return current is not None and previous is not None and current == previous


def select_checks(changed: list[str], mapping: dict, old_mapping: dict, names: dict,
                  reverse: dict, trusted_paths: set[str], full_reason: str | None = None,
                  component_manifest: dict | None = None) -> dict:
    """Pure decision function. Unknown fork changes widen qualification."""
    packages, changed_packages, affected = set(), set(), set()
    reasons = []
    guards = False
    application = False
    ports = mapping["ports"] + old_mapping.get("ports", [])
    acceptance_tests = {test for port in ports for test in port.get("acceptance_tests", [])}
    acceptance_evidence = {path for port in ports for route in port.get("platform_routes", [])
                           for path in route.get("acceptance_evidence", {}).values()}
    component_manifest = {} if component_manifest is None else component_manifest
    if not isinstance(component_manifest, dict):
        raise ValueError("Acceptance test execution mappings must be an object")
    for path in sorted(acceptance_tests):
        nightly.require_relative_path(path)
        if path.endswith(".rs"):
            if not rust_members(path, names):
                raise ValueError(f"Acceptance test is outside the Cargo workspace: {path}")
            continue
        if path in HYBRID_ACCEPTANCE_TESTS:
            continue
        if path not in component_manifest:
            raise ValueError(f"Acceptance test needs an execution mapping in tools/acceptance-checks.json: {path}")
        spec = component_manifest[path]
        if not isinstance(spec, dict) or spec.get("package") not in names.values():
            raise ValueError(f"Acceptance test has an unknown Rust package execution mapping: {path}")

    def mapped_packages(owners: list[dict]) -> set[str]:
        nonlocal full_reason
        result = set()
        for port in owners:
            members = port.get("rust_crates", [])
            if not members or any(member not in names for member in members):
                full_reason = full_reason or f"Port has an unknown or removed Rust owner: {port['id']}"
            result.update(names[member] for member in members if member in names)
        return result

    for path in changed:
        nightly.require_relative_path(path)
        if path == "port-map.json" and provenance_only_mapping_change(mapping, old_mapping):
            guards = True
            reasons.append("Port-map provenance changed; validate the fork's retained source accounting")
        elif is_policy(path) or path in GLOBAL_INPUTS or path.startswith(".cargo/"):
            full_reason = full_reason or f"Shared build, mapping or CI input changed: {path}"
        exact_ports = [port for port in ports if path in
                       port.get("rust_paths", []) + port.get("adapter_paths", []) + port.get("acceptance_tests", [])
                       or any(source.get("path") == path for source in
                              port.get("upstream_sources", []) + port.get("retired_paths", []))]
        exact = {port["id"] for port in exact_ports}
        affected.update(exact)
        members = rust_members(path, names)
        test_only = is_test(path) or path in acceptance_tests
        runtime_owners = set() if test_only else set(exact)
        owner_packages = mapped_packages(exact_ports)
        if exact and not members:
            packages.update(owner_packages)
            if not test_only:
                changed_packages.update(owner_packages)
        for member in members:
            package = names[member]
            packages.add(package)
            guards = True
            if not test_only:
                changed_packages.add(package)
                if not exact:
                    owners = {port["id"] for port in ports if member in port.get("rust_crates", [])}
                    affected.update(owners)
                    runtime_owners.update(owners)
        application |= bool(runtime_owners & PRODUCTION_PORTS)
        if path.startswith("packages/contracts/"):
            affected.update(port["id"] for port in ports)
            changed_packages.update(mapped_packages(ports))
            application = True
            guards = True
        if path == "upstream.lock.json":
            guards = True
            reasons.append("Upstream provenance changed; validate the fork's retained source accounting")
        if path == "upstream-README.md":
            guards = True
        if path in acceptance_evidence:
            guards = True
        if path in {"upstream.lock.json", "upstream-README.md"} or path in acceptance_evidence:
            pass
        elif exact:
            guards = True
        elif members:
            pass
        elif path in trusted_paths:
            pass
        elif path.startswith(INTEGRATION_ROOTS):
            full_reason = full_reason or f"Unmapped fork integration changed: {path}"
        elif path.startswith("tools/"):
            if path.endswith("-tsconfig.json"):
                affected.update(PRODUCTION_PORTS)
                guards = True
            elif is_reporting_tool(path):
                # These standalone reports and Python tests run in the cheap job.
                pass
            elif not is_policy(path):
                full_reason = full_reason or f"Unclassified fork qualification helper changed: {path}"
        elif path.startswith("docs/") or path.endswith(".md") or path in {"LICENSE", ".gitignore", ".gitattributes"}:
            pass
        elif path not in GLOBAL_INPUTS and not is_policy(path):
            full_reason = full_reason or f"Unclassified fork input changed: {path}"

    trusted_changes = sorted(set(changed) & trusted_paths)
    if trusted_changes:
        reasons.append(f"{len(trusted_changes)} changed files match trusted upstream source; qualify affected fork boundaries only")
    if full_reason:
        packages = set(names.values())
        affected.update(port["id"] for port in ports)
        application = True
        guards = True
        reasons.insert(0, full_reason)
    else:
        expanded_packages = closure(changed_packages, reverse)
        packages.update(expanded_packages)
        dependent_members = {member for member, name in names.items()
                             if name in expanded_packages - changed_packages}
        dependent_owners = {port["id"] for port in ports
                            if dependent_members.intersection(port.get("rust_crates", []))}
        affected.update(dependent_owners)
        application |= bool(dependent_owners & PRODUCTION_PORTS)
    adapters = sorted(affected & PRODUCTION_PORTS)
    scheduled_tests = {path for path, port in HYBRID_ACCEPTANCE_TESTS.items() if port in adapters}
    selected_tests = acceptance_tests if full_reason else acceptance_tests.intersection(changed)
    component_tests = sorted(path for path in selected_tests
                             if not path.endswith(".rs") and path not in scheduled_tests)
    for path in component_tests:
        if path not in component_manifest:
            raise ValueError(f"Acceptance test needs a scheduled execution mapping: {path}")
        packages.add(component_manifest[path]["package"])
    if guards:
        packages.add(names["tools"])
    stage_native = bool(adapters or application)
    if stage_native:
        packages.add(names["crates/core"])
    return {
        "mode": "full-fork" if full_reason else "affected-fork",
        "gates": {"native": bool(packages), "adapters": bool(adapters), "application": application},
        "rust_packages": sorted(packages), "adapter_ports": adapters,
        "component_tests": component_tests,
        "affected_ports": sorted(affected), "source_guards": guards,
        "stage_native": stage_native, "reasons": reasons,
        "trusted_upstream_paths": trusted_changes,
        "upstream_tests": False,
    }


def make_plan(root: Path, head: str, qualification: dict | None, force_full: bool = False) -> dict:
    head = nightly.require_sha(head)
    tree = nightly.read_tree(root, head)
    digest = policy_digest(tree)
    source_tree = git(root, "rev-parse", f"{head}^{{tree}}").decode().strip()
    mapping = read_json(root, head, "port-map.json")
    component_manifest = read_json(root, head, "tools/acceptance-checks.json")
    names, reverse = cargo_graph(root, head)
    base = None if qualification is None else qualification["commit"]
    full_reason = "Explicit full fork qualification" if force_full else None
    if not base:
        full_reason = full_reason or "No verified qualified ancestor is available"
    if base:
        base = nightly.require_sha(base)
        if subprocess.run(["git", "--no-replace-objects", "-C", str(root), "merge-base", "--is-ancestor", base, head],
                          capture_output=True, timeout=60).returncode != 0:
            raise ValueError("Qualification base is not an ancestor of this source")
    old_tree = nightly.read_tree(root, base) if base else {}
    changed = sorted(path for path in tree.keys() | old_tree.keys() if tree.get(path) != old_tree.get(path))
    old_mapping = read_json(root, base, "port-map.json") if base else {}
    lock = read_json(root, head, "upstream.lock.json")
    trusted = set()
    try:
        upstream = nightly.read_tree(root, lock["upstream"]["commit"])
        if base:
            old_lock = read_json(root, base, "upstream.lock.json")
            old_upstream = nightly.read_tree(root, old_lock["upstream"]["commit"])
            # Trust only mechanical upstream changes. Replacing fork wiring with
            # an upstream blob still changes our integration and needs its gates.
            # A new file is mechanical only if both prior trees lacked its path.
            trusted = {path for path in changed
                       if path in tree and tree[path] == upstream.get(path)
                       and old_tree.get(path) == old_upstream.get(path)}
    except (nightly.TrackingError, KeyError, ValueError, subprocess.SubprocessError):
        full_reason = full_reason or "Integrated upstream source objects are unavailable"
    result = select_checks(changed, mapping, old_mapping, names, reverse, trusted, full_reason,
                           component_manifest)
    if qualification and qualification.get("reused") and not force_full:
        result.update(mode="reused", gates={key: False for key in ("native", "adapters", "application")},
                      rust_packages=[], adapter_ports=[], stage_native=False, source_guards=False,
                      component_tests=[],
                      reasons=["This exact source and CI policy already passed qualification"])
    return {
        "schema_version": 1, "head": head, "source_tree": source_tree, "policy_digest": digest,
        "base": base, "base_run_id": qualification.get("run_id") if qualification else None,
        "reused_run_id": qualification.get("run_id") if result["mode"] == "reused" else None,
        "changed_paths": changed, **result,
    }


def write_plan(plan: dict, output: Path, github_output: Path | None = None, summary: Path | None = None):
    nightly.write_json(output, plan)
    if github_output:
        values = {**plan["gates"], "stage_native": plan["stage_native"], "source_guards": plan["source_guards"],
                  "reused": plan["mode"] == "reused", "mode": plan["mode"],
                  "rust_packages": json.dumps(plan["rust_packages"], separators=(",", ":")),
                  "component_tests": json.dumps(plan["component_tests"], separators=(",", ":")),
                  "adapter_ports": ",".join(plan["adapter_ports"])}
        with github_output.open("a") as stream:
            for key, value in values.items():
                stream.write(f"{key}={str(value).lower() if isinstance(value, bool) else value}\n")
    if summary:
        lines = ["## Fork qualification plan", "", f"Mode: `{plan['mode']}`.", "",
                 "Upstream default-branch behavior is trusted. Checks cover fork changes and Rust integration.", "",
                 f"Changed files: {len(plan['changed_paths'])}.",
                 f"Rust packages: {', '.join(plan['rust_packages']) or 'none'}.",
                 f"Production ports: {', '.join(plan['adapter_ports']) or 'none'}.",
                 f"Component acceptance tests: {', '.join(plan['component_tests']) or 'none'}.",
                 f"Native client flow: {plan['gates']['application']}."]
        if plan.get("reused_run_id"):
            lines.append(f"Reused exact-source qualification run: {plan['reused_run_id']}.")
        lines += ["", *[f"- {reason}" for reason in plan["reasons"][:20]], ""]
        with summary.open("a") as stream:
            stream.write("\n".join(lines))


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--github-output", type=Path)
    parser.add_argument("--summary", type=Path)
    parser.add_argument("--full", action="store_true")
    parser.add_argument("--offline", action="store_true", help="Run full fork qualification without remote receipts")
    args = parser.parse_args(argv)
    root = args.repo_root.resolve()
    head = git(root, "rev-parse", "HEAD").decode().strip()
    qualification = None
    if not args.full and not args.offline and os.environ.get("GITHUB_REPOSITORY"):
        from ci_history import find_qualification
        qualification = find_qualification(root, os.environ["GITHUB_REPOSITORY"], head,
                                           policy_digest(nightly.read_tree(root, head)),
                                           os.environ.get("GITHUB_RUN_ID", ""), os.environ.get("GITHUB_TOKEN"))
    plan = make_plan(root, head, qualification, args.full)
    write_plan(plan, args.output, args.github_output, args.summary)
    print(json.dumps({"mode": plan["mode"], "gates": plan["gates"], "base": plan["base"]}))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (ValueError, KeyError, OSError, subprocess.SubprocessError, nightly.TrackingError) as error:
        print(f"CI planning failed: {error}", file=sys.stderr)
        raise SystemExit(1)
