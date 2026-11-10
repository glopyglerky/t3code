#!/usr/bin/env python3
"""Plan fork integration and Rust port work from immutable, trusted upstream source."""

from __future__ import annotations

import argparse
from collections import Counter, defaultdict
import json
import os
from pathlib import Path
import subprocess
import sys

import nightly


POLICY_FILES = {"AGENTS.md", "CLAUDE.md", "README.md", "upstream-README.md", "LICENSE"}
POLICY_PREFIXES = (".github/", ".agents/", ".claude/", ".cursor/", "tools/")
BOUNDARY_FILES = {"package.json", "pnpm-lock.yaml", "pnpm-workspace.yaml", ".pnpmfile.cjs",
                  "Cargo.toml", "Cargo.lock", "tsconfig.base.json", "tsconfig.effect.json",
                  "vite.config.ts"}
ACCEPTED = {"accepted", "retired", "synced"}
RUST_ROUTES = {"rust", "rust-backed-client"}


def bounded_git(root: Path, *args: str, missing_ok: bool = False) -> bytes | None:
    try:
        result = subprocess.run(
            ["git", "-C", str(root), *args], check=False, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, timeout=60,
            env={**os.environ, "GIT_TERMINAL_PROMPT": "0"},
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        raise nightly.TrackingError(f"git {args[0]} did not complete.") from error
    if result.returncode:
        if missing_ok:
            return None
        # Never include Git stderr, which can contain transport credentials.
        raise nightly.TrackingError(f"git {args[0]} failed.")
    return result.stdout


def parse_default_advertisement(raw: bytes) -> dict:
    try:
        lines = raw.decode("utf-8").splitlines()
    except UnicodeError as error:
        raise nightly.TrackingError("Invalid upstream HEAD advertisement.") from error
    branch, commit = None, None
    for line in lines:
        fields = line.split("\t")
        if len(fields) != 2 or fields[1] != "HEAD":
            raise nightly.TrackingError("Unexpected upstream HEAD advertisement.")
        value = fields[0]
        if value.startswith("ref: refs/heads/"):
            if branch is not None:
                raise nightly.TrackingError("Ambiguous upstream default branch.")
            branch = nightly.require_relative_path(value.removeprefix("ref: refs/heads/"))
        else:
            if commit is not None:
                raise nightly.TrackingError("Ambiguous upstream HEAD commit.")
            commit = nightly.require_sha(value)
    if branch is None or commit is None:
        raise nightly.TrackingError("Upstream must advertise both its default branch and exact HEAD commit.")
    return {"repository": nightly.REPOSITORY, "branch": branch, "commit": commit,
            "selection": "advertised-default-branch"}


def resolve_default(root: Path) -> dict:
    return parse_default_advertisement(
        bounded_git(root, "ls-remote", "--symref", nightly.REMOTE, "HEAD"),
    )


def ensure_metadata(root: Path, commit: str, fetch: bool) -> None:
    nightly.require_sha(commit)
    kind = bounded_git(root, "cat-file", "-t", commit, missing_ok=True)
    if kind is None and fetch:
        bounded_git(root, "-c", "core.hooksPath=/dev/null", "fetch", "--filter=blob:none",
                    "--no-tags", "--no-write-fetch-head", "--depth=1", nightly.REMOTE, commit)
        kind = bounded_git(root, "cat-file", "-t", commit)
    if kind is None:
        raise nightly.TrackingError(f"Missing immutable source {commit}; fetch it before an offline plan.")
    if kind.strip() != b"commit":
        raise nightly.TrackingError("A source identity does not name a commit.")


def committed_metadata(root: Path, fork: str, path: str) -> dict:
    raw = bounded_git(root, "show", f"{fork}:{path}")
    try:
        # port_impact reads the checkout map; bind that read to the same committed input.
        if (root / path).read_bytes() != raw:
            raise nightly.TrackingError(f"Commit {path} before generating an immutable porting plan.")
        value = json.loads(raw)
    except (OSError, ValueError) as error:
        raise nightly.TrackingError(f"Could not read committed {path}.") from error
    if not isinstance(value, dict) or value.get("schema_version") != 1:
        raise nightly.TrackingError(f"Unsupported {path} schema.")
    return value


def port_requirements(port: dict, targets: list[str]) -> dict:
    routes = {}
    compiler_routes = set()
    for route in port.get("platform_routes", []):
        if not isinstance(route, dict) or not isinstance(route.get("targets"), list):
            raise nightly.TrackingError("Invalid port platform route.")
        if route.get("implementation") in RUST_ROUTES:
            compiler_routes.add(route.get("compiler_route", "unspecified"))
        for target in route["targets"]:
            if target not in targets or target in routes:
                raise nightly.TrackingError("Unknown or duplicate port platform route.")
            routes[target] = route
    missing_rust, missing_receipts, unaccepted = [], [], []
    for target in targets:
        route = routes.get(target, {})
        if route.get("implementation") not in RUST_ROUTES:
            missing_rust.append(target)
        if not route.get("acceptance_evidence", {}).get(target):
            missing_receipts.append(target)
        if route.get("state") not in ACCEPTED:
            unaccepted.append(target)
    result = {}
    for key in ("rust_paths", "rust_crates", "acceptance_tests"):
        values = port.get(key)
        if not isinstance(values, list):
            raise nightly.TrackingError(f"Port {port['id']} requires {key}.")
        result[key] = sorted({nightly.require_relative_path(value) for value in values})
    return {**result, "compiler_routes": sorted(compiler_routes),
            "retirement": {"automatic": False, "missing_rust_targets": missing_rust,
                           "missing_receipt_targets": missing_receipts,
                           "unaccepted_targets": unaccepted,
                           "receipt_validity": "Not evaluated here; check-source must validate current evidence."}}


def classify(path: str, before: dict | None, after: dict | None, fork: dict | None,
             owners: list[str]) -> str:
    if owners:
        return "translation-review"
    if path in POLICY_FILES or path.startswith(POLICY_PREFIXES):
        return "fork-policy-review"
    if after is None:
        return "upstream-removal-review"
    if fork == after:
        return "already-matches-upstream"
    if fork != before:
        return "fork-conflict"
    if (path in BOUNDARY_FILES or path.startswith("patches/")
            or path.endswith(("/package.json", "/Cargo.toml"))):
        return "integration-boundary-review"
    return "mechanical-import-candidate"


def build_plan(root: Path, upstream: dict) -> dict:
    if upstream.get("repository") != nightly.REPOSITORY:
        raise nightly.TrackingError("Unexpected trusted upstream repository.")
    fork = nightly.require_sha(bounded_git(root, "rev-parse", "HEAD").decode().strip())
    mapping = committed_metadata(root, fork, "port-map.json")
    lock = committed_metadata(root, fork, "upstream.lock.json")
    if lock.get("upstream", {}).get("repository") != nightly.REPOSITORY:
        raise nightly.TrackingError("Unexpected integrated upstream repository.")
    base = nightly.require_sha(lock["upstream"].get("commit"))
    head = nightly.require_sha(upstream.get("commit"))
    before, after, fork_tree = (nightly.read_tree(root, commit) for commit in (base, head, fork))
    actual_base_tree = bounded_git(root, "rev-parse", f"{base}^{{tree}}").decode().strip()
    if actual_base_tree != lock["upstream"].get("root_tree"):
        raise nightly.TrackingError("Integrated upstream root tree does not match its pin.")
    changes = [{"path": path, "status": "added" if path not in before else
                "removed" if path not in after else "modified", "before": before.get(path),
                "after": after.get(path), "fork": fork_tree.get(path)}
               for path in sorted(before.keys() | after.keys()) if before.get(path) != after.get(path)]
    impact = nightly.port_impact(root, base, changes)
    if impact["status"] != "mapped":
        raise nightly.TrackingError("A validated port map is required for porting work selection.")
    ports = mapping["ports"]
    targets = mapping.get("supported_targets")
    if (not isinstance(targets, list) or not targets or any(not isinstance(x, str) for x in targets)
            or len(set(targets)) != len(targets)):
        raise nightly.TrackingError("Port map requires unique supported targets.")
    owners = defaultdict(set)
    for port in ports:
        for source in port["upstream_sources"] + port.get("retired_paths", []):
            owners[source["path"]].add(port["id"])
        for path in port.get("adapter_paths", []):
            owners[path].add(port["id"])
    removed_by_object = defaultdict(list)
    for change in changes:
        if change["path"].startswith("packages/contracts/"):
            owners[change["path"]].update(port["id"] for port in ports)
        if change["status"] == "removed":
            source = change["before"]
            removed_by_object[(source["mode"], source["kind"], source["oid"])].append(change["path"])
    for change in changes:
        path = change["path"]
        matching_old_paths = []
        if change["status"] == "added":
            source = change["after"]
            matching_old_paths = removed_by_object.get((source["mode"], source["kind"], source["oid"]), [])
            for old in matching_old_paths:
                owners[path].update(owners[old])
        change["ports"] = sorted(owners[path])
        change["possible_rename_sources"] = sorted(matching_old_paths)
        change["classification"] = classify(path, change["before"], change["after"], change["fork"], change["ports"])
    queue = []
    shared_contracts = impact["shared_contract_paths"]
    for port in sorted(ports, key=lambda item: item["id"]):
        paths = [change["path"] for change in changes if port["id"] in change["ports"]]
        requirements = port_requirements(port, targets)
        if paths:
            priority, action = 10, "adapt-rust-to-upstream"
        elif port["state"] == "candidate":
            priority, action = 50, "integrate-candidate-port"
        elif (port["state"] not in ACCEPTED or requirements["retirement"]["unaccepted_targets"]
              or requirements["retirement"]["missing_rust_targets"]
              or requirements["retirement"]["missing_receipt_targets"]):
            priority, action = 40, "qualify-integrated-port"
        else:
            continue
        queue.append({"id": f"port:{port['id']}", "priority": priority, "action": action,
                      "port": port["id"], "state": port["state"], "domain_owner": port["domain_owner"],
                      "upstream_paths": paths, "shared_contract_paths": shared_contracts if paths else [],
                      "blocks_upstream_integration": bool(paths), **requirements})
    for category, priority, action in [
        ("fork-conflict", 20, "resolve-fork-conflicts"),
        ("fork-policy-review", 20, "preserve-fork-policy"),
        ("upstream-removal-review", 20, "record-upstream-removals"),
        ("integration-boundary-review", 20, "check-fork-integration-boundaries"),
        ("mechanical-import-candidate", 30, "integrate-trusted-upstream-files"),
    ]:
        paths = [change["path"] for change in changes if change["classification"] == category]
        if paths:
            queue.append({"id": f"upstream:{category}", "priority": priority, "action": action,
                          "upstream_paths": paths, "blocks_upstream_integration": True})
    queue.sort(key=lambda item: (item["priority"], item["id"]))
    return {
        "schema_version": 1,
        "status": "current" if base == head else "update-available",
        "upstream": upstream,
        "trust": {"upstream_working": True, "authority": "user-policy",
                  "upstream_tests_required": False,
                  "fork_validation": "Validate Rust replacements and changed integration boundaries."},
        "revisions": {"integrated_upstream": base, "trusted_upstream": head, "fork": fork,
                      "qualified_upstream": lock.get("qualification", {}).get("last_qualified_commit")},
        "source_inputs": "Committed Git trees and committed port map; uncommitted source is excluded.",
        "classification_counts": dict(sorted(Counter(change["classification"] for change in changes).items())),
        "changed_file_count": len(changes), "changes": changes, "queue": queue,
        "automation": {"source_writes": False, "pin_updates": False, "acceptance_promotion": False,
                       "retirement": False, "provider_requests": False,
                       "mechanical_imports": "Candidates only; integrate the complete change with the listed fork work."},
    }


def markdown_summary(plan: dict) -> str:
    revisions = plan["revisions"]
    counts = plan["classification_counts"]
    lines = ["## Rust porting queue", "",
             f"Trusted upstream `{revisions['trusted_upstream']}`; integrated `{revisions['integrated_upstream']}`.",
             "Upstream is trusted as working. Checks target Rust replacements and fork integration.", "",
             f"Compared {plan['changed_file_count']} upstream paths across three immutable trees.", "",
             "| Classification | Paths |", "| --- | ---: |"]
    lines.extend(f"| {category} | {count} |" for category, count in counts.items())
    lines.extend(["", "| Priority | Work | Changed paths |", "| ---: | --- | ---: |"])
    lines.extend(f"| {item['priority']} | {item['id']}: {item['action']} | {len(item['upstream_paths'])} |"
                 for item in plan["queue"])
    lines.extend(["", "The JSON artifact contains exact source identities, Rust owners, tests, compiler routes,",
                  "and missing platform receipts. This plan does not change source, pins, acceptance, or retire files.", ""])
    return "\n".join(lines)


def output_path(root: Path, path: Path) -> Path:
    resolved = path.resolve()
    if resolved == root or root in resolved.parents:
        raise nightly.TrackingError("Porting reports must be outside the source checkout.")
    return resolved


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, required=True)
    selection = parser.add_mutually_exclusive_group(required=True)
    selection.add_argument("--upstream-commit", help="Already-fetched immutable snapshot of trusted upstream.")
    selection.add_argument("--fetch-default", action="store_true", help="Resolve default HEAD once and fetch immutable tree metadata.")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--summary", type=Path, help="Optional Markdown job summary outside source.")
    args = parser.parse_args(argv)
    try:
        root = args.repo_root.resolve()
        destination = output_path(root, args.output)
        summary = output_path(root, args.summary) if args.summary else None
        if summary == destination:
            raise nightly.TrackingError("JSON report and Markdown summary must use different files.")
        fork = nightly.require_sha(bounded_git(root, "rev-parse", "HEAD").decode().strip())
        lock = committed_metadata(root, fork, "upstream.lock.json")
        base = nightly.require_sha(lock.get("upstream", {}).get("commit"))
        upstream = resolve_default(root) if args.fetch_default else {
            "repository": nightly.REPOSITORY, "branch": None,
            "commit": nightly.require_sha(args.upstream_commit), "selection": "explicit-immutable-commit",
        }
        for commit in dict.fromkeys([base, upstream["commit"]]):
            ensure_metadata(root, commit, args.fetch_default)
        plan = build_plan(root, upstream)
        nightly.write_json(destination, plan)
        if summary:
            summary.parent.mkdir(parents=True, exist_ok=True)
            summary.write_text(markdown_summary(plan))
        print(json.dumps({"status": plan["status"], "trusted_upstream": upstream["commit"],
                          "changed_file_count": plan["changed_file_count"],
                          "queue_items": len(plan["queue"])}))
        return 0
    except (nightly.TrackingError, OSError) as error:
        print(f"Porting plan failed: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
