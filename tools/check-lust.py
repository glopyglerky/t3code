#!/usr/bin/env python3
"""Build pinned Lust and verify application decisions, generation, outbox, and provider processes.

Usage: python3 tools/check-lust.py --source /absolute/path/to/lust-lang

The checkout must match lust-toolchain.json. Cargo builds the compiler; Lust
itself checks, tests and builds our core. There is no --rustc or --release
fallback. Iroh remains Cargo-built. A JSON receipt is
written on both success and failure, defaulting to target/lust-proof.json.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import shutil
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
REPO = ROOT
PIN_FILE = ROOT / "lust-toolchain.json"


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def run(command: list[str], *, cwd: Path, env: dict[str, str],
        receipt: dict, name: str, timeout: int = 600) -> subprocess.CompletedProcess:
    print(f"[lust] {name}", flush=True)
    result = subprocess.run(command, cwd=cwd, env=env, capture_output=True,
                            text=True, timeout=timeout, check=False)
    receipt["steps"].append({"name": name, "command": command,
                              "returncode": result.returncode,
                              "stdout": result.stdout, "stderr": result.stderr})
    if result.returncode:
        if result.stdout:
            print(result.stdout, file=sys.stderr, end="")
        if result.stderr:
            print(result.stderr, file=sys.stderr, end="")
        raise RuntimeError(f"{name} failed with exit status {result.returncode}")
    return result


def git(source: Path, *args: str) -> str:
    result = subprocess.run(["git", "--no-replace-objects", "-C", str(source), *args],
                            capture_output=True, text=True, check=True)
    return result.stdout.strip()


def verify_source(source: Path, revision: str) -> None:
    actual = git(source, "rev-parse", "HEAD")
    if actual != revision:
        raise RuntimeError(f"Compiler revision mismatch: expected {revision}, found {actual}")
    if git(source, "status", "--porcelain", "--untracked-files=normal"):
        raise RuntimeError("The pinned Lust checkout has local changes; use a clean checkout")


def source_hashes() -> dict[str, str]:
    files = list((ROOT / "crates/core").rglob("*.rs"))
    files += list((ROOT / "tools").glob("check-*.mjs"))
    files += list((ROOT / "tools").glob("*-tsconfig.json"))
    files += [ROOT / "Cargo.toml", ROOT / "Cargo.lock", ROOT / "crates/core/Cargo.toml",
              ROOT / "tools/upstream-source.mjs",
              ROOT / "tools/install-server-tests.mjs",
              ROOT / "tools/server-test-runtime/package.json",
              ROOT / "tools/server-test-runtime/package-lock.json",
              ROOT / "upstream.lock.json", Path(__file__).resolve(), PIN_FILE]
    files += [REPO / "apps/server/src/orchestration-v2/NativeDispatchFacts.ts",
              REPO / "apps/server/src/orchestration-v2/NativeDispatchPolicy.ts",
              REPO / "apps/server/src/orchestration-v2/NativeCommandPolicy.ts",
              REPO / "apps/server/src/orchestration-v2/NativeGuestShare.ts",
              REPO / "apps/server/src/orchestration-v2/GuestShareDecider.ts",
              REPO / "apps/server/src/orchestration-v2/NativeProjectCommands.ts",
              REPO / "apps/server/src/orchestration-v2/ProjectCommands.ts",
              REPO / "apps/server/src/orchestration-v2/ProjectStore.ts",
              REPO / "apps/server/src/orchestration-v2/EffectOutbox.ts",
              REPO / "packages/provider-core/src/server/NativeMaintenancePolicy.ts",
              REPO / "packages/provider-core/src/server/maintenanceResolver.ts",
              REPO / "packages/provider-core/src/server/collectStreamText.ts",
              REPO / "apps/server/src/textGeneration/CodexTextGeneration.ts",
              REPO / "apps/server/src/textGeneration/NativeCodexTextGeneration.ts",
              REPO / "apps/server/src/provider/codexLaunchArgs.ts",
              REPO / "apps/server/src/codexModelOptions.ts",
              REPO / "packages/provider-core/src/server/textGenerationOperations.ts",
              REPO / "packages/provider-core/src/server/textGenerationUtils.ts",
              REPO / "packages/shared/src/model.ts",
              REPO / "packages/shared/src/cliArgs.ts",
              REPO / "packages/shared/src/semver.ts",
              REPO / "apps/server/src/project/ProjectService.ts",
              REPO / "packages/contracts/src/applicationEvent.ts",
              REPO / "packages/contracts/src/baseSchemas.ts",
              REPO / "packages/contracts/src/keybindings.ts",
              REPO / "packages/contracts/src/project.ts",
              REPO / "apps/server/src/orchestration-v2/CommandPolicy.ts",
              REPO / "apps/server/src/orchestration-v2/ProviderRecoveryPlanner.ts",
              REPO / "apps/server/src/orchestration-v2/ProviderRuntimeRecoveryService.ts",
              REPO / "apps/server/src/processRunner.ts",
              REPO / "apps/server/src/orchestration-v2/Orchestrator.ts"]
    hashes = {str(path.relative_to(REPO)): sha256(path) for path in sorted(files)}
    upstream = json.loads((ROOT / "upstream.lock.json").read_text())["upstream"]
    commit = upstream["commit"]
    if not re.fullmatch(r"[a-f0-9]{40}", commit):
        raise RuntimeError("Invalid locked upstream commit")
    # The oracles execute immutable Git blobs through upstream-source.mjs.
    # Hash those same blobs, not possibly modified working-tree TypeScript.
    for path in ("packages/contracts/src/auth.ts",
                 "packages/client-runtime/src/connection/supervisor.ts",
                 "packages/client-runtime/src/connection/wakeups.ts",
                 "apps/server/src/orchestration-v2/CommandPolicy.ts",
                 "apps/server/src/orchestration-v2/GuestShareDecider.ts",
                 "apps/server/src/orchestration-v2/ProjectCommands.ts",
                 "apps/server/src/orchestration-v2/ProjectStore.ts",
                 "apps/server/src/orchestration-v2/EffectOutbox.ts",
                 "packages/contracts/src/baseSchemas.ts",
                 "packages/contracts/src/keybindings.ts",
                 "packages/contracts/src/guestShare.ts",
                 "packages/provider-core/src/server/subagentProjection.ts",
                 "packages/provider-core/src/server/maintenanceResolver.ts",
                 "packages/provider-core/src/server/collectStreamText.ts",
                 "apps/server/src/textGeneration/CodexTextGeneration.ts",
                 "apps/server/src/provider/codexLaunchArgs.ts",
                 "apps/server/src/codexModelOptions.ts",
                 "packages/provider-core/src/server/textGenerationOperations.ts",
                 "packages/provider-core/src/server/textGenerationUtils.ts",
                 "packages/shared/src/model.ts",
                 "packages/shared/src/cliArgs.ts",
                 "packages/shared/src/semver.ts",
                 "packages/contracts/src/orchestrationV2.ts",
                 "packages/effect-codex-app-server/src/protocol.ts",
                 "packages/effect-codex-app-server/test/fixtures/codex-app-server-mock-peer.ts"):
        blob = subprocess.run(["git", "--no-replace-objects", "-C", str(REPO),
                               "cat-file", "blob", f"{commit}:{path}"],
                              capture_output=True, check=True).stdout
        hashes[f"git:{commit}:{path}"] = hashlib.sha256(blob).hexdigest()
    return hashes


def test_summary(output: str) -> dict:
    records = [json.loads(line) for line in output.splitlines() if line.strip()]
    summaries = [entry for entry in records if entry.get("kind") == "summary"]
    if len(summaries) != 1:
        raise RuntimeError("Lust did not return exactly one test summary")
    summary = summaries[0]
    if (summary.get("build") != "ok" or summary.get("total", 0) == 0
            or summary.get("passed") != summary.get("total")
            or any(summary.get(key, 0) for key in
                   ("failed", "timed_out", "ignored", "filtered_out"))):
        raise RuntimeError("Lust did not execute and pass every selected unit test")
    return summary


def write_receipt(path: Path, receipt: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", dir=path.parent, delete=False) as file:
        json.dump(receipt, file, indent=2)
        file.write("\n")
        temp = Path(file.name)
    temp.replace(path)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path,
                        help="clean Lust checkout at lust-toolchain.json's exact revision")
    parser.add_argument("--receipt", type=Path, default=ROOT / "target/lust-proof.json")
    parser.add_argument("--artifacts", type=Path,
                        help="save the verified standalone Lust executables here on success")
    parser.add_argument("--toolchain", type=Path, help="retained packaged compiler directory")
    args = parser.parse_args()
    source = args.source.resolve() if args.source else None
    receipt = {"schema": 1, "status": "failed", "steps": [],
               "generated_at": datetime.now(timezone.utc).isoformat(),
               "scope": "Lust application decisions, provider maintenance policy, Codex structured-generation runner, durable outbox policy, recovery wire, JSON codec, and Unix provider process runtime; "
                        "full application, UI, mobile, and Iroh qualification excluded"}
    try:
        pin = json.loads(PIN_FILE.read_text())
        receipt["compiler"] = pin
        if platform.system() != "Linux" or platform.machine() not in ("x86_64", "amd64"):
            raise RuntimeError("This proof is qualified for Linux x86_64; other hosts require review")
        if not args.toolchain:
            verify_source(source, pin["revision"])
        env = {key: value for key, value in os.environ.items() if not key.startswith("LUST_")}
        env["CARGO_INCREMENTAL"] = "0"
        for command in ("cargo", "node", "bash"):
            if not shutil.which(command, path=env.get("PATH")):
                raise RuntimeError(f"{command} is required on PATH")
        receipt["workspace_git_revision"] = git(REPO, "rev-parse", "HEAD")
        receipt["inputs"] = source_hashes()
        if args.toolchain:
            lust = args.toolchain.resolve() / "bin/lust"
            core = lust.with_name("lust-core")
            for binary in (lust, core):
                if sha256(binary) != pin["packaged_native_sha256"][binary.name]:
                    raise RuntimeError("Packaged native compiler hash mismatch")
        else:
            run(["bash", "compiler/native/scripts/build-tcc.sh"], cwd=source, env=env,
                receipt=receipt, name="build pinned TinyCC")
            tinycc = source / "compiler/native/third_party/tinycc"
            if git(tinycc, "rev-parse", "HEAD") != pin["tinycc_revision"]:
                raise RuntimeError("TinyCC revision does not match lust-toolchain.json")
            if git(tinycc, "status", "--porcelain", "--untracked-files=normal"):
                raise RuntimeError("TinyCC source has local changes")
            run(["cargo", "build", "--locked", "--profile", pin["build_profile"],
                 "--manifest-path", str(source / "compiler/native/Cargo.toml"),
                 "--target-dir", str(source / "compiler/native/target")], cwd=source, env=env,
                receipt=receipt, name="build pinned Lust compiler")
            lust = source / "compiler/native/target" / pin["build_profile"] / "lust"
            core = lust.with_name("lust-core")
        receipt["compiler_binary_sha256"] = sha256(lust)
        receipt["compiler_core_sha256"] = sha256(core)
        with tempfile.TemporaryDirectory(prefix="t3-code-ls-lust-") as temp:
            work = Path(temp)
            env["LUST_NO_DAEMON"] = "1"
            env["LUST_CACHE_DIR"] = str(work / "cache")
            # Separate PID namespaces can reuse Lust's temporary filename prefix.
            (work / "tmp").mkdir()
            env["TMPDIR"] = str(work / "tmp")
            version = run([str(lust), "--version"], cwd=ROOT, env=env,
                          receipt=receipt, name="read compiler build identity")
            receipt["compiler_version"] = version.stdout.strip()
            # This pinned compiler sets --lib's target name to the package
            # argument without Cargo's hyphen-to-underscore normalization.
            # Passing the actual Cargo lib target selects the unchanged core.
            run([str(lust), "check", "crates/core/src/lib.rs", "--manifest-path",
                 "crates/core/Cargo.toml", "--lib", "--package", "t3_code_ls_core", "--json"],
                cwd=ROOT, env=env, receipt=receipt, name="check core with Lust")
            # Lust's test harness rejects file modules. Execute each original
            # module containing inline unit tests, without rewriting the source.
            test_files = [path for path in sorted((ROOT / "crates/core/src").rglob("*.rs"))
                          if re.search(r"#\[\s*test\s*\]", path.read_text())]
            if not test_files:
                raise RuntimeError("Core has no unit test roots")
            receipt["unit_tests"] = []
            for path in test_files:
                result = run([str(lust), "test", str(path), "--json"], cwd=ROOT, env=env,
                             receipt=receipt, name=f"test {path.name} with Lust")
                receipt["unit_tests"].append(test_summary(result.stdout))
            generation_checks = work / "generation-checks-lust"
            run([str(lust), "build", "crates/core/examples/generation_checks.rs",
                 "--manifest-path", "crates/core/Cargo.toml", "--bin", "generation_checks",
                 "-o", str(generation_checks), "--json"], cwd=ROOT, env=env,
                receipt=receipt, name="build unchanged generation assertions with Lust")
            generation_result = run([str(generation_checks)], cwd=ROOT, env=env,
                                    receipt=receipt, name="execute every Lust generation assertion")
            generation_summary = test_summary(generation_result.stdout)
            if generation_summary["total"] != 5:
                raise RuntimeError("Lust did not execute all five generation assertion cases")
            receipt["generation_assertions"] = generation_summary
            receipt["generation_assertions_binary_sha256"] = sha256(generation_checks)
            receipt["compiler_test_harness_boundary"] = (
                "The pinned single-file test harness cannot import sibling crate modules. "
                "Five generation cases retain Cargo test coverage and run through an unchanged "
                "Lustr-built example importing the real core; they are counted separately from "
                "the test-harness unit cases."
            )
            print(generation_result.stdout, end="")
            # --bin selects each example by its Cargo target name in this
            # revision (select_unit's name fallback).
            for surface, example, oracle in (
                    ("permission", "permission_matrix", "check-permissions.mjs"),
                    ("connection", "connection_matrix", "check-connection.mjs"),
                    ("guest_share", "guest_share_matrix", "check-guest-share.mjs"),
                    ("project_commands", "project_commands_matrix", "check-project-commands.mjs"),
                    ("project_persistence", "project_persistence_matrix", "check-project-persistence.mjs"),
                    ("provider_recovery", "provider_recovery_matrix", "check-provider-recovery.mjs"),
                    ("json", "json_round_trip", "check-json.mjs")):
                executable = work / f"{surface}-matrix-lust"
                run([str(lust), "build", f"crates/core/examples/{example}.rs",
                     "--manifest-path", "crates/core/Cargo.toml", "--bin", example,
                     "-o", str(executable), "--json"], cwd=ROOT, env=env, receipt=receipt,
                    name=f"build unchanged {surface} matrix with Lust")
                receipt[f"{surface}_oracle_binary_sha256"] = sha256(executable)
                parity = run(["node", f"tools/{oracle}", str(executable)],
                             cwd=ROOT, env=env, receipt=receipt,
                             name=f"verify Lust {surface} against the independent oracle")
                receipt[f"{surface}_parity"] = parity.stdout.strip()
                print(parity.stdout, end="")
            executables = []
            for component, verifier in (("policy", "check-policy.mjs"),
                                        ("provider", "check-provider.mjs"),
                                        ("recovery", "check-recovery-runtime.mjs"),
                                        ("guest-share", "check-guest-share-runtime.mjs"),
                                        ("project", "check-project-runtime.mjs"),
                                        ("project-store", "check-project-persistence.mjs"),
                                        ("maintenance", "check-maintenance.mjs"),
                                        ("generate", "check-generation.mjs"),
                                        ("outbox", "check-outbox.mjs")):
                name = f"t3-code-ls-{component}"
                executable = work / name
                run([str(lust), "build", f"crates/core/src/bin/{name}.rs",
                     "--manifest-path", "crates/core/Cargo.toml", "--bin", name,
                     "-o", str(executable), "--json"], cwd=ROOT, env=env, receipt=receipt,
                    name=f"build {component} executable with Lust")
                receipt[f"{component}_binary_sha256"] = sha256(executable)
                verifier_args = (["--single"] if component == "project-store" else
                                 ["--policy-only"] if component == "outbox" else [])
                parity = run(["node", f"tools/{verifier}", str(executable), *verifier_args],
                             cwd=ROOT, env=env, receipt=receipt,
                             name=f"verify the Lust-built {component} executable")
                receipt[f"{component}_verification"] = parity.stdout.strip()
                print(parity.stdout, end="")
                if component == "outbox":
                    protocol = run(["node", "tools/check-outbox-protocol.mjs", str(executable)],
                                   cwd=ROOT, env=env, receipt=receipt,
                                   name="verify Lust outbox protocol and resource bounds")
                    receipt["outbox_protocol"] = protocol.stdout.strip()
                    print(protocol.stdout, end="")
                if component == "maintenance":
                    protocol = run(["node", "tools/check-maintenance-runtime.mjs", str(executable)],
                                   cwd=ROOT, env=env, receipt=receipt,
                                   name="verify Lust maintenance protocol and resource bounds")
                    receipt["maintenance_protocol"] = protocol.stdout.strip()
                    print(protocol.stdout, end="")
                if component == "generate":
                    protocol = run(["node", "tools/check-generation-runtime.mjs", str(executable)],
                                   cwd=ROOT, env=env, receipt=receipt,
                                   name="verify Lust generation protocol, resource bounds, and cleanup")
                    receipt["generation_protocol"] = protocol.stdout.strip()
                    print(protocol.stdout, end="")
                if component == "policy":
                    dispatch = run(["node", "tools/check-dispatch.mjs", str(executable)],
                                   cwd=ROOT, env=env, receipt=receipt,
                                   name="compare Lust message dispatch and goal policy to upstream")
                    receipt["dispatch_parity"] = dispatch.stdout.strip()
                    print(dispatch.stdout, end="")
                executables.append(executable)
                if component == "guest-share":
                    alias = work / "t3-code-ls-guest"
                    shutil.copy2(executable, alias)
                    alias_hash = sha256(alias)
                    if alias_hash != receipt[f"{component}_binary_sha256"]:
                        raise RuntimeError("Guest-share compatibility artifact differs from its verified executable")
                    receipt["guest_compatibility_alias"] = {
                        "file": alias.name, "alias_of": executable.name, "sha256": alias_hash,
                    }
                    executables.append(alias)

            if not args.toolchain:
                verify_source(source, pin["revision"])
            if source_hashes() != receipt["inputs"]:
                raise RuntimeError("Core or upstream oracle inputs changed during validation; run again")
            if args.artifacts:
                destination = args.artifacts.resolve()
                destination.mkdir(parents=True, exist_ok=True)
                receipt["artifacts"] = []
                for executable in executables:
                    target = destination / executable.name
                    shutil.copy2(executable, target)
                    receipt["artifacts"].append({"file": executable.name, "sha256": sha256(target)})
        receipt["status"] = "passed"
        total = sum(item["passed"] for item in receipt["unit_tests"])
        generation_total = receipt["generation_assertions"]["passed"]
        print(f"Lust check passed; {total} unit tests and {generation_total} generation assertion cases passed; all application oracles and runtime checks passed.")
        return 0
    except (RuntimeError, subprocess.SubprocessError, OSError, ValueError, KeyError) as error:
        receipt["error"] = str(error)
        print(f"Lust validation failed: {error}", file=sys.stderr)
        return 1
    finally:
        write_receipt(args.receipt.resolve(), receipt)
        print(f"Receipt: {args.receipt.resolve()}")


if __name__ == "__main__":
    raise SystemExit(main())
