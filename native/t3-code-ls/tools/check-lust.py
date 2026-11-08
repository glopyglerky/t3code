#!/usr/bin/env python3
"""Build pinned Lust and verify application decisions and the provider process runtime.

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
REPO = ROOT.parents[1]
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
    files += [ROOT / "Cargo.toml", ROOT / "Cargo.lock", ROOT / "crates/core/Cargo.toml",
              ROOT / "tools/upstream-source.mjs",
              ROOT / "upstream.lock.json", Path(__file__).resolve(), PIN_FILE]
    files += [REPO / "apps/server/src/orchestration-v2/NativeDispatchFacts.ts",
              REPO / "apps/server/src/orchestration-v2/NativeDispatchPolicy.ts",
              REPO / "apps/server/src/orchestration-v2/NativeCommandPolicy.ts",
              REPO / "apps/server/src/orchestration-v2/CommandPolicy.ts",
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
                 "packages/contracts/src/guestShare.ts",
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
    parser.add_argument("--source", type=Path, required=True,
                        help="clean Lust checkout at lust-toolchain.json's exact revision")
    parser.add_argument("--receipt", type=Path, default=ROOT / "target/lust-proof.json")
    parser.add_argument("--artifacts", type=Path,
                        help="save the verified standalone Lust executables here on success")
    args = parser.parse_args()
    source = args.source.resolve()
    receipt = {"schema": 1, "status": "failed", "steps": [],
               "generated_at": datetime.now(timezone.utc).isoformat(),
               "scope": "Lust application decisions, JSON codec, and Unix provider process runtime; "
                        "full application, UI, mobile, and Iroh qualification excluded"}
    try:
        pin = json.loads(PIN_FILE.read_text())
        receipt["compiler"] = pin
        if platform.system() != "Linux" or platform.machine() not in ("x86_64", "amd64"):
            raise RuntimeError("This proof is qualified for Linux x86_64; other hosts require review")
        verify_source(source, pin["revision"])
        env = {key: value for key, value in os.environ.items() if not key.startswith("LUST_")}
        for command in ("cargo", "node", "bash"):
            if not shutil.which(command, path=env.get("PATH")):
                raise RuntimeError(f"{command} is required on PATH")
        receipt["workspace_git_revision"] = git(REPO, "rev-parse", "HEAD")
        receipt["inputs"] = source_hashes()
        run(["bash", "scripts/build-tcc.sh"], cwd=source, env=env,
            receipt=receipt, name="build pinned TinyCC")
        tinycc = source / "third_party/tinycc"
        if git(tinycc, "rev-parse", "HEAD") != pin["tinycc_revision"]:
            raise RuntimeError("TinyCC revision does not match lust-toolchain.json")
        if git(tinycc, "status", "--porcelain", "--untracked-files=normal"):
            raise RuntimeError("TinyCC source has local changes")
        run(["cargo", "build", "--locked", "--profile", pin["build_profile"],
             "--target-dir", str(source / "target")], cwd=source, env=env,
            receipt=receipt, name="build pinned Lust compiler")
        lust = source / "target" / pin["build_profile"] / "lust"
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
            # --bin selects each example by its Cargo target name in this
            # revision (select_unit's name fallback).
            for surface, example, oracle in (
                    ("permission", "permission_matrix", "check-permissions.mjs"),
                    ("connection", "connection_matrix", "check-connection.mjs"),
                    ("guest_share", "guest_share_matrix", "check-guest-share.mjs"),
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
            for component in ("policy", "provider"):
                name = f"t3-code-ls-{component}"
                executable = work / name
                run([str(lust), "build", f"crates/core/src/bin/{name}.rs",
                     "--manifest-path", "crates/core/Cargo.toml", "--bin", name,
                     "-o", str(executable), "--json"], cwd=ROOT, env=env, receipt=receipt,
                    name=f"build {component} executable with Lust")
                receipt[f"{component}_binary_sha256"] = sha256(executable)
                parity = run(["node", f"tools/check-{component}.mjs", str(executable)],
                             cwd=ROOT, env=env, receipt=receipt,
                             name=f"verify the Lust-built {component} executable")
                receipt[f"{component}_verification"] = parity.stdout.strip()
                print(parity.stdout, end="")
                if component == "policy":
                    dispatch = run(["node", "tools/check-dispatch.mjs", str(executable)],
                                   cwd=ROOT, env=env, receipt=receipt,
                                   name="compare Lust message dispatch and goal policy to upstream")
                    receipt["dispatch_parity"] = dispatch.stdout.strip()
                    print(dispatch.stdout, end="")
                executables.append(executable)

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
        print(f"Lust check passed; {total} unit tests passed; all application oracles and runtime checks passed.")
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
