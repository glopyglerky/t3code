#!/usr/bin/env python3
"""Write qualification evidence only after every planned fork gate succeeds."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile

from ci_history import DIGEST, GATES, MAX_BYTES, SHA, valid_receipt
from ci_performance import MAX_REPORT_BYTES, validate_report, valid_summary


MODES = {"full-fork", "affected-fork", "reused"}
SELECTOR = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]*\Z")
PLAN_FIELDS = ("head", "gates", "mode", "base", "base_run_id", "reused_run_id",
               "rust_packages", "adapter_ports", "component_tests", "upstream_commit", "upstream_tree")


class ReceiptError(ValueError):
    pass


def require(condition: bool, message: str):
    if not condition:
        raise ReceiptError(message)


def sha(value: object) -> bool:
    return isinstance(value, str) and bool(SHA.fullmatch(value))


def positive_id(value: object) -> bool:
    return type(value) is int and value > 0


def selectors(value: object) -> bool:
    return (isinstance(value, list)
            and all(isinstance(item, str) and SELECTOR.fullmatch(item) for item in value)
            and len(value) == len(set(value)))


def job_results(needs: object) -> dict[str, str]:
    """Reject failed jobs before attempting to read their missing artifacts."""
    require(isinstance(needs, dict) and set(needs) == {"plan", *GATES},
            "CI_NEEDS must contain exactly plan and every qualification gate")
    results = {}
    for job in ("plan", *GATES):
        entry = needs[job]
        require(isinstance(entry, dict), f"Missing result for {job}")
        result = entry.get("result")
        require(result in ("success", "skipped"), f"Job {job} did not succeed or skip ({result!r})")
        results[job] = result
    require(results["plan"] == "success", "Required job plan did not succeed")
    return results


def make_receipt(plan: object, needs: object, head: str, run_id: str | None = None,
                 performance: tuple[object, bytes] | None = None, run_attempt: str = "1") -> dict:
    """Validate plan/results together, retaining only compact qualification data."""
    require(sha(head), "CI_HEAD must be a full commit SHA")
    require(isinstance(plan, dict), "CI plan must be an object")
    require(type(plan.get("schema_version")) is int and plan["schema_version"] == 1,
            "Unsupported CI plan schema")
    require(plan.get("head") == head, "CI plan does not describe CI_HEAD")
    require(sha(plan.get("source_tree")), "CI plan must identify its source tree")
    require(isinstance(plan.get("policy_digest"), str)
            and bool(DIGEST.fullmatch(plan["policy_digest"])), "Invalid CI policy digest")
    require(all(key in plan for key in PLAN_FIELDS), "CI plan is missing qualification fields")
    mode = plan["mode"]
    require(isinstance(mode, str) and mode in MODES, "Unknown CI plan mode")
    gates = plan["gates"]
    require(isinstance(gates, dict) and set(gates) == set(GATES)
            and all(type(value) is bool for value in gates.values()),
            "CI plan must contain exactly four boolean gates")
    require(selectors(plan["rust_packages"]) and selectors(plan["adapter_ports"]),
            "Invalid package or adapter selection")
    require(gates["native"] == bool(plan["rust_packages"]),
            "Native gate and package selection disagree")
    require(gates["adapters"] == bool(plan["adapter_ports"]),
            "Adapter gate and port selection disagree")
    component_tests = plan["component_tests"]
    require(isinstance(component_tests, list)
            and all(isinstance(path, str) and re.fullmatch(r"tools/[A-Za-z0-9_-]+\.mjs", path)
                    for path in component_tests)
            and len(set(component_tests)) == len(component_tests),
            "Invalid component test selection")
    require(not component_tests or gates["native"], "Component tests require the native gate")
    stage_native = plan.get("stage_native")
    require(type(stage_native) is bool
            and stage_native == (gates["adapters"] or gates["application"] or gates["performance"]),
            "Native staging and integration gates disagree")
    require(not stage_native or gates["native"], "Integration gates require native binaries")
    require(not gates["application"] or gates["performance"],
            "Application boundary changes require paired performance and reliability evidence")
    require(sha(plan["upstream_commit"]) and sha(plan["upstream_tree"]),
            "CI plan must identify the immutable stock comparison")

    base, base_id, reused_id = (plan[key] for key in ("base", "base_run_id", "reused_run_id"))
    require((base is None and base_id is None) or (sha(base) and positive_id(base_id)),
            "Qualification base and run must be recorded together")
    if mode == "reused":
        require(base == head and positive_id(reused_id) and reused_id == base_id,
                "Reused qualification must identify the same source and prior run")
        require(not any(gates.values()), "Reused qualification cannot schedule gates")
    else:
        require(reused_id is None, "Only reused qualification may identify a reused run")
        if mode == "affected-fork":
            require(base is not None, "Incremental qualification requires a qualified base")
        if mode == "full-fork":
            require(all(gates.values()), "Full qualification must require every gate")

    results = {}
    for job, result in job_results(needs).items():
        if job == "plan" or gates[job]:
            require(result == "success", f"Required job {job} did not succeed")
        if job != "plan":
            results[job] = result

    output = {
        "schema_version": 1, "source_commit": head, "source_tree": plan["source_tree"],
        "policy_digest": plan["policy_digest"], "status": "passed",
        "plan": {key: plan[key] for key in PLAN_FIELDS}, "gate_results": results,
    }
    if run_id:
        require(bool(re.fullmatch(r"[1-9][0-9]*", run_id)), "CI_RUN_ID must be a positive run ID")
        identifier = int(run_id)
        require(base_id != identifier, "A qualification run cannot reference itself as its base")
        require(isinstance(run_attempt, str) and bool(re.fullmatch(r"[1-9][0-9]*", run_attempt)),
                "CI_RUN_ATTEMPT must be a positive attempt ID")
        output["run_id"] = identifier
        output["run_attempt"] = int(run_attempt)
    if gates["performance"]:
        require(performance is not None and run_id is not None,
                "The required performance artifact and current run ID are missing")
        outputs = needs["performance"].get("outputs", {})
        require(isinstance(outputs, dict), "Missing performance job outputs")
        measured_attempt = outputs.get("proof_run_attempt")
        require(isinstance(measured_attempt, str) and bool(re.fullmatch(r"[1-9][0-9]*", measured_attempt))
                and int(measured_attempt) <= int(run_attempt),
                "Missing or future performance job attempt")
        output["performance_run_attempt"] = int(measured_attempt)
        output["performance_evidence"] = validate_report(
            performance[0], performance[1], plan, run_id, outputs.get("proof_sha256"), measured_attempt)
    elif plan.get("performance_reference") is not None:
        evidence = plan["performance_reference"]
        require(mode in ("reused", "affected-fork") and base is not None and isinstance(evidence, dict)
                and valid_summary(evidence, evidence.get("source_commit"), evidence.get("source_tree"), plan["policy_digest"])
                and evidence["upstream_commit"] == plan["upstream_commit"]
                and evidence["upstream_tree"] == plan["upstream_tree"],
                "Prior performance evidence must match the verified source policy and immutable stock revision")
        output["performance_evidence"] = evidence
    require(valid_receipt(output, head, plan["source_tree"], plan["policy_digest"]),
            "Generated qualification receipt is invalid")
    return output


def verify_checkout(head: str, tree: str):
    def git(*args: str) -> str:
        return subprocess.run(["git", "--no-replace-objects", *args], check=True,
                              capture_output=True, text=True, timeout=10).stdout.strip()

    require(git("rev-parse", "HEAD") == head, "Checkout does not match CI_HEAD")
    require(git("rev-parse", "HEAD^{tree}") == tree, "Checkout tree does not match the CI plan")
    try:
        git("diff", "--quiet", "--no-ext-diff", "HEAD", "--")
    except subprocess.CalledProcessError as error:
        if error.returncode == 1:
            raise ReceiptError("Checkout contains tracked source changes") from error
        raise


def write_receipt(path: Path, receipt: dict):
    encoded = (json.dumps(receipt, sort_keys=True, separators=(",", ":")) + "\n").encode()
    require(len(encoded) <= MAX_BYTES, "Qualification receipt exceeds its size limit")
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(dir=path.parent, prefix=".qualification-", delete=False) as file:
            temporary = Path(file.name)
            file.write(encoded)
        temporary.replace(path)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plan", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--performance", type=Path)
    args = parser.parse_args(argv)
    try:
        needs = json.loads(os.environ.get("CI_NEEDS", ""))
        job_results(needs)
        plan = json.loads(args.plan.read_bytes())
        performance = None
        if args.performance:
            with args.performance.open("rb") as source:
                raw = source.read(MAX_REPORT_BYTES + 1)
            require(len(raw) <= MAX_REPORT_BYTES, "Performance report exceeds its size bound")
            performance = (json.loads(raw), raw)
        receipt = make_receipt(plan, needs, os.environ.get("CI_HEAD", ""),
                               os.environ.get("CI_RUN_ID"), performance, os.environ.get("CI_RUN_ATTEMPT", "1"))
        verify_checkout(receipt["source_commit"], receipt["source_tree"])
        evidence = receipt.get("performance_evidence")
        if evidence is not None and evidence["source_commit"] != receipt["source_commit"]:
            from ci_plan import performance_reference_covers
            require(performance_reference_covers(Path.cwd(), receipt["source_commit"], evidence),
                    "Runtime, oracle or measurement changes make prior performance evidence stale")
        write_receipt(args.output, receipt)
    except (ValueError, OSError, subprocess.SubprocessError) as error:
        print(f"Qualification receipt rejected: {error}", file=sys.stderr)
        return 1
    print(f"Qualified {receipt['source_commit']} ({receipt['plan']['mode']})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
