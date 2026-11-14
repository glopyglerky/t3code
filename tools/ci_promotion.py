#!/usr/bin/env python3
"""Verify a qualified candidate and print a fresh, non-forced main update request."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import subprocess
import sys

from ci_history import GitHub, HistoryError, SHA, find_qualification, valid_receipt
from ci_plan import performance_reference_covers, policy_digest
from nightly import read_tree


class PromotionError(ValueError):
    pass


def require(condition: bool, message: str):
    if not condition:
        raise PromotionError(message)


def git(root: Path, *args: str) -> str:
    return subprocess.run(["git", "--no-replace-objects", "-C", str(root), *args], check=True,
                          capture_output=True, text=True, timeout=30).stdout.strip()


def main_head(client: GitHub) -> str:
    value = client.json("/git/ref/heads/main")
    require(isinstance(value, dict) and value.get("ref") == "refs/heads/main"
            and isinstance(value.get("object"), dict) and value["object"].get("type") == "commit",
            "GitHub did not return the main branch commit")
    head = value["object"].get("sha")
    require(isinstance(head, str) and bool(SHA.fullmatch(head)), "Invalid remote main SHA")
    return head


def verify_promotion(root: Path, repository: str, head: str, token: str | None,
                     expected_main: str | None = None) -> dict:
    require(isinstance(head, str) and bool(SHA.fullmatch(head)), "Candidate must be a full commit SHA")
    require(git(root, "rev-parse", "HEAD") == head, "Checkout must be at the candidate")
    require(not git(root, "status", "--porcelain", "--untracked-files=normal"),
            "Candidate checkout contains source changes or untracked files")
    client = GitHub(repository, token)
    before = main_head(client)
    require(expected_main is None or before == expected_main, "Remote main differs from the expected head")
    try:
        git(root, "merge-base", "--is-ancestor", before, head)
    except subprocess.CalledProcessError as error:
        raise PromotionError("Candidate is not a fast-forward from the current main; fetch and integrate it") from error
    source_tree = git(root, "rev-parse", f"{head}^{{tree}}")
    policy = policy_digest(read_tree(root, head))
    qualification = find_qualification(root, repository, head, policy, "", token)
    require(isinstance(qualification, dict) and qualification.get("commit") == head
            and qualification.get("reused") is True,
            "No completed successful exact-candidate qualification exists under the current policy")
    receipt = qualification.get("receipt")
    require(valid_receipt(receipt, head, source_tree, policy), "Qualification receipt is invalid")
    evidence = receipt.get("performance_evidence")
    require(isinstance(evidence, dict) and performance_reference_covers(root, head, evidence),
            "No current performance evidence covers this candidate; run full qualification")
    require(main_head(client) == before, "Main advanced during verification; fetch and verify again")
    return {
        "schema_version": 1, "status": "ready", "repository": repository,
        "candidate": head, "source_tree": source_tree, "policy_digest": policy,
        "expected_main": before, "qualification_run_id": qualification["run_id"],
        "qualification_url": f"https://github.com/{repository}/actions/runs/{qualification['run_id']}",
        "measurement_source_commit": evidence["source_commit"],
        "measurement_run_id": evidence["ci_run_id"],
        "measurement_run_attempt": evidence["ci_run_attempt"],
        "measurement_report_sha256": evidence["report_sha256"],
        "update_ref": {"repository_full_name": repository, "branch_name": "main",
                       "expected_sha": before, "sha": head, "force": False},
    }


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    parser.add_argument("--repository", default=os.environ.get("GITHUB_REPOSITORY", "glopyglerky/t3code-ls"))
    parser.add_argument("--candidate", required=True)
    parser.add_argument("--expected-main")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args(argv)
    try:
        proof = verify_promotion(args.repo_root, args.repository, args.candidate,
                                 os.environ.get("GITHUB_TOKEN"), args.expected_main)
        data = json.dumps(proof, indent=2) + "\n"
        if args.output:
            path = args.output.resolve()
            require(not path.is_relative_to(args.repo_root.resolve()),
                    "Promotion evidence must be written outside the checkout")
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(data)
        print(data, end="")
    except (PromotionError, HistoryError, OSError, ValueError, subprocess.SubprocessError) as error:
        print(f"Main promotion rejected: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
