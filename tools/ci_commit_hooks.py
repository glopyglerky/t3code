#!/usr/bin/env python3
"""Run the normal pre-commit hook on an exact disposable Actions candidate."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import time

import nightly
from ci_history import find_qualification
from ci_plan import policy_digest


class HookError(ValueError):
    pass


TERMINATION_GRACE_SECONDS = 10


def require(condition: bool, message: str):
    if not condition:
        raise HookError(message)


def immutable_sha(value: object) -> str:
    require(isinstance(value, str) and re.fullmatch(r"[a-f0-9]{40}", value) is not None,
            "A hook source reference must be an immutable 40-hex commit or tree")
    return value


def git(root: Path, *args: str, check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(["git", "--no-replace-objects", "-C", str(root), *args],
                          check=check, capture_output=True, timeout=30)


def value(root: Path, *args: str) -> str:
    return git(root, *args).stdout.decode().strip()


def status(root: Path) -> bytes:
    return git(root, "status", "--porcelain=v1", "--untracked-files=all").stdout


def digest(path: Path) -> dict:
    data = path.read_bytes()
    return {"path": str(path), "bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()}


def hook_baseline(root: Path, plan_path: Path, env: dict[str, str], head: str, tree: str) -> dict:
    """Cover inherited fork changes, using the same verified baseline as CI."""
    plan_path = plan_path.resolve()
    require(plan_path.is_relative_to(Path(env["RUNNER_TEMP"]).resolve())
            and not plan_path.is_relative_to(root), "The CI plan must be outside the checkout in RUNNER_TEMP")
    with plan_path.open("rb") as source:
        raw = source.read(2 * 1024 * 1024 + 1)
    require(len(raw) <= 2 * 1024 * 1024, "The CI plan exceeds its bound")
    plan = json.loads(raw)
    require(isinstance(plan, dict) and type(plan.get("schema_version")) is int
            and plan["schema_version"] == 1, "Unsupported CI plan")
    require(plan.get("head") == head and plan.get("source_tree") == tree,
            "The CI plan must bind the exact candidate source")
    repository = env.get("GITHUB_REPOSITORY", "")
    require(re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repository) is not None
            and plan.get("ci_repository") == repository,
            "The CI plan must bind the current repository")
    require(plan.get("ci_run_id") == env["GITHUB_RUN_ID"]
            and isinstance(plan.get("ci_run_attempt"), str)
            and re.fullmatch(r"[1-9][0-9]*", plan["ci_run_attempt"]) is not None
            and int(plan["ci_run_attempt"]) <= int(env["GITHUB_RUN_ATTEMPT"]),
            "The CI plan must come from this run and an existing producing attempt")
    current_policy = policy_digest(nightly.read_tree(root, head))
    require(plan.get("policy_digest") == current_policy, "The CI plan has a stale policy")
    lock = json.loads(git(root, "show", f"{head}:upstream.lock.json").stdout)
    require(isinstance(lock, dict) and isinstance(lock.get("upstream"), dict),
            "The committed upstream reference is malformed")
    upstream = lock.get("upstream", {})
    require(upstream.get("repository") == "pingdotgg/t3code", "Untrusted upstream repository")
    upstream_commit = immutable_sha(upstream.get("commit"))
    upstream_tree = immutable_sha(upstream.get("root_tree"))
    require(plan.get("upstream_commit") == upstream_commit
            and plan.get("upstream_tree") == upstream_tree,
            "The CI plan has a different immutable upstream reference")
    base = plan.get("base")
    provenance = None
    if base is not None:
        base = immutable_sha(base)
        require(type(plan.get("base_run_id")) is int and plan["base_run_id"] > 0,
                "A qualified hook baseline requires its producing run")
        provenance = find_qualification(
            root, repository, head, current_policy, env["GITHUB_RUN_ID"], env.get("GITHUB_TOKEN"),
            expected_run_id=plan["base_run_id"], expected_commit=base, fetch_missing=True)
        require(provenance is not None and provenance.get("commit") == base
                and provenance.get("run_id") == plan["base_run_id"],
                "The selected qualified ancestor could not be revalidated")
        reason = "verified-qualified-ancestor"
    else:
        require(plan.get("base_run_id") is None, "An absent qualified baseline cannot name a run")
        base = upstream_commit
        if git(root, "cat-file", "-e", f"{base}^{{commit}}", check=False).returncode:
            git(root, "fetch", "--no-tags", "--no-write-fetch-head",
                "https://github.com/pingdotgg/t3code.git", base)
        require(value(root, "rev-parse", f"{base}^{{tree}}") == upstream_tree,
                "The trusted upstream commit and tree disagree")
        reason = "all-fork-differences-from-trusted-upstream"
    require(git(root, "merge-base", "--is-ancestor", base, head, check=False).returncode == 0,
            "The hook baseline must be an ancestor of the candidate")
    changed = git(root, "diff", "--name-only", "--no-renames", "-z", base, head).stdout
    require(changed, "The candidate has no fork changes against its hook baseline")
    return {
        "hook_base": base, "hook_base_tree": value(root, "rev-parse", f"{base}^{{tree}}"),
        "hook_base_reason": reason, "qualified_base_run_id": plan.get("base_run_id"),
        "qualified_base_receipt_sha256": (hashlib.sha256(json.dumps(
            provenance["receipt"], sort_keys=True, separators=(",", ":")).encode()).hexdigest()
            if provenance is not None else None),
        "ci_plan": {"path": str(plan_path), "bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest(),
                    "producing_run_attempt": plan["ci_run_attempt"]},
        "policy_digest": current_policy, "upstream_commit": upstream_commit,
        "upstream_tree": upstream_tree,
        "changed_paths": [os.fsdecode(path) for path in changed.split(b"\0") if path],
        "changed_paths_sha256": hashlib.sha256(changed).hexdigest(),
    }


def require_checkout(root: Path, output: Path, env: dict[str, str]) -> tuple[str, str, str]:
    require(env.get("GITHUB_ACTIONS") == "true" and env.get("CI") == "true",
            "Hook verification requires a disposable GitHub Actions checkout")
    require(env.get("RUNNER_ENVIRONMENT") == "github-hosted",
            "Hook verification requires a disposable GitHub-hosted runner")
    require(env.get("GITHUB_WORKSPACE") and Path(env["GITHUB_WORKSPACE"]).resolve() == root,
            "The checkout must be the exact GITHUB_WORKSPACE")
    require(env.get("RUNNER_TEMP") and Path(env["RUNNER_TEMP"]).is_absolute(),
            "RUNNER_TEMP must identify the disposable runner directory")
    temporary = Path(env["RUNNER_TEMP"]).resolve()
    require(not temporary.is_relative_to(root) and output.is_relative_to(temporary),
            "Hook evidence must be inside RUNNER_TEMP and outside the checkout")
    require(all(re.fullmatch(r"[1-9][0-9]*", env.get(key, ""))
                for key in ("GITHUB_RUN_ID", "GITHUB_RUN_ATTEMPT")),
            "The Actions run and attempt must be positive identifiers")
    require(all(env.get(key) != "0" for key in ("HUSKY", "VP_GIT_HOOKS", "VITE_GIT_HOOKS")),
            "The normal hook must not be disabled")
    require(not any(env.get(key) for key in ("GIT_DIR", "GIT_WORK_TREE", "GIT_INDEX_FILE")),
            "External Git directory or index overrides are not allowed")
    require(Path(value(root, "rev-parse", "--show-toplevel")).resolve() == root,
            "Run from the root of the disposable checkout")
    require((root / ".git").is_dir() and not (root / ".git").is_symlink(),
            "Linked worktrees are not disposable Actions checkouts")
    require(git(root, "symbolic-ref", "-q", "HEAD", check=False).returncode == 1,
            "The Actions checkout must have a detached HEAD")
    require(not status(root), "The candidate checkout must be clean, including untracked files")
    head = value(root, "rev-parse", "HEAD")
    require(env.get("GITHUB_SHA") == head, "HEAD must be the exact Actions candidate SHA")
    require(not (root / ".git" / "MERGE_HEAD").exists(), "The checkout cannot have a pending merge")
    tree = value(root, "rev-parse", "HEAD^{tree}")
    require(value(root, "write-tree") == tree, "The candidate index differs from HEAD")
    parent = value(root, "rev-parse", "HEAD^1")
    hooks = value(root, "config", "--path", "--get", "core.hooksPath")
    require((root / hooks).resolve() == root / ".vite-hooks" / "_",
            "The repository's installed Vite hook path is required")
    for path in (root / ".vite-hooks" / "_" / "pre-commit", root / ".vite-hooks" / "pre-commit"):
        require(path.is_file(), f"The normal pre-commit hook is missing: {path.name}")
    require(os.access(root / ".vite-hooks" / "_" / "pre-commit", os.X_OK),
            "The installed pre-commit hook must be executable")
    require((root / ".vite-hooks" / "pre-commit").read_bytes()
            == git(root, "show", "HEAD:.vite-hooks/pre-commit").stdout,
            "The normal hook must match the candidate source")
    config_home = Path(env.get("XDG_CONFIG_HOME", str(Path(env.get("HOME", "")) / ".config")))
    require(not any((config_home / path).exists()
                    for path in ("vite-plus/hooks-init.sh", "husky/init.sh")),
            "A clean runner cannot have an external hook initialization script")
    return head, tree, parent


def run_owned(command: list[str], root: Path, env: dict[str, str], stdout, stderr,
              timeout_seconds: float) -> dict:
    child = subprocess.Popen(command, cwd=root, env=env, stdout=stdout, stderr=stderr,
                             start_new_session=True)

    def stop():
        try:
            os.killpg(child.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        try:
            child.wait(timeout=TERMINATION_GRACE_SECONDS)
        except subprocess.TimeoutExpired:
            pass
        finally:
            # The direct child can exit before one of its descendants. Terminate
            # any remainder of this owned group before restoring the checkout.
            try:
                os.killpg(child.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            child.wait(timeout=TERMINATION_GRACE_SECONDS)

    try:
        code = child.wait(timeout=timeout_seconds)
        try:
            os.killpg(child.pid, 0)
        except ProcessLookupError:
            return {"exit_code": code, "timed_out": False}
        stop()
        return {"exit_code": code, "timed_out": False, "leftover_processes": True}
    except subprocess.TimeoutExpired:
        stop()
        return {"exit_code": child.returncode, "timed_out": True}
    except HookError as failure:
        stop()
        return {"exit_code": child.returncode, "timed_out": False, "interrupted": str(failure)}
    except BaseException:
        stop()
        raise


def restore(root: Path, head: str, tree: str) -> dict:
    # Preflight required no untracked source. Only files created by this owned
    # hook invocation can occur here; ignored dependency/build files stay intact.
    errors = []
    try:
        created = git(root, "ls-files", "--others", "--exclude-standard", "-z").stdout
        for raw in created.split(b"\0"):
            if not raw:
                continue
            relative = Path(os.fsdecode(raw))
            try:
                require(not relative.is_absolute() and ".." not in relative.parts,
                        "Hook created an invalid source path")
                path = root / relative
                require(path.parent.resolve().is_relative_to(root), "Hook source cleanup left the checkout")
                path.unlink()
            except (HookError, OSError) as failure:
                errors.append(str(failure))
    except (HookError, OSError, subprocess.SubprocessError) as failure:
        errors.append(str(failure))
    for command in (("read-tree", "--reset", "-u", head),
                    ("update-ref", "--no-deref", "HEAD", head)):
        try:
            git(root, *command)
        except (HookError, OSError, subprocess.SubprocessError) as failure:
            errors.append(str(failure))
    result = {"head": value(root, "rev-parse", "HEAD"), "index_tree": value(root, "write-tree"),
              "detached": git(root, "symbolic-ref", "-q", "HEAD", check=False).returncode == 1,
              "clean": not status(root)}
    require(not errors and result == {"head": head, "index_tree": tree, "detached": True, "clean": True},
            "The exact candidate checkout was not restored" + (": " + "; ".join(errors) if errors else ""))
    return result


def observe(root: Path) -> dict:
    return {
        "observed_head": value(root, "rev-parse", "HEAD"),
        "observed_index_tree": value(root, "write-tree"),
        "unstaged_changes": git(root, "diff", "--name-only", "-z").stdout.decode().split("\0")[:-1],
        "new_source_files": git(root, "ls-files", "--others", "--exclude-standard", "-z").stdout.decode().split("\0")[:-1],
    }


def require_unchanged(proof: dict, tree: str, parent: str):
    require(proof["observed_index_tree"] == tree and not proof["unstaged_changes"]
            and not proof["new_source_files"],
            "The hook changed the candidate source; commit the correction and requalify")
    require(proof["observed_head"] == parent, "The hook changed the temporary detached HEAD")


def verify_hooks(root: Path, output: Path, env: dict[str, str], plan_path: Path,
                 timeout_seconds: float = 600) -> dict:
    root, output = root.resolve(), output.resolve()
    head, tree, parent = require_checkout(root, output, env)
    baseline = hook_baseline(root, plan_path, env, head, tree)
    hook_base = baseline["hook_base"]
    output.parent.mkdir(parents=True, exist_ok=True)
    stdout_path = output.with_suffix(".stdout.log")
    stderr_path = output.with_suffix(".stderr.log")
    require(not any(path.exists() for path in (output, stdout_path, stderr_path)),
            "Hook evidence paths must be new")
    package = json.loads((root / "package.json").read_text())
    scripts = package.get("scripts", {})
    names = ("knip:check", "knip:check:files", "knip:check:exports")
    require(all(isinstance(scripts.get(name), str) and scripts[name] for name in names),
            "Both normal Knip scans and their task must be configured")
    vp = root / "node_modules" / ".bin" / "vp"
    require(vp.is_file() and os.access(vp, os.X_OK), "The frozen workspace Vite runner is missing")
    proof = {
        "schema_version": 1, "status": "running", "source_commit": head,
        "source_tree": tree, "first_parent": parent,
        "ci_run_id": env["GITHUB_RUN_ID"], "ci_run_attempt": env["GITHUB_RUN_ATTEMPT"],
        "command": ["git", "hook", "run", "pre-commit"],
        "timeout_seconds": timeout_seconds,
        "configured_knip_tasks": {name: scripts[name] for name in names},
        **baseline,
        "hook_source": digest(root / ".vite-hooks" / "pre-commit"),
        "task_config": digest(root / "vite.config.ts"),
        "source_clean_before": True,
    }
    output.write_text(json.dumps(proof, indent=2) + "\n")
    moved = False
    error = None
    start = time.monotonic()
    try:
        # This detached runner HEAD is temporary. The index and working source
        # remain the candidate, so the real hook also sees unqualified inherited
        # fork changes, even when this commit only changes an unrelated file.
        moved = True
        git(root, "update-ref", "--no-deref", "HEAD", hook_base, head)
        git(root, "read-tree", tree)
        require(value(root, "write-tree") == tree, "Staged candidate identity changed")
        child_env = dict(env, VP_RUN_CONCURRENCY_LIMIT="1", GIT_TERMINAL_PROMPT="0")
        child_env.pop("GITHUB_TOKEN", None)
        with stdout_path.open("xb") as stdout, stderr_path.open("xb") as stderr:
            proof["hook_result"] = run_owned(
                ["git", "--no-replace-objects", "hook", "run", "pre-commit"],
                root, child_env, stdout, stderr, timeout_seconds)
            require(not proof["hook_result"].get("interrupted"), proof["hook_result"].get("interrupted", ""))
            require(not proof["hook_result"].get("leftover_processes"),
                    "The normal hook left its owned process group running")
            require(not proof["hook_result"]["timed_out"], "The normal pre-commit hook timed out")
            require(proof["hook_result"]["exit_code"] == 0,
                    f"The normal pre-commit hook failed with status {proof['hook_result']['exit_code']}")
            proof.update(observe(root))
            require_unchanged(proof, tree, hook_base)
            require(digest(plan_path)["sha256"] == baseline["ci_plan"]["sha256"],
                    "The CI plan changed during hook execution")
        proof["status"] = "passed"
    except (HookError, OSError, ValueError, subprocess.SubprocessError) as failure:
        error = failure
        proof["status"] = "failed"
        proof["error"] = str(failure)
    except BaseException as failure:
        proof["status"] = "failed"
        proof["error"] = f"Hook verification interrupted: {type(failure).__name__}"
        raise
    finally:
        try:
            proof["elapsed_seconds"] = round(time.monotonic() - start, 3)
            for key, path in (("stdout", stdout_path), ("stderr", stderr_path)):
                if path.exists():
                    proof[key] = digest(path)
            if moved:
                proof.update(observe(root))
        except (HookError, OSError, ValueError, subprocess.SubprocessError) as failure:
            error = failure
            proof["status"] = "failed"
            proof["evidence_error"] = str(failure)
        finally:
            if moved:
                try:
                    proof["restoration"] = restore(root, head, tree)
                except (HookError, OSError, ValueError, subprocess.SubprocessError) as failure:
                    error = failure
                    proof["status"] = "failed"
                    proof["restoration_error"] = str(failure)
        output.write_text(json.dumps(proof, indent=2) + "\n")
    if error is not None:
        raise HookError(str(error)) from error
    return proof


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--plan", type=Path, required=True)
    args = parser.parse_args(argv)
    def interrupted(signum, _frame):
        raise HookError(f"Hook verification interrupted by signal {signum}")
    previous = {number: signal.signal(number, interrupted) for number in (signal.SIGINT, signal.SIGTERM)}
    try:
        proof = verify_hooks(Path.cwd(), args.output, dict(os.environ), args.plan)
        print(json.dumps(proof, indent=2))
        return 0
    except (HookError, OSError, ValueError, subprocess.SubprocessError, nightly.TrackingError) as error:
        print(f"Hosted commit hooks rejected: {error}", file=sys.stderr)
        return 1
    finally:
        for number, handler in previous.items():
            signal.signal(number, handler)


if __name__ == "__main__":
    sys.exit(main())
