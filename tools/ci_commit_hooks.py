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


class HookError(ValueError):
    pass


TERMINATION_GRACE_SECONDS = 10


def require(condition: bool, message: str):
    if not condition:
        raise HookError(message)


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


def verify_hooks(root: Path, output: Path, env: dict[str, str], timeout_seconds: float = 600) -> dict:
    root, output = root.resolve(), output.resolve()
    head, tree, parent = require_checkout(root, output, env)
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
    changed = git(root, "diff", "--name-only", "--no-renames", "-z", parent, head).stdout
    require(changed, "The candidate has no changes against its first parent")
    proof = {
        "schema_version": 1, "status": "running", "source_commit": head,
        "source_tree": tree, "first_parent": parent,
        "ci_run_id": env["GITHUB_RUN_ID"], "ci_run_attempt": env["GITHUB_RUN_ATTEMPT"],
        "command": ["git", "hook", "run", "pre-commit"],
        "timeout_seconds": timeout_seconds,
        "configured_knip_tasks": {name: scripts[name] for name in names},
        "changed_paths": [os.fsdecode(path) for path in changed.split(b"\0") if path],
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
        # remain the candidate, so the real hook sees its exact first-parent diff.
        moved = True
        git(root, "update-ref", "--no-deref", "HEAD", parent, head)
        git(root, "read-tree", tree)
        require(value(root, "write-tree") == tree, "Staged candidate identity changed")
        child_env = dict(env, VP_RUN_CONCURRENCY_LIMIT="1", GIT_TERMINAL_PROMPT="0")
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
            require_unchanged(proof, tree, parent)
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
    args = parser.parse_args(argv)
    def interrupted(signum, _frame):
        raise HookError(f"Hook verification interrupted by signal {signum}")
    previous = {number: signal.signal(number, interrupted) for number in (signal.SIGINT, signal.SIGTERM)}
    try:
        proof = verify_hooks(Path.cwd(), args.output, dict(os.environ))
        print(json.dumps(proof, indent=2))
        return 0
    except (HookError, OSError, ValueError, subprocess.SubprocessError) as error:
        print(f"Hosted commit hooks rejected: {error}", file=sys.stderr)
        return 1
    finally:
        for number, handler in previous.items():
            signal.signal(number, handler)


if __name__ == "__main__":
    sys.exit(main())
