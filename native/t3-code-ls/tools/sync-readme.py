#!/usr/bin/env python3
"""Keep the upstream README bytes intact while preserving the fork's additions."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import sys
import tempfile

BEGIN = b"<!-- t3-code-ls:upstream-readme:start -->\n\n"
END = b"\n<!-- t3-code-ls:upstream-readme:end -->"
SHA = re.compile(r"[0-9a-f]{40}\Z")


class ReadmeError(Exception):
    pass


def git(repo: Path, *arguments: str) -> bytes:
    result = subprocess.run(
        ["git", "--no-replace-objects", "-C", str(repo), *arguments],
        check=False, capture_output=True, timeout=30,
    )
    if result.returncode:
        raise ReadmeError(
            "The pinned upstream Git object is unavailable or invalid. "
            "Fetch that exact commit before checking or updating the README."
        )
    return result.stdout


def source_readme(repo: Path) -> tuple[str, bytes]:
    lock = json.loads((repo / "native/t3-code-ls/upstream.lock.json").read_bytes())
    upstream = lock.get("upstream") if isinstance(lock, dict) else None
    if not isinstance(upstream, dict) or upstream.get("repository") != "pingdotgg/t3code":
        raise ReadmeError("The README source must be the pinned pingdotgg/t3code repository.")
    commit = upstream.get("commit")
    tree = upstream.get("root_tree")
    if not isinstance(commit, str) or not SHA.fullmatch(commit):
        raise ReadmeError("The upstream commit must be a full immutable lowercase Git SHA.")
    if not isinstance(tree, str) or not SHA.fullmatch(tree):
        raise ReadmeError("The upstream root tree must be a full lowercase Git SHA.")
    if git(repo, "cat-file", "-t", commit).strip() != b"commit":
        raise ReadmeError("The pinned upstream object must be a commit.")
    if git(repo, "rev-parse", f"{commit}^{{tree}}").strip().decode("ascii") != tree:
        raise ReadmeError("The pinned upstream commit and root tree disagree.")
    source = git(repo, "cat-file", "blob", f"{commit}:README.md")
    if BEGIN in source or END in source:
        raise ReadmeError("Upstream contains the fork's README markers; review the boundary manually.")
    return commit, source


def split_readme(document: bytes) -> tuple[bytes, bytes, bytes]:
    if document.count(BEGIN) != 1 or document.count(END) != 1:
        raise ReadmeError("The README must contain exactly one ordered pair of upstream markers.")
    start = document.index(BEGIN) + len(BEGIN)
    end = document.index(END)
    if end < start:
        raise ReadmeError("The README's upstream markers are reversed.")
    return document[:start], document[start:end], document[end:]


def sync_readme(repo: Path, write: bool = False) -> dict:
    commit, source = source_readme(repo)
    path = repo / "README.md"
    mode = path.lstat().st_mode
    if not stat.S_ISREG(mode):
        raise ReadmeError("README.md must be a regular file.")
    original = path.read_bytes()
    prefix, current, suffix = split_readme(original)
    changed = current != source
    if changed and not write:
        raise ReadmeError(
            "The upstream README section differs from the pinned source. "
            "Run tools/sync-readme.py --write from native/t3-code-ls to restore it."
        )
    if changed:
        temporary = None
        try:
            with tempfile.NamedTemporaryFile(dir=repo, prefix=".readme-", delete=False) as stream:
                temporary = Path(stream.name)
                stream.write(prefix + source + suffix)
                stream.flush()
                os.fsync(stream.fileno())
            temporary.chmod(stat.S_IMODE(mode))
            if path.is_symlink() or path.read_bytes() != original:
                raise ReadmeError("README.md changed during synchronization; no edits were saved.")
            os.replace(temporary, path)
        finally:
            if temporary is not None:
                temporary.unlink(missing_ok=True)
    return {
        "status": "updated" if changed else "matched",
        "upstream_commit": commit,
        "upstream_readme_sha256": hashlib.sha256(source).hexdigest(),
        "upstream_bytes": len(source),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=Path(__file__).resolve().parents[3])
    parser.add_argument("--write", action="store_true", help="Restore only the managed upstream section.")
    arguments = parser.parse_args()
    try:
        result = sync_readme(arguments.repo_root.resolve(), arguments.write)
    except (ReadmeError, OSError, ValueError, subprocess.TimeoutExpired) as error:
        print(f"README synchronization failed: {error}", file=sys.stderr)
        return 1
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
