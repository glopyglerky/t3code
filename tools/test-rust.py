#!/usr/bin/env python3
"""Run selected workspace packages with the verified produced compiler."""
import argparse
import json
import os
from pathlib import Path
import subprocess

from ci_plan import cargo_graph


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--packages", required=True, help="JSON array from the fork CI plan")
    args = parser.parse_args()
    root = Path(__file__).resolve().parent.parent
    packages = json.loads(args.packages)
    names, _ = cargo_graph(root, "HEAD")
    if (not isinstance(packages, list) or not packages
            or any(not isinstance(package, str) or package not in names.values() for package in packages)
            or len(set(packages)) != len(packages)):
        parser.error("Select unique, existing Cargo workspace package names")
    toolchain = Path(os.environ["T3_CODE_LS_TOOLCHAIN"]).resolve()
    command = [str(toolchain / "bin/lustr"), "--bluefe-root", str(toolchain),
               "test", "--locked", "--all-targets"]
    for package in sorted(packages):
        command += ["-p", package]
    subprocess.run(command, cwd=root, check=True)


if __name__ == "__main__":
    main()
