#!/usr/bin/env python3
"""Run selected workspace packages with the selected, pinned Rust compiler."""
import argparse
import json
import os
from pathlib import Path
import subprocess

from ci_plan import cargo_graph
from pinned_rust import pinned_stock_environment


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
    route = os.environ.get("T3_CODE_LS_COMPILER", "rustc")
    env = os.environ.copy()
    if route == "rustc":
        try:
            env, _, _ = pinned_stock_environment(root, env)
        except RuntimeError as error:
            parser.error(str(error))
        command = [env["CARGO"], "test", "--locked", "--all-targets"]
    elif route == "lustr":
        toolchain = Path(os.environ["T3_CODE_LS_TOOLCHAIN"]).resolve()
        command = [str(toolchain / "bin/lustr"), "--bluefe-root", str(toolchain),
                   "test", "--locked", "--all-targets"]
    else:
        parser.error("T3_CODE_LS_COMPILER must be rustc or lustr")
    for package in sorted(packages):
        command += ["-p", package]
    subprocess.run(command, cwd=root, env=env, check=True)


if __name__ == "__main__":
    main()
