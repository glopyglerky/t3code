"""Select pinned stock Rust without hiding an incompatible caller selection."""
from pathlib import Path
import re
import subprocess
import tomllib


def stock_rust_environment(env: dict[str, str], tools: dict[str, str],
                           sysroot: str, release: str, target: str) -> dict[str, str]:
    if not re.fullmatch(r"\d+\.\d+\.\d+", release):
        raise RuntimeError("Stock Rust requires an exact pinned release")
    if env.get("T3_CODE_LS_COMPILER", "rustc") != "rustc":
        raise RuntimeError("Stock Rust conflicts with the explicit compiler route")
    for variable in ("T3_CODE_LS_TOOLCHAIN", "T3_CODE_LS_SYSROOT"):
        if variable in env:
            raise RuntimeError(f"{variable} conflicts with the pinned stock Rust route")
    for variable in ("RUSTC_WRAPPER", "RUSTC_WORKSPACE_WRAPPER",
                     "CARGO_BUILD_RUSTC_WRAPPER", "CARGO_BUILD_RUSTC_WORKSPACE_WRAPPER"):
        if env.get(variable):
            raise RuntimeError(f"{variable} conflicts with the verified compiler route")
    for variable, selected in tools.items():
        if env.get(variable) and Path(env[variable]).resolve() != Path(selected).resolve():
            raise RuntimeError(f"{variable} conflicts with the pinned stock Rust route")
    if not isinstance(target, str) or not target:
        raise RuntimeError("The selected Rust target or compiler host is required")
    target_name = Path(target).stem if target.endswith(".json") else target
    target_flags = f"CARGO_TARGET_{target_name.upper().replace('-', '_')}_RUSTFLAGS"
    plain = env.get("RUSTFLAGS", env.get(target_flags, env.get("CARGO_BUILD_RUSTFLAGS", "")))
    encoded = env.get("CARGO_ENCODED_RUSTFLAGS")
    flags = ((encoded.split("\x1f") if encoded else []) if encoded is not None
             else plain.split())
    selected = False
    index = 0
    while index < len(flags):
        flag = flags[index]
        value = None
        if flag == "--sysroot":
            index += 1
            if index == len(flags):
                raise RuntimeError("Missing --sysroot value")
            value = flags[index]
        elif flag.startswith("--sysroot="):
            value = flag[len("--sysroot="):]
        if value is not None:
            if selected or not value or Path(value).resolve() != Path(sysroot).resolve():
                raise RuntimeError("Conflicting or repeated explicit sysroot selection")
            selected = True
        index += 1
    # Keep both flag variables and the caller's target exactly as supplied. A
    # synthetic encoded value would mask Cargo's per-target flags/configuration.
    return env | tools | {"RUSTUP_TOOLCHAIN": release}


def pinned_stock_environment(root: Path, env: dict[str, str]) -> tuple[dict[str, str], str, str]:
    release = tomllib.loads((root / "rust-toolchain.toml").read_text())["toolchain"]["channel"]
    if not re.fullmatch(r"\d+\.\d+\.\d+", release):
        raise RuntimeError("Stock Rust requires an exact pinned release")

    def output(command: list[str]) -> str:
        return subprocess.check_output(command, cwd=root, env=env, text=True).strip()

    tools = {variable: output(["rustup", "which", "--toolchain", release, binary])
             for variable, binary in (("CARGO", "cargo"), ("RUSTC", "rustc"), ("RUSTDOC", "rustdoc"))}
    identity = output([tools["RUSTC"], "-vV"])
    if f"release: {release}" not in identity.splitlines():
        raise RuntimeError("Installed rustc does not match rust-toolchain.toml")
    sysroot = output([tools["RUSTC"], "--print", "sysroot"])
    if not sysroot:
        raise RuntimeError("The pinned compiler did not report its sysroot")
    host = next((line.removeprefix("host: ") for line in identity.splitlines()
                 if line.startswith("host: ")), "")
    target = env.get("CARGO_BUILD_TARGET") or host
    return stock_rust_environment(env, tools, sysroot, release, target), identity, release
