mod migration;

use serde_json::Value;
use std::{
    env,
    error::Error,
    ffi::OsStr,
    fs,
    path::{Path, PathBuf},
    process::{Command, Stdio},
};
type Result<T> = std::result::Result<T, Box<dyn Error>>;

fn root() -> PathBuf {
    let current = env::current_dir().expect("read current directory");
    current
        .ancestors()
        .find(|directory| directory.join("upstream.lock.json").is_file())
        .unwrap_or(&current)
        .to_owned()
}
fn output(command: &mut Command) -> Result<Vec<u8>> {
    let result = command.output()?;
    if !result.status.success() {
        return Err(format!(
            "command failed: {}",
            String::from_utf8_lossy(&result.stderr)
        )
        .into());
    }
    Ok(result.stdout)
}
fn git(args: &[&str]) -> Result<Vec<u8>> {
    output(Command::new("git").current_dir(root()).args(args))
}
fn check_source() -> Result<()> {
    migration::check(&root())
}
fn upstream_readme() -> Result<()> {
    let lock: Value = serde_json::from_slice(&fs::read(root().join("upstream.lock.json"))?)?;
    let commit = lock["upstream"]["commit"]
        .as_str()
        .ok_or("missing upstream commit")?;
    let tree = lock["upstream"]["root_tree"]
        .as_str()
        .ok_or("missing upstream tree")?;
    for value in [commit, tree] {
        if value.len() != 40 || !value.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err("invalid immutable pin".into());
        }
    }
    if String::from_utf8(git(&["rev-parse", &format!("{commit}^{{tree}}")])?)?.trim() != tree {
        return Err("upstream tree mismatch".into());
    }
    let source = git(&["cat-file", "blob", &format!("{commit}:README.md")])?;
    let document = fs::read(root().join("upstream-README.md"))?;
    let begin = b"<!-- t3-code-ls:upstream-readme:start -->\n\n";
    let end = b"\n<!-- t3-code-ls:upstream-readme:end -->";
    let start = unique_boundary(&document, begin)? + begin.len();
    let stop = unique_boundary(&document, end)?;
    if start > stop || document[start..stop] != source {
        return Err("immutable upstream README body mismatch".into());
    }
    println!("Upstream README matches {commit}, {} bytes", source.len());
    Ok(())
}
fn unique_boundary(document: &[u8], marker: &[u8]) -> Result<usize> {
    let positions: Vec<_> = document
        .windows(marker.len())
        .enumerate()
        .filter_map(|(i, value)| (value == marker).then_some(i))
        .collect();
    if positions.len() != 1 {
        return Err("README boundary must occur exactly once".into());
    }
    Ok(positions[0])
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CompilerRoute {
    Rustc,
    Lustr,
}

fn compiler_route(requested: Option<&str>, has_package: bool) -> Result<CompilerRoute> {
    match requested.unwrap_or(if has_package { "lustr" } else { "rustc" }) {
        "rustc" => Ok(CompilerRoute::Rustc),
        "lustr" => Ok(CompilerRoute::Lustr),
        _ => Err("T3_CODE_LS_COMPILER must be rustc or lustr".into()),
    }
}

fn pinned_rust_release(document: &str) -> Result<String> {
    let mut toolchain = false;
    let mut release = None;
    for line in document.lines().map(str::trim) {
        if line.starts_with('[') {
            toolchain = line == "[toolchain]";
        } else if toolchain {
            let Some((key, value)) = line.split_once('=') else {
                continue;
            };
            if key.trim() != "channel" {
                continue;
            }
            if release.is_some() {
                return Err("duplicate Rust release pin".into());
            }
            let value = value.split('#').next().unwrap_or_default().trim();
            let value = value
                .strip_prefix('"')
                .and_then(|value| value.strip_suffix('"'));
            let Some(value) = value else {
                return Err("pin a complete Rust release in rust-toolchain.toml".into());
            };
            let parts = value.split('.').collect::<Vec<_>>();
            if parts.len() != 3
                || parts
                    .iter()
                    .any(|part| part.is_empty() || !part.bytes().all(|b| b.is_ascii_digit()))
            {
                return Err("pin a complete Rust release in rust-toolchain.toml".into());
            }
            release = Some(value.to_owned());
        }
    }
    release.ok_or_else(|| "missing Rust release pin in rust-toolchain.toml".into())
}

fn version_field<'a>(version: &'a str, name: &str) -> Option<&'a str> {
    version.lines().find_map(|line| {
        let (key, value) = line.split_once(':')?;
        (key == name).then_some(value.trim())
    })
}

fn build_target(requested: Option<String>, cargo: Option<String>, version: &str) -> Result<String> {
    if let (Some(requested), Some(cargo)) = (&requested, &cargo) {
        if requested != cargo {
            return Err("CARGO_BUILD_TARGET conflicts with T3_CODE_LS_RUST_TARGET".into());
        }
    }
    requested
        .or(cargo)
        .or_else(|| version_field(version, "host").map(str::to_owned))
        .filter(|value| !value.is_empty())
        .ok_or_else(|| "missing Rust compilation target".into())
}

fn pinned_tool(release: &str, tool: &str) -> Result<PathBuf> {
    let path = String::from_utf8(output(Command::new("rustup").current_dir(root()).args([
        "which",
        "--toolchain",
        release,
        tool,
    ]))?)?;
    Ok(PathBuf::from(path.trim()).canonicalize()?)
}

fn check_tool_override(variable: &str, selected: &Path) -> Result<()> {
    if let Some(value) = env::var_os(variable) {
        if PathBuf::from(value).canonicalize()? != selected.canonicalize()? {
            return Err(format!("{variable} conflicts with the selected compiler package").into());
        }
    }
    Ok(())
}

fn check_compiler_wrapper(variable: &str, value: Option<&OsStr>) -> Result<()> {
    if value.is_some_and(|value| !value.is_empty()) {
        return Err(format!("{variable} conflicts with the verified compiler route").into());
    }
    Ok(())
}

fn build() -> Result<()> {
    for variable in [
        "RUSTC_WRAPPER",
        "RUSTC_WORKSPACE_WRAPPER",
        "CARGO_BUILD_RUSTC_WRAPPER",
        "CARGO_BUILD_RUSTC_WORKSPACE_WRAPPER",
    ] {
        check_compiler_wrapper(variable, env::var_os(variable).as_deref())?;
    }
    let route = compiler_route(
        env::var("T3_CODE_LS_COMPILER").ok().as_deref(),
        env::var_os("T3_CODE_LS_TOOLCHAIN").is_some(),
    )?;
    let (mut command, compiler, rustdoc, sysroot, version) = match route {
        CompilerRoute::Rustc => {
            if env::var_os("T3_CODE_LS_TOOLCHAIN").is_some()
                || env::var_os("T3_CODE_LS_SYSROOT").is_some()
            {
                return Err(
                    "stock rustc selection conflicts with a Lustr package or sysroot override"
                        .into(),
                );
            }
            let release =
                pinned_rust_release(&fs::read_to_string(root().join("rust-toolchain.toml"))?)?;
            let compiler = pinned_tool(&release, "rustc")?;
            let cargo = pinned_tool(&release, "cargo")?;
            let rustdoc = pinned_tool(&release, "rustdoc")?;
            for (variable, selected) in [
                ("RUSTC", &compiler),
                ("CARGO", &cargo),
                ("RUSTDOC", &rustdoc),
            ] {
                check_tool_override(variable, selected)?;
            }
            let version = String::from_utf8(output(Command::new(&compiler).arg("-vV"))?)?;
            if version_field(&version, "release") != Some(release.as_str()) {
                return Err("installed rustc does not match rust-toolchain.toml".into());
            }
            let sysroot = String::from_utf8(output(
                Command::new(&compiler).args(["--print", "sysroot"]),
            )?)?;
            let sysroot = PathBuf::from(sysroot.trim()).canonicalize()?;
            let mut command = Command::new(&cargo);
            command
                .env("CARGO", &cargo)
                .env("RUSTUP_TOOLCHAIN", &release);
            (command, compiler, rustdoc, sysroot, version)
        }
        CompilerRoute::Lustr => {
            let toolchain = PathBuf::from(env::var("T3_CODE_LS_TOOLCHAIN")?).canonicalize()?;
            let compiler = toolchain.join("bin/rustc");
            let rustdoc = toolchain.join("bin/rustdoc");
            let cargo = toolchain.join("bin/cargo");
            let sysroot = PathBuf::from(
                env::var_os("T3_CODE_LS_SYSROOT")
                    .unwrap_or_else(|| toolchain.clone().into_os_string()),
            )
            .canonicalize()?;
            let version = String::from_utf8(output(Command::new(&compiler).arg("-vV"))?)?;
            let pin: Value =
                serde_json::from_slice(&fs::read(root().join("toolchain.lock.json"))?)?;
            for field in ["compiler_revision", "host"] {
                let expected = pin["compatibility"][field]
                    .as_str()
                    .ok_or("missing Lustr compiler identity")?;
                let key = if field == "compiler_revision" {
                    "commit-hash"
                } else {
                    field
                };
                if version_field(&version, key) != Some(expected) {
                    return Err("Lustr compiler identity does not match toolchain.lock.json".into());
                }
            }
            let mut command = Command::new(toolchain.join("bin/lustr"));
            command
                .arg("--bluefe-root")
                .arg(&toolchain)
                .env("CARGO", cargo);
            (command, compiler, rustdoc, sysroot, version)
        }
    };
    let target = build_target(
        env::var("T3_CODE_LS_RUST_TARGET").ok(),
        env::var("CARGO_BUILD_TARGET").ok(),
        &version,
    )?;
    if !sysroot
        .join("lib/rustlib")
        .join(&target)
        .join("lib")
        .is_dir()
    {
        return Err(format!("missing selected compiler's target sysroot for {target}").into());
    }
    let target_dir = PathBuf::from(env::var("CARGO_TARGET_DIR")?);
    if !target_dir.is_absolute() {
        return Err("CARGO_TARGET_DIR must be absolute and outside the source checkout".into());
    }
    fs::create_dir_all(&target_dir)?;
    if target_dir
        .canonicalize()?
        .starts_with(root().canonicalize()?)
    {
        return Err("generated output must be outside canonical source".into());
    }
    let flags = rust_flags(
        env::var("CARGO_ENCODED_RUSTFLAGS").ok(),
        env::var("RUSTFLAGS").ok(),
        &sysroot,
    )?;
    command
        .current_dir(root())
        .args([
            "build",
            "--locked",
            "--release",
            "--workspace",
            "--bins",
            "--target",
        ])
        .arg(&target)
        .arg("--target-dir")
        .arg(&target_dir)
        .arg("--message-format=json")
        .env("RUSTC", &compiler)
        .env("RUSTDOC", &rustdoc)
        .env("CARGO_ENCODED_RUSTFLAGS", flags)
        .stderr(Stdio::inherit());
    let compiled = output(&mut command)?;
    let destination = target_dir.join("deliverable").join(&target);
    fs::create_dir_all(&destination)?;
    let mut files = Vec::new();
    for line in compiled.split(|byte| *byte == b'\n') {
        let Ok(value) = serde_json::from_slice::<Value>(line) else {
            continue;
        };
        if value["reason"] != "compiler-artifact" {
            continue;
        }
        if value["target"]["name"] == "t3-code-ls-provider-fixture" {
            continue;
        }
        if let Some(executable) = value["executable"].as_str() {
            let input = Path::new(executable);
            let name = input.file_name().ok_or("missing executable name")?;
            fs::copy(input, destination.join(name))?;
            files.push(name.to_string_lossy().into_owned());
        }
    }
    if files.is_empty() {
        return Err("build produced no executable artifacts".into());
    }
    fs::write(
        destination.join("manifest.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "target":target,"executables":files,"compiler":version,
            "compilerRoute":match route { CompilerRoute::Rustc => "rustc", CompilerRoute::Lustr => "bluefe-rustc-via-lustr" },
            "sysroot":sysroot,
        }))?,
    )?;
    println!(
        "Staged {} Rust executables in {}",
        files.len(),
        destination.display()
    );
    Ok(())
}
fn rust_flags(encoded: Option<String>, plain: Option<String>, sysroot: &Path) -> Result<String> {
    let mut flags: Vec<String> = match encoded {
        Some(value) if value.is_empty() => Vec::new(),
        Some(value) => value.split('\x1f').map(str::to_owned).collect(),
        None => plain
            .unwrap_or_default()
            .split_whitespace()
            .map(str::to_owned)
            .collect(),
    };
    let expected = sysroot.to_str().ok_or("sysroot must be UTF-8")?;
    let mut selected = false;
    let mut index = 0;
    while index < flags.len() {
        let value = if flags[index] == "--sysroot" {
            index += 1;
            Some(flags.get(index).ok_or("missing --sysroot value")?.as_str())
        } else {
            flags[index].strip_prefix("--sysroot=")
        };
        if let Some(value) = value {
            if selected || value != expected {
                return Err("conflicting or repeated explicit sysroot selection".into());
            }
            selected = true;
        }
        index += 1;
    }
    if !selected {
        flags.push("--sysroot".into());
        flags.push(expected.into());
    }
    Ok(flags.join("\x1f"))
}
fn run() -> Result<()> {
    match env::args().nth(1).as_deref() {
        Some("check-source") => check_source(),
        Some("check-upstream-readme") => upstream_readme(),
        Some("build") => build(),
        _ => Err("usage: t3code-ls-tools <check-source|check-upstream-readme|build>".into()),
    }
}
fn main() {
    if let Err(error) = run() {
        eprintln!("{error}");
        std::process::exit(1);
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn compiler_selection_defaults_to_pinned_stock_without_changing_explicit_routes() {
        assert_eq!(compiler_route(None, false).unwrap(), CompilerRoute::Rustc);
        assert_eq!(compiler_route(None, true).unwrap(), CompilerRoute::Lustr);
        assert_eq!(
            compiler_route(Some("rustc"), true).unwrap(),
            CompilerRoute::Rustc
        );
        assert_eq!(
            compiler_route(Some("lustr"), false).unwrap(),
            CompilerRoute::Lustr
        );
        assert!(compiler_route(Some("nightly"), false).is_err());
        assert!(compiler_route(Some(""), false).is_err());
    }
    #[test]
    fn compiler_wrappers_cannot_replace_verified_tools() {
        assert!(check_compiler_wrapper("RUSTC_WRAPPER", None).is_ok());
        assert!(check_compiler_wrapper("RUSTC_WRAPPER", Some(OsStr::new(""))).is_ok());
        assert!(
            check_compiler_wrapper("RUSTC_WRAPPER", Some(OsStr::new("other-compiler"))).is_err()
        );
    }
    #[test]
    fn stock_compiler_requires_an_exact_release_pin() {
        assert_eq!(
            pinned_rust_release(
                "[toolchain]\nchannel = \"1.99.1\" # pinned\nprofile = \"minimal\""
            )
            .unwrap(),
            "1.99.1"
        );
        for pin in ["stable", "nightly", "1.99", "1.99.1-beta", "1..1", ""] {
            assert!(pinned_rust_release(&format!("[toolchain]\nchannel = \"{pin}\"")).is_err());
        }
        assert!(pinned_rust_release("[other]\nchannel = \"1.99.1\"").is_err());
        assert!(
            pinned_rust_release("[toolchain]\nchannel = \"1.99.1\"\nchannel = \"1.99.2\"").is_err()
        );
    }
    #[test]
    fn target_selection_preserves_cargo_targets_and_rejects_conflicts() {
        let version = "rustc 1.99.1\r\nhost: x86_64-unknown-linux-gnu\r\nrelease: 1.99.1\r\n";
        assert_eq!(version_field(version, "release"), Some("1.99.1"));
        assert_eq!(
            build_target(None, None, version).unwrap(),
            "x86_64-unknown-linux-gnu"
        );
        assert_eq!(
            build_target(None, Some("aarch64-apple-darwin".into()), version).unwrap(),
            "aarch64-apple-darwin"
        );
        assert_eq!(
            build_target(
                Some("x86_64-pc-windows-msvc".into()),
                Some("x86_64-pc-windows-msvc".into()),
                version
            )
            .unwrap(),
            "x86_64-pc-windows-msvc"
        );
        assert!(build_target(
            Some("aarch64-apple-darwin".into()),
            Some("x86_64-unknown-linux-gnu".into()),
            version
        )
        .is_err());
        assert!(build_target(None, None, "rustc without host").is_err());
    }
    #[test]
    fn flags_preserve_encoded_values_and_reject_competing_sysroots() {
        assert_eq!(
            rust_flags(
                Some("-C\x1flink-arg=a b".into()),
                None,
                Path::new("/produced")
            )
            .unwrap(),
            "-C\x1flink-arg=a b\x1f--sysroot\x1f/produced"
        );
        assert!(rust_flags(
            Some("--sysroot=/other".into()),
            None,
            Path::new("/produced")
        )
        .is_err());
        assert_eq!(
            rust_flags(
                Some("--sysroot\x1f/produced\x1f-C\x1flink-arg=a b".into()),
                None,
                Path::new("/produced")
            )
            .unwrap(),
            "--sysroot\x1f/produced\x1f-C\x1flink-arg=a b"
        );
        assert_eq!(
            rust_flags(
                Some("--sysroot=/produced".into()),
                None,
                Path::new("/produced")
            )
            .unwrap(),
            "--sysroot=/produced"
        );
        assert_eq!(
            rust_flags(
                Some(String::new()),
                Some("-C opt-level=2".into()),
                Path::new("/produced")
            )
            .unwrap(),
            "--sysroot\x1f/produced"
        );
        for flags in ["--sysroot", "--sysroot\x1f/produced\x1f--sysroot=/produced"] {
            assert!(rust_flags(Some(flags.into()), None, Path::new("/produced")).is_err());
        }
    }
    #[test]
    fn ambiguous_readme_boundaries_are_rejected() {
        assert!(unique_boundary(b"start start", b"start").is_err());
        assert_eq!(unique_boundary(b"before start after", b"start").unwrap(), 7);
    }
}
