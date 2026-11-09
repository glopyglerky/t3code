use serde_json::Value;
use std::{
    env,
    error::Error,
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
    let files = git(&["ls-files", "-z"])?;
    let mut count = 0;
    for file in files
        .split(|byte| *byte == 0)
        .filter(|file| !file.is_empty())
    {
        let name = std::str::from_utf8(file)?;
        let path = root().join(name);
        if !path.exists() {
            continue;
        }
        if name.starts_with(".repos/") || foreign_code(name) {
            return Err(format!("retired implementation remains: {name}").into());
        }
        count += 1;
    }
    if root().join(".repos").exists() {
        return Err("remove obsolete in-checkout reference caches".into());
    }
    println!(
        "Checked {count} tracked files; no retired implementation languages or reference cache"
    );
    Ok(())
}
fn foreign_code(name: &str) -> bool {
    let path = Path::new(name);
    let extension = path
        .extension()
        .and_then(|value| value.to_str())
        .unwrap_or("");
    matches!(
        extension,
        "ts" | "tsx"
            | "js"
            | "jsx"
            | "mjs"
            | "cjs"
            | "mts"
            | "cts"
            | "py"
            | "sh"
            | "bash"
            | "ps1"
            | "bat"
            | "cmd"
            | "c"
            | "cc"
            | "cpp"
            | "h"
            | "hpp"
            | "m"
            | "mm"
            | "swift"
            | "kt"
            | "kts"
            | "java"
            | "rb"
            | "qml"
            | "zig"
            | "html"
            | "css"
            | "scss"
            | "astro"
            | "sql"
            | "patch"
            | "so"
            | "a"
            | "wasm"
            | "tgz"
    ) || matches!(
        path.file_name().and_then(|value| value.to_str()),
        Some(
            "package.json"
                | "pre-commit"
                | "CMakeLists.txt"
                | "Dockerfile"
                | "Makefile"
                | "PKGBUILD"
                | "gradlew"
                | "Podfile"
                | "Brewfile"
        )
    )
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
fn build() -> Result<()> {
    let toolchain = PathBuf::from(env::var("T3_CODE_LS_TOOLCHAIN")?).canonicalize()?;
    let compiler = toolchain.join("bin/rustc");
    let sysroot = PathBuf::from(
        env::var_os("T3_CODE_LS_SYSROOT").unwrap_or_else(|| toolchain.clone().into_os_string()),
    )
    .canonicalize()?;
    let version = String::from_utf8(output(Command::new(&compiler).arg("-vV"))?)?;
    let target = env::var("T3_CODE_LS_RUST_TARGET").or_else(|_| {
        version
            .lines()
            .find_map(|line| line.strip_prefix("host: "))
            .map(str::to_owned)
            .ok_or(env::VarError::NotPresent)
    })?;
    if !sysroot
        .join("lib/rustlib")
        .join(&target)
        .join("lib")
        .is_dir()
    {
        return Err(format!("missing produced sysroot for {target}").into());
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
    let mut command = Command::new(toolchain.join("bin/lustr"));
    command
        .current_dir(root())
        .args(["--bluefe-root"])
        .arg(&toolchain)
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
        .env("RUSTDOC", toolchain.join("bin/rustdoc"))
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
        serde_json::to_vec_pretty(
            &serde_json::json!({"target":target,"executables":files,"compiler":version}),
        )?,
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
    if flags
        .iter()
        .any(|flag| flag == "--sysroot" || flag.starts_with("--sysroot="))
    {
        return Err("select sysroot with T3_CODE_LS_SYSROOT".into());
    }
    flags.push("--sysroot".into());
    flags.push(sysroot.to_str().ok_or("sysroot must be UTF-8")?.into());
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
        assert!(
            rust_flags(
                Some("--sysroot=/other".into()),
                None,
                Path::new("/produced")
            )
            .is_err()
        );
    }
    #[test]
    fn retired_languages_and_ambiguous_boundaries_are_rejected() {
        for file in [
            "src/old.ts",
            "src/code.py",
            "package.json",
            "lib.so",
            "src/style.css",
        ] {
            assert!(foreign_code(file));
        }
        for file in [
            "src/main.rs",
            "Cargo.toml",
            "fixtures/input.json",
            "icon.svg",
            "LICENSE",
        ] {
            assert!(!foreign_code(file));
        }
        assert!(unique_boundary(b"start start", b"start").is_err());
        assert_eq!(unique_boundary(b"before start after", b"start").unwrap(), 7);
    }
}
