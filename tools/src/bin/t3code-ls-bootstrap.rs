#![forbid(unsafe_code)]

//! Build the pinned compiler from GitHub sources outside this checkout.
//!
//! The downloaded nightly builds only this coordinator and BlueFe's bootstrap.
//! Application qualification is a separate invocation through the produced
//! compiler. Upstream compiler bootstrap programs stay in their own checkout.

use std::env;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

const BLUEFE: &str = "7005dc02c9fb6674380eade443022bbc04215276";
const LUSTR: &str = "6339fbb104d6adda4be7e6be7dc637a6a9e5586b";
const BOOTSTRAP_LUSTR: &str = "a09efac9ab746de66d208978d5ad46b3df728a77";
const LLVM: &str = "4282da708a1110147b1152801c3118b875b7f1b6";
const LLVM_SHA256: &str = "247f38620900fc2e0aa268080f69ea0efd5dd52530aff20d0b3a36916f81ed0a";
const STAGE0: &str = "nightly-2026-11-03";
const STAGE0_COMMIT: &str = "1c3a6506966e21845b31de0b075d2527660db132";
const TINYCC: &str = "43c7708b85681a2fd4451c8a541af4494a8919b2";
const HOST: &str = "x86_64-unknown-linux-gnu";
const WASM: &str = "wasm32-unknown-unknown";
const LLVM_INPUTS: &[&str] = &[
    "src/llvm-project",
    "src/bootstrap/download-ci-llvm-stamp",
    "src/version",
];

fn run(command: &mut Command) -> Result<()> {
    eprintln!("running: {command:?}");
    let status = command.status()?;
    if !status.success() {
        return Err(format!("command failed ({status}): {command:?}").into());
    }
    Ok(())
}

fn output(command: &mut Command) -> Result<String> {
    eprintln!("reading: {command:?}");
    let result = command.stderr(Stdio::inherit()).output()?;
    if !result.status.success() {
        return Err(format!("command failed ({}): {command:?}", result.status).into());
    }
    Ok(String::from_utf8(result.stdout)?.trim().to_owned())
}

fn git(directory: &Path) -> Command {
    let mut command = Command::new("git");
    command.arg("-C").arg(directory);
    command
}

fn require_equal(label: &str, actual: &str, expected: &str) -> Result<()> {
    if actual != expected {
        return Err(format!("{label}: expected {expected:?}, found {actual:?}").into());
    }
    Ok(())
}

fn require_file(path: &Path) -> Result<()> {
    if !path.is_file() {
        return Err(format!("required file is missing: {}", path.display()).into());
    }
    Ok(())
}

fn version_field<'a>(text: &'a str, name: &str) -> Result<&'a str> {
    text.lines()
        .find_map(|line| line.strip_prefix(name))
        .map(str::trim)
        .ok_or_else(|| format!("version output has no {name}").into())
}

fn version(binary: &Path, commit: &str) -> Result<String> {
    require_file(binary)?;
    let text = output(Command::new(binary).arg("-vV"))?;
    println!("{}\n{text}", binary.display());
    require_equal(
        "compiler commit",
        version_field(&text, "commit-hash:")?,
        commit,
    )?;
    require_equal("compiler host", version_field(&text, "host:")?, HOST)?;
    Ok(text)
}

fn stage0_program(name: &str) -> Result<PathBuf> {
    let value = output(Command::new("rustup").args(["which", "--toolchain", STAGE0, name]))?;
    let path = fs::canonicalize(value)?;
    require_file(&path)?;
    Ok(path)
}

/// Create a fresh isolated directory; never erase a previous build or source tree.
fn work_directory() -> Result<PathBuf> {
    let requested = env::var_os("T3_CODE_LS_BOOTSTRAP_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            env::temp_dir().join(format!("t3code-ls-bluefe-bootstrap-{}", std::process::id()))
        });
    if !requested.is_absolute() {
        return Err(
            "T3_CODE_LS_BOOTSTRAP_DIR must be an absolute path outside the checkout".into(),
        );
    }
    let parent = requested
        .parent()
        .ok_or("bootstrap directory needs a parent")?;
    fs::create_dir_all(parent)?;
    let directory = fs::canonicalize(parent)?.join(
        requested
            .file_name()
            .ok_or("bootstrap directory needs a name")?,
    );
    let current = env::current_dir()?.canonicalize()?;
    let checkout = current
        .ancestors()
        .find(|directory| directory.join("upstream.lock.json").is_file())
        .unwrap_or(&current);
    if directory.starts_with(&checkout) {
        return Err(
            "compiler sources and build output must stay outside the application checkout".into(),
        );
    }
    fs::create_dir(&directory)?;
    Ok(directory)
}

fn fetch_source(directory: &Path, repository: &str, commit: &str, depth: u32) -> Result<()> {
    fs::create_dir(directory)?;
    run(git(directory).arg("init"))?;
    run(git(directory).args(["remote", "add", "origin", repository]))?;
    fetch_history(directory, commit, depth)?;
    run(git(directory).args(["checkout", "--detach", "FETCH_HEAD"]))?;
    require_equal(
        "checked-out source commit",
        &output(git(directory).args(["rev-parse", "HEAD"]))?,
        commit,
    )
}

fn fetch_history(directory: &Path, commit: &str, depth: u32) -> Result<()> {
    run(git(directory).args([
        "fetch",
        "--no-tags",
        "--filter=blob:none",
        &format!("--depth={depth}"),
        "origin",
        commit,
    ]))
}

/// Match the pinned bootstrap's history lookup, without its rust-lang CI shortcut.
fn llvm_revision(source: &Path) -> Result<String> {
    let upstream = output(git(source).args([
        "rev-list",
        "--author-date-order",
        "--author=bors@rust-lang\\.org",
        "-n1",
        "HEAD",
    ]))?;
    if upstream.is_empty() {
        return Ok(String::new());
    }
    output(
        git(source)
            .args([
                "rev-list",
                "--first-parent",
                "-n1",
                &upstream,
                "--author=bors@rust-lang\\.org",
                "--",
            ])
            .args(LLVM_INPUTS),
    )
}

fn verify_llvm_source(source: &Path) -> Result<()> {
    for depth in [64, 128, 256, 512, 1024] {
        if depth != 64 {
            fetch_history(source, BLUEFE, depth)?;
        }
        let selected = llvm_revision(source)?;
        let shallow = match fs::read_to_string(source.join(".git/shallow")) {
            Ok(text) => text,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => String::new(),
            Err(error) => return Err(error.into()),
        };
        if selected.is_empty() || shallow.lines().any(|line| line == selected) {
            continue;
        }
        require_equal("upstream LLVM artifact revision", &selected, LLVM)?;
        run(git(source).args(["merge-base", "--is-ancestor", LLVM, "HEAD"]))?;
        run(git(source)
            .args(["diff", "--exit-code", LLVM, "HEAD", "--"])
            .args(LLVM_INPUTS))?;
        require_equal(
            "LLVM artifact channel",
            &output(git(source).args(["show", &format!("{LLVM}:src/ci/channel")]))?,
            "nightly",
        )?;
        println!("LLVM inputs match upstream {LLVM}; full LLVM source compilation is disabled");
        return Ok(());
    }
    Err("not enough pinned upstream history to select the reviewed LLVM artifact".into())
}

fn verify_submodule(source: &Path, path: &str, expected: Option<&str>) -> Result<()> {
    let record = output(git(source).args(["ls-tree", "HEAD", "--", path]))?;
    let mut fields = record.split_whitespace();
    require_equal(
        "submodule mode",
        fields.next().unwrap_or_default(),
        "160000",
    )?;
    require_equal(
        "submodule type",
        fields.next().unwrap_or_default(),
        "commit",
    )?;
    let pinned = fields.next().ok_or("submodule has no pinned commit")?;
    if let Some(expected) = expected {
        require_equal("bootstrap coordinator gitlink", pinned, expected)?;
    }
    require_equal(
        "initialized submodule commit",
        &output(git(&source.join(path)).args(["rev-parse", "HEAD"]))?,
        pinned,
    )
}

fn download_llvm(build: &Path) -> Result<()> {
    // This is exactly the cache location used by the pinned bootstrap. That
    // bootstrap reuses this verified archive instead of downloading it again.
    let cache = build
        .join("cache")
        .join(format!("llvm-{HOST}-{LLVM}-false"));
    fs::create_dir_all(&cache)?;
    let filename = format!("rust-dev-nightly-{HOST}.tar.xz");
    let archive = cache.join(&filename);
    let url = format!("https://ci-artifacts.rust-lang.org/rustc-builds/{LLVM}/{filename}");
    run(Command::new("curl")
        .args([
            "--fail",
            "--location",
            "--retry",
            "3",
            "--connect-timeout",
            "30",
            "--max-time",
            "600",
            "--output",
        ])
        .arg(&archive)
        .arg(&url))?;
    let actual = sha256(&archive)?;
    require_equal("prebuilt LLVM SHA256", &actual, LLVM_SHA256)?;
    println!("verified LLVM archive: {url} sha256={actual}");
    Ok(())
}

fn quoted(value: &str) -> String {
    let mut result = String::from("\"");
    for character in value.chars() {
        match character {
            '\\' => result.push_str("\\\\"),
            '\"' => result.push_str("\\\""),
            '\n' => result.push_str("\\n"),
            '\r' => result.push_str("\\r"),
            '\t' => result.push_str("\\t"),
            character if character.is_control() => {
                result.push_str(&format!("\\u{:04x}", character as u32));
            }
            character => result.push(character),
        }
    }
    result.push('\"');
    result
}

fn compiler_config(build: &Path, cargo: &Path, rustc: &Path, rustdoc: &Path) -> Result<String> {
    let mut config = String::from(
        r#"
change-id = "ignore"
[build]
build = "x86_64-unknown-linux-gnu"
host = ["x86_64-unknown-linux-gnu"]
target = ["x86_64-unknown-linux-gnu", "wasm32-unknown-unknown"]
jobs = 2
docs = false
submodules = false
locked-deps = true
optimized-compiler-builtins = false
sanitizers = false
profiler = false
print-step-timings = true
"#,
    );
    for (key, path) in [
        ("build-dir", build),
        ("cargo", cargo),
        ("rustc", rustc),
        ("rustdoc", rustdoc),
    ] {
        config.push_str(&format!(
            "{key} = {}\n",
            quoted(path.to_str().ok_or("bootstrap paths must be UTF-8")?)
        ));
    }
    config.push_str(
        r#"
[llvm]
download-ci-llvm = true
assertions = false
[rust]
download-rustc = false
channel = "dev"
omit-git-hash = false
debug = false
debug-assertions = false
debuginfo-level = 0
incremental = false
codegen-units = 16
optimize = 3
lld = true
bootstrap-override-lld = "self-contained"
"#,
    );
    Ok(config)
}

fn sha256(path: &Path) -> Result<String> {
    let result = Command::new("sha256sum").arg("--").arg(path).output()?;
    if !result.status.success() {
        return Err(format!(
            "cannot hash {}: {}",
            path.display(),
            String::from_utf8_lossy(&result.stderr)
        )
        .into());
    }
    let value = String::from_utf8(result.stdout)?;
    let hash = value
        .split_whitespace()
        .next()
        .ok_or("missing SHA-256 output")?;
    if hash.len() != 64 || !hash.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err("invalid SHA-256 output".into());
    }
    Ok(hash.to_owned())
}

/// Do not inherit application flags, wrappers, or Cargo output paths into bootstrap.
fn clear_build_environment(command: &mut Command) -> &mut Command {
    for name in [
        "RUSTC",
        "RUSTDOC",
        "RUSTC_WRAPPER",
        "RUSTC_WORKSPACE_WRAPPER",
        "RUSTFLAGS",
        "RUSTDOCFLAGS",
        "CARGO_ENCODED_RUSTFLAGS",
        "CARGO_TARGET_DIR",
        "LUST_CARGO",
        "LUSTC",
        "CARGO",
        "LD_LIBRARY_PATH",
    ] {
        command.env_remove(name);
    }
    command
        .env("CARGO_BUILD_JOBS", "2")
        .env("CARGO_INCREMENTAL", "0")
}

fn clean_source(source: &Path) -> Result<()> {
    let changes = output(git(source).args([
        "status",
        "--porcelain",
        "--untracked-files=no",
        "--ignore-submodules=none",
    ]))?;
    if !changes.is_empty() {
        return Err(format!("compiler source changed unexpectedly:\n{changes}").into());
    }
    Ok(())
}

fn build_native_compiler(lustr: &Path, sysroot: &Path, work: &Path) -> Result<()> {
    let native = lustr.join("compiler/native");
    let tinycc = native.join("third_party/tinycc");
    fs::create_dir_all(tinycc.parent().ok_or("TinyCC source needs a parent")?)?;
    fetch_source(&tinycc, "https://github.com/TinyCC/tinycc.git", TINYCC, 1)?;
    let prefix = native.join("third_party/tcc");
    // TinyCC detects an in-tree build from the spelling of $0. An absolute
    // configure path makes it copy its Makefiles onto themselves and fail.
    run(Command::new("./configure")
        .current_dir(&tinycc)
        .arg(format!("--prefix={}", prefix.display())))?;
    run(Command::new("make").current_dir(&tinycc).arg("-j2"))?;
    run(Command::new("make").current_dir(&tinycc).arg("install"))?;
    clean_source(&tinycc)?;
    let target = work.join("native-lust-target");
    let mut command = Command::new(sysroot.join("bin/lustr"));
    clear_build_environment(&mut command)
        .current_dir(&native)
        .arg("--bluefe-root")
        .arg(sysroot)
        .args(["build", "--locked", "--profile", "gate", "--target-dir"])
        .arg(&target);
    run(&mut command)?;
    clean_source(lustr)?;
    for name in ["lust", "lust-core"] {
        fs::copy(
            target.join("gate").join(name),
            sysroot.join("bin").join(name),
        )?;
    }
    let support = sysroot.join("lib/lust");
    copy_tree(
        &native.join("library"),
        &support.join("library"),
        &mut Vec::new(),
    )?;
    copy_tree(
        &prefix.join("bin"),
        &support.join("tcc/bin"),
        &mut Vec::new(),
    )?;
    copy_tree(
        &prefix.join("lib/tcc"),
        &support.join("tcc/lib/tcc"),
        &mut Vec::new(),
    )?;
    let licenses = sysroot.join("share/licenses/lustr");
    fs::create_dir_all(&licenses)?;
    fs::copy(lustr.join("LICENSE"), licenses.join("LICENSE"))?;
    fs::copy(tinycc.join("COPYING"), licenses.join("tinycc-COPYING"))?;
    run(Command::new(sysroot.join("bin/lust")).arg("--version"))
}

fn copy_tree(source: &Path, destination: &Path, parents: &mut Vec<PathBuf>) -> Result<()> {
    let resolved = source.canonicalize()?;
    if parents.contains(&resolved) {
        return Err(format!(
            "recursive symlink in compiler payload: {}",
            source.display()
        )
        .into());
    }
    let metadata = fs::metadata(source)?;
    if metadata.is_dir() {
        fs::create_dir_all(destination)?;
        parents.push(resolved);
        for entry in fs::read_dir(source)? {
            let entry = entry?;
            copy_tree(&entry.path(), &destination.join(entry.file_name()), parents)?;
        }
        parents.pop();
    } else if metadata.is_file() {
        fs::copy(source, destination)?;
    } else {
        return Err(format!("unsupported payload file: {}", source.display()).into());
    }
    Ok(())
}

fn payload_identity() -> String {
    format!(
        "bluefe={BLUEFE}\nlustr={LUSTR}\nbootstrap_lustr={BOOTSTRAP_LUSTR}\nllvm={LLVM}\nllvm_sha256={LLVM_SHA256}\nstage0={STAGE0_COMMIT}\ntinycc={TINYCC}\nhost={HOST}\nwasm={WASM}\n"
    )
}

fn payload_files(root: &Path, current: &Path, files: &mut Vec<PathBuf>) -> Result<()> {
    for entry in fs::read_dir(current)? {
        let entry = entry?;
        let path = entry.path();
        let metadata = fs::symlink_metadata(&path)?;
        if metadata.file_type().is_symlink() {
            return Err(format!("payload retains a symlink: {}", path.display()).into());
        }
        if metadata.is_dir() {
            payload_files(root, &path, files)?;
        } else if metadata.is_file() {
            let relative = path.strip_prefix(root)?;
            if relative != Path::new("payload.sha256") {
                files.push(relative.to_owned());
            }
        } else {
            return Err(format!("unsupported cached payload file: {}", path.display()).into());
        }
    }
    Ok(())
}

fn manifest(root: &Path) -> Result<String> {
    let mut files = Vec::new();
    payload_files(root, root, &mut files)?;
    files.sort();
    let mut text = String::new();
    for path in files {
        let name = path.to_str().ok_or("payload paths must be UTF-8")?;
        if name.contains(['\n', '\r', '\\']) {
            return Err("payload path cannot contain a newline or backslash".into());
        }
        text.push_str(&format!("{}  {name}\n", sha256(&root.join(&path))?));
    }
    Ok(text)
}

fn verify_payload(root: &Path, work: &Path) -> Result<()> {
    require_equal(
        "payload source identity",
        &fs::read_to_string(root.join("source-identity.txt"))?,
        &payload_identity(),
    )?;
    require_equal(
        "payload file hashes",
        &manifest(root)?,
        &fs::read_to_string(root.join("payload.sha256"))?,
    )?;
    version(&root.join("bin/rustc"), BLUEFE)?;
    version(&root.join("bin/rustdoc"), BLUEFE)?;
    let sysroot = output(Command::new(root.join("bin/rustc")).args(["--print", "sysroot"]))?;
    if Path::new(&sysroot).canonicalize()? != root.canonicalize()? {
        return Err("cached compiler does not select its relocated sysroot".into());
    }
    let probe = work.join("relocation-probe");
    fs::create_dir(&probe)?;
    let source = probe.join("main.rs");
    fs::write(
        &source,
        "fn main() { println!(\"t3code-ls toolchain\"); }\n",
    )?;
    let program = probe.join("native");
    run(Command::new(root.join("bin/rustc"))
        .arg(&source)
        .args(["--edition=2024", "-o"])
        .arg(&program))?;
    require_equal(
        "produced native execution",
        &output(&mut Command::new(&program))?,
        "t3code-ls toolchain",
    )?;
    let tests = probe.join("tests.rs");
    fs::write(
        &tests,
        "#[test] fn arithmetic() { assert_eq!(3 + 4, 7); }\n",
    )?;
    let test_program = probe.join("tests");
    run(Command::new(root.join("bin/rustc"))
        .arg(&tests)
        .args(["--test", "-o"])
        .arg(&test_program))?;
    run(&mut Command::new(test_program))?;
    let macros = probe.join("macros.rs");
    fs::write(
        &macros,
        "extern crate proc_macro; #[proc_macro] pub fn pass(input: proc_macro::TokenStream) -> proc_macro::TokenStream { input }\n",
    )?;
    run(Command::new(root.join("bin/rustc"))
        .arg(&macros)
        .args(["--crate-type", "proc-macro", "-o"])
        .arg(probe.join("macros.so")))?;
    let wasm = probe.join("probe.wasm");
    run(Command::new(root.join("bin/rustc"))
        .arg(&source)
        .args(["--target", WASM, "-o"])
        .arg(&wasm))?;
    if !fs::read(wasm)?.starts_with(b"\0asm\x01\0\0\0") {
        return Err("produced wasm output is not a WebAssembly module".into());
    }
    let lust_program = probe.join("lust-native");
    let mut native = Command::new(root.join("bin/lust"));
    clear_build_environment(&mut native)
        .current_dir(&probe)
        .env_remove("LUST_LIBRARY")
        .env_remove("LUST_STD_SRC")
        .env("LUST_NO_DAEMON", "1")
        .env("LUST_CACHE_DIR", probe.join("cache"))
        .env("TMPDIR", &probe)
        .arg("build")
        .arg(&source)
        .arg("-o")
        .arg(&lust_program)
        .arg("--json");
    run(&mut native)?;
    require_equal(
        "packaged native Lust execution",
        &output(&mut Command::new(lust_program))?,
        "t3code-ls toolchain",
    )?;
    Ok(())
}

fn assemble_payload(sysroot: &Path, source: &Path, work: &Path) -> Result<PathBuf> {
    let requested = env::var_os("T3_CODE_LS_TOOLCHAIN")
        .map(PathBuf::from)
        .unwrap_or_else(|| work.join("toolchain"));
    if !requested.is_absolute() || requested.exists() {
        return Err("T3_CODE_LS_TOOLCHAIN must name a new absolute payload directory".into());
    }
    fs::create_dir_all(requested.parent().ok_or("payload needs a parent")?)?;
    let root = requested
        .parent()
        .unwrap()
        .canonicalize()?
        .join(requested.file_name().ok_or("payload needs a filename")?);
    if root.starts_with(env::current_dir()?.canonicalize()?) || root.starts_with(source) {
        return Err("compiler payload must stay outside source checkouts".into());
    }
    copy_tree(sysroot, &root, &mut Vec::new())?;
    let licenses = root.join("share/licenses/bluefe");
    fs::create_dir_all(&licenses)?;
    for name in ["LICENSE-APACHE", "LICENSE-MIT", "COPYRIGHT"] {
        fs::copy(source.join(name), licenses.join(name))?;
    }
    fs::write(root.join("source-identity.txt"), payload_identity())?;
    write_provenance(&root, &root)?;
    fs::write(root.join("payload.sha256"), manifest(&root)?)?;
    verify_payload(&root, work)?;
    Ok(root)
}

fn append_environment(file: &Path, key: &str, value: &Path) -> Result<()> {
    let text = value.to_str().ok_or("environment path must be UTF-8")?;
    if text.contains(['\n', '\r']) {
        return Err("environment path cannot contain a newline".into());
    }
    writeln!(
        OpenOptions::new().create(true).append(true).open(file)?,
        "{key}={text}"
    )?;
    Ok(())
}

fn publish_environment(sysroot: &Path, work: &Path) -> Result<()> {
    let target = work.join("application-target");
    if let Some(file) = env::var_os("GITHUB_ENV") {
        let file = PathBuf::from(file);
        for (key, value) in [
            ("T3_CODE_LS_TOOLCHAIN", sysroot.to_owned()),
            ("CARGO_TARGET_DIR", target),
            ("RUSTC", sysroot.join("bin/rustc")),
            ("RUSTDOC", sysroot.join("bin/rustdoc")),
            ("LUST_CARGO", sysroot.join("bin/cargo")),
        ] {
            append_environment(&file, key, &value)?;
        }
    }
    if let Some(file) = env::var_os("GITHUB_PATH") {
        let value = sysroot.join("bin");
        let value = value.to_str().ok_or("toolchain path must be UTF-8")?;
        if value.contains(['\n', '\r']) {
            return Err("toolchain path cannot contain a newline".into());
        }
        writeln!(
            OpenOptions::new().create(true).append(true).open(file)?,
            "{value}"
        )?;
    }
    if let Some(file) = env::var_os("GITHUB_STEP_SUMMARY") {
        writeln!(
            OpenOptions::new().create(true).append(true).open(file)?,
            "### Produced BlueFe compiler\n\nBlueFe `{BLUEFE}`; Lustr `{LUSTR}`; upstream LLVM `{LLVM}`.\n\nThe native and Wasm standard libraries and rustdoc are present in `{}`. Compiler bootstrap completed; application qualification follows in its own step. Stage0 `{STAGE0}` was used only for bootstrap. Reused payloads must pass the same source, hash and execution checks.\n",
            sysroot.display(),
        )?;
    }
    println!("T3_CODE_LS_TOOLCHAIN={}", sysroot.display());
    println!(
        "Application qualification must use this produced sysroot; bootstrap success alone is not acceptance."
    );
    Ok(())
}

fn bootstrap() -> Result<()> {
    if env::consts::OS != "linux" || env::consts::ARCH != "x86_64" {
        return Err("this reviewed bootstrap targets x86_64 Linux GNU hosts only".into());
    }
    let stage0_rustc = stage0_program("rustc")?;
    let stage0_cargo = stage0_program("cargo")?;
    let stage0_rustdoc = stage0_program("rustdoc")?;
    version(&stage0_rustc, STAGE0_COMMIT)?;
    println!("{}", output(Command::new(&stage0_cargo).arg("-vV"))?);
    let work = work_directory()?;
    run(Command::new("df").arg("-h").arg(&work))?;
    let source = work.join("bluefe");
    let build = work.join("build");
    let lustr = work.join("lustr");
    fetch_source(
        &source,
        "https://github.com/glopyglerky/blueFe.git",
        BLUEFE,
        64,
    )?;
    verify_llvm_source(&source)?;
    let stage0_metadata = fs::read_to_string(source.join("src/stage0"))?;
    for required in [
        "compiler_date=2026-11-03".to_owned(),
        format!("compiler_git_commit_hash={STAGE0_COMMIT}"),
    ] {
        if !stage0_metadata.lines().any(|line| line == required) {
            return Err(format!("pinned stage0 metadata does not contain {required}").into());
        }
    }
    download_llvm(&build)?;
    run(git(&source).args([
        "submodule",
        "update",
        "--init",
        "--depth",
        "1",
        "--",
        "src/bluefe/lustr",
        "library/backtrace",
    ]))?;
    verify_submodule(&source, "src/bluefe/lustr", Some(BOOTSTRAP_LUSTR))?;
    verify_submodule(&source, "library/backtrace", None)?;
    require_file(&source.join("src/bluefe/lustr/tools/cargo-lust/Cargo.toml"))?;
    // Do not initialize LLVM: unavailable CI artifacts must fail, never trigger
    // an unplanned LLVM source build. Only the two required submodules exist.
    if source.join("src/llvm-project/llvm/CMakeLists.txt").exists() {
        return Err("unexpected LLVM source checkout; prebuilt LLVM is required".into());
    }
    fs::write(
        source.join("bootstrap.toml"),
        compiler_config(&build, &stage0_cargo, &stage0_rustc, &stage0_rustdoc)?,
    )?;
    clean_source(&source)?;
    let mut bootstrap = Command::new(source.join("x"));
    clear_build_environment(&mut bootstrap)
        .current_dir(&source)
        // Upstream assumes GITHUB_ACTIONS means a rust-lang PR merge and takes
        // HEAD^1 for LLVM. This fork needs its actual pinned upstream history.
        .env_remove("GITHUB_ACTIONS")
        .args([
            "build",
            "--stage",
            "1",
            "--target",
            "x86_64-unknown-linux-gnu,wasm32-unknown-unknown",
            "compiler/rustc",
            "library",
            "src/tools/rustdoc",
        ]);
    run(&mut bootstrap)?;
    clean_source(&source)?;
    let ci_llvm = build.join(HOST).join("ci-llvm");
    require_equal(
        "downloaded LLVM stamp",
        &fs::read_to_string(ci_llvm.join(".llvm-stamp"))?,
        &format!("{LLVM}false"),
    )?;
    run(Command::new(ci_llvm.join("bin/llvm-config")).arg("--version"))?;
    let sysroot = fs::canonicalize(build.join(HOST).join("stage1"))?;
    version(&sysroot.join("bin/rustc"), BLUEFE)?;
    version(&sysroot.join("bin/rustdoc"), BLUEFE)?;
    let reported = output(Command::new(sysroot.join("bin/rustc")).args(["--print", "sysroot"]))?;
    if fs::canonicalize(&reported)? != sysroot {
        return Err(format!("produced compiler reports a different sysroot: {reported}").into());
    }
    for target in [HOST, WASM] {
        let libraries = sysroot.join("lib/rustlib").join(target).join("lib");
        let names: Vec<_> = fs::read_dir(&libraries)?
            .map(|entry| entry.map(|entry| entry.file_name().to_string_lossy().into_owned()))
            .collect::<std::io::Result<_>>()?;
        let required = if target == HOST {
            &["std", "proc_macro", "test"][..]
        } else {
            &["std"][..]
        };
        for library in required {
            if !names
                .iter()
                .any(|name| name.starts_with(&format!("lib{library}-")) && name.ends_with(".rlib"))
            {
                return Err(
                    format!("produced sysroot has no {library} library for {target}").into(),
                );
            }
        }
    }
    require_file(&sysroot.join("lib/rustlib").join(HOST).join("bin/rust-lld"))?;
    fs::copy(&stage0_cargo, sysroot.join("bin/cargo"))?;
    fetch_source(&lustr, "https://github.com/glopyglerky/lustr.git", LUSTR, 1)?;
    let manifest = lustr.join("crates/lustr/Cargo.toml");
    require_file(&manifest)?;
    let lustr_target = work.join("lustr-target");
    let mut coordinator = Command::new(sysroot.join("bin/cargo"));
    clear_build_environment(&mut coordinator)
        .current_dir(&lustr)
        .env("RUSTC", sysroot.join("bin/rustc"))
        .env("RUSTDOC", sysroot.join("bin/rustdoc"))
        .env("CARGO_TARGET_DIR", &lustr_target)
        .args(["build", "--manifest-path"])
        .arg(&manifest)
        .args(["--bin", "lustr", "--release", "--locked", "--offline"]);
    run(&mut coordinator)?;
    clean_source(&lustr)?;
    fs::copy(
        lustr_target.join("release/lustr"),
        sysroot.join("bin/lustr"),
    )?;
    run(Command::new(sysroot.join("bin/lustr"))
        .arg("--bluefe-root")
        .arg(&sysroot)
        .arg("doctor"))?;
    build_native_compiler(&lustr, &sysroot, &work)?;
    let payload = assemble_payload(&sysroot, &source, &work)?;
    publish_environment(&payload, &work)
}

fn write_provenance(sysroot: &Path, work: &Path) -> Result<()> {
    let mut binaries = Vec::new();
    for name in ["rustc", "rustdoc", "cargo", "lustr", "lust", "lust-core"] {
        binaries.push(format!(
            "{}: {}",
            quoted(name),
            quoted(&sha256(&sysroot.join("bin").join(name))?)
        ));
    }
    let receipt = format!(
        r#"{{
  "schema": 1,
  "status": "compiler-built-application-qualification-required",
  "bluefe": "{BLUEFE}",
  "lustr": "{LUSTR}",
  "bootstrap_lustr": "{BOOTSTRAP_LUSTR}",
  "stage0": "{STAGE0_COMMIT}",
  "tinycc": "{TINYCC}",
  "llvm_revision": "{LLVM}",
  "llvm_sha256": "{LLVM_SHA256}",
  "native_target": "{HOST}",
  "wasm_target": "{WASM}",
  "rustc_version": {},
  "rustdoc_version": {},
  "cargo_version": {},
  "binary_sha256": {{{}}}
}}
"#,
        quoted(&output(Command::new(sysroot.join("bin/rustc")).arg("-vV"))?),
        quoted(&output(
            Command::new(sysroot.join("bin/rustdoc")).arg("-vV")
        )?),
        quoted(&output(Command::new(sysroot.join("bin/cargo")).arg("-vV"))?),
        binaries.join(", ")
    );
    fs::write(work.join("toolchain-proof.json"), receipt)?;
    Ok(())
}

fn main() {
    let args: Vec<_> = env::args_os().skip(1).collect();
    let result = if args.is_empty() {
        bootstrap()
    } else if args.len() == 2 && args[0] == "--restore" {
        (|| {
            let root = PathBuf::from(&args[1]).canonicalize()?;
            let work = work_directory()?;
            verify_payload(&root, &work)?;
            publish_environment(&root, &work)
        })()
    } else {
        Err("usage: t3code-ls-bootstrap [--restore PAYLOAD]".into())
    };
    if let Err(error) = result {
        eprintln!("BlueFe bootstrap failed: {error}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    struct Temporary(PathBuf);
    impl Temporary {
        fn new() -> Self {
            static NEXT: AtomicU64 = AtomicU64::new(0);
            let path = env::temp_dir().join(format!(
                "t3-bootstrap-test-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            fs::create_dir(&path).unwrap();
            Self(path)
        }
    }
    impl Drop for Temporary {
        fn drop(&mut self) {
            fs::remove_dir_all(&self.0).unwrap();
        }
    }

    #[test]
    fn payload_manifest_detects_file_changes_and_extra_files() {
        let directory = Temporary::new();
        fs::write(directory.0.join("compiler"), "first").unwrap();
        let initial = manifest(&directory.0).unwrap();
        fs::write(directory.0.join("payload.sha256"), &initial).unwrap();
        assert_eq!(manifest(&directory.0).unwrap(), initial);
        fs::write(directory.0.join("compiler"), "second").unwrap();
        assert_ne!(manifest(&directory.0).unwrap(), initial);
        fs::write(directory.0.join("compiler"), "first").unwrap();
        fs::write(directory.0.join("extra"), "unrecorded").unwrap();
        assert_ne!(manifest(&directory.0).unwrap(), initial);
    }

    #[cfg(unix)]
    #[test]
    fn payload_copy_dereferences_links_and_rejects_cycles() {
        let directory = Temporary::new();
        let source = directory.0.join("source");
        fs::create_dir(&source).unwrap();
        fs::write(source.join("file"), "compiler data").unwrap();
        std::os::unix::fs::symlink("file", source.join("alias")).unwrap();
        let destination = directory.0.join("payload");
        copy_tree(&source, &destination, &mut Vec::new()).unwrap();
        assert!(
            !fs::symlink_metadata(destination.join("alias"))
                .unwrap()
                .file_type()
                .is_symlink()
        );
        assert_eq!(
            fs::read(destination.join("alias")).unwrap(),
            b"compiler data"
        );
        std::os::unix::fs::symlink(".", source.join("cycle")).unwrap();
        assert!(copy_tree(&source, &directory.0.join("cycle-copy"), &mut Vec::new()).is_err());
    }

    #[test]
    fn unrecognized_cache_identity_is_rejected_before_execution() {
        let directory = Temporary::new();
        fs::write(directory.0.join("source-identity.txt"), "unreviewed source").unwrap();
        let error = verify_payload(&directory.0, &directory.0)
            .unwrap_err()
            .to_string();
        assert!(error.contains("payload source identity"));
    }

    #[test]
    fn bootstrap_configuration_builds_compiler_and_preserves_paths() {
        let config = compiler_config(
            Path::new("/build/with a space"),
            Path::new("/seed/cargo"),
            Path::new("/seed/rustc"),
            Path::new("/seed/rustdoc"),
        )
        .unwrap();
        assert!(config.contains("jobs = 2"));
        assert!(config.contains("download-rustc = false"));
        assert!(config.contains("omit-git-hash = false"));
        assert!(config.contains("build-dir = \"/build/with a space\""));
    }

    #[test]
    fn metadata_quoting_preserves_delimiters_and_control_bytes() {
        assert_eq!(quoted("a\"b\\c\n\t\u{1f}"), "\"a\\\"b\\\\c\\n\\t\\u001f\"");
    }

    #[test]
    fn compiler_identity_requires_full_expected_commit() {
        let version = "rustc 1.102.0-dev\ncommit-hash: 7005dc02c9fb6674380eade443022bbc04215276\nhost: x86_64-unknown-linux-gnu\n";
        assert_eq!(version_field(version, "commit-hash:").unwrap(), BLUEFE);
        assert!(version_field("rustc 1.102.0-dev", "commit-hash:").is_err());
        assert!(require_equal("compiler commit", STAGE0_COMMIT, BLUEFE).is_err());
    }
}
