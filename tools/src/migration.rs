//! Migration accounting protects the working application, regardless of language.
use crate::{Result, output};
use serde_json::Value;
use std::{collections::BTreeMap, collections::BTreeSet, fs, path::Path, process::Command};

const RECOVERY_COMMIT: &str = "d29a94f3a175eac1f26c548023fedd8695cedfaf";
const TARGETS: &[&str] = &[
    "linux-x64",
    "linux-arm64",
    "darwin-x64",
    "darwin-arm64",
    "win32-x64",
    "win32-arm64",
    "browser",
    "ios",
    "android",
];
const PROTECTED_ROOTS: &[&str] = &[
    "apps",
    "packages",
    "native",
    "infra",
    "scripts",
    "patches",
    "oxlint-plugin-t3code",
];
const REQUIRED: &[&str] = &[
    "Cargo.toml",
    "Cargo.lock",
    "package.json",
    "pnpm-lock.yaml",
    "pnpm-workspace.yaml",
    ".pnpmfile.cjs",
    "tsconfig.base.json",
    "tsconfig.effect.json",
    "vite.config.ts",
    "upstream.lock.json",
    "lust-toolchain.json",
    "toolchain.lock.json",
    "port-map.json",
    "apps/server/package.json",
    "apps/web/package.json",
    "apps/desktop/package.json",
    "apps/mobile/package.json",
    "apps/marketing/package.json",
    "packages/contracts/package.json",
    "packages/client-runtime/package.json",
    "packages/shared/package.json",
    "infra/relay/package.json",
];

fn git(root: &Path, args: &[&str]) -> Result<Vec<u8>> {
    output(Command::new("git").current_dir(root).args(args))
}

fn text<'a>(value: &'a Value, key: &str) -> Result<&'a str> {
    value[key]
        .as_str()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| format!("port map requires a nonempty {key}").into())
}

fn array<'a>(value: &'a Value, key: &str) -> Result<&'a Vec<Value>> {
    value[key]
        .as_array()
        .ok_or_else(|| format!("port map requires array {key}").into())
}

fn relative(path: &str) -> Result<()> {
    if path.is_empty()
        || path.contains(['\\', '\0', '\n', '\r', ':'])
        || path
            .split('/')
            .any(|part| part.is_empty() || part == "." || part == "..")
    {
        return Err(format!("not a repository-relative file path: {path:?}").into());
    }
    Ok(())
}

fn identity(value: &str) -> Result<()> {
    if value.len() != 40 || !value.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(format!("invalid immutable Git identity: {value}").into());
    }
    Ok(())
}

fn state(value: &str) -> Result<()> {
    if !matches!(
        value,
        "candidate" | "integrated" | "accepted" | "retired" | "synced"
    ) {
        return Err(format!("invalid port state: {value}").into());
    }
    Ok(())
}

fn qualified(value: &str) -> bool {
    matches!(value, "accepted" | "retired" | "synced")
}

fn tracked_file(root: &Path, tracked: &BTreeSet<String>, path: &str) -> Result<()> {
    relative(path)?;
    if !tracked.contains(path) || !root.join(path).is_file() {
        return Err(
            format!("required application or port file is missing/untracked: {path}").into(),
        );
    }
    if !root
        .join(path)
        .canonicalize()?
        .starts_with(root.canonicalize()?)
    {
        return Err(format!("tracked path escapes the checkout: {path}").into());
    }
    Ok(())
}

fn paths<'a>(port: &'a Value, key: &str) -> Result<Vec<&'a str>> {
    array(port, key)?
        .iter()
        .map(|value| {
            let path = value.as_str().ok_or("port paths must be strings")?;
            relative(path)?;
            Ok(path)
        })
        .collect()
}

fn crate_inputs<'a>(tracked: &'a BTreeSet<String>, roots: &[&str]) -> Result<Vec<&'a str>> {
    if roots.is_empty() {
        return Err("a port must identify its Rust crates and local crate dependencies".into());
    }
    for root in roots {
        relative(root)?;
        if !tracked.contains(&format!("{root}/Cargo.toml")) {
            return Err(format!("mapped Rust crate has no tracked manifest: {root}").into());
        }
    }
    Ok(tracked
        .iter()
        .filter(|path| roots.iter().any(|root| path.starts_with(&format!("{root}/"))))
        .map(String::as_str)
        .collect())
}

fn source_tree(root: &Path) -> Result<BTreeMap<String, String>> {
    let mut args = vec!["ls-tree", "-r", "-z", RECOVERY_COMMIT, "--"];
    args.extend(PROTECTED_ROOTS);
    let bytes = git(root, &args)?;
    let mut files = BTreeMap::new();
    for entry in bytes
        .split(|byte| *byte == 0)
        .filter(|entry| !entry.is_empty())
    {
        let entry = std::str::from_utf8(entry)?;
        let (header, path) = entry
            .split_once('\t')
            .ok_or("invalid recovery tree entry")?;
        let fields: Vec<_> = header.split_whitespace().collect();
        if fields.len() != 3 || fields[1] != "blob" {
            return Err(format!("unsupported recovery tree entry: {path}").into());
        }
        files.insert(path.to_owned(), fields[2].to_owned());
    }
    if files.is_empty() {
        return Err("immutable recovery source is unavailable; fetch its exact commit".into());
    }
    Ok(files)
}

fn evidence(
    root: &Path,
    tracked: &BTreeSet<String>,
    port: &Value,
    route: &Value,
    target: &str,
    path: &str,
    current_inputs: &[&str],
) -> Result<()> {
    tracked_file(root, tracked, path)?;
    let receipt: Value = serde_json::from_slice(&fs::read(root.join(path))?)?;
    if receipt["schema_version"] != 1
        || text(&receipt, "result")? != "passed"
        || text(&receipt, "port")? != text(port, "id")?
        || text(&receipt, "target")? != target
        || text(&receipt, "upstream_commit")? != text(port, "reviewed_upstream_commit")?
        || text(&receipt, "compiler_route")? != text(route, "compiler_route")?
    {
        return Err(format!("acceptance receipt does not qualify this port/target: {path}").into());
    }
    let run = text(&receipt, "run_url")?;
    let run_id = run
        .strip_prefix("https://github.com/glopyglerky/t3code-ls/actions/runs/")
        .ok_or("acceptance receipt must link the fork's exact Actions run")?;
    if run_id.is_empty() || !run_id.bytes().all(|b| b.is_ascii_digit()) {
        return Err("acceptance run URL must identify one numeric run".into());
    }
    let checks = paths(&receipt, "checks")?;
    for required in [
        "normal-client-flow",
        "contract-parity",
        "failure-recovery",
        "target-build",
    ] {
        if !checks.contains(&required) {
            return Err(format!("acceptance receipt lacks {required}: {path}").into());
        }
    }
    for input in current_inputs.iter().copied().chain([
        "Cargo.toml",
        "Cargo.lock",
        "pnpm-lock.yaml",
        "lust-toolchain.json",
        "toolchain.lock.json",
    ]) {
        tracked_file(root, tracked, input)?;
        let actual = String::from_utf8(git(root, &["hash-object", "--", input])?)?;
        if receipt["source_hashes"][input].as_str() != Some(actual.trim()) {
            return Err(format!("acceptance evidence is stale or omits {input}: {path}").into());
        }
    }
    Ok(())
}

fn retirement_targets(port: &Value) -> Result<BTreeSet<&str>> {
    let mut targets = BTreeSet::new();
    for route in array(port, "platform_routes")? {
        if qualified(text(route, "state")?) {
            if !matches!(
                text(route, "implementation")?,
                "rust" | "rust-backed-client"
            ) {
                return Err(
                    "accepted port routes must execute Rust in the actual application".into(),
                );
            }
            for target in paths(route, "targets")? {
                if route["acceptance_evidence"][target].as_str().is_none() {
                    return Err(
                        format!("qualified target lacks acceptance evidence: {target}").into(),
                    );
                }
                targets.insert(target);
            }
        }
    }
    Ok(targets)
}

fn authorize_retirements(
    map: &Value,
    baseline: &BTreeMap<String, String>,
) -> Result<BTreeSet<String>> {
    let mut retired = BTreeSet::new();
    let all_targets: BTreeSet<_> = TARGETS.iter().copied().collect();
    for port in array(map, "ports")? {
        for entry in array(port, "retired_paths")? {
            let path = text(entry, "path")?;
            relative(path)?;
            if !matches!(text(port, "state")?, "retired" | "synced")
                || retirement_targets(port)? != all_targets
            {
                return Err(format!(
                    "removing {path} requires retirement evidence for every product target"
                )
                .into());
            }
            if baseline.get(path).map(String::as_str) != Some(text(entry, "source_blob")?) {
                return Err(format!(
                    "retirement does not identify the recovery source blob: {path}"
                )
                .into());
            }
            let mapped_source = array(port, "upstream_sources")?
                .iter()
                .any(|source| source["path"].as_str() == Some(path));
            if !mapped_source && !paths(port, "adapter_paths")?.contains(&path) {
                return Err(format!("retired path is not mapped to this port: {path}").into());
            }
            if !retired.insert(path.to_owned()) {
                return Err(format!("duplicate source retirement: {path}").into());
            }
        }
    }
    Ok(retired)
}

fn require_preserved(
    baseline: &BTreeMap<String, String>,
    present: &BTreeSet<String>,
    retired: &BTreeSet<String>,
) -> Result<()> {
    let missing: Vec<_> = baseline
        .keys()
        .filter(|path| !present.contains(*path) && !retired.contains(*path))
        .collect();
    if !missing.is_empty() {
        return Err(format!(
            "{} recovery source files disappeared without accepted replacements; first paths: {}",
            missing.len(),
            missing
                .iter()
                .take(8)
                .map(|path| path.as_str())
                .collect::<Vec<_>>()
                .join(", "),
        )
        .into());
    }
    Ok(())
}

pub fn check(root: &Path) -> Result<()> {
    let bytes = git(root, &["ls-files", "-z"])?;
    let tracked: BTreeSet<String> = bytes
        .split(|byte| *byte == 0)
        .filter(|entry| !entry.is_empty())
        .map(|entry| Ok(std::str::from_utf8(entry)?.to_owned()))
        .collect::<Result<_>>()?;
    if fs::symlink_metadata(root.join(".repos")).is_ok()
        || tracked
            .iter()
            .any(|path| path == ".repos" || path.starts_with(".repos/"))
    {
        return Err("keep disposable upstream reference checkouts outside the product tree".into());
    }
    for path in REQUIRED {
        tracked_file(root, &tracked, path)?;
    }
    let map: Value = serde_json::from_slice(&fs::read(root.join("port-map.json"))?)?;
    let upstream: Value = serde_json::from_slice(&fs::read(root.join("upstream.lock.json"))?)?;
    if map["schema_version"] != 1 || text(&map, "recovery_commit")? != RECOVERY_COMMIT {
        return Err("port map must retain the immutable hybrid recovery baseline".into());
    }
    if paths(&map, "supported_targets")?.as_slice() != TARGETS {
        return Err("port map must retain every supported product target".into());
    }
    let integrated = text(&map, "integrated_upstream_commit")?;
    identity(integrated)?;
    if upstream["upstream"]["commit"].as_str() != Some(integrated) {
        return Err("port map and integrated upstream source disagree".into());
    }
    let baseline = source_tree(root)?;
    let retired = authorize_retirements(&map, &baseline)?;
    let present: BTreeSet<_> = tracked
        .iter()
        .filter(|path| root.join(path).is_file())
        .cloned()
        .collect();
    require_preserved(&baseline, &present, &retired)?;
    let mut ids = BTreeSet::new();
    for port in array(&map, "ports")? {
        let id = text(port, "id")?;
        if !ids.insert(id) {
            return Err(format!("duplicate port id: {id}").into());
        }
        state(text(port, "state")?)?;
        relative(text(port, "domain_owner")?)?;
        let reviewed = text(port, "reviewed_upstream_commit")?;
        identity(reviewed)?;
        if reviewed != integrated {
            return Err(
                format!("port {id} has not reviewed the integrated upstream revision").into(),
            );
        }
        for source in array(port, "upstream_sources")? {
            let path = text(source, "path")?;
            relative(path)?;
            let blob = text(source, "blob")?;
            identity(blob)?;
            let actual =
                String::from_utf8(git(root, &["rev-parse", &format!("{reviewed}:{path}")])?)?;
            if actual.trim() != blob {
                return Err(format!("upstream source identity mismatch: {path}").into());
            }
        }
        let rust = paths(port, "rust_paths")?;
        if rust.is_empty() {
            return Err(format!("port {id} has no Rust implementation paths").into());
        }
        let tests = paths(port, "acceptance_tests")?;
        if tests.is_empty() {
            return Err(format!("port {id} has no acceptance test paths").into());
        }
        let adapters = paths(port, "adapter_paths")?
            .into_iter()
            .filter(|path| !retired.contains(*path));
        let crate_files = crate_inputs(&tracked, &paths(port, "rust_crates")?)?;
        let current_inputs: Vec<_> = rust
            .into_iter()
            .chain(adapters)
            .chain(tests)
            .chain(crate_files)
            .collect();
        for path in &current_inputs {
            tracked_file(root, &tracked, path)?;
        }
        let mut seen_targets = BTreeSet::new();
        for route in array(port, "platform_routes")? {
            let route_state = text(route, "state")?;
            state(route_state)?;
            text(route, "implementation")?;
            text(route, "compiler_route")?;
            let targets = paths(route, "targets")?;
            if targets.is_empty() {
                return Err(format!("empty target route for {id}").into());
            }
            for target in targets {
                if !TARGETS.contains(&target) || !seen_targets.insert(target) {
                    return Err(format!("unknown or duplicated port target: {id}/{target}").into());
                }
                if qualified(route_state) {
                    if !matches!(
                        text(route, "implementation")?,
                        "rust" | "rust-backed-client"
                    ) {
                        return Err(
                            "accepted port routes must execute Rust in the actual application"
                                .into(),
                        );
                    }
                    let receipt = route["acceptance_evidence"][target]
                        .as_str()
                        .ok_or("accepted target requires an acceptance receipt")?;
                    evidence(
                        root,
                        &tracked,
                        port,
                        route,
                        target,
                        receipt,
                        &current_inputs,
                    )?;
                }
            }
        }
        if qualified(text(port, "state")?) && retirement_targets(port)?.is_empty() {
            return Err(format!("port {id} claims acceptance without qualified targets").into());
        }
    }
    println!(
        "Checked {} protected recovery files and {} mapped ports; application source is retained until target acceptance. This is a structure check, not application qualification.",
        baseline.len(),
        ids.len()
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn path_checks_allow_upstream_languages_and_reject_escape() {
        for path in [
            "apps/web/src/index.tsx",
            "package.json",
            "native/helper/main.c",
            "assets/main.css",
        ] {
            assert!(relative(path).is_ok());
        }
        for path in [
            "../outside",
            "/tmp/source",
            "apps//file",
            "apps/./file",
            "apps\\file",
            "C:/file",
        ] {
            assert!(relative(path).is_err());
        }
    }

    #[test]
    fn missing_recovery_source_cannot_pass_as_a_lower_rust_percentage() {
        let baseline = BTreeMap::from([("apps/web/src/index.tsx".into(), "original".into())]);
        assert!(require_preserved(&baseline, &BTreeSet::new(), &BTreeSet::new()).is_err());
        assert!(
            require_preserved(
                &baseline,
                &BTreeSet::from(["apps/web/src/index.tsx".into()]),
                &BTreeSet::new()
            )
            .is_ok()
        );
    }

    #[test]
    fn component_tests_and_linux_receipts_do_not_authorize_shared_retirement() {
        let baseline =
            BTreeMap::from([("packages/shared/src/policy.ts".into(), "original".into())]);
        let mut map = json!({"ports":[{
            "id":"policy", "state":"candidate",
            "upstream_sources":[{"path":"packages/shared/src/policy.ts"}],
            "adapter_paths":[],
            "retired_paths":[{"path":"packages/shared/src/policy.ts","source_blob":"original"}],
            "platform_routes":[{"targets":["linux-x64"],"state":"accepted","implementation":"rust","acceptance_evidence":{"linux-x64":"qualification/linux.json"}}]
        }]});
        assert!(authorize_retirements(&map, &baseline).is_err());
        map["ports"][0]["state"] = json!("retired");
        assert!(authorize_retirements(&map, &baseline).is_err());
        map["ports"][0]["platform_routes"][0]["targets"] = json!(TARGETS);
        let evidence: BTreeMap<_, _> = TARGETS
            .iter()
            .map(|target| (*target, format!("qualification/{target}.json")))
            .collect();
        map["ports"][0]["platform_routes"][0]["acceptance_evidence"] = json!(evidence);
        assert!(authorize_retirements(&map, &baseline).is_ok());
        map["ports"][0]["platform_routes"][0]["implementation"] = json!("upstream");
        assert!(authorize_retirements(&map, &baseline).is_err());
        map["ports"][0]["platform_routes"][0]["implementation"] = json!("rust");
        map["ports"][0]["retired_paths"][0]["source_blob"] = json!("different-source");
        assert!(authorize_retirements(&map, &baseline).is_err());
    }

    #[test]
    fn accepted_without_receipt_and_undefined_states_fail() {
        let port = json!({"platform_routes":[{"targets":["linux-x64"],"state":"accepted","implementation":"rust","acceptance_evidence":{}}]});
        assert!(retirement_targets(&port).is_err());
        assert!(state("compiled").is_err());
        assert!(state("integrated").is_ok());
    }

    #[test]
    fn acceptance_receipt_expires_when_an_adapter_compiler_or_shared_decoder_changes() {
        struct Directory(std::path::PathBuf);
        impl Drop for Directory {
            fn drop(&mut self) {
                let _ = fs::remove_dir_all(&self.0);
            }
        }
        let name = format!(
            "t3-port-map-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let directory = Directory(std::env::temp_dir().join(name));
        fs::create_dir(&directory.0).unwrap();
        let root = &directory.0;
        let inputs = ["replacement.rs", "adapter.ts", "acceptance.test.ts"];
        let mut hashes = BTreeMap::new();
        let mut tracked = BTreeSet::new();
        for path in
            inputs
                .into_iter()
                .chain([
                    "Cargo.toml",
                    "Cargo.lock",
                    "pnpm-lock.yaml",
                    "lust-toolchain.json",
                    "toolchain.lock.json",
                    "crates/core/Cargo.toml",
                    "crates/core/src/json.rs",
                ])
        {
            fs::create_dir_all(root.join(path).parent().unwrap()).unwrap();
            fs::write(root.join(path), "qualified source\n").unwrap();
            hashes.insert(
                path,
                String::from_utf8(git(root, &["hash-object", "--", path]).unwrap())
                    .unwrap()
                    .trim()
                    .to_owned(),
            );
            tracked.insert(path.to_owned());
        }
        let port = json!({"id":"policy","reviewed_upstream_commit":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"});
        let route = json!({"compiler_route":"lustr-bluefe-compatibility"});
        let receipt = json!({
            "schema_version":1,"port":"policy","target":"linux-x64","result":"passed",
            "upstream_commit":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "compiler_route":"lustr-bluefe-compatibility",
            "run_url":"https://github.com/glopyglerky/t3code-ls/actions/runs/123",
            "checks":["normal-client-flow","contract-parity","failure-recovery","target-build"],
            "source_hashes":hashes,
        });
        fs::write(
            root.join("receipt.json"),
            serde_json::to_vec(&receipt).unwrap(),
        )
        .unwrap();
        tracked.insert("receipt.json".to_owned());
        let mut all_inputs = inputs.to_vec();
        all_inputs.extend(crate_inputs(&tracked, &["crates/core"]).unwrap());
        assert!(all_inputs.contains(&"crates/core/src/json.rs"));
        assert!(all_inputs.contains(&"crates/core/Cargo.toml"));
        assert!(
            evidence(
                root,
                &tracked,
                &port,
                &route,
                "linux-x64",
                "receipt.json",
                &all_inputs
            )
            .is_ok()
        );
        for changed in ["adapter.ts", "toolchain.lock.json", "crates/core/src/json.rs"] {
            fs::write(root.join(changed), "changed acceptance input\n").unwrap();
            let error = evidence(
                root,
                &tracked,
                &port,
                &route,
                "linux-x64",
                "receipt.json",
                &all_inputs,
            )
            .unwrap_err();
            assert!(error.to_string().contains(&format!("stale or omits {changed}")));
            fs::write(root.join(changed), "qualified source\n").unwrap();
        }
    }
}
