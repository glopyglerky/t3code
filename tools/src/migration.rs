//! Migration accounting protects the working application, regardless of language.
use crate::{output, Result};
use serde_json::Value;
use std::{collections::BTreeMap, collections::BTreeSet, fs, path::Path, process::Command};

const RECOVERY_COMMIT: &str = "d29a94f3a175eac1f26c548023fedd8695cedfaf";
const RECOVERY_UPSTREAM: &str = "521ac7c218b3faa74281fb72fc36ce74a0951fb3";
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
        .filter(|path| {
            roots
                .iter()
                .any(|root| path.starts_with(&format!("{root}/")))
        })
        .map(String::as_str)
        .collect())
}

fn source_tree(root: &Path, revision: &str) -> Result<BTreeMap<String, String>> {
    identity(revision)?;
    if git(root, &["cat-file", "-t", revision])? != b"commit\n" {
        return Err(format!("source history object is not a commit: {revision}").into());
    }
    let mut args = vec!["ls-tree", "-r", "-z", revision, "--"];
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
            .ok_or("invalid protected source tree entry")?;
        let fields: Vec<_> = header.split_whitespace().collect();
        if fields.len() != 3 || fields[1] != "blob" {
            return Err(format!("unsupported protected source tree entry: {path}").into());
        }
        relative(path)?;
        files.insert(path.to_owned(), fields[2].to_owned());
    }
    if files.is_empty() {
        return Err("immutable protected source is unavailable; fetch its exact commit".into());
    }
    Ok(files)
}

struct SourceHistory {
    recovery: BTreeMap<String, String>,
    snapshots: Vec<(String, BTreeMap<String, String>)>,
    protected: BTreeMap<String, String>,
}

impl SourceHistory {
    fn new(
        recovery: BTreeMap<String, String>,
        snapshots: Vec<(String, BTreeMap<String, String>)>,
    ) -> Self {
        let mut protected = recovery.clone();
        for (_, tree) in &snapshots {
            protected.extend(tree.clone());
        }
        Self {
            recovery,
            snapshots,
            protected,
        }
    }

    fn current(&self) -> &BTreeMap<String, String> {
        &self
            .snapshots
            .last()
            .expect("validated nonempty source history")
            .1
    }

    fn latest_source(&self, path: &str) -> Option<(usize, &str, &str)> {
        self.snapshots
            .iter()
            .enumerate()
            .rev()
            .find_map(|(index, (revision, tree))| {
                tree.get(path)
                    .map(|blob| (index, revision.as_str(), blob.as_str()))
            })
    }

    fn blob(&self, revision: &str, path: &str) -> Option<&str> {
        if revision == RECOVERY_COMMIT {
            return self.recovery.get(path).map(String::as_str);
        }
        self.snapshots
            .iter()
            .find(|(commit, _)| commit == revision)
            .and_then(|(_, tree)| tree.get(path))
            .map(String::as_str)
    }
}

fn history_revisions(map: &Value) -> Result<Vec<&str>> {
    let revisions = paths(map, "source_history")?;
    if revisions.first().copied() != Some(RECOVERY_UPSTREAM)
        || revisions.last().copied() != Some(text(map, "integrated_upstream_commit")?)
    {
        return Err(
            "source history must start at the recovery upstream and end at the integrated revision"
                .into(),
        );
    }
    let mut seen = BTreeSet::new();
    for revision in &revisions {
        identity(revision)?;
        if !seen.insert(*revision) {
            return Err("source history contains a duplicate integration".into());
        }
    }
    Ok(revisions)
}

fn source_history(root: &Path, map: &Value) -> Result<SourceHistory> {
    let recovery = source_tree(root, RECOVERY_COMMIT)?;
    let snapshots = history_revisions(map)?
        .into_iter()
        .map(|revision| Ok((revision.to_owned(), source_tree(root, revision)?)))
        .collect::<Result<_>>()?;
    Ok(SourceHistory::new(recovery, snapshots))
}

fn authorize_upstream_removals(
    map: &Value,
    history: &SourceHistory,
    present: &BTreeSet<String>,
    retired: &BTreeSet<String>,
) -> Result<BTreeSet<String>> {
    let mut removed = BTreeSet::new();
    for removal in array(map, "upstream_removals")? {
        let path = text(removal, "path")?;
        relative(path)?;
        let source = text(removal, "source_commit")?;
        let blob = text(removal, "source_blob")?;
        let removed_in = text(removal, "removed_in")?;
        for value in [source, blob, removed_in] {
            identity(value)?;
        }
        text(removal, "reason")?;
        text(removal, "fork_disposition")?;
        let (index, actual_source, actual_blob) = history.latest_source(path).ok_or_else(|| {
            format!("upstream removal cannot remove a fork-only or unknown file: {path}")
        })?;
        if source != actual_source
            || blob != actual_blob
            || history
                .snapshots
                .get(index + 1)
                .map(|(revision, _)| revision.as_str())
                != Some(removed_in)
            || history.current().contains_key(path)
        {
            return Err(format!("upstream removal is stale or does not identify the actual source transition: {path}").into());
        }
        // Acknowledging upstream's deletion does not account for the fork's edits.
        // Preserve their recovery identity and require a separate disposition review.
        if let Some(recovered) = history.recovery.get(path) {
            if removal["recovery_blob"].as_str() != Some(recovered.as_str()) {
                return Err(format!(
                    "upstream removal omits the recovered fork source identity: {path}"
                )
                .into());
            }
        }
        for replacement in paths(removal, "replacement_paths")? {
            if !history.current().contains_key(replacement)
                || (!present.contains(replacement) && !retired.contains(replacement))
            {
                return Err(format!(
                    "upstream replacement must be integrated or accepted as retired: {replacement}"
                )
                .into());
            }
        }
        if !removed.insert(path.to_owned()) {
            return Err(format!("duplicate upstream source removal: {path}").into());
        }
    }
    Ok(removed)
}

fn upstream_source(
    source: &Value,
    integrated: &str,
    history: &SourceHistory,
    removed: &BTreeSet<String>,
) -> Result<()> {
    let path = text(source, "path")?;
    relative(path)?;
    let blob = text(source, "blob")?;
    identity(blob)?;
    let revision = if source.get("source_commit").is_some() {
        text(source, "source_commit")?
    } else {
        integrated
    };
    identity(revision)?;
    if history.blob(revision, path) != Some(blob) {
        return Err(format!("upstream source identity mismatch: {path}").into());
    }
    if revision != integrated {
        let latest = history.latest_source(path);
        if !removed.contains(path)
            || latest.map(|(_, commit, source_blob)| (commit, source_blob))
                != Some((revision, blob))
        {
            return Err(
                format!("historical mapping needs a reviewed upstream removal: {path}").into(),
            );
        }
    }
    Ok(())
}

fn check_upstream_sources(
    port: &Value,
    map: &Value,
    integrated: &str,
    history: &SourceHistory,
    removed: &BTreeSet<String>,
) -> Result<()> {
    let sources = array(port, "upstream_sources")?;
    for source in sources {
        upstream_source(source, integrated, history, removed)?;
        let Some(revision) = source["source_commit"].as_str() else {
            continue;
        };
        if revision == integrated {
            continue;
        }
        let path = text(source, "path")?;
        let removal = array(map, "upstream_removals")?
            .iter()
            .find(|entry| entry["path"].as_str() == Some(path))
            .ok_or("historical source has no upstream removal record")?;
        for replacement in paths(removal, "replacement_paths")? {
            if !sources.iter().any(|entry| {
                entry["path"].as_str() == Some(replacement)
                    && entry
                        .get("source_commit")
                        .is_none_or(|value| value.as_str() == Some(integrated))
            }) {
                return Err(format!(
                    "port {} must map upstream replacement {replacement} as a current source",
                    text(port, "id")?
                )
                .into());
            }
        }
    }
    Ok(())
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

fn authorize_retirements(map: &Value, history: &SourceHistory) -> Result<BTreeSet<String>> {
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
            let source = text(entry, "source_commit")?;
            let blob = text(entry, "source_blob")?;
            identity(source)?;
            identity(blob)?;
            if history.blob(source, path) != Some(blob) {
                return Err(format!(
                    "retirement does not identify a protected historical source blob: {path}"
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
    // Shared adapters and sources cannot inherit another owner's acceptance.
    // check() validates each owner's current receipts after this authorization.
    for path in &retired {
        for port in array(map, "ports")? {
            let owns_source = array(port, "upstream_sources")?
                .iter()
                .any(|source| source["path"].as_str() == Some(path.as_str()));
            if (owns_source || paths(port, "adapter_paths")?.contains(&path.as_str()))
                && (!matches!(text(port, "state")?, "retired" | "synced")
                    || retirement_targets(port)? != all_targets)
            {
                return Err(format!(
                    "shared retirement of {path} lacks acceptance for owner {}",
                    text(port, "id")?
                )
                .into());
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
            "{} protected source files disappeared without accepted replacements or reviewed upstream removals; first paths: {}",
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
    let history = source_history(root, &map)?;
    let retired = authorize_retirements(&map, &history)?;
    let present: BTreeSet<_> = tracked
        .iter()
        .filter(|path| root.join(path).is_file())
        .cloned()
        .collect();
    let removed = authorize_upstream_removals(&map, &history, &present, &retired)?;
    let accounted = retired.union(&removed).cloned().collect();
    require_preserved(&history.protected, &present, &accounted)?;
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
        check_upstream_sources(port, &map, integrated, &history, &removed)?;
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
        "Checked {} protected files across recovery and {} integrated upstream revisions, {} reviewed upstream removals and {} mapped ports. This is a structure check, not application qualification.",
        history.protected.len(),
        history.snapshots.len(),
        removed.len(),
        ids.len()
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const NEXT: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const LATER: &str = "cccccccccccccccccccccccccccccccccccccccc";
    const BLOB: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    const OLD: &str = "apps/server/src/old.ts";
    const NEW: &str = "apps/server/src/new.ts";

    fn files(paths: &[&str]) -> BTreeMap<String, String> {
        paths
            .iter()
            .map(|path| ((*path).to_owned(), BLOB.to_owned()))
            .collect()
    }

    fn removal() -> Value {
        json!({
            "path": OLD,
            "source_commit": RECOVERY_UPSTREAM,
            "source_blob": BLOB,
            "removed_in": NEXT,
            "recovery_blob": BLOB,
            "reason": "Upstream moved the implementation to new.ts.",
            "fork_disposition": "The fork adapter remains required and now calls new.ts.",
            "replacement_paths": [NEW]
        })
    }

    fn removed_history() -> SourceHistory {
        SourceHistory::new(
            files(&[OLD]),
            vec![
                (RECOVERY_UPSTREAM.into(), files(&[OLD])),
                (NEXT.into(), files(&[NEW])),
            ],
        )
    }

    #[test]
    fn integration_history_is_anchored_complete_and_unique() {
        let mut map = json!({
            "integrated_upstream_commit": NEXT,
            "source_history": [RECOVERY_UPSTREAM, NEXT],
        });
        assert_eq!(history_revisions(&map).unwrap(), [RECOVERY_UPSTREAM, NEXT]);
        for revisions in [
            json!([]),
            json!([NEXT]),
            json!([RECOVERY_UPSTREAM]),
            json!([RECOVERY_UPSTREAM, NEXT, NEXT]),
            json!([RECOVERY_UPSTREAM, "main", NEXT]),
        ] {
            map["source_history"] = revisions;
            assert!(history_revisions(&map).is_err());
        }
    }

    #[test]
    fn later_integrated_files_stay_protected_even_after_upstream_removes_them() {
        let history = SourceHistory::new(
            files(&[OLD]),
            vec![
                (RECOVERY_UPSTREAM.into(), files(&[OLD])),
                (NEXT.into(), files(&[OLD, NEW])),
                (LATER.into(), files(&[OLD])),
            ],
        );
        let present = BTreeSet::from([OLD.to_owned()]);
        assert!(history.protected.contains_key(NEW));
        assert!(require_preserved(&history.protected, &present, &BTreeSet::new()).is_err());
        let mut record = removal();
        record["path"] = json!(NEW);
        record["source_commit"] = json!(NEXT);
        record["removed_in"] = json!(LATER);
        record["replacement_paths"] = json!([]);
        let map = json!({"upstream_removals": [record]});
        let removed =
            authorize_upstream_removals(&map, &history, &present, &BTreeSet::new()).unwrap();
        require_preserved(&history.protected, &present, &removed).unwrap();
    }

    #[test]
    fn reviewed_upstream_removal_is_separate_from_rust_acceptance() {
        let history = removed_history();
        let present = BTreeSet::from([NEW.to_owned()]);
        assert!(require_preserved(&history.protected, &present, &BTreeSet::new()).is_err());
        let map = json!({"upstream_removals": [removal()]});
        let removed =
            authorize_upstream_removals(&map, &history, &present, &BTreeSet::new()).unwrap();
        require_preserved(&history.protected, &present, &removed).unwrap();
        // An upstream removal is not in the accepted retirement set. Existing
        // mapped adapters therefore remain mandatory current inputs in check().
        assert_eq!(removed, BTreeSet::from([OLD.to_owned()]));
    }

    #[test]
    fn upstream_removals_bind_provenance_replacements_and_fork_disposition() {
        let history = removed_history();
        let present = BTreeSet::from([NEW.to_owned()]);
        for (key, bad) in [
            ("source_commit", json!(NEXT)),
            ("source_blob", json!(NEXT)),
            ("removed_in", json!(LATER)),
            ("recovery_blob", json!(NEXT)),
            ("reason", json!("")),
            ("fork_disposition", json!("")),
            ("replacement_paths", json!(["apps/server/missing.ts"])),
            ("replacement_paths", json!(["../outside"])),
        ] {
            let mut record = removal();
            record[key] = bad;
            assert!(
                authorize_upstream_removals(
                    &json!({"upstream_removals":[record]}),
                    &history,
                    &present,
                    &BTreeSet::new()
                )
                .is_err(),
                "accepted invalid {key}"
            );
        }
        let map = json!({"upstream_removals":[removal()]});
        assert!(
            authorize_upstream_removals(&map, &history, &BTreeSet::new(), &BTreeSet::new())
                .is_err()
        );
        // Upstream can remove a source after its Rust replacement already retired
        // the fork's copy. Both records still carry independent provenance.
        assert!(authorize_upstream_removals(
            &map,
            &history,
            &present,
            &BTreeSet::from([OLD.to_owned()])
        )
        .is_ok());
        assert!(authorize_upstream_removals(
            &json!({"upstream_removals":[removal(), removal()]}),
            &history,
            &present,
            &BTreeSet::new()
        )
        .is_err());
    }

    #[test]
    fn upstream_absence_never_authorizes_removing_fork_only_adapters() {
        let history = SourceHistory::new(
            files(&[OLD]),
            vec![
                (RECOVERY_UPSTREAM.into(), files(&[NEW])),
                (NEXT.into(), files(&[NEW])),
            ],
        );
        assert!(authorize_upstream_removals(
            &json!({"upstream_removals":[removal()]}),
            &history,
            &BTreeSet::from([NEW.to_owned()]),
            &BTreeSet::new()
        )
        .is_err());
    }

    #[test]
    fn historical_translation_mapping_requires_reviewed_removal() {
        let history = removed_history();
        let source = json!({"path":OLD,"blob":BLOB,"source_commit":RECOVERY_UPSTREAM});
        assert!(upstream_source(&source, NEXT, &history, &BTreeSet::new()).is_err());
        let removed = BTreeSet::from([OLD.to_owned()]);
        upstream_source(&source, NEXT, &history, &removed).unwrap();
        let no_provenance = json!({"path":OLD,"blob":BLOB});
        assert!(upstream_source(&no_provenance, NEXT, &history, &removed).is_err());
        upstream_source(&json!({"path":NEW,"blob":BLOB}), NEXT, &history, &removed).unwrap();
    }

    #[test]
    fn renamed_replacements_keep_their_port_owner_and_current_blob() {
        let history = removed_history();
        let map = json!({"upstream_removals":[removal()]});
        let removed = BTreeSet::from([OLD.to_owned()]);
        let mut port = json!({"id":"renamed-port", "upstream_sources":[
            {"path":OLD,"blob":BLOB,"source_commit":RECOVERY_UPSTREAM}
        ]});
        assert!(check_upstream_sources(&port, &map, NEXT, &history, &removed).is_err());
        port["upstream_sources"]
            .as_array_mut()
            .unwrap()
            .push(json!({"path":NEW,"blob":BLOB}));
        check_upstream_sources(&port, &map, NEXT, &history, &removed).unwrap();
        port["upstream_sources"][1]["blob"] = json!(NEXT);
        assert!(check_upstream_sources(&port, &map, NEXT, &history, &removed).is_err());
        port["upstream_sources"][1]["blob"] = json!(BLOB);
        port["upstream_sources"][1]["source_commit"] = json!(RECOVERY_UPSTREAM);
        assert!(check_upstream_sources(&port, &map, NEXT, &history, &removed).is_err());
    }

    #[test]
    fn reintroduced_upstream_source_invalidates_old_removal_and_historical_mapping() {
        let mut history = removed_history();
        history.snapshots.push((LATER.into(), files(&[OLD, NEW])));
        let present = BTreeSet::from([NEW.to_owned()]);
        assert!(authorize_upstream_removals(
            &json!({"upstream_removals":[removal()]}),
            &history,
            &present,
            &BTreeSet::new()
        )
        .is_err());
        let source = json!({"path":OLD,"blob":BLOB,"source_commit":RECOVERY_UPSTREAM});
        assert!(
            upstream_source(&source, LATER, &history, &BTreeSet::from([OLD.to_owned()])).is_err()
        );
    }

    #[test]
    fn retirement_can_identify_a_file_introduced_after_recovery() {
        let history = SourceHistory::new(
            files(&[OLD]),
            vec![
                (RECOVERY_UPSTREAM.into(), files(&[OLD])),
                (NEXT.into(), files(&[OLD, NEW])),
            ],
        );
        let receipts: BTreeMap<_, _> = TARGETS
            .iter()
            .map(|target| (*target, format!("qualification/{target}.json")))
            .collect();
        let mut map = json!({"ports":[{
            "id":"new-port", "state":"retired",
            "upstream_sources":[{"path":NEW}], "adapter_paths":[],
            "retired_paths":[{"path":NEW,"source_commit":NEXT,"source_blob":BLOB}],
            "platform_routes":[{"targets":TARGETS,"state":"accepted","implementation":"rust","acceptance_evidence":receipts}]
        }]});
        assert_eq!(
            authorize_retirements(&map, &history).unwrap(),
            BTreeSet::from([NEW.to_owned()])
        );
        map["ports"][0]["retired_paths"][0]["source_commit"] = json!(RECOVERY_COMMIT);
        assert!(authorize_retirements(&map, &history).is_err());
    }

    #[test]
    fn one_ports_acceptance_cannot_retire_another_owners_shared_adapter() {
        let history = SourceHistory::new(
            files(&[OLD]),
            vec![(RECOVERY_UPSTREAM.into(), files(&[OLD]))],
        );
        let receipts: BTreeMap<_, _> = TARGETS
            .iter()
            .map(|target| (*target, format!("qualification/{target}.json")))
            .collect();
        let accepted = json!({"id":"accepted-owner", "state":"retired",
            "upstream_sources":[{"path":OLD}], "adapter_paths":[OLD],
            "retired_paths":[{"path":OLD,"source_commit":RECOVERY_COMMIT,"source_blob":BLOB}],
            "platform_routes":[{"targets":TARGETS,"state":"accepted","implementation":"rust","acceptance_evidence":receipts}]
        });
        let other = json!({"id":"unaccepted-owner", "state":"integrated",
            "upstream_sources":[], "adapter_paths":[OLD], "retired_paths":[],
            "platform_routes":[{"targets":TARGETS,"state":"integrated","implementation":"upstream","acceptance_evidence":{}}]
        });
        let mut map = json!({"ports":[accepted, other]});
        assert!(authorize_retirements(&map, &history)
            .unwrap_err()
            .to_string()
            .contains("unaccepted-owner"));
        map["ports"][1]["state"] = json!("synced");
        map["ports"][1]["platform_routes"] = map["ports"][0]["platform_routes"].clone();
        assert!(authorize_retirements(&map, &history).is_ok());
        map["ports"][1]["platform_routes"][0]["targets"] = json!(["linux-x64"]);
        assert!(authorize_retirements(&map, &history).is_err());
    }

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
        assert!(require_preserved(
            &baseline,
            &BTreeSet::from(["apps/web/src/index.tsx".into()]),
            &BTreeSet::new()
        )
        .is_ok());
    }

    #[test]
    fn component_tests_and_linux_receipts_do_not_authorize_shared_retirement() {
        let blob = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        let history = SourceHistory::new(
            BTreeMap::from([("packages/shared/src/policy.ts".into(), blob.into())]),
            vec![],
        );
        let mut map = json!({"ports":[{
            "id":"policy", "state":"candidate",
            "upstream_sources":[{"path":"packages/shared/src/policy.ts"}],
            "adapter_paths":[],
            "retired_paths":[{"path":"packages/shared/src/policy.ts","source_commit":RECOVERY_COMMIT,"source_blob":blob}],
            "platform_routes":[{"targets":["linux-x64"],"state":"accepted","implementation":"rust","acceptance_evidence":{"linux-x64":"qualification/linux.json"}}]
        }]});
        assert!(authorize_retirements(&map, &history).is_err());
        map["ports"][0]["state"] = json!("retired");
        assert!(authorize_retirements(&map, &history).is_err());
        map["ports"][0]["platform_routes"][0]["targets"] = json!(TARGETS);
        let evidence: BTreeMap<_, _> = TARGETS
            .iter()
            .map(|target| (*target, format!("qualification/{target}.json")))
            .collect();
        map["ports"][0]["platform_routes"][0]["acceptance_evidence"] = json!(evidence);
        assert!(authorize_retirements(&map, &history).is_ok());
        map["ports"][0]["platform_routes"][0]["implementation"] = json!("upstream");
        assert!(authorize_retirements(&map, &history).is_err());
        map["ports"][0]["platform_routes"][0]["implementation"] = json!("rust");
        map["ports"][0]["retired_paths"][0]["source_blob"] = json!("different-source");
        assert!(authorize_retirements(&map, &history).is_err());
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
        for path in inputs.into_iter().chain([
            "Cargo.toml",
            "Cargo.lock",
            "pnpm-lock.yaml",
            "lust-toolchain.json",
            "toolchain.lock.json",
            "crates/core/Cargo.toml",
            "crates/core/src/json.rs",
        ]) {
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
        assert!(evidence(
            root,
            &tracked,
            &port,
            &route,
            "linux-x64",
            "receipt.json",
            &all_inputs
        )
        .is_ok());
        for changed in [
            "adapter.ts",
            "toolchain.lock.json",
            "crates/core/src/json.rs",
        ] {
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
            assert!(error
                .to_string()
                .contains(&format!("stale or omits {changed}")));
            fs::write(root.join(changed), "qualified source\n").unwrap();
        }
    }
}
