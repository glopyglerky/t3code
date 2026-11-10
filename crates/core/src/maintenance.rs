//! Provider installer ownership and update-action decisions.

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Owner {
    pub provider: String,
    pub package_name: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Context {
    pub resolved_command_path: String,
    pub real_command_path: String,
    pub platform: String,
    pub host_platform: String,
    pub command_platform: String,
    pub chocolatey_install: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MiseTool {
    pub tool: String,
    pub upgrade: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NativeUpdate {
    pub args: Vec<String>,
    pub matches_command_path: bool,
    pub requires_command_path: bool,
    pub has_env: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Definition {
    pub mise: Option<MiseTool>,
    pub native: Option<NativeUpdate>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Homebrew {
    pub kind: String,
    pub name: String,
    pub prefix: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MiseInstall {
    pub path: String,
    pub requested_version: Option<String>,
    pub active: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Observation {
    Wrapper,
    ResolveMise,
    MiseWhich,
    MiseListing,
    MiseInstallPaths(Vec<String>),
    MiseToolPath(String),
    PnpmVersion,
    VoltaPackage,
    NpmShim,
    HomebrewExecutable(String),
    HomebrewPrefix,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ObservationValue {
    Text(Option<String>),
    Boolean(bool),
    Paths(Vec<String>),
    Listing {
        present: bool,
        installs: Vec<MiseInstall>,
    },
    NpmShim {
        directory: String,
        exists: bool,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Observed {
    pub request: Observation,
    pub result: ObservationValue,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Lookup {
    Mise {
        executable: String,
        tool: String,
    },
    Homebrew {
        executable: String,
        ownership: Homebrew,
    },
    Pnpm {
        package_name: String,
    },
    Chocolatey,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Update {
    pub command: String,
    pub executable: String,
    pub args: Vec<String>,
    pub lock_key: String,
    pub env: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UpdateActionInput {
    pub command: String,
    pub args: Vec<String>,
    pub lock_key: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SelectedUpdateAction {
    pub command: String,
    pub args: Vec<String>,
}

fn original_update_action(update: &UpdateActionInput) -> SelectedUpdateAction {
    SelectedUpdateAction {
        command: update.command.clone(),
        args: update.args.clone(),
    }
}

pub fn make_targeted_update_action(
    package_name: Option<&str>,
    update: Option<&UpdateActionInput>,
    version: &str,
) -> Option<SelectedUpdateAction> {
    // Match JavaScript's /^\d+\.\d+\.\d+$/ without numeric parsing or trimming.
    let mut parts = version.split('.');
    for _ in 0..3 {
        let part = parts.next()?;
        if part.is_empty() || !part.bytes().all(|byte| byte.is_ascii_digit()) {
            return None;
        }
    }
    if parts.next().is_some() {
        return None;
    }
    let update = update?;
    let package_name = package_name.filter(|name| !name.is_empty())?;
    if !update.lock_key.starts_with("npm-global:")
        && !matches!(
            update.lock_key.as_str(),
            "bun-global" | "pnpm-global" | "vite-plus-global" | "yarn-global" | "volta"
        )
    {
        return None;
    }
    let latest = format!("{package_name}@latest");
    let package_index = update
        .args
        .iter()
        .position(|arg| arg == &latest || arg == package_name)?;
    let previous = &update.args[package_index];
    let target = format!("{package_name}@{version}");
    let mut selected = original_update_action(update);
    selected.args[package_index] = target.clone();
    if let Some(command_index) = selected.command.rfind(previous) {
        selected
            .command
            .replace_range(command_index..command_index + previous.len(), &target);
    }
    Some(selected)
}

pub fn resolve_latest_update_action(
    package_name: Option<&str>,
    update: Option<&UpdateActionInput>,
    latest_version: Option<&str>,
) -> Option<SelectedUpdateAction> {
    let update = update?;
    if update.lock_key == "pnpm-global" {
        if let Some(version) = latest_version {
            if let Some(selected) = make_targeted_update_action(package_name, Some(update), version)
            {
                return Some(selected);
            }
        }
    }
    Some(original_update_action(update))
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Capabilities {
    pub owner: Owner,
    pub update: Option<Update>,
    pub install_path: Option<String>,
    pub manual_command: Option<String>,
    pub latest_unknown: bool,
    pub latest_lookup: Option<Lookup>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Request {
    pub mode: String,
    pub owner: Owner,
    pub context: Option<Context>,
    pub definition: Definition,
    pub homebrew: Option<Homebrew>,
    pub observations: Vec<Observed>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Decision {
    Observe(Observation),
    Resolved(Option<Box<Capabilities>>),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Error {
    InvalidRequest,
    ObservationMismatch,
    Need(Observation),
}

struct Cursor<'a> {
    values: &'a [Observed],
    index: usize,
}

impl Cursor<'_> {
    fn read(&mut self, request: Observation) -> Result<ObservationValue, Error> {
        let Some(observed) = self.values.get(self.index) else {
            return Err(Error::Need(request));
        };
        if observed.request != request {
            return Err(Error::ObservationMismatch);
        }
        self.index += 1;
        Ok(observed.result.clone())
    }

    fn text(&mut self, request: Observation) -> Result<Option<String>, Error> {
        match self.read(request)? {
            ObservationValue::Text(value) => Ok(value),
            _ => Err(Error::ObservationMismatch),
        }
    }

    fn boolean(&mut self, request: Observation) -> Result<bool, Error> {
        match self.read(request)? {
            ObservationValue::Boolean(value) => Ok(value),
            _ => Err(Error::ObservationMismatch),
        }
    }
}

pub fn normalize_path(path: &str) -> String {
    path.replace('\\', "/").to_lowercase()
}

pub fn js_whitespace(character: char) -> bool {
    matches!(
        character,
        '\u{0009}'..='\u{000d}'
            | '\u{0020}'
            | '\u{00a0}'
            | '\u{1680}'
            | '\u{2000}'..='\u{200a}'
            | '\u{2028}'
            | '\u{2029}'
            | '\u{202f}'
            | '\u{205f}'
            | '\u{3000}'
            | '\u{feff}'
    )
}

pub fn nonempty(value: &str) -> Option<String> {
    let trimmed = value.trim_matches(js_whitespace);
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

pub fn quote_shell_word(word: &str, platform: &str) -> String {
    let safe = !word.is_empty()
        && word.chars().all(|character| {
            character.is_ascii_alphanumeric()
                || matches!(character, '_' | '.' | '/' | ':' | '@' | '=' | '-')
                || (platform == "win32" && character == '\\')
        });
    if safe {
        return word.to_string();
    }
    let mut quoted = String::from("'");
    for character in word.chars() {
        if platform == "win32" && matches!(character, '\'' | '\u{2018}' | '\u{2019}') {
            quoted.push(character);
            quoted.push(character);
        } else if platform != "win32" && character == '\'' {
            quoted.push_str("'\\''");
        } else {
            quoted.push(character);
        }
    }
    quoted.push('\'');
    quoted
}

fn update(
    owner: &Owner,
    executable: &str,
    args: Vec<String>,
    lock_key: &str,
    platform: &str,
    env: &str,
    command: Option<String>,
) -> Capabilities {
    let command = command.unwrap_or_else(|| {
        let quoted = quote_shell_word(executable, platform);
        let mut words = vec![if platform == "win32" && quoted != executable {
            format!("& {quoted}")
        } else {
            quoted
        }];
        for arg in &args {
            words.push(quote_shell_word(arg, platform));
        }
        words.join(" ")
    });
    Capabilities {
        owner: owner.clone(),
        update: Some(Update {
            command,
            executable: executable.to_string(),
            args,
            lock_key: lock_key.to_string(),
            env: env.to_string(),
        }),
        install_path: None,
        manual_command: None,
        latest_unknown: false,
        latest_lookup: None,
    }
}

fn manual(owner: &Owner, install_path: Option<&str>) -> Capabilities {
    Capabilities {
        owner: owner.clone(),
        update: None,
        install_path: install_path
            .filter(|path| !path.is_empty())
            .map(str::to_string),
        manual_command: None,
        latest_unknown: false,
        latest_lookup: None,
    }
}

fn is_mise_path(path: &str) -> bool {
    let path = normalize_path(path);
    (path.contains("/mise/installs/") && !path.contains("/mise/installs/node/"))
        || path.contains("/mise/shims/")
        || path.contains("/mise/command-wrappers/")
}

fn is_mise_executable(path: &str) -> bool {
    let path = normalize_path(path);
    matches!(path.rsplit('/').next(), Some("mise" | "mise.exe"))
}

fn ascii_word(character: char) -> bool {
    character.is_ascii_alphanumeric() || character == '_'
}

pub fn is_mise_wrapper(script: &str) -> bool {
    if !script.starts_with("#!") {
        return false;
    }
    for (index, _) in script.match_indices("mise") {
        if script[..index].chars().next_back().is_some_and(ascii_word) {
            continue;
        }
        let tail = &script[index + 4..];
        let spaced = tail.trim_start_matches(js_whitespace);
        if spaced.len() == tail.len() {
            continue;
        }
        for command in ["x", "exec"] {
            if spaced
                .strip_prefix(command)
                .is_some_and(|rest| !rest.chars().next().is_some_and(ascii_word))
            {
                return true;
            }
        }
    }
    false
}

fn under(path: &str, directory: &str) -> bool {
    path.starts_with(&format!(
        "{}/",
        normalize_path(directory).trim_end_matches('/')
    ))
}

fn tool_directory(install_path: &str) -> String {
    let slash = install_path.replace('\\', "/");
    let trimmed = slash.trim_end_matches('/');
    trimmed
        .rsplit_once('/')
        .map_or(trimmed, |(parent, _)| parent)
        .to_string()
}

fn mise(
    owner: &Owner,
    tool: Option<&MiseTool>,
    context: &Context,
    cursor: &mut Cursor<'_>,
) -> Result<Option<Capabilities>, Error> {
    let is_shim = is_mise_executable(&context.real_command_path);
    let looks_mise = if is_shim
        || is_mise_path(&context.resolved_command_path)
        || is_mise_path(&context.real_command_path)
    {
        true
    } else {
        cursor
            .text(Observation::Wrapper)?
            .is_some_and(|script| is_mise_wrapper(&script))
    };
    let not_owned = |path: &str| looks_mise.then(|| manual(owner, Some(path)));
    let Some(tool) = tool else {
        return Ok(not_owned(&context.real_command_path));
    };
    let normalized = normalize_path(&context.resolved_command_path);
    let mut command_name = normalized.rsplit('/').next().unwrap_or("");
    for suffix in [".exe", ".cmd", ".bat", ".ps1"] {
        if let Some(name) = command_name.strip_suffix(suffix) {
            command_name = name;
            break;
        }
    }
    if is_shim && command_name != tool.tool {
        return Ok(Some(manual(owner, Some(&context.real_command_path))));
    }
    let executable = cursor
        .text(Observation::ResolveMise)?
        .or_else(|| is_shim.then(|| context.real_command_path.clone()));
    let Some(executable) = executable.filter(|path| !path.is_empty()) else {
        return Ok(not_owned(&context.real_command_path));
    };
    let resolved = cursor.text(Observation::MiseWhich)?;
    let binary_path = if is_shim {
        resolved.as_deref()
    } else {
        Some(context.real_command_path.as_str())
    };
    let Some(binary_path) = binary_path.filter(|value| !value.is_empty()) else {
        return Ok(Some(manual(owner, Some(&context.real_command_path))));
    };
    let binary = normalize_path(binary_path);
    let runs_binary = resolved
        .as_ref()
        .is_some_and(|path| normalize_path(path) == binary);
    let ObservationValue::Listing { present, installs } = cursor.read(Observation::MiseListing)?
    else {
        return Err(Error::ObservationMismatch);
    };
    if !present {
        return Ok(if runs_binary {
            Some(manual(owner, Some(binary_path)))
        } else {
            not_owned(binary_path)
        });
    }
    if installs.is_empty() {
        return Ok(not_owned(binary_path));
    }
    let paths = installs
        .iter()
        .map(|install| install.path.clone())
        .collect::<Vec<_>>();
    let ObservationValue::Paths(real_paths) = cursor.read(Observation::MiseInstallPaths(paths))?
    else {
        return Err(Error::ObservationMismatch);
    };
    if real_paths.len() != installs.len() {
        return Err(Error::ObservationMismatch);
    }
    let owning = installs
        .iter()
        .zip(&real_paths)
        .filter(|(_, path)| under(&binary, path));
    let mut any_owned = false;
    let mut active = None;
    for (install, _) in owning {
        any_owned = true;
        if active.is_none()
            && install.active
            && install.requested_version.as_deref() == Some("latest")
        {
            active = Some(install);
        }
    }
    if !any_owned {
        return Ok(not_owned(binary_path));
    }
    if !runs_binary || !tool.upgrade {
        return Ok(Some(manual(owner, Some(binary_path))));
    }
    let Some(active) = active else {
        return Ok(Some(manual(owner, Some(binary_path))));
    };
    if !is_shim {
        let directory = tool_directory(&active.path);
        let real_directory = cursor
            .text(Observation::MiseToolPath(directory.clone()))?
            .ok_or(Error::ObservationMismatch)?;
        let configured = normalize_path(&context.resolved_command_path);
        if !under(&configured, &format!("{directory}/latest"))
            && !under(&configured, &format!("{real_directory}/latest"))
        {
            return Ok(Some(manual(owner, Some(binary_path))));
        }
    }
    let args = vec!["upgrade".to_string(), tool.tool.clone()];
    let mut capabilities = update(
        owner,
        &executable,
        args,
        "mise",
        &context.platform,
        "context",
        Some(format!("mise upgrade {}", tool.tool)),
    );
    capabilities.latest_unknown = true;
    capabilities.latest_lookup = Some(Lookup::Mise {
        executable,
        tool: tool.tool.clone(),
    });
    Ok(Some(capabilities))
}

pub fn npm_global_prefix(real_path: &str, package: &str) -> Option<String> {
    let slash = real_path.replace('\\', "/");
    let normalized = slash.to_lowercase();
    let segment = format!("/lib/node_modules/{}/", package.to_lowercase());
    let index = normalized.rfind(&segment)?;
    let prefix = &normalized[..index];
    if prefix.contains("/node_modules/") {
        return None;
    }
    if prefix
        .rsplit_once("/mise/installs/")
        .and_then(|(_, tail)| tail.split_once('/'))
        .is_some_and(|(tool, version)| {
            !tool.is_empty() && !version.is_empty() && !version.contains('/') && tool != "node"
        })
    {
        return None;
    }
    if index == 0 {
        return Some("/".to_string());
    }
    let count = prefix.encode_utf16().count();
    let mut units = 0;
    let mut end = 0;
    for character in slash.chars() {
        if units == count {
            break;
        }
        units += character.len_utf16();
        end += character.len_utf8();
        if units > count {
            return None;
        }
    }
    Some(slash[..end].to_string())
}

pub fn homebrew_ownership(real_path: &str) -> Option<Homebrew> {
    let slash = real_path.replace('\\', "/");
    let lower = slash.to_ascii_lowercase();
    let mut answer = None;
    for (index, _) in lower.match_indices('/') {
        let tail = &lower[index..];
        let (kind, length) = if tail.starts_with("/cellar/") {
            ("formula", 8)
        } else if tail.starts_with("/caskroom/") {
            ("cask", 10)
        } else {
            continue;
        };
        if slash[..index]
            .chars()
            .any(|value| matches!(value, '\n' | '\r' | '\u{2028}' | '\u{2029}'))
        {
            continue;
        }
        let mut parts = slash[index + length..].split('/');
        let name = parts.next().unwrap_or("");
        let version = parts.next().unwrap_or("");
        if name.is_empty() || version.is_empty() || parts.next().is_none() {
            continue;
        }
        answer = Some(Homebrew {
            kind: kind.to_string(),
            name: name.to_string(),
            prefix: slash[..index].to_string(),
        });
    }
    answer
}

fn homebrew(
    owner: &Owner,
    ownership: &Homebrew,
    context: &Context,
    cursor: &mut Cursor<'_>,
) -> Result<Option<Capabilities>, Error> {
    let Some(executable) = cursor
        .text(Observation::HomebrewExecutable(ownership.prefix.clone()))?
        .filter(|path| !path.is_empty())
    else {
        return Ok(None);
    };
    let prefix = cursor.text(Observation::HomebrewPrefix)?;
    if !prefix.is_some_and(|prefix| {
        !prefix.is_empty() && normalize_path(&prefix) == normalize_path(&ownership.prefix)
    }) {
        return Ok(None);
    }
    let mut args = vec!["upgrade".to_string()];
    if ownership.kind == "cask" {
        args.push("--cask".to_string());
    }
    args.push(ownership.name.clone());
    let mut words = vec!["brew".to_string()];
    words.extend(args.clone());
    let mut capabilities = update(
        owner,
        &executable,
        args,
        "homebrew",
        &context.command_platform,
        "none",
        Some(words.join(" ")),
    );
    capabilities.latest_unknown = true;
    capabilities.latest_lookup = Some(Lookup::Homebrew {
        executable,
        ownership: ownership.clone(),
    });
    Ok(Some(capabilities))
}

fn winget_claude(path: &str) -> bool {
    let normalized = normalize_path(path);
    let bytes = normalized.as_bytes();
    if bytes.len() < 3 || !bytes[0].is_ascii_lowercase() || bytes[1] != b':' || bytes[2] != b'/' {
        return false;
    }
    let rest = &normalized[3..];
    let tail = if let Some(rest) = rest.strip_prefix("users/") {
        let Some((user, rest)) = rest.split_once('/') else {
            return false;
        };
        if user.is_empty() {
            return false;
        }
        rest.strip_prefix("appdata/local/microsoft/winget/")
    } else {
        rest.strip_prefix("program files/winget/")
            .or_else(|| rest.strip_prefix("program files (x86)/winget/"))
    };
    let Some(tail) = tail else {
        return false;
    };
    if tail == "links/claude.exe" {
        return true;
    }
    tail.strip_prefix("packages/anthropic.claudecode_")
        .and_then(|tail| tail.strip_suffix("/claude.exe"))
        .is_some_and(|package| !package.is_empty() && !package.contains('/'))
}

fn chocolatey_claude(path: &str, install: Option<&str>) -> bool {
    let normalized = normalize_path(path);
    if normalized.ends_with("/chocolatey/bin/claude.exe")
        || normalized.ends_with("/chocolatey/lib/claude-code/tools/claude.exe")
    {
        return true;
    }
    let Some(root) = install.and_then(nonempty) else {
        return false;
    };
    let prefix = normalize_path(root.trim_end_matches(['\\', '/']));
    normalized == format!("{prefix}/bin/claude.exe")
        || normalized == format!("{prefix}/lib/claude-code/tools/claude.exe")
}

fn manager_path(path: &str, manager: &str) -> bool {
    let normalized = normalize_path(path);
    match manager {
        "vp" => [
            "/.vite-plus/bin/",
            "/.local/share/vite-plus/bin/",
            "/appdata/local/vite-plus/bin/",
        ]
        .iter()
        .any(|part| normalized.contains(part)),
        "bun" => normalized.contains("/.bun/bin/"),
        "pnpm" => [
            "/.local/share/pnpm/",
            "/library/pnpm/",
            "/local/share/pnpm/",
            "/appdata/local/pnpm/",
            "/pnpm/global/",
        ]
        .iter()
        .any(|part| normalized.contains(part)),
        "yarn" => {
            normalized.contains("/yarn/global/node_modules/")
                || normalized.contains("/yarn/data/global/node_modules/")
        }
        _ => false,
    }
}

fn package(request: &Request, cursor: &mut Cursor<'_>) -> Result<Option<Capabilities>, Error> {
    let owner = &request.owner;
    let package_name = owner.package_name.as_deref().ok_or(Error::InvalidRequest)?;
    let Some(context) = &request.context else {
        return Ok(Some(manual(owner, None)));
    };
    let manual = manual(owner, Some(&context.real_command_path));
    let paths = [&context.resolved_command_path, &context.real_command_path];
    if owner.provider == "claudeAgent" && context.platform == "win32" {
        if paths.iter().any(|path| winget_claude(path)) {
            return Ok(Some(manual));
        }
        if paths
            .iter()
            .any(|path| chocolatey_claude(path, context.chocolatey_install.as_deref()))
        {
            let mut capabilities = manual;
            capabilities.manual_command = Some("choco upgrade claude-code".to_string());
            capabilities.latest_unknown = true;
            capabilities.latest_lookup = Some(Lookup::Chocolatey);
            return Ok(Some(capabilities));
        }
    }
    if let Some(capabilities) = mise(owner, request.definition.mise.as_ref(), context, cursor)? {
        return Ok(Some(capabilities));
    }
    let native = request.definition.native.as_ref().map_or_else(
        || manual.clone(),
        |native| {
            update(
                owner,
                &context.resolved_command_path,
                native.args.clone(),
                &format!("{}-native", owner.provider),
                &context.platform,
                if native.has_env { "native" } else { "none" },
                None,
            )
        },
    );
    let npm_prefix = npm_global_prefix(&context.real_command_path, package_name);
    if request
        .definition
        .native
        .as_ref()
        .is_some_and(|native| native.matches_command_path)
        && npm_prefix.is_none()
    {
        return Ok(Some(native));
    }
    let fallback = if request
        .definition
        .native
        .as_ref()
        .is_some_and(|native| native.requires_command_path)
        || paths
            .iter()
            .any(|path| normalize_path(path).contains("/node_modules/"))
    {
        manual
    } else {
        native
    };
    for manager in ["vp", "bun", "pnpm", "yarn"] {
        if !paths.iter().any(|path| manager_path(path, manager)) {
            continue;
        }
        let mut args = match manager {
            "vp" | "bun" => vec!["i".to_string(), "-g".to_string()],
            "pnpm" => vec!["add".to_string(), "-g".to_string()],
            _ => vec!["global".to_string(), "add".to_string()],
        };
        if manager == "pnpm" && cursor.boolean(Observation::PnpmVersion)? {
            args.push(format!("--allow-build={package_name}"));
        }
        args.push(if manager == "vp" {
            package_name.to_string()
        } else {
            format!("{package_name}@latest")
        });
        let lock = if manager == "vp" {
            "vite-plus-global".to_string()
        } else {
            format!("{manager}-global")
        };
        let mut capabilities = update(
            owner,
            manager,
            args,
            &lock,
            &context.command_platform,
            "context",
            None,
        );
        if manager == "pnpm" {
            capabilities.latest_lookup = Some(Lookup::Pnpm {
                package_name: package_name.to_string(),
            });
        }
        return Ok(Some(capabilities));
    }
    let normalized = normalize_path(&context.real_command_path);
    let basename = normalized
        .trim_end_matches('/')
        .rsplit('/')
        .next()
        .unwrap_or("");
    let basename = basename.strip_suffix(".exe").unwrap_or(basename);
    if basename == "volta-shim" && cursor.boolean(Observation::VoltaPackage)? {
        return Ok(Some(update(
            owner,
            "volta",
            vec!["install".to_string(), format!("{package_name}@latest")],
            "volta",
            &context.command_platform,
            "context",
            None,
        )));
    }
    let npm_prefix = if npm_prefix.is_some() || context.host_platform != "win32" {
        npm_prefix
    } else {
        match cursor.read(Observation::NpmShim)? {
            ObservationValue::NpmShim { directory, exists } => exists.then_some(directory),
            _ => return Err(Error::ObservationMismatch),
        }
    };
    if let Some(prefix) = npm_prefix.filter(|prefix| !prefix.is_empty()) {
        return Ok(Some(update(
            owner,
            "npm",
            vec![
                "install".to_string(),
                "-g".to_string(),
                "--prefix".to_string(),
                prefix.clone(),
                format!("--allow-scripts={package_name}"),
                format!("{package_name}@latest"),
            ],
            &format!("npm-global:{}", normalize_path(&prefix)),
            &context.command_platform,
            "context",
            None,
        )));
    }
    if let Some(ownership) = homebrew_ownership(&context.real_command_path) {
        return Ok(Some(
            homebrew(owner, &ownership, context, cursor)?.unwrap_or(fallback),
        ));
    }
    Ok(Some(fallback))
}

pub fn resolve(request: &Request) -> Result<Decision, Error> {
    let mut cursor = Cursor {
        values: &request.observations,
        index: 0,
    };
    let answer = match request.mode.as_str() {
        "package" => package(request, &mut cursor),
        "mise" => mise(
            &request.owner,
            request.definition.mise.as_ref(),
            request.context.as_ref().ok_or(Error::InvalidRequest)?,
            &mut cursor,
        ),
        "homebrew" => homebrew(
            &request.owner,
            request.homebrew.as_ref().ok_or(Error::InvalidRequest)?,
            request.context.as_ref().ok_or(Error::InvalidRequest)?,
            &mut cursor,
        ),
        _ => return Err(Error::InvalidRequest),
    };
    match answer {
        Err(Error::Need(observation)) => Ok(Decision::Observe(observation)),
        Err(error) => Err(error),
        Ok(capabilities) => {
            if cursor.index != request.observations.len() {
                return Err(Error::ObservationMismatch);
            }
            Ok(Decision::Resolved(capabilities.map(Box::new)))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn update_action(lock_key: &str) -> UpdateActionInput {
        UpdateActionInput {
            command: "pnpm add --global @openai/codex@latest".to_string(),
            args: vec![
                "add".to_string(),
                "--global".to_string(),
                "@openai/codex@latest".to_string(),
            ],
            lock_key: lock_key.to_string(),
        }
    }

    #[test]
    fn targeted_update_changes_first_argument_and_last_display_occurrence() {
        let input = UpdateActionInput {
            command: "echo 雪/pkg@latest; pnpm add 雪/pkg@latest".to_string(),
            args: vec![
                "add".to_string(),
                "雪/pkg@latest".to_string(),
                "雪/pkg".to_string(),
                "雪/pkg@latest".to_string(),
            ],
            lock_key: "pnpm-global".to_string(),
        };
        let selected = make_targeted_update_action(Some("雪/pkg"), Some(&input), "01.2.003")
            .expect("targeted update");
        assert_eq!(
            selected.command,
            "echo 雪/pkg@latest; pnpm add 雪/pkg@01.2.003"
        );
        assert_eq!(
            selected.args,
            ["add", "雪/pkg@01.2.003", "雪/pkg", "雪/pkg@latest"]
        );
        assert_eq!(input.args[1], "雪/pkg@latest");

        let mut bare = update_action("volta");
        bare.args[2] = "@openai/codex".to_string();
        bare.command = "display command without the package name".to_string();
        let selected = make_targeted_update_action(Some("@openai/codex"), Some(&bare), "1.2.3")
            .expect("bare package argument");
        assert_eq!(selected.command, bare.command);
        assert_eq!(selected.args[2], "@openai/codex@1.2.3");
    }

    #[test]
    fn targeted_versions_match_ascii_javascript_pattern() {
        let input = update_action("pnpm-global");
        for version in ["0.0.0", "01.002.0003", "999999999999999999999999.2.3"] {
            assert!(
                make_targeted_update_action(Some("@openai/codex"), Some(&input), version).is_some(),
                "valid version {version:?}"
            );
        }
        for version in [
            "",
            "1.2",
            "1.2.3.4",
            ".2.3",
            "1..3",
            "1.2.",
            "v1.2.3",
            "1.2.3-beta",
            "1.2.3+build",
            " 1.2.3",
            "1.2.3 ",
            "1.2.3\n",
            "1.2.3\r",
            "1.2.3\r\n",
            "1.2.3\u{2028}",
            "1.2.3\u{2029}",
            "١.٢.٣",
            "１.２.３",
        ] {
            assert_eq!(
                make_targeted_update_action(Some("@openai/codex"), Some(&input), version),
                None,
                "invalid version {version:?}"
            );
        }
    }

    #[test]
    fn targeted_update_requires_supported_lock_and_matching_package() {
        for lock_key in [
            "npm-global:",
            "npm-global:/usr",
            "npm-global:\n",
            "bun-global",
            "pnpm-global",
            "vite-plus-global",
            "yarn-global",
            "volta",
        ] {
            assert!(
                make_targeted_update_action(
                    Some("@openai/codex"),
                    Some(&update_action(lock_key)),
                    "1.2.3"
                )
                .is_some(),
                "supported lock {lock_key:?}"
            );
        }
        for lock_key in [
            "npm-global",
            "npm-globalx:",
            "xpnpm-global",
            "pnpm-global:other",
            "bun-global\n",
            "pnpm-global\r",
            "yarn-global\u{2028}",
            "volta\u{2029}",
            "homebrew",
            "mise",
            "",
        ] {
            assert_eq!(
                make_targeted_update_action(
                    Some("@openai/codex"),
                    Some(&update_action(lock_key)),
                    "1.2.3"
                ),
                None,
                "unsupported lock {lock_key:?}"
            );
        }
        let input = update_action("pnpm-global");
        for package_name in [None, Some(""), Some("codex"), Some("@other/codex")] {
            assert_eq!(
                make_targeted_update_action(package_name, Some(&input), "1.2.3"),
                None
            );
        }
        assert_eq!(
            make_targeted_update_action(Some("@openai/codex"), None, "1.2.3"),
            None
        );
        let mut pinned = input;
        pinned.args[2] = "@openai/codex@2.0.0".to_string();
        assert_eq!(
            make_targeted_update_action(Some("@openai/codex"), Some(&pinned), "1.2.3"),
            None
        );
    }

    #[test]
    fn latest_update_pins_only_pnpm_and_keeps_original_when_target_unavailable() {
        let input = update_action("pnpm-global");
        assert_eq!(
            resolve_latest_update_action(Some("@openai/codex"), Some(&input), Some("1.2.3"))
                .unwrap()
                .args[2],
            "@openai/codex@1.2.3"
        );
        for version in [None, Some(""), Some("1.2.3-beta"), Some("1.2.3\n")] {
            assert_eq!(
                resolve_latest_update_action(Some("@openai/codex"), Some(&input), version),
                Some(original_update_action(&input))
            );
        }
        for package_name in [None, Some(""), Some("@other/package")] {
            assert_eq!(
                resolve_latest_update_action(package_name, Some(&input), Some("1.2.3")),
                Some(original_update_action(&input))
            );
        }
        for lock_key in ["npm-global:/usr", "bun-global", "pnpm-global\n", "native"] {
            let input = update_action(lock_key);
            assert_eq!(
                resolve_latest_update_action(Some("@openai/codex"), Some(&input), Some("1.2.3")),
                Some(original_update_action(&input))
            );
        }
        assert_eq!(
            resolve_latest_update_action(Some("@openai/codex"), None, Some("1.2.3")),
            None
        );
    }

    fn request(path: &str) -> Request {
        Request {
            mode: "package".to_string(),
            owner: Owner {
                provider: "codex".to_string(),
                package_name: Some("@openai/codex".to_string()),
            },
            context: Some(Context {
                resolved_command_path: path.to_string(),
                real_command_path: path.to_string(),
                platform: "linux".to_string(),
                host_platform: "linux".to_string(),
                command_platform: "linux".to_string(),
                chocolatey_install: None,
            }),
            definition: Definition {
                mise: None,
                native: None,
            },
            homebrew: None,
            observations: vec![Observed {
                request: Observation::Wrapper,
                result: ObservationValue::Text(None),
            }],
        }
    }

    fn capabilities(request: &Request) -> Capabilities {
        let Decision::Resolved(Some(value)) = resolve(request).unwrap() else {
            panic!("expected resolved capabilities");
        };
        *value
    }

    #[test]
    fn installer_precedence_keeps_npm_inside_homebrew_and_mise_node() {
        for (path, prefix) in [
            (
                "/opt/homebrew/Cellar/node/22/lib/node_modules/@openai/codex/bin/codex",
                "/opt/homebrew/Cellar/node/22",
            ),
            (
                "/home/user/mise/installs/node/22/lib/node_modules/@openai/codex/bin/codex",
                "/home/user/mise/installs/node/22",
            ),
        ] {
            let value = capabilities(&request(path));
            let update = value.update.unwrap();
            assert_eq!(update.executable, "npm");
            assert_eq!(update.args[3], prefix);
        }
        assert_eq!(
            npm_global_prefix(
                "/project/node_modules/other/lib/node_modules/@openai/codex/bin/codex",
                "@openai/codex"
            ),
            None
        );
        assert_eq!(
            npm_global_prefix(
                "/x/mise/installs/codex/1/lib/node_modules/@openai/codex/bin/codex",
                "@openai/codex"
            ),
            None
        );
    }

    #[test]
    fn command_quoting_preserves_posix_and_powershell_words() {
        assert_eq!(quote_shell_word("a'b", "linux"), "'a'\\''b'");
        assert_eq!(
            quote_shell_word("C:\\Program Files\\provider.exe", "win32"),
            "'C:\\Program Files\\provider.exe'"
        );
        assert_eq!(
            quote_shell_word("a\u{2018}b\u{2019}'", "win32"),
            "'a\u{2018}\u{2018}b\u{2019}\u{2019}'''"
        );
        assert_eq!(quote_shell_word("", "linux"), "''");
        let mut input = request("C:\\Program Files\\codex.exe");
        input.context.as_mut().unwrap().platform = "win32".to_string();
        input.definition.native = Some(NativeUpdate {
            args: vec!["update".to_string()],
            matches_command_path: true,
            requires_command_path: true,
            has_env: true,
        });
        let update = capabilities(&input).update.unwrap();
        assert_eq!(update.command, "& 'C:\\Program Files\\codex.exe' update");
        assert_eq!(update.env, "native");
    }

    #[test]
    fn mise_wrapper_checks_use_javascript_whitespace_and_ascii_boundaries() {
        assert!(is_mise_wrapper("#!/bin/sh\nexec mise\u{feff}exec codex"));
        assert!(is_mise_wrapper("#!/bin/sh\nmise x -- codex"));
        assert!(!is_mise_wrapper("exec mise exec codex"));
        assert!(!is_mise_wrapper("#!/bin/sh\npromise exec codex"));
        assert!(!is_mise_wrapper("#!/bin/sh\nmise executor codex"));
        assert!(!is_mise_wrapper("#!/bin/sh\nmise\u{85}exec codex"));
    }

    #[test]
    fn mise_latest_link_can_upgrade_but_fixed_path_stays_manual() {
        let mut input = request("/data/mise/installs/codex/latest/bin/codex");
        input.context.as_mut().unwrap().real_command_path =
            "/data/mise/installs/codex/1/bin/codex".to_string();
        input.definition.mise = Some(MiseTool {
            tool: "codex".to_string(),
            upgrade: true,
        });
        input.observations = vec![
            Observed {
                request: Observation::ResolveMise,
                result: ObservationValue::Text(Some("/bin/mise".to_string())),
            },
            Observed {
                request: Observation::MiseWhich,
                result: ObservationValue::Text(Some(
                    "/data/mise/installs/codex/1/bin/codex".to_string(),
                )),
            },
            Observed {
                request: Observation::MiseListing,
                result: ObservationValue::Listing {
                    present: true,
                    installs: vec![MiseInstall {
                        path: "/data/mise/installs/codex/1".to_string(),
                        requested_version: Some("latest".to_string()),
                        active: true,
                    }],
                },
            },
            Observed {
                request: Observation::MiseInstallPaths(vec![
                    "/data/mise/installs/codex/1".to_string(),
                ]),
                result: ObservationValue::Paths(vec!["/data/mise/installs/codex/1".to_string()]),
            },
            Observed {
                request: Observation::MiseToolPath("/data/mise/installs/codex".to_string()),
                result: ObservationValue::Text(Some("/data/mise/installs/codex".to_string())),
            },
        ];
        assert_eq!(
            capabilities(&input).update.unwrap().command,
            "mise upgrade codex"
        );
        input.context.as_mut().unwrap().resolved_command_path =
            "/data/mise/installs/codex/1/bin/codex".to_string();
        assert!(capabilities(&input).update.is_none());
        input.definition.mise.as_mut().unwrap().upgrade = false;
        input.observations.pop();
        assert!(capabilities(&input).update.is_none());
    }

    #[test]
    fn observation_order_and_completion_are_bound_to_the_request() {
        let mut input = request("/home/user/.local/share/pnpm/codex");
        assert_eq!(
            resolve(&input),
            Ok(Decision::Observe(Observation::PnpmVersion))
        );
        input.observations.push(Observed {
            request: Observation::PnpmVersion,
            result: ObservationValue::Boolean(true),
        });
        assert!(
            capabilities(&input)
                .update
                .unwrap()
                .args
                .contains(&"--allow-build=@openai/codex".to_string()),
            "pnpm approval must be preserved"
        );
        input.observations[1].request = Observation::VoltaPackage;
        assert_eq!(resolve(&input), Err(Error::ObservationMismatch));
        input.observations.truncate(1);
        input.context = None;
        assert_eq!(resolve(&input), Err(Error::ObservationMismatch));
        input.observations.clear();
        assert!(capabilities(&input).update.is_none());
    }

    #[test]
    fn platform_managed_claude_stays_manual_before_other_probes() {
        let mut input = request("C:\\Program Files\\WinGet\\Links\\claude.exe");
        input.owner.provider = "claudeAgent".to_string();
        input.context.as_mut().unwrap().platform = "win32".to_string();
        input.observations.clear();
        assert!(capabilities(&input).update.is_none());
        input.context.as_mut().unwrap().resolved_command_path =
            "D:\\Chocolatey\\bin\\claude.exe".to_string();
        input.context.as_mut().unwrap().real_command_path =
            "D:\\Chocolatey\\bin\\claude.exe".to_string();
        let value = capabilities(&input);
        assert_eq!(
            value.manual_command.as_deref(),
            Some("choco upgrade claude-code")
        );
        assert_eq!(value.latest_lookup, Some(Lookup::Chocolatey));
        assert!(value.latest_unknown);
    }

    #[test]
    fn homebrew_uses_owning_prefix_and_last_valid_keg() {
        let ownership = homebrew_ownership("/opt/Cellar/outer/1/Cellar/Inner/2/bin/tool").unwrap();
        assert_eq!(ownership.prefix, "/opt/Cellar/outer/1");
        assert_eq!(ownership.name, "Inner");
        assert_eq!(ownership.kind, "formula");
        assert!(homebrew_ownership("/opt/Cellar/tool/1").is_none());
        assert!(homebrew_ownership("/opt\n/Cellar/tool/1/bin/tool").is_none());
        let mut input = request("/opt/Caskroom/codex/1/bin/codex");
        input.observations.extend([
            Observed {
                request: Observation::HomebrewExecutable("/opt".to_string()),
                result: ObservationValue::Text(Some("/opt/bin/brew".to_string())),
            },
            Observed {
                request: Observation::HomebrewPrefix,
                result: ObservationValue::Text(Some("/other".to_string())),
            },
        ]);
        assert!(capabilities(&input).update.is_none());
        input.observations.last_mut().unwrap().result =
            ObservationValue::Text(Some("/opt".to_string()));
        assert_eq!(
            capabilities(&input).update.unwrap().command,
            "brew upgrade --cask codex"
        );
    }
}
