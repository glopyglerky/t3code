//! Bounded JSONL for provider ownership and update-action decisions.

use crate::json::{self, Value};
use crate::maintenance::{
    self, Capabilities, Context, Decision, Definition, Homebrew, Lookup, MiseInstall, MiseTool,
    NativeUpdate, Observation, ObservationValue, Observed, Owner, Request, SelectedUpdateAction,
    UpdateActionInput,
};
use crate::provider_update_verification::{self, ProviderSnapshot};
use std::collections::HashSet;

pub const MAX_FRAME_BYTES: usize = 1024 * 1024;
pub const MAX_INPUT_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_OBSERVATIONS: usize = 4096;
const MAX_TEXT_BYTES: usize = 65_536;
const MAX_PROBE_BYTES: usize = 256 * 1024;
const MAX_WRAPPER_BYTES: usize = 16 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    InvalidRequest,
    DuplicateKey,
    InputLimit,
    OutputLimit,
    ObservationMismatch,
    Io,
}

impl Error {
    pub fn message(self) -> &'static str {
        match self {
            Self::InvalidRequest => "invalid request",
            Self::DuplicateKey => "duplicate object key",
            Self::InputLimit => "input limit exceeded",
            Self::OutputLimit => "output limit exceeded",
            Self::ObservationMismatch => "observation does not match request",
            Self::Io => "input or output failed",
        }
    }
}

type Result<T> = std::result::Result<T, Error>;

fn object(fields: Vec<(&str, Value)>) -> Value {
    Value::Object(
        fields
            .into_iter()
            .map(|(key, value)| (key.to_string(), value))
            .collect(),
    )
}

fn field<'a>(value: &'a Value, key: &str) -> Result<&'a Value> {
    value.get(key).ok_or(Error::InvalidRequest)
}

fn keys(value: &Value, allowed: &[&str]) -> Result<()> {
    let Value::Object(entries) = value else {
        return Err(Error::InvalidRequest);
    };
    let mut seen = HashSet::new();
    for (key, _) in entries {
        if !seen.insert(key.as_str()) {
            return Err(Error::DuplicateKey);
        }
        if !allowed.contains(&key.as_str()) {
            return Err(Error::InvalidRequest);
        }
    }
    if entries.len() != allowed.len() {
        return Err(Error::InvalidRequest);
    }
    Ok(())
}

fn text_limit(value: &Value, limit: usize) -> Result<String> {
    let text = value.as_str().ok_or(Error::InvalidRequest)?;
    if text.len() > limit {
        return Err(Error::InputLimit);
    }
    Ok(text.to_string())
}

fn text(value: &Value) -> Result<String> {
    text_limit(value, MAX_TEXT_BYTES)
}

fn identifier(value: &Value) -> Result<String> {
    let text = text(value)?;
    if text.is_empty() || text.contains('\0') {
        return Err(Error::InvalidRequest);
    }
    Ok(text)
}

fn optional_text(value: &Value, limit: usize) -> Result<Option<String>> {
    if value == &Value::Null {
        Ok(None)
    } else {
        text_limit(value, limit).map(Some)
    }
}

fn boolean(value: &Value) -> Result<bool> {
    value.as_bool().ok_or(Error::InvalidRequest)
}

fn text_array(value: &Value) -> Result<Vec<String>> {
    let values = value.as_array().ok_or(Error::InvalidRequest)?;
    values.iter().map(text).collect()
}

fn owner(value: &Value) -> Result<Owner> {
    keys(value, &["provider", "packageName"])?;
    Ok(Owner {
        provider: identifier(field(value, "provider")?)?,
        package_name: optional_text(field(value, "packageName")?, MAX_TEXT_BYTES)?,
    })
}

fn context(value: &Value) -> Result<Option<Context>> {
    if value == &Value::Null {
        return Ok(None);
    }
    keys(
        value,
        &[
            "resolvedCommandPath",
            "realCommandPath",
            "platform",
            "hostPlatform",
            "commandPlatform",
            "chocolateyInstall",
        ],
    )?;
    let resolved_command_path = text(field(value, "resolvedCommandPath")?)?;
    let real_command_path = text(field(value, "realCommandPath")?)?;
    if resolved_command_path.contains('\0') || real_command_path.contains('\0') {
        return Err(Error::InvalidRequest);
    }
    Ok(Some(Context {
        resolved_command_path,
        real_command_path,
        platform: identifier(field(value, "platform")?)?,
        host_platform: identifier(field(value, "hostPlatform")?)?,
        command_platform: identifier(field(value, "commandPlatform")?)?,
        chocolatey_install: optional_text(field(value, "chocolateyInstall")?, MAX_TEXT_BYTES)?,
    }))
}

fn definition(value: &Value) -> Result<Definition> {
    keys(value, &["mise", "native"])?;
    let mise_value = field(value, "mise")?;
    let mise = if mise_value == &Value::Null {
        None
    } else {
        keys(mise_value, &["tool", "upgrade"])?;
        Some(MiseTool {
            tool: identifier(field(mise_value, "tool")?)?,
            upgrade: boolean(field(mise_value, "upgrade")?)?,
        })
    };
    let native_value = field(value, "native")?;
    let native = if native_value == &Value::Null {
        None
    } else {
        keys(
            native_value,
            &[
                "args",
                "matchesCommandPath",
                "requiresCommandPath",
                "hasEnv",
            ],
        )?;
        Some(NativeUpdate {
            args: text_array(field(native_value, "args")?)?,
            matches_command_path: boolean(field(native_value, "matchesCommandPath")?)?,
            requires_command_path: boolean(field(native_value, "requiresCommandPath")?)?,
            has_env: boolean(field(native_value, "hasEnv")?)?,
        })
    };
    Ok(Definition { mise, native })
}

fn homebrew(value: &Value) -> Result<Option<Homebrew>> {
    if value == &Value::Null {
        return Ok(None);
    }
    keys(value, &["kind", "name", "prefix"])?;
    let kind = identifier(field(value, "kind")?)?;
    if kind != "formula" && kind != "cask" {
        return Err(Error::InvalidRequest);
    }
    Ok(Some(Homebrew {
        kind,
        name: text(field(value, "name")?)?,
        prefix: text(field(value, "prefix")?)?,
    }))
}

fn decode_observation(value: &Value) -> Result<Observation> {
    let kind = field(value, "kind")?
        .as_str()
        .ok_or(Error::InvalidRequest)?;
    let allowed = match kind {
        "miseInstallPaths" => vec!["kind", "paths"],
        "miseToolPath" => vec!["kind", "path"],
        "homebrewExecutable" => vec!["kind", "prefix"],
        _ => vec!["kind"],
    };
    keys(value, &allowed)?;
    match kind {
        "wrapper" => Ok(Observation::Wrapper),
        "resolveMise" => Ok(Observation::ResolveMise),
        "miseWhich" => Ok(Observation::MiseWhich),
        "miseListing" => Ok(Observation::MiseListing),
        "miseAllListing" => Ok(Observation::MiseAllListing),
        "miseInstallPaths" => Ok(Observation::MiseInstallPaths(text_array(field(
            value, "paths",
        )?)?)),
        "miseToolPath" => Ok(Observation::MiseToolPath(text(field(value, "path")?)?)),
        "pnpmVersion" => Ok(Observation::PnpmVersion),
        "voltaPackage" => Ok(Observation::VoltaPackage),
        "npmShim" => Ok(Observation::NpmShim),
        "macPortsOwner" => Ok(Observation::MacPortsOwner),
        "homebrewExecutable" => Ok(Observation::HomebrewExecutable(text(field(
            value, "prefix",
        )?)?)),
        "homebrewPrefix" => Ok(Observation::HomebrewPrefix),
        _ => Err(Error::InvalidRequest),
    }
}

pub fn encode_observation(value: &Observation) -> Value {
    match value {
        Observation::MiseInstallPaths(paths) => object(vec![
            ("kind", Value::String("miseInstallPaths".to_string())),
            (
                "paths",
                Value::Array(paths.iter().cloned().map(Value::String).collect()),
            ),
        ]),
        Observation::MiseToolPath(path) => object(vec![
            ("kind", Value::String("miseToolPath".to_string())),
            ("path", Value::String(path.clone())),
        ]),
        Observation::HomebrewExecutable(prefix) => object(vec![
            ("kind", Value::String("homebrewExecutable".to_string())),
            ("prefix", Value::String(prefix.clone())),
        ]),
        _ => object(vec![(
            "kind",
            Value::String(
                match value {
                    Observation::Wrapper => "wrapper",
                    Observation::ResolveMise => "resolveMise",
                    Observation::MiseWhich => "miseWhich",
                    Observation::MiseListing => "miseListing",
                    Observation::MiseAllListing => "miseAllListing",
                    Observation::PnpmVersion => "pnpmVersion",
                    Observation::VoltaPackage => "voltaPackage",
                    Observation::NpmShim => "npmShim",
                    Observation::MacPortsOwner => "macPortsOwner",
                    Observation::HomebrewPrefix => "homebrewPrefix",
                    _ => unreachable!(),
                }
                .to_string(),
            ),
        )]),
    }
}

fn decode_installs(value: &Value) -> Option<Vec<MiseInstall>> {
    let values = value.as_array()?;
    let mut installs = Vec::new();
    for value in values {
        let path = value.get("install_path")?.as_str()?.to_string();
        let requested_version = match value.get("requested_version") {
            None | Some(Value::Null) => None,
            Some(Value::String(value)) => Some(value.clone()),
            _ => return None,
        };
        let active = match value.get("active") {
            None => false,
            Some(Value::Bool(value)) => *value,
            _ => return None,
        };
        installs.push(MiseInstall {
            path,
            requested_version,
            active,
        });
    }
    Some(installs)
}

fn listing(value: &Value, tool: Option<&MiseTool>) -> Result<ObservationValue> {
    let raw = optional_text(value, MAX_PROBE_BYTES)?;
    let present = raw.as_ref().is_some_and(|raw| !raw.is_empty());
    let parsed = if let Some(raw) = raw.as_deref() {
        match json::parse_with_limits(
            raw,
            json::Limits {
                max_bytes: MAX_PROBE_BYTES,
                max_depth: 128,
                max_values: 131_072,
            },
        ) {
            Ok(value) => Some(value),
            Err(error) if error.message.contains("exceeds") => return Err(Error::InputLimit),
            Err(error) if error.message == "unpaired Unicode surrogate" => {
                return Err(Error::InvalidRequest);
            }
            Err(_) => None,
        }
    } else {
        None
    };
    let installs = parsed
        .as_ref()
        .and_then(|parsed| {
            if let Some(installs) = decode_installs(parsed) {
                return Some(installs);
            }
            let Value::Object(fields) = parsed else {
                return None;
            };
            let tool = tool?;
            if fields.iter().any(|(name, _)| name != &tool.tool) {
                return None;
            }
            parsed
                .get(&tool.tool)
                .map_or_else(|| Some(Vec::new()), decode_installs)
        })
        .unwrap_or_default();
    Ok(ObservationValue::Listing { present, installs })
}

fn all_listing(value: &Value) -> Result<ObservationValue> {
    let raw = optional_text(value, MAX_PROBE_BYTES)?;
    let parsed = match raw.as_deref() {
        Some(raw) => match json::parse_with_limits(
            raw,
            json::Limits {
                max_bytes: MAX_PROBE_BYTES,
                max_depth: 128,
                max_values: 131_072,
            },
        ) {
            Ok(value) => Some(value),
            Err(error) if error.message.contains("exceeds") => return Err(Error::InputLimit),
            Err(error) if error.message == "unpaired Unicode surrogate" => {
                return Err(Error::InvalidRequest);
            }
            Err(_) => None,
        },
        None => None,
    };
    let mut configured: Vec<(String, Vec<MiseInstall>)> = Vec::new();
    if let Some(Value::Object(fields)) = parsed {
        for (name, value) in fields {
            let Some(installs) = decode_installs(&value) else {
                return Ok(ObservationValue::AllInstalls(Vec::new()));
            };
            if let Some((_, previous)) = configured.iter_mut().find(|(key, _)| *key == name) {
                *previous = installs;
            } else {
                configured.push((name, installs));
            }
        }
        // Object.entries visits canonical array-index keys first, then other
        // keys in insertion order. Preserve that choice when paths are shared.
        let index = |key: &str| {
            key.parse::<u32>()
                .ok()
                .filter(|index| *index != u32::MAX && index.to_string() == key)
        };
        configured.sort_by_key(|(key, _)| index(key).map_or((1, 0), |index| (0, index)));
    }
    Ok(ObservationValue::AllInstalls(configured))
}

fn observed(value: &Value, tool: Option<&MiseTool>) -> Result<Observed> {
    keys(value, &["request", "result"])?;
    let request = decode_observation(field(value, "request")?)?;
    let value = field(value, "result")?;
    let result = match &request {
        Observation::Wrapper => ObservationValue::Text(optional_text(value, MAX_WRAPPER_BYTES)?),
        Observation::MiseListing => listing(value, tool)?,
        Observation::MiseAllListing => all_listing(value)?,
        Observation::PnpmVersion | Observation::VoltaPackage => {
            ObservationValue::Boolean(boolean(value)?)
        }
        Observation::MiseInstallPaths(paths) => {
            let values = text_array(value)?;
            if paths.len() != values.len() {
                return Err(Error::ObservationMismatch);
            }
            ObservationValue::Paths(values)
        }
        Observation::NpmShim => {
            keys(value, &["directory", "exists"])?;
            ObservationValue::NpmShim {
                directory: text(field(value, "directory")?)?,
                exists: boolean(field(value, "exists")?)?,
            }
        }
        Observation::MiseToolPath(_) => ObservationValue::Text(Some(text(value)?)),
        _ => ObservationValue::Text(optional_text(value, MAX_TEXT_BYTES)?),
    };
    Ok(Observed { request, result })
}

fn decode_request(value: &Value) -> Result<Request> {
    keys(
        value,
        &[
            "version",
            "operation",
            "mode",
            "owner",
            "context",
            "definition",
            "homebrew",
            "observations",
        ],
    )?;
    if field(value, "version")?.as_u64() != Some(1)
        || field(value, "operation")?.as_str() != Some("resolve")
    {
        return Err(Error::InvalidRequest);
    }
    let definition = definition(field(value, "definition")?)?;
    let values = field(value, "observations")?
        .as_array()
        .ok_or(Error::InvalidRequest)?;
    if values.len() > MAX_OBSERVATIONS {
        return Err(Error::InputLimit);
    }
    let mut observations = Vec::new();
    for value in values {
        observations.push(observed(value, definition.mise.as_ref())?);
    }
    Ok(Request {
        mode: identifier(field(value, "mode")?)?,
        owner: owner(field(value, "owner")?)?,
        context: context(field(value, "context")?)?,
        definition,
        homebrew: homebrew(field(value, "homebrew")?)?,
        observations,
    })
}

fn select_update_action(value: &Value) -> Result<Option<SelectedUpdateAction>> {
    keys(
        value,
        &[
            "version",
            "operation",
            "mode",
            "packageName",
            "update",
            "versionValue",
        ],
    )?;
    if field(value, "version")?.as_u64() != Some(1)
        || field(value, "operation")?.as_str() != Some("select-update-action")
    {
        return Err(Error::InvalidRequest);
    }
    let mode = field(value, "mode")?
        .as_str()
        .ok_or(Error::InvalidRequest)?;
    let package_name = optional_text(field(value, "packageName")?, MAX_TEXT_BYTES)?;
    let version = optional_text(field(value, "versionValue")?, MAX_TEXT_BYTES)?;
    let update = field(value, "update")?;
    let update = if update == &Value::Null {
        None
    } else {
        keys(update, &["command", "args", "lockKey"])?;
        Some(UpdateActionInput {
            command: text(field(update, "command")?)?,
            args: text_array(field(update, "args")?)?,
            lock_key: text(field(update, "lockKey")?)?,
        })
    };
    match mode {
        "target" => Ok(maintenance::make_targeted_update_action(
            package_name.as_deref(),
            update.as_ref(),
            version.as_deref().ok_or(Error::InvalidRequest)?,
        )),
        "latest" => Ok(maintenance::resolve_latest_update_action(
            package_name.as_deref(),
            update.as_ref(),
            version.as_deref(),
        )),
        _ => Err(Error::InvalidRequest),
    }
}

fn encode_selected_action(value: &SelectedUpdateAction) -> Value {
    object(vec![
        ("command", Value::String(value.command.clone())),
        (
            "args",
            Value::Array(value.args.iter().cloned().map(Value::String).collect()),
        ),
    ])
}

fn verified_provider(value: &Value) -> Result<ProviderSnapshot> {
    let mut allowed = vec![
        "checkedAt",
        "installed",
        "status",
        "authStatus",
        "version",
        "versionAdvisoryStatus",
    ];
    if value.get("installedVersion").is_some() {
        allowed.push("installedVersion");
    }
    if value.get("driver").is_some() {
        allowed.push("driver");
    }
    keys(value, &allowed)?;
    Ok(ProviderSnapshot {
        driver: value.get("driver").map(text).transpose()?,
        checked_at: text(field(value, "checkedAt")?)?,
        installed: boolean(field(value, "installed")?)?,
        status: text(field(value, "status")?)?,
        auth_status: text(field(value, "authStatus")?)?,
        version: optional_text(field(value, "version")?, MAX_TEXT_BYTES)?,
        installed_version: value
            .get("installedVersion")
            .map(|version| optional_text(version, MAX_TEXT_BYTES))
            .transpose()?,
        version_advisory_status: optional_text(
            field(value, "versionAdvisoryStatus")?,
            MAX_TEXT_BYTES,
        )?,
    })
}

fn verify_update(value: &Value) -> Result<Value> {
    let mode = field(value, "mode")?
        .as_str()
        .ok_or(Error::InvalidRequest)?;
    let allowed = match mode {
        "pre-update" => vec![
            "version",
            "operation",
            "mode",
            "priorCheckedAt",
            "advisoryEnriched",
            "verifiedProviders",
        ],
        "post-update" => vec![
            "version",
            "operation",
            "mode",
            "targetVersion",
            "verifiedProviders",
        ],
        _ => return Err(Error::InvalidRequest),
    };
    keys(value, &allowed)?;
    if field(value, "version")?.as_u64() != Some(1)
        || field(value, "operation")?.as_str() != Some("verify-update")
    {
        return Err(Error::InvalidRequest);
    }
    let providers = field(value, "verifiedProviders")?
        .as_array()
        .ok_or(Error::InvalidRequest)?
        .iter()
        .map(verified_provider)
        .collect::<Result<Vec<_>>>()?;
    let mut response = vec![
        ("kind", Value::String("verified".to_string())),
        ("mode", Value::String(mode.to_string())),
    ];
    if mode == "pre-update" {
        let prior = optional_text(field(value, "priorCheckedAt")?, MAX_TEXT_BYTES)?;
        let enriched = boolean(field(value, "advisoryEnriched")?)?;
        response.push((
            "alreadyCurrent",
            Value::Bool(provider_update_verification::already_current(
                prior.as_deref(),
                enriched,
                &providers,
            )),
        ));
    } else {
        let target = optional_text(field(value, "targetVersion")?, MAX_TEXT_BYTES)?;
        let verdict = provider_update_verification::verify_update(target.as_deref(), &providers);
        response.push(("status", Value::String(verdict.status().to_string())));
        response.push(("message", Value::String(verdict.message().to_string())));
    }
    Ok(object(response))
}

fn encode_response(result: Value) -> Result<String> {
    let response = object(vec![
        ("version", Value::Number("1".to_string())),
        ("ok", result),
    ]);
    let mut response = json::stringify(&response);
    response.push('\n');
    if response.len() > MAX_FRAME_BYTES {
        return Err(Error::OutputLimit);
    }
    Ok(response)
}

fn encode_lookup(value: &Lookup) -> Value {
    match value {
        Lookup::Mise { executable, tool } => object(vec![
            ("kind", Value::String("mise".to_string())),
            ("executable", Value::String(executable.clone())),
            ("tool", Value::String(tool.clone())),
        ]),
        Lookup::Homebrew {
            executable,
            ownership,
        } => object(vec![
            ("kind", Value::String("homebrew".to_string())),
            ("executable", Value::String(executable.clone())),
            ("name", Value::String(ownership.name.clone())),
            ("ownershipKind", Value::String(ownership.kind.clone())),
        ]),
        Lookup::Pnpm { package_name } => object(vec![
            ("kind", Value::String("pnpm".to_string())),
            ("packageName", Value::String(package_name.clone())),
        ]),
        Lookup::Npm {
            package_name,
            prefix,
        } => object(vec![
            ("kind", Value::String("npm".to_string())),
            ("packageName", Value::String(package_name.clone())),
            ("prefix", Value::String(prefix.clone())),
        ]),
        Lookup::VitePlus { package_name } => object(vec![
            ("kind", Value::String("vite-plus".to_string())),
            ("packageName", Value::String(package_name.clone())),
        ]),
        Lookup::Bun { package_name } => object(vec![
            ("kind", Value::String("bun".to_string())),
            ("packageName", Value::String(package_name.clone())),
        ]),
        Lookup::Chocolatey => object(vec![("kind", Value::String("chocolatey".to_string()))]),
    }
}

fn encode_capabilities(value: &Capabilities) -> Value {
    let mut fields = vec![
        ("provider", Value::String(value.owner.provider.clone())),
        (
            "packageName",
            value
                .owner
                .package_name
                .as_ref()
                .map_or(Value::Null, |name| Value::String(name.clone())),
        ),
        (
            "update",
            value.update.as_ref().map_or(Value::Null, |update| {
                object(vec![
                    ("command", Value::String(update.command.clone())),
                    ("executable", Value::String(update.executable.clone())),
                    (
                        "args",
                        Value::Array(update.args.iter().cloned().map(Value::String).collect()),
                    ),
                    ("lockKey", Value::String(update.lock_key.clone())),
                    ("env", Value::String(update.env.clone())),
                ])
            }),
        ),
    ];
    if let Some(path) = &value.install_path {
        fields.push(("installPath", Value::String(path.clone())));
    }
    if let Some(command) = &value.manual_command {
        fields.push(("manualCommand", Value::String(command.clone())));
    }
    if value.latest_unknown {
        fields.push(("latestVersion", Value::Null));
    }
    if let Some(lookup) = &value.latest_lookup {
        fields.push(("latestLookup", encode_lookup(lookup)));
    }
    object(fields)
}

pub struct Session {
    base: Option<Value>,
    observed: Vec<Value>,
    pending: Option<Observation>,
    completed: bool,
}

impl Default for Session {
    fn default() -> Self {
        Self::new()
    }
}

impl Session {
    pub fn new() -> Self {
        Self {
            base: None,
            observed: Vec::new(),
            pending: None,
            completed: false,
        }
    }

    pub fn respond(&mut self, frame: &str) -> Result<(String, bool)> {
        if frame.len() > MAX_FRAME_BYTES {
            return Err(Error::InputLimit);
        }
        if self.completed || !frame.ends_with('\n') {
            return Err(Error::InvalidRequest);
        }
        let value = json::parse_with_limits(
            frame,
            json::Limits {
                max_bytes: MAX_FRAME_BYTES,
                max_depth: 32,
                max_values: 131_072,
            },
        )
        .map_err(|error| {
            if error.message.contains("exceeds") {
                Error::InputLimit
            } else {
                Error::InvalidRequest
            }
        })?;
        if field(&value, "operation")?.as_str() == Some("select-update-action") {
            if self.base.is_some() {
                return Err(Error::ObservationMismatch);
            }
            let selected = select_update_action(&value)?;
            let response = encode_response(object(vec![
                ("kind", Value::String("selected".to_string())),
                (
                    "action",
                    selected
                        .as_ref()
                        .map_or(Value::Null, encode_selected_action),
                ),
            ]))?;
            self.completed = true;
            return Ok((response, true));
        }
        if field(&value, "operation")?.as_str() == Some("verify-update") {
            if self.base.is_some() {
                return Err(Error::ObservationMismatch);
            }
            let response = encode_response(verify_update(&value)?)?;
            self.completed = true;
            return Ok((response, true));
        }
        let request = decode_request(&value)?;
        let Value::Object(fields) = &value else {
            return Err(Error::InvalidRequest);
        };
        let base = Value::Object(
            fields
                .iter()
                .filter(|(key, _)| key != "observations")
                .cloned()
                .collect(),
        );
        let observations = field(&value, "observations")?
            .as_array()
            .ok_or(Error::InvalidRequest)?;
        if let Some(previous) = &self.base {
            if previous != &base
                || observations.len() != self.observed.len() + 1
                || !observations.starts_with(&self.observed)
            {
                return Err(Error::ObservationMismatch);
            }
            if request
                .observations
                .last()
                .map(|observed| &observed.request)
                != self.pending.as_ref()
            {
                return Err(Error::ObservationMismatch);
            }
        }
        let decision = maintenance::resolve(&request).map_err(|error| match error {
            maintenance::Error::ObservationMismatch => Error::ObservationMismatch,
            _ => Error::InvalidRequest,
        })?;
        let result = match &decision {
            Decision::Observe(observation) => object(vec![
                ("kind", Value::String("observe".to_string())),
                ("request", encode_observation(observation)),
            ]),
            Decision::Resolved(capabilities) => object(vec![
                ("kind", Value::String("resolved".to_string())),
                (
                    "capabilities",
                    capabilities
                        .as_ref()
                        .map_or(Value::Null, |value| encode_capabilities(value)),
                ),
            ]),
        };
        let response = encode_response(result)?;
        self.base = Some(base);
        self.observed = observations.to_vec();
        self.pending = if let Decision::Observe(observation) = decision {
            Some(observation)
        } else {
            None
        };
        self.completed = self.pending.is_none();
        Ok((response, self.completed))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn selection_request() -> Value {
        object(vec![
            ("version", Value::Number("1".to_string())),
            (
                "operation",
                Value::String("select-update-action".to_string()),
            ),
            ("mode", Value::String("target".to_string())),
            ("packageName", Value::String("@openai/codex".to_string())),
            (
                "update",
                object(vec![
                    (
                        "command",
                        Value::String("pnpm add @openai/codex@latest".to_string()),
                    ),
                    (
                        "args",
                        Value::Array(vec![
                            Value::String("add".to_string()),
                            Value::String("@openai/codex@latest".to_string()),
                        ]),
                    ),
                    ("lockKey", Value::String("pnpm-global".to_string())),
                ]),
            ),
            ("versionValue", Value::String("1.2.3".to_string())),
        ])
    }

    fn replace(value: &mut Value, key: &str, replacement: Value) {
        let Value::Object(fields) = value else {
            panic!("expected object");
        };
        fields.iter_mut().find(|entry| entry.0 == key).unwrap().1 = replacement;
    }

    fn frame(value: &Value) -> String {
        format!("{}\n", json::stringify(value))
    }

    fn verification_request(mode: &str) -> Value {
        let mut fields = vec![
            ("version", Value::Number("1".to_string())),
            ("operation", Value::String("verify-update".to_string())),
            ("mode", Value::String(mode.to_string())),
            (
                "verifiedProviders",
                Value::Array(vec![object(vec![
                    ("checkedAt", Value::String("new".to_string())),
                    ("installed", Value::Bool(true)),
                    ("status", Value::String("ready".to_string())),
                    ("authStatus", Value::String("authenticated".to_string())),
                    ("version", Value::String("1.2.3".to_string())),
                    (
                        "versionAdvisoryStatus",
                        Value::String("current".to_string()),
                    ),
                ])]),
            ),
        ];
        if mode == "pre-update" {
            fields.push(("priorCheckedAt", Value::String("old".to_string())));
            fields.push(("advisoryEnriched", Value::Bool(true)));
        } else {
            fields.push(("targetVersion", Value::String("1.2.3".to_string())));
        }
        object(fields)
    }

    #[test]
    fn verification_is_terminal_and_cannot_be_followed_by_another_operation() {
        for (mode, expected) in [
            (
                "pre-update",
                "{\"version\":1,\"ok\":{\"kind\":\"verified\",\"mode\":\"pre-update\",\"alreadyCurrent\":true}}\n",
            ),
            (
                "post-update",
                "{\"version\":1,\"ok\":{\"kind\":\"verified\",\"mode\":\"post-update\",\"status\":\"succeeded\",\"message\":\"Provider updated.\"}}\n",
            ),
        ] {
            let mut session = Session::new();
            let input = frame(&verification_request(mode));
            assert_eq!(session.respond(&input), Ok((expected.to_string(), true)));
            assert_eq!(session.respond(&input), Err(Error::InvalidRequest));
            assert_eq!(
                session.respond(&frame(&selection_request())),
                Err(Error::InvalidRequest)
            );
        }
    }

    #[test]
    fn verification_decoding_preserves_an_explicitly_unknown_installation() {
        let mut request = verification_request("post-update");
        let Value::Array(mut providers) = request.get("verifiedProviders").unwrap().clone() else {
            unreachable!();
        };
        let Value::Object(fields) = &mut providers[0] else {
            unreachable!();
        };
        fields.push(("installedVersion".to_string(), Value::Null));
        replace(&mut request, "verifiedProviders", Value::Array(providers));
        let (response, completed) = Session::new().respond(&frame(&request)).unwrap();
        assert!(completed);
        assert_eq!(
            response,
            "{\"version\":1,\"ok\":{\"kind\":\"verified\",\"mode\":\"post-update\",\"status\":\"unchanged\",\"message\":\"Update command completed, but T3 Code could not verify the provider version.\"}}\n"
        );
    }

    #[test]
    fn selection_is_a_single_terminal_frame_with_only_action_fields() {
        let input = frame(&selection_request());
        let mut session = Session::new();
        assert_eq!(
            session.respond(&input),
            Ok((
                "{\"version\":1,\"ok\":{\"kind\":\"selected\",\"action\":{\"command\":\"pnpm add @openai/codex@1.2.3\",\"args\":[\"add\",\"@openai/codex@1.2.3\"]}}}\n".to_string(),
                true,
            ))
        );
        assert_eq!(session.respond(&input), Err(Error::InvalidRequest));
    }

    #[test]
    fn selection_preserves_null_and_latest_original_action() {
        let mut input = selection_request();
        replace(&mut input, "mode", Value::String("latest".to_string()));
        replace(&mut input, "versionValue", Value::Null);
        let (response, completed) = Session::new().respond(&frame(&input)).unwrap();
        assert!(completed);
        let response = json::parse(&response).unwrap();
        assert_eq!(
            response
                .get("ok")
                .unwrap()
                .get("action")
                .unwrap()
                .get("command"),
            Some(&Value::String("pnpm add @openai/codex@latest".to_string()))
        );
        replace(&mut input, "update", Value::Null);
        replace(&mut input, "packageName", Value::Null);
        assert_eq!(
            Session::new().respond(&frame(&input)),
            Ok((
                "{\"version\":1,\"ok\":{\"kind\":\"selected\",\"action\":null}}\n".to_string(),
                true
            ))
        );
    }

    #[test]
    fn selection_rejects_invalid_fields_types_and_duplicate_keys() {
        for (key, value) in [
            ("version", Value::Number("2".to_string())),
            ("mode", Value::String("resolve".to_string())),
            ("mode", Value::Null),
            ("versionValue", Value::Null),
            ("versionValue", Value::Number("1".to_string())),
            ("packageName", Value::Bool(false)),
            ("update", Value::Array(vec![])),
        ] {
            let mut input = selection_request();
            replace(&mut input, key, value);
            assert_eq!(
                Session::new().respond(&frame(&input)),
                Err(Error::InvalidRequest),
                "{key}"
            );
        }
        let Value::Object(fields) = selection_request() else {
            unreachable!();
        };
        for index in 0..fields.len() {
            let mut incomplete = fields.clone();
            incomplete.remove(index);
            assert_eq!(
                Session::new().respond(&frame(&Value::Object(incomplete))),
                Err(Error::InvalidRequest)
            );
            let mut duplicate = fields.clone();
            duplicate.push(fields[index].clone());
            assert_eq!(
                Session::new().respond(&frame(&Value::Object(duplicate))),
                Err(Error::DuplicateKey)
            );
        }
        let mut extra = fields;
        extra.push(("observations".to_string(), Value::Array(vec![])));
        assert_eq!(
            Session::new().respond(&frame(&Value::Object(extra))),
            Err(Error::InvalidRequest)
        );

        for (key, value) in [
            ("command", Value::Null),
            ("args", Value::String("not an array".to_string())),
            ("args", Value::Array(vec![Value::Null])),
            ("lockKey", Value::Bool(false)),
        ] {
            let mut input = selection_request();
            let mut update = input.get("update").unwrap().clone();
            replace(&mut update, key, value);
            replace(&mut input, "update", update);
            assert_eq!(
                Session::new().respond(&frame(&input)),
                Err(Error::InvalidRequest),
                "update.{key}"
            );
        }
        let mut input = selection_request();
        let Value::Object(mut update) = input.get("update").unwrap().clone() else {
            unreachable!();
        };
        update.push(("env".to_string(), Value::Null));
        replace(&mut input, "update", Value::Object(update));
        assert_eq!(
            Session::new().respond(&frame(&input)),
            Err(Error::InvalidRequest)
        );
    }

    #[test]
    fn selection_enforces_text_frame_and_expanded_output_limits() {
        let mut input = selection_request();
        replace(
            &mut input,
            "packageName",
            Value::String("a".repeat(MAX_TEXT_BYTES + 1)),
        );
        assert_eq!(
            Session::new().respond(&frame(&input)),
            Err(Error::InputLimit)
        );
        assert_eq!(
            Session::new().respond(&" ".repeat(MAX_FRAME_BYTES + 1)),
            Err(Error::InputLimit)
        );
        assert_eq!(
            Session::new().respond(&json::stringify(&selection_request())),
            Err(Error::InvalidRequest)
        );

        let mut input = selection_request();
        replace(
            &mut input,
            "versionValue",
            Value::String(format!("{}.2.3", "1".repeat(MAX_TEXT_BYTES - 4))),
        );
        let mut update = input.get("update").unwrap().clone();
        let mut args = vec![Value::String("@openai/codex@latest".to_string())];
        args.extend((0..14).map(|_| Value::String("a".repeat(MAX_TEXT_BYTES))));
        replace(&mut update, "args", Value::Array(args));
        replace(&mut input, "update", update);
        let input = frame(&input);
        assert!(input.len() < MAX_FRAME_BYTES);
        assert_eq!(Session::new().respond(&input), Err(Error::OutputLimit));
    }

    #[test]
    fn selection_cannot_replace_pending_resolver_observations() {
        let request = "{\"version\":1,\"operation\":\"resolve\",\"mode\":\"package\",\"owner\":{\"provider\":\"codex\",\"packageName\":\"@openai/codex\"},\"context\":{\"resolvedCommandPath\":\"/usr/bin/codex\",\"realCommandPath\":\"/usr/bin/codex\",\"platform\":\"linux\",\"hostPlatform\":\"linux\",\"commandPlatform\":\"linux\",\"chocolateyInstall\":null},\"definition\":{\"mise\":null,\"native\":null},\"homebrew\":null,\"observations\":[]}\n";
        let mut session = Session::new();
        let (response, completed) = session.respond(request).unwrap();
        assert!(!completed);
        assert_eq!(
            response,
            "{\"version\":1,\"ok\":{\"kind\":\"observe\",\"request\":{\"kind\":\"wrapper\"}}}\n"
        );
        assert_eq!(
            session.respond(&frame(&selection_request())),
            Err(Error::ObservationMismatch)
        );
        for mode in ["pre-update", "post-update"] {
            assert_eq!(
                session.respond(&frame(&verification_request(mode))),
                Err(Error::ObservationMismatch)
            );
        }

        let mut request = json::parse(request).unwrap();
        let mut observations = vec![object(vec![
            (
                "request",
                object(vec![("kind", Value::String("wrapper".to_string()))]),
            ),
            ("result", Value::Null),
        ])];
        replace(
            &mut request,
            "observations",
            Value::Array(observations.clone()),
        );
        let (response, completed) = session.respond(&frame(&request)).unwrap();
        assert!(!completed);
        assert_eq!(
            response,
            "{\"version\":1,\"ok\":{\"kind\":\"observe\",\"request\":{\"kind\":\"macPortsOwner\"}}}\n"
        );
        assert_eq!(
            session.respond(&frame(&selection_request())),
            Err(Error::ObservationMismatch)
        );
        for mode in ["pre-update", "post-update"] {
            assert_eq!(
                session.respond(&frame(&verification_request(mode))),
                Err(Error::ObservationMismatch)
            );
        }
        observations.push(object(vec![
            (
                "request",
                object(vec![("kind", Value::String("macPortsOwner".to_string()))]),
            ),
            ("result", Value::Null),
        ]));
        replace(&mut request, "observations", Value::Array(observations));
        let (response, completed) = session.respond(&frame(&request)).unwrap();
        assert!(completed);
        assert_eq!(
            json::parse(&response)
                .unwrap()
                .get("ok")
                .unwrap()
                .get("kind"),
            Some(&Value::String("resolved".to_string()))
        );
    }
}
