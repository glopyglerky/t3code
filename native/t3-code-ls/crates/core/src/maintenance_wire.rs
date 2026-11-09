//! JSONL observations for the provider ownership policy.

use crate::json::{self, Value};
use crate::maintenance::{
    self, Capabilities, Context, Decision, Definition, Homebrew, Lookup, MiseInstall, MiseTool,
    NativeUpdate, Observation, ObservationValue, Observed, Owner, Request,
};
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
        "miseInstallPaths" => Ok(Observation::MiseInstallPaths(text_array(field(
            value, "paths",
        )?)?)),
        "miseToolPath" => Ok(Observation::MiseToolPath(text(field(value, "path")?)?)),
        "pnpmVersion" => Ok(Observation::PnpmVersion),
        "voltaPackage" => Ok(Observation::VoltaPackage),
        "npmShim" => Ok(Observation::NpmShim),
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
                    Observation::PnpmVersion => "pnpmVersion",
                    Observation::VoltaPackage => "voltaPackage",
                    Observation::NpmShim => "npmShim",
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

fn observed(value: &Value, tool: Option<&MiseTool>) -> Result<Observed> {
    keys(value, &["request", "result"])?;
    let request = decode_observation(field(value, "request")?)?;
    let value = field(value, "result")?;
    let result = match &request {
        Observation::Wrapper => ObservationValue::Text(optional_text(value, MAX_WRAPPER_BYTES)?),
        Observation::MiseListing => listing(value, tool)?,
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
        let response = object(vec![
            ("version", Value::Number("1".to_string())),
            ("ok", result),
        ]);
        let mut response = json::stringify(&response);
        response.push('\n');
        if response.len() > MAX_FRAME_BYTES {
            return Err(Error::OutputLimit);
        }
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
