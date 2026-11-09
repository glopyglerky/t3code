//! A single bounded request for the Codex structured-generation runner.

use crate::codex_generation::{self, Error as GenerationError, Model, Request};
use crate::json::{self, Value};
use std::collections::HashSet;

pub const MAX_REQUEST_BYTES: usize = 8 * 1024 * 1024;
pub const MAX_RESPONSE_BYTES: usize = 24 * 1024 * 1024 + 1024;
const MAX_TEXT_BYTES: usize = 65_536;
const MAX_ROWS: usize = 4096;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    InvalidRequest,
    InputLimit,
    OutputLimit,
    Io,
}

impl Error {
    pub fn message(self) -> &'static str {
        match self {
            Self::InvalidRequest => "invalid generation request",
            Self::InputLimit => "generation input limit exceeded",
            Self::OutputLimit => "generation output limit exceeded",
            Self::Io => "generation input or output failed",
        }
    }
}

type Result<T> = std::result::Result<T, Error>;

fn fields(value: &Value, names: &[&str]) -> Result<()> {
    let Value::Object(entries) = value else {
        return Err(Error::InvalidRequest);
    };
    if entries.len() != names.len()
        || entries
            .iter()
            .any(|(key, _)| !names.contains(&key.as_str()))
    {
        return Err(Error::InvalidRequest);
    }
    Ok(())
}

fn field<'a>(value: &'a Value, name: &str) -> Result<&'a Value> {
    value.get(name).ok_or(Error::InvalidRequest)
}

fn text(value: &Value, maximum: usize) -> Result<String> {
    let text = value.as_str().ok_or(Error::InvalidRequest)?;
    if text.len() > maximum {
        return Err(Error::InputLimit);
    }
    Ok(text.to_owned())
}

fn argument(value: &Value) -> Result<String> {
    let value = text(value, MAX_TEXT_BYTES)?;
    if value.contains('\0') {
        return Err(Error::InvalidRequest);
    }
    Ok(value)
}

fn path(value: &Value) -> Result<String> {
    let value = argument(value)?;
    if !value.starts_with('/') {
        return Err(Error::InvalidRequest);
    }
    Ok(value)
}

fn boolean(value: &Value) -> Result<bool> {
    value.as_bool().ok_or(Error::InvalidRequest)
}

fn array(value: &Value) -> Result<&[Value]> {
    let values = value.as_array().ok_or(Error::InvalidRequest)?;
    if values.len() > MAX_ROWS {
        return Err(Error::InputLimit);
    }
    Ok(values)
}

fn no_duplicates(value: &Value) -> Result<()> {
    match value {
        Value::Object(entries) => {
            let mut keys = HashSet::new();
            for (key, value) in entries {
                if !keys.insert(key.as_str()) {
                    return Err(Error::InvalidRequest);
                }
                no_duplicates(value)?;
            }
        }
        Value::Array(entries) => {
            for value in entries {
                no_duplicates(value)?;
            }
        }
        _ => {}
    }
    Ok(())
}

pub fn parse(frame: &str) -> Result<Request> {
    if frame.len() > MAX_REQUEST_BYTES {
        return Err(Error::InputLimit);
    }
    if !frame.ends_with('\n') || frame[..frame.len() - 1].contains('\n') {
        return Err(Error::InvalidRequest);
    }
    let value = json::parse_with_limits(
        frame,
        json::Limits {
            max_bytes: MAX_REQUEST_BYTES,
            max_depth: 64,
            max_values: 262_144,
        },
    )
    .map_err(|_| Error::InvalidRequest)?;
    no_duplicates(&value)?;
    fields(
        &value,
        &[
            "version",
            "operation",
            "binary",
            "cwd",
            "prompt",
            "outputSchema",
            "launchArgs",
            "models",
            "customModels",
            "modelSelection",
            "managed",
            "imagePaths",
            "timeoutMs",
        ],
    )?;
    if field(&value, "version")?.as_u64() != Some(1)
        || field(&value, "operation")?.as_str() != Some("run")
    {
        return Err(Error::InvalidRequest);
    }
    let mut models = Vec::new();
    for entry in array(field(&value, "models")?)? {
        fields(entry, &["slug", "isCustom", "isDefault", "isLegacy"])?;
        models.push(Model {
            slug: argument(field(entry, "slug")?)?,
            custom: boolean(field(entry, "isCustom")?)?,
            default: boolean(field(entry, "isDefault")?)?,
            legacy: boolean(field(entry, "isLegacy")?)?,
        });
    }
    let selection = field(&value, "modelSelection")?;
    fields(selection, &["model", "options"])?;
    let mut options = Vec::new();
    for entry in array(field(selection, "options")?)? {
        fields(entry, &["id", "value"])?;
        let option = field(entry, "value")?;
        if let Value::String(_) = option {
            argument(option)?;
        } else if !matches!(option, Value::Bool(_)) {
            return Err(Error::InvalidRequest);
        }
        options.push((argument(field(entry, "id")?)?, option.clone()));
    }
    let output_schema = field(&value, "outputSchema")?.clone();
    if json::stringify(&output_schema).len() > codex_generation::MAX_SCHEMA_BYTES {
        return Err(Error::InputLimit);
    }
    let timeout_ms = field(&value, "timeoutMs")?
        .as_u64()
        .ok_or(Error::InvalidRequest)?;
    if !(1..=900_000).contains(&timeout_ms) {
        return Err(Error::InvalidRequest);
    }
    let image_paths = array(field(&value, "imagePaths")?)?
        .iter()
        .map(path)
        .collect::<Result<Vec<_>>>()?;
    Ok(Request {
        binary: argument(field(&value, "binary")?)?,
        cwd: path(field(&value, "cwd")?)?,
        prompt: text(field(&value, "prompt")?, codex_generation::MAX_PROMPT_BYTES)?,
        output_schema,
        launch_args: argument(field(&value, "launchArgs")?)?,
        models,
        custom_models: field(&value, "customModels")?.clone(),
        model: argument(field(selection, "model")?)?,
        options,
        managed: boolean(field(&value, "managed")?)?,
        image_paths,
        timeout_ms,
    })
}

pub fn response(result: std::result::Result<String, GenerationError>) -> Result<String> {
    let mut fields = vec![("version".to_owned(), Value::Number("1".to_owned()))];
    fields.push(match result {
        Ok(output) => (
            "ok".to_owned(),
            Value::Object(vec![("output".to_owned(), Value::String(output))]),
        ),
        Err(error) => (
            "error".to_owned(),
            Value::Object(vec![
                ("kind".to_owned(), Value::String(error.kind.to_owned())),
                ("detail".to_owned(), Value::String(error.detail)),
            ]),
        ),
    });
    let response = format!("{}\n", json::stringify(&Value::Object(fields)));
    if response.len() > MAX_RESPONSE_BYTES {
        return Err(Error::OutputLimit);
    }
    Ok(response)
}
