//! Project projection updates paired with the existing command planner.
//! Copyright (c) 2026 T3 Tools Inc. Distributed under the repository's MIT license.

use crate::json::{self, Value};
use crate::project_commands_wire::{self, Error, WireResult};

pub const MAX_FRAME_BYTES: usize = 16 * 1024 * 1024;

fn field<'a>(value: &'a Value, key: &str) -> WireResult<&'a Value> {
    value.get(key).ok_or(Error::InvalidRequest)
}

fn set(value: &mut Value, key: &str, replacement: Value) -> WireResult<()> {
    let Value::Object(entries) = value else {
        return Err(Error::InvalidRequest);
    };
    if let Some(entry) = entries.iter_mut().find(|entry| entry.0 == key) {
        entry.1 = replacement;
    } else {
        entries.push((key.to_string(), replacement));
    }
    Ok(())
}

fn validate_row(row: &Value, project_id: &Value) -> WireResult<()> {
    if field(row, "projectId")? != project_id {
        return Err(Error::InvalidRequest);
    }
    for name in [
        "projectId",
        "title",
        "workspaceRoot",
        "createdAt",
        "updatedAt",
    ] {
        field(row, name)?.as_str().ok_or(Error::InvalidRequest)?;
    }
    for name in [
        "defaultThreadEnvMode",
        "faviconPath",
        "pinnedAt",
        "archivedAt",
        "deletedAt",
    ] {
        let value = field(row, name)?;
        if value != &Value::Null && value.as_str().is_none() {
            return Err(Error::InvalidRequest);
        }
    }
    for name in ["defaultModelSelection", "projectIcon"] {
        let value = field(row, name)?;
        if value != &Value::Null && !matches!(value, Value::Object(_)) {
            return Err(Error::InvalidRequest);
        }
    }
    field(row, "autoPull")?
        .as_bool()
        .ok_or(Error::InvalidRequest)?;
    field(row, "scripts")?
        .as_array()
        .ok_or(Error::InvalidRequest)?;
    Ok(())
}

/// Checks every field used when a stored row participates in project decisions.
pub fn validate_projection_shape(row: &Value) -> WireResult<()> {
    validate_row(row, field(row, "projectId")?)
}

fn ecma_whitespace(character: char) -> bool {
    matches!(character, '\u{0009}'..='\u{000d}' | '\u{0020}' | '\u{00a0}' | '\u{1680}'
        | '\u{2000}'..='\u{200a}' | '\u{2028}' | '\u{2029}' | '\u{202f}' | '\u{205f}'
        | '\u{3000}' | '\u{feff}')
}

fn trimmed(value: &Value) -> WireResult<Value> {
    let text = value.as_str().ok_or(Error::InvalidRequest)?;
    let text = text.trim_matches(ecma_whitespace);
    if text.is_empty() {
        return Err(Error::InvalidRequest);
    }
    Ok(Value::String(text.to_string()))
}

fn known_fields(value: &Value, names: &[&str]) -> WireResult<Value> {
    if !matches!(value, Value::Object(_)) {
        return Err(Error::InvalidRequest);
    }
    let mut result = Value::Object(Vec::new());
    for name in names {
        if let Some(value) = value.get(name) {
            set(&mut result, name, value.clone())?;
        }
    }
    Ok(result)
}

fn normalize_model(value: &Value) -> WireResult<Value> {
    if value == &Value::Null {
        return Ok(Value::Null);
    }
    let mut model = known_fields(value, &["instanceId", "model", "options"])?;
    for name in ["instanceId", "model"] {
        set(&mut model, name, trimmed(field(value, name)?)?)?;
    }
    if let Some(options) = value.get("options") {
        let mut normalized = Vec::new();
        for option in options.as_array().ok_or(Error::InvalidRequest)? {
            let mut result = known_fields(option, &["id", "value"])?;
            set(&mut result, "id", trimmed(field(option, "id")?)?)?;
            let value = field(option, "value")?;
            set(
                &mut result,
                "value",
                if value.as_bool().is_some() {
                    value.clone()
                } else {
                    trimmed(value)?
                },
            )?;
            normalized.push(result);
        }
        set(&mut model, "options", Value::Array(normalized))?;
    }
    Ok(model)
}

fn normalize_icon(value: &Value) -> WireResult<Value> {
    if value == &Value::Null {
        return Ok(Value::Null);
    }
    match field(value, "kind")?.as_str() {
        Some("emoji") => {
            let mut result = known_fields(value, &["kind", "emoji"])?;
            set(&mut result, "emoji", trimmed(field(value, "emoji")?)?)?;
            Ok(result)
        }
        Some("monogram") => {
            let mut result = known_fields(value, &["kind", "text", "color"])?;
            set(&mut result, "text", trimmed(field(value, "text")?)?)?;
            Ok(result)
        }
        Some("lucide") => {
            if let Some(text) = value.get("monogramText").or_else(|| value.get("monogram")) {
                Ok(Value::Object(vec![
                    ("kind".to_string(), Value::String("monogram".to_string())),
                    ("text".to_string(), trimmed(text)?),
                    ("color".to_string(), field(value, "color")?.clone()),
                ]))
            } else {
                let mut result = known_fields(value, &["kind", "name", "color"])?;
                set(&mut result, "name", trimmed(field(value, "name")?)?)?;
                Ok(result)
            }
        }
        _ => Err(Error::InvalidRequest),
    }
}

fn normalize_row(row: Value) -> WireResult<Value> {
    let mut result = known_fields(
        &row,
        &[
            "projectId",
            "title",
            "workspaceRoot",
            "defaultModelSelection",
            "defaultThreadEnvMode",
            "autoPull",
            "faviconPath",
            "projectIcon",
            "scripts",
            "pinnedAt",
            "archivedAt",
            "createdAt",
            "updatedAt",
            "deletedAt",
        ],
    )?;
    set(
        &mut result,
        "projectId",
        trimmed(field(&row, "projectId")?)?,
    )?;
    set(
        &mut result,
        "defaultModelSelection",
        normalize_model(field(&row, "defaultModelSelection")?)?,
    )?;
    set(
        &mut result,
        "projectIcon",
        normalize_icon(field(&row, "projectIcon")?)?,
    )?;
    let mut scripts = Vec::new();
    for script in field(&row, "scripts")?
        .as_array()
        .ok_or(Error::InvalidRequest)?
    {
        let mut normalized = known_fields(
            script,
            &[
                "id",
                "name",
                "command",
                "icon",
                "runOnWorktreeCreate",
                "runOnSettle",
                "stopOnSettle",
                "async",
                "previewUrl",
                "autoOpenPreview",
            ],
        )?;
        for name in ["id", "name", "command"] {
            set(&mut normalized, name, trimmed(field(script, name)?)?)?;
        }
        if let Some(value) = script.get("previewUrl") {
            set(&mut normalized, "previewUrl", trimmed(value)?)?;
        }
        scripts.push(normalized);
    }
    set(&mut result, "scripts", Value::Array(scripts))?;
    Ok(result)
}

/// Applies the same row changes as ProjectStoreV2.apply to a validated planner event.
pub fn project_event(existing: &Value, event: &Value) -> WireResult<Value> {
    let payload = field(event, "payload")?;
    let project_id = field(payload, "projectId")?;
    project_id.as_str().ok_or(Error::InvalidRequest)?;
    match field(event, "type")?.as_str() {
        Some("project.created") => {
            let mut row = Value::Object(Vec::new());
            for name in [
                "projectId",
                "title",
                "workspaceRoot",
                "defaultModelSelection",
                "scripts",
                "createdAt",
                "updatedAt",
            ] {
                set(&mut row, name, field(payload, name)?.clone())?;
            }
            for name in ["defaultThreadEnvMode", "faviconPath", "projectIcon"] {
                set(
                    &mut row,
                    name,
                    payload.get(name).cloned().unwrap_or(Value::Null),
                )?;
            }
            set(&mut row, "autoPull", Value::Bool(false))?;
            for name in ["pinnedAt", "archivedAt", "deletedAt"] {
                set(&mut row, name, Value::Null)?;
            }
            validate_row(&row, project_id)?;
            normalize_row(row)
        }
        Some("project.meta-updated") | Some("project.deleted") => {
            if existing == &Value::Null {
                return Ok(Value::Null);
            }
            validate_row(existing, project_id)?;
            let mut row = existing.clone();
            if field(event, "type")?.as_str() == Some("project.deleted") {
                let deleted_at = field(payload, "deletedAt")?.clone();
                set(&mut row, "deletedAt", deleted_at.clone())?;
                set(&mut row, "updatedAt", deleted_at)?;
            } else {
                for name in [
                    "title",
                    "workspaceRoot",
                    "defaultModelSelection",
                    "defaultThreadEnvMode",
                    "autoPull",
                    "faviconPath",
                    "projectIcon",
                    "scripts",
                    "pinnedAt",
                    "archivedAt",
                ] {
                    if let Some(value) = payload.get(name) {
                        set(&mut row, name, value.clone())?;
                    }
                }
                set(&mut row, "updatedAt", field(payload, "updatedAt")?.clone())?;
            }
            validate_row(&row, project_id)?;
            normalize_row(row)
        }
        _ => Err(Error::InvalidRequest),
    }
}

/// The database owns the snapshot revision and commits this event and row together.
pub fn evaluate(input: &Value) -> WireResult<Value> {
    if field(input, "operation")?.as_str() != Some("plan-and-project") {
        return Err(Error::InvalidRequest);
    }
    let mut planner_input = input.clone();
    set(
        &mut planner_input,
        "operation",
        Value::String("plan".to_string()),
    )?;
    let mut outcome = project_commands_wire::evaluate(&planner_input)?;
    if let Some(event) = outcome.get("ok").cloned() {
        let existing = field(field(input, "state")?, "project")?;
        let project = project_event(existing, &event)?;
        set(
            &mut outcome,
            "ok",
            Value::Object(vec![
                ("event".to_string(), event),
                ("project".to_string(), project),
            ]),
        )?;
    }
    Ok(outcome)
}

pub fn respond(frame: &str) -> WireResult<String> {
    if frame.len() > MAX_FRAME_BYTES {
        return Err(Error::InputLimit);
    }
    let body = frame.strip_suffix('\n').ok_or(Error::InvalidRequest)?;
    if body.contains('\n') || body.contains('\r') {
        return Err(Error::InvalidRequest);
    }
    let input = json::parse_with_limits(
        body,
        json::Limits {
            max_bytes: MAX_FRAME_BYTES,
            max_depth: 64,
            max_values: 262_144,
        },
    )
    .map_err(|_| Error::InvalidRequest)?;
    let mut response = json::stringify(&evaluate(&input)?);
    if response.len() >= MAX_FRAME_BYTES {
        return Err(Error::OutputLimit);
    }
    response.push('\n');
    Ok(response)
}
