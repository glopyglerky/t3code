//! Bounded project command planning with retained JSON payload fields.
//! Copyright (c) 2026 T3 Tools Inc. Distributed under the repository's MIT license.

use crate::guest_share::Timestamp;
use crate::json::{self, Value};
use crate::project_commands::{
    self, Command, CommandKind, Event, EventKind, Icon, MetadataChanges, Project, Rejection,
    Script, State,
};
use std::collections::HashSet;

pub const MAX_FRAME_BYTES: usize = 128 * 1024 * 1024;
const MAX_DEPTH: usize = 128;
const MAX_VALUES: usize = 1_048_576;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    InvalidRequest,
    DuplicateKey,
    InputLimit,
    OutputLimit,
    Io,
}

impl Error {
    pub fn message(self) -> &'static str {
        match self {
            Self::InvalidRequest => "invalid request",
            Self::DuplicateKey => "duplicate object key",
            Self::InputLimit => "input limit exceeded",
            Self::OutputLimit => "output limit exceeded",
            Self::Io => "input or output failed",
        }
    }
}

pub type WireResult<T> = Result<T, Error>;

fn field<'a>(value: &'a Value, key: &str) -> WireResult<&'a Value> {
    value.get(key).ok_or(Error::InvalidRequest)
}

fn text(value: &Value) -> WireResult<String> {
    value
        .as_str()
        .map(str::to_string)
        .ok_or(Error::InvalidRequest)
}

fn nullable_text(value: &Value) -> WireResult<Option<String>> {
    if *value == Value::Null {
        Ok(None)
    } else {
        text(value).map(Some)
    }
}

fn optional_bool(value: Option<&Value>) -> WireResult<Option<bool>> {
    value
        .map(|value| value.as_bool().ok_or(Error::InvalidRequest))
        .transpose()
}

fn object(entries: Vec<(&str, Value)>) -> Value {
    Value::Object(
        entries
            .into_iter()
            .map(|(key, value)| (key.to_string(), value))
            .collect(),
    )
}

fn retained_object(value: &Value) -> WireResult<Value> {
    if let Value::Object(_) = value {
        Ok(value.clone())
    } else {
        Err(Error::InvalidRequest)
    }
}

fn nullable_object(value: &Value) -> WireResult<Option<Value>> {
    if *value == Value::Null {
        Ok(None)
    } else {
        retained_object(value).map(Some)
    }
}

fn unique_keys(value: &Value) -> WireResult<()> {
    match value {
        Value::Object(entries) => {
            let mut keys = HashSet::new();
            for (key, value) in entries {
                if !keys.insert(key.as_str()) {
                    return Err(Error::DuplicateKey);
                }
                unique_keys(value)?;
            }
        }
        Value::Array(values) => {
            for value in values {
                unique_keys(value)?;
            }
        }
        Value::Number(value) => {
            let number: f64 = value.parse().map_err(|_| Error::InvalidRequest)?;
            if !number.is_finite() {
                return Err(Error::InvalidRequest);
            }
        }
        _ => {}
    }
    Ok(())
}

fn exact_fields(value: &Value, keys: &[&str]) -> WireResult<()> {
    let Value::Object(entries) = value else {
        return Err(Error::InvalidRequest);
    };
    if entries.len() != keys.len() || entries.iter().any(|(key, _)| !keys.contains(&key.as_str())) {
        return Err(Error::InvalidRequest);
    }
    Ok(())
}

fn scripts(value: &Value) -> WireResult<Vec<Script<Value>>> {
    value
        .as_array()
        .ok_or(Error::InvalidRequest)?
        .iter()
        .map(|script| {
            Ok(Script {
                id: text(field(script, "id")?)?,
                payload: retained_object(script)?,
            })
        })
        .collect()
}

fn project(value: &Value) -> WireResult<Option<Project>> {
    if *value == Value::Null {
        return Ok(None);
    }
    Ok(Some(Project {
        deleted_at: nullable_text(field(value, "deletedAt")?)?,
        script_ids: field(value, "scripts")?
            .as_array()
            .ok_or(Error::InvalidRequest)?
            .iter()
            .map(|script| text(field(script, "id")?))
            .collect::<WireResult<Vec<String>>>()?,
        pinned_at: nullable_text(field(value, "pinnedAt")?)?,
        archived_at: nullable_text(field(value, "archivedAt")?)?,
    }))
}

fn command(input: &Value) -> WireResult<Command<Value>> {
    let value = field(input, "command")?;
    let name = field(value, "type")?
        .as_str()
        .ok_or(Error::InvalidRequest)?;
    let count = field(input, "monogramGraphemeCount")?;
    let icon = value.get("projectIcon");
    let monogram = name == "project.meta.update"
        && icon
            .and_then(|icon| icon.get("kind"))
            .and_then(Value::as_str)
            == Some("monogram");
    let graphemes = if monogram {
        let count = count.as_u64().ok_or(Error::InvalidRequest)?;
        let text = icon
            .and_then(|icon| icon.get("text"))
            .and_then(Value::as_str)
            .ok_or(Error::InvalidRequest)?;
        // Segmenter cannot produce more graphemes than Unicode scalar values.
        if count > text.chars().count() as u64 || (count == 0) != text.is_empty() {
            return Err(Error::InvalidRequest);
        }
        Some(count)
    } else if *count == Value::Null {
        None
    } else {
        return Err(Error::InvalidRequest);
    };
    let kind = match name {
        "project.create" => CommandKind::Create {
            title: text(field(value, "title")?)?,
            workspace_root: text(field(value, "workspaceRoot")?)?,
            scripts: value.get("scripts").map(scripts).transpose()?,
        },
        "project.meta.update" => CommandKind::Update {
            changes: MetadataChanges {
                title: value.get("title").map(text).transpose()?,
                workspace_root: value.get("workspaceRoot").map(text).transpose()?,
                default_model_selection: value
                    .get("defaultModelSelection")
                    .map(nullable_object)
                    .transpose()?,
                default_thread_env_mode: value
                    .get("defaultThreadEnvMode")
                    .map(nullable_text)
                    .transpose()?,
                auto_pull: optional_bool(value.get("autoPull"))?,
                favicon_path: value.get("faviconPath").map(nullable_text).transpose()?,
                project_icon: icon
                    .map(|icon| -> WireResult<Option<Icon<Value>>> {
                        if *icon == Value::Null {
                            return Ok(None);
                        }
                        match field(icon, "kind")?.as_str() {
                            Some("lucide" | "emoji" | "monogram") => Ok(Some(Icon {
                                monogram_graphemes: graphemes,
                                payload: retained_object(icon)?,
                            })),
                            _ => Err(Error::InvalidRequest),
                        }
                    })
                    .transpose()?,
                scripts: value.get("scripts").map(scripts).transpose()?,
            },
            pinned: optional_bool(value.get("pinned"))?,
            archived: optional_bool(value.get("archived"))?,
        },
        "project.delete" => CommandKind::Delete,
        _ => return Err(Error::InvalidRequest),
    };
    Ok(Command {
        command_id: text(field(value, "commandId")?)?,
        project_id: text(field(value, "projectId")?)?,
        kind,
    })
}

fn string_option(value: &Option<String>) -> Value {
    value
        .as_ref()
        .map_or(Value::Null, |value| Value::String(value.clone()))
}

fn script_values(scripts: &[Script<Value>]) -> Value {
    Value::Array(
        scripts
            .iter()
            .map(|script| script.payload.clone())
            .collect(),
    )
}

fn metadata_fields(changes: &MetadataChanges<Value>) -> Vec<(&'static str, Value)> {
    let mut fields = Vec::new();
    if let Some(value) = &changes.title {
        fields.push(("title", Value::String(value.clone())));
    }
    if let Some(value) = &changes.workspace_root {
        fields.push(("workspaceRoot", Value::String(value.clone())));
    }
    if let Some(value) = &changes.default_model_selection {
        fields.push((
            "defaultModelSelection",
            value.clone().unwrap_or(Value::Null),
        ));
    }
    if let Some(value) = &changes.default_thread_env_mode {
        fields.push(("defaultThreadEnvMode", string_option(value)));
    }
    if let Some(value) = changes.auto_pull {
        fields.push(("autoPull", Value::Bool(value)));
    }
    if let Some(value) = &changes.favicon_path {
        fields.push(("faviconPath", string_option(value)));
    }
    if let Some(value) = &changes.project_icon {
        fields.push((
            "projectIcon",
            value
                .as_ref()
                .map_or(Value::Null, |icon| icon.payload.clone()),
        ));
    }
    if let Some(value) = &changes.scripts {
        fields.push(("scripts", script_values(value)));
    }
    fields
}

fn event_value(event: &Event<Value>) -> Value {
    let mut payload = vec![("projectId", Value::String(event.project_id.clone()))];
    let name = match &event.kind {
        EventKind::Created {
            title,
            workspace_root,
            scripts,
        } => {
            payload.extend([
                ("title", Value::String(title.clone())),
                ("workspaceRoot", Value::String(workspace_root.clone())),
                ("defaultModelSelection", Value::Null),
                ("faviconPath", Value::Null),
                ("projectIcon", Value::Null),
                ("scripts", script_values(scripts)),
                ("createdAt", Value::String(event.occurred_at.clone())),
                ("updatedAt", Value::String(event.occurred_at.clone())),
            ]);
            "project.created"
        }
        EventKind::Updated {
            changes,
            pinned_at,
            archived_at,
        } => {
            payload.extend(metadata_fields(changes));
            if let Some(value) = pinned_at {
                payload.push(("pinnedAt", string_option(value)));
            }
            if let Some(value) = archived_at {
                payload.push(("archivedAt", string_option(value)));
            }
            payload.push(("updatedAt", Value::String(event.occurred_at.clone())));
            "project.meta-updated"
        }
        EventKind::Deleted => {
            payload.push(("deletedAt", Value::String(event.occurred_at.clone())));
            "project.deleted"
        }
    };
    object(vec![
        ("eventId", Value::String(event.event_id.clone())),
        ("aggregateKind", Value::String("project".to_string())),
        ("aggregateId", Value::String(event.project_id.clone())),
        ("occurredAt", Value::String(event.occurred_at.clone())),
        ("commandId", Value::String(event.command_id.clone())),
        ("causationEventId", Value::Null),
        ("correlationId", Value::String(event.command_id.clone())),
        ("metadata", object(Vec::new())),
        ("type", Value::String(name.to_string())),
        ("payload", object(payload)),
    ])
}

fn rejection_value(rejection: Rejection) -> Value {
    match rejection {
        Rejection::Invariant {
            command_type,
            detail,
        } => object(vec![
            (
                "_tag",
                Value::String("ProjectCommandInvariantError".to_string()),
            ),
            ("commandType", Value::String(command_type.to_string())),
            ("detail", Value::String(detail)),
        ]),
        Rejection::MissingProject {
            command_type,
            project_id,
        } => object(vec![
            (
                "_tag",
                Value::String("ProjectCommandMissingProjectError".to_string()),
            ),
            ("commandType", Value::String(command_type.to_string())),
            ("projectId", Value::String(project_id)),
        ]),
        Rejection::WorkspaceConflict {
            workspace_root,
            conflicting_project_id,
        } => object(vec![
            (
                "_tag",
                Value::String("ProjectWorkspaceConflictError".to_string()),
            ),
            ("workspaceRoot", Value::String(workspace_root)),
            (
                "conflictingProjectId",
                Value::String(conflicting_project_id),
            ),
        ]),
    }
}

/// The executable and the independent batch oracle use the same validated request.
pub fn evaluate(input: &Value) -> WireResult<Value> {
    unique_keys(input)?;
    exact_fields(
        input,
        &[
            "version",
            "operation",
            "command",
            "state",
            "eventId",
            "now",
            "monogramGraphemeCount",
        ],
    )?;
    if field(input, "version")?.as_i64() != Some(1)
        || field(input, "operation")?.as_str() != Some("plan")
    {
        return Err(Error::InvalidRequest);
    }
    let state = field(input, "state")?;
    exact_fields(state, &["project", "workspaceOwner"])?;
    let owner = field(state, "workspaceOwner")?;
    let state = State {
        project: project(field(state, "project")?)?,
        workspace_owner_id: if *owner == Value::Null {
            None
        } else {
            Some(text(field(owner, "projectId")?)?)
        },
    };
    let command = command(input)?;
    let now = field(input, "now")?
        .as_i64()
        .and_then(Timestamp::from_epoch_millis)
        .ok_or(Error::InvalidRequest)?;
    let event_id = text(field(input, "eventId")?)?;
    let outcome = match project_commands::plan(&command, &state, &event_id, &now.format_iso()) {
        Ok(event) => ("ok", event_value(&event)),
        Err(rejection) => ("error", rejection_value(rejection)),
    };
    Ok(object(vec![
        ("version", Value::Number("1".to_string())),
        outcome,
    ]))
}

fn charge(remaining: &mut usize, amount: usize) -> WireResult<()> {
    *remaining = remaining.checked_sub(amount).ok_or(Error::OutputLimit)?;
    Ok(())
}

fn quoted_size(value: &str, remaining: &mut usize) -> WireResult<()> {
    charge(remaining, 2)?;
    for character in value.chars() {
        charge(
            remaining,
            match character {
                '"' | '\\' | '\n' | '\r' | '\t' | '\u{08}' | '\u{0c}' => 2,
                character if character < '\u{20}' => 6,
                character => character.len_utf8(),
            },
        )?;
    }
    Ok(())
}

fn encoded_size(
    value: &Value,
    depth: usize,
    remaining: &mut usize,
    remaining_values: &mut usize,
) -> WireResult<()> {
    charge(remaining_values, 1)?;
    match value {
        Value::Null => charge(remaining, 4),
        Value::Bool(value) => charge(remaining, if *value { 4 } else { 5 }),
        Value::Number(value) => charge(remaining, value.len()),
        Value::String(value) => quoted_size(value, remaining),
        Value::Array(values) => {
            if depth >= MAX_DEPTH {
                return Err(Error::OutputLimit);
            }
            charge(remaining, 2 + values.len().saturating_sub(1))?;
            for value in values {
                encoded_size(value, depth + 1, remaining, remaining_values)?;
            }
            Ok(())
        }
        Value::Object(fields) => {
            if depth >= MAX_DEPTH {
                return Err(Error::OutputLimit);
            }
            charge(remaining, 2 + fields.len().saturating_sub(1))?;
            for (key, value) in fields {
                quoted_size(key, remaining)?;
                charge(remaining, 1)?;
                encoded_size(value, depth + 1, remaining, remaining_values)?;
            }
            Ok(())
        }
    }
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
            max_depth: MAX_DEPTH,
            max_values: MAX_VALUES,
        },
    )
    .map_err(|_| Error::InvalidRequest)?;
    let output = evaluate(&input)?;
    let mut remaining = MAX_FRAME_BYTES - 1;
    let mut remaining_values = MAX_VALUES;
    // Validate the entire escaped response before writing anything to stdout.
    encoded_size(&output, 0, &mut remaining, &mut remaining_values)?;
    let mut response = json::stringify(&output);
    response.push('\n');
    Ok(response)
}
