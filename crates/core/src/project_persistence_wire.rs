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
    if let Some(value) = row.get("hasCustomTitle") {
        value.as_bool().ok_or(Error::InvalidRequest)?;
    }
    field(row, "scripts")?
        .as_array()
        .ok_or(Error::InvalidRequest)?;
    Ok(())
}

/// Legacy rows may lack the custom-title flag until their event history is read.
/// Present flags must be Boolean; projection updates require a resolved flag.
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
            "hasCustomTitle",
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

/// Reduces an event without running the shared application's row codecs.
/// Creation callers may pass null and leave sticky-title handling to the row upsert.
pub fn reduce_project_event(existing: &Value, event: &Value) -> WireResult<Value> {
    let payload = field(event, "payload")?;
    let project_id = field(payload, "projectId")?;
    project_id.as_str().ok_or(Error::InvalidRequest)?;
    match field(event, "type")?.as_str() {
        Some("project.created") => {
            let has_custom_title = if existing == &Value::Null {
                false
            } else {
                validate_row(existing, project_id)?;
                field(existing, "hasCustomTitle")?
                    .as_bool()
                    .ok_or(Error::InvalidRequest)?
            };
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
            if has_custom_title {
                set(&mut row, "title", field(existing, "title")?.clone())?;
            }
            set(&mut row, "hasCustomTitle", Value::Bool(has_custom_title))?;
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
            Ok(row)
        }
        Some("project.meta-updated") | Some("project.deleted") => {
            if existing == &Value::Null {
                return Ok(Value::Null);
            }
            validate_row(existing, project_id)?;
            field(existing, "hasCustomTitle")?
                .as_bool()
                .ok_or(Error::InvalidRequest)?;
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
                if payload.get("title").is_some() {
                    set(&mut row, "hasCustomTitle", Value::Bool(true))?;
                }
                set(&mut row, "updatedAt", field(payload, "updatedAt")?.clone())?;
            }
            validate_row(&row, project_id)?;
            Ok(row)
        }
        _ => Err(Error::InvalidRequest),
    }
}

/// Applies the same row changes as ProjectStoreV2.apply to a validated planner event.
/// The standalone planner keeps its existing native normalization boundary.
pub fn project_event(existing: &Value, event: &Value) -> WireResult<Value> {
    let row = reduce_project_event(existing, event)?;
    if row == Value::Null {
        Ok(row)
    } else {
        normalize_row(row)
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

#[cfg(test)]
mod tests {
    use super::*;

    fn event(kind: &str, payload: &str) -> Value {
        Value::Object(vec![
            ("type".into(), Value::String(kind.into())),
            ("payload".into(), json::parse(payload).unwrap()),
        ])
    }

    fn created() -> Value {
        event(
            "project.created",
            r#"{
                "projectId":"project:one","title":"Generated title",
                "workspaceRoot":"/fixture/project","defaultModelSelection":null,
                "scripts":[],"createdAt":"2026-11-08T12:00:00.000Z",
                "updatedAt":"2026-11-08T12:00:00.000Z"
            }"#,
        )
    }

    #[test]
    fn title_edits_are_sticky_even_when_the_text_does_not_change() {
        let mut row = project_event(&Value::Null, &created()).unwrap();
        assert_eq!(row.get("hasCustomTitle"), Some(&Value::Bool(false)));
        let metadata = event(
            "project.meta-updated",
            r#"{"projectId":"project:one","autoPull":true,
                "updatedAt":"2026-11-08T12:01:00.000Z"}"#,
        );
        row = project_event(&row, &metadata).unwrap();
        assert_eq!(row.get("hasCustomTitle"), Some(&Value::Bool(false)));
        row = project_event(
            &row,
            &event(
                "project.meta-updated",
                r#"{"projectId":"project:one","title":"Generated title",
                    "updatedAt":"2026-11-08T12:02:00.000Z"}"#,
            ),
        )
        .unwrap();
        assert_eq!(row.get("hasCustomTitle"), Some(&Value::Bool(true)));
        row = project_event(&row, &metadata).unwrap();
        assert_eq!(row.get("hasCustomTitle"), Some(&Value::Bool(true)));
        row = project_event(
            &row,
            &event(
                "project.deleted",
                r#"{"projectId":"project:one","deletedAt":"2026-11-08T12:03:00.000Z"}"#,
            ),
        )
        .unwrap();
        assert_eq!(row.get("hasCustomTitle"), Some(&Value::Bool(true)));
        assert_eq!(
            row.get("title").and_then(Value::as_str),
            Some("Generated title")
        );
    }

    #[test]
    fn recreated_rows_preserve_only_a_custom_title_and_reset_other_defaults() {
        for has_custom_title in [false, true] {
            let mut row = project_event(&Value::Null, &created()).unwrap();
            set(&mut row, "title", Value::String("Old title".into())).unwrap();
            set(&mut row, "hasCustomTitle", Value::Bool(has_custom_title)).unwrap();
            set(&mut row, "autoPull", Value::Bool(true)).unwrap();
            set(
                &mut row,
                "deletedAt",
                Value::String("2026-11-08T12:01:00.000Z".into()),
            )
            .unwrap();
            let actual = project_event(&row, &created()).unwrap();
            let mut expected = project_event(&Value::Null, &created()).unwrap();
            if has_custom_title {
                set(&mut expected, "title", Value::String("Old title".into())).unwrap();
                set(&mut expected, "hasCustomTitle", Value::Bool(true)).unwrap();
            }
            assert_eq!(actual, expected);
        }
    }

    #[test]
    fn application_reduction_leaves_codec_normalization_to_the_existing_boundary() {
        let mut input = created();
        let Value::Object(entries) = &mut input else {
            unreachable!()
        };
        let payload = &mut entries
            .iter_mut()
            .find(|entry| entry.0 == "payload")
            .unwrap()
            .1;
        set(payload, "projectId", Value::String(" project:one ".into())).unwrap();
        set(
            payload,
            "projectIcon",
            json::parse(r#"{"kind":"lucide","name":"folder","color":"blue","monogramText":"AB"}"#)
                .unwrap(),
        )
        .unwrap();
        set(payload, "scripts", json::parse(
            r#"[{"id":" test ","name":" Test ","command":" echo test ","icon":"test","runOnWorktreeCreate":false,"extra":true}]"#,
        ).unwrap()).unwrap();
        let raw = reduce_project_event(&Value::Null, &input).unwrap();
        assert_eq!(
            raw.get("projectId").and_then(Value::as_str),
            Some(" project:one ")
        );
        assert_eq!(
            raw.get("projectIcon")
                .unwrap()
                .get("kind")
                .and_then(Value::as_str),
            Some("lucide")
        );
        let script = &raw.get("scripts").unwrap().as_array().unwrap()[0];
        assert_eq!(script.get("id").and_then(Value::as_str), Some(" test "));
        assert_eq!(script.get("extra"), Some(&Value::Bool(true)));
        let standalone = project_event(&Value::Null, &input).unwrap();
        assert_eq!(
            standalone.get("projectId").and_then(Value::as_str),
            Some("project:one")
        );
        assert_eq!(
            standalone
                .get("projectIcon")
                .unwrap()
                .get("kind")
                .and_then(Value::as_str),
            Some("monogram")
        );
        assert_eq!(
            standalone.get("scripts").unwrap().as_array().unwrap()[0].get("extra"),
            None
        );
        assert_eq!(
            project_event(
                &Value::Null,
                &event("project.deleted", r#"{"projectId":"missing"}"#)
            ),
            Ok(Value::Null)
        );
    }

    #[test]
    fn legacy_rows_require_history_resolution_before_projection_updates() {
        let mut row = project_event(&Value::Null, &created()).unwrap();
        let Value::Object(entries) = &mut row else {
            unreachable!()
        };
        entries.retain(|(key, _)| key != "hasCustomTitle");
        assert_eq!(validate_projection_shape(&row), Ok(()));
        let update = event(
            "project.meta-updated",
            r#"{"projectId":"project:one","updatedAt":"2026-11-08T12:01:00.000Z"}"#,
        );
        assert_eq!(project_event(&row, &update), Err(Error::InvalidRequest));
        assert_eq!(project_event(&row, &created()), Err(Error::InvalidRequest));
        for invalid in [
            Value::Null,
            Value::Number("1".into()),
            Value::String("true".into()),
        ] {
            set(&mut row, "hasCustomTitle", invalid).unwrap();
            assert_eq!(validate_projection_shape(&row), Err(Error::InvalidRequest));
            assert_eq!(project_event(&row, &update), Err(Error::InvalidRequest));
        }
    }
}
