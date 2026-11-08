use std::sync::LazyLock;

use regex::Regex;
use serde_json::{Map, Value};
use unicode_segmentation::UnicodeSegmentation;

use crate::Error;

const CREATE_FIELDS: &[&str] = &[
    "type",
    "commandId",
    "projectId",
    "title",
    "workspaceRoot",
    "scripts",
];
const UPDATE_FIELDS: &[&str] = &[
    "type",
    "commandId",
    "projectId",
    "title",
    "workspaceRoot",
    "defaultModelSelection",
    "defaultThreadEnvMode",
    "autoPull",
    "faviconPath",
    "projectIcon",
    "scripts",
    "pinned",
    "archived",
];
const SCRIPT_FIELDS: &[&str] = &[
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
];
const ICON_COLORS: &[&str] = &[
    "gray", "red", "orange", "amber", "yellow", "lime", "green", "emerald", "teal", "cyan", "sky",
    "blue", "indigo", "violet", "purple", "fuchsia", "pink", "rose",
];

static MONOGRAM_PATTERN: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"\A[\p{L}\p{N}][\p{L}\p{N}\p{M}\u{200c}\u{200d}]*\z")
        .expect("the project monogram pattern is valid")
});

pub fn validate_command(command: &Value) -> Result<Value, Error> {
    let source = object(command, "command")?;
    let kind = required(source, "type", "command")?
        .as_str()
        .ok_or_else(|| invalid("command.type", "expected a command type string"))?;
    let create = match kind {
        "project.create" => {
            only_fields(source, CREATE_FIELDS, "command")?;
            true
        }
        "project.meta.update" => {
            only_fields(source, UPDATE_FIELDS, "command")?;
            false
        }
        "project.delete" => {
            return Err(invalid(
                "command.type",
                "project deletion is not supported by the native project service",
            ));
        }
        _ => return Err(invalid("command.type", "unsupported project command type")),
    };
    let mut normalized = source.clone();
    for field in ["commandId", "projectId"] {
        normalize_string_field(&mut normalized, field, "command", true, false)?;
    }
    for field in ["title", "workspaceRoot"] {
        normalize_string_field(&mut normalized, field, "command", create, false)?;
    }
    if let Some(scripts) = source.get("scripts") {
        let scripts = scripts
            .as_array()
            .ok_or_else(|| invalid("command.scripts", "expected an array"))?;
        let scripts = scripts
            .iter()
            .enumerate()
            .map(|(index, script)| validate_script(script, &format!("command.scripts[{index}]")))
            .collect::<Result<Vec<_>, _>>()?;
        normalized.insert("scripts".into(), Value::Array(scripts));
    }
    if !create {
        for field in ["autoPull", "pinned", "archived"] {
            validate_bool_field(source, field, "command", false)?;
        }
        normalize_string_field(&mut normalized, "faviconPath", "command", false, true)?;
        if let Some(mode) = source.get("defaultThreadEnvMode")
            && !mode.is_null()
        {
            literal(mode, &["local", "worktree"], "command.defaultThreadEnvMode")?;
        }
        if let Some(selection) = source.get("defaultModelSelection")
            && !selection.is_null()
        {
            normalized.insert(
                "defaultModelSelection".into(),
                validate_model_selection(selection)?,
            );
        }
        if let Some(icon) = source.get("projectIcon")
            && !icon.is_null()
        {
            normalized.insert("projectIcon".into(), validate_icon(icon)?);
        }
    }
    Ok(Value::Object(normalized))
}

pub fn monogram_graphemes(command: &Value) -> Option<u64> {
    if command.get("type")?.as_str()? != "project.meta.update" {
        return None;
    }
    let icon = command.get("projectIcon")?;
    if icon.get("kind")?.as_str()? != "monogram" {
        return None;
    }
    let text = icon.get("text")?.as_str()?;
    Some(text.graphemes(true).count() as u64)
}

fn validate_script(script: &Value, path: &str) -> Result<Value, Error> {
    let source = object(script, path)?;
    only_fields(source, SCRIPT_FIELDS, path)?;
    let mut normalized = source.clone();
    // Existing script IDs can predate the planner's current ID syntax.
    for field in ["id", "name", "command"] {
        normalize_string_field(&mut normalized, field, path, true, false)?;
    }
    literal(
        required(source, "icon", path)?,
        &["play", "test", "lint", "configure", "build", "debug"],
        &format!("{path}.icon"),
    )?;
    validate_bool_field(source, "runOnWorktreeCreate", path, true)?;
    for field in ["runOnSettle", "stopOnSettle", "async", "autoOpenPreview"] {
        validate_bool_field(source, field, path, false)?;
    }
    normalize_string_field(&mut normalized, "previewUrl", path, false, false)?;
    Ok(Value::Object(normalized))
}

fn validate_model_selection(selection: &Value) -> Result<Value, Error> {
    let path = "command.defaultModelSelection";
    let source = object(selection, path)?;
    only_fields(source, &["instanceId", "model", "options"], path)?;
    let mut normalized = source.clone();
    for field in ["instanceId", "model"] {
        normalize_string_field(&mut normalized, field, path, true, false)?;
    }
    let instance_id = normalized["instanceId"]
        .as_str()
        .expect("instanceId was normalized to a string");
    let mut chars = instance_id.chars();
    if instance_id.len() > 64
        || !chars.next().is_some_and(|c| c.is_ascii_alphabetic())
        || !chars.all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
    {
        return Err(invalid(
            "command.defaultModelSelection.instanceId",
            "expected a provider slug of at most 64 ASCII characters",
        ));
    }
    if let Some(options) = source.get("options") {
        let options = options.as_array().ok_or_else(|| {
            invalid(
                "command.defaultModelSelection.options",
                "expected a canonical options array",
            )
        })?;
        let options = options
            .iter()
            .enumerate()
            .map(|(index, option)| {
                let path = format!("command.defaultModelSelection.options[{index}]");
                let source = object(option, &path)?;
                only_fields(source, &["id", "value"], &path)?;
                let mut normalized = source.clone();
                normalize_string_field(&mut normalized, "id", &path, true, false)?;
                let value = required(source, "value", &path)?;
                if !value.is_boolean() {
                    normalized.insert(
                        "value".into(),
                        nonempty_string(value, &format!("{path}.value"))?,
                    );
                }
                Ok(Value::Object(normalized))
            })
            .collect::<Result<Vec<_>, Error>>()?;
        normalized.insert("options".into(), Value::Array(options));
    }
    Ok(Value::Object(normalized))
}

fn validate_icon(icon: &Value) -> Result<Value, Error> {
    let path = "command.projectIcon";
    let source = object(icon, path)?;
    let kind = literal(
        required(source, "kind", path)?,
        &["lucide", "emoji", "monogram"],
        "command.projectIcon.kind",
    )?;
    let (fields, text_field, max_length): (&[&str], &str, usize) = match kind {
        "lucide" => (&["kind", "name", "color"], "name", 64),
        "emoji" => (&["kind", "emoji"], "emoji", 32),
        "monogram" => (&["kind", "text", "color"], "text", 32),
        _ => unreachable!("the icon kind was validated"),
    };
    only_fields(source, fields, path)?;
    let mut normalized = source.clone();
    normalize_string_field(&mut normalized, text_field, path, true, false)?;
    let text = normalized[text_field]
        .as_str()
        .expect("the icon text was normalized to a string");
    let text_path = format!("{path}.{text_field}");
    if text.encode_utf16().count() > max_length {
        return Err(invalid(&text_path, "exceeds the UTF-16 length limit"));
    }
    match kind {
        "lucide" => {
            if !text.split('-').all(|part| {
                !part.is_empty()
                    && part
                        .bytes()
                        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit())
            }) {
                return Err(invalid(&text_path, "expected a lowercase icon name"));
            }
        }
        "monogram" if !MONOGRAM_PATTERN.is_match(text) => {
            return Err(invalid(
                &text_path,
                "expected a letter or number followed by letters, numbers, marks or joiners",
            ));
        }
        _ => {}
    }
    if kind != "emoji" {
        literal(
            required(source, "color", path)?,
            ICON_COLORS,
            "command.projectIcon.color",
        )?;
    }
    Ok(Value::Object(normalized))
}

fn normalize_string_field(
    value: &mut Map<String, Value>,
    field: &str,
    path: &str,
    is_required: bool,
    nullable: bool,
) -> Result<(), Error> {
    match value.get_mut(field) {
        Some(current) if nullable && current.is_null() => Ok(()),
        Some(current) => {
            *current = nonempty_string(current, &format!("{path}.{field}"))?;
            Ok(())
        }
        None if is_required => Err(invalid(
            &format!("{path}.{field}"),
            "missing required field",
        )),
        None => Ok(()),
    }
}

fn nonempty_string(value: &Value, path: &str) -> Result<Value, Error> {
    let text = value
        .as_str()
        .ok_or_else(|| invalid(path, "expected a nonblank string"))?
        .trim_matches(is_ecmascript_whitespace);
    if text.is_empty() {
        return Err(invalid(path, "expected a nonblank string"));
    }
    Ok(Value::String(text.to_owned()))
}

// ECMAScript String.trim includes BOM and excludes Unicode NEL, unlike Rust str::trim.
fn is_ecmascript_whitespace(c: char) -> bool {
    matches!(
        c,
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

fn object<'a>(value: &'a Value, path: &str) -> Result<&'a Map<String, Value>, Error> {
    value
        .as_object()
        .ok_or_else(|| invalid(path, "expected an object"))
}

fn required<'a>(
    value: &'a Map<String, Value>,
    field: &str,
    path: &str,
) -> Result<&'a Value, Error> {
    value
        .get(field)
        .ok_or_else(|| invalid(&format!("{path}.{field}"), "missing required field"))
}

fn only_fields(value: &Map<String, Value>, fields: &[&str], path: &str) -> Result<(), Error> {
    if value.keys().any(|field| !fields.contains(&field.as_str())) {
        return Err(invalid(path, "contains an unknown field"));
    }
    Ok(())
}

fn validate_bool_field(
    value: &Map<String, Value>,
    field: &str,
    path: &str,
    is_required: bool,
) -> Result<(), Error> {
    match value.get(field) {
        Some(value) if value.is_boolean() => Ok(()),
        Some(_) => Err(invalid(&format!("{path}.{field}"), "expected a boolean")),
        None if is_required => Err(invalid(
            &format!("{path}.{field}"),
            "missing required field",
        )),
        None => Ok(()),
    }
}

fn literal<'a>(value: &'a Value, choices: &[&str], path: &str) -> Result<&'a str, Error> {
    match value.as_str() {
        Some(value) if choices.contains(&value) => Ok(value),
        _ => Err(invalid(path, "expected one of the supported values")),
    }
}

fn invalid(path: &str, reason: &str) -> Error {
    Error::Invalid(format!("invalid project command at {path}: {reason}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn create() -> Value {
        json!({
            "type": "project.create",
            "commandId": "command-1",
            "projectId": "project-1",
            "title": "Project",
            "workspaceRoot": "/workspace/project"
        })
    }

    fn update() -> Value {
        json!({
            "type": "project.meta.update",
            "commandId": "command-2",
            "projectId": "project-1"
        })
    }

    fn script() -> Value {
        json!({
            "id": "legacy.ID / 你好",
            "name": "Run",
            "command": "npm run dev",
            "icon": "play",
            "runOnWorktreeCreate": false
        })
    }

    #[test]
    fn trims_ecmascript_whitespace_without_trimming_nel_or_zero_width_characters() {
        let whitespace = "\t\n\u{b}\u{c}\r \u{a0}\u{1680}\u{2000}\u{2001}\u{2002}\u{2003}\u{2004}\u{2005}\u{2006}\u{2007}\u{2008}\u{2009}\u{200a}\u{2028}\u{2029}\u{202f}\u{205f}\u{3000}\u{feff}";
        let mut command = create();
        command["commandId"] = json!(format!("{whitespace}command-1{whitespace}"));
        command["projectId"] = json!(format!("{whitespace}project-1{whitespace}"));
        command["title"] = json!(format!("{whitespace}Title{whitespace}"));
        command["workspaceRoot"] = json!(format!("{whitespace}/work{whitespace}"));
        let normalized = validate_command(&command).unwrap();
        assert_eq!(normalized["commandId"], "command-1");
        assert_eq!(normalized["projectId"], "project-1");
        assert_eq!(normalized["title"], "Title");
        assert_eq!(normalized["workspaceRoot"], "/work");
        for kept in ["\u{85}", "\u{180e}", "\u{200b}"] {
            command["title"] = json!(kept);
            assert_eq!(validate_command(&command).unwrap()["title"], kept);
        }
        command["title"] = json!(whitespace);
        assert!(validate_command(&command).is_err());
    }

    #[test]
    fn rejects_bad_ids_missing_fields_unknown_fields_and_unsupported_commands() {
        for field in ["commandId", "projectId", "title", "workspaceRoot"] {
            for value in [Value::Null, json!(false), json!(4), json!(" \u{feff} ")] {
                let mut command = create();
                command[field] = value;
                assert!(validate_command(&command).is_err(), "{field}");
            }
            let mut command = create();
            command.as_object_mut().unwrap().remove(field);
            assert!(validate_command(&command).is_err(), "{field}");
        }
        for field in [
            "force",
            "createWorkspaceRootIfMissing",
            "defaultModelSelection",
            "unexpected",
        ] {
            let mut command = create();
            command[field] = Value::Null;
            assert!(validate_command(&command).is_err(), "{field}");
        }
        for value in [Value::Null, json!([]), json!("project.create")] {
            assert!(validate_command(&value).is_err());
        }
        for kind in [
            "project.update",
            "project.delete",
            "project.create ",
            "thread.create",
        ] {
            let mut command = update();
            command["type"] = json!(kind);
            assert!(validate_command(&command).is_err(), "{kind}");
        }
    }

    #[test]
    fn keeps_omission_distinct_from_null_and_does_not_supply_defaults() {
        assert_eq!(validate_command(&create()).unwrap(), create());
        assert_eq!(validate_command(&update()).unwrap(), update());
        let mut command = update();
        for field in [
            "defaultModelSelection",
            "defaultThreadEnvMode",
            "faviconPath",
            "projectIcon",
        ] {
            command[field] = Value::Null;
        }
        for field in ["autoPull", "pinned", "archived"] {
            command[field] = json!(false);
        }
        assert_eq!(validate_command(&command).unwrap(), command);
        for field in [
            "title",
            "workspaceRoot",
            "scripts",
            "autoPull",
            "pinned",
            "archived",
        ] {
            let mut command = update();
            command[field] = Value::Null;
            assert!(validate_command(&command).is_err(), "{field}");
        }
    }

    #[test]
    fn accepts_legacy_script_ids_and_preserves_optional_fields() {
        let mut command = create();
        command["scripts"] = json!([script()]);
        assert_eq!(validate_command(&command).unwrap(), command);
        command["type"] = json!("project.meta.update");
        assert_eq!(validate_command(&command).unwrap(), command);
        command["scripts"][0]["id"] = json!(" \u{feff}legacy.ID / 你好 ");
        command["scripts"][0]["previewUrl"] = json!("  http://localhost:3000  ");
        for field in ["runOnSettle", "stopOnSettle", "async", "autoOpenPreview"] {
            command["scripts"][0][field] = json!(false);
        }
        let normalized = validate_command(&command).unwrap();
        assert_eq!(normalized["scripts"][0]["id"], "legacy.ID / 你好");
        assert_eq!(
            normalized["scripts"][0]["previewUrl"],
            "http://localhost:3000"
        );
        for field in ["runOnSettle", "stopOnSettle", "async", "autoOpenPreview"] {
            assert_eq!(normalized["scripts"][0][field], false);
        }
        for field in [
            "runOnWorktreeCreate",
            "runOnSettle",
            "stopOnSettle",
            "async",
            "autoOpenPreview",
            "previewUrl",
        ] {
            let mut command = create();
            command["scripts"] = json!([script()]);
            command["scripts"][0][field] = Value::Null;
            assert!(validate_command(&command).is_err(), "{field}");
        }
        for icon in ["PLAY", "unknown", " play "] {
            command["scripts"][0]["icon"] = json!(icon);
            assert!(validate_command(&command).is_err());
        }
    }

    #[test]
    fn canonical_model_options_preserve_order_duplicates_and_boolean_values() {
        let mut command = update();
        command["defaultModelSelection"] = json!({
            "instanceId": " \u{feff}Codex_2-remote ",
            "model": " gpt-6-astra ",
            "options": [
                {"id": " reasoning ", "value": " high "},
                {"id": "enabled", "value": false},
                {"id": "reasoning", "value": "low"}
            ]
        });
        let normalized = validate_command(&command).unwrap();
        assert_eq!(
            normalized["defaultModelSelection"],
            json!({
                "instanceId": "Codex_2-remote",
                "model": "gpt-6-astra",
                "options": [
                    {"id": "reasoning", "value": "high"},
                    {"id": "enabled", "value": false},
                    {"id": "reasoning", "value": "low"}
                ]
            })
        );
        for value in [
            Value::Null,
            json!({"reasoning":"high"}),
            json!([{"id":"x","value":1}]),
            json!([{"id":"x","value":null}]),
            json!([{"id":"x","value":" \u{feff}"}]),
            json!([{"id":" ","value":true}]),
        ] {
            command["defaultModelSelection"]["options"] = value;
            assert!(validate_command(&command).is_err());
        }
        command["defaultModelSelection"] = json!({"provider":"codex", "model":"gpt-6-astra"});
        assert!(validate_command(&command).is_err());
    }

    #[test]
    fn provider_slug_uses_ascii_identifier_rules_and_a_64_character_limit() {
        for id in ["codex", "A_9-x", &"x".repeat(64)] {
            let mut command = update();
            command["defaultModelSelection"] = json!({"instanceId":id,"model":"model"});
            assert!(validate_command(&command).is_ok(), "{id}");
        }
        for id in [
            "9codex",
            "_codex",
            "co.dex",
            "codex/remote",
            "códex",
            &"x".repeat(65),
        ] {
            let mut command = update();
            command["defaultModelSelection"] = json!({"instanceId":id,"model":"model"});
            assert!(validate_command(&command).is_err(), "{id}");
        }
    }

    #[test]
    fn monograms_use_unicode_categories_and_extended_grapheme_clusters() {
        for (text, count) in [
            ("e\u{301}", 1),
            ("क्\u{200d}ष", 1),
            ("A\u{200c}B", 2),
            ("中９", 2),
            ("Ⅷ", 1),
            ("ABC", 3),
        ] {
            let mut command = update();
            command["projectIcon"] = json!({"kind":"monogram","text":text,"color":"blue"});
            let normalized = validate_command(&command).unwrap();
            assert_eq!(monogram_graphemes(&normalized), Some(count), "{text}");
        }
        for text in [
            "\u{301}A",
            "\u{200d}A",
            "A B",
            "A-",
            "A🙂",
            "A\nB",
            "A\u{200b}",
        ] {
            let mut command = update();
            command["projectIcon"] = json!({"kind":"monogram","text":text,"color":"blue"});
            assert!(validate_command(&command).is_err(), "{text}");
        }
        assert_eq!(monogram_graphemes(&update()), None);
    }

    #[test]
    fn icon_length_limits_count_utf16_units_and_validate_variant_fields() {
        for kind in ["emoji", "monogram"] {
            let field = if kind == "emoji" { "emoji" } else { "text" };
            let mut icon = json!({"kind":kind});
            if kind == "monogram" {
                icon["color"] = json!("rose");
            }
            let mut command = update();
            icon[field] = json!("𐐀".repeat(16));
            command["projectIcon"] = icon;
            assert!(validate_command(&command).is_ok());
            command["projectIcon"][field] = json!("𐐀".repeat(17));
            assert!(validate_command(&command).is_err());
        }
        let mut command = update();
        command["projectIcon"] =
            json!({"kind":"lucide", "name":" book-open-2 ", "color":"emerald"});
        assert_eq!(
            validate_command(&command).unwrap()["projectIcon"]["name"],
            "book-open-2"
        );
        for name in [
            "-book",
            "book-",
            "book--open",
            "Book",
            "book_open",
            &"x".repeat(65),
        ] {
            command["projectIcon"]["name"] = json!(name);
            assert!(validate_command(&command).is_err(), "{name}");
        }
        for icon in [
            json!({"kind":"monogram","text":"A","color":"grey"}),
            json!({"kind":"monogram","text":"A"}),
            json!({"kind":"emoji","emoji":"🙂","color":"blue"}),
            json!({"kind":"lucide","name":"book","color":"blue","monogram":"A"}),
            json!({"kind":"unknown"}),
        ] {
            command["projectIcon"] = icon;
            assert!(validate_command(&command).is_err());
        }
    }
}
