//! JSON edits that preserve SQLite number/string lexemes and object order.

use crate::{Error, Result};
use serde_json::Value;

// Keep JSON number/string lexemes and object order when reproducing SQLite's
// json_remove/json_set. Parsing and reserializing the whole value would alter
// escaped strings, floating-point notation and therefore replay byte budgets.
#[derive(Clone)]
pub(crate) struct JsonField {
    pub(crate) name: String,
    encoded_name: String,
    value: String,
}

pub(crate) fn compact_json(text: &str) -> String {
    let mut output = String::with_capacity(text.len());
    let mut quoted = false;
    let mut escaped = false;
    for ch in text.chars() {
        if quoted {
            output.push(ch);
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '"' {
                quoted = false;
            }
        } else if ch == '"' {
            quoted = true;
            output.push(ch);
        } else if !matches!(ch, ' ' | '\r' | '\n' | '\t') {
            output.push(ch);
        }
    }
    output
}

pub(crate) fn object_fields(text: &str) -> Result<Option<Vec<JsonField>>> {
    let parsed: Value = serde_json::from_str(text).map_err(|_| Error::IncompatibleStore)?;
    if !parsed.is_object() {
        return Ok(None);
    }
    let source = compact_json(text);
    let bytes = source.as_bytes();
    let mut position = 1;
    let mut fields = Vec::new();
    while position < bytes.len() - 1 {
        let start = position;
        position += 1;
        let mut escaped = false;
        while position < bytes.len() {
            let byte = bytes[position];
            position += 1;
            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
            } else if byte == b'"' {
                break;
            }
        }
        let encoded_name = source[start..position].to_owned();
        let name = serde_json::from_str(&encoded_name).map_err(|_| Error::IncompatibleStore)?;
        if bytes.get(position) != Some(&b':') {
            return Err(Error::IncompatibleStore);
        }
        position += 1;
        let start = position;
        let mut depth = 0usize;
        let mut quoted = false;
        let mut escaped = false;
        while position < bytes.len() {
            let byte = bytes[position];
            if quoted {
                if escaped {
                    escaped = false;
                } else if byte == b'\\' {
                    escaped = true;
                } else if byte == b'"' {
                    quoted = false;
                }
            } else {
                match byte {
                    b'"' => quoted = true,
                    b'{' | b'[' => depth += 1,
                    b'}' if depth == 0 => break,
                    b'}' | b']' => depth -= 1,
                    b',' if depth == 0 => break,
                    _ => {}
                }
            }
            position += 1;
        }
        fields.push(JsonField {
            name,
            encoded_name,
            value: source[start..position].to_owned(),
        });
        if bytes.get(position) == Some(&b',') {
            position += 1;
        }
    }
    Ok(Some(fields))
}
pub(crate) fn field<'a>(fields: &'a [JsonField], name: &str) -> Option<&'a str> {
    fields
        .iter()
        .find(|field| field.name == name)
        .map(|field| field.value.as_str())
}
pub(crate) fn set_field(fields: &mut Vec<JsonField>, name: &str, value: String) {
    if let Some(found) = fields.iter_mut().find(|field| field.name == name) {
        found.value = value;
    } else {
        fields.push(JsonField {
            name: name.into(),
            encoded_name: format!("\"{name}\""),
            value,
        });
    }
}
pub(crate) fn render_fields(fields: &[JsonField]) -> String {
    format!(
        "{{{}}}",
        fields
            .iter()
            .map(|field| format!("{}:{}", field.encoded_name, field.value))
            .collect::<Vec<_>>()
            .join(",")
    )
}
