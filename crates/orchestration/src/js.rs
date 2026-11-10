//! JavaScript compatibility used by the direct ports, not application policy.

use serde_json::Value;

pub use t3_code_ls_core::codex_generation::js_trim as trim;

pub fn utf16_len(value: &str) -> usize {
    value.encode_utf16().count()
}

/// None means JS would create an unpaired surrogate, which Rust strings cannot
/// represent. Callers report this unsupported boundary instead of altering text.
pub fn prefix_utf16(value: &str, units: usize) -> Option<&str> {
    let mut used = 0;
    for (index, character) in value.char_indices() {
        if used == units { return Some(&value[..index]); }
        used += character.len_utf16();
        if used > units { return None; }
    }
    Some(value)
}

fn array_index(key: &str) -> Option<u32> {
    let index: u32 = key.parse().ok()?;
    (index != u32::MAX && index.to_string() == key).then_some(index)
}

/// JSON.stringify for the supported JSON/scalar-string domain: ECMAScript number
/// formatting and integer-index property ordering, followed by insertion order.
pub fn stringify(value: &Value) -> String {
    match value {
        Value::Null => "null".into(),
        Value::Bool(value) => value.to_string(),
        Value::Number(value) => value.as_f64()
            .map(|value| ryu_js::Buffer::new().format(value).to_owned())
            .unwrap_or_else(|| "null".into()),
        Value::String(value) => serde_json::to_string(value).expect("scalar string serializes"),
        Value::Array(values) => format!("[{}]", values.iter().map(stringify).collect::<Vec<_>>().join(",")),
        Value::Object(values) => {
            let mut indexed: Vec<_> = values.keys().filter_map(|key| array_index(key).map(|index| (index, key))).collect();
            indexed.sort_by_key(|(index, _)| *index);
            let keys = indexed.into_iter().map(|(_, key)| key)
                .chain(values.keys().filter(|key| array_index(key).is_none()));
            let fields: Vec<_> = keys.map(|key| format!("{}:{}", serde_json::to_string(key).expect("key serializes"), stringify(&values[key]))).collect();
            format!("{{{}}}", fields.join(","))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn js_order_and_numbers_are_not_rust_map_order_or_display() {
        let value: Value = serde_json::from_str(r#"{"z":1e20,"2":2,"10":10,"a":1e-7,"01":-0.0}"#).unwrap();
        assert_eq!(stringify(&value), r#"{"2":2,"10":10,"z":100000000000000000000,"a":1e-7,"01":0}"#);
    }

    #[test]
    fn utf16_boundary_is_explicit() {
        assert_eq!(prefix_utf16("a😀b", 3), Some("a😀"));
        assert_eq!(prefix_utf16("a😀b", 2), None);
        assert_eq!(utf16_len("a😀b"), 4);
    }
}
