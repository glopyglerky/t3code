#![forbid(unsafe_code)]

pub mod auth;
pub mod http;
#[cfg(unix)]
pub mod provider;
#[cfg(unix)]
pub mod runtime;
pub mod service;

use serde_json::Value;

pub const MAX_RPC_BYTES: usize = 8 * 1024 * 1024;

/// Decode bounded JSON without letting duplicate keys select another command or credential.
pub fn decode_json(bytes: &[u8]) -> Result<Value, &'static str> {
    use t3_code_ls_core::json::{self, Value as CoreValue};
    fn unique(value: &CoreValue) -> bool {
        match value {
            CoreValue::Object(fields) => {
                let mut keys = std::collections::HashSet::new();
                fields
                    .iter()
                    .all(|(key, value)| keys.insert(key) && unique(value))
            }
            CoreValue::Array(values) => values.iter().all(unique),
            _ => true,
        }
    }
    let text = std::str::from_utf8(bytes).map_err(|_| "request is not UTF-8")?;
    let value = json::parse_with_limits(
        text,
        json::Limits {
            max_bytes: MAX_RPC_BYTES,
            max_depth: 64,
            max_values: 262_144,
        },
    )
    .map_err(|_| "request exceeds JSON limits or is malformed")?;
    if !unique(&value) {
        return Err("duplicate JSON object keys are not accepted");
    }
    serde_json::from_str(text).map_err(|_| "request is not supported JSON")
}

pub fn new_id() -> Result<String, getrandom::Error> {
    let mut bytes = [0u8; 16];
    getrandom::fill(&mut bytes)?;
    Ok(bytes.iter().map(|byte| format!("{byte:02x}")).collect())
}
