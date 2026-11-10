//! Direct port of `packages/shared/src/toolOutput.ts` at upstream t3code
//! `521ac7c218b3faa74281fb72fc36ce74a0951fb3`.
//!
//! The imported reference readers below follow the same pinned `htmlRender.ts`,
//! `mcpApp.ts`, `t3McpToolPresentation.ts` and contracts `chatAttachment.ts`.
//! Inputs are JSON values; `None` represents JavaScript `undefined`, not `null`.
//! Rust cannot represent lone UTF-16 surrogates or non-finite JSON numbers. Those
//! cases, and JSON text beyond the parser's safe nesting limit, fail explicitly.

use std::borrow::Cow;
use std::fmt;
use std::sync::OnceLock;

use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

use crate::js;

const MAX_PARSED_BYTES: usize = 16_384;
const MAX_METADATA_BYTES: usize = 8_192;
const MAX_ID_LENGTH: usize = 256;
const MAX_THREADS: usize = 100;
const MAX_CONTENT_BLOCKS: usize = 32;
const MAX_ENVELOPE_DEPTH: usize = 4;
const MAX_ENVELOPE_NODES: usize = 128;
const MCP_APP_OUTPUT_KEY: &str = "t3McpApp";
const JS_SPACE: &str = r"[\x09-\x0d\x20\u{00a0}\u{1680}\u{2000}-\u{200a}\u{2028}\u{2029}\u{202f}\u{205f}\u{3000}\u{feff}]";

pub const MAX_TOOL_OUTPUT_IMAGES: usize = 8;
pub const MAX_TOOL_OUTPUT_IMAGE_BASE64_LENGTH: usize = (10 * 1024 * 1024_usize).div_ceil(3) * 4;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ToolOutputError {
    UnpairedUtf16Boundary,
    JsonParserDepthBoundary,
    NonFiniteJsonNumber,
}

impl fmt::Display for ToolOutputError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::UnpairedUtf16Boundary => "upstream output contains an unpaired UTF-16 surrogate; Rust JSON cannot represent it",
            Self::JsonParserDepthBoundary => "upstream JSON text exceeds the Rust parser's safe nesting limit",
            Self::NonFiniteJsonNumber => "upstream JSON text contains a non-finite JavaScript number",
        })
    }
}

impl std::error::Error for ToolOutputError {}

type Result<T> = std::result::Result<T, ToolOutputError>;

struct ResultReadBudget {
    remaining_bytes: usize,
    remaining_nodes: usize,
    exceeded: bool,
}

impl Default for ResultReadBudget {
    fn default() -> Self {
        Self { remaining_bytes: MAX_PARSED_BYTES, remaining_nodes: MAX_ENVELOPE_NODES, exceeded: false }
    }
}

#[derive(Default)]
struct ResultEnvelope {
    data: Option<Value>,
    failed: bool,
}

fn is_object(value: &Value) -> bool {
    value.is_object() || value.is_array()
}

fn nullish(value: Option<&Value>) -> Option<&Value> {
    value.filter(|value| !value.is_null())
}

// Used only to distinguish JSON.parse's valid lone-surrogate strings from malformed
// JSON. The replacement is parsed for validation, never returned to the caller.
fn replace_unpaired_escapes(text: &str) -> Option<String> {
    fn hex_unit(bytes: &[u8], start: usize) -> Option<u16> {
        if bytes.get(start..start + 2)? != b"\\u" { return None; }
        std::str::from_utf8(bytes.get(start + 2..start + 6)?).ok()
            .and_then(|digits| u16::from_str_radix(digits, 16).ok())
    }
    let bytes = text.as_bytes();
    let mut result = bytes.to_vec();
    let mut string = false;
    let mut changed = false;
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'"' {
            string = !string;
        } else if string && bytes[index] == b'\\' {
            if let Some(unit) = hex_unit(bytes, index) {
                if (0xd800..=0xdbff).contains(&unit)
                    && hex_unit(bytes, index + 6).is_some_and(|unit| (0xdc00..=0xdfff).contains(&unit))
                {
                    index += 12;
                    continue;
                }
                if (0xd800..=0xdfff).contains(&unit) {
                    result[index + 2..index + 6].copy_from_slice(b"fffd");
                    changed = true;
                }
                index += 6;
                continue;
            }
            index += 2;
            continue;
        }
        index += 1;
    }
    changed.then(|| String::from_utf8(result).expect("only ASCII hex digits were replaced"))
}

fn parse_result_text(text: &str) -> Result<Option<Value>> {
    match serde_json::from_str(text) {
        Ok(value) => Ok(Some(value)),
        Err(error) => {
            let message = error.to_string();
            if message.contains("recursion limit exceeded") {
                return Err(ToolOutputError::JsonParserDepthBoundary);
            }
            if message.contains("number out of range") {
                return Err(ToolOutputError::NonFiniteJsonNumber);
            }
            if replace_unpaired_escapes(text)
                .is_some_and(|replacement| serde_json::from_str::<Value>(&replacement).is_ok())
            {
                return Err(ToolOutputError::UnpairedUtf16Boundary);
            }
            Ok(None)
        }
    }
}

fn read_result(value: Option<&Value>, budget: &mut ResultReadBudget, depth: usize) -> Result<ResultEnvelope> {
    if depth > MAX_ENVELOPE_DEPTH || budget.remaining_nodes == 0 {
        budget.exceeded = true;
        return Ok(ResultEnvelope::default());
    }
    budget.remaining_nodes -= 1;
    let Some(value) = value else { return Ok(ResultEnvelope::default()); };
    if let Some(text) = value.as_str() {
        if js::utf16_len(text) > budget.remaining_bytes || text.len() > budget.remaining_bytes {
            budget.exceeded = true;
            return Ok(ResultEnvelope::default());
        }
        budget.remaining_bytes -= text.len();
        return match parse_result_text(text)? {
            Some(value) => read_result(Some(&value), budget, depth + 1),
            None => Ok(ResultEnvelope::default()),
        };
    }
    if let Some(blocks) = value.as_array() {
        if blocks.len() > MAX_CONTENT_BLOCKS {
            budget.exceeded = true;
            return Ok(ResultEnvelope::default());
        }
        let mut envelope = ResultEnvelope::default();
        for block in blocks {
            let text = if is_object(block) { block.get("text") } else { None };
            let text = match text {
                Some(text) if is_object(text) => nullish(text.get("text")).or(Some(text)),
                text => text,
            };
            let result = read_result(text, budget, depth + 1)?;
            if envelope.data.is_none() { envelope.data = result.data; }
            envelope.failed |= result.failed;
            if budget.exceeded { break; }
        }
        return Ok(envelope);
    }
    if !is_object(value) { return Ok(ResultEnvelope::default()); }
    let failed = value.get("isError") == Some(&Value::Bool(true))
        || value.get("is_error") == Some(&Value::Bool(true))
        || value.get("_tag").and_then(Value::as_str) == Some("OrchestratorMcpFailure")
        || nullish(value.get("error")).is_some();
    let content = nullish(value.get("structuredContent")).or_else(|| value.get("content"));
    if let Some(content) = content {
        let mut nested = read_result(Some(content), budget, depth + 1)?;
        nested.failed |= failed;
        return Ok(nested);
    }
    Ok(ResultEnvelope { data: Some(value.clone()), failed })
}

fn bounded_id(value: Option<&Value>) -> Option<&str> {
    value.and_then(Value::as_str).filter(|value| js::utf16_len(value) <= MAX_ID_LENGTH && !js::trim(value).is_empty())
}

fn clamp_html_render_height(height: f64) -> i64 {
    // Every value surviving the clamp is positive, so round() matches Math.round.
    height.round().clamp(80.0, 2000.0) as i64
}

fn read_html_render_reference(value: Option<&Value>) -> Result<Option<Value>> {
    let Some(value) = value.filter(|value| is_object(value)) else { return Ok(None); };
    let Some(attachment_id) = value.get("attachmentId").and_then(Value::as_str)
        .filter(|id| !id.is_empty() && js::utf16_len(id) <= 256) else { return Ok(None); };
    let Some(title) = value.get("title").and_then(Value::as_str) else { return Ok(None); };
    let Some(height) = value.get("height").and_then(Value::as_f64).filter(|value| value.is_finite()) else { return Ok(None); };
    let title = js::prefix_utf16(js::trim(title), 200).ok_or(ToolOutputError::UnpairedUtf16Boundary)?;
    let mut output = json!({
        "attachmentId":attachment_id,"title":if title.is_empty() {"HTML"} else {title},
        "height":clamp_html_render_height(height)
    });
    if let Some(heights) = value.get("heights").and_then(Value::as_array).filter(|values| !values.is_empty() && values.len() <= 24) {
        let measured = heights.iter().map(|entry| {
            let entry = entry.as_array().filter(|entry| entry.len() == 2)?;
            let width = entry[0].as_f64().filter(|value| value.fract() == 0.0 && (1.0..=10_000.0).contains(value))?;
            let height = entry[1].as_f64().filter(|value| value.is_finite())?;
            Some((width as i64, clamp_html_render_height(height)))
        }).collect::<Option<Vec<_>>>();
        if let Some(mut measured) = measured {
            measured.sort_by_key(|entry| entry.0);
            output["heights"] = json!(measured);
        }
    }
    Ok(Some(output))
}

fn read_domains(value: Option<&Value>) -> Option<Value> {
    static DOMAIN: OnceLock<Regex> = OnceLock::new();
    let domain = DOMAIN.get_or_init(|| Regex::new(r"^(?:https?|wss?)://(?:\*\.)?[a-z0-9-]+(?:\.[a-z0-9-]+)*(?::[0-9]{1,5})?$").expect("pinned domain regex"));
    let domains = value?.as_array()?.iter().filter_map(Value::as_str)
        .filter(|value| js::utf16_len(value) <= 256 && domain.is_match(&value.to_ascii_lowercase()))
        .take(32).map(|value| Value::String(value.into())).collect::<Vec<_>>();
    (!domains.is_empty()).then_some(Value::Array(domains))
}

fn read_mcp_app_reference(value: Option<&Value>) -> Option<Value> {
    let value = value.filter(|value| is_object(value))?;
    let attachment_id = bounded_id(value.get("attachmentId"))?;
    let server = bounded_id(value.get("server"))?;
    let tool = bounded_id(value.get("tool"))?;
    let resource_uri = value.get("resourceUri")?.as_str()
        .filter(|uri| js::utf16_len(uri) <= 4096 && uri.starts_with("ui://"))?;
    let mut output = json!({"attachmentId":attachment_id,"server":server,"tool":tool,"resourceUri":resource_uri});
    if let Some(source) = value.get("csp").filter(|value| is_object(value)) {
        let mut csp = Map::new();
        for key in ["connectDomains", "resourceDomains", "frameDomains", "baseUriDomains"] {
            if let Some(domains) = read_domains(source.get(key)) { csp.insert(key.into(), domains); }
        }
        if !csp.is_empty() { output["csp"] = Value::Object(csp); }
    }
    if let Some(source) = value.get("permissions").filter(|value| is_object(value)) {
        let mut permissions = Map::new();
        for key in ["camera", "microphone", "geolocation", "clipboardWrite"] {
            if source.get(key).is_some_and(is_object) { permissions.insert(key.into(), json!({})); }
        }
        if !permissions.is_empty() { output["permissions"] = Value::Object(permissions); }
    }
    if let Some(value) = value.get("prefersBorder").filter(|value| value.is_boolean()) {
        output["prefersBorder"] = value.clone();
    }
    Some(output)
}

pub fn compact_dynamic_tool_output(value: Option<&Value>) -> Result<Option<Value>> {
    let mut budget = ResultReadBudget::default();
    let result = read_result(value, &mut budget, 0)?;
    let mut output = Map::new();
    if result.failed { output.insert("isError".into(), Value::Bool(true)); }
    if let Some(data) = result.data.filter(|_| !budget.exceeded) {
        for key in ["threadId", "messageId", "taskId", "scheduledTaskId"] {
            if let Some(id) = bounded_id(data.get(key)) { output.insert(key.into(), json!(id)); }
        }
        if data.get("status").and_then(Value::as_str) == Some("rolled_back") {
            output.insert("status".into(), json!("rolled_back"));
        }
        for key in ["linkedCount", "unlinkedCount"] {
            if let Some(count) = data.get(key).and_then(Value::as_f64)
                .filter(|count| count.is_finite() && count.fract() == 0.0 && count.abs() <= 9_007_199_254_740_991.0)
            {
                output.insert(key.into(), json!(count as i64));
            }
        }
        if let Some(reference) = read_html_render_reference(data.get("htmlRender"))? {
            output.insert("htmlRender".into(), reference);
        }
        if let Some(reference) = read_mcp_app_reference(data.get(MCP_APP_OUTPUT_KEY)) {
            output.insert(MCP_APP_OUTPUT_KEY.into(), reference);
        }
        if let Some(thread) = data.get("thread").filter(|value| is_object(value))
            && let Some(id) = bounded_id(thread.get("threadId"))
        {
            output.insert("thread".into(), json!({"threadId":id}));
        }
        if let Some(threads) = data.get("threads").and_then(Value::as_array) {
            let mut complete = threads.len() <= MAX_THREADS;
            let mut compact = Vec::new();
            if complete {
                for entry in threads {
                    if !is_object(entry) { complete = false; break; }
                    let id = bounded_id(entry.get("threadId"));
                    let rolled_back = entry.get("status").and_then(Value::as_str) == Some("rolled_back");
                    if id.is_none() && !rolled_back { complete = false; break; }
                    let mut thread = Map::new();
                    if let Some(id) = id { thread.insert("threadId".into(), json!(id)); }
                    if rolled_back { thread.insert("status".into(), json!("rolled_back")); }
                    compact.push(Value::Object(thread));
                }
            }
            if complete { output.insert("threads".into(), Value::Array(compact)); }
            else { output.shift_remove("threadId"); output.shift_remove("status"); }
        }
    }
    let mut output = Value::Object(output);
    if js::stringify(&output).len() > MAX_METADATA_BYTES {
        let fields = output.as_object_mut().expect("output is an object");
        fields.shift_remove("threads"); fields.shift_remove("threadId"); fields.shift_remove("status");
    }
    if output.get(MCP_APP_OUTPUT_KEY).is_some_and(|app| app.get("csp").is_some())
        && js::stringify(&output).len() > MAX_METADATA_BYTES
    {
        output[MCP_APP_OUTPUT_KEY].as_object_mut().expect("validated MCP app").shift_remove("csp");
    }
    if js::stringify(&output).len() > MAX_METADATA_BYTES {
        output.as_object_mut().expect("output is an object").shift_remove(MCP_APP_OUTPUT_KEY);
    }
    Ok((!output.as_object().expect("output is an object").is_empty()).then_some(output))
}

// The two consumers in toolOutput.ts compare the resolver to these exact names.
// Other known tools cannot have a longer suffix ending in either of these names.
fn resolves_t3_tool(tool_name: Option<&str>, target: &str) -> bool {
    let Some(tool_name) = tool_name else { return false; };
    static PATTERNS: OnceLock<[Regex; 4]> = OnceLock::new();
    let patterns = PATTERNS.get_or_init(|| {
        let line = r"[^\r\n\u{2028}\u{2029}]";
        [
            Regex::new(&format!(r"{JS_SPACE}+(?:complete|completed){JS_SPACE}*$")).expect("suffix regex"),
            Regex::new(&format!(r"^mcp__({line}+?)__({line}+)$")).expect("MCP name regex"),
            Regex::new(&format!(r"^(t3[-_]code[-_]direct|t3-code|t3_code|t3code)(?:[.:/]|{JS_SPACE}*·{JS_SPACE}*)({line}+)$")).expect("namespace regex"),
            Regex::new(&format!(r"^(?:mcp[-_]{{1,2}})?t3[-_ ]?code(?:__|[-_.:/ ])({line}+)$")).expect("prefix regex"),
        ]
    });
    let lower = tool_name.to_ascii_lowercase();
    let label = match patterns[0].find(&lower) {
        Some(found) => js::trim(&tool_name[..found.start()]),
        None => js::trim(tool_name),
    };
    let lower = label.to_ascii_lowercase();
    if let Some(captures) = patterns[1].captures(&lower) {
        let server = captures.get(1).expect("server capture").as_str();
        let matched = captures.get(2).expect("tool capture");
        return ["t3-code", "t3_code", "t3code", "t3-code-direct", "t3_code_direct"].contains(&server)
            && &label[matched.start()..matched.end()] == target;
    }
    if let Some(captures) = patterns[2].captures(&lower) {
        let matched = captures.get(2).expect("tool capture");
        return &label[matched.start()..matched.end()] == target;
    }
    let candidate = patterns[3].captures(&lower).and_then(|captures| captures.get(1))
        .map(|matched| &label[matched.start()..matched.end()]).unwrap_or(label);
    candidate == target || (lower.starts_with("t3-code-") && label.ends_with(&format!("_{target}")))
}

pub fn html_render_from_tool_item(item: &Value) -> Result<Option<Value>> {
    if !resolves_t3_tool(item.get("toolName").and_then(Value::as_str), "html_render") { return Ok(None); }
    let output = compact_dynamic_tool_output(item.get("output"))?;
    Ok(output.filter(|output| output.get("isError") != Some(&Value::Bool(true)))
        .and_then(|output| output.get("htmlRender").cloned()))
}

pub fn preview_recording_id_from_tool_item(item: &Value) -> Result<Option<String>> {
    if !resolves_t3_tool(item.get("toolName").and_then(Value::as_str), "preview_recording_stop") { return Ok(None); }
    let mut budget = ResultReadBudget::default();
    let result = read_result(item.get("output"), &mut budget, 0)?;
    Ok(if result.failed || budget.exceeded { None }
        else { result.data.as_ref().and_then(|data| bounded_id(data.get("id"))).map(str::to_owned) })
}

fn claude_mcp_tool_name(server: &str, tool: &str) -> String {
    fn part(value: &str) -> String {
        value.encode_utf16().map(|unit| {
            if unit <= 127 && (unit as u8).is_ascii_alphanumeric() || unit == b'_' as u16 || unit == b'-' as u16 {
                char::from_u32(unit as u32).expect("ASCII character")
            } else { '_' }
        }).collect()
    }
    let mut server_part = part(server);
    if server.starts_with("claude.ai ") {
        let mut last_underscore = false;
        server_part = server_part.chars().filter(|character| {
            let duplicate = *character == '_' && last_underscore;
            last_underscore = *character == '_';
            !duplicate
        }).collect::<String>().trim_matches('_').to_owned();
    }
    format!("mcp__{server_part}__{}", part(tool))
}

pub fn mcp_app_from_tool_item(item: &Value) -> Result<Option<Value>> {
    let output = compact_dynamic_tool_output(item.get("output"))?;
    let Some(app) = output.and_then(|output| output.get(MCP_APP_OUTPUT_KEY).cloned()) else { return Ok(None); };
    let server = app["server"].as_str().expect("validated server");
    let tool = app["tool"].as_str().expect("validated tool");
    let name = item.get("toolName").and_then(Value::as_str);
    Ok((name == Some(format!("{server}.{tool}").as_str()) || name == Some(claude_mcp_tool_name(server, tool).as_str())).then_some(app))
}

pub fn tool_output_indicates_failure(text: &str) -> bool {
    static EXIT: OnceLock<[Regex; 3]> = OnceLock::new();
    let exits = EXIT.get_or_init(|| [
        Regex::new(&format!(r"<exited with exit code{JS_SPACE}+[1-9][0-9]*{JS_SPACE}*>")).expect("exit regex"),
        Regex::new(&format!(r"exit(?:ed)? with exit code{JS_SPACE}+[1-9][0-9]*")).expect("exit regex"),
        Regex::new(&format!(r"exit code{JS_SPACE}*[:\x09-\x0d\x20\u{{00a0}}\u{{1680}}\u{{2000}}-\u{{200a}}\u{{2028}}\u{{2029}}\u{{202f}}\u{{205f}}\u{{3000}}\u{{feff}}]{JS_SPACE}*[1-9][0-9]*(?-u:\b)")).expect("exit regex"),
    ]);
    let text = text.to_ascii_lowercase();
    ["file not found", "no files found", "enoent", "no such file", "commandnotfoundexception", "command not found", "is not recognized as the name of a cmdlet", "a parameter cannot be found that matches parameter name"]
        .iter().any(|phrase| text.contains(*phrase))
        || (text.contains("cannot find path") && text.contains("because it does not exist"))
        || (text.contains("is not recognized") && text.contains("the term '"))
        || exits.iter().any(|regex| regex.is_match(&text))
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ToolOutputImage {
    pub mime_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<String>,
}

fn image_parts(block: Option<&Value>) -> Option<(String, Option<&str>)> {
    let block = block.filter(|block| is_object(block) && block.get("type").and_then(Value::as_str) == Some("image"))?;
    let source = block.get("source").filter(|value| is_object(value));
    if source.is_some_and(|source| source.get("type").and_then(Value::as_str) != Some("base64")) { return None; }
    let mime_type = match source {
        Some(source) => source.get("media_type"),
        None => block.get("mimeType"),
    }?.as_str()?.to_lowercase();
    if !["image/gif", "image/jpeg", "image/png", "image/webp"].contains(&mime_type.as_str()) { return None; }
    let data = source.unwrap_or(block).get("data").and_then(Value::as_str);
    Some((mime_type, data))
}

pub fn read_tool_output_image(block: Option<&Value>) -> Option<ToolOutputImage> {
    let (mime_type, data) = image_parts(block)?;
    Some(ToolOutputImage { mime_type, data: data.map(str::to_owned) })
}

fn output_blocks(value: Option<&Value>) -> &[Value] {
    let Some(value) = value else { return &[]; };
    if let Some(blocks) = value.as_array() { return blocks; }
    if is_object(value) && let Some(blocks) = value.get("content").and_then(Value::as_array) {
        return blocks;
    }
    std::slice::from_ref(value)
}

pub fn tool_output_image_blocks(value: Option<&Value>) -> Vec<&Value> {
    output_blocks(value).iter().filter(|block| image_parts(Some(block)).is_some())
        .take(MAX_TOOL_OUTPUT_IMAGES).collect()
}

pub fn tool_output_images(value: Option<&Value>) -> Vec<ToolOutputImage> {
    tool_output_image_blocks(value).into_iter().filter_map(|block| read_tool_output_image(Some(block))).collect()
}

pub fn mcp_tool_result_output(result: &Value) -> Option<Cow<'_, Value>> {
    let content = result.get("content");
    let structured = nullish(result.get("structuredContent"));
    if let Some(structured) = structured && !tool_output_image_blocks(content).is_empty() {
        return Some(Cow::Owned(json!({"content":content,"structuredContent":structured})));
    }
    structured.or(content).map(Cow::Borrowed)
}

pub fn omit_tool_output_image_data(value: Option<&Value>) -> Option<Value> {
    fn omit(block: &Value) -> Value {
        match image_parts(Some(block)) {
            Some((mime_type, Some(_))) => json!({"type":"image","mimeType":mime_type}),
            _ => block.clone(),
        }
    }
    let value = value?;
    if let Some(blocks) = value.as_array() { return Some(Value::Array(blocks.iter().map(omit).collect())); }
    if is_object(value) && let Some(blocks) = value.get("content").and_then(Value::as_array) {
        let mut output = value.clone();
        output["content"] = Value::Array(blocks.iter().map(omit).collect());
        return Some(output);
    }
    Some(omit(value))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn compact(value: Value) -> Option<Value> {
        compact_dynamic_tool_output(Some(&value)).unwrap()
    }

    fn images(value: &Value) -> Value {
        serde_json::to_value(tool_output_images(Some(value))).unwrap()
    }

    // These first twelve cases are translations of every upstream test in
    // packages/shared/src/toolOutput.test.ts at the module's immutable pin.
    #[test]
    fn extracts_ids_through_the_mcp_result_envelopes_used_by_t3_summaries() {
        let metadata = json!({"threadId":"thread-1","messageId":"message-1"});
        let body = json!({"threadId":"thread-1","messageId":"message-1","response":"Private response body"});
        let text = js::stringify(&body);
        for value in [
            body.clone(),
            json!({"structuredContent":body,"content":"Ignored alternative body"}),
            json!([{"type":"text","text":text}]),
            json!({"content":[{"text":{"text":text}}],"isError":false}),
            json!(text),
        ] {
            assert_eq!(compact(value), Some(metadata.clone()));
        }
    }

    #[test]
    fn keeps_nested_thread_identity_and_scalar_task_ids_without_result_bodies() {
        let output = json!({
            "thread":{"threadId":"thread-1","messages":["Private message"]},
            "taskId":"task-1","scheduledTaskId":"scheduled-1","status":"failed",
            "summary":"A child failed; the tool itself succeeded.",
            "unlinked":[{"repository":"acme/app","state":"merged","numbers":[1]}],
            "unlinkedCount":1,"linkedCount":2
        });
        assert_eq!(compact_dynamic_tool_output(Some(&output)).unwrap(), Some(json!({
            "thread":{"threadId":"thread-1"},"taskId":"task-1","scheduledTaskId":"scheduled-1",
            "linkedCount":2,"unlinkedCount":1
        })));
        assert_eq!(output["thread"]["messages"], json!(["Private message"]));
    }

    #[test]
    fn keeps_explicit_envelope_failure_while_dropping_error_text_and_arbitrary_output() {
        for mut failure in [
            json!({"isError":true}), json!({"is_error":true}),
            json!({"_tag":"OrchestratorMcpFailure"}), json!({"error":{"message":"Private error"}}),
        ] {
            failure["structuredContent"] = json!({"threadId":"thread-1","error":"Private details"});
            assert_eq!(compact(failure), Some(json!({"isError":true,"threadId":"thread-1"})));
        }
        assert_eq!(compact(json!("Private plain text")), None);
        assert_eq!(compact(json!({"result":{"body":"Private result"}})), None);
    }

    #[test]
    fn uses_first_result_data_but_combines_explicit_failure_flags_across_content_blocks() {
        assert_eq!(compact(json!([
            {"text":js::stringify(&json!({"threadId":"first"}))},
            {"text":js::stringify(&json!({"threadId":"second","isError":true,"error":"Private error"}))}
        ])), Some(json!({"threadId":"first","isError":true})));
        assert_eq!(compact(json!([
            {"text":"{}"},{"text":js::stringify(&json!({"threadId":"second"}))}
        ])), None);
    }

    #[test]
    fn keeps_complete_thread_creation_batches_and_rollback_markers_without_row_bodies() {
        assert_eq!(compact(json!({"threads":[
            {"threadId":"created","status":"running","messages":["Private message"]},
            {"threadId":"reverted","status":"rolled_back","error":"Private error"},
            {"status":"rolled_back"}
        ]})), Some(json!({"threads":[
            {"threadId":"created"},{"threadId":"reverted","status":"rolled_back"},{"status":"rolled_back"}
        ]})));
        assert_eq!(compact(json!({"threadId":"reverted","status":"rolled_back"})), Some(json!({"threadId":"reverted","status":"rolled_back"})));
        assert_eq!(compact(json!({"threads":[]})), Some(json!({"threads":[]})));
    }

    #[test]
    fn omits_incomplete_or_oversized_creation_evidence_instead_of_returning_a_partial_count() {
        for threads in [
            json!([{"threadId":"known"},{"title":"No confirmed ID"}]),
            json!((0..101).map(|i| json!({"threadId":format!("thread-{i}")})).collect::<Vec<_>>()),
            json!((0..100).map(|i| {
                let mut id = i.to_string();
                id.push_str(&"x".repeat(256 - id.len()));
                json!({"threadId":id})
            }).collect::<Vec<_>>()),
            json!([{"threadId":"x".repeat(257)}]),
        ] {
            assert_eq!(compact(json!({"threadId":"must-not-be-counted-as-one","status":"rolled_back","taskId":"task-1","threads":threads})), Some(json!({"taskId":"task-1"})));
        }
        let normal = json!({"threads":(0..100).map(|i| json!({"threadId":format!("00000000-0000-4000-8000-{i:012}")})).collect::<Vec<_>>()});
        let output = compact(normal.clone()).unwrap();
        assert_eq!(output, normal);
        assert!(js::stringify(&output).len() <= 8192);
    }

    #[test]
    fn bounds_parsing_and_envelope_scanning_while_preserving_an_outer_failure_marker() {
        let oversized = js::stringify(&json!({"threadId":"hidden","body":"😄".repeat(5000)}));
        for content in [
            json!("x".repeat(1_000_000)), json!(oversized),
            json!((0..33).map(|_| json!({"text":js::stringify(&json!({"threadId":"hidden"}))})).collect::<Vec<_>>()),
            json!({"content":{"content":{"content":{"content":{"threadId":"too-deep"}}}}}),
        ] {
            assert_eq!(compact(json!({"isError":true,"content":content})), Some(json!({"isError":true})));
        }
        assert_eq!(compact(json!({"threadId":"known","body":"x".repeat(1_000_000)})), Some(json!({"threadId":"known"})));
    }

    #[test]
    fn preserves_command_failure_phrases_across_shells_without_copying_output() {
        for text in [
            "FILE NOT FOUND", "No files found", "ENOENT", "No such file or directory",
            "CommandNotFoundException", "command not found", "Cannot find path 'a' because it does not exist",
            "The term 'example' is not recognized", "is not recognized as the name of a cmdlet",
            "A parameter cannot be found that matches parameter name", "<exited with exit code 2>",
            "Exited with exit code 1", "exit code: 127",
        ] { assert!(tool_output_indicates_failure(text), "{text}"); }
    }

    #[test]
    fn does_not_mark_successful_exit_codes_or_incomplete_failure_phrases() {
        for text in ["Done", "exit code: 0", "Exited with exit code 0", "cannot find path", "is not recognized"] {
            assert!(!tool_output_indicates_failure(text), "{text}");
        }
    }

    #[test]
    fn finds_images_in_each_place_providers_store_them_in_order() {
        let mcp = json!({"type":"image","data":"AAAA","mimeType":"image/png"});
        let claude = json!({"type":"image","source":{"type":"base64","media_type":"image/JPEG","data":"BBBB"}});
        let text = json!({"type":"text","text":"{}"});
        assert_eq!(images(&json!({"content":[text,claude,mcp]})), json!([
            {"mimeType":"image/jpeg","data":"BBBB"},{"mimeType":"image/png","data":"AAAA"}
        ]));
        assert_eq!(images(&json!([mcp])), json!([{"mimeType":"image/png","data":"AAAA"}]));
        assert_eq!(images(&mcp), json!([{"mimeType":"image/png","data":"AAAA"}]));
        let structured = json!({"screenshot":{"mimeType":"image/png","width":728,"height":354}});
        let content = json!([text,mcp]);
        let result = json!({"content":content,"structuredContent":structured});
        let output = mcp_tool_result_output(&result).unwrap();
        assert_eq!(images(&output), json!([{"mimeType":"image/png","data":"AAAA"}]));
        assert_eq!(*output, result);
        let result = json!({"content":[text],"structuredContent":structured});
        let output = mcp_tool_result_output(&result).unwrap();
        assert!(matches!(output, Cow::Borrowed(_)));
        assert!(std::ptr::eq(output.as_ref(), &result["structuredContent"]));
        let result = json!({"content":content});
        assert!(std::ptr::eq(mcp_tool_result_output(&result).unwrap().as_ref(), &result["content"]));
    }

    #[test]
    fn skips_types_the_server_must_not_serve_inline() {
        assert_eq!(images(&json!([
            {"type":"image","data":"AAAA","mimeType":"image/svg+xml"},
            {"type":"image","data":"AAAA","mimeType":"text/html"},
            {"type":"image","source":{"type":"url","url":"https://example.com/a.png"}}
        ])), json!([]));
    }

    #[test]
    fn stops_after_the_first_few_images() {
        let mcp = json!({"type":"image","data":"AAAA","mimeType":"image/png"});
        let value = json!(vec![mcp; 10_000]);
        let result = tool_output_images(Some(&value));
        assert_eq!(result.len(), MAX_TOOL_OUTPUT_IMAGES);
        let blocks = tool_output_image_blocks(Some(&value));
        assert!(std::ptr::eq(blocks[0], &value[0]));
        assert_eq!(MAX_TOOL_OUTPUT_IMAGE_BASE64_LENGTH, 13_981_016);
    }

    #[test]
    fn undefined_and_null_are_not_conflated() {
        assert_eq!(compact_dynamic_tool_output(None).unwrap(), None);
        assert_eq!(omit_tool_output_image_data(None), None);
        assert_eq!(omit_tool_output_image_data(Some(&Value::Null)), Some(Value::Null));
        assert!(mcp_tool_result_output(&json!({})).is_none());
        let value = json!({"content":null,"structuredContent":null});
        assert!(matches!(mcp_tool_result_output(&value), Some(Cow::Borrowed(Value::Null))));
        assert_eq!(compact(json!({"threadId":"ignored","content":null})), None);
        assert_eq!(compact(json!({"structuredContent":null,"content":{"threadId":"selected"}})), Some(json!({"threadId":"selected"})));
        assert_eq!(compact(json!({"error":false,"threadId":"id"})), Some(json!({"isError":true,"threadId":"id"})));
    }

    #[test]
    fn identifiers_use_js_trim_and_utf16_lengths_and_counts_use_safe_integers() {
        let id = "😀".repeat(128);
        assert_eq!(compact(json!({"threadId":id})), Some(json!({"threadId":id})));
        assert_eq!(compact(json!({"threadId":"😀".repeat(129)})), None);
        assert_eq!(compact(json!({"threadId":" \u{feff}\u{00a0}"})), None);
        assert_eq!(compact(json!({"threadId":"\u{0085}"})), Some(json!({"threadId":"\u{0085}"})));
        assert_eq!(compact(json!({"linkedCount":9007199254740991_u64,"unlinkedCount":-9007199254740991_i64})), Some(json!({"linkedCount":9007199254740991_u64,"unlinkedCount":-9007199254740991_i64})));
        assert_eq!(compact(json!({"linkedCount":9007199254740992_u64,"unlinkedCount":1.5})), None);
        assert_eq!(compact(json!({"linkedCount":1.0,"unlinkedCount":-0.0})), Some(json!({"linkedCount":1,"unlinkedCount":0})));
    }

    #[test]
    fn strips_only_supported_inline_image_data_and_preserves_other_fields() {
        let value = json!({"content":[
            {"type":"image","source":{"type":"base64","media_type":"image/PNG","data":"AAAA"},"extra":1},
            {"type":"image","mimeType":"image/webp","data":null,"extra":2},
            {"type":"image","mimeType":"image/svg+xml","data":"unchanged"},
            {"type":"text","text":"unchanged"}
        ],"structuredContent":{"threadId":"kept"},"isError":false,"unknown":7});
        let mut expected = value.clone();
        expected["content"][0] = json!({"type":"image","mimeType":"image/png"});
        assert_eq!(omit_tool_output_image_data(Some(&value)), Some(expected));
        assert_eq!(value["content"][0]["source"]["data"], "AAAA");
        assert_eq!(read_tool_output_image(Some(&json!({"type":"image","mimeType":"image/gif","data":19}))), Some(ToolOutputImage {mime_type:"image/gif".into(),data:None}));
        assert!(read_tool_output_image(Some(&json!({"type":"image","mimeType":"image/png","data":"AAAA","source":[]}))).is_none());
    }

    #[test]
    fn html_reference_normalization_and_tool_names_follow_the_imported_readers() {
        let reference = json!({"attachmentId":"attachment","title":" \u{feff}Hello ","height":2999.5,"heights":[[800,400.5],[320,10.0]],"private":"omitted"});
        let expected = json!({"attachmentId":"attachment","title":"Hello","height":2000,"heights":[[320,80],[800,401]]});
        for name in ["html_render", "t3-code.html_render", "T3_CODE/html_render", "mcp__t3-code-direct__html_render", "t3-code · html_render Completed", "mcp_t3-code_html_render", "t3-code-thread_with_under_scores_html_render"] {
            assert_eq!(html_render_from_tool_item(&json!({"toolName":name,"output":{"htmlRender":reference}})).unwrap(), Some(expected.clone()), "{name}");
        }
        for name in ["mcp__untrusted__html_render", "HTML_RENDER", "other.html_render"] {
            assert_eq!(html_render_from_tool_item(&json!({"toolName":name,"output":{"htmlRender":reference}})).unwrap(), None);
        }
        assert_eq!(html_render_from_tool_item(&json!({"toolName":"html_render","output":{"isError":true,"htmlRender":reference}})).unwrap(), None);
        assert_eq!(compact(json!({"htmlRender":{"attachmentId":" ","title":" ","height":1,"heights":[[320,1],["bad",100]]}})), Some(json!({"htmlRender":{"attachmentId":" ","title":"HTML","height":80}})));
    }

    #[test]
    fn recording_id_requires_the_correct_successful_tool_and_envelope_budget() {
        let output = json!({"content":[{"text":"{\"id\":\"recording-1\",\"private\":\"dropped\"}"}]});
        assert_eq!(preview_recording_id_from_tool_item(&json!({"toolName":"mcp__t3-code__preview_recording_stop","output":output})).unwrap(), Some("recording-1".into()));
        assert_eq!(preview_recording_id_from_tool_item(&json!({"toolName":"preview_recording_start","output":output})).unwrap(), None);
        assert_eq!(preview_recording_id_from_tool_item(&json!({"toolName":"preview_recording_stop","output":{"id":"recording","error":"failed"}})).unwrap(), None);
    }

    #[test]
    fn app_reference_requires_source_match_and_preserves_its_own_failure_semantics() {
        let app = json!({"attachmentId":"app-1","server":"claude.ai My App","tool":"draw😀","resourceUri":"ui://app","csp":{"connectDomains":["https://example.com","*","http://*.example.org:12345","data:"]},"permissions":{"camera":[],"microphone":true,"geolocation":{"ignored":1}},"prefersBorder":false,"private":"omitted"});
        let expected = json!({"attachmentId":"app-1","server":"claude.ai My App","tool":"draw😀","resourceUri":"ui://app","csp":{"connectDomains":["https://example.com","http://*.example.org:12345"]},"permissions":{"camera":{},"geolocation":{}},"prefersBorder":false});
        for name in ["claude.ai My App.draw😀", "mcp__claude_ai_My_App__draw__"] {
            assert_eq!(mcp_app_from_tool_item(&json!({"toolName":name,"output":{"isError":true,"t3McpApp":app}})).unwrap(), Some(expected.clone()));
        }
        assert_eq!(mcp_app_from_tool_item(&json!({"toolName":"different.draw😀","output":{"t3McpApp":app}})).unwrap(), None);
        assert_eq!(compact(json!({"t3McpApp":{"attachmentId":"a","server":"s","tool":"t","resourceUri":"https://example.com"}})), None);
    }

    #[test]
    fn metadata_budget_drops_threads_then_csp_then_app_as_upstream_does() {
        let domain = format!("https://{}", "x".repeat(248));
        let domains = vec![domain; 32];
        let app = json!({"attachmentId":"a","server":"s","tool":"t","resourceUri":"ui://app","csp":{
            "connectDomains":domains,"resourceDomains":domains,"frameDomains":domains,"baseUriDomains":domains
        }});
        let output = compact(json!({"threadId":"drop","status":"rolled_back","threads":[],"t3McpApp":app})).unwrap();
        assert_eq!(output, json!({"t3McpApp":{"attachmentId":"a","server":"s","tool":"t","resourceUri":"ui://app"}}));
        assert_eq!(compact(json!({"taskId":"keep","t3McpApp":{
            "attachmentId":"a","server":"s","tool":"t","resourceUri":format!("ui://{}", "\0".repeat(4091))
        }})), Some(json!({"taskId":"keep"})));
    }

    #[test]
    fn ecmascript_failure_regex_uses_ascii_digits_boundaries_and_js_whitespace() {
        for text in ["exit code:\u{feff}2", "exit code 2é", "Exited with exit code\u{00a0}12"] {
            assert!(tool_output_indicates_failure(text), "{text}");
        }
        for text in ["exit code:\u{0085}2", "exit code: ٢", "exit code: 2x", "NO FILEſ FOUND"] {
            assert!(!tool_output_indicates_failure(text), "{text}");
        }
    }

    #[test]
    fn unsupported_js_string_and_json_representation_boundaries_are_explicit() {
        let title = format!("{}😀", "a".repeat(199));
        let value = json!({"htmlRender":{"attachmentId":"a","title":title,"height":80}});
        assert_eq!(compact_dynamic_tool_output(Some(&value)), Err(ToolOutputError::UnpairedUtf16Boundary));
        assert_eq!(html_render_from_tool_item(&json!({"toolName":"html_render","output":value})), Err(ToolOutputError::UnpairedUtf16Boundary));
        for text in [r#"{"threadId":"\ud800"}"#, r#"{"threadId":"\udc00"}"#] {
            assert_eq!(compact_dynamic_tool_output(Some(&json!(text))), Err(ToolOutputError::UnpairedUtf16Boundary));
        }
        assert_eq!(compact(json!(r#"{"threadId":"\\ud800"}"#)), Some(json!({"threadId":"\\ud800"})));
        assert_eq!(compact(json!(r#"{"threadId":"\ud83d\ude00"}"#)), Some(json!({"threadId":"😀"})));
        assert_eq!(compact_dynamic_tool_output(Some(&json!(r#"{"threadId":"id","ignored":1e400}"#))), Err(ToolOutputError::NonFiniteJsonNumber));
        let deep = format!("{{\"threadId\":\"id\",\"ignored\":{}0{}}}", "[".repeat(130), "]".repeat(130));
        assert_eq!(compact_dynamic_tool_output(Some(&json!(deep))), Err(ToolOutputError::JsonParserDepthBoundary));
    }
}
