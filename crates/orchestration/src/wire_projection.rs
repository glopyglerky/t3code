//! Port of apps/server/src/orchestration-v2/WireProjection.ts from T3 Code
//! 521ac7c218b3faa74281fb72fc36ce74a0951fb3 (MIT). Record names and fields are
//! upstream's. The projector owns the upstream WeakMap caches; the live deduper
//! is per subscription, after coalescing, just as in the original module.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Weak};

use serde_json::{Value, json};

use crate::deduped_bodies::{BodySource, dedupe_subagent_bodies, dedupe_turn_item_bodies};
use crate::js;
use crate::tool_output::{self, ToolOutputError};
use crate::types::{Item, Projection};

pub const MAX_WIRE_CHECKPOINT_FILES: usize = 200;
pub const MAX_DETAIL_STRING_BYTES: usize = 32_768;
pub const MAX_DYNAMIC_VALUE_BYTES: usize = 16_384;
pub const MAX_ON_DEMAND_BYTES: usize = 256 * 1024;
pub const TRUNCATION_MARKER: &str = "\n… output truncated for transport";

#[derive(Debug)]
pub enum ProjectionError {
    InvalidContract(&'static str),
    UnpairedUtf16Boundary,
    ToolOutput(ToolOutputError),
}

impl std::fmt::Display for ProjectionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidContract(field) => write!(f, "invalid upstream projection contract: {field}"),
            Self::UnpairedUtf16Boundary => f.write_str("JavaScript UTF-16 slice would produce an unpaired surrogate; this representation is not ported"),
            Self::ToolOutput(error) => error.fmt(f),
        }
    }
}

impl std::error::Error for ProjectionError {}

impl From<ToolOutputError> for ProjectionError {
    fn from(error: ToolOutputError) -> Self { Self::ToolOutput(error) }
}

type Result<T> = std::result::Result<T, ProjectionError>;
type Cache = HashMap<usize, (Weak<Value>, Item)>;

fn cached(cache: &mut Cache, source: &Item) -> Option<Item> {
    cache.retain(|_, (source, _)| source.strong_count() != 0);
    cache.get(&(Arc::as_ptr(source) as usize)).and_then(|(key, value)| {
        key.upgrade().filter(|key| Arc::ptr_eq(key, source)).map(|_| value.clone())
    })
}

fn remember(cache: &mut Cache, source: &Item, projected: &Item) {
    cache.insert(Arc::as_ptr(source) as usize, (Arc::downgrade(source), projected.clone()));
}

fn object(value: &Value) -> Result<serde_json::Map<String, Value>> {
    value.as_object().cloned().ok_or(ProjectionError::InvalidContract("record must be an object"))
}

fn optional_string<'a>(value: &'a Value, field: &'static str) -> Result<Option<&'a str>> {
    value.get(field).map(|value| value.as_str().ok_or(ProjectionError::InvalidContract(field))).transpose()
}

/// Buffer's UTF-8 truncation drops its final replacement character, even if that
/// character was present in the original prefix. The marker counts toward the cap.
pub fn truncate_detail(value: &str, max_bytes: usize) -> String {
    if value.len() <= max_bytes { return value.into(); }
    let budget = max_bytes.saturating_sub(TRUNCATION_MARKER.len());
    let mut prefix = String::from_utf8_lossy(&value.as_bytes()[..budget.min(value.len())]).into_owned();
    if prefix.ends_with('\u{fffd}') { prefix.pop(); }
    prefix.push_str(TRUNCATION_MARKER);
    prefix
}

fn set_truncated(record: &mut serde_json::Map<String, Value>, source: &Value, field: &'static str, limit: usize) -> Result<()> {
    if let Some(text) = optional_string(source, field)? {
        record.insert(field.into(), json!(truncate_detail(text, limit)));
    }
    Ok(())
}

pub fn bound_file_change_for_storage(item: &Item) -> Result<Item> {
    if item["type"] != "file_change" { return Ok(item.clone()); }
    let mut bounded = object(item)?;
    for field in ["diffStr", "oldStr", "newStr"] {
        set_truncated(&mut bounded, item, field, MAX_DETAIL_STRING_BYTES)?;
    }
    let bounded = Value::Object(bounded);
    Ok(if bounded == **item { item.clone() } else { Arc::new(bounded) })
}

fn is_space(unit: u16) -> bool {
    char::from_u32(u32::from(unit)).is_some_and(|ch| js::trim(&ch.to_string()).is_empty())
}

fn summarize_dynamic_value(value: &Value) -> Result<Value> {
    let serialized = if let Some(text) = value.as_str().filter(|text| js::utf16_len(text) > MAX_DYNAMIC_VALUE_BYTES) {
        text.to_owned()
    } else {
        let encoded = js::stringify(value);
        if encoded.len() <= MAX_DYNAMIC_VALUE_BYTES { return Ok(value.clone()); }
        value.as_str().unwrap_or(&encoded).to_owned()
    };
    let units: Vec<u16> = serialized.encode_utf16().collect();
    let Some(start) = units.iter().position(|unit| !is_space(*unit)) else {
        return Ok(json!({"summary":"Large tool output","truncated":true}));
    };
    let mut line = Vec::new();
    let mut pending_space = false;
    for unit in &units[start..] {
        if *unit == 10 { break; }
        if is_space(*unit) { pending_space = true; continue; }
        if pending_space { line.push(32); }
        line.push(*unit);
        pending_space = false;
        if line.len() > 160 { break; }
    }
    if line.len() > 160 {
        line.truncate(159);
        while line.last().is_some_and(|unit| is_space(*unit)) { line.pop(); }
        line.push(0x2026);
    }
    let summary = String::from_utf16(&line).map_err(|_| ProjectionError::UnpairedUtf16Boundary)?;
    Ok(json!({"summary":summary,"truncated":true}))
}

fn has_dynamic_value(value: Option<&Value>) -> bool {
    match value {
        None | Some(Value::Null) => false,
        Some(Value::String(value)) => !js::trim(value).is_empty(),
        Some(Value::Array(value)) => !value.is_empty(),
        Some(Value::Object(value)) => !value.is_empty(),
        _ => true,
    }
}

fn bound_dynamic_value(value: Option<&Value>) -> Option<Value> {
    value.map(|value| {
        if let Some(text) = value.as_str() {
            return json!(truncate_detail(text, MAX_ON_DEMAND_BYTES));
        }
        let encoded = js::stringify(value);
        if encoded.len() <= MAX_ON_DEMAND_BYTES { value.clone() }
        else { json!(truncate_detail(&encoded, MAX_ON_DEMAND_BYTES)) }
    })
}

fn bound_tool_output(value: Option<&Value>) -> Option<Value> {
    let omitted = tool_output::omit_tool_output_image_data(value);
    let bounded = bound_dynamic_value(omitted.as_ref());
    if !bounded.as_ref().is_some_and(Value::is_string) || omitted.as_ref().is_some_and(Value::is_string) {
        return bounded;
    }
    let images = tool_output::tool_output_images(omitted.as_ref());
    if images.is_empty() { return bounded; }
    let mut blocks = vec![json!({"type":"text","text":bounded})];
    blocks.extend(images.into_iter().map(|image| json!({"type":"image","mimeType":image.mime_type})));
    Some(json!(blocks))
}

fn number(value: f64) -> Value {
    if value.abs() <= 9_007_199_254_740_991.0 && value.fract() == 0.0 { json!(value as i64) }
    else { json!(value) }
}

#[derive(Default)]
pub struct WireProjector {
    capped_checkpoint_files: Cache,
    projected_stored_events: Cache,
    thread_stream_events: Cache,
}

impl WireProjector {
    fn cap_checkpoint_files(&mut self, value: &Item) -> Result<Item> {
        let files = value["files"].as_array().ok_or(ProjectionError::InvalidContract("files"))?;
        if files.len() <= MAX_WIRE_CHECKPOINT_FILES || value.get("filesTotal").is_some() { return Ok(value.clone()); }
        if let Some(capped) = cached(&mut self.capped_checkpoint_files, value) { return Ok(capped); }
        let mut additions = 0.0;
        let mut deletions = 0.0;
        for file in files {
            additions += file["additions"].as_f64().ok_or(ProjectionError::InvalidContract("additions"))?;
            deletions += file["deletions"].as_f64().ok_or(ProjectionError::InvalidContract("deletions"))?;
        }
        let mut capped = object(value)?;
        capped.insert("files".into(), json!(&files[..MAX_WIRE_CHECKPOINT_FILES]));
        capped.insert("filesTotal".into(), json!({"count":files.len(),"additions":number(additions),"deletions":number(deletions)}));
        let capped = Arc::new(Value::Object(capped));
        remember(&mut self.capped_checkpoint_files, value, &capped);
        Ok(capped)
    }

    pub fn project_turn_item_for_wire(&mut self, item: &Item) -> Result<Item> {
        let kind = item["type"].as_str().ok_or(ProjectionError::InvalidContract("type"))?;
        if kind == "checkpoint" { return self.cap_checkpoint_files(item); }
        let mut projected = object(item)?;
        match kind {
            "handoff" => { projected.remove("summary"); }
            "command_execution" => {
                let output = optional_string(item, "output")?;
                projected.remove("output");
                let failed = if let Some(exit_code) = item.get("exitCode") {
                    exit_code.as_f64() != Some(0.0)
                } else {
                    item["outputIndicatesFailure"] == true || output.is_some_and(|text| {
                        // JS slices UTF-16 units. A dangling final surrogate cannot
                        // match the ASCII failure patterns, so exclude that unit.
                        let prefix = js::prefix_utf16(text, MAX_DETAIL_STRING_BYTES)
                            .or_else(|| js::prefix_utf16(text, MAX_DETAIL_STRING_BYTES - 1)).unwrap_or(text);
                        tool_output::tool_output_indicates_failure(prefix)
                    })
                };
                if failed { projected.insert("outputIndicatesFailure".into(), json!(true)); }
                if output.is_some_and(|text| !js::trim(text).is_empty()) { projected.insert("outputOmitted".into(), json!(true)); }
            }
            "file_change" => {
                for field in ["diffStr", "oldStr", "newStr"] { projected.remove(field); }
                if item["status"] == "failed"
                    && let Some(diff) = optional_string(item, "diffStr")?.filter(|text| !js::trim(text).is_empty())
                {
                    projected.insert("diffStr".into(), json!(truncate_detail(diff, MAX_DETAIL_STRING_BYTES)));
                }
            }
            "subagent" => {
                projected.insert("prompt".into(), json!(truncate_detail(optional_string(item,"prompt")?.unwrap_or(""), MAX_DETAIL_STRING_BYTES)));
                set_truncated(&mut projected,item,"progress",MAX_DETAIL_STRING_BYTES)?;
                if item["result"].is_null() { projected.insert("result".into(), Value::Null); }
                else { set_truncated(&mut projected,item,"result",MAX_DETAIL_STRING_BYTES)?; }
            }
            "dynamic_tool" => {
                projected.remove("output");
                if let Some(input) = item.get("input") { projected.insert("input".into(), summarize_dynamic_value(input)?); }
                if let Some(output) = tool_output::compact_dynamic_tool_output(item.get("output"))? { projected.insert("output".into(), output); }
                if has_dynamic_value(item.get("output")) { projected.insert("outputOmitted".into(), json!(true)); }
            }
            _ => return Ok(item.clone()),
        }
        Ok(Arc::new(Value::Object(projected)))
    }

    pub fn project_turn_item_for_detail(&mut self, item: &Item) -> Result<Item> {
        let kind = item["type"].as_str().ok_or(ProjectionError::InvalidContract("type"))?;
        let mut projected = object(item)?;
        match kind {
            "command_execution" => {
                projected.insert("input".into(), json!(truncate_detail(optional_string(item,"input")?.unwrap_or(""),MAX_ON_DEMAND_BYTES)));
                set_truncated(&mut projected,item,"output",MAX_ON_DEMAND_BYTES)?;
            }
            "dynamic_tool" => {
                if let Some(input) = bound_dynamic_value(item.get("input")) { projected.insert("input".into(),input); }
                if let Some(output) = bound_tool_output(item.get("output")) { projected.insert("output".into(),output); }
            }
            "subagent" => {
                projected.insert("prompt".into(),json!(truncate_detail(optional_string(item,"prompt")?.unwrap_or(""),MAX_ON_DEMAND_BYTES)));
                set_truncated(&mut projected,item,"progress",MAX_ON_DEMAND_BYTES)?;
                if item["result"].is_null() { projected.insert("result".into(),Value::Null); }
                else { set_truncated(&mut projected,item,"result",MAX_ON_DEMAND_BYTES)?; }
            }
            "file_change" => {
                projected = object(&self.project_turn_item_for_wire(item)?)?;
                set_truncated(&mut projected,item,"diffStr",MAX_ON_DEMAND_BYTES)?;
                if optional_string(item,"diffStr")?.is_none_or(|text| js::trim(text).is_empty()) {
                    set_truncated(&mut projected,item,"oldStr",MAX_ON_DEMAND_BYTES)?;
                    set_truncated(&mut projected,item,"newStr",MAX_ON_DEMAND_BYTES)?;
                }
            }
            "handoff" => return self.project_turn_item_for_wire(item),
            _ => return Ok(item.clone()),
        }
        Ok(Arc::new(Value::Object(projected)))
    }

    pub fn cap_projection_checkpoint_files_for_wire(&mut self, projection: &Projection) -> Result<Projection> {
        let mut next = (**projection).clone();
        let mut changed = false;
        for checkpoint in &mut next.checkpoints {
            let original = Arc::new(checkpoint.clone());
            let capped = self.cap_checkpoint_files(&original)?;
            changed |= !Arc::ptr_eq(&original,&capped);
            *checkpoint = (*capped).clone();
        }
        for item in &mut next.turn_items {
            if item["type"] == "checkpoint" {
                let capped = self.cap_checkpoint_files(item)?;
                changed |= !Arc::ptr_eq(item,&capped);
                *item = capped;
            }
        }
        for row in &mut next.visible_turn_items {
            if row.item["type"] == "checkpoint" {
                let item = self.cap_checkpoint_files(&row.item)?;
                if !Arc::ptr_eq(&row.item,&item) {
                    let mut value = (**row).clone(); value.item = item; *row = Arc::new(value); changed = true;
                }
            }
        }
        Ok(if changed { Arc::new(next) } else { projection.clone() })
    }

    pub fn project_thread_projection_for_wire(&mut self, projection: &Projection) -> Result<Projection> {
        let mut next = (**projection).clone();
        for checkpoint in &mut next.checkpoints { *checkpoint = (*self.cap_checkpoint_files(&Arc::new(checkpoint.clone()))?).clone(); }
        for handoff in &mut next.context_handoffs { *handoff = project_context_handoff_for_wire(handoff)?; }
        let mut by_id = HashMap::<String,Item>::new();
        for item in &mut next.turn_items {
            *item = self.project_once(item, &mut by_id)?;
        }
        for row in &mut next.visible_turn_items {
            let mut value = (**row).clone();
            value.item = self.project_once(&row.item, &mut by_id)?;
            *row = Arc::new(value);
        }
        Ok(Arc::new(next))
    }

    fn project_once(&mut self, item: &Item, by_id: &mut HashMap<String,Item>) -> Result<Item> {
        let thread = item["threadId"].as_str().ok_or(ProjectionError::InvalidContract("threadId"))?;
        let id = item["id"].as_str().ok_or(ProjectionError::InvalidContract("id"))?;
        // Deliberately the same composite key as upstream, not a new identity scheme.
        let key = format!("{thread}:{id}");
        if let Some(projected) = by_id.get(&key) { return Ok(projected.clone()); }
        let projected = self.project_turn_item_for_wire(item)?;
        by_id.insert(key, projected.clone());
        Ok(projected)
    }

    pub fn project_domain_event_for_wire(&mut self, event: &Item) -> Result<Item> {
        let payload = match event["type"].as_str() {
            Some("turn-item.updated") => self.project_turn_item_for_wire(&Arc::new(event["payload"].clone()))?,
            Some("context-handoff.updated") => Arc::new(project_context_handoff_for_wire(&event["payload"])?),
            Some("checkpoint.captured") => self.cap_checkpoint_files(&Arc::new(event["payload"].clone()))?,
            _ => return Ok(event.clone()),
        };
        let mut projected = object(event)?;
        projected.insert("payload".into(),(*payload).clone());
        Ok(Arc::new(Value::Object(projected)))
    }

    pub fn project_stored_event_for_wire(&mut self, stored: &Item) -> Result<Item> {
        if let Some(cached) = cached(&mut self.projected_stored_events,stored) { return Ok(cached); }
        let event = Arc::new(stored["event"].clone());
        let projected_event = self.project_domain_event_for_wire(&event)?;
        if Arc::ptr_eq(&event,&projected_event) { return Ok(stored.clone()); }
        let mut projected = object(stored)?;
        projected.insert("event".into(),(*projected_event).clone());
        let projected = Arc::new(Value::Object(projected));
        remember(&mut self.projected_stored_events,stored,&projected);
        Ok(projected)
    }

    /// Input is already projected for wire, as required by upstream.
    pub fn thread_stream_event_item(&mut self, stored: &Item) -> Result<Item> {
        if let Some(cached) = cached(&mut self.thread_stream_events,stored) { return Ok(cached); }
        let item = Arc::new(json!({"kind":"event","sequence":stored["sequence"],"event":stored["event"]}));
        remember(&mut self.thread_stream_events,stored,&item);
        Ok(item)
    }
}

pub fn project_context_handoff_for_wire(handoff: &Value) -> Result<Value> {
    let mut projected = object(handoff)?;
    projected.remove("history");
    projected.remove("delivery");
    projected.insert("summaryText".into(),json!(""));
    Ok(Value::Object(projected))
}

const LIVE_DEDUPE_MEMORY: usize = 32;

#[derive(Default)]
struct Memory(VecDeque<(String,Value)>);

impl Memory {
    fn remember(&mut self, id: &str, value: Value) {
        self.0.retain(|(key,_)| key != id);
        self.0.push_back((id.into(),value));
        if self.0.len() > LIVE_DEDUPE_MEMORY { self.0.pop_front(); }
    }
    fn get(&self, id: &str) -> Option<&Value> {
        self.0.iter().find(|(key,_)| key == id).map(|(_,value)| value)
    }
}

#[derive(Default)]
pub struct LiveBodyDeduper {
    messages: Memory,
    checkpoints: Memory,
    subagents: Memory,
}

impl BodySource for LiveBodyDeduper {
    fn message_text(&self, id: &str) -> Option<&str> { self.messages.get(id).and_then(Value::as_str) }
    fn checkpoint_files(&self, id: &str) -> Option<&Value> { self.checkpoints.get(id) }
    fn subagent(&self, id: &str) -> Option<&Value> { self.subagents.get(id) }
}

fn record_id(value: &Value) -> Result<&str> {
    value["id"].as_str().ok_or(ProjectionError::InvalidContract("record id"))
}

impl LiveBodyDeduper {
    pub fn apply(&mut self, item: &Item) -> Result<Item> {
        if item["kind"] == "snapshot" {
            let projection = &item["projection"];
            for (field,memory,body) in [("messages",&mut self.messages,Some("text")),("checkpoints",&mut self.checkpoints,Some("files")),("subagents",&mut self.subagents,None)] {
                for record in projection[field].as_array().ok_or(ProjectionError::InvalidContract("snapshot records"))? {
                    memory.remember(record_id(record)?,body.map(|field| record[field].clone()).unwrap_or_else(|| record.clone()));
                }
            }
            return Ok(item.clone());
        }
        if item["kind"] != "event" { return Ok(item.clone()); }
        let event = &item["event"];
        let original = Arc::new(event["payload"].clone());
        let payload = match event["type"].as_str() {
            Some("message.updated") => { self.messages.remember(record_id(&original)?,original["text"].clone()); return Ok(item.clone()); }
            Some("checkpoint.captured") => { self.checkpoints.remember(record_id(&original)?,original["files"].clone()); return Ok(item.clone()); }
            Some("subagent.updated") => {
                let id = record_id(&original)?;
                let next = dedupe_subagent_bodies(original.clone(),self.subagents.get(id));
                self.subagents.remember(id,(*original).clone());
                next
            }
            Some("turn-item.updated") => dedupe_turn_item_bodies(original.clone(),self),
            _ => return Ok(item.clone()),
        };
        if Arc::ptr_eq(&original,&payload) { return Ok(item.clone()); }
        let mut next = (**item).clone();
        next["event"]["payload"] = (*payload).clone();
        Ok(Arc::new(next))
    }
}
