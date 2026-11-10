//! Port of apps/server/src/orchestration-v2/WireProjection.ts from T3 Code
//! 521ac7c218b3faa74281fb72fc36ce74a0951fb3 (MIT). Record names and fields are
//! upstream's. The projector owns the upstream WeakMap caches; the live deduper
//! is per subscription, after coalescing, just as in the original module.

use std::borrow::Cow;
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Weak};

use serde_json::{Map, Value, json};

use crate::deduped_bodies::{BodySource, dedupe_subagent_bodies, dedupe_turn_item_bodies};
use crate::js;
use crate::tool_output::{self, ToolOutputError};
use crate::types::{Item, Projection, ThreadProjection};

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
#[derive(Default)]
struct Cache {
    entries: HashMap<usize, (Weak<Value>, Item)>,
    cleanup: VecDeque<usize>,
}

fn cached(cache: &mut Cache, source: &Item) -> Option<Item> {
    // Weak keys cannot notify us when the final source owner drops. Rotate a
    // bounded number on each access instead of turning N lookups into N² work.
    // An idle cache retains expired results until reused or its projector drops.
    for _ in 0..cache.cleanup.len().min(8) {
        let key = cache.cleanup.pop_front().expect("bounded by queue length");
        if cache.entries.get(&key).is_some_and(|(source, _)| source.strong_count() != 0) {
            cache.cleanup.push_back(key);
        } else {
            cache.entries.remove(&key);
        }
    }
    cache.entries.get(&(Arc::as_ptr(source) as usize)).and_then(|(key, value)| {
        key.upgrade().filter(|key| Arc::ptr_eq(key, source)).map(|_| value.clone())
    })
}

fn remember(cache: &mut Cache, source: &Item, projected: &Item) {
    let key = Arc::as_ptr(source) as usize;
    if cache.entries.insert(key, (Arc::downgrade(source), projected.clone())).is_none() {
        cache.cleanup.push_back(key);
    }
}

/// Model JS rest/spread without copying bodies that are removed or overwritten.
/// Overwrites keep an existing key's position; removed/reintroduced keys append.
fn object_with(
    value: &Value,
    omitted: &[&str],
    replacements: impl IntoIterator<Item = (&'static str, Value)>,
) -> Result<Map<String, Value>> {
    let source = value.as_object().ok_or(ProjectionError::InvalidContract("record must be an object"))?;
    let mut replacements: Map<String, Value> = replacements.into_iter().map(|(key, value)| (key.into(), value)).collect();
    let mut projected = Map::new();
    for (key, value) in source {
        if omitted.contains(&key.as_str()) { continue; }
        projected.insert(key.clone(), replacements.shift_remove(key).unwrap_or_else(|| value.clone()));
    }
    projected.extend(replacements);
    Ok(projected)
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

fn truncated_field(source: &Value, field: &'static str, limit: usize) -> Result<Option<Value>> {
    Ok(optional_string(source, field)?.map(|text| json!(truncate_detail(text, limit))))
}

pub fn bound_file_change_for_storage(item: &Item) -> Result<Item> {
    if item["type"] != "file_change" { return Ok(item.clone()); }
    let mut replacements = Vec::new();
    for field in ["diffStr", "oldStr", "newStr"] {
        if let Some(value) = truncated_field(item, field, MAX_DETAIL_STRING_BYTES)? {
            replacements.push((field, value));
        }
    }
    let bounded = Value::Object(object_with(item, &[], replacements)?);
    Ok(if bounded == **item { item.clone() } else { Arc::new(bounded) })
}

fn is_space(unit: u16) -> bool {
    char::from_u32(u32::from(unit)).is_some_and(|ch| {
        let mut encoded = [0;4];
        js::trim(ch.encode_utf8(&mut encoded)).is_empty()
    })
}

fn summarize_dynamic_value(value: &Value) -> Result<Value> {
    let serialized = if let Some(text) = value.as_str().filter(|text| text.encode_utf16().take(MAX_DYNAMIC_VALUE_BYTES + 1).count() > MAX_DYNAMIC_VALUE_BYTES) {
        Cow::Borrowed(text)
    } else {
        let encoded = js::stringify(value);
        if encoded.len() <= MAX_DYNAMIC_VALUE_BYTES { return Ok(value.clone()); }
        value.as_str().map(Cow::Borrowed).unwrap_or(Cow::Owned(encoded))
    };
    let mut units = serialized.encode_utf16().skip_while(|unit| is_space(*unit)).peekable();
    if units.peek().is_none() {
        return Ok(json!({"summary":"Large tool output","truncated":true}));
    }
    let mut line = Vec::new();
    let mut pending_space = false;
    for unit in units {
        if unit == 10 { break; }
        if is_space(unit) { pending_space = true; continue; }
        if pending_space { line.push(32); }
        line.push(unit);
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

fn needs_checkpoint_cap(value: &Value) -> Result<bool> {
    let files = value["files"].as_array().ok_or(ProjectionError::InvalidContract("files"))?;
    Ok(files.len() > MAX_WIRE_CHECKPOINT_FILES && value.get("filesTotal").is_none())
}

fn capped_checkpoint_record(value: &Value) -> Result<Option<Value>> {
    if !needs_checkpoint_cap(value)? { return Ok(None); }
    let files = value["files"].as_array().expect("validated checkpoint files");
    let mut additions = 0.0;
    let mut deletions = 0.0;
    for file in files {
        additions += file["additions"].as_f64().ok_or(ProjectionError::InvalidContract("additions"))?;
        deletions += file["deletions"].as_f64().ok_or(ProjectionError::InvalidContract("deletions"))?;
    }
    Ok(Some(Value::Object(object_with(value, &[], [
        ("files", json!(&files[..MAX_WIRE_CHECKPOINT_FILES])),
        ("filesTotal", json!({"count":files.len(),"additions":number(additions),"deletions":number(deletions)})),
    ])?)))
}

fn subagent_fields(value: &Value, limit: usize) -> Result<Vec<(&'static str, Value)>> {
    let mut fields = vec![("prompt", json!(truncate_detail(optional_string(value,"prompt")?.unwrap_or(""),limit)))];
    if let Some(progress) = truncated_field(value,"progress",limit)? { fields.push(("progress",progress)); }
    fields.push(("result",if value["result"].is_null() { Value::Null }
        else { truncated_field(value,"result",limit)?.unwrap_or(Value::Null) }));
    Ok(fields)
}

// A borrowed nested JSON record has no separate Arc identity. Returning None
// lets the owning public entry point reuse its existing object when unchanged.
fn turn_item_record_for_wire(item: &Value) -> Result<Option<Value>> {
    let kind = item["type"].as_str().ok_or(ProjectionError::InvalidContract("type"))?;
    let projected = match kind {
        "checkpoint" => return capped_checkpoint_record(item),
        "handoff" => object_with(item,&["summary"],[])?,
        "command_execution" => {
            let output = optional_string(item,"output")?;
            let failed = if let Some(exit_code) = item.get("exitCode") {
                exit_code.as_f64() != Some(0.0)
            } else {
                item["outputIndicatesFailure"] == true || output.is_some_and(|text| {
                    // JS slices UTF-16 units. A dangling final surrogate cannot
                    // match the ASCII failure patterns, so exclude that unit.
                    let prefix = js::prefix_utf16(text,MAX_DETAIL_STRING_BYTES)
                        .or_else(|| js::prefix_utf16(text,MAX_DETAIL_STRING_BYTES-1)).unwrap_or(text);
                    tool_output::tool_output_indicates_failure(prefix)
                })
            };
            let mut fields = Vec::new();
            if failed { fields.push(("outputIndicatesFailure",json!(true))); }
            if output.is_some_and(|text| !js::trim(text).is_empty()) { fields.push(("outputOmitted",json!(true))); }
            object_with(item,&["output"],fields)?
        }
        "file_change" => {
            let diff = if item["status"] == "failed" {
                optional_string(item,"diffStr")?.filter(|text| !js::trim(text).is_empty())
                    .map(|text| ("diffStr",json!(truncate_detail(text,MAX_DETAIL_STRING_BYTES))))
            } else { None };
            object_with(item,&["diffStr","oldStr","newStr"],diff)?
        }
        "subagent" => object_with(item,&[],subagent_fields(item,MAX_DETAIL_STRING_BYTES)?)?,
        "dynamic_tool" => {
            let mut fields = Vec::new();
            if let Some(input) = item.get("input") { fields.push(("input",summarize_dynamic_value(input)?)); }
            if let Some(output) = tool_output::compact_dynamic_tool_output(item.get("output"))? { fields.push(("output",output)); }
            if has_dynamic_value(item.get("output")) { fields.push(("outputOmitted",json!(true))); }
            object_with(item,&["output"],fields)?
        }
        _ => return Ok(None),
    };
    Ok(Some(Value::Object(projected)))
}

fn domain_event_record_for_wire(event: &Value) -> Result<Option<Value>> {
    let payload = &event["payload"];
    let projected = match event["type"].as_str() {
        Some("turn-item.updated") => turn_item_record_for_wire(payload)?.unwrap_or_else(|| payload.clone()),
        Some("context-handoff.updated") => project_context_handoff_for_wire(payload)?,
        Some("checkpoint.captured") => capped_checkpoint_record(payload)?.unwrap_or_else(|| payload.clone()),
        _ => return Ok(None),
    };
    Ok(Some(Value::Object(object_with(event,&[],[("payload",projected)])?)))
}

#[derive(Default)]
pub struct WireProjector {
    capped_checkpoint_files: Cache,
    projected_stored_events: Cache,
    thread_stream_events: Cache,
}

impl WireProjector {
    fn cap_checkpoint_files(&mut self, value: &Item) -> Result<Item> {
        if !needs_checkpoint_cap(value)? { return Ok(value.clone()); }
        if let Some(capped) = cached(&mut self.capped_checkpoint_files, value) { return Ok(capped); }
        let capped = Arc::new(capped_checkpoint_record(value)?.expect("checkpoint needs cap"));
        remember(&mut self.capped_checkpoint_files, value, &capped);
        Ok(capped)
    }

    pub fn project_turn_item_for_wire(&mut self, item: &Item) -> Result<Item> {
        if item["type"] == "checkpoint" { return self.cap_checkpoint_files(item); }
        Ok(turn_item_record_for_wire(item)?.map(Arc::new).unwrap_or_else(|| item.clone()))
    }

    pub fn project_turn_item_for_detail(&mut self, item: &Item) -> Result<Item> {
        let kind = item["type"].as_str().ok_or(ProjectionError::InvalidContract("type"))?;
        let projected = match kind {
            "command_execution" => {
                let mut fields = vec![("input",json!(truncate_detail(optional_string(item,"input")?.unwrap_or(""),MAX_ON_DEMAND_BYTES)))];
                if let Some(output) = truncated_field(item,"output",MAX_ON_DEMAND_BYTES)? { fields.push(("output",output)); }
                object_with(item,&[],fields)?
            }
            "dynamic_tool" => {
                let mut fields = Vec::new();
                if let Some(input) = bound_dynamic_value(item.get("input")) { fields.push(("input",input)); }
                if let Some(output) = bound_tool_output(item.get("output")) { fields.push(("output",output)); }
                object_with(item,&[],fields)?
            }
            "subagent" => object_with(item,&[],subagent_fields(item,MAX_ON_DEMAND_BYTES)?)?,
            "file_change" => {
                let mut fields = Vec::new();
                if let Some(diff) = truncated_field(item,"diffStr",MAX_ON_DEMAND_BYTES)? { fields.push(("diffStr",diff)); }
                if optional_string(item,"diffStr")?.is_none_or(|text| js::trim(text).is_empty()) {
                    for field in ["oldStr","newStr"] {
                        if let Some(value) = truncated_field(item,field,MAX_ON_DEMAND_BYTES)? { fields.push((field,value)); }
                    }
                }
                object_with(item,&["diffStr","oldStr","newStr"],fields)?
            }
            "handoff" => return self.project_turn_item_for_wire(item),
            _ => return Ok(item.clone()),
        };
        Ok(Arc::new(Value::Object(projected)))
    }

    pub fn cap_projection_checkpoint_files_for_wire(&mut self, projection: &Projection) -> Result<Projection> {
        let mut changed = false;
        let checkpoints = projection.checkpoints.iter().map(|checkpoint| {
            Ok(match capped_checkpoint_record(checkpoint)? {
                Some(capped) => { changed = true; Cow::Owned(capped) }
                None => Cow::Borrowed(checkpoint),
            })
        }).collect::<Result<Vec<_>>>()?;
        let mut turn_items = projection.turn_items.clone();
        for item in &mut turn_items {
            if item["type"] == "checkpoint" {
                let capped = self.cap_checkpoint_files(item)?;
                changed |= !Arc::ptr_eq(item,&capped);
                *item = capped;
            }
        }
        let mut visible_turn_items = projection.visible_turn_items.clone();
        for row in &mut visible_turn_items {
            if row.item["type"] == "checkpoint" {
                let item = self.cap_checkpoint_files(&row.item)?;
                if !Arc::ptr_eq(&row.item,&item) {
                    let mut value = (**row).clone(); value.item = item; *row = Arc::new(value); changed = true;
                }
            }
        }
        if !changed { return Ok(projection.clone()); }
        Ok(Arc::new(ThreadProjection {
            thread:projection.thread.clone(),messages:projection.messages.clone(),
            checkpoints:checkpoints.into_iter().map(Cow::into_owned).collect(),
            subagents:projection.subagents.clone(),context_handoffs:projection.context_handoffs.clone(),
            turn_items,visible_turn_items,extra:projection.extra.clone(),
        }))
    }

    pub fn project_thread_projection_for_wire(&mut self, projection: &Projection) -> Result<Projection> {
        let checkpoints = projection.checkpoints.iter().map(|checkpoint| {
            Ok(capped_checkpoint_record(checkpoint)?.unwrap_or_else(|| checkpoint.clone()))
        }).collect::<Result<Vec<_>>>()?;
        let context_handoffs = projection.context_handoffs.iter()
            .map(project_context_handoff_for_wire).collect::<Result<Vec<_>>>()?;
        let mut by_id = HashMap::<String,Item>::new();
        let turn_items = projection.turn_items.iter().map(|item| self.project_once(item,&mut by_id))
            .collect::<Result<Vec<_>>>()?;
        let visible_turn_items = projection.visible_turn_items.iter().map(|row| {
            let mut value = (**row).clone();
            value.item = self.project_once(&row.item, &mut by_id)?;
            Ok(Arc::new(value))
        }).collect::<Result<Vec<_>>>()?;
        Ok(Arc::new(ThreadProjection {
            thread:projection.thread.clone(),messages:projection.messages.clone(),checkpoints,
            subagents:projection.subagents.clone(),context_handoffs,turn_items,visible_turn_items,
            extra:projection.extra.clone(),
        }))
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
        Ok(domain_event_record_for_wire(event)?.map(Arc::new).unwrap_or_else(|| event.clone()))
    }

    pub fn project_stored_event_for_wire(&mut self, stored: &Item) -> Result<Item> {
        if let Some(cached) = cached(&mut self.projected_stored_events,stored) { return Ok(cached); }
        let Some(event) = domain_event_record_for_wire(&stored["event"])? else { return Ok(stored.clone()); };
        let projected = Arc::new(Value::Object(object_with(stored,&[],[("event",event)])?));
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
    Ok(Value::Object(object_with(handoff,&["history","delivery"],[("summaryText",json!(""))])?))
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
    fn remember_subagent(&mut self, record: &Value) -> Result<()> {
        // Upstream keeps this Pick, not the record's progress or other bodies.
        let bodies = ["prompt", "result"].into_iter().filter_map(|field| {
            record.get(field).map(|value| (field.to_owned(), value.clone()))
        }).collect();
        self.subagents.remember(record_id(record)?, Value::Object(bodies));
        Ok(())
    }

    pub fn apply(&mut self, item: &Item) -> Result<Item> {
        if item["kind"] == "snapshot" {
            let projection = &item["projection"];
            for (field,memory,body) in [("messages",&mut self.messages,"text"),("checkpoints",&mut self.checkpoints,"files")] {
                for record in projection[field].as_array().ok_or(ProjectionError::InvalidContract("snapshot records"))? {
                    memory.remember(record_id(record)?,record[body].clone());
                }
            }
            for record in projection["subagents"].as_array().ok_or(ProjectionError::InvalidContract("snapshot records"))? {
                self.remember_subagent(record)?;
            }
            return Ok(item.clone());
        }
        if item["kind"] != "event" { return Ok(item.clone()); }
        let event = &item["event"];
        let original = &event["payload"];
        match event["type"].as_str() {
            Some("message.updated") => { self.messages.remember(record_id(original)?,original["text"].clone()); return Ok(item.clone()); }
            Some("checkpoint.captured") => { self.checkpoints.remember(record_id(original)?,original["files"].clone()); return Ok(item.clone()); }
            Some("subagent.updated" | "turn-item.updated") => {}
            _ => return Ok(item.clone()),
        }
        let original = Arc::new(original.clone());
        let payload = match event["type"].as_str() {
            Some("subagent.updated") => {
                let id = record_id(&original)?;
                let next = dedupe_subagent_bodies(original.clone(),self.subagents.get(id));
                self.remember_subagent(&original)?;
                next
            }
            Some("turn-item.updated") => dedupe_turn_item_bodies(original.clone(),self),
            _ => return Ok(item.clone()),
        };
        if Arc::ptr_eq(&original,&payload) { return Ok(item.clone()); }
        let event = Value::Object(object_with(event,&[],[("payload",Arc::unwrap_or_clone(payload))])?);
        Ok(Arc::new(Value::Object(object_with(item,&[],[("event",event)])?)))
    }
}
