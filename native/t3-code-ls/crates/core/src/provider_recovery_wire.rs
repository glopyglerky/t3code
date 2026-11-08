//! Bounded recovery protocol and lossless JSON record conversion.
//! Copyright (c) 2026 T3 Tools Inc. Distributed under the repository's MIT license.

use crate::json::{self, Value};
use crate::provider_recovery::{
    self, Completion, EmittedUpdate, Event, EventPayload, NativeSubagentEnd, Node, NodeKind,
    Projection, ProviderTurn, RootTurnsEnd, Subagent, SubagentOrigin, TerminalStatus, TurnItem,
    TurnItemKind, UpdateKind, WorkStatus,
};
use std::collections::HashSet;

pub const MAX_FRAME_BYTES: usize = 128 * 1024 * 1024;
pub const MAX_EVENTS: usize = 65_536;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    InvalidRequest,
    DuplicateKey,
    InputLimit,
    EventLimit,
    OutputLimit,
    Io,
}

impl Error {
    pub fn message(self) -> &'static str {
        match self {
            Self::InvalidRequest => "invalid request",
            Self::DuplicateKey => "duplicate object key",
            Self::InputLimit => "input limit exceeded",
            Self::EventLimit => "event limit exceeded",
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

fn nullable<T>(value: &Value, read: impl FnOnce(&Value) -> WireResult<T>) -> WireResult<Option<T>> {
    if *value == Value::Null {
        Ok(None)
    } else {
        read(value).map(Some)
    }
}

fn array(value: &Value) -> WireResult<&[Value]> {
    value.as_array().ok_or(Error::InvalidRequest)
}

fn object(entries: Vec<(&str, Value)>) -> Value {
    Value::Object(
        entries
            .into_iter()
            .map(|(key, value)| (key.to_string(), value))
            .collect(),
    )
}

fn remainder(value: &Value, keys: &[&str]) -> WireResult<Value> {
    match value {
        Value::Object(entries) => Ok(Value::Object(
            entries
                .iter()
                .filter(|(key, _)| !keys.contains(&key.as_str()))
                .cloned()
                .collect(),
        )),
        _ => Err(Error::InvalidRequest),
    }
}

fn append(payload: &Value, fields: Vec<(&str, Value)>) -> Value {
    let Value::Object(entries) = payload else {
        panic!("fixture object expected")
    };
    let mut entries = entries.clone();
    entries.extend(
        fields
            .into_iter()
            .map(|(key, value)| (key.to_string(), value)),
    );
    Value::Object(entries)
}

fn status(value: &Value) -> WireResult<WorkStatus> {
    value
        .as_str()
        .and_then(WorkStatus::from_wire)
        .ok_or(Error::InvalidRequest)
}

fn subagent<Time>(
    value: &Value,
    read_time: fn(&Value) -> WireResult<Time>,
) -> WireResult<Subagent<Value, Time>> {
    Ok(Subagent {
        id: text(field(value, "id")?)?,
        thread_id: text(field(value, "threadId")?)?,
        run_id: nullable(field(value, "runId")?, text)?,
        origin: match field(value, "origin")?.as_str() {
            Some("provider_native") => SubagentOrigin::ProviderNative,
            Some("app_owned") => SubagentOrigin::AppOwned,
            _ => return Err(Error::InvalidRequest),
        },
        driver: text(field(value, "driver")?)?,
        provider_instance_id: text(field(value, "providerInstanceId")?)?,
        status: status(field(value, "status")?)?,
        completed_at: nullable(field(value, "completedAt")?, read_time)?,
        updated_at: read_time(field(value, "updatedAt")?)?,
        result: nullable(field(value, "result")?, text)?,
        payload: remainder(
            value,
            &[
                "id",
                "threadId",
                "runId",
                "origin",
                "driver",
                "providerInstanceId",
                "status",
                "completedAt",
                "updatedAt",
                "result",
            ],
        )?,
    })
}

fn node<Time>(
    value: &Value,
    read_time: fn(&Value) -> WireResult<Time>,
) -> WireResult<Node<Value, Time>> {
    Ok(Node {
        id: text(field(value, "id")?)?,
        kind: match field(value, "kind")?.as_str() {
            Some("root_turn") => NodeKind::RootTurn,
            Some(kind) => NodeKind::Other(kind.to_string()),
            _ => return Err(Error::InvalidRequest),
        },
        run_id: nullable(field(value, "runId")?, text)?,
        status: status(field(value, "status")?)?,
        completed_at: nullable(field(value, "completedAt")?, read_time)?,
        payload: remainder(value, &["id", "kind", "runId", "status", "completedAt"])?,
    })
}

fn item<Time>(
    value: &Value,
    read_time: fn(&Value) -> WireResult<Time>,
) -> WireResult<TurnItem<Value, Time>> {
    let mut keys = vec![
        "id",
        "nodeId",
        "runId",
        "type",
        "status",
        "completedAt",
        "updatedAt",
    ];
    let kind = match field(value, "type")?.as_str() {
        Some("subagent") => {
            keys.extend(["subagentId", "result"]);
            TurnItemKind::Subagent {
                subagent_id: text(field(value, "subagentId")?)?,
                result: nullable(field(value, "result")?, text)?,
            }
        }
        Some("reasoning") => {
            keys.push("streaming");
            TurnItemKind::Reasoning {
                streaming: field(value, "streaming")?
                    .as_bool()
                    .ok_or(Error::InvalidRequest)?,
            }
        }
        Some("assistant_message") => {
            keys.push("streaming");
            TurnItemKind::AssistantMessage {
                streaming: field(value, "streaming")?
                    .as_bool()
                    .ok_or(Error::InvalidRequest)?,
            }
        }
        Some("command_execution") => TurnItemKind::CommandExecution,
        Some("dynamic_tool") => TurnItemKind::DynamicTool,
        Some(kind) => TurnItemKind::Other(kind.to_string()),
        _ => return Err(Error::InvalidRequest),
    };
    Ok(TurnItem {
        id: text(field(value, "id")?)?,
        node_id: nullable(field(value, "nodeId")?, text)?,
        run_id: nullable(field(value, "runId")?, text)?,
        kind,
        status: status(field(value, "status")?)?,
        completed_at: nullable(field(value, "completedAt")?, read_time)?,
        updated_at: read_time(field(value, "updatedAt")?)?,
        payload: remainder(value, &keys)?,
    })
}

fn provider_turn<Time>(
    value: &Value,
    read_time: fn(&Value) -> WireResult<Time>,
) -> WireResult<ProviderTurn<Value, Time>> {
    Ok(ProviderTurn {
        id: text(field(value, "id")?)?,
        node_id: text(field(value, "nodeId")?)?,
        run_attempt_id: nullable(field(value, "runAttemptId")?, text)?,
        status: status(field(value, "status")?)?,
        completed_at: nullable(field(value, "completedAt")?, read_time)?,
        payload: remainder(
            value,
            &["id", "nodeId", "runAttemptId", "status", "completedAt"],
        )?,
    })
}

fn optional_time<Time>(value: &Option<Time>, write_time: fn(&Time) -> Value) -> Value {
    value.as_ref().map_or(Value::Null, write_time)
}
fn optional_text(value: &Option<String>) -> Value {
    value.clone().map_or(Value::Null, Value::String)
}

pub fn encode_event<Time>(event: &Event<Value, Time>, write_time: fn(&Time) -> Value) -> Value {
    let (kind, payload) = match &event.payload {
        EventPayload::Subagent(subagent) => (
            "subagent.updated",
            append(
                &subagent.payload,
                vec![
                    ("id", Value::String(subagent.id.clone())),
                    ("threadId", Value::String(subagent.thread_id.clone())),
                    ("runId", optional_text(&subagent.run_id)),
                    (
                        "origin",
                        Value::String(
                            match subagent.origin {
                                SubagentOrigin::ProviderNative => "provider_native",
                                SubagentOrigin::AppOwned => "app_owned",
                            }
                            .to_string(),
                        ),
                    ),
                    ("driver", Value::String(subagent.driver.clone())),
                    (
                        "providerInstanceId",
                        Value::String(subagent.provider_instance_id.clone()),
                    ),
                    (
                        "status",
                        Value::String(subagent.status.as_wire().to_string()),
                    ),
                    (
                        "completedAt",
                        optional_time(&subagent.completed_at, write_time),
                    ),
                    ("updatedAt", write_time(&subagent.updated_at)),
                    ("result", optional_text(&subagent.result)),
                ],
            ),
        ),
        EventPayload::Node(node) => (
            "node.updated",
            append(
                &node.payload,
                vec![
                    ("id", Value::String(node.id.clone())),
                    (
                        "kind",
                        Value::String(
                            match &node.kind {
                                NodeKind::RootTurn => "root_turn",
                                NodeKind::Other(kind) => kind,
                            }
                            .to_string(),
                        ),
                    ),
                    ("runId", optional_text(&node.run_id)),
                    ("status", Value::String(node.status.as_wire().to_string())),
                    ("completedAt", optional_time(&node.completed_at, write_time)),
                ],
            ),
        ),
        EventPayload::TurnItem(item) => {
            let mut fields = vec![
                ("id", Value::String(item.id.clone())),
                ("nodeId", optional_text(&item.node_id)),
                ("runId", optional_text(&item.run_id)),
                ("status", Value::String(item.status.as_wire().to_string())),
                ("completedAt", optional_time(&item.completed_at, write_time)),
                ("updatedAt", write_time(&item.updated_at)),
            ];
            let kind = match &item.kind {
                TurnItemKind::Subagent {
                    subagent_id,
                    result,
                } => {
                    fields.push(("subagentId", Value::String(subagent_id.clone())));
                    fields.push(("result", optional_text(result)));
                    "subagent"
                }
                TurnItemKind::Reasoning { streaming } => {
                    fields.push(("streaming", Value::Bool(*streaming)));
                    "reasoning"
                }
                TurnItemKind::AssistantMessage { streaming } => {
                    fields.push(("streaming", Value::Bool(*streaming)));
                    "assistant_message"
                }
                TurnItemKind::CommandExecution => "command_execution",
                TurnItemKind::DynamicTool => "dynamic_tool",
                TurnItemKind::Other(kind) => kind,
            };
            fields.push(("type", Value::String(kind.to_string())));
            ("turn-item.updated", append(&item.payload, fields))
        }
        EventPayload::ProviderTurn(turn) => (
            "provider-turn.updated",
            append(
                &turn.payload,
                vec![
                    ("id", Value::String(turn.id.clone())),
                    ("nodeId", Value::String(turn.node_id.clone())),
                    ("runAttemptId", optional_text(&turn.run_attempt_id)),
                    ("status", Value::String(turn.status.as_wire().to_string())),
                    ("completedAt", optional_time(&turn.completed_at, write_time)),
                ],
            ),
        ),
    };
    let mut fields = vec![
        ("id", Value::String(event.id.clone())),
        ("type", Value::String(kind.to_string())),
        ("threadId", Value::String(event.thread_id.clone())),
        (
            "providerInstanceId",
            Value::String(event.provider_instance_id.clone()),
        ),
        ("nodeId", Value::String(event.node_id.clone())),
        ("occurredAt", write_time(&event.occurred_at)),
        ("payload", payload),
    ];
    if let Some(run_id) = &event.run_id {
        fields.push(("runId", Value::String(run_id.clone())));
    }
    if let EventPayload::Subagent(subagent) = &event.payload {
        fields.push(("driver", Value::String(subagent.driver.clone())));
    }
    object(fields)
}

fn records<'a>(source: &'a Value, key: &str) -> WireResult<&'a [Value]> {
    source
        .get(key)
        .map(array)
        .transpose()
        .map(|value| value.unwrap_or(&[]))
}

pub fn decode_projection<Time>(
    source: &Value,
    read_time: fn(&Value) -> WireResult<Time>,
) -> WireResult<Projection<Value, Time>> {
    Ok(Projection {
        subagents: records(source, "subagents")?
            .iter()
            .map(|value| subagent(value, read_time))
            .collect::<WireResult<Vec<_>>>()?,
        nodes: records(source, "nodes")?
            .iter()
            .map(|value| node(value, read_time))
            .collect::<WireResult<Vec<_>>>()?,
        turn_items: records(source, "turnItems")?
            .iter()
            .map(|value| item(value, read_time))
            .collect::<WireResult<Vec<_>>>()?,
        provider_turns: records(source, "providerTurns")?
            .iter()
            .map(|value| provider_turn(value, read_time))
            .collect::<WireResult<Vec<_>>>()?,
    })
}

pub fn decode_emitted(value: &Value) -> WireResult<Vec<EmittedUpdate>> {
    let mut emitted = Vec::new();
    for event in array(value)? {
        let kind = match field(event, "type")?
            .as_str()
            .ok_or(Error::InvalidRequest)?
        {
            "subagent.updated" => UpdateKind::Subagent,
            "node.updated" => UpdateKind::Node,
            "turn-item.updated" => UpdateKind::TurnItem,
            "provider-turn.updated" => UpdateKind::ProviderTurn,
            _ => continue,
        };
        emitted.push(EmittedUpdate {
            kind,
            id: text(field(field(event, "payload")?, "id")?)?,
        });
    }
    Ok(emitted)
}

pub fn decode_completion<Time>(
    value: &Value,
    read_time: fn(&Value) -> WireResult<Time>,
) -> WireResult<Completion<Time>> {
    Ok(Completion {
        status: match field(value, "status")?.as_str() {
            Some("completed") => TerminalStatus::Completed,
            Some("failed") => TerminalStatus::Failed,
            Some("interrupted") => TerminalStatus::Interrupted,
            Some("cancelled") => TerminalStatus::Cancelled,
            _ => return Err(Error::InvalidRequest),
        },
        now: read_time(field(value, "now")?)?,
        completed_at: value.get("completedAt").map(read_time).transpose()?,
    })
}

fn unique_keys(value: &Value) -> WireResult<()> {
    match value {
        Value::Object(fields) => {
            let mut seen = HashSet::new();
            for (key, value) in fields {
                if !seen.insert(key.as_str()) {
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
        _ => {}
    }
    Ok(())
}

struct RootScope {
    thread_id: String,
    provider_instance_id: String,
}

enum Operation {
    Orphan {
        subagent_id: String,
        result: Option<Option<String>>,
    },
    Roots(RootScope),
    Reconcile(RootScope),
}

struct Request {
    operation: Operation,
    projection: Projection<Value, String>,
    emitted: Vec<EmittedUpdate>,
    completion: Completion<String>,
}

fn decode_request(value: &Value) -> WireResult<Request> {
    unique_keys(value)?;
    if field(value, "version")?.as_u64() != Some(1) {
        return Err(Error::InvalidRequest);
    }
    let operation = field(value, "operation")?
        .as_str()
        .ok_or(Error::InvalidRequest)?;
    let scope_keys: &[&str] = match operation {
        "orphan" => &["subagentId", "result"],
        "roots" | "reconcile" => &["threadId", "providerInstanceId"],
        _ => return Err(Error::InvalidRequest),
    };
    let Value::Object(fields) = value else {
        return Err(Error::InvalidRequest);
    };
    for (key, _) in fields {
        if ![
            "version",
            "operation",
            "projection",
            "emitted",
            "status",
            "now",
            "completedAt",
        ]
        .contains(&key.as_str())
            && !scope_keys.contains(&key.as_str())
        {
            return Err(Error::InvalidRequest);
        }
    }
    let source = field(value, "projection")?;
    array(field(source, "nodes")?)?;
    array(field(source, "turnItems")?)?;
    if operation != "roots" {
        array(field(source, "subagents")?)?;
    }
    if operation != "orphan" {
        array(field(source, "providerTurns")?)?;
    }
    let operation = if operation == "orphan" {
        Operation::Orphan {
            subagent_id: text(field(value, "subagentId")?)?,
            result: value
                .get("result")
                .map(|value| nullable(value, text))
                .transpose()?,
        }
    } else {
        let scope = RootScope {
            thread_id: text(field(value, "threadId")?)?,
            provider_instance_id: text(field(value, "providerInstanceId")?)?,
        };
        if operation == "roots" {
            Operation::Roots(scope)
        } else {
            Operation::Reconcile(scope)
        }
    };
    Ok(Request {
        operation,
        projection: decode_projection(source, text)?,
        emitted: decode_emitted(field(value, "emitted")?)?,
        completion: decode_completion(value, text)?,
    })
}

fn charge(remaining: &mut usize, bytes: usize) -> WireResult<()> {
    *remaining = remaining.checked_sub(bytes).ok_or(Error::OutputLimit)?;
    Ok(())
}

fn quoted_size(value: &str, remaining: &mut usize) -> WireResult<()> {
    charge(remaining, 2)?;
    for character in value.chars() {
        charge(
            remaining,
            match character {
                '"' | '\\' | '\n' | '\r' | '\t' | '\u{0008}' | '\u{000c}' => 2,
                '\u{0000}'..='\u{001f}' => 6,
                _ => character.len_utf8(),
            },
        )?;
    }
    Ok(())
}

// Size before allocating the serialized event, including JSON escaping and UTF-8.
fn encoded_size(value: &Value, remaining: &mut usize) -> WireResult<()> {
    match value {
        Value::Null => charge(remaining, 4),
        Value::Bool(value) => charge(remaining, if *value { 4 } else { 5 }),
        Value::Number(value) => charge(remaining, value.len()),
        Value::String(value) => quoted_size(value, remaining),
        Value::Array(values) => {
            charge(remaining, 2 + values.len().saturating_sub(1))?;
            for value in values {
                encoded_size(value, remaining)?;
            }
            Ok(())
        }
        Value::Object(fields) => {
            charge(remaining, 2 + fields.len().saturating_sub(1))?;
            for (key, value) in fields {
                quoted_size(key, remaining)?;
                charge(remaining, 1)?;
                encoded_size(value, remaining)?;
            }
            Ok(())
        }
    }
}

struct Response {
    json: String,
    events: usize,
}

impl Response {
    fn push(&mut self, event: Event<Value, String>) -> WireResult<EmittedUpdate> {
        let update = match &event.payload {
            EventPayload::Subagent(record) => EmittedUpdate {
                kind: UpdateKind::Subagent,
                id: record.id.clone(),
            },
            EventPayload::Node(record) => EmittedUpdate {
                kind: UpdateKind::Node,
                id: record.id.clone(),
            },
            EventPayload::TurnItem(record) => EmittedUpdate {
                kind: UpdateKind::TurnItem,
                id: record.id.clone(),
            },
            EventPayload::ProviderTurn(record) => EmittedUpdate {
                kind: UpdateKind::ProviderTurn,
                id: record.id.clone(),
            },
        };
        let mut draft = encode_event(&event, |value| Value::String(value.clone()));
        let Value::Object(fields) = &mut draft else {
            return Err(Error::InvalidRequest);
        };
        fields.retain(|(key, _)| key != "id");
        let delimiter = usize::from(self.events != 0);
        let remaining = MAX_FRAME_BYTES
            .checked_sub(self.json.len() + 3 + delimiter)
            .ok_or(Error::OutputLimit)?;
        let mut budget = remaining;
        encoded_size(&draft, &mut budget)?;
        let encoded = json::stringify(&draft);
        if encoded.len() > remaining {
            return Err(Error::OutputLimit);
        }
        if delimiter != 0 {
            self.json.push(',');
        }
        self.json.push_str(&encoded);
        self.events += 1;
        Ok(update)
    }
}

fn visit_orphan(
    projection: &Projection<Value, String>,
    input: NativeSubagentEnd<'_, String>,
    emitted: &mut Vec<EmittedUpdate>,
    allocate: &mut impl FnMut() -> WireResult<String>,
    response: &mut Response,
) -> WireResult<()> {
    let mut updates = Vec::new();
    provider_recovery::visit_orphaned_native_subagent(
        projection,
        input,
        emitted,
        allocate,
        &mut |event| {
            updates.push(response.push(event)?);
            Ok(())
        },
    )?;
    emitted.extend(updates);
    Ok(())
}

/// One request in, one complete JSON response out. No bytes are exposed while
/// planning; a late parse, event-count, or output-limit failure returns no draft.
pub fn respond(frame: &str) -> WireResult<String> {
    if frame.len() > MAX_FRAME_BYTES {
        return Err(Error::InputLimit);
    }
    let body = frame.strip_suffix('\n').ok_or(Error::InvalidRequest)?;
    if body.contains('\n') || body.contains('\r') {
        return Err(Error::InvalidRequest);
    }
    let request = {
        let value = json::parse_with_limits(
            body,
            json::Limits {
                max_bytes: MAX_FRAME_BYTES,
                max_depth: 128,
                max_values: 1_048_576,
            },
        )
        .map_err(|_| Error::InvalidRequest)?;
        decode_request(&value)?
    };
    let Request {
        operation,
        projection,
        mut emitted,
        completion,
    } = request;
    let mut response = Response {
        json: "{\"version\":1,\"events\":[".to_string(),
        events: 0,
    };
    let mut allocations = 0usize;
    let mut allocate = || {
        if allocations >= MAX_EVENTS {
            return Err(Error::EventLimit);
        }
        allocations += 1;
        // IDs are assigned by the authoritative allocator after response validation.
        Ok(String::new())
    };
    let roots = match operation {
        Operation::Orphan {
            subagent_id,
            result,
        } => {
            visit_orphan(
                &projection,
                NativeSubagentEnd {
                    subagent_id: &subagent_id,
                    completion,
                    result,
                },
                &mut emitted,
                &mut allocate,
                &mut response,
            )?;
            None
        }
        Operation::Roots(scope) => Some(RootTurnsEnd {
            thread_id: scope.thread_id,
            provider_instance_id: scope.provider_instance_id,
            completion,
        }),
        Operation::Reconcile(scope) => {
            for subagent in &projection.subagents {
                visit_orphan(
                    &projection,
                    NativeSubagentEnd {
                        subagent_id: &subagent.id,
                        completion: completion.clone(),
                        result: None,
                    },
                    &mut emitted,
                    &mut allocate,
                    &mut response,
                )?;
            }
            Some(RootTurnsEnd {
                thread_id: scope.thread_id,
                provider_instance_id: scope.provider_instance_id,
                completion,
            })
        }
    };
    if let Some(input) = roots {
        provider_recovery::visit_runless_root_turns(
            &projection,
            input,
            &emitted,
            &mut allocate,
            &mut |event| response.push(event).map(|_| ()),
        )?;
    }
    response.json.push_str("]}\n");
    Ok(response.json)
}
