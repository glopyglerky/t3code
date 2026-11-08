use std::io::{self, BufRead};
use t3_code_ls_core::json::{self, Value};
use t3_code_ls_core::provider_recovery::{
    self, Completion, EmittedUpdate, Event, EventPayload, NativeSubagentEnd, Node, NodeKind,
    Projection, ProviderTurn, RootTurnsEnd, Subagent, SubagentOrigin, TerminalStatus, TurnItem,
    TurnItemKind, UpdateKind, WorkStatus,
};

type FixtureResult<T> = Result<T, &'static str>;

fn field<'a>(value: &'a Value, key: &str) -> FixtureResult<&'a Value> {
    value.get(key).ok_or("missing fixture field")
}

fn text(value: &Value) -> FixtureResult<String> {
    value
        .as_str()
        .map(str::to_string)
        .ok_or("invalid fixture string")
}

fn timestamp(value: &Value) -> FixtureResult<i64> {
    value.as_i64().ok_or("invalid fixture timestamp")
}

fn nullable<T>(
    value: &Value,
    read: impl FnOnce(&Value) -> FixtureResult<T>,
) -> FixtureResult<Option<T>> {
    if *value == Value::Null {
        Ok(None)
    } else {
        read(value).map(Some)
    }
}

fn array(value: &Value) -> FixtureResult<&[Value]> {
    value.as_array().ok_or("invalid fixture array")
}

fn object(entries: Vec<(&str, Value)>) -> Value {
    Value::Object(
        entries
            .into_iter()
            .map(|(key, value)| (key.to_string(), value))
            .collect(),
    )
}

fn remainder(value: &Value, keys: &[&str]) -> FixtureResult<Value> {
    match value {
        Value::Object(entries) => Ok(Value::Object(
            entries
                .iter()
                .filter(|(key, _)| !keys.contains(&key.as_str()))
                .cloned()
                .collect(),
        )),
        _ => Err("invalid fixture object"),
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

fn status(value: &Value) -> FixtureResult<WorkStatus> {
    value
        .as_str()
        .and_then(WorkStatus::from_wire)
        .ok_or("invalid fixture work status")
}

fn subagent(value: &Value) -> FixtureResult<Subagent<Value, i64>> {
    Ok(Subagent {
        id: text(field(value, "id")?)?,
        thread_id: text(field(value, "threadId")?)?,
        run_id: nullable(field(value, "runId")?, text)?,
        origin: match field(value, "origin")?.as_str() {
            Some("provider_native") => SubagentOrigin::ProviderNative,
            Some("app_owned") => SubagentOrigin::AppOwned,
            _ => return Err("invalid fixture subagent origin"),
        },
        driver: text(field(value, "driver")?)?,
        provider_instance_id: text(field(value, "providerInstanceId")?)?,
        status: status(field(value, "status")?)?,
        completed_at: nullable(field(value, "completedAt")?, timestamp)?,
        updated_at: timestamp(field(value, "updatedAt")?)?,
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

fn node(value: &Value) -> FixtureResult<Node<Value, i64>> {
    Ok(Node {
        id: text(field(value, "id")?)?,
        kind: match field(value, "kind")?.as_str() {
            Some("root_turn") => NodeKind::RootTurn,
            Some(kind) => NodeKind::Other(kind.to_string()),
            _ => return Err("invalid fixture node kind"),
        },
        run_id: nullable(field(value, "runId")?, text)?,
        status: status(field(value, "status")?)?,
        completed_at: nullable(field(value, "completedAt")?, timestamp)?,
        payload: remainder(value, &["id", "kind", "runId", "status", "completedAt"])?,
    })
}

fn item(value: &Value) -> FixtureResult<TurnItem<Value, i64>> {
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
                    .ok_or("invalid streaming flag")?,
            }
        }
        Some("assistant_message") => {
            keys.push("streaming");
            TurnItemKind::AssistantMessage {
                streaming: field(value, "streaming")?
                    .as_bool()
                    .ok_or("invalid streaming flag")?,
            }
        }
        Some("command_execution") => TurnItemKind::CommandExecution,
        Some("dynamic_tool") => TurnItemKind::DynamicTool,
        Some(kind) => TurnItemKind::Other(kind.to_string()),
        _ => return Err("invalid fixture item type"),
    };
    Ok(TurnItem {
        id: text(field(value, "id")?)?,
        node_id: nullable(field(value, "nodeId")?, text)?,
        run_id: nullable(field(value, "runId")?, text)?,
        kind,
        status: status(field(value, "status")?)?,
        completed_at: nullable(field(value, "completedAt")?, timestamp)?,
        updated_at: timestamp(field(value, "updatedAt")?)?,
        payload: remainder(value, &keys)?,
    })
}

fn provider_turn(value: &Value) -> FixtureResult<ProviderTurn<Value, i64>> {
    Ok(ProviderTurn {
        id: text(field(value, "id")?)?,
        node_id: text(field(value, "nodeId")?)?,
        run_attempt_id: nullable(field(value, "runAttemptId")?, text)?,
        status: status(field(value, "status")?)?,
        completed_at: nullable(field(value, "completedAt")?, timestamp)?,
        payload: remainder(
            value,
            &["id", "nodeId", "runAttemptId", "status", "completedAt"],
        )?,
    })
}

fn time_value(value: i64) -> Value {
    Value::Number(value.to_string())
}
fn optional_time(value: Option<i64>) -> Value {
    value.map_or(Value::Null, time_value)
}
fn optional_text(value: &Option<String>) -> Value {
    value.clone().map_or(Value::Null, Value::String)
}

fn event_value(event: &Event<Value, i64>) -> Value {
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
                    ("completedAt", optional_time(subagent.completed_at)),
                    ("updatedAt", time_value(subagent.updated_at)),
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
                    ("completedAt", optional_time(node.completed_at)),
                ],
            ),
        ),
        EventPayload::TurnItem(item) => {
            let mut fields = vec![
                ("id", Value::String(item.id.clone())),
                ("nodeId", optional_text(&item.node_id)),
                ("runId", optional_text(&item.run_id)),
                ("status", Value::String(item.status.as_wire().to_string())),
                ("completedAt", optional_time(item.completed_at)),
                ("updatedAt", time_value(item.updated_at)),
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
                    ("completedAt", optional_time(turn.completed_at)),
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
        ("occurredAt", time_value(event.occurred_at)),
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

fn evaluate(input: &Value) -> FixtureResult<Value> {
    let source = field(input, "projection")?;
    let projection = Projection {
        subagents: array(field(source, "subagents")?)?
            .iter()
            .map(subagent)
            .collect::<FixtureResult<Vec<_>>>()?,
        nodes: array(field(source, "nodes")?)?
            .iter()
            .map(node)
            .collect::<FixtureResult<Vec<_>>>()?,
        turn_items: array(field(source, "turnItems")?)?
            .iter()
            .map(item)
            .collect::<FixtureResult<Vec<_>>>()?,
        provider_turns: array(field(source, "providerTurns")?)?
            .iter()
            .map(provider_turn)
            .collect::<FixtureResult<Vec<_>>>()?,
    };
    let before = projection.clone();
    let mut emitted = Vec::new();
    for event in array(field(input, "emitted")?)? {
        let kind = match field(event, "type")?.as_str() {
            Some("subagent.updated") => UpdateKind::Subagent,
            Some("node.updated") => UpdateKind::Node,
            Some("turn-item.updated") => UpdateKind::TurnItem,
            Some("provider-turn.updated") => UpdateKind::ProviderTurn,
            _ => continue,
        };
        emitted.push(EmittedUpdate {
            kind,
            id: text(field(field(event, "payload")?, "id")?)?,
        });
    }
    let completion = Completion {
        status: match field(input, "status")?.as_str() {
            Some("completed") => TerminalStatus::Completed,
            Some("failed") => TerminalStatus::Failed,
            Some("interrupted") => TerminalStatus::Interrupted,
            Some("cancelled") => TerminalStatus::Cancelled,
            _ => return Err("invalid terminal status"),
        },
        completed_at: input.get("completedAt").map(timestamp).transpose()?,
        now: timestamp(field(input, "now")?)?,
    };
    let failure_at = input
        .get("failureAt")
        .map(|value| value.as_u64().ok_or("invalid failure index"))
        .transpose()?;
    let allocation_start = input
        .get("allocationStart")
        .map(|value| value.as_u64().ok_or("invalid allocation start"))
        .transpose()?
        .unwrap_or(0);
    let mut allocations = 0u64;
    let mut allocate = || {
        let attempt = allocations;
        allocations += 1;
        if failure_at == Some(attempt) {
            Err(attempt)
        } else {
            Ok(format!("event:recovery:{}", allocation_start + attempt))
        }
    };
    let result = match field(input, "operation")?.as_str() {
        Some("orphan") => provider_recovery::end_orphaned_native_subagent(
            &projection,
            NativeSubagentEnd {
                subagent_id: field(input, "subagentId")?
                    .as_str()
                    .ok_or("invalid subagent id")?,
                completion,
                result: input
                    .get("result")
                    .map(|value| nullable(value, text))
                    .transpose()?,
            },
            &emitted,
            &mut allocate,
        ),
        Some("roots") => provider_recovery::end_runless_root_turns(
            &projection,
            RootTurnsEnd {
                thread_id: text(field(input, "threadId")?)?,
                provider_instance_id: text(field(input, "providerInstanceId")?)?,
                completion,
            },
            &emitted,
            &mut allocate,
        ),
        _ => return Err("invalid operation"),
    };
    assert_eq!(projection, before, "planner mutated its projection");
    let outcome = match result {
        Ok(events) => (
            "events",
            Value::Array(events.iter().map(event_value).collect()),
        ),
        Err(attempt) => (
            "error",
            object(vec![
                ("tag", Value::String("AllocationFailure".to_string())),
                ("attempt", Value::Number(attempt.to_string())),
            ]),
        ),
    };
    Ok(object(vec![
        outcome,
        ("allocations", Value::Number(allocations.to_string())),
    ]))
}

fn main() {
    for input in io::stdin().lock().lines() {
        let input = input.expect("read recovery fixture");
        let value = json::parse(&input).expect("parse recovery fixture");
        println!(
            "{}",
            json::stringify(&evaluate(&value).expect("evaluate recovery fixture"))
        );
    }
}
