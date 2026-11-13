//! Bounded primary-key lookup queries for the application's projection reader.

use std::collections::{BTreeSet, HashSet};

use super::*;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ProjectionLookup {
    Thread {
        thread_id: String,
    },
    ThreadExists {
        thread_id: String,
    },
    Plan {
        thread_id: String,
        record_id: String,
    },
    RuntimeRequest {
        thread_id: String,
        record_id: String,
    },
    TurnItem {
        thread_id: String,
        record_id: String,
    },
    Node {
        record_id: String,
    },
    Messages {
        thread_id: String,
        record_ids: Vec<String>,
    },
    RunMessage {
        thread_id: String,
        run_id: String,
    },
    Run {
        thread_id: String,
        run_id: String,
    },
    TranscriptByteFloor {
        thread_id: String,
        limit: u64,
    },
    PendingDelegatedCompletion {
        thread_id: String,
    },
    CanStartQueuedRun {
        thread_id: String,
    },
    HeldQueuedRuns {
        thread_id: String,
    },
    MessageCount {
        thread_id: String,
    },
    SubagentCount {
        thread_id: String,
    },
    AppOwnedChildThreads {
        thread_id: String,
    },
    NextTurnItemOrdinal {
        thread_id: String,
    },
    TurnStartHistory {
        thread_id: String,
        run_ids: Option<Vec<String>>,
    },
    ThreadMetadata {
        thread_ids: Vec<String>,
    },
    UnpairedInterrupt {
        thread_id: String,
        request_id: String,
        result_id: String,
    },
    WorkspacePaths {},
}

#[derive(Default, Serialize)]
struct LookupResult {
    owner_exists: bool,
    payloads: Vec<String>,
    metadata: Vec<Value>,
    workspaces: Vec<Value>,
    ids: Vec<String>,
    count: i64,
    found: bool,
}

async fn owned_payload(
    session: &mut Session,
    name: &str,
    id: &str,
    thread: Option<&str>,
) -> Result<Option<String>> {
    let Some(source) = read_row(
        &mut session.transaction,
        &session.catalog,
        name,
        &[text_cell(id)],
    )
    .await?
    else {
        return Ok(None);
    };
    if let Some(thread) = thread {
        if text(&source.row, "thread_id")? != thread {
            return Ok(None);
        }
    }
    text(&source.row, "payload_json").map(Some)
}

async fn owns_row(session: &mut Session, name: &str, id: &str, thread: &str) -> Result<bool> {
    Ok(read_row(
        &mut session.transaction,
        &session.catalog,
        name,
        &[text_cell(id)],
    )
    .await?
    .map(|source| text(&source.row, "thread_id").map(|owner| owner == thread))
    .transpose()?
    .unwrap_or(false))
}

async fn required_payload(
    session: &mut Session,
    name: &str,
    thread_id: &str,
    record_id: &str,
) -> Result<LookupResult> {
    let owner_exists = owns_row(session, THREADS, thread_id, thread_id).await?;
    let mut result = LookupResult {
        owner_exists,
        ..Default::default()
    };
    if owner_exists {
        result
            .payloads
            .extend(owned_payload(session, name, record_id, Some(thread_id)).await?);
    }
    Ok(result)
}

fn payload(row: &Map<String, Value>) -> Result<Value> {
    serde_json::from_str(&text(row, "payload_json")?).map_err(|_| Error::IncompatibleStore)
}

fn is_one(value: Option<&Value>) -> bool {
    matches!(value, Some(Value::Bool(true))) || value.and_then(Value::as_f64) == Some(1.0)
}

fn transcript_output_bytes(raw: &str) -> Result<i64> {
    use t3_code_ls_core::json::{self, Value as Json};
    let value = match json::parse_with_limits(
        raw,
        json::Limits {
            max_bytes: crate::MAX_BATCH_BYTES,
            max_depth: 1000,
            max_values: 1_048_576,
        },
    ) {
        Ok(value) => value,
        Err(error) if error.message == "JSON exceeds value limit" => {
            return Err(invalid(
                "transcript payload exceeds the native JSON value budget",
            ));
        }
        Err(_) => return Ok(0), // SQLite json_valid is false for invalid/deeper JSON.
    };
    // SQLite selects the first matching object member and counts every nested
    // string leaf, including duplicate keys. Do not collapse object entries.
    let output = match &value {
        Json::Object(fields) => fields
            .iter()
            .find(|(name, _)| name == "output")
            .map(|(_, value)| value),
        _ => None,
    };
    let mut pending: Vec<_> = output.into_iter().collect();
    let mut bytes = 0_i64;
    while let Some(value) = pending.pop() {
        match value {
            Json::String(value) => {
                bytes = bytes
                    .checked_add(
                        value
                            .len()
                            .try_into()
                            .map_err(|_| Error::IncompatibleStore)?,
                    )
                    .ok_or(Error::IncompatibleStore)?
            }
            Json::Array(values) => pending.extend(values),
            Json::Object(values) => pending.extend(values.iter().map(|(_, value)| value)),
            _ => (),
        }
    }
    Ok(bytes)
}

async fn query(session: &mut Session, query: ProjectionLookup) -> Result<LookupResult> {
    ready(session).await?;
    let mut result = LookupResult {
        owner_exists: true,
        ..Default::default()
    };
    match query {
        ProjectionLookup::Thread { thread_id } => {
            result
                .payloads
                .extend(owned_payload(session, THREADS, &thread_id, None).await?);
        }
        ProjectionLookup::ThreadExists { thread_id } => {
            result.owner_exists = owns_row(session, THREADS, &thread_id, &thread_id).await?;
        }
        ProjectionLookup::Plan {
            thread_id,
            record_id,
        } => return required_payload(session, PLANS, &thread_id, &record_id).await,
        ProjectionLookup::RuntimeRequest {
            thread_id,
            record_id,
        } => return required_payload(session, REQUESTS, &thread_id, &record_id).await,
        ProjectionLookup::TurnItem {
            thread_id,
            record_id,
        } => {
            result
                .payloads
                .extend(owned_payload(session, ITEMS, &record_id, Some(&thread_id)).await?);
        }
        ProjectionLookup::Node { record_id } => {
            result
                .payloads
                .extend(owned_payload(session, NODES, &record_id, None).await?);
        }
        ProjectionLookup::Messages {
            thread_id,
            record_ids,
        } => {
            let mut seen = HashSet::new();
            for id in record_ids {
                if seen.insert(id.clone()) {
                    result
                        .payloads
                        .extend(owned_payload(session, MESSAGES, &id, Some(&thread_id)).await?);
                }
            }
        }
        ProjectionLookup::RunMessage { thread_id, run_id } => {
            result.owner_exists = owns_row(session, THREADS, &thread_id, &thread_id).await?;
            if result.owner_exists {
                if let Some(run) = read_row(
                    &mut session.transaction,
                    &session.catalog,
                    RUNS,
                    &[text_cell(&run_id)],
                )
                .await?
                {
                    if text(&run.row, "thread_id")? == thread_id {
                        if let Some(message_id) = payload(&run.row)?
                            .get("userMessageId")
                            .and_then(Value::as_str)
                        {
                            result.payloads.extend(
                                owned_payload(session, MESSAGES, message_id, Some(&thread_id))
                                    .await?,
                            );
                        }
                    }
                }
            }
        }
        ProjectionLookup::Run { thread_id, run_id } => {
            return required_payload(session, RUNS, &thread_id, &run_id).await;
        }
        ProjectionLookup::PendingDelegatedCompletion { thread_id } => {
            for task in read_index::equal_rows(
                session,
                SUBAGENTS,
                "child_thread_id",
                &text_cell(&thread_id),
            )
            .await?
            {
                if text(&task.row, "origin")? != "app_owned" {
                    continue;
                }
                if payload(&task.row)?.get("result").is_none_or(Value::is_null) {
                    result.found = true;
                    break;
                }
                if nullable(&task.row, "completion_delivery_state")?.as_deref() == Some("claimed") {
                    if let Some(run_id) = nullable(&task.row, "run_id")? {
                        if let Some(owner) = owned_payload(session, RUNS, &run_id, None).await? {
                            let owner: Value = serde_json::from_str(&owner)
                                .map_err(|_| Error::IncompatibleStore)?;
                            let completion = &owner["delegatedCompletion"];
                            if completion["disposition"].as_str() == Some("open")
                                && completion["delivery"].is_object()
                            {
                                result.found = true;
                                break;
                            }
                        }
                    }
                }
            }
        }
        ProjectionLookup::TranscriptByteFloor { thread_id, limit } => {
            let mut rolled_back = HashSet::new();
            for run in read_index::window::equal_metadata(
                session,
                RUNS,
                "thread_id",
                &text_cell(&thread_id),
            )
            .await?
            {
                if text(&run.row, "status")? == "rolled_back" {
                    rolled_back.insert(text(&run.row, "run_id")?);
                }
            }
            let mut bytes = 0_u64;
            let mut selected = Vec::new();
            for item in read_index::window::equal_metadata(
                session,
                ITEMS,
                "thread_id",
                &text_cell(&thread_id),
            )
            .await?
            {
                if !matches!(
                    text(&item.row, "type")?.as_str(),
                    "command_execution" | "dynamic_tool"
                ) || nullable(&item.row, "run_id")?
                    .is_some_and(|run_id| rolled_back.contains(&run_id))
                {
                    continue;
                }
                let size = integer(&item.row, "_native_payload_bytes")?;
                bytes = bytes
                    .checked_add(size.try_into().map_err(|_| Error::IncompatibleStore)?)
                    .ok_or(Error::IncompatibleStore)?;
                selected.push(text(&item.row, "turn_item_id")?);
            }
            if bytes > limit {
                for id in selected {
                    let raw = owned_payload(session, ITEMS, &id, Some(&thread_id))
                        .await?
                        .ok_or(Error::IncompatibleStore)?;
                    result.count = result
                        .count
                        .checked_add(transcript_output_bytes(&raw)?)
                        .ok_or(Error::IncompatibleStore)?;
                }
            }
        }
        ProjectionLookup::CanStartQueuedRun { thread_id } => {
            let owner = read_row(
                &mut session.transaction,
                &session.catalog,
                THREADS,
                &[text_cell(&thread_id)],
            )
            .await?;
            result.owner_exists = owner.is_some();
            if let Some(owner) = owner {
                if nullable(&owner.row, "archived_at")?.is_none()
                    && nullable(&owner.row, "deleted_at")?.is_none()
                {
                    let runs = read_index::window::equal_metadata(
                        session,
                        RUNS,
                        "thread_id",
                        &text_cell(&thread_id),
                    )
                    .await?;
                    let mut queued = false;
                    let mut active = false;
                    for run in runs {
                        match text(&run.row, "status")?.as_str() {
                            "queued" => queued |= !is_one(payload(&run.row)?.get("queueHeld")),
                            "preparing" | "starting" | "running" | "waiting" => active = true,
                            _ => (),
                        }
                    }
                    result.found = queued && !active;
                }
            }
        }
        ProjectionLookup::HeldQueuedRuns { thread_id } => {
            let runs = read_index::window::equal_metadata(
                session,
                RUNS,
                "thread_id",
                &text_cell(&thread_id),
            )
            .await?;
            let mut selected = Vec::new();
            for run in runs {
                if text(&run.row, "status")? == "queued"
                    && is_one(payload(&run.row)?.get("queueHeld"))
                {
                    selected.push((integer(&run.row, "ordinal")?, text(&run.row, "run_id")?));
                }
            }
            selected.sort_by_key(|(ordinal, _)| *ordinal);
            for (_, id) in selected {
                result
                    .payloads
                    .extend(owned_payload(session, RUNS, &id, Some(&thread_id)).await?);
            }
        }
        ProjectionLookup::MessageCount { thread_id } => {
            result.count = i64::try_from(
                read_index::count_equal(session, MESSAGES, "thread_id", &text_cell(&thread_id))
                    .await?,
            )
            .map_err(|_| Error::IncompatibleStore)?;
        }
        ProjectionLookup::SubagentCount { thread_id } => {
            result.count = i64::try_from(
                read_index::count_equal(session, SUBAGENTS, "thread_id", &text_cell(&thread_id))
                    .await?,
            )
            .map_err(|_| Error::IncompatibleStore)?;
        }
        ProjectionLookup::NextTurnItemOrdinal { thread_id } => {
            result.count = read_index::next_item_ordinal(session, &thread_id).await?;
        }
        ProjectionLookup::AppOwnedChildThreads { thread_id } => {
            let tasks = read_index::window::equal_metadata(
                session,
                SUBAGENTS,
                "thread_id",
                &text_cell(&thread_id),
            )
            .await?;
            let mut selected = Vec::new();
            for task in tasks {
                if nullable(&task.row, "origin")?.as_deref() == Some("app_owned") {
                    if let Some(child) = nullable(&task.row, "child_thread_id")? {
                        selected.push((
                            nullable(&task.row, "started_at")?.unwrap_or_default(),
                            text(&task.row, "subagent_id")?,
                            child,
                        ));
                    }
                }
            }
            selected.sort();
            let mut seen = HashSet::new();
            result.ids = selected
                .into_iter()
                .map(|(_, _, child)| child)
                .filter(|child| seen.insert(child.clone()))
                .collect();
        }
        ProjectionLookup::ThreadMetadata { thread_ids } => {
            for id in thread_ids.into_iter().collect::<BTreeSet<_>>() {
                let Some(row) = read_index::window::read_metadata(session, THREADS, &id).await?
                else {
                    continue;
                };
                if nullable(&row.row, "deleted_at")?.is_none() {
                    result.metadata.push(json!({"thread_id": text(&row.row,"thread_id")?,
                        "project_id":text(&row.row,"project_id")?,"title":text(&row.row,"title")?}));
                }
            }
        }
        ProjectionLookup::UnpairedInterrupt {
            thread_id,
            request_id,
            result_id,
        } => {
            result.found = owns_row(session, ITEMS, &request_id, &thread_id).await?
                && !owns_row(session, ITEMS, &result_id, &thread_id).await?;
        }
        ProjectionLookup::TurnStartHistory { thread_id, run_ids } => {
            let mut rows = Vec::new();
            for row in read_index::window::equal_metadata(
                session,
                ITEMS,
                "thread_id",
                &text_cell(&thread_id),
            )
            .await?
            {
                if ![
                    "user_message",
                    "assistant_message",
                    "command_execution",
                    "error",
                    "run_interrupt_result",
                    "file_change",
                    "proposed_plan",
                ]
                .contains(&text(&row.row, "type")?.as_str())
                {
                    continue;
                }
                if let Some(ids) = &run_ids {
                    if !nullable(&row.row, "run_id")?.is_some_and(|id| ids.contains(&id)) {
                        continue;
                    }
                }
                rows.push((
                    integer(&row.row, "ordinal")?,
                    text(&row.row, "turn_item_id")?,
                ));
            }
            rows.sort();
            for (_, id) in rows {
                result
                    .payloads
                    .extend(owned_payload(session, ITEMS, &id, Some(&thread_id)).await?);
            }
        }
        ProjectionLookup::WorkspacePaths {} => {
            result.workspaces = read_index::workspace_paths(session).await?;
        }
    }
    Ok(result)
}

impl ApplicationTransactions {
    pub async fn projections_lookup(&mut self, id: &str, input: ProjectionLookup) -> Result<Value> {
        let mut session = self.take(id)?;
        let result = async {
            let result = query(&mut session, input).await?;
            serde_json::to_value(result).map_err(crate::json_error)
        }
        .await;
        self.finish(id, session, result).await
    }
}

#[cfg(test)]
mod output_tests {
    use super::*;
    #[test]
    fn transcript_floor_counts_nested_utf8_leaves_with_sqlite_object_semantics() {
        assert_eq!(
            transcript_output_bytes(
                r#"{"output":{"a":"🦊","a":"xy","items":[42,true,"é"]},"output":"ignored"}"#
            )
            .unwrap(),
            8
        );
        let nested = format!(
            "{{\"output\":{}\"🦊\"{}}}",
            "[".repeat(200),
            "]".repeat(200)
        );
        assert_eq!(transcript_output_bytes(&nested).unwrap(), 4);
        assert_eq!(transcript_output_bytes("{broken").unwrap(), 0);
    }
}
