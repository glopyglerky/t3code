//! Indexed historical usage and pull-request links within a single read snapshot.

use std::collections::{BTreeMap, BTreeSet};

use super::read_index::derived::{pull_request_links, sqlite_integer};
use super::read_index::{ensure_ready, equal_rows};
use super::*;
use crate::application_events::{EventRead, read_events};

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ProjectionHistoryQuery {
    CodexUsage { native_turn_ids: Vec<String> },
    LinkedPullRequests { number: i64 },
}

fn payload(row: &Map<String, Value>) -> Result<Value> {
    serde_json::from_str(&text(row, "payload_json")?).map_err(|_| Error::IncompatibleStore)
}

/// SQLite JSON extraction yields SQL scalars; objects and arrays retain JSON text.
fn extracted(value: Option<&Value>) -> Value {
    match value {
        Some(Value::Array(_) | Value::Object(_)) => Value::String(value.unwrap().to_string()),
        Some(Value::Bool(value)) => json!(i64::from(*value)),
        Some(value) => value.clone(),
        None => Value::Null,
    }
}

async fn first_running_selection(
    session: &mut Session,
    thread: &str,
    run: &str,
    attempt: &str,
) -> Result<Value> {
    let mut after_sequence = 0;
    loop {
        let events = read_events(
            session,
            EventRead {
                after_sequence,
                through_sequence: None,
                thread_id: Some(thread.into()),
                command_id: None,
                event_type: Some("run.updated".into()),
                only_agent_events: Some(true),
                limit: 128,
            },
        )
        .await?;
        let count = events.len();
        for event in events {
            after_sequence = event.sequence;
            let value: Value =
                serde_json::from_str(&event.payload_json).map_err(|_| Error::IncompatibleStore)?;
            if value.get("id").and_then(Value::as_str) == Some(run)
                && value.get("activeAttemptId").and_then(Value::as_str) == Some(attempt)
                && value.get("status").and_then(Value::as_str) == Some("running")
            {
                // A null or absent model on the first running event is definitive.
                return Ok(extracted(value.get("modelSelection")));
            }
        }
        if count < 128 {
            return Ok(Value::Null);
        }
    }
}

async fn codex_usage(session: &mut Session, native_turn_ids: Vec<String>) -> Result<Vec<Value>> {
    if !native_turn_ids.is_empty() {
        for row in super::read_index::window::equal_metadata(
            session,
            TURNS,
            "_unreadable_payload",
            &int_cell(1),
        )
        .await?
        {
            // The original unguarded json_extract visits all provider turns.
            // Valid scalar JSON remains non-matching; syntax errors must fail.
            payload(&row.row)?;
        }
    }
    let mut output = Vec::new();
    let mut selections: BTreeMap<(String, String, String), Value> = BTreeMap::new();
    for id in native_turn_ids.into_iter().collect::<BTreeSet<_>>() {
        for indexed in equal_rows(session, TURNS, "codex_native_turn", &text_cell(&id)).await? {
            let row = indexed.row;
            let thread = text(&row, "thread_id")?;
            let provider_thread = nullable(&row, "provider_thread_id")?;
            let attempt = if let Some(id) = nullable(&row, "run_attempt_id")? {
                read_row(
                    &mut session.transaction,
                    &session.catalog,
                    ATTEMPTS,
                    &[text_cell(&id)],
                )
                .await?
            } else {
                None
            };
            let attempt = match attempt {
                Some(attempt)
                    if text(&attempt.row, "thread_id")? == thread
                        && provider_thread.is_some()
                        && nullable(&attempt.row, "provider_thread_id")? == provider_thread =>
                {
                    Some(attempt)
                }
                _ => None,
            };
            let (native_thread_id, selection_json) = if let Some(attempt) = attempt {
                let native_thread_id = extracted(payload(&attempt.row)?.get("nativeThreadId"));
                let key = (
                    thread,
                    text(&attempt.row, "run_id")?,
                    text(&attempt.row, "attempt_id")?,
                );
                let selection = if let Some(selection) = selections.get(&key) {
                    selection.clone()
                } else {
                    let selection =
                        first_running_selection(session, &key.0, &key.1, &key.2).await?;
                    selections.insert(key, selection.clone());
                    selection
                };
                (native_thread_id, selection)
            } else {
                (Value::Null, Value::Null)
            };
            // Preserve every matching turn. Shared tier resolution treats duplicates
            // as ambiguous even if only one of them has a matching attempt.
            output.push(json!({"native_thread_id":native_thread_id,"native_turn_id":id,"selection_json":selection_json}));
        }
    }
    Ok(output)
}

async fn linked_pull_requests(session: &mut Session, number: i64) -> Result<Vec<Value>> {
    for column in ["_unreadable_payload", "_invalid_pr_links"] {
        for row in super::read_index::window::equal_metadata(session, THREADS, column, &int_cell(1))
            .await?
        {
            if nullable(&row.row, "deleted_at")?.is_some() {
                continue;
            }
            let value = payload(&row.row)?;
            if column == "_invalid_pr_links"
                && value
                    .get("lineage")
                    .and_then(|value| value.get("sideChat"))
                    .and_then(sqlite_integer)
                    != Some(1)
            {
                return Err(Error::IncompatibleStore);
            }
        }
    }
    let mut matches = Vec::new();
    for indexed in equal_rows(session, THREADS, "pull_request_number", &int_cell(number)).await? {
        let row = indexed.row;
        if nullable(&row, "deleted_at")?.is_some() {
            continue;
        }
        let payload = payload(&row)?;
        if payload
            .get("lineage")
            .and_then(|value| value.get("sideChat"))
            .and_then(sqlite_integer)
            == Some(1)
        {
            continue;
        }
        let settled = payload
            .get("settledAt")
            .is_some_and(|value| !value.is_null())
            || payload.get("settledOverride").and_then(Value::as_str) == Some("settled");
        let updated_at = text(&row, "updated_at")?;
        let id = text(&row, "thread_id")?;
        for link in pull_request_links(&payload)? {
            if link.get("number").and_then(sqlite_integer) != Some(number)
                || link.get("source").is_none_or(|source| {
                    source.is_null() || source.as_str() == Some("stack-dismissed")
                })
            {
                continue;
            }
            let record = json!({
                "id":id,"projectId":text(&row,"project_id")?,"title":text(&row,"title")?,
                "archivedAt":nullable(&row,"archived_at")?,"settled":i64::from(settled),
                "host":extracted(link.get("host")),"repository":extracted(link.get("repository")),
                "number":extracted(link.get("number")),"url":extracted(link.get("url")),
            });
            matches.push((updated_at.clone(), id.clone(), record));
        }
    }
    matches.sort_by(|left, right| right.0.cmp(&left.0).then_with(|| left.1.cmp(&right.1)));
    Ok(matches.into_iter().map(|(_, _, row)| row).collect())
}

impl ApplicationTransactions {
    pub async fn projections_history(
        &mut self,
        id: &str,
        query: ProjectionHistoryQuery,
    ) -> Result<Value> {
        let mut session = self.take(id)?;
        let result = async {
            ensure_ready(&mut session).await?;
            let rows = match query {
                ProjectionHistoryQuery::CodexUsage { native_turn_ids } => {
                    codex_usage(&mut session, native_turn_ids).await?
                }
                ProjectionHistoryQuery::LinkedPullRequests { number } => {
                    linked_pull_requests(&mut session, number).await?
                }
            };
            crate::validate_response_bytes(&rows)?;
            Ok(Value::Array(rows))
        }
        .await;
        self.finish(id, session, result).await
    }
}
