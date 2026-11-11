//! Typed row selections for provider control. Decoding and branch decisions stay
//! in the shared application service; requests share the caller's read snapshot.

use std::collections::BTreeSet;

use super::read_index::window::equal_metadata;
use super::read_index::{IndexedRow, ensure_ready};
use super::*;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ProjectionControlQuery {
    PendingUserRequests {
        thread_id: String,
        provider_turn_id: String,
    },
    PendingUserInputDetails {
        thread_id: String,
        provider_turn_id: String,
        node_ids: Vec<String>,
    },
    ProviderControl {
        thread_id: String,
        provider_thread_id: String,
        provider_turn_id: String,
        attempt_id: Option<String>,
        message_id: Option<String>,
    },
    ProviderControlRun {
        thread_id: String,
        active_attempt_id: Option<String>,
    },
    RunningRun {
        thread_id: String,
    },
    RunningTurnDetails {
        provider_thread_id: Option<String>,
        run_attempt_id: Option<String>,
        node_id: Option<String>,
    },
    ThreadProviders {
        thread_id: String,
        active_provider_thread_id: Option<String>,
        target_instance_id: Option<String>,
    },
    RuntimeResponse {
        thread_id: String,
        request_id: String,
        node_id: String,
        session_id: Option<String>,
    },
}

#[derive(Default, Serialize)]
#[serde(rename_all = "camelCase")]
struct ControlResult {
    requests: Vec<String>,
    nodes: Vec<String>,
    items: Vec<String>,
    provider_threads: Vec<String>,
    provider_turns: Vec<String>,
    attempts: Vec<String>,
    messages: Vec<String>,
    runs: Vec<String>,
    sessions: Vec<String>,
}

fn payload(row: &Map<String, Value>) -> Result<Value> {
    serde_json::from_str(&text(row, "payload_json")?).map_err(|_| Error::IncompatibleStore)
}

async fn payloads(session: &mut Session, name: &str, rows: Vec<IndexedRow>) -> Result<Vec<String>> {
    let mut result = Vec::with_capacity(rows.len());
    for row in rows {
        let id = text(&row.row, spec(name).primary[0])?;
        let source = read_row(
            &mut session.transaction,
            &session.catalog,
            name,
            &[text_cell(&id)],
        )
        .await?
        .ok_or(Error::IncompatibleStore)?;
        result.push(text(&source.row, "payload_json")?);
    }
    Ok(result)
}

async fn related(
    session: &mut Session,
    name: &str,
    column: &str,
    value: &str,
) -> Result<Vec<IndexedRow>> {
    equal_metadata(session, name, column, &text_cell(value)).await
}

async fn point(
    session: &mut Session,
    name: &str,
    id: &str,
    thread: Option<&str>,
) -> Result<Option<Map<String, Value>>> {
    let Some(row) = read_row(
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
        if text(&row.row, "thread_id")? != thread {
            return Ok(None);
        }
    }
    Ok(Some(row.row))
}

async fn point_payload(
    session: &mut Session,
    name: &str,
    id: &str,
    thread: Option<&str>,
) -> Result<Vec<String>> {
    point(session, name, id, thread)
        .await?
        .map(|row| text(&row, "payload_json"))
        .transpose()
        .map(|value| value.into_iter().collect())
}

async fn accessible_provider(
    session: &mut Session,
    thread_id: &str,
    provider_id: &str,
) -> Result<Vec<String>> {
    let Some(provider) = point(session, PROVIDER_THREADS, provider_id, None).await? else {
        return Ok(Vec::new());
    };
    let mut accessible = nullable(&provider, "thread_id")?.as_deref() == Some(thread_id);
    if !accessible {
        if let Some(owner) = nullable(&provider, "owner_node_id")? {
            accessible = point(session, NODES, &owner, Some(thread_id))
                .await?
                .is_some();
        }
    }
    if !accessible {
        for subagent in related(session, SUBAGENTS, "provider_thread_id", provider_id).await? {
            if text(&subagent.row, "thread_id")? == thread_id {
                accessible = true;
                break;
            }
        }
    }
    if accessible {
        Ok(vec![text(&provider, "payload_json")?])
    } else {
        Ok(Vec::new())
    }
}

fn by_ordinal(rows: Vec<IndexedRow>) -> Result<Vec<IndexedRow>> {
    let mut rows = rows
        .into_iter()
        .map(|row| Ok((integer(&row.row, "ordinal")?, row)))
        .collect::<Result<Vec<_>>>()?;
    rows.sort_by_key(|(ordinal, row)| (*ordinal, row.order));
    Ok(rows.into_iter().map(|(_, row)| row).collect())
}

fn by_updated(rows: Vec<IndexedRow>, name: &str, descending: bool) -> Result<Vec<IndexedRow>> {
    let primary = spec(name).primary[0];
    let mut rows = rows
        .into_iter()
        .map(|row| Ok((text(&row.row, "updated_at")?, text(&row.row, primary)?, row)))
        .collect::<Result<Vec<_>>>()?;
    rows.sort_by(|a, b| {
        let time = if descending {
            b.0.cmp(&a.0)
        } else {
            a.0.cmp(&b.0)
        };
        time.then_with(|| a.1.cmp(&b.1))
    });
    Ok(rows.into_iter().map(|(_, _, row)| row).collect())
}

async fn bound_sessions(
    session: &mut Session,
    thread_id: &str,
    session_id: Option<&str>,
) -> Result<Vec<IndexedRow>> {
    let mut result = Vec::new();
    for binding in related(session, BINDINGS, "thread_id", thread_id).await? {
        let id = text(&binding.row, "provider_session_id")?;
        if session_id.is_none_or(|selected| selected == id) {
            result.extend(related(session, SESSIONS, "provider_session_id", &id).await?);
        }
    }
    by_updated(result, SESSIONS, false)
}

async fn query(session: &mut Session, input: ProjectionControlQuery) -> Result<ControlResult> {
    ensure_ready(session).await?;
    let mut result = ControlResult::default();
    match input {
        ProjectionControlQuery::PendingUserRequests {
            thread_id,
            provider_turn_id,
        } => {
            let mut rows = Vec::new();
            for row in related(session, REQUESTS, "provider_turn_id", &provider_turn_id).await? {
                if text(&row.row, "thread_id")? == thread_id
                    && text(&row.row, "kind")? == "user_input"
                    && text(&row.row, "status")? == "pending"
                {
                    rows.push(row);
                }
            }
            rows.sort_by_key(|row| row.order);
            result.requests = payloads(session, REQUESTS, rows).await?;
        }
        ProjectionControlQuery::PendingUserInputDetails {
            thread_id,
            provider_turn_id,
            node_ids,
        } => {
            let mut nodes = Vec::new();
            for id in node_ids.into_iter().collect::<BTreeSet<_>>() {
                for row in related(session, NODES, "node_id", &id).await? {
                    if text(&row.row, "thread_id")? == thread_id {
                        nodes.push(row);
                    }
                }
            }
            // The source IN lookup walks unique node IDs in primary-key order.
            result.nodes = payloads(session, NODES, nodes).await?;
            let mut items = Vec::new();
            for row in related(session, ITEMS, "provider_turn_id", &provider_turn_id).await? {
                if text(&row.row, "thread_id")? == thread_id
                    && text(&row.row, "type")? == "user_input_request"
                {
                    items.push(row);
                }
            }
            items.sort_by_key(|row| row.order);
            result.items = payloads(session, ITEMS, items).await?;
        }
        ProjectionControlQuery::ProviderControl {
            thread_id,
            provider_thread_id,
            provider_turn_id,
            attempt_id,
            message_id,
        } => {
            result.provider_threads =
                accessible_provider(session, &thread_id, &provider_thread_id).await?;
            result.provider_turns =
                point_payload(session, TURNS, &provider_turn_id, Some(&thread_id)).await?;
            if let Some(id) = attempt_id {
                result.attempts = point_payload(session, ATTEMPTS, &id, Some(&thread_id)).await?;
            }
            if let Some(id) = message_id {
                result.messages = point_payload(session, MESSAGES, &id, Some(&thread_id)).await?;
            }
        }
        ProjectionControlQuery::ProviderControlRun {
            thread_id,
            active_attempt_id,
        } => {
            let mut candidates = Vec::new();
            for row in related(session, RUNS, "thread_id", &thread_id).await? {
                let value = payload(&row.row)?;
                let active = value.get("activeAttemptId").unwrap_or(&Value::Null);
                if active
                    == &active_attempt_id
                        .as_ref()
                        .map(|id| json!(id))
                        .unwrap_or(Value::Null)
                {
                    candidates.push(row);
                }
            }
            result.runs = payloads(
                session,
                RUNS,
                by_ordinal(candidates)?.into_iter().take(1).collect(),
            )
            .await?;
        }
        ProjectionControlQuery::RunningRun { thread_id } => {
            let mut candidates = Vec::new();
            for row in related(session, RUNS, "thread_id", &thread_id).await? {
                if text(&row.row, "status")? == "running" {
                    candidates.push(row);
                }
            }
            result.runs = payloads(
                session,
                RUNS,
                by_ordinal(candidates)?.into_iter().take(1).collect(),
            )
            .await?;
        }
        ProjectionControlQuery::RunningTurnDetails {
            provider_thread_id,
            run_attempt_id,
            node_id,
        } => {
            if let Some(id) = provider_thread_id {
                result.provider_threads =
                    point_payload(session, PROVIDER_THREADS, &id, None).await?;
                if let (Some(attempt), Some(node)) = (run_attempt_id, node_id) {
                    let mut turns = Vec::new();
                    for row in related(session, TURNS, "provider_thread_id", &id).await? {
                        if nullable(&row.row, "run_attempt_id")?.as_deref()
                            == Some(attempt.as_str())
                            && nullable(&row.row, "node_id")?.as_deref() == Some(node.as_str())
                            && text(&row.row, "status")? == "running"
                        {
                            turns.push(row);
                        }
                    }
                    result.provider_turns = payloads(session, TURNS, by_ordinal(turns)?).await?;
                }
            }
        }
        ProjectionControlQuery::ThreadProviders {
            thread_id,
            active_provider_thread_id,
            target_instance_id,
        } => {
            let sessions = bound_sessions(session, &thread_id, None).await?;
            result.sessions = payloads(session, SESSIONS, sessions).await?;
            if let Some(instance) = target_instance_id {
                let mut candidates = Vec::new();
                for row in related(session, PROVIDER_THREADS, "thread_id", &thread_id).await? {
                    if nullable(&row.row, "owner_node_id")?.is_none()
                        && text(&row.row, "provider_instance_id")? == instance
                    {
                        candidates.push(row);
                    }
                }
                let mut selected: Vec<_> = by_updated(candidates, PROVIDER_THREADS, true)?
                    .into_iter()
                    .take(1)
                    .collect();
                if let Some(active) = active_provider_thread_id {
                    if !selected.iter().any(|row| {
                        text(&row.row, "provider_thread_id").ok().as_deref()
                            == Some(active.as_str())
                    }) {
                        selected.extend(
                            related(session, PROVIDER_THREADS, "provider_thread_id", &active)
                                .await?,
                        );
                    }
                }
                result.provider_threads = payloads(
                    session,
                    PROVIDER_THREADS,
                    by_updated(selected, PROVIDER_THREADS, false)?,
                )
                .await?;
            }
        }
        ProjectionControlQuery::RuntimeResponse {
            thread_id,
            request_id,
            node_id,
            session_id,
        } => {
            result.nodes = point_payload(session, NODES, &node_id, Some(&thread_id)).await?;
            let mut candidates = Vec::new();
            for row in related(session, ITEMS, "node_id", &node_id).await? {
                if text(&row.row, "thread_id")? == thread_id
                    && ["approval_request", "user_input_request"]
                        .contains(&text(&row.row, "type")?.as_str())
                    && payload(&row.row)?.get("requestId").and_then(Value::as_str)
                        == Some(request_id.as_str())
                {
                    candidates.push(row);
                }
            }
            result.items = payloads(
                session,
                ITEMS,
                by_ordinal(candidates)?.into_iter().take(1).collect(),
            )
            .await?;
            if let Some(id) = session_id {
                let sessions = bound_sessions(session, &thread_id, Some(&id)).await?;
                result.sessions = payloads(session, SESSIONS, sessions).await?;
            }
        }
    }
    Ok(result)
}

impl ApplicationTransactions {
    pub async fn projections_control(
        &mut self,
        id: &str,
        input: ProjectionControlQuery,
    ) -> Result<Value> {
        let mut session = self.take(id)?;
        let result = query(&mut session, input)
            .await
            .and_then(|result| serde_json::to_value(result).map_err(|_| Error::IncompatibleStore));
        self.finish(id, session, result).await
    }
}
