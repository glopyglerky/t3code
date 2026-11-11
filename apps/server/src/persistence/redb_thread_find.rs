//! The raw storage boundary for upstream's in-thread find parser and navigation.
//! No text parsing runs while this read owner is held. Fork revisions follow
//! every ancestor, including ancestors absent from the current visible index.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::application::{ApplicationTransactions, Session};
use crate::application_events::EventLatest;
use crate::application_projections::read_index;
use crate::application_rows::{nullable, read_row, text, text_cell};
use crate::{Error, Result};

const THREADS: &str = "orchestration_v2_projection_threads";
const ITEMS: &str = "orchestration_v2_projection_turn_items";
const PROJECTS: &str = "projection_projects";

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ThreadFindSource {
    pub thread_id: String,
    pub item_id: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ThreadFindQuery {
    ActiveThread { thread_id: String },
    Payloads { sources: Vec<ThreadFindSource> },
    Revision { thread_ids: Vec<String> },
}

fn payload(raw: &str) -> Result<Value> {
    serde_json::from_str(raw).map_err(|_| Error::IncompatibleStore)
}

fn nullable_json_text(value: Option<&Value>) -> Result<Option<&str>> {
    match value {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) => Ok(Some(value)),
        _ => Err(Error::IncompatibleStore),
    }
}

async fn active_thread(session: &mut Session, thread_id: &str) -> Result<Value> {
    let Some(thread) = read_row(
        &mut session.transaction,
        &session.catalog,
        THREADS,
        &[text_cell(thread_id)],
    )
    .await?
    else {
        return Ok(json!([]));
    };
    if nullable(&thread.row, "deleted_at")?.is_some() {
        return Ok(json!([]));
    }
    let project_id = text(&thread.row, "project_id")?;
    let Some(project) = read_row(
        &mut session.transaction,
        &session.catalog,
        PROJECTS,
        &[text_cell(&project_id)],
    )
    .await?
    else {
        return Ok(json!([]));
    };
    if nullable(&project.row, "deleted_at")?.is_some() {
        return Ok(json!([]));
    }
    let thread = payload(&text(&thread.row, "payload_json")?)?;
    let worktree = nullable_json_text(thread.get("worktreePath"))?;
    let workspace = nullable(&project.row, "workspace_root")?;
    Ok(json!([{ "cwd": worktree.or(workspace.as_deref()) }]))
}

async fn payloads(session: &mut Session, sources: Vec<ThreadFindSource>) -> Result<Value> {
    if sources.len() > 128 {
        return Err(Error::Invalid(
            "find payload batch exceeds 128 records".into(),
        ));
    }
    let mut result = Vec::with_capacity(sources.len());
    for source in sources {
        let Some(item) = read_row(
            &mut session.transaction,
            &session.catalog,
            ITEMS,
            &[text_cell(&source.item_id)],
        )
        .await?
        else {
            continue;
        };
        if text(&item.row, "thread_id")? != source.thread_id {
            continue;
        }
        let raw = text(&item.row, "payload_json")?;
        let item = payload(&raw)?;
        let entry_id = nullable_json_text(item.get("messageId"))?.unwrap_or(&source.item_id);
        let item_type = item
            .get("type")
            .and_then(Value::as_str)
            .ok_or(Error::IncompatibleStore)?;
        result.push(json!({
            "thread_id":source.thread_id, "turn_item_id":source.item_id,
            "payload_json":raw, "entry_id":entry_id, "item_type":item_type,
        }));
    }
    Ok(json!(result))
}

async fn source_threads(session: &mut Session, sources: Vec<String>) -> Result<Vec<String>> {
    let mut visited = BTreeSet::new();
    let mut pending = sources;
    while let Some(thread_id) = pending.pop() {
        if !visited.insert(thread_id.clone()) {
            continue;
        }
        let Some(thread) = read_row(
            &mut session.transaction,
            &session.catalog,
            THREADS,
            &[text_cell(&thread_id)],
        )
        .await?
        else {
            continue;
        };
        let thread = payload(&text(&thread.row, "payload_json")?)?;
        let Some(fork) = thread.get("forkedFrom") else {
            continue;
        };
        if fork.get("type").and_then(Value::as_str) == Some("run") {
            if let Some(parent) = nullable_json_text(fork.get("threadId"))? {
                pending.push(parent.into());
            }
        }
    }
    Ok(visited.into_iter().collect())
}

impl ApplicationTransactions {
    pub async fn thread_find(&mut self, id: &str, query: ThreadFindQuery) -> Result<Value> {
        let mut session = self.take(id)?;
        if let ThreadFindQuery::Revision { thread_ids } = query {
            let sources = async {
                read_index::ensure_ready(&mut session).await?;
                source_threads(&mut session, thread_ids).await
            }
            .await;
            let sources = self.finish(id, session, sources).await?;
            let mut sequence = 0;
            // Each call returns the same session to its owner; it never opens
            // another snapshot or commits the current transaction.
            for thread_id in sources {
                sequence = sequence.max(
                    self.events_latest(
                        id,
                        EventLatest {
                            kind: "agent".into(),
                            thread_id: Some(thread_id),
                        },
                    )
                    .await?,
                );
            }
            return Ok(json!(sequence));
        }
        let result = async {
            read_index::ensure_ready(&mut session).await?;
            match query {
                ThreadFindQuery::ActiveThread { thread_id } => {
                    active_thread(&mut session, &thread_id).await
                }
                ThreadFindQuery::Payloads { sources } => payloads(&mut session, sources).await,
                ThreadFindQuery::Revision { .. } => unreachable!(),
            }
        }
        .await;
        self.finish(id, session, result).await
    }
}
