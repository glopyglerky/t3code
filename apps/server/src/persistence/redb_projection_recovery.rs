//! Recovery candidates use compact indexes; unreadability audits page original rows.

use std::collections::BTreeSet;

use super::read_index::derived::sqlite_integer;
use super::read_index::window::{equal_metadata, read_metadata};
use super::read_index::{IndexedRow, ensure_ready};
use super::records::{UNIX_EPOCH_JULIAN_MS, timestamp};
use super::*;

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum RecoveryKind {
    QueuedRuns,
    DelegatedCompletions,
    SubagentResults,
    RestartContinuations,
    Runtime,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ProjectionRecoveryQuery {
    Candidates {
        now: String,
        auto_resume: bool,
        snooze: bool,
    },
    WakeAt {
        now: String,
    },
    Threads {
        recovery_kind: RecoveryKind,
        include: Option<Vec<String>>,
        exclude: Option<Vec<String>>,
    },
    UnreadablePage {
        table: String,
        after: Option<String>,
        limit: u32,
    },
    Bindings {
        record_id: String,
    },
    ProviderThreadOwners {
        record_id: String,
    },
}

fn payload(row: &Map<String, Value>) -> Result<Value> {
    serde_json::from_str(&text(row, "payload_json")?).map_err(|_| Error::IncompatibleStore)
}
fn live(status: &str) -> bool {
    matches!(status, "preparing" | "starting" | "running" | "waiting")
}
fn terminal(status: &str) -> bool {
    matches!(
        status,
        "completed" | "interrupted" | "failed" | "cancelled" | "rolled_back"
    )
}
async fn flag(session: &mut Session, name: &str, value: &str) -> Result<Vec<IndexedRow>> {
    equal_metadata(session, name, "_recovery", &text_cell(value)).await
}
async fn by_thread(session: &mut Session, name: &str, id: &str) -> Result<Vec<IndexedRow>> {
    equal_metadata(session, name, "thread_id", &text_cell(id)).await
}
fn add_thread(ids: &mut BTreeSet<String>, row: &Map<String, Value>) -> Result<()> {
    if let Some(id) = nullable(row, "thread_id")? {
        ids.insert(id);
    }
    Ok(())
}
fn statement_now() -> Result<i64> {
    Ok(UNIX_EPOCH_JULIAN_MS
        + std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|_| Error::IncompatibleStore)?
            .as_millis() as i64)
}
async fn full_row(session: &mut Session, name: &str, id: &str) -> Result<Map<String, Value>> {
    Ok(read_row(
        &mut session.transaction,
        &session.catalog,
        name,
        &[text_cell(id)],
    )
    .await?
    .ok_or(Error::IncompatibleStore)?
    .row)
}

async fn session_threads(
    session: &mut Session,
    ids: &mut BTreeSet<String>,
    extensions: bool,
) -> Result<()> {
    for name in if extensions {
        vec!["live_session", "extension_session"]
    } else {
        vec!["live_session"]
    } {
        for session_row in flag(session, SESSIONS, name).await? {
            let id = text(&session_row.row, "provider_session_id")?;
            let bindings =
                equal_metadata(session, BINDINGS, "provider_session_id", &text_cell(&id)).await?;
            if bindings.is_empty() && name == "live_session" {
                add_thread(ids, &session_row.row)?;
            }
            for binding in bindings {
                add_thread(ids, &binding.row)?;
            }
        }
    }
    Ok(())
}

async fn subagent_result_threads(session: &mut Session) -> Result<BTreeSet<String>> {
    let mut ids = BTreeSet::new();
    for child in flag(session, THREADS, "subagent_result").await? {
        let id = text(&child.row, "thread_id")?;
        let p = payload(&child.row)?;
        let parent = p["lineage"]["parentThreadId"].as_str();
        let runs = by_thread(session, RUNS, &id).await?;
        let mut selected = false;
        for run in &runs {
            if !terminal(&text(&run.row, "status")?) {
                continue;
            }
            let p = payload(&run.row)?;
            if let Some(task_id) = p["delegatedTaskId"].as_str() {
                if let Some(task) = read_metadata(session, SUBAGENTS, task_id).await? {
                    if nullable(&task.row, "child_thread_id")?.as_deref() == Some(id.as_str())
                        && payload(&task.row)?["nativeAppOwnedPendingResult"] == true
                    {
                        selected = true;
                        break;
                    }
                }
            }
        }
        if !selected {
            let mut latest = None;
            for run in runs {
                if text(&run.row, "status")? == "queued"
                    && sqlite_integer(&payload(&run.row)?["queueHeld"]) == Some(1)
                {
                    continue;
                }
                let ordinal = integer(&run.row, "ordinal")?;
                if latest.as_ref().is_none_or(|(old, _)| ordinal > *old) {
                    latest = Some((ordinal, run));
                }
            }
            if let Some((_, run)) = latest {
                if terminal(&text(&run.row, "status")?) {
                    let transfers =
                        equal_metadata(session, TRANSFERS, "source_thread_id", &text_cell(&id))
                            .await?;
                    let mut delivered = false;
                    for transfer in transfers {
                        if parent.is_some()
                            && nullable(&transfer.row, "target_thread_id")?.as_deref() == parent
                            && text(&transfer.row, "type")? == "subagent_result"
                        {
                            delivered = true;
                            break;
                        }
                    }
                    selected = !delivered;
                }
            }
        }
        if selected {
            ids.insert(id);
        }
    }
    Ok(ids)
}

async fn recovery_threads(
    session: &mut Session,
    kind: RecoveryKind,
    include: Option<Vec<String>>,
    exclude: Option<Vec<String>>,
) -> Result<Value> {
    ensure_ready(session).await?;
    let mut ids = BTreeSet::new();
    match kind {
        RecoveryKind::QueuedRuns => {
            for row in flag(session, RUNS, "queued_run").await? {
                let id = text(&row.row, "thread_id")?;
                let mut active = false;
                for run in by_thread(session, RUNS, &id).await? {
                    if live(&text(&run.row, "status")?) {
                        active = true;
                        break;
                    }
                }
                if !active {
                    ids.insert(id);
                }
            }
        }
        RecoveryKind::DelegatedCompletions => {
            for row in flag(session, RUNS, "delegated_completion").await? {
                add_thread(&mut ids, &row.row)?;
            }
        }
        RecoveryKind::SubagentResults => ids = subagent_result_threads(session).await?,
        RecoveryKind::RestartContinuations => {
            session_threads(session, &mut ids, false).await?;
            for row in flag(session, RUNS, "restart_continuation").await? {
                add_thread(&mut ids, &row.row)?;
            }
        }
        RecoveryKind::Runtime => {
            for (name, selector) in [
                (RUNS, "runtime_run"),
                (REQUESTS, "pending_request"),
                (SUBAGENTS, "runtime_subagent"),
            ] {
                for row in flag(session, name, selector).await? {
                    add_thread(&mut ids, &row.row)?;
                }
            }
            session_threads(session, &mut ids, true).await?;
            for provider in flag(session, PROVIDER_THREADS, "pending_provider_thread").await? {
                add_thread(&mut ids, &provider.row)?;
                if let Some(id) = nullable(&provider.row, "owner_node_id")? {
                    if let Some(node) = read_metadata(session, NODES, &id).await? {
                        add_thread(&mut ids, &node.row)?;
                    }
                }
                for subagent in equal_metadata(
                    session,
                    SUBAGENTS,
                    "provider_thread_id",
                    &text_cell(&text(&provider.row, "provider_thread_id")?),
                )
                .await?
                {
                    add_thread(&mut ids, &subagent.row)?;
                }
            }
            for node in flag(session, NODES, "live_root_node").await? {
                let id = text(&node.row, "thread_id")?;
                if !equal_metadata(session, SUBAGENTS, "child_thread_id", &text_cell(&id))
                    .await?
                    .is_empty()
                {
                    ids.insert(id);
                }
            }
            for item in flag(session, ITEMS, "runtime_item").await? {
                let run = match nullable(&item.row, "run_id")? {
                    Some(id) => read_metadata(session, RUNS, &id).await?,
                    None => None,
                };
                if run
                    .as_ref()
                    .map(|run| text(&run.row, "status"))
                    .transpose()?
                    .as_deref()
                    != Some("rolled_back")
                {
                    add_thread(&mut ids, &item.row)?;
                }
            }
            ids.extend(crate::application_auxiliary_reads::active_outbox_threads(session).await?);
        }
    }
    let mut output = Vec::new();
    for id in ids {
        if let Some(thread) = read_metadata(session, THREADS, &id).await? {
            let project = text(&thread.row, "project_id")?;
            if nullable(&thread.row, "deleted_at")?.is_some()
                || (kind == RecoveryKind::QueuedRuns
                    && nullable(&thread.row, "archived_at")?.is_some())
                || include
                    .as_ref()
                    .is_some_and(|values| !values.contains(&project))
                || exclude
                    .as_ref()
                    .is_some_and(|values| values.contains(&project))
            {
                continue;
            }
            output.push((text(&thread.row, "updated_at")?, id));
        }
    }
    output.sort();
    Ok(json!(
        output
            .into_iter()
            .map(|(_, id)| json!({"thread_id":id}))
            .collect::<Vec<_>>()
    ))
}

async fn wake_at(session: &mut Session, now: String) -> Result<Value> {
    let clock = statement_now()?;
    let now = timestamp(&json!(now), clock).ok_or_else(|| invalid("invalid recovery clock"))?;
    for thread in equal_metadata(session, THREADS, "_unreadable_payload", &int_cell(1)).await? {
        if nullable(&thread.row, "deleted_at")?.is_none() {
            payload(&thread.row)?;
        }
    }
    let mut reset = None;
    let mut snooze = None;
    for thread in flag(session, THREADS, "auto_resume").await? {
        if nullable(&thread.row, "deleted_at")?.is_some() {
            continue;
        }
        let p = payload(&thread.row)?;
        for (field, result) in [
            ("nativeLimitRecoveryResetAt", &mut reset),
            ("snoozedUntil", &mut snooze),
        ] {
            if let Some(time) = timestamp(&p[field], clock).filter(|time| *time > now) {
                if result.is_none_or(|previous| time < previous) {
                    *result = Some(time);
                }
            }
        }
    }
    let epoch = |value: Option<i64>| {
        value.map(|value| (value as f64 / 86_400_000.0 - 2_440_587.5) * 86_400_000.0)
    };
    Ok(json!([{"reset_ms":epoch(reset),"snooze_ms":epoch(snooze)}]))
}

async fn limit_candidates(
    session: &mut Session,
    now: String,
    auto_resume: bool,
    snooze: bool,
) -> Result<Value> {
    let clock = statement_now()?;
    let now = timestamp(&json!(now), clock).ok_or_else(|| invalid("invalid recovery clock"))?;
    for item in equal_metadata(session, ITEMS, "_unreadable_payload", &int_cell(1)).await? {
        if text(&item.row, "type")? == "error" && text(&item.row, "status")? == "failed" {
            payload(&item.row)?;
        }
    }
    let mut ids = BTreeSet::new();
    for item in flag(session, ITEMS, "usage_limit").await? {
        ids.insert(text(&item.row, "thread_id")?);
    }
    let mut output = Vec::new();
    for id in ids {
        let Some(thread) = read_metadata(session, THREADS, &id).await? else {
            continue;
        };
        if nullable(&thread.row, "deleted_at")?.is_some() {
            continue;
        }
        let p = payload(&thread.row)?;
        if !p["archivedAt"].is_null() || p["settledOverride"] == "settled" {
            continue;
        }
        let mut executed = Vec::new();
        for run in by_thread(session, RUNS, &id).await? {
            let status = text(&run.row, "status")?;
            let data = payload(&run.row)?;
            if status == "queued" || (status == "cancelled" && data["startedAt"].is_null()) {
                continue;
            }
            let completed = nullable(&run.row, "completed_at")?;
            executed.push((
                completed.is_none(),
                completed,
                integer(&run.row, "ordinal")?,
                text(&run.row, "run_id")?,
                run,
            ));
        }
        executed.sort_by(|a, b| {
            b.0.cmp(&a.0)
                .then_with(|| b.1.cmp(&a.1))
                .then_with(|| b.2.cmp(&a.2))
                .then_with(|| b.3.cmp(&a.3))
        });
        let Some((_, completed, _, run_id, run)) = executed.into_iter().next() else {
            continue;
        };
        if text(&run.row, "status")? != "failed" {
            continue;
        }
        let root = payload(&run.row)?["rootNodeId"].clone();
        let mut failures = Vec::new();
        for item in equal_metadata(session, ITEMS, "run_id", &text_cell(&run_id)).await? {
            if text(&item.row, "thread_id")? != id
                || text(&item.row, "type")? != "error"
                || text(&item.row, "status")? != "failed"
                || json!(nullable(&item.row, "node_id")?) != root
            {
                continue;
            }
            failures.push((
                text(&item.row, "updated_at")?,
                integer(&item.row, "ordinal")?,
                text(&item.row, "turn_item_id")?,
                item,
            ));
        }
        failures.sort_by(|a, b| {
            b.0.cmp(&a.0)
                .then_with(|| b.1.cmp(&a.1))
                .then_with(|| b.2.cmp(&a.2))
        });
        let Some((_, _, item_id, item)) = failures.into_iter().next() else {
            continue;
        };
        let item_payload = payload(&item.row)?;
        if item_payload["nativeFailureClass"] != "usage_limit" {
            continue;
        }
        let reset = &item_payload["nativeFailureResetAt"];
        let Some(reset_time) = timestamp(reset, clock) else {
            continue;
        };
        let completed_value = completed
            .clone()
            .map(Value::String)
            .unwrap_or_else(|| p["updatedAt"].clone());
        if timestamp(&completed_value, clock).is_none_or(|time| reset_time <= time) {
            continue;
        }
        let tracked =
            p["nativeLimitRecoveryRunId"] == run_id && &p["nativeLimitRecoveryResetAt"] == reset;
        let tracked_ready = tracked
            && sqlite_integer(&p["nativeLimitRecoveryAutoResume"]) == Some(1)
            && reset_time <= now
            && (p["snoozedUntil"].is_null()
                || timestamp(&p["snoozedUntil"], clock).is_some_and(|time| time <= now));
        let mut new_ready = !tracked && (auto_resume || (snooze && reset_time > now));
        if new_ready && p["lineage"]["relationshipToParent"] == "subagent" {
            if let Some(parent) = p["lineage"]["parentThreadId"].as_str() {
                if let Some(parent) = read_metadata(session, THREADS, parent).await? {
                    let parent_payload = payload(&parent.row)?;
                    if parent_payload["settledOverride"] == "settled"
                        || !parent_payload["archivedAt"].is_null()
                        || nullable(&parent.row, "deleted_at")?.is_some()
                    {
                        new_ready = false;
                    }
                }
            }
        }
        if !tracked_ready && !new_ready {
            continue;
        }
        let mut pending = false;
        for request in by_thread(session, REQUESTS, &id).await? {
            if text(&request.row, "status")? == "pending" {
                pending = true;
                break;
            }
        }
        if pending {
            continue;
        }
        let provider = nullable(&thread.row, "provider_instance_id")?;
        let mut sessions = Vec::new();
        if provider.is_some() {
            for binding in by_thread(session, BINDINGS, &id).await? {
                if let Some(session_row) = read_metadata(
                    session,
                    SESSIONS,
                    &text(&binding.row, "provider_session_id")?,
                )
                .await?
                {
                    if nullable(&session_row.row, "provider_instance_id")? == provider {
                        sessions.push((
                            text(&session_row.row, "updated_at")?,
                            text(&session_row.row, "provider_session_id")?,
                        ));
                    }
                }
            }
        }
        sessions.sort_by(|a, b| b.cmp(a));
        let last_error = if let Some((_, id)) = sessions.first() {
            payload(&full_row(session, SESSIONS, id).await?)?["lastError"].clone()
        } else {
            Value::Null
        };
        output.push(json!({"payload_json":text(&full_row(session,THREADS,&id).await?,"payload_json")?,"run_id":run_id,"completed_at":completed,"failure_payload_json":text(&full_row(session,ITEMS,&item_id).await?,"payload_json")?,"last_error":last_error}));
    }
    Ok(json!(output))
}

fn audit_spec(name: &str) -> Result<(&'static str, &'static str)> {
    Ok(match name {
        "threads" => (THREADS, "thread_id"),
        "runs" => (RUNS, "run_id"),
        "run_attempts" => (ATTEMPTS, "attempt_id"),
        "nodes" => (NODES, "node_id"),
        "subagents" => (SUBAGENTS, "subagent_id"),
        "provider_sessions" => (SESSIONS, "provider_session_id"),
        "provider_threads" => (PROVIDER_THREADS, "provider_thread_id"),
        "provider_turns" => (TURNS, "provider_turn_id"),
        "runtime_requests" => (REQUESTS, "runtime_request_id"),
        "messages" => (MESSAGES, "message_id"),
        "plans" => (PLANS, "plan_id"),
        "turn_items" => (ITEMS, "turn_item_id"),
        "checkpoint_scopes" => (SCOPES, "scope_id"),
        "checkpoints" => (CHECKPOINTS, "checkpoint_id"),
        "context_handoffs" => (HANDOFFS, "context_handoff_id"),
        "context_transfers" => (TRANSFERS, "context_transfer_id"),
        _ => return Err(invalid("unsupported projection audit table")),
    })
}

async fn raw_page(
    session: &mut Session,
    name: &str,
    after: Option<String>,
    limit: usize,
) -> Result<(Vec<(String, Map<String, Value>)>, bool)> {
    if limit == 0 || limit > 500 {
        return Err(invalid(
            "projection audit page limit must be between 1 and 500",
        ));
    }
    let prefix = row_prefix(&session.catalog.generation, name)?;
    let mut cursor = prefix.clone();
    let mut stop = prefix.clone();
    *stop.last_mut().ok_or(Error::IncompatibleStore)? += 1;
    if let Some(after) = after {
        if after.len() != 64
            || !after
                .bytes()
                .all(|value| value.is_ascii_hexdigit() && !value.is_ascii_uppercase())
        {
            return Err(invalid("invalid projection audit cursor"));
        }
        cursor.extend_from_slice(after.as_bytes());
        cursor.push(0);
    }
    let mut size = limit.min(32);
    let page = loop {
        match session.transaction.scan(&cursor, &stop, size).await {
            Err(Error::ResourceLimit(_)) if size > 1 => size = (size / 2).max(1),
            result => break result?,
        }
    };
    let more = page.len() == size;
    let mut output = Vec::new();
    for (encoded, bytes) in page {
        let row = decode_row(
            &session.catalog,
            table(&session.catalog, name)?,
            &encoded,
            &bytes,
        )?;
        let cursor = std::str::from_utf8(
            encoded
                .strip_prefix(prefix.as_slice())
                .ok_or(Error::IncompatibleStore)?,
        )
        .map_err(|_| Error::IncompatibleStore)?
        .into();
        output.push((cursor, row.row));
    }
    Ok((output, more))
}

async fn unreadable_page(
    session: &mut Session,
    name: String,
    after: Option<String>,
    limit: u32,
) -> Result<Value> {
    let (name, id) = audit_spec(&name)?;
    let (page, more) = raw_page(session, name, after, limit as usize).await?;
    let mut output = Vec::new();
    let mut next = None;
    let mut bytes = 32;
    for (cursor, row) in page {
        let value = json!({"row_id":cursor,"entity_id":text(&row,id)?,"thread_id":nullable(&row,if name==TRANSFERS {"source_thread_id"} else {"thread_id"})?,"target_thread_id":if name==TRANSFERS {nullable(&row,"target_thread_id")?} else {None},"payload_json":text(&row,"payload_json")?});
        let size = serde_json::to_vec(&value).map_err(crate::json_error)?.len();
        if bytes + size > crate::MAX_BATCH_BYTES - 1024 && !output.is_empty() {
            return Ok(json!({"rows":output,"next":next}));
        }
        bytes += size;
        next = Some(cursor);
        output.push(value);
    }
    Ok(json!({"rows":output,"next":if more {next} else {None}}))
}

async fn raw_related_threads(
    session: &mut Session,
    name: &str,
    column: &str,
    id: &str,
) -> Result<BTreeSet<String>> {
    if read_index::is_prepared(session).await? {
        let mut output = BTreeSet::new();
        for row in equal_metadata(session, name, column, &text_cell(id)).await? {
            add_thread(&mut output, &row.row)?;
        }
        return Ok(output);
    }
    let mut after = None;
    let mut output = BTreeSet::new();
    loop {
        let (rows, more) = raw_page(session, name, after.take(), 128).await?;
        for (cursor, row) in rows {
            after = Some(cursor);
            if nullable(&row, column)?.as_deref() == Some(id) {
                if let Some(thread) = nullable(&row, "thread_id")? {
                    output.insert(thread);
                }
            }
        }
        if !more {
            break;
        }
    }
    Ok(output)
}
async fn owners(session: &mut Session, id: String) -> Result<Value> {
    let mut output = raw_related_threads(session, SUBAGENTS, "provider_thread_id", &id).await?;
    if read_index::is_prepared(session).await? {
        if let Some(provider) = read_metadata(session, PROVIDER_THREADS, &id).await? {
            if let Some(node_id) = nullable(&provider.row, "owner_node_id")? {
                if let Some(node) = read_metadata(session, NODES, &node_id).await? {
                    add_thread(&mut output, &node.row)?;
                }
            }
        }
        return Ok(json!(
            output
                .into_iter()
                .map(|id| json!({"thread_id":id}))
                .collect::<Vec<_>>()
        ));
    }
    if let Some(provider) = read_row(
        &mut session.transaction,
        &session.catalog,
        PROVIDER_THREADS,
        &[text_cell(&id)],
    )
    .await?
    {
        if let Some(node) = nullable(&provider.row, "owner_node_id")? {
            if let Some(node) = read_row(
                &mut session.transaction,
                &session.catalog,
                NODES,
                &[text_cell(&node)],
            )
            .await?
            {
                if let Some(thread) = nullable(&node.row, "thread_id")? {
                    output.insert(thread);
                }
            }
        }
    }
    Ok(json!(
        output
            .into_iter()
            .map(|id| json!({"thread_id":id}))
            .collect::<Vec<_>>()
    ))
}

impl ApplicationTransactions {
    pub async fn projections_recovery(
        &mut self,
        id: &str,
        query: ProjectionRecoveryQuery,
    ) -> Result<Value> {
        let mut session = self.take(id)?;
        let result = async {
            let value = match query {
                ProjectionRecoveryQuery::Candidates {
                    now,
                    auto_resume,
                    snooze,
                } => limit_candidates(&mut session, now, auto_resume, snooze).await?,
                ProjectionRecoveryQuery::WakeAt { now } => wake_at(&mut session, now).await?,
                ProjectionRecoveryQuery::Threads {
                    recovery_kind,
                    include,
                    exclude,
                } => recovery_threads(&mut session, recovery_kind, include, exclude).await?,
                ProjectionRecoveryQuery::UnreadablePage {
                    table,
                    after,
                    limit,
                } => unreadable_page(&mut session, table, after, limit).await?,
                ProjectionRecoveryQuery::Bindings { record_id } => json!(
                    raw_related_threads(&mut session, BINDINGS, "provider_session_id", &record_id)
                        .await?
                        .into_iter()
                        .map(|id| json!({"thread_id":id}))
                        .collect::<Vec<_>>()
                ),
                ProjectionRecoveryQuery::ProviderThreadOwners { record_id } => {
                    owners(&mut session, record_id).await?
                }
            };
            crate::validate_response_bytes(&value)?;
            Ok(value)
        }
        .await;
        self.finish(id, session, result).await
    }
}
