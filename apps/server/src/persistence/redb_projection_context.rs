//! Targeted context selections for turn start, recovery, checkpoints and files.
//! Compact metadata selects a cohort before full JSON is materialized. Every
//! query joins the caller's native snapshot and keeps decoding in the shared TS
//! service, including its original early returns and error boundaries.

use super::read_index::window::{equal_metadata, read_metadata};
use super::read_index::{IndexedRow, ensure_ready};
use super::*;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectionContextQuery {
    pub kind: String,
    pub thread_id: String,
    pub run_id: Option<String>,
    pub record_id: Option<String>,
    pub scope_id: Option<String>,
    pub ordinal: Option<i64>,
    pub ids: Option<Vec<Option<String>>>,
    pub provider_thread_id: Option<String>,
    pub provider_turn_id: Option<String>,
    pub attempt_id: Option<String>,
}
fn copy(row: &IndexedRow) -> IndexedRow {
    IndexedRow {
        row: row.row.clone(),
        order: row.order,
    }
}
fn p(row: &IndexedRow) -> Result<Value> {
    serde_json::from_str(&text(&row.row, "payload_json")?).map_err(|_| Error::IncompatibleStore)
}
fn flag(row: &IndexedRow, name: &str) -> Result<bool> {
    Ok(p(row)?.get(name) == Some(&Value::Bool(true)))
}
// Only predicates guarded by SQLite's json_valid deliberately ignore an
// unreadable historical payload. Unguarded json_extract must propagate it.
fn guarded_flag(row: &IndexedRow, name: &str) -> bool {
    flag(row, name).unwrap_or(false)
}
fn string(row: &IndexedRow, column: &str) -> Result<String> {
    text(&row.row, column)
}
fn optional(row: &IndexedRow, column: &str) -> Result<Option<String>> {
    nullable(&row.row, column)
}
fn id(row: &IndexedRow, name: &str) -> Result<String> {
    string(row, spec(name).primary[0])
}
fn one_of(row: &IndexedRow, column: &str, values: &[&str]) -> Result<bool> {
    Ok(optional(row, column)?.is_some_and(|v| values.contains(&v.as_str())))
}
fn required(value: &Option<String>) -> Result<&str> {
    value
        .as_deref()
        .ok_or_else(|| invalid("context query is missing its identity"))
}
fn required_ordinal(value: Option<i64>) -> Result<i64> {
    value.ok_or_else(|| invalid("context query is missing its ordinal"))
}
fn sql_json(value: Option<&Value>) -> Result<Value> {
    Ok(match value.unwrap_or(&Value::Null) {
        Value::Bool(v) => json!(i64::from(*v)),
        Value::Array(_) | Value::Object(_) => {
            json!(serde_json::to_string(value.unwrap()).map_err(crate::json_error)?)
        }
        value => value.clone(),
    })
}

async fn related(
    session: &mut Session,
    name: &str,
    column: &str,
    value: &str,
) -> Result<Vec<IndexedRow>> {
    equal_metadata(session, name, column, &text_cell(value)).await
}
async fn thread_rows(session: &mut Session, name: &str, thread: &str) -> Result<Vec<IndexedRow>> {
    related(session, name, "thread_id", thread).await
}
async fn point(
    session: &mut Session,
    name: &str,
    record: Option<&str>,
    thread: Option<&str>,
) -> Result<Vec<IndexedRow>> {
    let Some(record) = record else {
        return Ok(Vec::new());
    };
    let Some(row) = read_metadata(session, name, record).await? else {
        return Ok(Vec::new());
    };
    if let Some(thread) = thread {
        if optional(&row, "thread_id")?.as_deref() != Some(thread) {
            return Ok(Vec::new());
        }
    }
    Ok(vec![row])
}
fn unique(name: &str, rows: Vec<IndexedRow>) -> Result<Vec<IndexedRow>> {
    let mut seen = BTreeSet::new();
    let mut out = Vec::new();
    for row in rows {
        if seen.insert(id(&row, name)?) {
            out.push(row);
        }
    }
    Ok(out)
}

#[derive(PartialEq, Eq, PartialOrd, Ord)]
enum OrderValue {
    Null,
    Int(i64),
    Text(String),
}
fn order_value(row: &IndexedRow, column: &str) -> Result<OrderValue> {
    let value = row.row.get(column).ok_or(Error::IncompatibleStore)?;
    Ok(match value.get("type").and_then(Value::as_str) {
        Some("null") if column == "first_run_ordinal" => OrderValue::Int(0),
        Some("null") if column == "started_at" => OrderValue::Text(String::new()),
        Some("null") => OrderValue::Null,
        Some("integer") => OrderValue::Int(integer(&row.row, column)?),
        Some("text") => OrderValue::Text(text(&row.row, column)?),
        _ => return Err(Error::IncompatibleStore),
    })
}
fn sorted(rows: Vec<IndexedRow>, columns: &[&str]) -> Result<Vec<IndexedRow>> {
    let mut keys = rows
        .into_iter()
        .map(|row| {
            Ok((
                columns
                    .iter()
                    .map(|column| order_value(&row, column))
                    .collect::<Result<Vec<_>>>()?,
                row,
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    keys.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.order.cmp(&b.1.order)));
    Ok(keys.into_iter().map(|(_, row)| row).collect())
}
async fn payloads(session: &mut Session, name: &str, rows: Vec<IndexedRow>) -> Result<Value> {
    let mut values = Vec::new();
    for row in rows {
        let primary = spec(name)
            .primary
            .iter()
            .map(|column| {
                row.row
                    .get(*column)
                    .cloned()
                    .ok_or(Error::IncompatibleStore)
            })
            .collect::<Result<Vec<_>>>()?;
        let source = read_row(&mut session.transaction, &session.catalog, name, &primary)
            .await?
            .ok_or(Error::IncompatibleStore)?;
        values.push(json!({"payload_json":text(&source.row,"payload_json")?}));
    }
    Ok(json!(values))
}
async fn bound_sessions(session: &mut Session, thread: &str) -> Result<Vec<IndexedRow>> {
    let mut out = Vec::new();
    for binding in thread_rows(session, BINDINGS, thread).await? {
        out.extend(
            point(
                session,
                SESSIONS,
                Some(&string(&binding, "provider_session_id")?),
                None,
            )
            .await?,
        );
    }
    unique(SESSIONS, out)
}
async fn accessible_provider(
    session: &mut Session,
    thread: &str,
    provider: Option<&str>,
) -> Result<Vec<IndexedRow>> {
    let Some(row) = point(session, PROVIDER_THREADS, provider, None)
        .await?
        .pop()
    else {
        return Ok(Vec::new());
    };
    let mut allowed = optional(&row, "thread_id")?.as_deref() == Some(thread);
    if !allowed {
        if let Some(owner) = optional(&row, "owner_node_id")? {
            allowed = !point(session, NODES, Some(&owner), Some(thread))
                .await?
                .is_empty();
        }
    }
    if !allowed {
        for subagent in related(
            session,
            SUBAGENTS,
            "provider_thread_id",
            &id(&row, PROVIDER_THREADS)?,
        )
        .await?
        {
            if string(&subagent, "thread_id")? == thread {
                allowed = true;
                break;
            }
        }
    }
    Ok(if allowed { vec![row] } else { Vec::new() })
}
fn selected_ids(ids: &Option<Vec<Option<String>>>, value: &str) -> bool {
    ids.as_ref()
        .is_some_and(|ids| ids.iter().any(|id| id.as_deref() == Some(value)))
}
fn keep(
    rows: Vec<IndexedRow>,
    mut predicate: impl FnMut(&IndexedRow) -> Result<bool>,
) -> Result<Vec<IndexedRow>> {
    let mut out = Vec::new();
    for row in rows {
        if predicate(&row)? {
            out.push(row);
        }
    }
    Ok(out)
}

async fn select(session: &mut Session, q: ProjectionContextQuery) -> Result<Value> {
    ensure_ready(session).await?;
    let t = &q.thread_id;
    let (name,rows)=match q.kind.as_str(){
        "thread"=>(THREADS,point(session,THREADS,Some(t),None).await?),
        "thread_exists"=>return Ok(json!(point(session,THREADS,Some(t),None).await?.iter().map(|_|json!({"thread_id":t})).collect::<Vec<_>>())),
        "side_chat"=>return Ok(json!(point(session,THREADS,Some(t),None).await?.iter().map(|row|Ok(json!({"side_chat":sql_json(p(row)?.get("lineage").and_then(|p|p.get("sideChat")))?}))).collect::<Result<Vec<_>>>()?)),
        "turn_runs"=>(RUNS,sorted(thread_rows(session,RUNS,t).await?,&["ordinal"])?),
        "turn_attempts"=>(ATTEMPTS,sorted(thread_rows(session,ATTEMPTS,t).await?,&["run_id","attempt_ordinal"])?),
        "turn_nodes"=>{let run=point(session,RUNS,Some(required(&q.run_id)?),Some(t)).await?;let node=run.first().map(p).transpose()?.and_then(|p|p.get("rootNodeId").and_then(Value::as_str).map(str::to_owned));(NODES,point(session,NODES,node.as_deref(),Some(t)).await?)},
        "turn_subagents"=>(SUBAGENTS,sorted(keep(thread_rows(session,SUBAGENTS,t).await?,|row|Ok(!one_of(row,"status",&["interrupted","failed","cancelled"])?))?,&["subagent_id"])?),
        "turn_provider_threads"=>(PROVIDER_THREADS,sorted(thread_rows(session,PROVIDER_THREADS,t).await?,&["first_run_ordinal","provider_thread_id"])?),
        "turn_provider_turns"=>(TURNS,sorted(thread_rows(session,TURNS,t).await?,&["provider_thread_id","ordinal"])?),
        "turn_scopes"=>(SCOPES,sorted(keep(thread_rows(session,SCOPES,t).await?,|row|Ok(selected_ids(&q.ids,&id(row,SCOPES)?)))?,&["ordinal_within_parent","scope_id"])?),
        "turn_handoffs"=>(HANDOFFS,sorted(thread_rows(session,HANDOFFS,t).await?,&[])?),
        "turn_transfers"=>{let mut rows=related(session,TRANSFERS,"source_thread_id",t).await?;rows.extend(related(session,TRANSFERS,"target_thread_id",t).await?);(TRANSFERS,sorted(unique(TRANSFERS,rows)?,&[])?)},
        "turn_items"=>(ITEMS,sorted(keep(related(session,ITEMS,"run_id",required(&q.run_id)?).await?,|row|Ok(string(row,"thread_id")?==*t))?,&["ordinal","turn_item_id"])?),
        "turn_sessions"=>(SESSIONS,sorted(bound_sessions(session,t).await?,&["updated_at","provider_session_id"])?),
        "turn_messages"=>{let run=point(session,RUNS,Some(required(&q.run_id)?),Some(t)).await?;let user=run.first().map(p).transpose()?.and_then(|p|p.get("userMessageId").and_then(Value::as_str).map(str::to_owned));
            (MESSAGES,sorted(keep(thread_rows(session,MESSAGES,t).await?,|row|Ok(user.as_deref()==Some(id(row,MESSAGES)?.as_str())||string(row,"role")?=="user"&&(flag(row,"nativeCompactCommand")?||flag(row,"nativeGoalCommand")?)))?,&["created_at","message_id"])?)},
        "turn_conversation" => {
            let mut present = false;
            for row in thread_rows(session, MESSAGES, t).await? {
                if string(&row, "role")? == "user" {
                    present |= flag(&row, "nativeConversationUser")?;
                }
            }
            return Ok(json!([{"present":u8::from(present)}]));
        },
        "run"=>(RUNS,point(session,RUNS,q.record_id.as_deref(),Some(t)).await?),
        "node"=>(NODES,point(session,NODES,q.record_id.as_deref(),Some(t)).await?),
        "scope"=>(SCOPES,point(session,SCOPES,q.record_id.as_deref(),Some(t)).await?),
        "checkpoint"=>(CHECKPOINTS,point(session,CHECKPOINTS,q.record_id.as_deref(),Some(t)).await?),
        "provider"=>(PROVIDER_THREADS,accessible_provider(session,t,q.record_id.as_deref()).await?),
        "checkpoint_runs"=>return Ok(json!(sorted(thread_rows(session,RUNS,t).await?,&["ordinal"])?.iter().map(|row|Ok(json!({"id":id(row,RUNS)?,"ordinal":integer(&row.row,"ordinal")?,"status":string(row,"status")?}))).collect::<Result<Vec<_>>>()?)),
        "checkpoint_scopes"=>return Ok(json!(sorted(thread_rows(session,SCOPES,t).await?,&["ordinal_within_parent","scope_id"])?.iter().map(|row|Ok(json!({"id":id(row,SCOPES)?,"runId":optional(row,"run_id")?,"kind":string(row,"kind")?,"cwd":sql_json(p(row)?.get("cwd"))?}))).collect::<Result<Vec<_>>>()?)),
        "checkpoint_records"=>return Ok(json!(sorted(thread_rows(session,CHECKPOINTS,t).await?,&["scope_id","ordinal_within_scope"])?.iter().map(|row|Ok(json!({"scopeId":string(row,"scope_id")?,"runId":optional(row,"run_id")?,"ordinalWithinScope":integer(&row.row,"ordinal_within_scope")?,"appRunOrdinal":cell_json(row,"app_run_ordinal")?,"status":string(row,"status")?,"ref":sql_json(p(row)?.get("ref"))?}))).collect::<Result<Vec<_>>>()?)),
        "ready_ordinals"=>return Ok(json!(sorted(keep(thread_rows(session,CHECKPOINTS,t).await?,|row|Ok(string(row,"scope_id")?==required(&q.scope_id)?&&string(row,"status")?=="ready"))?,&[])?.iter().map(|row|Ok(json!({"ordinal":integer(&row.row,"ordinal_within_scope")?}))).collect::<Result<Vec<_>>>()?)),
        "rollback_target_attempt"=>{let ordinal=required_ordinal(q.ordinal)?;let mut out=Vec::new();for run in thread_rows(session,RUNS,t).await?{if integer(&run.row,"ordinal")?!=ordinal{continue;}let payload=p(&run)?;if let Some(attempt)=point(session,ATTEMPTS,payload.get("activeAttemptId").and_then(Value::as_str),Some(t)).await?.pop(){out.push(json!({"id":id(&attempt,ATTEMPTS)?,"providerTurnId":optional(&attempt,"provider_turn_id")?}));}}return Ok(json!(out));},
        "rollback_runs"=>(RUNS,sorted(keep(thread_rows(session,RUNS,t).await?,|row|Ok(integer(&row.row,"ordinal")?>required_ordinal(q.ordinal)?&&one_of(row,"status",&["completed","interrupted","failed","cancelled"])?))?,&["ordinal"])?),
        "bound_session"=>{let id=q.record_id.as_deref();let rows=keep(bound_sessions(session,t).await?,|row|Ok(id==Some(string(row,"provider_session_id")?.as_str())))?;(SESSIONS,rows)},
        "rolledback_attempts"=>{let runs=sorted(keep(thread_rows(session,RUNS,t).await?,|row|Ok(string(row,"status")?=="rolled_back"))?,&["run_id"])?;let mut out=Vec::new();for run in runs{for attempt in related(session,ATTEMPTS,"run_id",&id(&run,RUNS)?).await?{if string(&attempt,"thread_id")?==*t{out.push(json!({"id":id(&attempt,ATTEMPTS)?}));}}}return Ok(json!(out));},
        "stale_checkpoints"=>(CHECKPOINTS,sorted(keep(thread_rows(session,CHECKPOINTS,t).await?,|row|Ok(string(row,"scope_id")?==required(&q.scope_id)?&&string(row,"status")?=="ready"&&cell_json(row,"app_run_ordinal")?.as_i64().is_some_and(|v|q.ordinal.is_some_and(|n|v>n))))?,&["ordinal_within_scope"])?),
        "rollback_nodes"=>{let mut rows=Vec::new();for id in q.ids.as_ref().ok_or_else(||invalid("context node identities are required"))?.iter().flatten().collect::<BTreeSet<_>>(){rows.extend(point(session,NODES,Some(id),Some(t)).await?);}(NODES,sorted(rows,&["node_id"])?)},
        "rollback_turns"=>(TURNS,sorted(keep(thread_rows(session,TURNS,t).await?,|row|Ok(optional(row,"provider_thread_id")?.as_deref()==q.provider_thread_id.as_deref()||q.provider_turn_id.as_ref().is_some_and(|id|id==&string(row,"provider_turn_id").unwrap_or_default())||q.attempt_id.as_ref().is_some_and(|id|optional(row,"run_attempt_id").ok().flatten().as_ref()==Some(id))))?,&["provider_thread_id","ordinal"])?),
        "attachment_messages"=>return attachment_values(sorted(thread_rows(session,MESSAGES,t).await?,&["created_at","message_id"])?,"nativeContextAttachmentIds"),
        "attachment_answers"=>return attachment_values(sorted(keep(thread_rows(session,ITEMS,t).await?,|row|Ok(string(row,"type")?=="user_input_request"))?,&[])?,"nativeAnswerAttachmentIds"),
        "attachment_guests"=>return attachment_values(point(session,THREADS,Some(t),None).await?,"nativeGuestAttachmentIds"),
        "attachment_renders"=>(ITEMS,sorted(keep(thread_rows(session,ITEMS,t).await?,|row|Ok(string(row,"type")?=="dynamic_tool"&&integer(&row.row,"_native_attachment_render")?==1))?,&[])?),
        "runtime_recovery"=>return runtime_recovery(session,t).await,
        _=>return Err(invalid("unknown projection context query")),
    };
    payloads(session, name, rows).await
}
fn cell_json(row: &IndexedRow, column: &str) -> Result<Value> {
    match row
        .row
        .get(column)
        .and_then(|v| v.get("type"))
        .and_then(Value::as_str)
    {
        Some("null") => Ok(Value::Null),
        Some("integer") => Ok(json!(integer(&row.row, column)?)),
        Some("text") => Ok(json!(string(row, column)?)),
        _ => Err(Error::IncompatibleStore),
    }
}
fn attachment_values(rows: Vec<IndexedRow>, field: &str) -> Result<Value> {
    let mut seen = BTreeSet::new();
    let mut out = Vec::new();
    for row in rows {
        let payload = p(&row)?;
        for value in payload
            .get(field)
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            let value = sql_json(Some(value))?;
            if seen.insert(serde_json::to_string(&value).map_err(crate::json_error)?) {
                out.push(json!({"id":value}));
            }
        }
    }
    Ok(json!(out))
}

const ACTIVE_RUNS: &[&str] = &["queued", "preparing", "starting", "running", "waiting"];
const LIVE: &[&str] = &["pending", "running", "waiting"];
const STARTING_LIVE: &[&str] = &["pending", "starting", "running", "waiting"];
const WORK: &[&str] = &["command_execution", "dynamic_tool", "subagent"];
fn ids(rows: &[IndexedRow], name: &str) -> Result<BTreeSet<String>> {
    rows.iter().map(|row| id(row, name)).collect()
}
fn reference_in(row: &IndexedRow, column: &str, ids: &BTreeSet<String>) -> Result<bool> {
    Ok(optional(row, column)?.is_some_and(|id| ids.contains(&id)))
}
fn payload_ref(row: &IndexedRow, field: &str) -> Result<Option<String>> {
    Ok(p(row)?
        .get(field)
        .and_then(Value::as_str)
        .map(str::to_owned))
}

async fn runtime_recovery(session: &mut Session, t: &str) -> Result<Value> {
    let all_runs = sorted(thread_rows(session, RUNS, t).await?, &["ordinal"])?;
    let active = keep(all_runs.iter().map(copy).collect(), |row| {
        one_of(row, "status", ACTIVE_RUNS)
    })?;
    let active_ids = ids(&active, RUNS)?;
    let all_nodes = thread_rows(session, NODES, t).await?;
    let all_subagents = thread_rows(session, SUBAGENTS, t).await?;
    let all_items = thread_rows(session, ITEMS, t).await?;
    let live_items = keep(all_items.iter().map(copy).collect(), |row| {
        one_of(row, "status", LIVE)
    })?;
    let work_items = keep(live_items.iter().map(copy).collect(), |row| {
        one_of(row, "type", WORK)
    })?;
    let pending_requests = keep(thread_rows(session, REQUESTS, t).await?, |row| {
        Ok(string(row, "status")? == "pending")
    })?;
    let input_requests = keep(pending_requests.iter().map(copy).collect(), |row| {
        Ok(string(row, "kind")? == "user_input")
    })?;
    let input_ids = ids(&input_requests, REQUESTS)?;
    let input_nodes = input_requests
        .iter()
        .map(|row| optional(row, "node_id"))
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .flatten()
        .collect::<BTreeSet<_>>();
    let work_nodes = work_items
        .iter()
        .map(|row| optional(row, "node_id"))
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .flatten()
        .collect::<BTreeSet<_>>();
    let mut item_subagents = BTreeSet::new();
    for item in &live_items {
        if string(item, "type")? == "subagent" {
            if let Some(id) = payload_ref(item, "subagentId")? {
                item_subagents.insert(id);
            }
        }
    }
    let native_subagents = ids(
        &keep(all_subagents.iter().map(copy).collect(), |row| {
            Ok(
                optional(row, "origin")?.as_deref() == Some("provider_native")
                    && one_of(row, "status", LIVE)?,
            )
        })?,
        SUBAGENTS,
    )?;
    let roots = keep(all_nodes.iter().map(copy).collect(), |row| {
        Ok(optional(row, "run_id")?.is_none()
            && string(row, "kind")? == "root_turn"
            && one_of(row, "status", LIVE)?)
    })?;
    let root_ids = ids(&roots, NODES)?;

    let own_providers = thread_rows(session, PROVIDER_THREADS, t).await?;
    let mut selected_run_ids = active_ids.clone();
    if let Some(last) = all_runs.last() {
        selected_run_ids.insert(id(last, RUNS)?);
    }
    for item in &work_items {
        if let Some(run) = optional(item, "run_id")? {
            selected_run_ids.insert(run);
        }
    }
    for provider in &own_providers {
        if !guarded_flag(provider, "nativeHasPendingBackgroundTasks") {
            continue;
        }
        let provider_id = id(provider, PROVIDER_THREADS)?;
        let candidates = keep(all_runs.iter().map(copy).collect(), |run| {
            if optional(run, "provider_thread_id")?.as_deref() != Some(&provider_id)
                || one_of(run, "status", &["queued", "rolled_back"])?
            {
                return Ok(false);
            }
            let payload = p(run)?;
            Ok(payload.get("startedAt").is_some_and(|v| !v.is_null())
                || payload.get("restartContinuationOfRunId").is_some())
        })?;
        let mut candidates = candidates
            .into_iter()
            .map(|run| {
                Ok((
                    optional(&run, "completed_at")?,
                    integer(&run.row, "ordinal")?,
                    run,
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        candidates.sort_by(|a, b| {
            b.0.is_none()
                .cmp(&a.0.is_none())
                .then_with(|| b.0.cmp(&a.0))
                .then_with(|| b.1.cmp(&a.1))
        });
        if let Some((_, _, run)) = candidates.first() {
            selected_run_ids.insert(id(run, RUNS)?);
        }
    }
    let runs = keep(all_runs.iter().map(copy).collect(), |row| {
        Ok(selected_run_ids.contains(&id(row, RUNS)?))
    })?;
    let attempts = sorted(
        keep(thread_rows(session, ATTEMPTS, t).await?, |row| {
            reference_in(row, "run_id", &active_ids)
        })?,
        &["run_id", "attempt_ordinal"],
    )?;
    let attempt_ids = ids(&attempts, ATTEMPTS)?;
    let nodes = sorted(
        keep(all_nodes.iter().map(copy).collect(), |row| {
            if !one_of(row, "status", STARTING_LIVE)? {
                return Ok(false);
            }
            let node = id(row, NODES)?;
            Ok(
                optional(row, "run_id")?.is_none() && string(row, "kind")? == "root_turn"
                    || input_nodes.contains(&node)
                    || reference_in(row, "run_id", &active_ids)?
                    || work_nodes.contains(&node)
                    || item_subagents.contains(&node)
                    || native_subagents.contains(&node),
            )
        })?,
        &["started_at", "node_id"],
    )?;
    let subagents = sorted(
        keep(all_subagents.iter().map(copy).collect(), |row| {
            Ok(one_of(row, "status", STARTING_LIVE)?
                && (optional(row, "origin")?.as_deref() == Some("provider_native")
                    || reference_in(row, "run_id", &active_ids)?
                    || item_subagents.contains(&id(row, SUBAGENTS)?)))
        })?,
        &["started_at", "subagent_id"],
    )?;

    let active_provider_ids = active
        .iter()
        .map(|run| optional(run, "provider_thread_id"))
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .flatten()
        .collect::<BTreeSet<_>>();
    let item_provider_ids = live_items
        .iter()
        .map(|item| optional(item, "provider_thread_id"))
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .flatten()
        .collect::<BTreeSet<_>>();
    let mut item_run_provider_ids = BTreeSet::new();
    for item in &live_items {
        if let Some(run) = optional(item, "run_id")? {
            if let Some(run) = point(session, RUNS, Some(&run), None).await?.first() {
                if let Some(provider) = optional(run, "provider_thread_id")? {
                    item_run_provider_ids.insert(provider);
                }
            }
        }
    }
    let mut all_providers = own_providers;
    for node in &all_nodes {
        all_providers.extend(
            related(
                session,
                PROVIDER_THREADS,
                "owner_node_id",
                &id(node, NODES)?,
            )
            .await?,
        );
    }
    for subagent in &all_subagents {
        if let Some(provider) = optional(subagent, "provider_thread_id")? {
            all_providers.extend(point(session, PROVIDER_THREADS, Some(&provider), None).await?);
        }
    }
    let provider_threads = sorted(
        keep(unique(PROVIDER_THREADS, all_providers)?, |row| {
            let id = id(row, PROVIDER_THREADS)?;
            Ok(string(row, "status")? == "active"
                || guarded_flag(row, "nativeHasPendingBackgroundTasks")
                || active_provider_ids.contains(&id)
                || item_provider_ids.contains(&id)
                || item_run_provider_ids.contains(&id))
        })?,
        &["first_run_ordinal", "provider_thread_id"],
    )?;
    let mut active_session_ids = BTreeSet::new();
    for provider in &active_provider_ids {
        if let Some(row) = point(session, PROVIDER_THREADS, Some(provider), None)
            .await?
            .first()
        {
            if let Some(id) = optional(row, "provider_session_id")? {
                active_session_ids.insert(id);
            }
        }
    }
    let mut sessions = keep(bound_sessions(session, t).await?, |row| {
        Ok(!one_of(row, "status", &["stopped", "error"])?
            || string(row, "status")? == "error" && guarded_flag(row, "nativeHasExtensionStatuses")
            || active_session_ids.contains(&id(row, SESSIONS)?))
    })?;
    for row in thread_rows(session, SESSIONS, t).await? {
        if !one_of(&row, "status", &["stopped", "error"])?
            && related(
                session,
                BINDINGS,
                "provider_session_id",
                &id(&row, SESSIONS)?,
            )
            .await?
            .is_empty()
        {
            sessions.push(row);
        }
    }
    let sessions = sorted(
        unique(SESSIONS, sessions)?,
        &["updated_at", "provider_session_id"],
    )?;
    let turns = sorted(
        keep(thread_rows(session, TURNS, t).await?, |row| {
            Ok(one_of(row, "status", STARTING_LIVE)?
                && (reference_in(row, "run_attempt_id", &attempt_ids)?
                    || optional(row, "run_attempt_id")?.is_none()
                        && reference_in(row, "node_id", &root_ids)?))
        })?,
        &["provider_thread_id", "ordinal"],
    )?;
    let requests = sorted(pending_requests, &["created_at", "runtime_request_id"])?;
    let messages = sorted(
        keep(thread_rows(session, MESSAGES, t).await?, |row| {
            Ok(reference_in(row, "run_id", &active_ids)? && integer(&row.row, "streaming")? == 1)
        })?,
        &["created_at", "message_id"],
    )?;
    let items = sorted(
        keep(live_items, |row| {
            Ok(reference_in(row, "run_id", &active_ids)?
                || one_of(row, "type", WORK)?
                || string(row, "type")? == "user_input_request"
                    && payload_ref(row, "requestId")?.is_some_and(|id| input_ids.contains(&id))
                || optional(row, "run_id")?.is_none() && reference_in(row, "node_id", &root_ids)?)
        })?,
        &["ordinal", "turn_item_id"],
    )?;
    let groups = BTreeMap::from([
        ("runs", (RUNS, runs)),
        ("attempts", (ATTEMPTS, attempts)),
        ("nodes", (NODES, nodes)),
        ("subagents", (SUBAGENTS, subagents)),
        ("providerSessions", (SESSIONS, sessions)),
        ("providerThreads", (PROVIDER_THREADS, provider_threads)),
        ("providerTurns", (TURNS, turns)),
        ("runtimeRequests", (REQUESTS, requests)),
        ("messages", (MESSAGES, messages)),
        ("turnItems", (ITEMS, items)),
    ]);
    let mut output = Vec::new();
    for (section, (name, rows)) in groups {
        for mut row in payloads(session, name, rows)
            .await?
            .as_array()
            .ok_or(Error::IncompatibleStore)?
            .clone()
        {
            row.as_object_mut()
                .ok_or(Error::IncompatibleStore)?
                .insert("section".into(), json!(section));
            output.push(row);
        }
    }
    Ok(json!(output))
}

impl ApplicationTransactions {
    pub async fn projections_context(
        &mut self,
        id: &str,
        query: ProjectionContextQuery,
    ) -> Result<Value> {
        let mut session = self.take(id)?;
        let result = select(&mut session, query).await;
        self.finish(id, session, result).await
    }
}
