//! Scheduled-task and webhook persistence inside the reserved application writer.
//! The TypeScript service retains dispatch policy; this module owns row predicates,
//! ordered indexes, compare-and-set updates, cascades and webhook uniqueness.

use crate::application::{
    ApplicationCatalog, ApplicationTransactions, Session, decode_catalog, decode_row, row_prefix,
    schema_key, table,
};
use crate::application_rows::{
    delete_row, int_cell, integer, managed_marker, nullable_cell, read_row, text, text_cell,
    validate_table, write_row,
};
use crate::backend::Transaction;
use crate::{Error, Result, Store, request_digest, store_key};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Map, Value, json};
use std::collections::BTreeSet;
use std::time::Duration;

const TASKS: &str = "scheduled_tasks";
const DELIVERIES: &str = "scheduled_task_webhook_deliveries";
const RELAYS: &str = "scheduled_task_webhook_relay_deliveries";
const REPLAYS: &str = "scheduled_task_webhook_replay_keys";
const PENDING: &str = "scheduled_task_webhook_pending";
const TABLES: [&str; 5] = [TASKS, DELIVERIES, RELAYS, REPLAYS, PENDING];
const MARKER: &str = "scheduled-rows-v1";
const PAGE: usize = 128;

const TASK_COLUMNS: &[&str] = &[
    "task_id",
    "title",
    "prompt",
    "enabled",
    "schedule_json",
    "project_id",
    "thread_id",
    "thread_mode",
    "workspace_strategy_json",
    "model_selection_json",
    "runtime_mode",
    "interaction_mode",
    "created_by",
    "creation_source",
    "created_at",
    "updated_at",
    "next_run_at",
    "last_run_at",
    "last_run_status",
    "last_run_error",
    "run_count",
    "webhook_token",
    "webhook_secret",
    "last_run_webhook_delivery_id",
];
const DELIVERY_COLUMNS: &[&str] = &[
    "delivery_id",
    "task_id",
    "received_at",
    "method",
    "query",
    "headers_json",
    "body",
    "body_bytes",
    "body_truncated",
    "outcome",
    "signature_verified",
    "missing_fields_json",
    "rendered_prompt",
    "error",
];
const RELAY_COLUMNS: &[&str] = &["relay_delivery_id", "task_id", "seen_at"];
const REPLAY_COLUMNS: &[&str] = &["task_id", "replay_key", "delivery_id", "expires_at"];
const PENDING_COLUMNS: &[&str] = &[
    "delivery_id",
    "task_id",
    "coalesce_key",
    "due_at",
    "state",
    "task_json",
    "request_json",
    "delivery_ids_json",
];
type Row = Map<String, Value>;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ScheduledRead {
    Tasks {
        task_id: Option<String>,
        thread_id: Option<String>,
        due_before: Option<String>,
    },
    NextDue {},
    Deliveries {
        task_id: String,
        delivery_id: Option<String>,
    },
    Pending {
        delivery_id: Option<String>,
        task_id: Option<String>,
        states: Option<Vec<String>>,
        coalesce_key: Option<String>,
        due_after: Option<i64>,
        first: bool,
    },
    PendingCount {
        task_id: String,
    },
    NextPending {},
    ActivePendingTasks {},
    HasReplay {
        task_id: String,
        replay_key: String,
        now: i64,
    },
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ScheduledWrite {
    SaveTask {
        row: Row,
        require_existing: bool,
        kept_schedule_json: Option<String>,
        secret_changed: bool,
    },
    PatchTask {
        task_id: Option<String>,
        thread_ids: Option<Vec<String>>,
        expected: Row,
        different: Row,
        patch: Row,
        increment_run_count: bool,
    },
    DeleteTasks {
        task_id: Option<String>,
        thread_id: Option<String>,
    },
    RecoverTask {
        task_id: String,
        patch: Row,
        pending_delivery_id: Option<String>,
        match_pending_task: bool,
    },
    RecordDelivery {
        row: Row,
    },
    PatchDelivery {
        delivery_id: String,
        patch: Row,
    },
    PruneDeliveries {
        task_id: String,
        bucket: String,
        limit: usize,
    },
    ClaimRelay {
        relay_delivery_id: String,
        task_id: String,
        seen_at: String,
    },
    DeleteRelay {
        relay_delivery_id: String,
    },
    PruneRelays {
        before: String,
    },
    ClaimReplay {
        task_id: String,
        replay_key: String,
        delivery_id: String,
        now: i64,
        expires_at: i64,
    },
    ReleaseReplay {
        delivery_id: String,
    },
    InsertPending {
        row: Row,
    },
    PatchPending {
        delivery_id: String,
        states: Option<Vec<String>>,
        not_states: Option<Vec<String>>,
        patch: Row,
        append_delivery_id: Option<String>,
    },
    DeletePending {
        delivery_id: String,
    },
    ExpirePending {
        now: i64,
        task_id: Option<String>,
        coalesce_key: Option<String>,
    },
    ReleaseFailedRelays {
        delivery_id: String,
    },
}

#[derive(Debug, Deserialize, Serialize)]
pub struct PreparationResult {
    pub prepared: bool,
    pub indexed_rows: u64,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Preparation {
    version: u32,
    table: usize,
    after: Option<String>,
    indexed_rows: u64,
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Index {
    primary: Vec<Value>,
    sequence: u64,
    summary: Row,
}

fn invalid(message: &str) -> Error {
    Error::Invalid(message.into())
}
fn encode(value: &impl Serialize) -> Result<Vec<u8>> {
    serde_json::to_vec(value).map_err(crate::json_error)
}
fn decode<T: DeserializeOwned>(value: &[u8]) -> Result<T> {
    serde_json::from_slice(value).map_err(|_| Error::IncompatibleStore)
}
fn key(c: &ApplicationCatalog, suffix: &str) -> Vec<u8> {
    store_key(&format!(
        "application/generation/{}/scheduled/v1/{suffix}",
        c.generation
    ))
}
fn hex(value: &str) -> String {
    value.bytes().map(|b| format!("{b:02x}")).collect()
}
fn desc(value: &str) -> String {
    let mut v: String = value.bytes().map(|b| format!("{:02x}", 255 - b)).collect();
    v.push('~');
    v
}
fn hash(value: &str) -> String {
    request_digest(&json!(value))
}
fn number(value: i64) -> String {
    format!("{:016x}", (value as u64) ^ (1 << 63))
}
fn end(prefix: &[u8]) -> Result<Vec<u8>> {
    let mut v = prefix.to_vec();
    *v.last_mut().ok_or(Error::IncompatibleStore)? += 1;
    Ok(v)
}
fn columns(name: &str) -> Result<&'static [&'static str]> {
    match name {
        TASKS => Ok(TASK_COLUMNS),
        DELIVERIES => Ok(DELIVERY_COLUMNS),
        RELAYS => Ok(RELAY_COLUMNS),
        REPLAYS => Ok(REPLAY_COLUMNS),
        PENDING => Ok(PENDING_COLUMNS),
        _ => Err(Error::IncompatibleStore),
    }
}
fn primary_columns(name: &str) -> &'static [&'static str] {
    match name {
        TASKS => &["task_id"],
        RELAYS => &["relay_delivery_id"],
        REPLAYS => &["task_id", "replay_key"],
        _ => &["delivery_id"],
    }
}
fn index_columns(name: &str) -> &'static [&'static str] {
    match name {
        TASKS => &[
            "task_id",
            "created_at",
            "thread_id",
            "enabled",
            "next_run_at",
            "last_run_status",
        ],
        DELIVERIES => &["delivery_id", "task_id", "received_at"],
        RELAYS => &["relay_delivery_id", "seen_at"],
        REPLAYS => &["task_id", "replay_key", "delivery_id", "expires_at"],
        PENDING => &["delivery_id", "task_id", "coalesce_key", "due_at", "state"],
        _ => &[],
    }
}
fn summary(name: &str, row: &Row) -> Result<Row> {
    index_columns(name)
        .iter()
        .map(|field| {
            Ok((
                (*field).into(),
                row.get(*field).cloned().ok_or(Error::IncompatibleStore)?,
            ))
        })
        .collect()
}
fn primary(name: &str, row: &Row) -> Result<Vec<Value>> {
    primary_columns(name)
        .iter()
        .map(|field| row.get(*field).cloned().ok_or(Error::IncompatibleStore))
        .collect()
}
fn integer_column(field: &str) -> bool {
    matches!(
        field,
        "enabled"
            | "run_count"
            | "body_bytes"
            | "body_truncated"
            | "signature_verified"
            | "expires_at"
            | "due_at"
    )
}
fn nullable_column(field: &str) -> bool {
    matches!(
        field,
        "thread_id"
            | "next_run_at"
            | "last_run_at"
            | "last_run_error"
            | "webhook_token"
            | "webhook_secret"
            | "last_run_webhook_delivery_id"
            | "rendered_prompt"
            | "error"
    )
}
fn string<'a>(row: &'a Row, field: &str) -> Result<&'a str> {
    row.get(field)
        .and_then(Value::as_str)
        .ok_or(Error::IncompatibleStore)
}
fn int(row: &Row, field: &str) -> Result<i64> {
    row.get(field)
        .and_then(Value::as_i64)
        .ok_or(Error::IncompatibleStore)
}
fn validate_plain(name: &str, row: &Row) -> Result<()> {
    let fields = columns(name)?;
    if fields.len() != row.len() {
        return Err(invalid("scheduled row does not match its schema"));
    }
    for field in fields {
        let value = row
            .get(*field)
            .ok_or_else(|| invalid("scheduled row is missing a column"))?;
        if !(nullable_column(field) && value.is_null())
            && if integer_column(field) {
                value.as_i64().is_none()
            } else {
                !value.is_string()
            }
        {
            return Err(invalid("scheduled row contains an invalid column value"));
        }
    }
    Ok(())
}
fn stored(name: &str, row: &Row) -> Result<Row> {
    validate_plain(name, row)?;
    row.iter()
        .map(|(field, value)| {
            Ok((
                field.clone(),
                if value.is_null() {
                    nullable_cell(None)
                } else if integer_column(field) {
                    int_cell(value.as_i64().ok_or(Error::IncompatibleStore)?)
                } else {
                    text_cell(value.as_str().ok_or(Error::IncompatibleStore)?)
                },
            ))
        })
        .collect()
}
fn plain(name: &str, row: &Row) -> Result<Row> {
    let mut out = Row::new();
    for field in columns(name)? {
        out.insert(
            (*field).into(),
            if row.get(*field) == Some(&json!({"type":"null"})) {
                Value::Null
            } else if integer_column(field) {
                json!(integer(row, field)?)
            } else {
                json!(text(row, field)?)
            },
        );
    }
    validate_plain(name, &out).map_err(|_| Error::IncompatibleStore)?;
    Ok(out)
}
fn tagged_primary(name: &str, values: &[Value]) -> Result<Vec<Value>> {
    if values.len() != primary_columns(name).len() {
        return Err(Error::IncompatibleStore);
    }
    values
        .iter()
        .map(|v| v.as_str().map(text_cell).ok_or(Error::IncompatibleStore))
        .collect()
}
async fn load(
    tx: &mut Transaction,
    c: &ApplicationCatalog,
    name: &str,
    values: &[Value],
) -> Result<Option<Row>> {
    read_row(tx, c, name, &tagged_primary(name, values)?)
        .await?
        .map(|r| plain(name, &r.row))
        .transpose()
}
fn sequence_key(c: &ApplicationCatalog, name: &str, values: &[Value]) -> Vec<u8> {
    key(
        c,
        &format!("sequence/{}/{}", hex(name), request_digest(&json!(values))),
    )
}
async fn sequence(
    tx: &mut Transaction,
    c: &ApplicationCatalog,
    name: &str,
    row: &Row,
    importing: bool,
) -> Result<u64> {
    let values = primary(name, row)?;
    let k = sequence_key(c, name, &values);
    if let Some(bytes) = tx.get(&k).await? {
        return decode(&bytes);
    }
    let source_key = store_key(&format!(
        "application/generation/{}/metadata/source/{}",
        c.generation,
        request_digest(&json!([name, tagged_primary(name, &values)?]))
    ));
    let imported = if importing {
        tx.get(&source_key)
            .await?
            .map(|bytes| decode::<Value>(&bytes))
            .transpose()?
    } else {
        None
    };
    let counter_key = key(c, &format!("counter/{}", hex(name)));
    let next: u64 = tx
        .get(&counter_key)
        .await?
        .as_deref()
        .map(decode)
        .transpose()?
        .unwrap_or(0);
    let assigned = if let Some(source) = imported {
        if source["table"] != name || source["key"] != json!(tagged_primary(name, &values)?) {
            return Err(Error::IncompatibleStore);
        }
        source["ordinal"].as_u64().ok_or(Error::IncompatibleStore)?
    } else {
        next
    };
    tx.put(
        counter_key,
        encode(&next.max(assigned.checked_add(1).ok_or(Error::IncompatibleStore)?))?,
    )
    .await?;
    tx.put(k, encode(&assigned)?).await?;
    Ok(assigned)
}
fn index_keys(c: &ApplicationCatalog, name: &str, row: &Row, seq: u64) -> Result<Vec<Vec<u8>>> {
    let mut paths = Vec::new();
    match name {
        TASKS => {
            let id = hex(string(row, "task_id")?);
            paths.push(format!(
                "tasks/all/{}{id}",
                desc(string(row, "created_at")?)
            ));
            if let Some(thread) = row["thread_id"].as_str() {
                paths.push(format!("tasks/thread/{}/{id}", hash(thread)));
            }
            if int(row, "enabled")? == 1
                && row["next_run_at"].is_string()
                && string(row, "last_run_status")? != "running"
            {
                paths.push(format!(
                    "tasks/due/{}/{id}",
                    hex(string(row, "next_run_at")?)
                ));
            }
        }
        DELIVERIES => paths.push(format!(
            "deliveries/task/{}/{}{:016x}/{}",
            hash(string(row, "task_id")?),
            desc(string(row, "received_at")?),
            u64::MAX - seq,
            hex(string(row, "delivery_id")?)
        )),
        RELAYS => paths.push(format!(
            "relays/seen/{}/{}",
            hex(string(row, "seen_at")?),
            hex(string(row, "relay_delivery_id")?)
        )),
        REPLAYS => {
            let id = request_digest(&json!(primary(name, row)?));
            paths.push(format!(
                "replays/expiry/{}/{id}",
                number(int(row, "expires_at")?)
            ));
            paths.push(format!(
                "replays/delivery/{}/{id}",
                hash(string(row, "delivery_id")?)
            ));
            paths.push(format!(
                "replays/task/{}/{id}",
                hash(string(row, "task_id")?)
            ));
        }
        PENDING => {
            let id = hex(string(row, "delivery_id")?);
            let due = number(int(row, "due_at")?);
            paths.push(format!(
                "pending/state/{}/{due}/{seq:016x}/{id}",
                hex(string(row, "state")?)
            ));
            paths.push(format!(
                "pending/task/{}/{due}/{seq:016x}/{id}",
                hash(string(row, "task_id")?)
            ));
            if string(row, "state")? == "pending" {
                paths.push(format!(
                    "pending/unique/{}",
                    request_digest(&json!([
                        string(row, "task_id")?,
                        string(row, "coalesce_key")?
                    ]))
                ));
            }
        }
        _ => return Err(Error::IncompatibleStore),
    }
    Ok(paths.iter().map(|p| key(c, p)).collect())
}
async fn index_row(
    tx: &mut Transaction,
    c: &ApplicationCatalog,
    name: &str,
    row: &Row,
    seq: u64,
) -> Result<()> {
    let index = Index {
        primary: primary(name, row)?,
        sequence: seq,
        summary: summary(name, row)?,
    };
    for k in index_keys(c, name, row, seq)? {
        if let Some(bytes) = tx.get(&k).await? {
            if decode::<Index>(&bytes)? != index {
                return Err(Error::DuplicateRecord);
            }
        }
        tx.put(k, encode(&index)?).await?;
    }
    Ok(())
}
async fn unindex(
    tx: &mut Transaction,
    c: &ApplicationCatalog,
    name: &str,
    row: &Row,
) -> Result<u64> {
    let seq: u64 = tx
        .get(&sequence_key(c, name, &primary(name, row)?))
        .await?
        .as_deref()
        .map(decode)
        .transpose()?
        .ok_or(Error::IncompatibleStore)?;
    let expected = Index {
        primary: primary(name, row)?,
        sequence: seq,
        summary: summary(name, row)?,
    };
    for k in index_keys(c, name, row, seq)? {
        if tx
            .get(&k)
            .await?
            .as_deref()
            .map(decode::<Index>)
            .transpose()?
            != Some(expected.clone())
        {
            return Err(Error::IncompatibleStore);
        }
        tx.delete(k).await?;
    }
    Ok(seq)
}
async fn put(tx: &mut Transaction, c: &ApplicationCatalog, name: &str, row: Row) -> Result<()> {
    let encoded = stored(name, &row)?;
    let values = primary(name, &row)?;
    let previous = load(tx, c, name, &values).await?;
    let seq = if let Some(old) = previous {
        unindex(tx, c, name, &old).await?
    } else {
        sequence(tx, c, name, &row, false).await?
    };
    if matches!(name, REPLAYS | PENDING)
        && load(tx, c, TASKS, &[row["task_id"].clone()])
            .await?
            .is_none()
    {
        return Err(invalid("webhook task does not exist"));
    }
    write_row(tx, c, name, encoded).await?;
    index_row(tx, c, name, &row, seq).await
}
async fn remove(
    tx: &mut Transaction,
    c: &ApplicationCatalog,
    name: &str,
    values: &[Value],
) -> Result<bool> {
    let Some(row) = load(tx, c, name, values).await? else {
        return Ok(false);
    };
    unindex(tx, c, name, &row).await?;
    tx.delete(sequence_key(c, name, values)).await?;
    delete_row(tx, c, name, &tagged_primary(name, values)?).await
}
async fn scan_indexes(
    tx: &mut Transaction,
    c: &ApplicationCatalog,
    name: &str,
    suffix: &str,
    limit: Option<usize>,
    through: Option<&str>,
) -> Result<Vec<Index>> {
    let prefix = key(c, suffix);
    let end = through.map_or_else(|| end(&prefix), |last| end(&key(c, last)))?;
    let mut start = prefix.clone();
    let mut out = Vec::new();
    loop {
        let count = limit.map_or(PAGE, |n| PAGE.min(n.saturating_sub(out.len())));
        if count == 0 {
            break;
        }
        let records = tx.scan(&start, &end, count).await?;
        if records.is_empty() {
            break;
        }
        for (k, bytes) in records {
            let index: Index = decode(&bytes)?;
            if primary(name, &index.summary)? != index.primary
                || !index_keys(c, name, &index.summary, index.sequence)?.contains(&k)
            {
                return Err(Error::IncompatibleStore);
            }
            out.push(index);
            start = k;
            start.push(0);
        }
    }
    Ok(out)
}
async fn indexed_row(
    tx: &mut Transaction,
    c: &ApplicationCatalog,
    name: &str,
    index: &Index,
) -> Result<Row> {
    let row = load(tx, c, name, &index.primary)
        .await?
        .ok_or(Error::IncompatibleStore)?;
    if summary(name, &row)? != index.summary {
        return Err(Error::IncompatibleStore);
    }
    Ok(row)
}
async fn scan(
    tx: &mut Transaction,
    c: &ApplicationCatalog,
    name: &str,
    suffix: &str,
    limit: Option<usize>,
) -> Result<Vec<Row>> {
    let indexes = scan_indexes(tx, c, name, suffix, limit, None).await?;
    let mut out = Vec::with_capacity(indexes.len());
    for index in indexes {
        out.push(indexed_row(tx, c, name, &index).await?);
    }
    Ok(out)
}
fn patch(name: &str, row: &mut Row, changes: &Row) -> Result<()> {
    for (field, value) in changes {
        if primary_columns(name).contains(&field.as_str()) {
            return Err(invalid("cannot patch a scheduled primary key"));
        }
        row.insert(field.clone(), value.clone());
    }
    validate_plain(name, row)
}
fn selected(row: &Row, expected: &Row, different: &Row) -> bool {
    expected.iter().all(|(k, v)| row.get(k) == Some(v))
        && different
            .iter()
            .all(|(k, v)| row.get(k).is_some_and(|old| old != v))
}
fn increment(row: &mut Row) -> Result<()> {
    let next = int(row, "run_count")?
        .checked_add(1)
        .ok_or_else(|| invalid("scheduled run count overflow"))?;
    row.insert("run_count".into(), json!(next));
    Ok(())
}
fn returning(field: &str, value: Value) -> Value {
    Value::Array(vec![Value::Object(Map::from_iter([(field.into(), value)]))])
}
fn rows(values: Vec<Row>) -> Value {
    Value::Array(values.into_iter().map(Value::Object).collect())
}
async fn ready(session: &mut Session) -> Result<()> {
    let state: Preparation = session
        .transaction
        .get(&key(&session.catalog, "preparation"))
        .await?
        .as_deref()
        .map(decode)
        .transpose()?
        .ok_or_else(|| invalid("application scheduled rows are not prepared"))?;
    if state.version != 1 {
        return Err(Error::IncompatibleStore);
    }
    if state.table != TABLES.len() {
        return Err(invalid("application scheduled rows are not prepared"));
    }
    for name in TABLES {
        if session
            .transaction
            .get(&managed_marker(&session.catalog, name))
            .await?
            .as_deref()
            .map(decode::<String>)
            .transpose()?
            .as_deref()
            != Some(MARKER)
        {
            return Err(Error::IncompatibleStore);
        }
    }
    Ok(())
}

impl Store {
    pub async fn prepare_application_scheduled(&self) -> Result<PreparationResult> {
        let mut limit = 32;
        loop {
            match self.prepare_application_scheduled_batch(limit).await {
                Err(Error::ResourceLimit(_)) if limit > 1 => limit = (limit / 2).max(1),
                result => return result,
            }
        }
    }
    async fn prepare_application_scheduled_batch(&self, limit: usize) -> Result<PreparationResult> {
        let mut tx = self
            .db
            .begin_reserved(Duration::from_secs(5), Duration::from_secs(300))
            .await?;
        let c = decode_catalog(
            &tx.get(&schema_key())
                .await?
                .ok_or_else(|| invalid("application schema is not initialized"))?,
        )?;
        for name in TABLES {
            validate_table(&c, name, columns(name)?, primary_columns(name))?;
        }
        let state_key = key(&c, "preparation");
        let mut state = match tx.get(&state_key).await? {
            Some(bytes) => {
                let state: Preparation = decode(&bytes)?;
                if state.version != 1 || state.table > TABLES.len() {
                    return Err(Error::IncompatibleStore);
                }
                for name in TABLES {
                    if tx
                        .get(&managed_marker(&c, name))
                        .await?
                        .as_deref()
                        .map(decode::<String>)
                        .transpose()?
                        .as_deref()
                        != Some(MARKER)
                    {
                        return Err(Error::IncompatibleStore);
                    }
                }
                state
            }
            None => {
                for name in TABLES {
                    let marker = managed_marker(&c, name);
                    if tx.get(&marker).await?.is_some() {
                        return Err(Error::IncompatibleStore);
                    }
                    tx.put(marker, encode(&MARKER)?).await?;
                }
                Preparation {
                    version: 1,
                    table: 0,
                    after: None,
                    indexed_rows: 0,
                }
            }
        };
        if state.table == TABLES.len() {
            tx.rollback().await?;
            return Ok(PreparationResult {
                prepared: true,
                indexed_rows: state.indexed_rows,
            });
        }
        let name = TABLES[state.table];
        let prefix = row_prefix(&c.generation, name)?;
        let mut start = prefix.clone();
        if let Some(after) = &state.after {
            if after.len() != 64
                || !after
                    .bytes()
                    .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
            {
                return Err(Error::IncompatibleStore);
            }
            start.extend_from_slice(after.as_bytes());
            start.push(0);
        }
        let source = tx.scan(&start, &end(&prefix)?, limit).await?;
        let count = source.len();
        for (k, bytes) in source {
            let row = plain(name, &decode_row(&c, table(&c, name)?, &k, &bytes)?.row)?;
            let seq = sequence(&mut tx, &c, name, &row, true).await?;
            index_row(&mut tx, &c, name, &row, seq).await?;
            state.after = Some(
                std::str::from_utf8(
                    k.strip_prefix(prefix.as_slice())
                        .ok_or(Error::IncompatibleStore)?,
                )
                .map_err(|_| Error::IncompatibleStore)?
                .into(),
            );
            state.indexed_rows = state
                .indexed_rows
                .checked_add(1)
                .ok_or(Error::IncompatibleStore)?;
        }
        if count < limit {
            state.table += 1;
            state.after = None;
        }
        tx.put(state_key, encode(&state)?).await?;
        tx.commit().await?;
        Ok(PreparationResult {
            prepared: state.table == TABLES.len(),
            indexed_rows: state.indexed_rows,
        })
    }
}

impl ApplicationTransactions {
    pub async fn scheduled_read(&mut self, id: &str, query: ScheduledRead) -> Result<Value> {
        let mut session = self.take(id)?;
        let result = async {
            ready(&mut session).await?;
            read(&mut session.transaction, &session.catalog, query).await
        }
        .await;
        self.finish(id, session, result).await
    }
    pub async fn scheduled_write(&mut self, id: &str, mutation: ScheduledWrite) -> Result<Value> {
        let mut session = self.take(id)?;
        let result = async {
            session.transaction.require_write()?;
            ready(&mut session).await?;
            write(&mut session.transaction, &session.catalog, mutation).await
        }
        .await;
        self.finish(id, session, result).await
    }
}

async fn tasks(
    tx: &mut Transaction,
    c: &ApplicationCatalog,
    task_id: Option<String>,
    thread_id: Option<String>,
    due_before: Option<String>,
) -> Result<Vec<Row>> {
    let prefix = if let Some(thread) = &thread_id {
        format!("tasks/thread/{}/", hash(thread))
    } else if due_before.is_some() {
        "tasks/due/".into()
    } else {
        "tasks/all/".into()
    };
    let through = due_before
        .as_ref()
        .map(|now| format!("tasks/due/{}/", hex(now)));
    let mut result = if let Some(id) = task_id {
        load(tx, c, TASKS, &[json!(id)])
            .await?
            .into_iter()
            .collect()
    } else {
        let indexes = scan_indexes(
            tx,
            c,
            TASKS,
            &prefix,
            None,
            if thread_id.is_none() {
                through.as_deref()
            } else {
                None
            },
        )
        .await?;
        let mut rows = Vec::new();
        for index in indexes {
            rows.push(indexed_row(tx, c, TASKS, &index).await?);
        }
        rows
    };
    result.retain(|r| {
        thread_id.as_ref().is_none_or(|id| r["thread_id"] == *id)
            && due_before.as_ref().is_none_or(|now| {
                r["enabled"] == 1
                    && r["last_run_status"] != "running"
                    && r["next_run_at"]
                        .as_str()
                        .is_some_and(|at| at <= now.as_str())
            })
    });
    if due_before.is_some() {
        result.sort_by(|a, b| {
            a["next_run_at"]
                .as_str()
                .cmp(&b["next_run_at"].as_str())
                .then_with(|| a["task_id"].as_str().cmp(&b["task_id"].as_str()))
        });
    }
    Ok(result)
}
async fn read(tx: &mut Transaction, c: &ApplicationCatalog, query: ScheduledRead) -> Result<Value> {
    Ok(match query {
        ScheduledRead::Tasks {
            task_id,
            thread_id,
            due_before,
        } => rows(tasks(tx, c, task_id, thread_id, due_before).await?),
        ScheduledRead::NextDue {} => {
            json!([{"next_run_at":scan_indexes(tx,c,TASKS,"tasks/due/",Some(1),None).await?.first().map(|r|r.summary["next_run_at"].clone()).unwrap_or(Value::Null)}])
        }
        ScheduledRead::Deliveries {
            task_id,
            delivery_id,
        } => {
            let mut found = if let Some(id) = delivery_id {
                load(tx, c, DELIVERIES, &[json!(id)])
                    .await?
                    .into_iter()
                    .collect()
            } else {
                scan(
                    tx,
                    c,
                    DELIVERIES,
                    &format!("deliveries/task/{}/", hash(&task_id)),
                    None,
                )
                .await?
            };
            found.retain(|r| r["task_id"] == task_id);
            for row in &mut found {
                let pending = load(tx, c, PENDING, &[row["delivery_id"].clone()]).await?;
                row.insert(
                    "pending_window".into(),
                    json!(if pending.is_some_and(|p| matches!(
                        p["state"].as_str(),
                        Some("pending" | "dispatching")
                    )) {
                        1
                    } else {
                        0
                    }),
                );
            }
            rows(found)
        }
        ScheduledRead::Pending {
            delivery_id,
            task_id,
            states,
            coalesce_key,
            due_after,
            first,
        } => {
            let mut found = if let Some(id) = delivery_id {
                load(tx, c, PENDING, &[json!(id)])
                    .await?
                    .into_iter()
                    .collect()
            } else if let Some(task) = &task_id {
                let mut found = Vec::new();
                for index in scan_indexes(
                    tx,
                    c,
                    PENDING,
                    &format!("pending/task/{}/", hash(task)),
                    None,
                    None,
                )
                .await?
                {
                    let row = &index.summary;
                    if row["task_id"] != *task {
                        return Err(Error::IncompatibleStore);
                    }
                    if states.as_ref().is_none_or(|ss| {
                        row["state"]
                            .as_str()
                            .is_some_and(|s| ss.iter().any(|v| v == s))
                    }) && coalesce_key
                        .as_ref()
                        .is_none_or(|k| row["coalesce_key"] == *k)
                        && due_after
                            .is_none_or(|now| row["due_at"].as_i64().is_some_and(|at| at > now))
                    {
                        found.push(indexed_row(tx, c, PENDING, &index).await?);
                        if first {
                            break;
                        }
                    }
                }
                found
            } else {
                return Err(invalid("pending rows require a task or delivery identity"));
            };
            found.retain(|r| {
                task_id.as_ref().is_none_or(|id| r["task_id"] == *id)
                    && states.as_ref().is_none_or(|ss| {
                        r["state"]
                            .as_str()
                            .is_some_and(|s| ss.iter().any(|v| v == s))
                    })
                    && coalesce_key
                        .as_ref()
                        .is_none_or(|k| r["coalesce_key"] == *k)
                    && due_after.is_none_or(|now| r["due_at"].as_i64().is_some_and(|at| at > now))
            });
            if first {
                found.truncate(1);
            }
            rows(found)
        }
        ScheduledRead::PendingCount { task_id } => {
            json!([{"count":scan_indexes(tx,c,PENDING,&format!("pending/task/{}/",hash(&task_id)),None,None).await?.iter()
            .filter(|r|r.summary["task_id"]==task_id&&matches!(r.summary["state"].as_str(),Some("pending"|"dispatching"|"running"))).count()}])
        }
        ScheduledRead::NextPending {} => {
            json!([{"due_at":scan_indexes(tx,c,PENDING,&format!("pending/state/{}/",hex("pending")),Some(1),None).await?.first().map(|r|r.summary["due_at"].clone()).unwrap_or(Value::Null)}])
        }
        ScheduledRead::ActivePendingTasks {} => {
            let mut ids = BTreeSet::new();
            for state in ["dispatching", "running", "completed", "failed"] {
                for row in scan_indexes(
                    tx,
                    c,
                    PENDING,
                    &format!("pending/state/{}/", hex(state)),
                    None,
                    None,
                )
                .await?
                {
                    ids.insert(string(&row.summary, "task_id")?.to_owned());
                }
            }
            Value::Array(
                ids.into_iter()
                    .map(|task_id| json!({"task_id":task_id}))
                    .collect(),
            )
        }
        ScheduledRead::HasReplay {
            task_id,
            replay_key,
            now,
        } => match load(tx, c, REPLAYS, &[json!(task_id), json!(replay_key)]).await? {
            Some(row) if int(&row, "expires_at")? > now => json!([{"present":1}]),
            _ => json!([]),
        },
    })
}

async fn write(
    tx: &mut Transaction,
    c: &ApplicationCatalog,
    mutation: ScheduledWrite,
) -> Result<Value> {
    match mutation {
        ScheduledWrite::SaveTask {
            mut row,
            require_existing,
            kept_schedule_json,
            secret_changed,
        } => {
            // The SQL settings INSERT never accepts execution ownership from its caller.
            row.insert("last_run_webhook_delivery_id".into(), Value::Null);
            validate_plain(TASKS, &row)?;
            let id = row["task_id"].clone();
            let previous = load(tx, c, TASKS, &[id.clone()]).await?;
            if require_existing && previous.is_none() {
                return Ok(json!([]));
            }
            if let Some(old) = previous {
                let incoming = row.clone();
                for field in [
                    "created_by",
                    "created_at",
                    "last_run_at",
                    "last_run_status",
                    "last_run_error",
                    "run_count",
                    "last_run_webhook_delivery_id",
                ] {
                    row.insert(field.into(), old[field].clone());
                }
                row.insert(
                    "thread_id".into(),
                    if incoming["thread_mode"] == "new_thread" {
                        Value::Null
                    } else if incoming["project_id"] != old["project_id"] {
                        incoming["thread_id"].clone()
                    } else if incoming["thread_id"].is_null() {
                        old["thread_id"].clone()
                    } else {
                        incoming["thread_id"].clone()
                    },
                );
                if kept_schedule_json.as_ref().is_some_and(|kept| {
                    (old["schedule_json"] == incoming["schedule_json"]
                        || old["schedule_json"] == *kept)
                        && old["enabled"] == incoming["enabled"]
                }) {
                    row.insert("next_run_at".into(), old["next_run_at"].clone());
                }
                row.insert(
                    "webhook_token".into(),
                    if incoming["webhook_token"].is_null() {
                        Value::Null
                    } else if old["webhook_token"].is_null() {
                        incoming["webhook_token"].clone()
                    } else {
                        old["webhook_token"].clone()
                    },
                );
                if !secret_changed {
                    row.insert("webhook_secret".into(), old["webhook_secret"].clone());
                }
            }
            put(tx, c, TASKS, row).await?;
            Ok(returning("task_id", id))
        }
        ScheduledWrite::PatchTask {
            task_id,
            thread_ids,
            expected,
            different,
            patch: changes,
            increment_run_count,
        } => {
            let candidates = if let Some(ids) = thread_ids {
                let mut out = Vec::new();
                for id in ids.into_iter().collect::<BTreeSet<_>>() {
                    out.extend(tasks(tx, c, None, Some(id), None).await?);
                }
                out
            } else if task_id.is_some() {
                tasks(tx, c, task_id, None, None).await?
            } else {
                return Err(invalid("task patch requires an identity"));
            };
            let mut changed = Vec::new();
            for mut row in candidates {
                if !selected(&row, &expected, &different) {
                    continue;
                }
                patch(TASKS, &mut row, &changes)?;
                if increment_run_count {
                    increment(&mut row)?;
                }
                let id = row["task_id"].clone();
                put(tx, c, TASKS, row).await?;
                changed.push(json!({"task_id":id}));
            }
            Ok(Value::Array(changed))
        }
        ScheduledWrite::DeleteTasks { task_id, thread_id } => {
            if task_id.is_none() && thread_id.is_none() {
                return Err(invalid("task deletion requires an identity"));
            }
            let mut changed = Vec::new();
            for row in tasks(tx, c, task_id, thread_id, None).await? {
                let id = string(&row, "task_id")?;
                for (table, prefix) in [
                    (DELIVERIES, format!("deliveries/task/{}/", hash(id))),
                    (REPLAYS, format!("replays/task/{}/", hash(id))),
                    (PENDING, format!("pending/task/{}/", hash(id))),
                ] {
                    for child in scan(tx, c, table, &prefix, None).await? {
                        if string(&child, "task_id")? != id {
                            return Err(Error::IncompatibleStore);
                        }
                        remove(tx, c, table, &primary(table, &child)?).await?;
                    }
                }
                remove(tx, c, TASKS, &[json!(id)]).await?;
                changed.push(json!({"task_id":id}));
            }
            Ok(Value::Array(changed))
        }
        ScheduledWrite::RecoverTask {
            task_id,
            patch: changes,
            pending_delivery_id,
            match_pending_task,
        } => {
            let Some(mut row) = load(tx, c, TASKS, &[json!(task_id)]).await? else {
                return Ok(json!([]));
            };
            if row["last_run_status"] != "running" {
                return Ok(json!([]));
            }
            let delivery = if match_pending_task {
                row["last_run_webhook_delivery_id"]
                    .as_str()
                    .map(str::to_owned)
            } else {
                pending_delivery_id
            };
            let pending = if let Some(id) = delivery {
                load(tx, c, PENDING, &[json!(id)]).await?
            } else {
                None
            };
            if !pending.is_some_and(|p| {
                p["state"] == "running" && (!match_pending_task || p["task_id"] == task_id)
            }) {
                increment(&mut row)?;
            }
            patch(TASKS, &mut row, &changes)?;
            put(tx, c, TASKS, row).await?;
            Ok(json!([]))
        }
        ScheduledWrite::RecordDelivery { mut row } => {
            validate_plain(DELIVERIES, &row)?;
            if load(tx, c, TASKS, &[row["task_id"].clone()])
                .await?
                .is_none()
            {
                return Ok(json!([]));
            }
            if let Some(mut old) = load(tx, c, DELIVERIES, &primary(DELIVERIES, &row)?).await? {
                for field in [
                    "outcome",
                    "signature_verified",
                    "missing_fields_json",
                    "rendered_prompt",
                    "error",
                ] {
                    old.insert(field.into(), row[field].clone());
                }
                row = old;
            }
            put(tx, c, DELIVERIES, row).await?;
            Ok(json!([]))
        }
        ScheduledWrite::PatchDelivery {
            delivery_id,
            patch: changes,
        } => {
            let Some(mut row) = load(tx, c, DELIVERIES, &[json!(delivery_id)]).await? else {
                return Ok(json!([]));
            };
            patch(DELIVERIES, &mut row, &changes)?;
            let id = row["task_id"].clone();
            put(tx, c, DELIVERIES, row).await?;
            Ok(returning("task_id", id))
        }
        ScheduledWrite::PruneDeliveries {
            task_id,
            bucket,
            limit,
        } => {
            if !["main", "filtered", "dedupe"].contains(&bucket.as_str()) || limit > 500 {
                return Err(invalid("invalid webhook retention policy"));
            }
            let mut retained = 0;
            for row in scan(
                tx,
                c,
                DELIVERIES,
                &format!("deliveries/task/{}/", hash(&task_id)),
                None,
            )
            .await?
            {
                if row["task_id"] != task_id {
                    return Err(Error::IncompatibleStore);
                }
                let kind = if row["outcome"] == "filtered" {
                    "filtered"
                } else if matches!(
                    row["outcome"].as_str(),
                    Some("duplicate" | "coalesced" | "rate_limited")
                ) || row["error"] == "The filled-in prompt is too long."
                {
                    "dedupe"
                } else {
                    "main"
                };
                if kind != bucket {
                    continue;
                }
                if retained >= limit {
                    remove(tx, c, DELIVERIES, &primary(DELIVERIES, &row)?).await?;
                }
                retained += 1;
            }
            Ok(json!([]))
        }
        ScheduledWrite::ClaimRelay {
            relay_delivery_id,
            task_id,
            seen_at,
        } => {
            if load(tx, c, RELAYS, &[json!(relay_delivery_id)])
                .await?
                .is_some()
            {
                return Ok(json!([]));
            }
            put(
                tx,
                c,
                RELAYS,
                json!({"relay_delivery_id":relay_delivery_id,"task_id":task_id,"seen_at":seen_at})
                    .as_object()
                    .unwrap()
                    .clone(),
            )
            .await?;
            Ok(returning("relay_delivery_id", json!(relay_delivery_id)))
        }
        ScheduledWrite::DeleteRelay { relay_delivery_id } => {
            remove(tx, c, RELAYS, &[json!(relay_delivery_id)]).await?;
            Ok(json!([]))
        }
        ScheduledWrite::PruneRelays { before } => {
            for index in scan_indexes(
                tx,
                c,
                RELAYS,
                "relays/seen/",
                None,
                Some(&format!("relays/seen/{}/", hex(&before))),
            )
            .await?
            {
                if string(&index.summary, "seen_at")? >= before.as_str() {
                    break;
                }
                remove(tx, c, RELAYS, &index.primary).await?;
            }
            Ok(json!([]))
        }
        ScheduledWrite::ClaimReplay {
            task_id,
            replay_key,
            delivery_id,
            now,
            expires_at,
        } => {
            for index in scan_indexes(
                tx,
                c,
                REPLAYS,
                "replays/expiry/",
                None,
                Some(&format!("replays/expiry/{}/", number(now))),
            )
            .await?
            {
                remove(tx, c, REPLAYS, &index.primary).await?;
            }
            if load(tx, c, REPLAYS, &[json!(task_id), json!(replay_key)])
                .await?
                .is_some()
            {
                return Err(Error::DuplicateRecord);
            }
            put(tx,c,REPLAYS,json!({"task_id":task_id,"replay_key":replay_key,"delivery_id":delivery_id,"expires_at":expires_at}).as_object().unwrap().clone()).await?;
            Ok(json!([]))
        }
        ScheduledWrite::ReleaseReplay { delivery_id } => {
            for row in scan(
                tx,
                c,
                REPLAYS,
                &format!("replays/delivery/{}/", hash(&delivery_id)),
                None,
            )
            .await?
            {
                if row["delivery_id"] != delivery_id {
                    return Err(Error::IncompatibleStore);
                }
                remove(tx, c, REPLAYS, &primary(REPLAYS, &row)?).await?;
            }
            Ok(json!([]))
        }
        ScheduledWrite::InsertPending { mut row } => {
            row.entry("state").or_insert(json!("pending"));
            validate_plain(PENDING, &row)?;
            if load(tx, c, PENDING, &primary(PENDING, &row)?)
                .await?
                .is_some()
            {
                return Err(Error::DuplicateRecord);
            }
            put(tx, c, PENDING, row).await?;
            Ok(json!([]))
        }
        ScheduledWrite::PatchPending {
            delivery_id,
            states,
            not_states,
            patch: changes,
            append_delivery_id,
        } => {
            let Some(mut row) = load(tx, c, PENDING, &[json!(delivery_id)]).await? else {
                return Ok(json!([]));
            };
            let state = string(&row, "state")?;
            if states
                .as_ref()
                .is_some_and(|ss| !ss.iter().any(|s| s == state))
                || not_states
                    .as_ref()
                    .is_some_and(|ss| ss.iter().any(|s| s == state))
            {
                return Ok(json!([]));
            }
            patch(PENDING, &mut row, &changes)?;
            if let Some(id) = append_delivery_id {
                let mut ids: Value = serde_json::from_str(string(&row, "delivery_ids_json")?)
                    .map_err(|_| invalid("invalid pending delivery IDs"))?;
                if let Some(array) = ids.as_array_mut() {
                    array.push(json!(id));
                    row.insert(
                        "delivery_ids_json".into(),
                        json!(serde_json::to_string(&ids).map_err(crate::json_error)?),
                    );
                }
            }
            put(tx, c, PENDING, row).await?;
            Ok(returning("delivery_id", json!(delivery_id)))
        }
        ScheduledWrite::DeletePending { delivery_id } => {
            remove(tx, c, PENDING, &[json!(delivery_id)]).await?;
            Ok(json!([]))
        }
        ScheduledWrite::ExpirePending {
            now,
            task_id,
            coalesce_key,
        } => {
            for index in scan_indexes(
                tx,
                c,
                PENDING,
                &format!("pending/state/{}/", hex("pending")),
                None,
                Some(&format!(
                    "pending/state/{}/{}/",
                    hex("pending"),
                    number(now)
                )),
            )
            .await?
            {
                let mut row = indexed_row(tx, c, PENDING, &index).await?;
                if task_id.as_ref().is_some_and(|id| row["task_id"] != *id)
                    || coalesce_key
                        .as_ref()
                        .is_some_and(|k| row["coalesce_key"] != *k)
                {
                    continue;
                }
                row.insert("state".into(), json!("dispatching"));
                put(tx, c, PENDING, row).await?;
            }
            Ok(json!([]))
        }
        ScheduledWrite::ReleaseFailedRelays { delivery_id } => {
            if let Some(row) = load(tx, c, PENDING, &[json!(delivery_id)]).await? {
                let ids: Value = serde_json::from_str(string(&row, "delivery_ids_json")?)
                    .map_err(|_| invalid("invalid pending delivery IDs"))?;
                if let Some(ids) = ids.as_array() {
                    for id in ids {
                        if let Some(id) = id
                            .as_str()
                            .filter(|v| {
                                v.get(..15).is_some_and(|prefix| {
                                    prefix.eq_ignore_ascii_case("delivery:relay:")
                                })
                            })
                            .map(|v| &v[15..])
                        {
                            remove(tx, c, RELAYS, &[json!(id)]).await?;
                        }
                    }
                }
            }
            if let Some(id) = delivery_id.strip_prefix("delivery:relay:") {
                remove(tx, c, RELAYS, &[json!(id)]).await?;
            }
            Ok(json!([]))
        }
    }
}

#[cfg(test)]
#[path = "redb_scheduled_rows_tests.rs"]
mod tests;
