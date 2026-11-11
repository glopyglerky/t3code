//! Indexed V1 import source rows and mutable import receipts in the application owner.

use std::collections::BTreeSet;
use std::time::Duration;

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Map, Value, json};

use crate::application::{
    ApplicationCatalog, ApplicationTransactions, Session, decode_catalog, decode_row, row_prefix,
    schema_key, table,
};
use crate::application_events::{EventRead, read_events};
use crate::application_rows::{
    int_cell, integer, managed_marker, nullable, nullable_cell, read_row, text, text_cell,
    write_row,
};
use crate::backend::Transaction;
use crate::{Error, Result, Store, request_digest, store_key};

const THREADS: &str = "projection_threads";
const MESSAGES: &str = "projection_thread_messages";
const PULL_REQUESTS: &str = "projection_thread_pull_requests";
const IMPORTS: &str = "orchestration_v2_legacy_imports";
const V2_THREADS: &str = "orchestration_v2_projection_threads";
const TABLES: [&str; 4] = [THREADS, MESSAGES, PULL_REQUESTS, IMPORTS];
const MARKER: &str = "legacy-v1-v1";
const PAGE_ROWS: usize = 128;
const PREPARE_ROWS: usize = 32;
const THREAD_FIELDS: [&str; 23] = [
    "thread_id",
    "project_id",
    "title",
    "model_selection_json",
    "runtime_mode",
    "interaction_mode",
    "branch",
    "worktree_path",
    "created_at",
    "updated_at",
    "archived_at",
    "settled_override",
    "settled_at",
    "unsettled_at",
    "snoozed_until",
    "snoozed_at",
    "pinned_at",
    "auto_settle_disabled_at",
    "pin_order_key",
    "linked_pull_request_json",
    "branch_pull_request_json",
    "active_order_key",
    "deleted_at",
];
const MESSAGE_FIELDS: [&str; 9] = [
    "message_id",
    "thread_id",
    "role",
    "text",
    "attachments_json",
    "context_json",
    "is_streaming",
    "created_at",
    "updated_at",
];
const REPAIR_FIELDS: [&str; 9] = [
    "pinnedAt",
    "pinOrderKey",
    "snoozedUntil",
    "snoozedAt",
    "unsettledAt",
    "linkedPullRequest",
    "pullRequests",
    "branchPullRequest",
    "activeOrderKey",
];

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MessageMode {
    All,
    Latest,
    LatestUser,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum LegacyV1Read {
    Messages {
        thread_id: String,
        mode: MessageMode,
    },
    Repairs,
    PendingThreads,
    PendingCount,
    GetImport {
        thread_id: String,
    },
    ImportedEvents {
        thread_id: String,
    },
    PendingImports,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum LegacyV1Write {
    InsertPosition {
        thread_id: String,
        turn_item_id: String,
        ordinal: i64,
    },
    InsertShell {
        thread_id: String,
        source_updated_at: String,
        shell_imported_at: String,
        imported_message_count: i64,
    },
    Complete {
        thread_id: String,
        completed_at: String,
        message_count: i64,
    },
    Failure {
        thread_id: String,
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
    domain: usize,
    after: Option<String>,
    indexed_rows: u64,
}

/// Index witnesses contain identities and ordering only, never message or thread payloads.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
struct Entry {
    table: String,
    identity: Vec<Value>,
    thread_id: String,
    order: [String; 2],
    role: Option<String>,
    number: Option<i64>,
    pending: bool,
}

fn invalid(message: &str) -> Error {
    Error::Invalid(message.into())
}
fn encode(value: &impl Serialize) -> Result<Vec<u8>> {
    serde_json::to_vec(value).map_err(crate::json_error)
}
fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    serde_json::from_slice(bytes).map_err(|_| Error::IncompatibleStore)
}
fn hex(value: &str) -> String {
    value.bytes().map(|byte| format!("{byte:02x}")).collect()
}
fn key(catalog: &ApplicationCatalog, suffix: &str) -> Vec<u8> {
    store_key(&format!(
        "application/generation/{}/legacy-v1/v1/{suffix}",
        catalog.generation
    ))
}
fn end(prefix: &[u8]) -> Result<Vec<u8>> {
    let mut value = prefix.to_vec();
    *value.last_mut().ok_or(Error::IncompatibleStore)? += 1;
    Ok(value)
}
fn scope(kind: &str, thread: &str) -> String {
    format!("{kind}/{}/", request_digest(&json!([thread])))
}

impl Entry {
    fn from_row(name: &str, row: &Map<String, Value>) -> Result<Option<Self>> {
        let thread_id = text(row, "thread_id")?;
        let mut result = Self {
            table: name.into(),
            identity: vec![text_cell(&thread_id)],
            thread_id: thread_id.clone(),
            order: [String::new(), thread_id],
            role: None,
            number: None,
            pending: false,
        };
        match name {
            THREADS => result.order[0] = text(row, "created_at")?,
            MESSAGES => {
                let role = text(row, "role")?;
                if !matches!(role.as_str(), "user" | "assistant") {
                    return Ok(None);
                }
                result.order = [text(row, "created_at")?, text(row, "message_id")?];
                result.identity = vec![text_cell(&result.order[1])];
                result.role = Some(role);
            }
            PULL_REQUESTS => {
                result.order = [text(row, "host")?, text(row, "repository")?];
                result.number = Some(integer(row, "number")?);
                result.identity = vec![
                    text_cell(&result.thread_id),
                    text_cell(&result.order[0]),
                    text_cell(&result.order[1]),
                    int_cell(result.number.unwrap()),
                ];
            }
            IMPORTS => {
                result.order[0] = text(row, "shell_imported_at")?;
                result.pending = nullable(row, "transcript_imported_at")?.is_none();
            }
            _ => return Err(Error::IncompatibleStore),
        }
        Ok(Some(result))
    }
    fn keys(&self, catalog: &ApplicationCatalog) -> Result<Vec<Vec<u8>>> {
        let ordered = format!("{}/{}", hex(&self.order[0]), hex(&self.order[1]));
        let suffix = match self.table.as_str() {
            THREADS => format!("threads/{ordered}"),
            MESSAGES => format!("{}{ordered}", scope("messages", &self.thread_id)),
            PULL_REQUESTS => format!(
                "{}{ordered}/{:016x}",
                scope("pull-requests", &self.thread_id),
                (self.number.ok_or(Error::IncompatibleStore)? as u64) ^ (1_u64 << 63)
            ),
            IMPORTS => {
                let mut values = vec![key(
                    catalog,
                    &format!("imports/all/{}", hex(&self.thread_id)),
                )];
                if self.pending {
                    values.push(key(catalog, &format!("imports/pending/{ordered}")));
                }
                return Ok(values);
            }
            _ => return Err(Error::IncompatibleStore),
        };
        Ok(vec![key(catalog, &suffix)])
    }
}

async fn insert_entry(
    tx: &mut Transaction,
    catalog: &ApplicationCatalog,
    entry: &Entry,
) -> Result<()> {
    for key in entry.keys(catalog)? {
        if let Some(bytes) = tx.get(&key).await? {
            if decode::<Entry>(&bytes)? != *entry {
                return Err(Error::IncompatibleStore);
            }
        }
        tx.put(key, encode(entry)?).await?;
    }
    Ok(())
}

async fn remove_entry(
    tx: &mut Transaction,
    catalog: &ApplicationCatalog,
    entry: &Entry,
) -> Result<()> {
    for key in entry.keys(catalog)? {
        if decode::<Entry>(&tx.get(&key).await?.ok_or(Error::IncompatibleStore)?)? != *entry {
            return Err(Error::IncompatibleStore);
        }
        tx.delete(key).await?;
    }
    Ok(())
}

async fn ready(session: &mut Session) -> Result<()> {
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
            return Err(invalid("legacy V1 import source indexes are not prepared"));
        }
    }
    let state: Preparation = decode(
        &session
            .transaction
            .get(&key(&session.catalog, "preparation"))
            .await?
            .ok_or(Error::IncompatibleStore)?,
    )?;
    if state.version != 1 || state.domain != TABLES.len() {
        return Err(invalid("legacy V1 import source indexes are not prepared"));
    }
    Ok(())
}

async fn entries(session: &mut Session, suffix: &str) -> Result<Vec<Entry>> {
    let prefix = key(&session.catalog, suffix);
    let stop = end(&prefix)?;
    let mut cursor = prefix;
    let mut output = Vec::new();
    loop {
        let page = session.transaction.scan(&cursor, &stop, PAGE_ROWS).await?;
        let count = page.len();
        for (encoded, bytes) in page {
            let value: Entry = decode(&bytes)?;
            if !value.keys(&session.catalog)?.contains(&encoded) {
                return Err(Error::IncompatibleStore);
            }
            cursor = encoded;
            cursor.push(0);
            output.push(value);
        }
        if count < PAGE_ROWS {
            return Ok(output);
        }
    }
}

async fn entry_row(session: &mut Session, entry: &Entry) -> Result<Map<String, Value>> {
    let row = read_row(
        &mut session.transaction,
        &session.catalog,
        &entry.table,
        &entry.identity,
    )
    .await?
    .ok_or(Error::IncompatibleStore)?
    .row;
    if Entry::from_row(&entry.table, &row)?.as_ref() != Some(entry) {
        return Err(Error::IncompatibleStore);
    }
    Ok(row)
}

fn scalar(cell: &Value) -> Result<Value> {
    match cell.get("type").and_then(Value::as_str) {
        Some("null") => Ok(Value::Null),
        Some("text") if cell.get("encoding").is_none() => Ok(Value::String(
            cell.get("value")
                .and_then(Value::as_str)
                .ok_or(Error::IncompatibleStore)?
                .into(),
        )),
        Some("integer") => Ok(json!(
            cell.get("value")
                .and_then(Value::as_str)
                .ok_or(Error::IncompatibleStore)?
                .parse::<i64>()
                .map_err(|_| Error::IncompatibleStore)?
        )),
        _ => Err(Error::IncompatibleStore),
    }
}
fn selected(row: &Map<String, Value>, fields: &[&str]) -> Result<Map<String, Value>> {
    fields
        .iter()
        .map(|name| {
            Ok((
                (*name).into(),
                scalar(row.get(*name).ok_or(Error::IncompatibleStore)?)?,
            ))
        })
        .collect()
}
fn json_column(row: &Map<String, Value>, name: &str) -> Result<Value> {
    nullable(row, name)?
        .map(|text| serde_json::from_str(&text).map_err(|_| Error::IncompatibleStore))
        .transpose()
        .map(|value| value.unwrap_or(Value::Null))
}

async fn thread_row(session: &mut Session, row: &Map<String, Value>) -> Result<Map<String, Value>> {
    let mut output = selected(row, &THREAD_FIELDS)?;
    let thread = text(row, "thread_id")?;
    let mut links = Vec::new();
    for entry in entries(session, &scope("pull-requests", &thread)).await? {
        let row = entry_row(session, &entry).await?;
        links.push(json!({"host":text(&row,"host")?,"repository":text(&row,"repository")?,"number":integer(&row,"number")?,"url":text(&row,"url")?,"source":text(&row,"source")?,"linkedAt":text(&row,"linked_at")?,"snapshot":json_column(&row,"snapshot_json")?,"stack":json_column(&row,"stack_json")?}));
    }
    output.insert(
        "pull_requests_json".into(),
        Value::String(serde_json::to_string(&links).map_err(crate::json_error)?),
    );
    Ok(output)
}

async fn messages(session: &mut Session, thread: &str, mode: MessageMode) -> Result<Vec<Value>> {
    let mut chosen = Vec::new();
    for (offset, entry) in entries(session, &scope("messages", thread))
        .await?
        .into_iter()
        .enumerate()
    {
        if matches!(mode, MessageMode::LatestUser) && entry.role.as_deref() != Some("user") {
            continue;
        }
        if !matches!(mode, MessageMode::All) {
            chosen.clear();
        }
        chosen.push((entry, offset + 1));
    }
    let mut output = Vec::new();
    for (entry, ordinal) in chosen {
        let row = entry_row(session, &entry).await?;
        let mut row = selected(&row, &MESSAGE_FIELDS)?;
        row.insert("ordinal".into(), json!(ordinal));
        output.push(Value::Object(row));
    }
    Ok(output)
}

async fn has_created(session: &mut Session, thread: &str) -> Result<bool> {
    Ok(!read_events(
        session,
        EventRead {
            after_sequence: 0,
            through_sequence: None,
            thread_id: Some(thread.into()),
            command_id: None,
            event_type: Some("thread.created".into()),
            only_agent_events: Some(true),
            limit: 1,
        },
    )
    .await?
    .is_empty())
}

async fn pending_threads(session: &mut Session) -> Result<Vec<Entry>> {
    let mut result = Vec::new();
    for entry in entries(session, "threads/").await? {
        if !has_created(session, &entry.thread_id).await? {
            result.push(entry);
        }
    }
    Ok(result)
}

fn import_row(row: &Map<String, Value>) -> Result<Value> {
    Ok(Value::Object(selected(
        row,
        &["thread_id", "transcript_imported_at"],
    )?))
}

async fn read(session: &mut Session, query: LegacyV1Read) -> Result<Value> {
    ready(session).await?;
    Ok(match query {
        LegacyV1Read::Messages { thread_id, mode } => {
            json!(messages(session, &thread_id, mode).await?)
        }
        LegacyV1Read::PendingThreads => {
            let mut output = Vec::new();
            for entry in pending_threads(session).await? {
                let row = entry_row(session, &entry).await?;
                output.push(thread_row(session, &row).await?);
            }
            json!(output)
        }
        LegacyV1Read::PendingCount => {
            let mut threads: BTreeSet<String> = pending_threads(session)
                .await?
                .into_iter()
                .map(|entry| entry.thread_id)
                .collect();
            threads.extend(
                entries(session, "imports/pending/")
                    .await?
                    .into_iter()
                    .map(|entry| entry.thread_id),
            );
            json!(threads.len())
        }
        LegacyV1Read::GetImport { thread_id } => {
            let value = read_row(
                &mut session.transaction,
                &session.catalog,
                IMPORTS,
                &[text_cell(&thread_id)],
            )
            .await?;
            json!(
                value
                    .as_ref()
                    .map(|value| import_row(&value.row))
                    .transpose()?
                    .into_iter()
                    .collect::<Vec<_>>()
            )
        }
        LegacyV1Read::PendingImports => {
            let mut output = Vec::new();
            for entry in entries(session, "imports/pending/").await? {
                output.push(import_row(&entry_row(session, &entry).await?)?);
            }
            json!(output)
        }
        LegacyV1Read::Repairs => {
            let mut output = Vec::new();
            for entry in entries(session, "imports/all/").await? {
                let source = read_row(
                    &mut session.transaction,
                    &session.catalog,
                    THREADS,
                    &[text_cell(&entry.thread_id)],
                )
                .await?;
                let projection = read_row(
                    &mut session.transaction,
                    &session.catalog,
                    V2_THREADS,
                    &[text_cell(&entry.thread_id)],
                )
                .await?;
                if let (Some(source), Some(projection)) = (source, projection) {
                    let payload = text(&projection.row, "payload_json")?;
                    let parsed: Value =
                        serde_json::from_str(&payload).map_err(|_| Error::IncompatibleStore)?;
                    if REPAIR_FIELDS.iter().any(|name| parsed.get(*name).is_none()) {
                        let mut result = thread_row(session, &source.row).await?;
                        result.insert("payload_json".into(), json!(payload));
                        output.push((text(&source.row, "created_at")?, entry.thread_id, result));
                    }
                }
            }
            output.sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));
            json!(
                output
                    .into_iter()
                    .map(|(_, _, row)| row)
                    .collect::<Vec<_>>()
            )
        }
        LegacyV1Read::ImportedEvents { thread_id } => {
            let mut after_sequence = 0;
            let mut output = Vec::new();
            loop {
                let events = read_events(
                    session,
                    EventRead {
                        after_sequence,
                        through_sequence: None,
                        thread_id: Some(thread_id.clone()),
                        command_id: None,
                        event_type: None,
                        only_agent_events: Some(true),
                        limit: PAGE_ROWS as u32,
                    },
                )
                .await?;
                let count = events.len();
                for event in events {
                    after_sequence = event.sequence;
                    if event
                        .event_id
                        .get(..21)
                        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("migration:v1:message:"))
                    {
                        output.push(json!({"event_id":event.event_id}));
                    }
                }
                if count < PAGE_ROWS {
                    break;
                }
            }
            json!(output)
        }
    })
}

async fn save_import(
    session: &mut Session,
    previous: Option<&Map<String, Value>>,
    next: Map<String, Value>,
) -> Result<()> {
    if let Some(previous) = previous {
        remove_entry(
            &mut session.transaction,
            &session.catalog,
            &Entry::from_row(IMPORTS, previous)?.ok_or(Error::IncompatibleStore)?,
        )
        .await?;
    }
    insert_entry(
        &mut session.transaction,
        &session.catalog,
        &Entry::from_row(IMPORTS, &next)?.ok_or(Error::IncompatibleStore)?,
    )
    .await?;
    write_row(&mut session.transaction, &session.catalog, IMPORTS, next).await
}

async fn write(session: &mut Session, operation: LegacyV1Write) -> Result<Value> {
    session.transaction.require_write()?;
    ready(session).await?;
    match operation {
        LegacyV1Write::InsertPosition {
            thread_id,
            turn_item_id,
            ordinal,
        } => {
            crate::application_positions::insert_imported_position(
                session,
                thread_id,
                turn_item_id,
                ordinal,
            )
            .await?
        }
        LegacyV1Write::InsertShell {
            thread_id,
            source_updated_at,
            shell_imported_at,
            imported_message_count,
        } => {
            if imported_message_count < 0 {
                return Err(invalid("legacy imported message count must be nonnegative"));
            }
            if read_row(
                &mut session.transaction,
                &session.catalog,
                IMPORTS,
                &[text_cell(&thread_id)],
            )
            .await?
            .is_none()
            {
                save_import(
                    session,
                    None,
                    Map::from_iter([
                        ("thread_id".into(), text_cell(&thread_id)),
                        ("source_updated_at".into(), text_cell(&source_updated_at)),
                        ("shell_imported_at".into(), text_cell(&shell_imported_at)),
                        ("transcript_imported_at".into(), nullable_cell(None)),
                        (
                            "imported_message_count".into(),
                            int_cell(imported_message_count),
                        ),
                        ("last_error".into(), nullable_cell(None)),
                    ]),
                )
                .await?;
            }
        }
        LegacyV1Write::Complete {
            thread_id,
            completed_at,
            message_count,
        } => {
            if message_count < 0 {
                return Err(invalid("legacy imported message count must be nonnegative"));
            }
            if let Some(previous) = read_row(
                &mut session.transaction,
                &session.catalog,
                IMPORTS,
                &[text_cell(&thread_id)],
            )
            .await?
            {
                let mut next = previous.row.clone();
                next.insert("transcript_imported_at".into(), text_cell(&completed_at));
                next.insert("imported_message_count".into(), int_cell(message_count));
                next.insert("last_error".into(), nullable_cell(None));
                save_import(session, Some(&previous.row), next).await?;
            }
        }
        LegacyV1Write::Failure { thread_id } => {
            if let Some(previous) = read_row(
                &mut session.transaction,
                &session.catalog,
                IMPORTS,
                &[text_cell(&thread_id)],
            )
            .await?
            {
                let mut next = previous.row.clone();
                next.insert(
                    "last_error".into(),
                    text_cell("Transcript hydration failed; retry on next open."),
                );
                save_import(session, Some(&previous.row), next).await?;
            }
        }
    }
    Ok(Value::Null)
}

impl Store {
    pub async fn prepare_application_legacy_v1(&self) -> Result<PreparationResult> {
        let mut limit = PREPARE_ROWS;
        loop {
            match self.prepare_application_legacy_v1_batch(limit).await {
                Err(Error::ResourceLimit(_)) if limit > 1 => limit = (limit / 2).max(1),
                result => return result,
            }
        }
    }
    async fn prepare_application_legacy_v1_batch(&self, limit: usize) -> Result<PreparationResult> {
        let mut tx = self
            .db
            .begin_reserved(Duration::from_secs(5), Duration::from_secs(300))
            .await?;
        let catalog = decode_catalog(
            &tx.get(&schema_key())
                .await?
                .ok_or_else(|| invalid("application schema is not initialized"))?,
        )?;
        for name in TABLES {
            let definition = table(&catalog, name)?;
            let expected = match name {
                MESSAGES => vec!["message_id"],
                PULL_REQUESTS => vec!["thread_id", "host", "repository", "number"],
                _ => vec!["thread_id"],
            };
            if definition.primary_key != expected {
                return Err(Error::IncompatibleStore);
            }
        }
        let state_key = key(&catalog, "preparation");
        let mut state = match tx.get(&state_key).await? {
            Some(bytes) => {
                let state: Preparation = decode(&bytes)?;
                if state.version != 1 || state.domain > TABLES.len() {
                    return Err(Error::IncompatibleStore);
                }
                for name in TABLES {
                    if tx
                        .get(&managed_marker(&catalog, name))
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
                    let marker = managed_marker(&catalog, name);
                    if tx.get(&marker).await?.is_some() {
                        return Err(Error::IncompatibleStore);
                    }
                    tx.put(marker, encode(&MARKER)?).await?;
                }
                Preparation {
                    version: 1,
                    domain: 0,
                    after: None,
                    indexed_rows: 0,
                }
            }
        };
        if state.domain == TABLES.len() {
            tx.rollback().await?;
            return Ok(PreparationResult {
                prepared: true,
                indexed_rows: state.indexed_rows,
            });
        }
        let name = TABLES[state.domain];
        let prefix = row_prefix(&catalog.generation, name)?;
        let mut cursor = prefix.clone();
        if let Some(after) = &state.after {
            if after.len() != 64
                || !after
                    .bytes()
                    .all(|value| value.is_ascii_hexdigit() && !value.is_ascii_uppercase())
            {
                return Err(Error::IncompatibleStore);
            }
            cursor.extend_from_slice(after.as_bytes());
            cursor.push(0);
        }
        let rows = tx.scan(&cursor, &end(&prefix)?, limit).await?;
        let count = rows.len();
        for (encoded, bytes) in rows {
            let row = decode_row(&catalog, table(&catalog, name)?, &encoded, &bytes)?;
            if let Some(entry) = Entry::from_row(name, &row.row)? {
                insert_entry(&mut tx, &catalog, &entry).await?;
            }
            state.after = Some(
                std::str::from_utf8(
                    encoded
                        .strip_prefix(prefix.as_slice())
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
            state.domain += 1;
            state.after = None;
        }
        tx.put(state_key, encode(&state)?).await?;
        tx.commit().await?;
        Ok(PreparationResult {
            prepared: state.domain == TABLES.len(),
            indexed_rows: state.indexed_rows,
        })
    }
}

impl ApplicationTransactions {
    pub async fn legacy_v1_read(&mut self, id: &str, query: LegacyV1Read) -> Result<Value> {
        let mut session = self.take(id)?;
        let result = async {
            let result = read(&mut session, query).await?;
            crate::validate_response_bytes(&result)?;
            Ok(result)
        }
        .await;
        self.finish(id, session, result).await
    }
    pub async fn legacy_v1_write(&mut self, id: &str, operation: LegacyV1Write) -> Result<Value> {
        let mut session = self.take(id)?;
        let result = write(&mut session, operation).await;
        self.finish(id, session, result).await
    }
}
