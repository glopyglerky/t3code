//! Provider process state and MCP application context in the shared application owner.

use std::time::Duration;

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Map, Value, json};

use crate::application::{
    ApplicationCatalog, ApplicationTransactions, Session, decode_catalog, decode_row, row_prefix,
    schema_key, table,
};
use crate::application_json::{compact_json, field, object_fields, render_fields, set_field};
use crate::application_rows::{
    delete_row, managed_marker, nullable, nullable_cell, read_row, text, text_cell, validate_table,
    write_row,
};
use crate::backend::Transaction;
use crate::{Error, Result, Store, request_digest, store_key};

const RUNTIME: &str = "provider_session_runtime";
const RUNTIME_COLUMNS: [&str; 9] = [
    "thread_id",
    "provider_name",
    "provider_instance_id",
    "adapter_key",
    "runtime_mode",
    "status",
    "last_seen_at",
    "resume_cursor_json",
    "runtime_payload_json",
];
const CONTEXT: &str = "mcp_app_model_context";
const CONTEXT_COLUMNS: [&str; 6] = [
    "thread_id",
    "item_id",
    "server",
    "tool",
    "text",
    "updated_at",
];
const ITEMS: &str = "orchestration_v2_projection_turn_items";
const RUNS: &str = "orchestration_v2_projection_runs";
const DOMAINS: [&str; 2] = [RUNTIME, CONTEXT];
const MARKER: &str = "runtime-state-v1";
const PREPARE_ROWS: usize = 32;
const PAGE_ROWS: usize = 128;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct RuntimeRecord {
    pub thread_id: String,
    pub provider_name: String,
    pub provider_instance_id: Option<String>,
    pub adapter_key: String,
    pub runtime_mode: String,
    pub status: String,
    pub last_seen_at: String,
    pub resume_cursor: Option<String>,
    pub runtime_payload: Option<String>,
}

impl RuntimeRecord {
    fn validate(&self) -> Result<()> {
        if !matches!(
            self.status.as_str(),
            "starting" | "running" | "stopped" | "error"
        ) {
            return Err(invalid("invalid provider runtime status"));
        }
        if !matches!(
            self.runtime_mode.as_str(),
            "approval-required" | "auto-accept-edits" | "auto" | "full-access"
        ) {
            return Err(invalid("invalid provider runtime mode"));
        }
        for value in [
            self.resume_cursor.as_deref(),
            self.runtime_payload.as_deref(),
        ]
        .into_iter()
        .flatten()
        {
            serde_json::from_str::<Value>(value)
                .map_err(|_| invalid("invalid provider runtime JSON"))?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ContextRecord {
    pub thread_id: String,
    pub item_id: String,
    pub server: String,
    pub tool: String,
    pub text: String,
    pub updated_at: String,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum RuntimeStateOperation {
    RuntimeUpsert {
        runtime: RuntimeRecord,
        ignore: bool,
    },
    RuntimeImport {
        thread_id: String,
        source: String,
    },
    RuntimeGet {
        thread_id: String,
    },
    RuntimeList {
        exclude_stopped: bool,
    },
    RuntimeDelete {
        thread_id: String,
    },
    ContextUpsert {
        context: ContextRecord,
    },
    ContextDelete {
        thread_id: String,
        item_id: String,
    },
    ContextForThread {
        thread_id: String,
    },
}

impl RuntimeStateOperation {
    pub fn is_write(&self) -> bool {
        !matches!(
            self,
            Self::RuntimeGet { .. } | Self::RuntimeList { .. } | Self::ContextForThread { .. }
        )
    }
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

#[derive(Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
struct IndexEntry {
    table: String,
    identity: Vec<Value>,
    order: Value,
    status: Option<Value>,
    provider: Option<[Value; 2]>,
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
fn key(catalog: &ApplicationCatalog, suffix: &str) -> Vec<u8> {
    store_key(&format!(
        "application/generation/{}/runtime-state/v1/{suffix}",
        catalog.generation
    ))
}
fn hex(value: &str) -> String {
    value.bytes().map(|byte| format!("{byte:02x}")).collect()
}
fn prefix_end(prefix: &[u8]) -> Result<Vec<u8>> {
    let mut end = prefix.to_vec();
    *end.last_mut().ok_or(Error::IncompatibleStore)? += 1;
    Ok(end)
}
fn schema(catalog: &ApplicationCatalog) -> Result<()> {
    validate_table(catalog, RUNTIME, &RUNTIME_COLUMNS, &["thread_id"])?;
    validate_table(
        catalog,
        CONTEXT,
        &CONTEXT_COLUMNS,
        &["thread_id", "item_id"],
    )
}

fn context_scope(catalog: &ApplicationCatalog, thread: &str) -> Vec<u8> {
    key(
        catalog,
        &format!("context/{}/", request_digest(&json!([thread]))),
    )
}

// Invalid historical runtime values are returned to the shared decoder, which logs
// and skips them. Valid rows retain SQLite's BINARY string order. Invalid values
// need not participate in that order because they never become public runtimes.
fn order_cell(cell: &Value) -> Result<String> {
    if cell.get("type").and_then(Value::as_str) == Some("text") {
        Ok(format!(
            "1/{}",
            hex(cell
                .get("value")
                .and_then(Value::as_str)
                .ok_or(Error::IncompatibleStore)?)
        ))
    } else {
        Ok(format!("0/{}", request_digest(cell)))
    }
}

impl IndexEntry {
    fn from_row(name: &str, row: &Map<String, Value>) -> Result<Self> {
        let thread = text(row, "thread_id")?;
        match name {
            RUNTIME => Ok(Self {
                table: name.into(),
                identity: vec![text_cell(&thread)],
                order: row
                    .get("last_seen_at")
                    .cloned()
                    .ok_or(Error::IncompatibleStore)?,
                status: Some(row.get("status").cloned().ok_or(Error::IncompatibleStore)?),
                provider: Some([
                    row.get("provider_name")
                        .cloned()
                        .ok_or(Error::IncompatibleStore)?,
                    row.get("provider_instance_id")
                        .cloned()
                        .ok_or(Error::IncompatibleStore)?,
                ]),
            }),
            CONTEXT => Ok(Self {
                table: name.into(),
                identity: vec![text_cell(&thread), text_cell(&text(row, "item_id")?)],
                order: text_cell(&text(row, "updated_at")?),
                status: None,
                provider: None,
            }),
            _ => Err(Error::IncompatibleStore),
        }
    }

    fn keys(&self, catalog: &ApplicationCatalog) -> Result<Vec<Vec<u8>>> {
        let thread = self
            .identity
            .first()
            .and_then(|v| v.get("value"))
            .and_then(Value::as_str)
            .ok_or(Error::IncompatibleStore)?;
        let order = order_cell(&self.order)?;
        match self.table.as_str() {
            RUNTIME => {
                let suffix = format!("{order}/{}", hex(thread));
                let mut keys = vec![key(catalog, &format!("runtime/all/{suffix}"))];
                if self.status != Some(json!({"type":"null"}))
                    && self.status != Some(text_cell("stopped"))
                {
                    keys.push(key(catalog, &format!("runtime/active/{suffix}")));
                }
                if let Some(provider) = &self.provider {
                    if provider[0].get("type").and_then(Value::as_str) == Some("text")
                        && provider[0].get("encoding").is_none()
                    {
                        let name = provider[0]
                            .get("value")
                            .and_then(Value::as_str)
                            .ok_or(Error::IncompatibleStore)?;
                        keys.push(key(
                            catalog,
                            &format!(
                                "runtime/provider/{}/{}/{}",
                                hex(name),
                                request_digest(&json!(provider)),
                                hex(thread)
                            ),
                        ));
                    }
                }
                Ok(keys)
            }
            CONTEXT => {
                let item = self
                    .identity
                    .get(1)
                    .and_then(|v| v.get("value"))
                    .and_then(Value::as_str)
                    .ok_or(Error::IncompatibleStore)?;
                let mut encoded = context_scope(catalog, thread);
                encoded.extend_from_slice(format!("{order}/{}", hex(item)).as_bytes());
                Ok(vec![encoded])
            }
            _ => Err(Error::IncompatibleStore),
        }
    }
}

/// Compact runtime-provider history for settings migration. This reads no payload JSON.
pub(crate) async fn provider_history(
    session: &mut Session,
    providers: &[&str],
) -> Result<Vec<Value>> {
    ready(session).await?;
    let mut result = Vec::new();
    for provider in providers {
        let prefix = key(
            &session.catalog,
            &format!("runtime/provider/{}/", hex(provider)),
        );
        let end = prefix_end(&prefix)?;
        let mut cursor = prefix;
        loop {
            let page = session.transaction.scan(&cursor, &end, PAGE_ROWS).await?;
            let count = page.len();
            for (encoded, bytes) in page {
                let entry: IndexEntry = decode(&bytes)?;
                let pair = entry.provider.as_ref().ok_or(Error::IncompatibleStore)?;
                if entry.table != RUNTIME
                    || pair[0] != text_cell(provider)
                    || !entry.keys(&session.catalog)?.contains(&encoded)
                {
                    return Err(Error::IncompatibleStore);
                }
                result.push(json!({"providerName":scalar(&pair[0])?,"providerInstanceId":scalar(&pair[1])?}));
                cursor = encoded;
                cursor.push(0);
            }
            if count < PAGE_ROWS {
                break;
            }
        }
    }
    Ok(result)
}

async fn insert_index(
    tx: &mut Transaction,
    catalog: &ApplicationCatalog,
    name: &str,
    row: &Map<String, Value>,
) -> Result<()> {
    let entry = IndexEntry::from_row(name, row)?;
    for key in entry.keys(catalog)? {
        if let Some(bytes) = tx.get(&key).await? {
            if decode::<IndexEntry>(&bytes)? != entry {
                return Err(Error::IncompatibleStore);
            }
        }
        tx.put(key, encode(&entry)?).await?;
    }
    Ok(())
}

async fn remove_index(
    tx: &mut Transaction,
    catalog: &ApplicationCatalog,
    name: &str,
    row: &Map<String, Value>,
) -> Result<()> {
    let entry = IndexEntry::from_row(name, row)?;
    for key in entry.keys(catalog)? {
        if decode::<IndexEntry>(&tx.get(&key).await?.ok_or(Error::IncompatibleStore)?)? != entry {
            return Err(Error::IncompatibleStore);
        }
        tx.delete(key).await?;
    }
    Ok(())
}

async fn ready(session: &mut Session) -> Result<()> {
    schema(&session.catalog)?;
    for domain in DOMAINS {
        if session
            .transaction
            .get(&managed_marker(&session.catalog, domain))
            .await?
            .as_deref()
            .map(decode::<String>)
            .transpose()?
            .as_deref()
            != Some(MARKER)
        {
            return Err(invalid("application runtime state is not prepared"));
        }
    }
    let state: Preparation = session
        .transaction
        .get(&key(&session.catalog, "preparation"))
        .await?
        .as_deref()
        .map(decode)
        .transpose()?
        .ok_or(Error::IncompatibleStore)?;
    if state.version != 1 || state.domain > DOMAINS.len() {
        return Err(Error::IncompatibleStore);
    }
    if state.domain != DOMAINS.len() {
        return Err(invalid("application runtime state is not prepared"));
    }
    Ok(())
}

impl Store {
    pub async fn prepare_application_runtime_state(&self) -> Result<PreparationResult> {
        let mut limit = PREPARE_ROWS;
        loop {
            match self.prepare_application_runtime_state_batch(limit).await {
                Err(Error::ResourceLimit(_)) if limit > 1 => limit = (limit / 2).max(1),
                result => return result,
            }
        }
    }

    async fn prepare_application_runtime_state_batch(
        &self,
        limit: usize,
    ) -> Result<PreparationResult> {
        let mut tx = self
            .db
            .begin_reserved(Duration::from_secs(5), Duration::from_secs(300))
            .await?;
        let catalog = decode_catalog(
            &tx.get(&schema_key())
                .await?
                .ok_or_else(|| invalid("application schema is not initialized"))?,
        )?;
        schema(&catalog)?;
        let state_key = key(&catalog, "preparation");
        let mut state: Preparation = match tx.get(&state_key).await? {
            Some(bytes) => {
                let state: Preparation = decode(&bytes)?;
                if state.version != 1 || state.domain > DOMAINS.len() {
                    return Err(Error::IncompatibleStore);
                }
                for name in DOMAINS {
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
                for name in DOMAINS {
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
        if state.domain == DOMAINS.len() {
            tx.rollback().await?;
            return Ok(PreparationResult {
                prepared: true,
                indexed_rows: state.indexed_rows,
            });
        }
        let name = DOMAINS[state.domain];
        let prefix = row_prefix(&catalog.generation, name)?;
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
        let records = tx.scan(&start, &prefix_end(&prefix)?, limit).await?;
        let count = records.len();
        for (encoded, bytes) in records {
            let row = decode_row(&catalog, table(&catalog, name)?, &encoded, &bytes)?;
            insert_index(&mut tx, &catalog, name, &row.row).await?;
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
            prepared: state.domain == DOMAINS.len(),
            indexed_rows: state.indexed_rows,
        })
    }
}

fn scalar(cell: &Value) -> Result<Value> {
    match cell.get("type").and_then(Value::as_str) {
        Some("null") => Ok(Value::Null),
        Some("text") if cell.get("encoding").is_some() => Ok(json!({"unsupportedSqliteCell":cell})),
        Some("text") => Ok(Value::String(
            cell.get("value")
                .and_then(Value::as_str)
                .ok_or(Error::IncompatibleStore)?
                .into(),
        )),
        Some("integer") => {
            let source = cell
                .get("value")
                .and_then(Value::as_str)
                .ok_or(Error::IncompatibleStore)?;
            // Non-text values intentionally reach the shared schema decoder as
            // non-text, retaining its per-row skip behavior on imported bad rows.
            serde_json::from_str(source).map_err(|_| Error::IncompatibleStore)
        }
        Some("real") => {
            let bits = cell
                .get("value")
                .and_then(Value::as_str)
                .ok_or(Error::IncompatibleStore)?;
            let number = f64::from_bits(
                u64::from_str_radix(bits, 16).map_err(|_| Error::IncompatibleStore)?,
            );
            Ok(serde_json::Number::from_f64(number)
                .map(Value::Number)
                .unwrap_or_else(|| json!({"unsupportedSqliteCell":cell})))
        }
        Some("blob") => Ok(json!({"unsupportedSqliteCell":cell})),
        _ => Err(Error::IncompatibleStore),
    }
}

fn runtime_wire(row: &Map<String, Value>) -> Result<Value> {
    let names = [
        ("thread_id", "threadId"),
        ("provider_name", "providerName"),
        ("provider_instance_id", "providerInstanceId"),
        ("adapter_key", "adapterKey"),
        ("runtime_mode", "runtimeMode"),
        ("status", "status"),
        ("last_seen_at", "lastSeenAt"),
        ("resume_cursor_json", "resumeCursor"),
        ("runtime_payload_json", "runtimePayload"),
    ];
    Ok(Value::Object(
        names
            .into_iter()
            .map(|(stored, wire)| {
                Ok((
                    wire.into(),
                    scalar(row.get(stored).ok_or(Error::IncompatibleStore)?)?,
                ))
            })
            .collect::<Result<_>>()?,
    ))
}

fn upsert_payload(incoming: Option<&str>, previous: Option<&str>) -> Result<Option<String>> {
    let mut new_fields = incoming.map(object_fields).transpose()?;
    let stripped = match new_fields.as_mut() {
        Some(Some(fields)) => {
            if let Some(index) = fields
                .iter()
                .position(|field| field.name == "importedTranscripts")
            {
                fields.remove(index);
            }
            Some(render_fields(fields))
        }
        _ => incoming.map(str::to_owned),
    };
    let old_fields = previous.and_then(|value| object_fields(value).ok().flatten());
    let Some(imports) = old_fields
        .as_ref()
        .and_then(|fields| field(fields, "importedTranscripts"))
    else {
        return Ok(stripped);
    };
    let mut fields = new_fields.flatten().unwrap_or_default();
    let imports = match imports {
        "true" => "1".into(),
        "false" => "0".into(),
        other => other.into(),
    };
    set_field(&mut fields, "importedTranscripts", imports);
    Ok(Some(render_fields(&fields)))
}

fn array_elements(source: &str) -> Result<Vec<String>> {
    let value: Value = serde_json::from_str(source).map_err(|_| Error::IncompatibleStore)?;
    if !value.is_array() {
        return Ok(Vec::new());
    }
    let source = compact_json(source);
    let mut elements = Vec::new();
    let mut start = 1;
    let mut depth = 0usize;
    let mut quoted = false;
    let mut escaped = false;
    for (position, byte) in source.bytes().enumerate().skip(1).take(source.len() - 2) {
        if quoted {
            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
            } else if byte == b'"' {
                quoted = false;
            }
        } else {
            match byte {
                b'"' => quoted = true,
                b'[' | b'{' => depth += 1,
                b']' | b'}' => depth -= 1,
                b',' if depth == 0 => {
                    elements.push(source[start..position].into());
                    start = position + 1;
                }
                _ => (),
            }
        }
    }
    if start < source.len() - 1 {
        elements.push(source[start..source.len() - 1].into());
    }
    Ok(elements)
}

fn imported_payload(previous: Option<&str>, source: &str) -> Result<String> {
    let new_source = object_fields(source)?
        .ok_or_else(|| invalid("imported transcript source must be an object"))?;
    let identity = |fields: &[_], name| -> Result<Value> {
        field(fields, name)
            .map(|value| serde_json::from_str(value).map_err(|_| Error::IncompatibleStore))
            .transpose()
            .map(|value| value.unwrap_or(Value::Null))
    };
    let provider = identity(&new_source, "providerInstanceId")?;
    let path = identity(&new_source, "filePath")?;
    let mut fields = previous
        .and_then(|value| object_fields(value).ok().flatten())
        .unwrap_or_default();
    let existing = field(&fields, "importedTranscripts")
        .map(array_elements)
        .transpose()?
        .unwrap_or_default();
    let mut kept = Vec::new();
    for entry in existing {
        if let Some(object) = object_fields(&entry)? {
            if identity(&object, "providerInstanceId")? != provider
                || identity(&object, "filePath")? != path
            {
                kept.push(entry);
            }
        }
    }
    kept.push(compact_json(source));
    set_field(
        &mut fields,
        "importedTranscripts",
        format!("[{}]", kept.join(",")),
    );
    Ok(render_fields(&fields))
}

fn runtime_stored(runtime: &RuntimeRecord, payload: Option<&str>) -> Map<String, Value> {
    Map::from_iter([
        ("thread_id".into(), text_cell(&runtime.thread_id)),
        ("provider_name".into(), text_cell(&runtime.provider_name)),
        (
            "provider_instance_id".into(),
            nullable_cell(runtime.provider_instance_id.as_deref()),
        ),
        ("adapter_key".into(), text_cell(&runtime.adapter_key)),
        ("runtime_mode".into(), text_cell(&runtime.runtime_mode)),
        ("status".into(), text_cell(&runtime.status)),
        ("last_seen_at".into(), text_cell(&runtime.last_seen_at)),
        (
            "resume_cursor_json".into(),
            nullable_cell(runtime.resume_cursor.as_deref()),
        ),
        ("runtime_payload_json".into(), nullable_cell(payload)),
    ])
}

async fn set_row(
    session: &mut Session,
    name: &str,
    identity: &[Value],
    row: Map<String, Value>,
) -> Result<()> {
    if let Some(old) = read_row(&mut session.transaction, &session.catalog, name, identity).await? {
        remove_index(&mut session.transaction, &session.catalog, name, &old.row).await?;
    }
    write_row(
        &mut session.transaction,
        &session.catalog,
        name,
        row.clone(),
    )
    .await?;
    insert_index(&mut session.transaction, &session.catalog, name, &row).await
}

async fn remove_row(session: &mut Session, name: &str, identity: &[Value]) -> Result<()> {
    if let Some(row) = read_row(&mut session.transaction, &session.catalog, name, identity).await? {
        remove_index(&mut session.transaction, &session.catalog, name, &row.row).await?;
        delete_row(&mut session.transaction, &session.catalog, name, identity).await?;
    }
    Ok(())
}

async fn indexed_rows(
    session: &mut Session,
    prefix: Vec<u8>,
    context_thread: Option<&str>,
) -> Result<Vec<Value>> {
    let mut start = prefix.clone();
    let end = prefix_end(&prefix)?;
    let mut output = Vec::new();
    loop {
        let records = session.transaction.scan(&start, &end, PAGE_ROWS).await?;
        let count = records.len();
        for (encoded, bytes) in records {
            let index: IndexEntry = decode(&bytes)?;
            if !index.keys(&session.catalog)?.contains(&encoded) {
                return Err(Error::IncompatibleStore);
            }
            let row = read_row(
                &mut session.transaction,
                &session.catalog,
                &index.table,
                &index.identity,
            )
            .await?
            .ok_or(Error::IncompatibleStore)?;
            if IndexEntry::from_row(&index.table, &row.row)? != index {
                return Err(Error::IncompatibleStore);
            }
            if let Some(thread) = context_thread {
                if index.table != CONTEXT || text(&row.row, "thread_id")? != thread {
                    return Err(Error::IncompatibleStore);
                }
                let item_id = text(&row.row, "item_id")?;
                if let Some(item) = read_row(
                    &mut session.transaction,
                    &session.catalog,
                    ITEMS,
                    &[text_cell(&item_id)],
                )
                .await?
                {
                    let run = if let Some(id) = nullable(&item.row, "run_id")? {
                        read_row(
                            &mut session.transaction,
                            &session.catalog,
                            RUNS,
                            &[text_cell(&id)],
                        )
                        .await?
                    } else {
                        None
                    };
                    let hidden = if let Some(run) = run {
                        nullable(&run.row, "status")?.as_deref() == Some("rolled_back")
                            && nullable(&run.row, "thread_id")?
                                .as_deref()
                                .is_none_or(|owner| owner == thread)
                    } else {
                        false
                    };
                    if !hidden {
                        output.push(json!({"itemId":item_id,"server":text(&row.row,"server")?,"tool":text(&row.row,"tool")?,"text":text(&row.row,"text")?}));
                    }
                }
            } else {
                if index.table != RUNTIME {
                    return Err(Error::IncompatibleStore);
                }
                output.push(runtime_wire(&row.row)?);
            }
            start = encoded;
            start.push(0);
        }
        if count < PAGE_ROWS {
            return Ok(output);
        }
    }
}

impl ApplicationTransactions {
    pub async fn runtime_state(
        &mut self,
        id: &str,
        operation: RuntimeStateOperation,
    ) -> Result<Value> {
        let mut session = self.take(id)?;
        let result = async {
            ready(&mut session).await?;
            if operation.is_write() {
                session.transaction.require_write()?;
            }
            match operation {
                RuntimeStateOperation::RuntimeGet { thread_id } => read_row(
                    &mut session.transaction,
                    &session.catalog,
                    RUNTIME,
                    &[text_cell(&thread_id)],
                )
                .await?
                .map(|row| runtime_wire(&row.row))
                .transpose()
                .map(|row| row.unwrap_or(Value::Null)),
                RuntimeStateOperation::RuntimeList { exclude_stopped } => {
                    let prefix = key(
                        &session.catalog,
                        if exclude_stopped {
                            "runtime/active/"
                        } else {
                            "runtime/all/"
                        },
                    );
                    Ok(Value::Array(
                        indexed_rows(&mut session, prefix, None).await?,
                    ))
                }
                RuntimeStateOperation::RuntimeUpsert { runtime, ignore } => {
                    runtime.validate()?;
                    let identity = vec![text_cell(&runtime.thread_id)];
                    let previous = read_row(
                        &mut session.transaction,
                        &session.catalog,
                        RUNTIME,
                        &identity,
                    )
                    .await?;
                    if !ignore || previous.is_none() {
                        let old_payload = previous
                            .as_ref()
                            .and_then(|row| row.row.get("runtime_payload_json"))
                            .and_then(|cell| {
                                if cell.get("type").and_then(Value::as_str) == Some("text")
                                    && cell.get("encoding").is_none()
                                {
                                    cell.get("value").and_then(Value::as_str)
                                } else {
                                    None
                                }
                            });
                        let payload =
                            upsert_payload(runtime.runtime_payload.as_deref(), old_payload)?;
                        set_row(
                            &mut session,
                            RUNTIME,
                            &identity,
                            runtime_stored(&runtime, payload.as_deref()),
                        )
                        .await?;
                    }
                    Ok(json!({"written":true}))
                }
                RuntimeStateOperation::RuntimeImport { thread_id, source } => {
                    if object_fields(&source)?.is_none() {
                        return Err(invalid("imported transcript source must be an object"));
                    }
                    let identity = vec![text_cell(&thread_id)];
                    if let Some(mut previous) = read_row(
                        &mut session.transaction,
                        &session.catalog,
                        RUNTIME,
                        &identity,
                    )
                    .await?
                    {
                        let old_payload =
                            previous.row.get("runtime_payload_json").and_then(|cell| {
                                if cell.get("type").and_then(Value::as_str) == Some("text")
                                    && cell.get("encoding").is_none()
                                {
                                    cell.get("value").and_then(Value::as_str)
                                } else {
                                    None
                                }
                            });
                        let payload = imported_payload(old_payload, &source)?;
                        previous
                            .row
                            .insert("runtime_payload_json".into(), text_cell(&payload));
                        set_row(&mut session, RUNTIME, &identity, previous.row).await?;
                    }
                    Ok(json!({"written":true}))
                }
                RuntimeStateOperation::RuntimeDelete { thread_id } => {
                    remove_row(&mut session, RUNTIME, &[text_cell(&thread_id)]).await?;
                    Ok(json!({"written":true}))
                }
                RuntimeStateOperation::ContextUpsert { context } => {
                    let identity = vec![text_cell(&context.thread_id), text_cell(&context.item_id)];
                    let previous = read_row(
                        &mut session.transaction,
                        &session.catalog,
                        CONTEXT,
                        &identity,
                    )
                    .await?;
                    let row = Map::from_iter([
                        ("thread_id".into(), text_cell(&context.thread_id)),
                        ("item_id".into(), text_cell(&context.item_id)),
                        (
                            "server".into(),
                            previous
                                .as_ref()
                                .map(|row| text(&row.row, "server"))
                                .transpose()?
                                .map_or_else(
                                    || text_cell(&context.server),
                                    |value| text_cell(&value),
                                ),
                        ),
                        (
                            "tool".into(),
                            previous
                                .as_ref()
                                .map(|row| text(&row.row, "tool"))
                                .transpose()?
                                .map_or_else(
                                    || text_cell(&context.tool),
                                    |value| text_cell(&value),
                                ),
                        ),
                        ("text".into(), text_cell(&context.text)),
                        ("updated_at".into(), text_cell(&context.updated_at)),
                    ]);
                    set_row(&mut session, CONTEXT, &identity, row).await?;
                    Ok(json!({"written":true}))
                }
                RuntimeStateOperation::ContextDelete { thread_id, item_id } => {
                    remove_row(
                        &mut session,
                        CONTEXT,
                        &[text_cell(&thread_id), text_cell(&item_id)],
                    )
                    .await?;
                    Ok(json!({"written":true}))
                }
                RuntimeStateOperation::ContextForThread { thread_id } => {
                    let prefix = context_scope(&session.catalog, &thread_id);
                    Ok(Value::Array(
                        indexed_rows(&mut session, prefix, Some(&thread_id)).await?,
                    ))
                }
            }
        }
        .await;
        self.finish(id, session, result).await
    }
}

#[cfg(test)]
#[path = "redb_runtime_state_tests.rs"]
mod tests;
