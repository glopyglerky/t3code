#![forbid(unsafe_code)]

pub mod project_service;
pub mod project_validation;
pub mod sqlite_import;
#[cfg(test)]
mod test_support;

use std::collections::HashSet;
use std::fmt;

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use surrealdb::Surreal;
use surrealdb::engine::any::{Any, connect};
use surrealdb::method::Transaction;
use surrealdb::types::QueryError;

pub const MAX_BATCH_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_BATCH_RECORDS: usize = 65_536;
pub const MAX_ID_BYTES: usize = 1_024;
const FORMAT_VERSION: u64 = 1;
const MAX_COUNTER: u64 = 9_007_199_254_740_991;
const NAMESPACE: &str = "t3_code_ls";
const DATABASE: &str = "state";

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Record {
    pub id: String,
    pub payload: Value,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
pub enum ProjectionMutation {
    Put { key: String, payload: Value },
    Delete { key: String },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommitBatch {
    pub command_id: String,
    pub request_digest: String,
    pub expected_revision: u64,
    pub events: Vec<Record>,
    pub projections: Vec<ProjectionMutation>,
    pub effects: Vec<Record>,
    pub result: Value,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Receipt {
    pub command_id: String,
    pub request_digest: String,
    pub revision: u64,
    pub first_sequence: Option<u64>,
    pub last_sequence: u64,
    pub result: Value,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommitOutcome {
    pub receipt: Receipt,
    pub replayed: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Projection {
    pub key: String,
    pub payload: Value,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Snapshot {
    pub revision: u64,
    pub last_sequence: u64,
    pub projections: Vec<Projection>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoredEvent {
    pub id: String,
    pub sequence: u64,
    pub command_id: String,
    pub payload: Value,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PendingEffect {
    pub id: String,
    pub command_id: String,
    pub payload: Value,
}

#[derive(Debug)]
pub enum Error {
    Invalid(String),
    IncompatibleStore,
    StaleRevision { expected: u64, actual: u64 },
    CommandConflict,
    DuplicateRecord,
    TransactionConflict,
    UnknownCommit,
    Database(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Invalid(message) => write!(f, "Invalid database request: {message}"),
            Self::IncompatibleStore => {
                write!(f, "The database is not a supported t3 code ls store")
            }
            Self::StaleRevision { expected, actual } => write!(
                f,
                "Database revision changed: expected {expected}, found {actual}"
            ),
            Self::CommandConflict => write!(f, "The command ID belongs to another request"),
            Self::DuplicateRecord => write!(f, "An immutable event or effect ID already exists"),
            Self::TransactionConflict => write!(f, "The transaction conflicted and did not commit"),
            Self::UnknownCommit => write!(
                f,
                "Commit outcome is unknown; read the original command receipt before retrying"
            ),
            Self::Database(message) => write!(f, "Database operation failed: {message}"),
        }
    }
}

impl std::error::Error for Error {}

type Result<T> = std::result::Result<T, Error>;

fn database_error(error: surrealdb::Error) -> Error {
    if matches!(error.query_details(), Some(QueryError::TransactionConflict)) {
        Error::TransactionConflict
    } else if error.details().is_already_exists() {
        Error::DuplicateRecord
    } else {
        // Database messages can echo query bindings, which may contain prompts or secrets.
        Error::Database(error.kind_str().to_string())
    }
}

fn commit_error(error: surrealdb::Error) -> Error {
    if matches!(error.query_details(), Some(QueryError::TransactionConflict)) {
        Error::TransactionConflict
    } else {
        Error::UnknownCommit
    }
}

fn json_error(_: serde_json::Error) -> Error {
    Error::Database("invalid stored JSON".to_string())
}

fn take_json(response: &mut surrealdb::IndexedResults, index: usize) -> Result<Value> {
    Ok(response
        .take::<surrealdb::types::Value>(index)
        .map_err(database_error)?
        .into_json_value())
}

fn id_is_valid(value: &str) -> bool {
    !value.is_empty() && value.len() <= MAX_ID_BYTES && !value.chars().any(char::is_control)
}

fn validate_id(value: &str) -> Result<()> {
    if id_is_valid(value) {
        Ok(())
    } else {
        Err(Error::Invalid("invalid record identifier".to_string()))
    }
}

fn validate_batch(batch: &CommitBatch) -> Result<()> {
    validate_id(&batch.command_id)?;
    if batch.request_digest.len() != 64
        || !batch
            .request_digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(Error::Invalid("request_digest must be SHA-256 hex".into()));
    }
    if batch.expected_revision >= MAX_COUNTER {
        return Err(Error::Invalid(
            "revision exceeds supported counter range".into(),
        ));
    }
    if batch.events.len() > MAX_BATCH_RECORDS
        || batch.projections.len() > MAX_BATCH_RECORDS
        || batch.effects.len() > MAX_BATCH_RECORDS
    {
        return Err(Error::Invalid("too many records in one batch".into()));
    }
    for records in [&batch.events, &batch.effects] {
        let mut seen = HashSet::new();
        for record in records {
            validate_id(&record.id)?;
            if !seen.insert(&record.id) {
                return Err(Error::Invalid(
                    "duplicate immutable record identifier".into(),
                ));
            }
        }
    }
    let mut seen = HashSet::new();
    for mutation in &batch.projections {
        let key = match mutation {
            ProjectionMutation::Put { key, .. } | ProjectionMutation::Delete { key } => key,
        };
        validate_id(key)?;
        if !seen.insert(key) {
            return Err(Error::Invalid("duplicate projection identifier".into()));
        }
    }
    if serde_json::to_vec(batch).map_err(json_error)?.len() > MAX_BATCH_BYTES {
        return Err(Error::Invalid("batch exceeds 16 MiB".into()));
    }
    Ok(())
}

/// Hash the original validated command, excluding attempt-specific revision and event IDs.
pub fn request_digest(request: &Value) -> String {
    let mut hash = Sha256::new();
    hash_json(request, &mut hash);
    hash.finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn hash_json(value: &Value, hash: &mut Sha256) {
    match value {
        Value::Object(object) => {
            hash.update(b"{");
            let mut keys: Vec<_> = object.keys().collect();
            keys.sort();
            for (index, key) in keys.into_iter().enumerate() {
                if index > 0 {
                    hash.update(b",");
                }
                hash.update(serde_json::to_vec(key).expect("JSON key always serializes"));
                hash.update(b":");
                hash_json(&object[key], hash);
            }
            hash.update(b"}");
        }
        Value::Array(values) => {
            hash.update(b"[");
            for (index, value) in values.iter().enumerate() {
                if index > 0 {
                    hash.update(b",");
                }
                hash_json(value, hash);
            }
            hash.update(b"]");
        }
        _ => hash.update(serde_json::to_vec(value).expect("JSON Value always serializes")),
    }
}

pub struct Store {
    db: Surreal<Any>,
}

impl Clone for Store {
    fn clone(&self) -> Self {
        Self {
            db: self.db.clone(),
        }
    }
}

impl Store {
    pub async fn shutdown(self) -> Result<()> {
        self.db.shutdown().await.map_err(database_error)
    }

    pub async fn initialize_loopback(endpoint: &str, token: &str) -> Result<Self> {
        validate_loopback(endpoint, token)?;
        let db = connect(endpoint).await.map_err(database_error)?;
        db.authenticate(token.to_owned())
            .await
            .map_err(database_error)?;
        db.query(
            "DEFINE NAMESPACE IF NOT EXISTS t3_code_ls; \
             USE NS t3_code_ls; DEFINE DATABASE IF NOT EXISTS state;",
        )
        .await
        .map_err(database_error)?
        .check()
        .map_err(database_error)?;
        db.use_ns(NAMESPACE)
            .use_db(DATABASE)
            .await
            .map_err(database_error)?;
        let store = Self { db };
        store.initialize().await?;
        Ok(store)
    }

    /// Connect to an already initialized, authenticated deployment of this store.
    pub async fn connect_remote(endpoint: &str, token: &str) -> Result<Self> {
        let url = url::Url::parse(endpoint)
            .map_err(|_| Error::Invalid("invalid database endpoint".into()))?;
        if url.scheme() != "wss"
            || url.host_str().is_none()
            || !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
            || token.is_empty()
        {
            return Err(Error::Invalid(
                "remote database requires WSS and a token".into(),
            ));
        }
        Self::connect_authenticated(endpoint, token).await
    }

    pub async fn connect_loopback(endpoint: &str, token: &str) -> Result<Self> {
        validate_loopback(endpoint, token)?;
        Self::connect_authenticated(endpoint, token).await
    }

    async fn connect_authenticated(endpoint: &str, token: &str) -> Result<Self> {
        let db = connect(endpoint).await.map_err(database_error)?;
        db.authenticate(token.to_owned())
            .await
            .map_err(database_error)?;
        db.use_ns(NAMESPACE)
            .use_db(DATABASE)
            .await
            .map_err(database_error)?;
        let store = Self { db };
        store.metadata().await?;
        Ok(store)
    }

    async fn initialize(&self) -> Result<()> {
        let mut info = self
            .db
            .query("INFO FOR DB")
            .await
            .map_err(database_error)?
            .check()
            .map_err(database_error)?;
        let info = take_json(&mut info, 0)?;
        let tables = info
            .get("tables")
            .and_then(Value::as_object)
            .ok_or(Error::IncompatibleStore)?;
        if !tables.is_empty() && !tables.contains_key("ls_meta") {
            return Err(Error::IncompatibleStore);
        }
        if tables.is_empty() {
            let response = self
                .db
                .query(INITIALIZE_QUERY)
                .await
                .map_err(database_error)?;
            match response.check().map_err(database_error) {
                Ok(_) | Err(Error::DuplicateRecord) => {}
                Err(error) => return Err(error),
            }
        }
        self.metadata().await?;
        Ok(())
    }

    async fn metadata(&self) -> Result<Metadata> {
        let mut response = self
            .db
            .query(METADATA_QUERY)
            .await
            .map_err(database_error)?
            .check()
            .map_err(database_error)?;
        decode_metadata(take_json(&mut response, 0)?)
    }

    pub async fn read_receipt(&self, command_id: &str) -> Result<Option<Receipt>> {
        validate_id(command_id)?;
        let mut response = self
            .db
            .query(RECEIPT_QUERY)
            .bind(("command_id", command_id.to_owned()))
            .await
            .map_err(database_error)?
            .check()
            .map_err(database_error)?;
        let receipt = decode_receipt(take_json(&mut response, 0)?)?;
        if receipt
            .as_ref()
            .is_some_and(|receipt| receipt.command_id != command_id)
        {
            return Err(Error::IncompatibleStore);
        }
        Ok(receipt)
    }

    pub async fn snapshot(&self) -> Result<Snapshot> {
        self.snapshot_matching(false).await
    }

    pub async fn project_snapshot(&self) -> Result<Snapshot> {
        self.snapshot_matching(true).await
    }

    async fn snapshot_matching(&self, projects_only: bool) -> Result<Snapshot> {
        let tx = self
            .db
            .clone()
            .begin()
            .read_only()
            .await
            .map_err(database_error)?;
        let result = self.snapshot_in(&tx, projects_only).await;
        tx.cancel().await.map_err(database_error)?;
        result
    }

    async fn snapshot_in(&self, tx: &Transaction<Any>, projects_only: bool) -> Result<Snapshot> {
        let predicate = if projects_only {
            "WHERE record_key >= 'project/' AND record_key < 'project0' "
        } else {
            ""
        };
        let mut response = tx.query(
            format!("SELECT format_version, store_kind, revision, sequence FROM ONLY ls_meta:state; \
             SELECT record_key, payload_bytes FROM ls_projection {predicate}ORDER BY record_key LIMIT $row_limit;"),
        ).bind(("row_limit", (MAX_BATCH_RECORDS + 1) as u64))
            .await.map_err(database_error)?.check().map_err(database_error)?;
        let metadata = decode_metadata(take_json(&mut response, 0)?)?;
        let sizes: Vec<Value> = response.take(1).map_err(database_error)?;
        if sizes.len() > MAX_BATCH_RECORDS {
            return Err(Error::Invalid("snapshot exceeds record limit".into()));
        }
        let mut snapshot_bytes = 0_u64;
        for row in &sizes {
            snapshot_bytes = snapshot_bytes
                .saturating_add(stored_counter(row, "payload_bytes")?)
                .saturating_add(stored_string(row, "record_key")?.len() as u64);
            if snapshot_bytes > MAX_BATCH_BYTES as u64 {
                return Err(Error::Invalid("snapshot exceeds 16 MiB".into()));
            }
        }
        let mut response = tx.query(
            format!("SELECT record_key, payload_json FROM ls_projection {predicate}ORDER BY record_key LIMIT $row_limit;"),
        ).bind(("row_limit", (MAX_BATCH_RECORDS + 1) as u64))
            .await.map_err(database_error)?.check().map_err(database_error)?;
        let rows: Vec<Value> = response.take(0).map_err(database_error)?;
        if rows.len() != sizes.len() {
            return Err(Error::IncompatibleStore);
        }
        let mut projections = Vec::with_capacity(rows.len());
        for (row, size) in rows.into_iter().zip(sizes.iter()) {
            if stored_string(&row, "record_key")? != stored_string(size, "record_key")?
                || stored_string(&row, "payload_json")?.len() as u64
                    != stored_counter(size, "payload_bytes")?
            {
                return Err(Error::IncompatibleStore);
            }
            projections.push(Projection {
                key: stored_string(&row, "record_key")?.to_owned(),
                payload: serde_json::from_str(stored_string(&row, "payload_json")?)
                    .map_err(json_error)?,
            });
        }
        let snapshot = Snapshot {
            revision: metadata.revision,
            last_sequence: metadata.sequence,
            projections,
        };
        if serde_json::to_vec(&snapshot).map_err(json_error)?.len() > MAX_BATCH_BYTES {
            return Err(Error::Invalid("snapshot exceeds 16 MiB".into()));
        }
        Ok(snapshot)
    }

    pub async fn events(&self, after_sequence: u64, limit: u32) -> Result<Vec<StoredEvent>> {
        if after_sequence > MAX_COUNTER || limit == 0 || limit > 4096 {
            return Err(Error::Invalid("invalid event page".into()));
        }
        let mut response = self
            .db
            .query(
                "SELECT record_key, sequence, payload_bytes FROM ls_event \
             WHERE sequence > $after ORDER BY sequence LIMIT $limit;",
            )
            .bind(("after", after_sequence))
            .bind(("limit", limit))
            .await
            .map_err(database_error)?
            .check()
            .map_err(database_error)?;
        let sizes: Vec<Value> = response.take(0).map_err(database_error)?;
        validate_page_bytes(&sizes)?;
        let Some(last) = sizes.last() else {
            return Ok(Vec::new());
        };
        let mut response = self
            .db
            .query(
                "SELECT record_key, sequence, command_id, payload_json FROM ls_event \
             WHERE sequence > $after AND sequence <= $through ORDER BY sequence;",
            )
            .bind(("after", after_sequence))
            .bind(("through", stored_counter(last, "sequence")?))
            .await
            .map_err(database_error)?
            .check()
            .map_err(database_error)?;
        let rows: Vec<Value> = response.take(0).map_err(database_error)?;
        validate_page_rows(&rows, &sizes)?;
        let events: Vec<StoredEvent> = rows
            .into_iter()
            .map(|row| {
                Ok(StoredEvent {
                    id: stored_string(&row, "record_key")?.to_owned(),
                    sequence: stored_counter(&row, "sequence")?,
                    command_id: stored_string(&row, "command_id")?.to_owned(),
                    payload: serde_json::from_str(stored_string(&row, "payload_json")?)
                        .map_err(json_error)?,
                })
            })
            .collect::<Result<_>>()?;
        validate_response_bytes(&events)?;
        Ok(events)
    }

    pub async fn pending_effects(&self, limit: u32) -> Result<Vec<PendingEffect>> {
        if limit == 0 || limit > 4096 {
            return Err(Error::Invalid("invalid effect page".into()));
        }
        let mut response = self
            .db
            .query(
                "SELECT record_key, payload_bytes FROM ls_outbox \
             WHERE state = 'pending' ORDER BY record_key LIMIT $limit;",
            )
            .bind(("limit", limit))
            .await
            .map_err(database_error)?
            .check()
            .map_err(database_error)?;
        let sizes: Vec<Value> = response.take(0).map_err(database_error)?;
        validate_page_bytes(&sizes)?;
        if sizes.is_empty() {
            return Ok(Vec::new());
        }
        let keys: Vec<String> = sizes
            .iter()
            .map(|row| stored_string(row, "record_key").map(str::to_owned))
            .collect::<Result<_>>()?;
        let mut response = self
            .db
            .query(
                "SELECT record_key, command_id, payload_json FROM ls_outbox \
             WHERE record_key IN $keys ORDER BY record_key;",
            )
            .bind(("keys", keys))
            .await
            .map_err(database_error)?
            .check()
            .map_err(database_error)?;
        let rows: Vec<Value> = response.take(0).map_err(database_error)?;
        validate_page_rows(&rows, &sizes)?;
        let effects: Vec<PendingEffect> = rows
            .into_iter()
            .map(|row| {
                Ok(PendingEffect {
                    id: stored_string(&row, "record_key")?.to_owned(),
                    command_id: stored_string(&row, "command_id")?.to_owned(),
                    payload: serde_json::from_str(stored_string(&row, "payload_json")?)
                        .map_err(json_error)?,
                })
            })
            .collect::<Result<_>>()?;
        validate_response_bytes(&effects)?;
        Ok(effects)
    }

    pub async fn commit(&self, batch: &CommitBatch) -> Result<CommitOutcome> {
        validate_batch(batch)?;
        let tx = self.db.clone().begin().await.map_err(database_error)?;
        let staged = self.stage(&tx, batch).await;
        match staged {
            Ok(outcome) if outcome.replayed => {
                tx.cancel().await.map_err(database_error)?;
                Ok(outcome)
            }
            Ok(outcome) => {
                tx.commit().await.map_err(commit_error)?;
                Ok(outcome)
            }
            Err(error) => {
                // No COMMIT was sent, so cancellation failure cannot make this batch durable.
                let _ = tx.cancel().await;
                Err(error)
            }
        }
    }

    async fn stage(&self, tx: &Transaction<Any>, batch: &CommitBatch) -> Result<CommitOutcome> {
        let mut response = tx
            .query(format!("{RECEIPT_QUERY}; {METADATA_QUERY}"))
            .bind(("command_id", batch.command_id.clone()))
            .await
            .map_err(database_error)?
            .check()
            .map_err(database_error)?;
        if let Some(receipt) = decode_receipt(take_json(&mut response, 0)?)? {
            if receipt.command_id != batch.command_id {
                return Err(Error::IncompatibleStore);
            }
            if receipt.request_digest != batch.request_digest {
                return Err(Error::CommandConflict);
            }
            return Ok(CommitOutcome {
                receipt,
                replayed: true,
            });
        }
        let meta = decode_metadata(take_json(&mut response, 1)?)?;
        if meta.revision != batch.expected_revision {
            return Err(Error::StaleRevision {
                expected: batch.expected_revision,
                actual: meta.revision,
            });
        }
        let last_sequence = meta
            .sequence
            .checked_add(batch.events.len() as u64)
            .filter(|sequence| *sequence <= MAX_COUNTER)
            .ok_or_else(|| Error::Invalid("event sequence exhausted".into()))?;
        let receipt = Receipt {
            command_id: batch.command_id.clone(),
            request_digest: batch.request_digest.clone(),
            revision: meta.revision + 1,
            first_sequence: if batch.events.is_empty() {
                None
            } else {
                Some(meta.sequence + 1)
            },
            last_sequence,
            result: batch.result.clone(),
        };
        let events: Vec<Value> = batch
            .events
            .iter()
            .enumerate()
            .map(|(index, event)| {
                json!({
                    "record_key": event.id,
                    "sequence": meta.sequence + index as u64 + 1,
                    "command_id": batch.command_id,
                    "payload_json": event.payload.to_string(),
                    "payload_bytes": event.payload.to_string().len(),
                })
            })
            .collect();
        let projections: Vec<Value> = batch
            .projections
            .iter()
            .map(|mutation| match mutation {
                ProjectionMutation::Put { key, payload } => json!({
                    "record_key": key, "remove": false, "payload_json": payload.to_string(),
                    "payload_bytes": payload.to_string().len(),
                }),
                ProjectionMutation::Delete { key } => json!({"record_key": key, "remove": true}),
            })
            .collect();
        let effects: Vec<Value> = batch
            .effects
            .iter()
            .map(|effect| {
                json!({
                    "record_key": effect.id, "command_id": batch.command_id,
                    "payload_json": effect.payload.to_string(), "state": "pending",
                    "payload_bytes": effect.payload.to_string().len(),
                })
            })
            .collect();
        let stored_receipt = json!({
            "command_id": receipt.command_id,
            "request_digest": receipt.request_digest,
            "revision": receipt.revision,
            "first_sequence": receipt.first_sequence,
            "last_sequence": receipt.last_sequence,
            "result_json": receipt.result.to_string(),
        });
        tx.query(
            "UPDATE ls_meta:state SET revision = $revision, sequence = $sequence; \
             FOR $event IN $events { \
                 CREATE type::record('ls_event', $event.record_key) CONTENT $event; \
             }; \
             FOR $projection IN $projections { \
                 IF $projection.remove { \
                     DELETE type::record('ls_projection', $projection.record_key); \
                 } ELSE { \
                     UPSERT type::record('ls_projection', $projection.record_key) \
                         SET record_key = $projection.record_key, payload_json = $projection.payload_json, \
                         payload_bytes = $projection.payload_bytes; \
                 }; \
             }; \
             FOR $effect IN $effects { \
                 CREATE type::record('ls_outbox', $effect.record_key) CONTENT $effect; \
             }; \
             CREATE type::record('ls_receipt', $command_id) CONTENT $receipt;",
        ).bind(("revision", receipt.revision))
            .bind(("sequence", receipt.last_sequence))
            .bind(("events", events))
            .bind(("projections", projections))
            .bind(("effects", effects))
            .bind(("command_id", batch.command_id.clone()))
            .bind(("receipt", stored_receipt))
            .await.map_err(database_error)?.check().map_err(database_error)?;
        Ok(CommitOutcome {
            receipt,
            replayed: false,
        })
    }
}

fn validate_loopback(endpoint: &str, token: &str) -> Result<()> {
    let url = url::Url::parse(endpoint)
        .map_err(|_| Error::Invalid("invalid database endpoint".into()))?;
    let loopback = match url.host() {
        Some(url::Host::Ipv4(address)) => address.is_loopback(),
        Some(url::Host::Ipv6(address)) => address.is_loopback(),
        _ => false,
    };
    if url.scheme() != "ws"
        || !loopback
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || token.is_empty()
    {
        return Err(Error::Invalid(
            "plaintext database connections require a numeric loopback address and a token".into(),
        ));
    }
    Ok(())
}

const METADATA_QUERY: &str =
    "SELECT format_version, store_kind, revision, sequence FROM ONLY ls_meta:state";
const RECEIPT_QUERY: &str = "SELECT command_id, request_digest, revision, first_sequence, last_sequence, result_json \
     FROM ONLY type::record('ls_receipt', $command_id)";

const INITIALIZE_QUERY: &str = "BEGIN; \
    DEFINE TABLE ls_meta SCHEMALESS; \
    DEFINE TABLE ls_event SCHEMALESS; \
    DEFINE TABLE ls_projection SCHEMALESS; \
    DEFINE TABLE ls_receipt SCHEMALESS; \
    DEFINE TABLE ls_outbox SCHEMALESS; \
    DEFINE TABLE ls_sqlite_archive SCHEMALESS; \
    DEFINE TABLE ls_sqlite_archive_chunk SCHEMALESS; \
    DEFINE INDEX ls_event_sequence ON ls_event FIELDS sequence UNIQUE; \
    DEFINE INDEX ls_projection_key ON ls_projection FIELDS record_key UNIQUE; \
    DEFINE INDEX ls_outbox_pending ON ls_outbox FIELDS state, record_key; \
    CREATE ls_meta:state SET format_version = 1, \
        store_kind = 't3-code-ls-command-store', revision = 0, sequence = 0; \
    COMMIT;";

struct Metadata {
    revision: u64,
    sequence: u64,
}

fn stored_counter(value: &Value, key: &str) -> Result<u64> {
    value
        .get(key)
        .and_then(Value::as_u64)
        .filter(|v| *v <= MAX_COUNTER)
        .ok_or(Error::IncompatibleStore)
}

fn stored_string<'a>(value: &'a Value, key: &str) -> Result<&'a str> {
    value
        .get(key)
        .and_then(Value::as_str)
        .ok_or(Error::IncompatibleStore)
}

fn validate_page_bytes(rows: &[Value]) -> Result<()> {
    let mut bytes = 0_u64;
    for row in rows {
        bytes = bytes
            .saturating_add(stored_counter(row, "payload_bytes")?)
            .saturating_add(stored_string(row, "record_key")?.len() as u64);
        if bytes > MAX_BATCH_BYTES as u64 {
            return Err(Error::Invalid(
                "page exceeds 16 MiB; request fewer records".into(),
            ));
        }
    }
    Ok(())
}

fn validate_page_rows(rows: &[Value], sizes: &[Value]) -> Result<()> {
    if rows.len() != sizes.len() {
        return Err(Error::IncompatibleStore);
    }
    for (row, size) in rows.iter().zip(sizes) {
        if stored_string(row, "record_key")? != stored_string(size, "record_key")?
            || stored_string(row, "payload_json")?.len() as u64
                != stored_counter(size, "payload_bytes")?
        {
            return Err(Error::IncompatibleStore);
        }
    }
    Ok(())
}

fn validate_response_bytes(value: &impl Serialize) -> Result<()> {
    if serde_json::to_vec(value).map_err(json_error)?.len() > MAX_BATCH_BYTES {
        Err(Error::Invalid(
            "response exceeds 16 MiB; request fewer records".into(),
        ))
    } else {
        Ok(())
    }
}

fn decode_metadata(value: Value) -> Result<Metadata> {
    if value.get("format_version").and_then(Value::as_u64) != Some(FORMAT_VERSION)
        || value.get("store_kind").and_then(Value::as_str) != Some("t3-code-ls-command-store")
    {
        return Err(Error::IncompatibleStore);
    }
    Ok(Metadata {
        revision: stored_counter(&value, "revision")?,
        sequence: stored_counter(&value, "sequence")?,
    })
}

fn decode_receipt(value: Value) -> Result<Option<Receipt>> {
    if value.is_null() {
        return Ok(None);
    }
    let first_sequence = if value.get("first_sequence").is_some_and(Value::is_null) {
        None
    } else {
        Some(stored_counter(&value, "first_sequence")?)
    };
    let receipt = Receipt {
        command_id: stored_string(&value, "command_id")?.to_owned(),
        request_digest: stored_string(&value, "request_digest")?.to_owned(),
        revision: stored_counter(&value, "revision")?,
        first_sequence,
        last_sequence: stored_counter(&value, "last_sequence")?,
        result: serde_json::from_str(stored_string(&value, "result_json")?).map_err(json_error)?,
    };
    if !id_is_valid(&receipt.command_id)
        || receipt.request_digest.len() != 64
        || !receipt
            .request_digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        || receipt.revision == 0
        || receipt
            .first_sequence
            .is_some_and(|first| first == 0 || first > receipt.last_sequence)
    {
        return Err(Error::IncompatibleStore);
    }
    Ok(Some(receipt))
}
