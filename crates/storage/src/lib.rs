#![forbid(unsafe_code)]

mod backend;

pub use backend::run_on_database_thread;

#[path = "../../../apps/server/src/persistence/redb_transactions.rs"]
pub mod application;
#[path = "../../../apps/server/src/persistence/redb_auth_rows.rs"]
pub mod application_auth;
#[path = "../../../apps/server/src/persistence/redb_auxiliary_reads.rs"]
pub mod application_auxiliary_reads;
#[path = "../../../apps/server/src/persistence/redb_bootstrap.rs"]
pub mod application_bootstrap;
#[path = "../../../apps/server/src/persistence/redb_event_rows.rs"]
pub mod application_events;
#[path = "../../../apps/server/src/persistence/redb_import.rs"]
pub mod application_import;
#[path = "../../../apps/server/src/persistence/redb_json.rs"]
pub(crate) mod application_json;
#[path = "../../../apps/server/src/orchestration-v2/legacy/redb_legacy_v1.rs"]
pub mod application_legacy_v1;
#[path = "../../../apps/server/src/persistence/redb_message_rowids.rs"]
pub mod application_message_rowids;
#[path = "../../../apps/server/src/persistence/redb_message_search_text.rs"]
pub(crate) mod application_message_search_text;
#[path = "../../../apps/server/src/orchestration-v2/redb_effect_outbox.rs"]
pub mod application_outbox;
#[path = "../../../apps/server/src/peer/redb_peer_rows.rs"]
pub mod application_peers;
#[path = "../../../apps/server/src/orchestration-v2/redb_turn_item_positions.rs"]
pub mod application_positions;
#[path = "../../../apps/server/src/persistence/redb_projection_commit.rs"]
pub mod application_projections;
#[path = "../../../apps/server/src/orchestration-v2/redb_project_rows.rs"]
pub mod application_projects;
#[path = "../../../apps/server/src/pullRequest/redb_pull_request_rows.rs"]
pub mod application_pull_requests;
#[path = "../../../apps/server/src/persistence/redb_command_receipts.rs"]
pub mod application_receipts;
#[path = "../../../apps/server/src/persistence/redb_application_rows.rs"]
pub(crate) mod application_rows;
#[path = "../../../apps/server/src/persistence/redb_runtime_state.rs"]
pub mod application_runtime_state;
#[path = "../../../apps/server/src/scheduledTasks/redb_scheduled_rows.rs"]
pub mod application_scheduled;
#[path = "../../../apps/server/src/persistence/redb_thread_search.rs"]
pub mod application_search;
#[path = "../../../apps/server/src/persistence/redb_server_ownership.rs"]
pub mod application_server_ownership;
#[path = "../../../apps/server/src/persistence/redb_database_storage.rs"]
pub mod application_storage;
#[path = "../../../apps/server/src/persistence/redb_thread_find.rs"]
pub mod application_thread_find;
#[path = "../../../apps/server/src/persistence/redb_turn_item_counts.rs"]
pub(crate) mod application_turn_item_counts;

pub mod outbox;
pub mod project_service;
pub mod project_validation;
pub mod sqlite_import;
#[cfg(test)]
mod test_support;
pub mod thread_service;

use std::collections::HashSet;
use std::fmt;
use std::path::Path;

use backend::{Backend, Transaction};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;
use sha2::{Digest, Sha256};

pub const MAX_BATCH_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_BATCH_RECORDS: usize = 65_536;
pub const MAX_ID_BYTES: usize = 1_024;
const FORMAT_VERSION: u64 = 2;
const MAX_COUNTER: u64 = 9_007_199_254_740_991;
const STORE_KIND: &str = "t3-code-ls-redb-store";

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
pub struct OutboxBatch {
    pub enqueue: Vec<outbox::EnqueueEffect>,
    pub enqueue_at: String,
    pub cancel: Vec<outbox::CancelUnsettled>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OutboxCommitOutcome {
    pub commit: CommitOutcome,
    pub enqueued: usize,
    pub cancelled: Vec<String>,
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ResourceLimit {
    ScanBytes,
    ReadBytes,
    MutationBytes,
    ObservationBudget,
}

impl fmt::Display for ResourceLimit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::ScanBytes => "database scan exceeds 16 MiB",
            Self::ReadBytes => "database read exceeds 16 MiB",
            Self::MutationBytes => "transaction mutations exceed 16 MiB",
            Self::ObservationBudget => "transaction observations exceed read limits",
        })
    }
}

#[derive(Debug)]
pub enum Error {
    Invalid(String),
    ResourceLimit(ResourceLimit),
    IncompatibleStore,
    StaleRevision { expected: u64, actual: u64 },
    CommandConflict,
    DuplicateRecord,
    TransactionConflict,
    WriterBusy,
    UnknownCommit,
    Database(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Invalid(message) => write!(f, "Invalid database request: {message}"),
            Self::ResourceLimit(limit) => write!(f, "Invalid database request: {limit}"),
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
            Self::WriterBusy => write!(f, "The database writer was busy; no transaction body ran"),
            Self::UnknownCommit => write!(
                f,
                "Commit outcome is unknown; resolve the original operation before retrying"
            ),
            Self::Database(message) => write!(f, "Database operation failed: {message}"),
        }
    }
}

impl std::error::Error for Error {}

type Result<T> = std::result::Result<T, Error>;

fn json_error(_: serde_json::Error) -> Error {
    Error::IncompatibleStore
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

#[derive(Clone)]
pub struct Store {
    pub(crate) db: Backend,
}

pub(crate) fn store_key(path: &str) -> Vec<u8> {
    let mut key = b"t3-code-ls/v2/".to_vec();
    key.extend_from_slice(path.as_bytes());
    key
}

fn prefix_range(path: &str) -> (Vec<u8>, Vec<u8>) {
    let start = store_key(path);
    let mut end = start.clone();
    // All internal prefixes end in '/', whose lexical successor is '0'.
    *end.last_mut().expect("internal store prefix is nonempty") += 1;
    (start, end)
}

fn encode(value: &impl Serialize) -> Result<Vec<u8>> {
    serde_json::to_vec(value).map_err(json_error)
}

fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    serde_json::from_slice(bytes).map_err(json_error)
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Metadata {
    format_version: u64,
    store_kind: String,
    revision: u64,
    sequence: u64,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProjectionMetadata {
    key: String,
    payload_bytes: usize,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct EventMetadata {
    id: String,
    sequence: u64,
    command_id: String,
    payload_bytes: usize,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct EffectMetadata {
    id: String,
    command_id: String,
    payload_bytes: usize,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct OutboxReceipt {
    command_id: String,
    request_digest: String,
    outbox_digest: String,
    enqueued: usize,
    cancelled: Vec<String>,
}

fn outbox_receipt_key(command_id: &str) -> Vec<u8> {
    store_key(&format!("outbox/commit/{command_id}"))
}

fn decode_outbox_receipt(bytes: &[u8], receipt: &Receipt) -> Result<OutboxReceipt> {
    let extra: OutboxReceipt = decode(bytes)?;
    if extra.command_id != receipt.command_id
        || extra.request_digest != receipt.request_digest
        || extra.outbox_digest.len() != 64
        || !extra
            .outbox_digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        || extra.enqueued > MAX_BATCH_RECORDS
        || extra.cancelled.len() > MAX_BATCH_RECORDS
        || extra.cancelled.iter().any(|id| !id_is_valid(id))
        || extra.cancelled.iter().collect::<HashSet<_>>().len() != extra.cancelled.len()
    {
        return Err(Error::IncompatibleStore);
    }
    Ok(extra)
}

impl Store {
    /// Explicitly create a new store, or validate an already initialized compatible store.
    pub async fn initialize_local(path: impl AsRef<Path>) -> Result<Self> {
        let store = Self {
            db: Backend::open(path, true).await?,
        };
        let mut tx = store.db.begin().await?;
        let initialized = async {
            if let Some(bytes) = tx.get(&store_key("meta/state")).await? {
                decode_metadata(&bytes)?;
                return Ok(false);
            }
            let (start, end) = prefix_range("");
            if !tx.scan(&start, &end, 1).await?.is_empty() {
                return Err(Error::IncompatibleStore);
            }
            let metadata = Metadata {
                format_version: FORMAT_VERSION,
                store_kind: STORE_KIND.into(),
                revision: 0,
                sequence: 0,
            };
            tx.insert(store_key("meta/state"), encode(&metadata)?)
                .await?;
            Ok(true)
        }
        .await;
        match initialized {
            Ok(true) => match tx.commit().await {
                Ok(()) => {}
                Err(Error::TransactionConflict) => {
                    store.metadata().await?;
                }
                Err(error) => return Err(error),
            },
            Ok(false) => tx.rollback().await?,
            Err(error) => {
                let _ = tx.rollback().await;
                return Err(error);
            }
        }
        Ok(store)
    }

    /// Open an existing compatible store. This never creates a missing database file.
    pub async fn open_local(path: impl AsRef<Path>) -> Result<Self> {
        let store = Self {
            db: Backend::open(path, false).await?,
        };
        store.metadata().await?;
        Ok(store)
    }

    /// Strict read-only inspection; never creates or repairs a database file.
    pub async fn open_read_only(path: impl AsRef<Path>) -> Result<Self> {
        let store = Self {
            db: Backend::open_read_only(path).await?,
        };
        store.metadata().await?;
        Ok(store)
    }

    pub async fn shutdown(self) -> Result<()> {
        self.db.shutdown().await
    }

    async fn metadata(&self) -> Result<Metadata> {
        let bytes = self
            .db
            .read(&store_key("meta/state"))
            .await?
            .ok_or(Error::IncompatibleStore)?;
        decode_metadata(&bytes)
    }

    pub async fn read_receipt(&self, command_id: &str) -> Result<Option<Receipt>> {
        validate_id(command_id)?;
        self.db
            .read(&store_key(&format!("receipt/{command_id}")))
            .await?
            .map(|bytes| decode_receipt(&bytes, command_id))
            .transpose()
    }

    pub async fn snapshot(&self) -> Result<Snapshot> {
        self.snapshot_matching(false).await
    }

    pub async fn project_snapshot(&self) -> Result<Snapshot> {
        self.snapshot_matching(true).await
    }

    async fn snapshot_matching(&self, projects_only: bool) -> Result<Snapshot> {
        let mut tx = self.db.begin().await?;
        let result = self.snapshot_in(&mut tx, projects_only).await;
        tx.rollback().await?;
        result
    }

    async fn snapshot_in(&self, tx: &mut Transaction, projects_only: bool) -> Result<Snapshot> {
        let bytes = tx
            .get(&store_key("meta/state"))
            .await?
            .ok_or(Error::IncompatibleStore)?;
        let metadata = decode_metadata(&bytes)?;
        let prefix = if projects_only {
            "projection/meta/project/"
        } else {
            "projection/meta/"
        };
        let (start, end) = prefix_range(prefix);
        let rows = tx.scan(&start, &end, MAX_BATCH_RECORDS + 1).await?;
        if rows.len() > MAX_BATCH_RECORDS {
            return Err(Error::Invalid("snapshot exceeds record limit".into()));
        }
        let mut total = 0usize;
        let mut entries = Vec::with_capacity(rows.len());
        for (key, bytes) in rows {
            let entry: ProjectionMetadata = decode(&bytes)?;
            if !id_is_valid(&entry.key)
                || key != store_key(&format!("projection/meta/{}", entry.key))
                || (projects_only && !entry.key.starts_with("project/"))
                || entry.payload_bytes > MAX_BATCH_BYTES
            {
                return Err(Error::IncompatibleStore);
            }
            total = total
                .saturating_add(entry.key.len())
                .saturating_add(entry.payload_bytes);
            if total > MAX_BATCH_BYTES {
                return Err(Error::Invalid("snapshot exceeds 16 MiB".into()));
            }
            entries.push(entry);
        }
        let keys = entries
            .iter()
            .map(|entry| store_key(&format!("projection/payload/{}", entry.key)))
            .collect::<Vec<_>>();
        let payloads = tx.get_many(&keys).await?;
        let mut projections = Vec::with_capacity(entries.len());
        for (entry, bytes) in entries.into_iter().zip(payloads) {
            let payload = decode_payload(bytes, entry.payload_bytes)?;
            projections.push(Projection {
                key: entry.key,
                payload,
            });
        }
        let snapshot = Snapshot {
            revision: metadata.revision,
            last_sequence: metadata.sequence,
            projections,
        };
        validate_response_bytes(&snapshot)?;
        Ok(snapshot)
    }

    pub async fn events(&self, after_sequence: u64, limit: u32) -> Result<Vec<StoredEvent>> {
        if after_sequence > MAX_COUNTER || limit == 0 || limit > 4096 {
            return Err(Error::Invalid("invalid event page".into()));
        }
        let mut tx = self.db.begin().await?;
        let result = self.events_in(&mut tx, after_sequence, limit).await;
        tx.rollback().await?;
        result
    }

    async fn events_in(
        &self,
        tx: &mut Transaction,
        after: u64,
        limit: u32,
    ) -> Result<Vec<StoredEvent>> {
        let start = store_key(&format!("event/meta/{:020}", after + 1));
        let (_, end) = prefix_range("event/meta/");
        let rows = tx.scan(&start, &end, limit as usize).await?;
        let mut entries = Vec::with_capacity(rows.len());
        let mut total = 0usize;
        let mut previous = after;
        for (key, bytes) in rows {
            let entry: EventMetadata = decode(&bytes)?;
            if !id_is_valid(&entry.id)
                || !id_is_valid(&entry.command_id)
                || entry.sequence <= previous
                || entry.sequence > MAX_COUNTER
                || key != store_key(&format!("event/meta/{:020}", entry.sequence))
                || entry.payload_bytes > MAX_BATCH_BYTES
            {
                return Err(Error::IncompatibleStore);
            }
            previous = entry.sequence;
            total = total
                .saturating_add(entry.payload_bytes)
                .saturating_add(entry.id.len())
                .saturating_add(entry.command_id.len());
            validate_page_bytes(total)?;
            entries.push(entry);
        }
        let keys = entries
            .iter()
            .map(|entry| store_key(&format!("event/payload/{:020}", entry.sequence)))
            .collect::<Vec<_>>();
        let payloads = tx.get_many(&keys).await?;
        let mut events = Vec::with_capacity(entries.len());
        for (entry, bytes) in entries.into_iter().zip(payloads) {
            let payload = decode_payload(bytes, entry.payload_bytes)?;
            events.push(StoredEvent {
                id: entry.id,
                sequence: entry.sequence,
                command_id: entry.command_id,
                payload,
            });
        }
        validate_response_bytes(&events)?;
        Ok(events)
    }

    pub async fn pending_effects(&self, limit: u32) -> Result<Vec<PendingEffect>> {
        if limit == 0 || limit > 4096 {
            return Err(Error::Invalid("invalid effect page".into()));
        }
        let mut tx = self.db.begin().await?;
        let result = self.effects_in(&mut tx, limit).await;
        tx.rollback().await?;
        result
    }

    async fn effects_in(&self, tx: &mut Transaction, limit: u32) -> Result<Vec<PendingEffect>> {
        let (start, end) = prefix_range("outbox/pending/");
        let rows = tx.scan(&start, &end, limit as usize).await?;
        let mut entries = Vec::with_capacity(rows.len());
        let mut total = 0usize;
        for (key, bytes) in rows {
            let entry: EffectMetadata = decode(&bytes)?;
            if !id_is_valid(&entry.id)
                || !id_is_valid(&entry.command_id)
                || key != store_key(&format!("outbox/pending/{}", entry.id))
                || entry.payload_bytes > MAX_BATCH_BYTES
            {
                return Err(Error::IncompatibleStore);
            }
            total = total
                .saturating_add(entry.payload_bytes)
                .saturating_add(entry.id.len())
                .saturating_add(entry.command_id.len());
            validate_page_bytes(total)?;
            entries.push(entry);
        }
        let keys = entries
            .iter()
            .map(|entry| store_key(&format!("outbox/payload/{}", entry.id)))
            .collect::<Vec<_>>();
        let payloads = tx.get_many(&keys).await?;
        let mut effects = Vec::with_capacity(entries.len());
        for (entry, bytes) in entries.into_iter().zip(payloads) {
            let payload = decode_payload(bytes, entry.payload_bytes)?;
            effects.push(PendingEffect {
                id: entry.id,
                command_id: entry.command_id,
                payload,
            });
        }
        validate_response_bytes(&effects)?;
        Ok(effects)
    }

    pub async fn commit(&self, batch: &CommitBatch) -> Result<CommitOutcome> {
        validate_batch(batch)?;
        let mut tx = self.db.begin().await?;
        if tx
            .get(&outbox_receipt_key(&batch.command_id))
            .await?
            .is_some()
        {
            tx.rollback().await?;
            return Err(Error::CommandConflict);
        }
        let staged = self.stage(&mut tx, batch).await;
        match staged {
            Ok(outcome) if outcome.replayed => {
                tx.rollback().await?;
                Ok(outcome)
            }
            Ok(outcome) => {
                tx.commit().await?;
                Ok(outcome)
            }
            Err(error) => {
                let _ = tx.rollback().await;
                Err(error)
            }
        }
    }

    /// Enqueue and cancel worker effects in the event/projection/receipt transaction.
    /// Retries reuse the original outbox envelope, including its host-supplied timestamps.
    pub async fn commit_outbox(
        &self,
        batch: &CommitBatch,
        outbox: &OutboxBatch,
    ) -> Result<OutboxCommitOutcome> {
        validate_batch(batch)?;
        if outbox.cancel.len() > MAX_BATCH_RECORDS
            || encode(batch)?.len().saturating_add(encode(outbox)?.len()) > MAX_BATCH_BYTES
        {
            return Err(Error::Invalid("outbox commit exceeds batch limits".into()));
        }
        outbox::validate_enqueue(&outbox.enqueue, &outbox.enqueue_at)?;
        for cancel in &outbox.cancel {
            outbox::validate_cancel(cancel)?;
        }
        let digest = request_digest(&serde_json::to_value(outbox).map_err(json_error)?);
        let mut tx = self.db.begin().await?;
        let staged = async {
            let commit = self.stage(&mut tx, batch).await?;
            let key = outbox_receipt_key(&batch.command_id);
            if commit.replayed {
                let bytes = tx.get(&key).await?.ok_or(Error::CommandConflict)?;
                let extra = decode_outbox_receipt(&bytes, &commit.receipt)?;
                if extra.outbox_digest != digest {
                    return Err(Error::CommandConflict);
                }
                return Ok(OutboxCommitOutcome {
                    commit,
                    enqueued: extra.enqueued,
                    cancelled: extra.cancelled,
                });
            }
            let enqueued =
                outbox::stage_enqueue(&mut tx, &outbox.enqueue, &outbox.enqueue_at).await?;
            let mut cancelled = Vec::new();
            for cancel in &outbox.cancel {
                cancelled.extend(outbox::stage_cancel_unsettled(&mut tx, cancel).await?);
                if cancelled.len() > MAX_BATCH_RECORDS {
                    return Err(Error::Invalid(
                        "too many cancelled effects in one commit".into(),
                    ));
                }
            }
            let extra = OutboxReceipt {
                command_id: batch.command_id.clone(),
                request_digest: batch.request_digest.clone(),
                outbox_digest: digest,
                enqueued,
                cancelled: cancelled.clone(),
            };
            tx.insert(key, encode(&extra)?).await?;
            let outcome = OutboxCommitOutcome {
                commit,
                enqueued,
                cancelled,
            };
            validate_response_bytes(&outcome)?;
            Ok(outcome)
        }
        .await;
        match staged {
            Ok(outcome) if outcome.commit.replayed => {
                tx.rollback().await?;
                Ok(outcome)
            }
            Ok(outcome) => {
                tx.commit().await?;
                Ok(outcome)
            }
            Err(error) => {
                let _ = tx.rollback().await;
                Err(error)
            }
        }
    }

    pub async fn read_outbox_receipt(
        &self,
        command_id: &str,
    ) -> Result<Option<OutboxCommitOutcome>> {
        validate_id(command_id)?;
        let mut tx = self.db.begin().await?;
        let result = async {
            let Some(extra) = tx.get(&outbox_receipt_key(command_id)).await? else {
                return Ok(None);
            };
            let bytes = tx
                .get(&store_key(&format!("receipt/{command_id}")))
                .await?
                .ok_or(Error::IncompatibleStore)?;
            let receipt = decode_receipt(&bytes, command_id)?;
            let extra = decode_outbox_receipt(&extra, &receipt)?;
            let outcome = OutboxCommitOutcome {
                commit: CommitOutcome {
                    receipt,
                    replayed: true,
                },
                enqueued: extra.enqueued,
                cancelled: extra.cancelled,
            };
            validate_response_bytes(&outcome)?;
            Ok(Some(outcome))
        }
        .await;
        tx.rollback().await?;
        result
    }

    async fn stage(&self, tx: &mut Transaction, batch: &CommitBatch) -> Result<CommitOutcome> {
        let receipt_key = store_key(&format!("receipt/{}", batch.command_id));
        if let Some(bytes) = tx.get(&receipt_key).await? {
            let receipt = decode_receipt(&bytes, &batch.command_id)?;
            if receipt.request_digest != batch.request_digest {
                return Err(Error::CommandConflict);
            }
            return Ok(CommitOutcome {
                receipt,
                replayed: true,
            });
        }
        let bytes = tx
            .get(&store_key("meta/state"))
            .await?
            .ok_or(Error::IncompatibleStore)?;
        let mut meta = decode_metadata(&bytes)?;
        if meta.revision != batch.expected_revision {
            return Err(Error::StaleRevision {
                expected: batch.expected_revision,
                actual: meta.revision,
            });
        }
        let sequence = meta.sequence;
        let last_sequence = sequence
            .checked_add(batch.events.len() as u64)
            .filter(|sequence| *sequence <= MAX_COUNTER)
            .ok_or_else(|| Error::Invalid("event sequence exhausted".into()))?;
        let receipt = Receipt {
            command_id: batch.command_id.clone(),
            request_digest: batch.request_digest.clone(),
            revision: meta.revision + 1,
            first_sequence: (!batch.events.is_empty()).then_some(sequence + 1),
            last_sequence,
            result: batch.result.clone(),
        };
        meta.revision = receipt.revision;
        meta.sequence = last_sequence;
        tx.put(store_key("meta/state"), encode(&meta)?).await?;
        for (index, event) in batch.events.iter().enumerate() {
            let sequence = sequence + index as u64 + 1;
            let payload = encode(&event.payload)?;
            let entry = EventMetadata {
                id: event.id.clone(),
                sequence,
                command_id: batch.command_id.clone(),
                payload_bytes: payload.len(),
            };
            tx.insert(
                store_key(&format!("event/id/{}", event.id)),
                sequence.to_be_bytes().to_vec(),
            )
            .await?;
            tx.insert(
                store_key(&format!("event/meta/{sequence:020}")),
                encode(&entry)?,
            )
            .await?;
            tx.insert(store_key(&format!("event/payload/{sequence:020}")), payload)
                .await?;
        }
        for mutation in &batch.projections {
            match mutation {
                ProjectionMutation::Put { key, payload } => {
                    let payload = encode(payload)?;
                    let entry = ProjectionMetadata {
                        key: key.clone(),
                        payload_bytes: payload.len(),
                    };
                    tx.put(
                        store_key(&format!("projection/meta/{key}")),
                        encode(&entry)?,
                    )
                    .await?;
                    tx.put(store_key(&format!("projection/payload/{key}")), payload)
                        .await?;
                }
                ProjectionMutation::Delete { key } => {
                    tx.delete(store_key(&format!("projection/meta/{key}")))
                        .await?;
                    tx.delete(store_key(&format!("projection/payload/{key}")))
                        .await?;
                }
            }
        }
        for effect in &batch.effects {
            let payload = encode(&effect.payload)?;
            let entry = EffectMetadata {
                id: effect.id.clone(),
                command_id: batch.command_id.clone(),
                payload_bytes: payload.len(),
            };
            tx.insert(
                store_key(&format!("outbox/id/{}", effect.id)),
                batch.command_id.as_bytes().to_vec(),
            )
            .await?;
            tx.insert(
                store_key(&format!("outbox/pending/{}", effect.id)),
                encode(&entry)?,
            )
            .await?;
            tx.insert(store_key(&format!("outbox/payload/{}", effect.id)), payload)
                .await?;
        }
        tx.insert(receipt_key, encode(&receipt)?).await?;
        Ok(CommitOutcome {
            receipt,
            replayed: false,
        })
    }
}

fn decode_payload(bytes: Option<Vec<u8>>, expected_bytes: usize) -> Result<Value> {
    let bytes = bytes.ok_or(Error::IncompatibleStore)?;
    if bytes.len() != expected_bytes {
        return Err(Error::IncompatibleStore);
    }
    decode(&bytes)
}

fn decode_metadata(bytes: &[u8]) -> Result<Metadata> {
    let metadata: Metadata = decode(bytes)?;
    if metadata.format_version != FORMAT_VERSION
        || metadata.store_kind != STORE_KIND
        || metadata.revision > MAX_COUNTER
        || metadata.sequence > MAX_COUNTER
    {
        return Err(Error::IncompatibleStore);
    }
    Ok(metadata)
}

fn decode_receipt(bytes: &[u8], command_id: &str) -> Result<Receipt> {
    let receipt: Receipt = decode(bytes)?;
    if receipt.command_id != command_id
        || !id_is_valid(&receipt.command_id)
        || receipt.request_digest.len() != 64
        || !receipt
            .request_digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        || receipt.revision == 0
        || receipt.revision > MAX_COUNTER
        || receipt.last_sequence > MAX_COUNTER
        || receipt
            .first_sequence
            .is_some_and(|first| first == 0 || first > receipt.last_sequence)
    {
        return Err(Error::IncompatibleStore);
    }
    Ok(receipt)
}

fn validate_page_bytes(bytes: usize) -> Result<()> {
    if bytes > MAX_BATCH_BYTES {
        return Err(Error::Invalid(
            "page exceeds 16 MiB; request fewer records".into(),
        ));
    }
    Ok(())
}

fn validate_response_bytes(value: &impl Serialize) -> Result<()> {
    if encode(value)?.len() > MAX_BATCH_BYTES {
        return Err(Error::Invalid(
            "response exceeds 16 MiB; request fewer records".into(),
        ));
    }
    Ok(())
}
