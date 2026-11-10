//! The application's shared event log over its imported redb row namespace.
//! Index preparation is resumable. Once preparation starts, generic row writes
//! cannot bypass the event identity, stream-version, sequence or replay indexes.

use std::collections::BTreeSet;
use std::time::Duration;

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Map, Value, json};

use crate::application::{
    ApplicationCatalog, ApplicationRow, ApplicationTransactions, Session, decode_catalog,
    decode_row, encode_row, row_key, row_prefix, schema_key, table, validate_row,
};
use crate::application_json::{compact_json, field, object_fields, render_fields, set_field};
use crate::application_rows::{integer, nullable, text, text_cell};
use crate::backend::Transaction;
use crate::{Error, MAX_BATCH_BYTES, Result, Store, request_digest, store_key};

const EVENTS: &str = "orchestration_events";
const METADATA: &str = "orchestration_v2_projection_metadata";
const MAX_SEQUENCE: u64 = (1 << 53) - 1;
const PREPARE_ROWS: usize = 32;
const MAX_PAGE: u32 = 4096;
const COLUMNS: [&str; 14] = [
    "sequence",
    "event_id",
    "aggregate_kind",
    "stream_id",
    "stream_version",
    "event_type",
    "occurred_at",
    "command_id",
    "causation_event_id",
    "correlation_id",
    "actor_kind",
    "payload_json",
    "metadata_json",
    "application_event_version",
];

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EventAppendRow {
    pub event_id: String,
    pub aggregate_kind: String,
    pub stream_id: String,
    pub event_type: String,
    pub occurred_at: String,
    pub command_id: Option<String>,
    pub causation_event_id: Option<String>,
    pub correlation_id: Option<String>,
    pub actor_kind: String,
    pub payload_json: String,
    pub metadata_json: String,
    pub application_event_version: i64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct EventRead {
    pub after_sequence: u64,
    pub through_sequence: Option<u64>,
    pub thread_id: Option<String>,
    pub command_id: Option<String>,
    pub event_type: Option<String>,
    pub only_agent_events: Option<bool>,
    pub limit: u32,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct EventStats {
    pub kind: String,
    pub after_sequence: u64,
    pub through_sequence: u64,
    pub thread_id: Option<String>,
    pub max_events: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct EventLatest {
    pub kind: String,
    pub thread_id: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct AppendResult {
    pub sequence: u64,
    pub stores_full_prompt: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct EventResultRow {
    pub sequence: u64,
    pub event_id: String,
    pub command_id: Option<String>,
    pub aggregate_kind: String,
    pub stream_id: String,
    pub event_type: String,
    pub occurred_at: String,
    pub payload_json: String,
    pub metadata_json: String,
    pub application_event_version: i64,
    pub causation_event_id: Option<String>,
    pub correlation_id: Option<String>,
}

#[derive(Clone, Debug)]
struct EventRecord {
    sequence: u64,
    stream_version: i64,
    row: EventAppendRow,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Preparation {
    version: u32,
    ready: bool,
    after: Option<String>,
    indexed_rows: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PreparationResult {
    pub prepared: bool,
    pub indexed_rows: u64,
}

#[derive(Clone, Copy, Default, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
struct Aggregate {
    count: u64,
    bytes: u64,
    creates: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct StatsResult {
    pub event_count: u64,
    pub raw_payload_bytes: u64,
    pub has_create_event: bool,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StreamVersion {
    kind: String,
    stream: String,
    version: i64,
}

fn invalid(message: &str) -> Error {
    Error::Invalid(message.into())
}
fn encode(value: &impl Serialize) -> Result<Vec<u8>> {
    let bytes = serde_json::to_vec(value).map_err(crate::json_error)?;
    if bytes.len() > MAX_BATCH_BYTES {
        return Err(invalid("event value exceeds transaction limit"));
    }
    Ok(bytes)
}
fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    serde_json::from_slice(bytes).map_err(|_| Error::IncompatibleStore)
}
fn key(catalog: &ApplicationCatalog, suffix: &str) -> Vec<u8> {
    store_key(&format!(
        "application/generation/{}/events/v1/{suffix}",
        catalog.generation
    ))
}
fn marker(catalog: &ApplicationCatalog) -> Vec<u8> {
    let name: String = EVENTS.bytes().map(|b| format!("{b:02x}")).collect();
    store_key(&format!(
        "application/generation/{}/managed/{name}",
        catalog.generation
    ))
}
fn int_cell(value: impl ToString) -> Value {
    json!({"type":"integer","value":value.to_string()})
}
fn nullable_cell(value: &Option<String>) -> Value {
    crate::application_rows::nullable_cell(value.as_deref())
}
fn safe_sequence(value: u64) -> Result<u64> {
    if value > MAX_SEQUENCE {
        Err(invalid("event sequence exceeds JavaScript integer range"))
    } else {
        Ok(value)
    }
}
fn validate_event_table(catalog: &ApplicationCatalog) -> Result<()> {
    let definition = table(catalog, EVENTS)?;
    let columns: BTreeSet<&str> = definition.columns.iter().map(String::as_str).collect();
    if definition.primary_key != ["sequence"] || columns != COLUMNS.into_iter().collect() {
        return Err(invalid(
            "event repository requires the shared application event schema",
        ));
    }
    let metadata = table(catalog, METADATA)?;
    if metadata.primary_key != ["projection_name"]
        || !metadata
            .columns
            .iter()
            .any(|column| column == "last_sequence")
    {
        return Err(invalid("event repository requires projection metadata"));
    }
    Ok(())
}

impl EventRecord {
    fn from_row(row: Map<String, Value>) -> Result<Self> {
        let sequence = integer(&row, "sequence")?;
        if sequence <= 0 {
            return Err(Error::IncompatibleStore);
        }
        safe_sequence(sequence as u64)?;
        Ok(Self {
            sequence: sequence as u64,
            stream_version: integer(&row, "stream_version")?,
            row: EventAppendRow {
                event_id: text(&row, "event_id")?,
                aggregate_kind: text(&row, "aggregate_kind")?,
                stream_id: text(&row, "stream_id")?,
                event_type: text(&row, "event_type")?,
                occurred_at: text(&row, "occurred_at")?,
                command_id: nullable(&row, "command_id")?,
                causation_event_id: nullable(&row, "causation_event_id")?,
                correlation_id: nullable(&row, "correlation_id")?,
                actor_kind: text(&row, "actor_kind")?,
                payload_json: text(&row, "payload_json")?,
                metadata_json: text(&row, "metadata_json")?,
                application_event_version: integer(&row, "application_event_version")?,
            },
        })
    }

    fn stored(&self) -> ApplicationRow {
        let mut row = Map::new();
        row.insert("sequence".into(), int_cell(self.sequence));
        row.insert("stream_version".into(), int_cell(self.stream_version));
        row.insert(
            "application_event_version".into(),
            int_cell(self.row.application_event_version),
        );
        for (name, value) in [
            ("event_id", &self.row.event_id),
            ("aggregate_kind", &self.row.aggregate_kind),
            ("stream_id", &self.row.stream_id),
            ("event_type", &self.row.event_type),
            ("occurred_at", &self.row.occurred_at),
            ("actor_kind", &self.row.actor_kind),
            ("payload_json", &self.row.payload_json),
            ("metadata_json", &self.row.metadata_json),
        ] {
            row.insert(name.into(), text_cell(value));
        }
        row.insert("command_id".into(), nullable_cell(&self.row.command_id));
        row.insert(
            "causation_event_id".into(),
            nullable_cell(&self.row.causation_event_id),
        );
        row.insert(
            "correlation_id".into(),
            nullable_cell(&self.row.correlation_id),
        );
        ApplicationRow {
            key: vec![int_cell(self.sequence)],
            row,
        }
    }

    fn agent(&self) -> bool {
        self.row.aggregate_kind == "thread" && self.row.application_event_version == 2
    }
    fn application(&self) -> bool {
        self.row.aggregate_kind == "project" || self.agent()
    }
    fn output(self, payload_json: String) -> EventResultRow {
        EventResultRow {
            sequence: self.sequence,
            event_id: self.row.event_id,
            command_id: self.row.command_id,
            aggregate_kind: self.row.aggregate_kind,
            stream_id: self.row.stream_id,
            event_type: self.row.event_type,
            occurred_at: self.row.occurred_at,
            payload_json,
            metadata_json: self.row.metadata_json,
            application_event_version: self.row.application_event_version,
            causation_event_id: self.row.causation_event_id,
            correlation_id: self.row.correlation_id,
        }
    }
}

async fn event_at(
    tx: &mut Transaction,
    catalog: &ApplicationCatalog,
    sequence: u64,
) -> Result<Option<EventRecord>> {
    let encoded_key = row_key(&catalog.generation, EVENTS, &[int_cell(sequence)])?;
    let Some(bytes) = tx.get(&encoded_key).await? else {
        return Ok(None);
    };
    let value = decode_row(catalog, table(catalog, EVENTS)?, &encoded_key, &bytes)?;
    let event = EventRecord::from_row(value.row)?;
    if event.sequence != sequence {
        return Err(Error::IncompatibleStore);
    }
    Ok(Some(event))
}

async fn restored_payload(
    tx: &mut Transaction,
    catalog: &ApplicationCatalog,
    event: &EventRecord,
) -> Result<String> {
    if !matches!(
        event.row.event_type.as_str(),
        "subagent.updated" | "turn-item.updated"
    ) {
        return Ok(event.row.payload_json.clone());
    }
    let Some(mut fields) = object_fields(&event.row.payload_json)? else {
        return Ok(event.row.payload_json.clone());
    };
    let Some(raw_sequence) = field(&fields, "promptSequence") else {
        return Ok(event.row.payload_json.clone());
    };
    // SQLite's JSON type follows the integer token, including -0 and integers
    // too large for i64. serde_json may represent those tokens as f64 instead.
    let digits = raw_sequence.strip_prefix('-').unwrap_or(raw_sequence);
    if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return Ok(event.row.payload_json.clone());
    }
    let anchor = match raw_sequence.parse::<u64>().ok() {
        Some(sequence) if sequence > 0 && sequence <= MAX_SEQUENCE => {
            event_at(tx, catalog, sequence).await?
        }
        _ => None,
    };
    let prompt = match anchor {
        Some(anchor) => {
            let fields = object_fields(&anchor.row.payload_json)?.unwrap_or_default();
            match field(&fields, "prompt") {
                None => "null".into(),
                Some(value) => {
                    let parsed: Value =
                        serde_json::from_str(value).map_err(|_| Error::IncompatibleStore)?;
                    match parsed {
                        Value::Array(_) | Value::Object(_) => compact_json(value),
                        Value::Bool(value) => if value { "1" } else { "0" }.into(),
                        Value::Number(number) => {
                            // SQLite coerces a scalar JSON REAL through its SQL
                            // formatter. Non-string prompts are not valid domain
                            // records; do not invent different bytes for a REAL.
                            number.as_i64().ok_or(Error::IncompatibleStore)?.to_string()
                        }
                        value => serde_json::to_string(&value).map_err(crate::json_error)?,
                    }
                }
            }
        }
        None => "null".into(),
    };
    if let Some(index) = fields
        .iter()
        .position(|field| field.name == "promptSequence")
    {
        fields.remove(index);
    }
    set_field(&mut fields, "prompt", prompt);
    Ok(render_fields(&fields))
}

async fn compact_prompt(
    tx: &mut Transaction,
    catalog: &ApplicationCatalog,
    row: &mut EventAppendRow,
    hint: Option<u64>,
) -> Result<bool> {
    let Some(hint) = hint else {
        return Ok(true);
    };
    safe_sequence(hint)?;
    let Some(anchor) = event_at(tx, catalog, hint).await? else {
        return Ok(true);
    };
    let Some(anchor_fields) = object_fields(&anchor.row.payload_json)? else {
        return Ok(true);
    };
    if field(&anchor_fields, "promptSequence").is_some() {
        return Ok(true);
    }
    let Some(mut fields) = object_fields(&row.payload_json)? else {
        return Ok(true);
    };
    let Some(prompt) = field(&fields, "prompt") else {
        return Ok(true);
    };
    let Some(anchor_prompt) = field(&anchor_fields, "prompt") else {
        return Ok(true);
    };
    let prompt: Value = serde_json::from_str(prompt).map_err(|_| Error::IncompatibleStore)?;
    let anchor_prompt: Value =
        serde_json::from_str(anchor_prompt).map_err(|_| Error::IncompatibleStore)?;
    if !prompt.is_string() || prompt != anchor_prompt {
        return Ok(true);
    }
    set_field(&mut fields, "prompt", "\"\"".into());
    set_field(&mut fields, "promptSequence", hint.to_string());
    row.payload_json = render_fields(&fields);
    Ok(false)
}

fn event_filter_prefix(
    catalog: &ApplicationCatalog,
    agent: bool,
    stream: Option<&str>,
    command: Option<&str>,
    event_type: Option<&str>,
) -> Vec<u8> {
    let mask = usize::from(stream.is_some())
        | usize::from(command.is_some()) << 1
        | usize::from(event_type.is_some()) << 2;
    let scope = request_digest(&json!([stream, command, event_type]));
    key(
        catalog,
        &format!(
            "index/{}/{mask}/{scope}/",
            if agent { "agent" } else { "application" }
        ),
    )
}
fn ordered_key(mut prefix: Vec<u8>, sequence: u64) -> Vec<u8> {
    prefix.extend_from_slice(format!("{sequence:016x}").as_bytes());
    prefix
}
fn prefix_end(prefix: &[u8]) -> Vec<u8> {
    let mut end = prefix.to_vec();
    *end.last_mut().expect("index prefix is nonempty") += 1;
    end
}
fn sequence_from_key(prefix: &[u8], encoded: &[u8]) -> Result<u64> {
    let suffix = encoded
        .strip_prefix(prefix)
        .and_then(|bytes| std::str::from_utf8(bytes).ok())
        .ok_or(Error::IncompatibleStore)?;
    if suffix.len() != 16 {
        return Err(Error::IncompatibleStore);
    }
    u64::from_str_radix(suffix, 16).map_err(|_| Error::IncompatibleStore)
}
fn aggregate_scope(thread: Option<&str>) -> String {
    thread.map_or_else(
        || "application".into(),
        |thread| format!("thread/{}", request_digest(&json!(thread))),
    )
}
fn aggregate_key(catalog: &ApplicationCatalog, scope: &str, index: u64) -> Vec<u8> {
    key(catalog, &format!("statistics/{scope}/{index:016x}"))
}
impl Aggregate {
    fn add(self, other: Self) -> Result<Self> {
        let result = Self {
            count: self
                .count
                .checked_add(other.count)
                .ok_or(Error::IncompatibleStore)?,
            bytes: self
                .bytes
                .checked_add(other.bytes)
                .ok_or(Error::IncompatibleStore)?,
            creates: self
                .creates
                .checked_add(other.creates)
                .ok_or(Error::IncompatibleStore)?,
        };
        if result.count > MAX_SEQUENCE
            || result.bytes > MAX_SEQUENCE
            || result.creates > result.count
        {
            return Err(Error::IncompatibleStore);
        }
        Ok(result)
    }
    fn subtract(self, other: Self) -> Result<Self> {
        Ok(Self {
            count: self
                .count
                .checked_sub(other.count)
                .ok_or(Error::IncompatibleStore)?,
            bytes: self
                .bytes
                .checked_sub(other.bytes)
                .ok_or(Error::IncompatibleStore)?,
            creates: self
                .creates
                .checked_sub(other.creates)
                .ok_or(Error::IncompatibleStore)?,
        })
    }
}
async fn add_statistics(
    tx: &mut Transaction,
    catalog: &ApplicationCatalog,
    scope: &str,
    sequence: u64,
    delta: Aggregate,
) -> Result<()> {
    let mut index = sequence;
    let mut keys = Vec::new();
    while index <= MAX_SEQUENCE {
        keys.push(aggregate_key(catalog, scope, index));
        index += index & index.wrapping_neg();
    }
    let values = tx.get_many(&keys).await?;
    for (key, value) in keys.into_iter().zip(values) {
        let existing: Aggregate = value
            .as_deref()
            .map(decode)
            .transpose()?
            .unwrap_or_default();
        tx.put(key, encode(&existing.add(delta)?)?).await?;
    }
    Ok(())
}
async fn prefix_statistics(
    tx: &mut Transaction,
    catalog: &ApplicationCatalog,
    scope: &str,
    mut sequence: u64,
) -> Result<Aggregate> {
    let mut keys = Vec::new();
    while sequence > 0 {
        keys.push(aggregate_key(catalog, scope, sequence));
        sequence -= sequence & sequence.wrapping_neg();
    }
    let mut total = Aggregate::default();
    for value in tx.get_many(&keys).await? {
        total = total.add(
            value
                .as_deref()
                .map(decode)
                .transpose()?
                .unwrap_or_default(),
        )?;
    }
    Ok(total)
}
async fn sequence_at_count(
    tx: &mut Transaction,
    catalog: &ApplicationCatalog,
    scope: &str,
    mut count: u64,
) -> Result<u64> {
    let mut index = 0u64;
    let mut bit = 1 << 52;
    while bit != 0 {
        let next = index + bit;
        let aggregate: Aggregate = tx
            .get(&aggregate_key(catalog, scope, next))
            .await?
            .as_deref()
            .map(decode)
            .transpose()?
            .unwrap_or_default();
        if aggregate.count < count {
            count -= aggregate.count;
            index = next;
        }
        bit >>= 1;
    }
    if index >= MAX_SEQUENCE || count == 0 {
        return Err(Error::IncompatibleStore);
    }
    Ok(index + 1)
}
async fn max_value(tx: &mut Transaction, key: Vec<u8>, value: u64) -> Result<()> {
    let old: u64 = tx
        .get(&key)
        .await?
        .as_deref()
        .map(decode)
        .transpose()?
        .unwrap_or(0);
    if value > old {
        tx.put(key, encode(&value)?).await?;
    }
    Ok(())
}
fn stream_key(catalog: &ApplicationCatalog, kind: &str, stream: &str) -> Vec<u8> {
    key(
        catalog,
        &format!("stream/{}", request_digest(&json!([kind, stream]))),
    )
}

async fn index_event(
    tx: &mut Transaction,
    catalog: &ApplicationCatalog,
    event: &EventRecord,
) -> Result<()> {
    // Original identities accompany hashed keys so collisions fail closed.
    for (suffix, witness) in [
        (
            format!("identity/{}", request_digest(&json!(event.row.event_id))),
            json!([event.row.event_id, event.sequence]),
        ),
        (
            format!(
                "version/{}",
                request_digest(&json!([
                    event.row.aggregate_kind,
                    event.row.stream_id,
                    event.stream_version
                ]))
            ),
            json!([
                event.row.aggregate_kind,
                event.row.stream_id,
                event.stream_version,
                event.sequence
            ]),
        ),
    ] {
        let key = key(catalog, &suffix);
        if let Some(existing) = tx.get(&key).await? {
            let existing: Value = decode(&existing)?;
            let mut left = existing.as_array().ok_or(Error::IncompatibleStore)?.clone();
            let mut right = witness.as_array().ok_or(Error::IncompatibleStore)?.clone();
            left.pop();
            right.pop();
            return Err(if left == right {
                Error::DuplicateRecord
            } else {
                Error::IncompatibleStore
            });
        }
        tx.put(key, encode(&witness)?).await?;
    }
    let stream_key = stream_key(catalog, &event.row.aggregate_kind, &event.row.stream_id);
    let previous: Option<StreamVersion> = tx
        .get(&stream_key)
        .await?
        .as_deref()
        .map(decode)
        .transpose()?;
    if previous.as_ref().is_some_and(|previous| {
        previous.kind != event.row.aggregate_kind || previous.stream != event.row.stream_id
    }) {
        return Err(Error::IncompatibleStore);
    }
    if previous
        .as_ref()
        .is_none_or(|previous| previous.version < event.stream_version)
    {
        tx.put(
            stream_key,
            encode(&StreamVersion {
                kind: event.row.aggregate_kind.clone(),
                stream: event.row.stream_id.clone(),
                version: event.stream_version,
            })?,
        )
        .await?;
    }
    max_value(tx, key(catalog, "sequence"), event.sequence).await?;
    if !event.application() {
        return Ok(());
    }
    let payload = restored_payload(tx, catalog, event).await?;
    let delta = Aggregate {
        count: 1,
        bytes: payload.len() as u64,
        creates: u64::from(event.row.event_type == "thread.created"),
    };
    add_statistics(tx, catalog, &aggregate_scope(None), event.sequence, delta).await?;
    max_value(tx, key(catalog, "latest/application"), event.sequence).await?;
    if event.agent() {
        let witness_key = key(
            catalog,
            &format!(
                "statistics-owner/{}",
                aggregate_scope(Some(&event.row.stream_id))
            ),
        );
        if let Some(bytes) = tx.get(&witness_key).await? {
            if decode::<String>(&bytes)? != event.row.stream_id {
                return Err(Error::IncompatibleStore);
            }
        } else {
            tx.put(witness_key, encode(&event.row.stream_id)?).await?;
        }
        add_statistics(
            tx,
            catalog,
            &aggregate_scope(Some(&event.row.stream_id)),
            event.sequence,
            delta,
        )
        .await?;
        max_value(tx, key(catalog, "latest/agent"), event.sequence).await?;
        max_value(
            tx,
            key(
                catalog,
                &format!(
                    "latest/thread/{}",
                    request_digest(&json!(event.row.stream_id))
                ),
            ),
            event.sequence,
        )
        .await?;
    }
    for agent in [false, true] {
        if agent && !event.agent() {
            continue;
        }
        for mask in 0..8 {
            if mask & 2 != 0 && event.row.command_id.is_none() {
                continue;
            }
            let prefix = event_filter_prefix(
                catalog,
                agent,
                (mask & 1 != 0).then_some(event.row.stream_id.as_str()),
                if mask & 2 != 0 {
                    event.row.command_id.as_deref()
                } else {
                    None
                },
                (mask & 4 != 0).then_some(event.row.event_type.as_str()),
            );
            tx.insert(
                ordered_key(prefix, event.sequence),
                encode(&event.sequence)?,
            )
            .await?;
        }
    }
    Ok(())
}

async fn preparation(tx: &mut Transaction, catalog: &ApplicationCatalog) -> Result<Preparation> {
    if tx
        .get(&marker(catalog))
        .await?
        .as_deref()
        .map(decode::<String>)
        .transpose()?
        .as_deref()
        != Some("event-index-v1")
    {
        return Err(Error::IncompatibleStore);
    }
    let state: Preparation = tx
        .get(&key(catalog, "preparation"))
        .await?
        .as_deref()
        .map(decode)
        .transpose()?
        .ok_or_else(|| invalid("application event indexes are not prepared"))?;
    if state.version != 1 {
        return Err(Error::IncompatibleStore);
    }
    Ok(state)
}
async fn ready(session: &mut Session) -> Result<()> {
    if !preparation(&mut session.transaction, &session.catalog)
        .await?
        .ready
    {
        return Err(invalid("application event indexes are not prepared"));
    }
    Ok(())
}

impl Store {
    /// Publish one bounded index-building batch. A lost acknowledgement is resolved
    /// by the durable cursor; repeating a completed preparation never rebuilds live rows.
    pub async fn prepare_application_events(&self) -> Result<PreparationResult> {
        // A large source row can make a multi-row read exceed the existing budget.
        // Shrinking a batch is safe only before commit, after a known resource error.
        let mut limit = PREPARE_ROWS;
        loop {
            match self.prepare_application_events_batch(limit).await {
                Err(Error::ResourceLimit(_)) if limit > 1 => {
                    limit = (limit / 2).max(1);
                }
                result => return result,
            }
        }
    }

    async fn prepare_application_events_batch(&self, limit: usize) -> Result<PreparationResult> {
        let mut tx = self
            .db
            .begin_reserved(Duration::from_secs(5), Duration::from_secs(300))
            .await?;
        let catalog = decode_catalog(
            &tx.get(&schema_key())
                .await?
                .ok_or_else(|| invalid("application schema is not initialized"))?,
        )?;
        validate_event_table(&catalog)?;
        let stored = tx.get(&key(&catalog, "preparation")).await?;
        let mut state = match stored {
            Some(bytes) => {
                let state: Preparation = decode(&bytes)?;
                if state.version != 1 {
                    return Err(Error::IncompatibleStore);
                }
                if tx
                    .get(&marker(&catalog))
                    .await?
                    .as_deref()
                    .map(decode::<String>)
                    .transpose()?
                    .as_deref()
                    != Some("event-index-v1")
                {
                    return Err(Error::IncompatibleStore);
                }
                state
            }
            None => {
                if tx.get(&marker(&catalog)).await?.is_some() {
                    return Err(Error::IncompatibleStore);
                }
                tx.put(marker(&catalog), encode(&"event-index-v1")?).await?;
                let high_water = if catalog.generation == "native" {
                    0
                } else {
                    let header_key = store_key(&format!(
                        "application/generation/{}/metadata/header",
                        catalog.generation
                    ));
                    let header: Value =
                        decode(&tx.get(&header_key).await?.ok_or(Error::IncompatibleStore)?)?;
                    let sequences = header
                        .get("sequences")
                        .and_then(Value::as_array)
                        .ok_or(Error::IncompatibleStore)?;
                    match sequences
                        .iter()
                        .find(|value| value.get("name").and_then(Value::as_str) == Some(EVENTS))
                    {
                        None => 0,
                        Some(value) => {
                            let value: i64 = value
                                .get("sequence")
                                .and_then(Value::as_str)
                                .ok_or(Error::IncompatibleStore)?
                                .parse()
                                .map_err(|_| Error::IncompatibleStore)?;
                            safe_sequence(value.max(0) as u64)?
                        }
                    }
                };
                tx.put(key(&catalog, "sequence"), encode(&high_water)?)
                    .await?;
                Preparation {
                    version: 1,
                    ready: false,
                    after: None,
                    indexed_rows: 0,
                }
            }
        };
        if state.ready {
            tx.rollback().await?;
            return Ok(PreparationResult {
                prepared: true,
                indexed_rows: state.indexed_rows,
            });
        }
        let prefix = row_prefix(&catalog.generation, EVENTS)?;
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
        let rows = tx.scan(&start, &prefix_end(&prefix), limit).await?;
        let count = rows.len();
        for (encoded, bytes) in rows {
            let source = decode_row(&catalog, table(&catalog, EVENTS)?, &encoded, &bytes)?;
            let event = EventRecord::from_row(source.row)?;
            index_event(&mut tx, &catalog, &event).await?;
            state.after = Some(
                std::str::from_utf8(
                    encoded
                        .strip_prefix(prefix.as_slice())
                        .ok_or(Error::IncompatibleStore)?,
                )
                .map_err(|_| Error::IncompatibleStore)?
                .to_owned(),
            );
            state.indexed_rows = state
                .indexed_rows
                .checked_add(1)
                .ok_or(Error::IncompatibleStore)?;
        }
        state.ready = count < limit;
        tx.put(key(&catalog, "preparation"), encode(&state)?)
            .await?;
        tx.commit().await?;
        Ok(PreparationResult {
            prepared: state.ready,
            indexed_rows: state.indexed_rows,
        })
    }
}

impl ApplicationTransactions {
    pub async fn events_append(
        &mut self,
        id: &str,
        mut row: EventAppendRow,
        hint: Option<u64>,
    ) -> Result<AppendResult> {
        let mut session = self.take(id)?;
        let result = async {
            session.transaction.require_write()?;
            ready(&mut session).await?;
            serde_json::from_str::<Value>(&row.payload_json)
                .map_err(|_| invalid("invalid event payload JSON"))?;
            serde_json::from_str::<Value>(&row.metadata_json)
                .map_err(|_| invalid("invalid event metadata JSON"))?;
            if !matches!(row.aggregate_kind.as_str(), "project" | "thread")
                || row.application_event_version != 2
                || !matches!(row.actor_kind.as_str(), "client" | "server" | "provider")
            {
                return Err(invalid("invalid application event identity"));
            }
            let sequence: u64 = session
                .transaction
                .get(&key(&session.catalog, "sequence"))
                .await?
                .as_deref()
                .map(decode)
                .transpose()?
                .ok_or(Error::IncompatibleStore)?;
            let sequence = sequence
                .checked_add(1)
                .filter(|value| *value <= MAX_SEQUENCE)
                .ok_or_else(|| invalid("application event sequence exhausted"))?;
            let previous: Option<StreamVersion> = session
                .transaction
                .get(&stream_key(
                    &session.catalog,
                    &row.aggregate_kind,
                    &row.stream_id,
                ))
                .await?
                .as_deref()
                .map(decode)
                .transpose()?;
            if previous.as_ref().is_some_and(|previous| {
                previous.kind != row.aggregate_kind || previous.stream != row.stream_id
            }) {
                return Err(Error::IncompatibleStore);
            }
            let stream_version = match previous {
                Some(previous) => previous
                    .version
                    .checked_add(1)
                    .ok_or_else(|| invalid("event stream version exhausted"))?,
                None => 0,
            };
            let stores_full_prompt =
                compact_prompt(&mut session.transaction, &session.catalog, &mut row, hint).await?;
            let event = EventRecord {
                sequence,
                stream_version,
                row,
            };
            let stored = event.stored();
            validate_row(table(&session.catalog, EVENTS)?, &stored.key, &stored.row)?;
            session
                .transaction
                .insert(
                    row_key(&session.catalog.generation, EVENTS, &stored.key)?,
                    encode_row(&stored)?,
                )
                .await?;
            index_event(&mut session.transaction, &session.catalog, &event).await?;
            Ok(AppendResult {
                sequence,
                stores_full_prompt,
            })
        }
        .await;
        self.finish(id, session, result).await
    }

    pub async fn events_read(&mut self, id: &str, input: EventRead) -> Result<Vec<EventResultRow>> {
        let mut session = self.take(id)?;
        let result = async {
            ready(&mut session).await?;
            safe_sequence(input.after_sequence)?;
            let through = safe_sequence(input.through_sequence.unwrap_or(MAX_SEQUENCE))?;
            if input.limit > MAX_PAGE {
                return Err(invalid("event page exceeds 4096 rows"));
            }
            if input.limit == 0 || input.after_sequence >= through {
                return Ok(Vec::new());
            }
            let agent = input.only_agent_events.unwrap_or(false);
            let prefix = event_filter_prefix(
                &session.catalog,
                agent,
                input.thread_id.as_deref(),
                input.command_id.as_deref(),
                input.event_type.as_deref(),
            );
            let start = ordered_key(prefix.clone(), input.after_sequence + 1);
            let end = ordered_key(prefix.clone(), through + 1);
            let rows = session
                .transaction
                .scan(&start, &end, input.limit as usize)
                .await?;
            let mut output = Vec::with_capacity(rows.len());
            for (encoded, bytes) in rows {
                let sequence = sequence_from_key(&prefix, &encoded)?;
                if decode::<u64>(&bytes)? != sequence {
                    return Err(Error::IncompatibleStore);
                }
                let event = event_at(&mut session.transaction, &session.catalog, sequence)
                    .await?
                    .ok_or(Error::IncompatibleStore)?;
                if !(if agent {
                    event.agent()
                } else {
                    event.application()
                }) || input
                    .thread_id
                    .as_ref()
                    .is_some_and(|value| *value != event.row.stream_id)
                    || input
                        .command_id
                        .as_ref()
                        .is_some_and(|value| event.row.command_id.as_ref() != Some(value))
                    || input
                        .event_type
                        .as_ref()
                        .is_some_and(|value| *value != event.row.event_type)
                {
                    return Err(Error::IncompatibleStore);
                }
                let payload =
                    restored_payload(&mut session.transaction, &session.catalog, &event).await?;
                output.push(event.output(payload));
            }
            crate::validate_response_bytes(&output)?;
            Ok(output)
        }
        .await;
        self.finish(id, session, result).await
    }

    pub async fn events_stats(&mut self, id: &str, input: EventStats) -> Result<StatsResult> {
        let mut session = self.take(id)?;
        let result = async {
            ready(&mut session).await?;
            safe_sequence(input.after_sequence)?;
            safe_sequence(input.through_sequence)?;
            let scope = match input.kind.as_str() {
                "application" if input.thread_id.is_none() && input.max_events.is_none() => {
                    aggregate_scope(None)
                }
                "agent" => aggregate_scope(Some(
                    input
                        .thread_id
                        .as_deref()
                        .ok_or_else(|| invalid("agent statistics require a thread"))?,
                )),
                _ => return Err(invalid("invalid event statistics scope")),
            };
            if let Some(thread) = &input.thread_id {
                let witness_key = key(&session.catalog, &format!("statistics-owner/{scope}"));
                if let Some(bytes) = session.transaction.get(&witness_key).await? {
                    if decode::<String>(&bytes)? != *thread {
                        return Err(Error::IncompatibleStore);
                    }
                }
            }
            if input.after_sequence >= input.through_sequence {
                return Ok(StatsResult {
                    event_count: 0,
                    raw_payload_bytes: 0,
                    has_create_event: false,
                });
            }
            let before = prefix_statistics(
                &mut session.transaction,
                &session.catalog,
                &scope,
                input.after_sequence,
            )
            .await?;
            let through = prefix_statistics(
                &mut session.transaction,
                &session.catalog,
                &scope,
                input.through_sequence,
            )
            .await?;
            let mut selected = through.subtract(before)?;
            if let Some(max) = input.max_events {
                safe_sequence(max)?;
                let limit = max.saturating_add(1);
                if selected.count > limit {
                    let sequence = sequence_at_count(
                        &mut session.transaction,
                        &session.catalog,
                        &scope,
                        before.count + limit,
                    )
                    .await?;
                    selected = prefix_statistics(
                        &mut session.transaction,
                        &session.catalog,
                        &scope,
                        sequence,
                    )
                    .await?
                    .subtract(before)?;
                }
            }
            Ok(StatsResult {
                event_count: selected.count,
                raw_payload_bytes: selected.bytes,
                has_create_event: input.kind == "agent" && selected.creates != 0,
            })
        }
        .await;
        self.finish(id, session, result).await
    }

    pub async fn events_latest(&mut self, id: &str, input: EventLatest) -> Result<u64> {
        let mut session = self.take(id)?;
        let result = async {
            ready(&mut session).await?;
            let suffix = match input.kind.as_str() {
                "application" if input.thread_id.is_none() => "latest/application".into(),
                "agent" => input.thread_id.as_ref().map_or_else(
                    || "latest/agent".into(),
                    |thread| format!("latest/thread/{}", request_digest(&json!(thread))),
                ),
                _ => return Err(invalid("invalid latest event scope")),
            };
            let sequence: u64 = session
                .transaction
                .get(&key(&session.catalog, &suffix))
                .await?
                .as_deref()
                .map(decode)
                .transpose()?
                .unwrap_or(0);
            if sequence > 0 {
                let event = event_at(&mut session.transaction, &session.catalog, sequence)
                    .await?
                    .ok_or(Error::IncompatibleStore)?;
                if !(if input.kind == "agent" {
                    event.agent()
                } else {
                    event.application()
                }) || input
                    .thread_id
                    .as_ref()
                    .is_some_and(|thread| *thread != event.row.stream_id)
                {
                    return Err(Error::IncompatibleStore);
                }
            }
            Ok(sequence)
        }
        .await;
        self.finish(id, session, result).await
    }

    pub async fn events_pruned_through(&mut self, id: &str) -> Result<u64> {
        let mut session = self.take(id)?;
        let result = async {
            ready(&mut session).await?;
            let encoded = row_key(
                &session.catalog.generation,
                METADATA,
                &[text_cell("event-log-pruned-through")],
            )?;
            let Some(bytes) = session.transaction.get(&encoded).await? else {
                return Ok(0);
            };
            let row = decode_row(
                &session.catalog,
                table(&session.catalog, METADATA)?,
                &encoded,
                &bytes,
            )?;
            let sequence = integer(&row.row, "last_sequence")?;
            if sequence < 0 {
                return Err(Error::IncompatibleStore);
            }
            safe_sequence(sequence as u64)
        }
        .await;
        self.finish(id, session, result).await
    }
}

#[cfg(test)]
#[path = "redb_event_rows_tests.rs"]
mod tests;
