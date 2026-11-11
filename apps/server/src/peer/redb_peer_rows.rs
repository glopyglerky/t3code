//! Peer link rows. Credentials stay in the existing secret store.

use std::time::Duration;

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Map, Value, json};

use crate::application::{
    ApplicationCatalog, ApplicationTransactions, Session, decode_catalog, decode_row, row_prefix,
    schema_key, table,
};
use crate::application_rows::{
    delete_row, int_cell, integer, managed_marker, nullable, nullable_cell, read_row, text,
    text_cell, validate_table, write_row,
};
use crate::backend::Transaction;
use crate::{Error, Result, Store, store_key};

const TABLE: &str = "peer_environment_links";
const COLUMNS: [&str; 10] = [
    "environment_id",
    "secret_name",
    "label",
    "urls_json",
    "access",
    "linked_at",
    "expires_at",
    "last_reached_at",
    "last_error",
    "fenced",
];
const MARKER: &str = "peer-links-v1";
const PREPARE_ROWS: usize = 32;
const READ_ROWS: usize = 256;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PeerRecord {
    pub environment_id: String,
    pub secret_name: String,
    pub label: String,
    pub urls_json: String,
    pub access: String,
    pub linked_at: String,
    pub expires_at: String,
    pub last_reached_at: Option<String>,
    pub last_error: Option<String>,
    pub fenced: i64,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum PeerOperation {
    List,
    Get {
        environment_id: String,
    },
    Upsert {
        row: PeerRecord,
    },
    Relabel {
        environment_id: String,
        label: String,
    },
    Delete {
        environment_id: String,
    },
    RecordOutcome {
        environment_id: String,
        error: Option<String>,
        reached_at: String,
    },
}

impl PeerOperation {
    pub fn is_write(&self) -> bool {
        !matches!(self, Self::List | Self::Get { .. })
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct PeerIndex {
    environment_id: String,
    label: String,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Preparation {
    version: u32,
    ready: bool,
    after: Option<String>,
    indexed_rows: u64,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct PreparationResult {
    pub prepared: bool,
    pub indexed_rows: u64,
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
        "application/generation/{}/peers/v1/{suffix}",
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

impl PeerRecord {
    fn from_row(row: &Map<String, Value>) -> Result<Self> {
        Ok(Self {
            environment_id: text(row, "environment_id")?,
            secret_name: text(row, "secret_name")?,
            label: text(row, "label")?,
            urls_json: text(row, "urls_json")?,
            access: text(row, "access")?,
            linked_at: text(row, "linked_at")?,
            expires_at: text(row, "expires_at")?,
            last_reached_at: nullable(row, "last_reached_at")?,
            last_error: nullable(row, "last_error")?,
            fenced: integer(row, "fenced")?,
        })
    }
    fn stored(&self) -> Map<String, Value> {
        Map::from_iter([
            ("environment_id".into(), text_cell(&self.environment_id)),
            ("secret_name".into(), text_cell(&self.secret_name)),
            ("label".into(), text_cell(&self.label)),
            ("urls_json".into(), text_cell(&self.urls_json)),
            ("access".into(), text_cell(&self.access)),
            ("linked_at".into(), text_cell(&self.linked_at)),
            ("expires_at".into(), text_cell(&self.expires_at)),
            (
                "last_reached_at".into(),
                nullable_cell(self.last_reached_at.as_deref()),
            ),
            (
                "last_error".into(),
                nullable_cell(self.last_error.as_deref()),
            ),
            ("fenced".into(), int_cell(self.fenced)),
        ])
    }
    fn index(&self) -> PeerIndex {
        PeerIndex {
            environment_id: self.environment_id.clone(),
            label: self.label.clone(),
        }
    }
}

impl PeerIndex {
    fn key(&self, catalog: &ApplicationCatalog) -> Vec<u8> {
        // Hex preserves SQLite BINARY ordering, including NUL and prefix labels.
        key(
            catalog,
            &format!("label/{}/{}", hex(&self.label), hex(&self.environment_id)),
        )
    }
}

async fn index_peer(
    tx: &mut Transaction,
    catalog: &ApplicationCatalog,
    row: &PeerRecord,
) -> Result<()> {
    let index = row.index();
    let encoded = index.key(catalog);
    if let Some(bytes) = tx.get(&encoded).await? {
        if decode::<PeerIndex>(&bytes)? != index {
            return Err(Error::IncompatibleStore);
        }
    }
    tx.put(encoded, encode(&index)?).await
}

async fn remove_index(
    tx: &mut Transaction,
    catalog: &ApplicationCatalog,
    row: &PeerRecord,
) -> Result<()> {
    let index = row.index();
    let encoded = index.key(catalog);
    if decode::<PeerIndex>(&tx.get(&encoded).await?.ok_or(Error::IncompatibleStore)?)? != index {
        return Err(Error::IncompatibleStore);
    }
    tx.delete(encoded).await?;
    Ok(())
}

async fn ready(session: &mut Session) -> Result<()> {
    validate_table(&session.catalog, TABLE, &COLUMNS, &["environment_id"])?;
    let marker = session
        .transaction
        .get(&managed_marker(&session.catalog, TABLE))
        .await?;
    if marker
        .as_deref()
        .map(decode::<String>)
        .transpose()?
        .as_deref()
        != Some(MARKER)
    {
        return Err(invalid("application peer links are not prepared"));
    }
    let state: Preparation = decode(
        &session
            .transaction
            .get(&key(&session.catalog, "preparation"))
            .await?
            .ok_or(Error::IncompatibleStore)?,
    )?;
    if state.version != 1 {
        return Err(Error::IncompatibleStore);
    }
    if !state.ready {
        return Err(invalid("application peer links are not prepared"));
    }
    Ok(())
}

async fn find(session: &mut Session, environment_id: &str) -> Result<Option<PeerRecord>> {
    read_row(
        &mut session.transaction,
        &session.catalog,
        TABLE,
        &[text_cell(environment_id)],
    )
    .await?
    .map(|row| PeerRecord::from_row(&row.row))
    .transpose()
}

async fn replace(session: &mut Session, row: &PeerRecord) -> Result<()> {
    if let Some(previous) = find(session, &row.environment_id).await? {
        remove_index(&mut session.transaction, &session.catalog, &previous).await?;
    }
    write_row(
        &mut session.transaction,
        &session.catalog,
        TABLE,
        row.stored(),
    )
    .await?;
    index_peer(&mut session.transaction, &session.catalog, row).await
}

async fn list(session: &mut Session) -> Result<Vec<PeerRecord>> {
    let prefix = key(&session.catalog, "label/");
    let end = prefix_end(&prefix)?;
    let mut start = prefix;
    let mut rows = Vec::new();
    loop {
        let page = session.transaction.scan(&start, &end, READ_ROWS).await?;
        let count = page.len();
        for (encoded, bytes) in page {
            let index: PeerIndex = decode(&bytes)?;
            if index.key(&session.catalog) != encoded {
                return Err(Error::IncompatibleStore);
            }
            let row = find(session, &index.environment_id)
                .await?
                .ok_or(Error::IncompatibleStore)?;
            if row.index() != index {
                return Err(Error::IncompatibleStore);
            }
            rows.push(row);
            start = encoded;
            start.push(0);
        }
        if count < READ_ROWS {
            break;
        }
    }
    Ok(rows)
}

impl Store {
    pub async fn prepare_application_peers(&self) -> Result<PreparationResult> {
        let mut limit = PREPARE_ROWS;
        loop {
            match self.prepare_application_peers_batch(limit).await {
                Err(Error::ResourceLimit(_)) if limit > 1 => limit = (limit / 2).max(1),
                result => return result,
            }
        }
    }
    async fn prepare_application_peers_batch(&self, limit: usize) -> Result<PreparationResult> {
        let mut tx = self
            .db
            .begin_reserved(Duration::from_secs(5), Duration::from_secs(300))
            .await?;
        let catalog = decode_catalog(
            &tx.get(&schema_key())
                .await?
                .ok_or_else(|| invalid("application schema is not initialized"))?,
        )?;
        validate_table(&catalog, TABLE, &COLUMNS, &["environment_id"])?;
        let marker = managed_marker(&catalog, TABLE);
        let state_key = key(&catalog, "preparation");
        let mut state = match tx.get(&state_key).await? {
            Some(bytes) => {
                let state: Preparation = decode(&bytes)?;
                if state.version != 1
                    || tx
                        .get(&marker)
                        .await?
                        .as_deref()
                        .map(decode::<String>)
                        .transpose()?
                        .as_deref()
                        != Some(MARKER)
                {
                    return Err(Error::IncompatibleStore);
                }
                state
            }
            None => {
                if tx.get(&marker).await?.is_some() {
                    return Err(Error::IncompatibleStore);
                }
                tx.put(marker, encode(&MARKER)?).await?;
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
        let prefix = row_prefix(&catalog.generation, TABLE)?;
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
        let page = tx.scan(&start, &prefix_end(&prefix)?, limit).await?;
        let count = page.len();
        for (encoded, bytes) in page {
            let source = decode_row(&catalog, table(&catalog, TABLE)?, &encoded, &bytes)?;
            index_peer(&mut tx, &catalog, &PeerRecord::from_row(&source.row)?).await?;
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
        state.ready = count < limit;
        tx.put(state_key, encode(&state)?).await?;
        tx.commit().await?;
        Ok(PreparationResult {
            prepared: state.ready,
            indexed_rows: state.indexed_rows,
        })
    }
}

impl ApplicationTransactions {
    pub async fn peers(&mut self, id: &str, request: PeerOperation) -> Result<Value> {
        let mut session = self.take(id)?;
        let result = async {
            if request.is_write() {
                session.transaction.require_write()?;
            }
            ready(&mut session).await?;
            match request {
                PeerOperation::List => {
                    serde_json::to_value(list(&mut session).await?).map_err(crate::json_error)
                }
                PeerOperation::Get { environment_id } => {
                    serde_json::to_value(find(&mut session, &environment_id).await?)
                        .map_err(crate::json_error)
                }
                PeerOperation::Upsert { row } => {
                    replace(&mut session, &row).await?;
                    Ok(json!({"written":true}))
                }
                PeerOperation::Relabel {
                    environment_id,
                    label,
                } => {
                    if let Some(mut row) = find(&mut session, &environment_id).await? {
                        row.label = label;
                        replace(&mut session, &row).await?;
                    }
                    Ok(json!({"written":true}))
                }
                PeerOperation::Delete { environment_id } => {
                    let Some(row) = find(&mut session, &environment_id).await? else {
                        return Ok(Value::Null);
                    };
                    remove_index(&mut session.transaction, &session.catalog, &row).await?;
                    delete_row(
                        &mut session.transaction,
                        &session.catalog,
                        TABLE,
                        &[text_cell(&environment_id)],
                    )
                    .await?;
                    Ok(Value::String(row.secret_name))
                }
                PeerOperation::RecordOutcome {
                    environment_id,
                    error,
                    reached_at,
                } => {
                    if let Some(mut row) = find(&mut session, &environment_id).await? {
                        if error.is_none() {
                            row.last_reached_at = Some(reached_at);
                        }
                        row.last_error = error;
                        replace(&mut session, &row).await?;
                    }
                    Ok(json!({"written":true}))
                }
            }
        }
        .await;
        self.finish(id, session, result).await
    }
}

#[cfg(test)]
#[path = "redb_peer_rows_tests.rs"]
mod tests;
