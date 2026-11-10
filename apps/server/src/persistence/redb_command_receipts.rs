//! Shared project/thread command receipts in the application's row transaction.

use std::time::Duration;

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Map, Value};

use crate::application::{
    ApplicationCatalog, ApplicationTransactions, Session, decode_catalog, decode_row, row_prefix,
    schema_key, table,
};
use crate::application_rows::{
    int_cell, integer, managed_marker, nullable, nullable_cell, read_row, text, text_cell,
    validate_table, write_row,
};
use crate::{Error, Result, Store, store_key};

const RECEIPTS: &str = "orchestration_command_receipts";
const COLUMNS: [&str; 8] = [
    "command_id",
    "aggregate_kind",
    "aggregate_id",
    "accepted_at",
    "result_sequence",
    "status",
    "error",
    "command_type",
];
const PREPARE_ROWS: usize = 32;
const MAX_SAFE_INTEGER: i64 = (1_i64 << 53) - 1;
const MARKER: &str = "command-receipts-v1";

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ReceiptRow {
    pub command_id: String,
    pub aggregate_kind: String,
    pub aggregate_id: String,
    pub command_type: String,
    pub accepted_at: String,
    pub result_sequence: i64,
    pub status: String,
    pub error: Option<String>,
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

fn preparation_key(catalog: &ApplicationCatalog) -> Vec<u8> {
    store_key(&format!(
        "application/generation/{}/receipts/v1/preparation",
        catalog.generation
    ))
}

impl ReceiptRow {
    fn validate(&self) -> Result<()> {
        if !matches!(self.aggregate_kind.as_str(), "project" | "thread")
            || !matches!(self.status.as_str(), "accepted" | "rejected")
            || !(0..=MAX_SAFE_INTEGER).contains(&self.result_sequence)
        {
            return Err(invalid("invalid application command receipt"));
        }
        Ok(())
    }

    fn from_row(row: &Map<String, Value>) -> Result<Self> {
        let receipt = Self {
            command_id: text(row, "command_id")?,
            aggregate_kind: text(row, "aggregate_kind")?,
            aggregate_id: text(row, "aggregate_id")?,
            command_type: text(row, "command_type")?,
            accepted_at: text(row, "accepted_at")?,
            result_sequence: integer(row, "result_sequence")?,
            status: text(row, "status")?,
            error: nullable(row, "error")?,
        };
        receipt.validate().map_err(|_| Error::IncompatibleStore)?;
        Ok(receipt)
    }

    fn stored(&self) -> Map<String, Value> {
        Map::from_iter([
            ("command_id".into(), text_cell(&self.command_id)),
            ("aggregate_kind".into(), text_cell(&self.aggregate_kind)),
            ("aggregate_id".into(), text_cell(&self.aggregate_id)),
            ("command_type".into(), text_cell(&self.command_type)),
            ("accepted_at".into(), text_cell(&self.accepted_at)),
            ("result_sequence".into(), int_cell(self.result_sequence)),
            ("status".into(), text_cell(&self.status)),
            ("error".into(), nullable_cell(self.error.as_deref())),
        ])
    }
}

async fn ready(session: &mut Session) -> Result<()> {
    let marker = session
        .transaction
        .get(&managed_marker(&session.catalog, RECEIPTS))
        .await?;
    if marker
        .as_deref()
        .map(decode::<String>)
        .transpose()?
        .as_deref()
        != Some(MARKER)
    {
        return Err(invalid("application command receipts are not prepared"));
    }
    let state: Preparation = session
        .transaction
        .get(&preparation_key(&session.catalog))
        .await?
        .as_deref()
        .map(decode)
        .transpose()?
        .ok_or(Error::IncompatibleStore)?;
    if state.version != 1 {
        return Err(Error::IncompatibleStore);
    }
    if !state.ready {
        return Err(invalid("application command receipts are not prepared"));
    }
    Ok(())
}

impl Store {
    /// Validate imported rows in bounded, resumable batches before accepting writes.
    pub async fn prepare_application_receipts(&self) -> Result<PreparationResult> {
        let mut limit = PREPARE_ROWS;
        loop {
            match self.prepare_application_receipts_batch(limit).await {
                Err(Error::ResourceLimit(_)) if limit > 1 => limit = (limit / 2).max(1),
                result => return result,
            }
        }
    }

    async fn prepare_application_receipts_batch(&self, limit: usize) -> Result<PreparationResult> {
        let mut tx = self
            .db
            .begin_reserved(Duration::from_secs(5), Duration::from_secs(300))
            .await?;
        let catalog = decode_catalog(
            &tx.get(&schema_key())
                .await?
                .ok_or_else(|| invalid("application schema is not initialized"))?,
        )?;
        validate_table(&catalog, RECEIPTS, &COLUMNS, &["command_id"])?;
        let marker = managed_marker(&catalog, RECEIPTS);
        let state_key = preparation_key(&catalog);
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
        let prefix = row_prefix(&catalog.generation, RECEIPTS)?;
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
        let mut end = prefix.clone();
        *end.last_mut().ok_or(Error::IncompatibleStore)? += 1;
        let rows = tx.scan(&start, &end, limit).await?;
        let count = rows.len();
        for (encoded, bytes) in rows {
            let source = decode_row(&catalog, table(&catalog, RECEIPTS)?, &encoded, &bytes)?;
            ReceiptRow::from_row(&source.row)?;
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
    pub async fn receipts_insert(&mut self, id: &str, receipt: ReceiptRow) -> Result<bool> {
        let mut session = self.take(id)?;
        let result = async {
            session.transaction.require_write()?;
            ready(&mut session).await?;
            receipt.validate()?;
            if read_row(
                &mut session.transaction,
                &session.catalog,
                RECEIPTS,
                &[text_cell(&receipt.command_id)],
            )
            .await?
            .is_some()
            {
                return Ok(false);
            }
            write_row(
                &mut session.transaction,
                &session.catalog,
                RECEIPTS,
                receipt.stored(),
            )
            .await?;
            Ok(true)
        }
        .await;
        self.finish(id, session, result).await
    }

    pub async fn receipts_upsert(&mut self, id: &str, receipt: ReceiptRow) -> Result<()> {
        let mut session = self.take(id)?;
        let result = async {
            session.transaction.require_write()?;
            ready(&mut session).await?;
            receipt.validate()?;
            write_row(
                &mut session.transaction,
                &session.catalog,
                RECEIPTS,
                receipt.stored(),
            )
            .await
        }
        .await;
        self.finish(id, session, result).await
    }

    pub async fn receipts_get(&mut self, id: &str, command_id: &str) -> Result<Option<ReceiptRow>> {
        let mut session = self.take(id)?;
        let result = async {
            ready(&mut session).await?;
            read_row(
                &mut session.transaction,
                &session.catalog,
                RECEIPTS,
                &[text_cell(command_id)],
            )
            .await?
            .map(|row| ReceiptRow::from_row(&row.row))
            .transpose()
        }
        .await;
        self.finish(id, session, result).await
    }
}

#[cfg(test)]
#[path = "redb_command_receipts_tests.rs"]
mod tests;
