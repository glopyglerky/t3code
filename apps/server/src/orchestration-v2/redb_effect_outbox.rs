//! The complete effect outbox over authoritative application rows.
//!
//! Scheduling and lifecycle transitions stay in the shared outbox engine. This
//! module preserves SQLite cells, imported FIFO order, and application ownership.

use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

use crate::application::{
    ApplicationCatalog, ApplicationRow, ApplicationTransactions, decode_catalog, decode_row,
    row_prefix, schema_key, table,
};
use crate::application_rows as cells;
use crate::backend::Transaction;
use crate::outbox::{self, Repository};
use crate::{Error, MAX_COUNTER, Result, Store, request_digest, store_key};

pub const TABLE: &str = "orchestration_v2_effect_outbox";
const COLUMNS: [&str; 14] = [
    "effect_id",
    "command_id",
    "thread_id",
    "effect_type",
    "payload_json",
    "status",
    "attempt_count",
    "available_at",
    "lease_owner",
    "lease_expires_at",
    "created_at",
    "updated_at",
    "completed_at",
    "last_error",
];
const PREPARE_ROWS: usize = 32;
const MARKER: &str = "outbox-index-v1";

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Preparation {
    version: u32,
    ready: bool,
    after: Option<String>,
    indexed_rows: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PreparationResult {
    pub prepared: bool,
    pub indexed_rows: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Ordinal {
    id: String,
    ordinal: u64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SourceWitness {
    table: String,
    key: Vec<Value>,
    ordinal: u64,
    #[serde(default, rename = "rowid")]
    _rowid: Option<String>,
}

pub(crate) fn index_key(catalog: &ApplicationCatalog, suffix: &str) -> Vec<u8> {
    store_key(&format!(
        "application/generation/{}/outbox/v1/{suffix}",
        catalog.generation
    ))
}

fn ordinal_key(catalog: &ApplicationCatalog, id: &str) -> Vec<u8> {
    index_key(catalog, &format!("ordinal/{}", request_digest(&json!(id))))
}

fn ordinal_owner_key(catalog: &ApplicationCatalog, ordinal: u64) -> Vec<u8> {
    index_key(catalog, &format!("ordinal-owner/{ordinal:016x}"))
}

fn decode<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    serde_json::from_slice(bytes).map_err(|_| Error::IncompatibleStore)
}

fn row_from_cells(row: &Map<String, Value>, ordinal: u64) -> Result<outbox::OutboxEffect> {
    let payload = cells::text(row, "payload_json")?;
    let request: Value = serde_json::from_str(&payload).map_err(|_| Error::IncompatibleStore)?;
    let request = outbox::normalize_request(&request).map_err(|_| Error::IncompatibleStore)?;
    if cells::text(row, "effect_type")? != outbox::request_type(&request)?.as_wire() {
        return Err(Error::IncompatibleStore);
    }
    let attempts = cells::integer(row, "attempt_count")?;
    if attempts < 0 || attempts as u64 > MAX_COUNTER {
        return Err(Error::IncompatibleStore);
    }
    Ok(outbox::OutboxEffect {
        id: cells::text(row, "effect_id")?,
        command_id: cells::text(row, "command_id")?,
        thread_id: cells::text(row, "thread_id")?,
        request,
        status: outbox::EffectStatus::from_wire(&cells::text(row, "status")?)
            .ok_or(Error::IncompatibleStore)?,
        attempt_count: attempts as u64,
        available_at: cells::text(row, "available_at")?,
        lease_owner: cells::nullable(row, "lease_owner")?,
        // Production does not expire this lease. Retain even historical opaque
        // values because lifecycle statements never rewrote this column.
        lease_expires_at: cells::nullable(row, "lease_expires_at")?,
        created_at: cells::text(row, "created_at")?,
        updated_at: cells::text(row, "updated_at")?,
        completed_at: cells::nullable(row, "completed_at")?,
        last_error: cells::nullable(row, "last_error")?,
        insertion_ordinal: ordinal,
    })
}

async fn read_ordinal(tx: &mut Transaction, catalog: &ApplicationCatalog, id: &str) -> Result<u64> {
    let bytes = tx
        .get(&ordinal_key(catalog, id))
        .await?
        .ok_or(Error::IncompatibleStore)?;
    let witness: Ordinal = decode(&bytes)?;
    if witness.id != id || witness.ordinal == 0 || witness.ordinal > MAX_COUNTER {
        return Err(Error::IncompatibleStore);
    }
    let owner = tx
        .get(&ordinal_owner_key(catalog, witness.ordinal))
        .await?
        .ok_or(Error::IncompatibleStore)?;
    if decode::<String>(&owner)? != id {
        return Err(Error::IncompatibleStore);
    }
    Ok(witness.ordinal)
}

async fn record_ordinal(
    tx: &mut Transaction,
    catalog: &ApplicationCatalog,
    id: &str,
    ordinal: u64,
) -> Result<()> {
    if ordinal == 0 || ordinal > MAX_COUNTER {
        return Err(Error::IncompatibleStore);
    }
    let key = ordinal_key(catalog, id);
    if tx.get(&key).await?.is_some() {
        if read_ordinal(tx, catalog, id).await? != ordinal {
            return Err(Error::IncompatibleStore);
        }
        return Ok(());
    }
    tx.insert(ordinal_owner_key(catalog, ordinal), crate::encode(&id)?)
        .await?;
    tx.insert(
        key,
        crate::encode(&Ordinal {
            id: id.into(),
            ordinal,
        })?,
    )
    .await
}

pub(crate) async fn load_row(
    tx: &mut Transaction,
    catalog: &ApplicationCatalog,
    id: &str,
) -> Result<Option<(outbox::OutboxEffect, usize)>> {
    let Some(stored) = cells::read_row(tx, catalog, TABLE, &[cells::text_cell(id)]).await? else {
        if tx.get(&ordinal_key(catalog, id)).await?.is_some() {
            return Err(Error::IncompatibleStore);
        }
        return Ok(None);
    };
    let ordinal = read_ordinal(tx, catalog, id).await?;
    let row = row_from_cells(&stored.row, ordinal)?;
    Ok(Some((row, crate::encode(&stored)?.len())))
}

pub(crate) async fn save_row(
    tx: &mut Transaction,
    catalog: &ApplicationCatalog,
    next: &outbox::OutboxEffect,
) -> Result<()> {
    let existing = cells::read_row(tx, catalog, TABLE, &[cells::text_cell(&next.id)]).await?;
    let mut row = match existing {
        Some(stored) => {
            let ordinal = read_ordinal(tx, catalog, &next.id).await?;
            let previous = row_from_cells(&stored.row, ordinal)?;
            if previous.id != next.id
                || previous.command_id != next.command_id
                || previous.thread_id != next.thread_id
                || previous.request != next.request
                || previous.created_at != next.created_at
                || previous.lease_expires_at != next.lease_expires_at
                || previous.insertion_ordinal != next.insertion_ordinal
            {
                return Err(Error::IncompatibleStore);
            }
            crate::application_auxiliary_reads::replace_outbox(
                tx,
                catalog,
                Some(&previous),
                Some(next),
            )
            .await?;
            stored.row
        }
        None => {
            if tx.get(&ordinal_key(catalog, &next.id)).await?.is_some() {
                return Err(Error::IncompatibleStore);
            }
            crate::application_auxiliary_reads::replace_outbox(tx, catalog, None, Some(next))
                .await?;
            let mut row = Map::new();
            for (name, value) in [
                ("effect_id", next.id.as_str()),
                ("command_id", next.command_id.as_str()),
                ("thread_id", next.thread_id.as_str()),
                (
                    "effect_type",
                    outbox::request_type(&next.request)?.as_wire(),
                ),
                ("created_at", next.created_at.as_str()),
            ] {
                row.insert(name.into(), cells::text_cell(value));
            }
            row.insert(
                "payload_json".into(),
                cells::text_cell(&serde_json::to_string(&next.request).map_err(crate::json_error)?),
            );
            row.insert(
                "lease_expires_at".into(),
                cells::nullable_cell(next.lease_expires_at.as_deref()),
            );
            row
        }
    };
    row.insert("status".into(), cells::text_cell(next.status.as_wire()));
    row.insert(
        "attempt_count".into(),
        cells::int_cell(i64::try_from(next.attempt_count).map_err(|_| Error::IncompatibleStore)?),
    );
    row.insert("available_at".into(), cells::text_cell(&next.available_at));
    row.insert(
        "lease_owner".into(),
        cells::nullable_cell(next.lease_owner.as_deref()),
    );
    row.insert("updated_at".into(), cells::text_cell(&next.updated_at));
    row.insert(
        "completed_at".into(),
        cells::nullable_cell(next.completed_at.as_deref()),
    );
    row.insert(
        "last_error".into(),
        cells::nullable_cell(next.last_error.as_deref()),
    );
    cells::write_row(tx, catalog, TABLE, row).await?;
    record_ordinal(tx, catalog, &next.id, next.insertion_ordinal).await
}

pub(crate) async fn delete_row(
    tx: &mut Transaction,
    catalog: &ApplicationCatalog,
    row: &outbox::OutboxEffect,
) -> Result<()> {
    if read_ordinal(tx, catalog, &row.id).await? != row.insertion_ordinal
        || !cells::delete_row(tx, catalog, TABLE, &[cells::text_cell(&row.id)]).await?
    {
        return Err(Error::IncompatibleStore);
    }
    crate::application_auxiliary_reads::replace_outbox(tx, catalog, Some(row), None).await?;
    tx.delete(ordinal_key(catalog, &row.id)).await?;
    tx.delete(ordinal_owner_key(catalog, row.insertion_ordinal))
        .await
}

async fn source_ordinal(
    tx: &mut Transaction,
    catalog: &ApplicationCatalog,
    row: &ApplicationRow,
) -> Result<u64> {
    // Export records are ordered by SQLite rowid, independently of effect IDs.
    // Native-created tables must start empty; a generic put has no FIFO witness.
    if catalog.generation == "native" {
        return Err(Error::IncompatibleStore);
    }
    let key = store_key(&format!(
        "application/generation/{}/metadata/source/{}",
        catalog.generation,
        request_digest(&json!([TABLE, row.key]))
    ));
    let bytes = tx.get(&key).await?.ok_or(Error::IncompatibleStore)?;
    let witness: SourceWitness = decode(&bytes)?;
    if witness.table != TABLE || witness.key != row.key || witness.ordinal >= MAX_COUNTER {
        return Err(Error::IncompatibleStore);
    }
    Ok(witness.ordinal + 1)
}

pub(crate) async fn ready(tx: &mut Transaction, catalog: &ApplicationCatalog) -> Result<()> {
    let marker = tx
        .get(&cells::managed_marker(catalog, TABLE))
        .await?
        .ok_or(Error::IncompatibleStore)?;
    let state = tx
        .get(&index_key(catalog, "preparation"))
        .await?
        .ok_or(Error::IncompatibleStore)?;
    let state: Preparation = decode(&state)?;
    if decode::<String>(&marker)? != MARKER || state.version != 1 || !state.ready {
        return Err(Error::IncompatibleStore);
    }
    Ok(())
}

fn valid_cursor(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

impl Store {
    pub async fn prepare_application_outbox(&self) -> Result<PreparationResult> {
        let mut limit = PREPARE_ROWS;
        loop {
            match self.prepare_application_outbox_batch(limit).await {
                Err(Error::ResourceLimit(_)) if limit > 1 => limit = (limit / 2).max(1),
                result => return result,
            }
        }
    }

    async fn prepare_application_outbox_batch(&self, limit: usize) -> Result<PreparationResult> {
        let mut tx = self
            .db
            .begin_reserved(Duration::from_secs(5), Duration::from_secs(300))
            .await?;
        let catalog = decode_catalog(
            &tx.get(&schema_key())
                .await?
                .ok_or(Error::IncompatibleStore)?,
        )?;
        cells::validate_table(&catalog, TABLE, &COLUMNS, &["effect_id"])?;
        let marker_key = cells::managed_marker(&catalog, TABLE);
        let state_key = index_key(&catalog, "preparation");
        let mut state = match tx.get(&state_key).await? {
            Some(bytes) => {
                let state: Preparation = decode(&bytes)?;
                let marker = tx.get(&marker_key).await?.ok_or(Error::IncompatibleStore)?;
                if state.version != 1 || decode::<String>(&marker)? != MARKER {
                    return Err(Error::IncompatibleStore);
                }
                state
            }
            None => {
                if tx.get(&marker_key).await?.is_some() {
                    return Err(Error::IncompatibleStore);
                }
                tx.put(marker_key, crate::encode(&MARKER)?).await?;
                tx.put(index_key(&catalog, "counter"), crate::encode(&0u64)?)
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
        let prefix = row_prefix(&catalog.generation, TABLE)?;
        let mut start = prefix.clone();
        if let Some(after) = &state.after {
            if !valid_cursor(after) {
                return Err(Error::IncompatibleStore);
            }
            start.extend_from_slice(after.as_bytes());
            start.push(0);
        }
        let mut end = prefix.clone();
        end.push(255);
        let entries = tx.scan(&start, &end, limit).await?;
        let count = entries.len();
        for (encoded_key, bytes) in entries {
            let source = decode_row(&catalog, table(&catalog, TABLE)?, &encoded_key, &bytes)?;
            let ordinal = source_ordinal(&mut tx, &catalog, &source).await?;
            let row = row_from_cells(&source.row, ordinal)?;
            record_ordinal(&mut tx, &catalog, &row.id, ordinal).await?;
            outbox::index_imported(&mut tx, &catalog, &row).await?;
            let suffix = encoded_key
                .strip_prefix(prefix.as_slice())
                .ok_or(Error::IncompatibleStore)?;
            let suffix = std::str::from_utf8(suffix).map_err(|_| Error::IncompatibleStore)?;
            if !valid_cursor(suffix) {
                return Err(Error::IncompatibleStore);
            }
            state.after = Some(suffix.into());
            state.indexed_rows = state
                .indexed_rows
                .checked_add(1)
                .filter(|n| *n <= MAX_COUNTER)
                .ok_or(Error::IncompatibleStore)?;
        }
        state.ready = count < limit;
        tx.put(state_key, crate::encode(&state)?).await?;
        tx.commit().await?;
        Ok(PreparationResult {
            prepared: state.ready,
            indexed_rows: state.indexed_rows,
        })
    }
}

// These methods never open or commit another transaction. The shared application
// owner controls commit, savepoints, cancellation, and uncertain outcomes.
macro_rules! operation {
    ($name:ident, $stage:ident, $write:literal, ($($arg:ident: $ty:ty),*) -> $output:ty) => {
        impl ApplicationTransactions {
            pub async fn $name(&mut self, id: &str, $($arg: $ty),*) -> Result<$output> {
                let mut session = self.take(id)?;
                let result = async {
                    if $write { session.transaction.require_write()?; }
                    ready(&mut session.transaction, &session.catalog).await?;
                    let result = outbox::$stage(&mut session.transaction, Repository::Application(&session.catalog), $($arg),*).await?;
                    crate::validate_response_bytes(&result)?;
                    Ok(result)
                }.await;
                self.finish(id, session, result).await
            }
        }
    };
}

operation!(outbox_enqueue, stage_enqueue_in, true, (effects: &[outbox::EnqueueEffect], now: &str) -> usize);
operation!(outbox_get, stage_get_in, false, (effect_id: &str) -> Option<outbox::OutboxEffect>);
operation!(outbox_list, stage_list_in, false, (command_id: &str) -> Vec<outbox::OutboxEffect>);
operation!(outbox_cancel, stage_cancel_unsettled_in, true, (input: &outbox::CancelUnsettled) -> Vec<String>);
operation!(outbox_reconcile, stage_reconcile_in, true, (input: &outbox::Reconcile) -> outbox::ReconcileOutcome);
operation!(outbox_claim_once, stage_claim_once_in, true, (input: &outbox::ClaimOnce) -> outbox::ClaimOutcome);
operation!(outbox_claim, stage_claim_in, true, (input: &outbox::Claim) -> Option<outbox::OutboxEffect>);
operation!(outbox_next_claimable_at, stage_next_in, false, () -> Option<String>);
operation!(outbox_next_claimable_at_limited, stage_next_limited_in, false, (max_running_title_generations: Option<f64>) -> Option<String>);
operation!(outbox_succeed, stage_succeed_in, true, (input: &outbox::Settle) -> bool);
operation!(outbox_retry, stage_retry_in, true, (input: &outbox::Retry) -> bool);
operation!(outbox_fail, stage_fail_in, true, (input: &outbox::Failure) -> bool);
operation!(outbox_prune, stage_prune_in, true, (input: &outbox::Prune) -> usize);

#[cfg(test)]
#[path = "redb_effect_outbox_tests.rs"]
mod tests;
