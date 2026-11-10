//! Stable turn-item ordinals allocated inside the application's write transaction.

use std::time::Duration;

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Map, Value, json};

use crate::application::{
    ApplicationCatalog, ApplicationTransactions, Session, decode_catalog, decode_row, row_prefix,
    schema_key, table,
};
use crate::application_rows::{
    int_cell, integer, managed_marker, read_row, text, text_cell, validate_table, write_row,
};
use crate::backend::Transaction;
use crate::{Error, Result, Store, request_digest, store_key};

const POSITIONS: &str = "orchestration_v2_turn_item_positions";
const RUNS: &str = "orchestration_v2_projection_runs";
const COLUMNS: [&str; 3] = ["thread_id", "turn_item_id", "ordinal"];
const RUN_RANGE: i64 = 1_000_000;
const MAX_SAFE_INTEGER: i64 = (1_i64 << 53) - 1;
const PREPARE_ROWS: usize = 32;
const MARKER: &str = "turn-item-positions-v1";

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PositionAllocate {
    pub thread_id: String,
    pub turn_item_id: String,
    pub run_id: Option<String>,
    pub run_ordinal: Option<i64>,
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
struct Position {
    thread_id: String,
    turn_item_id: String,
    ordinal: i64,
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
        "application/generation/{}/positions/v1/{suffix}",
        catalog.generation
    ))
}

fn ordinal_key(catalog: &ApplicationCatalog, thread: &str, ordinal: i64) -> Vec<u8> {
    key(
        catalog,
        &format!("ordinal/{}/{ordinal}", request_digest(&json!([thread]))),
    )
}

fn maximum_key(catalog: &ApplicationCatalog, thread: &str, bucket: i64) -> Vec<u8> {
    key(
        catalog,
        &format!("maximum/{}/{bucket}", request_digest(&json!([thread]))),
    )
}

fn validate_tables(catalog: &ApplicationCatalog) -> Result<()> {
    validate_table(catalog, POSITIONS, &COLUMNS, &["thread_id", "turn_item_id"])?;
    let runs = table(catalog, RUNS)?;
    if runs.primary_key != ["run_id"]
        || !["thread_id", "ordinal"]
            .iter()
            .all(|name| runs.columns.iter().any(|column| column == name))
    {
        return Err(invalid(
            "turn-item positions require the application run schema",
        ));
    }
    Ok(())
}

impl Position {
    fn from_row(row: &Map<String, Value>) -> Result<Self> {
        let position = Self {
            thread_id: text(row, "thread_id")?,
            turn_item_id: text(row, "turn_item_id")?,
            ordinal: integer(row, "ordinal")?,
        };
        if !(-MAX_SAFE_INTEGER..=MAX_SAFE_INTEGER).contains(&position.ordinal) {
            return Err(Error::IncompatibleStore);
        }
        Ok(position)
    }

    fn row_key(&self) -> [Value; 2] {
        [text_cell(&self.thread_id), text_cell(&self.turn_item_id)]
    }

    fn stored(&self) -> Map<String, Value> {
        Map::from_iter([
            ("thread_id".into(), text_cell(&self.thread_id)),
            ("turn_item_id".into(), text_cell(&self.turn_item_id)),
            ("ordinal".into(), int_cell(self.ordinal)),
        ])
    }
}

async fn maximum(
    tx: &mut Transaction,
    catalog: &ApplicationCatalog,
    thread: &str,
    bucket: i64,
) -> Result<Option<Position>> {
    let value = tx.get(&maximum_key(catalog, thread, bucket)).await?;
    let position = value.as_deref().map(decode::<Position>).transpose()?;
    if position.as_ref().is_some_and(|value| {
        value.thread_id != thread || value.ordinal.div_euclid(RUN_RANGE) != bucket
    }) {
        return Err(Error::IncompatibleStore);
    }
    Ok(position)
}

async fn index_position(
    tx: &mut Transaction,
    catalog: &ApplicationCatalog,
    position: &Position,
) -> Result<()> {
    let unique = ordinal_key(catalog, &position.thread_id, position.ordinal);
    if let Some(bytes) = tx.get(&unique).await? {
        let existing: Position = decode(&bytes)?;
        if existing.thread_id != position.thread_id || existing.ordinal != position.ordinal {
            return Err(Error::IncompatibleStore);
        }
        if existing.turn_item_id != position.turn_item_id {
            return Err(Error::DuplicateRecord);
        }
    } else {
        tx.put(unique, encode(position)?).await?;
    }
    let bucket = position.ordinal.div_euclid(RUN_RANGE);
    if maximum(tx, catalog, &position.thread_id, bucket)
        .await?
        .is_none_or(|previous| previous.ordinal < position.ordinal)
    {
        tx.put(
            maximum_key(catalog, &position.thread_id, bucket),
            encode(position)?,
        )
        .await?;
    }
    Ok(())
}

async fn ready(session: &mut Session) -> Result<()> {
    let marker = session
        .transaction
        .get(&managed_marker(&session.catalog, POSITIONS))
        .await?;
    if marker
        .as_deref()
        .map(decode::<String>)
        .transpose()?
        .as_deref()
        != Some(MARKER)
    {
        return Err(invalid("application turn-item positions are not prepared"));
    }
    let state: Preparation = session
        .transaction
        .get(&key(&session.catalog, "preparation"))
        .await?
        .as_deref()
        .map(decode)
        .transpose()?
        .ok_or(Error::IncompatibleStore)?;
    if state.version != 1 {
        return Err(Error::IncompatibleStore);
    }
    if !state.ready {
        return Err(invalid("application turn-item positions are not prepared"));
    }
    Ok(())
}

impl Store {
    /// Each published batch blocks generic mutations and advances the durable index cursor.
    pub async fn prepare_application_positions(&self) -> Result<PreparationResult> {
        let mut limit = PREPARE_ROWS;
        loop {
            match self.prepare_application_positions_batch(limit).await {
                Err(Error::ResourceLimit(_)) if limit > 1 => limit = (limit / 2).max(1),
                result => return result,
            }
        }
    }

    async fn prepare_application_positions_batch(&self, limit: usize) -> Result<PreparationResult> {
        let mut tx = self
            .db
            .begin_reserved(Duration::from_secs(5), Duration::from_secs(300))
            .await?;
        let catalog = decode_catalog(
            &tx.get(&schema_key())
                .await?
                .ok_or_else(|| invalid("application schema is not initialized"))?,
        )?;
        validate_tables(&catalog)?;
        let marker = managed_marker(&catalog, POSITIONS);
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
        let prefix = row_prefix(&catalog.generation, POSITIONS)?;
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
            let source = decode_row(&catalog, table(&catalog, POSITIONS)?, &encoded, &bytes)?;
            index_position(&mut tx, &catalog, &Position::from_row(&source.row)?).await?;
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
    pub async fn positions_allocate(&mut self, id: &str, input: PositionAllocate) -> Result<i64> {
        let mut session = self.take(id)?;
        let result = async {
            session.transaction.require_write()?;
            ready(&mut session).await?;
            // Repeated streaming updates keep their first position, even after their run changes.
            if let Some(row) = read_row(
                &mut session.transaction,
                &session.catalog,
                POSITIONS,
                &[text_cell(&input.thread_id), text_cell(&input.turn_item_id)],
            )
            .await?
            {
                return Ok(Position::from_row(&row.row)?.ordinal);
            }
            if input
                .run_ordinal
                .is_some_and(|value| !(0..=MAX_SAFE_INTEGER).contains(&value))
            {
                return Err(invalid("run ordinal must be a nonnegative safe integer"));
            }
            let run_ordinal = match input.run_ordinal {
                Some(value) => Some(value),
                None => match input.run_id.as_deref() {
                    None => None,
                    Some(run_id) => match read_row(
                        &mut session.transaction,
                        &session.catalog,
                        RUNS,
                        &[text_cell(run_id)],
                    )
                    .await?
                    {
                        Some(run) if text(&run.row, "thread_id")? == input.thread_id => {
                            Some(integer(&run.row, "ordinal")?)
                        }
                        _ => None,
                    },
                },
            };
            if run_ordinal.is_some_and(|value| !(0..=MAX_SAFE_INTEGER).contains(&value)) {
                return Err(Error::IncompatibleStore);
            }
            let bucket = run_ordinal.unwrap_or(0);
            let lower = bucket
                .checked_mul(RUN_RANGE)
                .ok_or_else(|| invalid("turn-item ordinal exceeds integer range"))?;
            // Integer run ordinals make each SQL range exactly one disjoint million-item bucket.
            // The maximum cache includes imported rows and every allocation in this transaction.
            let previous = maximum(
                &mut session.transaction,
                &session.catalog,
                &input.thread_id,
                bucket,
            )
            .await?;
            let base = if let Some(previous) = previous {
                let row = read_row(
                    &mut session.transaction,
                    &session.catalog,
                    POSITIONS,
                    &previous.row_key(),
                )
                .await?
                .ok_or(Error::IncompatibleStore)?;
                if Position::from_row(&row.row)? != previous {
                    return Err(Error::IncompatibleStore);
                }
                previous.ordinal
            } else {
                lower
            };
            let ordinal = base
                .checked_add(1)
                .filter(|value| (-MAX_SAFE_INTEGER..=MAX_SAFE_INTEGER).contains(value))
                .ok_or_else(|| invalid("turn-item ordinal exceeds JavaScript integer range"))?;
            let position = Position {
                thread_id: input.thread_id,
                turn_item_id: input.turn_item_id,
                ordinal,
            };
            // Deliberately retain SQL's range overflow behavior; the next ordinal's uniqueness
            // decides whether a full bucket may allocate its first position in the next range.
            index_position(&mut session.transaction, &session.catalog, &position).await?;
            write_row(
                &mut session.transaction,
                &session.catalog,
                POSITIONS,
                position.stored(),
            )
            .await?;
            Ok(ordinal)
        }
        .await;
        self.finish(id, session, result).await
    }
}

#[cfg(test)]
#[path = "redb_turn_item_positions_tests.rs"]
mod tests;
