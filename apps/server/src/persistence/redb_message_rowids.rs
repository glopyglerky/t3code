//! SQLite-compatible hidden message rowids, including signed imported gaps.
//! This is search order, not the message primary key or source export ordinal.
use std::hash::BuildHasher;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

use crate::application::{
    ApplicationCatalog, decode_catalog, decode_row, row_prefix, schema_key, table,
};
use crate::application_rows::{read_row, text, text_cell};
use crate::backend::Transaction;
use crate::{Error, Result, Store, request_digest, store_key};

const TABLE: &str = "orchestration_v2_projection_messages";

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct State {
    after: Option<String>,
    indexed_rows: u64,
    ready: bool,
}

#[derive(Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
struct Identity {
    message_id: String,
    rowid: i64,
}

#[derive(Serialize)]
pub struct PreparationResult {
    pub prepared: bool,
    pub indexed_rows: u64,
}

fn invalid(message: &str) -> Error {
    Error::Invalid(format!("Message rowids: {message}"))
}
fn encode(value: &impl Serialize) -> Result<Vec<u8>> {
    serde_json::to_vec(value).map_err(|_| Error::IncompatibleStore)
}
fn decode<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    serde_json::from_slice(bytes).map_err(|_| Error::IncompatibleStore)
}
fn location(catalog: &ApplicationCatalog, suffix: &str) -> Vec<u8> {
    store_key(&format!(
        "application/generation/{}/message-rowids/v1/{suffix}",
        catalog.generation
    ))
}
fn id_key(catalog: &ApplicationCatalog, message_id: &str) -> Vec<u8> {
    location(
        catalog,
        &format!("identity/{}", request_digest(&json!(message_id))),
    )
}
fn ordinal_key(catalog: &ApplicationCatalog, rowid: i64) -> Vec<u8> {
    let descending = !((rowid as u64) ^ (1 << 63));
    location(catalog, &format!("rows/{descending:016x}"))
}
fn prefix_end(prefix: &[u8]) -> Vec<u8> {
    let mut end = prefix.to_vec();
    *end.last_mut().expect("nonempty storage prefix") += 1;
    end
}
async fn state(tx: &mut Transaction, catalog: &ApplicationCatalog) -> Result<Option<State>> {
    tx.get(&location(catalog, "preparation"))
        .await?
        .as_deref()
        .map(decode)
        .transpose()
}
async fn ready(tx: &mut Transaction, catalog: &ApplicationCatalog) -> Result<()> {
    if !state(tx, catalog).await?.is_some_and(|state| state.ready) {
        return Err(invalid("search rowids are not prepared"));
    }
    Ok(())
}
async fn insert(tx: &mut Transaction, catalog: &ApplicationCatalog, value: Identity) -> Result<()> {
    let bytes = encode(&value)?;
    for key in [
        id_key(catalog, &value.message_id),
        ordinal_key(catalog, value.rowid),
    ] {
        if let Some(previous) = tx.get(&key).await? {
            if decode::<Identity>(&previous)? != value {
                return Err(Error::IncompatibleStore);
            }
        }
        tx.put(key, bytes.clone()).await?;
    }
    Ok(())
}

pub(crate) async fn rowid(
    tx: &mut Transaction,
    catalog: &ApplicationCatalog,
    message_id: &str,
) -> Result<i64> {
    ready(tx, catalog).await?;
    let value: Identity = decode(
        &tx.get(&id_key(catalog, message_id))
            .await?
            .ok_or(Error::IncompatibleStore)?,
    )?;
    if value.message_id != message_id {
        return Err(Error::IncompatibleStore);
    }
    if tx
        .get(&ordinal_key(catalog, value.rowid))
        .await?
        .as_deref()
        .map(decode::<Identity>)
        .transpose()?
        != Some(value.clone())
    {
        return Err(Error::IncompatibleStore);
    }
    Ok(value.rowid)
}

/// Migration can run before an imported candidate has completed preparation.
/// Native writers require ready identities; otherwise only the immutable source
/// witness can establish SQLite's hidden rowid, never a new allocation.
pub(crate) async fn migration_rowid(
    tx: &mut Transaction,
    catalog: &ApplicationCatalog,
    row: &crate::application::ApplicationRow,
) -> Result<i64> {
    let message_id = text(&row.row, "message_id")?;
    if state(tx, catalog).await?.is_some_and(|state| state.ready) {
        return rowid(tx, catalog, &message_id).await;
    }
    let witness_key = store_key(&format!(
        "application/generation/{}/metadata/source/{}",
        catalog.generation,
        request_digest(&json!([TABLE, row.key]))
    ));
    let witness: Value = decode(
        &tx.get(&witness_key)
            .await?
            .ok_or_else(|| invalid("migration requires a witnessed hidden message rowid"))?,
    )?;
    let raw = witness["rowid"].as_str().ok_or(Error::IncompatibleStore)?;
    let rowid: i64 = raw.parse().map_err(|_| Error::IncompatibleStore)?;
    if rowid.to_string() != raw || witness["table"] != TABLE || witness["key"] != json!(row.key) {
        return Err(Error::IncompatibleStore);
    }
    let identity = Identity { message_id, rowid };
    let existing_id = tx.get(&id_key(catalog, &identity.message_id)).await?;
    let existing_ordinal = tx.get(&ordinal_key(catalog, rowid)).await?;
    for existing in [&existing_id, &existing_ordinal].into_iter().flatten() {
        if decode::<Identity>(existing)? != identity {
            return Err(Error::IncompatibleStore);
        }
    }
    if existing_id.is_some() != existing_ordinal.is_some() {
        return Err(Error::IncompatibleStore);
    }
    Ok(rowid)
}

pub(crate) async fn identity(
    tx: &mut Transaction,
    catalog: &ApplicationCatalog,
    rowid: i64,
) -> Result<Option<String>> {
    ready(tx, catalog).await?;
    let Some(bytes) = tx.get(&ordinal_key(catalog, rowid)).await? else {
        return Ok(None);
    };
    let value: Identity = decode(&bytes)?;
    if value.rowid != rowid
        || tx
            .get(&id_key(catalog, &value.message_id))
            .await?
            .as_deref()
            .map(decode::<Identity>)
            .transpose()?
            != Some(value.clone())
    {
        return Err(Error::IncompatibleStore);
    }
    if read_row(tx, catalog, TABLE, &[text_cell(&value.message_id)])
        .await?
        .is_none()
    {
        return Err(Error::IncompatibleStore);
    }
    Ok(Some(value.message_id))
}

/// Descending numeric order. `after` excludes that rowid and all newer rows.
pub(crate) async fn newest(
    tx: &mut Transaction,
    catalog: &ApplicationCatalog,
    after: Option<i64>,
    limit: usize,
) -> Result<Vec<(i64, String)>> {
    ready(tx, catalog).await?;
    if limit == 0 || limit > 4096 {
        return Err(invalid("invalid rowid page limit"));
    }
    let prefix = location(catalog, "rows/");
    let start = after
        .map(|rowid| {
            let mut key = ordinal_key(catalog, rowid);
            key.push(0);
            key
        })
        .unwrap_or_else(|| prefix.clone());
    let mut result = Vec::new();
    for (key, bytes) in tx.scan(&start, &prefix_end(&prefix), limit).await? {
        let value: Identity = decode(&bytes)?;
        if key != ordinal_key(catalog, value.rowid)
            || after.is_some_and(|after| value.rowid >= after)
        {
            return Err(Error::IncompatibleStore);
        }
        result.push((value.rowid, value.message_id));
    }
    Ok(result)
}

/// Called within the projection writer, before the source row changes. A legacy
/// component that has not selected search leaves no fabricated rowid metadata.
pub(crate) async fn replace(
    tx: &mut Transaction,
    catalog: &ApplicationCatalog,
    old: Option<&Map<String, Value>>,
    new: Option<&Map<String, Value>>,
) -> Result<Option<i64>> {
    let Some(state) = state(tx, catalog).await? else {
        return Ok(None);
    };
    if !state.ready {
        return Err(invalid("message mutation during search preparation"));
    }
    if let Some(old) = old {
        let id = text(old, "message_id")?;
        let found = rowid(tx, catalog, &id).await?;
        if let Some(new) = new {
            if text(new, "message_id")? != id {
                return Err(invalid("message identity cannot change"));
            }
        } else {
            tx.delete(id_key(catalog, &id)).await?;
            tx.delete(ordinal_key(catalog, found)).await?;
        }
        return Ok(Some(found));
    }
    let Some(new) = new else {
        return Ok(None);
    };
    let id = text(new, "message_id")?;
    let maximum = newest(tx, catalog, None, 1)
        .await?
        .first()
        .map(|(rowid, _)| *rowid);
    let allocated = match maximum {
        None => 1,
        Some(value) if value < i64::MAX => value + 1,
        Some(_) => {
            // SQLite likewise chooses an unused positive random rowid once
            // the signed maximum is present. The exact random value is not a
            // stable contract, but the 100-attempt collision bound is.
            let mut found = None;
            for attempt in 0..100 {
                let value = (std::collections::hash_map::RandomState::new()
                    .hash_one((&id, attempt))
                    & i64::MAX as u64) as i64;
                if value > 0 && tx.get(&ordinal_key(catalog, value)).await?.is_none() {
                    found = Some(value);
                    break;
                }
            }
            found.ok_or_else(|| invalid("no available random message rowid"))?
        }
    };
    insert(
        tx,
        catalog,
        Identity {
            message_id: id,
            rowid: allocated,
        },
    )
    .await?;
    Ok(Some(allocated))
}

impl Store {
    pub async fn prepare_application_message_rowids(&self) -> Result<PreparationResult> {
        let base = self.prepare_application_projections().await?;
        if !base.prepared {
            return Ok(PreparationResult {
                prepared: false,
                indexed_rows: 0,
            });
        }
        let mut limit = 64;
        loop {
            match self.prepare_message_rowids_batch(limit).await {
                Err(Error::ResourceLimit(_)) if limit > 1 => limit = (limit / 2).max(1),
                result => return result,
            }
        }
    }
    async fn prepare_message_rowids_batch(&self, limit: usize) -> Result<PreparationResult> {
        let mut tx = self
            .db
            .begin_reserved(Duration::from_secs(5), Duration::from_secs(300))
            .await?;
        let catalog = decode_catalog(
            &tx.get(&schema_key())
                .await?
                .ok_or(Error::IncompatibleStore)?,
        )?;
        let mut state = state(&mut tx, &catalog).await?.unwrap_or(State {
            after: None,
            indexed_rows: 0,
            ready: false,
        });
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
        let page = tx.scan(&start, &prefix_end(&prefix), limit).await?;
        let count = page.len();
        for (key, bytes) in page {
            let source = decode_row(&catalog, table(&catalog, TABLE)?, &key, &bytes)?;
            let witness_key = store_key(&format!(
                "application/generation/{}/metadata/source/{}",
                catalog.generation,
                request_digest(&json!([TABLE, source.key]))
            ));
            let witness:Value=decode(&tx.get(&witness_key).await?.ok_or_else(||invalid("hidden rowid unavailable; import a version 2 SQLite export before selecting native search"))?)?;
            let raw = witness["rowid"]
                .as_str()
                .ok_or_else(|| invalid("source export omits hidden message rowids"))?;
            let rowid: i64 = raw.parse().map_err(|_| Error::IncompatibleStore)?;
            if rowid.to_string() != raw
                || witness["table"] != TABLE
                || witness["key"] != json!(source.key)
            {
                return Err(Error::IncompatibleStore);
            }
            insert(
                &mut tx,
                &catalog,
                Identity {
                    message_id: text(&source.row, "message_id")?,
                    rowid,
                },
            )
            .await?;
            state.after = Some(
                std::str::from_utf8(
                    key.strip_prefix(prefix.as_slice())
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
        tx.put(location(&catalog, "preparation"), encode(&state)?)
            .await?;
        tx.commit().await?;
        Ok(PreparationResult {
            prepared: state.ready,
            indexed_rows: state.indexed_rows,
        })
    }
}
