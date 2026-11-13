//! Migration 85's per-thread/run counts share the projection transaction.
use crate::application::ApplicationCatalog;
use crate::application_rows::{
    delete_row, int_cell, integer, nullable, read_row, text, text_cell, validate_table, write_row,
};
use crate::backend::Transaction;
use crate::{Error, Result};
use serde_json::{Map, Value};

pub(crate) const TABLE: &str = "orchestration_v2_projection_turn_item_counts";
pub(crate) fn validate(catalog: &ApplicationCatalog) -> Result<()> {
    validate_table(
        catalog,
        TABLE,
        &["thread_id", "run_key", "item_count"],
        &["thread_id", "run_key"],
    )
}
fn group(row: &Map<String, Value>) -> Result<(String, String)> {
    Ok((
        text(row, "thread_id")?,
        nullable(row, "run_id")?.unwrap_or_default(),
    ))
}
async fn adjust(
    tx: &mut Transaction,
    catalog: &ApplicationCatalog,
    group: &(String, String),
    delta: i64,
) -> Result<()> {
    let key = [text_cell(&group.0), text_cell(&group.1)];
    let count = match read_row(tx, catalog, TABLE, &key).await? {
        Some(row) => integer(&row.row, "item_count")?,
        None => 0,
    };
    let next = count
        .checked_add(delta)
        .filter(|count| *count >= 0)
        .ok_or(Error::IncompatibleStore)?;
    if next == 0 {
        delete_row(tx, catalog, TABLE, &key).await?;
    } else {
        write_row(
            tx,
            catalog,
            TABLE,
            Map::from_iter([
                ("thread_id".into(), key[0].clone()),
                ("run_key".into(), key[1].clone()),
                ("item_count".into(), int_cell(next)),
            ]),
        )
        .await?;
    }
    Ok(())
}
pub(crate) async fn replace(
    tx: &mut Transaction,
    catalog: &ApplicationCatalog,
    old: Option<&Map<String, Value>>,
    new: Option<&Map<String, Value>>,
) -> Result<()> {
    let old = old.map(group).transpose()?;
    let new = new.map(group).transpose()?;
    if old == new {
        return Ok(());
    }
    if let Some(old) = &old {
        adjust(tx, catalog, old, -1).await?;
    }
    if let Some(new) = &new {
        adjust(tx, catalog, new, 1).await?;
    }
    Ok(())
}
