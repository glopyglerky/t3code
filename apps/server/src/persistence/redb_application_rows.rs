//! Exact SQLite cells and witnessed application rows shared by native repositories.

use std::collections::BTreeSet;

use serde_json::{Map, Value, json};

use crate::application::{
    ApplicationCatalog, ApplicationRow, decode_row, encode_row, row_key, table, validate_row,
};
use crate::backend::Transaction;
use crate::{Error, Result, store_key};

pub(crate) fn text_cell(value: &str) -> Value {
    json!({"type":"text","value":value})
}

pub(crate) fn int_cell(value: i64) -> Value {
    json!({"type":"integer","value":value.to_string()})
}

pub(crate) fn nullable_cell(value: Option<&str>) -> Value {
    value.map_or_else(|| json!({"type":"null"}), text_cell)
}

pub(crate) fn text(row: &Map<String, Value>, name: &str) -> Result<String> {
    let cell = row.get(name).ok_or(Error::IncompatibleStore)?;
    if cell.as_object().is_none_or(|fields| fields.len() != 2)
        || cell.get("type").and_then(Value::as_str) != Some("text")
    {
        return Err(Error::IncompatibleStore);
    }
    cell.get("value")
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or(Error::IncompatibleStore)
}

pub(crate) fn nullable(row: &Map<String, Value>, name: &str) -> Result<Option<String>> {
    if row.get(name) == Some(&json!({"type":"null"})) {
        Ok(None)
    } else {
        text(row, name).map(Some)
    }
}

pub(crate) fn integer(row: &Map<String, Value>, name: &str) -> Result<i64> {
    let cell = row.get(name).ok_or(Error::IncompatibleStore)?;
    if cell.as_object().is_none_or(|fields| fields.len() != 2)
        || cell.get("type").and_then(Value::as_str) != Some("integer")
    {
        return Err(Error::IncompatibleStore);
    }
    let encoded = cell
        .get("value")
        .and_then(Value::as_str)
        .ok_or(Error::IncompatibleStore)?;
    let value: i64 = encoded.parse().map_err(|_| Error::IncompatibleStore)?;
    if value.to_string() != encoded {
        return Err(Error::IncompatibleStore);
    }
    Ok(value)
}

pub(crate) fn validate_table(
    catalog: &ApplicationCatalog,
    name: &str,
    columns: &[&str],
    primary: &[&str],
) -> Result<()> {
    let definition = table(catalog, name)?;
    let actual: BTreeSet<&str> = definition.columns.iter().map(String::as_str).collect();
    if actual != columns.iter().copied().collect()
        || definition.columns.len() != columns.len()
        || definition.primary_key != primary
    {
        return Err(Error::IncompatibleStore);
    }
    Ok(())
}

pub(crate) fn managed_marker(catalog: &ApplicationCatalog, name: &str) -> Vec<u8> {
    let name: String = name.bytes().map(|byte| format!("{byte:02x}")).collect();
    store_key(&format!(
        "application/generation/{}/managed/{name}",
        catalog.generation
    ))
}

pub(crate) async fn read_row(
    transaction: &mut Transaction,
    catalog: &ApplicationCatalog,
    name: &str,
    key: &[Value],
) -> Result<Option<ApplicationRow>> {
    let definition = table(catalog, name)?;
    let encoded_key = row_key(&catalog.generation, name, key)?;
    let Some(bytes) = transaction.get(&encoded_key).await? else {
        return Ok(None);
    };
    let row = decode_row(catalog, definition, &encoded_key, &bytes)?;
    if row.key != key {
        return Err(Error::IncompatibleStore);
    }
    Ok(Some(row))
}

pub(crate) async fn write_row(
    transaction: &mut Transaction,
    catalog: &ApplicationCatalog,
    name: &str,
    row: Map<String, Value>,
) -> Result<()> {
    transaction.require_write()?;
    let definition = table(catalog, name)?;
    let key: Vec<Value> = definition
        .primary_key
        .iter()
        .map(|column| row.get(column).cloned().ok_or(Error::IncompatibleStore))
        .collect::<Result<_>>()?;
    validate_row(definition, &key, &row)?;
    // Check the existing witness before replacing a row addressed by a hash.
    read_row(transaction, catalog, name, &key).await?;
    transaction
        .put(
            row_key(&catalog.generation, name, &key)?,
            encode_row(&ApplicationRow { key, row })?,
        )
        .await
}

pub(crate) async fn delete_row(
    transaction: &mut Transaction,
    catalog: &ApplicationCatalog,
    name: &str,
    key: &[Value],
) -> Result<bool> {
    transaction.require_write()?;
    if read_row(transaction, catalog, name, key).await?.is_none() {
        return Ok(false);
    }
    transaction
        .delete(row_key(&catalog.generation, name, key)?)
        .await?;
    Ok(true)
}
