//! Publish validated SQLite rows into the native application table namespace.
//! This is data hydration, not a replacement for the application's SQL repositories.
use std::io::{self, BufReader, Cursor, Read};
use std::path::Path;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use tokio::runtime::Handle;
use tokio::sync::mpsc;

use crate::application::{
    ApplicationCatalog, ApplicationRow, ApplicationSchema, ApplicationTable, encode_catalog,
    encode_row, row_key, schema_key, validate_row, validate_schema,
};
use crate::sqlite_import::{ChunkRef, ExportSummary, ImportLimits, validate_reader};
use crate::{Error, MAX_BATCH_BYTES, Result, Store, request_digest, store_key};

const STAGE_BYTES: usize = 2 * 1024 * 1024;
const STAGE_RECORDS: usize = 1024;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApplicationImportReceipt {
    pub generation: String,
    pub summary: ExportSummary,
    pub excluded_derived_tables: Vec<String>,
    pub replayed: bool,
}

fn invalid(message: &str) -> Error {
    Error::Invalid(format!("Application import: {message}"))
}

fn encode(value: &impl Serialize) -> Result<Vec<u8>> {
    serde_json::to_vec(value).map_err(|_| Error::IncompatibleStore)
}

fn receipt_key() -> Vec<u8> {
    store_key("application/import/active")
}

fn metadata_key(generation: &str, name: &str) -> Vec<u8> {
    store_key(&format!(
        "application/generation/{generation}/metadata/{name}"
    ))
}

// This reader runs only on a blocking worker. It obtains bounded, hash-checked,
// immutable archive chunks, so changing the source export cannot poison staging.
struct ArchiveReader {
    store: Store,
    runtime: Handle,
    chunks: std::vec::IntoIter<ChunkRef>,
    current: Cursor<Vec<u8>>,
}

impl Read for ArchiveReader {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        if output.is_empty() {
            return Ok(0);
        }
        loop {
            let count = self.current.read(output)?;
            if count > 0 {
                return Ok(count);
            }
            let Some(chunk) = self.chunks.next() else {
                return Ok(0);
            };
            let bytes = self
                .runtime
                .block_on(self.store.read_archive_chunk(&chunk))
                .map_err(|_| io::Error::other("invalid archived SQLite chunk"))?;
            self.current = Cursor::new(bytes);
        }
    }
}

struct Stage {
    entries: Vec<(Vec<u8>, Vec<u8>)>,
    bytes: usize,
}

impl Stage {
    fn new() -> Self {
        Self {
            entries: Vec::new(),
            bytes: 0,
        }
    }

    async fn push(&mut self, store: &Store, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        let size = key.len().saturating_add(value.len());
        if size.saturating_add(schema_key().len()) > MAX_BATCH_BYTES {
            return Err(invalid(
                "a row or schema record exceeds the transaction limit",
            ));
        }
        if !self.entries.is_empty()
            && (self.bytes.saturating_add(size) > STAGE_BYTES
                || self.entries.len() >= STAGE_RECORDS)
        {
            self.flush(store).await?;
        }
        self.entries.push((key, value));
        self.bytes += size;
        Ok(())
    }

    async fn flush(&mut self, store: &Store) -> Result<()> {
        if self.entries.is_empty() {
            return Ok(());
        }
        let mut tx = store.db.begin().await?;
        // A competing importer may publish this generation while we stage it.
        // Observing the active pointer in every batch prevents any later staging
        // write from restoring a row that an application transaction has changed.
        if tx.get(&schema_key()).await?.is_some() {
            return Err(invalid("application tables are already initialized"));
        }
        for (key, value) in &self.entries {
            match tx.get(key).await? {
                Some(existing) if existing == *value => {}
                Some(_) => return Err(invalid("duplicate key or inconsistent staged row")),
                None => tx.insert(key.clone(), value.clone()).await?,
            }
        }
        tx.commit().await?;
        self.entries.clear();
        self.bytes = 0;
        Ok(())
    }
}

fn table_from_record(record: &Value) -> Result<ApplicationTable> {
    let name = record["name"]
        .as_str()
        .ok_or(Error::IncompatibleStore)?
        .to_owned();
    let columns = record["columns"]
        .as_array()
        .ok_or(Error::IncompatibleStore)?;
    let mut primary = Vec::new();
    let mut names = Vec::new();
    for column in columns {
        let name = column["name"]
            .as_str()
            .ok_or(Error::IncompatibleStore)?
            .to_owned();
        let order = column["primaryKeyOrder"]
            .as_u64()
            .ok_or(Error::IncompatibleStore)?;
        if order > 0 {
            primary.push((order, name.clone()));
        }
        names.push(name);
    }
    primary.sort_by_key(|(order, _)| *order);
    if primary.is_empty() {
        return Err(invalid(
            "a source table has no primary key; an explicit mapping is required",
        ));
    }
    Ok(ApplicationTable {
        name,
        columns: names,
        primary_key: primary.into_iter().map(|(_, name)| name).collect(),
    })
}

fn row_from_record(table: &ApplicationTable, record: &Value) -> Result<ApplicationRow> {
    let values = record["values"]
        .as_array()
        .ok_or(Error::IncompatibleStore)?;
    let row: Map<String, Value> = table
        .columns
        .iter()
        .cloned()
        .zip(values.iter().cloned())
        .collect();
    let key: Vec<Value> = table
        .primary_key
        .iter()
        .map(|name| row[name].clone())
        .collect();
    if key.iter().any(|cell| cell["type"] == "null") {
        return Err(invalid(
            "a nullable SQLite primary key needs an explicit row identity mapping",
        ));
    }
    validate_row(table, &key, &row)?;
    Ok(ApplicationRow { key, row })
}

impl Store {
    /// Returns the durable activation receipt. It remains valid after application
    /// writes, and never asks a retry to overwrite imported rows with old values.
    pub async fn application_import(&self) -> Result<Option<ApplicationImportReceipt>> {
        let mut tx = self.db.begin().await?;
        let result = async {
            let Some(bytes) = tx.get(&receipt_key()).await? else {
                return Ok(None);
            };
            let mut receipt: ApplicationImportReceipt =
                serde_json::from_slice(&bytes).map_err(|_| Error::IncompatibleStore)?;
            let catalog: ApplicationCatalog = serde_json::from_slice(
                &tx.get(&schema_key())
                    .await?
                    .ok_or(Error::IncompatibleStore)?,
            )
            .map_err(|_| Error::IncompatibleStore)?;
            validate_schema(&catalog.schema).map_err(|_| Error::IncompatibleStore)?;
            if receipt.generation != receipt.summary.export_sha256
                || catalog.generation != receipt.generation
                || catalog.schema.tables.len() as u64 != receipt.summary.table_count
                || receipt.replayed
            {
                return Err(Error::IncompatibleStore);
            }
            let archived = self
                .archive_record(&receipt.generation)
                .await?
                .ok_or(Error::IncompatibleStore)?;
            if archived.summary != receipt.summary {
                return Err(Error::IncompatibleStore);
            }
            receipt.replayed = true;
            Ok(Some(receipt))
        }
        .await;
        tx.rollback().await?;
        result
    }

    /// Preserve a verified archive, stage bounded typed rows, and atomically
    /// publish the application catalog with its receipt. SQLite stays untouched.
    pub async fn import_sqlite_application(
        &self,
        path: impl AsRef<Path>,
        limits: ImportLimits,
    ) -> Result<ApplicationImportReceipt> {
        if self.application_import().await?.is_none()
            && self.db.read(&schema_key()).await?.is_some()
        {
            return Err(invalid("application tables are already initialized"));
        }
        let archived = self.archive_sqlite_export(path, limits).await?;
        if let Some(receipt) = self.application_import().await? {
            return if receipt.summary == archived.summary {
                Ok(receipt)
            } else {
                Err(invalid("a different application import is already active"))
            };
        }
        let record = self
            .archive_record(&archived.archive_id)
            .await?
            .ok_or(Error::IncompatibleStore)?;
        let generation = archived.archive_id.clone();
        let reader = ArchiveReader {
            store: self.clone(),
            runtime: Handle::current(),
            chunks: record.chunks.into_iter(),
            current: Cursor::new(Vec::new()),
        };
        let (send, mut records) = mpsc::channel::<Value>(2);
        let parser = tokio::task::spawn_blocking(move || {
            validate_reader(BufReader::new(reader), limits, |line| {
                let value = serde_json::from_slice(line).map_err(|_| Error::IncompatibleStore)?;
                send.blocking_send(value)
                    .map_err(|_| invalid("import was cancelled"))
            })
        });
        let mut schema = ApplicationSchema {
            version: 1,
            tables: Vec::new(),
        };
        let mut excluded_derived_tables = Vec::new();
        let mut stage = Stage::new();
        let mut survivor_source_flushed = false;
        let staged = async {
            while let Some(record) = records.recv().await {
                match record["type"].as_str().ok_or(Error::IncompatibleStore)? {
                    "header" => {
                        excluded_derived_tables =
                            serde_json::from_value(record["excludedDerivedTables"].clone())
                                .map_err(|_| Error::IncompatibleStore)?;
                        stage
                            .push(self, metadata_key(&generation, "header"), encode(&record)?)
                            .await?;
                    }
                    "table" => {
                        let table = table_from_record(&record)?;
                        validate_schema(&ApplicationSchema {
                            version: 1,
                            tables: vec![table.clone()],
                        })?;
                        stage
                            .push(
                                self,
                                metadata_key(
                                    &generation,
                                    &format!("table/{}", request_digest(&json!(table.name))),
                                ),
                                encode(&record)?,
                            )
                            .await?;
                        schema.tables.push(table);
                    }
                    "row" => {
                        let table = schema.tables.last().ok_or(Error::IncompatibleStore)?;
                        let row = row_from_record(table, &record)?;
                        let key = row_key(&generation, &table.name, &row.key)?;
                        // A second row with the same primary key must fail even
                        // when its contents happen to be identical. The ordinal
                        // witness also makes a repeated partial import idempotent.
                        let witness = metadata_key(
                            &generation,
                            &format!("source/{}", request_digest(&json!([table.name, row.key]))),
                        );
                        let mut source = json!({
                            "table": table.name, "key": row.key, "ordinal": record["ordinal"]
                        });
                        if let Some(rowid) = record.get("rowid") {
                            source["rowid"] = rowid.clone();
                        }
                        stage.push(self, witness, encode(&source)?).await?;
                        stage.push(self, key, encode_row(&row)?).await?;
                    }
                    "search_posting" | "search_survivor_posting" => {
                        let survivor = record["type"] == "search_survivor_posting";
                        let term = record["term"].as_str().ok_or(Error::IncompatibleStore)?;
                        let rowid: i64 = record["rowid"].as_str()
                            .and_then(|value| value.parse().ok()).ok_or(Error::IncompatibleStore)?;
                        let offset = record["offset"].as_u64().ok_or(Error::IncompatibleStore)?;
                        let term_key: String = term.as_bytes().iter()
                            .map(|byte| format!("{byte:02x}")).collect();
                        let ordered_rowid = !((rowid as u64) ^ (1 << 63));
                        let posting = encode(&json!({"term":term,"rowid":rowid.to_string(),"offset":offset}))?;
                        let original = metadata_key(&generation, &format!("search-posting/{term_key}/{ordered_rowid:016x}/{offset:016x}"));
                        if survivor {
                            if !survivor_source_flushed {
                                // Original postings precede every survivor in the
                                // validated stream. Prove each survivor is actual
                                // source evidence before publishing its metadata.
                                stage.flush(self).await?;
                                survivor_source_flushed = true;
                            }
                            if self.db.read(&original).await?.as_deref() != Some(posting.as_slice()) {
                                return Err(invalid("search survivor is not an exact original posting"));
                            }
                            stage.push(
                                self,
                                metadata_key(&generation, &format!("search-survivor-posting/{term_key}/{ordered_rowid:016x}/{offset:016x}")),
                                posting.clone(),
                            ).await?;
                            stage.push(
                                self,
                                metadata_key(&generation, &format!("search-survivor-document/{ordered_rowid:016x}/{term_key}/{offset:016x}")),
                                posting,
                            ).await?;
                            continue;
                        }
                        stage.push(
                            self,
                            original,
                            posting.clone(),
                        ).await?;
                        stage.push(
                            self,
                            metadata_key(&generation, &format!("search-document/{ordered_rowid:016x}/{term_key}/{offset:016x}")),
                            posting,
                        ).await?;
                    }
                    "search_current_document" => {
                        let rowid: i64 = record["rowid"].as_str()
                            .and_then(|value| value.parse().ok()).ok_or(Error::IncompatibleStore)?;
                        let ordered_rowid = !((rowid as u64) ^ (1 << 63));
                        stage.push(
                            self,
                            metadata_key(&generation, &format!("search-current-document/{ordered_rowid:016x}")),
                            encode(&json!({"rowid":rowid.to_string()}))?,
                        ).await?;
                    }
                    "manifest" => {
                        stage
                            .push(
                                self,
                                metadata_key(&generation, "manifest"),
                                encode(&record)?,
                            )
                            .await?;
                    }
                    _ => return Err(Error::IncompatibleStore),
                }
            }
            validate_schema(&schema)?;
            stage.flush(self).await
        }
        .await;
        drop(records);
        let parsed = parser
            .await
            .map_err(|_| Error::Database("import validation worker failed".into()))?;
        if let Err(error) = staged {
            // A competing importer may have completed this exact generation.
            // Resolve only known non-commit errors. Unknown outcomes stay unknown.
            if !matches!(error, Error::UnknownCommit)
                && let Some(receipt) = self.application_import().await?
                && receipt.summary == archived.summary
            {
                return Ok(receipt);
            }
            return Err(error);
        }
        if parsed? != archived.summary {
            return Err(Error::IncompatibleStore);
        }
        let receipt = ApplicationImportReceipt {
            generation: generation.clone(),
            summary: archived.summary,
            excluded_derived_tables,
            replayed: false,
        };
        let catalog = ApplicationCatalog { schema, generation };
        let mut tx = self.db.begin().await?;
        if tx.get(&schema_key()).await?.is_some() {
            tx.rollback().await?;
            return match self.application_import().await? {
                Some(existing) if existing.summary == receipt.summary => Ok(existing),
                _ => Err(invalid("application tables are already initialized")),
            };
        }
        tx.insert(schema_key(), encode_catalog(&catalog)?).await?;
        tx.insert(receipt_key(), encode(&receipt)?).await?;
        tx.commit().await?;
        Ok(receipt)
    }
}

#[cfg(test)]
#[path = "redb_import_tests.rs"]
mod tests;
