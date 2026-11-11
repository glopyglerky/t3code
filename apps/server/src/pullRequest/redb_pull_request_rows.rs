//! Pull-request review guides and viewed-file marks in the application omdb owner.
//! Viewed-file order comes from a witnessed secondary index, never a full table scan.

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
use crate::{Error, Result, Store, request_digest, store_key};

const GUIDES: &str = "pull_request_review_guides";
const GUIDE_COLUMNS: [&str; 4] = ["host", "repository", "number", "guide_json"];
const FILES: &str = "pull_request_files_viewed";
const FILE_COLUMNS: [&str; 8] = [
    "provider",
    "host",
    "repository",
    "number",
    "viewer",
    "path",
    "revision",
    "viewed_at",
];
const FILE_PRIMARY: [&str; 6] = ["provider", "host", "repository", "number", "viewer", "path"];
const MARKER: &str = "pull-request-files-v1";
const PREPARE_ROWS: usize = 32;
const MAX_VIEWED: usize = 500;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct GuideKey {
    pub host: String,
    pub repository: String,
    pub number: i64,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ViewedScope {
    pub provider: String,
    pub host: String,
    pub repository: String,
    pub number: i64,
    pub viewer: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct FileMark {
    pub path: String,
    pub revision: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FileUpdate {
    pub path: String,
    pub revision: Option<String>,
    pub viewed: bool,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ViewedPage {
    pub files: Vec<FileMark>,
    pub truncated: bool,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct PreparationResult {
    pub prepared: bool,
    pub indexed_rows: u64,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Preparation {
    version: u32,
    ready: bool,
    after: Option<String>,
    indexed_rows: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct FileIndex {
    scope: ViewedScope,
    path: String,
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
        "application/generation/{}/pr-files/v1/{suffix}",
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
fn guide_key(input: &GuideKey) -> Vec<Value> {
    vec![
        text_cell(&input.host),
        text_cell(&input.repository),
        int_cell(input.number),
    ]
}
fn file_key(scope: &ViewedScope, path: &str) -> Vec<Value> {
    vec![
        text_cell(&scope.provider),
        text_cell(&scope.host),
        text_cell(&scope.repository),
        int_cell(scope.number),
        text_cell(&scope.viewer),
        text_cell(path),
    ]
}
fn scope_prefix(catalog: &ApplicationCatalog, scope: &ViewedScope) -> Vec<u8> {
    key(
        catalog,
        &format!(
            "scope/{}/path/",
            request_digest(&json!([
                scope.provider,
                scope.host,
                scope.repository,
                scope.number,
                scope.viewer
            ]))
        ),
    )
}
fn file_index_key(catalog: &ApplicationCatalog, scope: &ViewedScope, path: &str) -> Vec<u8> {
    let mut key = scope_prefix(catalog, scope);
    key.extend_from_slice(hex(path).as_bytes());
    key
}
fn row_scope(row: &Map<String, Value>) -> Result<ViewedScope> {
    Ok(ViewedScope {
        provider: text(row, "provider")?,
        host: text(row, "host")?,
        repository: text(row, "repository")?,
        number: integer(row, "number")?,
        viewer: text(row, "viewer")?,
    })
}
fn row_mark(row: &Map<String, Value>) -> Result<FileMark> {
    Ok(FileMark {
        path: text(row, "path")?,
        revision: nullable(row, "revision")?,
    })
}
fn validate_files(catalog: &ApplicationCatalog) -> Result<()> {
    validate_table(catalog, FILES, &FILE_COLUMNS, &FILE_PRIMARY)
}
async fn indexed_ready(session: &mut Session) -> Result<()> {
    validate_files(&session.catalog)?;
    let marker = session
        .transaction
        .get(&managed_marker(&session.catalog, FILES))
        .await?;
    if marker
        .as_deref()
        .map(decode::<String>)
        .transpose()?
        .as_deref()
        != Some(MARKER)
    {
        return Err(invalid("application viewed files are not prepared"));
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
        return Err(invalid("application viewed files are not prepared"));
    }
    Ok(())
}

async fn index_file(
    tx: &mut Transaction,
    catalog: &ApplicationCatalog,
    scope: &ViewedScope,
    path: &str,
) -> Result<()> {
    let record = FileIndex {
        scope: scope.clone(),
        path: path.into(),
    };
    let encoded = file_index_key(catalog, scope, path);
    if let Some(bytes) = tx.get(&encoded).await? {
        if decode::<FileIndex>(&bytes)? != record {
            return Err(Error::IncompatibleStore);
        }
    }
    tx.put(encoded, encode(&record)?).await
}

async fn remove_index(
    tx: &mut Transaction,
    catalog: &ApplicationCatalog,
    scope: &ViewedScope,
    path: &str,
) -> Result<()> {
    let encoded = file_index_key(catalog, scope, path);
    let witness: FileIndex = decode(&tx.get(&encoded).await?.ok_or(Error::IncompatibleStore)?)?;
    if witness.scope != *scope || witness.path != path {
        return Err(Error::IncompatibleStore);
    }
    tx.delete(encoded).await?;
    Ok(())
}

impl Store {
    pub async fn prepare_application_viewed_files(&self) -> Result<PreparationResult> {
        let mut limit = PREPARE_ROWS;
        loop {
            match self.prepare_application_viewed_files_batch(limit).await {
                Err(Error::ResourceLimit(_)) if limit > 1 => limit = (limit / 2).max(1),
                result => return result,
            }
        }
    }

    async fn prepare_application_viewed_files_batch(
        &self,
        limit: usize,
    ) -> Result<PreparationResult> {
        let mut tx = self
            .db
            .begin_reserved(Duration::from_secs(5), Duration::from_secs(300))
            .await?;
        let catalog = decode_catalog(
            &tx.get(&schema_key())
                .await?
                .ok_or_else(|| invalid("application schema is not initialized"))?,
        )?;
        validate_files(&catalog)?;
        let marker = managed_marker(&catalog, FILES);
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
        let prefix = row_prefix(&catalog.generation, FILES)?;
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
        let rows = tx.scan(&start, &prefix_end(&prefix)?, limit).await?;
        let count = rows.len();
        for (encoded, bytes) in rows {
            let source = decode_row(&catalog, table(&catalog, FILES)?, &encoded, &bytes)?;
            let scope = row_scope(&source.row)?;
            let mark = row_mark(&source.row)?;
            text(&source.row, "viewed_at")?;
            index_file(&mut tx, &catalog, &scope, &mark.path).await?;
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
    pub async fn guide_get(&mut self, id: &str, input: GuideKey) -> Result<Option<String>> {
        let mut session = self.take(id)?;
        let result = async {
            validate_table(
                &session.catalog,
                GUIDES,
                &GUIDE_COLUMNS,
                &["host", "repository", "number"],
            )?;
            read_row(
                &mut session.transaction,
                &session.catalog,
                GUIDES,
                &guide_key(&input),
            )
            .await?
            .map(|row| text(&row.row, "guide_json"))
            .transpose()
        }
        .await;
        self.finish(id, session, result).await
    }

    pub async fn guide_put(&mut self, id: &str, input: GuideKey, guide_json: String) -> Result<()> {
        let mut session = self.take(id)?;
        let result = async {
            session.transaction.require_write()?;
            validate_table(
                &session.catalog,
                GUIDES,
                &GUIDE_COLUMNS,
                &["host", "repository", "number"],
            )?;
            write_row(
                &mut session.transaction,
                &session.catalog,
                GUIDES,
                Map::from_iter([
                    ("host".into(), text_cell(&input.host)),
                    ("repository".into(), text_cell(&input.repository)),
                    ("number".into(), int_cell(input.number)),
                    ("guide_json".into(), text_cell(&guide_json)),
                ]),
            )
            .await
        }
        .await;
        self.finish(id, session, result).await
    }

    pub async fn viewed_files_set(
        &mut self,
        id: &str,
        scope: ViewedScope,
        files: Vec<FileUpdate>,
        viewed_at: String,
    ) -> Result<()> {
        let mut session = self.take(id)?;
        let result = async {
            session.transaction.require_write()?;
            indexed_ready(&mut session).await?;
            for file in files {
                let key = file_key(&scope, &file.path);
                let previous =
                    read_row(&mut session.transaction, &session.catalog, FILES, &key).await?;
                if previous.is_some() {
                    remove_index(
                        &mut session.transaction,
                        &session.catalog,
                        &scope,
                        &file.path,
                    )
                    .await?;
                }
                if file.viewed {
                    write_row(
                        &mut session.transaction,
                        &session.catalog,
                        FILES,
                        Map::from_iter([
                            ("provider".into(), text_cell(&scope.provider)),
                            ("host".into(), text_cell(&scope.host)),
                            ("repository".into(), text_cell(&scope.repository)),
                            ("number".into(), int_cell(scope.number)),
                            ("viewer".into(), text_cell(&scope.viewer)),
                            ("path".into(), text_cell(&file.path)),
                            ("revision".into(), nullable_cell(file.revision.as_deref())),
                            ("viewed_at".into(), text_cell(&viewed_at)),
                        ]),
                    )
                    .await?;
                    index_file(
                        &mut session.transaction,
                        &session.catalog,
                        &scope,
                        &file.path,
                    )
                    .await?;
                } else {
                    delete_row(&mut session.transaction, &session.catalog, FILES, &key).await?;
                }
            }
            Ok(())
        }
        .await;
        self.finish(id, session, result).await
    }

    pub async fn viewed_files_list(&mut self, id: &str, scope: ViewedScope) -> Result<ViewedPage> {
        let mut session = self.take(id)?;
        let result = async {
            indexed_ready(&mut session).await?;
            let prefix = scope_prefix(&session.catalog, &scope);
            let records = session
                .transaction
                .scan(&prefix, &prefix_end(&prefix)?, MAX_VIEWED + 1)
                .await?;
            let truncated = records.len() > MAX_VIEWED;
            let mut files = Vec::with_capacity(records.len().min(MAX_VIEWED));
            for (encoded, bytes) in records {
                let index: FileIndex = decode(&bytes)?;
                if index.scope != scope
                    || file_index_key(&session.catalog, &scope, &index.path) != encoded
                {
                    return Err(Error::IncompatibleStore);
                }
                let row = read_row(
                    &mut session.transaction,
                    &session.catalog,
                    FILES,
                    &file_key(&scope, &index.path),
                )
                .await?
                .ok_or(Error::IncompatibleStore)?;
                if row_scope(&row.row)? != scope || row_mark(&row.row)?.path != index.path {
                    return Err(Error::IncompatibleStore);
                }
                if files.len() < MAX_VIEWED {
                    files.push(row_mark(&row.row)?);
                }
            }
            Ok(ViewedPage { files, truncated })
        }
        .await;
        self.finish(id, session, result).await
    }
}

#[cfg(test)]
#[path = "redb_pull_request_rows_tests.rs"]
mod tests;
