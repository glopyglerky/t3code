//! Project rows and ordered active/workspace indexes in the shared application transaction.

use std::collections::BTreeSet;
use std::time::Duration;

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Map, Value, json};

use crate::application::{
    ApplicationCatalog, ApplicationTransactions, Session, decode_catalog, decode_row, row_prefix,
    schema_key, table,
};
use crate::application_rows::{
    int_cell, integer, managed_marker, nullable, nullable_cell, read_row, text, text_cell,
    validate_table, write_row,
};
use crate::backend::Transaction;
use crate::{Error, Result, Store, request_digest, store_key};

const PROJECTS: &str = "projection_projects";
const COLUMNS: [&str; 14] = [
    "project_id",
    "title",
    "workspace_root",
    "default_model_selection_json",
    "default_thread_env_mode",
    "auto_pull",
    "favicon_path",
    "project_icon_json",
    "scripts_json",
    "pinned_at",
    "archived_at",
    "created_at",
    "updated_at",
    "deleted_at",
];
const PREPARE_ROWS: usize = 32;
const READ_ROWS: usize = 256;
const MARKER: &str = "project-rows-v1";

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ProjectRecord {
    pub project_id: String,
    pub title: String,
    pub workspace_root: String,
    pub default_model_selection: Option<String>,
    pub default_thread_env_mode: Option<String>,
    pub auto_pull: i64,
    pub favicon_path: Option<String>,
    pub project_icon: Option<String>,
    pub scripts: String,
    pub pinned_at: Option<String>,
    pub archived_at: Option<String>,
    pub created_at: String,
    pub updated_at: String,
    pub deleted_at: Option<String>,
}

#[derive(Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectSelection {
    pub project_id: Option<String>,
    pub project_ids: Option<Vec<String>>,
    pub workspace_root: Option<String>,
    pub include_deleted: bool,
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
struct ProjectIndex {
    project_id: String,
    created_at: String,
    workspace_root: String,
    active: bool,
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
        "application/generation/{}/projects/v1/{suffix}",
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

impl ProjectRecord {
    fn validate(&self) -> Result<()> {
        if !matches!(self.auto_pull, 0 | 1) {
            return Err(invalid("project auto-pull must be a Boolean bit"));
        }
        Ok(())
    }

    fn from_row(row: &Map<String, Value>) -> Result<Self> {
        let record = Self {
            project_id: text(row, "project_id")?,
            title: text(row, "title")?,
            workspace_root: text(row, "workspace_root")?,
            default_model_selection: nullable(row, "default_model_selection_json")?,
            default_thread_env_mode: nullable(row, "default_thread_env_mode")?,
            auto_pull: integer(row, "auto_pull")?,
            favicon_path: nullable(row, "favicon_path")?,
            project_icon: nullable(row, "project_icon_json")?,
            scripts: text(row, "scripts_json")?,
            pinned_at: nullable(row, "pinned_at")?,
            archived_at: nullable(row, "archived_at")?,
            created_at: text(row, "created_at")?,
            updated_at: text(row, "updated_at")?,
            deleted_at: nullable(row, "deleted_at")?,
        };
        record.validate().map_err(|_| Error::IncompatibleStore)?;
        Ok(record)
    }

    fn stored(&self) -> Map<String, Value> {
        Map::from_iter([
            ("project_id".into(), text_cell(&self.project_id)),
            ("title".into(), text_cell(&self.title)),
            ("workspace_root".into(), text_cell(&self.workspace_root)),
            (
                "default_model_selection_json".into(),
                nullable_cell(self.default_model_selection.as_deref()),
            ),
            (
                "default_thread_env_mode".into(),
                nullable_cell(self.default_thread_env_mode.as_deref()),
            ),
            ("auto_pull".into(), int_cell(self.auto_pull)),
            (
                "favicon_path".into(),
                nullable_cell(self.favicon_path.as_deref()),
            ),
            (
                "project_icon_json".into(),
                nullable_cell(self.project_icon.as_deref()),
            ),
            ("scripts_json".into(), text_cell(&self.scripts)),
            ("pinned_at".into(), nullable_cell(self.pinned_at.as_deref())),
            (
                "archived_at".into(),
                nullable_cell(self.archived_at.as_deref()),
            ),
            ("created_at".into(), text_cell(&self.created_at)),
            ("updated_at".into(), text_cell(&self.updated_at)),
            (
                "deleted_at".into(),
                nullable_cell(self.deleted_at.as_deref()),
            ),
        ])
    }

    fn index(&self) -> ProjectIndex {
        ProjectIndex {
            project_id: self.project_id.clone(),
            created_at: self.created_at.clone(),
            workspace_root: self.workspace_root.clone(),
            active: self.deleted_at.is_none(),
        }
    }

    fn matches(&self, input: &ProjectSelection) -> bool {
        (input.include_deleted || self.deleted_at.is_none())
            && input
                .project_id
                .as_ref()
                .is_none_or(|id| id == &self.project_id)
            && input
                .project_ids
                .as_ref()
                .is_none_or(|ids| ids.contains(&self.project_id))
            && input
                .workspace_root
                .as_ref()
                .is_none_or(|root| root == &self.workspace_root)
    }
}

impl ProjectIndex {
    fn keys(&self, catalog: &ApplicationCatalog) -> Vec<Vec<u8>> {
        // Hex preserves SQLite BINARY text ordering, including prefix strings and NUL bytes.
        let order = format!("{}/{}", hex(&self.created_at), hex(&self.project_id));
        let workspace = request_digest(&json!([self.workspace_root]));
        let mut values = vec![
            key(catalog, &format!("all/{order}")),
            key(catalog, &format!("workspace/{workspace}/all/{order}")),
        ];
        if self.active {
            values.push(key(catalog, &format!("active/{order}")));
            values.push(key(
                catalog,
                &format!("workspace/{workspace}/active/{order}"),
            ));
        }
        values
    }
}

async fn index_project(
    tx: &mut Transaction,
    catalog: &ApplicationCatalog,
    record: &ProjectRecord,
) -> Result<()> {
    let index = record.index();
    for encoded_key in index.keys(catalog) {
        if let Some(bytes) = tx.get(&encoded_key).await? {
            if decode::<ProjectIndex>(&bytes)? != index {
                return Err(Error::IncompatibleStore);
            }
        }
        tx.put(encoded_key, encode(&index)?).await?;
    }
    Ok(())
}

async fn remove_indexes(
    tx: &mut Transaction,
    catalog: &ApplicationCatalog,
    record: &ProjectRecord,
) -> Result<()> {
    let index = record.index();
    for encoded_key in index.keys(catalog) {
        if decode::<ProjectIndex>(
            &tx.get(&encoded_key)
                .await?
                .ok_or(Error::IncompatibleStore)?,
        )? != index
        {
            return Err(Error::IncompatibleStore);
        }
        tx.delete(encoded_key).await?;
    }
    Ok(())
}

async fn ready(session: &mut Session) -> Result<()> {
    let marker = session
        .transaction
        .get(&managed_marker(&session.catalog, PROJECTS))
        .await?;
    if marker
        .as_deref()
        .map(decode::<String>)
        .transpose()?
        .as_deref()
        != Some(MARKER)
    {
        return Err(invalid("application projects are not prepared"));
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
        return Err(invalid("application projects are not prepared"));
    }
    Ok(())
}

impl Store {
    pub async fn prepare_application_projects(&self) -> Result<PreparationResult> {
        let mut limit = PREPARE_ROWS;
        loop {
            match self.prepare_application_projects_batch(limit).await {
                Err(Error::ResourceLimit(_)) if limit > 1 => limit = (limit / 2).max(1),
                result => return result,
            }
        }
    }

    async fn prepare_application_projects_batch(&self, limit: usize) -> Result<PreparationResult> {
        let mut tx = self
            .db
            .begin_reserved(Duration::from_secs(5), Duration::from_secs(300))
            .await?;
        let catalog = decode_catalog(
            &tx.get(&schema_key())
                .await?
                .ok_or_else(|| invalid("application schema is not initialized"))?,
        )?;
        validate_table(&catalog, PROJECTS, &COLUMNS, &["project_id"])?;
        let marker = managed_marker(&catalog, PROJECTS);
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
        let prefix = row_prefix(&catalog.generation, PROJECTS)?;
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
            let source = decode_row(&catalog, table(&catalog, PROJECTS)?, &encoded, &bytes)?;
            index_project(&mut tx, &catalog, &ProjectRecord::from_row(&source.row)?).await?;
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
    pub async fn projects_upsert(&mut self, id: &str, record: ProjectRecord) -> Result<()> {
        let mut session = self.take(id)?;
        let result = async {
            session.transaction.require_write()?;
            ready(&mut session).await?;
            record.validate()?;
            if let Some(previous) = read_row(
                &mut session.transaction,
                &session.catalog,
                PROJECTS,
                &[text_cell(&record.project_id)],
            )
            .await?
            {
                remove_indexes(
                    &mut session.transaction,
                    &session.catalog,
                    &ProjectRecord::from_row(&previous.row)?,
                )
                .await?;
            }
            write_row(
                &mut session.transaction,
                &session.catalog,
                PROJECTS,
                record.stored(),
            )
            .await?;
            index_project(&mut session.transaction, &session.catalog, &record).await
        }
        .await;
        self.finish(id, session, result).await
    }

    pub async fn projects_select(
        &mut self,
        id: &str,
        input: ProjectSelection,
    ) -> Result<Vec<ProjectRecord>> {
        let mut session = self.take(id)?;
        let result = async {
            ready(&mut session).await?;
            let selected_ids: Option<BTreeSet<String>> = input
                .project_id
                .as_ref()
                .map(|id| BTreeSet::from([id.clone()]))
                .or_else(|| {
                    input
                        .project_ids
                        .as_ref()
                        .map(|ids| ids.iter().cloned().collect())
                });
            let mut result = Vec::new();
            if let Some(ids) = selected_ids {
                for project_id in ids {
                    if input
                        .project_ids
                        .as_ref()
                        .is_some_and(|ids| !ids.contains(&project_id))
                    {
                        continue;
                    }
                    if let Some(row) = read_row(
                        &mut session.transaction,
                        &session.catalog,
                        PROJECTS,
                        &[text_cell(&project_id)],
                    )
                    .await?
                    {
                        let record = ProjectRecord::from_row(&row.row)?;
                        if record.matches(&input) {
                            result.push(record);
                        }
                    }
                }
                result.sort_by(|a, b| {
                    a.created_at
                        .cmp(&b.created_at)
                        .then(a.project_id.cmp(&b.project_id))
                });
                return Ok(result);
            }
            let activity = if input.include_deleted {
                "all"
            } else {
                "active"
            };
            let prefix = key(
                &session.catalog,
                &match input.workspace_root.as_ref() {
                    Some(root) => {
                        format!("workspace/{}/{activity}/", request_digest(&json!([root])))
                    }
                    None => format!("{activity}/"),
                },
            );
            let mut start = prefix.clone();
            loop {
                let rows = session
                    .transaction
                    .scan(&start, &prefix_end(&prefix)?, READ_ROWS)
                    .await?;
                let count = rows.len();
                for (encoded, bytes) in rows {
                    let index: ProjectIndex = decode(&bytes)?;
                    if !index.keys(&session.catalog).contains(&encoded) {
                        return Err(Error::IncompatibleStore);
                    }
                    let row = read_row(
                        &mut session.transaction,
                        &session.catalog,
                        PROJECTS,
                        &[text_cell(&index.project_id)],
                    )
                    .await?
                    .ok_or(Error::IncompatibleStore)?;
                    let record = ProjectRecord::from_row(&row.row)?;
                    if record.index() != index || !record.matches(&input) {
                        return Err(Error::IncompatibleStore);
                    }
                    result.push(record);
                    start = encoded;
                    start.push(0);
                }
                if count < READ_ROWS {
                    return Ok(result);
                }
            }
        }
        .await;
        self.finish(id, session, result).await
    }
}

#[cfg(test)]
#[path = "redb_project_rows_tests.rs"]
mod tests;
