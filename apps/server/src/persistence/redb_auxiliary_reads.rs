//! Typed settings-migration and cleanup reads. No SQL interpreter or authority fallback.

use std::collections::BTreeSet;
use std::time::Duration;

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Value, json};

use crate::application::{
    ApplicationCatalog, ApplicationTransactions, Session, decode_catalog, decode_row, row_prefix,
    schema_key, table,
};
use crate::application_projections::read_index;
use crate::application_projects::ProjectSelection;
use crate::application_rows::{int_cell, nullable, read_row, text, text_cell};
use crate::backend::Transaction;
use crate::outbox::OutboxEffect;
use crate::{Error, Result, Store, request_digest, store_key};

const THREADS: &str = "orchestration_v2_projection_threads";
const PROJECTS: &str = "projection_projects";
const SESSIONS: &str = "orchestration_v2_projection_provider_sessions";
const LEGACY_SESSIONS: &str = "projection_thread_sessions";
const OUTBOX: &str = "orchestration_v2_effect_outbox";
const PAGE_ROWS: usize = 128;
const PREPARE_ROWS: usize = 32;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum AuxiliaryRead {
    ActiveRuns,
    LegacySubagentThreadIds,
    ProviderHistory,
    LegacyProjectSettings,
    AllThreadWorktrees,
    DeletedThreadWorktrees,
    PendingThreadCleanup { thread_id: String },
    NonStoppedProviderSessions,
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
    after: Option<String>,
    indexed_rows: u64,
    ready: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct OutboxWitness {
    effect_id: String,
    thread_id: String,
    status: String,
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
        "application/generation/{}/auxiliary-reads/v1/{suffix}",
        catalog.generation
    ))
}
fn prefix_end(prefix: &[u8]) -> Vec<u8> {
    let mut end = prefix.to_vec();
    end.push(255);
    end
}
fn thread_prefix(catalog: &ApplicationCatalog, thread_id: &str) -> Vec<u8> {
    key(
        catalog,
        &format!("outbox/{}/", request_digest(&json!(thread_id))),
    )
}
fn outbox_key(catalog: &ApplicationCatalog, witness: &OutboxWitness) -> Vec<u8> {
    let mut key = thread_prefix(catalog, &witness.thread_id);
    key.extend_from_slice(request_digest(&json!(witness.effect_id)).as_bytes());
    key
}
fn pending(status: &str) -> bool {
    !matches!(status, "succeeded" | "cancelled")
}

async fn put_outbox(
    tx: &mut Transaction,
    catalog: &ApplicationCatalog,
    witness: &OutboxWitness,
) -> Result<()> {
    if !pending(&witness.status) {
        return Ok(());
    }
    let key = outbox_key(catalog, witness);
    if let Some(bytes) = tx.get(&key).await? {
        if decode::<OutboxWitness>(&bytes)? != *witness {
            return Err(Error::IncompatibleStore);
        }
    }
    tx.put(key, encode(witness)?).await
}

/// Called by every live application outbox save/delete in its owning transaction.
pub(crate) async fn replace_outbox(
    tx: &mut Transaction,
    catalog: &ApplicationCatalog,
    previous: Option<&OutboxEffect>,
    next: Option<&OutboxEffect>,
) -> Result<()> {
    let state: Option<Preparation> = tx
        .get(&key(catalog, "preparation"))
        .await?
        .as_deref()
        .map(decode)
        .transpose()?;
    if state.as_ref().is_some_and(|state| state.version != 1) {
        return Err(Error::IncompatibleStore);
    }
    let strict = state.as_ref().is_some_and(|state| state.ready);
    if let Some(old) = previous {
        if pending(old.status.as_wire()) {
            let witness = OutboxWitness {
                effect_id: old.id.clone(),
                thread_id: old.thread_id.clone(),
                status: old.status.as_wire().into(),
            };
            let encoded = outbox_key(catalog, &witness);
            match tx.get(&encoded).await? {
                Some(bytes) if decode::<OutboxWitness>(&bytes)? == witness => {
                    tx.delete(encoded).await?;
                }
                None if !strict => (),
                _ => return Err(Error::IncompatibleStore),
            }
        }
    }
    if let Some(next) = next {
        put_outbox(
            tx,
            catalog,
            &OutboxWitness {
                effect_id: next.id.clone(),
                thread_id: next.thread_id.clone(),
                status: next.status.as_wire().into(),
            },
        )
        .await?;
    }
    Ok(())
}

async fn pending_cleanup(session: &mut Session, thread_id: &str) -> Result<Value> {
    crate::application_outbox::ready(&mut session.transaction, &session.catalog).await?;
    let state: Preparation = session
        .transaction
        .get(&key(&session.catalog, "preparation"))
        .await?
        .as_deref()
        .map(decode)
        .transpose()?
        .ok_or_else(|| invalid("auxiliary reads are not prepared"))?;
    if state.version != 1 || !state.ready {
        return Err(invalid("auxiliary reads are not prepared"));
    }
    let prefix = thread_prefix(&session.catalog, thread_id);
    let end = prefix_end(&prefix);
    let mut cursor = prefix;
    let mut statuses = Vec::new();
    loop {
        let entries = session.transaction.scan(&cursor, &end, PAGE_ROWS).await?;
        let count = entries.len();
        for (encoded, bytes) in entries {
            let witness: OutboxWitness = decode(&bytes)?;
            if witness.thread_id != thread_id
                || !pending(&witness.status)
                || outbox_key(&session.catalog, &witness) != encoded
            {
                return Err(Error::IncompatibleStore);
            }
            // This managed index is backfilled before use and replaced with the
            // authoritative row in the same transaction. Do not load potentially
            // large effect request bodies for a status-only cleanup predicate.
            statuses.push(witness.status);
            cursor = encoded;
            cursor.push(0);
        }
        if count < PAGE_ROWS {
            break;
        }
    }
    statuses.sort();
    Ok(Value::Array(
        statuses
            .into_iter()
            .map(|status| json!({"status":status}))
            .collect(),
    ))
}

/// Recovery asks for owning threads, not effect request payloads.
pub(crate) async fn active_outbox_threads(session: &mut Session) -> Result<Vec<String>> {
    crate::application_outbox::ready(&mut session.transaction, &session.catalog).await?;
    let state: Preparation = session
        .transaction
        .get(&key(&session.catalog, "preparation"))
        .await?
        .as_deref()
        .map(decode)
        .transpose()?
        .ok_or_else(|| invalid("auxiliary reads are not prepared"))?;
    if state.version != 1 || !state.ready {
        return Err(invalid("auxiliary reads are not prepared"));
    }
    let prefix = key(&session.catalog, "outbox/");
    let end = prefix_end(&prefix);
    let mut cursor = prefix;
    let mut threads = BTreeSet::new();
    loop {
        let entries = session.transaction.scan(&cursor, &end, PAGE_ROWS).await?;
        let count = entries.len();
        for (encoded, bytes) in entries {
            let witness: OutboxWitness = decode(&bytes)?;
            if outbox_key(&session.catalog, &witness) != encoded || !pending(&witness.status) {
                return Err(Error::IncompatibleStore);
            }
            if matches!(witness.status.as_str(), "pending" | "running") {
                threads.insert(witness.thread_id);
            }
            cursor = encoded;
            cursor.push(0);
        }
        if count < PAGE_ROWS {
            break;
        }
    }
    Ok(threads.into_iter().collect())
}

async fn provider_history(session: &mut Session) -> Result<Value> {
    let providers = ["cursor", "grok", "opencode"];
    let mut values: BTreeSet<(String, Option<String>)> = BTreeSet::new();
    for row in crate::application_runtime_state::provider_history(session, &providers).await? {
        let name = row
            .get("providerName")
            .and_then(Value::as_str)
            .ok_or(Error::IncompatibleStore)?
            .to_owned();
        let instance = match row.get("providerInstanceId") {
            Some(Value::Null) => None,
            Some(Value::String(value)) => Some(value.clone()),
            _ => return Err(Error::IncompatibleStore),
        };
        values.insert((name, instance));
    }
    // V1 history is retained for migration. V2 services never mutate this table.
    let definition = table(&session.catalog, LEGACY_SESSIONS)?;
    if definition.primary_key != ["thread_id"]
        || !definition
            .columns
            .iter()
            .any(|column| column == "provider_instance_id")
    {
        return Err(Error::IncompatibleStore);
    }
    let prefix = row_prefix(&session.catalog.generation, LEGACY_SESSIONS)?;
    let end = prefix_end(&prefix);
    let mut cursor = prefix;
    loop {
        let page = session.transaction.scan(&cursor, &end, PAGE_ROWS).await?;
        let count = page.len();
        for (encoded, bytes) in page {
            let source = decode_row(
                &session.catalog,
                table(&session.catalog, LEGACY_SESSIONS)?,
                &encoded,
                &bytes,
            )?;
            if let Some(name) = nullable(&source.row, "provider_name")? {
                if providers.contains(&name.as_str()) {
                    values.insert((name, nullable(&source.row, "provider_instance_id")?));
                }
            }
            cursor = encoded;
            cursor.push(0);
        }
        if count < PAGE_ROWS {
            break;
        }
    }
    // SQLite UNION uses BINARY text ordering with null before text.
    Ok(Value::Array(
        values
            .into_iter()
            .map(|(name, instance)| json!({"providerName":name,"providerInstanceId":instance}))
            .collect(),
    ))
}

impl Store {
    pub async fn prepare_application_auxiliary(&self) -> Result<PreparationResult> {
        let mut limit = PREPARE_ROWS;
        loop {
            match self.prepare_application_auxiliary_batch(limit).await {
                Err(Error::ResourceLimit(_)) if limit > 1 => limit = (limit / 2).max(1),
                result => return result,
            }
        }
    }
    async fn prepare_application_auxiliary_batch(&self, limit: usize) -> Result<PreparationResult> {
        let mut tx = self
            .db
            .begin_reserved(Duration::from_secs(5), Duration::from_secs(300))
            .await?;
        let catalog = decode_catalog(
            &tx.get(&schema_key())
                .await?
                .ok_or(Error::IncompatibleStore)?,
        )?;
        crate::application_outbox::ready(&mut tx, &catalog).await?;
        let state_key = key(&catalog, "preparation");
        let mut state: Preparation = tx
            .get(&state_key)
            .await?
            .as_deref()
            .map(decode)
            .transpose()?
            .unwrap_or(Preparation {
                version: 1,
                after: None,
                indexed_rows: 0,
                ready: false,
            });
        if state.version != 1 {
            return Err(Error::IncompatibleStore);
        }
        if state.ready {
            tx.rollback().await?;
            return Ok(PreparationResult {
                prepared: true,
                indexed_rows: state.indexed_rows,
            });
        }
        let prefix = row_prefix(&catalog.generation, OUTBOX)?;
        let mut cursor = prefix.clone();
        if let Some(after) = &state.after {
            if after.len() != 64
                || !after
                    .bytes()
                    .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
            {
                return Err(Error::IncompatibleStore);
            }
            cursor.extend_from_slice(after.as_bytes());
            cursor.push(0);
        }
        let page = tx.scan(&cursor, &prefix_end(&prefix), limit).await?;
        let count = page.len();
        for (encoded, bytes) in page {
            let row = decode_row(&catalog, table(&catalog, OUTBOX)?, &encoded, &bytes)?;
            put_outbox(
                &mut tx,
                &catalog,
                &OutboxWitness {
                    effect_id: text(&row.row, "effect_id")?,
                    thread_id: text(&row.row, "thread_id")?,
                    status: text(&row.row, "status")?,
                },
            )
            .await?;
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
    pub async fn auxiliary_read(&mut self, id: &str, query: AuxiliaryRead) -> Result<Value> {
        if matches!(query, AuxiliaryRead::LegacyProjectSettings) {
            return Ok(Value::Array(self.projects_select(id, ProjectSelection::default()).await?.into_iter().map(|row| json!({
                "projectId":row.project_id,"defaultModelSelection":row.default_model_selection,
                "defaultThreadEnvMode":row.default_thread_env_mode,"autoPull":row.auto_pull,"scripts":row.scripts,
            })).collect()));
        }
        let mut session = self.take(id)?;
        let result = async {
            match query {
                AuxiliaryRead::ActiveRuns => {
                    // The existing recovery index contains active/waiting and unheld queued runs.
                    // Narrow it to the inhibitor's exact states; a waiting run permits sleep.
                    read_index::any_equal(
                        &mut session, "orchestration_v2_projection_runs", "_recovery", &text_cell("runtime_run"),
                        |row| Ok(matches!(text(row, "status")?.as_str(), "preparing" | "starting" | "running")),
                    ).await.map(Value::Bool)
                }
                AuxiliaryRead::LegacySubagentThreadIds => {
                    read_index::filter_map_equal(
                        &mut session, THREADS, "all", &Value::Null,
                        |row| {
                            let id = text(&row, "thread_id")?;
                            // SQLite LIKE folds ASCII case and has no live/archive filter here.
                            Ok(id.to_ascii_lowercase().starts_with("thread:provider:").then_some(Value::String(id)))
                        },
                    ).await.map(Value::Array)
                }
                AuxiliaryRead::ProviderHistory => provider_history(&mut session).await,
                AuxiliaryRead::PendingThreadCleanup { thread_id } => pending_cleanup(&mut session, &thread_id).await,
                AuxiliaryRead::AllThreadWorktrees => {
                    let rows = read_index::all_workspace_paths(&mut session).await?;
                    let mut seen = BTreeSet::new(); let mut result = Vec::new();
                    for row in rows {
                        let path = row.get("worktree_path").and_then(Value::as_str).ok_or(Error::IncompatibleStore)?;
                        if seen.insert(path.to_owned()) { result.push(json!({"worktreePath":path})); }
                    }
                    Ok(Value::Array(result))
                }
                AuxiliaryRead::DeletedThreadWorktrees => {
                    let mut rows = read_index::equal_rows(&mut session, THREADS, "_deleted", &int_cell(1)).await?;
                    rows.sort_by_key(|row| row.order);
                    let mut result = Vec::new();
                    for row in rows {
                        if nullable(&row.row, "deleted_at")?.is_none() { continue; }
                        if let Some(project) = read_row(&mut session.transaction, &session.catalog, PROJECTS, &[text_cell(&text(&row.row,"project_id")?)]).await? {
                            result.push(json!({"payload_json":text(&row.row,"payload_json")?,"workspaceRoot":text(&project.row,"workspace_root")?}));
                        }
                    }
                    Ok(Value::Array(result))
                }
                AuxiliaryRead::NonStoppedProviderSessions => {
                    let mut rows = read_index::equal_rows(&mut session, SESSIONS, "_non_stopped", &int_cell(1)).await?;
                    rows.sort_by_key(|row| row.order);
                    let mut result = Vec::new();
                    for row in rows {
                        if text(&row.row, "status")? != "stopped" { result.push(json!({"payload_json":text(&row.row,"payload_json")?})); }
                    }
                    Ok(Value::Array(result))
                }
                AuxiliaryRead::LegacyProjectSettings => unreachable!(),
            }
        }.await;
        self.finish(id, session, result).await
    }
}
