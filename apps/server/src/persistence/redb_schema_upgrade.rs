//! Exclusive, bounded upgrades from the exact schema-80, schema-88 and schema-89 histories.
//! Staging never changes the source
//! generation. Retry clears the candidate and copies the current source again,
//! including writes an older executable may have made after an interrupted run.
use super::*;
use crate::application::{ApplicationRow, encode_row, row_key};
use crate::application_rows::{managed_marker, nullable, nullable_cell, read_row};
use crate::backend::Transaction;
use crate::{request_digest, store_key};
use serde_json::{Map, Value};

const MESSAGES: &str = "orchestration_v2_projection_messages";
const PROJECTS: &str = "projection_projects";
const ITEMS: &str = "orchestration_v2_projection_turn_items";
const EVENTS: &str = "orchestration_events";
const SESSIONS: &str = "auth_sessions";
const LINKS: &str = "auth_pairing_links";
const PAGE: usize = 32;
const COUNT_ORIGIN_NAMESPACE: &[u8] = b"upgrade-count-origins/v1/";
fn prefix(generation: &str) -> Vec<u8> {
    store_key(&format!("application/generation/{generation}/"))
}
fn end(prefix: &[u8]) -> Vec<u8> {
    let mut end = prefix.to_vec();
    *end.last_mut().expect("nonempty key prefix") += 1;
    end
}
fn encode(value: &impl Serialize) -> Result<Vec<u8>> {
    serde_json::to_vec(value).map_err(|_| Error::IncompatibleStore)
}

#[derive(Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
struct CountOrigin {
    generation: String,
    thread_id: String,
    run_id: Option<String>,
}

fn count_origin_prefix(catalog: &ApplicationCatalog) -> Vec<u8> {
    let mut key = prefix(&catalog.generation);
    key.extend_from_slice(COUNT_ORIGIN_NAMESPACE);
    key
}

async fn validate_count_origin(
    tx: &mut Transaction,
    catalog: &ApplicationCatalog,
    row: &Map<String, Value>,
) -> Result<()> {
    let run_id = nullable(row, "run_id")?;
    if run_id.as_deref().is_some_and(|run| !run.is_empty()) {
        return Ok(());
    }
    let witness = CountOrigin {
        generation: catalog.generation.clone(),
        thread_id: text(row, "thread_id")?,
        run_id,
    };
    let mut key = count_origin_prefix(catalog);
    key.extend_from_slice(request_digest(&json!(witness.thread_id)).as_bytes());
    if let Some(bytes) = tx.get(&key).await? {
        let previous: CountOrigin =
            serde_json::from_slice(&bytes).map_err(|_| Error::IncompatibleStore)?;
        if previous != witness {
            // Migration 85 GROUP BY keeps NULL and empty run IDs distinct,
            // then COALESCE maps both groups to the same unique count key.
            // Reject that source collision instead of silently summing it.
            return Err(Error::IncompatibleStore);
        }
    } else {
        tx.put(key, encode(&witness)?).await?;
    }
    Ok(())
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ImportLineage {
    version: u32,
    source: ApplicationCatalog,
}

fn lineage_key(target: &ApplicationCatalog) -> Vec<u8> {
    store_key(&format!(
        "application/generation/{}/metadata/native-schema-upgrade-80-88",
        target.generation
    ))
}

fn lineage_89_key(target: &ApplicationCatalog) -> Vec<u8> {
    store_key(&format!(
        "application/generation/{}/metadata/native-schema-upgrade-88-89",
        target.generation
    ))
}

fn target_89(source: &ApplicationCatalog, current: &Definition) -> ApplicationCatalog {
    ApplicationCatalog {
        generation: request_digest(&json!([
            "native-schema-upgrade-88-89",
            source.generation,
            current.schema
        ])),
        schema: current.schema.clone(),
    }
}

fn definition_89() -> Result<Definition> {
    let previous: Definition = serde_json::from_str(include_str!("omdb-schema-89.json"))
        .map_err(|_| Error::IncompatibleStore)?;
    let current = definition()?;
    if previous.format != 1
        || previous.migrations.len() != 89
        || current.migrations.len() != 91
        || current.migrations[..89] != previous.migrations
        || current.migrations[89].id != "90"
        || current.migrations[89].name != "AuthConnectDevices"
        || current.migrations[90].id != "91"
        || current.migrations[90].name != "AuthConnectDeniedRequests"
    {
        return Err(Error::IncompatibleStore);
    }
    validate_schema(&previous.schema)?;
    Ok(previous)
}

fn definition_88() -> Result<Definition> {
    let previous: Definition = serde_json::from_str(include_str!("omdb-schema-88.json"))
        .map_err(|_| Error::IncompatibleStore)?;
    let current = definition_89()?;
    if previous.format != 1
        || previous.migrations.len() != 88
        || current.migrations.len() != 89
        || current.migrations[..88] != previous.migrations
        || current.migrations[88].id != "89"
        || current.migrations[88].name != "ShellThreadReadIndexes"
        || previous.schema != current.schema
    {
        return Err(Error::IncompatibleStore);
    }
    validate_schema(&previous.schema)?;
    Ok(previous)
}

fn previous_definition() -> Result<Definition> {
    let previous: Definition = serde_json::from_str(include_str!("omdb-schema-80.json"))
        .map_err(|_| Error::IncompatibleStore)?;
    if previous.format != 1 {
        return Err(Error::IncompatibleStore);
    }
    validate_schema(&previous.schema)?;
    Ok(previous)
}

/// The archive receipt continues to identify the original, immutable export.
/// Only this known schema transition may establish its new active catalog.
async fn validate_80_lineage(
    tx: &mut Transaction,
    catalog: &ApplicationCatalog,
    imported_generation: &str,
    imported_table_count: u64,
) -> Result<()> {
    let lineage: ImportLineage = serde_json::from_slice(
        &tx.get(&lineage_key(catalog))
            .await?
            .ok_or(Error::IncompatibleStore)?,
    )
    .map_err(|_| Error::IncompatibleStore)?;
    let previous = previous_definition()?;
    let intermediate = definition_88()?;
    let current = definition_89()?;
    if lineage.version != 1
        || lineage.source.generation != imported_generation
        || lineage.source.schema != previous.schema
        || previous.schema.tables.len() as u64 != imported_table_count
        || catalog.schema != current.schema
        || catalog.generation
            != request_digest(&json!([
                "native-schema-upgrade",
                lineage.source.generation,
                current.schema
            ]))
    {
        return Err(Error::IncompatibleStore);
    }
    validate_history(tx, &lineage.source, &previous.migrations).await?;
    let actual = history(tx, catalog, current.migrations.len()).await?;
    // Omdb inspects the immutable receipt before taking its exclusive upgrade
    // handle. Exactly the retained 88 target and the current 89 target are valid.
    if actual != intermediate.migrations && actual != current.migrations {
        return Err(Error::IncompatibleStore);
    }
    Ok(())
}

async fn history(
    tx: &mut Transaction,
    owner: &ApplicationCatalog,
    maximum: usize,
) -> Result<Vec<Migration>> {
    let prefix = row_prefix(&owner.generation, "effect_sql_migrations")?;
    let rows = tx.scan(&prefix, &end(&prefix), maximum + 1).await?;
    if rows.len() > maximum {
        return Err(Error::IncompatibleStore);
    }
    let mut actual = Vec::new();
    for (key, bytes) in rows {
        let row = decode_row(owner, table(owner, "effect_sql_migrations")?, &key, &bytes)?;
        actual.push(Migration {
            id: integer(&row.row, "migration_id")?.to_string(),
            name: text(&row.row, "name")?,
        });
    }
    actual.sort_by_key(|entry| entry.id.parse::<i64>().unwrap_or(i64::MAX));
    Ok(actual)
}

async fn validate_history(
    tx: &mut Transaction,
    owner: &ApplicationCatalog,
    expected: &[Migration],
) -> Result<()> {
    if history(tx, owner, expected.len()).await? != expected {
        return Err(Error::IncompatibleStore);
    }
    Ok(())
}

async fn validate_legacy_import_lineage(
    tx: &mut Transaction,
    catalog: &ApplicationCatalog,
    imported_generation: &str,
    imported_table_count: u64,
) -> Result<()> {
    let Some(bytes) = tx.get(&lineage_89_key(catalog)).await? else {
        return validate_80_lineage(tx, catalog, imported_generation, imported_table_count).await;
    };
    let lineage: ImportLineage =
        serde_json::from_slice(&bytes).map_err(|_| Error::IncompatibleStore)?;
    let previous = definition_88()?;
    let current = definition_89()?;
    if lineage.version != 1
        || lineage.source.schema != previous.schema
        || *catalog != target_89(&lineage.source, &current)
        || lineage.source.generation == catalog.generation
    {
        return Err(Error::IncompatibleStore);
    }
    validate_history(tx, catalog, &current.migrations).await?;
    validate_history(tx, &lineage.source, &previous.migrations).await?;
    if lineage.source.generation == imported_generation {
        if previous.schema.tables.len() as u64 != imported_table_count {
            return Err(Error::IncompatibleStore);
        }
        Ok(())
    } else {
        // There is one supported predecessor chain, never an unbounded walk of
        // caller-provided generation links: original 80 -> retained 88 -> 89.
        validate_80_lineage(
            tx,
            &lineage.source,
            imported_generation,
            imported_table_count,
        )
        .await
    }
}

fn lineage_91_key(target: &ApplicationCatalog) -> Vec<u8> {
    store_key(&format!(
        "application/generation/{}/metadata/native-schema-upgrade-to-91",
        target.generation
    ))
}

fn target_91(source: &ApplicationCatalog, current: &Definition) -> ApplicationCatalog {
    ApplicationCatalog {
        generation: request_digest(&json!([
            "native-schema-upgrade-to-91",
            source.generation,
            current.schema
        ])),
        schema: current.schema.clone(),
    }
}

async fn source_definition(
    tx: &mut Transaction,
    source: &ApplicationCatalog,
) -> Result<Definition> {
    let actual = history(tx, source, 89).await?;
    let oldest = previous_definition()?;
    let intermediate = definition_88()?;
    let latest = definition_89()?;
    if latest.migrations[..80] != oldest.migrations {
        return Err(Error::IncompatibleStore);
    }
    [oldest, intermediate, latest]
        .into_iter()
        .find(|known| source.schema == known.schema && actual == known.migrations)
        .ok_or(Error::IncompatibleStore)
}

/// A bounded receipt chain: an original 80/88/89 import may have first passed
/// through the two previously shipped upgrades. Only those exact catalogs and
/// histories can precede this release's schema-91 publication.
pub(crate) async fn validate_import_lineage(
    tx: &mut Transaction,
    catalog: &ApplicationCatalog,
    imported_generation: &str,
    imported_table_count: u64,
) -> Result<()> {
    let current = definition()?;
    if catalog.schema != current.schema {
        return validate_legacy_import_lineage(
            tx,
            catalog,
            imported_generation,
            imported_table_count,
        )
        .await;
    }
    let lineage: ImportLineage = serde_json::from_slice(
        &tx.get(&lineage_91_key(catalog))
            .await?
            .ok_or(Error::IncompatibleStore)?,
    )
    .map_err(|_| Error::IncompatibleStore)?;
    if lineage.version != 1
        || *catalog != target_91(&lineage.source, &current)
        || lineage.source.generation == catalog.generation
    {
        return Err(Error::IncompatibleStore);
    }
    validate_history(tx, catalog, &current.migrations).await?;
    let previous = source_definition(tx, &lineage.source).await?;
    if lineage.source.generation == imported_generation {
        if previous.schema.tables.len() as u64 != imported_table_count {
            return Err(Error::IncompatibleStore);
        }
        Ok(())
    } else {
        validate_legacy_import_lineage(
            tx,
            &lineage.source,
            imported_generation,
            imported_table_count,
        )
        .await
    }
}

#[derive(Clone, Copy)]
enum Stage {
    Clear,
    Copy,
    Messages,
    Projects,
    ProjectEvents,
    Counts,
    ClearCountOrigins,
    AuthSessions,
    AuthLinks,
}
impl Stage {
    fn table(self) -> Option<&'static str> {
        match self {
            Self::Messages => Some(MESSAGES),
            Self::Projects => Some(PROJECTS),
            Self::ProjectEvents => Some(EVENTS),
            Self::Counts => Some(ITEMS),
            Self::AuthSessions => Some(SESSIONS),
            Self::AuthLinks => Some(LINKS),
            _ => None,
        }
    }
}

async fn transform(
    tx: &mut Transaction,
    source: &ApplicationCatalog,
    target: &ApplicationCatalog,
    stage: Stage,
    key: &[u8],
    bytes: &[u8],
) -> Result<()> {
    let name = stage.table().ok_or(Error::IncompatibleStore)?;
    let old = decode_row(source, table(source, name)?, key, bytes)?;
    match stage {
        Stage::Messages => {
            let raw = text(&old.row, "payload_json")?;
            // Like migration 81, malformed noncitation payloads remain auditable.
            let search = if raw.to_ascii_lowercase().contains("t3-citation://") {
                let payload: Value =
                    serde_json::from_str(&raw).map_err(|_| Error::IncompatibleStore)?;
                payload
                    .get("text")
                    .and_then(Value::as_str)
                    .and_then(crate::application_message_search_text::readable_search_text)
            } else {
                None
            };
            let mut row = old.row.clone();
            row.insert("search_text".into(), nullable_cell(search.as_deref()));
            if search.is_some() {
                let mut before = old.row;
                before.insert("search_text".into(), nullable_cell(None));
                let rowid = crate::application_message_rowids::upgrade_rowid(
                    tx,
                    target,
                    &text(&row, "message_id")?,
                )
                .await?;
                crate::application_search::replace_message(
                    tx,
                    target,
                    Some(&before),
                    Some(&row),
                    rowid,
                )
                .await?;
            }
            tx.put(
                row_key(&target.generation, name, &old.key)?,
                encode_row(&ApplicationRow { key: old.key, row })?,
            )
            .await?;
        }
        Stage::Projects => {
            let mut row = old.row;
            row.insert("has_custom_title".into(), int_cell(0));
            tx.put(
                row_key(&target.generation, name, &old.key)?,
                encode_row(&ApplicationRow { key: old.key, row })?,
            )
            .await?;
        }
        Stage::ProjectEvents => {
            if text(&old.row, "aggregate_kind")? == "project"
                && text(&old.row, "event_type")? == "project.meta-updated"
            {
                let payload: Value = serde_json::from_str(&text(&old.row, "payload_json")?)
                    .map_err(|_| Error::IncompatibleStore)?;
                if payload.get("title").is_some_and(Value::is_string) {
                    if let Some(mut project) = read_row(
                        tx,
                        target,
                        PROJECTS,
                        &[text_cell(&text(&old.row, "stream_id")?)],
                    )
                    .await?
                    {
                        project.row.insert("has_custom_title".into(), int_cell(1));
                        write_row(tx, target, PROJECTS, project.row).await?;
                    }
                }
            }
        }
        Stage::Counts => {
            validate_count_origin(tx, target, &old.row).await?;
            crate::application_turn_item_counts::replace(tx, target, None, Some(&old.row)).await?
        }
        Stage::AuthSessions | Stage::AuthLinks => {
            let mut row = old.row;
            if matches!(stage, Stage::AuthSessions) {
                row.insert("proof_key_thumbprint".into(), nullable_cell(None));
            } else {
                row.insert("requires_connect_approval".into(), int_cell(0));
                row.insert("requested_client_json".into(), nullable_cell(None));
                row.insert("connect_denied".into(), int_cell(0));
            }
            crate::application::validate_row(table(target, name)?, &old.key, &row)?;
            tx.put(
                row_key(&target.generation, name, &old.key)?,
                encode_row(&ApplicationRow { key: old.key, row })?,
            )
            .await?;
        }
        Stage::Clear | Stage::Copy | Stage::ClearCountOrigins => return Err(Error::IncompatibleStore),
    }
    Ok(())
}

impl Store {
    /// Older schema-91 publication claimed the count table before the first
    /// projection preparation. Accept that interrupted state only for the
    /// exact published target and retained, supported source history.
    pub(crate) async fn published_projection_count_owner(
        tx: &mut Transaction,
        catalog: &ApplicationCatalog,
    ) -> Result<bool> {
        let Some(bytes) = tx.get(&lineage_91_key(catalog)).await? else {
            return Ok(false);
        };
        let lineage: ImportLineage =
            serde_json::from_slice(&bytes).map_err(|_| Error::IncompatibleStore)?;
        let current = definition()?;
        if lineage.version != 1
            || *catalog != target_91(&lineage.source, &current)
            || lineage.source.generation == catalog.generation
        {
            return Err(Error::IncompatibleStore);
        }
        validate_history(tx, catalog, &current.migrations).await?;
        source_definition(tx, &lineage.source).await?;
        Ok(true)
    }

    pub(super) async fn upgrade_application_91(
        &self,
        source: ApplicationCatalog,
        current: Definition,
    ) -> Result<BootstrapResult> {
        let mut inspection = self.db.begin_read_only().await?;
        let previous = source_definition(&mut inspection, &source).await?;
        inspection.rollback().await?;
        self.db.require_exclusive()?;
        let target = target_91(&source, &current);
        for stage in [Stage::Clear, Stage::Copy] {
            self.upgrade_stage(&source, &target, stage).await?;
        }
        if previous.migrations.len() == 80 {
            for stage in [
                Stage::Messages,
                Stage::Projects,
                Stage::ProjectEvents,
                Stage::Counts,
                Stage::ClearCountOrigins,
            ] {
                self.upgrade_stage(&source, &target, stage).await?;
            }
        }
        for stage in [Stage::AuthSessions, Stage::AuthLinks] {
            self.upgrade_stage(&source, &target, stage).await?;
        }
        let lineage = lineage_91_key(&target);
        self.publish_upgrade(
            source,
            target,
            current.migrations,
            previous.migrations.len(),
            lineage,
        )
        .await
    }

    async fn upgrade_batch(
        &self,
        source: &ApplicationCatalog,
        target: &ApplicationCatalog,
        stage: Stage,
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<(bool, Option<Vec<u8>>)> {
        let mut tx = self
            .db
            .begin_reserved(Duration::from_secs(5), Duration::from_secs(300))
            .await?;
        if decode_catalog(
            &tx.get(&schema_key())
                .await?
                .ok_or(Error::IncompatibleStore)?,
        )? != *source
        {
            return Err(Error::IncompatibleStore);
        }
        let scan_prefix = match stage {
            Stage::Clear => prefix(&target.generation),
            Stage::Copy => prefix(&source.generation),
            Stage::ClearCountOrigins => count_origin_prefix(target),
            _ => row_prefix(
                &source.generation,
                stage.table().ok_or(Error::IncompatibleStore)?,
            )?,
        };
        let mut cursor = after
            .map(<[u8]>::to_vec)
            .unwrap_or_else(|| scan_prefix.clone());
        if after.is_some() {
            cursor.push(0);
        }
        let page = tx.scan(&cursor, &end(&scan_prefix), limit).await?;
        let done = page.len() < limit;
        let mut last = None;
        for (key, bytes) in page {
            match stage {
                Stage::Clear | Stage::ClearCountOrigins => tx.delete(key.clone()).await?,
                Stage::Copy => {
                    let suffix = key
                        .strip_prefix(scan_prefix.as_slice())
                        .ok_or(Error::IncompatibleStore)?;
                    // Count groups enumerate canonical migration-85 rows. Their
                    // preparation witness belongs to the source generation;
                    // rebuild it after all target count rows are staged.
                    if suffix.starts_with(crate::application_turn_item_counts::INDEX_NAMESPACE)
                        || suffix.starts_with(COUNT_ORIGIN_NAMESPACE)
                    {
                        last = Some(key);
                        continue;
                    }
                    // Authentication gained proof-key indexes and columns. Its
                    // old preparation witness cannot qualify the staged catalog.
                    let auth_changed =
                        table(source, SESSIONS)?.columns != table(target, SESSIONS)?.columns;
                    if auth_changed
                        && (suffix.starts_with(b"auth/v1/")
                            || key == managed_marker(source, SESSIONS)
                            || key == managed_marker(source, LINKS))
                    {
                        last = Some(key);
                        continue;
                    }
                    // Rebuild all relationship, count and compact-window indexes.
                    // Retain their insertion orders so historical SQL ordering survives.
                    if !suffix.starts_with(b"projection-reads/v1/")
                        || suffix.starts_with(b"projection-reads/v1/order/")
                        || suffix.starts_with(b"projection-reads/v1/next-order/")
                    {
                        let mut destination = prefix(&target.generation);
                        destination.extend_from_slice(suffix);
                        tx.put(destination, bytes).await?;
                    }
                }
                _ => transform(&mut tx, source, target, stage, &key, &bytes).await?,
            }
            last = Some(key);
        }
        tx.commit().await?;
        Ok((done, last))
    }

    async fn upgrade_stage(
        &self,
        source: &ApplicationCatalog,
        target: &ApplicationCatalog,
        stage: Stage,
    ) -> Result<()> {
        if source.generation == target.generation {
            return Err(Error::IncompatibleStore);
        }
        let mut after = None;
        let mut limit = PAGE;
        loop {
            match self
                .upgrade_batch(source, target, stage, after.as_deref(), limit)
                .await
            {
                Ok((true, _)) => return Ok(()),
                Ok((false, next)) => after = next,
                Err(Error::ResourceLimit(_)) if limit > 1 => limit = (limit / 2).max(1),
                Err(error) => return Err(error),
            }
        }
    }

    #[cfg(test)]
    pub(super) async fn upgrade_application_80(
        &self,
        source: ApplicationCatalog,
        current: Definition,
    ) -> Result<BootstrapResult> {
        let previous = previous_definition()?;
        if source.schema != previous.schema
            || self.application_migrations().await? != previous.migrations
        {
            return Err(Error::Invalid("Native application schema or migration history is unsupported by this build; the existing generation was retained".into()));
        }
        self.db.require_exclusive()?;
        let target = ApplicationCatalog {
            generation: request_digest(&json!([
                "native-schema-upgrade",
                source.generation,
                current.schema
            ])),
            schema: current.schema.clone(),
        };
        for stage in [
            Stage::Clear,
            Stage::Copy,
            Stage::Messages,
            Stage::Projects,
            Stage::ProjectEvents,
            Stage::Counts,
            Stage::ClearCountOrigins,
        ] {
            self.upgrade_stage(&source, &target, stage).await?;
        }
        let lineage = lineage_key(&target);
        self.publish_upgrade(
            source,
            target,
            current.migrations,
            previous.migrations.len(),
            lineage,
        )
        .await
    }

    #[cfg(test)]
    pub(super) async fn upgrade_application_88(
        &self,
        source: ApplicationCatalog,
        current: Definition,
    ) -> Result<BootstrapResult> {
        let previous = definition_88()?;
        if source.schema != previous.schema
            || self.application_migrations().await? != previous.migrations
        {
            return Err(Error::Invalid("Native application schema or migration history is unsupported by this build; the existing generation was retained".into()));
        }
        self.db.require_exclusive()?;
        let target = target_89(&source, &current);
        // This transition changes indexes only. Copy all canonical rows, search
        // identities and turn counts unchanged; rebuild read indexes in staging.
        // An interrupted attempt is discarded before copying any newer 88 writes.
        for stage in [Stage::Clear, Stage::Copy] {
            self.upgrade_stage(&source, &target, stage).await?;
        }
        let lineage = lineage_89_key(&target);
        self.publish_upgrade(
            source,
            target,
            current.migrations,
            previous.migrations.len(),
            lineage,
        )
        .await
    }

    async fn publish_upgrade(
        &self,
        source: ApplicationCatalog,
        target: ApplicationCatalog,
        migrations: Vec<Migration>,
        previous_migrations: usize,
        lineage: Vec<u8>,
    ) -> Result<BootstrapResult> {
        while !self
            .prepare_projection_reads_for_catalog(&target)
            .await?
            .prepared
        {}
        let mut tx = self
            .db
            .begin_reserved(Duration::from_secs(5), Duration::from_secs(300))
            .await?;
        if decode_catalog(
            &tx.get(&schema_key())
                .await?
                .ok_or(Error::IncompatibleStore)?,
        )? != source
        {
            return Err(Error::IncompatibleStore);
        }
        crate::application_projections::validate_item_owner(&mut tx, &target).await?;
        crate::application_turn_item_counts::ensure_ready(&mut tx, &target).await?;
        for migration in migrations.iter().skip(previous_migrations) {
            write_row(
                &mut tx,
                &target,
                "effect_sql_migrations",
                Map::from_iter([
                    (
                        "migration_id".into(),
                        int_cell(migration.id.parse().map_err(|_| Error::IncompatibleStore)?),
                    ),
                    ("name".into(), text_cell(&migration.name)),
                    ("created_at".into(), text_cell("1970-01-01 00:00:00")),
                ]),
            )
            .await?;
        }
        let header_key = store_key(&format!(
            "application/generation/{}/metadata/header",
            target.generation
        ));
        if let Some(bytes) = tx.get(&header_key).await? {
            let mut header: Value =
                serde_json::from_slice(&bytes).map_err(|_| Error::IncompatibleStore)?;
            header["migrations"] =
                serde_json::to_value(&migrations).map_err(|_| Error::IncompatibleStore)?;
            tx.put(header_key, encode(&header)?).await?;
        }
        tx.put(lineage, encode(&ImportLineage { version: 1, source })?)
            .await?;
        // The sole publish point: canonical rows, index backfills and migration
        // history become visible together. The source generation stays intact.
        tx.put(schema_key(), encode_catalog(&target)?).await?;
        tx.commit().await?;
        Ok(BootstrapResult {
            initialized: false,
            migrations,
            schema_version: 1,
        })
    }
}

#[cfg(test)]
#[path = "redb_schema_upgrade_tests.rs"]
mod tests;
