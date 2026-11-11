//! Resumable native migration 81. A catalog fence excludes both old and new
//! application writers until every row and its exact FTS transition is durable.
//! Each page and its cursor commit together; no preserved SQLite file is read.

use super::{Definition, Migration, definition, read_migrations};
use crate::application::{
    ApplicationCatalog, decode_catalog, decode_row, encode_catalog, encode_row, row_prefix,
    schema_key, table, validate_row, validate_schema,
};
use crate::application_rows::{int_cell, nullable_cell, text, text_cell, write_row};
use crate::{Error, Result, Store, request_digest, store_key};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::time::Duration;

const MESSAGES: &str = "orchestration_v2_projection_messages";
const FORMAT: &str = "t3code-omdb/schema-upgrade-81/v1";

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Progress {
    format: String,
    original: ApplicationCatalog,
    target: String,
    after: Option<String>,
    upgraded_rows: u64,
}

fn legacy_definition(target: &Definition) -> Result<Definition> {
    let legacy: Definition = serde_json::from_str(include_str!("omdb-schema-v80.json"))
        .map_err(|_| Error::IncompatibleStore)?;
    validate_schema(&legacy.schema)?;
    let mut expected = legacy.schema.clone();
    expected
        .tables
        .iter_mut()
        .find(|table| table.name == MESSAGES)
        .ok_or(Error::IncompatibleStore)?
        .columns
        .push("search_text".into());
    if legacy.format != 1
        || legacy.migrations.len() != 80
        || target.schema != expected
        || target.migrations.len() != 81
        || target.migrations[..80] != legacy.migrations
        || target.migrations[80]
            != (Migration {
                id: "81".into(),
                name: "ProjectionMessageSearchText".into(),
            })
    {
        return Err(Error::IncompatibleStore);
    }
    Ok(legacy)
}

fn decode_progress(bytes: &[u8], target: &Definition) -> Result<Progress> {
    let progress: Progress = serde_json::from_slice(bytes).map_err(|_| Error::IncompatibleStore)?;
    if progress.format != FORMAT
        || progress.original.schema != legacy_definition(target)?.schema
        || progress.target != request_digest(&json!(target.schema))
        || progress.after.as_ref().is_some_and(|after| {
            after.len() != 64
                || !after
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        })
        || (progress.after.is_none() != (progress.upgraded_rows == 0))
    {
        return Err(Error::IncompatibleStore);
    }
    // Includes strict generation and schema validation without accepting a fence.
    encode_catalog(&progress.original)?;
    Ok(progress)
}

pub(super) fn pending_catalog(bytes: &[u8]) -> Result<ApplicationCatalog> {
    Ok(decode_progress(bytes, &definition()?)?.original)
}

fn header_key(catalog: &ApplicationCatalog) -> Vec<u8> {
    store_key(&format!(
        "application/generation/{}/metadata/header",
        catalog.generation
    ))
}

async fn stage_message(
    tx: &mut crate::backend::Transaction,
    original: &ApplicationCatalog,
    upgraded: &ApplicationCatalog,
    key: &[u8],
    bytes: &[u8],
) -> Result<()> {
    let old = decode_row(original, table(original, MESSAGES)?, key, bytes)?;
    let mut next = old.clone();
    let raw = text(&old.row, "payload_json")?;
    // Match migration 81's raw SQL LIKE filter before decoding JSON. Escaped
    // protocol slashes deliberately do not get an eager extra backfill.
    let readable = if !text(&old.row, "message_id")?.is_empty()
        && raw
            .split('\0')
            .next()
            .unwrap_or("")
            .to_ascii_lowercase()
            .contains("t3-citation://")
    {
        let value: Value = serde_json::from_str(&raw).map_err(|_| Error::IncompatibleStore)?;
        value
            .get("text")
            .and_then(Value::as_str)
            .and_then(crate::application_search::readable_search_text)
    } else {
        None
    };
    next.row
        .insert("search_text".into(), nullable_cell(readable.as_deref()));
    validate_row(table(upgraded, MESSAGES)?, &next.key, &next.row)?;
    let encoded = encode_row(&next)?;
    if readable.is_some() && crate::application_search::has_index(tx, original).await? {
        let rowid = crate::application_message_rowids::migration_rowid(tx, original, &old).await?;
        crate::application_search::replace_message(
            tx,
            original,
            Some(&old.row),
            Some(&next.row),
            rowid,
        )
        .await?;
    }
    tx.put(key.to_vec(), encoded).await
}

async fn stage_activation(
    tx: &mut crate::backend::Transaction,
    original: &ApplicationCatalog,
    upgraded: &ApplicationCatalog,
    target: &Definition,
) -> Result<()> {
    if read_migrations(tx, original).await? != legacy_definition(target)?.migrations {
        return Err(Error::IncompatibleStore);
    }
    write_row(
        tx,
        upgraded,
        "effect_sql_migrations",
        serde_json::Map::from_iter([
            ("migration_id".into(), int_cell(81)),
            ("name".into(), text_cell("ProjectionMessageSearchText")),
            ("created_at".into(), text_cell("1970-01-01 00:00:00")),
        ]),
    )
    .await?;
    if let Some(bytes) = tx.get(&header_key(upgraded)).await? {
        let mut header: Value =
            serde_json::from_slice(&bytes).map_err(|_| Error::IncompatibleStore)?;
        if !header.is_object() {
            return Err(Error::IncompatibleStore);
        }
        header["migrations"] = json!(target.migrations);
        tx.put(
            header_key(upgraded),
            serde_json::to_vec(&header).map_err(|_| Error::IncompatibleStore)?,
        )
        .await?;
    }
    tx.put(schema_key(), encode_catalog(upgraded)?).await
}

async fn preflight(
    store: &Store,
    owner: &mut crate::backend::Transaction,
    progress: &Progress,
    target: &Definition,
) -> Result<()> {
    owner.require_write()?;
    let upgraded = ApplicationCatalog {
        schema: target.schema.clone(),
        generation: progress.original.generation.clone(),
    };
    let checkpoint = owner.checkpoint();
    // Include finalization and the widest resumable cursor in each single-row
    // capacity check. Restoring this small checkpoint bounds observations too.
    stage_activation(owner, &progress.original, &upgraded, target).await?;
    let worst_cursor = Progress {
        format: progress.format.clone(),
        original: progress.original.clone(),
        target: progress.target.clone(),
        after: Some("f".repeat(64)),
        upgraded_rows: u64::MAX,
    };
    owner
        .put(
            schema_key(),
            serde_json::to_vec(&worst_cursor).map_err(|_| Error::IncompatibleStore)?,
        )
        .await?;
    let capacity_checkpoint = owner.checkpoint();
    // The reserved writer stays held throughout. This separate immutable reader
    // pages the source without accumulating a whole database's conflict reads.
    let mut reader = store.db.begin_read_only().await?;
    let prefix = row_prefix(&progress.original.generation, MESSAGES)?;
    let mut end = prefix.clone();
    *end.last_mut().ok_or(Error::IncompatibleStore)? += 1;
    let mut cursor = prefix.clone();
    loop {
        owner.require_write()?;
        let page = reader.scan(&cursor, &end, 1).await?;
        let Some((key, bytes)) = page.into_iter().next() else {
            break;
        };
        if owner.get(&key).await?.as_deref() != Some(bytes.as_slice()) {
            return Err(Error::IncompatibleStore);
        }
        // Exercise the real per-row index mutation and finalization budgets,
        // then discard all staged writes. A large readable-text expansion or a
        // missing rowid witness must fail before the durable fence exists.
        stage_message(owner, &progress.original, &upgraded, &key, &bytes).await?;
        owner.restore(&capacity_checkpoint);
        cursor = key;
        cursor.push(0);
    }
    owner.restore(&checkpoint);
    reader.rollback().await?;
    owner.require_write()
}

pub(super) async fn upgrade(store: &Store, target: &Definition) -> Result<()> {
    let mut limit = 64;
    loop {
        match upgrade_page(store, target, limit).await {
            Err(Error::ResourceLimit(_)) if limit > 1 => limit = (limit / 2).max(1),
            Err(error) => return Err(error),
            Ok(true) => return Ok(()),
            Ok(false) => tokio::task::yield_now().await,
        }
    }
}

/// One bounded transaction, also used by restart tests to stop at real durable
/// boundaries. Publishing the final catalog is the only activation operation.
async fn upgrade_page(store: &Store, target: &Definition, limit: usize) -> Result<bool> {
    let legacy = legacy_definition(target)?;
    if limit == 0 || limit > 64 {
        return Err(Error::IncompatibleStore);
    }
    let mut tx = store
        .db
        .begin_reserved(Duration::from_secs(5), Duration::from_secs(300))
        .await?;
    let bytes = tx
        .get(&schema_key())
        .await?
        .ok_or(Error::IncompatibleStore)?;
    let mut progress = if let Ok(catalog) = decode_catalog(&bytes) {
        if catalog.schema == target.schema {
            if read_migrations(&mut tx, &catalog).await? != target.migrations {
                return Err(Error::IncompatibleStore);
            }
            tx.rollback().await?;
            return Ok(true);
        }
        if catalog.schema != legacy.schema
            || read_migrations(&mut tx, &catalog).await? != legacy.migrations
        {
            return Err(Error::Invalid("Native schema or migration history has no supported in-place upgrade; the existing store was retained".into()));
        }
        let progress = Progress {
            format: FORMAT.into(),
            original: catalog,
            target: request_digest(&json!(target.schema)),
            after: None,
            upgraded_rows: 0,
        };
        preflight(store, &mut tx, &progress, target).await.map_err(|error| Error::Invalid(format!(
            "Native migration 81 preflight failed before changing this database: {error}. Keep the authoritative native file and use its matching previous build until this row size or metadata is supported; do not replace it from preserved SQLite"
        )))?;
        tx.put(
            schema_key(),
            serde_json::to_vec(&progress).map_err(|_| Error::IncompatibleStore)?,
        )
        .await?;
        tx.commit().await?;
        return Ok(false);
    } else {
        decode_progress(&bytes, target)?
    };
    let upgraded = ApplicationCatalog {
        schema: target.schema.clone(),
        generation: progress.original.generation.clone(),
    };
    let prefix = row_prefix(&progress.original.generation, MESSAGES)?;
    let mut end = prefix.clone();
    *end.last_mut().ok_or(Error::IncompatibleStore)? += 1;
    let mut start = prefix.clone();
    if let Some(after) = &progress.after {
        start.extend_from_slice(after.as_bytes());
        start.push(0);
    }
    let page = tx.scan(&start, &end, limit).await?;
    let count = page.len();
    for (key, bytes) in page {
        stage_message(&mut tx, &progress.original, &upgraded, &key, &bytes).await?;
        progress.after = Some(
            std::str::from_utf8(
                key.strip_prefix(prefix.as_slice())
                    .ok_or(Error::IncompatibleStore)?,
            )
            .map_err(|_| Error::IncompatibleStore)?
            .to_owned(),
        );
        progress.upgraded_rows = progress
            .upgraded_rows
            .checked_add(1)
            .ok_or(Error::IncompatibleStore)?;
    }
    if count == limit {
        tx.put(
            schema_key(),
            serde_json::to_vec(&progress).map_err(|_| Error::IncompatibleStore)?,
        )
        .await?;
    } else {
        stage_activation(&mut tx, &progress.original, &upgraded, target).await?;
    }
    tx.commit().await?;
    Ok(count < limit)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::{ApplicationTransactionMode, ApplicationTransactions, row_key};
    use crate::application_rows::{nullable, read_row};
    use crate::application_search::ThreadSearchQuery;
    use crate::test_support::TestFixture;
    use serde_json::Map;

    const CITATION: &str = "before [Assistant quote](t3-citation://v1/env/thread/message?text=cedar%20moth&start=0&end=10&prefix=&suffix=&comment=branch%20comment) after";

    fn empty_row(catalog: &ApplicationCatalog, name: &str) -> Map<String, Value> {
        table(catalog, name)
            .unwrap()
            .columns
            .iter()
            .map(|column| (column.clone(), nullable_cell(None)))
            .collect()
    }

    async fn seed_legacy(store: &Store, ready: bool) -> ApplicationCatalog {
        let legacy = legacy_definition(&definition().unwrap()).unwrap();
        let catalog = ApplicationCatalog {
            schema: legacy.schema,
            generation: "native".into(),
        };
        let mut tx = store
            .db
            .begin_reserved(Duration::from_secs(5), Duration::from_secs(300))
            .await
            .unwrap();
        tx.put(schema_key(), encode_catalog(&catalog).unwrap())
            .await
            .unwrap();
        for migration in &legacy.migrations {
            write_row(
                &mut tx,
                &catalog,
                "effect_sql_migrations",
                Map::from_iter([
                    (
                        "migration_id".into(),
                        int_cell(migration.id.parse().unwrap()),
                    ),
                    ("name".into(), text_cell(&migration.name)),
                    ("created_at".into(), text_cell("2026-11-10 00:00:00")),
                ]),
            )
            .await
            .unwrap();
        }
        tx.put(
            header_key(&catalog),
            serde_json::to_vec(&json!({
                "version":2,"searchIndex":true,"searchDeleteWitness":true,
                "migrations":legacy.migrations,
            }))
            .unwrap(),
        )
        .await
        .unwrap();
        if ready {
            tx.put(
                store_key("application/generation/native/message-rowids/v1/preparation"),
                serde_json::to_vec(&json!({"after":null,"indexed_rows":0,"ready":true})).unwrap(),
            )
            .await
            .unwrap();
        }
        let mut project = empty_row(&catalog, "projection_projects");
        project.insert("project_id".into(), text_cell("project:upgrade"));
        write_row(&mut tx, &catalog, "projection_projects", project)
            .await
            .unwrap();
        let mut thread = empty_row(&catalog, "orchestration_v2_projection_threads");
        thread.insert("thread_id".into(), text_cell("thread:upgrade"));
        thread.insert("project_id".into(), text_cell("project:upgrade"));
        for (name, value) in [
            ("title", "Upgrade fixture"),
            ("default_provider", "codex"),
            ("runtime_mode", "full-access"),
            ("interaction_mode", "default"),
            ("created_at", "2026-11-10T00:00:00.000Z"),
            ("updated_at", "2026-11-10T00:00:00.000Z"),
        ] {
            thread.insert(name.into(), text_cell(value));
        }
        thread.insert("payload_json".into(), text_cell("{}"));
        write_row(
            &mut tx,
            &catalog,
            "orchestration_v2_projection_threads",
            thread,
        )
        .await
        .unwrap();
        for (index, (id, value, escaped)) in [
            ("native-only", "native state survives", false),
            ("citation", CITATION, false),
            ("escaped", CITATION, true),
        ]
        .into_iter()
        .enumerate()
        {
            let mut row = empty_row(&catalog, MESSAGES);
            for (name, value) in [
                ("message_id", id),
                ("thread_id", "thread:upgrade"),
                ("role", "user"),
                ("created_at", "2026-11-10T00:00:00.000Z"),
                ("updated_at", "2026-11-10T00:00:00.000Z"),
            ] {
                row.insert(name.into(), text_cell(value));
            }
            row.insert("streaming".into(), int_cell(0));
            let raw = json!({"id":id,"text":value,"createdBy":"user"}).to_string();
            let raw = if escaped {
                raw.replace("t3-citation://", "t3-citation:\\/\\/")
            } else {
                raw
            };
            row.insert("payload_json".into(), text_cell(&raw));
            let rowid = if ready {
                crate::application_message_rowids::replace(&mut tx, &catalog, None, Some(&row))
                    .await
                    .unwrap()
                    .unwrap()
            } else {
                let rowid = index as i64 + 1;
                let key = vec![text_cell(id)];
                let witness = store_key(&format!(
                    "application/generation/native/metadata/source/{}",
                    request_digest(&json!([MESSAGES, key]))
                ));
                tx.put(
                    witness,
                    serde_json::to_vec(
                        &json!({"table":MESSAGES,"key":key,"rowid":rowid.to_string()}),
                    )
                    .unwrap(),
                )
                .await
                .unwrap();
                rowid
            };
            crate::application_search::replace_message(&mut tx, &catalog, None, Some(&row), rowid)
                .await
                .unwrap();
            write_row(&mut tx, &catalog, MESSAGES, row).await.unwrap();
        }
        tx.put(
            store_key("upgrade-test/native-only-write"),
            b"preserve this native write".to_vec(),
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();
        catalog
    }

    #[tokio::test]
    async fn native_upgrade_resumes_after_a_page_and_preserves_search_and_native_writes() {
        let fixture = TestFixture::start().await;
        let store = fixture.initialize().await;
        let catalog = seed_legacy(&store, true).await;
        let target = definition().unwrap();
        let before = store
            .db
            .read(&row_key("native", MESSAGES, &[text_cell("native-only")]).unwrap())
            .await
            .unwrap()
            .unwrap();
        assert!(!upgrade_page(&store, &target, 1).await.unwrap());
        let fence = store.db.read(&schema_key()).await.unwrap().unwrap();
        // This is the exact serde shape accepted by the previous native binary.
        assert!(serde_json::from_slice::<ApplicationCatalog>(&fence).is_err());
        let mut application = ApplicationTransactions::new(store.clone());
        assert_eq!(application.schema().await.unwrap().unwrap(), catalog.schema);
        assert!(application.begin("blocked-during-upgrade").await.is_err());
        assert_eq!(store.application_migrations().await.unwrap().len(), 80);
        drop(application);
        assert!(!upgrade_page(&store, &target, 1).await.unwrap());
        store.shutdown().await.unwrap();
        let reopened = fixture.connect().await;
        assert!(!reopened.bootstrap_application().await.unwrap().initialized);
        assert_eq!(reopened.application_migrations().await.unwrap().len(), 81);
        assert_eq!(
            reopened
                .db
                .read(&store_key("upgrade-test/native-only-write"))
                .await
                .unwrap()
                .unwrap(),
            b"preserve this native write"
        );
        let final_catalog_bytes = reopened.db.read(&schema_key()).await.unwrap().unwrap();
        assert!(serde_json::from_slice::<ApplicationCatalog>(&final_catalog_bytes).is_err());
        let final_catalog = decode_catalog(&final_catalog_bytes).unwrap();
        let mut tx = reopened.db.begin_read_only().await.unwrap();
        let native = read_row(
            &mut tx,
            &final_catalog,
            MESSAGES,
            &[text_cell("native-only")],
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(
            text(&native.row, "payload_json").unwrap(),
            text(
                &decode_row(
                    &catalog,
                    table(&catalog, MESSAGES).unwrap(),
                    &row_key("native", MESSAGES, &[text_cell("native-only")]).unwrap(),
                    &before
                )
                .unwrap()
                .row,
                "payload_json"
            )
            .unwrap()
        );
        assert_eq!(nullable(&native.row, "search_text").unwrap(), None);
        let citation = read_row(&mut tx, &final_catalog, MESSAGES, &[text_cell("citation")])
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            nullable(&citation.row, "search_text").unwrap().as_deref(),
            Some("before cedar moth\nComment: branch comment after")
        );
        let escaped = read_row(&mut tx, &final_catalog, MESSAGES, &[text_cell("escaped")])
            .await
            .unwrap()
            .unwrap();
        assert_eq!(nullable(&escaped.row, "search_text").unwrap(), None);
        tx.rollback().await.unwrap();
        while !reopened
            .prepare_application_projection_reads()
            .await
            .unwrap()
            .prepared
        {}
        let mut application = ApplicationTransactions::new(reopened.clone());
        application
            .begin_mode("query", ApplicationTransactionMode::Read, Some(5_000))
            .await
            .unwrap();
        let hits = application
            .thread_search(
                "query",
                ThreadSearchQuery::MessageHits {
                    r#match: Some("\"cedar moth\"".into()),
                    substrings: "[]".into(),
                    short_spellings: "[]".into(),
                    project_id: None,
                    excluded_thread_ids: "[]".into(),
                },
            )
            .await
            .unwrap();
        assert_eq!(hits.as_array().unwrap().len(), 1);
        assert_eq!(hits[0]["messageId"], "citation");
        assert_eq!(
            hits[0]["text"],
            "before cedar moth\nComment: branch comment after"
        );
        application.rollback("query").await.unwrap();
        drop(application);
        assert!(!reopened.bootstrap_application().await.unwrap().initialized);
        reopened.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn imported_candidate_uses_witnesses_before_rowid_preparation() {
        let fixture = TestFixture::start().await;
        let store = fixture.initialize().await;
        seed_legacy(&store, false).await;
        store.bootstrap_application().await.unwrap();
        let catalog =
            decode_catalog(&store.db.read(&schema_key()).await.unwrap().unwrap()).unwrap();
        let mut tx = store.db.begin_read_only().await.unwrap();
        let row = read_row(&mut tx, &catalog, MESSAGES, &[text_cell("citation")])
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            nullable(&row.row, "search_text").unwrap().as_deref(),
            Some("before cedar moth\nComment: branch comment after")
        );
        tx.rollback().await.unwrap();
        store.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn wrong_old_migration_history_is_rejected_without_a_fence_or_row_changes() {
        let fixture = TestFixture::start().await;
        let store = fixture.initialize().await;
        let catalog = seed_legacy(&store, true).await;
        let mut tx = store
            .db
            .begin_reserved(Duration::from_secs(5), Duration::from_secs(300))
            .await
            .unwrap();
        write_row(
            &mut tx,
            &catalog,
            "effect_sql_migrations",
            Map::from_iter([
                ("migration_id".into(), int_cell(80)),
                ("name".into(), text_cell("unexpected")),
                ("created_at".into(), text_cell("2026-11-10 00:00:00")),
            ]),
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();
        let before = store.db.read(&schema_key()).await.unwrap();
        let message_key = row_key("native", MESSAGES, &[text_cell("citation")]).unwrap();
        let message_before = store.db.read(&message_key).await.unwrap();
        assert!(store.bootstrap_application().await.is_err());
        assert_eq!(store.db.read(&schema_key()).await.unwrap(), before);
        assert_eq!(store.db.read(&message_key).await.unwrap(), message_before);
        store.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn missing_rowid_witness_is_rejected_before_the_upgrade_fence() {
        let fixture = TestFixture::start().await;
        let store = fixture.initialize().await;
        seed_legacy(&store, false).await;
        let witness = store_key(&format!(
            "application/generation/native/metadata/source/{}",
            request_digest(&json!([MESSAGES, [text_cell("citation")]]))
        ));
        let mut tx = store
            .db
            .begin_reserved(Duration::from_secs(5), Duration::from_secs(300))
            .await
            .unwrap();
        tx.delete(witness).await.unwrap();
        tx.commit().await.unwrap();
        let before = store.db.read(&schema_key()).await.unwrap();
        let message_key = row_key("native", MESSAGES, &[text_cell("citation")]).unwrap();
        let message_before = store.db.read(&message_key).await.unwrap();
        let error = store
            .bootstrap_application()
            .await
            .err()
            .unwrap()
            .to_string();
        assert!(error.contains("preflight failed before changing this database"));
        assert_eq!(store.db.read(&schema_key()).await.unwrap(), before);
        assert_eq!(store.db.read(&message_key).await.unwrap(), message_before);
        store.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn oversized_readable_text_expansion_is_rejected_before_the_upgrade_fence() {
        let fixture = TestFixture::start().await;
        let store = fixture.initialize().await;
        let catalog = seed_legacy(&store, true).await;
        let mut tx = store
            .db
            .begin_reserved(Duration::from_secs(5), Duration::from_secs(300))
            .await
            .unwrap();
        let old = read_row(&mut tx, &catalog, MESSAGES, &[text_cell("citation")])
            .await
            .unwrap()
            .unwrap();
        let mut next = old.row.clone();
        let raw = json!({"id":"citation","text":format!("{}{CITATION}", "a".repeat(9 * 1024 * 1024)),"createdBy":"user"}).to_string();
        next.insert("payload_json".into(), text_cell(&raw));
        let rowid = crate::application_message_rowids::rowid(&mut tx, &catalog, "citation")
            .await
            .unwrap();
        crate::application_search::replace_message(
            &mut tx,
            &catalog,
            Some(&old.row),
            Some(&next),
            rowid,
        )
        .await
        .unwrap();
        write_row(&mut tx, &catalog, MESSAGES, next).await.unwrap();
        tx.commit().await.unwrap();
        let before = store.db.read(&schema_key()).await.unwrap();
        let message_key = row_key("native", MESSAGES, &[text_cell("citation")]).unwrap();
        let message_before = store.db.read(&message_key).await.unwrap();
        let error = store
            .bootstrap_application()
            .await
            .err()
            .unwrap()
            .to_string();
        assert!(error.contains("preflight failed before changing this database"));
        assert_eq!(store.db.read(&schema_key()).await.unwrap(), before);
        assert_eq!(store.db.read(&message_key).await.unwrap(), message_before);
        assert_eq!(store.application_migrations().await.unwrap().len(), 80);
        store.shutdown().await.unwrap();
    }
}
