use super::*;
use crate::application::{ApplicationTransactionMode, ApplicationTransactions};
use crate::application_rows::nullable;
use crate::test_support::TestFixture;

const NOW: &str = "2026-11-13T00:00:00.000Z";
const QUOTE: &str = "[Assistant quote](t3-citation://v1/env/thread/message?text=searchable+quoted+words&start=0&end=22&prefix=&suffix=&comment=review+this)";

async fn fixture_80() -> (TestFixture, Store, ApplicationCatalog) {
    let fixture = TestFixture::start().await;
    let store = fixture.initialize().await;
    let previous: Definition = serde_json::from_str(include_str!("omdb-schema-80.json")).unwrap();
    let source = ApplicationCatalog {
        schema: previous.schema,
        generation: "native".into(),
    };
    let mut tx = store.db.begin().await.unwrap();
    tx.put(schema_key(), encode_catalog(&source).unwrap())
        .await
        .unwrap();
    for migration in previous.migrations {
        write_row(
            &mut tx,
            &source,
            "effect_sql_migrations",
            Map::from_iter([
                (
                    "migration_id".into(),
                    int_cell(migration.id.parse().unwrap()),
                ),
                ("name".into(), text_cell(&migration.name)),
                ("created_at".into(), text_cell(NOW)),
            ]),
        )
        .await
        .unwrap();
    }
    let mut project: Map<String, Value> = table(&source, PROJECTS)
        .unwrap()
        .columns
        .iter()
        .map(|column| (column.clone(), nullable_cell(None)))
        .collect();
    for (column, value) in [
        ("project_id", "project"),
        ("title", "User title"),
        ("workspace_root", "/fixture"),
        ("scripts_json", "[]"),
        ("created_at", NOW),
        ("updated_at", NOW),
    ] {
        project.insert(column.into(), text_cell(value));
    }
    project.insert("auto_pull".into(), int_cell(0));
    write_row(&mut tx, &source, PROJECTS, project)
        .await
        .unwrap();
    let mut event: Map<String, Value> = table(&source, EVENTS)
        .unwrap()
        .columns
        .iter()
        .map(|column| (column.clone(), nullable_cell(None)))
        .collect();
    for (column, value) in [
        ("sequence", 1),
        ("stream_version", 1),
        ("application_event_version", 1),
    ] {
        event.insert(column.into(), int_cell(value));
    }
    for (column, value) in [
        ("event_id", "project-title"),
        ("aggregate_kind", "project"),
        ("stream_id", "project"),
        ("event_type", "project.meta-updated"),
        ("occurred_at", NOW),
        ("actor_kind", "user"),
        ("payload_json", "{\"title\":\"User title\"}"),
        ("metadata_json", "{}"),
    ] {
        event.insert(column.into(), text_cell(value));
    }
    write_row(&mut tx, &source, EVENTS, event).await.unwrap();
    let mut message: Map<String, Value> = table(&source, MESSAGES)
        .unwrap()
        .columns
        .iter()
        .map(|column| (column.clone(), nullable_cell(None)))
        .collect();
    for (column, value) in [
        ("message_id", "message"),
        ("thread_id", "thread"),
        ("role", "assistant"),
        ("created_at", NOW),
        ("updated_at", NOW),
    ] {
        message.insert(column.into(), text_cell(value));
    }
    message.insert("streaming".into(), int_cell(0));
    message.insert("payload_json".into(), text_cell(&json!({"id":"message","threadId":"thread","text":QUOTE,"role":"assistant","streaming":false}).to_string()));
    write_row(&mut tx, &source, MESSAGES, message)
        .await
        .unwrap();
    for (index, run) in [(1, Some("run")), (2, Some("run")), (3, None)] {
        let mut item: Map<String, Value> = table(&source, ITEMS)
            .unwrap()
            .columns
            .iter()
            .map(|column| (column.clone(), nullable_cell(None)))
            .collect();
        for (column, value) in [
            ("turn_item_id", format!("item-{index}")),
            ("thread_id", "thread".into()),
            ("type", "assistant_message".into()),
            ("status", "completed".into()),
            ("updated_at", NOW.into()),
            (
                "payload_json",
                json!({"id":format!("item-{index}"),"text":"output"}).to_string(),
            ),
        ] {
            item.insert(column.into(), text_cell(&value));
        }
        item.insert("run_id".into(), nullable_cell(run));
        item.insert("ordinal".into(), int_cell(index));
        write_row(&mut tx, &source, ITEMS, item).await.unwrap();
    }
    tx.put(
        store_key("application/generation/native/metadata/header"),
        encode(&json!({"version":2,"searchIndex":true,"searchDeleteWitness":true})).unwrap(),
    )
    .await
    .unwrap();
    tx.put(
        store_key("application/generation/native/message-rowids/v1/preparation"),
        encode(&json!({"after":null,"indexed_rows":1,"ready":true})).unwrap(),
    )
    .await
    .unwrap();
    let identity = encode(&json!({"message_id":"message","rowid":1})).unwrap();
    tx.put(
        store_key(&format!(
            "application/generation/native/message-rowids/v1/identity/{}",
            request_digest(&json!("message"))
        )),
        identity.clone(),
    )
    .await
    .unwrap();
    tx.put(
        store_key("application/generation/native/message-rowids/v1/rows/7ffffffffffffffe"),
        identity,
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    (fixture, store, source)
}

#[tokio::test]
async fn populated_80_upgrade_is_exclusive_atomic_and_preserves_original_rows() {
    let (fixture, store, source) = fixture_80().await;
    assert!(store.bootstrap_application().await.is_err());
    assert!(matches!(
        Store::open_exclusive(fixture.path()).await,
        Err(Error::WriterBusy)
    ));
    let original = store
        .db
        .read(&row_key("native", MESSAGES, &[text_cell("message")]).unwrap())
        .await
        .unwrap()
        .unwrap();
    store.shutdown().await.unwrap();
    let store = Store::open_exclusive(fixture.path()).await.unwrap();
    assert_eq!(
        store
            .bootstrap_application()
            .await
            .unwrap()
            .migrations
            .len(),
        89
    );
    let mut tx = store.db.begin_read_only().await.unwrap();
    let target = decode_catalog(&tx.get(&schema_key()).await.unwrap().unwrap()).unwrap();
    assert_ne!(target.generation, source.generation);
    assert_eq!(
        tx.get(&row_key("native", MESSAGES, &[text_cell("message")]).unwrap())
            .await
            .unwrap()
            .unwrap(),
        original
    );
    let message = read_row(&mut tx, &target, MESSAGES, &[text_cell("message")])
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        nullable(&message.row, "search_text").unwrap().as_deref(),
        Some("searchable quoted words\nComment: review this")
    );
    assert!(
        text(&message.row, "payload_json")
            .unwrap()
            .contains("t3-citation://")
    );
    let project = read_row(&mut tx, &target, PROJECTS, &[text_cell("project")])
        .await
        .unwrap()
        .unwrap();
    assert_eq!(integer(&project.row, "has_custom_title").unwrap(), 1);
    for (run, expected) in [("run", 2), ("", 1)] {
        let count = read_row(
            &mut tx,
            &target,
            crate::application_turn_item_counts::TABLE,
            &[text_cell("thread"), text_cell(run)],
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(integer(&count.row, "item_count").unwrap(), expected);
    }
    tx.rollback().await.unwrap();
    assert!(!store.bootstrap_application().await.unwrap().initialized);
    store.shutdown().await.unwrap();
    let reopened = fixture.connect().await;
    assert_eq!(reopened.application_migrations().await.unwrap().len(), 89);
    assert!(!reopened.bootstrap_application().await.unwrap().initialized);
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
async fn interrupted_staging_retries_from_current_source_and_removes_stale_candidate_rows() {
    let (fixture, store, source) = fixture_80().await;
    store.shutdown().await.unwrap();
    let store = Store::open_exclusive(fixture.path()).await.unwrap();
    let current = definition().unwrap();
    let target = ApplicationCatalog {
        generation: request_digest(&json!([
            "native-schema-upgrade",
            source.generation,
            current.schema
        ])),
        schema: current.schema,
    };
    store
        .upgrade_stage(&source, &target, Stage::Copy)
        .await
        .unwrap();
    store
        .upgrade_stage(&source, &target, Stage::Messages)
        .await
        .unwrap();
    assert_eq!(store.application_migrations().await.unwrap().len(), 80);
    store.shutdown().await.unwrap();
    // A previous build can write the unchanged original after interruption.
    let store = fixture.connect().await;
    let mut tx = store.db.begin().await.unwrap();
    let mut project = read_row(&mut tx, &source, PROJECTS, &[text_cell("project")])
        .await
        .unwrap()
        .unwrap();
    project
        .row
        .insert("title".into(), text_cell("Newer original title"));
    write_row(&mut tx, &source, PROJECTS, project.row)
        .await
        .unwrap();
    crate::application_rows::delete_row(&mut tx, &source, ITEMS, &[text_cell("item-2")])
        .await
        .unwrap();
    tx.commit().await.unwrap();
    store.shutdown().await.unwrap();
    let store = Store::open_exclusive(fixture.path()).await.unwrap();
    store.bootstrap_application().await.unwrap();
    let mut tx = store.db.begin_read_only().await.unwrap();
    let target = decode_catalog(&tx.get(&schema_key()).await.unwrap().unwrap()).unwrap();
    assert!(
        read_row(&mut tx, &target, ITEMS, &[text_cell("item-2")])
            .await
            .unwrap()
            .is_none()
    );
    let project = read_row(&mut tx, &target, PROJECTS, &[text_cell("project")])
        .await
        .unwrap()
        .unwrap();
    assert_eq!(text(&project.row, "title").unwrap(), "Newer original title");
    let count = read_row(
        &mut tx,
        &target,
        crate::application_turn_item_counts::TABLE,
        &[text_cell("thread"), text_cell("run")],
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(integer(&count.row, "item_count").unwrap(), 1);
    tx.rollback().await.unwrap();
    store.shutdown().await.unwrap();
}

#[tokio::test]
async fn read_only_and_future_migration_history_fail_without_changing_the_original() {
    let (fixture, store, source) = fixture_80().await;
    store.shutdown().await.unwrap();
    let before = std::fs::read(fixture.path()).unwrap();
    let store = Store::open_read_only(fixture.path()).await.unwrap();
    assert!(store.bootstrap_application().await.is_err());
    store.shutdown().await.unwrap();
    assert_eq!(std::fs::read(fixture.path()).unwrap(), before);
    let store = Store::open_exclusive(fixture.path()).await.unwrap();
    let mut tx = store.db.begin().await.unwrap();
    write_row(
        &mut tx,
        &source,
        "effect_sql_migrations",
        Map::from_iter([
            ("migration_id".into(), int_cell(90)),
            ("name".into(), text_cell("UnknownFutureMigration")),
            ("created_at".into(), text_cell(NOW)),
        ]),
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    assert!(store.bootstrap_application().await.is_err());
    let app = ApplicationTransactions::new(store.clone());
    assert_eq!(app.schema().await.unwrap(), Some(source.schema));
    let mut app = app;
    app.begin_mode("readonly", ApplicationTransactionMode::Read, Some(5000))
        .await
        .unwrap();
    app.rollback("readonly").await.unwrap();
    drop(app);
    store.shutdown().await.unwrap();
}

/// A real validated archive with the exact predecessor catalog. The populated
/// upgrade tests above cover data conversion; this archive isolates activation
/// proof and retry without substituting a hand-written import receipt.
fn write_empty_80_archive(path: &std::path::Path) {
    write_empty_archive(path, previous_definition().unwrap());
}

fn write_empty_archive(path: &std::path::Path, previous: Definition) {
    use sha2::{Digest, Sha256};
    let hash = |bytes: &[u8]| -> String {
        Sha256::digest(bytes)
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect()
    };
    let append = |bytes: &mut Vec<u8>, value: Value| {
        bytes.extend(serde_json::to_vec(&value).unwrap());
        bytes.push(b'\n');
    };
    let sql = |name: &str| format!("CREATE TABLE {name} (fixture)");
    let mut bytes = Vec::new();
    append(
        &mut bytes,
        json!({
            "type":"header","format":"t3-code-ls/sqlite-export","version":1,
            "sqliteVersion":"fixture","userVersion":0,"schemaVersion":1,"applicationId":0,
            "tableCount":previous.schema.tables.len(),"excludedDerivedTables":[],"sequences":[],
            "migrations":previous.migrations,
            "schema":previous.schema.tables.iter().map(|table|json!({
                "type":"table","name":table.name,"tableName":table.name,"sql":sql(&table.name)
            })).collect::<Vec<_>>()
        }),
    );
    let mut manifests = Vec::new();
    for table in &previous.schema.tables {
        let migrations = table.name == "effect_sql_migrations";
        let count = if migrations {
            previous.migrations.len()
        } else {
            0
        };
        let columns: Vec<_> = table.columns.iter().map(|name|json!({
            "name":name,"declaredType":"TEXT","notNull":false,"defaultSql":null,"hidden":0,
            "primaryKeyOrder":table.primary_key.iter().position(|column|column==name).map_or(0,|index|index+1)
        })).collect();
        append(
            &mut bytes,
            json!({"type":"table","name":table.name,"sql":sql(&table.name),"columns":columns,"rowCount":count}),
        );
        let mut rows = Vec::new();
        if migrations {
            for (ordinal, migration) in previous.migrations.iter().enumerate() {
                let values: Vec<_> = table
                    .columns
                    .iter()
                    .map(|name| match name.as_str() {
                        "migration_id" => int_cell(migration.id.parse().unwrap()),
                        "name" => text_cell(&migration.name),
                        "created_at" => text_cell(NOW),
                        _ => panic!("unexpected migration column"),
                    })
                    .collect();
                append(
                    &mut rows,
                    json!({"type":"row","table":table.name,"ordinal":ordinal,"values":values}),
                );
            }
        }
        bytes.extend_from_slice(&rows);
        manifests.push(json!({"name":table.name,"rowCount":count,"sha256":hash(&rows)}));
    }
    let digest = hash(&bytes);
    append(
        &mut bytes,
        json!({"type":"manifest","tableCount":previous.schema.tables.len(),"rowCount":previous.migrations.len(),"tables":manifests,"sha256":digest}),
    );
    std::fs::write(path, bytes).unwrap();
}

#[tokio::test]
async fn imported_upgrade_retains_receipt_and_requires_exact_durable_lineage_on_retry() {
    let fixture = TestFixture::start().await;
    let store = fixture.initialize().await;
    let archive = fixture.path().with_extension("schema-80.ndjson");
    write_empty_80_archive(&archive);
    let imported = store
        .import_sqlite_application(&archive, crate::sqlite_import::ImportLimits::default())
        .await
        .unwrap();
    let receipt_key = store_key("application/import/active");
    let receipt_bytes = store.db.read(&receipt_key).await.unwrap().unwrap();
    store.shutdown().await.unwrap();
    let store = Store::open_exclusive(fixture.path()).await.unwrap();
    store.bootstrap_application().await.unwrap();
    assert_eq!(
        store.db.read(&receipt_key).await.unwrap().unwrap(),
        receipt_bytes
    );
    let receipt = store.application_import().await.unwrap().unwrap();
    assert_eq!(receipt.generation, imported.generation);
    assert_eq!(receipt.summary, imported.summary);
    assert!(receipt.replayed);
    store.shutdown().await.unwrap();

    // This is the crash boundary before the outer startup activates its marker.
    let store = fixture.connect().await;
    let replayed = store
        .import_sqlite_application(&archive, crate::sqlite_import::ImportLimits::default())
        .await
        .unwrap();
    assert_eq!(replayed, receipt);
    let target = decode_catalog(&store.db.read(&schema_key()).await.unwrap().unwrap()).unwrap();
    let key = lineage_key(&target);
    let original = store.db.read(&key).await.unwrap().unwrap();
    let mut changed: Value = serde_json::from_slice(&original).unwrap();
    changed["source"]["generation"] = json!("unrelated");
    for damaged in [
        None,
        Some(b"invalid json".to_vec()),
        Some(encode(&changed).unwrap()),
    ] {
        let mut tx = store.db.begin().await.unwrap();
        match damaged {
            Some(bytes) => tx.put(key.clone(), bytes).await.unwrap(),
            None => {
                tx.delete(key.clone()).await.unwrap();
            }
        }
        tx.commit().await.unwrap();
        assert!(matches!(
            store.application_import().await,
            Err(Error::IncompatibleStore)
        ));
        assert_eq!(
            store.db.read(&receipt_key).await.unwrap().unwrap(),
            receipt_bytes
        );
    }
    let mut tx = store.db.begin().await.unwrap();
    tx.put(key, original).await.unwrap();
    tx.commit().await.unwrap();
    assert_eq!(store.application_import().await.unwrap().unwrap(), receipt);
    assert_eq!(store.application_migrations().await.unwrap().len(), 89);
    store.shutdown().await.unwrap();
}

const THREADS: &str = "orchestration_v2_projection_threads";

fn indexed_thread(id: &str, root: &str, worktree: &str) -> Value {
    json!({
        "id":id,"projectId":"project","title":"Thread","providerInstanceId":"instance",
        "runtimeMode":"full-access","interactionMode":"default","activeProviderThreadId":null,
        "createdAt":NOW,"updatedAt":NOW,"archivedAt":null,"deletedAt":null,
        "worktreePath":worktree,
        "lineage":{"rootThreadId":root,"parentThreadId":null,"relationshipToParent":null}
    })
}

async fn put_thread(tx: &mut Transaction, catalog: &ApplicationCatalog, payload: Value) {
    let row = Map::from_iter([
        (
            "thread_id".into(),
            text_cell(payload["id"].as_str().unwrap()),
        ),
        ("project_id".into(), text_cell("project")),
        ("title".into(), text_cell("Thread")),
        ("default_provider".into(), text_cell("instance")),
        ("provider_instance_id".into(), text_cell("instance")),
        ("runtime_mode".into(), text_cell("full-access")),
        ("interaction_mode".into(), text_cell("default")),
        ("active_provider_thread_id".into(), nullable_cell(None)),
        ("created_at".into(), text_cell(NOW)),
        ("updated_at".into(), text_cell(NOW)),
        ("archived_at".into(), nullable_cell(None)),
        ("deleted_at".into(), nullable_cell(None)),
        ("payload_json".into(), text_cell(&payload.to_string())),
    ]);
    write_row(tx, catalog, THREADS, row).await.unwrap();
}

async fn generation_bytes(store: &Store, catalog: &ApplicationCatalog) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut tx = store.db.begin_read_only().await.unwrap();
    let prefix = prefix(&catalog.generation);
    let rows = tx.scan(&prefix, &end(&prefix), 4096).await.unwrap();
    assert!(rows.len() < 4096);
    tx.rollback().await.unwrap();
    rows
}

async fn remove_89_indexes(store: &Store, catalog: &ApplicationCatalog) {
    let mut tx = store.db.begin().await.unwrap();
    for kind in ["rows", "counts"] {
        for column in ["_root_thread", "_worktree_path"] {
            let prefix = store_key(&format!(
                "application/generation/{}/projection-reads/v1/{kind}/{THREADS}/{column}/",
                catalog.generation
            ));
            for (key, _) in tx.scan(&prefix, &end(&prefix), 4096).await.unwrap() {
                tx.delete(key).await.unwrap();
            }
        }
    }
    tx.commit().await.unwrap();
}

async fn fixture_88() -> (TestFixture, Store, ApplicationCatalog) {
    let (fixture, store, source) = fixture_80().await;
    let mut tx = store.db.begin().await.unwrap();
    for index in 0..35 {
        put_thread(
            &mut tx,
            &source,
            indexed_thread(&format!("thread-{index:02}"), "family", "/old"),
        )
        .await;
    }
    tx.commit().await.unwrap();
    store.shutdown().await.unwrap();
    let store = Store::open_exclusive(fixture.path()).await.unwrap();
    store
        .upgrade_application_80(source, definition_88().unwrap())
        .await
        .unwrap();
    while !store
        .prepare_application_projection_reads()
        .await
        .unwrap()
        .prepared
    {}
    let source = decode_catalog(&store.db.read(&schema_key()).await.unwrap().unwrap()).unwrap();
    // Model an already prepared 88 store: its strict read-index witness has no
    // knowledge of the two new migration-89 JSON expressions.
    remove_89_indexes(&store, &source).await;
    (fixture, store, source)
}

async fn selected_thread_ids(
    app: &mut ApplicationTransactions,
    transaction: &str,
    options: Value,
) -> Vec<String> {
    let query = serde_json::from_value(json!({"kind":"threads","options":options})).unwrap();
    app.projections_shell(transaction, query)
        .await
        .unwrap()
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["thread_id"].as_str().unwrap().to_owned())
        .collect()
}

async fn replace_thread(app: &mut ApplicationTransactions, transaction: &str, payload: Value) {
    app.projections_apply(
        transaction,
        crate::application_projections::ProjectionApply {
            event_type: "thread.created".into(),
            thread_id: payload["id"].as_str().unwrap().into(),
            occurred_at: NOW.into(),
            payload_json: payload.to_string(),
            normalization: None,
        },
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn index_only_88_upgrade_rebuilds_after_interruption_preserves_counts_and_supports_live_writes()
 {
    let (fixture, store, source) = fixture_88().await;
    let current = definition().unwrap();
    let target = target_89(&source, &current);
    assert_ne!(source.generation, target.generation);
    assert!(
        store
            .upgrade_stage(&source, &source, Stage::Clear)
            .await
            .is_err()
    );
    store
        .upgrade_stage(&source, &target, Stage::Copy)
        .await
        .unwrap();
    store
        .prepare_projection_reads_for_catalog(&target)
        .await
        .unwrap();
    assert_eq!(store.application_migrations().await.unwrap().len(), 88);
    store.shutdown().await.unwrap();

    // The previous build can edit the still-active 88 rows before this retry.
    // Staged 89 indexes and payloads must not survive the recopy.
    let store = fixture.connect().await;
    let mut tx = store.db.begin().await.unwrap();
    put_thread(
        &mut tx,
        &source,
        indexed_thread("thread-00", "new-family", "/new"),
    )
    .await;
    put_thread(
        &mut tx,
        &source,
        indexed_thread("thread-new", "new-family", "/new"),
    )
    .await;
    crate::application_rows::delete_row(&mut tx, &source, THREADS, &[text_cell("thread-01")])
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let before = generation_bytes(&store, &source).await;
    assert!(store.bootstrap_application().await.is_err());
    assert_eq!(generation_bytes(&store, &source).await, before);
    store.shutdown().await.unwrap();
    let store = Store::open_exclusive(fixture.path()).await.unwrap();
    assert_eq!(
        store
            .bootstrap_application()
            .await
            .unwrap()
            .migrations
            .len(),
        89
    );
    assert_eq!(generation_bytes(&store, &source).await, before);
    let active = decode_catalog(&store.db.read(&schema_key()).await.unwrap().unwrap()).unwrap();
    assert_eq!(active, target);
    let mut tx = store.db.begin_read_only().await.unwrap();
    for (run, expected) in [("run", 2), ("", 1)] {
        let count = read_row(
            &mut tx,
            &active,
            crate::application_turn_item_counts::TABLE,
            &[text_cell("thread"), text_cell(run)],
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(integer(&count.row, "item_count").unwrap(), expected);
    }
    tx.rollback().await.unwrap();

    let mut app = ApplicationTransactions::new(store.clone());
    app.begin("write").await.unwrap();
    let expected = vec!["thread-00".to_string(), "thread-new".to_string()];
    assert_eq!(
        selected_thread_ids(&mut app, "write", json!({"worktreePath":"/new"})).await,
        expected
    );
    assert_eq!(
        selected_thread_ids(&mut app, "write", json!({"familyOf":"thread-00"})).await,
        expected
    );
    assert!(
        selected_thread_ids(&mut app, "write", json!({"threadIds":["thread-01"]}))
            .await
            .is_empty()
    );
    app.savepoint("write", "inner").await.unwrap();
    replace_thread(
        &mut app,
        "write",
        indexed_thread("thread-00", "moved", "/moved"),
    )
    .await;
    assert_eq!(
        selected_thread_ids(&mut app, "write", json!({"worktreePath":"/moved"})).await,
        ["thread-00"]
    );
    app.rollback_to("write", "inner").await.unwrap();
    app.release("write", "inner").await.unwrap();
    assert!(
        selected_thread_ids(&mut app, "write", json!({"worktreePath":"/moved"}))
            .await
            .is_empty()
    );
    replace_thread(
        &mut app,
        "write",
        indexed_thread("thread-00", "moved", "/moved"),
    )
    .await;
    app.commit("write").await.unwrap();
    drop(app);
    store.shutdown().await.unwrap();
    let reopened = fixture.connect().await;
    assert!(!reopened.bootstrap_application().await.unwrap().initialized);
    let mut app = ApplicationTransactions::new(reopened.clone());
    app.begin("read").await.unwrap();
    assert_eq!(
        selected_thread_ids(
            &mut app,
            "read",
            json!({"worktreePath":"/moved","familyOf":"thread-00"})
        )
        .await,
        ["thread-00"]
    );
    app.rollback("read").await.unwrap();
    drop(app);
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
async fn direct_88_import_and_80_88_89_chain_retain_the_original_receipt_and_exact_lineage() {
    for original_version in [80, 88] {
        let fixture = TestFixture::start().await;
        let store = fixture.initialize().await;
        let archive = fixture.path().with_extension("original.ndjson");
        write_empty_archive(
            &archive,
            if original_version == 80 {
                previous_definition().unwrap()
            } else {
                definition_88().unwrap()
            },
        );
        let original_archive = std::fs::read(&archive).unwrap();
        let imported = store
            .import_sqlite_application(&archive, crate::sqlite_import::ImportLimits::default())
            .await
            .unwrap();
        let receipt_key = store_key("application/import/active");
        let receipt_bytes = store.db.read(&receipt_key).await.unwrap().unwrap();
        let original =
            decode_catalog(&store.db.read(&schema_key()).await.unwrap().unwrap()).unwrap();
        let original_rows = generation_bytes(&store, &original).await;
        store.shutdown().await.unwrap();
        let store = Store::open_exclusive(fixture.path()).await.unwrap();
        if original_version == 80 {
            store
                .upgrade_application_80(original.clone(), definition_88().unwrap())
                .await
                .unwrap();
            // Read-only startup inspects this old receipt before requesting89.
            assert_eq!(
                store.application_import().await.unwrap().unwrap().summary,
                imported.summary
            );
        }
        let intermediate =
            decode_catalog(&store.db.read(&schema_key()).await.unwrap().unwrap()).unwrap();
        let intermediate_rows = generation_bytes(&store, &intermediate).await;
        store.bootstrap_application().await.unwrap();
        let active = decode_catalog(&store.db.read(&schema_key()).await.unwrap().unwrap()).unwrap();
        assert_eq!(active, target_89(&intermediate, &definition().unwrap()));
        assert_eq!(generation_bytes(&store, &original).await, original_rows);
        assert_eq!(
            generation_bytes(&store, &intermediate).await,
            intermediate_rows
        );
        assert_eq!(std::fs::read(&archive).unwrap(), original_archive);
        assert_eq!(
            store.db.read(&receipt_key).await.unwrap().unwrap(),
            receipt_bytes
        );
        let receipt = store.application_import().await.unwrap().unwrap();
        assert_eq!(receipt.generation, imported.generation);
        assert_eq!(receipt.summary, imported.summary);
        let key = lineage_89_key(&active);
        let good = store.db.read(&key).await.unwrap().unwrap();
        let mut wrong: Value = serde_json::from_slice(&good).unwrap();
        wrong["source"]["generation"] = json!("unrelated");
        for damaged in [
            None,
            Some(b"invalid".to_vec()),
            Some(encode(&wrong).unwrap()),
        ] {
            let mut tx = store.db.begin().await.unwrap();
            if let Some(bytes) = damaged {
                tx.put(key.clone(), bytes).await.unwrap();
            } else {
                tx.delete(key.clone()).await.unwrap();
            }
            tx.commit().await.unwrap();
            assert!(matches!(
                store.application_import().await,
                Err(Error::IncompatibleStore)
            ));
            assert_eq!(
                store.db.read(&receipt_key).await.unwrap().unwrap(),
                receipt_bytes
            );
        }
        let mut tx = store.db.begin().await.unwrap();
        tx.put(key, good).await.unwrap();
        tx.commit().await.unwrap();
        assert_eq!(store.application_import().await.unwrap().unwrap(), receipt);
        store.shutdown().await.unwrap();
        let reopened = fixture.connect().await;
        assert_eq!(
            reopened
                .import_sqlite_application(&archive, crate::sqlite_import::ImportLimits::default())
                .await
                .unwrap(),
            receipt
        );
        reopened.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn exact_88_history_is_required_and_read_only_future_or_damaged_histories_cannot_publish() {
    let (fixture, store, source) = fixture_88().await;
    store.shutdown().await.unwrap();
    let before = std::fs::read(fixture.path()).unwrap();
    let readonly = Store::open_read_only(fixture.path()).await.unwrap();
    assert!(readonly.bootstrap_application().await.is_err());
    readonly.shutdown().await.unwrap();
    assert_eq!(std::fs::read(fixture.path()).unwrap(), before);
    for (id, name) in [
        (88, "AlteredKnownMigration"),
        (90, "UnknownFutureMigration"),
    ] {
        let store = Store::open_exclusive(fixture.path()).await.unwrap();
        let mut tx = store.db.begin().await.unwrap();
        let previous = definition_88().unwrap();
        let known = previous.migrations.last().unwrap();
        write_row(
            &mut tx,
            &source,
            "effect_sql_migrations",
            Map::from_iter([
                ("migration_id".into(), int_cell(88)),
                ("name".into(), text_cell(&known.name)),
                ("created_at".into(), text_cell(NOW)),
            ]),
        )
        .await
        .unwrap();
        write_row(
            &mut tx,
            &source,
            "effect_sql_migrations",
            Map::from_iter([
                ("migration_id".into(), int_cell(id)),
                ("name".into(), text_cell(name)),
                ("created_at".into(), text_cell(NOW)),
            ]),
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();
        let before = generation_bytes(&store, &source).await;
        assert!(store.bootstrap_application().await.is_err());
        assert_eq!(generation_bytes(&store, &source).await, before);
        assert_eq!(
            decode_catalog(&store.db.read(&schema_key()).await.unwrap().unwrap()).unwrap(),
            source
        );
        store.shutdown().await.unwrap();
    }
}
