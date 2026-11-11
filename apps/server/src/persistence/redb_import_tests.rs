use std::path::Path;

use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use super::*;
use crate::application::ApplicationTransactions;
use crate::sqlite_import::validate_sqlite_export;
use crate::test_support::TestFixture;

fn hash(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn export(
    path: &Path,
    ids: &[u64],
    payload: &str,
) -> (ExportSummary, ApplicationTable, Vec<ApplicationRow>) {
    export_table(path, ids, payload, "records", "payload")
}

fn export_table(
    path: &Path,
    ids: &[u64],
    payload: &str,
    name: &str,
    value_column: &str,
) -> (ExportSummary, ApplicationTable, Vec<ApplicationRow>) {
    let sql = format!("CREATE TABLE {name} (id INTEGER PRIMARY KEY, {value_column} TEXT)");
    let header = json!({"type":"header","format":"t3-code-ls/sqlite-export","version":1,
        "sqliteVersion":"fixture","userVersion":0,"schemaVersion":1,"applicationId":0,
        "tableCount":1,"excludedDerivedTables":[],"sequences":[],"migrations":[],
        "schema":[{"type":"table","name":name,"tableName":name,"sql":sql}]});
    let columns = [
        json!({"name":"id","declaredType":"INTEGER","notNull":false,"defaultSql":null,"primaryKeyOrder":1,"hidden":0}),
        json!({"name":value_column,"declaredType":"TEXT","notNull":false,"defaultSql":null,"primaryKeyOrder":0,"hidden":0}),
    ];
    let table =
        json!({"type":"table","name":name,"sql":sql,"columns":columns,"rowCount":ids.len()});
    let definition = table_from_record(&table).unwrap();
    let mut bytes = Vec::new();
    let append = |bytes: &mut Vec<u8>, value: &Value| {
        bytes.extend(serde_json::to_vec(value).unwrap());
        bytes.push(b'\n');
    };
    append(&mut bytes, &header);
    append(&mut bytes, &table);
    let mut row_bytes = Vec::new();
    let mut rows = Vec::new();
    for (ordinal, id) in ids.iter().enumerate() {
        let record = json!({"type":"row","table":name,"ordinal":ordinal,"values":[
            {"type":"integer","value":id.to_string()}, {"type":"text","value":payload}
        ]});
        append(&mut bytes, &record);
        append(&mut row_bytes, &record);
        rows.push(row_from_record(&definition, &record).unwrap());
    }
    let manifest = json!({"type":"manifest","tableCount":1,"rowCount":ids.len(),
        "tables":[{"name":name,"rowCount":ids.len(),"sha256":hash(&row_bytes)}],"sha256":hash(&bytes)});
    append(&mut bytes, &manifest);
    std::fs::write(path, &bytes).unwrap();
    let summary = validate_sqlite_export(path, ImportLimits::default()).unwrap();
    (summary, definition, rows)
}

#[tokio::test]
async fn import_receipt_survives_the_current_reader_floor_and_read_only_reopen() {
    let fixture = TestFixture::start().await;
    let store = fixture.initialize().await;
    let source = fixture.path().with_extension("ndjson");
    let (summary, _, _) = export_table(
        &source,
        &[7],
        "readable citation",
        "orchestration_v2_projection_messages",
        "search_text",
    );
    store
        .import_sqlite_application(&source, ImportLimits::default())
        .await
        .unwrap();
    let catalog = store.db.read(&schema_key()).await.unwrap().unwrap();
    assert_eq!(
        serde_json::from_slice::<Value>(&catalog).unwrap()["minimum_reader_migration"],
        json!(81)
    );
    assert!(serde_json::from_slice::<ApplicationCatalog>(&catalog).is_err());
    let receipt = store.application_import().await.unwrap().unwrap();
    assert_eq!(receipt.summary, summary);
    assert!(receipt.replayed);
    store.shutdown().await.unwrap();
    let before = std::fs::read(fixture.path()).unwrap();
    let reopened = Store::open_read_only(fixture.path()).await.unwrap();
    assert_eq!(reopened.application_import().await.unwrap(), Some(receipt));
    reopened.shutdown().await.unwrap();
    assert_eq!(std::fs::read(fixture.path()).unwrap(), before);
}

#[tokio::test]
async fn a_durable_unpublished_generation_is_invisible_and_resumes_after_reopen() {
    let fixture = TestFixture::start().await;
    let store = fixture.initialize().await;
    let source = fixture.path().with_extension("ndjson");
    let (summary, table, rows) = export(&source, &[7, 9], "preserved");
    store
        .archive_sqlite_export(&source, ImportLimits::default())
        .await
        .unwrap();
    let mut staged = Stage::new();
    staged
        .push(
            &store,
            row_key(&summary.export_sha256, &table.name, &rows[0].key).unwrap(),
            encode_row(&rows[0]).unwrap(),
        )
        .await
        .unwrap();
    staged.flush(&store).await.unwrap();
    let mut reader = ApplicationTransactions::new(store.clone());
    assert!(reader.schema().await.unwrap().is_none());
    assert!(matches!(
        reader.begin("before-publication").await,
        Err(Error::Invalid(_))
    ));
    assert!(store.application_import().await.unwrap().is_none());
    drop(reader);
    store.shutdown().await.unwrap();

    let reopened = fixture.connect().await;
    let receipt = reopened
        .import_sqlite_application(&source, ImportLimits::default())
        .await
        .unwrap();
    assert_eq!(receipt.summary, summary);
    let mut reader = ApplicationTransactions::new(reopened.clone());
    reader.begin("after-publication").await.unwrap();
    for row in rows {
        assert_eq!(
            reader
                .get("after-publication", &table.name, &row.key)
                .await
                .unwrap(),
            Some(row.row)
        );
    }
    reader.rollback("after-publication").await.unwrap();
}

#[tokio::test]
async fn importer_cannot_write_a_generation_after_another_session_activates_it() {
    let fixture = TestFixture::start().await;
    let store = fixture.initialize().await;
    let source = fixture.path().with_extension("ndjson");
    let (summary, table, rows) = export(&source, &[7], "old");
    let mut stale_import = Stage::new();
    stale_import
        .push(
            &store,
            row_key(&summary.export_sha256, &table.name, &rows[0].key).unwrap(),
            encode_row(&rows[0]).unwrap(),
        )
        .await
        .unwrap();
    store
        .import_sqlite_application(&source, ImportLimits::default())
        .await
        .unwrap();
    let mut writer = ApplicationTransactions::new(store.clone());
    writer.begin("delete-after-import").await.unwrap();
    assert!(
        writer
            .delete("delete-after-import", &table.name, &rows[0].key)
            .await
            .unwrap()
    );
    writer.commit("delete-after-import").await.unwrap();
    assert!(matches!(
        stale_import.flush(&store).await,
        Err(Error::Invalid(_))
    ));
    let receipt = store
        .import_sqlite_application(&source, ImportLimits::default())
        .await
        .unwrap();
    assert!(receipt.replayed);
    writer.begin("verify-deleted").await.unwrap();
    assert!(
        writer
            .get("verify-deleted", &table.name, &rows[0].key)
            .await
            .unwrap()
            .is_none()
    );
    writer.rollback("verify-deleted").await.unwrap();
}

#[tokio::test]
async fn identical_duplicate_primary_keys_fail_even_after_an_earlier_batch_commits() {
    let fixture = TestFixture::start().await;
    let store = fixture.initialize().await;
    let source = fixture.path().with_extension("ndjson");
    let (summary, table, rows) = export(&source, &[7, 7], &"x".repeat(STAGE_BYTES + 1024));
    let original = std::fs::read(&source).unwrap();
    assert!(matches!(
        store
            .import_sqlite_application(&source, ImportLimits::default())
            .await,
        Err(Error::Invalid(_))
    ));
    assert!(
        store
            .db
            .read(&row_key(&summary.export_sha256, &table.name, &rows[0].key).unwrap())
            .await
            .unwrap()
            .is_some(),
        "first batch really committed to its unpublished generation"
    );
    assert!(
        ApplicationTransactions::new(store.clone())
            .schema()
            .await
            .unwrap()
            .is_none()
    );
    assert!(store.application_import().await.unwrap().is_none());
    assert_eq!(std::fs::read(&source).unwrap(), original);
}

#[tokio::test]
async fn import_streams_more_than_one_transaction_budget_and_keeps_each_row_readable() {
    let fixture = TestFixture::start().await;
    let store = fixture.initialize().await;
    let source = fixture.path().with_extension("ndjson");
    let ids: Vec<_> = (0..18).collect();
    let (summary, table, rows) = export(&source, &ids, &"x".repeat(1024 * 1024));
    assert!(summary.byte_count > MAX_BATCH_BYTES as u64);
    let receipt = store
        .import_sqlite_application(&source, ImportLimits::default())
        .await
        .unwrap();
    assert_eq!(receipt.summary.row_count, 18);
    let mut reader = ApplicationTransactions::new(store.clone());
    for (index, row) in rows.into_iter().enumerate() {
        let id = format!("reader-{index}");
        reader.begin(&id).await.unwrap();
        assert_eq!(
            reader.get(&id, &table.name, &row.key).await.unwrap(),
            Some(row.row)
        );
        reader.rollback(&id).await.unwrap();
    }
}
