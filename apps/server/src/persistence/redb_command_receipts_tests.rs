use super::*;
use crate::application::{ApplicationSchema, ApplicationTable, ApplicationTransactionMode};
use crate::test_support::TestFixture;

fn schema() -> ApplicationSchema {
    ApplicationSchema {
        version: 1,
        tables: vec![ApplicationTable {
            name: RECEIPTS.into(),
            columns: COLUMNS.iter().map(|value| (*value).into()).collect(),
            primary_key: vec!["command_id".into()],
        }],
    }
}

fn receipt(id: &str) -> ReceiptRow {
    ReceiptRow {
        command_id: id.into(),
        aggregate_kind: "thread".into(),
        aggregate_id: "thread:one".into(),
        command_type: "thread.turn.start".into(),
        accepted_at: "2026-11-10T12:00:00.000Z".into(),
        result_sequence: 0,
        status: "accepted".into(),
        error: None,
    }
}

async fn initialize(store: &Store) -> ApplicationTransactions {
    let mut app = ApplicationTransactions::new(store.clone());
    app.initialize(schema()).await.unwrap();
    app
}

async fn prepare(store: &Store) -> PreparationResult {
    loop {
        let result = store.prepare_application_receipts().await.unwrap();
        if result.prepared {
            return result;
        }
    }
}

#[tokio::test]
async fn receipt_identity_is_global_and_upsert_replaces_every_field() {
    let fixture = TestFixture::start().await;
    let store = fixture.initialize().await;
    let mut app = initialize(&store).await;
    prepare(&store).await;
    app.begin("write").await.unwrap();
    assert!(
        app.receipts_insert("write", receipt("command"))
            .await
            .unwrap()
    );
    let replacement = ReceiptRow {
        aggregate_kind: "project".into(),
        aggregate_id: "project:two".into(),
        command_type: "project.delete".into(),
        accepted_at: "2026-11-11T00:00:00.000Z".into(),
        result_sequence: 42,
        status: "rejected".into(),
        error: Some(String::new()),
        ..receipt("command")
    };
    assert!(
        !app.receipts_insert("write", replacement.clone())
            .await
            .unwrap()
    );
    assert_eq!(
        app.receipts_get("write", "command").await.unwrap(),
        Some(receipt("command"))
    );
    app.receipts_upsert("write", replacement.clone())
        .await
        .unwrap();
    assert_eq!(
        app.receipts_get("write", "command").await.unwrap(),
        Some(replacement.clone())
    );
    assert!(
        app.receipts_get("write", "missing")
            .await
            .unwrap()
            .is_none()
    );
    app.commit("write").await.unwrap();
    app.begin_mode("read", ApplicationTransactionMode::Read, None)
        .await
        .unwrap();
    assert_eq!(
        app.receipts_get("read", "command").await.unwrap(),
        Some(replacement)
    );
    let row = app
        .get("read", RECEIPTS, &[text_cell("command")])
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row["result_sequence"], int_cell(42));
    assert_eq!(row["error"], text_cell(""));
    app.rollback("read").await.unwrap();
}

#[tokio::test]
async fn receipt_savepoint_rejection_restores_reservation_and_outer_rollback_discards_all() {
    let fixture = TestFixture::start().await;
    let store = fixture.initialize().await;
    let mut app = initialize(&store).await;
    prepare(&store).await;
    app.begin("outer").await.unwrap();
    app.receipts_insert("outer", receipt("reserved"))
        .await
        .unwrap();
    app.savepoint("outer", "nested").await.unwrap();
    let finalized = ReceiptRow {
        result_sequence: 7,
        ..receipt("reserved")
    };
    app.receipts_upsert("outer", finalized).await.unwrap();
    let invalid = ReceiptRow {
        result_sequence: -1,
        ..receipt("bad")
    };
    assert!(matches!(
        app.receipts_insert("outer", invalid).await,
        Err(Error::Invalid(_))
    ));
    assert!(app.release("outer", "nested").await.is_err());
    app.rollback_to("outer", "nested").await.unwrap();
    app.release("outer", "nested").await.unwrap();
    assert_eq!(
        app.receipts_get("outer", "reserved").await.unwrap(),
        Some(receipt("reserved"))
    );
    app.receipts_insert("outer", receipt("sibling"))
        .await
        .unwrap();
    app.rollback("outer").await.unwrap();
    app.begin("check").await.unwrap();
    assert!(
        app.receipts_get("check", "reserved")
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        app.receipts_get("check", "sibling")
            .await
            .unwrap()
            .is_none()
    );
    app.rollback("check").await.unwrap();
}

#[tokio::test]
async fn receipt_preparation_resumes_and_never_rebuilds_live_receipts() {
    let fixture = TestFixture::start().await;
    let store = fixture.initialize().await;
    let mut app = initialize(&store).await;
    app.begin("seed").await.unwrap();
    for index in 0..70 {
        let row = receipt(&format!("imported:{index}"));
        app.put(
            "seed",
            RECEIPTS,
            vec![text_cell(&row.command_id)],
            row.stored(),
        )
        .await
        .unwrap();
    }
    app.commit("seed").await.unwrap();
    let first = store.prepare_application_receipts().await.unwrap();
    assert!(!first.prepared);
    assert_eq!(first.indexed_rows, PREPARE_ROWS as u64);
    app.begin("incomplete").await.unwrap();
    assert!(matches!(
        app.receipts_get("incomplete", "imported:0").await,
        Err(Error::Invalid(_))
    ));
    app.begin("bypass").await.unwrap();
    assert!(matches!(
        app.delete("bypass", RECEIPTS, &[text_cell("imported:0")])
            .await,
        Err(Error::Invalid(_))
    ));
    drop(app);
    store.shutdown().await.unwrap();
    let store = fixture.connect().await;
    assert_eq!(prepare(&store).await.indexed_rows, 70);
    let mut app = ApplicationTransactions::new(store.clone());
    app.begin("write").await.unwrap();
    app.receipts_insert("write", receipt("live")).await.unwrap();
    app.receipts_upsert(
        "write",
        ReceiptRow {
            result_sequence: 99,
            ..receipt("imported:0")
        },
    )
    .await
    .unwrap();
    app.commit("write").await.unwrap();
    assert_eq!(prepare(&store).await.indexed_rows, 70);
    app.begin("read").await.unwrap();
    assert!(app.receipts_get("read", "live").await.unwrap().is_some());
    assert_eq!(
        app.receipts_get("read", "imported:0")
            .await
            .unwrap()
            .unwrap()
            .result_sequence,
        99
    );
    app.rollback("read").await.unwrap();
}

#[tokio::test]
async fn receipt_preparation_rejects_lossy_cells_and_read_only_sessions_cannot_write() {
    let fixture = TestFixture::start().await;
    let store = fixture.initialize().await;
    let mut app = initialize(&store).await;
    app.begin("seed").await.unwrap();
    let mut row = receipt("bad").stored();
    row.insert(
        "result_sequence".into(),
        serde_json::json!({"type":"real","value":"0000000000000000"}),
    );
    app.put("seed", RECEIPTS, vec![text_cell("bad")], row)
        .await
        .unwrap();
    app.commit("seed").await.unwrap();
    assert!(matches!(
        store.prepare_application_receipts().await,
        Err(Error::IncompatibleStore)
    ));
    app.begin("fix").await.unwrap();
    app.put(
        "fix",
        RECEIPTS,
        vec![text_cell("bad")],
        receipt("bad").stored(),
    )
    .await
    .unwrap();
    app.commit("fix").await.unwrap();
    prepare(&store).await;
    app.begin_mode("read", ApplicationTransactionMode::Read, None)
        .await
        .unwrap();
    assert!(
        app.receipts_insert("read", receipt("no-write"))
            .await
            .is_err()
    );
    app.begin("check").await.unwrap();
    assert!(
        app.receipts_get("check", "no-write")
            .await
            .unwrap()
            .is_none()
    );
    app.rollback("check").await.unwrap();
}

#[tokio::test]
async fn receipt_preparation_adapts_before_commit_to_large_imported_rows() {
    let fixture = TestFixture::start().await;
    let store = fixture.initialize().await;
    let mut app = initialize(&store).await;
    for index in 0..3 {
        let row = ReceiptRow {
            error: Some("x".repeat(6 * 1024 * 1024)),
            ..receipt(&format!("large:{index}"))
        };
        app.begin("seed").await.unwrap();
        app.put(
            "seed",
            RECEIPTS,
            vec![text_cell(&row.command_id)],
            row.stored(),
        )
        .await
        .unwrap();
        app.commit("seed").await.unwrap();
    }
    assert_eq!(prepare(&store).await.indexed_rows, 3);
    app.begin("read").await.unwrap();
    assert_eq!(
        app.receipts_get("read", "large:2")
            .await
            .unwrap()
            .unwrap()
            .error
            .unwrap()
            .len(),
        6 * 1024 * 1024
    );
    app.rollback("read").await.unwrap();
}
