use super::*;
use crate::application::{ApplicationSchema, ApplicationTable, ApplicationTransactionMode};
use crate::test_support::TestFixture;

fn schema() -> ApplicationSchema {
    ApplicationSchema {
        version: 1,
        tables: vec![ApplicationTable {
            name: TABLE.into(),
            columns: COLUMNS.iter().map(|value| (*value).into()).collect(),
            primary_key: vec!["environment_id".into()],
        }],
    }
}

fn row(id: &str, label: &str) -> PeerRecord {
    PeerRecord {
        environment_id: id.into(),
        secret_name: format!("secret-{id}"),
        label: label.into(),
        urls_json: "[\"https://example.test\"]".into(),
        access: "full".into(),
        linked_at: "2026-11-10T00:00:00.000Z".into(),
        expires_at: "2026-11-11T00:00:00.000Z".into(),
        last_reached_at: None,
        last_error: None,
        fenced: 1,
    }
}

async fn prepare(store: &Store) -> PreparationResult {
    loop {
        let result = store.prepare_application_peers().await.unwrap();
        if result.prepared {
            return result;
        }
    }
}

async fn rows(app: &mut ApplicationTransactions, id: &str) -> Vec<PeerRecord> {
    serde_json::from_value(app.peers(id, PeerOperation::List).await.unwrap()).unwrap()
}

#[tokio::test]
async fn peer_lifecycle_preserves_label_order_credentials_and_outcome_fields() {
    let fixture = TestFixture::start().await;
    let store = fixture.initialize().await;
    let mut app = ApplicationTransactions::new(store.clone());
    app.initialize(schema()).await.unwrap();
    prepare(&store).await;
    app.begin("write").await.unwrap();
    for record in [row("b", "Zulu"), row("a", "Zulu"), row("c", "Alpha")] {
        app.peers("write", PeerOperation::Upsert { row: record })
            .await
            .unwrap();
    }
    assert_eq!(
        rows(&mut app, "write")
            .await
            .iter()
            .map(|row| row.environment_id.as_str())
            .collect::<Vec<_>>(),
        ["c", "a", "b"]
    );
    app.peers(
        "write",
        PeerOperation::Relabel {
            environment_id: "b".into(),
            label: "A".into(),
        },
    )
    .await
    .unwrap();
    app.peers(
        "write",
        PeerOperation::RecordOutcome {
            environment_id: "b".into(),
            error: None,
            reached_at: "success-time".into(),
        },
    )
    .await
    .unwrap();
    app.peers(
        "write",
        PeerOperation::RecordOutcome {
            environment_id: "b".into(),
            error: Some("offline".into()),
            reached_at: "ignored-time".into(),
        },
    )
    .await
    .unwrap();
    let b: PeerRecord = serde_json::from_value(
        app.peers(
            "write",
            PeerOperation::Get {
                environment_id: "b".into(),
            },
        )
        .await
        .unwrap(),
    )
    .unwrap();
    assert_eq!(b.last_reached_at.as_deref(), Some("success-time"));
    assert_eq!(b.last_error.as_deref(), Some("offline"));
    assert_eq!(rows(&mut app, "write").await[0].environment_id, "b");
    assert_eq!(
        app.peers(
            "write",
            PeerOperation::Delete {
                environment_id: "a".into()
            }
        )
        .await
        .unwrap(),
        json!("secret-a")
    );
    assert_eq!(
        app.peers(
            "write",
            PeerOperation::Delete {
                environment_id: "a".into()
            }
        )
        .await
        .unwrap(),
        Value::Null
    );
    app.commit("write").await.unwrap();
    let mut reopened = ApplicationTransactions::new(store.clone());
    reopened
        .begin_mode("read", ApplicationTransactionMode::Read, None)
        .await
        .unwrap();
    assert_eq!(
        rows(&mut reopened, "read")
            .await
            .iter()
            .map(|row| row.environment_id.as_str())
            .collect::<Vec<_>>(),
        ["b", "c"]
    );
    reopened.rollback("read").await.unwrap();
}

#[tokio::test]
async fn imported_peers_are_indexed_in_batches_and_rollback_restores_replacement() {
    let fixture = TestFixture::start().await;
    let store = fixture.initialize().await;
    let mut app = ApplicationTransactions::new(store.clone());
    app.initialize(schema()).await.unwrap();
    app.begin("import").await.unwrap();
    for i in 0..65 {
        let record = row(&format!("env-{i:03}"), &format!("label-{:03}", 64 - i));
        app.put(
            "import",
            TABLE,
            vec![text_cell(&record.environment_id)],
            record.stored(),
        )
        .await
        .unwrap();
    }
    app.commit("import").await.unwrap();
    let first = store.prepare_application_peers().await.unwrap();
    assert!(!first.prepared);
    assert_eq!(prepare(&store).await.indexed_rows, 65);
    assert_eq!(prepare(&store).await.indexed_rows, 65);
    app.begin("update").await.unwrap();
    let original = rows(&mut app, "update").await;
    assert_eq!(original.len(), 65);
    assert_eq!(original[0].environment_id, "env-064");
    let replacement = PeerRecord {
        secret_name: "replacement".into(),
        label: "!".into(),
        ..original[0].clone()
    };
    app.peers("update", PeerOperation::Upsert { row: replacement })
        .await
        .unwrap();
    app.rollback("update").await.unwrap();
    app.begin_mode("verify", ApplicationTransactionMode::Read, None)
        .await
        .unwrap();
    assert_eq!(rows(&mut app, "verify").await, original);
    app.rollback("verify").await.unwrap();
}
