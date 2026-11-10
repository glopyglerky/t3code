use std::collections::HashSet;

use serde_json::{Map, Value, json};

use super::*;
use crate::test_support::TestFixture;

fn schema() -> ApplicationSchema {
    ApplicationSchema {
        version: 1,
        tables: ["records", "receipts"]
            .into_iter()
            .map(|name| ApplicationTable {
                name: name.into(),
                columns: vec!["id".into(), "payload_json".into()],
                primary_key: vec!["id".into()],
            })
            .collect(),
    }
}

fn row(id: &str, value: Value) -> Map<String, Value> {
    json!({"id":id,"payload_json":value})
        .as_object()
        .unwrap()
        .clone()
}

async fn initialized(store: &Store) -> ApplicationTransactions {
    let mut transactions = ApplicationTransactions::new(store.clone());
    transactions.initialize(schema()).await.unwrap();
    transactions
}

#[tokio::test]
async fn application_schema_is_explicit_idempotent_and_immutable() {
    let fixture = TestFixture::start().await;
    let store = fixture.initialize().await;
    let mut transactions = ApplicationTransactions::new(store.clone());
    assert!(transactions.schema().await.unwrap().is_none());
    assert!(matches!(
        transactions.begin("before-init").await,
        Err(Error::Invalid(_))
    ));
    assert!(transactions.initialize(schema()).await.unwrap());
    assert_eq!(transactions.schema().await.unwrap(), Some(schema()));
    assert!(!transactions.initialize(schema()).await.unwrap());
    let mut changed = schema();
    changed.tables[0].columns.push("unexpected".into());
    assert!(matches!(
        transactions.initialize(changed).await,
        Err(Error::Invalid(_))
    ));
    transactions.begin("reader").await.unwrap();
    assert!(
        transactions
            .get("reader", "records", &[json!("one")])
            .await
            .unwrap()
            .is_none()
    );
    transactions.rollback("reader").await.unwrap();
    assert!(store.snapshot().await.unwrap().projections.is_empty());
    let mut corrupt = store.db.begin().await.unwrap();
    corrupt
        .put(
            schema_key(),
            br#"{"schema":{"version":99,"tables":[]},"generation":"native"}"#.to_vec(),
        )
        .await
        .unwrap();
    corrupt.commit().await.unwrap();
    assert!(matches!(
        transactions.schema().await,
        Err(Error::IncompatibleStore)
    ));
}

#[tokio::test]
async fn rows_commit_atomically_with_snapshot_isolation_and_restart() {
    let fixture = TestFixture::start().await;
    let store = fixture.initialize().await;
    let mut transactions = initialized(&store).await;
    transactions.begin("writer").await.unwrap();
    transactions.begin("reader-before").await.unwrap();
    let record = row("one", json!({"nested":true,"omitted":null}));
    transactions
        .put("writer", "records", vec![json!("one")], record.clone())
        .await
        .unwrap();
    transactions
        .put(
            "writer",
            "receipts",
            vec![json!("one")],
            row("one", json!("accepted")),
        )
        .await
        .unwrap();
    assert_eq!(
        transactions
            .get("writer", "records", &[json!("one")])
            .await
            .unwrap(),
        Some(record.clone())
    );
    assert!(
        transactions
            .get("reader-before", "records", &[json!("one")])
            .await
            .unwrap()
            .is_none()
    );
    transactions.commit("writer").await.unwrap();
    assert!(
        transactions
            .get("reader-before", "records", &[json!("one")])
            .await
            .unwrap()
            .is_none()
    );
    transactions.rollback("reader-before").await.unwrap();
    drop(transactions);
    store.shutdown().await.unwrap();

    let restarted = fixture.connect().await;
    let mut transactions = ApplicationTransactions::new(restarted.clone());
    transactions.begin("reader-after").await.unwrap();
    assert_eq!(
        transactions
            .get("reader-after", "records", &[json!("one")])
            .await
            .unwrap(),
        Some(record)
    );
    assert_eq!(
        transactions
            .get("reader-after", "receipts", &[json!("one")])
            .await
            .unwrap(),
        Some(row("one", json!("accepted")))
    );
    transactions.rollback("reader-after").await.unwrap();
}

#[tokio::test]
async fn conflicting_row_write_discards_every_staged_table_mutation() {
    let fixture = TestFixture::start().await;
    let store = fixture.initialize().await;
    let mut first = initialized(&store).await;
    let other_store = fixture.connect().await;
    let mut second = ApplicationTransactions::new(other_store);
    first.begin("one").await.unwrap();
    second.begin("two").await.unwrap();
    first
        .put(
            "one",
            "records",
            vec![json!("shared")],
            row("shared", json!(1)),
        )
        .await
        .unwrap();
    second
        .put(
            "two",
            "records",
            vec![json!("shared")],
            row("shared", json!(2)),
        )
        .await
        .unwrap();
    second
        .put(
            "two",
            "receipts",
            vec![json!("only-two")],
            row("only-two", json!(true)),
        )
        .await
        .unwrap();
    first.commit("one").await.unwrap();
    assert!(matches!(
        second.commit("two").await,
        Err(Error::TransactionConflict)
    ));
    assert!(matches!(second.commit("two").await, Err(Error::Invalid(_))));
    first.begin("verify").await.unwrap();
    assert_eq!(
        first
            .get("verify", "records", &[json!("shared")])
            .await
            .unwrap(),
        Some(row("shared", json!(1)))
    );
    assert!(
        first
            .get("verify", "receipts", &[json!("only-two")])
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn observed_scan_prevents_phantom_write_commits() {
    let fixture = TestFixture::start().await;
    let store = fixture.initialize().await;
    let mut transactions = initialized(&store).await;
    transactions.begin("guarded").await.unwrap();
    assert!(
        transactions
            .scan("guarded", "records", None, 1)
            .await
            .unwrap()
            .rows
            .is_empty()
    );
    transactions.begin("inserter").await.unwrap();
    transactions
        .put(
            "inserter",
            "records",
            vec![json!("phantom")],
            row("phantom", json!(true)),
        )
        .await
        .unwrap();
    transactions.commit("inserter").await.unwrap();
    transactions
        .put(
            "guarded",
            "receipts",
            vec![json!("stale")],
            row("stale", json!(true)),
        )
        .await
        .unwrap();
    assert!(matches!(
        transactions.commit("guarded").await,
        Err(Error::TransactionConflict)
    ));
    transactions.begin("verify").await.unwrap();
    assert!(
        transactions
            .get("verify", "receipts", &[json!("stale")])
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn paginated_scans_see_staged_rows_and_deletes_without_crossing_tables() {
    let fixture = TestFixture::start().await;
    let store = fixture.initialize().await;
    let mut transactions = initialized(&store).await;
    transactions.begin("writer").await.unwrap();
    for id in 0..11 {
        let id = id.to_string();
        transactions
            .put("writer", "records", vec![json!(id)], row(&id, Value::Null))
            .await
            .unwrap();
    }
    transactions
        .put(
            "writer",
            "receipts",
            vec![json!("foreign")],
            row("foreign", Value::Null),
        )
        .await
        .unwrap();
    assert!(
        transactions
            .delete("writer", "records", &[json!("3")])
            .await
            .unwrap()
    );
    assert!(
        !transactions
            .delete("writer", "records", &[json!("absent")])
            .await
            .unwrap()
    );
    let mut after = None;
    let mut seen = HashSet::new();
    loop {
        let page = transactions
            .scan("writer", "records", after.as_deref(), 3)
            .await
            .unwrap();
        assert!(!page.rows.is_empty());
        for row in page.rows {
            assert!(seen.insert(row.key[0].as_str().unwrap().to_owned()));
        }
        after = page.next;
        if after.is_none() {
            break;
        }
    }
    assert_eq!(seen.len(), 10);
    assert!(!seen.contains("3") && !seen.contains("foreign"));
    transactions.commit("writer").await.unwrap();
}

#[tokio::test]
async fn validation_errors_abort_earlier_mutations_and_do_not_fall_back() {
    let fixture = TestFixture::start().await;
    let store = fixture.initialize().await;
    let mut transactions = initialized(&store).await;
    for invalid in [
        row("different", Value::Null),
        json!({"id":"two"}).as_object().unwrap().clone(),
    ] {
        transactions.begin("reusable").await.unwrap();
        transactions
            .put(
                "reusable",
                "receipts",
                vec![json!("one")],
                row("one", Value::Null),
            )
            .await
            .unwrap();
        assert!(matches!(
            transactions
                .put("reusable", "records", vec![json!("two")], invalid)
                .await,
            Err(Error::Invalid(_))
        ));
        assert!(matches!(
            transactions.commit("reusable").await,
            Err(Error::Invalid(_))
        ));
    }
    transactions.begin("verify").await.unwrap();
    assert!(
        transactions
            .get("verify", "receipts", &[json!("one")])
            .await
            .unwrap()
            .is_none()
    );
    assert!(matches!(
        transactions
            .scan("verify", "records", Some("bad-cursor"), 10)
            .await,
        Err(Error::Invalid(_))
    ));
    assert!(matches!(
        transactions.get("verify", "records", &[json!("one")]).await,
        Err(Error::Invalid(_))
    ));
}

#[tokio::test]
async fn dropping_and_expiring_sessions_discards_uncommitted_rows() {
    let fixture = TestFixture::start().await;
    let store = fixture.initialize().await;
    let mut transactions = initialized(&store).await;
    transactions.begin("abandoned").await.unwrap();
    transactions
        .put(
            "abandoned",
            "records",
            vec![json!("one")],
            row("one", Value::Null),
        )
        .await
        .unwrap();
    transactions.sessions.get_mut("abandoned").unwrap().touched = Instant::now() - MAX_IDLE;
    assert!(matches!(
        transactions.commit("abandoned").await,
        Err(Error::Invalid(_))
    ));
    transactions.begin("dropped").await.unwrap();
    transactions
        .put(
            "dropped",
            "records",
            vec![json!("two")],
            row("two", Value::Null),
        )
        .await
        .unwrap();
    drop(transactions);
    let mut transactions = ApplicationTransactions::new(store);
    transactions.begin("verify").await.unwrap();
    assert!(
        transactions
            .scan("verify", "records", None, 10)
            .await
            .unwrap()
            .rows
            .is_empty()
    );
}

#[tokio::test]
async fn typed_sqlite_cells_and_compound_primary_keys_round_trip_exactly() {
    let fixture = TestFixture::start().await;
    let store = fixture.initialize().await;
    let mut transactions = ApplicationTransactions::new(store);
    let schema = ApplicationSchema {
        version: 1,
        tables: vec![ApplicationTable {
            name: "odd/name 雪".into(),
            columns: vec!["integer".into(), "blob".into(), "payload".into()],
            primary_key: vec!["integer".into(), "blob".into()],
        }],
    };
    transactions.initialize(schema).await.unwrap();
    let key = vec![
        json!({"type":"integer","value":"9223372036854775807"}),
        json!({"type":"blob","value":"AAEC/w=="}),
    ];
    let row = json!({"integer":key[0],"blob":key[1],"payload":{"type":"real","value":"8000000000000000"}}).as_object().unwrap().clone();
    transactions.begin("typed").await.unwrap();
    transactions
        .put("typed", "odd/name 雪", key.clone(), row.clone())
        .await
        .unwrap();
    transactions.commit("typed").await.unwrap();
    transactions.begin("verify").await.unwrap();
    assert_eq!(
        transactions
            .get("verify", "odd/name 雪", &key)
            .await
            .unwrap(),
        Some(row)
    );
}

#[tokio::test]
async fn active_generation_change_conflicts_with_staged_application_writes() {
    let fixture = TestFixture::start().await;
    let store = fixture.initialize().await;
    let mut transactions = initialized(&store).await;
    transactions.begin("old-generation").await.unwrap();
    transactions
        .put(
            "old-generation",
            "records",
            vec![json!("one")],
            row("one", Value::Null),
        )
        .await
        .unwrap();
    let mut publish = store.db.begin().await.unwrap();
    publish
        .put(
            schema_key(),
            encode_catalog(&ApplicationCatalog {
                schema: schema(),
                generation: "a".repeat(64),
            })
            .unwrap(),
        )
        .await
        .unwrap();
    publish.commit().await.unwrap();
    assert!(matches!(
        transactions.commit("old-generation").await,
        Err(Error::TransactionConflict)
    ));
    transactions.begin("new-generation").await.unwrap();
    assert!(
        transactions
            .scan("new-generation", "records", None, 10)
            .await
            .unwrap()
            .rows
            .is_empty()
    );
}

#[tokio::test]
async fn corrupt_row_identity_is_rejected_before_reads_or_overwrites() {
    let fixture = TestFixture::start().await;
    let store = fixture.initialize().await;
    let mut transactions = initialized(&store).await;
    let mut corrupt = store.db.begin().await.unwrap();
    corrupt
        .put(
            row_key("native", "records", &[json!("expected")]).unwrap(),
            encode_row(&ApplicationRow {
                key: vec![json!("different")],
                row: row("different", Value::Null),
            })
            .unwrap(),
        )
        .await
        .unwrap();
    corrupt.commit().await.unwrap();
    transactions.begin("get").await.unwrap();
    assert!(matches!(
        transactions
            .get("get", "records", &[json!("expected")])
            .await,
        Err(Error::IncompatibleStore)
    ));
    transactions.begin("put").await.unwrap();
    assert!(matches!(
        transactions
            .put(
                "put",
                "records",
                vec![json!("expected")],
                row("expected", Value::Null)
            )
            .await,
        Err(Error::IncompatibleStore)
    ));
}

#[tokio::test]
async fn session_capacity_recovers_after_expiration_and_rejected_begin_preserves_other_sessions() {
    let fixture = TestFixture::start().await;
    let store = fixture.initialize().await;
    let mut transactions = initialized(&store).await;
    for index in 0..MAX_SESSIONS {
        transactions.begin(&index.to_string()).await.unwrap();
    }
    transactions
        .put(
            "0",
            "records",
            vec![json!("kept")],
            row("kept", Value::Null),
        )
        .await
        .unwrap();
    assert!(matches!(
        transactions.begin("overflow").await,
        Err(Error::Invalid(_))
    ));
    assert!(matches!(
        transactions.begin("0").await,
        Err(Error::Invalid(_))
    ));
    transactions.sessions.get_mut("1").unwrap().touched = Instant::now() - MAX_IDLE;
    transactions.begin("replacement").await.unwrap();
    transactions.commit("0").await.unwrap();
    transactions.discard_all();
    transactions.begin("verify").await.unwrap();
    assert_eq!(
        transactions
            .get("verify", "records", &[json!("kept")])
            .await
            .unwrap(),
        Some(row("kept", Value::Null))
    );
}

#[tokio::test]
async fn transaction_write_budget_failure_discards_preceding_large_mutation() {
    let fixture = TestFixture::start().await;
    let store = fixture.initialize().await;
    let mut transactions = initialized(&store).await;
    transactions.begin("oversized").await.unwrap();
    let payload = Value::String("x".repeat(9 * 1024 * 1024));
    transactions
        .put(
            "oversized",
            "records",
            vec![json!("first")],
            row("first", payload.clone()),
        )
        .await
        .unwrap();
    assert!(matches!(
        transactions
            .put(
                "oversized",
                "records",
                vec![json!("second")],
                row("second", payload)
            )
            .await,
        Err(Error::Invalid(_))
    ));
    assert!(matches!(
        transactions.commit("oversized").await,
        Err(Error::Invalid(_))
    ));
    transactions.begin("verify").await.unwrap();
    assert!(
        transactions
            .scan("verify", "records", None, 10)
            .await
            .unwrap()
            .rows
            .is_empty()
    );
}
