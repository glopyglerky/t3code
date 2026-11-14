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
    transactions
        .begin_mode("reader-before", ApplicationTransactionMode::Read, None)
        .await
        .unwrap();
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
async fn writer_is_reserved_before_begin_and_waiter_reads_the_committed_state() {
    let fixture = TestFixture::start().await;
    let store = fixture.initialize().await;
    let mut first = initialized(&store).await;
    let other_store = fixture.connect().await;
    let mut second = ApplicationTransactions::new(other_store);
    first.begin("one").await.unwrap();
    first
        .put(
            "one",
            "records",
            vec![json!("shared")],
            row("shared", json!(1)),
        )
        .await
        .unwrap();
    assert!(matches!(
        second
            .begin_mode("too-soon", ApplicationTransactionMode::Write, Some(20))
            .await,
        Err(Error::WriterBusy)
    ));
    assert!(!second.sessions.contains_key("too-soon"));
    let mut waiter = tokio::spawn(async move {
        second
            .begin_mode("after-one", ApplicationTransactionMode::Write, Some(1000))
            .await
            .unwrap();
        let current = second
            .get("after-one", "records", &[json!("shared")])
            .await
            .unwrap();
        second
            .put(
                "after-one",
                "receipts",
                vec![json!("second")],
                row("second", json!(true)),
            )
            .await
            .unwrap();
        second.commit("after-one").await.unwrap();
        current
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut waiter)
            .await
            .is_err()
    );
    first.commit("one").await.unwrap();
    assert_eq!(waiter.await.unwrap(), Some(row("shared", json!(1))));
    first.begin("verify").await.unwrap();
    assert_eq!(
        first
            .get("verify", "receipts", &[json!("second")])
            .await
            .unwrap(),
        Some(row("second", json!(true)))
    );
}

#[tokio::test]
async fn writer_reservation_prevents_scan_phantoms_before_callback_work() {
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
    assert!(matches!(
        transactions
            .begin_mode("inserter", ApplicationTransactionMode::Write, Some(20))
            .await,
        Err(Error::WriterBusy)
    ));
    transactions
        .put(
            "guarded",
            "receipts",
            vec![json!("guarded")],
            row("guarded", json!(true)),
        )
        .await
        .unwrap();
    transactions.commit("guarded").await.unwrap();
    transactions.begin("inserter").await.unwrap();
    transactions
        .put(
            "inserter",
            "records",
            vec![json!("later")],
            row("later", json!(true)),
        )
        .await
        .unwrap();
    transactions.commit("inserter").await.unwrap();
    transactions.begin("verify").await.unwrap();
    assert_eq!(
        transactions
            .get("verify", "receipts", &[json!("guarded")])
            .await
            .unwrap(),
        Some(row("guarded", json!(true)))
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
async fn historical_compound_row_key_is_read_unchanged_after_reopen() {
    // Frozen pre-change physical key, including the table's UTF-8 hex prefix.
    // The seed must not use row_key/request_digest from the code being tested.
    const HISTORICAL_KEY: &[u8] = b"t3-code-ls/v2/application/generation/native/row/6f64642f6e616d6520e99baa/6c81a2a1f30e730dbd885c6f569d1e597432e560cc36269b12bc66bad0dfce1b";
    let fixture = TestFixture::start().await;
    let store = fixture.initialize().await;
    let mut transactions = ApplicationTransactions::new(store.clone());
    transactions
        .initialize(ApplicationSchema {
            version: 1,
            tables: vec![ApplicationTable {
                name: "odd/name 雪".into(),
                columns: vec!["integer".into(), "blob".into(), "payload".into()],
                primary_key: vec!["integer".into(), "blob".into()],
            }],
        })
        .await
        .unwrap();
    let key = vec![
        json!({"type":"integer","value":"9223372036854775807"}),
        json!({"type":"blob","value":"AAEC/w=="}),
    ];
    let row = json!({
        "integer":key[0], "blob":key[1],
        "payload":{"type":"real","value":"8000000000000000"},
    })
    .as_object()
    .unwrap()
    .clone();
    let stored = encode_row(&ApplicationRow {
        key: key.clone(),
        row: row.clone(),
    })
    .unwrap();
    let mut seed = store.db.begin().await.unwrap();
    seed.put(HISTORICAL_KEY.to_vec(), stored.clone())
        .await
        .unwrap();
    seed.commit().await.unwrap();
    drop(transactions);
    store.shutdown().await.unwrap();

    let reopened = fixture.connect().await;
    let mut transactions = ApplicationTransactions::new(reopened.clone());
    transactions
        .begin_mode("old-key", ApplicationTransactionMode::Read, None)
        .await
        .unwrap();
    assert_eq!(
        transactions
            .get("old-key", "odd/name 雪", &key)
            .await
            .unwrap(),
        Some(row)
    );
    transactions.rollback("old-key").await.unwrap();
    let mut inspect = reopened.db.begin_read_only().await.unwrap();
    assert_eq!(inspect.get(HISTORICAL_KEY).await.unwrap(), Some(stored));
    inspect.rollback().await.unwrap();
    drop(transactions);
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
async fn managed_tables_reject_generic_mutations_without_losing_domain_rows() {
    let fixture = TestFixture::start().await;
    let store = fixture.initialize().await;
    let mut transactions = initialized(&store).await;
    transactions.begin("seed").await.unwrap();
    transactions
        .put(
            "seed",
            "records",
            vec![json!("one")],
            row("one", json!("kept")),
        )
        .await
        .unwrap();
    transactions.commit("seed").await.unwrap();
    let mut mark = store.db.begin().await.unwrap();
    mark.put(
        store_key("application/generation/native/managed/7265636f726473"),
        b"managed".to_vec(),
    )
    .await
    .unwrap();
    mark.commit().await.unwrap();
    transactions.begin("put").await.unwrap();
    assert!(matches!(
        transactions
            .put(
                "put",
                "records",
                vec![json!("one")],
                row("one", Value::Null)
            )
            .await,
        Err(Error::Invalid(_))
    ));
    transactions.begin("delete").await.unwrap();
    assert!(matches!(
        transactions
            .delete("delete", "records", &[json!("one")])
            .await,
        Err(Error::Invalid(_))
    ));
    transactions.begin("read").await.unwrap();
    assert_eq!(
        transactions
            .get("read", "records", &[json!("one")])
            .await
            .unwrap(),
        Some(row("one", json!("kept")))
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
        transactions
            .begin_mode(
                &index.to_string(),
                if index == 0 {
                    ApplicationTransactionMode::Write
                } else {
                    ApplicationTransactionMode::Read
                },
                None,
            )
            .await
            .unwrap();
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
    transactions
        .begin_mode("replacement", ApplicationTransactionMode::Read, None)
        .await
        .unwrap();
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
        Err(Error::ResourceLimit(crate::ResourceLimit::MutationBytes))
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

#[tokio::test]
async fn resource_limit_inside_savepoint_can_roll_back_and_commit_outer_rows() {
    let fixture = TestFixture::start().await;
    let store = fixture.initialize().await;
    let mut transactions = initialized(&store).await;
    transactions.begin("outer").await.unwrap();
    transactions
        .put(
            "outer",
            "receipts",
            vec![json!("kept")],
            row("kept", Value::Null),
        )
        .await
        .unwrap();
    transactions.savepoint("outer", "bounded").await.unwrap();
    let payload = Value::String("x".repeat(9 * 1024 * 1024));
    transactions
        .put(
            "outer",
            "records",
            vec![json!("first")],
            row("first", payload.clone()),
        )
        .await
        .unwrap();
    let error = transactions
        .put(
            "outer",
            "records",
            vec![json!("second")],
            row("second", payload),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        Error::ResourceLimit(crate::ResourceLimit::MutationBytes)
    ));
    assert_eq!(
        error.to_string(),
        "Invalid database request: transaction mutations exceed 16 MiB"
    );
    assert!(matches!(
        transactions
            .get("outer", "receipts", &[json!("kept")])
            .await,
        Err(Error::Invalid(_))
    ));
    transactions.rollback_to("outer", "bounded").await.unwrap();
    transactions.release("outer", "bounded").await.unwrap();
    transactions.commit("outer").await.unwrap();

    transactions.begin("verify").await.unwrap();
    assert!(
        transactions
            .scan("verify", "records", None, 10)
            .await
            .unwrap()
            .rows
            .is_empty()
    );
    assert_eq!(
        transactions
            .get("verify", "receipts", &[json!("kept")])
            .await
            .unwrap(),
        Some(row("kept", Value::Null))
    );
}

#[tokio::test]
async fn nested_release_keeps_writes_private_until_outer_commit_and_rollback_restores_overlays() {
    let fixture = TestFixture::start().await;
    let store = fixture.initialize().await;
    let mut transactions = initialized(&store).await;
    transactions.begin("outer").await.unwrap();
    transactions
        .put(
            "outer",
            "records",
            vec![json!("one")],
            row("one", json!("outer")),
        )
        .await
        .unwrap();
    transactions.savepoint("outer", "first").await.unwrap();
    transactions
        .put(
            "outer",
            "records",
            vec![json!("one")],
            row("one", json!("first")),
        )
        .await
        .unwrap();
    transactions
        .put("outer", "records", vec![json!("two")], row("two", json!(2)))
        .await
        .unwrap();
    transactions.savepoint("outer", "second").await.unwrap();
    transactions
        .delete("outer", "records", &[json!("one")])
        .await
        .unwrap();
    transactions
        .put(
            "outer",
            "receipts",
            vec![json!("discard")],
            row("discard", Value::Null),
        )
        .await
        .unwrap();
    transactions.rollback_to("outer", "second").await.unwrap();
    assert_eq!(
        transactions
            .get("outer", "records", &[json!("one")])
            .await
            .unwrap(),
        Some(row("one", json!("first")))
    );
    assert!(
        transactions
            .get("outer", "receipts", &[json!("discard")])
            .await
            .unwrap()
            .is_none()
    );
    transactions.release("outer", "second").await.unwrap();
    transactions.release("outer", "first").await.unwrap();
    transactions
        .begin_mode("observer", ApplicationTransactionMode::Read, None)
        .await
        .unwrap();
    assert!(
        transactions
            .scan("observer", "records", None, 10)
            .await
            .unwrap()
            .rows
            .is_empty()
    );
    transactions.rollback("observer").await.unwrap();
    transactions.commit("outer").await.unwrap();
    transactions.begin("after").await.unwrap();
    assert_eq!(
        transactions
            .scan("after", "records", None, 10)
            .await
            .unwrap()
            .rows
            .len(),
        2
    );
}

#[tokio::test]
async fn recoverable_inner_failure_rolls_back_only_its_scope_and_allows_later_siblings() {
    let fixture = TestFixture::start().await;
    let store = fixture.initialize().await;
    let mut transactions = initialized(&store).await;
    transactions.begin("outer").await.unwrap();
    transactions
        .put(
            "outer",
            "records",
            vec![json!("one")],
            row("one", json!("outer")),
        )
        .await
        .unwrap();
    transactions.savepoint("outer", "failed").await.unwrap();
    transactions
        .put(
            "outer",
            "records",
            vec![json!("one")],
            row("one", json!("inner")),
        )
        .await
        .unwrap();
    transactions
        .put(
            "outer",
            "records",
            vec![json!("two")],
            row("two", Value::Null),
        )
        .await
        .unwrap();
    assert!(matches!(
        transactions
            .put(
                "outer",
                "receipts",
                vec![json!("bad")],
                row("mismatch", Value::Null)
            )
            .await,
        Err(Error::Invalid(_))
    ));
    assert!(matches!(
        transactions.get("outer", "records", &[json!("one")]).await,
        Err(Error::Invalid(_))
    ));
    assert!(matches!(
        transactions.release("outer", "failed").await,
        Err(Error::Invalid(_))
    ));
    assert!(matches!(
        transactions.commit("outer").await,
        Err(Error::Invalid(_))
    ));
    transactions.rollback_to("outer", "failed").await.unwrap();
    transactions.release("outer", "failed").await.unwrap();
    assert_eq!(
        transactions
            .get("outer", "records", &[json!("one")])
            .await
            .unwrap(),
        Some(row("one", json!("outer")))
    );
    assert!(
        transactions
            .get("outer", "records", &[json!("two")])
            .await
            .unwrap()
            .is_none()
    );
    transactions.savepoint("outer", "sibling").await.unwrap();
    transactions
        .put(
            "outer",
            "receipts",
            vec![json!("kept")],
            row("kept", Value::Null),
        )
        .await
        .unwrap();
    transactions.release("outer", "sibling").await.unwrap();
    transactions.commit("outer").await.unwrap();
    transactions.begin("verify").await.unwrap();
    assert_eq!(
        transactions
            .get("verify", "records", &[json!("one")])
            .await
            .unwrap(),
        Some(row("one", json!("outer")))
    );
    assert_eq!(
        transactions
            .get("verify", "receipts", &[json!("kept")])
            .await
            .unwrap(),
        Some(row("kept", Value::Null))
    );
}

#[tokio::test]
async fn corruption_inside_a_savepoint_discards_every_outer_mutation() {
    let fixture = TestFixture::start().await;
    let store = fixture.initialize().await;
    let mut transactions = initialized(&store).await;
    let mut corrupt = store.db.begin().await.unwrap();
    corrupt
        .put(
            row_key("native", "records", &[json!("bad")]).unwrap(),
            b"invalid".to_vec(),
        )
        .await
        .unwrap();
    corrupt.commit().await.unwrap();
    transactions.begin("outer").await.unwrap();
    transactions
        .put(
            "outer",
            "receipts",
            vec![json!("discard")],
            row("discard", Value::Null),
        )
        .await
        .unwrap();
    transactions.savepoint("outer", "nested").await.unwrap();
    assert!(matches!(
        transactions.get("outer", "records", &[json!("bad")]).await,
        Err(Error::IncompatibleStore)
    ));
    assert!(matches!(
        transactions.rollback_to("outer", "nested").await,
        Err(Error::Invalid(_))
    ));
    assert!(matches!(
        transactions.commit("outer").await,
        Err(Error::Invalid(_))
    ));
    transactions.begin("verify").await.unwrap();
    assert!(
        transactions
            .get("verify", "receipts", &[json!("discard")])
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn readonly_snapshot_rejects_writes_but_recovers_an_inner_scope() {
    let fixture = TestFixture::start().await;
    let store = fixture.initialize().await;
    let mut transactions = initialized(&store).await;
    transactions
        .begin_mode("reader", ApplicationTransactionMode::Read, None)
        .await
        .unwrap();
    transactions.savepoint("reader", "nested").await.unwrap();
    assert!(matches!(
        transactions
            .put(
                "reader",
                "records",
                vec![json!("blocked")],
                row("blocked", Value::Null)
            )
            .await,
        Err(Error::Invalid(_))
    ));
    transactions.rollback_to("reader", "nested").await.unwrap();
    transactions.release("reader", "nested").await.unwrap();
    assert!(
        transactions
            .scan("reader", "records", None, 10)
            .await
            .unwrap()
            .rows
            .is_empty()
    );
    transactions.commit("reader").await.unwrap();
}

#[tokio::test]
async fn savepoint_memory_limits_do_not_discard_the_recoverable_outer_scope() {
    let fixture = TestFixture::start().await;
    let store = fixture.initialize().await;
    let mut transactions = initialized(&store).await;
    transactions.begin("outer").await.unwrap();
    transactions
        .put(
            "outer",
            "records",
            vec![json!("large")],
            row("large", json!("x".repeat(9 * 1024 * 1024))),
        )
        .await
        .unwrap();
    transactions.savepoint("outer", "first").await.unwrap();
    assert!(matches!(
        transactions.savepoint("outer", "exceeds-budget").await,
        Err(Error::Invalid(_))
    ));
    transactions.rollback_to("outer", "first").await.unwrap();
    transactions.release("outer", "first").await.unwrap();
    transactions.commit("outer").await.unwrap();
    transactions.begin("verify").await.unwrap();
    assert_eq!(
        transactions
            .get("verify", "records", &[json!("large")])
            .await
            .unwrap()
            .unwrap()["payload_json"]
            .as_str()
            .unwrap()
            .len(),
        9 * 1024 * 1024
    );
}

#[tokio::test]
async fn idle_reservation_expires_without_another_request_and_releases_the_writer() {
    let fixture = TestFixture::start().await;
    let store = fixture.initialize().await;
    let peer = fixture.connect().await;
    let mut idle = store
        .db
        .begin_reserved(Duration::from_secs(1), Duration::from_millis(30))
        .await
        .unwrap();
    idle.put(store_key("test/expiry"), b"discard".to_vec())
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(80)).await;
    let replacement = peer
        .db
        .begin_reserved(Duration::from_millis(100), Duration::from_secs(1))
        .await
        .unwrap();
    assert!(matches!(idle.commit().await, Err(Error::Invalid(_))));
    replacement.rollback().await.unwrap();
    assert!(
        store
            .db
            .read(&store_key("test/expiry"))
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn cancelling_writer_acquisition_cannot_later_claim_an_abandoned_transaction() {
    let fixture = TestFixture::start().await;
    let store = fixture.initialize().await;
    let held = store
        .db
        .begin_reserved(Duration::from_secs(1), Duration::from_secs(1))
        .await
        .unwrap();
    // This peer has never opened a writable handle and must wait for promotion,
    // not merely for try_begin_write on an already-open database.
    let peer = tokio::time::timeout(Duration::from_secs(1), fixture.connect())
        .await
        .unwrap();
    let waiting_store = peer.clone();
    let mut waiting = tokio::spawn(async move {
        waiting_store
            .db
            .begin_reserved(Duration::from_secs(1), Duration::from_secs(1))
            .await
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut waiting)
            .await
            .is_err()
    );
    waiting.abort();
    assert!(matches!(waiting.await, Err(error) if error.is_cancelled()));
    held.rollback().await.unwrap();
    let next = peer
        .db
        .begin_reserved(Duration::from_millis(100), Duration::from_secs(1))
        .await
        .unwrap();
    next.rollback().await.unwrap();
}

#[tokio::test]
async fn new_helper_reads_beside_a_reserved_writer_and_promotes_without_changing_old_snapshots() {
    let fixture = TestFixture::start().await;
    let store = fixture.initialize().await;
    let mut owner = initialized(&store).await;
    owner.begin("seed").await.unwrap();
    owner
        .put("seed", "records", vec![json!("one")], row("one", json!(1)))
        .await
        .unwrap();
    owner.commit("seed").await.unwrap();
    owner.begin("held").await.unwrap();
    owner
        .put("held", "records", vec![json!("one")], row("one", json!(2)))
        .await
        .unwrap();

    let peer_store = tokio::time::timeout(Duration::from_secs(1), fixture.connect())
        .await
        .unwrap();
    let mut peer = ApplicationTransactions::new(peer_store);
    peer.begin_mode("old-reader", ApplicationTransactionMode::Read, None)
        .await
        .unwrap();
    assert_eq!(
        peer.get("old-reader", "records", &[json!("one")])
            .await
            .unwrap(),
        Some(row("one", json!(1)))
    );
    assert!(matches!(
        peer.begin_mode("blocked", ApplicationTransactionMode::Write, Some(20))
            .await,
        Err(Error::WriterBusy)
    ));

    owner.commit("held").await.unwrap();
    peer.begin_mode("promoted", ApplicationTransactionMode::Write, Some(1000))
        .await
        .unwrap();
    assert_eq!(
        peer.get("promoted", "records", &[json!("one")])
            .await
            .unwrap(),
        Some(row("one", json!(2)))
    );
    peer.put(
        "promoted",
        "receipts",
        vec![json!("published")],
        row("published", Value::Null),
    )
    .await
    .unwrap();
    peer.commit("promoted").await.unwrap();
    assert_eq!(
        peer.get("old-reader", "records", &[json!("one")])
            .await
            .unwrap(),
        Some(row("one", json!(1)))
    );
    peer.rollback("old-reader").await.unwrap();
    owner.begin("verify").await.unwrap();
    assert_eq!(
        owner
            .get("verify", "receipts", &[json!("published")])
            .await
            .unwrap(),
        Some(row("published", Value::Null))
    );
}

#[tokio::test]
async fn existing_empty_and_invalid_files_are_not_initialized_or_selected_as_readonly() {
    let temporary = tempfile::tempdir().unwrap();
    for bytes in [Vec::new(), b"not a redb database".to_vec()] {
        let path = temporary.path().join(if bytes.is_empty() {
            "empty.redb"
        } else {
            "invalid.redb"
        });
        std::fs::write(&path, &bytes).unwrap();
        assert!(matches!(
            Store::open_local(&path).await,
            Err(Error::Database(_))
        ));
        assert_eq!(std::fs::read(&path).unwrap(), bytes);
    }
}

#[tokio::test]
async fn pathname_replacement_refuses_promotion_and_commit_without_touching_either_store() {
    let fixture = TestFixture::start().await;
    let replacement = TestFixture::start().await;
    let store = fixture.initialize().await;
    let mut owner = initialized(&store).await;
    owner.begin("held").await.unwrap();
    owner
        .put(
            "held",
            "records",
            vec![json!("discard")],
            row("discard", Value::Null),
        )
        .await
        .unwrap();
    let mut peer = ApplicationTransactions::new(fixture.connect().await);

    let replacement_store = replacement.initialize().await;
    let mut replacement_rows = initialized(&replacement_store).await;
    replacement_rows.begin("seed").await.unwrap();
    replacement_rows
        .put(
            "seed",
            "records",
            vec![json!("untouched")],
            row("untouched", json!("replacement")),
        )
        .await
        .unwrap();
    replacement_rows.commit("seed").await.unwrap();
    drop(replacement_rows);
    replacement_store.shutdown().await.unwrap();
    let replacement_bytes = std::fs::read(replacement.path()).unwrap();
    let moved = fixture.path().with_file_name("original.redb");
    std::fs::rename(fixture.path(), &moved).unwrap();
    std::fs::rename(replacement.path(), fixture.path()).unwrap();

    assert!(matches!(
        peer.begin_mode("promote", ApplicationTransactionMode::Write, Some(20))
            .await,
        Err(Error::IncompatibleStore)
    ));
    assert!(matches!(
        owner.commit("held").await,
        Err(Error::IncompatibleStore)
    ));
    assert!(matches!(
        owner.begin("cached-writer").await,
        Err(Error::IncompatibleStore)
    ));
    assert_eq!(std::fs::read(fixture.path()).unwrap(), replacement_bytes);
    drop(peer);
    drop(owner);
    store.shutdown().await.unwrap();

    let original = Store::open_local(&moved).await.unwrap();
    let mut original_rows = ApplicationTransactions::new(original.clone());
    original_rows
        .begin_mode("verify", ApplicationTransactionMode::Read, None)
        .await
        .unwrap();
    assert!(
        original_rows
            .get("verify", "records", &[json!("discard")])
            .await
            .unwrap()
            .is_none()
    );
    original_rows.rollback("verify").await.unwrap();
    drop(original_rows);
    original.shutdown().await.unwrap();
}

#[tokio::test]
async fn helper_writer_guard_expires_owners_without_touching_live_read_snapshots() {
    let fixture = TestFixture::start().await;
    let store = fixture.initialize().await;
    let mut transactions = initialized(&store).await;
    transactions
        .begin_mode("reader", ApplicationTransactionMode::Read, None)
        .await
        .unwrap();
    assert!(!transactions.has_active_writer());
    transactions.begin("writer").await.unwrap();
    assert!(transactions.has_active_writer());
    transactions.sessions.get_mut("writer").unwrap().touched = Instant::now() - MAX_IDLE;
    assert!(!transactions.has_active_writer());
    transactions.rollback("reader").await.unwrap();
    transactions
        .begin_mode("replacement", ApplicationTransactionMode::Write, Some(1000))
        .await
        .unwrap();
    transactions.rollback("replacement").await.unwrap();
}
