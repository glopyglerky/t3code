use super::*;
use crate::application::{
    ApplicationSchema, ApplicationTable, ApplicationTransactionMode, encode_catalog,
};
use crate::test_support::TestFixture;

const NOW: &str = "2026-11-10T12:00:00.000Z";
const LATER: &str = "2026-11-10T12:01:00.000Z";

fn schema() -> ApplicationSchema {
    ApplicationSchema {
        version: 1,
        tables: vec![
            ApplicationTable {
                name: TABLE.into(),
                columns: COLUMNS.iter().map(|s| (*s).into()).collect(),
                primary_key: vec!["effect_id".into()],
            },
            ApplicationTable {
                name: "other".into(),
                columns: vec!["id".into(), "value".into()],
                primary_key: vec!["id".into()],
            },
        ],
    }
}

fn pending(id: &str, thread: &str) -> outbox::EnqueueEffect {
    outbox::EnqueueEffect {
        id: id.into(),
        command_id: "command".into(),
        thread_id: thread.into(),
        request: json!({"type":"terminal.cleanup"}),
        available_at: None,
    }
}

fn claim(stream: &str, sequence: u64, now: &str) -> outbox::ClaimOnce {
    outbox::ClaimOnce {
        stream_id: stream.into(),
        sequence,
        claim: outbox::Claim {
            worker_id: "worker".into(),
            now: now.into(),
            exclude_restart_continuations: false,
        },
    }
}

fn imported_row(id: &str, thread: &str) -> Map<String, Value> {
    let mut row = Map::new();
    for (name, value) in [
        ("effect_id", id),
        ("command_id", "command"),
        ("thread_id", thread),
        ("effect_type", "terminal.cleanup"),
        (
            "payload_json",
            " { \"type\" : \"terminal.cleanup\", \"future\": [1,2] } ",
        ),
        ("status", "pending"),
        ("available_at", NOW),
        ("created_at", NOW),
        ("updated_at", NOW),
    ] {
        row.insert(name.into(), cells::text_cell(value));
    }
    row.insert("attempt_count".into(), cells::int_cell(0));
    for name in ["lease_owner", "completed_at", "last_error"] {
        row.insert(name.into(), cells::nullable_cell(None));
    }
    row.insert(
        "lease_expires_at".into(),
        cells::text_cell("retained historical lease"),
    );
    row
}

async fn imported(store: &Store, rows: Vec<Map<String, Value>>) -> ApplicationCatalog {
    let catalog = ApplicationCatalog {
        schema: schema(),
        generation: "a".repeat(64),
    };
    let mut tx = store.db.begin().await.unwrap();
    tx.insert(schema_key(), encode_catalog(&catalog).unwrap())
        .await
        .unwrap();
    for (ordinal, row) in rows.into_iter().enumerate() {
        let key = vec![row["effect_id"].clone()];
        cells::write_row(&mut tx, &catalog, TABLE, row)
            .await
            .unwrap();
        let witness = store_key(&format!(
            "application/generation/{}/metadata/source/{}",
            catalog.generation,
            request_digest(&json!([TABLE, key]))
        ));
        tx.insert(
            witness,
            crate::encode(&json!({"table":TABLE,"key":key,"ordinal":ordinal})).unwrap(),
        )
        .await
        .unwrap();
    }
    tx.commit().await.unwrap();
    catalog
}

async fn prepare(store: &Store) -> PreparationResult {
    loop {
        let result = store.prepare_application_outbox().await.unwrap();
        if result.prepared {
            return result;
        }
    }
}

#[tokio::test]
async fn outbox_and_other_application_rows_share_rollback_savepoints_and_commit() {
    let fixture = TestFixture::start().await;
    let store = fixture.initialize().await;
    let mut app = ApplicationTransactions::new(store.clone());
    app.initialize(schema()).await.unwrap();
    prepare(&store).await;
    app.begin("write").await.unwrap();
    app.put(
        "write",
        "other",
        vec![json!("one")],
        json!({"id":"one","value":"coupled"})
            .as_object()
            .unwrap()
            .clone(),
    )
    .await
    .unwrap();
    assert_eq!(
        app.outbox_enqueue("write", &[pending("one", "thread")], NOW)
            .await
            .unwrap(),
        1
    );
    app.savepoint("write", "inner").await.unwrap();
    assert_eq!(
        app.outbox_cancel(
            "write",
            &outbox::CancelUnsettled {
                thread_id: "thread".into(),
                effect_types: vec![outbox::EffectType::TerminalCleanup],
                reason: "cancelled in savepoint".into(),
                now: NOW.into(),
            }
        )
        .await
        .unwrap(),
        vec!["one"]
    );
    app.rollback_to("write", "inner").await.unwrap();
    app.release("write", "inner").await.unwrap();
    assert_eq!(
        app.outbox_get("write", "one")
            .await
            .unwrap()
            .unwrap()
            .status,
        outbox::EffectStatus::Pending
    );
    app.rollback("write").await.unwrap();
    app.begin("check").await.unwrap();
    assert!(
        app.get("check", "other", &[json!("one")])
            .await
            .unwrap()
            .is_none()
    );
    assert!(app.outbox_get("check", "one").await.unwrap().is_none());
    assert_eq!(
        app.outbox_enqueue("check", &[pending("one", "thread")], NOW)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        app.outbox_get("check", "one")
            .await
            .unwrap()
            .unwrap()
            .insertion_ordinal,
        1
    );
    app.commit("check").await.unwrap();
    assert!(
        store.outbox_get("one").await.unwrap().is_none(),
        "legacy namespace must remain independent"
    );
    drop(app);
    store.shutdown().await.unwrap();
    let store = fixture.connect().await;
    let mut app = ApplicationTransactions::new(store);
    app.begin_mode("reopened", ApplicationTransactionMode::Read, None)
        .await
        .unwrap();
    assert_eq!(
        app.outbox_get("reopened", "one").await.unwrap().unwrap().id,
        "one"
    );
    app.rollback("reopened").await.unwrap();
}

#[tokio::test]
async fn imported_fifo_raw_payload_and_lease_cells_survive_claim_settle_and_reenqueue() {
    let fixture = TestFixture::start().await;
    let store = fixture.initialize().await;
    let mut first = imported_row("z-first", "thread");
    first.insert("available_at".into(), cells::text_cell(LATER));
    let second = imported_row("a-second", "thread");
    let original_payload = first["payload_json"].clone();
    let original_lease = first["lease_expires_at"].clone();
    imported(&store, vec![first, second]).await;
    assert_eq!(prepare(&store).await.indexed_rows, 2);
    let mut app = ApplicationTransactions::new(store.clone());
    app.begin("write").await.unwrap();
    assert_eq!(
        app.outbox_claim_once("write", &claim("poll", 1, NOW))
            .await
            .unwrap()
            .state,
        outbox::ClaimState::Empty
    );
    assert_eq!(
        app.outbox_next_claimable_at("write")
            .await
            .unwrap()
            .as_deref(),
        Some(LATER)
    );
    let request = claim("claim", 1, LATER);
    let selected = app.outbox_claim_once("write", &request).await.unwrap();
    assert_eq!(selected.effect.as_ref().unwrap().id, "z-first");
    let raw = app
        .get("write", TABLE, &[cells::text_cell("z-first")])
        .await
        .unwrap()
        .unwrap();
    assert_eq!(raw["payload_json"], original_payload);
    assert_eq!(raw["lease_expires_at"], original_lease);
    assert!(
        app.outbox_succeed(
            "write",
            &outbox::Settle {
                effect_id: "z-first".into(),
                worker_id: "worker".into(),
                now: LATER.into()
            }
        )
        .await
        .unwrap()
    );
    let raw = app
        .get("write", TABLE, &[cells::text_cell("z-first")])
        .await
        .unwrap()
        .unwrap();
    assert_eq!(raw["payload_json"], original_payload);
    assert_eq!(raw["lease_expires_at"], original_lease);
    assert_eq!(
        app.outbox_prune(
            "write",
            &outbox::Prune {
                before: "2026-12-10T12:00:00.000Z".into(),
                limit: 500
            }
        )
        .await
        .unwrap(),
        1
    );
    app.outbox_enqueue("write", &[pending("z-first", "other-thread")], LATER)
        .await
        .unwrap();
    assert_eq!(
        app.outbox_get("write", "z-first")
            .await
            .unwrap()
            .unwrap()
            .insertion_ordinal,
        3
    );
    let replay = app.outbox_claim_once("write", &request).await.unwrap();
    assert_eq!(replay.state, outbox::ClaimState::Retired);
    assert!(replay.replayed);
    app.commit("write").await.unwrap();
}

#[tokio::test]
async fn preparation_resumes_after_reopen_and_blocks_generic_mutation() {
    let fixture = TestFixture::start().await;
    let store = fixture.initialize().await;
    imported(
        &store,
        (0..65)
            .map(|n| imported_row(&format!("effect-{n}"), &format!("thread-{n}")))
            .collect(),
    )
    .await;
    let partial = store.prepare_application_outbox().await.unwrap();
    assert!(!partial.prepared);
    assert_eq!(partial.indexed_rows, 32);
    let mut app = ApplicationTransactions::new(store.clone());
    app.begin("managed").await.unwrap();
    assert!(
        app.delete("managed", TABLE, &[cells::text_cell("effect-0")])
            .await
            .is_err()
    );
    assert!(
        app.rollback("managed").await.is_err(),
        "the rejected root session was already aborted"
    );
    drop(app);
    store.shutdown().await.unwrap();
    let store = fixture.connect().await;
    assert_eq!(prepare(&store).await.indexed_rows, 65);
    assert_eq!(prepare(&store).await.indexed_rows, 65);
    let mut app = ApplicationTransactions::new(store);
    app.begin("write").await.unwrap();
    assert_eq!(app.outbox_list("write", "command").await.unwrap().len(), 65);
    app.outbox_enqueue("write", &[pending("new", "new-thread")], NOW)
        .await
        .unwrap();
    assert_eq!(
        app.outbox_get("write", "new")
            .await
            .unwrap()
            .unwrap()
            .insertion_ordinal,
        66
    );
    app.commit("write").await.unwrap();
}

#[tokio::test]
async fn malformed_source_does_not_publish_indexes_or_rewrite_cells() {
    let fixture = TestFixture::start().await;
    let store = fixture.initialize().await;
    let mut original = imported_row("bad", "thread");
    original.insert(
        "payload_json".into(),
        json!({"type":"text","encoding":"base64","value":"gA=="}),
    );
    let catalog = imported(&store, vec![original.clone()]).await;
    assert!(matches!(
        store.prepare_application_outbox().await,
        Err(Error::IncompatibleStore)
    ));
    let mut tx = store.db.begin().await.unwrap();
    assert!(
        tx.get(&cells::managed_marker(&catalog, TABLE))
            .await
            .unwrap()
            .is_none()
    );
    let actual = cells::read_row(&mut tx, &catalog, TABLE, &[cells::text_cell("bad")])
        .await
        .unwrap()
        .unwrap();
    assert_eq!(actual.row, original);
    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn read_only_sessions_cannot_claim_or_mutate_and_process_loss_keeps_all_worker_transitions() {
    let fixture = TestFixture::start().await;
    let store = fixture.initialize().await;
    let mut app = ApplicationTransactions::new(store.clone());
    app.initialize(schema()).await.unwrap();
    prepare(&store).await;
    app.begin_mode("read", ApplicationTransactionMode::Read, None)
        .await
        .unwrap();
    assert!(
        app.outbox_enqueue("read", &[pending("not-written", "thread")], NOW)
            .await
            .is_err()
    );
    assert!(
        app.rollback("read").await.is_err(),
        "the rejected root session was already aborted"
    );
    app.begin("write").await.unwrap();
    let process_bound = outbox::EnqueueEffect {
        request: json!({"type":"provider-turn.start","runId":"run"}),
        ..pending("bound", "bound-thread")
    };
    app.outbox_enqueue(
        "write",
        &[pending("safe", "safe-thread"), process_bound],
        NOW,
    )
    .await
    .unwrap();
    let first = app
        .outbox_claim_once("write", &claim("worker-stream", 1, NOW))
        .await
        .unwrap()
        .effect
        .unwrap();
    let second = app
        .outbox_claim_once("write", &claim("worker-stream", 2, NOW))
        .await
        .unwrap()
        .effect
        .unwrap();
    assert_ne!(first.id, second.id);
    assert!(
        !app.outbox_succeed(
            "write",
            &outbox::Settle {
                effect_id: "safe".into(),
                worker_id: "other".into(),
                now: NOW.into()
            }
        )
        .await
        .unwrap()
    );
    let counts = app
        .outbox_reconcile("write", &outbox::Reconcile { now: LATER.into() })
        .await
        .unwrap();
    assert_eq!(counts.requeued, 1);
    assert_eq!(counts.cancelled, 1);
    app.outbox_claim_once("write", &claim("worker-stream", 3, LATER))
        .await
        .unwrap();
    assert!(
        app.outbox_retry(
            "write",
            &outbox::Retry {
                effect_id: "safe".into(),
                worker_id: "worker".into(),
                now: LATER.into(),
                available_at: LATER.into(),
                error: "retry".into()
            }
        )
        .await
        .unwrap()
    );
    app.outbox_claim_once("write", &claim("worker-stream", 4, LATER))
        .await
        .unwrap();
    assert!(
        app.outbox_fail(
            "write",
            &outbox::Failure {
                effect_id: "safe".into(),
                worker_id: "worker".into(),
                now: LATER.into(),
                error: "retained failure".into()
            }
        )
        .await
        .unwrap()
    );
    assert_eq!(
        app.outbox_prune(
            "write",
            &outbox::Prune {
                before: "2026-12-10T12:00:00.000Z".into(),
                limit: 500
            }
        )
        .await
        .unwrap(),
        1
    );
    assert_eq!(
        app.outbox_get("write", "safe")
            .await
            .unwrap()
            .unwrap()
            .status,
        outbox::EffectStatus::Failed
    );
    app.commit("write").await.unwrap();
}

#[tokio::test]
async fn scoped_claims_follow_outer_and_savepoint_rollback_without_claim_stream_cursors() {
    let fixture = TestFixture::start().await;
    let store = fixture.initialize().await;
    let mut app = ApplicationTransactions::new(store.clone());
    app.initialize(schema()).await.unwrap();
    prepare(&store).await;
    app.begin("seed").await.unwrap();
    app.outbox_enqueue(
        "seed",
        &[pending("first", "first"), pending("second", "second")],
        NOW,
    )
    .await
    .unwrap();
    app.commit("seed").await.unwrap();
    let input = claim("durable", 1, NOW);
    app.begin("discarded").await.unwrap();
    assert_eq!(
        app.outbox_claim("discarded", &input.claim)
            .await
            .unwrap()
            .unwrap()
            .id,
        "first"
    );
    app.rollback("discarded").await.unwrap();
    app.begin("owner").await.unwrap();
    assert_eq!(
        app.outbox_get("owner", "first")
            .await
            .unwrap()
            .unwrap()
            .attempt_count,
        0
    );
    let first = app
        .outbox_claim("owner", &input.claim)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first.id, "first");
    assert_eq!(first.attempt_count, 1);
    app.savepoint("owner", "inner").await.unwrap();
    assert_eq!(
        app.outbox_claim("owner", &input.claim)
            .await
            .unwrap()
            .unwrap()
            .id,
        "second"
    );
    app.rollback_to("owner", "inner").await.unwrap();
    app.release("owner", "inner").await.unwrap();
    assert_eq!(
        app.outbox_get("owner", "second")
            .await
            .unwrap()
            .unwrap()
            .attempt_count,
        0
    );
    app.commit("owner").await.unwrap();
    app.begin("standalone").await.unwrap();
    let second = app
        .outbox_claim_once("standalone", &input)
        .await
        .unwrap()
        .effect
        .unwrap();
    assert_eq!(second.id, "second");
    assert_eq!(second.attempt_count, 1);
    app.commit("standalone").await.unwrap();
}
