use super::*;
use crate::application::{ApplicationSchema, ApplicationTable, ApplicationTransactionMode};
use crate::test_support::TestFixture;

fn schema() -> ApplicationSchema {
    ApplicationSchema {
        version: 1,
        tables: vec![
            ApplicationTable {
                name: POSITIONS.into(),
                columns: COLUMNS.iter().map(|value| (*value).into()).collect(),
                primary_key: vec!["thread_id".into(), "turn_item_id".into()],
            },
            ApplicationTable {
                name: RUNS.into(),
                columns: ["run_id", "thread_id", "ordinal"]
                    .map(str::to_owned)
                    .to_vec(),
                primary_key: vec!["run_id".into()],
            },
        ],
    }
}

fn input(item: &str, run: Option<&str>, ordinal: Option<i64>) -> PositionAllocate {
    PositionAllocate {
        thread_id: "thread".into(),
        turn_item_id: item.into(),
        run_id: run.map(str::to_owned),
        run_ordinal: ordinal,
    }
}

fn position(item: &str, ordinal: i64) -> Position {
    Position {
        thread_id: "thread".into(),
        turn_item_id: item.into(),
        ordinal,
    }
}

async fn initialize(store: &Store) -> ApplicationTransactions {
    let mut app = ApplicationTransactions::new(store.clone());
    app.initialize(schema()).await.unwrap();
    app
}

async fn prepare(store: &Store) -> PreparationResult {
    loop {
        let result = store.prepare_application_positions().await.unwrap();
        if result.prepared {
            return result;
        }
    }
}

async fn seed_position(app: &mut ApplicationTransactions, item: &str, ordinal: i64) {
    let row = position(item, ordinal);
    app.put("seed", POSITIONS, row.row_key().to_vec(), row.stored())
        .await
        .unwrap();
}

async fn seed_run(app: &mut ApplicationTransactions, id: &str, thread: &str, ordinal: i64) {
    app.put(
        "seed",
        RUNS,
        vec![text_cell(id)],
        Map::from_iter([
            ("run_id".into(), text_cell(id)),
            ("thread_id".into(), text_cell(thread)),
            ("ordinal".into(), int_cell(ordinal)),
        ]),
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn positions_reuse_identity_and_resolve_supplied_same_transaction_or_missing_run_ordinals() {
    let fixture = TestFixture::start().await;
    let store = fixture.initialize().await;
    let mut app = initialize(&store).await;
    prepare(&store).await;
    app.begin("seed").await.unwrap();
    seed_run(&mut app, "run:one", "thread", 1).await;
    seed_run(&mut app, "run:foreign", "other-thread", 20).await;
    assert_eq!(
        app.positions_allocate("seed", input("first", None, None))
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        app.positions_allocate("seed", input("second", None, None))
            .await
            .unwrap(),
        2
    );
    assert_eq!(
        app.positions_allocate("seed", input("first", Some("run:one"), Some(9)))
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        app.positions_allocate("seed", input("run-item", Some("run:one"), None))
            .await
            .unwrap(),
        1_000_001
    );
    assert_eq!(
        app.positions_allocate("seed", input("override", Some("run:one"), Some(2)))
            .await
            .unwrap(),
        2_000_001
    );
    assert_eq!(
        app.positions_allocate("seed", input("no-run-with-ordinal", None, Some(2)))
            .await
            .unwrap(),
        2_000_002
    );
    assert_eq!(
        app.positions_allocate("seed", input("foreign", Some("run:foreign"), None))
            .await
            .unwrap(),
        3
    );
    assert_eq!(
        app.positions_allocate("seed", input("missing", Some("run:missing"), None))
            .await
            .unwrap(),
        4
    );
    assert_eq!(
        app.positions_allocate(
            "seed",
            PositionAllocate {
                thread_id: "other-thread".into(),
                ..input("first", None, None)
            }
        )
        .await
        .unwrap(),
        1
    );
    app.commit("seed").await.unwrap();
}

#[tokio::test]
async fn position_savepoints_restore_both_rows_and_maximum_indexes() {
    let fixture = TestFixture::start().await;
    let store = fixture.initialize().await;
    let mut app = initialize(&store).await;
    prepare(&store).await;
    app.begin("write").await.unwrap();
    assert_eq!(
        app.positions_allocate("write", input("outer", None, None))
            .await
            .unwrap(),
        1
    );
    app.savepoint("write", "inner").await.unwrap();
    assert_eq!(
        app.positions_allocate("write", input("inner", None, None))
            .await
            .unwrap(),
        2
    );
    app.rollback_to("write", "inner").await.unwrap();
    app.release("write", "inner").await.unwrap();
    assert_eq!(
        app.positions_allocate("write", input("sibling", None, None))
            .await
            .unwrap(),
        2
    );
    app.commit("write").await.unwrap();
    app.begin("rollback").await.unwrap();
    assert_eq!(
        app.positions_allocate("rollback", input("discarded", None, None))
            .await
            .unwrap(),
        3
    );
    app.rollback("rollback").await.unwrap();
    app.begin("check").await.unwrap();
    assert_eq!(
        app.positions_allocate("check", input("next", None, None))
            .await
            .unwrap(),
        3
    );
    assert!(
        app.get(
            "check",
            POSITIONS,
            &[text_cell("thread"), text_cell("inner")]
        )
        .await
        .unwrap()
        .is_none()
    );
    assert!(
        app.get(
            "check",
            POSITIONS,
            &[text_cell("thread"), text_cell("discarded")]
        )
        .await
        .unwrap()
        .is_none()
    );
    app.rollback("check").await.unwrap();
}

#[tokio::test]
async fn full_ranges_preserve_sql_unique_constraint_and_nested_recovery() {
    let fixture = TestFixture::start().await;
    let store = fixture.initialize().await;
    let mut app = initialize(&store).await;
    app.begin("seed").await.unwrap();
    seed_position(&mut app, "last", 999_999).await;
    seed_position(&mut app, "taken", 1_000_000).await;
    seed_position(&mut app, "second-last", 2_999_999).await;
    app.commit("seed").await.unwrap();
    prepare(&store).await;
    app.begin("write").await.unwrap();
    app.savepoint("write", "full").await.unwrap();
    assert!(matches!(
        app.positions_allocate("write", input("conflict", None, None))
            .await,
        Err(Error::DuplicateRecord)
    ));
    assert!(app.release("write", "full").await.is_err());
    app.rollback_to("write", "full").await.unwrap();
    app.release("write", "full").await.unwrap();
    assert_eq!(
        app.positions_allocate("write", input("next-range", None, Some(1)))
            .await
            .unwrap(),
        1_000_001
    );
    assert_eq!(
        app.positions_allocate("write", input("overflow", None, Some(2)))
            .await
            .unwrap(),
        3_000_000
    );
    assert_eq!(
        app.positions_allocate("write", input("following", None, Some(3)))
            .await
            .unwrap(),
        3_000_001
    );
    app.commit("write").await.unwrap();
}

#[tokio::test]
async fn position_preparation_resumes_and_live_indexes_remain_authoritative() {
    let fixture = TestFixture::start().await;
    let store = fixture.initialize().await;
    let mut app = initialize(&store).await;
    app.begin("seed").await.unwrap();
    for ordinal in 1..=70 {
        seed_position(&mut app, &format!("item:{ordinal}"), ordinal).await;
    }
    app.commit("seed").await.unwrap();
    let first = store.prepare_application_positions().await.unwrap();
    assert!(!first.prepared);
    assert_eq!(first.indexed_rows, PREPARE_ROWS as u64);
    app.begin("incomplete").await.unwrap();
    assert!(matches!(
        app.positions_allocate("incomplete", input("blocked", None, None))
            .await,
        Err(Error::Invalid(_))
    ));
    app.begin("bypass").await.unwrap();
    assert!(matches!(
        app.delete(
            "bypass",
            POSITIONS,
            &[text_cell("thread"), text_cell("item:1")]
        )
        .await,
        Err(Error::Invalid(_))
    ));
    drop(app);
    store.shutdown().await.unwrap();
    let store = fixture.connect().await;
    assert_eq!(prepare(&store).await.indexed_rows, 70);
    let mut app = ApplicationTransactions::new(store.clone());
    app.begin("write").await.unwrap();
    assert_eq!(
        app.positions_allocate("write", input("live", None, None))
            .await
            .unwrap(),
        71
    );
    app.commit("write").await.unwrap();
    assert_eq!(prepare(&store).await.indexed_rows, 70);
    app.begin("check").await.unwrap();
    assert_eq!(
        app.positions_allocate("check", input("after-prepare", None, None))
            .await
            .unwrap(),
        72
    );
    app.rollback("check").await.unwrap();
}

#[tokio::test]
async fn preparation_rejects_duplicate_ordinals_and_noncanonical_cells_without_publication() {
    let fixture = TestFixture::start().await;
    let store = fixture.initialize().await;
    let mut app = initialize(&store).await;
    app.begin("seed").await.unwrap();
    seed_position(&mut app, "first", 1).await;
    seed_position(&mut app, "duplicate", 1).await;
    app.commit("seed").await.unwrap();
    assert!(matches!(
        store.prepare_application_positions().await,
        Err(Error::DuplicateRecord)
    ));
    app.begin("fix").await.unwrap();
    app.delete(
        "fix",
        POSITIONS,
        &[text_cell("thread"), text_cell("duplicate")],
    )
    .await
    .unwrap();
    let mut row = position("bad", 2).stored();
    row.insert("ordinal".into(), json!({"type":"integer","value":"02"}));
    app.put(
        "fix",
        POSITIONS,
        vec![text_cell("thread"), text_cell("bad")],
        row,
    )
    .await
    .unwrap();
    app.commit("fix").await.unwrap();
    assert!(matches!(
        store.prepare_application_positions().await,
        Err(Error::IncompatibleStore)
    ));
    app.begin("real").await.unwrap();
    let mut real = position("bad", 2).stored();
    real.insert(
        "ordinal".into(),
        json!({"type":"real","value":"4004000000000000"}),
    );
    app.put(
        "real",
        POSITIONS,
        vec![text_cell("thread"), text_cell("bad")],
        real,
    )
    .await
    .unwrap();
    app.commit("real").await.unwrap();
    assert!(matches!(
        store.prepare_application_positions().await,
        Err(Error::IncompatibleStore)
    ));
    app.begin("remove").await.unwrap();
    app.delete(
        "remove",
        POSITIONS,
        &[text_cell("thread"), text_cell("bad")],
    )
    .await
    .unwrap();
    app.commit("remove").await.unwrap();
    prepare(&store).await;
    app.begin_mode("read", ApplicationTransactionMode::Read, None)
        .await
        .unwrap();
    assert!(
        app.positions_allocate("read", input("cannot-write", None, None))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn supplied_run_ordinal_rejects_invalid_domain_values_after_existing_item_lookup() {
    let fixture = TestFixture::start().await;
    let store = fixture.initialize().await;
    let mut app = initialize(&store).await;
    prepare(&store).await;
    app.begin("outer").await.unwrap();
    assert_eq!(
        app.positions_allocate("outer", input("existing", None, None))
            .await
            .unwrap(),
        1
    );
    for (scope, invalid_ordinal) in [("negative", -1), ("unsafe", MAX_SAFE_INTEGER + 1)] {
        assert_eq!(
            app.positions_allocate("outer", input("existing", None, Some(invalid_ordinal)))
                .await
                .unwrap(),
            1
        );
        app.savepoint("outer", scope).await.unwrap();
        assert!(matches!(
            app.positions_allocate("outer", input(scope, None, Some(invalid_ordinal)))
                .await,
            Err(Error::Invalid(_))
        ));
        app.rollback_to("outer", scope).await.unwrap();
        app.release("outer", scope).await.unwrap();
    }
    assert_eq!(
        app.positions_allocate("outer", input("sibling", None, None))
            .await
            .unwrap(),
        2
    );
    app.commit("outer").await.unwrap();
}
