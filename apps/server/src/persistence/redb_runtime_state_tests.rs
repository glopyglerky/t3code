use super::*;
use crate::application::{ApplicationSchema, ApplicationTable, ApplicationTransactionMode};
use crate::test_support::TestFixture;

fn schema() -> ApplicationSchema {
    ApplicationSchema {
        version: 1,
        tables: vec![
            ApplicationTable {
                name: RUNTIME.into(),
                columns: RUNTIME_COLUMNS.iter().map(|name| (*name).into()).collect(),
                primary_key: vec!["thread_id".into()],
            },
            ApplicationTable {
                name: CONTEXT.into(),
                columns: CONTEXT_COLUMNS.iter().map(|name| (*name).into()).collect(),
                primary_key: vec!["thread_id".into(), "item_id".into()],
            },
            ApplicationTable {
                name: ITEMS.into(),
                columns: vec!["turn_item_id".into(), "run_id".into()],
                primary_key: vec!["turn_item_id".into()],
            },
            ApplicationTable {
                name: RUNS.into(),
                columns: vec!["run_id".into(), "thread_id".into(), "status".into()],
                primary_key: vec!["run_id".into()],
            },
        ],
    }
}

fn runtime(thread: &str, at: &str, payload: Option<&str>) -> RuntimeRecord {
    RuntimeRecord {
        thread_id: thread.into(),
        provider_name: "codex".into(),
        provider_instance_id: Some("codex".into()),
        adapter_key: "codex".into(),
        runtime_mode: "full-access".into(),
        status: "running".into(),
        last_seen_at: at.into(),
        resume_cursor: Some("{\"cursor\":1}".into()),
        runtime_payload: payload.map(str::to_owned),
    }
}

async fn prepare(store: &Store) {
    while !store
        .prepare_application_runtime_state()
        .await
        .unwrap()
        .prepared
    {}
}

async fn get(app: &mut ApplicationTransactions, tx: &str, thread: &str) -> Value {
    app.runtime_state(
        tx,
        RuntimeStateOperation::RuntimeGet {
            thread_id: thread.into(),
        },
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn runtime_import_records_survive_stale_upserts_ignore_and_reopen() {
    let mut fixture = TestFixture::start().await;
    let store = fixture.initialize().await;
    let mut app = ApplicationTransactions::new(store.clone());
    app.initialize(schema()).await.unwrap();
    prepare(&store).await;
    app.begin("write").await.unwrap();
    app.runtime_state(
        "write",
        RuntimeStateOperation::RuntimeUpsert {
            runtime: runtime(
                "a",
                "2026-11-10",
                Some("{\"other\":1e2,\"importedTranscripts\":[\"stale\"]}"),
            ),
            ignore: false,
        },
    )
    .await
    .unwrap();
    assert_eq!(
        get(&mut app, "write", "a").await["runtimePayload"],
        "{\"other\":1e2}"
    );
    for source in [
        r#"{"providerInstanceId":"codex","filePath":"/a","size":1}"#,
        r#"{"providerInstanceId":"codex","filePath":"/b","size":2}"#,
        r#"{"providerInstanceId":"codex","filePath":"/a","size":3}"#,
    ] {
        app.runtime_state(
            "write",
            RuntimeStateOperation::RuntimeImport {
                thread_id: "a".into(),
                source: source.into(),
            },
        )
        .await
        .unwrap();
    }
    app.runtime_state(
        "write",
        RuntimeStateOperation::RuntimeUpsert {
            runtime: runtime("a", "2026-11-11", Some("null")),
            ignore: false,
        },
    )
    .await
    .unwrap();
    let expected = "{\"importedTranscripts\":[{\"providerInstanceId\":\"codex\",\"filePath\":\"/b\",\"size\":2},{\"providerInstanceId\":\"codex\",\"filePath\":\"/a\",\"size\":3}]}";
    assert_eq!(
        get(&mut app, "write", "a").await["runtimePayload"],
        expected
    );
    app.runtime_state(
        "write",
        RuntimeStateOperation::RuntimeUpsert {
            runtime: runtime("a", "wrong", None),
            ignore: true,
        },
    )
    .await
    .unwrap();
    assert_eq!(
        get(&mut app, "write", "a").await["lastSeenAt"],
        "2026-11-11"
    );
    app.commit("write").await.unwrap();
    drop(app);
    drop(store);
    fixture.restart().await;
    let store = fixture.connect().await;
    let mut app = ApplicationTransactions::new(store);
    app.begin_mode("read", ApplicationTransactionMode::Read, None)
        .await
        .unwrap();
    assert_eq!(get(&mut app, "read", "a").await["runtimePayload"], expected);
    app.rollback("read").await.unwrap();
}

#[tokio::test]
async fn runtime_order_and_status_indexes_follow_rollback_and_deletion() {
    let fixture = TestFixture::start().await;
    let store = fixture.initialize().await;
    let mut app = ApplicationTransactions::new(store.clone());
    app.initialize(schema()).await.unwrap();
    prepare(&store).await;
    app.begin("write").await.unwrap();
    for (id, at) in [
        ("z", "2026-11-10"),
        ("b", "2026-11-11"),
        ("a", "2026-11-10"),
    ] {
        app.runtime_state(
            "write",
            RuntimeStateOperation::RuntimeUpsert {
                runtime: runtime(id, at, None),
                ignore: false,
            },
        )
        .await
        .unwrap();
    }
    let listed = app
        .runtime_state(
            "write",
            RuntimeStateOperation::RuntimeList {
                exclude_stopped: false,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        listed
            .as_array()
            .unwrap()
            .iter()
            .map(|row| row["threadId"].as_str().unwrap())
            .collect::<Vec<_>>(),
        ["a", "z", "b"]
    );
    app.savepoint("write", "inner").await.unwrap();
    let mut stopped = runtime("a", "2026-11-12", None);
    stopped.status = "stopped".into();
    app.runtime_state(
        "write",
        RuntimeStateOperation::RuntimeUpsert {
            runtime: stopped,
            ignore: false,
        },
    )
    .await
    .unwrap();
    assert_eq!(
        app.runtime_state(
            "write",
            RuntimeStateOperation::RuntimeList {
                exclude_stopped: true
            }
        )
        .await
        .unwrap()
        .as_array()
        .unwrap()
        .len(),
        2
    );
    app.rollback_to("write", "inner").await.unwrap();
    assert_eq!(
        app.runtime_state(
            "write",
            RuntimeStateOperation::RuntimeList {
                exclude_stopped: true
            }
        )
        .await
        .unwrap(),
        listed
    );
    app.runtime_state(
        "write",
        RuntimeStateOperation::RuntimeDelete {
            thread_id: "z".into(),
        },
    )
    .await
    .unwrap();
    assert!(get(&mut app, "write", "z").await.is_null());
    app.rollback("write").await.unwrap();
}

#[tokio::test]
async fn context_filters_source_rollback_but_preserves_fork_copy_and_original_identity() {
    let fixture = TestFixture::start().await;
    let store = fixture.initialize().await;
    let mut app = ApplicationTransactions::new(store.clone());
    app.initialize(schema()).await.unwrap();
    prepare(&store).await;
    app.begin("write").await.unwrap();
    app.put(
        "write",
        ITEMS,
        vec![text_cell("item")],
        Map::from_iter([
            ("turn_item_id".into(), text_cell("item")),
            ("run_id".into(), text_cell("run")),
        ]),
    )
    .await
    .unwrap();
    app.put(
        "write",
        RUNS,
        vec![text_cell("run")],
        Map::from_iter([
            ("run_id".into(), text_cell("run")),
            ("thread_id".into(), text_cell("source")),
            ("status".into(), text_cell("rolled_back")),
        ]),
    )
    .await
    .unwrap();
    for thread in ["source", "fork"] {
        app.runtime_state(
            "write",
            RuntimeStateOperation::ContextUpsert {
                context: ContextRecord {
                    thread_id: thread.into(),
                    item_id: "item".into(),
                    server: "original".into(),
                    tool: "original-tool".into(),
                    text: "first".into(),
                    updated_at: "1".into(),
                },
            },
        )
        .await
        .unwrap();
    }
    app.runtime_state(
        "write",
        RuntimeStateOperation::ContextUpsert {
            context: ContextRecord {
                thread_id: "fork".into(),
                item_id: "item".into(),
                server: "ignored".into(),
                tool: "ignored".into(),
                text: "latest".into(),
                updated_at: "2".into(),
            },
        },
    )
    .await
    .unwrap();
    assert_eq!(
        app.runtime_state(
            "write",
            RuntimeStateOperation::ContextForThread {
                thread_id: "source".into()
            }
        )
        .await
        .unwrap(),
        json!([])
    );
    assert_eq!(
        app.runtime_state(
            "write",
            RuntimeStateOperation::ContextForThread {
                thread_id: "fork".into()
            }
        )
        .await
        .unwrap(),
        json!([
            {"itemId":"item","server":"original","tool":"original-tool","text":"latest"}
        ])
    );
    app.runtime_state(
        "write",
        RuntimeStateOperation::ContextDelete {
            thread_id: "fork".into(),
            item_id: "item".into(),
        },
    )
    .await
    .unwrap();
    assert_eq!(
        app.runtime_state(
            "write",
            RuntimeStateOperation::ContextForThread {
                thread_id: "fork".into()
            }
        )
        .await
        .unwrap(),
        json!([])
    );
    app.rollback("write").await.unwrap();
}
