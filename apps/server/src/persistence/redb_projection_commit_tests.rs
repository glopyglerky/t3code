use super::*;
use crate::application::{ApplicationSchema, ApplicationTable};
use crate::test_support::TestFixture;

const NOW: &str = "2026-11-10T12:34:56.123Z";

#[tokio::test]
async fn historical_unreadable_provider_payload_can_be_audited_without_releasing_ownership() {
    let fixture = TestFixture::start().await;
    let store = fixture.initialize().await;
    let mut app = initialize(&store).await;
    let mut row = spec(PROVIDER_THREADS)
        .columns
        .iter()
        .map(|(name, kind)| {
            let value = match kind {
                T => text_cell("fixture"),
                I => int_cell(1),
                NT | NI => nullable_cell(None),
            };
            ((*name).to_owned(), value)
        })
        .collect::<Map<String, Value>>();
    for (name, value) in [
        ("provider_thread_id", "corrupt"),
        ("thread_id", "thread"),
        ("driver", "claude"),
        ("provider_instance_id", "instance"),
        ("payload_json", "{bad"),
    ] {
        row.insert(name.into(), text_cell(value));
    }
    app.begin("seed").await.unwrap();
    app.put("seed", PROVIDER_THREADS, vec![text_cell("corrupt")], row)
        .await
        .unwrap();
    app.commit("seed").await.unwrap();
    prepare(&store).await;
    prepare_reads(&store).await;

    app.begin("read").await.unwrap();
    let page = app
        .projections_recovery(
            "read",
            ProjectionRecoveryQuery::UnreadablePage {
                table: "provider_threads".into(),
                after: None,
                limit: 32,
            },
        )
        .await
        .unwrap();
    assert_eq!(page["rows"][0]["payload_json"], "{bad");
    assert!(
        !app.projections_guard(
            "read",
            ProjectionGuard::NativeThreadOwned {
                driver: "claude".into(),
                provider_instance_id: "instance".into(),
                native_id: Some("native".into()),
                app_thread_id: Some("thread".into()),
            }
        )
        .await
        .unwrap()
    );
    assert!(matches!(
        app.projections_guard(
            "read",
            ProjectionGuard::NativeThreadOwned {
                driver: "claude".into(),
                provider_instance_id: "instance".into(),
                native_id: Some("native".into()),
                app_thread_id: Some("other".into()),
            }
        )
        .await,
        Err(Error::IncompatibleStore)
    ));
    // Incompatible data aborts the transaction, not the persisted ownership
    // evidence. A caller must start a new snapshot after this terminal error.
    assert!(!app.has_active_writer());
    assert!(matches!(app.rollback("read").await, Err(Error::Invalid(_))));
    app.begin("verify").await.unwrap();
    let retained = app
        .projections_recovery(
            "verify",
            ProjectionRecoveryQuery::UnreadablePage {
                table: "provider_threads".into(),
                after: None,
                limit: 32,
            },
        )
        .await
        .unwrap();
    assert_eq!(retained, page);
    app.rollback("verify").await.unwrap();
}

#[tokio::test]
async fn selected_projection_records_preserve_filters_order_and_atomic_visibility() {
    use super::records::{
        ProjectionRecordField as F, ProjectionRecordFilter, ProjectionRecordsQuery,
    };
    let fixture = TestFixture::start().await;
    let store = fixture.initialize().await;
    let mut app = initialize(&store).await;
    prepare(&store).await;
    prepare_reads(&store).await;
    app.begin("write").await.unwrap();
    apply(
        &mut app,
        "write",
        event("thread.created", "thread", thread("thread")),
    )
    .await
    .unwrap();
    for (id, ordinal, status) in [
        ("old", 1, "completed"),
        ("current", 2, "running"),
        ("cancelled", 3, "cancelled"),
    ] {
        let mut value = run(id, ordinal);
        value["status"] = json!(status);
        apply(&mut app, "write", event("run.created", "thread", value))
            .await
            .unwrap();
    }
    for (id, ordinal, role) in [("late", 9, "assistant"), ("early", 3, "user")] {
        apply(
            &mut app,
            "write",
            event(
                "message.updated",
                "thread",
                json!({
                    "id":id,"threadId":"thread","runId":"current","nodeId":null,"role":role,
                    "streaming":false,"createdAt":NOW,"updatedAt":NOW,"text":id,
                }),
            ),
        )
        .await
        .unwrap();
        apply(&mut app, "write", event("turn-item.updated", "thread", json!({
            "id":format!("item-{id}"),"threadId":"thread","runId":"current","nodeId":null,
            "providerThreadId":null,"providerTurnId":null,"parentItemId":null,"ordinal":ordinal,
            "type":format!("{role}_message"),"status":"completed","updatedAt":NOW,"messageId":id,
        }))).await.unwrap();
    }
    let result = app
        .projection_records(
            "write",
            ProjectionRecordsQuery {
                thread_id: "thread".into(),
                fields: vec![F::Runs, F::Messages, F::TurnItems],
                window: None,
                filter: Some(ProjectionRecordFilter {
                    run_statuses: Some(vec!["completed".into()]),
                    include_latest_run: Some(true),
                    message_roles: Some(vec!["user".into()]),
                    ..Default::default()
                }),
            },
        )
        .await
        .unwrap();
    let ids = |values: &Vec<String>| {
        values
            .iter()
            .map(|value| {
                serde_json::from_str::<Value>(value).unwrap()["id"]
                    .as_str()
                    .unwrap()
                    .to_owned()
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(ids(&result.rows[&F::Runs]), ["old", "current"]);
    assert_eq!(ids(&result.rows[&F::Messages]), ["early"]);
    assert_eq!(
        ids(&result.rows[&F::TurnItems]),
        ["item-early", "item-late"]
    );
    assert!(result.rows[&F::Nodes].is_empty());
    assert!(result.message_ordinals.is_empty());
    let both = app
        .projection_records(
            "write",
            ProjectionRecordsQuery {
                thread_id: "thread".into(),
                fields: vec![F::Messages],
                window: None,
                filter: None,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        both.message_ordinals
            .iter()
            .map(|row| (row.message_id.as_str(), row.ordinal))
            .collect::<Vec<_>>(),
        [("early", 3), ("late", 9)]
    );
    app.rollback("write").await.unwrap();
    app.begin("read").await.unwrap();
    let gone = app
        .projection_records(
            "read",
            ProjectionRecordsQuery {
                thread_id: "thread".into(),
                fields: vec![F::Runs],
                window: None,
                filter: None,
            },
        )
        .await
        .unwrap();
    assert!(gone.thread.is_none());
    app.rollback("read").await.unwrap();
}

async fn prepare_reads(store: &Store) {
    while !store
        .prepare_application_projection_reads()
        .await
        .unwrap()
        .prepared
    {}
}

#[tokio::test]
async fn queued_and_explicit_run_reads_do_not_enumerate_completed_history() {
    use super::records::{
        ProjectionRecordField as F, ProjectionRecordFilter, ProjectionRecordsQuery,
    };
    let fixture = TestFixture::start().await;
    let store = fixture.initialize().await;
    let mut app = initialize(&store).await;
    prepare(&store).await;
    prepare_reads(&store).await;
    app.begin("write").await.unwrap();
    for id in ["thread", "other"] {
        apply(&mut app, "write", event("thread.created", id, thread(id)))
            .await
            .unwrap();
    }
    for ordinal in 0..1024 {
        let mut value = run(&format!("completed:{ordinal}"), ordinal);
        value["status"] = json!("completed");
        apply(&mut app, "write", event("run.created", "thread", value))
            .await
            .unwrap();
    }
    for (id, owner, ordinal) in [
        ("queued:later", "thread", 2049),
        ("queued:earlier", "thread", 2048),
        ("queued:other", "other", 2050),
    ] {
        let mut value = run(id, ordinal);
        value["threadId"] = json!(owner);
        value["status"] = json!("queued");
        apply(&mut app, "write", event("run.created", owner, value))
            .await
            .unwrap();
    }
    // Leave a tripwire in an unrelated completed run's compact metadata. A
    // history scan must encounter it; queued and explicit-key reads must not.
    // The final full-history query verifies that the tripwire is effective.
    let mut session = app.take("write").unwrap();
    let metadata = store_key(&format!(
        "application/generation/{}/projection-reads/v1/window-meta/{}/{}",
        session.catalog.generation,
        RUNS,
        request_digest(&json!([text_cell("completed:0")]))
    ));
    assert!(session.transaction.get(&metadata).await.unwrap().is_some());
    session.transaction.delete(metadata).await.unwrap();
    app.finish("write", session, Ok(())).await.unwrap();

    let query = |filter| ProjectionRecordsQuery {
        thread_id: "thread".into(),
        fields: vec![F::Runs],
        filter: Some(filter),
        window: None,
    };
    let ids = |result: &records::ProjectionRecordResult| {
        result.rows[&F::Runs]
            .iter()
            .map(|value| {
                serde_json::from_str::<Value>(value).unwrap()["id"]
                    .as_str()
                    .unwrap()
                    .to_owned()
            })
            .collect::<Vec<_>>()
    };
    let queued = app
        .projection_records("write", query(ProjectionRecordFilter {
            run_statuses: Some(vec!["queued".into(), "queued".into()]),
            ..Default::default()
        }))
        .await
        .unwrap();
    assert_eq!(ids(&queued), ["queued:earlier", "queued:later"]);
    let selected = app
        .projection_records("write", query(ProjectionRecordFilter {
            run_ids: Some(vec![
                "queued:later".into(), "queued:other".into(), "completed:1023".into(),
                "queued:earlier".into(), "queued:later".into(), "missing".into(),
            ]),
            ..Default::default()
        }))
        .await
        .unwrap();
    assert_eq!(ids(&selected), ["completed:1023", "queued:earlier", "queued:later"]);
    let combined = app
        .projection_records("write", query(ProjectionRecordFilter {
            run_ids: Some(vec!["completed:1023".into(), "queued:later".into(), "queued:other".into()]),
            run_statuses: Some(vec!["queued".into()]),
            ..Default::default()
        }))
        .await
        .unwrap();
    assert_eq!(ids(&combined), ["queued:later"]);
    for filter in [
        ProjectionRecordFilter { run_ids: Some(vec![]), ..Default::default() },
        ProjectionRecordFilter { run_statuses: Some(vec![]), ..Default::default() },
        ProjectionRecordFilter { run_ids: Some(vec!["queued:later".into()]), run_statuses: Some(vec![]), ..Default::default() },
    ] {
        assert!(ids(&app.projection_records("write", query(filter)).await.unwrap()).is_empty());
    }
    assert!(matches!(
        app.projection_records("write", query(ProjectionRecordFilter::default())).await,
        Err(Error::IncompatibleStore)
    ));
    assert!(!app.has_active_writer());
}

#[tokio::test]
async fn projection_lookups_preserve_owner_checks_requested_order_and_deleted_metadata() {
    let fixture = TestFixture::start().await;
    let store = fixture.initialize().await;
    let mut app = initialize(&store).await;
    prepare(&store).await;
    prepare_reads(&store).await;
    app.begin("write").await.unwrap();
    for id in ["thread", "other", "deleted"] {
        let mut payload = thread(id);
        payload["title"] = json!(format!("Title {id}"));
        if id == "deleted" {
            payload["deletedAt"] = json!(NOW);
        }
        apply(&mut app, "write", event("thread.created", id, payload))
            .await
            .unwrap();
    }
    for (id, owner) in [
        ("message-a", "thread"),
        ("message-b", "thread"),
        ("foreign", "other"),
    ] {
        apply(
            &mut app,
            "write",
            event(
                "message.updated",
                owner,
                json!({
                    "id": id, "threadId": owner, "runId": null, "nodeId": null, "role": "assistant",
                    "streaming": false, "createdAt": NOW, "updatedAt": NOW, "text": id,
                }),
            ),
        )
        .await
        .unwrap();
    }
    let messages = app
        .projections_lookup(
            "write",
            ProjectionLookup::Messages {
                thread_id: "thread".into(),
                record_ids: vec![
                    "message-b".into(),
                    "foreign".into(),
                    "message-a".into(),
                    "message-b".into(),
                    "missing".into(),
                ],
            },
        )
        .await
        .unwrap();
    let ids = messages["payloads"]
        .as_array()
        .unwrap()
        .iter()
        .map(|payload| {
            serde_json::from_str::<Value>(payload.as_str().unwrap()).unwrap()["id"]
                .as_str()
                .unwrap()
                .to_string()
        })
        .collect::<Vec<_>>();
    assert_eq!(ids, ["message-b", "message-a"]);
    let metadata = app
        .projections_lookup(
            "write",
            ProjectionLookup::ThreadMetadata {
                thread_ids: vec![
                    "deleted".into(),
                    "thread".into(),
                    "other".into(),
                    "thread".into(),
                    "missing".into(),
                ],
            },
        )
        .await
        .unwrap();
    assert_eq!(
        metadata["metadata"],
        json!([
            {"thread_id":"other","project_id":"project","title":"Title other"},
            {"thread_id":"thread","project_id":"project","title":"Title thread"},
        ])
    );
    let deleted = app
        .projections_lookup(
            "write",
            ProjectionLookup::Thread {
                thread_id: "deleted".into(),
            },
        )
        .await
        .unwrap();
    assert_eq!(deleted["payloads"].as_array().unwrap().len(), 1);
    let missing = app
        .projections_lookup(
            "write",
            ProjectionLookup::Plan {
                thread_id: "absent".into(),
                record_id: "missing".into(),
            },
        )
        .await
        .unwrap();
    assert_eq!(missing["owner_exists"], false);
    let existing = app
        .projections_lookup(
            "write",
            ProjectionLookup::Plan {
                thread_id: "thread".into(),
                record_id: "missing".into(),
            },
        )
        .await
        .unwrap();
    assert_eq!(existing["owner_exists"], true);
    assert_eq!(existing["payloads"], json!([]));
    app.commit("write").await.unwrap();
}

#[tokio::test]
async fn projection_read_indexes_backfill_and_follow_updates_savepoints_and_reopen() {
    let fixture = TestFixture::start().await;
    let store = fixture.initialize().await;
    let mut app = initialize(&store).await;
    prepare(&store).await;
    app.begin("write").await.unwrap();
    let mut payload = thread("thread");
    payload["worktreePath"] = json!("/first");
    apply(
        &mut app,
        "write",
        event("thread.created", "thread", payload.clone()),
    )
    .await
    .unwrap();
    app.commit("write").await.unwrap();
    prepare_reads(&store).await;
    app.begin("write").await.unwrap();
    let before = app
        .projections_lookup("write", ProjectionLookup::WorkspacePaths {})
        .await
        .unwrap();
    assert_eq!(
        before["workspaces"],
        json!([{"project_id":"project","worktree_path":"/first"}])
    );
    app.savepoint("write", "move").await.unwrap();
    payload["worktreePath"] = json!("/second");
    apply(
        &mut app,
        "write",
        event("thread.metadata-updated", "thread", payload.clone()),
    )
    .await
    .unwrap();
    assert_eq!(
        app.projections_lookup("write", ProjectionLookup::WorkspacePaths {})
            .await
            .unwrap()["workspaces"],
        json!([{"project_id":"project","worktree_path":"/second"}])
    );
    app.rollback_to("write", "move").await.unwrap();
    app.release("write", "move").await.unwrap();
    assert_eq!(
        app.projections_lookup("write", ProjectionLookup::WorkspacePaths {})
            .await
            .unwrap()["workspaces"],
        before["workspaces"]
    );
    payload["worktreePath"] = json!("/third");
    apply(
        &mut app,
        "write",
        event("thread.metadata-updated", "thread", payload.clone()),
    )
    .await
    .unwrap();
    app.commit("write").await.unwrap();
    let mut reopened = ApplicationTransactions::new(store.clone());
    reopened.begin("read").await.unwrap();
    assert_eq!(
        reopened
            .projections_lookup("read", ProjectionLookup::WorkspacePaths {})
            .await
            .unwrap()["workspaces"],
        json!([{"project_id":"project","worktree_path":"/third"}])
    );
    payload["deletedAt"] = json!(NOW);
    apply(
        &mut reopened,
        "read",
        event("thread.deleted", "thread", payload),
    )
    .await
    .unwrap();
    assert_eq!(
        reopened
            .projections_lookup("read", ProjectionLookup::WorkspacePaths {})
            .await
            .unwrap()["workspaces"],
        json!([])
    );
    reopened.rollback("read").await.unwrap();
}

fn schema() -> ApplicationSchema {
    let mut tables: Vec<_> = SPECS.iter().map(|spec| ApplicationTable {
        name: spec.name.into(),
        columns: spec.columns.iter().map(|(name, _)| (*name).into()).collect(),
        primary_key: spec.primary.iter().map(|name| (*name).into()).collect(),
    }).collect();
    tables.push(ApplicationTable {
        name: crate::application_turn_item_counts::TABLE.into(),
        columns: vec!["thread_id".into(), "run_key".into(), "item_count".into()],
        primary_key: vec!["thread_id".into(), "run_key".into()],
    });
    ApplicationSchema { version: 1, tables }
}

async fn initialize(store: &Store) -> ApplicationTransactions {
    let mut app = ApplicationTransactions::new(store.clone());
    app.initialize(schema()).await.unwrap();
    app
}

async fn prepare(store: &Store) -> ProjectionPreparation {
    loop {
        let result = store.prepare_application_projections().await.unwrap();
        if result.prepared {
            return result;
        }
    }
}

fn event(kind: &str, thread: &str, payload: Value) -> ProjectionApply {
    ProjectionApply {
        event_type: kind.into(),
        thread_id: thread.into(),
        occurred_at: NOW.into(),
        payload_json: payload.to_string(),
        normalization: None,
    }
}

// These native fixtures supply their already encoded old payload unchanged.
// The adapter differential exercises the real application Schema round trips.
async fn apply(
    app: &mut ApplicationTransactions,
    id: &str,
    mut event: ProjectionApply,
) -> Result<()> {
    if let Some(context) = app.projections_normalization(id, event.clone()).await? {
        event.normalization = Some(ProjectionNormalizedPayload {
            expected_payload_json: context.payload_json.clone(),
            payload_json: context.payload_json,
        });
    }
    app.projections_apply(id, event).await
}

fn thread(id: &str) -> Value {
    json!({"id":id,"projectId":"project","title":"Thread","providerInstanceId":"instance",
        "runtimeMode":"full-access","interactionMode":"default","activeProviderThreadId":null,
        "createdAt":"2026-01-01T00:00:00.000Z","updatedAt":"2026-01-02T00:00:00.000Z",
        "archivedAt":null,"deletedAt":null,"lineage":{"relationshipToParent":"root"}})
}

fn run(id: &str, ordinal: i64) -> Value {
    json!({"id":id,"threadId":"thread","ordinal":ordinal,"providerInstanceId":"instance",
        "providerThreadId":"provider","status":"running","activeAttemptId":"attempt",
        "requestedAt":NOW,"completedAt":null})
}

fn provider(id: &str, thread: Option<&str>, native: Option<&str>) -> Value {
    json!({"id":id,"appThreadId":thread,"ownerNodeId":null,"providerInstanceId":"instance",
        "driver":"claude","providerSessionId":null,"status":"loaded","firstRunOrdinal":1,
        "lastRunOrdinal":1,"updatedAt":NOW,
        "nativeThreadRef":native.map(|value|json!({"nativeId":value}))})
}

async fn get(app: &mut ApplicationTransactions, table: &str, id: &str) -> Map<String, Value> {
    app.get("write", table, &[text_cell(id)])
        .await
        .unwrap()
        .unwrap()
}

#[tokio::test]
async fn all_projection_entity_families_and_shadow_columns_are_persisted() {
    let fixture = TestFixture::start().await;
    let store = fixture.initialize().await;
    let mut app = initialize(&store).await;
    prepare(&store).await;
    app.begin("write").await.unwrap();
    apply(
        &mut app,
        "write",
        event("thread.created", "thread", thread("thread")),
    )
    .await
    .unwrap();
    apply(
        &mut app,
        "write",
        event("run.created", "thread", run("run", 1)),
    )
    .await
    .unwrap();
    let cases = [
        (
            "run-attempt.created",
            ATTEMPTS,
            "attempt",
            json!({"id":"attempt","runId":"run","attemptOrdinal":1,
            "rootNodeId":"node","providerInstanceId":"instance","providerThreadId":"provider","providerTurnId":null,"status":"running"}),
        ),
        (
            "node.updated",
            NODES,
            "node",
            json!({"id":"node","threadId":"thread","runId":"run","parentNodeId":null,
            "rootNodeId":"node","kind":"root","status":"running","providerThreadId":"provider","providerTurnId":null,
            "startedAt":NOW,"completedAt":null,"runtimeRequestId":"not-a-shadow-write","checkpointScopeId":"not-a-shadow-write"}),
        ),
        (
            "subagent.updated",
            SUBAGENTS,
            "subagent",
            json!({"id":"subagent","threadId":"thread","runId":"run",
            "parentNodeId":"node","providerInstanceId":"instance","driver":"claude","providerThreadId":null,
            "childThreadId":null,"origin":"provider","status":"running","startedAt":NOW,"completedAt":null,"updatedAt":NOW}),
        ),
        (
            "provider-session.attached",
            SESSIONS,
            "session",
            json!({"id":"session","providerInstanceId":"instance",
            "driver":"claude","status":"ready","model":"not-a-shadow-write","updatedAt":NOW}),
        ),
        (
            "provider-thread.updated",
            PROVIDER_THREADS,
            "provider",
            provider("provider", Some("thread"), Some("native")),
        ),
        (
            "provider-turn.updated",
            TURNS,
            "turn",
            json!({"id":"turn","providerThreadId":"provider","nodeId":"node",
            "runAttemptId":"attempt","ordinal":1,"status":"running","startedAt":NOW,"completedAt":null}),
        ),
        (
            "runtime-request.updated",
            REQUESTS,
            "request",
            json!({"id":"request","nodeId":"node","providerTurnId":"turn",
            "kind":"user_input","status":"pending","createdAt":NOW,"resolvedAt":null}),
        ),
        (
            "message.updated",
            MESSAGES,
            "message",
            json!({"id":"message","threadId":"thread","runId":"run","nodeId":"node",
            "role":"assistant","streaming":false,"createdAt":NOW,"updatedAt":NOW,"text":"answer"}),
        ),
        (
            "plan.updated",
            PLANS,
            "plan",
            json!({"id":"plan","threadId":"thread","runId":"run","nodeId":"node",
            "kind":"plan","status":"completed"}),
        ),
        (
            "turn-item.updated",
            ITEMS,
            "item",
            json!({"id":"item","threadId":"thread","runId":"run","nodeId":"node",
            "providerThreadId":"provider","providerTurnId":"turn","parentItemId":null,"ordinal":1000001,
            "type":"assistant_message","status":"completed","updatedAt":NOW}),
        ),
        (
            "checkpoint-scope.created",
            SCOPES,
            "scope",
            json!({"id":"scope","threadId":"thread","runId":"run","nodeId":"node",
            "parentScopeId":"not-a-shadow-write","providerThreadId":"provider","kind":"run","ordinalWithinParent":1,
            "advancesAppRunCount":true,"createdAt":NOW}),
        ),
        (
            "checkpoint.captured",
            CHECKPOINTS,
            "checkpoint",
            json!({"id":"checkpoint","threadId":"thread","scopeId":"scope",
            "runId":"run","nodeId":"node","parentCheckpointId":"not-a-shadow-write","ordinalWithinScope":1,
            "appRunOrdinal":1,"status":"ready","capturedAt":NOW}),
        ),
        (
            "context-handoff.updated",
            HANDOFFS,
            "handoff",
            json!({"id":"handoff","threadId":"thread","targetRunId":"run",
            "toProviderThreadId":"provider","strategy":"manual_context","status":"ready","updatedAt":NOW}),
        ),
        (
            "context-transfer.created",
            TRANSFERS,
            "transfer",
            json!({"id":"transfer","sourceThreadId":"thread","targetThreadId":"target",
            "targetRunId":"run","type":"fork","status":"ready","sourceProviderInstanceId":"not-a-shadow-write","updatedAt":NOW}),
        ),
    ];
    for (kind, table, id, payload) in cases {
        apply(&mut app, "write", event(kind, "thread", payload.clone()))
            .await
            .unwrap();
        let row = get(&mut app, table, id).await;
        assert_eq!(
            serde_json::from_str::<Value>(&text(&row, "payload_json").unwrap()).unwrap(),
            payload,
            "{kind}"
        );
    }
    for (table, id, column) in [
        (NODES, "node", "runtime_request_id"),
        (NODES, "node", "checkpoint_scope_id"),
        (SESSIONS, "session", "model"),
        (SCOPES, "scope", "parent_scope_id"),
        (CHECKPOINTS, "checkpoint", "parent_checkpoint_id"),
        (TRANSFERS, "transfer", "source_provider_instance_id"),
    ] {
        assert_eq!(get(&mut app, table, id).await[column], nullable_cell(None));
    }
    assert!(
        app.get(
            "write",
            BINDINGS,
            &[text_cell("session"), text_cell("thread")]
        )
        .await
        .unwrap()
        .is_some()
    );
    apply(
        &mut app,
        "write",
        event(
            "provider-session.detached",
            "thread",
            json!({"providerSessionId":"session"}),
        ),
    )
    .await
    .unwrap();
    assert!(
        app.get(
            "write",
            BINDINGS,
            &[text_cell("session"), text_cell("thread")]
        )
        .await
        .unwrap()
        .is_none()
    );
    assert!(
        app.get("write", SESSIONS, &[text_cell("session")])
            .await
            .unwrap()
            .is_some()
    );
    apply(
        &mut app,
        "write",
        event("checkpoint.rollback-requested", "thread", json!({})),
    )
    .await
    .unwrap();
    assert_eq!(
        text(&get(&mut app, THREADS, "thread").await, "updated_at").unwrap(),
        NOW
    );
    app.projections_mark_applied("write", 17, NOW.into())
        .await
        .unwrap();
    app.commit("write").await.unwrap();
    app.begin("verify").await.unwrap();
    assert_eq!(
        integer(
            &app.get("verify", METADATA, &[text_cell("thread-projections")])
                .await
                .unwrap()
                .unwrap(),
            "last_sequence"
        )
        .unwrap(),
        17
    );
    app.rollback("verify").await.unwrap();
}

#[tokio::test]
async fn recorded_fields_and_background_cancellation_survive_lifecycle_updates() {
    let fixture = TestFixture::start().await;
    let store = fixture.initialize().await;
    let mut app = initialize(&store).await;
    prepare(&store).await;
    app.begin("write").await.unwrap();
    let mut first = run("run", 1);
    first["delegatedCompletion"] = json!({"state":"pending","attempt":100,"path":"a"});
    first["restartCancelledBackgroundWork"] = json!([{"id":"work"}]);
    let mut first = event("run.created", "thread", first);
    first.payload_json = first
        .payload_json
        .replace("\"attempt\":100", "\"attempt\":1e2")
        .replace("\"path\":\"a\"", "\"path\":\"\\u0061\"");
    apply(&mut app, "write", first).await.unwrap();
    let mut update = run("run", 1);
    update["status"] = json!("completed");
    apply(
        &mut app,
        "write",
        event("run.updated", "thread", update.clone()),
    )
    .await
    .unwrap();
    let payload = text(&get(&mut app, RUNS, "run").await, "payload_json").unwrap();
    assert!(payload.contains("\"attempt\":1e2"));
    assert!(payload.contains("\"path\":\"\\u0061\""));
    assert_eq!(
        serde_json::from_str::<Value>(&payload).unwrap()["restartCancelledBackgroundWork"],
        json!([{"id":"work"}])
    );
    apply(
        &mut app,
        "write",
        event(
            "run.background-work-cancelled",
            "thread",
            json!({"runId":"run","restartCancelledBackgroundWork":[]}),
        ),
    )
    .await
    .unwrap();
    let row = get(&mut app, RUNS, "run").await;
    assert_eq!(text(&row, "status").unwrap(), "completed");
    assert_eq!(
        serde_json::from_str::<Value>(&text(&row, "payload_json").unwrap()).unwrap()["restartCancelledBackgroundWork"],
        json!([])
    );
    update["delegatedCompletion"] = Value::Null;
    apply(&mut app, "write", event("run.updated", "thread", update))
        .await
        .unwrap();
    assert!(
        serde_json::from_str::<Value>(
            &text(&get(&mut app, RUNS, "run").await, "payload_json").unwrap()
        )
        .unwrap()["delegatedCompletion"]
            .is_null()
    );
    app.rollback("write").await.unwrap();
}

#[tokio::test]
async fn unique_rejection_rolls_back_inner_scope_and_released_indexes_remain_atomic() {
    let fixture = TestFixture::start().await;
    let store = fixture.initialize().await;
    let mut app = initialize(&store).await;
    prepare(&store).await;
    app.begin("write").await.unwrap();
    apply(
        &mut app,
        "write",
        event("run.created", "thread", run("one", 1)),
    )
    .await
    .unwrap();
    app.savepoint("write", "inner").await.unwrap();
    assert!(matches!(
        apply(
            &mut app,
            "write",
            event("run.created", "thread", run("two", 1))
        )
        .await,
        Err(Error::DuplicateRecord)
    ));
    assert!(
        app.projections_read(
            "write",
            ProjectionRead {
                kind: "run".into(),
                thread_id: "thread".into(),
                record_id: Some("one".into())
            }
        )
        .await
        .is_err()
    );
    app.rollback_to("write", "inner").await.unwrap();
    app.release("write", "inner").await.unwrap();
    apply(
        &mut app,
        "write",
        event("run.updated", "thread", run("one", 2)),
    )
    .await
    .unwrap();
    apply(
        &mut app,
        "write",
        event("run.created", "thread", run("two", 1)),
    )
    .await
    .unwrap();
    app.projections_mark_applied("write", 9, NOW.into())
        .await
        .unwrap();
    app.rollback("write").await.unwrap();
    app.begin("again").await.unwrap();
    assert!(
        app.get("again", RUNS, &[text_cell("one")])
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        app.get("again", METADATA, &[text_cell("thread-projections")])
            .await
            .unwrap()
            .is_none()
    );
    apply(
        &mut app,
        "again",
        event("run.created", "thread", run("three", 1)),
    )
    .await
    .unwrap();
    app.commit("again").await.unwrap();
}

#[tokio::test]
async fn ownership_guards_use_current_rows_and_exact_payload_bytes() {
    let fixture = TestFixture::start().await;
    let store = fixture.initialize().await;
    let mut app = initialize(&store).await;
    prepare(&store).await;
    app.begin("write").await.unwrap();
    apply(
        &mut app,
        "write",
        event("thread.created", "thread", thread("thread")),
    )
    .await
    .unwrap();
    apply(
        &mut app,
        "write",
        event("run.created", "thread", run("run", 1)),
    )
    .await
    .unwrap();
    apply(
        &mut app,
        "write",
        event(
            "provider-thread.updated",
            "thread",
            provider("provider", None, Some("native")),
        ),
    )
    .await
    .unwrap();
    let guard = |except: Option<&str>| ProjectionGuard::NativeThreadOwned {
        driver: "claude".into(),
        provider_instance_id: "instance".into(),
        native_id: Some("native".into()),
        app_thread_id: except.map(str::to_owned),
    };
    assert!(!app.projections_guard("write", guard(None)).await.unwrap());
    assert!(
        app.projections_guard("write", guard(Some("thread")))
            .await
            .unwrap()
    );
    let mut unidentified = self::provider("unidentified", Some("thread"), None);
    unidentified["nativeThreadRef"] = json!({"nativeId":null,"strength":"weak"});
    apply(
        &mut app,
        "write",
        event("provider-thread.updated", "thread", unidentified),
    )
    .await
    .unwrap();
    assert!(
        !app.projections_guard(
            "write",
            ProjectionGuard::NativeThreadOwned {
                driver: "claude".into(),
                provider_instance_id: "instance".into(),
                native_id: None,
                app_thread_id: Some("other".into())
            }
        )
        .await
        .unwrap()
    );
    let provider = provider("provider", Some("thread"), Some("native"));
    let expected_json = provider.to_string();
    apply(
        &mut app,
        "write",
        event("provider-thread.updated", "thread", provider),
    )
    .await
    .unwrap();
    assert!(
        !app.projections_guard("write", guard(Some("thread")))
            .await
            .unwrap()
    );
    assert!(
        app.projections_guard(
            "write",
            ProjectionGuard::RunCurrent {
                thread_id: "thread".into(),
                run_id: "run".into(),
                active_attempt_id: "attempt".into(),
                status: "running".into()
            }
        )
        .await
        .unwrap()
    );
    assert!(
        app.projections_guard(
            "write",
            ProjectionGuard::ProviderThreadOwner {
                provider_thread_id: "provider".into(),
                run_id: "run".into(),
                active_attempt_id: "attempt".into(),
                expected_last_run_ordinal: 1
            }
        )
        .await
        .unwrap()
    );
    assert!(
        app.projections_guard(
            "write",
            ProjectionGuard::ProviderThreadUnchanged {
                provider_thread_id: "provider".into(),
                expected_json: expected_json.clone()
            }
        )
        .await
        .unwrap()
    );
    assert!(
        !app.projections_guard(
            "write",
            ProjectionGuard::ProviderThreadUnchanged {
                provider_thread_id: "provider".into(),
                expected_json: format!(" {expected_json}")
            }
        )
        .await
        .unwrap()
    );
    for index in 0..40 {
        apply(
            &mut app,
            "write",
            event(
                "provider-thread.updated",
                "thread",
                self::provider(&format!("alias-{index}"), Some("thread"), Some("native")),
            ),
        )
        .await
        .unwrap();
    }
    assert!(
        !app.projections_guard("write", guard(Some("thread")))
            .await
            .unwrap()
    );
    apply(
        &mut app,
        "write",
        event(
            "provider-thread.updated",
            "other",
            self::provider("other-provider", Some("other"), Some("native")),
        ),
    )
    .await
    .unwrap();
    assert!(
        app.projections_guard("write", guard(Some("thread")))
            .await
            .unwrap()
    );
    apply(
        &mut app,
        "write",
        event(
            "provider-thread.updated",
            "other",
            self::provider("other-provider", Some("other"), Some("different")),
        ),
    )
    .await
    .unwrap();
    assert!(
        !app.projections_guard("write", guard(Some("thread")))
            .await
            .unwrap()
    );
    app.savepoint("write", "missing-thread").await.unwrap();
    assert!(matches!(
        app.projections_read(
            "write",
            ProjectionRead {
                kind: "runtime_request".into(),
                thread_id: "missing".into(),
                record_id: Some("request".into()),
            }
        )
        .await,
        Err(Error::Invalid(_))
    ));
    app.rollback_to("write", "missing-thread").await.unwrap();
    app.release("write", "missing-thread").await.unwrap();
    app.rollback("write").await.unwrap();
}

#[tokio::test]
async fn imported_uniqueness_is_validated_before_ready_and_generic_mutation_is_blocked() {
    let fixture = TestFixture::start().await;
    let store = fixture.initialize().await;
    let mut app = initialize(&store).await;
    app.begin("seed").await.unwrap();
    for id in ["one", "two"] {
        let mut row: Map<String, Value> = spec(RUNS)
            .columns
            .iter()
            .map(|(name, _)| ((*name).into(), nullable_cell(None)))
            .collect();
        for (name, value) in [
            ("run_id", id),
            ("thread_id", "thread"),
            ("provider", "instance"),
            ("status", "running"),
            ("requested_at", NOW),
            ("payload_json", "{}"),
        ] {
            row.insert(name.into(), text_cell(value));
        }
        row.insert("ordinal".into(), int_cell(1));
        app.put("seed", RUNS, vec![text_cell(id)], row)
            .await
            .unwrap();
    }
    app.commit("seed").await.unwrap();
    assert!(
        !store
            .prepare_application_projections()
            .await
            .unwrap()
            .prepared
    );
    assert!(matches!(
        store.prepare_application_projections().await,
        Err(Error::DuplicateRecord)
    ));
    app.begin("unready").await.unwrap();
    assert!(matches!(
        app.projections_read(
            "unready",
            ProjectionRead {
                kind: "thread".into(),
                thread_id: "thread".into(),
                record_id: None
            }
        )
        .await,
        Err(Error::Invalid(_))
    ));
    app.begin("bypass").await.unwrap();
    assert!(matches!(
        app.delete("bypass", RUNS, &[text_cell("one")]).await,
        Err(Error::Invalid(_))
    ));
}

#[tokio::test]
async fn schema_mismatch_is_rejected_before_any_projection_ownership_is_claimed() {
    let fixture = TestFixture::start().await;
    let store = fixture.initialize().await;
    let mut app = ApplicationTransactions::new(store.clone());
    let mut changed = schema();
    changed.tables[0].columns.push("future_column".into());
    app.initialize(changed).await.unwrap();
    assert!(matches!(
        store.prepare_application_projections().await,
        Err(Error::IncompatibleStore)
    ));
    let catalog = decode_catalog(&store.db.read(&schema_key()).await.unwrap().unwrap()).unwrap();
    assert!(
        store
            .db
            .read(&managed_marker(&catalog, RUNS))
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn normalization_is_required_witnessed_and_rolls_back_with_its_projection() {
    let fixture = TestFixture::start().await;
    let store = fixture.initialize().await;
    let mut app = initialize(&store).await;
    prepare(&store).await;
    app.begin("write").await.unwrap();
    apply(
        &mut app,
        "write",
        event("thread.created", "thread", thread("thread")),
    )
    .await
    .unwrap();
    let mut update = event(
        "provider-thread.updated",
        "thread",
        provider("provider", Some("thread"), Some("native")),
    );
    let context = app
        .projections_normalization("write", update.clone())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(context.kind, "thread");
    let mut normalized: Value = serde_json::from_str(&context.payload_json).unwrap();
    normalized["settledAt"] = Value::Null;
    normalized["lastVisitedAt"] = Value::Null;
    update.normalization = Some(ProjectionNormalizedPayload {
        expected_payload_json: context.payload_json,
        payload_json: normalized.to_string(),
    });
    app.projections_apply("write", update.clone())
        .await
        .unwrap();
    let row = get(&mut app, THREADS, "thread").await;
    let value: Value = serde_json::from_str(&text(&row, "payload_json").unwrap()).unwrap();
    assert_eq!(value["activeProviderThreadId"], "provider");
    assert_eq!(value["lastVisitedAt"], Value::Null);
    assert!(value.as_object().unwrap().contains_key("settledAt"));
    assert_eq!(text(&row, "title").unwrap(), "Thread");

    app.savepoint("write", "stale").await.unwrap();
    assert!(matches!(
        app.projections_apply("write", update).await,
        Err(Error::Invalid(_))
    ));
    app.rollback_to("write", "stale").await.unwrap();
    app.release("write", "stale").await.unwrap();
    assert_eq!(get(&mut app, THREADS, "thread").await, row);
    app.savepoint("write", "missing").await.unwrap();
    assert!(matches!(
        app.projections_apply(
            "write",
            event(
                "provider-thread.updated",
                "thread",
                provider("second", Some("thread"), Some("native"))
            )
        )
        .await,
        Err(Error::Invalid(_))
    ));
    app.rollback_to("write", "missing").await.unwrap();
    app.release("write", "missing").await.unwrap();
    assert!(
        app.get("write", PROVIDER_THREADS, &[text_cell("second")])
            .await
            .unwrap()
            .is_none()
    );
    app.rollback("write").await.unwrap();
}

#[tokio::test]
async fn turn_item_and_interrupt_guards_share_reserved_writer_and_exact_current_payload() {
    use crate::application::ApplicationTransactionMode;
    let fixture = TestFixture::start().await;
    let store = fixture.initialize().await;
    let mut owner = initialize(&store).await;
    let mut competing = ApplicationTransactions::new(store.clone());
    prepare(&store).await;
    let item = |id: &str, ordinal: i64, text: &str| json!({
        "id":id,"threadId":"thread","runId":null,"nodeId":null,"providerThreadId":null,
        "providerTurnId":null,"parentItemId":null,"ordinal":ordinal,"type":"reasoning",
        "status":"running","updatedAt":NOW,"text":text,
    });
    let original = item("reasoning", 1, "before");
    let unchanged = |payload: &Value| ProjectionGuard::TurnItemUnchanged {
        turn_item_id: "reasoning".into(), expected_json: payload.to_string(),
    };
    let pending = |thread: &str| ProjectionGuard::InterruptPending {
        thread_id: thread.into(), request_id: "interrupt-request".into(), result_id: "interrupt-result".into(),
    };
    owner.begin("first").await.unwrap();
    apply(&mut owner, "first", event("thread.created", "thread", thread("thread"))).await.unwrap();
    assert!(!owner.projections_guard("first", unchanged(&original)).await.unwrap());
    apply(&mut owner, "first", event("turn-item.updated", "thread", original.clone())).await.unwrap();
    assert!(owner.projections_guard("first", unchanged(&original)).await.unwrap());
    assert!(!owner.projections_guard("first", ProjectionGuard::TurnItemUnchanged {
        turn_item_id: "reasoning".into(), expected_json: format!(" {}", original),
    }).await.unwrap());
    assert!(!owner.projections_guard("first", pending("thread")).await.unwrap());
    apply(&mut owner, "first", event("turn-item.updated", "thread", item("interrupt-request", 2, "stop"))).await.unwrap();
    assert!(owner.projections_guard("first", pending("thread")).await.unwrap());
    assert!(!owner.projections_guard("first", pending("other-thread")).await.unwrap());
    // The competitor cannot change either protected row between a guard and its writes.
    assert!(matches!(competing.begin_mode("competing", ApplicationTransactionMode::Write, Some(1)).await, Err(Error::WriterBusy)));
    let changed = item("reasoning", 1, "after-without-timestamp-change");
    apply(&mut owner, "first", event("turn-item.updated", "thread", changed.clone())).await.unwrap();
    owner.savepoint("first", "result").await.unwrap();
    apply(&mut owner, "first", event("turn-item.updated", "thread", item("interrupt-result", 3, "done"))).await.unwrap();
    assert!(!owner.projections_guard("first", pending("thread")).await.unwrap());
    owner.rollback_to("first", "result").await.unwrap();
    owner.release("first", "result").await.unwrap();
    assert!(owner.projections_guard("first", pending("thread")).await.unwrap());
    owner.commit("first").await.unwrap();
    competing.begin("second").await.unwrap();
    assert!(!competing.projections_guard("second", unchanged(&original)).await.unwrap());
    assert!(competing.projections_guard("second", unchanged(&changed)).await.unwrap());
    assert!(competing.projections_guard("second", pending("thread")).await.unwrap());
    apply(&mut competing, "second", event("turn-item.updated", "thread", item("interrupt-result", 3, "done"))).await.unwrap();
    competing.commit("second").await.unwrap();
    owner.begin("final").await.unwrap();
    assert!(!owner.projections_guard("final", pending("thread")).await.unwrap());
    owner.rollback("final").await.unwrap();
}

#[tokio::test]
async fn turn_item_counts_follow_cross_run_moves_deletion_and_nested_rollback() {
    let fixture = TestFixture::start().await;
    let store = fixture.initialize().await;
    let mut app = initialize(&store).await;
    prepare(&store).await;
    prepare_reads(&store).await;
    let counts = crate::application_turn_item_counts::TABLE;
    let item = |thread: &str, run: Option<&str>| json!({
        "id":"counted-item","threadId":thread,"runId":run,"nodeId":null,
        "providerThreadId":null,"providerTurnId":null,"parentItemId":null,"ordinal":1,
        "type":"assistant_message","status":"completed","updatedAt":NOW,
    });
    app.begin("write").await.unwrap();
    apply(&mut app, "write", event("turn-item.updated", "first", item("first", None))).await.unwrap();
    let count = app.get("write", counts, &[text_cell("first"), text_cell("")]).await.unwrap().unwrap();
    assert_eq!(integer(&count, "item_count").unwrap(), 1);
    app.savepoint("write", "move").await.unwrap();
    apply(&mut app, "write", event("turn-item.updated", "second", item("second", Some("run")))).await.unwrap();
    assert!(app.get("write", counts, &[text_cell("first"), text_cell("")]).await.unwrap().is_none());
    assert_eq!(integer(&app.get("write", counts, &[text_cell("second"), text_cell("run")]).await.unwrap().unwrap(), "item_count").unwrap(), 1);
    app.rollback_to("write", "move").await.unwrap();
    assert!(app.get("write", counts, &[text_cell("second"), text_cell("run")]).await.unwrap().is_none());
    assert_eq!(integer(&app.get("write", counts, &[text_cell("first"), text_cell("")]).await.unwrap().unwrap(), "item_count").unwrap(), 1);
    // The projection owner's deletion path must remove an empty group in the
    // same transaction, including data removed by retention/normalization.
    let mut session = app.take("write").unwrap();
    delete_row(&mut session.transaction, &session.catalog, ITEMS, &[text_cell("counted-item")]).await.unwrap();
    app.finish("write", session, Ok(())).await.unwrap();
    assert!(app.get("write", counts, &[text_cell("first"), text_cell("")]).await.unwrap().is_none());
    app.rollback("write").await.unwrap();
    app.begin("verify").await.unwrap();
    assert!(app.get("verify", ITEMS, &[text_cell("counted-item")]).await.unwrap().is_none());
    assert!(app.get("verify", counts, &[text_cell("first"), text_cell("")]).await.unwrap().is_none());
    app.rollback("verify").await.unwrap();
}

#[tokio::test]
async fn rolled_back_attempts_follow_run_source_order_and_preserve_thread_ownership() {
    let fixture = TestFixture::start().await;
    let store = fixture.initialize().await;
    let mut app = initialize(&store).await;
    prepare(&store).await;
    prepare_reads(&store).await;
    app.begin("write").await.unwrap();
    // Insertion, IDs, and ordinals deliberately disagree. The upstream 89
    // plan visits runs by source rowid, then attempts by each run's rowids.
    for (id, ordinal, owner, status) in [
        ("z", 9, "thread", "rolled_back"),
        ("b", 2, "thread", "rolled_back"),
        ("a", 1, "thread", "completed"),
        ("foreign", 1, "other", "rolled_back"),
    ] {
        let mut payload = run(id, ordinal);
        payload["status"] = json!(status);
        payload["threadId"] = json!(owner);
        apply(&mut app, "write", event("run.created", owner, payload))
            .await
            .unwrap();
    }
    // Interleaving attempts also distinguishes run-major order from a global
    // attempt-row scan. Both sides of the join must belong to the thread.
    for (id, run_id, ordinal, owner) in [
        ("b-second", "b", 2, "thread"),
        ("z-second", "z", 2, "thread"),
        ("foreign-attempt", "z", 3, "other"),
        ("b-first", "b", 1, "thread"),
        ("z-first", "z", 1, "thread"),
        ("completed-run", "a", 1, "thread"),
        ("foreign-run", "foreign", 1, "thread"),
    ] {
        apply(&mut app, "write", event("run-attempt.created", owner, json!({
            "id":id,"runId":run_id,"attemptOrdinal":ordinal,"rootNodeId":"node",
            "providerInstanceId":"instance","providerThreadId":"provider",
            "providerTurnId":null,"status":"completed",
        }))).await.unwrap();
    }
    let query = || serde_json::from_value(json!({
        "kind":"rolledback_attempts","thread_id":"thread",
    })).unwrap();
    let expected = json!([
        {"id":"z-second"},{"id":"z-first"},{"id":"b-second"},{"id":"b-first"},
    ]);
    assert_eq!(app.projections_context("write", query()).await.unwrap(), expected);
    app.savepoint("write", "status").await.unwrap();
    let mut changed = run("z", 9);
    changed["status"] = json!("completed");
    apply(&mut app, "write", event("run.updated", "thread", changed.clone())).await.unwrap();
    assert_eq!(app.projections_context("write", query()).await.unwrap(), json!([
        {"id":"b-second"},{"id":"b-first"},
    ]));
    app.rollback_to("write", "status").await.unwrap();
    app.release("write", "status").await.unwrap();
    assert_eq!(app.projections_context("write", query()).await.unwrap(), expected);
    // Updating a selected row retains its original source order.
    changed["status"] = json!("rolled_back");
    changed["completedAt"] = json!(NOW);
    apply(&mut app, "write", event("run.updated", "thread", changed)).await.unwrap();
    app.commit("write").await.unwrap();
    drop(app);
    store.shutdown().await.unwrap();
    let reopened = fixture.connect().await;
    let mut app = ApplicationTransactions::new(reopened.clone());
    app.begin("read").await.unwrap();
    assert_eq!(app.projections_context("read", query()).await.unwrap(), expected);
    app.rollback("read").await.unwrap();
}
