use super::*;
use crate::application::{ApplicationSchema, ApplicationTable};
use crate::test_support::TestFixture;

const NOW: &str = "2026-11-10T12:34:56.123Z";

fn schema() -> ApplicationSchema {
    ApplicationSchema {
        version: 1,
        tables: SPECS
            .iter()
            .map(|spec| ApplicationTable {
                name: spec.name.into(),
                columns: spec
                    .columns
                    .iter()
                    .map(|(name, _)| (*name).into())
                    .collect(),
                primary_key: spec.primary.iter().map(|name| (*name).into()).collect(),
            })
            .collect(),
    }
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
