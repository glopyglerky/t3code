#[path = "../src/test_support.rs"]
mod test_support;

use serde_json::{Value, json};
use t3_code_ls_storage::outbox::{
    CancelUnsettled, Claim, EffectStatus, EffectType, EnqueueEffect, Settle,
};
use t3_code_ls_storage::thread_service::{ThreadRequest, execute_thread};
use t3_code_ls_storage::{
    CommitBatch, CommitOutcome, Error, ProjectionMutation, Store, request_digest,
};

const CREATED: &str = "2026-11-09T10:00:00.000Z";
const LATER: &str = "2026-11-09T10:01:00.000Z";
const LATEST: &str = "2026-11-09T10:02:00.000Z";

fn project() -> Value {
    json!({
        "projectId": "project-1", "title": "Project", "workspaceRoot": "/workspace/project",
        "defaultModelSelection": null, "defaultThreadEnvMode": null, "autoPull": false,
        "faviconPath": null, "projectIcon": null, "scripts": [], "pinnedAt": null,
        "archivedAt": null, "createdAt": CREATED, "updatedAt": CREATED, "deletedAt": null,
    })
}

async fn put(store: &Store, command: &str, key: &str, payload: Value) {
    let batch = CommitBatch {
        command_id: command.into(),
        request_digest: request_digest(&json!({"key": key, "payload": payload})),
        expected_revision: store.snapshot().await.unwrap().revision,
        events: vec![],
        projections: vec![ProjectionMutation::Put {
            key: key.into(),
            payload,
        }],
        effects: vec![],
        result: json!(null),
    };
    store.commit(&batch).await.unwrap();
}

async fn initialized() -> (test_support::TestFixture, Store) {
    let fixture = test_support::TestFixture::start().await;
    let store = fixture.initialize().await;
    put(&store, "seed-project", "project/project-1", project()).await;
    (fixture, store)
}

fn command(kind: &str, id: &str) -> Value {
    json!({"type": kind, "commandId": id, "threadId": "thread-1"})
}

fn create(id: &str) -> Value {
    json!({
        "type": "thread.create", "commandId": id, "threadId": "thread-1",
        "projectId": "project-1", "title": "Thread", "modelSelection": {
            "instanceId": "codex", "model": "test-model",
            "options": [{"id": "option", "value": false}],
        },
        "runtimeMode": "approval-required", "interactionMode": "default",
        "branch": null, "worktreePath": null, "createdBy": "user", "creationSource": "web",
    })
}

fn request(command: Value, now: &str) -> ThreadRequest {
    ThreadRequest {
        event_id: format!("event:{}", command["commandId"].as_str().unwrap()),
        command,
        now: now.into(),
    }
}

async fn apply(store: &Store, command: Value, now: &str) -> CommitOutcome {
    execute_thread(store, request(command, now)).await.unwrap()
}

async fn thread(store: &Store) -> Value {
    store
        .snapshot()
        .await
        .unwrap()
        .projections
        .into_iter()
        .find(|row| row.key == "thread/thread-1")
        .unwrap()
        .payload
}

#[tokio::test]
async fn creates_upstream_row_and_event_and_replays_after_reopening() {
    let (fixture, store) = initialized().await;
    let mut input = create("create");
    input["title"] = json!(" \u{feff}Thread  ");
    input["modelSelection"]["model"] = json!(" test-model ");
    let first = apply(&store, input.clone(), CREATED).await;
    let expected = json!({
        "createdBy": "user", "creationSource": "web", "id": "thread-1", "projectId": "project-1",
        "title": "Thread", "providerInstanceId": "codex", "modelSelection": {
            "instanceId": "codex", "model": "test-model", "options": [{"id":"option","value":false}],
        },
        "runtimeMode": "approval-required", "interactionMode": "default", "branch": null,
        "worktreePath": null, "activeProviderThreadId": null, "pullRequests": [],
        "lineage": {"parentThreadId": null, "relationshipToParent": null, "rootThreadId": "thread-1"},
        "forkedFrom": null, "createdAt": CREATED, "updatedAt": CREATED, "archivedAt": null,
        "settledOverride": null, "settledAt": null, "snoozedUntil": null, "snoozedAt": null,
        "lastVisitedAt": null, "deletedAt": null,
    });
    assert_eq!(thread(&store).await, expected);
    assert_eq!(first.receipt.first_sequence, Some(1));
    let event = json!({"id": "event:create", "type": "thread.created", "threadId": "thread-1",
        "providerInstanceId": "codex", "occurredAt": CREATED, "payload": expected});
    assert_eq!(store.events(0, 10).await.unwrap()[0].payload, event);
    assert_eq!(
        first.receipt.result,
        json!({"ok":{"event":event,"thread":expected}})
    );
    store.shutdown().await.unwrap();
    let reopened = fixture.connect().await;
    let mut replay_request = request(input, LATEST);
    replay_request.event_id = "unused-retry-event".into();
    let replay = execute_thread(&reopened, replay_request).await.unwrap();
    assert!(replay.replayed);
    assert_eq!(replay.receipt, first.receipt);
    assert_eq!(reopened.events(0, 10).await.unwrap().len(), 1);
    assert_eq!(thread(&reopened).await, expected);
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
async fn command_conflicts_and_tombstones_cannot_replace_thread_history() {
    let (_fixture, store) = initialized().await;
    apply(&store, create("create"), CREATED).await;
    let before = store.snapshot().await.unwrap();
    let mut different = create("create");
    different["title"] = json!("Another title");
    assert!(matches!(
        execute_thread(&store, request(different, LATER)).await,
        Err(Error::CommandConflict)
    ));
    assert!(
        execute_thread(&store, request(create("new-create"), LATER))
            .await
            .is_err()
    );
    assert_eq!(store.snapshot().await.unwrap(), before);
    assert!(store.read_receipt("new-create").await.unwrap().is_none());
    let mut deleted = thread(&store).await;
    deleted["deletedAt"] = json!(LATER);
    put(&store, "tombstone", "thread/thread-1", deleted).await;
    let before = store.snapshot().await.unwrap();
    assert!(
        execute_thread(&store, request(create("recreate"), LATEST))
            .await
            .is_err()
    );
    assert!(
        execute_thread(
            &store,
            request(command("thread.pin", "pin-deleted"), LATEST)
        )
        .await
        .is_err()
    );
    assert_eq!(store.snapshot().await.unwrap(), before);
}

#[tokio::test]
async fn metadata_preserves_omission_and_null_and_checks_expected_workspace() {
    let (_fixture, store) = initialized().await;
    apply(&store, create("create"), CREATED).await;
    let mut update = command("thread.metadata.update", "metadata");
    update["title"] = json!(" Renamed ");
    update["branch"] = json!(" feature ");
    update["expectedWorktreePath"] = Value::Null;
    update["expectedBranch"] = Value::Null;
    update["expectedEmpty"] = json!(true);
    apply(&store, update.clone(), LATER).await;
    let row = thread(&store).await;
    assert_eq!(row["title"], "Renamed");
    assert_eq!(row["branch"], "feature");
    assert_eq!(row["worktreePath"], Value::Null);
    assert_eq!(row["titleRegeneration"], Value::Null);
    assert_eq!(row["updatedAt"], LATER);
    assert!(!row.as_object().unwrap().contains_key("pinnedAt"));
    let before = store.snapshot().await.unwrap();
    update["commandId"] = json!("changed-expectation");
    assert!(
        execute_thread(&store, request(update, LATEST))
            .await
            .is_err()
    );
    assert_eq!(store.snapshot().await.unwrap(), before);
    let mut clear = command("thread.metadata.update", "clear-branch");
    clear["branch"] = Value::Null;
    clear["worktreePath"] = json!("/workspace/worktree");
    clear["expectedBranch"] = json!("feature");
    apply(&store, clear, LATEST).await;
    let row = thread(&store).await;
    assert_eq!(row["title"], "Renamed");
    assert_eq!(row["branch"], Value::Null);
    assert_eq!(row["worktreePath"], "/workspace/worktree");
    assert_eq!(row["createdAt"], CREATED);
}

#[tokio::test]
async fn pinning_reordering_and_repeated_unpin_preserve_upstream_activity_times() {
    let (_fixture, store) = initialized().await;
    apply(&store, create("create"), CREATED).await;
    let mut pin = command("thread.pin", "pin");
    pin["orderKey"] = json!(" first ");
    apply(&store, pin, LATER).await;
    let mut repeated = command("thread.pin", "pin-again");
    repeated["orderKey"] = json!("ignored");
    apply(&store, repeated, LATEST).await;
    let row = thread(&store).await;
    assert_eq!(row["pinnedAt"], LATER);
    assert_eq!(row["updatedAt"], LATER);
    assert_eq!(row["pinOrderKey"], "first");
    let mut order = command("thread.pin.reorder", "same-order");
    order["orderKey"] = json!("first");
    apply(&store, order.clone(), LATEST).await;
    assert_eq!(thread(&store).await["updatedAt"], LATER);
    order["commandId"] = json!("new-order");
    order["orderKey"] = json!("second");
    apply(&store, order, LATEST).await;
    assert_eq!(thread(&store).await["updatedAt"], LATEST);
    apply(&store, command("thread.unpin", "unpin"), LATEST).await;
    apply(
        &store,
        command("thread.unpin", "unpin-again"),
        "2026-11-09T10:03:00.000Z",
    )
    .await;
    let row = thread(&store).await;
    assert_eq!(row["pinnedAt"], Value::Null);
    assert_eq!(row["pinOrderKey"], Value::Null);
    assert_eq!(row["updatedAt"], LATEST);
    let mut active = command("thread.active.reorder", "active-order");
    active["orderKey"] = json!("a");
    apply(&store, active, "2026-11-09T10:04:00.000Z").await;
    assert_eq!(thread(&store).await["updatedAt"], LATEST);
}

#[tokio::test]
async fn pin_promotes_settled_and_snoozed_threads_and_retains_original_pin_time() {
    let (_fixture, store) = initialized().await;
    apply(&store, create("create"), CREATED).await;
    let mut row = thread(&store).await;
    row["pinnedAt"] = json!(CREATED);
    row["settledOverride"] = json!("settled");
    row["settledAt"] = json!(CREATED);
    row["snoozedUntil"] = json!(LATEST);
    row["snoozedAt"] = json!(CREATED);
    put(&store, "settled-state", "thread/thread-1", row).await;
    apply(&store, command("thread.pin", "promote"), LATER).await;
    let row = thread(&store).await;
    assert_eq!(row["pinnedAt"], CREATED);
    assert_eq!(row["settledOverride"], "active");
    assert_eq!(row["settledAt"], Value::Null);
    assert_eq!(row["unsettledAt"], LATER);
    assert_eq!(row["lastSnoozeWakeAt"], LATER);
    assert_eq!(row["snoozedUntil"], Value::Null);
    assert_eq!(row["updatedAt"], LATER);
}

#[tokio::test]
async fn visit_watermarks_only_move_forward_and_never_change_activity_time() {
    let (_fixture, store) = initialized().await;
    apply(&store, create("create"), CREATED).await;
    for (id, visited) in [("visit", LATER), ("older", CREATED), ("equal", LATER)] {
        let mut visit = command("thread.visit", id);
        visit["visitedAt"] = json!(visited);
        visit["opened"] = json!(false);
        visit["searchQuery"] = json!("a query without an opening");
        let outcome = apply(&store, visit, LATEST).await;
        assert_eq!(
            outcome.receipt.result["ok"]["event"]["type"],
            "thread.visited"
        );
    }
    let row = thread(&store).await;
    assert_eq!(row["lastVisitedAt"], LATER);
    assert_eq!(row["updatedAt"], CREATED);
    assert!(!row.as_object().unwrap().contains_key("opens"));
    assert_eq!(store.events(0, 10).await.unwrap().len(), 4);
}

#[tokio::test]
async fn archive_commits_cleanup_and_receipt_and_replay_does_not_requeue_cancelled_work() {
    let (fixture, store) = initialized().await;
    apply(&store, create("create"), CREATED).await;
    let first = apply(&store, command("thread.archive", "archive"), LATER).await;
    let effect_id = "effect:archive:terminal.cleanup";
    let effect = store.outbox_get(effect_id).await.unwrap().unwrap();
    assert_eq!(effect.thread_id, "thread-1");
    assert_eq!(effect.command_id, "archive");
    assert_eq!(effect.request, json!({"type":"terminal.cleanup"}));
    assert_eq!(effect.status, EffectStatus::Pending);
    assert_eq!(effect.created_at, LATER);
    assert_eq!(thread(&store).await["archivedAt"], LATER);
    let extra = store.read_outbox_receipt("archive").await.unwrap().unwrap();
    assert_eq!(extra.enqueued, 1);
    assert!(extra.cancelled.is_empty());
    assert_eq!(extra.commit.receipt, first.receipt);
    let cancelled = store
        .outbox_cancel_unsettled(&CancelUnsettled {
            thread_id: "thread-1".into(),
            effect_types: vec![EffectType::TerminalCleanup],
            reason: "controlled cancellation".into(),
            now: LATEST.into(),
        })
        .await
        .unwrap();
    assert_eq!(cancelled, [effect_id]);
    store.shutdown().await.unwrap();
    let reopened = fixture.connect().await;
    let replay = apply(&reopened, command("thread.archive", "archive"), LATEST).await;
    assert!(replay.replayed);
    assert_eq!(replay.receipt, first.receipt);
    assert_eq!(
        reopened
            .outbox_get(effect_id)
            .await
            .unwrap()
            .unwrap()
            .status,
        EffectStatus::Cancelled
    );
    assert_eq!(reopened.events(0, 10).await.unwrap().len(), 2);
    apply(&reopened, command("thread.unarchive", "unarchive"), LATEST).await;
    assert_eq!(thread(&reopened).await["archivedAt"], Value::Null);
    assert_eq!(thread(&reopened).await["updatedAt"], LATEST);
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
async fn archive_replay_keeps_a_completed_cleanup_and_rearchive_creates_a_new_obligation() {
    let (_fixture, store) = initialized().await;
    apply(&store, create("create"), CREATED).await;
    let first = apply(&store, command("thread.archive", "archive"), LATER).await;
    let claimed = store
        .outbox_claim_next(&Claim {
            worker_id: "test-worker".into(),
            now: LATER.into(),
            exclude_restart_continuations: false,
        })
        .await
        .unwrap()
        .unwrap();
    assert!(
        store
            .outbox_succeed(&Settle {
                effect_id: claimed.id.clone(),
                worker_id: "test-worker".into(),
                now: LATEST.into()
            })
            .await
            .unwrap()
    );
    let settled = store.outbox_get(&claimed.id).await.unwrap();
    let replay = apply(&store, command("thread.archive", "archive"), LATEST).await;
    assert_eq!(replay.receipt, first.receipt);
    assert_eq!(store.outbox_get(&claimed.id).await.unwrap(), settled);
    apply(&store, command("thread.unarchive", "unarchive"), LATEST).await;
    apply(&store, command("thread.archive", "archive-again"), LATEST).await;
    assert_eq!(
        store
            .outbox_get("effect:archive-again:terminal.cleanup")
            .await
            .unwrap()
            .unwrap()
            .status,
        EffectStatus::Pending
    );
    assert_eq!(store.outbox_get(&claimed.id).await.unwrap(), settled);
}

#[tokio::test]
async fn runtime_records_and_unsettled_effects_block_unported_cleanup_without_partial_writes() {
    let (_fixture, store) = initialized().await;
    apply(&store, create("create"), CREATED).await;
    store
        .outbox_enqueue(
            &[EnqueueEffect {
                id: "existing-work".into(),
                command_id: "old".into(),
                thread_id: "thread-1".into(),
                request: json!({"type":"terminal.cleanup"}),
                available_at: None,
            }],
            CREATED,
        )
        .await
        .unwrap();
    let before = store.snapshot().await.unwrap();
    assert!(
        execute_thread(
            &store,
            request(command("thread.archive", "blocked-archive"), LATER)
        )
        .await
        .is_err()
    );
    assert_eq!(store.snapshot().await.unwrap(), before);
    assert!(
        store
            .read_receipt("blocked-archive")
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .outbox_get("effect:blocked-archive:terminal.cleanup")
            .await
            .unwrap()
            .is_none()
    );
    store
        .outbox_cancel_unsettled(&CancelUnsettled {
            thread_id: "thread-1".into(),
            effect_types: vec![EffectType::TerminalCleanup],
            reason: "test".into(),
            now: LATER.into(),
        })
        .await
        .unwrap();
    put(
        &store,
        "runtime-state",
        "run/run-1",
        json!({"id":"run-1","threadId":"thread-1","status":"completed"}),
    )
    .await;
    let mut metadata = command("thread.metadata.update", "empty-guard");
    metadata["expectedEmpty"] = json!(true);
    metadata["title"] = json!("Must not land");
    let before = store.snapshot().await.unwrap();
    assert!(
        execute_thread(&store, request(metadata, LATEST))
            .await
            .is_err()
    );
    assert!(
        execute_thread(
            &store,
            request(command("thread.archive", "runtime-archive"), LATEST)
        )
        .await
        .is_err()
    );
    assert_eq!(store.snapshot().await.unwrap(), before);
}

#[tokio::test]
async fn native_provider_sessions_and_requests_fence_archive_workspace_and_empty_thread_commands() {
    for (key, payload) in [
        (
            "native/session/thread-1",
            json!({"threadId":"thread-1", "status":"stopped"}),
        ),
        (
            "native/request/request-1",
            json!({"threadId":"thread-1", "status":"completed"}),
        ),
    ] {
        let (_fixture, store) = initialized().await;
        apply(&store, create("create"), CREATED).await;
        put(&store, "native-runtime", key, payload).await;
        let mut workspace = command("thread.metadata.update", "move-workspace");
        workspace["worktreePath"] = json!("/workspace/new");
        let mut empty = command("thread.metadata.update", "requires-empty");
        empty["expectedEmpty"] = json!(true);
        empty["branch"] = json!("new-branch");
        let before = store.snapshot().await.unwrap();
        for blocked in [command("thread.archive", "archive"), workspace, empty] {
            let id = blocked["commandId"].as_str().unwrap().to_owned();
            assert!(
                execute_thread(&store, request(blocked, LATER))
                    .await
                    .is_err(),
                "{key}"
            );
            assert!(store.read_receipt(&id).await.unwrap().is_none());
            assert_eq!(store.snapshot().await.unwrap(), before);
        }
        let mut title = command("thread.metadata.update", "rename");
        title["title"] = json!("Still editable");
        apply(&store, title, LATER).await;
        assert_eq!(thread(&store).await["title"], "Still editable");
    }
}

fn stopped_session() -> Value {
    json!({
        "threadId":"thread-1", "sessionId":"session-1", "status":"closed", "loaded":false,
        "activeTurnId":null, "processState":"stopped", "nativeThreadId":"native-thread-1",
        "contextFingerprint":"original-workspace-fingerprint", "updatedAt":CREATED,
    })
}

#[tokio::test]
async fn reaped_native_session_allows_archive_or_workspace_change_without_rewriting_its_history() {
    for archive in [false, true] {
        let (_fixture, store) = initialized().await;
        apply(&store, create("create"), CREATED).await;
        let session = stopped_session();
        put(
            &store,
            "closed-session",
            "native/session/thread-1",
            session.clone(),
        )
        .await;
        let mutation = if archive {
            command("thread.archive", "archive-after-close")
        } else {
            let mut update = command("thread.metadata.update", "workspace-after-close");
            update["worktreePath"] = json!("/workspace/new");
            update["expectedWorktreePath"] = Value::Null;
            update
        };
        apply(&store, mutation, LATER).await;
        let snapshot = store.snapshot().await.unwrap();
        assert_eq!(
            snapshot
                .projections
                .iter()
                .find(|row| row.key == "native/session/thread-1")
                .unwrap()
                .payload,
            session
        );
        if archive {
            assert_eq!(thread(&store).await["archivedAt"], LATER);
            assert_eq!(
                store
                    .outbox_get("effect:archive-after-close:terminal.cleanup")
                    .await
                    .unwrap()
                    .unwrap()
                    .status,
                EffectStatus::Pending
            );
        } else {
            assert_eq!(thread(&store).await["worktreePath"], "/workspace/new");
            assert_eq!(thread(&store).await["archivedAt"], Value::Null);
        }
    }
}

#[tokio::test]
async fn unknown_or_incompletely_closed_native_process_never_authorizes_cleanup() {
    for (field, value) in [
        ("processState", json!("unknown")),
        ("processState", Value::Null),
        ("status", json!("open")),
        ("loaded", json!(true)),
        ("activeTurnId", json!("turn-1")),
        ("sessionId", json!("")),
        ("threadId", json!("different-thread")),
    ] {
        let (_fixture, store) = initialized().await;
        apply(&store, create("create"), CREATED).await;
        let mut session = stopped_session();
        session[field] = value;
        put(
            &store,
            "uncertain-session",
            "native/session/thread-1",
            session,
        )
        .await;
        let before = store.snapshot().await.unwrap();
        let mut workspace = command("thread.metadata.update", "move-workspace");
        workspace["worktreePath"] = json!("/workspace/new");
        for mutation in [command("thread.archive", "archive"), workspace] {
            assert!(
                execute_thread(&store, request(mutation, LATER))
                    .await
                    .is_err(),
                "{field}"
            );
            assert_eq!(store.snapshot().await.unwrap(), before);
        }
    }
    let (_fixture, store) = initialized().await;
    apply(&store, create("create"), CREATED).await;
    let mut session = stopped_session();
    session.as_object_mut().unwrap().remove("activeTurnId");
    put(
        &store,
        "missing-active-marker",
        "native/session/thread-1",
        session,
    )
    .await;
    assert!(
        execute_thread(&store, request(command("thread.archive", "archive"), LATER))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn closed_session_does_not_override_unresolved_requests_approvals_or_legacy_runtime_rows() {
    for (key, payload) in [
        (
            "native/request/accepted",
            json!({"threadId":"thread-1","state":"accepted"}),
        ),
        (
            "native/request/uncertain",
            json!({"threadId":"thread-1","state":"uncertain"}),
        ),
        (
            "native/approval/session-1/approval",
            json!({"threadId":"thread-1","state":"pending"}),
        ),
        (
            "native/approval/session-1/retained",
            json!({"threadId":"thread-1","state":"resolved"}),
        ),
        (
            "provider-session/legacy",
            json!({"threadId":"thread-1","status":"stopped"}),
        ),
        (
            "run/completed",
            json!({"threadId":"thread-1","status":"completed"}),
        ),
    ] {
        let (_fixture, store) = initialized().await;
        apply(&store, create("create"), CREATED).await;
        put(
            &store,
            "closed-session",
            "native/session/thread-1",
            stopped_session(),
        )
        .await;
        put(&store, "retained-runtime", key, payload).await;
        let before = store.snapshot().await.unwrap();
        assert!(
            execute_thread(&store, request(command("thread.archive", "archive"), LATER))
                .await
                .is_err(),
            "{key}"
        );
        let mut workspace = command("thread.metadata.update", "move");
        workspace["worktreePath"] = json!("/workspace/new");
        assert!(
            execute_thread(&store, request(workspace, LATER))
                .await
                .is_err(),
            "{key}"
        );
        assert_eq!(store.snapshot().await.unwrap(), before);
    }
}

#[tokio::test]
async fn auto_settle_preference_preserves_the_original_disable_time_and_noop_activity() {
    let (_fixture, store) = initialized().await;
    apply(&store, create("create"), CREATED).await;
    let mut enabled = command("thread.auto-settle.set", "already-enabled");
    enabled["enabled"] = json!(true);
    apply(&store, enabled.clone(), LATER).await;
    assert_eq!(thread(&store).await["updatedAt"], CREATED);
    let mut disabled = command("thread.auto-settle.set", "disable");
    disabled["enabled"] = json!(false);
    apply(&store, disabled.clone(), LATER).await;
    disabled["commandId"] = json!("disable-again");
    apply(&store, disabled, LATEST).await;
    let row = thread(&store).await;
    assert_eq!(row["autoSettleDisabledAt"], LATER);
    assert_eq!(row["updatedAt"], LATER);
    enabled["commandId"] = json!("enable");
    apply(&store, enabled, LATEST).await;
    let row = thread(&store).await;
    assert_eq!(row["autoSettleDisabledAt"], Value::Null);
    assert_eq!(row["updatedAt"], LATEST);
}

#[tokio::test]
async fn malformed_stored_thread_is_rejected_without_panicking_or_partially_mutating() {
    for field in ["modelSelection", "lineage", "updatedAt", "lastVisitedAt"] {
        let (_fixture, store) = initialized().await;
        apply(&store, create("create"), CREATED).await;
        let mut malformed = thread(&store).await;
        malformed.as_object_mut().unwrap().remove(field);
        put(&store, "malformed", "thread/thread-1", malformed).await;
        let before = store.snapshot().await.unwrap();
        assert!(
            execute_thread(&store, request(command("thread.pin", "pin"), LATER))
                .await
                .is_err(),
            "{field}"
        );
        assert_eq!(store.snapshot().await.unwrap(), before);
        assert!(store.read_receipt("pin").await.unwrap().is_none());
    }
}

#[tokio::test]
async fn invalid_and_unsupported_commands_do_not_create_receipts_or_mutate_state() {
    let (_fixture, store) = initialized().await;
    apply(&store, create("create"), CREATED).await;
    let mut cases = Vec::new();
    for (key, value) in [
        ("title", Value::Null),
        ("regenerateTitle", json!(true)),
        ("linkedPullRequest", Value::Null),
        ("limitRecovery", Value::Null),
        ("unknown", json!(true)),
    ] {
        let mut value_command = command("thread.metadata.update", key);
        value_command[key] = value;
        cases.push(value_command);
    }
    for kind in [
        "thread.meta.update",
        "thread.delete",
        "thread.stop",
        "thread.model-selection.set",
        "message.dispatch",
    ] {
        cases.push(command(kind, kind));
    }
    let mut open = command("thread.visit", "open");
    open["visitedAt"] = json!(CREATED);
    open["opened"] = json!(true);
    cases.push(open);
    for at in [
        "2026-02-30T00:00:00.000Z",
        "2026-11-09T10:00:00+00:00",
        "bad-time",
    ] {
        let mut visit = command("thread.visit", "bad-time");
        visit["visitedAt"] = json!(at);
        cases.push(visit);
    }
    let before = store.snapshot().await.unwrap();
    for value in cases {
        let id = value["commandId"].as_str().unwrap().to_owned();
        assert!(
            execute_thread(&store, request(value, LATER)).await.is_err(),
            "{id}"
        );
        assert!(store.read_receipt(&id).await.unwrap().is_none());
        assert_eq!(store.snapshot().await.unwrap(), before);
    }
}

#[tokio::test]
async fn unknown_deleted_or_mismatched_project_ownership_rejects_creation() {
    let (_fixture, store) = initialized().await;
    let mut missing = create("missing-project");
    missing["projectId"] = json!("absent");
    assert!(
        execute_thread(&store, request(missing, CREATED))
            .await
            .is_err()
    );
    let mut tombstone = project();
    tombstone["deletedAt"] = json!(CREATED);
    put(&store, "delete-project", "project/project-1", tombstone).await;
    assert!(
        execute_thread(&store, request(create("deleted-project"), CREATED))
            .await
            .is_err()
    );
    let mut wrong = project();
    wrong["projectId"] = json!("another-project");
    put(&store, "mismatch-project", "project/project-1", wrong).await;
    assert!(matches!(
        execute_thread(&store, request(create("mismatched-project"), CREATED)).await,
        Err(Error::IncompatibleStore)
    ));
    assert!(store.events(0, 10).await.unwrap().is_empty());
    assert_eq!(store.snapshot().await.unwrap().projections.len(), 1);
}

#[tokio::test]
async fn duplicate_event_rolls_back_archive_projection_receipt_and_cleanup() {
    let (_fixture, store) = initialized().await;
    apply(&store, create("create"), CREATED).await;
    let before = store.snapshot().await.unwrap();
    let mut archive = request(command("thread.archive", "archive"), LATER);
    archive.event_id = "event:create".into();
    assert!(matches!(
        execute_thread(&store, archive).await,
        Err(Error::DuplicateRecord)
    ));
    assert_eq!(store.snapshot().await.unwrap(), before);
    assert!(store.read_receipt("archive").await.unwrap().is_none());
    assert!(
        store
            .outbox_get("effect:archive:terminal.cleanup")
            .await
            .unwrap()
            .is_none()
    );
    apply(&store, command("thread.archive", "archive"), LATER).await;
    assert_eq!(thread(&store).await["archivedAt"], LATER);
}

#[tokio::test]
async fn concurrent_creation_has_one_winner_and_stale_external_plans_cannot_overwrite_it() {
    let (fixture, store) = initialized().await;
    let second = fixture.connect().await;
    let old = store.snapshot().await.unwrap();
    let (a, b) = tokio::join!(
        execute_thread(&store, request(create("create-a"), CREATED)),
        execute_thread(&second, request(create("create-b"), LATER)),
    );
    assert_ne!(a.is_ok(), b.is_ok());
    let losing = if a.is_ok() {
        b.unwrap_err()
    } else {
        a.unwrap_err()
    };
    assert!(matches!(
        losing,
        Error::TransactionConflict | Error::Invalid(_)
    ));
    assert_eq!(store.events(0, 10).await.unwrap().len(), 1);
    let before = store.snapshot().await.unwrap();
    let stale = CommitBatch {
        command_id: "stale-overwrite".into(),
        request_digest: request_digest(&json!({"stale":true})),
        expected_revision: old.revision,
        events: vec![],
        projections: vec![ProjectionMutation::Delete {
            key: "thread/thread-1".into(),
        }],
        effects: vec![],
        result: Value::Null,
    };
    assert!(matches!(
        store.commit(&stale).await,
        Err(Error::StaleRevision { .. })
    ));
    assert_eq!(store.snapshot().await.unwrap(), before);
    assert!(
        store
            .read_receipt("stale-overwrite")
            .await
            .unwrap()
            .is_none()
    );
}
