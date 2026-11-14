use super::*;
use crate::test_support::TestFixture;

fn legacy_row() -> Value {
    json!({
        "projectId": "project:one", "title": "Project", "workspaceRoot": "/fixture/project",
        "defaultModelSelection": null, "defaultThreadEnvMode": null, "autoPull": false,
        "faviconPath": null, "projectIcon": null, "scripts": [], "pinnedAt": null,
        "archivedAt": null, "createdAt": "2026-11-08T12:00:00.000Z",
        "updatedAt": "2026-11-08T12:00:00.000Z", "deletedAt": null,
    })
}

fn title_event() -> Value {
    json!({
        "aggregateKind": "project", "aggregateId": "project:one", "type": "project.meta-updated",
        "payload": { "projectId": "project:one", "title": "Project" },
    })
}

fn update(command_id: &str) -> ProjectRequest {
    ProjectRequest {
        command: json!({
            "type": "project.meta.update", "commandId": command_id,
            "projectId": "project:one", "autoPull": true,
        }),
        event_id: format!("event:{command_id}"),
        now: 1_794_139_200_000,
    }
}

async fn seed(store: &Store, row: Value, events: Vec<Value>) -> CommitOutcome {
    store
        .commit(&CommitBatch {
            command_id: "seed".into(),
            request_digest: request_digest(&json!({"seed": true})),
            expected_revision: 0,
            events: events
                .into_iter()
                .enumerate()
                .map(|(index, payload)| Record {
                    id: format!("old-event:{index}"),
                    payload,
                })
                .collect(),
            projections: vec![ProjectionMutation::Put {
                key: "project/project:one".into(),
                payload: row.clone(),
            }],
            effects: vec![],
            result: json!({"project": row}),
        })
        .await
        .unwrap()
}

#[tokio::test]
async fn legacy_title_history_is_bounded_by_sequence_and_does_not_mutate_reads() {
    let fixture = TestFixture::start().await;
    let store = fixture.initialize().await;
    let mut events = vec![title_event(); 257];
    events[0]["type"] = json!("project.created");
    events[1]["aggregateKind"] = json!("thread");
    events[2]["aggregateId"] = json!("project:another");
    for event in &mut events[3..256] {
        event["payload"]["title"] = json!(null);
    }
    // Migration 087 identifies the stream by its envelope, not by payload fields.
    events[256]["payload"]["projectId"] = json!("project:another");
    events[256]["payload"]["title"] = json!("");
    let original = seed(&store, legacy_row(), events).await;
    let before = store.snapshot().await.unwrap();
    assert!(
        !legacy_custom_title(&store, "project:one", 256)
            .await
            .unwrap()
    );
    assert!(
        legacy_custom_title(&store, "project:one", 257)
            .await
            .unwrap()
    );
    assert!(
        !legacy_custom_title(&store, "project:missing", 257)
            .await
            .unwrap()
    );
    assert!(matches!(
        legacy_custom_title(&store, "project:missing", 258).await,
        Err(Error::IncompatibleStore)
    ));
    assert_eq!(store.snapshot().await.unwrap(), before);
    assert_eq!(
        store.read_receipt("seed").await.unwrap(),
        Some(original.receipt.clone())
    );

    let request = update("metadata-after-upgrade");
    let committed = execute_project_native(&store, request.clone())
        .await
        .unwrap();
    assert_eq!(
        committed.receipt.result["ok"]["project"]["hasCustomTitle"],
        true
    );
    assert_eq!(
        store.snapshot().await.unwrap().projections[0].payload["hasCustomTitle"],
        true
    );
    assert_eq!(
        store.read_receipt("seed").await.unwrap(),
        Some(original.receipt)
    );
    store.shutdown().await.unwrap();

    let reopened = fixture.connect().await;
    let replay = execute_project_native(&reopened, request).await.unwrap();
    assert!(replay.replayed);
    assert_eq!(replay.receipt, committed.receipt);
    let next = execute_project_native(&reopened, update("after-restart"))
        .await
        .unwrap();
    assert_eq!(next.receipt.result["ok"]["project"]["hasCustomTitle"], true);
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
async fn legacy_rows_without_title_edits_default_false_until_a_title_command() {
    let fixture = TestFixture::start().await;
    let store = fixture.initialize().await;
    let mut event = title_event();
    event["type"] = json!("project.created");
    seed(&store, legacy_row(), vec![event]).await;
    let first = execute_project_native(&store, update("unrelated-update"))
        .await
        .unwrap();
    assert_eq!(
        first.receipt.result["ok"]["project"]["hasCustomTitle"],
        false
    );
    let mut same_title = update("same-title");
    same_title.command["title"] = json!("Project");
    let renamed = execute_project_native(&store, same_title).await.unwrap();
    assert_eq!(
        renamed.receipt.result["ok"]["project"]["hasCustomTitle"],
        true
    );
    assert_eq!(renamed.receipt.result["ok"]["project"]["title"], "Project");
    store.shutdown().await.unwrap();
}

#[tokio::test]
async fn legacy_command_receipts_replay_without_rewriting_the_original_result() {
    let fixture = TestFixture::start().await;
    let store = fixture.initialize().await;
    let request = update("legacy-command");
    let row = legacy_row();
    let event = title_event();
    // Historical canonical command digest: do not derive the seed with the
    // current encoder, which would hide a symmetrical compatibility regression.
    const LEGACY_DIGEST: &str = "e74c372438ee32037f302896179910e4a3f35c23b49aafa4100bbcecfd4af52d";
    let original = store
        .commit(&CommitBatch {
            command_id: "legacy-command".into(),
            request_digest: LEGACY_DIGEST.into(),
            expected_revision: 0,
            events: vec![Record {
                id: request.event_id.clone(),
                payload: event.clone(),
            }],
            projections: vec![ProjectionMutation::Put {
                key: "project/project:one".into(),
                payload: row.clone(),
            }],
            effects: vec![],
            result: json!({"version": 1, "ok": {"event": event, "project": row}}),
        })
        .await
        .unwrap();
    store.shutdown().await.unwrap();
    let reopened = fixture.connect().await;
    let before = reopened.snapshot().await.unwrap();
    let replay = execute_project_native(&reopened, request.clone())
        .await
        .unwrap();
    assert!(replay.replayed);
    assert_eq!(replay.receipt, original.receipt);
    assert!(
        replay.receipt.result["ok"]["project"]
            .get("hasCustomTitle")
            .is_none()
    );
    assert_eq!(reopened.snapshot().await.unwrap(), before);
    let mut conflicting = request;
    conflicting.command["autoPull"] = json!(false);
    assert!(matches!(
        execute_project_native(&reopened, conflicting).await,
        Err(Error::CommandConflict)
    ));
    assert_eq!(reopened.snapshot().await.unwrap(), before);
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
async fn history_corruption_cannot_guess_a_flag_or_commit_a_receipt() {
    for table in ["meta", "payload"] {
        let fixture = TestFixture::start().await;
        let store = fixture.initialize().await;
        seed(&store, legacy_row(), vec![title_event()]).await;
        let mut tx = store.db.begin().await.unwrap();
        tx.delete(crate::store_key(&format!("event/{table}/{:020}", 1)))
            .await
            .unwrap();
        tx.commit().await.unwrap();
        let before = store.snapshot().await.unwrap();
        assert!(matches!(
            execute_project_native(&store, update("must-not-commit")).await,
            Err(Error::IncompatibleStore)
        ));
        assert_eq!(store.snapshot().await.unwrap(), before);
        assert!(
            store
                .read_receipt("must-not-commit")
                .await
                .unwrap()
                .is_none()
        );
        store.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn tagged_rows_use_their_persisted_flag_without_reading_legacy_history() {
    for has_custom_title in [false, true] {
        let fixture = TestFixture::start().await;
        let store = fixture.initialize().await;
        let mut row = legacy_row();
        row["hasCustomTitle"] = json!(has_custom_title);
        seed(&store, row, vec![title_event()]).await;
        let mut tx = store.db.begin().await.unwrap();
        tx.delete(crate::store_key(&format!("event/payload/{:020}", 1)))
            .await
            .unwrap();
        tx.commit().await.unwrap();
        let updated = execute_project_native(&store, update("tagged"))
            .await
            .unwrap();
        assert_eq!(
            updated.receipt.result["ok"]["project"]["hasCustomTitle"],
            has_custom_title
        );
        store.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn valid_large_legacy_events_retry_with_a_bounded_single_record_page() {
    let fixture = TestFixture::start().await;
    let store = fixture.initialize().await;
    seed(&store, legacy_row(), vec![]).await;
    for revision in 1..=3 {
        let command_id = format!("history:{revision}");
        let payload = if revision < 3 {
            json!({"type": "provider.output", "text": "x".repeat(9 * 1024 * 1024)})
        } else {
            title_event()
        };
        store
            .commit(&CommitBatch {
                request_digest: request_digest(&json!({"command": command_id})),
                command_id,
                expected_revision: revision,
                events: vec![Record {
                    id: format!("event:{revision}"),
                    payload,
                }],
                projections: vec![],
                effects: vec![],
                result: Value::Null,
            })
            .await
            .unwrap();
    }
    let before = store.snapshot().await.unwrap();
    assert!(legacy_custom_title(&store, "project:one", 3).await.unwrap());
    assert_eq!(store.snapshot().await.unwrap(), before);
    store.shutdown().await.unwrap();
}
