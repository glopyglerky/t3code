use std::sync::Arc;

use serde_json::{Value, json};
use t3_code_ls_app::service::Application;
use t3_code_ls_storage::outbox::{EffectStatus, OutboxEffect};
use t3_code_ls_storage::{CommitOutcome, Receipt, Snapshot, Store, StoredEvent};

async fn rpc(application: &Application, method: &str, params: Value) -> Value {
    let response = application
        .handle(json!({
            "jsonrpc":"2.0", "id":"test-request", "method":method, "params":params,
        }))
        .await;
    assert_eq!(response["jsonrpc"], "2.0");
    assert_eq!(response["id"], "test-request");
    response
}

async fn successful(application: &Application, method: &str, params: Value) -> Value {
    let response = rpc(application, method, params).await;
    assert!(response.get("error").is_none(), "{response}");
    response["result"].clone()
}

async fn execute(application: &Application, command: Value) -> CommitOutcome {
    serde_json::from_value(
        successful(application, "command.execute", json!({"command":command})).await,
    )
    .unwrap()
}

async fn snapshot(application: &Application) -> Snapshot {
    serde_json::from_value(successful(application, "state.snapshot", json!({})).await).unwrap()
}

async fn events(application: &Application, after: u64, limit: u32) -> Vec<StoredEvent> {
    serde_json::from_value(
        successful(
            application,
            "events.list",
            json!({"afterSequence":after,"limit":limit}),
        )
        .await,
    )
    .unwrap()
}

async fn receipt(application: &Application, command_id: &str) -> Option<Receipt> {
    serde_json::from_value(
        successful(application, "receipt.get", json!({"commandId":command_id})).await,
    )
    .unwrap()
}

fn project(workspace: &str) -> Value {
    json!({
        "type":"project.create", "commandId":"project-create", "projectId":"project-1",
        "title":" Project ", "workspaceRoot":workspace,
    })
}

fn thread(command_id: &str) -> Value {
    json!({
        "type":"thread.create", "commandId":command_id, "threadId":"thread-1",
        "projectId":"project-1", "title":"Thread", "modelSelection": {
            "instanceId":"codex", "model":"test-model",
        },
        "runtimeMode":"approval-required", "interactionMode":"default", "branch":null,
        "worktreePath":null,
    })
}

fn thread_command(kind: &str, command_id: &str) -> Value {
    json!({"type":kind, "commandId":command_id, "threadId":"thread-1"})
}

#[tokio::test]
async fn application_commands_events_receipts_and_cleanup_survive_server_restart() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("state.redb");
    let workspace = directory.path().join("project");
    std::fs::create_dir(&workspace).unwrap();
    let store = Arc::new(Store::initialize_local(&path).await.unwrap());
    let application = Application::new(store.clone());
    let capabilities = successful(&application, "application.capabilities", json!({})).await;
    assert_eq!(capabilities["storage"], "redb");
    assert_eq!(capabilities["upstreamClientCompatible"], false);
    assert_eq!(capabilities["codexSessions"], false);
    assert!(
        capabilities["commands"]
            .as_array()
            .unwrap()
            .contains(&json!("thread.archive"))
    );

    let created_project = execute(&application, project(workspace.to_str().unwrap())).await;
    assert_eq!(
        created_project.receipt.result["ok"]["project"]["title"],
        "Project"
    );
    let mut create_thread = thread("thread-create");
    create_thread["createdBy"] = json!("system");
    create_thread["creationSource"] = json!("provider");
    let created_thread = execute(&application, create_thread.clone()).await;
    let created_row = &created_thread.receipt.result["ok"]["thread"];
    assert_eq!(created_row["createdBy"], "user");
    assert_eq!(created_row["creationSource"], "server");
    assert_eq!(created_row["id"], "thread-1");
    assert_eq!(created_row["projectId"], "project-1");

    let mut metadata = thread_command("thread.metadata.update", "metadata");
    metadata["title"] = json!("Renamed");
    metadata["branch"] = json!("feature");
    metadata["expectedBranch"] = Value::Null;
    execute(&application, metadata).await;
    let mut pin = thread_command("thread.pin", "pin");
    pin["orderKey"] = json!("first");
    execute(&application, pin).await;
    let archive_command = thread_command("thread.archive", "archive");
    let archived = execute(&application, archive_command.clone()).await;
    let archived_row = archived.receipt.result["ok"]["thread"].clone();
    assert_eq!(archived_row["title"], "Renamed");
    assert_eq!(archived_row["branch"], "feature");
    assert!(archived_row["archivedAt"].is_string());
    assert!(archived_row["pinnedAt"].is_string());
    assert_eq!(archived_row["createdAt"], created_row["createdAt"]);

    let first_page = events(&application, 0, 2).await;
    let second_page = events(&application, first_page.last().unwrap().sequence, 2).await;
    let third_page = events(&application, second_page.last().unwrap().sequence, 2).await;
    assert_eq!(
        first_page
            .iter()
            .map(|event| event.sequence)
            .collect::<Vec<_>>(),
        [1, 2]
    );
    assert_eq!(
        second_page
            .iter()
            .map(|event| event.sequence)
            .collect::<Vec<_>>(),
        [3, 4]
    );
    assert_eq!(
        third_page
            .iter()
            .map(|event| event.sequence)
            .collect::<Vec<_>>(),
        [5]
    );
    assert_eq!(third_page[0].payload["type"], "thread.archived");
    assert_eq!(third_page[0].payload["payload"], archived_row);
    assert!(
        events(&application, third_page[0].sequence, 2)
            .await
            .is_empty()
    );
    assert_eq!(
        receipt(&application, "archive").await,
        Some(archived.receipt.clone())
    );

    let effect_id = "effect:archive:terminal.cleanup";
    let effect: OutboxEffect = serde_json::from_value(
        successful(&application, "outbox.get", json!({"effectId":effect_id})).await,
    )
    .unwrap();
    assert_eq!(effect.thread_id, "thread-1");
    assert_eq!(effect.request, json!({"type":"terminal.cleanup"}));
    assert_eq!(effect.status, EffectStatus::Pending);
    assert_eq!(
        effect.created_at,
        archived_row["archivedAt"].as_str().unwrap()
    );
    let before_restart = snapshot(&application).await;
    application.shutdown().await.unwrap();
    drop(application);
    Arc::try_unwrap(store)
        .unwrap_or_else(|_| panic!("application retained the store after shutdown"))
        .shutdown()
        .await
        .unwrap();

    let store = Arc::new(Store::open_local(&path).await.unwrap());
    let application = Application::new(store.clone());
    assert_eq!(snapshot(&application).await, before_restart);
    let replay = execute(&application, archive_command).await;
    assert!(replay.replayed);
    assert_eq!(replay.receipt, archived.receipt);
    let create_replay = execute(&application, create_thread).await;
    assert!(create_replay.replayed);
    assert_eq!(create_replay.receipt, created_thread.receipt);
    assert_eq!(events(&application, 0, 100).await.len(), 5);
    let after_effect: OutboxEffect = serde_json::from_value(
        successful(&application, "outbox.get", json!({"effectId":effect_id})).await,
    )
    .unwrap();
    assert_eq!(after_effect, effect);
    execute(
        &application,
        thread_command("thread.unarchive", "unarchive"),
    )
    .await;
    let current = snapshot(&application).await;
    let row = current
        .projections
        .iter()
        .find(|row| row.key == "thread/thread-1")
        .unwrap();
    assert_eq!(row.payload["archivedAt"], Value::Null);
    assert_eq!(row.payload["title"], "Renamed");
    assert_eq!(current.last_sequence, 6);
    application.shutdown().await.unwrap();
    drop(application);
    Arc::try_unwrap(store)
        .unwrap_or_else(|_| panic!("application retained the reopened store"))
        .shutdown()
        .await
        .unwrap();
}

#[tokio::test]
async fn invalid_commands_and_changed_command_ids_leave_no_partial_thread_mutations() {
    let directory = tempfile::tempdir().unwrap();
    let store = Arc::new(
        Store::initialize_local(directory.path().join("state.redb"))
            .await
            .unwrap(),
    );
    let application = Application::new(store);
    execute(&application, project("/workspace/project")).await;
    execute(&application, thread("thread-create")).await;
    let mut rename = thread_command("thread.metadata.update", "rename");
    rename["title"] = json!("Accepted title");
    let accepted = execute(&application, rename.clone()).await;
    let before = snapshot(&application).await;
    rename["title"] = json!("Conflicting title");
    let conflict = rpc(&application, "command.execute", json!({"command":rename})).await;
    assert_eq!(conflict["error"]["data"]["kind"], "command_conflict");
    assert_eq!(
        receipt(&application, "rename").await,
        Some(accepted.receipt)
    );
    assert_eq!(snapshot(&application).await, before);

    let mut stale = thread_command("thread.metadata.update", "stale-worktree");
    stale["expectedWorktreePath"] = json!("/not-the-current-worktree");
    stale["title"] = json!("Must not land");
    let mut unsupported = thread_command("thread.metadata.update", "title-generation");
    unsupported["regenerateTitle"] = json!(true);
    let mut forged = thread("linked-create");
    forged["threadId"] = json!("thread-2");
    forged["linkOrigin"] = json!({"sessionId":"forged", "label":"forged"});
    let mut unknown_project = thread("unknown-project");
    unknown_project["threadId"] = json!("thread-2");
    unknown_project["projectId"] = json!("absent");
    for invalid in [
        stale,
        unsupported,
        forged,
        unknown_project,
        thread_command("thread.delete", "delete"),
    ] {
        let id = invalid["commandId"].as_str().unwrap().to_owned();
        let response = rpc(&application, "command.execute", json!({"command":invalid})).await;
        assert_eq!(
            response["error"]["data"]["kind"], "invalid_request",
            "{response}"
        );
        assert!(receipt(&application, &id).await.is_none());
        assert_eq!(snapshot(&application).await, before);
    }
}

#[tokio::test]
async fn reserved_native_command_ids_cannot_bypass_the_guard_with_ecmascript_whitespace() {
    let directory = tempfile::tempdir().unwrap();
    let store = Arc::new(
        Store::initialize_local(directory.path().join("state.redb"))
            .await
            .unwrap(),
    );
    let application = Application::new(store);
    execute(&application, project("/workspace/project")).await;
    execute(&application, thread("thread-create")).await;
    let before = snapshot(&application).await;
    let before_events = events(&application, 0, 100).await;

    let mut create_project = project("/workspace/other-project");
    create_project["projectId"] = json!("project-2");
    let update_project = json!({
        "type":"project.meta.update", "projectId":"project-1", "title":"Must not land",
    });
    let mut create_thread = thread("replaced-below");
    create_thread["threadId"] = json!("thread-2");
    let mut update_thread = thread_command("thread.metadata.update", "replaced-below");
    update_thread["title"] = json!("Must not land");
    for command in [create_project, update_project, create_thread, update_thread] {
        for command_id in [
            "native:result:future-provider-request",
            " native:result:future-provider-request ",
            "\u{feff}native:result:future-provider-request\u{feff}",
            " \u{feff}\u{00a0}native:result:future-provider-request\u{3000} ",
        ] {
            let mut command = command.clone();
            command["commandId"] = json!(command_id);
            let response = rpc(&application, "command.execute", json!({"command":command})).await;
            assert_eq!(response["error"]["code"], -32602, "{response}");
            assert!(response.get("result").is_none());
            assert_eq!(snapshot(&application).await, before);
            assert_eq!(events(&application, 0, 100).await, before_events);
            assert!(receipt(&application, command_id).await.is_none());
            assert!(
                receipt(&application, "native:result:future-provider-request")
                    .await
                    .is_none()
            );
        }
    }

    let mut allowed = thread_command(
        "thread.metadata.update",
        " \u{feff}ordinary-command\u{00a0} ",
    );
    allowed["title"] = json!("Accepted title");
    let accepted = execute(&application, allowed).await;
    assert_eq!(accepted.receipt.command_id, "ordinary-command");
    assert_eq!(
        accepted.receipt.result["ok"]["thread"]["title"],
        "Accepted title"
    );
    assert_eq!(
        snapshot(&application).await.last_sequence,
        before.last_sequence + 1
    );
}

#[tokio::test]
async fn project_workspace_rejections_are_durable_and_do_not_create_extra_projects_or_events() {
    let directory = tempfile::tempdir().unwrap();
    let store = Arc::new(
        Store::initialize_local(directory.path().join("state.redb"))
            .await
            .unwrap(),
    );
    let application = Application::new(store);
    execute(&application, project("/workspace/project")).await;
    let before = snapshot(&application).await;
    let mut conflicting = project("/workspace/project");
    conflicting["commandId"] = json!("project-conflict");
    conflicting["projectId"] = json!("project-2");
    let rejected = execute(&application, conflicting.clone()).await;
    assert!(rejected.receipt.result.get("error").is_some());
    assert!(rejected.receipt.result.get("ok").is_none());
    assert_eq!(rejected.receipt.first_sequence, None);
    let after = snapshot(&application).await;
    assert_eq!(after.projections, before.projections);
    assert_eq!(after.last_sequence, before.last_sequence);
    assert_eq!(after.revision, before.revision + 1);
    let retry = execute(&application, conflicting).await;
    assert!(retry.replayed);
    assert_eq!(retry.receipt, rejected.receipt);
    assert_eq!(snapshot(&application).await, after);
    assert_eq!(events(&application, 0, 100).await.len(), 1);
}

#[tokio::test]
async fn two_application_instances_replay_one_command_without_duplicate_events() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("state.redb");
    let first = Application::new(Arc::new(Store::initialize_local(&path).await.unwrap()));
    let second = Application::new(Arc::new(Store::open_local(&path).await.unwrap()));
    execute(&first, project("/workspace/project")).await;
    let (a, b) = tokio::join!(
        execute(&first, thread("create-once")),
        execute(&second, thread("create-once"))
    );
    assert_eq!(a.receipt, b.receipt);
    assert_ne!(a.replayed, b.replayed);
    assert_eq!(events(&first, 0, 100).await.len(), 2);
    assert_eq!(snapshot(&first).await, snapshot(&second).await);
    assert_eq!(receipt(&second, "create-once").await, Some(a.receipt));
}

#[tokio::test]
async fn invalid_rpc_envelopes_and_unconfigured_provider_methods_cannot_mutate_storage() {
    let directory = tempfile::tempdir().unwrap();
    let store = Arc::new(
        Store::initialize_local(directory.path().join("state.redb"))
            .await
            .unwrap(),
    );
    let application = Application::new(store);
    let before = snapshot(&application).await;
    for envelope in [
        Value::Null,
        json!({"jsonrpc":"1.0","id":1,"method":"state.snapshot"}),
        json!({"jsonrpc":"2.0","id":null,"method":"state.snapshot"}),
        json!({"jsonrpc":"2.0","id":{},"method":"state.snapshot"}),
        json!({"jsonrpc":"2.0","id":1,"method":"command.execute","params":[]}),
        json!({"jsonrpc":"2.0","id":1,"method":"state.snapshot","extra":true}),
    ] {
        let response = application.handle(envelope).await;
        assert_eq!(response["error"]["code"], -32600);
    }
    for (method, params, expected_code) in [
        ("state.snapshot", json!({"unexpected":true}), -32602),
        ("events.list", json!({"afterSequence":0,"limit":0}), -32001),
        ("events.list", json!({"afterSequence":-1}), -32602),
        ("command.execute", json!({"command":{}}), -32602),
        ("session.open", json!({"threadId":"thread-1"}), -32601),
        ("not.implemented", json!({}), -32601),
    ] {
        let response = rpc(&application, method, params).await;
        assert_eq!(response["error"]["code"], expected_code, "{response}");
    }
    assert_eq!(snapshot(&application).await, before);
    assert!(events(&application, 0, 100).await.is_empty());
}
