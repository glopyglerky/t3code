#![cfg(unix)]

use std::io;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use serde_json::{Value, json};
use t3_code_ls_app::{
    auth::Token,
    http,
    runtime::{ProviderConfig, ProviderHub},
    service::Application,
};
use t3_code_ls_storage::{
    CommitBatch, ProjectionMutation, Record, Snapshot, Store, request_digest,
};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

fn provider(scenario: &str) -> ProviderConfig {
    ProviderConfig {
        program: PathBuf::from(env!("CARGO_BIN_EXE_t3-code-ls-provider-fixture")),
        args: vec![scenario.into()],
    }
}

fn project(workspace: &Path) -> Value {
    json!({"type":"project.create","commandId":"create-project","projectId":"project-1",
        "title":"Provider fixture","workspaceRoot":workspace})
}

fn thread(id: &str) -> Value {
    json!({"type":"thread.create","commandId":format!("create-{id}"),"threadId":id,
        "projectId":"project-1","title":id,"modelSelection":{"instanceId":"codex","model":"fixture-model"},
        "runtimeMode":"approval-required","interactionMode":"default","branch":null,"worktreePath":null})
}

fn request(thread: &str, command: &str, method: &str, params: Value) -> Value {
    json!({"threadId":thread,"commandId":command,"method":method,"params":params})
}

fn projection<'a>(snapshot: &'a Snapshot, key: &str) -> &'a Value {
    &snapshot
        .projections
        .iter()
        .find(|row| row.key == key)
        .unwrap()
        .payload
}

fn fixture_messages(workspace: &Path) -> Vec<Value> {
    match std::fs::read_to_string(workspace.join("fixture.messages.jsonl")) {
        Ok(contents) => contents
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).unwrap()["message"].clone())
            .collect(),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Vec::new(),
        Err(error) => panic!("cannot read controlled fixture log: {error}"),
    }
}

struct Server {
    address: SocketAddr,
    token: Token,
    application: Arc<Application>,
    store: Arc<Store>,
    stop: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<io::Result<()>>>,
    workspace: PathBuf,
    directory: tempfile::TempDir,
}

impl Server {
    async fn start(scenario: &str) -> Self {
        let directory = tempfile::tempdir().unwrap();
        let workspace = directory.path().join("workspace");
        std::fs::create_dir(&workspace).unwrap();
        let store = Arc::new(
            Store::initialize_local(directory.path().join("state.redb"))
                .await
                .unwrap(),
        );
        let hub = ProviderHub::new(store.clone(), provider(scenario));
        hub.recover().await.unwrap();
        let application = Arc::new(Application::new(store.clone()).with_providers(hub));
        let token = Token::create(&directory.path().join("token")).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (stop, stopped) = oneshot::channel();
        let serving = application.clone();
        let credentials = token.clone();
        let task = tokio::spawn(async move {
            http::serve(listener, serving, credentials, async {
                let _ = stopped.await;
            })
            .await
        });
        let server = Self {
            address,
            token,
            application,
            store,
            stop: Some(stop),
            task: Some(task),
            workspace,
            directory,
        };
        server.command(project(&server.workspace)).await;
        server.command(thread("thread-1")).await;
        server
    }

    async fn raw(&self, method: &str, params: Value) -> Value {
        tokio::time::timeout(
            Duration::from_secs(20),
            http::request(
                self.address,
                &self.token,
                json!({"jsonrpc":"2.0","id":"provider-http-test","method":method,"params":params}),
            ),
        )
        .await
        .expect("controlled HTTP operation did not reach its protocol milestone")
        .unwrap()
    }

    async fn call(&self, method: &str, params: Value) -> Value {
        let response = self.raw(method, params).await;
        assert!(response.get("error").is_none(), "{response}");
        assert_ne!(response["result"]["pending"], true, "{response}");
        response["result"].clone()
    }

    async fn command(&self, command: Value) -> Value {
        let result = self
            .call("command.execute", json!({"command":command}))
            .await;
        assert!(
            result["receipt"]["result"].get("error").is_none(),
            "{result}"
        );
        result
    }

    async fn snapshot(&self) -> Snapshot {
        serde_json::from_value(self.call("state.snapshot", json!({})).await).unwrap()
    }

    async fn start_native_thread(&self, command: &str) -> Value {
        let accepted = self
            .call(
                "session.request",
                request("thread-1", command, "thread/start", json!({})),
            )
            .await;
        let completed = self
            .call("session.request.wait", json!({"commandId":command}))
            .await;
        assert_eq!(completed["completion"]["result"]["state"], "responded");
        assert_eq!(completed["receipt"], accepted["receipt"]);
        assert!(completed["unresolved"].is_null());
        accepted
    }

    async fn shutdown(mut self) {
        self.application.shutdown().await.unwrap();
        self.stop.take().unwrap().send(()).unwrap();
        self.task.take().unwrap().await.unwrap().unwrap();
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_provider_turn_approval_events_and_close_archive_are_durable() {
    let server = Server::start("happy").await;
    let opened = server
        .call("session.open", json!({"threadId":"thread-1"}))
        .await;
    assert_eq!(opened["status"], "open");
    assert_eq!(opened["processState"], "running");
    let accepted = server.start_native_thread("start-native").await;
    let replay = server
        .call(
            "session.request",
            request("thread-1", "start-native", "thread/start", json!({})),
        )
        .await;
    assert_eq!(replay["replayed"], true);
    assert_eq!(replay["receipt"], accepted["receipt"]);

    let turn = request(
        "thread-1",
        "turn-once",
        "turn/start",
        json!({"input":[{"type":"text","text":"controlled turn"}]}),
    );
    let accepted_turn = server.call("session.request", turn.clone()).await;
    let completed_turn = server
        .call("session.request.wait", json!({"commandId":"turn-once"}))
        .await;
    assert_eq!(completed_turn["completion"]["result"]["state"], "responded");
    let approval = server
        .call("session.approval.wait", json!({"threadId":"thread-1"}))
        .await;
    assert_eq!(approval["requestId"], 100);
    assert_eq!(approval["method"], "item/commandExecution/requestApproval");
    assert_eq!(approval["sessionId"], opened["sessionId"]);
    let before = server.snapshot().await;
    assert_eq!(
        projection(&before, "native/session/thread-1")["activeTurnId"],
        "native-turn"
    );
    let before_events = server.call("events.list", json!({"limit":1000})).await;
    assert!(
        before_events
            .as_array()
            .unwrap()
            .iter()
            .any(|event| event["payload"]["payload"]["kind"] == "server_request")
    );
    assert!(
        !before_events
            .as_array()
            .unwrap()
            .iter()
            .any(|event| event["payload"]["payload"]["params"]["delta"] == "After approval.")
    );
    assert!(
        !fixture_messages(&server.workspace)
            .iter()
            .any(|message| message.get("method").is_none() && message["id"] == 100)
    );

    let repeated = server.call("session.request", turn).await;
    assert_eq!(repeated["replayed"], true);
    assert_eq!(repeated["receipt"], accepted_turn["receipt"]);
    let conflicting = server
        .raw(
            "session.request",
            request(
                "thread-1",
                "turn-once",
                "turn/start",
                json!({"input":[{"type":"text","text":"different turn"}]}),
            ),
        )
        .await;
    assert!(conflicting.get("error").is_some());
    let unknown = server.raw("session.respond", json!({"threadId":"thread-1","commandId":"unknown-answer","requestId":999,"result":{"decision":"accept"}})).await;
    assert!(unknown.get("error").is_some());
    assert!(
        server
            .store
            .read_receipt("unknown-answer")
            .await
            .unwrap()
            .is_none()
    );

    server.command(thread("thread-2")).await;
    server
        .call("session.open", json!({"threadId":"thread-2"}))
        .await;
    let cross_thread = server.raw("session.respond", json!({"threadId":"thread-2","commandId":"wrong-thread-answer","requestId":100,"result":{"decision":"accept"}})).await;
    assert!(cross_thread.get("error").is_some());
    assert!(
        server
            .store
            .read_receipt("wrong-thread-answer")
            .await
            .unwrap()
            .is_none()
    );

    let answer_params = json!({"threadId":"thread-1","commandId":"approve-once","requestId":100,"result":{"decision":"accept"}});
    let answer = server.call("session.respond", answer_params.clone()).await;
    assert_eq!(answer["completion"]["result"]["state"], "submitted");
    let idle = server
        .call("session.idle.wait", json!({"threadId":"thread-1"}))
        .await;
    assert_eq!(idle["loaded"], true);
    assert!(idle["activeTurnId"].is_null());
    let answer_replay = server.call("session.respond", answer_params).await;
    assert_eq!(answer_replay["replayed"], true);
    assert_eq!(answer_replay["receipt"], answer["receipt"]);
    let stale = server.raw("session.respond", json!({"threadId":"thread-1","commandId":"stale-answer","requestId":100,"result":{"decision":"accept"}})).await;
    assert!(stale.get("error").is_some());

    let current = server.snapshot().await;
    assert!(!current.projections.iter().any(
        |row| row.key.starts_with("native/request/") || row.key.starts_with("native/approval/")
    ));
    let all_events = server.call("events.list", json!({"limit":1000})).await;
    let events = all_events.as_array().unwrap();
    assert!(
        events
            .iter()
            .any(|event| event["payload"]["payload"]["params"]["delta"] == "After approval.")
    );
    let intent = events
        .iter()
        .find(|event| {
            event["payload"]["type"] == "native.provider.intent"
                && event["payload"]["commandId"] == "turn-once"
        })
        .unwrap();
    let result = events
        .iter()
        .find(|event| {
            event["payload"]["type"] == "native.provider.event"
                && event["payload"]["commandId"] == "turn-once"
        })
        .unwrap();
    assert!(intent["sequence"].as_u64().unwrap() < result["sequence"].as_u64().unwrap());
    let cursor = result["sequence"].as_u64().unwrap();
    let tail = server
        .call("events.list", json!({"afterSequence":cursor,"limit":1000}))
        .await;
    assert!(
        tail.as_array()
            .unwrap()
            .iter()
            .all(|event| event["sequence"].as_u64().unwrap() > cursor)
    );

    for id in ["thread-1", "thread-2"] {
        let closed = server.call("session.close", json!({"threadId":id})).await;
        assert_eq!(closed["status"], "closed");
        assert_eq!(closed["processState"], "stopped");
        assert_eq!(closed["loaded"], false);
        assert!(closed["activeTurnId"].is_null());
    }
    let archived = server.command(json!({"type":"thread.archive","commandId":"archive-after-close","threadId":"thread-1"})).await;
    assert!(archived["receipt"]["result"]["ok"]["thread"]["archivedAt"].is_string());
    let effects = server
        .store
        .outbox_list_by_command("archive-after-close")
        .await
        .unwrap();
    assert_eq!(effects.len(), 1);
    assert_eq!(effects[0].request, json!({"type":"terminal.cleanup"}));
    assert_eq!(
        effects[0].status,
        t3_code_ls_storage::outbox::EffectStatus::Pending
    );
    let messages = fixture_messages(&server.workspace);
    assert_eq!(
        messages
            .iter()
            .filter(|message| message["method"] == "thread/start")
            .count(),
        1
    );
    assert_eq!(
        messages
            .iter()
            .filter(|message| message["method"] == "turn/start")
            .count(),
        1
    );
    assert_eq!(
        messages
            .iter()
            .filter(|message| message.get("method").is_none() && message["id"] == 100)
            .count(),
        1
    );
    let start = messages
        .iter()
        .find(|message| message["method"] == "thread/start")
        .unwrap();
    assert_eq!(
        start["params"]["cwd"],
        json!(server.workspace.canonicalize().unwrap())
    );
    assert_eq!(start["params"]["approvalPolicy"], "untrusted");
    let start_turn = messages
        .iter()
        .find(|message| message["method"] == "turn/start")
        .unwrap();
    assert_eq!(start_turn["params"]["threadId"], "native-thread");
    assert_eq!(start_turn["params"]["sandboxPolicy"]["type"], "readOnly");
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_resume_is_bound_to_the_original_workspace_and_reserved_ids_cannot_collide() {
    let server = Server::start("happy").await;
    server
        .call("session.open", json!({"threadId":"thread-1"}))
        .await;
    server.start_native_thread("start-native").await;
    server
        .call("session.close", json!({"threadId":"thread-1"}))
        .await;
    let reopened = server
        .call("session.open", json!({"threadId":"thread-1"}))
        .await;
    assert_eq!(reopened["nativeThreadId"], "native-thread");
    assert_eq!(reopened["loaded"], false);
    server
        .call(
            "session.request",
            request("thread-1", "resume-native", "thread/resume", json!({})),
        )
        .await;
    let resumed = server
        .call("session.request.wait", json!({"commandId":"resume-native"}))
        .await;
    assert_eq!(resumed["completion"]["result"]["state"], "responded");
    for params in [
        request(
            "thread-1",
            "native:result:forged",
            "thread/start",
            json!({}),
        ),
        request(
            "thread-1",
            "forged-workspace",
            "thread/start",
            json!({"cwd":"/outside"}),
        ),
        request("thread-1", "arbitrary-method", "command/exec", json!({})),
    ] {
        let command = params["commandId"].as_str().unwrap().to_owned();
        assert!(
            server
                .raw("session.request", params)
                .await
                .get("error")
                .is_some()
        );
        assert!(server.store.read_receipt(&command).await.unwrap().is_none());
    }
    assert!(server.raw("session.respond", json!({"threadId":"thread-1","commandId":"native:result:forged-answer","requestId":100,"result":{"decision":"accept"}})).await.get("error").is_some());
    assert!(
        server
            .store
            .read_receipt("native:result:forged-answer")
            .await
            .unwrap()
            .is_none()
    );
    server
        .call("session.close", json!({"threadId":"thread-1"}))
        .await;
    let other_workspace = server.directory.path().join("moved-worktree");
    std::fs::create_dir(&other_workspace).unwrap();
    server.command(json!({"type":"thread.metadata.update","commandId":"move-worktree","threadId":"thread-1","expectedWorktreePath":null,"worktreePath":other_workspace})).await;
    let refused = server
        .raw("session.open", json!({"threadId":"thread-1"}))
        .await;
    assert!(
        refused["error"]["message"]
            .as_str()
            .unwrap()
            .contains("another workspace or model"),
        "{refused}"
    );
    assert!(!other_workspace.join("fixture.pid").exists());
    let messages = fixture_messages(&server.workspace);
    assert_eq!(
        messages
            .iter()
            .filter(|message| message["method"] == "initialize")
            .count(),
        2
    );
    assert_eq!(
        messages
            .iter()
            .filter(|message| message["method"] == "thread/resume")
            .count(),
        1
    );
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn provider_rejections_complete_a_receipt_and_allow_a_new_request() {
    let server = Server::start("reject-first-thread").await;
    server
        .call("session.open", json!({"threadId":"thread-1"}))
        .await;
    server
        .call(
            "session.request",
            request("thread-1", "rejected", "thread/start", json!({})),
        )
        .await;
    let rejected = server
        .call("session.request.wait", json!({"commandId":"rejected"}))
        .await;
    assert_eq!(rejected["completion"]["result"]["state"], "rejected");
    assert!(rejected["unresolved"].is_null());
    server.start_native_thread("accepted-next").await;
    let events = server.call("events.list", json!({"limit":1000})).await;
    assert!(
        events
            .as_array()
            .unwrap()
            .iter()
            .any(
                |event| event["payload"]["payload"]["kind"] == "request_error"
                    && event["payload"]["payload"]["error"]["code"] == -32001
            )
    );
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn abrupt_provider_exit_keeps_the_request_uncertain_and_never_replays_it() {
    let server = Server::start("exit-on-thread").await;
    server
        .call("session.open", json!({"threadId":"thread-1"}))
        .await;
    let params = request("thread-1", "unknown-outcome", "thread/start", json!({}));
    let accepted = server.call("session.request", params.clone()).await;
    let failed = server
        .call(
            "session.request.wait",
            json!({"commandId":"unknown-outcome"}),
        )
        .await;
    assert_eq!(failed["unresolved"]["state"], "uncertain");
    let closed = server
        .call("session.close", json!({"threadId":"thread-1"}))
        .await;
    assert_eq!(closed["processState"], "stopped");
    assert_eq!(closed["uncertain"], true);
    let replay = server.call("session.request", params).await;
    assert_eq!(replay["replayed"], true);
    assert_eq!(replay["receipt"], accepted["receipt"]);
    assert_eq!(replay["unresolved"]["state"], "uncertain");
    assert!(
        server
            .raw("session.open", json!({"threadId":"thread-1"}))
            .await
            .get("error")
            .is_some()
    );
    assert!(server.raw("command.execute", json!({"command":{"type":"thread.archive","commandId":"unsafe-archive","threadId":"thread-1"}})).await.get("error").is_some());
    assert_eq!(
        fixture_messages(&server.workspace)
            .iter()
            .filter(|message| message["method"] == "thread/start")
            .count(),
        1
    );
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn restart_fences_unknown_processes_and_pending_intents_without_starting_a_provider() {
    let directory = tempfile::tempdir().unwrap();
    let workspace = directory.path().join("workspace");
    std::fs::create_dir(&workspace).unwrap();
    let store = Arc::new(
        Store::initialize_local(directory.path().join("state.redb"))
            .await
            .unwrap(),
    );
    let application = Application::new(store.clone());
    for command in [project(&workspace), thread("thread-1")] {
        let response = application.handle(json!({"jsonrpc":"2.0","id":1,"method":"command.execute","params":{"command":command}})).await;
        assert!(response.get("error").is_none(), "{response}");
    }
    drop(application);
    let digest = request_digest(
        &json!({"protocol":"native-provider-request-v1","threadId":"thread-1","method":"thread/start","params":{}}),
    );
    let snapshot = store.snapshot().await.unwrap();
    let crash = store.commit(&CommitBatch {
        command_id:"interrupted-before-reply".into(), request_digest:digest, expected_revision:snapshot.revision,
        events:vec![Record {id:"crash-intent".into(),payload:json!({"type":"native.provider.intent","threadId":"thread-1","sessionId":"lost-session"})}],
        projections:vec![
            ProjectionMutation::Put {key:"native/session/thread-1".into(),payload:json!({"threadId":"thread-1","sessionId":"lost-session","status":"open","processState":"running","processIdentity":null,"loaded":true,"activeTurnId":"lost-turn","nativeThreadId":null})},
            ProjectionMutation::Put {key:"native/request/interrupted-before-reply".into(),payload:json!({"threadId":"thread-1","sessionId":"lost-session","commandId":"interrupted-before-reply","method":"thread/start","state":"sent","rpcId":2})},
            ProjectionMutation::Put {key:"native/approval/lost-session/old".into(),payload:json!({"threadId":"thread-1","sessionId":"lost-session","state":"pending","requestId":100})},
        ], effects:vec![], result:json!({"accepted":true,"threadId":"thread-1","sessionId":"lost-session"}),
    }).await.unwrap();
    let hub = ProviderHub::new(store.clone(), provider("happy"));
    let recovered = hub.recover().await.unwrap();
    assert_eq!(recovered["closedSessions"], 1);
    assert_eq!(recovered["uncertainRequests"], 1);
    let after = store.snapshot().await.unwrap();
    let session = projection(&after, "native/session/thread-1");
    assert_eq!(session["status"], "closed");
    assert_eq!(session["processState"], "unknown");
    assert_eq!(session["loaded"], false);
    assert!(session["activeTurnId"].is_null());
    assert_eq!(
        projection(&after, "native/request/interrupted-before-reply")["state"],
        "uncertain"
    );
    assert!(
        !after
            .projections
            .iter()
            .any(|row| row.key.starts_with("native/approval/"))
    );
    let replay = hub
        .request(
            "thread-1",
            "interrupted-before-reply",
            "thread/start",
            json!({}),
        )
        .await
        .unwrap();
    assert_eq!(replay["replayed"], true);
    assert_eq!(
        replay["receipt"],
        serde_json::to_value(crash.receipt).unwrap()
    );
    assert_eq!(
        hub.wait_for_request("interrupted-before-reply")
            .await
            .unwrap()["unresolved"]["state"],
        "uncertain"
    );
    assert!(hub.open("thread-1").await.is_err());
    assert!(!workspace.join("fixture.pid").exists());
    assert!(fixture_messages(&workspace).is_empty());
    let application = Application::new(store.clone()).with_providers(hub);
    let archive = application.handle(json!({"jsonrpc":"2.0","id":1,"method":"command.execute","params":{"command":{"type":"thread.archive","commandId":"archive-unknown","threadId":"thread-1"}}})).await;
    assert!(archive.get("error").is_some());
    assert!(
        store
            .outbox_list_by_command("archive-unknown")
            .await
            .unwrap()
            .is_empty()
    );
    application.shutdown().await.unwrap();
}
