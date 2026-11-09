use std::io;
use std::net::SocketAddr;
use std::sync::Arc;

use serde_json::{Value, json};
use t3_code_ls_app::{auth::Token, http, service::Application};
use t3_code_ls_storage::{CommitOutcome, Snapshot, Store};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

struct TestServer {
    address: SocketAddr,
    token: Token,
    application: Arc<Application>,
    stop: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<io::Result<()>>>,
    _directory: tempfile::TempDir,
}

impl TestServer {
    async fn start() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let token = Token::create(&directory.path().join("api-token")).unwrap();
        let store = Arc::new(
            Store::initialize_local(directory.path().join("state.redb"))
                .await
                .unwrap(),
        );
        let application = Arc::new(Application::new(store));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (stop, stopped) = oneshot::channel();
        let server_application = application.clone();
        let server_token = token.clone();
        let task = tokio::spawn(async move {
            http::serve(listener, server_application, server_token, async {
                let _ = stopped.await;
            })
            .await
        });
        Self {
            address,
            token,
            application,
            stop: Some(stop),
            task: Some(task),
            _directory: directory,
        }
    }

    async fn call(&self, method: &str, params: Value) -> Value {
        let response = http::request(
            self.address,
            &self.token,
            json!({
                "jsonrpc":"2.0", "id":"over-http", "method":method, "params":params,
            }),
        )
        .await
        .unwrap();
        assert_eq!(response["jsonrpc"], "2.0");
        assert_eq!(response["id"], "over-http");
        assert!(response.get("error").is_none(), "{response}");
        response["result"].clone()
    }

    async fn snapshot(&self) -> Snapshot {
        serde_json::from_value(self.call("state.snapshot", json!({})).await).unwrap()
    }

    async fn shutdown(mut self) {
        self.stop.take().unwrap().send(()).unwrap();
        self.task.take().unwrap().await.unwrap().unwrap();
        self.application.shutdown().await.unwrap();
        assert!(
            TcpStream::connect(self.address).await.is_err(),
            "listener remained open after shutdown"
        );
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

struct RawResponse {
    status: u16,
    body: Value,
}

async fn raw_request(
    server: &TestServer,
    authorizations: &[String],
    extras: &[(&str, &str)],
    body: &[u8],
) -> RawResponse {
    let mut headers = format!(
        "POST /rpc HTTP/1.1\r\nHost: {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n",
        server.address,
        body.len(),
    );
    for authorization in authorizations {
        headers.push_str("Authorization: ");
        headers.push_str(authorization);
        headers.push_str("\r\n");
    }
    for (name, value) in extras {
        headers.push_str(name);
        headers.push_str(": ");
        headers.push_str(value);
        headers.push_str("\r\n");
    }
    headers.push_str("\r\n");
    let mut stream = TcpStream::connect(server.address).await.unwrap();
    stream.write_all(headers.as_bytes()).await.unwrap();
    stream.write_all(body).await.unwrap();
    stream.flush().await.unwrap();
    let mut bytes = Vec::new();
    stream
        .take(512 * 1024)
        .read_to_end(&mut bytes)
        .await
        .unwrap();
    let response = String::from_utf8(bytes).unwrap();
    let (head, body) = response
        .split_once("\r\n\r\n")
        .expect("HTTP response headers");
    let status = head
        .lines()
        .next()
        .unwrap()
        .split_whitespace()
        .nth(1)
        .unwrap()
        .parse()
        .unwrap();
    RawResponse {
        status,
        body: serde_json::from_str(body).unwrap(),
    }
}

fn project_command() -> Value {
    json!({
        "type":"project.create", "commandId":"project-create", "projectId":"project-1",
        "title":"Project", "workspaceRoot":"/workspace/isolated-http-project",
    })
}

#[tokio::test]
async fn credentials_and_origin_are_checked_before_any_command_mutates_storage() {
    let server = TestServer::start().await;
    let original = server.snapshot().await;
    let body = serde_json::to_vec(&json!({
        "jsonrpc":"2.0", "id":1, "method":"command.execute", "params":{"command":project_command()},
    }))
    .unwrap();
    let authorized = server.token.header();
    let mut wrong = authorized.clone().into_bytes();
    let last = wrong.last_mut().unwrap();
    *last = if *last == b'0' { b'1' } else { b'0' };
    let wrong = String::from_utf8(wrong).unwrap();
    for (credentials, expected_status) in [
        (Vec::new(), 401),
        (vec![wrong], 401),
        (vec![authorized.clone(), authorized.clone()], 401),
    ] {
        let response = raw_request(&server, &credentials, &[], &body).await;
        assert_eq!(response.status, expected_status);
        assert_eq!(server.snapshot().await, original);
    }
    for origin in ["null", "http://127.0.0.1", "https://untrusted.example"] {
        let response = raw_request(
            &server,
            std::slice::from_ref(&authorized),
            &[("Origin", origin)],
            &body,
        )
        .await;
        assert_eq!(response.status, 403);
        assert_eq!(server.snapshot().await, original);
    }
    assert_eq!(
        server
            .call("receipt.get", json!({"commandId":"project-create"}))
            .await,
        Value::Null
    );
    assert_eq!(server.call("events.list", json!({})).await, json!([]));
    server.shutdown().await;
}

#[tokio::test]
async fn duplicate_json_fields_are_rejected_before_the_application_sees_a_command() {
    let server = TestServer::start().await;
    let before = server.snapshot().await;
    let authorization = server.token.header();
    let requests: &[&[u8]] = &[
        br#"{"jsonrpc":"2.0","id":1,"method":"state.snapshot","method":"command.execute","params":{"command":{"type":"project.create","commandId":"project-create","projectId":"project-1","title":"Project","workspaceRoot":"/workspace/test"}}}"#,
        br#"{"jsonrpc":"2.0","id":1,"method":"command.execute","params":{"command":{"type":"project.create","commandId":"first","commandId":"second","projectId":"project-1","title":"Project","workspaceRoot":"/workspace/test"}}}"#,
    ];
    for body in requests {
        let response = raw_request(&server, std::slice::from_ref(&authorization), &[], body).await;
        assert_eq!(response.status, 400);
        assert_eq!(response.body["error"]["code"], -32700);
        assert_eq!(server.snapshot().await, before);
    }
    for command_id in ["project-create", "first", "second"] {
        assert_eq!(
            server
                .call("receipt.get", json!({"commandId":command_id}))
                .await,
            Value::Null
        );
    }
    server.shutdown().await;
}

#[tokio::test]
async fn real_socket_commands_preserve_receipts_and_do_not_duplicate_durable_events() {
    let server = TestServer::start().await;
    let created_project: CommitOutcome = serde_json::from_value(
        server
            .call("command.execute", json!({"command":project_command()}))
            .await,
    )
    .unwrap();
    assert!(!created_project.replayed);
    assert_eq!(
        created_project.receipt.result["ok"]["project"]["projectId"],
        "project-1"
    );
    let create_thread = json!({
        "type":"thread.create", "commandId":"thread-create", "threadId":"thread-1", "projectId":"project-1",
        "title":"Thread", "modelSelection":{"instanceId":"codex","model":"test-model"},
        "runtimeMode":"approval-required", "interactionMode":"default", "branch":null,"worktreePath":null,
    });
    let created_thread: CommitOutcome = serde_json::from_value(
        server
            .call("command.execute", json!({"command":create_thread}))
            .await,
    )
    .unwrap();
    assert!(!created_thread.replayed);
    assert_eq!(
        created_thread.receipt.result["ok"]["thread"]["createdBy"],
        "user"
    );
    assert_eq!(
        created_thread.receipt.result["ok"]["thread"]["creationSource"],
        "server"
    );
    let before_replay = server.snapshot().await;
    let replay: CommitOutcome = serde_json::from_value(
        server
            .call("command.execute", json!({"command":create_thread}))
            .await,
    )
    .unwrap();
    assert!(replay.replayed);
    assert_eq!(replay.receipt, created_thread.receipt);
    assert_eq!(server.snapshot().await, before_replay);
    assert_eq!(
        server
            .call("receipt.get", json!({"commandId":"thread-create"}))
            .await,
        serde_json::to_value(&created_thread.receipt).unwrap()
    );
    let events = server
        .call("events.list", json!({"afterSequence":1,"limit":1}))
        .await;
    assert_eq!(events.as_array().unwrap().len(), 1);
    assert_eq!(events[0]["sequence"], 2);
    assert_eq!(events[0]["payload"]["type"], "thread.created");
    assert_eq!(
        events[0]["payload"]["payload"],
        created_thread.receipt.result["ok"]["thread"]
    );
    assert_eq!(before_replay.projections.len(), 2);
    assert_eq!(before_replay.last_sequence, 2);
    server.shutdown().await;
}

#[tokio::test]
async fn shutdown_closes_the_listener_and_an_accepted_incomplete_connection() {
    let server = TestServer::start().await;
    let mut idle = TcpStream::connect(server.address).await.unwrap();
    idle.write_all(b"POST /rpc HTTP/1.1\r\n").await.unwrap();
    // A completed second request proves the accept loop has processed the earlier connection.
    server.call("application.capabilities", json!({})).await;
    server.shutdown().await;
    let mut byte = [0u8; 1];
    match idle.read(&mut byte).await {
        Ok(0) => {}
        Err(error)
            if matches!(
                error.kind(),
                io::ErrorKind::ConnectionReset
                    | io::ErrorKind::ConnectionAborted
                    | io::ErrorKind::BrokenPipe
            ) => {}
        result => panic!("incomplete connection survived shutdown: {result:?}"),
    }
}
