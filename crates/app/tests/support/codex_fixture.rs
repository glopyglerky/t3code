//! A controlled Codex JSON-RPC peer. It never runs an agent or a requested command.

use std::io::{self, BufRead, Write};

use serde_json::{Value, json};

struct Peer {
    output: io::Stdout,
    initialized: bool,
    startup_request_pending: bool,
    turn: Option<(String, String)>,
    thread_requests: u32,
}

impl Peer {
    fn write(&mut self, value: Value) -> io::Result<()> {
        serde_json::to_writer(&mut self.output, &value)?;
        writeln!(self.output)?;
        self.output.flush()
    }

    fn notify(&mut self, method: &str, params: Value) -> io::Result<()> {
        self.write(json!({ "method": method, "params": params }))
    }

    fn result(&mut self, id: &Value, result: Value) -> io::Result<()> {
        self.write(json!({ "id": id, "result": result }))
    }

    fn error(&mut self, id: &Value, code: i64, message: &str) -> io::Result<()> {
        self.write(json!({ "id": id, "error": { "code": code, "message": message } }))
    }

    fn complete(&mut self, status: &str) -> io::Result<()> {
        if let Some((thread, turn)) = self.turn.take() {
            self.notify(
                "turn/completed",
                json!({ "threadId": thread, "turn": { "id": turn, "status": status, "items": [], "error": null } }),
            )?;
        }
        Ok(())
    }

    fn handle(&mut self, message: Value, scenario: &str) -> io::Result<()> {
        let id = message.get("id").cloned().unwrap_or(Value::Null);
        let params = message.get("params").cloned().unwrap_or(Value::Null);
        match message.get("method").and_then(Value::as_str) {
            Some("initialize") => {
                if scenario == "abort-initialize" {
                    std::process::exit(17);
                }
                if scenario == "malformed-initialize" {
                    return self.result(&id, json!({}));
                }
                self.notify("fixture/initializing", json!({ "ready": false }))?;
                if scenario == "pre-init-traffic" {
                    self.startup_request_pending = true;
                    self.write(json!({
                        "id": "startup-request",
                        "method": "item/tool/requestUserInput",
                        "params": {"threadId":"startup","turnId":"startup","questions":[]}
                    }))?;
                }
                self.result(
                    &id,
                    json!({
                        "userAgent": "t3code-ls-controlled-peer/1",
                        "codexHome": std::env::current_dir()?.to_string_lossy(),
                        "platformFamily": "unix", "platformOs": std::env::consts::OS
                    }),
                )
            }
            Some("initialized") => {
                self.initialized = true;
                self.notify("fixture/initialized", json!({ "ready": true }))
            }
            Some("thread/start" | "thread/resume") => {
                if !self.initialized {
                    return self.error(&id, -32000, "initialized notification is required");
                }
                if scenario == "exit-on-thread" {
                    std::process::exit(17);
                }
                self.thread_requests += 1;
                if scenario == "reject-first-thread" && self.thread_requests == 1 {
                    return self.error(&id, -32001, "controlled rejection");
                }
                match params.get("fixture").and_then(Value::as_str) {
                    Some("provider-error") => {
                        return self.error(&id, -32001, "controlled rejection");
                    }
                    Some("exit") => std::process::exit(17),
                    Some("ambiguous") => {
                        return self.write(json!({
                            "id":id,"result":{"thread":{"id":"native-thread"}},
                            "error":{"code":-32002,"message":"ambiguous"}
                        }));
                    }
                    _ => {}
                }
                let thread_id =
                    if params.get("fixture").and_then(Value::as_str) == Some("wrong-thread") {
                        "another-native-thread"
                    } else {
                        params
                            .get("threadId")
                            .and_then(Value::as_str)
                            .unwrap_or("native-thread")
                    };
                self.result(
                    &id,
                    json!({
                        "thread": {"id":thread_id,"turns":[]},
                        "model": "fixture-model",
                        "fixture": {
                            "startupRequestPending": self.startup_request_pending,
                            "approvalPending": self.turn.is_some()
                        }
                    }),
                )
            }
            Some("turn/start") => {
                if !self.initialized {
                    return self.error(&id, -32000, "initialized notification is required");
                }
                if self.turn.is_some() {
                    return self.error(&id, -32003, "a controlled turn is already pending");
                }
                let thread = params
                    .get("threadId")
                    .and_then(Value::as_str)
                    .unwrap_or("native-thread")
                    .to_owned();
                let turn = "native-turn".to_owned();
                self.turn = Some((thread.clone(), turn.clone()));
                self.result(&id, json!({ "turn": { "id": turn, "status": "inProgress", "items": [], "error": null } }))?;
                self.notify("turn/started", json!({ "threadId": thread, "turn": { "id": turn, "status": "inProgress", "items": [], "error": null } }))?;
                self.notify("item/agentMessage/delta", json!({
                    "threadId":thread,"turnId":turn,"itemId":"assistant-1","delta":"Before approval. "
                }))?;
                self.write(json!({
                    "id":100,"method":"item/commandExecution/requestApproval",
                    "params": {
                        "threadId":thread,"turnId":turn,"itemId":"command-1",
                        "command":"controlled fixture action, never executed", "reason":"fixture"
                    }
                }))
            }
            Some("turn/interrupt") => {
                let matches = self.turn.as_ref().is_some_and(|(thread, turn)| {
                    params.get("threadId").and_then(Value::as_str) == Some(thread.as_str())
                        && params.get("turnId").and_then(Value::as_str) == Some(turn.as_str())
                });
                if !matches {
                    return self.error(&id, -32004, "controlled turn was not found");
                }
                self.notify(
                    "serverRequest/resolved",
                    json!({"requestId":100,"threadId":params["threadId"]}),
                )?;
                self.result(&id, json!({}))?;
                self.complete("interrupted")
            }
            Some(_) => self.error(&id, -32601, "unsupported fixture method"),
            None if id == json!("startup-request") => {
                if !self.startup_request_pending {
                    return Err(io::Error::other("startup request was answered twice"));
                }
                self.startup_request_pending = false;
                self.notify(
                    "fixture/startupAnswered",
                    json!({"result":message.get("result"),"error":message.get("error")}),
                )
            }
            None if id == json!(100) => {
                let Some((thread, turn)) = self.turn.clone() else {
                    return Err(io::Error::other("approval arrived without a pending turn"));
                };
                let accepted = message
                    .get("result")
                    .and_then(|result| result.get("decision"))
                    .and_then(Value::as_str)
                    == Some("accept");
                self.notify("fixture/approvalHandled", json!({"accepted":accepted,"result":message.get("result"),"error":message.get("error")}))?;
                self.notify(
                    "serverRequest/resolved",
                    json!({"requestId":100,"threadId":thread}),
                )?;
                if accepted {
                    self.notify("item/agentMessage/delta", json!({
                        "threadId":thread,"turnId":turn,"itemId":"assistant-1","delta":"After approval."
                    }))?;
                    self.notify("item/completed", json!({
                        "threadId":thread,"turnId":turn,
                        "item":{"id":"assistant-1","type":"agentMessage","text":"Before approval. After approval."}
                    }))?;
                }
                self.complete(if accepted { "completed" } else { "interrupted" })
            }
            None => Err(io::Error::other("unexpected fixture response")),
        }
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let scenario = std::env::args().nth(1).unwrap_or_else(|| "happy".into());
    std::fs::write("fixture.pid", std::process::id().to_string())?;
    let mut received = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open("fixture.messages.jsonl")?;
    let mut peer = Peer {
        output: io::stdout(),
        initialized: false,
        startup_request_pending: false,
        turn: None,
        thread_requests: 0,
    };
    for line in io::stdin().lock().lines() {
        let line = line?;
        if !line.trim().is_empty() {
            let message: Value = serde_json::from_str(&line)?;
            let mut record =
                serde_json::to_vec(&json!({"pid":std::process::id(),"message":message}))?;
            record.push(b'\n');
            received.write_all(&record)?;
            received.flush()?;
            peer.handle(message, &scenario)?;
        }
    }
    Ok(())
}
