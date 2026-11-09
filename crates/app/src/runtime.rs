//! Durable native RPC sessions. This does not implement upstream's orchestration model.

#![cfg(unix)]

use std::collections::BTreeMap;
use std::fmt;
use std::path::PathBuf;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
    mpsc,
};
use std::time::Duration;

use chrono::{SecondsFormat, Utc};
use serde_json::{Value, json};
use t3_code_ls_core::provider_process::{Config, Limits};
use t3_code_ls_storage::{
    CommitBatch, CommitOutcome, Error as StoreError, ProjectionMutation, Record, Snapshot, Store,
    request_digest,
};
use tokio::runtime::Handle;
use tokio::sync::{Mutex, Notify, oneshot};
use tokio::task::JoinHandle;

use crate::provider::{CodexSession, SessionEvent};

const MAX_SESSIONS: usize = 16;
const MAX_REQUEST_BYTES: usize = 2 * 1024 * 1024;
const MAX_RETRIES: usize = 16;

#[derive(Clone)]
pub struct ProviderConfig {
    pub program: PathBuf,
    pub args: Vec<String>,
}

#[derive(Debug)]
pub enum RuntimeError {
    Invalid(&'static str),
    Conflict(&'static str),
    Busy,
    Closed,
    Storage(StoreError),
    Provider(String),
}

impl fmt::Display for RuntimeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Invalid(message) => write!(f, "Invalid native provider request: {message}"),
            Self::Conflict(message) => write!(f, "Native provider state changed: {message}"),
            Self::Busy => f.write_str("Native provider capacity is full; no action was submitted"),
            Self::Closed => f.write_str("Native provider session is closed"),
            Self::Storage(error) => error.fmt(f),
            Self::Provider(message) => write!(f, "Native provider failed: {message}"),
        }
    }
}

impl std::error::Error for RuntimeError {}

impl From<StoreError> for RuntimeError {
    fn from(error: StoreError) -> Self {
        Self::Storage(error)
    }
}

type Result<T> = std::result::Result<T, RuntimeError>;

struct Actor {
    session_id: String,
    sender: mpsc::SyncSender<Command>,
    task: JoinHandle<()>,
    stop: Arc<AtomicBool>,
}

enum Command {
    Request {
        command_id: String,
        method: String,
        params: Value,
        digest: String,
        reply: oneshot::Sender<Result<Value>>,
    },
    Respond {
        command_id: String,
        request_id: Value,
        result: Value,
        digest: String,
        reply: oneshot::Sender<Result<Value>>,
    },
}

pub struct ProviderHub {
    store: Arc<Store>,
    config: ProviderConfig,
    actors: Mutex<BTreeMap<String, Actor>>,
    closed: Arc<AtomicBool>,
    changes: Arc<Notify>,
}

impl Drop for ProviderHub {
    fn drop(&mut self) {
        self.closed.store(true, Ordering::Release);
        self.changes.notify_waiters();
    }
}

fn now() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true)
}

fn identifier(value: &str) -> Result<()> {
    if value.is_empty() || value.len() > 256 || value.chars().any(char::is_control) {
        return Err(RuntimeError::Invalid(
            "identifiers must contain 1 to 256 non-control bytes",
        ));
    }
    Ok(())
}

fn session_key(thread: &str) -> String {
    format!("native/session/{thread}")
}
fn intent_key(command: &str) -> String {
    format!("native/request/{command}")
}
fn completion_key(command: &str) -> String {
    format!("native:result:{command}")
}
fn approval_key(session: &str, id: &Value) -> String {
    format!("native/approval/{session}/{}", request_digest(id))
}
fn rpc_key(id: &Value) -> Result<String> {
    match id {
        Value::String(text) if text.len() <= 4096 => Ok(text.clone()),
        Value::Number(number) => Ok(number.to_string()),
        _ => Err(RuntimeError::Invalid(
            "RPC request ID must be a string or number",
        )),
    }
}
fn row<'a>(snapshot: &'a Snapshot, key: &str) -> Option<&'a Value> {
    snapshot
        .projections
        .iter()
        .find(|projection| projection.key == key)
        .map(|projection| &projection.payload)
}
fn session_row(snapshot: &Snapshot, thread: &str, session: &str) -> Result<Value> {
    let value = row(snapshot, &session_key(thread)).ok_or(RuntimeError::Closed)?;
    if value["threadId"] != thread || value["sessionId"] != session || value["status"] == "closed" {
        return Err(RuntimeError::Conflict("session ownership was lost"));
    }
    Ok(value.clone())
}

struct Change {
    events: Vec<Record>,
    projections: Vec<ProjectionMutation>,
    result: Value,
}

async fn commit<F>(store: &Store, command: &str, digest: &str, plan: F) -> Result<CommitOutcome>
where
    F: Fn(&Snapshot) -> Result<Change>,
{
    for _ in 0..MAX_RETRIES {
        if let Some(receipt) = store.read_receipt(command).await? {
            if receipt.request_digest != digest {
                return Err(StoreError::CommandConflict.into());
            }
            return Ok(CommitOutcome {
                receipt,
                replayed: true,
            });
        }
        let snapshot = store.snapshot().await?;
        let change = plan(&snapshot)?;
        let batch = CommitBatch {
            command_id: command.into(),
            request_digest: digest.into(),
            expected_revision: snapshot.revision,
            events: change.events,
            projections: change.projections,
            effects: Vec::new(),
            result: change.result,
        };
        match store.commit(&batch).await {
            Ok(outcome) => return Ok(outcome),
            Err(StoreError::StaleRevision { .. } | StoreError::TransactionConflict) => continue,
            Err(StoreError::UnknownCommit) => {
                if let Some(receipt) = store.read_receipt(command).await? {
                    if receipt.request_digest == digest {
                        return Ok(CommitOutcome {
                            receipt,
                            replayed: true,
                        });
                    }
                }
                return Err(StoreError::UnknownCommit.into());
            }
            Err(error) => return Err(error.into()),
        }
    }
    Err(StoreError::TransactionConflict.into())
}

async fn reply_for(store: &Store, outcome: CommitOutcome) -> Result<Value> {
    let completion = store
        .read_receipt(&completion_key(&outcome.receipt.command_id))
        .await?;
    let snapshot = store.snapshot().await?;
    let unresolved = row(&snapshot, &intent_key(&outcome.receipt.command_id)).cloned();
    Ok(
        json!({"receipt":outcome.receipt,"replayed":outcome.replayed,"completion":completion,"unresolved":unresolved}),
    )
}

async fn duplicate(store: &Store, command: &str, digest: &str) -> Result<Option<Value>> {
    let Some(receipt) = store.read_receipt(command).await? else {
        return Ok(None);
    };
    if receipt.request_digest != digest {
        return Err(StoreError::CommandConflict.into());
    }
    Ok(Some(
        reply_for(
            store,
            CommitOutcome {
                receipt,
                replayed: true,
            },
        )
        .await?,
    ))
}

fn unique_id() -> Result<String> {
    let mut bytes = [0u8; 16];
    getrandom::fill(&mut bytes)
        .map_err(|_| RuntimeError::Provider("session identity generation failed".into()))?;
    Ok(bytes.iter().map(|byte| format!("{byte:02x}")).collect())
}

#[cfg(target_os = "linux")]
fn process_identity(pid: u32) -> Value {
    let start = std::fs::read_to_string(format!("/proc/{pid}/stat"))
        .ok()
        .and_then(|stat| {
            stat.rsplit_once(") ")
                .and_then(|(_, tail)| tail.split_whitespace().nth(19))
                .map(str::to_owned)
        });
    match start {
        Some(start) => json!({"platform":"linux","pid":pid,"startTime":start}),
        None => Value::Null,
    }
}

#[cfg(not(target_os = "linux"))]
fn process_identity(_: u32) -> Value {
    Value::Null
}

fn process_has_exited(identity: &Value) -> bool {
    #[cfg(target_os = "linux")]
    {
        if identity["platform"] != "linux" {
            return false;
        }
        let Some(pid) = identity["pid"]
            .as_u64()
            .and_then(|pid| u32::try_from(pid).ok())
            .filter(|pid| *pid > 0)
        else {
            return false;
        };
        let Some(start) = identity["startTime"].as_str() else {
            return false;
        };
        match std::fs::read_to_string(format!("/proc/{pid}/stat")) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => true,
            Ok(stat) => stat
                .rsplit_once(") ")
                .and_then(|(_, tail)| tail.split_whitespace().nth(19))
                .is_some_and(|current| current != start),
            _ => false,
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = identity;
        false
    }
}

#[derive(Clone)]
struct Context {
    cwd: PathBuf,
    fingerprint: String,
    model: String,
    mode: String,
    interaction: String,
    effort: Option<String>,
    tier: Option<String>,
}

fn context(snapshot: &Snapshot, thread_id: &str) -> Result<Context> {
    let thread = row(snapshot, &format!("thread/{thread_id}"))
        .ok_or(RuntimeError::Invalid("thread was not found"))?;
    if thread["id"] != thread_id
        || !thread["deletedAt"].is_null()
        || !thread["archivedAt"].is_null()
    {
        return Err(RuntimeError::Invalid(
            "thread is deleted, archived or malformed",
        ));
    }
    if thread["providerInstanceId"] != "codex" || thread["modelSelection"]["instanceId"] != "codex"
    {
        return Err(RuntimeError::Invalid(
            "only the configured Codex provider is implemented",
        ));
    }
    let project_id = thread["projectId"]
        .as_str()
        .ok_or(RuntimeError::Invalid("thread has no project"))?;
    let project = row(snapshot, &format!("project/{project_id}"))
        .ok_or(RuntimeError::Invalid("project was not found"))?;
    if project["projectId"] != project_id || !project["deletedAt"].is_null() {
        return Err(RuntimeError::Invalid("project is deleted or malformed"));
    }
    let workspace = thread["worktreePath"]
        .as_str()
        .or_else(|| project["workspaceRoot"].as_str())
        .ok_or(RuntimeError::Invalid("thread has no workspace directory"))?;
    let cwd = PathBuf::from(workspace);
    if !cwd.is_absolute() || !cwd.is_dir() {
        return Err(RuntimeError::Invalid(
            "workspace must be an existing absolute directory",
        ));
    }
    let cwd = cwd
        .canonicalize()
        .map_err(|_| RuntimeError::Invalid("workspace could not be resolved"))?;
    let model = thread["modelSelection"]["model"]
        .as_str()
        .filter(|model| !model.is_empty())
        .ok_or(RuntimeError::Invalid("thread has no model"))?
        .to_owned();
    let mode = thread["runtimeMode"]
        .as_str()
        .ok_or(RuntimeError::Invalid("thread has no runtime mode"))?
        .to_owned();
    if ![
        "approval-required",
        "auto-accept-edits",
        "auto",
        "full-access",
    ]
    .contains(&mode.as_str())
    {
        return Err(RuntimeError::Invalid("unsupported runtime mode"));
    }
    let interaction = thread["interactionMode"]
        .as_str()
        .ok_or(RuntimeError::Invalid("thread has no interaction mode"))?
        .to_owned();
    if !["default", "plan"].contains(&interaction.as_str()) {
        return Err(RuntimeError::Invalid("unsupported interaction mode"));
    }
    let mut effort = None;
    let mut tier = None;
    if let Some(options) = thread["modelSelection"].get("options") {
        for option in options
            .as_array()
            .ok_or(RuntimeError::Invalid("model options must be an array"))?
        {
            let target = match option["id"].as_str() {
                Some("reasoningEffort") => &mut effort,
                Some("serviceTier") => &mut tier,
                _ => {
                    return Err(RuntimeError::Invalid(
                        "native execution does not support this model option",
                    ));
                }
            };
            if target.is_some() {
                return Err(RuntimeError::Invalid("model option is repeated"));
            }
            *target = Some(
                option["value"]
                    .as_str()
                    .ok_or(RuntimeError::Invalid("model option must be a string"))?
                    .to_owned(),
            );
        }
    }
    let fingerprint = request_digest(
        &json!({"project":project_id,"cwd":cwd,"model":thread["modelSelection"],"mode":mode,"interaction":interaction}),
    );
    Ok(Context {
        cwd,
        fingerprint,
        model,
        mode,
        interaction,
        effort,
        tier,
    })
}

fn validated_input(method: &str, params: &Value) -> Result<()> {
    let object = params
        .as_object()
        .ok_or(RuntimeError::Invalid("params must be an object"))?;
    match method {
        "thread/start" | "thread/resume" | "turn/interrupt" if object.is_empty() => {}
        "turn/start" if object.len() == 1 && object.contains_key("input") => {
            let input = params["input"]
                .as_array()
                .ok_or(RuntimeError::Invalid("turn input must be an array"))?;
            if input.is_empty() || input.len() > 32 {
                return Err(RuntimeError::Invalid(
                    "turn input needs 1 to 32 text entries",
                ));
            }
            for entry in input {
                let entry = entry
                    .as_object()
                    .ok_or(RuntimeError::Invalid("input entry must be an object"))?;
                if entry
                    .keys()
                    .any(|key| !["type", "text", "text_elements"].contains(&key.as_str()))
                    || entry.get("type") != Some(&json!("text"))
                    || entry
                        .get("text")
                        .and_then(Value::as_str)
                        .is_none_or(str::is_empty)
                    || entry.get("text_elements").is_some_and(|elements| {
                        elements
                            .as_array()
                            .is_none_or(|elements| !elements.is_empty())
                    })
                {
                    return Err(RuntimeError::Invalid(
                        "only plain text input is supported; attachment authorization is not ported",
                    ));
                }
            }
        }
        _ => {
            return Err(RuntimeError::Invalid(
                "unsupported method or caller-supplied provider settings",
            ));
        }
    }
    if serde_json::to_vec(params)
        .map_err(|_| RuntimeError::Invalid("params could not be encoded"))?
        .len()
        > MAX_REQUEST_BYTES
    {
        return Err(RuntimeError::Invalid("params exceed 2 MiB"));
    }
    Ok(())
}

impl ProviderHub {
    pub fn new(store: Arc<Store>, config: ProviderConfig) -> Self {
        Self {
            store,
            config,
            actors: Mutex::new(BTreeMap::new()),
            closed: Arc::new(AtomicBool::new(false)),
            changes: Arc::new(Notify::new()),
        }
    }

    /// The caller must hold its process-lifetime exclusive database/server lock.
    pub async fn recover(&self) -> Result<Value> {
        if self.closed.load(Ordering::Acquire) {
            return Err(RuntimeError::Closed);
        }
        let actors = self.actors.lock().await;
        if !actors.is_empty() {
            return Err(RuntimeError::Conflict("cannot recover a running hub"));
        }
        let operation = format!("native:recover:{}", unique_id()?);
        let occurred_at = now();
        let digest = request_digest(&json!({"operation":"recover-native-sessions","id":operation}));
        let result = commit(&self.store, &operation, &digest, |snapshot| {
            let mut changes = Vec::new();
            let mut sessions = 0;
            let mut uncertain = 0;
            for projection in &snapshot.projections {
                let mut value = projection.payload.clone();
                if projection.key.starts_with("native/session/") && value["status"] != "closed" {
                    value["status"] = json!("closed");
                    value["reason"] = json!("server_restart");
                    value["activeTurnId"] = Value::Null;
                    value["loaded"] = json!(false);
                    value["processState"] = json!(if process_has_exited(&value["processIdentity"]) {"stopped"} else {"unknown"});
                    value["updatedAt"] = json!(occurred_at);
                    value["uncertain"] = json!(value["uncertain"] == true || snapshot.projections.iter().any(|request| request.key.starts_with("native/request/") && request.payload["sessionId"] == value["sessionId"] && matches!(request.payload["state"].as_str(), Some("accepted" | "sent" | "uncertain"))));
                    sessions += 1;
                } else if projection.key.starts_with("native/request/") && matches!(value["state"].as_str(), Some("accepted" | "sent")) {
                    value["state"] = json!("uncertain");
                    value["reason"] = json!("server_restart; provider action is never replayed");
                    value["updatedAt"] = json!(occurred_at);
                    uncertain += 1;
                } else if projection.key.starts_with("native/approval/") {
                    changes.push(ProjectionMutation::Delete { key: projection.key.clone() });
                    continue;
                } else { continue; }
                changes.push(ProjectionMutation::Put { key: projection.key.clone(), payload: value });
            }
            Ok(Change {
                events: if changes.is_empty() {Vec::new()} else {vec![Record {id:operation.clone(), payload:json!({"type":"native.provider.recovered","occurredAt":occurred_at,"sessions":sessions,"uncertainRequests":uncertain})}]},
                projections:changes,
                result:json!({"closedSessions":sessions,"uncertainRequests":uncertain}),
            })
        }).await?;
        drop(actors);
        self.changes.notify_waiters();
        Ok(result.receipt.result)
    }

    pub async fn open(&self, thread_id: &str) -> Result<Value> {
        identifier(thread_id)?;
        if self.closed.load(Ordering::Acquire) {
            return Err(RuntimeError::Closed);
        }
        if !self.config.program.is_absolute() || !self.config.program.is_file() {
            return Err(RuntimeError::Invalid(
                "configured provider executable must be an absolute file",
            ));
        }
        let mut actors = self.actors.lock().await;
        let finished: Vec<_> = actors
            .iter()
            .filter(|(_, actor)| actor.task.is_finished())
            .map(|(thread, _)| thread.clone())
            .collect();
        for finished_thread in finished {
            let actor = actors
                .remove(&finished_thread)
                .expect("finished actor was found");
            let _ = actor.task.await;
            close_owned(
                &self.store,
                &finished_thread,
                &actor.session_id,
                "provider_stopped",
                false,
            )
            .await?;
        }
        if actors.contains_key(thread_id) {
            return Ok(row(&self.store.snapshot().await?, &session_key(thread_id))
                .cloned()
                .unwrap_or(Value::Null));
        }
        if actors.len() >= MAX_SESSIONS {
            return Err(RuntimeError::Busy);
        }
        let snapshot = self.store.snapshot().await?;
        let selected = context(&snapshot, thread_id)?;
        let session_id = unique_id()?;
        let operation = format!("native:open:{session_id}");
        let occurred_at = now();
        let digest = request_digest(
            &json!({"threadId":thread_id,"sessionId":session_id,"context":selected.fingerprint}),
        );
        let opening = commit(&self.store, &operation, &digest, |snapshot| {
            if context(snapshot, thread_id)?.fingerprint != selected.fingerprint {
                return Err(RuntimeError::Conflict("thread configuration changed while opening"));
            }
            let previous = row(snapshot, &session_key(thread_id));
            if previous.is_some_and(|value| value["status"] != "closed") {
                return Err(RuntimeError::Conflict("another native session owns this thread"));
            }
            let native = previous.and_then(|value| value.get("nativeThreadId")).cloned().unwrap_or(Value::Null);
            if !native.is_null() && previous.is_none_or(|value| value["contextFingerprint"] != selected.fingerprint) {
                return Err(RuntimeError::Conflict("stored native thread belongs to another workspace or model configuration"));
            }
            if previous.is_some_and(|value| value["processState"] == "unknown" && !process_has_exited(&value["processIdentity"])) {
                return Err(RuntimeError::Conflict("previous provider process exit is unconfirmed"));
            }
            if previous.is_some_and(|value| value["uncertain"] == true) {
                return Err(RuntimeError::Conflict("previous provider request outcomes remain uncertain"));
            }
            let value = json!({"threadId":thread_id,"sessionId":session_id,"status":"opening","processState":"starting","contextFingerprint":selected.fingerprint,"nativeThreadId":native,"loaded":false,"activeTurnId":null,"uncertain":false,"createdAt":occurred_at,"updatedAt":occurred_at});
            Ok(Change {
                events:vec![Record {id:operation.clone(),payload:json!({"type":"native.provider.opening","threadId":thread_id,"sessionId":session_id,"occurredAt":occurred_at})}],
                projections:vec![ProjectionMutation::Put {key:session_key(thread_id),payload:value.clone()}],result:value,
            })
        }).await?;
        let (sender, receiver) = mpsc::sync_channel(32);
        let (ready, initialized) = oneshot::channel();
        let store = self.store.clone();
        let thread = thread_id.to_owned();
        let session = session_id.clone();
        let config = self.config.clone();
        let handle = Handle::current();
        let changes = self.changes.clone();
        let stop = Arc::new(AtomicBool::new(false));
        let actor_stop = stop.clone();
        let hub_stop = self.closed.clone();
        let initial = opening.receipt.result;
        let task = tokio::task::spawn_blocking(move || {
            actor_loop(
                store, handle, thread, session, selected, config, initial, receiver, ready,
                changes, actor_stop, hub_stop,
            )
        });
        actors.insert(
            thread_id.to_owned(),
            Actor {
                session_id,
                sender,
                task,
                stop,
            },
        );
        let result = initialized.await.unwrap_or(Err(RuntimeError::Closed));
        if result.is_err() {
            if let Some(actor) = actors.remove(thread_id) {
                let _ = actor.task.await;
            }
        }
        result
    }

    pub async fn request(
        &self,
        thread: &str,
        command: &str,
        method: &str,
        params: Value,
    ) -> Result<Value> {
        identifier(thread)?;
        identifier(command)?;
        if command.starts_with("native:") {
            return Err(RuntimeError::Invalid("native: command IDs are reserved"));
        }
        validated_input(method, &params)?;
        let digest = request_digest(
            &json!({"protocol":"native-provider-request-v1","threadId":thread,"method":method,"params":params}),
        );
        if let Some(result) = duplicate(&self.store, command, &digest).await? {
            return Ok(result);
        }
        let (reply, received) = oneshot::channel();
        self.send(
            thread,
            Command::Request {
                command_id: command.into(),
                method: method.into(),
                params,
                digest,
                reply,
            },
        )
        .await?;
        received.await.unwrap_or(Err(RuntimeError::Closed))
    }

    pub async fn respond(
        &self,
        thread: &str,
        command: &str,
        request_id: Value,
        result: Value,
    ) -> Result<Value> {
        identifier(thread)?;
        identifier(command)?;
        if command.starts_with("native:") {
            return Err(RuntimeError::Invalid("native: command IDs are reserved"));
        }
        rpc_key(&request_id)?;
        if serde_json::to_vec(&result)
            .map_err(|_| RuntimeError::Invalid("answer cannot be encoded"))?
            .len()
            > MAX_REQUEST_BYTES
        {
            return Err(RuntimeError::Invalid("answer exceeds 2 MiB"));
        }
        let digest = request_digest(
            &json!({"protocol":"native-provider-answer-v1","threadId":thread,"requestId":request_id,"result":result}),
        );
        if let Some(value) = duplicate(&self.store, command, &digest).await? {
            return Ok(value);
        }
        let (reply, received) = oneshot::channel();
        self.send(
            thread,
            Command::Respond {
                command_id: command.into(),
                request_id,
                result,
                digest,
                reply,
            },
        )
        .await?;
        received.await.unwrap_or(Err(RuntimeError::Closed))
    }

    async fn send(&self, thread: &str, command: Command) -> Result<()> {
        if self.closed.load(Ordering::Acquire) {
            return Err(RuntimeError::Closed);
        }
        let actors = self.actors.lock().await;
        let actor = actors.get(thread).ok_or(RuntimeError::Closed)?;
        actor.sender.try_send(command).map_err(|error| match error {
            mpsc::TrySendError::Full(_) => RuntimeError::Busy,
            mpsc::TrySendError::Disconnected(_) => RuntimeError::Closed,
        })
    }

    pub async fn close(&self, thread: &str) -> Result<Value> {
        identifier(thread)?;
        let mut actors = self.actors.lock().await;
        let Some(actor) = actors.get_mut(thread) else {
            return Ok(row(&self.store.snapshot().await?, &session_key(thread))
                .cloned()
                .unwrap_or(Value::Null));
        };
        actor.stop.store(true, Ordering::Release);
        // Keep the handle owned by the hub if this caller is cancelled while joining.
        let _ = (&mut actor.task).await;
        let actor = actors.remove(thread).expect("joined actor remains owned");
        let result = close_owned(
            &self.store,
            thread,
            &actor.session_id,
            "closed_by_client",
            false,
        )
        .await;
        self.changes.notify_waiters();
        let value = result?;
        if value["processState"] != "stopped" {
            return Err(RuntimeError::Provider(
                "owned process exit could not be confirmed".into(),
            ));
        }
        Ok(value)
    }

    /// Wait for a persisted RPC response or an explicitly uncertain outcome.
    pub async fn wait_for_request(&self, command: &str) -> Result<Value> {
        identifier(command)?;
        loop {
            let changed = self.changes.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let receipt = self
                .store
                .read_receipt(command)
                .await?
                .ok_or(RuntimeError::Invalid("request was not accepted"))?;
            let completion = self.store.read_receipt(&completion_key(command)).await?;
            let snapshot = self.store.snapshot().await?;
            let unresolved = row(&snapshot, &intent_key(command)).cloned();
            if completion.is_some()
                || unresolved
                    .as_ref()
                    .is_some_and(|value| value["state"] == "uncertain")
            {
                return Ok(
                    json!({"receipt":receipt,"completion":completion,"unresolved":unresolved}),
                );
            }
            if self.closed.load(Ordering::Acquire) {
                return Err(RuntimeError::Closed);
            }
            changed.await;
        }
    }

    /// Wait for an observed provider request that is already committed to redb.
    pub async fn wait_for_approval(&self, thread: &str) -> Result<Value> {
        identifier(thread)?;
        loop {
            let changed = self.changes.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let snapshot = self.store.snapshot().await?;
            if let Some(approval) = snapshot.projections.iter().find(|row| {
                row.key.starts_with("native/approval/") && row.payload["threadId"] == thread
            }) {
                return Ok(approval.payload.clone());
            }
            if self.closed.load(Ordering::Acquire)
                || row(&snapshot, &session_key(thread))
                    .is_none_or(|value| value["status"] != "open")
            {
                return Err(RuntimeError::Closed);
            }
            changed.await;
        }
    }

    pub async fn wait_for_idle(&self, thread: &str) -> Result<Value> {
        identifier(thread)?;
        loop {
            let changed = self.changes.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let snapshot = self.store.snapshot().await?;
            let current = row(&snapshot, &session_key(thread)).ok_or(RuntimeError::Closed)?;
            if current["uncertain"] == true {
                return Err(RuntimeError::Conflict("provider outcome is uncertain"));
            }
            if self.closed.load(Ordering::Acquire) || current["status"] != "open" {
                return Err(RuntimeError::Closed);
            }
            let starting = snapshot.projections.iter().any(|row| {
                row.key.starts_with("native/request/")
                    && row.payload["threadId"] == thread
                    && matches!(
                        row.payload["method"].as_str(),
                        Some("thread/start" | "thread/resume" | "turn/start")
                    )
            });
            if current["activeTurnId"].is_null() && !starting {
                return Ok(current.clone());
            }
            changed.await;
        }
    }

    pub async fn shutdown(&self) -> Result<()> {
        self.closed.store(true, Ordering::Release);
        self.changes.notify_waiters();
        let threads: Vec<_> = {
            let actors = self.actors.lock().await;
            for actor in actors.values() {
                actor.stop.store(true, Ordering::Release);
            }
            actors.keys().cloned().collect()
        };
        let mut first = None;
        for thread in threads {
            if let Err(error) = self.close(&thread).await {
                if first.is_none() {
                    first = Some(error);
                }
            }
        }
        match first {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}

async fn close_owned(
    store: &Store,
    thread: &str,
    session: &str,
    reason: &str,
    stopped: bool,
) -> Result<Value> {
    let operation = format!("native:close:{session}");
    let digest = request_digest(
        &json!({"operation":"close-native-session","threadId":thread,"sessionId":session}),
    );
    let occurred_at = now();
    let result = commit(store, &operation, &digest, |snapshot| {
        let mut current = row(snapshot, &session_key(thread)).cloned().ok_or(RuntimeError::Closed)?;
        if current["sessionId"] != session {return Err(RuntimeError::Conflict("session ownership was lost"));}
        let confirmed = stopped || current["processState"] == "stopped" || process_has_exited(&current["processIdentity"]);
        current["status"] = json!("closed"); current["loaded"] = json!(false); current["activeTurnId"] = Value::Null;
        current["processState"] = json!(if confirmed {"stopped"} else {"unknown"});
        current["updatedAt"] = json!(occurred_at); current["reason"] = json!(reason);
        let mut projections = vec![ProjectionMutation::Put {key:session_key(thread),payload:current.clone()}];
        let mut uncertain = 0;
        for projection in &snapshot.projections {
            if projection.payload["sessionId"] != session {continue;}
            if projection.key.starts_with("native/request/") && matches!(projection.payload["state"].as_str(), Some("accepted" | "sent")) {
                let mut value = projection.payload.clone(); value["state"] = json!("uncertain"); value["reason"] = json!(reason); value["updatedAt"] = json!(occurred_at);
                projections.push(ProjectionMutation::Put {key:projection.key.clone(),payload:value}); uncertain += 1;
            } else if projection.key.starts_with("native/approval/") {
                projections.push(ProjectionMutation::Delete {key:projection.key.clone()});
            }
        }
        if uncertain > 0 {
            current["uncertain"] = json!(true);
            projections[0] = ProjectionMutation::Put {key:session_key(thread),payload:current.clone()};
        }
        Ok(Change {events:vec![Record {id:operation.clone(),payload:json!({"type":"native.provider.closed","threadId":thread,"sessionId":session,"occurredAt":occurred_at,"reason":reason,"uncertainRequests":uncertain})}],projections,result:current})
    }).await?;
    Ok(result.receipt.result)
}

struct Pending {
    command: String,
    method: String,
}

#[derive(Clone)]
struct View {
    native_thread: Option<String>,
    loaded: bool,
    active_turn: Option<String>,
    last_completed: Option<String>,
    uncertain: bool,
}

fn apply_turn_notification(
    view: &mut View,
    method: &str,
    params: &Value,
) -> Result<Option<String>> {
    if !matches!(method, "turn/started" | "turn/completed")
        || view.native_thread.is_none()
        || params["threadId"].as_str() != view.native_thread.as_deref()
    {
        return Ok(None);
    }
    let turn = params["turn"]["id"]
        .as_str()
        .filter(|id| !id.is_empty() && id.len() <= 1024 && !id.chars().any(char::is_control))
        .ok_or_else(|| {
            RuntimeError::Provider(format!(
                "malformed {method} notification: turn.id is invalid"
            ))
        })?
        .to_owned();
    if method == "turn/started" {
        view.active_turn = Some(turn);
        return Ok(None);
    }
    if view.active_turn.as_deref() == Some(turn.as_str()) {
        view.active_turn = None;
    }
    view.last_completed = Some(turn.clone());
    Ok(Some(turn))
}

struct Approval {
    id: Value,
    method: String,
    native_thread: Option<String>,
    native_turn: Option<String>,
}

struct SessionActor {
    store: Arc<Store>,
    handle: Handle,
    thread: String,
    session_id: String,
    context: Context,
    session: CodexSession,
    view: View,
    pending: BTreeMap<String, Pending>,
    approvals: BTreeMap<String, Approval>,
    sequence: u64,
    changes: Arc<Notify>,
    stop: Arc<AtomicBool>,
    hub_stop: Arc<AtomicBool>,
}

impl SessionActor {
    fn intent(
        &self,
        command: &str,
        method: &str,
        params: &Value,
        digest: &str,
    ) -> Result<CommitOutcome> {
        let occurred_at = now();
        let event_id = format!("native:intent:{command}");
        self.handle.block_on(commit(&self.store, command, digest, |snapshot| {
            let current = session_row(snapshot, &self.thread, &self.session_id)?;
            if current["status"] != "open" || context(snapshot, &self.thread)?.fingerprint != self.context.fingerprint {
                return Err(RuntimeError::Conflict("thread or workspace configuration changed"));
            }
            let value = json!({"commandId":command,"threadId":self.thread,"sessionId":self.session_id,"method":method,"state":"accepted","rpcId":null,"createdAt":occurred_at,"updatedAt":occurred_at});
            Ok(Change {
                events:vec![Record {id:event_id.clone(),payload:json!({"type":"native.provider.intent","threadId":self.thread,"sessionId":self.session_id,"occurredAt":occurred_at,"commandId":command,"method":method,"params":params})}],
                projections:vec![ProjectionMutation::Put {key:intent_key(command),payload:value}],
                result:json!({"status":"accepted","threadId":self.thread,"sessionId":self.session_id,"commandId":command}),
            })
        }))
    }

    fn params(&self, method: &str, input: &Value) -> Result<Value> {
        if self.view.uncertain && method != "turn/interrupt" {
            return Err(RuntimeError::Conflict(
                "a previous provider outcome is uncertain",
            ));
        }
        let opening = self
            .pending
            .values()
            .any(|pending| matches!(pending.method.as_str(), "thread/start" | "thread/resume"));
        let starting = self
            .pending
            .values()
            .any(|pending| pending.method == "turn/start");
        match method {
            "thread/start" if self.view.native_thread.is_some() || opening => {
                return Err(RuntimeError::Conflict(
                    "thread already has a provider identity; resume it",
                ));
            }
            "thread/resume" if self.view.native_thread.is_none() || opening || self.view.loaded => {
                return Err(RuntimeError::Conflict(
                    "thread cannot be resumed in its current state",
                ));
            }
            "turn/start" if !self.view.loaded || starting || self.view.active_turn.is_some() => {
                return Err(RuntimeError::Conflict(
                    "resume the thread and wait for its current turn to finish",
                ));
            }
            "turn/interrupt" if self.view.active_turn.is_none() => {
                return Err(RuntimeError::Conflict("thread has no active provider turn"));
            }
            _ => {}
        }
        if method == "turn/interrupt" {
            return Ok(json!({"threadId":self.view.native_thread,"turnId":self.view.active_turn}));
        }
        let (approval, reviewer, sandbox, sandbox_type) = match self.context.mode.as_str() {
            "approval-required" => ("untrusted", "user", "read-only", "readOnly"),
            "auto-accept-edits" => ("on-request", "user", "workspace-write", "workspaceWrite"),
            "auto" => (
                "on-request",
                "auto_review",
                "workspace-write",
                "workspaceWrite",
            ),
            "full-access" => ("never", "user", "danger-full-access", "dangerFullAccess"),
            _ => return Err(RuntimeError::Invalid("unsupported stored runtime mode")),
        };
        let mut result = json!({"cwd":self.context.cwd,"model":self.context.model,"approvalPolicy":approval,"approvalsReviewer":reviewer});
        if method != "thread/start" {
            result["threadId"] = json!(self.view.native_thread);
        }
        if let Some(tier) = &self.context.tier {
            result["serviceTier"] = json!(tier);
        }
        if method == "turn/start" {
            result["input"] = input["input"].clone();
            result["sandboxPolicy"] = json!({"type":sandbox_type});
            result["summary"] = json!("detailed");
            result["collaborationMode"] =
                json!({"mode":self.context.interaction,"settings":{"model":self.context.model}});
            if let Some(effort) = &self.context.effort {
                result["effort"] = json!(effort);
                result["collaborationMode"]["settings"]["reasoning_effort"] = json!(effort);
            }
        } else {
            result["sandbox"] = json!(sandbox);
            if let Some(effort) = &self.context.effort {
                result["config"] = json!({"model_reasoning_effort":effort});
            }
        }
        Ok(result)
    }

    fn request(
        &mut self,
        command: &str,
        method: &str,
        params: Value,
        digest: &str,
    ) -> Result<Value> {
        if let Some(value) = self
            .handle
            .block_on(duplicate(&self.store, command, digest))?
        {
            return Ok(value);
        }
        let actual = self.params(method, &params)?;
        let accepted = self.intent(command, method, &params, digest)?;
        if accepted.replayed {
            return self.handle.block_on(reply_for(&self.store, accepted));
        }
        if self.stop.load(Ordering::Acquire) || self.hub_stop.load(Ordering::Acquire) {
            self.finish_answer(
                command,
                "not_sent",
                Some("session stopped before dispatch".into()),
                None,
            )?;
            return self.handle.block_on(reply_for(&self.store, accepted));
        }
        let sent = match method {
            "thread/start" => self.session.start_thread(actual),
            "thread/resume" => self.session.resume_thread(actual),
            "turn/start" => self.session.start_turn(actual),
            "turn/interrupt" => self.session.interrupt_turn(
                actual["threadId"].as_str().unwrap_or(""),
                actual["turnId"].as_str().unwrap_or(""),
            ),
            _ => unreachable!("method was validated before the intent"),
        };
        match sent {
            Ok(id) => {
                self.pending.insert(
                    rpc_key(&id)?,
                    Pending {
                        command: command.into(),
                        method: method.into(),
                    },
                );
                let operation = format!("native:sent:{command}");
                let stamp = now();
                let digest = request_digest(
                    &json!({"sessionId":self.session_id,"commandId":command,"rpcId":id}),
                );
                self.handle
                    .block_on(commit(&self.store, &operation, &digest, |snapshot| {
                        session_row(snapshot, &self.thread, &self.session_id)?;
                        let mut value = row(snapshot, &intent_key(command))
                            .cloned()
                            .ok_or(RuntimeError::Conflict("request intent disappeared"))?;
                        value["state"] = json!("sent");
                        value["rpcId"] = id.clone();
                        value["updatedAt"] = json!(stamp);
                        Ok(Change {
                            events: Vec::new(),
                            projections: vec![ProjectionMutation::Put {
                                key: intent_key(command),
                                payload: value,
                            }],
                            result: json!({"state":"sent","rpcId":id}),
                        })
                    }))?;
            }
            Err(error) => {
                self.finish_answer(command, "not_sent", Some(error.to_string()), None)?;
            }
        }
        self.handle.block_on(reply_for(&self.store, accepted))
    }

    fn respond(&mut self, command: &str, id: Value, result: Value, digest: &str) -> Result<Value> {
        if let Some(value) = self
            .handle
            .block_on(duplicate(&self.store, command, digest))?
        {
            return Ok(value);
        }
        let key = rpc_key(&id)?;
        let approval = self.approvals.get(&key).ok_or(RuntimeError::Invalid(
            "provider request is no longer pending",
        ))?;
        if approval.native_thread.as_deref() != self.view.native_thread.as_deref()
            || self.view.native_thread.is_none()
            || ![
                "item/commandExecution/requestApproval",
                "item/fileChange/requestApproval",
                "item/permissions/requestApproval",
                "item/tool/requestUserInput",
            ]
            .contains(&approval.method.as_str())
        {
            return Err(RuntimeError::Invalid(
                "this native provider request cannot be answered by the application",
            ));
        }
        let accepted = self.intent(
            command,
            "provider/respond",
            &json!({"requestId":id,"result":result}),
            digest,
        )?;
        if accepted.replayed {
            return self.handle.block_on(reply_for(&self.store, accepted));
        }
        if self.stop.load(Ordering::Acquire) || self.hub_stop.load(Ordering::Acquire) {
            self.finish_answer(
                command,
                "not_sent",
                Some("session stopped before dispatch".into()),
                None,
            )?;
            return self.handle.block_on(reply_for(&self.store, accepted));
        }
        match self.session.respond(&id, result) {
            Ok(()) => {
                self.finish_answer(command, "submitted", None, Some(&id))?;
                self.approvals.remove(&key);
            }
            Err(error) => {
                self.finish_answer(command, "not_sent", Some(error.to_string()), None)?;
            }
        }
        self.handle.block_on(reply_for(&self.store, accepted))
    }

    fn finish_answer(
        &mut self,
        command: &str,
        state: &str,
        error: Option<String>,
        answered_id: Option<&Value>,
    ) -> Result<()> {
        let operation = completion_key(command);
        let stamp = now();
        let digest = request_digest(
            &json!({"sessionId":self.session_id,"commandId":command,"state":state,"error":error}),
        );
        self.handle.block_on(commit(&self.store, &operation, &digest, |snapshot| {
            session_row(snapshot, &self.thread, &self.session_id)?;
            let mut projections = vec![ProjectionMutation::Delete {key:intent_key(command)}];
            if let Some(id) = answered_id {
                projections.push(ProjectionMutation::Delete {key:approval_key(&self.session_id,id)});
            }
            Ok(Change {events:vec![Record {id:operation.clone(),payload:json!({"type":"native.provider.submission","threadId":self.thread,"sessionId":self.session_id,"occurredAt":stamp,"commandId":command,"state":state,"error":error})}],
                projections,
                result:json!({"state":state,"error":error})})
        }))?;
        self.changes.notify_waiters();
        Ok(())
    }

    fn event(&mut self, event: SessionEvent) -> Result<()> {
        self.sequence += 1;
        let event_id = format!("native:event:{}:{}", self.session_id, self.sequence);
        let mut view = self.view.clone();
        let mut completed = None;
        let mut remove_approvals = Vec::new();
        let mut new_approval = None;
        let mut outcome = None;
        match &event {
            SessionEvent::Response { id, method, result } => {
                let key = rpc_key(id)?;
                let pending = self
                    .pending
                    .get(&key)
                    .ok_or(RuntimeError::Conflict("response has no durable request"))?;
                completed = Some((key, pending.command.clone()));
                outcome = Some("responded");
                if method == "thread/start" || method == "thread/resume" {
                    view.native_thread = result["thread"]["id"].as_str().map(str::to_owned);
                    view.loaded = true;
                } else if method == "turn/start" {
                    let turn = result["turn"]["id"].as_str().map(str::to_owned);
                    if turn != view.last_completed {
                        view.active_turn = turn;
                    }
                }
            }
            SessionEvent::RequestError { id, .. } | SessionEvent::RequestFailed { id, .. } => {
                let key = rpc_key(id)?;
                let pending = self
                    .pending
                    .get(&key)
                    .ok_or(RuntimeError::Conflict("failure has no durable request"))?;
                completed = Some((key, pending.command.clone()));
                outcome = Some(if matches!(&event, SessionEvent::RequestError { .. }) {
                    "rejected"
                } else {
                    view.uncertain = true;
                    "uncertain"
                });
            }
            SessionEvent::ServerRequest { id, method, params } => {
                new_approval = Some((
                    rpc_key(id)?,
                    Approval {
                        id: id.clone(),
                        method: method.clone(),
                        native_thread: params["threadId"].as_str().map(str::to_owned),
                        native_turn: params["turnId"].as_str().map(str::to_owned),
                    },
                ));
            }
            SessionEvent::Notification { method, params } => {
                if method == "serverRequest/resolved" {
                    remove_approvals.push(rpc_key(&params["requestId"])?);
                }
                if let Some(turn) = apply_turn_notification(&mut view, method, params)? {
                    for (key, approval) in &self.approvals {
                        if approval.native_thread.as_deref() == view.native_thread.as_deref()
                            && approval.native_turn.as_deref() == Some(turn.as_str())
                        {
                            remove_approvals.push(key.clone());
                        }
                    }
                }
            }
        }
        let operation = completed
            .as_ref()
            .map(|(_, command)| completion_key(command))
            .unwrap_or_else(|| event_id.clone());
        let stamp = now();
        let payload = serde_json::to_value(&event)
            .map_err(|_| RuntimeError::Invalid("provider event cannot be encoded"))?;
        let digest = request_digest(
            &json!({"sessionId":self.session_id,"sequence":self.sequence,"event":payload}),
        );
        self.handle.block_on(commit(&self.store, &operation, &digest, |snapshot| {
            let mut current = session_row(snapshot, &self.thread, &self.session_id)?;
            current["nativeThreadId"] = json!(view.native_thread); current["loaded"] = json!(view.loaded);
            current["activeTurnId"] = json!(view.active_turn); current["updatedAt"] = json!(stamp);
            current["uncertain"] = json!(view.uncertain);
            current["lastEventSequence"] = json!(snapshot.last_sequence + 1);
            let mut projections = vec![ProjectionMutation::Put {key:session_key(&self.thread),payload:current}];
            if let Some((_,command)) = &completed {
                if outcome == Some("uncertain") {
                    let mut intent = row(snapshot,&intent_key(command)).cloned().ok_or(RuntimeError::Conflict("request intent disappeared"))?;
                    intent["state"] = json!("uncertain"); intent["updatedAt"] = json!(stamp);
                    projections.push(ProjectionMutation::Put {key:intent_key(command),payload:intent});
                } else {projections.push(ProjectionMutation::Delete {key:intent_key(command)});}
            }
            if let Some((_, approval)) = &new_approval {
                projections.push(ProjectionMutation::Put {key:approval_key(&self.session_id,&approval.id),payload:json!({"threadId":self.thread,"sessionId":self.session_id,"requestId":approval.id,"method":approval.method,"nativeThreadId":approval.native_thread,"nativeTurnId":approval.native_turn,"state":"pending"})});
            }
            for key in &remove_approvals {
                if let Some(approval) = self.approvals.get(key) {projections.push(ProjectionMutation::Delete {key:approval_key(&self.session_id,&approval.id)});}
            }
            Ok(Change {events:vec![Record {id:event_id.clone(),payload:json!({"type":"native.provider.event","threadId":self.thread,"sessionId":self.session_id,"occurredAt":stamp,"commandId":completed.as_ref().map(|(_,command)| command),"payload":payload})}],
                projections,result:json!({"state":outcome.unwrap_or("recorded"),"eventId":event_id})})
        }))?;
        if let Some((key, _)) = completed {
            self.pending.remove(&key);
        }
        for key in remove_approvals {
            self.approvals.remove(&key);
        }
        if let Some((key, approval)) = new_approval {
            self.approvals.insert(key, approval);
        }
        self.view = view;
        self.changes.notify_waiters();
        Ok(())
    }
}

fn actor_loop(
    store: Arc<Store>,
    handle: Handle,
    thread: String,
    session_id: String,
    selected: Context,
    config: ProviderConfig,
    initial: Value,
    receiver: mpsc::Receiver<Command>,
    ready: oneshot::Sender<Result<Value>>,
    changes: Arc<Notify>,
    stop: Arc<AtomicBool>,
    hub_stop: Arc<AtomicBool>,
) {
    if stop.load(Ordering::Acquire) || hub_stop.load(Ordering::Acquire) {
        let _ = handle.block_on(close_owned(
            &store,
            &thread,
            &session_id,
            "stopped_before_spawn",
            true,
        ));
        let _ = ready.send(Err(RuntimeError::Closed));
        changes.notify_waiters();
        return;
    }
    let process_config = Config {
        program: config.program,
        args: config.args,
        cwd: selected.cwd.clone(),
        home: None,
        inherit_environment: true,
        inherit_stderr: false,
        track_requests: true,
        environment: Vec::new(),
        limits: Limits {
            max_frame_bytes: MAX_REQUEST_BYTES,
            max_pending_requests: 32,
            max_queued_messages: 32,
            request_timeout: Duration::from_secs(30),
        },
    };
    let mut session = match CodexSession::spawn_tracked(process_config) {
        Ok(session) => session,
        Err((error, stopped)) => {
            let _ = handle.block_on(close_owned(
                &store,
                &thread,
                &session_id,
                "initialization_failed",
                stopped,
            ));
            let _ = ready.send(Err(RuntimeError::Provider(error.to_string())));
            changes.notify_waiters();
            return;
        }
    };
    let operation = format!("native:ready:{session_id}");
    let identity = process_identity(session.process_id());
    let stamp = now();
    let digest = request_digest(&json!({"operation":"ready","sessionId":session_id}));
    let opened = handle.block_on(commit(&store,&operation,&digest,|snapshot| {
        let mut current = session_row(snapshot,&thread,&session_id)?;
        current["status"] = json!("open"); current["processState"] = json!("running"); current["processIdentity"] = identity.clone(); current["updatedAt"] = json!(stamp);
        Ok(Change {events:vec![Record {id:operation.clone(),payload:json!({"type":"native.provider.ready","threadId":thread,"sessionId":session_id,"occurredAt":stamp})}],projections:vec![ProjectionMutation::Put {key:session_key(&thread),payload:current.clone()}],result:current})
    }));
    let opened = match opened {
        Ok(value) => value,
        Err(error) => {
            let stopped = session.shutdown().is_ok() || process_has_exited(&identity);
            let _ = handle.block_on(close_owned(
                &store,
                &thread,
                &session_id,
                "persistence_failed",
                stopped,
            ));
            let _ = ready.send(Err(error));
            changes.notify_waiters();
            return;
        }
    };
    let mut actor = SessionActor {
        store,
        handle,
        thread,
        session_id,
        context: selected,
        session,
        view: View {
            native_thread: initial["nativeThreadId"].as_str().map(str::to_owned),
            loaded: false,
            active_turn: None,
            last_completed: None,
            uncertain: false,
        },
        pending: BTreeMap::new(),
        approvals: BTreeMap::new(),
        sequence: 0,
        changes,
        stop,
        hub_stop,
    };
    let _ = ready.send(Ok(opened.receipt.result));
    let mut reason = "server_closed";
    loop {
        if actor.stop.load(Ordering::Acquire) || actor.hub_stop.load(Ordering::Acquire) {
            reason = "closed_by_client";
            break;
        }
        match receiver.try_recv() {
            Ok(Command::Request {
                command_id,
                method,
                params,
                digest,
                reply,
            }) => {
                let result = actor.request(&command_id, &method, params, &digest);
                let fatal = matches!(&result, Err(RuntimeError::Storage(_)));
                let _ = reply.send(result);
                if fatal {
                    reason = "persistence_failed";
                    break;
                }
            }
            Ok(Command::Respond {
                command_id,
                request_id,
                result,
                digest,
                reply,
            }) => {
                let result = actor.respond(&command_id, request_id, result, &digest);
                let fatal = matches!(&result, Err(RuntimeError::Storage(_)));
                let _ = reply.send(result);
                if fatal {
                    reason = "persistence_failed";
                    break;
                }
            }
            Err(mpsc::TryRecvError::Disconnected) => break,
            Err(mpsc::TryRecvError::Empty) => {}
        }
        match actor.session.poll(Duration::from_millis(25)) {
            Ok(Some(event)) => {
                if actor.event(event).is_err() {
                    reason = "persistence_or_protocol_failed";
                    break;
                }
            }
            Ok(None) => {}
            Err(_) => {
                reason = "provider_disconnected";
                break;
            }
        }
    }
    let stopped = actor.session.shutdown().is_ok();
    let _ = actor.handle.block_on(close_owned(
        &actor.store,
        &actor.thread,
        &actor.session_id,
        reason,
        stopped,
    ));
    actor.changes.notify_waiters();
}

#[cfg(test)]
mod tests {
    use super::*;

    fn working_view() -> View {
        View {
            native_thread: Some("native-thread".into()),
            loaded: true,
            active_turn: Some("working-turn".into()),
            last_completed: Some("previous-turn".into()),
            uncertain: false,
        }
    }

    #[test]
    fn malformed_bound_turn_notifications_cannot_clear_active_work() {
        for method in ["turn/started", "turn/completed"] {
            for turn in [
                Value::Null,
                json!({}),
                json!({"id":null}),
                json!({"id":0}),
                json!({"id":""}),
                json!({"id":"invalid\nturn"}),
                json!({"id":"x".repeat(1025)}),
            ] {
                let mut view = working_view();
                let result = apply_turn_notification(
                    &mut view,
                    method,
                    &json!({"threadId":"native-thread", "turn":turn}),
                );
                assert!(result.is_err());
                assert_eq!(view.active_turn.as_deref(), Some("working-turn"));
                assert_eq!(view.last_completed.as_deref(), Some("previous-turn"));
            }
        }
    }

    #[test]
    fn unrelated_notifications_leave_turn_lifecycle_unchanged() {
        for (method, params) in [
            ("unknown/notification", json!({"threadId":"native-thread"})),
            ("turn/started", json!({"threadId":"another-thread"})),
            ("turn/completed", json!({"threadId":"another-thread"})),
        ] {
            let mut view = working_view();
            assert!(
                apply_turn_notification(&mut view, method, &params)
                    .unwrap()
                    .is_none()
            );
            assert_eq!(view.active_turn.as_deref(), Some("working-turn"));
            assert_eq!(view.last_completed.as_deref(), Some("previous-turn"));
        }
    }

    #[test]
    fn completing_another_turn_cannot_clear_current_work() {
        let mut view = working_view();
        let started = json!({"threadId":"native-thread", "turn":{"id":"new-turn"}});
        assert!(
            apply_turn_notification(&mut view, "turn/started", &started)
                .unwrap()
                .is_none()
        );
        let previous = json!({"threadId":"native-thread", "turn":{"id":"previous-turn"}});
        assert_eq!(
            apply_turn_notification(&mut view, "turn/completed", &previous)
                .unwrap()
                .as_deref(),
            Some("previous-turn")
        );
        assert_eq!(view.active_turn.as_deref(), Some("new-turn"));
        assert_eq!(
            apply_turn_notification(&mut view, "turn/completed", &started)
                .unwrap()
                .as_deref(),
            Some("new-turn")
        );
        assert!(view.active_turn.is_none());
        assert_eq!(view.last_completed.as_deref(), Some("new-turn"));
    }

    #[test]
    fn native_requests_cannot_choose_workspace_or_provider_authority() {
        for (method, params) in [
            ("thread/start", json!({"cwd":"/other"})),
            ("thread/resume", json!({"threadId":"someone-else"})),
            (
                "turn/start",
                json!({"input":[{"type":"localImage","path":"/private"}]}),
            ),
            (
                "turn/start",
                json!({"input":[{"type":"text","text":"hello"}],"approvalPolicy":"never"}),
            ),
            ("command/exec", json!({})),
        ] {
            assert!(validated_input(method, &params).is_err());
        }
        assert!(
            validated_input(
                "turn/start",
                &json!({"input":[{"type":"text","text":"hello"}]})
            )
            .is_ok()
        );
        assert!(validated_input("thread/start", &json!({})).is_ok());
    }
}
