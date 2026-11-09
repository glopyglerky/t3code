//! Codex application sessions over the ported Unix JSON-RPC process transport.
//! Protocol behavior follows the immutable T3 Code revision in upstream.lock.json.
//! Upstream protocol copyright (c) 2026 T3 Tools Inc., MIT license.

#![cfg(unix)]

use std::collections::{BTreeMap, HashSet, VecDeque};
use std::fmt;
use std::time::{Duration, Instant};

use serde::Serialize;
use serde_json::{Value, json};
use t3_code_ls_core::json as core_json;
use t3_code_ls_core::provider_process::{
    Config, Error as TransportError, Event, MessageKind, ProviderProcess, RequestId,
};

const MAX_MESSAGE_BYTES: usize = 8 * 1024 * 1024;
const MAX_BUFFER_BYTES: usize = 16 * 1024 * 1024;
const MAX_PENDING: usize = 64;
const MAX_BUFFERED_EVENTS: usize = 64;
const MAX_REQUEST_TIMEOUT: Duration = Duration::from_secs(300);

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct RpcError {
    pub code: i64,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RequestFailure {
    TimedOut,
    Cancelled,
    Disconnected,
    Transport,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SessionEvent {
    Response {
        id: Value,
        method: String,
        result: Value,
    },
    RequestError {
        id: Value,
        method: String,
        error: RpcError,
    },
    RequestFailed {
        id: Value,
        method: String,
        failure: RequestFailure,
    },
    Notification {
        method: String,
        params: Value,
    },
    ServerRequest {
        id: Value,
        method: String,
        params: Value,
    },
}

#[derive(Debug)]
pub enum SessionError {
    InvalidInput(&'static str),
    Protocol(&'static str),
    Provider(RpcError),
    Transport(TransportError),
    Closed,
}

impl fmt::Display for SessionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidInput(message) => write!(formatter, "Invalid provider request: {message}"),
            Self::Protocol(message) => write!(formatter, "Invalid provider response: {message}"),
            Self::Provider(error) => write!(
                formatter,
                "Provider error {}: {}",
                error.code, error.message
            ),
            Self::Transport(error) => write!(formatter, "Provider transport: {error}"),
            Self::Closed => formatter.write_str("Provider session is closed"),
        }
    }
}

impl std::error::Error for SessionError {}

impl From<TransportError> for SessionError {
    fn from(error: TransportError) -> Self {
        Self::Transport(error)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Method {
    Initialize,
    StartThread,
    ResumeThread,
    StartTurn,
    InterruptTurn,
}

impl Method {
    fn name(self) -> &'static str {
        match self {
            Self::Initialize => "initialize",
            Self::StartThread => "thread/start",
            Self::ResumeThread => "thread/resume",
            Self::StartTurn => "turn/start",
            Self::InterruptTurn => "turn/interrupt",
        }
    }
}

struct PendingRequest {
    id: RequestId,
    method: Method,
    thread_id: Option<String>,
}

struct ServerRequest {
    id: RequestId,
    thread_id: Option<String>,
    turn_id: Option<String>,
}

#[derive(Default)]
struct SessionState {
    pending: BTreeMap<String, PendingRequest>,
    server_requests: BTreeMap<String, ServerRequest>,
}

fn id_key(id: &RequestId) -> String {
    match id {
        RequestId::Number(value) | RequestId::String(value) => value.clone(),
    }
}

fn to_json(value: &core_json::Value) -> Result<Value, SessionError> {
    fn unique(value: &core_json::Value) -> bool {
        match value {
            core_json::Value::Object(fields) => {
                let mut keys = HashSet::new();
                fields
                    .iter()
                    .all(|(key, value)| keys.insert(key) && unique(value))
            }
            core_json::Value::Array(values) => values.iter().all(unique),
            _ => true,
        }
    }
    if !unique(value) {
        return Err(SessionError::Protocol("duplicate object key"));
    }
    serde_json::from_str(&core_json::stringify(value))
        .map_err(|_| SessionError::Protocol("JSON cannot be represented"))
}

fn to_core(value: &Value) -> Result<core_json::Value, SessionError> {
    let text = serde_json::to_string(value)
        .map_err(|_| SessionError::InvalidInput("JSON cannot be encoded"))?;
    core_json::parse_with_limits(
        &text,
        core_json::Limits {
            max_bytes: MAX_MESSAGE_BYTES,
            max_depth: 64,
            max_values: 262_144,
        },
    )
    .map_err(|_| SessionError::InvalidInput("JSON exceeds protocol limits"))
}

fn request_id(value: &Value) -> Result<RequestId, SessionError> {
    RequestId::from_value(&to_core(value)?).ok_or(SessionError::InvalidInput(
        "request ID must be a string or number",
    ))
}

fn identifier(value: Option<&Value>) -> Option<&str> {
    value.and_then(Value::as_str).filter(|value| {
        !value.is_empty() && value.len() <= 1024 && !value.chars().any(char::is_control)
    })
}

fn validate_request(method: Method, params: &Value) -> Result<Option<String>, SessionError> {
    if !params.is_object() {
        return Err(SessionError::InvalidInput("parameters must be an object"));
    }
    let thread = match method {
        Method::ResumeThread | Method::StartTurn | Method::InterruptTurn => Some(
            identifier(params.get("threadId"))
                .ok_or(SessionError::InvalidInput("threadId is required"))?
                .to_owned(),
        ),
        Method::Initialize | Method::StartThread => None,
    };
    if method == Method::StartTurn && !params.get("input").is_some_and(Value::is_array) {
        return Err(SessionError::InvalidInput("turn input must be an array"));
    }
    if method == Method::InterruptTurn && identifier(params.get("turnId")).is_none() {
        return Err(SessionError::InvalidInput("turnId is required"));
    }
    Ok(thread)
}

fn validate_result(pending: &PendingRequest, result: &Value) -> Result<(), SessionError> {
    if !result.is_object() {
        return Err(SessionError::Protocol("RPC result must be an object"));
    }
    match pending.method {
        Method::Initialize => {
            if !result
                .get("userAgent")
                .and_then(Value::as_str)
                .is_some_and(|s| !s.is_empty())
            {
                return Err(SessionError::Protocol("initialize omitted userAgent"));
            }
        }
        Method::StartThread | Method::ResumeThread => {
            let native_id = identifier(result.get("thread").and_then(|thread| thread.get("id")))
                .ok_or(SessionError::Protocol(
                    "thread response omitted its identity",
                ))?;
            if pending.method == Method::ResumeThread
                && pending.thread_id.as_deref() != Some(native_id)
            {
                return Err(SessionError::Protocol("resumed another thread"));
            }
        }
        Method::StartTurn => {
            if identifier(result.get("turn").and_then(|turn| turn.get("id"))).is_none() {
                return Err(SessionError::Protocol("turn response omitted its identity"));
            }
        }
        Method::InterruptTurn => {}
    }
    Ok(())
}

fn request_failure(error: TransportError) -> RequestFailure {
    match error {
        TransportError::TimedOut => RequestFailure::TimedOut,
        TransportError::Cancelled => RequestFailure::Cancelled,
        TransportError::Read
        | TransportError::Write
        | TransportError::InputClosed
        | TransportError::OutputClosed
        | TransportError::Shutdown => RequestFailure::Disconnected,
        _ => RequestFailure::Transport,
    }
}

impl SessionState {
    fn observe(&mut self, event: Event) -> Result<SessionEvent, SessionError> {
        let Event::Message(message) = event else {
            let Event::RequestFailed { id, error } = event else {
                unreachable!()
            };
            let pending = self
                .pending
                .remove(&id_key(&id))
                .ok_or(SessionError::Protocol("failed an untracked request"))?;
            return Ok(SessionEvent::RequestFailed {
                id: to_json(&pending.id.value())?,
                method: pending.method.name().to_owned(),
                failure: request_failure(error),
            });
        };
        let value = to_json(&message.value)?;
        match message.kind {
            MessageKind::Response(id) => {
                let pending = self
                    .pending
                    .remove(&id_key(&id))
                    .ok_or(SessionError::Protocol("responded to an untracked request"))?;
                let result = value.get("result");
                let error = value.get("error");
                if result.is_some() == error.is_some() {
                    return Err(SessionError::Protocol(
                        "response needs exactly one result or error",
                    ));
                }
                let id = to_json(&pending.id.value())?;
                let method = pending.method.name().to_owned();
                if let Some(error) = error {
                    let code = error
                        .get("code")
                        .and_then(Value::as_i64)
                        .ok_or(SessionError::Protocol("RPC error code must be an integer"))?;
                    let message = error
                        .get("message")
                        .and_then(Value::as_str)
                        .ok_or(SessionError::Protocol("RPC error omitted its message"))?;
                    return Ok(SessionEvent::RequestError {
                        id,
                        method,
                        error: RpcError {
                            code,
                            message: message.to_owned(),
                            data: error.get("data").cloned(),
                        },
                    });
                }
                let result = result.ok_or(SessionError::Protocol("response omitted its result"))?;
                validate_result(&pending, result)?;
                Ok(SessionEvent::Response {
                    id,
                    method,
                    result: result.clone(),
                })
            }
            MessageKind::Request(id) => {
                let method = identifier(value.get("method"))
                    .ok_or(SessionError::Protocol("invalid server request method"))?;
                let params = value.get("params").cloned().unwrap_or(Value::Null);
                if self.server_requests.len() >= MAX_PENDING {
                    return Err(SessionError::Protocol(
                        "too many unanswered server requests",
                    ));
                }
                let key = id_key(&id);
                if self.server_requests.contains_key(&key) {
                    return Err(SessionError::Protocol("duplicate server request ID"));
                }
                let event_id = to_json(&id.value())?;
                self.server_requests.insert(
                    key,
                    ServerRequest {
                        id,
                        thread_id: identifier(params.get("threadId")).map(str::to_owned),
                        turn_id: identifier(params.get("turnId")).map(str::to_owned),
                    },
                );
                Ok(SessionEvent::ServerRequest {
                    id: event_id,
                    method: method.to_owned(),
                    params,
                })
            }
            MessageKind::Notification => {
                let method = identifier(value.get("method"))
                    .ok_or(SessionError::Protocol("invalid notification method"))?;
                let params = value.get("params").cloned().unwrap_or(Value::Null);
                if method == "serverRequest/resolved" {
                    let id = params
                        .get("requestId")
                        .ok_or(SessionError::Protocol("resolved request omitted its ID"))?;
                    let id = request_id(id).map_err(|_| {
                        SessionError::Protocol("resolved request has an invalid ID")
                    })?;
                    self.server_requests.remove(&id_key(&id));
                } else if method == "turn/completed" {
                    let thread = identifier(params.get("threadId"));
                    let turn = identifier(params.get("turn").and_then(|turn| turn.get("id")));
                    if let (Some(thread), Some(turn)) = (thread, turn) {
                        self.server_requests.retain(|_, pending| {
                            pending.thread_id.as_deref() != Some(thread)
                                || pending.turn_id.as_deref() != Some(turn)
                        });
                    }
                }
                Ok(SessionEvent::Notification {
                    method: method.to_owned(),
                    params,
                })
            }
        }
    }
}

pub struct CodexSession {
    process: ProviderProcess,
    state: SessionState,
    buffered: VecDeque<(SessionEvent, usize)>,
    buffered_bytes: usize,
    initialization: Value,
    closed: bool,
}

impl CodexSession {
    /// Configuration comes from the host, including its authorized credential environment.
    pub fn spawn(config: Config) -> Result<Self, SessionError> {
        Self::spawn_tracked(config).map_err(|(error, _)| error)
    }

    /// The boolean reports confirmed cleanup on failure, not merely a dropped handle.
    pub(crate) fn spawn_tracked(mut config: Config) -> Result<Self, (SessionError, bool)> {
        if config.limits.request_timeout.is_zero()
            || config.limits.request_timeout > MAX_REQUEST_TIMEOUT
        {
            return Err((
                SessionError::InvalidInput("request timeout must be in (0, 300 seconds]"),
                true,
            ));
        }
        config.track_requests = true;
        config.limits.max_frame_bytes = config.limits.max_frame_bytes.min(MAX_MESSAGE_BYTES);
        config.limits.max_pending_requests = config.limits.max_pending_requests.min(MAX_PENDING);
        config.limits.max_queued_messages =
            config.limits.max_queued_messages.min(MAX_BUFFERED_EVENTS);
        if !config
            .environment
            .iter()
            .any(|(name, _)| name == "CODEX_INTERNAL_ORIGINATOR_OVERRIDE")
        {
            // Upstream uses this identity for its provider-specific model catalog.
            config.environment.push((
                "CODEX_INTERNAL_ORIGINATOR_OVERRIDE".into(),
                "T3 Code".into(),
            ));
        }
        let timeout = config.limits.request_timeout;
        let mut session = Self {
            process: ProviderProcess::spawn(config)
                .map_err(|error| (SessionError::Transport(error), false))?,
            state: SessionState::default(),
            buffered: VecDeque::new(),
            buffered_bytes: 0,
            initialization: Value::Null,
            closed: false,
        };
        match session.initialize(timeout) {
            Ok(()) => Ok(session),
            Err(error) => {
                let stopped = session.shutdown().is_ok();
                Err((error, stopped))
            }
        }
    }

    fn initialize(&mut self, timeout: Duration) -> Result<(), SessionError> {
        let id = self.send(Method::Initialize, json!({
            "clientInfo": { "name": "T3 Code", "title": "T3 Code", "version": env!("CARGO_PKG_VERSION") },
            "capabilities": { "experimentalApi": true }
        }))?;
        let deadline = Instant::now() + timeout;
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(SessionError::Transport(TransportError::TimedOut));
            }
            let Some(event) = self.read(remaining)? else {
                return Err(SessionError::Transport(TransportError::TimedOut));
            };
            match event {
                SessionEvent::Response {
                    id: response_id,
                    result,
                    ..
                } if response_id == id => {
                    self.initialization = result;
                    self.process.notify("initialized", None)?;
                    return Ok(());
                }
                SessionEvent::RequestError {
                    id: response_id,
                    error,
                    ..
                } if response_id == id => {
                    return Err(SessionError::Provider(error));
                }
                SessionEvent::RequestFailed {
                    id: response_id,
                    failure,
                    ..
                } if response_id == id => {
                    return Err(SessionError::Transport(match failure {
                        RequestFailure::TimedOut => TransportError::TimedOut,
                        RequestFailure::Cancelled => TransportError::Cancelled,
                        RequestFailure::Disconnected => TransportError::OutputClosed,
                        RequestFailure::Transport => TransportError::InvalidMessage,
                    }));
                }
                event => self.buffer(event)?,
            }
        }
    }

    pub fn initialization(&self) -> &Value {
        &self.initialization
    }

    pub fn process_id(&self) -> u32 {
        self.process.id()
    }

    pub fn pending_requests(&self) -> usize {
        self.state.pending.len()
    }

    pub fn start_thread(&mut self, params: Value) -> Result<Value, SessionError> {
        self.send(Method::StartThread, params)
    }

    pub fn resume_thread(&mut self, params: Value) -> Result<Value, SessionError> {
        self.send(Method::ResumeThread, params)
    }

    pub fn start_turn(&mut self, params: Value) -> Result<Value, SessionError> {
        self.send(Method::StartTurn, params)
    }

    pub fn interrupt_turn(
        &mut self,
        thread_id: &str,
        turn_id: &str,
    ) -> Result<Value, SessionError> {
        self.send(
            Method::InterruptTurn,
            json!({ "threadId": thread_id, "turnId": turn_id }),
        )
    }

    fn send(&mut self, method: Method, params: Value) -> Result<Value, SessionError> {
        if self.closed {
            return Err(SessionError::Closed);
        }
        if self.state.pending.len() >= MAX_PENDING {
            return Err(SessionError::InvalidInput("too many outstanding requests"));
        }
        let thread_id = validate_request(method, &params)?;
        let params = to_core(&params)?;
        let id = self.process.request(method.name(), Some(params))?;
        let result = to_json(&id.value())?;
        self.state.pending.insert(
            id_key(&id),
            PendingRequest {
                id,
                method,
                thread_id,
            },
        );
        Ok(result)
    }

    /// Local cancellation does not stop provider work; use turn/interrupt for that.
    pub fn cancel(&mut self, id: &Value) -> Result<bool, SessionError> {
        if self.closed {
            return Err(SessionError::Closed);
        }
        let id = request_id(id)?;
        let Some(pending) = self.state.pending.get(&id_key(&id)) else {
            return Ok(false);
        };
        Ok(self.process.cancel_request(&pending.id)?)
    }

    pub fn poll(&mut self, timeout: Duration) -> Result<Option<SessionEvent>, SessionError> {
        if self.closed {
            return Err(SessionError::Closed);
        }
        if timeout > Duration::from_secs(60) {
            return Err(SessionError::InvalidInput(
                "poll timeout exceeds 60 seconds",
            ));
        }
        if let Some((event, bytes)) = self.buffered.pop_front() {
            self.buffered_bytes -= bytes;
            return Ok(Some(event));
        }
        self.read(timeout)
    }

    fn read(&mut self, timeout: Duration) -> Result<Option<SessionEvent>, SessionError> {
        let result = self
            .process
            .recv_timeout(timeout)
            .map_err(SessionError::Transport)
            .and_then(|event| event.map(|event| self.state.observe(event)).transpose());
        if result.is_err() {
            let _ = self.shutdown();
        }
        result
    }

    fn buffer(&mut self, event: SessionEvent) -> Result<(), SessionError> {
        let bytes = serde_json::to_vec(&event)
            .map_err(|_| SessionError::Protocol("event could not be encoded"))?
            .len();
        if self.buffered.len() >= MAX_BUFFERED_EVENTS
            || bytes > MAX_BUFFER_BYTES.saturating_sub(self.buffered_bytes)
        {
            return Err(SessionError::Protocol(
                "too much traffic before initialization",
            ));
        }
        self.buffered.push_back((event, bytes));
        self.buffered_bytes += bytes;
        Ok(())
    }

    pub fn respond(&mut self, id: &Value, result: Value) -> Result<(), SessionError> {
        if self.closed {
            return Err(SessionError::Closed);
        }
        let key = id_key(&request_id(id)?);
        let request = self
            .state
            .server_requests
            .get(&key)
            .ok_or(SessionError::InvalidInput(
                "server request is no longer pending",
            ))?;
        self.process
            .respond(request.id.clone(), to_core(&result)?)?;
        self.state.server_requests.remove(&key);
        Ok(())
    }

    pub fn reject(&mut self, id: &Value, code: i64, message: &str) -> Result<(), SessionError> {
        if self.closed {
            return Err(SessionError::Closed);
        }
        if message.len() > 4096 {
            return Err(SessionError::InvalidInput(
                "rejection message exceeds 4096 bytes",
            ));
        }
        let key = id_key(&request_id(id)?);
        let request = self
            .state
            .server_requests
            .get(&key)
            .ok_or(SessionError::InvalidInput(
                "server request is no longer pending",
            ))?;
        self.process
            .respond_error(request.id.clone(), code, message)?;
        self.state.server_requests.remove(&key);
        Ok(())
    }

    pub fn shutdown(&mut self) -> Result<(), SessionError> {
        self.closed = true;
        self.state.pending.clear();
        self.state.server_requests.clear();
        self.buffered.clear();
        self.buffered_bytes = 0;
        self.process.shutdown()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use t3_code_ls_core::provider_process::Message;

    fn incoming(value: Value) -> Event {
        Event::Message(Message::decode(to_core(&value).unwrap()).unwrap())
    }

    fn pending(state: &mut SessionState, method: Method, thread: Option<&str>) {
        let id = RequestId::Number("1".into());
        state.pending.insert(
            id_key(&id),
            PendingRequest {
                id,
                method,
                thread_id: thread.map(str::to_owned),
            },
        );
    }

    #[test]
    fn correlates_responses_and_preserves_provider_rejections() {
        let mut state = SessionState::default();
        pending(&mut state, Method::StartThread, None);
        let result = json!({ "thread": { "id": "native-1" }, "model": "selected-model" });
        assert_eq!(
            state
                .observe(incoming(json!({ "id": "1", "result": result })))
                .unwrap(),
            SessionEvent::Response {
                id: json!(1),
                method: "thread/start".into(),
                result
            }
        );
        assert!(state.pending.is_empty());
        assert!(
            state
                .observe(incoming(json!({ "id": 1, "result": {} })))
                .is_err()
        );
        pending(&mut state, Method::StartTurn, Some("native-1"));
        assert_eq!(
            state.observe(incoming(json!({ "id": 1, "error": { "code": -32602, "message": "invalid model", "data": {"field":"model"} } }))).unwrap(),
            SessionEvent::RequestError {
                id: json!(1), method: "turn/start".into(),
                error: RpcError { code: -32602, message: "invalid model".into(), data: Some(json!({"field":"model"})) }
            }
        );
        assert!(state.pending.is_empty());
    }

    #[test]
    fn malformed_or_cross_thread_results_cannot_bind_a_session() {
        for (method, thread, result) in [
            (Method::Initialize, None, json!({})),
            (Method::StartThread, None, json!({"thread": {"id": ""}})),
            (
                Method::ResumeThread,
                Some("wanted"),
                json!({"thread": {"id": "another"}}),
            ),
            (Method::StartTurn, Some("wanted"), json!({"turn": {}})),
            (Method::InterruptTurn, Some("wanted"), Value::Null),
        ] {
            let mut state = SessionState::default();
            pending(&mut state, method, thread);
            assert!(
                state
                    .observe(incoming(json!({"id": 1, "result": result})))
                    .is_err()
            );
        }
        for response in [
            json!({"id":1}),
            json!({"id":1,"result":{},"error":{"code":1,"message":"ambiguous"}}),
            json!({"id":1,"error":{"code":1.5,"message":"non-integral"}}),
        ] {
            let mut state = SessionState::default();
            pending(&mut state, Method::StartThread, None);
            assert!(state.observe(incoming(response)).is_err());
        }
        let duplicate =
            core_json::parse(r#"{"id":1,"result":{"thread":{"id":"a","id":"b"}}}"#).unwrap();
        let mut state = SessionState::default();
        pending(&mut state, Method::StartThread, None);
        assert!(
            state
                .observe(Event::Message(Message::decode(duplicate).unwrap()))
                .is_err()
        );
    }

    #[test]
    fn resolved_and_completed_requests_cannot_be_answered_later() {
        let mut state = SessionState::default();
        let request = |id, turn| {
            json!({
                "id": id, "method": "item/commandExecution/requestApproval",
                "params": {"threadId":"native-1","turnId":turn,"command":"needs approval"}
            })
        };
        assert!(matches!(
            state.observe(incoming(request(10, "turn-1"))).unwrap(),
            SessionEvent::ServerRequest { .. }
        ));
        state.observe(incoming(request(11, "turn-2"))).unwrap();
        assert!(state.observe(incoming(request(11, "turn-2"))).is_err());
        state
            .observe(incoming(
                json!({"method":"serverRequest/resolved","params":{"requestId":"10"}}),
            ))
            .unwrap();
        assert!(!state.server_requests.contains_key("10"));
        assert!(state.server_requests.contains_key("11"));
        state.observe(incoming(json!({"method":"turn/completed","params":{"threadId":"another","turn":{"id":"turn-2"}}}))).unwrap();
        assert!(state.server_requests.contains_key("11"));
        state.observe(incoming(json!({"method":"turn/completed","params":{"threadId":"native-1","turn":{"id":"turn-2"}}}))).unwrap();
        assert!(state.server_requests.is_empty());
    }

    #[test]
    fn request_failures_retire_methods_without_reporting_provider_success() {
        for (error, failure) in [
            (TransportError::TimedOut, RequestFailure::TimedOut),
            (TransportError::Cancelled, RequestFailure::Cancelled),
            (TransportError::OutputClosed, RequestFailure::Disconnected),
            (TransportError::InvalidMessage, RequestFailure::Transport),
        ] {
            let mut state = SessionState::default();
            pending(&mut state, Method::StartTurn, Some("native-1"));
            assert_eq!(
                state
                    .observe(Event::RequestFailed {
                        id: RequestId::Number("1".into()),
                        error
                    })
                    .unwrap(),
                SessionEvent::RequestFailed {
                    id: json!(1),
                    method: "turn/start".into(),
                    failure
                }
            );
            assert!(state.pending.is_empty());
        }
    }

    #[test]
    fn rejects_malformed_client_requests_before_transmission() {
        for (method, request) in [
            (Method::StartThread, Value::Null),
            (Method::ResumeThread, json!({})),
            (Method::StartTurn, json!({"threadId":"n","input":"text"})),
            (Method::InterruptTurn, json!({"threadId":"n","turnId":""})),
            (
                Method::InterruptTurn,
                json!({"threadId":"n\n","turnId":"t"}),
            ),
        ] {
            assert!(validate_request(method, &request).is_err());
        }
        assert_eq!(validate_request(Method::StartTurn, &json!({"threadId":"native-1","input":[{"type":"text","text":"hello","text_elements":[]}]})).unwrap(), Some("native-1".into()));
    }
}
