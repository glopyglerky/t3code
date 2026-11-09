//! Codex app-server JSONL transport, ported from effect-codex-app-server/protocol.ts.
//! Copyright (c) 2026 T3 Tools Inc. Distributed under the repository's MIT license.

use crate::json::{self, Value};
use std::collections::VecDeque;
use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::Shutdown;
use std::os::fd::OwnedFd;
use std::os::unix::net::UnixStream;
use std::path::PathBuf;
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::mpsc::{self, SyncSender, TrySendError};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

pub const MAX_FRAME_BYTES: usize = 128 * 1024 * 1024;
const STDERR_TAIL_BYTES: usize = 8192;
const MAX_SAFE_ID: i64 = 9_007_199_254_740_991;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    InvalidConfig,
    Spawn,
    Read,
    Write,
    FrameTooLarge,
    InvalidUtf8,
    InvalidJson,
    InvalidMessage,
    DuplicateId,
    QueueFull,
    TooManyRequests,
    InputClosed,
    OutputClosed,
    TimedOut,
    Cancelled,
    Shutdown,
    WorkerFailed,
}

impl std::fmt::Display for Error {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::InvalidConfig => "invalid provider process configuration",
            Self::Spawn => "could not start the provider process",
            Self::Read => "could not read provider output",
            Self::Write => "could not write provider input",
            Self::FrameTooLarge => "provider message exceeds the byte limit",
            Self::InvalidUtf8 => "provider message is not UTF-8",
            Self::InvalidJson => "provider message is not valid bounded JSON",
            Self::InvalidMessage => "provider message has an invalid JSON-RPC envelope",
            Self::DuplicateId => "request ID is already active",
            Self::QueueFull => "provider message queue is full",
            Self::TooManyRequests => "provider request limit reached",
            Self::InputClosed => "provider input is closed",
            Self::OutputClosed => "provider output is closed",
            Self::TimedOut => "provider request timed out",
            Self::Cancelled => "provider request was cancelled locally",
            Self::Shutdown => "provider process was shut down",
            Self::WorkerFailed => "provider transport worker failed",
        })
    }
}

impl std::error::Error for Error {}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RequestId {
    Number(String),
    String(String),
}

impl RequestId {
    pub fn from_value(value: &Value) -> Option<Self> {
        match value {
            Value::String(text) => Some(Self::String(text.clone())),
            Value::Number(text) => {
                let number: f64 = text.parse().ok()?;
                if !number.is_finite() {
                    return None;
                }
                Some(Self::Number(
                    ryu_js::Buffer::new().format(number).to_owned(),
                ))
            }
            _ => None,
        }
    }

    pub fn value(&self) -> Value {
        match self {
            Self::Number(number) => Value::Number(number.clone()),
            Self::String(text) => Value::String(text.clone()),
        }
    }

    // Upstream keys pending client requests with String(response.id).
    fn same_client_id(&self, other: &Self) -> bool {
        match (self, other) {
            (
                Self::Number(left) | Self::String(left),
                Self::Number(right) | Self::String(right),
            ) => left == right,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MessageKind {
    Request(RequestId),
    Response(RequestId),
    Notification,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Message {
    pub kind: MessageKind,
    pub value: Value,
}

impl Message {
    pub fn decode(value: Value) -> Result<Self, Error> {
        let id = value.get("id");
        let method = value.get("method").and_then(Value::as_str);
        let kind = if method.is_some() {
            match id {
                Some(value) => {
                    MessageKind::Request(RequestId::from_value(value).ok_or(Error::InvalidMessage)?)
                }
                None => MessageKind::Notification,
            }
        } else {
            let id = RequestId::from_value(id.ok_or(Error::InvalidMessage)?)
                .ok_or(Error::InvalidMessage)?;
            if let Some(error) = value.get("error")
                && (!matches!(error.get("code"), Some(Value::Number(_)))
                    || error.get("message").and_then(Value::as_str).is_none())
            {
                return Err(Error::InvalidMessage);
            }
            MessageKind::Response(id)
        };
        Ok(Self { kind, value })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Event {
    Message(Message),
    RequestFailed { id: RequestId, error: Error },
}

#[derive(Clone, Debug)]
pub struct Limits {
    pub max_frame_bytes: usize,
    pub max_pending_requests: usize,
    pub max_queued_messages: usize,
    pub request_timeout: Duration,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_frame_bytes: MAX_FRAME_BYTES,
            max_pending_requests: 32,
            max_queued_messages: 32,
            request_timeout: Duration::from_secs(30),
        }
    }
}

#[derive(Clone, Debug)]
pub struct Config {
    pub program: PathBuf,
    pub args: Vec<String>,
    pub cwd: PathBuf,
    pub home: Option<PathBuf>,
    pub inherit_environment: bool,
    pub inherit_stderr: bool,
    /// Disable when the caller owns request correlation and cancellation.
    pub track_requests: bool,
    pub environment: Vec<(String, String)>,
    pub limits: Limits,
}

impl Config {
    fn validate(&self) -> Result<(), Error> {
        if self.program.as_os_str().is_empty()
            || (!self.inherit_environment && (!self.program.is_absolute() || self.home.is_none()))
            || !self.cwd.is_absolute()
            || !self.cwd.is_dir()
            || self
                .home
                .as_ref()
                .is_some_and(|home| !home.is_absolute() || !home.is_dir())
            || self.limits.max_frame_bytes == 0
            || self.limits.max_frame_bytes > MAX_FRAME_BYTES
            || self.limits.max_pending_requests == 0
            || self.limits.max_pending_requests > 1024
            || self.limits.max_queued_messages == 0
            || self.limits.max_queued_messages > 1024
            || self.limits.request_timeout > Duration::from_secs(86400)
            || self.environment.iter().any(|(key, _)| {
                key.is_empty() || key == "HOME" || key.contains('=') || key.contains('\0')
            })
        {
            return Err(Error::InvalidConfig);
        }
        Ok(())
    }
}

struct Pending {
    id: RequestId,
    deadline: Option<Instant>,
}

struct State {
    events: VecDeque<Event>,
    pending: Vec<Pending>,
    retired: Vec<RequestId>,
    server_requests: Vec<RequestId>,
    next_id: i64,
    terminal: Option<Error>,
}

struct Shared {
    state: Mutex<State>,
    ready: Condvar,
    stderr: Mutex<Vec<u8>>,
    limits: Limits,
    track_requests: bool,
}

impl Shared {
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|error| error.into_inner())
    }

    fn terminate(&self, error: Error) {
        let mut state = self.lock();
        if state.terminal.is_none() {
            state.terminal = Some(error);
        }
        state.server_requests.clear();
        self.ready.notify_all();
    }

    fn enqueue(&self, state: &mut State, event: Event) -> Result<(), Error> {
        if state.events.len() >= self.limits.max_queued_messages {
            state.terminal = Some(Error::QueueFull);
            state.server_requests.clear();
            self.ready.notify_all();
            return Err(Error::QueueFull);
        }
        state.events.push_back(event);
        self.ready.notify_all();
        Ok(())
    }

    fn route(&self, message: Message) -> Result<(), Error> {
        let mut state = self.lock();
        while state.events.len() >= self.limits.max_queued_messages && state.terminal.is_none() {
            state = self
                .ready
                .wait(state)
                .unwrap_or_else(|error| error.into_inner());
        }
        if let Some(error) = state.terminal {
            return Err(error);
        }
        if !self.track_requests {
            return self.enqueue(&mut state, Event::Message(message));
        }
        match &message.kind {
            MessageKind::Response(id) => {
                let Some(index) = state
                    .pending
                    .iter()
                    .position(|pending| pending.id.same_client_id(id))
                else {
                    state.retired.retain(|retired| !retired.same_client_id(id));
                    return Ok(());
                };
                let pending = state.pending.remove(index);
                if pending
                    .deadline
                    .is_some_and(|deadline| deadline <= Instant::now())
                {
                    return self.enqueue(
                        &mut state,
                        Event::RequestFailed {
                            id: pending.id,
                            error: Error::TimedOut,
                        },
                    );
                }
            }
            MessageKind::Request(id) => {
                if state
                    .server_requests
                    .iter()
                    .any(|request| request.same_client_id(id))
                {
                    return Err(Error::DuplicateId);
                }
                if state.server_requests.len() >= self.limits.max_pending_requests {
                    return Err(Error::TooManyRequests);
                }
                state.server_requests.push(id.clone());
            }
            MessageKind::Notification => {
                if message.value.get("method").and_then(Value::as_str)
                    == Some("serverRequest/resolved")
                    && let Some(id) = message
                        .value
                        .get("params")
                        .and_then(|params| params.get("requestId"))
                        .and_then(RequestId::from_value)
                {
                    state
                        .server_requests
                        .retain(|request| !request.same_client_id(&id));
                }
            }
        }
        self.enqueue(&mut state, Event::Message(message))
    }
}

pub struct ProviderProcess {
    shared: Arc<Shared>,
    outgoing: Option<SyncSender<String>>,
    child: Child,
    sockets: Vec<UnixStream>,
    workers: Vec<JoinHandle<()>>,
    stopped: bool,
}

impl ProviderProcess {
    pub fn spawn(config: Config) -> Result<Self, Error> {
        config.validate()?;
        let (mut input, child_input) = UnixStream::pair().map_err(|_| Error::Spawn)?;
        let (output, child_output) = UnixStream::pair().map_err(|_| Error::Spawn)?;
        let (mut errors, child_errors) = UnixStream::pair().map_err(|_| Error::Spawn)?;
        let sockets = vec![
            input.try_clone().map_err(|_| Error::Spawn)?,
            output.try_clone().map_err(|_| Error::Spawn)?,
            errors.try_clone().map_err(|_| Error::Spawn)?,
        ];
        let mut command = Command::new(&config.program);
        command
            .args(&config.args)
            .current_dir(&config.cwd)
            .stdin(Stdio::from(OwnedFd::from(child_input)))
            .stdout(Stdio::from(OwnedFd::from(child_output)))
            .stderr(if config.inherit_stderr {
                Stdio::inherit()
            } else {
                Stdio::from(OwnedFd::from(child_errors))
            });
        if config.inherit_environment {
            if let Some(home) = &config.home {
                command.env("HOME", home);
            }
        } else {
            let home = config.home.as_ref().ok_or(Error::InvalidConfig)?;
            command
                .env_clear()
                .env("HOME", home)
                .env("CODEX_HOME", home.join(".codex"))
                .env("XDG_CONFIG_HOME", home.join(".config"))
                .env("XDG_CACHE_HOME", home.join(".cache"))
                .env("XDG_STATE_HOME", home.join(".local/state"));
        }
        for (key, value) in &config.environment {
            command.env(key, value);
        }
        let child = command.spawn().map_err(|_| Error::Spawn)?;
        // Command retains its own descriptor copies until it is dropped.
        drop(command);
        let shared = Arc::new(Shared {
            state: Mutex::new(State {
                events: VecDeque::new(),
                pending: Vec::new(),
                retired: Vec::new(),
                server_requests: Vec::new(),
                next_id: 1,
                terminal: None,
            }),
            ready: Condvar::new(),
            stderr: Mutex::new(Vec::new()),
            limits: config.limits,
            track_requests: config.track_requests,
        });
        let (sender, receiver) = mpsc::sync_channel::<String>(shared.limits.max_queued_messages);
        let mut process = Self {
            shared: shared.clone(),
            outgoing: Some(sender),
            child,
            sockets,
            workers: Vec::new(),
            stopped: false,
        };
        let writer_state = shared.clone();
        process.workers.push(
            thread::Builder::new()
                .name("t3ls-provider-input".to_owned())
                .spawn(move || {
                    while let Ok(frame) = receiver.recv() {
                        if writer_state.lock().terminal.is_some() {
                            break;
                        }
                        if input.write_all(frame.as_bytes()).is_err() {
                            writer_state.terminate(Error::Write);
                            break;
                        }
                    }
                    let _ = input.shutdown(Shutdown::Write);
                })
                .map_err(|_| Error::Spawn)?,
        );
        let reader_state = shared.clone();
        process.workers.push(
            thread::Builder::new()
                .name("t3ls-provider-output".to_owned())
                .spawn(move || {
                    let mut reader = BufReader::new(output);
                    loop {
                        let frame =
                            match read_frame(&mut reader, reader_state.limits.max_frame_bytes) {
                                Ok(Some(frame)) if is_blank_frame(&frame) => continue,
                                Ok(Some(frame)) => frame,
                                Ok(None) => {
                                    reader_state.terminate(Error::OutputClosed);
                                    break;
                                }
                                Err(error) => {
                                    reader_state.terminate(error);
                                    break;
                                }
                            };
                        let result = decode_frame(&frame, reader_state.limits.max_frame_bytes)
                            .and_then(|message| reader_state.route(message));
                        if let Err(error) = result {
                            reader_state.terminate(error);
                            break;
                        }
                    }
                })
                .map_err(|_| Error::Spawn)?,
        );
        if !config.inherit_stderr {
            process.workers.push(
                thread::Builder::new()
                    .name("t3ls-provider-stderr".to_owned())
                    .spawn(move || {
                        let mut buffer = [0u8; 4096];
                        loop {
                            match errors.read(&mut buffer) {
                                Ok(0) => break,
                                Ok(count) => {
                                    let mut tail = shared
                                        .stderr
                                        .lock()
                                        .unwrap_or_else(|error| error.into_inner());
                                    tail.extend_from_slice(&buffer[..count]);
                                    if tail.len() > STDERR_TAIL_BYTES {
                                        let remove = tail.len() - STDERR_TAIL_BYTES;
                                        tail.drain(..remove);
                                    }
                                }
                                Err(error) if error.kind() == io::ErrorKind::Interrupted => {
                                    continue;
                                }
                                Err(_) => break,
                            }
                        }
                    })
                    .map_err(|_| Error::Spawn)?,
            );
        }
        Ok(process)
    }

    pub fn id(&self) -> u32 {
        self.child.id()
    }

    pub fn pending_requests(&self) -> usize {
        self.shared.lock().pending.len()
    }

    pub fn stderr_tail(&self) -> String {
        let tail = self
            .shared
            .stderr
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        String::from_utf8_lossy(&tail).into_owned()
    }

    pub fn send(&self, value: Value) -> Result<(), Error> {
        let message = Message::decode(value)?;
        let mut encoded = json::stringify(&message.value);
        if encoded.len() > self.shared.limits.max_frame_bytes {
            return Err(Error::FrameTooLarge);
        }
        encoded.push('\n');
        let mut state = self.shared.lock();
        if let Some(error) = state.terminal {
            return Err(error);
        }
        let sender = self.outgoing.as_ref().ok_or(Error::InputClosed)?;
        if self.shared.track_requests
            && let MessageKind::Request(id) = &message.kind
        {
            if state
                .pending
                .iter()
                .any(|pending| pending.id.same_client_id(id))
                || state
                    .retired
                    .iter()
                    .any(|retired| retired.same_client_id(id))
            {
                return Err(Error::DuplicateId);
            }
            if state.pending.len() >= self.shared.limits.max_pending_requests {
                return Err(Error::TooManyRequests);
            }
        }
        sender.try_send(encoded).map_err(|error| match error {
            TrySendError::Full(_) => Error::QueueFull,
            TrySendError::Disconnected(_) => Error::InputClosed,
        })?;
        if self.shared.track_requests {
            match message.kind {
                MessageKind::Request(id) => state.pending.push(Pending {
                    id,
                    deadline: if self.shared.limits.request_timeout.is_zero() {
                        None
                    } else {
                        Some(Instant::now() + self.shared.limits.request_timeout)
                    },
                }),
                MessageKind::Response(id) => state
                    .server_requests
                    .retain(|request| !request.same_client_id(&id)),
                MessageKind::Notification => {}
            }
        }
        self.shared.ready.notify_all();
        Ok(())
    }

    pub fn request(&self, method: &str, params: Option<Value>) -> Result<RequestId, Error> {
        let id = {
            let mut state = self.shared.lock();
            if state.next_id > MAX_SAFE_ID {
                return Err(Error::TooManyRequests);
            }
            let id = RequestId::Number(state.next_id.to_string());
            state.next_id += 1;
            id
        };
        let mut fields = vec![
            ("id".to_owned(), id.value()),
            ("method".to_owned(), Value::String(method.to_owned())),
        ];
        if let Some(params) = params {
            fields.push(("params".to_owned(), params));
        }
        self.send(Value::Object(fields))?;
        Ok(id)
    }

    pub fn notify(&self, method: &str, params: Option<Value>) -> Result<(), Error> {
        let mut fields = vec![("method".to_owned(), Value::String(method.to_owned()))];
        if let Some(params) = params {
            fields.push(("params".to_owned(), params));
        }
        self.send(Value::Object(fields))
    }

    pub fn respond(&self, id: RequestId, result: Value) -> Result<(), Error> {
        self.send(Value::Object(vec![
            ("id".to_owned(), id.value()),
            ("result".to_owned(), result),
        ]))
    }

    pub fn respond_error(&self, id: RequestId, code: i64, message: &str) -> Result<(), Error> {
        self.send(error_response(&id, code, message))
    }

    /// Cancels the local wait; stopping a Codex turn requires its turn/interrupt request.
    pub fn cancel_request(&self, id: &RequestId) -> Result<bool, Error> {
        let mut state = self.shared.lock();
        let Some(index) = state
            .pending
            .iter()
            .position(|pending| pending.id.same_client_id(id))
        else {
            return Ok(false);
        };
        if state.events.len() >= self.shared.limits.max_queued_messages {
            state.terminal = Some(Error::QueueFull);
            state.server_requests.clear();
            self.shared.ready.notify_all();
            return Err(Error::QueueFull);
        }
        let request = state.pending.remove(index);
        if state.retired.len() >= self.shared.limits.max_pending_requests {
            state.pending.insert(index, request);
            return Err(Error::TooManyRequests);
        }
        state.retired.push(request.id.clone());
        self.shared.enqueue(
            &mut state,
            Event::RequestFailed {
                id: request.id,
                error: Error::Cancelled,
            },
        )?;
        Ok(true)
    }

    pub fn recv_timeout(&self, timeout: Duration) -> Result<Option<Event>, Error> {
        let deadline = Instant::now()
            .checked_add(timeout)
            .ok_or(Error::InvalidConfig)?;
        let mut state = self.shared.lock();
        loop {
            let now = Instant::now();
            if state.terminal.is_none()
                && let Some(index) = state
                    .pending
                    .iter()
                    .position(|pending| pending.deadline.is_some_and(|deadline| deadline <= now))
            {
                if state.retired.len() >= self.shared.limits.max_pending_requests {
                    state.terminal = Some(Error::TooManyRequests);
                    state.server_requests.clear();
                } else {
                    let pending = state.pending.remove(index);
                    state.retired.push(pending.id.clone());
                    return Ok(Some(Event::RequestFailed {
                        id: pending.id,
                        error: Error::TimedOut,
                    }));
                }
            }
            if let Some(event) = state.events.pop_front() {
                self.shared.ready.notify_all();
                return Ok(Some(event));
            }
            if let Some(error) = state.terminal {
                if !state.pending.is_empty() {
                    let pending = state.pending.remove(0);
                    return Ok(Some(Event::RequestFailed {
                        id: pending.id,
                        error,
                    }));
                }
                return Err(error);
            }
            if now >= deadline {
                return Ok(None);
            }
            let next = state
                .pending
                .iter()
                .filter_map(|pending| pending.deadline)
                .min()
                .unwrap_or(deadline);
            let wait = next.min(deadline).saturating_duration_since(now);
            let (updated, _) = self
                .shared
                .ready
                .wait_timeout(state, wait)
                .unwrap_or_else(|error| error.into_inner());
            state = updated;
        }
    }

    /// Delivers already accepted frames before closing the provider's stdin.
    pub fn close_input(&mut self) {
        self.outgoing.take();
    }

    pub fn try_wait(&mut self) -> Result<Option<ExitStatus>, Error> {
        self.child.try_wait().map_err(|_| Error::Read)
    }

    pub fn wait_for_exit(&mut self, timeout: Duration) -> Result<Option<ExitStatus>, Error> {
        let deadline = Instant::now()
            .checked_add(timeout)
            .ok_or(Error::InvalidConfig)?;
        loop {
            if let Some(status) = self.try_wait()? {
                return Ok(Some(status));
            }
            let now = Instant::now();
            if now >= deadline {
                return Ok(None);
            }
            let wait = deadline
                .saturating_duration_since(now)
                .min(Duration::from_millis(10));
            let state = self.shared.lock();
            drop(
                self.shared
                    .ready
                    .wait_timeout(state, wait)
                    .unwrap_or_else(|error| error.into_inner()),
            );
        }
    }

    pub fn shutdown(&mut self) -> Result<ExitStatus, Error> {
        self.shared.terminate(Error::Shutdown);
        self.outgoing.take();
        // Socket shutdown also releases workers when descendants retain their stdio.
        for socket in &self.sockets {
            let _ = socket.shutdown(Shutdown::Both);
        }
        let status = match self.child.try_wait() {
            Ok(Some(status)) => Ok(status),
            Ok(None) => {
                let _ = self.child.kill();
                self.child.wait().map_err(|_| Error::Read)
            }
            Err(_) => Err(Error::Read),
        };
        self.stopped = true;
        let mut failed = false;
        while let Some(worker) = self.workers.pop() {
            if worker.join().is_err() {
                failed = true;
            }
        }
        if failed {
            Err(Error::WorkerFailed)
        } else {
            status
        }
    }
}

impl Drop for ProviderProcess {
    fn drop(&mut self) {
        if !self.stopped {
            let _ = self.shutdown();
        }
    }
}

pub fn decode_frame(frame: &str, max_bytes: usize) -> Result<Message, Error> {
    let value = json::parse_with_limits(
        frame,
        json::Limits {
            max_bytes,
            ..json::Limits::default()
        },
    )
    .map_err(|_| Error::InvalidJson)?;
    Message::decode(value)
}

pub fn error_response(id: &RequestId, code: i64, message: &str) -> Value {
    Value::Object(vec![
        ("id".to_owned(), id.value()),
        (
            "error".to_owned(),
            Value::Object(vec![
                ("code".to_owned(), Value::Number(code.to_string())),
                ("message".to_owned(), Value::String(message.to_owned())),
            ]),
        ),
    ])
}

pub fn read_frame<R: BufRead>(reader: &mut R, maximum: usize) -> Result<Option<String>, Error> {
    let mut bytes = Vec::new();
    loop {
        let available = reader.fill_buf().map_err(|_| Error::Read)?;
        if available.is_empty() {
            return if bytes.is_empty() {
                Ok(None)
            } else {
                String::from_utf8(bytes)
                    .map(Some)
                    .map_err(|_| Error::InvalidUtf8)
            };
        }
        let newline = available.iter().position(|byte| *byte == b'\n');
        let count = newline.unwrap_or(available.len());
        if count > maximum.saturating_sub(bytes.len()) {
            return Err(Error::FrameTooLarge);
        }
        bytes.extend_from_slice(&available[..count]);
        reader.consume(count + usize::from(newline.is_some()));
        if newline.is_some() {
            return String::from_utf8(bytes)
                .map(Some)
                .map_err(|_| Error::InvalidUtf8);
        }
    }
}

pub fn is_blank_frame(frame: &str) -> bool {
    frame.chars().all(|character| {
        matches!(character,
            '\u{0009}'..='\u{000d}' | '\u{0020}' | '\u{00a0}' | '\u{1680}'
            | '\u{2000}'..='\u{200a}' | '\u{2028}' | '\u{2029}' | '\u{202f}'
            | '\u{205f}' | '\u{3000}' | '\u{feff}'
        )
    })
}
