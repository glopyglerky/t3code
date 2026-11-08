use iroh::{
    Endpoint, EndpointAddr, EndpointId, RelayMode, RelayUrl, SecretKey, Watcher,
    endpoint::{
        ConnectError, ConnectWithOptsError, ConnectingError, Connection, ConnectionError,
        QuicConnectError, QuicTransportConfig, presets,
    },
};
use std::{
    io,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use t3_code_ls_core::{
    connection::{
        AttemptEnd, AttemptOutcome, BACKOFF_RESET_AFTER_MS, RetryDecision, RetryPolicy, Wakeup,
    },
    peers::PeerPolicy,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{Mutex, Semaphore, watch},
    task::{JoinHandle, JoinSet},
    time::{Instant, sleep_until, timeout, timeout_at},
};

pub const ALPN: &[u8] = b"t3-code-ls/connect/1";
const REJECTED: u32 = 1;
const SHUTDOWN: u32 = 2;
const BUSY: u32 = 3;
const STREAM_FAILED: u32 = 4;

#[derive(Debug, thiserror::Error)]
pub enum BridgeError {
    #[error("{0}")]
    Configuration(&'static str),
    #[error("{0}")]
    Io(#[from] io::Error),
    #[error("iroh: {0}")]
    Transport(String),
    #[error("connection blocked: {0}")]
    ConnectionBlocked(String),
    #[error("bridge task: {0}")]
    Task(#[from] tokio::task::JoinError),
}

fn transport(error: impl std::fmt::Display) -> BridgeError {
    BridgeError::Transport(error.to_string())
}

#[derive(Clone, Debug)]
pub struct Limits {
    pub connections: usize,
    pub streams_per_connection: u32,
    pub total_streams: usize,
    pub connect_timeout: Duration,
    pub io_timeout: Duration,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            connections: 16,
            streams_per_connection: 32,
            total_streams: 128,
            connect_timeout: Duration::from_secs(10),
            io_timeout: Duration::from_secs(600),
        }
    }
}

impl Limits {
    fn validate(&self) -> Result<(), BridgeError> {
        if self.connections == 0
            || self.connections > 1024
            || self.streams_per_connection == 0
            || self.streams_per_connection > 4096
            || self.total_streams == 0
            || self.total_streams > 4096
        {
            return Err(BridgeError::Configuration(
                "connection/stream limits are outside supported bounds",
            ));
        }
        if self.connect_timeout.is_zero() || self.io_timeout.is_zero() {
            return Err(BridgeError::Configuration("timeouts must be nonzero"));
        }
        Ok(())
    }
}

/// Address publication is opt-in. A custom relay does not enable public DNS discovery.
#[derive(Clone, Debug, Default)]
pub enum Network {
    /// Bind UDP only on localhost, for isolated tests or local processes.
    Loopback,
    #[default]
    Direct,
    Relays(Vec<RelayUrl>),
    Public,
}

async fn endpoint(
    key: SecretKey,
    network: Network,
    limits: &Limits,
    accepts_connections: bool,
) -> Result<Endpoint, BridgeError> {
    limits.validate()?;
    let quic = QuicTransportConfig::builder()
        .max_concurrent_bidi_streams(limits.streams_per_connection.into())
        .max_concurrent_uni_streams(0u32.into())
        .stream_receive_window((1024u32 * 1024).into())
        .receive_window((8u32 * 1024 * 1024).into())
        .send_window(8 * 1024 * 1024)
        .keep_alive_interval(Duration::from_secs(20))
        .build();
    let builder = match network {
        Network::Loopback => Endpoint::builder(presets::Minimal)
            .clear_ip_transports()
            .bind_addr("127.0.0.1:0")
            .map_err(transport)?,
        Network::Direct => Endpoint::builder(presets::Minimal),
        Network::Relays(relays) => {
            if relays.is_empty() {
                return Err(BridgeError::Configuration("at least one relay is required"));
            }
            Endpoint::builder(presets::Minimal).relay_mode(RelayMode::custom(relays))
        }
        Network::Public => Endpoint::builder(presets::N0),
    };
    let alpns = if accepts_connections {
        vec![ALPN.to_vec()]
    } else {
        Vec::new()
    };
    let endpoint = builder
        .secret_key(key)
        .alpns(alpns)
        .transport_config(quic)
        .bind()
        .await
        .map_err(transport)?;
    // bind() can complete before the initial local-interface discovery is published.
    let mut addresses = endpoint.watch_addr();
    let ready = timeout(limits.connect_timeout, async {
        while addresses.get().is_empty() {
            addresses.updated().await.map_err(transport)?;
        }
        Ok::<(), BridgeError>(())
    })
    .await;
    match ready {
        Ok(Ok(())) => Ok(endpoint),
        Ok(Err(error)) => {
            endpoint.close().await;
            Err(error)
        }
        Err(_) => {
            endpoint.close().await;
            Err(io::Error::new(io::ErrorKind::TimedOut, "endpoint has no usable address").into())
        }
    }
}

#[derive(Clone, Debug)]
pub struct Admission {
    policy: watch::Sender<PeerPolicy>,
}

impl Default for Admission {
    fn default() -> Self {
        Self::new()
    }
}

impl Admission {
    pub fn new() -> Self {
        let (policy, _) = watch::channel(PeerPolicy::new());
        Self { policy }
    }
    pub fn grant(&self, peer: EndpointId) -> bool {
        self.policy
            .send_if_modified(|policy| policy.grant(*peer.as_bytes()))
    }
    pub fn revoke(&self, peer: EndpointId) -> bool {
        self.policy
            .send_if_modified(|policy| policy.revoke(peer.as_bytes()))
    }
    pub fn permits(&self, peer: EndpointId) -> bool {
        self.policy.borrow().permits(peer.as_bytes())
    }
    /// Capture this value at admission, then use `wait_for_revocation` to close a session.
    pub fn generation(&self, peer: EndpointId) -> Option<u64> {
        self.policy.borrow().generation(peer.as_bytes())
    }
    /// Also detects revoke/regrant changes coalesced by the watch channel.
    pub async fn wait_for_revocation(&self, peer: EndpointId, generation: u64) {
        let mut policy = self.policy.subscribe();
        loop {
            if policy.borrow().generation(peer.as_bytes()) != Some(generation) {
                return;
            }
            if policy.changed().await.is_err() {
                return;
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct HostConfig {
    pub target: SocketAddr,
    pub admission: Admission,
    pub limits: Limits,
}

impl HostConfig {
    pub fn new(target: SocketAddr, admission: Admission) -> Self {
        Self {
            target,
            admission,
            limits: Limits::default(),
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct Stats {
    pub rejected_peers: u64,
    pub backend_attempts: u64,
    pub failed_streams: u64,
}

#[derive(Debug, Default)]
struct Counters {
    rejected_peers: AtomicU64,
    backend_attempts: AtomicU64,
    failed_streams: AtomicU64,
}

impl Counters {
    fn snapshot(&self) -> Stats {
        Stats {
            rejected_peers: self.rejected_peers.load(Ordering::Relaxed),
            backend_attempts: self.backend_attempts.load(Ordering::Relaxed),
            failed_streams: self.failed_streams.load(Ordering::Relaxed),
        }
    }
}

pub struct Host {
    endpoint: Endpoint,
    cancel: watch::Sender<bool>,
    task: Option<JoinHandle<()>>,
    counters: Arc<Counters>,
}

impl Host {
    pub async fn bind(
        key: SecretKey,
        network: Network,
        config: HostConfig,
    ) -> Result<Self, BridgeError> {
        require_loopback(config.target)?;
        if config.target.port() == 0 {
            return Err(BridgeError::Configuration(
                "backend target port must be nonzero",
            ));
        }
        let endpoint = endpoint(key, network, &config.limits, true).await?;
        let (cancel, receiver) = watch::channel(false);
        let counters = Arc::new(Counters::default());
        let task = tokio::spawn(serve(endpoint.clone(), config, receiver, counters.clone()));
        Ok(Self {
            endpoint,
            cancel,
            task: Some(task),
            counters,
        })
    }
    pub fn id(&self) -> EndpointId {
        self.endpoint.id()
    }
    pub fn addr(&self) -> EndpointAddr {
        self.endpoint.addr()
    }
    pub fn stats(&self) -> Stats {
        self.counters.snapshot()
    }
    pub async fn online(&self, deadline: Duration) -> Result<(), BridgeError> {
        timeout(deadline, self.endpoint.online())
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "relay registration timed out"))?;
        Ok(())
    }
    pub async fn network_changed(&self) {
        self.endpoint.network_change().await;
    }
    pub async fn shutdown(mut self) -> Result<(), BridgeError> {
        let _ = self.cancel.send(true);
        if let Some(task) = self.task.take() {
            task.await?;
        }
        Ok(())
    }
}

impl Drop for Host {
    fn drop(&mut self) {
        let _ = self.cancel.send(true);
    }
}

fn require_loopback(addr: SocketAddr) -> Result<(), BridgeError> {
    if !addr.ip().is_loopback() {
        return Err(BridgeError::Configuration(
            "bridge target and local listener must be explicit loopback addresses",
        ));
    }
    Ok(())
}

async fn serve(
    endpoint: Endpoint,
    config: HostConfig,
    mut stop: watch::Receiver<bool>,
    counters: Arc<Counters>,
) {
    let connections = Arc::new(Semaphore::new(config.limits.connections));
    let streams = Arc::new(Semaphore::new(config.limits.total_streams));
    let mut tasks = JoinSet::new();
    loop {
        tokio::select! {
            biased;
            _ = stop.changed() => break,
            Some(_) = tasks.join_next(), if !tasks.is_empty() => {},
            incoming = endpoint.accept() => {
                let Some(incoming) = incoming else { break; };
                let Ok(permit) = connections.clone().try_acquire_owned() else {
                    incoming.refuse();
                    continue;
                };
                let config = config.clone();
                let streams = streams.clone();
                let counters = counters.clone();
                tasks.spawn(async move {
                    let _permit = permit;
                    if let Ok(Ok(connection)) = timeout(config.limits.connect_timeout, incoming).await {
                        serve_connection(connection, config, streams, counters).await;
                    }
                });
            }
        }
    }
    tasks.abort_all();
    while tasks.join_next().await.is_some() {}
    endpoint.close().await;
}

async fn serve_connection(
    connection: Connection,
    config: HostConfig,
    global_streams: Arc<Semaphore>,
    counters: Arc<Counters>,
) {
    let peer = connection.remote_id();
    let mut policy = config.admission.policy.subscribe();
    let Some(generation) = policy.borrow().generation(peer.as_bytes()) else {
        counters.rejected_peers.fetch_add(1, Ordering::Relaxed);
        connection.close(REJECTED.into(), b"peer not granted");
        return;
    };
    let streams = Arc::new(Semaphore::new(
        config.limits.streams_per_connection as usize,
    ));
    let mut tasks = JoinSet::new();
    loop {
        tokio::select! {
            biased;
            changed = policy.changed() => {
                if changed.is_err() || policy.borrow().generation(peer.as_bytes()) != Some(generation) {
                    connection.close(REJECTED.into(), b"peer grant revoked");
                    break;
                }
            },
            Some(_) = tasks.join_next(), if !tasks.is_empty() => {},
            incoming = connection.accept_bi() => {
                let Ok((mut send, mut recv)) = incoming else { break; };
                if policy.borrow().generation(peer.as_bytes()) != Some(generation) {
                    connection.close(REJECTED.into(), b"peer grant revoked");
                    break;
                }
                let local_permit = streams.clone().try_acquire_owned();
                let global_permit = global_streams.clone().try_acquire_owned();
                let (Ok(local_permit), Ok(global_permit)) = (local_permit, global_permit) else {
                    let _ = send.reset(BUSY.into());
                    let _ = recv.stop(BUSY.into());
                    continue;
                };
                let config = config.clone();
                let counters = counters.clone();
                tasks.spawn(async move {
                    let _permits = (local_permit, global_permit);
                    counters.backend_attempts.fetch_add(1, Ordering::Relaxed);
                    let result = async {
                        let socket = timeout(config.limits.connect_timeout, TcpStream::connect(config.target)).await
                            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "backend connection timed out"))??;
                        socket.set_nodelay(true)?;
                        bridge(socket, &mut send, &mut recv, config.limits.io_timeout).await
                    }.await;
                    if result.is_err() {
                        counters.failed_streams.fetch_add(1, Ordering::Relaxed);
                        let _ = send.reset(STREAM_FAILED.into());
                        let _ = recv.stop(STREAM_FAILED.into());
                    }
                });
            }
        }
    }
    tasks.abort_all();
    while tasks.join_next().await.is_some() {}
}

async fn pump<R: AsyncRead + Unpin, W: AsyncWrite + Unpin>(
    reader: &mut R,
    writer: &mut W,
    deadline: Duration,
) -> io::Result<()> {
    let mut buffer = [0; 16 * 1024];
    loop {
        let count = timeout(deadline, reader.read(&mut buffer))
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "bridge read timed out"))??;
        if count == 0 {
            timeout(deadline, writer.shutdown()).await.map_err(|_| {
                io::Error::new(io::ErrorKind::TimedOut, "bridge half-close timed out")
            })??;
            return Ok(());
        }
        timeout(deadline, writer.write_all(&buffer[..count]))
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "bridge write timed out"))??;
    }
}

async fn bridge(
    socket: TcpStream,
    send: &mut iroh::endpoint::SendStream,
    recv: &mut iroh::endpoint::RecvStream,
    deadline: Duration,
) -> io::Result<()> {
    let (mut read, mut write) = socket.into_split();
    tokio::try_join!(
        pump(&mut read, send, deadline),
        pump(recv, &mut write, deadline)
    )?;
    Ok(())
}

pub struct Forwarder {
    address: SocketAddr,
    endpoint: Endpoint,
    cancel: watch::Sender<bool>,
    retry_wakeups: watch::Sender<RetryWakeup>,
    task: Option<JoinHandle<Result<(), BridgeError>>>,
}

impl Forwarder {
    pub async fn bind(
        key: SecretKey,
        network: Network,
        remote: EndpointAddr,
        listen: SocketAddr,
        limits: Limits,
    ) -> Result<Self, BridgeError> {
        require_loopback(listen)?;
        limits.validate()?;
        let endpoint = endpoint(key, network, &limits, false).await?;
        let listener = match TcpListener::bind(listen).await {
            Ok(listener) => listener,
            Err(error) => {
                endpoint.close().await;
                return Err(error.into());
            }
        };
        let address = listener.local_addr()?;
        let (cancel, receiver) = watch::channel(false);
        let (retry_wakeups, wakeups) = watch::channel(RetryWakeup::Explicit);
        let task = tokio::spawn(forward(
            endpoint.clone(),
            listener,
            remote,
            limits,
            receiver,
            wakeups,
        ));
        Ok(Self {
            address,
            endpoint,
            cancel,
            retry_wakeups,
            task: Some(task),
        })
    }
    pub fn local_addr(&self) -> SocketAddr {
        self.address
    }
    pub fn id(&self) -> EndpointId {
        self.endpoint.id()
    }
    pub async fn network_changed(&self) {
        self.endpoint.network_change().await;
        self.retry_wakeups.send_replace(RetryWakeup::NetworkChanged);
    }
    /// Clear connection backoff or an admission/authentication block for the
    /// next new TCP request. Existing streams are never restarted or replayed.
    pub fn retry_now(&self) {
        self.retry_wakeups.send_replace(RetryWakeup::Explicit);
    }
    /// Waits for the forwarding loop; cancelling this wait leaves shutdown available.
    pub async fn wait(&mut self) -> Result<(), BridgeError> {
        let result = match self.task.as_mut() {
            Some(task) => task.await,
            None => return Ok(()),
        };
        self.task.take();
        result?
    }
    pub async fn shutdown(mut self) -> Result<(), BridgeError> {
        let _ = self.cancel.send(true);
        self.wait().await
    }
}

impl Drop for Forwarder {
    fn drop(&mut self) {
        let _ = self.cancel.send(true);
    }
}

#[derive(Clone, Copy)]
enum RetryWakeup {
    Explicit,
    NetworkChanged,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ConnectionFailure {
    Transient,
    Blocked,
    Stopped,
}

fn connection_failure(error: &ConnectionError) -> ConnectionFailure {
    match error {
        ConnectionError::ApplicationClosed(close) if close.error_code == REJECTED.into() => {
            ConnectionFailure::Blocked
        }
        ConnectionError::VersionMismatch | ConnectionError::CidsExhausted => {
            ConnectionFailure::Blocked
        }
        ConnectionError::TransportError(error)
            if (0x100..0x200).contains(&u64::from(error.code)) =>
        {
            ConnectionFailure::Blocked
        }
        ConnectionError::ConnectionClosed(close)
            if (0x100..0x200).contains(&u64::from(close.error_code)) =>
        {
            ConnectionFailure::Blocked
        }
        ConnectionError::LocallyClosed => ConnectionFailure::Stopped,
        _ => ConnectionFailure::Transient,
    }
}

fn connect_failure(error: &ConnectError) -> ConnectionFailure {
    match error {
        ConnectError::Connection { source, .. }
        | ConnectError::Connecting {
            source: ConnectingError::ConnectionError { source, .. },
            ..
        } => connection_failure(source),
        ConnectError::Connect {
            source: ConnectWithOptsError::NoAddress { .. },
            ..
        } => ConnectionFailure::Transient,
        ConnectError::Connect {
            source:
                ConnectWithOptsError::EndpointClosed { .. }
                | ConnectWithOptsError::InternalConsistencyError { .. },
            ..
        }
        | ConnectError::Connect {
            source:
                ConnectWithOptsError::Noq {
                    source: QuicConnectError::EndpointStopping,
                    ..
                },
            ..
        }
        | ConnectError::Connecting {
            source: ConnectingError::InternalConsistencyError { .. },
            ..
        } => ConnectionFailure::Stopped,
        // Authentication, unsupported protocol, local hooks, and invalid local
        // configuration need an explicit wakeup rather than timed retries.
        _ => ConnectionFailure::Blocked,
    }
}

fn failure_error(kind: ConnectionFailure, message: String) -> BridgeError {
    match kind {
        ConnectionFailure::Transient => BridgeError::Transport(message),
        ConnectionFailure::Blocked => BridgeError::ConnectionBlocked(message),
        ConnectionFailure::Stopped => io::Error::new(io::ErrorKind::NotConnected, message).into(),
    }
}

fn connection_timeout() -> BridgeError {
    io::Error::new(io::ErrorKind::TimedOut, "Iroh connection wait timed out").into()
}

#[derive(Clone)]
struct ConnectionEnded {
    reason: ConnectionError,
    at: Instant,
    stable: bool,
}

// A ready close event wins over a simultaneously elapsed stability timer. This
// also handles an observer first polled long after a short session has ended.
async fn observe_session_end(
    closed: impl std::future::Future<Output = ConnectionError>,
    stable_at: Instant,
) -> ConnectionEnded {
    tokio::pin!(closed);
    let (reason, stable) = tokio::select! {
        biased;
        reason = &mut closed => (reason, false),
        _ = sleep_until(stable_at) => (closed.await, true),
    };
    ConnectionEnded {
        reason,
        at: Instant::now(),
        stable,
    }
}

struct LiveConnection {
    connection: Connection,
    ended: watch::Receiver<Option<ConnectionEnded>>,
    observer: JoinHandle<()>,
}

impl LiveConnection {
    fn new(connection: Connection) -> Self {
        let (sender, ended) = watch::channel(None);
        let observed = connection.clone();
        let stable_at = Instant::now() + Duration::from_millis(BACKOFF_RESET_AFTER_MS);
        let observer = tokio::spawn(async move {
            let ended = observe_session_end(observed.closed(), stable_at).await;
            let _ = sender.send(Some(ended));
        });
        Self {
            connection,
            ended,
            observer,
        }
    }

    async fn end(&mut self) -> Result<ConnectionEnded, BridgeError> {
        loop {
            if let Some(ended) = self.ended.borrow().clone() {
                return Ok(ended);
            }
            self.ended.changed().await.map_err(|_| {
                io::Error::other("connection observer stopped before reporting closure")
            })?;
        }
    }
}

impl Drop for LiveConnection {
    fn drop(&mut self) {
        self.observer.abort();
    }
}

struct ConnectionCache {
    live: Option<LiveConnection>,
    retry: RetryPolicy,
    retry_at: Option<Instant>,
    blocked: Option<(ConnectionFailure, String)>,
    wakeups: watch::Receiver<RetryWakeup>,
}

impl ConnectionCache {
    fn new(wakeups: watch::Receiver<RetryWakeup>) -> Self {
        Self {
            live: None,
            retry: RetryPolicy::new(),
            retry_at: None,
            blocked: None,
            wakeups,
        }
    }

    fn receive_wakeup(&mut self) {
        match *self.wakeups.borrow_and_update() {
            RetryWakeup::Explicit => self.retry.reset(),
            RetryWakeup::NetworkChanged => {
                self.retry.wakeup(Wakeup::NetworkChanged);
            }
        }
        self.retry_at = None;
        if self
            .blocked
            .as_ref()
            .is_some_and(|(kind, _)| *kind == ConnectionFailure::Blocked)
        {
            self.blocked = None;
        }
    }

    fn record_failure(
        &mut self,
        kind: ConnectionFailure,
        established: bool,
        stable: bool,
        at: Instant,
        message: String,
    ) {
        let end = match kind {
            ConnectionFailure::Transient => AttemptEnd::TransientFailure,
            ConnectionFailure::Blocked => AttemptEnd::BlockedFailure,
            ConnectionFailure::Stopped => AttemptEnd::Interrupted { reset_retry: false },
        };
        let decision = self.retry.after_attempt(
            AttemptOutcome {
                established,
                stable,
                probe_failed: false,
                end,
            },
            fastrand::f64(),
        );
        self.retry_at = match decision {
            RetryDecision::RetryAfter(delay_ms) => Some(at + Duration::from_millis(delay_ms)),
            _ => None,
        };
        self.blocked = if kind == ConnectionFailure::Transient {
            None
        } else {
            Some((kind, message))
        };
    }
}

async fn cached_connection(
    cached: &Mutex<ConnectionCache>,
    endpoint: &Endpoint,
    remote: EndpointAddr,
    deadline: Duration,
) -> Result<Connection, BridgeError> {
    let expires = Instant::now()
        .checked_add(deadline)
        .ok_or(BridgeError::Configuration(
            "connection timeout exceeds clock range",
        ))?;
    // Every local request has its own total deadline, including queueing behind
    // another request and backoff. Only a dial failure increments the ladder.
    let mut cached = timeout_at(expires, cached.lock())
        .await
        .map_err(|_| connection_timeout())?;
    if let Some(live) = &mut cached.live {
        if live.connection.close_reason().is_none() {
            let connection = live.connection.clone();
            if cached.wakeups.has_changed().unwrap_or(false) {
                cached.receive_wakeup();
            }
            return Ok(connection);
        }
        let ended = timeout_at(expires, live.end())
            .await
            .map_err(|_| connection_timeout())??;
        cached.live = None;
        cached.record_failure(
            connection_failure(&ended.reason),
            true,
            ended.stable,
            ended.at,
            ended.reason.to_string(),
        );
    }
    loop {
        if cached.wakeups.has_changed().unwrap_or(false) {
            cached.receive_wakeup();
        }
        if let Some((kind, message)) = &cached.blocked {
            return Err(failure_error(*kind, message.clone()));
        }
        let Some(retry_at) = cached.retry_at else {
            break;
        };
        if retry_at <= Instant::now() {
            cached.retry_at = None;
            break;
        }
        tokio::select! {
            biased;
            changed = cached.wakeups.changed() => {
                changed.map_err(|_| io::Error::new(io::ErrorKind::NotConnected, "forwarder stopped"))?;
                cached.receive_wakeup();
            }
            _ = sleep_until(expires) => return Err(connection_timeout()),
            _ = sleep_until(retry_at) => { cached.retry_at = None; }
        }
    }
    if Instant::now() >= expires {
        return Err(connection_timeout());
    }
    // Exactly one handshake per request. A failed dial returns to its TCP
    // caller; only a later request can use the next retry deadline. No bytes
    // have been read from that TCP connection at this point.
    match timeout_at(expires, endpoint.connect(remote, ALPN)).await {
        Ok(Ok(connection)) => {
            cached.live = Some(LiveConnection::new(connection.clone()));
            Ok(connection)
        }
        Ok(Err(error)) => {
            let kind = connect_failure(&error);
            let message = error.to_string();
            cached.record_failure(kind, false, false, Instant::now(), message.clone());
            Err(failure_error(kind, message))
        }
        Err(_) => {
            cached.record_failure(
                ConnectionFailure::Transient,
                false,
                false,
                Instant::now(),
                String::new(),
            );
            Err(connection_timeout())
        }
    }
}

async fn forward(
    endpoint: Endpoint,
    listener: TcpListener,
    remote: EndpointAddr,
    limits: Limits,
    mut stop: watch::Receiver<bool>,
    wakeups: watch::Receiver<RetryWakeup>,
) -> Result<(), BridgeError> {
    let cached = Arc::new(Mutex::new(ConnectionCache::new(wakeups)));
    let streams = Arc::new(Semaphore::new(limits.total_streams));
    let mut tasks = JoinSet::new();
    let result = loop {
        tokio::select! {
            biased;
            _ = stop.changed() => break Ok(()),
            Some(_) = tasks.join_next(), if !tasks.is_empty() => {},
            accepted = listener.accept() => {
                let socket = match accepted {
                    Ok((socket, _)) => socket,
                    Err(error) if matches!(error.kind(),
                        io::ErrorKind::Interrupted
                        | io::ErrorKind::ConnectionAborted
                        | io::ErrorKind::ConnectionReset) => continue,
                    Err(error) => break Err(BridgeError::Io(error)),
                };
                let Ok(permit) = streams.clone().try_acquire_owned() else { continue; };
                let endpoint = endpoint.clone();
                let cached = cached.clone();
                let remote = remote.clone();
                let limits = limits.clone();
                tasks.spawn(async move {
                    let _permit = permit;
                    let Ok(connection) = cached_connection(&cached, &endpoint, remote, limits.connect_timeout).await else { return; };
                    let Ok(Ok((mut send, mut recv))) = timeout(limits.connect_timeout, connection.open_bi()).await else { return; };
                    if socket.set_nodelay(true).is_err() || bridge(socket, &mut send, &mut recv, limits.io_timeout).await.is_err() {
                        let _ = send.reset(STREAM_FAILED.into());
                        let _ = recv.stop(STREAM_FAILED.into());
                    }
                });
            }
        }
    };
    tasks.abort_all();
    while tasks.join_next().await.is_some() {}
    if let Some(live) = cached.lock().await.live.take() {
        live.connection.close(SHUTDOWN.into(), b"forwarder stopped");
    }
    endpoint.close().await;
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn listener_failure_reaches_wait_and_closes_active_backend() {
        use std::{net::Shutdown, os::fd::OwnedFd};

        timeout(Duration::from_secs(15), async {
            let backend = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let key = SecretKey::generate();
            let admission = Admission::new();
            admission.grant(key.public());
            let host = Host::bind(
                SecretKey::generate(),
                Network::Loopback,
                HostConfig::new(backend.local_addr().unwrap(), admission),
            )
            .await
            .unwrap();
            let limits = Limits::default();
            let endpoint = endpoint(key, Network::Loopback, &limits, false)
                .await
                .unwrap();
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            listener.set_nonblocking(true).unwrap();
            // On Linux, shutting down a listening socket makes accept return EINVAL.
            let control = std::net::TcpStream::from(OwnedFd::from(listener.try_clone().unwrap()));
            let listener = TcpListener::from_std(listener).unwrap();
            let address = listener.local_addr().unwrap();
            let (cancel, receiver) = watch::channel(false);
            let (retry_wakeups, wakeups) = watch::channel(RetryWakeup::Explicit);
            let task = tokio::spawn(forward(
                endpoint.clone(),
                listener,
                host.addr(),
                limits,
                receiver,
                wakeups,
            ));
            let mut forwarder = Forwarder {
                address,
                endpoint,
                cancel,
                retry_wakeups,
                task: Some(task),
            };

            let mut local = TcpStream::connect(forwarder.local_addr()).await.unwrap();
            local.write_all(b"active").await.unwrap();
            let (mut service, _) = backend.accept().await.unwrap();
            let mut bytes = [0; 6];
            service.read_exact(&mut bytes).await.unwrap();
            assert_eq!(&bytes, b"active");

            control.shutdown(Shutdown::Both).unwrap();
            let result = forwarder.wait().await;
            assert!(
                matches!(result, Err(BridgeError::Io(ref error)) if error.kind() == io::ErrorKind::InvalidInput),
                "terminal listener failures must reach the caller: {result:?}"
            );
            assert_eq!(service.read(&mut bytes).await.unwrap(), 0);
            forwarder.shutdown().await.unwrap();
            host.shutdown().await.unwrap();
        })
        .await
        .unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn connection_deadline_includes_waiting_for_another_attempt() {
        let endpoint = Endpoint::builder(presets::Minimal)
            .secret_key(SecretKey::generate())
            .bind_addr("127.0.0.1:0".parse::<SocketAddr>().unwrap())
            .unwrap()
            .bind()
            .await
            .unwrap();
        let (_wakeups, receiver) = watch::channel(RetryWakeup::Explicit);
        let cached = Mutex::new(ConnectionCache::new(receiver));
        let blocked = cached.lock().await;
        let remote = EndpointAddr::new(SecretKey::generate().public());
        let result = timeout(
            Duration::from_secs(2),
            cached_connection(&cached, &endpoint, remote, Duration::from_secs(1)),
        )
        .await
        .expect("a queued attempt must respect its own connection deadline");
        assert!(
            matches!(result, Err(BridgeError::Io(error)) if error.kind() == io::ErrorKind::TimedOut)
        );
        drop(blocked);
        endpoint.close().await;
    }

    #[tokio::test(start_paused = true)]
    async fn stability_uses_live_time_not_late_cache_observation() {
        let began = Instant::now();
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let observer = tokio::spawn(observe_session_end(
            async { receiver.await.unwrap() },
            began + Duration::from_millis(BACKOFF_RESET_AFTER_MS),
        ));
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(1)).await;
        sender.send(ConnectionError::Reset).unwrap();
        tokio::task::yield_now().await;
        // The cache need not be read until a later local TCP client arrives.
        tokio::time::advance(Duration::from_secs(40)).await;
        let ended = observer.await.unwrap();
        assert!(!ended.stable);
        assert_eq!(ended.at - began, Duration::from_secs(1));

        let (sender, receiver) = tokio::sync::oneshot::channel();
        let observer = tokio::spawn(observe_session_end(
            async { receiver.await.unwrap() },
            Instant::now() + Duration::from_millis(BACKOFF_RESET_AFTER_MS),
        ));
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(BACKOFF_RESET_AFTER_MS)).await;
        tokio::task::yield_now().await;
        assert!(!observer.is_finished());
        sender.send(ConnectionError::Reset).unwrap();
        assert!(observer.await.unwrap().stable);

        // If both conditions are ready before the observer is first polled,
        // an already closed connection cannot be classified as stable.
        let ended = observe_session_end(async { ConnectionError::Reset }, began).await;
        assert!(!ended.stable);
    }

    #[tokio::test(start_paused = true)]
    async fn a_backoff_wait_timeout_does_not_count_as_another_failed_dial() {
        let endpoint = Endpoint::builder(presets::Minimal)
            .bind_addr("127.0.0.1:0")
            .unwrap()
            .bind()
            .await
            .unwrap();
        let (_sender, receiver) = watch::channel(RetryWakeup::Explicit);
        let mut state = ConnectionCache::new(receiver);
        state.record_failure(
            ConnectionFailure::Transient,
            false,
            false,
            Instant::now(),
            String::new(),
        );
        state.retry_at = Some(Instant::now() + Duration::from_secs(300));
        let cached = Mutex::new(state);
        let result = cached_connection(
            &cached,
            &endpoint,
            EndpointAddr::new(endpoint.id()),
            Duration::from_secs(1),
        )
        .await;
        assert!(
            matches!(result, Err(BridgeError::Io(error)) if error.kind() == io::ErrorKind::TimedOut)
        );
        assert_eq!(cached.lock().await.retry.failure_count(), 1);
        assert!(
            cached.lock().await.blocked.is_none(),
            "a timed-out waiter must not dial the self address"
        );
        endpoint.close().await;
    }

    #[tokio::test(start_paused = true)]
    async fn retry_wakeups_interrupt_an_active_backoff_waiter() {
        for wakeup in [RetryWakeup::Explicit, RetryWakeup::NetworkChanged] {
            let endpoint = Endpoint::builder(presets::Minimal)
                .bind_addr("127.0.0.1:0")
                .unwrap()
                .bind()
                .await
                .unwrap();
            let (sender, receiver) = watch::channel(RetryWakeup::Explicit);
            let mut state = ConnectionCache::new(receiver);
            state.record_failure(
                ConnectionFailure::Transient,
                false,
                false,
                Instant::now(),
                String::new(),
            );
            state.retry_at = Some(Instant::now() + Duration::from_secs(300));
            let cached = Arc::new(Mutex::new(state));
            let worker_cache = cached.clone();
            let worker_endpoint = endpoint.clone();
            let request = tokio::spawn(async move {
                cached_connection(
                    &worker_cache,
                    &worker_endpoint,
                    EndpointAddr::new(worker_endpoint.id()),
                    Duration::from_secs(1),
                )
                .await
            });
            tokio::task::yield_now().await;
            assert!(
                cached.try_lock().is_err(),
                "request must be waiting in shared backoff"
            );
            sender.send_replace(wakeup);
            let result = timeout(Duration::from_millis(100), request)
                .await
                .unwrap()
                .unwrap();
            // A self-dial is immediately blocked, proving the timer was cleared
            // and that reset did not require the lock held by the waiting request.
            assert!(matches!(result, Err(BridgeError::ConnectionBlocked(_))));
            assert_eq!(cached.lock().await.retry.failure_count(), 0);
            endpoint.close().await;
        }
    }

    #[tokio::test]
    async fn an_actual_dial_timeout_counts_once_before_releasing_the_cache() {
        let limits = Limits::default();
        let endpoint = endpoint(SecretKey::generate(), Network::Loopback, &limits, false)
            .await
            .unwrap();
        let silent = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let remote = EndpointAddr::new(SecretKey::generate().public())
            .with_ip_addr(silent.local_addr().unwrap());
        let (_sender, receiver) = watch::channel(RetryWakeup::Explicit);
        let cached = Mutex::new(ConnectionCache::new(receiver));
        let result = cached_connection(&cached, &endpoint, remote, Duration::from_millis(50)).await;
        assert!(
            matches!(result, Err(BridgeError::Io(error)) if error.kind() == io::ErrorKind::TimedOut)
        );
        let state = cached.lock().await;
        assert_eq!(state.retry.failure_count(), 1);
        assert!(state.retry_at.is_some_and(|at| at > Instant::now()));
        assert!(state.live.is_none());
        drop(state);
        endpoint.close().await;
    }
}
