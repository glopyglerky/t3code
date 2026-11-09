use std::{io, net::SocketAddr, time::Duration};

use iroh::{Endpoint, EndpointAddr, endpoint::presets};
use t3_code_ls_transport::{
    ALPN, Admission, Forwarder, Host, HostConfig, Limits, Network, SecretKey,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream, UdpSocket},
    time::timeout,
};

const DEADLINE: Duration = Duration::from_secs(15);

fn loopback() -> SocketAddr {
    "127.0.0.1:0".parse().unwrap()
}

async fn client(key: SecretKey) -> Endpoint {
    Endpoint::builder(presets::Minimal)
        .clear_ip_transports()
        .bind_addr("127.0.0.1:0")
        .unwrap()
        .secret_key(key)
        .bind()
        .await
        .unwrap()
}

async fn assert_closed_without_response(socket: &mut TcpStream) {
    let mut response = [0; 1];
    match socket.read(&mut response).await {
        Ok(0) => {}
        Err(error) => assert!(
            matches!(
                error.kind(),
                io::ErrorKind::ConnectionReset
                    | io::ErrorKind::ConnectionAborted
                    | io::ErrorKind::BrokenPipe
                    | io::ErrorKind::NotConnected
            ),
            "unexpected TCP failure: {error}"
        ),
        Ok(_) => panic!("failed request unexpectedly received a response"),
    }
}

#[tokio::test]
async fn preserves_http_auth_websocket_bytes_and_binary_half_close() {
    timeout(DEADLINE, async {
        let backend = TcpListener::bind(loopback()).await.unwrap();
        let key = SecretKey::generate();
        let admission = Admission::new();
        admission.grant(key.public());
        let host = Host::bind(SecretKey::generate(), Network::Loopback,
            HostConfig::new(backend.local_addr().unwrap(), admission)).await.unwrap();
        let forwarder = Forwarder::bind(key, Network::Loopback, host.addr(), loopback(), Limits::default()).await.unwrap();
        let request = b"GET /api/ws HTTP/1.1\r\nHost: localhost:3773\r\nOrigin: http://localhost:3773\r\nCookie: t3_session=unchanged\r\nAuthorization: Bearer preserve-this\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\r\n";
        let response = b"HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\r\n";
        let payload: Vec<u8> = (0..1024 * 1024).map(|i| ((i * 17) % 256) as u8).collect();
        let expected = payload.clone();
        let service = tokio::spawn(async move {
            let (mut socket, _) = backend.accept().await.unwrap();
            let mut received = vec![0; request.len()];
            socket.read_exact(&mut received).await.unwrap();
            assert_eq!(received, request);
            socket.write_all(response).await.unwrap();
            let mut bytes = Vec::new();
            socket.read_to_end(&mut bytes).await.unwrap();
            assert_eq!(bytes, expected);
            // This response is sent only after observing the client's half-close.
            socket.write_all(&bytes).await.unwrap();
            socket.shutdown().await.unwrap();
        });
        let mut socket = TcpStream::connect(forwarder.local_addr()).await.unwrap();
        socket.write_all(request).await.unwrap();
        let mut upgraded = vec![0; response.len()];
        socket.read_exact(&mut upgraded).await.unwrap();
        assert_eq!(upgraded, response);
        socket.write_all(&payload).await.unwrap();
        socket.shutdown().await.unwrap();
        let mut echoed = Vec::new();
        socket.read_to_end(&mut echoed).await.unwrap();
        assert_eq!(echoed, payload);
        service.await.unwrap();
        assert_eq!(host.stats().backend_attempts, 1);
        forwarder.shutdown().await.unwrap();
        host.shutdown().await.unwrap();
    }).await.unwrap();
}

#[tokio::test]
async fn unauthorized_endpoint_never_connects_to_backend() {
    timeout(DEADLINE, async {
        let backend = TcpListener::bind(loopback()).await.unwrap();
        let host = Host::bind(
            SecretKey::generate(),
            Network::Loopback,
            HostConfig::new(backend.local_addr().unwrap(), Admission::new()),
        )
        .await
        .unwrap();
        let unauthorized = client(SecretKey::generate()).await;
        if let Ok(connection) = unauthorized.connect(host.addr(), ALPN).await {
            connection.closed().await;
        }
        assert_eq!(host.stats().rejected_peers, 1);
        assert_eq!(host.stats().backend_attempts, 0);
        assert!(
            timeout(Duration::from_millis(50), backend.accept())
                .await
                .is_err()
        );
        unauthorized.close().await;
        host.shutdown().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn revoke_then_regrant_does_not_resurrect_existing_connection() {
    timeout(DEADLINE, async {
        let backend = TcpListener::bind(loopback()).await.unwrap();
        let key = SecretKey::generate();
        let admission = Admission::new();
        admission.grant(key.public());
        let host = Host::bind(
            SecretKey::generate(),
            Network::Loopback,
            HostConfig::new(backend.local_addr().unwrap(), admission.clone()),
        )
        .await
        .unwrap();
        let service = tokio::spawn(async move {
            let mut tasks = tokio::task::JoinSet::new();
            for _ in 0..2 {
                let (socket, _) = backend.accept().await.unwrap();
                tasks.spawn(async move {
                    let (mut read, mut write) = socket.into_split();
                    let _ = tokio::io::copy(&mut read, &mut write).await;
                });
            }
            while tasks.join_next().await.is_some() {}
        });
        let endpoint = client(key.clone()).await;
        let old = endpoint.connect(host.addr(), ALPN).await.unwrap();
        let (mut send, mut recv) = old.open_bi().await.unwrap();
        send.write_all(b"a").await.unwrap();
        let mut byte = [0];
        recv.read_exact(&mut byte).await.unwrap();
        assert_eq!(byte, *b"a");
        // No yield between these changes: watch can coalesce them.
        assert!(admission.revoke(key.public()));
        assert!(admission.grant(key.public()));
        old.closed().await;
        let new = endpoint.connect(host.addr(), ALPN).await.unwrap();
        let (mut send, mut recv) = new.open_bi().await.unwrap();
        send.write_all(b"b").await.unwrap();
        recv.read_exact(&mut byte).await.unwrap();
        assert_eq!(byte, *b"b");
        new.close(0u32.into(), b"test complete");
        endpoint.close().await;
        service.await.unwrap();
        assert_eq!(host.stats().backend_attempts, 2);
        host.shutdown().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn backend_failure_resets_stream_without_hanging_client() {
    timeout(DEADLINE, async {
        let reserved = TcpListener::bind(loopback()).await.unwrap();
        let target = reserved.local_addr().unwrap();
        drop(reserved);
        let key = SecretKey::generate();
        let admission = Admission::new();
        admission.grant(key.public());
        let host = Host::bind(
            SecretKey::generate(),
            Network::Loopback,
            HostConfig::new(target, admission),
        )
        .await
        .unwrap();
        let endpoint = client(key).await;
        let connection = endpoint.connect(host.addr(), ALPN).await.unwrap();
        let (mut send, mut recv) = connection.open_bi().await.unwrap();
        send.write_all(b"request").await.unwrap();
        assert!(recv.read_to_end(1024).await.is_err());
        assert_eq!(host.stats().backend_attempts, 1);
        assert_eq!(host.stats().failed_streams, 1);
        endpoint.close().await;
        host.shutdown().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn concurrent_tcp_connections_remain_isolated() {
    timeout(DEADLINE, async {
        let backend = TcpListener::bind(loopback()).await.unwrap();
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
        let service = tokio::spawn(async move {
            let mut tasks = tokio::task::JoinSet::new();
            for _ in 0..8 {
                let (mut socket, _) = backend.accept().await.unwrap();
                tasks.spawn(async move {
                    let mut bytes = Vec::new();
                    socket.read_to_end(&mut bytes).await.unwrap();
                    bytes.reverse();
                    socket.write_all(&bytes).await.unwrap();
                    socket.shutdown().await.unwrap();
                });
            }
            while let Some(result) = tasks.join_next().await {
                result.unwrap();
            }
        });
        let forwarder = Forwarder::bind(
            key,
            Network::Loopback,
            host.addr(),
            loopback(),
            Limits::default(),
        )
        .await
        .unwrap();
        let mut requests = tokio::task::JoinSet::new();
        for number in 0..8u8 {
            let addr = forwarder.local_addr();
            requests.spawn(async move {
                let mut bytes: Vec<u8> = (0..64 * 1024).map(|i| (i % 251) as u8).collect();
                bytes[0] = number;
                let mut socket = TcpStream::connect(addr).await.unwrap();
                socket.write_all(&bytes).await.unwrap();
                socket.shutdown().await.unwrap();
                let mut response = Vec::new();
                socket.read_to_end(&mut response).await.unwrap();
                bytes.reverse();
                assert_eq!(response, bytes);
            });
        }
        while let Some(result) = requests.join_next().await {
            result.unwrap();
        }
        service.await.unwrap();
        assert_eq!(host.stats().backend_attempts, 8);
        forwarder.shutdown().await.unwrap();
        host.shutdown().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn public_tcp_bindings_and_zero_limits_are_refused() {
    let target = "0.0.0.0:3773".parse().unwrap();
    assert!(
        Host::bind(
            SecretKey::generate(),
            Network::Loopback,
            HostConfig::new(target, Admission::new())
        )
        .await
        .is_err()
    );
    assert!(
        Host::bind(
            SecretKey::generate(),
            Network::Loopback,
            HostConfig::new(loopback(), Admission::new())
        )
        .await
        .is_err()
    );
    let mut config = HostConfig::new("127.0.0.1:3773".parse().unwrap(), Admission::new());
    config.limits.total_streams = 0;
    assert!(
        Host::bind(SecretKey::generate(), Network::Loopback, config)
            .await
            .is_err()
    );
    let remote = SecretKey::generate().public().into();
    assert!(
        Forwarder::bind(
            SecretKey::generate(),
            Network::Loopback,
            remote,
            target,
            Limits::default()
        )
        .await
        .is_err()
    );
}

#[tokio::test]
async fn stream_budget_rejects_excess_before_connecting_to_backend() {
    timeout(DEADLINE, async {
        let backend = TcpListener::bind(loopback()).await.unwrap();
        let key = SecretKey::generate();
        let admission = Admission::new();
        admission.grant(key.public());
        let mut config = HostConfig::new(backend.local_addr().unwrap(), admission);
        config.limits.total_streams = 1;
        let host = Host::bind(SecretKey::generate(), Network::Loopback, config)
            .await
            .unwrap();
        let endpoint = client(key).await;
        let connection = endpoint.connect(host.addr(), ALPN).await.unwrap();
        let (mut first_send, mut first_recv) = connection.open_bi().await.unwrap();
        first_send.write_all(b"held").await.unwrap();
        let (mut backend_socket, _) = backend.accept().await.unwrap();
        backend_socket.write_all(b"ready").await.unwrap();
        let mut ready = [0; 5];
        first_recv.read_exact(&mut ready).await.unwrap();
        let (mut excess_send, mut excess_recv) = connection.open_bi().await.unwrap();
        excess_send.write_all(b"excess").await.unwrap();
        assert!(excess_recv.read_to_end(1024).await.is_err());
        assert_eq!(host.stats().backend_attempts, 1);
        endpoint.close().await;
        host.shutdown().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn idle_io_timeout_resets_stalled_stream() {
    timeout(DEADLINE, async {
        let backend = TcpListener::bind(loopback()).await.unwrap();
        let key = SecretKey::generate();
        let admission = Admission::new();
        admission.grant(key.public());
        let mut config = HostConfig::new(backend.local_addr().unwrap(), admission);
        config.limits.io_timeout = Duration::from_millis(100);
        let host = Host::bind(SecretKey::generate(), Network::Loopback, config)
            .await
            .unwrap();
        let endpoint = client(key).await;
        let connection = endpoint.connect(host.addr(), ALPN).await.unwrap();
        let (mut send, mut recv) = connection.open_bi().await.unwrap();
        send.write_all(b"request").await.unwrap();
        let (_backend_socket, _) = backend.accept().await.unwrap();
        assert!(recv.read_to_end(1024).await.is_err());
        assert_eq!(host.stats().failed_streams, 1);
        endpoint.close().await;
        host.shutdown().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn host_shutdown_closes_active_backend_and_remote_connection() {
    timeout(DEADLINE, async {
        let backend = TcpListener::bind(loopback()).await.unwrap();
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
        let endpoint = client(key).await;
        let connection = endpoint.connect(host.addr(), ALPN).await.unwrap();
        let (mut send, _) = connection.open_bi().await.unwrap();
        send.write_all(b"a").await.unwrap();
        let (mut backend_socket, _) = backend.accept().await.unwrap();
        let mut byte = [0];
        backend_socket.read_exact(&mut byte).await.unwrap();
        assert_eq!(byte, *b"a");
        host.shutdown().await.unwrap();
        connection.closed().await;
        assert_eq!(backend_socket.read(&mut byte).await.unwrap(), 0);
        endpoint.close().await;
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn blocked_forwarder_requires_explicit_retry_after_admission_changes() {
    timeout(DEADLINE, async {
        let backend = TcpListener::bind(loopback()).await.unwrap();
        let key = SecretKey::generate();
        let admission = Admission::new();
        let host = Host::bind(
            SecretKey::generate(),
            Network::Loopback,
            HostConfig::new(backend.local_addr().unwrap(), admission.clone()),
        )
        .await
        .unwrap();
        let forwarder = Forwarder::bind(
            key.clone(),
            Network::Loopback,
            host.addr(),
            loopback(),
            Limits::default(),
        )
        .await
        .unwrap();

        let mut original = TcpStream::connect(forwarder.local_addr()).await.unwrap();
        // The peer can deny admission before the local write reaches its socket.
        let _ = original.write_all(b"denied request").await;
        assert_closed_without_response(&mut original).await;
        assert_eq!(host.stats().rejected_peers, 1);
        for _ in 0..3 {
            let mut denied = TcpStream::connect(forwarder.local_addr()).await.unwrap();
            let _ = denied.write_all(b"still denied").await;
            assert_closed_without_response(&mut denied).await;
        }
        assert_eq!(host.stats().rejected_peers, 1);
        assert_eq!(host.stats().backend_attempts, 0);

        assert!(admission.grant(key.public()));
        // Granting admission does not resurrect a failed TCP request or clear
        // the client's blocked state until its explicit recovery signal.
        let mut still_blocked = TcpStream::connect(forwarder.local_addr()).await.unwrap();
        let _ = still_blocked.write_all(b"needs retry signal").await;
        assert_closed_without_response(&mut still_blocked).await;
        assert_eq!(host.stats().backend_attempts, 0);
        forwarder.retry_now();

        let mut fresh = TcpStream::connect(forwarder.local_addr()).await.unwrap();
        fresh.write_all(b"new request").await.unwrap();
        let (mut service, _) = backend.accept().await.unwrap();
        let mut request = [0; 11];
        service.read_exact(&mut request).await.unwrap();
        assert_eq!(&request, b"new request");
        service.write_all(b"ok").await.unwrap();
        let mut response = [0; 2];
        fresh.read_exact(&mut response).await.unwrap();
        assert_eq!(&response, b"ok");
        assert_closed_without_response(&mut original).await;
        assert_eq!(host.stats().rejected_peers, 1);
        assert_eq!(host.stats().backend_attempts, 1);

        forwarder.shutdown().await.unwrap();
        host.shutdown().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn backend_close_after_receiving_request_never_replays_application_bytes() {
    timeout(DEADLINE, async {
        let backend = TcpListener::bind(loopback()).await.unwrap();
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
        let forwarder = Forwarder::bind(
            key,
            Network::Loopback,
            host.addr(),
            loopback(),
            Limits::default(),
        )
        .await
        .unwrap();

        let mut original = TcpStream::connect(forwarder.local_addr()).await.unwrap();
        original.write_all(b"side effect").await.unwrap();
        original.shutdown().await.unwrap();
        let (mut service, _) = backend.accept().await.unwrap();
        let mut marker = [0; 11];
        service.read_exact(&mut marker).await.unwrap();
        assert_eq!(&marker, b"side effect");
        // The backend has performed work, but closes without acknowledging it.
        // The forwarding layer cannot know whether replay would be safe.
        drop(service);
        assert_closed_without_response(&mut original).await;
        assert!(
            timeout(Duration::from_millis(300), backend.accept())
                .await
                .is_err(),
            "a failed request must not automatically open another backend connection"
        );
        assert_eq!(host.stats().backend_attempts, 1);

        let mut fresh = TcpStream::connect(forwarder.local_addr()).await.unwrap();
        fresh.write_all(b"new request").await.unwrap();
        fresh.shutdown().await.unwrap();
        let (mut service, _) = backend.accept().await.unwrap();
        let mut request = Vec::new();
        service.read_to_end(&mut request).await.unwrap();
        assert_eq!(&request, b"new request");
        service.write_all(b"ok").await.unwrap();
        service.shutdown().await.unwrap();
        let mut response = Vec::new();
        fresh.read_to_end(&mut response).await.unwrap();
        assert_eq!(&response, b"ok");
        assert_eq!(host.stats().backend_attempts, 2);

        forwarder.shutdown().await.unwrap();
        host.shutdown().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn forwarder_shutdown_cancels_pending_handshake_and_closes_tcp_client() {
    timeout(DEADLINE, async {
        // Keep the UDP address bound without answering so the QUIC handshake
        // remains in flight rather than failing immediately on a closed port.
        let blackhole = UdpSocket::bind(loopback()).await.unwrap();
        let remote = EndpointAddr::new(SecretKey::generate().public())
            .with_ip_addr(blackhole.local_addr().unwrap());
        let limits = Limits {
            connect_timeout: Duration::from_secs(60),
            ..Limits::default()
        };
        let forwarder = Forwarder::bind(
            SecretKey::generate(),
            Network::Loopback,
            remote,
            loopback(),
            limits,
        )
        .await
        .unwrap();
        let mut pending = TcpStream::connect(forwarder.local_addr()).await.unwrap();
        pending.write_all(b"pending request").await.unwrap();
        let mut packet = [0; 2048];
        let (length, _) = blackhole.recv_from(&mut packet).await.unwrap();
        assert!(length > 0, "the connection attempt must have started");

        timeout(Duration::from_secs(5), forwarder.shutdown())
            .await
            .expect("shutdown must not wait for the 60-second handshake deadline")
            .unwrap();
        assert_closed_without_response(&mut pending).await;
    })
    .await
    .unwrap();
}
