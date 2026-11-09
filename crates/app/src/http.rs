use std::convert::Infallible;
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http_body_util::{BodyExt, Full, Limited};
use hyper::body::Incoming;
use hyper::header::{AUTHORIZATION, CONTENT_TYPE, ORIGIN};
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::{TokioIo, TokioTimer};
use serde_json::{Value, json};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

use crate::auth::Token;
use crate::service::{Application, failure};
use crate::{MAX_RPC_BYTES, decode_json};

const MAX_CONNECTIONS: usize = 64;
const MAX_RESPONSE_BYTES: usize = 32 * 1024 * 1024;
const REQUEST_DEADLINE: Duration = Duration::from_secs(45);

fn response(status: StatusCode, value: Value) -> Response<Full<Bytes>> {
    let bytes = serde_json::to_vec(&value).expect("JSON value always encodes");
    Response::builder()
        .status(status)
        .header(CONTENT_TYPE, "application/json")
        .header("cache-control", "no-store")
        .header("x-content-type-options", "nosniff")
        .header("connection", "close")
        .body(Full::new(Bytes::from(bytes)))
        .expect("static response headers are valid")
}

async fn handle(
    request: Request<Incoming>,
    app: Arc<Application>,
    token: Token,
) -> Result<Response<Full<Bytes>>, Infallible> {
    if request.method() != Method::POST
        || request.uri().path() != "/rpc"
        || request.uri().query().is_some()
    {
        return Ok(response(
            StatusCode::NOT_FOUND,
            json!({"error":"unknown endpoint"}),
        ));
    }
    let credentials: Vec<_> = request.headers().get_all(AUTHORIZATION).iter().collect();
    if credentials.len() != 1 || !token.matches_header(credentials[0].as_bytes()) {
        return Ok(response(
            StatusCode::UNAUTHORIZED,
            json!({"error":"authentication required"}),
        ));
    }
    // Browser clients need an explicit origin policy before this native API admits them.
    if request.headers().contains_key(ORIGIN) {
        return Ok(response(
            StatusCode::FORBIDDEN,
            json!({"error":"browser origins are not enabled"}),
        ));
    }
    if request.headers().get_all(CONTENT_TYPE).iter().count() != 1
        || request
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.split(';').next())
            != Some("application/json")
    {
        return Ok(response(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            json!({"error":"application/json required"}),
        ));
    }
    let body = match tokio::time::timeout(
        Duration::from_secs(10),
        Limited::new(request.into_body(), MAX_RPC_BYTES).collect(),
    )
    .await
    {
        Ok(Ok(body)) => body.to_bytes(),
        Ok(Err(_)) => {
            return Ok(response(
                StatusCode::PAYLOAD_TOO_LARGE,
                json!({"error":"request body exceeds limit or is incomplete"}),
            ));
        }
        Err(_) => {
            return Ok(response(
                StatusCode::REQUEST_TIMEOUT,
                json!({"error":"request body timed out"}),
            ));
        }
    };
    let value = match decode_json(&body) {
        Ok(value) => value,
        Err(error) => {
            return Ok(response(
                StatusCode::BAD_REQUEST,
                failure(Value::Null, -32700, error, Value::Null),
            ));
        }
    };
    Ok(response(StatusCode::OK, app.handle(value).await))
}

/// Serve only on loopback. The existing authenticated Iroh forwarder can carry this connection.
pub async fn serve(
    listener: TcpListener,
    app: Arc<Application>,
    token: Token,
    shutdown: impl Future<Output = ()> + Send,
) -> io::Result<()> {
    if !listener.local_addr()?.ip().is_loopback() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "native server must listen on loopback",
        ));
    }
    let permits = Arc::new(Semaphore::new(MAX_CONNECTIONS));
    let mut connections = JoinSet::new();
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            _ = &mut shutdown => break,
            _ = connections.join_next(), if !connections.is_empty() => {},
            accepted = listener.accept() => {
                let (stream, _) = accepted?;
                let Ok(permit) = permits.clone().try_acquire_owned() else { continue };
                let app = app.clone();
                let token = token.clone();
                connections.spawn(async move {
                    let _permit = permit;
                    let service = service_fn(move |request| handle(request, app.clone(), token.clone()));
                    let mut builder = hyper::server::conn::http1::Builder::new();
                    builder.timer(TokioTimer::new())
                        .header_read_timeout(Duration::from_secs(10))
                        .max_headers(32).keep_alive(false);
                    // A lost reply is recovered by resubmitting the original durable command ID.
                    let _ = tokio::time::timeout(REQUEST_DEADLINE, builder.serve_connection(TokioIo::new(stream), service)).await;
                });
            }
        }
    }
    connections.shutdown().await;
    Ok(())
}

pub async fn request(address: SocketAddr, token: &Token, value: Value) -> io::Result<Value> {
    if !address.ip().is_loopback() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "use a loopback endpoint, directly or through Iroh forwarding",
        ));
    }
    let encoded = serde_json::to_vec(&value).map_err(io::Error::other)?;
    decode_json(&encoded).map_err(io::Error::other)?;
    tokio::time::timeout(REQUEST_DEADLINE, async {
        let stream = TcpStream::connect(address).await?;
        let (mut sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
            .await
            .map_err(io::Error::other)?;
        let request = Request::builder()
            .method(Method::POST)
            .uri("/rpc")
            .header("host", address.to_string())
            .header(AUTHORIZATION, token.header())
            .header(CONTENT_TYPE, "application/json")
            .header("connection", "close")
            .body(Full::new(Bytes::from(encoded)))
            .map_err(io::Error::other)?;
        let exchange = async {
            let response = sender
                .send_request(request)
                .await
                .map_err(io::Error::other)?;
            if response.status() != StatusCode::OK {
                return Err(io::Error::other(format!(
                    "server rejected request: HTTP {}",
                    response.status().as_u16()
                )));
            }
            let bytes = Limited::new(response.into_body(), MAX_RESPONSE_BYTES)
                .collect()
                .await
                .map_err(io::Error::other)?
                .to_bytes();
            serde_json::from_slice(&bytes).map_err(io::Error::other)
        };
        tokio::pin!(connection);
        tokio::pin!(exchange);
        tokio::select! {
            result = &mut exchange => result,
            result = &mut connection => {
                result.map_err(io::Error::other)?;
                exchange.await
            }
        }
    })
    .await
    .map_err(|_| {
        io::Error::new(
            io::ErrorKind::TimedOut,
            "request timed out; read its receipt before retrying",
        )
    })?
}
