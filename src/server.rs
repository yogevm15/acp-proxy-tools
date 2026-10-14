//! Client-facing ACP listener: Streamable HTTP and WebSocket on a single `/acp` endpoint,
//! following the ACP "Streamable HTTP & WebSocket Transport" RFD.

use std::collections::HashMap;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use anyhow::{Context, Result};
use axum::Router;
use axum::body::Bytes;
use axum::extract::State;
use axum::extract::ws::rejection::WebSocketUpgradeRejection;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use tokio::sync::oneshot;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tracing::{debug, info, warn};

use crate::bridge::{Upstream, bridge};
use crate::jsonrpc::{self, Kind};
use crate::proxy::Options;
use crate::transport::Duplex;

const CONNECTION_HEADER: &str = "acp-connection-id";
const SESSION_HEADER: &str = "acp-session-id";
const INITIALIZE_TIMEOUT: Duration = Duration::from_secs(120);
/// How long a Streamable HTTP connection survives without a connection-scoped GET stream.
const ORPHAN_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Clone, Debug)]
pub struct ListenOptions {
    pub addr: SocketAddr,
    /// Required `Authorization: Bearer <token>`, if set.
    pub token: Option<String>,
    /// Browser origins allowed to connect. Requests carrying any other `Origin` are rejected.
    pub allowed_origins: Vec<String>,
}

struct Server {
    upstream: Upstream,
    proxy: Options,
    listen: ListenOptions,
    connections: Mutex<HashMap<String, Arc<Connection>>>,
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

pub async fn serve(listen: ListenOptions, upstream: Upstream, proxy: Options) -> Result<()> {
    upstream.validate()?;
    let listener = tokio::net::TcpListener::bind(listen.addr)
        .await
        .with_context(|| format!("cannot listen on {}", listen.addr))?;
    let local = listener.local_addr()?;
    if !local.ip().is_loopback() && listen.token.is_none() {
        warn!(
            "listening on a non-loopback address without --listen-token: anyone who can reach {local} \
             can drive the agent with permissions bypassed"
        );
    }
    info!(addr = %local, "listening (Streamable HTTP and WebSocket at /acp)");
    println!("http://{local}/acp");

    let server = Arc::new(Server { upstream, proxy, listen, connections: Mutex::new(HashMap::new()) });
    let app = Router::new().route("/acp", axum::routing::post(post).get(get).delete(delete)).with_state(server);
    axum::serve(listener, app).await?;
    Ok(())
}

fn header_str<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers.get(name).and_then(|v| v.to_str().ok())
}

type Rejection = (StatusCode, &'static str);

fn reject(status: StatusCode, message: &'static str) -> Response {
    (status, message).into_response()
}

impl Server {
    fn authorize(&self, headers: &HeaderMap) -> Result<(), Rejection> {
        if let Some(origin) = header_str(headers, header::ORIGIN.as_str())
            && !self.listen.allowed_origins.iter().any(|o| o == origin)
        {
            warn!(%origin, "rejected request from a disallowed origin");
            return Err((StatusCode::FORBIDDEN, "origin not allowed"));
        }
        if let Some(token) = &self.listen.token {
            let expected = format!("Bearer {token}");
            if header_str(headers, header::AUTHORIZATION.as_str()) != Some(expected.as_str()) {
                return Err((StatusCode::UNAUTHORIZED, "missing or invalid bearer token"));
            }
        }
        Ok(())
    }

    fn connection(&self, headers: &HeaderMap) -> Result<Arc<Connection>, Rejection> {
        let id =
            header_str(headers, CONNECTION_HEADER).ok_or((StatusCode::BAD_REQUEST, "missing Acp-Connection-Id"))?;
        lock(&self.connections).get(id).cloned().ok_or((StatusCode::NOT_FOUND, "unknown Acp-Connection-Id"))
    }

    fn remove(&self, id: &str) {
        if let Some(conn) = lock(&self.connections).remove(id) {
            conn.close();
        }
    }

    async fn initialize(self: Arc<Self>, msg: Value) -> Response {
        let request_id = msg["id"].clone();
        let agent = match self.upstream.connect().await {
            Ok(agent) => agent,
            Err(e) => {
                warn!(error = format!("{e:#}"), "cannot connect to agent");
                let error = jsonrpc::error_response(request_id, -32603, format!("cannot reach agent: {e:#}"));
                return (StatusCode::BAD_GATEWAY, axum::Json(error)).into_response();
            }
        };

        let id = uuid::Uuid::new_v4().to_string();
        let (init_tx, init_rx) = oneshot::channel();
        let (conn, client) = Connection::new(id.clone(), Some((jsonrpc::id_key(&request_id), init_tx)));
        lock(&self.connections).insert(id.clone(), conn.clone());
        info!(connection = %id, "client connected (streamable HTTP)");

        let server = self.clone();
        let opts = self.proxy.clone();
        let conn_id = id.clone();
        tokio::spawn(async move {
            bridge(client, agent, opts).await;
            server.remove(&conn_id);
            info!(connection = %conn_id, "connection closed");
        });
        conn.watch_orphan(self.clone(), conn.stream_generation.load(Ordering::SeqCst));
        conn.forward(msg);

        let mut response = match tokio::time::timeout(INITIALIZE_TIMEOUT, init_rx).await {
            Ok(Ok(response)) => response,
            _ => {
                self.remove(&id);
                return reject(StatusCode::GATEWAY_TIMEOUT, "agent did not answer `initialize`");
            }
        };
        if let Some(result) = response.get_mut("result").and_then(Value::as_object_mut) {
            result.insert("connectionId".into(), Value::String(id.clone()));
        }
        let mut response = axum::Json(response).into_response();
        response.headers_mut().insert(CONNECTION_HEADER, HeaderValue::from_str(&id).expect("uuid is ASCII"));
        response
    }

    async fn websocket(self: Arc<Self>, socket: WebSocket, id: String) {
        info!(connection = %id, "client connected (websocket)");
        let agent = match self.upstream.connect().await {
            Ok(agent) => agent,
            Err(e) => {
                warn!(connection = %id, error = format!("{e:#}"), "cannot connect to agent");
                return;
            }
        };
        let (mut sink, mut stream) = socket.split();
        let (to_proxy, incoming) = unbounded_channel();
        let (outgoing, mut out_rx) = unbounded_channel::<Value>();
        tokio::spawn(async move {
            while let Some(Ok(frame)) = stream.next().await {
                match frame {
                    Message::Text(text) => match serde_json::from_str::<Value>(&text) {
                        Ok(msg) => {
                            if to_proxy.send(msg).is_err() {
                                break;
                            }
                        }
                        Err(e) => warn!(error = %e, "ignoring non-JSON text frame"),
                    },
                    Message::Close(_) => break,
                    _ => {}
                }
            }
        });
        let writer = tokio::spawn(async move {
            while let Some(msg) = out_rx.recv().await {
                if sink.send(Message::Text(msg.to_string().into())).await.is_err() {
                    break;
                }
            }
            let _ = sink.close().await;
        });
        bridge(Duplex { outgoing, incoming, writer }, agent, self.proxy.clone()).await;
        info!(connection = %id, "connection closed");
    }
}

async fn post(State(server): State<Arc<Server>>, headers: HeaderMap, body: Bytes) -> Response {
    if let Err(r) = server.authorize(&headers) {
        return r.into_response();
    }
    if !header_str(&headers, header::CONTENT_TYPE.as_str()).is_some_and(|c| c.starts_with("application/json")) {
        return reject(StatusCode::UNSUPPORTED_MEDIA_TYPE, "Content-Type must be application/json");
    }
    let msg: Value = match serde_json::from_slice(&body) {
        Ok(msg) => msg,
        Err(_) => return reject(StatusCode::BAD_REQUEST, "invalid JSON"),
    };
    if msg.is_array() {
        return reject(StatusCode::NOT_IMPLEMENTED, "batch requests are not supported");
    }
    if header_str(&headers, CONNECTION_HEADER).is_none() {
        return match jsonrpc::kind(&msg) {
            Kind::Request { method: "initialize", .. } => server.initialize(msg).await,
            _ => reject(StatusCode::BAD_REQUEST, "missing Acp-Connection-Id"),
        };
    }
    let conn = match server.connection(&headers) {
        Ok(conn) => conn,
        Err(r) => return r.into_response(),
    };
    if let Kind::Request { .. } | Kind::Notification { .. } = jsonrpc::kind(&msg)
        && let Some(sid) = jsonrpc::session_id(&msg)
        && header_str(&headers, SESSION_HEADER) != Some(sid)
    {
        return reject(StatusCode::BAD_REQUEST, "session-scoped message without matching Acp-Session-Id");
    }
    if !conn.forward(msg) {
        return reject(StatusCode::NOT_FOUND, "connection closed");
    }
    StatusCode::ACCEPTED.into_response()
}

async fn get(
    State(server): State<Arc<Server>>,
    headers: HeaderMap,
    ws: Result<WebSocketUpgrade, WebSocketUpgradeRejection>,
) -> Response {
    if let Err(r) = server.authorize(&headers) {
        return r.into_response();
    }
    if header_str(&headers, header::UPGRADE.as_str()).is_some_and(|u| u.eq_ignore_ascii_case("websocket")) {
        let ws = match ws {
            Ok(ws) => ws,
            Err(rejection) => return rejection.into_response(),
        };
        let id = uuid::Uuid::new_v4().to_string();
        let header_value = HeaderValue::from_str(&id).expect("uuid is ASCII");
        let mut response = ws.on_upgrade(move |socket| server.websocket(socket, id));
        response.headers_mut().insert(CONNECTION_HEADER, header_value);
        return response;
    }
    if !header_str(&headers, header::ACCEPT.as_str()).is_some_and(|a| a.contains("text/event-stream")) {
        return reject(StatusCode::NOT_ACCEPTABLE, "Accept must include text/event-stream");
    }
    let conn = match server.connection(&headers) {
        Ok(conn) => conn,
        Err(r) => return r.into_response(),
    };
    let session = header_str(&headers, SESSION_HEADER).map(str::to_owned);
    debug!(connection = %conn.id, session = session.as_deref().unwrap_or("-"), "SSE stream open");

    let guard = session.is_none().then(|| StreamGuard {
        server: server.clone(),
        conn: conn.clone(),
        generation: conn.stream_generation.fetch_add(1, Ordering::SeqCst) + 1,
    });
    let events = UnboundedReceiverStream::new(conn.open_stream(session)).map(move |msg| {
        let _keep_alive = &guard;
        Ok::<_, Infallible>(Event::default().data(msg.to_string()))
    });
    Sse::new(events).keep_alive(KeepAlive::default()).into_response()
}

async fn delete(State(server): State<Arc<Server>>, headers: HeaderMap) -> Response {
    if let Err(r) = server.authorize(&headers) {
        return r.into_response();
    }
    match server.connection(&headers) {
        Ok(conn) => {
            info!(connection = %conn.id, "client terminated connection");
            server.remove(&conn.id);
            StatusCode::ACCEPTED.into_response()
        }
        Err(r) => r.into_response(),
    }
}

/// A server-to-client SSE stream. Messages are buffered until the client opens (or reopens) it.
struct Slot {
    tx: UnboundedSender<Value>,
    rx: Option<UnboundedReceiver<Value>>,
}

impl Slot {
    fn new() -> Self {
        let (tx, rx) = unbounded_channel();
        Self { tx, rx: Some(rx) }
    }
}

type InitWaiter = (String, oneshot::Sender<Value>);

/// One Streamable HTTP client connection, bridged to its own agent.
struct Connection {
    id: String,
    to_proxy: Mutex<Option<UnboundedSender<Value>>>,
    /// Keyed by session id; `None` is the connection-scoped stream.
    streams: Mutex<HashMap<Option<String>, Slot>>,
    /// Session of each in-flight client request, so its response goes to that session's stream.
    request_sessions: Mutex<HashMap<String, String>>,
    init_waiter: Mutex<Option<InitWaiter>>,
    stream_generation: AtomicU64,
}

impl Connection {
    /// Returns the connection and the client-side [`Duplex`] to hand to the bridge.
    fn new(id: String, init_waiter: Option<InitWaiter>) -> (Arc<Self>, Duplex) {
        let (to_proxy, incoming) = unbounded_channel();
        let (outgoing, mut out_rx) = unbounded_channel::<Value>();
        let conn = Arc::new(Self {
            id,
            to_proxy: Mutex::new(Some(to_proxy)),
            streams: Mutex::new(HashMap::new()),
            request_sessions: Mutex::new(HashMap::new()),
            init_waiter: Mutex::new(init_waiter),
            stream_generation: AtomicU64::new(0),
        });
        let router = conn.clone();
        let writer = tokio::spawn(async move {
            while let Some(msg) = out_rx.recv().await {
                router.route(msg);
            }
        });
        (conn, Duplex { outgoing, incoming, writer })
    }

    fn forward(&self, msg: Value) -> bool {
        if let Kind::Request { id, .. } = jsonrpc::kind(&msg)
            && let Some(sid) = jsonrpc::session_id(&msg)
        {
            lock(&self.request_sessions).insert(jsonrpc::id_key(id), sid.to_owned());
        }
        lock(&self.to_proxy).as_ref().is_some_and(|tx| tx.send(msg).is_ok())
    }

    fn close(&self) {
        lock(&self.to_proxy).take();
    }

    fn route(&self, msg: Value) {
        let session = match jsonrpc::kind(&msg) {
            Kind::Response { id } => {
                let key = jsonrpc::id_key(id);
                let mut waiter = lock(&self.init_waiter);
                if waiter.as_ref().is_some_and(|(k, _)| *k == key) {
                    let (_, tx) = waiter.take().expect("checked above");
                    let _ = tx.send(msg);
                    return;
                }
                drop(waiter);
                lock(&self.request_sessions).remove(&key)
            }
            _ => jsonrpc::session_id(&msg).map(str::to_owned),
        };
        let mut streams = lock(&self.streams);
        let slot = streams.entry(session).or_insert_with(Slot::new);
        if let Err(unsent) = slot.tx.send(msg) {
            // The client dropped this stream: buffer for its next GET.
            *slot = Slot::new();
            let _ = slot.tx.send(unsent.0);
        }
    }

    fn open_stream(&self, session: Option<String>) -> UnboundedReceiver<Value> {
        let mut streams = lock(&self.streams);
        let slot = streams.entry(session).or_insert_with(Slot::new);
        if slot.rx.is_none() {
            *slot = Slot::new();
        }
        slot.rx.take().expect("fresh slot has a receiver")
    }

    /// Closes the connection if no connection-scoped stream is opened after `generation`.
    fn watch_orphan(self: &Arc<Self>, server: Arc<Server>, generation: u64) {
        let conn = self.clone();
        tokio::spawn(async move {
            tokio::time::sleep(ORPHAN_TIMEOUT).await;
            if conn.stream_generation.load(Ordering::SeqCst) == generation {
                info!(connection = %conn.id, "no connection-scoped stream, closing connection");
                server.remove(&conn.id);
            }
        });
    }
}

/// Lives as long as one connection-scoped SSE response.
struct StreamGuard {
    server: Arc<Server>,
    conn: Arc<Connection>,
    generation: u64,
}

impl Drop for StreamGuard {
    fn drop(&mut self) {
        debug!(connection = %self.conn.id, "connection-scoped SSE stream closed");
        self.conn.watch_orphan(self.server.clone(), self.generation);
    }
}
