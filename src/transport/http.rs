//! Streamable HTTP profile of the ACP remote transport RFD.
//!
//! * `initialize` is POSTed without a connection id and answered with `200` + JSON
//!   and an `Acp-Connection-Id` header.
//! * Every later message is POSTed (usually answered `202`), carrying
//!   `Acp-Connection-Id` and, for session-scoped messages, `Acp-Session-Id`.
//! * Server-to-client messages arrive on long-lived SSE GET streams: one
//!   connection-scoped, plus one per session.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};

use anyhow::{Context, Result, bail};
use futures_util::StreamExt;
use reqwest::header::{ACCEPT, CONTENT_TYPE, HeaderMap, HeaderName, HeaderValue};
use serde_json::Value;
use tokio::sync::OnceCell;
use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};
use tracing::{debug, info, trace, warn};
use url::Url;

use super::Duplex;
use crate::jsonrpc::{self, Kind};

const CONNECTION_HEADER: &str = "acp-connection-id";
const SESSION_HEADER: &str = "acp-session-id";

pub struct HttpOptions {
    pub url: Url,
    pub headers: Vec<(String, String)>,
    pub http2_prior_knowledge: bool,
}

struct Inner {
    client: reqwest::Client,
    url: Url,
    connection_id: Mutex<Option<String>>,
    session_streams: Mutex<HashMap<String, Arc<OnceCell<()>>>>,
    /// Session of each in-flight agent-to-client request, so the reply carries `Acp-Session-Id`.
    agent_request_sessions: Mutex<HashMap<String, String>>,
    incoming: Mutex<Option<UnboundedSender<Value>>>,
}

pub fn connect(opts: HttpOptions) -> Result<Duplex> {
    let mut headers = HeaderMap::new();
    for (name, value) in &opts.headers {
        headers.append(HeaderName::from_bytes(name.as_bytes())?, HeaderValue::from_str(value)?);
    }
    let mut builder = reqwest::Client::builder().default_headers(headers).cookie_store(true);
    if opts.http2_prior_knowledge {
        builder = builder.http2_prior_knowledge();
    }
    let client = builder.build()?;

    let (in_tx, incoming) = unbounded_channel();
    let (outgoing, mut out_rx) = unbounded_channel::<Value>();
    let inner = Arc::new(Inner {
        client,
        url: opts.url,
        connection_id: Mutex::new(None),
        session_streams: Mutex::new(HashMap::new()),
        agent_request_sessions: Mutex::new(HashMap::new()),
        incoming: Mutex::new(Some(in_tx)),
    });

    let writer = tokio::spawn(async move {
        while let Some(msg) = out_rx.recv().await {
            inner.post(msg).await;
        }
        inner.terminate().await;
    });

    Ok(Duplex { outgoing, incoming, writer })
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

impl Inner {
    fn connection_id(&self) -> Option<String> {
        lock(&self.connection_id).clone()
    }

    fn deliver(self: &Arc<Self>, msg: Value) {
        match jsonrpc::kind(&msg) {
            Kind::Request { id, .. } => {
                if let Some(sid) = jsonrpc::session_id(&msg) {
                    lock(&self.agent_request_sessions).insert(jsonrpc::id_key(id), sid.to_owned());
                }
            }
            Kind::Response { .. } => {
                // `session/new` & co. answer on the connection stream; subscribe to the new session early.
                if let Some(sid) = msg.pointer("/result/sessionId").and_then(Value::as_str) {
                    let this = self.clone();
                    let sid = sid.to_owned();
                    tokio::spawn(async move { this.ensure_session_stream(&sid).await });
                }
            }
            _ => {}
        }
        if let Some(tx) = lock(&self.incoming).as_ref() {
            let _ = tx.send(msg);
        }
    }

    async fn post(self: &Arc<Self>, msg: Value) {
        let (is_initialize, request_id, session) = match jsonrpc::kind(&msg) {
            Kind::Request { id, method } => {
                (method == "initialize", Some(id.clone()), jsonrpc::session_id(&msg).map(str::to_owned))
            }
            Kind::Response { id } => (false, None, lock(&self.agent_request_sessions).remove(&jsonrpc::id_key(id))),
            _ => (false, None, jsonrpc::session_id(&msg).map(str::to_owned)),
        };
        if let Err(e) = self.try_post(&msg, is_initialize, session.as_deref()).await {
            warn!(error = format!("{e:#}"), "POST failed");
            if let Some(id) = request_id {
                let message = format!("upstream HTTP transport error: {e:#}");
                self.deliver(jsonrpc::error_response(id, -32603, message));
            }
        }
    }

    async fn try_post(self: &Arc<Self>, msg: &Value, is_initialize: bool, session: Option<&str>) -> Result<()> {
        let connection = self.connection_id();
        if connection.is_none() && !is_initialize {
            bail!("not connected: the first message must be `initialize`");
        }
        if let (Some(_), Some(sid)) = (&connection, session) {
            self.ensure_session_stream(sid).await;
        }

        let body = msg.to_string();
        trace!(%body, "POST");
        let mut request = self
            .client
            .post(self.url.clone())
            .header(CONTENT_TYPE, "application/json")
            .header(ACCEPT, "application/json, text/event-stream")
            .body(body);
        if let Some(c) = &connection {
            request = request.header(CONNECTION_HEADER, c);
        }
        if let Some(s) = session {
            request = request.header(SESSION_HEADER, s);
        }
        let response = request.send().await?;
        let status = response.status();
        if !status.is_success() {
            let text = response.text().await.unwrap_or_default();
            bail!("HTTP {status}: {}", text.trim());
        }

        let content_type =
            response.headers().get(CONTENT_TYPE).and_then(|v| v.to_str().ok()).unwrap_or_default().to_owned();

        if is_initialize {
            let header_id = response.headers().get(CONNECTION_HEADER).and_then(|v| v.to_str().ok()).map(str::to_owned);
            let body: Value = response.json().await.context("invalid `initialize` response body")?;
            let id = header_id.or_else(|| body.pointer("/result/connectionId")?.as_str().map(str::to_owned));
            match id {
                Some(id) => {
                    info!(url = %self.url, connection = %id, "connected (streamable HTTP)");
                    *lock(&self.connection_id) = Some(id);
                    self.open_stream(None).await.context("cannot open connection-scoped SSE stream")?;
                }
                None => warn!("`initialize` response has no Acp-Connection-Id"),
            }
            self.deliver(body);
            return Ok(());
        }

        if content_type.starts_with("text/event-stream") {
            let this = self.clone();
            tokio::spawn(async move { this.read_sse(response).await });
        } else if content_type.starts_with("application/json") {
            let bytes = response.bytes().await?;
            if !bytes.trim_ascii().is_empty() {
                self.deliver(serde_json::from_slice(&bytes).context("invalid JSON response body")?);
            }
        }
        Ok(())
    }

    async fn ensure_session_stream(self: &Arc<Self>, sid: &str) {
        let cell = lock(&self.session_streams).entry(sid.to_owned()).or_default().clone();
        cell.get_or_init(|| async {
            if let Err(e) = self.open_stream(Some(sid)).await {
                warn!(
                    session = %sid,
                    error = format!("{e:#}"),
                    "cannot open session-scoped SSE stream; relying on the connection stream"
                );
            }
        })
        .await;
    }

    async fn open_stream(self: &Arc<Self>, session: Option<&str>) -> Result<()> {
        let connection = self.connection_id().context("not connected")?;
        let mut request =
            self.client.get(self.url.clone()).header(ACCEPT, "text/event-stream").header(CONNECTION_HEADER, connection);
        if let Some(s) = session {
            request = request.header(SESSION_HEADER, s);
        }
        let response = request.send().await?.error_for_status()?;
        debug!(session = session.unwrap_or("-"), "SSE stream open");

        let this = self.clone();
        let session = session.map(str::to_owned);
        tokio::spawn(async move {
            this.read_sse(response).await;
            match session {
                None => {
                    info!("connection-scoped SSE stream closed");
                    lock(&this.incoming).take();
                }
                Some(sid) => {
                    debug!(session = %sid, "session-scoped SSE stream closed");
                    lock(&this.session_streams).remove(&sid);
                }
            }
        });
        Ok(())
    }

    async fn read_sse(self: &Arc<Self>, response: reqwest::Response) {
        let mut stream = response.bytes_stream();
        let mut buf = Vec::new();
        let mut data = String::new();
        while let Some(chunk) = stream.next().await {
            let chunk = match chunk {
                Ok(c) => c,
                Err(e) => {
                    warn!(error = %e, "SSE stream failed");
                    break;
                }
            };
            buf.extend_from_slice(&chunk);
            while let Some(pos) = buf.iter().position(|b| *b == b'\n') {
                let raw: Vec<u8> = buf.drain(..=pos).collect();
                let line = String::from_utf8_lossy(&raw);
                let line = line.trim_end_matches(['\n', '\r']);
                if line.is_empty() {
                    self.dispatch_event(&mut data);
                } else if let Some(rest) = line.strip_prefix("data:") {
                    if !data.is_empty() {
                        data.push('\n');
                    }
                    data.push_str(rest.strip_prefix(' ').unwrap_or(rest));
                }
            }
        }
        self.dispatch_event(&mut data);
    }

    fn dispatch_event(self: &Arc<Self>, data: &mut String) {
        if data.is_empty() {
            return;
        }
        trace!(%data, "SSE event");
        match serde_json::from_str::<Value>(data) {
            Ok(msg) => self.deliver(msg),
            Err(e) => warn!(error = %e, "ignoring non-JSON SSE event"),
        }
        data.clear();
    }

    async fn terminate(&self) {
        if let Some(connection) = self.connection_id() {
            let result = self.client.delete(self.url.clone()).header(CONNECTION_HEADER, connection).send().await;
            if let Err(e) = result {
                debug!(error = %e, "DELETE failed");
            }
        }
        lock(&self.incoming).take();
    }
}
