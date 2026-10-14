//! Minimal ACP agent used to exercise `acp-proxy`.
//!
//! ```text
//! mock_agent           # stdio
//! mock_agent --ws      # WebSocket on a random local port, prints the URL on stdout
//! mock_agent --http    # Streamable HTTP on a random local port, prints the URL on stdout
//! ```
//!
//! Every `session/prompt` asks for permission once and then reports the
//! outcome it received in an `agent_message_chunk`.

use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::{Arc, Mutex};

use axum::Router;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::sse::{Event, Sse};
use axum::response::{IntoResponse, Response};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_tungstenite::tungstenite::Message;

const MODES: [&str; 4] = ["agent", "plan", "debug", "ask"];

#[derive(Default)]
struct Agent {
    mode: String,
    sessions: u32,
    permissions: u32,
    pending_prompts: HashMap<String, (Value, String)>,
}

fn response(id: Value, result: Value) -> Value {
    json!({ "jsonrpc": "2.0", "id": id, "result": result })
}

fn update(sid: &str, update: Value) -> Value {
    json!({ "jsonrpc": "2.0", "method": "session/update", "params": { "sessionId": sid, "update": update } })
}

fn title(mode: &str) -> String {
    let mut c = mode.chars();
    c.next().map(|f| f.to_uppercase().chain(c).collect()).unwrap_or_default()
}

impl Agent {
    fn modes(&self) -> Value {
        let available: Vec<Value> = MODES.iter().map(|m| json!({ "id": m, "name": title(m) })).collect();
        json!({ "currentModeId": self.mode, "availableModes": available })
    }

    fn config(&self) -> Value {
        let values: Vec<Value> = MODES.iter().map(|m| json!({ "value": m, "name": title(m) })).collect();
        json!([{ "id": "mode", "name": "Mode", "category": "mode", "type": "select", "currentValue": self.mode, "options": values }])
    }

    fn handle(&mut self, msg: Value) -> Vec<Value> {
        let id = msg.get("id").cloned().unwrap_or(Value::Null);
        let params = &msg["params"];
        let sid = params["sessionId"].as_str().unwrap_or_default().to_owned();
        let Some(method) = msg.get("method").and_then(Value::as_str) else {
            // A reply to one of our permission requests.
            let Some((prompt_id, sid)) = self.pending_prompts.remove(&id.to_string()) else { return vec![] };
            let text = format!("permission outcome: {}", msg["result"]["outcome"]);
            return vec![
                update(
                    &sid,
                    json!({ "sessionUpdate": "agent_message_chunk", "content": { "type": "text", "text": text } }),
                ),
                response(prompt_id, json!({ "stopReason": "end_turn" })),
            ];
        };
        if self.mode.is_empty() {
            self.mode = "agent".into();
        }
        match method {
            "initialize" => {
                vec![response(id, json!({ "protocolVersion": 1, "agentCapabilities": {}, "authMethods": [] }))]
            }
            "session/new" => {
                self.sessions += 1;
                let sid = format!("sess_{}", self.sessions);
                vec![response(id, json!({ "sessionId": sid, "modes": self.modes(), "configOptions": self.config() }))]
            }
            "session/set_mode" => {
                self.mode = params["modeId"].as_str().unwrap_or_default().to_owned();
                vec![response(id, json!({}))]
            }
            "session/set_config_option" => {
                self.mode = params["value"].as_str().unwrap_or_default().to_owned();
                vec![response(id, json!({ "configOptions": self.config() }))]
            }
            "session/prompt" => {
                self.permissions += 1;
                let perm_id = json!(format!("perm-{}", self.permissions));
                self.pending_prompts.insert(perm_id.to_string(), (id, sid.clone()));
                let tool_call =
                    json!({ "toolCallId": "call_1", "title": "Write file", "kind": "edit", "status": "pending" });
                vec![
                    update(
                        &sid,
                        json!({ "sessionUpdate": "tool_call", "toolCallId": "call_1", "title": "Write file" }),
                    ),
                    json!({
                        "jsonrpc": "2.0",
                        "id": perm_id,
                        "method": "session/request_permission",
                        "params": {
                            "sessionId": sid,
                            "toolCall": tool_call,
                            "options": [
                                { "optionId": "allow-always", "name": "Always allow", "kind": "allow_always" },
                                { "optionId": "allow-once", "name": "Allow", "kind": "allow_once" },
                                { "optionId": "reject-once", "name": "Reject", "kind": "reject_once" },
                            ],
                        },
                    }),
                ]
            }
            _ if msg.get("id").is_some() => vec![json!({
                "jsonrpc": "2.0", "id": id, "error": { "code": -32601, "message": format!("unknown method {method}") }
            })],
            _ => vec![],
        }
    }
}

#[tokio::main]
async fn main() {
    match std::env::args().nth(1).as_deref() {
        None => stdio().await,
        Some("--ws") => websocket().await,
        Some("--http") => http().await,
        Some(other) => panic!("unknown argument {other}"),
    }
}

async fn stdio() {
    let mut agent = Agent::default();
    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    let mut stdout = tokio::io::stdout();
    while let Ok(Some(line)) = lines.next_line().await {
        for out in agent.handle(serde_json::from_str(&line).expect("valid JSON")) {
            stdout.write_all(format!("{out}\n").as_bytes()).await.unwrap();
            stdout.flush().await.unwrap();
        }
    }
}

async fn websocket() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    println!("ws://{}/acp", listener.local_addr().unwrap());
    loop {
        let (socket, _) = listener.accept().await.unwrap();
        tokio::spawn(async move {
            let mut ws = tokio_tungstenite::accept_async(socket).await.unwrap();
            let mut agent = Agent::default();
            while let Some(Ok(frame)) = ws.next().await {
                let Message::Text(text) = frame else { continue };
                for out in agent.handle(serde_json::from_str(&text).unwrap()) {
                    ws.send(Message::Text(out.to_string().into())).await.unwrap();
                }
            }
        });
    }
}

/// A server-to-client SSE stream; messages are buffered until the client opens it.
struct Stream {
    tx: UnboundedSender<Value>,
    rx: Option<UnboundedReceiver<Value>>,
}

impl Default for Stream {
    fn default() -> Self {
        let (tx, rx) = unbounded_channel();
        Self { tx, rx: Some(rx) }
    }
}

#[derive(Default)]
struct Connection {
    agent: Agent,
    streams: HashMap<Option<String>, Stream>,
    request_sessions: HashMap<String, String>,
}

#[derive(Default)]
struct HttpState {
    next_id: u32,
    connections: HashMap<String, Connection>,
}

type Shared = Arc<Mutex<HttpState>>;

async fn http() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    println!("http://{}/acp", listener.local_addr().unwrap());
    let app = Router::new()
        .route("/acp", axum::routing::post(http_post).get(http_get).delete(http_delete))
        .with_state(Shared::default());
    axum::serve(listener, app).await.unwrap();
}

fn header<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers.get(name).and_then(|v| v.to_str().ok())
}

async fn http_post(State(state): State<Shared>, headers: HeaderMap, body: String) -> Response {
    let msg: Value = serde_json::from_str(&body).unwrap();
    let mut state = state.lock().unwrap();
    if msg["method"] == "initialize" {
        state.next_id += 1;
        let cid = format!("conn_{}", state.next_id);
        let mut conn = Connection::default();
        let out = conn.agent.handle(msg).remove(0);
        state.connections.insert(cid.clone(), conn);
        return ([("acp-connection-id", cid)], axum::Json(out)).into_response();
    }
    let Some(conn) = header(&headers, "acp-connection-id").and_then(|c| state.connections.get_mut(c)) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    if let Some(sid) = msg["params"]["sessionId"].as_str() {
        if header(&headers, "acp-session-id") != Some(sid) {
            return (StatusCode::BAD_REQUEST, "missing Acp-Session-Id").into_response();
        }
        if msg.get("method").is_some() && msg.get("id").is_some() {
            conn.request_sessions.insert(msg["id"].to_string(), sid.to_owned());
        }
    }
    for out in conn.agent.handle(msg) {
        let sid = match out["params"]["sessionId"].as_str() {
            Some(sid) => Some(sid.to_owned()),
            None => conn.request_sessions.remove(&out["id"].to_string()),
        };
        let _ = conn.streams.entry(sid).or_default().tx.send(out);
    }
    StatusCode::ACCEPTED.into_response()
}

async fn http_get(State(state): State<Shared>, headers: HeaderMap) -> Response {
    let mut state = state.lock().unwrap();
    let Some(conn) = header(&headers, "acp-connection-id").and_then(|c| state.connections.get_mut(c)) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let key = header(&headers, "acp-session-id").map(str::to_owned);
    let Some(rx) = conn.streams.entry(key).or_default().rx.take() else {
        return StatusCode::CONFLICT.into_response();
    };
    let events = UnboundedReceiverStream::new(rx).map(|v| Ok::<_, Infallible>(Event::default().data(v.to_string())));
    Sse::new(events).into_response()
}

async fn http_delete(State(state): State<Shared>, headers: HeaderMap) -> StatusCode {
    let mut state = state.lock().unwrap();
    match header(&headers, "acp-connection-id").and_then(|c| state.connections.remove(c)) {
        Some(_) => StatusCode::ACCEPTED,
        None => StatusCode::NOT_FOUND,
    }
}
