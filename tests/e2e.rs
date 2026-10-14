//! Runs the real `acp-proxy` binary against `examples/mock_agent.rs`, over every client-facing
//! and agent-facing transport.

use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;

use acp_proxy::transport::{Duplex, http, stdio, websocket};
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, Command};
use url::Url;

const TIMEOUT: Duration = Duration::from_secs(10);

fn mock_agent_path() -> String {
    let deps = std::env::current_exe().unwrap().parent().unwrap().to_owned();
    let path: PathBuf = deps.parent().unwrap().join("examples").join("mock_agent");
    path.to_str().unwrap().to_owned()
}

/// Spawns `program args...` and returns it together with the first line it prints.
async fn spawn_with_url(program: &str, args: &[&str]) -> (Child, String) {
    let mut child = Command::new(program)
        .args(args)
        .env("ACP_PROXY_LOG", "debug")
        .stdout(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut lines = BufReader::new(child.stdout.take().unwrap()).lines();
    let url = tokio::time::timeout(TIMEOUT, lines.next_line()).await.unwrap().unwrap().unwrap();
    (child, url)
}

#[derive(Clone, Copy)]
enum Setup {
    /// Client --stdio--> proxy --stdio--> agent.
    Stdio,
    /// Client --stdio--> proxy --WebSocket--> agent.
    UpstreamWebsocket,
    /// Client --stdio--> proxy --Streamable HTTP--> agent.
    UpstreamHttp,
    /// Client --WebSocket--> proxy (`--listen`) --stdio--> agent.
    ListenWebsocket,
    /// Client --Streamable HTTP--> proxy (`--listen`) --stdio--> agent.
    ListenHttp,
}

struct Client {
    conn: Duplex,
    _procs: Vec<Child>,
}

impl Client {
    async fn start(setup: Setup) -> Self {
        let proxy = env!("CARGO_BIN_EXE_acp-proxy");
        let agent = mock_agent_path();
        let mut procs = vec![];
        let conn = match setup {
            Setup::Stdio => stdio::spawn_agent(&[proxy.into(), "--".into(), agent]).unwrap(),
            Setup::UpstreamWebsocket | Setup::UpstreamHttp => {
                let flag = if matches!(setup, Setup::UpstreamWebsocket) { "--ws" } else { "--http" };
                let (child, url) = spawn_with_url(&agent, &[flag]).await;
                procs.push(child);
                stdio::spawn_agent(&[proxy.into(), "--url".into(), url]).unwrap()
            }
            Setup::ListenWebsocket | Setup::ListenHttp => {
                let (child, url) = spawn_with_url(proxy, &["--listen", "127.0.0.1:0", "--", &agent]).await;
                procs.push(child);
                let mut url: Url = url.parse().unwrap();
                if matches!(setup, Setup::ListenWebsocket) {
                    url.set_scheme("ws").unwrap();
                    websocket::connect(&url, &[]).await.unwrap()
                } else {
                    http::connect(http::HttpOptions { url, headers: vec![], http2_prior_knowledge: false }).unwrap()
                }
            }
        };
        Self { conn, _procs: procs }
    }

    fn send(&self, msg: Value) {
        self.conn.outgoing.send(msg).unwrap();
    }

    async fn recv(&mut self) -> Value {
        tokio::time::timeout(TIMEOUT, self.conn.incoming.recv()).await.expect("proxy reply in time").unwrap()
    }

    /// Sends a request and collects everything until its response.
    /// Agent-to-client requests are answered by `on_request`.
    async fn call(
        &mut self,
        id: u64,
        method: &str,
        params: Value,
        mut on_request: impl FnMut(&Value) -> Value,
    ) -> (Value, Vec<Value>) {
        self.send(json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params }));
        let mut seen = vec![];
        loop {
            let msg = self.recv().await;
            if msg.get("method").is_none() && msg["id"] == id {
                return (msg, seen);
            }
            if msg.get("method").is_some() && msg.get("id").is_some() {
                let result = on_request(&msg);
                self.send(json!({ "jsonrpc": "2.0", "id": msg["id"], "result": result }));
            }
            seen.push(msg);
        }
    }
}

fn no_requests(msg: &Value) -> Value {
    panic!("unexpected request reached the client: {msg}")
}

fn message_text(seen: &[Value]) -> String {
    seen.iter()
        .filter(|m| m["params"]["update"]["sessionUpdate"] == "agent_message_chunk")
        .map(|m| m["params"]["update"]["content"]["text"].as_str().unwrap().to_owned())
        .collect()
}

fn values(list: &Value, key: &str) -> Vec<String> {
    list.as_array().unwrap().iter().map(|v| v[key].as_str().unwrap().to_owned()).collect()
}

async fn duplicated_modes_flow(setup: Setup) {
    let mut c = Client::start(setup).await;
    let (init, _) =
        c.call(0, "initialize", json!({ "protocolVersion": 1, "clientCapabilities": {} }), no_requests).await;
    assert_eq!(init["result"]["protocolVersion"], 1, "{init}");

    let (new, _) = c.call(1, "session/new", json!({ "cwd": "/tmp", "mcpServers": [] }), no_requests).await;
    let result = &new["result"];
    let sid = result["sessionId"].as_str().unwrap().to_owned();
    let expected = [
        "agent",
        "agent#bypass-permissions",
        "plan",
        "plan#bypass-permissions",
        "debug",
        "debug#bypass-permissions",
        "ask",
        "ask#bypass-permissions",
    ];
    assert_eq!(values(&result["modes"]["availableModes"], "id"), expected);
    assert_eq!(values(&result["configOptions"][0]["options"], "value"), expected);
    assert_eq!(result["modes"]["availableModes"][7]["name"], "Ask - bypass permissions");

    // Normal mode: the client decides.
    let prompt = json!({ "sessionId": sid, "prompt": [{ "type": "text", "text": "hi" }] });
    let mut asked = 0;
    let (resp, seen) = c
        .call(2, "session/prompt", prompt.clone(), |req| {
            assert_eq!(req["method"], "session/request_permission");
            asked += 1;
            json!({ "outcome": { "outcome": "selected", "optionId": "reject-once" } })
        })
        .await;
    assert_eq!(resp["result"]["stopReason"], "end_turn");
    assert_eq!(asked, 1);
    assert!(message_text(&seen).contains("reject-once"));

    let set_mode = json!({ "sessionId": sid, "modeId": "plan#bypass-permissions" });
    let (resp, seen) = c.call(3, "session/set_mode", set_mode, no_requests).await;
    assert_eq!(resp["result"], json!({}), "{resp}");
    let sync = seen.iter().find(|m| m["params"]["update"]["sessionUpdate"] == "config_option_update").unwrap();
    assert_eq!(sync["params"]["update"]["configOptions"][0]["currentValue"], "plan#bypass-permissions");

    // Bypass mode: the proxy answers, the client never sees the request.
    let (resp, seen) = c.call(4, "session/prompt", prompt, no_requests).await;
    assert_eq!(resp["result"]["stopReason"], "end_turn");
    assert!(message_text(&seen).contains("allow-once"), "{seen:?}");
}

async fn checkbox_flow(setup: Setup) {
    let mut c = Client::start(setup).await;
    let caps = json!({ "session": { "configOptions": { "boolean": {} } } });
    c.call(0, "initialize", json!({ "protocolVersion": 1, "clientCapabilities": caps }), no_requests).await;
    let (new, _) = c.call(1, "session/new", json!({ "cwd": "/tmp", "mcpServers": [] }), no_requests).await;
    let sid = new["result"]["sessionId"].as_str().unwrap().to_owned();
    let options = &new["result"]["configOptions"];
    assert_eq!(values(options, "id"), ["mode", "bypass_permissions"]);
    assert_eq!(options[1]["type"], "boolean");
    assert_eq!(options[0]["options"].as_array().unwrap().len(), 4);

    let toggle = json!({ "sessionId": sid, "configId": "bypass_permissions", "type": "boolean", "value": true });
    let (resp, _) = c.call(2, "session/set_config_option", toggle, no_requests).await;
    assert_eq!(resp["result"]["configOptions"][1]["currentValue"], true);

    let set_mode = json!({ "sessionId": sid, "configId": "mode", "value": "plan" });
    let (resp, _) = c.call(3, "session/set_config_option", set_mode, no_requests).await;
    assert_eq!(values(&resp["result"]["configOptions"], "id"), ["mode", "bypass_permissions"]);
    assert_eq!(resp["result"]["configOptions"][0]["currentValue"], "plan");

    let prompt = json!({ "sessionId": sid, "prompt": [{ "type": "text", "text": "hi" }] });
    let (resp, seen) = c.call(4, "session/prompt", prompt, no_requests).await;
    assert_eq!(resp["result"]["stopReason"], "end_turn");
    assert!(message_text(&seen).contains("allow-once"), "{seen:?}");
}

macro_rules! flows {
    ($($name:ident: $setup:expr;)*) => {
        mod duplicated_modes {
            use super::*;
            $(#[tokio::test] async fn $name() { duplicated_modes_flow($setup).await })*
        }
        mod checkbox {
            use super::*;
            $(#[tokio::test] async fn $name() { checkbox_flow($setup).await })*
        }
    };
}

flows! {
    stdio: Setup::Stdio;
    upstream_websocket: Setup::UpstreamWebsocket;
    upstream_http: Setup::UpstreamHttp;
    listen_websocket: Setup::ListenWebsocket;
    listen_http: Setup::ListenHttp;
}

#[tokio::test]
async fn listener_rejects_browser_origins_and_missing_token() {
    let proxy = env!("CARGO_BIN_EXE_acp-proxy");
    let agent = mock_agent_path();
    let args =
        ["--listen", "127.0.0.1:0", "--listen-token", "s3cret", "--allow-origin", "https://ok.example", "--", &agent];
    let (_child, url) = spawn_with_url(proxy, &args).await;
    let init = json!({ "jsonrpc": "2.0", "id": 0, "method": "initialize", "params": { "protocolVersion": 1 } });
    let client = reqwest::Client::new();
    let post = |origin: Option<&str>, token: Option<&str>| {
        let mut req = client.post(&url).json(&init);
        if let Some(o) = origin {
            req = req.header("origin", o);
        }
        if let Some(t) = token {
            req = req.bearer_auth(t);
        }
        req.send()
    };

    assert_eq!(post(Some("https://evil.example"), Some("s3cret")).await.unwrap().status(), 403);
    assert_eq!(post(None, None).await.unwrap().status(), 401);
    assert_eq!(post(None, Some("wrong")).await.unwrap().status(), 401);

    let ok = post(Some("https://ok.example"), Some("s3cret")).await.unwrap();
    assert_eq!(ok.status(), 200);
    let connection = ok.headers()["acp-connection-id"].to_str().unwrap().to_owned();
    let body: Value = ok.json().await.unwrap();
    assert_eq!(body["result"]["connectionId"], connection.as_str());

    let deleted = client.delete(&url).bearer_auth("s3cret").header("acp-connection-id", &connection).send().await;
    assert_eq!(deleted.unwrap().status(), 202);
    let gone = client.delete(&url).bearer_auth("s3cret").header("acp-connection-id", &connection).send().await;
    assert_eq!(gone.unwrap().status(), 404);
}
