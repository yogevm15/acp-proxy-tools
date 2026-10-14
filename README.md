# acp-proxy-tools

`acp-proxy` is an [Agent Client Protocol](https://agentclientprotocol.com) proxy. It sits between an
ACP client (Zed, JetBrains, …) and an ACP agent, and adds a **bypass permissions** switch to every
agent mode. When the switch is on, the proxy answers each `session/request_permission` with an
"allow" option. The client does not see the request.

An agent that exposes `agent`, `plan`, `debug` and `ask` becomes:

```text
agent
agent - bypass permissions
plan
plan - bypass permissions
debug
debug - bypass permissions
ask
ask - bypass permissions
```

## Checkbox or duplicated modes

ACP has [boolean session config options](https://agentclientprotocol.com/protocol/v1/session-config-options#boolean-config-options).
If the client advertises `clientCapabilities.session.configOptions.boolean`, the proxy keeps the
mode list as it is. It adds a **Bypass permissions** checkbox next to the mode selector instead.
Clients without that capability get the duplicated modes. To force one style, use `--ui modes` or
`--ui checkbox`.

In both styles the flag belongs to the session, not to the mode. If the agent changes its own
mode (for example, when it exits plan mode), bypass stays on. The proxy selects `allow_once` before
`allow_always`, so that bypass never saves "always allow" rules in the agent.

## Usage

```sh
cargo install --path .

# stdio agent: the proxy starts it as a subprocess
acp-proxy -- <agent command> [args...]

# remote agent, Streamable HTTP (POST + SSE)
acp-proxy --url https://agent.example.com/acp -H "Authorization: Bearer $TOKEN"

# remote agent, WebSocket
acp-proxy --url wss://agent.example.com/acp
```

For a stdio client, set the client's agent command to `acp-proxy`, for example in Zed:

```json
{
  "agent_servers": {
    "My agent (bypass)": {
      "command": "acp-proxy",
      "args": ["--", "<agent command>", "<args>"]
    }
  }
}
```

### HTTP listener

By default the proxy talks to its client over stdio. Use `--listen` to serve clients over HTTP instead:

```sh
acp-proxy --listen 127.0.0.1:8080 -- <agent command>
acp-proxy --listen 0.0.0.0:8080 --listen-token "$TOKEN" --url wss://agent.example.com/acp
```

The proxy serves Streamable HTTP and WebSocket on `/acp`. It prints the URL on stdout. Each client
connection gets its own agent (a new subprocess, or a new remote connection) and its own proxy state.
The proxy closes a connection when the client sends `DELETE`, when the WebSocket closes, or when a
Streamable HTTP client has no connection-scoped SSE stream open for 60 seconds.

The proxy approves tool calls automatically, so protect the listener:

- Requests that have an `Origin` header are refused (403). Browsers send this header, but native
  clients do not. This stops web pages from using the listener. Add `--allow-origin <ORIGIN>` for
  each browser origin that you trust.
- `--listen-token` (or `ACP_PROXY_LISTEN_TOKEN`) makes `Authorization: Bearer <token>` necessary.
  The proxy logs a warning if it listens on a non-loopback address without a token.
- The listener does not use TLS. Cleartext HTTP/2 (h2c) and HTTP/1.1 are both accepted. For remote
  access, put a TLS reverse proxy in front.

Logs go to stderr. Use `ACP_PROXY_LOG` to set the level, for example `ACP_PROXY_LOG=debug` or `ACP_PROXY_LOG=trace`. The
`trace` level shows every message.

## Remote transports

The HTTP transports (to the agent, and in `--listen` mode) follow the draft
[Streamable HTTP & WebSocket Transport RFD](https://agentclientprotocol.com/rfds/streamable-http-websocket-transport):

- **Streamable HTTP** (`http://`, `https://`). The proxy POSTs `initialize` and reads `Acp-Connection-Id`.
  Then it opens a connection-scoped SSE GET stream, and one SSE GET stream for each session. It sends
  `Acp-Session-Id` on session-scoped messages, keeps cookies, and sends `DELETE` on exit. HTTPS
  negotiates HTTP/2 with ALPN. Use `--http2-prior-knowledge` for cleartext h2c.
- **WebSocket** (`ws://`, `wss://`, or `--transport websocket`). One text frame holds one JSON-RPC message.

## Development

```sh
cargo test   # unit tests + end-to-end tests for every client-side and agent-side transport
cargo run --example mock_agent -- --http   # local test agent; also --ws, or no flag for stdio
```
