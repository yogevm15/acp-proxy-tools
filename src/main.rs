use std::net::SocketAddr;
use std::process::ExitCode;
use std::time::Duration;

use acp_proxy::bridge::{RemoteTransport, Upstream, bridge};
use acp_proxy::proxy::{Options, UiStyle};
use acp_proxy::server::{self, ListenOptions};
use acp_proxy::transport::stdio;
use anyhow::Result;
use clap::Parser;
use tracing::error;
use tracing_subscriber::EnvFilter;
use url::Url;

/// ACP proxy that adds a "bypass permissions" variant to every mode of an ACP agent.
///
/// Clients talk to the proxy over stdio (default) or over HTTP with `--listen`. The proxy
/// reaches the real agent either by launching it (`-- <command>`) or over the network (`--url`).
#[derive(Parser)]
#[command(version)]
struct Cli {
    /// URL of a remote ACP agent: `http(s)://` uses Streamable HTTP, `ws(s)://` uses WebSocket.
    #[arg(long, env = "ACP_PROXY_URL", conflicts_with = "command")]
    url: Option<Url>,

    /// Remote transport. `auto` picks it from the URL scheme.
    #[arg(long, value_enum, default_value_t = RemoteTransport::Auto)]
    transport: RemoteTransport,

    /// Extra header sent to the remote agent, as `Name: value`. Repeatable.
    #[arg(short = 'H', long = "header", value_name = "HEADER", value_parser = parse_header)]
    headers: Vec<(String, String)>,

    /// Use HTTP/2 without TLS negotiation (h2c) for Streamable HTTP.
    #[arg(long)]
    http2_prior_knowledge: bool,

    /// Serve clients over HTTP at `/acp` (Streamable HTTP and WebSocket) instead of stdio, e.g. `127.0.0.1:8080`.
    #[arg(long, value_name = "ADDR")]
    listen: Option<SocketAddr>,

    /// Require `Authorization: Bearer <TOKEN>` from clients of `--listen`.
    #[arg(long, env = "ACP_PROXY_LISTEN_TOKEN", requires = "listen", hide_env_values = true)]
    listen_token: Option<String>,

    /// Browser origin allowed to connect to `--listen`; requests with any other `Origin` are rejected. Repeatable.
    #[arg(long = "allow-origin", value_name = "ORIGIN", requires = "listen")]
    allowed_origins: Vec<String>,

    /// How the bypass switch is shown to the client.
    #[arg(long, value_enum, default_value_t = UiStyle::Auto)]
    ui: UiStyle,

    /// Suffix for the names of duplicated modes.
    #[arg(long, default_value = " - bypass permissions")]
    name_suffix: String,

    /// Command (with arguments) that starts a stdio ACP agent.
    #[arg(last = true, value_name = "COMMAND")]
    command: Vec<String>,
}

fn parse_header(raw: &str) -> Result<(String, String), String> {
    let (name, value) = raw.split_once(':').ok_or("expected `Name: value`")?;
    Ok((name.trim().to_owned(), value.trim().to_owned()))
}

fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_ansi(false)
        .with_env_filter(EnvFilter::try_from_env("ACP_PROXY_LOG").unwrap_or_else(|_| EnvFilter::new("info")))
        .init();
    let cli = Cli::parse();

    let runtime = tokio::runtime::Runtime::new().expect("failed to start tokio runtime");
    let code = match runtime.block_on(run(cli)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            error!("{e:#}");
            ExitCode::FAILURE
        }
    };
    // Blocking stdin reads would otherwise keep the runtime alive.
    runtime.shutdown_timeout(Duration::from_millis(500));
    code
}

async fn run(cli: Cli) -> Result<()> {
    let upstream = Upstream {
        url: cli.url,
        transport: cli.transport,
        headers: cli.headers,
        http2_prior_knowledge: cli.http2_prior_knowledge,
        command: cli.command,
    };
    let opts = Options { ui: cli.ui, name_suffix: cli.name_suffix };

    if let Some(addr) = cli.listen {
        let listen = ListenOptions { addr, token: cli.listen_token, allowed_origins: cli.allowed_origins };
        return server::serve(listen, upstream, opts).await;
    }

    let agent = upstream.connect().await?;
    let client = stdio::from_io(tokio::io::stdin(), tokio::io::stdout(), "client");
    bridge(client, agent, opts).await;
    Ok(())
}
