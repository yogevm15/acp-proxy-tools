use std::time::Duration;

use anyhow::{Result, bail};
use clap::ValueEnum;
use tracing::info;
use url::Url;

use crate::proxy::{Options, Out, Proxy};
use crate::transport::{Duplex, http, stdio, websocket};

#[derive(Clone, Copy, Debug, ValueEnum)]
pub enum RemoteTransport {
    Auto,
    Http,
    Websocket,
}

/// How to reach the real ACP agent. Every client connection gets its own agent connection.
#[derive(Clone, Debug)]
pub struct Upstream {
    pub url: Option<Url>,
    pub transport: RemoteTransport,
    pub headers: Vec<(String, String)>,
    pub http2_prior_knowledge: bool,
    pub command: Vec<String>,
}

impl Upstream {
    pub fn validate(&self) -> Result<()> {
        if self.url.is_none() && self.command.is_empty() {
            bail!("pass either `--url <URL>` or `-- <agent command>`");
        }
        Ok(())
    }

    pub async fn connect(&self) -> Result<Duplex> {
        self.validate()?;
        let Some(url) = &self.url else {
            return stdio::spawn_agent(&self.command);
        };
        let websocket = match self.transport {
            RemoteTransport::Websocket => true,
            RemoteTransport::Http => false,
            RemoteTransport::Auto => matches!(url.scheme(), "ws" | "wss"),
        };
        let mut url = url.clone();
        let scheme = match (websocket, url.scheme()) {
            (true, "http") => "ws",
            (true, "https") => "wss",
            (false, "ws") => "http",
            (false, "wss") => "https",
            (_, other) => other,
        }
        .to_owned();
        if url.set_scheme(&scheme).is_err() {
            bail!("unsupported URL scheme `{}`", url.scheme());
        }
        if websocket {
            websocket::connect(&url, &self.headers).await
        } else {
            http::connect(http::HttpOptions {
                url,
                headers: self.headers.clone(),
                http2_prior_knowledge: self.http2_prior_knowledge,
            })
        }
    }
}

/// Pumps messages between one client and one agent through a fresh [`Proxy`] until either side closes.
pub async fn bridge(client: Duplex, agent: Duplex, opts: Options) {
    let mut proxy = Proxy::new(opts);
    let Duplex { outgoing: to_client, incoming: mut from_client, writer: client_writer } = client;
    let Duplex { outgoing: to_agent, incoming: mut from_agent, writer: agent_writer } = agent;
    let route = |outs: Vec<Out>| {
        for out in outs {
            let _ = match out {
                Out::Client(msg) => to_client.send(msg),
                Out::Agent(msg) => to_agent.send(msg),
            };
        }
    };

    loop {
        tokio::select! {
            msg = from_client.recv() => match msg {
                Some(msg) => route(proxy.on_client(msg)),
                None => { info!("client disconnected"); break; }
            },
            msg = from_agent.recv() => match msg {
                Some(msg) => route(proxy.on_agent(msg)),
                None => { info!("agent disconnected"); break; }
            },
        }
    }

    drop(to_client);
    drop(to_agent);
    let _ = tokio::time::timeout(Duration::from_secs(5), async {
        let _ = client_writer.await;
        let _ = agent_writer.await;
    })
    .await;
}
