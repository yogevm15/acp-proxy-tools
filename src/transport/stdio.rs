use std::process::Stdio;

use anyhow::{Context, Result};
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::process::Command;
use tokio::sync::mpsc::unbounded_channel;
use tracing::{debug, info, trace, warn};

use super::Duplex;

/// Newline-delimited JSON-RPC over a reader/writer pair.
pub fn from_io<R, W>(reader: R, mut writer: W, peer: &'static str) -> Duplex
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
{
    let (in_tx, incoming) = unbounded_channel();
    let (outgoing, mut out_rx) = unbounded_channel::<Value>();

    tokio::spawn(async move {
        let mut lines = BufReader::new(reader).lines();
        loop {
            match lines.next_line().await {
                Ok(Some(line)) => {
                    let line = line.trim();
                    if line.is_empty() {
                        continue;
                    }
                    match serde_json::from_str::<Value>(line) {
                        Ok(msg) => {
                            trace!(peer, %line, "recv");
                            if in_tx.send(msg).is_err() {
                                break;
                            }
                        }
                        Err(e) => warn!(peer, error = %e, %line, "ignoring non-JSON line"),
                    }
                }
                Ok(None) => break,
                Err(e) => {
                    warn!(peer, error = %e, "read failed");
                    break;
                }
            }
        }
        debug!(peer, "input closed");
    });

    let writer = tokio::spawn(async move {
        while let Some(msg) = out_rx.recv().await {
            let mut line = msg.to_string();
            trace!(peer, %line, "send");
            line.push('\n');
            if let Err(e) = async {
                writer.write_all(line.as_bytes()).await?;
                writer.flush().await
            }
            .await
            {
                warn!(peer, error = %e, "write failed");
                break;
            }
        }
        let _ = writer.shutdown().await;
    });

    Duplex { outgoing, incoming, writer }
}

/// Launches a stdio ACP agent as a subprocess. Its stderr is inherited.
pub fn spawn_agent(command: &[String]) -> Result<Duplex> {
    let (program, args) = command.split_first().context("empty agent command")?;
    let mut child = Command::new(program)
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .kill_on_drop(true)
        .spawn()
        .with_context(|| format!("failed to start agent `{program}`"))?;
    info!(command = ?command, pid = child.id(), "started agent");

    let stdin = child.stdin.take().context("agent stdin unavailable")?;
    let stdout = child.stdout.take().context("agent stdout unavailable")?;
    tokio::spawn(async move {
        match child.wait().await {
            Ok(status) => info!(%status, "agent exited"),
            Err(e) => warn!(error = %e, "failed to wait for agent"),
        }
    });
    Ok(from_io(stdout, stdin, "agent"))
}
