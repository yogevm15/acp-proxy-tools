//! WebSocket profile of the ACP remote transport RFD: one text frame per JSON-RPC message.

use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use tokio::sync::mpsc::unbounded_channel;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::{HeaderName, HeaderValue};
use tracing::{debug, info, trace, warn};
use url::Url;

use super::Duplex;

pub async fn connect(url: &Url, headers: &[(String, String)]) -> Result<Duplex> {
    let mut request = url.as_str().into_client_request()?;
    for (name, value) in headers {
        request.headers_mut().append(HeaderName::from_bytes(name.as_bytes())?, HeaderValue::from_str(value)?);
    }
    let (ws, response) = tokio_tungstenite::connect_async(request)
        .await
        .with_context(|| format!("WebSocket connection to {url} failed"))?;
    let connection_id = response.headers().get("acp-connection-id").and_then(|v| v.to_str().ok());
    info!(%url, connection = connection_id.unwrap_or("-"), "connected (websocket)");

    let (mut sink, mut stream) = ws.split();
    let (in_tx, incoming) = unbounded_channel();
    let (outgoing, mut out_rx) = unbounded_channel::<Value>();

    tokio::spawn(async move {
        while let Some(frame) = stream.next().await {
            match frame {
                Ok(Message::Text(text)) => match serde_json::from_str::<Value>(&text) {
                    Ok(msg) => {
                        trace!(%text, "recv");
                        if in_tx.send(msg).is_err() {
                            break;
                        }
                    }
                    Err(e) => warn!(error = %e, "ignoring non-JSON text frame"),
                },
                Ok(Message::Close(frame)) => {
                    debug!(?frame, "server closed websocket");
                    break;
                }
                Ok(_) => {}
                Err(e) => {
                    warn!(error = %e, "websocket read failed");
                    break;
                }
            }
        }
    });

    let writer = tokio::spawn(async move {
        while let Some(msg) = out_rx.recv().await {
            let text = msg.to_string();
            trace!(%text, "send");
            if let Err(e) = sink.send(Message::Text(text.into())).await {
                warn!(error = %e, "websocket write failed");
                break;
            }
        }
        let _ = sink.close().await;
    });

    Ok(Duplex { outgoing, incoming, writer })
}
