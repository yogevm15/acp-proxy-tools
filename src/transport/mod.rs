pub mod http;
pub mod stdio;
pub mod websocket;

use serde_json::Value;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::task::JoinHandle;

/// A bidirectional stream of JSON-RPC messages with one peer.
///
/// Dropping `outgoing` closes the connection gracefully; `writer` completes once
/// everything queued before that has been flushed.
pub struct Duplex {
    pub outgoing: UnboundedSender<Value>,
    pub incoming: UnboundedReceiver<Value>,
    pub writer: JoinHandle<()>,
}
