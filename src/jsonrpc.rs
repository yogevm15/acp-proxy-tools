use serde_json::{Value, json};

pub enum Kind<'a> {
    Request { id: &'a Value, method: &'a str },
    Notification { method: &'a str },
    Response { id: &'a Value },
    Invalid,
}

pub fn kind(msg: &Value) -> Kind<'_> {
    let method = msg.get("method").and_then(Value::as_str);
    let id = msg.get("id").filter(|id| !id.is_null());
    match (method, id) {
        (Some(method), Some(id)) => Kind::Request { id, method },
        (Some(method), None) => Kind::Notification { method },
        (None, Some(id)) if msg.get("result").is_some() || msg.get("error").is_some() => Kind::Response { id },
        _ => Kind::Invalid,
    }
}

/// Stable map key for a JSON-RPC id (`1` and `"1"` stay distinct).
pub fn id_key(id: &Value) -> String {
    id.to_string()
}

pub fn session_id(msg: &Value) -> Option<&str> {
    msg.get("params")?.get("sessionId")?.as_str()
}

pub fn response(id: Value, result: Value) -> Value {
    json!({ "jsonrpc": "2.0", "id": id, "result": result })
}

pub fn error_response(id: Value, code: i64, message: impl Into<String>) -> Value {
    json!({ "jsonrpc": "2.0", "id": id, "error": { "code": code, "message": message.into() } })
}

pub fn notification(method: &str, params: Value) -> Value {
    json!({ "jsonrpc": "2.0", "method": method, "params": params })
}
