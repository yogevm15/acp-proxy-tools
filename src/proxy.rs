//! Transport-independent ACP rewrite logic.
//!
//! The proxy keeps a per-session `bypass` flag. When it is set, every
//! `session/request_permission` from the agent is answered by the proxy with an
//! "allow" option and never reaches the client.
//!
//! The flag is exposed to the client in one of two ways:
//! * `Modes`: every session mode `X` gets a sibling `X - bypass permissions`,
//!   both in the legacy `modes` state and in the `category: "mode"` config option.
//! * `Checkbox`: a `type: "boolean"` session config option is injected next to
//!   the mode selector.

use std::collections::HashMap;

use serde_json::{Map, Value, json};
use tracing::{debug, info};

use crate::jsonrpc::{self, Kind};

pub const BYPASS_CONFIG_ID: &str = "bypass_permissions";
pub const BYPASS_MODE_SUFFIX: &str = "#bypass-permissions";
const SYNTHETIC_MODE_CONFIG_ID: &str = "mode";
const DEFAULT_MODE_ID: &str = "default";

#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum)]
pub enum UiStyle {
    /// Checkbox if the client advertises boolean config options, duplicated modes otherwise.
    Auto,
    /// Always duplicate every mode with a "bypass permissions" variant.
    Modes,
    /// Always inject a boolean "Bypass permissions" config option.
    Checkbox,
}

#[derive(Clone, Debug)]
pub struct Options {
    pub ui: UiStyle,
    /// Appended to the display name of duplicated modes.
    pub name_suffix: String,
}

impl Default for Options {
    fn default() -> Self {
        Self { ui: UiStyle::Auto, name_suffix: " - bypass permissions".into() }
    }
}

#[derive(Debug, PartialEq)]
pub enum Out {
    Client(Value),
    Agent(Value),
}

/// Upstream (agent-side) view of a session, plus the proxy-owned bypass flag.
#[derive(Default)]
struct Session {
    bypass: bool,
    modes: Option<Value>,
    config: Option<Vec<Value>>,
}

enum Pending {
    SessionSetup {
        session_id: Option<String>,
        fresh: bool,
    },
    SetMode {
        session_id: String,
        mode_id: String,
        bypass: Option<bool>,
    },
    SetConfig {
        session_id: String,
        mode_value: Option<String>,
        bypass: Option<bool>,
    },
    /// `session/set_config_option` on a synthesized mode selector, sent upstream as `session/set_mode`.
    SetConfigViaMode {
        session_id: String,
        mode_id: String,
        bypass: Option<bool>,
    },
    Close {
        session_id: String,
    },
}

pub struct Proxy {
    opts: Options,
    checkbox: bool,
    sessions: HashMap<String, Session>,
    pending: HashMap<String, Pending>,
}

impl Proxy {
    pub fn new(opts: Options) -> Self {
        let checkbox = opts.ui == UiStyle::Checkbox;
        Self { opts, checkbox, sessions: HashMap::new(), pending: HashMap::new() }
    }

    pub fn on_client(&mut self, msg: Value) -> Vec<Out> {
        let (id, method) = match jsonrpc::kind(&msg) {
            Kind::Request { id, method } => (id.clone(), method.to_owned()),
            _ => return vec![Out::Agent(msg)],
        };
        match method.as_str() {
            "initialize" => {
                let boolean = msg
                    .pointer("/params/clientCapabilities/session/configOptions/boolean")
                    .is_some_and(|v| !v.is_null());
                self.checkbox = match self.opts.ui {
                    UiStyle::Auto => boolean,
                    UiStyle::Modes => false,
                    UiStyle::Checkbox => true,
                };
                info!(client_boolean_config_options = boolean, checkbox = self.checkbox, "initialize");
            }
            "session/new" | "session/fork" => {
                self.track(&id, Pending::SessionSetup { session_id: None, fresh: true });
            }
            "session/load" | "session/resume" => {
                let session_id = jsonrpc::session_id(&msg).map(str::to_owned);
                self.track(&id, Pending::SessionSetup { session_id, fresh: false });
            }
            "session/set_mode" => return self.client_set_mode(id, msg),
            "session/set_config_option" => return self.client_set_config(id, msg),
            "session/close" | "session/delete" => {
                if let Some(sid) = jsonrpc::session_id(&msg) {
                    let session_id = sid.to_owned();
                    self.track(&id, Pending::Close { session_id });
                }
            }
            _ => {}
        }
        vec![Out::Agent(msg)]
    }

    pub fn on_agent(&mut self, msg: Value) -> Vec<Out> {
        match jsonrpc::kind(&msg) {
            Kind::Response { id } => {
                if let Some(pending) = self.pending.remove(&jsonrpc::id_key(id)) {
                    return self.agent_response(pending, msg);
                }
            }
            Kind::Request { id, method: "session/request_permission" } => {
                let id = id.clone();
                return self.agent_permission(id, msg);
            }
            Kind::Notification { method: "session/update" } => return self.agent_update(msg),
            _ => {}
        }
        vec![Out::Client(msg)]
    }

    fn track(&mut self, id: &Value, pending: Pending) {
        self.pending.insert(jsonrpc::id_key(id), pending);
    }

    fn client_set_mode(&mut self, id: Value, mut msg: Value) -> Vec<Out> {
        let Some(sid) = jsonrpc::session_id(&msg).map(str::to_owned) else {
            return vec![Out::Agent(msg)];
        };
        let Some(mode_id) = msg.pointer("/params/modeId").and_then(Value::as_str) else {
            return vec![Out::Agent(msg)];
        };
        let (base, bypass) = self.parse_mode_value(mode_id);

        match self.sessions.get_mut(&sid) {
            Some(s) if s.modes.is_none() => {
                // Synthesized modes: the agent knows nothing about them.
                if let Some(b) = bypass {
                    s.bypass = b;
                }
                let mut out = self.config_update(&sid);
                out.push(Out::Client(jsonrpc::response(id, json!({}))));
                out
            }
            _ => {
                msg["params"]["modeId"] = Value::String(base.clone());
                self.track(&id, Pending::SetMode { session_id: sid, mode_id: base, bypass });
                vec![Out::Agent(msg)]
            }
        }
    }

    fn client_set_config(&mut self, id: Value, mut msg: Value) -> Vec<Out> {
        let Some(sid) = jsonrpc::session_id(&msg).map(str::to_owned) else {
            return vec![Out::Agent(msg)];
        };
        let Some(config_id) = msg.pointer("/params/configId").and_then(Value::as_str).map(str::to_owned) else {
            return vec![Out::Agent(msg)];
        };

        if self.checkbox && config_id == BYPASS_CONFIG_ID {
            let on = truthy(msg.pointer("/params/value"));
            info!(session = %sid, bypass = on, "bypass permissions toggled");
            self.sessions.entry(sid.clone()).or_default().bypass = on;
            let options = self.render_config(&self.sessions[&sid]);
            return vec![Out::Client(jsonrpc::response(id, json!({ "configOptions": options })))];
        }

        let session = self.sessions.get(&sid);
        let upstream_mode_id = session
            .and_then(|s| s.config.as_deref())
            .and_then(|c| mode_option_index(c).map(|i| &c[i]))
            .and_then(|o| o.get("id")?.as_str())
            .map(str::to_owned);
        let has_upstream_modes = session.is_some_and(|s| s.modes.is_some());
        let value = msg.pointer("/params/value").and_then(Value::as_str).map(str::to_owned);

        if upstream_mode_id.as_deref() == Some(config_id.as_str()) {
            let (mode_value, bypass) = match value {
                Some(v) => {
                    let (base, bypass) = self.parse_mode_value(&v);
                    msg["params"]["value"] = Value::String(base.clone());
                    (Some(base), bypass)
                }
                None => (None, None),
            };
            self.track(&id, Pending::SetConfig { session_id: sid, mode_value, bypass });
            return vec![Out::Agent(msg)];
        }

        if config_id == SYNTHETIC_MODE_CONFIG_ID
            && upstream_mode_id.is_none()
            && session.is_some()
            && let Some(v) = value
        {
            let (base, bypass) = self.parse_mode_value(&v);
            if has_upstream_modes {
                let set_mode = json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "method": "session/set_mode",
                    "params": { "sessionId": sid, "modeId": base },
                });
                self.track(&id, Pending::SetConfigViaMode { session_id: sid, mode_id: base, bypass });
                return vec![Out::Agent(set_mode)];
            }
            let s = self.sessions.get_mut(&sid).expect("session checked above");
            if let Some(b) = bypass {
                s.bypass = b;
            }
            let options = self.render_config(&self.sessions[&sid]);
            let mut out = self.mode_update(&sid);
            out.push(Out::Client(jsonrpc::response(id, json!({ "configOptions": options }))));
            return out;
        }

        self.track(&id, Pending::SetConfig { session_id: sid, mode_value: None, bypass: None });
        vec![Out::Agent(msg)]
    }

    fn agent_response(&mut self, pending: Pending, mut msg: Value) -> Vec<Out> {
        if msg.get("result").is_none() {
            return vec![Out::Client(msg)];
        }
        match pending {
            Pending::SessionSetup { session_id, fresh } => {
                let sid = msg.pointer("/result/sessionId").and_then(Value::as_str).map(str::to_owned).or(session_id);
                let (Some(sid), Some(result)) = (sid, msg.get_mut("result").and_then(Value::as_object_mut)) else {
                    return vec![Out::Client(msg)];
                };
                let bypass = !fresh && self.sessions.get(&sid).is_some_and(|s| s.bypass);
                let session = Session {
                    bypass,
                    modes: result.get("modes").filter(|m| !m.is_null()).cloned(),
                    config: result.get("configOptions").and_then(Value::as_array).cloned(),
                };
                self.sessions.insert(sid.clone(), session);
                self.write_views(&sid, result);
                vec![Out::Client(msg)]
            }
            Pending::SetMode { session_id, mode_id, bypass } => {
                let Some(s) = self.sessions.get_mut(&session_id) else {
                    return vec![Out::Client(msg)];
                };
                if let Some(b) = bypass {
                    s.bypass = b;
                }
                set_current_mode(s, &mode_id);
                if let Some(i) = s.config.as_deref().and_then(mode_option_index) {
                    let config = s.config.as_mut().expect("index found");
                    if select_has_value(&config[i], &mode_id) {
                        config[i]["currentValue"] = Value::String(mode_id);
                    }
                }
                let mut out = self.config_update(&session_id);
                out.push(Out::Client(msg));
                out
            }
            Pending::SetConfig { session_id, mode_value, bypass } => {
                let Some(s) = self.sessions.get_mut(&session_id) else {
                    return vec![Out::Client(msg)];
                };
                if let Some(b) = bypass {
                    s.bypass = b;
                }
                if let Some(list) = msg.pointer("/result/configOptions").and_then(Value::as_array) {
                    s.config = Some(list.clone());
                }
                if let Some(mode) = &mode_value {
                    set_current_mode(s, mode);
                }
                let s = &self.sessions[&session_id];
                if let (Some(options), Some(result)) =
                    (self.render_config(s), msg.get_mut("result").and_then(Value::as_object_mut))
                {
                    result.insert("configOptions".into(), Value::Array(options));
                }
                let mut out = if mode_value.is_some() { self.mode_update(&session_id) } else { vec![] };
                out.push(Out::Client(msg));
                out
            }
            Pending::SetConfigViaMode { session_id, mode_id, bypass } => {
                let Some(s) = self.sessions.get_mut(&session_id) else {
                    return vec![Out::Client(msg)];
                };
                if let Some(b) = bypass {
                    s.bypass = b;
                }
                set_current_mode(s, &mode_id);
                let options = self.render_config(&self.sessions[&session_id]);
                msg["result"] = json!({ "configOptions": options });
                let mut out = self.mode_update(&session_id);
                out.push(Out::Client(msg));
                out
            }
            Pending::Close { session_id } => {
                self.sessions.remove(&session_id);
                vec![Out::Client(msg)]
            }
        }
    }

    fn agent_permission(&mut self, id: Value, msg: Value) -> Vec<Out> {
        let sid = jsonrpc::session_id(&msg);
        let bypass = sid.and_then(|sid| self.sessions.get(sid)).is_some_and(|s| s.bypass);
        if bypass && let Some(option_id) = pick_allow_option(msg.pointer("/params/options")) {
            let title = msg.pointer("/params/toolCall/title").and_then(Value::as_str).unwrap_or("");
            info!(session = sid.unwrap_or(""), option = %option_id, title, "auto-accepted permission request");
            let outcome = json!({ "outcome": { "outcome": "selected", "optionId": option_id } });
            return vec![Out::Agent(jsonrpc::response(id, outcome))];
        }
        vec![Out::Client(msg)]
    }

    fn agent_update(&mut self, mut msg: Value) -> Vec<Out> {
        let Some(sid) = jsonrpc::session_id(&msg).map(str::to_owned) else {
            return vec![Out::Client(msg)];
        };
        let Some(s) = self.sessions.get_mut(&sid) else {
            return vec![Out::Client(msg)];
        };
        let Some(update) = msg.pointer_mut("/params/update") else {
            return vec![Out::Client(msg)];
        };
        match update.get("sessionUpdate").and_then(Value::as_str) {
            Some("current_mode_update") => {
                let Some(mode) = update.get("currentModeId").and_then(Value::as_str).map(str::to_owned) else {
                    return vec![Out::Client(msg)];
                };
                set_current_mode(s, &mode);
                if !self.checkbox {
                    update["currentModeId"] = Value::String(virtual_id(&mode, s.bypass));
                }
                let synthesized_select = s.config.as_deref().and_then(mode_option_index).is_none();
                let mut out = vec![Out::Client(msg)];
                if synthesized_select {
                    out.extend(self.config_update(&sid));
                }
                out
            }
            Some("config_option_update") => {
                if let Some(list) = update.get("configOptions").and_then(Value::as_array) {
                    s.config = Some(list.clone());
                    if let Some(rendered) = self.render_config(&self.sessions[&sid]) {
                        msg["params"]["update"]["configOptions"] = Value::Array(rendered);
                    }
                }
                vec![Out::Client(msg)]
            }
            _ => vec![Out::Client(msg)],
        }
    }

    fn write_views(&self, sid: &str, result: &mut Map<String, Value>) {
        let s = &self.sessions[sid];
        if let Some(modes) = self.render_modes(s) {
            result.insert("modes".into(), modes);
        }
        if let Some(options) = self.render_config(s) {
            result.insert("configOptions".into(), Value::Array(options));
        }
    }

    fn config_update(&self, sid: &str) -> Vec<Out> {
        let Some(options) = self.sessions.get(sid).and_then(|s| self.render_config(s)) else {
            return vec![];
        };
        let update = json!({ "sessionUpdate": "config_option_update", "configOptions": options });
        vec![Out::Client(session_update(sid, update))]
    }

    fn mode_update(&self, sid: &str) -> Vec<Out> {
        let Some(modes) = self.sessions.get(sid).and_then(|s| self.render_modes(s)) else {
            return vec![];
        };
        let update = json!({ "sessionUpdate": "current_mode_update", "currentModeId": modes["currentModeId"] });
        vec![Out::Client(session_update(sid, update))]
    }

    /// Returns the upstream mode id and the bypass flag it encodes (`None` = unchanged).
    fn parse_mode_value(&self, value: &str) -> (String, Option<bool>) {
        if self.checkbox {
            return (value.to_owned(), None);
        }
        match value.strip_suffix(BYPASS_MODE_SUFFIX) {
            Some(base) => (base.to_owned(), Some(true)),
            None => (value.to_owned(), Some(false)),
        }
    }

    fn bypass_variant(&self, item: &Value, id_key: &str) -> Value {
        let mut v = item.clone();
        let id = item.get(id_key).and_then(Value::as_str).unwrap_or_default();
        let name = item.get("name").and_then(Value::as_str).unwrap_or(id);
        v[id_key] = Value::String(virtual_id(id, true));
        v["name"] = Value::String(format!("{name}{}", self.opts.name_suffix));
        v["description"] = Value::String(match item.get("description").and_then(Value::as_str) {
            Some(d) if !d.is_empty() => format!("{d} (permission requests are accepted automatically)"),
            _ => "Permission requests are accepted automatically".into(),
        });
        v
    }

    fn duplicate_modes(&self, modes: &Value, bypass: bool) -> Value {
        let mut m = modes.clone();
        if let Some(list) = modes.get("availableModes").and_then(Value::as_array) {
            m["availableModes"] = list.iter().flat_map(|x| [x.clone(), self.bypass_variant(x, "id")]).collect();
        }
        if let Some(current) = modes.get("currentModeId").and_then(Value::as_str) {
            m["currentModeId"] = Value::String(virtual_id(current, bypass));
        }
        m
    }

    fn duplicate_select(&self, option: &Value, bypass: bool) -> Value {
        let mut o = option.clone();
        let duplicate = |values: &[Value]| -> Value {
            values.iter().flat_map(|v| [v.clone(), self.bypass_variant(v, "value")]).collect()
        };
        if let Some(values) = option.get("options").and_then(Value::as_array) {
            let grouped = values.iter().any(|v| v.get("group").is_some());
            o["options"] = if grouped {
                values
                    .iter()
                    .map(|group| {
                        let mut g = group.clone();
                        if let Some(inner) = group.get("options").and_then(Value::as_array) {
                            g["options"] = duplicate(inner);
                        }
                        g
                    })
                    .collect()
            } else {
                duplicate(values)
            };
        }
        if let Some(current) = option.get("currentValue").and_then(Value::as_str) {
            o["currentValue"] = Value::String(virtual_id(current, bypass));
        }
        o
    }

    fn render_modes(&self, s: &Session) -> Option<Value> {
        if self.checkbox {
            return s.modes.clone();
        }
        if let Some(m) = &s.modes {
            return Some(self.duplicate_modes(m, s.bypass));
        }
        if s.config.as_deref().and_then(mode_option_index).is_some() {
            return None;
        }
        Some(self.duplicate_modes(&synthetic_modes(), s.bypass))
    }

    fn render_config(&self, s: &Session) -> Option<Vec<Value>> {
        if self.checkbox {
            let mut list = s.config.clone().unwrap_or_default();
            let index = match mode_option_index(&list) {
                Some(i) => Some(i),
                None => s.modes.as_ref().map(|m| {
                    list.insert(0, mode_select_from(m));
                    0
                }),
            };
            list.insert(index.map_or(0, |i| i + 1), bypass_option(s.bypass));
            return Some(list);
        }
        let mut list = s.config.clone()?;
        let index = match mode_option_index(&list) {
            Some(i) => i,
            None => {
                let modes = s.modes.clone().unwrap_or_else(synthetic_modes);
                list.insert(0, mode_select_from(&modes));
                0
            }
        };
        list[index] = self.duplicate_select(&list[index], s.bypass);
        Some(list)
    }
}

fn virtual_id(base: &str, bypass: bool) -> String {
    if bypass { format!("{base}{BYPASS_MODE_SUFFIX}") } else { base.to_owned() }
}

fn session_update(sid: &str, update: Value) -> Value {
    jsonrpc::notification("session/update", json!({ "sessionId": sid, "update": update }))
}

fn set_current_mode(s: &mut Session, mode_id: &str) {
    if let Some(modes) = s.modes.as_mut().and_then(Value::as_object_mut) {
        modes.insert("currentModeId".into(), Value::String(mode_id.to_owned()));
    }
}

fn mode_option_index(config: &[Value]) -> Option<usize> {
    let is_select = |o: &Value| o.get("type").and_then(Value::as_str) == Some("select");
    let field = |o: &Value, k: &str| o.get(k).and_then(Value::as_str).map(str::to_owned);
    config
        .iter()
        .position(|o| is_select(o) && field(o, "category").as_deref() == Some("mode"))
        .or_else(|| config.iter().position(|o| is_select(o) && field(o, "id").as_deref() == Some("mode")))
}

fn select_has_value(option: &Value, value: &str) -> bool {
    let Some(values) = option.get("options").and_then(Value::as_array) else {
        return false;
    };
    values.iter().any(|v| match v.get("options").and_then(Value::as_array) {
        Some(grouped) => grouped.iter().any(|g| g.get("value").and_then(Value::as_str) == Some(value)),
        None => v.get("value").and_then(Value::as_str) == Some(value),
    })
}

fn synthetic_modes() -> Value {
    json!({
        "currentModeId": DEFAULT_MODE_ID,
        "availableModes": [{ "id": DEFAULT_MODE_ID, "name": "Default" }],
    })
}

fn mode_select_from(modes: &Value) -> Value {
    let values: Vec<Value> = modes
        .get("availableModes")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .map(|m| {
            let mut v = json!({ "value": m["id"], "name": m.get("name").unwrap_or(&m["id"]) });
            if let Some(d) = m.get("description").filter(|d| !d.is_null()) {
                v["description"] = d.clone();
            }
            v
        })
        .collect();
    json!({
        "id": SYNTHETIC_MODE_CONFIG_ID,
        "name": "Mode",
        "category": "mode",
        "type": "select",
        "currentValue": modes.get("currentModeId").cloned().unwrap_or(Value::Null),
        "options": values,
    })
}

fn bypass_option(on: bool) -> Value {
    json!({
        "id": BYPASS_CONFIG_ID,
        "name": "Bypass permissions",
        "description": "Automatically accept every permission request from the agent",
        "type": "boolean",
        "currentValue": on,
    })
}

fn truthy(value: Option<&Value>) -> bool {
    match value {
        Some(Value::Bool(b)) => *b,
        Some(Value::String(s)) => matches!(s.as_str(), "true" | "on" | "1" | "yes"),
        _ => false,
    }
}

/// Prefers one-shot approval so that bypassing never persists "always allow" rules in the agent.
fn pick_allow_option(options: Option<&Value>) -> Option<String> {
    let options = options?.as_array()?;
    let kind = |o: &Value| o.get("kind").and_then(Value::as_str).unwrap_or_default().to_owned();
    let found = ["allow_once", "allow_always"]
        .iter()
        .find_map(|k| options.iter().find(|o| kind(o) == *k))
        .or_else(|| options.iter().find(|o| kind(o).starts_with("allow")));
    let id = found?.get("optionId")?.as_str()?.to_owned();
    debug!(option = %id, "selected allow option");
    Some(id)
}

#[cfg(test)]
mod tests;
