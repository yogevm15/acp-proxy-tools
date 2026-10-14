use serde_json::{Value, json};

use super::*;

fn modes() -> Value {
    json!({
        "currentModeId": "agent",
        "availableModes": [
            { "id": "agent", "name": "Agent" },
            { "id": "plan", "name": "Plan", "description": "Plan first" },
            { "id": "debug", "name": "Debug" },
            { "id": "ask", "name": "Ask" },
        ],
    })
}

fn mode_config() -> Value {
    json!({
        "id": "mode",
        "name": "Mode",
        "category": "mode",
        "type": "select",
        "currentValue": "agent",
        "options": [
            { "value": "agent", "name": "Agent" },
            { "value": "plan", "name": "Plan" },
        ],
    })
}

fn initialize(boolean: bool) -> Value {
    let caps = if boolean { json!({ "session": { "configOptions": { "boolean": {} } } }) } else { json!({}) };
    json!({ "jsonrpc": "2.0", "id": 0, "method": "initialize", "params": { "protocolVersion": 1, "clientCapabilities": caps } })
}

fn permission_request(id: u64) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": "session/request_permission",
        "params": {
            "sessionId": "s1",
            "toolCall": { "toolCallId": "call_1", "title": "rm -rf build" },
            "options": [
                { "optionId": "always", "name": "Always", "kind": "allow_always" },
                { "optionId": "once", "name": "Once", "kind": "allow_once" },
                { "optionId": "no", "name": "No", "kind": "reject_once" },
            ],
        },
    })
}

/// Initializes the proxy and opens session `s1` with the given upstream session state.
fn setup(ui: UiStyle, client_boolean: bool, result: Value) -> (Proxy, Value) {
    let mut p = Proxy::new(Options { ui, ..Options::default() });
    assert_eq!(p.on_client(initialize(client_boolean)).len(), 1);
    p.on_client(
        json!({ "jsonrpc": "2.0", "id": 1, "method": "session/new", "params": { "cwd": "/", "mcpServers": [] } }),
    );
    let mut result = result;
    result["sessionId"] = json!("s1");
    let out = p.on_agent(json!({ "jsonrpc": "2.0", "id": 1, "result": result }));
    let Ok([Out::Client(resp)]) = <[Out; 1]>::try_from(out) else { panic!("expected one client message") };
    (p, resp["result"].clone())
}

fn ids(list: &Value, key: &str) -> Vec<String> {
    list.as_array().unwrap().iter().map(|v| v[key].as_str().unwrap().to_owned()).collect()
}

fn single_client(out: Vec<Out>) -> Value {
    let Ok([Out::Client(v)]) = <[Out; 1]>::try_from(out) else { panic!("expected one client message") };
    v
}

fn single_agent(out: Vec<Out>) -> Value {
    let Ok([Out::Agent(v)]) = <[Out; 1]>::try_from(out) else { panic!("expected one agent message") };
    v
}

#[test]
fn duplicates_modes_interleaved() {
    let (_, result) = setup(UiStyle::Auto, false, json!({ "modes": modes() }));
    assert_eq!(
        ids(&result["modes"]["availableModes"], "id"),
        [
            "agent",
            "agent#bypass-permissions",
            "plan",
            "plan#bypass-permissions",
            "debug",
            "debug#bypass-permissions",
            "ask",
            "ask#bypass-permissions",
        ]
    );
    assert_eq!(result["modes"]["availableModes"][1]["name"], "Agent - bypass permissions");
    assert_eq!(result["modes"]["currentModeId"], "agent");
    assert!(result.get("configOptions").is_none());
}

#[test]
fn duplicates_mode_config_option() {
    let (_, result) = setup(UiStyle::Modes, true, json!({ "modes": modes(), "configOptions": [mode_config()] }));
    let options = &result["configOptions"];
    assert_eq!(options.as_array().unwrap().len(), 1);
    assert_eq!(
        ids(&options[0]["options"], "value"),
        ["agent", "agent#bypass-permissions", "plan", "plan#bypass-permissions"]
    );
}

#[test]
fn bypass_mode_auto_accepts_permissions() {
    let (mut p, _) = setup(UiStyle::Auto, false, json!({ "modes": modes() }));

    // Without bypass the request reaches the client.
    assert_eq!(single_client(p.on_agent(permission_request(7)))["id"], 7);

    let set = json!({ "jsonrpc": "2.0", "id": 2, "method": "session/set_mode", "params": { "sessionId": "s1", "modeId": "plan#bypass-permissions" } });
    let forwarded = single_agent(p.on_client(set));
    assert_eq!(forwarded["params"]["modeId"], "plan");
    assert_eq!(single_client(p.on_agent(json!({ "jsonrpc": "2.0", "id": 2, "result": {} })))["id"], 2);

    let answer = single_agent(p.on_agent(permission_request(8)));
    assert_eq!(
        answer,
        json!({ "jsonrpc": "2.0", "id": 8, "result": { "outcome": { "outcome": "selected", "optionId": "once" } } })
    );

    // Agent-driven mode switch keeps the bypass flag.
    let update = json!({ "jsonrpc": "2.0", "method": "session/update", "params": { "sessionId": "s1", "update": { "sessionUpdate": "current_mode_update", "currentModeId": "agent" } } });
    assert_eq!(single_client(p.on_agent(update))["params"]["update"]["currentModeId"], "agent#bypass-permissions");

    // Back to a normal mode disables bypass.
    let set = json!({ "jsonrpc": "2.0", "id": 3, "method": "session/set_mode", "params": { "sessionId": "s1", "modeId": "ask" } });
    single_agent(p.on_client(set));
    p.on_agent(json!({ "jsonrpc": "2.0", "id": 3, "result": {} }));
    assert_eq!(single_client(p.on_agent(permission_request(9)))["id"], 9);
}

#[test]
fn set_mode_emits_config_sync() {
    let (mut p, _) = setup(UiStyle::Modes, false, json!({ "modes": modes(), "configOptions": [mode_config()] }));
    let set = json!({ "jsonrpc": "2.0", "id": 2, "method": "session/set_mode", "params": { "sessionId": "s1", "modeId": "plan#bypass-permissions" } });
    p.on_client(set);
    let out = p.on_agent(json!({ "jsonrpc": "2.0", "id": 2, "result": {} }));
    assert_eq!(out.len(), 2);
    let Out::Client(update) = &out[0] else { panic!() };
    assert_eq!(update["params"]["update"]["configOptions"][0]["currentValue"], "plan#bypass-permissions");
}

#[test]
fn set_config_option_mode_variant() {
    let (mut p, _) = setup(UiStyle::Modes, false, json!({ "configOptions": [mode_config()] }));
    let set = json!({ "jsonrpc": "2.0", "id": 2, "method": "session/set_config_option", "params": { "sessionId": "s1", "configId": "mode", "value": "agent#bypass-permissions" } });
    assert_eq!(single_agent(p.on_client(set))["params"]["value"], "agent");

    let mut upstream = mode_config();
    upstream["currentValue"] = json!("agent");
    let out = p.on_agent(json!({ "jsonrpc": "2.0", "id": 2, "result": { "configOptions": [upstream] } }));
    let Some(Out::Client(resp)) = out.last() else { panic!() };
    assert_eq!(resp["result"]["configOptions"][0]["currentValue"], "agent#bypass-permissions");
    single_agent(p.on_agent(permission_request(5)));
}

#[test]
fn checkbox_when_client_supports_boolean() {
    let (mut p, result) = setup(UiStyle::Auto, true, json!({ "modes": modes(), "configOptions": [mode_config()] }));
    assert_eq!(result["modes"], modes(), "legacy modes stay untouched");
    let options = result["configOptions"].as_array().unwrap();
    assert_eq!(ids(&result["configOptions"], "id"), ["mode", BYPASS_CONFIG_ID]);
    assert_eq!(options[1]["type"], "boolean");
    assert_eq!(options[1]["currentValue"], false);
    assert_eq!(options[0]["options"].as_array().unwrap().len(), 2);

    let toggle = json!({ "jsonrpc": "2.0", "id": 2, "method": "session/set_config_option", "params": { "sessionId": "s1", "configId": BYPASS_CONFIG_ID, "type": "boolean", "value": true } });
    let resp = single_client(p.on_client(toggle));
    assert_eq!(resp["result"]["configOptions"][1]["currentValue"], true);

    single_agent(p.on_agent(permission_request(5)));

    // Upstream config updates keep the injected checkbox.
    let update = json!({ "jsonrpc": "2.0", "method": "session/update", "params": { "sessionId": "s1", "update": { "sessionUpdate": "config_option_update", "configOptions": [mode_config()] } } });
    let forwarded = single_client(p.on_agent(update));
    assert_eq!(forwarded["params"]["update"]["configOptions"][1]["currentValue"], true);
}

#[test]
fn checkbox_synthesizes_mode_select_from_legacy_modes() {
    let (mut p, result) = setup(UiStyle::Checkbox, false, json!({ "modes": modes() }));
    assert_eq!(ids(&result["configOptions"], "id"), ["mode", BYPASS_CONFIG_ID]);
    assert_eq!(ids(&result["configOptions"][0]["options"], "value"), ["agent", "plan", "debug", "ask"]);

    let set = json!({ "jsonrpc": "2.0", "id": 2, "method": "session/set_config_option", "params": { "sessionId": "s1", "configId": "mode", "value": "plan" } });
    let translated = single_agent(p.on_client(set));
    assert_eq!(translated["method"], "session/set_mode");
    assert_eq!(translated["params"], json!({ "sessionId": "s1", "modeId": "plan" }));

    let out = p.on_agent(json!({ "jsonrpc": "2.0", "id": 2, "result": {} }));
    let Some(Out::Client(resp)) = out.last() else { panic!() };
    assert_eq!(resp["result"]["configOptions"][0]["currentValue"], "plan");
}

#[test]
fn synthesizes_default_mode_when_agent_has_none() {
    let (mut p, result) = setup(UiStyle::Modes, false, json!({}));
    assert_eq!(ids(&result["modes"]["availableModes"], "id"), ["default", "default#bypass-permissions"]);

    let set = json!({ "jsonrpc": "2.0", "id": 2, "method": "session/set_mode", "params": { "sessionId": "s1", "modeId": "default#bypass-permissions" } });
    let resp = single_client(p.on_client(set));
    assert_eq!(resp, json!({ "jsonrpc": "2.0", "id": 2, "result": {} }));
    single_agent(p.on_agent(permission_request(5)));
}

#[test]
fn grouped_select_values_are_duplicated_per_group() {
    let mut config = mode_config();
    config["options"] = json!([{ "group": "g", "name": "G", "options": [{ "value": "agent", "name": "Agent" }] }]);
    let (_, result) = setup(UiStyle::Modes, false, json!({ "configOptions": [config] }));
    assert_eq!(
        ids(&result["configOptions"][0]["options"][0]["options"], "value"),
        ["agent", "agent#bypass-permissions"]
    );
}

#[test]
fn errors_do_not_change_state() {
    let (mut p, _) = setup(UiStyle::Modes, false, json!({ "modes": modes() }));
    let set = json!({ "jsonrpc": "2.0", "id": 2, "method": "session/set_mode", "params": { "sessionId": "s1", "modeId": "agent#bypass-permissions" } });
    p.on_client(set);
    p.on_agent(json!({ "jsonrpc": "2.0", "id": 2, "error": { "code": -32603, "message": "nope" } }));
    assert_eq!(single_client(p.on_agent(permission_request(5)))["id"], 5);
}

#[test]
fn unrelated_messages_pass_through() {
    let mut p = Proxy::new(Options::default());
    let prompt =
        json!({ "jsonrpc": "2.0", "id": 9, "method": "session/prompt", "params": { "sessionId": "x", "prompt": [] } });
    assert_eq!(single_agent(p.on_client(prompt.clone())), prompt);
    let chunk = json!({ "jsonrpc": "2.0", "method": "session/update", "params": { "sessionId": "x", "update": { "sessionUpdate": "agent_message_chunk" } } });
    assert_eq!(single_client(p.on_agent(chunk.clone())), chunk);
}
