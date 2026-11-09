use t3_code_ls_core::codex_generation::{
    Model, Request, disabled_mcp_config, enabled_mcp_names, exec_launch_args, select,
    tokenize_launch_args,
};
use t3_code_ls_core::codex_generation_wire::parse;
use t3_code_ls_core::json::Value;

fn strings(values: &[&str]) -> Vec<String> {
    values.iter().map(|value| (*value).to_owned()).collect()
}

fn request() -> Request {
    Request {
        binary: "codex".to_owned(),
        cwd: "/tmp".to_owned(),
        prompt: "prompt".to_owned(),
        output_schema: Value::Object(vec![]),
        launch_args: String::new(),
        models: Vec::new(),
        custom_models: Value::Array(vec![]),
        model: "requested".to_owned(),
        options: vec![],
        managed: false,
        image_paths: vec![],
        timeout_ms: 180_000,
    }
}

#[cfg_attr(test, test)]
fn preserves_cli_quote_escape_and_filter_semantics() {
    assert_eq!(
        tokenize_launch_args("a '' \"b\\\"c\" 'd\\e' f\\ g"),
        strings(&["a", "", "b\"c", "d\\e", "f g"])
    );
    assert_eq!(
        exec_launch_args(
            "--listen http://x --config 'a=two words' --strict-config --enable --bad --disable=x -c=y"
        ),
        strings(&[
            "--config",
            "a=two words",
            "--strict-config",
            "--disable=x",
            "-c=y"
        ])
    );
    assert_eq!(
        tokenize_launch_args("a\u{feff}b\u{0085}c"),
        strings(&["a", "b\u{0085}c"])
    );
}

#[cfg_attr(test, test)]
fn fallback_drops_options_but_family_and_custom_matches_keep_them() {
    let mut input = request();
    input.options = vec![
        (
            "reasoningEffort".to_owned(),
            Value::String("high".to_owned()),
        ),
        ("fastMode".to_owned(), Value::Bool(true)),
    ];
    input.models.push(Model {
        slug: "gpt-other".to_owned(),
        custom: false,
        default: true,
        legacy: false,
    });
    assert_eq!(select(&input, None).reasoning, "low");
    assert_eq!(select(&input, None).service_tier, None);
    input.model = "openai.gpt-other".to_owned();
    assert_eq!(select(&input, None).reasoning, "high");
    assert_eq!(select(&input, None).service_tier, Some("fast".to_owned()));
    input.model = "custom".to_owned();
    input.custom_models = Value::Array(vec![Value::String(" custom ".to_owned())]);
    assert_eq!(select(&input, None).model, "custom");
    input.managed = true;
    assert_eq!(select(&input, None).service_tier, None);
}

#[cfg_attr(test, test)]
fn first_option_and_environment_override_match_upstream() {
    let mut input = request();
    input.options = vec![
        ("reasoningEffort".to_owned(), Value::Bool(true)),
        (
            "reasoningEffort".to_owned(),
            Value::String("high".to_owned()),
        ),
    ];
    input.launch_args = "--config saved=true".to_owned();
    let result = select(&input, Some("\u{feff} --config env=true \u{feff}"));
    assert_eq!(result.reasoning, "low");
    assert_eq!(result.launch_args, strings(&["--config", "env=true"]));
    assert_eq!(
        select(&input, Some("  ")).launch_args,
        strings(&["--config", "saved=true"])
    );
}

#[cfg_attr(test, test)]
fn mcp_configuration_preserves_names_and_disabled_rows() {
    let names = enabled_mcp_names(br#"[{"name":"a.b","enabled":true},{"name":"off","enabled":false},{"name":"quote\"name","enabled":true}]"#).unwrap().unwrap();
    assert_eq!(
        disabled_mcp_config(&names),
        Some("mcp_servers={\"a.b\"={enabled=false},\"quote\\\"name\"={enabled=false}}".to_owned())
    );
    assert_eq!(enabled_mcp_names(br#"[{"name":"x"}]"#).unwrap(), None);
    assert!(
        enabled_mcp_names(br#"[{"name":"x","enabled":true,"extra":"\ud800"}]"#).is_err(),
        "unsupported Unicode evidence fails closed"
    );
    assert_eq!(
        enabled_mcp_names("\u{feff}[{\"name\":\"bom\",\"enabled\":true}]".as_bytes()).unwrap(),
        Some(vec!["bom".to_owned()])
    );
}

fn valid() -> String {
    "{\"version\":1,\"operation\":\"run\",\"binary\":\"codex\",\"cwd\":\"/tmp\",\"prompt\":\"p\",\"outputSchema\":{},\"launchArgs\":\"\",\"models\":[],\"customModels\":[],\"modelSelection\":{\"model\":\"m\",\"options\":[]},\"managed\":false,\"imagePaths\":[],\"timeoutMs\":180000}\n".to_owned()
}

#[cfg_attr(test, test)]
fn requires_one_exact_request_and_bounded_data() {
    assert!(parse(&valid()).is_ok());
    assert!(parse(valid().trim_end()).is_err());
    assert!(parse(&(valid() + "\n")).is_err());
    assert!(
        parse(&valid().replace("\"version\":1", "\"version\":1,\"version\":1")).is_err(),
        "duplicate envelope fields are rejected"
    );
    assert!(
        parse(&valid().replace("\"outputSchema\":{}", "\"outputSchema\":{\"a\":1,\"a\":2}"))
            .is_err(),
        "duplicate schema fields are rejected"
    );
    assert!(parse(&valid().replace("/tmp", "relative")).is_err());
    assert!(parse(&valid().replace("180000", "0")).is_err());
}

fn main() {
    let mut passed = 0;
    preserves_cli_quote_escape_and_filter_semantics();
    passed += 1;
    fallback_drops_options_but_family_and_custom_matches_keep_them();
    passed += 1;
    first_option_and_environment_override_match_upstream();
    passed += 1;
    mcp_configuration_preserves_names_and_disabled_rows();
    passed += 1;
    requires_one_exact_request_and_bounded_data();
    passed += 1;
    println!(
        "{{\"schema\":1,\"kind\":\"summary\",\"build\":\"ok\",\"total\":5,\"passed\":{},\"failed\":0,\"timed_out\":0,\"ignored\":0,\"filtered_out\":0}}",
        passed
    );
}
