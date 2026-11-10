//! Behavioral ports from apps/server/src/orchestration-v2/WireProjection.test.ts
//! at pingdotgg/t3code 521ac7c218b3faa74281fb72fc36ce74a0951fb3 (MIT).
//! Copyright (c) 2026 T3 Tools Inc.
//!
//! The ThreadShell test and asynchronous coalescer are not ported here. The
//! live-deduper test starts at the upstream post-coalescing boundary instead.

use std::sync::Arc;

use serde_json::{Map, Value, json};
use t3_code_ls_orchestration::bounded_snapshot::{
    bounded_snapshot_projection, omit_local_visible_turn_items,
};
use t3_code_ls_orchestration::deduped_bodies::{BodySource, dedupe_snapshot_bodies};
use t3_code_ls_orchestration::tool_output::{MAX_TOOL_OUTPUT_IMAGES, tool_output_images};
use t3_code_ls_orchestration::types::{Item, ProjectedTurnItem, Projection, ThreadProjection};
use t3_code_ls_orchestration::wire_projection::{
    LiveBodyDeduper, ProjectionError, WireProjector, bound_file_change_for_storage,
    project_context_handoff_for_wire,
};

const AT: &str = "2026-08-13T00:00:01.000Z";
const MARKER: &str = "\n… output truncated for transport";

fn tool(fields: Value) -> Item {
    let mut value = json!({
        "id":"tool-1","type":"dynamic_tool","threadId":"thread-1",
        "runId":null,"nodeId":null,"providerThreadId":null,"providerTurnId":null,
        "nativeItemRef":null,"parentItemId":null,"ordinal":1,"status":"completed",
        "title":"MCP tool","toolName":"mcp__github__fetch_pr","input":{"pr":42},
        "startedAt":"2026-08-13T00:00:00.000Z","completedAt":AT,"updatedAt":AT
    });
    value.as_object_mut().unwrap().extend(fields.as_object().unwrap().clone());
    Arc::new(value)
}

fn event(kind: &str, payload: Value) -> Item {
    Arc::new(json!({
        "id":"event-1","type":kind,"threadId":"thread-1","occurredAt":AT,"payload":payload
    }))
}

fn stream(sequence: u64, kind: &str, payload: Value) -> Item {
    Arc::new(json!({
        "kind":"event","sequence":sequence,
        "event":{"id":format!("event-{sequence}"),"threadId":"thread-1","occurredAt":AT,"type":kind,"payload":payload}
    }))
}

fn message(sequence: u64, id: &str, text: &str) -> Item {
    stream(sequence,"message.updated",json!({
        "id":id,"threadId":"thread-1","runId":null,"nodeId":null,
        "role":"assistant","text":text,"attachments":[],"streaming":true,
        "createdBy":"agent","creationSource":"provider","createdAt":AT,"updatedAt":AT
    }))
}

fn reply(sequence: u64, message_id: &str, text: &str) -> Item {
    stream(sequence,"turn-item.updated",(*tool(json!({
        "type":"assistant_message","status":"running","messageId":message_id,"text":text,"streaming":true
    }))).clone())
}

fn payload(item: &Item) -> &Value {
    &item["event"]["payload"]
}

fn files(count: usize) -> Value {
    json!((0..count).map(|index|json!({
        "path":format!("vendor/file-{index}.ts"),"kind":"modified","additions":2,"deletions":1
    })).collect::<Vec<_>>())
}

fn projection(items: Vec<Item>) -> Projection {
    let visible_turn_items = items.iter().enumerate().map(|(position,item)| {
        let mut extra = Map::new();
        extra.insert("position".into(),json!(position));
        extra.insert("sourceItemId".into(),item["id"].clone());
        Arc::new(ProjectedTurnItem { source_thread_id:"thread-1".into(),visibility:"local".into(),item:item.clone(),extra })
    }).collect();
    Arc::new(ThreadProjection {
        thread:json!({"id":"thread-1"}),messages:vec![],checkpoints:vec![],subagents:vec![],
        context_handoffs:vec![],turn_items:items,visible_turn_items,extra:Map::new(),
    })
}

fn wire(value: &Projection) -> Value {
    serde_json::to_value(value.as_ref()).unwrap()
}

#[test]
fn upstream_handoff_activity_and_events_do_not_expose_copied_transcripts() {
    let item = tool(json!({
        "type":"handoff","contextHandoffId":"handoff:wire","fromProviderThreadIds":[],
        "toProviderThreadId":"target","fromProviderInstanceIds":[],"toProviderInstanceId":"codex",
        "strategy":"full_thread_summary","summary":"PRIVATE_HANDOFF_TRANSCRIPT","future":{"keep":true}
    }));
    let original = (*item).clone();
    let mut projector = WireProjector::default();
    for projected in [projector.project_turn_item_for_wire(&item).unwrap(),projector.project_turn_item_for_detail(&item).unwrap()] {
        assert!(projected.get("summary").is_none());
        assert_eq!(projected["contextHandoffId"],json!("handoff:wire"));
        assert_eq!(projected["future"],json!({"keep":true}));
    }
    let event = event("turn-item.updated",original.clone());
    let projected = projector.project_domain_event_for_wire(&event).unwrap();
    assert!(!serde_json::to_string(projected.as_ref()).unwrap().contains("PRIVATE_HANDOFF_TRANSCRIPT"));
    assert_eq!(*item,original);
    assert_eq!(event["payload"]["summary"],json!("PRIVATE_HANDOFF_TRANSCRIPT"));
}

#[test]
fn redaction_keeps_javascript_spread_order_and_appends_reintroduced_fields() {
    let mut projector = WireProjector::default();
    for (source, expected) in [
        (r#"{"type":"handoff","summary":"private","first":1,"last":2}"#,
         r#"{"type":"handoff","first":1,"last":2}"#),
        (r#"{"type":"command_execution","output":"private","input":"echo","last":2,"exitCode":0}"#,
         r#"{"type":"command_execution","input":"echo","last":2,"exitCode":0,"outputOmitted":true}"#),
        (r#"{"type":"file_change","diffStr":"failed change","oldStr":"old","status":"failed","last":2,"newStr":"new"}"#,
         r#"{"type":"file_change","status":"failed","last":2,"diffStr":"failed change"}"#),
        (r#"{"type":"dynamic_tool","output":{"threadId":"child","text":"private"},"input":{"x":1},"last":2}"#,
         r#"{"type":"dynamic_tool","input":{"x":1},"last":2,"output":{"threadId":"child"},"outputOmitted":true}"#),
        (r#"{"type":"subagent","prompt":"task","last":2,"result":null,"progress":"working"}"#,
         r#"{"type":"subagent","prompt":"task","last":2,"result":null,"progress":"working"}"#),
    ] {
        let source = Arc::new(serde_json::from_str(source).unwrap());
        let projected = projector.project_turn_item_for_wire(&source).unwrap();
        assert_eq!(serde_json::to_string(projected.as_ref()).unwrap(),expected);
    }
    let handoff: Value = serde_json::from_str(r#"{"id":"h","history":["private"],"summaryText":"private","delivery":{},"last":2}"#).unwrap();
    assert_eq!(serde_json::to_string(&project_context_handoff_for_wire(&handoff).unwrap()).unwrap(),
        r#"{"id":"h","summaryText":"","last":2}"#);
}

#[test]
fn large_discarded_bodies_are_omitted_through_nested_stored_event_projection() {
    let mut projector = WireProjector::default();
    for body in [
        json!({"type":"handoff","summary":"x".repeat(2_000_000)}),
        json!({"type":"command_execution","input":"echo","output":"x".repeat(2_000_000),"exitCode":0}),
        json!({"type":"file_change","status":"completed","diffStr":"x".repeat(2_000_000),"oldStr":"y".repeat(2_000_000),"newStr":"z".repeat(2_000_000)}),
        json!({"type":"dynamic_tool","input":{},"output":{"threadId":"child","text":"x".repeat(2_000_000)}}),
    ] {
        let stored = Arc::new(json!({"sequence":1,"event":{"type":"turn-item.updated","payload":body}}));
        let projected = projector.project_stored_event_for_wire(&stored).unwrap();
        assert!(serde_json::to_string(projected.as_ref()).unwrap().len()<300);
        assert!(Arc::ptr_eq(&projected,&projector.project_stored_event_for_wire(&stored).unwrap()));
        assert!(serde_json::to_string(stored.as_ref()).unwrap().len()>2_000_000);
    }
}

#[test]
fn upstream_image_metadata_survives_json_while_raw_output_is_redacted() {
    let item = tool(json!({
        "toolName":"Read","input":{"file_path":"/workspace/reference.png"},
        "viewedImagePath":"/workspace/reference.png","output":{"data":"private-image-data"},
        "future":{"keep":[1,2]}
    }));
    let projected = WireProjector::default().project_turn_item_for_wire(&item).unwrap();
    let decoded: Value = serde_json::from_str(&serde_json::to_string(projected.as_ref()).unwrap()).unwrap();
    assert_eq!(decoded["viewedImagePath"],json!("/workspace/reference.png"));
    assert_eq!(decoded["future"],json!({"keep":[1,2]}));
    assert!(decoded.get("output").is_none());
    assert_eq!(item["output"]["data"],json!("private-image-data"));
}

#[test]
fn upstream_even_small_dynamic_results_are_omitted_without_changing_input() {
    let item = tool(json!({"output":{"ok":true}}));
    let projected = WireProjector::default().project_turn_item_for_wire(&item).unwrap();
    let expected = tool(json!({"outputOmitted":true}));
    assert_eq!(*projected,*expected);
    assert_eq!(item["output"],json!({"ok":true}));
}

#[test]
fn upstream_dynamic_string_budget_counts_encoded_json_bytes() {
    let mut projector = WireProjector::default();
    let small = tool(json!({"input":"\"".repeat(8191)}));
    assert_eq!(*projector.project_turn_item_for_wire(&small).unwrap(),*small);
    let large = tool(json!({"input":"\"".repeat(8192)}));
    assert_eq!(projector.project_turn_item_for_wire(&large).unwrap()["input"],json!({"summary":format!("{}…","\"".repeat(159)),"truncated":true}));
    assert_eq!(large["input"].as_str().unwrap().len(),8192);
}

#[test]
fn unsupported_javascript_summary_surrogate_boundary_returns_an_explicit_error() {
    // Upstream slice(0,159) keeps only the emoji's high surrogate here.
    let input = format!("{}😀{}","a".repeat(158),"x".repeat(17000));
    let item = tool(json!({"input":input}));
    assert!(matches!(
        WireProjector::default().project_turn_item_for_wire(&item),
        Err(ProjectionError::UnpairedUtf16Boundary)
    ));
    assert!(item["input"].as_str().unwrap().contains('😀'));
}

#[test]
fn upstream_subagent_detail_is_capped_at_a_utf8_boundary_and_idempotent() {
    let prefix_bytes = 32768 - MARKER.len();
    let progress = format!("{}😀{}","a".repeat(prefix_bytes-1),"x".repeat(100000));
    let item = tool(json!({
        "type":"subagent","subagentId":"child-agent","origin":"app_owned","driver":"codex",
        "providerInstanceId":"codex","childThreadId":null,"prompt":"漢".repeat(12000),
        "result":null,"progress":progress
    }));
    let mut projector = WireProjector::default();
    let projected = projector.project_turn_item_for_wire(&item).unwrap();
    assert_eq!(*projector.project_turn_item_for_wire(&projected).unwrap(),*projected);
    assert!(projected["prompt"].as_str().unwrap().len()<=32768);
    assert_eq!(projected["progress"],json!(format!("{}{}","a".repeat(prefix_bytes-1),MARKER)));
    assert_eq!(item["progress"],json!(progress));
    assert_eq!(item["prompt"].as_str().unwrap().len(),36000);
}

#[test]
fn upstream_detail_removes_screenshot_bytes_but_keeps_image_marker() {
    let data = "A".repeat(600000);
    let item = tool(json!({
        "toolName":"mcp__t3-code__device_screenshot","input":{"deviceId":"phone"},
        "output":{"content":[{"type":"image","source":{"type":"base64","media_type":"image/png","data":data}}]}
    }));
    let projected = WireProjector::default().project_turn_item_for_detail(&item).unwrap();
    assert_eq!(projected["output"],json!({"content":[{"type":"image","mimeType":"image/png"}]}));
    assert_eq!(item["output"]["content"][0]["source"]["data"],json!(data));
}

#[test]
fn upstream_truncated_tool_output_keeps_ordered_image_markers() {
    let item = tool(json!({"output":{"content":[
        {"type":"text","text":"x".repeat(300000)},
        {"type":"image","source":{"type":"base64","media_type":"image/png","data":"AAAA"}}
    ]}}));
    let projected = WireProjector::default().project_turn_item_for_detail(&item).unwrap();
    let output = projected["output"].as_array().unwrap();
    assert_eq!(&output[1..],&[json!({"type":"image","mimeType":"image/png"})]);
    assert_eq!(output[0]["type"],json!("text"));
    assert!(output[0]["text"].as_str().unwrap().ends_with(MARKER));
    assert!(serde_json::to_string(&projected["output"]).unwrap().len()<270000);
    assert_eq!(item["output"]["content"][0]["text"].as_str().unwrap().len(),300000);
}

#[test]
fn upstream_many_images_produce_a_bounded_detail_payload() {
    let image = json!({"type":"image","data":"AAAA","mimeType":"image/png"});
    let item = tool(json!({"output":vec![image;10000]}));
    let projected = WireProjector::default().project_turn_item_for_detail(&item).unwrap();
    let output = projected["output"].as_array().unwrap();
    assert_eq!(output.len(),1+MAX_TOOL_OUTPUT_IMAGES);
    assert!(output[0]["text"].as_str().unwrap().len()<263000);
    assert_eq!(tool_output_images(projected.get("output")).len(),MAX_TOOL_OUTPUT_IMAGES);
    assert_eq!(item["output"].as_array().unwrap().len(),10000);
}

#[test]
fn upstream_command_failure_evidence_survives_output_redaction() {
    let item = tool(json!({"type":"command_execution","input":"cat missing-file","output":"cat: missing-file: No such file or directory"}));
    let mut projector = WireProjector::default();
    let projected = projector.project_turn_item_for_wire(&item).unwrap();
    assert!(projected.get("output").is_none());
    assert_eq!(projected["outputIndicatesFailure"],json!(true));
    assert_eq!(*projector.project_turn_item_for_wire(&projected).unwrap(),*projected);
    let explicit = tool(json!({"type":"command_execution","input":"false","output":"","exitCode":2}));
    let projected = projector.project_turn_item_for_wire(&explicit).unwrap();
    assert_eq!(projected["outputIndicatesFailure"],json!(true));
    assert_eq!(projected["exitCode"],json!(2));
    assert!(projected.get("outputOmitted").is_none());
    assert_eq!(item["output"],json!("cat: missing-file: No such file or directory"));
}

#[test]
fn upstream_zero_exit_code_is_authoritative_over_failure_text() {
    for output in [
        "logs/app.log:12: Error: ENOENT: no such file or directory",
        "fixtures/ci.txt:3: Process exited with exit code 2",
    ] {
        let item = tool(json!({"type":"command_execution","input":"rg -n error logs fixtures","output":output,"exitCode":0}));
        let projected = WireProjector::default().project_turn_item_for_wire(&item).unwrap();
        assert!(projected.get("outputIndicatesFailure").is_none());
        assert_eq!(projected["exitCode"],json!(0));
        assert_eq!(projected["outputOmitted"],json!(true));
    }
}

#[test]
fn upstream_file_change_wire_and_detail_preserve_identity_counts_and_failure_evidence() {
    let item = tool(json!({
        "type":"file_change","fileName":"src/main.ts","additions":3,"deletions":1,
        "diffStr":"+new code","oldStr":"old code","newStr":"new code","future":{"keep":true}
    }));
    let mut projector = WireProjector::default();
    let projected = projector.project_turn_item_for_wire(&item).unwrap();
    for field in ["diffStr","oldStr","newStr"] { assert!(projected.get(field).is_none()); }
    for field in ["fileName","additions","deletions","future"] { assert_eq!(projected[field],item[field]); }
    let detail = projector.project_turn_item_for_detail(&item).unwrap();
    assert_eq!(detail["diffStr"],json!("+new code"));
    assert!(detail.get("oldStr").is_none());
    assert!(detail.get("newStr").is_none());
    let mut content = (*item).clone();
    content.as_object_mut().unwrap().remove("diffStr");
    let content = Arc::new(content);
    let detail = projector.project_turn_item_for_detail(&content).unwrap();
    assert_eq!(detail["oldStr"],json!("old code"));
    assert_eq!(detail["newStr"],json!("new code"));
    assert!(projector.project_turn_item_for_wire(&content).unwrap().get("newStr").is_none());
    let failed = tool(json!({"type":"file_change","status":"failed","diffStr":"String to replace not found","newStr":"private body"}));
    let projected = projector.project_turn_item_for_wire(&failed).unwrap();
    assert_eq!(projected["diffStr"],json!("String to replace not found"));
    assert!(projected.get("newStr").is_none());
    assert_eq!(item["diffStr"],json!("+new code"));
}

#[test]
fn stored_file_bodies_are_bounded_once_without_changing_unrelated_items() {
    let unrelated = tool(json!({"output":"private"}));
    assert!(Arc::ptr_eq(&bound_file_change_for_storage(&unrelated).unwrap(),&unrelated));
    let small = tool(json!({"type":"file_change","diffStr":"+ok","oldStr":"before","newStr":"after"}));
    assert!(Arc::ptr_eq(&bound_file_change_for_storage(&small).unwrap(),&small));
    let large = tool(json!({"type":"file_change","diffStr":"漢".repeat(20000),"oldStr":"a".repeat(50000),"newStr":"😀".repeat(20000),"future":42}));
    let bounded = bound_file_change_for_storage(&large).unwrap();
    for field in ["diffStr","oldStr","newStr"] {
        assert!(bounded[field].as_str().unwrap().len()<=32768);
        assert!(bounded[field].as_str().unwrap().ends_with(MARKER));
    }
    assert_eq!(bounded["future"],json!(42));
    assert!(Arc::ptr_eq(&bound_file_change_for_storage(&bounded).unwrap(),&bounded));
    assert_eq!(large["diffStr"].as_str().unwrap().len(),60000);
}

#[test]
fn fetched_command_input_and_output_use_the_larger_detail_budget() {
    let item = tool(json!({"type":"command_execution","input":"a".repeat(300000),"output":"😀".repeat(100000),"exitCode":3}));
    let projected = WireProjector::default().project_turn_item_for_detail(&item).unwrap();
    for field in ["input","output"] {
        assert!(projected[field].as_str().unwrap().len()<=262144);
        assert!(projected[field].as_str().unwrap().ends_with(MARKER));
    }
    assert_eq!(projected["exitCode"],json!(3));
    assert_eq!(item["output"].as_str().unwrap().len(),400000);
}

#[test]
fn upstream_live_dynamic_results_keep_only_identity_and_failure_metadata() {
    let output = json!({
        "isError":true,"structuredContent":{"threadId":"child-thread","messageId":"message","text":"PRIVATE_BODY"},
        "content":[{"type":"text","text":"PRIVATE_BODY"}]
    });
    let input = event("turn-item.updated",(*tool(json!({"output":output}))).clone());
    let original = (*input).clone();
    let mut projector = WireProjector::default();
    let projected = projector.project_domain_event_for_wire(&input).unwrap();
    assert_eq!(projected["payload"]["output"],json!({"isError":true,"threadId":"child-thread","messageId":"message"}));
    assert!(!serde_json::to_string(projected.as_ref()).unwrap().contains("PRIVATE_BODY"));
    assert_eq!(*input,original);
    assert_eq!(*projector.project_domain_event_for_wire(&projected).unwrap(),*projected);
}

#[test]
fn upstream_stored_event_projection_is_cached_and_detached_events_keep_identity() {
    let stored = Arc::new(json!({"sequence":1,"commandId":null,"future":{"keep":true},"event":event("turn-item.updated",(*tool(json!({"output":{"content":[{"type":"text","text":"PRIVATE_BODY"}]}}))).clone())}));
    let original = (*stored).clone();
    let mut projector = WireProjector::default();
    let projected = projector.project_stored_event_for_wire(&stored).unwrap();
    assert!(Arc::ptr_eq(&projected,&projector.project_stored_event_for_wire(&stored).unwrap()));
    assert_eq!(projected["future"],json!({"keep":true}));
    let expected_event = projector.project_domain_event_for_wire(&Arc::new(stored["event"].clone())).unwrap();
    assert_eq!(projected["event"],*expected_event);
    assert_eq!(*stored,original);
    let equal_but_distinct = Arc::new((*stored).clone());
    assert!(!Arc::ptr_eq(&projected,&projector.project_stored_event_for_wire(&equal_but_distinct).unwrap()));
    let detached = Arc::new(json!({"sequence":2,"commandId":null,"event":event("provider-session.detached",json!({"providerSessionId":"session","detachedAt":AT}))}));
    assert!(Arc::ptr_eq(&projector.project_stored_event_for_wire(&detached).unwrap(),&detached));
}

#[test]
fn thread_stream_events_share_cached_wire_envelopes_without_command_metadata() {
    let mut projector = WireProjector::default();
    let stored = Arc::new(json!({"sequence":7,"commandId":"private-receipt","event":event("provider-session.detached",json!({"providerSessionId":"session","detachedAt":AT})),"future":"stored-only"}));
    let first = projector.thread_stream_event_item(&stored).unwrap();
    let second = projector.thread_stream_event_item(&stored).unwrap();
    assert!(Arc::ptr_eq(&first,&second));
    assert_eq!(*first,json!({"kind":"event","sequence":7,"event":stored["event"]}));
    assert_eq!(stored["commandId"],json!("private-receipt"));
}

#[test]
fn cached_stream_envelopes_release_expired_sources_and_keep_live_identity() {
    let mut projector = WireProjector::default();
    let mut sources = Vec::new();
    let mut results = Vec::new();
    for sequence in 0..1024 {
        let stored = Arc::new(json!({"sequence":sequence,"event":{"type":"unrelated","payload":sequence}}));
        let projected = projector.thread_stream_event_item(&stored).unwrap();
        results.push(Arc::downgrade(&projected));
        sources.push(stored);
    }
    let live = sources.split_off(512);
    drop(sources);
    // Active consumers eventually release expired entries without invalidating
    // another still-live source's shared output, even after many insertions.
    for _ in 0..1024 {
        let projected = projector.thread_stream_event_item(&live[0]).unwrap();
        assert!(Arc::ptr_eq(&projected,&results[512].upgrade().unwrap()));
    }
    assert!(results[..512].iter().all(|result| result.upgrade().is_none()));
    for (source, expected) in live.iter().zip(&results[512..]) {
        assert!(Arc::ptr_eq(&projector.thread_stream_event_item(source).unwrap(),&expected.upgrade().unwrap()));
    }
    drop(live);
    drop(projector);
    assert!(results.iter().all(|result| result.upgrade().is_none()));
}

#[test]
fn checkpoint_caps_preserve_totals_and_cached_identity() {
    let item = tool(json!({"type":"checkpoint","checkpointId":"checkpoint-large","scopeId":"scope-large","files":files(250),"future":{"keep":true}}));
    let mut projector = WireProjector::default();
    let capped = projector.project_turn_item_for_wire(&item).unwrap();
    assert_eq!(capped["files"],files(200));
    assert_eq!(capped["filesTotal"],json!({"count":250,"additions":500,"deletions":250}));
    assert_eq!(capped["future"],json!({"keep":true}));
    assert!(Arc::ptr_eq(&capped,&projector.project_turn_item_for_wire(&item).unwrap()));
    assert!(Arc::ptr_eq(&capped,&projector.project_turn_item_for_wire(&capped).unwrap()));
    assert_eq!(item["files"].as_array().unwrap().len(),250);
    let already_totaled = tool(json!({"type":"checkpoint","files":files(250),"filesTotal":{"count":300,"additions":700,"deletions":350}}));
    assert!(Arc::ptr_eq(&projector.project_turn_item_for_wire(&already_totaled).unwrap(),&already_totaled));
}

#[test]
fn projection_checkpoint_cap_keeps_shared_items_and_is_idempotent() {
    let item = tool(json!({"type":"checkpoint","checkpointId":"checkpoint-large","files":files(250)}));
    let mut full = (*projection(vec![item])).clone();
    full.checkpoints = vec![json!({"id":"checkpoint-large","files":files(250)})];
    full.extra.insert("future".into(),json!({"keep":true}));
    let full = Arc::new(full);
    let mut projector = WireProjector::default();
    let capped = projector.cap_projection_checkpoint_files_for_wire(&full).unwrap();
    assert_eq!(capped.checkpoints[0]["files"],files(200));
    assert_eq!(capped.checkpoints[0]["filesTotal"],json!({"count":250,"additions":500,"deletions":250}));
    assert!(Arc::ptr_eq(&capped.turn_items[0],&capped.visible_turn_items[0].item));
    assert!(Arc::ptr_eq(&projector.cap_projection_checkpoint_files_for_wire(&capped).unwrap(),&capped));
    assert_eq!(capped.extra["future"],json!({"keep":true}));
    assert_eq!(full.checkpoints[0]["files"].as_array().unwrap().len(),250);
    let unrelated = projection(vec![tool(json!({"type":"assistant_message","messageId":"m","text":"hi"}))]);
    assert!(Arc::ptr_eq(&projector.cap_projection_checkpoint_files_for_wire(&unrelated).unwrap(),&unrelated));
}

#[test]
fn thread_projection_reuses_the_first_item_with_the_same_upstream_composite_identity() {
    let first = tool(json!({"id":"shared","output":{"content":[{"type":"text","text":"PRIVATE_FIRST"}]}}));
    let mut full = (*projection(vec![first])).clone();
    let same_id = tool(json!({"id":"shared","input":{"pr":99},"output":"PRIVATE_SECOND"}));
    let different_thread = tool(json!({"id":"shared","threadId":"parent-thread","input":{"pr":88}}));
    let mut local = full.visible_turn_items[0].as_ref().clone();
    local.item = same_id;
    let mut inherited = local.clone();
    inherited.source_thread_id = "parent-thread".into();
    inherited.visibility = "inherited".into();
    inherited.item = different_thread;
    full.visible_turn_items = vec![Arc::new(local),Arc::new(inherited)];
    let full = Arc::new(full);
    let projected = WireProjector::default().project_thread_projection_for_wire(&full).unwrap();
    assert!(Arc::ptr_eq(&projected.turn_items[0],&projected.visible_turn_items[0].item));
    assert_eq!(projected.visible_turn_items[0].item["input"],json!({"pr":42}));
    assert!(!Arc::ptr_eq(&projected.turn_items[0],&projected.visible_turn_items[1].item));
    assert_eq!(projected.visible_turn_items[1].item["input"],json!({"pr":88}));
    assert_eq!(full.visible_turn_items[0].item["input"],json!({"pr":99}));
}

#[test]
fn context_handoff_records_and_events_strip_private_delivery_and_history() {
    let record = json!({"id":"handoff","history":[{"text":"PRIVATE_HISTORY"}],"delivery":{"text":"PRIVATE_DELIVERY"},"summaryText":"PRIVATE_SUMMARY","strategy":"full_thread_summary","future":{"keep":true}});
    let expected = json!({"id":"handoff","summaryText":"","strategy":"full_thread_summary","future":{"keep":true}});
    assert_eq!(project_context_handoff_for_wire(&record).unwrap(),expected);
    let projected = WireProjector::default().project_domain_event_for_wire(&event("context-handoff.updated",record.clone())).unwrap();
    assert_eq!(projected["payload"],expected);
    assert_eq!(record["summaryText"],json!("PRIVATE_SUMMARY"));
}

#[test]
fn upstream_live_reply_is_deduped_after_an_already_coalesced_message() {
    // The original test coalesces [message(1,"a"), reply(2,"a"), message(3,"ab"),
    // reply(4,"ab")]. Its coalescer is not part of this port; these are its outputs.
    let delivered_message = message(3,"message-live","ab");
    let delivered_reply = reply(4,"message-live","ab");
    let mut deduper = LiveBodyDeduper::default();
    assert!(Arc::ptr_eq(&deduper.apply(&delivered_message).unwrap(),&delivered_message));
    let projected = deduper.apply(&delivered_reply).unwrap();
    let mut expected = (*delivered_reply).clone();
    expected["event"]["payload"]["text"] = json!("");
    expected["event"]["payload"]["textOmitted"] = json!(true);
    assert_eq!(*projected,expected);
    assert_eq!(payload(&delivered_reply)["text"],json!("ab"));
}

#[test]
fn upstream_live_checkpoint_is_capped_then_deduped_against_its_record() {
    let captured = stream(1,"checkpoint.captured",json!({
        "id":"checkpoint-large","threadId":"thread-1","scopeId":"scope-large","runId":null,
        "nodeId":"node-large","parentCheckpointId":null,"ordinalWithinScope":1,"appRunOrdinal":null,
        "ref":"refs/t3/checkpoints/large","status":"ready","files":files(250),"capturedAt":AT
    }));
    let card = stream(2,"turn-item.updated",(*tool(json!({"type":"checkpoint","checkpointId":"checkpoint-large","scopeId":"scope-large","files":files(250)}))).clone());
    let mut projector = WireProjector::default();
    let mut deduper = LiveBodyDeduper::default();
    let mut delivered = Vec::new();
    for streamed in [&captured,&card] {
        let projected_event = projector.project_domain_event_for_wire(&Arc::new(streamed["event"].clone())).unwrap();
        let mut envelope = (**streamed).clone();
        envelope["event"] = (*projected_event).clone();
        delivered.push(deduper.apply(&Arc::new(envelope)).unwrap());
    }
    assert_eq!(payload(&delivered[0])["files"],files(200));
    assert_eq!(payload(&delivered[0])["filesTotal"],json!({"count":250,"additions":500,"deletions":250}));
    assert_eq!(payload(&delivered[1])["files"],json!([]));
    assert_eq!(payload(&delivered[1])["filesOmitted"],json!(true));
    assert_eq!(payload(&delivered[1])["filesTotal"],json!({"count":250,"additions":500,"deletions":250}));
    assert_eq!(payload(&card)["files"].as_array().unwrap().len(),250);
}

#[test]
fn upstream_reply_sent_before_its_message_keeps_its_body_and_identity() {
    let first = reply(1,"message-live","a");
    let mut deduper = LiveBodyDeduper::default();
    assert!(Arc::ptr_eq(&deduper.apply(&first).unwrap(),&first));
    deduper.apply(&message(2,"message-live","a")).unwrap();
    assert_eq!(payload(&first)["text"],json!("a"));
    let mismatch = reply(3,"message-live","different");
    assert!(Arc::ptr_eq(&deduper.apply(&mismatch).unwrap(),&mismatch));
}

#[test]
fn live_deduper_uses_32_records_and_reinsertion_refreshes_eviction_order() {
    let mut deduper = LiveBodyDeduper::default();
    for index in 0..32 {
        deduper.apply(&message(index,&format!("m-{index}"),"body")).unwrap();
    }
    deduper.apply(&message(32,"m-0","updated")).unwrap();
    deduper.apply(&message(33,"m-32","body")).unwrap();
    let retained = deduper.apply(&reply(34,"m-0","updated")).unwrap();
    assert_eq!(payload(&retained)["textOmitted"],json!(true));
    let evicted = reply(35,"m-1","body");
    assert!(Arc::ptr_eq(&deduper.apply(&evicted).unwrap(),&evicted));
    // A body lookup is not a reinsertion; m-2 remains oldest and is evicted next.
    assert_eq!(payload(&deduper.apply(&reply(36,"m-2","body")).unwrap())["textOmitted"],json!(true));
    deduper.apply(&message(37,"m-33","body")).unwrap();
    let evicted = reply(38,"m-2","body");
    assert!(Arc::ptr_eq(&deduper.apply(&evicted).unwrap(),&evicted));
}

#[test]
fn snapshot_seeds_all_body_indexes_and_subagent_repetition_is_field_specific() {
    let snapshot = Arc::new(json!({"kind":"snapshot","projection":{
        "messages":[{"id":"m","text":"hello"}],
        "checkpoints":[{"id":"c","files":files(1)}],
        "subagents":[{"id":"s","prompt":"task","result":"done"}]
    }}));
    let mut deduper = LiveBodyDeduper::default();
    assert!(Arc::ptr_eq(&deduper.apply(&snapshot).unwrap(),&snapshot));
    assert_eq!(payload(&deduper.apply(&reply(1,"m","hello")).unwrap())["textOmitted"],json!(true));
    let checkpoint = stream(2,"turn-item.updated",(*tool(json!({"type":"checkpoint","checkpointId":"c","files":files(1)}))).clone());
    assert_eq!(payload(&deduper.apply(&checkpoint).unwrap())["filesOmitted"],json!(true));
    let updated = stream(3,"subagent.updated",json!({"id":"s","prompt":"task","result":"new result","future":{"keep":true}}));
    let projected = deduper.apply(&updated).unwrap();
    assert_eq!(payload(&projected),&json!({"id":"s","prompt":"","promptOmitted":true,"result":"new result","future":{"keep":true}}));
    assert_eq!(payload(&updated)["prompt"],json!("task"));
    let subagent_item = stream(4,"turn-item.updated",(*tool(json!({"type":"subagent","subagentId":"s","prompt":"task","result":"new result"}))).clone());
    let projected = deduper.apply(&subagent_item).unwrap();
    assert_eq!(payload(&projected)["promptOmitted"],json!(true));
    assert_eq!(payload(&projected)["resultOmitted"],json!(true));
    let unrelated = Arc::new(json!({"kind":"heartbeat","future":true}));
    assert!(Arc::ptr_eq(&deduper.apply(&unrelated).unwrap(),&unrelated));
}

#[test]
fn live_subagent_memory_retains_only_the_received_prompt_and_result() {
    let snapshot = Arc::new(json!({"kind":"snapshot","projection":{
        "messages":[],"checkpoints":[],"subagents":[{
            "id":"s","prompt":"task","result":"done","progress":"x".repeat(2_000_000),
            "promptOmitted":true,"future":{"private":"not part of body memory"}
        }]
    }}));
    let mut deduper = LiveBodyDeduper::default();
    assert!(Arc::ptr_eq(&deduper.apply(&snapshot).unwrap(),&snapshot));
    assert_eq!(BodySource::subagent(&deduper,"s"),Some(&json!({"prompt":"task","result":"done"})));

    let updated = stream(1,"subagent.updated",json!({
        "id":"s","prompt":"task","result":"new result","progress":"y".repeat(2_000_000),
        "future":{"private":"also not retained"}
    }));
    let projected = deduper.apply(&updated).unwrap();
    assert_eq!(payload(&projected)["promptOmitted"],json!(true));
    assert_eq!(BodySource::subagent(&deduper,"s"),Some(&json!({"prompt":"task","result":"new result"})));
    assert_eq!(payload(&updated)["prompt"],json!("task"));
}

#[test]
fn client_restores_server_compactions_without_restoring_private_tool_or_handoff_bodies() {
    let message_item = tool(json!({"id":"message-item","type":"assistant_message","messageId":"m","text":"answer","streaming":false}));
    let checkpoint_item = tool(json!({"id":"checkpoint-item","type":"checkpoint","checkpointId":"c","files":files(250)}));
    let tool_item = tool(json!({"id":"dynamic-item","output":{"content":[{"type":"text","text":"PRIVATE_TOOL_BODY"}]}}));
    let mut full = (*projection(vec![message_item,checkpoint_item,tool_item])).clone();
    full.messages = vec![json!({"id":"m","text":"answer"})];
    full.checkpoints = vec![json!({"id":"c","files":files(250)})];
    full.context_handoffs = vec![json!({"id":"h","history":["PRIVATE_HISTORY"],"delivery":{"body":"PRIVATE_DELIVERY"},"summaryText":"PRIVATE_HANDOFF"})];
    full.extra.insert("future".into(),json!({"nested":[1,2]}));
    let full = Arc::new(full);
    let original = wire(&full);
    let mut projector = WireProjector::default();
    let projected = projector.project_thread_projection_for_wire(&full).unwrap();
    let expected = wire(&projected);
    assert!(Arc::ptr_eq(&projected.turn_items[0],&projected.visible_turn_items[0].item));
    let deduped = dedupe_snapshot_bodies(projected);
    assert_eq!(deduped.turn_items[0]["textOmitted"],json!(true));
    assert_eq!(deduped.turn_items[1]["filesOmitted"],json!(true));
    let compact = omit_local_visible_turn_items(deduped).unwrap();
    assert!(compact.turn_items.is_empty());
    let encoded = serde_json::to_string(compact.as_ref()).unwrap();
    for secret in ["PRIVATE_TOOL_BODY","PRIVATE_HISTORY","PRIVATE_DELIVERY","PRIVATE_HANDOFF"] {
        assert!(!encoded.contains(secret));
    }
    let decoded:ThreadProjection = serde_json::from_str(&encoded).unwrap();
    let restored = bounded_snapshot_projection(Arc::new(decoded),true);
    assert_eq!(wire(&restored),expected);
    assert_eq!(restored.extra["future"],json!({"nested":[1,2]}));
    assert_eq!(wire(&full),original);
}
