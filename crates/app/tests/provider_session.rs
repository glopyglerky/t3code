#![cfg(unix)]

use std::path::PathBuf;
use std::time::{Duration, Instant};

use serde_json::{Value, json};
use t3_code_ls_app::provider::{CodexSession, RequestFailure, SessionError, SessionEvent};
use t3_code_ls_core::provider_process::{Config, Limits};
use tempfile::TempDir;

fn config(directory: &TempDir, scenario: &str) -> Config {
    Config {
        program: PathBuf::from(env!("CARGO_BIN_EXE_t3-code-ls-provider-fixture")),
        args: vec![scenario.to_owned()],
        cwd: directory.path().to_owned(),
        home: Some(directory.path().to_owned()),
        inherit_environment: false,
        inherit_stderr: false,
        track_requests: true,
        environment: Vec::new(),
        limits: Limits {
            max_frame_bytes: 1024 * 1024,
            request_timeout: Duration::from_secs(5),
            ..Limits::default()
        },
    }
}

struct Fixture {
    session: CodexSession,
    _directory: TempDir,
}

fn fixture(scenario: &str) -> Fixture {
    let directory = tempfile::tempdir().unwrap();
    let session = CodexSession::spawn(config(&directory, scenario)).unwrap();
    Fixture {
        session,
        _directory: directory,
    }
}

fn until(
    session: &mut CodexSession,
    predicate: impl Fn(&SessionEvent) -> bool,
) -> Vec<SessionEvent> {
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut events = Vec::new();
    for _ in 0..64 {
        let remaining = deadline.saturating_duration_since(Instant::now());
        assert!(
            !remaining.is_zero(),
            "fixture did not produce the requested protocol milestone"
        );
        let event = session
            .poll(remaining)
            .unwrap()
            .expect("fixture stopped producing protocol events");
        let finished = predicate(&event);
        events.push(event);
        if finished {
            return events;
        }
    }
    panic!("fixture produced too many events without the requested protocol milestone")
}

fn response(session: &mut CodexSession, expected: &Value) -> Value {
    let events = until(
        session,
        |event| matches!(event, SessionEvent::Response { id, .. } if id == expected),
    );
    match events.last().unwrap() {
        SessionEvent::Response { result, .. } => result.clone(),
        _ => unreachable!(),
    }
}

fn start_waiting_turn(session: &mut CodexSession) -> Value {
    let id = session
        .start_thread(json!({"model":"fixture-model"}))
        .unwrap();
    let thread = response(session, &id)["thread"]["id"].clone();
    let id = session
        .start_turn(json!({
            "threadId":thread,
            "input":[{"type":"text","text":"controlled input","text_elements":[]}]
        }))
        .unwrap();
    assert_eq!(response(session, &id)["turn"]["id"], "native-turn");
    let events = until(
        session,
        |event| matches!(event, SessionEvent::ServerRequest { id, .. } if id == &json!(100)),
    );
    assert!(events.iter().any(|event| matches!(event, SessionEvent::Notification { method, params } if method == "item/agentMessage/delta" && params["delta"] == "Before approval. ")));
    thread
}

#[cfg(target_os = "linux")]
fn assert_reaped(id: u32) {
    assert!(
        !std::path::Path::new(&format!("/proc/{id}")).exists(),
        "owned provider child is still present"
    );
}

#[cfg(not(target_os = "linux"))]
fn assert_reaped(_: u32) {}

#[test]
fn buffers_initialization_traffic_and_requires_an_explicit_startup_answer() {
    let mut fixture = fixture("pre-init-traffic");
    let session = &mut fixture.session;
    assert_eq!(
        session.initialization()["userAgent"],
        "t3code-ls-controlled-peer/1"
    );
    assert!(
        matches!(session.poll(Duration::ZERO).unwrap(), Some(SessionEvent::Notification { method, .. }) if method == "fixture/initializing")
    );
    assert!(
        matches!(session.poll(Duration::ZERO).unwrap(), Some(SessionEvent::ServerRequest { id, .. }) if id == json!("startup-request"))
    );
    let id = session.start_thread(json!({})).unwrap();
    assert_eq!(
        response(session, &id)["fixture"]["startupRequestPending"],
        true
    );
    session
        .respond(&json!("startup-request"), json!({"answers":{}}))
        .unwrap();
    until(
        session,
        |event| matches!(event, SessionEvent::Notification { method, .. } if method == "fixture/startupAnswered"),
    );
    assert!(
        session
            .respond(&json!("startup-request"), json!({"answers":{}}))
            .is_err()
    );
    let id = session.process_id();
    session.shutdown().unwrap();
    assert!(matches!(
        session.poll(Duration::ZERO),
        Err(SessionError::Closed)
    ));
    assert_reaped(id);
}

#[test]
fn starts_resumes_and_streams_a_turn_only_after_the_observed_approval_is_answered() {
    let mut fixture = fixture("happy");
    let session = &mut fixture.session;
    let thread = start_waiting_turn(session);
    let id = session
        .resume_thread(json!({"threadId":thread,"fixture":"approval-status"}))
        .unwrap();
    let resumed = response(session, &id);
    assert_eq!(resumed["thread"]["id"], thread);
    assert_eq!(resumed["fixture"]["approvalPending"], true);
    assert!(
        session
            .respond(&json!(999), json!({"decision":"accept"}))
            .is_err()
    );
    session
        .respond(&json!(100), json!({"decision":"accept"}))
        .unwrap();
    let events = until(
        session,
        |event| matches!(event, SessionEvent::Notification { method, .. } if method == "turn/completed"),
    );
    assert!(events.iter().any(|event| matches!(event, SessionEvent::Notification { method, params } if method == "item/agentMessage/delta" && params["delta"] == "After approval.")));
    assert!(
        matches!(events.last(), Some(SessionEvent::Notification { params, .. }) if params["turn"]["status"] == "completed")
    );
    assert!(
        session
            .respond(&json!(100), json!({"decision":"accept"}))
            .is_err()
    );
    assert_eq!(session.pending_requests(), 0);
    session.shutdown().unwrap();
}

#[test]
fn an_rpc_error_does_not_destroy_the_session_or_the_next_request() {
    let mut fixture = fixture("happy");
    let session = &mut fixture.session;
    let id = session
        .start_thread(json!({"fixture":"provider-error"}))
        .unwrap();
    let events = until(
        session,
        |event| matches!(event, SessionEvent::RequestError { id: actual, .. } if actual == &id),
    );
    assert!(
        matches!(events.last(), Some(SessionEvent::RequestError { method, error, .. }) if method == "thread/start" && error.code == -32001 && error.message == "controlled rejection")
    );
    assert_eq!(session.pending_requests(), 0);
    let id = session.start_thread(json!({})).unwrap();
    assert_eq!(response(session, &id)["thread"]["id"], "native-thread");
    session.shutdown().unwrap();
}

#[test]
fn interrupt_retires_an_outstanding_approval_and_keeps_the_session_open() {
    let mut fixture = fixture("happy");
    let session = &mut fixture.session;
    let thread = start_waiting_turn(session);
    let id = session
        .interrupt_turn(thread.as_str().unwrap(), "native-turn")
        .unwrap();
    assert_eq!(response(session, &id), json!({}));
    assert!(
        session
            .respond(&json!(100), json!({"decision":"accept"}))
            .is_err()
    );
    let events = until(
        session,
        |event| matches!(event, SessionEvent::Notification { method, .. } if method == "turn/completed"),
    );
    assert!(
        matches!(events.last(), Some(SessionEvent::Notification { params, .. }) if params["turn"]["status"] == "interrupted")
    );
    let id = session.resume_thread(json!({"threadId":thread})).unwrap();
    assert_eq!(response(session, &id)["thread"]["id"], thread);
    session.shutdown().unwrap();
}

#[test]
fn malformed_and_wrong_thread_replies_close_the_owned_process() {
    for params in [
        json!({"threadId":"native-thread","fixture":"wrong-thread"}),
        json!({"threadId":"native-thread","fixture":"ambiguous"}),
    ] {
        let mut fixture = fixture("happy");
        let session = &mut fixture.session;
        let process = session.process_id();
        session.resume_thread(params).unwrap();
        let mut failure = None;
        for _ in 0..8 {
            match session.poll(Duration::from_secs(5)) {
                Err(error) => {
                    failure = Some(error);
                    break;
                }
                Ok(Some(_)) => {}
                Ok(None) => panic!("controlled malformed response was not received"),
            }
        }
        assert!(matches!(failure, Some(SessionError::Protocol(_))));
        assert!(matches!(
            session.start_thread(json!({})),
            Err(SessionError::Closed)
        ));
        assert_reaped(process);
    }
}

#[test]
fn provider_death_reports_unknown_request_outcome_and_reaps_the_child() {
    let mut fixture = fixture("happy");
    let session = &mut fixture.session;
    let process = session.process_id();
    let id = session.start_thread(json!({"fixture":"exit"})).unwrap();
    let events = until(
        session,
        |event| matches!(event, SessionEvent::RequestFailed { id: actual, .. } if actual == &id),
    );
    assert!(matches!(
        events.last(),
        Some(SessionEvent::RequestFailed {
            failure: RequestFailure::Disconnected,
            ..
        })
    ));
    assert_eq!(session.pending_requests(), 0);
    assert!(matches!(
        session.poll(Duration::ZERO),
        Err(SessionError::Transport(_))
    ));
    assert_reaped(process);
}

#[test]
fn initialization_failure_reaps_even_before_a_session_is_returned() {
    for scenario in ["abort-initialize", "malformed-initialize"] {
        let directory = tempfile::tempdir().unwrap();
        assert!(CodexSession::spawn(config(&directory, scenario)).is_err());
        let pid: u32 = std::fs::read_to_string(directory.path().join("fixture.pid"))
            .unwrap()
            .parse()
            .unwrap();
        assert_reaped(pid);
    }
}
