#[path = "../src/test_support.rs"]
mod test_support;

use std::io::{BufRead, BufReader, Write};
use std::process::{Command, Stdio};

use serde_json::{Value, json};
use t3_code_ls_storage::{CommitBatch, Error, ProjectionMutation, Record, Store, request_digest};

fn batch(command: &str, revision: u64) -> CommitBatch {
    CommitBatch {
        command_id: command.to_owned(),
        request_digest: request_digest(&json!({"command": command})),
        expected_revision: revision,
        events: vec![Record {
            id: format!("event:{command}"),
            payload: json!({"nested": {"null": null}, "number": 7}),
        }],
        projections: vec![ProjectionMutation::Put {
            key: "project:one".into(),
            payload: json!({"title": command, "optional": null}),
        }],
        effects: vec![Record {
            id: format!("effect:{command}"),
            payload: json!({"type": "cleanup"}),
        }],
        result: json!({"accepted": true}),
    }
}

#[tokio::test]
async fn atomic_batch_receipt_and_stale_snapshot() {
    let fixture = test_support::TestFixture::start().await;
    let store = fixture.initialize().await;
    assert_eq!(store.snapshot().await.unwrap().revision, 0);
    let command = batch("one", 0);
    let result = store.commit(&command).await.unwrap();
    assert!(!result.replayed);
    assert_eq!(result.receipt.first_sequence, Some(1));
    assert_eq!(result.receipt.last_sequence, 1);
    assert_eq!(
        store.events(0, 100).await.unwrap()[0].payload,
        command.events[0].payload
    );
    assert_eq!(store.pending_effects(100).await.unwrap().len(), 1);
    let snapshot = store.snapshot().await.unwrap();
    assert_eq!(snapshot.revision, 1);
    assert_eq!(
        snapshot.projections[0].payload,
        json!({"title":"one", "optional":null})
    );
    assert_eq!(
        store.read_receipt("one").await.unwrap(),
        Some(result.receipt.clone())
    );
    let replay = store.commit(&command).await.unwrap();
    assert!(replay.replayed);
    assert_eq!(replay.receipt, result.receipt);
    assert_eq!(store.events(0, 100).await.unwrap().len(), 1);
    let mut wrong = command.clone();
    wrong.request_digest = request_digest(&json!({"other": "request"}));
    assert!(matches!(
        store.commit(&wrong).await,
        Err(Error::CommandConflict)
    ));
    assert!(matches!(
        store.commit(&batch("stale", 0)).await,
        Err(Error::StaleRevision {
            expected: 0,
            actual: 1
        })
    ));
    assert_eq!(store.snapshot().await.unwrap(), snapshot);
    assert!(store.read_receipt("stale").await.unwrap().is_none());
}

#[tokio::test]
async fn late_duplicate_rolls_back_every_part_of_batch() {
    let fixture = test_support::TestFixture::start().await;
    let store = fixture.initialize().await;
    store.commit(&batch("first", 0)).await.unwrap();
    let before = store.snapshot().await.unwrap();
    let mut collision = batch("collision", 1);
    collision.effects[0].id = "effect:first".into();
    assert!(matches!(
        store.commit(&collision).await,
        Err(Error::DuplicateRecord)
    ));
    assert_eq!(store.snapshot().await.unwrap(), before);
    assert_eq!(store.events(0, 100).await.unwrap().len(), 1);
    assert_eq!(store.pending_effects(100).await.unwrap().len(), 1);
    assert!(store.read_receipt("collision").await.unwrap().is_none());
    let committed = store.commit(&batch("next", 1)).await.unwrap();
    assert_eq!(committed.receipt.first_sequence, Some(2));
}

#[tokio::test]
async fn concurrent_sessions_cannot_both_commit_a_stale_plan() {
    let fixture = test_support::TestFixture::start().await;
    let store = fixture.initialize().await;
    let other = fixture.connect().await;
    let left = batch("left", 0);
    let mut right = batch("right", 0);
    right.projections = vec![ProjectionMutation::Put {
        key: "project:two".into(),
        payload: json!({"independent_project": true}),
    }];
    let (left, right) = tokio::join!(store.commit(&left), other.commit(&right));
    assert_eq!(usize::from(left.is_ok()) + usize::from(right.is_ok()), 1);
    let rejected = if left.is_err() { left } else { right };
    assert!(matches!(
        rejected,
        Err(Error::TransactionConflict | Error::StaleRevision { .. })
    ));
    let snapshot = store.snapshot().await.unwrap();
    assert_eq!(snapshot.revision, 1);
    assert_eq!(snapshot.last_sequence, 1);
    assert_eq!(store.events(0, 100).await.unwrap().len(), 1);
    assert_eq!(store.pending_effects(100).await.unwrap().len(), 1);
}

#[tokio::test]
async fn null_projection_and_delete_have_distinct_meanings() {
    let fixture = test_support::TestFixture::start().await;
    let store = fixture.initialize().await;
    let mut put = batch("put-null", 0);
    put.events.clear();
    put.effects.clear();
    put.projections = vec![ProjectionMutation::Put {
        key: "nullable".into(),
        payload: Value::Null,
    }];
    let outcome = store.commit(&put).await.unwrap();
    assert_eq!(outcome.receipt.first_sequence, None);
    assert_eq!(outcome.receipt.last_sequence, 0);
    assert_eq!(
        store.snapshot().await.unwrap().projections[0].payload,
        Value::Null
    );
    assert!(store.commit(&put).await.unwrap().replayed);
    let mut delete = batch("delete-null", 1);
    delete.events.clear();
    delete.effects.clear();
    delete.projections = vec![ProjectionMutation::Delete {
        key: "nullable".into(),
    }];
    store.commit(&delete).await.unwrap();
    assert!(store.snapshot().await.unwrap().projections.is_empty());
}

#[tokio::test]
async fn arbitrary_identifiers_and_invalid_batches_never_change_state() {
    let fixture = test_support::TestFixture::start().await;
    let store = fixture.initialize().await;
    let mut valid = batch("x'); DELETE ls_meta; --", 0);
    valid.projections[0] = ProjectionMutation::Put {
        key: "复杂 / key:[]{}'".into(),
        payload: json!({"text": "NONE", "number": 9007199254740991_u64, "uint64": u64::MAX, "int64": i64::MIN}),
    };
    store.commit(&valid).await.unwrap();
    let before = store.snapshot().await.unwrap();
    assert_eq!(before.projections[0].key, "复杂 / key:[]{}'");
    assert_eq!(
        before.projections[0].payload["uint64"].as_u64(),
        Some(u64::MAX)
    );
    assert_eq!(
        before.projections[0].payload["int64"].as_i64(),
        Some(i64::MIN)
    );
    let mut invalid = batch("duplicate", 1);
    invalid.events.push(invalid.events[0].clone());
    assert!(matches!(
        store.commit(&invalid).await,
        Err(Error::Invalid(_))
    ));
    invalid.events.pop();
    invalid.request_digest = "not-a-hash".into();
    assert!(matches!(
        store.commit(&invalid).await,
        Err(Error::Invalid(_))
    ));
    invalid.request_digest = "a".repeat(64);
    invalid.command_id = "nul\0id".into();
    assert!(matches!(
        store.commit(&invalid).await,
        Err(Error::Invalid(_))
    ));
    assert_eq!(store.snapshot().await.unwrap(), before);
}

#[tokio::test]
async fn oversized_snapshot_and_event_page_fail_without_truncating() {
    let fixture = test_support::TestFixture::start().await;
    let store = fixture.initialize().await;
    for index in 0..2 {
        let mut command = batch(&format!("large-{index}"), index * 2);
        let payload = json!({"text": "x".repeat(9 * 1024 * 1024)});
        command.events[0].payload = payload.clone();
        command.projections.clear();
        command.effects.clear();
        // Each commit stays below the 16 MiB batch limit; the later page exceeds it.
        store.commit(&command).await.unwrap();
        let mut projection = batch(&format!("projection-{index}"), index * 2 + 1);
        projection.events.clear();
        projection.effects.clear();
        projection.projections = vec![ProjectionMutation::Put {
            key: format!("large:{index}"),
            payload,
        }];
        store.commit(&projection).await.unwrap();
    }
    assert!(matches!(store.snapshot().await, Err(Error::Invalid(_))));
    let mut project = batch("bounded-project", 4);
    project.events.clear();
    project.effects.clear();
    project.projections = vec![ProjectionMutation::Put {
        key: "project/scoped".into(),
        payload: json!({"projectId": "scoped"}),
    }];
    store.commit(&project).await.unwrap();
    let scoped = store.project_snapshot().await.unwrap();
    assert_eq!(scoped.revision, 5);
    assert_eq!(scoped.last_sequence, 2);
    assert_eq!(scoped.projections.len(), 1);
    assert_eq!(scoped.projections[0].key, "project/scoped");
    assert!(matches!(store.snapshot().await, Err(Error::Invalid(_))));
    assert!(matches!(store.events(0, 2).await, Err(Error::Invalid(_))));
    assert_eq!(store.events(0, 1).await.unwrap().len(), 1);
    assert_eq!(store.events(1, 1).await.unwrap().len(), 1);
}

#[tokio::test]
async fn acknowledged_batch_survives_process_kill_and_reopens() {
    let mut fixture = test_support::TestFixture::start().await;
    fixture.initialize().await.shutdown().await.unwrap();
    let executable = env!("CARGO_BIN_EXE_t3-code-ls-db");
    let mut child = Command::new(executable)
        .arg("local")
        .arg(fixture.path())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut stdin = child.stdin.take().unwrap();
    let command = batch("durable", 0);
    serde_json::to_writer(&mut stdin, &json!({"operation":"commit", "batch":command})).unwrap();
    writeln!(stdin).unwrap();
    stdin.flush().unwrap();
    let mut stdout = BufReader::new(child.stdout.take().unwrap());
    let mut line = String::new();
    stdout.read_line(&mut line).unwrap();
    let acknowledgement: Value = serde_json::from_str(&line).unwrap();
    assert_eq!(acknowledgement["ok"], true, "{acknowledgement}");
    child.kill().unwrap();
    child.wait().unwrap();
    fixture.restart().await;
    let mut reader = Command::new(executable)
        .arg("local")
        .arg(fixture.path())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut input = reader.stdin.take().unwrap();
    writeln!(input, "{{\"operation\":\"snapshot\"}}").unwrap();
    writeln!(
        input,
        "{{\"operation\":\"receipt\",\"command_id\":\"durable\"}}"
    )
    .unwrap();
    drop(input);
    let output = reader.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let lines: Vec<Value> = String::from_utf8(output.stdout)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(lines[0]["result"]["revision"], 1);
    assert_eq!(lines[0]["result"]["last_sequence"], 1);
    assert_eq!(
        lines[0]["result"]["projections"][0]["payload"]["title"],
        "durable"
    );
    assert_eq!(lines[1]["result"], acknowledgement["result"]["receipt"]);
}
