use super::*;
use crate::application::{ApplicationSchema, ApplicationTable};
use crate::test_support::TestFixture;

fn schema() -> ApplicationSchema {
    ApplicationSchema {
        version: 1,
        tables: vec![
            ApplicationTable {
                name: EVENTS.into(),
                columns: COLUMNS.iter().map(|value| (*value).into()).collect(),
                primary_key: vec!["sequence".into()],
            },
            ApplicationTable {
                name: METADATA.into(),
                columns: [
                    "projection_name",
                    "schema_version",
                    "last_sequence",
                    "updated_at",
                ]
                .map(str::to_owned)
                .to_vec(),
                primary_key: vec!["projection_name".into()],
            },
        ],
    }
}

fn row(id: &str, stream: &str) -> EventAppendRow {
    EventAppendRow {
        event_id: id.into(),
        aggregate_kind: "thread".into(),
        stream_id: stream.into(),
        event_type: "thread.created".into(),
        occurred_at: "2026-11-10T00:00:00.000Z".into(),
        command_id: Some("command".into()),
        causation_event_id: None,
        correlation_id: Some("command".into()),
        actor_kind: "server".into(),
        payload_json: "{\"title\":\"hello\"}".into(),
        metadata_json: "{}".into(),
        application_event_version: 2,
    }
}

async fn initialize(store: &Store) -> ApplicationTransactions {
    let mut transactions = ApplicationTransactions::new(store.clone());
    transactions.initialize(schema()).await.unwrap();
    transactions
}

async fn prepare(store: &Store) -> PreparationResult {
    loop {
        let result = store.prepare_application_events().await.unwrap();
        if result.prepared {
            return result;
        }
    }
}

fn read_all() -> EventRead {
    EventRead {
        after_sequence: 0,
        through_sequence: None,
        thread_id: None,
        command_id: None,
        event_type: None,
        only_agent_events: None,
        limit: 4096,
    }
}

fn stats(
    kind: &str,
    thread: Option<&str>,
    after: u64,
    through: u64,
    max: Option<u64>,
) -> EventStats {
    EventStats {
        kind: kind.into(),
        after_sequence: after,
        through_sequence: through,
        thread_id: thread.map(str::to_owned),
        max_events: max,
    }
}

#[tokio::test]
async fn event_append_joins_savepoint_rollback_and_preserves_typed_row_identity() {
    let fixture = TestFixture::start().await;
    let store = fixture.initialize().await;
    let mut app = initialize(&store).await;
    prepare(&store).await;
    app.begin("write").await.unwrap();
    assert_eq!(
        app.events_append("write", row("one", "thread"), None)
            .await
            .unwrap()
            .sequence,
        1
    );
    app.savepoint("write", "inner").await.unwrap();
    assert!(matches!(
        app.events_append("write", row("one", "thread"), None).await,
        Err(Error::DuplicateRecord)
    ));
    assert!(app.events_read("write", read_all()).await.is_err());
    app.rollback_to("write", "inner").await.unwrap();
    app.release("write", "inner").await.unwrap();
    assert_eq!(
        app.events_append("write", row("two", "thread"), None)
            .await
            .unwrap()
            .sequence,
        2
    );
    assert_eq!(
        app.events_stats("write", stats("application", None, 0, MAX_SEQUENCE, None))
            .await
            .unwrap()
            .event_count,
        2
    );
    app.commit("write").await.unwrap();
    app.begin("read").await.unwrap();
    let stored = app
        .get("read", EVENTS, &[int_cell(2)])
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored["event_id"], text_cell("two"));
    assert_eq!(stored["stream_version"], int_cell(1));
    app.rollback("read").await.unwrap();
    app.begin("bypass").await.unwrap();
    assert!(matches!(
        app.delete("bypass", EVENTS, &[int_cell(1)]).await,
        Err(Error::Invalid(_))
    ));
    assert!(app.commit("bypass").await.is_err());
}

#[tokio::test]
async fn preparation_resumes_after_reopen_and_does_not_rebuild_live_events() {
    let fixture = TestFixture::start().await;
    let store = fixture.initialize().await;
    let mut app = initialize(&store).await;
    app.begin("seed").await.unwrap();
    for sequence in 1..=70 {
        let event = EventRecord {
            sequence,
            stream_version: (sequence - 1) as i64,
            row: row(&format!("seed-{sequence}"), "thread"),
        }
        .stored();
        app.put("seed", EVENTS, event.key, event.row).await.unwrap();
    }
    app.commit("seed").await.unwrap();
    let first = store.prepare_application_events().await.unwrap();
    assert!(!first.prepared);
    assert_eq!(first.indexed_rows, PREPARE_ROWS as u64);
    app.begin("unready").await.unwrap();
    assert!(app.events_read("unready", read_all()).await.is_err());
    drop(app);
    store.shutdown().await.unwrap();
    let reopened = fixture.connect().await;
    assert_eq!(prepare(&reopened).await.indexed_rows, 70);
    let mut app = ApplicationTransactions::new(reopened.clone());
    app.begin("live").await.unwrap();
    assert_eq!(
        app.events_append("live", row("live", "thread"), None)
            .await
            .unwrap()
            .sequence,
        71
    );
    app.commit("live").await.unwrap();
    assert_eq!(prepare(&reopened).await.indexed_rows, 70);
    app.begin("verify").await.unwrap();
    assert_eq!(
        app.events_read("verify", read_all()).await.unwrap().len(),
        71
    );
    assert_eq!(
        app.events_stats("verify", stats("application", None, 0, MAX_SEQUENCE, None))
            .await
            .unwrap()
            .event_count,
        71
    );
    app.rollback("verify").await.unwrap();
}

#[tokio::test]
async fn sparse_replay_statistics_respect_ranges_limits_and_malformed_ordinary_payloads() {
    let fixture = TestFixture::start().await;
    let store = fixture.initialize().await;
    let mut app = initialize(&store).await;
    app.begin("seed").await.unwrap();
    let sequences = [1, 255, 256, 1024, 1_000_000, MAX_SEQUENCE - 1];
    let mut payload_bytes = Vec::new();
    for (ordinal, sequence) in sequences.into_iter().enumerate() {
        let mut input = row(&format!("sparse-{ordinal}"), "thread");
        input.event_type = if ordinal == 1 {
            "thread.created"
        } else {
            "thread.metadata-updated"
        }
        .into();
        input.payload_json = format!("{{malformed:{}é", "x".repeat(ordinal * 13));
        payload_bytes.push(input.payload_json.len() as u64);
        let event = EventRecord {
            sequence,
            stream_version: ordinal as i64,
            row: input,
        }
        .stored();
        app.put("seed", EVENTS, event.key, event.row).await.unwrap();
    }
    app.commit("seed").await.unwrap();
    prepare(&store).await;
    app.begin("stats").await.unwrap();
    for (after, through, max) in [
        (0, MAX_SEQUENCE, 0),
        (0, MAX_SEQUENCE, 3),
        (1, 1_000_000, 1),
        (255, MAX_SEQUENCE, 100),
        (256, 1024, 0),
    ] {
        let expected: Vec<_> = sequences
            .iter()
            .enumerate()
            .filter(|(_, sequence)| **sequence > after && **sequence <= through)
            .take(max as usize + 1)
            .collect();
        let actual = app
            .events_stats(
                "stats",
                stats("agent", Some("thread"), after, through, Some(max)),
            )
            .await
            .unwrap();
        assert_eq!(
            actual,
            StatsResult {
                event_count: expected.len() as u64,
                raw_payload_bytes: expected
                    .iter()
                    .map(|(index, _)| payload_bytes[*index])
                    .sum(),
                has_create_event: expected.iter().any(|(index, _)| *index == 1),
            }
        );
    }
    assert_eq!(
        app.events_stats("stats", stats("application", None, 0, MAX_SEQUENCE, None))
            .await
            .unwrap()
            .raw_payload_bytes,
        payload_bytes.iter().sum::<u64>()
    );
    app.rollback("stats").await.unwrap();
}

#[tokio::test]
async fn all_filter_combinations_return_sequence_order_without_unrelated_rows() {
    let fixture = TestFixture::start().await;
    let store = fixture.initialize().await;
    let mut app = initialize(&store).await;
    prepare(&store).await;
    app.begin("write").await.unwrap();
    let mut expected = Vec::new();
    for index in 0..24 {
        let mut input = row(
            &format!("event-{index}"),
            if index % 2 == 0 { "a" } else { "b" },
        );
        input.command_id = Some(if index % 3 == 0 { "x" } else { "y" }.into());
        input.event_type = if index % 4 == 0 {
            "thread.created"
        } else {
            "thread.metadata-updated"
        }
        .into();
        expected.push(input.clone());
        app.events_append("write", input, None).await.unwrap();
    }
    app.commit("write").await.unwrap();
    app.begin("query").await.unwrap();
    for mask in 0..8 {
        let mut query = read_all();
        query.only_agent_events = Some(true);
        query.thread_id = (mask & 1 != 0).then(|| "a".into());
        query.command_id = (mask & 2 != 0).then(|| "x".into());
        query.event_type = (mask & 4 != 0).then(|| "thread.created".into());
        let rows = app.events_read("query", query.clone()).await.unwrap();
        let expected_ids: Vec<_> = expected
            .iter()
            .filter(|row| {
                query
                    .thread_id
                    .as_ref()
                    .is_none_or(|value| value == &row.stream_id)
                    && query
                        .command_id
                        .as_ref()
                        .is_none_or(|value| Some(value) == row.command_id.as_ref())
                    && query
                        .event_type
                        .as_ref()
                        .is_none_or(|value| value == &row.event_type)
            })
            .map(|row| row.event_id.as_str())
            .collect();
        assert_eq!(
            rows.iter()
                .map(|row| row.event_id.as_str())
                .collect::<Vec<_>>(),
            expected_ids
        );
    }
    app.rollback("query").await.unwrap();
}

#[tokio::test]
async fn prompt_restoration_preserves_unrelated_json_lexemes_and_byte_budgets() {
    let fixture = TestFixture::start().await;
    let store = fixture.initialize().await;
    let mut app = initialize(&store).await;
    prepare(&store).await;
    app.begin("write").await.unwrap();
    let payload = r#"{"id":"sub","prompt":"héllo\nworld","number":1e+21,"encoded":"\u0061"}"#;
    let mut anchor = row("anchor", "thread");
    anchor.event_type = "subagent.updated".into();
    anchor.payload_json = payload.into();
    app.events_append("write", anchor.clone(), None)
        .await
        .unwrap();
    anchor.event_id = "compact".into();
    let compact = app.events_append("write", anchor, Some(1)).await.unwrap();
    assert!(!compact.stores_full_prompt);
    let rows = app.events_read("write", read_all()).await.unwrap();
    assert_eq!(rows[0].payload_json, payload);
    assert_eq!(rows[1].payload_json, payload);
    assert_eq!(
        app.events_stats("write", stats("application", None, 0, MAX_SEQUENCE, None))
            .await
            .unwrap()
            .raw_payload_bytes,
        payload.len() as u64 * 2
    );
    app.commit("write").await.unwrap();
}

#[tokio::test]
async fn event_preparation_rejects_encoded_text_without_changing_imported_values() {
    for column in ["event_id", "stream_id", "payload_json"] {
        let fixture = TestFixture::start().await;
        let store = fixture.initialize().await;
        let mut app = initialize(&store).await;
        let mut stored = EventRecord {
            sequence: 1,
            stream_version: 0,
            row: row("one", "thread"),
        }
        .stored();
        let encoded_text = json!({"type":"text","encoding":"base64","value":"gA=="});
        stored.row.insert(column.into(), encoded_text.clone());
        app.begin("seed").await.unwrap();
        app.put("seed", EVENTS, stored.key, stored.row)
            .await
            .unwrap();
        app.commit("seed").await.unwrap();
        assert!(matches!(
            store.prepare_application_events().await,
            Err(Error::IncompatibleStore)
        ));
        app.begin("read-original").await.unwrap();
        assert_eq!(
            app.get("read-original", EVENTS, &[int_cell(1)])
                .await
                .unwrap()
                .unwrap()[column],
            encoded_text
        );
        assert!(app.events_read("read-original", read_all()).await.is_err());
    }
}

#[tokio::test]
async fn index_preparation_and_statistics_do_not_require_a_whole_history_snapshot() {
    let fixture = TestFixture::start().await;
    let store = fixture.initialize().await;
    let mut app = initialize(&store).await;
    let payload = "x".repeat(800_000);
    for chunk in 0..3 {
        app.begin("seed").await.unwrap();
        for offset in 1..=8 {
            let sequence = chunk * 8 + offset;
            let mut input = row(&format!("large-{sequence}"), "thread");
            input.event_type = "thread.metadata-updated".into();
            input.payload_json = payload.clone();
            let stored = EventRecord {
                sequence,
                stream_version: (sequence - 1) as i64,
                row: input,
            }
            .stored();
            app.put("seed", EVENTS, stored.key, stored.row)
                .await
                .unwrap();
        }
        app.commit("seed").await.unwrap();
    }
    assert!(payload.len() * 24 > MAX_BATCH_BYTES);
    assert_eq!(prepare(&store).await.indexed_rows, 24);
    app.begin("stats").await.unwrap();
    let actual = app
        .events_stats("stats", stats("application", None, 0, MAX_SEQUENCE, None))
        .await
        .unwrap();
    assert_eq!(actual.event_count, 24);
    assert_eq!(actual.raw_payload_bytes, payload.len() as u64 * 24);
    assert_eq!(
        app.events_stats("stats", stats("agent", Some("thread"), 5, 23, Some(1)))
            .await
            .unwrap()
            .raw_payload_bytes,
        payload.len() as u64 * 2
    );
    app.rollback("stats").await.unwrap();
}

#[tokio::test]
async fn restored_non_string_prompts_keep_sqlite_types_or_fail_without_coercing_to_text() {
    for (value, expected) in [
        ("[1, 2]", "[1,2]"),
        ("{\"x\": 1}", "{\"x\":1}"),
        ("true", "1"),
        ("false", "0"),
        ("null", "null"),
        ("-17", "-17"),
    ] {
        let fixture = TestFixture::start().await;
        let store = fixture.initialize().await;
        let mut app = initialize(&store).await;
        app.begin("seed").await.unwrap();
        for (sequence, payload) in [
            (1, format!("{{\"prompt\":{value}}}")),
            (2, "{\"prompt\":\"\",\"promptSequence\":1}".into()),
        ] {
            let mut input = row(&format!("prompt-{sequence}"), "thread");
            input.event_type = "subagent.updated".into();
            input.payload_json = payload;
            let event = EventRecord {
                sequence,
                stream_version: (sequence - 1) as i64,
                row: input,
            }
            .stored();
            app.put("seed", EVENTS, event.key, event.row).await.unwrap();
        }
        app.commit("seed").await.unwrap();
        prepare(&store).await;
        app.begin("read").await.unwrap();
        let rows = app.events_read("read", read_all()).await.unwrap();
        assert_eq!(rows[1].payload_json, format!("{{\"prompt\":{expected}}}"));
        app.rollback("read").await.unwrap();
    }
    let fixture = TestFixture::start().await;
    let store = fixture.initialize().await;
    let mut app = initialize(&store).await;
    app.begin("real-prompt").await.unwrap();
    for (sequence, payload) in [(1, "{\"prompt\":1e+21}"), (2, "{\"promptSequence\":1}")] {
        let mut input = row(&format!("real-{sequence}"), "thread");
        input.event_type = "subagent.updated".into();
        input.payload_json = payload.into();
        let stored = EventRecord {
            sequence,
            stream_version: (sequence - 1) as i64,
            row: input,
        }
        .stored();
        app.put("real-prompt", EVENTS, stored.key, stored.row)
            .await
            .unwrap();
    }
    app.commit("real-prompt").await.unwrap();
    assert!(matches!(
        store.prepare_application_events().await,
        Err(Error::IncompatibleStore)
    ));
}
