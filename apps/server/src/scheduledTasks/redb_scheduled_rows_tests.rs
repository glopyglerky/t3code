use super::*;
use crate::application::{ApplicationSchema, ApplicationTable};
use crate::test_support::TestFixture;

fn schema() -> ApplicationSchema {
    ApplicationSchema {
        version: 1,
        tables: TABLES
            .iter()
            .map(|name| ApplicationTable {
                name: (*name).into(),
                columns: columns(name).unwrap().iter().map(|v| (*v).into()).collect(),
                primary_key: primary_columns(name).iter().map(|v| (*v).into()).collect(),
            })
            .collect(),
    }
}
fn task(id: &str, created: &str, next: Option<&str>) -> Row {
    json!({"task_id":id,"title":"Task","prompt":"hello","enabled":1,"schedule_json":"{\"type\":\"interval\",\"seconds\":60}",
        "project_id":"project","thread_id":null,"thread_mode":"continue","workspace_strategy_json":"{}","model_selection_json":"{}",
        "runtime_mode":"local","interaction_mode":"default","created_by":"owner","creation_source":"user","created_at":created,"updated_at":created,
        "next_run_at":next,"last_run_at":null,"last_run_status":"never","last_run_error":null,"run_count":0,"webhook_token":"token-1",
        "webhook_secret":"secret-1","last_run_webhook_delivery_id":null}).as_object().unwrap().clone()
}
fn pending(id: &str, task: &str, coalesce: &str, due: i64) -> Row {
    json!({"delivery_id":id,"task_id":task,"coalesce_key":coalesce,"due_at":due,"state":"pending",
        "task_json":"{}","request_json":"{}","delivery_ids_json":format!("[\"{id}\"]")})
    .as_object()
    .unwrap()
    .clone()
}
fn delivery(id: &str, task: &str, outcome: &str) -> Row {
    json!({"delivery_id":id,"task_id":task,"received_at":"2026-11-10T10:00:00Z","method":"POST","query":"",
        "headers_json":"{}","body":"payload","body_bytes":7,"body_truncated":0,"outcome":outcome,"signature_verified":1,
        "missing_fields_json":"[]","rendered_prompt":null,"error":null}).as_object().unwrap().clone()
}
async fn setup(store: &Store) -> ApplicationTransactions {
    let mut app = ApplicationTransactions::new(store.clone());
    app.initialize(schema()).await.unwrap();
    while !store
        .prepare_application_scheduled()
        .await
        .unwrap()
        .prepared
    {}
    app
}
async fn save(app: &mut ApplicationTransactions, id: &str, row: Row) {
    app.scheduled_write(
        id,
        ScheduledWrite::SaveTask {
            row,
            require_existing: false,
            kept_schedule_json: None,
            secret_changed: true,
        },
    )
    .await
    .unwrap();
}
async fn query(app: &mut ApplicationTransactions, id: &str, input: Value) -> Value {
    app.scheduled_read(id, serde_json::from_value(input).unwrap())
        .await
        .unwrap()
}
async fn mutate(app: &mut ApplicationTransactions, id: &str, input: Value) -> Value {
    app.scheduled_write(id, serde_json::from_value(input).unwrap())
        .await
        .unwrap()
}

#[tokio::test]
async fn task_order_cas_and_concurrent_settings_preserve_run_state_and_rotated_token() {
    let fixture = TestFixture::start().await;
    let store = fixture.initialize().await;
    let mut app = setup(&store).await;
    app.begin("write").await.unwrap();
    save(
        &mut app,
        "write",
        task("task-b", "2026-11-02", Some("2026-11-12")),
    )
    .await;
    save(
        &mut app,
        "write",
        task("task-a", "2026-11-02", Some("2026-11-11")),
    )
    .await;
    save(
        &mut app,
        "write",
        task("task-c", "2026-11-01", Some("2026-11-10")),
    )
    .await;
    let all = query(&mut app, "write", json!({"kind":"tasks"})).await;
    assert_eq!(
        all.as_array()
            .unwrap()
            .iter()
            .map(|r| r["task_id"].as_str().unwrap())
            .collect::<Vec<_>>(),
        ["task-a", "task-b", "task-c"]
    );
    let due = query(
        &mut app,
        "write",
        json!({"kind":"tasks","due_before":"2026-11-11"}),
    )
    .await;
    assert_eq!(
        due.as_array()
            .unwrap()
            .iter()
            .map(|r| r["task_id"].as_str().unwrap())
            .collect::<Vec<_>>(),
        ["task-c", "task-a"]
    );
    mutate(&mut app,"write",json!({"kind":"patch_task","task_id":"task-a","expected":{},"different":{},
        "patch":{"last_run_at":"start","last_run_status":"running","webhook_token":"token-2","thread_id":"thread"},"increment_run_count":false})).await;
    assert_eq!(
        query(&mut app, "write", json!({"kind":"next_due"})).await,
        json!([{"next_run_at":"2026-11-10"}])
    );
    let mut edit = task("task-a", "stale-created", Some("stale-next"));
    edit.insert("title".into(), json!("edited"));
    app.scheduled_write(
        "write",
        ScheduledWrite::SaveTask {
            kept_schedule_json: Some(edit["schedule_json"].as_str().unwrap().into()),
            row: edit,
            require_existing: true,
            secret_changed: false,
        },
    )
    .await
    .unwrap();
    let saved = query(
        &mut app,
        "write",
        json!({"kind":"tasks","task_id":"task-a"}),
    )
    .await;
    assert_eq!(saved[0]["last_run_status"], "running");
    assert_eq!(saved[0]["last_run_at"], "start");
    assert_eq!(saved[0]["webhook_token"], "token-2");
    assert_eq!(saved[0]["thread_id"], "thread");
    assert_eq!(saved[0]["created_at"], "2026-11-02");
    assert_eq!(saved[0]["next_run_at"], "2026-11-11");
    let miss=mutate(&mut app,"write",json!({"kind":"patch_task","task_id":"task-a","expected":{"last_run_at":"other","last_run_status":"running"},"different":{},"patch":{"last_run_status":"succeeded"},"increment_run_count":true})).await;
    assert_eq!(miss, json!([]));
    mutate(&mut app,"write",json!({"kind":"patch_task","task_id":"task-a","expected":{"last_run_at":"start","last_run_status":"running"},"different":{},"patch":{"last_run_status":"succeeded","next_run_at":"2026-11-15"},"increment_run_count":true})).await;
    assert_eq!(
        query(
            &mut app,
            "write",
            json!({"kind":"tasks","task_id":"task-a"})
        )
        .await[0]["run_count"],
        1
    );
    app.commit("write").await.unwrap();
}

#[tokio::test]
async fn replay_and_pending_windows_keep_unique_claims_expiry_and_atomic_rollbacks() {
    let fixture = TestFixture::start().await;
    let store = fixture.initialize().await;
    let mut app = setup(&store).await;
    app.begin("seed").await.unwrap();
    save(&mut app, "seed", task("task", "created", None)).await;
    app.commit("seed").await.unwrap();
    app.begin("claim").await.unwrap();
    mutate(&mut app,"claim",json!({"kind":"claim_replay","task_id":"task","replay_key":"key","delivery_id":"delivery-a","now":10,"expires_at":30})).await;
    app.scheduled_write(
        "claim",
        ScheduledWrite::InsertPending {
            row: pending("delivery-a", "task", "group", 20),
        },
    )
    .await
    .unwrap();
    assert_eq!(
        query(
            &mut app,
            "claim",
            json!({"kind":"has_replay","task_id":"task","replay_key":"key","now":29})
        )
        .await
        .as_array()
        .unwrap()
        .len(),
        1
    );
    assert_eq!(
        query(
            &mut app,
            "claim",
            json!({"kind":"has_replay","task_id":"task","replay_key":"key","now":30})
        )
        .await,
        json!([])
    );
    app.commit("claim").await.unwrap();
    app.begin("duplicate").await.unwrap();
    assert!(matches!(
        app.scheduled_write(
            "duplicate",
            ScheduledWrite::InsertPending {
                row: pending("delivery-b", "task", "group", 40)
            }
        )
        .await,
        Err(Error::DuplicateRecord)
    ));
    app.begin("verify").await.unwrap();
    assert_eq!(
        query(
            &mut app,
            "verify",
            json!({"kind":"pending_count","task_id":"task"})
        )
        .await,
        json!([{"count":1}])
    );
    mutate(
        &mut app,
        "verify",
        json!({"kind":"expire_pending","now":20}),
    )
    .await;
    app.scheduled_write(
        "verify",
        ScheduledWrite::InsertPending {
            row: pending("delivery-b", "task", "group", 40),
        },
    )
    .await
    .unwrap();
    mutate(&mut app,"verify",json!({"kind":"patch_pending","delivery_id":"delivery-b","patch":{"request_json":"{\"new\":true}"},"append_delivery_id":"delivery-c"})).await;
    let pending = query(
        &mut app,
        "verify",
        json!({"kind":"pending","task_id":"task","states":["pending"],"first":false}),
    )
    .await;
    assert_eq!(
        pending[0]["delivery_ids_json"],
        "[\"delivery-b\",\"delivery-c\"]"
    );
    assert_eq!(
        query(&mut app, "verify", json!({"kind":"active_pending_tasks"})).await,
        json!([{"task_id":"task"}])
    );
    mutate(
        &mut app,
        "verify",
        json!({"kind":"delete_tasks","task_id":"task"}),
    )
    .await;
    assert_eq!(
        query(
            &mut app,
            "verify",
            json!({"kind":"pending_count","task_id":"task"})
        )
        .await,
        json!([{"count":0}])
    );
    assert_eq!(
        query(
            &mut app,
            "verify",
            json!({"kind":"has_replay","task_id":"task","replay_key":"key","now":10})
        )
        .await,
        json!([])
    );
    app.rollback("verify").await.unwrap();
    app.begin("reopen").await.unwrap();
    assert_eq!(
        query(
            &mut app,
            "reopen",
            json!({"kind":"pending_count","task_id":"task"})
        )
        .await,
        json!([{"count":1}])
    );
    app.rollback("reopen").await.unwrap();
}

#[tokio::test]
async fn delivery_retention_uses_insertion_order_at_equal_times_and_separate_buckets() {
    let fixture = TestFixture::start().await;
    let store = fixture.initialize().await;
    let mut app = setup(&store).await;
    app.begin("write").await.unwrap();
    save(&mut app, "write", task("task", "created", None)).await;
    for id in ["z", "a", "m"] {
        app.scheduled_write(
            "write",
            ScheduledWrite::RecordDelivery {
                row: delivery(id, "task", "accepted"),
            },
        )
        .await
        .unwrap();
    }
    app.scheduled_write(
        "write",
        ScheduledWrite::RecordDelivery {
            row: delivery("filtered", "task", "filtered"),
        },
    )
    .await
    .unwrap();
    mutate(
        &mut app,
        "write",
        json!({"kind":"prune_deliveries","task_id":"task","bucket":"main","limit":2}),
    )
    .await;
    let rows = query(
        &mut app,
        "write",
        json!({"kind":"deliveries","task_id":"task"}),
    )
    .await;
    assert_eq!(
        rows.as_array()
            .unwrap()
            .iter()
            .map(|r| r["delivery_id"].as_str().unwrap())
            .collect::<Vec<_>>(),
        ["filtered", "m", "a"]
    );
    // An upsert changes outcomes but retains request fields and insertion ordering.
    let mut replace = delivery("a", "task", "duplicate");
    replace.insert("body".into(), json!("must-not-replace"));
    app.scheduled_write("write", ScheduledWrite::RecordDelivery { row: replace })
        .await
        .unwrap();
    assert_eq!(
        query(
            &mut app,
            "write",
            json!({"kind":"deliveries","task_id":"task","delivery_id":"a"})
        )
        .await[0]["body"],
        "payload"
    );
    app.commit("write").await.unwrap();
}

#[tokio::test]
async fn pending_recovery_counts_only_the_delivery_that_owned_the_running_task() {
    let fixture = TestFixture::start().await;
    let store = fixture.initialize().await;
    let mut app = setup(&store).await;
    app.begin("write").await.unwrap();
    let mut row = task("task", "created", None);
    row.insert("last_run_status".into(), json!("running"));
    save(&mut app, "write", row).await;
    mutate(&mut app,"write",json!({"kind":"patch_task","task_id":"task","expected":{},"different":{},"patch":{"last_run_webhook_delivery_id":"owner"},"increment_run_count":false})).await;
    let mut pending = pending("owner", "task", "group", 20);
    pending.insert("state".into(), json!("running"));
    app.scheduled_write("write", ScheduledWrite::InsertPending { row: pending })
        .await
        .unwrap();
    mutate(&mut app,"write",json!({"kind":"recover_task","task_id":"task","patch":{"last_run_status":"failed"},"match_pending_task":true})).await;
    assert_eq!(
        query(&mut app, "write", json!({"kind":"tasks","task_id":"task"})).await[0]["run_count"],
        0
    );
    mutate(&mut app,"write",json!({"kind":"patch_task","task_id":"task","expected":{},"different":{},"patch":{"last_run_status":"running","last_run_webhook_delivery_id":"other"},"increment_run_count":false})).await;
    mutate(&mut app,"write",json!({"kind":"recover_task","task_id":"task","patch":{"last_run_status":"failed"},"match_pending_task":true})).await;
    assert_eq!(
        query(&mut app, "write", json!({"kind":"tasks","task_id":"task"})).await[0]["run_count"],
        1
    );
    app.rollback("write").await.unwrap();
}
