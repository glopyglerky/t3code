use super::*;
use crate::application::{ApplicationSchema, ApplicationTable, ApplicationTransactionMode};
use crate::test_support::TestFixture;

fn schema() -> ApplicationSchema {
    ApplicationSchema {
        version: 1,
        tables: vec![ApplicationTable {
            name: PROJECTS.into(),
            columns: COLUMNS.iter().map(|value| (*value).into()).collect(),
            primary_key: vec!["project_id".into()],
        }],
    }
}

fn project(id: &str, created: &str, workspace: &str) -> ProjectRecord {
    ProjectRecord {
        project_id: id.into(),
        title: format!("Project {id} 🦊"),
        has_custom_title: 0,
        workspace_root: workspace.into(),
        default_model_selection: Some("{ \"provider\": \"codex\", \"model\": \"test\" }".into()),
        default_thread_env_mode: Some("local".into()),
        auto_pull: 1,
        favicon_path: Some(String::new()),
        project_icon: Some("{\"type\":\"emoji\",\"emoji\":\"🦊\"}".into()),
        scripts: "[]".into(),
        pinned_at: None,
        archived_at: None,
        created_at: created.into(),
        updated_at: "2026-11-10T12:00:00.000Z".into(),
        deleted_at: None,
    }
}

async fn initialize(store: &Store) -> ApplicationTransactions {
    let mut app = ApplicationTransactions::new(store.clone());
    app.initialize(schema()).await.unwrap();
    app
}

async fn prepare(store: &Store) -> PreparationResult {
    loop {
        let result = store.prepare_application_projects().await.unwrap();
        if result.prepared {
            return result;
        }
    }
}

fn ids(rows: Vec<ProjectRecord>) -> Vec<String> {
    rows.into_iter().map(|row| row.project_id).collect()
}

fn creation_event(id: &str) -> String {
    json!({"type":"project.created","payload":{
        "projectId":id,"title":"Created","workspaceRoot":"/fixture/reduced",
        "defaultModelSelection":null,"scripts":[],"createdAt":"date-text","updatedAt":"date-text"
    }})
    .to_string()
}

#[tokio::test]
async fn project_reduction_is_pure_and_requires_a_live_session() {
    let fixture = TestFixture::start().await;
    let store = fixture.initialize().await;
    let mut app = initialize(&store).await;
    prepare(&store).await;
    let event = creation_event("reduced");
    assert!(matches!(
        app.projects_reduce_event("missing", None, &event).await,
        Err(Error::Invalid(_))
    ));
    app.begin_mode("read", ApplicationTransactionMode::Read, None)
        .await
        .unwrap();
    let ProjectEventReduction::Reduced { row } = app
        .projects_reduce_event("read", None, &event)
        .await
        .unwrap()
    else {
        panic!("expected reduction")
    };
    assert_eq!(row["projectId"], "reduced");
    assert_eq!(row["hasCustomTitle"], false);
    assert_eq!(row["createdAt"], "date-text");
    assert!(
        app.projects_select("read", ProjectSelection::default())
            .await
            .unwrap()
            .is_empty()
    );
    let deleted = json!({"type":"project.deleted","payload":{"projectId":"missing"}}).to_string();
    assert_eq!(
        app.projects_reduce_event("read", None, &deleted)
            .await
            .unwrap(),
        ProjectEventReduction::Reduced { row: Value::Null }
    );
    app.commit("read").await.unwrap();
    assert!(matches!(
        app.projects_reduce_event("read", None, &event).await,
        Err(Error::Invalid(_))
    ));
    app.begin("cancelled").await.unwrap();
    app.projects_reduce_event("cancelled", None, &event)
        .await
        .unwrap();
    app.rollback("cancelled").await.unwrap();
    app.begin("verify").await.unwrap();
    assert!(
        app.projects_select("verify", ProjectSelection::default())
            .await
            .unwrap()
            .is_empty()
    );
    app.rollback("verify").await.unwrap();
}

#[tokio::test]
async fn project_domain_rejection_does_not_poison_writers_or_savepoints() {
    let fixture = TestFixture::start().await;
    let store = fixture.initialize().await;
    let mut app = initialize(&store).await;
    prepare(&store).await;
    app.begin("writer").await.unwrap();
    let before = project("before", "a", "/before");
    app.projects_upsert("writer", before.clone()).await.unwrap();
    let invalid = json!({"type":"project.created","payload":{"projectId":"invalid","title":false}})
        .to_string();
    assert_eq!(
        app.projects_reduce_event("writer", None, &invalid)
            .await
            .unwrap(),
        ProjectEventReduction::InvalidEvent
    );
    assert_eq!(
        app.projects_reduce_event("writer", Some("{"), &creation_event("invalid"))
            .await
            .unwrap(),
        ProjectEventReduction::InvalidEvent
    );
    app.savepoint("writer", "caught").await.unwrap();
    assert_eq!(
        app.projects_reduce_event("writer", None, "{")
            .await
            .unwrap(),
        ProjectEventReduction::InvalidEvent
    );
    let after = project("after", "b", "/after");
    app.projects_upsert("writer", after.clone()).await.unwrap();
    app.release("writer", "caught").await.unwrap();
    app.savepoint("writer", "discarded").await.unwrap();
    app.projects_reduce_event("writer", None, &creation_event("not-written"))
        .await
        .unwrap();
    app.projects_upsert("writer", project("discarded", "c", "/discarded"))
        .await
        .unwrap();
    app.rollback_to("writer", "discarded").await.unwrap();
    app.release("writer", "discarded").await.unwrap();
    app.commit("writer").await.unwrap();
    app.begin_mode("verify", ApplicationTransactionMode::Read, None)
        .await
        .unwrap();
    assert_eq!(
        app.projects_select("verify", ProjectSelection::default())
            .await
            .unwrap(),
        vec![before, after]
    );
    app.rollback("verify").await.unwrap();
}

#[test]
fn project_reduction_bounds_invalid_input_without_normalizing_rows() {
    let too_large = " ".repeat(crate::MAX_BATCH_BYTES + 1);
    assert_eq!(
        reduce_event(None, &too_large).unwrap(),
        ProjectEventReduction::InvalidEvent
    );
    let too_deep = format!("{}0{}", "[".repeat(65), "]".repeat(65));
    assert_eq!(
        reduce_event(None, &too_deep).unwrap(),
        ProjectEventReduction::InvalidEvent
    );
    let mut event: Value = serde_json::from_str(&creation_event("reduced")).unwrap();
    event["payload"]["defaultThreadEnvMode"] = json!("unknown-mode");
    event["payload"]["projectIcon"] =
        json!({"kind":"lucide","name":" folder ","color":"blue","monogramText":"AB"});
    let ProjectEventReduction::Reduced { row } = reduce_event(None, &event.to_string()).unwrap()
    else {
        panic!("expected raw row")
    };
    // Shared application codecs, rather than this pure reducer, decide whether
    // these values are accepted. Early normalization would change that decision.
    assert_eq!(row["defaultThreadEnvMode"], "unknown-mode");
    assert_eq!(row["projectIcon"]["name"], " folder ");
    assert_eq!(row["projectIcon"]["monogramText"], "AB");
}

#[tokio::test]
async fn project_selection_matches_ordered_filters_and_preserves_encoded_values() {
    let fixture = TestFixture::start().await;
    let store = fixture.initialize().await;
    let mut app = initialize(&store).await;
    prepare(&store).await;
    app.begin("write").await.unwrap();
    let a = project("a", "2026-11-01T00:00:00.000Z", "/fixture/exact");
    let b = project("b", "2026-11-01T00:00:00.000Z", "/fixture/exact");
    let c = project("c", "2026-10-01T00:00:00.000Z", "/fixture/exact/");
    let gone = ProjectRecord {
        deleted_at: Some("2026-11-09T00:00:00.000Z".into()),
        ..project("gone", "2026-01-01T00:00:00.000Z", "/fixture/exact")
    };
    for row in [&b, &gone, &c, &a] {
        app.projects_upsert("write", row.clone()).await.unwrap();
    }
    assert_eq!(
        ids(app
            .projects_select("write", ProjectSelection::default())
            .await
            .unwrap()),
        ["c", "a", "b"]
    );
    assert_eq!(
        ids(app
            .projects_select(
                "write",
                ProjectSelection {
                    include_deleted: true,
                    ..Default::default()
                }
            )
            .await
            .unwrap()),
        ["gone", "c", "a", "b"]
    );
    assert_eq!(
        ids(app
            .projects_select(
                "write",
                ProjectSelection {
                    workspace_root: Some("/fixture/exact".into()),
                    ..Default::default()
                }
            )
            .await
            .unwrap()),
        ["a", "b"]
    );
    assert_eq!(
        ids(app
            .projects_select(
                "write",
                ProjectSelection {
                    project_ids: Some(vec!["b".into(), "a".into(), "b".into(), "missing".into()]),
                    ..Default::default()
                }
            )
            .await
            .unwrap()),
        ["a", "b"]
    );
    assert!(
        app.projects_select(
            "write",
            ProjectSelection {
                project_ids: Some(vec![]),
                ..Default::default()
            }
        )
        .await
        .unwrap()
        .is_empty()
    );
    assert!(
        app.projects_select(
            "write",
            ProjectSelection {
                project_id: Some("a".into()),
                project_ids: Some(vec!["b".into()]),
                ..Default::default()
            }
        )
        .await
        .unwrap()
        .is_empty()
    );
    assert_eq!(
        app.projects_select(
            "write",
            ProjectSelection {
                project_id: Some("a".into()),
                ..Default::default()
            }
        )
        .await
        .unwrap(),
        vec![a.clone()]
    );
    let raw = app
        .get("write", PROJECTS, &[text_cell("a")])
        .await
        .unwrap()
        .unwrap();
    assert_eq!(raw["auto_pull"], int_cell(1));
    assert_eq!(
        raw["default_model_selection_json"],
        nullable_cell(a.default_model_selection.as_deref())
    );
    assert_eq!(raw["favicon_path"], text_cell(""));
    app.commit("write").await.unwrap();
}

#[tokio::test]
async fn project_reindexing_and_nested_rollback_restore_all_access_paths() {
    let fixture = TestFixture::start().await;
    let store = fixture.initialize().await;
    let mut app = initialize(&store).await;
    prepare(&store).await;
    let original = project("project", "2026-11-01T00:00:00.000Z", "/old");
    app.begin("write").await.unwrap();
    app.projects_upsert("write", original.clone())
        .await
        .unwrap();
    app.savepoint("write", "inner").await.unwrap();
    let changed = ProjectRecord {
        workspace_root: "/new".into(),
        created_at: "2026-10-01T00:00:00.000Z".into(),
        deleted_at: Some("2026-11-02T00:00:00.000Z".into()),
        ..original.clone()
    };
    app.projects_upsert("write", changed.clone()).await.unwrap();
    assert!(
        app.projects_select("write", ProjectSelection::default())
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        app.projects_select(
            "write",
            ProjectSelection {
                workspace_root: Some("/old".into()),
                include_deleted: true,
                ..Default::default()
            }
        )
        .await
        .unwrap()
        .is_empty()
    );
    assert_eq!(
        app.projects_select(
            "write",
            ProjectSelection {
                workspace_root: Some("/new".into()),
                include_deleted: true,
                ..Default::default()
            }
        )
        .await
        .unwrap(),
        vec![changed]
    );
    let invalid = ProjectRecord {
        auto_pull: 2,
        ..original.clone()
    };
    assert!(matches!(
        app.projects_upsert("write", invalid).await,
        Err(Error::Invalid(_))
    ));
    app.rollback_to("write", "inner").await.unwrap();
    app.release("write", "inner").await.unwrap();
    assert_eq!(
        app.projects_select("write", ProjectSelection::default())
            .await
            .unwrap(),
        vec![original.clone()]
    );
    assert!(
        app.projects_select(
            "write",
            ProjectSelection {
                workspace_root: Some("/new".into()),
                include_deleted: true,
                ..Default::default()
            }
        )
        .await
        .unwrap()
        .is_empty()
    );
    app.commit("write").await.unwrap();
    assert_eq!(prepare(&store).await.indexed_rows, 0);
    app.begin_mode("read", ApplicationTransactionMode::Read, None)
        .await
        .unwrap();
    assert_eq!(
        app.projects_select("read", ProjectSelection::default())
            .await
            .unwrap(),
        vec![original.clone()]
    );
    assert!(app.projects_upsert("read", original).await.is_err());
}

#[tokio::test]
async fn project_preparation_resumes_and_index_pages_do_not_truncate_results() {
    let fixture = TestFixture::start().await;
    let store = fixture.initialize().await;
    let mut app = initialize(&store).await;
    app.begin("seed").await.unwrap();
    for index in 0..513 {
        let row = project(
            &format!("project:{index:04}"),
            "2026-11-01T00:00:00.000Z",
            "/many",
        );
        app.put(
            "seed",
            PROJECTS,
            vec![text_cell(&row.project_id)],
            row.stored(),
        )
        .await
        .unwrap();
    }
    app.commit("seed").await.unwrap();
    let first = store.prepare_application_projects().await.unwrap();
    assert!(!first.prepared);
    assert_eq!(first.indexed_rows, PREPARE_ROWS as u64);
    app.begin("incomplete").await.unwrap();
    assert!(matches!(
        app.projects_select("incomplete", ProjectSelection::default())
            .await,
        Err(Error::Invalid(_))
    ));
    app.begin("bypass").await.unwrap();
    assert!(matches!(
        app.delete("bypass", PROJECTS, &[text_cell("project:0000")])
            .await,
        Err(Error::Invalid(_))
    ));
    drop(app);
    store.shutdown().await.unwrap();
    let store = fixture.connect().await;
    assert_eq!(prepare(&store).await.indexed_rows, 513);
    let mut app = ApplicationTransactions::new(store.clone());
    app.begin("read").await.unwrap();
    let selected = app
        .projects_select(
            "read",
            ProjectSelection {
                workspace_root: Some("/many".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(selected.len(), 513);
    assert_eq!(selected.first().unwrap().project_id, "project:0000");
    assert_eq!(selected.last().unwrap().project_id, "project:0512");
    app.rollback("read").await.unwrap();
}

#[tokio::test]
async fn project_index_order_matches_binary_text_with_prefix_and_unicode_values() {
    let fixture = TestFixture::start().await;
    let store = fixture.initialize().await;
    let mut app = initialize(&store).await;
    prepare(&store).await;
    app.begin("write").await.unwrap();
    let mut expected = vec![
        project("z", "a\0", "/x"),
        project("🦊", "a", "/x"),
        project("a", "aa", "/x"),
        project("a", "a", "/x"),
        project("中", "a", "/x"),
    ];
    // Distinct primary keys; dates intentionally exercise the row layer's BINARY ordering.
    expected[2].project_id = "aa".into();
    for row in &expected {
        app.projects_upsert("write", row.clone()).await.unwrap();
    }
    expected.sort_by(|a, b| {
        a.created_at
            .cmp(&b.created_at)
            .then(a.project_id.cmp(&b.project_id))
    });
    assert_eq!(
        app.projects_select("write", ProjectSelection::default())
            .await
            .unwrap(),
        expected
    );
    app.rollback("write").await.unwrap();
}

#[tokio::test]
async fn project_preparation_rejects_noncanonical_cells_without_reencoding_json() {
    let fixture = TestFixture::start().await;
    let store = fixture.initialize().await;
    let mut app = initialize(&store).await;
    let original = project("project", "2026-11-01T00:00:00.000Z", "/fixture");
    let mut row = original.stored();
    row.insert("auto_pull".into(), json!({"type":"integer","value":"01"}));
    app.begin("seed").await.unwrap();
    app.put("seed", PROJECTS, vec![text_cell("project")], row)
        .await
        .unwrap();
    app.commit("seed").await.unwrap();
    assert!(matches!(
        store.prepare_application_projects().await,
        Err(Error::IncompatibleStore)
    ));
    app.begin("fix").await.unwrap();
    app.put(
        "fix",
        PROJECTS,
        vec![text_cell("project")],
        original.stored(),
    )
    .await
    .unwrap();
    app.commit("fix").await.unwrap();
    prepare(&store).await;
    app.begin("read").await.unwrap();
    assert_eq!(
        app.projects_select("read", ProjectSelection::default())
            .await
            .unwrap(),
        vec![original]
    );
    app.rollback("read").await.unwrap();
}

#[tokio::test]
async fn imported_defaults_preserve_a_custom_project_title_until_an_explicit_title_change() {
    let fixture = TestFixture::start().await;
    let store = fixture.initialize().await;
    let mut app = initialize(&store).await;
    prepare(&store).await;
    let mut custom = project("custom", "2026-11-13T00:00:00.000Z", "/fixture");
    custom.title = "Chosen title".into();
    custom.has_custom_title = 1;
    app.begin("write").await.unwrap();
    app.projects_upsert("write", custom.clone()).await.unwrap();
    let mut automatic = custom.clone();
    automatic.has_custom_title = 0;
    automatic.title = "Repository default".into();
    automatic.auto_pull = 0;
    app.projects_upsert("write", automatic).await.unwrap();
    let rows = app
        .projects_select(
            "write",
            ProjectSelection {
                project_id: Some("custom".into()),
                include_deleted: false,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(rows[0].title, "Chosen title");
    assert_eq!(rows[0].has_custom_title, 1);
    assert_eq!(rows[0].auto_pull, 0);
    custom.title = "New chosen title".into();
    app.projects_upsert("write", custom).await.unwrap();
    app.commit("write").await.unwrap();
    app.begin("verify").await.unwrap();
    let rows = app
        .projects_select(
            "verify",
            ProjectSelection {
                project_id: Some("custom".into()),
                include_deleted: false,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(rows[0].title, "New chosen title");
    app.rollback("verify").await.unwrap();
}
