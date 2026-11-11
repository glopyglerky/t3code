use super::*;
use crate::application::{ApplicationSchema, ApplicationTable, ApplicationTransactionMode};
use crate::test_support::TestFixture;

fn schema() -> ApplicationSchema {
    ApplicationSchema {
        version: 1,
        tables: vec![
            ApplicationTable {
                name: GUIDES.into(),
                columns: GUIDE_COLUMNS.iter().map(|name| (*name).into()).collect(),
                primary_key: vec!["host".into(), "repository".into(), "number".into()],
            },
            ApplicationTable {
                name: FILES.into(),
                columns: FILE_COLUMNS.iter().map(|name| (*name).into()).collect(),
                primary_key: FILE_PRIMARY.iter().map(|name| (*name).into()).collect(),
            },
        ],
    }
}

fn scope(viewer: &str) -> ViewedScope {
    ViewedScope {
        provider: "github".into(),
        host: "github.com".into(),
        repository: "owner/repository".into(),
        number: 17,
        viewer: viewer.into(),
    }
}

async fn prepare(store: &Store) {
    loop {
        if store
            .prepare_application_viewed_files()
            .await
            .unwrap()
            .prepared
        {
            break;
        }
    }
}

#[tokio::test]
async fn guides_and_viewed_marks_share_transaction_and_survive_reopening() {
    let fixture = TestFixture::start().await;
    let store = fixture.initialize().await;
    let mut app = ApplicationTransactions::new(store.clone());
    app.initialize(schema()).await.unwrap();
    prepare(&store).await;
    let key = GuideKey {
        host: "github.com".into(),
        repository: "owner/repository".into(),
        number: 17,
    };
    app.begin("write").await.unwrap();
    assert_eq!(app.guide_get("write", key.clone()).await.unwrap(), None);
    app.guide_put("write", key.clone(), "{\"text\":\"hello\"}".into())
        .await
        .unwrap();
    app.viewed_files_set(
        "write",
        scope("alice"),
        vec![
            FileUpdate {
                path: "z.rs".into(),
                revision: None,
                viewed: true,
            },
            FileUpdate {
                path: "a.rs".into(),
                revision: Some("sha".into()),
                viewed: true,
            },
        ],
        "2026-11-10T00:00:00.000Z".into(),
    )
    .await
    .unwrap();
    assert_eq!(
        app.viewed_files_list("write", scope("alice"))
            .await
            .unwrap()
            .files[0]
            .path,
        "a.rs"
    );
    app.commit("write").await.unwrap();

    let mut again = ApplicationTransactions::new(store.clone());
    again
        .begin_mode("read", ApplicationTransactionMode::Read, None)
        .await
        .unwrap();
    assert_eq!(
        again.guide_get("read", key).await.unwrap(),
        Some("{\"text\":\"hello\"}".into())
    );
    let page = again
        .viewed_files_list("read", scope("alice"))
        .await
        .unwrap();
    assert!(!page.truncated);
    assert_eq!(
        page.files,
        vec![
            FileMark {
                path: "a.rs".into(),
                revision: Some("sha".into())
            },
            FileMark {
                path: "z.rs".into(),
                revision: None
            },
        ]
    );
    assert!(
        again
            .viewed_files_list("read", scope("bob"))
            .await
            .unwrap()
            .files
            .is_empty()
    );
    again.rollback("read").await.unwrap();

    again.begin("remove").await.unwrap();
    again
        .viewed_files_set(
            "remove",
            scope("alice"),
            vec![FileUpdate {
                path: "a.rs".into(),
                revision: None,
                viewed: false,
            }],
            "2026-11-10T01:00:00.000Z".into(),
        )
        .await
        .unwrap();
    again.commit("remove").await.unwrap();
    again
        .begin_mode("verify", ApplicationTransactionMode::Read, None)
        .await
        .unwrap();
    assert_eq!(
        again
            .viewed_files_list("verify", scope("alice"))
            .await
            .unwrap()
            .files
            .len(),
        1
    );
    again.rollback("verify").await.unwrap();
}

#[tokio::test]
async fn viewed_file_limit_is_bounded_and_rollback_discards_marks() {
    let fixture = TestFixture::start().await;
    let store = fixture.initialize().await;
    let mut app = ApplicationTransactions::new(store.clone());
    app.initialize(schema()).await.unwrap();
    prepare(&store).await;
    app.begin("batch").await.unwrap();
    let files = (0..501)
        .map(|i| FileUpdate {
            path: format!("file-{i:04}.rs"),
            revision: None,
            viewed: true,
        })
        .collect();
    app.viewed_files_set("batch", scope("alice"), files, "now".into())
        .await
        .unwrap();
    let page = app
        .viewed_files_list("batch", scope("alice"))
        .await
        .unwrap();
    assert!(page.truncated);
    assert_eq!(page.files.len(), 500);
    assert_eq!(page.files.first().unwrap().path, "file-0000.rs");
    assert_eq!(page.files.last().unwrap().path, "file-0499.rs");
    app.rollback("batch").await.unwrap();
    app.begin_mode("check", ApplicationTransactionMode::Read, None)
        .await
        .unwrap();
    assert!(
        app.viewed_files_list("check", scope("alice"))
            .await
            .unwrap()
            .files
            .is_empty()
    );
    app.rollback("check").await.unwrap();
}
