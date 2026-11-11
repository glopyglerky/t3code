use super::*;
use crate::application::{ApplicationSchema, ApplicationTable, ApplicationTransactionMode};
use crate::test_support::TestFixture;

fn schema() -> ApplicationSchema {
    ApplicationSchema {
        version: 1,
        tables: vec![
            ApplicationTable {
                name: SESSIONS.into(),
                columns: SESSION_COLUMNS.iter().map(|s| (*s).into()).collect(),
                primary_key: vec!["session_id".into()],
            },
            ApplicationTable {
                name: LINKS.into(),
                columns: LINK_COLUMNS.iter().map(|s| (*s).into()).collect(),
                primary_key: vec!["id".into()],
            },
        ],
    }
}
fn session(id: &str, expires: &str) -> CreateSession {
    CreateSession {
        session_id: id.into(),
        subject: "owner".into(),
        scopes: vec!["orchestration:read".into()],
        method: "bearer-access-token".into(),
        client: ClientMetadata {
            label: Some("desktop".into()),
            ip_address: None,
            user_agent: None,
            device_type: "desktop".into(),
            os: None,
            browser: None,
        },
        issued_at: "2026-11-10T00:00:00.000Z".into(),
        expires_at: expires.into(),
    }
}
fn link(id: &str) -> CreateLink {
    CreateLink {
        id: id.into(),
        credential: format!("credential-{id}"),
        method: "one-time-token".into(),
        scopes: vec!["orchestration:read".into(), "access:read".into()],
        subject: "owner".into(),
        label: None,
        proof_key_thumbprint: Some("proof".into()),
        created_at: "2026-11-10T00:00:00.000Z".into(),
        expires_at: "2027-01-01T00:00:00.000Z".into(),
    }
}
async fn setup(store: &Store) -> ApplicationTransactions {
    let mut app = ApplicationTransactions::new(store.clone());
    app.initialize(schema()).await.unwrap();
    loop {
        if store.prepare_application_auth().await.unwrap().prepared {
            break;
        }
    }
    app
}
fn consume(id: &str) -> AuthOperation {
    AuthOperation::PairingConsume {
        credential: format!("credential-{id}"),
        proof_key_thumbprint: Some("proof".into()),
        requested_scopes: Some(vec!["access:read".into()]),
        required_scopes: vec!["orchestration:read".into()],
        mcp_approval_subject: None,
        consumed_at: "2026-11-10T12:00:00.000Z".into(),
        now: "2026-11-10T12:00:00.000Z".into(),
    }
}

#[tokio::test]
async fn active_sessions_include_connected_expired_and_order_newest_id_first() {
    let fixture = TestFixture::start().await;
    let store = fixture.initialize().await;
    let mut app = setup(&store).await;
    app.begin("seed").await.unwrap();
    for (id, expires) in [
        ("old", "2020-01-01"),
        ("a", "2030-01-01"),
        ("b", "2030-01-01"),
    ] {
        app.auth_write(
            "seed",
            AuthOperation::SessionCreate {
                session: session(id, expires),
                if_absent: false,
            },
        )
        .await
        .unwrap();
    }
    let rows = app
        .auth_read(
            "seed",
            AuthOperation::SessionListActive {
                now: "2026-11-10".into(),
                connected_session_ids: vec!["old".into(), "old".into(), "missing".into()],
            },
        )
        .await
        .unwrap();
    assert_eq!(
        rows.as_array()
            .unwrap()
            .iter()
            .map(|r| r["sessionId"].as_str().unwrap())
            .collect::<Vec<_>>(),
        ["old", "b", "a"]
    );
    app.auth_write(
        "seed",
        AuthOperation::SessionRevoke {
            session_id: "old".into(),
            revoked_at: "now".into(),
        },
    )
    .await
    .unwrap();
    let rows = app
        .auth_read(
            "seed",
            AuthOperation::SessionListActive {
                now: "2026-11-10".into(),
                connected_session_ids: vec!["old".into()],
            },
        )
        .await
        .unwrap();
    assert_eq!(rows.as_array().unwrap().len(), 2);
    app.commit("seed").await.unwrap();
    let mut reopened = ApplicationTransactions::new(store);
    reopened
        .begin_mode("read", ApplicationTransactionMode::Read, None)
        .await
        .unwrap();
    let row = reopened
        .auth_read(
            "read",
            AuthOperation::SessionGet {
                session_id: "a".into(),
            },
        )
        .await
        .unwrap();
    assert_eq!(row["clientLabel"], "desktop");
    assert_eq!(row["scopes"], "[\"orchestration:read\"]");
    reopened.rollback("read").await.unwrap();
}

#[tokio::test]
async fn failed_replacement_rolls_back_revocations_and_if_absent_preserves_original() {
    let fixture = TestFixture::start().await;
    let store = fixture.initialize().await;
    let mut app = setup(&store).await;
    app.begin("seed").await.unwrap();
    app.auth_write(
        "seed",
        AuthOperation::SessionCreate {
            session: session("a", "2030"),
            if_absent: false,
        },
    )
    .await
    .unwrap();
    app.auth_write(
        "seed",
        AuthOperation::SessionCreate {
            session: session("b", "2030"),
            if_absent: false,
        },
    )
    .await
    .unwrap();
    app.commit("seed").await.unwrap();
    app.begin("replace").await.unwrap();
    assert!(matches!(
        app.auth_write(
            "replace",
            AuthOperation::SessionCreateReplacing {
                session: session("b", "2031"),
                revoked_at: "2026".into(),
                replace_session_id: None
            }
        )
        .await,
        Err(Error::DuplicateRecord)
    ));
    app.begin("verify").await.unwrap();
    let rows = app
        .auth_read(
            "verify",
            AuthOperation::SessionListActive {
                now: "2026".into(),
                connected_session_ids: vec![],
            },
        )
        .await
        .unwrap();
    assert_eq!(rows.as_array().unwrap().len(), 2);
    app.auth_write(
        "verify",
        AuthOperation::SessionCreate {
            session: session("a", "2040"),
            if_absent: true,
        },
    )
    .await
    .unwrap();
    assert_eq!(
        app.auth_read(
            "verify",
            AuthOperation::SessionGet {
                session_id: "a".into()
            }
        )
        .await
        .unwrap()["expiresAt"],
        "2030"
    );
    app.rollback("verify").await.unwrap();
}

#[tokio::test]
async fn pairing_checks_do_not_consume_rejected_codes_and_rollback_restores_available_code() {
    let fixture = TestFixture::start().await;
    let store = fixture.initialize().await;
    let mut app = setup(&store).await;
    app.begin("seed").await.unwrap();
    app.auth_write("seed", AuthOperation::PairingCreate { link: link("x") })
        .await
        .unwrap();
    app.commit("seed").await.unwrap();
    app.begin("consume").await.unwrap();
    let mut invalid = consume("x");
    if let AuthOperation::PairingConsume {
        proof_key_thumbprint,
        ..
    } = &mut invalid
    {
        *proof_key_thumbprint = Some("wrong".into());
    }
    assert!(app.auth_write("consume", invalid).await.unwrap().is_null());
    let mut invalid = consume("x");
    if let AuthOperation::PairingConsume {
        requested_scopes, ..
    } = &mut invalid
    {
        *requested_scopes = Some(vec![]);
    }
    assert!(app.auth_write("consume", invalid).await.unwrap().is_null());
    let mut invalid = consume("x");
    if let AuthOperation::PairingConsume {
        required_scopes, ..
    } = &mut invalid
    {
        *required_scopes = vec!["access:write".into()];
    }
    assert!(app.auth_write("consume", invalid).await.unwrap().is_null());
    assert_eq!(
        app.auth_write("consume", consume("x")).await.unwrap()["id"],
        "x"
    );
    assert!(
        app.auth_write("consume", consume("x"))
            .await
            .unwrap()
            .is_null()
    );
    app.auth_write(
        "consume",
        AuthOperation::SessionCreate {
            session: session("paired", "2030"),
            if_absent: false,
        },
    )
    .await
    .unwrap();
    app.rollback("consume").await.unwrap();
    app.begin("retry").await.unwrap();
    assert!(
        app.auth_read(
            "retry",
            AuthOperation::SessionGet {
                session_id: "paired".into()
            }
        )
        .await
        .unwrap()
        .is_null()
    );
    assert_eq!(
        app.auth_write("retry", consume("x")).await.unwrap()["id"],
        "x"
    );
    app.commit("retry").await.unwrap();
}

#[tokio::test]
async fn approval_subject_is_case_insensitive_for_prefix_but_exact_for_authorization() {
    let fixture = TestFixture::start().await;
    let store = fixture.initialize().await;
    let mut app = setup(&store).await;
    app.begin("scope").await.unwrap();
    let mut approval = link("approval");
    approval.subject = "MCP-APPROVAL:read".into();
    app.auth_write("scope", AuthOperation::PairingCreate { link: approval })
        .await
        .unwrap();
    assert!(
        app.auth_write("scope", consume("approval"))
            .await
            .unwrap()
            .is_null()
    );
    let mut wrong = consume("approval");
    if let AuthOperation::PairingConsume {
        mcp_approval_subject,
        ..
    } = &mut wrong
    {
        *mcp_approval_subject = Some("mcp-approval:read".into());
    }
    assert!(app.auth_write("scope", wrong).await.unwrap().is_null());
    let mut right = consume("approval");
    if let AuthOperation::PairingConsume {
        mcp_approval_subject,
        ..
    } = &mut right
    {
        *mcp_approval_subject = Some("MCP-APPROVAL:read".into());
    }
    assert_eq!(
        app.auth_write("scope", right).await.unwrap()["id"],
        "approval"
    );
    app.rollback("scope").await.unwrap();
}

#[tokio::test]
async fn imported_rows_are_prepared_and_read_route_cannot_execute_mutations() {
    let fixture = TestFixture::start().await;
    let store = fixture.initialize().await;
    let mut app = ApplicationTransactions::new(store.clone());
    app.initialize(schema()).await.unwrap();
    app.begin("import").await.unwrap();
    let row = session_row(session("imported", "2030")).unwrap();
    app.put("import", SESSIONS, vec![text_cell("imported")], row)
        .await
        .unwrap();
    app.commit("import").await.unwrap();
    loop {
        if store.prepare_application_auth().await.unwrap().prepared {
            break;
        }
    }
    assert_eq!(
        store.prepare_application_auth().await.unwrap().indexed_rows,
        1
    );
    app.begin("read-route").await.unwrap();
    assert!(
        app.auth_read(
            "read-route",
            AuthOperation::SessionRevoke {
                session_id: "imported".into(),
                revoked_at: "2026".into()
            }
        )
        .await
        .is_err()
    );
    app.begin_mode("verify", ApplicationTransactionMode::Read, None)
        .await
        .unwrap();
    assert!(
        app.auth_read(
            "verify",
            AuthOperation::SessionGet {
                session_id: "imported".into()
            }
        )
        .await
        .unwrap()["revokedAt"]
            .is_null()
    );
    app.rollback("verify").await.unwrap();
}
