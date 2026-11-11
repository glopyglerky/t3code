//! Authentication repositories sharing the application's reserved omdb transaction.
//! Active/credential indexes are witnessed and prepared from imported rows in bounded batches.

use std::{collections::BTreeMap, time::Duration};

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Map, Value, json};

use crate::application::{
    ApplicationCatalog, ApplicationTransactions, Session, decode_catalog, decode_row, row_prefix,
    schema_key, table,
};
use crate::application_rows::{
    managed_marker, nullable, nullable_cell, read_row, text, text_cell, validate_table, write_row,
};
use crate::backend::Transaction;
use crate::{Error, Result, Store, request_digest, store_key};

const SESSIONS: &str = "auth_sessions";
const LINKS: &str = "auth_pairing_links";
const SESSION_COLUMNS: [&str; 16] = [
    "session_id",
    "subject",
    "scopes",
    "method",
    "client_label",
    "client_ip_address",
    "client_user_agent",
    "client_device_type",
    "client_os",
    "client_browser",
    "issued_at",
    "expires_at",
    "last_connected_at",
    "revoked_at",
    "client_surface",
    "client_app_version",
];
const LINK_COLUMNS: [&str; 11] = [
    "id",
    "credential",
    "method",
    "scopes",
    "subject",
    "label",
    "proof_key_thumbprint",
    "created_at",
    "expires_at",
    "consumed_at",
    "revoked_at",
];
const MARKER: &str = "auth-v1";
const PAGE: usize = 128;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ClientMetadata {
    pub label: Option<String>,
    pub ip_address: Option<String>,
    pub user_agent: Option<String>,
    pub device_type: String,
    pub os: Option<String>,
    pub browser: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CreateSession {
    pub session_id: String,
    pub subject: String,
    pub scopes: Vec<String>,
    pub method: String,
    pub client: ClientMetadata,
    pub issued_at: String,
    pub expires_at: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CreateLink {
    pub id: String,
    pub credential: String,
    pub method: String,
    pub scopes: Vec<String>,
    pub subject: String,
    pub label: Option<String>,
    pub proof_key_thumbprint: Option<String>,
    pub created_at: String,
    pub expires_at: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum AuthOperation {
    SessionCreate {
        session: CreateSession,
        #[serde(default)]
        if_absent: bool,
    },
    SessionCreateReplacing {
        session: CreateSession,
        revoked_at: String,
        replace_session_id: Option<String>,
    },
    SessionGet {
        session_id: String,
    },
    SessionListActive {
        now: String,
        #[serde(default)]
        connected_session_ids: Vec<String>,
    },
    SessionRevoke {
        session_id: String,
        revoked_at: String,
    },
    SessionRevokeAllExcept {
        current_session_id: String,
        revoked_at: String,
    },
    SessionSetLastConnected {
        session_id: String,
        last_connected_at: String,
    },
    PairingCreate {
        link: CreateLink,
    },
    PairingConsume {
        credential: String,
        proof_key_thumbprint: Option<String>,
        requested_scopes: Option<Vec<String>>,
        #[serde(default)]
        required_scopes: Vec<String>,
        mcp_approval_subject: Option<String>,
        consumed_at: String,
        now: String,
    },
    PairingListActive {
        now: String,
    },
    PairingRevoke {
        id: String,
        revoked_at: String,
    },
    PairingGet {
        credential: String,
    },
}

impl AuthOperation {
    pub fn is_read(&self) -> bool {
        matches!(
            self,
            Self::SessionGet { .. }
                | Self::SessionListActive { .. }
                | Self::PairingListActive { .. }
                | Self::PairingGet { .. }
        )
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct PreparationResult {
    pub prepared: bool,
    pub indexed_rows: u64,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Preparation {
    version: u32,
    table: usize,
    after: Option<String>,
    indexed_rows: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Witness {
    id: String,
    value: String,
}

fn invalid(message: &str) -> Error {
    Error::Invalid(message.into())
}
fn encode(value: &impl Serialize) -> Result<Vec<u8>> {
    serde_json::to_vec(value).map_err(crate::json_error)
}
fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    serde_json::from_slice(bytes).map_err(|_| Error::IncompatibleStore)
}
fn hex(value: &str) -> String {
    value.bytes().map(|byte| format!("{byte:02x}")).collect()
}
fn key(catalog: &ApplicationCatalog, suffix: &str) -> Vec<u8> {
    store_key(&format!(
        "application/generation/{}/auth/v1/{suffix}",
        catalog.generation
    ))
}
fn end(prefix: &[u8]) -> Result<Vec<u8>> {
    let mut bound = prefix.to_vec();
    let last = bound.last_mut().ok_or(Error::IncompatibleStore)?;
    *last = last.checked_add(1).ok_or(Error::IncompatibleStore)?;
    Ok(bound)
}
fn validate(catalog: &ApplicationCatalog) -> Result<()> {
    validate_table(catalog, SESSIONS, &SESSION_COLUMNS, &["session_id"])?;
    validate_table(catalog, LINKS, &LINK_COLUMNS, &["id"])
}

async fn ready(session: &mut Session) -> Result<()> {
    validate(&session.catalog)?;
    for name in [SESSIONS, LINKS] {
        let marker = session
            .transaction
            .get(&managed_marker(&session.catalog, name))
            .await?;
        if marker
            .as_deref()
            .map(decode::<String>)
            .transpose()?
            .as_deref()
            != Some(MARKER)
        {
            return Err(invalid("application authentication is not prepared"));
        }
    }
    let state: Preparation = session
        .transaction
        .get(&key(&session.catalog, "preparation"))
        .await?
        .as_deref()
        .map(decode)
        .transpose()?
        .ok_or(Error::IncompatibleStore)?;
    if state.version != 1 || state.table > 2 {
        return Err(Error::IncompatibleStore);
    }
    if state.table != 2 {
        return Err(invalid("application authentication is not prepared"));
    }
    Ok(())
}

fn active_prefix(catalog: &ApplicationCatalog, name: &str) -> Vec<u8> {
    key(catalog, &format!("{name}/active/"))
}
fn active_key(
    catalog: &ApplicationCatalog,
    name: &str,
    row: &Map<String, Value>,
) -> Result<Vec<u8>> {
    let id = text(row, if name == SESSIONS { "session_id" } else { "id" })?;
    let expires = text(row, "expires_at")?;
    Ok(key(
        catalog,
        &format!("{name}/active/{}/{}/", hex(&expires), hex(&id)),
    ))
}
fn replacement_prefix(catalog: &ApplicationCatalog, subject: &str, method: &str) -> Vec<u8> {
    key(
        catalog,
        &format!(
            "sessions/replacement/{}/",
            request_digest(&json!([subject, method]))
        ),
    )
}
fn replacement_key(catalog: &ApplicationCatalog, row: &Map<String, Value>) -> Result<Vec<u8>> {
    let mut result = replacement_prefix(catalog, &text(row, "subject")?, &text(row, "method")?);
    result.extend_from_slice(hex(&text(row, "session_id")?).as_bytes());
    Ok(result)
}
fn credential_key(catalog: &ApplicationCatalog, credential: &str) -> Vec<u8> {
    key(
        catalog,
        &format!("links/credential/{}", request_digest(&json!(credential))),
    )
}
fn is_active(name: &str, row: &Map<String, Value>) -> Result<bool> {
    Ok(nullable(row, "revoked_at")?.is_none()
        && (name == SESSIONS || nullable(row, "consumed_at")?.is_none()))
}

fn indexes(
    catalog: &ApplicationCatalog,
    name: &str,
    row: &Map<String, Value>,
) -> Result<Vec<(Vec<u8>, Witness)>> {
    let id = text(row, if name == SESSIONS { "session_id" } else { "id" })?;
    let mut entries = Vec::new();
    if name == LINKS {
        let credential = text(row, "credential")?;
        entries.push((
            credential_key(catalog, &credential),
            Witness {
                id: id.clone(),
                value: credential,
            },
        ));
    }
    if is_active(name, row)? {
        entries.push((
            active_key(catalog, name, row)?,
            Witness {
                id: id.clone(),
                value: text(row, "expires_at")?,
            },
        ));
        if name == SESSIONS {
            entries.push((
                replacement_key(catalog, row)?,
                Witness {
                    id,
                    value: serde_json::to_string(&json!([
                        text(row, "subject")?,
                        text(row, "method")?
                    ]))
                    .map_err(crate::json_error)?,
                },
            ));
        }
    }
    Ok(entries)
}
async fn add_indexes(
    tx: &mut Transaction,
    catalog: &ApplicationCatalog,
    name: &str,
    row: &Map<String, Value>,
) -> Result<()> {
    for (encoded, witness) in indexes(catalog, name, row)? {
        if let Some(bytes) = tx.get(&encoded).await? {
            let old: Witness = decode(&bytes)?;
            if old.value != witness.value {
                return Err(Error::IncompatibleStore);
            }
            if old.id != witness.id {
                return Err(Error::DuplicateRecord);
            }
        }
        tx.put(encoded, encode(&witness)?).await?;
    }
    Ok(())
}
async fn remove_indexes(
    tx: &mut Transaction,
    catalog: &ApplicationCatalog,
    name: &str,
    row: &Map<String, Value>,
) -> Result<()> {
    for (encoded, witness) in indexes(catalog, name, row)? {
        let old: Witness = decode(&tx.get(&encoded).await?.ok_or(Error::IncompatibleStore)?)?;
        if old != witness {
            return Err(Error::IncompatibleStore);
        }
        tx.delete(encoded).await?;
    }
    Ok(())
}
async fn replace_row(
    session: &mut Session,
    name: &str,
    previous: &Map<String, Value>,
    row: Map<String, Value>,
) -> Result<()> {
    remove_indexes(&mut session.transaction, &session.catalog, name, previous).await?;
    write_row(
        &mut session.transaction,
        &session.catalog,
        name,
        row.clone(),
    )
    .await?;
    add_indexes(&mut session.transaction, &session.catalog, name, &row).await
}

async fn get_row(
    session: &mut Session,
    name: &str,
    id: &str,
) -> Result<Option<Map<String, Value>>> {
    Ok(read_row(
        &mut session.transaction,
        &session.catalog,
        name,
        &[text_cell(id)],
    )
    .await?
    .map(|row| row.row))
}

// Only the rows selected by an index are materialized. Results remain subject to the
// application's shared read budget; this never silently truncates a repository result.
async fn scan_index(
    session: &mut Session,
    prefix: Vec<u8>,
    after: Option<&str>,
    name: &str,
) -> Result<Vec<Map<String, Value>>> {
    let mut start = prefix.clone();
    if let Some(after) = after {
        start.extend_from_slice(hex(after).as_bytes());
        start.push(b'/');
        start = end(&start)?;
    }
    let mut rows = Vec::new();
    loop {
        let page = session
            .transaction
            .scan(&start, &end(&prefix)?, PAGE)
            .await?;
        let count = page.len();
        for (encoded, bytes) in page {
            let witness: Witness = decode(&bytes)?;
            let row = get_row(session, name, &witness.id)
                .await?
                .ok_or(Error::IncompatibleStore)?;
            if !indexes(&session.catalog, name, &row)?
                .iter()
                .any(|(k, w)| k == &encoded && w == &witness)
            {
                return Err(Error::IncompatibleStore);
            }
            start = encoded;
            start.push(0);
            rows.push(row);
        }
        if count < PAGE {
            return Ok(rows);
        }
    }
}

fn plain_cell(cell: &Value) -> Result<Value> {
    match cell.get("type").and_then(Value::as_str) {
        Some("null") if cell.as_object().is_some_and(|fields| fields.len() == 1) => Ok(Value::Null),
        Some("text") => cell
            .get("value")
            .filter(|value| value.is_string())
            .cloned()
            .ok_or(Error::IncompatibleStore),
        Some("integer") => cell
            .get("value")
            .and_then(Value::as_str)
            .and_then(|value| value.parse::<i64>().ok())
            .map(|value| json!(value))
            .ok_or(Error::IncompatibleStore),
        Some("real") => cell.get("value").cloned().ok_or(Error::IncompatibleStore),
        _ => Err(Error::IncompatibleStore),
    }
}
fn output(row: &Map<String, Value>, columns: &[(&str, &str)]) -> Result<Value> {
    Ok(Value::Object(
        columns
            .iter()
            .map(|(column, alias)| {
                Ok((
                    (*alias).into(),
                    plain_cell(row.get(*column).ok_or(Error::IncompatibleStore)?)?,
                ))
            })
            .collect::<Result<Map<_, _>>>()?,
    ))
}
fn session_output(row: &Map<String, Value>) -> Result<Value> {
    output(
        row,
        &[
            ("session_id", "sessionId"),
            ("subject", "subject"),
            ("scopes", "scopes"),
            ("method", "method"),
            ("client_label", "clientLabel"),
            ("client_ip_address", "clientIpAddress"),
            ("client_user_agent", "clientUserAgent"),
            ("client_device_type", "clientDeviceType"),
            ("client_os", "clientOs"),
            ("client_browser", "clientBrowser"),
            ("issued_at", "issuedAt"),
            ("expires_at", "expiresAt"),
            ("last_connected_at", "lastConnectedAt"),
            ("revoked_at", "revokedAt"),
        ],
    )
}
fn link_output(row: &Map<String, Value>) -> Result<Value> {
    output(
        row,
        &[
            ("id", "id"),
            ("credential", "credential"),
            ("method", "method"),
            ("scopes", "scopes"),
            ("subject", "subject"),
            ("label", "label"),
            ("proof_key_thumbprint", "proofKeyThumbprint"),
            ("created_at", "createdAt"),
            ("expires_at", "expiresAt"),
            ("consumed_at", "consumedAt"),
            ("revoked_at", "revokedAt"),
        ],
    )
}

fn session_row(session: CreateSession) -> Result<Map<String, Value>> {
    Ok(Map::from_iter([
        ("session_id".into(), text_cell(&session.session_id)),
        ("subject".into(), text_cell(&session.subject)),
        (
            "scopes".into(),
            text_cell(&serde_json::to_string(&session.scopes).map_err(crate::json_error)?),
        ),
        ("method".into(), text_cell(&session.method)),
        (
            "client_label".into(),
            nullable_cell(session.client.label.as_deref()),
        ),
        (
            "client_ip_address".into(),
            nullable_cell(session.client.ip_address.as_deref()),
        ),
        (
            "client_user_agent".into(),
            nullable_cell(session.client.user_agent.as_deref()),
        ),
        (
            "client_device_type".into(),
            text_cell(&session.client.device_type),
        ),
        (
            "client_os".into(),
            nullable_cell(session.client.os.as_deref()),
        ),
        (
            "client_browser".into(),
            nullable_cell(session.client.browser.as_deref()),
        ),
        ("issued_at".into(), text_cell(&session.issued_at)),
        ("expires_at".into(), text_cell(&session.expires_at)),
        ("last_connected_at".into(), nullable_cell(None)),
        ("revoked_at".into(), nullable_cell(None)),
        ("client_surface".into(), nullable_cell(None)),
        ("client_app_version".into(), nullable_cell(None)),
    ]))
}
fn link_row(link: CreateLink) -> Result<Map<String, Value>> {
    Ok(Map::from_iter([
        ("id".into(), text_cell(&link.id)),
        ("credential".into(), text_cell(&link.credential)),
        ("method".into(), text_cell(&link.method)),
        (
            "scopes".into(),
            text_cell(&serde_json::to_string(&link.scopes).map_err(crate::json_error)?),
        ),
        ("subject".into(), text_cell(&link.subject)),
        ("label".into(), nullable_cell(link.label.as_deref())),
        (
            "proof_key_thumbprint".into(),
            nullable_cell(link.proof_key_thumbprint.as_deref()),
        ),
        ("created_at".into(), text_cell(&link.created_at)),
        ("expires_at".into(), text_cell(&link.expires_at)),
        ("consumed_at".into(), nullable_cell(None)),
        ("revoked_at".into(), nullable_cell(None)),
    ]))
}
async fn create_session(
    session: &mut Session,
    input: CreateSession,
    if_absent: bool,
) -> Result<()> {
    if get_row(session, SESSIONS, &input.session_id)
        .await?
        .is_some()
    {
        return if if_absent {
            Ok(())
        } else {
            Err(Error::DuplicateRecord)
        };
    }
    let row = session_row(input)?;
    write_row(
        &mut session.transaction,
        &session.catalog,
        SESSIONS,
        row.clone(),
    )
    .await?;
    add_indexes(&mut session.transaction, &session.catalog, SESSIONS, &row).await
}
async fn credential_row(
    session: &mut Session,
    credential: &str,
) -> Result<Option<Map<String, Value>>> {
    let Some(bytes) = session
        .transaction
        .get(&credential_key(&session.catalog, credential))
        .await?
    else {
        return Ok(None);
    };
    let witness: Witness = decode(&bytes)?;
    if witness.value != credential {
        return Err(Error::IncompatibleStore);
    }
    let row = get_row(session, LINKS, &witness.id)
        .await?
        .ok_or(Error::IncompatibleStore)?;
    if text(&row, "credential")? != credential {
        return Err(Error::IncompatibleStore);
    }
    Ok(Some(row))
}
async fn revoke_session(
    session: &mut Session,
    row: Map<String, Value>,
    revoked_at: &str,
) -> Result<()> {
    let mut updated = row.clone();
    updated.insert("revoked_at".into(), text_cell(revoked_at));
    replace_row(session, SESSIONS, &row, updated).await
}

impl Store {
    pub async fn prepare_application_auth(&self) -> Result<PreparationResult> {
        let mut limit = 32;
        loop {
            match self.prepare_application_auth_batch(limit).await {
                Err(Error::ResourceLimit(_)) if limit > 1 => limit = (limit / 2).max(1),
                result => return result,
            }
        }
    }
    async fn prepare_application_auth_batch(&self, limit: usize) -> Result<PreparationResult> {
        let mut tx = self
            .db
            .begin_reserved(Duration::from_secs(5), Duration::from_secs(300))
            .await?;
        let catalog = decode_catalog(
            &tx.get(&schema_key())
                .await?
                .ok_or_else(|| invalid("application schema is not initialized"))?,
        )?;
        validate(&catalog)?;
        let state_key = key(&catalog, "preparation");
        let mut state = match tx.get(&state_key).await? {
            Some(bytes) => {
                let state: Preparation = decode(&bytes)?;
                if state.version != 1 || state.table > 2 {
                    return Err(Error::IncompatibleStore);
                }
                for name in [SESSIONS, LINKS] {
                    if tx
                        .get(&managed_marker(&catalog, name))
                        .await?
                        .as_deref()
                        .map(decode::<String>)
                        .transpose()?
                        .as_deref()
                        != Some(MARKER)
                    {
                        return Err(Error::IncompatibleStore);
                    }
                }
                state
            }
            None => {
                for name in [SESSIONS, LINKS] {
                    let marker = managed_marker(&catalog, name);
                    if tx.get(&marker).await?.is_some() {
                        return Err(Error::IncompatibleStore);
                    }
                    tx.put(marker, encode(&MARKER)?).await?;
                }
                Preparation {
                    version: 1,
                    table: 0,
                    after: None,
                    indexed_rows: 0,
                }
            }
        };
        if state.table == 2 {
            tx.rollback().await?;
            return Ok(PreparationResult {
                prepared: true,
                indexed_rows: state.indexed_rows,
            });
        }
        let name = [SESSIONS, LINKS][state.table];
        let prefix = row_prefix(&catalog.generation, name)?;
        let mut start = prefix.clone();
        if let Some(after) = &state.after {
            if after.len() != 64
                || !after
                    .bytes()
                    .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
            {
                return Err(Error::IncompatibleStore);
            }
            start.extend_from_slice(after.as_bytes());
            start.push(0);
        }
        let rows = tx.scan(&start, &end(&prefix)?, limit).await?;
        let count = rows.len();
        for (encoded, bytes) in rows {
            let row = decode_row(&catalog, table(&catalog, name)?, &encoded, &bytes)?;
            add_indexes(&mut tx, &catalog, name, &row.row).await?;
            state.after = Some(
                std::str::from_utf8(
                    encoded
                        .strip_prefix(prefix.as_slice())
                        .ok_or(Error::IncompatibleStore)?,
                )
                .map_err(|_| Error::IncompatibleStore)?
                .into(),
            );
            state.indexed_rows = state
                .indexed_rows
                .checked_add(1)
                .ok_or(Error::IncompatibleStore)?;
        }
        if count < limit {
            state.table += 1;
            state.after = None;
        }
        tx.put(state_key, encode(&state)?).await?;
        tx.commit().await?;
        Ok(PreparationResult {
            prepared: state.table == 2,
            indexed_rows: state.indexed_rows,
        })
    }
}

impl ApplicationTransactions {
    pub async fn auth_read(&mut self, id: &str, request: AuthOperation) -> Result<Value> {
        self.auth_execute(id, request, true).await
    }
    pub async fn auth_write(&mut self, id: &str, request: AuthOperation) -> Result<Value> {
        self.auth_execute(id, request, false).await
    }
    async fn auth_execute(
        &mut self,
        id: &str,
        request: AuthOperation,
        read_only: bool,
    ) -> Result<Value> {
        let mut session = self.take(id)?;
        let result = async {
            if request.is_read() != read_only {
                return Err(invalid(
                    "authentication operation does not match its transaction route",
                ));
            }
            if !read_only {
                session.transaction.require_write()?;
            }
            ready(&mut session).await?;
            execute(&mut session, request).await
        }
        .await;
        self.finish(id, session, result).await
    }
}

async fn execute(session: &mut Session, request: AuthOperation) -> Result<Value> {
    match request {
        AuthOperation::SessionCreate {
            session: input,
            if_absent,
        } => {
            create_session(session, input, if_absent).await?;
            Ok(json!({"written":true}))
        }
        AuthOperation::SessionCreateReplacing {
            session: input,
            revoked_at,
            replace_session_id,
        } => {
            let by_subject = replace_session_id.is_none();
            let candidates = if let Some(id) = replace_session_id {
                get_row(session, SESSIONS, &id).await?.into_iter().collect()
            } else {
                let prefix = replacement_prefix(&session.catalog, &input.subject, &input.method);
                scan_index(session, prefix, None, SESSIONS).await?
            };
            let mut ids = Vec::new();
            for row in candidates {
                if by_subject
                    && (text(&row, "subject")? != input.subject
                        || text(&row, "method")? != input.method)
                {
                    return Err(Error::IncompatibleStore);
                }
                if nullable(&row, "revoked_at")?.is_none()
                    && text(&row, "method")? == input.method
                    && text(&row, "expires_at")? > revoked_at
                {
                    ids.push(text(&row, "session_id")?);
                    revoke_session(session, row, &revoked_at).await?;
                }
            }
            create_session(session, input, false).await?;
            Ok(json!(ids))
        }
        AuthOperation::SessionGet { session_id } => get_row(session, SESSIONS, &session_id)
            .await?
            .map(|row| session_output(&row))
            .transpose()
            .map(|row| row.unwrap_or(Value::Null)),
        AuthOperation::SessionListActive {
            now,
            connected_session_ids,
        } => {
            let prefix = active_prefix(&session.catalog, SESSIONS);
            let mut rows: BTreeMap<String, Map<String, Value>> =
                scan_index(session, prefix, Some(&now), SESSIONS)
                    .await?
                    .into_iter()
                    .map(|row| Ok((text(&row, "session_id")?, row)))
                    .collect::<Result<_>>()?;
            for id in connected_session_ids {
                if rows.contains_key(&id) {
                    continue;
                }
                if let Some(row) = get_row(session, SESSIONS, &id).await? {
                    if nullable(&row, "revoked_at")?.is_none() {
                        rows.insert(id, row);
                    }
                }
            }
            let mut rows: Vec<_> = rows
                .into_values()
                .map(|row| Ok((text(&row, "issued_at")?, text(&row, "session_id")?, row)))
                .collect::<Result<_>>()?;
            rows.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| b.1.cmp(&a.1)));
            rows.into_iter()
                .map(|(_, _, row)| session_output(&row))
                .collect::<Result<Vec<_>>>()
                .map(Value::Array)
        }
        AuthOperation::SessionRevoke {
            session_id,
            revoked_at,
        } => {
            if let Some(row) = get_row(session, SESSIONS, &session_id).await? {
                if nullable(&row, "revoked_at")?.is_none() {
                    revoke_session(session, row, &revoked_at).await?;
                    return Ok(Value::Bool(true));
                }
            }
            Ok(Value::Bool(false))
        }
        AuthOperation::SessionRevokeAllExcept {
            current_session_id,
            revoked_at,
        } => {
            let prefix = active_prefix(&session.catalog, SESSIONS);
            let candidates = scan_index(session, prefix, None, SESSIONS).await?;
            let mut ids = Vec::new();
            for row in candidates {
                let id = text(&row, "session_id")?;
                if id != current_session_id {
                    ids.push(id);
                    revoke_session(session, row, &revoked_at).await?;
                }
            }
            Ok(json!(ids))
        }
        AuthOperation::SessionSetLastConnected {
            session_id,
            last_connected_at,
        } => {
            if let Some(mut row) = get_row(session, SESSIONS, &session_id).await? {
                if nullable(&row, "revoked_at")?.is_none() {
                    row.insert("last_connected_at".into(), text_cell(&last_connected_at));
                    write_row(&mut session.transaction, &session.catalog, SESSIONS, row).await?;
                }
            }
            Ok(json!({"written":true}))
        }
        AuthOperation::PairingCreate { link } => {
            if get_row(session, LINKS, &link.id).await?.is_some()
                || credential_row(session, &link.credential).await?.is_some()
            {
                return Err(Error::DuplicateRecord);
            }
            let row = link_row(link)?;
            write_row(
                &mut session.transaction,
                &session.catalog,
                LINKS,
                row.clone(),
            )
            .await?;
            add_indexes(&mut session.transaction, &session.catalog, LINKS, &row).await?;
            Ok(json!({"written":true}))
        }
        AuthOperation::PairingConsume {
            credential,
            proof_key_thumbprint,
            requested_scopes,
            required_scopes,
            mcp_approval_subject,
            consumed_at,
            now,
        } => {
            let Some(row) = credential_row(session, &credential).await? else {
                return Ok(Value::Null);
            };
            let subject = text(&row, "subject")?;
            // SQLite's default LIKE is ASCII case-insensitive; '=' remains case-sensitive.
            let is_approval = subject
                .get(..13)
                .is_some_and(|prefix| prefix.eq_ignore_ascii_case("mcp-approval:"));
            let proof = nullable(&row, "proof_key_thumbprint")?;
            if !is_active(LINKS, &row)?
                || text(&row, "expires_at")? <= now
                || (is_approval && mcp_approval_subject.as_deref() != Some(subject.as_str()))
                || (proof.is_some() && proof != proof_key_thumbprint)
            {
                return Ok(Value::Null);
            }
            let granted: Value = serde_json::from_str(&text(&row, "scopes")?)
                .map_err(|_| invalid("malformed authentication scopes JSON"))?;
            let granted = granted
                .as_array()
                .ok_or_else(|| invalid("authentication scopes must be an array"))?;
            if requested_scopes.as_ref().is_some_and(|requested| {
                !requested
                    .iter()
                    .any(|scope| granted.iter().any(|value| value.as_str() == Some(scope)))
            }) || required_scopes
                .iter()
                .any(|scope| !granted.iter().any(|value| value.as_str() == Some(scope)))
            {
                return Ok(Value::Null);
            }
            let mut updated = row.clone();
            updated.insert("consumed_at".into(), text_cell(&consumed_at));
            replace_row(session, LINKS, &row, updated.clone()).await?;
            link_output(&updated)
        }
        AuthOperation::PairingListActive { now } => {
            let prefix = active_prefix(&session.catalog, LINKS);
            let mut rows: Vec<_> = scan_index(session, prefix, Some(&now), LINKS)
                .await?
                .into_iter()
                .map(|row| Ok((text(&row, "created_at")?, text(&row, "id")?, row)))
                .collect::<Result<_>>()?;
            rows.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| b.1.cmp(&a.1)));
            rows.into_iter()
                .map(|(_, _, row)| link_output(&row))
                .collect::<Result<Vec<_>>>()
                .map(Value::Array)
        }
        AuthOperation::PairingRevoke { id, revoked_at } => {
            if let Some(row) = get_row(session, LINKS, &id).await? {
                if is_active(LINKS, &row)? {
                    let mut updated = row.clone();
                    updated.insert("revoked_at".into(), text_cell(&revoked_at));
                    replace_row(session, LINKS, &row, updated).await?;
                    return Ok(Value::Bool(true));
                }
            }
            Ok(Value::Bool(false))
        }
        AuthOperation::PairingGet { credential } => credential_row(session, &credential)
            .await?
            .map(|row| link_output(&row))
            .transpose()
            .map(|row| row.unwrap_or(Value::Null)),
    }
}

#[cfg(test)]
#[path = "redb_auth_rows_tests.rs"]
mod tests;
