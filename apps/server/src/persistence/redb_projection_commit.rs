//! The projection mutation boundary used by the real EventSink.
//! Source rows retain their SQLite identities. Derived indexes are generation
//! local, prepared in bounded transactions, and protected from generic writes.

use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

use crate::application::{
    ApplicationCatalog, ApplicationTransactions, Session, decode_catalog, decode_row, row_prefix,
    schema_key, table,
};
use crate::application_json::{field, object_fields, render_fields, set_field};
use crate::application_rows::{
    delete_row as delete_application_row, int_cell, integer, managed_marker, nullable,
    nullable_cell, read_row, text, text_cell, validate_table, write_row as write_application_row,
};
use crate::backend::Transaction;
use crate::{Error, Result, Store, request_digest, store_key};

const THREADS: &str = "orchestration_v2_projection_threads";
const RUNS: &str = "orchestration_v2_projection_runs";
const ATTEMPTS: &str = "orchestration_v2_projection_run_attempts";
const NODES: &str = "orchestration_v2_projection_nodes";
const SUBAGENTS: &str = "orchestration_v2_projection_subagents";
const SESSIONS: &str = "orchestration_v2_projection_provider_sessions";
const BINDINGS: &str = "orchestration_v2_projection_provider_session_bindings";
const PROVIDER_THREADS: &str = "orchestration_v2_projection_provider_threads";
const TURNS: &str = "orchestration_v2_projection_provider_turns";
const REQUESTS: &str = "orchestration_v2_projection_runtime_requests";
const MESSAGES: &str = "orchestration_v2_projection_messages";
const PLANS: &str = "orchestration_v2_projection_plans";
const ITEMS: &str = "orchestration_v2_projection_turn_items";
const SCOPES: &str = "orchestration_v2_projection_checkpoint_scopes";
const CHECKPOINTS: &str = "orchestration_v2_projection_checkpoints";
const HANDOFFS: &str = "orchestration_v2_projection_context_handoffs";
const TRANSFERS: &str = "orchestration_v2_projection_context_transfers";
const METADATA: &str = "orchestration_v2_projection_metadata";
const OWNER: &str = "projection-commit-v1";
const PREPARE_ROWS: usize = 32;
const MAX_SAFE_INTEGER: u64 = (1 << 53) - 1;

#[path = "redb_projection_lookup.rs"]
mod lookup;
pub use lookup::ProjectionLookup;
#[path = "redb_projection_control.rs"]
mod control;
pub use control::ProjectionControlQuery;
#[path = "redb_projection_shell.rs"]
mod shell;
pub use shell::ProjectionShellQuery;
#[path = "redb_projection_recovery.rs"]
mod recovery;
pub use recovery::ProjectionRecoveryQuery;
#[path = "redb_projection_read_index.rs"]
pub(crate) mod read_index;

async fn write_row(
    tx: &mut Transaction,
    catalog: &ApplicationCatalog,
    name: &str,
    row: Map<String, Value>,
) -> Result<()> {
    let spec = spec(name);
    let primary = spec
        .primary
        .iter()
        .map(|column| row.get(*column).cloned().ok_or(Error::IncompatibleStore))
        .collect::<Result<Vec<_>>>()?;
    let previous = read_row(tx, catalog, name, &primary).await?;
    if name == MESSAGES {
        if let Some(rowid) = crate::application_message_rowids::replace(
            tx,
            catalog,
            previous.as_ref().map(|old| &old.row),
            Some(&row),
        )
        .await?
        {
            crate::application_search::replace_message(
                tx,
                catalog,
                previous.as_ref().map(|old| &old.row),
                Some(&row),
                rowid,
            )
            .await?;
        }
    }
    read_index::replace(
        tx,
        catalog,
        name,
        previous.as_ref().map(|old| &old.row),
        Some(&row),
    )
    .await?;
    write_application_row(tx, catalog, name, row).await
}

async fn delete_row(
    tx: &mut Transaction,
    catalog: &ApplicationCatalog,
    name: &str,
    primary: &[Value],
) -> Result<bool> {
    let previous = read_row(tx, catalog, name, primary).await?;
    if let Some(previous) = previous {
        if name == MESSAGES {
            if let Some(rowid) =
                crate::application_message_rowids::replace(tx, catalog, Some(&previous.row), None)
                    .await?
            {
                crate::application_search::replace_message(
                    tx,
                    catalog,
                    Some(&previous.row),
                    None,
                    rowid,
                )
                .await?;
            }
        }
        read_index::replace(tx, catalog, name, Some(&previous.row), None).await?;
    }
    delete_application_row(tx, catalog, name, primary).await
}

/// Retention metadata uses the same row owner while preserving schema_version
/// on conflict, as the original maintenance SQL does.
pub(crate) async fn write_maintenance_metadata(
    tx: &mut Transaction,
    catalog: &ApplicationCatalog,
    name: &str,
    sequence: u64,
    schema_version: i64,
    updated_at: &str,
) -> Result<()> {
    let existing = read_row(tx, catalog, METADATA, &[text_cell(name)]).await?;
    let version = existing
        .as_ref()
        .map(|r| integer(&r.row, "schema_version"))
        .transpose()?
        .unwrap_or(schema_version);
    write_row(
        tx,
        catalog,
        METADATA,
        Map::from_iter([
            ("projection_name".into(), text_cell(name)),
            ("schema_version".into(), int_cell(version)),
            ("last_sequence".into(), int_cell(sequence as i64)),
            ("updated_at".into(), text_cell(updated_at)),
        ]),
    )
    .await
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectionApply {
    pub event_type: String,
    pub thread_id: String,
    pub occurred_at: String,
    pub payload_json: String,
    pub normalization: Option<ProjectionNormalizedPayload>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectionNormalizedPayload {
    pub expected_payload_json: String,
    pub payload_json: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectionNormalization {
    pub kind: String,
    pub payload_json: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectionRead {
    pub kind: String,
    pub thread_id: String,
    pub record_id: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ProjectionGuard {
    NativeThreadOwned {
        driver: String,
        provider_instance_id: String,
        native_id: Option<String>,
        app_thread_id: Option<String>,
    },
    RunCurrent {
        thread_id: String,
        run_id: String,
        active_attempt_id: String,
        status: String,
    },
    ProviderThreadOwner {
        provider_thread_id: String,
        run_id: String,
        active_attempt_id: String,
        expected_last_run_ordinal: i64,
    },
    ProviderThreadUnchanged {
        provider_thread_id: String,
        expected_json: String,
    },
}

#[derive(Clone, Copy)]
enum Kind {
    Text,
    NullableText,
    Integer,
    NullableInteger,
}
#[derive(Clone, Copy)]
enum Source {
    Payload(&'static str),
    EventThread,
    PayloadJson,
    Boolean(&'static str),
}
use Kind::{Integer as I, NullableInteger as NI, NullableText as NT, Text as T};
use Source::{Boolean as B, EventThread as E, Payload as P, PayloadJson as J};

struct TableSpec {
    name: &'static str,
    primary: &'static [&'static str],
    columns: &'static [(&'static str, Kind)],
    fields: &'static [(&'static str, Source)],
    unique: &'static [&'static str],
}

const SPECS: &[TableSpec] = &[
    TableSpec {
        name: THREADS,
        primary: &["thread_id"],
        unique: &[],
        columns: &[
            ("thread_id", T),
            ("project_id", T),
            ("title", T),
            ("default_provider", T),
            ("runtime_mode", T),
            ("interaction_mode", T),
            ("active_provider_thread_id", NT),
            ("created_at", T),
            ("updated_at", T),
            ("archived_at", NT),
            ("deleted_at", NT),
            ("payload_json", T),
            ("provider_instance_id", NT),
        ],
        fields: &[
            ("thread_id", P("id")),
            ("project_id", P("projectId")),
            ("title", P("title")),
            ("default_provider", P("providerInstanceId")),
            ("provider_instance_id", P("providerInstanceId")),
            ("runtime_mode", P("runtimeMode")),
            ("interaction_mode", P("interactionMode")),
            ("active_provider_thread_id", P("activeProviderThreadId")),
            ("created_at", P("createdAt")),
            ("updated_at", P("updatedAt")),
            ("archived_at", P("archivedAt")),
            ("deleted_at", P("deletedAt")),
            ("payload_json", J),
        ],
    },
    TableSpec {
        name: RUNS,
        primary: &["run_id"],
        unique: &["thread_id", "ordinal"],
        columns: &[
            ("run_id", T),
            ("thread_id", T),
            ("ordinal", I),
            ("provider", T),
            ("provider_thread_id", NT),
            ("status", T),
            ("requested_at", T),
            ("completed_at", NT),
            ("payload_json", T),
            ("provider_instance_id", NT),
        ],
        fields: &[
            ("run_id", P("id")),
            ("thread_id", P("threadId")),
            ("ordinal", P("ordinal")),
            ("provider", P("providerInstanceId")),
            ("provider_instance_id", P("providerInstanceId")),
            ("provider_thread_id", P("providerThreadId")),
            ("status", P("status")),
            ("requested_at", P("requestedAt")),
            ("completed_at", P("completedAt")),
            ("payload_json", J),
        ],
    },
    TableSpec {
        name: ATTEMPTS,
        primary: &["attempt_id"],
        unique: &["run_id", "attempt_ordinal"],
        columns: &[
            ("attempt_id", T),
            ("thread_id", T),
            ("run_id", T),
            ("attempt_ordinal", I),
            ("root_node_id", T),
            ("provider", T),
            ("provider_thread_id", T),
            ("provider_turn_id", NT),
            ("status", T),
            ("payload_json", T),
            ("provider_instance_id", NT),
        ],
        fields: &[
            ("attempt_id", P("id")),
            ("thread_id", E),
            ("run_id", P("runId")),
            ("attempt_ordinal", P("attemptOrdinal")),
            ("root_node_id", P("rootNodeId")),
            ("provider", P("providerInstanceId")),
            ("provider_instance_id", P("providerInstanceId")),
            ("provider_thread_id", P("providerThreadId")),
            ("provider_turn_id", P("providerTurnId")),
            ("status", P("status")),
            ("payload_json", J),
        ],
    },
    TableSpec {
        name: NODES,
        primary: &["node_id"],
        unique: &[],
        columns: &[
            ("node_id", T),
            ("thread_id", T),
            ("run_id", NT),
            ("parent_node_id", NT),
            ("root_node_id", T),
            ("kind", T),
            ("status", T),
            ("provider_thread_id", NT),
            ("provider_turn_id", NT),
            ("runtime_request_id", NT),
            ("checkpoint_scope_id", NT),
            ("started_at", NT),
            ("completed_at", NT),
            ("payload_json", T),
        ],
        fields: &[
            ("node_id", P("id")),
            ("thread_id", P("threadId")),
            ("run_id", P("runId")),
            ("parent_node_id", P("parentNodeId")),
            ("root_node_id", P("rootNodeId")),
            ("kind", P("kind")),
            ("status", P("status")),
            ("provider_thread_id", P("providerThreadId")),
            ("provider_turn_id", P("providerTurnId")),
            ("started_at", P("startedAt")),
            ("completed_at", P("completedAt")),
            ("payload_json", J),
        ],
    },
    TableSpec {
        name: SUBAGENTS,
        primary: &["subagent_id"],
        unique: &[],
        columns: &[
            ("subagent_id", T),
            ("thread_id", T),
            ("run_id", NT),
            ("parent_node_id", T),
            ("provider", T),
            ("provider_thread_id", NT),
            ("child_thread_id", NT),
            ("origin", T),
            ("status", T),
            ("started_at", NT),
            ("completed_at", NT),
            ("updated_at", T),
            ("payload_json", T),
            ("driver", NT),
            ("provider_instance_id", NT),
            ("completion_delivery_state", NT),
        ],
        fields: &[
            ("subagent_id", P("id")),
            ("thread_id", P("threadId")),
            ("run_id", P("runId")),
            ("parent_node_id", P("parentNodeId")),
            ("provider", P("providerInstanceId")),
            ("driver", P("driver")),
            ("provider_instance_id", P("providerInstanceId")),
            ("provider_thread_id", P("providerThreadId")),
            ("child_thread_id", P("childThreadId")),
            ("origin", P("origin")),
            ("status", P("status")),
            ("started_at", P("startedAt")),
            ("completed_at", P("completedAt")),
            ("updated_at", P("updatedAt")),
            ("payload_json", J),
        ],
    },
    TableSpec {
        name: SESSIONS,
        primary: &["provider_session_id"],
        unique: &[],
        columns: &[
            ("provider_session_id", T),
            ("thread_id", NT),
            ("provider", T),
            ("status", T),
            ("model", NT),
            ("updated_at", T),
            ("payload_json", T),
            ("driver", NT),
            ("provider_instance_id", NT),
        ],
        fields: &[
            ("provider_session_id", P("id")),
            ("thread_id", E),
            ("provider", P("providerInstanceId")),
            ("driver", P("driver")),
            ("provider_instance_id", P("providerInstanceId")),
            ("status", P("status")),
            ("updated_at", P("updatedAt")),
            ("payload_json", J),
        ],
    },
    TableSpec {
        name: BINDINGS,
        primary: &["provider_session_id", "thread_id"],
        unique: &[],
        columns: &[("provider_session_id", T), ("thread_id", T)],
        fields: &[],
    },
    TableSpec {
        name: PROVIDER_THREADS,
        primary: &["provider_thread_id"],
        unique: &[],
        columns: &[
            ("provider_thread_id", T),
            ("thread_id", NT),
            ("owner_node_id", NT),
            ("provider", T),
            ("provider_session_id", NT),
            ("status", T),
            ("first_run_ordinal", NI),
            ("last_run_ordinal", NI),
            ("updated_at", T),
            ("payload_json", T),
            ("driver", NT),
            ("provider_instance_id", NT),
        ],
        fields: &[
            ("provider_thread_id", P("id")),
            ("thread_id", P("appThreadId")),
            ("owner_node_id", P("ownerNodeId")),
            ("provider", P("providerInstanceId")),
            ("driver", P("driver")),
            ("provider_instance_id", P("providerInstanceId")),
            ("provider_session_id", P("providerSessionId")),
            ("status", P("status")),
            ("first_run_ordinal", P("firstRunOrdinal")),
            ("last_run_ordinal", P("lastRunOrdinal")),
            ("updated_at", P("updatedAt")),
            ("payload_json", J),
        ],
    },
    TableSpec {
        name: TURNS,
        primary: &["provider_turn_id"],
        unique: &["provider_thread_id", "ordinal"],
        columns: &[
            ("provider_turn_id", T),
            ("thread_id", T),
            ("provider_thread_id", T),
            ("node_id", T),
            ("run_attempt_id", NT),
            ("ordinal", I),
            ("status", T),
            ("started_at", NT),
            ("completed_at", NT),
            ("payload_json", T),
        ],
        fields: &[
            ("provider_turn_id", P("id")),
            ("thread_id", E),
            ("provider_thread_id", P("providerThreadId")),
            ("node_id", P("nodeId")),
            ("run_attempt_id", P("runAttemptId")),
            ("ordinal", P("ordinal")),
            ("status", P("status")),
            ("started_at", P("startedAt")),
            ("completed_at", P("completedAt")),
            ("payload_json", J),
        ],
    },
    TableSpec {
        name: REQUESTS,
        primary: &["runtime_request_id"],
        unique: &[],
        columns: &[
            ("runtime_request_id", T),
            ("thread_id", T),
            ("node_id", T),
            ("provider_turn_id", NT),
            ("kind", T),
            ("status", T),
            ("created_at", T),
            ("resolved_at", NT),
            ("payload_json", T),
        ],
        fields: &[
            ("runtime_request_id", P("id")),
            ("thread_id", E),
            ("node_id", P("nodeId")),
            ("provider_turn_id", P("providerTurnId")),
            ("kind", P("kind")),
            ("status", P("status")),
            ("created_at", P("createdAt")),
            ("resolved_at", P("resolvedAt")),
            ("payload_json", J),
        ],
    },
    TableSpec {
        name: MESSAGES,
        primary: &["message_id"],
        unique: &[],
        columns: &[
            ("message_id", T),
            ("thread_id", T),
            ("run_id", NT),
            ("node_id", NT),
            ("role", T),
            ("streaming", I),
            ("created_at", T),
            ("updated_at", T),
            ("payload_json", T),
        ],
        fields: &[
            ("message_id", P("id")),
            ("thread_id", P("threadId")),
            ("run_id", P("runId")),
            ("node_id", P("nodeId")),
            ("role", P("role")),
            ("streaming", B("streaming")),
            ("created_at", P("createdAt")),
            ("updated_at", P("updatedAt")),
            ("payload_json", J),
        ],
    },
    TableSpec {
        name: PLANS,
        primary: &["plan_id"],
        unique: &[],
        columns: &[
            ("plan_id", T),
            ("thread_id", T),
            ("run_id", NT),
            ("node_id", T),
            ("kind", T),
            ("status", T),
            ("payload_json", T),
        ],
        fields: &[
            ("plan_id", P("id")),
            ("thread_id", P("threadId")),
            ("run_id", P("runId")),
            ("node_id", P("nodeId")),
            ("kind", P("kind")),
            ("status", P("status")),
            ("payload_json", J),
        ],
    },
    TableSpec {
        name: ITEMS,
        primary: &["turn_item_id"],
        unique: &[],
        columns: &[
            ("turn_item_id", T),
            ("thread_id", T),
            ("run_id", NT),
            ("node_id", NT),
            ("provider_thread_id", NT),
            ("provider_turn_id", NT),
            ("parent_item_id", NT),
            ("ordinal", I),
            ("type", T),
            ("status", T),
            ("updated_at", T),
            ("payload_json", T),
        ],
        fields: &[
            ("turn_item_id", P("id")),
            ("thread_id", P("threadId")),
            ("run_id", P("runId")),
            ("node_id", P("nodeId")),
            ("provider_thread_id", P("providerThreadId")),
            ("provider_turn_id", P("providerTurnId")),
            ("parent_item_id", P("parentItemId")),
            ("ordinal", P("ordinal")),
            ("type", P("type")),
            ("status", P("status")),
            ("updated_at", P("updatedAt")),
            ("payload_json", J),
        ],
    },
    TableSpec {
        name: SCOPES,
        primary: &["scope_id"],
        unique: &[],
        columns: &[
            ("scope_id", T),
            ("thread_id", T),
            ("run_id", NT),
            ("node_id", T),
            ("parent_scope_id", NT),
            ("provider_thread_id", NT),
            ("kind", T),
            ("ordinal_within_parent", I),
            ("advances_app_run_count", I),
            ("created_at", T),
            ("payload_json", T),
        ],
        fields: &[
            ("scope_id", P("id")),
            ("thread_id", P("threadId")),
            ("run_id", P("runId")),
            ("node_id", P("nodeId")),
            ("provider_thread_id", P("providerThreadId")),
            ("kind", P("kind")),
            ("ordinal_within_parent", P("ordinalWithinParent")),
            ("advances_app_run_count", B("advancesAppRunCount")),
            ("created_at", P("createdAt")),
            ("payload_json", J),
        ],
    },
    TableSpec {
        name: CHECKPOINTS,
        primary: &["checkpoint_id"],
        unique: &["scope_id", "ordinal_within_scope"],
        columns: &[
            ("checkpoint_id", T),
            ("thread_id", T),
            ("scope_id", T),
            ("run_id", NT),
            ("node_id", T),
            ("parent_checkpoint_id", NT),
            ("ordinal_within_scope", I),
            ("app_run_ordinal", NI),
            ("status", T),
            ("captured_at", T),
            ("payload_json", T),
        ],
        fields: &[
            ("checkpoint_id", P("id")),
            ("thread_id", P("threadId")),
            ("scope_id", P("scopeId")),
            ("run_id", P("runId")),
            ("node_id", P("nodeId")),
            ("ordinal_within_scope", P("ordinalWithinScope")),
            ("app_run_ordinal", P("appRunOrdinal")),
            ("status", P("status")),
            ("captured_at", P("capturedAt")),
            ("payload_json", J),
        ],
    },
    TableSpec {
        name: HANDOFFS,
        primary: &["context_handoff_id"],
        unique: &[],
        columns: &[
            ("context_handoff_id", T),
            ("thread_id", T),
            ("target_run_id", T),
            ("to_provider_thread_id", T),
            ("strategy", T),
            ("status", T),
            ("updated_at", T),
            ("payload_json", T),
        ],
        fields: &[
            ("context_handoff_id", P("id")),
            ("thread_id", P("threadId")),
            ("target_run_id", P("targetRunId")),
            ("to_provider_thread_id", P("toProviderThreadId")),
            ("strategy", P("strategy")),
            ("status", P("status")),
            ("updated_at", P("updatedAt")),
            ("payload_json", J),
        ],
    },
    TableSpec {
        name: TRANSFERS,
        primary: &["context_transfer_id"],
        unique: &[],
        columns: &[
            ("context_transfer_id", T),
            ("source_thread_id", T),
            ("target_thread_id", T),
            ("target_run_id", NT),
            ("type", T),
            ("status", T),
            ("source_provider", NT),
            ("target_provider", NT),
            ("updated_at", T),
            ("payload_json", T),
            ("source_provider_instance_id", NT),
            ("target_provider_instance_id", NT),
        ],
        fields: &[
            ("context_transfer_id", P("id")),
            ("source_thread_id", P("sourceThreadId")),
            ("target_thread_id", P("targetThreadId")),
            ("target_run_id", P("targetRunId")),
            ("type", P("type")),
            ("status", P("status")),
            ("updated_at", P("updatedAt")),
            ("payload_json", J),
        ],
    },
    TableSpec {
        name: METADATA,
        primary: &["projection_name"],
        unique: &[],
        columns: &[
            ("projection_name", T),
            ("schema_version", I),
            ("last_sequence", I),
            ("updated_at", T),
        ],
        fields: &[],
    },
];

fn invalid(message: &str) -> Error {
    Error::Invalid(message.into())
}
fn encode(value: &impl Serialize) -> Result<Vec<u8>> {
    serde_json::to_vec(value).map_err(crate::json_error)
}
fn decode<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    serde_json::from_slice(bytes).map_err(|_| Error::IncompatibleStore)
}
fn key(catalog: &ApplicationCatalog, suffix: &str) -> Vec<u8> {
    store_key(&format!(
        "application/generation/{}/projections/v1/{suffix}",
        catalog.generation
    ))
}
fn spec(name: &str) -> &'static TableSpec {
    SPECS
        .iter()
        .find(|spec| spec.name == name)
        .expect("declared projection table")
}
fn validate_schema(catalog: &ApplicationCatalog) -> Result<()> {
    for spec in SPECS {
        let columns: Vec<_> = spec.columns.iter().map(|(name, _)| *name).collect();
        validate_table(catalog, spec.name, &columns, spec.primary)?;
    }
    Ok(())
}
fn validate_stored_cells(spec: &TableSpec, row: &Map<String, Value>) -> Result<()> {
    for (name, kind) in spec.columns {
        match kind {
            T => {
                text(row, name)?;
            }
            NT => {
                nullable(row, name)?;
            }
            I => {
                integer(row, name)?;
            }
            NI if row.get(*name) == Some(&json!({"type":"null"})) => {}
            NI => {
                integer(row, name)?;
            }
        }
    }
    Ok(())
}
fn validate_stored_row(spec: &TableSpec, row: &Map<String, Value>) -> Result<()> {
    validate_stored_cells(spec, row)?;
    if row.contains_key("payload_json") {
        let payload: Value = serde_json::from_str(&text(row, "payload_json")?)
            .map_err(|_| Error::IncompatibleStore)?;
        if !payload.is_object() {
            return Err(Error::IncompatibleStore);
        }
    }
    Ok(())
}
fn payload_string<'a>(payload: &'a Map<String, Value>, name: &str) -> Result<&'a str> {
    payload
        .get(name)
        .and_then(Value::as_str)
        .ok_or_else(|| invalid("projection payload requires a text field"))
}
fn payload_cell(payload: &Map<String, Value>, name: &str, kind: Kind) -> Result<Value> {
    let value = payload.get(name).unwrap_or(&Value::Null);
    match kind {
        T => value
            .as_str()
            .map(text_cell)
            .ok_or_else(|| invalid("projection payload requires a text field")),
        NT if value.is_null() => Ok(nullable_cell(None)),
        NT => value
            .as_str()
            .map(text_cell)
            .ok_or_else(|| invalid("projection payload requires nullable text")),
        I => value
            .as_i64()
            .map(int_cell)
            .ok_or_else(|| invalid("projection payload requires an integer")),
        NI if value.is_null() => Ok(nullable_cell(None)),
        NI => value
            .as_i64()
            .map(int_cell)
            .ok_or_else(|| invalid("projection payload requires a nullable integer")),
    }
}

fn prefix_end(prefix: &[u8]) -> Vec<u8> {
    let mut end = prefix.to_vec();
    for byte in end.iter_mut().rev() {
        if *byte != u8::MAX {
            *byte += 1;
            return end;
        }
    }
    Vec::new()
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Preparation {
    version: u32,
    table_index: usize,
    after: Option<String>,
    indexed_rows: u64,
    ready: bool,
}
#[derive(Deserialize, Serialize)]
pub struct ProjectionPreparation {
    pub prepared: bool,
    pub indexed_rows: u64,
}
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct UniqueWitness {
    values: Vec<Value>,
    id: String,
}
#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct NativeOwner {
    identity: [String; 3],
    thread_id: Option<String>,
    provider_thread_id: String,
}
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct UnreadableNativeOwner {
    provider_thread_id: String,
    thread_id: Option<String>,
}
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct SearchDirty {
    revision: u64,
    last_sequence: u64,
}

async fn ready(session: &mut Session) -> Result<()> {
    let state: Preparation = decode(
        &session
            .transaction
            .get(&key(&session.catalog, "preparation"))
            .await?
            .ok_or_else(|| invalid("projection indexes are not prepared"))?,
    )?;
    if state.version != 1 || state.table_index > SPECS.len() {
        return Err(Error::IncompatibleStore);
    }
    if !state.ready || state.table_index != SPECS.len() {
        return Err(invalid("projection indexes are not prepared"));
    }
    for spec in SPECS {
        if session
            .transaction
            .get(&managed_marker(&session.catalog, spec.name))
            .await?
            .as_deref()
            .map(decode::<String>)
            .transpose()?
            .as_deref()
            != Some(OWNER)
        {
            return Err(Error::IncompatibleStore);
        }
    }
    Ok(())
}

fn unique_location(
    catalog: &ApplicationCatalog,
    spec: &TableSpec,
    row: &Map<String, Value>,
) -> Result<(Vec<u8>, Vec<Value>)> {
    let values = spec
        .unique
        .iter()
        .map(|column| row.get(*column).cloned().ok_or(Error::IncompatibleStore))
        .collect::<Result<Vec<_>>>()?;
    Ok((
        key(
            catalog,
            &format!("unique/{}/{}", spec.name, request_digest(&json!(values))),
        ),
        values,
    ))
}

async fn index_unique(
    tx: &mut Transaction,
    catalog: &ApplicationCatalog,
    spec: &TableSpec,
    row: &Map<String, Value>,
) -> Result<()> {
    if spec.unique.is_empty() {
        return Ok(());
    }
    let (location, values) = unique_location(catalog, spec, row)?;
    let id = text(row, spec.primary[0])?;
    if let Some(bytes) = tx.get(&location).await? {
        let found: UniqueWitness = decode(&bytes)?;
        if found.values != values {
            return Err(Error::IncompatibleStore);
        }
        if found.id != id {
            return Err(Error::DuplicateRecord);
        }
    }
    tx.put(location, encode(&UniqueWitness { values, id })?)
        .await
}

fn native_owner(row: &Map<String, Value>) -> Result<Option<NativeOwner>> {
    let Some(driver) = nullable(row, "driver")? else {
        return Ok(None);
    };
    let Some(instance) = nullable(row, "provider_instance_id")? else {
        return Ok(None);
    };
    let payload: Value =
        serde_json::from_str(&text(row, "payload_json")?).map_err(|_| Error::IncompatibleStore)?;
    let Some(reference) = payload
        .get("nativeThreadRef")
        .filter(|value| !value.is_null())
    else {
        return Ok(None);
    };
    let native = match reference.get("nativeId") {
        Some(Value::Null) => return Ok(None),
        Some(Value::String(value)) => value,
        _ => return Err(Error::IncompatibleStore),
    };
    Ok(Some(NativeOwner {
        identity: [driver, instance, native.into()],
        thread_id: nullable(row, "thread_id")?,
        provider_thread_id: text(row, "provider_thread_id")?,
    }))
}
fn native_prefix(catalog: &ApplicationCatalog, identity: &[String; 3]) -> Vec<u8> {
    key(
        catalog,
        &format!("native/{}/", request_digest(&json!(identity))),
    )
}
fn owner_prefix(catalog: &ApplicationCatalog, owner: &NativeOwner) -> Vec<u8> {
    let mut prefix = native_prefix(catalog, &owner.identity);
    match &owner.thread_id {
        None => prefix.extend_from_slice(b"N/"),
        Some(thread) => {
            prefix.push(b'S');
            let encoded: String = thread.bytes().map(|byte| format!("{byte:02x}")).collect();
            prefix.extend_from_slice(encoded.as_bytes());
            prefix.push(b'/');
        }
    }
    prefix
}
fn owner_key(catalog: &ApplicationCatalog, owner: &NativeOwner) -> Vec<u8> {
    let mut location = owner_prefix(catalog, owner);
    location.extend_from_slice(request_digest(&json!(owner.provider_thread_id)).as_bytes());
    location
}
fn unreadable_owner_prefix(catalog: &ApplicationCatalog, driver: &str, instance: &str) -> Vec<u8> {
    key(
        catalog,
        &format!(
            "native-unreadable/{}/",
            request_digest(&json!([driver, instance]))
        ),
    )
}

async fn prepare_native_owner(
    tx: &mut Transaction,
    catalog: &ApplicationCatalog,
    row: &Map<String, Value>,
) -> Result<()> {
    match native_owner(row) {
        Ok(_) => index_native_owner(tx, catalog, row).await,
        Err(Error::IncompatibleStore) => {
            // Historical malformed payloads remain auditable. Keep a separate
            // owner witness so their unknown native identity cannot be treated
            // as permission to claim a potentially conflicting thread.
            let driver = nullable(row, "driver")?.ok_or(Error::IncompatibleStore)?;
            let instance =
                nullable(row, "provider_instance_id")?.ok_or(Error::IncompatibleStore)?;
            let id = text(row, "provider_thread_id")?;
            let mut location = unreadable_owner_prefix(catalog, &driver, &instance);
            location.extend_from_slice(request_digest(&json!(id)).as_bytes());
            tx.insert(
                location,
                encode(&UnreadableNativeOwner {
                    provider_thread_id: id,
                    thread_id: nullable(row, "thread_id")?,
                })?,
            )
            .await
        }
        Err(error) => Err(error),
    }
}

async fn check_unreadable_owners(
    tx: &mut Transaction,
    catalog: &ApplicationCatalog,
    driver: &str,
    instance: &str,
    app_thread_id: &Option<String>,
) -> Result<()> {
    let prefix = unreadable_owner_prefix(catalog, driver, instance);
    let end = prefix_end(&prefix);
    let mut cursor = prefix;
    loop {
        let rows = tx.scan(&cursor, &end, 128).await?;
        let count = rows.len();
        for (location, bytes) in rows {
            let witness: UnreadableNativeOwner = decode(&bytes)?;
            let id = &witness.provider_thread_id;
            let mut expected = unreadable_owner_prefix(catalog, driver, instance);
            expected.extend_from_slice(request_digest(&json!(id)).as_bytes());
            if expected != location || &witness.thread_id != app_thread_id {
                return Err(Error::IncompatibleStore);
            }
            cursor = location;
            cursor.push(0);
        }
        if count < 128 {
            return Ok(());
        }
    }
}
async fn index_native_owner(
    tx: &mut Transaction,
    catalog: &ApplicationCatalog,
    row: &Map<String, Value>,
) -> Result<()> {
    let Some(owner) = native_owner(row)? else {
        return Ok(());
    };
    let location = owner_key(catalog, &owner);
    if let Some(bytes) = tx.get(&location).await? {
        let found: NativeOwner = decode(&bytes)?;
        if found.identity != owner.identity
            || found.thread_id != owner.thread_id
            || found.provider_thread_id != owner.provider_thread_id
        {
            return Err(Error::IncompatibleStore);
        }
    }
    tx.put(location, encode(&owner)?).await
}
async fn replace_indexes(
    tx: &mut Transaction,
    catalog: &ApplicationCatalog,
    spec: &TableSpec,
    old: Option<&Map<String, Value>>,
    row: &Map<String, Value>,
) -> Result<()> {
    index_unique(tx, catalog, spec, row).await?;
    if let Some(old) = old.filter(|_| !spec.unique.is_empty()) {
        let (previous, values) = unique_location(catalog, spec, old)?;
        if previous != unique_location(catalog, spec, row)?.0 {
            let witness: UniqueWitness =
                decode(&tx.get(&previous).await?.ok_or(Error::IncompatibleStore)?)?;
            if witness.values != values || witness.id != text(old, spec.primary[0])? {
                return Err(Error::IncompatibleStore);
            }
            tx.delete(previous).await?;
        }
    }
    if spec.name == PROVIDER_THREADS {
        if let Some(old) = old {
            if let Some(owner) = native_owner(old)? {
                let location = owner_key(catalog, &owner);
                let found: NativeOwner =
                    decode(&tx.get(&location).await?.ok_or(Error::IncompatibleStore)?)?;
                if found.identity != owner.identity
                    || found.thread_id != owner.thread_id
                    || found.provider_thread_id != owner.provider_thread_id
                {
                    return Err(Error::IncompatibleStore);
                }
                tx.delete(location).await?;
            }
        }
        index_native_owner(tx, catalog, row).await?;
    }
    Ok(())
}

impl Store {
    pub async fn prepare_application_projections(&self) -> Result<ProjectionPreparation> {
        let mut limit = PREPARE_ROWS;
        loop {
            match self.prepare_projection_batch(limit).await {
                Err(Error::ResourceLimit(_)) if limit > 1 => limit = (limit / 2).max(1),
                result => return result,
            }
        }
    }

    async fn prepare_projection_batch(&self, limit: usize) -> Result<ProjectionPreparation> {
        let mut tx = self
            .db
            .begin_reserved(Duration::from_secs(5), Duration::from_secs(300))
            .await?;
        let catalog = decode_catalog(
            &tx.get(&schema_key())
                .await?
                .ok_or_else(|| invalid("application schema is not initialized"))?,
        )?;
        validate_schema(&catalog)?;
        let state = tx.get(&key(&catalog, "preparation")).await?;
        let mut state = if let Some(bytes) = state {
            let state: Preparation = decode(&bytes)?;
            if state.version != 1
                || state.table_index > SPECS.len()
                || (state.ready != (state.table_index == SPECS.len()))
            {
                return Err(Error::IncompatibleStore);
            }
            for spec in SPECS {
                if tx
                    .get(&managed_marker(&catalog, spec.name))
                    .await?
                    .as_deref()
                    .map(decode::<String>)
                    .transpose()?
                    .as_deref()
                    != Some(OWNER)
                {
                    return Err(Error::IncompatibleStore);
                }
            }
            state
        } else {
            for spec in SPECS {
                let marker = managed_marker(&catalog, spec.name);
                if tx.get(&marker).await?.is_some() {
                    return Err(Error::IncompatibleStore);
                }
                tx.put(marker, encode(&OWNER)?).await?;
            }
            // SQLite FTS shadow tables are deliberately excluded from imports.
            // A future native search reader must rebuild from these source rows.
            tx.put(
                key(&catalog, "search-dirty"),
                encode(&SearchDirty {
                    revision: 0,
                    last_sequence: 0,
                })?,
            )
            .await?;
            Preparation {
                version: 1,
                table_index: 0,
                after: None,
                indexed_rows: 0,
                ready: false,
            }
        };
        if state.ready {
            tx.rollback().await?;
            return Ok(ProjectionPreparation {
                prepared: true,
                indexed_rows: state.indexed_rows,
            });
        }
        let spec = &SPECS[state.table_index];
        let prefix = row_prefix(&catalog.generation, spec.name)?;
        let mut start = prefix.clone();
        if let Some(after) = &state.after {
            if after.len() != 64
                || !after
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            {
                return Err(Error::IncompatibleStore);
            }
            start.extend_from_slice(after.as_bytes());
            start.push(0);
        }
        let rows = tx.scan(&start, &prefix_end(&prefix), limit).await?;
        let count = rows.len();
        for (encoded, bytes) in rows {
            let row = decode_row(&catalog, table(&catalog, spec.name)?, &encoded, &bytes)?.row;
            validate_stored_cells(spec, &row)?;
            index_unique(&mut tx, &catalog, spec, &row).await?;
            if spec.name == PROVIDER_THREADS {
                prepare_native_owner(&mut tx, &catalog, &row).await?;
            }
            state.after = Some(
                std::str::from_utf8(
                    encoded
                        .strip_prefix(prefix.as_slice())
                        .ok_or(Error::IncompatibleStore)?,
                )
                .map_err(|_| Error::IncompatibleStore)?
                .to_owned(),
            );
            state.indexed_rows = state
                .indexed_rows
                .checked_add(1)
                .ok_or(Error::IncompatibleStore)?;
        }
        if count < limit {
            state.table_index += 1;
            state.after = None;
            state.ready = state.table_index == SPECS.len();
        }
        tx.put(key(&catalog, "preparation"), encode(&state)?)
            .await?;
        tx.commit().await?;
        Ok(ProjectionPreparation {
            prepared: state.ready,
            indexed_rows: state.indexed_rows,
        })
    }
}

fn thread_event(event: &str) -> bool {
    matches!(
        event,
        "thread.created"
            | "thread.archived"
            | "thread.unarchived"
            | "thread.deleted"
            | "thread.settled"
            | "thread.unsettled"
            | "thread.snoozed"
            | "thread.unsnoozed"
            | "thread.auto-settle-set"
            | "thread.pinned"
            | "thread.unpinned"
            | "thread.pin-reordered"
            | "thread.active-reordered"
            | "thread.tags-updated"
            | "thread.visited"
            | "thread.marked-unread"
            | "thread.metadata-updated"
            | "thread.pull-request-synced"
            | "thread.runtime-mode-updated"
            | "thread.interaction-mode-updated"
            | "thread.model-selection-updated"
            | "thread.provider-switched"
            | "thread.guest-share-updated"
    )
}

fn event_table(event: &str) -> Result<&'static str> {
    if thread_event(event) {
        return Ok(THREADS);
    }
    Ok(match event {
        "run.created" | "run.updated" => RUNS,
        "run-attempt.created" | "run-attempt.updated" => ATTEMPTS,
        "node.updated" => NODES,
        "subagent.updated" => SUBAGENTS,
        "provider-session.attached" | "provider-session.updated" => SESSIONS,
        "provider-thread.updated" => PROVIDER_THREADS,
        "provider-turn.updated" => TURNS,
        "runtime-request.updated" => REQUESTS,
        "message.updated" => MESSAGES,
        "plan.updated" => PLANS,
        "turn-item.updated" => ITEMS,
        "checkpoint-scope.created" => SCOPES,
        "checkpoint.captured" => CHECKPOINTS,
        "context-handoff.updated" => HANDOFFS,
        "context-transfer.created" | "context-transfer.updated" => TRANSFERS,
        "thread.pull-request-link-updated" => {
            return Err(invalid(
                "pull-request links require source-control domain normalization",
            ));
        }
        _ => return Err(invalid("unknown projection event type")),
    })
}

fn set_payload_field(payload: &str, name: &str, value: String) -> Result<String> {
    let mut fields = object_fields(payload)?.ok_or(Error::IncompatibleStore)?;
    set_field(&mut fields, name, value);
    Ok(render_fields(&fields))
}

fn keep_omitted_fields(payload: &str, previous: &str, names: &[&str]) -> Result<String> {
    let mut next = object_fields(payload)?.ok_or(Error::IncompatibleStore)?;
    let old = object_fields(previous)?.ok_or(Error::IncompatibleStore)?;
    let mut changed = false;
    for name in names {
        if field(&next, name).is_none() {
            if let Some(raw) = field(&old, name) {
                // json_extract passes scalar booleans to json_set as SQL integers.
                let raw = match raw {
                    "true" => "1",
                    "false" => "0",
                    value => value,
                };
                set_field(&mut next, name, raw.into());
                changed = true;
            }
        }
    }
    Ok(if changed {
        render_fields(&next)
    } else {
        payload.to_owned()
    })
}

fn keep_turn_usage(payload: &str, previous: &str) -> Result<String> {
    let mut next = object_fields(payload)?.ok_or(Error::IncompatibleStore)?;
    if field(&next, "tokenUsage").is_some() {
        return Ok(payload.into());
    }
    let old = object_fields(previous)?.ok_or(Error::IncompatibleStore)?;
    let Some(usage) = field(&old, "tokenUsage") else {
        return Ok(payload.into());
    };
    // ProviderTurn's schema encoder puts tokenUsage before these optional fields.
    // The SQL implementation decodes and re-encodes this one merged entity.
    let position = next
        .iter()
        .position(|entry| matches!(entry.name.as_str(), "turnTokenUsage" | "model"))
        .unwrap_or(next.len());
    set_field(&mut next, "tokenUsage", usage.into());
    let added = next.pop().ok_or(Error::IncompatibleStore)?;
    next.insert(position, added);
    Ok(render_fields(&next))
}

async fn upsert_projection(
    session: &mut Session,
    event: &ProjectionApply,
    payload: &Map<String, Value>,
    name: &str,
    normalized: Option<&str>,
) -> Result<()> {
    let spec = spec(name);
    let id = payload_string(payload, "id")?;
    let old = read_row(
        &mut session.transaction,
        &session.catalog,
        name,
        &[text_cell(id)],
    )
    .await?;
    if let Some(old) = &old {
        validate_stored_row(spec, &old.row)?;
    }
    let mut payload_json = event.payload_json.clone();
    if let Some(old) = &old {
        let previous = text(&old.row, "payload_json")?;
        payload_json = match name {
            RUNS => keep_omitted_fields(
                &payload_json,
                &previous,
                &["delegatedCompletion", "restartCancelledBackgroundWork"],
            )?,
            SUBAGENTS => keep_omitted_fields(&payload_json, &previous, &["completionDelivery"])?,
            TURNS => keep_turn_usage(&payload_json, normalized.unwrap_or(&previous))?,
            _ => payload_json,
        };
    }
    // Match INSERT/ON CONFLICT's omitted columns: null on creation and preserved
    // on update. Some legacy shadow columns intentionally have no payload mapping.
    let mut row = old.as_ref().map(|old| old.row.clone()).unwrap_or_else(|| {
        spec.columns
            .iter()
            .map(|(name, _)| ((*name).into(), nullable_cell(None)))
            .collect()
    });
    for (column, source) in spec.fields {
        let kind = spec
            .columns
            .iter()
            .find(|(name, _)| name == column)
            .ok_or(Error::IncompatibleStore)?
            .1;
        let value = match source {
            P(name) => payload_cell(payload, name, kind)?,
            E => text_cell(&event.thread_id),
            J => text_cell(&payload_json),
            B(name) => int_cell(i64::from(
                payload
                    .get(*name)
                    .and_then(Value::as_bool)
                    .ok_or_else(|| invalid("projection payload requires a boolean"))?,
            )),
        };
        row.insert((*column).into(), value);
    }
    if name == SUBAGENTS {
        let state = payload
            .get("completionDelivery")
            .and_then(|delivery| delivery.get("state"))
            .and_then(Value::as_str);
        let value = state
            .map(text_cell)
            .or_else(|| {
                old.as_ref()
                    .and_then(|old| old.row.get("completion_delivery_state"))
                    .cloned()
            })
            .unwrap_or_else(|| nullable_cell(None));
        row.insert("completion_delivery_state".into(), value);
    }
    validate_stored_row(spec, &row)?;
    replace_indexes(
        &mut session.transaction,
        &session.catalog,
        spec,
        old.as_ref().map(|old| &old.row),
        &row,
    )
    .await?;
    write_row(&mut session.transaction, &session.catalog, name, row).await?;

    if event.event_type == "provider-session.attached" {
        write_row(
            &mut session.transaction,
            &session.catalog,
            BINDINGS,
            Map::from_iter([
                ("provider_session_id".into(), text_cell(id)),
                ("thread_id".into(), text_cell(&event.thread_id)),
            ]),
        )
        .await?;
    }
    if name == PROVIDER_THREADS {
        let placeholder = queued_provider_thread_placeholder(payload);
        if let Some(thread_id) = payload
            .get("appThreadId")
            .and_then(Value::as_str)
            .filter(|_| !placeholder)
        {
            if let Some(mut thread) = read_row(
                &mut session.transaction,
                &session.catalog,
                THREADS,
                &[text_cell(thread_id)],
            )
            .await?
            {
                let updated_at = payload_string(payload, "updatedAt")?;
                let updated_payload = set_payload_field(
                    normalized
                        .ok_or_else(|| invalid("thread payload normalization is required"))?,
                    "activeProviderThreadId",
                    serde_json::to_string(id).map_err(crate::json_error)?,
                )?;
                let updated_payload = set_payload_field(
                    &updated_payload,
                    "updatedAt",
                    serde_json::to_string(updated_at).map_err(crate::json_error)?,
                )?;
                thread
                    .row
                    .insert("active_provider_thread_id".into(), text_cell(id));
                thread
                    .row
                    .insert("updated_at".into(), text_cell(updated_at));
                thread
                    .row
                    .insert("payload_json".into(), text_cell(&updated_payload));
                write_row(
                    &mut session.transaction,
                    &session.catalog,
                    THREADS,
                    thread.row,
                )
                .await?;
            }
        }
    }
    if name == MESSAGES {
        let location = key(&session.catalog, "search-dirty");
        let mut dirty: SearchDirty = decode(
            &session
                .transaction
                .get(&location)
                .await?
                .ok_or(Error::IncompatibleStore)?,
        )?;
        dirty.revision = dirty
            .revision
            .checked_add(1)
            .ok_or(Error::IncompatibleStore)?;
        session.transaction.put(location, encode(&dirty)?).await?;
    }
    Ok(())
}

fn queued_provider_thread_placeholder(payload: &Map<String, Value>) -> bool {
    payload.get("status").and_then(Value::as_str) == Some("not_loaded")
        && payload.get("firstRunOrdinal").is_some_and(Value::is_null)
        && payload.get("nativeThreadRef").is_some_and(Value::is_null)
        && payload.get("providerSessionId").is_some_and(Value::is_null)
}

async fn normalization_context(
    session: &mut Session,
    event: &ProjectionApply,
    payload: &Map<String, Value>,
) -> Result<Option<ProjectionNormalization>> {
    let (name, id, kind) = match event.event_type.as_str() {
        "provider-turn.updated" if !payload.contains_key("tokenUsage") => {
            (TURNS, payload_string(payload, "id")?, "provider_turn")
        }
        "provider-thread.updated" if !queued_provider_thread_placeholder(payload) => {
            let Some(id) = payload.get("appThreadId").and_then(Value::as_str) else {
                return Ok(None);
            };
            (THREADS, id, "thread")
        }
        _ => return Ok(None),
    };
    let Some(row) = read_row(
        &mut session.transaction,
        &session.catalog,
        name,
        &[text_cell(id)],
    )
    .await?
    else {
        return Ok(None);
    };
    Ok(Some(ProjectionNormalization {
        kind: kind.into(),
        payload_json: text(&row.row, "payload_json")?,
    }))
}

fn normalized_payload(
    event: &ProjectionApply,
    context: Option<&ProjectionNormalization>,
) -> Result<Option<String>> {
    match (context, event.normalization.as_ref()) {
        (None, None) => Ok(None),
        (Some(context), Some(normalized)) => {
            if context.payload_json != normalized.expected_payload_json {
                return Err(invalid("projection normalization source changed"));
            }
            let payload: Value = serde_json::from_str(&normalized.payload_json)
                .map_err(|_| invalid("invalid normalized projection payload"))?;
            if !payload.is_object() {
                return Err(invalid("normalized projection payload must be an object"));
            }
            Ok(Some(normalized.payload_json.clone()))
        }
        (Some(_), None) => Err(invalid("projection payload normalization is required")),
        (None, Some(_)) => Err(invalid("unexpected projection payload normalization")),
    }
}

async fn apply_event(session: &mut Session, event: ProjectionApply) -> Result<()> {
    session.transaction.require_write()?;
    ready(session).await?;
    let payload: Value = serde_json::from_str(&event.payload_json)
        .map_err(|_| invalid("invalid projection payload JSON"))?;
    let payload = payload
        .as_object()
        .ok_or_else(|| invalid("projection payload must be an object"))?;
    let context = normalization_context(session, &event, payload).await?;
    let normalized = normalized_payload(&event, context.as_ref())?;
    match event.event_type.as_str() {
        "provider-session.detached" => {
            let id = payload_string(payload, "providerSessionId")?;
            delete_row(
                &mut session.transaction,
                &session.catalog,
                BINDINGS,
                &[text_cell(id), text_cell(&event.thread_id)],
            )
            .await?;
        }
        "run.background-work-cancelled" => {
            let run_id = payload_string(payload, "runId")?;
            if let Some(mut run) = read_row(
                &mut session.transaction,
                &session.catalog,
                RUNS,
                &[text_cell(run_id)],
            )
            .await?
            {
                if text(&run.row, "thread_id")? == event.thread_id {
                    let fields =
                        object_fields(&event.payload_json)?.ok_or(Error::IncompatibleStore)?;
                    let work = field(&fields, "restartCancelledBackgroundWork")
                        .ok_or_else(|| invalid("cancelled run work is missing"))?;
                    let updated = set_payload_field(
                        &text(&run.row, "payload_json")?,
                        "restartCancelledBackgroundWork",
                        work.into(),
                    )?;
                    run.row.insert("payload_json".into(), text_cell(&updated));
                    write_row(&mut session.transaction, &session.catalog, RUNS, run.row).await?;
                }
            }
        }
        "checkpoint.rollback-requested" => {}
        _ => {
            upsert_projection(
                session,
                &event,
                payload,
                event_table(&event.event_type)?,
                normalized.as_deref(),
            )
            .await?
        }
    }
    if !thread_event(&event.event_type) {
        if let Some(mut thread) = read_row(
            &mut session.transaction,
            &session.catalog,
            THREADS,
            &[text_cell(&event.thread_id)],
        )
        .await?
        {
            let updated = set_payload_field(
                &text(&thread.row, "payload_json")?,
                "updatedAt",
                serde_json::to_string(&event.occurred_at).map_err(crate::json_error)?,
            )?;
            thread
                .row
                .insert("updated_at".into(), text_cell(&event.occurred_at));
            thread
                .row
                .insert("payload_json".into(), text_cell(&updated));
            write_row(
                &mut session.transaction,
                &session.catalog,
                THREADS,
                thread.row,
            )
            .await?;
        }
    }
    Ok(())
}

async fn checked_owner(
    tx: &mut Transaction,
    catalog: &ApplicationCatalog,
    location: &[u8],
    bytes: &[u8],
    identity: &[String; 3],
) -> Result<NativeOwner> {
    let owner: NativeOwner = decode(bytes)?;
    if owner.identity != *identity || owner_key(catalog, &owner) != location {
        return Err(Error::IncompatibleStore);
    }
    let source = read_row(
        tx,
        catalog,
        PROVIDER_THREADS,
        &[text_cell(&owner.provider_thread_id)],
    )
    .await?
    .ok_or(Error::IncompatibleStore)?;
    let current = native_owner(&source.row)?.ok_or(Error::IncompatibleStore)?;
    if current.identity != owner.identity
        || current.thread_id != owner.thread_id
        || current.provider_thread_id != owner.provider_thread_id
    {
        return Err(Error::IncompatibleStore);
    }
    Ok(owner)
}

async fn guard(session: &mut Session, guard: ProjectionGuard) -> Result<bool> {
    ready(session).await?;
    let tx = &mut session.transaction;
    let catalog = &session.catalog;
    match guard {
        ProjectionGuard::NativeThreadOwned {
            driver,
            provider_instance_id,
            native_id,
            app_thread_id,
        } => {
            // SQL equality with a null native ID never selects an owner.
            let Some(native_id) = native_id else {
                return Ok(false);
            };
            check_unreadable_owners(tx, catalog, &driver, &provider_instance_id, &app_thread_id)
                .await?;
            let identity = [driver, provider_instance_id, native_id];
            let prefix = native_prefix(catalog, &identity);
            let end = prefix_end(&prefix);
            let rows = tx.scan(&prefix, &end, 1).await?;
            let Some((location, bytes)) = rows.first() else {
                return Ok(false);
            };
            let first = checked_owner(tx, catalog, location, bytes, &identity).await?;
            if first.thread_id != app_thread_id {
                return Ok(true);
            }
            // All rows for this owner are contiguous. Skip them in one bounded
            // seek, even when one thread has thousands of provider aliases.
            let rows = tx
                .scan(&prefix_end(&owner_prefix(catalog, &first)), &end, 1)
                .await?;
            let Some((location, bytes)) = rows.first() else {
                return Ok(false);
            };
            Ok(checked_owner(tx, catalog, location, bytes, &identity)
                .await?
                .thread_id
                != app_thread_id)
        }
        ProjectionGuard::RunCurrent {
            thread_id,
            run_id,
            active_attempt_id,
            status,
        } => {
            let Some(run) = read_row(tx, catalog, RUNS, &[text_cell(&run_id)]).await? else {
                return Ok(false);
            };
            let payload: Value = serde_json::from_str(&text(&run.row, "payload_json")?)
                .map_err(|_| Error::IncompatibleStore)?;
            Ok(text(&run.row, "thread_id")? == thread_id
                && text(&run.row, "status")? == status
                && payload.get("activeAttemptId").and_then(Value::as_str)
                    == Some(active_attempt_id.as_str()))
        }
        ProjectionGuard::ProviderThreadOwner {
            provider_thread_id,
            run_id,
            active_attempt_id,
            expected_last_run_ordinal,
        } => {
            let Some(provider) = read_row(
                tx,
                catalog,
                PROVIDER_THREADS,
                &[text_cell(&provider_thread_id)],
            )
            .await?
            else {
                return Ok(false);
            };
            let Some(thread_id) = nullable(&provider.row, "thread_id")? else {
                return Ok(false);
            };
            if provider.row.get("last_run_ordinal") == Some(&nullable_cell(None)) {
                return Ok(false);
            }
            if integer(&provider.row, "last_run_ordinal")? != expected_last_run_ordinal {
                return Ok(false);
            }
            let Some(run) = read_row(tx, catalog, RUNS, &[text_cell(&run_id)]).await? else {
                return Ok(false);
            };
            let payload: Value = serde_json::from_str(&text(&run.row, "payload_json")?)
                .map_err(|_| Error::IncompatibleStore)?;
            Ok(text(&run.row, "thread_id")? == thread_id
                && payload.get("activeAttemptId").and_then(Value::as_str)
                    == Some(active_attempt_id.as_str()))
        }
        ProjectionGuard::ProviderThreadUnchanged {
            provider_thread_id,
            expected_json,
        } => {
            let Some(provider) = read_row(
                tx,
                catalog,
                PROVIDER_THREADS,
                &[text_cell(&provider_thread_id)],
            )
            .await?
            else {
                return Ok(false);
            };
            Ok(text(&provider.row, "payload_json")? == expected_json)
        }
    }
}

impl ApplicationTransactions {
    pub async fn projections_normalization(
        &mut self,
        id: &str,
        event: ProjectionApply,
    ) -> Result<Option<ProjectionNormalization>> {
        let mut session = self.take(id)?;
        let result = async {
            ready(&mut session).await?;
            if event.normalization.is_some() {
                return Err(invalid("normalization preflight must not contain a result"));
            }
            let payload: Value = serde_json::from_str(&event.payload_json)
                .map_err(|_| invalid("invalid projection payload JSON"))?;
            let payload = payload
                .as_object()
                .ok_or_else(|| invalid("projection payload must be an object"))?;
            normalization_context(&mut session, &event, payload).await
        }
        .await;
        self.finish(id, session, result).await
    }

    pub async fn projections_apply(&mut self, id: &str, event: ProjectionApply) -> Result<()> {
        let mut session = self.take(id)?;
        let result = apply_event(&mut session, event).await;
        self.finish(id, session, result).await
    }

    pub async fn projections_read(
        &mut self,
        id: &str,
        read: ProjectionRead,
    ) -> Result<Option<String>> {
        let mut session = self.take(id)?;
        let result = async {
            ready(&mut session).await?;
            let (name, key) = match read.kind.as_str() {
                "thread" if read.record_id.is_none() => (THREADS, read.thread_id.as_str()),
                "run" => (
                    RUNS,
                    read.record_id
                        .as_deref()
                        .ok_or_else(|| invalid("projection record id is required"))?,
                ),
                "runtime_request" => (
                    REQUESTS,
                    read.record_id
                        .as_deref()
                        .ok_or_else(|| invalid("projection record id is required"))?,
                ),
                _ => return Err(invalid("unknown projection read")),
            };
            if name != THREADS {
                // Both original control reads require the owning thread even
                // when the requested child record has not been written yet.
                let thread = read_row(
                    &mut session.transaction,
                    &session.catalog,
                    THREADS,
                    &[text_cell(&read.thread_id)],
                )
                .await?
                .ok_or_else(|| invalid("projection thread does not exist"))?;
                if name == RUNS {
                    validate_stored_row(spec(THREADS), &thread.row)?;
                }
            }
            let Some(row) = read_row(
                &mut session.transaction,
                &session.catalog,
                name,
                &[text_cell(key)],
            )
            .await?
            else {
                return Ok(None);
            };
            if text(&row.row, "thread_id")? != read.thread_id {
                return Ok(None);
            }
            text(&row.row, "payload_json").map(Some)
        }
        .await;
        self.finish(id, session, result).await
    }

    pub async fn projections_guard(
        &mut self,
        id: &str,
        condition: ProjectionGuard,
    ) -> Result<bool> {
        let mut session = self.take(id)?;
        let result = guard(&mut session, condition).await;
        self.finish(id, session, result).await
    }

    pub async fn projections_mark_applied(
        &mut self,
        id: &str,
        sequence: u64,
        updated_at: String,
    ) -> Result<()> {
        let mut session = self.take(id)?;
        let result = async {
            session.transaction.require_write()?;
            ready(&mut session).await?;
            if sequence > MAX_SAFE_INTEGER {
                return Err(invalid(
                    "projection sequence exceeds JavaScript integer range",
                ));
            }
            write_row(
                &mut session.transaction,
                &session.catalog,
                METADATA,
                Map::from_iter([
                    ("projection_name".into(), text_cell("thread-projections")),
                    ("schema_version".into(), int_cell(2)),
                    ("last_sequence".into(), int_cell(sequence as i64)),
                    ("updated_at".into(), text_cell(&updated_at)),
                ]),
            )
            .await?;
            let location = key(&session.catalog, "search-dirty");
            let mut dirty: SearchDirty = decode(
                &session
                    .transaction
                    .get(&location)
                    .await?
                    .ok_or(Error::IncompatibleStore)?,
            )?;
            dirty.last_sequence = sequence;
            session.transaction.put(location, encode(&dirty)?).await?;
            Ok(())
        }
        .await;
        self.finish(id, session, result).await
    }
}

#[cfg(test)]
#[path = "redb_projection_commit_tests.rs"]
mod tests;

#[path = "redb_projection_records.rs"]
pub mod records;

#[path = "redb_projection_history.rs"]
mod history;
pub use history::ProjectionHistoryQuery;

#[path = "redb_projection_context.rs"]
mod context;
pub use context::ProjectionContextQuery;
