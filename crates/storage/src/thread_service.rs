//! Durable thread commands from upstream Orchestrator.ts at 521ac7c218b3faa74281fb72fc36ce74a0951fb3.
//! Copyright (c) 2026 T3 Tools Inc. Distributed under the repository's MIT license.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use t3_code_ls_core::{json as core_json, project_persistence_wire};

use crate::backend::Transaction;
use crate::outbox::{self, EnqueueEffect};
use crate::{
    CommitBatch, CommitOutcome, Error, OutboxBatch, OutboxReceipt, Projection, ProjectionMutation,
    Record, Snapshot, Store, request_digest,
};

type Result<T> = std::result::Result<T, Error>;

/// The caller supplies server-owned event IDs, creation provenance and canonical UTC milliseconds.
/// Provider, linked-environment and imported-thread commands are not accepted by this service.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ThreadRequest {
    pub command: Value,
    pub event_id: String,
    pub now: String,
}

struct Plan {
    event: Value,
    thread: Value,
    guard_runtime: bool,
    effects: Vec<EnqueueEffect>,
}

/// Commits the event, thread row, receipt and any terminal-cleanup obligation together.
/// A concurrent writer can return `TransactionConflict`; retry with the same command ID.
/// Successful archive queues terminal cleanup. It does not execute that cleanup.
pub async fn execute_thread(store: &Store, request: ThreadRequest) -> Result<CommitOutcome> {
    let command = normalize_command(&request.command)?;
    crate::validate_id(&request.event_id)?;
    timestamp(&request.now)?;
    let command_id = string(&command, "commandId")?;
    let digest = request_digest(&json!({
        "protocol": "native-thread-command-v1", "command": command,
    }));
    let mut tx = store.db.begin().await?;
    let staged = async {
        if let Some(bytes) = tx
            .get(&crate::store_key(&format!("receipt/{command_id}")))
            .await?
        {
            let receipt = crate::decode_receipt(&bytes, command_id)?;
            if receipt.request_digest != digest {
                return Err(Error::CommandConflict);
            }
            return Ok(CommitOutcome {
                receipt,
                replayed: true,
            });
        }
        if tx
            .get(&crate::outbox_receipt_key(command_id))
            .await?
            .is_some()
        {
            return Err(Error::IncompatibleStore);
        }
        let snapshot = store.snapshot_in(&mut tx, false).await?;
        let plan = plan_thread(&snapshot, &command, &request.event_id, &request.now)?;
        if plan.guard_runtime {
            guard_pending_effects(&mut tx, string(&command, "threadId")?).await?;
        }
        let batch = CommitBatch {
            command_id: command_id.into(),
            request_digest: digest,
            expected_revision: snapshot.revision,
            events: vec![Record {
                id: request.event_id,
                payload: plan.event.clone(),
            }],
            projections: vec![ProjectionMutation::Put {
                key: format!("thread/{}", string(&command, "threadId")?),
                payload: plan.thread.clone(),
            }],
            effects: Vec::new(),
            result: json!({"ok": {"event": plan.event, "thread": plan.thread}}),
        };
        crate::validate_batch(&batch)?;
        let outcome = store.stage(&mut tx, &batch).await?;
        if !plan.effects.is_empty() {
            for effect in &plan.effects {
                let key =
                    crate::store_key(&format!("outbox/v1/row/{}", hex_component(&effect.id),));
                if tx.get(&key).await?.is_some() {
                    return Err(Error::DuplicateRecord);
                }
            }
            let envelope = OutboxBatch {
                enqueue: plan.effects,
                enqueue_at: request.now,
                cancel: Vec::new(),
            };
            let enqueued =
                outbox::stage_enqueue(&mut tx, &envelope.enqueue, &envelope.enqueue_at).await?;
            let extra = OutboxReceipt {
                command_id: batch.command_id.clone(),
                request_digest: batch.request_digest.clone(),
                outbox_digest: request_digest(
                    &serde_json::to_value(&envelope).map_err(crate::json_error)?,
                ),
                enqueued,
                cancelled: Vec::new(),
            };
            tx.insert(
                crate::outbox_receipt_key(&batch.command_id),
                crate::encode(&extra)?,
            )
            .await?;
        }
        crate::validate_response_bytes(&outcome)?;
        Ok(outcome)
    }
    .await;
    match staged {
        Ok(outcome) if outcome.replayed => {
            tx.rollback().await?;
            Ok(outcome)
        }
        Ok(outcome) => {
            tx.commit().await?;
            Ok(outcome)
        }
        Err(error) => {
            let _ = tx.rollback().await;
            Err(error)
        }
    }
}

fn normalize_command(command: &Value) -> Result<Map<String, Value>> {
    let source = object(command, "command")?;
    let kind = string(source, "type")?;
    let extra: &[&str] = match kind {
        "thread.create" => &[
            "projectId",
            "title",
            "modelSelection",
            "runtimeMode",
            "interactionMode",
            "branch",
            "worktreePath",
            "createdBy",
            "creationSource",
        ],
        "thread.metadata.update" => &[
            "title",
            "branch",
            "worktreePath",
            "expectedBranch",
            "expectedWorktreePath",
            "expectedEmpty",
            "regenerateTitle",
        ],
        "thread.pin" | "thread.pin.reorder" | "thread.active.reorder" => &["orderKey"],
        "thread.visit" => &["visitedAt", "opened", "searchQuery"],
        "thread.auto-settle.set" => &["enabled"],
        "thread.archive" | "thread.unarchive" | "thread.unpin" => &[],
        _ => return Err(invalid("unsupported native thread command")),
    };
    for key in source.keys() {
        if !["type", "commandId", "threadId"].contains(&key.as_str())
            && !extra.contains(&key.as_str())
        {
            return Err(invalid(&format!("unsupported field in {kind}: {key}")));
        }
    }
    let mut command = source.clone();
    for key in ["commandId", "threadId"] {
        normalize_string(&mut command, key, true, false)?;
        crate::validate_id(string(&command, key)?)?;
    }
    match kind {
        "thread.create" => {
            for key in ["projectId", "title"] {
                normalize_string(&mut command, key, true, false)?;
            }
            crate::validate_id(string(&command, "projectId")?)?;
            for key in ["branch", "worktreePath"] {
                normalize_string(&mut command, key, true, true)?;
            }
            literal(
                &command,
                "runtimeMode",
                &[
                    "approval-required",
                    "auto-accept-edits",
                    "auto",
                    "full-access",
                ],
            )?;
            literal(&command, "interactionMode", &["default", "plan"])?;
            literal(&command, "createdBy", &["user", "agent", "system"])?;
            literal(
                &command,
                "creationSource",
                &["web", "mobile", "mcp", "provider", "server"],
            )?;
            let model = normalize_model(field(&command, "modelSelection")?)?;
            command.insert("modelSelection".into(), model);
        }
        "thread.metadata.update" => {
            normalize_string(&mut command, "title", false, false)?;
            for key in [
                "branch",
                "worktreePath",
                "expectedBranch",
                "expectedWorktreePath",
            ] {
                normalize_string(&mut command, key, false, true)?;
            }
            optional_bool(&command, "expectedEmpty")?;
            optional_bool(&command, "regenerateTitle")?;
            if command.get("regenerateTitle") == Some(&Value::Bool(true)) {
                return Err(invalid(
                    "native title-generation execution is not available",
                ));
            }
        }
        "thread.pin" | "thread.pin.reorder" | "thread.active.reorder" => {
            normalize_string(&mut command, "orderKey", kind != "thread.pin", false)?;
        }
        "thread.visit" => {
            timestamp(string(&command, "visitedAt")?)?;
            optional_bool(&command, "opened")?;
            if command.get("opened") == Some(&Value::Bool(true)) {
                return Err(invalid(
                    "native thread-open ranking statistics are not available",
                ));
            }
            if let Some(query) = command.get("searchQuery") {
                let query = query
                    .as_str()
                    .ok_or_else(|| invalid("searchQuery must be a string"))?;
                if query.encode_utf16().count() > 200 {
                    return Err(invalid("searchQuery exceeds 200 UTF-16 units"));
                }
            }
        }
        "thread.auto-settle.set" => {
            field(&command, "enabled")?
                .as_bool()
                .ok_or_else(|| invalid("enabled must be boolean"))?;
        }
        _ => {}
    }
    Ok(command)
}

fn plan_thread(
    snapshot: &Snapshot,
    command: &Map<String, Value>,
    event_id: &str,
    now: &str,
) -> Result<Plan> {
    let kind = string(command, "type")?;
    let id = string(command, "threadId")?;
    let key = format!("thread/{id}");
    let existing = snapshot.projections.iter().find(|row| row.key == key);
    if kind == "thread.create" && existing.is_some() {
        return Err(invalid(&format!("Thread {id} already exists.")));
    }
    let mut thread = match existing {
        Some(row) => {
            validate_thread(&row.payload, id)?;
            object(&row.payload, "stored thread")?.clone()
        }
        None if kind == "thread.create" => create_thread(command, now)?,
        None => return Err(invalid(&format!("Thread {id} was not found."))),
    };
    let project_id = string(&thread, "projectId")?;
    validate_project(snapshot, project_id)?;
    if thread["deletedAt"] != Value::Null {
        return Err(invalid(&format!("Thread {id} is deleted.")));
    }
    if [
        "thread.pin",
        "thread.unpin",
        "thread.pin.reorder",
        "thread.active.reorder",
        "thread.auto-settle.set",
    ]
    .contains(&kind)
        && thread["archivedAt"] != Value::Null
    {
        return Err(invalid(&format!("Thread {id} is archived.")));
    }
    let mut guard_runtime = false;
    let mut effects = Vec::new();
    let event_type = match kind {
        "thread.create" => "thread.created",
        "thread.metadata.update" => {
            for (expected, stored, label) in [
                ("expectedWorktreePath", "worktreePath", "worktree"),
                ("expectedBranch", "branch", "branch"),
            ] {
                if let Some(value) = command.get(expected)
                    && value != &thread[stored]
                {
                    return Err(invalid(&format!(
                        "Thread {id} {label} changed before the metadata update could be applied."
                    )));
                }
            }
            if command.get("expectedEmpty") == Some(&Value::Bool(true))
                && snapshot
                    .projections
                    .iter()
                    .any(|row| related_record(row, id, &key))
            {
                return Err(invalid(&format!("Thread {id} is no longer empty.")));
            }
            if command
                .get("worktreePath")
                .is_some_and(|path| path != &thread["worktreePath"])
            {
                ensure_no_runtime(snapshot, &thread, id, &key)?;
                guard_runtime = true;
            }
            for name in ["title", "branch", "worktreePath"] {
                if let Some(value) = command.get(name) {
                    thread.insert(name.into(), value.clone());
                }
            }
            if command.contains_key("title")
                || command.get("regenerateTitle") == Some(&Value::Bool(false))
            {
                thread.insert("titleRegeneration".into(), Value::Null);
            }
            thread.insert("updatedAt".into(), json!(now));
            "thread.metadata-updated"
        }
        "thread.archive" => {
            if thread["archivedAt"] != Value::Null {
                return Err(invalid(&format!("Thread {id} is already archived.")));
            }
            ensure_no_runtime(snapshot, &thread, id, &key)?;
            guard_runtime = true;
            thread.insert("archivedAt".into(), json!(now));
            thread.insert("updatedAt".into(), json!(now));
            thread.insert("titleRegeneration".into(), Value::Null);
            thread.insert("guestShare".into(), Value::Null);
            effects.push(EnqueueEffect {
                id: format!("effect:{}:terminal.cleanup", string(command, "commandId")?),
                command_id: string(command, "commandId")?.into(),
                thread_id: id.into(),
                request: json!({"type": "terminal.cleanup"}),
                available_at: None,
            });
            "thread.archived"
        }
        "thread.unarchive" => {
            if thread["archivedAt"] == Value::Null {
                return Err(invalid(&format!("Thread {id} is not archived.")));
            }
            thread.insert("archivedAt".into(), Value::Null);
            thread.insert("updatedAt".into(), json!(now));
            "thread.unarchived"
        }
        "thread.pin" => {
            let pinned = nonnull(&thread, "pinnedAt");
            let settled = thread["settledOverride"] == "settled";
            let snoozed = nonnull(&thread, "snoozedUntil");
            if !pinned {
                thread.insert("pinnedAt".into(), json!(now));
                if let Some(order) = command.get("orderKey") {
                    thread.insert("pinOrderKey".into(), order.clone());
                }
            }
            if settled {
                thread.insert("settledOverride".into(), json!("active"));
                thread.insert("settledAt".into(), Value::Null);
                thread.insert("unsettledAt".into(), json!(now));
            }
            if snoozed {
                thread.insert("lastSnoozeWakeAt".into(), json!(now));
            }
            thread.insert("snoozedUntil".into(), Value::Null);
            thread.insert("snoozedAt".into(), Value::Null);
            if !pinned || settled || snoozed {
                thread.insert("updatedAt".into(), json!(now));
            }
            "thread.pinned"
        }
        "thread.unpin" => {
            if nonnull(&thread, "pinnedAt") {
                thread.insert("updatedAt".into(), json!(now));
            }
            thread.insert("pinnedAt".into(), Value::Null);
            thread.insert("pinOrderKey".into(), Value::Null);
            "thread.unpinned"
        }
        "thread.pin.reorder" => {
            if !nonnull(&thread, "pinnedAt") {
                return Err(invalid(&format!(
                    "Thread {id} is not pinned and cannot be reordered."
                )));
            }
            if thread.get("pinOrderKey") != command.get("orderKey") {
                thread.insert("updatedAt".into(), json!(now));
            }
            thread.insert("pinOrderKey".into(), command["orderKey"].clone());
            "thread.pin-reordered"
        }
        "thread.active.reorder" => {
            if nonnull(&thread, "pinnedAt") || thread["settledOverride"] == "settled" {
                return Err(invalid(&format!(
                    "Thread {id} is not active and cannot be reordered."
                )));
            }
            thread.insert("activeOrderKey".into(), command["orderKey"].clone());
            "thread.active-reordered"
        }
        "thread.auto-settle.set" => {
            let enabled = command["enabled"] == true;
            let disabled = nonnull(&thread, "autoSettleDisabledAt");
            if enabled == disabled {
                thread.insert("updatedAt".into(), json!(now));
            }
            if enabled {
                thread.insert("autoSettleDisabledAt".into(), Value::Null);
            } else if !disabled {
                thread.insert("autoSettleDisabledAt".into(), json!(now));
            }
            "thread.auto-settle-set"
        }
        "thread.visit" => {
            let at = string(command, "visitedAt")?;
            if thread["lastVisitedAt"]
                .as_str()
                .is_none_or(|before| at > before)
            {
                thread.insert("lastVisitedAt".into(), json!(at));
            }
            "thread.visited"
        }
        _ => return Err(invalid("unsupported native thread command")),
    };
    let thread = Value::Object(thread);
    let event = json!({
        "id": event_id, "type": event_type, "threadId": id,
        "providerInstanceId": thread["providerInstanceId"], "occurredAt": now,
        "payload": thread,
    });
    Ok(Plan {
        event,
        thread,
        guard_runtime,
        effects,
    })
}

fn create_thread(command: &Map<String, Value>, now: &str) -> Result<Map<String, Value>> {
    let mut thread = Map::new();
    for name in [
        "projectId",
        "title",
        "modelSelection",
        "runtimeMode",
        "interactionMode",
        "branch",
        "worktreePath",
        "createdBy",
        "creationSource",
    ] {
        thread.insert(name.into(), field(command, name)?.clone());
    }
    thread.insert("id".into(), command["threadId"].clone());
    thread.insert(
        "providerInstanceId".into(),
        command["modelSelection"]["instanceId"].clone(),
    );
    thread.insert("pullRequests".into(), json!([]));
    thread.insert("lineage".into(), json!({
        "parentThreadId": null, "relationshipToParent": null, "rootThreadId": command["threadId"],
    }));
    for name in [
        "activeProviderThreadId",
        "forkedFrom",
        "archivedAt",
        "settledOverride",
        "settledAt",
        "snoozedUntil",
        "snoozedAt",
        "lastVisitedAt",
        "deletedAt",
    ] {
        thread.insert(name.into(), Value::Null);
    }
    thread.insert("createdAt".into(), json!(now));
    thread.insert("updatedAt".into(), json!(now));
    Ok(thread)
}

fn validate_project(snapshot: &Snapshot, project_id: &str) -> Result<()> {
    let key = format!("project/{project_id}");
    let row = snapshot
        .projections
        .iter()
        .find(|row| row.key == key)
        .ok_or_else(|| invalid(&format!("Project {project_id} was not found.")))?;
    let encoded = serde_json::to_string(&row.payload).map_err(crate::json_error)?;
    let value = core_json::parse(&encoded).map_err(|_| Error::IncompatibleStore)?;
    project_persistence_wire::validate_projection_shape(&value)
        .map_err(|_| Error::IncompatibleStore)?;
    if row.payload["projectId"] != project_id {
        return Err(Error::IncompatibleStore);
    }
    if row.payload["deletedAt"] != Value::Null {
        return Err(invalid(&format!("Project {project_id} is deleted.")));
    }
    Ok(())
}

fn validate_thread(value: &Value, id: &str) -> Result<()> {
    let row = object(value, "stored thread")?;
    let fields = [
        "createdBy",
        "creationSource",
        "id",
        "projectId",
        "title",
        "providerInstanceId",
        "modelSelection",
        "runtimeMode",
        "interactionMode",
        "branch",
        "worktreePath",
        "linkedPullRequest",
        "pullRequests",
        "branchPullRequest",
        "activeProviderThreadId",
        "historyOrigin",
        "linkOrigin",
        "delegatedFrom",
        "lineage",
        "forkedFrom",
        "createdAt",
        "updatedAt",
        "archivedAt",
        "settledOverride",
        "settledAt",
        "unsettledAt",
        "snoozedUntil",
        "snoozedAt",
        "lastSnoozeWakeAt",
        "limitRecovery",
        "guestShare",
        "pinnedAt",
        "autoSettleDisabledAt",
        "pinOrderKey",
        "activeOrderKey",
        "tags",
        "lastVisitedAt",
        "opens",
        "titleRegeneration",
        "rollbackRequestId",
        "rollbackFailure",
        "deletedAt",
    ];
    if row.keys().any(|key| !fields.contains(&key.as_str())) {
        return Err(Error::IncompatibleStore);
    }
    for name in ["id", "projectId", "title", "providerInstanceId"] {
        let value = string(row, name)?;
        if value.is_empty() || value.trim_matches(ecma_whitespace) != value {
            return Err(Error::IncompatibleStore);
        }
    }
    let model = field(row, "modelSelection")?;
    if row["id"] != id || row["providerInstanceId"] != model["instanceId"] {
        return Err(Error::IncompatibleStore);
    }
    crate::validate_id(string(row, "projectId")?)?;
    literal(
        row,
        "runtimeMode",
        &[
            "approval-required",
            "auto-accept-edits",
            "auto",
            "full-access",
        ],
    )?;
    literal(row, "interactionMode", &["default", "plan"])?;
    literal(row, "createdBy", &["user", "agent", "system"])?;
    literal(
        row,
        "creationSource",
        &["web", "mobile", "mcp", "provider", "server"],
    )?;
    if normalize_model(model)? != *model {
        return Err(Error::IncompatibleStore);
    }
    for name in ["branch", "worktreePath", "activeProviderThreadId"] {
        let value = field(row, name)?;
        if !value.is_null()
            && value
                .as_str()
                .is_none_or(|s| s.is_empty() || s.trim_matches(ecma_whitespace) != s)
        {
            return Err(Error::IncompatibleStore);
        }
    }
    for name in ["createdAt", "updatedAt"] {
        timestamp(string(row, name)?)?;
    }
    for name in ["archivedAt", "settledAt", "lastVisitedAt", "deletedAt"] {
        nullable_timestamp(field(row, name)?)?;
    }
    for name in [
        "unsettledAt",
        "snoozedUntil",
        "snoozedAt",
        "lastSnoozeWakeAt",
        "pinnedAt",
        "autoSettleDisabledAt",
    ] {
        if let Some(value) = row.get(name) {
            nullable_timestamp(value)?;
        }
    }
    let settled = field(row, "settledOverride")?;
    if !settled.is_null() && !matches!(settled.as_str(), Some("settled" | "active")) {
        return Err(Error::IncompatibleStore);
    }
    for name in ["pinOrderKey", "activeOrderKey"] {
        if let Some(value) = row.get(name)
            && !value.is_null()
            && value
                .as_str()
                .is_none_or(|s| s.is_empty() || s.trim_matches(ecma_whitespace) != s)
        {
            return Err(Error::IncompatibleStore);
        }
    }
    let lineage = object(field(row, "lineage")?, "thread.lineage")?;
    crate::validate_id(string(lineage, "rootThreadId")?)?;
    let parent = field(lineage, "parentThreadId")?;
    let relationship = field(lineage, "relationshipToParent")?;
    if (!parent.is_null() && parent.as_str().is_none())
        || (!relationship.is_null() && !matches!(relationship.as_str(), Some("fork" | "subagent")))
        || parent.is_null() != relationship.is_null()
    {
        return Err(Error::IncompatibleStore);
    }
    optional_bool(lineage, "sideChat")?;
    if let Some(links) = row.get("pullRequests")
        && !links.is_array()
    {
        return Err(Error::IncompatibleStore);
    }
    for name in [
        "forkedFrom",
        "linkedPullRequest",
        "branchPullRequest",
        "linkOrigin",
        "delegatedFrom",
        "limitRecovery",
        "guestShare",
        "opens",
        "titleRegeneration",
        "rollbackFailure",
    ] {
        if let Some(value) = row.get(name)
            && !value.is_null()
            && !value.is_object()
        {
            return Err(Error::IncompatibleStore);
        }
    }
    field(row, "forkedFrom")?;
    if let Some(tags) = row.get("tags")
        && tags
            .as_array()
            .is_none_or(|tags| tags.iter().any(|tag| !tag.is_string()))
    {
        return Err(Error::IncompatibleStore);
    }
    Ok(())
}

fn related_record(row: &Projection, id: &str, own_key: &str) -> bool {
    if row.key == own_key || row.key.starts_with("project/") {
        return false;
    }
    row.key.starts_with(&format!("{own_key}/"))
        || row.key == format!("native/session/{id}")
        || ["threadId", "appThreadId", "parentThreadId"]
            .iter()
            .any(|key| row.payload.get(key).and_then(Value::as_str) == Some(id))
        || row
            .payload
            .get("thread")
            .and_then(|thread| thread.get("id"))
            .and_then(Value::as_str)
            == Some(id)
        || row
            .payload
            .get("lineage")
            .and_then(|lineage| lineage.get("parentThreadId"))
            .and_then(Value::as_str)
            == Some(id)
}

fn stopped_native_session(row: &Projection, id: &str) -> bool {
    // The native runtime writes this state only after its owned provider process is reaped.
    row.key == format!("native/session/{id}")
        && row.payload.get("threadId").and_then(Value::as_str) == Some(id)
        && row
            .payload
            .get("sessionId")
            .and_then(Value::as_str)
            .is_some_and(crate::id_is_valid)
        && row.payload.get("status").and_then(Value::as_str) == Some("closed")
        && row.payload.get("loaded") == Some(&Value::Bool(false))
        && row.payload.get("activeTurnId") == Some(&Value::Null)
        && row.payload.get("processState").and_then(Value::as_str) == Some("stopped")
}

fn ensure_no_runtime(
    snapshot: &Snapshot,
    row: &Map<String, Value>,
    id: &str,
    own_key: &str,
) -> Result<()> {
    let has_links = row
        .get("pullRequests")
        .and_then(Value::as_array)
        .is_some_and(|links| !links.is_empty());
    if has_links
        || [
            "activeProviderThreadId",
            "forkedFrom",
            "guestShare",
            "titleRegeneration",
            "linkOrigin",
            "delegatedFrom",
            "linkedPullRequest",
            "limitRecovery",
        ]
        .iter()
        .any(|key| nonnull(row, key))
        || row
            .get("lineage")
            .and_then(|lineage| lineage.get("parentThreadId"))
            .is_some_and(|parent| !parent.is_null())
        || row
            .get("historyOrigin")
            .is_some_and(|origin| origin != "native")
        || snapshot.projections.iter().any(|projection| {
            related_record(projection, id, own_key) && !stopped_native_session(projection, id)
        })
    {
        return Err(invalid(
            "thread runtime, guest, linked-work or provider cleanup is not ported; command was not applied",
        ));
    }
    Ok(())
}

async fn guard_pending_effects(tx: &mut Transaction, id: &str) -> Result<()> {
    // These range reads participate in the same optimistic transaction as the thread update.
    for state in ["lane-pending", "lane-running"] {
        let path = format!("outbox/v1/{state}/{}/", hex_component(id));
        let (start, end) = crate::prefix_range(&path);
        if !tx.scan(&start, &end, 1).await?.is_empty() {
            return Err(invalid(
                "thread has unsettled effects requiring unported lifecycle coordination",
            ));
        }
    }
    let (start, end) = crate::prefix_range("outbox/pending/");
    if !tx.scan(&start, &end, 1).await?.is_empty() {
        return Err(invalid(
            "untyped pending effects prevent native thread lifecycle cleanup",
        ));
    }
    Ok(())
}

fn hex_component(value: &str) -> String {
    value.bytes().map(|byte| format!("{byte:02x}")).collect()
}

fn normalize_model(value: &Value) -> Result<Value> {
    let normalized = crate::project_validation::validate_command(&json!({
        "type": "project.meta.update", "commandId": "validation", "projectId": "validation",
        "defaultModelSelection": value,
    }))?;
    let model = normalized["defaultModelSelection"].clone();
    if !model.is_object() {
        return Err(invalid("modelSelection must be an object"));
    }
    Ok(model)
}

fn normalize_string(
    row: &mut Map<String, Value>,
    name: &str,
    required: bool,
    nullable: bool,
) -> Result<()> {
    match row.get_mut(name) {
        Some(value) if nullable && value.is_null() => Ok(()),
        Some(value) => {
            let text = value
                .as_str()
                .ok_or_else(|| invalid(&format!("{name} must be a nonblank string")))?
                .trim_matches(ecma_whitespace);
            if text.is_empty() {
                return Err(invalid(&format!("{name} must be a nonblank string")));
            }
            *value = json!(text);
            Ok(())
        }
        None if required => Err(invalid(&format!("missing {name}"))),
        None => Ok(()),
    }
}

fn ecma_whitespace(c: char) -> bool {
    matches!(c, '\u{0009}'..='\u{000d}' | '\u{0020}' | '\u{00a0}' | '\u{1680}'
        | '\u{2000}'..='\u{200a}' | '\u{2028}' | '\u{2029}' | '\u{202f}' | '\u{205f}'
        | '\u{3000}' | '\u{feff}')
}

fn timestamp(value: &str) -> Result<()> {
    outbox::validate_timestamp(value)
        .map_err(|_| invalid("thread timestamps must be valid canonical UTC milliseconds"))
}

fn nullable_timestamp(value: &Value) -> Result<()> {
    if value.is_null() {
        Ok(())
    } else {
        timestamp(value.as_str().ok_or(Error::IncompatibleStore)?)
    }
}

fn nonnull(row: &Map<String, Value>, key: &str) -> bool {
    row.get(key).is_some_and(|value| !value.is_null())
}

fn optional_bool(row: &Map<String, Value>, key: &str) -> Result<()> {
    if row.get(key).is_some_and(|value| !value.is_boolean()) {
        Err(invalid(&format!("{key} must be boolean")))
    } else {
        Ok(())
    }
}

fn literal(row: &Map<String, Value>, key: &str, choices: &[&str]) -> Result<()> {
    if choices.contains(&string(row, key)?) {
        Ok(())
    } else {
        Err(invalid(&format!("unsupported {key}")))
    }
}

fn object<'a>(value: &'a Value, label: &str) -> Result<&'a Map<String, Value>> {
    value
        .as_object()
        .ok_or_else(|| invalid(&format!("{label} must be an object")))
}

fn field<'a>(row: &'a Map<String, Value>, key: &str) -> Result<&'a Value> {
    row.get(key)
        .ok_or_else(|| invalid(&format!("missing {key}")))
}

fn string<'a>(row: &'a Map<String, Value>, key: &str) -> Result<&'a str> {
    field(row, key)?
        .as_str()
        .ok_or_else(|| invalid(&format!("{key} must be a string")))
}

fn invalid(message: &str) -> Error {
    Error::Invalid(message.into())
}
