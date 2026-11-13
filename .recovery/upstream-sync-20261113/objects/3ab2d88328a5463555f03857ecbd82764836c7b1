//! Durable orchestration effects. Generic immutable `Record` effects deliberately keep their
//! existing namespace: these rows implement the production worker's mutable lifecycle.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use t3_code_ls_core::outbox as policy;

use crate::application::ApplicationCatalog;
use crate::backend::Transaction;
use crate::{Error, MAX_BATCH_BYTES, MAX_BATCH_RECORDS, MAX_COUNTER, Result, Store, store_key};

const PREFIX: &str = "outbox/v1/";
const PAGE_SIZE: usize = 128;
const MAX_ERROR_BYTES: usize = 64 * 1024;

/// The scheduling engine is shared; only authoritative rows and auxiliary keys differ.
#[derive(Clone, Copy)]
pub(crate) enum Repository<'a> {
    Legacy,
    Application(&'a ApplicationCatalog),
}

impl Repository<'_> {
    fn key(self, path: &str) -> Vec<u8> {
        match self {
            Self::Legacy => store_key(&format!("{PREFIX}{path}")),
            Self::Application(catalog) => crate::application_outbox::index_key(catalog, path),
        }
    }

    fn validate(self, row: &OutboxEffect) -> Result<()> {
        validate_row_with_lease(row, matches!(self, Self::Application(_)))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum EffectType {
    #[serde(rename = "provider-runtime.continue")]
    ProviderRuntimeContinue,
    #[serde(rename = "provider-session.detach")]
    ProviderSessionDetach,
    #[serde(rename = "provider-session.apply-model-selection")]
    ProviderSessionApplyModelSelection,
    #[serde(rename = "provider-turn.start")]
    ProviderTurnStart,
    #[serde(rename = "provider-turn.interrupt")]
    ProviderTurnInterrupt,
    #[serde(rename = "provider-turn.steer")]
    ProviderTurnSteer,
    #[serde(rename = "provider-turn.restart")]
    ProviderTurnRestart,
    #[serde(rename = "runtime-request.respond")]
    RuntimeRequestRespond,
    #[serde(rename = "provider-thread.rollback")]
    ProviderThreadRollback,
    #[serde(rename = "checkpoint.capture")]
    CheckpointCapture,
    #[serde(rename = "scheduled-tasks.cleanup")]
    ScheduledTasksCleanup,
    #[serde(rename = "terminal.cleanup")]
    TerminalCleanup,
    #[serde(rename = "preview.cleanup")]
    PreviewCleanup,
    #[serde(rename = "attachment.cleanup")]
    AttachmentCleanup,
    #[serde(rename = "thread-title.generate")]
    ThreadTitleGenerate,
    #[serde(rename = "delegated-tasks.stop")]
    DelegatedTasksStop,
}

impl EffectType {
    pub fn as_wire(self) -> &'static str {
        match self {
            Self::ProviderRuntimeContinue => "provider-runtime.continue",
            Self::ProviderSessionDetach => "provider-session.detach",
            Self::ProviderSessionApplyModelSelection => "provider-session.apply-model-selection",
            Self::ProviderTurnStart => "provider-turn.start",
            Self::ProviderTurnInterrupt => "provider-turn.interrupt",
            Self::ProviderTurnSteer => "provider-turn.steer",
            Self::ProviderTurnRestart => "provider-turn.restart",
            Self::RuntimeRequestRespond => "runtime-request.respond",
            Self::ProviderThreadRollback => "provider-thread.rollback",
            Self::CheckpointCapture => "checkpoint.capture",
            Self::ScheduledTasksCleanup => "scheduled-tasks.cleanup",
            Self::TerminalCleanup => "terminal.cleanup",
            Self::PreviewCleanup => "preview.cleanup",
            Self::AttachmentCleanup => "attachment.cleanup",
            Self::ThreadTitleGenerate => "thread-title.generate",
            Self::DelegatedTasksStop => "delegated-tasks.stop",
        }
    }

    pub fn from_wire(value: &str) -> Option<Self> {
        Some(match value {
            "provider-runtime.continue" => Self::ProviderRuntimeContinue,
            "provider-session.detach" => Self::ProviderSessionDetach,
            "provider-session.apply-model-selection" => Self::ProviderSessionApplyModelSelection,
            "provider-turn.start" => Self::ProviderTurnStart,
            "provider-turn.interrupt" => Self::ProviderTurnInterrupt,
            "provider-turn.steer" => Self::ProviderTurnSteer,
            "provider-turn.restart" => Self::ProviderTurnRestart,
            "runtime-request.respond" => Self::RuntimeRequestRespond,
            "provider-thread.rollback" => Self::ProviderThreadRollback,
            "checkpoint.capture" => Self::CheckpointCapture,
            "scheduled-tasks.cleanup" => Self::ScheduledTasksCleanup,
            "terminal.cleanup" => Self::TerminalCleanup,
            "preview.cleanup" => Self::PreviewCleanup,
            "attachment.cleanup" => Self::AttachmentCleanup,
            "thread-title.generate" => Self::ThreadTitleGenerate,
            "delegated-tasks.stop" => Self::DelegatedTasksStop,
            _ => return None,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EffectStatus {
    Pending,
    Running,
    Succeeded,
    Failed,
    Cancelled,
}

impl EffectStatus {
    pub fn as_wire(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }

    pub(crate) fn from_wire(value: &str) -> Option<Self> {
        Some(match value {
            "pending" => Self::Pending,
            "running" => Self::Running,
            "succeeded" => Self::Succeeded,
            "failed" => Self::Failed,
            "cancelled" => Self::Cancelled,
            _ => return None,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OutboxEffect {
    pub id: String,
    pub command_id: String,
    pub thread_id: String,
    pub request: Value,
    pub status: EffectStatus,
    pub attempt_count: u64,
    pub available_at: String,
    pub lease_owner: Option<String>,
    /// Retained for faithful SQLite row exports. Production has no expiring worker lease.
    pub lease_expires_at: Option<String>,
    pub created_at: String,
    pub updated_at: String,
    pub completed_at: Option<String>,
    pub last_error: Option<String>,
    pub insertion_ordinal: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EnqueueEffect {
    pub id: String,
    pub command_id: String,
    pub thread_id: String,
    pub request: Value,
    #[serde(default)]
    pub available_at: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Claim {
    pub worker_id: String,
    pub now: String,
    #[serde(default)]
    pub exclude_restart_continuations: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_running_title_generations: Option<f64>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClaimOnce {
    pub stream_id: String,
    pub sequence: u64,
    pub claim: Claim,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ClaimState {
    Claimed,
    Empty,
    Retired,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClaimOutcome {
    pub state: ClaimState,
    pub replayed: bool,
    pub effect: Option<OutboxEffect>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ClaimIdentity {
    effect_id: String,
    insertion_ordinal: u64,
    attempt_count: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ClaimCursor {
    stream_id: String,
    sequence: u64,
    claim: Claim,
    effect: Option<ClaimIdentity>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Settle {
    pub effect_id: String,
    pub worker_id: String,
    pub now: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Retry {
    pub effect_id: String,
    pub worker_id: String,
    pub now: String,
    /// The host applies max(0, delayMs) before formatting this timestamp.
    pub available_at: String,
    pub error: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Failure {
    pub effect_id: String,
    pub worker_id: String,
    pub now: String,
    pub error: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CancelUnsettled {
    pub thread_id: String,
    pub effect_types: Vec<EffectType>,
    pub reason: String,
    pub now: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Reconcile {
    pub now: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReconcileOutcome {
    pub requeued: usize,
    pub cancelled: usize,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Prune {
    pub before: String,
    /// One upstream-sized batch. Call again after a successful batch to release the write lock.
    pub limit: u32,
}

fn invalid(message: &str) -> Error {
    Error::Invalid(message.to_owned())
}

/// Lexicographic index order equals time order only for one canonical representation.
pub fn validate_timestamp(value: &str) -> Result<()> {
    let b = value.as_bytes();
    if b.len() != 24
        || b[4] != b'-'
        || b[7] != b'-'
        || b[10] != b'T'
        || b[13] != b':'
        || b[16] != b':'
        || b[19] != b'.'
        || b[23] != b'Z'
        || b.iter()
            .enumerate()
            .any(|(i, c)| !matches!(i, 4 | 7 | 10 | 13 | 16 | 19 | 23) && !c.is_ascii_digit())
    {
        return Err(invalid(
            "outbox timestamp must be canonical UTC milliseconds",
        ));
    }
    let number = |start: usize, end: usize| {
        b[start..end]
            .iter()
            .fold(0u32, |n, digit| n * 10 + u32::from(digit - b'0'))
    };
    let year = number(0, 4);
    let month = number(5, 7);
    let day = number(8, 10);
    let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    let days = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if leap => 29,
        2 => 28,
        _ => 0,
    };
    if day == 0 || day > days || number(11, 13) > 23 || number(14, 16) > 59 || number(17, 19) > 59 {
        return Err(invalid("outbox timestamp contains an invalid date or time"));
    }
    Ok(())
}

fn validate_error(value: &str) -> Result<()> {
    if value.len() > MAX_ERROR_BYTES {
        Err(invalid("outbox error exceeds 64 KiB"))
    } else {
        Ok(())
    }
}

pub(crate) fn request_type(request: &Value) -> Result<EffectType> {
    request
        .get("type")
        .and_then(Value::as_str)
        .and_then(EffectType::from_wire)
        .ok_or_else(|| invalid("unknown orchestration effect type"))
}

fn select_fields(value: &Value, fields: &[&str]) -> Result<Value> {
    let object = value
        .as_object()
        .ok_or_else(|| invalid("outbox request record must be an object"))?;
    Ok(Value::Object(
        fields
            .iter()
            .filter_map(|name| {
                object
                    .get(*name)
                    .map(|value| ((*name).to_owned(), value.clone()))
            })
            .collect(),
    ))
}

fn validate_request_id(value: &str) -> Result<()> {
    if value.is_empty() || value.len() > crate::MAX_ID_BYTES {
        return Err(invalid("invalid outbox request identifier"));
    }
    Ok(())
}

fn trim_request_id(record: &mut Value, field: &str) -> Result<()> {
    let Some(value) = record.get_mut(field) else {
        return Ok(());
    };
    let text = value
        .as_str()
        .ok_or_else(|| invalid("outbox request identifier must be a string"))?;
    // Match ECMAScript String.trim: BOM is whitespace, but Unicode NEL is not.
    let text = text.trim_matches(|c| {
        matches!(c,
            '\u{0009}'..='\u{000d}' | '\u{0020}' | '\u{00a0}' | '\u{1680}'
            | '\u{2000}'..='\u{200a}' | '\u{2028}' | '\u{2029}' | '\u{202f}'
            | '\u{205f}' | '\u{3000}' | '\u{feff}'
        )
    });
    validate_request_id(text)?;
    *value = Value::String(text.to_owned());
    Ok(())
}

/// Effect Schema encodes only the selected union variant, including its nested records.
/// Preserve unknown user answer values, but never persist transport extras as request fields.
pub(crate) fn normalize_request(request: &Value) -> Result<Value> {
    let fields: &[&str] = match request_type(request)? {
        EffectType::ProviderRuntimeContinue => &["type", "sourceRunId"],
        EffectType::ProviderSessionDetach => {
            &["type", "providerSessionId", "detail", "revokeMcpCredential"]
        }
        EffectType::ProviderSessionApplyModelSelection => &[
            "type",
            "providerSessionId",
            "providerThreadId",
            "modelSelection",
        ],
        EffectType::ProviderTurnStart => &["type", "runId"],
        EffectType::ProviderTurnInterrupt => &[
            "type",
            "providerSessionId",
            "providerThreadId",
            "providerTurnId",
            "subagent",
        ],
        EffectType::ProviderTurnSteer => &[
            "type",
            "providerSessionId",
            "providerThreadId",
            "providerTurnId",
            "messageId",
            "modelSelection",
            "steerFollowUpAfterRunOrdinal",
        ],
        EffectType::ProviderTurnRestart => &[
            "type",
            "providerSessionId",
            "providerThreadId",
            "providerTurnId",
            "interruptedAttemptId",
            "runId",
            "sessionTransition",
        ],
        EffectType::RuntimeRequestRespond => &[
            "type",
            "providerSessionId",
            "requestId",
            "decision",
            "feedback",
            "answers",
        ],
        EffectType::ProviderThreadRollback => &[
            "type",
            "restoreFiles",
            "providerThreadId",
            "checkpointId",
            "scopeId",
        ],
        EffectType::CheckpointCapture => &["type", "runId", "scopeId"],
        EffectType::ScheduledTasksCleanup
        | EffectType::TerminalCleanup
        | EffectType::PreviewCleanup => &["type"],
        EffectType::AttachmentCleanup => &["type", "attachmentIds"],
        EffectType::ThreadTitleGenerate => &["type", "kind"],
        EffectType::DelegatedTasksStop => &["type", "reason"],
    };
    let mut normalized = select_fields(request, fields)?;
    for field in [
        "sourceRunId",
        "providerSessionId",
        "runId",
        "providerThreadId",
        "providerTurnId",
        "messageId",
        "interruptedAttemptId",
        "requestId",
        "checkpointId",
        "scopeId",
    ] {
        trim_request_id(&mut normalized, field)?;
    }
    if let Some(value) = normalized.get_mut("modelSelection") {
        *value = select_fields(value, &["instanceId", "model", "options"])?;
        if let Some(options) = value.get_mut("options") {
            let options = options
                .as_array_mut()
                .ok_or_else(|| invalid("model options must be an array"))?;
            for option in options {
                *option = select_fields(option, &["id", "value"])?;
            }
        }
        let command = crate::project_validation::validate_command(&serde_json::json!({
            "type": "project.meta.update", "commandId": "validation", "projectId": "validation",
            "defaultModelSelection": value,
        }))?;
        *value = command["defaultModelSelection"].clone();
    }
    if let Some(value) = normalized.get_mut("subagent") {
        *value = select_fields(value, &["id", "nativeTaskId"])?;
        trim_request_id(value, "id")?;
    }
    if let Some(value) = normalized.get_mut("sessionTransition") {
        let fields: &[&str] = match value.get("type").and_then(Value::as_str) {
            Some("replace") => &["type", "replacementProviderSessionId"],
            Some("detach") => &["type"],
            _ => return Err(invalid("invalid outbox session transition")),
        };
        *value = select_fields(value, fields)?;
        trim_request_id(value, "replacementProviderSessionId")?;
    }
    if let Some(value) = normalized.get_mut("kind") {
        let fields: &[&str] = match value.get("type").and_then(Value::as_str) {
            Some("initial") => &["type", "messageId"],
            Some("regenerate") => &["type"],
            _ => return Err(invalid("invalid title generation kind")),
        };
        *value = select_fields(value, fields)?;
        trim_request_id(value, "messageId")?;
    }
    validate_request(&normalized)?;
    Ok(normalized)
}

fn validate_request(request: &Value) -> Result<()> {
    let kind = request_type(request)?;
    let object = request
        .as_object()
        .ok_or_else(|| invalid("outbox request must be an object"))?;
    let ids: &[&str] = match kind {
        EffectType::ProviderRuntimeContinue => &["sourceRunId"],
        EffectType::ProviderSessionDetach => &["providerSessionId"],
        EffectType::ProviderSessionApplyModelSelection => {
            &["providerSessionId", "providerThreadId"]
        }
        EffectType::ProviderTurnStart => &["runId"],
        EffectType::ProviderTurnInterrupt => {
            &["providerSessionId", "providerThreadId", "providerTurnId"]
        }
        EffectType::ProviderTurnSteer => &[
            "providerSessionId",
            "providerThreadId",
            "providerTurnId",
            "messageId",
        ],
        EffectType::ProviderTurnRestart => &[
            "providerSessionId",
            "providerThreadId",
            "providerTurnId",
            "interruptedAttemptId",
            "runId",
        ],
        EffectType::RuntimeRequestRespond => &["providerSessionId", "requestId"],
        EffectType::ProviderThreadRollback => &["providerThreadId", "checkpointId", "scopeId"],
        EffectType::CheckpointCapture => &["runId", "scopeId"],
        _ => &[],
    };
    for key in ids {
        validate_request_id(
            object
                .get(*key)
                .and_then(Value::as_str)
                .ok_or_else(|| invalid("outbox request is missing a required identifier"))?,
        )?;
    }
    for key in ["detail", "feedback", "reason"] {
        if object.get(key).is_some_and(|value| !value.is_string()) {
            return Err(invalid("outbox optional text must be a string"));
        }
    }
    for key in ["revokeMcpCredential", "restoreFiles"] {
        if object.get(key).is_some_and(|value| !value.is_boolean()) {
            return Err(invalid("outbox optional flag must be a boolean"));
        }
    }
    if let Some(value) = object.get("decision")
        && !matches!(
            value.as_str(),
            Some("accept" | "acceptForSession" | "acceptAlways" | "decline" | "cancel")
        )
    {
        return Err(invalid("outbox approval decision is invalid"));
    }
    if object
        .get("answers")
        .is_some_and(|value| !value.is_object())
    {
        return Err(invalid("outbox user-input answers must be an object"));
    }
    if kind == EffectType::ProviderSessionApplyModelSelection
        && !object.contains_key("modelSelection")
    {
        return Err(invalid("model selection effect requires modelSelection"));
    }
    if let Some(value) = object.get("steerFollowUpAfterRunOrdinal") {
        if value.as_u64().is_none_or(|value| value > MAX_COUNTER) {
            return Err(invalid(
                "steer follow-up ordinal must be a nonnegative safe integer",
            ));
        }
    }
    if kind == EffectType::AttachmentCleanup {
        let ids = object
            .get("attachmentIds")
            .and_then(Value::as_array)
            .ok_or_else(|| invalid("attachment cleanup requires attachmentIds"))?;
        if ids.len() > MAX_BATCH_RECORDS || ids.iter().any(|id| !id.is_string()) {
            return Err(invalid("invalid outbox attachment identifiers"));
        }
    }
    if kind == EffectType::ThreadTitleGenerate {
        let value = object
            .get("kind")
            .ok_or_else(|| invalid("title generation requires kind"))?;
        match value.get("type").and_then(Value::as_str) {
            Some("regenerate") => {}
            Some("initial") => validate_request_id(
                value
                    .get("messageId")
                    .and_then(Value::as_str)
                    .ok_or_else(|| invalid("initial title generation requires messageId"))?,
            )?,
            _ => return Err(invalid("invalid title generation kind")),
        }
    }
    if let Some(value) = object.get("subagent") {
        validate_request_id(
            value
                .get("id")
                .and_then(Value::as_str)
                .ok_or_else(|| invalid("outbox subagent requires id"))?,
        )?;
        if value.get("nativeTaskId").and_then(Value::as_str).is_none() {
            return Err(invalid("outbox subagent requires nativeTaskId"));
        }
    }
    if let Some(value) = object.get("sessionTransition") {
        match value.get("type").and_then(Value::as_str) {
            Some("detach") => {}
            Some("replace") => validate_request_id(
                value
                    .get("replacementProviderSessionId")
                    .and_then(Value::as_str)
                    .ok_or_else(|| invalid("session replacement requires provider session id"))?,
            )?,
            _ => return Err(invalid("invalid outbox session transition")),
        }
    }
    Ok(())
}

pub fn validate_enqueue(effects: &[EnqueueEffect], now: &str) -> Result<()> {
    validate_timestamp(now)?;
    if effects.len() > MAX_BATCH_RECORDS {
        return Err(invalid("too many outbox effects"));
    }
    if crate::encode(&effects)?.len() > MAX_BATCH_BYTES {
        return Err(invalid("outbox enqueue exceeds 16 MiB"));
    }
    for effect in effects {
        crate::validate_id(&effect.id)?;
        crate::validate_id(&effect.command_id)?;
        crate::validate_id(&effect.thread_id)?;
        normalize_request(&effect.request)?;
        if let Some(at) = &effect.available_at {
            validate_timestamp(at)?;
        }
    }
    Ok(())
}

pub fn validate_cancel(input: &CancelUnsettled) -> Result<()> {
    crate::validate_id(&input.thread_id)?;
    validate_timestamp(&input.now)?;
    validate_error(&input.reason)?;
    if input.effect_types.len() > 15 {
        return Err(invalid("too many outbox cancellation effect types"));
    }
    Ok(())
}

fn component(value: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut result = String::with_capacity(value.len() * 2);
    for byte in value.bytes() {
        result.push(HEX[usize::from(byte >> 4)] as char);
        result.push(HEX[usize::from(byte & 15)] as char);
    }
    result
}

fn key(repo: Repository<'_>, path: &str) -> Vec<u8> {
    repo.key(path)
}

fn range(repo: Repository<'_>, path: &str) -> (Vec<u8>, Vec<u8>) {
    let start = key(repo, path);
    let mut end = start.clone();
    end.push(255);
    (start, end)
}

fn row_key(repo: Repository<'_>, id: &str) -> Vec<u8> {
    key(repo, &format!("row/{}", component(id)))
}

fn claim_stream_key(repo: Repository<'_>, id: &str) -> Vec<u8> {
    key(repo, &format!("claim-stream/{}", component(id)))
}

fn validate_claim(input: &Claim) -> Result<()> {
    crate::validate_id(&input.worker_id)?;
    if input
        .max_running_title_generations
        .is_some_and(|value| !value.is_finite())
    {
        return Err(invalid("title generation limit must be finite"));
    }
    validate_timestamp(&input.now)
}

fn validate_claim_once(input: &ClaimOnce) -> Result<()> {
    crate::validate_id(&input.stream_id)?;
    validate_claim(&input.claim)?;
    if input.sequence == 0 || input.sequence > MAX_COUNTER {
        return Err(invalid(
            "outbox claim sequence exceeds supported counter range",
        ));
    }
    Ok(())
}

fn decode_claim_cursor(bytes: &[u8], stream_id: &str) -> Result<ClaimCursor> {
    let cursor: ClaimCursor = crate::decode(bytes)?;
    if cursor.stream_id != stream_id
        || crate::validate_id(&cursor.stream_id).is_err()
        || validate_claim(&cursor.claim).is_err()
        || cursor.sequence == 0
        || cursor.sequence > MAX_COUNTER
        || cursor.effect.as_ref().is_some_and(|identity| {
            crate::validate_id(&identity.effect_id).is_err()
                || identity.insertion_ordinal == 0
                || identity.insertion_ordinal > MAX_COUNTER
                || identity.attempt_count == 0
                || identity.attempt_count > MAX_COUNTER
        })
    {
        return Err(Error::IncompatibleStore);
    }
    Ok(cursor)
}

fn lane(effect: &OutboxEffect) -> Result<String> {
    Ok(format!(
        "{}/{}",
        component(&effect.thread_id),
        if request_type(&effect.request)? == EffectType::ThreadTitleGenerate {
            "title"
        } else {
            "work"
        }
    ))
}

fn index_keys(repo: Repository<'_>, effect: &OutboxEffect) -> Result<BTreeSet<Vec<u8>>> {
    let id = component(&effect.id);
    let order = format!("{:016x}/{id}", effect.insertion_ordinal);
    let lane = lane(effect)?;
    let mut indexes = BTreeSet::from([key(
        repo,
        &format!(
            "command/{}/{}/{id}",
            component(&effect.command_id),
            effect.created_at
        ),
    )]);
    match effect.status {
        EffectStatus::Pending => {
            indexes.insert(key(
                repo,
                &format!("pending/{}/{}/{id}", effect.available_at, effect.created_at),
            ));
            indexes.insert(key(repo, &format!("lane-pending/{lane}/{order}")));
            if request_type(&effect.request)? != EffectType::ProviderRuntimeContinue {
                indexes.insert(key(repo, &format!("lane-pending-ordinary/{lane}/{order}")));
            }
            indexes.insert(key(repo, &format!("unsettled/{order}")));
        }
        EffectStatus::Running => {
            indexes.insert(key(repo, &format!("lane-running/{lane}/{order}")));
            indexes.insert(key(repo, &format!("unsettled/{order}")));
        }
        EffectStatus::Succeeded | EffectStatus::Cancelled => {
            indexes.insert(key(repo, &format!("settled/{order}")));
        }
        EffectStatus::Failed => {}
    }
    Ok(indexes)
}

fn validate_row_with_lease(row: &OutboxEffect, retain_lease_expiry: bool) -> Result<()> {
    crate::validate_id(&row.id)?;
    crate::validate_id(&row.command_id)?;
    crate::validate_id(&row.thread_id)?;
    if normalize_request(&row.request)? != row.request {
        return Err(Error::IncompatibleStore);
    }
    for at in [&row.available_at, &row.created_at, &row.updated_at] {
        validate_timestamp(at)?;
    }
    if let Some(at) = &row.completed_at {
        validate_timestamp(at)?;
    }
    if let Some(error) = &row.last_error {
        validate_error(error)?;
    }
    if let Some(owner) = &row.lease_owner {
        crate::validate_id(owner)?;
    }
    if row.insertion_ordinal == 0
        || row.insertion_ordinal > MAX_COUNTER
        || row.attempt_count > MAX_COUNTER
        || (!retain_lease_expiry && row.lease_expires_at.is_some())
        || (row.status == EffectStatus::Running) != row.lease_owner.is_some()
        || matches!(row.status, EffectStatus::Pending | EffectStatus::Running)
            == row.completed_at.is_some()
    {
        return Err(Error::IncompatibleStore);
    }
    Ok(())
}

#[derive(Default)]
struct ReadBudget {
    bytes: usize,
    rows: usize,
}

async fn read_row(
    tx: &mut Transaction,
    repo: Repository<'_>,
    id: &str,
    budget: &mut ReadBudget,
) -> Result<Option<OutboxEffect>> {
    let loaded = match repo {
        Repository::Legacy => tx
            .get(&row_key(repo, id))
            .await?
            .map(|bytes| crate::decode(&bytes).map(|row| (row, bytes.len())))
            .transpose()?,
        Repository::Application(catalog) => {
            crate::application_outbox::load_row(tx, catalog, id).await?
        }
    };
    let Some((row, bytes)) = loaded else {
        return Ok(None);
    };
    budget.bytes = budget.bytes.saturating_add(bytes);
    budget.rows += 1;
    if budget.bytes > MAX_BATCH_BYTES || budget.rows > MAX_BATCH_RECORDS {
        return Err(invalid("outbox operation exceeds its read budget"));
    }
    repo.validate(&row).map_err(|_| Error::IncompatibleStore)?;
    if row.id != id {
        return Err(Error::IncompatibleStore);
    }
    Ok(Some(row))
}

async fn indexed_row(
    tx: &mut Transaction,
    repo: Repository<'_>,
    entry: &(Vec<u8>, Vec<u8>),
    budget: &mut ReadBudget,
) -> Result<OutboxEffect> {
    let id = std::str::from_utf8(&entry.1).map_err(|_| Error::IncompatibleStore)?;
    crate::validate_id(id).map_err(|_| Error::IncompatibleStore)?;
    let row = read_row(tx, repo, id, budget)
        .await?
        .ok_or(Error::IncompatibleStore)?;
    if !index_keys(repo, &row)?.contains(&entry.0) {
        return Err(Error::IncompatibleStore);
    }
    Ok(row)
}

async fn save_row(
    tx: &mut Transaction,
    repo: Repository<'_>,
    previous: Option<&OutboxEffect>,
    next: &OutboxEffect,
) -> Result<()> {
    repo.validate(next)?;
    let old = previous
        .map(|row| index_keys(repo, row))
        .transpose()?
        .unwrap_or_default();
    let new = index_keys(repo, next)?;
    for key in old.difference(&new) {
        tx.delete(key.clone()).await?;
    }
    for key in new.difference(&old) {
        tx.insert(key.clone(), next.id.as_bytes().to_vec()).await?;
    }
    match repo {
        Repository::Legacy => tx.put(row_key(repo, &next.id), crate::encode(next)?).await,
        Repository::Application(catalog) => {
            crate::application_outbox::save_row(tx, catalog, next).await
        }
    }
}

/// Bootstrap only auxiliary data. Imported application row bytes remain untouched.
pub(crate) async fn index_imported(
    tx: &mut Transaction,
    catalog: &ApplicationCatalog,
    row: &OutboxEffect,
) -> Result<()> {
    let repo = Repository::Application(catalog);
    repo.validate(row).map_err(|_| Error::IncompatibleStore)?;
    for key in index_keys(repo, row)? {
        tx.insert(key, row.id.as_bytes().to_vec()).await?;
    }
    let counter_key = key(repo, "counter");
    let previous = tx
        .get(&counter_key)
        .await?
        .as_deref()
        .map(crate::decode::<u64>)
        .transpose()?
        .unwrap_or(0);
    if previous > MAX_COUNTER {
        return Err(Error::IncompatibleStore);
    }
    if row.insertion_ordinal > previous {
        tx.put(counter_key, crate::encode(&row.insertion_ordinal)?)
            .await?;
    }
    Ok(())
}

fn policy_row(row: &OutboxEffect) -> Result<policy::Row> {
    Ok(policy::Row {
        id: row.id.clone(),
        thread_id: row.thread_id.clone(),
        effect_type: policy::EffectType::from_wire(request_type(&row.request)?.as_wire())
            .ok_or(Error::IncompatibleStore)?,
        status: policy::EffectStatus::from_wire(row.status.as_wire())
            .ok_or(Error::IncompatibleStore)?,
        insertion_ordinal: row.insertion_ordinal,
        available_at: row.available_at.clone(),
        created_at: row.created_at.clone(),
        updated_at: row.updated_at.clone(),
        completed_at: row.completed_at.clone(),
        last_error: row.last_error.clone(),
        lease_owner: row.lease_owner.clone(),
        attempt_count: row.attempt_count,
    })
}

async fn transition(
    tx: &mut Transaction,
    repo: Repository<'_>,
    row: &OutboxEffect,
    action: policy::Action,
    now: &str,
) -> Result<Option<OutboxEffect>> {
    let Some(changed) = policy::transition(&policy_row(row)?, action, now)
        .map_err(|_| invalid("outbox policy rejected transition"))?
    else {
        return Ok(None);
    };
    let next = OutboxEffect {
        status: EffectStatus::from_wire(changed.status.as_wire())
            .ok_or(Error::IncompatibleStore)?,
        attempt_count: changed.attempt_count,
        available_at: changed.available_at,
        lease_owner: changed.lease_owner,
        updated_at: changed.updated_at,
        completed_at: changed.completed_at,
        last_error: changed.last_error,
        ..row.clone()
    };
    if next == *row {
        return Ok(None);
    }
    save_row(tx, repo, Some(row), &next).await?;
    Ok(Some(next))
}

pub(crate) async fn stage_enqueue_in(
    tx: &mut Transaction,
    repo: Repository<'_>,
    effects: &[EnqueueEffect],
    now: &str,
) -> Result<usize> {
    validate_enqueue(effects, now)?;
    let mut budget = ReadBudget::default();
    let mut counter = None;
    let mut inserted = 0;
    for effect in effects {
        if read_row(tx, repo, &effect.id, &mut budget).await?.is_some() {
            continue;
        }
        let last = match counter {
            Some(last) => last,
            None => match tx.get(&key(repo, "counter")).await? {
                Some(bytes) => crate::decode::<u64>(&bytes)?,
                None => 0,
            },
        };
        if last >= MAX_COUNTER {
            return Err(invalid("outbox insertion ordinal exhausted"));
        }
        let row = OutboxEffect {
            id: effect.id.clone(),
            command_id: effect.command_id.clone(),
            thread_id: effect.thread_id.clone(),
            request: normalize_request(&effect.request)?,
            status: EffectStatus::Pending,
            attempt_count: 0,
            available_at: effect
                .available_at
                .clone()
                .unwrap_or_else(|| now.to_owned()),
            lease_owner: None,
            lease_expires_at: None,
            created_at: now.to_owned(),
            updated_at: now.to_owned(),
            completed_at: None,
            last_error: None,
            insertion_ordinal: last + 1,
        };
        save_row(tx, repo, None, &row).await?;
        counter = Some(last + 1);
        inserted += 1;
    }
    if let Some(counter) = counter {
        tx.put(key(repo, "counter"), crate::encode(&counter)?)
            .await?;
    }
    Ok(inserted)
}

async fn page(
    tx: &mut Transaction,
    repo: Repository<'_>,
    path: &str,
    after: Option<&Vec<u8>>,
    limit: usize,
) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
    let (mut start, end) = range(repo, path);
    if let Some(after) = after {
        start = after.clone();
        start.push(0);
    }
    tx.scan(&start, &end, limit).await
}

async fn lane_blockers(
    tx: &mut Transaction,
    repo: Repository<'_>,
    row: &OutboxEffect,
    exclude: bool,
    budget: &mut ReadBudget,
) -> Result<Vec<policy::Row>> {
    let lane = lane(row)?;
    let pending = if exclude {
        "lane-pending-ordinary"
    } else {
        "lane-pending"
    };
    let mut rows = Vec::new();
    for path in [
        format!("{pending}/{lane}/"),
        format!("lane-running/{lane}/"),
    ] {
        for entry in page(tx, repo, &path, None, 1).await? {
            if entry.1 == row.id.as_bytes() {
                if !index_keys(repo, row)?.contains(&entry.0) {
                    return Err(Error::IncompatibleStore);
                }
                rows.push(policy_row(row)?);
            } else {
                rows.push(policy_row(&indexed_row(tx, repo, &entry, budget).await?)?);
            }
        }
    }
    Ok(rows)
}

async fn title_limit_reached(
    tx: &mut Transaction,
    repo: Repository<'_>,
    limit: f64,
    budget: &mut ReadBudget,
) -> Result<bool> {
    if !limit.is_finite() {
        return Err(invalid("title generation limit must be finite"));
    }
    if limit <= 0.0 {
        return Ok(true);
    }
    let mut count = 0_u64;
    let mut after = None;
    loop {
        let entries = page(tx, repo, "lane-running/", after.as_ref(), PAGE_SIZE).await?;
        for entry in &entries {
            let row = indexed_row(tx, repo, entry, budget).await?;
            if request_type(&row.request)? == EffectType::ThreadTitleGenerate {
                count += 1;
                if count as f64 >= limit {
                    return Ok(true);
                }
            }
        }
        if entries.len() < PAGE_SIZE {
            return Ok(false);
        }
        after = entries.last().map(|entry| entry.0.clone());
    }
}

async fn candidate(
    tx: &mut Transaction,
    repo: Repository<'_>,
    now: Option<&str>,
    exclude: bool,
    max_running_title_generations: Option<f64>,
) -> Result<Option<OutboxEffect>> {
    let mut after = None;
    let mut budget = ReadBudget::default();
    let mut titles_blocked = None;
    loop {
        let entries = page(tx, repo, "pending/", after.as_ref(), PAGE_SIZE).await?;
        for entry in &entries {
            let row = indexed_row(tx, repo, entry, &mut budget).await?;
            if now.is_some_and(|now| row.available_at.as_str() > now) {
                return Ok(None);
            }
            if exclude && request_type(&row.request)? == EffectType::ProviderRuntimeContinue {
                continue;
            }
            if request_type(&row.request)? == EffectType::ThreadTitleGenerate {
                if let Some(limit) = max_running_title_generations {
                    let blocked = match titles_blocked {
                        Some(value) => value,
                        None => {
                            let value = title_limit_reached(tx, repo, limit, &mut budget).await?;
                            titles_blocked = Some(value);
                            value
                        }
                    };
                    if blocked {
                        continue;
                    }
                }
            }
            let blockers = lane_blockers(tx, repo, &row, exclude, &mut budget).await?;
            if policy::claimable(&policy_row(&row)?, &blockers, now, exclude) {
                return Ok(Some(row));
            }
        }
        if entries.len() < PAGE_SIZE {
            return Ok(None);
        }
        after = entries.last().map(|entry| entry.0.clone());
    }
}

pub(crate) async fn stage_claim_in(
    tx: &mut Transaction,
    repo: Repository<'_>,
    input: &Claim,
) -> Result<Option<OutboxEffect>> {
    validate_claim(input)?;
    match candidate(
        tx,
        repo,
        Some(&input.now),
        input.exclude_restart_continuations,
        input.max_running_title_generations,
    )
    .await?
    {
        Some(row) => {
            transition(
                tx,
                repo,
                &row,
                policy::Action::Claim {
                    worker_id: input.worker_id.clone(),
                },
                &input.now,
            )
            .await
        }
        None => Ok(None),
    }
}

async fn replay_claim(
    tx: &mut Transaction,
    repo: Repository<'_>,
    cursor: &ClaimCursor,
) -> Result<ClaimOutcome> {
    let Some(identity) = &cursor.effect else {
        return Ok(ClaimOutcome {
            state: ClaimState::Empty,
            replayed: true,
            effect: None,
        });
    };
    let effect = read_row(tx, repo, &identity.effect_id, &mut ReadBudget::default())
        .await?
        .filter(|row| {
            row.status == EffectStatus::Running
                && row.lease_owner.as_deref() == Some(cursor.claim.worker_id.as_str())
                && row.insertion_ordinal == identity.insertion_ordinal
                && row.attempt_count == identity.attempt_count
        });
    Ok(ClaimOutcome {
        state: if effect.is_some() {
            ClaimState::Claimed
        } else {
            ClaimState::Retired
        },
        replayed: true,
        effect,
    })
}

pub(crate) async fn stage_claim_once_in(
    tx: &mut Transaction,
    repo: Repository<'_>,
    input: &ClaimOnce,
) -> Result<ClaimOutcome> {
    validate_claim_once(input)?;
    let cursor_key = claim_stream_key(repo, &input.stream_id);
    if let Some(bytes) = tx.get(&cursor_key).await? {
        let cursor = decode_claim_cursor(&bytes, &input.stream_id)?;
        if input.sequence == cursor.sequence {
            if input.claim != cursor.claim {
                return Err(invalid("outbox claim sequence belongs to another request"));
            }
            return replay_claim(tx, repo, &cursor).await;
        }
        if input.sequence != cursor.sequence + 1 {
            return Err(invalid(
                "outbox claim sequence must follow the retained cursor",
            ));
        }
        if input.claim.worker_id != cursor.claim.worker_id {
            return Err(invalid("outbox claim stream belongs to another worker"));
        }
    } else if input.sequence != 1 {
        return Err(invalid("outbox claim stream must start at sequence one"));
    }
    let effect = stage_claim_in(tx, repo, &input.claim).await?;
    let cursor = ClaimCursor {
        stream_id: input.stream_id.clone(),
        sequence: input.sequence,
        claim: input.claim.clone(),
        effect: effect.as_ref().map(|row| ClaimIdentity {
            effect_id: row.id.clone(),
            insertion_ordinal: row.insertion_ordinal,
            attempt_count: row.attempt_count,
        }),
    };
    tx.put(cursor_key, crate::encode(&cursor)?).await?;
    Ok(ClaimOutcome {
        state: if effect.is_some() {
            ClaimState::Claimed
        } else {
            ClaimState::Empty
        },
        replayed: false,
        effect,
    })
}

pub(crate) async fn stage_cancel_unsettled_in(
    tx: &mut Transaction,
    repo: Repository<'_>,
    input: &CancelUnsettled,
) -> Result<Vec<String>> {
    validate_cancel(input)?;
    if input.effect_types.is_empty() {
        return Ok(Vec::new());
    }
    let mut budget = ReadBudget::default();
    let mut matches = BTreeMap::new();
    for lane in ["title", "work"] {
        for status in ["lane-pending", "lane-running"] {
            let path = format!("{status}/{}/{lane}/", component(&input.thread_id));
            let mut after = None;
            loop {
                let entries = page(tx, repo, &path, after.as_ref(), PAGE_SIZE).await?;
                for entry in &entries {
                    let row = indexed_row(tx, repo, entry, &mut budget).await?;
                    if input.effect_types.contains(&request_type(&row.request)?) {
                        matches.insert(row.insertion_ordinal, row);
                    }
                }
                if entries.len() < PAGE_SIZE {
                    break;
                }
                after = entries.last().map(|entry| entry.0.clone());
            }
        }
    }
    let mut cancelled = Vec::new();
    for row in matches.into_values() {
        if transition(
            tx,
            repo,
            &row,
            policy::Action::Cancel {
                reason: input.reason.clone(),
            },
            &input.now,
        )
        .await?
        .is_some()
        {
            cancelled.push(row.id);
        }
    }
    Ok(cancelled)
}

async fn finish<T>(tx: Transaction, result: Result<T>, changed: bool) -> Result<T> {
    match result {
        Ok(value) => {
            if changed {
                tx.commit().await?;
            } else {
                tx.rollback().await?;
            }
            Ok(value)
        }
        Err(error) => {
            let _ = tx.rollback().await;
            Err(error)
        }
    }
}

// The component entry points and application sessions share these transaction-local operations.
pub(crate) async fn stage_enqueue(
    tx: &mut Transaction,
    effects: &[EnqueueEffect],
    now: &str,
) -> Result<usize> {
    stage_enqueue_in(tx, Repository::Legacy, effects, now).await
}

pub(crate) async fn stage_cancel_unsettled(
    tx: &mut Transaction,
    input: &CancelUnsettled,
) -> Result<Vec<String>> {
    stage_cancel_unsettled_in(tx, Repository::Legacy, input).await
}

pub(crate) async fn stage_get_in(
    tx: &mut Transaction,
    repo: Repository<'_>,
    id: &str,
) -> Result<Option<OutboxEffect>> {
    crate::validate_id(id)?;
    let row = read_row(tx, repo, id, &mut ReadBudget::default()).await?;
    crate::validate_response_bytes(&row)?;
    Ok(row)
}

pub(crate) async fn stage_list_in(
    tx: &mut Transaction,
    repo: Repository<'_>,
    command_id: &str,
) -> Result<Vec<OutboxEffect>> {
    crate::validate_id(command_id)?;
    let path = format!("command/{}/", component(command_id));
    let mut rows = Vec::new();
    let mut after = None;
    let mut budget = ReadBudget::default();
    loop {
        let entries = page(tx, repo, &path, after.as_ref(), PAGE_SIZE).await?;
        for entry in &entries {
            rows.push(indexed_row(tx, repo, entry, &mut budget).await?);
        }
        if entries.len() < PAGE_SIZE {
            crate::validate_response_bytes(&rows)?;
            return Ok(rows);
        }
        after = entries.last().map(|entry| entry.0.clone());
    }
}

pub(crate) async fn stage_next_in(
    tx: &mut Transaction,
    repo: Repository<'_>,
) -> Result<Option<String>> {
    stage_next_limited_in(tx, repo, None).await
}

pub(crate) async fn stage_next_limited_in(
    tx: &mut Transaction,
    repo: Repository<'_>,
    max_running_title_generations: Option<f64>,
) -> Result<Option<String>> {
    if max_running_title_generations.is_some_and(|limit| !limit.is_finite()) {
        return Err(invalid("title generation limit must be finite"));
    }
    candidate(tx, repo, None, false, max_running_title_generations)
        .await
        .map(|row| row.map(|row| row.available_at))
}

async fn stage_settle_in(
    tx: &mut Transaction,
    repo: Repository<'_>,
    input: &Settle,
    action: policy::Action,
) -> Result<bool> {
    crate::validate_id(&input.effect_id)?;
    crate::validate_id(&input.worker_id)?;
    validate_timestamp(&input.now)?;
    let Some(row) = read_row(tx, repo, &input.effect_id, &mut ReadBudget::default()).await? else {
        return Ok(false);
    };
    transition(tx, repo, &row, action, &input.now)
        .await
        .map(|row| row.is_some())
}

pub(crate) async fn stage_succeed_in(
    tx: &mut Transaction,
    repo: Repository<'_>,
    input: &Settle,
) -> Result<bool> {
    stage_settle_in(
        tx,
        repo,
        input,
        policy::Action::Succeed {
            worker_id: input.worker_id.clone(),
        },
    )
    .await
}

pub(crate) async fn stage_retry_in(
    tx: &mut Transaction,
    repo: Repository<'_>,
    input: &Retry,
) -> Result<bool> {
    validate_timestamp(&input.available_at)?;
    validate_error(&input.error)?;
    if input.available_at < input.now {
        return Err(invalid("outbox retry deadline precedes now"));
    }
    stage_settle_in(
        tx,
        repo,
        &Settle {
            effect_id: input.effect_id.clone(),
            worker_id: input.worker_id.clone(),
            now: input.now.clone(),
        },
        policy::Action::Retry {
            worker_id: input.worker_id.clone(),
            error: input.error.clone(),
            available_at: input.available_at.clone(),
        },
    )
    .await
}

pub(crate) async fn stage_fail_in(
    tx: &mut Transaction,
    repo: Repository<'_>,
    input: &Failure,
) -> Result<bool> {
    validate_error(&input.error)?;
    stage_settle_in(
        tx,
        repo,
        &Settle {
            effect_id: input.effect_id.clone(),
            worker_id: input.worker_id.clone(),
            now: input.now.clone(),
        },
        policy::Action::Fail {
            worker_id: input.worker_id.clone(),
            error: input.error.clone(),
        },
    )
    .await
}

pub(crate) async fn stage_reconcile_in(
    tx: &mut Transaction,
    repo: Repository<'_>,
    input: &Reconcile,
) -> Result<ReconcileOutcome> {
    validate_timestamp(&input.now)?;
    let mut counts = ReconcileOutcome {
        requeued: 0,
        cancelled: 0,
    };
    let mut after = None;
    let mut budget = ReadBudget::default();
    loop {
        let entries = page(tx, repo, "unsettled/", after.as_ref(), PAGE_SIZE).await?;
        for entry in &entries {
            let row = indexed_row(tx, repo, entry, &mut budget).await?;
            if let Some(next) =
                transition(tx, repo, &row, policy::Action::Reconcile, &input.now).await?
            {
                match next.status {
                    EffectStatus::Pending => counts.requeued += 1,
                    EffectStatus::Cancelled => counts.cancelled += 1,
                    _ => return Err(Error::IncompatibleStore),
                }
            }
        }
        if entries.len() < PAGE_SIZE {
            return Ok(counts);
        }
        after = entries.last().map(|entry| entry.0.clone());
    }
}

pub(crate) async fn stage_prune_in(
    tx: &mut Transaction,
    repo: Repository<'_>,
    input: &Prune,
) -> Result<usize> {
    validate_timestamp(&input.before)?;
    if input.limit == 0 || input.limit as usize > policy::PRUNE_BATCH_SIZE {
        return Err(invalid("outbox prune limit must be between 1 and 500"));
    }
    let mut removed = 0;
    let mut after = None;
    let mut budget = ReadBudget::default();
    loop {
        let entries = page(tx, repo, "settled/", after.as_ref(), PAGE_SIZE).await?;
        for entry in &entries {
            let row = indexed_row(tx, repo, entry, &mut budget).await?;
            if policy::prunable(&policy_row(&row)?, &input.before) {
                for key in index_keys(repo, &row)? {
                    tx.delete(key).await?;
                }
                match repo {
                    Repository::Legacy => tx.delete(row_key(repo, &row.id)).await?,
                    Repository::Application(catalog) => {
                        crate::application_outbox::delete_row(tx, catalog, &row).await?
                    }
                }
                removed += 1;
                if removed == input.limit as usize {
                    return Ok(removed);
                }
            }
        }
        if entries.len() < PAGE_SIZE {
            return Ok(removed);
        }
        after = entries.last().map(|entry| entry.0.clone());
    }
}

impl Store {
    pub async fn outbox_get(&self, id: &str) -> Result<Option<OutboxEffect>> {
        let mut tx = self.db.begin().await?;
        let result = stage_get_in(&mut tx, Repository::Legacy, id).await;
        finish(tx, result, false).await
    }

    pub async fn outbox_list_by_command(&self, command_id: &str) -> Result<Vec<OutboxEffect>> {
        let mut tx = self.db.begin().await?;
        let result = stage_list_in(&mut tx, Repository::Legacy, command_id).await;
        finish(tx, result, false).await
    }

    pub async fn outbox_enqueue(&self, effects: &[EnqueueEffect], now: &str) -> Result<usize> {
        let mut tx = self.db.begin().await?;
        let result = stage_enqueue(&mut tx, effects, now).await;
        let changed = result.as_ref().is_ok_and(|count| *count > 0);
        finish(tx, result, changed).await
    }

    pub async fn outbox_claim_next(&self, input: &Claim) -> Result<Option<OutboxEffect>> {
        validate_claim(input)?;
        let mut tx = self.db.begin().await?;
        let result = stage_claim_in(&mut tx, Repository::Legacy, input).await;
        let changed = result.as_ref().is_ok_and(Option::is_some);
        finish(tx, result, changed).await
    }

    /// Keep the same stream, sequence and input until a claim response is resolved.
    /// Use a new stream per worker incarnation; database reconnects retain the original stream.
    pub async fn outbox_claim_once(&self, input: &ClaimOnce) -> Result<ClaimOutcome> {
        let mut tx = self.db.begin().await?;
        let result = stage_claim_once_in(&mut tx, Repository::Legacy, input)
            .await
            .and_then(|outcome| {
                crate::validate_response_bytes(&outcome)?;
                Ok(outcome)
            });
        let changed = result.as_ref().is_ok_and(|outcome| !outcome.replayed);
        finish(tx, result, changed).await
    }

    pub async fn outbox_next_claimable_at(&self) -> Result<Option<String>> {
        let mut tx = self.db.begin().await?;
        let result = stage_next_in(&mut tx, Repository::Legacy).await;
        finish(tx, result, false).await
    }

    pub async fn outbox_next_claimable_at_limited(
        &self,
        max_running_title_generations: Option<f64>,
    ) -> Result<Option<String>> {
        let mut tx = self.db.begin().await?;
        let result =
            stage_next_limited_in(&mut tx, Repository::Legacy, max_running_title_generations).await;
        finish(tx, result, false).await
    }

    pub async fn outbox_succeed(&self, input: &Settle) -> Result<bool> {
        let mut tx = self.db.begin().await?;
        let result = stage_succeed_in(&mut tx, Repository::Legacy, input).await;
        let changed = result.as_ref().is_ok_and(|value| *value);
        finish(tx, result, changed).await
    }

    pub async fn outbox_retry(&self, input: &Retry) -> Result<bool> {
        let mut tx = self.db.begin().await?;
        let result = stage_retry_in(&mut tx, Repository::Legacy, input).await;
        let changed = result.as_ref().is_ok_and(|value| *value);
        finish(tx, result, changed).await
    }

    pub async fn outbox_fail(&self, input: &Failure) -> Result<bool> {
        let mut tx = self.db.begin().await?;
        let result = stage_fail_in(&mut tx, Repository::Legacy, input).await;
        let changed = result.as_ref().is_ok_and(|value| *value);
        finish(tx, result, changed).await
    }

    pub async fn outbox_cancel_unsettled(&self, input: &CancelUnsettled) -> Result<Vec<String>> {
        let mut tx = self.db.begin().await?;
        let result = stage_cancel_unsettled(&mut tx, input).await;
        let changed = result.as_ref().is_ok_and(|ids| !ids.is_empty());
        finish(tx, result, changed).await
    }

    pub async fn outbox_reconcile(&self, input: &Reconcile) -> Result<ReconcileOutcome> {
        let mut tx = self.db.begin().await?;
        let result = stage_reconcile_in(&mut tx, Repository::Legacy, input).await;
        let changed = result
            .as_ref()
            .is_ok_and(|counts| counts.requeued + counts.cancelled > 0);
        finish(tx, result, changed).await
    }

    pub async fn outbox_prune(&self, input: &Prune) -> Result<usize> {
        let mut tx = self.db.begin().await?;
        let result = stage_prune_in(&mut tx, Repository::Legacy, input).await;
        let changed = result.as_ref().is_ok_and(|count| *count > 0);
        finish(tx, result, changed).await
    }
}

#[cfg(test)]
#[path = "outbox_tests.rs"]
mod tests;
