//! Selected, unwindowed projection records from one native read snapshot.
//! Relationship indexes constrain reads to the requested thread; the existing
//! Effect service still owns domain decoding and display-message ordering.

use std::collections::{BTreeMap, BTreeSet};

use super::read_index::window::{equal_metadata as compact_rows, read_metadata as compact_row};
use super::read_index::{IndexedRow, ensure_ready};
use super::*;

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "camelCase")]
pub enum ProjectionRecordField {
    Runs,
    Attempts,
    Nodes,
    Subagents,
    ProviderSessions,
    ProviderThreads,
    ProviderTurns,
    RuntimeRequests,
    Messages,
    Plans,
    TurnItems,
    CheckpointScopes,
    Checkpoints,
    ContextHandoffs,
    ContextTransfers,
}

impl ProjectionRecordField {
    fn table(self) -> &'static str {
        match self {
            Self::Runs => RUNS,
            Self::Attempts => ATTEMPTS,
            Self::Nodes => NODES,
            Self::Subagents => SUBAGENTS,
            Self::ProviderSessions => SESSIONS,
            Self::ProviderThreads => PROVIDER_THREADS,
            Self::ProviderTurns => TURNS,
            Self::RuntimeRequests => REQUESTS,
            Self::Messages => MESSAGES,
            Self::Plans => PLANS,
            Self::TurnItems => ITEMS,
            Self::CheckpointScopes => SCOPES,
            Self::Checkpoints => CHECKPOINTS,
            Self::ContextHandoffs => HANDOFFS,
            Self::ContextTransfers => TRANSFERS,
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ProjectionRecordFilter {
    pub provider_thread_ids: Option<Vec<String>>,
    pub provider_turn_attempt_ids: Option<Vec<String>>,
    pub provider_turn_thread_ids: Option<Vec<String>>,
    pub latest_provider_turn_usage_only: Option<bool>,
    pub message_roles: Option<Vec<String>>,
    pub monitor_notifications_only: Option<bool>,
    pub message_ids: Option<Vec<String>>,
    pub message_run_ids: Option<Vec<String>>,
    pub turn_item_run_id: Option<String>,
    pub turn_item_run_ids: Option<Vec<Option<String>>>,
    pub turn_item_node_ids: Option<Vec<String>>,
    pub turn_item_request_ids: Option<Vec<String>>,
    pub turn_item_types: Option<Vec<String>>,
    pub turn_item_statuses: Option<Vec<String>>,
    pub run_ids: Option<Vec<String>>,
    pub run_statuses: Option<Vec<String>>,
    pub include_latest_run: Option<bool>,
    pub runtime_request_statuses: Option<Vec<String>>,
    pub context_transfer_statuses: Option<Vec<String>>,
    pub subagent_ids: Option<Vec<String>>,
    pub subagent_run_ids: Option<Vec<String>>,
    pub node_ids: Option<Vec<String>>,
    pub omit_settled_leaf_nodes: Option<bool>,
    pub omit_settled_subagents: Option<OmitSettledSubagents>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(untagged)]
pub enum OmitSettledSubagents {
    All(bool),
    Except { except: Vec<String> },
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectionRecordsQuery {
    pub thread_id: String,
    pub fields: Vec<ProjectionRecordField>,
    pub filter: Option<ProjectionRecordFilter>,
    pub window: Option<window::ProjectionRecordWindow>,
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MessageOrdinal {
    pub message_id: String,
    pub ordinal: i64,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ProjectionRecordResult {
    pub thread: Option<String>,
    pub rows: BTreeMap<ProjectionRecordField, Vec<String>>,
    pub message_ordinals: Vec<MessageOrdinal>,
    pub deferred_work: Vec<window::DeferredRecord>,
}

const FIELDS: [ProjectionRecordField; 15] = [
    ProjectionRecordField::Runs,
    ProjectionRecordField::Attempts,
    ProjectionRecordField::Nodes,
    ProjectionRecordField::Subagents,
    ProjectionRecordField::ProviderSessions,
    ProjectionRecordField::ProviderThreads,
    ProjectionRecordField::ProviderTurns,
    ProjectionRecordField::RuntimeRequests,
    ProjectionRecordField::Messages,
    ProjectionRecordField::Plans,
    ProjectionRecordField::TurnItems,
    ProjectionRecordField::CheckpointScopes,
    ProjectionRecordField::Checkpoints,
    ProjectionRecordField::ContextHandoffs,
    ProjectionRecordField::ContextTransfers,
];
const ACTIVE_RUNS: [&str; 5] = ["preparing", "queued", "starting", "running", "waiting"];

fn payload(row: &Map<String, Value>) -> Result<Value> {
    serde_json::from_str(&text(row, "payload_json")?).map_err(|_| Error::IncompatibleStore)
}
fn selection(values: &Option<Vec<String>>, candidate: Option<&str>) -> bool {
    values.as_ref().is_none_or(|values| {
        candidate.is_some_and(|candidate| values.iter().any(|value| value == candidate))
    })
}
fn selected(values: &Option<Vec<String>>, row: &Map<String, Value>, column: &str) -> Result<bool> {
    if values.is_none() {
        return Ok(true);
    }
    Ok(selection(values, nullable(row, column)?.as_deref()))
}
fn row_id(name: &str, row: &Map<String, Value>) -> Result<String> {
    text(row, spec(name).primary[0])
}
fn unique_rows(name: &str, rows: Vec<IndexedRow>) -> Result<Vec<IndexedRow>> {
    let mut seen = BTreeSet::new();
    let mut result = Vec::new();
    for row in rows {
        if seen.insert(row_id(name, &row.row)?) {
            result.push(row);
        }
    }
    Ok(result)
}

async fn related(
    session: &mut Session,
    name: &str,
    column: &str,
    id: &str,
) -> Result<Vec<IndexedRow>> {
    compact_rows(session, name, column, &text_cell(id)).await
}
async fn active_run(
    session: &mut Session,
    cache: &mut BTreeMap<String, bool>,
    id: Option<String>,
) -> Result<bool> {
    let Some(id) = id else {
        return Ok(false);
    };
    if let Some(active) = cache.get(&id) {
        return Ok(*active);
    }
    let row = compact_row(session, RUNS, &id).await?;
    let active = row
        .map(|row| text(&row.row, "status").map(|status| ACTIVE_RUNS.contains(&status.as_str())))
        .transpose()?
        .unwrap_or(false);
    cache.insert(id, active);
    Ok(active)
}

async fn candidates(
    session: &mut Session,
    field: ProjectionRecordField,
    thread: &str,
) -> Result<Vec<IndexedRow>> {
    match field {
        ProjectionRecordField::ProviderSessions => {
            let mut rows = Vec::new();
            for binding in related(session, BINDINGS, "thread_id", thread).await? {
                rows.extend(
                    related(
                        session,
                        SESSIONS,
                        "provider_session_id",
                        &text(&binding.row, "provider_session_id")?,
                    )
                    .await?,
                );
            }
            unique_rows(SESSIONS, rows)
        }
        ProjectionRecordField::ProviderThreads => {
            let mut rows = related(session, PROVIDER_THREADS, "thread_id", thread).await?;
            // Owner nodes may belong to this app thread while the provider thread
            // itself is shared with, or attached to, another app thread.
            for node in related(session, NODES, "thread_id", thread).await? {
                rows.extend(
                    related(
                        session,
                        PROVIDER_THREADS,
                        "owner_node_id",
                        &text(&node.row, "node_id")?,
                    )
                    .await?,
                );
            }
            for subagent in related(session, SUBAGENTS, "thread_id", thread).await? {
                if let Some(id) = nullable(&subagent.row, "provider_thread_id")? {
                    rows.extend(
                        related(session, PROVIDER_THREADS, "provider_thread_id", &id).await?,
                    );
                }
            }
            unique_rows(PROVIDER_THREADS, rows)
        }
        ProjectionRecordField::ContextTransfers => {
            let mut rows = related(session, TRANSFERS, "source_thread_id", thread).await?;
            rows.extend(related(session, TRANSFERS, "target_thread_id", thread).await?);
            unique_rows(TRANSFERS, rows)
        }
        _ => related(session, field.table(), "thread_id", thread).await,
    }
}

async fn retain(
    session: &mut Session,
    field: ProjectionRecordField,
    row: &Map<String, Value>,
    filter: &ProjectionRecordFilter,
    latest_run: Option<&str>,
    active: &mut BTreeMap<String, bool>,
) -> Result<bool> {
    Ok(match field {
        ProjectionRecordField::Runs => {
            selected(&filter.run_ids, row, "run_id")?
                && (selected(&filter.run_statuses, row, "status")?
                    || (filter.include_latest_run == Some(true)
                        && latest_run == Some(text(row, "run_id")?.as_str())))
        }
        ProjectionRecordField::Nodes => {
            let settled_leaf = filter.omit_settled_leaf_nodes == Some(true)
                && ["tool_call", "assistant_message", "reasoning"]
                    .contains(&text(row, "kind")?.as_str())
                && [
                    "completed",
                    "interrupted",
                    "failed",
                    "cancelled",
                    "rolled_back",
                ]
                .contains(&text(row, "status")?.as_str())
                && nullable(row, "run_id")?.is_some()
                && !active_run(session, active, nullable(row, "run_id")?).await?;
            selected(&filter.node_ids, row, "node_id")? && !settled_leaf
        }
        ProjectionRecordField::Subagents => {
            let keep_exception = match &filter.omit_settled_subagents {
                Some(OmitSettledSubagents::Except { except }) => {
                    except.contains(&text(row, "subagent_id")?)
                }
                _ => false,
            };
            let omit = filter.omit_settled_subagents.is_some()
                && !keep_exception
                && ["completed", "failed", "cancelled", "interrupted"]
                    .contains(&text(row, "status")?.as_str())
                && ["acknowledged", "delivered", "disposed"].contains(
                    &nullable(row, "completion_delivery_state")?
                        .as_deref()
                        .unwrap_or(""),
                )
                && !active_run(session, active, nullable(row, "run_id")?).await?;
            selected(&filter.subagent_ids, row, "subagent_id")?
                && selected(&filter.subagent_run_ids, row, "run_id")?
                && !omit
        }
        ProjectionRecordField::ProviderThreads => {
            selected(&filter.provider_thread_ids, row, "provider_thread_id")?
        }
        ProjectionRecordField::ProviderTurns => {
            selected(&filter.provider_turn_attempt_ids, row, "run_attempt_id")?
                && selected(&filter.provider_turn_thread_ids, row, "provider_thread_id")?
                && (filter.latest_provider_turn_usage_only != Some(true)
                    || payload(row)?["nativeHasTokenUsage"] == true)
        }
        ProjectionRecordField::RuntimeRequests => {
            selected(&filter.runtime_request_statuses, row, "status")?
        }
        ProjectionRecordField::Messages => {
            selected(&filter.message_ids, row, "message_id")?
                && selected(&filter.message_roles, row, "role")?
                && selected(&filter.message_run_ids, row, "run_id")?
                && (filter.monitor_notifications_only != Some(true)
                    || payload(row)?["nativeMonitorNotification"] == true)
        }
        ProjectionRecordField::TurnItems => {
            let run = nullable(row, "run_id")?;
            selected(&filter.turn_item_types, row, "type")?
                && selected(&filter.turn_item_statuses, row, "status")?
                && selected(&filter.turn_item_node_ids, row, "node_id")?
                && filter
                    .turn_item_run_id
                    .as_ref()
                    .is_none_or(|required| run.as_ref() == Some(required))
                && filter
                    .turn_item_run_ids
                    .as_ref()
                    .is_none_or(|runs| runs.contains(&run))
                && (filter.turn_item_request_ids.is_none()
                    || selection(
                        &filter.turn_item_request_ids,
                        payload(row)?["requestId"].as_str(),
                    ))
        }
        ProjectionRecordField::ContextTransfers => {
            selected(&filter.context_transfer_statuses, row, "status")?
        }
        _ => true,
    })
}

#[derive(PartialEq, Eq, PartialOrd, Ord)]
enum SortValue {
    Number(i64),
    Text(String),
}

fn text_order(row: &Map<String, Value>, column: &str) -> Result<SortValue> {
    Ok(SortValue::Text(nullable(row, column)?.unwrap_or_default()))
}
fn number_order(row: &Map<String, Value>, column: &str) -> Result<SortValue> {
    Ok(SortValue::Number(
        if row.get(column).is_some_and(|cell| cell["type"] == "null") {
            0
        } else {
            integer(row, column)?
        },
    ))
}

fn order(field: ProjectionRecordField, row: &Map<String, Value>) -> Result<Vec<SortValue>> {
    Ok(match field {
        ProjectionRecordField::Runs => vec![number_order(row, "ordinal")?],
        ProjectionRecordField::Attempts => vec![
            text_order(row, "run_id")?,
            number_order(row, "attempt_ordinal")?,
        ],
        ProjectionRecordField::Nodes => {
            vec![text_order(row, "started_at")?, text_order(row, "node_id")?]
        }
        ProjectionRecordField::Subagents => vec![
            text_order(row, "started_at")?,
            text_order(row, "subagent_id")?,
        ],
        ProjectionRecordField::ProviderSessions => vec![
            text_order(row, "updated_at")?,
            text_order(row, "provider_session_id")?,
        ],
        ProjectionRecordField::ProviderThreads => vec![
            number_order(row, "first_run_ordinal")?,
            text_order(row, "provider_thread_id")?,
        ],
        ProjectionRecordField::ProviderTurns => vec![
            text_order(row, "provider_thread_id")?,
            number_order(row, "ordinal")?,
        ],
        ProjectionRecordField::RuntimeRequests => vec![
            text_order(row, "created_at")?,
            text_order(row, "runtime_request_id")?,
        ],
        ProjectionRecordField::Messages => vec![
            text_order(row, "created_at")?,
            text_order(row, "message_id")?,
        ],
        ProjectionRecordField::Plans => vec![text_order(row, "plan_id")?],
        ProjectionRecordField::TurnItems => vec![
            number_order(row, "ordinal")?,
            text_order(row, "turn_item_id")?,
        ],
        ProjectionRecordField::CheckpointScopes => vec![
            number_order(row, "ordinal_within_parent")?,
            text_order(row, "scope_id")?,
        ],
        ProjectionRecordField::Checkpoints => vec![
            text_order(row, "scope_id")?,
            number_order(row, "ordinal_within_scope")?,
        ],
        ProjectionRecordField::ContextHandoffs | ProjectionRecordField::ContextTransfers => vec![],
    })
}

// This field is Schema.String, not DateTime. Match the single-argument
// julianday date grammar (including date-only, time-only, numeric Julian days,
// and the statement-stable clock aliases), not only producer-normalized ISO.
// Reference: SQLite's public-domain src/date.c parseDateOrTime/computeJD;
// the immutable application oracle also checks the runtime's date behavior.
pub(crate) const UNIX_EPOCH_JULIAN_MS: i64 = 210_866_760_000_000;
const MAX_JULIAN_MS: i64 = 464_269_060_799_999;

fn valid_timestamp(value: i64) -> Option<i64> {
    (0..=MAX_JULIAN_MS).contains(&value).then_some(value)
}

fn numeric_timestamp(value: f64) -> Option<i64> {
    if !value.is_finite() || !(0.0..5_373_484.5).contains(&value) {
        return None;
    }
    valid_timestamp((value * 86_400_000.0 + 0.5) as i64)
}

fn date_number(bytes: &[u8], start: usize, width: usize) -> Option<i64> {
    let digits = bytes.get(start..start.checked_add(width)?)?;
    if !digits.iter().all(u8::is_ascii_digit) {
        return None;
    }
    Some(
        digits
            .iter()
            .fold(0, |number, digit| number * 10 + i64::from(digit - b'0')),
    )
}

fn time_parts(bytes: &[u8]) -> Option<(i64, i64, i64, i64)> {
    if bytes.get(2) != Some(&b':') {
        return None;
    }
    let (hour, minute) = (date_number(bytes, 0, 2)?, date_number(bytes, 3, 2)?);
    if hour > 24 || minute > 59 {
        return None;
    }
    let mut end = 5;
    let mut seconds = 0.0;
    if bytes.get(end) == Some(&b':') {
        let integer = date_number(bytes, end + 1, 2)?;
        if integer > 59 {
            return None;
        }
        seconds = integer as f64;
        end += 3;
        if bytes.get(end) == Some(&b'.') {
            end += 1;
            let start = end;
            let mut numerator: f64 = 0.0;
            let mut scale: f64 = 1.0;
            while let Some(digit) = bytes.get(end).filter(|digit| digit.is_ascii_digit()) {
                numerator = numerator * 10.0 + f64::from(digit - b'0');
                scale *= 10.0;
                end += 1;
            }
            if start == end || !numerator.is_finite() || !scale.is_finite() {
                return None;
            }
            // SQLite caps sub-millisecond precision before rounding seconds.
            seconds += (numerator / scale).min(0.999);
        }
    }
    while bytes.get(end).is_some_and(u8::is_ascii_whitespace) {
        end += 1;
    }
    let mut offset = 0;
    match bytes.get(end) {
        None => {}
        Some(b'z' | b'Z') => end += 1,
        Some(sign @ (b'+' | b'-')) => {
            if bytes.get(end + 3) != Some(&b':') {
                return None;
            }
            let (h, m) = (
                date_number(bytes, end + 1, 2)?,
                date_number(bytes, end + 4, 2)?,
            );
            if h > 14 || m > 59 {
                return None;
            }
            offset = (h * 60 + m) * if *sign == b'+' { 1 } else { -1 };
            end += 6;
        }
        _ => return None,
    }
    while bytes.get(end).is_some_and(u8::is_ascii_whitespace) {
        end += 1;
    }
    (end == bytes.len()).then_some((hour, minute, (seconds * 1000.0 + 0.5) as i64, offset))
}

pub(crate) fn timestamp(value: &Value, now: i64) -> Option<i64> {
    if let Some(number) = value.as_f64() {
        return numeric_timestamp(number);
    }
    if let Some(boolean) = value.as_bool() {
        return numeric_timestamp(f64::from(u8::from(boolean)));
    }
    let value = value.as_str()?.split('\0').next()?;
    let bytes = value.as_bytes();
    let mut year = 2000;
    let mut month = 1;
    let mut day = 1;
    let sign = usize::from(bytes.first() == Some(&b'-'));
    let is_date = bytes.get(sign + 4) == Some(&b'-') && bytes.get(sign + 7) == Some(&b'-');
    let (hour, minute, second_ms, offset) = if is_date {
        year = date_number(bytes, sign, 4)? * if sign == 1 { -1 } else { 1 };
        month = date_number(bytes, sign + 5, 2)?;
        day = date_number(bytes, sign + 8, 2)?;
        if !(-4713..=9999).contains(&year) || !(1..=12).contains(&month) || !(1..=31).contains(&day)
        {
            return None;
        }
        let mut start = sign + 10;
        while bytes
            .get(start)
            .is_some_and(|c| c.is_ascii_whitespace() || *c == b'T')
        {
            start += 1;
        }
        if start == bytes.len() {
            (0, 0, 0, 0)
        } else {
            time_parts(bytes.get(start..)?)?
        }
    } else if let Some(time) = time_parts(bytes) {
        time
    } else {
        if ["now", "subsec", "subsecond"]
            .iter()
            .any(|name| value.eq_ignore_ascii_case(name))
        {
            return valid_timestamp(now);
        }
        return numeric_timestamp(
            value
                .trim_matches(|c: char| c.is_ascii_whitespace())
                .parse::<f64>()
                .ok()?,
        );
    };
    year -= i64::from(month <= 2);
    let era = year.div_euclid(400);
    let yoe = year - era * 400;
    let mp = month + if month > 2 { -3 } else { 9 };
    let doy = (153 * mp + 2) / 5 + day - 1;
    let days = era * 146097 + yoe * 365 + yoe / 4 - yoe / 100 + doy - 719468;
    valid_timestamp(
        UNIX_EPOCH_JULIAN_MS + ((days * 24 + hour) * 60 + minute - offset) * 60000 + second_ms,
    )
}

async fn query(
    session: &mut Session,
    input: ProjectionRecordsQuery,
) -> Result<ProjectionRecordResult> {
    ensure_ready(session).await?;
    let mut result = ProjectionRecordResult {
        thread: None,
        rows: FIELDS.into_iter().map(|field| (field, vec![])).collect(),
        message_ordinals: vec![],
        deferred_work: vec![],
    };
    let Some(thread) = read_row(
        &mut session.transaction,
        &session.catalog,
        THREADS,
        &[text_cell(&input.thread_id)],
    )
    .await?
    else {
        return Ok(result);
    };
    result.thread = Some(text(&thread.row, "payload_json")?);
    if input.window.is_some() {
        return window::query(session, input, result).await;
    }
    let filter = input.filter.unwrap_or_default();
    if matches!(
        filter.omit_settled_subagents,
        Some(OmitSettledSubagents::All(false))
    ) {
        return Err(invalid(
            "omitSettledSubagents must be true or an except list",
        ));
    }
    let mut active = BTreeMap::new();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| UNIX_EPOCH_JULIAN_MS + elapsed.as_millis() as i64)
        .map_err(|_| Error::IncompatibleStore)?;
    for field in input.fields.into_iter().collect::<BTreeSet<_>>() {
        let rows = candidates(session, field, &input.thread_id).await?;
        let latest_run =
            if field == ProjectionRecordField::Runs && filter.include_latest_run == Some(true) {
                let mut latest: Option<(i64, String)> = None;
                for row in &rows {
                    if text(&row.row, "status")? == "cancelled"
                        && payload(&row.row)?["startedAt"].is_null()
                    {
                        continue;
                    }
                    let ordinal = integer(&row.row, "ordinal")?;
                    if latest
                        .as_ref()
                        .is_none_or(|(previous, _)| ordinal > *previous)
                    {
                        latest = Some((ordinal, text(&row.row, "run_id")?));
                    }
                }
                latest.map(|(_, id)| id)
            } else {
                None
            };
        let mut ordered = Vec::new();
        for row in rows {
            if retain(
                session,
                field,
                &row.row,
                &filter,
                latest_run.as_deref(),
                &mut active,
            )
            .await?
            {
                let usage = if field == ProjectionRecordField::ProviderTurns
                    && filter.latest_provider_turn_usage_only == Some(true)
                {
                    timestamp(&payload(&row.row)?["nativeTokenUsageUpdatedAt"], now)
                } else {
                    None
                };
                ordered.push((usage, order(field, &row.row)?, row));
            }
        }
        ordered.sort_by(|left, right| {
            right
                .0
                .cmp(&left.0)
                .then_with(|| left.1.cmp(&right.1))
                .then_with(|| left.2.order.cmp(&right.2.order))
        });
        if field == ProjectionRecordField::ProviderTurns
            && filter.latest_provider_turn_usage_only == Some(true)
        {
            ordered.truncate(1);
        }
        let mut payloads = Vec::with_capacity(ordered.len());
        for (_, _, row) in ordered {
            let source = read_row(
                &mut session.transaction,
                &session.catalog,
                field.table(),
                &[text_cell(&row_id(field.table(), &row.row)?)],
            )
            .await?
            .ok_or(Error::IncompatibleStore)?;
            payloads.push(text(&source.row, "payload_json")?);
        }
        result.rows.insert(field, payloads);
    }
    if result.rows[&ProjectionRecordField::Messages].len() > 1 {
        let mut ordinals = BTreeMap::<String, i64>::new();
        for row in related(session, ITEMS, "thread_id", &input.thread_id).await? {
            if !["user_message", "assistant_message"].contains(&text(&row.row, "type")?.as_str()) {
                continue;
            }
            if let Some(id) = payload(&row.row)?["messageId"].as_str() {
                let ordinal = integer(&row.row, "ordinal")?;
                ordinals
                    .entry(id.into())
                    .and_modify(|value| *value = (*value).min(ordinal))
                    .or_insert(ordinal);
            }
        }
        result.message_ordinals = ordinals
            .into_iter()
            .map(|(message_id, ordinal)| MessageOrdinal {
                message_id,
                ordinal,
            })
            .collect();
    }
    Ok(result)
}

#[path = "redb_projection_window.rs"]
pub mod window;

impl ApplicationTransactions {
    pub async fn projection_records(
        &mut self,
        id: &str,
        input: ProjectionRecordsQuery,
    ) -> Result<ProjectionRecordResult> {
        let mut session = self.take(id)?;
        let result = query(&mut session, input).await;
        self.finish(id, session, result).await
    }
}
