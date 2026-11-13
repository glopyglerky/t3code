//! Bounded standalone wire for the same outbox policy called by the redb store.

use crate::json::{self, Value};
use crate::outbox::{self, Action, EffectStatus, EffectType, Row};
use std::collections::HashSet;

pub const MAX_FRAME_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_ROWS: usize = 65_536;
const MAX_TEXT_BYTES: usize = 65_536;
const ROW_KEYS: &[&str] = &[
    "id",
    "threadId",
    "effectType",
    "status",
    "insertionOrdinal",
    "attemptCount",
    "availableAt",
    "leaseOwner",
    "createdAt",
    "updatedAt",
    "completedAt",
    "lastError",
];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    InvalidRequest,
    DuplicateKey,
    InputLimit,
    OutputLimit,
    CounterOverflow,
    Io,
}

impl Error {
    pub fn message(self) -> &'static str {
        match self {
            Self::InvalidRequest => "invalid request",
            Self::DuplicateKey => "duplicate object key",
            Self::InputLimit => "input limit exceeded",
            Self::OutputLimit => "output limit exceeded",
            Self::CounterOverflow => "attempt count exceeds safe integer limit",
            Self::Io => "input or output failed",
        }
    }
}

type WireResult<T> = Result<T, Error>;

fn object(fields: Vec<(&str, Value)>) -> Value {
    Value::Object(
        fields
            .into_iter()
            .map(|(key, value)| (key.to_string(), value))
            .collect(),
    )
}

fn field<'a>(value: &'a Value, key: &str) -> WireResult<&'a Value> {
    value.get(key).ok_or(Error::InvalidRequest)
}

fn keys(value: &Value, allowed: &[&str]) -> WireResult<()> {
    let Value::Object(entries) = value else {
        return Err(Error::InvalidRequest);
    };
    let mut seen = HashSet::new();
    for (key, _) in entries {
        if !seen.insert(key.as_str()) {
            return Err(Error::DuplicateKey);
        }
        if !allowed.contains(&key.as_str()) {
            return Err(Error::InvalidRequest);
        }
    }
    Ok(())
}

fn text(value: &Value) -> WireResult<String> {
    let text = value.as_str().ok_or(Error::InvalidRequest)?;
    if text.len() > MAX_TEXT_BYTES {
        return Err(Error::InputLimit);
    }
    Ok(text.to_string())
}

fn identifier(value: &Value) -> WireResult<String> {
    let text = text(value)?;
    if text.is_empty() || text.contains('\0') {
        return Err(Error::InvalidRequest);
    }
    Ok(text)
}

fn optional_text(value: &Value) -> WireResult<Option<String>> {
    if value == &Value::Null {
        Ok(None)
    } else {
        text(value).map(Some)
    }
}

fn counter(value: &Value) -> WireResult<u64> {
    let number = value.as_u64().ok_or(Error::InvalidRequest)?;
    if number > outbox::MAX_COUNTER {
        return Err(Error::InvalidRequest);
    }
    Ok(number)
}

fn valid_timestamp(value: &str) -> bool {
    let bytes = value.as_bytes();
    if bytes.len() != 24
        || bytes[4] != b'-'
        || bytes[7] != b'-'
        || bytes[10] != b'T'
        || bytes[13] != b':'
        || bytes[16] != b':'
        || bytes[19] != b'.'
        || bytes[23] != b'Z'
    {
        return false;
    }
    for (index, byte) in bytes.iter().enumerate() {
        if ![4, 7, 10, 13, 16, 19, 23].contains(&index) && !byte.is_ascii_digit() {
            return false;
        }
    }
    let year = value[0..4].parse::<u32>().unwrap_or(0);
    let month = value[5..7].parse::<u32>().unwrap_or(0);
    let day = value[8..10].parse::<u32>().unwrap_or(0);
    let hour = value[11..13].parse::<u32>().unwrap_or(24);
    let minute = value[14..16].parse::<u32>().unwrap_or(60);
    let second = value[17..19].parse::<u32>().unwrap_or(60);
    let days = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if year % 4 == 0 && (year % 100 != 0 || year % 400 == 0) => 29,
        2 => 28,
        _ => 0,
    };
    day >= 1 && day <= days && hour < 24 && minute < 60 && second < 60
}

fn timestamp(value: &Value) -> WireResult<String> {
    let text = text(value)?;
    if !valid_timestamp(&text) {
        return Err(Error::InvalidRequest);
    }
    Ok(text)
}

fn nullable_timestamp(value: &Value) -> WireResult<Option<String>> {
    if value == &Value::Null {
        Ok(None)
    } else {
        timestamp(value).map(Some)
    }
}

fn decode_row(value: &Value) -> WireResult<Row> {
    keys(value, ROW_KEYS)?;
    let insertion_ordinal = counter(field(value, "insertionOrdinal")?)?;
    if insertion_ordinal == 0 {
        return Err(Error::InvalidRequest);
    }
    let lease_owner = optional_text(field(value, "leaseOwner")?)?;
    if lease_owner
        .as_deref()
        .is_some_and(|value| value.is_empty() || value.contains('\0'))
    {
        return Err(Error::InvalidRequest);
    }
    Ok(Row {
        id: identifier(field(value, "id")?)?,
        thread_id: identifier(field(value, "threadId")?)?,
        effect_type: EffectType::from_wire(
            field(value, "effectType")?
                .as_str()
                .ok_or(Error::InvalidRequest)?,
        )
        .ok_or(Error::InvalidRequest)?,
        status: EffectStatus::from_wire(
            field(value, "status")?
                .as_str()
                .ok_or(Error::InvalidRequest)?,
        )
        .ok_or(Error::InvalidRequest)?,
        insertion_ordinal,
        attempt_count: counter(field(value, "attemptCount")?)?,
        available_at: timestamp(field(value, "availableAt")?)?,
        lease_owner,
        created_at: timestamp(field(value, "createdAt")?)?,
        updated_at: timestamp(field(value, "updatedAt")?)?,
        completed_at: nullable_timestamp(field(value, "completedAt")?)?,
        last_error: optional_text(field(value, "lastError")?)?,
    })
}

fn nullable(value: &Option<String>) -> Value {
    value.clone().map_or(Value::Null, Value::String)
}

pub fn encode_row(row: &Row) -> Value {
    object(vec![
        ("id", Value::String(row.id.clone())),
        ("threadId", Value::String(row.thread_id.clone())),
        (
            "effectType",
            Value::String(row.effect_type.as_wire().to_string()),
        ),
        ("status", Value::String(row.status.as_wire().to_string())),
        (
            "insertionOrdinal",
            Value::Number(row.insertion_ordinal.to_string()),
        ),
        ("attemptCount", Value::Number(row.attempt_count.to_string())),
        ("availableAt", Value::String(row.available_at.clone())),
        ("leaseOwner", nullable(&row.lease_owner)),
        ("createdAt", Value::String(row.created_at.clone())),
        ("updatedAt", Value::String(row.updated_at.clone())),
        ("completedAt", nullable(&row.completed_at)),
        ("lastError", nullable(&row.last_error)),
    ])
}

fn change(index: usize, row: &Row) -> Value {
    object(vec![
        ("index", Value::Number(index.to_string())),
        ("row", encode_row(row)),
    ])
}

fn no_change() -> Value {
    object(vec![("index", Value::Null), ("row", Value::Null)])
}

fn transition(row: &Row, action: Action, now: &str) -> WireResult<Option<Row>> {
    outbox::transition(row, action, now).map_err(|_| Error::CounterOverflow)
}

fn push_change(
    changes: &mut Vec<Value>,
    remaining: &mut usize,
    index: usize,
    row: &Row,
) -> WireResult<()> {
    let value = change(index, row);
    // Charge each escaped row before retaining it: a long cancellation reason
    // repeated across many rows must never allocate an unbounded response.
    let bytes = json::stringify(&value).len() + 1;
    if bytes > *remaining {
        return Err(Error::OutputLimit);
    }
    *remaining -= bytes;
    changes.push(value);
    Ok(())
}

pub fn evaluate(input: &Value) -> WireResult<Value> {
    if field(input, "version")?.as_u64() != Some(1) {
        return Err(Error::InvalidRequest);
    }
    let operation = field(input, "operation")?
        .as_str()
        .ok_or(Error::InvalidRequest)?;
    let operation_keys: &[&str] = match operation {
        "claim" => &[
            "now",
            "workerId",
            "excludeRestartContinuations",
            "maxRunningTitleGenerations",
        ],
        "next" => &["maxRunningTitleGenerations"],
        "transition" => &[
            "now",
            "effectId",
            "action",
            "workerId",
            "error",
            "availableAt",
        ],
        "reconcile" => &["now"],
        "cancel" => &["now", "threadId", "effectTypes", "reason"],
        "prune" => &["cutoff", "limit"],
        _ => return Err(Error::InvalidRequest),
    };
    let mut allowed = vec!["version", "operation", "rows"];
    allowed.extend_from_slice(operation_keys);
    keys(input, &allowed)?;
    let values = field(input, "rows")?
        .as_array()
        .ok_or(Error::InvalidRequest)?;
    if values.len() > MAX_ROWS {
        return Err(Error::InputLimit);
    }
    let mut rows = Vec::new();
    let mut ids = HashSet::new();
    let mut ordinals = HashSet::new();
    for value in values {
        let row = decode_row(value)?;
        if !ids.insert(row.id.clone()) || !ordinals.insert(row.insertion_ordinal) {
            return Err(Error::InvalidRequest);
        }
        rows.push(row);
    }
    let title_limit = match input.get("maxRunningTitleGenerations") {
        None => None,
        Some(Value::Number(value)) => {
            let value: f64 = value.parse().map_err(|_| Error::InvalidRequest)?;
            if !value.is_finite() {
                return Err(Error::InvalidRequest);
            }
            Some(value)
        }
        _ => return Err(Error::InvalidRequest),
    };
    let output = match operation {
        "claim" => {
            let now = timestamp(field(input, "now")?)?;
            let worker_id = identifier(field(input, "workerId")?)?;
            let exclude = field(input, "excludeRestartContinuations")?
                .as_bool()
                .ok_or(Error::InvalidRequest)?;
            if let Some(index) =
                outbox::claim_next_with_title_limit(&rows, &now, exclude, title_limit)
            {
                let row = transition(&rows[index], Action::Claim { worker_id }, &now)?
                    .ok_or(Error::InvalidRequest)?;
                change(index, &row)
            } else {
                no_change()
            }
        }
        "next" => object(vec![(
            "availableAt",
            nullable(&outbox::next_claimable_at_with_title_limit(
                &rows,
                title_limit,
            )),
        )]),
        "transition" => {
            let now = timestamp(field(input, "now")?)?;
            let effect_id = identifier(field(input, "effectId")?)?;
            let worker_id = identifier(field(input, "workerId")?)?;
            let action = match field(input, "action")?.as_str() {
                Some("succeed")
                    if input.get("error").is_none() && input.get("availableAt").is_none() =>
                {
                    Action::Succeed { worker_id }
                }
                Some("retry") => {
                    let available_at = timestamp(field(input, "availableAt")?)?;
                    if available_at < now {
                        return Err(Error::InvalidRequest);
                    }
                    Action::Retry {
                        worker_id,
                        error: text(field(input, "error")?)?,
                        available_at,
                    }
                }
                Some("fail") if input.get("availableAt").is_none() => Action::Fail {
                    worker_id,
                    error: text(field(input, "error")?)?,
                },
                _ => return Err(Error::InvalidRequest),
            };
            if let Some(index) = rows.iter().position(|row| row.id == effect_id) {
                transition(&rows[index], action, &now)?
                    .map_or_else(|| Ok(no_change()), |row| Ok(change(index, &row)))?
            } else {
                no_change()
            }
        }
        "reconcile" => {
            let now = timestamp(field(input, "now")?)?;
            let mut changes = Vec::new();
            let mut remaining = MAX_FRAME_BYTES - 256;
            let mut requeued = 0usize;
            let mut cancelled = 0usize;
            for (index, row) in rows.iter().enumerate() {
                if let Some(next) = transition(row, Action::Reconcile, &now)? {
                    if next.status == EffectStatus::Pending {
                        requeued += 1;
                    } else {
                        cancelled += 1;
                    }
                    push_change(&mut changes, &mut remaining, index, &next)?;
                }
            }
            object(vec![
                ("changes", Value::Array(changes)),
                ("requeued", Value::Number(requeued.to_string())),
                ("cancelled", Value::Number(cancelled.to_string())),
            ])
        }
        "cancel" => {
            let now = timestamp(field(input, "now")?)?;
            let thread_id = identifier(field(input, "threadId")?)?;
            let reason = text(field(input, "reason")?)?;
            let mut effect_types = Vec::new();
            for value in field(input, "effectTypes")?
                .as_array()
                .ok_or(Error::InvalidRequest)?
            {
                effect_types.push(
                    EffectType::from_wire(value.as_str().ok_or(Error::InvalidRequest)?)
                        .ok_or(Error::InvalidRequest)?,
                );
            }
            let mut changes = Vec::new();
            let mut remaining = MAX_FRAME_BYTES - 128;
            for (index, row) in rows.iter().enumerate() {
                if outbox::cancel_matches(row, &thread_id, &effect_types) {
                    let next = transition(
                        row,
                        Action::Cancel {
                            reason: reason.clone(),
                        },
                        &now,
                    )?
                    .ok_or(Error::InvalidRequest)?;
                    push_change(&mut changes, &mut remaining, index, &next)?;
                }
            }
            object(vec![("changes", Value::Array(changes))])
        }
        "prune" => {
            let cutoff = timestamp(field(input, "cutoff")?)?;
            let limit = input
                .get("limit")
                .map(counter)
                .transpose()?
                .unwrap_or(outbox::PRUNE_BATCH_SIZE as u64);
            if limit == 0 || limit > outbox::PRUNE_BATCH_SIZE as u64 {
                return Err(Error::InvalidRequest);
            }
            object(vec![(
                "indices",
                Value::Array(
                    outbox::prune_indices(&rows, &cutoff, limit as usize)
                        .into_iter()
                        .map(|index| Value::Number(index.to_string()))
                        .collect(),
                ),
            )])
        }
        _ => return Err(Error::InvalidRequest),
    };
    Ok(object(vec![
        ("version", Value::Number("1".to_string())),
        ("ok", output),
    ]))
}

pub fn respond(frame: &str) -> WireResult<String> {
    if frame.len() > MAX_FRAME_BYTES {
        return Err(Error::InputLimit);
    }
    let body = frame.strip_suffix('\n').ok_or(Error::InvalidRequest)?;
    if body.contains('\n') || body.contains('\r') {
        return Err(Error::InvalidRequest);
    }
    let input = json::parse_with_limits(
        body,
        json::Limits {
            max_bytes: MAX_FRAME_BYTES,
            max_depth: 16,
            max_values: 1_048_576,
        },
    )
    .map_err(|_| Error::InvalidRequest)?;
    let mut response = json::stringify(&evaluate(&input)?);
    if response.len() >= MAX_FRAME_BYTES {
        return Err(Error::OutputLimit);
    }
    response.push('\n');
    Ok(response)
}
