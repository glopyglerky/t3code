//! Bounded guest-share decisions with retained JSON record fields.
//! Copyright (c) 2026 T3 Tools Inc. Distributed under the repository's MIT license.

use crate::guest_share::{
    self, Attachment, Attribution, Command, Expiration, GuestMessage, GuestShare, MessageContext,
    MessageStatus, SettlementOverride, Thread, Timestamp,
};
use crate::guest_uploads::{self, AttachmentDraft, InspectedUpload};
use crate::json::{self, Value};
use std::collections::HashSet;

type GuestThread = Thread<Value, Value>;
pub const MAX_FRAME_BYTES: usize = 128 * 1024 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    InvalidRequest,
    DuplicateKey,
    InputLimit,
    OutputLimit,
    Io,
}

impl Error {
    pub fn message(self) -> &'static str {
        match self {
            Self::InvalidRequest => "invalid request",
            Self::DuplicateKey => "duplicate object key",
            Self::InputLimit => "input limit exceeded",
            Self::OutputLimit => "output limit exceeded",
            Self::Io => "input or output failed",
        }
    }
}

pub type WireResult<T> = Result<T, Error>;

fn field<'a>(value: &'a Value, key: &str) -> WireResult<&'a Value> {
    value.get(key).ok_or(Error::InvalidRequest)
}

fn text(value: &Value) -> WireResult<String> {
    value
        .as_str()
        .map(str::to_string)
        .ok_or(Error::InvalidRequest)
}

fn number(value: &Value) -> WireResult<f64> {
    match value {
        Value::Number(value) => {
            let number: f64 = value.parse().map_err(|_| Error::InvalidRequest)?;
            if number.is_finite() {
                Ok(number)
            } else {
                Err(Error::InvalidRequest)
            }
        }
        _ => Err(Error::InvalidRequest),
    }
}

fn unsigned_integer(value: &Value, maximum: u16) -> WireResult<u16> {
    let value = number(value)?;
    if value < 0.0 || value > f64::from(maximum) || value.fract() != 0.0 {
        return Err(Error::InvalidRequest);
    }
    Ok(value as u16)
}

fn code_units(value: &Value) -> WireResult<Vec<u16>> {
    value
        .as_array()
        .ok_or(Error::InvalidRequest)?
        .iter()
        .map(|value| unsigned_integer(value, u16::MAX))
        .collect()
}

fn inspected_upload(value: &Value) -> WireResult<InspectedUpload> {
    allowed_fields(
        value,
        &["nameUnits", "contentTypeLowercaseUnits", "sizeBytes", "header"],
    )?;
    Ok(InspectedUpload {
        name_units: code_units(field(value, "nameUnits")?)?,
        content_type_lowercase_units: code_units(field(value, "contentTypeLowercaseUnits")?)?,
        size_bytes: number(field(value, "sizeBytes")?)?,
        header: field(value, "header")?
            .as_array()
            .ok_or(Error::InvalidRequest)?
            .iter()
            .map(|value| unsigned_integer(value, u16::from(u8::MAX)).map(|byte| byte as u8))
            .collect::<WireResult<Vec<u8>>>()?,
    })
}

fn code_units_value(units: &[u16]) -> Value {
    Value::Array(
        units
            .iter()
            .map(|unit| Value::Number(unit.to_string()))
            .collect(),
    )
}

fn upload_draft_value(draft: &AttachmentDraft) -> Value {
    object(vec![
        ("type", Value::String(draft.kind.as_str().to_string())),
        ("nameUnits", code_units_value(&draft.name_units)),
        ("mimeTypeUnits", code_units_value(&draft.mime_type_units)),
        ("sizeBytes", Value::Number(draft.size_bytes.to_string())),
    ])
}

fn classify_uploads(input: &Value) -> WireResult<Value> {
    let uploads = field(input, "uploads")?
        .as_array()
        .ok_or(Error::InvalidRequest)?
        .iter()
        .map(inspected_upload)
        .collect::<WireResult<Vec<InspectedUpload>>>()?;
    let result = match guest_uploads::classify_uploads(&uploads) {
        Ok(attachments) => object(vec![
            ("ok", Value::Bool(true)),
            (
                "attachments",
                Value::Array(attachments.iter().map(upload_draft_value).collect()),
            ),
        ]),
        Err(reason) => object(vec![
            ("ok", Value::Bool(false)),
            ("reason", Value::String(reason.as_str().to_string())),
        ]),
    };
    Ok(success(result, None))
}

fn timestamp(value: &Value) -> WireResult<Timestamp> {
    value
        .as_i64()
        .and_then(Timestamp::from_epoch_millis)
        .ok_or(Error::InvalidRequest)
}

fn nullable_timestamp(value: &Value) -> WireResult<Option<Timestamp>> {
    if *value == Value::Null {
        Ok(None)
    } else {
        timestamp(value).map(Some)
    }
}

fn object_without(value: &Value, keys: &[&str]) -> WireResult<Value> {
    match value {
        Value::Object(entries) => Ok(Value::Object(
            entries
                .iter()
                .filter(|entry| !keys.contains(&entry.0.as_str()))
                .cloned()
                .collect(),
        )),
        _ => Err(Error::InvalidRequest),
    }
}

fn object(entries: Vec<(&str, Value)>) -> Value {
    Value::Object(
        entries
            .into_iter()
            .map(|(key, value)| (key.to_string(), value))
            .collect(),
    )
}

fn set(object: &mut Value, key: &str, value: Value) {
    let Value::Object(entries) = object else {
        panic!("retained object expected")
    };
    entries.retain(|entry| entry.0 != key);
    entries.push((key.to_string(), value));
}

fn time_value(time: Timestamp) -> Value {
    Value::Number(time.epoch_millis().to_string())
}

fn optional_time(time: Option<Timestamp>) -> Value {
    time.map_or(Value::Null, time_value)
}

fn expiration(value: &Value, dates: &Value) -> WireResult<Expiration> {
    let value = text(value)?;
    let parsed = nullable_timestamp(field(dates, &value)?)?;
    Ok(Expiration { value, parsed })
}

fn attachment(value: &Value) -> WireResult<Attachment<Value>> {
    Ok(Attachment {
        id: text(field(value, "id")?)?,
        size_bytes: number(field(value, "sizeBytes")?)?,
        payload: object_without(value, &["id", "sizeBytes"])?,
    })
}

fn attachments(value: Option<&Value>) -> WireResult<Option<Vec<Attachment<Value>>>> {
    value
        .map(|value| {
            value
                .as_array()
                .ok_or(Error::InvalidRequest)?
                .iter()
                .map(attachment)
                .collect::<WireResult<Vec<Attachment<Value>>>>()
        })
        .transpose()
}

fn share(value: &Value, dates: &Value) -> WireResult<GuestShare<Value>> {
    let mut messages = Vec::new();
    for message in field(value, "messages")?
        .as_array()
        .ok_or(Error::InvalidRequest)?
    {
        messages.push(GuestMessage {
            id: text(field(message, "id")?)?,
            text: text(field(message, "text")?)?,
            status: match field(message, "status")?.as_str() {
                Some("pending") => MessageStatus::Pending,
                Some("sent") => MessageStatus::Sent,
                Some("dismissed") => MessageStatus::Dismissed,
                _ => return Err(Error::InvalidRequest),
            },
            submitted_at: text(field(message, "submittedAt")?)?,
            attachments: attachments(message.get("attachments"))?,
            retained: Some(object_without(
                message,
                &["id", "text", "status", "submittedAt", "attachments"],
            )?),
        });
    }
    Ok(GuestShare {
        id: text(field(value, "id")?)?,
        host: text(field(value, "host")?)?,
        created_at: text(field(value, "createdAt")?)?,
        expires_at: expiration(field(value, "expiresAt")?, dates)?,
        guest_name: match field(value, "guestName")? {
            Value::Null => None,
            value => Some(text(value)?),
        },
        messages,
        retained: Some(object_without(
            value,
            &[
                "id",
                "host",
                "createdAt",
                "expiresAt",
                "guestName",
                "messages",
            ],
        )?),
    })
}

fn thread(value: &Value, dates: &Value) -> WireResult<GuestThread> {
    Ok(Thread {
        guest_share: value
            .get("guestShare")
            .map(|value| {
                if *value == Value::Null {
                    Ok(None)
                } else {
                    share(value, dates).map(Some)
                }
            })
            .transpose()?,
        archived_at: nullable_timestamp(field(value, "archivedAt")?)?,
        settled_override: match field(value, "settledOverride")? {
            Value::Null => None,
            Value::String(value) if value == "settled" => Some(SettlementOverride::Settled),
            Value::String(value) if value == "active" => Some(SettlementOverride::Active),
            _ => return Err(Error::InvalidRequest),
        },
        settled_at: nullable_timestamp(field(value, "settledAt")?)?,
        unsettled_at: value
            .get("unsettledAt")
            .map(nullable_timestamp)
            .transpose()?,
        updated_at: timestamp(field(value, "updatedAt")?)?,
        payload: object_without(
            value,
            &[
                "guestShare",
                "archivedAt",
                "settledOverride",
                "settledAt",
                "unsettledAt",
                "updatedAt",
            ],
        )?,
    })
}

fn attachment_value(attachment: &Attachment<Value>) -> Value {
    let mut value = attachment.payload.clone();
    set(&mut value, "id", Value::String(attachment.id.clone()));
    set(
        &mut value,
        "sizeBytes",
        Value::Number(attachment.size_bytes.to_string()),
    );
    value
}

fn share_value(share: &GuestShare<Value>) -> Value {
    let messages = share
        .messages
        .iter()
        .map(|message| {
            let mut value = message
                .retained
                .clone()
                .unwrap_or_else(|| object(Vec::new()));
            for (key, field) in [
                ("id", Value::String(message.id.clone())),
                ("text", Value::String(message.text.clone())),
                (
                    "status",
                    Value::String(
                        match message.status {
                            MessageStatus::Pending => "pending",
                            MessageStatus::Sent => "sent",
                            MessageStatus::Dismissed => "dismissed",
                        }
                        .to_string(),
                    ),
                ),
                ("submittedAt", Value::String(message.submitted_at.clone())),
            ] {
                set(&mut value, key, field);
            }
            if let Some(files) = &message.attachments {
                set(
                    &mut value,
                    "attachments",
                    Value::Array(files.iter().map(attachment_value).collect()),
                );
            }
            value
        })
        .collect();
    let mut value = share.retained.clone().unwrap_or_else(|| object(Vec::new()));
    for (key, field) in [
        ("id", Value::String(share.id.clone())),
        ("host", Value::String(share.host.clone())),
        ("createdAt", Value::String(share.created_at.clone())),
        ("expiresAt", Value::String(share.expires_at.value.clone())),
        (
            "guestName",
            share
                .guest_name
                .as_ref()
                .map_or(Value::Null, |name| Value::String(name.clone())),
        ),
        ("messages", Value::Array(messages)),
    ] {
        set(&mut value, key, field);
    }
    value
}

fn thread_value(thread: &GuestThread) -> Value {
    let mut value = thread.payload.clone();
    if let Some(share) = &thread.guest_share {
        set(
            &mut value,
            "guestShare",
            share.as_ref().map_or(Value::Null, share_value),
        );
    }
    set(&mut value, "archivedAt", optional_time(thread.archived_at));
    set(&mut value, "settledAt", optional_time(thread.settled_at));
    set(
        &mut value,
        "settledOverride",
        match thread.settled_override {
            Some(SettlementOverride::Settled) => Value::String("settled".to_string()),
            Some(SettlementOverride::Active) => Value::String("active".to_string()),
            None => Value::Null,
        },
    );
    if let Some(time) = thread.unsettled_at {
        set(&mut value, "unsettledAt", optional_time(time));
    }
    set(&mut value, "updatedAt", time_value(thread.updated_at));
    value
}

fn command(value: &Value, dates: &Value) -> WireResult<Command<Value>> {
    Ok(match field(value, "type")?.as_str() {
        Some("thread.guest-share.open") => Command::Open {
            share_id: text(field(value, "shareId")?)?,
            host: text(field(value, "host")?)?,
            expires_at: expiration(field(value, "expiresAt")?, dates)?,
        },
        Some("thread.guest-share.close") => Command::Close,
        Some("thread.guest-share.redeem") => Command::Redeem {
            share_id: text(field(value, "shareId")?)?,
            guest_name: text(field(value, "guestName")?)?,
        },
        Some("thread.guest-message.submit") => Command::Submit {
            share_id: text(field(value, "shareId")?)?,
            message_id: text(field(value, "guestMessageId")?)?,
            text: text(field(value, "text")?)?,
            attachments: attachments(value.get("attachments"))?,
        },
        Some("thread.guest-message.dismiss") => Command::Dismiss {
            message_id: text(field(value, "guestMessageId")?)?,
        },
        _ => return Err(Error::InvalidRequest),
    })
}

fn attribution(value: &Value) -> WireResult<Attribution> {
    Ok(Attribution {
        message_id: text(field(value, "guestMessageId")?)?,
        name: value.get("name").map(text).transpose()?.unwrap_or_default(),
        text: value.get("text").map(text).transpose()?.unwrap_or_default(),
        share_id: value.get("shareId").map(text).transpose()?,
    })
}

fn attribution_value(value: Attribution) -> Value {
    let mut output = object(vec![
        ("guestMessageId", Value::String(value.message_id)),
        ("name", Value::String(value.name)),
        ("text", Value::String(value.text)),
    ]);
    if let Some(id) = value.share_id {
        set(&mut output, "shareId", Value::String(id));
    }
    output
}

fn released(before: Option<&GuestShare<Value>>, after: Option<&GuestShare<Value>>) -> Value {
    Value::Array(
        guest_share::released_attachment_ids(before, after)
            .into_iter()
            .map(Value::String)
            .collect(),
    )
}

fn unique_keys(value: &Value) -> WireResult<()> {
    match value {
        Value::Object(entries) => {
            let mut keys = HashSet::new();
            for (key, value) in entries {
                if !keys.insert(key.as_str()) {
                    return Err(Error::DuplicateKey);
                }
                unique_keys(value)?;
            }
        }
        Value::Array(values) => {
            for value in values {
                unique_keys(value)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn allowed_fields(value: &Value, allowed: &[&str]) -> WireResult<()> {
    let Value::Object(entries) = value else {
        return Err(Error::InvalidRequest);
    };
    if entries
        .iter()
        .any(|(key, _)| !allowed.contains(&key.as_str()))
    {
        return Err(Error::InvalidRequest);
    }
    Ok(())
}

fn date_key<'a>(share: &'a Value, keys: &mut HashSet<&'a str>) -> WireResult<()> {
    if *share != Value::Null {
        keys.insert(
            field(share, "expiresAt")?
                .as_str()
                .ok_or(Error::InvalidRequest)?,
        );
    }
    Ok(())
}

fn checked_dates<'a>(input: &'a Value, operation: &str) -> WireResult<&'a Value> {
    let mut expected = HashSet::new();
    if operation == "release" {
        date_key(field(input, "before")?, &mut expected)?;
        date_key(field(input, "after")?, &mut expected)?;
    } else {
        if let Some(share) = field(input, "thread")?.get("guestShare") {
            date_key(share, &mut expected)?;
        }
        if operation == "decide" {
            let command = field(input, "command")?;
            if field(command, "type")?.as_str() == Some("thread.guest-share.open") {
                expected.insert(
                    field(command, "expiresAt")?
                        .as_str()
                        .ok_or(Error::InvalidRequest)?,
                );
            }
        }
    }
    let dates = field(input, "parsedDates")?;
    let Value::Object(entries) = dates else {
        return Err(Error::InvalidRequest);
    };
    if entries.len() != expected.len() {
        return Err(Error::InvalidRequest);
    }
    for (key, value) in entries {
        if !expected.contains(key.as_str()) {
            return Err(Error::InvalidRequest);
        }
        nullable_timestamp(value)?;
    }
    Ok(dates)
}

fn success(value: Value, changed: Option<bool>) -> Value {
    let mut response = object(vec![
        ("version", Value::Number("1".to_string())),
        ("ok", value),
    ]);
    if let Some(changed) = changed {
        set(&mut response, "changed", Value::Bool(changed));
    }
    response
}

fn rejection(message: String) -> Value {
    object(vec![
        ("version", Value::Number("1".to_string())),
        ("error", Value::String(message)),
    ])
}

/// Evaluates the same validated envelope used by the executable and batch oracle.
pub fn evaluate(input: &Value) -> WireResult<Value> {
    unique_keys(input)?;
    if field(input, "version")?.as_i64() != Some(1) {
        return Err(Error::InvalidRequest);
    }
    let operation = field(input, "operation")?
        .as_str()
        .ok_or(Error::InvalidRequest)?;
    let keys: &[&str] = match operation {
        "decide" => &[
            "version",
            "operation",
            "thread",
            "command",
            "now",
            "parsedDates",
        ],
        "send" => &[
            "version",
            "operation",
            "thread",
            "messageId",
            "now",
            "parsedDates",
        ],
        "cancel" => &[
            "version",
            "operation",
            "thread",
            "guest",
            "now",
            "parsedDates",
        ],
        "release" => &["version", "operation", "before", "after", "parsedDates"],
        "strip" => &["version", "operation", "context"],
        "classify-uploads" => &["version", "operation", "uploads"],
        _ => return Err(Error::InvalidRequest),
    };
    allowed_fields(input, keys)?;
    if operation == "classify-uploads" {
        return classify_uploads(input);
    }
    if operation == "strip" {
        let context = input
            .get("context")
            .map(|value| -> WireResult<MessageContext<Value, Value>> {
                Ok(MessageContext {
                    guest: value.get("guest").cloned(),
                    payload: object_without(value, &["guest"])?,
                })
            })
            .transpose()?;
        let changed = context.as_ref().is_some_and(|value| value.guest.is_some());
        let mut output = object(Vec::new());
        if let Some(context) = guest_share::without_guest_attribution(context.as_ref()) {
            set(&mut output, "context", context.payload);
        }
        return Ok(success(output, Some(changed)));
    }
    let dates = checked_dates(input, operation)?;
    if operation == "release" {
        let decode = |key| -> WireResult<Option<GuestShare<Value>>> {
            let value = field(input, key)?;
            if *value == Value::Null {
                Ok(None)
            } else {
                share(value, dates).map(Some)
            }
        };
        let before = decode("before")?;
        let after = decode("after")?;
        return Ok(success(
            object(vec![(
                "attachmentIds",
                released(before.as_ref(), after.as_ref()),
            )]),
            None,
        ));
    }
    let thread = thread(field(input, "thread")?, dates)?;
    let now = timestamp(field(input, "now")?)?;
    Ok(match operation {
        "decide" => match guest_share::decide_transition(
            &thread,
            &command(field(input, "command")?, dates)?,
            now,
        ) {
            Ok(transition) => success(thread_value(&transition.value), Some(transition.changed)),
            Err(error) => rejection(error),
        },
        "send" => match guest_share::send_message(&thread, &text(field(input, "messageId")?)?, now)
        {
            Ok(sent) => success(
                object(vec![
                    ("thread", thread_value(&sent.thread)),
                    ("text", Value::String(sent.text)),
                    ("guest", attribution_value(sent.guest)),
                ]),
                Some(true),
            ),
            Err(error) => rejection(error),
        },
        "cancel" => {
            let transition = guest_share::cancelled_send_transition(
                &thread,
                &attribution(field(input, "guest")?)?,
                now,
            );
            success(thread_value(&transition.value), Some(transition.changed))
        }
        _ => return Err(Error::InvalidRequest),
    })
}

fn charge(remaining: &mut usize, amount: usize) -> WireResult<()> {
    *remaining = remaining.checked_sub(amount).ok_or(Error::OutputLimit)?;
    Ok(())
}

fn quoted_size(value: &str, remaining: &mut usize) -> WireResult<()> {
    charge(remaining, 2)?;
    for character in value.chars() {
        charge(
            remaining,
            match character {
                '"' | '\\' | '\n' | '\r' | '\t' | '\u{08}' | '\u{0c}' => 2,
                character if character < '\u{20}' => 6,
                character => character.len_utf8(),
            },
        )?;
    }
    Ok(())
}

fn encoded_size(value: &Value, remaining: &mut usize) -> WireResult<()> {
    match value {
        Value::Null => charge(remaining, 4),
        Value::Bool(value) => charge(remaining, if *value { 4 } else { 5 }),
        Value::Number(value) => charge(remaining, value.len()),
        Value::String(value) => quoted_size(value, remaining),
        Value::Array(values) => {
            charge(remaining, 2 + values.len().saturating_sub(1))?;
            for value in values {
                encoded_size(value, remaining)?;
            }
            Ok(())
        }
        Value::Object(fields) => {
            charge(remaining, 2 + fields.len().saturating_sub(1))?;
            for (key, value) in fields {
                quoted_size(key, remaining)?;
                charge(remaining, 1)?;
                encoded_size(value, remaining)?;
            }
            Ok(())
        }
    }
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
            max_depth: 128,
            max_values: 1_048_576,
        },
    )
    .map_err(|_| Error::InvalidRequest)?;
    let response = evaluate(&input)?;
    // Include the frame delimiter before allocating the escaped output string.
    let mut remaining = MAX_FRAME_BYTES - 1;
    encoded_size(&response, &mut remaining)?;
    let mut encoded = json::stringify(&response);
    encoded.push('\n');
    Ok(encoded)
}

#[cfg(test)]
mod upload_tests {
    use super::*;

    fn upload() -> Value {
        object(vec![
            ("nameUnits", code_units_value(&[97, 0xd800])),
            (
                "contentTypeLowercaseUnits",
                code_units_value(&"text/plain".encode_utf16().collect::<Vec<_>>()),
            ),
            ("sizeBytes", Value::Number("1.5".to_string())),
            ("header", Value::Array(vec![])),
        ])
    }

    fn request(upload: Value) -> Value {
        object(vec![
            ("version", Value::Number("1".to_string())),
            ("operation", Value::String("classify-uploads".to_string())),
            ("uploads", Value::Array(vec![upload])),
        ])
    }

    fn frame(value: &Value) -> String {
        format!("{}\n", json::stringify(value))
    }

    fn numeric_array(values: &[&str]) -> Value {
        Value::Array(
            values
                .iter()
                .map(|value| Value::Number((*value).to_string()))
                .collect(),
        )
    }

    #[test]
    fn classification_round_trips_utf16_and_sizes_without_dates_or_changed() {
        let response = respond(&frame(&request(upload()))).unwrap();
        assert_eq!(
            response,
            "{\"version\":1,\"ok\":{\"ok\":true,\"attachments\":[{\"type\":\"file\",\"nameUnits\":[97,55296],\"mimeTypeUnits\":[116,101,120,116,47,112,108,97,105,110],\"sizeBytes\":1.5}]}}\n"
        );
        let mut input = request(upload());
        set(&mut input, "uploads", Value::Array(vec![]));
        assert_eq!(
            respond(&frame(&input)).unwrap(),
            "{\"version\":1,\"ok\":{\"ok\":true,\"attachments\":[]}}\n"
        );
        set(&mut input, "uploads", Value::Array(vec![upload(); 5]));
        assert_eq!(
            respond(&frame(&input)).unwrap(),
            "{\"version\":1,\"ok\":{\"ok\":false,\"reason\":\"too_many_files\"}}\n"
        );
    }

    #[test]
    fn classification_accepts_integral_code_units_and_bytes_in_json_number_forms() {
        let mut source = upload();
        set(&mut source, "nameUnits", numeric_array(&["6.5e1", "65535.0"]));
        set(&mut source, "header", numeric_array(&["0.0", "2.55e2"]));
        set(&mut source, "sizeBytes", Value::Number("-1.25".to_string()));
        let response = evaluate(&request(source)).unwrap();
        let draft = &response
            .get("ok")
            .unwrap()
            .get("attachments")
            .unwrap()
            .as_array()
            .unwrap()[0];
        assert_eq!(draft.get("nameUnits"), Some(&numeric_array(&["65", "65535"])));
        assert_eq!(draft.get("sizeBytes"), Some(&Value::Number("-1.25".to_string())));
        assert!(response.get("changed").is_none());
    }

    #[test]
    fn classification_rejects_missing_unknown_and_duplicate_fields() {
        let input = request(upload());
        for key in ["version", "operation", "uploads"] {
            assert_eq!(
                evaluate(&object_without(&input, &[key]).unwrap()),
                Err(Error::InvalidRequest)
            );
        }
        for key in ["nameUnits", "contentTypeLowercaseUnits", "sizeBytes", "header"] {
            assert_eq!(
                evaluate(&request(object_without(&upload(), &[key]).unwrap())),
                Err(Error::InvalidRequest)
            );
        }
        let mut extra_root = input.clone();
        set(&mut extra_root, "parsedDates", object(vec![]));
        assert_eq!(evaluate(&extra_root), Err(Error::InvalidRequest));
        let mut extra_upload = upload();
        set(&mut extra_upload, "path", Value::String("/not-sent".to_string()));
        assert_eq!(evaluate(&request(extra_upload)), Err(Error::InvalidRequest));

        for mut duplicate in [input, upload()] {
            let Value::Object(fields) = &mut duplicate else {
                unreachable!();
            };
            fields.push(fields[0].clone());
            let input = if duplicate.get("operation").is_some() {
                duplicate
            } else {
                request(duplicate)
            };
            assert_eq!(respond(&frame(&input)), Err(Error::DuplicateKey));
        }
    }

    #[test]
    fn classification_rejects_invalid_arrays_ranges_types_and_nonfinite_sizes() {
        for key in ["nameUnits", "contentTypeLowercaseUnits", "header"] {
            let beyond = if key == "header" { "256" } else { "65536" };
            for invalid in [
                Value::Null,
                Value::String("units".to_string()),
                numeric_array(&["-1"]),
                numeric_array(&["1.5"]),
                numeric_array(&[beyond]),
                numeric_array(&["1e309"]),
                Value::Array(vec![Value::Null]),
                Value::Array(vec![Value::String("1".to_string())]),
                Value::Array(vec![Value::Bool(true)]),
            ] {
                let mut source = upload();
                set(&mut source, key, invalid);
                assert_eq!(respond(&frame(&request(source))), Err(Error::InvalidRequest), "{key}");
            }
        }
        for invalid in [
            Value::Null,
            Value::String("1".to_string()),
            Value::Bool(false),
            Value::Number("1e309".to_string()),
            Value::Number("-1e309".to_string()),
        ] {
            let mut source = upload();
            set(&mut source, "sizeBytes", invalid);
            assert_eq!(respond(&frame(&request(source))), Err(Error::InvalidRequest));
        }
        for invalid in [Value::Null, object(vec![]), Value::Array(vec![Value::Null])] {
            let mut input = request(upload());
            set(&mut input, "uploads", invalid);
            assert_eq!(respond(&frame(&input)), Err(Error::InvalidRequest));
        }
    }
}
