use std::io::{self, BufRead};
use t3_code_ls_core::guest_share::{
    self, Attachment, Attribution, Command, Expiration, GuestMessage, GuestShare, MessageContext,
    MessageStatus, SettlementOverride, Thread, Timestamp,
};
use t3_code_ls_core::json::{self, Value};

type GuestThread = Thread<Value, Value>;
type FixtureResult<T> = Result<T, &'static str>;

fn field<'a>(value: &'a Value, key: &str) -> FixtureResult<&'a Value> {
    value.get(key).ok_or("missing fixture field")
}

fn text(value: &Value) -> FixtureResult<String> {
    value
        .as_str()
        .map(str::to_string)
        .ok_or("invalid fixture string")
}

fn number(value: &Value) -> FixtureResult<f64> {
    match value {
        Value::Number(value) => value.parse().map_err(|_| "invalid fixture number"),
        _ => Err("invalid fixture number"),
    }
}

fn timestamp(value: &Value) -> FixtureResult<Timestamp> {
    value
        .as_i64()
        .and_then(Timestamp::from_epoch_millis)
        .ok_or("invalid fixture timestamp")
}

fn nullable_timestamp(value: &Value) -> FixtureResult<Option<Timestamp>> {
    if *value == Value::Null {
        Ok(None)
    } else {
        timestamp(value).map(Some)
    }
}

fn object_without(value: &Value, keys: &[&str]) -> FixtureResult<Value> {
    match value {
        Value::Object(entries) => Ok(Value::Object(
            entries
                .iter()
                .filter(|entry| !keys.contains(&entry.0.as_str()))
                .cloned()
                .collect(),
        )),
        _ => Err("invalid fixture object"),
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
        panic!("fixture object expected")
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

fn expiration(value: &Value, dates: &Value) -> FixtureResult<Expiration> {
    let value = text(value)?;
    let parsed = nullable_timestamp(field(dates, &value)?)?;
    Ok(Expiration { value, parsed })
}

fn attachment(value: &Value) -> FixtureResult<Attachment<Value>> {
    Ok(Attachment {
        id: text(field(value, "id")?)?,
        size_bytes: number(field(value, "sizeBytes")?)?,
        payload: object_without(value, &["id", "sizeBytes"])?,
    })
}

fn attachments(value: Option<&Value>) -> FixtureResult<Option<Vec<Attachment<Value>>>> {
    value
        .map(|value| {
            value
                .as_array()
                .ok_or("invalid fixture attachments")?
                .iter()
                .map(attachment)
                .collect::<FixtureResult<Vec<Attachment<Value>>>>()
        })
        .transpose()
}

fn share(value: &Value, dates: &Value) -> FixtureResult<GuestShare<Value>> {
    let mut messages = Vec::new();
    for message in field(value, "messages")?
        .as_array()
        .ok_or("invalid fixture messages")?
    {
        messages.push(GuestMessage {
            id: text(field(message, "id")?)?,
            text: text(field(message, "text")?)?,
            status: match field(message, "status")?.as_str() {
                Some("pending") => MessageStatus::Pending,
                Some("sent") => MessageStatus::Sent,
                Some("dismissed") => MessageStatus::Dismissed,
                _ => return Err("invalid fixture message status"),
            },
            submitted_at: text(field(message, "submittedAt")?)?,
            attachments: attachments(message.get("attachments"))?,
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
    })
}

fn thread(value: &Value, dates: &Value) -> FixtureResult<GuestThread> {
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
            _ => return Err("invalid fixture settlement"),
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
            let mut value = object(vec![
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
            ]);
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
    object(vec![
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
    ])
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

fn command(value: &Value, dates: &Value) -> FixtureResult<Command<Value>> {
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
        _ => return Err("invalid fixture command"),
    })
}

fn attribution(value: &Value) -> FixtureResult<Attribution> {
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

fn evaluate(input: &Value) -> FixtureResult<Value> {
    let operation = field(input, "operation")?
        .as_str()
        .ok_or("invalid fixture operation")?;
    if operation == "timestamp" {
        return Ok(object(vec![(
            "iso",
            Value::String(timestamp(field(input, "now")?)?.format_iso()),
        )]));
    }
    if operation == "strip" {
        let command = field(input, "command")?;
        let context = command
            .get("context")
            .map(|value| -> FixtureResult<MessageContext<Value>> {
                Ok(MessageContext {
                    guest: value.get("guest").map(attribution).transpose()?,
                    payload: object_without(value, &["guest"])?,
                })
            })
            .transpose()?;
        let mut output = command.clone();
        if let Some(context) = guest_share::without_guest_attribution(context.as_ref()) {
            set(&mut output, "context", context.payload);
        }
        return Ok(object(vec![("command", output)]));
    }
    let dates = field(input, "parsedDates")?;
    if operation == "release" {
        let before = field(input, "before")?;
        let before = if *before == Value::Null {
            None
        } else {
            Some(share(before, dates)?)
        };
        let after = field(input, "after")?;
        let after = if *after == Value::Null {
            None
        } else {
            Some(share(after, dates)?)
        };
        return Ok(object(vec![(
            "released",
            released(before.as_ref(), after.as_ref()),
        )]));
    }
    let thread = thread(field(input, "thread")?, dates)?;
    let now = timestamp(field(input, "now")?)?;
    let outcome = match operation {
        "decide" => guest_share::decide(&thread, &command(field(input, "command")?, dates)?, now)
            .map(|next| {
                let output = thread_value(&next);
                (next, output)
            }),
        "send" => guest_share::send_message(&thread, &text(field(input, "messageId")?)?, now).map(
            |sent| {
                let output = object(vec![
                    ("thread", thread_value(&sent.thread)),
                    ("text", Value::String(sent.text)),
                    ("guest", attribution_value(sent.guest)),
                ]);
                (sent.thread, output)
            },
        ),
        "cancel" => {
            let next = guest_share::after_cancelled_send(
                &thread,
                &attribution(field(input, "guest")?)?,
                now,
            );
            let output = thread_value(&next);
            Ok((next, output))
        }
        _ => return Err("invalid fixture operation"),
    };
    Ok(match outcome {
        Ok((next, value)) => object(vec![
            ("ok", value),
            ("released", released(thread.share(), next.share())),
            (
                "pending",
                Value::Number(guest_share::pending_message_count(next.share()).to_string()),
            ),
        ]),
        Err(error) => object(vec![("error", Value::String(error))]),
    })
}

fn main() {
    for input in io::stdin().lock().lines() {
        let input = input.expect("read matrix input");
        let value = json::parse(&input).expect("parse matrix input");
        println!(
            "{}",
            json::stringify(&evaluate(&value).expect("evaluate matrix fixture"))
        );
    }
}
