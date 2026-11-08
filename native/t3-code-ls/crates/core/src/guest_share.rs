//! Guest-share decisions from apps/server/src/orchestration-v2/GuestShareDecider.ts.
//! Copyright (c) 2026 T3 Tools Inc. Distributed under the repository's MIT license.

pub const PENDING_MESSAGE_LIMIT: usize = 3;
pub const RESOLVED_MESSAGE_LIMIT: usize = 20;
pub const PENDING_ATTACHMENT_BYTES_LIMIT: f64 = 20.0 * 1024.0 * 1024.0;

pub const INACTIVE: &str = "This guest link is no longer active.";
pub const USED: &str = "This guest link was already used.";
pub const BUSY: &str = "Wait for the owner to answer your earlier messages.";
pub const FILES_BUSY: &str =
    "Wait for the owner to answer your earlier messages before you attach more files.";

/// UTC milliseconds, with the same valid range as JavaScript's Date.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Timestamp(i64);

impl Timestamp {
    pub fn from_epoch_millis(value: i64) -> Option<Self> {
        if (-8_640_000_000_000_000..=8_640_000_000_000_000).contains(&value) {
            Some(Self(value))
        } else {
            None
        }
    }

    pub fn epoch_millis(self) -> i64 {
        self.0
    }

    pub fn format_iso(self) -> String {
        let days = self.0.div_euclid(86_400_000);
        let time = self.0.rem_euclid(86_400_000);
        let shifted = days + 719_468;
        let era = shifted.div_euclid(146_097);
        let day_of_era = shifted - era * 146_097;
        let year_of_era =
            (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
        let mut year = year_of_era + era * 400;
        let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
        let month_index = (5 * day_of_year + 2) / 153;
        let day = day_of_year - (153 * month_index + 2) / 5 + 1;
        let month = month_index + if month_index < 10 { 3 } else { -9 };
        if month <= 2 {
            year += 1;
        }
        let year_text = if year < 0 {
            format!("-{:06}", -year)
        } else if year > 9_999 {
            format!("+{year:06}")
        } else {
            format!("{year:04}")
        };
        format!(
            "{year_text}-{month:02}-{day:02}T{:02}:{:02}:{:02}.{:03}Z",
            time / 3_600_000,
            time / 60_000 % 60,
            time / 1_000 % 60,
            time % 1_000
        )
    }
}

/// Retain the original wire value; the date adapter supplies Date.parse's result.
/// None represents NaN, for which upstream's expiry comparison is false.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Expiration {
    pub value: String,
    pub parsed: Option<Timestamp>,
}

impl Expiration {
    pub fn from_timestamp(value: Timestamp) -> Self {
        Self {
            value: value.format_iso(),
            parsed: Some(value),
        }
    }

    pub fn is_expired(&self, now: Timestamp) -> bool {
        self.parsed.is_some_and(|expires| now.0 >= expires.0)
    }
}

/// Payload retains attachment fields which the decision layer does not inspect.
#[derive(Clone, Debug, PartialEq)]
pub struct Attachment<Payload> {
    pub id: String,
    pub size_bytes: f64,
    pub payload: Payload,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MessageStatus {
    Pending,
    Sent,
    Dismissed,
}

#[derive(Clone, Debug, PartialEq)]
pub struct GuestMessage<AttachmentPayload> {
    pub id: String,
    pub text: String,
    pub status: MessageStatus,
    pub submitted_at: String,
    pub attachments: Option<Vec<Attachment<AttachmentPayload>>>,
    pub retained: Option<AttachmentPayload>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct GuestShare<AttachmentPayload> {
    pub id: String,
    pub host: String,
    pub created_at: String,
    pub expires_at: Expiration,
    pub guest_name: Option<String>,
    pub messages: Vec<GuestMessage<AttachmentPayload>>,
    pub retained: Option<AttachmentPayload>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SettlementOverride {
    Settled,
    Active,
}

/// Nested options preserve absent optional properties separately from JSON null.
/// Payload retains every unrelated thread field across a guest-share transition.
#[derive(Clone, Debug, PartialEq)]
pub struct Thread<AttachmentPayload, ThreadPayload> {
    pub guest_share: Option<Option<GuestShare<AttachmentPayload>>>,
    pub archived_at: Option<Timestamp>,
    pub settled_override: Option<SettlementOverride>,
    pub settled_at: Option<Timestamp>,
    pub unsettled_at: Option<Option<Timestamp>>,
    pub updated_at: Timestamp,
    pub payload: ThreadPayload,
}

impl<A, T> Thread<A, T> {
    pub fn share(&self) -> Option<&GuestShare<A>> {
        self.guest_share.as_ref().and_then(Option::as_ref)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum Command<AttachmentPayload> {
    Open {
        share_id: String,
        host: String,
        expires_at: Expiration,
    },
    Close,
    Redeem {
        share_id: String,
        guest_name: String,
    },
    Submit {
        share_id: String,
        message_id: String,
        text: String,
        attachments: Option<Vec<Attachment<AttachmentPayload>>>,
    },
    Dismiss {
        message_id: String,
    },
}

pub fn pending_message_count<A>(share: Option<&GuestShare<A>>) -> usize {
    share.map_or(0, |value| {
        value
            .messages
            .iter()
            .filter(|message| message.status == MessageStatus::Pending)
            .count()
    })
}

pub fn released_attachment_ids<A>(
    before: Option<&GuestShare<A>>,
    after: Option<&GuestShare<A>>,
) -> Vec<String> {
    let Some(before) = before else {
        return Vec::new();
    };
    let mut kept = Vec::new();
    if let Some(after) = after.filter(|share| share.id == before.id) {
        for message in &after.messages {
            if message.status == MessageStatus::Dismissed {
                continue;
            }
            if let Some(attachments) = &message.attachments {
                for attachment in attachments {
                    kept.push(attachment.id.as_str());
                }
            }
        }
    }
    let mut released = Vec::new();
    for message in &before.messages {
        if message.status != MessageStatus::Pending {
            continue;
        }
        if let Some(attachments) = &message.attachments {
            for attachment in attachments {
                if !kept.contains(&attachment.id.as_str()) {
                    released.push(attachment.id.clone());
                }
            }
        }
    }
    released
}

fn trim_resolved<A>(messages: Vec<GuestMessage<A>>) -> Vec<GuestMessage<A>> {
    let mut remaining = RESOLVED_MESSAGE_LIMIT;
    let mut kept = Vec::new();
    for message in messages.into_iter().rev() {
        if message.status != MessageStatus::Pending {
            if remaining == 0 {
                continue;
            }
            remaining -= 1;
        }
        kept.push(message);
    }
    kept.reverse();
    kept
}

fn resolve_message<A: Clone>(
    share: &GuestShare<A>,
    message_id: &str,
    status: MessageStatus,
) -> Result<GuestShare<A>, String> {
    if !share
        .messages
        .iter()
        .find(|message| message.id == message_id)
        .is_some_and(|message| message.status == MessageStatus::Pending)
    {
        return Err(format!("Guest message {message_id} is not pending."));
    }
    let mut next = share.clone();
    for message in &mut next.messages {
        if message.id == message_id {
            message.status = status;
        }
    }
    next.messages = trim_resolved(next.messages);
    Ok(next)
}

#[derive(Clone, Debug, PartialEq)]
pub struct Transition<T> {
    pub value: T,
    pub changed: bool,
}

pub fn decide_transition<A: Clone, T: Clone>(
    thread: &Thread<A, T>,
    command: &Command<A>,
    now: Timestamp,
) -> Result<Transition<Thread<A, T>>, String> {
    let share = thread.share();
    let mut next = thread.clone();
    let next_share = match command {
        Command::Open {
            share_id,
            host,
            expires_at,
        } => {
            if thread.archived_at.is_some() {
                return Err("Archived threads cannot be shared.".to_string());
            }
            Some(GuestShare {
                id: share_id.clone(),
                host: host.clone(),
                created_at: now.format_iso(),
                expires_at: expires_at.clone(),
                guest_name: None,
                messages: Vec::new(),
                retained: None,
            })
        }
        Command::Close => {
            if share.is_none() {
                return Ok(Transition {
                    value: next,
                    changed: false,
                });
            }
            None
        }
        Command::Redeem {
            share_id,
            guest_name,
        } => {
            let share = share
                .filter(|value| value.id == *share_id && !value.expires_at.is_expired(now))
                .ok_or_else(|| INACTIVE.to_string())?;
            if share.guest_name.is_some() {
                return Err(USED.to_string());
            }
            let mut value = share.clone();
            value.guest_name = Some(guest_name.clone());
            Some(value)
        }
        Command::Submit {
            share_id,
            message_id,
            text,
            attachments,
        } => {
            let share = share
                .filter(|value| {
                    value.id == *share_id
                        && value.guest_name.is_some()
                        && !value.expires_at.is_expired(now)
                })
                .ok_or_else(|| INACTIVE.to_string())?;
            if pending_message_count(Some(share)) >= PENDING_MESSAGE_LIMIT {
                return Err(BUSY.to_string());
            }
            let mut bytes = 0.0;
            for message in &share.messages {
                if message.status != MessageStatus::Pending {
                    continue;
                }
                if let Some(files) = &message.attachments {
                    for file in files {
                        bytes += file.size_bytes;
                    }
                }
            }
            if let Some(files) = attachments {
                for file in files {
                    bytes += file.size_bytes;
                }
                if !files.is_empty() && bytes > PENDING_ATTACHMENT_BYTES_LIMIT {
                    return Err(FILES_BUSY.to_string());
                }
            }
            let mut value = share.clone();
            value.messages.push(GuestMessage {
                id: message_id.clone(),
                text: text.clone(),
                status: MessageStatus::Pending,
                submitted_at: now.format_iso(),
                attachments: attachments
                    .as_ref()
                    .filter(|files| !files.is_empty())
                    .cloned(),
                retained: None,
            });
            value.messages = trim_resolved(value.messages);
            if next.settled_override == Some(SettlementOverride::Settled) {
                next.settled_override = None;
                next.settled_at = None;
                next.unsettled_at = Some(Some(now));
            }
            Some(value)
        }
        Command::Dismiss { message_id } => Some(resolve_message(
            share.ok_or_else(|| "This thread has no guest link.".to_string())?,
            message_id,
            MessageStatus::Dismissed,
        )?),
    };
    next.guest_share = Some(next_share);
    next.updated_at = now;
    Ok(Transition {
        value: next,
        changed: true,
    })
}

pub fn decide<A: Clone, T: Clone>(
    thread: &Thread<A, T>,
    command: &Command<A>,
    now: Timestamp,
) -> Result<Thread<A, T>, String> {
    decide_transition(thread, command, now).map(|transition| transition.value)
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Attribution {
    pub message_id: String,
    pub name: String,
    pub text: String,
    pub share_id: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SentMessage<A, T> {
    pub thread: Thread<A, T>,
    pub text: String,
    pub guest: Attribution,
}

pub fn send_message<A: Clone, T: Clone>(
    thread: &Thread<A, T>,
    message_id: &str,
    now: Timestamp,
) -> Result<SentMessage<A, T>, String> {
    let share = thread
        .share()
        .ok_or_else(|| "This thread has no guest link.".to_string())?;
    let message = share
        .messages
        .iter()
        .find(|message| message.id == message_id);
    let name = share.guest_name.as_deref().unwrap_or("Guest");
    let text = message.map_or("", |message| message.text.as_str());
    let next_share = resolve_message(share, message_id, MessageStatus::Sent)?;
    let mut next = thread.clone();
    next.guest_share = Some(Some(next_share));
    next.updated_at = now;
    Ok(SentMessage {
        thread: next,
        text: format!("Message from guest {name}, sent by the owner:\n\n{text}"),
        guest: Attribution {
            message_id: message_id.to_string(),
            name: name.to_string(),
            text: text.to_string(),
            share_id: Some(share.id.clone()),
        },
    })
}

pub fn cancelled_send_transition<A: Clone, T: Clone>(
    thread: &Thread<A, T>,
    guest: &Attribution,
    now: Timestamp,
) -> Transition<Thread<A, T>> {
    let Some(share) = thread
        .share()
        .filter(|share| guest.share_id.as_ref() == Some(&share.id))
    else {
        return Transition {
            value: thread.clone(),
            changed: false,
        };
    };
    if !share
        .messages
        .iter()
        .find(|message| message.id == guest.message_id)
        .is_some_and(|message| message.status == MessageStatus::Sent)
    {
        return Transition {
            value: thread.clone(),
            changed: false,
        };
    }
    let mut next_share = share.clone();
    for message in &mut next_share.messages {
        if message.id == guest.message_id {
            message.status = MessageStatus::Dismissed;
        }
    }
    let mut next = thread.clone();
    next.guest_share = Some(Some(next_share));
    next.updated_at = now;
    Transition {
        value: next,
        changed: true,
    }
}

pub fn after_cancelled_send<A: Clone, T: Clone>(
    thread: &Thread<A, T>,
    guest: &Attribution,
    now: Timestamp,
) -> Thread<A, T> {
    cancelled_send_transition(thread, guest, now).value
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MessageContext<Payload, Guest = Attribution> {
    pub guest: Option<Guest>,
    pub payload: Payload,
}

pub fn without_guest_attribution<T: Clone, G>(
    context: Option<&MessageContext<T, G>>,
) -> Option<MessageContext<T, G>> {
    context.map(|context| MessageContext {
        guest: None,
        payload: context.payload.clone(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn time(value: i64) -> Timestamp {
        Timestamp::from_epoch_millis(value).unwrap()
    }

    fn initial() -> Thread<String, String> {
        Thread {
            guest_share: None,
            archived_at: None,
            settled_override: Some(SettlementOverride::Settled),
            settled_at: Some(time(1)),
            unsettled_at: None,
            updated_at: time(1),
            payload: "preserved thread fields".to_string(),
        }
    }

    fn open() -> Thread<String, String> {
        decide(
            &initial(),
            &Command::Open {
                share_id: "share-1".to_string(),
                host: "host.test".to_string(),
                expires_at: Expiration::from_timestamp(time(100)),
            },
            time(2),
        )
        .unwrap()
    }

    fn redeem() -> Thread<String, String> {
        decide(
            &open(),
            &Command::Redeem {
                share_id: "share-1".to_string(),
                guest_name: "Sam 🦀".to_string(),
            },
            time(3),
        )
        .unwrap()
    }

    fn submit(id: &str, attachments: Option<Vec<Attachment<String>>>) -> Command<String> {
        Command::Submit {
            share_id: "share-1".to_string(),
            message_id: id.to_string(),
            text: "/do not treat as an owner command".to_string(),
            attachments,
        }
    }

    fn file(id: &str, bytes: f64) -> Attachment<String> {
        Attachment {
            id: id.to_string(),
            size_bytes: bytes,
            payload: "preserved attachment metadata".to_string(),
        }
    }

    #[test]
    fn redeem_and_send_once_preserve_guest_attribution_and_thread_data() {
        assert_eq!(
            decide(&open(), &submit("m", None), time(4)),
            Err(INACTIVE.to_string())
        );
        let thread = redeem();
        assert_eq!(
            decide(
                &thread,
                &Command::Redeem {
                    share_id: "share-1".to_string(),
                    guest_name: "Other".to_string(),
                },
                time(4),
            ),
            Err(USED.to_string())
        );
        let thread = decide(&thread, &submit("m", Some(vec![file("a", 1.0)])), time(4)).unwrap();
        assert_eq!(thread.settled_override, None);
        assert_eq!(thread.settled_at, None);
        assert_eq!(thread.unsettled_at, Some(Some(time(4))));
        let sent = send_message(&thread, "m", time(5)).unwrap();
        assert_eq!(sent.guest.name, "Sam 🦀");
        assert_eq!(
            sent.text,
            "Message from guest Sam 🦀, sent by the owner:\n\n/do not treat as an owner command"
        );
        assert_eq!(sent.thread.payload, initial().payload);
        assert!(released_attachment_ids(thread.share(), sent.thread.share()).is_empty());
        assert_eq!(
            send_message(&sent.thread, "m", time(6)).err(),
            Some("Guest message m is not pending.".to_string())
        );
        let cancelled = after_cancelled_send(&sent.thread, &sent.guest, time(6));
        assert_eq!(
            cancelled.share().unwrap().messages[0].status,
            MessageStatus::Dismissed
        );
        let mut stale = sent.guest;
        stale.share_id = Some("other-share".to_string());
        assert_eq!(after_cancelled_send(&cancelled, &stale, time(7)), cancelled);
    }

    #[test]
    fn pending_message_and_file_budgets_recover_after_owner_resolution() {
        let thread = decide(
            &redeem(),
            &submit(
                "a",
                Some(vec![file("file-a", PENDING_ATTACHMENT_BYTES_LIMIT)]),
            ),
            time(4),
        )
        .unwrap();
        assert_eq!(
            decide(
                &thread,
                &submit("b", Some(vec![file("file-b", 1.0)])),
                time(5)
            ),
            Err(FILES_BUSY.to_string())
        );
        let thread = decide(&thread, &submit("b", None), time(5)).unwrap();
        let thread = decide(&thread, &submit("c", Some(Vec::new())), time(6)).unwrap();
        assert_eq!(
            decide(&thread, &submit("d", None), time(7)),
            Err(BUSY.to_string())
        );
        let dismissed = decide(
            &thread,
            &Command::Dismiss {
                message_id: "a".to_string(),
            },
            time(7),
        )
        .unwrap();
        assert_eq!(
            released_attachment_ids(thread.share(), dismissed.share()),
            vec!["file-a".to_string()]
        );
        let accepted = decide(
            &dismissed,
            &submit("d", Some(vec![file("file-b", 1.0)])),
            time(8),
        )
        .unwrap();
        assert_eq!(
            pending_message_count(accepted.share()),
            PENDING_MESSAGE_LIMIT
        );
        assert_eq!(accepted.share().unwrap().messages[1].attachments, None);
        assert_eq!(accepted.share().unwrap().messages[2].attachments, None);
    }

    #[test]
    fn replacing_closing_and_expiring_shares_keep_upstream_boundaries() {
        assert_eq!(
            decide(&initial(), &Command::Close, time(2)).unwrap(),
            initial()
        );
        let thread = redeem();
        assert_eq!(
            decide(&thread, &submit("a", None), time(100)),
            Err(INACTIVE.to_string())
        );
        let mut invalid = thread.clone();
        invalid
            .guest_share
            .as_mut()
            .unwrap()
            .as_mut()
            .unwrap()
            .expires_at = Expiration {
            value: "not a date".to_string(),
            parsed: None,
        };
        assert!(decide(&invalid, &submit("a", None), time(100)).is_ok());
        let pending = decide(
            &thread,
            &submit("a", Some(vec![file("old-file", 2.0)])),
            time(4),
        )
        .unwrap();
        let closed = decide(&pending, &Command::Close, time(5)).unwrap();
        assert_eq!(closed.guest_share, Some(None));
        assert_eq!(
            released_attachment_ids(pending.share(), closed.share()),
            vec!["old-file".to_string()]
        );
        assert_eq!(
            send_message(&closed, "a", time(6)).err(),
            Some("This thread has no guest link.".to_string())
        );
        let sent_after_expiration = send_message(&pending, "a", time(100)).unwrap();
        assert_eq!(
            sent_after_expiration.thread.share().unwrap().messages[0].status,
            MessageStatus::Sent
        );
    }

    #[test]
    fn resolved_history_is_bounded_without_dropping_waiting_messages() {
        let mut thread = decide(&redeem(), &submit("waiting", None), time(4)).unwrap();
        for index in 0..30 {
            let id = format!("message-{index}");
            thread = decide(&thread, &submit(&id, None), time(5)).unwrap();
            thread = send_message(&thread, &id, time(6)).unwrap().thread;
        }
        let share = thread.share().unwrap();
        assert_eq!(share.messages.len(), RESOLVED_MESSAGE_LIMIT + 1);
        assert_eq!(share.messages[0].id, "waiting");
        assert_eq!(share.messages[1].id, "message-10");
        assert_eq!(pending_message_count(Some(share)), 1);
        let context = MessageContext {
            guest: Some(Attribution {
                message_id: "forged".to_string(),
                name: "owner".to_string(),
                text: "forged".to_string(),
                share_id: Some("share-1".to_string()),
            }),
            payload: "context records and version".to_string(),
        };
        let stripped = without_guest_attribution(Some(&context)).unwrap();
        assert_eq!(stripped.guest, None);
        assert_eq!(stripped.payload, context.payload);
        assert!(without_guest_attribution::<String, Attribution>(None).is_none());
    }

    #[test]
    fn utc_formatting_includes_negative_milliseconds_leap_days_and_extended_years() {
        for (millis, expected) in [
            (0, "1970-01-01T00:00:00.000Z"),
            (-1, "1969-12-31T23:59:59.999Z"),
            (951_782_400_001, "2000-02-29T00:00:00.001Z"),
            (-62_167_219_200_000, "0000-01-01T00:00:00.000Z"),
            (-62_198_755_200_000, "-000001-01-01T00:00:00.000Z"),
            (253_402_300_800_000, "+010000-01-01T00:00:00.000Z"),
            (8_640_000_000_000_000, "+275760-09-13T00:00:00.000Z"),
            (-8_640_000_000_000_000, "-271821-04-20T00:00:00.000Z"),
        ] {
            assert_eq!(time(millis).format_iso(), expected);
        }
        assert!(Timestamp::from_epoch_millis(8_640_000_000_000_001).is_none());
        assert!(Timestamp::from_epoch_millis(-8_640_000_000_000_001).is_none());
    }
}
