//! Compact projection metadata: select a bounded history window before loading
//! message bodies, plan markdown, and potentially megabytes of tool output.

use super::*;

#[derive(Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
struct Metadata {
    table: String,
    primary: Vec<Value>,
    order: RowOrder,
    row: Map<String, Value>,
}

fn key(catalog: &ApplicationCatalog, name: &str, primary: &[Value]) -> Vec<u8> {
    location(
        catalog,
        &format!("window-meta/{name}/{}", request_digest(&json!(primary))),
    )
}

fn compact(name: &str, row: &Map<String, Value>, order: RowOrder) -> Result<Metadata> {
    let mut slim = row.clone();
    if let Some(cell) = row.get("payload_json") {
        let raw = cell
            .get("value")
            .and_then(Value::as_str)
            .ok_or(Error::IncompatibleStore)?;
        if name == ITEMS {
            // This selection is SQL LIKE over raw bytes, not json_extract.
            // Keep it even if JSON is unreadable so matching corrupt render
            // rows reach the original decoder instead of disappearing.
            let lower = raw.to_ascii_lowercase();
            let recording = lower.match_indices("preview").any(|(offset, prefix)| {
                let tail = &lower[offset + prefix.len()..];
                tail.chars().next().is_some_and(|first| {
                    let tail = &tail[first.len_utf8()..];
                    tail.strip_prefix("recording").is_some_and(|tail| {
                        tail.chars()
                            .next()
                            .is_some_and(|second| tail[second.len_utf8()..].starts_with("stop"))
                    })
                })
            });
            slim.insert(
                "_native_attachment_render".into(),
                int_cell(i64::from(
                    lower.contains("htmlrender") || lower.contains("t3mcpapp") || recording,
                )),
            );
        }
        let payload: Value = match serde_json::from_str(raw) {
            Ok(value) => value,
            Err(_) => {
                // Preparation must leave historical corruption discoverable by
                // the unreadable-row audit. A compact query touching it fails,
                // and full row reads still use the original stored bytes.
                slim.insert("payload_json".into(), text_cell("{unreadable"));
                return Ok(Metadata {
                    table: name.into(),
                    primary: primary(name, row)?,
                    order,
                    row: slim,
                });
            }
        };
        let mut retained = Map::new();
        for field in [
            "rootNodeId",
            "startedAt",
            "inputIntent",
            "createdBy",
            "messageId",
            "planId",
            "checkpointId",
            "contextHandoffId",
            "requestId",
            "historyOrigin",
            "forkedFrom",
            "queueHeld",
            "userMessageId",
            "workStartedAt",
            "activeAttemptId",
            "creationSource",
            "archivedAt",
            "settledAt",
            "settledOverride",
            "pinnedAt",
            "autoSettleDisabledAt",
            "snoozedUntil",
            "worktreePath",
            "lineage",
            "updatedAt",
            "delegatedTaskId",
            "subagentId",
            "restartContinuationOfRunId",
        ] {
            if let Some(value) = payload.get(field) {
                retained.insert(field.into(), value.clone());
            }
        }
        if name == MESSAGES {
            retained.insert(
                "nativeMonitorNotification".into(),
                json!(payload["notification"]["source"]["kind"] == "monitor"),
            );
            let whitespace = |ch: char| {
                matches!(ch, '\u{0009}'..='\u{000d}' | '\u{0020}' | '\u{00a0}' | '\u{1680}'
                | '\u{2000}'..='\u{200a}' | '\u{2028}' | '\u{2029}' | '\u{202f}' | '\u{205f}' | '\u{3000}' | '\u{feff}')
            };
            let idle = text(row, "role")? == "user"
                && payload
                    .get("text")
                    .and_then(Value::as_str)
                    .is_some_and(|text| {
                        text.trim_matches(whitespace)
                            .eq_ignore_ascii_case("/compact")
                    })
                && payload
                    .get("attachments")
                    .and_then(Value::as_array)
                    .is_none_or(Vec::is_empty)
                && (payload
                    .get("idleCompaction")
                    .is_some_and(|value| !value.is_null())
                    || (payload.get("createdBy").and_then(Value::as_str) == Some("agent")
                        && payload.get("creationSource").and_then(Value::as_str)
                            == Some("server")));
            retained.insert("nativeIdleCompactionMessage".into(), json!(idle));
            let text = payload
                .get("text")
                .and_then(Value::as_str)
                .map(|text| text.trim_matches(whitespace));
            let compact = text.is_some_and(|text| text.eq_ignore_ascii_case("/compact"));
            let attachments = payload.get("attachments").and_then(Value::as_array);
            retained.insert("nativeCompactCommand".into(), json!(compact));
            retained.insert(
                "nativeGoalCommand".into(),
                json!(text.is_some_and(|text| text.starts_with("/goal"))),
            );
            retained.insert(
                "nativeConversationUser".into(),
                json!(
                    text.is_some_and(|_| !compact)
                        || attachments.is_some_and(|items| !items.is_empty())
                ),
            );
            retained.insert(
                "nativeContextAttachmentIds".into(),
                json!(
                    attachments
                        .into_iter()
                        .flatten()
                        .map(|item| item.get("id").cloned().unwrap_or(Value::Null))
                        .collect::<Vec<_>>()
                ),
            );
        }
        if name == TURNS {
            retained.insert(
                "nativeHasTokenUsage".into(),
                json!(
                    payload
                        .get("tokenUsage")
                        .is_some_and(|value| !value.is_null())
                ),
            );
            if let Some(value) = payload
                .get("tokenUsage")
                .and_then(|usage| usage.get("updatedAt"))
            {
                retained.insert("nativeTokenUsageUpdatedAt".into(), value.clone());
            }
        }
        if name == SESSIONS {
            retained.insert(
                "nativeHasExtensionStatuses".into(),
                json!(
                    payload
                        .get("extensionStatuses")
                        .and_then(Value::as_array)
                        .is_some_and(|values| !values.is_empty())
                ),
            );
        }
        if name == SCOPES {
            if let Some(value) = payload.get("cwd") {
                retained.insert("cwd".into(), value.clone());
            }
        }
        if name == CHECKPOINTS {
            if let Some(value) = payload.get("ref") {
                retained.insert("ref".into(), value.clone());
            }
        }
        if name == THREADS {
            let files = payload
                .get("guestShare")
                .and_then(|share| share.get("messages"))
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter(|message| message.get("status").and_then(Value::as_str) == Some("pending"))
                .flat_map(|message| {
                    message
                        .get("attachments")
                        .and_then(Value::as_array)
                        .into_iter()
                        .flatten()
                })
                .map(|item| item.get("id").cloned().unwrap_or(Value::Null))
                .collect::<Vec<_>>();
            retained.insert("nativeGuestAttachmentIds".into(), json!(files));
            for (field, source) in [
                ("nativeLimitRecoveryRunId", "runId"),
                ("nativeLimitRecoveryResetAt", "resetAt"),
                ("nativeLimitRecoveryAutoResume", "autoResume"),
            ] {
                if let Some(value) = payload
                    .get("limitRecovery")
                    .and_then(|recovery| recovery.get(source))
                {
                    retained.insert(field.into(), value.clone());
                }
            }
            if let Some(id) = payload.get("guestShare").and_then(|share| share.get("id")) {
                retained.insert("nativeGuestShareId".into(), id.clone());
            }
            let links = payload.get("pullRequests").and_then(Value::as_array);
            retained.insert(
                "nativeHasPullRequests".into(),
                json!(links.is_some_and(|links| !links.is_empty())),
            );
            retained.insert(
                "nativeHasPullRequestWatch".into(),
                json!(
                    links.is_some_and(|links| links.iter().any(|link| link.get("watch").is_some()))
                ),
            );
        }
        if name == PROVIDER_THREADS {
            retained.insert(
                "nativeHasPendingBackgroundTasks".into(),
                json!(
                    payload
                        .get("pendingBackgroundTasks")
                        .and_then(Value::as_array)
                        .is_some_and(|tasks| !tasks.is_empty())
                ),
            );
        }
        if name == SUBAGENTS {
            retained.insert(
                "nativeAppOwnedPendingResult".into(),
                json!(
                    payload.get("origin").and_then(Value::as_str) == Some("app_owned")
                        && payload.get("result").is_none_or(Value::is_null)
                ),
            );
            let remote = payload.get("remoteChild").filter(|value| value.is_object());
            let open = remote.is_some_and(|remote| {
                payload.get("result") == Some(&Value::Null)
                    || ([Some("cancelled"), Some("interrupted")]
                        .contains(&payload.get("status").and_then(Value::as_str))
                        && remote.get("stoppedThere") != Some(&Value::Bool(true)))
                    || remote.get("stoppedThere") == Some(&Value::Bool(false))
            });
            retained.insert("nativeOpenRemoteDelegatedTask".into(), json!(open));
        }
        if name == ITEMS {
            let mut files = Vec::new();
            if text(row, "type")? == "user_input_request" {
                // Preserve JSON question-key insertion order, as json_each does.
                if let Some(fields) = object_fields(raw)? {
                    if let Some(answer) = field(&fields, "questionAnswer")
                        .map(object_fields)
                        .transpose()?
                        .flatten()
                    {
                        if let Some(questions) = field(&answer, "attachmentsByQuestionId")
                            .map(object_fields)
                            .transpose()?
                            .flatten()
                        {
                            for question in &questions {
                                let values: Value = serde_json::from_str(
                                    field(&questions, &question.name)
                                        .ok_or(Error::IncompatibleStore)?,
                                )
                                .map_err(|_| Error::IncompatibleStore)?;
                                for item in values.as_array().into_iter().flatten() {
                                    files.push(item.get("id").cloned().unwrap_or(Value::Null));
                                }
                            }
                        }
                    }
                }
            }
            retained.insert("nativeAnswerAttachmentIds".into(), json!(files));
            for (field, source) in [
                ("nativeFailureClass", "class"),
                ("nativeFailureResetAt", "resetAt"),
            ] {
                if let Some(value) = payload
                    .get("failure")
                    .and_then(|failure| failure.get(source))
                {
                    retained.insert(field.into(), value.clone());
                }
            }
            if text(row, "type")? == "user_message" {
                retained.insert(
                    "nativeAttachmentIds".into(),
                    json!(
                        payload
                            .get("attachments")
                            .and_then(Value::as_array)
                            .into_iter()
                            .flatten()
                            .filter_map(|attachment| attachment.get("id").and_then(Value::as_str))
                            .collect::<Vec<_>>()
                    ),
                );
            }
            let lower = raw.to_ascii_lowercase();
            retained.insert(
                "nativeAttachmentRender".into(),
                json!(
                    lower.contains("htmlrender")
                        || lower.contains("t3mcpapp")
                        || lower.contains("preview_recording_stop")
                ),
            );
            let delegate = lower.find("\"toolname\":\"").is_some_and(|offset| {
                let suffix = &lower[offset + "\"toolname\":\"".len()..];
                // The upstream LIKE pattern leaves '_' unescaped, so it
                // matches one Unicode character, not only an underscore.
                suffix.match_indices("delegate").any(|(offset, prefix)| {
                    let tail = &suffix[offset + prefix.len()..];
                    tail.chars()
                        .next()
                        .is_some_and(|character| tail[character.len_utf8()..].starts_with("task\""))
                })
            });
            retained.insert(
                "nativeInlineWorkOutput".into(),
                json!(
                    text(row, "type")? == "dynamic_tool"
                        && (lower.contains("htmlrender")
                            || lower.contains("t3mcpapp")
                            || lower.contains("\"viewedimagepath\"")
                            || delegate)
                ),
            );
        }
        slim.insert(
            "payload_json".into(),
            text_cell(&serde_json::to_string(&retained).map_err(|_| Error::IncompatibleStore)?),
        );
    }
    Ok(Metadata {
        table: name.into(),
        primary: primary(name, row)?,
        order,
        row: slim,
    })
}

pub(super) async fn insert(
    tx: &mut Transaction,
    catalog: &ApplicationCatalog,
    name: &str,
    row: &Map<String, Value>,
    order: RowOrder,
) -> Result<()> {
    let metadata = compact(name, row, order)?;
    tx.put(key(catalog, name, &metadata.primary), encode(&metadata)?)
        .await
}

pub(super) async fn remove(
    tx: &mut Transaction,
    catalog: &ApplicationCatalog,
    name: &str,
    row: &Map<String, Value>,
    order: RowOrder,
    strict: bool,
) -> Result<()> {
    let expected = compact(name, row, order)?;
    let key = key(catalog, name, &expected.primary);
    match tx.get(&key).await? {
        Some(bytes) if decode::<Metadata>(&bytes)? == expected => tx.delete(key).await,
        None if !strict => Ok(()),
        _ => Err(Error::IncompatibleStore),
    }
}

pub(crate) async fn equal_metadata(
    session: &mut Session,
    name: &str,
    column: &str,
    value: &Value,
) -> Result<Vec<IndexedRow>> {
    ensure_ready(session).await?;
    if !SPECS.iter().any(|spec| spec.name == name)
        || !(column == "all"
            || RELATIONSHIPS.contains(&column)
            || derived::COLUMNS.contains(&column))
    {
        return Err(invalid("unsupported projection window relationship"));
    }
    let prefix = index_prefix(&session.catalog, name, column, value);
    let end = prefix_end(&prefix);
    let mut cursor = prefix.clone();
    let mut result = Vec::new();
    loop {
        let page = session.transaction.scan(&cursor, &end, 128).await?;
        let count = page.len();
        for (index, bytes) in page {
            let witness: IndexRow = decode(&bytes)?;
            if witness.table != name
                || witness.column != column
                || &witness.value != value
                || index_key(&session.catalog, &witness) != index
            {
                return Err(Error::IncompatibleStore);
            }
            let bytes = session
                .transaction
                .get(&key(&session.catalog, name, &witness.primary))
                .await?
                .ok_or(Error::IncompatibleStore)?;
            let metadata: Metadata = decode(&bytes)?;
            if metadata.table != name
                || metadata.primary != witness.primary
                || metadata.order != witness.order
            {
                return Err(Error::IncompatibleStore);
            }
            result.push(IndexedRow {
                row: metadata.row,
                order: metadata.order,
            });
            cursor = index;
            cursor.push(0);
        }
        if count < 128 {
            break;
        }
    }
    Ok(result)
}

pub(crate) async fn read_metadata(
    session: &mut Session,
    name: &str,
    id: &str,
) -> Result<Option<IndexedRow>> {
    ensure_ready(session).await?;
    let primary = vec![text_cell(id)];
    let Some(bytes) = session
        .transaction
        .get(&key(&session.catalog, name, &primary))
        .await?
    else {
        return Ok(None);
    };
    let metadata: Metadata = decode(&bytes)?;
    if metadata.table != name || metadata.primary != primary {
        return Err(Error::IncompatibleStore);
    }
    Ok(Some(IndexedRow {
        row: metadata.row,
        order: metadata.order,
    }))
}

#[cfg(test)]
mod compact_tests {
    use super::*;

    #[test]
    fn shell_metadata_drops_bodies_and_keeps_idle_compaction_flags() {
        let row=Map::from_iter([
            ("message_id".into(),text_cell("message:compact")),
            ("role".into(),text_cell("user")),
            ("payload_json".into(),text_cell(&json!({
                "text":"\u{00a0}/COMPACT\u{feff}","attachments":[],"createdBy":"agent","creationSource":"server",
                "unrelatedBody":"private-body".repeat(10000)
            }).to_string())),
        ]);
        let compact = compact(
            MESSAGES,
            &row,
            RowOrder {
                epoch: 0,
                ordinal: 3,
            },
        )
        .expect("compact message");
        let encoded = text(&compact.row, "payload_json").expect("metadata");
        assert!(encoded.len() < 512);
        assert!(!encoded.contains("private-body"));
        let parsed: Value = serde_json::from_str(&encoded).expect("valid compact payload");
        assert_eq!(parsed["nativeIdleCompactionMessage"], true);
        assert!(parsed.get("text").is_none());
    }

    #[test]
    fn corrupt_payload_remains_auditable_without_copying_corrupt_bytes() {
        let row = Map::from_iter([
            ("message_id".into(), text_cell("message:corrupt")),
            ("role".into(), text_cell("user")),
            (
                "payload_json".into(),
                text_cell(&format!("{{{}", "private-body".repeat(10000))),
            ),
        ]);
        let compact = compact(
            MESSAGES,
            &row,
            RowOrder {
                epoch: 0,
                ordinal: 5,
            },
        )
        .expect("index malformed source without losing it");
        let encoded = text(&compact.row, "payload_json").expect("metadata");
        assert!(encoded.len() < 32);
        assert!(serde_json::from_str::<Value>(&encoded).is_err());
        assert_eq!(
            text(&row, "payload_json").expect("original remains").len(),
            120001
        );
    }
}
