//! Retention row access and index-aware removal for the shared maintenance policy.
//! This is a child of the event owner: deleting a source row without all of its
//! identity, replay, prompt-size statistics and stream indexes is not permitted.

use super::*;
use crate::application_rows::{delete_row, read_row};

const RECEIPTS: &str = "orchestration_command_receipts";
const LEGACY: &str = "orchestration_v2_legacy_imports";
const RECEIPT_EPOCH: u64 = 1 << 52;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum MaintenanceRead {
    Metadata {},
    Bounds {},
    PromptReferences {
        after_sequence: u64,
        through_sequence: u64,
    },
    HeadReferences {
        through_sequence: u64,
    },
    Candidates {
        after_sequence: u64,
        through_sequence: u64,
        limit: u32,
    },
    Appended {
        after_sequence: u64,
    },
    LegacyEvents {
        after_sequence: u64,
        through_sequence: u64,
        limit: u32,
    },
    LegacyReceipts {
        after_row_id: u64,
        through_row_id: u64,
        limit: u32,
    },
    Reclaimable {},
}
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum MaintenanceWrite {
    Metadata {
        name: String,
        sequence: u64,
        schema_version: i64,
        updated_at: String,
    },
    DeleteEvents {
        sequences: Vec<u64>,
    },
    DeleteReceipts {
        command_ids: Vec<String>,
    },
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
struct EventIndex {
    sequence: u64,
    aggregate_kind: String,
    stream_id: String,
    event_type: String,
    application_event_version: i64,
    prompt_sequence: Option<i64>,
    running: bool,
    // Computed before any pruning. Removing an earlier anchor must not change
    // the amount subsequently subtracted for another obsolete compacted row.
    restored_bytes: u64,
}
impl EventIndex {
    fn application(&self) -> bool {
        self.aggregate_kind == "project" || self.agent()
    }
    fn agent(&self) -> bool {
        self.aggregate_kind == "thread" && self.application_event_version == 2
    }
}
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
struct ReceiptIndex {
    command_id: String,
    row_id: u64,
}
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct MaintenancePreparation {
    version: u32,
    table: usize,
    after: Option<String>,
    indexed_rows: u64,
}

fn location(c: &ApplicationCatalog, suffix: &str) -> Vec<u8> {
    key(c, &format!("maintenance/v1/{suffix}"))
}
fn stream_scope(kind: &str, stream: &str) -> String {
    request_digest(&json!([kind, stream]))
}
fn head_key(c: &ApplicationCatalog, kind: &str, stream: &str) -> Vec<u8> {
    location(c, &format!("heads/{}", stream_scope(kind, stream)))
}
fn reverse(prefix: Vec<u8>, sequence: u64) -> Result<Vec<u8>> {
    Ok(ordered_key(
        prefix,
        MAX_SEQUENCE
            .checked_sub(safe_sequence(sequence)?)
            .ok_or(Error::IncompatibleStore)?,
    ))
}

fn prompt_reference(event: &EventRecord) -> Result<Option<i64>> {
    if !matches!(
        event.row.event_type.as_str(),
        "subagent.updated" | "turn-item.updated"
    ) {
        return Ok(None);
    }
    let Some(fields) = object_fields(&event.row.payload_json)? else {
        return Ok(None);
    };
    field(&fields, "promptSequence")
        .map(|raw| {
            serde_json::from_str::<Value>(raw)
                .map(|value| value.as_i64())
                .map_err(crate::json_error)
        })
        .transpose()
        .map(Option::flatten)
}
async fn summary(
    tx: &mut Transaction,
    c: &ApplicationCatalog,
    event: &EventRecord,
) -> Result<EventIndex> {
    let running = if event.row.event_type == "run.updated" {
        object_fields(&event.row.payload_json)?
            .and_then(|fields| {
                field(&fields, "status").and_then(|raw| serde_json::from_str::<Value>(raw).ok())
            })
            .is_some_and(|v| v == "running")
    } else {
        false
    };
    Ok(EventIndex {
        sequence: event.sequence,
        aggregate_kind: event.row.aggregate_kind.clone(),
        stream_id: event.row.stream_id.clone(),
        event_type: event.row.event_type.clone(),
        application_event_version: event.row.application_event_version,
        prompt_sequence: prompt_reference(event)?,
        running,
        restored_bytes: if event.application() {
            restored_payload(tx, c, event).await?.len() as u64
        } else {
            0
        },
    })
}
async fn checked_put<T: Serialize + DeserializeOwned + PartialEq>(
    tx: &mut Transaction,
    key: Vec<u8>,
    value: &T,
) -> Result<()> {
    if let Some(bytes) = tx.get(&key).await? {
        if decode::<T>(&bytes)? != *value {
            return Err(Error::IncompatibleStore);
        }
    }
    tx.put(key, encode(value)?).await
}
pub(super) async fn index_event(
    tx: &mut Transaction,
    c: &ApplicationCatalog,
    event: &EventRecord,
) -> Result<()> {
    let indexed = summary(tx, c, event).await?;
    checked_put(
        tx,
        ordered_key(location(c, "all/"), event.sequence),
        &indexed,
    )
    .await?;
    checked_put(
        tx,
        reverse(location(c, "all-reverse/"), event.sequence)?,
        &indexed,
    )
    .await?;
    if event.row.application_event_version == 1 {
        checked_put(
            tx,
            ordered_key(location(c, "legacy/"), event.sequence),
            &indexed,
        )
        .await?;
    }
    if indexed.application() {
        checked_put(
            tx,
            reverse(location(c, "application-reverse/"), event.sequence)?,
            &indexed,
        )
        .await?;
    }
    if indexed.agent() {
        checked_put(
            tx,
            reverse(location(c, "agent-reverse/"), event.sequence)?,
            &indexed,
        )
        .await?;
        checked_put(
            tx,
            reverse(
                location(
                    c,
                    &format!(
                        "thread-reverse/{}/",
                        request_digest(&json!(event.row.stream_id))
                    ),
                ),
                event.sequence,
            )?,
            &indexed,
        )
        .await?;
    }
    let scope = stream_scope(&event.row.aggregate_kind, &event.row.stream_id);
    checked_put(
        tx,
        reverse(
            location(c, &format!("stream-reverse/{scope}/")),
            event.sequence,
        )?,
        &indexed,
    )
    .await?;
    let version = !(event.stream_version as u64 ^ (1 << 63));
    checked_put(
        tx,
        ordered_key(location(c, &format!("version-reverse/{scope}/")), version),
        &json!([
            event.row.aggregate_kind,
            event.row.stream_id,
            event.stream_version,
            event.sequence
        ]),
    )
    .await?;
    let head = head_key(c, &event.row.aggregate_kind, &event.row.stream_id);
    let previous: Option<EventIndex> = tx.get(&head).await?.as_deref().map(decode).transpose()?;
    if previous.as_ref().is_some_and(|old| {
        old.aggregate_kind != indexed.aggregate_kind || old.stream_id != indexed.stream_id
    }) {
        return Err(Error::IncompatibleStore);
    }
    if previous.is_none_or(|old| old.sequence < event.sequence) {
        tx.put(head, encode(&indexed)?).await?;
    }
    Ok(())
}

/// Called by the receipt owner for each insert/upsert, and during preparation.
pub(crate) async fn index_receipt(
    tx: &mut Transaction,
    c: &ApplicationCatalog,
    row: &Map<String, Value>,
    importing: bool,
) -> Result<()> {
    let command_id = text(row, "command_id")?;
    let identity = location(
        c,
        &format!("receipt-identity/{}", request_digest(&json!(command_id))),
    );
    let indexed = match tx.get(&identity).await? {
        Some(bytes) => {
            let indexed: ReceiptIndex = decode(&bytes)?;
            if indexed.command_id != command_id {
                return Err(Error::IncompatibleStore);
            }
            indexed
        }
        None => {
            let original = if importing {
                let primary = [text_cell(&command_id)];
                let source = store_key(&format!(
                    "application/generation/{}/metadata/source/{}",
                    c.generation,
                    request_digest(&json!([RECEIPTS, primary]))
                ));
                match tx.get(&source).await? {
                    Some(bytes) => {
                        let witness: Value = decode(&bytes)?;
                        if witness["table"] != RECEIPTS || witness["key"] != json!(primary) {
                            return Err(Error::IncompatibleStore);
                        }
                        Some(
                            witness["ordinal"]
                                .as_u64()
                                .and_then(|v| v.checked_add(1))
                                .filter(|v| *v < RECEIPT_EPOCH)
                                .ok_or(Error::IncompatibleStore)?,
                        )
                    }
                    None => None,
                }
            } else {
                None
            };
            let row_id = match original {
                Some(value) => value,
                None => {
                    let next = location(c, "receipt-counter");
                    let value = tx
                        .get(&next)
                        .await?
                        .as_deref()
                        .map(decode::<u64>)
                        .transpose()?
                        .unwrap_or(RECEIPT_EPOCH)
                        .checked_add(1)
                        .filter(|v| *v <= MAX_SEQUENCE)
                        .ok_or(Error::IncompatibleStore)?;
                    tx.put(next, encode(&value)?).await?;
                    value
                }
            };
            ReceiptIndex { command_id, row_id }
        }
    };
    checked_put(tx, identity, &indexed).await?;
    checked_put(
        tx,
        ordered_key(location(c, "receipts/"), indexed.row_id),
        &indexed,
    )
    .await?;
    checked_put(
        tx,
        reverse(location(c, "receipts-reverse/"), indexed.row_id)?,
        &indexed,
    )
    .await
}

async fn scan<T: DeserializeOwned>(
    tx: &mut Transaction,
    prefix: Vec<u8>,
    after: Option<u64>,
    through: Option<u64>,
    limit: Option<usize>,
) -> Result<Vec<T>> {
    let mut start = after.map_or_else(
        || prefix.clone(),
        |n| {
            let mut s = ordered_key(prefix.clone(), n);
            s.push(0);
            s
        },
    );
    let end = through.map_or_else(
        || prefix_end(&prefix),
        |n| {
            let mut s = ordered_key(prefix.clone(), n);
            s.push(0);
            s
        },
    );
    if start >= end {
        return Ok(Vec::new());
    }
    let mut values = Vec::new();
    loop {
        let count = limit.map_or(128, |l| l.saturating_sub(values.len()).min(128));
        if count == 0 {
            break;
        }
        let page = tx.scan(&start, &end, count).await?;
        let full = page.len() == count;
        for (key, bytes) in page {
            start = key;
            start.push(0);
            values.push(decode(&bytes)?);
        }
        if !full {
            break;
        }
    }
    Ok(values)
}
async fn first<T: DeserializeOwned>(tx: &mut Transaction, prefix: Vec<u8>) -> Result<Option<T>> {
    Ok(scan(tx, prefix, None, None, Some(1))
        .await?
        .into_iter()
        .next())
}
async fn imported(tx: &mut Transaction, c: &ApplicationCatalog, thread: &str) -> Result<bool> {
    Ok(read_row(tx, c, LEGACY, &[text_cell(thread)])
        .await?
        .is_some_and(|r| {
            r.row
                .get("transcript_imported_at")
                .is_some_and(|v| v.get("type").and_then(Value::as_str) != Some("null"))
        }))
}
async fn maintenance_ready(session: &mut Session) -> Result<()> {
    ready(session).await?;
    let state: MaintenancePreparation = session
        .transaction
        .get(&location(&session.catalog, "preparation"))
        .await?
        .as_deref()
        .map(decode)
        .transpose()?
        .ok_or_else(|| invalid("maintenance indexes are not prepared"))?;
    if state.version != 1 {
        return Err(Error::IncompatibleStore);
    }
    if state.table != 2 {
        return Err(invalid("maintenance indexes are not prepared"));
    }
    Ok(())
}

impl Store {
    pub async fn prepare_application_maintenance(&self) -> Result<PreparationResult> {
        let mut limit = 32;
        loop {
            match self.prepare_application_maintenance_batch(limit).await {
                Err(Error::ResourceLimit(_)) if limit > 1 => limit = (limit / 2).max(1),
                result => return result,
            }
        }
    }
    async fn prepare_application_maintenance_batch(
        &self,
        limit: usize,
    ) -> Result<PreparationResult> {
        let mut tx = self
            .db
            .begin_reserved(Duration::from_secs(5), Duration::from_secs(300))
            .await?;
        let catalog = decode_catalog(
            &tx.get(&schema_key())
                .await?
                .ok_or(Error::IncompatibleStore)?,
        )?;
        if !preparation(&mut tx, &catalog).await?.ready {
            return Err(invalid("event indexes must be prepared before maintenance"));
        }
        let state_key = location(&catalog, "preparation");
        let mut state = tx
            .get(&state_key)
            .await?
            .as_deref()
            .map(decode::<MaintenancePreparation>)
            .transpose()?
            .unwrap_or(MaintenancePreparation {
                version: 1,
                table: 0,
                after: None,
                indexed_rows: 0,
            });
        if state.version != 1 || state.table > 2 {
            return Err(Error::IncompatibleStore);
        }
        if state.table == 2 {
            tx.rollback().await?;
            return Ok(PreparationResult {
                prepared: true,
                indexed_rows: state.indexed_rows,
            });
        }
        let name = [EVENTS, RECEIPTS][state.table];
        let prefix = row_prefix(&catalog.generation, name)?;
        let mut start = prefix.clone();
        if let Some(after) = &state.after {
            if after.len() != 64
                || !after
                    .bytes()
                    .all(|v| v.is_ascii_hexdigit() && !v.is_ascii_uppercase())
            {
                return Err(Error::IncompatibleStore);
            }
            start.extend_from_slice(after.as_bytes());
            start.push(0);
        }
        let page = tx.scan(&start, &prefix_end(&prefix), limit).await?;
        let full = page.len() == limit;
        for (encoded, bytes) in page {
            let source = decode_row(&catalog, table(&catalog, name)?, &encoded, &bytes)?;
            if name == EVENTS {
                index_event(&mut tx, &catalog, &EventRecord::from_row(source.row)?).await?;
            } else {
                index_receipt(&mut tx, &catalog, &source.row, true).await?;
            }
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
        if !full {
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

async fn subtract_statistics(
    tx: &mut Transaction,
    c: &ApplicationCatalog,
    scope: &str,
    sequence: u64,
    delta: Aggregate,
) -> Result<()> {
    let mut i = sequence;
    let mut keys = Vec::new();
    while i <= MAX_SEQUENCE {
        keys.push(aggregate_key(c, scope, i));
        i += i & i.wrapping_neg();
    }
    let values = tx.get_many(&keys).await?;
    for (key, bytes) in keys.into_iter().zip(values) {
        let aggregate: Aggregate = decode(&bytes.ok_or(Error::IncompatibleStore)?)?;
        let next = aggregate.subtract(delta)?;
        if next == Aggregate::default() {
            tx.delete(key).await?;
        } else {
            tx.put(key, encode(&next)?).await?;
        }
    }
    Ok(())
}
async fn update_latest(
    tx: &mut Transaction,
    c: &ApplicationCatalog,
    suffix: &str,
    prefix: Vec<u8>,
) -> Result<()> {
    let latest = first::<EventIndex>(tx, prefix)
        .await?
        .map_or(0, |v| v.sequence);
    tx.put(key(c, suffix), encode(&latest)?).await
}
async fn remove_event(tx: &mut Transaction, c: &ApplicationCatalog, sequence: u64) -> Result<()> {
    let Some(event) = event_at(tx, c, sequence).await? else {
        return Ok(());
    };
    let indexed: EventIndex = tx
        .get(&ordered_key(location(c, "all/"), sequence))
        .await?
        .as_deref()
        .map(decode)
        .transpose()?
        .ok_or(Error::IncompatibleStore)?;
    if indexed.sequence != sequence
        || indexed.stream_id != event.row.stream_id
        || indexed.aggregate_kind != event.row.aggregate_kind
        || indexed.event_type != event.row.event_type
        || indexed.application_event_version != event.row.application_event_version
    {
        return Err(Error::IncompatibleStore);
    }
    if event.application() {
        let delta = Aggregate {
            count: 1,
            bytes: indexed.restored_bytes,
            creates: u64::from(event.row.event_type == "thread.created"),
        };
        subtract_statistics(tx, c, &aggregate_scope(None), sequence, delta).await?;
        if event.agent() {
            subtract_statistics(
                tx,
                c,
                &aggregate_scope(Some(&event.row.stream_id)),
                sequence,
                delta,
            )
            .await?;
        }
        for agent in [false, true] {
            if agent && !event.agent() {
                continue;
            }
            for mask in 0..8 {
                if mask & 2 != 0 && event.row.command_id.is_none() {
                    continue;
                }
                let prefix = event_filter_prefix(
                    c,
                    agent,
                    (mask & 1 != 0).then_some(event.row.stream_id.as_str()),
                    if mask & 2 != 0 {
                        event.row.command_id.as_deref()
                    } else {
                        None
                    },
                    (mask & 4 != 0).then_some(event.row.event_type.as_str()),
                );
                tx.delete(ordered_key(prefix, sequence)).await?;
            }
        }
    }
    for (suffix, witness) in [
        (
            format!("identity/{}", request_digest(&json!(event.row.event_id))),
            json!([event.row.event_id, sequence]),
        ),
        (
            format!(
                "version/{}",
                request_digest(&json!([
                    event.row.aggregate_kind,
                    event.row.stream_id,
                    event.stream_version
                ]))
            ),
            json!([
                event.row.aggregate_kind,
                event.row.stream_id,
                event.stream_version,
                sequence
            ]),
        ),
    ] {
        let key = key(c, &suffix);
        if tx
            .get(&key)
            .await?
            .as_deref()
            .map(decode::<Value>)
            .transpose()?
            .as_ref()
            != Some(&witness)
        {
            return Err(Error::IncompatibleStore);
        }
        tx.delete(key).await?;
    }
    tx.delete(ordered_key(location(c, "all/"), sequence))
        .await?;
    tx.delete(reverse(location(c, "all-reverse/"), sequence)?)
        .await?;
    if event.row.application_event_version == 1 {
        tx.delete(ordered_key(location(c, "legacy/"), sequence))
            .await?;
    }
    if event.application() {
        tx.delete(reverse(location(c, "application-reverse/"), sequence)?)
            .await?;
        update_latest(
            tx,
            c,
            "latest/application",
            location(c, "application-reverse/"),
        )
        .await?;
    }
    if event.agent() {
        tx.delete(reverse(location(c, "agent-reverse/"), sequence)?)
            .await?;
        update_latest(tx, c, "latest/agent", location(c, "agent-reverse/")).await?;
        let scope = request_digest(&json!(event.row.stream_id));
        let prefix = location(c, &format!("thread-reverse/{scope}/"));
        tx.delete(reverse(prefix.clone(), sequence)?).await?;
        update_latest(tx, c, &format!("latest/thread/{scope}"), prefix).await?;
    }
    let scope = stream_scope(&event.row.aggregate_kind, &event.row.stream_id);
    let stream_prefix = location(c, &format!("stream-reverse/{scope}/"));
    tx.delete(reverse(stream_prefix.clone(), sequence)?).await?;
    let head = head_key(c, &event.row.aggregate_kind, &event.row.stream_id);
    match first::<EventIndex>(tx, stream_prefix).await? {
        Some(latest) => tx.put(head, encode(&latest)?).await?,
        None => tx.delete(head).await?,
    }
    let version_prefix = location(c, &format!("version-reverse/{scope}/"));
    tx.delete(ordered_key(
        version_prefix.clone(),
        !(event.stream_version as u64 ^ (1 << 63)),
    ))
    .await?;
    let stream = stream_key(c, &event.row.aggregate_kind, &event.row.stream_id);
    match first::<Value>(tx, version_prefix).await? {
        Some(witness) => {
            if witness[0] != event.row.aggregate_kind || witness[1] != event.row.stream_id {
                return Err(Error::IncompatibleStore);
            }
            tx.put(
                stream,
                encode(&StreamVersion {
                    kind: event.row.aggregate_kind.clone(),
                    stream: event.row.stream_id.clone(),
                    version: witness[2].as_i64().ok_or(Error::IncompatibleStore)?,
                })?,
            )
            .await?;
        }
        None => {
            tx.delete(stream).await?;
        }
    }
    delete_row(tx, c, EVENTS, &[int_cell(sequence)]).await?;
    Ok(())
}

impl ApplicationTransactions {
    pub async fn maintenance_read(&mut self, id: &str, query: MaintenanceRead) -> Result<Value> {
        let mut session = self.take(id)?;
        let result=async{maintenance_ready(&mut session).await?;let tx=&mut session.transaction;let c=&session.catalog;
            Ok(match query{
                MaintenanceRead::Metadata{}=>{let mut rows=Vec::new();for name in ["thread-projections","event-log-prune-mark","event-log-pruned-through"]{if let Some(row)=read_row(tx,c,METADATA,&[text_cell(name)]).await?{rows.push(json!({"projection_name":name,"last_sequence":integer(&row.row,"last_sequence")?,"updated_at":text(&row.row,"updated_at")?}));}}json!(rows)},
                MaintenanceRead::Bounds{}=>json!([{"sequence":first::<EventIndex>(tx,location(c,"all-reverse/")).await?.map_or(0,|v|v.sequence),"receipt_row_id":first::<ReceiptIndex>(tx,location(c,"receipts-reverse/")).await?.map_or(0,|v|v.row_id)}]),
                MaintenanceRead::PromptReferences{after_sequence,through_sequence}=>{safe_sequence(after_sequence)?;safe_sequence(through_sequence)?;
                    json!(scan::<EventIndex>(tx,location(c,"all/"),Some(after_sequence),Some(through_sequence),None).await?.iter().filter_map(|r|r.prompt_sequence.map(|p|json!({"prompt_sequence":p}))).collect::<Vec<_>>())},
                MaintenanceRead::HeadReferences{through_sequence}=>{safe_sequence(through_sequence)?;
                    json!(scan::<EventIndex>(tx,location(c,"heads/"),None,None,None).await?.iter().filter(|r|r.sequence<=through_sequence).filter_map(|r|r.prompt_sequence.map(|p|json!({"prompt_sequence":p}))).collect::<Vec<_>>())},
                MaintenanceRead::Candidates{after_sequence,through_sequence,limit}=>{
                    if limit==0||limit>4096{return Err(invalid("invalid maintenance page size"));}safe_sequence(after_sequence)?;safe_sequence(through_sequence)?;let mut result=Vec::new();
                    for row in scan::<EventIndex>(tx,location(c,"all/"),Some(after_sequence),Some(through_sequence),Some(limit as usize)).await?{
                        let head:EventIndex=tx.get(&head_key(c,&row.aggregate_kind,&row.stream_id)).await?.as_deref().map(decode).transpose()?.ok_or(Error::IncompatibleStore)?;
                        if head.aggregate_kind!=row.aggregate_kind||head.stream_id!=row.stream_id{return Err(Error::IncompatibleStore);}
                        let prunable=row.application_event_version!=1&&row.event_type!="thread.created"&&!row.running&&head.sequence!=row.sequence;
                        result.push(json!({"sequence":row.sequence,"prunable":u8::from(prunable)}));
                    }json!(result)
                },
                MaintenanceRead::Appended{after_sequence}=>{safe_sequence(after_sequence)?;json!(scan::<EventIndex>(tx,location(c,"all/"),Some(after_sequence),None,None).await?.iter().map(|r|json!({"sequence":r.sequence,"prompt_sequence":r.prompt_sequence})).collect::<Vec<_>>())},
                MaintenanceRead::LegacyEvents{after_sequence,through_sequence,limit}=>{
                    if limit==0||limit>4096{return Err(invalid("invalid maintenance page size"));}safe_sequence(after_sequence)?;safe_sequence(through_sequence)?;let mut result=Vec::new();
                    for row in scan::<EventIndex>(tx,location(c,"legacy/"),Some(after_sequence),Some(through_sequence),Some(limit as usize)).await?{let imported=row.aggregate_kind=="thread"&&imported(tx,c,&row.stream_id).await?;result.push(json!({"sequence":row.sequence,"imported":u8::from(imported)}));}json!(result)
                },
                MaintenanceRead::LegacyReceipts{after_row_id,through_row_id,limit}=>{
                    if limit==0||limit>4096{return Err(invalid("invalid maintenance page size"));}safe_sequence(after_row_id)?;safe_sequence(through_row_id)?;let mut result=Vec::new();
                    for index in scan::<ReceiptIndex>(tx,location(c,"receipts/"),Some(after_row_id),Some(through_row_id),Some(limit as usize)).await?{
                        let row=read_row(tx,c,RECEIPTS,&[text_cell(&index.command_id)]).await?.ok_or(Error::IncompatibleStore)?;
                        let imported=text(&row.row,"command_type")?=="legacy"&&text(&row.row,"aggregate_kind")?=="thread"&&imported(tx,c,&text(&row.row,"aggregate_id")?).await?;
                        result.push(json!({"row_id":index.row_id,"command_id":index.command_id,"imported":u8::from(imported)}));
                    }json!(result)
                },
                // This is a conservative estimate, not a physical free-page claim.
                // DatabaseStorage obtains allocator statistics outside reserved transactions.
                MaintenanceRead::Reclaimable{}=>json!([{"reclaimable_bytes":0}]),
            })
        }.await;
        self.finish(id, session, result).await
    }
    pub async fn maintenance_write(
        &mut self,
        id: &str,
        mutation: MaintenanceWrite,
    ) -> Result<Value> {
        let mut session = self.take(id)?;
        let result = async {
            session.transaction.require_write()?;
            maintenance_ready(&mut session).await?;
            let tx = &mut session.transaction;
            let c = &session.catalog;
            match mutation {
                MaintenanceWrite::Metadata {
                    name,
                    sequence,
                    schema_version,
                    updated_at,
                } => {
                    safe_sequence(sequence)?;
                    if !matches!(
                        name.as_str(),
                        "event-log-prune-mark" | "event-log-pruned-through"
                    ) {
                        return Err(invalid("invalid maintenance metadata identity"));
                    }
                    crate::application_projections::write_maintenance_metadata(
                        tx,
                        c,
                        &name,
                        sequence,
                        schema_version,
                        &updated_at,
                    )
                    .await?;
                }
                MaintenanceWrite::DeleteEvents { sequences } => {
                    if sequences.len() > 4096 {
                        return Err(invalid("maintenance delete page exceeds limit"));
                    }
                    for sequence in sequences.into_iter().collect::<BTreeSet<_>>() {
                        safe_sequence(sequence)?;
                        remove_event(tx, c, sequence).await?;
                    }
                }
                MaintenanceWrite::DeleteReceipts { command_ids } => {
                    if command_ids.len() > 4096 {
                        return Err(invalid("maintenance delete page exceeds limit"));
                    }
                    for command_id in command_ids.into_iter().collect::<BTreeSet<_>>() {
                        let identity = location(
                            c,
                            &format!("receipt-identity/{}", request_digest(&json!(command_id))),
                        );
                        if let Some(bytes) = tx.get(&identity).await? {
                            let index: ReceiptIndex = decode(&bytes)?;
                            if index.command_id != command_id {
                                return Err(Error::IncompatibleStore);
                            }
                            tx.delete(ordered_key(location(c, "receipts/"), index.row_id))
                                .await?;
                            tx.delete(reverse(location(c, "receipts-reverse/"), index.row_id)?)
                                .await?;
                            tx.delete(identity).await?;
                        }
                        delete_row(tx, c, RECEIPTS, &[text_cell(&command_id)]).await?;
                    }
                }
            }
            Ok(json!([]))
        }
        .await;
        self.finish(id, session, result).await
    }
}
