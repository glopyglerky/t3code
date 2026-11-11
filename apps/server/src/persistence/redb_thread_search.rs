//! Native lexical candidate queries for the application's unchanged palette ranker.
//! Exact source FTS5 positions are imported, including stale/orphan postings.
//! Only the generated AND-of-quoted-phrases grammar is accepted, never SQL.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

use crate::application::{
    ApplicationCatalog, ApplicationTransactions, Session, decode_catalog, schema_key,
};
use crate::application_message_rowids as rowids;
use crate::application_projections::read_index;
use crate::application_rows::{integer, nullable, read_row, text, text_cell};
use crate::backend::Transaction;
use crate::{Error, Result, Store, store_key};

#[path = "redb_search_positions.rs"]
mod position_codec;
#[path = "redb_readable_search_text.rs"]
mod readable;
#[path = "redb_search_unicode.rs"]
mod unicode;
pub(crate) use readable::readable_search_text;

const THREADS: &str = "orchestration_v2_projection_threads";
const MESSAGES: &str = "orchestration_v2_projection_messages";
const PROJECTS: &str = "projection_projects";
const CANDIDATE_LIMIT: usize = 10_000;
const HIT_LIMIT: usize = 2000;
const SCAN_LIMIT: i128 = 20_000;
const PLACEHOLDER: &str = "[User attached one or more files without additional text. Respond using the conversation context and the attached files.]";
const PREFIX_WHITESPACE: &str = "\t\n\u{b}\u{c}\r \u{a0}\u{1680}\u{2000}\u{2001}\u{2002}\u{2003}\u{2004}\u{2005}\u{2006}\u{2007}\u{2008}\u{2009}\u{200a}\u{2028}\u{2029}\u{202f}\u{205f}\u{3000}\u{feff}";

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ThreadSearchQuery {
    HasIndex {},
    MessageHits {
        r#match: Option<String>,
        substrings: String,
        short_spellings: String,
        project_id: Option<String>,
        excluded_thread_ids: String,
    },
    ThreadOpens {
        thread_ids: String,
    },
    TitleCandidates {
        project_id: Option<String>,
    },
    OlderSettledTitles {
        before: String,
        project_ids: Option<String>,
    },
    OlderSettledIds {
        before: String,
        thread_ids: String,
        project_ids: Option<String>,
    },
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
struct Posting {
    term: String,
    rowid: String,
    offset: u64,
}

/// Live messages store one record per distinct trigram, not per occurrence.
/// Consecutive occurrences collapse into ranges, so long repeated model output
/// is represented by a few bytes rather than tens of thousands of mutations.
type Positions = Vec<[u64; 2]>;
const DIRECT_TERM_LIMIT: usize = 4096;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct LivePosting {
    term: String,
    rowid: String,
    #[serde(with = "position_codec")]
    positions: Positions,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct LiveDocument {
    rowid: String,
    /// A source DELETE selects its witnessed survivor vocabulary. A later
    /// native DELETE removes only terms owned by the latest insertion; older
    /// unmatched terms are historical contentless-index remnants.
    source_deleted: bool,
    source_present: bool,
    latest: LatestGeneration,
    suppressed: BTreeSet<String>,
    terms: Vec<String>,
    buckets: Vec<u16>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum LatestGeneration {
    Source,
    Missing,
    Native { terms: Vec<String> },
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct LiveBucket {
    rowid: String,
    bucket: u16,
    #[serde(with = "position_codec::map")]
    postings: BTreeMap<String, Positions>,
}

fn invalid(message: &str) -> Error {
    Error::Invalid(format!("Thread search: {message}"))
}
fn encode(value: &impl Serialize) -> Result<Vec<u8>> {
    serde_json::to_vec(value).map_err(|_| Error::IncompatibleStore)
}
fn decode<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    serde_json::from_slice(bytes).map_err(|_| Error::IncompatibleStore)
}
fn metadata(catalog: &ApplicationCatalog, suffix: &str) -> Vec<u8> {
    store_key(&format!(
        "application/generation/{}/metadata/{suffix}",
        catalog.generation
    ))
}
fn hex(value: &str) -> String {
    value.bytes().map(|byte| format!("{byte:02x}")).collect()
}
fn descending(rowid: i64) -> String {
    format!("{:016x}", !((rowid as u64) ^ (1 << 63)))
}
fn prefix_end(prefix: &[u8]) -> Vec<u8> {
    let mut end = prefix.to_vec();
    *end.last_mut().expect("nonempty storage prefix") += 1;
    end
}
fn term_prefix(catalog: &ApplicationCatalog, term: &str) -> Vec<u8> {
    metadata(catalog, &format!("search-posting/{}/", hex(term)))
}
fn posting_prefix(catalog: &ApplicationCatalog, term: &str, rowid: i64) -> Vec<u8> {
    source_posting_prefix(catalog, term, rowid, false)
}
fn source_posting_prefix(
    catalog: &ApplicationCatalog,
    term: &str,
    rowid: i64,
    survivor: bool,
) -> Vec<u8> {
    let index = if survivor {
        "search-survivor-posting"
    } else {
        "search-posting"
    };
    metadata(
        catalog,
        &format!("{index}/{}/{}/", hex(term), descending(rowid)),
    )
}
async fn source_present(
    tx: &mut Transaction,
    catalog: &ApplicationCatalog,
    rowid: i64,
    survivor: bool,
) -> Result<bool> {
    let index = if survivor {
        "search-survivor-document"
    } else {
        "search-document"
    };
    let prefix = metadata(catalog, &format!("{index}/{}/", descending(rowid)));
    Ok(!tx.scan(&prefix, &prefix_end(&prefix), 1).await?.is_empty())
}
fn live_document_key(catalog: &ApplicationCatalog, rowid: i64) -> Vec<u8> {
    metadata(
        catalog,
        &format!("search-live-document/{}", descending(rowid)),
    )
}
fn live_term_prefix(catalog: &ApplicationCatalog, term: &str) -> Vec<u8> {
    metadata(catalog, &format!("search-live-posting/{}/", hex(term)))
}
fn live_posting_key(catalog: &ApplicationCatalog, term: &str, rowid: i64) -> Vec<u8> {
    metadata(
        catalog,
        &format!("search-live-posting/{}/{}", hex(term), descending(rowid)),
    )
}
fn term_bucket(term: &str) -> u16 {
    // A storage partition, not an identity or security hash. Collisions only
    // co-locate exact term maps, and are checked before candidate counting.
    (term.bytes().fold(2166136261_u32, |hash, byte| {
        (hash ^ u32::from(byte)).wrapping_mul(16777619)
    }) & 255) as u16
}
fn live_bucket_prefix(catalog: &ApplicationCatalog, bucket: u16) -> Vec<u8> {
    metadata(catalog, &format!("search-live-bucket/{bucket:02x}/"))
}
fn live_bucket_key(catalog: &ApplicationCatalog, bucket: u16, rowid: i64) -> Vec<u8> {
    metadata(
        catalog,
        &format!("search-live-bucket/{bucket:02x}/{}", descending(rowid)),
    )
}
async fn live_document(
    tx: &mut Transaction,
    catalog: &ApplicationCatalog,
    rowid: i64,
) -> Result<Option<LiveDocument>> {
    let Some(bytes) = tx.get(&live_document_key(catalog, rowid)).await? else {
        return Ok(None);
    };
    let document: LiveDocument = decode(&bytes)?;
    if document.rowid != rowid.to_string()
        || document.terms.len() > DIRECT_TERM_LIMIT
        || document.terms.windows(2).any(|pair| pair[0] >= pair[1])
        || (!document.terms.is_empty() && !document.buckets.is_empty())
        || document.buckets.iter().any(|&bucket| bucket > 255)
        || document.buckets.windows(2).any(|pair| pair[0] >= pair[1])
        || matches!(&document.latest, LatestGeneration::Native { terms }
            if terms.windows(2).any(|pair| pair[0] >= pair[1]))
        || (!document.source_present && !document.suppressed.is_empty())
    {
        return Err(Error::IncompatibleStore);
    }
    Ok(Some(document))
}
fn validate_positions(positions: &Positions) -> Result<()> {
    if positions.is_empty()
        || positions.iter().any(|[start, end]| start > end)
        || positions
            .windows(2)
            .any(|pair| pair[0][1].saturating_add(1) >= pair[1][0])
    {
        return Err(Error::IncompatibleStore);
    }
    Ok(())
}

fn validate_bucket(bucket: &LiveBucket, rowid: i64, number: u16) -> Result<()> {
    if bucket.rowid != rowid.to_string() || bucket.bucket != number || bucket.postings.is_empty() {
        return Err(Error::IncompatibleStore);
    }
    for (term, positions) in &bucket.postings {
        if term_bucket(term) != number {
            return Err(Error::IncompatibleStore);
        }
        validate_positions(positions)?;
    }
    Ok(())
}

async fn load_live_postings(
    tx: &mut Transaction,
    catalog: &ApplicationCatalog,
    rowid: i64,
    document: &LiveDocument,
) -> Result<BTreeMap<String, Positions>> {
    let mut result = BTreeMap::new();
    for term in &document.terms {
        let posting: LivePosting = tx
            .get(&live_posting_key(catalog, term, rowid))
            .await?
            .as_deref()
            .map(decode)
            .transpose()?
            .ok_or(Error::IncompatibleStore)?;
        if posting.rowid != rowid.to_string() || &posting.term != term {
            return Err(Error::IncompatibleStore);
        }
        validate_positions(&posting.positions)?;
        result.insert(term.clone(), posting.positions);
    }
    for &number in &document.buckets {
        let bucket: LiveBucket = tx
            .get(&live_bucket_key(catalog, number, rowid))
            .await?
            .as_deref()
            .map(decode)
            .transpose()?
            .ok_or(Error::IncompatibleStore)?;
        validate_bucket(&bucket, rowid, number)?;
        result.extend(bucket.postings);
    }
    Ok(result)
}
fn add_position(positions: &mut Positions, offset: u64) -> Result<()> {
    if let Some([_, end]) = positions.last_mut() {
        if offset <= *end {
            return Err(Error::IncompatibleStore);
        }
        if end.checked_add(1) == Some(offset) {
            *end = offset;
            return Ok(());
        }
    }
    positions.push([offset, offset]);
    Ok(())
}
fn posting_keys(catalog: &ApplicationCatalog, posting: &Posting) -> Result<[Vec<u8>; 2]> {
    let rowid = posting
        .rowid
        .parse::<i64>()
        .map_err(|_| Error::IncompatibleStore)?;
    if posting.rowid != rowid.to_string() {
        return Err(Error::IncompatibleStore);
    }
    let order = descending(rowid);
    let term = hex(&posting.term);
    Ok([
        metadata(
            catalog,
            &format!("search-posting/{term}/{order}/{:016x}", posting.offset),
        ),
        metadata(
            catalog,
            &format!("search-document/{order}/{term}/{:016x}", posting.offset),
        ),
    ])
}

pub(crate) async fn has_index(tx: &mut Transaction, catalog: &ApplicationCatalog) -> Result<bool> {
    let Some(header) = tx.get(&metadata(catalog, "header")).await? else {
        // New, empty native databases have the same lexical index as a modern SQLite install.
        return Ok(true);
    };
    let header: Value = decode(&header)?;
    if header.get("version").and_then(Value::as_u64) != Some(2) {
        return Err(invalid(
            "exact search metadata requires a version 2 SQLite import",
        ));
    }
    let indexed = header
        .get("searchIndex")
        .and_then(Value::as_bool)
        .ok_or(Error::IncompatibleStore)?;
    if indexed && header.get("searchDeleteWitness").and_then(Value::as_bool) != Some(true) {
        return Err(invalid(
            "exact contentless-delete metadata is missing; reimport a witnessed SQLite snapshot before activation",
        ));
    }
    Ok(indexed)
}

#[derive(Serialize)]
pub struct PreparationResult {
    pub prepared: bool,
    pub indexed_rows: u64,
}

impl Store {
    /// Search source positions and delete-survivor witnesses are imported as a
    /// single validated generation. Check their version before selecting native
    /// persistence; never silently rebuild a historically stale contentless index.
    pub async fn prepare_application_search(&self) -> Result<PreparationResult> {
        let mut tx = self.db.begin_read_only().await?;
        let catalog = decode_catalog(
            &tx.get(&schema_key())
                .await?
                .ok_or(Error::IncompatibleStore)?,
        )?;
        has_index(&mut tx, &catalog).await?;
        tx.rollback().await?;
        Ok(PreparationResult {
            prepared: true,
            indexed_rows: 0,
        })
    }
}

fn payload(row: &Map<String, Value>) -> Result<Value> {
    serde_json::from_str(&text(row, "payload_json")?).map_err(|_| Error::IncompatibleStore)
}
fn extracted(value: Option<&Value>) -> Value {
    match value {
        Some(Value::Array(_) | Value::Object(_)) => json!(value.unwrap().to_string()),
        Some(Value::Bool(value)) => json!(i64::from(*value)),
        Some(value) => value.clone(),
        None => Value::Null,
    }
}
fn sql_text(value: &Value) -> Option<String> {
    match value {
        Value::Null => None,
        Value::String(value) => Some(value.clone()),
        Value::Bool(value) => Some(i64::from(*value).to_string()),
        value => Some(value.to_string()),
    }
}
pub(crate) fn message_text(row: &Map<String, Value>, payload: &Value) -> Result<Value> {
    // Missing is only used while migration 81 reads its old, fenced row.
    let readable = if row.contains_key("search_text") {
        nullable(row, "search_text")?
    } else {
        None
    };
    Ok(readable
        .map(Value::String)
        .unwrap_or_else(|| extracted(payload.get("text"))))
}
fn indexed_text(row: &Map<String, Value>, payload: &Value) -> Result<Option<String>> {
    Ok(sql_text(&message_text(row, payload)?).map(|text| text.replace(['’', '‘'], "'")))
}
fn is_one(value: Option<&Value>) -> bool {
    value == Some(&Value::Bool(true)) || value.and_then(Value::as_f64) == Some(1.0)
}
fn sql_is(left: Value, right: Value) -> bool {
    match (left.as_f64(), right.as_f64()) {
        (Some(left), Some(right)) => left == right,
        _ => left == right,
    }
}
fn non_placeholder(value: &Value) -> bool {
    let Some(text) = sql_text(&extracted(value.get("text"))) else {
        return false;
    };
    // SQLite substr() stops at an embedded NUL, while the unprefixed BINARY
    // comparison uses the complete text bytes.
    let compared = if text
        .get(..11)
        .is_some_and(|head| head.eq_ignore_ascii_case("Ultrathink:"))
    {
        text[11..]
            .split('\0')
            .next()
            .unwrap_or("")
            .trim_start_matches(|c| PREFIX_WHITESPACE.contains(c))
    } else {
        &text
    };
    compared != PLACEHOLDER
}
fn listed(value: &Value) -> bool {
    let lineage = value.get("lineage");
    lineage
        .and_then(|lineage| lineage.get("relationshipToParent"))
        .and_then(Value::as_str)
        != Some("subagent")
        && !is_one(lineage.and_then(|lineage| lineage.get("sideChat")))
}

async fn indexable(
    tx: &mut Transaction,
    catalog: &ApplicationCatalog,
    row: &Map<String, Value>,
) -> Result<bool> {
    if integer(row, "streaming")? != 0 {
        return Ok(false);
    }
    let value = payload(row)?;
    let role = text(row, "role")?;
    let created_by = extracted(value.get("createdBy"));
    if role != "assistant"
        && !(role == "user" && (created_by.is_null() || created_by.as_str() == Some("user")))
    {
        return Ok(false);
    }
    let thread_id = text(row, "thread_id")?;
    if let Some(thread) = read_row(tx, catalog, THREADS, &[text_cell(&thread_id)]).await? {
        if payload(&thread.row)?
            .get("lineage")
            .and_then(|lineage| lineage.get("relationshipToParent"))
            .and_then(Value::as_str)
            == Some("subagent")
        {
            return Ok(false);
        }
    }
    Ok(non_placeholder(&value))
}

/// Called in the projection owner's write transaction after allocating/preserving rowid.
/// Conditions mirror the original insert/update/delete triggers, including updates
/// that intentionally leave the index untouched.
pub(crate) async fn replace_message(
    tx: &mut Transaction,
    catalog: &ApplicationCatalog,
    old: Option<&Map<String, Value>>,
    new: Option<&Map<String, Value>>,
    rowid: i64,
) -> Result<()> {
    tx.require_write()?;
    if !has_index(tx, catalog).await? {
        return Ok(());
    }
    if let (Some(old), Some(new)) = (old, new) {
        let old_streaming = integer(old, "streaming")?;
        let new_streaming = integer(new, "streaming")?;
        if old_streaming != 0 && new_streaming != 0 {
            return Ok(());
        }
        let old_payload = payload(old)?;
        let new_payload = payload(new)?;
        if old_streaming == new_streaming
            && text(old, "role")? == text(new, "role")?
            && indexed_text(old, &old_payload)? == indexed_text(new, &new_payload)?
            && sql_is(
                extracted(old_payload.get("createdBy")),
                extracted(new_payload.get("createdBy")),
            )
        {
            return Ok(());
        }
    }
    let delete = old.map(|row| integer(row, "streaming")).transpose()? == Some(0);
    let mut insert = false;
    let mut postings: BTreeMap<String, Positions> = BTreeMap::new();
    if let Some(new) = new {
        if indexable(tx, catalog, new).await? {
            let value = payload(new)?;
            if !extracted(value.get("text")).is_null() {
                // Even an insertion with fewer than three folded characters
                // owns a generation. Its later DELETE must not remove older
                // postings merely because its own vocabulary is empty.
                insert = true;
                let text = indexed_text(new, &value)?.ok_or(Error::IncompatibleStore)?;
                for (offset, term) in unicode::trigram_iter(&text).enumerate() {
                    add_position(postings.entry(term).or_default(), offset as u64)?;
                }
            }
        }
    }
    if !delete && !insert {
        return Ok(());
    }
    let mut document = match live_document(tx, catalog, rowid).await? {
        Some(document) => document,
        None => LiveDocument {
            rowid: rowid.to_string(),
            source_deleted: false,
            source_present: source_present(tx, catalog, rowid, false).await?,
            latest: if tx
                .get(&metadata(
                    catalog,
                    &format!("search-current-document/{}", descending(rowid)),
                ))
                .await?
                .is_some()
            {
                LatestGeneration::Source
            } else {
                LatestGeneration::Missing
            },
            suppressed: BTreeSet::new(),
            terms: Vec::new(),
            buckets: Vec::new(),
        },
    };
    let mut previous = load_live_postings(tx, catalog, rowid, &document).await?;
    if delete {
        match &document.latest {
            LatestGeneration::Source => {
                // The source may already have duplicate insert generations.
                // Its exact per-row deletion witness, not its current payload,
                // tells us which old terms survive the first DELETE.
                document.source_deleted = true;
                document.source_present = source_present(tx, catalog, rowid, true).await?;
            }
            LatestGeneration::Native { terms } => {
                for term in terms {
                    previous.remove(term);
                    if document.source_present {
                        // Do not reveal an earlier source term's positions after
                        // deleting a native generation that overwrote that term.
                        document.suppressed.insert(term.clone());
                    }
                }
            }
            LatestGeneration::Missing => {}
        }
        document.latest = LatestGeneration::Missing;
    }
    if insert {
        document.latest = LatestGeneration::Native {
            terms: postings.keys().cloned().collect(),
        };
        previous.extend(postings);
    }
    let postings = previous;
    for term in &document.terms {
        tx.delete(live_posting_key(catalog, term, rowid)).await?;
    }
    for &bucket in &document.buckets {
        tx.delete(live_bucket_key(catalog, bucket, rowid)).await?;
    }
    document.terms.clear();
    document.buckets.clear();
    if postings.len() <= DIRECT_TERM_LIMIT {
        for (term, positions) in postings {
            let posting = LivePosting {
                term: term.clone(),
                rowid: rowid.to_string(),
                positions,
            };
            tx.put(live_posting_key(catalog, &term, rowid), encode(&posting)?)
                .await?;
            document.terms.push(term);
        }
    } else {
        // Large, high-entropy documents otherwise exceed an atomic transaction's
        // record cap even when their source text is modest. Only these documents
        // use bounded fan-out maps; ordinary messages retain direct term lookup.
        let mut buckets: BTreeMap<u16, BTreeMap<String, Positions>> = BTreeMap::new();
        for (term, positions) in postings {
            buckets
                .entry(term_bucket(&term))
                .or_default()
                .insert(term, positions);
        }
        for (number, postings) in buckets {
            let bucket = LiveBucket {
                rowid: rowid.to_string(),
                bucket: number,
                postings,
            };
            tx.put(live_bucket_key(catalog, number, rowid), encode(&bucket)?)
                .await?;
            document.buckets.push(number);
        }
    }
    tx.put(live_document_key(catalog, rowid), encode(&document)?)
        .await?;
    Ok(())
}

fn phrases(input: &str) -> Result<Vec<Vec<String>>> {
    let mut source = input.chars().peekable();
    let mut result = Vec::new();
    while source.peek().is_some() {
        while source.peek().is_some_and(|c| c.is_ascii_whitespace()) {
            source.next();
        }
        if source.peek().is_none() {
            break;
        }
        if source.next() != Some('"') {
            return Err(invalid("only quoted lexical phrases are accepted"));
        }
        let mut phrase = String::new();
        loop {
            match source.next() {
                None => return Err(invalid("unterminated lexical phrase")),
                Some('"') if source.peek() == Some(&'"') => {
                    source.next();
                    phrase.push('"');
                }
                Some('"') => break,
                Some(character) => phrase.push(character),
            }
        }
        if source.peek().is_some_and(|c| !c.is_ascii_whitespace()) {
            return Err(invalid("lexical phrases must be separated by whitespace"));
        }
        let tokens = unicode::trigrams(&phrase);
        // FTS5 ignores an empty phrase when combined with nonempty phrases.
        if !tokens.is_empty() {
            result.push(tokens);
        }
    }
    Ok(result)
}

async fn legacy_next_document(
    tx: &mut Transaction,
    catalog: &ApplicationCatalog,
    term: &str,
    mut maximum: i64,
) -> Result<Option<i64>> {
    let prefix = term_prefix(catalog, term);
    loop {
        let start = posting_prefix(catalog, term, maximum);
        let Some((key, bytes)) = tx.scan(&start, &prefix_end(&prefix), 1).await?.pop() else {
            return Ok(None);
        };
        let posting: Posting = decode(&bytes)?;
        let rowid = posting
            .rowid
            .parse::<i64>()
            .map_err(|_| Error::IncompatibleStore)?;
        if posting.term != term || rowid > maximum || posting_keys(catalog, &posting)?[0] != key {
            return Err(Error::IncompatibleStore);
        }
        let available = match live_document(tx, catalog, rowid).await? {
            None => true,
            Some(document) if !document.source_present || document.suppressed.contains(term) => {
                false
            }
            Some(document) if document.source_deleted => {
                let prefix = source_posting_prefix(catalog, term, rowid, true);
                !tx.scan(&prefix, &prefix_end(&prefix), 1).await?.is_empty()
            }
            Some(_) => true,
        };
        if available {
            return Ok(Some(rowid));
        }
        let Some(next) = rowid.checked_sub(1) else {
            return Ok(None);
        };
        maximum = next;
    }
}

async fn direct_next_document(
    tx: &mut Transaction,
    catalog: &ApplicationCatalog,
    term: &str,
    maximum: i64,
) -> Result<Option<i64>> {
    let prefix = live_term_prefix(catalog, term);
    let start = live_posting_key(catalog, term, maximum);
    let Some((key, bytes)) = tx.scan(&start, &prefix_end(&prefix), 1).await?.pop() else {
        return Ok(None);
    };
    let posting: LivePosting = decode(&bytes)?;
    let rowid = posting
        .rowid
        .parse::<i64>()
        .map_err(|_| Error::IncompatibleStore)?;
    if posting.rowid != rowid.to_string()
        || posting.term != term
        || rowid > maximum
        || live_posting_key(catalog, term, rowid) != key
    {
        return Err(Error::IncompatibleStore);
    }
    validate_positions(&posting.positions)?;
    let document = live_document(tx, catalog, rowid)
        .await?
        .ok_or(Error::IncompatibleStore)?;
    if document
        .terms
        .binary_search_by(|candidate| candidate.as_str().cmp(term))
        .is_err()
    {
        return Err(Error::IncompatibleStore);
    }
    Ok(Some(rowid))
}

async fn bucket_next_document(
    tx: &mut Transaction,
    catalog: &ApplicationCatalog,
    term: &str,
    mut maximum: i64,
) -> Result<Option<i64>> {
    let number = term_bucket(term);
    let prefix = live_bucket_prefix(catalog, number);
    loop {
        let start = live_bucket_key(catalog, number, maximum);
        let Some((key, bytes)) = tx.scan(&start, &prefix_end(&prefix), 1).await?.pop() else {
            return Ok(None);
        };
        let bucket: LiveBucket = decode(&bytes)?;
        let rowid = bucket
            .rowid
            .parse::<i64>()
            .map_err(|_| Error::IncompatibleStore)?;
        validate_bucket(&bucket, rowid, number)?;
        if rowid > maximum || live_bucket_key(catalog, number, rowid) != key {
            return Err(Error::IncompatibleStore);
        }
        let document = live_document(tx, catalog, rowid)
            .await?
            .ok_or(Error::IncompatibleStore)?;
        if document.buckets.binary_search(&number).is_err() {
            return Err(Error::IncompatibleStore);
        }
        if bucket.postings.contains_key(term) {
            return Ok(Some(rowid));
        }
        let Some(next) = rowid.checked_sub(1) else {
            return Ok(None);
        };
        maximum = next;
    }
}

async fn next_document(
    tx: &mut Transaction,
    catalog: &ApplicationCatalog,
    term: &str,
    maximum: i64,
) -> Result<Option<i64>> {
    Ok([
        legacy_next_document(tx, catalog, term, maximum).await?,
        direct_next_document(tx, catalog, term, maximum).await?,
        bucket_next_document(tx, catalog, term, maximum).await?,
    ]
    .into_iter()
    .flatten()
    .max())
}

async fn positions(
    tx: &mut Transaction,
    catalog: &ApplicationCatalog,
    term: &str,
    rowid: i64,
) -> Result<Positions> {
    let mut survivor = false;
    if let Some(document) = live_document(tx, catalog, rowid).await? {
        if document
            .terms
            .binary_search_by(|candidate| candidate.as_str().cmp(term))
            .is_ok()
        {
            let posting: LivePosting = tx
                .get(&live_posting_key(catalog, term, rowid))
                .await?
                .as_deref()
                .map(decode)
                .transpose()?
                .ok_or(Error::IncompatibleStore)?;
            if posting.rowid != rowid.to_string() || posting.term != term {
                return Err(Error::IncompatibleStore);
            }
            validate_positions(&posting.positions)?;
            return Ok(posting.positions);
        }
        let number = term_bucket(term);
        if document.buckets.binary_search(&number).is_ok() {
            let mut bucket: LiveBucket = tx
                .get(&live_bucket_key(catalog, number, rowid))
                .await?
                .as_deref()
                .map(decode)
                .transpose()?
                .ok_or(Error::IncompatibleStore)?;
            validate_bucket(&bucket, rowid, number)?;
            if let Some(positions) = bucket.postings.remove(term) {
                return Ok(positions);
            }
        }
        if !document.source_present || document.suppressed.contains(term) {
            return Ok(Vec::new());
        }
        survivor = document.source_deleted;
    }
    let prefix = source_posting_prefix(catalog, term, rowid, survivor);
    let end = prefix_end(&prefix);
    let mut cursor = prefix.clone();
    let mut result = Vec::new();
    loop {
        let page = tx.scan(&cursor, &end, 128).await?;
        let count = page.len();
        for (key, bytes) in page {
            let posting: Posting = decode(&bytes)?;
            if posting.term != term || posting.rowid != rowid.to_string() || {
                let mut expected = prefix.clone();
                expected.extend_from_slice(format!("{:016x}", posting.offset).as_bytes());
                expected != key
            } {
                return Err(Error::IncompatibleStore);
            }
            add_position(&mut result, posting.offset)?;
            cursor = key;
            cursor.push(0);
        }
        if count < 128 {
            return Ok(result);
        }
    }
}

fn intersect(left: &Positions, right: &Positions, shift: u64) -> Positions {
    let mut result = Vec::new();
    let (mut a, mut b) = (0, 0);
    while a < left.len() && b < right.len() {
        if right[b][1] < shift {
            b += 1;
            continue;
        }
        let start = left[a][0].max(right[b][0].saturating_sub(shift));
        let end = left[a][1].min(right[b][1] - shift);
        if start <= end {
            result.push([start, end]);
        }
        if left[a][1] < right[b][1] - shift {
            a += 1;
        } else {
            b += 1;
        }
    }
    result
}

async fn matching_documents(
    tx: &mut Transaction,
    catalog: &ApplicationCatalog,
    input: &str,
) -> Result<Vec<i64>> {
    let phrases = phrases(input)?;
    let terms: BTreeSet<&str> = phrases.iter().flatten().map(String::as_str).collect();
    if terms.is_empty() {
        return Ok(Vec::new());
    }
    let mut maximum = i64::MAX;
    let mut result = Vec::new();
    loop {
        let mut aligned = true;
        for term in &terms {
            let Some(found) = next_document(tx, catalog, term, maximum).await? else {
                return Ok(result);
            };
            if found != maximum {
                maximum = found;
                aligned = false;
            }
        }
        if !aligned {
            continue;
        }
        let mut indexed = BTreeMap::new();
        for term in &terms {
            indexed.insert(*term, positions(tx, catalog, term, maximum).await?);
        }
        let matches = phrases.iter().all(|phrase| {
            let mut starts = indexed[phrase[0].as_str()].clone();
            for (offset, term) in phrase.iter().enumerate().skip(1) {
                starts = intersect(&starts, &indexed[term.as_str()], offset as u64);
                if starts.is_empty() {
                    break;
                }
            }
            !starts.is_empty()
        });
        if matches {
            result.push(maximum);
            if result.len() == CANDIDATE_LIMIT {
                return Ok(result);
            }
        }
        let Some(next) = maximum.checked_sub(1) else {
            return Ok(result);
        };
        maximum = next;
    }
}

fn list(input: &str) -> Result<Vec<String>> {
    serde_json::from_str(input).map_err(|_| invalid("expected an encoded string list"))
}

async fn latest_user_message_at(session: &mut Session, thread_id: &str) -> Result<Option<String>> {
    let mut candidates = Vec::new();
    // The shared compact index retains role/createdBy and scalar timestamps.
    // Ranking never needs to load historical message bodies for recency.
    for indexed in
        read_index::window::equal_metadata(session, MESSAGES, "thread_id", &text_cell(thread_id))
            .await?
    {
        let row = indexed.row;
        if text(&row, "role")? != "user" {
            continue;
        }
        candidates.push((text(&row, "updated_at")?, text(&row, "message_id")?, row));
    }
    candidates.sort_by(|a, b| (&b.0, &b.1).cmp(&(&a.0, &a.1)));
    for (updated_at, _, row) in candidates {
        let value = payload(&row)?;
        let created_by = extracted(value.get("createdBy"));
        if !created_by.is_null() && created_by.as_str() != Some("user") {
            continue;
        }
        return Ok(Some(updated_at));
    }
    Ok(None)
}

async fn eligible_thread(
    session: &mut Session,
    thread_id: &str,
    project_id: Option<&str>,
    excluded: &BTreeSet<String>,
) -> Result<Option<String>> {
    if excluded.contains(thread_id) {
        return Ok(None);
    }
    let Some(thread) = read_row(
        &mut session.transaction,
        &session.catalog,
        THREADS,
        &[text_cell(thread_id)],
    )
    .await?
    else {
        return Ok(None);
    };
    let project = text(&thread.row, "project_id")?;
    if project_id.is_some_and(|scope| scope != project)
        || nullable(&thread.row, "deleted_at")?.is_some()
        || nullable(&thread.row, "archived_at")?.is_some()
        || !listed(&payload(&thread.row)?)
    {
        return Ok(None);
    }
    let Some(source) = read_row(
        &mut session.transaction,
        &session.catalog,
        PROJECTS,
        &[text_cell(&project)],
    )
    .await?
    else {
        return Ok(None);
    };
    if nullable(&source.row, "deleted_at")?.is_some() {
        return Ok(None);
    }
    Ok(Some(project))
}

async fn message_hits(
    session: &mut Session,
    matching: Option<String>,
    substrings: String,
    short_spellings: String,
    project_id: Option<String>,
    excluded_thread_ids: String,
) -> Result<Value> {
    let substrings = list(&substrings)?;
    let spellings: Vec<Vec<String>> = serde_json::from_str(&short_spellings)
        .map_err(|_| invalid("invalid short-token spellings"))?;
    let excluded = list(&excluded_thread_ids)?.into_iter().collect();
    let indexed = matching.is_some();
    let mut selected = Vec::new();
    if let Some(input) = matching {
        if !has_index(&mut session.transaction, &session.catalog).await? {
            return Err(invalid("lexical index is unavailable"));
        }
        let candidates =
            matching_documents(&mut session.transaction, &session.catalog, &input).await?;
        let cutoff = if candidates.len() == CANDIDATE_LIMIT {
            *candidates.last().unwrap()
        } else {
            0
        };
        for rowid in candidates {
            if rowid < cutoff {
                continue;
            }
            if let Some(id) =
                rowids::identity(&mut session.transaction, &session.catalog, rowid).await?
            {
                selected.push((rowid, id));
            }
        }
    } else {
        let newest = rowids::newest(&mut session.transaction, &session.catalog, None, 1).await?;
        let cutoff = i128::from(newest.first().map(|row| row.0).unwrap_or(0)) - SCAN_LIMIT;
        let mut after = None;
        loop {
            let page =
                rowids::newest(&mut session.transaction, &session.catalog, after, 128).await?;
            let count = page.len();
            let mut exhausted = false;
            for (rowid, id) in page {
                after = Some(rowid);
                if i128::from(rowid) <= cutoff {
                    exhausted = true;
                    break;
                }
                selected.push((rowid, id));
            }
            if exhausted || count < 128 {
                break;
            }
        }
    }
    let mut result = Vec::new();
    let mut eligible = BTreeMap::new();
    for (rowid, message_id) in selected {
        let row = read_row(
            &mut session.transaction,
            &session.catalog,
            MESSAGES,
            &[text_cell(&message_id)],
        )
        .await?
        .ok_or(Error::IncompatibleStore)?
        .row;
        let thread_id = text(&row, "thread_id")?;
        if !eligible.contains_key(&thread_id) {
            eligible.insert(
                thread_id.clone(),
                eligible_thread(session, &thread_id, project_id.as_deref(), &excluded).await?,
            );
        }
        let Some(project_id) = eligible[&thread_id].as_ref() else {
            continue;
        };
        let value = payload(&row)?;
        if (indexed && !non_placeholder(&value))
            || (!indexed && !indexable(&mut session.transaction, &session.catalog, &row).await?)
            || message_id
                .get(..13)
                .is_some_and(|prefix| prefix.eq_ignore_ascii_case("async-answer:"))
        {
            continue;
        }
        let raw_text = message_text(&row, &value)?;
        let text = sql_text(&raw_text).unwrap_or_default();
        let lower = text.to_ascii_lowercase();
        if substrings.iter().any(|token| !lower.contains(token))
            || spellings
                .iter()
                .any(|forms| !forms.iter().any(|spelling| text.contains(spelling)))
        {
            continue;
        }
        // The unchanged application boundary uses Node SQLite's default numeric
        // rows, which rejects integers outside the JS safe range. Keep exact
        // signed rowids internally, but do not round a returned identity.
        const MAX_SAFE_ROWID: i64 = (1_i64 << 53) - 1;
        if !(-MAX_SAFE_ROWID..=MAX_SAFE_ROWID).contains(&rowid) {
            return Err(invalid(
                "matching rowid cannot be represented as an application number",
            ));
        }
        result.push(json!({
            "messageRowid":rowid, "messageId":message_id, "threadId":thread_id,
            "projectId":project_id, "source":crate::application_rows::text(&row,"role")?,
            "text":raw_text, "messageCreatedAt":nullable(&row,"created_at")?,
        }));
        if result.len() == HIT_LIMIT {
            break;
        }
    }
    Ok(json!(result))
}

async fn thread_opens(session: &mut Session, thread_ids: String) -> Result<Value> {
    let mut result = Vec::new();
    for id in list(&thread_ids)?.into_iter().collect::<BTreeSet<_>>() {
        let Some(thread) = read_row(
            &mut session.transaction,
            &session.catalog,
            THREADS,
            &[text_cell(&id)],
        )
        .await?
        else {
            continue;
        };
        result.push(json!({
            "threadId":id, "opens":extracted(payload(&thread.row)?.get("opens")),
            "latestUserMessageAt":latest_user_message_at(session,&id).await?,
        }));
    }
    Ok(json!(result))
}

fn older_settled(
    row: &Map<String, Value>,
    value: &Value,
    before: &str,
    projects: Option<&BTreeSet<String>>,
) -> Result<bool> {
    Ok(nullable(row, "deleted_at")?.is_none()
        && value.get("archivedAt").is_none_or(Value::is_null)
        && listed(value)
        && value.get("settledOverride").and_then(Value::as_str) == Some("settled")
        && value
            .get("settledAt")
            .and_then(Value::as_str)
            .is_some_and(|at| at < before)
        && projects.is_none_or(|scope| text(row, "project_id").is_ok_and(|id| scope.contains(&id))))
}

async fn title_candidates(
    session: &mut Session,
    project_id: Option<String>,
    older: Option<(String, Option<BTreeSet<String>>)>,
) -> Result<Value> {
    let mut threads = read_index::all_rows(session, THREADS).await?;
    threads.sort_by_key(|row| row.order);
    let mut result = Vec::new();
    for indexed in threads {
        let row = indexed.row;
        if nullable(&row, "deleted_at")?.is_some() {
            continue;
        }
        let id = text(&row, "thread_id")?;
        let project = text(&row, "project_id")?;
        if older.is_none()
            && (nullable(&row, "archived_at")?.is_some()
                || project_id.as_ref().is_some_and(|scope| *scope != project))
        {
            continue;
        }
        let value = payload(&row)?;
        if let Some((before, projects)) = &older {
            if !older_settled(&row, &value, before, projects.as_ref())? {
                continue;
            }
        } else if !listed(&value) {
            continue;
        }
        let Some(project) = read_row(
            &mut session.transaction,
            &session.catalog,
            PROJECTS,
            &[text_cell(&project)],
        )
        .await?
        else {
            continue;
        };
        if nullable(&project.row, "deleted_at")?.is_some() {
            continue;
        }
        let latest = latest_user_message_at(session, &id).await?;
        let project_title = text(&project.row, "title")?;
        if older.is_some() {
            result.push(json!({
                "id":id,"title":extracted(value.get("title")),"branch":extracted(value.get("branch")),
                "pullRequests":extracted(value.get("pullRequests")),"linkedPullRequest":extracted(value.get("linkedPullRequest")),
                "branchPullRequest":extracted(value.get("branchPullRequest")),"opens":extracted(value.get("opens")),
                "createdAt":extracted(value.get("createdAt")),"projectTitle":project_title,"latestUserMessageAt":latest,
            }));
        } else {
            result.push(json!({"thread":text(&row,"payload_json")?,"projectTitle":project_title,"latestUserMessageAt":latest}));
        }
    }
    Ok(json!(result))
}

async fn older_ids(
    session: &mut Session,
    before: String,
    thread_ids: String,
    project_ids: Option<String>,
) -> Result<Value> {
    let projects = project_ids
        .as_deref()
        .map(list)
        .transpose()?
        .map(|ids| ids.into_iter().collect());
    let mut result = Vec::new();
    for id in list(&thread_ids)?.into_iter().collect::<BTreeSet<_>>() {
        let Some(thread) = read_row(
            &mut session.transaction,
            &session.catalog,
            THREADS,
            &[text_cell(&id)],
        )
        .await?
        else {
            continue;
        };
        if nullable(&thread.row, "deleted_at")?.is_some() {
            continue;
        }
        if older_settled(
            &thread.row,
            &payload(&thread.row)?,
            &before,
            projects.as_ref(),
        )? {
            result.push(json!({"id":id}));
        }
    }
    Ok(json!(result))
}

impl ApplicationTransactions {
    pub async fn thread_search(&mut self, id: &str, query: ThreadSearchQuery) -> Result<Value> {
        let mut session = self.take(id)?;
        let result = async {
            read_index::ensure_ready(&mut session).await?;
            match query {
                ThreadSearchQuery::HasIndex {} => {
                    has_index(&mut session.transaction, &session.catalog)
                        .await
                        .map(|enabled| json!(enabled))
                }
                ThreadSearchQuery::MessageHits {
                    r#match,
                    substrings,
                    short_spellings,
                    project_id,
                    excluded_thread_ids,
                } => {
                    message_hits(
                        &mut session,
                        r#match,
                        substrings,
                        short_spellings,
                        project_id,
                        excluded_thread_ids,
                    )
                    .await
                }
                ThreadSearchQuery::ThreadOpens { thread_ids } => {
                    thread_opens(&mut session, thread_ids).await
                }
                ThreadSearchQuery::TitleCandidates { project_id } => {
                    title_candidates(&mut session, project_id, None).await
                }
                ThreadSearchQuery::OlderSettledTitles {
                    before,
                    project_ids,
                } => {
                    let projects = project_ids
                        .as_deref()
                        .map(list)
                        .transpose()?
                        .map(|ids| ids.into_iter().collect());
                    title_candidates(&mut session, None, Some((before, projects))).await
                }
                ThreadSearchQuery::OlderSettledIds {
                    before,
                    thread_ids,
                    project_ids,
                } => older_ids(&mut session, before, thread_ids, project_ids).await,
            }
        }
        .await;
        self.finish(id, session, result).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn generated_phrases_keep_quotes_and_ignore_empty_tokens_like_fts5() {
        assert_eq!(
            phrases("\"ab\" \"A\"\"BC\"").unwrap(),
            vec![vec!["a\"b", "\"bc"]]
        );
        assert!(phrases("\"ab\"").unwrap().is_empty());
        assert!(phrases("\"abc\" OR \"def\"").is_err());
        assert!(phrases("\"unterminated").is_err());
    }
    #[test]
    fn position_ranges_preserve_phrase_offsets_without_expanding_repeats() {
        let mut ranges = Vec::new();
        for offset in 0..40000 {
            add_position(&mut ranges, offset).unwrap();
        }
        assert_eq!(ranges, vec![[0, 39999]]);
        assert_eq!(intersect(&ranges, &ranges, 1), vec![[0, 39998]]);
        assert_eq!(
            intersect(
                &vec![[0, 10], [20, 30]],
                &vec![[3, 6], [21, 21], [25, 26]],
                2
            ),
            vec![[1, 4], [23, 24]]
        );
        assert!(validate_positions(&vec![[0, 2], [3, 4]]).is_err());
        assert!(validate_positions(&vec![[4, 3]]).is_err());
    }
}
