//! Native window selection. Only compact indexed metadata participates in the
//! selection and sort; full rows are loaded after the retained cohort is known.

use super::super::read_index::window::{equal_metadata, read_metadata};
use super::*;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ProjectionRecordWindow {
    pub row_limit: usize,
    pub user_turn_limit: Option<usize>,
    pub defer_work: Option<bool>,
    pub anchor_item_id: Option<String>,
    pub required_run_id: Option<String>,
    pub fork_created_at: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DeferredRecord {
    pub row_index: usize,
    pub item_id: String,
    pub last_item_id: String,
    pub last_ordinal: i64,
    pub item_count: usize,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ProjectionWindowQuery {
    ForkBoundary {
        thread_id: String,
        run_id: String,
        anchor_item_id: Option<String>,
    },
    InheritedCount {
        thread_id: String,
        run_id: String,
        window: ProjectionRecordWindow,
    },
    Timeline {
        thread_id: String,
        include_persistent_output: Option<bool>,
    },
    AnchorThread {
        item_id: String,
    },
    PendingCards {
        thread_id: String,
    },
    Materialize {
        sources_json: String,
    },
    AttachmentOwners {
        thread_ids: Vec<String>,
        attachment_id: String,
    },
}

const MAX_RAW_ANCHORS: usize = 152;
const MAX_TURN_ITEMS: usize = 500;
const TURN_SNAPSHOT_ROWS: usize = 502;
const INLINE_WORK_LIMIT: usize = 150;
const NODE_ROOT_CACHE_ENTRIES: usize = 256;
const NODE_ROOT_CACHE_BYTES: usize = 64 * 1024;

/// One retained-window query may visit the same node for many turn items.
/// Keep only successfully validated roots (including missing nodes), and only
/// while the caller owns an immutable read snapshot. Writers still read their
/// current staged overlay and retain the original observations and budgets.
#[derive(Default)]
struct NodeRoots {
    values: BTreeMap<String, Option<String>>,
    bytes: usize,
}

impl NodeRoots {
    async fn read(&mut self, session: &mut Session, id: &str) -> Result<Option<String>> {
        let immutable = session.read_snapshot_validation.is_some();
        if immutable {
            if let Some(root) = self.values.get(id) {
                ensure_ready(session).await?;
                return Ok(root.clone());
            }
        }
        let node = read_metadata(session, NODES, id).await?;
        let root = node
            .as_ref()
            .map(|node| string(node, "root_node_id"))
            .transpose()?;
        let bytes = id
            .len()
            .saturating_add(root.as_ref().map_or(0, String::len));
        if immutable
            && self.values.len() < NODE_ROOT_CACHE_ENTRIES
            && bytes <= NODE_ROOT_CACHE_BYTES.saturating_sub(self.bytes)
        {
            self.values.insert(id.to_owned(), root.clone());
            self.bytes += bytes;
        }
        Ok(root)
    }
}

fn cloned(row: &IndexedRow) -> IndexedRow {
    IndexedRow {
        row: row.row.clone(),
        order: row.order,
    }
}
fn id(row: &IndexedRow, name: &str) -> Result<String> {
    row_id(name, &row.row)
}
fn string(row: &IndexedRow, column: &str) -> Result<String> {
    text(&row.row, column)
}
fn number(row: &IndexedRow, column: &str) -> Result<i64> {
    integer(&row.row, column)
}
fn null_string(row: &IndexedRow, column: &str) -> Result<Option<String>> {
    nullable(&row.row, column)
}
fn json(row: &IndexedRow) -> Result<Value> {
    payload(&row.row)
}
fn status(row: &IndexedRow, statuses: &[&str]) -> Result<bool> {
    Ok(statuses.contains(&string(row, "status")?.as_str()))
}
fn member(row: &IndexedRow, column: &str, set: &BTreeSet<String>) -> Result<bool> {
    Ok(null_string(row, column)?.is_some_and(|value| set.contains(&value)))
}
fn collect(rows: &[IndexedRow], field: &str) -> Result<BTreeSet<String>> {
    rows.iter()
        .map(|row| {
            Ok(json(row)?
                .get(field)
                .and_then(Value::as_str)
                .map(str::to_owned))
        })
        .collect::<Result<Vec<_>>>()
        .map(|values| values.into_iter().flatten().collect())
}
async fn metadata(
    session: &mut Session,
    table: &str,
    column: &str,
    value: &str,
) -> Result<Vec<IndexedRow>> {
    equal_metadata(session, table, column, &text_cell(value)).await
}
fn sort_rows(field: ProjectionRecordField, rows: Vec<IndexedRow>) -> Result<Vec<IndexedRow>> {
    let mut ordered = rows
        .into_iter()
        .map(|row| Ok((order(field, &row.row)?, row)))
        .collect::<Result<Vec<_>>>()?;
    ordered.sort_by(|left, right| {
        left.0
            .cmp(&right.0)
            .then_with(|| left.1.order.cmp(&right.1.order))
    });
    Ok(ordered.into_iter().map(|(_, row)| row).collect())
}

struct Eligible<'a> {
    row: &'a IndexedRow,
    run_status: Option<String>,
    attempt_id: Option<String>,
    attempt_status: Option<String>,
}

struct Retained {
    rows: Vec<IndexedRow>,
    deferred: Vec<DeferredGroup>,
}

struct DeferredGroup {
    run_id: String,
    first_ordinal: i64,
    last_item_id: String,
    last_ordinal: i64,
    item_count: usize,
}

async fn retained(
    session: &mut Session,
    thread: &str,
    window: &ProjectionRecordWindow,
) -> Result<Retained> {
    let items = metadata(session, ITEMS, "thread_id", thread).await?;
    let thread_row = read_metadata(session, THREADS, thread)
        .await?
        .ok_or(Error::IncompatibleStore)?;
    let imported = json(&thread_row)?["historyOrigin"] == "v1_import";
    let mut runs = BTreeMap::new();
    let mut run_items = BTreeMap::new();
    let mut attempts = BTreeMap::new();
    let mut node_roots = NodeRoots::default();
    let own_runs = metadata(session, RUNS, "thread_id", thread).await?;
    for run in &own_runs {
        runs.insert(id(run, RUNS)?, cloned(run));
    }
    for item in &items {
        if let Some(run) = null_string(item, "run_id")? {
            if !runs.contains_key(&run) {
                if let Some(found) = read_metadata(session, RUNS, &run).await? {
                    runs.insert(run.clone(), found);
                }
            }
            if !attempts.contains_key(&run) {
                attempts.insert(
                    run.clone(),
                    metadata(session, ATTEMPTS, "run_id", &run).await?,
                );
            }
            if !run_items.contains_key(&run) {
                run_items.insert(run.clone(), metadata(session, ITEMS, "run_id", &run).await?);
            }
        }
    }
    let anchor = items
        .iter()
        .find(|item| string(item, "turn_item_id").ok().as_ref() == window.anchor_item_id.as_ref());
    let anchor_ordinal = anchor
        .map(|item| number(item, "ordinal"))
        .transpose()?
        .unwrap_or(i64::MAX);
    let anchor_run = anchor
        .and_then(|item| null_string(item, "run_id").ok().flatten())
        .and_then(|run| runs.get(&run));
    let anchor_run_ordinal = anchor_run
        .map(|run| number(run, "ordinal"))
        .transpose()?
        .unwrap_or(i64::MAX);
    let required_ordinal = match window.required_run_id.as_ref().and_then(|id| runs.get(id)) {
        Some(run) if string(run, "thread_id")? == thread => Some(number(run, "ordinal")?),
        _ => None,
    };
    let mut eligible = Vec::new();
    for row in &items {
        if number(row, "ordinal")? > anchor_ordinal {
            continue;
        }
        let run_id = null_string(row, "run_id")?;
        let run = run_id.as_ref().and_then(|id| runs.get(id));
        let run_status = run.map(|run| string(run, "status")).transpose()?;
        let kind = string(row, "type")?;
        let related_attempts = run_id.as_ref().and_then(|id| attempts.get(id));
        if window.required_run_id.is_none() && run_status.as_deref() == Some("rolled_back") {
            continue;
        }
        if kind == "user_message"
            && json(row)?["inputIntent"] == "queued_turn"
            && (matches!(run_status.as_deref(), Some("queued" | "cancelled"))
                || (run_status.as_deref() == Some("rolled_back")
                    && related_attempts.is_some_and(|attempts| {
                        attempts.iter().any(|attempt| {
                            string(attempt, "status").ok().as_deref() == Some("cancelled")
                        })
                    })))
        {
            continue;
        }
        if window.required_run_id.is_some() && !(run_id.is_none() && imported) {
            if !run
                .map(|run| number(run, "ordinal"))
                .transpose()?
                .zip(required_ordinal)
                .is_some_and(|(ordinal, required)| ordinal <= required)
            {
                continue;
            }
            if run_status.as_deref() == Some("rolled_back") {
                let ended = run
                    .map(|run| null_string(run, "completed_at"))
                    .transpose()?
                    .flatten()
                    .and_then(|value| timestamp(&json!(value), 0));
                let fork = window
                    .fork_created_at
                    .as_ref()
                    .and_then(|value| timestamp(&json!(value), 0));
                if ended.zip(fork).is_some_and(|(ended, fork)| ended < fork) {
                    continue;
                }
            }
        }
        if kind == "run_interrupt_result" {
            let node = null_string(row, "node_id")?;
            let superseded = related_attempts.is_some_and(|attempts| {
                attempts.iter().any(|attempt| {
                    string(attempt, "root_node_id").ok() == node
                        && string(attempt, "status").ok().as_deref() == Some("superseded")
                })
            });
            let request = run_id
                .as_ref()
                .and_then(|id| run_items.get(id))
                .is_some_and(|rows| {
                    rows.iter().any(|item| {
                        string(item, "thread_id").ok().as_deref() == Some(thread)
                            && string(item, "type").ok().as_deref() == Some("run_interrupt_request")
                    })
                });
            if superseded && !request {
                continue;
            }
        }
        let mut matched_attempt = None;
        if window.defer_work == Some(true) {
            let node_id = null_string(row, "node_id")?;
            let root = match node_id.as_deref() {
                Some(id) => node_roots.read(session, id).await?,
                None => None,
            }
            .or(node_id);
            if let Some(attempts) = related_attempts {
                for attempt in attempts {
                    if string(attempt, "root_node_id").ok() == root
                        && matched_attempt
                            .as_ref()
                            .is_none_or(|previous: &&IndexedRow| {
                                number(attempt, "attempt_ordinal").unwrap_or(0)
                                    > number(previous, "attempt_ordinal").unwrap_or(0)
                            })
                    {
                        matched_attempt = Some(attempt);
                    }
                }
            }
        }
        eligible.push(Eligible {
            row,
            run_status,
            attempt_id: matched_attempt
                .map(|attempt| string(attempt, "attempt_id"))
                .transpose()?,
            attempt_status: matched_attempt
                .map(|attempt| string(attempt, "status"))
                .transpose()?,
        });
    }
    let eligible_ids = eligible
        .iter()
        .map(|item| id(&item.row, ITEMS))
        .collect::<Result<BTreeSet<_>>>()?;
    let mut anchors = Vec::<(i64, bool)>::new();
    if window.user_turn_limit.is_some() {
        for item in &eligible {
            if string(&item.row, "type")? == "user_message"
                && ["turn_start", "queued_turn"]
                    .contains(&json(&item.row)?["inputIntent"].as_str().unwrap_or(""))
            {
                anchors.push((
                    number(&item.row, "ordinal")?,
                    json(&item.row)?["createdBy"] == "user",
                ));
            }
        }
        anchors.sort_by(|left, right| right.0.cmp(&left.0));
        anchors.truncate(MAX_RAW_ANCHORS);
        let mut starts = Vec::new();
        for run in &own_runs {
            if number(run, "ordinal")? > anchor_run_ordinal {
                continue;
            }
            let Some(rows) = run_items.get(&id(run, RUNS)?) else {
                continue;
            };
            let mut first: Option<&IndexedRow> = None;
            for row in rows {
                if !["user_message", "notification"].contains(&string(row, "type")?.as_str()) {
                    continue;
                }
                if first.is_none_or(|previous| {
                    (number(row, "ordinal").unwrap_or(0), row.order)
                        < (number(previous, "ordinal").unwrap_or(0), previous.order)
                }) {
                    first = Some(row);
                }
            }
            if let Some(first) =
                first.filter(|row| eligible_ids.contains(&id(row, ITEMS).unwrap_or_default()))
            {
                starts.push((
                    number(run, "ordinal")?,
                    number(first, "ordinal")?,
                    string(first, "type")? == "notification",
                ));
            }
        }
        starts.sort_by(|left, right| right.0.cmp(&left.0));
        starts.truncate(MAX_RAW_ANCHORS);
        anchors.extend(
            starts
                .into_iter()
                .filter(|(_, _, notification)| *notification)
                .map(|(_, ordinal, _)| (ordinal, false)),
        );
        anchors.sort_by(|left, right| right.0.cmp(&left.0));
        anchors.truncate(MAX_RAW_ANCHORS);
    }
    let user_limit = window
        .user_turn_limit
        .unwrap_or(0)
        .checked_add(2)
        .ok_or_else(|| invalid("projection user-turn limit overflow"))?;
    let users = anchors
        .iter()
        .filter(|(_, user)| *user)
        .map(|(ordinal, _)| *ordinal)
        .take(user_limit)
        .collect::<Vec<_>>();
    let boundary = if users.len() >= user_limit {
        users.iter().min().copied().unwrap_or(0)
    } else if anchors.len() >= MAX_RAW_ANCHORS {
        anchors
            .iter()
            .map(|(ordinal, _)| *ordinal)
            .min()
            .unwrap_or(0)
    } else {
        0
    };
    let latest_user = users.iter().max().copied();
    let mut blocked = BTreeSet::new();
    for item in &items {
        let kind = string(item, "type")?;
        if ["run_interrupt_request", "run_interrupt_result"].contains(&kind.as_str())
            || (kind == "error"
                && string(item, "status")? == "failed"
                && null_string(item, "parent_item_id")?.is_none())
        {
            if let Some(run) = null_string(item, "run_id")? {
                blocked.insert(run);
            }
        }
    }
    // Window MAX's default RANGE frame includes every peer at the same ordinal.
    let mut user_segments = BTreeMap::<Option<String>, Vec<i64>>::new();
    for item in &eligible {
        if number(&item.row, "ordinal")? >= boundary && string(&item.row, "type")? == "user_message"
        {
            user_segments
                .entry(null_string(&item.row, "run_id")?)
                .or_default()
                .push(number(&item.row, "ordinal")?);
        }
    }
    let mut groups = BTreeMap::<(String, i64, Option<String>), Vec<&Eligible<'_>>>::new();
    if window.defer_work == Some(true) && latest_user.is_some() {
        for item in &eligible {
            let ordinal = number(&item.row, "ordinal")?;
            if ordinal < boundary {
                continue;
            }
            let Some(run) = null_string(&item.row, "run_id")? else {
                continue;
            };
            let segment = user_segments
                .get(&Some(run.clone()))
                .and_then(|ordinals| {
                    ordinals
                        .iter()
                        .copied()
                        .filter(|value| *value <= ordinal)
                        .max()
                })
                .unwrap_or(0);
            let defer = string(&item.row, "status")? == "completed"
                && (item.run_status.as_deref() == Some("completed")
                    || item.attempt_status.as_deref() == Some("superseded"))
                && [
                    "command_execution",
                    "reasoning",
                    "file_change",
                    "file_search",
                    "web_search",
                    "dynamic_tool",
                ]
                .contains(&string(&item.row, "type")?.as_str())
                && json(&item.row)?["nativeInlineWorkOutput"] != true
                && !blocked.contains(&run);
            if defer {
                groups
                    .entry((run, segment, item.attempt_id.clone()))
                    .or_default()
                    .push(item);
            }
        }
    }
    let mut hidden = BTreeSet::new();
    let mut deferred = Vec::new();
    for ((run_id, _, _), rows) in groups
        .into_iter()
        .filter(|(_, rows)| rows.len() > INLINE_WORK_LIMIT)
    {
        let first = rows
            .iter()
            .map(|row| number(&row.row, "ordinal"))
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .min()
            .unwrap();
        let last = rows
            .iter()
            .map(|row| number(&row.row, "ordinal"))
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .max()
            .unwrap();
        let tail = items
            .iter()
            .filter(|row| number(row, "ordinal").ok() == Some(last))
            .map(|row| id(row, ITEMS))
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .max()
            .ok_or(Error::IncompatibleStore)?;
        deferred.push(DeferredGroup {
            run_id,
            first_ordinal: first,
            last_item_id: tail,
            last_ordinal: last,
            item_count: rows.len(),
        });
        for item in &rows {
            if number(&item.row, "ordinal")? != first {
                hidden.insert(id(&item.row, ITEMS)?);
            }
        }
    }
    let mut selected = Vec::new();
    for item in &eligible {
        if number(&item.row, "ordinal")? >= boundary && !hidden.contains(&id(&item.row, ITEMS)?) {
            selected.push(cloned(&item.row));
        }
    }
    let latest_count = latest_user
        .map(|latest| {
            selected
                .iter()
                .filter(|item| number(item, "ordinal").unwrap_or(-1) >= latest)
                .count()
        })
        .unwrap_or(0);
    selected = sort_rows(ProjectionRecordField::TurnItems, selected)?;
    selected.reverse();
    selected.truncate(if window.row_limit == 0 {
        0
    } else if (1..=MAX_TURN_ITEMS).contains(&latest_count) {
        TURN_SNAPSHOT_ROWS
    } else {
        window.row_limit
    });
    let mut retained_ids = selected
        .iter()
        .map(|row| id(row, ITEMS))
        .collect::<Result<BTreeSet<_>>>()?;
    let interrupt_runs = selected
        .iter()
        .filter(|item| string(item, "type").ok().as_deref() == Some("run_interrupt_result"))
        .map(|item| null_string(item, "run_id"))
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .flatten()
        .collect::<BTreeSet<_>>();
    for run in interrupt_runs {
        for item in run_items.get(&run).into_iter().flatten() {
            if string(item, "type")? == "run_interrupt_request"
                && retained_ids.insert(id(item, ITEMS)?)
            {
                selected.push(cloned(item));
            }
        }
    }
    if window.anchor_item_id.is_none() && window.required_run_id.is_none() {
        let mut all = sort_rows(ProjectionRecordField::TurnItems, items)?;
        if let Some(latest) = all.pop() {
            if retained_ids.insert(id(&latest, ITEMS)?) {
                selected.push(latest);
            }
        }
    }
    Ok(Retained {
        rows: sort_rows(ProjectionRecordField::TurnItems, selected)?,
        deferred,
    })
}

pub(super) async fn query(
    session: &mut Session,
    input: ProjectionRecordsQuery,
    mut result: ProjectionRecordResult,
) -> Result<ProjectionRecordResult> {
    let window = input
        .window
        .as_ref()
        .ok_or_else(|| invalid("projection window is required"))?;
    let fields = input.fields.into_iter().collect::<BTreeSet<_>>();
    let retained = if fields.contains(&ProjectionRecordField::TurnItems) {
        retained(session, &input.thread_id, window).await?
    } else {
        Retained {
            rows: vec![],
            deferred: vec![],
        }
    };
    let cohort_runs = retained
        .rows
        .iter()
        .map(|row| null_string(row, "run_id"))
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .flatten()
        .collect::<BTreeSet<_>>();
    let mut cohort_nodes = retained
        .rows
        .iter()
        .map(|row| null_string(row, "node_id"))
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .flatten()
        .collect::<BTreeSet<_>>();
    let cohort_provider_threads = retained
        .rows
        .iter()
        .map(|row| null_string(row, "provider_thread_id"))
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .flatten()
        .collect::<BTreeSet<_>>();
    let cohort_provider_turns = retained
        .rows
        .iter()
        .map(|row| null_string(row, "provider_turn_id"))
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .flatten()
        .collect::<BTreeSet<_>>();
    let cohort_messages = collect(&retained.rows, "messageId")?;
    let cohort_plans = collect(&retained.rows, "planId")?;
    let cohort_checkpoints = collect(&retained.rows, "checkpointId")?;
    let cohort_handoffs = collect(&retained.rows, "contextHandoffId")?;
    let mut tables = BTreeMap::new();
    for table in [RUNS, ATTEMPTS, NODES, SUBAGENTS, TURNS, REQUESTS, PLANS] {
        tables.insert(
            table,
            metadata(session, table, "thread_id", &input.thread_id).await?,
        );
    }
    if !fields.iter().all(|field| {
        matches!(
            field,
            ProjectionRecordField::Runs
                | ProjectionRecordField::Attempts
                | ProjectionRecordField::TurnItems
        )
    }) {
        for node in &tables[NODES] {
            let settled_todo = string(node, "kind")? == "todo_list"
                && null_string(node, "run_id")?.is_some()
                && !tables[RUNS].iter().any(|run| {
                    id(run, RUNS).ok().as_ref()
                        == null_string(node, "run_id").ok().flatten().as_ref()
                        && status(run, &ACTIVE_RUNS).unwrap_or(false)
                });
            if status(node, &["pending", "starting", "running", "waiting"])? && !settled_todo {
                cohort_nodes.insert(id(node, NODES)?);
            }
        }
        for attempt in &tables[ATTEMPTS] {
            if status(attempt, &["pending", "starting", "running", "waiting"])?
                || member(attempt, "run_id", &cohort_runs)?
                || null_string(attempt, "run_id")? == window.required_run_id
            {
                cohort_nodes.insert(string(attempt, "root_node_id")?);
            }
        }
        for run in &tables[RUNS] {
            if status(run, &ACTIVE_RUNS)?
                || cohort_runs.contains(&id(run, RUNS)?)
                || Some(id(run, RUNS)?) == window.required_run_id
            {
                if let Some(root) = json(run)?["rootNodeId"].as_str() {
                    cohort_nodes.insert(root.to_owned());
                }
            }
        }
        for request in &tables[REQUESTS] {
            if status(request, &["pending", "waiting"])? {
                cohort_nodes.insert(string(request, "node_id")?);
            }
        }
        for subagent in &tables[SUBAGENTS] {
            if status(subagent, &["pending", "starting", "running", "waiting"])? {
                cohort_nodes.insert(string(subagent, "parent_node_id")?);
            }
        }
        for turn in &tables[TURNS] {
            if status(turn, &["starting", "running", "waiting"])? {
                cohort_nodes.insert(string(turn, "node_id")?);
            }
        }
        let parents = tables[NODES]
            .iter()
            .map(|node| Ok((id(node, NODES)?, null_string(node, "parent_node_id")?)))
            .collect::<Result<BTreeMap<_, _>>>()?;
        let mut pending = cohort_nodes.iter().cloned().collect::<Vec<_>>();
        while let Some(node) = pending.pop() {
            if let Some(Some(parent)) = parents.get(&node) {
                if cohort_nodes.insert(parent.clone()) {
                    pending.push(parent.clone());
                }
            }
        }
    }
    // The newest run owns the todo list. Within that run a turn-item ordinal,
    // not its random plan UUID, identifies a replacement artifact.
    let mut latest_todo: Option<(String, String, Option<String>, String)> = None;
    if fields.iter().any(|field| {
        matches!(
            field,
            ProjectionRecordField::Plans
                | ProjectionRecordField::Runs
                | ProjectionRecordField::ProviderThreads
        )
    }) {
        let run_map = tables[RUNS]
            .iter()
            .map(|run| Ok((id(run, RUNS)?, run)))
            .collect::<Result<BTreeMap<_, _>>>()?;
        let mut owners = Vec::new();
        for plan in &tables[PLANS] {
            if string(plan, "kind")? != "todo_list" || string(plan, "status")? == "superseded" {
                continue;
            }
            if let Some(run) = null_string(plan, "run_id")?
                .as_ref()
                .and_then(|id| run_map.get(id))
            {
                owners.push((
                    number(run, "ordinal")?,
                    id(plan, PLANS)?,
                    id(run, RUNS)?,
                    null_string(run, "provider_thread_id")?,
                ));
            }
        }
        owners.sort_by(|left, right| right.0.cmp(&left.0).then_with(|| right.1.cmp(&left.1)));
        if let Some((_, mut plan, run, provider)) = owners.into_iter().next() {
            let alternatives = tables[PLANS]
                .iter()
                .filter(|candidate| {
                    null_string(candidate, "run_id").ok().flatten().as_ref() == Some(&run)
                        && string(candidate, "kind").ok().as_deref() == Some("todo_list")
                        && string(candidate, "status").ok().as_deref() != Some("superseded")
                })
                .map(|candidate| id(candidate, PLANS))
                .collect::<Result<BTreeSet<_>>>()?;
            if alternatives.len() > 1 {
                let mut replacements = Vec::new();
                for item in metadata(session, ITEMS, "run_id", &run).await? {
                    if string(&item, "thread_id")? != input.thread_id
                        || string(&item, "type")? != "todo_list"
                    {
                        continue;
                    }
                    if let Some(candidate) = json(&item)?["planId"]
                        .as_str()
                        .filter(|candidate| alternatives.contains(*candidate))
                    {
                        replacements.push((
                            number(&item, "ordinal")?,
                            item.order,
                            candidate.to_owned(),
                        ));
                    }
                }
                // The original run/ordinal index is scanned backwards for
                // this ORDER BY, including its hidden insertion-order key.
                replacements
                    .sort_by(|left, right| right.0.cmp(&left.0).then_with(|| right.1.cmp(&left.1)));
                if let Some((_, _, selected)) = replacements.into_iter().next() {
                    plan = selected;
                }
            }
            let last = tables[PLANS]
                .iter()
                .filter(|candidate| {
                    null_string(candidate, "run_id").ok().flatten().as_ref() == Some(&run)
                })
                .map(|candidate| id(candidate, PLANS))
                .collect::<Result<Vec<_>>>()?
                .into_iter()
                .max()
                .ok_or(Error::IncompatibleStore)?;
            latest_todo = Some((plan, run, provider, last));
        }
    }
    let active_runs = tables[RUNS]
        .iter()
        .filter(|run| status(run, &ACTIVE_RUNS).unwrap_or(false))
        .map(|run| id(run, RUNS))
        .collect::<Result<BTreeSet<_>>>()?;
    let latest_started = if latest_todo.is_some() {
        let mut candidates = tables[RUNS]
            .iter()
            .filter(|run| {
                null_string(run, "provider_thread_id")
                    .ok()
                    .flatten()
                    .is_some()
                    && json(run)
                        .ok()
                        .is_some_and(|payload| !payload["startedAt"].is_null())
            })
            .map(|run| Ok((number(run, "ordinal")?, id(run, RUNS)?)))
            .collect::<Result<Vec<_>>>()?;
        candidates.sort();
        candidates.pop().map(|(_, id)| id)
    } else {
        None
    };
    let presented_run = if window.anchor_item_id.is_none()
        && window.required_run_id.is_none()
        && fields.contains(&ProjectionRecordField::Runs)
    {
        super::super::shell::presented_run_id(session, &input.thread_id).await?
    } else {
        None
    };
    for field in fields {
        let mut rows = match field {
            ProjectionRecordField::TurnItems => retained.rows.iter().map(cloned).collect(),
            ProjectionRecordField::ProviderSessions => {
                let mut rows = Vec::new();
                for binding in metadata(session, BINDINGS, "thread_id", &input.thread_id).await? {
                    if let Some(session_row) =
                        read_metadata(session, SESSIONS, &string(&binding, "provider_session_id")?)
                            .await?
                    {
                        let mut selected =
                            status(&session_row, &["starting", "running", "waiting"])?;
                        if !selected {
                            for thread in metadata(
                                session,
                                PROVIDER_THREADS,
                                "provider_session_id",
                                &id(&session_row, SESSIONS)?,
                            )
                            .await?
                            {
                                if cohort_provider_threads.contains(&id(&thread, PROVIDER_THREADS)?)
                                {
                                    selected = true;
                                    break;
                                }
                            }
                        }
                        if selected {
                            rows.push(session_row);
                        }
                    }
                }
                unique_rows(SESSIONS, rows)?
            }
            ProjectionRecordField::ProviderThreads => {
                let mut rows = metadata(session, PROVIDER_THREADS, "thread_id", &input.thread_id)
                    .await?
                    .into_iter()
                    .filter(|thread| {
                        status(thread, &["active"]).unwrap_or(false)
                            || latest_todo
                                .as_ref()
                                .and_then(|(_, _, provider, _)| provider.as_ref())
                                == id(thread, PROVIDER_THREADS).ok().as_ref()
                    })
                    .collect::<Vec<_>>();
                for id in &cohort_provider_threads {
                    if let Some(row) = read_metadata(session, PROVIDER_THREADS, id).await? {
                        rows.push(row);
                    }
                }
                for node in &cohort_nodes {
                    rows.extend(metadata(session, PROVIDER_THREADS, "owner_node_id", node).await?);
                }
                unique_rows(PROVIDER_THREADS, rows)?
            }
            ProjectionRecordField::ContextTransfers => {
                let mut rows =
                    metadata(session, TRANSFERS, "source_thread_id", &input.thread_id).await?;
                rows.extend(
                    metadata(session, TRANSFERS, "target_thread_id", &input.thread_id).await?,
                );
                unique_rows(TRANSFERS, rows)?
            }
            _ => match tables.get(field.table()) {
                Some(rows) => rows.iter().map(cloned).collect(),
                None => metadata(session, field.table(), "thread_id", &input.thread_id).await?,
            },
        };
        let mut selected = Vec::new();
        for row in rows.drain(..) {
            let keep = match field {
                ProjectionRecordField::Runs => {
                    status(&row, &ACTIVE_RUNS)?
                        || presented_run.as_ref() == Some(&id(&row, RUNS)?)
                        || cohort_runs.contains(&id(&row, RUNS)?)
                        || Some(id(&row, RUNS)?) == window.required_run_id
                        || latest_started.as_ref() == Some(&id(&row, RUNS)?)
                        || latest_todo
                            .as_ref()
                            .is_some_and(|(_, run, _, _)| id(&row, RUNS).ok().as_ref() == Some(run))
                }
                ProjectionRecordField::Attempts => {
                    status(&row, &["pending", "starting", "running", "waiting"])?
                        || member(&row, "run_id", &cohort_runs)?
                }
                ProjectionRecordField::Nodes => cohort_nodes.contains(&id(&row, NODES)?),
                ProjectionRecordField::Subagents => {
                    status(&row, &["pending", "starting", "running", "waiting"])?
                        || member(&row, "run_id", &cohort_runs)?
                        || member(&row, "parent_node_id", &cohort_nodes)?
                }
                ProjectionRecordField::ProviderTurns => {
                    status(&row, &["starting", "running", "waiting"])?
                        || cohort_provider_turns.contains(&id(&row, TURNS)?)
                        || member(&row, "node_id", &cohort_nodes)?
                }
                ProjectionRecordField::RuntimeRequests => {
                    status(&row, &["pending", "waiting"])?
                        || member(&row, "node_id", &cohort_nodes)?
                        || member(&row, "provider_turn_id", &cohort_provider_turns)?
                }
                ProjectionRecordField::Messages => {
                    cohort_messages.contains(&id(&row, MESSAGES)?)
                        || member(&row, "run_id", &active_runs)?
                }
                ProjectionRecordField::Plans => {
                    status(&row, &["active"])?
                        || cohort_plans.contains(&id(&row, PLANS)?)
                        || latest_todo.as_ref().is_some_and(|(plan, _, _, _)| {
                            id(&row, PLANS).ok().as_ref() == Some(plan)
                        })
                }
                ProjectionRecordField::CheckpointScopes => {
                    member(&row, "run_id", &cohort_runs)? || member(&row, "node_id", &cohort_nodes)?
                }
                ProjectionRecordField::Checkpoints => {
                    status(&row, &["pending", "capturing"])?
                        || (null_string(&row, "run_id")?.is_none()
                            && number(&row, "ordinal_within_scope")? == 0
                            && status(&row, &["ready"])?)
                        || cohort_checkpoints.contains(&id(&row, CHECKPOINTS)?)
                        || member(&row, "run_id", &cohort_runs)?
                        || member(&row, "node_id", &cohort_nodes)?
                }
                ProjectionRecordField::ContextHandoffs => {
                    status(&row, &["pending", "ready"])?
                        || cohort_handoffs.contains(&id(&row, HANDOFFS)?)
                }
                ProjectionRecordField::ContextTransfers => {
                    status(&row, &["pending", "running", "waiting"])?
                        || member(&row, "target_run_id", &cohort_runs)?
                        || json(&row)?["contextHandoffId"]
                            .as_str()
                            .is_some_and(|id| cohort_handoffs.contains(id))
                }
                _ => true,
            };
            if keep {
                selected.push(row);
            }
        }
        selected = sort_rows(field, selected)?;
        if field == ProjectionRecordField::Plans {
            if let Some((plan, _, _, last)) = &latest_todo {
                selected.sort_by_key(|row| {
                    let id = id(row, PLANS).unwrap_or_default();
                    let is_latest = id == *plan;
                    (if is_latest { last.clone() } else { id }, is_latest)
                });
            }
        }
        let mut bodies = Vec::with_capacity(selected.len());
        for selected in selected {
            let source = read_row(
                &mut session.transaction,
                &session.catalog,
                field.table(),
                &[text_cell(&id(&selected, field.table())?)],
            )
            .await?
            .ok_or(Error::IncompatibleStore)?;
            let body = text(&source.row, "payload_json")?;
            let groups = if field == ProjectionRecordField::TurnItems {
                let run = null_string(&selected, "run_id")?;
                let ordinal = number(&selected, "ordinal")?;
                retained
                    .deferred
                    .iter()
                    .filter(|group| {
                        run.as_ref() == Some(&group.run_id) && ordinal == group.first_ordinal
                    })
                    .collect::<Vec<_>>()
            } else {
                vec![]
            };
            if groups.is_empty() {
                bodies.push(body);
            } else {
                // Upstream attaches cohort metadata by run and first ordinal,
                // rather than item identity or attempt. Preserve each joined
                // row when distinct attempts begin at the same ordinal.
                for group in groups {
                    result.deferred_work.push(DeferredRecord {
                        row_index: bodies.len(),
                        item_id: id(&selected, ITEMS)?,
                        last_item_id: group.last_item_id.clone(),
                        last_ordinal: group.last_ordinal,
                        item_count: group.item_count,
                    });
                    bodies.push(body.clone());
                }
            }
        }
        result.rows.insert(field, bodies);
    }
    Ok(result)
}

async fn history_query(session: &mut Session, query: ProjectionWindowQuery) -> Result<Value> {
    ensure_ready(session).await?;
    match query {
        ProjectionWindowQuery::Materialize { sources_json } => {
            #[derive(Deserialize)]
            #[serde(rename_all = "camelCase", deny_unknown_fields)]
            struct Source {
                thread_id: String,
                id: String,
            }
            let sources: Vec<Source> = serde_json::from_str(&sources_json)
                .map_err(|_| invalid("invalid projection timeline sources"))?;
            let mut result = Vec::new();
            for source in sources {
                if let Some(row) = read_row(
                    &mut session.transaction,
                    &session.catalog,
                    ITEMS,
                    &[text_cell(&source.id)],
                )
                .await?
                {
                    if text(&row.row, "thread_id")? == source.thread_id {
                        result.push(json!({"thread_id": source.thread_id, "turn_item_id": source.id, "payload_json": text(&row.row, "payload_json")?}));
                    }
                }
            }
            Ok(json!(result))
        }
        ProjectionWindowQuery::AttachmentOwners {
            thread_ids,
            attachment_id,
        } => {
            let mut result = Vec::new();
            for thread in thread_ids.into_iter().collect::<BTreeSet<_>>() {
                for item in metadata(session, ITEMS, "thread_id", &thread).await? {
                    if string(&item, "type")? == "user_message"
                        && json(&item)?["nativeAttachmentIds"]
                            .as_array()
                            .is_some_and(|ids| {
                                ids.iter().any(|id| id.as_str() == Some(&attachment_id))
                            })
                    {
                        result
                            .push(json!({"thread_id": thread, "turn_item_id": id(&item, ITEMS)?}));
                    }
                }
            }
            Ok(json!(result))
        }
        ProjectionWindowQuery::AnchorThread { item_id } => Ok(
            json!({"thread_id": read_metadata(session, ITEMS, &item_id).await?
            .as_ref().map(|item| string(item, "thread_id")).transpose()?}),
        ),
        ProjectionWindowQuery::PendingCards { thread_id } => {
            let mut result = Vec::new();
            for request in metadata(session, REQUESTS, "thread_id", &thread_id).await? {
                if !status(&request, &["pending"])? {
                    continue;
                }
                for item in
                    metadata(session, ITEMS, "node_id", &string(&request, "node_id")?).await?
                {
                    if string(&item, "thread_id")? == thread_id
                        && ["approval_request", "user_input_request"]
                            .contains(&string(&item, "type")?.as_str())
                        && json(&item)?["requestId"].as_str()
                            == Some(id(&request, REQUESTS)?.as_str())
                    {
                        let row = read_row(
                            &mut session.transaction,
                            &session.catalog,
                            ITEMS,
                            &[text_cell(&id(&item, ITEMS)?)],
                        )
                        .await?
                        .ok_or(Error::IncompatibleStore)?;
                        result.push(text(&row.row, "payload_json")?);
                    }
                }
            }
            Ok(json!(result))
        }
        ProjectionWindowQuery::ForkBoundary {
            thread_id,
            run_id,
            anchor_item_id,
        } => {
            let runs = metadata(session, RUNS, "thread_id", &thread_id).await?;
            let items = metadata(session, ITEMS, "thread_id", &thread_id).await?;
            let run = runs
                .iter()
                .find(|run| id(run, RUNS).ok().as_ref() == Some(&run_id));
            let ordinal = run.map(|run| number(run, "ordinal")).transpose()?;
            let mut eligible_runs = runs
                .iter()
                .filter(|run| {
                    number(run, "ordinal")
                        .ok()
                        .zip(ordinal)
                        .is_some_and(|(value, limit)| value <= limit)
                })
                .map(|run| Ok((number(run, "ordinal")?, id(run, RUNS)?)))
                .collect::<Result<Vec<_>>>()?;
            eligible_runs.sort_by(|left, right| right.0.cmp(&left.0));
            let mut boundary = None;
            for (_, id) in eligible_runs {
                if let Some(maximum) = metadata(session, ITEMS, "run_id", &id)
                    .await?
                    .iter()
                    .map(|row| number(row, "ordinal"))
                    .collect::<Result<Vec<_>>>()?
                    .into_iter()
                    .max()
                {
                    boundary = Some(maximum);
                    break;
                }
            }
            if boundary.is_none() {
                boundary = items
                    .iter()
                    .filter(|row| {
                        null_string(row, "run_id").ok().flatten().is_none()
                            && json(row)
                                .ok()
                                .is_some_and(|value| value["historyOrigin"] == "v1_import")
                    })
                    .map(|row| number(row, "ordinal"))
                    .collect::<Result<Vec<_>>>()?
                    .into_iter()
                    .max();
            }
            let boundary = boundary.unwrap_or(-1);
            let anchor = items
                .iter()
                .find(|row| id(row, ITEMS).ok().as_ref() == anchor_item_id.as_ref())
                .map(|row| number(row, "ordinal"))
                .transpose()?
                .unwrap_or(boundary);
            let limit = boundary.min(anchor);
            let mut eligible = Vec::new();
            for item in items {
                if number(&item, "ordinal")? <= limit {
                    eligible.push(item);
                }
            }
            let last = sort_rows(ProjectionRecordField::TurnItems, eligible)?.pop();
            Ok(json!({"anchor": last.as_ref().map(|row| id(row, ITEMS)).transpose()?}))
        }
        ProjectionWindowQuery::InheritedCount {
            thread_id,
            run_id,
            window,
        } => {
            let Some(thread) = read_metadata(session, THREADS, &thread_id).await? else {
                return Ok(json!({"count": null}));
            };
            if json(&thread)?["forkedFrom"]["type"] == "run" {
                return Ok(json!({"count": null}));
            }
            let exists = read_metadata(session, RUNS, &run_id)
                .await?
                .as_ref()
                .map(|run| string(run, "thread_id"))
                .transpose()?
                .as_deref()
                == Some(&thread_id);
            Ok(
                json!({"count": if exists { retained(session, &thread_id, &window).await?.rows.len() } else { 0 }}),
            )
        }
        ProjectionWindowQuery::Timeline {
            thread_id,
            include_persistent_output,
        } => {
            let rows = sort_rows(
                ProjectionRecordField::TurnItems,
                metadata(session, ITEMS, "thread_id", &thread_id).await?,
            )?;
            let mut result = Vec::with_capacity(rows.len());
            let mut attempts = BTreeMap::new();
            for row in rows {
                let kind = string(&row, "type")?;
                let payload = json(&row)?;
                let mut attempt: Option<&IndexedRow> = None;
                if include_persistent_output == Some(true) {
                    let node_id = null_string(&row, "node_id")?;
                    let node = match node_id.as_deref() {
                        Some(id) => read_metadata(session, NODES, id).await?,
                        None => None,
                    };
                    let root = node
                        .as_ref()
                        .map(|node| string(node, "root_node_id"))
                        .transpose()?
                        .or(node_id);
                    if let Some(run_id) = null_string(&row, "run_id")? {
                        if !attempts.contains_key(&run_id) {
                            attempts.insert(
                                run_id.clone(),
                                metadata(session, ATTEMPTS, "run_id", &run_id).await?,
                            );
                        }
                        for candidate in &attempts[&run_id] {
                            if string(candidate, "root_node_id").ok() == root
                                && attempt.is_none_or(|previous| {
                                    number(candidate, "attempt_ordinal").unwrap_or(0)
                                        > number(previous, "attempt_ordinal").unwrap_or(0)
                                })
                            {
                                attempt = Some(candidate);
                            }
                        }
                    }
                }
                result.push(json!({
                    "turn_item_id": id(&row, ITEMS)?, "run_id": null_string(&row, "run_id")?,
                    "node_id": null_string(&row, "node_id")?, "parent_item_id": null_string(&row, "parent_item_id")?,
                    "type": kind, "status": string(&row, "status")?, "ordinal": number(&row, "ordinal")?,
                    "attempt_id": attempt.map(|row| id(row, ATTEMPTS)).transpose()?,
                    "attempt_status": attempt.map(|row| string(row, "status")).transpose()?,
                    "persistent_output": i64::from(include_persistent_output == Some(true) && payload["nativeInlineWorkOutput"] == true),
                    "input_intent": if kind == "user_message" { payload.get("inputIntent").cloned().unwrap_or(Value::Null) } else { Value::Null },
                    "created_by": if kind == "user_message" { payload.get("createdBy").cloned().unwrap_or(Value::Null) } else { Value::Null },
                    "message_id": if ["user_message", "assistant_message"].contains(&kind.as_str()) { payload.get("messageId").cloned().unwrap_or(Value::Null) } else { Value::Null },
                }));
            }
            Ok(json!(result))
        }
    }
}

impl ApplicationTransactions {
    pub async fn projection_window_query(
        &mut self,
        id: &str,
        query: ProjectionWindowQuery,
    ) -> Result<Value> {
        let mut session = self.take(id)?;
        let result = history_query(&mut session, query).await;
        self.finish(id, session, result).await
    }
}

#[cfg(test)]
mod node_root_reuse_tests {
    use super::*;
    use crate::application::ApplicationTransactionMode;
    use crate::test_support::TestFixture;

    const NOW: &str = "2026-11-10T12:34:56.123Z";

    async fn fixture() -> (
        TestFixture,
        Store,
        ApplicationTransactions,
        ApplicationCatalog,
    ) {
        let fixture = TestFixture::start().await;
        let store = fixture.initialize().await;
        store.bootstrap_application().await.unwrap();
        while !store
            .prepare_application_projection_reads()
            .await
            .unwrap()
            .prepared
        {}
        let catalog =
            decode_catalog(&store.db.read(&schema_key()).await.unwrap().unwrap()).unwrap();
        let application = ApplicationTransactions::new(store.clone());
        (fixture, store, application, catalog)
    }

    // Address the compact witness itself so corruption tests do not have to
    // weaken canonical projection writes or their ownership checks.
    fn node_key(catalog: &ApplicationCatalog, id: &str) -> Vec<u8> {
        store_key(&format!(
            "application/generation/{}/projection-reads/v1/window-meta/{}/{}",
            catalog.generation,
            NODES,
            request_digest(&json!([text_cell(id)])),
        ))
    }

    fn node_metadata(id: &str, root: &str) -> Value {
        json!({
            "table": NODES,
            "primary": [text_cell(id)],
            "order": { "epoch": 1, "ordinal": 1 },
            "row": {
                "node_id": text_cell(id),
                "thread_id": text_cell("foreign-thread"),
                "root_node_id": text_cell(root),
            },
        })
    }

    async fn replace(store: &Store, key: &[u8], value: Option<Vec<u8>>) {
        let mut transaction = store.db.begin().await.unwrap();
        match value {
            Some(value) => transaction.put(key.to_vec(), value).await.unwrap(),
            None => transaction.delete(key.to_vec()).await.unwrap(),
        }
        transaction.commit().await.unwrap();
    }

    async fn read_session(application: &mut ApplicationTransactions, id: &str) -> Session {
        application
            .begin_mode(id, ApplicationTransactionMode::Read, None)
            .await
            .unwrap();
        application.take(id).unwrap()
    }

    #[tokio::test]
    async fn shared_foreign_and_missing_node_roots_remain_correct_beyond_the_entry_cap() {
        let (_fixture, store, mut application, catalog) = fixture().await;
        let mut seed = store.db.begin().await.unwrap();
        seed.put(
            node_key(&catalog, "shared"),
            encode(&node_metadata("shared", "foreign-root")).unwrap(),
        )
        .await
        .unwrap();
        for index in 0..=NODE_ROOT_CACHE_ENTRIES {
            let id = format!("node-{index}");
            seed.put(
                node_key(&catalog, &id),
                encode(&node_metadata(&id, &format!("root-{index}"))).unwrap(),
            )
            .await
            .unwrap();
        }
        seed.commit().await.unwrap();
        let mut session = read_session(&mut application, "read").await;
        let mut roots = NodeRoots::default();
        for _ in 0..1_500 {
            assert_eq!(
                roots.read(&mut session, "shared").await.unwrap().as_deref(),
                Some("foreign-root")
            );
        }
        for _ in 0..2 {
            assert_eq!(roots.read(&mut session, "missing").await.unwrap(), None);
        }
        for index in 0..=NODE_ROOT_CACHE_ENTRIES {
            let id = format!("node-{index}");
            assert_eq!(
                roots.read(&mut session, &id).await.unwrap(),
                Some(format!("root-{index}"))
            );
        }
        assert_eq!(roots.values.len(), NODE_ROOT_CACHE_ENTRIES);
        assert!(roots.bytes <= NODE_ROOT_CACHE_BYTES);
        assert!(
            !roots
                .values
                .contains_key(&format!("node-{NODE_ROOT_CACHE_ENTRIES}"))
        );
        assert_eq!(
            roots
                .read(&mut session, &format!("node-{NODE_ROOT_CACHE_ENTRIES}"))
                .await
                .unwrap(),
            Some(format!("root-{NODE_ROOT_CACHE_ENTRIES}"))
        );
        session.transaction.rollback().await.unwrap();
    }

    #[tokio::test]
    async fn roots_beyond_the_byte_cap_are_read_without_being_retained() {
        let (_fixture, store, mut application, catalog) = fixture().await;
        let root = "r".repeat(NODE_ROOT_CACHE_BYTES / 2 - 16);
        let large = "x".repeat(NODE_ROOT_CACHE_BYTES + 1);
        for (id, value) in [
            ("first", &root),
            ("second", &root),
            ("third", &root),
            ("large", &large),
        ] {
            replace(
                &store,
                &node_key(&catalog, id),
                Some(encode(&node_metadata(id, value)).unwrap()),
            )
            .await;
        }
        let mut session = read_session(&mut application, "read").await;
        let mut roots = NodeRoots::default();
        for id in ["first", "second", "third", "third"] {
            assert_eq!(
                roots.read(&mut session, id).await.unwrap().as_deref(),
                Some(root.as_str())
            );
        }
        for _ in 0..2 {
            assert_eq!(
                roots.read(&mut session, "large").await.unwrap().as_deref(),
                Some(large.as_str())
            );
        }
        assert_eq!(roots.values.len(), 2);
        assert!(!roots.values.contains_key("third"));
        assert!(!roots.values.contains_key("large"));
        assert!(roots.bytes <= NODE_ROOT_CACHE_BYTES);
        session.transaction.rollback().await.unwrap();
    }

    async fn seed_items(application: &mut ApplicationTransactions) {
        let mut events = vec![(
            "thread.created",
            json!({
                "id":"thread", "projectId":"project", "title":"Thread", "providerInstanceId":"instance",
                "runtimeMode":"full-access", "interactionMode":"default", "activeProviderThreadId":null,
                "createdAt":NOW, "updatedAt":NOW, "archivedAt":null, "deletedAt":null,
                "lineage":{"relationshipToParent":"root"},
            }),
        )];
        for (id, ordinal, node) in [("first", 1, "good"), ("second", 2, "bad")] {
            events.push(("turn-item.updated", json!({
                "id":id, "threadId":"thread", "runId":null, "nodeId":node,
                "providerThreadId":null, "providerTurnId":null, "parentItemId":null,
                "ordinal":ordinal, "type":"reasoning", "status":"completed", "text":id, "updatedAt":NOW,
            })));
        }
        apply_events(application, events).await;
    }

    async fn apply_events(application: &mut ApplicationTransactions, events: Vec<(&str, Value)>) {
        application.begin("seed-items").await.unwrap();
        for (kind, payload) in events {
            let mut event = ProjectionApply {
                event_type: kind.into(),
                thread_id: "thread".into(),
                occurred_at: NOW.into(),
                payload_json: payload.to_string(),
                normalization: None,
            };
            if let Some(context) = application
                .projections_normalization("seed-items", event.clone())
                .await
                .unwrap()
            {
                event.normalization = Some(ProjectionNormalizedPayload {
                    expected_payload_json: context.payload_json.clone(),
                    payload_json: context.payload_json,
                });
            }
            application
                .projections_apply("seed-items", event)
                .await
                .unwrap();
        }
        application.commit("seed-items").await.unwrap();
    }

    #[tokio::test]
    async fn records_reuse_missing_and_foreign_roots_and_reject_queries_after_closure() {
        let (_fixture, store, mut application, catalog) = fixture().await;
        seed_items(&mut application).await;
        let mut events = Vec::new();
        for (index, node, root) in [(1, "missing", "missing"), (2, "foreign", "foreign-root")] {
            let run = format!("run-{index}");
            let attempt = format!("attempt-{index}");
            events.push((
                "run.created",
                json!({
                    "id":run, "threadId":"thread", "ordinal":index, "providerInstanceId":"instance",
                    "providerThreadId":null, "status":"running", "activeAttemptId":attempt,
                    "requestedAt":NOW, "completedAt":null,
                }),
            ));
            events.push((
                "run-attempt.created",
                json!({
                    "id":attempt, "runId":run, "attemptOrdinal":1, "rootNodeId":root,
                    "providerInstanceId":"instance", "providerThreadId":format!("provider-{index}"), "providerTurnId":null,
                    "status":"superseded", "completedAt":NOW,
                }),
            ));
            let message = format!("message-{index}");
            events.push(("message.updated", json!({
                "id":message, "threadId":"thread", "runId":run, "nodeId":node,
                "role":"user", "text":"Fixture input", "createdBy":"user", "creationSource":"web",
                "streaming":false, "createdAt":NOW, "updatedAt":NOW,
            })));
            for offset in 0..=INLINE_WORK_LIMIT + 1 {
                let mut item = json!({
                    "id":format!("item-{index}-{offset}"), "threadId":"thread", "runId":run, "nodeId":node,
                    "providerThreadId":null, "providerTurnId":null, "parentItemId":null,
                    "ordinal":index * 1000 + offset, "type":"reasoning", "status":"completed",
                    "text":"Completed fixture work", "updatedAt":NOW,
                });
                if offset == 0 {
                    item["type"] = json!("user_message");
                    item["inputIntent"] = json!("turn_start");
                    item["createdBy"] = json!("user");
                    item["creationSource"] = json!("web");
                    item["messageId"] = json!(message);
                    item["attachments"] = json!([]);
                }
                events.push(("turn-item.updated", item));
            }
        }
        apply_events(&mut application, events).await;
        replace(
            &store,
            &node_key(&catalog, "foreign"),
            Some(encode(&node_metadata("foreign", "foreign-root")).unwrap()),
        )
        .await;
        let request = ProjectionRecordsQuery {
            thread_id: "thread".into(),
            fields: vec![ProjectionRecordField::TurnItems],
            filter: None,
            window: Some(ProjectionRecordWindow {
                row_limit: 75,
                user_turn_limit: Some(10),
                defer_work: Some(true),
                anchor_item_id: None,
                required_run_id: None,
                fork_created_at: None,
            }),
        };
        for commit in [false, true] {
            application
                .begin_mode("public", ApplicationTransactionMode::Read, None)
                .await
                .unwrap();
            let result = application
                .projection_records("public", request.clone())
                .await
                .unwrap();
            // A running run folds completed work here only if its superseded
            // attempt was matched through the missing-node fallback or the
            // foreign node's root. Each run revisits that node more than 150 times.
            assert_eq!(result.deferred_work.len(), 2);
            assert!(
                result
                    .deferred_work
                    .iter()
                    .all(|group| group.item_count == INLINE_WORK_LIMIT + 1)
            );
            if commit {
                application.commit("public").await.unwrap();
            } else {
                application.rollback("public").await.unwrap();
            }
            assert!(
                matches!(application.projection_records("public", request.clone()).await,
                Err(Error::Invalid(message)) if message == "application transaction does not exist")
            );
        }
    }

    #[tokio::test]
    async fn retained_window_does_not_read_ineligible_nodes_or_cache_first_use_corruption() {
        let (_fixture, store, mut application, catalog) = fixture().await;
        seed_items(&mut application).await;
        replace(
            &store,
            &node_key(&catalog, "good"),
            Some(encode(&node_metadata("good", "foreign-root")).unwrap()),
        )
        .await;
        let anchored = ProjectionRecordWindow {
            row_limit: 75,
            user_turn_limit: None,
            defer_work: Some(true),
            anchor_item_id: Some("first".into()),
            required_run_id: None,
            fork_created_at: None,
        };
        for corrupt in 0..3 {
            let mut value = node_metadata("bad", "root");
            match corrupt {
                0 => value["table"] = json!(THREADS),
                1 => value["primary"] = json!([text_cell("another-node")]),
                _ => value["row"]["root_node_id"] = int_cell(7),
            }
            replace(
                &store,
                &node_key(&catalog, "bad"),
                Some(encode(&value).unwrap()),
            )
            .await;
            let mut session = read_session(&mut application, "read").await;
            let selected = retained(&mut session, "thread", &anchored).await.unwrap();
            assert_eq!(selected.rows.len(), 1);
            assert_eq!(id(&selected.rows[0], ITEMS).unwrap(), "first");
            assert!(matches!(
                retained(
                    &mut session,
                    "thread",
                    &ProjectionRecordWindow {
                        anchor_item_id: None,
                        ..anchored.clone()
                    }
                )
                .await,
                Err(Error::IncompatibleStore)
            ));
            let mut roots = NodeRoots::default();
            assert!(matches!(
                roots.read(&mut session, "bad").await,
                Err(Error::IncompatibleStore)
            ));
            assert!(roots.values.is_empty());
            session.transaction.rollback().await.unwrap();
        }
    }

    #[tokio::test]
    async fn node_reuse_stays_with_one_snapshot_across_updates_deletion_and_corruption() {
        let (_fixture, store, mut application, catalog) = fixture().await;
        let key = node_key(&catalog, "node");
        replace(
            &store,
            &key,
            Some(encode(&node_metadata("node", "old-root")).unwrap()),
        )
        .await;
        let mut old = read_session(&mut application, "old").await;
        let mut old_roots = NodeRoots::default();
        assert_eq!(
            old_roots.read(&mut old, "node").await.unwrap().as_deref(),
            Some("old-root")
        );
        let mut corrupt = node_metadata("node", "unreadable");
        corrupt["row"]["root_node_id"] = Value::Null;
        let mutations = [
            Some(encode(&node_metadata("node", "new-root")).unwrap()),
            None,
            Some(encode(&corrupt).unwrap()),
        ];
        for (index, bytes) in mutations.into_iter().enumerate() {
            replace(&store, &key, bytes).await;
            assert_eq!(
                old_roots.read(&mut old, "node").await.unwrap().as_deref(),
                Some("old-root")
            );
            assert_eq!(
                NodeRoots::default()
                    .read(&mut old, "node")
                    .await
                    .unwrap()
                    .as_deref(),
                Some("old-root")
            );
            let mut fresh = read_session(&mut application, "fresh").await;
            let current = NodeRoots::default().read(&mut fresh, "node").await;
            match index {
                0 => assert_eq!(current.unwrap().as_deref(), Some("new-root")),
                1 => assert_eq!(current.unwrap(), None),
                _ => assert!(matches!(current, Err(Error::IncompatibleStore))),
            }
            fresh.transaction.rollback().await.unwrap();
        }
        old.transaction.rollback().await.unwrap();
    }

    #[tokio::test]
    async fn writers_keep_staged_root_validation_savepoint_restore_and_observation_limits() {
        let (_fixture, store, mut application, catalog) = fixture().await;
        let key = node_key(&catalog, "node");
        replace(
            &store,
            &key,
            Some(encode(&node_metadata("node", "old-root")).unwrap()),
        )
        .await;
        let large_size = crate::MAX_BATCH_BYTES - 64 * 1024;
        replace(&store, b"node-root-budget/a", Some(vec![b'a'; large_size])).await;
        replace(&store, b"node-root-budget/b", Some(vec![b'b'; 64 * 1024])).await;
        application.begin("writer").await.unwrap();
        let mut writer = application.take("writer").unwrap();
        let mut roots = NodeRoots::default();
        assert_eq!(
            roots.read(&mut writer, "node").await.unwrap().as_deref(),
            Some("old-root")
        );
        let checkpoint = writer.transaction.checkpoint();
        writer
            .transaction
            .put(
                key.clone(),
                encode(&node_metadata("node", "staged-root")).unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            roots.read(&mut writer, "node").await.unwrap().as_deref(),
            Some("staged-root")
        );
        writer.transaction.restore(&checkpoint);
        assert_eq!(
            roots.read(&mut writer, "node").await.unwrap().as_deref(),
            Some("old-root")
        );
        let mut corrupt = node_metadata("node", "root");
        corrupt["row"]["root_node_id"] = int_cell(7);
        writer
            .transaction
            .put(key, encode(&corrupt).unwrap())
            .await
            .unwrap();
        assert!(matches!(
            roots.read(&mut writer, "node").await,
            Err(Error::IncompatibleStore)
        ));
        // Inspect the helper before the public finish path would abort this
        // corrupted transaction; restoration here tests its staged read source.
        writer.transaction.restore(&checkpoint);
        assert!(roots.values.is_empty());
        assert_eq!(roots.bytes, 0);
        assert_eq!(
            writer
                .transaction
                .get(b"node-root-budget/a")
                .await
                .unwrap()
                .unwrap()
                .len(),
            large_size
        );
        assert_eq!(
            roots.read(&mut writer, "node").await.unwrap().as_deref(),
            Some("old-root")
        );
        assert!(matches!(
            writer.transaction.get(b"node-root-budget/b").await,
            Err(Error::ResourceLimit(
                crate::ResourceLimit::ObservationBudget
            ))
        ));
        writer.transaction.rollback().await.unwrap();
    }
}
