//! Shell summaries over compact projection metadata. Historical message/tool
//! bodies are loaded only when selected for an actual shell response.

use std::collections::{BTreeMap, BTreeSet};

use super::read_index::{RowOrder, window};
use super::*;

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ShellOptions {
    location: Option<String>,
    project_id: Option<String>,
    unsettled_only: Option<bool>,
    family_of: Option<String>,
    worktree_path: Option<String>,
    thread_ids: Option<Vec<String>>,
    listed_only: Option<bool>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SettledCursor {
    settled_at: String,
    thread_id: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ProjectionShellQuery {
    Threads {
        thread_id: Option<String>,
        options: Option<ShellOptions>,
    },
    Runs {
        thread_ids: Option<Vec<String>>,
    },
    RunItemCounts {
        thread_ids: Vec<String>,
    },
    ItemCounts {
        thread_ids: Vec<String>,
    },
    ProviderThreads {
        thread_ids: Option<Vec<String>>,
    },
    PendingItems {
        thread_ids: Option<Vec<String>>,
        active_run_ids: Vec<String>,
    },
    SettlementThreads {
        thread_id: Option<String>,
    },
    RemoteTasks {},
    ThreadsWithPullRequests {
        thread_id: Option<String>,
    },
    Worktrees {
        include_archived: bool,
    },
    PullRequestThreads {
        thread_id: Option<String>,
        unsettled_only: bool,
    },
    ActiveSubagents {},
    CountThreads {},
    ActiveIndex {},
    GuestShare {
        share_id: String,
    },
    OlderSettled {
        before: SettledCursor,
        project_ids: Option<Vec<String>>,
        limit: i64,
    },
}

#[derive(Clone)]
struct Meta {
    row: Map<String, Value>,
    payload: Value,
    order: RowOrder,
}

impl Meta {
    fn from_index(row: read_index::IndexedRow) -> Result<Self> {
        let payload = match row.row.get("payload_json") {
            Some(_) => serde_json::from_str(&text(&row.row, "payload_json")?)
                .map_err(|_| Error::IncompatibleStore)?,
            None => Value::Null,
        };
        Ok(Self {
            row: row.row,
            payload,
            order: row.order,
        })
    }
    fn col(&self, name: &str) -> Option<&str> {
        self.row.get(name)?.get("value")?.as_str()
    }
    fn p(&self, name: &str) -> &Value {
        self.payload.get(name).unwrap_or(&Value::Null)
    }
    fn pstr(&self, name: &str) -> Option<&str> {
        self.p(name).as_str()
    }
    fn flag(&self, name: &str) -> bool {
        self.p(name) == &Value::Bool(true)
    }
    fn number(&self, name: &str) -> Result<i64> {
        integer(&self.row, name)
    }
    fn id(&self, name: &str) -> Result<String> {
        text(&self.row, spec(name).primary[0])
    }
}

fn one(value: &Value) -> bool {
    value == &Value::Bool(true) || value.as_f64() == Some(1.0)
}

async fn matching(
    session: &mut Session,
    name: &str,
    column: &str,
    value: Value,
) -> Result<Vec<Meta>> {
    window::equal_metadata(session, name, column, &value)
        .await?
        .into_iter()
        .map(Meta::from_index)
        .collect()
}
async fn related(
    session: &mut Session,
    name: &str,
    column: &str,
    value: &str,
) -> Result<Vec<Meta>> {
    matching(session, name, column, text_cell(value)).await
}
async fn point(session: &mut Session, name: &str, id: &str) -> Result<Option<Meta>> {
    window::read_metadata(session, name, id)
        .await?
        .map(Meta::from_index)
        .transpose()
}
async fn selected(
    session: &mut Session,
    name: &str,
    threads: Option<&[String]>,
) -> Result<Vec<Meta>> {
    let mut rows = Vec::new();
    match threads {
        None => rows = matching(session, name, "all", Value::Null).await?,
        Some(ids) => {
            for id in ids.iter().collect::<BTreeSet<_>>() {
                rows.extend(related(session, name, "thread_id", id).await?);
            }
        }
    }
    rows.sort_by_key(|row| row.order);
    Ok(rows)
}
async fn full(session: &mut Session, name: &str, metadata: &Meta) -> Result<String> {
    let primary = spec(name)
        .primary
        .iter()
        .map(|key| {
            metadata
                .row
                .get(*key)
                .cloned()
                .ok_or(Error::IncompatibleStore)
        })
        .collect::<Result<Vec<_>>>()?;
    let row = read_row(&mut session.transaction, &session.catalog, name, &primary)
        .await?
        .ok_or(Error::IncompatibleStore)?;
    text(&row.row, "payload_json")
}
async fn optional_full(
    session: &mut Session,
    name: &str,
    metadata: Option<&Meta>,
) -> Result<Value> {
    match metadata {
        Some(row) => Ok(json!(full(session, name, row).await?)),
        None => Ok(Value::Null),
    }
}

fn sort_updated(rows: &mut [Meta], id: &str, descending: bool) {
    rows.sort_by(|a, b| {
        let order = a
            .col("updated_at")
            .cmp(&b.col("updated_at"))
            .then_with(|| a.col(id).cmp(&b.col(id)));
        if descending { order.reverse() } else { order }
    });
}
fn sort_runs(rows: &mut [Meta]) -> Result<()> {
    for row in rows.iter() {
        row.number("ordinal")?;
    }
    rows.sort_by(|a, b| {
        b.number("ordinal")
            .unwrap_or(0)
            .cmp(&a.number("ordinal").unwrap_or(0))
            .then_with(|| b.col("run_id").cmp(&a.col("run_id")))
    });
    Ok(())
}

async fn threads(
    session: &mut Session,
    thread_id: Option<&str>,
    options: &ShellOptions,
) -> Result<Vec<Meta>> {
    let family = match options.family_of.as_deref() {
        Some(id) => point(session, THREADS, id)
            .await?
            .and_then(|row| row.p("lineage").get("rootThreadId").cloned()),
        None => None,
    };
    let candidates = match thread_id {
        Some(id) => window::equal_metadata(session, THREADS, "thread_id", &text_cell(id)).await?,
        None => window::equal_metadata(session, THREADS, "all", &Value::Null).await?,
    };
    let mut rows = Vec::new();
    for row in candidates {
        // SQL rejects deleted rows before evaluating their JSON expressions.
        if nullable(&row.row, "deleted_at")?.is_none() {
            rows.push(Meta::from_index(row)?);
        }
    }
    rows.retain(|row| {
        let lineage = row.p("lineage");
        row.col("deleted_at").is_none()
            && options
                .project_id
                .as_deref()
                .is_none_or(|project| row.col("project_id") == Some(project))
            && options.thread_ids.as_ref().is_none_or(|ids| {
                row.col("thread_id")
                    .is_some_and(|id| ids.iter().any(|candidate| candidate == id))
            })
            && (options.listed_only != Some(true)
                || (lineage.get("relationshipToParent").and_then(Value::as_str)
                    != Some("subagent")
                    && !one(lineage.get("sideChat").unwrap_or(&Value::Null))))
            && (options.location.as_deref() != Some("active") || row.p("archivedAt").is_null())
            && (options.location.as_deref() != Some("archive") || !row.p("archivedAt").is_null())
            && (options.unsettled_only != Some(true)
                || (row.p("settledAt").is_null() && row.pstr("settledOverride") != Some("settled")))
            && (options.family_of.is_none()
                || family.as_ref().is_some_and(|root| {
                    !root.is_null() && lineage.get("rootThreadId") == Some(root)
                }))
            && options
                .worktree_path
                .as_deref()
                .is_none_or(|path| row.pstr("worktreePath") == Some(path))
    });
    sort_updated(&mut rows, "thread_id", false);
    Ok(rows)
}

struct ThreadData {
    runs: Vec<Meta>,
    items: Vec<Meta>,
    messages: Vec<Meta>,
    requests: Vec<Meta>,
    plans: Vec<Meta>,
    providers: Vec<Meta>,
}
impl ThreadData {
    async fn load(session: &mut Session, id: &str) -> Result<Self> {
        let mut runs = related(session, RUNS, "thread_id", id).await?;
        sort_runs(&mut runs)?;
        Ok(Self {
            runs,
            items: related(session, ITEMS, "thread_id", id).await?,
            messages: related(session, MESSAGES, "thread_id", id).await?,
            requests: related(session, REQUESTS, "thread_id", id).await?,
            plans: related(session, PLANS, "thread_id", id).await?,
            providers: related(session, PROVIDER_THREADS, "thread_id", id).await?,
        })
    }
    async fn silent_runs(&self, session: &mut Session) -> Result<BTreeSet<String>> {
        let mut silent = BTreeSet::new();
        for run in &self.runs {
            if run.col("status") == Some("failed") {
                continue;
            }
            let Some(message) = run.pstr("userMessageId") else {
                continue;
            };
            let prompt = point(session, MESSAGES, message).await?;
            if prompt.is_some_and(|row| row.flag("nativeIdleCompactionMessage"))
                && !self.items.iter().any(|item| {
                    item.col("run_id") == run.col("run_id")
                        && item.col("type") == Some("compaction")
                        && item.col("status") == Some("failed")
                })
            {
                silent.insert(run.id(RUNS)?);
            }
        }
        Ok(silent)
    }
    fn latest_message(&self, authored: bool) -> Option<&Meta> {
        self.messages
            .iter()
            .filter(|row| {
                row.col("role") == Some("user")
                    && !row.flag("nativeIdleCompactionMessage")
                    && (!authored || row.pstr("createdBy") == Some("user"))
            })
            .max_by(|a, b| {
                a.col("updated_at")
                    .cmp(&b.col("updated_at"))
                    .then_with(|| a.col("message_id").cmp(&b.col("message_id")))
            })
    }
    fn pending_request(&self) -> Option<&Meta> {
        self.requests
            .iter()
            .filter(|row| row.col("status") == Some("pending"))
            .min_by(|a, b| {
                (a.col("kind") == Some("user_input"))
                    .cmp(&(b.col("kind") == Some("user_input")))
                    .then_with(|| b.col("created_at").cmp(&a.col("created_at")))
                    .then_with(|| {
                        b.col("runtime_request_id")
                            .cmp(&a.col("runtime_request_id"))
                    })
            })
    }
    fn pending_link(&self) -> Option<&Meta> {
        self.items
            .iter()
            .filter(|item| {
                item.col("type") == Some("link_request") && item.col("status") == Some("waiting")
            })
            .max_by(|a, b| {
                a.col("updated_at")
                    .cmp(&b.col("updated_at"))
                    .then_with(|| a.col("turn_item_id").cmp(&b.col("turn_item_id")))
            })
    }
    fn has_active(&self) -> bool {
        self.runs.iter().any(|run| active(run.col("status"), true))
    }
    fn live(&self, thread: &Meta) -> bool {
        self.runs
            .iter()
            .any(|run| run.col("status") == Some("queued") || active(run.col("status"), true))
            || self.pending_request().is_some()
            || self.pending_link().is_some()
            || self.items.iter().any(pending_work)
            || self
                .providers
                .iter()
                .any(|row| row.flag("nativeHasPendingBackgroundTasks"))
            || thread.flag("nativeHasPullRequestWatch")
    }
}
fn active(status: Option<&str>, waiting: bool) -> bool {
    matches!(status, Some("preparing" | "starting" | "running"))
        || (waiting && status == Some("waiting"))
}
fn pending_work(item: &Meta) -> bool {
    matches!(
        item.col("type"),
        Some("command_execution" | "dynamic_tool" | "subagent")
    ) && !matches!(
        item.col("status"),
        Some("completed" | "interrupted" | "failed" | "cancelled")
    )
}
fn stop_requested(data: &ThreadData, run: Option<&Meta>) -> bool {
    run.is_some_and(|run| {
        data.items.iter().any(|item| {
            item.col("run_id") == run.col("run_id")
                && item.col("type") == Some("run_interrupt_request")
        })
    })
}
fn failure<'a>(data: &'a ThreadData, run: Option<&Meta>) -> Result<Option<&'a Meta>> {
    let Some(run) = run else {
        return Ok(None);
    };
    let mut matches = Vec::new();
    for item in &data.items {
        if item.col("run_id") == run.col("run_id")
            && item.col("type") == Some("error")
            && item.col("status") == Some("failed")
            && item.col("node_id") == run.pstr("rootNodeId")
        {
            matches.push((
                item.col("updated_at"),
                item.number("ordinal")?,
                item.col("turn_item_id"),
                item,
            ));
        }
    }
    matches.sort_by(|a, b| {
        b.0.cmp(&a.0)
            .then_with(|| b.1.cmp(&a.1))
            .then_with(|| b.2.cmp(&a.2))
    });
    Ok(matches.first().map(|row| row.3))
}
fn opt_col(row: Option<&Meta>, name: &str) -> Value {
    row.and_then(|row| row.col(name))
        .map_or(Value::Null, |value| json!(value))
}
fn opt_p(row: Option<&Meta>, name: &str) -> Value {
    row.map_or(Value::Null, |row| row.p(name).clone())
}

async fn shell_thread(session: &mut Session, thread: &Meta) -> Result<Value> {
    let id = thread.id(THREADS)?;
    let data = ThreadData::load(session, &id).await?;
    let silent = data.silent_runs(session).await?;
    let presentable = |run: &&Meta| !silent.contains(run.col("run_id").unwrap_or(""));
    let presented = data
        .runs
        .iter()
        .filter(presentable)
        .find(|run| !(run.col("status") == Some("queued") && one(run.p("queueHeld"))));
    let active_run = data
        .runs
        .iter()
        .filter(presentable)
        .find(|run| active(run.col("status"), false));
    let activity = data
        .runs
        .iter()
        .filter(presentable)
        .find(|run| active(run.col("status"), true));
    let mut executed = Vec::new();
    for run in &data.runs {
        if run.col("status") != Some("queued")
            && !(run.col("status") == Some("cancelled") && run.p("startedAt").is_null())
        {
            executed.push((
                run.col("completed_at").is_none(),
                run.col("completed_at"),
                run.number("ordinal")?,
                run.col("run_id"),
                run,
            ));
        }
    }
    executed.sort_by(|a, b| {
        b.0.cmp(&a.0)
            .then_with(|| b.1.cmp(&a.1))
            .then_with(|| b.2.cmp(&a.2))
            .then_with(|| b.3.cmp(&a.3))
    });
    let blocked = executed
        .first()
        .map(|row| row.4)
        .filter(|row| row.col("status") == Some("failed"));
    let mut sessions = Vec::new();
    for binding in related(session, BINDINGS, "thread_id", &id).await? {
        if let Some(session_id) = binding.col("provider_session_id") {
            if let Some(row) = point(session, SESSIONS, session_id).await? {
                if row.col("provider_instance_id") == thread.col("provider_instance_id") {
                    sessions.push(row);
                }
            }
        }
    }
    sort_updated(&mut sessions, "provider_session_id", true);
    let last_error = match sessions.first() {
        Some(row) => serde_json::from_str::<Value>(&full(session, SESSIONS, row).await?)
            .map_err(|_| Error::IncompatibleStore)?
            .get("lastError")
            .cloned()
            .unwrap_or(Value::Null),
        None => Value::Null,
    };
    let secret = data
        .items
        .iter()
        .filter(|item| {
            item.col("type") == Some("secret_request")
                && item.col("status") == Some("waiting")
                && data.runs.iter().any(|run| {
                    run.col("run_id") == item.col("run_id") && active(run.col("status"), true)
                })
        })
        .max_by(|a, b| {
            a.col("updated_at")
                .cmp(&b.col("updated_at"))
                .then_with(|| a.col("turn_item_id").cmp(&b.col("turn_item_id")))
        });
    let fork = thread.p("forkedFrom");
    let source = if fork.get("type").and_then(Value::as_str) == Some("run")
        && !one(thread.p("lineage").get("sideChat").unwrap_or(&Value::Null))
    {
        fork.get("threadId").cloned().unwrap_or(Value::Null)
    } else {
        Value::Null
    };
    let activity_started = activity
        .map(|run| {
            [run.p("workStartedAt"), run.p("startedAt")]
                .into_iter()
                .find(|value| !value.is_null())
                .cloned()
                .unwrap_or_else(|| opt_col(Some(run), "requested_at"))
        })
        .unwrap_or(Value::Null);
    Ok(
        json!({"thread_id":id,"payload_json":full(session,THREADS,thread).await?,"forked_from_run_source_thread_id":source,
        "latest_run_id":opt_col(presented,"run_id"),"latest_run_status":opt_col(presented,"status"),"latest_run_requested_at":opt_col(presented,"requested_at"),"latest_run_started_at":opt_p(presented,"startedAt"),"latest_run_completed_at":opt_col(presented,"completed_at"),"latest_run_stop_requested":u8::from(stop_requested(&data,presented)),
        "active_run_id":opt_col(active_run,"run_id"),"active_run_status":opt_col(active_run,"status"),"activity_run_status":opt_col(activity,"status"),"activity_run_started_at":activity_started,"last_error":last_error,
        "terminal_failure_payload_json":optional_full(session,ITEMS,failure(&data,presented.filter(|run|run.col("status")==Some("failed")))?).await?,
        "blocking_run_id":opt_col(blocked,"run_id"),"blocking_run_requested_at":opt_col(blocked,"requested_at"),"blocking_run_started_at":opt_p(blocked,"startedAt"),"blocking_run_completed_at":opt_col(blocked,"completed_at"),"blocking_run_stop_requested":u8::from(stop_requested(&data,blocked)),"blocking_failure_payload_json":optional_full(session,ITEMS,failure(&data,blocked)?).await?,
        "pending_request_payload_json":optional_full(session,REQUESTS,data.pending_request()).await?,"pending_secret_request_payload_json":optional_full(session,ITEMS,secret).await?,"pending_link_request_payload_json":optional_full(session,ITEMS,data.pending_link()).await?,
        "latest_user_message_at":opt_col(data.latest_message(false),"updated_at"),"latest_user_authored_message_at":opt_col(data.latest_message(true),"updated_at"),"has_actionable_proposed_plan":u8::from(data.plans.iter().any(|row|row.col("kind")==Some("proposed_plan")&&row.col("status")==Some("active")))}),
    )
}

async fn settlement(session: &mut Session, thread: &Meta) -> Result<Option<Value>> {
    if [
        "archivedAt",
        "settledOverride",
        "pinnedAt",
        "autoSettleDisabledAt",
    ]
    .into_iter()
    .any(|key| !thread.p(key).is_null())
    {
        return Ok(None);
    }
    let id = thread.id(THREADS)?;
    let data = ThreadData::load(session, &id).await?;
    if data.has_active() || data.pending_request().is_some() {
        return Ok(None);
    }
    let authored = data
        .latest_message(true)
        .and_then(|row| row.col("updated_at"));
    if data.items.iter().any(|item| {
        item.col("type") == Some("link_request")
            && item.col("status") == Some("waiting")
            && !authored.is_some_and(|time| {
                item.pstr("startedAt")
                    .or_else(|| item.col("updated_at"))
                    .is_some_and(|started| time > started)
            })
    }) {
        return Ok(None);
    }
    let silent = data.silent_runs(session).await?;
    let latest = data
        .runs
        .iter()
        .find(|run| !silent.contains(run.col("run_id").unwrap_or("")));
    Ok(Some(
        json!({"thread_id":id,"payload_json":full(session,THREADS,thread).await?,"latest_run_id":opt_col(latest,"run_id"),"latest_run_status":opt_col(latest,"status"),"latest_run_requested_at":opt_col(latest,"requested_at"),"latest_run_started_at":opt_p(latest,"startedAt"),"latest_run_completed_at":opt_col(latest,"completed_at"),"latest_user_message_at":opt_col(data.latest_message(false),"updated_at"),"latest_user_authored_message_at":opt_col(data.latest_message(true),"updated_at")}),
    ))
}

async fn item_counts(session: &mut Session, ids: &[String], per_run: bool) -> Result<Value> {
    let mut groups: BTreeMap<(String, Option<String>), u64> = BTreeMap::new();
    for item in selected(session, ITEMS, Some(ids)).await? {
        let key = (
            text(&item.row, "thread_id")?,
            nullable(&item.row, "run_id")?,
        );
        *groups.entry(key).or_default() += 1;
    }
    if per_run {
        return Ok(json!(
            groups
                .into_iter()
                .filter_map(|((thread, run), count)| run
                    .map(|run| json!({"thread_id":thread,"run_id":run,"item_count":count})))
                .collect::<Vec<_>>()
        ));
    }
    let mut totals: BTreeMap<String, (u64, u64)> = BTreeMap::new();
    for ((thread, run), count) in groups {
        let entry = totals.entry(thread).or_default();
        match run {
            None => {
                entry.0 += count;
                entry.1 += count;
            }
            Some(id) => {
                if point(session, RUNS, &id)
                    .await?
                    .is_some_and(|run| run.col("status") != Some("rolled_back"))
                {
                    entry.0 += count;
                }
            }
        }
    }
    Ok(json!(totals.into_iter().map(|(thread,(total,runless))|json!({"thread_id":thread,"item_count":total,"runless_item_count":runless})).collect::<Vec<_>>()))
}

async fn pending_items(
    session: &mut Session,
    ids: Option<&[String]>,
    active_ids: &[String],
) -> Result<Value> {
    let mut items = selected(session, ITEMS, ids).await?;
    items.retain(pending_work);
    items.sort_by(|a, b| {
        a.col("thread_id")
            .cmp(&b.col("thread_id"))
            .then_with(|| a.col("run_id").cmp(&b.col("run_id")))
            .then_with(|| a.order.cmp(&b.order))
    });
    let mut result = Vec::new();
    for item in items {
        if let Some(run) = item.col("run_id") {
            if !point(session, RUNS, run)
                .await?
                .is_some_and(|run| run.col("status") != Some("rolled_back"))
            {
                continue;
            }
        }
        let instance = match item.col("provider_thread_id") {
            Some(id) => point(session, PROVIDER_THREADS, id)
                .await?
                .and_then(|row| row.col("provider_instance_id").map(str::to_owned)),
            None => None,
        };
        result.push(json!({"thread_id":item.col("thread_id"),"payload_json":full(session,ITEMS,&item).await?,"owning_provider_instance_id":instance}));
    }
    for id in active_ids.iter().collect::<BTreeSet<_>>() {
        let mut items = related(session, ITEMS, "run_id", id).await?;
        items.retain(|item| {
            matches!(
                item.col("type"),
                Some("file_change" | "file_search" | "web_search" | "compaction")
            ) && matches!(item.col("status"), Some("running" | "waiting"))
        });
        for row in &items {
            row.number("ordinal")?;
        }
        items.sort_by(|a, b| {
            a.number("ordinal")
                .unwrap_or(0)
                .cmp(&b.number("ordinal").unwrap_or(0))
                .then_with(|| a.order.cmp(&b.order))
        });
        for item in items {
            result.push(json!({"thread_id":item.col("thread_id"),"payload_json":full(session,ITEMS,&item).await?,"owning_provider_instance_id":Value::Null}));
        }
    }
    Ok(json!(result))
}

async fn query(session: &mut Session, input: ProjectionShellQuery) -> Result<Value> {
    read_index::ensure_ready(session).await?;
    match input {
        ProjectionShellQuery::Threads{thread_id,options}=>{let mut result=Vec::new();for thread in threads(session,thread_id.as_deref(),&options.unwrap_or_default()).await?{result.push(shell_thread(session,&thread).await?);}Ok(json!(result))}
        ProjectionShellQuery::Runs{thread_ids}=>{let mut result=Vec::new();for run in selected(session,RUNS,thread_ids.as_deref()).await?{result.push(json!({"thread_id":run.col("thread_id"),"run_id":run.col("run_id"),"ordinal":run.number("ordinal")?}));}Ok(json!(result))}
        ProjectionShellQuery::RunItemCounts{thread_ids}=>item_counts(session,&thread_ids,true).await,
        ProjectionShellQuery::ItemCounts{thread_ids}=>item_counts(session,&thread_ids,false).await,
        ProjectionShellQuery::ProviderThreads{thread_ids}=>{let mut result=Vec::new();let mut rows=selected(session,PROVIDER_THREADS,thread_ids.as_deref()).await?;if thread_ids.is_some(){rows.sort_by(|a,b|a.col("thread_id").cmp(&b.col("thread_id")).then_with(||a.order.cmp(&b.order)));}for row in rows{if row.col("thread_id").is_some(){result.push(json!({"thread_id":row.col("thread_id"),"payload_json":full(session,PROVIDER_THREADS,&row).await?}));}}Ok(json!(result))}
        ProjectionShellQuery::PendingItems{thread_ids,active_run_ids}=>pending_items(session,thread_ids.as_deref(),&active_run_ids).await,
        ProjectionShellQuery::SettlementThreads{thread_id}=>{let mut result=Vec::new();for row in threads(session,thread_id.as_deref(),&ShellOptions::default()).await?{if let Some(row)=settlement(session,&row).await?{result.push(row);}}Ok(json!(result))}
        ProjectionShellQuery::RemoteTasks{}=>{Ok(json!(selected(session,SUBAGENTS,None).await?.into_iter().filter(|row|row.col("origin")==Some("app_owned")&&row.col("child_thread_id").is_none()&&row.flag("nativeOpenRemoteDelegatedTask")).map(|row|json!({"thread_id":row.col("thread_id"),"subagent_id":row.col("subagent_id")})).collect::<Vec<_>>()))}
        ProjectionShellQuery::ThreadsWithPullRequests{thread_id}=>{let mut result=Vec::new();for row in threads(session,thread_id.as_deref(),&ShellOptions{location:Some("active".into()),..Default::default()}).await?{if row.flag("nativeHasPullRequests"){result.push(json!({"payload_json":full(session,THREADS,&row).await?}));}}Ok(json!(result))}
        ProjectionShellQuery::Worktrees{include_archived}=>{let mut rows=threads(session,None,&ShellOptions{location:if include_archived{None}else{Some("active".into())},..Default::default()}).await?;rows.sort_by_key(|row|row.order);Ok(json!(rows.into_iter().filter_map(|row|row.pstr("worktreePath").map(|path|json!({"thread_id":row.col("thread_id"),"worktree_path":path}))).collect::<Vec<_>>()))}
        ProjectionShellQuery::PullRequestThreads{thread_id,unsettled_only}=>{let options=ShellOptions{location:if thread_id.is_none(){Some("active".into())}else{None},unsettled_only:Some(unsettled_only),..Default::default()};let mut result=Vec::new();for row in threads(session,thread_id.as_deref(),&options).await?{result.push(json!({"payload_json":full(session,THREADS,&row).await?}));}Ok(json!(result))}
        ProjectionShellQuery::ActiveSubagents{}=>{let mut rows=threads(session,None,&ShellOptions{location:Some("active".into()),..Default::default()}).await?;rows.sort_by_key(|row|row.order);Ok(json!(rows.into_iter().filter(|row|row.p("lineage").get("relationshipToParent").and_then(Value::as_str)==Some("subagent")).map(|row|json!({"thread_id":row.col("thread_id")})).collect::<Vec<_>>()))}
        ProjectionShellQuery::CountThreads{}=>{
            let all=read_index::count_equal(session,THREADS,"all",&Value::Null).await?;
            let deleted=read_index::count_equal(session,THREADS,"_deleted",&int_cell(1)).await?;
            Ok(json!([{"count":all.checked_sub(deleted).ok_or(Error::IncompatibleStore)?}]))
        },
        ProjectionShellQuery::ActiveIndex{}=>{let mut result=Vec::new();for row in threads(session,None,&ShellOptions{location:Some("active".into()),..Default::default()}).await?{let data=ThreadData::load(session,&row.id(THREADS)?).await?;let lineage=row.p("lineage");let side=lineage.get("sideChat").map(|value|if let Some(value)=value.as_bool(){json!(u8::from(value))}else{value.clone()}).unwrap_or(Value::Null);result.push(json!({"thread_id":row.col("thread_id"),"project_id":row.col("project_id"),"relationship_to_parent":lineage.get("relationshipToParent"),"parent_thread_id":lineage.get("parentThreadId"),"root_thread_id":lineage.get("rootThreadId"),"side_chat":side,"settled_override":row.p("settledOverride"),"settled_at":row.p("settledAt"),"pinned_at":row.p("pinnedAt"),"snoozed_until":row.p("snoozedUntil"),"may_be_live":u8::from(data.live(&row))}));}Ok(json!(result))}
        ProjectionShellQuery::GuestShare{share_id}=>{let rows=selected(session,THREADS,None).await?;Ok(json!(rows.into_iter().find(|row|row.pstr("nativeGuestShareId")==Some(share_id.as_str())).map(|row|json!({"thread_id":row.col("thread_id")})).into_iter().collect::<Vec<_>>()))}
        ProjectionShellQuery::OlderSettled{before,project_ids,limit}=>{let mut rows=threads(session,None,&ShellOptions{location:Some("active".into()),listed_only:Some(true),..Default::default()}).await?;rows.retain(|row|row.pstr("settledOverride")==Some("settled")&&row.pstr("settledAt").is_some_and(|time|time<before.settled_at.as_str()||(time==before.settled_at&&before.thread_id.as_deref().is_some_and(|id|row.col("thread_id").is_some_and(|thread|thread>id))))&&project_ids.as_ref().is_none_or(|ids|row.col("project_id").is_some_and(|project|ids.iter().any(|id|id==project))));rows.sort_by(|a,b|b.pstr("settledAt").cmp(&a.pstr("settledAt")).then_with(||a.col("thread_id").cmp(&b.col("thread_id"))));if limit==0{let mut counts=BTreeMap::<String,u64>::new();for row in rows{*counts.entry(text(&row.row,"project_id")?).or_default()+=1;}Ok(json!({"pageRows":[],"countRows":counts.into_iter().map(|(project,count)|json!({"project_id":project,"count":count})).collect::<Vec<_>>()}))}else{Ok(json!({"pageRows":rows.into_iter().take(if limit<0{usize::MAX}else{limit as usize}).map(|row|json!({"thread_id":row.col("thread_id")})).collect::<Vec<_>>(),"countRows":[]}))}}
    }
}

impl ApplicationTransactions {
    pub async fn projections_shell(
        &mut self,
        id: &str,
        input: ProjectionShellQuery,
    ) -> Result<Value> {
        let mut session = self.take(id)?;
        let result = query(&mut session, input).await;
        self.finish(id, session, result).await
    }
}
