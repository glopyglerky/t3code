use std::collections::HashSet;
use std::io::{self, BufRead, Read, Write};
use std::path::{Path, PathBuf};

use serde::Deserialize;
use serde_json::{Map, Value, json};
use t3_code_ls_core::json as core_json;
use t3_code_ls_storage::application::{
    ApplicationSchema, ApplicationTransactionMode, ApplicationTransactions,
};
use t3_code_ls_storage::application_events::{EventAppendRow, EventLatest, EventRead, EventStats};
use t3_code_ls_storage::outbox::{self, EnqueueEffect};
use t3_code_ls_storage::project_service::{ProjectRequest, execute_project};
use t3_code_ls_storage::sqlite_import::ImportLimits;
use t3_code_ls_storage::{CommitBatch, Error, MAX_BATCH_BYTES, OutboxBatch, Store};

#[derive(Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
enum Request {
    ApplicationSchema {},
    ApplicationInit {
        schema: ApplicationSchema,
    },
    ApplicationBegin {
        transaction_id: String,
        #[serde(default)]
        mode: ApplicationTransactionMode,
        lock_timeout_ms: Option<u64>,
    },
    ApplicationSavepoint {
        transaction_id: String,
        savepoint_id: String,
    },
    ApplicationRelease {
        transaction_id: String,
        savepoint_id: String,
    },
    ApplicationRollbackTo {
        transaction_id: String,
        savepoint_id: String,
    },
    ApplicationGet {
        transaction_id: String,
        table: String,
        key: Vec<Value>,
    },
    ApplicationScan {
        transaction_id: String,
        table: String,
        after: Option<String>,
        limit: u32,
    },
    ApplicationPut {
        transaction_id: String,
        table: String,
        key: Vec<Value>,
        row: Map<String, Value>,
    },
    ApplicationDelete {
        transaction_id: String,
        table: String,
        key: Vec<Value>,
    },
    ApplicationCommit {
        transaction_id: String,
    },
    ApplicationRollback {
        transaction_id: String,
    },
    ApplicationEventsPrepare {},
    ApplicationEventsAppend {
        transaction_id: String,
        row: EventAppendRow,
        prompt_hint: Option<u64>,
    },
    ApplicationEventsRead {
        transaction_id: String,
        after_sequence: u64,
        through_sequence: Option<u64>,
        thread_id: Option<String>,
        command_id: Option<String>,
        event_type: Option<String>,
        only_agent_events: Option<bool>,
        limit: u32,
    },
    ApplicationEventsStats {
        transaction_id: String,
        kind: String,
        after_sequence: u64,
        through_sequence: u64,
        thread_id: Option<String>,
        max_events: Option<u64>,
    },
    ApplicationEventsLatest {
        transaction_id: String,
        kind: String,
        thread_id: Option<String>,
    },
    ApplicationEventsPrunedThrough {
        transaction_id: String,
    },
    ImportSqliteApplication {
        path: PathBuf,
    },
    ApplicationImport {},
    Snapshot {},
    Receipt {
        command_id: String,
    },
    Commit {
        batch: CommitBatch,
    },
    CommitOutbox {
        batch: CommitBatch,
        outbox: OutboxBatch,
    },
    OutboxReceipt {
        command_id: String,
    },
    OutboxGet {
        effect_id: String,
    },
    OutboxList {
        command_id: String,
    },
    OutboxEnqueue {
        effects: Vec<EnqueueEffect>,
        now: String,
    },
    OutboxClaim {
        worker_id: String,
        now: String,
        exclude_restart_continuations: bool,
    },
    OutboxClaimOnce {
        stream_id: String,
        sequence: u64,
        worker_id: String,
        now: String,
        exclude_restart_continuations: bool,
    },
    OutboxNextClaimableAt {},
    OutboxSucceed {
        effect_id: String,
        worker_id: String,
        now: String,
    },
    OutboxRetry {
        effect_id: String,
        worker_id: String,
        now: String,
        available_at: String,
        error: String,
    },
    OutboxFail {
        effect_id: String,
        worker_id: String,
        now: String,
        error: String,
    },
    OutboxCancel {
        thread_id: String,
        effect_types: Vec<outbox::EffectType>,
        reason: String,
        now: String,
    },
    OutboxReconcile {
        now: String,
    },
    OutboxPrune {
        before: String,
        limit: u32,
    },
    Events {
        after_sequence: u64,
        limit: u32,
    },
    PendingEffects {
        limit: u32,
    },
    Project {
        request: ProjectRequest,
    },
    ArchiveSqlite {
        path: PathBuf,
    },
    SqliteArchive {
        archive_id: String,
    },
    RestoreSqliteExport {
        archive_id: String,
        destination: PathBuf,
    },
}

impl Request {
    fn is_unscoped_mutation(&self) -> bool {
        match self {
            Self::ApplicationInit { .. }
            | Self::ApplicationEventsPrepare { .. }
            | Self::ImportSqliteApplication { .. }
            | Self::Commit { .. }
            | Self::CommitOutbox { .. }
            | Self::OutboxEnqueue { .. }
            | Self::OutboxClaim { .. }
            | Self::OutboxClaimOnce { .. }
            | Self::OutboxSucceed { .. }
            | Self::OutboxRetry { .. }
            | Self::OutboxFail { .. }
            | Self::OutboxCancel { .. }
            | Self::OutboxReconcile { .. }
            | Self::OutboxPrune { .. }
            | Self::Project { .. }
            | Self::ArchiveSqlite { .. } => true,
            Self::ApplicationSchema { .. }
            | Self::ApplicationBegin { .. }
            | Self::ApplicationSavepoint { .. }
            | Self::ApplicationRelease { .. }
            | Self::ApplicationRollbackTo { .. }
            | Self::ApplicationGet { .. }
            | Self::ApplicationScan { .. }
            | Self::ApplicationPut { .. }
            | Self::ApplicationDelete { .. }
            | Self::ApplicationCommit { .. }
            | Self::ApplicationRollback { .. }
            | Self::ApplicationEventsAppend { .. }
            | Self::ApplicationEventsRead { .. }
            | Self::ApplicationEventsStats { .. }
            | Self::ApplicationEventsLatest { .. }
            | Self::ApplicationEventsPrunedThrough { .. }
            | Self::ApplicationImport { .. }
            | Self::Snapshot { .. }
            | Self::Receipt { .. }
            | Self::OutboxReceipt { .. }
            | Self::OutboxGet { .. }
            | Self::OutboxList { .. }
            | Self::OutboxNextClaimableAt { .. }
            | Self::Events { .. }
            | Self::PendingEffects { .. }
            | Self::SqliteArchive { .. }
            | Self::RestoreSqliteExport { .. } => false,
        }
    }
}

async fn evaluate(
    store: &Store,
    transactions: &mut ApplicationTransactions,
    planner: Option<&Path>,
    request: Request,
) -> Result<Value, Error> {
    if request.is_unscoped_mutation() && transactions.has_active_writer() {
        return Err(Error::WriterBusy);
    }
    match request {
        Request::ApplicationSchema {} => serde_json::to_value(transactions.schema().await?),
        Request::ApplicationInit { schema } => {
            Ok(json!({"initialized": transactions.initialize(schema).await?}))
        }
        Request::ApplicationBegin {
            transaction_id,
            mode,
            lock_timeout_ms,
        } => {
            transactions
                .begin_mode(&transaction_id, mode, lock_timeout_ms)
                .await?;
            Ok(json!({"transaction_id": transaction_id}))
        }
        Request::ApplicationSavepoint {
            transaction_id,
            savepoint_id,
        } => {
            transactions
                .savepoint(&transaction_id, &savepoint_id)
                .await?;
            Ok(json!({"savepoint_id": savepoint_id}))
        }
        Request::ApplicationRelease {
            transaction_id,
            savepoint_id,
        } => {
            transactions.release(&transaction_id, &savepoint_id).await?;
            Ok(json!({"released": true}))
        }
        Request::ApplicationRollbackTo {
            transaction_id,
            savepoint_id,
        } => {
            transactions
                .rollback_to(&transaction_id, &savepoint_id)
                .await?;
            Ok(json!({"rolled_back": true}))
        }
        Request::ApplicationGet {
            transaction_id,
            table,
            key,
        } => serde_json::to_value(transactions.get(&transaction_id, &table, &key).await?),
        Request::ApplicationScan {
            transaction_id,
            table,
            after,
            limit,
        } => serde_json::to_value(
            transactions
                .scan(&transaction_id, &table, after.as_deref(), limit)
                .await?,
        ),
        Request::ApplicationPut {
            transaction_id,
            table,
            key,
            row,
        } => {
            transactions.put(&transaction_id, &table, key, row).await?;
            Ok(json!({"written": true}))
        }
        Request::ApplicationDelete {
            transaction_id,
            table,
            key,
        } => Ok(json!({"deleted": transactions.delete(&transaction_id, &table, &key).await?})),
        Request::ApplicationCommit { transaction_id } => {
            transactions.commit(&transaction_id).await?;
            Ok(json!({"committed": true}))
        }
        Request::ApplicationRollback { transaction_id } => {
            transactions.rollback(&transaction_id).await?;
            Ok(json!({"rolled_back": true}))
        }
        Request::ApplicationEventsPrepare {} => {
            serde_json::to_value(store.prepare_application_events().await?)
        }
        Request::ApplicationEventsAppend {
            transaction_id,
            row,
            prompt_hint,
        } => serde_json::to_value(
            transactions
                .events_append(&transaction_id, row, prompt_hint)
                .await?,
        ),
        Request::ApplicationEventsRead {
            transaction_id,
            after_sequence,
            through_sequence,
            thread_id,
            command_id,
            event_type,
            only_agent_events,
            limit,
        } => serde_json::to_value(
            transactions
                .events_read(
                    &transaction_id,
                    EventRead {
                        after_sequence,
                        through_sequence,
                        thread_id,
                        command_id,
                        event_type,
                        only_agent_events,
                        limit,
                    },
                )
                .await?,
        ),
        Request::ApplicationEventsStats {
            transaction_id,
            kind,
            after_sequence,
            through_sequence,
            thread_id,
            max_events,
        } => serde_json::to_value(
            transactions
                .events_stats(
                    &transaction_id,
                    EventStats {
                        kind,
                        after_sequence,
                        through_sequence,
                        thread_id,
                        max_events,
                    },
                )
                .await?,
        ),
        Request::ApplicationEventsLatest {
            transaction_id,
            kind,
            thread_id,
        } => serde_json::to_value(
            transactions
                .events_latest(&transaction_id, EventLatest { kind, thread_id })
                .await?,
        ),
        Request::ApplicationEventsPrunedThrough { transaction_id } => {
            serde_json::to_value(transactions.events_pruned_through(&transaction_id).await?)
        }
        Request::ImportSqliteApplication { path } => serde_json::to_value(
            store
                .import_sqlite_application(path, ImportLimits::default())
                .await?,
        ),
        Request::ApplicationImport {} => serde_json::to_value(store.application_import().await?),
        Request::Snapshot {} => serde_json::to_value(store.snapshot().await?),
        Request::Receipt { command_id } => {
            serde_json::to_value(store.read_receipt(&command_id).await?)
        }
        Request::Commit { batch } => serde_json::to_value(store.commit(&batch).await?),
        Request::CommitOutbox { batch, outbox } => {
            serde_json::to_value(store.commit_outbox(&batch, &outbox).await?)
        }
        Request::OutboxReceipt { command_id } => {
            serde_json::to_value(store.read_outbox_receipt(&command_id).await?)
        }
        Request::OutboxGet { effect_id } => {
            serde_json::to_value(store.outbox_get(&effect_id).await?)
        }
        Request::OutboxList { command_id } => {
            serde_json::to_value(store.outbox_list_by_command(&command_id).await?)
        }
        Request::OutboxEnqueue { effects, now } => {
            serde_json::to_value(store.outbox_enqueue(&effects, &now).await?)
        }
        Request::OutboxClaim {
            worker_id,
            now,
            exclude_restart_continuations,
        } => serde_json::to_value(
            store
                .outbox_claim_next(&outbox::Claim {
                    worker_id,
                    now,
                    exclude_restart_continuations,
                })
                .await?,
        ),
        Request::OutboxClaimOnce {
            stream_id,
            sequence,
            worker_id,
            now,
            exclude_restart_continuations,
        } => serde_json::to_value(
            store
                .outbox_claim_once(&outbox::ClaimOnce {
                    stream_id,
                    sequence,
                    claim: outbox::Claim {
                        worker_id,
                        now,
                        exclude_restart_continuations,
                    },
                })
                .await?,
        ),
        Request::OutboxNextClaimableAt {} => {
            serde_json::to_value(store.outbox_next_claimable_at().await?)
        }
        Request::OutboxSucceed {
            effect_id,
            worker_id,
            now,
        } => serde_json::to_value(
            store
                .outbox_succeed(&outbox::Settle {
                    effect_id,
                    worker_id,
                    now,
                })
                .await?,
        ),
        Request::OutboxRetry {
            effect_id,
            worker_id,
            now,
            available_at,
            error,
        } => serde_json::to_value(
            store
                .outbox_retry(&outbox::Retry {
                    effect_id,
                    worker_id,
                    now,
                    available_at,
                    error,
                })
                .await?,
        ),
        Request::OutboxFail {
            effect_id,
            worker_id,
            now,
            error,
        } => serde_json::to_value(
            store
                .outbox_fail(&outbox::Failure {
                    effect_id,
                    worker_id,
                    now,
                    error,
                })
                .await?,
        ),
        Request::OutboxCancel {
            thread_id,
            effect_types,
            reason,
            now,
        } => serde_json::to_value(
            store
                .outbox_cancel_unsettled(&outbox::CancelUnsettled {
                    thread_id,
                    effect_types,
                    reason,
                    now,
                })
                .await?,
        ),
        Request::OutboxReconcile { now } => {
            serde_json::to_value(store.outbox_reconcile(&outbox::Reconcile { now }).await?)
        }
        Request::OutboxPrune { before, limit } => {
            serde_json::to_value(store.outbox_prune(&outbox::Prune { before, limit }).await?)
        }
        Request::Events {
            after_sequence,
            limit,
        } => serde_json::to_value(store.events(after_sequence, limit).await?),
        Request::PendingEffects { limit } => {
            serde_json::to_value(store.pending_effects(limit).await?)
        }
        Request::Project { request } => {
            let planner = planner.ok_or_else(|| {
                Error::Invalid(
                    "T3_CODE_LS_PROJECT_STORE_EXECUTABLE is required for project commands".into(),
                )
            })?;
            serde_json::to_value(execute_project(store, planner, request).await?)
        }
        Request::ArchiveSqlite { path } => serde_json::to_value(
            store
                .archive_sqlite_export(path, ImportLimits::default())
                .await?,
        ),
        Request::SqliteArchive { archive_id } => {
            serde_json::to_value(store.sqlite_archive(&archive_id).await?)
        }
        Request::RestoreSqliteExport {
            archive_id,
            destination,
        } => serde_json::to_value(
            store
                .restore_sqlite_export(&archive_id, destination)
                .await?,
        ),
    }
    .map_err(|_| Error::Database("response serialization failed".into()))
}

fn unique_keys(value: &core_json::Value) -> bool {
    match value {
        core_json::Value::Object(entries) => {
            let mut names = HashSet::new();
            entries
                .iter()
                .all(|(key, value)| names.insert(key) && unique_keys(value))
        }
        core_json::Value::Array(values) => values.iter().all(unique_keys),
        _ => true,
    }
}

fn decode_frame(frame: &[u8]) -> Result<Request, Error> {
    let malformed = || Error::Invalid("malformed database request".into());
    let text = std::str::from_utf8(frame).map_err(|_| malformed())?;
    let text = text.strip_suffix('\n').ok_or_else(malformed)?;
    if text.contains('\r') || text.contains('\n') {
        return Err(malformed());
    }
    let value = core_json::parse_with_limits(
        text,
        core_json::Limits {
            max_bytes: MAX_BATCH_BYTES,
            max_depth: 64,
            max_values: 262_144,
        },
    )
    .map_err(|_| malformed())?;
    if !unique_keys(&value) {
        return Err(malformed());
    }
    serde_json::from_str(text).map_err(|_| malformed())
}

fn error_response(error: &Error) -> Value {
    let kind = match error {
        Error::Invalid(_) | Error::ResourceLimit(_) => "invalid_request",
        Error::IncompatibleStore => "incompatible_store",
        Error::StaleRevision { .. } => "stale_revision",
        Error::CommandConflict => "command_conflict",
        Error::DuplicateRecord => "duplicate_record",
        Error::TransactionConflict => "transaction_conflict",
        Error::WriterBusy => "writer_busy",
        Error::UnknownCommit => "unknown_commit",
        Error::Database(_) => "database_error",
    };
    json!({"ok": false, "error": {"kind": kind, "message": error.to_string()}})
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let store = match args.as_slice() {
        [mode, path] if mode == "initialize-local" => Store::initialize_local(path).await?,
        [mode, path] if mode == "local" => Store::open_local(path).await?,
        _ => {
            return Err(Error::Invalid(
                "usage: t3-code-ls-db initialize-local PATH | local PATH".into(),
            )
            .into());
        }
    };
    let planner = std::env::var_os("T3_CODE_LS_PROJECT_STORE_EXECUTABLE").map(PathBuf::from);
    let mut transactions = ApplicationTransactions::new(store.clone());
    let stdin = io::stdin();
    let stdout = io::stdout();
    let mut input = stdin.lock();
    let mut output = stdout.lock();
    loop {
        let mut frame = Vec::new();
        let count = input
            .by_ref()
            .take((MAX_BATCH_BYTES + 1) as u64)
            .read_until(b'\n', &mut frame)?;
        if count == 0 {
            break;
        }
        if count > MAX_BATCH_BYTES {
            serde_json::to_writer(
                &mut output,
                &error_response(&Error::Invalid("frame exceeds 16 MiB".into())),
            )?;
            writeln!(output)?;
            output.flush()?;
            return Err(Error::Invalid("frame exceeds 16 MiB".into()).into());
        }
        let response = match decode_frame(&frame) {
            Ok(request) => {
                match evaluate(&store, &mut transactions, planner.as_deref(), request).await {
                    Ok(result) => json!({"ok": true, "result": result}),
                    Err(error) => error_response(&error),
                }
            }
            Err(error) => {
                transactions.discard_all();
                error_response(&error)
            }
        };
        serde_json::to_writer(&mut output, &response)?;
        writeln!(output)?;
        output.flush()?;
    }
    drop(transactions);
    store.shutdown().await?;
    Ok(())
}

#[tokio::main(worker_threads = 2)]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("{error}");
        std::process::exit(1);
    }
}
