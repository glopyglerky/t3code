use std::collections::HashSet;
use std::io::{self, BufRead, Read, Write};
use std::path::{Path, PathBuf};

use serde::Deserialize;
use serde_json::{Value, json};
use t3_code_ls_core::json as core_json;
use t3_code_ls_storage::outbox::{self, EnqueueEffect};
use t3_code_ls_storage::project_service::{ProjectRequest, execute_project};
use t3_code_ls_storage::sqlite_import::ImportLimits;
use t3_code_ls_storage::{CommitBatch, Error, MAX_BATCH_BYTES, OutboxBatch, Store};

#[derive(Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
enum Request {
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

async fn evaluate(store: &Store, planner: Option<&Path>, request: Request) -> Result<Value, Error> {
    match request {
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
        Error::Invalid(_) => "invalid_request",
        Error::IncompatibleStore => "incompatible_store",
        Error::StaleRevision { .. } => "stale_revision",
        Error::CommandConflict => "command_conflict",
        Error::DuplicateRecord => "duplicate_record",
        Error::TransactionConflict => "transaction_conflict",
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
            Ok(request) => match evaluate(&store, planner.as_deref(), request).await {
                Ok(result) => json!({"ok": true, "result": result}),
                Err(error) => error_response(&error),
            },
            Err(error) => error_response(&error),
        };
        serde_json::to_writer(&mut output, &response)?;
        writeln!(output)?;
        output.flush()?;
    }
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
