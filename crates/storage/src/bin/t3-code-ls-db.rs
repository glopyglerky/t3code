use std::collections::HashSet;
use std::io::{self, BufRead, Read, Write};
use std::path::{Path, PathBuf};

use serde::Deserialize;
use serde_json::{Map, Value, json};
use t3_code_ls_core::json as core_json;
use t3_code_ls_storage::application::{
    ApplicationSchema, ApplicationTransactionMode, ApplicationTransactions,
};
use t3_code_ls_storage::application_auth::AuthOperation;
use t3_code_ls_storage::application_auxiliary_reads::AuxiliaryRead;
use t3_code_ls_storage::application_events::{
    EventAppendRow, EventLatest, EventRead, EventStats, MaintenanceRead, MaintenanceWrite,
};
use t3_code_ls_storage::application_legacy_v1::{LegacyV1Read, LegacyV1Write};
use t3_code_ls_storage::application_peers::PeerOperation;
use t3_code_ls_storage::application_positions::{PositionAllocate, PositionPlacement};
use t3_code_ls_storage::application_projections::records::ProjectionRecordsQuery;
use t3_code_ls_storage::application_projections::records::window::ProjectionWindowQuery;
use t3_code_ls_storage::application_projections::{
    ProjectionApply, ProjectionContextQuery, ProjectionControlQuery, ProjectionGuard,
    ProjectionHistoryQuery, ProjectionLookup, ProjectionRead, ProjectionRecoveryQuery,
    ProjectionShellQuery,
};
use t3_code_ls_storage::application_projects::{ProjectRecord, ProjectSelection};
use t3_code_ls_storage::application_pull_requests::{FileUpdate, GuideKey, ViewedScope};
use t3_code_ls_storage::application_receipts::ReceiptRow;
use t3_code_ls_storage::application_runtime_state::RuntimeStateOperation;
use t3_code_ls_storage::application_scheduled::{ScheduledRead, ScheduledWrite};
use t3_code_ls_storage::application_search::ThreadSearchQuery;
use t3_code_ls_storage::application_thread_find::ThreadFindQuery;
use t3_code_ls_storage::outbox::{self, EnqueueEffect};
use t3_code_ls_storage::project_service::{ProjectRequest, execute_project};
use t3_code_ls_storage::sqlite_import::ImportLimits;
use t3_code_ls_storage::{CommitBatch, Error, MAX_BATCH_BYTES, OutboxBatch, Store};

#[derive(Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
enum Request {
    Shutdown {},
    ApplicationProjectionsContext {
        transaction_id: String,
        query: ProjectionContextQuery,
    },
    ApplicationProjectionsRecovery {
        transaction_id: String,
        query: ProjectionRecoveryQuery,
    },
    ApplicationProjectionsShell {
        transaction_id: String,
        query: ProjectionShellQuery,
    },
    ApplicationThreadSearchRead {
        transaction_id: String,
        query: ThreadSearchQuery,
    },
    ApplicationBootstrap {},
    ApplicationMigrations {},
    ApplicationProjectionWindowQuery {
        transaction_id: String,
        query: ProjectionWindowQuery,
    },
    StorageStatistics {},
    StorageBackup {
        destination: PathBuf,
    },
    StorageCompact {},
    ApplicationMessageRowidsPrepare {},
    ApplicationLegacyV1Prepare {},
    ApplicationLegacyV1Read {
        transaction_id: String,
        query: LegacyV1Read,
    },
    ApplicationLegacyV1Write {
        transaction_id: String,
        mutation: LegacyV1Write,
    },
    ApplicationMaintenancePrepare {},
    ApplicationMaintenanceRead {
        transaction_id: String,
        query: MaintenanceRead,
    },
    ApplicationMaintenanceWrite {
        transaction_id: String,
        mutation: MaintenanceWrite,
    },
    ApplicationThreadFindRead {
        transaction_id: String,
        query: ThreadFindQuery,
    },
    ApplicationProjectionsControl {
        transaction_id: String,
        query: ProjectionControlQuery,
    },
    ApplicationAuxiliaryPrepare {},
    ApplicationThreadSearchPrepare {},
    ApplicationAuxiliaryRead {
        transaction_id: String,
        query: AuxiliaryRead,
    },
    ApplicationProjectionsHistory {
        transaction_id: String,
        query: ProjectionHistoryQuery,
    },
    ApplicationProjectionReadsPrepare {},
    ApplicationProjectionsLookup {
        transaction_id: String,
        query: ProjectionLookup,
    },
    ApplicationProjectionsRecords {
        transaction_id: String,
        query: ProjectionRecordsQuery,
    },
    ApplicationScheduledPrepare {},
    ApplicationScheduledRead {
        transaction_id: String,
        query: ScheduledRead,
    },
    ApplicationScheduledWrite {
        transaction_id: String,
        mutation: ScheduledWrite,
    },
    ApplicationAuthPrepare {},
    ApplicationAuthRead {
        transaction_id: String,
        request: AuthOperation,
    },
    ApplicationAuthWrite {
        transaction_id: String,
        request: AuthOperation,
    },
    ApplicationPeersPrepare {},
    ApplicationPeersRead {
        transaction_id: String,
        request: PeerOperation,
    },
    ApplicationPeersWrite {
        transaction_id: String,
        request: PeerOperation,
    },
    ApplicationRuntimeStatePrepare {},
    ApplicationRuntimeStateRead {
        transaction_id: String,
        request: RuntimeStateOperation,
    },
    ApplicationRuntimeStateWrite {
        transaction_id: String,
        request: RuntimeStateOperation,
    },
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
    ApplicationReceiptsPrepare {},
    ApplicationReceiptsInsert {
        transaction_id: String,
        receipt: ReceiptRow,
    },
    ApplicationReceiptsUpsert {
        transaction_id: String,
        receipt: ReceiptRow,
    },
    ApplicationReceiptsGet {
        transaction_id: String,
        command_id: String,
    },
    ApplicationPositionsPrepare {},
    ApplicationPositionsAllocate {
        transaction_id: String,
        thread_id: String,
        turn_item_id: String,
        run_id: Option<String>,
        run_ordinal: Option<i64>,
        placement: Option<PositionPlacement>,
    },
    ApplicationProjectsPrepare {},
    ApplicationProjectsSelect {
        transaction_id: String,
        project_id: Option<String>,
        project_ids: Option<Vec<String>>,
        workspace_root: Option<String>,
        include_deleted: bool,
    },
    ApplicationProjectsUpsert {
        transaction_id: String,
        row: ProjectRecord,
    },
    ApplicationProjectsReduceEvent {
        transaction_id: String,
        existing_json: Option<String>,
        event_json: String,
    },
    ApplicationViewedFilesPrepare {},
    ApplicationGuideGet {
        transaction_id: String,
        key: GuideKey,
    },
    ApplicationGuidePut {
        transaction_id: String,
        key: GuideKey,
        guide_json: String,
    },
    ApplicationViewedFilesList {
        transaction_id: String,
        scope: ViewedScope,
    },
    ApplicationViewedFilesSet {
        transaction_id: String,
        scope: ViewedScope,
        files: Vec<FileUpdate>,
        viewed_at: String,
    },
    ApplicationProjectionsPrepare {},
    ApplicationProjectionsApply {
        transaction_id: String,
        event: ProjectionApply,
    },
    ApplicationProjectionsNormalization {
        transaction_id: String,
        event: ProjectionApply,
    },
    ApplicationProjectionsRead {
        transaction_id: String,
        read: ProjectionRead,
    },
    ApplicationProjectionsGuard {
        transaction_id: String,
        guard: ProjectionGuard,
    },
    ApplicationProjectionsMarkApplied {
        transaction_id: String,
        sequence: u64,
        updated_at: String,
    },
    ApplicationOutboxPrepare {},
    ApplicationOutboxEnqueue {
        transaction_id: String,
        effects: Vec<EnqueueEffect>,
        now: String,
    },
    ApplicationOutboxGet {
        transaction_id: String,
        effect_id: String,
    },
    ApplicationOutboxList {
        transaction_id: String,
        command_id: String,
    },
    ApplicationOutboxCancel {
        transaction_id: String,
        thread_id: String,
        effect_types: Vec<outbox::EffectType>,
        reason: String,
        now: String,
    },
    ApplicationOutboxReconcile {
        transaction_id: String,
        now: String,
    },
    ApplicationOutboxClaim {
        transaction_id: String,
        worker_id: String,
        now: String,
        #[serde(default)]
        exclude_restart_continuations: bool,
        #[serde(default)]
        max_running_title_generations: Option<f64>,
    },
    ApplicationOutboxClaimOnce {
        transaction_id: String,
        stream_id: String,
        sequence: u64,
        worker_id: String,
        now: String,
        exclude_restart_continuations: bool,
        #[serde(default)]
        max_running_title_generations: Option<f64>,
    },
    ApplicationOutboxNextClaimableAt {
        transaction_id: String,
        max_running_title_generations: Option<f64>,
    },
    ApplicationOutboxSucceed {
        transaction_id: String,
        effect_id: String,
        worker_id: String,
        now: String,
    },
    ApplicationOutboxRetry {
        transaction_id: String,
        effect_id: String,
        worker_id: String,
        now: String,
        available_at: String,
        error: String,
    },
    ApplicationOutboxFail {
        transaction_id: String,
        effect_id: String,
        worker_id: String,
        now: String,
        error: String,
    },
    ApplicationOutboxPrune {
        transaction_id: String,
        before: String,
        limit: u32,
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
        #[serde(default)]
        max_running_title_generations: Option<f64>,
    },
    OutboxClaimOnce {
        stream_id: String,
        sequence: u64,
        worker_id: String,
        now: String,
        exclude_restart_continuations: bool,
        #[serde(default)]
        max_running_title_generations: Option<f64>,
    },
    OutboxNextClaimableAt {
        max_running_title_generations: Option<f64>,
    },
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
            | Self::ApplicationAuthPrepare { .. }
            | Self::ApplicationPeersPrepare { .. }
            | Self::ApplicationRuntimeStatePrepare { .. }
            | Self::ApplicationScheduledPrepare { .. }
            | Self::ApplicationProjectionReadsPrepare { .. }
            | Self::ApplicationAuxiliaryPrepare { .. }
            | Self::ApplicationMessageRowidsPrepare { .. }
            | Self::ApplicationThreadSearchPrepare { .. }
            | Self::ApplicationLegacyV1Prepare { .. }
            | Self::ApplicationMaintenancePrepare { .. }
            | Self::StorageCompact { .. }
            | Self::ApplicationBootstrap { .. }
            | Self::StorageBackup { .. }
            | Self::ApplicationEventsPrepare { .. }
            | Self::ApplicationReceiptsPrepare { .. }
            | Self::ApplicationPositionsPrepare { .. }
            | Self::ApplicationProjectsPrepare { .. }
            | Self::ApplicationViewedFilesPrepare { .. }
            | Self::ApplicationProjectionsPrepare { .. }
            | Self::ApplicationOutboxPrepare { .. }
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
            Self::Shutdown { .. }
            | Self::ApplicationSchema { .. }
            | Self::ApplicationProjectionsContext { .. }
            | Self::ApplicationProjectionsRecovery { .. }
            | Self::ApplicationProjectionsShell { .. }
            | Self::ApplicationThreadSearchRead { .. }
            | Self::ApplicationMigrations { .. }
            | Self::ApplicationProjectionWindowQuery { .. }
            | Self::StorageStatistics { .. }
            | Self::ApplicationLegacyV1Read { .. }
            | Self::ApplicationLegacyV1Write { .. }
            | Self::ApplicationMaintenanceRead { .. }
            | Self::ApplicationMaintenanceWrite { .. }
            | Self::ApplicationThreadFindRead { .. }
            | Self::ApplicationProjectionsControl { .. }
            | Self::ApplicationAuthRead { .. }
            | Self::ApplicationAuthWrite { .. }
            | Self::ApplicationPeersRead { .. }
            | Self::ApplicationPeersWrite { .. }
            | Self::ApplicationRuntimeStateRead { .. }
            | Self::ApplicationRuntimeStateWrite { .. }
            | Self::ApplicationScheduledRead { .. }
            | Self::ApplicationScheduledWrite { .. }
            | Self::ApplicationProjectionsLookup { .. }
            | Self::ApplicationProjectionsRecords { .. }
            | Self::ApplicationAuxiliaryRead { .. }
            | Self::ApplicationProjectionsHistory { .. }
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
            | Self::ApplicationReceiptsInsert { .. }
            | Self::ApplicationReceiptsUpsert { .. }
            | Self::ApplicationReceiptsGet { .. }
            | Self::ApplicationPositionsAllocate { .. }
            | Self::ApplicationProjectsSelect { .. }
            | Self::ApplicationProjectsUpsert { .. }
            | Self::ApplicationProjectsReduceEvent { .. }
            | Self::ApplicationGuideGet { .. }
            | Self::ApplicationGuidePut { .. }
            | Self::ApplicationViewedFilesList { .. }
            | Self::ApplicationViewedFilesSet { .. }
            | Self::ApplicationProjectionsApply { .. }
            | Self::ApplicationProjectionsNormalization { .. }
            | Self::ApplicationProjectionsRead { .. }
            | Self::ApplicationProjectionsGuard { .. }
            | Self::ApplicationProjectionsMarkApplied { .. }
            | Self::ApplicationOutboxEnqueue { .. }
            | Self::ApplicationOutboxGet { .. }
            | Self::ApplicationOutboxList { .. }
            | Self::ApplicationOutboxCancel { .. }
            | Self::ApplicationOutboxReconcile { .. }
            | Self::ApplicationOutboxClaim { .. }
            | Self::ApplicationOutboxClaimOnce { .. }
            | Self::ApplicationOutboxNextClaimableAt { .. }
            | Self::ApplicationOutboxSucceed { .. }
            | Self::ApplicationOutboxRetry { .. }
            | Self::ApplicationOutboxFail { .. }
            | Self::ApplicationOutboxPrune { .. }
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
        Request::Shutdown {} => {
            return Err(Error::Invalid(
                "shutdown must be handled by the helper owner".into(),
            ));
        }
        Request::ApplicationProjectionsContext {
            transaction_id,
            query,
        } => Ok(transactions
            .projections_context(&transaction_id, query)
            .await?),
        Request::ApplicationProjectionsRecovery {
            transaction_id,
            query,
        } => Ok(transactions
            .projections_recovery(&transaction_id, query)
            .await?),
        Request::ApplicationProjectionsShell {
            transaction_id,
            query,
        } => Ok(transactions
            .projections_shell(&transaction_id, query)
            .await?),
        Request::ApplicationThreadSearchRead {
            transaction_id,
            query,
        } => Ok(transactions.thread_search(&transaction_id, query).await?),
        Request::ApplicationBootstrap {} => {
            serde_json::to_value(store.bootstrap_application().await?)
        }
        Request::ApplicationMigrations {} => {
            serde_json::to_value(store.application_migrations().await?)
        }
        Request::ApplicationProjectionWindowQuery {
            transaction_id,
            query,
        } => Ok(transactions
            .projection_window_query(&transaction_id, query)
            .await?),
        Request::StorageStatistics {} => serde_json::to_value(store.storage_statistics().await?),
        Request::StorageBackup { destination } => {
            serde_json::to_value(store.storage_backup(destination).await?)
        }
        Request::StorageCompact {} => serde_json::to_value(store.storage_compact().await?),
        Request::ApplicationMessageRowidsPrepare {} => {
            serde_json::to_value(store.prepare_application_message_rowids().await?)
        }
        Request::ApplicationThreadSearchPrepare {} => {
            serde_json::to_value(store.prepare_application_search().await?)
        }
        Request::ApplicationLegacyV1Prepare {} => {
            serde_json::to_value(store.prepare_application_legacy_v1().await?)
        }
        Request::ApplicationLegacyV1Read {
            transaction_id,
            query,
        } => Ok(transactions.legacy_v1_read(&transaction_id, query).await?),
        Request::ApplicationLegacyV1Write {
            transaction_id,
            mutation,
        } => Ok(transactions
            .legacy_v1_write(&transaction_id, mutation)
            .await?),
        Request::ApplicationMaintenancePrepare {} => {
            serde_json::to_value(store.prepare_application_maintenance().await?)
        }
        Request::ApplicationMaintenanceRead {
            transaction_id,
            query,
        } => Ok(transactions
            .maintenance_read(&transaction_id, query)
            .await?),
        Request::ApplicationMaintenanceWrite {
            transaction_id,
            mutation,
        } => Ok(transactions
            .maintenance_write(&transaction_id, mutation)
            .await?),
        Request::ApplicationThreadFindRead {
            transaction_id,
            query,
        } => Ok(transactions.thread_find(&transaction_id, query).await?),
        Request::ApplicationProjectionsControl {
            transaction_id,
            query,
        } => Ok(transactions
            .projections_control(&transaction_id, query)
            .await?),
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
        Request::ApplicationReceiptsPrepare {} => {
            serde_json::to_value(store.prepare_application_receipts().await?)
        }
        Request::ApplicationReceiptsInsert {
            transaction_id,
            receipt,
        } => Ok(json!({"inserted": transactions.receipts_insert(&transaction_id, receipt).await?})),
        Request::ApplicationReceiptsUpsert {
            transaction_id,
            receipt,
        } => {
            transactions
                .receipts_upsert(&transaction_id, receipt)
                .await?;
            Ok(json!({"written":true}))
        }
        Request::ApplicationReceiptsGet {
            transaction_id,
            command_id,
        } => serde_json::to_value(
            transactions
                .receipts_get(&transaction_id, &command_id)
                .await?,
        ),
        Request::ApplicationPositionsPrepare {} => {
            serde_json::to_value(store.prepare_application_positions().await?)
        }
        Request::ApplicationPositionsAllocate {
            transaction_id,
            thread_id,
            turn_item_id,
            run_id,
            run_ordinal,
            placement,
        } => serde_json::to_value(
            transactions
                .positions_allocate(
                    &transaction_id,
                    PositionAllocate {
                        thread_id,
                        turn_item_id,
                        run_id,
                        run_ordinal,
                        placement,
                    },
                )
                .await?,
        ),
        Request::ApplicationProjectsPrepare {} => {
            serde_json::to_value(store.prepare_application_projects().await?)
        }
        Request::ApplicationProjectsSelect {
            transaction_id,
            project_id,
            project_ids,
            workspace_root,
            include_deleted,
        } => serde_json::to_value(
            transactions
                .projects_select(
                    &transaction_id,
                    ProjectSelection {
                        project_id,
                        project_ids,
                        workspace_root,
                        include_deleted,
                    },
                )
                .await?,
        ),
        Request::ApplicationProjectsUpsert {
            transaction_id,
            row,
        } => {
            transactions.projects_upsert(&transaction_id, row).await?;
            Ok(json!({"written":true}))
        }
        Request::ApplicationProjectsReduceEvent {
            transaction_id,
            existing_json,
            event_json,
        } => serde_json::to_value(
            transactions
                .projects_reduce_event(&transaction_id, existing_json.as_deref(), &event_json)
                .await?,
        ),
        Request::ApplicationViewedFilesPrepare {} => {
            serde_json::to_value(store.prepare_application_viewed_files().await?)
        }
        Request::ApplicationAuthPrepare {} => {
            serde_json::to_value(store.prepare_application_auth().await?)
        }
        Request::ApplicationScheduledPrepare {} => {
            serde_json::to_value(store.prepare_application_scheduled().await?)
        }
        Request::ApplicationProjectionReadsPrepare {} => {
            serde_json::to_value(store.prepare_application_projection_reads().await?)
        }
        Request::ApplicationAuxiliaryPrepare {} => {
            serde_json::to_value(store.prepare_application_auxiliary().await?)
        }
        Request::ApplicationAuxiliaryRead {
            transaction_id,
            query,
        } => Ok(transactions.auxiliary_read(&transaction_id, query).await?),
        Request::ApplicationProjectionsHistory {
            transaction_id,
            query,
        } => Ok(transactions
            .projections_history(&transaction_id, query)
            .await?),
        Request::ApplicationProjectionsLookup {
            transaction_id,
            query,
        } => Ok(transactions
            .projections_lookup(&transaction_id, query)
            .await?),
        Request::ApplicationProjectionsRecords {
            transaction_id,
            query,
        } => serde_json::to_value(
            transactions
                .projection_records(&transaction_id, query)
                .await?,
        ),
        Request::ApplicationScheduledRead {
            transaction_id,
            query,
        } => Ok(transactions.scheduled_read(&transaction_id, query).await?),
        Request::ApplicationScheduledWrite {
            transaction_id,
            mutation,
        } => Ok(transactions
            .scheduled_write(&transaction_id, mutation)
            .await?),
        Request::ApplicationAuthRead {
            transaction_id,
            request,
        } => Ok(transactions.auth_read(&transaction_id, request).await?),
        Request::ApplicationAuthWrite {
            transaction_id,
            request,
        } => Ok(transactions.auth_write(&transaction_id, request).await?),
        Request::ApplicationPeersPrepare {} => {
            serde_json::to_value(store.prepare_application_peers().await?)
        }
        Request::ApplicationPeersRead {
            transaction_id,
            request,
        } => {
            if request.is_write() {
                return Err(Error::Invalid(
                    "write operation requires the peer writer route".into(),
                ));
            }
            Ok(transactions.peers(&transaction_id, request).await?)
        }
        Request::ApplicationPeersWrite {
            transaction_id,
            request,
        } => {
            if !request.is_write() {
                return Err(Error::Invalid(
                    "read operation requires the peer reader route".into(),
                ));
            }
            Ok(transactions.peers(&transaction_id, request).await?)
        }
        Request::ApplicationRuntimeStatePrepare {} => {
            serde_json::to_value(store.prepare_application_runtime_state().await?)
        }
        Request::ApplicationRuntimeStateRead {
            transaction_id,
            request,
        } => {
            if request.is_write() {
                return Err(Error::Invalid(
                    "write operation requires the runtime writer route".into(),
                ));
            }
            Ok(transactions.runtime_state(&transaction_id, request).await?)
        }
        Request::ApplicationRuntimeStateWrite {
            transaction_id,
            request,
        } => {
            if !request.is_write() {
                return Err(Error::Invalid(
                    "read operation requires the runtime reader route".into(),
                ));
            }
            Ok(transactions.runtime_state(&transaction_id, request).await?)
        }
        Request::ApplicationGuideGet {
            transaction_id,
            key,
        } => serde_json::to_value(transactions.guide_get(&transaction_id, key).await?),
        Request::ApplicationGuidePut {
            transaction_id,
            key,
            guide_json,
        } => {
            transactions
                .guide_put(&transaction_id, key, guide_json)
                .await?;
            Ok(json!({"written":true}))
        }
        Request::ApplicationViewedFilesList {
            transaction_id,
            scope,
        } => serde_json::to_value(
            transactions
                .viewed_files_list(&transaction_id, scope)
                .await?,
        ),
        Request::ApplicationViewedFilesSet {
            transaction_id,
            scope,
            files,
            viewed_at,
        } => {
            transactions
                .viewed_files_set(&transaction_id, scope, files, viewed_at)
                .await?;
            Ok(json!({"written":true}))
        }
        Request::ApplicationProjectionsPrepare {} => {
            serde_json::to_value(store.prepare_application_projections().await?)
        }
        Request::ApplicationProjectionsNormalization {
            transaction_id,
            event,
        } => serde_json::to_value(
            transactions
                .projections_normalization(&transaction_id, event)
                .await?,
        ),
        Request::ApplicationProjectionsApply {
            transaction_id,
            event,
        } => {
            transactions
                .projections_apply(&transaction_id, event)
                .await?;
            Ok(Value::Null)
        }
        Request::ApplicationProjectionsRead {
            transaction_id,
            read,
        } => serde_json::to_value(transactions.projections_read(&transaction_id, read).await?),
        Request::ApplicationProjectionsGuard {
            transaction_id,
            guard,
        } => serde_json::to_value(
            transactions
                .projections_guard(&transaction_id, guard)
                .await?,
        ),
        Request::ApplicationProjectionsMarkApplied {
            transaction_id,
            sequence,
            updated_at,
        } => {
            transactions
                .projections_mark_applied(&transaction_id, sequence, updated_at)
                .await?;
            Ok(Value::Null)
        }
        Request::ApplicationOutboxPrepare {} => {
            serde_json::to_value(store.prepare_application_outbox().await?)
        }
        Request::ApplicationOutboxEnqueue {
            transaction_id,
            effects,
            now,
        } => serde_json::to_value(
            transactions
                .outbox_enqueue(&transaction_id, &effects, &now)
                .await?,
        ),
        Request::ApplicationOutboxGet {
            transaction_id,
            effect_id,
        } => serde_json::to_value(transactions.outbox_get(&transaction_id, &effect_id).await?),
        Request::ApplicationOutboxList {
            transaction_id,
            command_id,
        } => serde_json::to_value(
            transactions
                .outbox_list(&transaction_id, &command_id)
                .await?,
        ),
        Request::ApplicationOutboxCancel {
            transaction_id,
            thread_id,
            effect_types,
            reason,
            now,
        } => serde_json::to_value(
            transactions
                .outbox_cancel(
                    &transaction_id,
                    &outbox::CancelUnsettled {
                        thread_id,
                        effect_types,
                        reason,
                        now,
                    },
                )
                .await?,
        ),
        Request::ApplicationOutboxReconcile {
            transaction_id,
            now,
        } => serde_json::to_value(
            transactions
                .outbox_reconcile(&transaction_id, &outbox::Reconcile { now })
                .await?,
        ),
        Request::ApplicationOutboxClaim {
            transaction_id,
            worker_id,
            now,
            exclude_restart_continuations,
            max_running_title_generations,
        } => serde_json::to_value(
            transactions
                .outbox_claim(
                    &transaction_id,
                    &outbox::Claim {
                        worker_id,
                        now,
                        exclude_restart_continuations,
                        max_running_title_generations,
                    },
                )
                .await?,
        ),
        Request::ApplicationOutboxClaimOnce {
            transaction_id,
            stream_id,
            sequence,
            worker_id,
            now,
            exclude_restart_continuations,
            max_running_title_generations,
        } => serde_json::to_value(
            transactions
                .outbox_claim_once(
                    &transaction_id,
                    &outbox::ClaimOnce {
                        stream_id,
                        sequence,
                        claim: outbox::Claim {
                            worker_id,
                            now,
                            exclude_restart_continuations,
                            max_running_title_generations,
                        },
                    },
                )
                .await?,
        ),
        Request::ApplicationOutboxNextClaimableAt {
            transaction_id,
            max_running_title_generations,
        } => serde_json::to_value(
            transactions
                .outbox_next_claimable_at_limited(&transaction_id, max_running_title_generations)
                .await?,
        ),
        Request::ApplicationOutboxSucceed {
            transaction_id,
            effect_id,
            worker_id,
            now,
        } => serde_json::to_value(
            transactions
                .outbox_succeed(
                    &transaction_id,
                    &outbox::Settle {
                        effect_id,
                        worker_id,
                        now,
                    },
                )
                .await?,
        ),
        Request::ApplicationOutboxRetry {
            transaction_id,
            effect_id,
            worker_id,
            now,
            available_at,
            error,
        } => serde_json::to_value(
            transactions
                .outbox_retry(
                    &transaction_id,
                    &outbox::Retry {
                        effect_id,
                        worker_id,
                        now,
                        available_at,
                        error,
                    },
                )
                .await?,
        ),
        Request::ApplicationOutboxFail {
            transaction_id,
            effect_id,
            worker_id,
            now,
            error,
        } => serde_json::to_value(
            transactions
                .outbox_fail(
                    &transaction_id,
                    &outbox::Failure {
                        effect_id,
                        worker_id,
                        now,
                        error,
                    },
                )
                .await?,
        ),
        Request::ApplicationOutboxPrune {
            transaction_id,
            before,
            limit,
        } => serde_json::to_value(
            transactions
                .outbox_prune(&transaction_id, &outbox::Prune { before, limit })
                .await?,
        ),
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
            max_running_title_generations,
        } => serde_json::to_value(
            store
                .outbox_claim_next(&outbox::Claim {
                    worker_id,
                    now,
                    exclude_restart_continuations,
                    max_running_title_generations,
                })
                .await?,
        ),
        Request::OutboxClaimOnce {
            stream_id,
            sequence,
            worker_id,
            now,
            exclude_restart_continuations,
            max_running_title_generations,
        } => serde_json::to_value(
            store
                .outbox_claim_once(&outbox::ClaimOnce {
                    stream_id,
                    sequence,
                    claim: outbox::Claim {
                        worker_id,
                        now,
                        exclude_restart_continuations,
                        max_running_title_generations,
                    },
                })
                .await?,
        ),
        Request::OutboxNextClaimableAt {
            max_running_title_generations,
        } => serde_json::to_value(
            store
                .outbox_next_claimable_at_limited(max_running_title_generations)
                .await?,
        ),
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
    if let [mode, path] = args.as_slice() {
        if mode == "owner-lock" {
            return t3_code_ls_storage::application_server_ownership::run(Path::new(path))
                .map_err(Into::into);
        }
    }
    let opened = match args.as_slice() {
        [mode, path] if mode == "initialize-local" => Store::initialize_local(path).await,
        [mode, path] if mode == "local" => Store::open_local(path).await,
        [mode, path] if mode == "read-only-local" => Store::open_read_only(path).await,
        [mode, path] if mode == "exclusive-local" => Store::open_exclusive(path).await,
        _ => {
            return Err(Error::Invalid(
                "usage: t3-code-ls-db initialize-local PATH | local PATH | read-only-local PATH | exclusive-local PATH | owner-lock PATH".into(),
            )
            .into());
        }
    };
    let store = match opened {
        Ok(store) => store,
        Err(error) => {
            // Never emit an unsolicited response. A failed open cannot have
            // executed the caller's request, and a proven lock collision must
            // remain writer_busy rather than an uncertain transport exit.
            let mut frame = Vec::new();
            let count = io::stdin()
                .lock()
                .take((MAX_BATCH_BYTES + 1) as u64)
                .read_until(b'\n', &mut frame)?;
            if count != 0 {
                let response = if count > MAX_BATCH_BYTES {
                    error_response(&Error::Invalid("frame exceeds 16 MiB".into()))
                } else {
                    match decode_frame(&frame) {
                        Ok(_) => error_response(&error),
                        Err(error) => error_response(&error),
                    }
                };
                let mut output = io::stdout().lock();
                serde_json::to_writer(&mut output, &response)?;
                writeln!(output)?;
                output.flush()?;
            }
            return Ok(());
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
            Ok(Request::Shutdown {}) => {
                // Abort any leftover uncommitted transactions, then drop the
                // database handles before acknowledging a clean shutdown.
                drop(transactions);
                let response = match store.shutdown().await {
                    Ok(()) => json!({"ok": true, "result": null}),
                    Err(error) => error_response(&error),
                };
                serde_json::to_writer(&mut output, &response)?;
                writeln!(output)?;
                output.flush()?;
                return Ok(());
            }
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

fn main() {
    let owned = t3_code_ls_storage::run_on_database_thread(|| {
        let runtime = match tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
        {
            Ok(runtime) => runtime,
            Err(error) => {
                eprintln!("{error}");
                std::process::exit(1);
            }
        };
        runtime.block_on(async {
            if let Err(error) = run().await {
                eprintln!("{error}");
                std::process::exit(1);
            }
        });
    });
    if let Err(error) = owned {
        eprintln!("{error}");
        std::process::exit(1);
    }
}
