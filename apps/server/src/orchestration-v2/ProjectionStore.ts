import type { ProjectionAuditTable, ProjectionRecoveryRows } from "./ProjectionRecoveryRows.ts";
import * as SqliteProjectionRecoveryRows from "./SqliteProjectionRecoveryRows.ts";
import type { ProjectThreadAwarenessV2Input } from "@t3tools/shared/agentAwareness";
import { pendingGuestMessageCount } from "@t3tools/contracts";
import { applyThreadPullRequestLinkUpdate } from "../sourceControl/threadPullRequests.ts";
import { projectTurnItemForWire } from "./WireProjection.ts";
import * as Stream from "effect/Stream";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import { makeThreadFind } from "./ThreadFind.ts";
import type { ProjectionLookupRows } from "./ProjectionLookupRows.ts";
import * as SqliteProjectionLookupRows from "./SqliteProjectionLookupRows.ts";
import {
  projectionRecordFields,
  ProjectionRecordRowsError,
  type ProjectionRecordRows,
  type ProjectionRecordWindow,
} from "./ProjectionRecordRows.ts";
import type {
  ProjectionAttachmentOwner,
  ProjectionHistoryRows,
  ProjectionMaterializedRow,
  ProjectionSnapshotRows,
  ProjectionTimelineRow,
  ProjectionTimelineRows,
} from "./ProjectionHistoryRows.ts";
import * as SqliteProjectionRecordRows from "./SqliteProjectionRecordRows.ts";
import type { ProjectionControlRows } from "./ProjectionControlRows.ts";
import * as SqliteProjectionControlRows from "./SqliteProjectionControlRows.ts";
import * as ProjectionContextRows from "./ProjectionContextRows.ts";
import * as SqliteProjectionContextRows from "./SqliteProjectionContextRows.ts";
import type { ProjectionShellRows } from "./ProjectionShellRows.ts";
import * as SqliteProjectionShellRows from "./SqliteProjectionShellRows.ts";
import type { PersistenceTransactionError } from "../persistence/PersistenceTransactions.ts";
import type {
  OrchestrationV2ThreadLinkMetadata,
  OrchestrationV2SearchThreadInput,
  OrchestrationV2SearchThreadResult,
  OrchestrationV2ThreadHistoryPage,
} from "@t3tools/contracts";
import {
  latestRootProviderFailure,
  latestUnheldRun,
  threadErrorSummary,
  usageLimitRunPresentedAsLatest,
} from "@t3tools/shared/orchestrationV2ThreadError";
import { threadPullRequestsOf } from "../sourceControl/threadPullRequests.ts";
import type {
  OrchestrationV2AppThread,
  OrchestrationV2Checkpoint,
  OrchestrationV2CheckpointScope,
  OrchestrationV2ExecutionNode,
  OrchestrationV2PlanArtifact,
  OrchestrationV2PendingRuntimeRequestSummary,
  PlanId,
  OrchestrationV2ConversationMessage,
  OrchestrationV2ContextTransfer,
  OrchestrationV2DomainEvent,
  OrchestrationV2OlderSettledCount,
  OrchestrationV2OlderSettledCursor,
  OrchestrationV2ProjectedTurnItem,
  OrchestrationV2ProviderThread,
  OrchestrationV2ProviderTurn,
  OrchestrationV2Run,
  OrchestrationV2RuntimeRequest,
  OrchestrationV2Subagent,
  OrchestrationV2ThreadShellSnapshot,
  OrchestrationV2ShellThreadStatus,
  OrchestrationV2ThreadShell,
  OrchestrationV2ThreadProjection,
  OrchestrationV2TurnItem,
  GuestShareId,
  ProviderThreadId,
  ProviderTurnId,
  RunAttemptId,
} from "@t3tools/contracts";
import {
  IDLE_COMPACTION_TEXT,
  idleCompactionOf,
  OrchestrationV2AppThreadJson as OrchestrationV2AppThreadJsonSchema,
  OrchestrationV2CheckpointJson as OrchestrationV2CheckpointJsonSchema,
  OrchestrationV2CheckpointScopeJson as OrchestrationV2CheckpointScopeJsonSchema,
  OrchestrationV2ContextHandoffJson as OrchestrationV2ContextHandoffJsonSchema,
  OrchestrationV2ContextTransferJson as OrchestrationV2ContextTransferJsonSchema,
  OrchestrationV2ConversationMessageJson as OrchestrationV2ConversationMessageJsonSchema,
  OrchestrationV2ExecutionNodeJson as OrchestrationV2ExecutionNodeJsonSchema,
  OrchestrationV2PlanArtifact as OrchestrationV2PlanArtifactSchema,
  OrchestrationV2ProviderSessionJson as OrchestrationV2ProviderSessionJsonSchema,
  OrchestrationV2ProviderThreadJson as OrchestrationV2ProviderThreadJsonSchema,
  OrchestrationV2ProviderTurnJson as OrchestrationV2ProviderTurnJsonSchema,
  OrchestrationV2RunAttemptJson as OrchestrationV2RunAttemptJsonSchema,
  OrchestrationV2RunJson as OrchestrationV2RunJsonSchema,
  OrchestrationV2RuntimeRequestJson as OrchestrationV2RuntimeRequestJsonSchema,
  OrchestrationV2SubagentJson as OrchestrationV2SubagentJsonSchema,
  OrchestrationV2TurnItemJson as OrchestrationV2TurnItemJsonSchema,
  orchestrationV2RunWorkStartedAt,
  RunId,
  MessageId,
  RuntimeRequestId,
  CheckpointScopeId,
  CheckpointId,
  ThreadId,
  TurnItemId,
  NodeId,
  ProjectId,
  ProviderInstanceId,
} from "@t3tools/contracts";
import {
  createOrchestrationV2TurnItemVisibility,
  isOrchestrationV2SupersededInterrupt,
} from "@t3tools/shared/orchestrationV2Timeline";
import {
  deriveBlockedOnThreadWait,
  derivePendingBackgroundWork,
} from "@t3tools/shared/orchestrationV2PendingBackgroundWork";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import type * as Statement from "effect/sql/Statement";

import { MCP_APP_OUTPUT_KEY } from "@t3tools/shared/mcpApp";
import { threadHtmlRenderAttachmentIds } from "../attachmentStore.ts";
import {
  isThreadHistoryUserTurn,
  isConversationHistoryItem,
  THREAD_HISTORY_MAX_RAW_TURNS,
  missingInterruptRequests,
  deferWorkHistoryRows,
  selectDeferredWorkPage,
  encodeThreadHistoryCursor,
  olderHistoryPagePolicy,
  selectHistoryPageFromCursor,
  selectRecentTimelineWindow,
  THREAD_HISTORY_PAGE_POLICY,
  THREAD_HISTORY_INLINE_WORK_ITEM_LIMIT,
  THREAD_HISTORY_TURN_SNAPSHOT_ROW_LIMIT,
} from "./threadHistoryPaging.ts";

export class ProjectionStoreApplyEventError extends Schema.TaggedError<ProjectionStoreApplyEventError>()(
  "ProjectionStoreApplyEventError",
  {
    eventType: Schema.String,
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    return `Failed to apply orchestration projection event ${this.eventType}.`;
  }
}

export class ProjectionStoreSetupError extends Schema.TaggedError<ProjectionStoreSetupError>()(
  "ProjectionStoreSetupError",
  {
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    return "Failed to initialize orchestration projection store.";
  }
}

export class ProjectionStoreThreadNotFoundError extends Schema.TaggedError<ProjectionStoreThreadNotFoundError>()(
  "ProjectionStoreThreadNotFoundError",
  {
    threadId: ThreadId,
  },
) {
  override get message(): string {
    return `No orchestration projection exists for thread ${this.threadId}.`;
  }
}

export class ProjectionStoreReadError extends Schema.TaggedError<ProjectionStoreReadError>()(
  "ProjectionStoreReadError",
  {
    threadId: ThreadId,
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    return `Failed to read orchestration projection for thread ${this.threadId}.`;
  }
}

export const ProjectionStoreV2Error = Schema.Union([
  ProjectionStoreSetupError,
  ProjectionStoreApplyEventError,
  ProjectionStoreThreadNotFoundError,
  ProjectionStoreReadError,
]);
export type ProjectionStoreV2Error = typeof ProjectionStoreV2Error.Type;

export type ProjectionRecoveryKind =
  | "queued-runs"
  | "runtime"
  | "restart-continuations"
  | "subagent-results"
  | "delegated-completions";

export type ProjectionLimitRecoveryCandidate = Pick<
  OrchestrationV2ThreadShell,
  | "id"
  | "status"
  | "lastErrorClass"
  | "latestRunId"
  | "usageLimitResetAt"
  | "archivedAt"
  | "settledOverride"
  | "pendingRuntimeRequest"
  | "latestRunCompletedAt"
  | "updatedAt"
  | "limitRecovery"
  | "snoozedUntil"
>;

export type ProjectionThreadPullRequests = Pick<
  OrchestrationV2AppThread,
  "id" | "projectId" | "lineage" | "settledOverride" | "settledAt" | "pullRequests"
>;

export type ProjectionSettlementCandidate = Pick<
  OrchestrationV2ThreadShell,
  | "id"
  | "projectId"
  | "branch"
  | "worktreePath"
  | "pullRequests"
  | "linkedPullRequest"
  | "branchPullRequest"
  | "createdAt"
  | "updatedAt"
  | "archivedAt"
  | "settledOverride"
  | "pinnedAt"
  | "autoSettleDisabledAt"
  | "snoozedUntil"
  | "snoozedAt"
  | "latestRunId"
  | "latestRunRequestedAt"
  | "latestRunStartedAt"
  | "latestRunCompletedAt"
  | "latestUserMessageAt"
  | "status"
  | "activityRunStartedAt"
  | "activityRunStatus"
  | "pendingRuntimeRequest"
  | "pendingBackgroundTasks"
  | "pendingGuestMessageCount"
> &
  Pick<OrchestrationV2AppThread, "lastSnoozeWakeAt"> & {
    readonly latestUserAuthoredMessageAt: DateTime.Utc | null;
  };

const ProjectionCheckpointContext = Schema.Struct({
  runs: Schema.Array(
    OrchestrationV2RunJsonSchema.mapFields(({ id, ordinal, status }) => ({ id, ordinal, status })),
  ),
  checkpointScopes: Schema.Array(
    OrchestrationV2CheckpointScopeJsonSchema.mapFields(({ id, runId, kind, cwd }) => ({
      id,
      runId,
      kind,
      cwd,
    })),
  ),
  checkpoints: Schema.Array(
    OrchestrationV2CheckpointJsonSchema.mapFields(
      ({ scopeId, runId, ordinalWithinScope, appRunOrdinal, status, ref }) => ({
        scopeId,
        runId,
        ordinalWithinScope,
        appRunOrdinal,
        status,
        ref,
      }),
    ),
  ),
});
export type ProjectionCheckpointContext = typeof ProjectionCheckpointContext.Type;
const decodeCheckpointContext = Schema.decodeUnknownEffect(ProjectionCheckpointContext);

export interface ProjectionCheckpointCaptureContext {
  readonly run: OrchestrationV2Run | undefined;
  readonly rootNode: OrchestrationV2ExecutionNode | undefined;
  readonly scope: OrchestrationV2CheckpointScope | undefined;
  readonly providerThread: OrchestrationV2ProviderThread | undefined;
  readonly readyCheckpointOrdinals: ReadonlyArray<number>;
  /** Side chats share their parent's worktree and never snapshot it. */
  readonly sideChat: boolean;
}

const RollbackTargetAttempts = Schema.Array(
  OrchestrationV2RunAttemptJsonSchema.mapFields(({ id, providerTurnId }) => ({
    id,
    providerTurnId,
  })),
);
const decodeRollbackTargetAttempts = Schema.decodeUnknownEffect(RollbackTargetAttempts);

export interface ProjectionCheckpointRollbackContext {
  readonly thread: OrchestrationV2AppThread;
  readonly providerThread: OrchestrationV2ProviderThread | undefined;
  readonly providerSession: OrchestrationV2ThreadProjection["providerSessions"][number] | undefined;
  readonly checkpoint: OrchestrationV2Checkpoint | undefined;
  readonly scope: OrchestrationV2CheckpointScope | undefined;
  readonly targetAttempt: (typeof RollbackTargetAttempts.Type)[number] | undefined;
  readonly providerTurns: ReadonlyArray<OrchestrationV2ProviderTurn>;
  readonly rolledBackAttemptIds: ReadonlyArray<RunAttemptId>;
  readonly runsToRollback: ReadonlyArray<OrchestrationV2Run>;
  readonly rootNodes: ReadonlyArray<OrchestrationV2ExecutionNode>;
  readonly staleCheckpoints: ReadonlyArray<OrchestrationV2Checkpoint>;
}

export interface ProjectionProviderControlContext {
  readonly providerThread: OrchestrationV2ThreadProjection["providerThreads"][number] | undefined;
  readonly providerTurn: OrchestrationV2ProviderTurn | undefined;
  readonly attempt: OrchestrationV2ThreadProjection["attempts"][number] | undefined;
  readonly message: OrchestrationV2ConversationMessage | undefined;
  readonly run: OrchestrationV2Run | undefined;
}

export interface ProjectionProviderControlTarget {
  readonly providerThreadId: ProviderThreadId;
  readonly providerTurnId: ProviderTurnId;
  readonly attemptId?: RunAttemptId;
  readonly messageId?: MessageId;
}

export type ProjectionRuntimeRecoveryState = Pick<
  OrchestrationV2ThreadProjection,
  | "thread"
  | "runs"
  | "attempts"
  | "nodes"
  | "subagents"
  | "providerSessions"
  | "providerThreads"
  | "providerTurns"
  | "runtimeRequests"
  | "messages"
  | "turnItems"
>;

export type ProjectionPendingUserInputs = Pick<
  OrchestrationV2ThreadProjection,
  "runtimeRequests" | "nodes" | "turnItems"
>;

export type ProjectionThreadProviderContext = Pick<
  OrchestrationV2ThreadProjection,
  "thread" | "providerSessions" | "providerThreads"
>;
export interface ProjectionRuntimeResponseContext {
  readonly request: OrchestrationV2ThreadProjection["runtimeRequests"][number] | undefined;
  readonly node: OrchestrationV2ThreadProjection["nodes"][number] | undefined;
  readonly item: OrchestrationV2ThreadProjection["turnItems"][number] | undefined;
  readonly session: OrchestrationV2ThreadProjection["providerSessions"][number] | undefined;
}

export interface ProjectionRecordFilter {
  readonly providerThreadIds?: ReadonlyArray<ProviderThreadId>;
  readonly providerTurnAttemptIds?: ReadonlyArray<RunAttemptId>;
  readonly messageRoles?: ReadonlyArray<OrchestrationV2ConversationMessage["role"]>;
  readonly monitorNotificationsOnly?: boolean;
  readonly turnItemRunId?: RunId;
  readonly messageIds?: ReadonlyArray<MessageId>;
  readonly messageRunIds?: ReadonlyArray<RunId>;
  readonly turnItemRunIds?: ReadonlyArray<RunId | null>;
  readonly turnItemNodeIds?: ReadonlyArray<NodeId>;
  readonly runIds?: ReadonlyArray<RunId>;
  readonly runStatuses?: ReadonlyArray<OrchestrationV2Run["status"]>;
  /** Keeps the thread's highest-ordinal run even when `runStatuses` excludes it. */
  readonly includeLatestRun?: true;
  readonly providerTurnThreadIds?: ReadonlyArray<ProviderThreadId>;
  /** Only the provider turn whose token usage was updated last. */
  readonly latestProviderTurnUsageOnly?: true;
  readonly runtimeRequestStatuses?: ReadonlyArray<OrchestrationV2RuntimeRequest["status"]>;
  readonly contextTransferStatuses?: ReadonlyArray<OrchestrationV2ContextTransfer["status"]>;
  readonly subagentIds?: ReadonlyArray<NodeId>;
  readonly subagentRunIds?: ReadonlyArray<RunId>;
  readonly nodeIds?: ReadonlyArray<NodeId>;
  readonly turnItemRequestIds?: ReadonlyArray<RuntimeRequestId>;
  readonly turnItemTypes?: ReadonlyArray<OrchestrationV2TurnItem["type"]>;
  readonly turnItemStatuses?: ReadonlyArray<OrchestrationV2TurnItem["status"]>;
  readonly omitSettledLeafNodes?: boolean;
  /**
   * Skips ended subagents whose completion was acknowledged, delivered or disposed, unless their
   * run is active or `except` names them.
   */
  readonly omitSettledSubagents?: true | { readonly except: ReadonlyArray<NodeId> };
}

// A fork inherits its source's history before it has messages of its own,
// unless a portable handoff's budget left none of that history to deliver.
const inheritsForkConversation = ({
  thread,
  contextTransfers,
  contextHandoffs,
}: Pick<OrchestrationV2ThreadProjection, "thread" | "contextTransfers" | "contextHandoffs">) => {
  if (thread.forkedFrom?.type !== "run") return false;
  const resolution = contextTransfers.find(
    (transfer) => transfer.type === "fork" && transfer.targetThreadId === thread.id,
  )?.resolution;
  return (
    resolution?.strategy !== "portable_context" ||
    contextHandoffs.some(
      (handoff) =>
        handoff.id === resolution.contextHandoffId && (handoff.history?.messages.length ?? 0) > 0,
    )
  );
};

export type ProjectionRecordField = Exclude<
  keyof OrchestrationV2ThreadProjection,
  "thread" | "updatedAt" | "visibleTurnItems"
>;
export type ProjectionRecords<K extends ProjectionRecordField> = Pick<
  OrchestrationV2ThreadProjection,
  "thread" | K
>;

export interface ProjectionTimelinePageOptions {
  readonly afterPosition?: number;
  readonly itemId?: TurnItemId;
  readonly attachmentId?: string;
  readonly view?: "messages" | "activity";
  readonly limit: number;
}
export interface ProjectionTimelinePage {
  readonly items: ReadonlyArray<OrchestrationV2ProjectedTurnItem>;
  readonly totalItems: number;
  readonly hasMore: boolean;
}

export interface ShellSnapshotOptions {
  readonly location?: "active" | "archive";
  readonly projectId?: ProjectId;
  readonly unsettledOnly?: boolean;
  /**
   * Only the threads sharing this thread's `lineage.rootThreadId`: the root, its forks, and their
   * subagent trees. The thread may be deleted: forks keep a deleted source's root.
   */
  readonly familyOf?: ThreadId;
  readonly worktreePath?: string;
  readonly threadIds?: ReadonlyArray<ThreadId>;
}

/** What a shell subscriber's trims read from an active thread, without building its shell. */
export interface ActiveShellIndexEntry extends Pick<
  OrchestrationV2ThreadShell,
  "id" | "projectId" | "lineage" | "settledOverride" | "settledAt" | "pinnedAt" | "snoozedUntil"
> {
  /** False only when no run, request, work item, or watch can make the shell live. */
  readonly mayBeLive: boolean;
}

export interface OlderSettledThreadsQuery {
  readonly before: OrchestrationV2OlderSettledCursor;
  readonly projectIds?: ReadonlyArray<ProjectId> | undefined;
  readonly limit: number;
}

export interface OlderSettledThreadsListing {
  readonly threadIds: ReadonlyArray<ThreadId>;
  /** Only for `limit: 0`. */
  readonly counts: ReadonlyArray<OrchestrationV2OlderSettledCount>;
}

export interface ProjectionStoreV2Shape {
  readonly searchThreadStream: (
    input: OrchestrationV2SearchThreadInput,
  ) => Stream.Stream<OrchestrationV2SearchThreadResult, ProjectionStoreV2Error>;
  readonly searchThread: (
    input: OrchestrationV2SearchThreadInput,
  ) => Effect.Effect<OrchestrationV2SearchThreadResult, ProjectionStoreV2Error>;
  readonly getThreadAttachmentIds: (
    threadId: ThreadId,
  ) => Effect.Effect<ReadonlyArray<string>, ProjectionStoreV2Error>;
  readonly getThreadHistoryPage: (
    threadId: ThreadId,
    cursor: string,
    throughEntryId?: string,
    conversationOnly?: boolean,
    workMode?: "deferred" | "work",
  ) => Effect.Effect<OrchestrationV2ThreadHistoryPage, ProjectionStoreV2Error>;
  readonly getTimelinePage: (
    threadId: ThreadId,
    options: ProjectionTimelinePageOptions,
  ) => Effect.Effect<ProjectionTimelinePage, ProjectionStoreV2Error>;
  readonly getMessageCount: (threadId: ThreadId) => Effect.Effect<number, ProjectionStoreV2Error>;
  readonly getSubagentCount: (threadId: ThreadId) => Effect.Effect<number, ProjectionStoreV2Error>;
  /** Child threads of the thread's app-owned tasks, in task order, without decoding the tasks. */
  readonly listAppOwnedChildThreadIds: (
    threadId: ThreadId,
  ) => Effect.Effect<ReadonlyArray<ThreadId>, ProjectionStoreV2Error>;
  readonly getNextTurnItemOrdinal: (
    threadId: ThreadId,
  ) => Effect.Effect<number, ProjectionStoreV2Error>;
  readonly getTurnItem: (input: {
    readonly threadId: ThreadId;
    readonly itemId: TurnItemId;
  }) => Effect.Effect<OrchestrationV2TurnItem | null, ProjectionStoreV2Error>;
  readonly getMessagesById: (
    threadId: ThreadId,
    messageIds: ReadonlyArray<MessageId>,
  ) => Effect.Effect<
    ReadonlyMap<MessageId, OrchestrationV2ConversationMessage>,
    ProjectionStoreV2Error
  >;
  readonly getThreadRecords: <K extends ProjectionRecordField>(
    threadId: ThreadId,
    fields: ReadonlyArray<K>,
    filter?: ProjectionRecordFilter,
  ) => Effect.Effect<ProjectionRecords<K>, ProjectionStoreV2Error>;

  readonly apply: (
    event: OrchestrationV2DomainEvent,
  ) => Effect.Effect<void, ProjectionStoreV2Error>;
  readonly getPullRequestSnapshot: (options: {
    readonly threadId?: ThreadId;
    readonly unsettledOnly?: boolean;
  }) => Effect.Effect<
    {
      readonly snapshotSequence: number;
      readonly threads: ReadonlyArray<typeof PullRequestThread.Type>;
    },
    ProjectionStoreV2Error
  >;
  readonly getAgentAwarenessSnapshot: (
    threadId?: ThreadId,
  ) => Effect.Effect<
    { readonly snapshotSequence: number; readonly threads: ReadonlyArray<AgentAwarenessThread> },
    ProjectionStoreV2Error
  >;
  readonly getShellSnapshot: (
    options?: ShellSnapshotOptions,
  ) => Effect.Effect<OrchestrationV2ThreadShellSnapshot, ProjectionStoreV2Error>;
  readonly listActiveSubagentThreadIds: () => Effect.Effect<
    ReadonlyArray<ThreadId>,
    ProjectionStoreV2Error
  >;
  /** Threads that are not deleted, archived ones included. */
  readonly countThreads: () => Effect.Effect<number, ProjectionStoreV2Error>;
  /** Every active thread, in shell order. */
  readonly listActiveShellIndex: () => Effect.Effect<
    ReadonlyArray<ActiveShellIndexEntry>,
    ProjectionStoreV2Error
  >;
  /** Older settled thread ids below a cursor, newest settle first, and per-project counts. */
  readonly listOlderSettledThreads: (
    query: OlderSettledThreadsQuery,
  ) => Effect.Effect<OlderSettledThreadsListing, ProjectionStoreV2Error>;
  /**
   * Callers that combine the shell with other reads run this inside their own
   * transaction and the returned decode after it commits, so the shared
   * connection is not held while thousands of rows are converted.
   */
  readonly readShellSnapshot: (
    options?: ShellSnapshotOptions,
  ) => Effect.Effect<
    Effect.Effect<OrchestrationV2ThreadShellSnapshot, ProjectionStoreV2Error>,
    ProjectionStoreV2Error
  >;
  readonly getThreadShell: (
    threadId: ThreadId,
  ) => Effect.Effect<OrchestrationV2ThreadShell | null, ProjectionStoreV2Error>;
  /** The thread whose current guest share has this id. Scans thread rows; only redeem uses it. */
  readonly findGuestShareThreadId: (
    shareId: GuestShareId,
  ) => Effect.Effect<ThreadId | null, ProjectionStoreV2Error>;
  readonly getThread: (
    threadId: ThreadId,
  ) => Effect.Effect<OrchestrationV2AppThread, ProjectionStoreV2Error>;
  readonly getLimitRecoveryCandidates: (options: {
    readonly now: DateTime.Utc;
    readonly autoResume: boolean;
    readonly snooze: boolean;
  }) => Effect.Effect<ReadonlyArray<ProjectionLimitRecoveryCandidate>, ProjectionStoreV2Error>;
  /** Epoch ms of the next armed reset or snooze end after `now`, when a waiting thread becomes a candidate. */
  readonly getLimitRecoveryWakeAt: (
    now: DateTime.Utc,
  ) => Effect.Effect<number | null, ProjectionStoreV2Error>;
  readonly getSettlementCandidates: (
    threadId?: ThreadId,
  ) => Effect.Effect<ReadonlyArray<ProjectionSettlementCandidate>, ProjectionStoreV2Error>;
  readonly getThreadsWithPullRequests: (
    threadId?: ThreadId,
  ) => Effect.Effect<ReadonlyArray<ProjectionThreadPullRequests>, ProjectionStoreV2Error>;
  /** Titles of the given threads that exist, archived ones included. */
  readonly getThreadLinkMetadata: (
    threadIds: ReadonlyArray<ThreadId>,
  ) => Effect.Effect<Record<ThreadId, OrchestrationV2ThreadLinkMetadata>, ProjectionStoreV2Error>;
  readonly getThreadTitles: (
    threadIds: ReadonlyArray<ThreadId>,
  ) => Effect.Effect<Record<ThreadId, string>, ProjectionStoreV2Error>;
  readonly getThreadWorktreePaths: (options?: {
    readonly includeArchived?: boolean;
  }) => Effect.Effect<
    ReadonlyArray<{ readonly threadId: ThreadId; readonly worktreePath: string }>,
    ProjectionStoreV2Error
  >;
  /**
   * Delegated tasks in a linked environment with work left for this one: no
   * result yet, or cancelled or stopped here without that stop reaching there,
   * including a Stop after the task had its result.
   */
  readonly getOpenRemoteDelegatedTasks: Effect.Effect<
    ReadonlyArray<{ readonly parentThreadId: ThreadId; readonly taskId: NodeId }>,
    ProjectionStoreV2Error
  >;
  readonly getTurnStartContext: (
    threadId: ThreadId,
    runId: RunId,
  ) => Effect.Effect<
    Pick<
      OrchestrationV2ThreadProjection,
      | "thread"
      | "runs"
      | "attempts"
      | "nodes"
      | "subagents"
      | "providerSessions"
      | "providerThreads"
      | "providerTurns"
      | "messages"
      | "checkpointScopes"
      | "contextHandoffs"
      | "contextTransfers"
      | "turnItems"
    > & { readonly hasConversation: boolean },
    ProjectionStoreV2Error
  >;
  readonly getTurnStartHistory: (
    threadId: ThreadId,
    runIds?: ReadonlyArray<RunId>,
  ) => Effect.Effect<ReadonlyArray<OrchestrationV2TurnItem>, ProjectionStoreV2Error>;
  readonly getThreadProjection: (
    threadId: ThreadId,
  ) => Effect.Effect<OrchestrationV2ThreadProjection, ProjectionStoreV2Error>;
  readonly getRuntimeRecoveryProjection: (
    threadId: ThreadId,
  ) => Effect.Effect<ProjectionRuntimeRecoveryState, ProjectionStoreV2Error>;
  readonly getPendingNativeUserInputs: (
    threadId: ThreadId,
    providerTurnId: ProviderTurnId,
  ) => Effect.Effect<ProjectionPendingUserInputs, ProjectionStoreV2Error>;
  readonly getPlan: (
    threadId: ThreadId,
    planId: PlanId,
  ) => Effect.Effect<OrchestrationV2PlanArtifact | undefined, ProjectionStoreV2Error>;
  readonly hasUnpairedRunInterruptRequest: (
    threadId: ThreadId,
    requestId: TurnItemId,
    resultId: TurnItemId,
  ) => Effect.Effect<boolean, ProjectionStoreV2Error>;
  readonly isNodeStored: (
    node: OrchestrationV2ExecutionNode,
  ) => Effect.Effect<boolean, ProjectionStoreV2Error>;
  readonly getRuntimeRequest: (
    threadId: ThreadId,
    requestId: RuntimeRequestId,
  ) => Effect.Effect<
    OrchestrationV2ThreadProjection["runtimeRequests"][number] | undefined,
    ProjectionStoreV2Error
  >;
  readonly getProviderControlContext: (
    threadId: ThreadId,
    target: ProjectionProviderControlTarget,
  ) => Effect.Effect<ProjectionProviderControlContext, ProjectionStoreV2Error>;
  readonly getRunningTurnContext: (
    threadId: ThreadId,
  ) => Effect.Effect<
    Pick<ProjectionProviderControlContext, "run" | "providerThread" | "providerTurn">,
    ProjectionStoreV2Error
  >;
  readonly getThreadProviderContext: (
    threadId: ThreadId,
    targetInstanceId?: ProviderInstanceId,
  ) => Effect.Effect<ProjectionThreadProviderContext, ProjectionStoreV2Error>;
  readonly getRuntimeResponseContext: (
    threadId: ThreadId,
    requestId: RuntimeRequestId,
  ) => Effect.Effect<ProjectionRuntimeResponseContext, ProjectionStoreV2Error>;
  readonly getCheckpointContext: (
    threadId: ThreadId,
  ) => Effect.Effect<ProjectionCheckpointContext, ProjectionStoreV2Error>;
  readonly getCheckpointCaptureContext: (
    threadId: ThreadId,
    target: { readonly runId: RunId; readonly scopeId: CheckpointScopeId },
  ) => Effect.Effect<ProjectionCheckpointCaptureContext, ProjectionStoreV2Error>;
  readonly getCheckpointRollbackContext: (
    threadId: ThreadId,
    target: {
      readonly providerThreadId: ProviderThreadId;
      readonly checkpointId: CheckpointId;
      readonly scopeId: CheckpointScopeId;
    },
  ) => Effect.Effect<ProjectionCheckpointRollbackContext, ProjectionStoreV2Error>;
  readonly getRunMessage: (
    threadId: ThreadId,
    runId: RunId,
  ) => Effect.Effect<OrchestrationV2ConversationMessage | undefined, ProjectionStoreV2Error>;
  readonly canStartQueuedRun: (
    threadId: ThreadId,
  ) => Effect.Effect<boolean, ProjectionStoreV2Error>;
  readonly getHeldQueuedRuns: (
    threadId: ThreadId,
  ) => Effect.Effect<ReadonlyArray<OrchestrationV2Run>, ProjectionStoreV2Error>;
  readonly getRecoveryThreadIds: (
    kind: ProjectionRecoveryKind,
    projects?: {
      readonly include?: ReadonlyArray<string>;
      readonly exclude?: ReadonlyArray<string>;
    },
  ) => Effect.Effect<ReadonlyArray<ThreadId>, ProjectionStoreV2Error>;
  readonly getUnreadableThreadIds: () => Effect.Effect<
    ReadonlyArray<ThreadId>,
    ProjectionStoreV2Error
  >;
  readonly getThreadSnapshot: (threadId: ThreadId) => Effect.Effect<
    {
      readonly schemaVersion: number;
      readonly snapshotSequence: number;
      readonly projection: OrchestrationV2ThreadProjection;
    },
    ProjectionStoreV2Error
  >;
  readonly getThreadSnapshotWindow: (
    threadId: ThreadId,
    options: {
      readonly rowLimit: number;
      readonly userTurnLimit?: number | undefined;
      readonly deferWork?: boolean | undefined;
      readonly anchorItemId?: TurnItemId | undefined;
      readonly anchorThreadId?: ThreadId | undefined;
      readonly requiredRunId?: RunId | undefined;
    },
  ) => Effect.Effect<
    {
      readonly schemaVersion: number;
      readonly snapshotSequence: number;
      readonly projection: OrchestrationV2ThreadProjection;
    },
    ProjectionStoreV2Error
  >;
}

export type ProjectionWindow = Omit<
  Parameters<ProjectionStoreV2Shape["getThreadSnapshotWindow"]>[1],
  "anchorThreadId"
> & {
  readonly suppressLocal?: boolean | undefined;
  readonly historyAnchor?: { readonly threadId: ThreadId; readonly itemId: TurnItemId } | undefined;
};

export class ProjectionStoreV2 extends Context.Service<ProjectionStoreV2, ProjectionStoreV2Shape>()(
  "t3/orchestration-v2/ProjectionStore/ProjectionStoreV2",
) {}

export const ORCHESTRATION_V2_PROJECTION_SCHEMA_VERSION = 2;

function upsertById<T extends { readonly id: string }>(items: ReadonlyArray<T>, next: T): Array<T> {
  const index = items.findIndex((item) => item.id === next.id);
  if (index === -1) {
    return [...items, next];
  }

  const updated = [...items];
  updated[index] = next;
  return updated;
}

function upsertProviderTurn(
  turns: ReadonlyArray<OrchestrationV2ProviderTurn>,
  next: OrchestrationV2ProviderTurn,
): Array<OrchestrationV2ProviderTurn> {
  const current = turns.find((turn) => turn.id === next.id);
  return upsertById(turns, {
    ...next,
    ...((next.tokenUsage ?? current?.tokenUsage) === undefined
      ? {}
      : { tokenUsage: next.tokenUsage ?? current?.tokenUsage }),
  });
}

function preserveRunRecordedFields(
  current: OrchestrationV2Run | undefined,
  next: OrchestrationV2Run,
): OrchestrationV2Run {
  if (current === undefined) return next;
  return {
    ...next,
    ...(next.delegatedCompletion === undefined && current.delegatedCompletion !== undefined
      ? { delegatedCompletion: current.delegatedCompletion }
      : {}),
    ...(next.restartCancelledBackgroundWork === undefined &&
    current.restartCancelledBackgroundWork !== undefined
      ? { restartCancelledBackgroundWork: current.restartCancelledBackgroundWork }
      : {}),
  };
}

function preserveCompletionDelivery(
  current: OrchestrationV2Subagent | undefined,
  next: OrchestrationV2Subagent,
): OrchestrationV2Subagent {
  if (next.completionDelivery !== undefined || current?.completionDelivery === undefined) {
    return next;
  }
  return { ...next, completionDelivery: current.completionDelivery };
}

export function emptyProjection(
  event: Extract<OrchestrationV2DomainEvent, { readonly type: "thread.created" }>,
): OrchestrationV2ThreadProjection {
  return {
    thread: event.payload,
    runs: [],
    attempts: [],
    nodes: [],
    subagents: [],
    providerSessions: [],
    providerThreads: [],
    providerTurns: [],
    runtimeRequests: [],
    messages: [],
    plans: [],
    turnItems: [],
    checkpointScopes: [],
    checkpoints: [],
    contextHandoffs: [],
    contextTransfers: [],
    visibleTurnItems: [],
    updatedAt: event.occurredAt,
  };
}

function isQueuedProviderThreadPlaceholder(providerThread: OrchestrationV2ProviderThread): boolean {
  return (
    providerThread.status === "not_loaded" &&
    providerThread.firstRunOrdinal === null &&
    providerThread.nativeThreadRef === null &&
    providerThread.providerSessionId === null
  );
}

export function applyToProjection(
  projection: OrchestrationV2ThreadProjection,
  event: OrchestrationV2DomainEvent,
): OrchestrationV2ThreadProjection {
  const base = {
    ...projection,
    thread: {
      ...projection.thread,
      updatedAt: event.occurredAt,
    },
    updatedAt: event.occurredAt,
  };

  switch (event.type) {
    case "thread.pull-request-link-updated":
      return {
        ...projection,
        thread: applyThreadPullRequestLinkUpdate(projection.thread, event.payload),
      };
    case "thread.created":
    case "thread.archived":
    case "thread.unarchived":
    case "thread.deleted":
    case "thread.settled":
    case "thread.unsettled":
    case "thread.snoozed":
    case "thread.unsnoozed":
    case "thread.auto-settle-set":
    case "thread.pinned":
    case "thread.unpinned":
    case "thread.pin-reordered":
    case "thread.metadata-updated":
    case "thread.runtime-mode-updated":
    case "thread.interaction-mode-updated":
    case "thread.model-selection-updated":
    case "thread.provider-switched":
    case "thread.guest-share-updated":
      return {
        ...base,
        thread: event.payload,
      };
    // Reordering and PR refreshes preserve the orchestrator's activity timestamp.
    case "thread.active-reordered":
    case "thread.tags-updated":
    case "thread.pull-request-synced":
      return {
        ...base,
        thread: event.payload,
        updatedAt: event.payload.updatedAt,
      };
    case "thread.visited":
    case "thread.marked-unread":
      return {
        ...projection,
        thread: event.payload,
      };
    case "run.created":
    case "run.updated":
      return withLocalVisibleTurnItems({
        ...base,
        runs: upsertById(
          base.runs,
          preserveRunRecordedFields(
            base.runs.find((run) => run.id === event.payload.id),
            event.payload,
          ),
        ),
      });
    case "run.background-work-cancelled":
      return {
        ...base,
        runs: base.runs.map((run) =>
          run.id === event.payload.runId
            ? {
                ...run,
                restartCancelledBackgroundWork: event.payload.restartCancelledBackgroundWork,
              }
            : run,
        ),
      };
    case "run-attempt.created":
    case "run-attempt.updated":
      return withLocalVisibleTurnItems({
        ...base,
        attempts: upsertById(base.attempts, event.payload),
      });
    case "node.updated":
      return {
        ...base,
        nodes: upsertById(base.nodes, event.payload),
      };
    case "subagent.updated":
      return {
        ...base,
        subagents: upsertById(
          base.subagents,
          preserveCompletionDelivery(
            base.subagents.find((task) => task.id === event.payload.id),
            event.payload,
          ),
        ),
      };
    case "provider-session.attached":
    case "provider-session.updated":
      return {
        ...base,
        providerSessions: upsertById(base.providerSessions, event.payload),
      };
    case "provider-session.detached":
      return {
        ...base,
        providerSessions: base.providerSessions.filter(
          (session) => session.id !== event.payload.providerSessionId,
        ),
      };
    case "provider-thread.updated":
      return {
        ...base,
        thread:
          event.payload.appThreadId === base.thread.id &&
          !isQueuedProviderThreadPlaceholder(event.payload)
            ? {
                ...base.thread,
                activeProviderThreadId: event.payload.id,
              }
            : base.thread,
        providerThreads: upsertById(base.providerThreads, event.payload),
      };
    case "provider-turn.updated":
      return {
        ...base,
        providerTurns: upsertProviderTurn(base.providerTurns, event.payload),
      };
    case "runtime-request.updated":
      return {
        ...base,
        runtimeRequests: upsertById(base.runtimeRequests, event.payload),
      };
    case "message.updated":
      return {
        ...base,
        messages: upsertById(base.messages, event.payload),
      };
    case "turn-item.updated":
      return withLocalVisibleTurnItems({
        ...base,
        turnItems: upsertById(base.turnItems, event.payload),
      });
    case "plan.updated":
      return {
        ...base,
        plans: upsertById(base.plans, event.payload),
      };
    case "checkpoint-scope.created":
      return {
        ...base,
        checkpointScopes: upsertById(base.checkpointScopes, event.payload),
      };
    case "checkpoint.captured":
      return {
        ...base,
        checkpoints: upsertById(base.checkpoints, event.payload),
      };
    case "checkpoint.rollback-requested":
      return base;
    case "context-handoff.updated":
      return {
        ...base,
        contextHandoffs: upsertById(base.contextHandoffs, event.payload),
      };
    case "context-transfer.created":
    case "context-transfer.updated":
      return {
        ...base,
        contextTransfers: upsertById(base.contextTransfers, event.payload),
      };
  }
}

type PayloadRow = {
  readonly payload_json: string;
};

export type ShellPendingTurnItemRow = PayloadRow & {
  readonly thread_id: string;
  readonly owning_provider_instance_id: string | null;
};

export type AgentAwarenessThread = ProjectThreadAwarenessV2Input["thread"] &
  Pick<
    OrchestrationV2ThreadShell,
    | "projectId"
    | "creationSource"
    | "latestRunCompletedAt"
    | "archivedAt"
    | "activeRunId"
    | "latestRunId"
    | "hasActionableProposedPlan"
  >;

export type ShellThreadRow = {
  readonly thread_id: string;
  readonly payload_json: string;
  readonly forked_from_run_source_thread_id: string | null;
  readonly latest_run_id: string | null;
  readonly latest_run_status: string | null;
  readonly latest_run_requested_at: string | null;
  readonly latest_run_started_at: string | null;
  readonly latest_run_completed_at: string | null;
  readonly latest_run_stop_requested: number;
  readonly active_run_id: string | null;
  readonly active_run_status: string | null;
  readonly activity_run_status: string | null;
  readonly activity_run_started_at: string | null;
  readonly last_error: string | null;
  readonly terminal_failure_payload_json: string | null;
  readonly blocking_run_id: string | null;
  readonly blocking_run_requested_at: string | null;
  readonly blocking_run_started_at: string | null;
  readonly blocking_run_completed_at: string | null;
  readonly blocking_run_stop_requested: number;
  readonly blocking_failure_payload_json: string | null;
  readonly pending_request_payload_json: string | null;
  readonly pending_secret_request_payload_json: string | null;
  readonly pending_link_request_payload_json: string | null;
  readonly latest_user_message_at: string | null;
  readonly latest_user_authored_message_at: string | null;
  readonly has_actionable_proposed_plan: number;
};

export type ShellRunRow = {
  readonly thread_id: string;
  readonly run_id: string;
  readonly ordinal: number;
};

export type SettlementThreadRow = Pick<
  ShellThreadRow,
  | "thread_id"
  | "payload_json"
  | "latest_run_id"
  | "latest_run_status"
  | "latest_run_requested_at"
  | "latest_run_started_at"
  | "latest_run_completed_at"
  | "latest_user_message_at"
  | "latest_user_authored_message_at"
>;

export type ShellRunItemCountRow = {
  readonly thread_id: string;
  readonly run_id: string;
  readonly item_count: number;
};

export type ShellItemCountRow = {
  readonly thread_id: string;
  readonly item_count: number;
  readonly runless_item_count: number;
};

const encodeTimelineSources = Schema.encodeEffect(
  Schema.fromJsonString(
    Schema.Array(Schema.Struct({ threadId: Schema.String, id: Schema.String })),
  ),
);
const encodeIdList = Schema.encodeSync(Schema.fromJsonString(Schema.Array(Schema.String)));

// Empty filters still scan SQLite rows; short-circuit them, including turnItemTypes: [].
const matchesNothing = (...filters: ReadonlyArray<ReadonlyArray<unknown> | undefined>) =>
  filters.some((values) => values?.length === 0);

const encodeThreadPayload = Schema.encodeEffect(
  Schema.fromJsonString(OrchestrationV2AppThreadJsonSchema),
);
const encodeThreadUpdatedAt = Schema.encodeEffect(
  OrchestrationV2AppThreadJsonSchema.fields.updatedAt,
);
const encodeRunPayload = Schema.encodeEffect(Schema.fromJsonString(OrchestrationV2RunJsonSchema));
const encodeRestartCancelledBackgroundWork = Schema.encodeEffect(
  Schema.fromJsonString(OrchestrationV2RunJsonSchema.fields.restartCancelledBackgroundWork),
);
const encodeRunAttemptPayload = Schema.encodeEffect(
  Schema.fromJsonString(OrchestrationV2RunAttemptJsonSchema),
);
const encodeNodePayload = Schema.encodeEffect(
  Schema.fromJsonString(OrchestrationV2ExecutionNodeJsonSchema),
);
const encodeSubagentPayload = Schema.encodeEffect(
  Schema.fromJsonString(OrchestrationV2SubagentJsonSchema),
);
const encodeProviderSessionPayload = Schema.encodeEffect(
  Schema.fromJsonString(OrchestrationV2ProviderSessionJsonSchema),
);
export const encodeProviderThreadPayload = Schema.encodeEffect(
  Schema.fromJsonString(OrchestrationV2ProviderThreadJsonSchema),
);
const encodeProviderTurnPayload = Schema.encodeEffect(
  Schema.fromJsonString(OrchestrationV2ProviderTurnJsonSchema),
);
const encodeRuntimeRequestPayload = Schema.encodeEffect(
  Schema.fromJsonString(OrchestrationV2RuntimeRequestJsonSchema),
);
const encodeMessagePayload = Schema.encodeEffect(
  Schema.fromJsonString(OrchestrationV2ConversationMessageJsonSchema),
);
const encodePlanPayload = Schema.encodeEffect(
  Schema.fromJsonString(OrchestrationV2PlanArtifactSchema),
);
const encodeTurnItemPayload = Schema.encodeEffect(
  Schema.fromJsonString(OrchestrationV2TurnItemJsonSchema),
);
// The codec every *Json payload schema uses for timestamps, so index columns match payload_json.
const encodePayloadTimestamp = Schema.encodeEffect(Schema.DateTimeUtcFromString);
const encodeCheckpointScopePayload = Schema.encodeEffect(
  Schema.fromJsonString(OrchestrationV2CheckpointScopeJsonSchema),
);
const encodeCheckpointPayload = Schema.encodeEffect(
  Schema.fromJsonString(OrchestrationV2CheckpointJsonSchema),
);
const encodeContextHandoffPayload = Schema.encodeEffect(
  Schema.fromJsonString(OrchestrationV2ContextHandoffJsonSchema),
);
const encodeContextTransferPayload = Schema.encodeEffect(
  Schema.fromJsonString(OrchestrationV2ContextTransferJsonSchema),
);

const PullRequestThread = OrchestrationV2AppThreadJsonSchema.mapFields(
  ({
    id,
    projectId,
    lineage,
    branch,
    worktreePath,
    archivedAt,
    settledOverride,
    settledAt,
    linkedPullRequest,
    branchPullRequest,
    pullRequests,
  }) => ({
    id,
    projectId,
    lineage,
    branch,
    worktreePath,
    archivedAt,
    settledOverride,
    settledAt,
    linkedPullRequest,
    branchPullRequest,
    pullRequests,
  }),
);
const decodePullRequestThread = Schema.decodeUnknownEffect(
  Schema.fromJsonString(PullRequestThread),
);

const decodeThreadPayload = Schema.decodeUnknownEffect(
  Schema.fromJsonString(OrchestrationV2AppThreadJsonSchema),
);
const decodeRunPayload = Schema.decodeUnknownEffect(
  Schema.fromJsonString(OrchestrationV2RunJsonSchema),
);
const decodeRunAttemptPayload = Schema.decodeUnknownEffect(
  Schema.fromJsonString(OrchestrationV2RunAttemptJsonSchema),
);
const decodeNodePayload = Schema.decodeUnknownEffect(
  Schema.fromJsonString(OrchestrationV2ExecutionNodeJsonSchema),
);
const decodeSubagentPayload = Schema.decodeUnknownEffect(
  Schema.fromJsonString(OrchestrationV2SubagentJsonSchema),
);
const decodeProviderSessionPayload = Schema.decodeUnknownEffect(
  Schema.fromJsonString(OrchestrationV2ProviderSessionJsonSchema),
);
const decodeProviderThreadPayload = Schema.decodeUnknownEffect(
  Schema.fromJsonString(OrchestrationV2ProviderThreadJsonSchema),
);
const decodeProviderTurnPayload = Schema.decodeUnknownEffect(
  Schema.fromJsonString(OrchestrationV2ProviderTurnJsonSchema),
);
const decodeRuntimeRequestPayload = Schema.decodeUnknownEffect(
  Schema.fromJsonString(OrchestrationV2RuntimeRequestJsonSchema),
);
const decodeMessagePayload = Schema.decodeUnknownEffect(
  Schema.fromJsonString(OrchestrationV2ConversationMessageJsonSchema),
);
const decodePlanJson = Schema.decodeUnknownEffect(
  Schema.fromJsonString(OrchestrationV2PlanArtifactSchema),
);
const decodeTurnItemPayload = Schema.decodeUnknownEffect(
  Schema.fromJsonString(OrchestrationV2TurnItemJsonSchema),
);
const decodeCheckpointScopePayload = Schema.decodeUnknownEffect(
  Schema.fromJsonString(OrchestrationV2CheckpointScopeJsonSchema),
);
const decodeCheckpointPayload = Schema.decodeUnknownEffect(
  Schema.fromJsonString(OrchestrationV2CheckpointJsonSchema),
);
const decodeContextHandoffPayload = Schema.decodeUnknownEffect(
  Schema.fromJsonString(OrchestrationV2ContextHandoffJsonSchema),
);
const decodeContextTransferPayload = Schema.decodeUnknownEffect(
  Schema.fromJsonString(OrchestrationV2ContextTransferJsonSchema),
);

const isProjectionStoreThreadNotFoundError = Schema.is(ProjectionStoreThreadNotFoundError);
const isProjectionStoreReadError = Schema.is(ProjectionStoreReadError);

function parseEncodedPayload(json: string): Record<string, unknown> {
  return JSON.parse(json) as Record<string, unknown>;
}

function stringField(payload: Record<string, unknown>, field: string): string {
  const value = payload[field];
  return typeof value === "string" ? value : String(value);
}

function nullableStringField(payload: Record<string, unknown>, field: string): string | null {
  const value = payload[field];
  if (value === null || value === undefined) {
    return null;
  }
  return typeof value === "string" ? value : String(value);
}

function booleanInt(value: boolean): 0 | 1 {
  return value ? 1 : 0;
}

const decodeRows =
  <A, E>(decode: (json: string) => Effect.Effect<A, E>, threadId: ThreadId) =>
  (rows: ReadonlyArray<PayloadRow>): Effect.Effect<Array<A>, ProjectionStoreReadError> =>
    Effect.forEach(rows, (row) => decode(row.payload_json)).pipe(
      Effect.mapError(
        (cause) =>
          new ProjectionStoreReadError({
            threadId,
            cause,
          }),
      ),
    );

function messageIdForTurnItem(item: OrchestrationV2TurnItem): string | null {
  switch (item.type) {
    case "user_message":
    case "assistant_message":
      return item.messageId;
    default:
      return null;
  }
}

function sortMessagesByTurnItemOrder(
  messages: ReadonlyArray<OrchestrationV2ConversationMessage>,
  turnItems: ReadonlyArray<OrchestrationV2TurnItem>,
  messageOrdinals = new Map<string, number>(),
): Array<OrchestrationV2ConversationMessage> {
  for (const turnItem of turnItems) {
    const messageId = messageIdForTurnItem(turnItem);
    if (messageId === null) {
      continue;
    }
    const existing = messageOrdinals.get(messageId);
    if (existing === undefined || turnItem.ordinal < existing) {
      messageOrdinals.set(messageId, turnItem.ordinal);
    }
  }

  return messages.toSorted((left, right) => {
    const leftOrdinal = messageOrdinals.get(left.id) ?? Number.MAX_SAFE_INTEGER;
    const rightOrdinal = messageOrdinals.get(right.id) ?? Number.MAX_SAFE_INTEGER;
    if (leftOrdinal !== rightOrdinal) {
      return leftOrdinal - rightOrdinal;
    }

    const leftCreatedAt = DateTime.toEpochMillis(left.createdAt);
    const rightCreatedAt = DateTime.toEpochMillis(right.createdAt);
    if (leftCreatedAt !== rightCreatedAt) {
      return leftCreatedAt - rightCreatedAt;
    }

    return left.id.localeCompare(right.id);
  });
}

function activeLocalTurnItems(
  projection: OrchestrationV2ThreadProjection,
): Array<OrchestrationV2ProjectedTurnItem> {
  const isVisible = createOrchestrationV2TurnItemVisibility({
    runs: projection.runs,
    attempts: projection.attempts,
    items: projection.turnItems,
  });
  return projection.turnItems.filter(isVisible).map((item, position) => ({
    position,
    visibility: "local" as const,
    sourceThreadId: item.threadId,
    sourceItemId: item.id,
    item,
  }));
}

function localVisibleTurnItems(
  projection: OrchestrationV2ThreadProjection,
): Array<OrchestrationV2ProjectedTurnItem> {
  return activeLocalTurnItems(projection);
}

function inheritedVisibleTurnItemsFromLocalItems(
  items: ReadonlyArray<OrchestrationV2TurnItem>,
): Array<Omit<OrchestrationV2ProjectedTurnItem, "position">> {
  return items.map((item) => ({
    visibility: "inherited" as const,
    sourceThreadId: item.threadId,
    sourceItemId: item.id,
    item,
  }));
}

function withLocalVisibleTurnItems(
  projection: OrchestrationV2ThreadProjection,
): OrchestrationV2ThreadProjection {
  return {
    ...projection,
    visibleTurnItems: localVisibleTurnItems(projection),
  };
}

function renumberVisibleTurnItems(
  rows: ReadonlyArray<Omit<OrchestrationV2ProjectedTurnItem, "position">>,
  offset = 0,
): Array<OrchestrationV2ProjectedTurnItem> {
  return rows.map((row, position) => ({ ...row, position: position + offset }));
}

function makeForkMarkerTurnItem(input: {
  readonly targetProjection: Pick<OrchestrationV2ThreadProjection, "thread">;
  readonly sourceThreadId: ThreadId;
  readonly sourceRunId: NonNullable<OrchestrationV2TurnItem["runId"]>;
}): Extract<OrchestrationV2TurnItem, { type: "fork" }> {
  const createdAt = input.targetProjection.thread.createdAt;
  return {
    id: TurnItemId.make(`turn-item:fork:${input.targetProjection.thread.id}`),
    threadId: input.targetProjection.thread.id,
    runId: null,
    nodeId: null,
    providerTurnId: null,
    nativeItemRef: null,
    parentItemId: null,
    ordinal: 0,
    status: "completed",
    title: "Forked from conversation",
    startedAt: null,
    completedAt: createdAt,
    updatedAt: createdAt,
    type: "fork",
    source: { type: "run", threadId: input.sourceThreadId, runId: input.sourceRunId },
    targetThreadId: input.targetProjection.thread.id,
  };
}

function isTurnItemAtOrBeforeRun(input: {
  readonly historyOrigin: OrchestrationV2ThreadProjection["thread"]["historyOrigin"];
  readonly itemRunId: OrchestrationV2TurnItem["runId"];
  readonly runOrdinalById: ReadonlyMap<NonNullable<OrchestrationV2TurnItem["runId"]>, number>;
  readonly sourceRunOrdinal: number;
}): boolean {
  if (input.itemRunId === null) {
    return input.historyOrigin === "v1_import";
  }
  const ordinal = input.runOrdinalById.get(input.itemRunId);
  return ordinal !== undefined && ordinal <= input.sourceRunOrdinal;
}

function visibleTurnItemsThroughRun(input: {
  readonly sourceProjection: OrchestrationV2ThreadProjection;
  readonly sourceRunId: NonNullable<OrchestrationV2TurnItem["runId"]>;
}): Array<Omit<OrchestrationV2ProjectedTurnItem, "position">> {
  const sourceRun = input.sourceProjection.runs.find((run) => run.id === input.sourceRunId);
  if (sourceRun === undefined) {
    return [];
  }

  const runOrdinalById = new Map(input.sourceProjection.runs.map((run) => [run.id, run.ordinal]));
  const inheritedPrefix = input.sourceProjection.visibleTurnItems
    .filter(
      (row) => row.item.threadId !== input.sourceProjection.thread.id || row.item.type === "fork",
    )
    .map((row) => ({
      visibility: "inherited" as const,
      sourceThreadId: row.sourceThreadId,
      sourceItemId: row.sourceItemId,
      item: row.item,
    }));
  const localPrefix = inheritedVisibleTurnItemsFromLocalItems(
    input.sourceProjection.turnItems.filter((item) => {
      if (
        isOrchestrationV2SupersededInterrupt({
          item,
          attempts: input.sourceProjection.attempts,
          items: input.sourceProjection.turnItems,
        })
      ) {
        return false;
      }
      return isTurnItemAtOrBeforeRun({
        historyOrigin: input.sourceProjection.thread.historyOrigin,
        itemRunId: item.runId,
        runOrdinalById,
        sourceRunOrdinal: sourceRun.ordinal,
      });
    }),
  );

  return [...inheritedPrefix, ...localPrefix];
}

// Side chats show only their own rows; the fork transfer carries the parent's context to the model.
function inheritedRunFork(thread: Pick<OrchestrationV2AppThread, "forkedFrom" | "lineage">) {
  return thread.forkedFrom?.type === "run" && thread.lineage.sideChat !== true
    ? thread.forkedFrom
    : null;
}

function buildVisibleTurnItems(input: {
  readonly projection: OrchestrationV2ThreadProjection;
  readonly sourceProjection: OrchestrationV2ThreadProjection | null;
  readonly inheritedRowCount?: number;
}): Array<OrchestrationV2ProjectedTurnItem> {
  const forkedFrom = inheritedRunFork(input.projection.thread);
  if (
    forkedFrom === null ||
    (input.sourceProjection === null && input.inheritedRowCount === undefined)
  ) {
    return localVisibleTurnItems(input.projection);
  }

  const inherited =
    input.sourceProjection === null
      ? []
      : visibleTurnItemsThroughRun({
          sourceProjection: input.sourceProjection,
          sourceRunId: forkedFrom.runId,
        });
  const markerItem = makeForkMarkerTurnItem({
    targetProjection: input.projection,
    sourceThreadId: forkedFrom.threadId,
    sourceRunId: forkedFrom.runId,
  });
  const local = activeLocalTurnItems(input.projection).map((row) => ({
    visibility: "local" as const,
    sourceThreadId: row.sourceThreadId,
    sourceItemId: row.sourceItemId,
    item: row.item,
  }));

  return renumberVisibleTurnItems(
    [
      ...inherited,
      {
        visibility: "synthetic",
        sourceThreadId: forkedFrom.threadId,
        sourceItemId: markerItem.id,
        item: markerItem,
      },
      ...local,
    ],
    input.inheritedRowCount ?? 0,
  );
}

function pendingRuntimeRequestSummary(
  request: OrchestrationV2ThreadProjection["runtimeRequests"][number] | null,
): OrchestrationV2PendingRuntimeRequestSummary | null {
  if (request === null) return null;
  return {
    id: request.id,
    kind: request.kind,
    createdAt: request.createdAt,
    blocking: request.responseCapability.type !== "message",
  };
}

function secretRequestAsPendingInput(
  item: OrchestrationV2TurnItem | null,
): OrchestrationV2PendingRuntimeRequestSummary | null {
  if ((item?.type !== "secret_request" && item?.type !== "link_request") || item.nodeId === null)
    return null;
  return {
    id: RuntimeRequestId.make(item.id),
    kind: "user_input",
    createdAt: item.startedAt ?? item.updatedAt,
    blocking: true,
  };
}

/**
 * A secret card is answered while its agent waits in the run. A link card's
 * agent asked without waiting, so the card waits on the user after its run,
 * until the user writes in the thread again and so has moved past it.
 */
const waitsOnUser = (
  item: OrchestrationV2TurnItem,
  liveRunIds: ReadonlySet<RunId>,
  latestUserAuthoredMessageAt: DateTime.Utc | null,
) =>
  (item.type === "link_request" && linkCardStillAsks(item, latestUserAuthoredMessageAt)) ||
  (item.type === "secret_request" && item.runId !== null && liveRunIds.has(item.runId));

const linkCardStillAsks = (
  item: OrchestrationV2TurnItem,
  latestUserAuthoredMessageAt: DateTime.Utc | null,
) =>
  latestUserAuthoredMessageAt === null ||
  !DateTime.isGreaterThan(latestUserAuthoredMessageAt, item.startedAt ?? item.updatedAt);

export function threadShellFromProjection(
  projection: OrchestrationV2ThreadProjection,
): OrchestrationV2ThreadShell {
  const providerSession =
    projection.providerSessions
      .filter((session) => session.providerInstanceId === projection.thread.providerInstanceId)
      .toSorted(
        (left, right) =>
          DateTime.toEpochMillis(right.updatedAt) - DateTime.toEpochMillis(left.updatedAt),
      )[0] ?? null;
  // T3 Code's own idle compactions stay out of the shell unless they fail, so thread lists,
  // notifications and the relay keep presenting the user's last turn.
  const runs = projection.runs.filter((run) => !isSilentRunForShell(run, projection));
  const latestRun =
    usageLimitRunPresentedAsLatest(
      runs,
      projection.turnItems,
      providerSession?.lastError ?? null,
    ) ?? latestUnheldRun(runs);
  const activeRun =
    runs
      .filter(isInterruptibleRunForShell)
      .toSorted((left, right) => right.ordinal - left.ordinal)[0] ?? null;
  const activityRun =
    runs.filter(isActivityRunForShell).toSorted((left, right) => right.ordinal - left.ordinal)[0] ??
    null;
  const liveRunIds = new Set(runs.filter(isActivityRunForShell).map((run) => run.id));
  let pendingRuntimeRequest = pendingRuntimeRequestSummary(
    projection.runtimeRequests
      .filter((request) => request.status === "pending")
      .toSorted(
        (left, right) =>
          Number(left.kind === "user_input") - Number(right.kind === "user_input") ||
          DateTime.toEpochMillis(right.createdAt) - DateTime.toEpochMillis(left.createdAt),
      )[0] ?? null,
  );

  const userMessages = projection.messages
    .filter((message) => message.role === "user" && idleCompactionOf(message) === undefined)
    .toSorted(
      (left, right) =>
        DateTime.toEpochMillis(right.updatedAt) - DateTime.toEpochMillis(left.updatedAt),
    );
  const latestUserAuthoredMessageAt =
    userMessages.find((message) => message.createdBy === "user")?.updatedAt ?? null;
  if (pendingRuntimeRequest === null || pendingRuntimeRequest.kind === "user_input") {
    const waitingCard = secretRequestAsPendingInput(
      projection.turnItems
        .filter(
          (item) =>
            item.status === "waiting" && waitsOnUser(item, liveRunIds, latestUserAuthoredMessageAt),
        )
        .toSorted(
          (left, right) =>
            DateTime.toEpochMillis(right.updatedAt) - DateTime.toEpochMillis(left.updatedAt),
        )[0] ?? null,
    );
    if (
      waitingCard !== null &&
      (pendingRuntimeRequest === null ||
        DateTime.toEpochMillis(waitingCard.createdAt) >=
          DateTime.toEpochMillis(pendingRuntimeRequest.createdAt))
    ) {
      pendingRuntimeRequest = waitingCard;
    }
  }
  const latestUserMessage = userMessages[0] ?? null;
  const pendingBackgroundTasks = derivePendingBackgroundWork({
    latestRun,
    providerThreads: projection.providerThreads,
    turnItems: projection.turnItems,
    activeProviderThreadId: projection.thread.activeProviderThreadId,
    runs,
    pullRequests: projection.thread.pullRequests,
  });
  return {
    createdBy: projection.thread.createdBy,
    creationSource: projection.thread.creationSource,
    id: projection.thread.id,
    projectId: projection.thread.projectId,
    title: projection.thread.title,
    providerInstanceId: projection.thread.providerInstanceId,
    modelSelection: projection.thread.modelSelection,
    runtimeMode: projection.thread.runtimeMode,
    interactionMode: projection.thread.interactionMode,
    branch: projection.thread.branch,
    worktreePath: projection.thread.worktreePath,
    pullRequests: threadPullRequestsOf(projection.thread),
    ...(projection.thread.linkedPullRequest === undefined
      ? {}
      : { linkedPullRequest: projection.thread.linkedPullRequest }),
    ...(projection.thread.branchPullRequest === undefined
      ? {}
      : { branchPullRequest: projection.thread.branchPullRequest }),
    ...(projection.thread.activeOrderKey === undefined
      ? {}
      : { activeOrderKey: projection.thread.activeOrderKey }),
    lineage: projection.thread.lineage,
    forkedFrom: projection.thread.forkedFrom,
    activeProviderThreadId: projection.thread.activeProviderThreadId,
    ...(projection.thread.historyOrigin === undefined
      ? {}
      : { historyOrigin: projection.thread.historyOrigin }),
    ...(projection.thread.linkOrigin === undefined
      ? {}
      : { linkOrigin: projection.thread.linkOrigin }),
    ...(projection.thread.delegatedFrom === undefined
      ? {}
      : { delegatedFrom: projection.thread.delegatedFrom }),
    latestRunId: latestRun?.id ?? null,
    latestRunRequestedAt: latestRun?.requestedAt ?? null,
    latestRunStartedAt: latestRun?.startedAt ?? null,
    latestRunCompletedAt: latestRun?.completedAt ?? null,
    latestRunStopRequested:
      latestRun != null &&
      projection.turnItems.some(
        (item) => item.runId === latestRun.id && item.type === "run_interrupt_request",
      ),
    activeRunId: activeRun?.id ?? null,
    blockedOnThreadWait: deriveBlockedOnThreadWait({ activeRun, turnItems: projection.turnItems }),
    activityRunStatus: activityRun?.status ?? null,
    activityRunStartedAt:
      activityRun === null ? null : orchestrationV2RunWorkStartedAt(activityRun),
    status: latestRun?.status ?? "idle",
    ...threadErrorSummary(
      latestRootProviderFailure(latestRun, projection.turnItems),
      providerSession?.lastError ?? null,
    ),
    pendingRuntimeRequest,
    pendingGuestMessageCount: pendingGuestMessageCount(projection.thread.guestShare),
    // Thread detail owns message bodies. Keeping them out of shell rows makes
    // initial hydration and streaming updates independent of transcript size.
    latestVisibleMessage: null,
    latestUserMessageAt: latestUserMessage?.updatedAt ?? null,
    latestUserAuthoredMessageAt,
    hasActionableProposedPlan: projection.plans.some(
      (plan) => plan.kind === "proposed_plan" && plan.status === "active",
    ),
    pendingBackgroundTasks: [...pendingBackgroundTasks],
    providerInstanceHistory: providerInstanceHistoryForShell({
      threadId: projection.thread.id,
      providerThreads: projection.providerThreads,
    }),
    goal: activeProviderGoalForShell(
      projection.providerThreads,
      projection.thread.activeProviderThreadId,
    ),
    itemCount: activeLocalTurnItems(projection).length,
    visibleItemCount: projection.visibleTurnItems.length,
    createdAt: projection.thread.createdAt,
    updatedAt: projection.thread.updatedAt,
    archivedAt: projection.thread.archivedAt,
    settledOverride: projection.thread.settledOverride,
    settledAt: projection.thread.settledAt,
    unsettledAt: projection.thread.unsettledAt ?? null,
    snoozedUntil: projection.thread.snoozedUntil ?? null,
    snoozedAt: projection.thread.snoozedAt ?? null,
    pinnedAt: projection.thread.pinnedAt ?? null,

    autoSettleDisabledAt: projection.thread.autoSettleDisabledAt ?? null,
    pinOrderKey: projection.thread.pinOrderKey ?? null,
    ...(projection.thread.tags?.length ? { tags: projection.thread.tags } : {}),
    lastVisitedAt: projection.thread.lastVisitedAt,
    opens: projection.thread.opens ?? null,
    titleRegeneration: projection.thread.titleRegeneration ?? null,
    limitRecovery: projection.thread.limitRecovery ?? null,
    deletedAt: projection.thread.deletedAt,
  };
}

function providerInstanceHistoryForShell(input: {
  readonly threadId: ThreadId;
  readonly providerThreads: ReadonlyArray<
    OrchestrationV2ThreadProjection["providerThreads"][number]
  >;
}): ReadonlyArray<ProviderInstanceId> {
  const history: Array<ProviderInstanceId> = [];
  for (const providerThread of input.providerThreads
    .filter((thread) => thread.appThreadId === input.threadId && thread.ownerNodeId === null)
    .toSorted(
      (left, right) =>
        DateTime.toEpochMillis(left.createdAt) - DateTime.toEpochMillis(right.createdAt) ||
        left.id.localeCompare(right.id),
    )) {
    if (!history.includes(providerThread.providerInstanceId)) {
      history.push(providerThread.providerInstanceId);
    }
  }
  return history;
}

function activeProviderGoalForShell(
  providerThreads: ReadonlyArray<OrchestrationV2ThreadProjection["providerThreads"][number]>,
  activeProviderThreadId: OrchestrationV2ThreadProjection["thread"]["activeProviderThreadId"],
): OrchestrationV2ThreadShell["goal"] {
  return providerThreads.find((thread) => thread.id === activeProviderThreadId)?.goal ?? null;
}

function isSilentRunForShell(
  run: OrchestrationV2ThreadProjection["runs"][number],
  projection: Pick<OrchestrationV2ThreadProjection, "messages" | "turnItems">,
): boolean {
  return (
    run.status !== "failed" &&
    projection.messages.some(
      (message) => message.id === run.userMessageId && idleCompactionOf(message) !== undefined,
    ) &&
    !projection.turnItems.some(
      (item) => item.runId === run.id && item.type === "compaction" && item.status === "failed",
    )
  );
}

function isInterruptibleRunForShell(run: OrchestrationV2ThreadProjection["runs"][number]): boolean {
  return run.status === "preparing" || run.status === "starting" || run.status === "running";
}

type ShellActivityRunStatus = "preparing" | "running" | "starting" | "waiting";

function isActivityRunForShell(
  run: OrchestrationV2ThreadProjection["runs"][number],
): run is OrchestrationV2ThreadProjection["runs"][number] & {
  readonly status: ShellActivityRunStatus;
} {
  return isInterruptibleRunForShell(run) || run.status === "waiting";
}

type ShellThreadState = {
  readonly thread: OrchestrationV2ThreadProjection["thread"];
  readonly latestRunId: RunId | null;
  readonly latestRunStatus: OrchestrationV2ShellThreadStatus;
  readonly latestRunRequestedAt: DateTime.Utc | null;
  readonly latestRunStartedAt: DateTime.Utc | null;
  readonly latestRunCompletedAt: DateTime.Utc | null;
  readonly latestRunStopRequested: boolean;
  readonly activeRunId: RunId | null;
  readonly blockedOnThreadWait: boolean;
  readonly activityRunStatus: ShellActivityRunStatus | null;
  readonly activityRunStartedAt: DateTime.Utc | null;
  readonly lastError: string | null;
  readonly lastErrorClass: OrchestrationV2ThreadShell["lastErrorClass"];
  readonly lastErrorCode: OrchestrationV2ThreadShell["lastErrorCode"];
  readonly usageLimitResetAt: OrchestrationV2ThreadShell["usageLimitResetAt"];
  readonly pendingRuntimeRequest: OrchestrationV2PendingRuntimeRequestSummary | null;
  readonly latestUserMessageAt: DateTime.Utc | null;
  readonly latestUserAuthoredMessageAt: DateTime.Utc | null;
  readonly hasActionableProposedPlan: boolean;
  readonly pendingBackgroundTasks: OrchestrationV2ThreadShell["pendingBackgroundTasks"];
  readonly providerInstanceHistory: OrchestrationV2ThreadShell["providerInstanceHistory"];
  readonly goal: OrchestrationV2ThreadShell["goal"];
  readonly itemCount: number;
  readonly runlessItemCount: number;
  readonly updatedAt: OrchestrationV2ThreadProjection["updatedAt"];
  readonly runOrdinalById: ReadonlyMap<RunId, number>;
  readonly itemCountByRunId: ReadonlyMap<RunId, number>;
};

function shellStatusFromStoredRunStatus(status: string | null): OrchestrationV2ShellThreadStatus {
  switch (status) {
    case null:
      return "idle";
    case "preparing":
    case "queued":
    case "starting":
    case "running":
    case "waiting":
    case "completed":
    case "interrupted":
    case "failed":
    case "cancelled":
    case "rolled_back":
      return status;
    default:
      return "failed";
  }
}

function itemCountThroughRun(input: {
  readonly state: ShellThreadState;
  readonly runId: RunId;
}): number {
  const runOrdinal = input.state.runOrdinalById.get(input.runId);
  if (runOrdinal === undefined) {
    return 0;
  }

  let count = input.state.thread.historyOrigin === "v1_import" ? input.state.runlessItemCount : 0;
  for (const [runId, itemCount] of input.state.itemCountByRunId) {
    const itemRunOrdinal = input.state.runOrdinalById.get(runId);
    if (itemRunOrdinal !== undefined && itemRunOrdinal <= runOrdinal) {
      count += itemCount;
    }
  }
  return count;
}

function shellForkSourceIds(
  rows: ReadonlyArray<{ readonly forked_from_run_source_thread_id: string | null }>,
): ReadonlyArray<ThreadId> {
  return [
    ...new Set(
      rows.flatMap((row) =>
        row.forked_from_run_source_thread_id === null
          ? []
          : [ThreadId.make(row.forked_from_run_source_thread_id)],
      ),
    ),
  ];
}

function visibleItemCountForShell(input: {
  readonly threadId: ThreadId;
  readonly statesByThreadId: ReadonlyMap<ThreadId, ShellThreadState>;
  readonly seenThreadIds?: ReadonlySet<ThreadId>;
}): number {
  const state = input.statesByThreadId.get(input.threadId);
  if (state === undefined) {
    return 0;
  }

  const forkedFrom = inheritedRunFork(state.thread);
  if (forkedFrom === null) {
    return state.itemCount;
  }

  const seenThreadIds = input.seenThreadIds ?? new Set<ThreadId>();
  if (seenThreadIds.has(state.thread.id)) {
    return state.itemCount;
  }

  const sourceState = input.statesByThreadId.get(forkedFrom.threadId);
  if (sourceState === undefined) {
    return state.itemCount;
  }

  const inheritedPrefixCount =
    inheritedRunFork(sourceState.thread) !== null
      ? visibleItemCountForShell({
          threadId: sourceState.thread.id,
          statesByThreadId: input.statesByThreadId,
          seenThreadIds: new Set([...seenThreadIds, state.thread.id]),
        }) - sourceState.itemCount
      : 0;

  return (
    inheritedPrefixCount +
    itemCountThroughRun({ state: sourceState, runId: forkedFrom.runId }) +
    1 +
    state.itemCount
  );
}

function shellFromState(input: {
  readonly state: ShellThreadState;
  readonly visibleItemCount: number;
}): OrchestrationV2ThreadShell {
  return {
    createdBy: input.state.thread.createdBy,
    creationSource: input.state.thread.creationSource,
    id: input.state.thread.id,
    projectId: input.state.thread.projectId,
    title: input.state.thread.title,
    providerInstanceId: input.state.thread.providerInstanceId,
    modelSelection: input.state.thread.modelSelection,
    runtimeMode: input.state.thread.runtimeMode,
    interactionMode: input.state.thread.interactionMode,
    branch: input.state.thread.branch,
    worktreePath: input.state.thread.worktreePath,
    pullRequests: threadPullRequestsOf(input.state.thread),
    ...(input.state.thread.linkedPullRequest === undefined
      ? {}
      : { linkedPullRequest: input.state.thread.linkedPullRequest }),
    ...(input.state.thread.branchPullRequest === undefined
      ? {}
      : { branchPullRequest: input.state.thread.branchPullRequest }),
    ...(input.state.thread.activeOrderKey === undefined
      ? {}
      : { activeOrderKey: input.state.thread.activeOrderKey }),
    lineage: input.state.thread.lineage,
    forkedFrom: input.state.thread.forkedFrom,
    activeProviderThreadId: input.state.thread.activeProviderThreadId,
    ...(input.state.thread.historyOrigin === undefined
      ? {}
      : { historyOrigin: input.state.thread.historyOrigin }),
    ...(input.state.thread.linkOrigin === undefined
      ? {}
      : { linkOrigin: input.state.thread.linkOrigin }),
    ...(input.state.thread.delegatedFrom === undefined
      ? {}
      : { delegatedFrom: input.state.thread.delegatedFrom }),
    latestRunId: input.state.latestRunId,
    latestRunRequestedAt: input.state.latestRunRequestedAt,
    latestRunStartedAt: input.state.latestRunStartedAt,
    latestRunCompletedAt: input.state.latestRunCompletedAt,
    latestRunStopRequested: input.state.latestRunStopRequested,
    activeRunId: input.state.activeRunId,
    blockedOnThreadWait: input.state.blockedOnThreadWait,
    activityRunStatus: input.state.activityRunStatus,
    activityRunStartedAt: input.state.activityRunStartedAt,
    status: input.state.latestRunStatus,
    lastError: input.state.lastError,
    lastErrorClass: input.state.lastErrorClass,
    lastErrorCode: input.state.lastErrorCode,
    usageLimitResetAt: input.state.usageLimitResetAt,
    pendingRuntimeRequest: input.state.pendingRuntimeRequest,
    pendingGuestMessageCount: pendingGuestMessageCount(input.state.thread.guestShare),
    latestVisibleMessage: null,
    latestUserMessageAt: input.state.latestUserMessageAt,
    latestUserAuthoredMessageAt: input.state.latestUserAuthoredMessageAt,
    hasActionableProposedPlan: input.state.hasActionableProposedPlan,
    pendingBackgroundTasks: input.state.pendingBackgroundTasks,
    providerInstanceHistory: input.state.providerInstanceHistory,
    goal: input.state.goal,
    itemCount: input.state.itemCount,
    visibleItemCount: input.visibleItemCount,
    createdAt: input.state.thread.createdAt,
    updatedAt: input.state.updatedAt,
    archivedAt: input.state.thread.archivedAt,
    settledOverride: input.state.thread.settledOverride,
    settledAt: input.state.thread.settledAt,
    unsettledAt: input.state.thread.unsettledAt ?? null,
    snoozedUntil: input.state.thread.snoozedUntil ?? null,
    snoozedAt: input.state.thread.snoozedAt ?? null,
    pinnedAt: input.state.thread.pinnedAt ?? null,

    autoSettleDisabledAt: input.state.thread.autoSettleDisabledAt ?? null,
    pinOrderKey: input.state.thread.pinOrderKey ?? null,
    ...(input.state.thread.tags?.length ? { tags: input.state.thread.tags } : {}),
    lastVisitedAt: input.state.thread.lastVisitedAt,
    opens: input.state.thread.opens ?? null,
    titleRegeneration: input.state.thread.titleRegeneration ?? null,
    limitRecovery: input.state.thread.limitRecovery ?? null,
    deletedAt: input.state.thread.deletedAt,
  };
}

/** Shared recovery codecs and propagation; storage supplies raw candidate rows. */
export function makeProjectionRecovery(reader: ProjectionRecoveryRows) {
  const getLimitRecoveryCandidates = Effect.fn("ProjectionStore.getLimitRecoveryCandidates")(
    function* (options: Parameters<ProjectionStoreV2Shape["getLimitRecoveryCandidates"]>[0]) {
      const rows = yield* reader.limitCandidates(options);
      const candidates: Array<ProjectionLimitRecoveryCandidate> = [];
      for (const row of rows) {
        const thread = yield* decodeThreadPayload(row.payload_json);
        const item = yield* decodeTurnItemPayload(row.failure_payload_json);
        const summary = threadErrorSummary(
          item.type === "error" ? item.failure : null,
          row.last_error,
        );
        if (summary.lastErrorClass !== "usage_limit") continue;
        candidates.push({
          id: thread.id,
          status: "failed",
          lastErrorClass: summary.lastErrorClass,
          usageLimitResetAt: summary.usageLimitResetAt,
          latestRunId: RunId.make(row.run_id),
          latestRunCompletedAt:
            row.completed_at === null ? null : DateTime.makeUnsafe(row.completed_at),
          updatedAt: thread.updatedAt,
          archivedAt: thread.archivedAt,
          settledOverride: thread.settledOverride,
          pendingRuntimeRequest: null,
          limitRecovery: thread.limitRecovery ?? null,
          snoozedUntil: thread.snoozedUntil ?? null,
        });
      }
      return candidates;
    },
    Effect.mapError((cause) => new ProjectionStoreSetupError({ cause })),
  );

  const getLimitRecoveryWakeAt = Effect.fn("ProjectionStore.getLimitRecoveryWakeAt")(
    function* (now: DateTime.Utc) {
      const nowIso = DateTime.formatIso(now);
      const rows = yield* reader.wakeAt(nowIso);
      const wakes = [rows[0]?.reset_ms, rows[0]?.snooze_ms].filter(
        (value): value is number => value != null,
      );
      return wakes.length === 0 ? null : Math.round(Math.min(...wakes));
    },
    Effect.mapError((cause) => new ProjectionStoreSetupError({ cause })),
  );

  const getRecoveryThreadIds = Effect.fn("ProjectionStore.getRecoveryThreadIds")(
    function* (
      kind: ProjectionRecoveryKind,
      projects?: {
        readonly include?: ReadonlyArray<string>;
        readonly exclude?: ReadonlyArray<string>;
      },
    ) {
      const rows = yield* reader.recoveryThreads(kind, projects);
      return rows.map((row) => ThreadId.make(row.thread_id));
    },
    Effect.mapError((cause) => new ProjectionStoreSetupError({ cause })),
  );

  const getUnreadableThreadIds = Effect.fn("ProjectionStore.getUnreadableThreadIds")(
    function* () {
      const actualThreadIds = new Set<ThreadId>();
      const unreadable = new Set<ThreadId>();
      const forkChildren = new Map<ThreadId, Array<ThreadId>>();
      const tables: ReadonlyArray<{
        readonly name: ProjectionAuditTable;
        readonly id: string;
        readonly decode: (
          payload: string,
          entityId: string,
        ) => Effect.Effect<unknown, Schema.SchemaError>;
      }> = [
        {
          name: "threads",
          id: "thread_id",
          decode: (payload: string, entityId: string) =>
            decodeThreadPayload(payload).pipe(
              Effect.tap((thread) =>
                Effect.sync(() => {
                  if (thread.forkedFrom?.type !== "run") return;
                  const sourceId = thread.forkedFrom.threadId;
                  const children = forkChildren.get(sourceId) ?? [];
                  children.push(ThreadId.make(entityId));
                  forkChildren.set(sourceId, children);
                }),
              ),
            ),
        },
        { name: "runs", id: "run_id", decode: (payload) => decodeRunPayload(payload) },
        {
          name: "run_attempts",
          id: "attempt_id",
          decode: (payload) => decodeRunAttemptPayload(payload),
        },
        { name: "nodes", id: "node_id", decode: (payload) => decodeNodePayload(payload) },
        {
          name: "subagents",
          id: "subagent_id",
          decode: (payload) => decodeSubagentPayload(payload),
        },
        {
          name: "provider_sessions",
          id: "provider_session_id",
          decode: (payload) => decodeProviderSessionPayload(payload),
        },
        {
          name: "provider_threads",
          id: "provider_thread_id",
          decode: (payload) => decodeProviderThreadPayload(payload),
        },
        {
          name: "provider_turns",
          id: "provider_turn_id",
          decode: (payload) => decodeProviderTurnPayload(payload),
        },
        {
          name: "runtime_requests",
          id: "runtime_request_id",
          decode: (payload) => decodeRuntimeRequestPayload(payload),
        },
        {
          name: "messages",
          id: "message_id",
          decode: (payload) => decodeMessagePayload(payload),
        },
        { name: "plans", id: "plan_id", decode: (payload) => decodePlanJson(payload) },
        {
          name: "turn_items",
          id: "turn_item_id",
          decode: (payload) => decodeTurnItemPayload(payload),
        },
        {
          name: "checkpoint_scopes",
          id: "scope_id",
          decode: (payload) => decodeCheckpointScopePayload(payload),
        },
        {
          name: "checkpoints",
          id: "checkpoint_id",
          decode: (payload) => decodeCheckpointPayload(payload),
        },
        {
          name: "context_handoffs",
          id: "context_handoff_id",
          decode: (payload) => decodeContextHandoffPayload(payload),
        },
        {
          name: "context_transfers",
          id: "context_transfer_id",
          decode: (payload) => decodeContextTransferPayload(payload),
        },
      ];
      const pageSize = 500;
      for (const table of tables) {
        let afterRowId: string | null = null;
        while (true) {
          const page: {
            readonly rows: ReadonlyArray<{
              readonly row_id: string;
              readonly entity_id: string;
              readonly thread_id: string | null;
              readonly target_thread_id: string | null;
              readonly payload_json: string;
            }>;
            readonly next: string | null;
          } = yield* reader.unreadablePage(table.name, table.id, afterRowId, pageSize);
          for (const row of page.rows) {
            if (table.name === "threads") actualThreadIds.add(ThreadId.make(row.entity_id));
            const decoded = yield* Effect.exit(
              Effect.suspend(() => table.decode(row.payload_json, row.entity_id)),
            );
            if (decoded._tag === "Success") continue;
            if (table.name === "provider_sessions") {
              const bindings = yield* reader.bindings(row.entity_id);
              for (const binding of bindings) unreadable.add(ThreadId.make(binding.thread_id));
            } else {
              if (row.thread_id !== null) unreadable.add(ThreadId.make(row.thread_id));
              if (row.target_thread_id !== null)
                unreadable.add(ThreadId.make(row.target_thread_id));
            }
            if (table.name === "provider_threads") {
              const owners = yield* reader.providerThreadOwners(row.entity_id);
              for (const owner of owners) unreadable.add(ThreadId.make(owner.thread_id));
            }
          }
          if (page.next === null) break;
          afterRowId = page.next;
          yield* Effect.yieldNow;
        }
      }
      const pending = [
        ...unreadable,
        ...[...forkChildren.keys()].filter((sourceId) => !actualThreadIds.has(sourceId)),
      ];
      for (let index = 0; index < pending.length; index += 1) {
        for (const childId of forkChildren.get(pending[index]!) ?? []) {
          if (unreadable.has(childId)) continue;
          unreadable.add(childId);
          pending.push(childId);
        }
      }
      return [...unreadable].filter((threadId) => actualThreadIds.has(threadId)).sort();
    },
    Effect.mapError((cause) => new ProjectionStoreSetupError({ cause })),
  );

  return {
    getLimitRecoveryCandidates,
    getLimitRecoveryWakeAt,
    getRecoveryThreadIds,
    getUnreadableThreadIds,
  };
}
export function makeCanonicalProjection(rows: ProjectionRecordRows) {
  return Effect.fn("ProjectionStore.readCanonicalRecords")(
    function* (
      threadId: ThreadId,
      window?: ProjectionRecordWindow,
      fields: ReadonlyArray<ProjectionRecordField> = projectionRecordFields,
      filter?: ProjectionRecordFilter,
    ) {
      const data = yield* rows.read({
        thread_id: threadId,
        fields,
        ...(filter === undefined ? {} : { filter }),
        ...(window === undefined ? {} : { window }),
      });
      if (data.thread === null) return yield* new ProjectionStoreThreadNotFoundError({ threadId });
      const payloads = (field: ProjectionRecordField) =>
        data.rows[field].map((payload_json) => ({ payload_json }));
      const thread = yield* decodeThreadPayload(data.thread);
      const values = yield* Effect.all({
        runs: decodeRows(decodeRunPayload, threadId)(payloads("runs")),
        attempts: decodeRows(decodeRunAttemptPayload, threadId)(payloads("attempts")),
        nodes: decodeRows(decodeNodePayload, threadId)(payloads("nodes")),
        subagents: decodeRows(decodeSubagentPayload, threadId)(payloads("subagents")),
        providerSessions: decodeRows(
          decodeProviderSessionPayload,
          threadId,
        )(payloads("providerSessions")),
        providerThreads: decodeRows(
          decodeProviderThreadPayload,
          threadId,
        )(payloads("providerThreads")),
        providerTurns: decodeRows(decodeProviderTurnPayload, threadId)(payloads("providerTurns")),
        runtimeRequests: decodeRows(
          decodeRuntimeRequestPayload,
          threadId,
        )(payloads("runtimeRequests")),
        messages: decodeRows(decodeMessagePayload, threadId)(payloads("messages")),
        plans: decodeRows(decodePlanJson, threadId)(payloads("plans")),
        turnItems: decodeRows(decodeTurnItemPayload, threadId)(payloads("turnItems")),
        checkpointScopes: decodeRows(
          decodeCheckpointScopePayload,
          threadId,
        )(payloads("checkpointScopes")),
        checkpoints: decodeRows(decodeCheckpointPayload, threadId)(payloads("checkpoints")),
        contextHandoffs: decodeRows(
          decodeContextHandoffPayload,
          threadId,
        )(payloads("contextHandoffs")),
        contextTransfers: decodeRows(
          decodeContextTransferPayload,
          threadId,
        )(payloads("contextTransfers")),
      });
      values.messages = sortMessagesByTurnItemOrder(
        values.messages,
        values.turnItems,
        new Map(data.messageOrdinals.map(({ messageId, ordinal }) => [messageId, ordinal])),
      );
      const deferred = new Map(data.deferredWork.map((row) => [row.rowIndex, row]));
      values.turnItems = values.turnItems.map((item, index) => {
        const row = deferred.get(index);
        return row === undefined
          ? item
          : {
              ...item,
              deferredWork: {
                cursor: encodeThreadHistoryCursor({
                  snapshotSequence: 0,
                  sourceThreadId: threadId,
                  sourceItemId: TurnItemId.make(row.lastItemId),
                  position: row.lastOrdinal,
                  inclusive: true,
                }),
                itemCount: row.itemCount,
                lastOrdinal: row.lastOrdinal,
              },
            };
      });
      return {
        thread,
        ...values,
        visibleTurnItems: [],
        updatedAt: thread.updatedAt,
      } satisfies OrchestrationV2ThreadProjection;
    },
    (effect, threadId, _window?, _fields?, _filter?) =>
      effect.pipe(
        Effect.mapError((cause) =>
          isProjectionStoreThreadNotFoundError(cause) || isProjectionStoreReadError(cause)
            ? cause
            : new ProjectionStoreReadError({ threadId, cause }),
        ),
      ),
  );
}

export function makeProjectionRecords(
  rows: ProjectionRecordRows,
): ProjectionStoreV2Shape["getThreadRecords"] {
  const canonical = makeCanonicalProjection(rows);
  return Effect.fn("ProjectionStore.getThreadRecords")(function* <K extends ProjectionRecordField>(
    threadId: ThreadId,
    fields: ReadonlyArray<K>,
    filter?: ProjectionRecordFilter,
  ) {
    const values = yield* canonical(threadId, undefined, fields, filter);
    return Object.fromEntries([
      ["thread", values.thread],
      ...fields.map((field) => [field, values[field]]),
    ]) as ProjectionRecords<K>;
  });
}

/** Provider control keeps its original codecs and branch order inside one backend snapshot. */
export function makeProjectionControls(
  rows: ProjectionControlRows,
  lookups: ReturnType<typeof makeProjectionLookups>,
) {
  const controlReadError = (threadId: ThreadId) => (cause: unknown) =>
    isProjectionStoreThreadNotFoundError(cause)
      ? cause
      : new ProjectionStoreReadError({ threadId, cause });
  const payloads = (values: ReadonlyArray<string>) =>
    values.map((payload_json) => ({ payload_json }));
  const { getThread, requireThread, getRuntimeRequest } = lookups;

  const getPendingNativeUserInputs: ProjectionStoreV2Shape["getPendingNativeUserInputs"] = (
    threadId,
    providerTurnId,
  ) =>
    rows
      .withReadTransaction(
        Effect.gen(function* () {
          const selected = yield* rows.query({
            kind: "pending_user_requests",
            thread_id: threadId,
            provider_turn_id: providerTurnId,
          });
          const runtimeRequests = (yield* decodeRows(
            decodeRuntimeRequestPayload,
            threadId,
          )(payloads(selected.requests))).filter(
            (request) => request.responseCapability.type !== "message",
          );
          if (runtimeRequests.length === 0) return { runtimeRequests, nodes: [], turnItems: [] };
          const related = yield* rows.query({
            kind: "pending_user_input_details",
            thread_id: threadId,
            provider_turn_id: providerTurnId,
            node_ids: runtimeRequests.map((request) => request.nodeId),
          });
          const requestIds = new Set(runtimeRequests.map((request) => request.id));
          return {
            runtimeRequests,
            nodes: yield* decodeRows(decodeNodePayload, threadId)(payloads(related.nodes)),
            turnItems: (yield* decodeRows(
              decodeTurnItemPayload,
              threadId,
            )(payloads(related.items))).filter(
              (item) => item.type === "user_input_request" && requestIds.has(item.requestId),
            ),
          };
        }),
      )
      .pipe(Effect.mapError(controlReadError(threadId)));

  const getProviderControlContext: ProjectionStoreV2Shape["getProviderControlContext"] = (
    threadId,
    target,
  ) =>
    rows
      .withReadTransaction(
        Effect.gen(function* () {
          yield* requireThread(threadId);
          const selected = yield* rows.query({
            kind: "provider_control",
            thread_id: threadId,
            provider_thread_id: target.providerThreadId,
            provider_turn_id: target.providerTurnId,
            ...(target.attemptId === undefined ? {} : { attempt_id: target.attemptId }),
            ...(target.messageId === undefined ? {} : { message_id: target.messageId }),
          });
          const providerThread =
            selected.providerThreads[0] === undefined
              ? undefined
              : yield* decodeProviderThreadPayload(selected.providerThreads[0]);
          const providerTurn =
            selected.providerTurns[0] === undefined
              ? undefined
              : yield* decodeProviderTurnPayload(selected.providerTurns[0]);
          const attempt =
            selected.attempts[0] === undefined
              ? undefined
              : yield* decodeRunAttemptPayload(selected.attempts[0]);
          const message =
            selected.messages[0] === undefined
              ? undefined
              : yield* decodeMessagePayload(selected.messages[0]);
          let run: OrchestrationV2Run | undefined;
          if (target.messageId !== undefined && providerTurn !== undefined) {
            const selectedRun = yield* rows.query({
              kind: "provider_control_run",
              thread_id: threadId,
              active_attempt_id: providerTurn.runAttemptId,
            });
            if (selectedRun.runs[0] !== undefined)
              run = yield* decodeRunPayload(selectedRun.runs[0]);
          }
          return { providerThread, providerTurn, attempt, message, run };
        }),
      )
      .pipe(Effect.mapError(controlReadError(threadId)));

  const getRunningTurnContext: ProjectionStoreV2Shape["getRunningTurnContext"] = (threadId) =>
    rows
      .withReadTransaction(
        Effect.gen(function* () {
          yield* requireThread(threadId);
          const selected = yield* rows.query({ kind: "running_run", thread_id: threadId });
          const run =
            selected.runs[0] === undefined ? undefined : yield* decodeRunPayload(selected.runs[0]);
          if (run === undefined) return { run, providerThread: undefined, providerTurn: undefined };
          const related = yield* rows.query({
            kind: "running_turn_details",
            provider_thread_id: run.providerThreadId,
            run_attempt_id: run.activeAttemptId,
            node_id: run.rootNodeId,
          });
          return {
            run,
            providerThread:
              related.providerThreads[0] === undefined
                ? undefined
                : yield* decodeProviderThreadPayload(related.providerThreads[0]),
            providerTurn:
              related.providerTurns[0] === undefined
                ? undefined
                : yield* decodeProviderTurnPayload(related.providerTurns[0]),
          };
        }),
      )
      .pipe(Effect.mapError(controlReadError(threadId)));

  const getThreadProviderContext: ProjectionStoreV2Shape["getThreadProviderContext"] = (
    threadId,
    targetInstanceId,
  ) =>
    rows
      .withReadTransaction(
        Effect.gen(function* () {
          const thread = yield* getThread(threadId);
          const selected = yield* rows.query({
            kind: "thread_providers",
            thread_id: threadId,
            active_provider_thread_id: thread.activeProviderThreadId,
            ...(targetInstanceId === undefined ? {} : { target_instance_id: targetInstanceId }),
          });
          return {
            thread,
            providerSessions: yield* decodeRows(
              decodeProviderSessionPayload,
              threadId,
            )(payloads(selected.sessions)),
            providerThreads: yield* decodeRows(
              decodeProviderThreadPayload,
              threadId,
            )(payloads(selected.providerThreads)),
          };
        }),
      )
      .pipe(Effect.mapError(controlReadError(threadId)));

  const getRuntimeResponseContext: ProjectionStoreV2Shape["getRuntimeResponseContext"] = (
    threadId,
    requestId,
  ) =>
    rows
      .withReadTransaction(
        Effect.gen(function* () {
          yield* requireThread(threadId);
          const request = yield* getRuntimeRequest(threadId, requestId);
          if (request === undefined)
            return { request, node: undefined, item: undefined, session: undefined };
          const selected = yield* rows.query({
            kind: "runtime_response",
            thread_id: threadId,
            request_id: requestId,
            node_id: request.nodeId,
            ...(request.responseCapability.type === "live"
              ? { session_id: request.responseCapability.providerSessionId }
              : {}),
          });
          return {
            request,
            node:
              selected.nodes[0] === undefined
                ? undefined
                : yield* decodeNodePayload(selected.nodes[0]),
            item:
              selected.items[0] === undefined
                ? undefined
                : yield* decodeTurnItemPayload(selected.items[0]),
            session:
              selected.sessions[0] === undefined
                ? undefined
                : yield* decodeProviderSessionPayload(selected.sessions[0]),
          };
        }),
      )
      .pipe(Effect.mapError(controlReadError(threadId)));

  return {
    getPendingNativeUserInputs,
    getProviderControlContext,
    getRunningTurnContext,
    getThreadProviderContext,
    getRuntimeResponseContext,
  };
}

/** The same public lookup semantics and codecs are used by both database readers. */
export function makeProjectionLookups(rows: ProjectionLookupRows) {
  const readError = (threadId: ThreadId) => (cause: unknown) =>
    isProjectionStoreThreadNotFoundError(cause)
      ? cause
      : new ProjectionStoreReadError({ threadId, cause });
  const shellError = readError(ThreadId.make("thread:shell"));
  const requireThread = Effect.fn("ProjectionStore.requireThread")(function* (threadId: ThreadId) {
    const result = yield* rows.query({ kind: "thread_exists", thread_id: threadId });
    if (!result.owner_exists) return yield* new ProjectionStoreThreadNotFoundError({ threadId });
  });
  const getThread: ProjectionStoreV2Shape["getThread"] = Effect.fn("ProjectionStore.getThread")(
    function* (threadId) {
      const result = yield* rows.query({ kind: "thread", thread_id: threadId });
      if (result.payloads[0] === undefined)
        return yield* new ProjectionStoreThreadNotFoundError({ threadId });
      return yield* decodeThreadPayload(result.payloads[0]);
    },
    (effect, threadId) => effect.pipe(Effect.mapError(readError(threadId))),
  );
  const getPlan: ProjectionStoreV2Shape["getPlan"] = Effect.fn("ProjectionStore.getPlan")(
    function* (threadId, planId) {
      const result = yield* rows.query({ kind: "plan", thread_id: threadId, record_id: planId });
      if (!result.owner_exists) return yield* new ProjectionStoreThreadNotFoundError({ threadId });
      return result.payloads[0] === undefined
        ? undefined
        : yield* decodePlanJson(result.payloads[0]);
    },
    (effect, threadId) => effect.pipe(Effect.mapError(readError(threadId))),
  );
  const getRuntimeRequest: ProjectionStoreV2Shape["getRuntimeRequest"] = Effect.fn(
    "ProjectionStore.getRuntimeRequest",
  )(
    function* (threadId, requestId) {
      const result = yield* rows.query({
        kind: "runtime_request",
        thread_id: threadId,
        record_id: requestId,
      });
      if (!result.owner_exists) return yield* new ProjectionStoreThreadNotFoundError({ threadId });
      return result.payloads[0] === undefined
        ? undefined
        : yield* decodeRuntimeRequestPayload(result.payloads[0]);
    },
    (effect, threadId) => effect.pipe(Effect.mapError(readError(threadId))),
  );
  const getTurnItem: ProjectionStoreV2Shape["getTurnItem"] = Effect.fn(
    "ProjectionStore.getTurnItem",
  )(
    function* ({ threadId, itemId }) {
      const result = yield* rows.query({
        kind: "turn_item",
        thread_id: threadId,
        record_id: itemId,
      });
      return result.payloads[0] === undefined
        ? null
        : yield* decodeTurnItemPayload(result.payloads[0]);
    },
    (effect, input) => effect.pipe(Effect.mapError(readError(input.threadId))),
  );
  const getMessagesById: ProjectionStoreV2Shape["getMessagesById"] = Effect.fn(
    "ProjectionStore.getMessagesById",
  )(
    function* (threadId, messageIds) {
      if (messageIds.length === 0) return new Map<MessageId, OrchestrationV2ConversationMessage>();
      const result = yield* rows.query({
        kind: "messages",
        thread_id: threadId,
        record_ids: messageIds,
      });
      const messages = yield* decodeRows(
        decodeMessagePayload,
        threadId,
      )(result.payloads.map((payload_json) => ({ payload_json })));
      return new Map(messages.map((message) => [message.id, message]));
    },
    (effect, threadId) => effect.pipe(Effect.mapError(readError(threadId))),
  );
  const hasUnpairedRunInterruptRequest: ProjectionStoreV2Shape["hasUnpairedRunInterruptRequest"] =
    Effect.fn("ProjectionStore.hasUnpairedRunInterruptRequest")(
      function* (threadId, requestId, resultId) {
        return (yield* rows.query({
          kind: "unpaired_interrupt",
          thread_id: threadId,
          request_id: requestId,
          result_id: resultId,
        })).found;
      },
      (effect, threadId) => effect.pipe(Effect.mapError(readError(threadId))),
    );
  const isNodeStored: ProjectionStoreV2Shape["isNodeStored"] = Effect.fn(
    "ProjectionStore.isNodeStored",
  )(
    function* (node) {
      const payloadJson = yield* encodeNodePayload(node);
      const result = yield* rows.query({ kind: "node", record_id: node.id });
      return result.payloads[0] === payloadJson;
    },
    (effect, node) => effect.pipe(Effect.mapError(readError(node.threadId))),
  );
  const getThreadLinkMetadata: ProjectionStoreV2Shape["getThreadLinkMetadata"] = Effect.fn(
    "ProjectionStore.getThreadLinkMetadata",
  )(function* (threadIds) {
    if (threadIds.length === 0) return {};
    const result = yield* rows.query({ kind: "thread_metadata", thread_ids: threadIds });
    return Object.fromEntries(
      result.metadata.map((row) => [
        ThreadId.make(row.thread_id),
        { title: row.title, projectId: ProjectId.make(row.project_id) },
      ]),
    );
  }, Effect.mapError(shellError));
  const getThreadTitles: ProjectionStoreV2Shape["getThreadTitles"] = Effect.fn(
    "ProjectionStore.getThreadTitles",
  )(function* (threadIds) {
    if (threadIds.length === 0) return {};
    const result = yield* rows.query({ kind: "thread_metadata", thread_ids: threadIds });
    return Object.fromEntries(
      result.metadata.map((row) => [ThreadId.make(row.thread_id), row.title]),
    );
  }, Effect.mapError(shellError));
  const getRunMessage: ProjectionStoreV2Shape["getRunMessage"] = Effect.fn(
    "ProjectionStore.getRunMessage",
  )(
    function* (threadId, runId) {
      const result = yield* rows.query({ kind: "run_message", thread_id: threadId, run_id: runId });
      if (!result.owner_exists) return yield* new ProjectionStoreThreadNotFoundError({ threadId });
      return result.payloads[0] === undefined
        ? undefined
        : yield* decodeMessagePayload(result.payloads[0]);
    },
    (effect, threadId) => effect.pipe(Effect.mapError(readError(threadId))),
  );
  const canStartQueuedRun: ProjectionStoreV2Shape["canStartQueuedRun"] = Effect.fn(
    "ProjectionStore.canStartQueuedRun",
  )(
    function* (threadId) {
      const result = yield* rows.query({ kind: "can_start_queued_run", thread_id: threadId });
      if (!result.owner_exists) return yield* new ProjectionStoreThreadNotFoundError({ threadId });
      return result.found;
    },
    (effect, threadId) => effect.pipe(Effect.mapError(readError(threadId))),
  );
  const getHeldQueuedRuns: ProjectionStoreV2Shape["getHeldQueuedRuns"] = Effect.fn(
    "ProjectionStore.getHeldQueuedRuns",
  )(
    function* (threadId) {
      const result = yield* rows.query({ kind: "held_queued_runs", thread_id: threadId });
      return yield* decodeRows(
        decodeRunPayload,
        threadId,
      )(result.payloads.map((payload_json) => ({ payload_json })));
    },
    (effect, threadId) => effect.pipe(Effect.mapError(readError(threadId))),
  );
  const getMessageCount: ProjectionStoreV2Shape["getMessageCount"] = (threadId) =>
    rows.query({ kind: "message_count", thread_id: threadId }).pipe(
      Effect.map((result) => result.count),
      Effect.mapError(readError(threadId)),
    );
  const getSubagentCount: ProjectionStoreV2Shape["getSubagentCount"] = (threadId) =>
    rows.query({ kind: "subagent_count", thread_id: threadId }).pipe(
      Effect.map((result) => result.count),
      Effect.mapError(readError(threadId)),
    );
  const getNextTurnItemOrdinal: ProjectionStoreV2Shape["getNextTurnItemOrdinal"] = (threadId) =>
    rows.query({ kind: "next_turn_item_ordinal", thread_id: threadId }).pipe(
      Effect.map((result) => result.count),
      Effect.mapError(readError(threadId)),
    );
  const listAppOwnedChildThreadIds: ProjectionStoreV2Shape["listAppOwnedChildThreadIds"] = (
    threadId,
  ) =>
    rows.query({ kind: "app_owned_child_threads", thread_id: threadId }).pipe(
      Effect.map((result) => result.ids.map((id) => ThreadId.make(id))),
      Effect.mapError(readError(threadId)),
    );
  const getTurnStartHistory: ProjectionStoreV2Shape["getTurnStartHistory"] = Effect.fn(
    "ProjectionStore.getTurnStartHistory",
  )(
    function* (threadId, runIds) {
      const result = yield* rows.query({
        kind: "turn_start_history",
        thread_id: threadId,
        ...(runIds === undefined ? {} : { run_ids: runIds }),
      });
      return yield* decodeRows(
        decodeTurnItemPayload,
        threadId,
      )(result.payloads.map((payload_json) => ({ payload_json })));
    },
    (effect, threadId, _runIds) =>
      effect.pipe(Effect.mapError((cause) => new ProjectionStoreReadError({ threadId, cause }))),
  );
  return {
    requireThread,
    getThread,
    getPlan,
    getRuntimeRequest,
    getTurnItem,
    getMessagesById,
    hasUnpairedRunInterruptRequest,
    isNodeStored,
    getThreadLinkMetadata,
    getThreadTitles,
    getRunMessage,
    canStartQueuedRun,
    getHeldQueuedRuns,
    getMessageCount,
    getSubagentCount,
    getNextTurnItemOrdinal,
    listAppOwnedChildThreadIds,
    getTurnStartHistory,
  };
}

export type TimelineIndexItem = Pick<
  OrchestrationV2TurnItem,
  "id" | "threadId" | "runId" | "nodeId" | "parentItemId" | "type" | "status" | "ordinal"
> & {
  readonly inputIntent?: Extract<OrchestrationV2TurnItem, { type: "user_message" }>["inputIntent"];
  readonly createdBy?: "user" | "agent" | "system";
  readonly messageId?: MessageId;
  readonly runStatus?: string | undefined;
  readonly attemptId?: string | null;
  readonly attemptStatus?: string | null;
  readonly persistentOutput?: boolean;
  readonly deferredWork?: OrchestrationV2TurnItem["deferredWork"];
};
export type TimelineIndexRow = {
  readonly sourceThreadId: ThreadId;
  readonly sourceItemId: TurnItemId;
  readonly visibility: OrchestrationV2ProjectedTurnItem["visibility"];
  readonly item: TimelineIndexItem;
  readonly synthetic?: OrchestrationV2TurnItem;
};
export function makeTimelineIndex(
  getThreadRecords: ProjectionStoreV2Shape["getThreadRecords"],
  readRows: ProjectionHistoryRows["timeline"],
) {
  const readTimelineIndex: (
    threadId: ThreadId,
    seen: ReadonlySet<ThreadId>,
    includePersistentOutput?: boolean,
  ) => Effect.Effect<
    {
      readonly records: ProjectionRecords<"runs" | "attempts">;
      readonly local: ReadonlyArray<TimelineIndexRow>;
      readonly visible: ReadonlyArray<TimelineIndexRow>;
    },
    ProjectionStoreV2Error
  > = Effect.fn("ProjectionStore.readTimelineIndex")(
    function* (threadId: ThreadId, seen: ReadonlySet<ThreadId>, includePersistentOutput = false) {
      const records = yield* getThreadRecords(threadId, ["runs", "attempts"]);
      const rows = yield* readRows(threadId, includePersistentOutput);
      const runStatuses = new Map(records.runs.map((run) => [String(run.id), run.status]));
      const items: Array<TimelineIndexItem> = rows.map((row) => ({
        id: TurnItemId.make(row.turn_item_id),
        threadId,
        runId: row.run_id === null ? null : RunId.make(row.run_id),
        nodeId: row.node_id === null ? null : NodeId.make(row.node_id),
        parentItemId: row.parent_item_id === null ? null : TurnItemId.make(row.parent_item_id),
        type: row.type,
        status: row.status,
        ordinal: row.ordinal,
        runStatus: row.run_id === null ? undefined : runStatuses.get(row.run_id),
        attemptId: row.attempt_id,
        attemptStatus: row.attempt_status,
        persistentOutput: row.persistent_output === 1,
        ...(row.input_intent === null ? {} : { inputIntent: row.input_intent }),
        ...(row.created_by === null ? {} : { createdBy: row.created_by }),
        ...(row.message_id === null ? {} : { messageId: MessageId.make(row.message_id) }),
      }));
      const local: Array<TimelineIndexRow> = items.map((item) => ({
        sourceThreadId: threadId,
        sourceItemId: item.id,
        visibility: "local",
        item,
      }));
      const isVisible = createOrchestrationV2TurnItemVisibility({
        runs: records.runs,
        attempts: records.attempts,
        items,
      });
      const visible = local.filter((row) => isVisible(row.item));
      const fork = inheritedRunFork(records.thread);
      if (fork === null || seen.has(fork.threadId)) return { records, local, visible };
      const source = yield* readTimelineIndex(
        fork.threadId,
        new Set([...seen, threadId]),
        includePersistentOutput,
      );
      const sourceRun = source.records.runs.find((run) => run.id === fork.runId);
      let inherited: Array<TimelineIndexRow> = [];
      if (sourceRun !== undefined) {
        const ordinals = new Map(source.records.runs.map((run) => [run.id, run.ordinal]));
        const sourceItems = source.local.map((row) => row.item);
        inherited = [
          ...source.visible.filter(
            (row) => row.item.threadId !== fork.threadId || row.item.type === "fork",
          ),
          ...source.local.filter(
            (row) =>
              !isOrchestrationV2SupersededInterrupt({
                item: row.item,
                attempts: source.records.attempts,
                items: sourceItems,
              }) &&
              isTurnItemAtOrBeforeRun({
                historyOrigin: source.records.thread.historyOrigin,
                itemRunId: row.item.runId,
                runOrdinalById: ordinals,
                sourceRunOrdinal: sourceRun.ordinal,
              }),
          ),
        ].map((row) => ({ ...row, visibility: "inherited" }));
      }
      const marker = makeForkMarkerTurnItem({
        targetProjection: records,
        sourceThreadId: fork.threadId,
        sourceRunId: fork.runId,
      });
      return {
        records,
        local,
        visible: [
          ...inherited,
          {
            sourceThreadId: fork.threadId,
            sourceItemId: marker.id,
            visibility: "synthetic" as const,
            item: marker,
            synthetic: marker,
          },
          ...visible,
        ],
      };
    },
    (effect, threadId, _seen) =>
      effect.pipe(
        Effect.mapError((cause) =>
          isProjectionStoreThreadNotFoundError(cause) || isProjectionStoreReadError(cause)
            ? cause
            : new ProjectionStoreReadError({ threadId, cause }),
        ),
      ),
  );

  return readTimelineIndex;
}

export function makeProjectionReader(
  readCanonicalProjection: ReturnType<typeof makeCanonicalProjection>,
  historyRows: Pick<ProjectionHistoryRows, "forkBoundary" | "inheritedCount">,
) {
  const readProjection: (
    threadId: ThreadId,
    seenThreadIds: ReadonlySet<ThreadId>,
    window?: ProjectionWindow,
  ) => Effect.Effect<OrchestrationV2ThreadProjection, ProjectionStoreV2Error> = Effect.fn(
    "ProjectionStore.readProjection",
  )(
    function* (
      threadId: ThreadId,
      seenThreadIds: ReadonlySet<ThreadId>,
      window?: ProjectionWindow,
    ) {
      const localWindow =
        window?.suppressLocal === true ||
        (window?.historyAnchor !== undefined && window.historyAnchor.threadId !== threadId)
          ? { ...window, rowLimit: 0, anchorItemId: undefined }
          : window;
      const projection = yield* readCanonicalProjection(
        threadId,
        localWindow,
        window !== undefined && seenThreadIds.size > 0
          ? ["runs", "attempts", "turnItems"]
          : undefined,
      );
      const forkedFrom = inheritedRunFork(projection.thread);
      if (forkedFrom === null || seenThreadIds.has(forkedFrom.threadId)) {
        return withLocalVisibleTurnItems(projection);
      }

      // Page unanchored local segments fully before inherited turns affect the boundary.
      if (
        window?.userTurnLimit !== undefined &&
        localWindow !== undefined &&
        localWindow.rowLimit > 0 &&
        projection.turnItems.length >= localWindow.rowLimit &&
        !projection.turnItems.some(isThreadHistoryUserTurn)
      ) {
        return withLocalVisibleTurnItems(projection);
      }

      const sourceWindow =
        window === undefined
          ? undefined
          : yield* Effect.gen(function* () {
              const historyAnchor = window.historyAnchor;
              const anchorBelongsToSource = historyAnchor?.threadId === forkedFrom.threadId;
              const anchor =
                (yield* historyRows.forkBoundary(
                  forkedFrom.threadId,
                  forkedFrom.runId,
                  anchorBelongsToSource ? historyAnchor?.itemId : undefined,
                )) ?? undefined;
              const anchorIsInDescendant =
                historyAnchor !== undefined &&
                historyAnchor.threadId !== threadId &&
                historyAnchor.threadId !== forkedFrom.threadId;
              return {
                rowLimit: window.rowLimit,
                userTurnLimit: window.userTurnLimit,
                deferWork: window.deferWork,
                requiredRunId: forkedFrom.runId,
                suppressLocal: anchor === undefined || anchorIsInDescendant,
                ...(anchor === undefined || anchorIsInDescendant
                  ? {}
                  : { anchorItemId: TurnItemId.make(anchor) }),
                ...(historyAnchor === undefined || historyAnchor.threadId === threadId
                  ? {}
                  : { historyAnchor }),
              };
            });

      // Filled local pages need only the inherited count for cursor positions.
      // Run-forked parents still need the recursive read to include their ancestry.
      if (
        window?.userTurnLimit !== undefined &&
        window.historyAnchor === undefined &&
        seenThreadIds.size === 0 &&
        sourceWindow !== undefined &&
        activeLocalTurnItems(projection).filter((row) => isThreadHistoryUserTurn(row.item))
          .length >= Math.max(window.userTurnLimit, THREAD_HISTORY_PAGE_POLICY.maxUserTurns)
      ) {
        const inheritedRowCount = yield* historyRows.inheritedCount(
          forkedFrom.threadId,
          forkedFrom.runId,
          {
            ...sourceWindow,
            rowLimit: sourceWindow.suppressLocal ? 0 : sourceWindow.rowLimit,
          },
        );
        if (inheritedRowCount !== null) {
          return {
            ...projection,
            visibleTurnItems: buildVisibleTurnItems({
              projection,
              sourceProjection: null,
              inheritedRowCount,
            }),
          };
        }
      }

      const sourceProjection = yield* readProjection(
        forkedFrom.threadId,
        new Set([...seenThreadIds, threadId]),
        sourceWindow,
      );
      return {
        ...projection,
        visibleTurnItems: buildVisibleTurnItems({
          projection,
          sourceProjection,
        }),
      };
    },
    (effect, threadId, _seenThreadIds, _window?) =>
      effect.pipe(
        Effect.mapError((cause) =>
          isProjectionStoreThreadNotFoundError(cause) || isProjectionStoreReadError(cause)
            ? cause
            : new ProjectionStoreReadError({ threadId, cause }),
        ),
      ),
  );

  return readProjection;
}

/** Shared shell decoding, fork inheritance and presentation; storage supplies typed row selections. */
export function makeProjectionShells(reader: ProjectionShellRows) {
  const {
    selectShellThreadRows,
    selectShellRunRows,
    selectShellRunItemCounts,
    selectShellItemCounts,
    selectShellProviderThreadRows,
    selectShellPendingTurnItemRows,
  } = reader;

  const runMapsByThreadId = (input: {
    readonly runRows: ReadonlyArray<ShellRunRow>;
    readonly itemCountRows: ReadonlyArray<ShellRunItemCountRow>;
  }) => {
    const runOrdinalsByThreadId = new Map<ThreadId, Map<RunId, number>>();
    for (const row of input.runRows) {
      const threadId = ThreadId.make(row.thread_id);
      const runId = RunId.make(row.run_id);
      const existing = runOrdinalsByThreadId.get(threadId) ?? new Map<RunId, number>();
      existing.set(runId, row.ordinal);
      runOrdinalsByThreadId.set(threadId, existing);
    }
    const itemCountsByThreadId = new Map<ThreadId, Map<RunId, number>>();
    for (const row of input.itemCountRows) {
      const threadId = ThreadId.make(row.thread_id);
      const runId = RunId.make(row.run_id);
      const existing = itemCountsByThreadId.get(threadId) ?? new Map<RunId, number>();
      existing.set(runId, row.item_count);
      itemCountsByThreadId.set(threadId, existing);
    }
    return { runOrdinalsByThreadId, itemCountsByThreadId };
  };

  const shellItemCountMap = (rows: ReadonlyArray<ShellItemCountRow>) =>
    new Map(
      rows.map(
        (row) =>
          [
            ThreadId.make(row.thread_id),
            { itemCount: row.item_count, runlessItemCount: row.runless_item_count },
          ] as const,
      ),
    );

  const pendingBackgroundDataByThreadId = Effect.fn(
    "ProjectionStore.pendingBackgroundDataByThreadId",
  )(function* (input: {
    readonly providerThreadRows: ReadonlyArray<PayloadRow & { readonly thread_id: string }>;
    readonly pendingTurnItemRows: ReadonlyArray<ShellPendingTurnItemRow>;
  }) {
    const providerThreadsByThreadId = new Map<
      ThreadId,
      Array<OrchestrationV2ThreadProjection["providerThreads"][number]>
    >();
    for (const row of input.providerThreadRows) {
      const providerThread = yield* decodeProviderThreadPayload(row.payload_json);
      const threadId =
        row.thread_id.length > 0 ? ThreadId.make(row.thread_id) : providerThread.appThreadId;
      if (threadId === null) {
        continue;
      }
      const existing = providerThreadsByThreadId.get(threadId) ?? [];
      existing.push(providerThread);
      providerThreadsByThreadId.set(threadId, existing);
    }

    const pendingTurnItemsByThreadId = new Map<ThreadId, Array<OrchestrationV2TurnItem>>();
    for (const row of input.pendingTurnItemRows) {
      const turnItem = {
        ...(yield* decodeTurnItemPayload(row.payload_json)),
        ...(row.owning_provider_instance_id === null
          ? {}
          : {
              owningProviderInstanceId: ProviderInstanceId.make(row.owning_provider_instance_id),
            }),
      };
      const threadId = ThreadId.make(row.thread_id);
      const existing = pendingTurnItemsByThreadId.get(threadId) ?? [];
      existing.push(turnItem);
      pendingTurnItemsByThreadId.set(threadId, existing);
    }

    return { providerThreadsByThreadId, pendingTurnItemsByThreadId };
  });

  const getSettlementCandidates: ProjectionStoreV2Shape["getSettlementCandidates"] = (threadId) =>
    reader
      .withReadTransaction(
        Effect.gen(function* () {
          const rows = yield* reader.selectSettlementThreads(threadId);
          if (rows.length === 0) return [];
          const threadIds = rows.map((row) => ThreadId.make(row.thread_id));
          const { providerThreadsByThreadId, pendingTurnItemsByThreadId } =
            yield* pendingBackgroundDataByThreadId({
              providerThreadRows: yield* selectShellProviderThreadRows(threadIds),
              pendingTurnItemRows: yield* selectShellPendingTurnItemRows(threadIds),
            });
          return yield* Effect.forEach(rows, (row) =>
            Effect.gen(function* () {
              const thread = yield* decodeThreadPayload(row.payload_json);
              const status = shellStatusFromStoredRunStatus(row.latest_run_status);
              const latestRunId = row.latest_run_id === null ? null : RunId.make(row.latest_run_id);
              return {
                ...thread,
                pinnedAt: thread.pinnedAt ?? null,

                autoSettleDisabledAt: thread.autoSettleDisabledAt ?? null,
                snoozedUntil: thread.snoozedUntil ?? null,
                snoozedAt: thread.snoozedAt ?? null,
                status,
                latestRunId,
                latestRunRequestedAt:
                  row.latest_run_requested_at === null
                    ? null
                    : DateTime.makeUnsafe(row.latest_run_requested_at),
                latestRunStartedAt:
                  row.latest_run_started_at === null
                    ? null
                    : DateTime.makeUnsafe(row.latest_run_started_at),
                latestRunCompletedAt:
                  row.latest_run_completed_at === null
                    ? null
                    : DateTime.makeUnsafe(row.latest_run_completed_at),
                latestUserMessageAt:
                  row.latest_user_message_at === null
                    ? null
                    : DateTime.makeUnsafe(row.latest_user_message_at),
                latestUserAuthoredMessageAt:
                  row.latest_user_authored_message_at === null
                    ? null
                    : DateTime.makeUnsafe(row.latest_user_authored_message_at),
                activityRunStatus: null,
                activityRunStartedAt: null,
                pendingRuntimeRequest: null,
                pendingGuestMessageCount: pendingGuestMessageCount(thread.guestShare),
                pendingBackgroundTasks: derivePendingBackgroundWork({
                  latestRun:
                    latestRunId === null || status === "idle"
                      ? null
                      : { id: latestRunId, ordinal: 0, status },
                  providerThreads: providerThreadsByThreadId.get(thread.id) ?? [],
                  turnItems: pendingTurnItemsByThreadId.get(thread.id) ?? [],
                  activeProviderThreadId: thread.activeProviderThreadId,
                  hasActiveRun: false,
                  pullRequests: thread.pullRequests,
                }),
              } satisfies ProjectionSettlementCandidate;
            }),
          );
        }),
      )
      .pipe(Effect.mapError((cause) => new ProjectionStoreSetupError({ cause })));

  const getOpenRemoteDelegatedTasks: ProjectionStoreV2Shape["getOpenRemoteDelegatedTasks"] =
    reader.selectRemoteDelegatedTasks.pipe(
      Effect.map((rows) =>
        rows.map((row) => ({
          parentThreadId: ThreadId.make(row.thread_id),
          taskId: NodeId.make(row.subagent_id),
        })),
      ),
      Effect.mapError((cause) => new ProjectionStoreSetupError({ cause })),
    );

  const getThreadsWithPullRequests: ProjectionStoreV2Shape["getThreadsWithPullRequests"] =
    Effect.fn("ProjectionStore.getThreadsWithPullRequests")(
      function* (threadId) {
        const rows = yield* reader.selectThreadsWithPullRequests(threadId);
        return yield* Effect.forEach(rows, (row) =>
          decodeThreadPayload(row.payload_json).pipe(
            Effect.map((thread): ProjectionThreadPullRequests => ({
              id: thread.id,
              projectId: thread.projectId,
              lineage: thread.lineage,
              settledOverride: thread.settledOverride,
              settledAt: thread.settledAt,
              pullRequests: thread.pullRequests ?? [],
            })),
          ),
        );
      },
      Effect.mapError((cause) => new ProjectionStoreSetupError({ cause })),
    );

  const getThreadWorktreePaths: ProjectionStoreV2Shape["getThreadWorktreePaths"] = (options) =>
    reader.selectThreadWorktreePaths(options).pipe(
      Effect.map((rows) =>
        rows.map((row) => ({
          threadId: ThreadId.make(row.thread_id),
          worktreePath: row.worktree_path,
        })),
      ),
      Effect.mapError((cause) => new ProjectionStoreSetupError({ cause })),
    );

  const shellThreadStateFromRow =
    /* Runs once per shell row; the enclosing snapshot or thread read owns the span. */
    Effect.fnUntraced(function* (input: {
      readonly row: ShellThreadRow;
      readonly runOrdinalsByThreadId: ReadonlyMap<ThreadId, Map<RunId, number>>;
      readonly itemCountsByThreadId: ReadonlyMap<ThreadId, Map<RunId, number>>;
      readonly shellItemCountsByThreadId: ReadonlyMap<
        ThreadId,
        { readonly itemCount: number; readonly runlessItemCount: number }
      >;
      readonly providerThreadsByThreadId: ReadonlyMap<
        ThreadId,
        ReadonlyArray<OrchestrationV2ThreadProjection["providerThreads"][number]>
      >;
      readonly pendingTurnItemsByThreadId: ReadonlyMap<
        ThreadId,
        ReadonlyArray<OrchestrationV2TurnItem>
      >;
    }) {
      const {
        row,
        runOrdinalsByThreadId,
        itemCountsByThreadId,
        shellItemCountsByThreadId,
        providerThreadsByThreadId,
        pendingTurnItemsByThreadId,
      } = input;
      const thread = yield* decodeThreadPayload(row.payload_json);
      const latestUserAuthoredMessageAt =
        row.latest_user_authored_message_at === null
          ? null
          : DateTime.makeUnsafe(row.latest_user_authored_message_at);
      const waitingCards = [
        row.pending_secret_request_payload_json === null
          ? null
          : yield* decodeTurnItemPayload(row.pending_secret_request_payload_json),
        row.pending_link_request_payload_json === null
          ? null
          : yield* decodeTurnItemPayload(row.pending_link_request_payload_json),
      ]
        .filter((item) => item !== null)
        .filter(
          (item) =>
            item.type !== "link_request" || linkCardStillAsks(item, latestUserAuthoredMessageAt),
        );
      let pendingRuntimeRequest =
        row.pending_request_payload_json !== null
          ? pendingRuntimeRequestSummary(
              yield* decodeRuntimeRequestPayload(row.pending_request_payload_json),
            )
          : null;
      if (
        (pendingRuntimeRequest === null || pendingRuntimeRequest.kind === "user_input") &&
        waitingCards.length > 0
      ) {
        const waitingCard = secretRequestAsPendingInput(
          waitingCards.toSorted(
            (left, right) =>
              DateTime.toEpochMillis(right.updatedAt) - DateTime.toEpochMillis(left.updatedAt),
          )[0] ?? null,
        );
        if (
          waitingCard !== null &&
          (pendingRuntimeRequest === null ||
            DateTime.toEpochMillis(waitingCard.createdAt) >=
              DateTime.toEpochMillis(pendingRuntimeRequest.createdAt))
        ) {
          pendingRuntimeRequest = waitingCard;
        }
      }
      let terminalFailureItem =
        row.terminal_failure_payload_json === null
          ? null
          : yield* decodeTurnItemPayload(row.terminal_failure_payload_json);
      let latestRunId = row.latest_run_id === null ? null : RunId.make(row.latest_run_id);
      let latestRunStatus = shellStatusFromStoredRunStatus(row.latest_run_status);
      let latestRunRequestedAt =
        row.latest_run_requested_at === null
          ? null
          : DateTime.makeUnsafe(row.latest_run_requested_at);
      let latestRunStartedAt =
        row.latest_run_started_at === null ? null : DateTime.makeUnsafe(row.latest_run_started_at);
      let latestRunCompletedAt =
        row.latest_run_completed_at === null
          ? null
          : DateTime.makeUnsafe(row.latest_run_completed_at);
      let latestRunStopRequested = row.latest_run_stop_requested === 1;
      if (row.blocking_run_id !== null && row.blocking_failure_payload_json !== null) {
        const blockingFailure = yield* decodeTurnItemPayload(
          row.blocking_failure_payload_json,
        ).pipe(Effect.orElseSucceed(() => null));
        const blocksQueue =
          threadErrorSummary(
            blockingFailure?.type === "error" ? blockingFailure.failure : null,
            row.last_error,
          ).lastErrorClass === "usage_limit";
        if (blocksQueue && blockingFailure !== null) {
          terminalFailureItem = blockingFailure;
          latestRunId = RunId.make(row.blocking_run_id);
          latestRunStopRequested = row.blocking_run_stop_requested === 1;
          latestRunStatus = "failed";
          latestRunRequestedAt =
            row.blocking_run_requested_at === null
              ? null
              : DateTime.makeUnsafe(row.blocking_run_requested_at);
          latestRunStartedAt =
            row.blocking_run_started_at === null
              ? null
              : DateTime.makeUnsafe(row.blocking_run_started_at);
          latestRunCompletedAt =
            row.blocking_run_completed_at === null
              ? null
              : DateTime.makeUnsafe(row.blocking_run_completed_at);
        }
      }
      const pendingBackgroundTasks = [
        ...derivePendingBackgroundWork({
          latestRun:
            latestRunId === null || latestRunStatus === "idle"
              ? null
              : {
                  id: latestRunId,
                  ordinal: 0,
                  status: latestRunStatus,
                },
          providerThreads: providerThreadsByThreadId.get(thread.id) ?? [],
          turnItems: pendingTurnItemsByThreadId.get(thread.id) ?? [],
          activeProviderThreadId: thread.activeProviderThreadId,
          hasActiveRun: row.active_run_id !== null,
          pullRequests: thread.pullRequests,
        }),
      ];
      return {
        thread,
        latestRunId,
        latestRunStatus,
        latestRunRequestedAt,
        latestRunStartedAt,
        latestRunCompletedAt,
        latestRunStopRequested,
        activeRunId: row.active_run_id === null ? null : RunId.make(row.active_run_id),
        blockedOnThreadWait: deriveBlockedOnThreadWait({
          activeRun:
            row.active_run_id === null || row.active_run_status !== "running"
              ? null
              : { id: RunId.make(row.active_run_id), status: "running" },
          turnItems: pendingTurnItemsByThreadId.get(thread.id) ?? [],
        }),
        activityRunStartedAt:
          row.activity_run_started_at === null
            ? null
            : DateTime.makeUnsafe(row.activity_run_started_at),
        activityRunStatus:
          row.activity_run_status === "preparing" ||
          row.activity_run_status === "starting" ||
          row.activity_run_status === "running" ||
          row.activity_run_status === "waiting"
            ? row.activity_run_status
            : null,
        ...threadErrorSummary(
          terminalFailureItem?.type === "error" ? terminalFailureItem.failure : null,
          row.last_error,
        ),
        pendingRuntimeRequest,
        latestUserMessageAt:
          row.latest_user_message_at === null
            ? null
            : DateTime.makeUnsafe(row.latest_user_message_at),
        latestUserAuthoredMessageAt,
        hasActionableProposedPlan: row.has_actionable_proposed_plan === 1,
        pendingBackgroundTasks,
        providerInstanceHistory: providerInstanceHistoryForShell({
          threadId: thread.id,
          providerThreads: providerThreadsByThreadId.get(thread.id) ?? [],
        }),
        goal: activeProviderGoalForShell(
          providerThreadsByThreadId.get(thread.id) ?? [],
          thread.activeProviderThreadId,
        ),
        itemCount: shellItemCountsByThreadId.get(thread.id)?.itemCount ?? 0,
        runlessItemCount: shellItemCountsByThreadId.get(thread.id)?.runlessItemCount ?? 0,
        updatedAt: thread.updatedAt,
        runOrdinalById: runOrdinalsByThreadId.get(ThreadId.make(row.thread_id)) ?? new Map(),
        itemCountByRunId: itemCountsByThreadId.get(ThreadId.make(row.thread_id)) ?? new Map(),
      } satisfies ShellThreadState;
    });

  const shellSnapshotReadError = (cause: unknown) =>
    new ProjectionStoreReadError({ threadId: ThreadId.make("thread:shell"), cause });

  const readShellSnapshotRows = Effect.fn("ProjectionStore.readShellSnapshotRows")(function* (
    options: ShellSnapshotOptions | undefined,
  ) {
    const targetThreadRows = yield* selectShellThreadRows(undefined, options);
    const targetThreadIds = new Set(targetThreadRows.map((row) => ThreadId.make(row.thread_id)));
    const rowsByThreadId = new Map(
      targetThreadRows.map((row) => [ThreadId.make(row.thread_id), row] as const),
    );
    const pendingSourceIds = targetThreadRows.flatMap((row) =>
      row.forked_from_run_source_thread_id === null
        ? []
        : [ThreadId.make(row.forked_from_run_source_thread_id)],
    );
    while (pendingSourceIds.length > 0) {
      const sourceId = pendingSourceIds.pop();
      if (sourceId === undefined || rowsByThreadId.has(sourceId)) continue;
      const source = (yield* selectShellThreadRows(sourceId))[0];
      if (source === undefined) continue;
      rowsByThreadId.set(sourceId, source);
      if (source.forked_from_run_source_thread_id !== null) {
        pendingSourceIds.push(ThreadId.make(source.forked_from_run_source_thread_id));
      }
    }
    const threadRows = [...rowsByThreadId.values()];
    const threadIds = [...rowsByThreadId.keys()];
    const forkSourceIds = shellForkSourceIds(threadRows);
    const readForThreadIds = <A>(
      read: (
        ids: ReadonlyArray<ThreadId>,
      ) => Effect.Effect<ReadonlyArray<A>, PersistenceTransactionError>,
      ids: ReadonlyArray<ThreadId> = threadIds,
    ) => (ids.length === 0 ? Effect.succeed([] as ReadonlyArray<A>) : read(ids));
    const [
      runRows,
      itemCountRows,
      shellItemCountRows,
      sequenceRows,
      providerThreadRows,
      pendingTurnItemRows,
    ] = yield* Effect.all([
      readForThreadIds(selectShellRunRows, forkSourceIds),
      readForThreadIds(selectShellRunItemCounts, forkSourceIds),
      readForThreadIds(selectShellItemCounts),
      reader.sequenceRows,
      readForThreadIds(selectShellProviderThreadRows),
      readForThreadIds((ids) =>
        selectShellPendingTurnItemRows(
          ids,
          threadRows.flatMap((row) =>
            row.active_run_id !== null && row.active_run_status === "running"
              ? [row.active_run_id]
              : [],
          ),
        ),
      ),
    ]);
    return {
      targetThreadIds,
      threadRows,
      runRows,
      itemCountRows,
      shellItemCountRows,
      sequenceRows,
      providerThreadRows,
      pendingTurnItemRows,
    };
  });

  const decodeShellSnapshot = Effect.fn("ProjectionStore.decodeShellSnapshot")(function* ({
    targetThreadIds,
    threadRows,
    runRows,
    itemCountRows,
    shellItemCountRows,
    sequenceRows,
    providerThreadRows,
    pendingTurnItemRows,
  }: Effect.Success<ReturnType<typeof readShellSnapshotRows>>) {
    const { runOrdinalsByThreadId, itemCountsByThreadId } = runMapsByThreadId({
      runRows,
      itemCountRows,
    });
    const shellItemCountsByThreadId = shellItemCountMap(shellItemCountRows);
    const { providerThreadsByThreadId, pendingTurnItemsByThreadId } =
      yield* pendingBackgroundDataByThreadId({ providerThreadRows, pendingTurnItemRows });
    const states = yield* Effect.forEach(threadRows, (row) =>
      shellThreadStateFromRow({
        row,
        runOrdinalsByThreadId,
        itemCountsByThreadId,
        shellItemCountsByThreadId,
        providerThreadsByThreadId,
        pendingTurnItemsByThreadId,
      }),
    );
    const statesByThreadId = new Map(states.map((state) => [state.thread.id, state]));

    const shells = states
      .filter((state) => targetThreadIds.has(state.thread.id))
      .map((state) =>
        shellFromState({
          state,
          visibleItemCount: visibleItemCountForShell({
            threadId: state.thread.id,
            statesByThreadId,
          }),
        }),
      );

    return {
      schemaVersion: ORCHESTRATION_V2_PROJECTION_SCHEMA_VERSION,
      snapshotSequence: sequenceRows[0]?.snapshot_sequence ?? 0,
      threads: shells.filter((thread) => thread.archivedAt === null),
      archivedThreads: shells.filter((thread) => thread.archivedAt !== null),
    };
  }, Effect.mapError(shellSnapshotReadError));

  const readShellSnapshot: ProjectionStoreV2Shape["readShellSnapshot"] = (options) =>
    readShellSnapshotRows(options).pipe(
      Effect.mapError(shellSnapshotReadError),
      Effect.map(decodeShellSnapshot),
    );

  const getPullRequestSnapshot: ProjectionStoreV2Shape["getPullRequestSnapshot"] = Effect.fn(
    "ProjectionStore.getPullRequestSnapshot",
  )(function* ({ threadId, unsettledOnly = false }) {
    const { rows, sequenceRows } = yield* reader.withReadTransaction(
      Effect.gen(function* () {
        const rows = yield* reader.selectPullRequestThreads(threadId, unsettledOnly);
        const sequenceRows = yield* reader.sequenceRows;
        return { rows, sequenceRows };
      }),
    );
    // Decode after releasing the reader, keeping rows and sequence from the same transaction.
    const threads = yield* Effect.forEach(rows, (row) =>
      decodePullRequestThread(row.payload_json).pipe(
        Effect.map((thread) => ({ ...thread, pullRequests: threadPullRequestsOf(thread) })),
      ),
    );
    return { snapshotSequence: sequenceRows[0]?.snapshot_sequence ?? 0, threads };
  }, Effect.mapError(shellSnapshotReadError));

  const getAgentAwarenessSnapshot = Effect.fn("ProjectionStore.getAgentAwarenessSnapshot")(
    function* (threadId?: ThreadId) {
      const { threadRows, providerThreadRows, pendingTurnItemRows, sequenceRows } =
        yield* reader.withReadTransaction(
          Effect.gen(function* () {
            const threadRows = yield* selectShellThreadRows(
              threadId,
              threadId === undefined ? { location: "active", listedOnly: true } : {},
            );
            const ids = threadRows.map((row) => ThreadId.make(row.thread_id));
            const providerThreadRows =
              ids.length === 0 ? [] : yield* selectShellProviderThreadRows(ids);
            const pendingTurnItemRows =
              ids.length === 0
                ? []
                : yield* selectShellPendingTurnItemRows(
                    ids,
                    threadRows.flatMap((row) =>
                      row.active_run_id !== null && row.active_run_status === "running"
                        ? [row.active_run_id]
                        : [],
                    ),
                  );
            const sequenceRows = yield* reader.sequenceRows;
            return { threadRows, providerThreadRows, pendingTurnItemRows, sequenceRows };
          }),
        );
      const { providerThreadsByThreadId, pendingTurnItemsByThreadId } =
        yield* pendingBackgroundDataByThreadId({ providerThreadRows, pendingTurnItemRows });
      const threads = yield* Effect.forEach(threadRows, (row) =>
        shellThreadStateFromRow({
          row,
          // Awareness never consumes timeline counts or inherited fork history.
          runOrdinalsByThreadId: new Map(),
          itemCountsByThreadId: new Map(),
          shellItemCountsByThreadId: new Map(),
          providerThreadsByThreadId,
          pendingTurnItemsByThreadId,
        }).pipe(
          Effect.map((state): AgentAwarenessThread => ({
            id: state.thread.id,
            projectId: state.thread.projectId,
            creationSource: state.thread.creationSource,
            archivedAt: state.thread.archivedAt,
            activeRunId: state.activeRunId,
            latestRunId: state.latestRunId,
            hasActionableProposedPlan: state.hasActionableProposedPlan,
            title: state.thread.title,
            lineage: state.thread.lineage,
            modelSelection: state.thread.modelSelection,
            settledOverride: state.thread.settledOverride,
            updatedAt: state.updatedAt,
            status: state.latestRunStatus,
            activityRunStatus: state.activityRunStatus,
            latestRunCompletedAt: state.latestRunCompletedAt,
            latestRunStopRequested: state.latestRunStopRequested,
            pendingRuntimeRequest: state.pendingRuntimeRequest,
            pendingBackgroundTasks: state.pendingBackgroundTasks,
          })),
        ),
      );
      return { snapshotSequence: sequenceRows[0]?.snapshot_sequence ?? 0, threads };
    },
    Effect.mapError(shellSnapshotReadError),
  );

  const getShellSnapshot: ProjectionStoreV2Shape["getShellSnapshot"] = (options) =>
    reader
      .withReadTransaction(readShellSnapshotRows(options))
      .pipe(Effect.mapError(shellSnapshotReadError), Effect.flatMap(decodeShellSnapshot));

  const listActiveSubagentThreadIds = Effect.fn("ProjectionStore.listActiveSubagentThreadIds")(
    function* () {
      const rows = yield* reader.selectActiveSubagentThreads;
      return rows.map((row) => ThreadId.make(row.thread_id));
    },
    Effect.mapError(shellSnapshotReadError),
  );

  const countThreads = Effect.fn("ProjectionStore.countThreads")(function* () {
    const rows = yield* reader.countThreads;
    return rows[0]?.count ?? 0;
  }, Effect.mapError(shellSnapshotReadError));

  const listActiveShellIndex = Effect.fn("ProjectionStore.listActiveShellIndex")(function* () {
    const rows = yield* reader.selectActiveShellIndex;
    return rows.map((row): ActiveShellIndexEntry => ({
      id: ThreadId.make(row.thread_id),
      projectId: ProjectId.make(row.project_id),
      lineage: {
        parentThreadId: row.parent_thread_id === null ? null : ThreadId.make(row.parent_thread_id),
        relationshipToParent:
          row.relationship_to_parent === "fork" || row.relationship_to_parent === "subagent"
            ? row.relationship_to_parent
            : null,
        rootThreadId: ThreadId.make(row.root_thread_id),
        ...(row.side_chat === null ? {} : { sideChat: row.side_chat === 1 }),
      },
      settledOverride:
        row.settled_override === "settled" || row.settled_override === "active"
          ? row.settled_override
          : null,
      settledAt: row.settled_at === null ? null : DateTime.makeUnsafe(row.settled_at),
      pinnedAt: row.pinned_at === null ? null : DateTime.makeUnsafe(row.pinned_at),
      snoozedUntil: row.snoozed_until === null ? null : DateTime.makeUnsafe(row.snoozed_until),
      mayBeLive: row.may_be_live === 1,
    }));
  }, Effect.mapError(shellSnapshotReadError));

  const findGuestShareThreadId = Effect.fn("ProjectionStore.findGuestShareThreadId")(function* (
    shareId: GuestShareId,
  ) {
    const rows = yield* reader.findGuestShareThread(shareId);
    return rows[0] === undefined ? null : ThreadId.make(rows[0].thread_id);
  }, Effect.mapError(shellSnapshotReadError));

  const listOlderSettledThreads: ProjectionStoreV2Shape["listOlderSettledThreads"] = Effect.fn(
    "ProjectionStore.listOlderSettledThreads",
  )(function* ({ before, projectIds, limit }) {
    const { pageRows, countRows } = yield* reader.selectOlderSettledThreads({
      before,
      projectIds,
      limit,
    });
    return {
      threadIds: pageRows.map((row) => ThreadId.make(row.thread_id)),
      counts: countRows.map((row) => ({
        projectId: ProjectId.make(row.project_id),
        count: row.count,
      })),
    };
  }, Effect.mapError(shellSnapshotReadError));

  const getThreadShell: ProjectionStoreV2Shape["getThreadShell"] = (threadId) =>
    reader
      .withReadTransaction(
        Effect.gen(function* () {
          const rowsByThreadId = new Map<ThreadId, ShellThreadRow>();
          const pending: Array<ThreadId> = [threadId];
          while (pending.length > 0) {
            const nextId = pending.pop();
            if (nextId === undefined || rowsByThreadId.has(nextId)) {
              continue;
            }
            const rows = yield* selectShellThreadRows(nextId);
            const row = rows[0];
            if (row === undefined) {
              continue;
            }
            rowsByThreadId.set(nextId, row);
            if (row.forked_from_run_source_thread_id !== null) {
              pending.push(ThreadId.make(row.forked_from_run_source_thread_id));
            }
          }
          if (!rowsByThreadId.has(threadId)) {
            return null;
          }

          const threadIds = [...rowsByThreadId.keys()];
          const forkSourceIds = shellForkSourceIds([...rowsByThreadId.values()]);
          const [
            runRows,
            itemCountRows,
            shellItemCountRows,
            providerThreadRows,
            pendingTurnItemRows,
          ] = yield* Effect.all([
            forkSourceIds.length === 0
              ? Effect.succeed([] as ReadonlyArray<ShellRunRow>)
              : selectShellRunRows(forkSourceIds),
            forkSourceIds.length === 0
              ? Effect.succeed([] as ReadonlyArray<ShellRunItemCountRow>)
              : selectShellRunItemCounts(forkSourceIds),
            selectShellItemCounts(threadIds),
            selectShellProviderThreadRows(threadIds),
            selectShellPendingTurnItemRows(
              threadIds,
              [...rowsByThreadId.values()].flatMap((row) =>
                row.active_run_id !== null && row.active_run_status === "running"
                  ? [row.active_run_id]
                  : [],
              ),
            ),
          ]);
          const { runOrdinalsByThreadId, itemCountsByThreadId } = runMapsByThreadId({
            runRows,
            itemCountRows,
          });
          const shellItemCountsByThreadId = shellItemCountMap(shellItemCountRows);
          const { providerThreadsByThreadId, pendingTurnItemsByThreadId } =
            yield* pendingBackgroundDataByThreadId({ providerThreadRows, pendingTurnItemRows });

          const states = yield* Effect.forEach([...rowsByThreadId.values()], (row) =>
            shellThreadStateFromRow({
              row,
              runOrdinalsByThreadId,
              itemCountsByThreadId,
              shellItemCountsByThreadId,
              providerThreadsByThreadId,
              pendingTurnItemsByThreadId,
            }),
          );
          const statesByThreadId = new Map(states.map((state) => [state.thread.id, state]));
          const state = statesByThreadId.get(threadId);
          if (state === undefined) {
            return null;
          }
          return shellFromState({
            state,
            visibleItemCount: visibleItemCountForShell({ threadId, statesByThreadId }),
          });
        }),
      )
      .pipe(Effect.mapError((cause) => new ProjectionStoreReadError({ threadId, cause })));

  return {
    getSettlementCandidates,
    getOpenRemoteDelegatedTasks,
    getThreadsWithPullRequests,
    getThreadWorktreePaths,
    readShellSnapshot,
    getPullRequestSnapshot,
    getAgentAwarenessSnapshot,
    getShellSnapshot,
    listActiveSubagentThreadIds,
    countThreads,
    listActiveShellIndex,
    findGuestShareThreadId,
    listOlderSettledThreads,
    getThreadShell,
  };
}
export function makeProjectionSnapshots(
  readProjection: ReturnType<typeof makeProjectionReader>,
  rows: ProjectionSnapshotRows,
) {
  const readError = (threadId: ThreadId) => (cause: unknown) =>
    isProjectionStoreThreadNotFoundError(cause) || isProjectionStoreReadError(cause)
      ? cause
      : new ProjectionStoreReadError({ threadId, cause });
  const getThreadProjection: ProjectionStoreV2Shape["getThreadProjection"] = (threadId) =>
    rows
      .withReadTransaction(readProjection(threadId, new Set()))
      .pipe(Effect.mapError(readError(threadId)));
  const getThreadSnapshot: ProjectionStoreV2Shape["getThreadSnapshot"] = Effect.fn(
    "ProjectionStore.getThreadSnapshot",
  )(
    function* (threadId) {
      return {
        schemaVersion: ORCHESTRATION_V2_PROJECTION_SCHEMA_VERSION,
        projection: yield* getThreadProjection(threadId),
        snapshotSequence: yield* rows.snapshotSequence(threadId),
      };
    },
    (effect, threadId) =>
      rows.withReadTransaction(effect).pipe(Effect.mapError(readError(threadId))),
  );
  const getThreadSnapshotWindow: ProjectionStoreV2Shape["getThreadSnapshotWindow"] = Effect.fn(
    "ProjectionStore.getThreadSnapshotWindow",
  )(
    function* (threadId, options) {
      const historyAnchor =
        options.anchorItemId === undefined
          ? undefined
          : {
              itemId: options.anchorItemId,
              threadId:
                options.anchorThreadId ??
                (yield* rows.anchorThread(options.anchorItemId)) ??
                threadId,
            };
      const projection = yield* readProjection(threadId, new Set(), {
        rowLimit: options.rowLimit,
        userTurnLimit: options.userTurnLimit,
        deferWork: options.deferWork,
        ...(historyAnchor?.threadId === threadId ? { anchorItemId: historyAnchor.itemId } : {}),
        ...(historyAnchor === undefined ? {} : { historyAnchor }),
      });
      const retainedIds = new Set(projection.turnItems.map((item) => item.id));
      const pendingCards = (yield* decodeRows(
        decodeTurnItemPayload,
        threadId,
      )((yield* rows.pendingCards(threadId)).map((payload_json) => ({ payload_json })))).filter(
        (item) => !retainedIds.has(item.id),
      );
      const windowProjection =
        pendingCards.length === 0
          ? projection
          : {
              ...projection,
              turnItems: [...projection.turnItems, ...pendingCards].toSorted(
                (left, right) => left.ordinal - right.ordinal || left.id.localeCompare(right.id),
              ),
            };
      return {
        schemaVersion: ORCHESTRATION_V2_PROJECTION_SCHEMA_VERSION,
        snapshotSequence: yield* rows.snapshotSequence(threadId),
        projection: windowProjection,
      };
    },
    (effect, threadId, _options) =>
      rows.withReadTransaction(effect).pipe(Effect.mapError(readError(threadId))),
  );
  return { getThreadProjection, getThreadSnapshot, getThreadSnapshotWindow };
}

/** Context decoding and branch order are shared by both persistence backends. */
export function makeProjectionContexts(
  source: ProjectionContextRows.ProjectionContextRows,
  lookups: ReturnType<typeof makeProjectionLookups>,
) {
  const { getThread } = lookups;
  const controlReadError = (threadId: ThreadId) => (cause: unknown) =>
    isProjectionStoreThreadNotFoundError(cause)
      ? cause
      : new ProjectionStoreReadError({ threadId, cause });
  const getTurnStartContext: ProjectionStoreV2Shape["getTurnStartContext"] = (threadId, runId) =>
    source
      .withReadTransaction(
        Effect.gen(function* () {
          const thread = yield* getThread(threadId);
          const runs = yield* source
            .read(
              { kind: "turn_runs", thread_id: threadId, run_id: runId },
              ProjectionContextRows.PayloadRows,
            )
            .pipe(Effect.flatMap(decodeRows(decodeRunPayload, threadId)));
          const attempts = yield* source
            .read(
              { kind: "turn_attempts", thread_id: threadId, run_id: runId },
              ProjectionContextRows.PayloadRows,
            )
            .pipe(Effect.flatMap(decodeRows(decodeRunAttemptPayload, threadId)));
          const nodes = yield* source
            .read(
              { kind: "turn_nodes", thread_id: threadId, run_id: runId },
              ProjectionContextRows.PayloadRows,
            )
            .pipe(Effect.flatMap(decodeRows(decodeNodePayload, threadId)));
          const subagents = yield* source
            .read(
              { kind: "turn_subagents", thread_id: threadId, run_id: runId },
              ProjectionContextRows.PayloadRows,
            )
            .pipe(Effect.flatMap(decodeRows(decodeSubagentPayload, threadId)));
          const providerThreads = yield* source
            .read(
              { kind: "turn_provider_threads", thread_id: threadId, run_id: runId },
              ProjectionContextRows.PayloadRows,
            )
            .pipe(Effect.flatMap(decodeRows(decodeProviderThreadPayload, threadId)));
          const providerTurns = yield* source
            .read(
              { kind: "turn_provider_turns", thread_id: threadId, run_id: runId },
              ProjectionContextRows.PayloadRows,
            )
            .pipe(Effect.flatMap(decodeRows(decodeProviderTurnPayload, threadId)));
          const checkpointScopes = yield* source
            .read(
              {
                kind: "turn_scopes",
                thread_id: threadId,
                run_id: runId,
                ids: nodes.flatMap((node) =>
                  node.checkpointScopeId === null ? [] : [node.checkpointScopeId],
                ),
              },
              ProjectionContextRows.PayloadRows,
            )
            .pipe(Effect.flatMap(decodeRows(decodeCheckpointScopePayload, threadId)));
          const contextHandoffs = yield* source
            .read(
              { kind: "turn_handoffs", thread_id: threadId, run_id: runId },
              ProjectionContextRows.PayloadRows,
            )
            .pipe(Effect.flatMap(decodeRows(decodeContextHandoffPayload, threadId)));
          const contextTransfers = yield* source
            .read(
              { kind: "turn_transfers", thread_id: threadId, run_id: runId },
              ProjectionContextRows.PayloadRows,
            )
            .pipe(Effect.flatMap(decodeRows(decodeContextTransferPayload, threadId)));
          const turnItems = yield* source
            .read(
              { kind: "turn_items", thread_id: threadId, run_id: runId },
              ProjectionContextRows.PayloadRows,
            )
            .pipe(Effect.flatMap(decodeRows(decodeTurnItemPayload, threadId)));
          const providerSessions = yield* source
            .read(
              { kind: "turn_sessions", thread_id: threadId, run_id: runId },
              ProjectionContextRows.PayloadRows,
            )
            .pipe(Effect.flatMap(decodeRows(decodeProviderSessionPayload, threadId)));
          const messages = yield* source
            .read(
              { kind: "turn_messages", thread_id: threadId, run_id: runId },
              ProjectionContextRows.PayloadRows,
            )
            .pipe(Effect.flatMap(decodeRows(decodeMessagePayload, threadId)));
          const conversation = yield* source.read(
            { kind: "turn_conversation", thread_id: threadId, run_id: runId },
            ProjectionContextRows.ConversationRows,
          );
          return {
            hasConversation:
              conversation[0]?.present === 1 ||
              inheritsForkConversation({ thread, contextTransfers, contextHandoffs }),
            thread,
            runs,
            attempts,
            nodes,
            subagents,
            providerSessions,
            providerThreads,
            providerTurns,
            messages,
            checkpointScopes,
            contextHandoffs,
            contextTransfers,
            turnItems,
          };
        }),
      )
      .pipe(
        Effect.mapError((cause) =>
          isProjectionStoreThreadNotFoundError(cause)
            ? cause
            : new ProjectionStoreReadError({ threadId, cause }),
        ),
      );

  const getCheckpointContext: ProjectionStoreV2Shape["getCheckpointContext"] = (threadId) =>
    source
      .withReadTransaction(
        Effect.gen(function* () {
          const threads = yield* source.read(
            { kind: "thread_exists", thread_id: threadId },
            ProjectionContextRows.ThreadRows,
          );
          if (threads.length === 0) {
            return yield* new ProjectionStoreThreadNotFoundError({ threadId });
          }
          // Select checkpoint metadata only; diffs need neither transcripts nor large run/checkpoint JSON.
          const [runs, checkpointScopes, checkpoints] = yield* Effect.all([
            source.read(
              { kind: "checkpoint_runs", thread_id: threadId },
              ProjectionContextRows.RawRows,
            ),
            source.read(
              { kind: "checkpoint_scopes", thread_id: threadId },
              ProjectionContextRows.RawRows,
            ),
            source.read(
              { kind: "checkpoint_records", thread_id: threadId },
              ProjectionContextRows.RawRows,
            ),
          ]);
          return yield* decodeCheckpointContext({ runs, checkpointScopes, checkpoints });
        }),
      )
      .pipe(
        Effect.mapError((cause) =>
          isProjectionStoreThreadNotFoundError(cause)
            ? cause
            : new ProjectionStoreReadError({ threadId, cause }),
        ),
      );

  const getCheckpointCaptureContext: ProjectionStoreV2Shape["getCheckpointCaptureContext"] = (
    threadId,
    target,
  ) =>
    source
      .withReadTransaction(
        Effect.gen(function* () {
          const threadRows = yield* source.read(
            { kind: "side_chat", thread_id: threadId },
            ProjectionContextRows.SideChatRows,
          );
          if (threadRows[0] === undefined) {
            return yield* new ProjectionStoreThreadNotFoundError({ threadId });
          }
          const runRows = yield* source.read(
            { kind: "run", thread_id: threadId, record_id: target.runId },
            ProjectionContextRows.PayloadRows,
          );
          const run =
            runRows[0] === undefined ? undefined : yield* decodeRunPayload(runRows[0].payload_json);
          const [nodeRows, scopeRows, providerRows, readyRows] = yield* Effect.all([
            source.read(
              { kind: "node", thread_id: threadId, record_id: run?.rootNodeId ?? null },
              ProjectionContextRows.PayloadRows,
            ),
            source.read(
              { kind: "scope", thread_id: threadId, record_id: target.scopeId },
              ProjectionContextRows.PayloadRows,
            ),
            source.read(
              { kind: "provider", thread_id: threadId, record_id: run?.providerThreadId ?? null },
              ProjectionContextRows.PayloadRows,
            ),
            source.read(
              { kind: "ready_ordinals", thread_id: threadId, scope_id: target.scopeId },
              ProjectionContextRows.OrdinalRows,
            ),
          ]);
          return {
            run,
            rootNode:
              nodeRows[0] === undefined
                ? undefined
                : yield* decodeNodePayload(nodeRows[0].payload_json),
            scope:
              scopeRows[0] === undefined
                ? undefined
                : yield* decodeCheckpointScopePayload(scopeRows[0].payload_json),
            providerThread:
              providerRows[0] === undefined
                ? undefined
                : yield* decodeProviderThreadPayload(providerRows[0].payload_json),
            readyCheckpointOrdinals: readyRows.map(({ ordinal }) => ordinal),
            sideChat: threadRows[0].side_chat === 1,
          };
        }),
      )
      .pipe(Effect.mapError(controlReadError(threadId)));

  const getCheckpointRollbackContext: ProjectionStoreV2Shape["getCheckpointRollbackContext"] = (
    threadId,
    target,
  ) =>
    source
      .withReadTransaction(
        Effect.gen(function* () {
          const thread = yield* getThread(threadId);
          const [providerRows, checkpointRows, scopeRows] = yield* Effect.all([
            source.read(
              { kind: "provider", thread_id: threadId, record_id: target.providerThreadId },
              ProjectionContextRows.PayloadRows,
            ),
            source.read(
              { kind: "checkpoint", thread_id: threadId, record_id: target.checkpointId },
              ProjectionContextRows.PayloadRows,
            ),
            source.read(
              { kind: "scope", thread_id: threadId, record_id: target.scopeId },
              ProjectionContextRows.PayloadRows,
            ),
          ]);
          const providerThread =
            providerRows[0] === undefined
              ? undefined
              : yield* decodeProviderThreadPayload(providerRows[0].payload_json);
          const checkpoint =
            checkpointRows[0] === undefined
              ? undefined
              : yield* decodeCheckpointPayload(checkpointRows[0].payload_json);
          const scope =
            scopeRows[0] === undefined
              ? undefined
              : yield* decodeCheckpointScopePayload(scopeRows[0].payload_json);
          const context: ProjectionCheckpointRollbackContext = {
            thread,
            providerThread,
            checkpoint,
            scope,
            providerSession: undefined,
            targetAttempt: undefined,
            providerTurns: [],
            rolledBackAttemptIds: [],
            runsToRollback: [],
            rootNodes: [],
            staleCheckpoints: [],
          };
          if (
            providerThread === undefined ||
            providerThread.providerSessionId === null ||
            checkpoint === undefined ||
            scope === undefined ||
            checkpoint.scopeId !== scope.id ||
            checkpoint.status !== "ready" ||
            providerThread.id !== thread.activeProviderThreadId ||
            providerThread.providerInstanceId !== thread.modelSelection.instanceId
          )
            return context;

          const targetOrdinal = checkpoint.appRunOrdinal ?? 0;
          const [attemptRows, runRows, sessionRows, rolledBackAttempts, staleRows] =
            yield* Effect.all([
              source.read(
                { kind: "rollback_target_attempt", thread_id: threadId, ordinal: targetOrdinal },
                ProjectionContextRows.RawRows,
              ),
              source.read(
                { kind: "rollback_runs", thread_id: threadId, ordinal: targetOrdinal },
                ProjectionContextRows.PayloadRows,
              ),
              source.read(
                {
                  kind: "bound_session",
                  thread_id: threadId,
                  record_id: providerThread.providerSessionId,
                },
                ProjectionContextRows.PayloadRows,
              ),
              source.read(
                { kind: "rolledback_attempts", thread_id: threadId },
                ProjectionContextRows.AttemptIdRows,
              ),
              source.read(
                {
                  kind: "stale_checkpoints",
                  thread_id: threadId,
                  scope_id: scope.id,
                  ordinal: targetOrdinal,
                },
                ProjectionContextRows.PayloadRows,
              ),
            ]);
          const targetAttempt = (yield* decodeRollbackTargetAttempts(attemptRows))[0];
          const runsToRollback = yield* decodeRows(decodeRunPayload, threadId)(runRows);
          const [nodeRows, turnRows] = yield* Effect.all([
            source.read(
              {
                kind: "rollback_nodes",
                thread_id: threadId,
                ids: runsToRollback.map((run) => run.rootNodeId),
              },
              ProjectionContextRows.PayloadRows,
            ),
            source.read(
              {
                kind: "rollback_turns",
                thread_id: threadId,
                provider_thread_id: providerThread.id,
                provider_turn_id: targetAttempt?.providerTurnId ?? null,
                attempt_id: targetAttempt?.id ?? null,
              },
              ProjectionContextRows.PayloadRows,
            ),
          ]);
          return {
            ...context,
            targetAttempt,
            runsToRollback,
            providerSession:
              sessionRows[0] === undefined
                ? undefined
                : yield* decodeProviderSessionPayload(sessionRows[0].payload_json),
            rolledBackAttemptIds: rolledBackAttempts.map(({ id }) => id),
            rootNodes: yield* decodeRows(decodeNodePayload, threadId)(nodeRows),
            providerTurns: yield* decodeRows(decodeProviderTurnPayload, threadId)(turnRows),
            staleCheckpoints: yield* decodeRows(decodeCheckpointPayload, threadId)(staleRows),
          };
        }),
      )
      .pipe(Effect.mapError(controlReadError(threadId)));

  const getThreadAttachmentIds: ProjectionStoreV2Shape["getThreadAttachmentIds"] = (threadId) =>
    Effect.all([
      source.read(
        { kind: "attachment_messages", thread_id: threadId },
        ProjectionContextRows.AttachmentRows,
      ),
      source.read(
        { kind: "attachment_renders", thread_id: threadId },
        ProjectionContextRows.PayloadRows,
      ),
      source.read(
        { kind: "attachment_answers", thread_id: threadId },
        ProjectionContextRows.AttachmentRows,
      ),
      // A guest's waiting files are pending uploads that no message row holds yet.
      source.read(
        { kind: "attachment_guests", thread_id: threadId },
        ProjectionContextRows.AttachmentRows,
      ),
    ]).pipe(
      Effect.map(([messages, renders, answerFiles, guestFiles]) => [
        ...new Set([
          ...messages.map((row) => row.id as string),
          ...guestFiles.map((row) => row.id as string),
          ...answerFiles.map((row) => row.id as string),
          ...threadHtmlRenderAttachmentIds(
            threadId,
            renders.map((row) => {
              const item = parseEncodedPayload(row.payload_json);
              return {
                toolName: typeof item.toolName === "string" ? item.toolName : null,
                output: item.output,
              };
            }),
          ),
        ]),
      ]),
      Effect.mapError((cause) => new ProjectionStoreReadError({ threadId, cause })),
    );

  const getRuntimeRecoveryProjection: ProjectionStoreV2Shape["getRuntimeRecoveryProjection"] =
    Effect.fn("ProjectionStore.getRuntimeRecoveryProjection")(
      function* (threadId) {
        const threadRows = yield* source.read(
          { kind: "thread", thread_id: threadId },
          ProjectionContextRows.PayloadRows,
        );
        if (threadRows[0] === undefined) {
          return yield* new ProjectionStoreThreadNotFoundError({ threadId });
        }
        const thread = yield* decodeThreadPayload(threadRows[0].payload_json);
        // One worker round trip and one SQLite snapshot; decisions still read after the prior commit.
        const rows = yield* source.read(
          { kind: "runtime_recovery", thread_id: threadId },
          ProjectionContextRows.SectionPayloadRows,
        );
        const readRows = (section: string) => rows.filter((row) => row.section === section);
        const [
          runs,
          attempts,
          nodes,
          subagents,
          providerSessions,
          providerThreads,
          providerTurns,
          runtimeRequests,
          messages,
          turnItems,
        ] = yield* Effect.all([
          decodeRows(decodeRunPayload, threadId)(readRows("runs")),
          decodeRows(decodeRunAttemptPayload, threadId)(readRows("attempts")),
          decodeRows(decodeNodePayload, threadId)(readRows("nodes")),
          decodeRows(decodeSubagentPayload, threadId)(readRows("subagents")),
          decodeRows(decodeProviderSessionPayload, threadId)(readRows("providerSessions")),
          decodeRows(decodeProviderThreadPayload, threadId)(readRows("providerThreads")),
          decodeRows(decodeProviderTurnPayload, threadId)(readRows("providerTurns")),
          decodeRows(decodeRuntimeRequestPayload, threadId)(readRows("runtimeRequests")),
          decodeRows(decodeMessagePayload, threadId)(readRows("messages")),
          decodeRows(decodeTurnItemPayload, threadId)(readRows("turnItems")),
        ]);
        return {
          thread,
          runs,
          attempts,
          nodes,
          subagents,
          providerSessions,
          providerThreads,
          providerTurns,
          runtimeRequests,
          messages,
          turnItems,
        } satisfies ProjectionRuntimeRecoveryState;
      },
      (effect, threadId) =>
        effect.pipe(
          Effect.mapError((cause) =>
            isProjectionStoreThreadNotFoundError(cause)
              ? cause
              : new ProjectionStoreReadError({ threadId, cause }),
          ),
        ),
    );

  return {
    getTurnStartContext,
    getRuntimeRecoveryProjection,
    getCheckpointContext,
    getCheckpointCaptureContext,
    getCheckpointRollbackContext,
    getThreadAttachmentIds,
  };
}

export function makeProjectionTimelinePages(
  readTimelineIndex: ReturnType<typeof makeTimelineIndex>,
  persistence: ProjectionTimelineRows,
) {
  const controlReadError = (threadId: ThreadId) => (cause: unknown) =>
    isProjectionStoreThreadNotFoundError(cause)
      ? cause
      : new ProjectionStoreReadError({ threadId, cause });
  const getThreadHistoryPage: ProjectionStoreV2Shape["getThreadHistoryPage"] = (
    threadId,
    cursor,
    throughEntryId,
    conversationOnly,
    workMode,
  ) =>
    persistence
      .withReadTransaction(
        Effect.gen(function* () {
          const index = yield* readTimelineIndex(threadId, new Set(), workMode !== undefined);
          if (index.records.thread.deletedAt !== null)
            return yield* new ProjectionStoreThreadNotFoundError({ threadId });
          const snapshotSequence = yield* persistence.snapshotSequence(threadId);
          const indexed = index.visible.map((row, position) => ({
            ...row,
            position,
            canonicalPosition: position,
          }));
          const historyItems = workMode === "deferred" ? deferWorkHistoryRows(indexed) : indexed;
          const selection = yield* Effect.try({
            try: () => {
              const policy = olderHistoryPagePolicy({ items: indexed, cursor, throughEntryId });
              const page =
                workMode === "work"
                  ? selectDeferredWorkPage({
                      items: indexed,
                      cursor,
                      throughEntryId,
                      snapshotSequence,
                      rowEncodedBytes: () => 0,
                    })
                  : selectHistoryPageFromCursor({
                      items: historyItems,
                      cursor,
                      snapshotSequence,
                      rowEncodedBytes: () => 0,
                      policy,
                    });
              return { policy, page };
            },
            catch: (cause) => new ProjectionStoreReadError({ threadId, cause }),
          });
          const { page } = selection;
          const policy = workMode === "work" ? THREAD_HISTORY_PAGE_POLICY : selection.policy;
          const end = (page.items.at(-1)?.canonicalPosition ?? -1) + 1;
          const hasUserTurns = index.visible
            .slice(0, end)
            .some((row) => isThreadHistoryUserTurn(row.item));
          const materialize = Effect.fn("ProjectionStore.materializeHistoryRows")(function* (
            rows: ReadonlyArray<(typeof indexed)[number]>,
          ) {
            if (rows.length === 0) return [];
            const sources = yield* encodeTimelineSources(
              rows
                .filter((row) => !row.synthetic)
                .map((row) => ({ threadId: row.sourceThreadId, id: row.sourceItemId })),
            );
            const payloads = yield* persistence.materialize(sources);
            const byIdentity = new Map(
              payloads.map((row) => [`${row.thread_id}:${row.turn_item_id}`, row.payload_json]),
            );
            return yield* Effect.forEach(rows, (row) =>
              Effect.gen(function* () {
                const payload = byIdentity.get(`${row.sourceThreadId}:${row.sourceItemId}`);
                const item =
                  row.synthetic ??
                  (payload === undefined
                    ? yield* new ProjectionStoreReadError({ threadId })
                    : yield* decodeTurnItemPayload(payload));
                return {
                  position: row.canonicalPosition,
                  visibility: row.visibility,
                  sourceThreadId: row.sourceThreadId,
                  sourceItemId: row.sourceItemId,
                  item: projectTurnItemForWire(
                    row.item.deferredWork === undefined
                      ? item
                      : { ...item, deferredWork: row.item.deferredWork },
                  ),
                };
              }),
            );
          });
          const items = yield* materialize(
            conversationOnly && hasUserTurns
              ? page.items.filter((row) => isConversationHistoryItem(row.item))
              : page.items,
          );
          const bounded = selectRecentTimelineWindow({ items, snapshotSequence, policy });
          const requests =
            workMode === "work"
              ? []
              : yield* materialize(missingInterruptRequests(indexed, bounded.items));
          const pageItems = [...requests, ...bounded.items].map((row, position) => ({
            ...row,
            position,
          }));
          return {
            snapshotSequence,
            items: conversationOnly
              ? pageItems.filter((row) => isConversationHistoryItem(row.item))
              : pageItems,
            nextCursor: bounded.hasMoreHistory ? bounded.nextCursor : page.nextCursor,
            hasMoreHistory: bounded.hasMoreHistory || page.hasMoreHistory,
          };
        }),
      )
      .pipe(Effect.mapError(controlReadError(threadId)));

  const getTimelinePage: ProjectionStoreV2Shape["getTimelinePage"] = (threadId, options) =>
    persistence
      .withReadTransaction(
        Effect.gen(function* () {
          const index = yield* readTimelineIndex(threadId, new Set());
          const attachmentOwners =
            options.attachmentId === undefined
              ? undefined
              : new Set(
                  (yield* persistence.attachmentOwners(
                    [...new Set(index.visible.map((row) => row.sourceThreadId))],
                    options.attachmentId,
                  )).map((row) => `${row.thread_id}:${row.turn_item_id}`),
                );
          const matching = index.visible
            .map((row, position) => ({ ...row, position }))
            .filter((row) =>
              options.itemId === undefined
                ? row.position > (options.afterPosition ?? -1) &&
                  (options.view === "activity" ||
                    ["user_message", "assistant_message", "proposed_plan"].includes(row.item.type))
                : row.sourceItemId === options.itemId,
            )
            .filter(
              (row) => attachmentOwners?.has(`${row.sourceThreadId}:${row.sourceItemId}`) ?? true,
            );
          const page = matching.slice(0, options.limit);
          const sources = yield* encodeTimelineSources(
            page
              .filter((row) => row.synthetic === undefined)
              .map((row) => ({ threadId: row.sourceThreadId, id: row.sourceItemId })),
          );
          const payloads = yield* persistence.materialize(sources);
          const byIdentity = new Map(
            payloads.map((row) => [`${row.thread_id}:${row.turn_item_id}`, row.payload_json]),
          );
          const items = yield* Effect.forEach(page, (row) =>
            Effect.gen(function* () {
              if (row.synthetic !== undefined)
                return {
                  position: row.position,
                  sourceThreadId: row.sourceThreadId,
                  sourceItemId: row.sourceItemId,
                  visibility: row.visibility,
                  item: row.synthetic,
                };
              const payload = byIdentity.get(`${row.sourceThreadId}:${row.sourceItemId}`);
              if (payload === undefined)
                return yield* new ProjectionStoreReadError({
                  threadId,
                  cause: "Timeline item disappeared during snapshot",
                });
              const item = yield* decodeTurnItemPayload(payload).pipe(
                Effect.mapError(
                  (cause) => new ProjectionStoreReadError({ threadId: row.sourceThreadId, cause }),
                ),
              );
              return {
                position: row.position,
                sourceThreadId: row.sourceThreadId,
                sourceItemId: row.sourceItemId,
                visibility: row.visibility,
                item,
              };
            }),
          );
          return {
            items,
            totalItems: index.visible.length,
            hasMore: matching.length > page.length,
          };
        }),
      )
      .pipe(
        Effect.mapError((cause) =>
          isProjectionStoreThreadNotFoundError(cause) || isProjectionStoreReadError(cause)
            ? cause
            : new ProjectionStoreReadError({ threadId, cause }),
        ),
      );

  return { getThreadHistoryPage, getTimelinePage };
}

export const layer: Layer.Layer<ProjectionStoreV2, never, SqlClient.SqlClient> = Layer.effect(
  ProjectionStoreV2,
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const lookups = makeProjectionLookups(yield* SqliteProjectionLookupRows.make);
    const controls = makeProjectionControls(yield* SqliteProjectionControlRows.make, lookups);
    const contexts = makeProjectionContexts(yield* SqliteProjectionContextRows.make, lookups);
    const shells = makeProjectionShells(yield* SqliteProjectionShellRows.make);

    const inlineWorkOutput = sql`item.type = 'dynamic_tool' AND (
      item.payload_json LIKE '%htmlRender%' OR item.payload_json LIKE ${`%${MCP_APP_OUTPUT_KEY}%`}
      OR item.payload_json LIKE '%"viewedImagePath"%'
      OR item.payload_json LIKE '%"toolName":"%delegate_task"%')`;

    // Start from the few owned provider threads; a thread can have a large settled node history.
    const nodesOwningProviderThreads = (threadId: ThreadId) => sql`
      SELECT owner.node_id
      FROM orchestration_v2_projection_provider_threads AS owned
        INDEXED BY orchestration_v2_projection_provider_threads_owner_idx
      CROSS JOIN orchestration_v2_projection_nodes AS owner
        ON owner.node_id = owned.owner_node_id
      WHERE owned.owner_node_id IS NOT NULL AND owner.thread_id = ${threadId}
    `;

    const keepRecordedRunField = (payload: Statement.Fragment, path: string) => sql`
      CASE
        WHEN json_type(excluded.payload_json, ${path}) IS NULL
          AND json_type(orchestration_v2_projection_runs.payload_json, ${path}) IS NOT NULL
        THEN json_set(
          ${payload},
          ${path},
          json_extract(orchestration_v2_projection_runs.payload_json, ${path})
        )
        ELSE ${payload}
      END
    `;

    const apply: ProjectionStoreV2Shape["apply"] =
      /* This helper runs for individual orchestration events; the enclosing operation owns the span. */
      Effect.fnUntraced(
        function* (event) {
          switch (event.type) {
            case "thread.pull-request-link-updated": {
              const thread = yield* getThread(event.threadId);
              yield* apply({
                ...event,
                type: "thread.pull-request-synced",
                payload: applyThreadPullRequestLinkUpdate(thread, event.payload),
              });
              break;
            }
            case "thread.created":
            case "thread.archived":
            case "thread.unarchived":
            case "thread.deleted":
            case "thread.settled":
            case "thread.unsettled":
            case "thread.snoozed":
            case "thread.unsnoozed":
            case "thread.auto-settle-set":
            case "thread.pinned":
            case "thread.unpinned":
            case "thread.pin-reordered":
            case "thread.active-reordered":
            case "thread.tags-updated":
            case "thread.visited":
            case "thread.marked-unread":
            case "thread.metadata-updated":
            case "thread.pull-request-synced":
            case "thread.runtime-mode-updated":
            case "thread.interaction-mode-updated":
            case "thread.model-selection-updated":
            case "thread.provider-switched":
            case "thread.guest-share-updated": {
              const payloadJson = yield* encodeThreadPayload(event.payload);
              const payload = parseEncodedPayload(payloadJson);
              yield* sql`
              INSERT INTO orchestration_v2_projection_threads (
                thread_id,
                project_id,
                title,
                default_provider,
                provider_instance_id,
                runtime_mode,
                interaction_mode,
                active_provider_thread_id,
                created_at,
                updated_at,
                archived_at,
                deleted_at,
                payload_json
              )
              VALUES (
                ${event.payload.id},
                ${event.payload.projectId},
                ${event.payload.title},
                ${event.payload.providerInstanceId},
                ${event.payload.providerInstanceId},
                ${event.payload.runtimeMode},
                ${event.payload.interactionMode},
                ${event.payload.activeProviderThreadId},
                ${stringField(payload, "createdAt")},
                ${stringField(payload, "updatedAt")},
                ${nullableStringField(payload, "archivedAt")},
                ${nullableStringField(payload, "deletedAt")},
                ${payloadJson}
              )
              ON CONFLICT(thread_id)
              DO UPDATE SET
                project_id = excluded.project_id,
                title = excluded.title,
                default_provider = excluded.default_provider,
                provider_instance_id = excluded.provider_instance_id,
                runtime_mode = excluded.runtime_mode,
                interaction_mode = excluded.interaction_mode,
                active_provider_thread_id = excluded.active_provider_thread_id,
                created_at = excluded.created_at,
                updated_at = excluded.updated_at,
                archived_at = excluded.archived_at,
                deleted_at = excluded.deleted_at,
                payload_json = excluded.payload_json
            `;
              break;
            }
            case "run.created":
            case "run.updated": {
              const payloadJson = yield* encodeRunPayload(event.payload);
              const payload = parseEncodedPayload(payloadJson);
              yield* sql`
              INSERT INTO orchestration_v2_projection_runs (
                run_id,
                thread_id,
                ordinal,
                provider,
                provider_instance_id,
                provider_thread_id,
                status,
                requested_at,
                completed_at,
                payload_json
              )
              VALUES (
                ${event.payload.id},
                ${event.payload.threadId},
                ${event.payload.ordinal},
                ${event.payload.providerInstanceId},
                ${event.payload.providerInstanceId},
                ${event.payload.providerThreadId},
                ${event.payload.status},
                ${stringField(payload, "requestedAt")},
                ${nullableStringField(payload, "completedAt")},
                ${payloadJson}
              )
              ON CONFLICT(run_id)
              DO UPDATE SET
                thread_id = excluded.thread_id,
                ordinal = excluded.ordinal,
                provider = excluded.provider,
                provider_instance_id = excluded.provider_instance_id,
                provider_thread_id = excluded.provider_thread_id,
                status = excluded.status,
                requested_at = excluded.requested_at,
                completed_at = excluded.completed_at,
                payload_json = ${keepRecordedRunField(
                  keepRecordedRunField(sql`excluded.payload_json`, "$.delegatedCompletion"),
                  "$.restartCancelledBackgroundWork",
                )}
            `;
              break;
            }
            case "run.background-work-cancelled": {
              // Only this field changes, so a concurrent lifecycle write is never regressed.
              const workJson = yield* encodeRestartCancelledBackgroundWork(
                event.payload.restartCancelledBackgroundWork,
              );
              yield* sql`
              UPDATE orchestration_v2_projection_runs
              SET payload_json = json_set(
                payload_json,
                '$.restartCancelledBackgroundWork',
                json(${workJson})
              )
              WHERE run_id = ${event.payload.runId} AND thread_id = ${event.threadId}
            `;
              break;
            }
            case "run-attempt.created":
            case "run-attempt.updated": {
              const payloadJson = yield* encodeRunAttemptPayload(event.payload);
              yield* sql`
              INSERT INTO orchestration_v2_projection_run_attempts (
                attempt_id,
                thread_id,
                run_id,
                attempt_ordinal,
                root_node_id,
                provider,
                provider_instance_id,
                provider_thread_id,
                provider_turn_id,
                status,
                payload_json
              )
              VALUES (
                ${event.payload.id},
                ${event.threadId},
                ${event.payload.runId},
                ${event.payload.attemptOrdinal},
                ${event.payload.rootNodeId},
                ${event.payload.providerInstanceId},
                ${event.payload.providerInstanceId},
                ${event.payload.providerThreadId},
                ${event.payload.providerTurnId},
                ${event.payload.status},
                ${payloadJson}
              )
              ON CONFLICT(attempt_id)
              DO UPDATE SET
                thread_id = excluded.thread_id,
                run_id = excluded.run_id,
                attempt_ordinal = excluded.attempt_ordinal,
                root_node_id = excluded.root_node_id,
                provider = excluded.provider,
                provider_instance_id = excluded.provider_instance_id,
                provider_thread_id = excluded.provider_thread_id,
                provider_turn_id = excluded.provider_turn_id,
                status = excluded.status,
                payload_json = excluded.payload_json
            `;
              break;
            }
            case "node.updated": {
              const payloadJson = yield* encodeNodePayload(event.payload);
              const payload = parseEncodedPayload(payloadJson);
              yield* sql`
              INSERT INTO orchestration_v2_projection_nodes (
                node_id,
                thread_id,
                run_id,
                parent_node_id,
                root_node_id,
                kind,
                status,
                provider_thread_id,
                provider_turn_id,
                started_at,
                completed_at,
                payload_json
              )
              VALUES (
                ${event.payload.id},
                ${event.payload.threadId},
                ${event.payload.runId},
                ${event.payload.parentNodeId},
                ${event.payload.rootNodeId},
                ${event.payload.kind},
                ${event.payload.status},
                ${event.payload.providerThreadId},
                ${event.payload.providerTurnId},
                ${nullableStringField(payload, "startedAt")},
                ${nullableStringField(payload, "completedAt")},
                ${payloadJson}
              )
              ON CONFLICT(node_id)
              DO UPDATE SET
                thread_id = excluded.thread_id,
                run_id = excluded.run_id,
                parent_node_id = excluded.parent_node_id,
                root_node_id = excluded.root_node_id,
                kind = excluded.kind,
                status = excluded.status,
                provider_thread_id = excluded.provider_thread_id,
                provider_turn_id = excluded.provider_turn_id,
                started_at = excluded.started_at,
                completed_at = excluded.completed_at,
                payload_json = excluded.payload_json
            `;
              break;
            }
            case "subagent.updated": {
              const payloadJson = yield* encodeSubagentPayload(event.payload);
              const payload = parseEncodedPayload(payloadJson);
              yield* sql`
              INSERT INTO orchestration_v2_projection_subagents (
                subagent_id,
                thread_id,
                run_id,
                parent_node_id,
                provider,
                driver,
                provider_instance_id,
                provider_thread_id,
                child_thread_id,
                origin,
                status,
                started_at,
                completed_at,
                updated_at,
                completion_delivery_state,
                payload_json
              )
              VALUES (
                ${event.payload.id},
                ${event.payload.threadId},
                ${event.payload.runId},
                ${event.payload.parentNodeId},
                ${event.payload.providerInstanceId},
                ${event.payload.driver},
                ${event.payload.providerInstanceId},
                ${event.payload.providerThreadId},
                ${event.payload.childThreadId},
                ${event.payload.origin},
                ${event.payload.status},
                ${nullableStringField(payload, "startedAt")},
                ${nullableStringField(payload, "completedAt")},
                ${stringField(payload, "updatedAt")},
                ${event.payload.completionDelivery?.state ?? null},
                ${payloadJson}
              )
              ON CONFLICT(subagent_id)
              DO UPDATE SET
                thread_id = excluded.thread_id,
                run_id = excluded.run_id,
                parent_node_id = excluded.parent_node_id,
                provider = excluded.provider,
                driver = excluded.driver,
                provider_instance_id = excluded.provider_instance_id,
                provider_thread_id = excluded.provider_thread_id,
                child_thread_id = excluded.child_thread_id,
                origin = excluded.origin,
                status = excluded.status,
                started_at = excluded.started_at,
                completed_at = excluded.completed_at,
                updated_at = excluded.updated_at,
                completion_delivery_state = COALESCE(
                  excluded.completion_delivery_state,
                  orchestration_v2_projection_subagents.completion_delivery_state
                ),
                payload_json = CASE
                  WHEN json_type(excluded.payload_json, '$.completionDelivery') IS NULL
                    AND json_type(orchestration_v2_projection_subagents.payload_json, '$.completionDelivery') IS NOT NULL
                  THEN json_set(
                    excluded.payload_json,
                    '$.completionDelivery',
                    json_extract(
                      orchestration_v2_projection_subagents.payload_json,
                      '$.completionDelivery'
                    )
                  )
                  ELSE excluded.payload_json
                END
            `;
              break;
            }
            case "provider-session.attached":
            case "provider-session.updated": {
              const payloadJson = yield* encodeProviderSessionPayload(event.payload);
              const payload = parseEncodedPayload(payloadJson);
              yield* sql`
              INSERT INTO orchestration_v2_projection_provider_sessions (
                provider_session_id,
                thread_id,
                provider,
                driver,
                provider_instance_id,
                status,
                updated_at,
                payload_json
              )
              VALUES (
                ${event.payload.id},
                ${event.threadId},
                ${event.payload.providerInstanceId},
                ${event.payload.driver},
                ${event.payload.providerInstanceId},
                ${event.payload.status},
                ${stringField(payload, "updatedAt")},
                ${payloadJson}
              )
              ON CONFLICT(provider_session_id)
              DO UPDATE SET
                thread_id = excluded.thread_id,
                provider = excluded.provider,
                driver = excluded.driver,
                provider_instance_id = excluded.provider_instance_id,
                status = excluded.status,
                updated_at = excluded.updated_at,
                payload_json = excluded.payload_json
            `;
              if (event.type === "provider-session.attached") {
                yield* sql`
                INSERT OR IGNORE INTO orchestration_v2_projection_provider_session_bindings (
                  provider_session_id,
                  thread_id
                )
                VALUES (${event.payload.id}, ${event.threadId})
              `;
              }
              break;
            }
            case "provider-session.detached": {
              yield* sql`
              DELETE FROM orchestration_v2_projection_provider_session_bindings
              WHERE provider_session_id = ${event.payload.providerSessionId}
                AND thread_id = ${event.threadId}
            `;
              break;
            }
            case "provider-thread.updated": {
              const payloadJson = yield* encodeProviderThreadPayload(event.payload);
              const payload = parseEncodedPayload(payloadJson);
              yield* sql`
              INSERT INTO orchestration_v2_projection_provider_threads (
                provider_thread_id,
                thread_id,
                owner_node_id,
                provider,
                driver,
                provider_instance_id,
                provider_session_id,
                status,
                first_run_ordinal,
                last_run_ordinal,
                updated_at,
                payload_json
              )
              VALUES (
                ${event.payload.id},
                ${event.payload.appThreadId},
                ${event.payload.ownerNodeId},
                ${event.payload.providerInstanceId},
                ${event.payload.driver},
                ${event.payload.providerInstanceId},
                ${event.payload.providerSessionId},
                ${event.payload.status},
                ${event.payload.firstRunOrdinal},
                ${event.payload.lastRunOrdinal},
                ${stringField(payload, "updatedAt")},
                ${payloadJson}
              )
              ON CONFLICT(provider_thread_id)
              DO UPDATE SET
                thread_id = excluded.thread_id,
                owner_node_id = excluded.owner_node_id,
                provider = excluded.provider,
                driver = excluded.driver,
                provider_instance_id = excluded.provider_instance_id,
                provider_session_id = excluded.provider_session_id,
                status = excluded.status,
                first_run_ordinal = excluded.first_run_ordinal,
                last_run_ordinal = excluded.last_run_ordinal,
                updated_at = excluded.updated_at,
                payload_json = excluded.payload_json
            `;
              if (
                event.payload.appThreadId !== null &&
                !isQueuedProviderThreadPlaceholder(event.payload)
              ) {
                const threadRows = yield* sql<PayloadRow>`
                SELECT payload_json
                FROM orchestration_v2_projection_threads
                WHERE thread_id = ${event.payload.appThreadId}
                LIMIT 1
              `;
                const threadRow = threadRows[0];
                if (threadRow !== undefined) {
                  const thread = yield* decodeThreadPayload(threadRow.payload_json);
                  const updatedThread = {
                    ...thread,
                    activeProviderThreadId: event.payload.id,
                    updatedAt: event.payload.updatedAt,
                  };
                  const updatedThreadPayloadJson = yield* encodeThreadPayload(updatedThread);
                  yield* sql`
                  UPDATE orchestration_v2_projection_threads
                  SET
                    active_provider_thread_id = ${event.payload.id},
                    updated_at = ${stringField(parseEncodedPayload(updatedThreadPayloadJson), "updatedAt")},
                    payload_json = ${updatedThreadPayloadJson}
                  WHERE thread_id = ${event.payload.appThreadId}
                `;
                }
              }
              break;
            }
            case "provider-turn.updated": {
              const existingRows =
                event.payload.tokenUsage === undefined
                  ? yield* sql<PayloadRow>`
                    SELECT payload_json
                    FROM orchestration_v2_projection_provider_turns
                    WHERE provider_turn_id = ${event.payload.id}
                    LIMIT 1
                  `
                  : [];
              const existing = existingRows[0];
              const providerTurn =
                existing === undefined
                  ? event.payload
                  : upsertProviderTurn(
                      [yield* decodeProviderTurnPayload(existing.payload_json)],
                      event.payload,
                    )[0]!;
              const payloadJson = yield* encodeProviderTurnPayload(providerTurn);
              const payload = parseEncodedPayload(payloadJson);
              yield* sql`
              INSERT INTO orchestration_v2_projection_provider_turns (
                provider_turn_id,
                thread_id,
                provider_thread_id,
                node_id,
                run_attempt_id,
                ordinal,
                status,
                started_at,
                completed_at,
                payload_json
              )
              VALUES (
                ${event.payload.id},
                ${event.threadId},
                ${providerTurn.providerThreadId},
                ${providerTurn.nodeId},
                ${providerTurn.runAttemptId},
                ${providerTurn.ordinal},
                ${providerTurn.status},
                ${nullableStringField(payload, "startedAt")},
                ${nullableStringField(payload, "completedAt")},
                ${payloadJson}
              )
              ON CONFLICT(provider_turn_id)
              DO UPDATE SET
                thread_id = excluded.thread_id,
                provider_thread_id = excluded.provider_thread_id,
                node_id = excluded.node_id,
                run_attempt_id = excluded.run_attempt_id,
                ordinal = excluded.ordinal,
                status = excluded.status,
                started_at = excluded.started_at,
                completed_at = excluded.completed_at,
                payload_json = excluded.payload_json
            `;
              break;
            }
            case "runtime-request.updated": {
              const payloadJson = yield* encodeRuntimeRequestPayload(event.payload);
              const payload = parseEncodedPayload(payloadJson);
              yield* sql`
              INSERT INTO orchestration_v2_projection_runtime_requests (
                runtime_request_id,
                thread_id,
                node_id,
                provider_turn_id,
                kind,
                status,
                created_at,
                resolved_at,
                payload_json
              )
              VALUES (
                ${event.payload.id},
                ${event.threadId},
                ${event.payload.nodeId},
                ${event.payload.providerTurnId},
                ${event.payload.kind},
                ${event.payload.status},
                ${stringField(payload, "createdAt")},
                ${nullableStringField(payload, "resolvedAt")},
                ${payloadJson}
              )
              ON CONFLICT(runtime_request_id)
              DO UPDATE SET
                thread_id = excluded.thread_id,
                node_id = excluded.node_id,
                provider_turn_id = excluded.provider_turn_id,
                kind = excluded.kind,
                status = excluded.status,
                created_at = excluded.created_at,
                resolved_at = excluded.resolved_at,
                payload_json = excluded.payload_json
            `;
              break;
            }
            case "message.updated": {
              const payloadJson = yield* encodeMessagePayload(event.payload);
              const createdAt = yield* encodePayloadTimestamp(event.payload.createdAt);
              const updatedAt = yield* encodePayloadTimestamp(event.payload.updatedAt);
              yield* sql`
              INSERT INTO orchestration_v2_projection_messages (
                message_id,
                thread_id,
                run_id,
                node_id,
                role,
                streaming,
                created_at,
                updated_at,
                payload_json
              )
              VALUES (
                ${event.payload.id},
                ${event.payload.threadId},
                ${event.payload.runId},
                ${event.payload.nodeId},
                ${event.payload.role},
                ${booleanInt(event.payload.streaming)},
                ${createdAt},
                ${updatedAt},
                ${payloadJson}
              )
              ON CONFLICT(message_id)
              DO UPDATE SET
                thread_id = excluded.thread_id,
                run_id = excluded.run_id,
                node_id = excluded.node_id,
                role = excluded.role,
                streaming = excluded.streaming,
                created_at = excluded.created_at,
                updated_at = excluded.updated_at,
                payload_json = excluded.payload_json
            `;
              break;
            }
            case "plan.updated": {
              const payloadJson = yield* encodePlanPayload(event.payload);
              yield* sql`
              INSERT INTO orchestration_v2_projection_plans (
                plan_id,
                thread_id,
                run_id,
                node_id,
                kind,
                status,
                payload_json
              )
              VALUES (
                ${event.payload.id},
                ${event.payload.threadId},
                ${event.payload.runId},
                ${event.payload.nodeId},
                ${event.payload.kind},
                ${event.payload.status},
                ${payloadJson}
              )
              ON CONFLICT(plan_id)
              DO UPDATE SET
                thread_id = excluded.thread_id,
                run_id = excluded.run_id,
                node_id = excluded.node_id,
                kind = excluded.kind,
                status = excluded.status,
                payload_json = excluded.payload_json
            `;
              break;
            }
            case "turn-item.updated": {
              const payloadJson = yield* encodeTurnItemPayload(event.payload);
              const updatedAt = yield* encodePayloadTimestamp(event.payload.updatedAt);
              yield* sql`
              INSERT INTO orchestration_v2_projection_turn_items (
                turn_item_id,
                thread_id,
                run_id,
                node_id,
                provider_thread_id,
                provider_turn_id,
                parent_item_id,
                ordinal,
                type,
                status,
                updated_at,
                payload_json
              )
              VALUES (
                ${event.payload.id},
                ${event.payload.threadId},
                ${event.payload.runId},
                ${event.payload.nodeId},
                ${event.payload.providerThreadId},
                ${event.payload.providerTurnId},
                ${event.payload.parentItemId},
                ${event.payload.ordinal},
                ${event.payload.type},
                ${event.payload.status},
                ${updatedAt},
                ${payloadJson}
              )
              ON CONFLICT(turn_item_id)
              DO UPDATE SET
                thread_id = excluded.thread_id,
                run_id = excluded.run_id,
                node_id = excluded.node_id,
                provider_thread_id = excluded.provider_thread_id,
                provider_turn_id = excluded.provider_turn_id,
                parent_item_id = excluded.parent_item_id,
                ordinal = excluded.ordinal,
                type = excluded.type,
                status = excluded.status,
                updated_at = excluded.updated_at,
                payload_json = excluded.payload_json
            `;
              break;
            }
            case "checkpoint-scope.created": {
              const payloadJson = yield* encodeCheckpointScopePayload(event.payload);
              const payload = parseEncodedPayload(payloadJson);
              yield* sql`
              INSERT INTO orchestration_v2_projection_checkpoint_scopes (
                scope_id,
                thread_id,
                run_id,
                node_id,
                provider_thread_id,
                kind,
                ordinal_within_parent,
                advances_app_run_count,
                created_at,
                payload_json
              )
              VALUES (
                ${event.payload.id},
                ${event.payload.threadId},
                ${event.payload.runId},
                ${event.payload.nodeId},
                ${event.payload.providerThreadId},
                ${event.payload.kind},
                ${event.payload.ordinalWithinParent},
                ${booleanInt(event.payload.advancesAppRunCount)},
                ${stringField(payload, "createdAt")},
                ${payloadJson}
              )
              ON CONFLICT(scope_id)
              DO UPDATE SET
                thread_id = excluded.thread_id,
                run_id = excluded.run_id,
                node_id = excluded.node_id,
                provider_thread_id = excluded.provider_thread_id,
                kind = excluded.kind,
                ordinal_within_parent = excluded.ordinal_within_parent,
                advances_app_run_count = excluded.advances_app_run_count,
                created_at = excluded.created_at,
                payload_json = excluded.payload_json
            `;
              break;
            }
            case "checkpoint.captured": {
              const payloadJson = yield* encodeCheckpointPayload(event.payload);
              const payload = parseEncodedPayload(payloadJson);
              yield* sql`
              INSERT INTO orchestration_v2_projection_checkpoints (
                checkpoint_id,
                thread_id,
                scope_id,
                run_id,
                node_id,
                ordinal_within_scope,
                app_run_ordinal,
                status,
                captured_at,
                payload_json
              )
              VALUES (
                ${event.payload.id},
                ${event.payload.threadId},
                ${event.payload.scopeId},
                ${event.payload.runId},
                ${event.payload.nodeId},
                ${event.payload.ordinalWithinScope},
                ${event.payload.appRunOrdinal},
                ${event.payload.status},
                ${stringField(payload, "capturedAt")},
                ${payloadJson}
              )
              ON CONFLICT(checkpoint_id)
              DO UPDATE SET
                thread_id = excluded.thread_id,
                scope_id = excluded.scope_id,
                run_id = excluded.run_id,
                node_id = excluded.node_id,
                ordinal_within_scope = excluded.ordinal_within_scope,
                app_run_ordinal = excluded.app_run_ordinal,
                status = excluded.status,
                captured_at = excluded.captured_at,
                payload_json = excluded.payload_json
            `;
              break;
            }
            case "checkpoint.rollback-requested":
              break;
            case "context-handoff.updated": {
              const payloadJson = yield* encodeContextHandoffPayload(event.payload);
              const payload = parseEncodedPayload(payloadJson);
              yield* sql`
              INSERT INTO orchestration_v2_projection_context_handoffs (
                context_handoff_id,
                thread_id,
                target_run_id,
                to_provider_thread_id,
                strategy,
                status,
                updated_at,
                payload_json
              )
              VALUES (
                ${event.payload.id},
                ${event.payload.threadId},
                ${event.payload.targetRunId},
                ${event.payload.toProviderThreadId},
                ${event.payload.strategy},
                ${event.payload.status},
                ${stringField(payload, "updatedAt")},
                ${payloadJson}
              )
              ON CONFLICT(context_handoff_id)
              DO UPDATE SET
                thread_id = excluded.thread_id,
                target_run_id = excluded.target_run_id,
                to_provider_thread_id = excluded.to_provider_thread_id,
                strategy = excluded.strategy,
                status = excluded.status,
                updated_at = excluded.updated_at,
                payload_json = excluded.payload_json
            `;
              break;
            }
            case "context-transfer.created":
            case "context-transfer.updated": {
              const payloadJson = yield* encodeContextTransferPayload(event.payload);
              const payload = parseEncodedPayload(payloadJson);
              yield* sql`
              INSERT INTO orchestration_v2_projection_context_transfers (
                context_transfer_id,
                source_thread_id,
                target_thread_id,
                target_run_id,
                type,
                status,
                updated_at,
                payload_json
              )
              VALUES (
                ${event.payload.id},
                ${event.payload.sourceThreadId},
                ${event.payload.targetThreadId},
                ${event.payload.targetRunId},
                ${event.payload.type},
                ${event.payload.status},
                ${stringField(payload, "updatedAt")},
                ${payloadJson}
              )
              ON CONFLICT(context_transfer_id)
              DO UPDATE SET
                source_thread_id = excluded.source_thread_id,
                target_thread_id = excluded.target_thread_id,
                target_run_id = excluded.target_run_id,
                type = excluded.type,
                status = excluded.status,
                updated_at = excluded.updated_at,
                payload_json = excluded.payload_json
            `;
              break;
            }
          }

          if (
            event.type !== "thread.created" &&
            event.type !== "thread.archived" &&
            event.type !== "thread.unarchived" &&
            event.type !== "thread.deleted" &&
            event.type !== "thread.settled" &&
            event.type !== "thread.unsettled" &&
            event.type !== "thread.snoozed" &&
            event.type !== "thread.unsnoozed" &&
            event.type !== "thread.auto-settle-set" &&
            event.type !== "thread.pinned" &&
            event.type !== "thread.unpinned" &&
            event.type !== "thread.pin-reordered" &&
            event.type !== "thread.active-reordered" &&
            event.type !== "thread.tags-updated" &&
            event.type !== "thread.visited" &&
            event.type !== "thread.marked-unread" &&
            event.type !== "thread.metadata-updated" &&
            event.type !== "thread.pull-request-synced" &&
            event.type !== "thread.pull-request-link-updated" &&
            event.type !== "thread.runtime-mode-updated" &&
            event.type !== "thread.interaction-mode-updated" &&
            event.type !== "thread.model-selection-updated" &&
            event.type !== "thread.provider-switched" &&
            event.type !== "thread.guest-share-updated"
          ) {
            const updatedAt = yield* encodeThreadUpdatedAt(event.occurredAt);
            yield* sql`
            UPDATE orchestration_v2_projection_threads
            SET
              updated_at = ${updatedAt},
              payload_json = json_set(payload_json, '$.updatedAt', ${updatedAt})
            WHERE thread_id = ${event.threadId}
          `;
          }
        },
        (effect, event) =>
          effect.pipe(
            Effect.mapError(
              (cause) =>
                new ProjectionStoreApplyEventError({
                  eventType: event.type,
                  cause,
                }),
            ),
          ),
      );

    // Share selection so counting discarded ancestry preserves payload-read positions.
    const retainedWindowRows = Effect.fn("ProjectionStore.retainedWindowRows")(function* (
      threadId: ThreadId,
      window: ProjectionWindow,
    ) {
      const eligible = sql`
                WITH eligible AS NOT MATERIALIZED (
                  SELECT item.rowid AS item_rowid, item.payload_json, item.ordinal, item.turn_item_id,
                    item.run_id, item.node_id, item.type, item.status, run.status AS run_status,
                    attempt.attempt_id, attempt.status AS attempt_status
                  FROM orchestration_v2_projection_turn_items AS item
                  LEFT JOIN orchestration_v2_projection_runs AS run
                    ON run.run_id = item.run_id
                  LEFT JOIN orchestration_v2_projection_nodes AS node
                    ON ${window.deferWork ? 1 : 0} = 1 AND node.node_id = item.node_id
                  LEFT JOIN orchestration_v2_projection_run_attempts AS attempt
                    ON ${window.deferWork ? 1 : 0} = 1 AND attempt.attempt_id = (
                      SELECT attempt_id FROM orchestration_v2_projection_run_attempts
                      WHERE run_id = item.run_id AND root_node_id = COALESCE(node.root_node_id, item.node_id)
                      ORDER BY attempt_ordinal DESC LIMIT 1
                    )
                  WHERE item.thread_id = ${threadId}
                    AND item.ordinal <= COALESCE(
                      (
                        SELECT ordinal
                        FROM orchestration_v2_projection_turn_items
                        WHERE thread_id = ${threadId}
                          AND turn_item_id = ${window.anchorItemId ?? null}
                        LIMIT 1
                      ),
                      9223372036854775807
                    )
                    AND (
                      ${window.requiredRunId ?? null} IS NOT NULL
                      OR (
                        (run.status IS NULL OR run.status <> 'rolled_back')
                        AND NOT (
                          item.type = 'user_message'
                          AND json_extract(item.payload_json, '$.inputIntent') = 'queued_turn'
                          AND run.status IS 'cancelled'
                        )
                      )
                    )
                    AND (
                      ${window.requiredRunId ?? null} IS NULL
                      OR (
                        item.run_id IS NULL
                        AND EXISTS (
                          SELECT 1
                          FROM orchestration_v2_projection_threads AS source_thread
                          WHERE source_thread.thread_id = item.thread_id
                            AND json_extract(source_thread.payload_json, '$.historyOrigin') = 'v1_import'
                        )
                      )
                      OR run.ordinal <= (
                        SELECT required_run.ordinal
                        FROM orchestration_v2_projection_runs AS required_run
                        WHERE required_run.thread_id = item.thread_id
                          AND required_run.run_id = ${window.requiredRunId ?? null}
                        LIMIT 1
                      )
                    )
                    AND NOT (
                      item.type = 'run_interrupt_result'
                      AND EXISTS (
                        SELECT 1
                        FROM orchestration_v2_projection_run_attempts AS attempt
                        WHERE attempt.run_id = item.run_id
                          AND attempt.root_node_id = item.node_id
                          AND attempt.status = 'superseded'
                      )
                      AND NOT EXISTS (
                        SELECT 1
                        FROM orchestration_v2_projection_turn_items AS request
                        WHERE request.thread_id = item.thread_id
                          AND request.run_id = item.run_id
                          AND request.type = 'run_interrupt_request'
                      )
                    )
                )`;
      // Resolve anchors separately: repeated CTE expansion exceeds SQLite's subquery limit.
      const anchors = yield* sql<{ readonly ordinal: number; readonly latest: number | null }>`
                ${eligible}, run_starts AS (
                  -- A wake (PR watch, task completion) starts its run with a
                  -- notification instead of a user message. Read the first
                  -- input of each run, newest run first, and add the
                  -- notifications to the user-message turn starts. A run
                  -- without an eligible first input (queued, rolled back) is
                  -- skipped, so it cannot hide older turns. CROSS JOIN keeps
                  -- the runs as the outer loop, and each probe reads only the
                  -- first rows of a run.
                  SELECT start.ordinal, start.type
                  FROM orchestration_v2_projection_runs AS recent_run
                  CROSS JOIN eligible AS start ON start.item_rowid = (
                    SELECT first_input.rowid
                    FROM orchestration_v2_projection_turn_items AS first_input
                    WHERE first_input.run_id = recent_run.run_id
                      AND first_input.type IN ('user_message', 'notification')
                    ORDER BY first_input.ordinal
                    LIMIT 1
                  )
                  WHERE recent_run.thread_id = ${threadId}
                    AND recent_run.ordinal <= COALESCE(
                      (
                        SELECT anchor_run.ordinal
                        FROM orchestration_v2_projection_turn_items AS anchor_item
                        JOIN orchestration_v2_projection_runs AS anchor_run
                          ON anchor_run.run_id = anchor_item.run_id
                        WHERE anchor_item.thread_id = ${threadId}
                          AND anchor_item.turn_item_id = ${window.anchorItemId ?? null}
                        LIMIT 1
                      ),
                      9223372036854775807
                    )
                    AND ${window.userTurnLimit ?? null} IS NOT NULL
                  ORDER BY recent_run.ordinal DESC
                  LIMIT ${THREAD_HISTORY_MAX_RAW_TURNS + 2}
                ), turn_anchors AS (
                  SELECT ordinal, payload_json
                  FROM (
                    SELECT ordinal, payload_json
                    FROM eligible
                    WHERE type = 'user_message'
                      AND json_extract(payload_json, '$.inputIntent') IN ('turn_start', 'queued_turn')
                      AND ${window.userTurnLimit ?? null} IS NOT NULL
                    ORDER BY ordinal DESC
                    LIMIT ${THREAD_HISTORY_MAX_RAW_TURNS + 2}
                  )
                  UNION ALL
                  SELECT ordinal, NULL FROM run_starts WHERE type = 'notification'
                  ORDER BY ordinal DESC
                  LIMIT ${THREAD_HISTORY_MAX_RAW_TURNS + 2}
                ), user_anchors AS (
                  SELECT ordinal
                  FROM turn_anchors
                  WHERE json_extract(payload_json, '$.createdBy') = 'user'
                  ORDER BY ordinal DESC
                  LIMIT ${(window.userTurnLimit ?? 0) + 2}
                ), boundary AS (
                  SELECT CASE
                    WHEN COUNT(*) >= ${(window.userTurnLimit ?? 0) + 2} THEN MIN(ordinal)
                    WHEN (SELECT COUNT(*) FROM turn_anchors) >= ${THREAD_HISTORY_MAX_RAW_TURNS + 2}
                      THEN (SELECT MIN(ordinal) FROM turn_anchors)
                    ELSE 0
                  END AS ordinal
                  FROM user_anchors
                )
                SELECT ordinal, (SELECT MAX(ordinal) FROM user_anchors) AS latest FROM boundary
              `;
      const boundary = anchors[0]!;
      return sql`
                ${eligible}, blocked_runs AS MATERIALIZED (
                  SELECT DISTINCT run_id FROM orchestration_v2_projection_turn_items
                  WHERE thread_id = ${threadId} AND run_id IS NOT NULL
                    AND ${window.deferWork ? 1 : 0} = 1
                    AND run_id IN (
                      SELECT run_id FROM eligible
                      WHERE ordinal >= ${boundary.ordinal} AND run_id IS NOT NULL
                    )
                    AND (type IN ('run_interrupt_request', 'run_interrupt_result')
                      OR (type = 'error' AND status = 'failed' AND parent_item_id IS NULL))
                ), work_segments AS (
                  SELECT item_rowid, ordinal, turn_item_id, run_id, type, attempt_id,
                    MAX(CASE WHEN type = 'user_message' THEN ordinal ELSE 0 END)
                      OVER (PARTITION BY run_id ORDER BY ordinal) AS segment,
                    CASE WHEN status = 'completed' AND (run_status = 'completed' OR attempt_status = 'superseded')
                      AND type IN ('command_execution', 'reasoning', 'file_change', 'file_search', 'web_search', 'dynamic_tool')
                      AND NOT (${inlineWorkOutput})
                      AND run_id NOT IN (SELECT run_id FROM blocked_runs)
                      THEN 1 ELSE 0 END AS can_defer
                  FROM eligible AS item
                  WHERE ${window.deferWork ? 1 : 0} = 1
                    AND ${boundary.latest === null ? 0 : 1} = 1
                    AND ordinal >= ${boundary.ordinal}
                ), deferred_work AS (
                  SELECT run_id, segment, attempt_id, MIN(ordinal) AS first_ordinal,
                    MAX(ordinal) AS last_ordinal, COUNT(*) AS item_count
                  FROM work_segments WHERE can_defer = 1
                  GROUP BY run_id, segment, attempt_id HAVING COUNT(*) > ${THREAD_HISTORY_INLINE_WORK_ITEM_LIMIT}
                ), window_items AS NOT MATERIALIZED (
                  SELECT item_rowid, ordinal, turn_item_id, run_id, type
                  FROM eligible WHERE ${window.deferWork ? 1 : 0} = 0
                    OR ${boundary.latest === null ? 1 : 0} = 1
                  UNION ALL
                  SELECT item.item_rowid, item.ordinal, item.turn_item_id, item.run_id, item.type
                  FROM work_segments item LEFT JOIN deferred_work deferred
                    ON deferred.run_id = item.run_id AND deferred.segment = item.segment
                    AND deferred.attempt_id IS item.attempt_id
                  WHERE item.can_defer = 0 OR deferred.run_id IS NULL OR item.ordinal = deferred.first_ordinal
                ), latest_user_turn AS (
                  -- Mirrors selectOlderTimelinePage: whole turns while the
                  -- newest user turn fits the turn row budget, otherwise the
                  -- window pages from inside that turn with the row limit.
                  SELECT COUNT(*) AS row_count
                  FROM (
                    SELECT 1
                    FROM window_items
                    WHERE ordinal >= ${boundary.latest}
                    LIMIT ${THREAD_HISTORY_PAGE_POLICY.maxTurnItems + 1}
                  )
                ), selected AS (
                  -- Choose and sort rows by ID, then fetch payloads in that
                  -- order. A turn window can hold megabytes of tool output, and
                  -- carrying it through the union and sort cost about a second.
                  SELECT item_rowid, ordinal, turn_item_id, run_id, type
                  FROM window_items
                  WHERE ordinal >= ${boundary.ordinal}
                  ORDER BY ordinal DESC, turn_item_id DESC
                  LIMIT CASE
                    WHEN ${window.rowLimit} = 0 THEN 0
                    WHEN (SELECT row_count FROM latest_user_turn)
                      BETWEEN 1 AND ${THREAD_HISTORY_PAGE_POLICY.maxTurnItems}
                      THEN ${THREAD_HISTORY_TURN_SNAPSHOT_ROW_LIMIT}
                    ELSE ${window.rowLimit}
                  END
                ), retained AS MATERIALIZED (
                  SELECT item_rowid, ordinal, turn_item_id FROM selected
                  UNION
                  SELECT request.rowid AS item_rowid, request.ordinal, request.turn_item_id
                  FROM orchestration_v2_projection_turn_items AS request
                  WHERE request.run_id IN (
                      SELECT run_id FROM selected
                      WHERE type = 'run_interrupt_result' AND run_id IS NOT NULL
                    )
                    AND request.type = 'run_interrupt_request'
                  UNION
                  SELECT latest.item_rowid, latest.ordinal, latest.turn_item_id
                  FROM (
                    SELECT rowid AS item_rowid, ordinal, turn_item_id
                    FROM orchestration_v2_projection_turn_items
                    WHERE thread_id = ${threadId}
                      AND ${window.anchorItemId ?? null} IS NULL
                      AND ${window.requiredRunId ?? null} IS NULL
                    ORDER BY ordinal DESC, turn_item_id DESC
                    LIMIT 1
                  ) AS latest
                  ORDER BY ordinal ASC, turn_item_id ASC
                )
              `;
    });

    const readCanonicalProjection = Effect.fn("ProjectionStore.readCanonicalProjection")(
      function* (
        threadId: ThreadId,
        window?: ProjectionWindow,
        fields?: ReadonlyArray<ProjectionRecordField>,
        filter?: ProjectionRecordFilter,
      ) {
        const threadRows = yield* sql<PayloadRow>`
          SELECT payload_json
          FROM orchestration_v2_projection_threads
          WHERE thread_id = ${threadId}
          LIMIT 1
        `;
        const threadRow = threadRows[0];
        if (!threadRow) {
          return yield* new ProjectionStoreThreadNotFoundError({ threadId });
        }

        type WindowPayloadRow = PayloadRow & {
          readonly work_last_id?: string | null;
          readonly work_last_ordinal?: number | null;
          readonly work_item_count?: number | null;
        };
        const boundedTurnItemRows: ReadonlyArray<WindowPayloadRow> =
          (fields !== undefined && !fields.includes("turnItems")) ||
          (window === undefined &&
            matchesNothing(
              filter?.turnItemRequestIds,
              filter?.turnItemTypes,
              filter?.turnItemStatuses,
              filter?.turnItemRunIds,
              filter?.turnItemNodeIds,
            ))
            ? []
            : window === undefined
              ? yield* sql<PayloadRow>`
                SELECT payload_json
                FROM orchestration_v2_projection_turn_items
                WHERE thread_id = ${threadId}
                  ${filter?.turnItemRequestIds === undefined ? sql`` : sql`AND json_extract(payload_json, '$.requestId') IN (SELECT value FROM json_each(${encodeIdList(filter.turnItemRequestIds)}))`}
                  ${filter?.turnItemTypes === undefined ? sql`` : sql`AND type IN (SELECT value FROM json_each(${encodeIdList(filter.turnItemTypes)}))`}
                  ${filter?.turnItemStatuses === undefined ? sql`` : sql`AND status IN (SELECT value FROM json_each(${encodeIdList(filter.turnItemStatuses)}))`}
                  ${filter?.turnItemRunId === undefined ? sql`` : sql`AND run_id = ${filter.turnItemRunId}`}
                  ${filter?.turnItemNodeIds === undefined ? sql`` : sql`AND node_id IN (SELECT value FROM json_each(${encodeIdList(filter.turnItemNodeIds)}))`}
                  ${filter?.turnItemRunIds === undefined ? sql`` : sql`AND (run_id IN (SELECT value FROM json_each(${encodeIdList(filter.turnItemRunIds.filter((id): id is RunId => id !== null))})) OR (${filter.turnItemRunIds.includes(null) ? 1 : 0} = 1 AND run_id IS NULL))`}
                ORDER BY ordinal ASC, turn_item_id ASC
              `
              : yield* sql<PayloadRow>`
                ${yield* retainedWindowRows(threadId, window)}
                -- CROSS JOIN keeps the sorted IDs as the outer loop, so SQLite
                -- skips sorting again once the payloads are attached. Carry the
                -- rowid through selection to avoid a second string-key index lookup.
                SELECT item.payload_json, tail.turn_item_id AS work_last_id,
                  deferred.last_ordinal AS work_last_ordinal, deferred.item_count AS work_item_count
                FROM retained
                CROSS JOIN orchestration_v2_projection_turn_items AS item
                  ON item.rowid = retained.item_rowid
                LEFT JOIN deferred_work deferred ON deferred.run_id = item.run_id
                  AND deferred.first_ordinal = item.ordinal
                LEFT JOIN orchestration_v2_projection_turn_items tail
                  ON tail.turn_item_id = (
                    SELECT turn_item_id FROM orchestration_v2_projection_turn_items
                    WHERE thread_id = item.thread_id AND ordinal = deferred.last_ordinal
                    ORDER BY turn_item_id DESC LIMIT 1
                  )
                ORDER BY retained.ordinal ASC, retained.turn_item_id ASC
              `;
        // Decode items once for cohort ids and projection; repeated parsing duplicates retained tool output.
        const decodedTurnItems = yield* decodeRows(
          decodeTurnItemPayload,
          threadId,
        )(boundedTurnItemRows);
        const turnItems = decodedTurnItems.map((item, index) => {
          const row = boundedTurnItemRows[index]!;
          return row.work_last_id == null ||
            row.work_item_count == null ||
            row.work_last_ordinal == null
            ? item
            : {
                ...item,
                deferredWork: {
                  cursor: encodeThreadHistoryCursor({
                    snapshotSequence: 0,
                    sourceThreadId: threadId,
                    sourceItemId: TurnItemId.make(row.work_last_id),
                    position: row.work_last_ordinal,
                    inclusive: true,
                  }),
                  itemCount: row.work_item_count,
                  lastOrdinal: row.work_last_ordinal,
                },
              };
        });
        const cohortJson = (field: string) =>
          JSON.stringify(
            turnItems.flatMap((payload) => {
              const value = nullableStringField(payload, field);
              return value === null ? [] : [value];
            }),
          );
        const cohortRunIds = cohortJson("runId");
        const cohortNodeIds =
          window === undefined ||
          (fields !== undefined &&
            fields.every(
              (field) => field === "runs" || field === "attempts" || field === "turnItems",
            ))
            ? cohortJson("nodeId")
            : encodeIdList(
                (yield* sql<{ readonly node_id: string }>`
            WITH RECURSIVE retained(node_id) AS (
              SELECT value FROM json_each(${cohortJson("nodeId")})
              UNION
              SELECT node_id FROM orchestration_v2_projection_nodes
              WHERE thread_id = ${threadId} AND status IN ('pending','starting','running','waiting')
              UNION
              SELECT root_node_id FROM orchestration_v2_projection_run_attempts
              WHERE thread_id = ${threadId} AND (
                status IN ('pending','starting','running','waiting')
                OR run_id IN (SELECT value FROM json_each(${cohortRunIds}))
                OR run_id = ${window.requiredRunId ?? null})
              UNION
              SELECT json_extract(payload_json, '$.rootNodeId') FROM orchestration_v2_projection_runs
              WHERE thread_id = ${threadId} AND (
                status IN ('queued','preparing','starting','running','waiting')
                OR run_id IN (SELECT value FROM json_each(${cohortRunIds}))
                OR run_id = ${window.requiredRunId ?? null})
              UNION
              SELECT node_id FROM orchestration_v2_projection_runtime_requests
              WHERE thread_id = ${threadId} AND status IN ('pending','waiting')
              UNION
              SELECT parent_node_id FROM orchestration_v2_projection_subagents
              WHERE thread_id = ${threadId} AND status IN ('pending','starting','running','waiting')
              UNION
              SELECT node_id FROM orchestration_v2_projection_provider_turns
              WHERE thread_id = ${threadId} AND status IN ('starting','running','waiting')
              UNION
              SELECT parent.parent_node_id FROM orchestration_v2_projection_nodes AS parent
              INNER JOIN retained ON parent.node_id = retained.node_id
              WHERE parent.thread_id = ${threadId} AND parent.parent_node_id IS NOT NULL
            )
            -- Sort IDs before fetching bodies, so large payloads never enter
            -- SQLite's temporary sort. The node read restores this order by key.
            SELECT retained.node_id
            FROM retained
            LEFT JOIN orchestration_v2_projection_nodes AS node
              ON node.node_id = retained.node_id AND node.thread_id = ${threadId}
            WHERE retained.node_id IS NOT NULL
            ORDER BY COALESCE(node.started_at, ''), retained.node_id ASC
          `).map((row) => row.node_id),
              );
        const cohortProviderThreadIds = cohortJson("providerThreadId");
        const cohortProviderTurnIds = cohortJson("providerTurnId");
        const cohortMessageIds = cohortJson("messageId");
        const cohortPlanIds = cohortJson("planId");
        const cohortCheckpointIds = cohortJson("checkpointId");
        const cohortHandoffIds = cohortJson("contextHandoffId");
        // A newer completion must keep an older active list from resurfacing after paging.
        const [latestTodoList] =
          window === undefined ||
          (fields !== undefined &&
            !fields.some(
              (field) => field === "plans" || field === "runs" || field === "providerThreads",
            ))
            ? []
            : yield* sql<{
                readonly plan_id: string;
                readonly run_id: string;
                readonly provider_thread_id: string | null;
                readonly last_plan_id: string;
              }>`
                WITH latest_owner AS MATERIALIZED (
                  SELECT plan.plan_id, owner.run_id, owner.provider_thread_id
                  FROM orchestration_v2_projection_plans AS plan
                  INNER JOIN orchestration_v2_projection_runs AS owner
                    ON owner.thread_id = plan.thread_id AND owner.run_id = plan.run_id
                  WHERE plan.thread_id = ${threadId}
                    AND plan.kind = 'todo_list' AND plan.status <> 'superseded'
                  ORDER BY owner.ordinal DESC, plan.plan_id DESC
                  LIMIT 1
                )
                SELECT COALESCE(CASE WHEN EXISTS (
                  SELECT 1 FROM orchestration_v2_projection_plans
                  WHERE thread_id = ${threadId} AND run_id = owner.run_id
                    AND kind = 'todo_list' AND status <> 'superseded'
                    AND plan_id <> owner.plan_id
                ) THEN (
                  -- Plan UUIDs cannot order replacements within a run.
                  SELECT plan.plan_id
                  FROM orchestration_v2_projection_turn_items AS item
                  CROSS JOIN orchestration_v2_projection_plans AS plan
                    ON plan.plan_id = json_extract(item.payload_json, '$.planId')
                  WHERE item.run_id = owner.run_id AND item.thread_id = ${threadId}
                    AND item.type = 'todo_list'
                    AND plan.run_id = owner.run_id AND plan.thread_id = ${threadId}
                    AND plan.kind = 'todo_list' AND plan.status <> 'superseded'
                  ORDER BY item.ordinal DESC
                  LIMIT 1
                ) END, owner.plan_id) AS plan_id, owner.run_id, owner.provider_thread_id,
                (
                  SELECT MAX(plan_id) FROM orchestration_v2_projection_plans
                  WHERE thread_id = ${threadId} AND run_id = owner.run_id
                ) AS last_plan_id
                FROM latest_owner AS owner
              `;

        const [
          thread,
          runRows,
          attemptRows,
          nodeRows,
          subagentRows,
          providerSessionRows,
          providerThreadRows,
          providerTurnRows,
          runtimeRequestRows,
          messageRows,
          planRows,
          checkpointScopeRows,
          checkpointRows,
          contextHandoffRows,
          contextTransferRows,
        ] = yield* Effect.all([
          decodeThreadPayload(threadRow.payload_json),
          (fields !== undefined && !fields.includes("runs")) ||
          (window === undefined &&
            (matchesNothing(filter?.runIds) ||
              (matchesNothing(filter?.runStatuses) && filter?.includeLatestRun !== true)))
            ? Effect.succeed([])
            : window === undefined
              ? sql<PayloadRow>`
            SELECT payload_json
            FROM orchestration_v2_projection_runs
            WHERE thread_id = ${threadId}
              ${filter?.runIds === undefined ? sql`` : sql`AND run_id IN (SELECT value FROM json_each(${encodeIdList(filter.runIds)}))`}
              ${
                filter?.runStatuses === undefined
                  ? sql``
                  : sql`AND (status IN (SELECT value FROM json_each(${encodeIdList(filter.runStatuses)}))
                    OR (${filter.includeLatestRun === true ? 1 : 0} = 1 AND run_id = (
                      SELECT run_id FROM orchestration_v2_projection_runs
                      WHERE thread_id = ${threadId}
                        AND (status <> 'cancelled' OR json_extract(payload_json, '$.startedAt') IS NOT NULL)
                      ORDER BY ordinal DESC
                      LIMIT 1
                    )))`
              }
            ORDER BY ordinal ASC
          `
              : sql<PayloadRow>`
            SELECT payload_json FROM orchestration_v2_projection_runs
            WHERE thread_id = ${threadId}
              AND (status IN ('queued','preparing','starting','running','waiting')
                OR run_id IN (SELECT value FROM json_each(${cohortRunIds}))
                OR run_id = ${window.requiredRunId ?? null}
                -- Queued or cancelled turns must not hide the latest actual session.
                OR run_id = ${latestTodoList?.run_id ?? null}
                OR run_id = (
                  SELECT run_id FROM orchestration_v2_projection_runs
                  WHERE thread_id = ${threadId}
                    AND provider_thread_id IS NOT NULL
                    AND json_extract(payload_json, '$.startedAt') IS NOT NULL
                    AND ${latestTodoList?.plan_id ?? null} IS NOT NULL
                  ORDER BY ordinal DESC
                  LIMIT 1
                ))
            ORDER BY ordinal ASC
          `,
          fields !== undefined && !fields.includes("attempts")
            ? Effect.succeed([])
            : window === undefined
              ? sql<PayloadRow>`
            SELECT payload_json
            FROM orchestration_v2_projection_run_attempts
            WHERE thread_id = ${threadId}
            ORDER BY run_id ASC, attempt_ordinal ASC
          `
              : sql<PayloadRow>`
            SELECT payload_json FROM orchestration_v2_projection_run_attempts
            WHERE thread_id = ${threadId}
              AND (status IN ('pending','starting','running','waiting')
                OR run_id IN (SELECT value FROM json_each(${cohortRunIds})))
            ORDER BY run_id ASC, attempt_ordinal ASC
          `,
          (fields !== undefined && !fields.includes("nodes")) ||
          (window === undefined && matchesNothing(filter?.nodeIds))
            ? Effect.succeed([])
            : window === undefined
              ? sql<PayloadRow>`
            SELECT payload_json
            FROM orchestration_v2_projection_nodes AS node
            WHERE node.thread_id = ${threadId}
              ${filter?.nodeIds === undefined ? sql`` : sql`AND node.node_id IN (SELECT value FROM json_each(${encodeIdList(filter.nodeIds)}))`}
              ${
                filter?.omitSettledLeafNodes === true
                  ? sql`AND NOT (
                      node.kind IN ('tool_call', 'assistant_message', 'reasoning')
                      AND node.status IN ('completed', 'interrupted', 'failed', 'cancelled', 'rolled_back')
                      AND node.run_id IS NOT NULL
                      AND NOT EXISTS (
                        SELECT 1 FROM orchestration_v2_projection_runs AS run
                        WHERE run.run_id = node.run_id
                          AND run.status IN ('preparing', 'queued', 'starting', 'running', 'waiting')
                      )
                    )`
                  : sql``
              }
            ORDER BY COALESCE(node.started_at, ''), node.node_id ASC
              `
              : sql<PayloadRow & { readonly position: number }>`
            -- Keep retained IDs outermost so SQLite looks up each node by its
            -- primary key instead of scanning every historical node in the thread.
            SELECT node.payload_json, retained.key AS position
            FROM json_each(${cohortNodeIds}) AS retained
            CROSS JOIN orchestration_v2_projection_nodes AS node
              ON node.node_id = retained.value
            WHERE node.thread_id = ${threadId}
          `.pipe(
                  Effect.map((rows) =>
                    rows.toSorted((left, right) => left.position - right.position),
                  ),
                ),
          (fields !== undefined && !fields.includes("subagents")) ||
          (window === undefined &&
            (matchesNothing(filter?.subagentIds) || matchesNothing(filter?.subagentRunIds)))
            ? Effect.succeed([])
            : window === undefined
              ? sql<PayloadRow>`
            SELECT payload_json
            FROM orchestration_v2_projection_subagents AS subagent
            WHERE thread_id = ${threadId}
              ${filter?.subagentIds === undefined ? sql`` : sql`AND subagent_id IN (SELECT value FROM json_each(${encodeIdList(filter.subagentIds)}))`}
              ${filter?.subagentRunIds === undefined ? sql`` : sql`AND run_id IN (SELECT value FROM json_each(${encodeIdList(filter.subagentRunIds)}))`}
              ${
                filter?.omitSettledSubagents !== undefined
                  ? sql`AND (${
                      filter.omitSettledSubagents === true
                        ? sql`0`
                        : sql`subagent_id IN (SELECT value FROM json_each(${encodeIdList(filter.omitSettledSubagents.except)}))`
                    } OR NOT (
                      subagent.status IN ('completed', 'failed', 'cancelled', 'interrupted')
                      AND COALESCE(subagent.completion_delivery_state, '')
                        IN ('acknowledged', 'delivered', 'disposed')
                      AND NOT EXISTS (
                        SELECT 1 FROM orchestration_v2_projection_runs AS run
                        WHERE run.run_id = subagent.run_id
                          AND run.status IN ('preparing', 'queued', 'starting', 'running', 'waiting')
                      )
                    ))`
                  : sql``
              }
            ORDER BY COALESCE(started_at, ''), subagent_id ASC
          `
              : sql<PayloadRow>`
            SELECT payload_json FROM orchestration_v2_projection_subagents
            WHERE thread_id = ${threadId}
              AND (status IN ('pending','starting','running','waiting')
                OR run_id IN (SELECT value FROM json_each(${cohortRunIds}))
                OR parent_node_id IN (SELECT value FROM json_each(${cohortNodeIds})))
            ORDER BY COALESCE(started_at, ''), subagent_id ASC
          `,
          fields !== undefined && !fields.includes("providerSessions")
            ? Effect.succeed([])
            : window === undefined
              ? sql<PayloadRow>`
            SELECT sessions.payload_json
            FROM orchestration_v2_projection_provider_sessions AS sessions
            INNER JOIN orchestration_v2_projection_provider_session_bindings AS bindings
              ON bindings.provider_session_id = sessions.provider_session_id
            WHERE bindings.thread_id = ${threadId}
            ORDER BY sessions.updated_at ASC, sessions.provider_session_id ASC
          `
              : sql<PayloadRow>`
            SELECT DISTINCT sessions.payload_json
            FROM orchestration_v2_projection_provider_sessions AS sessions
            INNER JOIN orchestration_v2_projection_provider_session_bindings AS bindings
              ON bindings.provider_session_id = sessions.provider_session_id
            LEFT JOIN orchestration_v2_projection_provider_threads AS threads
              ON threads.provider_session_id = sessions.provider_session_id
            WHERE bindings.thread_id = ${threadId}
              AND (sessions.status IN ('starting','running','waiting')
                OR threads.provider_thread_id IN (SELECT value FROM json_each(${cohortProviderThreadIds})))
            ORDER BY sessions.updated_at ASC, sessions.provider_session_id ASC
          `,
          (fields !== undefined && !fields.includes("providerThreads")) ||
          (window === undefined && matchesNothing(filter?.providerThreadIds))
            ? Effect.succeed([])
            : window === undefined
              ? sql<PayloadRow>`
            SELECT payload_json
            FROM orchestration_v2_projection_provider_threads
            WHERE (thread_id = ${threadId}
               OR owner_node_id IN (${nodesOwningProviderThreads(threadId)})
               OR provider_thread_id IN (
                 SELECT provider_thread_id
                 FROM orchestration_v2_projection_subagents
                 WHERE thread_id = ${threadId}
                   AND provider_thread_id IS NOT NULL
               ))
              ${filter?.providerThreadIds === undefined ? sql`` : sql`AND provider_thread_id IN (SELECT value FROM json_each(${encodeIdList(filter.providerThreadIds)}))`}
            ORDER BY COALESCE(first_run_ordinal, 0), provider_thread_id ASC
          `
              : sql<PayloadRow>`
            SELECT payload_json FROM orchestration_v2_projection_provider_threads
            WHERE (thread_id = ${threadId} AND status = 'active')
              OR provider_thread_id IN (SELECT value FROM json_each(${cohortProviderThreadIds}))
              OR owner_node_id IN (SELECT value FROM json_each(${cohortNodeIds}))
              OR (thread_id = ${threadId}
                AND provider_thread_id = ${latestTodoList?.provider_thread_id ?? null})
            ORDER BY COALESCE(first_run_ordinal, 0), provider_thread_id ASC
          `,
          (fields !== undefined && !fields.includes("providerTurns")) ||
          (window === undefined &&
            matchesNothing(filter?.providerTurnAttemptIds, filter?.providerTurnThreadIds))
            ? Effect.succeed([])
            : window === undefined
              ? sql<PayloadRow>`
            SELECT payload_json
            FROM orchestration_v2_projection_provider_turns
            WHERE thread_id = ${threadId}
              ${filter?.providerTurnAttemptIds === undefined ? sql`` : sql`AND run_attempt_id IN (SELECT value FROM json_each(${encodeIdList(filter.providerTurnAttemptIds)}))`}
              ${filter?.providerTurnThreadIds === undefined ? sql`` : sql`AND provider_thread_id IN (SELECT value FROM json_each(${encodeIdList(filter.providerTurnThreadIds)}))`}
              ${
                filter?.latestProviderTurnUsageOnly === true
                  ? sql`AND json_extract(payload_json, '$.tokenUsage') IS NOT NULL
            ORDER BY julianday(json_extract(payload_json, '$.tokenUsage.updatedAt')) DESC,
              provider_thread_id ASC, ordinal ASC
            LIMIT 1`
                  : sql`ORDER BY provider_thread_id ASC, ordinal ASC`
              }
          `
              : sql<PayloadRow>`
            SELECT payload_json FROM orchestration_v2_projection_provider_turns
            WHERE thread_id = ${threadId}
              AND (status IN ('starting','running','waiting')
                OR provider_turn_id IN (SELECT value FROM json_each(${cohortProviderTurnIds}))
                OR node_id IN (SELECT value FROM json_each(${cohortNodeIds})))
            ORDER BY provider_thread_id ASC, ordinal ASC
          `,
          (fields !== undefined && !fields.includes("runtimeRequests")) ||
          (window === undefined && matchesNothing(filter?.runtimeRequestStatuses))
            ? Effect.succeed([])
            : window === undefined
              ? sql<PayloadRow>`
            SELECT payload_json
            FROM orchestration_v2_projection_runtime_requests
            WHERE thread_id = ${threadId}
              ${filter?.runtimeRequestStatuses === undefined ? sql`` : sql`AND status IN (SELECT value FROM json_each(${encodeIdList(filter.runtimeRequestStatuses)}))`}
            ORDER BY created_at ASC, runtime_request_id ASC
          `
              : sql<PayloadRow>`
            SELECT payload_json FROM orchestration_v2_projection_runtime_requests
            WHERE thread_id = ${threadId}
              AND (status IN ('pending','waiting')
                OR node_id IN (SELECT value FROM json_each(${cohortNodeIds}))
                OR provider_turn_id IN (SELECT value FROM json_each(${cohortProviderTurnIds})))
            ORDER BY created_at ASC, runtime_request_id ASC
          `,
          (fields !== undefined && !fields.includes("messages")) ||
          (window === undefined &&
            matchesNothing(filter?.messageIds, filter?.messageRoles, filter?.messageRunIds))
            ? Effect.succeed([])
            : window === undefined
              ? sql<PayloadRow>`
                SELECT payload_json FROM orchestration_v2_projection_messages
                WHERE thread_id = ${threadId}
                  ${filter?.messageIds === undefined ? sql`` : sql`AND message_id IN (SELECT value FROM json_each(${encodeIdList(filter.messageIds)}))`}
                  ${filter?.messageRoles === undefined ? sql`` : sql`AND role IN (SELECT value FROM json_each(${encodeIdList(filter.messageRoles)}))`}
                  ${filter?.monitorNotificationsOnly === true ? sql`AND json_extract(payload_json, '$.notification.source.kind') = 'monitor'` : sql``}
                  ${filter?.messageRunIds === undefined ? sql`` : sql`AND run_id IN (SELECT value FROM json_each(${encodeIdList(filter.messageRunIds)}))`}
                ORDER BY created_at ASC, message_id ASC
              `
              : sql<PayloadRow>`
                SELECT payload_json FROM orchestration_v2_projection_messages AS message
                WHERE message.thread_id = ${threadId}
                  AND (
                    message.message_id IN (
                      SELECT value FROM json_each(${cohortMessageIds})
                    )
                    OR message.run_id IN (
                      SELECT run_id FROM orchestration_v2_projection_runs
                      WHERE thread_id = ${threadId}
                        AND status IN ('queued', 'preparing', 'starting', 'running', 'waiting')
                    )
                  )
                ORDER BY created_at ASC, message_id ASC
              `,
          fields !== undefined && !fields.includes("plans")
            ? Effect.succeed([])
            : window === undefined
              ? sql<PayloadRow>`
                SELECT payload_json FROM orchestration_v2_projection_plans
                WHERE thread_id = ${threadId} ORDER BY plan_id ASC
              `
              : sql<PayloadRow>`
                SELECT payload_json FROM orchestration_v2_projection_plans AS plan
                WHERE plan.thread_id = ${threadId}
                  AND (
                    plan.status = 'active'
                    OR plan.plan_id = ${latestTodoList?.plan_id ?? null}
                    OR plan.plan_id IN (
                      SELECT value FROM json_each(${cohortPlanIds})
                    )
                  )
                -- Keep the selected artifact last within its run for equal-run consumers.
                ORDER BY CASE WHEN plan_id = ${latestTodoList?.plan_id ?? null}
                  THEN ${latestTodoList?.last_plan_id ?? null} ELSE plan_id END ASC,
                  plan_id = ${latestTodoList?.plan_id ?? null} ASC
              `,
          fields !== undefined && !fields.includes("checkpointScopes")
            ? Effect.succeed([])
            : window === undefined
              ? sql<PayloadRow>`
            SELECT payload_json
            FROM orchestration_v2_projection_checkpoint_scopes
            WHERE thread_id = ${threadId}
            ORDER BY ordinal_within_parent ASC, scope_id ASC
          `
              : sql<PayloadRow>`
            SELECT payload_json FROM orchestration_v2_projection_checkpoint_scopes
            WHERE thread_id = ${threadId}
              AND (run_id IN (SELECT value FROM json_each(${cohortRunIds}))
                OR node_id IN (SELECT value FROM json_each(${cohortNodeIds})))
            ORDER BY ordinal_within_parent ASC, scope_id ASC
          `,
          fields !== undefined && !fields.includes("checkpoints")
            ? Effect.succeed([])
            : window === undefined
              ? sql<PayloadRow>`
            SELECT payload_json
            FROM orchestration_v2_projection_checkpoints
            WHERE thread_id = ${threadId}
            ORDER BY scope_id ASC, ordinal_within_scope ASC
          `
              : sql<PayloadRow>`
            SELECT payload_json FROM orchestration_v2_projection_checkpoints
            WHERE thread_id = ${threadId}
              AND (status IN ('pending','capturing')
                OR (run_id IS NULL AND ordinal_within_scope = 0 AND status = 'ready')
                OR checkpoint_id IN (SELECT value FROM json_each(${cohortCheckpointIds}))
                OR run_id IN (SELECT value FROM json_each(${cohortRunIds}))
                OR node_id IN (SELECT value FROM json_each(${cohortNodeIds})))
            ORDER BY scope_id ASC, ordinal_within_scope ASC
          `,
          fields !== undefined && !fields.includes("contextHandoffs")
            ? Effect.succeed([])
            : window === undefined
              ? sql<PayloadRow>`
                SELECT payload_json FROM orchestration_v2_projection_context_handoffs
                WHERE thread_id = ${threadId} ORDER BY rowid ASC
              `
              : sql<PayloadRow>`
                SELECT payload_json FROM orchestration_v2_projection_context_handoffs AS handoff
                WHERE handoff.thread_id = ${threadId}
                  AND (
                    handoff.status IN ('pending', 'ready')
                    OR handoff.context_handoff_id IN (
                      SELECT value FROM json_each(${cohortHandoffIds})
                    )
                  )
                ORDER BY rowid ASC
              `,
          (fields !== undefined && !fields.includes("contextTransfers")) ||
          (window === undefined && matchesNothing(filter?.contextTransferStatuses))
            ? Effect.succeed([])
            : window === undefined
              ? sql<PayloadRow>`
            SELECT payload_json
            FROM orchestration_v2_projection_context_transfers
            WHERE (source_thread_id = ${threadId} OR target_thread_id = ${threadId})
              ${filter?.contextTransferStatuses === undefined ? sql`` : sql`AND status IN (SELECT value FROM json_each(${encodeIdList(filter.contextTransferStatuses)}))`}
            ORDER BY rowid ASC
          `
              : sql<PayloadRow>`
            SELECT payload_json FROM orchestration_v2_projection_context_transfers
            WHERE (source_thread_id = ${threadId} OR target_thread_id = ${threadId})
              AND (status IN ('pending','running','waiting')
                OR target_run_id IN (SELECT value FROM json_each(${cohortRunIds}))
                OR json_extract(payload_json, '$.contextHandoffId') IN
                  (SELECT value FROM json_each(${cohortHandoffIds})))
            ORDER BY rowid ASC
          `,
        ]);

        const [
          runs,
          attempts,
          nodes,
          subagents,
          providerSessions,
          providerThreads,
          providerTurns,
          runtimeRequests,
          messages,
          plans,
          checkpointScopes,
          checkpoints,
          contextHandoffs,
          contextTransfers,
        ] = yield* Effect.all([
          decodeRows(decodeRunPayload, threadId)(runRows),
          decodeRows(decodeRunAttemptPayload, threadId)(attemptRows),
          decodeRows(decodeNodePayload, threadId)(nodeRows),
          decodeRows(decodeSubagentPayload, threadId)(subagentRows),
          decodeRows(decodeProviderSessionPayload, threadId)(providerSessionRows),
          decodeRows(decodeProviderThreadPayload, threadId)(providerThreadRows),
          decodeRows(decodeProviderTurnPayload, threadId)(providerTurnRows),
          decodeRows(decodeRuntimeRequestPayload, threadId)(runtimeRequestRows),
          decodeRows(decodeMessagePayload, threadId)(messageRows),
          decodeRows(decodePlanJson, threadId)(planRows),
          decodeRows(decodeCheckpointScopePayload, threadId)(checkpointScopeRows),
          decodeRows(decodeCheckpointPayload, threadId)(checkpointRows),
          decodeRows(decodeContextHandoffPayload, threadId)(contextHandoffRows),
          decodeRows(decodeContextTransferPayload, threadId)(contextTransferRows),
        ]);
        const messageOrdinals = new Map<string, number>();
        if (fields !== undefined && fields.includes("messages") && messages.length > 1) {
          // Broad role reads keep the original query to avoid per-ID lookup overhead.
          const loadedMessageIds =
            filter?.messageIds === undefined &&
            filter?.messageRunIds === undefined &&
            (filter?.messageRoles === undefined ||
              (filter.messageRoles.includes("user") && filter.messageRoles.includes("assistant")))
              ? undefined
              : messages.map((message) => message.id);
          const rows = yield* sql<{ message_id: string; ordinal: number }>`
            SELECT json_extract(payload_json, '$.messageId') AS message_id, MIN(ordinal) AS ordinal
            FROM orchestration_v2_projection_turn_items
            WHERE thread_id = ${threadId} AND type IN ('user_message', 'assistant_message')
              ${loadedMessageIds === undefined ? sql`` : sql`AND json_extract(payload_json, '$.messageId') IN (SELECT value FROM json_each(${encodeIdList(loadedMessageIds)}))`}
            GROUP BY json_extract(payload_json, '$.messageId')
          `;
          for (const row of rows) messageOrdinals.set(row.message_id, row.ordinal);
        }
        const orderedMessages = sortMessagesByTurnItemOrder(messages, turnItems, messageOrdinals);
        const projection = {
          thread,
          runs,
          attempts,
          nodes,
          subagents,
          providerSessions,
          providerThreads,
          providerTurns,
          runtimeRequests,
          messages: orderedMessages,
          plans,
          turnItems,
          checkpointScopes,
          checkpoints,
          contextHandoffs,
          contextTransfers,
          visibleTurnItems: [],
          updatedAt: thread.updatedAt,
        } satisfies OrchestrationV2ThreadProjection;
        return projection;
      },
      (effect, threadId, _window?, _fields?, _filter?) =>
        effect.pipe(
          Effect.mapError((cause) =>
            isProjectionStoreThreadNotFoundError(cause)
              ? cause
              : new ProjectionStoreReadError({
                  threadId,
                  cause,
                }),
          ),
        ),
    );

    const readProjection = makeProjectionReader(readCanonicalProjection, {
      forkBoundary: (threadId, runId, anchorItemId) =>
        sql<{ readonly turn_item_id: string }>`
                  WITH fork_run AS (
                    SELECT ordinal
                    FROM orchestration_v2_projection_runs
                    WHERE thread_id = ${threadId}
                      AND run_id = ${runId}
                    LIMIT 1
                  ), fork_boundary AS (
                    SELECT (
                      SELECT item.ordinal
                      FROM orchestration_v2_projection_turn_items AS item
                      WHERE item.run_id = run.run_id
                      ORDER BY item.ordinal DESC
                      LIMIT 1
                    ) AS ordinal
                    FROM orchestration_v2_projection_runs AS run
                    WHERE run.thread_id = ${threadId}
                      AND run.ordinal <= (SELECT ordinal FROM fork_run)
                      AND EXISTS (
                        SELECT 1
                        FROM orchestration_v2_projection_turn_items AS item
                        WHERE item.run_id = run.run_id
                        LIMIT 1
                      )
                    ORDER BY run.ordinal DESC
                    LIMIT 1
                  ), effective_boundary AS (
                    SELECT COALESCE(
                      (SELECT ordinal FROM fork_boundary),
                      (
                        SELECT ordinal
                        FROM orchestration_v2_projection_turn_items
                        WHERE thread_id = ${threadId}
                          AND run_id IS NULL
                          AND json_extract(payload_json, '$.historyOrigin') = 'v1_import'
                        ORDER BY ordinal DESC, turn_item_id DESC
                        LIMIT 1
                      ),
                      -1
                    ) AS ordinal
                  )
                  SELECT turn_item_id
                  FROM orchestration_v2_projection_turn_items
                  WHERE thread_id = ${threadId}
                    AND ordinal <= COALESCE(
                      (SELECT ordinal FROM effective_boundary),
                      -1
                    )
                    AND (
                      ${anchorItemId === undefined ? 0 : 1} = 0
                      OR ordinal <= COALESCE(
                        (SELECT ordinal FROM orchestration_v2_projection_turn_items
                         WHERE thread_id = ${threadId}
                           AND turn_item_id = ${anchorItemId ?? null}
                         LIMIT 1),
                        (SELECT ordinal FROM effective_boundary),
                        -1
                      )
                    )
                  ORDER BY ordinal DESC, turn_item_id DESC
                  LIMIT 1
                `.pipe(
          Effect.map((rows) => rows[0]?.turn_item_id ?? null),
          Effect.mapError((cause) => new ProjectionRecordRowsError({ cause })),
        ),
      inheritedCount: Effect.fnUntraced(
        function* (threadId, runId, window) {
          const counted = yield* sql<{ readonly inherited_rows: number | null }>`
            ${yield* retainedWindowRows(threadId, window)}
            SELECT CASE
              WHEN json_extract(source.payload_json, '$.forkedFrom.type') = 'run' THEN NULL
              WHEN NOT EXISTS (
                SELECT 1 FROM orchestration_v2_projection_runs
                WHERE thread_id = source.thread_id AND run_id = ${runId}
              ) THEN 0
              ELSE (SELECT COUNT(*) FROM retained)
            END AS inherited_rows
            FROM orchestration_v2_projection_threads AS source
            WHERE source.thread_id = ${threadId}
          `;
          return counted[0]?.inherited_rows ?? null;
        },
        Effect.mapError((cause) => new ProjectionRecordRowsError({ cause })),
      ),
    });

    const {
      getLimitRecoveryCandidates,
      getLimitRecoveryWakeAt,
      getRecoveryThreadIds,
      getUnreadableThreadIds,
    } = makeProjectionRecovery(yield* SqliteProjectionRecoveryRows.make);

    // SQLite trim defaults to ASCII spaces; compact recognition uses JavaScript trim.
    const javascriptTrimWhitespace =
      " \t\n\r\v\f\u00a0\u1680\u2000\u2001\u2002\u2003\u2004\u2005\u2006\u2007\u2008\u2009\u200a\u2028\u2029\u202f\u205f\u3000\ufeff";

    const {
      getTurnStartContext,
      getRuntimeRecoveryProjection,
      getCheckpointContext,
      getCheckpointCaptureContext,
      getCheckpointRollbackContext,
      getThreadAttachmentIds,
    } = contexts;

    const { getTurnStartHistory } = lookups;

    const { getThreadProjection, getThreadSnapshot, getThreadSnapshotWindow } =
      makeProjectionSnapshots(readProjection, {
        withReadTransaction: (effect) => sql.withTransaction(effect),
        anchorThread: (itemId) =>
          sql<{ readonly thread_id: string }>`SELECT thread_id
        FROM orchestration_v2_projection_turn_items WHERE turn_item_id = ${itemId} LIMIT 1`.pipe(
            Effect.map((rows) => (rows[0] === undefined ? null : ThreadId.make(rows[0].thread_id))),
            Effect.mapError((cause) => new ProjectionRecordRowsError({ cause })),
          ),
        pendingCards: (threadId) =>
          sql<PayloadRow>`SELECT item.payload_json
        FROM orchestration_v2_projection_runtime_requests AS request
        CROSS JOIN orchestration_v2_projection_turn_items AS item
          ON item.node_id = request.node_id
          AND item.type IN ('approval_request', 'user_input_request')
          AND json_extract(item.payload_json, '$.requestId') = request.runtime_request_id
        WHERE request.thread_id = ${threadId} AND request.status = 'pending' AND item.thread_id = ${threadId}`.pipe(
            Effect.map((rows) => rows.map((row) => row.payload_json)),
            Effect.mapError((cause) => new ProjectionRecordRowsError({ cause })),
          ),
        snapshotSequence: (threadId) =>
          sql<{
            readonly snapshot_sequence: number | null;
          }>`SELECT MAX(sequence) AS snapshot_sequence
        FROM orchestration_events WHERE application_event_version = 2 AND aggregate_kind = 'thread' AND stream_id = ${threadId}`.pipe(
            Effect.map((rows) => rows[0]?.snapshot_sequence ?? 0),
            Effect.mapError((cause) => new ProjectionRecordRowsError({ cause })),
          ),
      });

    const { getThread, requireThread } = lookups;
    const controlReadError = (threadId: ThreadId) => (cause: unknown) =>
      isProjectionStoreThreadNotFoundError(cause)
        ? cause
        : new ProjectionStoreReadError({ threadId, cause });

    const {
      getPendingNativeUserInputs,
      getProviderControlContext,
      getRunningTurnContext,
      getThreadProviderContext,
      getRuntimeResponseContext,
    } = controls;
    const { getPlan, hasUnpairedRunInterruptRequest, isNodeStored, getRuntimeRequest } = lookups;

    const {
      getRunMessage,
      canStartQueuedRun,
      getHeldQueuedRuns,
      getMessageCount,
      getSubagentCount,
      listAppOwnedChildThreadIds,
      getNextTurnItemOrdinal,
    } = lookups;

    const { getTurnItem } = lookups;

    const { getMessagesById } = lookups;

    const getThreadRecords = makeProjectionRecords(yield* SqliteProjectionRecordRows.make);

    const readTimelineIndex = makeTimelineIndex(
      getThreadRecords,
      (threadId, includePersistentOutput) =>
        sql<ProjectionTimelineRow>`SELECT item.turn_item_id, item.run_id, item.node_id, item.parent_item_id, item.type, item.status, item.ordinal,
          ${includePersistentOutput ? sql`attempt.attempt_id` : sql`NULL`} AS attempt_id,
          ${includePersistentOutput ? sql`attempt.status` : sql`NULL`} AS attempt_status,
          ${
            includePersistentOutput ? sql`CASE WHEN ${inlineWorkOutput} THEN 1 ELSE 0 END` : sql`0`
          } AS persistent_output,
          CASE WHEN item.type = 'user_message' THEN json_extract(item.payload_json, '$.inputIntent') END AS input_intent,
          CASE WHEN item.type = 'user_message' THEN json_extract(item.payload_json, '$.createdBy') END AS created_by,
          CASE WHEN item.type IN ('user_message', 'assistant_message') THEN json_extract(item.payload_json, '$.messageId') END AS message_id
        FROM orchestration_v2_projection_turn_items AS item
        ${
          includePersistentOutput
            ? sql`LEFT JOIN orchestration_v2_projection_nodes AS node ON node.node_id = item.node_id
        LEFT JOIN orchestration_v2_projection_run_attempts AS attempt ON attempt.attempt_id = (
          SELECT attempt_id FROM orchestration_v2_projection_run_attempts
          WHERE run_id = item.run_id AND root_node_id = COALESCE(node.root_node_id, item.node_id)
          ORDER BY attempt_ordinal DESC LIMIT 1
        )`
            : sql``
        }
        WHERE item.thread_id = ${threadId}
        ORDER BY item.ordinal ASC, item.turn_item_id ASC`.pipe(
          Effect.mapError((cause) => new ProjectionRecordRowsError({ cause })),
        ),
    );

    const { searchThread, searchThreadStream } = yield* makeThreadFind((threadId) =>
      readTimelineIndex(threadId, new Set()).pipe(Effect.map((index) => index.visible)),
    );

    const { getThreadHistoryPage, getTimelinePage } = makeProjectionTimelinePages(
      readTimelineIndex,
      {
        withReadTransaction: (effect) => sql.withTransaction(effect),
        snapshotSequence: (threadId) =>
          sql<{ readonly sequence: number }>`SELECT COALESCE(MAX(sequence), 0) AS sequence
        FROM orchestration_events WHERE application_event_version = 2 AND aggregate_kind = 'thread' AND stream_id = ${threadId}`.pipe(
            Effect.map((rows) => rows[0]?.sequence ?? 0),
            Effect.mapError((cause) => new ProjectionRecordRowsError({ cause })),
          ),
        materialize: (sources) =>
          sql<ProjectionMaterializedRow>`
        SELECT item.thread_id, item.turn_item_id, item.payload_json
        FROM orchestration_v2_projection_turn_items item JOIN json_each(${sources}) wanted
        ON item.thread_id = json_extract(wanted.value, '$.threadId')
          AND item.turn_item_id = json_extract(wanted.value, '$.id')`.pipe(
            Effect.mapError((cause) => new ProjectionRecordRowsError({ cause })),
          ),
        attachmentOwners: (threadIds, attachmentId) =>
          sql<ProjectionAttachmentOwner>`
          SELECT item.thread_id, item.turn_item_id
          FROM orchestration_v2_projection_turn_items AS item,
            json_each(item.payload_json, '$.attachments') AS attachment
          WHERE item.thread_id IN ${sql.in(threadIds)}
            AND item.type = 'user_message'
            AND json_extract(attachment.value, '$.id') = ${attachmentId}`.pipe(
            Effect.mapError((cause) => new ProjectionRecordRowsError({ cause })),
          ),
      },
    );

    const {
      getSettlementCandidates,
      getOpenRemoteDelegatedTasks,
      getThreadsWithPullRequests,
      getThreadWorktreePaths,
      readShellSnapshot,
      getPullRequestSnapshot,
      getAgentAwarenessSnapshot,
      getShellSnapshot,
      listActiveSubagentThreadIds,
      countThreads,
      listActiveShellIndex,
      findGuestShareThreadId,
      listOlderSettledThreads,
      getThreadShell,
    } = shells;
    const { getThreadLinkMetadata, getThreadTitles } = lookups;

    const readOnReader =
      <Args extends ReadonlyArray<unknown>, A, E, R>(
        method: (...args: Args) => Effect.Effect<A, E, R>,
      ) =>
      (...args: Args): Effect.Effect<A, E, R> =>
        method(...args).pipe(NodeSqliteClient.readOnly);

    return {
      apply,
      getAgentAwarenessSnapshot: readOnReader(getAgentAwarenessSnapshot),
      getShellSnapshot: readOnReader(getShellSnapshot),
      getPullRequestSnapshot: readOnReader(getPullRequestSnapshot),
      listActiveSubagentThreadIds: readOnReader(listActiveSubagentThreadIds),
      countThreads: readOnReader(countThreads),
      listActiveShellIndex: readOnReader(listActiveShellIndex),
      readShellSnapshot: readOnReader(readShellSnapshot),
      listOlderSettledThreads: readOnReader(listOlderSettledThreads),
      getThreadShell: readOnReader(getThreadShell),
      findGuestShareThreadId: readOnReader(findGuestShareThreadId),
      getThread: readOnReader(getThread),
      getSettlementCandidates: readOnReader(getSettlementCandidates),
      getThreadsWithPullRequests: readOnReader(getThreadsWithPullRequests),
      getThreadTitles: readOnReader(getThreadTitles),
      getThreadLinkMetadata: readOnReader(getThreadLinkMetadata),
      getThreadWorktreePaths: readOnReader(getThreadWorktreePaths),
      getOpenRemoteDelegatedTasks: getOpenRemoteDelegatedTasks.pipe(NodeSqliteClient.readOnly),
      getThreadProjection: readOnReader(getThreadProjection),
      getTurnStartContext: readOnReader(getTurnStartContext),
      getTurnStartHistory: readOnReader(getTurnStartHistory),
      getRuntimeRecoveryProjection: readOnReader(getRuntimeRecoveryProjection),
      getRunningTurnContext: readOnReader(getRunningTurnContext),
      getThreadProviderContext: readOnReader(getThreadProviderContext),
      getRuntimeResponseContext: readOnReader(getRuntimeResponseContext),
      getCheckpointContext: readOnReader(getCheckpointContext),
      getCheckpointCaptureContext: readOnReader(getCheckpointCaptureContext),
      getCheckpointRollbackContext: readOnReader(getCheckpointRollbackContext),
      getRunMessage: readOnReader(getRunMessage),
      canStartQueuedRun: readOnReader(canStartQueuedRun),
      getHeldQueuedRuns: readOnReader(getHeldQueuedRuns),
      getPendingNativeUserInputs: readOnReader(getPendingNativeUserInputs),
      hasUnpairedRunInterruptRequest: readOnReader(hasUnpairedRunInterruptRequest),
      isNodeStored: readOnReader(isNodeStored),
      getMessageCount: readOnReader(getMessageCount),
      getSubagentCount: readOnReader(getSubagentCount),
      listAppOwnedChildThreadIds: readOnReader(listAppOwnedChildThreadIds),
      getNextTurnItemOrdinal: readOnReader(getNextTurnItemOrdinal),
      getTurnItem: readOnReader(getTurnItem),
      getMessagesById: readOnReader(getMessagesById),
      getThreadRecords: (threadId, fields, filter) =>
        getThreadRecords(threadId, fields, filter).pipe(NodeSqliteClient.readOnly),
      getRuntimeRequest: readOnReader(getRuntimeRequest),
      getPlan: readOnReader(getPlan),
      getProviderControlContext: readOnReader(getProviderControlContext),
      getLimitRecoveryCandidates: readOnReader(getLimitRecoveryCandidates),
      getLimitRecoveryWakeAt: readOnReader(getLimitRecoveryWakeAt),
      getRecoveryThreadIds: readOnReader(getRecoveryThreadIds),
      getUnreadableThreadIds: readOnReader(getUnreadableThreadIds),
      getThreadSnapshot: readOnReader(getThreadSnapshot),
      getThreadSnapshotWindow: readOnReader(getThreadSnapshotWindow),
      getTimelinePage: readOnReader(getTimelinePage),
      getThreadHistoryPage: readOnReader(getThreadHistoryPage),
      searchThread: readOnReader(searchThread),
      searchThreadStream,
      getThreadAttachmentIds: readOnReader(getThreadAttachmentIds),
    } satisfies ProjectionStoreV2Shape;
  }),
);
