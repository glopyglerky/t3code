import {
  threadPullRequestKeysEqual,
  threadPullRequestsOf,
} from "../../sourceControl/threadPullRequests.ts";
import {
  ChatAttachment,
  OrchestrationMessageContext,
  DEFAULT_MODEL,
  EventId,
  MessageId,
  ModelSelection,
  type OrchestrationV2AppThread,
  OrchestrationV2AppThreadJson,
  type OrchestrationV2ConversationMessage,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2TurnItem,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
  ThreadLinkedPullRequest,
  ThreadPullRequestLink,
  TurnItemId,
} from "@t3tools/contracts";
import * as KeyedLock from "@t3tools/shared/KeyedLock";
import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import type * as SqlClient from "effect/sql/SqlClient";
import { makeSqliteRows } from "./LegacyV1SqliteRows.ts";

import * as EventSink from "../EventSink.ts";
import { randomUuidV4 } from "@t3tools/provider-core/server/randomUuid";

const IMPORT_EVENT_PREFIX = "migration:v1";
const TRANSCRIPT_EVENT_BATCH_SIZE = 100;

export interface LegacyThreadRow {
  readonly thread_id: string;
  readonly project_id: string;
  readonly title: string;
  readonly model_selection_json: string | null;
  readonly runtime_mode: string;
  readonly interaction_mode: string;
  readonly branch: string | null;
  readonly worktree_path: string | null;
  readonly created_at: string;
  readonly updated_at: string;
  readonly archived_at: string | null;
  readonly settled_override: string | null;
  readonly settled_at: string | null;
  readonly unsettled_at: string | null;
  readonly snoozed_until: string | null;
  readonly snoozed_at: string | null;
  readonly pinned_at: string | null;
  readonly auto_settle_disabled_at: string | null;
  readonly pin_order_key: string | null;
  readonly pull_requests_json: string;
  readonly linked_pull_request_json: string | null;
  readonly branch_pull_request_json: string | null;
  readonly active_order_key: string | null;
  readonly deleted_at: string | null;
}

export interface LegacyRepairRow extends LegacyThreadRow {
  readonly payload_json: string;
}

export interface LegacyMessageRow {
  readonly message_id: string;
  readonly thread_id: string;
  readonly role: "user" | "assistant";
  readonly text: string;
  readonly attachments_json: string | null;
  readonly context_json?: string | null;
  readonly is_streaming: number;
  readonly created_at: string;
  readonly updated_at: string;
  readonly ordinal: number;
}

export interface LegacyImportRow {
  readonly thread_id: string;
  readonly transcript_imported_at: string | null;
}

export interface LegacyV1Rows<E> {
  readonly messages: (threadId: ThreadId) => Effect.Effect<readonly LegacyMessageRow[], E>;
  readonly latestMessage: (
    threadId: ThreadId,
    userOnly: boolean,
  ) => Effect.Effect<readonly LegacyMessageRow[], E>;
  readonly repairs: Effect.Effect<readonly LegacyRepairRow[], E>;
  readonly pendingThreads: Effect.Effect<readonly LegacyThreadRow[], E>;
  readonly pendingThreadCount: Effect.Effect<number, E>;
  readonly getImport: (threadId: ThreadId) => Effect.Effect<readonly LegacyImportRow[], E>;
  readonly importedEvents: (
    threadId: ThreadId,
  ) => Effect.Effect<readonly { readonly event_id: string }[], E>;
  readonly pendingImports: Effect.Effect<readonly LegacyImportRow[], E>;
  readonly insertPosition: (input: {
    readonly threadId: ThreadId;
    readonly turnItemId: TurnItemId;
    readonly ordinal: number;
  }) => Effect.Effect<unknown, E>;
  readonly insertShell: (input: {
    readonly threadId: ThreadId;
    readonly sourceUpdatedAt: string;
    readonly shellImportedAt: string;
    readonly importedMessageCount: number;
  }) => Effect.Effect<unknown, E>;
  readonly complete: (input: {
    readonly threadId: ThreadId;
    readonly completedAt: string;
    readonly messageCount: number;
  }) => Effect.Effect<unknown, E>;
  readonly failure: (threadId: string) => Effect.Effect<unknown, E>;
  readonly withTransaction: <A, E2, R>(
    body: Effect.Effect<A, E2, R>,
  ) => Effect.Effect<A, E | E2, R>;
}

export interface LegacyV1ImportSummary {
  readonly importedThreadCount: number;
  readonly importedMessageCount: number;
}

export class LegacyV1ThreadImportError extends Schema.TaggedError<LegacyV1ThreadImportError>()(
  "LegacyV1ThreadImportError",
  {
    operation: Schema.String,
    threadId: Schema.optional(ThreadId),
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    return this.threadId === undefined
      ? `Failed to ${this.operation} legacy v1 threads.`
      : `Failed to ${this.operation} legacy v1 thread ${this.threadId}.`;
  }
}

export interface LegacyV1ThreadImporterShape {
  readonly pendingThreadCount: Effect.Effect<number, LegacyV1ThreadImportError>;
  readonly reconcileShells: Effect.Effect<LegacyV1ImportSummary, LegacyV1ThreadImportError>;
  readonly ensureTranscript: (
    threadId: ThreadId,
  ) => Effect.Effect<LegacyV1ImportSummary, LegacyV1ThreadImportError>;
  readonly importPendingTranscripts: Effect.Effect<LegacyV1ImportSummary, never>;
}

export class LegacyV1ThreadImporter extends Context.Service<
  LegacyV1ThreadImporter,
  LegacyV1ThreadImporterShape
>()("t3/orchestration-v2/legacy/LegacyV1ThreadImporter") {}

const decodeModelSelection = Schema.decodeUnknownOption(ModelSelection);
const decodeAttachments = Schema.decodeUnknownOption(Schema.Array(ChatAttachment));
const decodePullRequests = Schema.decodeUnknownOption(Schema.Array(ThreadPullRequestLink));
const decodeLinkedPullRequest = Schema.decodeUnknownOption(ThreadLinkedPullRequest);
const decodeStoredThread = Schema.decodeUnknownOption(
  Schema.fromJsonString(OrchestrationV2AppThreadJson),
);
const decodeMessageContext = Schema.decodeUnknownOption(OrchestrationMessageContext);

function parseJson(json: string): unknown {
  try {
    return JSON.parse(json);
  } catch {
    return undefined;
  }
}

function modelSelectionFor(row: LegacyThreadRow) {
  const decoded =
    row.model_selection_json === null
      ? Option.none()
      : decodeModelSelection(parseJson(row.model_selection_json));
  return Option.getOrElse(decoded, () => ({
    instanceId: ProviderInstanceId.make("codex"),
    model: DEFAULT_MODEL,
  }));
}

function attachmentsFor(row: LegacyMessageRow) {
  if (row.attachments_json === null) return [];
  return Option.getOrElse(decodeAttachments(parseJson(row.attachments_json)), () => []);
}

function contextFor(row: LegacyMessageRow) {
  if (!row.context_json) return undefined;
  return Option.getOrUndefined(decodeMessageContext(parseJson(row.context_json)));
}

function linkedPullRequestFor(row: LegacyThreadRow) {
  if (row.linked_pull_request_json === null) return null;
  return Option.getOrNull(decodeLinkedPullRequest(parseJson(row.linked_pull_request_json)));
}

function branchPullRequestFor(row: LegacyThreadRow) {
  if (row.branch_pull_request_json === null) return null;
  return Option.getOrNull(decodeLinkedPullRequest(parseJson(row.branch_pull_request_json)));
}

function runtimeModeFor(value: string): OrchestrationV2AppThread["runtimeMode"] {
  return value === "approval-required" ||
    value === "auto-accept-edits" ||
    value === "auto" ||
    value === "full-access"
    ? value
    : "full-access";
}

function interactionModeFor(value: string): OrchestrationV2AppThread["interactionMode"] {
  return value === "plan" ? "plan" : "default";
}

function settledOverrideFor(value: string | null): OrchestrationV2AppThread["settledOverride"] {
  return value === "settled" || value === "active" ? value : null;
}

// V1 rows can hold unparseable timestamps; fall back to another time rather than fail the import.
function dateTimeOr(value: string | null, fallback: DateTime.Utc): DateTime.Utc {
  return value === null ? fallback : Option.getOrElse(DateTime.make(value), () => fallback);
}

// A malformed marker stays set so archived or deleted threads keep that state.
function nullableDateTime(value: string | null, fallback: DateTime.Utc): DateTime.Utc | null {
  return value === null ? null : dateTimeOr(value, fallback);
}

function importedThread(row: LegacyThreadRow, importedAt: DateTime.Utc): OrchestrationV2AppThread {
  const createdAt = dateTimeOr(row.created_at, dateTimeOr(row.updated_at, importedAt));
  const updatedAt = dateTimeOr(row.updated_at, createdAt);
  const threadId = ThreadId.make(row.thread_id);
  const modelSelection = modelSelectionFor(row);
  const branch = row.branch?.trim() || null;
  const worktreePath = row.worktree_path?.trim() || null;
  const pullRequests = Option.getOrElse(
    decodePullRequests(parseJson(row.pull_requests_json)),
    () => [],
  );
  const linkedPullRequest = linkedPullRequestFor(row);
  const legacyLink = threadPullRequestsOf({ linkedPullRequest })[0];
  const importedPullRequests =
    legacyLink !== undefined &&
    !pullRequests.some((link) => threadPullRequestKeysEqual(link, legacyLink))
      ? [...pullRequests, legacyLink]
      : pullRequests;
  return {
    createdBy: "system",
    creationSource: "server",
    id: threadId,
    projectId: ProjectId.make(row.project_id),
    title: row.title.trim() === "" ? "Untitled thread" : row.title,
    providerInstanceId: modelSelection.instanceId,
    modelSelection,
    runtimeMode: runtimeModeFor(row.runtime_mode),
    interactionMode: interactionModeFor(row.interaction_mode),
    branch,
    worktreePath,
    linkedPullRequest,
    pullRequests: importedPullRequests,
    branchPullRequest: branchPullRequestFor(row),
    activeOrderKey: row.active_order_key?.trim() || null,
    activeProviderThreadId: null,
    historyOrigin: "v1_import",
    lineage: {
      parentThreadId: null,
      relationshipToParent: null,
      rootThreadId: threadId,
    },
    forkedFrom: null,
    createdAt,
    updatedAt,
    archivedAt: nullableDateTime(row.archived_at, updatedAt),
    settledOverride: settledOverrideFor(row.settled_override),
    settledAt: nullableDateTime(row.settled_at, updatedAt),
    unsettledAt: nullableDateTime(row.unsettled_at, updatedAt),
    snoozedUntil: nullableDateTime(row.snoozed_until, updatedAt),
    snoozedAt: nullableDateTime(row.snoozed_at, updatedAt),
    pinnedAt: nullableDateTime(row.pinned_at, updatedAt),
    autoSettleDisabledAt: nullableDateTime(row.auto_settle_disabled_at, updatedAt),
    pinOrderKey: row.pin_order_key?.trim() || null,
    lastVisitedAt: null,
    deletedAt: nullableDateTime(row.deleted_at, updatedAt),
  };
}

function messageEvents(
  row: LegacyMessageRow,
  importedAt: DateTime.Utc,
): ReadonlyArray<OrchestrationV2DomainEvent> {
  const threadId = ThreadId.make(row.thread_id);
  const messageId = MessageId.make(row.message_id);
  const createdAt = dateTimeOr(row.created_at, dateTimeOr(row.updated_at, importedAt));
  const updatedAt = dateTimeOr(row.updated_at, createdAt);
  const attachments = attachmentsFor(row);
  const context = contextFor(row);
  const message: OrchestrationV2ConversationMessage = {
    createdBy: row.role === "user" ? "user" : "agent",
    creationSource: "server",
    id: messageId,
    threadId,
    runId: null,
    nodeId: null,
    role: row.role,
    text: row.text,
    ...(context !== undefined ? { context } : {}),
    attachments,
    streaming: false,
    createdAt,
    updatedAt,
  };
  const baseTurnItem = {
    id: TurnItemId.make(`${IMPORT_EVENT_PREFIX}:turn-item:${row.message_id}`),
    threadId,
    runId: null,
    nodeId: null,
    providerThreadId: null,
    providerTurnId: null,
    nativeItemRef: null,
    parentItemId: null,
    ordinal: row.ordinal,
    status: row.is_streaming === 1 ? ("interrupted" as const) : ("completed" as const),
    title: null,
    startedAt: createdAt,
    completedAt: updatedAt,
    updatedAt,
  };
  const turnItem: OrchestrationV2TurnItem =
    row.role === "user"
      ? {
          ...baseTurnItem,
          createdBy: "user",
          creationSource: "server",
          type: "user_message",
          messageId,
          inputIntent: "turn_start",
          text: row.text,
          ...(context !== undefined ? { context } : {}),
          attachments,
        }
      : {
          ...baseTurnItem,
          type: "assistant_message",
          messageId,
          text: row.text,
          ...(context !== undefined ? { context } : {}),
          streaming: false,
        };
  return [
    {
      id: EventId.make(`${IMPORT_EVENT_PREFIX}:message:${row.message_id}`),
      type: "message.updated",
      threadId,
      occurredAt: updatedAt,
      payload: message,
    },
    {
      id: EventId.make(`${IMPORT_EVENT_PREFIX}:turn-item:${row.message_id}`),
      type: "turn-item.updated",
      threadId,
      occurredAt: updatedAt,
      payload: turnItem,
    },
  ];
}

function chunks<A>(items: ReadonlyArray<A>, size: number): Array<ReadonlyArray<A>> {
  const result: Array<ReadonlyArray<A>> = [];
  for (let index = 0; index < items.length; index += size) {
    result.push(items.slice(index, index + size));
  }
  return result;
}

export const makeFromRows = Effect.fn("LegacyV1ThreadImporter.makeFromRows")(function* <E>(
  storage: LegacyV1Rows<E>,
) {
  const eventSink = yield* EventSink.EventSinkV2;
  const transcriptImports = yield* KeyedLock.make<ThreadId>();

  const listMessages = (threadId: ThreadId) => storage.messages(threadId);

  const listShellMessages = Effect.fn("LegacyV1ThreadImporter.listShellMessages")(function* (
    threadId: ThreadId,
  ) {
    const latest = yield* storage.latestMessage(threadId, false);
    const latestUser = yield* storage.latestMessage(threadId, true);
    return [latestUser[0], latest[0]].filter(
      (message, index, selected): message is LegacyMessageRow =>
        message !== undefined &&
        selected.findIndex((candidate) => candidate?.message_id === message.message_id) === index,
    );
  });

  const reconcileShellsBase = Effect.gen(function* () {
    const now = yield* DateTime.now;
    const repairRows = yield* storage.repairs;
    let repairedThreadCount = 0;
    for (const row of repairRows) {
      const decoded = decodeStoredThread(row.payload_json);
      if (Option.isNone(decoded)) continue;
      const current = decoded.value;
      const legacy = importedThread(row, now);
      const legacyPullRequests = legacy.pullRequests ?? [];
      const repaired: OrchestrationV2AppThread = {
        ...current,
        pinnedAt: current.pinnedAt === undefined ? legacy.pinnedAt : current.pinnedAt,
        autoSettleDisabledAt:
          current.autoSettleDisabledAt === undefined
            ? legacy.autoSettleDisabledAt
            : current.autoSettleDisabledAt,
        pinOrderKey: current.pinOrderKey === undefined ? legacy.pinOrderKey : current.pinOrderKey,
        snoozedUntil:
          current.snoozedUntil === undefined ? legacy.snoozedUntil : current.snoozedUntil,
        snoozedAt: current.snoozedAt === undefined ? legacy.snoozedAt : current.snoozedAt,
        unsettledAt: current.unsettledAt === undefined ? legacy.unsettledAt : current.unsettledAt,
        linkedPullRequest:
          current.linkedPullRequest === undefined
            ? legacy.linkedPullRequest
            : current.linkedPullRequest,
        pullRequests:
          current.pullRequests === undefined
            ? current.linkedPullRequest === null
              ? []
              : legacyPullRequests.length > 0
                ? legacyPullRequests
                : threadPullRequestsOf({
                    linkedPullRequest:
                      current.linkedPullRequest === undefined
                        ? legacy.linkedPullRequest
                        : current.linkedPullRequest,
                  })
            : current.pullRequests,
        branchPullRequest:
          current.branchPullRequest === undefined
            ? legacy.branchPullRequest
            : current.branchPullRequest,
        activeOrderKey:
          current.activeOrderKey === undefined ? legacy.activeOrderKey : current.activeOrderKey,
      };
      // Later schema additions can require another repair for the same thread.
      const repairId = yield* randomUuidV4;
      yield* eventSink.write({
        events: [
          {
            id: EventId.make(
              `${IMPORT_EVENT_PREFIX}:thread:${row.thread_id}:metadata-repair:${repairId}`,
            ),
            type: "thread.metadata-updated",
            threadId: repaired.id,
            providerInstanceId: repaired.providerInstanceId,
            occurredAt: now,
            payload: repaired,
          },
        ],
      });
      repairedThreadCount += 1;
    }
    const rows = yield* storage.pendingThreads;
    let importedThreadCount = repairedThreadCount;
    let importedMessageCount = 0;
    for (const row of rows) {
      const thread = importedThread(row, now);
      const previews = yield* listShellMessages(thread.id);
      const events: Array<OrchestrationV2DomainEvent> = [
        {
          id: EventId.make(`${IMPORT_EVENT_PREFIX}:thread:${row.thread_id}:created`),
          type: "thread.created",
          threadId: thread.id,
          providerInstanceId: thread.providerInstanceId,
          occurredAt: thread.createdAt,
          payload: thread,
        },
        ...previews.flatMap((message) => messageEvents(message, now)),
        {
          id: EventId.make(`${IMPORT_EVENT_PREFIX}:thread:${row.thread_id}:shell`),
          type: "thread.metadata-updated",
          threadId: thread.id,
          providerInstanceId: thread.providerInstanceId,
          occurredAt: thread.updatedAt,
          payload: thread,
        },
      ];
      yield* storage.withTransaction(
        Effect.gen(function* () {
          yield* Effect.forEach(
            previews,
            (message) =>
              storage.insertPosition({
                threadId: thread.id,
                turnItemId: TurnItemId.make(
                  `${IMPORT_EVENT_PREFIX}:turn-item:${message.message_id}`,
                ),
                ordinal: message.ordinal,
              }),
            { discard: true },
          );
          yield* eventSink.write({ events });
          yield* storage.insertShell({
            threadId: thread.id,
            sourceUpdatedAt: row.updated_at,
            shellImportedAt: DateTime.formatIso(now),
            importedMessageCount: previews.length,
          });
        }),
      );
      importedThreadCount += 1;
      importedMessageCount += previews.length;
    }
    return { importedThreadCount, importedMessageCount };
  });

  const reconcileShells = reconcileShellsBase.pipe(
    Effect.mapError((cause) => new LegacyV1ThreadImportError({ operation: "import", cause })),
  );

  const pendingThreadCount = storage.pendingThreadCount.pipe(
    Effect.mapError(
      (cause) => new LegacyV1ThreadImportError({ operation: "inspect pending", cause }),
    ),
  );

  // Both results are final for this process: transcript_imported_at never resets, and only startup shell reconciliation creates legacy rows.
  const confirmedTranscriptThreadIds = new Set<ThreadId>();

  const ensureTranscriptBase = (threadId: ThreadId) =>
    transcriptImports.withLock(
      threadId,
      Effect.gen(function* () {
        const imports = yield* storage.getImport(threadId);
        const imported = imports[0];
        if (imported === undefined || imported.transcript_imported_at !== null) {
          confirmedTranscriptThreadIds.add(threadId);
          return { importedThreadCount: 0, importedMessageCount: 0 };
        }
        const importedAt = yield* DateTime.now;
        const messages = yield* listMessages(threadId);
        // Hint the index to skip V1 events without scanning their payloads.
        const existingRows = yield* storage.importedEvents(threadId);
        const existing = new Set(existingRows.map((row) => row.event_id));
        const missing = messages.filter(
          (message) => !existing.has(`${IMPORT_EVENT_PREFIX}:message:${message.message_id}`),
        );
        for (const batch of chunks(missing, TRANSCRIPT_EVENT_BATCH_SIZE / 2)) {
          yield* Effect.forEach(
            batch,
            (message) =>
              storage.insertPosition({
                threadId: threadId,
                turnItemId: TurnItemId.make(
                  `${IMPORT_EVENT_PREFIX}:turn-item:${message.message_id}`,
                ),
                ordinal: message.ordinal,
              }),
            { discard: true },
          );
          yield* eventSink.write({
            events: batch.flatMap((message) => messageEvents(message, importedAt)),
          });
          yield* Effect.yieldNow;
        }
        const now = DateTime.formatIso(yield* DateTime.now);
        yield* storage.complete({ threadId, completedAt: now, messageCount: messages.length });
        confirmedTranscriptThreadIds.add(threadId);
        return {
          importedThreadCount: 1,
          importedMessageCount: missing.length,
        };
      }),
    );

  const ensureTranscript = (threadId: ThreadId) =>
    confirmedTranscriptThreadIds.has(threadId)
      ? Effect.succeed({ importedThreadCount: 0, importedMessageCount: 0 })
      : ensureTranscriptBase(threadId).pipe(
          Effect.mapError(
            (cause) =>
              new LegacyV1ThreadImportError({
                operation: "hydrate transcript for",
                threadId,
                cause,
              }),
          ),
        );

  const importPendingTranscripts = Effect.gen(function* () {
    const rows = yield* storage.pendingImports;
    let importedThreadCount = 0;
    let importedMessageCount = 0;
    for (const row of rows) {
      const result = yield* ensureTranscript(ThreadId.make(row.thread_id)).pipe(
        Effect.catchCauseIf(
          (cause) => !Cause.hasInterrupts(cause),
          (cause) =>
            Effect.logWarning("Failed to hydrate migrated v1 thread transcript", {
              threadId: row.thread_id,
              cause,
            }).pipe(
              Effect.andThen(() =>
                storage.failure(row.thread_id).pipe(
                  Effect.catchCauseIf(
                    (cause) => !Cause.hasInterrupts(cause),
                    () => Effect.void,
                  ),
                ),
              ),
              Effect.as({ importedThreadCount: 0, importedMessageCount: 0 }),
            ),
        ),
      );
      importedThreadCount += result.importedThreadCount;
      importedMessageCount += result.importedMessageCount;
      yield* Effect.yieldNow;
    }
    return { importedThreadCount, importedMessageCount };
  }).pipe(
    Effect.catchCause((cause) =>
      Cause.hasInterrupts(cause)
        ? Effect.failCause(Cause.fromReasons<never>(cause.reasons.filter(Cause.isInterruptReason)))
        : Effect.logWarning("Legacy v1 transcript background import stopped", { cause }).pipe(
            Effect.as({ importedThreadCount: 0, importedMessageCount: 0 }),
          ),
    ),
  );

  return LegacyV1ThreadImporter.of({
    pendingThreadCount,
    reconcileShells,
    ensureTranscript,
    importPendingTranscripts,
  });
});

export const make = Effect.gen(function* () {
  return yield* makeFromRows(yield* makeSqliteRows);
});

export const layer: Layer.Layer<
  LegacyV1ThreadImporter,
  never,
  EventSink.EventSinkV2 | SqlClient.SqlClient
> = Layer.effect(LegacyV1ThreadImporter, make);
