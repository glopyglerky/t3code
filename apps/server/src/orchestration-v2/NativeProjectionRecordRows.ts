import * as Effect from "effect/Effect";
import * as DateTime from "effect/DateTime";
import * as Schema from "effect/Schema";
import { ThreadId } from "@t3tools/contracts";

import { RedbClient } from "../persistence/RedbClient.ts";
import * as NativeEventRows from "../persistence/NativeEventRows.ts";
import {
  ProjectionRecordRowsError,
  type ProjectionRecordRows,
  type ProjectionRecordWindow,
} from "./ProjectionRecordRows.ts";
import {
  makeCanonicalProjection,
  makeProjectionReader,
  makeProjectionSnapshots,
  makeProjectionTimelinePages,
  makeProjectionRecords,
  makeTimelineIndex as timelineIndex,
} from "./ProjectionStore.ts";
import {
  ProjectionAttachmentOwner,
  ProjectionMaterializedRow,
  ProjectionTimelineRow,
  type ProjectionHistoryRows,
} from "./ProjectionHistoryRows.ts";

const Payloads = Schema.Array(Schema.String);
const wireWindow = (window: ProjectionRecordWindow) =>
  Object.fromEntries(
    Object.entries({
      rowLimit: window.rowLimit,
      userTurnLimit: window.userTurnLimit,
      deferWork: window.deferWork,
      anchorItemId: window.anchorItemId,
      requiredRunId: window.requiredRunId,
      forkCreatedAt:
        window.forkCreatedAt === undefined ? undefined : DateTime.formatIso(window.forkCreatedAt),
    }).filter(([, value]) => value !== undefined),
  );
const Result = Schema.Struct({
  thread: Schema.NullOr(Schema.String),
  rows: Schema.Struct({
    runs: Payloads,
    attempts: Payloads,
    nodes: Payloads,
    subagents: Payloads,
    providerSessions: Payloads,
    providerThreads: Payloads,
    providerTurns: Payloads,
    runtimeRequests: Payloads,
    messages: Payloads,
    plans: Payloads,
    turnItems: Payloads,
    checkpointScopes: Payloads,
    checkpoints: Payloads,
    contextHandoffs: Payloads,
    contextTransfers: Payloads,
  }),
  messageOrdinals: Schema.Array(
    Schema.Struct({ messageId: Schema.String, ordinal: Schema.Number }),
  ),
  deferredWork: Schema.Array(
    Schema.Struct({
      rowIndex: Schema.Number,
      itemId: Schema.String,
      lastItemId: Schema.String,
      lastOrdinal: Schema.Number,
      itemCount: Schema.Number,
    }),
  ),
});

export const makeRows = Effect.gen(function* () {
  const client = yield* RedbClient;
  while (!(yield* client.prepareProjectionReads).prepared) yield* Effect.yieldNow;
  return {
    read: (query) =>
      client
        .request(
          "application_projections_records",
          {
            query: {
              thread_id: query.thread_id,
              fields: query.fields,
              ...(query.filter === undefined
                ? {}
                : {
                    filter: Object.fromEntries(
                      Object.entries(query.filter).filter(([, value]) => value !== undefined),
                    ),
                  }),
              ...(query.window === undefined ? {} : { window: wireWindow(query.window) }),
            },
          },
          Result,
        )
        .pipe(Effect.mapError((cause) => new ProjectionRecordRowsError({ cause }))),
  } satisfies ProjectionRecordRows;
});

/** The complete unwindowed selected-record API; timeline and window APIs remain separate. */
export const make = makeRows.pipe(Effect.map(makeProjectionRecords));
export const makeCanonical = makeRows.pipe(Effect.map(makeCanonicalProjection));

export const makeHistoryRows = Effect.gen(function* () {
  const client = yield* RedbClient;
  while (!(yield* client.prepareProjectionReads).prepared) yield* Effect.yieldNow;
  const failure = (cause: unknown) => new ProjectionRecordRowsError({ cause });
  return {
    timeline: (threadId, includePersistentOutput) =>
      client
        .request(
          "application_projection_window_query",
          {
            query: {
              kind: "timeline",
              thread_id: threadId,
              include_persistent_output: includePersistentOutput,
            },
          },
          Schema.Array(ProjectionTimelineRow),
        )
        .pipe(Effect.mapError(failure)),
    forkBoundary: (threadId, runId, anchorItemId) =>
      client
        .request(
          "application_projection_window_query",
          {
            query: {
              kind: "fork_boundary",
              thread_id: threadId,
              run_id: runId,
              ...(anchorItemId === undefined ? {} : { anchor_item_id: anchorItemId }),
            },
          },
          Schema.Struct({ anchor: Schema.NullOr(Schema.String) }),
        )
        .pipe(
          Effect.map((result) => result.anchor),
          Effect.mapError(failure),
        ),
    inheritedCount: (threadId, runId, window) =>
      client
        .request(
          "application_projection_window_query",
          {
            query: {
              kind: "inherited_count",
              thread_id: threadId,
              run_id: runId,
              window: wireWindow(window),
            },
          },
          Schema.Struct({ count: Schema.NullOr(Schema.Number) }),
        )
        .pipe(
          Effect.map((result) => result.count),
          Effect.mapError(failure),
        ),
  } satisfies ProjectionHistoryRows;
});

export const makeTimelineIndex = Effect.gen(function* () {
  const records = yield* make;
  const rows = yield* makeHistoryRows;
  return timelineIndex(records, rows.timeline);
});

export const makeProjection = Effect.gen(function* () {
  const canonical = yield* makeCanonical;
  const history = yield* makeHistoryRows;
  return makeProjectionReader(canonical, history);
});

export const makeSnapshots = Effect.gen(function* () {
  const client = yield* RedbClient;
  const projection = yield* makeProjection;
  const events = yield* NativeEventRows.make;
  const failure = (cause: unknown) => new ProjectionRecordRowsError({ cause });
  return makeProjectionSnapshots(projection, {
    withReadTransaction: client.withReadTransaction,
    anchorThread: (itemId) =>
      client
        .request(
          "application_projection_window_query",
          {
            query: { kind: "anchor_thread", item_id: itemId },
          },
          Schema.Struct({ thread_id: Schema.NullOr(Schema.String) }),
        )
        .pipe(
          Effect.map((result) =>
            result.thread_id === null ? null : ThreadId.make(result.thread_id),
          ),
          Effect.mapError(failure),
        ),
    pendingCards: (threadId) =>
      client
        .request(
          "application_projection_window_query",
          {
            query: { kind: "pending_cards", thread_id: threadId },
          },
          Schema.Array(Schema.String),
        )
        .pipe(Effect.mapError(failure)),
    snapshotSequence: (threadId) =>
      events.latest({ kind: "agent", thread_id: threadId }).pipe(Effect.mapError(failure)),
  });
});

export const makeTimelinePages = Effect.gen(function* () {
  const client = yield* RedbClient;
  const timeline = yield* makeTimelineIndex;
  const events = yield* NativeEventRows.make;
  const failure = (cause: unknown) => new ProjectionRecordRowsError({ cause });
  return makeProjectionTimelinePages(timeline, {
    withReadTransaction: client.withReadTransaction,
    snapshotSequence: (threadId) =>
      events.latest({ kind: "agent", thread_id: threadId }).pipe(Effect.mapError(failure)),
    materialize: (sources) =>
      client
        .request(
          "application_projection_window_query",
          {
            query: { kind: "materialize", sources_json: sources },
          },
          Schema.Array(ProjectionMaterializedRow),
        )
        .pipe(Effect.mapError(failure)),
    attachmentOwners: (threadIds, attachmentId) =>
      client
        .request(
          "application_projection_window_query",
          {
            query: {
              kind: "attachment_owners",
              thread_ids: threadIds,
              attachment_id: attachmentId,
            },
          },
          Schema.Array(ProjectionAttachmentOwner),
        )
        .pipe(Effect.mapError(failure)),
  });
});
