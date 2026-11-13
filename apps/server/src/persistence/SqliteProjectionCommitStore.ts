import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";

import {
  ProjectionCommitStore,
  toProjectionCommitStoreError,
  type ProjectionCommitStoreShape,
} from "../orchestration-v2/ProjectionCommitStore.ts";
import {
  ORCHESTRATION_V2_PROJECTION_SCHEMA_VERSION,
  ProjectionStoreV2,
} from "../orchestration-v2/ProjectionStore.ts";

const encodeTime = Schema.encodeEffect(Schema.DateTimeUtcFromString);

/** The production SQLite projection implementation remains the mutation oracle. */
export const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const projections = yield* ProjectionStoreV2;
  return ProjectionCommitStore.of({
    apply: (event) =>
      projections.apply(event).pipe(Effect.mapError(toProjectionCommitStoreError("apply"))),
    getThread: (threadId) =>
      projections.getThread(threadId).pipe(
        Effect.catchTag("ProjectionStoreThreadNotFoundError", () => Effect.undefined),
        Effect.mapError(toProjectionCommitStoreError("get-thread")),
      ),
    getRuntimeRequest: (threadId, requestId) =>
      projections
        .getRuntimeRequest(threadId, requestId)
        .pipe(Effect.mapError(toProjectionCommitStoreError("get-runtime-request"))),
    getRun: (threadId, runId) =>
      projections.getThreadRecords(threadId, ["runs"], { runIds: [runId] }).pipe(
        Effect.map((records) => records.runs[0]),
        Effect.mapError(toProjectionCommitStoreError("get-run")),
      ),
    markApplied: Effect.fnUntraced(
      function* (sequence, updatedAt) {
        const encodedTime = yield* encodeTime(updatedAt);
        yield* sql`
        INSERT INTO orchestration_v2_projection_metadata (
          projection_name, schema_version, last_sequence, updated_at
        ) VALUES (
          'thread-projections', ${ORCHESTRATION_V2_PROJECTION_SCHEMA_VERSION},
          ${sequence}, ${encodedTime}
        )
        ON CONFLICT(projection_name) DO UPDATE SET
          schema_version = excluded.schema_version,
          last_sequence = excluded.last_sequence,
          updated_at = excluded.updated_at
      `;
      },
      Effect.mapError(toProjectionCommitStoreError("mark-applied")),
    ),
    nativeThreadOwned: Effect.fnUntraced(
      function* (input) {
        if (input.nativeThreadRef === null) return false;
        const rows = yield* sql`
        SELECT provider_thread_id FROM orchestration_v2_projection_provider_threads
        WHERE driver = ${input.driver}
          AND provider_instance_id = ${input.providerInstanceId}
          AND thread_id IS NOT ${input.appThreadId}
          AND json_extract(payload_json, '$.nativeThreadRef.nativeId') = ${input.nativeThreadRef.nativeId}
        LIMIT 1
      `;
        return rows.length > 0;
      },
      Effect.mapError(toProjectionCommitStoreError("native-thread-owned")),
    ),
    runCurrent: Effect.fnUntraced(
      function* (input) {
        const rows = yield* sql<{
          readonly status: string;
          readonly active_attempt_id: string | null;
        }>`
        SELECT status, json_extract(payload_json, '$.activeAttemptId') AS active_attempt_id
        FROM orchestration_v2_projection_runs
        WHERE run_id = ${input.runId} AND thread_id = ${input.threadId}
        LIMIT 1
      `;
        return (
          rows[0]?.status === input.expectedStatus &&
          rows[0]?.active_attempt_id === input.activeAttemptId
        );
      },
      Effect.mapError(toProjectionCommitStoreError("run-current")),
    ),
    providerThreadOwner: Effect.fnUntraced(
      function* (input) {
        const rows = yield* sql<{
          readonly active_attempt_id: string | null;
          readonly last_run_ordinal: number | null;
        }>`
        SELECT json_extract(r.payload_json, '$.activeAttemptId') AS active_attempt_id,
          p.last_run_ordinal
        FROM orchestration_v2_projection_provider_threads p
        JOIN orchestration_v2_projection_runs r
          ON r.run_id = ${input.runId} AND r.thread_id = p.thread_id
        WHERE p.provider_thread_id = ${input.providerThreadId}
        LIMIT 1
      `;
        return (
          rows[0]?.active_attempt_id === input.activeAttemptId &&
          rows[0]?.last_run_ordinal === input.expectedLastRunOrdinal
        );
      },
      Effect.mapError(toProjectionCommitStoreError("provider-thread-owner")),
    ),
    providerThreadUnchanged: Effect.fnUntraced(
      function* (providerThreadId, expectedJson) {
        const rows = yield* sql<{ readonly payload_json: string }>`
        SELECT payload_json FROM orchestration_v2_projection_provider_threads
        WHERE provider_thread_id = ${providerThreadId} LIMIT 1
      `;
        return rows[0]?.payload_json === expectedJson;
      },
      Effect.mapError(toProjectionCommitStoreError("provider-thread-unchanged")),
    ),
    turnItemUnchanged: Effect.fnUntraced(
      function* (turnItemId, expectedJson) {
        const rows = yield* sql<{ readonly payload_json: string }>`
        SELECT payload_json FROM orchestration_v2_projection_turn_items
        WHERE turn_item_id = ${turnItemId} LIMIT 1
      `;
        return rows[0]?.payload_json === expectedJson;
      },
      Effect.mapError(toProjectionCommitStoreError("turn-item-unchanged")),
    ),
    interruptPending: Effect.fnUntraced(
      function* ({ threadId, requestId, resultId }) {
        const rows = yield* sql<{ readonly pending: number }>`
        SELECT EXISTS (
          SELECT 1 FROM orchestration_v2_projection_turn_items
          WHERE thread_id = ${threadId} AND turn_item_id = ${requestId}
        ) AND NOT EXISTS (
          SELECT 1 FROM orchestration_v2_projection_turn_items
          WHERE thread_id = ${threadId} AND turn_item_id = ${resultId}
        ) AS pending
      `;
        return rows[0]?.pending === 1;
      },
      Effect.mapError(toProjectionCommitStoreError("interrupt-pending")),
    ),
  } satisfies ProjectionCommitStoreShape);
});

export const layer = Layer.effect(ProjectionCommitStore, make);
