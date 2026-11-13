import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";

import {
  ProjectionLookupRowsError,
  type ProjectionLookup,
  type ProjectionLookupRows,
  type ProjectionLookupResult,
} from "./ProjectionLookupRows.ts";

const empty: ProjectionLookupResult = {
  owner_exists: true,
  payloads: [],
  metadata: [],
  workspaces: [],
  ids: [],
  count: 0,
  found: false,
};
const encodeIds = Schema.encodeSync(Schema.fromJsonString(Schema.Array(Schema.String)));

export const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const exists = (threadId: string) =>
    sql<{ readonly thread_id: string }>`
    SELECT thread_id FROM orchestration_v2_projection_threads WHERE thread_id = ${threadId} LIMIT 1
  `.pipe(Effect.map((rows) => rows.length !== 0));
  const payloads = (
    rows: ReadonlyArray<{ readonly payload_json: string }>,
  ): ProjectionLookupResult => ({
    ...empty,
    payloads: rows.map((row) => row.payload_json),
  });
  return {
    query: Effect.fnUntraced(
      function* (query: ProjectionLookup) {
        switch (query.kind) {
          case "thread_exists":
            return { ...empty, owner_exists: yield* exists(query.thread_id) };
          case "thread":
            return payloads(
              yield* sql<{ readonly payload_json: string }>`
          SELECT payload_json FROM orchestration_v2_projection_threads WHERE thread_id = ${query.thread_id}
        `,
            );
          case "node":
            return payloads(
              yield* sql<{ readonly payload_json: string }>`
          SELECT payload_json FROM orchestration_v2_projection_nodes WHERE node_id = ${query.record_id}
        `,
            );
          case "turn_item":
            return payloads(
              yield* sql<{ readonly payload_json: string }>`
          SELECT payload_json FROM orchestration_v2_projection_turn_items
          WHERE thread_id = ${query.thread_id} AND turn_item_id = ${query.record_id}
        `,
            );
          case "plan":
          case "runtime_request":
            return yield* sql.withTransaction(
              Effect.gen(function* () {
                if (!(yield* exists(query.thread_id))) return { ...empty, owner_exists: false };
                return payloads(
                  yield* query.kind === "plan"
                    ? sql<{
                        readonly payload_json: string;
                      }>`SELECT payload_json FROM orchestration_v2_projection_plans
              WHERE thread_id = ${query.thread_id} AND plan_id = ${query.record_id}`
                    : sql<{
                        readonly payload_json: string;
                      }>`SELECT payload_json FROM orchestration_v2_projection_runtime_requests
              WHERE thread_id = ${query.thread_id} AND runtime_request_id = ${query.record_id}`,
                );
              }),
            );
          case "messages": {
            if (query.record_ids.length === 0) return empty;
            return payloads(
              yield* sql<{ readonly payload_json: string }>`
            SELECT message.payload_json
            FROM json_each(${encodeIds([...new Set(query.record_ids)])}) AS requested
            CROSS JOIN orchestration_v2_projection_messages AS message ON message.message_id = requested.value
            WHERE message.thread_id = ${query.thread_id}
          `,
            );
          }
          case "run":
            return yield* sql.withTransaction(
              Effect.gen(function* () {
                if (!(yield* exists(query.thread_id))) return { ...empty, owner_exists: false };
                return payloads(
                  yield* sql<{ readonly payload_json: string }>`
                  SELECT payload_json FROM orchestration_v2_projection_runs
                  WHERE run_id = ${query.run_id} AND thread_id = ${query.thread_id}
                `,
                );
              }),
            );
          case "transcript_byte_floor": {
            const rows = yield* sql<{ readonly bytes: number }>`
              WITH tool_rows AS (
                SELECT payload_json FROM orchestration_v2_projection_turn_items
                WHERE thread_id = ${query.thread_id}
                  AND type IN ('command_execution', 'dynamic_tool')
                  AND (run_id IS NULL OR run_id NOT IN (
                    SELECT run_id FROM orchestration_v2_projection_runs
                    WHERE thread_id = ${query.thread_id} AND status = 'rolled_back'
                  ))
              )
              SELECT CASE
                WHEN (SELECT COALESCE(SUM(octet_length(payload_json)), 0) FROM tool_rows) <= ${query.limit}
                  THEN 0
                ELSE (
                  SELECT COALESCE(SUM(CASE WHEN json_valid(payload_json) THEN (
                    SELECT COALESCE(SUM(octet_length(leaf.value)), 0)
                    FROM json_tree(tool_rows.payload_json, '$.output') AS leaf
                    WHERE leaf.type = 'text'
                  ) ELSE 0 END), 0)
                  FROM tool_rows
                )
              END AS bytes
            `;
            return { ...empty, count: rows[0]?.bytes ?? 0 };
          }
          case "pending_delegated_completion": {
            const rows = yield* sql`SELECT 1 FROM orchestration_v2_projection_subagents AS task
              LEFT JOIN orchestration_v2_projection_runs AS owner ON owner.run_id = task.run_id
              WHERE task.child_thread_id = ${query.thread_id} AND task.origin = 'app_owned'
                AND (json_extract(task.payload_json, '$.result') IS NULL
                  OR (task.completion_delivery_state = 'claimed'
                    AND json_extract(owner.payload_json, '$.delegatedCompletion.disposition') = 'open'
                    AND json_type(owner.payload_json, '$.delegatedCompletion.delivery') = 'object'))
              LIMIT 1`;
            return { ...empty, found: rows.length > 0 };
          }
          case "run_message":
            return yield* sql.withTransaction(
              Effect.gen(function* () {
                if (!(yield* exists(query.thread_id))) return { ...empty, owner_exists: false };
                return payloads(
                  yield* sql<{ readonly payload_json: string }>`
            SELECT message.payload_json FROM orchestration_v2_projection_messages AS message
            JOIN orchestration_v2_projection_runs AS run ON message.message_id = json_extract(run.payload_json, '$.userMessageId')
            WHERE run.thread_id = ${query.thread_id} AND run.run_id = ${query.run_id} AND message.thread_id = ${query.thread_id}
          `,
                );
              }),
            );
          case "can_start_queued_run":
            return yield* sql.withTransaction(
              Effect.gen(function* () {
                if (!(yield* exists(query.thread_id))) return { ...empty, owner_exists: false };
                const rows = yield* sql<{ readonly ready: number }>`
            SELECT 1 AS ready FROM orchestration_v2_projection_threads AS thread
            WHERE thread.thread_id = ${query.thread_id} AND thread.archived_at IS NULL AND thread.deleted_at IS NULL
              AND EXISTS (SELECT 1 FROM orchestration_v2_projection_runs
                WHERE thread_id = ${query.thread_id} AND status = 'queued' AND json_extract(payload_json, '$.queueHeld') IS NOT 1)
              AND NOT EXISTS (SELECT 1 FROM orchestration_v2_projection_runs
                WHERE thread_id = ${query.thread_id} AND status IN ('preparing', 'starting', 'running', 'waiting'))
          `;
                return { ...empty, found: rows.length > 0 };
              }),
            );
          case "held_queued_runs":
            return payloads(
              yield* sql<{ readonly payload_json: string }>`
          SELECT payload_json FROM orchestration_v2_projection_runs
          WHERE thread_id = ${query.thread_id} AND status = 'queued' AND json_extract(payload_json, '$.queueHeld') IS 1
          ORDER BY ordinal ASC
        `,
            );
          case "message_count": {
            const rows = yield* sql<{
              readonly count: number;
            }>`SELECT COUNT(*) AS count FROM orchestration_v2_projection_messages WHERE thread_id = ${query.thread_id}`;
            return { ...empty, count: rows[0]?.count ?? 0 };
          }
          case "subagent_count": {
            const rows = yield* sql<{
              readonly count: number;
            }>`SELECT COUNT(*) AS count FROM orchestration_v2_projection_subagents WHERE thread_id = ${query.thread_id}`;
            return { ...empty, count: rows[0]?.count ?? 0 };
          }
          case "app_owned_child_threads": {
            const rows = yield* sql<{ readonly child_thread_id: string }>`SELECT child_thread_id
            FROM orchestration_v2_projection_subagents
            WHERE thread_id = ${query.thread_id} AND origin = 'app_owned' AND child_thread_id IS NOT NULL
            ORDER BY COALESCE(started_at, ''), subagent_id ASC`;
            return { ...empty, ids: [...new Set(rows.map((row) => row.child_thread_id))] };
          }
          case "next_turn_item_ordinal": {
            const rows = yield* sql<{
              readonly ordinal: number | null;
            }>`SELECT MAX(ordinal) AS ordinal
            FROM orchestration_v2_projection_turn_items WHERE thread_id = ${query.thread_id}`;
            return { ...empty, count: (rows[0]?.ordinal ?? 0) + 1 };
          }
          case "turn_start_history":
            return payloads(
              yield* sql<{ readonly payload_json: string }>`
          SELECT payload_json FROM orchestration_v2_projection_turn_items
          WHERE thread_id = ${query.thread_id}
            AND type IN ('user_message','assistant_message','command_execution','error','run_interrupt_result','file_change','proposed_plan')
            AND ${query.run_ids === undefined ? sql`1` : sql`run_id IN ${sql.in(query.run_ids)}`}
          ORDER BY ordinal ASC, turn_item_id ASC
        `,
            );
          case "thread_metadata": {
            if (query.thread_ids.length === 0) return empty;
            const metadata = yield* sql<{
              readonly thread_id: string;
              readonly project_id: string;
              readonly title: string;
            }>`
            SELECT thread_id, project_id, title FROM orchestration_v2_projection_threads
            WHERE deleted_at IS NULL AND thread_id IN ${sql.in(query.thread_ids)}
          `;
            return { ...empty, metadata };
          }
          case "unpaired_interrupt": {
            const rows = yield* sql<{ readonly pending: number }>`
            SELECT EXISTS (SELECT 1 FROM orchestration_v2_projection_turn_items
              WHERE thread_id = ${query.thread_id} AND turn_item_id = ${query.request_id})
            AND NOT EXISTS (SELECT 1 FROM orchestration_v2_projection_turn_items
              WHERE thread_id = ${query.thread_id} AND turn_item_id = ${query.result_id}) AS pending
          `;
            return { ...empty, found: rows[0]?.pending === 1 };
          }
        }
      },
      Effect.mapError((cause) => new ProjectionLookupRowsError({ cause })),
    ),
  } satisfies ProjectionLookupRows;
});
