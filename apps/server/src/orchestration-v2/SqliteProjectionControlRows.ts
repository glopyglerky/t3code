import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

import {
  ProjectionControlRowsError,
  type ProjectionControlQuery,
  type ProjectionControlRows,
  type ProjectionControlResult,
} from "./ProjectionControlRows.ts";

type PayloadRow = { readonly payload_json: string };
const empty: ProjectionControlResult = {
  requests: [],
  nodes: [],
  items: [],
  providerThreads: [],
  providerTurns: [],
  attempts: [],
  messages: [],
  runs: [],
  sessions: [],
};
const payloads = (rows: ReadonlyArray<PayloadRow>) => rows.map((row) => row.payload_json);

export const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const owningNodes = (threadId: string) => sql`
    SELECT owner.node_id FROM orchestration_v2_projection_provider_threads AS owned
      INDEXED BY orchestration_v2_projection_provider_threads_owner_idx
    CROSS JOIN orchestration_v2_projection_nodes AS owner ON owner.node_id = owned.owner_node_id
    WHERE owned.owner_node_id IS NOT NULL AND owner.thread_id = ${threadId}
  `;
  return {
    withReadTransaction: sql.withTransaction,
    query: Effect.fnUntraced(
      function* (
        query: ProjectionControlQuery,
      ): Effect.fn.Return<ProjectionControlResult, import("effect/sql/SqlError").SqlError> {
        switch (query.kind) {
          case "pending_user_requests":
            return {
              ...empty,
              requests: payloads(
                yield* sql<PayloadRow>`
          SELECT payload_json FROM orchestration_v2_projection_runtime_requests
          WHERE thread_id = ${query.thread_id} AND provider_turn_id = ${query.provider_turn_id}
            AND kind = 'user_input' AND status = 'pending'
        `,
              ),
            };
          case "pending_user_input_details":
            return {
              ...empty,
              nodes: payloads(
                yield* sql<PayloadRow>`SELECT payload_json FROM orchestration_v2_projection_nodes
            WHERE thread_id = ${query.thread_id} AND ${sql.in("node_id", query.node_ids)}`,
              ),
              items: payloads(
                yield* sql<PayloadRow>`SELECT payload_json FROM orchestration_v2_projection_turn_items
            WHERE thread_id = ${query.thread_id} AND provider_turn_id = ${query.provider_turn_id} AND type = 'user_input_request'`,
              ),
            };
          case "provider_control": {
            const [threads, turns, attempts, messages] = yield* Effect.all([
              sql<PayloadRow>`SELECT payload_json FROM orchestration_v2_projection_provider_threads
              WHERE provider_thread_id = ${query.provider_thread_id} AND (
                thread_id = ${query.thread_id} OR owner_node_id IN (${owningNodes(query.thread_id)})
                OR provider_thread_id IN (SELECT provider_thread_id FROM orchestration_v2_projection_subagents WHERE thread_id = ${query.thread_id}))`,
              sql<PayloadRow>`SELECT payload_json FROM orchestration_v2_projection_provider_turns
              WHERE thread_id = ${query.thread_id} AND provider_turn_id = ${query.provider_turn_id}`,
              query.attempt_id === undefined
                ? Effect.succeed([])
                : sql<PayloadRow>`SELECT payload_json FROM orchestration_v2_projection_run_attempts
              WHERE thread_id = ${query.thread_id} AND attempt_id = ${query.attempt_id}`,
              query.message_id === undefined
                ? Effect.succeed([])
                : sql<PayloadRow>`SELECT payload_json FROM orchestration_v2_projection_messages
              WHERE thread_id = ${query.thread_id} AND message_id = ${query.message_id}`,
            ]);
            return {
              ...empty,
              providerThreads: payloads(threads),
              providerTurns: payloads(turns),
              attempts: payloads(attempts),
              messages: payloads(messages),
            };
          }
          case "provider_control_run":
            return {
              ...empty,
              runs: payloads(
                yield* sql<PayloadRow>`
          SELECT payload_json FROM orchestration_v2_projection_runs
          WHERE thread_id = ${query.thread_id} AND json_extract(payload_json, '$.activeAttemptId') IS ${query.active_attempt_id}
          ORDER BY ordinal ASC LIMIT 1
        `,
              ),
            };
          case "running_run":
            return {
              ...empty,
              runs: payloads(
                yield* sql<PayloadRow>`
          SELECT payload_json FROM orchestration_v2_projection_runs
          WHERE thread_id = ${query.thread_id} AND status = 'running' ORDER BY ordinal ASC LIMIT 1
        `,
              ),
            };
          case "running_turn_details":
            return {
              ...empty,
              providerThreads: payloads(
                yield* sql<PayloadRow>`SELECT payload_json FROM orchestration_v2_projection_provider_threads
            WHERE provider_thread_id = ${query.provider_thread_id}`,
              ),
              providerTurns: payloads(
                yield* sql<PayloadRow>`SELECT payload_json FROM orchestration_v2_projection_provider_turns
            WHERE provider_thread_id = ${query.provider_thread_id} AND run_attempt_id = ${query.run_attempt_id}
              AND node_id = ${query.node_id} AND status = 'running'`,
              ),
            };
          case "thread_providers":
            return {
              ...empty,
              sessions: payloads(
                yield* sql<PayloadRow>`SELECT session.payload_json FROM orchestration_v2_projection_provider_sessions AS session
            JOIN orchestration_v2_projection_provider_session_bindings AS binding ON binding.provider_session_id = session.provider_session_id
            WHERE binding.thread_id = ${query.thread_id} ORDER BY session.updated_at ASC, session.provider_session_id ASC`,
              ),
              providerThreads:
                query.target_instance_id === undefined
                  ? []
                  : payloads(
                      yield* sql<PayloadRow>`
            SELECT payload_json FROM orchestration_v2_projection_provider_threads
            WHERE provider_thread_id = ${query.active_provider_thread_id}
              OR provider_thread_id = (SELECT provider_thread_id FROM orchestration_v2_projection_provider_threads
                WHERE thread_id = ${query.thread_id} AND owner_node_id IS NULL AND provider_instance_id = ${query.target_instance_id}
                ORDER BY updated_at DESC, provider_thread_id ASC LIMIT 1)
            ORDER BY updated_at ASC, provider_thread_id ASC
          `,
                    ),
            };
          case "runtime_response":
            return {
              ...empty,
              nodes: payloads(
                yield* sql<PayloadRow>`SELECT payload_json FROM orchestration_v2_projection_nodes
            WHERE thread_id = ${query.thread_id} AND node_id = ${query.node_id}`,
              ),
              items: payloads(
                yield* sql<PayloadRow>`SELECT payload_json FROM orchestration_v2_projection_turn_items
            WHERE thread_id = ${query.thread_id} AND node_id = ${query.node_id} AND type IN ('approval_request', 'user_input_request')
              AND json_extract(payload_json, '$.requestId') = ${query.request_id} ORDER BY ordinal ASC LIMIT 1`,
              ),
              sessions:
                query.session_id === undefined
                  ? []
                  : payloads(
                      yield* sql<PayloadRow>`
            SELECT session.payload_json FROM orchestration_v2_projection_provider_sessions AS session
            JOIN orchestration_v2_projection_provider_session_bindings AS binding ON binding.provider_session_id = session.provider_session_id
            WHERE binding.thread_id = ${query.thread_id} AND session.provider_session_id = ${query.session_id}`,
                    ),
            };
        }
      },
      (effect) =>
        effect.pipe(Effect.mapError((cause) => new ProjectionControlRowsError({ cause }))),
    ),
  } satisfies ProjectionControlRows;
});
