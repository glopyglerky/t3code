import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

import {
  emptyProjectionRecordRows,
  ProjectionRecordRowsError,
  type ProjectionRecordRows,
} from "./ProjectionRecordRows.ts";

interface PayloadRow {
  readonly payload_json: string;
}
const ids = (values: ReadonlyArray<unknown>) => JSON.stringify(values);

/** The original selected-record SQL, isolated from the shared decoding pipeline. */
export const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  // An id filter reads by primary key instead of scanning every row of the thread.
  const threadColumn = (selectedIds: ReadonlyArray<unknown> | undefined, column = "thread_id") =>
    sql.literal(selectedIds === undefined ? column : `+${column}`);
  return {
    read: (query) =>
      sql
        .withTransaction(
          Effect.gen(function* () {
            if (query.window !== undefined)
              return yield* new ProjectionRecordRowsError({
                cause: "Use the canonical SQLite window query for windowed records",
              });
            const threadId = query.thread_id;
            const filter = query.filter ?? {};
            const rows = emptyProjectionRecordRows();
            const thread =
              yield* sql<PayloadRow>`SELECT payload_json FROM orchestration_v2_projection_threads
        WHERE thread_id = ${threadId} LIMIT 1`;
            if (thread[0] === undefined)
              return { thread: null, rows, messageOrdinals: [], deferredWork: [] };
            for (const name of new Set(query.fields)) {
              let selected: ReadonlyArray<PayloadRow>;
              switch (name) {
                case "runs":
                  selected =
                    yield* sql<PayloadRow>`SELECT payload_json FROM orchestration_v2_projection_runs
            WHERE ${threadColumn(filter.runIds)} = ${threadId}
              ${filter.runIds === undefined ? sql`` : sql`AND run_id IN (SELECT value FROM json_each(${ids(filter.runIds)}))`}
              ${
                filter.runStatuses === undefined
                  ? sql`ORDER BY ordinal ASC`
                  : filter.includeLatestRun !== true
                    ? sql`AND status IN (SELECT value FROM json_each(${ids(filter.runStatuses)}))
                ORDER BY +ordinal ASC`
                    : sql`AND (status IN (SELECT value FROM json_each(${ids(filter.runStatuses)}))
                OR run_id = (
                  SELECT run_id FROM orchestration_v2_projection_runs WHERE thread_id = ${threadId}
                    AND (status <> 'cancelled' OR json_extract(payload_json, '$.startedAt') IS NOT NULL)
                  ORDER BY ordinal DESC LIMIT 1))
                ORDER BY ordinal ASC`
              }`;
                  break;
                case "attempts":
                  selected =
                    yield* sql<PayloadRow>`SELECT payload_json FROM orchestration_v2_projection_run_attempts
            WHERE thread_id = ${threadId} ORDER BY run_id ASC, attempt_ordinal ASC`;
                  break;
                case "nodes":
                  selected =
                    yield* sql<PayloadRow>`SELECT payload_json FROM orchestration_v2_projection_nodes AS node
            WHERE ${threadColumn(filter.nodeIds, "node.thread_id")} = ${threadId}
              ${filter.nodeIds === undefined ? sql`` : sql`AND node.node_id IN (SELECT value FROM json_each(${ids(filter.nodeIds)}))`}
              ${
                filter.omitSettledLeafNodes === true
                  ? sql`AND NOT (
                node.kind IN ('tool_call', 'assistant_message', 'reasoning')
                AND node.status IN ('completed', 'interrupted', 'failed', 'cancelled', 'rolled_back')
                AND node.run_id IS NOT NULL AND NOT EXISTS (
                  SELECT 1 FROM orchestration_v2_projection_runs AS run WHERE run.run_id = node.run_id
                    AND run.status IN ('preparing', 'queued', 'starting', 'running', 'waiting')))`
                  : sql``
              }
            ORDER BY COALESCE(node.started_at, ''), node.node_id ASC`;
                  break;
                case "subagents":
                  selected =
                    yield* sql<PayloadRow>`SELECT payload_json FROM orchestration_v2_projection_subagents AS subagent
            WHERE ${threadColumn(filter.subagentIds)} = ${threadId}
              ${filter.subagentIds === undefined ? sql`` : sql`AND subagent_id IN (SELECT value FROM json_each(${ids(filter.subagentIds)}))`}
              ${filter.subagentRunIds === undefined ? sql`` : sql`AND run_id IN (SELECT value FROM json_each(${ids(filter.subagentRunIds)}))`}
              ${
                filter.omitSettledSubagents === undefined
                  ? sql``
                  : sql`AND (${
                      filter.omitSettledSubagents === true
                        ? sql`0`
                        : sql`subagent_id IN (SELECT value FROM json_each(${ids(filter.omitSettledSubagents.except)}))`
                    } OR NOT (
                  subagent.status IN ('completed', 'failed', 'cancelled', 'interrupted')
                  AND COALESCE(subagent.completion_delivery_state, '') IN ('acknowledged', 'delivered', 'disposed')
                  AND NOT EXISTS (SELECT 1 FROM orchestration_v2_projection_runs AS run WHERE run.run_id = subagent.run_id
                    AND run.status IN ('preparing', 'queued', 'starting', 'running', 'waiting'))))`
              }
            ORDER BY COALESCE(started_at, ''), subagent_id ASC`;
                  break;
                case "providerSessions":
                  selected = yield* sql<PayloadRow>`SELECT sessions.payload_json
            FROM orchestration_v2_projection_provider_sessions AS sessions
            INNER JOIN orchestration_v2_projection_provider_session_bindings AS bindings
              ON bindings.provider_session_id = sessions.provider_session_id
            WHERE bindings.thread_id = ${threadId} ORDER BY sessions.updated_at ASC, sessions.provider_session_id ASC`;
                  break;
                case "providerThreads":
                  selected =
                    yield* sql<PayloadRow>`SELECT payload_json FROM orchestration_v2_projection_provider_threads
            WHERE (thread_id = ${threadId} OR owner_node_id IN (
                SELECT owner.node_id FROM orchestration_v2_projection_provider_threads AS owned
                  INDEXED BY orchestration_v2_projection_provider_threads_owner_idx
                CROSS JOIN orchestration_v2_projection_nodes AS owner ON owner.node_id = owned.owner_node_id
                WHERE owned.owner_node_id IS NOT NULL AND owner.thread_id = ${threadId})
              OR provider_thread_id IN (SELECT provider_thread_id FROM orchestration_v2_projection_subagents
                WHERE thread_id = ${threadId} AND provider_thread_id IS NOT NULL))
              ${filter.providerThreadIds === undefined ? sql`` : sql`AND provider_thread_id IN (SELECT value FROM json_each(${ids(filter.providerThreadIds)}))`}
            ORDER BY COALESCE(first_run_ordinal, 0), provider_thread_id ASC`;
                  break;
                case "providerTurns":
                  selected =
                    yield* sql<PayloadRow>`SELECT payload_json FROM orchestration_v2_projection_provider_turns
            WHERE thread_id = ${threadId}
              ${filter.providerTurnAttemptIds === undefined ? sql`` : sql`AND run_attempt_id IN (SELECT value FROM json_each(${ids(filter.providerTurnAttemptIds)}))`}
              ${filter.providerTurnThreadIds === undefined ? sql`` : sql`AND provider_thread_id IN (SELECT value FROM json_each(${ids(filter.providerTurnThreadIds)}))`}
              ${
                filter.latestProviderTurnUsageOnly === true
                  ? sql`AND json_extract(payload_json, '$.tokenUsage') IS NOT NULL
                ORDER BY julianday(json_extract(payload_json, '$.tokenUsage.updatedAt')) DESC, provider_thread_id ASC, ordinal ASC LIMIT 1`
                  : sql`ORDER BY provider_thread_id ASC, ordinal ASC`
              }`;
                  break;
                case "runtimeRequests":
                  selected =
                    yield* sql<PayloadRow>`SELECT payload_json FROM orchestration_v2_projection_runtime_requests
            WHERE thread_id = ${threadId}
              ${filter.runtimeRequestStatuses === undefined ? sql`` : sql`AND status IN (SELECT value FROM json_each(${ids(filter.runtimeRequestStatuses)}))`}
            ORDER BY created_at ASC, runtime_request_id ASC`;
                  break;
                case "messages":
                  selected =
                    yield* sql<PayloadRow>`SELECT payload_json FROM orchestration_v2_projection_messages
            WHERE ${threadColumn(filter.messageIds)} = ${threadId}
              ${filter.messageIds === undefined ? sql`` : sql`AND message_id IN (SELECT value FROM json_each(${ids(filter.messageIds)}))`}
              ${filter.messageRoles === undefined ? sql`` : sql`AND role IN (SELECT value FROM json_each(${ids(filter.messageRoles)}))`}
              ${filter.monitorNotificationsOnly === true ? sql`AND json_extract(payload_json, '$.notification.source.kind') = 'monitor'` : sql``}
              ${filter.messageRunIds === undefined ? sql`` : sql`AND run_id IN (SELECT value FROM json_each(${ids(filter.messageRunIds)}))`}
            ORDER BY created_at ASC, message_id ASC`;
                  break;
                case "plans":
                  selected =
                    yield* sql<PayloadRow>`SELECT payload_json FROM orchestration_v2_projection_plans
            WHERE thread_id = ${threadId} ORDER BY plan_id ASC`;
                  break;
                case "turnItems":
                  selected =
                    yield* sql<PayloadRow>`SELECT payload_json FROM orchestration_v2_projection_turn_items
            WHERE thread_id = ${threadId}
              ${filter.turnItemRequestIds === undefined ? sql`` : sql`AND json_extract(payload_json, '$.requestId') IN (SELECT value FROM json_each(${ids(filter.turnItemRequestIds)}))`}
              ${filter.turnItemTypes === undefined ? sql`` : sql`AND type IN (SELECT value FROM json_each(${ids(filter.turnItemTypes)}))`}
              ${filter.turnItemStatuses === undefined ? sql`` : sql`AND status IN (SELECT value FROM json_each(${ids(filter.turnItemStatuses)}))`}
              ${filter.turnItemRunId === undefined ? sql`` : sql`AND run_id = ${filter.turnItemRunId}`}
              ${filter.turnItemNodeIds === undefined ? sql`` : sql`AND node_id IN (SELECT value FROM json_each(${ids(filter.turnItemNodeIds)}))`}
              ${
                filter.turnItemRunIds === undefined
                  ? sql``
                  : sql`AND (run_id IN (SELECT value FROM json_each(${ids(filter.turnItemRunIds.filter((id) => id !== null))}))
                OR (${filter.turnItemRunIds.includes(null) ? 1 : 0} = 1 AND run_id IS NULL))`
              }
            ORDER BY ordinal ASC, turn_item_id ASC`;
                  break;
                case "checkpointScopes":
                  selected =
                    yield* sql<PayloadRow>`SELECT payload_json FROM orchestration_v2_projection_checkpoint_scopes
            WHERE thread_id = ${threadId} ORDER BY ordinal_within_parent ASC, scope_id ASC`;
                  break;
                case "checkpoints":
                  selected =
                    yield* sql<PayloadRow>`SELECT payload_json FROM orchestration_v2_projection_checkpoints
            WHERE thread_id = ${threadId} ORDER BY scope_id ASC, ordinal_within_scope ASC`;
                  break;
                case "contextHandoffs":
                  selected =
                    yield* sql<PayloadRow>`SELECT payload_json FROM orchestration_v2_projection_context_handoffs
            WHERE thread_id = ${threadId} ORDER BY rowid ASC`;
                  break;
                case "contextTransfers":
                  selected =
                    yield* sql<PayloadRow>`SELECT payload_json FROM orchestration_v2_projection_context_transfers
            WHERE (source_thread_id = ${threadId} OR target_thread_id = ${threadId})
              ${filter.contextTransferStatuses === undefined ? sql`` : sql`AND status IN (SELECT value FROM json_each(${ids(filter.contextTransferStatuses)}))`}
            ORDER BY rowid ASC`;
                  break;
              }
              rows[name] = selected.map((row) => row.payload_json);
            }
            const messageOrdinals =
              rows.messages.length > 1
                ? yield* sql<{ readonly messageId: string; readonly ordinal: number }>`
        SELECT json_extract(payload_json, '$.messageId') AS "messageId", MIN(ordinal) AS ordinal
        FROM orchestration_v2_projection_turn_items WHERE thread_id = ${threadId}
          AND type IN ('user_message', 'assistant_message')
        GROUP BY json_extract(payload_json, '$.messageId')`
                : [];
            return { thread: thread[0].payload_json, rows, messageOrdinals, deferredWork: [] };
          }),
        )
        .pipe(Effect.mapError((cause) => new ProjectionRecordRowsError({ cause }))),
  } satisfies ProjectionRecordRows;
});
