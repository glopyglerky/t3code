import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import type * as SqlError from "effect/sql/SqlError";
import type { RunAttemptId } from "@t3tools/contracts";
import { MCP_APP_OUTPUT_KEY } from "@t3tools/shared/mcpApp";
import {
  ProjectionContextRowsError,
  type ProjectionContextRows,
  type ProjectionContextQuery,
} from "./ProjectionContextRows.ts";

interface PayloadRow {
  readonly payload_json: string;
}
const javascriptTrimWhitespace =
  " \t\n\r\v\f\u00a0\u1680\u2000\u2001\u2002\u2003\u2004\u2005\u2006\u2007\u2008\u2009\u200a\u2028\u2029\u202f\u205f\u3000\ufeff";

export const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const nodesOwningProviderThreads = (threadId: string) => sql`
      SELECT owner.node_id
      FROM orchestration_v2_projection_provider_threads AS owned
        INDEXED BY orchestration_v2_projection_provider_threads_owner_idx
      CROSS JOIN orchestration_v2_projection_nodes AS owner
        ON owner.node_id = owned.owner_node_id
      WHERE owned.owner_node_id IS NOT NULL AND owner.thread_id = ${threadId}
    `;

  const select = (
    query: ProjectionContextQuery,
  ): Effect.Effect<ReadonlyArray<unknown>, SqlError.SqlError> => {
    const threadId = query.thread_id,
      runId = query.run_id ?? null,
      recordId = query.record_id ?? null;
    const scopeId = query.scope_id ?? null,
      ordinal = query.ordinal ?? null,
      ids = query.ids ?? [];
    const providerThreadId = query.provider_thread_id ?? null,
      providerTurnId = query.provider_turn_id ?? null,
      attemptId = query.attempt_id ?? null;
    switch (query.kind) {
      case "turn_runs":
        return sql<PayloadRow>`
              SELECT payload_json FROM orchestration_v2_projection_runs
              WHERE thread_id = ${threadId} ORDER BY ordinal ASC
            `;
      case "turn_attempts":
        return sql<PayloadRow>`
              SELECT payload_json FROM orchestration_v2_projection_run_attempts
              WHERE thread_id = ${threadId} ORDER BY run_id ASC, attempt_ordinal ASC
            `;
      case "turn_nodes":
        return sql<PayloadRow>`
              SELECT payload_json FROM orchestration_v2_projection_nodes
              -- Look up the root node by key, not every node in the thread.
              INDEXED BY sqlite_autoindex_orchestration_v2_projection_nodes_1
              WHERE thread_id = ${threadId} AND node_id IN (SELECT json_extract(payload_json, '$.rootNodeId') FROM orchestration_v2_projection_runs WHERE thread_id = ${threadId} AND run_id = ${runId}) ORDER BY node_id ASC
            `;
      case "turn_subagents":
        return sql<PayloadRow>`
              SELECT payload_json FROM orchestration_v2_projection_subagents
              WHERE thread_id = ${threadId} AND status NOT IN ('interrupted','failed','cancelled') ORDER BY subagent_id ASC
            `;
      case "turn_provider_threads":
        return sql<PayloadRow>`
              SELECT payload_json FROM orchestration_v2_projection_provider_threads
              WHERE thread_id = ${threadId} ORDER BY COALESCE(first_run_ordinal, 0), provider_thread_id ASC
            `;
      case "turn_provider_turns":
        return sql<PayloadRow>`
              SELECT payload_json FROM orchestration_v2_projection_provider_turns
              WHERE thread_id = ${threadId} ORDER BY provider_thread_id ASC, ordinal ASC
            `;
      case "turn_scopes":
        return sql<PayloadRow>`
              SELECT payload_json FROM orchestration_v2_projection_checkpoint_scopes
              WHERE thread_id = ${threadId} AND scope_id IN ${sql.in(ids)} ORDER BY ordinal_within_parent ASC, scope_id ASC
            `;
      case "turn_handoffs":
        return sql<PayloadRow>`
              SELECT payload_json FROM orchestration_v2_projection_context_handoffs
              WHERE thread_id = ${threadId} ORDER BY rowid ASC
            `;
      case "turn_transfers":
        return sql<PayloadRow>`
              SELECT payload_json FROM orchestration_v2_projection_context_transfers
              WHERE source_thread_id = ${threadId} OR target_thread_id = ${threadId} ORDER BY rowid ASC
            `;
      case "turn_items":
        return sql<PayloadRow>`
              SELECT payload_json FROM orchestration_v2_projection_turn_items
              -- Look up one run's items by key, not by walking the thread's whole history in ordinal order.
              INDEXED BY orchestration_v2_projection_turn_items_run_ordinal_idx
              WHERE thread_id = ${threadId} AND run_id = ${runId} ORDER BY ordinal ASC, turn_item_id ASC
            `;
      case "turn_sessions":
        return sql<PayloadRow>`
              SELECT sessions.payload_json FROM orchestration_v2_projection_provider_sessions AS sessions
              INNER JOIN orchestration_v2_projection_provider_session_bindings AS bindings
                ON bindings.provider_session_id = sessions.provider_session_id
              WHERE bindings.thread_id = ${threadId}
              ORDER BY sessions.updated_at ASC, sessions.provider_session_id ASC
            `;
      case "turn_messages":
        return sql<PayloadRow>`
              SELECT payload_json FROM orchestration_v2_projection_messages
              WHERE thread_id = ${threadId} AND (
                message_id IN (SELECT json_extract(payload_json, '$.userMessageId') FROM orchestration_v2_projection_runs
                  WHERE thread_id = ${threadId} AND run_id = ${runId})
                OR (role = 'user' AND lower(trim(json_extract(payload_json, '$.text'), ${javascriptTrimWhitespace})) = '/compact')
                -- Earlier /goal turns defer context too; parseGoalCommand narrows this prefix match.
                OR (role = 'user' AND trim(json_extract(payload_json, '$.text'), ${javascriptTrimWhitespace}) GLOB '/goal*')
              ) ORDER BY created_at ASC, message_id ASC
            `;
      case "turn_conversation":
        return sql<{ present: number }>`
              SELECT EXISTS(
                SELECT 1 FROM orchestration_v2_projection_messages
                WHERE thread_id = ${threadId} AND role = 'user'
                    AND (lower(trim(json_extract(payload_json, '$.text'), ${javascriptTrimWhitespace})) <> '/compact'
                      OR json_array_length(payload_json, '$.attachments') > 0)
              ) AS present
            `;
      case "thread_exists":
        return sql<{ readonly thread_id: string }>`
            SELECT thread_id FROM orchestration_v2_projection_threads
            WHERE thread_id = ${threadId} LIMIT 1
          `;
      case "checkpoint_runs":
        return sql`
              SELECT run_id AS id, ordinal, status
              FROM orchestration_v2_projection_runs
              WHERE thread_id = ${threadId}
              ORDER BY ordinal ASC
            `;
      case "checkpoint_scopes":
        return sql`
              SELECT scope_id AS id, run_id AS "runId", kind,
                json_extract(payload_json, '$.cwd') AS cwd
              FROM orchestration_v2_projection_checkpoint_scopes
              WHERE thread_id = ${threadId}
              ORDER BY ordinal_within_parent ASC, scope_id ASC
            `;
      case "checkpoint_records":
        return sql`
              SELECT scope_id AS "scopeId", run_id AS "runId",
                ordinal_within_scope AS "ordinalWithinScope",
                app_run_ordinal AS "appRunOrdinal", status,
                json_extract(payload_json, '$.ref') AS ref
              FROM orchestration_v2_projection_checkpoints
              WHERE thread_id = ${threadId}
              ORDER BY scope_id ASC, ordinal_within_scope ASC
            `;
      case "side_chat":
        return sql<{ readonly side_chat: number | null }>`
          SELECT json_extract(payload_json, '$.lineage.sideChat') AS side_chat
          FROM orchestration_v2_projection_threads WHERE thread_id = ${threadId} LIMIT 1`;
      case "run":
        return sql<PayloadRow>`
          SELECT payload_json FROM orchestration_v2_projection_runs
          WHERE thread_id = ${threadId} AND run_id = ${recordId}`;
      case "node":
        return sql<PayloadRow>`SELECT payload_json FROM orchestration_v2_projection_nodes
            WHERE thread_id = ${threadId} AND node_id = ${recordId}`;
      case "scope":
        return sql<PayloadRow>`SELECT payload_json FROM orchestration_v2_projection_checkpoint_scopes
            WHERE thread_id = ${threadId} AND scope_id = ${recordId}`;
      case "provider":
        return sql<PayloadRow>`SELECT payload_json FROM orchestration_v2_projection_provider_threads
            WHERE provider_thread_id = ${recordId}
              AND (thread_id = ${threadId}
                OR owner_node_id IN (${nodesOwningProviderThreads(threadId)})
                OR provider_thread_id IN (SELECT provider_thread_id FROM orchestration_v2_projection_subagents WHERE thread_id = ${threadId}))`;
      case "ready_ordinals":
        return sql<{ readonly ordinal: number }>`SELECT ordinal_within_scope AS ordinal
            FROM orchestration_v2_projection_checkpoints
            WHERE thread_id = ${threadId} AND scope_id = ${scopeId} AND status = 'ready'`;
      case "checkpoint":
        return sql<PayloadRow>`SELECT payload_json FROM orchestration_v2_projection_checkpoints
            WHERE thread_id = ${threadId} AND checkpoint_id = ${recordId}`;
      case "rollback_target_attempt":
        return sql`SELECT attempt.attempt_id AS id, attempt.provider_turn_id AS "providerTurnId"
            FROM orchestration_v2_projection_run_attempts AS attempt
            JOIN orchestration_v2_projection_runs AS run
              ON attempt.attempt_id = json_extract(run.payload_json, '$.activeAttemptId')
            WHERE run.thread_id = ${threadId} AND run.ordinal = ${ordinal}
              AND attempt.thread_id = ${threadId}`;
      case "rollback_runs":
        return sql<PayloadRow>`SELECT payload_json FROM orchestration_v2_projection_runs
            WHERE thread_id = ${threadId} AND ordinal > ${ordinal}
              AND status IN ('completed', 'interrupted', 'failed', 'cancelled')
            ORDER BY ordinal ASC`;
      case "bound_session":
        return sql<PayloadRow>`SELECT session.payload_json
            FROM orchestration_v2_projection_provider_sessions AS session
            JOIN orchestration_v2_projection_provider_session_bindings AS binding
              ON binding.provider_session_id = session.provider_session_id
            WHERE binding.thread_id = ${threadId}
              AND session.provider_session_id = ${recordId}`;
      case "rolledback_attempts":
        return sql<{ readonly id: RunAttemptId }>`SELECT attempt.attempt_id AS id
            FROM orchestration_v2_projection_run_attempts AS attempt
            JOIN orchestration_v2_projection_runs AS run ON run.run_id = attempt.run_id
            WHERE run.thread_id = ${threadId} AND run.status = 'rolled_back'
              AND attempt.thread_id = ${threadId}`;
      case "stale_checkpoints":
        return sql<PayloadRow>`SELECT payload_json FROM orchestration_v2_projection_checkpoints
            WHERE thread_id = ${threadId} AND scope_id = ${scopeId} AND status = 'ready'
              AND app_run_ordinal > ${ordinal}
            ORDER BY ordinal_within_scope ASC`;
      case "rollback_nodes":
        return sql<PayloadRow>`SELECT payload_json FROM orchestration_v2_projection_nodes
            WHERE thread_id = ${threadId} AND ${sql.in("node_id", ids)}
            ORDER BY node_id ASC`;
      case "rollback_turns":
        return sql<PayloadRow>`SELECT payload_json FROM orchestration_v2_projection_provider_turns
            WHERE thread_id = ${threadId} AND (
              provider_thread_id = ${providerThreadId}
              OR provider_turn_id = ${providerTurnId}
              OR run_attempt_id = ${attemptId})
            ORDER BY provider_thread_id ASC, ordinal ASC`;
      case "attachment_messages":
        return sql<{ id: string }>`
      SELECT DISTINCT json_extract(attachment.value, '$.id') AS id
      FROM orchestration_v2_projection_messages AS message,
        json_each(message.payload_json, '$.attachments') AS attachment
      WHERE message.thread_id = ${threadId}
    `;
      case "attachment_renders":
        return sql<{ payload_json: string }>`
      SELECT payload_json
      FROM orchestration_v2_projection_turn_items
      WHERE thread_id = ${threadId}
        AND type = 'dynamic_tool'
        AND (payload_json LIKE '%htmlRender%' OR payload_json LIKE ${`%${MCP_APP_OUTPUT_KEY}%`}
          OR payload_json LIKE '%preview_recording_stop%')
    `;
      case "attachment_answers":
        return sql<{ id: string }>`
      SELECT DISTINCT json_extract(attachment.value, '$.id') AS id
      FROM orchestration_v2_projection_turn_items AS item,
        json_each(item.payload_json, '$.questionAnswer.attachmentsByQuestionId') AS question,
        json_each(question.value) AS attachment
      WHERE item.thread_id = ${threadId}
        AND item.type = 'user_input_request'
    `;
      case "attachment_guests":
        return sql<{ id: string }>`
      SELECT DISTINCT json_extract(attachment.value, '$.id') AS id
      FROM orchestration_v2_projection_threads AS thread,
        json_each(thread.payload_json, '$.guestShare.messages') AS message,
        json_each(message.value, '$.attachments') AS attachment
      WHERE thread.thread_id = ${threadId}
        AND json_extract(message.value, '$.status') = 'pending'
    `;
      case "thread":
        return sql<PayloadRow>`
            SELECT payload_json FROM orchestration_v2_projection_threads
            WHERE thread_id = ${threadId}
          `;
      case "runtime_recovery": {
        const queries = {
          runs: sql<PayloadRow>`
              SELECT payload_json FROM orchestration_v2_projection_runs AS run
              WHERE run.thread_id = ${threadId}
                AND (
                  run.status IN ('queued', 'preparing', 'starting', 'running', 'waiting')
                  OR run.run_id = (
                    SELECT latest.run_id FROM orchestration_v2_projection_runs AS latest
                    WHERE latest.thread_id = ${threadId}
                    ORDER BY latest.ordinal DESC LIMIT 1
                  )
                  -- Retain the latest run for each provider thread with lost
                  -- background work, including owners used before a handoff.
                  OR run.run_id IN (
                    SELECT (
                      SELECT ended.run_id FROM orchestration_v2_projection_runs AS ended
                      WHERE ended.thread_id = ${threadId}
                        AND ended.provider_thread_id = roster.provider_thread_id
                        AND ended.status NOT IN ('queued', 'rolled_back')
                        -- A run that never started (see neverStarted) owns no work.
                        AND NOT (
                          json_type(ended.payload_json, '$.startedAt') = 'null'
                          AND json_type(ended.payload_json, '$.restartContinuationOfRunId') IS NULL
                        )
                      ORDER BY ended.completed_at IS NULL DESC, ended.completed_at DESC,
                        ended.ordinal DESC
                      LIMIT 1
                    )
                    FROM orchestration_v2_projection_provider_threads AS roster
                    WHERE roster.thread_id = ${threadId}
                      AND CASE WHEN json_valid(roster.payload_json)
                        THEN json_array_length(roster.payload_json, '$.pendingBackgroundTasks') > 0
                        ELSE 0 END
                  )
                  OR run.run_id IN (
                    SELECT item.run_id FROM orchestration_v2_projection_turn_items AS item
                    WHERE item.thread_id = ${threadId}
                      AND item.type IN ('command_execution', 'dynamic_tool', 'subagent')
                      AND item.status IN ('pending', 'running', 'waiting')
                      AND item.run_id IS NOT NULL
                  )
                )
              ORDER BY run.ordinal ASC
            `,
          attempts: sql<PayloadRow>`
              SELECT attempt.payload_json
              FROM orchestration_v2_projection_run_attempts AS attempt
              WHERE attempt.thread_id = ${threadId}
                AND attempt.run_id IN (
                  SELECT run_id FROM orchestration_v2_projection_runs
                  WHERE thread_id = ${threadId}
                    AND status IN ('queued', 'preparing', 'starting', 'running', 'waiting')
                )
              ORDER BY attempt.run_id ASC, attempt.attempt_ordinal ASC
            `,
          nodes: sql<PayloadRow>`
              SELECT node.payload_json FROM orchestration_v2_projection_nodes AS node
              WHERE node.thread_id = ${threadId}
                AND node.status IN ('pending', 'starting', 'running', 'waiting')
                AND (
                  (node.run_id IS NULL AND node.kind = 'root_turn')
                  OR node.node_id IN (
                    SELECT node_id FROM orchestration_v2_projection_runtime_requests
                    WHERE thread_id = ${threadId} AND status = 'pending' AND kind = 'user_input'
                  )
                  OR node.run_id IN (
                    SELECT run_id FROM orchestration_v2_projection_runs
                    WHERE thread_id = ${threadId}
                      AND status IN ('queued', 'preparing', 'starting', 'running', 'waiting')
                  )
                  OR node.node_id IN (
                    SELECT item.node_id FROM orchestration_v2_projection_turn_items AS item
                    WHERE item.thread_id = ${threadId}
                      AND item.type IN ('command_execution', 'dynamic_tool', 'subagent')
                      AND item.status IN ('pending', 'running', 'waiting')
                  )
                  OR node.node_id IN (
                    SELECT json_extract(item.payload_json, '$.subagentId')
                    FROM orchestration_v2_projection_turn_items AS item
                    WHERE item.thread_id = ${threadId} AND item.type = 'subagent'
                      AND item.status IN ('pending', 'running', 'waiting')
                  )
                  OR node.node_id IN (
                    SELECT subagent_id FROM orchestration_v2_projection_subagents
                    WHERE thread_id = ${threadId} AND origin = 'provider_native'
                      AND status IN ('pending', 'running', 'waiting')
                  )
                )
              ORDER BY COALESCE(node.started_at, ''), node.node_id ASC
            `,
          subagents: sql<PayloadRow>`
              SELECT subagent.payload_json FROM orchestration_v2_projection_subagents AS subagent
              WHERE subagent.thread_id = ${threadId}
                AND subagent.status IN ('pending', 'starting', 'running', 'waiting')
                AND (
                  subagent.origin = 'provider_native'
                  OR subagent.run_id IN (
                    SELECT run_id FROM orchestration_v2_projection_runs
                    WHERE thread_id = ${threadId}
                      AND status IN ('queued', 'preparing', 'starting', 'running', 'waiting')
                  )
                  OR subagent.subagent_id IN (
                    SELECT json_extract(item.payload_json, '$.subagentId')
                    FROM orchestration_v2_projection_turn_items AS item
                    WHERE item.thread_id = ${threadId} AND item.type = 'subagent'
                      AND item.status IN ('pending', 'running', 'waiting')
                  )
                )
              ORDER BY COALESCE(subagent.started_at, ''), subagent.subagent_id ASC
            `,
          providerSessions: sql<PayloadRow>`
              SELECT session.payload_json
              FROM orchestration_v2_projection_provider_sessions AS session
              WHERE (
                  session.provider_session_id IN (
                    SELECT binding.provider_session_id
                    FROM orchestration_v2_projection_provider_session_bindings AS binding
                    WHERE binding.thread_id = ${threadId}
                  )
                  AND (
                    session.status NOT IN ('stopped', 'error')
                    OR (session.status = 'error' AND CASE WHEN json_valid(session.payload_json)
                      THEN json_array_length(session.payload_json, '$.extensionStatuses') > 0
                      ELSE 0 END)
                    OR session.provider_session_id IN (
                      SELECT provider_thread.provider_session_id
                      FROM orchestration_v2_projection_provider_threads AS provider_thread
                      WHERE provider_thread.provider_thread_id IN (
                        SELECT run.provider_thread_id
                        FROM orchestration_v2_projection_runs AS run
                        WHERE run.thread_id = ${threadId}
                          AND run.status IN ('queued', 'preparing', 'starting', 'running', 'waiting')
                      )
                    )
                  )
                )
                -- This thread detached it, which keeps its status. Its process
                -- did not outlive the server either, so recovery stops it.
                OR (
                  session.thread_id = ${threadId}
                  AND session.status NOT IN ('stopped', 'error')
                  AND NOT EXISTS (
                    SELECT 1 FROM orchestration_v2_projection_provider_session_bindings AS binding
                    WHERE binding.provider_session_id = session.provider_session_id
                  )
                )
              ORDER BY session.updated_at ASC, session.provider_session_id ASC
            `,
          providerThreads: sql<PayloadRow>`
              SELECT provider_thread.payload_json
              FROM orchestration_v2_projection_provider_threads AS provider_thread
              WHERE provider_thread.provider_thread_id IN (
                  SELECT provider_thread_id FROM orchestration_v2_projection_provider_threads
                  WHERE thread_id = ${threadId}
                  UNION
                  -- Start with non-null owners; a thread can have a large settled node history.
                  SELECT owned.provider_thread_id
                  FROM orchestration_v2_projection_provider_threads AS owned
                    INDEXED BY orchestration_v2_projection_provider_threads_owner_idx
                  CROSS JOIN orchestration_v2_projection_nodes AS owner
                    ON owner.node_id = owned.owner_node_id
                  WHERE owned.owner_node_id IS NOT NULL AND owner.thread_id = ${threadId}
                  UNION
                  SELECT provider_thread_id FROM orchestration_v2_projection_subagents
                  WHERE thread_id = ${threadId} AND provider_thread_id IS NOT NULL
                )
                AND (
                  provider_thread.status = 'active'
                  OR CASE WHEN json_valid(provider_thread.payload_json)
                    THEN json_array_length(provider_thread.payload_json, '$.pendingBackgroundTasks') > 0
                    ELSE 0 END
                  OR provider_thread.provider_thread_id IN (
                    SELECT run.provider_thread_id FROM orchestration_v2_projection_runs AS run
                    WHERE run.thread_id = ${threadId}
                      AND run.status IN ('queued', 'preparing', 'starting', 'running', 'waiting')
                  )
                  OR provider_thread.provider_thread_id IN (
                    SELECT item.provider_thread_id
                    FROM orchestration_v2_projection_turn_items AS item
                    WHERE item.thread_id = ${threadId}
                      AND item.status IN ('pending', 'running', 'waiting')
                  )
                  -- A native subagent's item names its own provider thread; the
                  -- run that waits on it (see awaitingProviderThreadId) is woken.
                  OR provider_thread.provider_thread_id IN (
                    SELECT run.provider_thread_id
                    FROM orchestration_v2_projection_turn_items AS item
                    JOIN orchestration_v2_projection_runs AS run ON run.run_id = item.run_id
                    WHERE item.thread_id = ${threadId}
                      AND item.status IN ('pending', 'running', 'waiting')
                  )
                )
              ORDER BY COALESCE(provider_thread.first_run_ordinal, 0), provider_thread.provider_thread_id ASC
            `,
          providerTurns: sql<PayloadRow>`
              SELECT provider_turn.payload_json
              FROM orchestration_v2_projection_provider_turns AS provider_turn
              WHERE provider_turn.thread_id = ${threadId}
                AND provider_turn.status IN ('pending', 'starting', 'running', 'waiting')
                AND (
                  provider_turn.run_attempt_id IN (
                    SELECT attempt_id FROM orchestration_v2_projection_run_attempts
                    WHERE thread_id = ${threadId}
                      AND run_id IN (
                        SELECT run_id FROM orchestration_v2_projection_runs
                        WHERE thread_id = ${threadId}
                          AND status IN ('queued', 'preparing', 'starting', 'running', 'waiting')
                      )
                  )
                  OR (
                    provider_turn.run_attempt_id IS NULL
                    AND provider_turn.node_id IN (
                      SELECT node_id FROM orchestration_v2_projection_nodes
                      WHERE thread_id = ${threadId}
                        AND run_id IS NULL
                        AND kind = 'root_turn'
                        AND status IN ('pending', 'running', 'waiting')
                    )
                  )
                )
              ORDER BY provider_turn.provider_thread_id ASC, provider_turn.ordinal ASC
            `,
          runtimeRequests: sql<PayloadRow>`
              SELECT payload_json FROM orchestration_v2_projection_runtime_requests
              WHERE thread_id = ${threadId} AND status = 'pending'
              ORDER BY created_at ASC, runtime_request_id ASC
            `,
          // CROSS JOIN keeps empty run cohorts out of message history; the run index bounds each lookup.
          messages: sql<PayloadRow>`
              SELECT message.payload_json FROM orchestration_v2_projection_runs AS message_run
              CROSS JOIN orchestration_v2_projection_messages AS message
                INDEXED BY orchestration_v2_projection_messages_run_idx ON message.run_id = message_run.run_id
              WHERE message_run.thread_id = ${threadId}
                AND message_run.status IN ('queued', 'preparing', 'starting', 'running', 'waiting')
                AND message.thread_id = ${threadId} AND message.streaming = 1
              ORDER BY message.created_at ASC, message.message_id ASC
            `,
          turnItems: sql<PayloadRow>`
              SELECT item.payload_json FROM orchestration_v2_projection_turn_items AS item
              WHERE item.thread_id = ${threadId}
                AND item.status IN ('pending', 'running', 'waiting')
                AND (
                  item.run_id IN (
                    SELECT run_id FROM orchestration_v2_projection_runs
                    WHERE thread_id = ${threadId}
                      AND status IN ('queued', 'preparing', 'starting', 'running', 'waiting')
                  )
                  OR item.type IN ('command_execution', 'dynamic_tool', 'subagent')
                  OR (
                    item.type = 'user_input_request'
                    AND json_extract(item.payload_json, '$.requestId') IN (
                      SELECT runtime_request_id FROM orchestration_v2_projection_runtime_requests
                      WHERE thread_id = ${threadId} AND status = 'pending' AND kind = 'user_input'
                    )
                  )
                  OR (
                    item.run_id IS NULL
                    AND item.node_id IN (
                      SELECT node_id FROM orchestration_v2_projection_nodes
                      WHERE thread_id = ${threadId}
                        AND run_id IS NULL
                        AND kind = 'root_turn'
                        AND status IN ('pending', 'running', 'waiting')
                    )
                  )
                )
              ORDER BY item.ordinal ASC, item.turn_item_id ASC
            `,
        };
        const sections = Object.entries(queries).map(
          ([section, query]) => sql`
              SELECT ${section} AS section, payload_json, row_number() OVER () AS position
              FROM (${query})
            `,
        );
        return sql<PayloadRow & { readonly section: string }>`
            ${sql.join(" UNION ALL ", false)(sections)}
            ORDER BY section, position
          `;
      }
    }
  };
  return {
    read: Effect.fn("ProjectionContextRows.read")(
      <A>(query: ProjectionContextQuery, response: Schema.Decoder<A>) =>
        select(query).pipe(
          NodeSqliteClient.readOnly,
          Effect.flatMap(Schema.decodeUnknownEffect(response)),
          Effect.mapError((cause) => new ProjectionContextRowsError({ cause })),
        ),
    ),
    withReadTransaction: sql.withTransaction,
  } satisfies ProjectionContextRows;
});
