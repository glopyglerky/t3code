import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import {
  ProjectionRecoveryRowsError,
  type ProjectionRecoveryRows,
} from "./ProjectionRecoveryRows.ts";
const booleanInt = (value: boolean) => (value ? 1 : 0);
const encodeIdList = Schema.encodeSync(Schema.fromJsonString(Schema.Array(Schema.String)));

/** Original SQLite selectors feeding shared recovery codecs and candidate shaping. */
export const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const reader: ProjectionRecoveryRows = {
    limitCandidates: (options) =>
      sql<{
        readonly payload_json: string;
        readonly run_id: string;
        readonly completed_at: string | null;
        readonly failure_payload_json: string;
        readonly last_error: string | null;
      }>`
          SELECT t.payload_json, r.run_id, r.completed_at,
            item.payload_json AS failure_payload_json,
            (
              SELECT json_extract(session.payload_json, '$.lastError')
              -- Keep bindings outermost: one provider instance can hold every session.
              FROM orchestration_v2_projection_provider_session_bindings binding
              CROSS JOIN orchestration_v2_projection_provider_sessions session
                ON session.provider_session_id = binding.provider_session_id
              WHERE binding.thread_id = t.thread_id
                AND session.provider_instance_id = t.provider_instance_id
              ORDER BY session.updated_at DESC, session.provider_session_id DESC
              LIMIT 1
            ) AS last_error
          FROM orchestration_v2_projection_threads t
          INNER JOIN orchestration_v2_projection_runs r ON r.run_id = (
            SELECT latest.run_id FROM orchestration_v2_projection_runs latest
            WHERE latest.thread_id = t.thread_id
              AND latest.status <> 'queued'
              AND NOT (
                latest.status = 'cancelled'
                AND json_extract(latest.payload_json, '$.startedAt') IS NULL
              )
            -- latestExecutedRun: the run that ended last (runRanAfter).
            ORDER BY latest.completed_at IS NULL DESC, latest.completed_at DESC,
              latest.ordinal DESC, latest.run_id DESC
            LIMIT 1
          ) AND r.status = 'failed'
          INNER JOIN orchestration_v2_projection_turn_items item ON item.turn_item_id = (
            SELECT error.turn_item_id FROM orchestration_v2_projection_turn_items error
            WHERE error.thread_id = t.thread_id AND error.run_id = r.run_id
              AND error.type = 'error' AND error.status = 'failed'
              AND error.node_id IS json_extract(r.payload_json, '$.rootNodeId')
            ORDER BY error.updated_at DESC, error.ordinal DESC, error.turn_item_id DESC
            LIMIT 1
          )
          WHERE t.deleted_at IS NULL
            -- The sweep runs every scheduler tick. Without this list the latest-run
            -- lookup above sorts every run of every thread, tens of milliseconds
            -- for one long thread; only threads with a usage-limit failure qualify.
            AND t.thread_id IN (
              SELECT failure.thread_id FROM orchestration_v2_projection_turn_items failure
              WHERE failure.type = 'error' AND failure.status = 'failed'
                AND json_extract(failure.payload_json, '$.failure.class') = 'usage_limit'
            )
            AND json_extract(t.payload_json, '$.archivedAt') IS NULL
            AND json_extract(t.payload_json, '$.settledOverride') IS NOT 'settled'
            AND json_extract(item.payload_json, '$.failure.class') = 'usage_limit'
            AND json_extract(item.payload_json, '$.failure.resetAt') IS NOT NULL
            AND julianday(json_extract(item.payload_json, '$.failure.resetAt')) > julianday(COALESCE(r.completed_at, json_extract(t.payload_json, '$.updatedAt')))
            AND (
              (
                json_extract(t.payload_json, '$.limitRecovery.runId') IS r.run_id
                AND json_extract(t.payload_json, '$.limitRecovery.resetAt') IS json_extract(item.payload_json, '$.failure.resetAt')
                AND json_extract(t.payload_json, '$.limitRecovery.autoResume') = 1
                AND julianday(json_extract(item.payload_json, '$.failure.resetAt')) <= julianday(${DateTime.formatIso(options.now)})
                AND (
                  json_extract(t.payload_json, '$.snoozedUntil') IS NULL
                  OR julianday(json_extract(t.payload_json, '$.snoozedUntil')) <= julianday(${DateTime.formatIso(options.now)})
                )
              )
              OR (
                (
                  json_extract(t.payload_json, '$.limitRecovery.runId') IS NOT r.run_id
                  OR json_extract(t.payload_json, '$.limitRecovery.resetAt') IS NOT json_extract(item.payload_json, '$.failure.resetAt')
                )
                AND (
                  ${booleanInt(options.autoResume)}
                  OR (${booleanInt(options.snooze)} AND julianday(json_extract(item.payload_json, '$.failure.resetAt')) > julianday(${DateTime.formatIso(options.now)}))
                )
                -- A subagent of a settled, archived or deleted parent is abandoned work.
                AND NOT EXISTS (
                  SELECT 1 FROM orchestration_v2_projection_threads parent
                  WHERE json_extract(t.payload_json, '$.lineage.relationshipToParent') = 'subagent'
                    AND parent.thread_id = json_extract(t.payload_json, '$.lineage.parentThreadId')
                    AND (
                      json_extract(parent.payload_json, '$.settledOverride') = 'settled'
                      OR json_extract(parent.payload_json, '$.archivedAt') IS NOT NULL
                      OR parent.deleted_at IS NOT NULL
                    )
                )
              )
            )
            AND NOT EXISTS (
              SELECT 1 FROM orchestration_v2_projection_runtime_requests request
              WHERE request.thread_id = t.thread_id AND request.status = 'pending'
            )
          ORDER BY t.thread_id
        `.pipe(Effect.mapError((cause) => new ProjectionRecoveryRowsError({ cause }))),
    wakeAt: (nowIso) =>
      sql<{
        readonly reset_ms: number | null;
        readonly snooze_ms: number | null;
      }>`
          SELECT
            MIN(CASE WHEN julianday(reset_at) > julianday(${nowIso})
              THEN (julianday(reset_at) - 2440587.5) * 86400000 END) AS reset_ms,
            MIN(CASE WHEN julianday(snoozed_until) > julianday(${nowIso})
              THEN (julianday(snoozed_until) - 2440587.5) * 86400000 END) AS snooze_ms
          FROM (
            SELECT json_extract(payload_json, '$.limitRecovery.resetAt') AS reset_at,
              json_extract(payload_json, '$.snoozedUntil') AS snoozed_until
            FROM orchestration_v2_projection_threads
            WHERE deleted_at IS NULL
              AND json_extract(payload_json, '$.limitRecovery.autoResume') = 1
          )
        `.pipe(Effect.mapError((cause) => new ProjectionRecoveryRowsError({ cause }))),
    recoveryThreads: Effect.fn("SqliteProjectionRecoveryRows.recoveryThreads")(
      function* (
        kind: Parameters<ProjectionRecoveryRows["recoveryThreads"]>[0],
        projects?: Parameters<ProjectionRecoveryRows["recoveryThreads"]>[1],
      ) {
        const candidates = (() => {
          switch (kind) {
            case "queued-runs":
              return sql`
                SELECT thread_id FROM orchestration_v2_projection_runs
                WHERE status = 'queued'
                  AND NOT EXISTS (
                    SELECT 1 FROM orchestration_v2_projection_runs AS active
                    WHERE active.thread_id = orchestration_v2_projection_runs.thread_id
                      AND active.status IN ('preparing', 'starting', 'running', 'waiting')
                  )
              `;
            case "delegated-completions":
              return sql`
                SELECT thread_id FROM orchestration_v2_projection_runs
                WHERE CASE WHEN json_valid(payload_json)
                  THEN json_type(payload_json, '$.delegatedCompletion.delivery') = 'object'
                  ELSE 0 END
              `;
            case "subagent-results":
              return sql`
                SELECT child.thread_id FROM orchestration_v2_projection_threads AS child
                WHERE CASE WHEN json_valid(child.payload_json) THEN
                  json_extract(child.payload_json, '$.lineage.relationshipToParent') = 'subagent'
                  AND json_extract(child.payload_json, '$.lineage.parentThreadId') IS NOT NULL
                  AND json_extract(child.payload_json, '$.forkedFrom.type') = 'node'
                  ELSE 0 END
                  AND (EXISTS (
                    SELECT 1 FROM orchestration_v2_projection_runs AS requested
                    JOIN orchestration_v2_projection_subagents AS task
                      ON task.subagent_id = json_extract(requested.payload_json, '$.delegatedTaskId')
                    WHERE requested.thread_id = child.thread_id
                      AND requested.status IN ('completed', 'interrupted', 'failed', 'cancelled', 'rolled_back')
                      AND task.child_thread_id = child.thread_id
                      AND json_extract(task.payload_json, '$.origin') = 'app_owned'
                      AND json_extract(task.payload_json, '$.result') IS NULL
                  ) OR (
                    -- The original task still waits for the latest unheld work.
                    (SELECT status FROM orchestration_v2_projection_runs
                     WHERE thread_id = child.thread_id
                       AND NOT (status = 'queued' AND json_extract(payload_json, '$.queueHeld') IS 1)
                     ORDER BY ordinal DESC LIMIT 1
                    ) IN ('completed', 'interrupted', 'failed', 'cancelled', 'rolled_back')
                    AND NOT EXISTS (
                    SELECT 1 FROM orchestration_v2_projection_context_transfers
                    WHERE source_thread_id = child.thread_id
                      AND target_thread_id = json_extract(child.payload_json, '$.lineage.parentThreadId')
                      AND type = 'subagent_result'
                  )))
              `;
            case "restart-continuations":
              return sql`
                SELECT bindings.thread_id
                FROM orchestration_v2_projection_provider_sessions AS sessions
                JOIN orchestration_v2_projection_provider_session_bindings AS bindings
                  ON bindings.provider_session_id = sessions.provider_session_id
                WHERE sessions.status NOT IN ('stopped', 'error')
                UNION
                SELECT thread_id FROM orchestration_v2_projection_provider_sessions AS sessions
                WHERE status NOT IN ('stopped', 'error') AND thread_id IS NOT NULL
                  AND NOT EXISTS (
                    SELECT 1 FROM orchestration_v2_projection_provider_session_bindings AS bindings
                    WHERE bindings.provider_session_id = sessions.provider_session_id
                  )
                UNION
                SELECT thread_id FROM orchestration_v2_projection_runs
                WHERE status = 'starting'
                  AND CASE WHEN json_valid(payload_json)
                    THEN json_extract(payload_json, '$.restartContinuationOfRunId') IS NOT NULL
                    ELSE 0 END
              `;
            case "runtime":
              return sql`
                WITH pending_provider_threads AS MATERIALIZED (
                  SELECT provider_thread_id, thread_id, owner_node_id
                  FROM orchestration_v2_projection_provider_threads
                  WHERE status = 'active'
                    OR CASE WHEN json_valid(payload_json)
                      THEN json_array_length(payload_json, '$.pendingBackgroundTasks') > 0
                      ELSE 0 END
                )
                SELECT thread_id FROM orchestration_v2_projection_runs
                WHERE status IN ('preparing', 'starting', 'running', 'waiting')
                UNION
                -- A held queue already went through recovery; rereading it on
                -- every boot costs a projection read per held thread.
                SELECT thread_id FROM orchestration_v2_projection_runs
                WHERE status = 'queued'
                  AND CASE WHEN json_valid(payload_json)
                    THEN json_extract(payload_json, '$.queueHeld') IS NOT 1
                    ELSE 1 END
                UNION
                SELECT thread_id FROM orchestration_v2_projection_runtime_requests
                WHERE status = 'pending'
                UNION
                SELECT bindings.thread_id
                FROM orchestration_v2_projection_provider_sessions AS sessions
                CROSS JOIN orchestration_v2_projection_provider_session_bindings AS bindings
                  ON bindings.provider_session_id = sessions.provider_session_id
                WHERE sessions.status NOT IN ('stopped', 'error')
                  OR (sessions.status = 'error' AND CASE WHEN json_valid(sessions.payload_json)
                    THEN json_array_length(sessions.payload_json, '$.extensionStatuses') > 0
                    ELSE 0 END)
                UNION
                -- A detach drops the binding but keeps the session's status.
                SELECT sessions.thread_id
                FROM orchestration_v2_projection_provider_sessions AS sessions
                WHERE sessions.status NOT IN ('stopped', 'error')
                  AND sessions.thread_id IS NOT NULL
                  AND NOT EXISTS (
                    SELECT 1 FROM orchestration_v2_projection_provider_session_bindings AS bindings
                    WHERE bindings.provider_session_id = sessions.provider_session_id
                  )
                UNION
                SELECT thread_id FROM pending_provider_threads
                UNION
                SELECT nodes.thread_id FROM pending_provider_threads
                CROSS JOIN orchestration_v2_projection_nodes AS nodes
                  ON nodes.node_id = pending_provider_threads.owner_node_id
                UNION
                SELECT subagents.thread_id FROM pending_provider_threads
                CROSS JOIN orchestration_v2_projection_subagents AS subagents
                  ON subagents.provider_thread_id = pending_provider_threads.provider_thread_id
                WHERE subagents.origin = 'provider_native'
                UNION
                SELECT subagents.child_thread_id FROM orchestration_v2_projection_subagents AS subagents
                WHERE subagents.child_thread_id IS NOT NULL
                  AND EXISTS (
                    SELECT 1 FROM orchestration_v2_projection_nodes AS node
                      INDEXED BY orchestration_v2_projection_nodes_live_idx
                    WHERE node.thread_id = subagents.child_thread_id
                      AND node.status IN ('pending', 'starting', 'running', 'waiting')
                      AND node.run_id IS NULL
                      AND node.kind = 'root_turn'
                  )
                UNION
                -- No native subagent outlives the provider process that ran it.
                SELECT thread_id FROM orchestration_v2_projection_subagents
                WHERE origin = 'provider_native'
                  AND status IN ('pending', 'running', 'waiting')
                UNION
                SELECT item.thread_id FROM orchestration_v2_projection_turn_items AS item
                WHERE NOT EXISTS (
                    SELECT 1 FROM orchestration_v2_projection_runs AS run
                    WHERE run.run_id = item.run_id AND run.status = 'rolled_back'
                  )
                  AND type IN ('command_execution', 'dynamic_tool', 'subagent')
                  AND status IN ('pending', 'running', 'waiting')
                UNION
                SELECT thread_id FROM orchestration_v2_effect_outbox
                WHERE status IN ('pending', 'running')
              `;
          }
        })();
        const rows = yield* sql<{ readonly thread_id: string }>`
          SELECT thread_id FROM orchestration_v2_projection_threads
          WHERE deleted_at IS NULL
            AND thread_id IN (${candidates})
            ${projects?.include === undefined ? sql`` : sql`AND project_id IN (SELECT value FROM json_each(${encodeIdList(projects.include)}))`}
            ${projects?.exclude === undefined ? sql`` : sql`AND project_id NOT IN (SELECT value FROM json_each(${encodeIdList(projects.exclude)}))`}
            ${kind === "queued-runs" ? sql`AND archived_at IS NULL` : sql``}
          ORDER BY updated_at ASC, thread_id ASC
        `;

        return rows;
      },
      Effect.mapError((cause) => new ProjectionRecoveryRowsError({ cause })),
    ),
    unreadablePage: (name, id, after, limit) => {
      const table = { name, id },
        afterRowId = Number(after ?? 0),
        pageSize = limit;
      return sql<{
        readonly row_id: number;
        readonly entity_id: string;
        readonly thread_id: string | null;
        readonly target_thread_id: string | null;
        readonly payload_json: string;
      }>`
              SELECT rowid AS row_id, ${sql(table.id)} AS entity_id,
                ${sql(table.name === "context_transfers" ? "source_thread_id" : "thread_id")} AS thread_id,
                ${table.name === "context_transfers" ? sql`target_thread_id` : sql`NULL`} AS target_thread_id,
                payload_json
              FROM ${sql(`orchestration_v2_projection_${table.name}`)}
              WHERE rowid > ${afterRowId}
              ORDER BY rowid ASC LIMIT ${pageSize}
            `.pipe(
        Effect.mapError((cause) => new ProjectionRecoveryRowsError({ cause })),
        Effect.map((rows) => ({
          rows: rows.map((row) => ({ ...row, row_id: String(row.row_id) })),
          next: rows.length < pageSize ? null : String(rows[rows.length - 1]!.row_id),
        })),
      );
    },
    bindings: (entity_id) => {
      const row = { entity_id };
      return sql<{ readonly thread_id: string }>`
                  SELECT thread_id FROM orchestration_v2_projection_provider_session_bindings
                  WHERE provider_session_id = ${row.entity_id}
                `.pipe(Effect.mapError((cause) => new ProjectionRecoveryRowsError({ cause })));
    },
    providerThreadOwners: (entity_id) => {
      const row = { entity_id };
      return sql<{ readonly thread_id: string }>`
                  SELECT thread_id FROM orchestration_v2_projection_nodes
                  WHERE node_id = (
                    SELECT owner_node_id FROM orchestration_v2_projection_provider_threads
                    WHERE provider_thread_id = ${row.entity_id}
                  )
                  UNION
                  SELECT thread_id FROM orchestration_v2_projection_subagents
                  WHERE provider_thread_id = ${row.entity_id}
                `.pipe(Effect.mapError((cause) => new ProjectionRecoveryRowsError({ cause })));
    },
  };
  return reader;
});
