import { IDLE_COMPACTION_TEXT, type ThreadId, type GuestShareId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import type {
  ShellThreadRow,
  ShellRunRow,
  ShellRunItemCountRow,
  ShellItemCountRow,
  ShellPendingTurnItemRow,
  SettlementThreadRow,
  ShellSnapshotOptions,
  OlderSettledThreadsQuery,
} from "./ProjectionStore.ts";
import type { ProjectionShellRows } from "./ProjectionShellRows.ts";
type PayloadRow = { readonly payload_json: string };
const encodeIdList = Schema.encodeSync(Schema.fromJsonString(Schema.Array(Schema.String)));

export const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  // Mirror idleCompactionOf, including JavaScript trim whitespace.
  const isIdleCompactionMessage = (alias: string) => {
    const payload = sql.literal(`${alias}.payload_json`);
    return sql`(
        ${sql.literal(`${alias}.role`)} = 'user'
        AND lower(trim(json_extract(${payload}, '$.text'),
          char(9, 10, 11, 12, 13, 32, 160, 5760, 8192, 8193, 8194, 8195,
            8196, 8197, 8198, 8199, 8200, 8201, 8202, 8232, 8233, 8239, 8287, 12288, 65279)
        )) = ${IDLE_COMPACTION_TEXT}
        AND coalesce(json_array_length(${payload}, '$.attachments'), 0) = 0
        AND (
          json_extract(${payload}, '$.idleCompaction') IS NOT NULL
          OR (
            json_extract(${payload}, '$.createdBy') = 'agent'
            AND json_extract(${payload}, '$.creationSource') = 'server'
          )
        )
      )`;
  };
  // Mirrors isSilentRunForShell.
  const isSilentShellRun = (alias: string) => {
    const run = sql.literal(alias);
    return sql`(
        ${run}.status <> 'failed'
        AND EXISTS (
          SELECT 1 FROM orchestration_v2_projection_messages prompt
          WHERE prompt.message_id = json_extract(${run}.payload_json, '$.userMessageId')
            AND ${isIdleCompactionMessage("prompt")}
        ) AND NOT EXISTS (
          SELECT 1 FROM orchestration_v2_projection_turn_items compaction
          WHERE compaction.thread_id = ${run}.thread_id AND compaction.run_id = ${run}.run_id
            AND compaction.type = 'compaction'
            AND compaction.status = 'failed'
        )
      )`;
  };

  const selectShellThreadRows = (
    threadId?: ThreadId,
    {
      location,
      projectId,
      unsettledOnly = false,
      familyOf,
      worktreePath,
      threadIds,
      listedOnly = false,
    }: ShellSnapshotOptions & { readonly listedOnly?: boolean } = {},
  ) =>
    sql<ShellThreadRow>`
            SELECT
              t.thread_id,
              t.payload_json,
              CASE
                WHEN json_extract(t.payload_json, '$.forkedFrom.type') = 'run'
                  AND json_extract(t.payload_json, '$.lineage.sideChat') IS NOT 1
                  THEN json_extract(t.payload_json, '$.forkedFrom.threadId')
                ELSE NULL
              END AS forked_from_run_source_thread_id,
              presented.run_id AS latest_run_id,
              presented.status AS latest_run_status,
              presented.requested_at AS latest_run_requested_at,
              json_extract(presented.payload_json, '$.startedAt') AS latest_run_started_at,
              presented.completed_at AS latest_run_completed_at,
              EXISTS (
                SELECT 1 FROM orchestration_v2_projection_turn_items stop_request
                WHERE stop_request.thread_id = t.thread_id AND stop_request.run_id = presented.run_id
                  AND stop_request.type = 'run_interrupt_request'
              ) AS latest_run_stop_requested,
              active.run_id AS active_run_id,
              active.status AS active_run_status,
              (
                SELECT r.status
                FROM orchestration_v2_projection_runs r
                WHERE r.thread_id = t.thread_id
                  AND r.status IN ('preparing', 'starting', 'running', 'waiting')
                  AND NOT ${isSilentShellRun("r")}
                ORDER BY r.ordinal DESC, r.run_id DESC
                LIMIT 1
              ) AS activity_run_status,
              (
                -- Mirrors orchestrationV2RunWorkStartedAt.
                SELECT COALESCE(
                  json_extract(r.payload_json, '$.workStartedAt'),
                  json_extract(r.payload_json, '$.startedAt'),
                  r.requested_at
                )
                FROM orchestration_v2_projection_runs r
                WHERE r.thread_id = t.thread_id
                  AND r.status IN ('preparing', 'starting', 'running', 'waiting')
                  AND NOT ${isSilentShellRun("r")}
                ORDER BY r.ordinal DESC, r.run_id DESC
                LIMIT 1
              ) AS activity_run_started_at,
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
              ) AS last_error,
              (
                SELECT item.payload_json
                FROM orchestration_v2_projection_turn_items item
                INNER JOIN orchestration_v2_projection_runs r ON r.run_id = item.run_id
                WHERE r.run_id = presented.run_id
                  AND r.status = 'failed'
                  AND item.thread_id = t.thread_id
                  AND item.type = 'error' AND item.status = 'failed'
                  AND item.node_id IS json_extract(r.payload_json, '$.rootNodeId')
                ORDER BY item.updated_at DESC, item.ordinal DESC, item.turn_item_id DESC
                LIMIT 1
              ) AS terminal_failure_payload_json,
              blocked.run_id AS blocking_run_id,
              blocked.requested_at AS blocking_run_requested_at,
              json_extract(blocked.payload_json, '$.startedAt') AS blocking_run_started_at,
              blocked.completed_at AS blocking_run_completed_at,
              EXISTS (
                SELECT 1 FROM orchestration_v2_projection_turn_items stop_request
                WHERE stop_request.thread_id = t.thread_id AND stop_request.run_id = blocked.run_id
                  AND stop_request.type = 'run_interrupt_request'
              ) AS blocking_run_stop_requested,
              (
                SELECT item.payload_json
                FROM orchestration_v2_projection_turn_items item
                WHERE item.thread_id = t.thread_id
                  AND item.run_id = blocked.run_id
                  AND item.type = 'error' AND item.status = 'failed'
                  AND item.node_id IS json_extract(blocked.payload_json, '$.rootNodeId')
                ORDER BY item.updated_at DESC, item.ordinal DESC, item.turn_item_id DESC
                LIMIT 1
              ) AS blocking_failure_payload_json,
              (
                SELECT request.payload_json
                FROM orchestration_v2_projection_runtime_requests request
                WHERE request.thread_id = t.thread_id
                  AND request.status = 'pending'
                ORDER BY (request.kind = 'user_input') ASC,
                  request.created_at DESC, request.runtime_request_id DESC
                LIMIT 1
              ) AS pending_request_payload_json,
              (
                SELECT secret.payload_json
                -- Keep runs outermost so completed history is never scanned for a pending secret.
                FROM orchestration_v2_projection_runs r
                CROSS JOIN orchestration_v2_projection_turn_items secret
                  INDEXED BY orchestration_v2_projection_turn_items_thread_run_idx
                WHERE r.thread_id = t.thread_id
                  AND r.status IN ('preparing', 'starting', 'running', 'waiting')
                  AND secret.thread_id = t.thread_id AND secret.run_id = r.run_id
                  AND secret.type = 'secret_request' AND secret.status = 'waiting'
                ORDER BY secret.updated_at DESC, secret.turn_item_id DESC
                LIMIT 1
              ) AS pending_secret_request_payload_json,
              (
                SELECT link.payload_json
                FROM orchestration_v2_projection_turn_items link
                  INDEXED BY orchestration_v2_projection_turn_items_waiting_link_idx
                WHERE link.thread_id = t.thread_id
                  AND link.type = 'link_request' AND link.status = 'waiting'
                ORDER BY link.updated_at DESC, link.turn_item_id DESC
                LIMIT 1
              ) AS pending_link_request_payload_json,
              (
                SELECT message.updated_at
                FROM orchestration_v2_projection_messages message
                WHERE message.thread_id = t.thread_id
                  AND message.role = 'user'
                  AND NOT ${isIdleCompactionMessage("message")}
                ORDER BY message.updated_at DESC, message.message_id DESC
                LIMIT 1
              ) AS latest_user_message_at,
              (
                SELECT message.updated_at
                FROM orchestration_v2_projection_messages message
                WHERE message.thread_id = t.thread_id
                  AND message.role = 'user'
                  AND json_extract(message.payload_json, '$.createdBy') = 'user'
                  AND NOT ${isIdleCompactionMessage("message")}
                ORDER BY message.updated_at DESC, message.message_id DESC
                LIMIT 1
              ) AS latest_user_authored_message_at,
              EXISTS (
                SELECT 1
                FROM orchestration_v2_projection_plans plan
                WHERE plan.thread_id = t.thread_id
                  AND plan.kind = 'proposed_plan'
                  AND plan.status = 'active'
              ) AS has_actionable_proposed_plan
            FROM orchestration_v2_projection_threads t
            LEFT JOIN orchestration_v2_projection_runs active ON active.run_id = (
              SELECT r.run_id
              FROM orchestration_v2_projection_runs r
              WHERE r.thread_id = t.thread_id
                AND r.status IN ('preparing', 'starting', 'running')
                AND NOT ${isSilentShellRun("r")}
              ORDER BY r.ordinal DESC, r.run_id DESC
              LIMIT 1
            )
            -- The newest run not waiting in a held queue, matching latestUnheldRun.
            LEFT JOIN orchestration_v2_projection_runs presented ON presented.run_id = (
              SELECT candidate.run_id
              FROM orchestration_v2_projection_runs candidate
              WHERE candidate.thread_id = t.thread_id
                AND NOT (
                  candidate.status = 'queued'
                  AND json_extract(candidate.payload_json, '$.queueHeld') IS 1
                )
                AND NOT ${isSilentShellRun("candidate")}
              ORDER BY candidate.ordinal DESC, candidate.run_id DESC
              LIMIT 1
            )
            LEFT JOIN orchestration_v2_projection_runs blocked ON blocked.run_id = (
              SELECT candidate.run_id
              FROM orchestration_v2_projection_runs candidate
              WHERE candidate.thread_id = t.thread_id
                AND candidate.status <> 'queued'
                AND NOT (
                  candidate.status = 'cancelled'
                  AND json_extract(candidate.payload_json, '$.startedAt') IS NULL
                )
              -- latestExecutedRun: the run that ended last (runRanAfter).
              ORDER BY candidate.completed_at IS NULL DESC, candidate.completed_at DESC,
                candidate.ordinal DESC, candidate.run_id DESC
              LIMIT 1
            ) AND blocked.status = 'failed'
            WHERE t.deleted_at IS NULL${
              listedOnly
                ? sql` AND json_extract(t.payload_json, '$.lineage.relationshipToParent') IS NOT 'subagent'
                    AND json_extract(t.payload_json, '$.lineage.sideChat') IS NOT 1`
                : sql``
            }${threadId === undefined ? sql`` : sql` AND t.thread_id = ${threadId}`}${
              projectId === undefined ? sql`` : sql` AND t.project_id = ${projectId}`
            }${
              location === "active"
                ? sql` AND json_extract(t.payload_json, '$.archivedAt') IS NULL`
                : location === "archive"
                  ? sql` AND json_extract(t.payload_json, '$.archivedAt') IS NOT NULL`
                  : sql``
            }${
              unsettledOnly
                ? sql` AND json_extract(t.payload_json, '$.settledAt') IS NULL AND json_extract(t.payload_json, '$.settledOverride') IS NOT 'settled'`
                : sql``
            }${
              familyOf === undefined
                ? sql``
                : sql` AND json_extract(t.payload_json, '$.lineage.rootThreadId') = (
                    SELECT json_extract(member.payload_json, '$.lineage.rootThreadId')
                    FROM orchestration_v2_projection_threads member
                    WHERE member.thread_id = ${familyOf}
                  )`
            }${
              worktreePath === undefined
                ? sql``
                : sql` AND json_extract(t.payload_json, '$.worktreePath') = ${worktreePath}`
            }${threadIds === undefined ? sql`` : sql` AND t.thread_id IN (SELECT value FROM json_each(${encodeIdList(threadIds)}))`}
            ORDER BY t.updated_at ASC, t.thread_id ASC
          `;

  const selectShellRunRows = (threadIds?: ReadonlyArray<ThreadId>) =>
    threadIds === undefined
      ? sql<ShellRunRow>`
            SELECT thread_id, run_id, ordinal
            FROM orchestration_v2_projection_runs
          `
      : sql<ShellRunRow>`
            SELECT thread_id, run_id, ordinal
            FROM orchestration_v2_projection_runs
            WHERE thread_id IN (SELECT value FROM json_each(${encodeIdList(threadIds)}))
          `;

  const selectShellRunItemCounts = (threadIds: ReadonlyArray<ThreadId>) =>
    sql<ShellRunItemCountRow>`
        SELECT thread_id, run_id, COUNT(*) AS item_count
        FROM orchestration_v2_projection_turn_items
        WHERE run_id IS NOT NULL AND thread_id IN (SELECT value FROM json_each(${encodeIdList(threadIds)}))
        GROUP BY thread_id, run_id
      `;

  // Count all selected threads in one index walk; fork sources still need per-run counts.
  const selectShellItemCounts = (threadIds: ReadonlyArray<ThreadId>) =>
    sql<ShellItemCountRow>`
        SELECT counts.thread_id,
          SUM(CASE WHEN counts.run_id IS NULL OR r.status <> 'rolled_back'
            THEN counts.item_count ELSE 0 END) AS item_count,
          SUM(CASE WHEN counts.run_id IS NULL THEN counts.item_count ELSE 0 END)
            AS runless_item_count
        FROM (
          SELECT thread_id, run_id, COUNT(*) AS item_count
          FROM orchestration_v2_projection_turn_items
          WHERE thread_id IN (SELECT value FROM json_each(${encodeIdList(threadIds)}))
          GROUP BY thread_id, run_id
        ) counts
        LEFT JOIN orchestration_v2_projection_runs r ON r.run_id = counts.run_id
        GROUP BY counts.thread_id
      `;

  const selectShellProviderThreadRows = (threadIds?: ReadonlyArray<ThreadId>) =>
    threadIds === undefined
      ? sql<PayloadRow & { readonly thread_id: string }>`
            SELECT thread_id, payload_json
            FROM orchestration_v2_projection_provider_threads
            WHERE thread_id IS NOT NULL
          `
      : sql<PayloadRow & { readonly thread_id: string }>`
            SELECT thread_id, payload_json
            FROM orchestration_v2_projection_provider_threads
            WHERE thread_id IN (SELECT value FROM json_each(${encodeIdList(threadIds)}))
          `;

  const selectShellPendingTurnItemRows = (
    threadIds?: ReadonlyArray<ThreadId>,
    activeRunIds: ReadonlyArray<string> = [],
  ) => sql<ShellPendingTurnItemRow>`
      SELECT i.thread_id, i.payload_json, p.provider_instance_id AS owning_provider_instance_id
      FROM orchestration_v2_projection_turn_items i
      LEFT JOIN orchestration_v2_projection_provider_threads p
        ON p.provider_thread_id = i.provider_thread_id
      LEFT JOIN orchestration_v2_projection_runs r
        ON r.run_id = i.run_id
      WHERE i.type IN ('command_execution', 'dynamic_tool', 'subagent')
        AND i.status NOT IN ('completed', 'interrupted', 'failed', 'cancelled')
        AND (i.run_id IS NULL OR r.status <> 'rolled_back')
        ${threadIds === undefined ? sql`` : sql`AND i.thread_id IN (SELECT value FROM json_each(${encodeIdList(threadIds)}))`}
      ${
        activeRunIds.length === 0
          ? sql``
          : sql`
        -- Preserve the pending-item partial index; read other work only from running runs.
        UNION ALL
        SELECT i.thread_id, i.payload_json, NULL AS owning_provider_instance_id
        FROM orchestration_v2_projection_turn_items i
        WHERE i.run_id IN (SELECT value FROM json_each(${encodeIdList(activeRunIds)}))
          AND i.type IN ('file_change', 'file_search', 'web_search', 'compaction')
          AND i.status IN ('running', 'waiting')
      `
      }
    `;

  const selectSettlementThreads = (threadId?: ThreadId) => sql<SettlementThreadRow>`
            SELECT t.thread_id, t.payload_json,
              r.run_id AS latest_run_id,
              r.status AS latest_run_status,
              r.requested_at AS latest_run_requested_at,
              json_extract(r.payload_json, '$.startedAt') AS latest_run_started_at,
              r.completed_at AS latest_run_completed_at,
              (
                SELECT message.updated_at
                FROM orchestration_v2_projection_messages message
                WHERE message.thread_id = t.thread_id AND message.role = 'user'
                  AND NOT ${isIdleCompactionMessage("message")}
                ORDER BY message.updated_at DESC, message.message_id DESC
                LIMIT 1
              ) AS latest_user_message_at,
              (
                SELECT message.updated_at
                FROM orchestration_v2_projection_messages message
                WHERE message.thread_id = t.thread_id AND message.role = 'user'
                  AND json_extract(message.payload_json, '$.createdBy') = 'user'
                  AND NOT ${isIdleCompactionMessage("message")}
                ORDER BY message.updated_at DESC, message.message_id DESC
                LIMIT 1
              ) AS latest_user_authored_message_at
            FROM orchestration_v2_projection_threads t
            LEFT JOIN orchestration_v2_projection_runs r ON r.run_id = (
              SELECT latest.run_id FROM orchestration_v2_projection_runs latest
              WHERE latest.thread_id = t.thread_id
                AND NOT ${isSilentShellRun("latest")}
              ORDER BY latest.ordinal DESC, latest.run_id DESC
              LIMIT 1
            )
            WHERE t.deleted_at IS NULL${threadId === undefined ? sql`` : sql` AND t.thread_id = ${threadId}`}
              AND json_extract(t.payload_json, '$.archivedAt') IS NULL
              AND json_extract(t.payload_json, '$.settledOverride') IS NULL
              AND json_extract(t.payload_json, '$.pinnedAt') IS NULL
              AND json_extract(t.payload_json, '$.autoSettleDisabledAt') IS NULL
              AND NOT EXISTS (
                SELECT 1 FROM orchestration_v2_projection_runs active
                WHERE active.thread_id = t.thread_id
                  AND active.status IN ('preparing', 'starting', 'running', 'waiting')
              )
              AND NOT EXISTS (
                SELECT 1 FROM orchestration_v2_projection_runtime_requests request
                WHERE request.thread_id = t.thread_id AND request.status = 'pending'
              )
              AND NOT EXISTS (
                SELECT 1 FROM orchestration_v2_projection_turn_items link
                  INDEXED BY orchestration_v2_projection_turn_items_waiting_link_idx
                WHERE link.thread_id = t.thread_id
                  AND link.type = 'link_request' AND link.status = 'waiting'
                  -- linkCardStillAsks: once the user writes after the card, it stops asking.
                  AND NOT EXISTS (
                    SELECT 1 FROM orchestration_v2_projection_messages message
                    WHERE message.thread_id = t.thread_id AND message.role = 'user'
                      AND json_extract(message.payload_json, '$.createdBy') = 'user'
                      AND NOT ${isIdleCompactionMessage("message")}
                      AND message.updated_at > COALESCE(
                        json_extract(link.payload_json, '$.startedAt'), link.updated_at
                      )
                  )
              )
            ORDER BY t.updated_at ASC, t.thread_id ASC
          `;

  const selectRemoteDelegatedTasks = sql<{
    readonly thread_id: string;
    readonly subagent_id: string;
  }>`
      SELECT thread_id, subagent_id FROM orchestration_v2_projection_subagents
      WHERE origin = 'app_owned'
        AND child_thread_id IS NULL
        AND json_type(payload_json, '$.remoteChild') = 'object'
        AND (
          json_type(payload_json, '$.result') = 'null'
          OR (
            json_extract(payload_json, '$.status') IN ('cancelled', 'interrupted')
            AND json_type(payload_json, '$.remoteChild.stoppedThere') IS NOT 'true'
          )
          OR json_type(payload_json, '$.remoteChild.stoppedThere') = 'false'
        )
    `;

  const selectThreadsWithPullRequests = (threadId?: ThreadId) => sql<PayloadRow>`
          SELECT payload_json
          FROM orchestration_v2_projection_threads
          WHERE deleted_at IS NULL${threadId === undefined ? sql`` : sql` AND thread_id = ${threadId}`}
            AND json_extract(payload_json, '$.archivedAt') IS NULL
            AND json_array_length(payload_json, '$.pullRequests') > 0
          ORDER BY updated_at ASC, thread_id ASC
        `;

  const selectThreadWorktreePaths = (options?: { readonly includeArchived?: boolean }) => sql<{
    readonly thread_id: string;
    readonly worktree_path: string;
  }>`
        SELECT thread_id, json_extract(payload_json, '$.worktreePath') AS worktree_path
        FROM orchestration_v2_projection_threads
        WHERE deleted_at IS NULL
          ${options?.includeArchived === true ? sql`` : sql`AND json_extract(payload_json, '$.archivedAt') IS NULL`}
          AND json_extract(payload_json, '$.worktreePath') IS NOT NULL
      `;

  const sequenceRows = sql<{ readonly snapshot_sequence: number | null }>`
        SELECT MAX(sequence) AS snapshot_sequence
        FROM orchestration_events
        WHERE application_event_version = 2
          AND aggregate_kind = 'thread'
      `;

  const selectPullRequestThreads = (threadId?: ThreadId, unsettledOnly = false) => sql<PayloadRow>`
          SELECT t.payload_json FROM orchestration_v2_projection_threads t
          WHERE t.deleted_at IS NULL${
            threadId === undefined
              ? sql` AND json_extract(t.payload_json, '$.archivedAt') IS NULL`
              : sql` AND t.thread_id = ${threadId}`
          }${
            unsettledOnly
              ? sql` AND json_extract(t.payload_json, '$.settledAt') IS NULL
                AND json_extract(t.payload_json, '$.settledOverride') IS NOT 'settled'`
              : sql``
          }
          ORDER BY t.updated_at ASC, t.thread_id ASC
        `;

  const selectActiveSubagentThreads = sql<{ readonly thread_id: string }>`
        SELECT thread_id
        FROM orchestration_v2_projection_threads
        WHERE deleted_at IS NULL
          AND json_extract(payload_json, '$.archivedAt') IS NULL
          AND json_extract(payload_json, '$.lineage.relationshipToParent') = 'subagent'
      `;

  const countThreads = sql<{ readonly count: number }>`
        SELECT COUNT(*) AS count FROM orchestration_v2_projection_threads WHERE deleted_at IS NULL
      `;

  const selectActiveShellIndex = sql<{
    readonly thread_id: string;
    readonly project_id: string;
    readonly relationship_to_parent: string | null;
    readonly parent_thread_id: string | null;
    readonly root_thread_id: string;
    readonly side_chat: number | null;
    readonly settled_override: string | null;
    readonly settled_at: string | null;
    readonly pinned_at: string | null;
    readonly snoozed_until: string | null;
    readonly may_be_live: number;
  }>`
        SELECT
          t.thread_id,
          t.project_id,
          json_extract(t.payload_json, '$.lineage.relationshipToParent') AS relationship_to_parent,
          json_extract(t.payload_json, '$.lineage.parentThreadId') AS parent_thread_id,
          json_extract(t.payload_json, '$.lineage.rootThreadId') AS root_thread_id,
          json_extract(t.payload_json, '$.lineage.sideChat') AS side_chat,
          json_extract(t.payload_json, '$.settledOverride') AS settled_override,
          json_extract(t.payload_json, '$.settledAt') AS settled_at,
          json_extract(t.payload_json, '$.pinnedAt') AS pinned_at,
          json_extract(t.payload_json, '$.snoozedUntil') AS snoozed_until,
          -- A superset of isLiveSubagentShell: the sources of its run status, pending request,
          -- and pending background work.
          (
            EXISTS (
              SELECT 1 FROM orchestration_v2_projection_runs r
              WHERE r.thread_id = t.thread_id
                AND r.status IN ('queued', 'preparing', 'starting', 'running', 'waiting')
            )
            OR EXISTS (
              SELECT 1 FROM orchestration_v2_projection_runtime_requests request
              WHERE request.thread_id = t.thread_id AND request.status = 'pending'
            )
            -- Same predicates as the shell's pending item and link reads, for their partial indexes.
            OR EXISTS (
              SELECT 1 FROM orchestration_v2_projection_turn_items item
              WHERE item.thread_id = t.thread_id
                AND item.type IN ('command_execution', 'dynamic_tool', 'subagent')
                AND item.status NOT IN ('completed', 'interrupted', 'failed', 'cancelled')
            )
            OR EXISTS (
              SELECT 1 FROM orchestration_v2_projection_turn_items link_request
              WHERE link_request.thread_id = t.thread_id
                AND link_request.type = 'link_request' AND link_request.status = 'waiting'
            )
            OR EXISTS (
              SELECT 1 FROM orchestration_v2_projection_provider_threads provider_thread
              WHERE provider_thread.thread_id = t.thread_id
                AND json_array_length(provider_thread.payload_json, '$.pendingBackgroundTasks') > 0
            )
            OR EXISTS (
              SELECT 1 FROM json_each(t.payload_json, '$.pullRequests') link
              WHERE json_type(link.value, '$.watch') IS NOT NULL
            )
          ) AS may_be_live
        FROM orchestration_v2_projection_threads t
        WHERE t.deleted_at IS NULL
          AND json_extract(t.payload_json, '$.archivedAt') IS NULL
        ORDER BY t.updated_at ASC, t.thread_id ASC
      `;

  const findGuestShareThread = (shareId: GuestShareId) => sql<{ readonly thread_id: string }>`
        SELECT thread_id
        FROM orchestration_v2_projection_threads
        WHERE json_extract(payload_json, '$.guestShare.id') = ${shareId}
        LIMIT 1
      `;

  const selectOlderSettledThreads = Effect.fn(
    "SqliteProjectionShellRows.selectOlderSettledThreads",
  )(function* ({ before, projectIds, limit }: OlderSettledThreadsQuery) {
    // ISO strings in one format, so string order is time order.
    const older = sql`
        SELECT t.thread_id, t.project_id,
          json_extract(t.payload_json, '$.settledAt') AS settled_at
        FROM orchestration_v2_projection_threads t
        WHERE t.deleted_at IS NULL
          AND json_extract(t.payload_json, '$.archivedAt') IS NULL
          AND json_extract(t.payload_json, '$.lineage.relationshipToParent') IS NOT 'subagent'
          AND json_extract(t.payload_json, '$.lineage.sideChat') IS NOT 1
          AND json_extract(t.payload_json, '$.settledOverride') = 'settled'
          AND json_extract(t.payload_json, '$.settledAt') IS NOT NULL${
            projectIds === undefined ? sql`` : sql` AND t.project_id IN ${sql.in(projectIds)}`
          }
      `;
    const belowCursor =
      before.threadId === undefined
        ? sql`older.settled_at < ${before.settledAt}`
        : sql`(older.settled_at < ${before.settledAt}
              OR (older.settled_at = ${before.settledAt} AND older.thread_id > ${before.threadId}))`;
    const pageRows =
      limit === 0
        ? []
        : yield* sql<{ readonly thread_id: string }>`
              WITH older AS (${older})
              SELECT older.thread_id FROM older
              WHERE ${belowCursor}
              ORDER BY older.settled_at DESC, older.thread_id ASC
              LIMIT ${limit}
            `;
    const countRows =
      limit !== 0
        ? []
        : yield* sql<{ readonly project_id: string; readonly count: number }>`
              WITH older AS (${older})
              SELECT older.project_id, COUNT(*) AS count FROM older
              WHERE ${belowCursor}
              GROUP BY older.project_id
            `;
    return { pageRows, countRows };
  });

  return {
    withReadTransaction: sql.withTransaction,
    selectShellThreadRows,
    selectShellRunRows,
    selectShellRunItemCounts,
    selectShellItemCounts,
    selectShellProviderThreadRows,
    selectShellPendingTurnItemRows,
    selectSettlementThreads,
    selectRemoteDelegatedTasks,
    selectThreadsWithPullRequests,
    selectThreadWorktreePaths,
    sequenceRows,
    selectPullRequestThreads,
    selectActiveSubagentThreads,
    countThreads,
    selectActiveShellIndex,
    findGuestShareThread,
    selectOlderSettledThreads,
  } satisfies ProjectionShellRows;
});
