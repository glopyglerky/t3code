import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import { RedbClient } from "../persistence/RedbClient.ts";
import type { ProjectionShellRows } from "./ProjectionShellRows.ts";
import { makeProjectionShells } from "./ProjectionStore.ts";

const Nullable = Schema.NullOr(Schema.String);
const ThreadIdRow = Schema.Struct({ thread_id: Schema.String });
const PayloadRow = Schema.Struct({ payload_json: Schema.String });
const ShellThreadRow = Schema.Struct({
  thread_id: Schema.String,
  payload_json: Schema.String,
  forked_from_run_source_thread_id: Nullable,
  latest_run_id: Nullable,
  latest_run_status: Nullable,
  latest_run_requested_at: Nullable,
  latest_run_started_at: Nullable,
  latest_run_completed_at: Nullable,
  latest_run_stop_requested: Schema.Number,
  latest_run_background_wake: Schema.Number,
  active_run_id: Nullable,
  active_run_status: Nullable,
  activity_run_status: Nullable,
  activity_run_started_at: Nullable,
  last_error: Nullable,
  terminal_failure_payload_json: Nullable,
  blocking_run_id: Nullable,
  blocking_run_requested_at: Nullable,
  blocking_run_started_at: Nullable,
  blocking_run_completed_at: Nullable,
  blocking_run_stop_requested: Schema.Number,
  blocking_run_background_wake: Schema.Number,
  blocking_failure_payload_json: Nullable,
  pending_request_payload_json: Nullable,
  pending_secret_request_payload_json: Nullable,
  pending_link_request_payload_json: Nullable,
  latest_user_message_at: Nullable,
  latest_user_authored_message_at: Nullable,
  has_actionable_proposed_plan: Schema.Number,
});
const SettlementThreadRow = Schema.Struct({
  thread_id: Schema.String,
  payload_json: Schema.String,
  latest_run_id: Nullable,
  latest_run_status: Nullable,
  latest_run_requested_at: Nullable,
  latest_run_started_at: Nullable,
  latest_run_completed_at: Nullable,
  latest_user_message_at: Nullable,
  latest_user_authored_message_at: Nullable,
});
const ActiveShellIndexRow = Schema.Struct({
  thread_id: Schema.String,
  project_id: Schema.String,
  relationship_to_parent: Nullable,
  parent_thread_id: Nullable,
  root_thread_id: Schema.String,
  side_chat: Schema.NullOr(Schema.Number),
  settled_override: Nullable,
  settled_at: Nullable,
  pinned_at: Nullable,
  snoozed_until: Nullable,
  may_be_live: Schema.Number,
});

const makeRows = Effect.gen(function* () {
  const client = yield* RedbClient;
  while (!(yield* client.prepareProjectionReads).prepared) yield* Effect.yieldNow;
  while (!(yield* client.prepareEvents).prepared) yield* Effect.yieldNow;
  const request = <A>(query: Readonly<Record<string, unknown>>, schema: Schema.Decoder<A>) =>
    client.request("application_projections_shell", { query }, schema);
  return {
    withReadTransaction: client.withReadTransaction,
    selectShellThreadRows: (threadId, options) =>
      request(
        {
          kind: "threads",
          ...(threadId === undefined ? {} : { thread_id: threadId }),
          ...(options === undefined ? {} : { options }),
        },
        Schema.Array(ShellThreadRow),
      ),
    selectShellRunRows: (threadIds) =>
      request(
        { kind: "runs", ...(threadIds === undefined ? {} : { thread_ids: threadIds }) },
        Schema.Array(
          Schema.Struct({
            thread_id: Schema.String,
            run_id: Schema.String,
            ordinal: Schema.Number,
          }),
        ),
      ),
    selectShellRunItemCounts: (threadIds) =>
      request(
        { kind: "run_item_counts", thread_ids: threadIds },
        Schema.Array(
          Schema.Struct({
            thread_id: Schema.String,
            run_id: Schema.String,
            item_count: Schema.Number,
          }),
        ),
      ),
    selectShellItemCounts: (threadIds) =>
      request(
        { kind: "item_counts", thread_ids: threadIds },
        Schema.Array(
          Schema.Struct({
            thread_id: Schema.String,
            item_count: Schema.Number,
            runless_item_count: Schema.Number,
          }),
        ),
      ),
    selectShellProviderThreadRows: (threadIds) =>
      request(
        { kind: "provider_threads", ...(threadIds === undefined ? {} : { thread_ids: threadIds }) },
        Schema.Array(Schema.Struct({ thread_id: Schema.String, payload_json: Schema.String })),
      ),
    selectShellPendingTurnItemRows: (threadIds, activeRunIds = []) =>
      request(
        {
          kind: "pending_items",
          ...(threadIds === undefined ? {} : { thread_ids: threadIds }),
          active_run_ids: activeRunIds,
        },
        Schema.Array(
          Schema.Struct({
            thread_id: Schema.String,
            payload_json: Schema.String,
            owning_provider_instance_id: Nullable,
          }),
        ),
      ),
    selectSettlementThreads: (threadId) =>
      request(
        { kind: "settlement_threads", ...(threadId === undefined ? {} : { thread_id: threadId }) },
        Schema.Array(SettlementThreadRow),
      ),
    selectRemoteDelegatedTasks: request(
      { kind: "remote_tasks" },
      Schema.Array(Schema.Struct({ thread_id: Schema.String, subagent_id: Schema.String })),
    ),
    selectThreadsWithPullRequests: (threadId) =>
      request(
        {
          kind: "threads_with_pull_requests",
          ...(threadId === undefined ? {} : { thread_id: threadId }),
        },
        Schema.Array(PayloadRow),
      ),
    selectThreadWorktreePaths: (options) =>
      request(
        { kind: "worktrees", include_archived: options?.includeArchived === true },
        Schema.Array(Schema.Struct({ thread_id: Schema.String, worktree_path: Schema.String })),
      ),
    sequenceRows: client
      .request("application_events_latest", { kind: "agent", thread_id: null }, Schema.Number)
      .pipe(Effect.map((sequence) => [{ snapshot_sequence: sequence === 0 ? null : sequence }])),
    selectPullRequestThreads: (threadId, unsettledOnly = false) =>
      request(
        {
          kind: "pull_request_threads",
          ...(threadId === undefined ? {} : { thread_id: threadId }),
          unsettled_only: unsettledOnly,
        },
        Schema.Array(PayloadRow),
      ),
    selectActiveSubagentThreads: request({ kind: "active_subagents" }, Schema.Array(ThreadIdRow)),
    countThreads: request(
      { kind: "count_threads" },
      Schema.Array(Schema.Struct({ count: Schema.Number })),
    ),
    selectActiveShellIndex: request({ kind: "active_index" }, Schema.Array(ActiveShellIndexRow)),
    findGuestShareThread: (shareId) =>
      request({ kind: "guest_share", share_id: shareId }, Schema.Array(ThreadIdRow)),
    selectOlderSettledThreads: ({ before, projectIds, limit }) =>
      request(
        {
          kind: "older_settled",
          before,
          ...(projectIds === undefined ? {} : { project_ids: projectIds }),
          limit,
        },
        Schema.Struct({
          pageRows: Schema.Array(ThreadIdRow),
          countRows: Schema.Array(
            Schema.Struct({ project_id: Schema.String, count: Schema.Number }),
          ),
        }),
      ),
  } satisfies ProjectionShellRows;
});

export const make = makeRows.pipe(Effect.map(makeProjectionShells));
