import type * as Effect from "effect/Effect";
import type { ThreadId, GuestShareId } from "@t3tools/contracts";
import type {
  PersistenceTransactionError,
  PersistenceTransactionsShape,
} from "../persistence/PersistenceTransactions.ts";
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

type Read<A> = Effect.Effect<A, PersistenceTransactionError>;
type PayloadRow = { readonly payload_json: string };
export interface ActiveShellIndexRow {
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
  readonly has_remote_tasks: number;
}

/** Purpose-specific shell queries; never a SQL string or full-store snapshot boundary. */
export interface ProjectionShellRows {
  readonly withReadTransaction: PersistenceTransactionsShape["withReadTransaction"];
  readonly selectShellThreadRows: (
    threadId?: ThreadId,
    options?: ShellSnapshotOptions & { readonly listedOnly?: boolean },
  ) => Read<ReadonlyArray<ShellThreadRow>>;
  readonly selectShellRunRows: (
    threadIds?: ReadonlyArray<ThreadId>,
  ) => Read<ReadonlyArray<ShellRunRow>>;
  readonly selectShellRunItemCounts: (
    threadIds: ReadonlyArray<ThreadId>,
  ) => Read<ReadonlyArray<ShellRunItemCountRow>>;
  readonly selectShellItemCounts: (
    threadIds: ReadonlyArray<ThreadId>,
  ) => Read<ReadonlyArray<ShellItemCountRow>>;
  readonly selectShellProviderThreadRows: (
    threadIds?: ReadonlyArray<ThreadId>,
  ) => Read<ReadonlyArray<PayloadRow & { readonly thread_id: string }>>;
  readonly selectShellPendingTurnItemRows: (
    threadIds?: ReadonlyArray<ThreadId>,
    activeRunIds?: ReadonlyArray<string>,
  ) => Read<ReadonlyArray<ShellPendingTurnItemRow>>;
  readonly selectSettlementThreads: (
    threadId?: ThreadId,
  ) => Read<ReadonlyArray<SettlementThreadRow>>;
  readonly selectRemoteDelegatedTasks: Read<
    ReadonlyArray<{ readonly thread_id: string; readonly subagent_id: string }>
  >;
  readonly selectThreadsWithPullRequests: (threadId?: ThreadId) => Read<ReadonlyArray<PayloadRow>>;
  readonly selectThreadWorktreePaths: (options?: {
    readonly includeArchived?: boolean;
  }) => Read<ReadonlyArray<{ readonly thread_id: string; readonly worktree_path: string }>>;
  readonly sequenceRows: Read<ReadonlyArray<{ readonly snapshot_sequence: number | null }>>;
  readonly selectPullRequestThreads: (
    threadId?: ThreadId,
    unsettledOnly?: boolean,
  ) => Read<ReadonlyArray<PayloadRow>>;
  readonly selectActiveSubagentThreads: Read<ReadonlyArray<{ readonly thread_id: string }>>;
  readonly countThreads: Read<ReadonlyArray<{ readonly count: number }>>;
  readonly selectActiveShellIndex: Read<ReadonlyArray<ActiveShellIndexRow>>;
  readonly findGuestShareThread: (
    shareId: GuestShareId,
  ) => Read<ReadonlyArray<{ readonly thread_id: string }>>;
  readonly selectOlderSettledThreads: (input: OlderSettledThreadsQuery) => Read<{
    readonly pageRows: ReadonlyArray<{ readonly thread_id: string }>;
    readonly countRows: ReadonlyArray<{ readonly project_id: string; readonly count: number }>;
  }>;
}
