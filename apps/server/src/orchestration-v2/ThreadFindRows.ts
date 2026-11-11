import type { OrchestrationV2TurnItem, ThreadId, TurnItemId } from "@t3tools/contracts";
import type * as Effect from "effect/Effect";
import type * as Schema from "effect/Schema";

import type {
  PersistenceTransactionError,
  PersistenceTransactionsShape,
} from "../persistence/PersistenceTransactions.ts";

export type ThreadFindRowsError = PersistenceTransactionError | Schema.SchemaError;

export interface ThreadFindSource {
  readonly threadId: ThreadId;
  readonly id: TurnItemId;
}

export interface ThreadFindPayloadRow {
  readonly thread_id: string;
  readonly turn_item_id: string;
  readonly payload_json: string;
  readonly entry_id: string;
  readonly item_type: OrchestrationV2TurnItem["type"];
}

/** Raw records needed by find, read under the same owner as its projected index. */
export interface ThreadFindRows {
  readonly activeThread: (
    threadId: ThreadId,
  ) => Effect.Effect<ReadonlyArray<{ readonly cwd: string | null }>, ThreadFindRowsError>;
  readonly payloads: (
    sources: ReadonlyArray<ThreadFindSource>,
  ) => Effect.Effect<ReadonlyArray<ThreadFindPayloadRow>, ThreadFindRowsError>;
  readonly revision: (
    threadIds: ReadonlyArray<ThreadId>,
  ) => Effect.Effect<number, ThreadFindRowsError>;
  readonly withReadTransaction: PersistenceTransactionsShape["withReadTransaction"];
}
