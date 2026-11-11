import type { ProjectId, ThreadId } from "@t3tools/contracts";
import type * as Effect from "effect/Effect";
import type * as Schema from "effect/Schema";

import type {
  PersistenceTransactionError,
  PersistenceTransactionsShape,
} from "../persistence/PersistenceTransactions.ts";
import type {
  OlderSettledTitleRow,
  SearchRequest,
  SearchRow,
  ThreadOpensRow,
  TitleCandidateRow,
} from "./ThreadSearch.ts";

export type ThreadSearchRowsError = PersistenceTransactionError | Schema.SchemaError;

/** Bounded lexical candidates and scoring facts, before the shared palette ranker. */
export interface ThreadSearchRows {
  readonly hasIndex: Effect.Effect<boolean, ThreadSearchRowsError>;
  readonly searchRows: (
    input: typeof SearchRequest.Type,
  ) => Effect.Effect<ReadonlyArray<typeof SearchRow.Type>, ThreadSearchRowsError>;
  readonly threadOpens: (input: {
    readonly threadIds: ReadonlyArray<string>;
  }) => Effect.Effect<ReadonlyArray<typeof ThreadOpensRow.Type>, ThreadSearchRowsError>;
  readonly titleCandidates: (input: {
    readonly projectId: ProjectId | null;
  }) => Effect.Effect<ReadonlyArray<typeof TitleCandidateRow.Type>, ThreadSearchRowsError>;
  readonly olderSettledTitleCandidates: (input: {
    readonly before: string;
    readonly projectIds: ReadonlyArray<string> | null;
  }) => Effect.Effect<ReadonlyArray<typeof OlderSettledTitleRow.Type>, ThreadSearchRowsError>;
  readonly olderSettledIds: (input: {
    readonly before: string;
    readonly threadIds: ReadonlyArray<string>;
    readonly projectIds: ReadonlyArray<string> | null;
  }) => Effect.Effect<ReadonlyArray<{ readonly id: ThreadId }>, ThreadSearchRowsError>;
  readonly withReadTransaction: PersistenceTransactionsShape["withReadTransaction"];
}
