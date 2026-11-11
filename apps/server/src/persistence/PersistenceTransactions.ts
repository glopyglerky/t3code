import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as SqlError from "effect/sql/SqlError";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";

import * as NativeDatabase from "../orchestration-v2/NativeDatabase.ts";
import * as RedbClient from "./RedbClient.ts";

export type PersistenceTransactionError = SqlError.SqlError | RedbClient.RedbClientError;

const isNativeTransactionError = Schema.is(
  Schema.Union([
    NativeDatabase.NativeDatabaseInputError,
    NativeDatabase.NativeDatabaseUnavailable,
    NativeDatabase.NativeDatabaseRejected,
    NativeDatabase.NativeDatabaseOutcomeUnknown,
    RedbClient.RedbTransactionError,
  ]),
);

export function isPersistenceTransactionError(
  cause: unknown,
): cause is PersistenceTransactionError {
  return SqlError.isSqlError(cause) || isNativeTransactionError(cause);
}

export function isLockTimeout(cause: unknown): boolean {
  return SqlError.isSqlError(cause)
    ? cause.reason._tag === "LockTimeoutError"
    : Schema.is(NativeDatabase.NativeDatabaseRejected)(cause) && cause.kind === "writer_busy";
}

export interface PersistenceTransactionsShape {
  readonly isInTransaction: Effect.Effect<boolean>;
  readonly withReadTransaction: <A, E, R>(
    effect: Effect.Effect<A, E, R>,
  ) => Effect.Effect<A, E | PersistenceTransactionError, R>;
  readonly withTransaction: <A, E, R>(
    effect: Effect.Effect<A, E, R>,
  ) => Effect.Effect<A, E | PersistenceTransactionError, R>;
  /** Native owners defer publication through nested savepoints to the root commit. */
  readonly afterCommit?: (
    effect: Effect.Effect<void>,
  ) => Effect.Effect<void, PersistenceTransactionError>;
}

export class PersistenceTransactions extends Context.Service<
  PersistenceTransactions,
  PersistenceTransactionsShape
>()("t3/persistence/PersistenceTransactions") {}

export const sqliteLayer = Layer.effect(
  PersistenceTransactions,
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    return PersistenceTransactions.of({
      isInTransaction: Effect.serviceOption(sql.transactionService).pipe(Effect.map(Option.isSome)),
      withReadTransaction: Effect.fn("PersistenceTransactions.withReadTransaction")(function* <
        A,
        E,
        R,
      >(effect: Effect.Effect<A, E, R>) {
        if (yield* RedbClient.isInRedbTransaction) {
          return yield* new RedbClient.RedbTransactionError({
            reason: "mixed-storage-transaction",
          });
        }
        return yield* sql.withTransaction(effect).pipe(NodeSqliteClient.readOnly);
      }),
      withTransaction: Effect.fn("PersistenceTransactions.withTransaction")(function* <A, E, R>(
        effect: Effect.Effect<A, E, R>,
      ) {
        if (yield* RedbClient.isInRedbTransaction) {
          return yield* new RedbClient.RedbTransactionError({
            reason: "mixed-storage-transaction",
          });
        }
        return yield* sql.withTransaction(effect);
      }),
    });
  }),
);

export const redbLayer = Layer.effect(
  PersistenceTransactions,
  Effect.gen(function* () {
    const client = yield* RedbClient.RedbClient;
    return PersistenceTransactions.of({
      isInTransaction: client.isInTransaction,
      withReadTransaction: client.withReadTransaction,
      withTransaction: client.withTransaction,
      afterCommit: client.afterCommit,
    });
  }),
);
