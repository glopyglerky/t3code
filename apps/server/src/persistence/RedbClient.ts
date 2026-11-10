import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";

import * as NativeDatabase from "../orchestration-v2/NativeDatabase.ts";

export const ApplicationSchema = Schema.Struct({
  version: Schema.Literal(1),
  tables: Schema.Array(
    Schema.Struct({
      name: Schema.String,
      columns: Schema.Array(Schema.String),
      primary_key: Schema.Array(Schema.String),
    }),
  ),
});
export type ApplicationSchema = typeof ApplicationSchema.Type;

export const Row = Schema.JsonObject;
export type Row = typeof Row.Type;
export type RowKey = ReadonlyArray<Schema.Json>;

export const RowPage = Schema.Struct({
  rows: Schema.Array(Schema.Struct({ key: Schema.Array(Schema.Json), row: Row })),
  next: Schema.NullOr(Schema.String),
});
export type RowPage = typeof RowPage.Type;

export class RedbTransactionError extends Schema.TaggedError<RedbTransactionError>()(
  "RedbTransactionError",
  {
    reason: Schema.Literals([
      "nested-transaction",
      "mixed-storage-transaction",
      "different-client",
      "different-fiber",
      "closed-transaction",
      "transaction-id-exhausted",
    ]),
  },
) {
  override get message(): string {
    return `redb transaction rejected: ${this.reason}. No request was sent.`;
  }
}

export type RedbClientError = NativeDatabase.NativeDatabaseError | RedbTransactionError;

export interface RedbClientShape {
  readonly initialize: (
    schema: ApplicationSchema,
  ) => Effect.Effect<{ readonly initialized: boolean }, RedbClientError>;
  /** Nested transactions and child-fiber use are rejected before their first request. */
  readonly withTransaction: <A, E, R>(
    effect: Effect.Effect<A, E, R>,
  ) => Effect.Effect<A, E | RedbClientError, R>;
  /** Operations reuse this client's current transaction, or create one when called alone. */
  readonly get: (table: string, key: RowKey) => Effect.Effect<Row | null, RedbClientError>;
  readonly put: (table: string, key: RowKey, row: Row) => Effect.Effect<void, RedbClientError>;
  readonly delete: (table: string, key: RowKey) => Effect.Effect<boolean, RedbClientError>;
  /** Cursor order is native storage order. It does not imply SQL key or row order. */
  readonly scan: (
    table: string,
    options: { readonly after?: string | null; readonly limit: number },
  ) => Effect.Effect<RowPage, RedbClientError>;
}

export class RedbClient extends Context.Service<RedbClient, RedbClientShape>()(
  "t3/persistence/RedbClient",
) {}

interface Transaction {
  readonly client: object;
  readonly ownerId: number;
  readonly id: string;
  readonly session: NativeDatabase.NativeDatabaseSession;
  active: boolean;
  failure: RedbClientError | undefined;
}

const CurrentTransaction = Context.Reference<Transaction | undefined>(
  "t3/persistence/RedbClient/CurrentTransaction",
  { defaultValue: () => undefined },
);
const Begun = Schema.Struct({ transaction_id: Schema.String });
const Committed = Schema.Struct({ committed: Schema.Literal(true) });
const RolledBack = Schema.Struct({ rolled_back: Schema.Literal(true) });
const Initialized = Schema.Struct({ initialized: Schema.Boolean });
const Written = Schema.Struct({ written: Schema.Literal(true) });
const Deleted = Schema.Struct({ deleted: Schema.Boolean });

export const make = Effect.gen(function* () {
  const database = yield* NativeDatabase.NativeDatabase;
  const constructionSql = yield* Effect.serviceOption(SqlClient.SqlClient);
  const clientIdentity = {};
  let transactionSequence = 0;

  const outsideSqlTransaction = Effect.gen(function* () {
    const runtimeSql = yield* Effect.serviceOption(SqlClient.SqlClient);
    for (const sql of [constructionSql, runtimeSql]) {
      if (
        Option.isSome(sql) &&
        Option.isSome(yield* Effect.serviceOption(sql.value.transactionService))
      ) {
        return yield* new RedbTransactionError({ reason: "mixed-storage-transaction" });
      }
    }
  });

  const withTransaction: RedbClientShape["withTransaction"] = Effect.fn("RedbClient.withTransaction")(
    function* <A, E, R>(effect: Effect.Effect<A, E, R>) {
      if ((yield* CurrentTransaction) !== undefined)
        return yield* new RedbTransactionError({ reason: "nested-transaction" });
      yield* outsideSqlTransaction;
      return yield* database.withSession((session) =>
        Effect.uninterruptibleMask((restore) =>
          Effect.gen(function* () {
            if (transactionSequence >= Number.MAX_SAFE_INTEGER)
              return yield* new RedbTransactionError({ reason: "transaction-id-exhausted" });
            const id = `application:${++transactionSequence}`;
            const begun = yield* restore(
              session.request({ operation: "application_begin", transaction_id: id }, Begun),
            );
            if (begun.transaction_id !== id) {
              yield* session.discard;
              return yield* new NativeDatabase.NativeDatabaseOutcomeUnknown({ stage: "response" });
            }
            const transaction: Transaction = {
              client: clientIdentity,
              ownerId: yield* Effect.withFiber((fiber) => Effect.succeed(fiber.id)),
              id,
              session,
              active: true,
              failure: undefined,
            };
            const exit = yield* Effect.exit(
              restore(effect).pipe(Effect.provideService(CurrentTransaction, transaction)),
            );
            transaction.active = false;
            if (Exit.isFailure(exit) || transaction.failure !== undefined) {
              // The helper aborts a transaction when a row operation fails. If rollback
              // cannot acknowledge it, closing the helper drops its snapshot and staged writes.
              yield* session
                .request({ operation: "application_rollback", transaction_id: id }, RolledBack)
                .pipe(Effect.catch(() => session.discard));
              if (Exit.isFailure(exit)) return yield* Effect.failCause(exit.cause);
              return yield* transaction.failure!;
            }
            // The commit acknowledgement is the only success boundary. An uncertain
            // outcome propagates unchanged and never causes another commit or fallback.
            yield* session.request(
              { operation: "application_commit", transaction_id: id },
              Committed,
            );
            return exit.value;
          }),
        ),
      );
    },
  );

  const inTransaction = <A>(
    run: (transaction: Transaction) => Effect.Effect<A, NativeDatabase.NativeDatabaseError>,
  ): Effect.Effect<A, RedbClientError> =>
    Effect.gen(function* () {
      const transaction = yield* CurrentTransaction;
      if (transaction === undefined) return yield* withTransaction(inTransaction(run));
      if (transaction.client !== clientIdentity)
        return yield* new RedbTransactionError({ reason: "different-client" });
      if (!transaction.active)
        return yield* new RedbTransactionError({ reason: "closed-transaction" });
      const ownerId = yield* Effect.withFiber((fiber) => Effect.succeed(fiber.id));
      if (transaction.ownerId !== ownerId)
        return yield* new RedbTransactionError({ reason: "different-fiber" });
      if (transaction.failure !== undefined) return yield* transaction.failure;
      return yield* outsideSqlTransaction.pipe(
        Effect.andThen(run(transaction)),
        Effect.catch((error) => {
          // Catching an operation error in the body cannot commit earlier writes.
          transaction.failure ??= error;
          return Effect.fail(error);
        }),
      );
    });

  const request = <A>(
    input: Record<string, unknown>,
    response: Schema.Decoder<A>,
  ): Effect.Effect<A, RedbClientError> =>
    inTransaction((transaction) =>
      Schema.decodeUnknownEffect(Schema.Json)(input).pipe(
        Effect.mapError((cause) =>
          new NativeDatabase.NativeDatabaseInputError({ stage: "request", cause }),
        ),
        Effect.andThen(
          transaction.session.request({ ...input, transaction_id: transaction.id }, response),
        ),
      ),
    );

  return RedbClient.of({
    initialize: Effect.fn("RedbClient.initialize")(function* (schema) {
      if ((yield* CurrentTransaction) !== undefined)
        return yield* new RedbTransactionError({ reason: "nested-transaction" });
      yield* outsideSqlTransaction;
      return yield* database.request({ operation: "application_init", schema }, Initialized);
    }),
    withTransaction,
    get: Effect.fn("RedbClient.get")((table, key) =>
      request({ operation: "application_get", table, key }, Schema.NullOr(Row)),
    ),
    put: Effect.fn("RedbClient.put")((table, key, row) =>
      request({ operation: "application_put", table, key, row }, Written).pipe(Effect.asVoid),
    ),
    delete: Effect.fn("RedbClient.delete")((table, key) =>
      request({ operation: "application_delete", table, key }, Deleted).pipe(
        Effect.map((result) => result.deleted),
      ),
    ),
    scan: Effect.fn("RedbClient.scan")((table, options) =>
      request({ operation: "application_scan", table, ...options }, RowPage),
    ),
  });
});

export const layer = Layer.effect(RedbClient, make);
