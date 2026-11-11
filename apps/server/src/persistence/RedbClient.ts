import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
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

const applicationOperations = {
  application_events_append: "write",
  application_events_read: "read",
  application_events_stats: "read",
  application_events_latest: "read",
  application_events_pruned_through: "read",
  application_receipts_insert: "write",
  application_receipts_upsert: "write",
  application_receipts_get: "read",
  application_positions_allocate: "write",
  application_projects_select: "read",
  application_projects_upsert: "write",
  application_projections_apply: "write",
  application_projections_normalization: "read",
  application_projections_read: "read",
  application_projections_guard: "read",
  application_projections_mark_applied: "write",
  application_outbox_enqueue: "write",
  application_outbox_get: "read",
  application_outbox_list: "read",
  application_outbox_cancel: "write",
  application_outbox_reconcile: "write",
  application_outbox_claim: "write",
  application_outbox_claim_once: "write",
  application_outbox_next_claimable_at: "read",
  application_outbox_succeed: "write",
  application_outbox_retry: "write",
  application_outbox_fail: "write",
  application_outbox_prune: "write",
  application_guide_get: "read",
  application_guide_put: "write",
  application_viewed_files_list: "read",
  application_viewed_files_set: "write",
} as const;
export type ApplicationOperation = keyof typeof applicationOperations;
const allowedOperations = new Set<string>(Object.keys(applicationOperations));

export class RedbTransactionError extends Schema.TaggedError<RedbTransactionError>()(
  "RedbTransactionError",
  {
    reason: Schema.Literals([
      "initialization-in-transaction",
      "mixed-storage-transaction",
      "different-client",
      "different-fiber",
      "different-scope",
      "closed-transaction",
      "missing-transaction",
      "publication-write-reentry",
      "transaction-id-exhausted",
      "savepoint-id-exhausted",
    ]),
  },
) {
  override get message(): string {
    return `redb transaction rejected: ${this.reason}. No request was sent.`;
  }
}

export type RedbClientError = NativeDatabase.NativeDatabaseError | RedbTransactionError;

export interface RedbClientShape {
  /** Scope presence includes failed scopes; it never authorizes a request or replay. */
  readonly isInTransaction: Effect.Effect<boolean>;
  readonly initialize: (
    schema: ApplicationSchema,
  ) => Effect.Effect<{ readonly initialized: boolean }, RedbClientError>;
  /** Reserve the writer before the body runs. Same-fiber nesting uses native savepoints. */
  readonly withTransaction: <A, E, R>(
    effect: Effect.Effect<A, E, R>,
  ) => Effect.Effect<A, E | RedbClientError, R>;
  /** Run once after this scope's root commit is acknowledged; rollback discards the callback. */
  readonly afterCommit: (effect: Effect.Effect<void>) => Effect.Effect<void, RedbClientError>;
  /** Operations reuse this client's current transaction, or create one when called alone. */
  readonly get: (table: string, key: RowKey) => Effect.Effect<Row | null, RedbClientError>;
  readonly put: (table: string, key: RowKey, row: Row) => Effect.Effect<void, RedbClientError>;
  readonly delete: (table: string, key: RowKey) => Effect.Effect<boolean, RedbClientError>;
  /** Cursor order is native storage order. It does not imply SQL key or row order. */
  readonly scan: (
    table: string,
    options: { readonly after?: string | null; readonly limit: number },
  ) => Effect.Effect<RowPage, RedbClientError>;
  /** Typed domain operations share the current scope; callers cannot supply transaction IDs. */
  readonly request: <A>(
    operation: ApplicationOperation,
    input: Readonly<Record<string, unknown>>,
    response: Schema.Decoder<A>,
  ) => Effect.Effect<A, RedbClientError>;
  /** Build one bounded batch of event indexes before opening an application transaction. */
  readonly prepareEvents: Effect.Effect<
    { readonly prepared: boolean; readonly indexed_rows: number },
    RedbClientError
  >;
  readonly prepareReceipts: Effect.Effect<PreparationResult, RedbClientError>;
  readonly preparePositions: Effect.Effect<PreparationResult, RedbClientError>;
  readonly prepareProjects: Effect.Effect<PreparationResult, RedbClientError>;
  readonly prepareViewedFiles: Effect.Effect<PreparationResult, RedbClientError>;
  readonly prepareProjections: Effect.Effect<PreparationResult, RedbClientError>;
  readonly prepareOutbox: Effect.Effect<PreparationResult, RedbClientError>;
}

export interface PreparationResult {
  readonly prepared: boolean;
  readonly indexed_rows: number;
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
  savepointSequence: number;
  currentScope: TransactionScope | undefined;
}

interface TransactionScope {
  readonly transaction: Transaction;
  active: boolean;
  failure: RedbClientError | undefined;
  readonly afterCommit: Array<Effect.Effect<void>>;
}

const CurrentTransaction = Context.Reference<TransactionScope | undefined>(
  "t3/persistence/RedbClient/CurrentTransaction",
  { defaultValue: () => undefined },
);
const CurrentCommitPublication = Context.Reference<ReadonlyArray<object>>(
  "t3/persistence/RedbClient/CurrentCommitPublication",
  { defaultValue: () => [] },
);
/** Let another transaction coordinator reject mixed ownership before opening its store. */
export const isInRedbTransaction = CurrentTransaction.pipe(
  Effect.map((scope) => scope !== undefined),
);
const Begun = Schema.Struct({ transaction_id: Schema.String });
const Saved = Schema.Struct({ savepoint_id: Schema.String });
const Released = Schema.Struct({ released: Schema.Literal(true) });
const Committed = Schema.Struct({ committed: Schema.Literal(true) });
const RolledBack = Schema.Struct({ rolled_back: Schema.Literal(true) });
const Initialized = Schema.Struct({ initialized: Schema.Boolean });
const Written = Schema.Struct({ written: Schema.Literal(true) });
const Deleted = Schema.Struct({ deleted: Schema.Boolean });
const Prepared = Schema.Struct({
  prepared: Schema.Boolean,
  indexed_rows: Schema.Number.check(
    Schema.isInt(),
    Schema.isBetween({ minimum: 0, maximum: Number.MAX_SAFE_INTEGER }),
  ),
});

function terminal(error: RedbClientError): boolean {
  switch (error._tag) {
    case "NativeDatabaseOutcomeUnknown":
    case "NativeDatabaseUnavailable":
      return true;
    case "NativeDatabaseRejected":
      return error.kind !== "invalid_request" && error.kind !== "duplicate_record";
    case "NativeDatabaseInputError":
      return error.stage !== "request";
    case "RedbTransactionError":
      return false;
  }
}

export const make = Effect.gen(function* () {
  const database = yield* NativeDatabase.NativeDatabase;
  const constructionSql = yield* Effect.serviceOption(SqlClient.SqlClient);
  const clientIdentity = {};
  let transactionSequence = 0;
  // A later writer may begin as soon as redb acknowledges the previous commit.
  // Keep that writer's commit behind the previous owner's publications as well.
  const commitLane = yield* Semaphore.make(1);

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

  const authorize = Effect.fn("RedbClient.authorize")(function* (scope: TransactionScope) {
    const transaction = scope.transaction;
    if (transaction.client !== clientIdentity)
      return yield* new RedbTransactionError({ reason: "different-client" });
    if (!scope.active || !transaction.active)
      return yield* new RedbTransactionError({ reason: "closed-transaction" });
    const ownerId = yield* Effect.withFiber((fiber) => Effect.succeed(fiber.id));
    if (transaction.ownerId !== ownerId)
      return yield* new RedbTransactionError({ reason: "different-fiber" });
    if (transaction.currentScope !== scope)
      return yield* new RedbTransactionError({ reason: "different-scope" });
    if (transaction.failure !== undefined) return yield* transaction.failure;
    if (scope.failure !== undefined) return yield* scope.failure;
  });

  // A failed lifecycle acknowledgement leaves the savepoint stack uncertain.
  // Discard the complete transaction even when an enclosing body catches the error.
  const control = <A>(
    transaction: Transaction,
    input: Record<string, unknown>,
    response: Schema.Decoder<A>,
  ) =>
    transaction.session
      .request({ ...input, transaction_id: transaction.id }, response)
      .pipe(
        Effect.onExit((exit) => {
          if (Exit.isSuccess(exit)) return Effect.void;
          const failure = exit.cause.reasons.find((reason) => reason._tag === "Fail");
          transaction.failure ??=
            failure?.error ??
            new NativeDatabase.NativeDatabaseOutcomeUnknown({
              stage: "response",
              cause: exit.cause,
            });
          return transaction.session.discard;
        }),
      );

  const nested = <A, E, R>(
    parent: TransactionScope,
    effect: Effect.Effect<A, E, R>,
  ): Effect.Effect<A, E | RedbClientError, R> =>
    Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        const transaction = parent.transaction;
        if (transaction.savepointSequence >= Number.MAX_SAFE_INTEGER)
          return yield* new RedbTransactionError({ reason: "savepoint-id-exhausted" });
        const savepointId = `scope:${++transaction.savepointSequence}`;
        const saved = yield* control(
          transaction,
          { operation: "application_savepoint", savepoint_id: savepointId },
          Saved,
        );
        if (saved.savepoint_id !== savepointId) {
          transaction.failure = new NativeDatabase.NativeDatabaseOutcomeUnknown({
            stage: "response",
          });
          yield* transaction.session.discard;
          return yield* transaction.failure;
        }
        const scope: TransactionScope = { transaction, active: true, failure: undefined, afterCommit: [] };
        transaction.currentScope = scope;
        const exit = yield* Effect.exit(
          restore(effect).pipe(Effect.provideService(CurrentTransaction, scope)),
        );
        scope.active = false;
        transaction.currentScope = parent;
        if (transaction.failure !== undefined) {
          yield* transaction.session.discard;
          return yield* transaction.failure;
        }
        if (Exit.isFailure(exit) || scope.failure !== undefined) {
          yield* control(
            transaction,
            { operation: "application_rollback_to", savepoint_id: savepointId },
            RolledBack,
          );
        }
        // ROLLBACK TO retains its savepoint, just as SQLite does. RELEASE closes
        // it on both paths without committing any part of the outer transaction.
        yield* control(
          transaction,
          { operation: "application_release", savepoint_id: savepointId },
          Released,
        );
        if (Exit.isFailure(exit)) return yield* Effect.failCause(exit.cause);
        if (scope.failure !== undefined) return yield* scope.failure;
        parent.afterCommit.push(...scope.afterCommit);
        return exit.value;
      }),
    );

  const root = <A, E, R>(
    effect: Effect.Effect<A, E, R>,
    mode: "read" | "write",
  ): Effect.Effect<A, E | RedbClientError, R> =>
    Effect.gen(function* () {
      yield* outsideSqlTransaction;
      const activePublications = yield* CurrentCommitPublication;
      if (mode === "write" && activePublications.includes(clientIdentity)) {
        return yield* new RedbTransactionError({ reason: "publication-write-reentry" });
      }
      const publications: Array<Effect.Effect<void>> = [];
      const operation = Effect.uninterruptibleMask((restore) =>
        database.withSession((session) =>
          Effect.gen(function* () {
            if (transactionSequence >= Number.MAX_SAFE_INTEGER)
              return yield* new RedbTransactionError({ reason: "transaction-id-exhausted" });
            const id = `application:${++transactionSequence}`;
            const begun = yield* restore(
              session.request({ operation: "application_begin", transaction_id: id, mode }, Begun),
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
              savepointSequence: 0,
              currentScope: undefined,
            };
            const scope: TransactionScope = { transaction, active: true, failure: undefined, afterCommit: [] };
            transaction.currentScope = scope;
            const exit = yield* Effect.exit(
              restore(effect).pipe(Effect.provideService(CurrentTransaction, scope)),
            );
            scope.active = false;
            transaction.active = false;
            transaction.currentScope = undefined;
            if (
              Exit.isFailure(exit) ||
              transaction.failure !== undefined ||
              scope.failure !== undefined
            ) {
              // Fatal native errors can already have removed the transaction.
              // Closing the helper also drops any remaining writer reservation.
              yield* session
                .request({ operation: "application_rollback", transaction_id: id }, RolledBack)
                .pipe(Effect.catch(() => session.discard));
              if (transaction.failure !== undefined) return yield* transaction.failure;
              if (Exit.isFailure(exit)) return yield* Effect.failCause(exit.cause);
              if (scope.failure !== undefined) return yield* scope.failure;
            }
            // The commit acknowledgement is the only success boundary. An uncertain
            // outcome propagates unchanged and never causes another commit or fallback.
            yield* session.request(
              { operation: "application_commit", transaction_id: id },
              Committed,
            );
            publications.push(...scope.afterCommit);
            return exit.value;
          }),
        ).pipe(Effect.tap(() =>
          Effect.forEach(publications, (publish) => publish, { discard: true }).pipe(
            Effect.provideService(CurrentCommitPublication, [...activePublications, clientIdentity]),
          ),
        )),
      );
      // Acquire before the session: a waiting writer must not hold the connection
      // that an acknowledged owner's publication uses for a durable read.
      return yield* mode === "write" ? operation.pipe(commitLane.withPermit) : operation;
    });

  const withTransaction: RedbClientShape["withTransaction"] = Effect.fn("RedbClient.withTransaction")(
    function* <A, E, R>(effect: Effect.Effect<A, E, R>) {
      const scope = yield* CurrentTransaction;
      if (scope === undefined) return yield* root(effect, "write");
      yield* authorize(scope);
      yield* outsideSqlTransaction;
      return yield* nested(scope, effect);
    },
  );

  const inTransaction = <A>(
    run: (transaction: Transaction) => Effect.Effect<A, NativeDatabase.NativeDatabaseError>,
    mode: "read" | "write",
  ): Effect.Effect<A, RedbClientError> =>
    Effect.gen(function* () {
      const scope = yield* CurrentTransaction;
      if (scope === undefined) return yield* root(inTransaction(run, mode), mode);
      yield* authorize(scope);
      const transaction = scope.transaction;
      return yield* outsideSqlTransaction.pipe(
        Effect.andThen(run(transaction)),
        Effect.catch((error) => {
          // Recoverable statement errors invalidate only this savepoint. The
          // wrapper must still roll it back if the body catches the error.
          scope.failure ??= error;
          if (terminal(error)) transaction.failure ??= error;
          return Effect.fail(error);
        }),
      );
    });

  const send = <A>(
    transaction: Transaction,
    input: Record<string, unknown>,
    response: Schema.Decoder<A>,
  ) =>
    Schema.decodeUnknownEffect(Schema.Json)(input).pipe(
      Effect.mapError((cause) =>
        new NativeDatabase.NativeDatabaseInputError({ stage: "request", cause }),
      ),
      Effect.andThen(
        transaction.session.request({ ...input, transaction_id: transaction.id }, response),
      ),
    );

  const request = <A>(
    input: Record<string, unknown>,
    response: Schema.Decoder<A>,
    mode: "read" | "write",
  ): Effect.Effect<A, RedbClientError> =>
    inTransaction((transaction) => send(transaction, input, response), mode);

  const outsideTransaction = Effect.gen(function* () {
    if ((yield* CurrentTransaction) !== undefined)
      return yield* new RedbTransactionError({ reason: "initialization-in-transaction" });
    yield* outsideSqlTransaction;
  });

  return RedbClient.of({
    isInTransaction: isInRedbTransaction,
    afterCommit: Effect.fn("RedbClient.afterCommit")(function* (effect) {
      const scope = yield* CurrentTransaction;
      if (scope === undefined) return yield* new RedbTransactionError({ reason: "missing-transaction" });
      yield* authorize(scope);
      yield* outsideSqlTransaction;
      scope.afterCommit.push(effect);
    }),
    initialize: Effect.fn("RedbClient.initialize")(function* (schema) {
      yield* outsideTransaction;
      return yield* database.request({ operation: "application_init", schema }, Initialized);
    }),
    prepareEvents: Effect.gen(function* () {
      yield* outsideTransaction;
      return yield* database.request({ operation: "application_events_prepare" }, Prepared);
    }),
    prepareReceipts: Effect.gen(function* () {
      yield* outsideTransaction;
      return yield* database.request({ operation: "application_receipts_prepare" }, Prepared);
    }),
    preparePositions: Effect.gen(function* () {
      yield* outsideTransaction;
      return yield* database.request({ operation: "application_positions_prepare" }, Prepared);
    }),
    prepareProjects: Effect.gen(function* () {
      yield* outsideTransaction;
      return yield* database.request({ operation: "application_projects_prepare" }, Prepared);
    }),
    prepareViewedFiles: Effect.gen(function* () {
      yield* outsideTransaction;
      return yield* database.request({ operation: "application_viewed_files_prepare" }, Prepared);
    }),
    prepareProjections: Effect.gen(function* () {
      yield* outsideTransaction;
      return yield* database.request({ operation: "application_projections_prepare" }, Prepared);
    }),
    prepareOutbox: Effect.gen(function* () {
      yield* outsideTransaction;
      return yield* database.request({ operation: "application_outbox_prepare" }, Prepared);
    }),
    withTransaction,
    get: Effect.fn("RedbClient.get")((table, key) =>
      request({ operation: "application_get", table, key }, Schema.NullOr(Row), "read"),
    ),
    put: Effect.fn("RedbClient.put")((table, key, row) =>
      request({ operation: "application_put", table, key, row }, Written, "write").pipe(Effect.asVoid),
    ),
    delete: Effect.fn("RedbClient.delete")((table, key) =>
      request({ operation: "application_delete", table, key }, Deleted, "write").pipe(
        Effect.map((result) => result.deleted),
      ),
    ),
    scan: Effect.fn("RedbClient.scan")((table, options) =>
      request({ operation: "application_scan", table, ...options }, RowPage, "read"),
    ),
    request: Effect.fn("RedbClient.request")(function* <A>(
      operation: ApplicationOperation,
      input: Readonly<Record<string, unknown>>,
      response: Schema.Decoder<A>,
    ) {
      return yield* inTransaction(
        (transaction) =>
          Effect.gen(function* () {
            if (
              !allowedOperations.has(operation) ||
              Object.hasOwn(input, "operation") ||
              Object.hasOwn(input, "transaction_id")
            )
              return yield* new NativeDatabase.NativeDatabaseInputError({ stage: "request" });
            return yield* send(transaction, { ...input, operation }, response);
          }),
        applicationOperations[operation] ?? "read",
      );
    }),
  });
});

export const layer = Layer.effect(RedbClient, make);
