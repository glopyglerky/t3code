import { CommandId, ThreadId } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
import * as SecureRandom from "effect/crypto/SecureRandom";
import * as SqlClient from "effect/sql/SqlClient";

import * as EffectOutbox from "./EffectOutbox.ts";
import * as NativeDatabase from "./NativeDatabase.ts";

const isEffectOutboxError = Schema.is(EffectOutbox.EffectOutboxError);
const Counter = Schema.Number.check(
  Schema.isInt(),
  Schema.isBetween({ minimum: 0, maximum: Number.MAX_SAFE_INTEGER }),
);
const Identifier = Schema.String.check(
  Schema.makeFilter(
    (value) =>
      value.length > 0 &&
      value.isWellFormed() &&
      Buffer.byteLength(value, "utf8") <= 1_024 &&
      !/\p{Cc}/u.test(value),
  ),
);
const Timestamp = Schema.String.check(
  Schema.makeFilter((value) => {
    const date = DateTime.make(value);
    return (
      /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/.test(value) &&
      Option.isSome(date) &&
      DateTime.formatIso(date.value) === value
    );
  }),
);
const StoredEffect = Schema.Struct({
  id: Identifier,
  command_id: CommandId,
  thread_id: ThreadId,
  request: EffectOutbox.OrchestrationEffectRequestV2,
  status: EffectOutbox.OrchestrationEffectStatusV2,
  attempt_count: Counter,
  available_at: Timestamp,
  lease_owner: Schema.NullOr(Identifier),
  lease_expires_at: Schema.Null,
  created_at: Timestamp,
  updated_at: Timestamp,
  completed_at: Schema.NullOr(Timestamp),
  last_error: Schema.NullOr(Schema.String),
  insertion_ordinal: Counter.check(Schema.isGreaterThanOrEqualTo(1)),
});
const ClaimOutcome = Schema.Union([
  Schema.Struct({
    state: Schema.Literal("claimed"),
    replayed: Schema.Boolean,
    effect: StoredEffect,
  }),
  Schema.Struct({
    state: Schema.Literals(["empty", "retired"]),
    replayed: Schema.Boolean,
    effect: Schema.Null,
  }),
]);

function toEffect(row: typeof StoredEffect.Type): EffectOutbox.OrchestrationEffectV2 {
  return {
    id: row.id,
    commandId: row.command_id,
    threadId: row.thread_id,
    request: row.request,
    status: row.status,
    attemptCount: row.attempt_count,
    availableAt: row.available_at,
    leaseOwner: row.lease_owner,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
    completedAt: row.completed_at,
    lastError: row.last_error,
  };
}

interface ClaimRequest {
  readonly operation: "outbox_claim_once";
  readonly stream_id: string;
  readonly sequence: number;
  readonly worker_id: string;
  readonly now: string;
  readonly exclude_restart_continuations: boolean;
}

interface ClaimStream {
  readonly id: string;
  sequence: number;
  pending: ClaimRequest | undefined;
}

// This layer is selected together with native event/projection repositories, never inside SQL commits.
export const layer = Layer.effect(
  EffectOutbox.EffectOutboxV2,
  Effect.gen(function* () {
    const database = yield* NativeDatabase.NativeDatabase;
    const random = yield* SecureRandom.SecureRandom;
    const constructionSql = yield* Effect.serviceOption(SqlClient.SqlClient);
    const claims = yield* Semaphore.make(1);
    const streams = new Map<string, ClaimStream>();
    const available = yield* Queue.dropping<void>(64);
    const cancellations = new Map<string, Deferred.Deferred<void>>();
    const settled = new Map<string, Deferred.Deferred<void>>();
    const uncertainSettlements = new Set<string>();

    const outsideSqlTransaction = Effect.gen(function* () {
      const sql = Option.orElse(
        yield* Effect.serviceOption(SqlClient.SqlClient),
        () => constructionSql,
      );
      if (
        Option.isSome(sql) &&
        Option.isSome(yield* Effect.serviceOption(sql.value.transactionService))
      ) {
        return yield* new EffectOutbox.EffectOutboxError({
          operation: "mixed-storage-transaction",
        });
      }
    });

    // Idle polls already belong to the worker span and must not create a span per database request.
    const request = Effect.fnUntraced(function* <A>(input: unknown, schema: Schema.Decoder<A>) {
      yield* outsideSqlTransaction;
      return yield* database.request(input, schema);
    });
    const mapError = (operation: string, effectId?: string) =>
      Effect.mapError((cause: unknown) =>
        isEffectOutboxError(cause)
          ? cause
          : new EffectOutbox.EffectOutboxError({
              operation,
              ...(effectId === undefined ? {} : { effectId }),
              cause,
            }),
      );
    const notifyAvailable = (count = 1) =>
      Queue.offerAll(
        available,
        Array.from({ length: Math.min(64, Math.max(0, Math.floor(count))) }, () => undefined),
      ).pipe(Effect.asVoid);
    const signalSettled = (ids: ReadonlyArray<string>) =>
      Effect.forEach(
        ids,
        (id) => {
          const signal = settled.get(id);
          if (signal === undefined) return Effect.void;
          settled.delete(id);
          return Deferred.succeed(signal, undefined);
        },
        { discard: true },
      );

    const get = Effect.fn("NativeEffectOutbox.get")(
      function* (effectId: string) {
        const row = yield* request(
          { operation: "outbox_get", effect_id: effectId },
          Schema.NullOr(StoredEffect),
        );
        if (row !== null && row.id !== effectId)
          return yield* new NativeDatabase.NativeDatabaseOutcomeUnknown({ stage: "response" });
        return row === null ? Option.none() : Option.some(toEffect(row));
      },
      (effect, effectId) => effect.pipe(mapError("get", effectId)),
    );

    const recoverSettlement = Effect.fn("NativeEffectOutbox.recoverSettlement")(function* (
      effectId: string,
    ) {
      const current = yield* get(effectId);
      if (
        Option.isNone(current) ||
        current.value.status === "succeeded" ||
        current.value.status === "failed" ||
        current.value.status === "cancelled"
      ) {
        yield* signalSettled([effectId]);
      }
      if (Option.isSome(current) && current.value.status === "cancelled") {
        const signal = cancellations.get(effectId);
        if (signal !== undefined) yield* Deferred.succeed(signal, undefined);
      }
      if (Option.isNone(current) || current.value.status !== "running") yield* notifyAvailable();
      uncertainSettlements.delete(effectId);
    });

    const settledMutation = Effect.fn("NativeEffectOutbox.settle")(
      function* (
        operation: "outbox_succeed" | "outbox_fail" | "outbox_retry",
        input: {
          readonly effectId: string;
          readonly workerId: string;
          readonly error?: string;
          readonly delayMs?: number;
        },
      ) {
        const now = yield* DateTime.now;
        const changed = yield* request(
          {
            operation,
            effect_id: input.effectId,
            worker_id: input.workerId,
            now: DateTime.formatIso(now),
            ...(input.error === undefined ? {} : { error: input.error }),
            ...(operation !== "outbox_retry"
              ? {}
              : {
                  available_at: DateTime.formatIso(
                    DateTime.add(now, { milliseconds: Math.max(0, input.delayMs ?? 0) }),
                  ),
                }),
          },
          Schema.Boolean,
        ).pipe(
          Effect.catchTags({
            NativeDatabaseOutcomeUnknown: Effect.fnUntraced(function* (error) {
              uncertainSettlements.add(input.effectId);
              yield* recoverSettlement(input.effectId);
              return yield* error;
            }),
          }),
        );
        if (changed) {
          cancellations.delete(input.effectId);
          if (operation !== "outbox_retry") yield* signalSettled([input.effectId]);
          yield* notifyAvailable();
          uncertainSettlements.delete(input.effectId);
        } else if (uncertainSettlements.has(input.effectId)) {
          yield* recoverSettlement(input.effectId);
        }
        return changed;
      },
      (effect, operation, input) => effect.pipe(mapError(operation, input.effectId)),
    );

    return EffectOutbox.EffectOutboxV2.of({
      awaitAvailable: Queue.take(available),
      notifyAvailable,
      get,
      enqueue: Effect.fn("NativeEffectOutbox.enqueue")(function* (effects) {
        const now = DateTime.formatIso(yield* DateTime.now);
        yield* request(
          {
            operation: "outbox_enqueue",
            now,
            effects: effects.map((effect) => ({
              id: effect.id,
              command_id: effect.commandId,
              thread_id: effect.threadId,
              request: effect.request,
              available_at:
                effect.availableAt === undefined ? null : DateTime.formatIso(effect.availableAt),
            })),
          },
          Counter,
        );
      }, mapError("enqueue")),
      listByCommandId: Effect.fn("NativeEffectOutbox.listByCommandId")(function* (commandId) {
        const rows = yield* request(
          { operation: "outbox_list", command_id: commandId },
          Schema.Array(StoredEffect),
        );
        if (rows.some((row) => row.command_id !== commandId))
          return yield* new NativeDatabase.NativeDatabaseOutcomeUnknown({ stage: "response" });
        return rows.map(toEffect);
      }, mapError("list")),
      cancelUnsettled: Effect.fn("NativeEffectOutbox.cancelUnsettled")(function* ({
        threadId,
        effectTypes,
        reason,
      }) {
        return yield* request(
          {
            operation: "outbox_cancel",
            thread_id: threadId,
            effect_types: effectTypes,
            reason,
            now: DateTime.formatIso(yield* DateTime.now),
          },
          Schema.Array(Schema.String),
        );
      }, mapError("cancel-unsettled")),
      signalCancellations: Effect.fn("NativeEffectOutbox.signalCancellations")(function* (ids) {
        yield* Effect.forEach(
          ids,
          (id) => {
            const signal = cancellations.get(id);
            return signal === undefined ? Effect.void : Deferred.succeed(signal, undefined);
          },
          { discard: true },
        );
        yield* signalSettled(ids);
        if (ids.length > 0) yield* notifyAvailable(ids.length);
      }),
      awaitCancellation: Effect.fn("NativeEffectOutbox.awaitCancellation")((id) => {
        const signal = cancellations.get(id) ?? Deferred.makeUnsafe<void>();
        cancellations.set(id, signal);
        return Deferred.await(signal);
      }),
      clearCancellation: Effect.fn("NativeEffectOutbox.clearCancellation")((id) =>
        Effect.sync(() => void cancellations.delete(id)),
      ),
      awaitSettled: Effect.fn("NativeEffectOutbox.awaitSettled")(function* (id) {
        const signal = settled.get(id) ?? Deferred.makeUnsafe<void>();
        settled.set(id, signal);
        const effect = yield* get(id);
        if (Option.exists(effect, ({ status }) => status === "pending" || status === "running"))
          return yield* Deferred.await(signal);
        yield* signalSettled([id]);
      }),
      reconcileAfterProcessLoss: Effect.gen(function* () {
        const result = yield* request(
          { operation: "outbox_reconcile", now: DateTime.formatIso(yield* DateTime.now) },
          Schema.Struct({ requeued: Counter, cancelled: Counter }),
        );
        if (result.requeued > 0) yield* notifyAvailable(result.requeued);
        return result;
      }).pipe(mapError("reconcile-process-loss")),
      // Only claims are serialized; the worker still executes effects concurrently.
      claimNext: Effect.fnUntraced(
        function* ({ workerId, excludeRestartContinuations = false }) {
          yield* Effect.forEach(uncertainSettlements, recoverSettlement, { discard: true });
          let stream = streams.get(workerId);
          if (stream === undefined) {
            stream = { id: yield* random.uuidV4(), sequence: 1, pending: undefined };
            streams.set(workerId, stream);
          }
          if (
            stream.pending !== undefined &&
            stream.pending.exclude_restart_continuations !== excludeRestartContinuations
          ) {
            return yield* new EffectOutbox.EffectOutboxError({
              operation: "unresolved-claim-mode",
            });
          }
          if (stream.pending === undefined) {
            stream.pending = {
              operation: "outbox_claim_once",
              stream_id: stream.id,
              sequence: stream.sequence,
              worker_id: workerId,
              now: DateTime.formatIso(yield* DateTime.now),
              exclude_restart_continuations: excludeRestartContinuations,
            };
          }
          const input = stream.pending;
          const attempt = request(input, ClaimOutcome);
          const result = yield* attempt.pipe(
            Effect.catchTags({
              NativeDatabaseOutcomeUnknown: () => attempt,
              NativeDatabaseRejected: (error) =>
                error.kind === "transaction_conflict" ? attempt : Effect.fail(error),
            }),
          );
          if (
            (result.state === "retired" && !result.replayed) ||
            (result.state === "claimed" &&
              (result.effect.status !== "running" ||
                result.effect.lease_owner !== workerId ||
                result.effect.updated_at !== input.now ||
                result.effect.available_at > input.now ||
                result.effect.attempt_count < 1 ||
                result.effect.completed_at !== null ||
                result.effect.last_error !== null ||
                (excludeRestartContinuations &&
                  result.effect.request.type === "provider-runtime.continue")))
          )
            return yield* new NativeDatabase.NativeDatabaseOutcomeUnknown({ stage: "response" });
          stream.sequence += 1;
          stream.pending = undefined;
          if (result.state !== "claimed") return Option.none();
          cancellations.set(result.effect.id, Deferred.makeUnsafe<void>());
          return Option.some(toEffect(result.effect));
        },
        claims.withPermit,
        mapError("claim"),
      ),
      nextClaimableAt: request(
        { operation: "outbox_next_claimable_at" },
        Schema.NullOr(Timestamp),
      ).pipe(
        Effect.map((value) => (value === null ? Option.none() : DateTime.make(value))),
        mapError("next-claimable"),
      ),
      // Settlement wrappers share the NativeEffectOutbox.settle span.
      succeed: Effect.fnUntraced(function* (input) {
        return yield* settledMutation("outbox_succeed", input);
      }),
      retry: Effect.fnUntraced(function* (input) {
        return yield* settledMutation("outbox_retry", input);
      }),
      fail: Effect.fnUntraced(function* (input) {
        return yield* settledMutation("outbox_fail", input);
      }),
      pruneSettled: Effect.gen(function* () {
        const before = DateTime.formatIso(
          DateTime.subtractDuration(yield* DateTime.now, EffectOutbox.SETTLED_EFFECT_RETENTION),
        );
        let count = 0;
        while (true) {
          const removed = yield* request(
            { operation: "outbox_prune", before, limit: 500 },
            Counter,
          );
          count += removed;
          if (removed < 500) return count;
        }
      }).pipe(mapError("prune-settled")),
    });
  }),
);
