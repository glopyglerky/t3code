#!/usr/bin/env node
import * as NodeAssert from "node:assert/strict";
import * as NodeChildProcess from "node:child_process";
import * as NodeCrypto from "node:crypto";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { CommandId, RunId, ThreadId } from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as References from "effect/References";
import * as Schema from "effect/Schema";
import * as SecureRandom from "effect/crypto/SecureRandom";

import * as EffectOutbox from "../apps/server/src/orchestration-v2/EffectOutbox.ts";
import * as EffectWorker from "../apps/server/src/orchestration-v2/EffectWorker.ts";
import * as NativeDatabase from "../apps/server/src/orchestration-v2/NativeDatabase.ts";
import * as NativeEffectOutbox from "../apps/server/src/orchestration-v2/NativeEffectOutbox.ts";
import { createFixture } from "./database-fixture.mjs";

const [database, proofPath] = process.argv.slice(2);
NodeAssert.ok(
  database && NodePath.isAbsolute(database),
  "Pass an absolute database executable path",
);
await NodeFSP.access(database, NodeFSP.constants.X_OK);
const nowMillis = Date.parse("2026-11-08T12:00:00.123Z");
const iso = (offset = 0) => new Date(nowMillis + offset).toISOString();
const checks = [];
const metrics = {
  committedClaimResponsesDiscarded: 0,
  committedMutationResponsesDiscarded: 0,
  readResponsesDiscarded: 0,
  effectsExecuted: 0,
};
const workerId = "native-fixture-worker";

function fixedClock(milliseconds) {
  const clock = Effect.runSync(Clock.Clock);
  return Object.assign(Object.create(clock), {
    currentTimeMillis: Effect.succeed(milliseconds),
    currentTimeMillisUnsafe: () => milliseconds,
  });
}
const at = (offset, effect) =>
  effect.pipe(Effect.provideService(Clock.Clock, fixedClock(nowMillis + offset)));
const pending = (id, overrides = {}) => ({
  id,
  commandId: CommandId.make(`command:${id}`),
  threadId: ThreadId.make(`thread:${id}`),
  request: { type: "terminal.cleanup" },
  ...overrides,
});
function row(value) {
  NodeAssert.equal(Option.isSome(value), true);
  return value.value;
}
function jsonValueCount(value) {
  if (value === null || typeof value !== "object") return 1;
  return 1 + Object.values(value).reduce((total, entry) => total + jsonValueCount(entry), 0);
}
function beforeWorkerEnds(signal, fiber) {
  return Effect.raceFirst(
    Deferred.await(signal),
    Fiber.join(fiber).pipe(
      Effect.flatMap(() =>
        Effect.die(new Error("Worker completed before the expected execution barrier")),
      ),
    ),
  );
}

async function scenario(description, run) {
  const fixture = await createFixture();
  try {
    const initialized = NodeChildProcess.spawnSync(database, fixture.cliArgs("initialize"), {
      env: {},
      input: '{"operation":"snapshot"}\n',
      encoding: "utf8",
      timeout: 15_000,
    });
    NodeAssert.equal(initialized.status, 0, initialized.stderr);
    NodeAssert.deepEqual(JSON.parse(initialized.stdout), {
      ok: true,
      result: { revision: 0, last_sequence: 0, projections: [] },
    });
    await Effect.runPromise(
      Effect.gen(function* () {
        const actual = yield* NativeDatabase.make({
          executable: database,
          databasePath: fixture.databasePath,
        });
        const state = {
          dropClaimResponses: 0,
          dropResponses: new Map(),
          requests: [],
          claimRequests: [],
          executions: [],
          readBarriers: new Map(),
          execute: undefined,
          transformClaimResponse: undefined,
        };
        const service = NativeDatabase.NativeDatabase.of({
          request: Effect.fn("NativeOutboxWorkerFixture.request")(function* (input, schema) {
            state.requests.push(structuredClone(input));
            const discardResponse = state.dropResponses.get(input.operation) ?? 0;
            if (discardResponse > 0) state.dropResponses.set(input.operation, discardResponse - 1);
            if (input.operation === "outbox_claim_once")
              state.claimRequests.push(structuredClone(input));
            const result = yield* actual.request(input, schema);
            if (input.operation === "outbox_get") {
              const barrier = state.readBarriers.get(input.effect_id);
              if (barrier !== undefined) {
                state.readBarriers.delete(input.effect_id);
                yield* Deferred.succeed(barrier, undefined);
              }
            }
            if (input.operation === "outbox_claim_once" && state.dropClaimResponses > 0) {
              state.dropClaimResponses -= 1;
              metrics.committedClaimResponsesDiscarded += 1;
              return yield* new NativeDatabase.NativeDatabaseOutcomeUnknown({
                stage: "response",
              });
            }
            if (discardResponse > 0) {
              if (input.operation === "outbox_get") metrics.readResponsesDiscarded += 1;
              else metrics.committedMutationResponsesDiscarded += 1;
              return yield* new NativeDatabase.NativeDatabaseOutcomeUnknown({
                stage: "response",
              });
            }
            if (
              input.operation === "outbox_claim_once" &&
              state.transformClaimResponse !== undefined
            ) {
              return yield* Schema.decodeUnknownEffect(schema, { onExcessProperty: "error" })(
                state.transformClaimResponse(structuredClone(result)),
              ).pipe(
                Effect.mapError(
                  () => new NativeDatabase.NativeDatabaseOutcomeUnknown({ stage: "response" }),
                ),
              );
            }
            return result;
          }),
        });
        const native = NativeEffectOutbox.layer.pipe(
          Layer.provide(Layer.succeed(NativeDatabase.NativeDatabase, service)),
          Layer.provide(SecureRandom.layerWebCrypto),
        );
        const executor = Layer.succeed(EffectWorker.OrchestrationEffectExecutorV2, {
          execute: (effect, options) =>
            Effect.suspend(() => {
              state.executions.push({ id: effect.id, attemptCount: effect.attemptCount, options });
              metrics.effectsExecuted += 1;
              return state.execute === undefined ? Effect.void : state.execute(effect, options);
            }),
        });
        const services = EffectWorker.layerWithOptions({ workerId }).pipe(
          Layer.provideMerge(Layer.mergeAll(native, executor)),
        );
        yield* run({ state, actual }).pipe(Effect.provide(services));
      }).pipe(
        Effect.scoped,
        Effect.provide(NodeServices.layer),
        Effect.provideService(Clock.Clock, fixedClock(nowMillis)),
        Effect.provideService(References.MinimumLogLevel, "None"),
      ),
    );
    checks.push(description);
  } finally {
    await fixture.close();
  }
}

await scenario(
  "the server outbox adapter maps persisted rows, retry deadlines, notifications, settlement and retention",
  () =>
    Effect.gen(function* () {
      const outbox = yield* EffectOutbox.EffectOutboxV2;
      const command = CommandId.make("adapter-command");
      const thread = ThreadId.make("adapter-thread");
      yield* outbox.awaitSettled("missing");
      NodeAssert.equal(Option.isNone(yield* outbox.get("missing")), true);
      yield* outbox.enqueue([
        pending("adapter-a", { commandId: command, threadId: thread }),
        pending("adapter-b", { commandId: command, threadId: thread }),
      ]);
      const rows = yield* outbox.listByCommandId(command);
      NodeAssert.deepEqual(
        rows.map((value) => [value.id, value.status, value.attemptCount]),
        [
          ["adapter-a", "pending", 0],
          ["adapter-b", "pending", 0],
        ],
      );
      NodeAssert.equal(rows[0].commandId, command);
      NodeAssert.equal(rows[0].threadId, thread);
      NodeAssert.equal(rows[0].createdAt, iso());
      NodeAssert.equal(rows[0].leaseOwner, null);
      NodeAssert.equal(rows[0].completedAt, null);
      NodeAssert.equal(DateTime.formatIso(row(yield* outbox.nextClaimableAt())), iso());
      yield* outbox.notifyAvailable(2);
      yield* outbox.awaitAvailable;
      yield* outbox.awaitAvailable;
      const first = row(yield* outbox.claimNext({ workerId }));
      NodeAssert.equal(first.id, "adapter-a");
      NodeAssert.equal(first.attemptCount, 1);
      NodeAssert.equal(
        yield* outbox.succeed({ effectId: first.id, workerId: "wrong-owner" }),
        false,
      );
      NodeAssert.equal(
        yield* outbox.retry({ effectId: first.id, workerId, error: "retry detail", delayMs: 25 }),
        true,
      );
      const retried = row(yield* outbox.get(first.id));
      NodeAssert.equal(retried.status, "pending");
      NodeAssert.equal(retried.availableAt, iso(25));
      NodeAssert.equal(retried.lastError, "retry detail");
      NodeAssert.equal(Option.isNone(yield* outbox.claimNext({ workerId })), true);
      const reclaimed = row(yield* at(25, outbox.claimNext({ workerId })));
      NodeAssert.equal(reclaimed.id, first.id);
      NodeAssert.equal(reclaimed.attemptCount, 2);
      NodeAssert.equal(
        yield* at(25, outbox.fail({ effectId: first.id, workerId, error: "failed detail" })),
        true,
      );
      yield* outbox.awaitSettled(first.id);
      const second = row(yield* at(25, outbox.claimNext({ workerId })));
      NodeAssert.equal(second.id, "adapter-b");
      NodeAssert.equal(yield* at(25, outbox.succeed({ effectId: second.id, workerId })), true);
      NodeAssert.equal(row(yield* outbox.get(second.id)).completedAt, iso(25));
      NodeAssert.equal(yield* at(8 * 86_400_000, outbox.pruneSettled), 1);
      NodeAssert.equal(Option.isNone(yield* outbox.get(second.id)), true);
      NodeAssert.equal(row(yield* outbox.get(first.id)).status, "failed");
    }),
);

await scenario(
  "the real worker recovers a lost committed claim response and signals awaitSettled after one execution",
  ({ state }) =>
    Effect.gen(function* () {
      const outbox = yield* EffectOutbox.EffectOutboxV2;
      const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
      yield* outbox.enqueue([pending("lost-response")]);
      const read = yield* Deferred.make();
      state.readBarriers.set("lost-response", read);
      const settled = yield* outbox
        .awaitSettled("lost-response")
        .pipe(Effect.andThen(outbox.get("lost-response")), Effect.forkScoped);
      yield* Deferred.await(read);
      state.dropClaimResponses = 1;
      NodeAssert.equal(yield* worker.runOnce, true);
      NodeAssert.equal(row(yield* Fiber.join(settled)).status, "succeeded");
      NodeAssert.deepEqual(
        state.executions.map(({ id, attemptCount }) => [id, attemptCount]),
        [["lost-response", 1]],
      );
      NodeAssert.equal(state.claimRequests.length, 2);
      NodeAssert.deepEqual(state.claimRequests[0], state.claimRequests[1]);
      const persisted = row(yield* outbox.get("lost-response"));
      NodeAssert.equal(persisted.status, "succeeded");
      NodeAssert.equal(persisted.attemptCount, 1);
      NodeAssert.equal(persisted.leaseOwner, null);
    }),
);

await scenario(
  "two lost claim responses return an error and the next worker call resolves the original tuple before dispatch",
  ({ state }) =>
    Effect.gen(function* () {
      const outbox = yield* EffectOutbox.EffectOutboxV2;
      const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
      yield* outbox.enqueue([pending("unresolved-claim")]);
      state.dropClaimResponses = 2;
      const failure = yield* worker.runOnce.pipe(Effect.flip);
      NodeAssert.equal(failure._tag, "OrchestrationEffectWorkerError");
      NodeAssert.equal(state.executions.length, 0);
      const unresolved = row(yield* outbox.get("unresolved-claim"));
      NodeAssert.equal(unresolved.status, "running");
      NodeAssert.equal(unresolved.attemptCount, 1);
      NodeAssert.equal(state.claimRequests.length, 2);
      NodeAssert.deepEqual(state.claimRequests[0], state.claimRequests[1]);
      NodeAssert.equal(yield* at(60_000, worker.runOnce), true);
      NodeAssert.equal(state.claimRequests.length, 3);
      NodeAssert.deepEqual(state.claimRequests[2], state.claimRequests[0]);
      NodeAssert.deepEqual(
        state.executions.map(({ id, attemptCount }) => [id, attemptCount]),
        [["unresolved-claim", 1]],
      );
      NodeAssert.equal(row(yield* outbox.get("unresolved-claim")).status, "succeeded");
      NodeAssert.equal(yield* at(60_000, worker.drain()), 0);
    }),
);

await scenario(
  "an unresolved claim blocks a worker mode change until its original empty result is resolved",
  ({ state }) =>
    Effect.gen(function* () {
      const outbox = yield* EffectOutbox.EffectOutboxV2;
      const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
      state.dropClaimResponses = 2;
      NodeAssert.equal(
        (yield* worker.runOnce.pipe(Effect.flip))._tag,
        "OrchestrationEffectWorkerError",
      );
      NodeAssert.equal(
        (yield* worker.runRecoveryOnce.pipe(Effect.flip))._tag,
        "OrchestrationEffectWorkerError",
      );
      NodeAssert.equal(state.claimRequests.length, 2);
      NodeAssert.deepEqual(state.claimRequests[0], state.claimRequests[1]);
      NodeAssert.equal(state.claimRequests[0].exclude_restart_continuations, false);
      yield* outbox.enqueue([
        pending("a-continuation", {
          request: { type: "provider-runtime.continue", sourceRunId: RunId.make("fixture-run") },
        }),
        pending("b-ordinary"),
      ]);
      NodeAssert.equal(yield* at(60_000, worker.runOnce), false);
      NodeAssert.deepEqual(state.claimRequests[2], state.claimRequests[0]);
      NodeAssert.equal(state.executions.length, 0);
      NodeAssert.equal(yield* at(60_000, worker.runRecoveryOnce), true);
      NodeAssert.equal(yield* at(60_000, worker.runOnce), true);
      NodeAssert.equal(state.claimRequests[3].stream_id, state.claimRequests[0].stream_id);
      NodeAssert.equal(state.claimRequests[3].sequence, 2);
      NodeAssert.equal(state.claimRequests[3].exclude_restart_continuations, true);
      NodeAssert.equal(state.claimRequests[4].sequence, 3);
      NodeAssert.equal(state.claimRequests[4].exclude_restart_continuations, false);
      NodeAssert.deepEqual(
        state.executions.map(({ id }) => id),
        ["b-ordinary", "a-continuation"],
      );
      for (const id of ["a-continuation", "b-ordinary"]) {
        const persisted = row(yield* outbox.get(id));
        NodeAssert.equal(persisted.status, "succeeded");
        NodeAssert.equal(persisted.attemptCount, 1);
      }
    }),
);

await scenario(
  "four concurrent calls to the real worker execute distinct effects with one attempt each",
  ({ state }) =>
    Effect.gen(function* () {
      const outbox = yield* EffectOutbox.EffectOutboxV2;
      const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
      const ids = Array.from({ length: 4 }, (_, index) => `parallel-${index}`);
      yield* outbox.enqueue(ids.map((id) => pending(id)));
      const entered = yield* Deferred.make();
      const release = yield* Deferred.make();
      state.execute = () =>
        Effect.gen(function* () {
          if (state.executions.length === 4) yield* Deferred.succeed(entered, undefined);
          yield* Deferred.await(release);
        });
      const running = yield* Effect.all(
        ids.map(() => worker.runOnce),
        { concurrency: "unbounded" },
      ).pipe(Effect.forkScoped);
      yield* Effect.gen(function* () {
        yield* beforeWorkerEnds(entered, running);
        NodeAssert.deepEqual(state.executions.map(({ id }) => id).sort(), ids);
        for (const execution of state.executions) NodeAssert.equal(execution.attemptCount, 1);
        yield* Deferred.succeed(release, undefined);
        NodeAssert.deepEqual(yield* Fiber.join(running), [true, true, true, true]);
        for (const id of ids) NodeAssert.equal(row(yield* outbox.get(id)).status, "succeeded");
      }).pipe(Effect.ensuring(Deferred.succeed(release, undefined)));
    }),
);

await scenario(
  "durable cancellation during execution resolves waiters and blocks late worker completion",
  ({ state }) =>
    Effect.gen(function* () {
      const outbox = yield* EffectOutbox.EffectOutboxV2;
      const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
      const input = pending("cancel-in-flight");
      yield* outbox.enqueue([input]);
      const entered = yield* Deferred.make();
      const release = yield* Deferred.make();
      let lateCompletions = 0;
      state.execute = () =>
        Effect.gen(function* () {
          yield* Deferred.succeed(entered, undefined);
          yield* Deferred.await(release);
          lateCompletions += 1;
        }).pipe(Effect.uninterruptible);
      const running = yield* worker.runOnce.pipe(Effect.forkScoped);
      yield* Effect.gen(function* () {
        yield* beforeWorkerEnds(entered, running);
        const read = yield* Deferred.make();
        state.readBarriers.set(input.id, read);
        const settled = yield* outbox.awaitSettled(input.id).pipe(Effect.forkScoped);
        yield* Deferred.await(read);
        const cancelled = yield* outbox.cancelUnsettled({
          threadId: input.threadId,
          effectTypes: ["terminal.cleanup"],
          reason: "fixture cancelled during execution",
        });
        NodeAssert.deepEqual(cancelled, [input.id]);
        yield* outbox.signalCancellations(cancelled);
        yield* Deferred.succeed(release, undefined);
        NodeAssert.equal(yield* Fiber.join(running), true);
        yield* Fiber.join(settled);
        NodeAssert.equal(lateCompletions, 1);
        NodeAssert.equal(yield* outbox.succeed({ effectId: input.id, workerId }), false);
        const persisted = row(yield* outbox.get(input.id));
        NodeAssert.equal(persisted.status, "cancelled");
        NodeAssert.equal(persisted.attemptCount, 1);
        NodeAssert.equal(persisted.leaseOwner, null);
        NodeAssert.equal(persisted.lastError, "fixture cancelled during execution");
      }).pipe(Effect.ensuring(Deferred.succeed(release, undefined)));
    }),
);

await scenario(
  "a cancelled unresolved claim resolves as retired and is never sent to the executor",
  ({ state, actual }) =>
    Effect.gen(function* () {
      const outbox = yield* EffectOutbox.EffectOutboxV2;
      const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
      const input = pending("cancel-before-recovery");
      yield* outbox.enqueue([input]);
      state.dropClaimResponses = 2;
      yield* worker.runOnce.pipe(Effect.flip);
      NodeAssert.equal(state.executions.length, 0);
      const cancelled = yield* outbox.cancelUnsettled({
        threadId: input.threadId,
        effectTypes: ["terminal.cleanup"],
        reason: "cancelled while response was unavailable",
      });
      yield* outbox.signalCancellations(cancelled);
      NodeAssert.equal(yield* at(60_000, worker.runOnce), false);
      NodeAssert.equal(state.executions.length, 0);
      NodeAssert.deepEqual(state.claimRequests[2], state.claimRequests[0]);
      const stored = yield* actual.request(
        { operation: "outbox_get", effect_id: input.id },
        Schema.Unknown,
      );
      NodeAssert.equal(stored.status, "cancelled");
      NodeAssert.equal(stored.attempt_count, 1);
    }),
);

await scenario(
  "a lost success response wakes settlement waiters without rerunning the executor or settlement",
  ({ state }) =>
    Effect.gen(function* () {
      const outbox = yield* EffectOutbox.EffectOutboxV2;
      const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
      const input = pending("lost-success-response");
      yield* outbox.enqueue([input]);
      const entered = yield* Deferred.make();
      const release = yield* Deferred.make();
      state.execute = () =>
        Effect.gen(function* () {
          yield* Deferred.succeed(entered, undefined);
          yield* Deferred.await(release);
        });
      state.dropResponses.set("outbox_succeed", 1);
      const running = yield* worker.runOnce.pipe(Effect.exit, Effect.forkScoped);
      yield* Effect.gen(function* () {
        yield* beforeWorkerEnds(entered, running);
        const read = yield* Deferred.make();
        state.readBarriers.set(input.id, read);
        const settled = yield* outbox
          .awaitSettled(input.id)
          .pipe(Effect.andThen(outbox.get(input.id)), Effect.forkScoped);
        yield* Deferred.await(read);
        yield* Deferred.succeed(release, undefined);
        NodeAssert.equal(Exit.isFailure(yield* Fiber.join(running)), true);
        const observed = row(yield* Fiber.join(settled));
        NodeAssert.equal(observed.status, "succeeded");
        NodeAssert.equal(observed.attemptCount, 1);
        NodeAssert.deepEqual(
          state.executions.map(({ id }) => id),
          [input.id],
        );
        NodeAssert.equal(
          state.requests.filter(({ operation }) => operation === "outbox_succeed").length,
          1,
        );
        NodeAssert.equal(row(yield* outbox.get(input.id)).status, "succeeded");
      }).pipe(Effect.ensuring(Deferred.succeed(release, undefined)));
    }),
);

await scenario(
  "lost settlement and two failed readbacks retain a waiter until the next claim recovers the terminal row",
  ({ state, actual }) =>
    Effect.gen(function* () {
      const outbox = yield* EffectOutbox.EffectOutboxV2;
      const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
      const input = pending("settlement-readback-unavailable");
      yield* outbox.enqueue([input]);
      const entered = yield* Deferred.make();
      const release = yield* Deferred.make();
      const completed = yield* Deferred.make();
      state.execute = () =>
        Effect.gen(function* () {
          yield* Deferred.succeed(entered, undefined);
          yield* Deferred.await(release);
        });
      const running = yield* worker.runOnce.pipe(Effect.exit, Effect.forkScoped);
      yield* Effect.gen(function* () {
        yield* beforeWorkerEnds(entered, running);
        const read = yield* Deferred.make();
        state.readBarriers.set(input.id, read);
        const settled = yield* outbox
          .awaitSettled(input.id)
          .pipe(Effect.andThen(Deferred.succeed(completed, undefined)), Effect.forkScoped);
        yield* Deferred.await(read);
        state.dropResponses.set("outbox_succeed", 1);
        state.dropResponses.set("outbox_get", 2);
        yield* Deferred.succeed(release, undefined);
        NodeAssert.equal(Exit.isFailure(yield* Fiber.join(running)), true);
        NodeAssert.equal(state.dropResponses.get("outbox_get"), 0);
        NodeAssert.equal(yield* Deferred.isDone(completed), false);
        const stored = yield* actual.request(
          { operation: "outbox_get", effect_id: input.id },
          Schema.Unknown,
        );
        NodeAssert.equal(stored.status, "succeeded");
        NodeAssert.equal(stored.attempt_count, 1);
        NodeAssert.equal(yield* at(60_000, worker.runOnce), false);
        yield* Fiber.join(settled);
        NodeAssert.equal(yield* Deferred.isDone(completed), true);
        NodeAssert.deepEqual(
          state.executions.map(({ id }) => id),
          [input.id],
        );
        NodeAssert.equal(
          state.requests.filter(({ operation }) => operation === "outbox_succeed").length,
          1,
        );
        NodeAssert.equal(row(yield* outbox.get(input.id)).status, "succeeded");
      }).pipe(Effect.ensuring(Deferred.succeed(release, undefined)));
    }),
);

await scenario(
  "a lost retry response wakes availability from durable pending state and preserves the original error",
  ({ state }) =>
    Effect.gen(function* () {
      const outbox = yield* EffectOutbox.EffectOutboxV2;
      const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
      const input = pending("lost-retry-response");
      yield* outbox.enqueue([input]);
      NodeAssert.equal(row(yield* outbox.claimNext({ workerId })).id, input.id);
      const available = yield* outbox.awaitAvailable.pipe(Effect.forkScoped);
      state.dropResponses.set("outbox_retry", 1);
      const failure = yield* outbox
        .retry({
          effectId: input.id,
          workerId,
          error: "fixture retry with lost response",
          delayMs: 250,
        })
        .pipe(Effect.flip);
      NodeAssert.equal(failure._tag, "EffectOutboxError");
      yield* Fiber.join(available);
      const observed = row(yield* outbox.get(input.id));
      NodeAssert.equal(observed.status, "pending");
      NodeAssert.equal(observed.availableAt, iso(250));
      NodeAssert.equal(observed.attemptCount, 1);
      NodeAssert.equal(
        state.requests.filter(({ operation }) => operation === "outbox_retry").length,
        1,
      );
      NodeAssert.equal(yield* at(250, worker.runOnce), true);
      NodeAssert.deepEqual(
        state.executions.map(({ id, attemptCount }) => [id, attemptCount]),
        [[input.id, 2]],
      );
      NodeAssert.equal(row(yield* outbox.get(input.id)).status, "succeeded");
    }),
);

for (const [label, transform] of [
  [
    "future claim availability",
    (result) => ({ ...result, effect: { ...result.effect, available_at: iso(60_000) } }),
  ],
  [
    "non-null running claim error",
    (result) => ({
      ...result,
      effect: { ...result.effect, last_error: "unexpected retained error" },
    }),
  ],
  [
    "invalid effect identifier",
    (result) => ({ ...result, effect: { ...result.effect, id: "invalid\u0000id" } }),
  ],
  ["unreplayed retired claim", () => ({ state: "retired", replayed: false, effect: null })],
]) {
  await scenario(
    `${label} fails before execution and retains the original recoverable request`,
    ({ state }) =>
      Effect.gen(function* () {
        const outbox = yield* EffectOutbox.EffectOutboxV2;
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        const input = pending("malformed-reply");
        yield* outbox.enqueue([input]);
        state.transformClaimResponse = transform;
        NodeAssert.equal(
          (yield* worker.runOnce.pipe(Effect.flip))._tag,
          "OrchestrationEffectWorkerError",
        );
        NodeAssert.equal(state.executions.length, 0);
        const before = row(yield* outbox.get(input.id));
        NodeAssert.equal(before.status, "running");
        NodeAssert.equal(before.attemptCount, 1);
        state.transformClaimResponse = undefined;
        NodeAssert.equal(yield* at(120_000, worker.runOnce), true);
        for (const request of state.claimRequests)
          NodeAssert.deepEqual(request, state.claimRequests[0]);
        NodeAssert.deepEqual(
          state.executions.map(({ id, attemptCount }) => [id, attemptCount]),
          [[input.id, 1]],
        );
        NodeAssert.equal(row(yield* outbox.get(input.id)).status, "succeeded");
      }),
  );
}

await scenario(
  "a 262144-value request remains claimable and readable when response metadata adds values",
  ({ state }) =>
    Effect.gen(function* () {
      const outbox = yield* EffectOutbox.EffectOutboxV2;
      const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
      const input = pending("wide-request", {
        request: {
          type: "runtime-request.respond",
          providerSessionId: "fixture-session",
          requestId: "fixture-request",
          answers: { values: [] },
        },
      });
      const wire = {
        operation: "outbox_enqueue",
        now: iso(),
        effects: [
          {
            id: input.id,
            command_id: input.commandId,
            thread_id: input.threadId,
            request: input.request,
            available_at: null,
          },
        ],
      };
      const valueCount = 262_144 - jsonValueCount(wire);
      input.request.answers.values = Array(valueCount).fill(0);
      NodeAssert.equal(jsonValueCount(wire), 262_144);
      yield* outbox.enqueue([input]);
      NodeAssert.equal(jsonValueCount(state.requests.at(-1)), 262_144);
      state.execute = (claimed) =>
        Effect.sync(() => {
          NodeAssert.equal(claimed.request.answers.values.length, valueCount);
          NodeAssert.equal(
            claimed.request.answers.values.every((value) => value === 0),
            true,
          );
        });
      NodeAssert.equal(yield* worker.runOnce, true);
      NodeAssert.deepEqual(
        state.executions.map(({ id, attemptCount }) => [id, attemptCount]),
        [[input.id, 1]],
      );
      const persisted = row(yield* outbox.get(input.id));
      NodeAssert.equal(persisted.status, "succeeded");
      NodeAssert.equal(persisted.request.answers.values.length, valueCount);
      NodeAssert.equal(state.claimRequests.length, 1);
    }),
);

const proof = {
  status: "passed",
  sourceCommit: process.env.GITHUB_SHA ?? null,
  databaseSha256: NodeCrypto.createHash("sha256")
    .update(await NodeFSP.readFile(database))
    .digest("hex"),
  passed: checks.length,
  ...metrics,
  checks,
  boundaries: [
    "real NativeDatabase, NativeEffectOutbox and OrchestrationEffectWorkerV2 with a controlled executor",
    "response faults occur after actual native claim commits; the production worker decides dispatch and settlement",
    "isolated redb files; no production data, provider process, network call or paid turn",
    "production project/thread transactions and server backend selection still require migration",
    "no exactly-once external effect or distributed fencing guarantee",
  ],
};
if (proofPath) {
  await NodeFSP.mkdir(NodePath.dirname(proofPath), { recursive: true });
  await NodeFSP.writeFile(proofPath, JSON.stringify(proof, null, 2) + "\n");
}
console.log(JSON.stringify(proof));
