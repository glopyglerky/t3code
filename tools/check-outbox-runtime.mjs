import * as NodeAssert from "node:assert/strict";
import * as NodeCrypto from "node:crypto";
import * as NodeModule from "node:module";

import * as NodeServices from "@effect/platform-node/NodeServices";
import * as contracts from "@t3tools/contracts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as References from "effect/References";
import * as Schedule from "effect/Schedule";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";

import * as SqlitePersistence from "../apps/server/src/persistence/Sqlite.ts";
import { forkParked } from "../apps/server/src/serverActivation.ts";
import { upstreamSource } from "./upstream-source.mjs";

const sourcePath = "apps/server/src/orchestration-v2/EffectOutbox.ts";
const original = upstreamSource(sourcePath);
export const oracleSource = {
  path: sourcePath,
  sha256: NodeCrypto.createHash("sha256").update(original).digest("hex"),
};
const source = NodeModule.stripTypeScriptTypes(original, { mode: "strip" })
  .replace(/^import\s[\s\S]*?;\s*$/gm, "")
  .replace(/^export (?=(?:function|const|class) )/gm, "");
const imports = {
  Context,
  DateTime,
  Deferred,
  Duration,
  Effect,
  Layer,
  Option,
  Queue,
  Schedule,
  Schema,
  SqlClient,
  NodeSqliteClient,
  forkParked,
  ...Object.fromEntries(
    [
      "CheckpointId",
      "CheckpointScopeId",
      "CommandId",
      "MessageId",
      "ModelSelection",
      "NodeId",
      "NonNegativeInt",
      "ProviderSessionId",
      "RunAttemptId",
      "ProviderApprovalDecision",
      "ProviderUserInputAnswers",
      "ProviderThreadId",
      "ProviderTurnId",
      "RunId",
      "RuntimeRequestId",
      "ThreadId",
    ].map((name) => [name, contracts[name]]),
  ),
};

// Only imports/TypeScript syntax are removed. The executed service is the exact
// pinned Git blob, including its SQL statements, predicates and state changes.
export const upstream = new Function(
  ...Object.keys(imports),
  `${source}\nreturn { EffectOutboxV2, layer, OrchestrationEffectRequestV2, PROCESS_BOUND_EFFECT_TYPES, REPLAY_SAFE_EFFECT_TYPES_AFTER_PROCESS_LOSS };`,
)(...Object.values(imports));

export function at(now, effect) {
  const milliseconds = Date.parse(now);
  NodeAssert.equal(new Date(milliseconds).toISOString(), now);
  const clock = Effect.runSync(Clock.Clock);
  return effect.pipe(
    Effect.provideService(
      Clock.Clock,
      Object.assign(Object.create(clock), {
        currentTimeMillis: Effect.succeed(milliseconds),
        currentTimeMillisUnsafe: () => milliseconds,
      }),
    ),
  );
}

export const snapshot = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const rows = yield* sql`
    SELECT rowid AS insertion_ordinal, * FROM orchestration_v2_effect_outbox ORDER BY rowid
  `;
  return rows.map((row) => {
    const request = JSON.parse(row.payload_json);
    NodeAssert.equal(request.type, row.effect_type);
    NodeAssert.equal(row.lease_expires_at, null);
    return {
      id: row.effect_id,
      command_id: row.command_id,
      thread_id: row.thread_id,
      request,
      status: row.status,
      attempt_count: row.attempt_count,
      available_at: row.available_at,
      lease_owner: row.lease_owner,
      lease_expires_at: row.lease_expires_at,
      created_at: row.created_at,
      updated_at: row.updated_at,
      completed_at: row.completed_at,
      last_error: row.last_error,
      insertion_ordinal: row.insertion_ordinal,
    };
  });
});

function assertServiceRow(actual, stored) {
  if (actual === null) {
    NodeAssert.equal(stored, undefined);
    return null;
  }
  NodeAssert.deepEqual(actual, {
    id: stored.id,
    commandId: stored.command_id,
    threadId: stored.thread_id,
    request: stored.request,
    status: stored.status,
    attemptCount: stored.attempt_count,
    availableAt: stored.available_at,
    leaseOwner: stored.lease_owner,
    createdAt: stored.created_at,
    updatedAt: stored.updated_at,
    completedAt: stored.completed_at,
    lastError: stored.last_error,
  });
  return stored;
}

export const invokeOracle = Effect.fn("OutboxOracle.invoke")(
  function* (outbox, input) {
    let result;
    switch (input.operation) {
      case "outbox_enqueue": {
        const before = yield* snapshot;
        yield* outbox.enqueue(
          input.effects.map((row) => ({
            id: row.id,
            commandId: contracts.CommandId.make(row.command_id),
            threadId: contracts.ThreadId.make(row.thread_id),
            request: row.request,
            ...(row.available_at == null
              ? {}
              : { availableAt: DateTime.makeUnsafe(row.available_at) }),
          })),
        );
        result = (yield* snapshot).length - before.length;
        break;
      }
      case "outbox_get":
        result = Option.getOrNull(yield* outbox.get(input.effect_id));
        break;
      case "outbox_list":
        result = yield* outbox.listByCommandId(contracts.CommandId.make(input.command_id));
        break;
      case "outbox_claim":
        result = Option.getOrNull(
          yield* outbox.claimNext({
            workerId: input.worker_id,
            excludeRestartContinuations: input.exclude_restart_continuations,
            ...(input.max_running_title_generations === undefined
              ? {}
              : { maxRunningTitleGenerations: input.max_running_title_generations }),
          }),
        );
        break;
      case "outbox_next_claimable_at": {
        const next = yield* outbox.nextClaimableAt(
          input.max_running_title_generations === undefined
            ? undefined
            : { maxRunningTitleGenerations: input.max_running_title_generations },
        );
        result = Option.isNone(next) ? null : DateTime.formatIso(next.value);
        break;
      }
      case "outbox_succeed":
        result = yield* outbox.succeed({ effectId: input.effect_id, workerId: input.worker_id });
        break;
      case "outbox_retry":
        result = yield* outbox.retry({
          effectId: input.effect_id,
          workerId: input.worker_id,
          error: input.error,
          delayMs: Date.parse(input.available_at) - Date.parse(input.now),
        });
        break;
      case "outbox_fail":
        result = yield* outbox.fail({
          effectId: input.effect_id,
          workerId: input.worker_id,
          error: input.error,
        });
        break;
      case "outbox_cancel":
        result = yield* outbox.cancelUnsettled({
          threadId: contracts.ThreadId.make(input.thread_id),
          effectTypes: input.effect_types,
          reason: input.reason,
        });
        break;
      case "outbox_reconcile":
        result = yield* outbox.reconcileAfterProcessLoss;
        break;
      case "outbox_prune":
        result = yield* outbox.pruneSettled;
        break;
      default:
        throw new Error(`Unknown oracle operation: ${input.operation}`);
    }
    const rows = yield* snapshot;
    if (input.operation === "outbox_claim" || input.operation === "outbox_get") {
      result = assertServiceRow(
        result,
        rows.find(({ id }) => id === result?.id),
      );
    } else if (input.operation === "outbox_list") {
      result = result.map((row) =>
        assertServiceRow(
          row,
          rows.find(({ id }) => id === row.id),
        ),
      );
    }
    return { result, rows };
  },
  (effect, _outbox, input) =>
    at(
      input.operation === "outbox_prune"
        ? new Date(Date.parse(input.before) + 7 * 86_400_000).toISOString()
        : (input.now ?? "2026-11-08T12:00:00.123Z"),
      effect,
    ),
);

export async function runOracleScenario(filename, body) {
  return Effect.runPromise(
    Effect.gen(function* () {
      return yield* body(yield* upstream.EffectOutboxV2);
    }).pipe(
      Effect.provide(
        upstream.layer.pipe(Layer.provideMerge(SqlitePersistence.layerFromPath(filename))),
      ),
      Effect.provide(NodeServices.layer),
      Effect.provideService(References.MinimumLogLevel, "Warning"),
      Effect.scoped,
    ),
  );
}
