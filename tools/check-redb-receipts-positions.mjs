#!/usr/bin/env node
import * as Assert from "node:assert/strict";
import * as FS from "node:fs/promises";
import * as Path from "node:path";
import * as Sqlite from "node:sqlite";
import * as Contracts from "@t3tools/contracts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as SqlSchema from "effect/sql/SqlSchema";

import * as ApplicationReceipts from "../apps/server/src/persistence/OrchestrationCommandReceipts.ts";
import * as NativeCommandReceipts from "../apps/server/src/persistence/NativeCommandReceipts.ts";
import * as CommandReceiptStore from "../apps/server/src/orchestration-v2/CommandReceiptStore.ts";
import * as TurnItemPositionStore from "../apps/server/src/orchestration-v2/TurnItemPositionStore.ts";
import * as NativeTurnItemPositionStore from "../apps/server/src/orchestration-v2/NativeTurnItemPositionStore.ts";
import * as NativeDatabase from "../apps/server/src/orchestration-v2/NativeDatabase.ts";
import { toPersistenceSqlError } from "../apps/server/src/persistence/Errors.ts";
import { createFixture } from "./database-fixture.mjs";
import { exportSqliteState } from "./export-sqlite-state.mjs";
import {
  createMigratedSqlite, finishProof, hash, loadUpstream, nativeRequest, normalized,
  withNative, withSqlite,
} from "./redb-domain-fixture.mjs";

const [executable, proofPath] = process.argv.slice(2);
Assert.ok(executable && Path.isAbsolute(executable), "Pass an absolute native database executable");
await FS.access(executable, FS.constants.X_OK);
const at = "2026-11-10T12:00:00.000Z";
const now = DateTime.makeUnsafe(at);
const imports = { ...Contracts, Context, DateTime, Effect, Layer, Option, Schema, SqlClient, SqlSchema, NodeSqliteClient, toPersistenceSqlError };
const receiptOracle = loadUpstream("apps/server/src/persistence/OrchestrationCommandReceipts.ts", imports, ["OrchestrationCommandReceiptRepository", "layer"]);
const positionOracle = loadUpstream("apps/server/src/orchestration-v2/TurnItemPositionStore.ts", imports, ["TurnItemPositionStoreV2", "layer"]);
const wrapperOracle = loadUpstream("apps/server/src/orchestration-v2/CommandReceiptStore.ts", {
  ...imports, OrchestrationCommandReceipts: receiptOracle.module,
}, ["CommandReceiptStoreV2", "layerFromApplicationReceipts"]);
const fixture = await createFixture();
const checks = [];

const receipt = (id, fields = {}) => ({
  commandId: id, threadId: "thread:commands", commandType: "thread.turn.start", acceptedAt: now,
  resultSequence: 0, status: "accepted", error: null, ...fields,
});
const allocate = (id, fields = {}) => ({ threadId: "thread:positions", turnItemId: id, runId: null, ...fields });
const expectFailure = (effect) => Effect.exit(effect).pipe(Effect.tap((exit) => Effect.sync(() => Assert.ok(Exit.isFailure(exit)))));

async function withServices(kind, filename, use) {
  const construct = (transaction, client) => Effect.gen(function* () {
    const receiptModule = kind === "upstream" ? receiptOracle.module : ApplicationReceipts;
    const wrapperModule = kind === "upstream" ? wrapperOracle.module : CommandReceiptStore;
    const positionModule = kind === "upstream" ? positionOracle.module : TurnItemPositionStore;
    const applicationReceipts = kind === "redb" ? yield* NativeCommandReceipts.make
      : Context.get(yield* Layer.build(receiptModule.layer), receiptModule.OrchestrationCommandReceiptRepository);
    const context = yield* Layer.build(wrapperModule.layerFromApplicationReceipts).pipe(
      Effect.provideService(receiptModule.OrchestrationCommandReceiptRepository, applicationReceipts),
    );
    const receipts = Context.get(context, wrapperModule.CommandReceiptStoreV2);
    const positions = kind === "redb" ? yield* NativeTurnItemPositionStore.make
      : Context.get(yield* Layer.build(positionModule.layer), positionModule.TurnItemPositionStoreV2);
    return yield* use({ applicationReceipts, receipts, positions, transaction, client });
  });
  return kind === "redb"
    ? withNative(executable, fixture, (client) => construct((body) => client.withTransaction(body), client))
    : withSqlite(filename, (sql) => construct((body) => sql.withTransaction(body)));
}

function exercise({ applicationReceipts, receipts, positions, transaction }) {
  return Effect.gen(function* () {
    const result = {};
    result.importedThread = yield* receipts.getByCommandId("imported:thread");
    result.importedProject = yield* receipts.getProjectByCommandId("imported:project");
    Assert.ok(Option.isSome(result.importedThread));
    Assert.ok(Option.isSome(result.importedProject));
    Assert.equal(result.importedProject.value.error, "");
    Assert.ok(Option.isNone(yield* receipts.getProjectByCommandId("imported:thread")));
    Assert.ok(Option.isNone(yield* receipts.getByCommandId("imported:project")));
    Assert.ok(Option.isNone(yield* receipts.getByCommandId("absent")));
    result.importedPosition = yield* positions.allocate(allocate("imported:item", { threadId: "thread:imported", runOrdinal: 99 }));
    Assert.equal(result.importedPosition, 37);
    result.transaction = yield* transaction(Effect.gen(function* () {
      Assert.equal(yield* receipts.insertIfAbsent(receipt("command:shared")), true);
      Assert.equal(yield* receipts.insertIfAbsent({
        commandId: "command:shared", projectId: "project:collision", commandType: "project.create",
        acceptedAt: now, resultSequence: 99, status: "rejected", error: "unchanged",
      }), false);
      Assert.ok(Option.isNone(yield* receipts.getProjectByCommandId("command:shared")));
      yield* receipts.upsert(receipt("command:shared", { resultSequence: 42, status: "rejected", error: "" }));
      Assert.equal(yield* positions.allocate(allocate("first")), 1);
      Assert.equal(yield* positions.allocate(allocate("second")), 2);
      Assert.equal(yield* positions.allocate(allocate("first", { runId: "run:seed", runOrdinal: 8 })), 1);
      Assert.equal(yield* positions.allocate(allocate("run", { runId: "run:seed" })), 3_000_001);
      Assert.equal(yield* positions.allocate(allocate("supplied", { runId: "run:seed", runOrdinal: 4 })), 4_000_001);
      Assert.equal(yield* positions.allocate(allocate("foreign", { runId: "run:foreign" })), 3);
      yield* expectFailure(transaction(Effect.gen(function* () {
        yield* receipts.upsert(receipt("command:shared", { resultSequence: 999 }));
        Assert.equal(yield* positions.allocate(allocate("rolled-back")), 4);
        return yield* Effect.fail("nested body failure");
      })));
      const restored = yield* receipts.getByCommandId("command:shared");
      Assert.equal(restored.value.resultSequence, 42);
      Assert.equal(yield* positions.allocate(allocate("sibling")), 4);
      yield* transaction(receipts.insertIfAbsent(receipt("command:nested")));
      const item = Schema.decodeUnknownSync(Contracts.OrchestrationV2TurnItem)({
        id: "item:normalize", type: "reasoning", threadId: "thread:normalize", runId: null,
        nodeId: null, providerThreadId: null, providerTurnId: null, nativeItemRef: null, parentItemId: null,
        ordinal: 400, status: "running", title: null, startedAt: now, completedAt: null, updatedAt: now,
        text: "Preserve this item", streaming: true,
      });
      const changed = yield* positions.normalize(item, 2);
      Assert.equal(changed.ordinal, 2_000_001);
      Assert.equal(changed.text, item.text);
      Assert.strictEqual(yield* positions.normalize(changed, 9), changed);
      const projectReceipt = {
        commandId: "command:project", projectId: "project:written", commandType: "project.create",
        acceptedAt: now, resultSequence: 43, status: "accepted", error: null,
      };
      Assert.equal(yield* receipts.insertIfAbsent(projectReceipt), true);
      return { restored, project: yield* receipts.getProjectByCommandId("command:project"), normalized: changed };
    }));
    yield* expectFailure(transaction(Effect.gen(function* () {
      yield* receipts.insertIfAbsent(receipt("command:discarded"));
      yield* positions.allocate(allocate("discarded"));
      return yield* Effect.die(new Error("outer defect"));
    })));
    Assert.ok(Option.isNone(yield* receipts.getByCommandId("command:discarded")));
    Assert.equal(yield* positions.allocate(allocate("after-rollback")), 5);
    result.overflow = yield* transaction(Effect.gen(function* () {
      yield* expectFailure(transaction(positions.allocate(allocate("conflict", { threadId: "thread:edge" }))));
      const next = yield* positions.allocate(allocate("next-range", { threadId: "thread:edge", runOrdinal: 1 }));
      const overflow = yield* positions.allocate(allocate("overflow", { threadId: "thread:edge", runOrdinal: 2 }));
      const following = yield* positions.allocate(allocate("following", { threadId: "thread:edge", runOrdinal: 3 }));
      Assert.deepEqual([next, overflow, following], [1_000_001, 3_000_000, 3_000_001]);
      return [next, overflow, following];
    }));
    const replaced = {
      commandId: "command:shared", aggregateKind: "project", aggregateId: "project:replacement",
      commandType: "project.delete", acceptedAt: "2026-11-11T00:00:00.000Z", resultSequence: 50,
      status: "accepted", error: null,
    };
    yield* applicationReceipts.upsert(replaced);
    Assert.ok(Option.isNone(yield* receipts.getByCommandId("command:shared")));
    result.replaced = yield* applicationReceipts.getByCommandId({ commandId: "command:shared" });
    Assert.deepEqual(Option.getOrThrow(result.replaced), replaced);
    return normalized(result);
  });
}

try {
  const source = Path.join(fixture.root, "source.sqlite");
  const sqlitePath = Path.join(fixture.root, "sqlite.sqlite");
  const upstreamPath = Path.join(fixture.root, "upstream.sqlite");
  const exported = Path.join(fixture.root, "source.ndjson");
  await createMigratedSqlite(source);
  const database = new Sqlite.DatabaseSync(source);
  try {
    const insertReceipt = database.prepare(`INSERT INTO orchestration_command_receipts
      (command_id, aggregate_kind, aggregate_id, command_type, accepted_at, result_sequence, status, error)
      VALUES (?, ?, ?, 'imported', ?, ?, ?, ?)`);
    insertReceipt.run("imported:thread", "thread", "thread:imported", at, 7, "accepted", null);
    insertReceipt.run("imported:project", "project", "project:imported", at, 9, "rejected", "");
    const insertPosition = database.prepare("INSERT INTO orchestration_v2_turn_item_positions VALUES (?, ?, ?)");
    insertPosition.run("thread:imported", "imported:item", 37);
    for (let index = 0; index < 70; index++) insertPosition.run("thread:many", `imported:${index}`, index + 1);
    for (const [id, ordinal] of [["last", 999_999], ["taken", 1_000_000], ["second-last", 2_999_999]]) {
      insertPosition.run("thread:edge", id, ordinal);
    }
    const insertRun = database.prepare(`INSERT INTO orchestration_v2_projection_runs
      (run_id, thread_id, ordinal, provider, status, requested_at, payload_json) VALUES (?, ?, ?, 'codex', 'running', ?, '{}')`);
    insertRun.run("run:seed", "thread:positions", 3, at);
    insertRun.run("run:foreign", "another-thread", 7, at);
  } finally { database.close(); }
  const sourceHash = hash(await FS.readFile(source));
  await FS.copyFile(source, sqlitePath); await FS.copyFile(source, upstreamPath);
  exportSqliteState({ source, destination: exported });
  nativeRequest(executable, fixture, "initialize", { operation: "import_sqlite_application", path: exported });
  const expected = await withServices("upstream", upstreamPath, exercise);
  const retained = await withServices("sqlite", sqlitePath, exercise);
  const actual = await withServices("redb", source, exercise);
  Assert.deepEqual(retained, expected, "The retained SQLite implementations must match immutable upstream");
  Assert.deepEqual(actual, expected, "Native receipts and positions must match immutable upstream SQLite");
  checks.push(
    "All application receipt methods, all four V2 receipt methods and both position methods agree with immutable upstream SQLite",
    "Imported rows preserve null versus empty errors, global project/thread command identity and stable item positions",
    "Position allocation preserves supplied/run/missing-run precedence and filters foreign-thread runs",
    "Normalization preserves the unchanged object and every field other than the allocated ordinal",
    "Nested rollback restores receipts and position indexes; root defects discard the complete staged mutation",
    "Million-item range overflow and ordinal uniqueness match SQLite, including recovery in the surrounding transaction",
  );
  await withServices("redb", source, ({ receipts, positions }) => Effect.gen(function* () {
    Assert.ok(Option.isSome(yield* receipts.getProjectByCommandId("command:shared")));
    Assert.equal(yield* positions.allocate(allocate("reopened")), 6);
  }));
  checks.push("Actual helper restart retains the shared receipt namespace and committed allocation indexes");
  await withServices("redb", source, ({ positions, transaction }) => transaction(Effect.gen(function* () {
    const base = { threadId: "thread:domain", turnItemId: "existing", runId: null };
    Assert.equal(yield* positions.allocate(base), 1);
    for (const runOrdinal of [-1, Number.MAX_SAFE_INTEGER + 1]) {
      Assert.equal(yield* positions.allocate({ ...base, runOrdinal }), 1);
      const error = yield* transaction(positions.allocate({ ...base, turnItemId: `invalid:${runOrdinal}`, runOrdinal })).pipe(Effect.flip);
      Assert.equal(error._tag, "TurnItemPositionStoreError");
      Assert.equal(error.cause._tag, "NativeDatabaseRejected");
      Assert.equal(error.cause.kind, "invalid_request");
    }
    Assert.equal(yield* positions.allocate({ ...base, turnItemId: "sibling" }), 2);
  })));
  checks.push("Native run ordinals require nonnegative safe integers after existing-item lookup; invalid values reject within their savepoint, while fractional inputs and REAL-cell coercion are outside the supported transport domain");
  let commits = 0;
  await withNative(executable, fixture, (client) => Effect.gen(function* () {
    const receipts = yield* NativeCommandReceipts.make;
    const positions = yield* NativeTurnItemPositionStore.make;
    const failed = yield* client.withTransaction(Effect.gen(function* () {
      yield* receipts.insertIfAbsent({ commandId: "unknown:receipt", aggregateKind: "thread", aggregateId: "thread:unknown", commandType: "test", acceptedAt: at, resultSequence: 60, status: "accepted", error: null });
      yield* positions.allocate({ threadId: "thread:unknown", turnItemId: "unknown:item", runId: null });
    })).pipe(Effect.flip);
    Assert.equal(failed._tag, "NativeDatabaseOutcomeUnknown");
  }), (database) => NativeDatabase.NativeDatabase.of({
    request: database.request,
    withSession: (use) => database.withSession((session) => use({
      discard: session.discard,
      request: (input, response) => session.request(input, response).pipe(Effect.flatMap((value) => {
        if (input.operation !== "application_commit") return Effect.succeed(value);
        commits++;
        return session.discard.pipe(Effect.andThen(Effect.fail(new NativeDatabase.NativeDatabaseOutcomeUnknown({ stage: "response" }))));
      })),
    })),
  }));
  Assert.equal(commits, 1, "Unknown commits must not be retried");
  await withServices("redb", source, ({ applicationReceipts, positions }) => Effect.gen(function* () {
    Assert.ok(Option.isSome(yield* applicationReceipts.getByCommandId({ commandId: "unknown:receipt" })));
    Assert.equal(yield* positions.allocate({ threadId: "thread:unknown", turnItemId: "unknown:item", runId: null }), 1);
    Assert.equal(yield* positions.allocate({ threadId: "thread:unknown", turnItemId: "unknown:next", runId: null }), 2);
  }));
  checks.push("Loss of the actual commit acknowledgement stays unknown; reopening finds both durable rows and no duplicate allocation");
  Assert.equal(hash(await FS.readFile(source)), sourceHash);
  await finishProof({ executable, fixture, proofPath, checks, sourceHash,
    oracles: [receiptOracle.source, wrapperOracle.source, positionOracle.source],
    sourcePaths: [
      "apps/server/src/persistence/NativeCommandReceipts.ts", "apps/server/src/persistence/redb_command_receipts.rs",
      "apps/server/src/orchestration-v2/CommandReceiptStore.ts", "apps/server/src/orchestration-v2/TurnItemPositionStore.ts",
      "apps/server/src/orchestration-v2/NativeTurnItemPositionStore.ts", "apps/server/src/orchestration-v2/redb_turn_item_positions.rs",
      "tools/check-redb-receipts-positions.mjs", "tools/redb-domain-fixture.mjs",
    ],
  });
} finally { await fixture.close(); }
