#!/usr/bin/env node
import * as Assert from "node:assert/strict";
import * as ChildProcess from "node:child_process";
import * as Crypto from "node:crypto";
import * as FS from "node:fs/promises";
import * as Path from "node:path";
import * as Sqlite from "node:sqlite";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Logger from "effect/Logger";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SecureRandom from "effect/crypto/SecureRandom";
import * as SqlClient from "effect/sql/SqlClient";

import * as EffectOutbox from "../apps/server/src/orchestration-v2/EffectOutbox.ts";
import * as EffectWorker from "../apps/server/src/orchestration-v2/EffectWorker.ts";
import * as NativeApplicationEffectOutbox from "../apps/server/src/orchestration-v2/NativeApplicationEffectOutbox.ts";
import * as NativeDatabase from "../apps/server/src/orchestration-v2/NativeDatabase.ts";
import * as RedbClient from "../apps/server/src/persistence/RedbClient.ts";
import { at, oracleSource, upstream } from "./check-outbox-runtime.mjs";
import { createFixture } from "./database-fixture.mjs";
import { exportSqliteState } from "./export-sqlite-state.mjs";

const [executable, proofPath] = process.argv.slice(2);
Assert.ok(executable && Path.isAbsolute(executable), "Pass an absolute database executable");
await FS.access(executable, FS.constants.X_OK);
const now = Date.parse("2026-11-10T12:00:00.000Z");
const day = 86_400_000;
const iso = (offset = 0) => new Date(now + offset).toISOString();
const fixed = (offset, effect) => at(iso(offset), effect);
const hash = (bytes) => Crypto.createHash("sha256").update(bytes).digest("hex");
const normalized = (value) => JSON.parse(JSON.stringify(value));
const checks = [];
const table = "orchestration_v2_effect_outbox";
const textCell = (value) => ({ type: "text", value });
const cleanup = { type: "terminal.cleanup" };
const pending = (id, threadId = id, request = cleanup, availableAt) => ({
  id, commandId: "command:flow", threadId, request,
  ...(availableAt === undefined ? {} : { availableAt: DateTime.makeUnsafe(availableAt) }),
});
const requests = [
  { type: "provider-runtime.continue", sourceRunId: "run:source" },
  { type: "provider-session.detach", providerSessionId: "session", detail: "detach 🦊", revokeMcpCredential: true },
  { type: "provider-turn.start", runId: "run" },
  { type: "provider-turn.interrupt", providerSessionId: "session", providerThreadId: "provider-thread", providerTurnId: "turn", subagent: { id: "node", nativeTaskId: "task" } },
  { type: "provider-turn.steer", providerSessionId: "session", providerThreadId: "provider-thread", providerTurnId: "turn", messageId: "message" },
  { type: "provider-turn.restart", providerSessionId: "session", providerThreadId: "provider-thread", providerTurnId: "turn", interruptedAttemptId: "attempt", runId: "run", sessionTransition: { type: "replace", replacementProviderSessionId: "replacement" } },
  { type: "runtime-request.respond", providerSessionId: "session", requestId: "request", feedback: "review 🦊", answers: { choice: ["one", "two"], optional: null, accepted: true } },
  { type: "provider-thread.rollback", providerThreadId: "provider-thread", checkpointId: "checkpoint", scopeId: "scope", restoreFiles: false },
  { type: "checkpoint.capture", runId: "run", scopeId: "scope" },
  { type: "scheduled-tasks.cleanup" }, cleanup, { type: "preview.cleanup" },
  { type: "attachment.cleanup", attachmentIds: ["attachment", "🦊"] },
  { type: "thread-title.generate", kind: { type: "regenerate" } },
  { type: "delegated-tasks.stop", reason: "parent stopped 🦊" },
];
Assert.equal(requests.length, 15);

function createSource(filename, seed = true) {
  const database = new Sqlite.DatabaseSync(filename);
  try {
    database.exec(`CREATE TABLE ${table} (
      effect_id TEXT PRIMARY KEY, command_id TEXT NOT NULL, thread_id TEXT NOT NULL,
      effect_type TEXT NOT NULL, payload_json TEXT NOT NULL,
      status TEXT NOT NULL CHECK(status IN ('pending','running','succeeded','failed','cancelled')),
      attempt_count INTEGER NOT NULL DEFAULT 0, available_at TEXT NOT NULL,
      lease_owner TEXT, lease_expires_at TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
      completed_at TEXT, last_error TEXT
    );
    CREATE INDEX outbox_claim ON ${table}(status, available_at, lease_expires_at, created_at);
    CREATE INDEX outbox_command ON ${table}(command_id, effect_id);
    CREATE INDEX outbox_thread ON ${table}(thread_id, status, effect_type);
    CREATE TABLE other (id TEXT PRIMARY KEY, value TEXT NOT NULL);`);
    if (!seed) return;
    const insert = database.prepare(`INSERT INTO ${table} VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`);
    const row = (id, command, thread, request, status, available = iso(day)) => {
      const settled = !["pending", "running"].includes(status);
      insert.run(id, command, thread, request.type,
        JSON.stringify({ ...request, retained_extra: { text: "🦊", number: 1 } }, null, 1), status,
        status === "pending" ? 0 : 2, available, status === "running" ? "seed-owner" : null,
        "opaque historical lease value", iso(), iso(), settled ? iso(-10 * day) : null,
        status === "failed" ? "retained failure" : null);
    };
    database.exec("BEGIN");
    row("z-imported", "command:imported", "thread:imported", cleanup, "pending", iso(100));
    row("a-imported", "command:imported", "thread:imported", cleanup, "pending", iso());
    for (const [index, request] of requests.entries()) {
      for (const status of ["pending", "running", "succeeded", "failed", "cancelled"]) {
        row(`matrix:${index}:${status}`, "command:matrix", `thread:${index}:${status}`, request, status);
      }
    }
    for (let index = 0; index < 501; index++) row(`prune:${index}`, "command:prune", `prune:${index}`, cleanup, "succeeded");
    database.exec("COMMIT");
  } finally { database.close(); }
}

function cli(fixture, mode, input) {
  const executed = ChildProcess.spawnSync(executable, fixture.cliArgs(mode), {
    input: JSON.stringify(input) + "\n", encoding: "utf8", env: {}, timeout: 45_000,
  });
  Assert.ifError(executed.error);
  Assert.equal(executed.status, 0, executed.stderr);
  const response = JSON.parse(executed.stdout);
  Assert.equal(response.ok, true, JSON.stringify(response));
  return response.result;
}

async function withBackend(kind, fixture, source, use, faults) {
  const work = Effect.gen(function* () {
    let outbox;
    let transaction;
    let snapshot;
    let putOther;
    let getOther;
    let client;
    if (kind === "sqlite") {
      const sql = yield* SqlClient.SqlClient;
      outbox = Context.get(yield* Layer.build(upstream.layer), upstream.EffectOutboxV2);
      transaction = (body) => sql.withTransaction(body);
      snapshot = sql`SELECT * FROM orchestration_v2_effect_outbox ORDER BY effect_id`.pipe(Effect.map(normalized));
      putOther = (id, value) => sql`INSERT OR REPLACE INTO other VALUES (${id}, ${value})`.pipe(Effect.asVoid);
      getOther = (id) => sql`SELECT value FROM other WHERE id = ${id}`.pipe(Effect.map((rows) => rows[0]?.value ?? null));
    } else {
      const actual = yield* NativeDatabase.make({ executable, databasePath: fixture.databasePath });
      const transport = faults === undefined ? actual : NativeDatabase.NativeDatabase.of({
        request: actual.request,
        withSession: (use) => actual.withSession((session) => {
          let claimed = false;
          return use({
            discard: session.discard,
            request: Effect.fnUntraced(function* (input, schema) {
              const result = yield* session.request(input, schema);
              if (input.operation === faults.corruptRead) {
                faults.corruptRead = undefined;
                const corrupt = input.operation === "application_outbox_get"
                  ? { ...result, id: "different-effect" }
                  : result.map((row) => ({ ...row, command_id: "different-command" }));
                return yield* Schema.decodeUnknownEffect(schema)(corrupt).pipe(
                  Effect.mapError((cause) => new NativeDatabase.NativeDatabaseOutcomeUnknown({ stage: "response", cause })),
                );
              }
              if (input.operation === "application_outbox_claim_once" || input.operation === "application_outbox_claim") {
                claimed = true;
                faults.claims.push(structuredClone(input));
                if (input.operation === "application_outbox_claim" && faults.corruptScopedClaim > 0) {
                  faults.corruptScopedClaim -= 1;
                  // The actual staged mutation succeeded; corrupt only its
                  // acknowledgement and apply the real response decoder.
                  return yield* Schema.decodeUnknownEffect(schema)({ ...result, lease_owner: "different-owner" }).pipe(
                    Effect.mapError((cause) => new NativeDatabase.NativeDatabaseOutcomeUnknown({ stage: "response", cause })),
                  );
                }
                if (faults.dropStage > 0) {
                  faults.dropStage -= 1;
                  return yield* new NativeDatabase.NativeDatabaseOutcomeUnknown({ stage: "response" });
                }
              }
              if (input.operation === "application_commit" && claimed && faults.dropCommit > 0) {
                faults.dropCommit -= 1;
                faults.discardedCommits += 1;
                return yield* new NativeDatabase.NativeDatabaseOutcomeUnknown({ stage: "response" });
              }
              return result;
            }),
          });
        }),
      });
      client = yield* RedbClient.make.pipe(Effect.provideService(NativeDatabase.NativeDatabase, transport));
      if (faults?.observeStandaloneClaim !== undefined) {
        const actualClient = client;
        client = RedbClient.RedbClient.of({
          ...actualClient,
          request: Effect.fnUntraced(function* (operation, input, response) {
            if (operation === "application_outbox_claim_once" && !(yield* actualClient.isInTransaction)) {
              yield* Deferred.succeed(faults.observeStandaloneClaim, undefined);
            }
            return yield* actualClient.request(operation, input, response);
          }),
        });
      }
      const context = yield* Layer.build(NativeApplicationEffectOutbox.layer).pipe(
        Effect.provideService(RedbClient.RedbClient, client),
      );
      outbox = Context.get(context, EffectOutbox.EffectOutboxV2);
      transaction = (body) => client.withTransaction(body);
      snapshot = Effect.gen(function* () {
        const rows = [];
        let after = null;
        do {
          const page = yield* client.scan(table, { after, limit: 4096 });
          for (const { row } of page.rows) rows.push(Object.fromEntries(Object.entries(row).map(([key, cell]) =>
            [key, cell.type === "null" ? null : cell.type === "integer" ? Number(cell.value) : cell.value])));
          after = page.next;
        } while (after !== null);
        return rows.sort((a, b) => a.effect_id < b.effect_id ? -1 : a.effect_id > b.effect_id ? 1 : 0);
      });
      putOther = (id, value) => client.put("other", [textCell(id)], { id: textCell(id), value: textCell(value) });
      getOther = (id) => client.get("other", [textCell(id)]).pipe(Effect.map((row) => row?.value.value ?? null));
    }
    return yield* use({ outbox, transaction, snapshot, putOther, getOther, client });
  }).pipe(Effect.scoped, Effect.timeout("30 seconds"), Effect.provide(NodeServices.layer), Effect.provide(SecureRandom.layerWebCrypto), Effect.provide(Logger.layer([])));
  return await Effect.runPromise(kind === "sqlite"
    ? work.pipe(Effect.provide(NodeSqliteClient.layer({ filename: source }))) : work);
}

function requireSome(value) { Assert.ok(Option.isSome(value)); return value.value; }

const exercise = ({ outbox, transaction, snapshot, putOther, getOther }) => Effect.gen(function* () {
  const observed = {};
  const sourceRows = yield* snapshot;
  observed.importedRaw = sourceRows.map(({ effect_id, payload_json, lease_expires_at }) => ({ effect_id, payload_json, lease_expires_at }));
  observed.matrix = yield* outbox.listByCommandId("command:matrix");
  Assert.equal(observed.matrix.length, 75);
  Assert.ok(Option.isNone(yield* outbox.get("missing")));
  yield* outbox.awaitSettled("missing");
  Assert.ok(Option.isNone(yield* fixed(0, outbox.claimNext({ workerId: "worker" }))));
  Assert.equal(DateTime.formatIso(requireSome(yield* outbox.nextClaimableAt)), iso(100));
  observed.importedFirst = requireSome(yield* fixed(100, outbox.claimNext({ workerId: "worker" })));
  Assert.equal(observed.importedFirst.id, "z-imported");
  Assert.equal(yield* fixed(101, outbox.succeed({ effectId: "z-imported", workerId: "worker" })), true);
  observed.importedSecond = requireSome(yield* fixed(102, outbox.claimNext({ workerId: "worker" })));
  Assert.equal(observed.importedSecond.id, "a-imported");
  Assert.equal(yield* fixed(103, outbox.fail({ effectId: "a-imported", workerId: "worker", error: "preserve failed source" })), true);
  yield* outbox.awaitAvailable;
  yield* outbox.awaitAvailable;

  const rollback = yield* fixed(0, transaction(Effect.gen(function* () {
    yield* putOther("rollback", "must disappear");
    yield* outbox.enqueue([pending("rollback")]);
    return yield* Effect.fail("rollback fixture");
  }))).pipe(Effect.exit);
  Assert.ok(Exit.isFailure(rollback));
  Assert.equal(yield* getOther("rollback"), null);
  Assert.ok(Option.isNone(yield* outbox.get("rollback")));
  const waiter = yield* outbox.awaitAvailable.pipe(Effect.forkChild);
  yield* Effect.yieldNow;
  yield* fixed(0, transaction(Effect.gen(function* () {
    yield* putOther("committed", "with outbox");
    yield* outbox.enqueue([
      pending("z-first", "thread:flow", cleanup, iso(100)),
      pending("a-second", "thread:flow"),
      pending("title", "thread:flow", { type: "thread-title.generate", kind: { type: "regenerate" } }),
    ]);
    yield* transaction(outbox.enqueue([pending("savepoint-discarded")]).pipe(Effect.andThen(Effect.fail("inner"))))
      .pipe(Effect.catch(() => Effect.void));
    Assert.ok(Option.isNone(yield* outbox.get("savepoint-discarded")));
  })));
  Assert.equal(yield* getOther("committed"), "with outbox");
  Assert.equal(waiter.pollUnsafe(), undefined, "enqueue must not notify workers before EventSink asks");
  yield* outbox.notifyAvailable();
  yield* Fiber.join(waiter);
  observed.title = requireSome(yield* fixed(0, outbox.claimNext({ workerId: "worker" })));
  Assert.equal(observed.title.id, "title");
  Assert.equal(yield* fixed(0, outbox.succeed({ effectId: "title", workerId: "wrong" })), false);
  Assert.equal(yield* fixed(0, outbox.retry({ effectId: "title", workerId: "worker", error: "retry", delayMs: -1 })), true);
  observed.titleRetry = requireSome(yield* fixed(0, outbox.claimNext({ workerId: "worker" })));
  Assert.equal(observed.titleRetry.attemptCount, 2);
  Assert.equal(yield* fixed(0, outbox.succeed({ effectId: "title", workerId: "worker" })), true);
  Assert.ok(Option.isNone(yield* fixed(0, outbox.claimNext({ workerId: "worker" }))));
  observed.first = requireSome(yield* fixed(100, outbox.claimNext({ workerId: "worker" })));
  Assert.equal(observed.first.id, "z-first");
  Assert.ok(Option.isNone(yield* fixed(100, outbox.claimNext({ workerId: "worker" }))));
  const cancelled = yield* outbox.awaitCancellation("z-first").pipe(Effect.forkChild);
  const settled = yield* outbox.awaitSettled("z-first").pipe(Effect.forkChild);
  yield* Effect.yieldNow;
  const cancel = outbox.cancelUnsettled({ threadId: "thread:flow", effectTypes: ["terminal.cleanup"], reason: "cancel fixture" });
  yield* fixed(100, transaction(cancel.pipe(Effect.andThen(Effect.fail("cancel rollback")))))
    .pipe(Effect.catch(() => Effect.void));
  Assert.equal(requireSome(yield* outbox.get("z-first")).status, "running");
  Assert.equal(cancelled.pollUnsafe(), undefined);
  observed.cancelled = yield* fixed(100, transaction(cancel));
  Assert.deepEqual(observed.cancelled, ["z-first", "a-second"]);
  Assert.equal(cancelled.pollUnsafe(), undefined, "cancellation is signaled only after commit");
  yield* outbox.signalCancellations(observed.cancelled);
  yield* Fiber.join(cancelled);
  yield* Fiber.join(settled);
  yield* outbox.clearCancellation("z-first");
  const nextCancellation = yield* outbox.awaitCancellation("z-first").pipe(Effect.forkChild);
  yield* Effect.yieldNow;
  Assert.equal(nextCancellation.pollUnsafe(), undefined);
  yield* outbox.signalCancellations(["z-first"]);
  yield* Fiber.join(nextCancellation);
  yield* fixed(0, outbox.enqueue([pending("z-first", "different-thread")]));
  Assert.equal(requireSome(yield* outbox.get("z-first")).threadId, "thread:flow");

  observed.reconciled = yield* fixed(2 * day, outbox.reconcileAfterProcessLoss);
  Assert.deepEqual(observed.reconciled, { requeued: 10, cancelled: 10 });
  const afterReconcile = yield* snapshot;
  for (const original of sourceRows) {
    const actual = afterReconcile.find((row) => row.effect_id === original.effect_id);
    Assert.equal(actual.payload_json, original.payload_json);
    Assert.equal(actual.lease_expires_at, original.lease_expires_at);
  }
  observed.afterReconcile = afterReconcile.map((row) => ({ ...row, payload_json: JSON.parse(row.payload_json) }));
  observed.pruned = yield* fixed(10 * day, outbox.pruneSettled);
  Assert.ok(observed.pruned > 500, "prune must finish multiple batches");
  Assert.equal(requireSome(yield* outbox.get("a-imported")).status, "failed");
  observed.finalRows = yield* snapshot;
  return normalized(observed);
});

async function emptyFixture(run) {
  const fixture = await createFixture();
  try {
    const source = Path.join(fixture.root, "source.sqlite");
    const exported = Path.join(fixture.root, "source.ndjson");
    createSource(source, false);
    exportSqliteState({ source, destination: exported });
    cli(fixture, "initialize", { operation: "import_sqlite_application", path: exported });
    return await run(fixture, source);
  } finally { await fixture.close(); }
}

const fixture = await createFixture();
try {
  const source = Path.join(fixture.root, "source.sqlite");
  const oracle = Path.join(fixture.root, "oracle.sqlite");
  const exported = Path.join(fixture.root, "source.ndjson");
  createSource(source);
  const sourceHash = hash(await FS.readFile(source));
  await FS.copyFile(source, oracle);
  exportSqliteState({ source, destination: exported });
  cli(fixture, "initialize", { operation: "import_sqlite_application", path: exported });
  const expected = await withBackend("sqlite", fixture, oracle, exercise);
  const actual = await withBackend("redb", fixture, source, exercise);
  Assert.deepEqual(actual, expected, "The complete application outbox must match immutable upstream SQLite");
  checks.push(
    "All 17 service members execute against immutable upstream SQLite and the real application-row outbox",
    "All 15 effect types and five lifecycle statuses retain decoded requests and raw imported payload/lease cells",
    "Imported FIFO order is independent of IDs; title lanes, retry backoff, owner checks and process-loss policies agree",
    "Enqueue and cancellation share other application writes, savepoints and rollback; notifications remain explicit after commit",
    "Pruning spans more than 500 rows and retains failed effects",
  );
  const reopened = await withBackend("redb", fixture, source, ({ outbox, snapshot }) => Effect.gen(function* () {
    Assert.equal(requireSome(yield* outbox.get("a-imported")).status, "failed");
    return yield* snapshot;
  }));
  Assert.deepEqual(reopened, expected.finalRows);
  Assert.equal(hash(await FS.readFile(source)), sourceHash);
  checks.push("Reopening preserves committed application rows without altering the SQLite source");

  const faultProof = await emptyFixture((target, source) => {
    const faults = { dropCommit: 1, dropStage: 0, discardedCommits: 0, claims: [] };
    return withBackend("redb", target, source, ({ outbox, transaction, putOther, getOther }) => Effect.gen(function* () {
      yield* fixed(0, outbox.enqueue([pending("uncertain-commit")]));
      const recovered = requireSome(yield* fixed(0, outbox.claimNext({ workerId: "worker" })));
      Assert.equal(recovered.attemptCount, 1);
      Assert.equal(faults.discardedCommits, 1);
      Assert.equal(faults.claims.length, 2);
      Assert.equal(faults.claims[0].stream_id, faults.claims[1].stream_id);
      Assert.equal(faults.claims[0].sequence, faults.claims[1].sequence);
      Assert.notEqual(faults.claims[0].transaction_id, faults.claims[1].transaction_id);
      yield* fixed(0, outbox.succeed({ effectId: recovered.id, workerId: "worker" }));
      yield* fixed(0, outbox.enqueue([pending("uncertain-shared")]));
      faults.dropStage = 1;
      const before = faults.claims.length;
      const failed = yield* fixed(0, transaction(Effect.gen(function* () {
        yield* putOther("uncertain-owner", "must roll back");
        yield* outbox.claimNext({ workerId: "shared-worker" });
      }))).pipe(Effect.exit);
      Assert.ok(Exit.isFailure(failed));
      Assert.equal(faults.claims.length, before + 1, "A shared uncertain scope must not retry its claim");
      Assert.equal(yield* getOther("uncertain-owner"), null);
      Assert.equal(requireSome(yield* outbox.get("uncertain-shared")).attemptCount, 0);
      const after = requireSome(yield* fixed(0, outbox.claimNext({ workerId: "shared-worker" })));
      Assert.equal(after.id, "uncertain-shared");
      Assert.equal(after.attemptCount, 1);
      yield* fixed(0, outbox.succeed({ effectId: after.id, workerId: "shared-worker" }));
      yield* fixed(0, outbox.enqueue([pending("successful-shared")]));
      const rolledBack = yield* fixed(0, transaction(outbox.claimNext({ workerId: "rollback-worker" }).pipe(
        Effect.tap((claimed) => Effect.sync(() => Assert.equal(requireSome(claimed).id, "successful-shared"))),
        Effect.andThen(Effect.fail("owner rollback after successful claim")),
      ))).pipe(Effect.exit);
      Assert.ok(Exit.isFailure(rolledBack));
      Assert.equal(requireSome(yield* outbox.get("successful-shared")).attemptCount, 0);
      const reclaimed = requireSome(yield* fixed(0, outbox.claimNext({ workerId: "rollback-worker" })));
      Assert.equal(reclaimed.id, "successful-shared");
      Assert.equal(reclaimed.attemptCount, 1);
      yield* fixed(0, outbox.succeed({ effectId: reclaimed.id, workerId: "rollback-worker" }));
      yield* fixed(0, outbox.enqueue([pending("successful-savepoint")]));
      yield* fixed(0, transaction(Effect.gen(function* () {
        yield* transaction(outbox.claimNext({ workerId: "savepoint-worker" }).pipe(
          Effect.andThen(Effect.fail("savepoint rollback after successful claim")),
        )).pipe(Effect.catch(() => Effect.void));
        Assert.equal(requireSome(yield* outbox.get("successful-savepoint")).attemptCount, 0);
      })));
      const savepointReclaimed = requireSome(yield* fixed(0, outbox.claimNext({ workerId: "savepoint-worker" })));
      Assert.equal(savepointReclaimed.id, "successful-savepoint");
      Assert.equal(savepointReclaimed.attemptCount, 1);
      yield* fixed(0, outbox.succeed({ effectId: savepointReclaimed.id, workerId: "savepoint-worker" }));
      yield* fixed(0, outbox.enqueue([pending("corrupt-shared-ack")]));
      faults.corruptScopedClaim = 1;
      const corrupted = yield* fixed(0, transaction(Effect.gen(function* () {
        yield* putOther("corrupt-ack-owner", "must roll back");
        const caught = yield* outbox.claimNext({ workerId: "corrupt-worker" }).pipe(Effect.exit);
        Assert.ok(Exit.isFailure(caught));
      }))).pipe(Effect.exit);
      Assert.ok(Exit.isFailure(corrupted), "Catching a bad claim acknowledgement cannot commit its shared owner");
      Assert.equal(yield* getOther("corrupt-ack-owner"), null);
      Assert.equal(requireSome(yield* outbox.get("corrupt-shared-ack")).attemptCount, 0);
      for (const [operation, read] of [
        ["application_outbox_get", outbox.get("corrupt-shared-ack")],
        ["application_outbox_list", outbox.listByCommandId("command:flow")],
      ]) {
        const badRead = yield* transaction(Effect.gen(function* () {
          yield* putOther(operation, "must roll back");
          faults.corruptRead = operation;
          const caught = yield* read.pipe(Effect.exit);
          Assert.ok(Exit.isFailure(caught));
        })).pipe(Effect.exit);
        Assert.ok(Exit.isFailure(badRead), "Catching an identity-mismatched read cannot commit its shared owner");
        Assert.equal(yield* getOther(operation), null);
      }
      return { discardedCommits: faults.discardedCommits, claimFrames: faults.claims.length };
    }), faults);
  });
  checks.push("Lost standalone commit acknowledgements replay one durable claim; shared uncertainty and successful outer/savepoint rollbacks never advance that stream");
  checks.push("Invalid scoped claim, get and list acknowledgements poison and roll back the shared owner even when their service errors are caught");

  await emptyFixture((target, source) => withBackend("redb", target, source,
    ({ outbox, transaction }) => Effect.gen(function* () {
      yield* fixed(0, outbox.enqueue([pending("settle-publication")]));
      Assert.equal(requireSome(yield* fixed(0, outbox.claimNext({ workerId: "publication-worker" }))).id, "settle-publication");
      const settled = yield* outbox.awaitSettled("settle-publication").pipe(Effect.forkChild);
      const available = yield* outbox.awaitAvailable.pipe(Effect.forkChild);
      yield* Effect.yieldNow;
      for (const operation of ["retry", "fail", "succeed"]) {
        const failed = yield* fixed(0, transaction(Effect.gen(function* () {
          Assert.equal(yield* outbox[operation]({
            effectId: "settle-publication", workerId: "publication-worker", error: "rollback", delayMs: 0,
          }), true);
          // This reads the owner's staged terminal row but must not release a
          // waiter belonging to another fiber before the owner commits.
          if (operation !== "retry") yield* outbox.awaitSettled("settle-publication");
          yield* Effect.yieldNow;
          Assert.equal(settled.pollUnsafe(), undefined);
          Assert.equal(available.pollUnsafe(), undefined);
          return yield* Effect.fail("discard settlement publication");
        }))).pipe(Effect.exit);
        Assert.ok(Exit.isFailure(failed));
        Assert.equal(requireSome(yield* outbox.get("settle-publication")).status, "running");
        Assert.equal(settled.pollUnsafe(), undefined);
        Assert.equal(available.pollUnsafe(), undefined);
      }
      yield* fixed(0, transaction(Effect.gen(function* () {
        yield* transaction(outbox.fail({ effectId: "settle-publication", workerId: "publication-worker", error: "discard child" })
          .pipe(Effect.andThen(Effect.fail("discard child publication"))))
          .pipe(Effect.catch(() => Effect.void));
        yield* outbox.notifyAvailable();
        Assert.equal(available.pollUnsafe(), undefined);
      })));
      yield* Fiber.join(available);
      Assert.equal(settled.pollUnsafe(), undefined);
      Assert.equal(requireSome(yield* outbox.get("settle-publication")).status, "running");
      const committedAvailable = yield* outbox.awaitAvailable.pipe(Effect.forkChild);
      yield* fixed(0, transaction(Effect.gen(function* () {
        const input = { effectId: "settle-publication", workerId: "publication-worker" };
        Assert.equal(yield* outbox.succeed(input), true);
        input.effectId = "caller-mutated-after-settlement";
        Assert.equal(settled.pollUnsafe(), undefined);
        Assert.equal(committedAvailable.pollUnsafe(), undefined);
      })));
      yield* Fiber.join(settled);
      yield* Fiber.join(committedAvailable);
      Assert.equal(requireSome(yield* outbox.get("settle-publication")).status, "succeeded");

      yield* fixed(0, outbox.enqueue([pending("cancel-publication")]));
      const cancelled = yield* outbox.awaitCancellation("cancel-publication").pipe(Effect.forkChild);
      const cancelledSettled = yield* outbox.awaitSettled("cancel-publication").pipe(Effect.forkChild);
      const cancelledAvailable = yield* outbox.awaitAvailable.pipe(Effect.forkChild);
      yield* Effect.yieldNow;
      const cancelAndSignal = Effect.gen(function* () {
        const ids = yield* outbox.cancelUnsettled({
          threadId: "cancel-publication", effectTypes: ["terminal.cleanup"], reason: "cancel publication",
        });
        yield* outbox.signalCancellations(ids);
        ids.splice(0, ids.length, "caller-mutated-after-registration");
        Assert.equal(cancelled.pollUnsafe(), undefined);
        Assert.equal(cancelledSettled.pollUnsafe(), undefined);
        Assert.equal(cancelledAvailable.pollUnsafe(), undefined);
      });
      const cancelledRollback = yield* fixed(0, transaction(cancelAndSignal.pipe(Effect.andThen(Effect.fail("cancel rollback")))))
        .pipe(Effect.exit);
      Assert.ok(Exit.isFailure(cancelledRollback));
      Assert.equal(requireSome(yield* outbox.get("cancel-publication")).status, "pending");
      Assert.equal(cancelled.pollUnsafe(), undefined);
      yield* fixed(0, transaction(cancelAndSignal));
      yield* Fiber.join(cancelled);
      yield* Fiber.join(cancelledSettled);
      yield* Fiber.join(cancelledAvailable);
      Assert.equal(requireSome(yield* outbox.get("cancel-publication")).status, "cancelled");

      yield* fixed(0, outbox.enqueue([pending("reconcile-publication")]));
      Assert.equal(requireSome(yield* fixed(0, outbox.claimNext({ workerId: "reconcile-worker" }))).id, "reconcile-publication");
      const reconciledAvailable = yield* outbox.awaitAvailable.pipe(Effect.forkChild);
      const reconcileRollback = yield* fixed(day, transaction(Effect.gen(function* () {
        Assert.deepEqual(yield* outbox.reconcileAfterProcessLoss, { requeued: 1, cancelled: 0 });
        Assert.equal(reconciledAvailable.pollUnsafe(), undefined);
        return yield* Effect.fail("discard reconcile publication");
      }))).pipe(Effect.exit);
      Assert.ok(Exit.isFailure(reconcileRollback));
      Assert.equal(requireSome(yield* outbox.get("reconcile-publication")).status, "running");
      Assert.equal(reconciledAvailable.pollUnsafe(), undefined);
      yield* fixed(day, transaction(transaction(outbox.reconcileAfterProcessLoss)));
      yield* Fiber.join(reconciledAvailable);
      Assert.equal(requireSome(yield* outbox.get("reconcile-publication")).status, "pending");
    })));
  checks.push("Settlement, retry, cancellation, explicit notifications and process-loss reconciliation publish only after root commit; rollback and savepoints retain parked waiters");

  await emptyFixture((target, source) => {
    const faults = { dropCommit: 0, dropStage: 0, discardedCommits: 0, claims: [],
      observeStandaloneClaim: Deferred.makeUnsafe() };
    return withBackend("redb", target, source, ({ outbox, transaction }) => Effect.gen(function* () {
      yield* fixed(0, outbox.enqueue([pending("claim-owner"), pending("claim-waiter")]));
      const ownerReady = yield* Deferred.make();
      const claimInOwner = yield* Deferred.make();
      const owner = yield* fixed(0, transaction(Effect.gen(function* () {
        yield* Deferred.succeed(ownerReady, undefined);
        yield* Deferred.await(claimInOwner);
        return requireSome(yield* outbox.claimNext({ workerId: "transaction-owner" }));
      }))).pipe(Effect.forkChild);
      yield* Deferred.await(ownerReady);
      const standalone = yield* fixed(0, outbox.claimNext({ workerId: "standalone-waiter" })).pipe(Effect.forkChild);
      // The standalone worker owns the claim-stream semaphore before its actual
      // RedbClient request, which is waiting for the owner's reserved writer.
      yield* Deferred.await(faults.observeStandaloneClaim);
      yield* Deferred.succeed(claimInOwner, undefined);
      const first = yield* Fiber.join(owner);
      const second = requireSome(yield* Fiber.join(standalone));
      Assert.equal(first.id, "claim-owner");
      Assert.equal(second.id, "claim-waiter");
      Assert.equal(first.attemptCount, 1);
      Assert.equal(second.attemptCount, 1);
    }).pipe(Effect.timeout("10 seconds")), faults);
  });
  checks.push("A shared owner can claim while a standalone worker waits for its writer, without reversing the writer and claim-stream semaphore order");

  await emptyFixture((target, source) => withBackend("redb", target, source, ({ outbox }) => Effect.gen(function* () {
    yield* outbox.enqueue([pending("worker-execution")]);
    const executed = yield* Deferred.make();
    const services = EffectWorker.layerWithOptions({ workerId: "application-worker" }).pipe(
      Layer.provideMerge(Layer.mergeAll(
        Layer.succeed(EffectOutbox.EffectOutboxV2, outbox),
        Layer.succeed(EffectWorker.OrchestrationEffectExecutorV2, {
          execute: (effect) => Effect.sync(() => Assert.equal(effect.id, "worker-execution"))
            .pipe(Effect.andThen(Deferred.succeed(executed, undefined))),
        }),
      )),
    );
    yield* Effect.gen(function* () {
      const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
      Assert.equal(yield* worker.runOnce, true);
      Assert.equal(yield* Deferred.isDone(executed), true);
      yield* outbox.awaitSettled("worker-execution");
      Assert.equal(requireSome(yield* outbox.get("worker-execution")).status, "succeeded");
      Assert.equal(yield* worker.runOnce, false);
    }).pipe(Effect.provide(services));
  })));
  checks.push("The actual OrchestrationEffectWorkerV2 executes and settles the application outbox with a controlled executor");
  const proof = { schema: 1, engine: fixture.engineLabel, runtime: process.version, checks,
    oracle: oracleSource, executable_sha256: hash(await FS.readFile(executable)),
    sqlite_source_sha256: sourceHash, faultProof };
  if (proofPath) await FS.writeFile(proofPath, JSON.stringify(proof, null, 2) + "\n");
  console.log(JSON.stringify(proof, null, 2));
} finally { await fixture.close(); }
