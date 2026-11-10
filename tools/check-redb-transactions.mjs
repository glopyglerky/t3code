#!/usr/bin/env node
import * as Assert from "node:assert/strict";
import * as ChildProcess from "node:child_process";
import * as Crypto from "node:crypto";
import * as FS from "node:fs/promises";
import * as Path from "node:path";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";

import * as NativeDatabase from "../apps/server/src/orchestration-v2/NativeDatabase.ts";
import * as RedbClient from "../apps/server/src/persistence/RedbClient.ts";
import { createFixture } from "./database-fixture.mjs";

const [executable, proofPath] = process.argv.slice(2);
Assert.ok(executable && Path.isAbsolute(executable), "Pass an absolute native database executable");
await FS.access(executable, FS.constants.X_OK);
const fixture = await createFixture();
const checks = [];
const requests = [];
const frames = [];
const fault = {
  dropCommit: false,
  mismatchBegin: false,
  mismatchSavepoint: false,
  discardBeforeCommit: false,
  dropOperation: undefined,
  malformedRow: false,
  lockTimeoutMs: undefined,
  beginObserved: undefined,
};
const schema = {
  version: 1,
  tables: ["accounts", "savepoint_accounts"].map((name) => ({
    name, columns: ["id", "payload"], primary_key: ["id"],
  })),
};
const row = (id, payload = {}) => ({ id, payload });
const checked = (description) => checks.push(description);

function failure(effect, tag, reason) {
  return effect.pipe(
    Effect.catch(Effect.succeed),
    Effect.tap((result) =>
      Effect.sync(() => {
        Assert.equal(result?._tag, tag);
        if (reason !== undefined) Assert.equal(result.reason, reason);
      }),
    ),
  );
}

function instrument(database) {
  return NativeDatabase.NativeDatabase.of({
    request: (input, response) => Effect.suspend(() => {
      requests.push(input.operation);
      frames.push(input);
      return database.request(input, response);
    }),
    withSession: (use) =>
      database.withSession((session) =>
        use({
          discard: session.discard,
          request: (input, response) =>
            Effect.gen(function* () {
              requests.push(input.operation);
              let wire = input;
              if (input.operation === "application_begin" && fault.lockTimeoutMs !== undefined) {
                wire = { ...input, lock_timeout_ms: fault.lockTimeoutMs };
                fault.lockTimeoutMs = undefined;
              }
              if (input.operation === "application_begin" && fault.beginObserved !== undefined) {
                const observed = fault.beginObserved;
                fault.beginObserved = undefined;
                yield* Deferred.succeed(observed, undefined);
              }
              if (input.operation === "application_put" && fault.malformedRow) {
                fault.malformedRow = false;
                wire = { ...input, unexpected_field: true };
              }
              frames.push(wire);
              if (input.operation === "application_commit" && fault.discardBeforeCommit) {
                fault.discardBeforeCommit = false;
                yield* session.discard;
              }
              const result = yield* session.request(wire, response);
              if (input.operation === "application_begin" && fault.mismatchBegin) {
                fault.mismatchBegin = false;
                return { transaction_id: "different-transaction" };
              }
              if (input.operation === "application_savepoint" && fault.mismatchSavepoint) {
                fault.mismatchSavepoint = false;
                return { savepoint_id: "different-savepoint" };
              }
              if (input.operation === fault.dropOperation) {
                fault.dropOperation = undefined;
                yield* session.discard;
                return yield* new NativeDatabase.NativeDatabaseOutcomeUnknown({ stage: "response" });
              }
              if (input.operation === "application_commit" && fault.dropCommit) {
                fault.dropCommit = false;
                // The actual native commit completed; discard only its acknowledgement.
                yield* session.discard;
                return yield* new NativeDatabase.NativeDatabaseOutcomeUnknown({ stage: "response" });
              }
              return result;
            }),
        }),
      ),
  });
}

// Run the same mutation history against the actual SQLite and redb adapters.
function savepointHistory(driver) {
  return Effect.gen(function* () {
    const failed = new Error("nested-history-failure");
    yield* driver.transaction(Effect.gen(function* () {
      yield* driver.put("before", { value: 1 });
      yield* driver.put("retained", null);
      const result = yield* driver.transaction(
        driver.put("released", "nested-success").pipe(Effect.as(42)),
      );
      Assert.equal(result, 42);
      const rejected = yield* driver.transaction(Effect.gen(function* () {
        yield* driver.put("before", { value: 2 });
        yield* driver.delete("retained");
        yield* driver.put("removed", "nested-failure");
        yield* driver.transaction(driver.put("released-grandchild", "still-in-parent"));
        return yield* Effect.fail(failed);
      })).pipe(Effect.catch(Effect.succeed));
      Assert.equal(rejected, failed);
      Assert.deepEqual(yield* driver.get("before"), row("before", { value: 1 }));
      Assert.deepEqual(yield* driver.get("retained"), row("retained", null));
      Assert.equal(yield* driver.get("released-grandchild"), null);
      const defect = yield* driver.transaction(
        driver.put("defective", "discarded").pipe(Effect.andThen(Effect.die("nested-history-defect"))),
      ).pipe(Effect.exit);
      Assert.ok(Exit.isFailure(defect));
      Assert.equal(yield* driver.get("defective"), null);
      const cancelled = yield* driver.transaction(
        driver.put("cancelled", "discarded").pipe(Effect.andThen(Effect.interrupt)),
      ).pipe(Effect.exit, Effect.uninterruptible);
      Assert.ok(Exit.isFailure(cancelled));
      Assert.equal(yield* driver.get("cancelled"), null);
      yield* driver.transaction(driver.put("sibling", [1, "after-failure"]));
    }));
    const outerFailed = yield* driver.transaction(Effect.gen(function* () {
      yield* driver.put("outer-rolled-back", "discarded");
      yield* driver.transaction(driver.put("released-then-outer-failed", "discarded"));
      return yield* Effect.fail(failed);
    })).pipe(Effect.catch(Effect.succeed));
    Assert.equal(outerFailed, failed);
    Assert.equal(yield* driver.get("released-then-outer-failed"), null);
    return yield* driver.list;
  });
}

function sqliteSavepointHistory() {
  return Effect.runPromise(Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE savepoint_accounts(id TEXT PRIMARY KEY, payload TEXT NOT NULL)`;
  return yield* savepointHistory({
    transaction: sql.withTransaction,
    put: (id, payload) => sql`INSERT INTO savepoint_accounts(id,payload) VALUES (${id},${JSON.stringify(payload)})
      ON CONFLICT(id) DO UPDATE SET payload=excluded.payload`,
    delete: (id) => sql`DELETE FROM savepoint_accounts WHERE id=${id}`,
    get: (id) => sql`SELECT id,payload FROM savepoint_accounts WHERE id=${id}`.pipe(
      Effect.map((rows) => rows.length === 0 ? null : row(rows[0].id, JSON.parse(rows[0].payload))),
    ),
    list: sql`SELECT id,payload FROM savepoint_accounts ORDER BY id`.pipe(
      Effect.map((rows) => rows.map((entry) => row(entry.id, JSON.parse(entry.payload)))),
    ),
  });
  }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" })), Effect.scoped));
}

async function run(body) {
  await Effect.runPromise(
    Effect.gen(function* () {
      const actual = yield* NativeDatabase.make({ executable, databasePath: fixture.databasePath });
      const database = instrument(actual);
      const client = yield* RedbClient.make.pipe(
        Effect.provideService(NativeDatabase.NativeDatabase, database),
      );
      yield* body(client, actual, database);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );
}

try {
  const sqliteHistory = await sqliteSavepointHistory();
  const initialized = ChildProcess.spawnSync(executable, fixture.cliArgs("initialize"), {
    env: {},
    input: '{"operation":"snapshot"}\n',
    encoding: "utf8",
    timeout: 15_000,
  });
  Assert.equal(initialized.status, 0, initialized.stderr);
  Assert.equal(JSON.parse(initialized.stdout).ok, true);
  await run((client, actual, database) =>
    Effect.gen(function* () {
      Assert.deepEqual(yield* client.initialize(schema), { initialized: true });
      yield* client.withTransaction(
        Effect.gen(function* () {
          yield* client.put("accounts", ["first"], row("first", { nullable: null, nested: [1, "x"] }));
          yield* client.put("accounts", ["second"], row("second", "value"));
          Assert.deepEqual(yield* client.get("accounts", ["first"]), row("first", { nullable: null, nested: [1, "x"] }));
        }),
      );
      Assert.deepEqual(yield* client.get("accounts", ["second"]), row("second", "value"));
      checked("Multiple rows commit atomically and transaction reads observe staged writes");

      const original = new Error("rollback-body-failure");
      const rejected = yield* client
        .withTransaction(
          client.put("accounts", ["rolled-back"], row("rolled-back")).pipe(Effect.andThen(Effect.fail(original))),
        )
        .pipe(Effect.catch(Effect.succeed));
      Assert.equal(rejected, original);
      Assert.equal(yield* client.get("accounts", ["rolled-back"]), null);
      const defect = yield* client.withTransaction(
        client.put("accounts", ["defect"], row("defect")).pipe(Effect.andThen(Effect.die("fixture-defect"))),
      ).pipe(Effect.exit);
      Assert.ok(Exit.isFailure(defect));
      Assert.equal(yield* client.get("accounts", ["defect"]), null);
      checked("Failed and defective bodies roll back their staged rows and preserve the original failure");

      const nativeHistory = yield* savepointHistory({
        transaction: client.withTransaction,
        put: (id, payload) => client.put("savepoint_accounts", [id], row(id, payload)),
        delete: (id) => client.delete("savepoint_accounts", [id]),
        get: (id) => client.get("savepoint_accounts", [id]),
        list: client.scan("savepoint_accounts", { limit: 100 }).pipe(
          Effect.map((page) => page.rows.map((entry) => entry.row).sort((a, b) => a.id.localeCompare(b.id))),
        ),
      });
      Assert.deepEqual(nativeHistory, sqliteHistory);
      Assert.deepEqual(nativeHistory.map((entry) => entry.id), ["before", "released", "retained", "sibling"]);
      checked("Real SQLite and redb agree on nested success, update/delete rollback, defects, recovered cancellation, siblings and outer rollback of released grandchildren");

      const beforeNested = requests.length;
      yield* client.withTransaction(client.withTransaction(client.put("accounts", ["nested-success"], row("nested-success"))));
      Assert.deepEqual(requests.slice(beforeNested), [
        "application_begin", "application_savepoint", "application_put", "application_release", "application_commit",
      ]);
      checked("Nested success releases its savepoint and only the outer scope commits");

      yield* client.withTransaction(Effect.gen(function* () {
        yield* client.put("accounts", ["outer-after-row-rejection"], row("outer-after-row-rejection"));
        yield* failure(client.withTransaction(Effect.gen(function* () {
          yield* client.put("accounts", ["rejected-scope"], row("rejected-scope"));
          yield* failure(client.put("accounts", ["wrong-key"], row("different-key")), "NativeDatabaseRejected");
          yield* failure(client.put("accounts", ["after-caught-row"], row("after-caught-row")), "NativeDatabaseRejected");
        })), "NativeDatabaseRejected");
        Assert.equal(yield* client.get("accounts", ["rejected-scope"]), null);
        yield* client.withTransaction(client.put("accounts", ["sibling-after-row-rejection"], row("sibling-after-row-rejection")));
      }));
      Assert.deepEqual(yield* client.get("accounts", ["outer-after-row-rejection"]), row("outer-after-row-rejection"));
      Assert.deepEqual(yield* client.get("accounts", ["sibling-after-row-rejection"]), row("sibling-after-row-rejection"));
      Assert.equal(yield* client.get("accounts", ["after-caught-row"]), null);
      checked("A caught native statement rejection forces its savepoint rollback while the parent and later sibling commit");

      yield* client.withTransaction(
        Effect.gen(function* () {
          const before = requests.length;
          const child = yield* failure(
            client.put("accounts", ["child"], row("child")),
            "RedbTransactionError",
            "different-fiber",
          ).pipe(Effect.forkChild);
          yield* Fiber.join(child);
          const nestedChild = yield* failure(
            client.withTransaction(client.put("accounts", ["nested-child"], row("nested-child"))),
            "RedbTransactionError",
            "different-fiber",
          ).pipe(Effect.forkChild);
          yield* Fiber.join(nestedChild);
          Assert.equal(requests.length, before);
          yield* client.put("accounts", ["parent"], row("parent"));
        }),
      );
      Assert.equal(yield* client.get("accounts", ["child"]), null);
      checked("Inherited transaction context cannot authorize writes or nested savepoints by a child fiber");

      const secondClient = yield* RedbClient.make.pipe(
        Effect.provideService(NativeDatabase.NativeDatabase, database),
      );
      yield* client.withTransaction(
        failure(secondClient.get("accounts", ["first"]), "RedbTransactionError", "different-client").pipe(
          Effect.andThen(failure(secondClient.withTransaction(Effect.void), "RedbTransactionError", "different-client")),
        ),
      );
      checked("A different client cannot reuse the current transaction or create a nested savepoint");

      const ready = yield* Deferred.make();
      const release = yield* Deferred.make();
      const start = requests.length;
      const writer = yield* client.withTransaction(
        client.put("accounts", ["serialized"], row("serialized")).pipe(
          Effect.andThen(Deferred.succeed(ready, undefined)),
          Effect.andThen(Deferred.await(release)),
        ),
      ).pipe(Effect.forkChild);
      yield* Deferred.await(ready);
      const other = yield* secondClient.put("accounts", ["serialized-other"], row("serialized-other")).pipe(Effect.forkChild);
      const plain = yield* database.request({ operation: "snapshot" }, Schema.Unknown).pipe(Effect.forkChild);
      yield* Effect.yieldNow;
      yield* Deferred.succeed(release, undefined);
      yield* Fiber.join(writer);
      yield* Fiber.join(other);
      yield* Fiber.join(plain);
      const transactionRequests = requests.slice(start).filter((operation) => operation !== "snapshot");
      Assert.deepEqual(transactionRequests, [
        "application_begin", "application_put", "application_commit",
        "application_begin", "application_put", "application_commit",
      ]);
      checked("Transactions on one connection stay contiguous under concurrent clients and standalone requests");

      const beforeLegacy = yield* database.request({ operation: "snapshot" }, Schema.Unknown);
      const legacyCommit = {
        operation: "commit",
        batch: {
          command_id: "command:legacy-writer-guard",
          request_digest: Crypto.createHash("sha256").update("legacy-writer-guard").digest("hex"),
          expected_revision: beforeLegacy.revision,
          events: [],
          projections: [{ operation: "put", key: "legacy-writer-guard", payload: { accepted: true } }],
          effects: [],
          result: { accepted: true },
        },
      };
      yield* database.withSession((session) => Effect.gen(function* () {
        const transaction_id = "wire:legacy-writer-guard";
        yield* session.request({ operation: "application_begin", transaction_id, mode: "write" }, Schema.Unknown);
        yield* session.request({
          operation: "application_put", transaction_id, table: "accounts",
          key: ["wire-guard-staged"], row: row("wire-guard-staged"),
        }, Schema.Unknown);
        const rejected = yield* failure(session.request(legacyCommit, Schema.Unknown), "NativeDatabaseRejected");
        Assert.equal(rejected.kind, "writer_busy");
        Assert.equal(yield* session.request({ operation: "receipt", command_id: legacyCommit.batch.command_id }, Schema.Unknown), null);
        Assert.deepEqual(yield* session.request({
          operation: "application_get", transaction_id, table: "accounts", key: ["wire-guard-staged"],
        }, Schema.Unknown), row("wire-guard-staged"));
        yield* session.request({ operation: "application_rollback", transaction_id }, Schema.Unknown);
        const accepted = yield* session.request(legacyCommit, Schema.Unknown);
        Assert.equal(accepted.replayed, false);
        Assert.equal(accepted.receipt.command_id, legacyCommit.batch.command_id);
        Assert.equal(accepted.receipt.revision, beforeLegacy.revision + 1);
        const after = yield* session.request({ operation: "snapshot" }, Schema.Unknown);
        Assert.deepEqual(after.projections.find((entry) => entry.key === "legacy-writer-guard"), {
          key: "legacy-writer-guard", payload: { accepted: true },
        });
      }));
      Assert.equal(yield* client.get("accounts", ["wire-guard-staged"]), null);
      checked("Direct-wire legacy commits reject with writer_busy while an application writer is live, preserve its scope and succeed unchanged after rollback");

      const independentDatabase = yield* NativeDatabase.make({ executable, databasePath: fixture.databasePath });
      const independentClient = yield* RedbClient.make.pipe(
        Effect.provideService(NativeDatabase.NativeDatabase, instrument(independentDatabase)),
      );
      yield* client.put("accounts", ["writer-reservation"], row("writer-reservation", "before"));
      const reserved = yield* Deferred.make();
      const releaseReservation = yield* Deferred.make();
      const reservation = yield* client.withTransaction(
        client.put("accounts", ["writer-reservation"], row("writer-reservation", "after")).pipe(
          Effect.andThen(Deferred.succeed(reserved, undefined)),
          Effect.andThen(Deferred.await(releaseReservation)),
        ),
      ).pipe(Effect.forkChild);
      yield* Deferred.await(reserved);
      yield* Effect.gen(function* () {
        Assert.deepEqual(yield* independentClient.get("accounts", ["writer-reservation"]), row("writer-reservation", "before"));
        Assert.equal(frames.findLast((frame) => frame.operation === "application_begin").mode, "read");
        let entered = 0;
        const busyStart = requests.length;
        fault.lockTimeoutMs = 30;
        const busy = yield* failure(independentClient.withTransaction(Effect.sync(() => { entered += 1; })), "NativeDatabaseRejected");
        Assert.equal(busy.kind, "writer_busy");
        Assert.equal(entered, 0);
        Assert.deepEqual(requests.slice(busyStart), ["application_begin"]);
        const observed = yield* Deferred.make();
        fault.beginObserved = observed;
        const waiting = yield* independentClient.withTransaction(Effect.sync(() => { entered += 1; })).pipe(Effect.forkChild);
        yield* Deferred.await(observed);
        yield* Effect.promise(() => new Promise((resolve) => setImmediate(resolve)));
        yield* Fiber.interrupt(waiting);
        Assert.equal(entered, 0);
      }).pipe(Effect.ensuring(Deferred.succeed(releaseReservation, undefined)));
      yield* Fiber.join(reservation);
      Assert.deepEqual(yield* independentClient.get("accounts", ["writer-reservation"]), row("writer-reservation", "after"));
      yield* independentClient.withTransaction(Effect.void);
      checked("Independent helpers reserve the writer before callbacks; busy acquisition never retries, cancellation never enters the body, and snapshot readers remain available");

      const interruptedReady = yield* Deferred.make();
      const interrupted = yield* client.withTransaction(
        client.put("accounts", ["interrupted"], row("interrupted")).pipe(
          Effect.andThen(Deferred.succeed(interruptedReady, undefined)),
          Effect.andThen(Effect.never),
        ),
      ).pipe(Effect.forkChild);
      yield* Deferred.await(interruptedReady);
      yield* Fiber.interrupt(interrupted);
      Assert.equal(yield* client.get("accounts", ["interrupted"]), null);
      checked("Interrupted bodies roll back and release the helper for subsequent transactions");

      const nestedInterruptedReady = yield* Deferred.make();
      const nestedInterruptedStart = requests.length;
      const nestedInterrupted = yield* client.withTransaction(
        client.put("accounts", ["interrupted-parent"], row("interrupted-parent")).pipe(
          Effect.andThen(client.withTransaction(
            client.put("accounts", ["interrupted-inner"], row("interrupted-inner")).pipe(
              Effect.andThen(Deferred.succeed(nestedInterruptedReady, undefined)),
              Effect.andThen(Effect.never),
            ),
          )),
        ),
      ).pipe(Effect.forkChild);
      yield* Deferred.await(nestedInterruptedReady);
      yield* Fiber.interrupt(nestedInterrupted);
      Assert.deepEqual(requests.slice(nestedInterruptedStart), [
        "application_begin", "application_put", "application_savepoint", "application_put",
        "application_rollback_to", "application_release", "application_rollback",
      ]);
      Assert.equal(yield* client.get("accounts", ["interrupted-parent"]), null);
      Assert.equal(yield* client.get("accounts", ["interrupted-inner"]), null);
      checked("External cancellation unwinds the inner savepoint before rolling back the outer transaction");

      const beforeMixed = requests.length;
      yield* Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        yield* failure(
          sql.withTransaction(client.put("accounts", ["mixed"], row("mixed"))),
          "RedbTransactionError",
          "mixed-storage-transaction",
        );
      }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" })));
      Assert.equal(requests.length, beforeMixed);
      yield* client.withTransaction(Effect.gen(function* () {
        const before = requests.length;
        yield* Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          yield* failure(sql.withTransaction(client.withTransaction(Effect.void)), "RedbTransactionError", "mixed-storage-transaction");
        }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" })));
        Assert.equal(requests.length, before);
        yield* client.put("accounts", ["after-mixed-rejection"], row("after-mixed-rejection"));
      }));
      checked("An actual SqlClient transaction is rejected before sending any native request");

      const invalid = yield* client.withTransaction(
        Effect.gen(function* () {
          yield* client.put("accounts", ["before-invalid"], row("before-invalid"));
          yield* failure(client.put("accounts", ["wrong-key"], row("different-key")), "NativeDatabaseRejected");
        }),
      ).pipe(Effect.catch(Effect.succeed));
      Assert.equal(invalid._tag, "NativeDatabaseRejected");
      Assert.equal(yield* client.get("accounts", ["before-invalid"]), null);
      checked("Catching a rejected row operation cannot commit earlier staged writes");

      for (const payload of [NaN, Infinity, undefined, { omitted: undefined }]) {
        yield* failure(
          client.put("accounts", ["non-json"], { id: "non-json", payload }),
          "NativeDatabaseInputError",
        );
      }
      yield* failure(client.get("accounts", [NaN]), "NativeDatabaseInputError");
      Assert.equal(yield* client.get("accounts", ["non-json"]), null);
      checked("Non-finite numbers and undefined keys or values fail without JSON coercion");

      let rootContext;
      yield* client.withTransaction(Effect.gen(function* () {
        rootContext = yield* Effect.context();
        yield* client.withTransaction(Effect.gen(function* () {
          const before = requests.length;
          yield* failure(
            client.put("accounts", ["parent-context-in-child"], row("parent-context-in-child")).pipe(Effect.provide(rootContext)),
            "RedbTransactionError", "different-scope",
          );
          Assert.equal(requests.length, before);
          yield* client.put("accounts", ["active-scope"], row("active-scope"));
        }));
        const nestedContext = yield* client.withTransaction(Effect.context());
        const before = requests.length;
        yield* failure(
          client.get("accounts", ["first"]).pipe(Effect.provide(nestedContext)),
          "RedbTransactionError", "closed-transaction",
        );
        yield* failure(
          client.withTransaction(Effect.void).pipe(Effect.provide(nestedContext)),
          "RedbTransactionError", "closed-transaction",
        );
        yield* failure(client.initialize(schema), "RedbTransactionError", "initialization-in-transaction");
        yield* failure(client.prepareEvents, "RedbTransactionError", "initialization-in-transaction");
        Assert.equal(requests.length, before);
        yield* client.put("accounts", ["after-closed-scope"], row("after-closed-scope"));
      }));
      const afterRoot = requests.length;
      yield* failure(client.get("accounts", ["first"]).pipe(Effect.provide(rootContext)), "RedbTransactionError", "closed-transaction");
      Assert.equal(requests.length, afterRoot);
      checked("Captured contexts cannot bypass the current savepoint or outlive their scope; initialization and index preparation reject inside transactions");

      yield* client.withTransaction(Effect.gen(function* () {
        for (const [index, operation, payload] of [
          [0, "application_commit", {}],
          [1, "application_events_read", { transaction_id: "forged" }],
          [2, "application_events_read", { operation: "application_commit" }],
        ]) {
          const before = requests.length;
          yield* failure(client.withTransaction(Effect.gen(function* () {
            yield* client.put("accounts", [`forged-${index}`], row(`forged-${index}`));
            yield* failure(client.request(operation, payload, Schema.Unknown), "NativeDatabaseInputError");
          })), "NativeDatabaseInputError");
          Assert.deepEqual(requests.slice(before), [
            "application_savepoint", "application_put", "application_rollback_to", "application_release",
          ]);
          Assert.equal(yield* client.get("accounts", [`forged-${index}`]), null);
        }
        yield* client.put("accounts", ["after-forged-request"], row("after-forged-request"));
      }));
      Assert.deepEqual(yield* client.get("accounts", ["after-forged-request"]), row("after-forged-request"));
      checked("The domain request hook rejects lifecycle operations and forged transaction IDs through the same savepoint failure handling");

      let escaped;
      yield* actual.withSession((session) =>
        Effect.sync(() => { escaped = session; }).pipe(
          Effect.andThen(failure(actual.request({ operation: "snapshot" }, Schema.Unknown), "NativeDatabaseInputError")),
        ),
      );
      yield* failure(escaped.request({ operation: "snapshot" }, Schema.Unknown), "NativeDatabaseInputError");
      yield* actual.withSession((session) =>
        Effect.gen(function* () {
          const child = yield* failure(
            session.request({ operation: "snapshot" }, Schema.Unknown),
            "NativeDatabaseInputError",
          ).pipe(Effect.forkChild);
          yield* Fiber.join(child);
        }),
      );
      checked("A released session cannot be reused and nested raw acquisition cannot deadlock");

      fault.mismatchBegin = true;
      const mismatch = yield* failure(client.put("accounts", ["bad-begin"], row("bad-begin")), "NativeDatabaseOutcomeUnknown");
      Assert.equal(mismatch.stage, "response");
      Assert.equal(yield* client.get("accounts", ["bad-begin"]), null);
      checked("A mismatched begin acknowledgement discards its helper before the body runs");

      let mismatchedBody = false;
      yield* failure(client.withTransaction(Effect.gen(function* () {
        yield* client.put("accounts", ["mismatched-savepoint-parent"], row("mismatched-savepoint-parent"));
        fault.mismatchSavepoint = true;
        yield* failure(client.withTransaction(Effect.sync(() => { mismatchedBody = true; })), "NativeDatabaseOutcomeUnknown");
      })), "NativeDatabaseOutcomeUnknown");
      Assert.equal(mismatchedBody, false);
      Assert.equal(yield* client.get("accounts", ["mismatched-savepoint-parent"]), null);
      checked("A mismatched savepoint acknowledgement invalidates the outer transaction before the nested body runs");

      for (const operation of ["application_savepoint", "application_release", "application_rollback_to"]) {
        const before = requests.length;
        const parentId = `lost-${operation}-parent`;
        const innerId = `lost-${operation}-inner`;
        yield* failure(client.withTransaction(Effect.gen(function* () {
          yield* client.put("accounts", [parentId], row(parentId));
          fault.dropOperation = operation;
          yield* failure(client.withTransaction(
            client.put("accounts", [innerId], row(innerId)).pipe(
              Effect.andThen(operation === "application_rollback_to" ? Effect.fail("nested-failure") : Effect.void),
            ),
          ), "NativeDatabaseOutcomeUnknown");
          const afterUnknown = requests.length;
          yield* failure(client.get("accounts", [parentId]), "NativeDatabaseOutcomeUnknown");
          Assert.equal(requests.length, afterUnknown);
        })), "NativeDatabaseOutcomeUnknown");
        Assert.equal(requests.slice(before).filter((entry) => entry === "application_begin").length, 1);
        Assert.ok(!requests.slice(before).includes("application_commit"));
        Assert.equal(yield* client.get("accounts", [parentId]), null);
        Assert.equal(yield* client.get("accounts", [innerId]), null);
      }
      checked("Lost savepoint, release and rollback acknowledgements remain unknown, abort the root and never retry or commit");

      const beforeMalformed = requests.length;
      yield* failure(client.withTransaction(Effect.gen(function* () {
        yield* client.put("accounts", ["fatal-parent"], row("fatal-parent"));
        yield* failure(client.withTransaction(Effect.gen(function* () {
          yield* client.put("accounts", ["fatal-inner"], row("fatal-inner"));
          fault.malformedRow = true;
          yield* client.put("accounts", ["malformed"], row("malformed"));
        })), "NativeDatabaseRejected");
        const rejected = requests.length;
        yield* failure(client.put("accounts", ["after-fatal"], row("after-fatal")), "NativeDatabaseRejected");
        Assert.equal(requests.length, rejected);
      })), "NativeDatabaseRejected");
      Assert.ok(!requests.slice(beforeMalformed).includes("application_commit"));
      Assert.equal(yield* client.get("accounts", ["fatal-parent"]), null);
      Assert.equal(yield* client.get("accounts", ["fatal-inner"]), null);
      checked("A malformed real helper frame discards the native session and failed savepoint recovery prevents all outer writes from committing");

      fault.discardBeforeCommit = true;
      yield* failure(client.put("accounts", ["helper-exit"], row("helper-exit")), "NativeDatabaseOutcomeUnknown");
      Assert.equal(yield* client.get("accounts", ["helper-exit"]), null);
      checked("Closing the helper before commit drops uncommitted state without reconnecting within the transaction");

      const beforeUnknown = requests.length;
      fault.dropCommit = true;
      const unknown = yield* failure(client.withTransaction(
        client.put("accounts", ["unknown-commit"], row("unknown-commit")).pipe(
          Effect.andThen(client.withTransaction(client.put("accounts", ["unknown-commit-inner"], row("unknown-commit-inner")))),
        ),
      ), "NativeDatabaseOutcomeUnknown");
      Assert.equal(unknown.stage, "response");
      Assert.deepEqual(requests.slice(beforeUnknown), [
        "application_begin", "application_put", "application_savepoint", "application_put", "application_release", "application_commit",
      ]);
      Assert.deepEqual(yield* client.get("accounts", ["unknown-commit"]), row("unknown-commit"));
      Assert.deepEqual(yield* client.get("accounts", ["unknown-commit-inner"]), row("unknown-commit-inner"));
      checked("A lost acknowledgement after a real commit stays unknown with exactly one commit and no retry");

      const ids = new Set();
      let after = null;
      yield* client.withTransaction(Effect.gen(function* () {
        do {
          const page = yield* client.scan("accounts", { after, limit: 2 });
          Assert.ok(page.rows.length <= 2);
          for (const entry of page.rows) {
            Assert.deepEqual(entry.key, [entry.row.id]);
            Assert.ok(!ids.has(entry.row.id));
            ids.add(entry.row.id);
          }
          Assert.notEqual(page.next, after === null ? "" : after);
          after = page.next;
        } while (after !== null);
      }));
      Assert.ok(ids.has("first") && ids.has("unknown-commit"));
      Assert.equal(yield* client.delete("accounts", ["second"]), true);
      Assert.equal(yield* client.delete("accounts", ["second"]), false);
      checked("Bounded cursor pages preserve row keys without duplicates and deletion reports existence");
    }),
  );

  await run((client) => Effect.gen(function* () {
    Assert.deepEqual(yield* client.initialize(schema), { initialized: false });
    Assert.deepEqual(yield* client.get("accounts", ["first"]), row("first", { nullable: null, nested: [1, "x"] }));
    Assert.equal(yield* client.get("accounts", ["second"]), null);
    Assert.deepEqual(yield* client.get("accounts", ["unknown-commit"]), row("unknown-commit"));
    checked("A fresh helper reopens committed data and preserves deletion after the original scope closes");
  }));

  const sources = {};
  for (const source of [
    "apps/server/src/orchestration-v2/NativeDatabase.ts",
    "apps/server/src/persistence/RedbClient.ts",
    "tools/check-redb-transactions.mjs",
  ]) {
    sources[source] = Crypto.createHash("sha256")
      .update(await FS.readFile(new URL(`../${source}`, import.meta.url)))
      .digest("hex");
  }
  const proof = {
    schema: 1,
    success: true,
    status: "passed",
    databaseExecutableSha256: Crypto.createHash("sha256").update(await FS.readFile(executable)).digest("hex"),
    implementation: "Real Effect RedbClient and NativeDatabase over the native redb row helper",
    scope: "Transaction adapter candidate; no production repository or database-default cutover",
    faultInjection: "Explicitly discarded helper sessions, acknowledged commit/savepoint responses, mismatched acknowledgements, one malformed native request and a bounded writer acquisition timeout",
    sources,
    checks,
  };
  if (proofPath) {
    await FS.mkdir(Path.dirname(Path.resolve(proofPath)), { recursive: true });
    await FS.writeFile(proofPath, JSON.stringify(proof, null, 2) + "\n");
  }
  console.log(JSON.stringify(proof));
} catch (error) {
  console.error(JSON.stringify({ completedChecks: checks, lastOperations: requests.slice(-16) }));
  throw error;
} finally {
  await fixture.close();
}
