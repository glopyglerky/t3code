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
const fault = { dropCommit: false, mismatchBegin: false, discardBeforeCommit: false };
const schema = {
  version: 1,
  tables: [{ name: "accounts", columns: ["id", "payload"], primary_key: ["id"] }],
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
    request: (input, response) => {
      requests.push(input.operation);
      return database.request(input, response);
    },
    withSession: (use) =>
      database.withSession((session) =>
        use({
          discard: session.discard,
          request: (input, response) =>
            Effect.gen(function* () {
              requests.push(input.operation);
              if (input.operation === "application_commit" && fault.discardBeforeCommit) {
                fault.discardBeforeCommit = false;
                yield* session.discard;
              }
              const result = yield* session.request(input, response);
              if (input.operation === "application_begin" && fault.mismatchBegin) {
                fault.mismatchBegin = false;
                return { transaction_id: "different-transaction" };
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

      yield* client.withTransaction(
        Effect.gen(function* () {
          const before = requests.length;
          yield* failure(
            client.withTransaction(client.put("accounts", ["nested"], row("nested"))),
            "RedbTransactionError",
            "nested-transaction",
          );
          Assert.equal(requests.length, before);
          yield* client.put("accounts", ["outer"], row("outer"));
        }),
      );
      Assert.equal(yield* client.get("accounts", ["nested"]), null);
      Assert.deepEqual(yield* client.get("accounts", ["outer"]), row("outer"));
      checked("Nested transactions reject before any nested request while the outer transaction stays valid");

      yield* client.withTransaction(
        Effect.gen(function* () {
          const before = requests.length;
          const child = yield* failure(
            client.put("accounts", ["child"], row("child")),
            "RedbTransactionError",
            "different-fiber",
          ).pipe(Effect.forkChild);
          yield* Fiber.join(child);
          Assert.equal(requests.length, before);
          yield* client.put("accounts", ["parent"], row("parent"));
        }),
      );
      Assert.equal(yield* client.get("accounts", ["child"]), null);
      checked("Inherited transaction context cannot authorize writes by a child fiber");

      const secondClient = yield* RedbClient.make.pipe(
        Effect.provideService(NativeDatabase.NativeDatabase, database),
      );
      yield* client.withTransaction(
        failure(secondClient.get("accounts", ["first"]), "RedbTransactionError", "different-client"),
      );
      checked("A different client cannot reuse the current transaction");

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

      fault.discardBeforeCommit = true;
      yield* failure(client.put("accounts", ["helper-exit"], row("helper-exit")), "NativeDatabaseOutcomeUnknown");
      Assert.equal(yield* client.get("accounts", ["helper-exit"]), null);
      checked("Closing the helper before commit drops uncommitted state without reconnecting within the transaction");

      const beforeUnknown = requests.length;
      fault.dropCommit = true;
      const unknown = yield* failure(client.put("accounts", ["unknown-commit"], row("unknown-commit")), "NativeDatabaseOutcomeUnknown");
      Assert.equal(unknown.stage, "response");
      Assert.deepEqual(requests.slice(beforeUnknown), ["application_begin", "application_put", "application_commit"]);
      Assert.deepEqual(yield* client.get("accounts", ["unknown-commit"]), row("unknown-commit"));
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
    faultInjection: "Explicitly discarded helper sessions and one acknowledged native commit response",
    sources,
    checks,
  };
  if (proofPath) {
    await FS.mkdir(Path.dirname(Path.resolve(proofPath)), { recursive: true });
    await FS.writeFile(proofPath, JSON.stringify(proof, null, 2) + "\n");
  }
  console.log(JSON.stringify(proof));
} finally {
  await fixture.close();
}
