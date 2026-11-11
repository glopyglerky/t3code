#!/usr/bin/env node
import * as NodeAssert from "node:assert/strict";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as Effect from "effect/Effect";
import * as Logger from "effect/Logger";
import * as Schema from "effect/Schema";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as NativeOwnership from "../apps/server/src/persistence/NativeServerOwnershipLock.ts";
import { persistServerRuntimeState } from "../apps/server/src/serverRuntimeState.ts";
import * as Omdb from "../apps/server/src/persistence/Omdb.ts";
import { runMigrations } from "../apps/server/src/persistence/Migrations.ts";
import { snapshotForExistingNativeImport } from "../apps/server/src/persistence/migrateSqliteToOmdb.ts";
import { createFixture } from "./database-fixture.mjs";
import {
  createMigratedSqlite,
  hash,
  nativeRequest,
  withNative,
  withSqlite,
} from "./redb-domain-fixture.mjs";
const [executable, proofPath] = process.argv.slice(2);
NodeAssert.ok(executable && NodePath.isAbsolute(executable));
const checks = [];
const currentSchema = JSON.parse(
  await NodeFSP.readFile(
    new URL("../apps/server/src/persistence/omdb-schema.json", import.meta.url),
    "utf8",
  ),
);
const legacySchema = JSON.parse(
  await NodeFSP.readFile(
    new URL("../apps/server/src/persistence/omdb-schema-v80.json", import.meta.url),
    "utf8",
  ),
);
const fixture = await createFixture();
const isolated = (fixture, effect) =>
  effect.pipe(
    Effect.provideService(HostProcess.HomeDirectory, fixture.root),
    Effect.provideService(HostProcess.Environment, {}),
    Effect.scoped,
    Effect.provide(NodeServices.layer),
    Effect.provide(NodeCrypto.layer),
    Effect.provide(Logger.layer([])),
  );
const run = (use) =>
  Effect.runPromise(
    Effect.gen(function* () {
      const runtime = yield* Omdb.make({ executable, databasePath: fixture.databasePath });
      return yield* use(runtime);
    }).pipe((effect) => isolated(fixture, effect)),
  );
const failedActivation = Effect.fn("OmdbStartupGate.failedActivation")(function* (fixture) {
  const locks = yield* NativeOwnership.make(executable);
  yield* locks.acquire(NodePath.join(fixture.root, "server-owner.sqlite"));
  return yield* Omdb.make({ executable, databasePath: fixture.databasePath }).pipe(
    Effect.provideService(Omdb.BootstrapOwnership, {
      activate: Effect.fail(
        new Omdb.OmdbStartupError({
          path: fixture.databasePath,
          cause: new Error("injected activation failure"),
        }),
      ),
    }),
  );
});
const importLegacyCandidate = Effect.fn("OmdbStartupGate.importLegacyCandidate")(
  function* (fixture, source) {
    // The previous automatic importer exported a private SQLite backup, not the
    // original file. Preserve that exact source boundary, including its schema cookie.
    const snapshot = yield* snapshotForExistingNativeImport(source);
    return yield* Effect.sync(() =>
      nativeRequest(executable, fixture, "initialize", {
        operation: "import_sqlite_application",
        path: snapshot.path,
      }),
    );
  },
);
const hasCauseMessage = (error, expected) => {
  for (let cause = error; cause; cause = cause.cause) {
    if (cause instanceof Error && cause.message.includes(expected)) return true;
  }
  return false;
};
try {
  await run(({ database }) =>
    Effect.gen(function* () {
      const migrations = yield* database.request(
        { operation: "application_migrations" },
        Schema.Array(Schema.Struct({ id: Schema.String, name: Schema.String })),
      );
      NodeAssert.deepEqual(migrations, currentSchema.migrations);
      const locks = yield* NativeOwnership.make(executable);
      yield* locks.acquire(NodePath.join(fixture.root, "server-owner.sqlite"));
    }),
  );
  NodeAssert.equal(
    await NodeFSP.readFile(NodePath.join(fixture.root, "server-owner.sqlite"), "utf8"),
    "t3code-omdb-owner-v1\n",
  );
  NodeAssert.deepEqual(
    (await NodeFSP.readdir(fixture.root)).filter((path) =>
      /state.*\.sqlite(?:-wal|-shm)?$/.test(path),
    ),
    [],
  );
  checks.push(
    "fresh native schema initializes without an application SQLite file, activates the old-server fence and releases its bootstrap lease while the database remains open",
  );
  await run(({ database }) =>
    database
      .request({ operation: "application_bootstrap" }, Omdb.Bootstrap)
      .pipe(Effect.tap((value) => Effect.sync(() => NodeAssert.equal(value.initialized, false)))),
  );
  checks.push("reopen verifies schema and migration identity without reinitializing state");
} finally {
  await fixture.close();
}
const nativeUpgrade = await createFixture();
try {
  const source = NodePath.join(nativeUpgrade.root, "statev2.sqlite");
  await withSqlite(source, () => runMigrations({ toMigrationInclusive: 80 }));
  await Effect.runPromise(isolated(nativeUpgrade, importLegacyCandidate(nativeUpgrade, source)));
  NodeAssert.deepEqual(
    nativeRequest(executable, nativeUpgrade, "connect", { operation: "application_schema" }),
    legacySchema.schema,
  );
  const text = (value) => ({ type: "text", value });
  await withNative(executable, nativeUpgrade, (client) =>
    client.withTransaction(
      Effect.gen(function* () {
        const row = Object.fromEntries(
          legacySchema.schema.tables
            .find((table) => table.name === "auth_sessions")
            .columns.map((column) => [column, { type: "null" }]),
        );
        for (const [key, value] of Object.entries({
          session_id: "native-only",
          subject: "authoritative-native-subject",
          scopes: "[]",
          method: "token",
          issued_at: "2026-01-01",
          expires_at: "2099-01-01",
        }))
          row[key] = text(value);
        yield* client.put("auth_sessions", [text("native-only")], row);
      }),
    ),
  );
  await NodeFSP.writeFile(
    NodePath.join(nativeUpgrade.root, "server-owner.sqlite"),
    "t3code-omdb-owner-v1\n",
  );
  await NodeFSP.writeFile(source, "stale preserved source must never be opened after activation");
  const sourceBefore = hash(await NodeFSP.readFile(source));
  for (let reopen = 0; reopen < 2; reopen++) {
    await Effect.runPromise(
      isolated(
        nativeUpgrade,
        Effect.gen(function* () {
          const { client, database } = yield* Omdb.make({
            executable,
            databasePath: nativeUpgrade.databasePath,
          });
          const migrations = yield* database.request(
            { operation: "application_migrations" },
            Schema.Array(Schema.Struct({ id: Schema.String, name: Schema.String })),
          );
          NodeAssert.deepEqual(migrations, currentSchema.migrations);
          const row = yield* client.get("auth_sessions", [text("native-only")]);
          NodeAssert.equal(row.subject.value, "authoritative-native-subject");
        }),
      ),
    );
  }
  NodeAssert.equal(hash(await NodeFSP.readFile(source)), sourceBefore);
  checks.push(
    "Activated native80 state upgrades in place to81 and reopens, preserving native-only writes without opening its stale SQLite source",
  );
} finally {
  await nativeUpgrade.close();
}
const migrated = await createFixture();
try {
  const source = NodePath.join(migrated.root, "statev2.sqlite");
  await createMigratedSqlite(source, (sql) =>
    sql`INSERT INTO auth_sessions (session_id,subject,scopes,method,issued_at,expires_at) VALUES ('kept','kept-subject','[]','token','2026-01-01','2099-01-01')`.pipe(
      Effect.asVoid,
    ),
  );
  const before = hash(await NodeFSP.readFile(source));
  await Effect.runPromise(
    Effect.gen(function* () {
      const { client } = yield* Omdb.make({ executable, databasePath: migrated.databasePath });
      const row = yield* client.get("auth_sessions", [{ type: "text", value: "kept" }]);
      NodeAssert.equal(row.subject.value, "kept-subject");
    }).pipe((effect) => isolated(migrated, effect)),
  );
  NodeAssert.equal(hash(await NodeFSP.readFile(source)), before);
  NodeAssert.equal(
    (await NodeFSP.readdir(migrated.root)).filter((path) => path.startsWith(".omdb-import-"))
      .length,
    0,
  );
  checks.push(
    "migrated SQLite copy imports all current rows, leaves source unchanged and cleans only owned staging files",
  );
} finally {
  await migrated.close();
}
const liveLegacy = await createFixture();
try {
  const source = NodePath.join(liveLegacy.root, "statev2.sqlite");
  await createMigratedSqlite(source);
  const before = hash(await NodeFSP.readFile(source));
  const runtimePath = NodePath.join(liveLegacy.root, "server-runtime.json");
  await Effect.runPromise(
    isolated(
      liveLegacy,
      persistServerRuntimeState({
        path: runtimePath,
        state: {
          version: 1,
          pid: process.pid,
          port: 19237,
          origin: "http://127.0.0.1:19237",
          startedAt: new Date().toISOString(),
        },
      }),
    ),
  );
  const originalRuntime = await NodeFSP.readFile(runtimePath, "utf8");
  await NodeAssert.rejects(
    Effect.runPromise(
      isolated(liveLegacy, Omdb.make({ executable, databasePath: liveLegacy.databasePath })),
    ),
    (error) =>
      error._tag === "OmdbStartupError" && error.cause?._tag === "ServerAlreadyRunningError",
  );
  NodeAssert.equal(
    hash(await NodeFSP.readFile(source)),
    before,
    "Rejected migration cannot alter the source",
  );
  NodeAssert.equal(
    await NodeFSP.readFile(runtimePath, "utf8"),
    originalRuntime,
    "Cannot clear another owner's runtime record",
  );
  await NodeAssert.rejects(NodeFSP.access(liveLegacy.databasePath), { code: "ENOENT" });
  NodeAssert.equal(
    await NodeFSP.readFile(NodePath.join(liveLegacy.root, "server-owner.sqlite"), "utf8"),
    "",
  );
  // Failed bootstrap must release its own native lease, without terminating the
  // live PID used as this controlled pre-lock-server fixture.
  await Effect.runPromise(
    isolated(
      liveLegacy,
      Effect.gen(function* () {
        const locks = yield* NativeOwnership.make(executable);
        yield* locks.acquire(NodePath.join(liveLegacy.root, "server-owner.sqlite"));
      }),
    ),
  );
  checks.push(
    "CLI migration rejects a live pre-lock server before native initialization, preserves source/runtime bytes and releases its temporary lease",
  );
} finally {
  await liveLegacy.close();
}
for (const changed of [false, true]) {
  const retry = await createFixture();
  try {
    const source = NodePath.join(retry.root, "statev2.sqlite");
    await createMigratedSqlite(source, (sql) =>
      sql`
      INSERT INTO auth_sessions (session_id,subject,scopes,method,issued_at,expires_at)
      VALUES ('retry-row','original','[]','token','2026-01-01','2099-01-01')
    `.pipe(Effect.asVoid),
    );
    await NodeAssert.rejects(Effect.runPromise(isolated(retry, failedActivation(retry))), (error) =>
      hasCauseMessage(error, "injected activation failure"),
    );
    NodeAssert.equal(
      await NodeFSP.readFile(NodePath.join(retry.root, "server-owner.sqlite"), "utf8"),
      "",
    );
    if (changed) {
      await withSqlite(
        source,
        (sql) => sql`UPDATE auth_sessions SET subject='newer-source' WHERE session_id='retry-row'`,
      );
      const sourceBefore = hash(await NodeFSP.readFile(source));
      const nativeBefore = hash(await NodeFSP.readFile(retry.databasePath));
      await NodeAssert.rejects(
        Effect.runPromise(
          isolated(retry, Omdb.make({ executable, databasePath: retry.databasePath })),
        ),
        (error) => hasCauseMessage(error, "SQLite no longer matches"),
      );
      NodeAssert.equal(hash(await NodeFSP.readFile(source)), sourceBefore);
      NodeAssert.equal(
        hash(await NodeFSP.readFile(retry.databasePath)),
        nativeBefore,
        "Stale-candidate rejection is strictly read-only",
      );
      NodeAssert.equal(
        await NodeFSP.readFile(NodePath.join(retry.root, "server-owner.sqlite"), "utf8"),
        "",
      );
      checks.push(
        "Retry rejects an unactivated import after new SQLite writes, retaining both files byte-for-byte without activating the fence",
      );
    } else {
      await Effect.runPromise(
        isolated(
          retry,
          Effect.gen(function* () {
            const { client } = yield* Omdb.make({ executable, databasePath: retry.databasePath });
            const row = yield* client.get("auth_sessions", [{ type: "text", value: "retry-row" }]);
            NodeAssert.equal(row.subject.value, "original");
          }),
        ),
      );
      NodeAssert.equal(
        await NodeFSP.readFile(NodePath.join(retry.root, "server-owner.sqlite"), "utf8"),
        "t3code-omdb-owner-v1\n",
      );
      checks.push(
        "Gracefully interrupted current-schema imports resume only after the fresh canonical export exactly matches the durable receipt",
      );
    }
    NodeAssert.equal(
      (await NodeFSP.readdir(retry.root)).filter((path) => path.startsWith(".omdb-import-")).length,
      0,
    );
  } finally {
    await retry.close();
  }
}
for (const changed of [false, true]) {
  const retry = await createFixture();
  try {
    const source = NodePath.join(retry.root, "statev2.sqlite");
    await withSqlite(source, (sql) =>
      Effect.gen(function* () {
        yield* runMigrations({ toMigrationInclusive: 80 });
        yield* sql`INSERT INTO auth_sessions (session_id,subject,scopes,method,issued_at,expires_at)
        VALUES ('retry80-row','original80','[]','token','2026-01-01','2099-01-01')`;
        yield* sql`PRAGMA wal_checkpoint(TRUNCATE)`;
      }),
    );
    await Effect.runPromise(isolated(retry, importLegacyCandidate(retry, source)));
    if (changed) {
      await withSqlite(
        source,
        (sql) => sql`UPDATE auth_sessions SET subject='newer80' WHERE session_id='retry80-row'`,
      );
    }
    const sourceBefore = hash(await NodeFSP.readFile(source));
    const nativeBefore = hash(await NodeFSP.readFile(retry.databasePath));
    if (changed) {
      await NodeAssert.rejects(
        Effect.runPromise(
          isolated(retry, Omdb.make({ executable, databasePath: retry.databasePath })),
        ),
        (error) => hasCauseMessage(error, "SQLite no longer matches"),
      );
      NodeAssert.equal(hash(await NodeFSP.readFile(retry.databasePath)), nativeBefore);
      NodeAssert.equal(
        await NodeFSP.readFile(NodePath.join(retry.root, "server-owner.sqlite"), "utf8"),
        "",
      );
      checks.push(
        "An unactivated80 candidate rejects newer80 SQLite writes without changing either file or activating ownership",
      );
    } else {
      for (let reopen = 0; reopen < 2; reopen++) {
        await Effect.runPromise(
          isolated(
            retry,
            Effect.gen(function* () {
              const { client, database } = yield* Omdb.make({
                executable,
                databasePath: retry.databasePath,
              });
              const row = yield* client.get("auth_sessions", [
                { type: "text", value: "retry80-row" },
              ]);
              NodeAssert.equal(row.subject.value, "original80");
              const migrations = yield* database.request(
                { operation: "application_migrations" },
                Schema.Array(Schema.Struct({ id: Schema.String, name: Schema.String })),
              );
              NodeAssert.deepEqual(migrations, currentSchema.migrations);
            }),
          ),
        );
      }
      NodeAssert.equal(
        await NodeFSP.readFile(NodePath.join(retry.root, "server-owner.sqlite"), "utf8"),
        "t3code-omdb-owner-v1\n",
      );
      checks.push(
        "An unchanged unactivated80 import verifies its original source boundary, upgrades natively to81 and reopens without migrating the retained source",
      );
    }
    NodeAssert.equal(hash(await NodeFSP.readFile(source)), sourceBefore);
    NodeAssert.equal(
      (await NodeFSP.readdir(retry.root)).filter((path) => path.startsWith(".omdb-import-")).length,
      0,
    );
  } finally {
    await retry.close();
  }
}
const unwitnessed = await createFixture();
try {
  const source = NodePath.join(unwitnessed.root, "statev2.sqlite");
  await createMigratedSqlite(source);
  nativeRequest(executable, unwitnessed, "initialize", { operation: "application_bootstrap" });
  const sourceBefore = hash(await NodeFSP.readFile(source));
  const nativeBefore = hash(await NodeFSP.readFile(unwitnessed.databasePath));
  await NodeAssert.rejects(
    Effect.runPromise(
      isolated(unwitnessed, Omdb.make({ executable, databasePath: unwitnessed.databasePath })),
    ),
    (error) => hasCauseMessage(error, "no verifiable SQLite import source"),
  );
  NodeAssert.equal(hash(await NodeFSP.readFile(source)), sourceBefore);
  NodeAssert.equal(hash(await NodeFSP.readFile(unwitnessed.databasePath)), nativeBefore);
  NodeAssert.equal(
    await NodeFSP.readFile(NodePath.join(unwitnessed.root, "server-owner.sqlite"), "utf8"),
    "",
  );
  checks.push(
    "An unactivated catalog without an import receipt cannot displace an existing SQLite source",
  );
} finally {
  await unwitnessed.close();
}
console.log(JSON.stringify({ checks }, null, 2));
if (proofPath)
  await NodeFSP.writeFile(
    proofPath,
    JSON.stringify(
      { schema: 1, checks, executable_sha256: hash(await NodeFSP.readFile(executable)) },
      null,
      2,
    ) + "\n",
  );
