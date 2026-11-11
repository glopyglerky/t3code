#!/usr/bin/env node
import * as Assert from "node:assert/strict";
import * as ChildProcess from "node:child_process";
import * as Events from "node:events";
import * as FS from "node:fs/promises";
import * as Path from "node:path";
import * as Readline from "node:readline";
import * as Sqlite from "node:sqlite";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Logger from "effect/Logger";
import * as Schema from "effect/Schema";
import * as SecureRandom from "effect/crypto/SecureRandom";

import * as NativeDatabase from "../apps/server/src/orchestration-v2/NativeDatabase.ts";
import * as NativeStorage from "../apps/server/src/persistence/NativeDatabaseStorageBackend.ts";
import * as NativeMaintenance from "../apps/server/src/persistence/NativeDatabaseMaintenance.ts";
import * as NativeOwnership from "../apps/server/src/persistence/NativeServerOwnershipLock.ts";
import * as ServerOwnership from "../apps/server/src/serverOwnership.ts";
import * as ProcessRunner from "../apps/server/src/processRunner.ts";
import { createFixture } from "./database-fixture.mjs";
import { exportSqliteState } from "./export-sqlite-state.mjs";
import {
  createMigratedSqlite,
  finishProof,
  hash,
  nativeRequest,
  normalized,
  withNative,
} from "./redb-domain-fixture.mjs";

const [executable, proofPath] = process.argv.slice(2);
Assert.ok(executable && Path.isAbsolute(executable), "Pass an absolute native database executable");
await FS.access(executable, FS.constants.X_OK);
const fixture = await createFixture();
const checks = [];
const run = (effect) =>
  Effect.runPromise(
    effect.pipe(
      Effect.scoped,
      Effect.provide(NodeServices.layer),
      Effect.provide(SecureRandom.layerWebCrypto),
      Effect.provide(Logger.layer([])),
    ),
  );
const failed = (body, tag) =>
  Effect.exit(body).pipe(
    Effect.tap((exit) =>
      Effect.sync(() => {
        Assert.ok(Exit.isFailure(exit));
        if (tag) Assert.ok(JSON.stringify(exit).includes(tag), JSON.stringify(exit));
      }),
    ),
  );
function owner(path) {
  const child = ChildProcess.spawn(executable, ["owner-lock", path], {
    env: {},
    stdio: ["pipe", "pipe", "pipe"],
  });
  const lines = Readline.createInterface({ input: child.stdout });
  const iterator = lines[Symbol.asyncIterator]();
  child.stderr.on("data", (chunk) => Assert.fail(`Ownership helper wrote stderr: ${chunk}`));
  return {
    child,
    request: async (operation) => {
      child.stdin.write(JSON.stringify({ operation }) + "\n");
      const frame = await iterator.next();
      Assert.equal(frame.done, false);
      return JSON.parse(frame.value);
    },
    close: async (signal) => {
      if (child.exitCode !== null || child.signalCode !== null) return;
      const closed = Events.once(child, "exit");
      if (signal) child.kill(signal);
      else child.stdin.end();
      await closed;
      lines.close();
    },
  };
}

try {
  const lockPath = Path.join(fixture.root, "server-owner.sqlite");
  await FS.writeFile(lockPath, "");
  // Read before taking SQLite's POSIX lock: closing any same-inode descriptor
  // in the locking process would release that process's SQLite locks.
  const beforeLock = hash(await FS.readFile(lockPath));
  const sqlite = new Sqlite.DatabaseSync(lockPath);
  sqlite.exec("PRAGMA busy_timeout = 0; BEGIN EXCLUSIVE;");
  const blockedNative = owner(lockPath);
  try {
    const rejected = await blockedNative.request("owner_acquire");
    Assert.equal(rejected.ok, false);
    Assert.equal(
      rejected.error.kind,
      "writer_busy",
      "SQLite exclusive byte range must block the native owner",
    );
  } finally {
    await blockedNative.close();
    sqlite.close();
  }
  const nativeOwner = owner(lockPath);
  let second;
  try {
    Assert.equal((await nativeOwner.request("owner_acquire")).result.owned, true);
    const competingSqlite = new Sqlite.DatabaseSync(lockPath);
    try {
      Assert.throws(
        () => competingSqlite.exec("PRAGMA busy_timeout = 0; BEGIN EXCLUSIVE;"),
        (error) => error.errcode === 5 || error.code === "SQLITE_BUSY",
      );
    } finally {
      competingSqlite.close();
    }
    second = owner(lockPath);
    Assert.equal((await second.request("owner_acquire")).error.kind, "writer_busy");
    await nativeOwner.close("SIGKILL");
    Assert.equal(
      (await second.request("owner_acquire")).result.owned,
      true,
      "Killed owner must release the byte-range lock",
    );
    Assert.equal((await second.request("owner_check")).result.owned, true);
  } finally {
    await nativeOwner.close();
    if (second) await second.close();
  }
  Assert.equal(
    hash(await FS.readFile(lockPath)),
    beforeLock,
    "Native locking never changes SQLite lock-file contents",
  );
  checks.push(
    "Real SQLite and native processes exclude each other in both directions; native peers are excluded and SIGKILL releases the lock without unlinking or changing lock-file bytes",
  );
  await run(
    Effect.gen(function* () {
      const locks = yield* NativeOwnership.make(executable);
      const lock = yield* locks.acquire(lockPath);
      Assert.ok(lock.activateNative);
      yield* lock.activateNative;
      yield* lock.activateNative;
    }),
  );
  Assert.equal(await FS.readFile(lockPath, "utf8"), "t3code-omdb-owner-v1\n");
  const retiredSqlite = new Sqlite.DatabaseSync(lockPath);
  try {
    Assert.throws(
      () => retiredSqlite.exec("PRAGMA busy_timeout = 0; BEGIN EXCLUSIVE;"),
      (error) => error.errcode === 26,
    );
  } finally {
    retiredSqlite.close();
  }
  checks.push(
    "Explicit native activation durably fences old SQLite servers after lock release; activation is idempotent and ordinary acquisition remains nonmutating",
  );
  await run(
    Effect.gen(function* () {
      const locks = yield* NativeOwnership.make(executable);
      const lock = yield* locks.acquire(lockPath);
      Assert.equal(yield* lock.verify, true);
      yield* failed(locks.acquire(lockPath), "OwnershipLockError");
      yield* Effect.promise(() => FS.rename(lockPath, `${lockPath}.retained`));
      yield* Effect.promise(() => FS.writeFile(lockPath, "replacement"));
      Assert.equal(
        yield* lock.verify,
        false,
        "Native adapter detects replacement and never reacquires ownership",
      );
    }),
  );
  checks.push(
    "Actual native ownership adapter checks the held inode and refuses reacquisition after lock replacement",
  );
  const sharedDirectory = Path.join(fixture.root, "shared-owner");
  const sharedStatePath = Path.join(sharedDirectory, "server-runtime.json");
  const sharedLockPath = Path.join(sharedDirectory, "server-owner.sqlite");
  const runtimeState = {
    version: 1,
    pid: process.pid,
    port: 4173,
    origin: "http://127.0.0.1:4173",
    startedAt: "2026-11-10T12:00:00.000Z",
  };
  await run(
    Effect.gen(function* () {
      const ownership = yield* ServerOwnership.acquireServerOwnershipWithLock(sharedStatePath).pipe(
        Effect.provide(NativeOwnership.layer(executable)),
      );
      yield* ownership.publish(runtimeState);
      const published = yield* Effect.promise(async () =>
        JSON.parse(await FS.readFile(sharedStatePath, "utf8")),
      );
      Assert.equal(typeof published.ownerId, "string");
      yield* ownership.activateNative;
      yield* Effect.promise(() => FS.rename(sharedLockPath, `${sharedLockPath}.retained`));
      yield* Effect.promise(() => FS.writeFile(sharedLockPath, "replacement"));
      yield* failed(ownership.publish(runtimeState), "ServerOwnershipError");
      yield* failed(ownership.awaitLost.pipe(Effect.timeout("7 seconds")), "ServerOwnershipError");
    }).pipe(
      Effect.provide(ProcessRunner.layer),
      Effect.provideService(HostProcess.HomeDirectory, fixture.root),
      Effect.provideService(HostProcess.Environment, {}),
    ),
  );
  Assert.equal(
    await FS.stat(sharedStatePath).then(
      () => true,
      () => false,
    ),
    false,
    "Owned runtime record is cleared on scope exit",
  );
  checks.push(
    "Shared ownership service publishes a tagged runtime record, refuses publication after lock loss, signals awaitLost for server shutdown and clears only its own runtime record on exit",
  );

  const source = Path.join(fixture.root, "source.sqlite");
  const legacy = Path.join(fixture.root, "state.sqlite");
  await createMigratedSqlite(source);
  const db = new Sqlite.DatabaseSync(source);
  try {
    db.exec("CREATE TABLE maintenance_fixture (id TEXT PRIMARY KEY,value TEXT NOT NULL)");
    const put = db.prepare("INSERT INTO maintenance_fixture VALUES (?,?)");
    db.exec("BEGIN");
    for (let index = 0; index < 192; index++)
      put.run(`row:${index}`, `value:${index}:` + "x".repeat(8_192));
    db.exec("COMMIT");
  } finally {
    db.close();
  }
  await FS.copyFile(source, legacy);
  const sourceHash = hash(await FS.readFile(source));
  const exported = Path.join(fixture.root, "source.ndjson");
  exportSqliteState({ source, destination: exported });
  nativeRequest(executable, fixture, "initialize", {
    operation: "import_sqlite_application",
    path: exported,
  });
  const manualBackup = Path.join(fixture.root, "verified-backup.omdb");
  let liveRows;
  await withNative(executable, fixture, (client, database) =>
    Effect.gen(function* () {
      const storage = yield* NativeStorage.make.pipe(
        Effect.provideService(NativeDatabase.NativeDatabase, database),
      );
      const before = yield* storage.inspect;
      Assert.ok(before.databaseBytes > 0);
      Assert.equal(before.incrementalVacuum, false);
      Assert.equal(
        before.reclaimableBytes,
        0,
        "Reusable allocator capacity is not promised filesystem shrink",
      );
      yield* storage.reclaim;
      for (let offset = 0; offset < 160; offset += 16)
        yield* client.withTransaction(
          Effect.gen(function* () {
            for (let index = offset; index < offset + 16; index++)
              yield* client.delete("maintenance_fixture", [
                { type: "text", value: `row:${index}` },
              ]);
          }),
        );
      liveRows = yield* Effect.all(
        [0, 159, 160, 191].map((index) =>
          client.get("maintenance_fixture", [{ type: "text", value: `row:${index}` }]),
        ),
      );
      yield* database.request(
        { operation: "storage_backup", destination: manualBackup },
        Schema.Unknown,
      );
      const backupHash = yield* Effect.promise(async () => hash(await FS.readFile(manualBackup)));
      yield* failed(
        database.request(
          { operation: "storage_backup", destination: manualBackup },
          Schema.Unknown,
        ),
        "NativeDatabaseRejected",
      );
      Assert.equal(
        yield* Effect.promise(async () => hash(await FS.readFile(manualBackup))),
        backupHash,
      );
      yield* failed(
        database.request({ operation: "storage_compact" }, Schema.Unknown),
        "NativeDatabaseRejected",
      );
      const maintenance = yield* NativeMaintenance.make(executable);
      yield* failed(maintenance.compact(fixture.databasePath), "DatabaseInUseError");
    }).pipe(Effect.provide(SecureRandom.layerWebCrypto)),
  );
  checks.push(
    "Physical stats use the native engine; online compact is rejected; snapshot backups use create-new and do not overwrite existing files; exclusive maintenance rejects live readers/writers",
  );

  const result = await run(
    Effect.gen(function* () {
      const maintenance = yield* NativeMaintenance.make(executable);
      const before = yield* maintenance.inspect(fixture.databasePath);
      const compacted = yield* maintenance.compact(fixture.databasePath);
      Assert.ok(compacted.beforeBytes > 0 && compacted.afterBytes > 0);
      Assert.ok(compacted.afterBytes <= compacted.beforeBytes);
      const after = yield* maintenance.inspect(fixture.databasePath);
      Assert.equal(after.databaseBytes, compacted.afterBytes);
      Assert.equal(
        before.legacyDatabaseBytes,
        null,
        "Protected migration files are not offered for legacy deletion",
      );
      yield* failed(
        maintenance.removeLegacyDatabase(fixture.databasePath),
        "LegacyDatabaseRemoveError",
      );
      yield* failed(
        maintenance.inspect(Path.join(fixture.root, "missing.omdb")),
        "DatabaseMissingError",
      );
      return { before, after, compacted };
    }),
  );
  await withNative(executable, fixture, (client) =>
    Effect.gen(function* () {
      Assert.deepEqual(
        normalized(
          yield* Effect.all(
            [0, 159, 160, 191].map((index) =>
              client.get("maintenance_fixture", [{ type: "text", value: `row:${index}` }]),
            ),
          ),
        ),
        normalized(liveRows),
      );
    }),
  );
  const backups = (await FS.readdir(fixture.root)).filter((name) =>
    name.startsWith(`${Path.basename(fixture.databasePath)}.precompact-`),
  );
  Assert.equal(backups.length, 1);
  for (const backup of [manualBackup, Path.join(fixture.root, backups[0])]) {
    const target = { ...fixture, databasePath: backup };
    await withNative(executable, target, (client) =>
      Effect.gen(function* () {
        Assert.deepEqual(
          normalized(
            yield* Effect.all(
              [0, 159, 160, 191].map((index) =>
                client.get("maintenance_fixture", [{ type: "text", value: `row:${index}` }]),
              ),
            ),
          ),
          normalized(liveRows),
        );
      }),
    );
  }
  Assert.equal(hash(await FS.readFile(source)), sourceHash);
  Assert.equal(hash(await FS.readFile(legacy)), sourceHash);
  checks.push(
    "Actual native maintenance makes a verified restorable backup before pruning/compaction, reopens the compacted store, preserves values and all legacy SQLite bytes, and classifies missing stores correctly",
  );
  console.log(JSON.stringify({ storage: result }, null, 2));
  await finishProof({
    executable,
    fixture,
    proofPath,
    checks,
    sourceHash,
    oracles: [],
    sourcePaths: [
      "apps/server/src/serverOwnership.ts",
      "apps/server/src/orchestration-v2/NativeDatabase.ts",
      "apps/server/src/persistence/ServerOwnershipLock.ts",
      "apps/server/src/persistence/NativeServerOwnershipLock.ts",
      "apps/server/src/persistence/SqliteServerOwnershipLock.ts",
      "apps/server/src/persistence/DatabaseStorage.ts",
      "apps/server/src/persistence/DatabaseStorageBackend.ts",
      "apps/server/src/persistence/NativeDatabaseStorageBackend.ts",
      "apps/server/src/persistence/SqliteDatabaseStorageBackend.ts",
      "apps/server/src/persistence/NativeDatabaseMaintenance.ts",
      "apps/server/src/persistence/redb_database_storage.rs",
      "apps/server/src/persistence/redb_server_ownership.rs",
      "crates/storage/src/backend.rs",
      "tools/check-redb-storage-maintenance.mjs",
    ],
  });
} finally {
  await fixture.close();
}
