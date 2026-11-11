#!/usr/bin/env node
import * as Assert from "node:assert/strict";
import * as ChildProcess from "node:child_process";
import * as FS from "node:fs/promises";
import * as Path from "node:path";
import * as Readline from "node:readline";
import * as Effect from "effect/Effect";

import {
  BUILD_MIGRATION_HEAD,
  databaseMigrationHead,
  recordedMigrations,
} from "../apps/server/src/persistence/databaseMigrationHead.ts";
import { migrationManifest } from "../apps/server/src/persistence/Migrations.ts";
import { runServicePreflight } from "../apps/server/src/cloud/servicePreflight.ts";
import { SERVICE_LAUNCHER_PROTOCOL } from "../apps/server/src/cloud/serviceProtocol.ts";
import { createFixture } from "./database-fixture.mjs";
import {
  createMigratedSqlite,
  finishProof,
  hash,
  nativeRequest,
  withNative,
} from "./redb-domain-fixture.mjs";

const [executable, proofPath] = process.argv.slice(2);
Assert.ok(executable && Path.isAbsolute(executable), "Pass the absolute native helper");
const fixture = await createFixture();
const checks = [];
const prior = {
  executable: process.env.T3_CODE_LS_DB_EXECUTABLE,
  implementation: process.env.T3_CODE_LS_IMPLEMENTATION,
};
process.env.T3_CODE_LS_DB_EXECUTABLE = executable;
delete process.env.T3_CODE_LS_IMPLEMENTATION;
const preflight = (databasePath) =>
  runServicePreflight({
    databasePath,
    launcherProtocol: SERVICE_LAUNCHER_PROTOCOL,
    version: "fixture-version",
  });
const inspectNative = (databasePath, request) => {
  const child = ChildProcess.spawnSync(executable, ["read-only-local", databasePath], {
    input: `${JSON.stringify(request)}\n`,
    encoding: "utf8",
    env: {},
    timeout: 10_000,
    maxBuffer: 1_048_576,
  });
  Assert.equal(child.error, undefined);
  Assert.equal(child.status, 0, child.stderr);
  return JSON.parse(child.stdout.trim());
};
try {
  for (const extension of ["omdb", "sqlite"]) {
    const missing = Path.join(fixture.root, `missing.${extension}`);
    Assert.deepEqual(recordedMigrations(missing), []);
    Assert.equal(databaseMigrationHead(missing), BUILD_MIGRATION_HEAD);
    Assert.equal(preflight(missing).status, "ready");
    await Assert.rejects(FS.access(missing));
  }
  const absentNative = Path.join(fixture.root, "never-created.omdb");
  Assert.equal(inspectNative(absentNative, { operation: "application_migrations" }).ok, false);
  await Assert.rejects(FS.access(absentNative));
  checks.push(
    "missing paths report a fresh install without creating SQLite or omdb files, even through the read-only helper",
  );

  nativeRequest(executable, fixture, "initialize", { operation: "application_bootstrap" });
  const baseline = hash(await FS.readFile(fixture.databasePath));
  Assert.deepEqual(
    recordedMigrations(fixture.databasePath),
    migrationManifest.map(([id, name]) => ({ id, name })),
  );
  Assert.equal(databaseMigrationHead(fixture.databasePath), BUILD_MIGRATION_HEAD);
  Assert.equal(preflight(fixture.databasePath).status, "ready");
  Assert.equal(
    inspectNative(fixture.databasePath, { operation: "application_bootstrap" }).ok,
    false,
  );
  Assert.equal(hash(await FS.readFile(fixture.databasePath)), baseline);
  checks.push(
    "real native migrations drive migration-head and service preflight; read-only handles reject bootstrap and leave source bytes unchanged",
  );

  const nativeAlias = Path.join(fixture.root, "native-under-sqlite-name.sqlite");
  await FS.copyFile(fixture.databasePath, nativeAlias);
  Assert.equal(databaseMigrationHead(nativeAlias), BUILD_MIGRATION_HEAD);
  Assert.equal(hash(await FS.readFile(nativeAlias)), baseline);
  checks.push(
    "format detection routes a native file with a legacy extension through omdb, never SQLite",
  );

  const future = BUILD_MIGRATION_HEAD + 1;
  await withNative(executable, fixture, (client) =>
    client.withTransaction(
      Effect.gen(function* () {
        yield* client.put("effect_sql_migrations", [{ type: "integer", value: String(future) }], {
          migration_id: { type: "integer", value: String(future) },
          name: { type: "text", value: "FromFutureNative" },
          created_at: { type: "text", value: "2026-11-10 12:00:00" },
        });
        Assert.equal(databaseMigrationHead(fixture.databasePath), BUILD_MIGRATION_HEAD);
      }),
    ),
  );
  const committedNative = inspectNative(fixture.databasePath, {
    operation: "application_migrations",
  });
  Assert.equal(committedNative.ok, true, JSON.stringify(committedNative));
  Assert.equal(databaseMigrationHead(fixture.databasePath), future);
  const blocked = preflight(fixture.databasePath);
  Assert.equal(blocked.status, "blocked");
  Assert.match(blocked.reason, new RegExp(`${future}_FromFutureNative`));
  checks.push(
    "read-only preflight sees committed migrations during an active native writer and blocks a newer migration after commit",
  );

  const crashed = Path.join(fixture.root, "crashed.omdb");
  await FS.copyFile(nativeAlias, crashed);
  const writer = ChildProcess.spawn(executable, ["local", crashed], {
    env: {},
    stdio: ["pipe", "pipe", "pipe"],
    signal: AbortSignal.timeout(15_000),
  });
  const exited = new Promise((resolve, reject) => {
    writer.once("exit", resolve);
    writer.once("error", reject);
  });
  const lines = Readline.createInterface({ input: writer.stdout });
  const replies = lines[Symbol.asyncIterator]();
  const transact = async (request) => {
    writer.stdin.write(JSON.stringify(request) + "\n");
    const response = await replies.next();
    Assert.equal(response.done, false);
    const envelope = JSON.parse(response.value);
    Assert.equal(envelope.ok, true, JSON.stringify(envelope));
  };
  try {
    await transact({
      operation: "application_begin",
      transaction_id: "crash-writer",
      mode: "write",
    });
    await transact({
      operation: "application_put",
      transaction_id: "crash-writer",
      table: "effect_sql_migrations",
      key: [{ type: "integer", value: String(future) }],
      row: {
        migration_id: { type: "integer", value: String(future) },
        name: { type: "text", value: "FromFutureNative" },
        created_at: { type: "text", value: "2026-11-10 12:00:00" },
      },
    });
    await transact({ operation: "application_commit", transaction_id: "crash-writer" });
  } finally {
    writer.kill("SIGKILL");
    await exited;
    lines.close();
  }
  const crashedHash = hash(await FS.readFile(crashed));
  Assert.equal(recordedMigrations(crashed), null);
  Assert.equal(preflight(crashed).status, "blocked");
  Assert.equal(hash(await FS.readFile(crashed)), crashedHash);
  checks.push(
    "a deliberately killed writer remains recovery-required; read-only preflight fails closed without repairing or changing the file",
  );

  const sqlite = Path.join(fixture.root, "legacy.sqlite");
  await createMigratedSqlite(sqlite);
  const sqliteHash = hash(await FS.readFile(sqlite));
  Assert.deepEqual(
    recordedMigrations(sqlite),
    migrationManifest.map(([id, name]) => ({ id, name })),
  );
  Assert.equal(preflight(sqlite).status, "ready");
  Assert.equal(hash(await FS.readFile(sqlite)), sqliteHash);
  const mislabeled = Path.join(fixture.root, "sqlite-is-not-native.omdb");
  await FS.copyFile(sqlite, mislabeled);
  Assert.equal(recordedMigrations(mislabeled), null);
  Assert.equal(preflight(mislabeled).status, "blocked");
  Assert.equal(hash(await FS.readFile(mislabeled)), sqliteHash);
  checks.push(
    "the original read-only SQLite path remains available; a native-path failure never retries SQLite",
  );

  const damaged = Path.join(fixture.root, "damaged.omdb");
  await FS.writeFile(damaged, "not a native database");
  Assert.equal(databaseMigrationHead(damaged), undefined);
  Assert.equal(preflight(damaged).status, "blocked");
  Assert.equal(await FS.readFile(damaged, "utf8"), "not a native database");
  process.env.T3_CODE_LS_DB_EXECUTABLE = Path.join(fixture.root, "missing-helper");
  Assert.equal(recordedMigrations(fixture.databasePath), null);
  Assert.equal(preflight(fixture.databasePath).status, "blocked");
  delete process.env.T3_CODE_LS_DB_EXECUTABLE;
  process.env.T3_CODE_LS_IMPLEMENTATION = "upstream";
  Assert.equal(recordedMigrations(fixture.databasePath), null);
  Assert.equal(recordedMigrations(sqlite).length, migrationManifest.length);
  checks.push(
    "corruption, unavailable helpers and explicitly unqualified native selection fail closed while legacy SQLite stays readable",
  );

  await finishProof({
    executable,
    fixture,
    proofPath,
    checks,
    sourceHash: baseline,
    oracles: [],
    sourcePaths: [
      "apps/server/src/persistence/databaseMigrationHead.ts",
      "apps/server/src/persistence/redb_bootstrap.rs",
      "apps/server/src/orchestration-v2/NativeDatabase.ts",
      "crates/storage/src/backend.rs",
      "crates/storage/src/lib.rs",
      "crates/storage/src/bin/t3-code-ls-db.rs",
      "packages/shared/src/applicationDatabase.ts",
      "tools/check-omdb-migration-head.mjs",
    ],
  });
} finally {
  if (prior.executable === undefined) delete process.env.T3_CODE_LS_DB_EXECUTABLE;
  else process.env.T3_CODE_LS_DB_EXECUTABLE = prior.executable;
  if (prior.implementation === undefined) delete process.env.T3_CODE_LS_IMPLEMENTATION;
  else process.env.T3_CODE_LS_IMPLEMENTATION = prior.implementation;
  await fixture.close();
}
