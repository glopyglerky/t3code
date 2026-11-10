import * as Assert from "node:assert/strict";
import * as ChildProcess from "node:child_process";
import * as Crypto from "node:crypto";
import * as FS from "node:fs/promises";
import * as Module from "node:module";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as Logger from "effect/Logger";
import * as SqlClient from "effect/sql/SqlClient";

import * as NativeDatabase from "../apps/server/src/orchestration-v2/NativeDatabase.ts";
import * as RedbClient from "../apps/server/src/persistence/RedbClient.ts";
import { runMigrations } from "../apps/server/src/persistence/Migrations.ts";
import { upstreamSource } from "./upstream-source.mjs";

export const hash = (bytes) => Crypto.createHash("sha256").update(bytes).digest("hex");
export const normalized = (value) => JSON.parse(JSON.stringify(value));

export function nativeRequest(executable, fixture, mode, request) {
  const output = ChildProcess.execFileSync(executable, fixture.cliArgs(mode), {
    input: JSON.stringify(request) + "\n", env: {},
    encoding: "utf8", maxBuffer: 32 * 1024 * 1024, timeout: 30_000,
  });
  const response = JSON.parse(output.trim());
  Assert.equal(response.ok, true, JSON.stringify(response));
  return response.result;
}

export function loadUpstream(path, imports, exports) {
  const original = upstreamSource(path);
  const source = Module.stripTypeScriptTypes(original, { mode: "strip" })
    .replace(/^import\s[\s\S]*?;\s*$/gm, "")
    .replace(/^export (?=(?:function|const|class) )/gm, "");
  return {
    module: new Function(...Object.keys(imports), `${source}\nreturn { ${exports.join(", ")} };`)(...Object.values(imports)),
    source: { path, sha256: hash(original) },
  };
}

export async function withNative(executable, fixture, use, wrap = (database) => database) {
  return Effect.runPromise(Effect.gen(function* () {
    const database = yield* NativeDatabase.make({ executable, databasePath: fixture.databasePath });
    const client = yield* RedbClient.make.pipe(Effect.provideService(NativeDatabase.NativeDatabase, wrap(database)));
    return yield* use(client, database).pipe(Effect.provideService(RedbClient.RedbClient, client));
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer), Effect.provide(Logger.layer([]))));
}

export async function withSqlite(filename, use) {
  return Effect.runPromise(Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    return yield* use(sql);
  }).pipe(Effect.scoped, Effect.provide(NodeSqliteClient.layer({ filename })),
    Effect.provide(NodeServices.layer), Effect.provide(Logger.layer([]))));
}

/** Create the application's complete migrated schema in an owned test file. */
export async function createMigratedSqlite(filename, seed = () => Effect.void) {
  return withSqlite(filename, (sql) => Effect.gen(function* () {
    yield* runMigrations();
    yield* seed(sql);
    yield* sql`PRAGMA wal_checkpoint(TRUNCATE)`;
  }));
}

export async function finishProof({ executable, fixture, proofPath, checks, oracles, sourceHash, sourcePaths }) {
  const upstream = JSON.parse(await FS.readFile(new URL("../upstream.lock.json", import.meta.url), "utf8"));
  const sources = {};
  for (const path of sourcePaths) sources[path] = hash(await FS.readFile(new URL(`../${path}`, import.meta.url)));
  const proof = {
    schema: 1, engine: fixture.engineLabel, checks,
    oracle: { commit: upstream.upstream.commit, sources: oracles },
    executable_sha256: hash(await FS.readFile(executable)), sqlite_source_sha256: sourceHash,
    sources,
  };
  if (proofPath) await FS.writeFile(proofPath, JSON.stringify(proof, null, 2) + "\n");
  console.log(JSON.stringify(proof, null, 2));
}
