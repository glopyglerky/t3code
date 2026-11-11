#!/usr/bin/env node
import * as Assert from "node:assert/strict";
import * as FS from "node:fs/promises";
import * as Path from "node:path";
import * as Sqlite from "node:sqlite";
import * as Contracts from "@t3tools/contracts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import { isLocalLoopbackHost, isTailnetHost } from "@t3tools/shared/hostClassification";
import * as Context from "effect/Context";
import * as Digest from "effect/crypto/Digest";
import * as SecureRandom from "effect/crypto/SecureRandom";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Base64Url from "effect/encoding/Base64Url";
import * as Hex from "effect/encoding/Hex";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Predicate from "effect/Predicate";
import * as Redacted from "effect/Redacted";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as FetchHttpClient from "effect/http/FetchHttpClient";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpApi from "effect/http-api/HttpApi";
import * as HttpApiClient from "effect/http-api/HttpApiClient";

import * as PeerLinks from "../apps/server/src/peer/PeerLinks.ts";
import * as PeerLinkRows from "../apps/server/src/peer/PeerLinkRows.ts";
import * as NativePeerLinkRows from "../apps/server/src/peer/NativePeerLinkRows.ts";
import * as SqlitePeerLinkRows from "../apps/server/src/peer/SqlitePeerLinkRows.ts";
import * as ServerSecretStore from "../apps/server/src/auth/ServerSecretStore.ts";
import * as ServerEnvironment from "../apps/server/src/environment/ServerEnvironment.ts";
import * as ServerSettings from "../apps/server/src/serverSettings.ts";
import * as KnownWorkspaceRows from "../apps/server/src/workspace/KnownWorkspaceRows.ts";
import * as NativeKnownWorkspaceRows from "../apps/server/src/workspace/NativeKnownWorkspaceRows.ts";
import { createFixture } from "./database-fixture.mjs";
import { exportSqliteState } from "./export-sqlite-state.mjs";
import {
  createMigratedSqlite,
  finishProof,
  hash,
  loadUpstream,
  nativeRequest,
  normalized,
  withNative,
  withSqlite,
} from "./redb-domain-fixture.mjs";

const [executable, proofPath] = process.argv.slice(2);
Assert.ok(executable && Path.isAbsolute(executable), "Pass an absolute native database executable");
await FS.access(executable, FS.constants.X_OK);
const oracle = loadUpstream(
  "apps/server/src/peer/PeerLinks.ts",
  {
    ...Contracts,
    NodeSqliteClient,
    Context,
    Digest,
    SecureRandom,
    DateTime,
    Duration,
    Effect,
    Base64Url,
    Hex,
    Layer,
    Option,
    Predicate,
    Redacted,
    Result,
    Schema,
    SqlClient,
    FetchHttpClient,
    HttpClient,
    HttpApi,
    HttpApiClient,
    isLocalLoopbackHost,
    isTailnetHost,
    ServerSecretStore,
    ServerEnvironment,
    ServerSettings,
  },
  ["make"],
);
const fixture = await createFixture();
const checks = [];
const at = "2020-01-01T00:00:00.000Z";
const later = "2020-01-02T00:00:00.000Z";
const written = (id, label = id) => ({
  environment_id: id,
  secret_name: `secret:${id}`,
  label,
  urls_json: "[]",
  access: "auto",
  linked_at: at,
  expires_at: later,
  last_reached_at: null,
  last_error: null,
  fenced: 1,
});
const failure = (effect) =>
  Effect.exit(effect).pipe(
    Effect.tap((exit) => Effect.sync(() => Assert.ok(Exit.isFailure(exit)))),
  );

function exerciseRows({ rows, transaction }) {
  return Effect.gen(function* () {
    const result = { initial: yield* rows.list };
    Assert.equal(result.initial.length, 304);
    Assert.deepEqual(
      result.initial.slice(0, 4).map((row) => row.environment_id),
      ["peer:empty", "peer:nul", "peer:a", "peer:b"],
    );
    Assert.equal(yield* rows.get("missing"), null);
    yield* transaction(
      Effect.gen(function* () {
        yield* rows.upsert(written("written", "!"));
        yield* rows.recordOutcome("written", null, at);
        yield* rows.recordOutcome("written", "unreachable", "ignored");
        result.error = yield* rows.get("written");
        Assert.equal(result.error.last_reached_at, at);
        Assert.equal(result.error.last_error, "unreachable");
        yield* rows.recordOutcome("written", null, later);
        result.success = yield* rows.get("written");
        Assert.equal(result.success.last_error, null);
        Assert.equal(result.success.last_reached_at, later);
        yield* failure(
          transaction(
            Effect.gen(function* () {
              yield* rows.upsert({
                ...written("written", "discard"),
                secret_name: "discard-secret",
              });
              yield* rows.remove("peer:a");
              return yield* Effect.fail("nested rollback");
            }),
          ),
        );
        Assert.deepEqual(yield* rows.get("written"), result.success);
        Assert.notEqual(yield* rows.get("peer:a"), null);
        yield* rows.relabel("written", "z-last");
        yield* rows.relabel("missing", "ignored");
        yield* rows.recordOutcome("missing", "ignored", "ignored");
        Assert.equal(yield* rows.remove("peer:b"), "secret:peer:b");
        Assert.equal(yield* rows.remove("peer:b"), null);
      }),
    );
    yield* failure(
      transaction(
        Effect.gen(function* () {
          yield* rows.upsert(written("root-discard"));
          yield* rows.remove("written");
          return yield* Effect.die("root rollback");
        }),
      ),
    );
    Assert.equal(yield* rows.get("root-discard"), null);
    result.final = yield* rows.list;
    Assert.equal(result.final.at(-1).environment_id, "written");
    return normalized(result);
  });
}

async function rowsService(kind, filename, use) {
  if (kind === "native")
    return withNative(executable, fixture, (client) =>
      Effect.gen(function* () {
        const rows = yield* NativePeerLinkRows.make;
        return yield* use({ rows, transaction: (effect) => client.withTransaction(effect) });
      }),
    );
  return withSqlite(filename, (sql) =>
    Effect.gen(function* () {
      const rows = yield* SqlitePeerLinkRows.make;
      return yield* use({ rows, transaction: (effect) => sql.withTransaction(effect) });
    }),
  );
}

function exerciseService(make) {
  const removed = [];
  return Effect.gen(function* () {
    const links = yield* make;
    const result = { list: yield* links.list };
    Assert.ok(result.list.every((link) => link.status === "expired"));
    Assert.ok(Option.isNone(yield* links.get("missing")));
    result.get = yield* links.get("peer:a");
    Assert.ok(Option.isSome(result.get));
    Assert.equal(
      DateTime.formatIso(Option.getOrThrow(yield* links.get("peer:empty")).expiresAt),
      at,
    );
    yield* links.recordOutcome("peer:a", { error: "offline" });
    result.outcome = yield* links.get("peer:a");
    Assert.equal(result.outcome.value.lastError, "offline");
    result.removed = yield* links.unlink("peer:a");
    Assert.equal(result.removed, true);
    Assert.equal(yield* links.unlink("peer:a"), false);
    Assert.deepEqual(removed, ["secret:peer:a"]);
    Assert.ok(Option.isNone(yield* links.get("peer:a")));
    return normalized(result);
  }).pipe(
    Effect.provide(NodeCrypto.layer),
    Effect.provide(FetchHttpClient.layer),
    Effect.provideService(ServerSecretStore.ServerSecretStore, {
      get: () => Effect.succeedNone,
      set: () => Effect.void,
      getOrCreateRandom: () => Effect.die("Unexpected credential operation"),
      remove: (name) =>
        Effect.sync(() => {
          removed.push(name);
        }),
    }),
    Effect.provideService(ServerEnvironment.ServerEnvironment, {
      getEnvironmentId: Effect.succeed("fixture:self"),
      getDescriptor: Effect.die("Unexpected peer network operation"),
    }),
    Effect.provideService(ServerSettings.ServerSettingsService, {
      getSettings: Effect.succeed({ linkedEnvironmentsEnabled: true }),
    }),
  );
}

try {
  const source = Path.join(fixture.root, "source.sqlite");
  const sqlitePath = Path.join(fixture.root, "retained.sqlite");
  const upstreamPath = Path.join(fixture.root, "upstream.sqlite");
  await createMigratedSqlite(source);
  const database = new Sqlite.DatabaseSync(source);
  try {
    const insert = database.prepare(`INSERT INTO peer_environment_links
      (environment_id,secret_name,label,urls_json,access,linked_at,expires_at,last_reached_at,last_error,fenced)
      VALUES (?,?,?,?,?,?,?,?,?,?)`);
    for (const [id, label] of [
      ["peer:empty", ""],
      ["peer:nul", "\u0000"],
      ["peer:a", "A"],
      ["peer:b", "A"],
    ]) {
      const row = written(id, label);
      if (id === "peer:empty") {
        row.fenced = 0;
        row.expires_at = "2999-01-01T00:00:00.000Z";
      }
      insert.run(...Object.values(row));
    }
    for (let i = 0; i < 300; i++) {
      const row = written(`many:${String(i).padStart(3, "0")}`, "Many");
      insert.run(...Object.values(row));
    }
    const insertThread = database.prepare(`INSERT INTO orchestration_v2_projection_threads
      (thread_id,project_id,title,default_provider,runtime_mode,interaction_mode,created_at,updated_at,deleted_at,payload_json)
      VALUES (?,?,'Fixture','codex','full-access','default',?,?,?,?)`);
    for (const [id, project, path, deleted] of [
      ["worktree:a", "project:a", "/fixture/a", null],
      ["worktree:duplicate", "project:a", "/fixture/a", null],
      ["worktree:b", "project:b", "/fixture/a", null],
      ["worktree:gone", "project:a", "/fixture/gone", later],
      ["worktree:null", "project:a", null, null],
    ])
      insertThread.run(id, project, at, at, deleted, JSON.stringify({ worktreePath: path }));
  } finally {
    database.close();
  }
  const sourceHash = hash(await FS.readFile(source));
  await FS.copyFile(source, sqlitePath);
  await FS.copyFile(source, upstreamPath);
  const exported = Path.join(fixture.root, "source.ndjson");
  exportSqliteState({ source, destination: exported });
  nativeRequest(executable, fixture, "initialize", {
    operation: "import_sqlite_application",
    path: exported,
  });
  const expected = await rowsService("sqlite", sqlitePath, exerciseRows);
  const actual = await rowsService("native", source, exerciseRows);
  Assert.deepEqual(
    actual,
    expected,
    "Native peer rows must match retained SQLite on every operation",
  );
  checks.push(
    "All peer row methods preserve imported values, BINARY label/id order across pages, conditional timestamps, relabeling, deletion and missing-row no-ops",
  );
  checks.push(
    "Root and nested rollback restore peer rows, secret references and ordered indexes together",
  );
  await rowsService("native", source, ({ rows }) =>
    Effect.gen(function* () {
      Assert.deepEqual(normalized(yield* rows.list), expected.final);
    }),
  );
  // Service comparisons start from the exact same post-mutation state.
  await FS.copyFile(sqlitePath, upstreamPath);
  const serviceExpected = await withSqlite(upstreamPath, () => exerciseService(oracle.module.make));
  const serviceRetained = await withSqlite(sqlitePath, () =>
    exerciseService(PeerLinks.make.pipe(Effect.provide(SqlitePeerLinkRows.layer))),
  );
  const serviceActual = await withNative(executable, fixture, () =>
    exerciseService(PeerLinks.make.pipe(Effect.provide(NativePeerLinkRows.layer))),
  );
  Assert.deepEqual(
    serviceRetained,
    serviceExpected,
    "Retained shared PeerLinks must match immutable upstream",
  );
  Assert.deepEqual(
    serviceActual,
    serviceExpected,
    "Native shared PeerLinks must match immutable upstream",
  );
  checks.push(
    "Actual PeerLinks list/get/unlink/outcome service matches immutable upstream, including fenced expiry and credential removal",
  );
  const workspaceExpected = await withSqlite(source, () =>
    Effect.gen(function* () {
      const context = yield* Layer.build(KnownWorkspaceRows.layerSqlite);
      return yield* Context.get(context, KnownWorkspaceRows.KnownWorkspaceRows).threadWorktrees;
    }),
  );
  const workspaceActual = await withNative(executable, fixture, () =>
    Effect.gen(function* () {
      const rows = yield* NativeKnownWorkspaceRows.make;
      return yield* rows.threadWorktrees;
    }),
  );
  const sort = (rows) =>
    [...rows].sort((a, b) => JSON.stringify(a).localeCompare(JSON.stringify(b)));
  Assert.deepEqual(sort(normalized(workspaceActual)), sort(normalized(workspaceExpected)));
  Assert.deepEqual(sort(workspaceActual), [
    { worktreePath: "/fixture/a", projectId: "project:a" },
    { worktreePath: "/fixture/a", projectId: "project:b" },
  ]);
  checks.push(
    "Native workspace discovery preserves distinct project/worktree pairs and excludes null/deleted thread paths",
  );
  Assert.equal(hash(await FS.readFile(source)), sourceHash);
  await finishProof({
    executable,
    fixture,
    proofPath,
    checks,
    sourceHash,
    oracles: [oracle.source],
    sourcePaths: [
      "apps/server/src/peer/PeerLinks.ts",
      "apps/server/src/peer/PeerLinkRows.ts",
      "apps/server/src/peer/SqlitePeerLinkRows.ts",
      "apps/server/src/peer/NativePeerLinkRows.ts",
      "apps/server/src/peer/redb_peer_rows.rs",
      "apps/server/src/workspace/KnownWorkspaces.ts",
      "apps/server/src/workspace/KnownWorkspaceRows.ts",
      "apps/server/src/workspace/NativeKnownWorkspaceRows.ts",
      "tools/check-redb-peers.mjs",
      "tools/redb-domain-fixture.mjs",
    ],
  });
} finally {
  await fixture.close();
}
