#!/usr/bin/env node
import * as Assert from "node:assert/strict";
import * as FS from "node:fs/promises";
import * as Path from "node:path";
import * as Sqlite from "node:sqlite";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Option from "effect/Option";
import * as SecureRandom from "effect/crypto/SecureRandom";

import * as NativeReads from "../apps/server/src/persistence/NativeAuxiliaryPersistenceReads.ts";
import * as SqliteReads from "../apps/server/src/persistence/SqliteAuxiliaryPersistenceReads.ts";
import * as NativeRuntime from "../apps/server/src/persistence/NativeProviderSessionRuntime.ts";
import * as Runtime from "../apps/server/src/persistence/ProviderSessionRuntime.ts";
import * as EffectOutbox from "../apps/server/src/orchestration-v2/EffectOutbox.ts";
import * as NativeOutbox from "../apps/server/src/orchestration-v2/NativeApplicationEffectOutbox.ts";
import { createFixture } from "./database-fixture.mjs";
import { exportSqliteState } from "./export-sqlite-state.mjs";
import { jsonPayloads } from "./redb-event-sink-fixtures.mjs";
import { upstreamSource } from "./upstream-source.mjs";
import { at } from "./check-outbox-runtime.mjs";
import {
  createMigratedSqlite,
  finishProof,
  hash,
  nativeRequest,
  normalized,
  withNative,
  withSqlite,
} from "./redb-domain-fixture.mjs";

const [executable, proofPath] = process.argv.slice(2);
Assert.ok(executable && Path.isAbsolute(executable), "Pass an absolute native database executable");
await FS.access(executable, FS.constants.X_OK);
const fixture = await createFixture();
const checks = [];
const now = "2026-11-10T12:00:00.000Z";
const future = "2999-01-01T00:00:00.000Z";
const settingsSource = upstreamSource("apps/server/src/serverSettings.ts");
const cleanupSource = upstreamSource("apps/server/src/storageCleanup.ts");
const sqlFromSource = (source, needle) => {
  const index = source.indexOf(needle);
  Assert.ok(index >= 0, `Immutable SQL oracle contains ${needle}`);
  const start = source.lastIndexOf("`", index);
  const end = source.indexOf("`", index);
  Assert.ok(start >= 0 && end > start);
  return source.slice(start + 1, end);
};
const queries = {
  providerHistory: sqlFromSource(settingsSource, "FROM projection_thread_sessions"),
  legacyProjectSettings: sqlFromSource(settingsSource, "FROM projection_projects"),
  allThreadWorktrees: sqlFromSource(cleanupSource, "SELECT DISTINCT json_extract"),
  deletedThreadWorktrees: sqlFromSource(cleanupSource, "SELECT t.payload_json"),
  pendingThreadCleanup: sqlFromSource(
    cleanupSource,
    "SELECT status FROM orchestration_v2_effect_outbox",
  ).replace("${thread.id}", "?"),
  nonStoppedProviderSessions: sqlFromSource(
    cleanupSource,
    "SELECT payload_json FROM orchestration_v2_projection_provider_sessions",
  ),
};
const sort = (rows) =>
  normalized(rows).sort((a, b) => JSON.stringify(a).localeCompare(JSON.stringify(b)));
const normalizeSnapshot = (snapshot) =>
  Object.fromEntries(Object.entries(snapshot).map(([key, value]) => [key, sort(value)]));
const snapshot = (rows) =>
  Effect.gen(function* () {
    return normalizeSnapshot({
      providerHistory: yield* rows.providerHistory,
      legacyProjectSettings: yield* rows.legacyProjectSettings,
      allThreadWorktrees: yield* rows.allThreadWorktrees,
      deletedThreadWorktrees: yield* rows.deletedThreadWorktrees,
      pendingThreadCleanup: yield* rows.pendingThreadCleanup("thread:cleanup"),
      nonStoppedProviderSessions: yield* rows.nonStoppedProviderSessions,
    });
  });
const failure = (body) =>
  Effect.exit(body).pipe(Effect.tap((exit) => Effect.sync(() => Assert.ok(Exit.isFailure(exit)))));
const blockers = (rows, threadId) =>
  rows.pendingThreadCleanup(threadId).pipe(Effect.map(normalized));
const runtimeRow = (threadId, providerName = "cursor") => ({
  threadId,
  providerName,
  providerInstanceId: `${providerName}-new`,
  adapterKey: providerName,
  runtimeMode: "full-access",
  status: "running",
  lastSeenAt: now,
  resumeCursor: null,
  runtimePayload: null,
});
const pending = (id) => ({
  id,
  commandId: "command:auxiliary",
  threadId: "thread:written",
  request: { type: "terminal.cleanup" },
});

async function services(kind, filename, use) {
  if (kind === "native")
    return withNative(
      executable,
      fixture,
      (client) =>
        Effect.gen(function* () {
          const rows = yield* NativeReads.make;
          const runtime = yield* NativeRuntime.make;
          const outbox = yield* EffectOutbox.EffectOutboxV2.pipe(
            Effect.provide(NativeOutbox.layer),
          );
          return yield* use({
            rows,
            runtime,
            outbox,
            transaction: (body) => client.withTransaction(body),
          });
        }).pipe(Effect.provide(SecureRandom.layerWebCrypto)),
      (database) => ({
        ...database,
        request: (input, response) =>
          database
            .request(input, response)
            .pipe(
              Effect.tapError(() =>
                Effect.sync(() =>
                  console.error(`Auxiliary fixture request failed: ${input.operation}`),
                ),
              ),
            ),
      }),
    );
  return withSqlite(filename, (sql) =>
    Effect.gen(function* () {
      const rows = yield* SqliteReads.make;
      const runtime = yield* Runtime.make;
      const outbox = yield* EffectOutbox.EffectOutboxV2.pipe(Effect.provide(EffectOutbox.layer));
      return yield* use({
        rows,
        runtime,
        outbox,
        transaction: (body) => sql.withTransaction(body),
      });
    }).pipe(Effect.provide(SecureRandom.layerWebCrypto)),
  );
}

function exercise({ rows, runtime, outbox, transaction }) {
  return at(
    now,
    Effect.gen(function* () {
      const result = { initial: yield* snapshot(rows) };
      Assert.equal(result.initial.pendingThreadCleanup.length, 81);
      Assert.ok(result.initial.pendingThreadCleanup.some((row) => row.status === "failed"));
      Assert.deepEqual(yield* blockers(rows, "missing"), []);
      yield* transaction(
        Effect.gen(function* () {
          yield* runtime.upsert(runtimeRow("runtime:written"));
          yield* outbox.enqueue([pending("effect:written")]);
          result.writtenHistory = yield* rows.providerHistory;
          Assert.ok(result.writtenHistory.some((row) => row.providerInstanceId === "cursor-new"));
          Assert.deepEqual(yield* blockers(rows, "thread:written"), [{ status: "pending" }]);
          yield* failure(
            transaction(
              Effect.gen(function* () {
                yield* runtime.upsert(runtimeRow("runtime:written", "grok"));
                yield* outbox.cancelUnsettled({
                  threadId: "thread:written",
                  effectTypes: ["terminal.cleanup"],
                  reason: "discard",
                });
                Assert.deepEqual(yield* blockers(rows, "thread:written"), []);
                return yield* Effect.fail("nested auxiliary rollback");
              }),
            ),
          );
          Assert.deepEqual(yield* rows.providerHistory, result.writtenHistory);
          Assert.deepEqual(yield* blockers(rows, "thread:written"), [{ status: "pending" }]);
        }),
      );
      const claim = yield* outbox.claimNext({ workerId: "worker:auxiliary" });
      Assert.ok(Option.isSome(claim));
      Assert.equal(claim.value.id, "effect:written");
      Assert.deepEqual(yield* blockers(rows, "thread:written"), [{ status: "running" }]);
      Assert.equal(
        yield* outbox.fail({
          effectId: "effect:written",
          workerId: "worker:auxiliary",
          error: "preserved blocker",
        }),
        true,
      );
      Assert.deepEqual(yield* blockers(rows, "thread:written"), [{ status: "failed" }]);
      yield* outbox.enqueue([pending("effect:succeeded")]);
      const successClaim = yield* outbox.claimNext({
        workerId: "worker:auxiliary",
      });
      Assert.equal(Option.getOrThrow(successClaim).id, "effect:succeeded");
      Assert.equal(
        yield* outbox.succeed({
          effectId: "effect:succeeded",
          workerId: "worker:auxiliary",
        }),
        true,
      );
      Assert.deepEqual(yield* blockers(rows, "thread:written"), [{ status: "failed" }]);
      yield* failure(
        transaction(
          Effect.gen(function* () {
            yield* runtime.deleteByThreadId({ threadId: "runtime:written" });
            yield* outbox.enqueue([pending("effect:discard")]);
            return yield* Effect.die("outer auxiliary rollback");
          }),
        ),
      );
      Assert.deepEqual(yield* rows.providerHistory, result.writtenHistory);
      Assert.deepEqual(yield* blockers(rows, "thread:written"), [{ status: "failed" }]);
      yield* runtime.deleteByThreadId({ threadId: "runtime:written" });
      result.final = yield* snapshot(rows);
      Assert.deepEqual(result.final, result.initial);
      return normalized(result);
    }),
  );
}

try {
  const source = Path.join(fixture.root, "source.sqlite");
  const retained = Path.join(fixture.root, "retained.sqlite");
  await createMigratedSqlite(source);
  const db = new Sqlite.DatabaseSync(source);
  let oracle;
  try {
    const project = db.prepare(`INSERT INTO projection_projects
      (project_id,title,workspace_root,default_model_selection_json,default_thread_env_mode,auto_pull,scripts_json,created_at,updated_at,deleted_at)
      VALUES (?,? ,?,?,?,?,'[]',?,?,?)`);
    project.run(
      "project:a",
      "A",
      "/fixture/a",
      '{ "instanceId": "codex", "model": "model-a" }',
      "worktree",
      1,
      now,
      now,
      null,
    );
    project.run(
      "project:deleted",
      "Deleted",
      "/fixture/deleted-project",
      null,
      null,
      0,
      now,
      now,
      now,
    );
    const thread = db.prepare(`INSERT INTO orchestration_v2_projection_threads
      (thread_id,project_id,title,default_provider,runtime_mode,interaction_mode,created_at,updated_at,deleted_at,payload_json)
      VALUES (?,?,'Fixture','codex','full-access','default',?,?,?,?)`);
    for (const [id, projectId, worktreePath, deletedAt] of [
      ["thread:a", "project:a", "/fixture/worktree", null],
      ["thread:duplicate", "project:a", "/fixture/worktree", null],
      ["thread:null", "project:a", null, null],
      ["thread:deleted", "project:a", "/fixture/deleted", now],
      ["thread:deleted-project", "project:deleted", "/fixture/deleted-project/worktree", now],
      ["thread:orphan", "project:absent", "/fixture/orphan", now],
    ])
      thread.run(
        id,
        projectId,
        now,
        now,
        deletedAt,
        JSON.stringify({
          ...jsonPayloads.thread,
          id,
          projectId,
          worktreePath,
          deletedAt,
        }),
      );
    const session = db.prepare(`INSERT INTO orchestration_v2_projection_provider_sessions
      (provider_session_id,thread_id,provider,status,model,updated_at,payload_json) VALUES (?,NULL,'codex',?,'fixture',?,?)`);
    for (const [id, status] of [
      ["session:ready", "ready"],
      ["session:error", "error"],
      ["session:stopped", "stopped"],
    ]) {
      session.run(id, status, now, JSON.stringify({ ...jsonPayloads.providerSession, id, status }));
    }
    const legacy =
      db.prepare(`INSERT INTO projection_thread_sessions (thread_id,status,provider_name,provider_instance_id,updated_at)
      VALUES (?,'stopped',?,?,?)`);
    for (const [id, name, instance] of [
      ["history:cursor", "cursor", null],
      ["history:grok", "grok", ""],
      ["history:excluded", "codex", "codex"],
    ]) {
      legacy.run(id, name, instance, now);
    }
    const runtime = db.prepare(`INSERT INTO provider_session_runtime
      (thread_id,provider_name,provider_instance_id,adapter_key,runtime_mode,status,last_seen_at,resume_cursor_json,runtime_payload_json)
      VALUES (?,?,?,'fixture','full-access','stopped',?,NULL,'{')`);
    for (let index = 0; index < 135; index++)
      runtime.run(
        `history:${index}`,
        index % 2 ? "cursor" : "opencode",
        index % 3 ? null : "opencode:retained",
        now,
      );
    const outbox = db.prepare(`INSERT INTO orchestration_v2_effect_outbox
      (effect_id,command_id,thread_id,effect_type,payload_json,status,attempt_count,available_at,lease_owner,lease_expires_at,created_at,updated_at,completed_at,last_error)
      VALUES (?,'command:seed',?,'terminal.cleanup','{"type":"terminal.cleanup"}',?,2,?,?,NULL,?,?,?,NULL)`);
    for (let index = 0; index < 135; index++) {
      const status = ["pending", "running", "failed", "succeeded", "cancelled"][index % 5];
      outbox.run(
        `effect:${index}`,
        "thread:cleanup",
        status,
        future,
        status === "running" ? "worker:seed" : null,
        now,
        now,
        ["pending", "running"].includes(status) ? null : now,
      );
    }
    outbox.run("effect:other", "thread:other", "pending", future, null, now, now, null);
    oracle = normalizeSnapshot(
      Object.fromEntries(
        Object.entries(queries).map(([name, query]) => [
          name,
          name === "pendingThreadCleanup"
            ? db.prepare(query).all("thread:cleanup")
            : db.prepare(query).all(),
        ]),
      ),
    );
    Assert.equal(
      oracle.deletedThreadWorktrees.length,
      2,
      "Join includes deleted projects and excludes orphan projects",
    );
    Assert.equal(
      oracle.allThreadWorktrees.length,
      4,
      "Referenced worktrees include deleted and orphan threads",
    );
    Assert.equal(oracle.nonStoppedProviderSessions.length, 2);
  } finally {
    db.close();
  }
  const sourceHash = hash(await FS.readFile(source));
  await FS.copyFile(source, retained);
  const exported = Path.join(fixture.root, "source.ndjson");
  exportSqliteState({ source, destination: exported });
  nativeRequest(executable, fixture, "initialize", {
    operation: "import_sqlite_application",
    path: exported,
  });
  const expected = await services("sqlite", retained, exercise);
  const actual = await services("native", source, exercise);
  Assert.deepEqual(
    expected.initial,
    oracle,
    "Extracted SQLite boundary matches immutable upstream SQL",
  );
  Assert.deepEqual(
    actual,
    expected,
    "Native reads match retained SQL before, during and after writer transactions",
  );
  checks.push(
    "Six settings/cleanup queries match immutable upstream SQL, including provider UNION nulls/duplicates, legacy values, deleted-project joins, orphan exclusion and shared live sessions",
  );
  checks.push(
    "Compact history and cleanup indexes backfill across pages; history does not parse unrelated malformed runtime payloads",
  );
  checks.push(
    "Real provider/outbox service writes update auxiliary reads transactionally; nested/root rollback restores indexes and failed effects continue blocking cleanup while succeeded/cancelled effects do not",
  );
  await services("native", source, ({ rows }) =>
    Effect.gen(function* () {
      Assert.deepEqual(yield* snapshot(rows), expected.final);
      Assert.deepEqual(yield* blockers(rows, "thread:written"), [{ status: "failed" }]);
    }),
  );
  checks.push("Restart preserves prepared indexes and never writes the source SQLite file");
  Assert.equal(hash(await FS.readFile(source)), sourceHash);
  await finishProof({
    executable,
    fixture,
    proofPath,
    checks,
    sourceHash,
    oracles: [
      {
        path: "apps/server/src/serverSettings.ts",
        sha256: hash(settingsSource),
      },
      {
        path: "apps/server/src/storageCleanup.ts",
        sha256: hash(cleanupSource),
      },
    ],
    sourcePaths: [
      "apps/server/src/serverSettings.ts",
      "apps/server/src/storageCleanup.ts",
      "apps/server/src/persistence/AuxiliaryPersistenceReads.ts",
      "apps/server/src/persistence/SqliteAuxiliaryPersistenceReads.ts",
      "apps/server/src/persistence/NativeAuxiliaryPersistenceReads.ts",
      "apps/server/src/persistence/redb_auxiliary_reads.rs",
      "apps/server/src/orchestration-v2/redb_effect_outbox.rs",
      "tools/check-redb-auxiliary-reads.mjs",
    ],
  });
} finally {
  await fixture.close();
}
