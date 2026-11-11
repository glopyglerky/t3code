#!/usr/bin/env node
import * as Assert from "node:assert/strict";
import * as FS from "node:fs/promises";
import * as Path from "node:path";
import * as Sqlite from "node:sqlite";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";

import * as Usage from "../apps/server/src/usage/CodexUsageHistory.ts";
import * as NativeUsage from "../apps/server/src/usage/NativeCodexUsageHistory.ts";
import * as Links from "../apps/server/src/pullRequest/linkedThreads.ts";
import * as NativeLinks from "../apps/server/src/pullRequest/NativeLinkedThreads.ts";
import * as ProjectionStore from "../apps/server/src/orchestration-v2/ProjectionStore.ts";
import * as NativeProjection from "../apps/server/src/persistence/NativeProjectionCommitStore.ts";
import { jsonPayloads, makeEvent, fixedTime } from "./redb-event-sink-fixtures.mjs";
import { createFixture } from "./database-fixture.mjs";
import { exportSqliteState } from "./export-sqlite-state.mjs";
import {
  createMigratedSqlite,
  finishProof,
  hash,
  loadUpstreamModule,
  nativeRequest,
  normalized,
  withNative,
  withSqlite,
} from "./redb-domain-fixture.mjs";

const [executable, proofPath] = process.argv.slice(2);
Assert.ok(executable && Path.isAbsolute(executable), "Pass an absolute native database executable");
await FS.access(executable, FS.constants.X_OK);
const usageOracle = await loadUpstreamModule("apps/server/src/usage/CodexUsageHistory.ts");
const linksOracle = await loadUpstreamModule("apps/server/src/pullRequest/linkedThreads.ts");
const fixture = await createFixture();
const corruptionFixtures = [];
const checks = [];
const model = "fixture-model";
const selection = (tier) => ({
  instanceId: "codex",
  model,
  ...(tier === undefined ? {} : { options: [{ id: "serviceTier", value: tier }] }),
});
const request = (nativeTurnId, sessionId = "native-session", modelName = model) => ({
  nativeTurnId,
  sessionId,
  model: modelName,
});
const key = { host: "github.com", repository: "acme/web", number: 7 };
const pr = (values = {}) => ({
  ...key,
  url: "https://github.com/acme/web/pull/7",
  source: "manual",
  linkedAt: fixedTime,
  snapshot: null,
  stack: null,
  ...values,
});
const thread = (id, values = {}) => ({
  ...jsonPayloads.thread,
  id,
  title: id,
  pullRequests: [pr()],
  ...values,
});
const failed = (body) =>
  Effect.exit(body).pipe(Effect.tap((exit) => Effect.sync(() => Assert.ok(Exit.isFailure(exit)))));

async function services(kind, filename, use, target = fixture) {
  if (kind === "redb")
    return withNative(executable, target, (client) =>
      Effect.gen(function* () {
        const usage = yield* NativeUsage.make;
        const links = yield* NativeLinks.make;
        const projection = yield* NativeProjection.make;
        return yield* use({
          usage,
          links,
          apply: projection.apply,
          transaction: client.withTransaction,
        });
      }),
    );
  return withSqlite(filename, (sql) =>
    Effect.gen(function* () {
      const usage = yield* kind === "upstream"
        ? usageOracle.module.CodexUsageHistory.pipe(Effect.provide(usageOracle.module.layer))
        : Usage.make;
      const links =
        kind === "upstream"
          ? { list: linksOracle.module.listLinkedPullRequestThreads }
          : yield* Links.makeSqlite;
      const projection = yield* ProjectionStore.ProjectionStoreV2.pipe(
        Effect.provide(ProjectionStore.layer),
      );
      return yield* use({
        usage,
        links,
        apply: projection.apply,
        transaction: (body) => sql.withTransaction(body),
      });
    }),
  );
}

function exercise({ usage, links, apply, transaction }) {
  return Effect.gen(function* () {
    const result = {};
    Assert.equal((yield* usage.resolve([])).size, 0);
    const requests = [
      request("native:fast"),
      request("native:priority"),
      request("native:ultrafast"),
      request("native:standard"),
      request("native:default"),
      request("native:no-selection"),
      request("native:null-selection"),
      request("native:invalid-tier"),
      request("native:unknown-tier"),
      request("native:duplicate"),
      request("native:missing-attempt"),
      request("native:wrong-thread"),
      request("native:wrong-provider-thread"),
      request("native:other-driver"),
      request("native:fast", "wrong-session"),
      request("native:fast", "native-session", "wrong-model"),
      request("native:missing"),
      request("native:fast"),
    ];
    const resolved = yield* usage.resolve(requests);
    result.usage = requests.map((item) => resolved.get(item) ?? null);
    Assert.deepEqual(result.usage, [
      "fast",
      "fast",
      "ultrafast",
      "standard",
      "standard",
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      "fast",
    ]);
    result.links = yield* links.list({ ...key, host: "GitHub.Com", repository: "ACME/WEB" });
    Assert.deepEqual(
      result.links.threads.map((row) => row.id),
      ["archived", "active", "auto-settled", "settled"],
    );
    result.azure = yield* links.list({
      host: "org.visualstudio.com",
      repository: "project/_git/web",
      number: 7,
    });
    Assert.deepEqual(
      result.azure.threads.map((row) => row.id),
      ["azure"],
    );
    result.forge = yield* links.list({
      host: "forge.example:3000",
      repository: "acme/web",
      number: 7,
    });
    Assert.deepEqual(
      result.forge.threads.map((row) => row.id),
      ["forge"],
    );
    Assert.deepEqual(yield* links.list({ ...key, number: 99 }), { threads: [] });
    const moved = makeEvent(
      "thread.metadata-updated",
      thread("active", {
        pullRequests: [pr({ number: 8, url: "https://github.com/acme/web/pull/8" })],
      }),
      { threadId: "active" },
    );
    yield* failed(
      transaction(
        Effect.gen(function* () {
          yield* apply(moved);
          Assert.ok(!(yield* links.list(key)).threads.some((row) => row.id === "active"));
          Assert.deepEqual(
            (yield* links.list({ ...key, number: 8 })).threads.map((row) => row.id),
            ["active", "other-number"],
          );
          return yield* Effect.fail("rollback derived PR index");
        }),
      ),
    );
    Assert.deepEqual(yield* links.list(key), result.links);
    yield* apply(moved);
    result.moved = yield* links.list(key);
    result.destination = yield* links.list({ ...key, number: 8 });
    Assert.deepEqual(
      result.destination.threads.map((row) => row.id),
      ["active", "other-number"],
    );
    return normalized(result);
  });
}

try {
  const source = Path.join(fixture.root, "source.sqlite");
  const retained = Path.join(fixture.root, "retained.sqlite");
  const upstream = Path.join(fixture.root, "upstream.sqlite");
  await createMigratedSqlite(source);
  const db = new Sqlite.DatabaseSync(source);
  try {
    const insertThread = db.prepare(`INSERT INTO orchestration_v2_projection_threads
      (thread_id,project_id,title,default_provider,runtime_mode,interaction_mode,created_at,updated_at,archived_at,deleted_at,payload_json)
      VALUES (?,'project:main',?,'codex','full-access','default',?,?,?,?,?)`);
    const inputs = [
      thread("active"),
      thread("settled", { settledOverride: "settled" }),
      thread("auto-settled", { settledAt: fixedTime }),
      thread("archived", { archivedAt: fixedTime, updatedAt: "2026-11-11T12:00:00.000Z" }),
      thread("deleted", { deletedAt: fixedTime }),
      thread("sidechat", { lineage: { ...jsonPayloads.thread.lineage, sideChat: true } }),
      thread("dismissed", { pullRequests: [pr({ source: "stack-dismissed" })] }),
      thread("missing-source", { pullRequests: [{ ...pr(), source: null }] }),
      thread("other-repo", { pullRequests: [pr({ repository: "acme/other" })] }),
      thread("other-host", { pullRequests: [pr({ host: "github.example.com" })] }),
      thread("other-number", { pullRequests: [pr({ number: 8 })] }),
      thread("azure", {
        pullRequests: [
          pr({
            host: "dev.azure.com",
            repository: "org/project/_git/web",
            url: "https://dev.azure.com/org/project/_git/web/pullrequest/7",
          }),
        ],
      }),
      thread("forge", {
        pullRequests: [
          pr({ host: "forge.example", url: "http://forge.example:3000/acme/web/pulls/7" }),
        ],
      }),
      thread("thread:main", { pullRequests: [] }),
    ];
    for (let i = 0; i < 132; i++)
      inputs.push(thread(`unrelated:${i}`, { pullRequests: [pr({ number: 1000 + i })] }));
    for (const input of inputs)
      insertThread.run(
        input.id,
        input.title,
        fixedTime,
        input.updatedAt,
        input.archivedAt ?? null,
        input.deletedAt ?? null,
        JSON.stringify(input),
      );
    const attempt = db.prepare(`INSERT INTO orchestration_v2_projection_run_attempts
      (attempt_id,thread_id,run_id,attempt_ordinal,root_node_id,provider,provider_thread_id,provider_turn_id,status,payload_json)
      VALUES (?,? ,?,1,'node:main','codex',?,NULL,'running',?)`);
    const turn = db.prepare(`INSERT INTO orchestration_v2_projection_provider_turns
      (provider_turn_id,thread_id,provider_thread_id,node_id,run_attempt_id,ordinal,status,started_at,completed_at,payload_json)
      VALUES (?, 'thread:main', ?, 'node:main', ?, ?, 'running', ?, NULL, ?)`);
    const event = db.prepare(`INSERT INTO orchestration_events
      (event_id,aggregate_kind,stream_id,stream_version,event_type,occurred_at,actor_kind,payload_json,metadata_json,application_event_version)
      VALUES (?,'thread','thread:main',?,'run.updated',?,'system',?,'{}',2)`);
    let sequence = 0,
      ordinal = 0;
    const emit = (payload) => {
      sequence++;
      event.run(`event:${sequence}`, sequence, fixedTime, JSON.stringify(payload));
    };
    const cases = [
      ["fast", selection("fast")],
      ["priority", selection("priority")],
      ["ultrafast", selection("ultrafast")],
      ["standard", selection()],
      ["default", selection("default")],
      ["no-selection", undefined],
      ["null-selection", null],
      ["invalid-tier", selection(1)],
      ["unknown-tier", selection("custom")],
      ["duplicate", selection("fast")],
      ["missing-attempt", selection("fast")],
      ["wrong-thread", selection("fast")],
      ["wrong-provider-thread", selection("fast")],
      ["other-driver", selection("fast")],
    ];
    for (const [id, selected] of cases) {
      const attemptId = `attempt:${id}`,
        runId = `run:${id}`,
        providerId = `provider:${id}`;
      if (id !== "missing-attempt")
        attempt.run(
          attemptId,
          id === "wrong-thread" ? "other-thread" : "thread:main",
          runId,
          id === "wrong-provider-thread" ? "provider:wrong" : providerId,
          JSON.stringify({ nativeThreadId: "native-session" }),
        );
      ordinal++;
      turn.run(
        `turn:${id}`,
        providerId,
        attemptId,
        ordinal,
        fixedTime,
        JSON.stringify({
          ...jsonPayloads.providerTurn,
          id: `turn:${id}`,
          providerThreadId: providerId,
          runAttemptId: attemptId,
          ordinal,
          nativeTurnRef: {
            driver: id === "other-driver" ? "claudeAgent" : "codex",
            nativeId: `native:${id}`,
            strength: "strong",
          },
        }),
      );
      if (id === "fast")
        for (let i = 0; i < 130; i++)
          emit({ id: "unrelated", activeAttemptId: "unrelated", status: "running" });
      emit({
        id: runId,
        activeAttemptId: attemptId,
        status: "starting",
        modelSelection: selection("ultrafast"),
      });
      emit({
        id: runId,
        activeAttemptId: attemptId,
        status: "running",
        ...(selected === undefined ? {} : { modelSelection: selected }),
      });
      emit({
        id: runId,
        activeAttemptId: attemptId,
        status: "running",
        modelSelection: selection("ultrafast"),
      });
    }
    ordinal++;
    turn.run(
      "duplicate-without-attempt",
      "provider:duplicate",
      null,
      ordinal,
      fixedTime,
      JSON.stringify({
        ...jsonPayloads.providerTurn,
        nativeTurnRef: { driver: "codex", nativeId: "native:duplicate", strength: "strong" },
      }),
    );
  } finally {
    db.close();
  }
  const sourceHash = hash(await FS.readFile(source));
  await FS.copyFile(source, retained);
  await FS.copyFile(source, upstream);
  const exported = Path.join(fixture.root, "source.ndjson");
  exportSqliteState({ source, destination: exported });
  nativeRequest(executable, fixture, "initialize", {
    operation: "import_sqlite_application",
    path: exported,
  });
  const expected = await services("upstream", upstream, exercise);
  Assert.deepEqual(
    await services("sqlite", retained, exercise),
    expected,
    "Retained history factories match immutable upstream",
  );
  Assert.deepEqual(
    await services("redb", source, exercise),
    expected,
    "Native indexed histories match immutable upstream",
  );
  checks.push(
    "Codex usage tiers preserve first-running-event semantics across multiple event pages, including missing/null first selections",
    "Duplicate native turn IDs remain ambiguous even when one attempt is missing; session, model, driver and attempt ownership are exact",
    "Shared tier interpretation preserves priority/fast/ultrafast/default behavior and rejects non-string/unknown tiers",
    "Pull-request results retain canonical GitHub, Azure and custom-port identity, settled/archive fields, order, and dismissal/sidechat/deleted filtering",
    "Projection mutations move derived PR-number index entries atomically and rollback restores prior lookup results",
  );
  await services("redb", source, ({ links }) =>
    Effect.gen(function* () {
      Assert.deepEqual(normalized(yield* links.list(key)), expected.moved);
      Assert.deepEqual(normalized(yield* links.list({ ...key, number: 8 })), expected.destination);
    }),
  );
  checks.push("Reopening retains changed derived projection indexes");
  for (const corruption of [
    "turn-json",
    "thread-json",
    "nested-link",
    "deleted-thread-json",
    "sidechat-link",
  ]) {
    const target = await createFixture();
    corruptionFixtures.push(target);
    const filename = Path.join(target.root, "source.sqlite");
    await FS.copyFile(source, filename);
    const database = new Sqlite.DatabaseSync(filename);
    try {
      // A corrupt historical payload can predate today's JSON expression indexes.
      // Remove only those derived indexes from this owned corruption fixture.
      const sourceTable =
        corruption === "turn-json"
          ? "orchestration_v2_projection_provider_turns"
          : "orchestration_v2_projection_threads";
      for (const { name } of database
        .prepare(
          "SELECT name FROM sqlite_schema WHERE type='index' AND tbl_name=? AND lower(sql) LIKE '%json_%'",
        )
        .all(sourceTable))
        database.exec(`DROP INDEX "${name.replaceAll('"', '""')}"`);
      if (corruption === "turn-json")
        database.exec(
          "UPDATE orchestration_v2_projection_provider_turns SET payload_json='{bad' WHERE provider_turn_id='turn:fast'",
        );
      else if (corruption === "thread-json" || corruption === "deleted-thread-json")
        database
          .prepare(
            "UPDATE orchestration_v2_projection_threads SET payload_json='{bad' WHERE thread_id=?",
          )
          .run(corruption === "thread-json" ? "active" : "deleted");
      else
        database
          .prepare(
            "UPDATE orchestration_v2_projection_threads SET payload_json=json_set(payload_json,'$.pullRequests',json('[\"not-json\"]')) WHERE thread_id=?",
          )
          .run(corruption === "nested-link" ? "active" : "sidechat");
    } finally {
      database.close();
    }
    const exportPath = Path.join(target.root, "source.ndjson");
    exportSqliteState({ source: filename, destination: exportPath });
    nativeRequest(executable, target, "initialize", {
      operation: "import_sqlite_application",
      path: exportPath,
    });
    const inspect = ({ links, usage }) =>
      Effect.gen(function* () {
        Assert.equal((yield* usage.resolve([])).size, 0);
        const outcome = yield* Effect.exit(
          corruption === "turn-json" ? usage.resolve([request("native:fast")]) : links.list(key),
        );
        return Exit.isFailure(outcome)
          ? { failed: true }
          : { failed: false, result: normalized(outcome.value) };
      });
    const oracleResult = await services("upstream", filename, inspect, target);
    Assert.equal(
      oracleResult.failed,
      ["turn-json", "thread-json", "nested-link"].includes(corruption),
      corruption,
    );
    Assert.deepEqual(
      await services("sqlite", filename, inspect, target),
      oracleResult,
      `Retained ${corruption}`,
    );
    Assert.deepEqual(
      await services("redb", filename, inspect, target),
      oracleResult,
      `Native ${corruption}`,
    );
  }
  checks.push(
    "Malformed turn/thread/link JSON preserves SQLite query errors while deleted threads and sidechats retain filtering behavior",
  );
  Assert.equal(hash(await FS.readFile(source)), sourceHash);
  await finishProof({
    executable,
    fixture,
    proofPath,
    checks,
    sourceHash,
    oracles: [usageOracle.source, linksOracle.source],
    sourcePaths: [
      "apps/server/src/usage/CodexUsageHistory.ts",
      "apps/server/src/usage/NativeCodexUsageHistory.ts",
      "apps/server/src/pullRequest/linkedThreads.ts",
      "apps/server/src/pullRequest/NativeLinkedThreads.ts",
      "apps/server/src/persistence/redb_projection_history.rs",
      "apps/server/src/persistence/redb_projection_derived_indexes.rs",
      "apps/server/src/persistence/redb_event_rows.rs",
      "tools/check-redb-projection-history.mjs",
    ],
  });
} finally {
  await fixture.close();
  for (const target of corruptionFixtures) await target.close();
}
