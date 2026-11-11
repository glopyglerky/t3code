#!/usr/bin/env node
import * as Assert from "node:assert/strict";
import * as FS from "node:fs/promises";
import * as Path from "node:path";
import * as Sqlite from "node:sqlite";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Schema from "effect/Schema";

import * as Projection from "../apps/server/src/orchestration-v2/ProjectionStore.ts";
import * as NativeRows from "../apps/server/src/orchestration-v2/NativeProjectionRecoveryRows.ts";
import * as NativeProjection from "../apps/server/src/persistence/NativeProjectionCommitStore.ts";
import * as EffectOutbox from "../apps/server/src/orchestration-v2/EffectOutbox.ts";
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
const oracle = await loadUpstreamModule("apps/server/src/orchestration-v2/ProjectionStore.ts");
const fixture = await createFixture(),
  auditFixture = await createFixture();
const checks = [];
const reset = "2026-11-12T00:00:00.000Z",
  snoozed = "2026-11-13T00:00:00.000Z";
const now = DateTime.makeUnsafe(fixedTime),
  due = DateTime.makeUnsafe(reset);
const thread = (id, values = {}) => ({
  ...jsonPayloads.thread,
  id,
  title: id,
  projectId: id.startsWith("q-") ? "project:queue" : "project:main",
  lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: id },
  ...values,
});
const run = (id, status, ordinal = 1, values = {}) => ({
  ...jsonPayloads.run,
  id: `run:${id}:${ordinal}`,
  threadId: id,
  ordinal,
  rootNodeId: `node:${id}:${ordinal}`,
  providerThreadId: null,
  activeAttemptId: null,
  status,
  completedAt: ["failed", "completed", "cancelled", "rolled_back"].includes(status)
    ? fixedTime
    : null,
  ...values,
});
const request = (id) => ({
  ...jsonPayloads.request,
  id: `request:${id}`,
  nodeId: `node:${id}:1`,
  providerTurnId: null,
});
const session = (id, status = "ready") => ({
  ...jsonPayloads.providerSession,
  id: `session:${id}`,
  status,
});
const provider = (id, ownerNodeId, appThreadId) => ({
  ...jsonPayloads.providerThread,
  id: `provider:${id}`,
  appThreadId,
  ownerNodeId,
  providerSessionId: `session:${id}`,
  nativeThreadRef: { ...jsonPayloads.providerThread.nativeThreadRef, nativeId: `native:${id}` },
});
const node = (owner, id, values = {}) => ({
  ...jsonPayloads.node,
  id,
  threadId: owner,
  rootNodeId: id,
  runId: null,
  ...values,
});
const subagent = (owner, id, values = {}) => ({
  ...jsonPayloads.subagent,
  id,
  threadId: owner,
  runId: null,
  providerThreadId: null,
  ...values,
});
const failure = (id) => ({
  ...jsonPayloads.turnItem,
  id: `error:${id}`,
  type: "error",
  threadId: id,
  runId: `run:${id}:1`,
  nodeId: `node:${id}:1`,
  providerThreadId: null,
  providerTurnId: null,
  ordinal: 2,
  status: "failed",
  completedAt: fixedTime,
  failure: {
    class: "usage_limit",
    message: "Plan limit reached.",
    resetAt: reset,
    code: "usageLimitExceeded",
    retryable: null,
  },
});
const options = (clock = now, autoResume = true, snooze = false) => ({
  now: clock,
  autoResume,
  snooze,
});
const stage = (name, effect) => effect.pipe(Effect.mapError((cause) => new Error(name, { cause })));

async function services(kind, filename, use, target = fixture, auditOnly = false) {
  if (kind === "redb")
    return withNative(executable, target, (client) =>
      Effect.gen(function* () {
        if (!auditOnly) {
          while (
            !(yield* stage(
              `prepare ${target === auditFixture ? "audit" : "primary"} projection recovery indexes`,
              client.prepareProjectionReads,
            )).prepared
          )
            yield* Effect.yieldNow;
          while (!(yield* stage("prepare recovery outbox", client.prepareOutbox)).prepared)
            yield* Effect.yieldNow;
          while (
            !(yield* stage("prepare auxiliary recovery indexes", client.prepareAuxiliary)).prepared
          )
            yield* Effect.yieldNow;
        }
        const store = Projection.makeProjectionRecovery(yield* NativeRows.make);
        const projection = auditOnly ? undefined : yield* NativeProjection.make;
        return yield* use({ store, apply: projection?.apply, transaction: client.withTransaction });
      }),
    );
  return withSqlite(filename, (sql) =>
    Effect.gen(function* () {
      const module = kind === "upstream" ? oracle.module : Projection;
      const store = yield* module.ProjectionStoreV2.pipe(Effect.provide(module.layer));
      return yield* use({
        store,
        apply: store.apply,
        transaction: (body) => sql.withTransaction(body),
      });
    }),
  );
}

function exercise({ store, apply, transaction }) {
  return Effect.gen(function* () {
    const result = {};
    for (const kind of [
      "queued-runs",
      "delegated-completions",
      "subagent-results",
      "restart-continuations",
      "runtime",
    ]) {
      result[kind] = yield* stage(`select recovery ${kind}`, store.getRecoveryThreadIds(kind));
      result[`${kind}:queue`] = yield* store.getRecoveryThreadIds(kind, {
        include: ["project:queue"],
      });
      result[`${kind}:exclude`] = yield* store.getRecoveryThreadIds(kind, {
        exclude: ["project:queue"],
      });
      Assert.deepEqual(yield* store.getRecoveryThreadIds(kind, { include: [] }), []);
    }
    Assert.deepEqual(result["queued-runs"], ["child:held", "q-held", "q-ready"]);
    Assert.deepEqual(result["delegated-completions"], ["delegated"]);
    Assert.deepEqual(result["subagent-results"], [
      "child:held",
      "child:pending-result",
      "child:result",
    ]);
    for (const id of [
      "q-ready",
      "q-blocked",
      "q-archived",
      "session-live",
      "session-detached",
      "session-extension",
      "provider-bg",
      "node-owner",
      "subagent-owner",
      "child-root",
      "native-subagent",
      "item-live",
      "outbox-pending",
    ])
      Assert.ok(result.runtime.includes(id), `Runtime candidate ${id}`);
    for (const id of ["q-held", "q-deleted", "session-stopped", "item-rolled", "outbox-failed"])
      Assert.ok(!result.runtime.includes(id), `Excluded runtime candidate ${id}`);
    result.arm = yield* store.getLimitRecoveryCandidates(options());
    Assert.deepEqual(
      result.arm.map((row) => row.id),
      ["limit:new", "limit:node", "limit:unstarted-cancel"],
    );
    result.snooze = yield* store.getLimitRecoveryCandidates(options(now, false, true));
    Assert.deepEqual(result.snooze, result.arm);
    Assert.deepEqual(yield* store.getLimitRecoveryCandidates(options(now, false, false)), []);
    result.due = yield* store.getLimitRecoveryCandidates(options(due, false, false));
    Assert.deepEqual(
      result.due.map((row) => row.id),
      ["limit:tracked"],
    );
    result.wake = yield* store.getLimitRecoveryWakeAt(now);
    Assert.equal(result.wake, DateTime.toEpochMillis(due));
    Assert.equal(yield* store.getLimitRecoveryWakeAt(due), null);
    const changed = thread("limit:tracked", {
      limitRecovery: {
        runId: "run:limit:tracked:1",
        resetAt: reset,
        autoResume: true,
        requestId: "choice",
      },
      snoozedUntil: snoozed,
    });
    const event = makeEvent("thread.metadata-updated", changed, { threadId: changed.id });
    const rolled = yield* transaction(
      Effect.gen(function* () {
        yield* apply(event);
        Assert.deepEqual(yield* store.getLimitRecoveryCandidates(options(due, false, false)), []);
        Assert.equal(
          yield* store.getLimitRecoveryWakeAt(due),
          DateTime.toEpochMillis(DateTime.makeUnsafe(snoozed)),
        );
        return yield* Effect.fail("rollback recovery metadata");
      }),
    ).pipe(Effect.exit);
    Assert.ok(Exit.isFailure(rolled));
    Assert.deepEqual(
      yield* store.getLimitRecoveryCandidates(options(due, false, false)),
      result.due,
    );
    yield* apply(event);
    result.finalWake = yield* store.getLimitRecoveryWakeAt(due);
    return normalized(result);
  });
}

async function seed(source) {
  await createMigratedSqlite(source);
  await withSqlite(source, () =>
    Effect.gen(function* () {
      const store = yield* Projection.ProjectionStoreV2.pipe(Effect.provide(Projection.layer));
      const events = [];
      const emit = (type, payload, id) => {
        try {
          events.push(makeEvent(type, payload, { threadId: id, id: `seed:${events.length}` }));
        } catch (cause) {
          throw new Error(`Invalid ${type} fixture ${id}: ${cause.message}`);
        }
      };
      const ids = [
        "q-ready",
        "q-held",
        "q-blocked",
        "q-archived",
        "q-deleted",
        "delegated",
        "restart",
        "session-live",
        "session-detached",
        "session-extension",
        "session-stopped",
        "provider-bg",
        "node-owner",
        "subagent-owner",
        "child-root",
        "native-subagent",
        "item-live",
        "item-rolled",
        "outbox-pending",
        "outbox-failed",
        "parent",
        "child:result",
        "child:delivered",
        "child:pending-result",
        "child:held",
        "child:running",
        "settled-parent",
        "limit:new",
        "limit:tracked",
        "limit:pending",
        "limit:later-success",
        "limit:abandoned",
        "limit:node",
        "limit:unstarted-cancel",
        "audit:clean",
        "audit:fork",
        "audit:grandchild",
        "audit:orphan",
      ];
      for (const id of ids)
        emit(
          "thread.created",
          thread(id, {
            ...(id === "q-archived" ? { archivedAt: fixedTime } : {}),
            ...(id === "q-deleted" ? { deletedAt: fixedTime } : {}),
            ...(id === "settled-parent" ? { settledOverride: "settled" } : {}),
            ...(id === "limit:tracked"
              ? {
                  limitRecovery: {
                    runId: "run:limit:tracked:1",
                    resetAt: reset,
                    autoResume: true,
                    requestId: "choice",
                  },
                }
              : {}),
          }),
          id,
        );
      for (const id of ["q-ready", "q-held", "q-blocked", "q-archived", "q-deleted"])
        emit(
          "run.created",
          run(id, "queued", 1, {
            startedAt: null,
            ...(id === "q-held" ? { queueHeld: true } : {}),
          }),
          id,
        );
      emit("run.created", run("q-blocked", "running", 2), "q-blocked");
      emit("run.created", run("delegated", "completed"), "delegated");
      emit(
        "run.created",
        run("restart", "starting", 1, { restartContinuationOfRunId: "before-restart" }),
        "restart",
      );
      for (const id of ["session-live", "session-detached", "session-extension", "session-stopped"])
        emit(
          "provider-session.attached",
          session(
            id,
            id === "session-extension" ? "error" : id === "session-stopped" ? "stopped" : "ready",
          ),
          id,
        );
      emit("node.updated", node("node-owner", "owner-node", { status: "completed" }), "node-owner");
      emit(
        "provider-thread.updated",
        provider("provider-bg", "owner-node", "provider-bg"),
        "provider-bg",
      );
      emit(
        "subagent.updated",
        subagent("subagent-owner", "provider-owner", {
          status: "completed",
          providerThreadId: "provider:provider-bg",
        }),
        "subagent-owner",
      );
      emit("node.updated", node("child-root", "child-root-node"), "child-root");
      emit(
        "subagent.updated",
        subagent("parent", "child-task", {
          childThreadId: "child-root",
          status: "completed",
          origin: "app_owned",
        }),
        "parent",
      );
      emit("subagent.updated", subagent("native-subagent", "native-task"), "native-subagent");
      for (const id of ["item-live", "item-rolled"]) {
        emit("run.created", run(id, id === "item-rolled" ? "rolled_back" : "completed"), id);
        emit(
          "turn-item.updated",
          {
            ...jsonPayloads.turnItem,
            id: `item:${id}`,
            threadId: id,
            runId: `run:${id}:1`,
            type: "dynamic_tool",
            toolName: "fixture",
            input: null,
            ordinal: 3,
            status: "running",
          },
          id,
        );
      }
      for (const id of [
        "child:result",
        "child:delivered",
        "child:pending-result",
        "child:held",
        "child:running",
      ]) {
        emit("run.created", run(id, id === "child:running" ? "running" : "completed"), id);
        if (id === "child:held")
          emit("run.created", run(id, "queued", 2, { startedAt: null, queueHeld: true }), id);
      }
      emit(
        "subagent.updated",
        subagent("parent", "pending-result-task", {
          childThreadId: "child:pending-result",
          origin: "app_owned",
          status: "completed",
          result: null,
        }),
        "parent",
      );
      for (const id of ["child:delivered", "child:pending-result"])
        emit(
          "context-transfer.created",
          {
            ...jsonPayloads.contextTransfer,
            id: `transfer:${id}`,
            sourceThreadId: id,
            targetThreadId: "parent",
          },
          id,
        );
      for (const id of ids.filter((id) => id.startsWith("limit:"))) {
        emit("run.created", run(id, "failed"), id);
        emit("turn-item.updated", failure(id), id);
      }
      emit("run.created", run("limit:later-success", "completed", 2), "limit:later-success");
      emit(
        "run.created",
        run("limit:unstarted-cancel", "cancelled", 2, { startedAt: null }),
        "limit:unstarted-cancel",
      );
      emit("runtime-request.updated", request("limit:pending"), "limit:pending");
      emit(
        "turn-item.updated",
        {
          ...failure("limit:node"),
          id: "error:wrong-node",
          nodeId: "unrelated",
          ordinal: 4,
          failure: { ...failure("limit:node").failure, class: "unknown", resetAt: null },
        },
        "limit:node",
      );
      for (const event of events) yield* store.apply(event);
      const outbox = yield* EffectOutbox.EffectOutboxV2.pipe(Effect.provide(EffectOutbox.layer));
      yield* outbox.enqueue(
        ["outbox-pending", "outbox-failed"].map((threadId) => ({
          id: threadId,
          commandId: `command:${threadId}`,
          threadId,
          request: { type: "terminal.cleanup" },
        })),
      );
    }),
  );
  const db = new Sqlite.DatabaseSync(source);
  try {
    db.exec(
      `UPDATE orchestration_v2_projection_runs SET payload_json=json_set(payload_json,'$.delegatedCompletion',json('{"delivery":{}}')) WHERE thread_id='delegated'`,
    );
    db.exec(
      `DELETE FROM orchestration_v2_projection_provider_session_bindings WHERE provider_session_id='session:session-detached'`,
    );
    db.exec(
      `UPDATE orchestration_v2_projection_provider_sessions SET payload_json=json_set(payload_json,'$.extensionStatuses',json('[{}]')) WHERE provider_session_id='session:session-extension'`,
    );
    db.exec(
      `UPDATE orchestration_v2_projection_provider_threads SET status='closed',payload_json=json_set(payload_json,'$.status','closed','$.pendingBackgroundTasks',json('[{}]')) WHERE provider_thread_id='provider:provider-bg'`,
    );
    db.prepare(
      `UPDATE orchestration_v2_effect_outbox SET status='failed',completed_at=?,last_error='fixture failure' WHERE effect_id='outbox-failed'`,
    ).run(fixedTime);
    for (const id of [
      "child:result",
      "child:delivered",
      "child:pending-result",
      "child:held",
      "child:running",
    ])
      db.prepare(
        `UPDATE orchestration_v2_projection_threads SET payload_json=json_set(payload_json,'$.lineage.parentThreadId','parent','$.lineage.relationshipToParent','subagent','$.forkedFrom',json('{"type":"node","threadId":"parent","nodeId":"node:parent"}')) WHERE thread_id=?`,
      ).run(id);
    db.exec(
      `UPDATE orchestration_v2_projection_runs SET payload_json=json_set(payload_json,'$.delegatedTaskId','pending-result-task') WHERE thread_id='child:pending-result'`,
    );
    db.exec(`UPDATE orchestration_v2_projection_context_transfers SET type='subagent_result'`);
    db.exec(
      `UPDATE orchestration_v2_projection_threads SET payload_json=json_set(payload_json,'$.lineage.parentThreadId','settled-parent','$.lineage.relationshipToParent','subagent') WHERE thread_id='limit:abandoned'`,
    );
  } finally {
    db.close();
  }
}

try {
  const source = Path.join(fixture.root, "source.sqlite"),
    retained = Path.join(fixture.root, "retained.sqlite"),
    upstream = Path.join(fixture.root, "upstream.sqlite");
  await seed(source);
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
    "Retained recovery factories match immutable upstream",
  );
  Assert.deepEqual(
    await services("redb", source, exercise),
    expected,
    "Native indexed recovery selectors match immutable upstream",
  );
  checks.push(
    "All five recovery kinds preserve queued/held, provider binding/detach, native subagent, background task, rollback and outbox selection",
    "Project include/exclude filters and recovery ordering match immutable upstream",
    "Usage-limit recovery preserves executed-run ordering, root failure selection, parent abandonment, pending prompts, arming and tracked due behavior",
    "Wake calculations and recovery metadata/index mutations preserve transaction rollback",
  );
  await services("redb", source, ({ store }) =>
    Effect.gen(function* () {
      Assert.equal(yield* store.getLimitRecoveryWakeAt(due), expected.finalWake);
    }),
  );
  checks.push("Reopening preserves changed recovery flags and wake times");
  const auditSource = Path.join(auditFixture.root, "audit.sqlite");
  await FS.copyFile(source, auditSource);
  const db = new Sqlite.DatabaseSync(auditSource);
  try {
    db.exec(
      `UPDATE orchestration_v2_projection_threads SET payload_json='{bad' WHERE thread_id='q-held'`,
    );
    db.exec(
      `UPDATE orchestration_v2_projection_provider_sessions SET payload_json='{bad' WHERE provider_session_id='session:session-live'`,
    );
    db.exec(
      `UPDATE orchestration_v2_projection_provider_threads SET payload_json='{bad' WHERE provider_thread_id='provider:provider-bg'`,
    );
    for (const [id, sourceId] of [
      ["audit:fork", "q-held"],
      ["audit:grandchild", "audit:fork"],
      ["audit:orphan", "missing-parent"],
    ]) {
      db.prepare(
        `UPDATE orchestration_v2_projection_threads SET payload_json=json_set(payload_json,'$.forkedFrom',json(?)) WHERE thread_id=?`,
      ).run(JSON.stringify({ type: "run", threadId: sourceId, runId: "missing-run" }), id);
    }
    const insert = db.prepare(
      `INSERT INTO orchestration_v2_projection_messages(message_id,thread_id,run_id,node_id,role,streaming,created_at,updated_at,payload_json) VALUES(?,'audit:clean',NULL,NULL,'user',0,?,?,?)`,
    );
    for (let i = 0; i < 503; i++)
      insert.run(
        `audit-message:${i}`,
        fixedTime,
        fixedTime,
        JSON.stringify({
          ...jsonPayloads.message,
          id: `audit-message:${i}`,
          threadId: "audit:clean",
          role: "user",
          text: i === 502 ? 42 : "okay",
        }),
      );
  } finally {
    db.close();
  }
  const auditExport = Path.join(auditFixture.root, "audit.ndjson");
  exportSqliteState({ source: auditSource, destination: auditExport });
  nativeRequest(executable, auditFixture, "initialize", {
    operation: "import_sqlite_application",
    path: auditExport,
  });
  const inspect = ({ store }) => store.getUnreadableThreadIds();
  const auditExpected = await services("upstream", auditSource, inspect, auditFixture, true);
  for (const id of [
    "q-held",
    "session-live",
    "provider-bg",
    "node-owner",
    "subagent-owner",
    "audit:fork",
    "audit:grandchild",
    "audit:orphan",
    "audit:clean",
  ])
    Assert.ok(auditExpected.includes(id), `Unreadable affected thread ${id}`);
  Assert.deepEqual(
    await services("sqlite", auditSource, inspect, auditFixture, true),
    auditExpected,
  );
  Assert.deepEqual(await services("redb", auditSource, inspect, auditFixture, true), auditExpected);
  checks.push(
    "Raw unreadability audit spans bounded pages before index preparation, including malformed JSON, provider owners and transitive/missing run-fork sources",
  );
  const guardedRecovery = ({ store }) =>
    Effect.gen(function* () {
      const results = {};
      for (const kind of [
        "queued-runs",
        "delegated-completions",
        "subagent-results",
        "restart-continuations",
        "runtime",
      ])
        results[kind] = yield* store.getRecoveryThreadIds(kind);
      Assert.ok(
        Exit.isFailure(yield* store.getLimitRecoveryWakeAt(now).pipe(Effect.exit)),
        "Unguarded wake JSON selection must report malformed source rows",
      );
      Assert.deepEqual(yield* store.getUnreadableThreadIds(), auditExpected);
      return results;
    });
  const guardedExpected = await services("upstream", auditSource, guardedRecovery, auditFixture);
  Assert.deepEqual(
    await services("sqlite", auditSource, guardedRecovery, auditFixture),
    guardedExpected,
  );
  Assert.deepEqual(
    await services("redb", auditSource, guardedRecovery, auditFixture),
    guardedExpected,
  );
  checks.push(
    "Index preparation retains corrupt historical rows for audit; guarded recovery tolerates them and unguarded wake queries fail like SQLite",
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
      "apps/server/src/orchestration-v2/ProjectionStore.ts",
      "apps/server/src/orchestration-v2/ProjectionRecoveryRows.ts",
      "apps/server/src/orchestration-v2/SqliteProjectionRecoveryRows.ts",
      "apps/server/src/orchestration-v2/NativeProjectionRecoveryRows.ts",
      "apps/server/src/persistence/redb_projection_recovery.rs",
      "apps/server/src/persistence/redb_projection_derived_indexes.rs",
      "apps/server/src/persistence/redb_projection_window_index.rs",
      "tools/check-redb-projection-recovery.mjs",
    ],
  });
} finally {
  await fixture.close();
  await auditFixture.close();
}
