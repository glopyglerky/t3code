#!/usr/bin/env node
import * as Assert from "node:assert/strict";
import * as FS from "node:fs/promises";
import * as Path from "node:path";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as ProjectionStore from "../apps/server/src/orchestration-v2/ProjectionStore.ts";
import * as NativeContexts from "../apps/server/src/orchestration-v2/NativeProjectionContextRows.ts";
import * as NativeLookups from "../apps/server/src/orchestration-v2/NativeProjectionLookupRows.ts";
import * as NativeProjectionCommit from "../apps/server/src/persistence/NativeProjectionCommitStore.ts";
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
import { jsonPayloads, sampleEvents, makeEvent, fixedTime } from "./redb-event-sink-fixtures.mjs";

const [executable, proofPath] = process.argv.slice(2);
Assert.ok(executable && Path.isAbsolute(executable), "Pass an absolute native database executable");
await FS.access(executable, FS.constants.X_OK);
const oracle = await loadUpstreamModule("apps/server/src/orchestration-v2/ProjectionStore.ts");
const fixture = await createFixture();
const checks = [];
const captureTarget = { runId: "run:main", scopeId: "scope:main" };
const rollbackTarget = {
  providerThreadId: "provider:main",
  scopeId: "scope:main",
  checkpointId: "checkpoint:main",
};
const file = (id) => ({
  id,
  type: "file",
  name: `${id}.txt`,
  mimeType: "text/plain",
  sizeBytes: 1,
});
const ownHtml = "thread-main-00000000-0000-4000-8000-000000000001-html";
const foreignHtml = "thread-other-00000000-0000-4000-8000-000000000001-html";
const threadEvent = (id, extra = {}) =>
  makeEvent(
    "thread.created",
    {
      ...jsonPayloads.thread,
      id,
      title: `Title ${id}`,
      lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: id },
      ...extra,
    },
    { threadId: id },
  );
const itemEvent = (id, type, ordinal, extra = {}) => {
  const { text, streaming, ...base } = jsonPayloads.turnItem;
  return makeEvent("turn-item.updated", { ...base, id, type, ordinal, ...extra });
};
const userMessage = (id, text, extra = {}) =>
  makeEvent("message.updated", {
    ...jsonPayloads.message,
    id,
    text,
    role: "user",
    createdBy: "user",
    creationSource: "web",
    streaming: false,
    ...extra,
  });
const failed = (body) =>
  Effect.exit(body).pipe(Effect.tap((exit) => Effect.sync(() => Assert.ok(Exit.isFailure(exit)))));

async function withServices(kind, filename, use, databaseFixture = fixture) {
  if (kind === "redb")
    return withNative(executable, databaseFixture, (client) =>
      Effect.gen(function* () {
        const writer = yield* NativeProjectionCommit.make;
        const source = yield* NativeContexts.make;
        const lookups = yield* NativeLookups.make;
        const store = ProjectionStore.makeProjectionContexts(source, lookups);
        return yield* use({ store, apply: writer.apply, transaction: client.withTransaction });
      }),
    );
  return withSqlite(filename, (sql) =>
    Effect.gen(function* () {
      const module = kind === "upstream" ? oracle.module : ProjectionStore;
      const store = yield* module.ProjectionStoreV2.pipe(Effect.provide(module.layer));
      return yield* use({
        store,
        apply: store.apply,
        transaction: (body) => sql.withTransaction(body),
      });
    }),
  );
}

const seed = (store) =>
  Effect.gen(function* () {
    for (const event of sampleEvents) yield* store.apply(event);
    yield* store.apply(threadEvent("thread:other"));
    // Reverse insertion makes implicit row order distinct from id/ordinal order.
    for (const [suffix, ordinal, status] of [
      ["z", 3, "rolled_back"],
      ["a", 2, "completed"],
      ["b", 4, "rolled_back"],
    ]) {
      const runId = `run:${suffix}`,
        nodeId = `node:${suffix}`,
        attemptId = `attempt:${suffix}`;
      yield* store.apply(
        makeEvent("run.created", {
          ...jsonPayloads.run,
          id: runId,
          ordinal,
          status,
          rootNodeId: nodeId,
          activeAttemptId: attemptId,
          completedAt: fixedTime,
          userMessageId: `message:${suffix}`,
        }),
      );
      yield* store.apply(
        makeEvent("node.updated", {
          ...jsonPayloads.node,
          id: nodeId,
          rootNodeId: nodeId,
          runId,
          status: "completed",
          completedAt: fixedTime,
        }),
      );
      for (const attemptOrdinal of [2, 1])
        yield* store.apply(
          makeEvent("run-attempt.created", {
            ...jsonPayloads.attempt,
            id: attemptOrdinal === 1 ? attemptId : `${attemptId}:second`,
            runId,
            rootNodeId: nodeId,
            attemptOrdinal,
            status: "completed",
            completedAt: fixedTime,
          }),
        );
      yield* store.apply(
        makeEvent("checkpoint.captured", {
          ...jsonPayloads.checkpoint,
          id: `checkpoint:${suffix}`,
          runId,
          nodeId,
          ordinalWithinScope: ordinal,
          appRunOrdinal: ordinal,
        }),
      );
    }
    yield* store.apply(
      makeEvent("checkpoint.captured", {
        ...jsonPayloads.checkpoint,
        id: "checkpoint:failed",
        ordinalWithinScope: 5,
        appRunOrdinal: 5,
        status: "error",
      }),
    );
    yield* store.apply(
      makeEvent("checkpoint-scope.created", {
        ...jsonPayloads.checkpointScope,
        id: "scope:unused",
        ordinalWithinParent: 1,
        cwd: "/fixture/other",
      }),
    );
    yield* store.apply(
      makeEvent("context-handoff.updated", { ...jsonPayloads.contextHandoff, id: "handoff:z" }),
    );
    yield* store.apply(
      makeEvent("context-handoff.updated", { ...jsonPayloads.contextHandoff, id: "handoff:a" }),
    );
    yield* store.apply(
      makeEvent("context-transfer.created", {
        ...jsonPayloads.contextTransfer,
        id: "transfer:incoming",
        sourceThreadId: "thread:other",
      }),
    );
    yield* store.apply(
      makeEvent("context-transfer.created", {
        ...jsonPayloads.contextTransfer,
        id: "transfer:outgoing",
        targetThreadId: "thread:other",
      }),
    );
    yield* store.apply(
      userMessage("message:z", "\u2000/CoMpAcT\ufeff", {
        attachments: [file("message_z"), file("duplicate")],
      }),
    );
    yield* store.apply(userMessage("message:a", "/goalfoo", { attachments: [file("message_a")] }));
    yield* store.apply(userMessage("message:ordinary", "Unrelated history", { runId: "run:a" }));
    yield* store.apply(userMessage("message:uppercase-goal", "/GOAL ignore"));
    yield* store.apply(
      itemEvent("item:answer-z", "user_input_request", 99, {
        requestId: "request:main",
        questions: [],
        questionAnswer: {
          requestId: "request:main",
          answers: {},
          attachmentsByQuestionId: {
            z: [file("answer_z"), file("duplicate")],
            a: [file("answer_a")],
          },
        },
      }),
    );
    yield* store.apply(
      itemEvent("item:answer-a", "user_input_request", 2, {
        requestId: "request:main",
        questions: [],
        questionAnswer: {
          requestId: "request:main",
          answers: {},
          attachmentsByQuestionId: { q: [file("answer_second")] },
        },
      }),
    );
    for (const [id, attachmentId, ordinal] of [
      ["item:render", ownHtml, 90],
      ["item:foreign-render", foreignHtml, 91],
    ]) {
      yield* store.apply(
        itemEvent(id, "dynamic_tool", ordinal, {
          toolName: "mcp__t3-code__html_render",
          input: {},
          output: [
            {
              type: "text",
              text: JSON.stringify({ htmlRender: { attachmentId, title: "Fixture", height: 300 } }),
            },
          ],
        }),
      );
    }
    yield* store.apply(
      makeEvent("thread.metadata-updated", {
        ...jsonPayloads.thread,
        guestShare: {
          id: "share:main",
          host: "fixture.invalid",
          createdAt: fixedTime,
          expiresAt: "2026-11-11T12:00:00.000Z",
          guestName: null,
          messages: [
            {
              id: "guest:pending",
              status: "pending",
              submittedAt: fixedTime,
              text: "Pending",
              attachments: [file("guest"), file("duplicate")],
            },
            {
              id: "guest:sent",
              status: "sent",
              submittedAt: fixedTime,
              text: "Sent",
              attachments: [file("guest_sent")],
            },
          ],
        },
      }),
    );
  });

function exercise({ store, apply, transaction }) {
  return Effect.gen(function* () {
    const result = {};
    result.turn = yield* store.getTurnStartContext("thread:main", "run:main");
    result.recovery = yield* store.getRuntimeRecoveryProjection("thread:main");
    result.checkpoint = yield* store.getCheckpointContext("thread:main");
    result.capture = yield* store.getCheckpointCaptureContext("thread:main", captureTarget);
    result.rollbackEarly = yield* store.getCheckpointRollbackContext("thread:main", rollbackTarget);
    result.attachments = yield* store.getThreadAttachmentIds("thread:main");
    Assert.equal(result.rollbackEarly.targetAttempt, undefined);
    Assert.equal(result.turn.hasConversation, true);
    Assert.deepEqual(
      result.turn.nodes.map((v) => v.id),
      ["node:main"],
    );
    Assert.deepEqual(
      result.turn.checkpointScopes.map((v) => v.id),
      ["scope:main"],
    );
    Assert.deepEqual(
      result.turn.messages.map((v) => v.id),
      ["message:a", "message:main", "message:z"],
    );
    Assert.ok(result.attachments.includes(ownHtml));
    Assert.ok(!result.attachments.includes(foreignHtml));
    Assert.ok(!result.attachments.includes("guest_sent"));
    Assert.equal(result.attachments.filter((id) => id === "duplicate").length, 1);
    for (const [method, arg] of [
      ["getTurnStartContext", "run:main"],
      ["getRuntimeRecoveryProjection"],
      ["getCheckpointContext"],
      ["getCheckpointCaptureContext", captureTarget],
      ["getCheckpointRollbackContext", rollbackTarget],
    ])
      Assert.equal(
        (yield* store[method]("missing", arg).pipe(Effect.flip))._tag,
        "ProjectionStoreThreadNotFoundError",
      );
    Assert.deepEqual(yield* store.getThreadAttachmentIds("missing"), []);
    result.foreignCapture = yield* store.getCheckpointCaptureContext("thread:other", captureTarget);
    Assert.equal(result.foreignCapture.run, undefined);
    result.missingRun = yield* store.getTurnStartContext("thread:main", "missing");
    Assert.deepEqual(result.missingRun.nodes, []);

    yield* apply(
      makeEvent("thread.metadata-updated", {
        ...jsonPayloads.thread,
        activeProviderThreadId: "provider:main",
        lineage: { ...jsonPayloads.thread.lineage, sideChat: true },
      }),
    );
    result.rollback = yield* store.getCheckpointRollbackContext("thread:main", rollbackTarget);
    result.sideChat = yield* store.getCheckpointCaptureContext("thread:main", captureTarget);
    Assert.equal(result.rollback.targetAttempt.id, "attempt:main");
    Assert.equal(result.rollback.providerSession.id, "session:main");
    Assert.deepEqual(
      result.rollback.runsToRollback.map((run) => run.id),
      ["run:a"],
    );
    Assert.equal(result.sideChat.sideChat, true);
    Assert.equal(result.rollback.rolledBackAttemptIds.length, 4);

    // Recovery keeps live work owners, pending input nodes and orphan root turns.
    for (const [suffix, ordinal, status] of [
      ["work", 5, "completed"],
      ["background", 6, "failed"],
      ["never", 7, "failed"],
      ["latest", 8, "completed"],
    ]) {
      yield* apply(
        makeEvent("run.created", {
          ...jsonPayloads.run,
          id: `run:${suffix}`,
          ordinal,
          status,
          providerThreadId:
            suffix === "background" || suffix === "never" ? "provider:background" : "provider:main",
          rootNodeId: `node:${suffix}`,
          activeAttemptId: null,
          completedAt: fixedTime,
          startedAt: suffix === "never" ? null : fixedTime,
        }),
      );
      yield* apply(
        makeEvent("node.updated", {
          ...jsonPayloads.node,
          id: `node:${suffix}`,
          runId: `run:${suffix}`,
          rootNodeId: `node:${suffix}`,
        }),
      );
    }
    yield* apply(
      makeEvent("provider-thread.updated", {
        ...jsonPayloads.providerThread,
        id: "provider:background",
        status: "idle",
        nativeThreadRef: null,
        pendingBackgroundTasks: [{ taskId: "watch", kind: "monitor", awaited: true }],
      }),
    );
    yield* apply(
      itemEvent("item:work", "command_execution", 130, {
        runId: "run:work",
        nodeId: "node:work",
        input: "fixture",
        status: "waiting",
      }),
    );
    yield* apply(
      itemEvent("item:input", "user_input_request", 140, {
        runId: "run:never",
        nodeId: "node:never",
        requestId: "request:input",
        questions: [],
      }),
    );
    yield* apply(
      makeEvent("runtime-request.updated", {
        ...jsonPayloads.request,
        id: "request:input",
        nodeId: "node:never",
      }),
    );
    yield* apply(
      makeEvent("node.updated", {
        ...jsonPayloads.node,
        id: "node:orphan",
        runId: null,
        rootNodeId: "node:orphan",
      }),
    );
    yield* apply(
      makeEvent("provider-turn.updated", {
        ...jsonPayloads.providerTurn,
        id: "turn:orphan",
        ordinal: 4,
        runAttemptId: null,
        nodeId: "node:orphan",
      }),
    );
    yield* apply(
      itemEvent("item:orphan", "reasoning", 150, {
        runId: null,
        nodeId: "node:orphan",
        text: "Orphan",
        streaming: true,
      }),
    );
    yield* apply(
      makeEvent("provider-session.attached", {
        ...jsonPayloads.providerSession,
        id: "session:detached",
      }),
    );
    yield* apply(
      makeEvent("provider-session.detached", {
        providerSessionId: "session:detached",
        detachedAt: fixedTime,
      }),
    );
    yield* apply(
      makeEvent("subagent.updated", {
        ...jsonPayloads.subagent,
        id: "subagent:failed",
        status: "failed",
        completedAt: fixedTime,
      }),
    );
    result.liveRecovery = yield* store.getRuntimeRecoveryProjection("thread:main");
    Assert.ok(result.liveRecovery.runs.some((run) => run.id === "run:background"));
    Assert.ok(!result.liveRecovery.runs.some((run) => run.id === "run:never"));
    Assert.ok(result.liveRecovery.runs.some((run) => run.id === "run:work"));
    Assert.ok(result.liveRecovery.nodes.some((node) => node.id === "node:never"));
    Assert.ok(result.liveRecovery.providerTurns.some((turn) => turn.id === "turn:orphan"));
    Assert.ok(
      result.liveRecovery.providerSessions.some((session) => session.id === "session:detached"),
    );
    Assert.ok(!result.liveRecovery.subagents.some((subagent) => subagent.id === "subagent:failed"));

    // Compaction alone is not conversation, while its attachments are.
    yield* apply(threadEvent("thread:compact"));
    const compact = (attachments) =>
      makeEvent(
        "message.updated",
        {
          ...jsonPayloads.message,
          id: "message:compact-only",
          threadId: "thread:compact",
          runId: null,
          nodeId: null,
          role: "user",
          text: "\u00a0 /COMPACT \ufeff",
          attachments,
          streaming: false,
        },
        { threadId: "thread:compact" },
      );
    yield* apply(compact([]));
    result.compactOnly = yield* store.getTurnStartContext("thread:compact", "missing");
    Assert.equal(result.compactOnly.hasConversation, false);
    yield* apply(compact([file("compact_file")]));
    result.compactFile = yield* store.getTurnStartContext("thread:compact", "missing");
    Assert.equal(result.compactFile.hasConversation, true);

    yield* transaction(
      Effect.gen(function* () {
        yield* failed(
          transaction(
            Effect.gen(function* () {
              yield* apply(
                makeEvent("checkpoint.captured", {
                  ...jsonPayloads.checkpoint,
                  id: "checkpoint:abort",
                  ordinalWithinScope: 999,
                  appRunOrdinal: 999,
                }),
              );
              Assert.ok(
                (yield* store.getCheckpointCaptureContext(
                  "thread:main",
                  captureTarget,
                )).readyCheckpointOrdinals.includes(999),
              );
              return yield* Effect.fail("rollback fixture checkpoint");
            }),
          ),
        );
        Assert.ok(
          !(yield* store.getCheckpointCaptureContext(
            "thread:main",
            captureTarget,
          )).readyCheckpointOrdinals.includes(999),
        );
      }),
    );
    result.finalCapture = yield* store.getCheckpointCaptureContext("thread:main", captureTarget);
    return normalized(result);
  });
}

try {
  const source = Path.join(fixture.root, "source.sqlite");
  await createMigratedSqlite(source, () =>
    Effect.gen(function* () {
      const store = yield* oracle.module.ProjectionStoreV2.pipe(
        Effect.provide(oracle.module.layer),
      );
      yield* seed(store);
    }),
  );
  const upstream = Path.join(fixture.root, "upstream.sqlite"),
    retained = Path.join(fixture.root, "retained.sqlite");
  await FS.copyFile(source, upstream);
  await FS.copyFile(source, retained);
  const sourceHash = hash(await FS.readFile(source));
  const exported = Path.join(fixture.root, "source.ndjson");
  exportSqliteState({ source, destination: exported });
  nativeRequest(executable, fixture, "initialize", {
    operation: "import_sqlite_application",
    path: exported,
  });
  const expected = await withServices("upstream", upstream, exercise);
  for (const kind of ["sqlite", "redb"]) {
    const actual = await withServices(kind, retained, exercise);
    const failures = [];
    for (const key of Object.keys(expected)) {
      try {
        Assert.deepEqual(actual[key], expected[key], `${kind} ${key} matches immutable source`);
      } catch (error) {
        failures.push(key);
        console.error(error.message);
      }
    }
    Assert.deepEqual(failures, [], `${kind} context methods match immutable source`);
  }
  checks.push(
    "Six context methods match immutable upstream and retained SQLite with imported and live projection rows",
    "Turn start preserves selected root/scopes, source row order, compact/goal commands and conversation detection",
    "Checkpoint metadata, capture, early rollback and complete rollback retain original joins and ordering",
    "Recovery includes active/latest/background/work run owners, pending inputs, orphan turns and detached sessions",
    "Message, guest, answered-input and render attachment IDs preserve distinct order and thread ownership",
    "Nested writer rollback restores checkpoint rows and derived native indexes",
  );
  await withServices("redb", source, ({ store }) =>
    Effect.gen(function* () {
      Assert.deepEqual(
        normalized(yield* store.getCheckpointCaptureContext("thread:main", captureTarget)),
        expected.finalCapture,
      );
      Assert.deepEqual(
        normalized(yield* store.getRuntimeRecoveryProjection("thread:main")),
        expected.liveRecovery,
      );
    }),
  );
  checks.push("Context results survive helper restart");
  // Imported malformed rows retain the source's distinction between a JSON
  // predicate error, a selected decoder error, and a safely excluded row.
  for (const test of [
    {
      name: "user predicate",
      table: "orchestration_v2_projection_messages",
      column: "message_id",
      id: "message:corrupt",
      event: userMessage("message:corrupt", "Unrelated history", { runId: "run:missing" }),
      payload: "{broken",
      method: "getTurnStartContext",
      argument: "run:main",
      fails: true,
    },
    {
      name: "excluded assistant",
      table: "orchestration_v2_projection_messages",
      column: "message_id",
      id: "message:corrupt",
      event: makeEvent("message.updated", {
        ...jsonPayloads.message,
        id: "message:corrupt",
        runId: "run:missing",
        streaming: false,
      }),
      payload: "{broken",
      method: "getTurnStartContext",
      argument: "run:main",
      fails: false,
    },
    ...[false, true].map((matches) => ({
      name: matches ? "selected corrupt render" : "excluded corrupt render",
      table: "orchestration_v2_projection_turn_items",
      column: "turn_item_id",
      id: "item:corrupt",
      event: itemEvent("item:corrupt", "dynamic_tool", 200, {
        runId: "run:missing",
        status: "completed",
        toolName: "fixture",
        input: {},
      }),
      payload: matches ? '{"htmlRender":' : "{broken",
      method: "getThreadAttachmentIds",
      fails: matches,
    })),
    {
      name: "guarded corrupt provider",
      table: "orchestration_v2_projection_provider_threads",
      column: "provider_thread_id",
      id: "provider:corrupt",
      event: makeEvent("provider-thread.updated", {
        ...jsonPayloads.providerThread,
        id: "provider:corrupt",
        ownerNodeId: null,
        status: "idle",
        nativeThreadRef: null,
      }),
      payload: "{broken",
      method: "getRuntimeRecoveryProjection",
      fails: false,
    },
    {
      name: "guarded corrupt session",
      table: "orchestration_v2_projection_provider_sessions",
      column: "provider_session_id",
      id: "session:corrupt",
      event: makeEvent("provider-session.attached", {
        ...jsonPayloads.providerSession,
        id: "session:corrupt",
        status: "error",
      }),
      payload: "{broken",
      method: "getRuntimeRecoveryProjection",
      fails: false,
    },
  ]) {
    const damaged = await createFixture();
    try {
      const input = Path.join(damaged.root, "source.sqlite");
      await createMigratedSqlite(input, (sql) =>
        Effect.gen(function* () {
          const store = yield* oracle.module.ProjectionStoreV2.pipe(
            Effect.provide(oracle.module.layer),
          );
          for (const event of sampleEvents) yield* store.apply(event);
          yield* store.apply(test.event);
          // Historical damage can predate today's derived JSON indexes and FTS
          // triggers. Remove only those derived objects in this owned fixture so
          // the invalid row can be represented, without altering the read query.
          const derived = yield* sql`SELECT name, type FROM sqlite_schema
          WHERE tbl_name = ${test.table} AND type IN ('index','trigger')
            AND lower(sql) LIKE '%json_%'`;
          for (const entry of derived) {
            const kind = entry.type === "index" ? "INDEX" : "TRIGGER";
            yield* sql.unsafe(`DROP ${kind} "${entry.name.replaceAll('"', '""')}"`);
          }
          yield* sql.unsafe(`UPDATE ${test.table} SET payload_json = ? WHERE ${test.column} = ?`, [
            test.payload,
            test.id,
          ]);
        }),
      );
      const exported = Path.join(damaged.root, "source.ndjson");
      exportSqliteState({ source: input, destination: exported });
      nativeRequest(executable, damaged, "initialize", {
        operation: "import_sqlite_application",
        path: exported,
      });
      const observe =
        (backend) =>
        ({ store }) =>
          Effect.gen(function* () {
            const result = yield* Effect.exit(store[test.method]("thread:main", test.argument));
            if (test.fails) {
              Assert.equal(result._tag, "Failure", `${backend}: ${test.name}`);
              const reasons = result.cause.reasons.map((reason) =>
                reason._tag === "Fail"
                  ? { kind: "failure", name: reason.error._tag }
                  : reason._tag === "Die"
                    ? { kind: "defect", name: reason.defect.name }
                    : { kind: "interruption" },
              );
              Assert.deepEqual(
                reasons,
                [
                  test.method === "getThreadAttachmentIds"
                    ? { kind: "defect", name: "SyntaxError" }
                    : { kind: "failure", name: "ProjectionStoreReadError" },
                ],
                `${backend}: ${test.name}`,
              );
              return reasons;
            }
            Assert.equal(result._tag, "Success", `${backend}: ${test.name}`);
            return normalized(result.value);
          });
      const expected = await withServices("upstream", input, observe("upstream"));
      Assert.deepEqual(
        await withServices("sqlite", input, observe("sqlite")),
        expected,
        `SQLite ${test.name}`,
      );
      Assert.deepEqual(
        await withServices("redb", input, observe("redb"), damaged),
        expected,
        `Native ${test.name}`,
      );
    } finally {
      await damaged.close();
    }
  }
  checks.push(
    "Malformed user predicates and selected renders fail, while unrelated assistant/tool rows and json_valid-guarded runtime rows stay excluded",
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
      "apps/server/src/orchestration-v2/ProjectionContextRows.ts",
      "apps/server/src/orchestration-v2/SqliteProjectionContextRows.ts",
      "apps/server/src/orchestration-v2/NativeProjectionContextRows.ts",
      "apps/server/src/persistence/redb_projection_context.rs",
      "apps/server/src/persistence/redb_projection_window_index.rs",
      "tools/check-redb-projection-context.mjs",
    ],
  });
} finally {
  await fixture.close();
}
