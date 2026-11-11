#!/usr/bin/env node
import * as Assert from "node:assert/strict";
import * as FS from "node:fs/promises";
import * as Path from "node:path";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";

import * as ProjectionStore from "../apps/server/src/orchestration-v2/ProjectionStore.ts";
import * as NativeControls from "../apps/server/src/orchestration-v2/NativeProjectionControlRows.ts";
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
const failed = (body) =>
  Effect.exit(body).pipe(Effect.tap((exit) => Effect.sync(() => Assert.ok(Exit.isFailure(exit)))));

async function withServices(kind, filename, use) {
  if (kind === "redb")
    return withNative(executable, fixture, (client) =>
      Effect.gen(function* () {
        const store = yield* NativeControls.make;
        const writer = yield* NativeProjectionCommit.make;
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
const itemEvent = (id, requestId, nodeId, ordinal) => {
  const { text, streaming, ...base } = jsonPayloads.turnItem;
  return makeEvent("turn-item.updated", {
    ...base,
    id,
    type: "user_input_request",
    requestId,
    nodeId,
    ordinal,
    questions: [],
  });
};
const providerEvent = (id, threadId, ownerNodeId, extra = {}) =>
  makeEvent(
    "provider-thread.updated",
    {
      ...jsonPayloads.providerThread,
      id,
      appThreadId: threadId,
      ownerNodeId,
      nativeThreadRef: null,
      ...extra,
    },
    { threadId },
  );

function exercise({ store, apply, transaction }) {
  return Effect.gen(function* () {
    const result = {};
    const target = {
      providerThreadId: "provider:main",
      providerTurnId: "turn:main",
      attemptId: "attempt:main",
      messageId: "message:main",
    };
    result.initialControl = yield* store.getProviderControlContext("thread:main", target);
    result.initialRunning = yield* store.getRunningTurnContext("thread:main");
    result.initialPending = yield* store.getPendingNativeUserInputs("thread:main", "turn:main");
    result.initialResponse = yield* store.getRuntimeResponseContext("thread:main", "request:main");
    result.writerPending = yield* transaction(
      store.getPendingNativeUserInputs("thread:main", "turn:main"),
    );
    result.writerResponse = yield* transaction(
      store.getRuntimeResponseContext("thread:main", "request:main"),
    );
    Assert.deepEqual(normalized(result.writerPending), normalized(result.initialPending));
    Assert.deepEqual(normalized(result.writerResponse), normalized(result.initialResponse));
    Assert.equal(result.initialResponse.item.id, "item:request");
    Assert.equal(result.initialRunning.providerTurn.id, "turn:main");
    Assert.equal(result.initialControl.run.id, "run:main");
    for (const method of [
      "getProviderControlContext",
      "getRunningTurnContext",
      "getThreadProviderContext",
      "getRuntimeResponseContext",
    ]) {
      const arg =
        method === "getProviderControlContext"
          ? target
          : method === "getRuntimeResponseContext"
            ? "request:main"
            : undefined;
      Assert.equal(
        (yield* store[method]("missing", arg).pipe(Effect.flip))._tag,
        "ProjectionStoreThreadNotFoundError",
      );
    }
    Assert.deepEqual(yield* store.getPendingNativeUserInputs("missing", "turn:main"), {
      runtimeRequests: [],
      nodes: [],
      turnItems: [],
    });
    result.foreignControl = yield* store.getProviderControlContext("thread:other", target);
    Assert.deepEqual(normalized(result.foreignControl), {});
    Assert.equal(
      (yield* store.getRuntimeResponseContext("thread:other", "request:main")).request,
      undefined,
    );
    Assert.equal(
      (yield* store.getRuntimeResponseContext("thread:main", "missing")).request,
      undefined,
    );
    Assert.equal((yield* store.getRunningTurnContext("thread:other")).run, undefined);

    // Pending requests retain insertion order while message-mode and settled requests are ignored.
    yield* apply(makeEvent("node.updated", { ...jsonPayloads.node, id: "node:z", runId: null }));
    yield* apply(makeEvent("node.updated", { ...jsonPayloads.node, id: "node:a", runId: "run:z" }));
    for (const [id, nodeId, responseCapability, status] of [
      ["request:z", "node:z", { type: "not_resumable", reason: "fixture" }, "pending"],
      ["request:a", "node:a", { type: "live", providerSessionId: "session:main" }, "pending"],
      ["request:message", "node:main", { type: "message" }, "pending"],
      [
        "request:resolved",
        "node:main",
        { type: "live", providerSessionId: "session:main" },
        "resolved",
      ],
    ])
      yield* apply(
        makeEvent("runtime-request.updated", {
          ...jsonPayloads.request,
          id,
          nodeId,
          responseCapability,
          status,
        }),
      );
    yield* apply(itemEvent("item:z", "request:z", "node:z", 50));
    yield* apply(itemEvent("item:a", "request:a", "node:a", 10));
    yield* apply(itemEvent("item:message", "request:message", "node:main", 9));
    yield* apply(itemEvent("item:unrelated", "request:unrelated", "node:main", 8));
    result.pending = yield* store.getPendingNativeUserInputs("thread:main", "turn:main");
    Assert.equal(result.pending.runtimeRequests.length, 3);
    Assert.equal(result.pending.turnItems.length, 3);
    Assert.equal(
      (yield* store.getRuntimeResponseContext("thread:main", "request:z")).session,
      undefined,
    );
    Assert.equal(
      (yield* store.getRuntimeResponseContext("thread:main", "request:message")).session,
      undefined,
    );

    // Provider visibility admits app ownership, a node owner, and a linked subagent independently.
    yield* apply(makeEvent("node.updated", { ...jsonPayloads.node, id: "node:owned" }));
    yield* apply(makeEvent("node.updated", { ...jsonPayloads.node, id: "node:unattached" }));
    yield* apply(
      makeEvent("provider-thread.updated", {
        ...jsonPayloads.providerThread,
        id: "provider:unattached",
        appThreadId: null,
        ownerNodeId: "node:unattached",
        nativeThreadRef: null,
      }),
    );
    yield* apply(providerEvent("provider:owned", "thread:other", "node:owned"));
    yield* apply(providerEvent("provider:linked", "thread:other", null));
    yield* apply(providerEvent("provider:hidden", "thread:other", null));
    yield* apply(
      makeEvent("subagent.updated", {
        ...jsonPayloads.subagent,
        id: "subagent:linked",
        providerThreadId: "provider:linked",
      }),
    );
    for (const id of [
      "provider:owned",
      "provider:linked",
      "provider:hidden",
      "provider:unattached",
    ]) {
      result[id] = yield* store.getProviderControlContext("thread:main", {
        ...target,
        providerThreadId: id,
      });
      Assert.equal(result[id].providerThread?.id, id === "provider:hidden" ? undefined : id);
    }

    yield* apply(
      makeEvent("provider-session.attached", {
        ...jsonPayloads.providerSession,
        id: "session:z",
        updatedAt: "2026-11-10T11:00:00.000Z",
      }),
    );
    yield* apply(
      makeEvent("provider-session.attached", { ...jsonPayloads.providerSession, id: "session:a" }),
    );
    yield* apply(
      providerEvent("provider:root-z", "thread:main", null, {
        updatedAt: "2026-11-10T11:00:00.000Z",
      }),
    );
    yield* apply(providerEvent("provider:root-a", "thread:main", null));
    yield* apply(
      makeEvent("thread.metadata-updated", {
        ...jsonPayloads.thread,
        activeProviderThreadId: "provider:main",
      }),
    );
    result.providerDefault = yield* store.getThreadProviderContext("thread:main");
    result.providerTarget = yield* store.getThreadProviderContext("thread:main", "codex");
    Assert.deepEqual(result.providerDefault.providerThreads, []);
    Assert.deepEqual(
      result.providerTarget.providerSessions.map((session) => session.id),
      ["session:z", "session:a", "session:main"],
    );
    Assert.deepEqual(
      result.providerTarget.providerThreads.map((thread) => thread.id),
      ["provider:main", "provider:root-a"],
    );

    yield* apply(
      makeEvent("provider-turn.updated", { ...jsonPayloads.providerTurn, runAttemptId: null }),
    );
    yield* apply(
      makeEvent("run.created", {
        ...jsonPayloads.run,
        id: "run:unassigned",
        ordinal: 2,
        activeAttemptId: null,
        status: "queued",
      }),
    );
    result.nullAttempt = yield* store.getProviderControlContext("thread:main", target);
    Assert.equal(result.nullAttempt.run.id, "run:unassigned");
    result.withoutMessage = yield* store.getProviderControlContext("thread:main", {
      providerThreadId: "provider:main",
      providerTurnId: "turn:main",
    });
    Assert.equal(result.withoutMessage.run, undefined);
    yield* apply(makeEvent("run.updated", { ...jsonPayloads.run, activeAttemptId: null }));
    result.runningNull = yield* store.getRunningTurnContext("thread:main");
    Assert.equal(result.runningNull.providerTurn, undefined);

    // Reads inside a writer observe staged updates, while nested failure restores them.
    yield* transaction(
      Effect.gen(function* () {
        yield* failed(
          transaction(
            Effect.gen(function* () {
              yield* apply(
                makeEvent("runtime-request.updated", {
                  ...jsonPayloads.request,
                  status: "resolved",
                }),
              );
              Assert.equal(
                (yield* store.getRuntimeResponseContext("thread:main", "request:main")).request
                  .status,
                "resolved",
              );
              return yield* Effect.fail("discard request update");
            }),
          ),
        );
        Assert.equal(
          (yield* store.getRuntimeResponseContext("thread:main", "request:main")).request.status,
          "pending",
        );
      }),
    );
    yield* apply(
      makeEvent("provider-session.detached", {
        providerSessionId: "session:main",
        detachedAt: fixedTime,
      }),
    );
    result.detached = yield* store.getRuntimeResponseContext("thread:main", "request:main");
    Assert.equal(result.detached.session, undefined);
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
      for (const event of sampleEvents) yield* store.apply(event);
      yield* store.apply(threadEvent("thread:other"));
      yield* store.apply(itemEvent("item:request", "request:main", "node:main", 70));
      // Small control reads must not load eighteen MiB of excluded tool output,
      // including when they join a live writer's bounded observation set.
      for (let index = 0; index < 24; index++)
        yield* store.apply(
          makeEvent("turn-item.updated", {
            ...jsonPayloads.turnItem,
            id: `item:excluded:${index}`,
            ordinal: 100 + index,
            text: "x".repeat(768 * 1024),
            status: "completed",
            streaming: false,
            completedAt: fixedTime,
          }),
        );
    }),
  );
  const upstream = Path.join(fixture.root, "upstream.sqlite");
  const retained = Path.join(fixture.root, "retained.sqlite");
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
  Assert.deepEqual(
    await withServices("sqlite", retained, exercise),
    expected,
    "Shared provider control preserves immutable source behavior",
  );
  Assert.deepEqual(
    await withServices("redb", source, exercise),
    expected,
    "Native provider control matches immutable SQLite",
  );
  checks.push(
    "All five provider/runtime control methods match immutable upstream and retained SQL",
    "Point ownership, shared provider nodes, linked subagents and hidden foreign providers preserve control visibility",
    "Pending inputs filter message-mode requests while preserving row order and request/item pairing",
    "Null-attempt IS semantics, running-turn SQL null exclusion and target-instance tie breaks match source",
    "Live session bindings, detachment and nested writer rollback share one native snapshot",
    "Writer-scoped control reads skip eighteen MiB of excluded historical tool output",
  );
  await withServices("redb", source, ({ store }) =>
    Effect.gen(function* () {
      Assert.deepEqual(
        normalized(yield* store.getRuntimeResponseContext("thread:main", "request:main")),
        expected.detached,
      );
    }),
  );
  checks.push("Control rows persist after helper restart");
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
      "apps/server/src/orchestration-v2/ProjectionControlRows.ts",
      "apps/server/src/orchestration-v2/SqliteProjectionControlRows.ts",
      "apps/server/src/orchestration-v2/NativeProjectionControlRows.ts",
      "apps/server/src/persistence/redb_projection_control.rs",
      "apps/server/src/persistence/redb_projection_read_index.rs",
      "apps/server/src/persistence/redb_projection_window_index.rs",
      "tools/check-redb-projection-controls.mjs",
    ],
  });
} finally {
  await fixture.close();
}
