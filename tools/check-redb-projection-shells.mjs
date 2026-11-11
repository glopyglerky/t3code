#!/usr/bin/env node
import * as NodeAssert from "node:assert/strict";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";

import * as ProjectionStore from "../apps/server/src/orchestration-v2/ProjectionStore.ts";
import * as NativeProjectionStore from "../apps/server/src/orchestration-v2/NativeProjectionStore.ts";
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
NodeAssert.ok(
  executable && NodePath.isAbsolute(executable),
  "Pass an absolute native database executable",
);
await NodeFSP.access(executable, NodeFSP.constants.X_OK);
const oracle = await loadUpstreamModule("apps/server/src/orchestration-v2/ProjectionStore.ts");
const fixture = await createFixture();
const checks = [];
const failed = (body) =>
  Effect.exit(body).pipe(
    Effect.tap((exit) => Effect.sync(() => NodeAssert.ok(Exit.isFailure(exit)))),
  );

async function withServices(kind, filename, use) {
  if (kind === "redb")
    return withNative(executable, fixture, (client) =>
      Effect.gen(function* () {
        const store = yield* NativeProjectionStore.make;
        return yield* use({ store, apply: store.apply, transaction: client.withTransaction });
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
const taskEvent = (id, type, status, ordinal, extra = {}) => {
  const { text: _text, streaming: _streaming, ...base } = jsonPayloads.turnItem;
  return makeEvent("turn-item.updated", { ...base, id, type, status, ordinal, ...extra });
};

const exercise = Effect.fn("checkRedbProjectionShells.exercise")(function* ({
  store,
  apply,
  transaction,
}) {
  const result = {};
  result.snapshot = yield* store.getShellSnapshot();
  NodeAssert.equal(result.snapshot.snapshotSequence, 7);
  NodeAssert.equal(result.snapshot.threads.length, 7);
  NodeAssert.equal(result.snapshot.archivedThreads.length, 1);
  for (const options of [
    { location: "active" },
    { location: "archive" },
    { projectId: "project:second" },
    { unsettledOnly: true },
    { familyOf: "thread:main" },
    { familyOf: "missing" },
    { worktreePath: "/fixture/one" },
    { threadIds: [] },
    { threadIds: ["thread:main", "thread:archived"] },
  ]) {
    result[JSON.stringify(options)] = yield* store.getShellSnapshot(options);
  }
  const deferred = yield* transaction(store.readShellSnapshot({ location: "active" }));
  result.deferred = yield* deferred;
  result.main = yield* store.getThreadShell("thread:main");
  result.fork = yield* store.getThreadShell("thread:fork");
  NodeAssert.equal(yield* store.getThreadShell("thread:deleted"), null);
  NodeAssert.equal(yield* store.getThreadShell("missing"), null);
  result.awareness = yield* store.getAgentAwarenessSnapshot();
  result.awarenessOne = yield* store.getAgentAwarenessSnapshot("thread:side");
  result.pullSnapshot = yield* store.getPullRequestSnapshot({});
  result.pullArchived = yield* store.getPullRequestSnapshot({ threadId: "thread:archived" });
  result.pullUnsettled = yield* store.getPullRequestSnapshot({ unsettledOnly: true });
  result.subagents = yield* store.listActiveSubagentThreadIds();
  result.count = yield* store.countThreads();
  result.activeIndex = yield* store.listActiveShellIndex();
  result.worktrees = yield* store.getThreadWorktreePaths();
  result.allWorktrees = yield* store.getThreadWorktreePaths({ includeArchived: true });
  result.remote = yield* store.getOpenRemoteDelegatedTasks;
  result.pullRequests = yield* store.getThreadsWithPullRequests();
  result.settlement = yield* store.getSettlementCandidates();
  NodeAssert.equal(yield* store.findGuestShareThreadId("missing"), null);
  NodeAssert.equal(yield* store.findGuestShareThreadId("share:fixture"), "thread:idle");
  for (const input of [
    { before: { settledAt: fixedTime }, limit: 10 },
    { before: { settledAt: "2026-11-11T00:00:00.000Z" }, limit: 1 },
    {
      before: { settledAt: "2026-11-09T12:00:00.000Z", threadId: "thread:settled-a" },
      limit: 10,
    },
    { before: { settledAt: "2026-11-11T00:00:00.000Z" }, limit: 0 },
    {
      before: { settledAt: "2026-11-11T00:00:00.000Z" },
      projectIds: ["project:second"],
      limit: 0,
    },
    { before: { settledAt: "2026-11-11T00:00:00.000Z" }, projectIds: [], limit: 10 },
  ])
    result[`older:${JSON.stringify(input)}`] = yield* store.listOlderSettledThreads(input);

  // A failed run's terminal error wins over session errors; held queues are not presented.
  yield* apply(
    makeEvent("run.updated", { ...jsonPayloads.run, status: "failed", completedAt: fixedTime }),
  );
  yield* apply(
    taskEvent("item:error", "error", "failed", 90, {
      failure: {
        class: "unknown",
        message: "fixture provider failure",
        code: null,
        retryable: null,
      },
    }),
  );
  yield* apply(
    makeEvent("run.created", {
      ...jsonPayloads.run,
      id: "run:held",
      ordinal: 2,
      status: "queued",
      queueHeld: true,
    }),
  );
  result.failed = yield* store.getThreadShell("thread:main");
  NodeAssert.equal(result.failed.latestRunId, "run:main");
  result.failedAwareness = yield* store.getAgentAwarenessSnapshot("thread:main");

  // Silent compaction uses JavaScript's full trim set but does not hide a failed compaction.
  yield* apply(
    makeEvent("message.updated", {
      ...jsonPayloads.message,
      id: "message:compact",
      role: "user",
      createdBy: "agent",
      creationSource: "server",
      text: "\u00a0/COMPACT\ufeff",
      runId: "run:compact",
    }),
  );
  yield* apply(
    makeEvent("run.created", {
      ...jsonPayloads.run,
      id: "run:compact",
      ordinal: 3,
      status: "running",
      userMessageId: "message:compact",
    }),
  );
  result.silent = yield* store.getThreadShell("thread:main");
  yield* apply(
    taskEvent("item:compaction", "compaction", "failed", 100, {
      runId: "run:compact",
      driver: null,
    }),
  );
  result.compactionFailed = yield* store.getThreadShell("thread:main");
  yield* apply(
    makeEvent("run.updated", {
      ...jsonPayloads.run,
      id: "run:compact",
      ordinal: 3,
      status: "rolled_back",
      userMessageId: "message:compact",
    }),
  );
  result.rolledBack = yield* store.getThreadShell("thread:main");

  result.backgroundWakes = [];
  for (const [index, [name, source, outcome, expected]] of [
    ["task", { kind: "background_task" }, "updated", true],
    ["monitor", { kind: "monitor" }, "updated", true],
    ["command", { kind: "command" }, "updated", true],
    ["finished", { kind: "command" }, "completed", false],
    ["stopped", { kind: "monitor" }, "cancelled", false],
    ["subagent", { kind: "subagent" }, "updated", false],
    ["delegated", { kind: "background_task" }, "updated", false],
    ["steered", { kind: "background_task" }, "updated", false],
  ].entries()) {
    const threadId = "thread:idle";
    const runId = `run:wake:${name}`;
    const messageId = `message:wake:${name}`;
    yield* apply(
      makeEvent(
        "message.updated",
        {
          ...jsonPayloads.message,
          id: messageId,
          threadId,
          runId: null,
          nodeId: null,
          role: "user",
          createdBy: "agent",
          creationSource: "server",
          text: name,
          streaming: false,
          notification: { source, outcome, summary: name },
          ...(name === "delegated"
            ? { delegatedCompletion: { parentRunId: "run:main", generation: 1, taskIds: [] } }
            : {}),
        },
        { threadId },
      ),
    );
    yield* apply(
      makeEvent(
        "run.created",
        {
          ...jsonPayloads.run,
          id: runId,
          threadId,
          ordinal: index + 10,
          userMessageId: messageId,
          status: "completed",
          completedAt: fixedTime,
        },
        { threadId },
      ),
    );
    if (name === "steered") {
      yield* apply(
        makeEvent(
          "message.updated",
          {
            ...jsonPayloads.message,
            id: "message:wake:follow-up",
            threadId,
            runId,
            nodeId: null,
            role: "user",
            createdBy: "user",
            creationSource: "web",
            text: "also update the fixture",
            streaming: false,
          },
          { threadId },
        ),
      );
    }
    const shell = yield* store.getThreadShell(threadId);
    NodeAssert.equal(shell.latestRunBackgroundWake, expected, name);
    const awareness = yield* store.getAgentAwarenessSnapshot(threadId);
    NodeAssert.equal(awareness.threads[0].latestRunBackgroundWake, expected, name);
    result.backgroundWakes.push({ shell, awareness });
  }

  // A usage-limit failure remains the presented run while a newer turn queues.
  // Its wake flag must follow the failure rather than that queued turn.
  for (const event of [
    makeEvent(
      "run.created",
      {
        ...jsonPayloads.run,
        id: "run:wake:blocked",
        threadId: "thread:idle",
        ordinal: 100,
        rootNodeId: "node:wake:blocked",
        userMessageId: "message:wake:task",
        status: "failed",
        completedAt: fixedTime,
      },
      { threadId: "thread:idle" },
    ),
    makeEvent(
      "turn-item.updated",
      {
        ...jsonPayloads.turnItem,
        id: "item:wake:limit",
        threadId: "thread:idle",
        runId: "run:wake:blocked",
        nodeId: "node:wake:blocked",
        ordinal: 100,
        type: "error",
        status: "failed",
        failure: {
          class: "usage_limit",
          message: "Fixture usage limit",
          code: "usageLimitExceeded",
          retryable: null,
        },
      },
      { threadId: "thread:idle" },
    ),
    makeEvent(
      "run.created",
      {
        ...jsonPayloads.run,
        id: "run:wake:queued",
        threadId: "thread:idle",
        ordinal: 101,
        rootNodeId: "node:wake:queued",
        userMessageId: "message:wake:queued",
        status: "queued",
        startedAt: null,
      },
      { threadId: "thread:idle" },
    ),
  ])
    yield* apply(event);
  result.blockedWake = yield* store.getThreadShell("thread:idle");
  NodeAssert.equal(result.blockedWake.latestRunId, "run:wake:blocked");
  NodeAssert.equal(result.blockedWake.latestRunBackgroundWake, true);
  result.blockedWakeAwareness = yield* store.getAgentAwarenessSnapshot("thread:idle");
  NodeAssert.equal(result.blockedWakeAwareness.threads[0].latestRunBackgroundWake, true);

  const before = normalized(yield* store.getShellSnapshot());
  yield* failed(
    transaction(
      Effect.gen(function* () {
        yield* apply(threadEvent("thread:discarded"));
        NodeAssert.equal(yield* store.countThreads(), result.count + 1);
        NodeAssert.ok((yield* store.getThreadShell("thread:discarded")) !== null);
        return yield* Effect.fail("rollback shell rows");
      }),
    ),
  );
  NodeAssert.deepEqual(normalized(yield* store.getShellSnapshot()), before);
  result.final = before;
  return normalized(result);
});

try {
  const source = NodePath.join(fixture.root, "source.sqlite");
  await createMigratedSqlite(source, (sql) =>
    Effect.gen(function* () {
      const store = yield* oracle.module.ProjectionStoreV2.pipe(
        Effect.provide(oracle.module.layer),
      );
      for (const event of sampleEvents) yield* store.apply(event);
      for (const [id, extra] of [
        [
          "thread:idle",
          {
            worktreePath: "/fixture/one",
            guestShare: {
              id: "share:fixture",
              host: "fixture.test",
              createdAt: fixedTime,
              expiresAt: "2026-11-11T12:00:00.000Z",
              guestName: null,
              messages: [],
            },
          },
        ],
        ["thread:archived", { archivedAt: fixedTime, worktreePath: "/fixture/archive" }],
        ["thread:deleted", { deletedAt: fixedTime }],
        [
          "thread:subagent",
          {
            lineage: {
              parentThreadId: "thread:main",
              relationshipToParent: "subagent",
              rootThreadId: "thread:main",
            },
          },
        ],
        [
          "thread:side",
          {
            lineage: {
              parentThreadId: "thread:main",
              relationshipToParent: "fork",
              rootThreadId: "thread:main",
              sideChat: true,
            },
          },
        ],
        ["thread:settled-a", { settledAt: "2026-11-09T12:00:00.000Z", settledOverride: "settled" }],
        [
          "thread:settled-b",
          {
            settledAt: "2026-11-09T12:00:00.000Z",
            settledOverride: "settled",
            projectId: "project:second",
          },
        ],
        [
          "thread:fork",
          {
            forkedFrom: { type: "run", threadId: "thread:main", runId: "run:main" },
            lineage: {
              parentThreadId: "thread:main",
              relationshipToParent: "fork",
              rootThreadId: "thread:main",
            },
          },
        ],
      ])
        yield* store.apply(threadEvent(id, extra));
      yield* store.apply(
        taskEvent("item:command", "command_execution", "running", 10, {
          input: "fixture",
          output: "BODY".repeat(50000),
        }),
      );
      yield* store.apply(
        taskEvent("item:search", "web_search", "running", 20, { query: "fixture" }),
      );
      yield* store.apply(
        makeEvent("message.updated", {
          ...jsonPayloads.message,
          id: "message:user",
          role: "user",
          createdBy: "user",
          creationSource: "web",
          text: "fixture",
        }),
      );
      yield* sql`INSERT INTO orchestration_events(sequence,event_id,aggregate_kind,stream_id,stream_version,event_type,occurred_at,command_id,causation_event_id,correlation_id,actor_kind,payload_json,metadata_json,application_event_version)
      VALUES(7,'event:shell','thread','thread:main',1,'thread.created',${fixedTime},NULL,NULL,NULL,'user',${JSON.stringify(jsonPayloads.thread)},'{}',2)`;
    }),
  );
  const upstream = NodePath.join(fixture.root, "upstream.sqlite"),
    retained = NodePath.join(fixture.root, "retained.sqlite");
  await NodeFSP.copyFile(source, upstream);
  await NodeFSP.copyFile(source, retained);
  const sourceHash = hash(await NodeFSP.readFile(source));
  const exported = NodePath.join(fixture.root, "source.ndjson");
  exportSqliteState({ source, destination: exported });
  nativeRequest(executable, fixture, "initialize", {
    operation: "import_sqlite_application",
    path: exported,
  });
  const expected = await withServices("upstream", upstream, exercise);
  NodeAssert.deepEqual(
    await withServices("sqlite", retained, exercise),
    expected,
    "Shared shell presentation preserves immutable upstream",
  );
  NodeAssert.deepEqual(
    await withServices("redb", source, exercise),
    expected,
    "Native shell summaries match immutable SQLite",
  );
  checks.push(
    "The complete NativeProjectionStore serves all fourteen shell/query methods with immutable upstream parity",
    "Active/archive/project/family/worktree/list filters, fork inheritance, event snapshot sequence and deferred decode agree",
    "Settlement lists, compact active index, user-authored timestamps and older-settled cursors/counts agree",
    "Held queues, failed roots, silent compaction, rolled-back counts and pending tool work retain source presentation",
    "Background progress wakes, terminal outcomes, delegated work, user steering and blocking failures retain the upstream shell and awareness flag",
    "Shell reads inside a writer observe staged rows and rollback restores all summaries",
  );
  await withServices("redb", source, ({ store }) =>
    Effect.gen(function* () {
      NodeAssert.deepEqual(normalized(yield* store.getShellSnapshot()), expected.final);
    }),
  );
  checks.push("Shell summaries survive native helper restart");
  NodeAssert.equal(hash(await NodeFSP.readFile(source)), sourceHash);
  await finishProof({
    executable,
    fixture,
    proofPath,
    checks,
    sourceHash,
    oracles: [oracle.source],
    sourcePaths: [
      "apps/server/src/orchestration-v2/ProjectionStore.ts",
      "apps/server/src/orchestration-v2/NativeProjectionStore.ts",
      "apps/server/src/orchestration-v2/ProjectionShellRows.ts",
      "apps/server/src/orchestration-v2/SqliteProjectionShellRows.ts",
      "apps/server/src/orchestration-v2/NativeProjectionShellRows.ts",
      "apps/server/src/persistence/redb_projection_shell.rs",
      "apps/server/src/persistence/redb_projection_window_index.rs",
      "tools/check-redb-projection-shells.mjs",
    ],
  });
} finally {
  await fixture.close();
}
