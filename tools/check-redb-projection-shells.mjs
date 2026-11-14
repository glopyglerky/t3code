#!/usr/bin/env node
import * as NodeAssert from "node:assert/strict";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Tracer from "effect/Tracer";

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
import { compareProjectionCounts } from "./redb-projection-count-fixture.mjs";

const [executable, proofPath] = process.argv.slice(2);
NodeAssert.ok(
  executable && NodePath.isAbsolute(executable),
  "Pass an absolute native database executable",
);
await NodeFSP.access(executable, NodeFSP.constants.X_OK);
const oracle = await loadUpstreamModule("apps/server/src/orchestration-v2/ProjectionStore.ts");
const fixture = await createFixture();
const checks = [];
const failed = Effect.fn("checkRedbProjectionShells.failed")((body) =>
  Effect.exit(body).pipe(
    Effect.tap((exit) => Effect.sync(() => NodeAssert.ok(Exit.isFailure(exit)))),
  ),
);

async function withServices(kind, filename, use, target = fixture) {
  if (kind === "redb")
    return withNative(executable, target, (client) =>
      Effect.gen(function* () {
        const store = yield* NativeProjectionStore.make;
        return yield* use({
          store,
          apply: store.apply,
          transaction: client.withTransaction,
        });
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
        sql,
      });
    }),
  );
}

const shellIds = (snapshot) =>
  [...snapshot.threads, ...snapshot.archivedThreads].map((thread) => thread.id).sort();
const family = (root, child = false) => ({
  parentThreadId: child ? root : null,
  relationshipToParent: child ? "subagent" : null,
  rootThreadId: root,
});
const indexedThread = (id, extra = {}) => ({
  ...jsonPayloads.thread,
  id,
  title: id,
  worktreePath: "/fixture/indexed",
  lineage: family(id),
  ...extra,
});
const archiveCases = [
  ["thread:column-archived", null, fixedTime, false],
  ["thread:payload-archived", fixedTime, null, false],
  ["thread:column-child", null, fixedTime, true],
  ["thread:payload-child", fixedTime, null, true],
];
const seedIndexed = (malformed) =>
  Effect.fn("checkRedbProjectionShells.seedIndexed")(function* (sql) {
    const store = yield* oracle.module.ProjectionStoreV2.pipe(Effect.provide(oracle.module.layer));
    for (const [id, extra] of [
      ["thread:family-root", {}],
      ["thread:family-child", { lineage: family("thread:family-root", true) }],
      [
        "thread:family-other-path",
        {
          lineage: family("thread:family-root", true),
          worktreePath: "/fixture/other",
        },
      ],
      ["thread:other-root", {}],
      ["thread:deleted-root", { deletedAt: fixedTime, worktreePath: "/fixture/deleted" }],
      [
        "thread:live-descendant",
        {
          lineage: family("thread:deleted-root", true),
          worktreePath: "/fixture/deleted",
        },
      ],
    ])
      yield* store.apply(makeEvent("thread.created", indexedThread(id, extra), { threadId: id }));
    for (const [id, payloadArchive, columnArchive, child] of archiveCases) {
      yield* store.apply(
        makeEvent(
          "thread.created",
          indexedThread(id, {
            archivedAt: payloadArchive,
            settledAt: fixedTime,
            settledOverride: "settled",
            lineage: family(child ? "thread:family-root" : id, child),
            pullRequests: [
              {
                host: "github.com",
                repository: "fixture/shell",
                number: 1,
                url: "https://github.com/fixture/shell/pull/1",
                source: "manual",
                linkedAt: fixedTime,
                snapshot: null,
                stack: null,
              },
            ],
          }),
          { threadId: id },
        ),
      );
      yield* sql`UPDATE orchestration_v2_projection_threads SET archived_at = ${columnArchive}
        WHERE thread_id = ${id}`;
    }
    if (malformed) {
      yield* store.apply(
        makeEvent("thread.created", indexedThread("thread:malformed"), {
          threadId: "thread:malformed",
        }),
      );
      yield* sql`UPDATE orchestration_v2_projection_threads SET payload_json = '{malformed'
        WHERE thread_id = 'thread:malformed'`;
    }
  });

const assertIndexedPlans = Effect.fn("checkRedbProjectionShells.assertIndexedPlans")(function* ({
  store,
  sql,
}) {
  if (sql === undefined) return;
  for (const [options, index] of [
    [{ worktreePath: "/fixture/indexed" }, "orchestration_v2_projection_threads_worktree_path_idx"],
    [{ familyOf: "thread:family-root" }, "orchestration_v2_projection_threads_root_thread_idx"],
  ]) {
    const statements = [];
    const tracer = Tracer.make({
      span(options) {
        const span = new Tracer.NativeSpan(options);
        const end = span.end.bind(span);
        span.end = (time, exit) => {
          end(time, exit);
          const statement = span.attributes.get("db.query.text");
          if (typeof statement === "string") statements.push(statement);
        };
        return span;
      },
    });
    yield* store.getShellSnapshot(options).pipe(Effect.withTracer(tracer));
    const threadQuery = statements.find((query) => query.includes("AS latest_run_stop_requested"));
    NodeAssert.ok(threadQuery, "Shell thread selection must be traced");
    const plan = (yield* sql.unsafe(`EXPLAIN QUERY PLAN ${threadQuery}`, [])).map(
      (row) => row.detail,
    );
    NodeAssert.ok(
      plan.some((detail) => detail.includes(index)),
      plan.join("\n"),
    );
    NodeAssert.ok(!plan.includes("SCAN t"), plan.join("\n"));
    const counts = statements.filter((query) =>
      query.includes("FROM orchestration_v2_projection_turn_item_counts"),
    );
    NodeAssert.ok(counts.length > 0, "Shell item count selection must be traced");
    for (const query of counts) {
      const countPlan = (yield* sql.unsafe(`EXPLAIN QUERY PLAN ${query}`, [])).map(
        (row) => row.detail,
      );
      for (const detail of countPlan)
        if (/^SEARCH (r|run) /.test(detail))
          NodeAssert.ok(detail.includes("COVERING INDEX"), detail);
    }
  }
});

const exerciseIndexed = (malformed) =>
  Effect.fn("checkRedbProjectionShells.exerciseIndexed")(function* (services) {
    const { store, apply, transaction } = services;
    const result = {};
    const queries = [
      { familyOf: "thread:family-root" },
      { familyOf: "thread:family-child" },
      { familyOf: "thread:deleted-root" },
      { familyOf: "missing" },
      { worktreePath: "/fixture/indexed" },
      { worktreePath: "/fixture/missing" },
      { familyOf: "thread:family-root", worktreePath: "/fixture/indexed" },
      {
        familyOf: "thread:family-root",
        worktreePath: "/fixture/indexed",
        threadIds: [
          "thread:family-root",
          "thread:family-child",
          "thread:family-child",
          "thread:other-root",
          "missing",
        ],
      },
      { familyOf: "thread:family-root", threadIds: [] },
      { familyOf: "thread:family-root", projectId: "project:missing" },
    ];
    for (const options of queries)
      result[JSON.stringify(options)] = yield* store.getShellSnapshot(options);
    NodeAssert.deepEqual(shellIds(result[JSON.stringify(queries[2])]), ["thread:live-descendant"]);
    NodeAssert.deepEqual(shellIds(result[JSON.stringify(queries[3])]), []);
    NodeAssert.deepEqual(shellIds(result[JSON.stringify(queries[5])]), []);
    NodeAssert.deepEqual(shellIds(result[JSON.stringify(queries[7])]), [
      "thread:family-child",
      "thread:family-root",
    ]);
    NodeAssert.deepEqual(shellIds(result[JSON.stringify(queries[8])]), []);
    NodeAssert.deepEqual(shellIds(result[JSON.stringify(queries[9])]), []);
    for (const snapshot of Object.values(result))
      for (const rows of [snapshot.threads, snapshot.archivedThreads]) {
        const ids = rows.map((row) => row.id);
        NodeAssert.deepEqual(ids, [...ids].sort(), "Equal timestamps retain thread-ID ordering");
      }
    yield* assertIndexedPlans(services);
    if (malformed) {
      for (const options of [
        { familyOf: "thread:malformed" },
        { threadIds: ["thread:malformed"] },
        {},
      ])
        yield* failed(store.getShellSnapshot(options));
      return normalized(result);
    }
    result.active = yield* store.getShellSnapshot({ location: "active" });
    result.archive = yield* store.getShellSnapshot({ location: "archive" });
    result.activeIndex = yield* store.listActiveShellIndex();
    result.pull = yield* store.getPullRequestSnapshot({});
    result.worktrees = yield* store.getThreadWorktreePaths();
    result.pullThreads = yield* store.getThreadsWithPullRequests();
    result.subagents = yield* store.listActiveSubagentThreadIds();
    result.older = yield* store.listOlderSettledThreads({
      before: { settledAt: "2026-11-11T00:00:00.000Z" },
      limit: 20,
    });
    for (const [id, payloadArchive, columnArchive, child] of archiveCases) {
      NodeAssert.equal(shellIds(result.active).includes(id), columnArchive === null);
      NodeAssert.equal(shellIds(result.archive).includes(id), columnArchive !== null);
      NodeAssert.equal(
        result.activeIndex.some((row) => row.id === id),
        columnArchive === null,
      );
      NodeAssert.equal(
        result.pull.threads.some((row) => row.id === id),
        columnArchive === null,
      );
      NodeAssert.equal(
        result.worktrees.some((row) => row.threadId === id),
        payloadArchive === null,
      );
      NodeAssert.equal(
        result.pullThreads.some((row) => row.id === id),
        payloadArchive === null,
      );
      NodeAssert.equal(result.subagents.includes(id), child && payloadArchive === null);
      NodeAssert.equal(result.older.threadIds.includes(id), !child && payloadArchive === null);
    }
    const moved = indexedThread("thread:family-child", {
      lineage: family("thread:other-root", true),
      worktreePath: "/fixture/moved",
    });
    yield* transaction(
      Effect.gen(function* () {
        yield* apply(makeEvent("thread.metadata-updated", moved, { threadId: moved.id }));
        NodeAssert.deepEqual(
          shellIds(
            yield* store.getShellSnapshot({
              familyOf: "thread:family-root",
              threadIds: [moved.id],
            }),
          ),
          [],
        );
        NodeAssert.deepEqual(
          shellIds(
            yield* store.getShellSnapshot({
              familyOf: "thread:other-root",
              worktreePath: "/fixture/moved",
            }),
          ),
          [moved.id],
        );
        yield* failed(
          transaction(
            Effect.gen(function* () {
              yield* apply(
                makeEvent(
                  "thread.metadata-updated",
                  { ...moved, worktreePath: "/fixture/nested" },
                  { threadId: moved.id },
                ),
              );
              NodeAssert.deepEqual(
                shellIds(
                  yield* store.getShellSnapshot({
                    worktreePath: "/fixture/nested",
                  }),
                ),
                [moved.id],
              );
              return yield* Effect.fail("rollback shell index change");
            }),
          ),
        );
        NodeAssert.deepEqual(
          shellIds(yield* store.getShellSnapshot({ worktreePath: "/fixture/nested" })),
          [],
        );
        NodeAssert.deepEqual(
          shellIds(yield* store.getShellSnapshot({ worktreePath: "/fixture/moved" })),
          [moved.id],
        );
      }),
    );
    yield* failed(
      transaction(
        Effect.gen(function* () {
          yield* apply(
            makeEvent("thread.deleted", { ...moved, deletedAt: fixedTime }, { threadId: moved.id }),
          );
          NodeAssert.deepEqual(
            shellIds(yield* store.getShellSnapshot({ worktreePath: "/fixture/moved" })),
            [],
          );
          return yield* Effect.fail("rollback shell index deletion");
        }),
      ),
    );
    result.committed = yield* store.getShellSnapshot({
      familyOf: "thread:other-root",
      worktreePath: "/fixture/moved",
    });
    NodeAssert.deepEqual(shellIds(result.committed), [moved.id]);
    return normalized(result);
  });

async function compareIndexed(malformed) {
  const target = await createFixture();
  try {
    const source = NodePath.join(target.root, "source.sqlite");
    await createMigratedSqlite(source, seedIndexed(malformed));
    const sourceHash = hash(await NodeFSP.readFile(source));
    const upstream = NodePath.join(target.root, "upstream.sqlite");
    const retained = NodePath.join(target.root, "retained.sqlite");
    await NodeFSP.copyFile(source, upstream);
    await NodeFSP.copyFile(source, retained);
    const archive = NodePath.join(target.root, "source.ndjson");
    exportSqliteState({ source, destination: archive });
    nativeRequest(executable, target, "initialize", {
      operation: "import_sqlite_application",
      path: archive,
    });
    const expected = await withServices("upstream", upstream, exerciseIndexed(malformed), target);
    NodeAssert.deepEqual(
      await withServices("sqlite", retained, exerciseIndexed(malformed), target),
      expected,
      "Extracted shell queries retain exact indexed upstream behavior",
    );
    NodeAssert.deepEqual(
      await withServices("redb", source, exerciseIndexed(malformed), target),
      expected,
      "Native indexed shell reads agree with immutable upstream",
    );
    if (!malformed)
      await withServices(
        "redb",
        source,
        ({ store }) =>
          Effect.gen(function* () {
            NodeAssert.deepEqual(
              normalized(
                yield* store.getShellSnapshot({
                  familyOf: "thread:other-root",
                  worktreePath: "/fixture/moved",
                }),
              ),
              expected.committed,
            );
          }),
        target,
      );
    NodeAssert.equal(hash(await NodeFSP.readFile(source)), sourceHash);
  } finally {
    await target.close();
  }
}
const threadEvent = (id, extra = {}) =>
  makeEvent(
    "thread.created",
    {
      ...jsonPayloads.thread,
      id,
      title: `Title ${id}`,
      lineage: {
        parentThreadId: null,
        relationshipToParent: null,
        rootThreadId: id,
      },
      ...extra,
    },
    { threadId: id },
  );
const awarenessSnooze = {
  snoozedAt: "2026-11-10T11:00:00.000Z",
  snoozedUntil: "2026-11-10T13:00:00.000Z",
};
const taskEvent = (id, type, status, ordinal, extra = {}) => {
  const { text: _text, streaming: _streaming, ...base } = jsonPayloads.turnItem;
  return makeEvent("turn-item.updated", {
    ...base,
    id,
    type,
    status,
    ordinal,
    ...extra,
  });
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
  const awareness = normalized(result.awareness);
  const idleAwareness = awareness.threads.find((thread) => thread.id === "thread:idle");
  const sideAwareness = normalized(result.awarenessOne).threads[0];
  const mainAwareness = awareness.threads.find((thread) => thread.id === "thread:main");
  NodeAssert.ok(idleAwareness && sideAwareness && mainAwareness);
  for (const thread of [idleAwareness, sideAwareness]) {
    NodeAssert.equal(thread.snoozedAt, awarenessSnooze.snoozedAt);
    NodeAssert.equal(thread.snoozedUntil, awarenessSnooze.snoozedUntil);
  }
  NodeAssert.equal(idleAwareness.pendingGuestMessageCount, 2);
  NodeAssert.equal(sideAwareness.pendingGuestMessageCount, 0);
  NodeAssert.equal(mainAwareness.pendingGuestMessageCount, 0);
  NodeAssert.equal(mainAwareness.snoozedAt, null);
  NodeAssert.equal(mainAwareness.snoozedUntil, null);
  result.pullSnapshot = yield* store.getPullRequestSnapshot({});
  result.pullArchived = yield* store.getPullRequestSnapshot({
    threadId: "thread:archived",
  });
  result.pullUnsettled = yield* store.getPullRequestSnapshot({
    unsettledOnly: true,
  });
  result.subagents = yield* store.listActiveSubagentThreadIds();
  result.count = yield* store.countThreads();
  result.activeIndex = yield* store.listActiveShellIndex();
  result.activeThreads = yield* store.listActiveThreads();
  result.projectActiveThreads = yield* store.listActiveThreads("project:second");
  result.missingProjectActiveThreads = yield* store.listActiveThreads("project:missing");
  NodeAssert.ok(result.activeThreads.some((row) => row.id === "thread:subagent"));
  NodeAssert.ok(result.activeThreads.some((row) => row.id === "thread:side"));
  NodeAssert.ok(
    !result.activeThreads.some(
      (row) => row.id === "thread:deleted" || row.id === "thread:archived",
    ),
  );
  NodeAssert.deepEqual(
    result.projectActiveThreads.map((row) => row.id),
    ["thread:settled-b"],
  );
  NodeAssert.deepEqual(result.missingProjectActiveThreads, []);
  result.worktrees = yield* store.getThreadWorktreePaths();
  result.allWorktrees = yield* store.getThreadWorktreePaths({
    includeArchived: true,
  });
  result.remote = yield* store.getOpenRemoteDelegatedTasks;
  result.pullRequests = yield* store.getThreadsWithPullRequests();
  result.settlement = yield* store.getSettlementCandidates();
  NodeAssert.equal(yield* store.findGuestShareThreadId("missing"), null);
  NodeAssert.equal(yield* store.findGuestShareThreadId("share:fixture"), "thread:idle");
  for (const input of [
    { before: { settledAt: fixedTime }, limit: 10 },
    { before: { settledAt: "2026-11-11T00:00:00.000Z" }, limit: 1 },
    {
      before: {
        settledAt: "2026-11-09T12:00:00.000Z",
        threadId: "thread:settled-a",
      },
      limit: 10,
    },
    { before: { settledAt: "2026-11-11T00:00:00.000Z" }, limit: 0 },
    {
      before: { settledAt: "2026-11-11T00:00:00.000Z" },
      projectIds: ["project:second"],
      limit: 0,
    },
    {
      before: { settledAt: "2026-11-11T00:00:00.000Z" },
      projectIds: [],
      limit: 10,
    },
  ])
    result[`older:${JSON.stringify(input)}`] = yield* store.listOlderSettledThreads(input);

  // A failed run's terminal error wins over session errors; held queues are not presented.
  yield* apply(
    makeEvent("run.updated", {
      ...jsonPayloads.run,
      status: "failed",
      completedAt: fixedTime,
    }),
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

  // A live fork still inherits run state after its source is deleted.
  const beforeSourceDelete = yield* store.getThreadShell("thread:fork");
  yield* apply(
    makeEvent(
      "thread.deleted",
      {
        ...jsonPayloads.thread,
        deletedAt: fixedTime,
      },
      { id: "event:delete-fork-source" },
    ),
  );
  NodeAssert.equal(yield* store.getThreadShell("thread:main"), null);
  result.forkAfterSourceDelete = yield* store.getThreadShell("thread:fork");
  NodeAssert.deepEqual(result.forkAfterSourceDelete, beforeSourceDelete);
  result.afterSourceDelete = yield* store.getShellSnapshot();
  NodeAssert.ok(!result.afterSourceDelete.threads.some((thread) => thread.id === "thread:main"));
  const countBeforeRollback = yield* store.countThreads();
  const before = normalized(yield* store.getShellSnapshot());
  yield* failed(
    transaction(
      Effect.gen(function* () {
        yield* apply(threadEvent("thread:discarded"));
        NodeAssert.equal(yield* store.countThreads(), countBeforeRollback + 1);
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
            ...awarenessSnooze,
            guestShare: {
              id: "share:fixture",
              host: "fixture.test",
              createdAt: fixedTime,
              expiresAt: "2026-11-11T12:00:00.000Z",
              guestName: null,
              messages: ["pending", "sent", "dismissed", "pending"].map((status, index) => ({
                id: `guest-message:awareness:${index}`,
                text: `Guest message ${index}`,
                status,
                submittedAt: fixedTime,
              })),
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
            ...awarenessSnooze,
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
            forkedFrom: {
              type: "run",
              threadId: "thread:main",
              runId: "run:main",
            },
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
        taskEvent("item:search", "web_search", "running", 20, {
          query: "fixture",
        }),
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
    "The complete NativeProjectionStore serves all fifteen shell/query methods with immutable upstream parity",
    "Active/archive/project/family/worktree/list filters, fork inheritance, event snapshot sequence and deferred decode agree",
    "Settlement lists, compact active index, user-authored timestamps and older-settled cursors/counts agree",
    "Held queues, failed roots, silent compaction, rolled-back counts and pending tool work retain source presentation",
    "Shell reads inside a writer observe staged rows and rollback restores all summaries",
    "Global and per-thread awareness preserve snooze timestamps, null defaults and only pending guest-message counts for relay decisions",
  );
  await withServices("redb", source, ({ store }) =>
    Effect.gen(function* () {
      NodeAssert.deepEqual(normalized(yield* store.getShellSnapshot()), expected.final);
    }),
  );
  checks.push("Shell summaries survive native helper restart");
  await compareIndexed(false);
  await compareIndexed(true);
  const countProof = await compareProjectionCounts(executable);
  checks.push(
    "Indexed family/worktree intersections preserve duplicate and missing IDs, deleted roots, empty filters and stable ordering",
    "Archive-column shell/index/pull-request filters and retained JSON-filter APIs agree despite deliberately inconsistent imported archive fields",
    "Unrelated malformed payloads are excluded before narrow shell decoding; malformed selected or family-reference rows still fail",
    "SQLite worktree/family reads use expression indexes and shell counts use covering run-status indexes",
    "Family/worktree index changes follow staged updates, nested rollback, committed movement, deletion rollback and helper restart",
    "Canonical aggregate counts preserve global cross-thread queued subtraction, signed results, dangling runs, NULL/empty keys, duplicate JSON members and unrelated malformed reasoning payloads",
  );
  NodeAssert.equal(hash(await NodeFSP.readFile(source)), sourceHash);
  await finishProof({
    executable,
    fixture,
    proofPath,
    checks,
    sourceHash,
    oracles: [oracle.source, countProof.migrationSource],
    sourcePaths: [
      "apps/server/src/orchestration-v2/ProjectionStore.ts",
      "apps/server/src/orchestration-v2/NativeProjectionStore.ts",
      "apps/server/src/orchestration-v2/ProjectionShellRows.ts",
      "apps/server/src/orchestration-v2/SqliteProjectionShellRows.ts",
      "apps/server/src/orchestration-v2/NativeProjectionShellRows.ts",
      "apps/server/src/persistence/redb_projection_shell.rs",
      "apps/server/src/persistence/redb_projection_window_index.rs",
      "apps/server/src/persistence/redb_projection_read_index.rs",
      "apps/server/src/persistence/redb_projection_derived_indexes.rs",
      "apps/server/src/persistence/redb_turn_item_counts.rs",
      "apps/server/src/persistence/redb_schema_upgrade.rs",
      "tools/redb-projection-count-fixture.mjs",
      "tools/check-redb-projection-shells.mjs",
    ],
  });
} finally {
  await fixture.close();
}
