#!/usr/bin/env node
import * as Assert from "node:assert/strict";
import * as FS from "node:fs/promises";
import * as Path from "node:path";
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
  const { text, streaming, ...base } = jsonPayloads.turnItem;
  return makeEvent("turn-item.updated", { ...base, id, type, status, ordinal, ...extra });
};

function exercise({ store, apply, transaction }) {
  return Effect.gen(function* () {
    const result = {};
    result.snapshot = yield* store.getShellSnapshot();
    Assert.equal(result.snapshot.snapshotSequence, 7);
    Assert.equal(result.snapshot.threads.length, 7);
    Assert.equal(result.snapshot.archivedThreads.length, 1);
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
    Assert.equal(yield* store.getThreadShell("thread:deleted"), null);
    Assert.equal(yield* store.getThreadShell("missing"), null);
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
    Assert.equal(yield* store.findGuestShareThreadId("missing"), null);
    Assert.equal(yield* store.findGuestShareThreadId("share:fixture"), "thread:idle");
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
    Assert.equal(result.failed.latestRunId, "run:main");
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

    const before = normalized(yield* store.getShellSnapshot());
    yield* failed(
      transaction(
        Effect.gen(function* () {
          yield* apply(threadEvent("thread:discarded"));
          Assert.equal(yield* store.countThreads(), result.count + 1);
          Assert.ok((yield* store.getThreadShell("thread:discarded")) !== null);
          return yield* Effect.fail("rollback shell rows");
        }),
      ),
    );
    Assert.deepEqual(normalized(yield* store.getShellSnapshot()), before);
    result.final = before;
    return normalized(result);
  });
}

try {
  const source = Path.join(fixture.root, "source.sqlite");
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
  Assert.deepEqual(
    await withServices("sqlite", retained, exercise),
    expected,
    "Shared shell presentation preserves immutable upstream",
  );
  Assert.deepEqual(
    await withServices("redb", source, exercise),
    expected,
    "Native shell summaries match immutable SQLite",
  );
  checks.push(
    "The complete NativeProjectionStore serves all fourteen shell/query methods with immutable upstream parity",
    "Active/archive/project/family/worktree/list filters, fork inheritance, event snapshot sequence and deferred decode agree",
    "Settlement lists, compact active index, user-authored timestamps and older-settled cursors/counts agree",
    "Held queues, failed roots, silent compaction, rolled-back counts and pending tool work retain source presentation",
    "Shell reads inside a writer observe staged rows and rollback restores all summaries",
  );
  await withServices("redb", source, ({ store }) =>
    Effect.gen(function* () {
      Assert.deepEqual(normalized(yield* store.getShellSnapshot()), expected.final);
    }),
  );
  checks.push("Shell summaries survive native helper restart");
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
