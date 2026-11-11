#!/usr/bin/env node
import * as Assert from "node:assert/strict";
import * as FS from "node:fs/promises";
import * as Path from "node:path";
import * as Contracts from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Schema from "effect/Schema";

import * as ProjectionStore from "../apps/server/src/orchestration-v2/ProjectionStore.ts";
import * as NativeProjectionLookups from "../apps/server/src/orchestration-v2/NativeProjectionLookupRows.ts";
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
import {
  jsonPayloads,
  payloads,
  sampleEvents,
  makeEvent,
  fixedTime,
} from "./redb-event-sink-fixtures.mjs";

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
        const store = yield* NativeProjectionLookups.make;
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

function exercise({ store, apply, transaction }) {
  return Effect.gen(function* () {
    const result = {};
    result.thread = yield* store.getThread("thread:main");
    result.deletedThread = yield* store.getThread("thread:deleted");
    result.metadata = yield* store.getThreadLinkMetadata([
      "thread:archived",
      "thread:main",
      "thread:deleted",
      "missing",
      "thread:main",
    ]);
    result.titles = yield* store.getThreadTitles([
      "thread:archived",
      "thread:main",
      "thread:deleted",
      "missing",
    ]);
    Assert.equal(Object.keys(result.metadata).length, 2);
    Assert.equal(result.titles["thread:deleted"], undefined);
    Assert.equal(result.titles["thread:archived"], "Title thread:archived");
    Assert.deepEqual(yield* store.getThreadTitles([]), {});
    Assert.deepEqual(yield* store.getThreadLinkMetadata([]), {});
    result.plan = yield* store.getPlan("thread:main", "plan:main");
    Assert.equal(yield* store.getPlan("thread:other", "plan:main"), undefined);
    Assert.equal(yield* store.getPlan("thread:main", "missing"), undefined);
    Assert.equal(
      (yield* store.getPlan("missing", "plan:main").pipe(Effect.flip))._tag,
      "ProjectionStoreThreadNotFoundError",
    );
    result.request = yield* store.getRuntimeRequest("thread:main", "request:main");
    Assert.equal(yield* store.getRuntimeRequest("thread:other", "request:main"), undefined);
    Assert.equal(
      (yield* store.getRuntimeRequest("missing", "request:main").pipe(Effect.flip))._tag,
      "ProjectionStoreThreadNotFoundError",
    );
    result.item = yield* store.getTurnItem({ threadId: "thread:main", itemId: "item:main" });
    Assert.equal(yield* store.getTurnItem({ threadId: "missing", itemId: "item:main" }), null);
    result.messages = [
      ...(yield* store.getMessagesById("thread:main", [
        "message:second",
        "message:main",
        "message:second",
        "missing",
        "message:foreign",
      ])),
    ];
    Assert.deepEqual(
      result.messages.map(([id]) => id),
      ["message:second", "message:main"],
    );
    Assert.equal((yield* store.getMessagesById("missing", ["message:main"])).size, 0);
    Assert.equal((yield* store.getMessagesById("thread:main", [])).size, 0);
    Assert.equal(yield* store.isNodeStored(payloads.node), true);
    Assert.equal(yield* store.isNodeStored({ ...payloads.node, status: "failed" }), false);
    Assert.equal(
      yield* store.hasUnpairedRunInterruptRequest("thread:main", "item:main", "missing"),
      true,
    );
    Assert.equal(
      yield* store.hasUnpairedRunInterruptRequest("thread:main", "item:main", "item:main"),
      false,
    );
    Assert.equal(
      yield* store.hasUnpairedRunInterruptRequest("thread:other", "item:main", "missing"),
      false,
    );
    result.messageCount = yield* store.getMessageCount("thread:main");
    result.subagentCount = yield* store.getSubagentCount("thread:main");
    result.nextOrdinal = yield* store.getNextTurnItemOrdinal("thread:main");
    Assert.equal(result.messageCount, 2);
    Assert.equal(result.subagentCount, 1);
    Assert.equal(result.nextOrdinal, 1_000_002);
    Assert.equal(yield* store.getMessageCount("missing"), 0);
    Assert.equal(yield* store.getSubagentCount("missing"), 0);
    Assert.equal(yield* store.getNextTurnItemOrdinal("missing"), 1);
    const { text, streaming, ...itemBase } = jsonPayloads.turnItem;
    yield* apply(
      makeEvent("turn-item.updated", {
        ...itemBase,
        id: "item:history",
        type: "command_execution",
        input: "fixture",
        ordinal: 77,
      }),
    );
    result.turnStartHistory = yield* store.getTurnStartHistory("thread:main");
    Assert.deepEqual(
      result.turnStartHistory.map((item) => item.id),
      ["item:history"],
    );
    Assert.deepEqual(yield* store.getTurnStartHistory("thread:main", []), []);
    Assert.deepEqual(yield* store.getTurnStartHistory("missing"), []);
    Assert.deepEqual(yield* store.getTurnStartHistory("thread:main", ["run:other"]), []);
    result.runMessage = yield* store.getRunMessage("thread:main", "run:main");
    Assert.equal(result.runMessage.id, "message:main");
    Assert.equal(yield* store.getRunMessage("thread:other", "run:main"), undefined);
    Assert.equal(
      (yield* store.getRunMessage("missing", "run:main").pipe(Effect.flip))._tag,
      "ProjectionStoreThreadNotFoundError",
    );
    Assert.equal(yield* store.canStartQueuedRun("thread:main"), false);
    Assert.equal(
      (yield* store.canStartQueuedRun("missing").pipe(Effect.flip))._tag,
      "ProjectionStoreThreadNotFoundError",
    );
    yield* apply(
      makeEvent("run.created", {
        ...jsonPayloads.run,
        id: "run:held",
        ordinal: 3,
        status: "queued",
        queueHeld: true,
      }),
    );
    yield* apply(
      makeEvent("run.created", {
        ...jsonPayloads.run,
        id: "run:ready",
        ordinal: 2,
        status: "queued",
        queueHeld: false,
      }),
    );
    Assert.equal(yield* store.canStartQueuedRun("thread:main"), false);
    yield* apply(
      makeEvent("run.updated", {
        ...jsonPayloads.run,
        status: "completed",
        completedAt: fixedTime,
      }),
    );
    Assert.equal(yield* store.canStartQueuedRun("thread:main"), true);
    result.held = yield* store.getHeldQueuedRuns("thread:main");
    Assert.deepEqual(
      result.held.map((run) => run.id),
      ["run:held"],
    );
    for (const [id, child, startedAt] of [
      ["task:c", "thread:child", fixedTime],
      ["task:a", "thread:first", null],
      ["task:b", "thread:child", fixedTime],
    ]) {
      yield* apply(
        makeEvent("subagent.updated", {
          ...jsonPayloads.subagent,
          id,
          origin: "app_owned",
          childThreadId: child,
          startedAt,
        }),
      );
    }
    result.children = yield* store.listAppOwnedChildThreadIds("thread:main");
    Assert.deepEqual(result.children, ["thread:first", "thread:child"]);
    Assert.equal(yield* store.getSubagentCount("thread:main"), 4);
    result.missingThreadTag = (yield* store.getThread("missing").pipe(Effect.flip))._tag;
    yield* transaction(
      Effect.gen(function* () {
        yield* apply(threadEvent("thread:written"));
        Assert.equal((yield* store.getThread("thread:written")).title, "Title thread:written");
        yield* failed(
          transaction(
            Effect.gen(function* () {
              yield* apply(threadEvent("thread:discarded"));
              yield* apply(
                makeEvent("message.updated", { ...jsonPayloads.message, id: "message:discarded" }),
              );
              Assert.equal(yield* store.getMessageCount("thread:main"), 3);
              yield* apply(
                makeEvent("turn-item.updated", {
                  ...jsonPayloads.turnItem,
                  id: "item:discarded",
                  ordinal: 1_000_010,
                }),
              );
              Assert.equal(yield* store.getNextTurnItemOrdinal("thread:main"), 1_000_011);
              return yield* Effect.fail("rollback inner projection");
            }),
          ),
        );
        Assert.equal(
          (yield* store.getThread("thread:discarded").pipe(Effect.flip))._tag,
          "ProjectionStoreThreadNotFoundError",
        );
        Assert.equal(yield* store.getMessageCount("thread:main"), 2);
        Assert.equal(yield* store.getNextTurnItemOrdinal("thread:main"), 1_000_002);
      }),
    );
    result.written = yield* store.getThread("thread:written");
    yield* failed(
      transaction(
        Effect.gen(function* () {
          yield* apply(threadEvent("thread:root-discarded"));
          return yield* Effect.fail("rollback root projection");
        }),
      ),
    );
    Assert.equal(
      (yield* store.getThread("thread:root-discarded").pipe(Effect.flip))._tag,
      "ProjectionStoreThreadNotFoundError",
    );
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
      for (const [id, extra] of [
        ["thread:other", {}],
        ["thread:archived", { archivedAt: fixedTime }],
        ["thread:deleted", { deletedAt: fixedTime }],
      ]) {
        yield* store.apply(threadEvent(id, extra));
      }
      yield* store.apply(
        makeEvent("message.updated", { ...jsonPayloads.message, id: "message:second" }),
      );
      yield* store.apply(
        makeEvent(
          "message.updated",
          { ...jsonPayloads.message, id: "message:foreign", threadId: "thread:other" },
          { threadId: "thread:other" },
        ),
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
    "Shared lookup semantics preserve immutable SQLite behavior",
  );
  Assert.deepEqual(
    await withServices("redb", source, exercise),
    expected,
    "Native lookups match immutable SQLite behavior",
  );
  checks.push(
    "All seventeen public lookup methods match immutable upstream and retained SQLite",
    "Read-only lookups preserve missing owner, foreign-owner record, null/undefined and archived/deleted distinctions",
    "Requested message order and duplicate elimination match the original json_each query",
    "Node identity compares exact encoded payload and interrupt pairing stays scoped to its thread",
    "Counts, next ordinal, child task order, run messages and held/active queue selection preserve source behavior",
    "Nested and root transaction rollback restore lookup results with no cross-store fallback",
  );
  await withServices("redb", source, ({ store }) =>
    Effect.gen(function* () {
      Assert.deepEqual(normalized(yield* store.getThread("thread:written")), expected.written);
      Assert.equal(
        (yield* store.getThread("thread:discarded").pipe(Effect.flip))._tag,
        "ProjectionStoreThreadNotFoundError",
      );
    }),
  );
  checks.push("A reopened helper preserves committed projection lookup state");
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
      "apps/server/src/orchestration-v2/ProjectionLookupRows.ts",
      "apps/server/src/orchestration-v2/SqliteProjectionLookupRows.ts",
      "apps/server/src/orchestration-v2/NativeProjectionLookupRows.ts",
      "apps/server/src/persistence/redb_projection_lookup.rs",
      "apps/server/src/persistence/redb_projection_read_index.rs",
      "apps/server/src/persistence/redb_projection_window_index.rs",
      "tools/check-redb-projection-lookups.mjs",
    ],
  });
} finally {
  await fixture.close();
}
