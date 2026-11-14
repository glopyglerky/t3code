import * as NodeAssert from "node:assert/strict";
import * as NodeFSP from "node:fs/promises";
import * as NodeModule from "node:module";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Schema from "effect/Schema";

import * as NativeProjectionStore from "../apps/server/src/orchestration-v2/NativeProjectionStore.ts";
import { createFixture } from "./database-fixture.mjs";
import { exportSqliteState } from "./export-sqlite-state.mjs";
import {
  createMigratedSqlite,
  hash,
  loadUpstreamModule,
  nativeRequest,
  normalized,
  withNative,
  withSqlite,
} from "./redb-domain-fixture.mjs";
import { fixedTime, jsonPayloads, makeEvent } from "./redb-event-sink-fixtures.mjs";
import { upstreamSource } from "./upstream-source.mjs";

const sourcePath = "apps/server/src/orchestration-v2/ProjectionStore.ts";
const original = upstreamSource(sourcePath);
const encoder = original.match(/^const encodeIdList = .+;$/m)?.[0];
const declarations = original.match(
  /    const selectShellRunItemCounts = [\s\S]+?(?=    const selectShellProviderThreadRows =)/,
)?.[0];
NodeAssert.ok(encoder && declarations, "The pinned upstream count declarations must exist");
// Execute the actual pinned declarations, including their ID encoder. The
// fixture supplies only their two runtime dependencies; it does not copy SQL.
const makeOracleQueries = new Function(
  "sql",
  "Schema",
  `${NodeModule.stripTypeScriptTypes(`${encoder}\n${declarations}`, { mode: "strip" })}
  return { selectShellItemCounts, selectShellRunItemCounts };`,
);
const oracle = await loadUpstreamModule(sourcePath);
const migrationOracle = await loadUpstreamModule(
  "apps/server/src/persistence/Migrations/085_TurnItemCounts.ts",
);
const threadA = "thread:counts-a";
const threadB = "thread:counts-b";
const owner = "thread:counts-owner";
const ids = [threadB, threadA, "thread:counts-zero", "thread:counts-empty"];
const maintenanceCases = [
  { name: "zero", count: 0, insert: true },
  { name: "negative-one", count: -1, insert: true },
  { name: "negative-three", count: -3, insert: true },
  { name: "missing-insert", count: null, insert: true },
  { name: "missing-old", count: null, insert: false },
];
const nullableCases = [
  { name: "null-to-empty", old: null, next: "", expected: 1 },
  { name: "empty-to-null", old: "", next: null, expected: 1 },
  { name: "null-to-null", old: null, next: null, expected: 0 },
  { name: "empty-to-empty", old: "", next: "", expected: 0 },
];
const edgeThread = (name) => `thread:count-edge-${name}`;
const edgeIds = [...maintenanceCases, ...nullableCases].map(({ name }) => edgeThread(name));
const runId = (name) => `run:counts-${name}`;
const runPayload = (name, status, ordinal) => ({
  ...jsonPayloads.run,
  id: runId(name),
  threadId: owner,
  ordinal,
  rootNodeId: `node:counts-${name}`,
  activeAttemptId: `attempt:counts-${name}`,
  userMessageId: `message:counts-${name}`,
  status,
  completedAt: status === "queued" ? null : fixedTime,
});
const itemPayload = (id, threadId, run, queued = false) => ({
  ...jsonPayloads.turnItem,
  id: `item:counts-${id}`,
  threadId,
  runId: run,
  nodeId: null,
  providerThreadId: null,
  providerTurnId: null,
  status: "completed",
  completedAt: fixedTime,
  streaming: false,
  ...(queued
    ? {
        type: "user_message",
        messageId: `message:counts-${id}`,
        createdBy: "user",
        creationSource: "web",
        inputIntent: "queued_turn",
        attachments: [],
      }
    : {}),
});
const updateItem = (payload) =>
  makeEvent("turn-item.updated", payload, { threadId: payload.threadId });
const seed = Effect.fn("projectionCountFixture.seed")(function* (sql, malformed) {
  const store = yield* oracle.module.ProjectionStoreV2.pipe(Effect.provide(oracle.module.layer));
  for (const [name, status, ordinal] of [
    ["queued", "queued", 1],
    ["cancelled", "cancelled", 2],
    ["rolled-cancelled", "rolled_back", 3],
    ["rolled-completed", "rolled_back", 4],
    ["completed", "completed", 5],
  ]) {
    yield* store.apply(
      makeEvent("run.created", runPayload(name, status, ordinal), { threadId: owner }),
    );
    if (name.startsWith("rolled-"))
      yield* store.apply(
        makeEvent(
          "run-attempt.created",
          {
            ...jsonPayloads.attempt,
            id: `attempt:counts-${name}`,
            runId: runId(name),
            rootNodeId: `node:counts-${name}`,
            status: name === "rolled-cancelled" ? "cancelled" : "completed",
            completedAt: fixedTime,
          },
          { threadId: owner },
        ),
      );
  }
  const items = [
    itemPayload("a-queued", threadA, runId("queued")),
    ...[1, 2, 3].map((index) => itemPayload(`b-queued-${index}`, threadB, runId("queued"), true)),
    itemPayload("unrelated-reasoning", "thread:counts-unselected", runId("queued")),
    itemPayload("a-cancelled", threadA, runId("cancelled")),
    ...[1, 2].map((index) =>
      itemPayload(`b-cancelled-${index}`, threadB, runId("cancelled"), true),
    ),
    itemPayload("a-rolled-cancelled", threadA, runId("rolled-cancelled")),
    itemPayload("b-rolled-cancelled", threadB, runId("rolled-cancelled"), true),
    itemPayload("a-rolled-completed", threadA, runId("rolled-completed"), true),
    itemPayload("a-completed", threadA, runId("completed")),
    itemPayload("a-dangling", threadA, runId("missing")),
    itemPayload("a-runless", threadA, null),
    itemPayload("a-runless-queued", threadA, null, true),
    itemPayload("a-empty-key", threadA, runId("empty-key")),
  ];
  for (let index = 0; index < items.length; index++)
    yield* store.apply(updateItem({ ...items[index], ordinal: index + 1 }));
  // The source permits cross-thread and dangling run references. Empty run IDs
  // are imported SQLite cells, and migration 85 groups them with NULL run IDs.
  yield* sql`UPDATE orchestration_v2_projection_turn_items
    SET run_id = '', payload_json = json_set(payload_json, '$.runId', '')
    WHERE turn_item_id = 'item:counts-a-empty-key'`;
  // SQLite keeps the first duplicate JSON member; serde_json's object map does
  // not. Count predicates must use the original JSON rather than a compact map.
  yield* sql`UPDATE orchestration_v2_projection_turn_items
    SET payload_json = '{"inputIntent":"queued_turn",' ||
      substr(json_set(payload_json, '$.inputIntent', 'turn_start'), 2)
    WHERE turn_item_id = 'item:counts-b-queued-3'`;
  yield* sql`INSERT INTO orchestration_v2_projection_turn_item_counts
    (thread_id, run_key, item_count) VALUES ('thread:counts-zero', 'run:counts-missing', 0)`;
  // Count SQL must ignore this reasoning payload even while scanning the
  // shared queued run. Later mutation cases operate on different valid rows.
  if (malformed)
    yield* sql`UPDATE orchestration_v2_projection_turn_items SET payload_json = '{malformed'
      WHERE turn_item_id = 'item:counts-unrelated-reasoning'`;
  for (const { name, count } of maintenanceCases) {
    const thread = edgeThread(name);
    yield* store.apply(updateItem(itemPayload(`edge-${name}-old`, thread, null)));
    if (count === null)
      yield* sql`DELETE FROM orchestration_v2_projection_turn_item_counts WHERE thread_id = ${thread}`;
    else
      yield* sql`UPDATE orchestration_v2_projection_turn_item_counts SET item_count = ${count}
        WHERE thread_id = ${thread}`;
  }
  for (const { name, old } of nullableCases) {
    const thread = edgeThread(name);
    const payload = itemPayload(`edge-${name}-old`, thread, null);
    yield* store.apply(updateItem(payload));
    yield* sql`UPDATE orchestration_v2_projection_turn_items
      SET run_id = ${old}, payload_json = ${JSON.stringify({ ...payload, runId: old })}
      WHERE turn_item_id = ${payload.id}`;
    yield* sql`UPDATE orchestration_v2_projection_turn_item_counts SET item_count = 0
      WHERE thread_id = ${thread}`;
  }
});

const exercise = Effect.fn("projectionCountFixture.exercise")(function* ({
  label,
  read,
  apply,
  rawUpdate,
  transaction,
}) {
  const result = { initial: yield* read(ids) };
  NodeAssert.equal(
    result.initial.runs.find((row) => row.thread_id === threadA && row.run_id === runId("queued"))
      ?.item_count,
    -2,
    `${label}: queued input is subtracted globally by run ID, even when the result is negative`,
  );
  NodeAssert.equal(
    result.initial.total.find((row) => row.thread_id === threadA)?.runless_item_count,
    3,
  );
  result.empty = yield* read([]);
  result.duplicates = yield* read([threadA, threadA, "missing"]);
  result.foreignOwner = yield* read([owner]);
  yield* transaction(
    Effect.gen(function* () {
      yield* apply(
        makeEvent("run.updated", runPayload("queued", "completed", 1), { threadId: owner }),
      );
      result.statusChanged = yield* read(ids);
      const sentinel = "rollback count movement";
      const exit = yield* Effect.exit(
        transaction(
          Effect.gen(function* () {
            yield* apply(updateItem(itemPayload("a-queued", threadB, null)));
            result.nested = yield* read(ids);
            return yield* Effect.fail(sentinel);
          }),
        ),
      );
      NodeAssert.ok(Exit.isFailure(exit));
      NodeAssert.equal(Cause.squash(exit.cause), sentinel);
      result.restored = yield* read(ids);
      NodeAssert.deepEqual(result.restored, result.statusChanged);
      yield* apply(updateItem(itemPayload("a-queued", threadB, runId("completed"))));
      result.moved = yield* read(ids);
    }),
  );
  result.committed = yield* read(ids);
  NodeAssert.deepEqual(result.committed, result.moved);
  result.edgeInitial = yield* read(edgeIds);
  yield* transaction(
    Effect.gen(function* () {
      for (const { name, insert } of maintenanceCases)
        if (insert)
          yield* apply(updateItem(itemPayload(`edge-${name}-new`, edgeThread(name), null)));
      result.edgeInserted = yield* read(edgeIds);
      for (const { name, count, insert } of maintenanceCases) {
        const row = result.edgeInserted.total.find((row) => row.thread_id === edgeThread(name));
        if (!insert) NodeAssert.equal(row, undefined);
        else NodeAssert.equal(row?.item_count, (count ?? 0) + 1);
      }
      // SQL's UPDATE trigger compares nullable run_id cells before COALESCE.
      // These are deliberately imported SQLite identities below the app's
      // nonempty-ID decoder, so exercise the native projection owner directly.
      for (const { name, next } of nullableCases)
        yield* rawUpdate(itemPayload(`edge-${name}-old`, edgeThread(name), next));
      result.nullableChanged = yield* read(edgeIds);
      for (const { name, expected } of nullableCases)
        NodeAssert.equal(
          result.nullableChanged.total.find((row) => row.thread_id === edgeThread(name))
            ?.item_count,
          expected,
          `${label}: nullable source cells preserve migration-85 WHEN behavior`,
        );
      for (const { name } of maintenanceCases)
        yield* apply(
          updateItem(itemPayload(`edge-${name}-old`, `${edgeThread(name)}-moved`, null)),
        );
      result.edgeRemoved = yield* read([
        ...edgeIds,
        ...maintenanceCases.map(({ name }) => `${edgeThread(name)}-moved`),
      ]);
      for (const { name } of maintenanceCases) {
        NodeAssert.equal(
          result.edgeRemoved.total.find((row) => row.thread_id === edgeThread(name)),
          undefined,
        );
        NodeAssert.equal(
          result.edgeRemoved.total.find((row) => row.thread_id === `${edgeThread(name)}-moved`)
            ?.item_count,
          1,
        );
      }
    }),
  );
  return normalized(result);
});

const sqliteRead = (queries) =>
  Effect.fn("projectionCountFixture.sqliteRead")(function* (threadIds) {
    return {
      total: yield* queries.selectShellItemCounts(threadIds),
      runs: yield* queries.selectShellRunItemCounts(threadIds),
    };
  });
const nativeRead = (client) =>
  Effect.fn("projectionCountFixture.nativeRead")(function* (threadIds) {
    return yield* client.withReadTransaction(
      Effect.gen(function* () {
        const request = (kind) =>
          client.request(
            "application_projections_shell",
            { query: { kind, thread_ids: threadIds } },
            Schema.Array(Schema.Unknown),
          );
        return { total: yield* request("item_counts"), runs: yield* request("run_item_counts") };
      }),
    );
  });

const traceNative = (malformed) => (database) => {
  const wrap = (client) => ({
    ...client,
    request: (input, response) =>
      client
        .request(input, response)
        .pipe(
          Effect.tapError(() =>
            Effect.sync(() =>
              console.error(
                "Native count fixture failed:",
                JSON.stringify({ malformed, ...input }),
              ),
            ),
          ),
        ),
  });
  return {
    ...wrap(database),
    withSession: (use) => database.withSession((session) => use(wrap(session))),
  };
};

async function compareCountCase(executable, malformed) {
  const fixture = await createFixture();
  try {
    const source = NodePath.join(fixture.root, "count-source.sqlite");
    await createMigratedSqlite(source, (sql) => seed(sql, malformed));
    const sourceHash = hash(await NodeFSP.readFile(source));
    const expectedFile = NodePath.join(fixture.root, "count-upstream.sqlite");
    await NodeFSP.copyFile(source, expectedFile);
    const archive = NodePath.join(fixture.root, "count-source.ndjson");
    exportSqliteState({ source, destination: archive });
    nativeRequest(executable, fixture, "initialize", {
      operation: "import_sqlite_application",
      path: archive,
    });
    const expected = await withSqlite(expectedFile, (sql) =>
      Effect.gen(function* () {
        const store = yield* oracle.module.ProjectionStoreV2.pipe(
          Effect.provide(oracle.module.layer),
        );
        return yield* exercise({
          label: "Pinned upstream",
          read: sqliteRead(makeOracleQueries(sql, Schema)),
          apply: store.apply,
          rawUpdate: (payload) => sql`UPDATE orchestration_v2_projection_turn_items
            SET thread_id = ${payload.threadId}, run_id = ${payload.runId}, payload_json = ${JSON.stringify(payload)}
            WHERE turn_item_id = ${payload.id}`,
          transaction: (body) => sql.withTransaction(body),
        });
      }),
    );
    const actual = await withNative(
      executable,
      fixture,
      (client) =>
        Effect.gen(function* () {
          const store = yield* NativeProjectionStore.make;
          return yield* exercise({
            label: "Native",
            read: nativeRead(client),
            apply: store.apply,
            rawUpdate: (payload) =>
              client.request(
                "application_projections_apply",
                {
                  event: {
                    event_type: "turn-item.updated",
                    thread_id: payload.threadId,
                    occurred_at: fixedTime,
                    payload_json: JSON.stringify(payload),
                  },
                },
                Schema.Null,
              ),
            transaction: client.withTransaction,
          });
        }),
      traceNative(malformed),
    );
    NodeAssert.deepEqual(
      actual,
      expected,
      "Native count rows preserve the exact pinned upstream aggregate queries",
    );
    await withNative(
      executable,
      fixture,
      (client) =>
        Effect.gen(function* () {
          yield* NativeProjectionStore.make;
          NodeAssert.deepEqual(normalized(yield* nativeRead(client)(ids)), expected.committed);
        }),
      traceNative(malformed),
    );
    NodeAssert.equal(hash(await NodeFSP.readFile(source)), sourceHash);
  } finally {
    await fixture.close();
  }
}

export async function verifyCountMigrationBackfill() {
  const fixture = await createFixture();
  try {
    for (const [name, runIds, collision] of [
      ["null-only", [null, null], false],
      ["empty-only", ["", ""], false],
      ["null-empty-collision", [null, ""], true],
    ]) {
      await withSqlite(NodePath.join(fixture.root, `${name}.sqlite`), (sql) =>
        Effect.gen(function* () {
          yield* sql`CREATE TABLE orchestration_v2_projection_turn_items
            (turn_item_id TEXT PRIMARY KEY, thread_id TEXT NOT NULL, run_id TEXT)`;
          for (let index = 0; index < runIds.length; index++)
            yield* sql`INSERT INTO orchestration_v2_projection_turn_items
              (turn_item_id, thread_id, run_id) VALUES (${String(index)}, 'thread', ${runIds[index]})`;
          const before =
            yield* sql`SELECT * FROM orchestration_v2_projection_turn_items ORDER BY turn_item_id`;
          const exit = yield* Effect.exit(sql.withTransaction(migrationOracle.module.default));
          if (collision) {
            NodeAssert.ok(
              Exit.isFailure(exit),
              "Pinned migration 85 must reject coalescing distinct NULL/empty groups",
            );
            const error = Cause.squash(exit.cause);
            NodeAssert.match(
              error.reason?.cause?.message ?? error.cause?.message ?? "",
              /UNIQUE constraint failed: orchestration_v2_projection_turn_item_counts\.thread_id, orchestration_v2_projection_turn_item_counts\.run_key/,
            );
            NodeAssert.deepEqual(
              yield* sql`SELECT name FROM sqlite_schema WHERE name = 'orchestration_v2_projection_turn_item_counts'`,
              [],
              "The failed migration transaction must not publish a partial count table",
            );
          } else {
            NodeAssert.ok(Exit.isSuccess(exit));
            NodeAssert.deepEqual(
              normalized(
                yield* sql`SELECT thread_id, run_key, item_count FROM orchestration_v2_projection_turn_item_counts`,
              ),
              [{ thread_id: "thread", run_key: "", item_count: 2 }],
            );
          }
          NodeAssert.deepEqual(
            yield* sql`SELECT * FROM orchestration_v2_projection_turn_items ORDER BY turn_item_id`,
            before,
            "Migration success or failure must retain the original source items",
          );
        }),
      );
    }
    return { source: migrationOracle.source, cases: 3 };
  } finally {
    await fixture.close();
  }
}

export async function compareProjectionCounts(executable) {
  await verifyCountMigrationBackfill();
  await compareCountCase(executable, false);
  await compareCountCase(executable, true);
  return { source: oracle.source, migrationSource: migrationOracle.source, checks: 25 };
}

if (
  process.argv[1] &&
  import.meta.url === NodeURL.pathToFileURL(NodePath.resolve(process.argv[1])).href
) {
  const executable = process.argv[2];
  if (executable === "--migration-only")
    console.log(JSON.stringify(await verifyCountMigrationBackfill()));
  else {
    NodeAssert.ok(executable && NodePath.isAbsolute(executable));
    console.log(JSON.stringify(await compareProjectionCounts(executable)));
  }
}
