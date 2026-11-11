#!/usr/bin/env node
import * as Assert from "node:assert/strict";
import * as FS from "node:fs/promises";
import * as Path from "node:path";
import * as Sqlite from "node:sqlite";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as Clock from "effect/Clock";
import * as ProjectionStore from "../apps/server/src/orchestration-v2/ProjectionStore.ts";
import * as Maintenance from "../apps/server/src/orchestration-v2/ProjectionMaintenance.ts";
import * as NativeMaintenance from "../apps/server/src/orchestration-v2/NativeProjectionMaintenance.ts";
import * as NativeRows from "../apps/server/src/orchestration-v2/NativeMaintenanceRows.ts";
import * as Rows from "../apps/server/src/orchestration-v2/MaintenanceRows.ts";
import * as NativeEvents from "../apps/server/src/persistence/NativeEventRows.ts";
import * as SqliteEvents from "../apps/server/src/persistence/SqliteEventRows.ts";
import {
  EVENT_LOG_PRUNE_MARK,
  EVENT_LOG_PRUNED_THROUGH,
} from "../apps/server/src/persistence/OrchestrationEventStore.ts";
import { createFixture } from "./database-fixture.mjs";
import { exportSqliteState } from "./export-sqlite-state.mjs";
import {
  createMigratedSqlite,
  finishProof,
  hash,
  loadUpstream,
  nativeRequest,
  normalized,
  withNative,
  withSqlite,
} from "./redb-domain-fixture.mjs";

const [executable, proofPath] = process.argv.slice(2);
Assert.ok(executable && Path.isAbsolute(executable));
await FS.access(executable, FS.constants.X_OK);
const oracle = loadUpstream(
  "apps/server/src/orchestration-v2/ProjectionMaintenance.ts",
  {
    NodeSqliteClient,
    Context,
    DateTime,
    Duration,
    Effect,
    Layer,
    Schema,
    SqlClient,
    ProjectionStore,
    EVENT_LOG_PRUNE_MARK,
    EVENT_LOG_PRUNED_THROUGH,
  },
  ["ProjectionMaintenanceV2", "layer"],
);
const fixture = await createFixture();
const at = "2026-11-10T12:00:00.000Z";
const old = "2026-11-08T12:00:00.000Z";
const checks = [];
const traceNative = (database) => {
  const wrap = (client) => ({
    ...client,
    request: (input, response) =>
      client
        .request(input, response)
        .pipe(
          Effect.tapError(() =>
            Effect.sync(() =>
              console.error(
                "Native maintenance fixture failed:",
                input.operation,
                input.query?.kind ?? input.mutation?.kind ?? "",
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
const event = (id, stream, type = "provider-session.detached", payload = {}) => ({
  event_id: id,
  aggregate_kind: "thread",
  stream_id: stream,
  event_type: type,
  occurred_at: at,
  command_id: null,
  causation_event_id: null,
  correlation_id: null,
  actor_kind: "server",
  payload_json: JSON.stringify(payload),
  metadata_json: "{}",
  application_event_version: 2,
});
const fixedClock = {
  currentTimeMillisUnsafe: () => Date.parse(at),
  currentTimeMillis: Effect.succeed(Date.parse(at)),
  currentTimeNanosUnsafe: () => BigInt(Date.parse(at)) * 1_000_000n,
  currentTimeNanos: Effect.succeed(BigInt(Date.parse(at)) * 1_000_000n),
  monotonicTimeNanosUnsafe: () => 0n,
  monotonicTimeNanos: Effect.succeed(0n),
  sleep: () => Effect.never,
};

function exercise(kind) {
  const serviceTag =
    kind === "upstream"
      ? oracle.module.ProjectionMaintenanceV2
      : Maintenance.ProjectionMaintenanceV2;
  const layer =
    kind === "upstream"
      ? oracle.module.layer
      : kind === "native"
        ? NativeMaintenance.layer
        : Maintenance.layer;
  return Effect.gen(function* () {
    const service = yield* serviceTag;
    const events = yield* kind === "native" ? NativeEvents.make : SqliteEvents.make;
    let yielded = 0;
    const betweenPages = Effect.gen(function* () {
      if (++yielded !== 1) return;
      // This append occurs after the retained-head reference scan. The same
      // writer that deletes a page must recheck appended references first.
      const appended = yield* events.append({
        row: event("concurrent-reference", "concurrent", "subagent.updated", {
          id: "race",
          prompt: "",
          promptSequence: 5,
        }),
      });
      Assert.equal(appended.sequence, 151);
    });
    const first = yield* service.pruneEventStore(betweenPages);
    Assert.ok(first.deletedEventCount > 50);
    Assert.equal(first.deletedReceiptCount, 61);
    Assert.ok(first.reclaimableBytes >= 0);
    const all = yield* events.read({ after_sequence: 0, limit: 4096 });
    const surviving = all.map((row) => row.sequence);
    for (const sequence of [1, 2, 5, 100, 101, 104, 131, 150, 151])
      Assert.ok(surviving.includes(sequence), `preserve ${sequence}`);
    for (const sequence of [3, 4, 6, 102, 103])
      Assert.ok(!surviving.includes(sequence), `remove ${sequence}`);
    Assert.equal(
      JSON.parse(all.find((row) => row.sequence === 151).payload_json).prompt,
      "race anchor",
    );
    const stats = yield* events.stats({
      kind: "application",
      after_sequence: 0,
      through_sequence: 151,
    });
    Assert.equal(stats.event_count, all.length);
    Assert.equal(
      stats.raw_payload_bytes,
      all.reduce((sum, row) => sum + Buffer.byteLength(row.payload_json), 0),
    );
    const threadStats = yield* events.stats({
      kind: "agent",
      thread_id: "a",
      after_sequence: 0,
      through_sequence: 151,
      max_events: 4096,
    });
    Assert.equal(threadStats.event_count, all.filter((r) => r.stream_id === "a").length);
    const pruned = yield* events.prunedThrough;
    Assert.equal(pruned, 150);
    const second = yield* service.pruneEventStore();
    Assert.equal(second.deletedEventCount, 0);
    Assert.equal(second.deletedReceiptCount, 0);
    return normalized({
      deleted: first.deletedEventCount,
      receipts: first.deletedReceiptCount,
      all,
      stats,
      threadStats,
      pruned,
      latest: yield* events.latest({ kind: "application" }),
    });
  }).pipe(Effect.provide(layer), Effect.provideService(Clock.Clock, fixedClock));
}

try {
  const source = Path.join(fixture.root, "source.sqlite"),
    exported = Path.join(fixture.root, "source.ndjson");
  await createMigratedSqlite(source);
  const db = new Sqlite.DatabaseSync(source);
  try {
    const insert = db.prepare(
      `INSERT INTO orchestration_events (sequence,event_id,aggregate_kind,stream_id,stream_version,event_type,occurred_at,command_id,causation_event_id,correlation_id,actor_kind,payload_json,metadata_json,application_event_version) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
    );
    const add = (sequence, row) =>
      insert.run(
        sequence,
        row.event_id,
        row.aggregate_kind,
        row.stream_id,
        sequence,
        row.event_type,
        row.occurred_at,
        null,
        null,
        null,
        "server",
        row.payload_json,
        "{}",
        row.application_event_version,
      );
    for (let seq = 1; seq <= 150; seq++) {
      let row = event(
        `e${seq}`,
        seq <= 100
          ? "a"
          : seq <= 104
            ? "b"
            : seq <= 131
              ? "legacy"
              : seq === 132
                ? "pending-legacy"
                : "final",
      );
      if ([1, 101, 131, 133].includes(seq)) row = { ...row, event_type: "thread.created" };
      if (seq === 2)
        row = {
          ...row,
          event_type: "subagent.updated",
          payload_json: JSON.stringify({ id: "kept", prompt: "retained prompt 🦊".repeat(40) }),
        };
      if (seq === 5)
        row = {
          ...row,
          event_type: "subagent.updated",
          payload_json: JSON.stringify({ id: "race", prompt: "race anchor" }),
        };
      if (seq === 100)
        row = {
          ...row,
          event_type: "subagent.updated",
          payload_json: JSON.stringify({ id: "kept", prompt: "", promptSequence: 2 }),
        };
      if (seq === 102)
        row = {
          ...row,
          event_type: "subagent.updated",
          payload_json: JSON.stringify({
            id: "removed",
            prompt: "remove both anchor and borrower".repeat(40),
          }),
        };
      if (seq === 103)
        row = {
          ...row,
          event_type: "subagent.updated",
          payload_json: JSON.stringify({ id: "removed", prompt: "", promptSequence: 102 }),
        };
      if ((seq >= 105 && seq <= 130) || seq === 132) row = { ...row, application_event_version: 1 };
      if (seq === 140)
        row = {
          ...row,
          event_type: "run.updated",
          payload_json: JSON.stringify({ status: "running" }),
        };
      add(seq, row);
    }
    db.prepare(
      `INSERT INTO orchestration_v2_legacy_imports (thread_id,source_updated_at,shell_imported_at,transcript_imported_at,imported_message_count,last_error) VALUES ('legacy',?,?,?,0,NULL)`,
    ).run(old, old, old);
    db.prepare(
      `INSERT INTO orchestration_v2_projection_metadata (projection_name,schema_version,last_sequence,updated_at) VALUES ('thread-projections',2,150,?),('event-log-prune-mark',2,150,?) ON CONFLICT(projection_name) DO UPDATE SET last_sequence=excluded.last_sequence,updated_at=excluded.updated_at`,
    ).run(old, old);
    const receipt = db.prepare(
      `INSERT INTO orchestration_command_receipts (command_id,aggregate_kind,aggregate_id,accepted_at,result_sequence,status,error,command_type) VALUES (?,'thread',?,?,1,'accepted',NULL,?)`,
    );
    for (let i = 0; i < 61; i++) receipt.run(`legacy:${i}`, "legacy", at, "legacy");
    receipt.run("pending", "pending-legacy", at, "legacy");
    receipt.run("kept", "legacy", at, "thread.create");
  } finally {
    db.close();
  }
  const sourceHash = hash(await FS.readFile(source));
  exportSqliteState({ source, destination: exported });
  nativeRequest(executable, fixture, "initialize", {
    operation: "import_sqlite_application",
    path: exported,
  });
  const upstream = Path.join(fixture.root, "upstream.sqlite"),
    sqlite = Path.join(fixture.root, "retained.sqlite");
  await FS.copyFile(source, upstream);
  await FS.copyFile(source, sqlite);
  const expected = await withSqlite(upstream, () => exercise("upstream"));
  Assert.deepEqual(await withSqlite(sqlite, () => exercise("sqlite")), expected);
  Assert.deepEqual(
    await withNative(executable, fixture, () => exercise("native"), traceNative),
    expected,
  );
  checks.push(
    "Real ProjectionMaintenance matches immutable upstream and retained SQLite across paged application/legacy event and receipt pruning",
  );
  checks.push(
    "Retained stream heads, thread creation, running usage records, referenced prompts and concurrent prompt borrowers survive",
  );
  checks.push(
    "Deleted anchors and borrowers subtract original restored byte sizes from application/thread Fenwick statistics without dangling replay indexes",
  );
  await withNative(executable, fixture, (client) =>
    Effect.gen(function* () {
      const rows = yield* NativeRows.make;
      const before = yield* rows.read({ kind: "appended", after_sequence: 0 }, Rows.AppendedRows);
      const failed = yield* Effect.exit(
        client.withTransaction(
          Effect.gen(function* () {
            yield* rows.write({ kind: "delete_events", sequences: [2, 5, 151] }, Rows.EmptyRows);
            return yield* Effect.fail("rollback");
          }),
        ),
      );
      Assert.ok(Exit.isFailure(failed));
      Assert.deepEqual(
        yield* rows.read({ kind: "appended", after_sequence: 0 }, Rows.AppendedRows),
        before,
      );
      const events = yield* NativeEvents.make;
      Assert.deepEqual(
        normalized(
          yield* events.stats({ kind: "application", after_sequence: 0, through_sequence: 151 }),
        ),
        expected.stats,
      );
      // Deleting an identity releases SQL's uniqueness constraint, but not its
      // sequence allocator. A replacement is a new row above the previous head.
      const reused = yield* events.append({ row: event("e3", "a") });
      Assert.equal(reused.sequence, 152);
    }),
  );
  checks.push(
    "Native reopen, transaction rollback, identity reuse and monotonic sequence allocation remain correct after removal",
  );
  Assert.equal(hash(await FS.readFile(source)), sourceHash);
  await finishProof({
    executable,
    fixture,
    proofPath,
    checks,
    oracles: [oracle.source],
    sourceHash,
    sourcePaths: [
      "apps/server/src/orchestration-v2/ProjectionMaintenance.ts",
      "apps/server/src/orchestration-v2/MaintenanceRows.ts",
      "apps/server/src/orchestration-v2/SqliteMaintenanceRows.ts",
      "apps/server/src/orchestration-v2/NativeMaintenanceRows.ts",
      "apps/server/src/persistence/redb_projection_maintenance.rs",
      "apps/server/src/persistence/redb_event_rows.rs",
      "apps/server/src/persistence/redb_command_receipts.rs",
      "tools/check-redb-maintenance.mjs",
    ],
  });
} finally {
  await fixture.close();
}
