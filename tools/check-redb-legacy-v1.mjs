#!/usr/bin/env node
import * as Assert from "node:assert/strict";
import * as FS from "node:fs/promises";
import * as Path from "node:path";
import * as Sqlite from "node:sqlite";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as SecureRandom from "effect/crypto/SecureRandom";

import * as Importer from "../apps/server/src/orchestration-v2/legacy/LegacyV1ThreadImporter.ts";
import * as NativeImporter from "../apps/server/src/orchestration-v2/legacy/NativeLegacyV1ThreadImporter.ts";
import * as EventSink from "../apps/server/src/orchestration-v2/EventSink.ts";
import * as NativeEventSink from "../apps/server/src/orchestration-v2/NativeEventSink.ts";
import * as CommandReceiptStore from "../apps/server/src/orchestration-v2/CommandReceiptStore.ts";
import * as EffectOutbox from "../apps/server/src/orchestration-v2/EffectOutbox.ts";
import * as EventStore from "../apps/server/src/orchestration-v2/EventStore.ts";
import * as ProjectStore from "../apps/server/src/orchestration-v2/ProjectStore.ts";
import * as ProjectionStore from "../apps/server/src/orchestration-v2/ProjectionStore.ts";
import * as TurnItemPositionStore from "../apps/server/src/orchestration-v2/TurnItemPositionStore.ts";
import { jsonPayloads, fixedTime } from "./redb-event-sink-fixtures.mjs";
import { at } from "./check-outbox-runtime.mjs";
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
const oracle = await loadUpstreamModule(
  "apps/server/src/orchestration-v2/legacy/LegacyV1ThreadImporter.ts",
);
const fixture = await createFixture();
const checks = [];
const tables = [
  "orchestration_events",
  "orchestration_v2_legacy_imports",
  "orchestration_v2_turn_item_positions",
  "orchestration_v2_projection_threads",
  "orchestration_v2_projection_messages",
  "orchestration_v2_projection_turn_items",
];

function canonical(value) {
  if (typeof value === "string") {
    if (value.startsWith("migration:v1:thread:legacy:repair:metadata-repair:"))
      return "migration:v1:thread:legacy:repair:metadata-repair:UUID";
    if (value.startsWith("{") || value.startsWith("[")) {
      try {
        return canonical(JSON.parse(value));
      } catch {}
    }
    return value;
  }
  if (Array.isArray(value)) return value.map(canonical);
  if (value !== null && typeof value === "object")
    return Object.fromEntries(
      Object.entries(value)
        .sort(([a], [b]) => a.localeCompare(b))
        .map(([key, item]) => [key, canonical(item)]),
    );
  return value;
}

async function services(kind, filename, use) {
  const run = (client, sql) =>
    Effect.gen(function* () {
      const stores = Layer.mergeAll(
        CommandReceiptStore.layer,
        EffectOutbox.layer,
        EventStore.layer,
        ProjectStore.layer,
        ProjectionStore.layer,
        TurnItemPositionStore.layer,
      );
      const services = yield* Layer.build(
        kind === "redb"
          ? NativeEventSink.layer
          : EventSink.layerFromStores.pipe(Layer.provideMerge(stores)),
      );
      const sink = Context.get(services, EventSink.EventSinkV2);
      const faults = { failAt: 0, calls: 0 };
      const controlled = EventSink.EventSinkV2.of({
        ...sink,
        write: (input) =>
          Effect.gen(function* () {
            if (faults.failAt > 0 && ++faults.calls === faults.failAt) {
              faults.failAt = 0;
              return yield* Effect.fail(
                new EventSink.EventSinkWriteError({
                  operation: "legacy fixture",
                  cause: "controlled write failure",
                }),
              );
            }
            return yield* sink.write(input);
          }),
      });
      const layer =
        kind === "redb"
          ? NativeImporter.layer
          : kind === "upstream"
            ? oracle.module.layer
            : Importer.layer;
      const importer = yield* (
        kind === "upstream" ? oracle.module.LegacyV1ThreadImporter : Importer.LegacyV1ThreadImporter
      ).pipe(Effect.provide(layer), Effect.provideService(EventSink.EventSinkV2, controlled));
      const snapshot = Effect.gen(function* () {
        const result = {};
        for (const table of tables) {
          const values = [];
          if (sql) values.push(...(yield* sql.unsafe(`SELECT * FROM ${table}`)));
          else {
            let after = null;
            do {
              const page = yield* client.scan(table, { after, limit: 128 });
              values.push(
                ...page.rows.map(({ row }) =>
                  Object.fromEntries(
                    Object.entries(row).map(([name, cell]) => [
                      name,
                      cell.type === "null"
                        ? null
                        : cell.type === "integer"
                          ? Number(cell.value)
                          : cell.value,
                    ]),
                  ),
                ),
              );
              after = page.next;
            } while (after !== null);
          }
          result[table] = values
            .map(canonical)
            .sort((a, b) => JSON.stringify(a).localeCompare(JSON.stringify(b)));
        }
        return result;
      });
      return yield* use({
        importer,
        faults,
        snapshot,
        transaction: client ? client.withTransaction : (body) => sql.withTransaction(body),
      });
    });
  return kind === "redb"
    ? withNative(executable, fixture, (client) =>
        at(fixedTime, run(client).pipe(Effect.provide(SecureRandom.layerWebCrypto))),
      )
    : withSqlite(filename, (sql) => at(fixedTime, run(undefined, sql)));
}

function exercise({ importer, faults, snapshot, transaction }) {
  return Effect.gen(function* () {
    const result = {};
    Assert.equal(yield* importer.pendingThreadCount, 3);
    const before = yield* snapshot;
    const rolled = yield* transaction(
      Effect.gen(function* () {
        yield* importer.reconcileShells;
        return yield* Effect.fail("rollback shell and index writes");
      }),
    ).pipe(Effect.exit);
    Assert.ok(Exit.isFailure(rolled));
    Assert.deepEqual(yield* snapshot, before);
    result.shells = yield* importer.reconcileShells;
    Assert.deepEqual(result.shells, { importedThreadCount: 4, importedMessageCount: 3 });
    Assert.deepEqual(yield* importer.reconcileShells, {
      importedThreadCount: 0,
      importedMessageCount: 0,
    });
    Assert.equal(yield* importer.pendingThreadCount, 3);
    const shellRows = (yield* snapshot).orchestration_v2_projection_turn_items;
    Assert.deepEqual(
      shellRows
        .filter((row) => row.thread_id === "legacy:a")
        .map((row) => row.ordinal)
        .sort((a, b) => a - b),
      [133, 134],
    );
    faults.failAt = 2;
    faults.calls = 0;
    const failed = yield* importer.ensureTranscript("legacy:a").pipe(Effect.exit);
    Assert.ok(Exit.isFailure(failed));
    Assert.equal(
      (yield* snapshot).orchestration_v2_projection_messages.filter(
        (row) => row.thread_id === "legacy:a",
      ).length,
      52,
    );
    result.retried = yield* importer.ensureTranscript("legacy:a");
    Assert.deepEqual(result.retried, { importedThreadCount: 1, importedMessageCount: 82 });
    result.background = yield* importer.importPendingTranscripts;
    Assert.deepEqual(result.background, { importedThreadCount: 2, importedMessageCount: 0 });
    Assert.equal(yield* importer.pendingThreadCount, 0);
    Assert.deepEqual(yield* importer.ensureTranscript("legacy:a"), {
      importedThreadCount: 0,
      importedMessageCount: 0,
    });
    Assert.deepEqual(yield* importer.ensureTranscript("missing"), {
      importedThreadCount: 0,
      importedMessageCount: 0,
    });
    result.rows = yield* snapshot;
    return normalized(result);
  });
}

try {
  const source = Path.join(fixture.root, "source.sqlite"),
    retained = Path.join(fixture.root, "retained.sqlite"),
    upstream = Path.join(fixture.root, "upstream.sqlite");
  await createMigratedSqlite(source);
  const db = new Sqlite.DatabaseSync(source);
  try {
    db.prepare(
      `INSERT INTO projection_projects(project_id,title,workspace_root,scripts_json,created_at,updated_at) VALUES('project:main','Legacy project','/fixture/legacy','[]',?,?)`,
    ).run(fixedTime, fixedTime);
    const putThread =
      db.prepare(`INSERT INTO projection_threads(thread_id,project_id,title,model_selection_json,runtime_mode,interaction_mode,branch,worktree_path,created_at,updated_at,pinned_at,pin_order_key,snoozed_until)
      VALUES(?,'project:main',?,'{"instanceId":"codex","model":"fixture"}','full-access','default','  main  ',' /fixture/tree ',?,?,?,?,?)`);
    for (const id of ["legacy:a", "legacy:b", "legacy:empty", "legacy:repair"])
      putThread.run(
        id,
        id === "legacy:empty" ? "  " : id,
        fixedTime,
        fixedTime,
        fixedTime,
        "00001",
        fixedTime,
      );
    const message = db.prepare(
      `INSERT INTO projection_thread_messages(message_id,thread_id,role,text,attachments_json,context_json,is_streaming,created_at,updated_at) VALUES(?,?,?,?,'[]',NULL,?,?,?)`,
    );
    for (let i = 0; i < 134; i++)
      message.run(
        `message:${String(i).padStart(3, "0")}`,
        "legacy:a",
        i % 2 ? "assistant" : "user",
        `body ${i}`,
        i === 133 ? 1 : 0,
        fixedTime,
        fixedTime,
      );
    message.run("one-user", "legacy:b", "user", "only one preview", 0, fixedTime, fixedTime);
    message.run(
      "ignored-role",
      "legacy:a",
      "system",
      "not a transcript message",
      0,
      fixedTime,
      fixedTime,
    );
    const link = db.prepare(
      `INSERT INTO projection_thread_pull_requests(thread_id,host,repository,number,url,source,linked_at,snapshot_json,stack_json) VALUES('legacy:a',?,?,?,?, 'manual',?,NULL,NULL)`,
    );
    link.run("github.com", "a/repo", 9, "https://github.com/a/repo/pull/9", fixedTime);
    link.run("forge.example", "a/repo", 2, "https://forge.example/a/repo/pulls/2", fixedTime);
    const repair = {
      ...jsonPayloads.thread,
      id: "legacy:repair",
      title: "Preserved title",
      historyOrigin: "v1_import",
      pinnedAt: undefined,
      pinOrderKey: undefined,
      snoozedUntil: undefined,
      snoozedAt: undefined,
      unsettledAt: undefined,
      pullRequests: undefined,
      linkedPullRequest: undefined,
      branchPullRequest: undefined,
      activeOrderKey: undefined,
    };
    db.prepare(`INSERT INTO orchestration_v2_projection_threads(thread_id,project_id,title,default_provider,runtime_mode,interaction_mode,created_at,updated_at,archived_at,deleted_at,payload_json)
      VALUES('legacy:repair','project:main','Preserved title','codex','full-access','default',?,?,NULL,NULL,?)`).run(
      fixedTime,
      fixedTime,
      JSON.stringify(repair),
    );
    db.prepare(
      `INSERT INTO orchestration_v2_legacy_imports(thread_id,source_updated_at,shell_imported_at,transcript_imported_at,imported_message_count,last_error) VALUES('legacy:repair',?,?,?,0,NULL)`,
    ).run(fixedTime, fixedTime, fixedTime);
    db.prepare(`INSERT INTO orchestration_events(event_id,aggregate_kind,stream_id,stream_version,event_type,occurred_at,actor_kind,payload_json,metadata_json,application_event_version)
      VALUES('repair-created','thread','legacy:repair',1,'thread.created',?,'system',?,'{}',2)`).run(
      fixedTime,
      JSON.stringify(repair),
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
    "Shared importer retains immutable upstream behavior",
  );
  Assert.deepEqual(
    await services("redb", source, exercise),
    expected,
    "Native importer commits the same rows as immutable upstream",
  );
  checks.push(
    "Shell reconciliation preserves empty titles, branch trimming, PR aggregation and missing metadata repairs",
    "Legacy previews deduplicate latest user and latest message while retaining their original transcript ordinals",
    "Shell position reservations, events, projections and import receipts roll back together",
    "Paginated transcript reads and interrupted 100-event batch hydration retry without duplicates or ordinal changes",
    "Background hydration and completed/missing-thread memoization retain upstream counts",
  );
  await services("redb", source, ({ importer, snapshot }) =>
    Effect.gen(function* () {
      Assert.equal(yield* importer.pendingThreadCount, 0);
      Assert.deepEqual(yield* snapshot, expected.rows);
    }),
  );
  checks.push("Reopening retains imported projections, positions and completed import indexes");
  Assert.equal(hash(await FS.readFile(source)), sourceHash);
  await finishProof({
    executable,
    fixture,
    proofPath,
    checks,
    sourceHash,
    oracles: [oracle.source],
    sourcePaths: [
      "apps/server/src/orchestration-v2/legacy/LegacyV1ThreadImporter.ts",
      "apps/server/src/orchestration-v2/legacy/LegacyV1SqliteRows.ts",
      "apps/server/src/orchestration-v2/legacy/NativeLegacyV1ThreadImporter.ts",
      "apps/server/src/orchestration-v2/legacy/redb_legacy_v1.rs",
      "apps/server/src/orchestration-v2/redb_turn_item_positions.rs",
      "tools/check-redb-legacy-v1.mjs",
    ],
  });
} finally {
  await fixture.close();
}
