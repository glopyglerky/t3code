import * as NodeAssert from "node:assert/strict";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Logger from "effect/Logger";
import * as Schema from "effect/Schema";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Omdb from "../apps/server/src/persistence/Omdb.ts";
import * as NativeOwnership from "../apps/server/src/persistence/NativeServerOwnershipLock.ts";
import schema88 from "../apps/server/src/persistence/omdb-schema-88.json" with { type: "json" };
import currentSchema from "../apps/server/src/persistence/omdb-schema.json" with { type: "json" };
import { snapshotForNativeImport } from "../apps/server/src/persistence/migrateSqliteToOmdb.ts";
import {
  createMessageSearchTriggers,
  rebuildMessageSearch,
} from "../apps/server/src/orchestration-v2/messageSearchIndex.ts";
import { createFixture } from "./database-fixture.mjs";
import { fixedTime, jsonPayloads } from "./redb-event-sink-fixtures.mjs";
import {
  createMigratedSqlite,
  hash,
  nativeRequest,
  withNative,
  withSqlite,
} from "./redb-domain-fixture.mjs";

const shellReadIndexes = [
  "orchestration_v2_projection_threads_worktree_path_idx",
  "orchestration_v2_projection_threads_root_thread_idx",
  "orchestration_v2_projection_runs_run_status_idx",
];
const migrationsSchema = Schema.Array(Schema.Struct({ id: Schema.String, name: Schema.String }));

const isolated = (fixture, effect) =>
  effect.pipe(
    Effect.provideService(HostProcess.HomeDirectory, fixture.root),
    Effect.provideService(HostProcess.Environment, {}),
    Effect.scoped,
    Effect.provide(Layer.merge(NodeServices.layer, NodeCrypto.layer)),
    Effect.provide(Logger.layer([])),
  );
const hasCauseMessage = (error, expected) => {
  for (let cause = error; cause; cause = cause.cause) {
    if (cause instanceof Error && cause.message.includes(expected)) return true;
  }
  return false;
};

async function verifyUnactivatedUpgrade(
  executable,
  originalSource,
  originalArchive,
  { version = 80, verify } = {},
) {
  for (const changed of [false, true]) {
    const fixture = await createFixture();
    try {
      const source = NodePath.join(fixture.root, "statev2.sqlite");
      const archive = NodePath.join(fixture.root, `schema-${version}.jsonl`);
      await NodeFSP.copyFile(originalSource, source);
      await NodeFSP.copyFile(originalArchive, archive);
      const imported = nativeRequest(executable, fixture, "initialize", {
        operation: "import_sqlite_application",
        path: archive,
      });
      const sourceBefore = hash(await NodeFSP.readFile(source));
      // Drive the actual selected adapter through conversion and every index
      // preparation, then interrupt just before publishing the ownership marker.
      await NodeAssert.rejects(
        Effect.runPromise(
          isolated(
            fixture,
            Effect.gen(function* () {
              const locks = yield* NativeOwnership.make(executable);
              yield* locks.acquire(NodePath.join(fixture.root, "server-owner.sqlite"));
              return yield* Omdb.make({ executable, databasePath: fixture.databasePath }).pipe(
                Effect.provideService(Omdb.BootstrapOwnership, {
                  activate: Effect.fail(
                    new Omdb.OmdbStartupError({
                      path: fixture.databasePath,
                      cause: new Error("injected upgrade activation failure"),
                    }),
                  ),
                }),
              );
            }),
          ),
        ),
        (error) => hasCauseMessage(error, "injected upgrade activation failure"),
      );
      NodeAssert.equal(hash(await NodeFSP.readFile(source)), sourceBefore);
      NodeAssert.deepEqual(
        nativeRequest(executable, fixture, "connect", { operation: "application_migrations" }),
        currentSchema.migrations,
      );
      NodeAssert.deepEqual(
        nativeRequest(executable, fixture, "connect", { operation: "application_import" }),
        { ...imported, replayed: true },
      );
      const marker = NodePath.join(fixture.root, "server-owner.sqlite");
      NodeAssert.equal(await NodeFSP.readFile(marker, "utf8"), "");
      if (changed) {
        await withSqlite(
          source,
          (sql) =>
            sql`UPDATE projection_projects SET title='Newer SQLite title' WHERE project_id='upgrade:project'`,
        );
        const changedSource = hash(await NodeFSP.readFile(source));
        const unchangedNative = hash(await NodeFSP.readFile(fixture.databasePath));
        await NodeAssert.rejects(
          Effect.runPromise(
            isolated(fixture, Omdb.make({ executable, databasePath: fixture.databasePath })),
          ),
          (error) => hasCauseMessage(error, "SQLite no longer matches"),
        );
        NodeAssert.equal(hash(await NodeFSP.readFile(source)), changedSource);
        NodeAssert.equal(
          hash(await NodeFSP.readFile(fixture.databasePath)),
          unchangedNative,
          "stale upgraded candidate rejection cannot modify the native store",
        );
        NodeAssert.equal(await NodeFSP.readFile(marker, "utf8"), "");
      } else {
        await Effect.runPromise(
          isolated(
            fixture,
            Effect.gen(function* () {
              const { client, database } = yield* Omdb.make({
                executable,
                databasePath: fixture.databasePath,
              });
              if (verify) yield* verify(client, database);
              else {
                const row = yield* client.get("projection_projects", [
                  { type: "text", value: "upgrade:project" },
                ]);
                NodeAssert.equal(row.title.value, "Chosen title");
                NodeAssert.equal(row.has_custom_title.value, "1");
              }
            }),
          ),
        );
        NodeAssert.equal(hash(await NodeFSP.readFile(source)), sourceBefore);
        NodeAssert.equal(await NodeFSP.readFile(marker, "utf8"), "t3code-omdb-owner-v1\n");
      }
      NodeAssert.deepEqual(
        nativeRequest(executable, fixture, "connect", { operation: "application_import" }),
        { ...imported, replayed: true },
        "activation retries retain the original import receipt",
      );
      NodeAssert.equal(
        (await NodeFSP.readdir(fixture.root)).filter((path) => path.startsWith(".omdb-import-"))
          .length,
        0,
      );
    } finally {
      await fixture.close();
    }
  }
}

/** Exercise the actual selected startup adapter against an activated old home. */
export async function verifyNativeSchemaUpgrade(executable) {
  const fixture = await createFixture();
  try {
    const source = NodePath.join(fixture.root, "statev2.sqlite");
    const archive = NodePath.join(fixture.root, "schema-80.jsonl");
    const quote =
      "[Assistant quote](t3-citation://v1/env/thread/message?text=quoted+history&start=0&end=14&prefix=&suffix=&comment=preserve+this)";
    await createMigratedSqlite(source, (sql) =>
      Effect.gen(function* () {
        // Retain the exact previous schema and migration history. SQLite is only
        // this fixture's source/oracle; activated startup must never reopen it.
        const triggers = yield* sql`SELECT name FROM sqlite_schema WHERE type = 'trigger'
        AND (sql LIKE '%orchestration_v2_projection_turn_item_counts%' OR name LIKE 'orchestration_v2_message_search_%')`;
        for (const trigger of triggers)
          yield* sql.unsafe(`DROP TRIGGER "${trigger.name.replaceAll('"', '""')}"`);
        for (const index of [
          "orchestration_v2_projection_turn_items_conversation_idx",
          "orchestration_v2_projection_nodes_settled_leaf_idx",
          "orchestration_v2_projection_context_transfers_source_thread_status_idx",
          "orchestration_v2_projection_runs_last_executed_idx",
          "orchestration_v2_projection_turn_items_queued_input_idx",
          "orchestration_v2_projection_turn_items_thread_type_idx",
          ...shellReadIndexes,
        ])
          yield* sql.unsafe(`DROP INDEX "${index}"`);
        yield* sql`CREATE INDEX orchestration_v2_projection_context_transfers_source_thread_idx
        ON orchestration_v2_projection_context_transfers(source_thread_id)`;
        yield* sql`DROP TABLE orchestration_v2_projection_turn_item_counts`;
        yield* sql`ALTER TABLE orchestration_v2_projection_messages DROP COLUMN search_text`;
        yield* sql`ALTER TABLE projection_projects DROP COLUMN has_custom_title`;
        yield* sql`DELETE FROM effect_sql_migrations WHERE migration_id > 80`;
        yield* sql`INSERT INTO projection_projects (project_id,title,workspace_root,scripts_json,created_at,updated_at)
        VALUES ('upgrade:project','Chosen title','/fixture','[]','2026-11-13T00:00:00.000Z','2026-11-13T00:00:00.000Z')`;
        yield* sql`INSERT INTO orchestration_events
        (event_id,aggregate_kind,stream_id,stream_version,event_type,occurred_at,actor_kind,payload_json,metadata_json)
        VALUES ('upgrade:title','project','upgrade:project',1,'project.meta-updated','2026-11-13T00:00:00.000Z','user','{"title":"Chosen title"}','{}')`;
        yield* sql`INSERT INTO orchestration_v2_projection_messages
        (message_id,thread_id,role,streaming,created_at,updated_at,payload_json)
        VALUES ('upgrade:message','upgrade:thread','assistant',0,'2026-11-13T00:00:00.000Z','2026-11-13T00:00:00.000Z',${JSON.stringify({ id: "upgrade:message", threadId: "upgrade:thread", role: "assistant", text: quote, streaming: false })})`;
        for (const [id, run] of [
          ["one", "upgrade:run"],
          ["two", "upgrade:run"],
          ["three", null],
        ]) {
          yield* sql`INSERT INTO orchestration_v2_projection_turn_items
          (turn_item_id,thread_id,run_id,ordinal,type,status,updated_at,payload_json)
          VALUES (${id},'upgrade:thread',${run},1,'assistant_message','completed','2026-11-13T00:00:00.000Z',${JSON.stringify({ id, type: "assistant_message" })})`;
        }
        yield* createMessageSearchTriggers(sql);
        yield* rebuildMessageSearch(sql);
      }),
    );
    // The prior startup also imported a private SQLite backup. backup() resets
    // the schema cookie, so a direct source-file export is a different receipt.
    await Effect.runPromise(
      isolated(
        fixture,
        snapshotForNativeImport(source, { migrate: false }).pipe(
          Effect.flatMap((snapshot) =>
            Effect.promise(() => NodeFSP.copyFile(snapshot.path, archive)),
          ),
        ),
      ),
    );
    const sourceDigest = hash(await NodeFSP.readFile(source));
    const imported = nativeRequest(executable, fixture, "initialize", {
      operation: "import_sqlite_application",
      path: archive,
    });
    NodeAssert.ok(imported.generation);
    NodeAssert.equal(
      nativeRequest(executable, fixture, "connect", { operation: "application_migrations" }).length,
      80,
    );
    await withNative(executable, fixture, (client) =>
      client.withTransaction(
        Effect.gen(function* () {
          const key = [{ type: "text", value: "upgrade:project" }];
          const row = yield* client.get("projection_projects", key);
          yield* client.put("projection_projects", key, {
            ...row,
            title: { type: "text", value: "Native-only title" },
          });
        }),
      ),
    );
    await NodeFSP.writeFile(
      NodePath.join(fixture.root, "server-owner.sqlite"),
      "t3code-omdb-owner-v1\n",
      { flag: "wx" },
    );
    // Native-only changes since cutover must survive even though the retained
    // source has the previous project title. No SQLite reimport is permissible.
    const before = nativeRequest(executable, fixture, "connect", {
      operation: "application_schema",
    });
    NodeAssert.ok(
      before.tables.some(
        (table) =>
          table.name === "projection_projects" && !table.columns.includes("has_custom_title"),
      ),
    );
    const runtime = (use) =>
      Effect.runPromise(
        Effect.gen(function* () {
          const { client, database } = yield* Omdb.make({
            executable,
            databasePath: fixture.databasePath,
          });
          return yield* use(client, database);
        }).pipe(
          Effect.provideService(HostProcess.HomeDirectory, fixture.root),
          Effect.provideService(HostProcess.Environment, {}),
          Effect.scoped,
          Effect.provide(Layer.merge(NodeServices.layer, NodeCrypto.layer)),
          Effect.provide(Logger.layer([])),
        ),
      );
    const verify = Effect.fn("SchemaUpgradeFixture.verify")(function* (client, database) {
      const migrations = yield* database.request(
        { operation: "application_migrations" },
        migrationsSchema,
      );
      NodeAssert.deepEqual(migrations, currentSchema.migrations);
      const project = yield* client.get("projection_projects", [
        { type: "text", value: "upgrade:project" },
      ]);
      NodeAssert.equal(project.has_custom_title.value, "1");
      NodeAssert.equal(project.title.value, "Native-only title");
      const message = yield* client.get("orchestration_v2_projection_messages", [
        { type: "text", value: "upgrade:message" },
      ]);
      NodeAssert.equal(message.search_text.value, "quoted history\nComment: preserve this");
      NodeAssert.ok(message.payload_json.value.includes("t3-citation://"));
      const count = yield* client.get("orchestration_v2_projection_turn_item_counts", [
        { type: "text", value: "upgrade:thread" },
        { type: "text", value: "upgrade:run" },
      ]);
      NodeAssert.equal(count.item_count.value, "2");
    });
    await runtime(verify);
    await runtime(verify);
    NodeAssert.equal(
      hash(await NodeFSP.readFile(source)),
      sourceDigest,
      "activated upgrade preserves the obsolete SQLite source byte-for-byte",
    );
    NodeAssert.equal(
      await NodeFSP.readFile(NodePath.join(fixture.root, "server-owner.sqlite"), "utf8"),
      "t3code-omdb-owner-v1\n",
    );
    await verifyUnactivatedUpgrade(executable, source, archive);
    return "Schema-80 homes upgrade through selected native startup; activated native-only edits survive, interrupted activation retries preserve the immutable import proof, changed SQLite sources fail closed, and citation text, custom titles and run counts survive restart";
  } finally {
    await fixture.close();
  }
}

/** Migration 89 changes indexes only, including homes already using native schema 88. */
export async function verifyNativeSchema88Upgrade(executable) {
  NodeAssert.equal(schema88.migrations.at(-1).id, "88");
  NodeAssert.equal(currentSchema.migrations.at(-1).id, "89");
  NodeAssert.deepEqual(schema88.schema, currentSchema.schema);
  NodeAssert.deepEqual(currentSchema.migrations.slice(0, -1), schema88.migrations);
  const fixture = await createFixture();
  try {
    const source = NodePath.join(fixture.root, "statev2.sqlite");
    const archive = NodePath.join(fixture.root, "schema-88.jsonl");
    const rootThread = "upgrade:thread";
    const childThread = "upgrade:child";
    const otherThread = "upgrade:other";
    const runId = "upgrade:run";
    const messageId = "upgrade:message";
    const originalWorktree = "/fixture/schema-88-worktree";
    const nativeWorktree = "/fixture/native-only-worktree";
    const quote =
      "[Assistant quote](t3-citation://v1/env/thread/message?text=quoted+history&start=0&end=14&prefix=&suffix=&comment=preserve+this)";
    await createMigratedSqlite(source, (sql) =>
      Effect.gen(function* () {
        for (const index of shellReadIndexes) yield* sql.unsafe(`DROP INDEX "${index}"`);
        yield* sql`DELETE FROM effect_sql_migrations WHERE migration_id > 88`;
        for (const [id, title, custom] of [
          ["upgrade:project", "Chosen title", 1],
          ["upgrade:automatic", "Automatic title", 0],
        ])
          yield* sql`INSERT INTO projection_projects
          (project_id,title,workspace_root,scripts_json,created_at,updated_at,has_custom_title)
          VALUES (${id},${title},'/fixture','[]',${fixedTime},${fixedTime},${custom})`;
        for (const id of [rootThread, childThread, otherThread]) {
          const thread = {
            ...jsonPayloads.thread,
            id,
            projectId: "upgrade:project",
            title: `Schema 88 ${id}`,
            worktreePath: id === otherThread ? "/fixture/other-worktree" : originalWorktree,
            lineage: {
              parentThreadId: id === childThread ? rootThread : null,
              relationshipToParent: id === childThread ? "fork" : null,
              rootThreadId: id === childThread ? rootThread : id,
            },
          };
          yield* sql`INSERT INTO orchestration_v2_projection_threads
          (thread_id,project_id,title,default_provider,provider_instance_id,runtime_mode,
           interaction_mode,created_at,updated_at,payload_json)
          VALUES (${id},${thread.projectId},${thread.title},'codex',${thread.providerInstanceId},
           ${thread.runtimeMode},${thread.interactionMode},${fixedTime},${fixedTime},${JSON.stringify(thread)})`;
        }
        const run = {
          ...jsonPayloads.run,
          id: runId,
          threadId: rootThread,
          userMessageId: messageId,
          status: "completed",
          completedAt: fixedTime,
        };
        yield* sql`INSERT INTO orchestration_v2_projection_runs
        (run_id,thread_id,ordinal,provider,provider_instance_id,provider_thread_id,status,requested_at,completed_at,payload_json)
        VALUES (${runId},${rootThread},1,'codex',${run.providerInstanceId},${run.providerThreadId},
         'completed',${fixedTime},${fixedTime},${JSON.stringify(run)})`;
        const message = {
          ...jsonPayloads.message,
          id: messageId,
          threadId: rootThread,
          runId,
          text: quote,
          streaming: false,
        };
        yield* sql`INSERT INTO orchestration_v2_projection_messages
        (message_id,thread_id,run_id,node_id,role,streaming,created_at,updated_at,payload_json,search_text)
        VALUES (${messageId},${rootThread},${runId},${message.nodeId},'assistant',0,
         ${fixedTime},${fixedTime},${JSON.stringify(message)},'quoted history\nComment: preserve this')`;
        for (const [ordinal, itemRun] of [
          [1, runId],
          [2, runId],
          [3, null],
        ]) {
          const item = {
            ...jsonPayloads.turnItem,
            id: `upgrade:item:${ordinal}`,
            threadId: rootThread,
            runId: itemRun,
            ordinal,
            status: "completed",
            completedAt: fixedTime,
            streaming: false,
          };
          yield* sql`INSERT INTO orchestration_v2_projection_turn_items
          (turn_item_id,thread_id,run_id,node_id,provider_thread_id,provider_turn_id,
           ordinal,type,status,updated_at,payload_json)
          VALUES (${item.id},${rootThread},${itemRun},${item.nodeId},${item.providerThreadId},${item.providerTurnId},
           ${ordinal},${item.type},'completed',${fixedTime},${JSON.stringify(item)})`;
        }
      }),
    );
    await Effect.runPromise(
      isolated(
        fixture,
        snapshotForNativeImport(source, { migrate: false }).pipe(
          Effect.flatMap((snapshot) =>
            Effect.promise(() => NodeFSP.copyFile(snapshot.path, archive)),
          ),
        ),
      ),
    );
    const sourceDigest = hash(await NodeFSP.readFile(source));
    const archiveDigest = hash(await NodeFSP.readFile(archive));
    const imported = nativeRequest(executable, fixture, "initialize", {
      operation: "import_sqlite_application",
      path: archive,
    });
    NodeAssert.deepEqual(
      nativeRequest(executable, fixture, "connect", { operation: "application_schema" }),
      schema88.schema,
    );
    NodeAssert.deepEqual(
      nativeRequest(executable, fixture, "connect", { operation: "application_migrations" }),
      schema88.migrations,
    );
    const preservedTables = [
      "projection_projects",
      "orchestration_v2_projection_threads",
      "orchestration_v2_projection_runs",
      "orchestration_v2_projection_messages",
      "orchestration_v2_projection_turn_items",
      "orchestration_v2_projection_turn_item_counts",
    ];
    const readRows = Effect.fn("Schema88UpgradeFixture.readRows")(function* (client) {
      const result = {};
      for (const table of preservedTables) {
        const page = yield* client.scan(table, { limit: 100 });
        NodeAssert.equal(page.next, null, `fixture must compare every ${table} row`);
        result[table] = page.rows;
      }
      return result;
    });
    const sourceRows = await withNative(executable, fixture, readRows);
    const textKey = (id) => [{ type: "text", value: id }];
    await withNative(executable, fixture, (client) =>
      client.withTransaction(
        Effect.gen(function* () {
          const project = yield* client.get("projection_projects", textKey("upgrade:project"));
          yield* client.put("projection_projects", textKey("upgrade:project"), {
            ...project,
            title: { type: "text", value: "Native-only schema 88 title" },
          });
          for (const id of [rootThread, childThread]) {
            const row = yield* client.get("orchestration_v2_projection_threads", textKey(id));
            const payload = JSON.parse(row.payload_json.value);
            if (id === rootThread) payload.worktreePath = nativeWorktree;
            else {
              payload.lineage.parentThreadId = otherThread;
              payload.lineage.rootThreadId = otherThread;
            }
            yield* client.put("orchestration_v2_projection_threads", textKey(id), {
              ...row,
              payload_json: { type: "text", value: JSON.stringify(payload) },
            });
          }
          const message = yield* client.get(
            "orchestration_v2_projection_messages",
            textKey(messageId),
          );
          const payload = JSON.parse(message.payload_json.value);
          payload.text = "Native-only schema 88 message";
          yield* client.put("orchestration_v2_projection_messages", textKey(messageId), {
            ...message,
            payload_json: { type: "text", value: JSON.stringify(payload) },
            search_text: { type: "text", value: payload.text },
          });
        }),
      ),
    );
    const nativeRows = await withNative(executable, fixture, readRows);
    NodeAssert.notDeepEqual(nativeRows, sourceRows);
    const verify = (expectedRows, nativeEdits) => (client, database) =>
      Effect.gen(function* () {
        NodeAssert.deepEqual(
          yield* database.request({ operation: "application_migrations" }, migrationsSchema),
          currentSchema.migrations,
        );
        NodeAssert.deepEqual(
          yield* readRows(client),
          expectedRows,
          "index-only upgrade preserves every application row",
        );
        const count = yield* client.get("orchestration_v2_projection_turn_item_counts", [
          ...textKey(rootThread),
          ...textKey(runId),
        ]);
        NodeAssert.equal(
          count.item_count.value,
          "2",
          "migration 89 must not repeat the count backfill",
        );
        const shellIds = (options) =>
          client
            .request(
              "application_projections_shell",
              { query: { kind: "threads", options } },
              Schema.Json,
            )
            .pipe(Effect.map((rows) => rows.map((row) => row.thread_id).sort()));
        NodeAssert.deepEqual(
          yield* shellIds({ worktreePath: nativeEdits ? nativeWorktree : originalWorktree }),
          (nativeEdits ? [rootThread] : [rootThread, childThread]).sort(),
        );
        NodeAssert.deepEqual(
          yield* shellIds({ familyOf: rootThread }),
          (nativeEdits ? [rootThread] : [rootThread, childThread]).sort(),
        );
        NodeAssert.deepEqual(
          yield* shellIds({ familyOf: otherThread }),
          (nativeEdits ? [otherThread, childThread] : [otherThread]).sort(),
        );
        NodeAssert.deepEqual(yield* shellIds({ familyOf: "upgrade:missing" }), []);
      });
    await NodeFSP.writeFile(
      NodePath.join(fixture.root, "server-owner.sqlite"),
      "t3code-omdb-owner-v1\n",
      { flag: "wx" },
    );
    for (let restart = 0; restart < 2; restart++) {
      await Effect.runPromise(
        isolated(
          fixture,
          Effect.gen(function* () {
            const { client, database } = yield* Omdb.make({
              executable,
              databasePath: fixture.databasePath,
            });
            yield* verify(nativeRows, true)(client, database);
          }),
        ),
      );
      NodeAssert.deepEqual(
        nativeRequest(executable, fixture, "connect", { operation: "application_import" }),
        { ...imported, replayed: true },
      );
      NodeAssert.equal(hash(await NodeFSP.readFile(source)), sourceDigest);
      NodeAssert.equal(hash(await NodeFSP.readFile(archive)), archiveDigest);
      NodeAssert.equal(
        await NodeFSP.readFile(NodePath.join(fixture.root, "server-owner.sqlite"), "utf8"),
        "t3code-omdb-owner-v1\n",
      );
    }
    await verifyUnactivatedUpgrade(executable, source, archive, {
      version: 88,
      verify: verify(sourceRows, false),
    });
    return "Schema-88 native homes upgrade through Omdb.make to exact migration 89, preserve custom-title flags, search text, run counts and native-only edits across restart, rebuild worktree/family reads, retain source/import proof, retry interrupted activation and reject changed SQLite sources without writes";
  } finally {
    await fixture.close();
  }
}
