#!/usr/bin/env node
import * as NodeAssert from "node:assert/strict";
import * as NodeChildProcess from "node:child_process";
import * as NodeCrypto from "node:crypto";
import * as NodeFSP from "node:fs/promises";
import * as NodeModule from "node:module";
import * as NodePath from "node:path";
import * as NodeSqlite from "node:sqlite";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Contracts from "@t3tools/contracts";
import { OrchestrationV2DomainEventJson } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Logger from "effect/Logger";
import * as Option from "effect/Option";
import * as PubSub from "effect/PubSub";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/sql/SqlClient";
import * as SqlSchema from "effect/sql/SqlSchema";

import * as EventRows from "../apps/server/src/persistence/EventRows.ts";
import * as NativeEventRows from "../apps/server/src/persistence/NativeEventRows.ts";
import * as SqliteEventRows from "../apps/server/src/persistence/SqliteEventRows.ts";
import * as OrchestrationEventStore from "../apps/server/src/persistence/OrchestrationEventStore.ts";
import * as NativeDatabase from "../apps/server/src/orchestration-v2/NativeDatabase.ts";
import * as RedbClient from "../apps/server/src/persistence/RedbClient.ts";
import { replayAndBufferProjectedLiveEvents } from "../apps/server/src/orchestration-v2/LiveStreamBudget.ts";
import {
  toPersistenceDecodeError,
  toPersistenceSqlError,
} from "../apps/server/src/persistence/Errors.ts";
import { createFixture } from "./database-fixture.mjs";
import { exportSqliteState } from "./export-sqlite-state.mjs";
import { upstreamSource } from "./upstream-source.mjs";

const [executable, proofPath] = process.argv.slice(2);
NodeAssert.ok(
  executable && NodePath.isAbsolute(executable),
  "Pass an absolute native database executable",
);
await NodeFSP.access(executable, NodeFSP.constants.X_OK);
const checks = [];
const at = "2026-11-10T12:00:00.000Z";
const now = DateTime.makeUnsafe(at);
const encodeEvent = Schema.encodeSync(OrchestrationV2DomainEventJson);
const hash = (bytes) => NodeCrypto.createHash("sha256").update(bytes).digest("hex");
const oraclePath = "apps/server/src/persistence/OrchestrationEventStore.ts";
const oracleCommit = "4aec277f79b2d4dabd88f13cfab264d9f80cbcef";
const oracleBlob = "e55d82713871bce675d605da926da0514879b422";
const upstreamLock = JSON.parse(
  await NodeFSP.readFile(new URL("../upstream.lock.json", import.meta.url), "utf8"),
);
NodeAssert.equal(upstreamLock.upstream.commit, oracleCommit);
const originalSource = upstreamSource(oraclePath);
NodeAssert.equal(
  NodeCrypto.createHash("sha1")
    .update(`blob ${Buffer.byteLength(originalSource)}\0`)
    .update(originalSource)
    .digest("hex"),
  oracleBlob,
);
const source = NodeModule.stripTypeScriptTypes(originalSource, { mode: "strip" })
  .replace(/^import\s[\s\S]*?;\s*$/gm, "")
  .replace(/^export (?=(?:function|const|class) )/gm, "");
const oracleImports = {
  Context,
  NodeSqliteClient,
  SqlClient,
  SqlSchema,
  Effect,
  Layer,
  PubSub,
  Option,
  Schema,
  Stream,
  replayAndBufferProjectedLiveEvents,
  toPersistenceDecodeError,
  toPersistenceSqlError,
  ...Object.fromEntries(
    [
      "ApplicationEventMetadata",
      "ApplicationProjectEvent",
      "CommandId",
      "EventId",
      "IsoDateTime",
      "isKnownOrchestrationV2EventType",
      "NonNegativeInt",
      "OrchestrationV2DomainEventJson",
      "OrchestrationV2StoredEvent",
      "ProjectId",
      "StoredProjectIcon",
      "ThreadId",
    ].map((name) => [name, Contracts[name]]),
  ),
};
// Execute the pinned service unchanged, apart from imports and TypeScript syntax.
// The independent service factory keeps shared-refactor mistakes out of the oracle.
const upstream = new Function(
  ...Object.keys(oracleImports),
  `${source}\nreturn { OrchestrationEventStore, layer };`,
)(...Object.values(oracleImports));
const collect = (stream) => stream.pipe(Stream.runCollect, Effect.map(Array.from));
const normalized = (value) => JSON.parse(JSON.stringify(value));
const detached = (id, threadId = "thread:written") => ({
  id,
  type: "provider-session.detached",
  threadId,
  occurredAt: now,
  payload: { providerSessionId: `session:${id}`, detachedAt: now },
});
const project = (id, commandId = null) => ({
  eventId: id,
  type: "project.created",
  aggregateKind: "project",
  aggregateId: `project:${id}`,
  occurredAt: at,
  commandId,
  causationEventId: null,
  correlationId: commandId,
  metadata: {},
  payload: {
    projectId: `project:${id}`,
    title: "Preserved project 🦊",
    workspaceRoot: `/fixture/${id}`,
    defaultModelSelection: null,
    scripts: [],
    createdAt: at,
    updatedAt: at,
  },
});
const subagent = (
  id,
  progress,
  prompt = "Inspect the repository. 🦊 ".repeat(200),
  task = "task:prompt",
) => ({
  id,
  type: "subagent.updated",
  threadId: "thread:prompt",
  occurredAt: now,
  payload: {
    id: task,
    threadId: "thread:prompt",
    runId: null,
    parentNodeId: "node:parent",
    origin: "provider_native",
    createdBy: "agent",
    driver: "claudeAgent",
    providerInstanceId: "claudeAgent",
    providerThreadId: null,
    childThreadId: null,
    nativeTaskRef: null,
    prompt,
    progress,
    title: "Inspect",
    model: null,
    status: "running",
    result: null,
    startedAt: now,
    completedAt: null,
    updatedAt: now,
  },
});
const subagentItem = (id, progress) => ({
  id,
  type: "turn-item.updated",
  threadId: "thread:prompt",
  occurredAt: now,
  payload: {
    id: "item:prompt",
    type: "subagent",
    threadId: "thread:prompt",
    runId: null,
    nodeId: null,
    providerThreadId: null,
    providerTurnId: null,
    nativeItemRef: null,
    parentItemId: null,
    ordinal: 1,
    status: "running",
    title: "Inspect",
    subagentId: "task:prompt",
    origin: "provider_native",
    driver: "claudeAgent",
    providerInstanceId: "claudeAgent",
    childThreadId: null,
    prompt: "Inspect the repository. 🦊 ".repeat(200),
    progress,
    result: null,
    startedAt: now,
    completedAt: null,
    updatedAt: now,
  },
});

function createSource(filename) {
  const database = new NodeSqlite.DatabaseSync(filename);
  try {
    database.exec(`
      CREATE TABLE orchestration_events (
        sequence INTEGER PRIMARY KEY AUTOINCREMENT,
        event_id TEXT NOT NULL UNIQUE, aggregate_kind TEXT NOT NULL,
        stream_id TEXT NOT NULL, stream_version INTEGER NOT NULL,
        event_type TEXT NOT NULL, occurred_at TEXT NOT NULL, command_id TEXT,
        causation_event_id TEXT, correlation_id TEXT, actor_kind TEXT NOT NULL,
        payload_json TEXT NOT NULL, metadata_json TEXT NOT NULL,
        application_event_version INTEGER NOT NULL DEFAULT 1
      );
      CREATE UNIQUE INDEX idx_orch_events_stream_version ON orchestration_events(aggregate_kind, stream_id, stream_version);
      CREATE INDEX idx_orch_events_command_id ON orchestration_events(command_id);
      CREATE INDEX idx_orchestration_events_application_sequence ON orchestration_events(application_event_version, sequence);
      CREATE INDEX idx_orchestration_events_application_high_water ON orchestration_events(sequence)
        WHERE aggregate_kind = 'project' OR (application_event_version = 2 AND aggregate_kind = 'thread');
      CREATE INDEX idx_orchestration_events_agent_stream_sequence ON orchestration_events(stream_id, sequence)
        WHERE application_event_version = 2 AND aggregate_kind = 'thread';
      CREATE TABLE orchestration_v2_projection_metadata (
        projection_name TEXT PRIMARY KEY, schema_version INTEGER NOT NULL,
        last_sequence INTEGER NOT NULL, updated_at TEXT NOT NULL
      );
      INSERT INTO orchestration_v2_projection_metadata VALUES ('event-log-pruned-through', 2, 12, '${at}');
      BEGIN;
    `);
    const insert = database.prepare(`INSERT INTO orchestration_events (
      event_id, aggregate_kind, stream_id, stream_version, event_type, occurred_at,
      command_id, causation_event_id, correlation_id, actor_kind, payload_json,
      metadata_json, application_event_version
    ) VALUES (?, ?, ?, ?, ?, ?, ?, NULL, NULL, 'server', ?, '{}', ?)`);
    let version = 0;
    const row = (id, streamId, type, payload, options = {}) =>
      Number(
        insert.run(
          id,
          options.kind ?? "thread",
          streamId,
          version++,
          type,
          at,
          options.command ?? null,
          payload,
          options.version ?? 2,
        ).lastInsertRowid,
      );
    row("legacy:thread", "thread:legacy", "old.thread.event", "{}", { version: 1 });
    const oldProject = project("imported-project");
    const projectSequence = row(
      oldProject.eventId,
      oldProject.aggregateId,
      oldProject.type,
      JSON.stringify(oldProject.payload),
      { kind: "project", version: 1 },
    );
    const unknownStart = projectSequence;
    for (let index = 0; index < 501; index++) {
      row(`future:${index}`, "thread:future", "future.unrecognized", "{}");
    }
    const knownAfterUnknown = row(
      "future:known",
      "thread:future",
      "provider-session.detached",
      JSON.stringify({ providerSessionId: "session:future-known", detachedAt: at }),
    );
    const commandStart = knownAfterUnknown;
    for (let index = 0; index < 603; index++) {
      row(
        `page:${index}`,
        "thread:pages",
        "provider-session.detached",
        JSON.stringify({ providerSessionId: `session:page:${index}`, detachedAt: at }),
        { command: "command:pages" },
      );
    }
    const commandEnd = Number(
      database.prepare("SELECT MAX(sequence) AS n FROM orchestration_events").get().n,
    );
    const unicodePayload = '{"text":"🦊"}';
    const malformedPayload = "{" + "x".repeat(1_048_576);
    const statsFirst = row("stats:first", "thread:stats", "thread.created", unicodePayload);
    row("stats:other", "thread:other", "thread.metadata-updated", malformedPayload);
    row("stats:legacy", "thread:stats", "thread.metadata-updated", malformedPayload, {
      version: 1,
    });
    const statsLast = row(
      "stats:malformed",
      "thread:stats",
      "thread.metadata-updated",
      malformedPayload,
    );
    const importedPrompt = encodeEvent(
      subagent("prompt:imported", "Imported", undefined, "task:imported"),
    );
    const anchor = row(
      importedPrompt.id,
      importedPrompt.threadId,
      importedPrompt.type,
      JSON.stringify(importedPrompt.payload),
    );
    const reference = row(
      "prompt:imported-reference",
      importedPrompt.threadId,
      importedPrompt.type,
      JSON.stringify({
        ...importedPrompt.payload,
        progress: "Referenced",
        prompt: "",
        promptSequence: anchor,
      }),
    );
    database.exec(`UPDATE sqlite_sequence SET seq = 9000 WHERE name = 'orchestration_events'`);
    row("legacy:high-water", "thread:legacy", "old.thread.event", "{}", { version: 1 });
    database.exec(
      `UPDATE sqlite_sequence SET seq = 10000 WHERE name = 'orchestration_events'; COMMIT;`,
    );
    return {
      projectSequence,
      unknownStart,
      knownAfterUnknown,
      commandStart,
      commandEnd,
      statsFirst,
      statsLast,
      anchor,
      reference,
      unicodePayload,
      malformedPayload,
    };
  } finally {
    database.close();
  }
}

function cliResponse(mode, input, target = fixture) {
  const result = NodeChildProcess.spawnSync(executable, target.cliArgs(mode), {
    env: {},
    input: JSON.stringify(input) + "\n",
    encoding: "utf8",
    timeout: 45_000,
  });
  NodeAssert.equal(result.status, 0, result.stderr);
  return JSON.parse(result.stdout);
}

function cli(mode, input, target = fixture) {
  const response = cliResponse(mode, input, target);
  NodeAssert.equal(response.ok, true, JSON.stringify(response));
  return response.result;
}

async function unsupportedTextEncoding(column) {
  const target = await createFixture();
  try {
    const source = NodePath.join(target.root, "unsupported-text.sqlite");
    const exported = NodePath.join(target.root, "unsupported-text.ndjson");
    const restored = NodePath.join(target.root, "restored.ndjson");
    const seed = createSource(source);
    const database = new NodeSqlite.DatabaseSync(source);
    try {
      database
        .prepare("DELETE FROM orchestration_events WHERE sequence != ?")
        .run(seed.knownAfterUnknown);
      NodeAssert.ok(column === "event_id" || column === "stream_id");
      database.exec(`UPDATE orchestration_events SET ${column} = CAST(X'80' AS TEXT)`);
    } finally {
      database.close();
    }
    const sourceHash = hash(await NodeFSP.readFile(source));
    const summary = exportSqliteState({ source, destination: exported });
    const imported = cli(
      "initialize",
      { operation: "import_sqlite_application", path: exported },
      target,
    );
    NodeAssert.equal(imported.summary.export_sha256, summary.sha256);
    const prepared = cliResponse("connect", { operation: "application_events_prepare" }, target);
    NodeAssert.equal(
      prepared.ok,
      false,
      `${column}: base64-tagged SQLite TEXT must not become literal event text`,
    );
    NodeAssert.equal(prepared.error.kind, "incompatible_store");
    cli(
      "connect",
      {
        operation: "restore_sqlite_export",
        archive_id: imported.generation,
        destination: restored,
      },
      target,
    );
    NodeAssert.equal(
      hash(await NodeFSP.readFile(restored)),
      hash(await NodeFSP.readFile(exported)),
    );
    NodeAssert.equal(hash(await NodeFSP.readFile(source)), sourceHash);
  } finally {
    await target.close();
  }
}

async function promptScalarRestoration() {
  const target = await createFixture();
  try {
    const source = NodePath.join(target.root, "prompt-scalars.sqlite");
    const exported = NodePath.join(target.root, "prompt-scalars.ndjson");
    createSource(source);
    const cases = [
      {
        name: "array",
        encoded: '[ 1, { "nested": [ true, null ] } ]',
        expected: [1, { nested: [true, null] }],
      },
      {
        name: "object",
        encoded: '{ "nested": [ 1, 2 ], "text": "🦊" }',
        expected: { nested: [1, 2], text: "🦊" },
      },
      { name: "true", encoded: "true", expected: 1 },
      { name: "false", encoded: "false", expected: 0 },
      { name: "integer", encoded: "-42", expected: -42 },
      {
        name: "reference-negative-zero",
        encoded: '"Anchor prompt"',
        reference: "-0",
        expected: null,
      },
      {
        name: "reference-out-of-range",
        encoded: '"Anchor prompt"',
        reference: "18446744073709551616",
        expected: null,
      },
      {
        name: "reference-real",
        encoded: '"Anchor prompt"',
        reference: "1.0",
        expected: "Original prompt",
        decodes: true,
      },
    ];
    const database = new NodeSqlite.DatabaseSync(source);
    try {
      database.exec("DELETE FROM orchestration_events");
      const insert = database.prepare(`INSERT INTO orchestration_events (
        sequence, event_id, aggregate_kind, stream_id, stream_version, event_type, occurred_at,
        command_id, causation_event_id, correlation_id, actor_kind, payload_json,
        metadata_json, application_event_version
      ) VALUES (?, ?, 'thread', 'thread:prompt', ?, 'subagent.updated', ?, NULL, NULL, NULL, 'server', ?, '{}', 2)`);
      for (const [index, test] of cases.entries()) {
        const anchor = index * 2 + 1;
        const base = encodeEvent(subagent(`scalar:${test.name}`, "Imported")).payload;
        const withoutPrompt = JSON.stringify({ ...base, prompt: undefined });
        insert.run(
          anchor,
          `scalar:${test.name}:anchor`,
          anchor,
          at,
          `${withoutPrompt.slice(0, -1)},"prompt":${test.encoded}}`,
        );
        const reference = JSON.stringify({
          ...base,
          prompt: test.reference === undefined ? "" : "Original prompt",
        });
        insert.run(
          anchor + 1,
          `scalar:${test.name}:reference`,
          anchor + 1,
          at,
          `${reference.slice(0, -1)},"promptSequence":${test.reference ?? anchor}}`,
        );
      }
    } finally {
      database.close();
    }
    const sourceHash = hash(await NodeFSP.readFile(source));
    exportSqliteState({ source, destination: exported });
    cli("initialize", { operation: "import_sqlite_application", path: exported }, target);
    const inspect = Effect.fn("EventsFixture.inspectPromptScalars")(function* ({ rows, store }) {
      const restored = yield* rows.read({
        after_sequence: 0,
        through_sequence: cases.length * 2,
        only_agent_events: true,
        limit: cases.length * 2,
      });
      NodeAssert.equal(restored.length, cases.length * 2);
      for (const [index, test] of cases.entries()) {
        const reference = restored[index * 2 + 1];
        NodeAssert.deepEqual(JSON.parse(reference.payload_json).prompt, test.expected);
        NodeAssert.equal(
          JSON.parse(reference.payload_json).promptSequence,
          test.reference === "1.0" ? 1 : undefined,
        );
        const decoded = collect(
          store.readAgentEvents({ afterSequence: index * 2 + 1, throughSequence: index * 2 + 2 }),
        );
        if (test.decodes) {
          const events = yield* decoded;
          NodeAssert.equal(events[0].event.payload.prompt, test.expected);
        } else {
          yield* failure(decoded, "PersistenceDecodeError");
        }
      }
      const stats = yield* rows.stats({
        kind: "agent",
        thread_id: "thread:prompt",
        after_sequence: 0,
        through_sequence: cases.length * 2,
        max_events: cases.length * 2,
      });
      NodeAssert.equal(
        stats.raw_payload_bytes,
        restored.reduce((total, row) => total + Buffer.byteLength(row.payload_json), 0),
      );
      return normalized({ restored, stats });
    });
    const expected = await withBackend("sqlite", source, inspect);
    const actual = await withBackend("redb", source, inspect, target);
    NodeAssert.deepEqual(
      actual,
      expected,
      "Restored prompt scalars must retain SQLite JSON subtype and exact bytes",
    );
    NodeAssert.equal(hash(await NodeFSP.readFile(source)), sourceHash);
  } finally {
    await target.close();
  }
}

async function unsupportedRealPrompt() {
  const target = await createFixture();
  try {
    const source = NodePath.join(target.root, "real-prompt.sqlite");
    const exported = NodePath.join(target.root, "real-prompt.ndjson");
    const restored = NodePath.join(target.root, "restored.ndjson");
    const seed = createSource(source);
    const database = new NodeSqlite.DatabaseSync(source);
    try {
      database
        .prepare("DELETE FROM orchestration_events WHERE sequence NOT IN (?, ?)")
        .run(seed.anchor, seed.reference);
      database
        .prepare(
          "UPDATE orchestration_events SET payload_json = json_set(payload_json, '$.prompt', 1.5) WHERE sequence = ?",
        )
        .run(seed.anchor);
    } finally {
      database.close();
    }
    const expected = await withBackend("sqlite", source, ({ rows }) =>
      rows.read({
        after_sequence: seed.anchor,
        through_sequence: seed.reference,
        only_agent_events: true,
        limit: 1,
      }),
    );
    NodeAssert.equal(JSON.parse(expected[0].payload_json).prompt, 1.5);
    const sourceHash = hash(await NodeFSP.readFile(source));
    const summary = exportSqliteState({ source, destination: exported });
    const imported = cli(
      "initialize",
      { operation: "import_sqlite_application", path: exported },
      target,
    );
    NodeAssert.equal(imported.summary.export_sha256, summary.sha256);
    const prepared = cliResponse("connect", { operation: "application_events_prepare" }, target);
    NodeAssert.equal(
      prepared.ok,
      false,
      "Unsupported SQLite REAL prompt coercion must fail preparation",
    );
    NodeAssert.equal(prepared.error.kind, "incompatible_store");
    cli(
      "connect",
      {
        operation: "restore_sqlite_export",
        archive_id: imported.generation,
        destination: restored,
      },
      target,
    );
    NodeAssert.equal(
      hash(await NodeFSP.readFile(restored)),
      hash(await NodeFSP.readFile(exported)),
    );
    NodeAssert.equal(hash(await NodeFSP.readFile(source)), sourceHash);
  } finally {
    await target.close();
  }
}

function failure(effect, tag) {
  return effect.pipe(
    Effect.flip,
    Effect.tap((error) => Effect.sync(() => NodeAssert.equal(error._tag, tag))),
  );
}

async function withBackend(kind, sqlitePath, use, target = fixture) {
  const work = Effect.gen(function* () {
    let rows;
    let transaction;
    let rawPayload;
    if (kind !== "redb") {
      const sql = yield* SqlClient.SqlClient;
      if (kind === "sqlite") rows = yield* SqliteEventRows.make;
      transaction = (body) => sql.withTransaction(body);
      rawPayload = (sequence) =>
        sql`SELECT payload_json FROM orchestration_events WHERE sequence = ${sequence}`.pipe(
          Effect.map((result) => result[0]?.payload_json),
        );
    } else {
      const database = yield* NativeDatabase.make({
        executable,
        databasePath: target.databasePath,
      });
      const client = yield* RedbClient.make.pipe(
        Effect.provideService(NativeDatabase.NativeDatabase, database),
      );
      rows = yield* NativeEventRows.make.pipe(Effect.provideService(RedbClient.RedbClient, client));
      transaction = (body) => client.withTransaction(body);
      rawPayload = (sequence) =>
        client
          .get("orchestration_events", [{ type: "integer", value: String(sequence) }])
          .pipe(Effect.map((result) => result?.payload_json?.value));
    }
    const services = yield* kind === "upstream"
      ? Layer.build(upstream.layer)
      : Layer.build(OrchestrationEventStore.layerFromEventRows).pipe(
          Effect.provideService(EventRows.EventRows, rows),
        );
    const store = Context.get(
      services,
      kind === "upstream"
        ? upstream.OrchestrationEventStore
        : OrchestrationEventStore.OrchestrationEventStore,
    );
    return yield* use({ store, rows, transaction, rawPayload });
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer), Effect.provide(Logger.layer([])));
  return await Effect.runPromise(
    kind === "redb"
      ? work
      : work.pipe(Effect.provide(NodeSqliteClient.layer({ filename: sqlitePath }))),
  );
}

function exercise(seed) {
  return ({ store, transaction, rawPayload }) =>
    Effect.gen(function* () {
      const observations = {};
      observations.initial = {
        application: yield* store.latestApplicationSequence,
        agent: yield* store.latestAgentSequence(),
        legacy: yield* store.latestAgentSequence("thread:legacy"),
        missing: yield* store.latestAgentSequence("thread:missing"),
        pruned: yield* store.prunedThroughSequence,
      };
      NodeAssert.deepEqual(observations.initial, {
        application: seed.reference,
        agent: seed.reference,
        legacy: 0,
        missing: 0,
        pruned: 12,
      });
      NodeAssert.deepEqual(yield* store.appendAgentEvents({ events: [] }), []);
      NodeAssert.equal(yield* store.latestApplicationSequence, seed.reference);
      observations.command = yield* collect(store.readAgentEvents({ commandId: "command:pages" }));
      NodeAssert.equal(observations.command.length, 603);
      NodeAssert.equal(observations.command[0].sequence, seed.commandStart + 1);
      NodeAssert.equal(observations.command.at(-1).sequence, seed.commandEnd);
      NodeAssert.deepEqual(yield* collect(store.readAgentEvents({ limit: 0 })), []);
      observations.filtered = yield* collect(
        store.readAgentEvents({
          threadId: "thread:pages",
          commandId: "command:pages",
          eventType: "provider-session.detached",
          afterSequence: seed.commandStart + 9,
          throughSequence: seed.commandStart + 12,
          limit: 1.9,
        }),
      );
      NodeAssert.equal(observations.filtered.length, 1);
      NodeAssert.equal(observations.filtered[0].sequence, seed.commandStart + 10);
      yield* failure(
        collect(store.readAgentEvents({ threadId: "thread:future" })),
        "PersistenceDecodeError",
      );
      observations.unknownPage = yield* collect(
        store.readAgentEvents({
          threadId: "thread:future",
          skipUnknownEventTypes: true,
        }),
      );
      NodeAssert.deepEqual(
        observations.unknownPage.map((event) => event.sequence),
        [seed.knownAfterUnknown],
      );
      observations.application = yield* collect(
        store.readApplicationEvents({
          afterSequence: 0,
          throughSequence: seed.knownAfterUnknown,
          skipUnknownEventTypes: true,
        }),
      );
      NodeAssert.deepEqual(
        observations.application.map((event) => event.sequence),
        [seed.projectSequence, seed.knownAfterUnknown],
      );
      yield* failure(
        collect(store.readAgentEvents({ threadId: "thread:stats", skipUnknownEventTypes: true })),
        "PersistenceDecodeError",
      );
      yield* failure(
        collect(
          store.readApplicationEvents({
            afterSequence: seed.statsFirst - 1,
            throughSequence: seed.statsLast,
            skipUnknownEventTypes: true,
          }),
        ),
        "PersistenceDecodeError",
      );
      const range = {
        threadId: "thread:stats",
        afterSequence: seed.statsFirst - 1,
        throughSequence: seed.statsLast,
        maxEvents: 128,
      };
      observations.stats = yield* store.getAgentReplayStats(range);
      NodeAssert.deepEqual(observations.stats, {
        eventCount: 2,
        rawPayloadBytes:
          Buffer.byteLength(seed.unicodePayload) + Buffer.byteLength(seed.malformedPayload),
        hasCreateEvent: true,
      });
      observations.boundedStats = yield* store.getAgentReplayStats({ ...range, maxEvents: 0 });
      NodeAssert.deepEqual(observations.boundedStats, {
        eventCount: 1,
        rawPayloadBytes: Buffer.byteLength(seed.unicodePayload),
        hasCreateEvent: true,
      });
      observations.allStats = yield* store.getReplayStats({
        afterSequence: seed.statsFirst - 1,
        throughSequence: seed.statsLast,
      });
      NodeAssert.equal(observations.allStats.eventCount, 3);
      NodeAssert.equal(
        observations.allStats.rawPayloadBytes,
        Buffer.byteLength(seed.unicodePayload) + 2 * Buffer.byteLength(seed.malformedPayload),
      );
      observations.importedPrompt = yield* collect(
        store.readAgentEvents({
          afterSequence: seed.anchor - 1,
          throughSequence: seed.reference,
        }),
      );
      NodeAssert.equal(
        observations.importedPrompt[0].event.payload.prompt,
        observations.importedPrompt[1].event.payload.prompt,
      );

      observations.written = yield* transaction(
        Effect.gen(function* () {
          const first = yield* store.appendProjectEvent(
            project("written:project", "command:written-project"),
          );
          const second = yield* store.appendAgentEvents({
            commandId: "command:written",
            events: [
              {
                ...detached("written:agent"),
                rawEventId: "raw:retained",
                runId: "run:retained",
                nodeId: "node:retained",
                driver: "codex",
                providerInstanceId: "codex",
              },
            ],
          });
          NodeAssert.equal(yield* store.latestApplicationSequence, second[0].sequence);
          return [first, ...second];
        }),
      );
      NodeAssert.deepEqual(
        observations.written.map((event) => event.sequence),
        [10001, 10002],
      );
      NodeAssert.deepEqual(
        yield* collect(store.readAgentEvents({ commandId: "command:written" })),
        [observations.written[1]],
      );

      const repeated = [
        subagent("prompt:1", "First"),
        subagent("prompt:2", "Second"),
        subagent("prompt:3", "Different", "A changed prompt."),
        subagentItem("prompt:item:1", "First item"),
        subagentItem("prompt:item:2", "Second item"),
      ];
      observations.prompts = yield* transaction(store.appendAgentEvents({ events: repeated }));
      const firstRaw = JSON.parse(yield* rawPayload(observations.prompts[0].sequence));
      const secondRaw = JSON.parse(yield* rawPayload(observations.prompts[1].sequence));
      const thirdRaw = JSON.parse(yield* rawPayload(observations.prompts[2].sequence));
      NodeAssert.equal(firstRaw.prompt, repeated[0].payload.prompt);
      NodeAssert.equal(secondRaw.prompt, "");
      NodeAssert.equal(secondRaw.promptSequence, observations.prompts[0].sequence);
      NodeAssert.equal(thirdRaw.prompt, "A changed prompt.");
      NodeAssert.equal(thirdRaw.promptSequence, undefined);
      const itemReference = JSON.parse(yield* rawPayload(observations.prompts[4].sequence));
      NodeAssert.equal(itemReference.prompt, "");
      NodeAssert.equal(itemReference.promptSequence, observations.prompts[3].sequence);
      observations.promptReplay = yield* collect(
        store.readAgentEvents({
          afterSequence: observations.prompts[0].sequence - 1,
          throughSequence: observations.prompts.at(-1).sequence,
        }),
      );
      NodeAssert.deepEqual(
        observations.promptReplay.map((event) => event.event),
        repeated,
      );
      observations.promptStats = yield* store.getAgentReplayStats({
        threadId: "thread:prompt",
        afterSequence: observations.prompts[0].sequence - 1,
        throughSequence: observations.prompts.at(-1).sequence,
        maxEvents: 100,
      });

      const beforeFailure = yield* store.latestApplicationSequence;
      const discarded = subagent("prompt:discarded", "Discarded", undefined, "task:rolled-back");
      const exit = yield* transaction(
        store
          .appendAgentEvents({ events: [discarded] })
          .pipe(Effect.andThen(Effect.fail("rollback fixture"))),
      ).pipe(Effect.exit);
      NodeAssert.ok(Exit.isFailure(exit));
      NodeAssert.equal(yield* store.latestApplicationSequence, beforeFailure);
      const replacement = subagent(
        "prompt:replacement",
        "Replacement",
        undefined,
        "task:rolled-back",
      );
      observations.afterRollback = yield* transaction(
        store.appendAgentEvents({ events: [replacement] }),
      );
      NodeAssert.equal(observations.afterRollback[0].sequence, beforeFailure + 1);
      NodeAssert.equal(
        JSON.parse(yield* rawPayload(observations.afterRollback[0].sequence)).promptSequence,
        undefined,
      );

      const beforeDuplicate = yield* store.latestApplicationSequence;
      yield* failure(
        transaction(
          store.appendAgentEvents({
            events: [detached("duplicate:before"), detached("written:agent")],
          }),
        ),
        "PersistenceSqlError",
      );
      NodeAssert.equal(yield* store.latestApplicationSequence, beforeDuplicate);
      NodeAssert.deepEqual(
        yield* collect(store.readAgentEvents({ commandId: "missing-command" })),
        [],
      );
      observations.savepoint = yield* transaction(
        Effect.gen(function* () {
          const first = yield* store.appendAgentEvents({ events: [detached("savepoint:first")] });
          yield* transaction(
            store
              .appendAgentEvents({ events: [detached("savepoint:discarded")] })
              .pipe(Effect.andThen(Effect.fail("savepoint fixture"))),
          ).pipe(Effect.catch(() => Effect.void));
          const last = yield* store.appendAgentEvents({ events: [detached("savepoint:last")] });
          return [...first, ...last];
        }),
      );
      NodeAssert.deepEqual(
        observations.savepoint.map((event) => event.sequence),
        [beforeDuplicate + 1, beforeDuplicate + 2],
      );

      const replayReady = yield* Deferred.make();
      const initial = observations.savepoint.at(-1);
      const subscription = yield* store
        .streamApplicationEvents({ afterSequence: initial.sequence - 1 })
        .pipe(
          Stream.tap((event) =>
            event.sequence === initial.sequence
              ? Deferred.succeed(replayReady, undefined)
              : Effect.void,
          ),
          Stream.take(2),
          Stream.runCollect,
          Effect.forkChild,
        );
      yield* Deferred.await(replayReady);
      const live = yield* transaction(
        store.appendAgentEvents({ events: [detached("live:agent")] }),
      );
      yield* Effect.yieldNow;
      NodeAssert.equal(
        subscription.pollUnsafe(),
        undefined,
        "Appends must not publish before the owner requests publication",
      );
      yield* store.publishCommitted(live);
      observations.live = Array.from(yield* Fiber.join(subscription));
      NodeAssert.deepEqual(
        observations.live.map((event) => event.sequence),
        [initial.sequence, live[0].sequence],
      );

      const firstProject = yield* transaction(store.appendProjectEvent(project("project:replay")));
      const projectedReady = yield* Deferred.make();
      const projected = yield* store
        .streamProjectedApplicationEvents({
          afterSequence: firstProject.sequence - 1,
          filter: (event) => "aggregateKind" in event,
          project: (event) => ({ sequence: event.sequence, id: event.eventId }),
        })
        .pipe(
          Stream.tap((event) =>
            event.sequence === firstProject.sequence
              ? Deferred.succeed(projectedReady, undefined)
              : Effect.void,
          ),
          Stream.take(2),
          Stream.runCollect,
          Effect.forkChild,
        );
      yield* Deferred.await(projectedReady);
      const omitted = yield* transaction(
        store.appendAgentEvents({ events: [detached("project:filtered-agent")] }),
      );
      yield* store.publishCommitted(omitted);
      const lastProject = yield* transaction(store.appendProjectEvent(project("project:live")));
      yield* store.publishCommitted([lastProject]);
      observations.projected = Array.from(yield* Fiber.join(projected));
      NodeAssert.deepEqual(
        observations.projected.map((event) => event.id),
        [firstProject.eventId, lastProject.eventId],
      );
      observations.finalSequence = yield* store.latestApplicationSequence;
      observations.finalAgentSequence = yield* store.latestAgentSequence();
      return normalized(observations);
    });
}

const fixture = await createFixture();
try {
  const source = NodePath.join(fixture.root, "source.sqlite");
  const sqlitePath = NodePath.join(fixture.root, "oracle.sqlite");
  const upstreamPath = NodePath.join(fixture.root, "upstream-oracle.sqlite");
  const exported = NodePath.join(fixture.root, "source.ndjson");
  const seed = createSource(source);
  const sourceHash = hash(await NodeFSP.readFile(source));
  await NodeFSP.copyFile(source, sqlitePath);
  await NodeFSP.copyFile(source, upstreamPath);
  exportSqliteState({ source, destination: exported });
  cli("initialize", { operation: "import_sqlite_application", path: exported });
  const expected = await withBackend("upstream", upstreamPath, exercise(seed));
  const refactored = await withBackend("sqlite", sqlitePath, exercise(seed));
  NodeAssert.deepEqual(
    refactored,
    expected,
    "The refactored SQLite event service must match immutable upstream",
  );
  const actual = await withBackend("redb", sqlitePath, exercise(seed));
  NodeAssert.deepEqual(
    actual,
    expected,
    "The native event service must match immutable upstream SQLite",
  );
  checks.push(
    "All 12 application event service members agree across immutable upstream SQLite, refactored SQLite and real redb",
    "Imported legacy history, application high-water and sqlite_sequence allocation retain distinct meanings",
    "Command replay crosses bounded pages; unknown rows advance cursors; malformed known events fail decoding",
    "Replay stats bound raw rows and count restored UTF-8 payload bytes without decoding unrelated payloads",
    "Project and agent appends share sequence allocation, preserve metadata and read their uncommitted writes",
    "Prompt anchors deduplicate, restore, and recover safely after a rolled-back hint",
    "Duplicate event IDs abort the transaction; savepoint rollback retains surrounding events and sequence state",
    "Replay-to-live and projected filtered streams publish only after explicit committed notification",
  );
  const reopened = await withBackend("redb", sqlitePath, ({ store, transaction }) =>
    Effect.gen(function* () {
      NodeAssert.equal(yield* store.latestApplicationSequence, expected.finalSequence);
      NodeAssert.equal(yield* store.latestAgentSequence(), expected.finalAgentSequence);
      NodeAssert.equal(yield* store.prunedThroughSequence, 12);
      const committed = yield* transaction(
        store.appendAgentEvents({ events: [detached("reopened:agent")] }),
      );
      NodeAssert.equal(committed[0].sequence, expected.finalSequence + 1);
      return committed[0].sequence;
    }),
  );
  NodeAssert.equal(hash(await NodeFSP.readFile(source)), sourceHash);
  checks.push(
    "Reopening the actual helper retains committed rows, sequence allocation and prune metadata without changing SQLite source",
  );
  for (const column of ["event_id", "stream_id"]) await unsupportedTextEncoding(column);
  checks.push(
    "Unsupported encoded SQLite TEXT identities fail event preparation while the original source and archived export remain intact",
  );
  await promptScalarRestoration();
  checks.push(
    "Prompt restoration preserves SQLite JSON subtypes, boolean coercion, integer-reference rules and exact byte counts; malformed domain prompts still fail decoding",
  );
  await unsupportedRealPrompt();
  checks.push(
    "Unsupported SQLite REAL prompt coercion fails preparation while preserving the source and archived export",
  );
  const proof = {
    schema: 1,
    engine: fixture.engineLabel,
    checks,
    oracle: {
      commit: oracleCommit,
      blob: oracleBlob,
      path: oraclePath,
      sha256: hash(originalSource),
    },
    executable_sha256: hash(await NodeFSP.readFile(executable)),
    sqlite_source_sha256: sourceHash,
    final_sequence: reopened,
  };
  if (proofPath) await NodeFSP.writeFile(proofPath, JSON.stringify(proof, null, 2) + "\n");
  console.log(JSON.stringify(proof, null, 2));
} finally {
  await fixture.close();
}
