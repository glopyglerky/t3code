#!/usr/bin/env node
import * as Assert from "node:assert/strict";
import * as FS from "node:fs/promises";
import * as Path from "node:path";
import * as Sqlite from "node:sqlite";
import * as Contracts from "@t3tools/contracts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Arr from "effect/Array";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import * as Struct from "effect/Struct";
import * as SqlClient from "effect/sql/SqlClient";
import * as SqlSchema from "effect/sql/SqlSchema";

import * as Runtime from "../apps/server/src/persistence/ProviderSessionRuntime.ts";
import * as NativeRuntime from "../apps/server/src/persistence/NativeProviderSessionRuntime.ts";
import * as Mcp from "../apps/server/src/mcpApps/McpAppModelContext.ts";
import * as NativeMcp from "../apps/server/src/mcpApps/NativeMcpAppModelContext.ts";
import * as Errors from "../apps/server/src/persistence/Errors.ts";
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
Assert.ok(executable && Path.isAbsolute(executable), "Pass an absolute native database executable");
await FS.access(executable, FS.constants.X_OK);
const imports = {
  ...Contracts,
  ...Errors,
  Arr,
  NodeSqliteClient,
  Context,
  DateTime,
  Effect,
  Layer,
  Option,
  Result,
  Schema,
  Struct,
  SqlClient,
  SqlSchema,
};
const runtimeOracle = loadUpstream(
  "apps/server/src/persistence/ProviderSessionRuntime.ts",
  imports,
  ["make"],
);
const contextOracle = loadUpstream("apps/server/src/mcpApps/McpAppModelContext.ts", imports, [
  "layer",
  "McpAppModelContext",
]);
const fixture = await createFixture();
const checks = [];
const at = "2026-11-10T12:00:00.000Z";
const later = "2026-11-11T12:00:00.000Z";
const transcript = (filePath, size = 3) => ({
  provider: "codex",
  providerInstanceId: "codex",
  providerSessionId: "native-session",
  filePath,
  size,
  mtimeMs: null,
  device: 2,
  inode: "9007199254740993",
  birthtimeMs: null,
});
const runtime = (threadId, values = {}) => ({
  threadId,
  providerName: "codex",
  providerInstanceId: "codex",
  adapterKey: "codex",
  runtimeMode: "full-access",
  status: "running",
  lastSeenAt: at,
  resumeCursor: { threadId: "native-thread" },
  runtimePayload: { mode: "fixture" },
  ...values,
});
const failed = (body) =>
  Effect.exit(body).pipe(Effect.tap((exit) => Effect.sync(() => Assert.ok(Exit.isFailure(exit)))));

async function services(kind, filename, use) {
  if (kind === "redb")
    return withNative(executable, fixture, (client) =>
      Effect.gen(function* () {
        const provider = yield* NativeRuntime.make;
        const context = yield* NativeMcp.make;
        return yield* use({
          provider,
          context,
          transaction: (body) => client.withTransaction(body),
        });
      }),
    );
  return withSqlite(filename, (sql) =>
    Effect.gen(function* () {
      const provider = yield* kind === "upstream" ? runtimeOracle.module.make : Runtime.make;
      const context = yield* kind === "upstream"
        ? contextOracle.module.McpAppModelContext.pipe(Effect.provide(contextOracle.module.layer))
        : Mcp.McpAppModelContext.pipe(Effect.provide(Mcp.layer));
      return yield* use({ provider, context, transaction: (body) => sql.withTransaction(body) });
    }),
  );
}

function exercise({ provider, context, transaction }) {
  return Effect.gen(function* () {
    const result = {};
    result.initial = yield* provider.list();
    Assert.equal(result.initial.length, 133);
    result.active = yield* provider.list({ excludeStopped: true });
    Assert.equal(result.active.length, 132);
    const corruptError = yield* provider.getByThreadId({ threadId: "corrupt" }).pipe(Effect.flip);
    Assert.equal(corruptError._tag, "PersistenceDecodeError");
    Assert.equal(corruptError.correlation.threadId, "corrupt");
    Assert.ok(Option.isNone(yield* provider.getByThreadId({ threadId: "missing" })));
    yield* provider.recordImportedTranscript({
      threadId: "missing",
      source: transcript("/missing"),
    });
    Assert.ok(Option.isNone(yield* provider.getByThreadId({ threadId: "missing" })));
    yield* provider.upsert(
      runtime("fresh", {
        runtimePayload: { importedTranscripts: [transcript("/stale")], kept: true },
      }),
    );
    Assert.deepEqual(
      Option.getOrThrow(yield* provider.getByThreadId({ threadId: "fresh" })).runtimePayload,
      { kept: true },
    );
    yield* provider.recordImportedTranscript({ threadId: "fresh", source: transcript("/a", 1) });
    yield* provider.recordImportedTranscript({ threadId: "fresh", source: transcript("/b", 2) });
    yield* provider.recordImportedTranscript({ threadId: "fresh", source: transcript("/a", 3) });
    yield* provider.upsert(
      runtime("fresh", { lastSeenAt: later, runtimePayload: null, resumeCursor: null }),
    );
    const preserved = Option.getOrThrow(yield* provider.getByThreadId({ threadId: "fresh" }));
    Assert.deepEqual(preserved.runtimePayload, {
      importedTranscripts: [transcript("/b", 2), transcript("/a", 3)],
    });
    Assert.equal(preserved.resumeCursor, null);
    yield* provider.upsert(runtime("fresh", { status: "stopped", runtimePayload: [] }), {
      onConflict: "ignore",
    });
    Assert.deepEqual(
      Option.getOrThrow(yield* provider.getByThreadId({ threadId: "fresh" })),
      preserved,
    );
    yield* provider.recordImportedTranscript({ threadId: "imported", source: transcript("/a", 4) });
    result.imported = yield* provider.getByThreadId({ threadId: "imported" });
    Assert.deepEqual(result.imported.value.runtimePayload, {
      other: 100,
      importedTranscripts: [transcript("/b", 2), transcript("/a", 4)],
    });
    yield* provider.recordImportedTranscript({ threadId: "corrupt", source: transcript("/fixed") });
    result.repaired = yield* provider.getByThreadId({ threadId: "corrupt" });
    Assert.deepEqual(result.repaired.value.runtimePayload, {
      importedTranscripts: [transcript("/fixed")],
    });
    yield* failed(
      transaction(
        Effect.gen(function* () {
          yield* provider.deleteByThreadId({ threadId: "fresh" });
          yield* provider.upsert(runtime("rolled-back"));
          return yield* Effect.fail("root rollback");
        }),
      ),
    );
    Assert.deepEqual(
      Option.getOrThrow(yield* provider.getByThreadId({ threadId: "fresh" })),
      preserved,
    );
    Assert.ok(Option.isNone(yield* provider.getByThreadId({ threadId: "rolled-back" })));
    yield* transaction(
      Effect.gen(function* () {
        yield* failed(
          transaction(
            Effect.gen(function* () {
              yield* provider.upsert(
                runtime("fresh", { lastSeenAt: "2025-01-01T00:00:00.000Z", runtimePayload: [1] }),
              );
              return yield* Effect.fail("nested rollback");
            }),
          ),
        );
        Assert.deepEqual(
          Option.getOrThrow(yield* provider.getByThreadId({ threadId: "fresh" })),
          preserved,
        );
      }),
    );
    yield* provider.deleteByThreadId({ threadId: "stopped" });
    yield* provider.deleteByThreadId({ threadId: "missing" });
    result.finalRuntimes = yield* provider.list();
    result.source = yield* context.forThread("source");
    Assert.deepEqual(
      result.source.map((row) => row.itemId),
      ["live", "no-run"],
    );
    result.fork = yield* context.forThread("fork");
    Assert.deepEqual(
      result.fork.map((row) => row.itemId),
      ["rolled-back"],
    );
    yield* context.set({
      threadId: "fork",
      itemId: "rolled-back",
      server: "ignored",
      tool: "ignored",
      text: "updated",
    });
    result.updated = yield* context.forThread("fork");
    Assert.deepEqual(normalized(result.updated), [
      { itemId: "rolled-back", server: "original", tool: "original-tool", text: "updated" },
    ]);
    yield* failed(
      transaction(
        Effect.gen(function* () {
          yield* context.set({
            threadId: "fork",
            itemId: "rolled-back",
            server: "ignored",
            tool: "ignored",
            text: "",
          });
          return yield* Effect.fail("context rollback");
        }),
      ),
    );
    Assert.deepEqual(yield* context.forThread("fork"), result.updated);
    yield* context.set({
      threadId: "fork",
      itemId: "rolled-back",
      server: "ignored",
      tool: "ignored",
      text: " \t\n\uFEFF",
    });
    Assert.deepEqual(yield* context.forThread("fork"), []);
    return normalized(result);
  });
}

try {
  const source = Path.join(fixture.root, "source.sqlite");
  const retained = Path.join(fixture.root, "retained.sqlite");
  const upstream = Path.join(fixture.root, "upstream.sqlite");
  await createMigratedSqlite(source);
  const db = new Sqlite.DatabaseSync(source);
  try {
    const insert = db.prepare(`INSERT INTO provider_session_runtime
      (thread_id,provider_name,provider_instance_id,adapter_key,runtime_mode,status,last_seen_at,resume_cursor_json,runtime_payload_json)
      VALUES (?, 'codex', 'codex', 'codex', 'full-access', ?, ?, NULL, ?)`);
    for (let i = 0; i < 130; i++)
      insert.run(`ordered:${String(i).padStart(3, "0")}`, "running", at, null);
    insert.run("stopped", "stopped", at, null);
    insert.run(
      "imported",
      "running",
      at,
      `{ "other": 1e2, "importedTranscripts": [false, null, 2, ${JSON.stringify(transcript("/a", 1))}, ${JSON.stringify(transcript("/b", 2))}] }`,
    );
    insert.run("plain", "running", at, "{}");
    insert.run("corrupt", "running", at, "{");
    db.prepare(`INSERT INTO orchestration_v2_projection_runs
      (run_id,thread_id,ordinal,provider,provider_thread_id,status,requested_at,completed_at,payload_json)
      VALUES ('rolled-run','source',1,'codex',NULL,'rolled_back',?,NULL,'{}')`).run(at);
    const item = db.prepare(`INSERT INTO orchestration_v2_projection_turn_items
      (turn_item_id,thread_id,run_id,node_id,provider_thread_id,provider_turn_id,parent_item_id,ordinal,type,status,updated_at,payload_json)
      VALUES (?, 'source', ?, NULL,NULL,NULL,NULL,1,'tool-call','completed',?,'{}')`);
    for (const id of ["rolled-back", "live", "no-run"])
      item.run(id, id === "rolled-back" ? "rolled-run" : null, at);
    const context = db.prepare(
      `INSERT INTO mcp_app_model_context VALUES (?, ?, 'original', 'original-tool', 'initial', ?)`,
    );
    for (const id of ["rolled-back", "live", "no-run", "deleted-item"])
      context.run("source", id, at);
    context.run("fork", "rolled-back", at);
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
    "Shared factories preserve immutable upstream SQLite behavior",
  );
  Assert.deepEqual(
    await services("redb", source, exercise),
    expected,
    "Native provider/MCP services preserve immutable upstream behavior",
  );
  checks.push(
    "All provider-runtime and MCP context methods match immutable upstream and retained SQLite",
    "Imported runtime rows cross index pages with stable ordering, stopped filtering and per-row corrupt JSON skipping",
    "Only transcript recording changes imported provenance; stale upserts and conflict-ignore preserve stored provenance",
    "Transcript recording removes non-object history, replaces matching source identity, and repairs invalid payload JSON",
    "MCP visibility joins respect deleted items, rolled-back source runs and fork-owned copies",
    "MCP updates preserve original server/tool identity and shared JavaScript whitespace clearing",
    "Root and nested rollbacks restore runtime rows, context rows and ordered indexes",
  );
  await services("redb", source, ({ provider, context }) =>
    Effect.gen(function* () {
      Assert.deepEqual(normalized(yield* provider.list()), expected.finalRuntimes);
      Assert.deepEqual(yield* context.forThread("fork"), []);
    }),
  );
  checks.push(
    "A reopened helper retains writes and completed preparation without rebuilding imported indexes",
  );
  Assert.equal(hash(await FS.readFile(source)), sourceHash);
  await finishProof({
    executable,
    fixture,
    proofPath,
    checks,
    sourceHash,
    oracles: [runtimeOracle.source, contextOracle.source],
    sourcePaths: [
      "apps/server/src/persistence/ProviderSessionRuntime.ts",
      "apps/server/src/persistence/NativeProviderSessionRuntime.ts",
      "apps/server/src/mcpApps/McpAppModelContext.ts",
      "apps/server/src/mcpApps/NativeMcpAppModelContext.ts",
      "apps/server/src/persistence/redb_runtime_state.rs",
      "tools/check-redb-runtime-state.mjs",
      "tools/redb-domain-fixture.mjs",
    ],
  });
} finally {
  await fixture.close();
}
