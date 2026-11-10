#!/usr/bin/env node
import * as Assert from "node:assert/strict";
import * as FS from "node:fs/promises";
import * as Path from "node:path";
import * as Sqlite from "node:sqlite";
import * as Contracts from "@t3tools/contracts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as SqlSchema from "effect/sql/SqlSchema";

import * as ProjectStore from "../apps/server/src/orchestration-v2/ProjectStore.ts";
import * as ProjectRows from "../apps/server/src/orchestration-v2/ProjectRows.ts";
import * as NativeProjectRows from "../apps/server/src/orchestration-v2/NativeProjectRows.ts";
import { createFixture } from "./database-fixture.mjs";
import { exportSqliteState } from "./export-sqlite-state.mjs";
import {
  createMigratedSqlite, finishProof, hash, loadUpstream, nativeRequest, normalized,
  withNative, withSqlite,
} from "./redb-domain-fixture.mjs";

const [executable, proofPath] = process.argv.slice(2);
Assert.ok(executable && Path.isAbsolute(executable), "Pass an absolute native database executable");
await FS.access(executable, FS.constants.X_OK);
const at = "2026-11-10T12:00:00.000Z";
const later = "2026-11-11T12:00:00.000Z";
const model = { instanceId: "codex", model: "fixture-model" };
const script = { id: "script:test", name: "Test", command: "echo fixture", icon: "test", runOnWorktreeCreate: true };
const oracle = loadUpstream("apps/server/src/orchestration-v2/ProjectStore.ts", {
  ...Contracts, Context, Effect, Layer, Option, Schema, SqlClient, SqlSchema, NodeSqliteClient,
}, ["make"]);
const fixture = await createFixture();
const checks = [];

function event(type, projectId, payload) {
  return {
    sequence: 1, eventId: `event:${type}:${projectId}`, aggregateKind: "project", aggregateId: projectId,
    occurredAt: at, commandId: null, causationEventId: null, correlationId: null, metadata: {},
    type, payload: { projectId, ...payload },
  };
}
const created = (id, payload = {}) => event("project.created", id, {
  title: `Project ${id}`, workspaceRoot: `/fixture/${id}`, defaultModelSelection: model,
  scripts: [], createdAt: at, updatedAt: at, ...payload,
});
const updated = (id, payload = {}) => event("project.meta-updated", id, { updatedAt: later, ...payload });
const deleted = (id) => event("project.deleted", id, { deletedAt: later });
const expectFailure = (effect) => Effect.exit(effect).pipe(Effect.tap((exit) => Effect.sync(() => Assert.ok(Exit.isFailure(exit)))));

async function withServices(kind, filename, use, target = fixture) {
  if (kind === "redb") {
    return withNative(executable, target, (client) => Effect.gen(function* () {
      const rows = yield* NativeProjectRows.make;
      const projects = yield* ProjectStore.makeWithRows.pipe(Effect.provideService(ProjectRows.ProjectRows, rows));
      const raw = (id) => client.get("projection_projects", [{ type: "text", value: id }]).pipe(
        Effect.map((row) => row === null ? null : {
          model: row.default_model_selection_json.value ?? null, icon: row.project_icon_json.value ?? null,
          scripts: row.scripts_json.value, autoPull: Number(row.auto_pull.value),
        }),
      );
      return yield* use({ projects, rows, raw, transaction: (body) => client.withTransaction(body) });
    }));
  }
  return withSqlite(filename, (sql) => Effect.gen(function* () {
    const projects = yield* kind === "upstream" ? oracle.module.make : ProjectStore.make;
    const raw = (id) => sql`SELECT default_model_selection_json AS model, project_icon_json AS icon,
      scripts_json AS scripts, auto_pull AS "autoPull" FROM projection_projects WHERE project_id = ${id}`.pipe(
        Effect.map((rows) => rows[0] ?? null),
      );
    return yield* use({ projects, raw, transaction: (body) => sql.withTransaction(body) });
  }));
}

function exercise({ projects, raw, transaction }) {
  return Effect.gen(function* () {
    const result = {};
    result.initial = yield* projects.list();
    Assert.equal(result.initial.length, 304);
    result.all = yield* projects.list({ includeDeleted: true });
    Assert.equal(result.all.length, 305);
    result.filtered = yield* projects.list({ projectIds: ["project:🦊", "project:z", "project:a", "project:z", "missing"] });
    Assert.deepEqual(result.filtered.map((row) => row.projectId), ["project:a", "project:z", "project:🦊"]);
    Assert.deepEqual(yield* projects.list({ projectIds: [] }), []);
    Assert.ok(Option.isNone(yield* projects.get("missing")));
    Assert.ok(Option.isNone(yield* projects.get("project:deleted")));
    result.deleted = yield* projects.get("project:deleted", { includeDeleted: true });
    Assert.ok(Option.isSome(result.deleted));
    result.activeWorkspace = yield* projects.findActiveByWorkspaceRoot("/same/root");
    Assert.equal(result.activeWorkspace.value.projectId, "project:a");
    Assert.ok(Option.isNone(yield* projects.findActiveByWorkspaceRoot("/same/root/")));
    result.shell = yield* projects.getShell("project:legacy-icon");
    Assert.equal(result.shell.value.repositoryIdentity, null);
    Assert.deepEqual(result.shell.value.projectIcon, { kind: "monogram", text: "AB", color: "blue" });
    result.shells = yield* projects.listShells({ projectIds: ["project:deleted", "project:a", "project:legacy-icon"] });
    Assert.equal(result.shells.length, 2);
    result.rawImported = yield* raw("project:legacy-icon");
    Assert.equal(result.rawImported.model, '{ "instanceId": "codex", "model": "fixture-model" }');
    Assert.ok(result.rawImported.icon.includes('"monogramText"'));
    result.written = yield* transaction(Effect.gen(function* () {
      yield* projects.apply(created("written"));
      const defaults = Option.getOrThrow(yield* projects.get("written"));
      Assert.equal(defaults.autoPull, false);
      Assert.equal(defaults.defaultThreadEnvMode, null);
      Assert.equal(defaults.projectIcon, null);
      Assert.equal(defaults.faviconPath, null);
      Assert.equal(defaults.pinnedAt, null);
      Assert.equal(defaults.archivedAt, null);
      Assert.equal((yield* raw("written")).model, JSON.stringify(model));
      yield* projects.apply(updated("written", {
        workspaceRoot: "/moved/root", defaultThreadEnvMode: "worktree", autoPull: true,
        faviconPath: "/fixture/favicon.svg", projectIcon: { kind: "emoji", emoji: "🦊" },
        scripts: [script], pinnedAt: at, archivedAt: at,
      }));
      yield* projects.apply(updated("written", { title: "Renamed only" }));
      const retained = Option.getOrThrow(yield* projects.get("written"));
      Assert.equal(retained.pinnedAt, at);
      Assert.equal(retained.archivedAt, at);
      Assert.equal(retained.autoPull, true);
      Assert.equal(retained.workspaceRoot, "/moved/root");
      Assert.deepEqual(retained.scripts, [script]);
      Assert.equal((yield* projects.findActiveByWorkspaceRoot("/moved/root")).value.projectId, "written");
      Assert.ok(Option.isNone(yield* projects.findActiveByWorkspaceRoot("/fixture/written")));
      yield* expectFailure(transaction(Effect.gen(function* () {
        yield* projects.apply(created("written", { workspaceRoot: "/rolled-back/root", createdAt: "2020-01-01T00:00:00.000Z" }));
        yield* projects.apply(created("discarded-inner"));
        return yield* Effect.fail("nested project rollback");
      })));
      Assert.deepEqual(Option.getOrThrow(yield* projects.get("written")), retained);
      Assert.ok(Option.isNone(yield* projects.findActiveByWorkspaceRoot("/rolled-back/root")));
      Assert.ok(Option.isNone(yield* projects.get("discarded-inner")));
      yield* projects.apply(updated("written", {
        defaultModelSelection: null, defaultThreadEnvMode: null, faviconPath: null, projectIcon: null,
        scripts: [], pinnedAt: null, archivedAt: null, autoPull: false,
      }));
      const cleared = Option.getOrThrow(yield* projects.get("written"));
      Assert.equal(cleared.defaultModelSelection, null);
      Assert.equal(cleared.pinnedAt, null);
      Assert.equal(cleared.archivedAt, null);
      yield* projects.apply(deleted("written"));
      Assert.ok(Option.isNone(yield* projects.get("written")));
      Assert.ok(Option.isNone(yield* projects.getShell("written")));
      Assert.ok(Option.isNone(yield* projects.findActiveByWorkspaceRoot("/moved/root")));
      yield* projects.apply(updated("written", { title: "Still deleted" }));
      Assert.ok(Option.isNone(yield* projects.get("written")));
      const tombstone = Option.getOrThrow(yield* projects.get("written", { includeDeleted: true }));
      Assert.equal(tombstone.title, "Still deleted");
      yield* projects.apply(created("written", { createdAt: "2021-01-01T00:00:00.000Z" }));
      const recreated = Option.getOrThrow(yield* projects.get("written"));
      Assert.equal(recreated.deletedAt, null);
      Assert.equal(recreated.createdAt, "2021-01-01T00:00:00.000Z");
      yield* projects.apply(updated("missing", { title: "Ignored" }));
      yield* projects.apply(deleted("missing"));
      Assert.ok(Option.isNone(yield* projects.get("missing", { includeDeleted: true })));
      return { defaults, retained, cleared, tombstone, recreated };
    }));
    yield* expectFailure(transaction(Effect.gen(function* () {
      yield* projects.apply(created("discarded-outer"));
      yield* projects.apply(deleted("written"));
      return yield* Effect.die(new Error("outer project rollback"));
    })));
    Assert.ok(Option.isNone(yield* projects.get("discarded-outer", { includeDeleted: true })));
    Assert.ok(Option.isSome(yield* projects.get("written")));
    result.final = yield* projects.listShells();
    Assert.equal(result.final[0].id, "written");
    return normalized(result);
  });
}

try {
  const source = Path.join(fixture.root, "source.sqlite");
  const sqlitePath = Path.join(fixture.root, "sqlite.sqlite");
  const upstreamPath = Path.join(fixture.root, "upstream.sqlite");
  const exported = Path.join(fixture.root, "source.ndjson");
  await createMigratedSqlite(source);
  const database = new Sqlite.DatabaseSync(source);
  try {
    const insert = database.prepare(`INSERT INTO projection_projects
      (project_id,title,workspace_root,default_model_selection_json,default_thread_env_mode,auto_pull,
       favicon_path,project_icon_json,scripts_json,pinned_at,archived_at,created_at,updated_at,deleted_at)
      VALUES (?, ?, ?, ?, 'local', 1, '', ?, '[]', NULL, NULL, ?, ?, ?)`);
    for (const id of ["project:z", "project:a", "project:🦊"]) insert.run(id, id, "/same/root", null, null, at, at, null);
    insert.run("project:deleted", "Deleted", "/same/root", null, null, "2025-01-01T00:00:00.000Z", at, later);
    insert.run("project:legacy-icon", "Legacy", "/legacy/root", '{ "instanceId": "codex", "model": "fixture-model" }',
      '{"kind":"lucide","name":"folder","color":"blue","monogramText":"AB"}', at, at, null);
    for (let index = 0; index < 300; index++) insert.run(`many:${String(index).padStart(4, "0")}`, "Many", "/many/root", null, null, at, at, null);
  } finally { database.close(); }
  const sourceHash = hash(await FS.readFile(source));
  await FS.copyFile(source, sqlitePath); await FS.copyFile(source, upstreamPath);
  exportSqliteState({ source, destination: exported });
  nativeRequest(executable, fixture, "initialize", { operation: "import_sqlite_application", path: exported });
  const expected = await withServices("upstream", upstreamPath, exercise);
  const retained = await withServices("sqlite", sqlitePath, exercise);
  const actual = await withServices("redb", source, exercise);
  Assert.deepEqual(retained, expected, "The shared factory with extracted SQLite rows must match immutable upstream");
  Assert.deepEqual(actual, expected, "The complete native project service must match immutable upstream SQLite");
  checks.push(
    "All six ProjectStore members agree across immutable upstream SQLite, extracted SQLite and native redb",
    "Full migrated fixtures retain imported project rows, raw JSON lexemes, auto-pull bits and legacy icon decoding",
    "Ordered project lists cross native index pages, deduplicate ID filters and preserve empty filters and exact workspace matching",
    "Shared event application preserves creation defaults, omitted updates, explicit nulls, pin/archive state, deletion and recreation",
    "Changing workspace, creation order or deleted status updates all access paths in the same transaction",
    "Nested failures and root defects restore both project rows and every ordered index",
  );
  await withServices("redb", source, ({ projects }) => Effect.gen(function* () {
    Assert.deepEqual(normalized(yield* projects.listShells()), expected.final);
    Assert.equal((yield* projects.findActiveByWorkspaceRoot("/fixture/written")).value.projectId, "written");
    Assert.ok(Option.isNone(yield* projects.findActiveByWorkspaceRoot("/moved/root")));
  }));
  checks.push("Reopened native helper preserves all project state and does not rebuild live indexes from imported rows");
  const badFixture = await createFixture();
  try {
    const badSource = Path.join(badFixture.root, "malformed.sqlite");
    await FS.copyFile(source, badSource);
    const bad = new Sqlite.DatabaseSync(badSource);
    try { bad.prepare("UPDATE projection_projects SET scripts_json = ? WHERE project_id = ?").run("{", "project:a"); }
    finally { bad.close(); }
    const badExport = Path.join(badFixture.root, "malformed.ndjson");
    exportSqliteState({ source: badSource, destination: badExport });
    nativeRequest(executable, badFixture, "initialize", { operation: "import_sqlite_application", path: badExport });
    const inspect = ({ projects, raw }) => Effect.gen(function* () {
      const error = yield* projects.get("project:a").pipe(Effect.flip);
      Assert.equal(error._tag, "ProjectStoreV2Error");
      Assert.ok(Option.isSome(yield* projects.get("project:z")));
      return yield* raw("project:a");
    });
    Assert.deepEqual(normalized(await withServices("redb", badSource, inspect, badFixture)),
      normalized(await withServices("upstream", badSource, inspect)));
  } finally { await badFixture.close(); }
  checks.push("Malformed stored project JSON fails through the existing service error while preserving the original bytes and unrelated point reads");
  Assert.equal(hash(await FS.readFile(source)), sourceHash);
  await finishProof({ executable, fixture, proofPath, checks, sourceHash, oracles: [oracle.source],
    sourcePaths: [
      "apps/server/src/orchestration-v2/ProjectStore.ts", "apps/server/src/orchestration-v2/ProjectRows.ts",
      "apps/server/src/orchestration-v2/SqliteProjectRows.ts", "apps/server/src/orchestration-v2/NativeProjectRows.ts",
      "apps/server/src/orchestration-v2/redb_project_rows.rs", "tools/check-redb-projects.mjs", "tools/redb-domain-fixture.mjs",
    ],
  });
} finally { await fixture.close(); }
