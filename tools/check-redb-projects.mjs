#!/usr/bin/env node
import * as NodeAssert from "node:assert/strict";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeSqlite from "node:sqlite";
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
import * as NativeProjectStore from "../apps/server/src/orchestration-v2/NativeProjectStore.ts";
import * as NativeDatabase from "../apps/server/src/orchestration-v2/NativeDatabase.ts";
import { RedbClient } from "../apps/server/src/persistence/RedbClient.ts";
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
NodeAssert.ok(
  executable && NodePath.isAbsolute(executable),
  "Pass an absolute native database executable",
);
await NodeFSP.access(executable, NodeFSP.constants.X_OK);
const at = "2026-11-10T12:00:00.000Z";
const later = "2026-11-11T12:00:00.000Z";
const model = { instanceId: "codex", model: "fixture-model" };
const script = {
  id: "script:test",
  name: "Test",
  command: "echo fixture",
  icon: "test",
  runOnWorktreeCreate: true,
};
const oracle = loadUpstream(
  "apps/server/src/orchestration-v2/ProjectStore.ts",
  {
    ...Contracts,
    Context,
    Effect,
    Layer,
    Option,
    Schema,
    SqlClient,
    SqlSchema,
    NodeSqliteClient,
  },
  ["make"],
);
const fixture = await createFixture();
const checks = [];

function event(type, projectId, payload) {
  return {
    sequence: 1,
    eventId: `event:${type}:${projectId}`,
    aggregateKind: "project",
    aggregateId: projectId,
    occurredAt: at,
    commandId: null,
    causationEventId: null,
    correlationId: null,
    metadata: {},
    type,
    payload: { projectId, ...payload },
  };
}
const created = (id, payload = {}) =>
  event("project.created", id, {
    title: `Project ${id}`,
    workspaceRoot: `/fixture/${id}`,
    defaultModelSelection: model,
    scripts: [],
    createdAt: at,
    updatedAt: at,
    ...payload,
  });
const updated = (id, payload = {}) =>
  event("project.meta-updated", id, { updatedAt: later, ...payload });
const deleted = (id) => event("project.deleted", id, { deletedAt: later });
const expectFailure = (effect) =>
  Effect.exit(effect).pipe(
    Effect.tap((exit) => Effect.sync(() => NodeAssert.ok(Exit.isFailure(exit)))),
  );

async function withServices(kind, filename, use, target = fixture) {
  if (kind === "redb" || kind === "redb-retained") {
    return withNative(executable, target, (client) => {
      const calls = [];
      const tracked = RedbClient.of({
        ...client,
        request: (operation, input, response) => {
          calls.push(operation);
          return client.request(operation, input, response);
        },
      });
      return Effect.gen(function* () {
        const rows = yield* NativeProjectRows.make;
        const make = kind === "redb" ? NativeProjectStore.makeWithRows : ProjectStore.makeWithRows;
        const projects = yield* make.pipe(Effect.provideService(ProjectRows.ProjectRows, rows));
        const raw = (id) =>
          client.get("projection_projects", [{ type: "text", value: id }]).pipe(
            Effect.map((row) =>
              row === null
                ? null
                : {
                    model: row.default_model_selection_json.value ?? null,
                    icon: row.project_icon_json.value ?? null,
                    scripts: row.scripts_json.value,
                    autoPull: Number(row.auto_pull.value),
                  },
            ),
          );
        return yield* use({
          projects,
          rows,
          raw,
          calls: kind === "redb" ? calls : undefined,
          client,
          transaction: (body) => client.withTransaction(body),
        });
      }).pipe(Effect.provideService(RedbClient, tracked));
    });
  }
  return withSqlite(filename, (sql) =>
    Effect.gen(function* () {
      const projects = yield* kind === "upstream" ? oracle.module.make : ProjectStore.make;
      const raw = (id) =>
        sql`SELECT default_model_selection_json AS model, project_icon_json AS icon,
      scripts_json AS scripts, auto_pull AS "autoPull" FROM projection_projects WHERE project_id = ${id}`.pipe(
          Effect.map((rows) => rows[0] ?? null),
        );
      return yield* use({ projects, raw, transaction: (body) => sql.withTransaction(body) });
    }),
  );
}

const exercise = Effect.fn("checkRedbProjects.exercise")(function* ({
  projects,
  raw,
  transaction,
}) {
  const result = {};
  result.initial = yield* projects.list();
  NodeAssert.equal(result.initial.length, 304);
  result.all = yield* projects.list({ includeDeleted: true });
  NodeAssert.equal(result.all.length, 305);
  result.filtered = yield* projects.list({
    projectIds: ["project:🦊", "project:z", "project:a", "project:z", "missing"],
  });
  NodeAssert.deepEqual(
    result.filtered.map((row) => row.projectId),
    ["project:a", "project:z", "project:🦊"],
  );
  NodeAssert.deepEqual(yield* projects.list({ projectIds: [] }), []);
  NodeAssert.ok(Option.isNone(yield* projects.get("missing")));
  NodeAssert.ok(Option.isNone(yield* projects.get("project:deleted")));
  result.deleted = yield* projects.get("project:deleted", { includeDeleted: true });
  NodeAssert.ok(Option.isSome(result.deleted));
  result.activeWorkspace = yield* projects.findActiveByWorkspaceRoot("/same/root");
  NodeAssert.equal(result.activeWorkspace.value.projectId, "project:a");
  NodeAssert.ok(Option.isNone(yield* projects.findActiveByWorkspaceRoot("/same/root/")));
  result.shell = yield* projects.getShell("project:legacy-icon");
  NodeAssert.equal(result.shell.value.repositoryIdentity, null);
  NodeAssert.deepEqual(result.shell.value.projectIcon, {
    kind: "monogram",
    text: "AB",
    color: "blue",
  });
  result.shells = yield* projects.listShells({
    projectIds: ["project:deleted", "project:a", "project:legacy-icon"],
  });
  NodeAssert.equal(result.shells.length, 2);
  result.rawImported = yield* raw("project:legacy-icon");
  NodeAssert.equal(result.rawImported.model, '{ "instanceId": "codex", "model": "fixture-model" }');
  NodeAssert.ok(result.rawImported.icon.includes('"monogramText"'));
  result.written = yield* transaction(
    Effect.gen(function* () {
      yield* projects.apply(created("written"));
      const defaults = Option.getOrThrow(yield* projects.get("written"));
      NodeAssert.equal(defaults.hasCustomTitle, false);
      NodeAssert.equal(defaults.autoPull, false);
      NodeAssert.equal(defaults.defaultThreadEnvMode, null);
      NodeAssert.equal(defaults.projectIcon, null);
      NodeAssert.equal(defaults.faviconPath, null);
      NodeAssert.equal(defaults.pinnedAt, null);
      NodeAssert.equal(defaults.archivedAt, null);
      NodeAssert.equal((yield* raw("written")).model, JSON.stringify(model));
      yield* projects.apply(
        updated("written", {
          workspaceRoot: "/moved/root",
          defaultThreadEnvMode: "worktree",
          autoPull: true,
          faviconPath: "/fixture/favicon.svg",
          projectIcon: { kind: "emoji", emoji: "🦊" },
          scripts: [script],
          pinnedAt: at,
          archivedAt: at,
        }),
      );
      yield* projects.apply(updated("written", { title: "Renamed only" }));
      const retained = Option.getOrThrow(yield* projects.get("written"));
      NodeAssert.equal(retained.hasCustomTitle, true);
      NodeAssert.equal(retained.pinnedAt, at);
      NodeAssert.equal(retained.archivedAt, at);
      NodeAssert.equal(retained.autoPull, true);
      NodeAssert.equal(retained.workspaceRoot, "/moved/root");
      NodeAssert.deepEqual(retained.scripts, [script]);
      NodeAssert.equal(
        (yield* projects.findActiveByWorkspaceRoot("/moved/root")).value.projectId,
        "written",
      );
      NodeAssert.ok(Option.isNone(yield* projects.findActiveByWorkspaceRoot("/fixture/written")));
      yield* expectFailure(
        transaction(
          Effect.gen(function* () {
            yield* projects.apply(
              created("written", {
                workspaceRoot: "/rolled-back/root",
                createdAt: "2020-01-01T00:00:00.000Z",
              }),
            );
            yield* projects.apply(created("discarded-inner"));
            return yield* Effect.fail("nested project rollback");
          }),
        ),
      );
      NodeAssert.deepEqual(Option.getOrThrow(yield* projects.get("written")), retained);
      NodeAssert.ok(Option.isNone(yield* projects.findActiveByWorkspaceRoot("/rolled-back/root")));
      NodeAssert.ok(Option.isNone(yield* projects.get("discarded-inner")));
      yield* projects.apply(
        updated("written", {
          defaultModelSelection: null,
          defaultThreadEnvMode: null,
          faviconPath: null,
          projectIcon: null,
          scripts: [],
          pinnedAt: null,
          archivedAt: null,
          autoPull: false,
        }),
      );
      const cleared = Option.getOrThrow(yield* projects.get("written"));
      NodeAssert.equal(cleared.defaultModelSelection, null);
      NodeAssert.equal(cleared.pinnedAt, null);
      NodeAssert.equal(cleared.archivedAt, null);
      yield* projects.apply(deleted("written"));
      NodeAssert.ok(Option.isNone(yield* projects.get("written")));
      NodeAssert.ok(Option.isNone(yield* projects.getShell("written")));
      NodeAssert.ok(Option.isNone(yield* projects.findActiveByWorkspaceRoot("/moved/root")));
      yield* projects.apply(updated("written", { title: "Still deleted" }));
      NodeAssert.ok(Option.isNone(yield* projects.get("written")));
      const tombstone = Option.getOrThrow(yield* projects.get("written", { includeDeleted: true }));
      NodeAssert.equal(tombstone.title, "Still deleted");
      yield* projects.apply(created("written", { createdAt: "2021-01-01T00:00:00.000Z" }));
      const recreated = Option.getOrThrow(yield* projects.get("written"));
      NodeAssert.equal(recreated.deletedAt, null);
      NodeAssert.equal(recreated.createdAt, "2021-01-01T00:00:00.000Z");
      NodeAssert.equal(
        recreated.title,
        "Still deleted",
        "automatic recreation retains a user-chosen title",
      );
      NodeAssert.equal(recreated.hasCustomTitle, true);
      yield* projects.apply(updated("missing", { title: "Ignored" }));
      yield* projects.apply(deleted("missing"));
      NodeAssert.ok(Option.isNone(yield* projects.get("missing", { includeDeleted: true })));
      return { defaults, retained, cleared, tombstone, recreated };
    }),
  );
  yield* expectFailure(
    transaction(
      Effect.gen(function* () {
        yield* projects.apply(created("discarded-outer"));
        yield* projects.apply(deleted("written"));
        return yield* Effect.die(new Error("outer project rollback"));
      }),
    ),
  );
  NodeAssert.ok(Option.isNone(yield* projects.get("discarded-outer", { includeDeleted: true })));
  NodeAssert.ok(Option.isSome(yield* projects.get("written")));
  result.final = yield* projects.listShells();
  NodeAssert.equal(result.final[0].id, "written");
  return normalized(result);
});

const errorLabel = Effect.fn("checkRedbProjects.errorLabel")(function* (effect) {
  const error = yield* effect.pipe(Effect.flip);
  NodeAssert.equal(error._tag, "ProjectStoreV2Error");
  return { tag: error._tag, operation: error.operation };
});

const directEvents = Effect.fn("checkRedbProjects.directEvents")(function* ({
  projects,
  raw,
  calls,
  transaction,
}) {
  const result = { repairs: [], failures: [] };
  yield* transaction(
    Effect.gen(function* () {
      for (const id of ["bad:model", "bad:scripts", "bad:icon", "bad:mode"]) {
        const failure = yield* errorLabel(
          projects.apply(updated(id, { title: "Must read first" })),
        );
        NodeAssert.equal(failure.operation, "get");
        const offset = calls?.length;
        yield* projects.apply(created(id));
        if (calls)
          NodeAssert.deepEqual(
            calls.slice(offset),
            ["application_projects_reduce_event", "application_projects_upsert"],
            "Creation must not decode an existing project row",
          );
        const repaired = Option.getOrThrow(yield* projects.get(id));
        NodeAssert.equal(
          repaired.title,
          id === "bad:mode" ? `Project ${id}` : "Preserved custom title",
        );
        NodeAssert.equal(repaired.hasCustomTitle, id !== "bad:mode");
        result.repairs.push({ failure, repaired, raw: yield* raw(id) });
      }
      let ignored = { ordinary: "extension" };
      for (let depth = 0; depth < 70; depth++) ignored = { ignored };
      const plain = {
        ...created("direct:plain", {
          title: "",
          workspaceRoot: "",
          createdAt: "not-an-ISO-date",
          repositoryIdentity: ignored,
          extra: ignored,
        }),
        sequence: -1,
        occurredAt: { unused: true },
        metadata: undefined,
      };
      yield* projects.apply(plain);
      const untrimmedScript = {
        ...script,
        id: "  script:direct  ",
        name: "  Direct  ",
        command: "  echo direct  ",
        previewUrl: "  http://localhost:9000  ",
        runOnSettle: undefined,
        extra: ignored,
      };
      yield* projects.apply(
        created("direct:base", {
          defaultModelSelection: {
            instanceId: "codex",
            model: " model ",
            options: [{ id: " reasoning ", value: " high ", extra: true }],
            extra: true,
          },
          projectIcon: { kind: "lucide", name: "folder", color: "blue", monogramText: "AB" },
          scripts: [untrimmedScript],
          faviconPath: "",
        }),
      );
      result.normalized = Option.getOrThrow(yield* projects.get("direct:base"));
      NodeAssert.deepEqual(
        result.normalized.projectIcon,
        { kind: "lucide", name: "folder", color: "blue" },
        "A direct typed icon does not use stored legacy decoding before encode",
      );
      NodeAssert.equal(result.normalized.scripts[0].command, "echo direct");
      yield* projects.apply(
        updated("direct:base", {
          title: undefined,
          workspaceRoot: undefined,
          defaultModelSelection: undefined,
          defaultThreadEnvMode: undefined,
          autoPull: undefined,
          faviconPath: undefined,
          projectIcon: undefined,
          scripts: undefined,
          pinnedAt: undefined,
          archivedAt: undefined,
        }),
      );
      NodeAssert.deepEqual(Option.getOrThrow(yield* projects.get("direct:base")), {
        ...result.normalized,
        updatedAt: later,
      });

      for (const [index, payload] of [
        { title: false },
        { autoPull: 2 },
        { defaultThreadEnvMode: "unknown-mode" },
        { projectIcon: { kind: "lucide", name: " folder ", color: "blue" } },
        { defaultModelSelection: { instanceId: "codex", model: "test", options: null } },
        { defaultModelSelection: { instanceId: " codex ", model: "test" } },
        { scripts: [{ ...script, command: " " }] },
      ].entries()) {
        const before = Option.getOrThrow(yield* projects.get("direct:base"));
        const failed = yield* errorLabel(projects.apply(updated("direct:base", payload)));
        NodeAssert.equal(failed.operation, "apply");
        NodeAssert.deepEqual(Option.getOrThrow(yield* projects.get("direct:base")), before);
        yield* projects.apply(created(`caught:root:${index}`));
        const nested = yield* transaction(
          Effect.gen(function* () {
            const failure = yield* errorLabel(projects.apply(updated("direct:base", payload)));
            yield* projects.apply(created(`caught:nested:${index}`));
            return failure;
          }),
        );
        result.failures.push({ failed, nested });
      }
      result.invalidCreation = yield* errorLabel(
        projects.apply(created("invalid:created", { title: false })),
      );
      NodeAssert.equal(result.invalidCreation.operation, "apply");
      yield* projects.apply(created("caught:after-invalid-creation"));
      const offset = calls?.length;
      yield* projects.apply(updated("missing:direct", { title: false, updatedAt: null }));
      yield* projects.apply(event("project.deleted", "missing:direct", {}));
      if (calls)
        NodeAssert.deepEqual(
          calls.slice(offset),
          ["application_projects_select", "application_projects_select"],
          "Missing updates and deletes must not validate or reduce unused payloads",
        );
      yield* expectFailure(
        transaction(
          Effect.gen(function* () {
            yield* projects.apply(created("discarded:direct"));
            yield* projects.apply(updated("direct:base", { autoPull: 2 }));
          }),
        ),
      );
      NodeAssert.ok(Option.isNone(yield* projects.get("discarded:direct")));
      yield* projects.apply(created("caught:after-savepoint"));
    }),
  );
  result.final = yield* projects.list({ includeDeleted: true });
  NodeAssert.ok(result.final.some((row) => row.projectId === "caught:after-savepoint"));
  NodeAssert.equal(
    result.final.find((row) => row.projectId === "direct:plain").createdAt,
    "not-an-ISO-date",
  );
  return normalized(result);
});

async function checkDirectEvents() {
  const target = await createFixture();
  try {
    const source = NodePath.join(target.root, "source.sqlite");
    const stock = NodePath.join(target.root, "stock.sqlite");
    const retained = NodePath.join(target.root, "retained.sqlite");
    await createMigratedSqlite(source);
    const db = new NodeSqlite.DatabaseSync(source);
    try {
      const insert = db.prepare(`INSERT INTO projection_projects
        (project_id,title,has_custom_title,workspace_root,default_model_selection_json,default_thread_env_mode,auto_pull,project_icon_json,scripts_json,created_at,updated_at)
        VALUES (?, 'Preserved custom title', ?, '/old/root', ?, ?, 0, ?, ?, ?, ?)`);
      insert.run("bad:model", 1, "{", null, null, "[]", at, at);
      insert.run("bad:scripts", 1, null, null, null, "{", at, at);
      insert.run("bad:icon", 1, null, null, "{", "[]", at, at);
      insert.run("bad:mode", 0, null, "unknown-mode", null, "[]", at, at);
    } finally {
      db.close();
    }
    await NodeFSP.copyFile(source, stock);
    await NodeFSP.copyFile(source, retained);
    const exported = NodePath.join(target.root, "source.ndjson");
    exportSqliteState({ source, destination: exported });
    nativeRequest(executable, target, "initialize", {
      operation: "import_sqlite_application",
      path: exported,
    });
    const expected = await withServices("upstream", stock, directEvents, target);
    NodeAssert.deepEqual(await withServices("sqlite", retained, directEvents, target), expected);
    NodeAssert.deepEqual(await withServices("redb", source, directEvents, target), expected);
    await withServices(
      "redb",
      source,
      ({ projects }) =>
        projects
          .list({ includeDeleted: true })
          .pipe(
            Effect.tap((rows) =>
              Effect.sync(() => NodeAssert.deepEqual(normalized(rows), expected.final)),
            ),
          ),
      target,
    );
  } finally {
    await target.close();
  }
}

function rowWitness(row) {
  const canonical = JSON.stringify(row, (_key, value) =>
    value !== null && typeof value === "object" && !Array.isArray(value)
      ? Object.fromEntries(
          Object.keys(value)
            .sort()
            .map((key) => [key, value[key]]),
        )
      : value,
  );
  return {
    projectId: row.projectId,
    title_code_units: row.title.length,
    scripts: row.scripts.length,
    row_sha256: hash(canonical),
  };
}

const compatibilityEvents = Effect.fn("checkRedbProjects.compatibilityEvents")(function* ({
  projects,
  raw,
  calls,
  transaction,
}) {
  const result = [];
  for (const [id, original, replacement] of [
    ["compat:large", "a".repeat(9 * 1024 * 1024), "b".repeat(9 * 1024 * 1024)],
    ["compat:escaped", '"'.repeat(3 * 1024 * 1024), "\\".repeat(3 * 1024 * 1024)],
  ]) {
    yield* transaction(projects.apply(created(id, { title: original })));
    const old = Option.getOrThrow(yield* projects.get(id));
    const next = updated(id, { title: replacement });
    if (id === "compat:escaped") {
      const existingJson = JSON.stringify(old);
      const eventJson = JSON.stringify({ type: next.type, payload: next.payload });
      NodeAssert.ok(existingJson.length + eventJson.length < 16 * 1024 * 1024);
      NodeAssert.ok(
        JSON.stringify({ existing_json: existingJson, event_json: eventJson }).length >
          16 * 1024 * 1024,
        "Escaping the nested JSON strings exceeds the frame although the inner inputs fit",
      );
    }
    const offset = calls?.length;
    yield* transaction(projects.apply(next));
    if (calls)
      NodeAssert.deepEqual(calls.slice(offset), [
        "application_projects_select",
        "application_projects_upsert",
      ]);
    const changed = Option.getOrThrow(yield* projects.get(id));
    NodeAssert.equal(changed.title.length, replacement.length);
    NodeAssert.equal(hash(JSON.stringify(changed.title)), hash(JSON.stringify(replacement)));
    NodeAssert.equal(changed.hasCustomTitle, true);
    result.push(rowWitness(changed));
  }

  const scripts = Array.from({ length: 45_000 }, (_, index) => ({
    id: `s${index}`,
    name: "Script",
    command: "true",
    icon: "test",
    runOnWorktreeCreate: false,
  }));
  // Each object and its five scalar fields already exceed the pure parser's
  // value budget; the old row stores this valid array in one encoded text cell.
  NodeAssert.ok(scripts.length * 6 > 262_144);
  NodeAssert.ok(JSON.stringify(scripts).length < 8 * 1024 * 1024);
  const manyOffset = calls?.length;
  yield* transaction(projects.apply(created("compat:values", { scripts })));
  if (calls) NodeAssert.deepEqual(calls.slice(manyOffset), ["application_projects_upsert"]);
  const many = Option.getOrThrow(yield* projects.get("compat:values"));
  NodeAssert.equal(many.scripts.length, scripts.length);
  NodeAssert.equal(hash(JSON.stringify(many.scripts)), hash(JSON.stringify(scripts)));
  NodeAssert.equal(hash((yield* raw("compat:values")).scripts), hash(JSON.stringify(scripts)));
  result.push(rowWitness(many));

  for (const [id, value, native] of [
    ["compat:high", "value\ud800", false],
    ["compat:low", "value\udc00", false],
    ["compat:paired", "value🦊", true],
    ["compat:literal", "value\\ud800", true],
  ]) {
    const payload = {
      defaultModelSelection: {
        instanceId: "codex",
        model: value,
        options: [{ id: "mode", value }],
      },
      scripts: [{ ...script, command: value }],
    };
    const offset = calls?.length;
    yield* transaction(projects.apply(created(id, payload)));
    if (calls)
      NodeAssert.deepEqual(
        calls.slice(offset),
        native
          ? ["application_projects_reduce_event", "application_projects_upsert"]
          : ["application_projects_upsert"],
      );
    const updateOffset = calls?.length;
    yield* transaction(projects.apply(updated(id, { title: "Retained JSON text" })));
    if (calls)
      NodeAssert.deepEqual(
        calls.slice(updateOffset),
        native
          ? [
              "application_projects_select",
              "application_projects_reduce_event",
              "application_projects_upsert",
            ]
          : ["application_projects_select", "application_projects_upsert"],
      );
    const stored = Option.getOrThrow(yield* projects.get(id));
    NodeAssert.equal(stored.defaultModelSelection.model, value);
    NodeAssert.equal(stored.defaultModelSelection.options[0].value, value);
    NodeAssert.equal(stored.scripts[0].command, value);
    result.push(rowWitness(stored));
  }

  yield* transaction(
    Effect.gen(function* () {
      for (const nested of [false, true]) {
        const body = Effect.gen(function* () {
          const offset = calls?.length;
          const failure = yield* errorLabel(
            projects.apply(
              created(`compat:invalid:${nested}`, {
                defaultModelSelection: { ...model, model: "value\ud800" },
                scripts: [{ ...script, command: " " }],
              }),
            ),
          );
          NodeAssert.equal(failure.operation, "apply");
          if (calls) NodeAssert.deepEqual(calls.slice(offset), []);
          yield* projects.apply(created(`compat:caught:${nested}`));
        });
        yield* nested ? transaction(body) : body;
      }
    }),
  );
  for (const nested of [false, true]) {
    NodeAssert.ok(Option.isNone(yield* projects.get(`compat:invalid:${nested}`)));
    result.push(rowWitness(Option.getOrThrow(yield* projects.get(`compat:caught:${nested}`))));
  }
  return result;
});

async function checkCompatibilityInputs() {
  let expected;
  // Keep the old native rows/shared-reducer composition as an explicit control,
  // as well as complete immutable SQLite apply/upsert. Fixtures are independent.
  for (const kind of ["upstream", "redb-retained", "redb"]) {
    const target = await createFixture();
    try {
      const source = NodePath.join(target.root, "compatibility.sqlite");
      await createMigratedSqlite(source);
      if (kind !== "upstream") {
        const exported = NodePath.join(target.root, "compatibility.ndjson");
        exportSqliteState({ source, destination: exported });
        nativeRequest(executable, target, "initialize", {
          operation: "import_sqlite_application",
          path: exported,
        });
      }
      const actual = await withServices(kind, source, compatibilityEvents, target);
      if (expected === undefined) expected = actual;
      else NodeAssert.deepEqual(actual, expected);
      await withServices(
        kind,
        source,
        ({ projects }) =>
          Effect.gen(function* () {
            for (const witness of actual) {
              const row = Option.getOrThrow(yield* projects.get(witness.projectId));
              NodeAssert.deepEqual(rowWitness(row), witness);
            }
          }),
        target,
      );
    } finally {
      await target.close();
    }
  }
  return expected;
}

function forgedReducer(database, forged, observed) {
  return NativeDatabase.NativeDatabase.of({
    request: database.request,
    withSession: (use) =>
      database.withSession((session) =>
        use({
          discard: session.discard,
          request: Effect.fn("checkRedbProjects.forgedResponse")(function* (input, response) {
            observed.push(input.operation);
            if (input.operation !== "application_projects_reduce_event")
              return yield* session.request(input, response);
            yield* session.request(input, Schema.Unknown);
            return yield* Schema.decodeUnknownEffect(response)(forged).pipe(
              Effect.mapError(
                (cause) =>
                  new NativeDatabase.NativeDatabaseOutcomeUnknown({ stage: "response", cause }),
              ),
            );
          }),
        }),
      ),
  });
}

async function checkResponseContract(target) {
  for (const forged of [
    { status: "reduced", row: { projectId: "wrong-project" } },
    { status: "reduced", row: null },
    { status: "unexpected" },
  ]) {
    const observed = [];
    await withNative(
      executable,
      target,
      (client) =>
        Effect.gen(function* () {
          const projects = yield* NativeProjectStore.make;
          yield* expectFailure(
            client.withTransaction(
              Effect.gen(function* () {
                const failure = yield* projects.apply(created("forged:response")).pipe(Effect.flip);
                NodeAssert.equal(failure.operation, "apply");
                yield* projects
                  .apply(created("forged:after-catch"))
                  .pipe(Effect.catch(() => Effect.void));
              }),
            ),
          );
          NodeAssert.equal(
            observed.filter((operation) => operation === "application_projects_reduce_event")
              .length,
            1,
          );
          NodeAssert.equal(
            observed.filter((operation) => operation === "application_projects_upsert").length,
            0,
          );
        }),
      (database) => forgedReducer(database, forged, observed),
    );
    await withServices(
      "redb",
      "unused",
      ({ projects }) =>
        Effect.gen(function* () {
          NodeAssert.ok(Option.isNone(yield* projects.get("forged:response")));
          NodeAssert.ok(Option.isNone(yield* projects.get("forged:after-catch")));
        }),
      target,
    );
  }
}

try {
  const source = NodePath.join(fixture.root, "source.sqlite");
  const sqlitePath = NodePath.join(fixture.root, "sqlite.sqlite");
  const upstreamPath = NodePath.join(fixture.root, "upstream.sqlite");
  const exported = NodePath.join(fixture.root, "source.ndjson");
  await createMigratedSqlite(source);
  const database = new NodeSqlite.DatabaseSync(source);
  try {
    const insert = database.prepare(`INSERT INTO projection_projects
      (project_id,title,workspace_root,default_model_selection_json,default_thread_env_mode,auto_pull,
       favicon_path,project_icon_json,scripts_json,pinned_at,archived_at,created_at,updated_at,deleted_at)
      VALUES (?, ?, ?, ?, 'local', 1, '', ?, '[]', NULL, NULL, ?, ?, ?)`);
    for (const id of ["project:z", "project:a", "project:🦊"])
      insert.run(id, id, "/same/root", null, null, at, at, null);
    insert.run(
      "project:deleted",
      "Deleted",
      "/same/root",
      null,
      null,
      "2025-01-01T00:00:00.000Z",
      at,
      later,
    );
    insert.run(
      "project:legacy-icon",
      "Legacy",
      "/legacy/root",
      '{ "instanceId": "codex", "model": "fixture-model" }',
      '{"kind":"lucide","name":"folder","color":"blue","monogramText":"AB"}',
      at,
      at,
      null,
    );
    for (let index = 0; index < 300; index++)
      insert.run(
        `many:${String(index).padStart(4, "0")}`,
        "Many",
        "/many/root",
        null,
        null,
        at,
        at,
        null,
      );
  } finally {
    database.close();
  }
  const sourceHash = hash(await NodeFSP.readFile(source));
  await NodeFSP.copyFile(source, sqlitePath);
  await NodeFSP.copyFile(source, upstreamPath);
  exportSqliteState({ source, destination: exported });
  nativeRequest(executable, fixture, "initialize", {
    operation: "import_sqlite_application",
    path: exported,
  });
  const expected = await withServices("upstream", upstreamPath, exercise);
  const retained = await withServices("sqlite", sqlitePath, exercise);
  const actual = await withServices("redb", source, exercise);
  NodeAssert.deepEqual(
    retained,
    expected,
    "The shared factory with extracted SQLite rows must match immutable upstream",
  );
  NodeAssert.deepEqual(
    actual,
    expected,
    "The complete native project service must match immutable upstream SQLite",
  );
  checks.push(
    "All six ProjectStore members agree across immutable upstream SQLite, extracted SQLite and native redb",
    "Full migrated fixtures retain imported project rows, raw JSON lexemes, auto-pull bits and legacy icon decoding",
    "Ordered project lists cross native index pages, deduplicate ID filters and preserve empty filters and exact workspace matching",
    "Shared event application preserves creation defaults, omitted updates, explicit nulls, pin/archive state, deletion and recreation",
    "Explicit titles remain custom through automatic recreation instead of reverting to a directory-derived title",
    "Changing workspace, creation order or deleted status updates all access paths in the same transaction",
    "Nested failures and root defects restore both project rows and every ordered index",
  );
  await withServices("redb", source, ({ projects }) =>
    Effect.gen(function* () {
      NodeAssert.deepEqual(normalized(yield* projects.listShells()), expected.final);
      NodeAssert.equal(
        (yield* projects.findActiveByWorkspaceRoot("/fixture/written")).value.projectId,
        "written",
      );
      NodeAssert.ok(Option.isNone(yield* projects.findActiveByWorkspaceRoot("/moved/root")));
    }),
  );
  checks.push(
    "Reopened native helper preserves all project state and does not rebuild live indexes from imported rows",
  );
  const badFixture = await createFixture();
  try {
    const badSource = NodePath.join(badFixture.root, "malformed.sqlite");
    await NodeFSP.copyFile(source, badSource);
    const bad = new NodeSqlite.DatabaseSync(badSource);
    try {
      bad
        .prepare("UPDATE projection_projects SET scripts_json = ? WHERE project_id = ?")
        .run("{", "project:a");
    } finally {
      bad.close();
    }
    const badExport = NodePath.join(badFixture.root, "malformed.ndjson");
    exportSqliteState({ source: badSource, destination: badExport });
    nativeRequest(executable, badFixture, "initialize", {
      operation: "import_sqlite_application",
      path: badExport,
    });
    const inspect = Effect.fn("checkRedbProjects.inspectMalformed")(function* ({ projects, raw }) {
      const error = yield* projects.get("project:a").pipe(Effect.flip);
      NodeAssert.equal(error._tag, "ProjectStoreV2Error");
      NodeAssert.ok(Option.isSome(yield* projects.get("project:z")));
      return yield* raw("project:a");
    });
    NodeAssert.deepEqual(
      normalized(await withServices("redb", badSource, inspect, badFixture)),
      normalized(await withServices("upstream", badSource, inspect)),
    );
  } finally {
    await badFixture.close();
  }
  checks.push(
    "Malformed stored project JSON fails through the existing service error while preserving the original bytes and unrelated point reads",
  );
  await checkDirectEvents();
  const compatibility = await checkCompatibilityInputs();
  await checkResponseContract(fixture);
  checks.push(
    "Malformed reducer envelopes, absent rows and wrong-project rows fail before upsert and retain terminal protocol handling even when caught inside a writer",
    "The real native project reducer matches immutable complete apply/upsert behavior for direct events, encoded normalization, legacy icon input and ignored envelope fields",
    "Creation replaces malformed old model/scripts/icon JSON and unsupported environment text without decoding it, while sticky custom titles remain owned by upsert",
    "Missing direct updates/deletes remain no-ops, and existing-row failures retain the nested get operation label",
    "Caught reducer and row-codec failures preserve outer writers and nested savepoints, allowing unrelated writes to commit; unhandled failures still roll back the nested scope",
    "Direct-event state and every ordered project access path survive reopening the native helper",
    "Before any reducer request, unsupported combined/frame sizes, large valid script arrays and lone UTF-16 surrogates use the one retained pure reducer with unchanged native rows and commit ownership",
    "Compatibility inputs agree with immutable SQLite and the former native rows/shared-reducer composition; paired Unicode and literal backslash-u controls still use Rust",
    "Caught representation-input codec failures send no request and preserve outer and nested writers; no native rejection or uncertain reply triggers reducer replay",
  );
  for (const witness of compatibility)
    checks.push(`Project representation compatibility witness: ${JSON.stringify(witness)}`);
  NodeAssert.equal(hash(await NodeFSP.readFile(source)), sourceHash);
  await finishProof({
    executable,
    fixture,
    proofPath,
    checks,
    sourceHash,
    oracles: [oracle.source],
    sourcePaths: [
      "apps/server/src/orchestration-v2/ProjectStore.ts",
      "apps/server/src/orchestration-v2/ProjectRows.ts",
      "apps/server/src/orchestration-v2/SqliteProjectRows.ts",
      "apps/server/src/orchestration-v2/NativeProjectRows.ts",
      "apps/server/src/orchestration-v2/NativeProjectStore.ts",
      "apps/server/src/persistence/RedbClient.ts",
      "crates/core/src/project_persistence_wire.rs",
      "crates/storage/src/bin/t3-code-ls-db.rs",
      "apps/server/src/orchestration-v2/redb_project_rows.rs",
      "tools/check-redb-projects.mjs",
      "tools/redb-domain-fixture.mjs",
    ],
  });
} finally {
  await fixture.close();
}
