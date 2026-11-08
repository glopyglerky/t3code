#!/usr/bin/env node
import * as assert from "node:assert/strict";
import * as childProcess from "node:child_process";
import * as module from "node:module";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as References from "effect/References";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as SqlSchema from "effect/sql/SqlSchema";
import * as contracts from "@t3tools/contracts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as SqlitePersistence from "../../../apps/server/src/persistence/Sqlite.ts";
import { projectCases } from "./check-project-commands.mjs";
import { upstreamSource } from "./upstream-source.mjs";

assert.ok(process.argv[2], "Pass project_persistence_matrix executable");
const executable = process.argv[2];
const singleMode = process.argv[3] === "--single";
const source = module
  .stripTypeScriptTypes(upstreamSource("apps/server/src/orchestration-v2/ProjectStore.ts"), {
    mode: "strip",
  })
  .replace(/^import\s[\s\S]*?;\s*$/gm, "")
  .replace(/^export (?=(?:function|const|class) )/gm, "");
const imports = {
  Context,
  Effect,
  Layer,
  Option,
  Schema,
  SqlClient,
  SqlSchema,
  NodeSqliteClient,
  ...Object.fromEntries(
    [
      "IsoDateTime",
      "ModelSelection",
      "StoredProjectIcon",
      "ProjectId",
      "ProjectScript",
      "ThreadEnvMode",
    ].map((key) => [key, contracts[key]]),
  ),
};
const upstream = new Function(
  ...Object.keys(imports),
  `${source}\nreturn { ProjectStoreV2, ProjectRow, layer };`,
)(...Object.values(imports));
const rowIsValid = Schema.is(upstream.ProjectRow);
const eventIsValid = Schema.is(
  Schema.toType(
    Schema.Union([
      contracts.ApplicationProjectCreatedEvent.mapFields(
        ({ sequence: _sequence, ...fields }) => fields,
      ),
      contracts.ApplicationProjectMetaUpdatedEvent.mapFields(
        ({ sequence: _sequence, ...fields }) => fields,
      ),
      contracts.ApplicationProjectDeletedEvent.mapFields(
        ({ sequence: _sequence, ...fields }) => fields,
      ),
    ]),
  ),
);
const eligible = projectCases.filter(
  ({ input, result }) =>
    result.error !== undefined ||
    (eventIsValid(result.ok) && (input.state.project === null || rowIsValid(input.state.project))),
);
const cases = singleMode ? eligible.filter(({ runtime }) => runtime) : eligible;
function invokePlanner(input) {
  const processResult = childProcess.spawnSync(executable, [], {
    input,
    encoding: "utf8",
    env: {},
    maxBuffer: 128 * 1024 * 1024,
    timeout: 120_000,
  });
  assert.ifError(processResult.error);
  assert.equal(processResult.status, 0, processResult.stderr);
  assert.equal(processResult.stderr, "");
  return processResult.stdout
    .trimEnd()
    .split("\n")
    .map((line) => JSON.parse(line));
}
const frames = cases.map(({ input }) =>
  JSON.stringify({ ...input, operation: "plan-and-project" }),
);
const actual = singleMode
  ? frames.flatMap((frame) => invokePlanner(frame + "\n"))
  : invokePlanner(frames.join("\n") + "\n");
assert.equal(actual.length, cases.length);
let committed = 0;
await Effect.runPromise(
  Effect.gen(function* () {
    const store = yield* upstream.ProjectStoreV2;
    const sql = yield* SqlClient.SqlClient;
    for (let index = 0; index < cases.length; index++) {
      const { input, result, label } = cases[index];
      if (result.error !== undefined) {
        assert.deepEqual(actual[index], result, label);
        continue;
      }
      yield* sql`DELETE FROM projection_projects`;
      const row = input.state.project;
      if (row !== null) {
        yield* sql`INSERT INTO projection_projects (
        project_id, title, workspace_root, default_model_selection_json,
        default_thread_env_mode, auto_pull, favicon_path, project_icon_json,
        scripts_json, pinned_at, archived_at, created_at, updated_at, deleted_at
      ) VALUES (
        ${row.projectId}, ${row.title}, ${row.workspaceRoot},
        ${row.defaultModelSelection === null ? null : JSON.stringify(row.defaultModelSelection)},
        ${row.defaultThreadEnvMode}, ${row.autoPull ? 1 : 0}, ${row.faviconPath},
        ${row.projectIcon === null ? null : JSON.stringify(row.projectIcon)},
        ${JSON.stringify(row.scripts)}, ${row.pinnedAt}, ${row.archivedAt},
        ${row.createdAt}, ${row.updatedAt}, ${row.deletedAt}
      )`;
      }
      yield* sql.withTransaction(store.apply(result.ok));
      const projected = yield* store.get(input.command.projectId, { includeDeleted: true });
      assert.deepEqual(
        actual[index],
        {
          version: 1,
          ok: { event: result.ok, project: Option.getOrNull(projected) },
        },
        label,
      );
      committed++;
    }
  }).pipe(
    Effect.provide(upstream.layer.pipe(Layer.provideMerge(SqlitePersistence.layerMemory))),
    Effect.provideService(References.MinimumLogLevel, "Warning"),
    Effect.scoped,
  ),
);

const frameExecutable = singleMode ? executable : process.argv[3];
if (frameExecutable) {
  const single = frameExecutable;
  const valid = { ...cases[0].input, operation: "plan-and-project" };
  const invoke = (input, args = []) =>
    childProcess.spawnSync(single, args, {
      input,
      encoding: "utf8",
      env: {},
      maxBuffer: 32 * 1024 * 1024,
      timeout: 30_000,
    });
  const good = invoke(JSON.stringify(valid) + "\n");
  assert.equal(good.status, 0, good.stderr);
  assert.deepEqual(JSON.parse(good.stdout), actual[0]);
  const malformed = [
    JSON.stringify(valid),
    JSON.stringify(valid) + "\n" + JSON.stringify(valid) + "\n",
    JSON.stringify({ ...valid, operation: "plan" }) + "\n",
    JSON.stringify({ ...valid, unexpected: true }) + "\n",
    '{"version":1,' + JSON.stringify(valid).slice(1) + "\n",
    " ".repeat(16 * 1024 * 1024) + "\n",
  ];
  for (const input of malformed) {
    const failed = invoke(input);
    assert.ifError(failed.error);
    assert.equal(failed.status, 2);
    assert.equal(failed.stdout, "");
  }
  assert.equal(invoke(JSON.stringify(valid) + "\n", ["unexpected"]).status, 2);
}
console.log(
  JSON.stringify({
    status: "passed",
    comparisons: cases.length,
    projectedRows: committed,
    plannerOnlyFixturesOutsidePersistenceSchema: projectCases.length - eligible.length,
    oracle: "immutable ProjectCommands and ProjectStoreV2 with real Effect and SQLite transactions",
    frameChecks: frameExecutable ? 8 : 0,
  }),
);
