import * as NodeAssert from "node:assert/strict";
import * as NodeChildProcess from "node:child_process";
import * as NodeModule from "node:module";
import * as NodeURL from "node:url";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import * as SchemaGetter from "effect/SchemaGetter";
import * as SchemaTransformation from "effect/SchemaTransformation";
import { upstreamSource } from "./upstream-source.mjs";

function immutableModule(path, imports, exports) {
  const source = NodeModule.stripTypeScriptTypes(upstreamSource(path), { mode: "strip" })
    .replace(/^import\s[\s\S]*?;\s*$/gm, "")
    .replace(/^export (?=(?:function|const|class) )/gm, "");
  return new Function(...Object.keys(imports), `${source}\nreturn {${exports.join(",")}};`)(
    ...Object.values(imports),
  );
}

// Execute the entire immutable dependency modules with the real Effect runtime.
// In particular, SCRIPT_RUN_COMMAND_PATTERN is the actual schema, not a regex stub.
const base = immutableModule(
  "packages/contracts/src/baseSchemas.ts",
  { Effect, Option, Schema, SchemaGetter, SchemaTransformation },
  ["ProjectId", "ForwardCompatibleArray", "TrimmedString"],
);
const keybindings = immutableModule("packages/contracts/src/keybindings.ts", { Schema, ...base }, [
  "MAX_SCRIPT_ID_LENGTH",
  "SCRIPT_RUN_COMMAND_PATTERN",
]);
const upstream = immutableModule(
  "apps/server/src/orchestration-v2/ProjectCommands.ts",
  { DateTime, Result, Schema, ProjectId: base.ProjectId, ...keybindings },
  ["planProjectCommand", "encodeProjectCommandRejection"],
);
NodeAssert.equal(keybindings.MAX_SCRIPT_ID_LENGTH, 24, "Review changed upstream script limit");

const copy = (value) => JSON.parse(JSON.stringify(value));
const segmenter = new Intl.Segmenter(undefined, { granularity: "grapheme" });
export const projectNow = Date.parse("2026-11-08T12:00:00.123Z");

export function projectRow(overrides = {}) {
  return {
    projectId: "project:one",
    title: "Project 🦀",
    workspaceRoot: "/fixture/project",
    defaultModelSelection: null,
    defaultThreadEnvMode: null,
    autoPull: false,
    faviconPath: null,
    projectIcon: null,
    scripts: [],
    pinnedAt: null,
    archivedAt: null,
    createdAt: "2025-12-01T00:00:00.000Z",
    updatedAt: "2026-01-01T00:00:00.000Z",
    deletedAt: null,
    ...overrides,
  };
}

export function projectScript(id, overrides = {}) {
  return {
    id,
    name: `Script ${id}`,
    command: "printf '%s' fixture",
    icon: "configure",
    runOnWorktreeCreate: false,
    ...overrides,
  };
}

export function projectRequest(command, state = {}, now = projectNow) {
  const monogram =
    command.type === "project.meta.update" && command.projectIcon?.kind === "monogram";
  return {
    version: 1,
    operation: "plan",
    command: copy(command),
    state: {
      project: state.project === undefined ? null : copy(state.project),
      workspaceOwner: state.workspaceOwner === undefined ? null : copy(state.workspaceOwner),
    },
    eventId: "event:project",
    now,
    monogramGraphemeCount: monogram
      ? Array.from(segmenter.segment(command.projectIcon.text)).length
      : null,
  };
}

export function projectOracle(input) {
  const command = copy(input.command);
  const state = {
    project: input.state.project === null ? undefined : copy(input.state.project),
    workspaceOwner:
      input.state.workspaceOwner === null ? undefined : copy(input.state.workspaceOwner),
  };
  const before = copy({ command, state });
  const result = upstream.planProjectCommand({
    command,
    state,
    eventId: input.eventId,
    now: DateTime.makeUnsafe(input.now),
  });
  NodeAssert.deepEqual(copy({ command, state }), before, "Upstream mutated command or state");
  return Result.isFailure(result)
    ? { version: 1, error: JSON.parse(upstream.encodeProjectCommandRejection(result.failure)) }
    : { version: 1, ok: copy(result.success) };
}

export const projectCases = [];
function add(label, command, state = {}, options = {}) {
  const input = projectRequest(command, state, options.now ?? projectNow);
  if (options.eventId !== undefined) input.eventId = options.eventId;
  const result = projectOracle(input);
  projectCases.push({ label, input, result, runtime: options.runtime === true });
  return result;
}

const create = (changes = {}) => ({
  type: "project.create",
  commandId: "command:create",
  projectId: "project:one",
  title: "Project 🦀",
  workspaceRoot: "/fixture/project",
  ...changes,
});
const update = (changes = {}) => ({
  type: "project.meta.update",
  commandId: "command:update",
  projectId: "project:one",
  ...changes,
});
const remove = (changes = {}) => ({
  type: "project.delete",
  commandId: "command:delete",
  projectId: "project:one",
  ...changes,
});
const active = { project: projectRow() };
const absent = {};
const deleted = { project: projectRow({ deletedAt: "2026-10-01T00:00:00.000Z" }) };
const other = projectRow({ projectId: "project:other" });
const monogram = (text) => ({ kind: "monogram", text, color: "indigo" });

add("create supplies only upstream defaults", create(), absent, { runtime: true });
add("create keeps an explicitly empty script list", create({ scripts: [] }), absent, {
  runtime: true,
});
add(
  "create preserves legacy script ids without applying update-only checks",
  create({ scripts: [projectScript("legacy.invalid"), projectScript("legacy.invalid")] }),
  absent,
  { runtime: true },
);
for (const [label, state] of [
  ["absent", absent],
  ["active", active],
  ["deleted", deleted],
]) {
  for (const [owner, workspaceOwner] of [
    ["none", undefined],
    ["self", projectRow()],
    ["other", other],
  ]) {
    add(
      `create ${label} project with ${owner} workspace owner`,
      create(),
      { ...state, workspaceOwner },
      {
        runtime: true,
      },
    );
    add(
      `update ${label} project with ${owner} workspace owner`,
      update({ workspaceRoot: "/new" }),
      {
        ...state,
        workspaceOwner,
      },
      { runtime: true },
    );
    add(
      `delete ${label} project with ${owner} workspace owner`,
      remove(),
      { ...state, workspaceOwner },
      {
        runtime: true,
      },
    );
  }
}

add("no metadata changes still creates the update event", update(), active, { runtime: true });
add(
  "workspace ownership is ignored unless the command changes the root",
  update({ title: "Renamed" }),
  { ...active, workspaceOwner: other },
  { runtime: true },
);
add(
  "unknown command fields do not leak into its event payload",
  update({ unknown: { ignored: true }, metadata: { ignored: true } }),
  active,
  { runtime: true },
);

const extension = JSON.parse(
  '{"__proto__":{"retained":true},"constructor":"payload data","nested":[null,false,0,1.25,"💬 é العربية",{"deep":"kept"}]}',
);
const allChanges = {
  title: "Edited project",
  workspaceRoot: "/fixture/new\nworkspace",
  defaultModelSelection: {
    instanceId: "provider:fixture",
    model: "fixture-model",
    options: [{ id: "reasoning", value: "high", extension }],
    extension,
  },
  defaultThreadEnvMode: "worktree",
  autoPull: true,
  faviconPath: "/fixture/icon.svg",
  projectIcon: { ...monogram("e\u0301A"), extension },
  scripts: [
    projectScript("build", {
      runOnSettle: true,
      stopOnSettle: false,
      async: true,
      previewUrl: "https://fixture.test/preview",
      autoOpenPreview: false,
      extension,
    }),
  ],
  pinned: true,
  archived: true,
};
add("all optional payloads and nested extensions survive", update(allChanges), active, {
  runtime: true,
});

// Exhaust every combination of omitted metadata fields. This checks absence itself,
// including the independent pin/archive flags, not merely equivalent final rows.
const fields = Object.entries(allChanges);
for (let mask = 0; mask < 1 << fields.length; mask++) {
  const changes = Object.fromEntries(fields.filter((_, index) => (mask & (1 << index)) !== 0));
  add(`optional field presence ${mask}`, update(changes), active);
}
const nullableFields = [
  "defaultModelSelection",
  "defaultThreadEnvMode",
  "faviconPath",
  "projectIcon",
];
for (let mask = 0; mask < 1 << nullableFields.length; mask++) {
  const changes = { ...allChanges };
  for (let index = 0; index < nullableFields.length; index++) {
    if ((mask & (1 << index)) !== 0) changes[nullableFields[index]] = null;
  }
  add(`explicit null payload combination ${mask}`, update(changes), active, {
    runtime: mask === 15,
  });
}
for (const [name, value] of [
  ["autoPull", false],
  ["scripts", []],
  ["projectIcon", { kind: "emoji", emoji: "👩🏾‍💻", extension }],
  ["projectIcon", { kind: "lucide", name: "terminal", color: "blue", extension }],
  ["defaultThreadEnvMode", "local"],
]) {
  add(`explicit ${name} ${JSON.stringify(value)}`, update({ [name]: value }), active, {
    runtime: true,
  });
}

for (const pinnedAt of [null, "2025-12-01T00:00:00.000Z", "opaque original pin timestamp"]) {
  for (const archivedAt of [
    null,
    "2025-12-02T00:00:00.000Z",
    "opaque original archive timestamp",
  ]) {
    for (const pinned of [undefined, false, true]) {
      for (const archived of [undefined, false, true]) {
        add(
          `pin/archive ${JSON.stringify([pinnedAt, archivedAt, pinned, archived])}`,
          update({ pinned, archived }),
          { project: projectRow({ pinnedAt, archivedAt }) },
          {
            runtime:
              pinnedAt !== null && archivedAt !== null && pinned === false && archived === false,
          },
        );
      }
    }
  }
}

const scriptIds = [
  "",
  "a",
  "0",
  "-a",
  "a-",
  "a-b",
  "a--b",
  "A",
  "UPPER",
  "a_b",
  "a.b",
  "a b",
  " a",
  "a ",
  "a\n",
  "a\r",
  "a\u0000",
  "a/run",
  "script.a.run",
  "__proto__",
  "constructor",
  "toString",
  "a".repeat(23),
  "a".repeat(24),
  "a".repeat(25),
  "a".repeat(100),
  "é",
  "e\u0301",
  "🦀",
  "🦀a",
  "a".repeat(23) + "𐐀",
  "日本語",
  "a\u2028",
  "a\u2029",
];
for (const id of scriptIds) {
  add(`new script ID ${JSON.stringify(id)}`, update({ scripts: [projectScript(id)] }), active, {
    runtime: ["a".repeat(24), "a".repeat(25), "🦀a", "a\n"].includes(id),
  });
  add(
    `legacy script ID remains editable ${JSON.stringify(id)}`,
    update({ scripts: [projectScript(id, { command: "edited", extension })] }),
    { project: projectRow({ scripts: [projectScript(id)] }) },
    { runtime: id === "🦀a" },
  );
}
add(
  "duplicate new valid scripts retain order and payload",
  update({
    scripts: [
      projectScript("valid", { name: "first" }),
      projectScript("valid", { name: "second" }),
    ],
  }),
  active,
  { runtime: true },
);
add(
  "duplicate legacy scripts retain order and payload",
  update({
    scripts: [
      projectScript("old.id", { name: "first" }),
      projectScript("old.id", { name: "second" }),
    ],
  }),
  { project: projectRow({ scripts: [projectScript("old.id"), projectScript("old.id")] }) },
  {
    runtime: true,
  },
);
add(
  "removing every invalid legacy script is accepted",
  update({ scripts: [] }),
  {
    project: projectRow({ scripts: [projectScript("old.id"), projectScript("UPPER")] }),
  },
  { runtime: true },
);
for (const ids of [
  ["🦀", "UPPER"],
  ["UPPER", "🦀"],
  ["valid", "a".repeat(25), "a.b"],
]) {
  add(
    `first invalid script wins ${JSON.stringify(ids)}`,
    update({ scripts: ids.map((id) => projectScript(id)) }),
    { ...active, workspaceOwner: other },
    { runtime: true },
  );
}

for (const text of [
  "",
  "A",
  "AB",
  "ABC",
  "e\u0301",
  "e\u0301A",
  "e\u0301AB",
  "क्‍ष",
  "क्‍षक्‍ष",
  "क्‍षAB",
  "각",
  "각A",
  "العربية",
  "a\u200db",
  "a\u200cb",
  "𐐀𐐀",
  "𐐀𐐀𐐀",
]) {
  add(
    `Intl grapheme monogram ${JSON.stringify(text)}`,
    update({ projectIcon: monogram(text) }),
    active,
    {
      runtime: text.length > 2 && Array.from(segmenter.segment(text)).length <= 2,
    },
  );
}
const invalidChanges = {
  projectIcon: monogram("ABC"),
  scripts: [projectScript("bad.id")],
  workspaceRoot: "/claimed",
};
for (const [label, state] of [
  ["missing", absent],
  ["deleted", deleted],
  ["active", active],
]) {
  add(
    `missing/monogram/script/workspace precedence with ${label} project`,
    update(invalidChanges),
    {
      ...state,
      workspaceOwner: other,
    },
    { runtime: true },
  );
}
add(
  "script rejection precedes workspace conflict",
  update({ ...invalidChanges, projectIcon: monogram("AB") }),
  { ...active, workspaceOwner: other },
  { runtime: true },
);
add(
  "workspace conflict follows valid monogram and scripts",
  update({
    ...invalidChanges,
    projectIcon: monogram("AB"),
    scripts: [projectScript("valid")],
  }),
  { ...active, workspaceOwner: other },
  { runtime: true },
);

const dates = new Set([
  -8_640_000_000_000_000,
  -8_640_000_000_000_000 + 1,
  -62_167_219_200_001,
  -62_167_219_200_000,
  -62_167_219_199_999,
  -86_400_001,
  -86_400_000,
  -86_399_999,
  -1,
  0,
  1,
  86_399_999,
  86_400_000,
  86_400_001,
  253_402_300_799_999,
  253_402_300_800_000,
  253_402_300_800_001,
  8_640_000_000_000_000 - 1,
  8_640_000_000_000_000,
]);
for (const year of [
  -400, -100, -1, 0, 1, 99, 100, 400, 1600, 1900, 2000, 2024, 2100, 9999, 10000,
]) {
  for (const month of [0, 1, 2, 11]) {
    const date = new Date(0);
    date.setUTCFullYear(year, month, month === 1 ? 28 : 1);
    dates.add(date.getTime());
    dates.add(date.getTime() + 86_400_000 - 1);
  }
}
for (const now of dates) {
  add(`DateTime project create ${now}`, create(), absent, {
    now,
    runtime: [-8_640_000_000_000_000, -1, 0, 253_402_300_800_000, 8_640_000_000_000_000].includes(
      now,
    ),
  });
  add(`DateTime project update ${now}`, update({ pinned: true, archived: true }), active, { now });
  add(`DateTime project delete ${now}`, remove(), active, { now });
}
for (const projectId of ["project:'quoted", "project:🦀\nالعربية", "__proto__", "constructor"]) {
  add(
    `identity and escaped rejection ${JSON.stringify(projectId)}`,
    create({ projectId }),
    active,
    {
      runtime: true,
      eventId: `event:${projectId}`,
    },
  );
}

// Exercise a complete decision lifecycle. Projection updates here apply the actual
// upstream event payload, so later commands are checked against earlier outcomes.
let row;
const lifecycle = [
  create({ scripts: [projectScript("legacy.invalid")] }),
  update({ title: "Renamed", pinned: true, archived: true }),
  update({ pinned: true, archived: true }),
  update({
    pinned: false,
    archived: false,
    defaultModelSelection: allChanges.defaultModelSelection,
  }),
  update({
    scripts: [projectScript("legacy.invalid", { command: "repaired" }), projectScript("lint")],
  }),
  update({ scripts: [projectScript("brand.new.invalid")] }),
  update({ scripts: [], defaultModelSelection: null, faviconPath: null, projectIcon: null }),
  remove(),
  update({ title: "Too late" }),
  remove(),
  create(),
];
for (let index = 0; index < lifecycle.length; index++) {
  const command = { ...lifecycle[index], commandId: `command:lifecycle:${index}` };
  const outcome = add(
    `lifecycle step ${index} ${command.type}`,
    command,
    { project: row },
    {
      now: projectNow + index,
      runtime: true,
    },
  );
  if (outcome.ok !== undefined) {
    row = projectRow({ ...row, ...outcome.ok.payload });
  }
}

export function checkProjectMatrix(executable) {
  const run = NodeChildProcess.spawnSync(executable, [], {
    input: projectCases.map(({ input }) => JSON.stringify(input)).join("\n") + "\n",
    encoding: "utf8",
    env: {},
    maxBuffer: 128 * 1024 * 1024,
  });
  NodeAssert.ifError(run.error);
  NodeAssert.equal(run.status, 0, run.stderr);
  NodeAssert.equal(run.stderr, "");
  const lines = run.stdout.trimEnd().split("\n");
  NodeAssert.equal(lines.length, projectCases.length);
  for (let index = 0; index < projectCases.length; index++) {
    NodeAssert.deepEqual(
      JSON.parse(lines[index]),
      projectCases[index].result,
      projectCases[index].label,
    );
  }
  return projectCases.length;
}

if (
  process.argv[1] !== undefined &&
  import.meta.url === NodeURL.pathToFileURL(process.argv[1]).href
) {
  NodeAssert.ok(process.argv[2], "Pass the Lust-built project_commands_matrix executable");
  const cases = checkProjectMatrix(process.argv[2]);
  process.stdout.write(
    JSON.stringify({
      status: "passed",
      cases,
      oracle: "immutable ProjectCommands.ts with real Effect and contract schemas",
    }) + "\n",
  );
}
