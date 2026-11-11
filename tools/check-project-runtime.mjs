import * as NodeAssert from "node:assert/strict";
import * as NodeChildProcess from "node:child_process";
import { checkArgumentRejection } from "./check-runtime-arguments.mjs";
import {
  projectCases,
  projectOracle,
  projectRequest,
  projectRow,
  projectScript,
} from "./check-project-commands.mjs";

const executable = process.argv[2];
NodeAssert.ok(executable, "Pass the native t3-code-ls-project executable");
const maximumBytes = 128 * 1024 * 1024;
const maximumValues = 1_048_576;
let checks = 0;
let comparisons = 0;
const copy = (value) => JSON.parse(JSON.stringify(value));
const frame = (value) => JSON.stringify(value) + "\n";

function execute(input, args = []) {
  return NodeChildProcess.spawnSync(executable, args, {
    input,
    encoding: "utf8",
    env: {},
    maxBuffer: maximumBytes + 1024,
  });
}

function succeeded(input) {
  const result = execute(input);
  NodeAssert.ifError(result.error);
  NodeAssert.equal(result.status, 0, result.stderr);
  NodeAssert.equal(result.stderr, "");
  NodeAssert.ok(result.stdout.endsWith("\n"));
  NodeAssert.equal(result.stdout.indexOf("\n"), result.stdout.length - 1);
  NodeAssert.ok(Buffer.byteLength(result.stdout) <= maximumBytes);
  checks++;
  return JSON.parse(result.stdout);
}

function rejected(label, input, category = "invalid request", args = []) {
  const result = execute(input, args);
  NodeAssert.ifError(result.error);
  NodeAssert.equal(result.status, 2, label);
  NodeAssert.equal(result.stdout, "", `${label}: no partial event or domain rejection may escape`);
  NodeAssert.equal(result.stderr, `t3-code-ls-project: ${category}\n`, label);
  checks++;
}

for (const { label, input, result } of projectCases.filter((fixture) => fixture.runtime)) {
  NodeAssert.deepEqual(succeeded(frame(input)), result, label);
  comparisons++;
}

const valid = projectRequest({
  type: "project.create",
  commandId: "command:runtime",
  projectId: "project:one",
  title: "Fixture",
  workspaceRoot: "/fixture",
});
const update = projectRequest(
  {
    type: "project.meta.update",
    commandId: "command:update",
    projectId: "project:one",
  },
  { project: projectRow() },
);
const validFrame = frame(valid);

for (const [label, input] of [
  ["empty input", ""],
  ["missing delimiter", validFrame.trimEnd()],
  ["CRLF framing", validFrame.replace(/\n$/, "\r\n")],
  ["extra delimiter", validFrame + "\n"],
  ["two valid requests", validFrame + validFrame],
  ["valid request followed by malformed record", validFrame + "{\n"],
  ["formatted multi-line body", JSON.stringify(valid, null, 2) + "\n"],
  ["trailing data before EOF", validFrame.trimEnd() + " false\n"],
  ["non-object request", "[]\n"],
  ["null request", "null\n"],
  ["byte order mark", "\uFEFF" + validFrame],
  ["invalid UTF-8", Buffer.from([0xff, 0xfe, 0x0a])],
  ["unpaired high surrogate", validFrame.replace('"Fixture"', '"\\ud800"')],
  ["unpaired low surrogate", validFrame.replace('"Fixture"', '"\\udc00"')],
  ["high surrogate followed by non-surrogate", validFrame.replace('"Fixture"', '"\\ud800\\u0041"')],
]) {
  rejected(label, input);
}
checkArgumentRejection(executable, ["--help"], validFrame, "t3-code-ls-project: invalid request\n");
checks++;
NodeAssert.deepEqual(succeeded(" \t" + validFrame.trimEnd() + " \t\n"), projectOracle(valid));
const unicode = copy(valid);
unicode.command.title = "💬";
NodeAssert.deepEqual(
  succeeded(frame(unicode).replace('"💬"', '"\\ud83d\\udcac"')),
  projectOracle(unicode),
);

for (const key of Object.keys(valid)) {
  const value = copy(valid);
  delete value[key];
  rejected(`missing envelope key ${key}`, frame(value));
}
for (const [key, value] of [
  ["version", 2],
  ["version", "1"],
  ["operation", "create"],
  ["operation", null],
  ["eventId", 1],
  ["command", null],
  ["command", []],
  ["state", null],
  ["now", "0"],
  ["now", 0.5],
  ["now", -8_640_000_000_000_001],
  ["now", 8_640_000_000_000_001],
  ["monogramGraphemeCount", 0],
  ["unexpected", true],
]) {
  rejected(`invalid envelope ${key} ${JSON.stringify(value)}`, frame({ ...valid, [key]: value }));
}
for (const state of [
  {},
  { project: null },
  { workspaceOwner: null },
  { ...valid.state, extra: null },
  { project: [], workspaceOwner: null },
  { project: null, workspaceOwner: {} },
]) {
  rejected("invalid project state", frame({ ...valid, state }));
}
for (const [key, value] of [
  ["type", "project.unknown"],
  ["commandId", null],
  ["projectId", false],
  ["title", null],
  ["workspaceRoot", []],
  ["scripts", null],
  ["scripts", [{ id: 7 }]],
]) {
  rejected(
    `invalid create field ${key}`,
    frame({ ...valid, command: { ...valid.command, [key]: value } }),
  );
}
for (const [key, value] of [
  ["title", null],
  ["defaultModelSelection", "model"],
  ["defaultThreadEnvMode", 1],
  ["autoPull", null],
  ["pinned", 1],
  ["archived", "true"],
  ["faviconPath", false],
  ["projectIcon", { kind: "unknown" }],
  ["scripts", [projectScript("valid"), {}]],
]) {
  rejected(
    `invalid update field ${key}`,
    frame({ ...update, command: { ...update.command, [key]: value } }),
  );
}
for (const [key, value] of [
  ["deletedAt", 1],
  ["scripts", null],
  ["pinnedAt", false],
  ["archivedAt", []],
]) {
  rejected(
    `invalid persisted project field ${key}`,
    frame({
      ...update,
      state: { ...update.state, project: { ...update.state.project, [key]: value } },
    }),
  );
}

const monogram = projectRequest(
  { ...update.command, projectIcon: { kind: "monogram", text: "e\u0301A", color: "blue" } },
  update.state,
);
for (const count of [null, -1, 0, 0.5, 4, "2", 9_007_199_254_740_991]) {
  rejected(
    `invalid monogram fact ${JSON.stringify(count)}`,
    frame({ ...monogram, monogramGraphemeCount: count }),
  );
}
rejected(
  "missing monogram text",
  frame({ ...monogram, command: { ...monogram.command, projectIcon: { kind: "monogram" } } }),
);
rejected("non-finite opaque number", validFrame.replace('"Fixture"', '"Fixture","opaque":1e999'));

rejected(
  "duplicate envelope key",
  validFrame.replace('"version":1', '"version":1,"version":1'),
  "duplicate object key",
);
rejected(
  "escaped duplicate key",
  validFrame.replace('"version":1', '"version":1,"\\u0076ersion":1'),
  "duplicate object key",
);
rejected(
  "duplicate command key",
  validFrame.replace('"title":"Fixture"', '"title":"Fixture","title":"Other"'),
  "duplicate object key",
);
rejected(
  "duplicate ignored extension key",
  validFrame.replace('"title":"Fixture"', '"title":"Fixture","extension":{"value":1,"value":2}'),
  "duplicate object key",
);

function nestedArray(depth) {
  let result = null;
  for (let level = 0; level < depth; level++) result = [result];
  return result;
}
function nestedRequest(depth) {
  return {
    ...update,
    command: {
      ...update.command,
      defaultModelSelection: {
        instanceId: "provider:fixture",
        model: "fixture",
        extension: nestedArray(depth),
      },
    },
  };
}
NodeAssert.deepEqual(succeeded(frame(nestedRequest(124))), projectOracle(nestedRequest(124)));
rejected(
  "retained payload expands beyond response depth",
  frame(nestedRequest(125)),
  "output limit exceeded",
);
rejected("request exceeds nesting limit", frame(nestedRequest(126)));

function valueCount(value) {
  if (value === null || typeof value !== "object") return 1;
  return 1 + Object.values(value).reduce((total, item) => total + valueCount(item), 0);
}
const valueLimited = projectRequest({
  ...valid.command,
  scripts: [projectScript("valid", { extension: [] })],
});
const availableValues = maximumValues - valueCount(valueLimited);
const outputDelta = valueCount(projectOracle(valueLimited)) - valueCount(valueLimited);
NodeAssert.ok(outputDelta > 0, "Fixture must exercise output amplification");
valueLimited.command.scripts[0].extension = Array.from({ length: availableValues }, () => null);
rejected("valid request exceeds output value budget", frame(valueLimited), "output limit exceeded");
valueLimited.command.scripts[0].extension.push(null);
rejected("request exceeds value budget", frame(valueLimited));
valueLimited.command.scripts[0].extension = [];

// Byte budgets include the LF delimiter. Whitespace reaches the exact input
// boundary without creating a second large payload in the planned event.
const body = validFrame.slice(0, -1);
const atLimit = body + " ".repeat(maximumBytes - Buffer.byteLength(body) - 1) + "\n";
NodeAssert.deepEqual(succeeded(atLimit), projectOracle(valid));
rejected("one byte beyond input boundary", atLimit.slice(0, -1) + " \n", "input limit exceeded");

// The event repeats commandId as correlationId. A legal bounded request can
// therefore produce an oversized event, which must fail before any stdout write.
const oversizedResponse = {
  ...valid,
  command: { ...valid.command, commandId: "c".repeat(65 * 1024 * 1024) },
};
rejected("complete response byte budget", frame(oversizedResponse), "output limit exceeded");

process.stdout.write(
  JSON.stringify({
    status: "passed",
    checks,
    comparisons,
    protocol: "project plan v1, LF and EOF, bounded complete responses",
  }) + "\n",
);
