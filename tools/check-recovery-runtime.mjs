import * as NodeAssert from "node:assert/strict";
import * as NodeChildProcess from "node:child_process";
import * as NodeModule from "node:module";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import { upstreamSource } from "./upstream-source.mjs";

const executable = process.argv[2];
NodeAssert.ok(executable, "Pass the Lust-built recovery executable");
const maximumBytes = 128 * 1024 * 1024;
const maximumEvents = 65_536;
const source = upstreamSource("packages/provider-core/src/server/subagentProjection.ts");
const contracts = upstreamSource("packages/contracts/src/orchestrationV2.ts");
const start = source.indexOf("function writtenIds(");
const activeStart = contracts.indexOf("export function isOrchestrationV2WorkActive(");
const activeEnd = contracts.indexOf("\n}", activeStart);
NodeAssert.ok(start >= 0 && activeStart >= 0 && activeEnd > activeStart);
const javascript = NodeModule.stripTypeScriptTypes(
  `${contracts.slice(activeStart, activeEnd + 2)}\n${source.slice(start)}`,
  { mode: "strip" },
).replace(/^export (?=(?:function|const) )/gm, "");
const upstream = new Function(
  "Effect",
  `${javascript}\nreturn {
  endOrphanedNativeSubagent, endRunlessRootTurns,
};`,
)(Effect);

const copy = (value) => JSON.parse(JSON.stringify(value));
let checks = 0;
let comparisons = 0;
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
  const response = JSON.parse(result.stdout);
  NodeAssert.deepEqual(Object.keys(response), ["version", "events"]);
  NodeAssert.equal(response.version, 1);
  for (const event of response.events) NodeAssert.equal(Object.hasOwn(event, "id"), false);
  checks++;
  return response;
}

function rejected(label, input, category = "invalid request", args = []) {
  const result = execute(input, args);
  NodeAssert.ifError(result.error);
  NodeAssert.equal(result.status, 2, label);
  NodeAssert.equal(result.stdout, "", `${label}: failure exposed a partial response`);
  NodeAssert.equal(result.stderr, `t3-code-ls-recovery: ${category}\n`, label);
  checks++;
}

function expected(fixture) {
  const input = copy(fixture);
  input.now = DateTime.makeUnsafe(input.now);
  if (input.completedAt !== undefined) input.completedAt = DateTime.makeUnsafe(input.completedAt);
  // Convert contract timestamps only. Provider-defined nested fields remain JSON.
  for (const records of Object.values(input.projection)) {
    if (!Array.isArray(records)) continue;
    for (const record of records) {
      for (const key of ["startedAt", "completedAt", "updatedAt"]) {
        if (typeof record[key] === "string") record[key] = DateTime.makeUnsafe(record[key]);
      }
    }
  }
  let allocated = 0;
  const allocateEventId = () => Effect.sync(() => `event:upstream:${allocated++}`);
  const effect =
    fixture.operation === "orphan"
      ? upstream.endOrphanedNativeSubagent({ ...input, allocateEventId })
      : fixture.operation === "roots"
        ? upstream.endRunlessRootTurns({ ...input, allocateEventId })
        : Effect.gen(function* () {
            const events = [];
            for (const subagent of input.projection.subagents) {
              events.push(
                ...(yield* upstream.endOrphanedNativeSubagent({
                  projection: input.projection,
                  subagentId: subagent.id,
                  status: input.status,
                  now: input.now,
                  completedAt: input.completedAt,
                  emitted: [...input.emitted, ...events],
                  allocateEventId,
                })),
              );
            }
            events.push(
              ...(yield* upstream.endRunlessRootTurns({
                ...input,
                emitted: [...input.emitted, ...events],
                allocateEventId,
              })),
            );
            return events;
          });
  const events = copy(Effect.runSync(effect)).map(({ id: _id, ...event }) => event);
  NodeAssert.equal(allocated, events.length);
  return { version: 1, events };
}

function compare(label, input) {
  NodeAssert.deepEqual(succeeded(JSON.stringify(input) + "\n"), expected(input), label);
  comparisons++;
}

const now = "2026-11-08T12:00:00.000Z";
const earlier = "2026-11-07T12:00:00.000Z";
const terminal = ["completed", "failed", "interrupted", "cancelled"];
function node(id, kind = "root_turn", overrides = {}) {
  return {
    id,
    kind,
    runId: null,
    status: "running",
    completedAt: null,
    threadId: "thread:source",
    updatedAt: earlier,
    extension: {
      timestampText: "preserve arbitrary text",
      rows: [{ opaque: [null, "💬", false, 1.5] }],
    },
    ...overrides,
  };
}
function item(id, type, nodeId = "root:one", overrides = {}) {
  return {
    id,
    type,
    nodeId,
    runId: null,
    status: "running",
    completedAt: null,
    updatedAt: earlier,
    text: "Retain text\n\u0000💬 e\u0301",
    extension: { array: [1, { retained: true }] },
    ...(type === "subagent" ? { subagentId: "subagent:one", result: "original result" } : {}),
    ...(["reasoning", "assistant_message", "proposed_plan"].includes(type)
      ? { streaming: true }
      : {}),
    ...overrides,
  };
}
function subagent(id = "subagent:one", overrides = {}) {
  return {
    id,
    threadId: "thread:source",
    runId: null,
    origin: "provider_native",
    driver: "codex",
    providerInstanceId: "provider:source",
    status: "running",
    completedAt: null,
    updatedAt: earlier,
    result: "original result",
    prompt: "Preserve delegated instructions",
    workflow: { phases: [{ title: "Inspect", state: "open" }] },
    ...overrides,
  };
}
function providerTurn(id, nodeId = "root:one", overrides = {}) {
  return {
    id,
    nodeId,
    runAttemptId: null,
    status: "running",
    completedAt: null,
    startedAt: earlier,
    updatedAt: earlier,
    tokenUsage: { input: 17, output: 23 },
    ...overrides,
  };
}
function projection() {
  return {
    subagents: [subagent(), subagent("subagent:owned", { origin: "app_owned" })],
    nodes: [
      node("subagent:one", "subagent"),
      node("root:one"),
      node("root:two"),
      node("root:owned", "root_turn", { runId: "run:owned" }),
    ],
    turnItems: [
      item("item:subagent", "subagent"),
      item("item:reasoning", "reasoning"),
      item("item:command", "command_execution"),
      item("item:tool", "dynamic_tool"),
      item("item:plan", "proposed_plan"),
      item("item:second", "assistant_message", "root:two"),
      item("item:owned", "dynamic_tool", "root:one", { runId: "run:owned" }),
    ],
    providerTurns: [
      providerTurn("turn:one"),
      providerTurn("turn:two", "root:two"),
      providerTurn("turn:owned", "root:one", { runAttemptId: "attempt:owned" }),
    ],
  };
}
function request(operation = "reconcile", overrides = {}) {
  return {
    version: 1,
    operation,
    projection: projection(),
    emitted: [],
    status: "cancelled",
    now,
    ...(operation === "orphan"
      ? { subagentId: "subagent:one" }
      : {
          threadId: "thread:context",
          providerInstanceId: "provider:context",
        }),
    ...overrides,
  };
}

for (const operation of ["orphan", "roots", "reconcile"]) {
  for (const status of terminal) {
    for (const completedAt of [undefined, earlier]) {
      compare(
        `${operation} ordered payload and timestamp parity`,
        request(operation, {
          status,
          ...(completedAt === undefined ? {} : { completedAt }),
        }),
      );
    }
  }
}
for (const result of [undefined, null, "", "Replacement result\n🦀"]) {
  const state = projection();
  state.subagents[0].runId = "run:native";
  compare(
    "orphan optional result and run scope",
    request("orphan", {
      projection: state,
      ...(result === undefined ? {} : { result }),
    }),
  );
}
for (const operation of ["orphan", "roots", "reconcile"]) {
  const initial = request(operation);
  const prior = expected(initial).events.map((event) => ({
    type: event.type,
    payload: { id: event.payload.id },
  }));
  compare(`${operation} earlier events independently exclude each record`, {
    ...initial,
    emitted: prior,
  });
  const partial = prior.filter((_, index) => index % 2 === 0);
  compare(`${operation} partial prior batch preserves remaining event order`, {
    ...initial,
    emitted: [...partial, { type: "ignored.event", opaque: true }],
  });
}
const duplicates = projection();
duplicates.subagents.push(copy(duplicates.subagents[0]));
duplicates.nodes.push(copy(duplicates.nodes[1]));
duplicates.turnItems.push(copy(duplicates.turnItems[1]));
duplicates.providerTurns.push(copy(duplicates.providerTurns[0]));
compare(
  "reconcile suppresses prior orphan updates while roots retain entry-only exclusions",
  request("reconcile", { projection: duplicates }),
);
compare(
  "root duplicate rows preserve original iteration semantics",
  request("roots", { projection: duplicates }),
);
for (const operation of ["orphan", "roots"]) {
  const input = request(operation);
  delete input.projection[operation === "orphan" ? "providerTurns" : "subagents"];
  compare("irrelevant projection arrays may be omitted", input);
}
for (const at of [
  "-271821-04-20T00:00:00.000Z",
  "0000-01-01T00:00:00.000Z",
  "+275760-09-13T00:00:00.000Z",
]) {
  compare(
    "extended UTC years keep their exact wire shape",
    request("reconcile", { now: at, completedAt: at }),
  );
}

const empty = request("reconcile", {
  projection: { nodes: [], subagents: [], turnItems: [], providerTurns: [] },
});
const valid = JSON.stringify(empty);
for (const [label, wire] of [
  ["empty input", ""],
  ["missing LF", valid],
  ["extra LF", `${valid}\n\n`],
  ["CRLF", `${valid}\r\n`],
  ["second JSON request", `${valid}\n${valid}\n`],
  ["trailing data", `${valid}x\n`],
  ["non-object", "[]\n"],
  ["invalid UTF-8", Buffer.from([0xff, 10])],
])
  rejected(label, wire);
for (const changes of [
  { version: 2 },
  { version: "1" },
  { operation: "unknown" },
  { status: "running" },
  { now: 0 },
  { now: null },
  { completedAt: null },
  { result: "wrong operation" },
  { subagentId: "wrong scope" },
  { extra: "PRIVATE INPUT MUST NOT APPEAR IN STDERR" },
  { emitted: null },
  { emitted: [{ type: "node.updated", payload: {} }] },
  { projection: { nodes: [], subagents: [], turnItems: [] } },
])
  rejected("invalid envelope or required field", JSON.stringify({ ...empty, ...changes }) + "\n");
rejected("unsupported command arguments", valid + "\n", "invalid request", ["unexpected"]);
rejected("duplicate envelope keys", valid.slice(0, -1) + ',"version":1}\n', "duplicate object key");
rejected(
  "escaped duplicate keys",
  valid.slice(0, -1) + ',"ver\\u0073ion":1}\n',
  "duplicate object key",
);
rejected(
  "nested duplicate payload keys",
  valid.replace('"nodes":[]', '"nodes":[],"extra":{"key":1,"key":2}') + "\n",
  "duplicate object key",
);
const unpaired = copy(empty);
unpaired.projection.extra = "\ud800";
rejected("unpaired UTF-16 surrogate", JSON.stringify(unpaired) + "\n");
const lateInvalid = request();
lateInvalid.projection.nodes.push(node("invalid", "root_turn", { completedAt: 0 }));
rejected(
  "late invalid record exposes no earlier planned events",
  JSON.stringify(lateInvalid) + "\n",
);

const deep = copy(empty);
deep.projection.extra = "leaf";
for (let depth = 0; depth < 128; depth++) deep.projection.extra = [deep.projection.extra];
rejected("nesting depth limit", JSON.stringify(deep) + "\n");
const manyValues = copy(empty);
manyValues.projection.extra = Array(1_048_576).fill(null);
rejected("JSON value limit", JSON.stringify(manyValues) + "\n");

// The byte boundary includes the final LF, and input is consumed through EOF.
{
  const frame = valid + " ".repeat(maximumBytes - Buffer.byteLength(valid) - 1) + "\n";
  NodeAssert.deepEqual(succeeded(frame), { version: 1, events: [] });
}
rejected("input byte limit", Buffer.alloc(maximumBytes + 1, 0x20), "input limit exceeded");

// Duplicate node IDs can amplify output even when the request itself is small.
// Each completed root/item pair must stay buffered until the whole plan succeeds.
{
  const amplified = request("roots", {
    projection: {
      nodes: Array.from({ length: 129 }, () => node("root:one")),
      turnItems: [
        item("item:large", "reasoning", "root:one", {
          text: '\u0000🚀"\\\n'.repeat(65_536),
        }),
      ],
      providerTurns: [],
    },
  });
  rejected(
    "escaped UTF-8 output expansion",
    JSON.stringify(amplified) + "\n",
    "output limit exceeded",
  );
}
{
  const minimal = {
    id: "root:one",
    kind: "root_turn",
    runId: null,
    status: "running",
    completedAt: null,
  };
  const boundary = request("roots", {
    projection: {
      nodes: Array.from({ length: maximumEvents }, () => ({ ...minimal })),
      turnItems: [],
      providerTurns: [],
    },
  });
  const response = succeeded(JSON.stringify(boundary) + "\n");
  NodeAssert.equal(response.events.length, maximumEvents);
  NodeAssert.equal(response.events[0].payload.status, "cancelled");
  NodeAssert.deepEqual(response.events.at(-1), response.events[0]);
  boundary.projection.nodes.push(minimal);
  rejected(
    "event count limit after earlier valid records",
    JSON.stringify(boundary) + "\n",
    "event limit exceeded",
  );
}

console.log(
  `${checks} recovery executable checks passed, including ${comparisons} complete ordered batch comparisons with immutable upstream functions, full payload/timestamp preservation, strict framing, duplicate keys, and input/output/event bounds. No failure emitted partial stdout.`,
);
