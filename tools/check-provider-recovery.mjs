import * as NodeAssert from "node:assert/strict";
import * as NodeChildProcess from "node:child_process";
import * as NodeModule from "node:module";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import { upstreamSource } from "./upstream-source.mjs";

const source = upstreamSource("packages/provider-core/src/server/subagentProjection.ts");
const contracts = upstreamSource("packages/contracts/src/orchestrationV2.ts");
const plannerStart = source.indexOf("function writtenIds(");
const activeStart = contracts.indexOf("export function isOrchestrationV2WorkActive(");
const activeEnd = contracts.indexOf("\n}", activeStart);
NodeAssert.ok(
  plannerStart >= 0 && activeStart >= 0 && activeEnd > activeStart,
  "Review changed upstream recovery planner boundaries",
);

// Both complete planner bodies and their two helpers come from the immutable
// nightly. The actual Effect runtime executes their generators and failures.
const javascript = NodeModule.stripTypeScriptTypes(
  `${contracts.slice(activeStart, activeEnd + 2)}\n${source.slice(plannerStart)}`,
  { mode: "strip" },
).replace(/^export (?=(?:function|const) )/gm, "");
const upstream = new Function(
  "Effect",
  `${javascript}\nreturn {
  endOrphanedNativeSubagent, endRunlessRootTurns,
};`,
)(Effect);

const copy = (value) => JSON.parse(JSON.stringify(value));
const timeFields = new Set(["now", "occurredAt", "startedAt", "completedAt", "updatedAt"]);
function nativeTimes(value, key) {
  if (typeof value === "number" && timeFields.has(key)) return DateTime.makeUnsafe(value);
  if (Array.isArray(value)) return value.map((entry) => nativeTimes(entry));
  if (value !== null && typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value).map(([name, entry]) => [name, nativeTimes(entry, name)]),
    );
  }
  return value;
}
function wireTimes(value) {
  if (DateTime.isDateTime(value)) return DateTime.toEpochMillis(value);
  if (Array.isArray(value)) return value.map(wireTimes);
  if (value !== null && typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value).map(([name, entry]) => [name, wireTimes(entry)]),
    );
  }
  return value;
}

function expected(fixture) {
  const input = nativeTimes(copy(fixture));
  let allocations = 0;
  const allocateEventId = () =>
    Effect.suspend(() => {
      const attempt = allocations++;
      return fixture.failureAt === attempt
        ? Effect.fail({ tag: "AllocationFailure", attempt })
        : Effect.succeed(`event:recovery:${(fixture.allocationStart ?? 0) + attempt}`);
    });
  const plan =
    fixture.operation === "orphan"
      ? upstream.endOrphanedNativeSubagent
      : upstream.endRunlessRootTurns;
  const outcome = Effect.runSync(
    Effect.match(plan({ ...input, allocateEventId }), {
      onSuccess: (events) => ({ events }),
      onFailure: (error) => ({ error }),
    }),
  );
  NodeAssert.deepEqual(
    wireTimes(input),
    fixture,
    "Upstream mutated the projection or supplied events",
  );
  return { ...wireTimes(outcome), allocations };
}

const cases = [];
function add(label, fixture) {
  const input = copy(fixture);
  const result = expected(input);
  cases.push({ label, input, result });
  return result;
}

const now = Date.parse("2026-11-08T12:00:00.000Z");
const old = now - 10_000;
const extra = () => ({
  untouched: { list: [null, false, 1.25, "💬 e\u0301 العربية", { nested: "retain" }] },
  startedAt: old,
  completedAt: null,
});
function node(id = "subagent:target", overrides = {}) {
  return {
    ...extra(),
    id,
    threadId: "thread:source",
    runId: null,
    kind: "subagent",
    parentNodeId: null,
    rootNodeId: id,
    status: "running",
    countsForRun: false,
    nativeItemRef: { driver: "codex", kind: "task", id: "native:item" },
    updatedAt: old,
    ...overrides,
  };
}
function subagent(id = "subagent:target", overrides = {}) {
  return {
    ...extra(),
    id,
    threadId: "thread:source",
    runId: null,
    origin: "provider_native",
    driver: "codex",
    providerInstanceId: "provider:source",
    parentNodeId: "root:one",
    childThreadId: "thread:child",
    providerThreadId: "provider-thread:child",
    prompt: "Preserve the delegated task",
    result: "earlier result",
    title: "Investigate 🦀",
    status: "running",
    updatedAt: old,
    workflow: { phases: [{ index: 0, status: "running", title: "Inspect" }] },
    ...overrides,
  };
}
function item(id = "item:subagent", type = "subagent", overrides = {}) {
  return {
    ...extra(),
    id,
    type,
    nodeId: "root:one",
    threadId: "thread:source",
    runId: null,
    status: "running",
    updatedAt: old,
    ordinal: 7,
    title: "Keep this title",
    ...(type === "subagent"
      ? { subagentId: "subagent:target", result: "earlier item result" }
      : {}),
    ...(type === "assistant_message" || type === "reasoning"
      ? { text: "Partial streaming text\n", streaming: true }
      : {}),
    ...(type === "command_execution"
      ? { command: "fixture command", output: "partial output", exitCode: null }
      : {}),
    ...(type === "dynamic_tool"
      ? { toolName: "fixture-tool", arguments: { inspect: true }, result: null }
      : {}),
    ...overrides,
  };
}
function providerTurn(id = "provider-turn:one", overrides = {}) {
  return {
    ...extra(),
    id,
    nodeId: "root:one",
    providerThreadId: "provider-thread:root",
    runAttemptId: null,
    status: "running",
    ordinal: 1,
    updatedAt: old,
    tokenUsage: { inputTokens: 13, outputTokens: 21 },
    ...overrides,
  };
}
function projection(overrides = {}) {
  return { subagents: [], nodes: [], turnItems: [], providerTurns: [], ...overrides };
}
function orphanProjection(overrides = {}) {
  return projection({
    subagents: [subagent()],
    nodes: [node()],
    turnItems: [item()],
    ...overrides,
  });
}
function fixture(operation, state, overrides = {}) {
  return {
    operation,
    projection: state,
    subagentId: "subagent:target",
    threadId: "thread:context",
    providerInstanceId: "provider:context",
    now,
    status: "cancelled",
    emitted: [],
    ...overrides,
  };
}
const update = (type, id) => ({
  id: `earlier:${type}:${id}`,
  type,
  payload: { id, status: "cancelled" },
});
const terminalStatuses = ["completed", "failed", "interrupted", "cancelled"];
const statuses = ["idle", "pending", "running", "waiting", ...terminalStatuses];

// These guards are independent: a settled subagent does not leave its still
// running node or item open, and each record takes only the first matching ID.
for (const subagentStatus of statuses) {
  for (const nodeStatus of [...statuses, "rolled_back"]) {
    for (const itemStatus of statuses) {
      add(
        "independent status gates for all three orphan records",
        fixture(
          "orphan",
          orphanProjection({
            subagents: [subagent(undefined, { status: subagentStatus })],
            nodes: [node(undefined, { status: nodeStatus })],
            turnItems: [item(undefined, undefined, { status: itemStatus })],
          }),
        ),
      );
    }
  }
}
for (const status of terminalStatuses) {
  for (const result of [undefined, null, "", "Recovered result\n💬"]) {
    for (const runId of [null, "run:belongs-to-subagent"]) {
      for (const completedAt of [undefined, 0, old]) {
        add(
          "terminal result override, completion time, and native run scope",
          fixture(
            "orphan",
            orphanProjection({
              subagents: [subagent(undefined, { runId })],
              nodes: [node(undefined, { runId: "run:different-node" })],
              turnItems: [item(undefined, undefined, { runId: "run:different-item" })],
            }),
            {
              status,
              ...(result === undefined ? {} : { result }),
              ...(completedAt === undefined ? {} : { completedAt }),
            },
          ),
        );
      }
    }
  }
}

for (let mask = 0; mask < 8; mask++) {
  const emitted = [
    update("node.created", "subagent:target"),
    update("subagent.updated", "other:subagent"),
  ];
  if (mask & 1) emitted.push(update("subagent.updated", "subagent:target"));
  if (mask & 2) emitted.push(update("node.updated", "subagent:target"));
  if (mask & 4) emitted.push(update("turn-item.updated", "item:subagent"));
  for (const failureAt of [undefined, 0, 1, 2, 3]) {
    add(
      "prior events exclude only their own record; allocation fails at the next actual write",
      fixture("orphan", orphanProjection(), {
        emitted,
        ...(failureAt === undefined ? {} : { failureAt }),
      }),
    );
  }
}
for (const origin of ["provider_native", "app_owned"]) {
  for (let mask = 0; mask < 4; mask++) {
    add(
      "native origin and missing related records",
      fixture(
        "orphan",
        orphanProjection({
          subagents: [subagent(undefined, { origin })],
          nodes: mask & 1 ? [] : [node()],
          turnItems: mask & 2 ? [] : [item()],
        }),
      ),
    );
  }
}
add(
  "missing target never allocates",
  fixture(
    "orphan",
    orphanProjection({
      subagents: [subagent("other:subagent")],
    }),
    { failureAt: 0 },
  ),
);
for (const first of [
  subagent(undefined, { origin: "app_owned" }),
  subagent(undefined, { status: "completed" }),
]) {
  add(
    "first matching subagent wins over a later duplicate",
    fixture("orphan", orphanProjection({ subagents: [first, subagent()] })),
  );
}
add(
  "first matching related records win over later active duplicates",
  fixture(
    "orphan",
    orphanProjection({
      nodes: [node(undefined, { status: "completed" }), node()],
      turnItems: [
        item("not-a-subagent", "dynamic_tool", { subagentId: "subagent:target" }),
        item("first", "subagent", { status: "idle" }),
        item("second"),
      ],
    }),
  ),
);
add(
  "event scope and driver come from the subagent, independent of the caller's context",
  fixture(
    "orphan",
    orphanProjection({
      subagents: [
        subagent(undefined, {
          driver: "claude",
          threadId: "thread:elsewhere",
          providerInstanceId: "provider:elsewhere",
        }),
      ],
    }),
  ),
);

const itemKinds = [
  "subagent",
  "reasoning",
  "assistant_message",
  "command_execution",
  "dynamic_tool",
  "proposed_plan",
  "file_change",
  "user_message",
];
for (const status of terminalStatuses) {
  add(
    "only active runless items close; unfinished tools fail on nominal completion",
    fixture(
      "roots",
      projection({
        nodes: [node("root:one", { kind: "root_turn" })],
        turnItems: itemKinds.flatMap((kind) =>
          statuses.map((workStatus) =>
            item(`${kind}:${workStatus}`, kind, {
              status: workStatus,
              ...(kind === "proposed_plan"
                ? { streaming: true, markdown: "Keep plan stream marker" }
                : {}),
            }),
          ),
        ),
        providerTurns: ["pending", "running", ...terminalStatuses].map((workStatus) =>
          providerTurn(`provider-turn:${workStatus}`, { status: workStatus }),
        ),
      }),
      { status },
    ),
  );
}

// New nightly contract fields must survive recovery's whole-record updates.
// Missing model stays missing; a provider-reported model must not be rewritten.
for (const model of [undefined, "requested-model", "provider-executed-model"]) {
  for (const status of terminalStatuses) {
    const result = add(
      "recovery preserves provider-reported models and secret setup URLs",
      fixture(
        "roots",
        projection({
          nodes: [node("root:one", { kind: "root_turn" })],
          turnItems: [
            item("item:secret", "secret_request", {
              label: "Service API key",
              reason: "Authenticate the configured service",
              url: "https://example.test/settings/api-keys?source=t3#new",
              secretStatus: "pending",
            }),
          ],
          providerTurns: [providerTurn(undefined, model === undefined ? {} : { model })],
        }),
        { status },
      ),
    );
    const turn = result.events.find((event) => event.type === "provider-turn.updated").payload;
    NodeAssert.equal(Object.hasOwn(turn, "model"), model !== undefined);
    NodeAssert.equal(turn.model, model);
    NodeAssert.equal(
      result.events.find((event) => event.type === "turn-item.updated").payload.url,
      "https://example.test/settings/api-keys?source=t3#new",
    );
  }
}

const nodeKinds = [
  "root_turn",
  "assistant_message",
  "reasoning",
  "plan",
  "todo_list",
  "tool_call",
  "approval_request",
  "user_input_request",
  "subagent",
  "hook",
  "system",
];
for (const kind of nodeKinds) {
  for (const status of [...statuses, "rolled_back"]) {
    for (const runId of [null, "run:owned"]) {
      add(
        "only active runless root nodes qualify",
        fixture(
          "roots",
          projection({
            nodes: [node("root:one", { kind, status, runId })],
            turnItems: [item("waiting-message", "assistant_message")],
            providerTurns: [providerTurn()],
          }),
        ),
      );
    }
  }
}

function rootsProjection() {
  return projection({
    nodes: [
      node("root:second", { kind: "root_turn" }),
      node("root:one", { kind: "root_turn", status: "waiting" }),
      node("root:owned", { kind: "root_turn", runId: "run:owned" }),
    ],
    turnItems: [
      item("second:text", "assistant_message", { nodeId: "root:second", streaming: false }),
      item("one:tool", "dynamic_tool"),
      item("one:reasoning", "reasoning"),
      item("foreign:run", "command_execution", { runId: "run:foreign" }),
      item("foreign:node", "command_execution", { nodeId: "root:foreign" }),
      item("no:node", "command_execution", { nodeId: null }),
    ],
    providerTurns: [
      providerTurn("second:turn", { nodeId: "root:second" }),
      providerTurn("one:turn"),
      providerTurn("foreign:attempt", { runAttemptId: "attempt:foreign" }),
      providerTurn("foreign:node", { nodeId: "root:foreign" }),
    ],
  });
}
for (let mask = 0; mask < 8; mask++) {
  const emitted = [
    update("turn-item.created", "one:tool"),
    update("node.updated", "one:reasoning"),
    update("provider-turn.updated", "other:turn"),
  ];
  if (mask & 1) emitted.push(update("node.updated", "root:one"));
  if (mask & 2) emitted.push(update("turn-item.updated", "one:tool"));
  if (mask & 4) emitted.push(update("provider-turn.updated", "one:turn"));
  add(
    "root, item and provider-turn exclusions are independent and ID based",
    fixture("roots", rootsProjection(), { emitted, status: "completed" }),
  );
}
const rootBatch = add(
  "root groups preserve node then item then provider-turn projection order",
  fixture("roots", rootsProjection()),
);
for (let failureAt = 0; failureAt <= rootBatch.allocations; failureAt++) {
  add(
    "allocator failure aborts the complete root batch at the exact write",
    fixture("roots", rootsProjection(), { failureAt }),
  );
}
add(
  "duplicate root and item IDs are not newly inserted into exclusion sets",
  fixture(
    "roots",
    projection({
      nodes: [node("root:one", { kind: "root_turn" }), node("root:one", { kind: "root_turn" })],
      turnItems: [item("duplicate", "reasoning"), item("duplicate", "dynamic_tool")],
      providerTurns: [providerTurn(), providerTurn(undefined, { ordinal: 2 })],
    }),
  ),
);

for (const at of [-8_640_000_000_000_000, -1, 0, 1, 8_640_000_000_000_000]) {
  for (const operation of ["orphan", "roots"]) {
    const state = operation === "orphan" ? orphanProjection() : rootsProjection();
    add(
      "opaque UTC timestamps retain the full DateTime range",
      fixture(operation, state, { now: at, completedAt: at }),
    );
  }
}

function applyEvents(state, events) {
  const next = copy(state);
  const collections = {
    "subagent.updated": "subagents",
    "node.updated": "nodes",
    "turn-item.updated": "turnItems",
    "provider-turn.updated": "providerTurns",
  };
  for (const event of events) {
    const key = collections[event.type];
    next[key] = next[key].map((record) =>
      record.id === event.payload.id ? copy(event.payload) : record,
    );
  }
  return next;
}

// Follow the recovery service's composition: native subagents settle first,
// then root turns skip item updates already included in the same event batch.
let state = orphanProjection({
  nodes: [node(), node("root:one", { kind: "root_turn" })],
  turnItems: [item(), item("root:reasoning", "reasoning"), item("root:tool", "command_execution")],
  providerTurns: [providerTurn()],
});
const first = add(
  "restart settles the native child before its parent root",
  fixture("orphan", state),
);
const second = add(
  "parent recovery does not rewrite the child item in the same batch",
  fixture("roots", state, {
    emitted: first.events,
    allocationStart: first.allocations,
  }),
);
const emitted = [...first.events, ...second.events];
NodeAssert.equal(new Set(emitted.map((event) => event.id)).size, emitted.length);
add(
  "retry against the same snapshot with emitted batch allocates nothing",
  fixture("orphan", state, { emitted }),
);
add(
  "root retry against the same snapshot with emitted batch allocates nothing",
  fixture("roots", state, { emitted }),
);
state = applyEvents(state, emitted);
add("replayed recovered subagent projection allocates nothing", fixture("orphan", state));
add("replayed recovered root projection allocates nothing", fixture("roots", state));

const executable = process.argv[2];
NodeAssert.ok(executable, "Pass the native provider recovery matrix executable");
const result = NodeChildProcess.spawnSync(executable, [], {
  input: cases.map(({ input }) => JSON.stringify(input)).join("\n") + "\n",
  encoding: "utf8",
  maxBuffer: 64 * 1024 * 1024,
});
NodeAssert.ifError(result.error);
NodeAssert.equal(result.status, 0, `Recovery matrix failed:\n${result.stderr}`);
const actual = result.stdout.replaceAll("\r\n", "\n").trimEnd().split("\n");
NodeAssert.equal(
  actual.length,
  cases.length,
  "Recovery matrix emitted a different number of cases",
);
for (let index = 0; index < cases.length; index++) {
  NodeAssert.deepEqual(
    JSON.parse(actual[index]),
    cases[index].result,
    `Recovery case ${index + 1}: ${cases[index].label}`,
  );
}
console.log(
  `${cases.length} recovery planning cases match the pinned upstream functions using the real Effect runtime: complete ordered events, preserved payloads, independent exclusions, UTC timestamps, allocation failures, and replayed recovery lifecycles.`,
);
