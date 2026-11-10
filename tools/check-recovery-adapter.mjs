#!/usr/bin/env node
import * as NodeAssert from "node:assert/strict";
import * as NodeCrypto from "node:crypto";
import * as NodeFSP from "node:fs/promises";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";
import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  CommandId,
  EventId,
  NodeId,
  OrchestrationV2DomainEventJson,
  OrchestrationV2ThreadProjectionJson,
  ProviderDriverKind,
  ProviderInstanceId,
  RunId,
  ThreadId,
} from "@t3tools/contracts";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as Cause from "effect/Cause";
import * as Clock from "effect/Clock";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as References from "effect/References";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/sql/SqlClient";
import * as SqlitePersistence from "../apps/server/src/persistence/Sqlite.ts";
import * as ServerSettings from "../apps/server/src/serverSettings.ts";
import * as ProcessRunner from "../apps/server/src/processRunner.ts";
import * as EffectOutbox from "../apps/server/src/orchestration-v2/EffectOutbox.ts";
import * as EventSink from "../apps/server/src/orchestration-v2/EventSink.ts";
import * as EventStore from "../apps/server/src/orchestration-v2/EventStore.ts";
import * as ProjectionStore from "../apps/server/src/orchestration-v2/ProjectionStore.ts";
import * as Recovery from "../apps/server/src/orchestration-v2/ProviderRuntimeRecoveryService.ts";
import * as Planner from "../apps/server/src/orchestration-v2/ProviderRecoveryPlanner.ts";
import * as Orchestrator from "../apps/server/src/orchestration-v2/Orchestrator.ts";
import * as ProviderAdapterRegistry from "../apps/server/src/orchestration-v2/ProviderAdapterRegistry.ts";
import * as ProviderReplayHarness from "../apps/server/src/orchestration-v2/testkit/ProviderReplayHarness.ts";
import * as ProjectionReplay from "../apps/server/src/orchestration-v2/testkit/ProjectionReplay.ts";

NodeAssert.ok(process.argv[2], "Pass the native recovery executable");
const executable = NodePath.resolve(process.argv[2]);
const proofPath = process.argv[3];
const baselineOnly = process.argv.includes("--baseline-only");
const root = NodeURL.fileURLToPath(new URL("../", import.meta.url));
const temporary = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "t3ls-recovery-adapter-"));
const scenarios = [];
const providerInstanceId = ProviderInstanceId.make("codex");
const driver = ProviderDriverKind.make("codex");
const nowMillis = Date.parse("2026-11-08T12:00:00.123Z");
const now = DateTime.makeUnsafe(nowMillis);
const started = DateTime.makeUnsafe(nowMillis - 12_345);
const encodeEvent = Schema.encodeSync(OrchestrationV2DomainEventJson);
const encodeProjection = Schema.encodeSync(OrchestrationV2ThreadProjectionJson);
const threadIds = [ThreadId.make("thread:recovery:a"), ThreadId.make("thread:recovery:b")];
const toolInput = {
  now: 17,
  updatedAt: "user data, not a timestamp",
  nested: { id: "payload-id", completedAt: -7, values: [null, false, 1.25, "💬 é العربية"] },
};

function pullRequestFixtures(at) {
  const approvals = [
    {
      awaitingApproval: ["https://example.test/actions/runs/42", "security-scan"],
      approvalAnnounced: true,
    },
    { awaitingApproval: [], approvalAnnounced: false },
    {},
  ];
  const watch = {
    startedAt: at,
    headSha: "fixture-head",
    failedChecks: [],
    passed: false,
    passedChecks: [],
    remarksThrough: at,
    remarkIds: [],
    conflicting: false,
    wakes: 0,
  };
  return approvals.map((approval, index) => ({
    host: "example.test",
    repository: "fixture/project",
    number: index + 1,
    url: `https://example.test/fixture/project/pull/${index + 1}`,
    source: "manual",
    linkedAt: at,
    snapshot: null,
    stack: null,
    watch: { ...watch, ...approval },
    endedWatch: { ...watch, ...approvals[(index + 1) % approvals.length] },
  }));
}

function seedEvents(threadId) {
  const rootNodeId = NodeId.make(`node:${threadId}:root`);
  const subagentId = NodeId.make(`node:${threadId}:subagent`);
  const providerThreadId = `provider-thread:${threadId}`;
  const providerTurnId = `provider-turn:${threadId}`;
  const event = (type, payload, scope = {}) => ({
    id: EventId.make(`event:seed:${type}:${payload.id}`),
    type,
    threadId,
    occurredAt: started,
    ...scope,
    payload,
  });
  const node = (id, kind) => ({
    id,
    threadId,
    runId: null,
    parentNodeId: kind === "root_turn" ? null : rootNodeId,
    rootNodeId,
    kind,
    status: "running",
    countsForRun: false,
    providerThreadId,
    providerTurnId,
    nativeItemRef: null,
    runtimeRequestId: null,
    checkpointScopeId: null,
    startedAt: started,
    completedAt: null,
  });
  const item = (type, ordinal, nodeId = rootNodeId) => ({
    id: `item:${threadId}:${type}`,
    type,
    threadId,
    runId: null,
    nodeId,
    providerThreadId,
    providerTurnId,
    nativeItemRef: null,
    parentItemId: null,
    ordinal,
    status: "running",
    title: type,
    startedAt: started,
    completedAt: null,
    updatedAt: started,
  });
  const subagent = {
    id: subagentId,
    threadId,
    runId: null,
    parentNodeId: rootNodeId,
    origin: "provider_native",
    createdBy: "agent",
    driver,
    providerInstanceId,
    providerThreadId,
    childThreadId: null,
    nativeTaskRef: { driver, nativeId: "provider:subagent", strength: "strong" },
    prompt: "inspect recovery 💬",
    title: "inspection",
    model: null,
    status: "running",
    result: "existing result é",
    startedAt: started,
    completedAt: null,
    updatedAt: started,
  };
  return [
    event("thread.created", {
      id: threadId,
      createdBy: "user",
      creationSource: "web",
      projectId: "project:recovery",
      title: threadId,
      providerInstanceId,
      modelSelection: { instanceId: providerInstanceId, model: "gpt-5.4" },
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: null,
      worktreePath: null,
      activeProviderThreadId: null,
      lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
      forkedFrom: null,
      pullRequests: pullRequestFixtures(DateTime.formatIso(started)),
      createdAt: started,
      updatedAt: started,
      archivedAt: null,
      settledOverride: null,
      settledAt: null,
      lastVisitedAt: null,
      deletedAt: null,
    }),
    event("node.updated", node(rootNodeId, "root_turn"), { nodeId: rootNodeId }),
    event("node.updated", node(subagentId, "subagent"), { nodeId: subagentId }),
    event("subagent.updated", subagent, { nodeId: subagentId }),
    event(
      "turn-item.updated",
      {
        ...item("subagent", 1, subagentId),
        subagentId,
        origin: "provider_native",
        driver,
        providerInstanceId,
        childThreadId: null,
        prompt: subagent.prompt,
        result: subagent.result,
      },
      { nodeId: subagentId },
    ),
    event(
      "turn-item.updated",
      {
        ...item("reasoning", 2),
        text: "still streaming 💬",
        streaming: true,
      },
      { nodeId: rootNodeId },
    ),
    event(
      "turn-item.updated",
      {
        ...item("dynamic_tool", 3),
        toolName: "fixture",
        input: toolInput,
        output: { id: "output-id", startedAt: 23, unchanged: true },
      },
      { nodeId: rootNodeId },
    ),
    event(
      "turn-item.updated",
      {
        ...item("command_execution", 4),
        input: "fixture --no-network",
        output: "preserved output",
      },
      { nodeId: rootNodeId },
    ),
    event(
      "turn-item.updated",
      {
        ...item("secret_request", 5),
        label: "Service API key",
        reason: "Authenticate the configured service",
        url: "https://example.test/settings/api-keys?source=t3#new",
        secretStatus: "pending",
      },
      { nodeId: rootNodeId },
    ),
    event(
      "provider-turn.updated",
      {
        id: providerTurnId,
        providerThreadId,
        nodeId: rootNodeId,
        runAttemptId: null,
        nativeTurnRef: null,
        ordinal: 1,
        status: "running",
        startedAt: started,
        completedAt: null,
        model: "provider-executed-model",
      },
      { nodeId: rootNodeId },
    ),
  ];
}

function databaseLayer(filename) {
  const database =
    filename === undefined
      ? SqlitePersistence.layerMemory
      : SqlitePersistence.layerFromPath(filename);
  const stores = Layer.mergeAll(
    EventStore.layer,
    ProjectionStore.layer,
    EffectOutbox.layer,
    IdAllocator.layer,
  ).pipe(Layer.provideMerge(database));
  return Layer.mergeAll(
    stores,
    EventSink.layer.pipe(Layer.provide(stores)),
    ProjectionReplay.layer.pipe(Layer.provide(stores)),
    ServerSettings.layerTest({ continueThreadsAfterServerUpdate: false }),
  );
}

async function run(effect, selection, filename, layerOverride) {
  const clock = Effect.runSync(Clock.Clock);
  return Effect.runPromise(
    effect.pipe(
      Effect.scoped,
      Effect.provideService(
        HostProcess.Environment,
        selection === undefined
          ? { T3_CODE_LS_IMPLEMENTATION: "upstream" }
          : { T3_CODE_LS_RECOVERY_EXECUTABLE: selection },
      ),
      Effect.provideService(HostProcess.WorkingDirectory, temporary),
      Effect.provide(layerOverride ?? databaseLayer(filename)),
      Effect.provide(NodeServices.layer),
      Effect.provideService(
        Clock.Clock,
        Object.assign(Object.create(clock), {
          currentTimeMillis: Effect.succeed(nowMillis),
          currentTimeMillisUnsafe: () => nowMillis,
        }),
      ),
      Effect.provideService(References.MinimumLogLevel, "Warn"),
      Effect.timeout("60 seconds"),
    ),
  );
}

const seed = Effect.fn("checkRecoveryAdapter.seed")(function* (ids = threadIds) {
  const sink = yield* EventSink.EventSinkV2;
  const outbox = yield* EffectOutbox.EffectOutboxV2;
  yield* sink.write({ events: ids.flatMap(seedEvents) });
  yield* outbox.enqueue(
    ids.map((threadId) => ({
      id: `effect:${threadId}`,
      commandId: CommandId.make(`command:seed:${threadId}`),
      threadId,
      request: { type: "provider-turn.start", runId: RunId.make(`run:${threadId}:lost`) },
    })),
  );
});

const snapshot = Effect.fn("checkRecoveryAdapter.snapshot")(function* (ids = threadIds) {
  const sql = yield* SqlClient.SqlClient;
  const store = yield* EventStore.EventStoreV2;
  const projections = yield* ProjectionStore.ProjectionStoreV2;
  const events = yield* store.read().pipe(Stream.runCollect);
  return {
    events: events.map((stored) => ({
      sequence: stored.sequence,
      event: encodeEvent(stored.event),
    })),
    projections: yield* Effect.forEach(ids, (threadId) =>
      projections.getThreadProjection(threadId).pipe(Effect.map(encodeProjection)),
    ),
    receipts: yield* sql`SELECT * FROM orchestration_command_receipts ORDER BY command_id`,
    outbox:
      yield* sql`SELECT effect_id, status FROM orchestration_v2_effect_outbox ORDER BY effect_id`,
  };
});

function comparable(state) {
  return {
    ...state,
    events: state.events.map(({ sequence, event }) => {
      const { id: _id, ...draft } = event;
      return { sequence, event: draft };
    }),
  };
}

function assertRecovered(state) {
  NodeAssert.equal(state.receipts.length, threadIds.length);
  NodeAssert.ok(state.outbox.every((row) => row.status === "cancelled"));
  for (const projection of state.projections) {
    NodeAssert.deepEqual(
      projection.thread.pullRequests,
      pullRequestFixtures(DateTime.formatIso(started)),
      "recovery must retain active and ended approval holds, including explicit false and absent fields",
    );
    NodeAssert.ok(projection.nodes.every((node) => node.status === "cancelled"));
    NodeAssert.equal(projection.subagents[0].status, "cancelled");
    NodeAssert.equal(projection.subagents[0].result, "existing result é");
    NodeAssert.equal(projection.providerTurns[0].status, "cancelled");
    NodeAssert.equal(projection.providerTurns[0].model, "provider-executed-model");
    NodeAssert.ok(projection.turnItems.every((item) => item.status === "cancelled"));
    NodeAssert.equal(
      projection.turnItems.find((item) => item.type === "secret_request").url,
      "https://example.test/settings/api-keys?source=t3#new",
    );
    NodeAssert.equal(
      projection.turnItems.find((item) => item.type === "reasoning").streaming,
      false,
    );
    NodeAssert.deepEqual(
      projection.turnItems.find((item) => item.type === "dynamic_tool").input,
      toolInput,
    );
    for (const node of projection.nodes) {
      NodeAssert.equal(node.completedAt, DateTime.formatIso(now));
      NodeAssert.equal(node.startedAt, DateTime.formatIso(started));
    }
  }
}

const reconcile = Effect.fn("checkRecoveryAdapter.reconcile")(function* (trigger) {
  const recovery = yield* Recovery.make;
  return yield* recovery.reconcile(trigger);
});

async function success(selection, trigger, name) {
  const filename = NodePath.join(temporary, `${name}.sqlite`);
  const first = await run(
    Effect.gen(function* () {
      yield* seed();
      yield* reconcile(trigger);
      const state = yield* snapshot();
      assertRecovered(state);
      return state;
    }),
    selection,
    filename,
  );
  // Close and reopen the real SQLite file before recovery and replay checks.
  await run(
    Effect.gen(function* () {
      NodeAssert.deepEqual(yield* snapshot(), first);
      yield* reconcile(trigger);
      NodeAssert.deepEqual(
        yield* snapshot(),
        first,
        "repeat recovery must not allocate durable events",
      );
      const replay = yield* ProjectionReplay.ProjectionReplay;
      NodeAssert.equal((yield* replay.rebuild).valid, true);
      NodeAssert.deepEqual(
        yield* snapshot(),
        first,
        "projections must equal replay of the durable event log",
      );
    }),
    selection,
    filename,
  );
  scenarios.push(
    `${name}: durable commit, reopen, replay and idempotence preserve pull-request approval state`,
  );
  return comparable(first);
}

async function fixture(name, body) {
  const path = NodePath.join(temporary, name);
  await NodeFSP.writeFile(path, `#!${process.execPath}\n${body}\n`, { mode: 0o700 });
  return path;
}

async function mutatedResponse(name, mutation) {
  return fixture(
    name,
    `
import * as fs from "node:fs";
import * as child from "node:child_process";
const result = child.spawnSync(${JSON.stringify(executable)}, process.argv.slice(2), {
  input: fs.readFileSync(0), env: {}, maxBuffer: 128 * 1024 * 1024,
});
if (result.error) throw result.error;
if (result.status !== 0) throw new Error("The real native planner failed before mutation");
const response = JSON.parse(result.stdout.toString("utf8"));
${mutation}
process.stdout.write(JSON.stringify(response) + "\\n");`,
  );
}

const directProjection = () => {
  const events = seedEvents(threadIds[0]);
  const records = (type) =>
    events
      .filter((event) => event.type === type)
      .map((event) => ({
        ...event.payload,
        future: {
          nested: { id: "future-record-id", updatedAt: "opaque", now: 29, completedAt: null },
          values: [false, "💬", 1.5],
        },
      }));
  return {
    nodes: records("node.updated"),
    subagents: records("subagent.updated"),
    turnItems: records("turn-item.updated"),
    providerTurns: records("provider-turn.updated"),
  };
};

async function directPlan(selection, method, input, allocation = undefined) {
  let count = 0;
  const events = await run(
    Effect.gen(function* () {
      const planner = yield* Planner.make();
      return yield* planner[method]({
        ...input,
        allocateEventId: () => {
          count++;
          return allocation === undefined
            ? Effect.succeed(EventId.make(`event:callback:${count}`))
            : allocation(count);
        },
      });
    }),
    selection,
    undefined,
    Layer.empty,
  );
  for (const event of events) {
    NodeAssert.ok(DateTime.isDateTime(event.occurredAt));
    NodeAssert.ok(DateTime.isDateTime(event.payload.completedAt));
  }
  return { count, events: JSON.parse(JSON.stringify(events)) };
}

async function liveSettle(selection, stoppedSubagent) {
  const database = SqlitePersistence.layerMemory;
  const layer = Layer.mergeAll(
    ProviderReplayHarness.layerWithRegistry(
      { name: "lust-recovery-live-settle" },
      ProviderAdapterRegistry.layerFromAdapters([]),
      { databaseLayer: database, runEffectWorker: false },
    ),
    Layer.mergeAll(EventStore.layer, ProjectionStore.layer).pipe(Layer.provideMerge(database)),
  );
  return run(
    Effect.gen(function* () {
      const sink = yield* EventSink.EventSinkV2;
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const [parent, child] = threadIds;
      const runId = `run:${parent}:settled`;
      const attemptId = `attempt:${parent}:settled`;
      const parentEvents = seedEvents(parent).map((event) => {
        let payload = event.payload;
        if (
          event.type === "subagent.updated" ||
          (event.type === "turn-item.updated" && payload.type === "subagent")
        )
          payload = { ...payload, childThreadId: child };
        if (!stoppedSubagent) {
          if (["node.updated", "turn-item.updated", "subagent.updated"].includes(event.type))
            payload = { ...payload, runId };
          if (event.type === "node.updated" && payload.kind === "root_turn")
            payload = { ...payload, status: "completed", completedAt: started, countsForRun: true };
          if (event.type === "provider-turn.updated")
            payload = {
              ...payload,
              status: "completed",
              completedAt: started,
              runAttemptId: attemptId,
            };
        }
        return { ...event, payload };
      });
      const childEvents = seedEvents(child).filter(
        (event) =>
          event.type !== "subagent.updated" &&
          !(event.type === "node.updated" && event.payload.kind === "subagent") &&
          !(event.type === "turn-item.updated" && event.payload.type === "subagent"),
      );
      if (!stoppedSubagent) {
        parentEvents.push(
          {
            id: "event:live:run",
            type: "run.created",
            threadId: parent,
            occurredAt: started,
            payload: {
              id: runId,
              threadId: parent,
              ordinal: 1,
              providerInstanceId,
              modelSelection: { instanceId: providerInstanceId, model: "gpt-5.4" },
              providerThreadId: `provider-thread:${parent}`,
              userMessageId: "message:live:run",
              rootNodeId: `node:${parent}:root`,
              activeAttemptId: attemptId,
              status: "completed",
              requestedAt: started,
              startedAt: started,
              completedAt: started,
              checkpointId: null,
              contextHandoffId: null,
            },
          },
          {
            id: "event:live:attempt",
            type: "run-attempt.created",
            threadId: parent,
            occurredAt: started,
            payload: {
              id: attemptId,
              runId,
              attemptOrdinal: 1,
              rootNodeId: `node:${parent}:root`,
              providerInstanceId,
              providerThreadId: `provider-thread:${parent}`,
              providerTurnId: `provider-turn:${parent}`,
              reason: "initial",
              status: "completed",
              startedAt: started,
              completedAt: started,
            },
          },
        );
      }
      yield* sink.write({ events: [...parentEvents, ...childEvents] });
      const completedAt = DateTime.makeUnsafe(nowMillis - 2_000);
      const command = {
        type: "thread.background-work.settle",
        commandId: "command:live:settle",
        threadId: parent,
        providerThreadId: `provider-thread:${parent}`,
        providerTurnId: `provider-turn:${parent}`,
        ...(stoppedSubagent
          ? {
              subagent: {
                id: `node:${parent}:subagent`,
                stoppedAt: now,
                outcome: { status: "completed", completedAt, result: "native completion 💬" },
              },
            }
          : {}),
      };
      const result = yield* orchestrator.dispatch(command);
      NodeAssert.ok(result.storedEvents.length > 0);
      const state = yield* snapshot();
      const [parentProjection, childProjection] = state.projections;
      const status = stoppedSubagent ? "completed" : "interrupted";
      NodeAssert.equal(parentProjection.subagents[0].status, status);
      NodeAssert.equal(
        parentProjection.subagents[0].result,
        stoppedSubagent ? "native completion 💬" : "existing result é",
      );
      NodeAssert.equal(
        parentProjection.nodes.find((node) => node.kind === "subagent").status,
        status,
      );
      NodeAssert.equal(childProjection.nodes[0].status, status);
      NodeAssert.equal(childProjection.providerTurns[0].status, status);
      NodeAssert.equal(
        childProjection.turnItems.find((item) => item.type === "reasoning").streaming,
        false,
      );
      NodeAssert.equal(
        childProjection.turnItems.find((item) => item.type === "dynamic_tool").status,
        stoppedSubagent ? "failed" : "interrupted",
      );
      NodeAssert.equal(
        childProjection.nodes[0].completedAt,
        DateTime.formatIso(stoppedSubagent ? completedAt : now),
      );
      const repeated = yield* orchestrator.dispatch(command);
      NodeAssert.equal(repeated.replayed, true);
      NodeAssert.deepEqual(yield* snapshot(), state, "same live command must replay its receipt");
      return comparable(state);
    }),
    selection,
    undefined,
    layer,
  );
}

async function failWithoutWrites(selection, label, customize = (value) => value) {
  await run(
    Effect.gen(function* () {
      yield* seed();
      const before = yield* snapshot();
      const outcome = yield* customize(reconcile("startup")).pipe(Effect.exit);
      NodeAssert.ok(Exit.isFailure(outcome), `${label} must fail closed`);
      NodeAssert.deepEqual(yield* snapshot(), before, `${label} changed durable state`);
    }),
    selection,
  );
  scenarios.push(`${label}: zero durable writes`);
}

try {
  for (const trigger of ["startup", "shutdown"]) {
    const upstream = await success(undefined, trigger, `upstream-${trigger}`);
    if (!baselineOnly) {
      const native = await success(executable, trigger, `native-${trigger}`);
      NodeAssert.deepEqual(
        native,
        upstream,
        `${trigger} native recovery differs from upstream service`,
      );
      scenarios.push(`${trigger}: complete durable native/upstream parity`);
    }
  }

  for (const stoppedSubagent of [false, true]) {
    const label = stoppedSubagent ? "subagent completion" : "background Stop";
    const upstream = await liveSettle(undefined, stoppedSubagent);
    if (!baselineOnly) {
      NodeAssert.deepEqual(await liveSettle(executable, stoppedSubagent), upstream);
      scenarios.push(
        `actual Orchestrator ${label}: native/upstream event, projection and receipt parity`,
      );
    } else {
      scenarios.push(`actual Orchestrator ${label}: upstream baseline`);
    }
  }

  if (!baselineOnly) {
    const projection = directProjection();
    const before = JSON.stringify(projection);
    const orphan = {
      projection,
      subagentId: projection.subagents[0].id,
      status: "completed",
      completedAt: DateTime.makeUnsafe(nowMillis - 777),
      now,
      emitted: [],
    };
    const isolatedForwarder = await fixture(
      "isolated-native-forwarder",
      `
import * as fs from "node:fs";
import * as child from "node:child_process";
const input = fs.readFileSync(0);
if (Object.keys(process.env).length !== 0) {
  process.stderr.write("Recovery planner inherited ambient environment\\n");
  process.exit(78);
}
const result = child.spawnSync(${JSON.stringify(executable)}, process.argv.slice(2), {
  input, env: {}, maxBuffer: 128 * 1024 * 1024,
});
if (result.error) throw result.error;
process.stdout.write(result.stdout);
process.stderr.write(result.stderr);
process.exitCode = result.status ?? 1;`,
    );
    const ambientKey = "T3LS_RECOVERY_AMBIENT_PROOF";
    const previousAmbient = process.env[ambientKey];
    try {
      process.env[ambientKey] = "synthetic-recovery-environment-marker";
      // The actual runner's default merge reproduces the old adapter bug.
      const inherited = await run(
        Effect.gen(function* () {
          const runner = yield* ProcessRunner.make();
          return yield* runner.run({
            command: isolatedForwarder,
            args: [],
            env: {},
            stdin: "{}\n",
            timeout: "5 seconds",
          });
        }),
        undefined,
        undefined,
        Layer.empty,
      );
      NodeAssert.equal(inherited.code, 78, "default env merging must trip the isolation witness");
      // This must succeed, so the regression cannot pass as an expected native failure.
      NodeAssert.deepEqual(
        await directPlan(isolatedForwarder, "endOrphanedNativeSubagent", orphan),
        await directPlan(undefined, "endOrphanedNativeSubagent", orphan),
      );
      scenarios.push(
        "actual native success has empty environment; inherited-env regression witness fails",
      );
    } finally {
      if (previousAmbient === undefined) delete process.env[ambientKey];
      else process.env[ambientKey] = previousAmbient;
    }
    for (const [label, result] of [
      ["omitted", {}],
      ["null", { result: null }],
      ["empty", { result: "" }],
      ["text", { result: "new result 💬" }],
    ]) {
      const input = { ...orphan, ...result };
      const upstream = await directPlan(undefined, "endOrphanedNativeSubagent", input);
      const native = await directPlan(executable, "endOrphanedNativeSubagent", input);
      NodeAssert.deepEqual(native, upstream);
      NodeAssert.equal(native.count, 3);
      for (const event of native.events) {
        NodeAssert.deepEqual(event.payload.future, projection.subagents[0].future);
        NodeAssert.equal(event.payload.completedAt, DateTime.formatIso(orphan.completedAt));
      }
      NodeAssert.equal(
        native.events[0].payload.result,
        label === "omitted" ? projection.subagents[0].result : result.result,
      );
      scenarios.push(
        `actual orphan adapter ${label} result, nested extensions and distinct completion time`,
      );
    }
    const rootsInput = {
      projection,
      threadId: threadIds[0],
      providerInstanceId,
      status: "completed",
      now,
      emitted: [],
    };
    NodeAssert.deepEqual(
      await directPlan(executable, "endRunlessRootTurns", rootsInput),
      await directPlan(undefined, "endRunlessRootTurns", rootsInput),
    );
    NodeAssert.equal(JSON.stringify(projection), before, "adapter mutated source records");
    scenarios.push(
      "actual roots adapter preserves nested extensions and completed-tool failure semantics",
    );

    const lateInvalid = await mutatedResponse(
      "late-invalid-draft",
      `
if (!response.events.length) throw new Error("Missing real drafts");
response.events.push({ ...response.events[0], id: "native-event-id-is-forbidden" });`,
    );
    let invalidAllocations = 0;
    await NodeAssert.rejects(
      directPlan(lateInvalid, "endOrphanedNativeSubagent", orphan, () => {
        invalidAllocations++;
        return Effect.succeed(EventId.make("event:unexpected-allocation"));
      }),
    );
    NodeAssert.equal(invalidAllocations, 0, "all drafts must validate before any event allocation");
    scenarios.push("valid native drafts followed by invalid draft allocate no event IDs");

    const foreignAssociation = await mutatedResponse(
      "foreign-association",
      `
const event = response.events.find((event) => event.type === "provider-turn.updated");
if (!event) throw new Error("Missing provider-turn draft in native result");
event.payload.providerThreadId = "provider-thread:foreign";`,
    );
    await failWithoutWrites(foreignAssociation, "foreign provider-thread association");

    for (const [label, selection] of [
      ["empty selector", ""],
      ["relative selector", "relative-recovery"],
      ["NUL selector", `${executable}\0`],
      ["missing executable", NodePath.join(temporary, "absent")],
    ])
      await failWithoutWrites(selection, label);

    const invalid = await fixture(
      "invalid-response",
      'process.stdin.resume(); process.stdin.on("end", () => process.stdout.write("{invalid\\n"));',
    );
    const failed = await fixture(
      "failed-process",
      'process.stdin.resume(); process.stdin.on("end", () => { process.stderr.write("synthetic failure\\n"); process.exitCode = 7; });',
    );
    await failWithoutWrites(invalid, "malformed native response");
    await failWithoutWrites(failed, "native nonzero exit");

    let allocationCount = 0;
    await failWithoutWrites(executable, "allocator failure", (effect) =>
      Effect.gen(function* () {
        const allocator = yield* IdAllocator.IdAllocatorV2;
        return yield* effect.pipe(
          Effect.provideService(IdAllocator.IdAllocatorV2, {
            ...allocator,
            allocate: {
              ...allocator.allocate,
              event: (input) =>
                Effect.suspend(() =>
                  ++allocationCount === 2
                    ? Effect.fail(
                        new IdAllocator.IdAllocatorV2AllocationError({ kind: "event", input }),
                      )
                    : allocator.allocate.event(input),
                ),
            },
          }),
        );
      }),
    );
    NodeAssert.equal(allocationCount, 2, "allocation must stop at the first typed failure");

    const grouped = await fixture(
      "fail-second-thread",
      `
import * as fs from "node:fs";
import * as child from "node:child_process";
const input = fs.readFileSync(0);
const request = JSON.parse(input.toString("utf8"));
if (request.threadId === ${JSON.stringify(threadIds[1])}) process.stdout.write("{invalid\\n");
else {
  const result = child.spawnSync(${JSON.stringify(executable)}, process.argv.slice(2), {
    input, env: {}, maxBuffer: 128 * 1024 * 1024,
  });
  if (result.error) throw result.error;
  process.stdout.write(result.stdout);
  process.stderr.write(result.stderr);
  process.exitCode = result.status ?? 1;
}`,
    );
    await run(
      Effect.gen(function* () {
        yield* seed();
        const before = yield* snapshot();
        const sink = yield* EventSink.EventSinkV2;
        const sql = yield* SqlClient.SqlClient;
        let firstMemberWrote = false;
        const outcome = yield* reconcile("shutdown").pipe(
          Effect.provideService(EventSink.EventSinkV2, {
            ...sink,
            commitCommand: (input) =>
              sink.commitCommand(input).pipe(
                Effect.tap(() =>
                  Effect.gen(function* () {
                    if (input.threadId !== threadIds[0]) return;
                    NodeAssert.ok(
                      Option.isSome(yield* Effect.serviceOption(sql.transactionService)),
                    );
                    const rows =
                      yield* sql`SELECT command_id FROM orchestration_command_receipts WHERE aggregate_id = ${threadIds[0]}`;
                    NodeAssert.equal(
                      rows.length,
                      1,
                      "first group member must have written inside the transaction",
                    );
                    firstMemberWrote = true;
                  }),
                ),
              ),
          }),
          Effect.exit,
        );
        NodeAssert.ok(Exit.isFailure(outcome));
        NodeAssert.equal(firstMemberWrote, true, "failure must follow an earlier member write");
        NodeAssert.deepEqual(
          yield* snapshot(),
          before,
          "failed shutdown group must roll back earlier writes",
        );
        yield* reconcile("shutdown").pipe(
          Effect.provideService(HostProcess.Environment, {
            T3_CODE_LS_RECOVERY_EXECUTABLE: executable,
          }),
        );
        assertRecovered(yield* snapshot());
      }),
      grouped,
    );
    scenarios.push("shutdown native failure rolls back prior group member; retry commits");

    const readyPath = NodePath.join(temporary, "planner-ready.json");
    let readyResolve;
    let readyReject;
    const ready = new Promise((resolve, reject) => {
      readyResolve = resolve;
      readyReject = reject;
    });
    const watcher = NodeFS.watch(temporary, (_event, filename) => {
      if (filename === "planner-ready.json") {
        NodeFSP.readFile(readyPath, "utf8").then(
          (value) => readyResolve(Number(value)),
          readyReject,
        );
      }
    });
    watcher.on("error", readyReject);
    const hanging = await fixture(
      "hanging-planner",
      `
import * as fs from "node:fs";
fs.watch(${JSON.stringify(temporary)}, () => {});
process.stdin.resume();
process.stdin.on("end", () => {
  fs.writeFileSync(${JSON.stringify(readyPath + ".tmp")}, String(process.pid));
  fs.renameSync(${JSON.stringify(readyPath + ".tmp")}, ${JSON.stringify(readyPath)});
});`,
    );
    try {
      await run(
        Effect.gen(function* () {
          yield* seed();
          const before = yield* snapshot();
          const running = yield* reconcile("shutdown").pipe(Effect.forkScoped);
          const pid = yield* Effect.promise(() => ready);
          NodeAssert.ok(Number.isSafeInteger(pid) && pid > 0);
          yield* Fiber.interrupt(running);
          const outcome = yield* Fiber.await(running);
          NodeAssert.ok(Exit.isFailure(outcome) && Cause.hasInterruptsOnly(outcome.cause));
          NodeAssert.throws(
            () => process.kill(pid, 0),
            { code: "ESRCH" },
            "interruption must reap the planner",
          );
          NodeAssert.deepEqual(yield* snapshot(), before, "interruption must roll back recovery");
        }),
        hanging,
      );
      scenarios.push(
        "interruption after native input receipt reaps child and preserves durable state",
      );
    } finally {
      watcher.close();
    }
  }

  const sources = [
    "apps/server/src/orchestration-v2/ProviderRecoveryPlanner.ts",
    "apps/server/src/orchestration-v2/ProviderRuntimeRecoveryService.ts",
    "apps/server/src/orchestration-v2/Orchestrator.ts",
    "apps/server/src/processRunner.ts",
    "apps/server/src/orchestration-v2/EventSink.ts",
    "apps/server/src/orchestration-v2/EventStore.ts",
    "apps/server/src/orchestration-v2/ProjectionStore.ts",
    "apps/server/src/orchestration-v2/EffectOutbox.ts",
    "apps/server/src/orchestration-v2/CommandReceiptStore.ts",
    "apps/server/src/orchestration-v2/ProviderCommitBatch.ts",
    "apps/server/src/orchestration-v2/ProviderAdapterRegistry.ts",
    "apps/server/src/orchestration-v2/ThreadCommandExecutor.ts",
    "apps/server/src/orchestration-v2/TurnItemPositionStore.ts",
    "apps/server/src/orchestration-v2/testkit/ProjectionReplay.ts",
    "apps/server/src/orchestration-v2/testkit/ProviderReplayHarness.ts",
    "apps/server/src/persistence/OrchestrationEventStore.ts",
    "apps/server/src/persistence/Sqlite.ts",
    "apps/server/src/persistence/Migrations.ts",
    "packages/shared/src/nodeSqliteClient.ts",
    "packages/shared/src/HostProcess.ts",
    "packages/shared/src/shell.ts",
    "packages/provider-core/src/server/subagentProjection.ts",
    "packages/provider-core/src/server/IdAllocator.ts",
    "packages/provider-core/src/server/collectStreamText.ts",
    "packages/contracts/src/orchestrationV2.ts",
    "packages/contracts/src/threadPullRequest.ts",
    "packages/contracts/src/pullRequest.ts",
    "crates/core/src/provider_recovery.rs",
    "crates/core/src/provider_recovery_wire.rs",
    "crates/core/src/bin/t3-code-ls-recovery.rs",
    "tools/check-recovery-adapter.mjs",
    "tools/recovery-tsconfig.json",
    "tools/install-server-tests.mjs",
    "tools/server-test-runtime/package.json",
    "tools/server-test-runtime/package-lock.json",
    "upstream.lock.json",
    "pnpm-workspace.yaml",
    "tsconfig.base.json",
    "tsconfig.effect.json",
    ...[
      "mdast-util-math@3.0.0",
      "micromark-extension-llm-math@3.1.0",
      "micromark-extension-gfm-table@2.1.1",
      "micromark@4.0.2",
      "mdast-util-to-hast@13.2.1",
      "@muse-code__sdk@1.4.6",
    ].map((name) => `patches/${name}.patch`),
    ...(await NodeFSP.readdir(NodePath.join(root, "apps/server/src/persistence/Migrations")))
      .filter((name) => name.endsWith(".ts"))
      .sort()
      .map((name) => `apps/server/src/persistence/Migrations/${name}`),
  ];
  const sourceSha256 = Object.fromEntries(
    await Promise.all(
      sources.map(async (path) => [
        path,
        NodeCrypto.createHash("sha256")
          .update(await NodeFSP.readFile(NodePath.join(root, path)))
          .digest("hex"),
      ]),
    ),
  );
  const proof = {
    runtime: process.version,
    executable_sha256: baselineOnly
      ? null
      : NodeCrypto.createHash("sha256")
          .update(await NodeFSP.readFile(executable))
          .digest("hex"),
    source_sha256: sourceSha256,
    scenarios,
    count: scenarios.length,
    actual_sqlite: true,
    actual_recovery_service: true,
    actual_native_planner: !baselineOnly,
    note: "Provider recovery integration only; this does not establish complete application or mobile parity.",
  };
  if (proofPath !== undefined && proofPath !== "--baseline-only") {
    await NodeFSP.writeFile(proofPath, JSON.stringify(proof, null, 2) + "\n");
  }
  console.log(
    `Recovery adapter: ${scenarios.length} real service/persistence scenarios passed${baselineOnly ? " (upstream baseline only)" : ""}.`,
  );
} finally {
  await NodeFSP.rm(temporary, { recursive: true, force: true });
}
