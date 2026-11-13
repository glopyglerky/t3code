#!/usr/bin/env node
import * as NodeAssert from "node:assert/strict";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";

import * as ProjectionStore from "../apps/server/src/orchestration-v2/ProjectionStore.ts";
import * as NativeProjectionStore from "../apps/server/src/orchestration-v2/NativeProjectionStore.ts";
import { createFixture } from "./database-fixture.mjs";
import { exportSqliteState } from "./export-sqlite-state.mjs";
import {
  createMigratedSqlite,
  finishProof,
  hash,
  loadUpstreamModule,
  nativeRequest,
  normalized,
  withNative,
  withSqlite,
} from "./redb-domain-fixture.mjs";
import { jsonPayloads, sampleEvents, makeEvent, fixedTime } from "./redb-event-sink-fixtures.mjs";

const [executable, proofPath] = process.argv.slice(2);
NodeAssert.ok(
  executable && NodePath.isAbsolute(executable),
  "Pass an absolute native database executable",
);
await NodeFSP.access(executable, NodeFSP.constants.X_OK);
const oracle = await loadUpstreamModule("apps/server/src/orchestration-v2/ProjectionStore.ts");
const fixture = await createFixture();
const fields = [
  "runs",
  "attempts",
  "nodes",
  "subagents",
  "providerSessions",
  "providerThreads",
  "providerTurns",
  "runtimeRequests",
  "messages",
  "plans",
  "turnItems",
  "checkpointScopes",
  "checkpoints",
  "contextHandoffs",
  "contextTransfers",
];
const checks = [];
const failed = (body) =>
  Effect.exit(body).pipe(
    Effect.tap((exit) => Effect.sync(() => NodeAssert.ok(Exit.isFailure(exit)))),
  );
const ids = (records) => records.map((record) => record.id);

async function withServices(kind, filename, use) {
  if (kind === "redb")
    return withNative(executable, fixture, (client) =>
      Effect.gen(function* () {
        const store = yield* NativeProjectionStore.make;
        return yield* use({
          read: store.getThreadRecords,
          readMessages: store.getMessagesById,
          apply: store.apply,
          transaction: client.withTransaction,
        });
      }),
    );
  return withSqlite(filename, (sql) =>
    Effect.gen(function* () {
      const module = kind === "upstream" ? oracle.module : ProjectionStore;
      const store = yield* module.ProjectionStoreV2.pipe(Effect.provide(module.layer));
      return yield* use({
        read: store.getThreadRecords,
        readMessages: store.getMessagesById,
        apply: store.apply,
        transaction: (body) => sql.withTransaction(body),
      });
    }),
  );
}

const handoff = (id, extra = {}) =>
  makeEvent("context-handoff.updated", { ...jsonPayloads.contextHandoff, id, ...extra });
const transfer = (id, extra = {}) =>
  makeEvent("context-transfer.created", { ...jsonPayloads.contextTransfer, id, ...extra });
const turn = (id, ordinal, updatedAt, extra = {}) =>
  makeEvent("provider-turn.updated", {
    ...jsonPayloads.providerTurn,
    id,
    ordinal,
    nativeTurnRef: null,
    tokenUsage: { usedTokens: ordinal, updatedAt },
    ...extra,
  });

const extraEvents = [
  makeEvent("run.created", {
    ...jsonPayloads.run,
    id: "run:old",
    ordinal: 2,
    status: "completed",
    completedAt: fixedTime,
  }),
  makeEvent("run.created", { ...jsonPayloads.run, id: "run:current", ordinal: 3 }),
  makeEvent("run.created", {
    ...jsonPayloads.run,
    id: "run:cancelled",
    ordinal: 4,
    status: "cancelled",
    startedAt: null,
  }),
  makeEvent("run-attempt.created", {
    ...jsonPayloads.attempt,
    id: "attempt:old",
    runId: "run:old",
    attemptOrdinal: 2,
  }),
  makeEvent("node.updated", {
    ...jsonPayloads.node,
    id: "node:settled",
    runId: "run:old",
    kind: "reasoning",
    status: "completed",
  }),
  makeEvent("node.updated", {
    ...jsonPayloads.node,
    id: "node:active-terminal",
    kind: "reasoning",
    status: "completed",
  }),
  makeEvent("node.updated", {
    ...jsonPayloads.node,
    id: "node:without-run",
    runId: null,
    kind: "reasoning",
    status: "completed",
    startedAt: null,
  }),
  makeEvent("subagent.updated", {
    ...jsonPayloads.subagent,
    id: "subagent:settled",
    runId: "run:old",
    status: "completed",
    completionDelivery: { state: "delivered", observedByRunId: null },
  }),
  makeEvent("subagent.updated", {
    ...jsonPayloads.subagent,
    id: "subagent:active-terminal",
    status: "completed",
    completionDelivery: { state: "acknowledged", observedByRunId: null },
  }),
  makeEvent("provider-thread.updated", {
    ...jsonPayloads.providerThread,
    id: "provider:owned",
    appThreadId: null,
    nativeThreadRef: null,
    firstRunOrdinal: 2,
  }),
  makeEvent("provider-thread.updated", {
    ...jsonPayloads.providerThread,
    id: "provider:subagent",
    appThreadId: null,
    ownerNodeId: null,
    nativeThreadRef: null,
    firstRunOrdinal: null,
  }),
  makeEvent("subagent.updated", {
    ...jsonPayloads.subagent,
    id: "subagent:linked",
    providerThreadId: "provider:subagent",
  }),
  turn("turn:earlier", 2, "2026-11-10T11:59:59.999Z"),
  turn("turn:later", 3, "2026-11-10T05:00:00.000-07:00"),
  turn("turn:tie", 4, "2026-11-10T12:00:00.000Z"),
  turn("turn:invalid", 5, "not-a-date"),
  makeEvent("runtime-request.updated", {
    ...jsonPayloads.request,
    id: "request:resolved",
    status: "resolved",
    resolvedAt: fixedTime,
  }),
  makeEvent("message.updated", {
    ...jsonPayloads.message,
    id: "message:user",
    role: "user",
    createdBy: "user",
    creationSource: "web",
    createdAt: "2026-11-10T13:00:00.000Z",
    streaming: false,
  }),
  makeEvent("message.updated", {
    ...jsonPayloads.message,
    id: "message:assistant",
    createdAt: "2026-11-10T11:00:00.000Z",
  }),
  makeEvent("message.updated", {
    ...jsonPayloads.message,
    id: "message:monitor",
    runId: "run:old",
    notification: {
      source: { kind: "monitor" },
      outcome: "updated",
      summary: "Fixture monitor update",
    },
  }),
  makeEvent("turn-item.updated", {
    ...jsonPayloads.turnItem,
    id: "item:user",
    type: "user_message",
    messageId: "message:user",
    ordinal: 2,
    inputIntent: "turn_start",
    createdBy: "user",
    creationSource: "web",
    attachments: [],
  }),
  makeEvent("turn-item.updated", {
    ...jsonPayloads.turnItem,
    id: "item:assistant",
    type: "assistant_message",
    messageId: "message:assistant",
    ordinal: 20,
  }),
  makeEvent("turn-item.updated", {
    ...jsonPayloads.turnItem,
    id: "item:request",
    type: "user_input_request",
    requestId: "request:main",
    questions: [],
    ordinal: 21,
    status: "waiting",
  }),
  makeEvent("turn-item.updated", {
    ...jsonPayloads.turnItem,
    id: "item:unowned",
    runId: null,
    nodeId: null,
    ordinal: 22,
  }),
  handoff("handoff:z"),
  handoff("handoff:a"),
  transfer("transfer:z"),
  transfer("transfer:a", { status: "failed", resolution: null, error: "Fixture error" }),
];

const datePairs = [
  ["2026-11-10", "2026-11-10T00:00:00.000Z"],
  ["2026-11-10 12:00", "2026-11-10T12:00:00.000Z"],
  ["2026-11-10T12:00:00", "2026-11-10T12:00:00.000Z"],
  ["2026-11-10TT   12:00:00Z", "2026-11-10T12:00:00.000Z"],
  ["12:00", "2000-01-01T12:00:00.000Z"],
  ["12:00:01.9", "2000-01-01T12:00:01.900Z"],
  ["2451545", "2000-01-01T12:00:00.000Z"],
  [" +2451545.0 ", "2000-01-01T12:00:00.000Z"],
  ["2026-11-31", "2026-12-01T00:00:00.000Z"],
  ["2026-02-30", "2026-03-02T00:00:00.000Z"],
  ["2026-01-01 24:59:59", "2026-01-02T00:59:59.000Z"],
  ["2026-01-01 12:00:00.9995Z", "2026-01-01T12:00:00.999Z"],
  ["2026-01-01 12:00:00.0005Z", "2026-01-01T12:00:00.001Z"],
  ["2026-01-01 12:00 +01:00", "2026-01-01T11:00:00.000Z"],
  ["2026-01-01 12:00:00+14:59", "2025-12-31T21:01:00.000Z"],
  ["2026-01-01T", "2026-01-01T00:00:00.000Z"],
  ["2026-01-01 12:00:00.0z \t", "2026-01-01T12:00:00.000Z"],
  ["-0001-01-01", "1720694.5"],
  ["-4713-11-24T12:00:00Z", "0"],
  ["9999-12-31T23:59:59.999Z", "5373484.49999999"],
  ["10000-01-01", "2000-01-01T00:00:00Z"],
  [" 2026-01-01", "2000-01-01T00:00:00Z"],
  ["2026-01-01Z", "2000-01-01T00:00:00Z"],
  ["2026-01-01T12:00:00+15:00", "2000-01-01T00:00:00Z"],
  ["5373484.5", "2000-01-01T00:00:00Z"],
  ["NaN", "2000-01-01T00:00:00Z"],
  ["now", "9999-12-31T23:59:59.999Z"],
  ["subsecond", "9999-12-31T23:59:59.999Z"],
  ["1e3", "+1000.0"],
  [".1", "+1e-1"],
  ["2026-01-01T12:00:00Z\u0000ignored", "2026-01-01T12:00:00.000Z"],
  ["2026-01-01 24:00", "2026-01-02T00:00:00.000Z"],
  ["2026-01-01 24:00:00.0000+01:00", "2026-01-01T23:00:00.000Z"],
  ["2026-01-01 24:00:00.0001", "2026-01-02T00:00:00.000Z"],
  ["24:00", "2000-01-02T00:00:00.000Z"],
  ["24:00:00.001", "2000-01-02T00:00:00.001Z"],
];

const queries = [
  [fields, undefined],
  [[], undefined],
  [["messages", "messages"], undefined],
  [["runs"], { runIds: [] }],
  [["runs"], { runIds: ["run:old"] }],
  [["runs"], { runStatuses: [] }],
  [["runs"], { runStatuses: ["completed"], includeLatestRun: true }],
  [["runs"], { runStatuses: [], includeLatestRun: true }],
  [["runs"], { runIds: ["run:old"], runStatuses: [], includeLatestRun: true }],
  [["runs"], { includeLatestRun: true }],
  [["nodes"], { nodeIds: [] }],
  [["nodes"], { omitSettledLeafNodes: true }],
  [["subagents"], { subagentIds: ["subagent:settled"] }],
  [["subagents"], { subagentRunIds: ["run:old"] }],
  [["subagents"], { omitSettledSubagents: true }],
  [["subagents"], { omitSettledSubagents: { except: ["subagent:settled"] } }],
  [["subagents"], { omitSettledSubagents: { except: [] } }],
  [["providerThreads"], { providerThreadIds: [] }],
  [["providerThreads"], { providerThreadIds: ["provider:owned", "provider:subagent"] }],
  [["providerTurns"], { providerTurnAttemptIds: [] }],
  [["providerTurns"], { providerTurnAttemptIds: ["attempt:main"] }],
  [["providerTurns"], { providerTurnThreadIds: ["provider:missing"] }],
  [["providerTurns"], { latestProviderTurnUsageOnly: true }],
  [["providerTurns"], { latestProviderTurnUsageOnly: true, providerTurnAttemptIds: [] }],
  [["runtimeRequests"], { runtimeRequestStatuses: [] }],
  [["runtimeRequests"], { runtimeRequestStatuses: ["resolved"] }],
  [["messages"], { messageIds: [] }],
  [["messages"], { messageIds: ["message:assistant", "message:user"] }],
  [["messages"], { messageRoles: ["user"] }],
  [["messages"], { messageRunIds: ["run:old"] }],
  [["messages"], { monitorNotificationsOnly: true }],
  [["messages", "turnItems"], { turnItemTypes: ["reasoning"] }],
  [["turnItems"], { turnItemRequestIds: ["request:main"] }],
  [["turnItems"], { turnItemTypes: [] }],
  [["turnItems"], { turnItemStatuses: ["waiting"] }],
  [["turnItems"], { turnItemRunId: "run:missing" }],
  [["turnItems"], { turnItemRunIds: [] }],
  [["turnItems"], { turnItemRunIds: [null] }],
  [["turnItems"], { turnItemRunIds: ["run:main", null] }],
  [["turnItems"], { turnItemRunIds: ["run:main"], turnItemNodeIds: ["node:main"] }],
  [["contextTransfers"], { contextTransferStatuses: [] }],
  [["contextTransfers"], { contextTransferStatuses: ["failed"] }],
];

const exerciseNarrowRuns = Effect.fn("fixture.projectionRecords.narrowRuns")(function* ({
  read,
  readMessages,
  apply,
  transaction,
}) {
  const threadId = "thread:narrow-runs";
  const otherId = "thread:narrow-other";
  for (const id of [threadId, otherId])
    yield* apply(
      makeEvent(
        "thread.created",
        {
          ...jsonPayloads.thread,
          id,
          lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: id },
        },
        { threadId: id },
      ),
    );
  const payloads = new Map();
  for (const [name, owner, ordinal, status] of [
    ["completed", threadId, 1, "completed"],
    ["later", threadId, 3, "queued"],
    ["earlier", threadId, 2, "queued"],
    ["other", otherId, 1, "queued"],
    ["cancelled", threadId, 4, "cancelled"],
  ]) {
    const run = {
      ...jsonPayloads.run,
      id: `run:narrow:${name}`,
      threadId: owner,
      userMessageId: `message:narrow:${name}`,
      ordinal,
      status,
      startedAt: status === "completed" ? fixedTime : null,
    };
    payloads.set(name, run);
    yield* apply(makeEvent("run.created", run, { threadId: owner }));
    yield* apply(
      makeEvent(
        "message.updated",
        {
          ...jsonPayloads.message,
          id: run.userMessageId,
          threadId: owner,
          runId: run.id,
          role: "user",
          text: `Queue input ${name}`,
          streaming: false,
        },
        { threadId: owner },
      ),
    );
  }
  const id = (name) => `run:narrow:${name}`;
  const cases = [
    [{ runStatuses: ["queued"] }, ["earlier", "later"]],
    [{ runStatuses: ["queued", "queued"] }, ["earlier", "later"]],
    [
      { runIds: ["later", "other", "completed", "earlier", "later", "missing"].map(id) },
      ["completed", "earlier", "later"],
    ],
    [
      {
        runIds: ["later", "other", "completed", "earlier", "later"].map(id),
        runStatuses: ["queued"],
      },
      ["earlier", "later"],
    ],
    [{ runIds: [id("completed")], runStatuses: ["queued"] }, []],
    [{ runIds: [id("missing")] }, []],
    [{ runIds: [] }, []],
    [{ runStatuses: [] }, []],
    [{ runIds: [id("later")], runStatuses: [] }, []],
    [{ runStatuses: ["completed", "queued"] }, ["completed", "earlier", "later"]],
    [{ runStatuses: [], includeLatestRun: true }, ["later"]],
    [{ runIds: [id("earlier")], runStatuses: [], includeLatestRun: true }, []],
    [{ runIds: [], includeLatestRun: true }, []],
  ];
  const selections = [];
  for (const [filter, expected] of cases) {
    const projection = yield* read(threadId, ["runs"], filter);
    NodeAssert.deepEqual(ids(projection.runs), expected.map(id));
    selections.push(projection);
  }
  const queued = selections[0];
  const messages = yield* readMessages(threadId, [
    ...queued.runs.map((run) => run.userMessageId),
    queued.runs[0].userMessageId,
    "message:narrow:other",
    "message:narrow:missing",
  ]);
  NodeAssert.deepEqual([...messages.keys()], ["message:narrow:earlier", "message:narrow:later"]);
  NodeAssert.deepEqual([...(yield* readMessages(threadId, [])).keys()], []);
  const inside = yield* transaction(
    Effect.gen(function* () {
      yield* apply(
        makeEvent("run.updated", { ...payloads.get("later"), status: "running" }, { threadId }),
      );
      yield* failed(
        transaction(
          Effect.gen(function* () {
            yield* apply(
              makeEvent(
                "run.updated",
                { ...payloads.get("earlier"), status: "running" },
                { threadId },
              ),
            );
            return yield* Effect.fail("rollback queued membership");
          }),
        ),
      );
      const result = yield* read(threadId, ["runs"], { runStatuses: ["queued"] });
      NodeAssert.deepEqual(ids(result.runs), [id("earlier")]);
      return result;
    }),
  );
  const committed = yield* read(threadId, ["runs"], { runStatuses: ["queued"] });
  NodeAssert.deepEqual(normalized(committed), normalized(inside));
  return { selections, messages: [...messages.entries()], committed };
});

const exercise = Effect.fn("fixture.projectionRecords.exercise")(function ({
  read,
  readMessages,
  apply,
  transaction,
}) {
  return Effect.gen(function* () {
    const result = { queries: [] };
    for (const [selected, filter] of queries)
      result.queries.push(yield* read("thread:main", selected, filter));
    for (const field of fields) result.queries.push(yield* read("thread:main", [field]));
    NodeAssert.deepEqual(ids(result.queries[6].runs), ["run:old", "run:current"]);
    NodeAssert.deepEqual(ids(result.queries[7].runs), ["run:current"]);
    const all = result.queries[0];
    NodeAssert.deepEqual(ids(all.contextHandoffs), ["handoff:main", "handoff:z", "handoff:a"]);
    NodeAssert.deepEqual(ids(all.contextTransfers), ["transfer:main", "transfer:z", "transfer:a"]);
    NodeAssert.ok(ids(all.providerThreads).includes("provider:owned"));
    NodeAssert.ok(ids(all.providerThreads).includes("provider:subagent"));
    const messages = yield* read("thread:main", ["messages"], {
      messageIds: ["message:assistant", "message:user"],
    });
    NodeAssert.deepEqual(ids(messages.messages), ["message:user", "message:assistant"]);
    const latest = yield* read("thread:main", ["providerTurns"], {
      latestProviderTurnUsageOnly: true,
    });
    NodeAssert.deepEqual(ids(latest.providerTurns), ["turn:later"]);
    const omitted = yield* read("thread:main", ["nodes", "subagents"], {
      omitSettledLeafNodes: true,
      omitSettledSubagents: true,
    });
    NodeAssert.ok(!ids(omitted.nodes).includes("node:settled"));
    NodeAssert.ok(ids(omitted.nodes).includes("node:active-terminal"));
    NodeAssert.ok(ids(omitted.nodes).includes("node:without-run"));
    NodeAssert.ok(!ids(omitted.subagents).includes("subagent:settled"));
    NodeAssert.ok(ids(omitted.subagents).includes("subagent:active-terminal"));
    const compactedThread = "thread:compacted";
    const scoped = (type, payload) => makeEvent(type, payload, { threadId: compactedThread });
    yield* apply(
      scoped("thread.created", {
        ...jsonPayloads.thread,
        id: compactedThread,
        lineage: {
          parentThreadId: null,
          relationshipToParent: null,
          rootThreadId: compactedThread,
        },
      }),
    );
    const compactedRunCases = [
      ["old", "completed", "2026-11-10T09:00:00.000Z"],
      ["executed-late", "completed", "2026-11-10T10:30:00.000Z"],
      ["explicit", "completed", "2026-11-10T09:00:00.000Z"],
      ["resent", "completed", "2026-11-10T09:00:00.000Z"],
      ["unfinished", "failed", null],
      ["boundary", "completed", "2026-11-10T11:00:00.000Z"],
      ["later", "completed", "2026-11-10T09:00:00.000Z"],
      ["held", "queued", "2026-11-10T09:00:00.000Z"],
      ["rolled", "rolled_back", fixedTime],
    ];
    for (const [index, [name, status, completedAt]] of compactedRunCases.entries()) {
      const runId = `run:compacted:${name}`,
        attemptId = `attempt:compacted:${name}`,
        nodeId = `node:compacted:${name}`;
      yield* apply(
        scoped("run.created", {
          ...jsonPayloads.run,
          id: runId,
          threadId: compactedThread,
          ordinal: index + 1,
          status,
          requestedAt: "2026-11-10T10:00:00.000Z",
          completedAt,
          userMessageId: `message:compacted:${name}`,
          activeAttemptId: attemptId,
          rootNodeId: nodeId,
        }),
      );
      yield* apply(
        scoped("run-attempt.created", {
          ...jsonPayloads.attempt,
          id: attemptId,
          runId,
          rootNodeId: nodeId,
        }),
      );
      yield* apply(
        scoped("node.updated", {
          ...jsonPayloads.node,
          id: nodeId,
          runId,
          threadId: compactedThread,
          rootNodeId: nodeId,
        }),
      );
      yield* apply(
        scoped("message.updated", {
          ...jsonPayloads.message,
          id: `message:compacted:${name}`,
          runId,
          threadId: compactedThread,
          nodeId,
        }),
      );
      yield* apply(
        scoped("provider-turn.updated", {
          ...jsonPayloads.providerTurn,
          id: `turn:compacted:${name}`,
          providerThreadId: "provider:compacted",
          runAttemptId: attemptId,
          ordinal: index + 1,
          nativeTurnRef: null,
          nodeId,
        }),
      );
    }
    const narrowedFields = ["runs", "attempts", "nodes", "messages", "providerTurns", "turnItems"];
    const sinceLastCompaction = {
      keepRunIds: ["run:compacted:explicit"],
      keepMessageId: "message:compacted:resent",
    };
    result.beforeCompaction = yield* read(compactedThread, narrowedFields, { sinceLastCompaction });
    NodeAssert.equal(result.beforeCompaction.messages.length, 9);
    for (const [name, ordinal] of [
      ["boundary", 100],
      ["rolled", 200],
    ]) {
      yield* apply(
        scoped("turn-item.updated", {
          ...jsonPayloads.turnItem,
          id: `item:compacted:${name}`,
          threadId: compactedThread,
          runId: `run:compacted:${name}`,
          nodeId: `node:compacted:${name}`,
          type: "compaction",
          status: "completed",
          driver: null,
          ordinal,
        }),
      );
    }
    result.afterCompaction = yield* read(compactedThread, narrowedFields, { sinceLastCompaction });
    NodeAssert.equal(
      result.afterCompaction.runs.length,
      9,
      "Compaction does not narrow run metadata",
    );
    NodeAssert.equal(
      result.afterCompaction.turnItems.length,
      2,
      "Compaction does not narrow turn items",
    );
    for (const field of ["attempts", "nodes", "messages", "providerTurns"]) {
      NodeAssert.equal(result.afterCompaction[field].length, 8);
      NodeAssert.ok(!ids(result.afterCompaction[field]).some((id) => id.endsWith(":old")));
    }
    result.withoutCompactionExceptions = yield* read(compactedThread, narrowedFields, {
      sinceLastCompaction: { keepRunIds: [], keepMessageId: "message:missing" },
    });
    NodeAssert.equal(result.withoutCompactionExceptions.messages.length, 6);

    for (const [name, sessionId] of [
      ["private", null],
      ["shared", jsonPayloads.providerThread.providerSessionId],
    ]) {
      const nodeId = `subagent:settled:${name}`,
        providerId = `provider:settled:${name}`;
      yield* apply(
        makeEvent("provider-thread.updated", {
          ...jsonPayloads.providerThread,
          id: providerId,
          appThreadId: null,
          ownerNodeId: null,
          providerSessionId: sessionId,
          nativeThreadRef: null,
        }),
      );
      yield* apply(
        makeEvent("node.updated", {
          ...jsonPayloads.node,
          id: nodeId,
          kind: "subagent",
          status: "completed",
          runId: "run:old",
        }),
      );
      yield* apply(
        makeEvent("subagent.updated", {
          ...jsonPayloads.subagent,
          id: nodeId,
          runId: "run:old",
          providerThreadId: providerId,
          status: "completed",
          completionDelivery: { state: "delivered", observedByRunId: null },
        }),
      );
    }
    result.omittedSubagentRecords = yield* read(
      "thread:main",
      ["nodes", "subagents", "providerThreads"],
      {
        omitSettledLeafNodes: true,
        omitSettledSubagents: true,
        omitSettledSubagentRecords: true,
      },
    );
    NodeAssert.ok(!ids(result.omittedSubagentRecords.nodes).includes("subagent:settled:private"));
    NodeAssert.ok(
      !ids(result.omittedSubagentRecords.subagents).includes("subagent:settled:private"),
    );
    NodeAssert.ok(
      !ids(result.omittedSubagentRecords.providerThreads).includes("provider:settled:private"),
    );
    NodeAssert.ok(
      ids(result.omittedSubagentRecords.providerThreads).includes("provider:settled:shared"),
    );
    NodeAssert.ok(ids(result.omittedSubagentRecords.providerThreads).includes("provider:owned"));
    result.missing = (yield* read("missing", fields).pipe(Effect.flip))._tag;
    NodeAssert.equal(result.missing, "ProjectionStoreThreadNotFoundError");
    result.dateOrder = [];
    for (let index = 0; index < datePairs.length; index += 1) {
      const selected = yield* read("thread:dates", ["providerTurns"], {
        latestProviderTurnUsageOnly: true,
        providerTurnAttemptIds: [`attempt:date:${index}`],
      });
      result.dateOrder.push(ids(selected.providerTurns));
    }
    const boundedRead = Effect.fn("fixture.projectionRecords.boundedRead")(function* () {
      const items = yield* read("thread:bounded", ["turnItems"], {
        turnItemTypes: ["user_message"],
      });
      const messages = yield* read("thread:bounded", ["messages"], {
        messageIds: ["message:bounded:a", "message:bounded:b"],
      });
      NodeAssert.deepEqual(ids(items.turnItems), ["item:bounded:input"]);
      NodeAssert.deepEqual(ids(messages.messages), ["message:bounded:a", "message:bounded:b"]);
      return { items, messages };
    });
    result.boundedRead = yield* boundedRead();
    NodeAssert.deepEqual(
      normalized(yield* transaction(boundedRead())),
      normalized(result.boundedRead),
    );
    yield* transaction(
      Effect.gen(function* () {
        yield* apply(handoff("handoff:z", { summaryText: "Updated without moving source order" }));
        yield* apply(handoff("handoff:0-live"));
        result.inside = yield* read("thread:main", ["contextHandoffs"]);
        NodeAssert.deepEqual(ids(result.inside.contextHandoffs), [
          "handoff:main",
          "handoff:z",
          "handoff:a",
          "handoff:0-live",
        ]);
        yield* failed(
          transaction(
            Effect.gen(function* () {
              yield* apply(handoff("handoff:discarded"));
              return yield* Effect.fail("rollback selected records");
            }),
          ),
        );
        NodeAssert.deepEqual(
          ids((yield* read("thread:main", ["contextHandoffs"])).contextHandoffs),
          ids(result.inside.contextHandoffs),
        );
      }),
    );
    result.committed = yield* read("thread:main", fields);
    yield* failed(
      transaction(
        Effect.gen(function* () {
          yield* apply(transfer("transfer:discarded"));
          return yield* Effect.fail("rollback root selected records");
        }),
      ),
    );
    NodeAssert.deepEqual(
      ids((yield* read("thread:main", ["contextTransfers"])).contextTransfers),
      ids(all.contextTransfers),
    );
    result.narrowRuns = yield* exerciseNarrowRuns({ read, readMessages, apply, transaction });
    return normalized(result);
  });
});

try {
  const source = NodePath.join(fixture.root, "source.sqlite");
  await createMigratedSqlite(source, () =>
    Effect.gen(function* () {
      const store = yield* oracle.module.ProjectionStoreV2.pipe(
        Effect.provide(oracle.module.layer),
      );
      for (const event of [...sampleEvents, ...extraEvents]) yield* store.apply(event);
      yield* store.apply(
        makeEvent(
          "thread.created",
          {
            ...jsonPayloads.thread,
            id: "thread:dates",
            lineage: {
              parentThreadId: null,
              relationshipToParent: null,
              rootThreadId: "thread:dates",
            },
          },
          { threadId: "thread:dates" },
        ),
      );
      for (let index = 0; index < datePairs.length; index += 1) {
        for (let side = 0; side < 2; side += 1) {
          yield* store.apply(
            makeEvent(
              "provider-turn.updated",
              {
                ...jsonPayloads.providerTurn,
                id: `turn:date:${index}:${side}`,
                nativeTurnRef: null,
                ordinal: 100 + index * 2 + side,
                runAttemptId: `attempt:date:${index}`,
                tokenUsage: { usedTokens: 1, updatedAt: datePairs[index][side] },
              },
              { threadId: "thread:dates" },
            ),
          );
        }
      }
      const thread = "thread:bounded";
      yield* store.apply(
        makeEvent(
          "thread.created",
          {
            ...jsonPayloads.thread,
            id: thread,
            lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: thread },
          },
          { threadId: thread },
        ),
      );
      for (let index = 0; index < 18; index++) {
        yield* store.apply(
          makeEvent(
            "turn-item.updated",
            {
              ...jsonPayloads.turnItem,
              id: `item:bounded:large:${index}`,
              threadId: thread,
              runId: null,
              nodeId: null,
              providerThreadId: null,
              providerTurnId: null,
              ordinal: index + 2,
              text: "excluded history ".repeat(65_536),
              status: "completed",
              completedAt: fixedTime,
            },
            { threadId: thread },
          ),
        );
      }
      for (const suffix of ["a", "b"]) {
        yield* store.apply(
          makeEvent(
            "message.updated",
            {
              ...jsonPayloads.message,
              id: `message:bounded:${suffix}`,
              threadId: thread,
              runId: null,
              nodeId: null,
              text: "Selected small message",
              streaming: false,
            },
            { threadId: thread },
          ),
        );
      }
      yield* store.apply(
        makeEvent(
          "turn-item.updated",
          {
            ...jsonPayloads.turnItem,
            id: "item:bounded:input",
            threadId: thread,
            runId: null,
            nodeId: null,
            providerThreadId: null,
            providerTurnId: null,
            ordinal: 1,
            type: "user_message",
            messageId: "message:bounded:a",
            inputIntent: "turn_start",
            createdBy: "user",
            creationSource: "web",
            attachments: [],
          },
          { threadId: thread },
        ),
      );
    }),
  );
  const upstream = NodePath.join(fixture.root, "upstream.sqlite");
  const retained = NodePath.join(fixture.root, "retained.sqlite");
  await NodeFSP.copyFile(source, upstream);
  await NodeFSP.copyFile(source, retained);
  const sourceHash = hash(await NodeFSP.readFile(source));
  const exported = NodePath.join(fixture.root, "source.ndjson");
  exportSqliteState({ source, destination: exported });
  nativeRequest(executable, fixture, "initialize", {
    operation: "import_sqlite_application",
    path: exported,
  });
  const expected = await withServices("upstream", upstream, exercise);
  NodeAssert.deepEqual(
    await withServices("sqlite", retained, exercise),
    expected,
    "Extracted selected-record SQL preserves immutable upstream behavior",
  );
  NodeAssert.deepEqual(
    await withServices("redb", source, exercise),
    expected,
    "Native selected records match immutable upstream behavior",
  );
  checks.push(
    `All 15 selected-record families and ${queries.length} filter combinations match immutable upstream and retained SQLite`,
    "Filtered turn items and message ordering avoid more than 16 MiB of excluded history bodies, including inside the live writer transaction",
    "Message order uses all relevant turn-item ordinals even when the requested turn-item fields are filtered",
    "Latest-run guards, settled leaf/subagent exclusions, owner-linked provider threads and nullable run filters match upstream",
    "Queued and explicit-run reads preserve ownership, ordering, duplicate/empty/combined filters, latest-run rules and atomic queued membership; queue-shaped message reads load only requested owned IDs",
    "Token-usage timestamp ordering preserves UTC-offset equivalence, equal-time ordinal ties and invalid-date ordering",
    `${datePairs.length} paired timestamp vectors preserve SQLite date/time/numeric/clock formats, boundaries and millisecond truncation`,
    "Imported context row order and update/new-row order survive root and nested transaction rollback",
  );
  await withServices("redb", source, ({ read }) =>
    Effect.gen(function* () {
      NodeAssert.deepEqual(normalized(yield* read("thread:main", fields)), expected.committed);
    }),
  );
  checks.push("A reopened native helper preserves the complete selected-record result");
  NodeAssert.equal(hash(await NodeFSP.readFile(source)), sourceHash);
  await finishProof({
    executable,
    fixture,
    proofPath,
    checks,
    sourceHash,
    oracles: [oracle.source],
    sourcePaths: [
      "apps/server/src/orchestration-v2/ProjectionStore.ts",
      "apps/server/src/orchestration-v2/ProjectionRecordRows.ts",
      "apps/server/src/orchestration-v2/NativeProjectionRecordRows.ts",
      "apps/server/src/orchestration-v2/NativeProjectionStore.ts",
      "apps/server/src/orchestration-v2/NativeProjectionLookupRows.ts",
      "apps/server/src/persistence/redb_projection_records.rs",
      "apps/server/src/persistence/redb_projection_lookup.rs",
      "apps/server/src/persistence/redb_projection_read_index.rs",
      "apps/server/src/persistence/redb_projection_window_index.rs",
      "tools/check-redb-projection-records.mjs",
    ],
  });
} finally {
  await fixture.close();
}
