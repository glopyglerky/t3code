#!/usr/bin/env node
import * as NodeAssert from "node:assert/strict";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";

import * as ProjectionStore from "../apps/server/src/orchestration-v2/ProjectionStore.ts";
import * as NativeProjectionRecords from "../apps/server/src/orchestration-v2/NativeProjectionRecordRows.ts";
import * as NativeProjectionStore from "../apps/server/src/orchestration-v2/NativeProjectionStore.ts";
import { encodeThreadHistoryCursor } from "../apps/server/src/orchestration-v2/threadHistoryPaging.ts";
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
const oracle = await loadUpstreamModule("apps/server/src/orchestration-v2/ProjectionStore.ts");
const fixture = await createFixture();
const checks = [];
const failed = (body) =>
  Effect.exit(body).pipe(
    Effect.tap((exit) => Effect.sync(() => NodeAssert.ok(Exit.isFailure(exit)))),
  );

function eventsFor(name, workCount, fork) {
  const thread = `thread:${name}`,
    run = `run:${name}`,
    node = `node:${name}`,
    attempt = `attempt:${name}`,
    message = `message:${name}`;
  const event = (type, payload) => makeEvent(type, payload, { threadId: thread });
  const item = (id, ordinal, fields) =>
    event("turn-item.updated", {
      ...jsonPayloads.turnItem,
      threadId: thread,
      id,
      runId: run,
      nodeId: node,
      providerThreadId: null,
      providerTurnId: null,
      ordinal,
      status: "completed",
      streaming: false,
      completedAt: fixedTime,
      ...fields,
    });
  return [
    event("thread.created", {
      ...jsonPayloads.thread,
      id: thread,
      lineage: {
        parentThreadId: fork?.threadId ?? null,
        relationshipToParent: fork ? "fork" : null,
        rootThreadId: "thread:main",
      },
      forkedFrom: fork ?? null,
    }),
    event("run.created", {
      ...jsonPayloads.run,
      id: run,
      threadId: thread,
      rootNodeId: node,
      activeAttemptId: attempt,
      userMessageId: message,
      providerThreadId: null,
      status: "completed",
      completedAt: fixedTime,
    }),
    event("run-attempt.created", {
      ...jsonPayloads.attempt,
      id: attempt,
      runId: run,
      rootNodeId: node,
      status: "completed",
      completedAt: fixedTime,
    }),
    event("node.updated", {
      ...jsonPayloads.node,
      id: node,
      threadId: thread,
      runId: run,
      rootNodeId: node,
      status: "completed",
      completedAt: fixedTime,
    }),
    event("message.updated", {
      ...jsonPayloads.message,
      id: message,
      threadId: thread,
      runId: run,
      nodeId: node,
      role: "user",
      createdBy: "user",
      creationSource: "web",
      streaming: false,
    }),
    item(`item:${name}:input`, 1, {
      type: "user_message",
      messageId: message,
      inputIntent: "turn_start",
      createdBy: "user",
      creationSource: "web",
      attachments: [
        {
          type: "file",
          id: `attachment_${name}`,
          name: "fixture.txt",
          mimeType: "text/plain",
          sizeBytes: 12,
        },
      ],
    }),
    ...Array.from({ length: workCount }, (_, index) =>
      item(`item:${name}:work:${String(index).padStart(4, "0")}`, index + 2, {
        text: `Owned tool output ${index}: ${"x".repeat(512)}`,
      }),
    ),
  ];
}

function edgeEvents() {
  const thread = "thread:edge",
    run = "run:edge",
    node = "node:edge";
  const event = (type, payload) => makeEvent(type, payload, { threadId: thread });
  const item = (id, ordinal, fields) =>
    event("turn-item.updated", {
      ...jsonPayloads.turnItem,
      id,
      ordinal,
      threadId: thread,
      runId: run,
      nodeId: node,
      providerThreadId: null,
      providerTurnId: null,
      status: "completed",
      completedAt: fixedTime,
      streaming: false,
      ...fields,
    });
  const events = [
    ...eventsFor("edge", 160),
    event("run.updated", {
      ...jsonPayloads.run,
      id: run,
      threadId: thread,
      rootNodeId: node,
      activeAttemptId: "attempt:edge:new",
      userMessageId: "message:edge",
      providerThreadId: null,
      status: "running",
    }),
    event("run-attempt.updated", {
      ...jsonPayloads.attempt,
      id: "attempt:edge",
      runId: run,
      rootNodeId: node,
      status: "superseded",
      completedAt: fixedTime,
    }),
    event("run-attempt.created", {
      ...jsonPayloads.attempt,
      id: "attempt:edge:new",
      runId: run,
      rootNodeId: "node:edge:new",
      attemptOrdinal: 2,
    }),
    event("node.updated", {
      ...jsonPayloads.node,
      id: "node:edge:new",
      threadId: thread,
      runId: run,
      rootNodeId: "node:edge:new",
      parentNodeId: node,
    }),
    item("item:edge:hidden-interrupt", 164, {
      type: "run_interrupt_result",
      message: "Superseded without request",
    }),
    item("item:edge:inline-image", 166, {
      type: "dynamic_tool",
      toolName: "view_image",
      viewedImagePath: "fixture.png",
      input: {},
    }),
    item("item:edge:inline-app", 167, {
      type: "dynamic_tool",
      toolName: "fixture",
      input: {},
      output: { t3McpApp: {} },
    }),
    item("item:edge:inline-html", 168, {
      type: "dynamic_tool",
      toolName: "fixture",
      input: { HtmlRender: true },
    }),
    item("item:edge:inline-delegate", 169, {
      type: "dynamic_tool",
      toolName: "fixture_delegate_task",
      input: {},
    }),
  ];
  for (const [suffix, ordinal, status, inputIntent] of [
    ["cancelled", 2, "cancelled", "queued_turn"],
    ["rolled", 3, "rolled_back", "turn_start"],
  ]) {
    events.push(
      event("run.created", {
        ...jsonPayloads.run,
        id: `run:edge:${suffix}`,
        threadId: thread,
        ordinal,
        rootNodeId: node,
        providerThreadId: null,
        status,
        completedAt: fixedTime,
      }),
    );
    events.push(
      item(`item:edge:${suffix}`, ordinal * 100, {
        type: "user_message",
        runId: `run:edge:${suffix}`,
        messageId: `message:edge:${suffix}`,
        inputIntent,
        createdBy: "user",
        creationSource: "web",
        attachments: [],
      }),
    );
  }
  events.push(
    event("run.created", {
      ...jsonPayloads.run,
      id: "run:edge:wake",
      threadId: thread,
      ordinal: 4,
      rootNodeId: node,
      providerThreadId: null,
      status: "completed",
      completedAt: fixedTime,
    }),
  );
  events.push(
    item("item:edge:wake", 400, {
      type: "notification",
      runId: "run:edge:wake",
      source: { kind: "background_task" },
      outcome: "completed",
      summary: "Controlled wake input",
    }),
  );
  for (const [id, ordinal] of [
    ["plan:edge:z-old", 401],
    ["plan:edge:a-new", 402],
    ["plan:edge:m-tied", 402],
  ]) {
    events.push(
      event("plan.updated", {
        id,
        threadId: thread,
        runId: "run:edge:wake",
        nodeId: node,
        kind: "todo_list",
        status: "completed",
        steps: [],
      }),
    );
    events.push(
      item(`item:${id}`, ordinal, {
        type: "todo_list",
        planId: id,
        runId: "run:edge:wake",
        steps: [],
      }),
    );
  }
  return events;
}

function duplicateOrdinalEvents() {
  const thread = "thread:duplicates",
    run = "run:duplicates",
    node = "node:duplicates:second";
  const event = (type, payload) => makeEvent(type, payload, { threadId: thread });
  return [
    ...eventsFor("duplicates", 151),
    event("run-attempt.created", {
      ...jsonPayloads.attempt,
      id: "attempt:duplicates:second",
      runId: run,
      attemptOrdinal: 2,
      rootNodeId: node,
      status: "completed",
      completedAt: fixedTime,
    }),
    event("node.updated", {
      ...jsonPayloads.node,
      id: node,
      threadId: thread,
      runId: run,
      rootNodeId: node,
      status: "completed",
      completedAt: fixedTime,
    }),
    ...Array.from({ length: 152 }, (_, index) =>
      event("turn-item.updated", {
        ...jsonPayloads.turnItem,
        id: `item:duplicates:second:${String(index).padStart(4, "0")}`,
        threadId: thread,
        runId: run,
        nodeId: node,
        providerThreadId: null,
        providerTurnId: null,
        ordinal: index === 0 ? 2 : index + 300,
        status: "completed",
        streaming: false,
        completedAt: fixedTime,
      }),
    ),
  ];
}

const cases = [
  ["thread:main", { rowLimit: 77 }],
  ["thread:main", { rowLimit: 0 }],
  ["thread:main", { rowLimit: 2, userTurnLimit: 1 }],
  ["thread:main", { rowLimit: 2, userTurnLimit: 1, deferWork: true }],
  ["thread:main", { rowLimit: 2, anchorItemId: "item:main:work:0020" }],
  ["thread:main", { rowLimit: 0, anchorItemId: "item:main:work:0020" }],
  ["thread:main", { rowLimit: 1, anchorItemId: "item:interrupt:result" }],
  ["thread:main", { rowLimit: 5, anchorItemId: "missing" }],
  ["thread:long", { rowLimit: 77, userTurnLimit: 1 }],
  ["thread:long", { rowLimit: 77, userTurnLimit: 1, deferWork: true }],
  ["thread:long", { rowLimit: 0, userTurnLimit: 1, deferWork: true }],
  ["thread:child", { rowLimit: 77, userTurnLimit: 1 }],
  ["thread:child", { rowLimit: 77, userTurnLimit: 1, deferWork: true }],
  ["thread:child", { rowLimit: 3, anchorItemId: "item:main:work:0010" }],
  [
    "thread:child",
    {
      rowLimit: 3,
      anchorThreadId: "thread:main",
      anchorItemId: "item:main:work:0010",
      deferWork: true,
    },
  ],
  ["thread:grandchild", { rowLimit: 77, userTurnLimit: 1 }],
  ["thread:grandchild", { rowLimit: 0, userTurnLimit: 1 }],
  ["thread:grandchild", { rowLimit: 3, anchorItemId: "item:main:work:0010" }],
  ["thread:edge", { rowLimit: 1 }],
  ["thread:edge", { rowLimit: 0, userTurnLimit: 1, deferWork: true }],
  ["thread:edge", { rowLimit: 10, userTurnLimit: 1, deferWork: true }],
  [
    "thread:edge",
    { rowLimit: 10, userTurnLimit: 1, deferWork: true, anchorItemId: "item:edge:work:0159" },
  ],
  ["thread:edge-fork", { rowLimit: 77, userTurnLimit: 1, deferWork: true }],
  ["thread:duplicates", { rowLimit: 77, userTurnLimit: 1, deferWork: true }],
  [
    "thread:duplicates",
    {
      rowLimit: 77,
      userTurnLimit: 1,
      deferWork: true,
      anchorItemId: "item:duplicates:second:0000",
    },
  ],
  ["thread:many-child", { rowLimit: 77, userTurnLimit: 10, deferWork: true }],
  ["thread:legacy-child", { rowLimit: 77, userTurnLimit: 1, deferWork: true }],
];

async function withServices(kind, filename, use) {
  if (kind === "redb")
    return withNative(executable, fixture, (client) =>
      Effect.gen(function* () {
        const store = yield* NativeProjectionStore.make;
        const timeline = yield* NativeProjectionRecords.makeTimelineIndex;
        return yield* use({
          store,
          apply: store.apply,
          transaction: client.withTransaction,
          timeline: (thread) =>
            client.withReadTransaction(
              timeline(thread, new Set()).pipe(Effect.map((index) => index.visible)),
            ),
        });
      }),
    );
  return withSqlite(filename, (sql) =>
    Effect.gen(function* () {
      const module = kind === "upstream" ? oracle.module : ProjectionStore;
      const store = yield* module.ProjectionStoreV2.pipe(Effect.provide(module.layer));
      return yield* use({
        store,
        apply: store.apply,
        transaction: (body) => sql.withTransaction(body),
        timeline: (thread) =>
          store
            .getTimelinePage(thread, { view: "activity", limit: 10_000 })
            .pipe(Effect.map((page) => page.items)),
      });
    }),
  );
}

const identity = (rows) =>
  rows.map((row) => ({
    sourceThreadId: row.sourceThreadId,
    sourceItemId: row.sourceItemId,
    visibility: row.visibility,
    item: {
      id: row.item.id,
      threadId: row.item.threadId,
      runId: row.item.runId,
      nodeId: row.item.nodeId,
      parentItemId: row.item.parentItemId,
      type: row.item.type,
      status: row.item.status,
      ordinal: row.item.ordinal,
    },
  }));

const exercise = Effect.fn("checkRedbProjectionWindows.exercise")(function* ({
  store,
  apply,
  transaction,
  timeline,
}) {
  const result = { windows: [], snapshots: [], timelines: [], pages: [], history: [] };
  for (const [thread, options] of cases)
    result.windows.push(yield* store.getThreadSnapshotWindow(thread, options));
  for (const thread of [
    "thread:main",
    "thread:child",
    "thread:grandchild",
    "thread:long",
    "thread:edge",
    "thread:edge-fork",
  ]) {
    result.snapshots.push(yield* store.getThreadSnapshot(thread));
    const timelineRows = yield* timeline(thread);
    result.timelines.push(identity(timelineRows));
    result.pages.push(
      yield* store.getTimelinePage(thread, { view: "activity", limit: 7, afterPosition: 2 }),
    );
    result.pages.push(yield* store.getTimelinePage(thread, { view: "conversation", limit: 3 }));
    result.pages.push(
      yield* store.getTimelinePage(thread, {
        view: "activity",
        limit: 7,
        attachmentId: "attachment_main",
      }),
    );
    result.pages.push(
      yield* store.getTimelinePage(thread, {
        view: "activity",
        limit: 7,
        itemId: "item:main:input",
      }),
    );
    const last = timelineRows.at(-1);
    const cursor = encodeThreadHistoryCursor({
      snapshotSequence: 0,
      sourceThreadId: last.sourceThreadId,
      sourceItemId: last.sourceItemId,
      position: last.item.ordinal,
      inclusive: true,
    });
    const page = yield* store.getThreadHistoryPage(thread, cursor, undefined, false, undefined);
    result.history.push(page);
    if (page.nextCursor !== null)
      result.history.push(
        yield* store.getThreadHistoryPage(thread, page.nextCursor, undefined, true, undefined),
      );
    const deferred = yield* store.getThreadHistoryPage(
      thread,
      cursor,
      undefined,
      false,
      "deferred",
    );
    result.history.push(deferred);
    const workAnchor = deferred.items.find((row) => row.item.deferredWork !== undefined);
    if (workAnchor !== undefined)
      result.history.push(
        yield* store.getThreadHistoryPage(
          thread,
          workAnchor.item.deferredWork.cursor,
          workAnchor.sourceItemId,
          false,
          "work",
        ),
      );
  }
  NodeAssert.ok(
    result.windows[3].projection.turnItems.some((item) => item.deferredWork?.itemCount > 150),
    "Completed tool work becomes a deferred cohort",
  );
  NodeAssert.ok(
    result.windows[6].projection.turnItems.some((item) => item.id === "item:interrupt:request"),
    "Interrupt result retains its earlier request",
  );
  NodeAssert.ok(
    result.windows[16].projection.visibleTurnItems.some((item) => item.visibility === "synthetic"),
    "Zero-row fork windows retain synthetic markers",
  );
  result.missing = (yield* store
    .getThreadSnapshotWindow("missing", { rowLimit: 1 })
    .pipe(Effect.flip))._tag;

  // A steer-cancelled run has no items or started session, but it is still the
  // shell's latest presented run. Held queues and silent compaction cannot hide it.
  const presentedThread = "thread:presented";
  const presentedEvent = (type, payload) =>
    makeEvent(type, { ...payload, threadId: presentedThread }, { threadId: presentedThread });
  for (const event of eventsFor("presented", 3)) yield* apply(event);
  yield* apply(
    presentedEvent("run.created", {
      ...jsonPayloads.run,
      id: "run:presented:cancelled",
      ordinal: 2,
      status: "cancelled",
      startedAt: null,
      completedAt: fixedTime,
      userMessageId: "message:presented:cancelled",
      activeAttemptId: null,
      providerThreadId: null,
    }),
  );
  yield* apply(
    presentedEvent("run.created", {
      ...jsonPayloads.run,
      id: "run:presented:held",
      ordinal: 3,
      status: "queued",
      queueHeld: true,
      startedAt: null,
      userMessageId: "message:presented:held",
      activeAttemptId: null,
      providerThreadId: null,
    }),
  );
  yield* apply(
    presentedEvent("message.updated", {
      ...jsonPayloads.message,
      id: "message:presented:compact",
      runId: "run:presented:compact",
      nodeId: null,
      role: "user",
      createdBy: "agent",
      creationSource: "server",
      text: "\u00a0/COMPACT\ufeff",
      streaming: false,
    }),
  );
  yield* apply(
    presentedEvent("run.created", {
      ...jsonPayloads.run,
      id: "run:presented:compact",
      ordinal: 4,
      status: "completed",
      completedAt: fixedTime,
      userMessageId: "message:presented:compact",
      activeAttemptId: null,
      providerThreadId: null,
    }),
  );
  result.presented = yield* store.getThreadSnapshotWindow(presentedThread, { rowLimit: 0 });
  NodeAssert.ok(
    result.presented.projection.runs.some((run) => run.id === "run:presented:cancelled"),
    "An unanchored empty window retains the shell's presented run",
  );
  result.presentedAnchored = yield* store.getThreadSnapshotWindow(presentedThread, {
    rowLimit: 1,
    anchorItemId: "item:presented:input",
  });
  for (const event of eventsFor("presented-child", 0, {
    type: "run",
    threadId: presentedThread,
    runId: "run:presented",
  }))
    yield* apply(event);
  result.presentedRequired = yield* store.getThreadSnapshotWindow("thread:presented-child", {
    rowLimit: 0,
  });
  for (const snapshot of [result.presentedAnchored, result.presentedRequired])
    NodeAssert.ok(
      !snapshot.projection.runs.some((run) => run.id === "run:presented:cancelled"),
      "Anchored and required-run windows do not add an unrelated presented run",
    );
  const change = makeEvent("turn-item.updated", {
    ...jsonPayloads.turnItem,
    id: "item:live",
    ordinal: 2_000_000,
    text: "Committed native window item",
  });
  yield* transaction(
    Effect.gen(function* () {
      yield* apply(change);
      result.inside = yield* store.getThreadSnapshotWindow("thread:main", { rowLimit: 1 });
      yield* failed(
        transaction(
          Effect.gen(function* () {
            yield* apply(
              makeEvent("turn-item.updated", {
                ...jsonPayloads.turnItem,
                id: "item:rolled-back",
                ordinal: 3_000_000,
              }),
            );
            return yield* Effect.fail("rollback window metadata and row");
          }),
        ),
      );
      NodeAssert.deepEqual(
        normalized(yield* store.getThreadSnapshotWindow("thread:main", { rowLimit: 1 })),
        normalized(result.inside),
      );
    }),
  );
  result.committed = yield* store.getThreadSnapshotWindow("thread:main", { rowLimit: 1 });
  return normalized(result);
});

try {
  const source = NodePath.join(fixture.root, "source.sqlite");
  await createMigratedSqlite(source, (sql) =>
    Effect.gen(function* () {
      const store = yield* oracle.module.ProjectionStoreV2.pipe(
        Effect.provide(oracle.module.layer),
      );
      for (const event of sampleEvents) yield* store.apply(event);
      for (const event of eventsFor("main", 160)) yield* store.apply(event);
      for (const event of eventsFor("long", 520)) yield* store.apply(event);
      for (const [index, toolName] of [
        "fixture_delegate_task",
        "fixture_delegateXtask",
        "fixture_delegate🦊task",
      ].entries()) {
        yield* store.apply(
          makeEvent(
            "turn-item.updated",
            {
              ...jsonPayloads.turnItem,
              id: `item:long:delegate:${index}`,
              threadId: "thread:long",
              runId: "run:long",
              nodeId: "node:long",
              providerThreadId: null,
              providerTurnId: null,
              ordinal: 3 + index,
              status: "completed",
              completedAt: fixedTime,
              type: "dynamic_tool",
              toolName,
              input: {},
              output: {},
            },
            { threadId: "thread:long" },
          ),
        );
      }
      for (const event of eventsFor("child", 4, {
        type: "run",
        threadId: "thread:main",
        runId: "run:main",
      }))
        yield* store.apply(event);
      for (const event of eventsFor("grandchild", 2, {
        type: "run",
        threadId: "thread:child",
        runId: "run:child",
      }))
        yield* store.apply(event);
      for (const event of edgeEvents()) yield* store.apply(event);
      for (const event of eventsFor("edge-fork", 3, {
        type: "run",
        threadId: "thread:edge",
        runId: "run:edge:rolled",
      }))
        yield* store.apply(event);
      for (const event of duplicateOrdinalEvents()) yield* store.apply(event);
      for (const event of eventsFor("many-child", 0, {
        type: "run",
        threadId: "thread:main",
        runId: "run:main",
      }))
        yield* store.apply(event);
      for (let index = 0; index < 12; index++) {
        yield* store.apply(
          makeEvent(
            "turn-item.updated",
            {
              ...jsonPayloads.turnItem,
              id: `item:many-child:${index}`,
              threadId: "thread:many-child",
              runId: "run:many-child",
              nodeId: "node:many-child",
              providerThreadId: null,
              providerTurnId: null,
              ordinal: 10 + index,
              status: "completed",
              completedAt: fixedTime,
              type: "user_message",
              inputIntent: "turn_start",
              createdBy: "user",
              creationSource: "web",
              messageId: `message:many-child:${index}`,
              attachments: [],
            },
            { threadId: "thread:many-child" },
          ),
        );
      }
      for (const event of eventsFor("legacy", 0)) yield* store.apply(event);
      // Existing V1 rows retain history-origin metadata beyond today's item codec.
      yield* sql`UPDATE orchestration_v2_projection_threads SET payload_json = json_set(payload_json, '$.historyOrigin', 'v1_import') WHERE thread_id = 'thread:legacy'`;
      yield* sql`UPDATE orchestration_v2_projection_turn_items SET run_id = NULL,
      payload_json = json_set(payload_json, '$.runId', NULL, '$.historyOrigin', 'v1_import') WHERE turn_item_id = 'item:legacy:input'`;
      for (const event of eventsFor("legacy-child", 0, {
        type: "run",
        threadId: "thread:legacy",
        runId: "run:legacy",
      }))
        yield* store.apply(event);
      // A different run owns interruption cards, so its block cannot suppress the
      // completed main run's deferred-work cohort.
      yield* store.apply(
        makeEvent("run.created", {
          ...jsonPayloads.run,
          id: "run:interrupt",
          ordinal: 2,
          status: "completed",
          completedAt: fixedTime,
        }),
      );
      for (const [id, type, ordinal] of [
        ["item:interrupt:request", "run_interrupt_request", 2000],
        ["item:interrupt:result", "run_interrupt_result", 3000],
      ]) {
        yield* store.apply(
          makeEvent("turn-item.updated", {
            ...jsonPayloads.turnItem,
            id,
            type,
            ordinal,
            runId: "run:interrupt",
            status: "completed",
            message: "Controlled interrupt",
          }),
        );
      }
      yield* store.apply(
        makeEvent("turn-item.updated", {
          ...jsonPayloads.turnItem,
          id: "item:pending",
          type: "user_input_request",
          ordinal: 0,
          requestId: "request:main",
          questions: [],
        }),
      );
    }),
  );
  const upstream = NodePath.join(fixture.root, "upstream.sqlite"),
    retained = NodePath.join(fixture.root, "retained.sqlite");
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
    "Shared window/timeline transformations preserve immutable upstream",
  );
  NodeAssert.deepEqual(
    await withServices("redb", source, exercise),
    expected,
    "Native indexed windows and fork timelines match immutable upstream",
  );
  checks.push(
    `${cases.length} snapshot windows match immutable upstream and retained SQLite`,
    "Whole-turn and long-turn row budgets, zero-row windows, deferred work, anchored reads, pending cards and interrupt retention preserve exact payloads",
    "Two-level fork ancestry and cross-thread anchor inference preserve synthetic markers and inherited positions",
    "Real projected timeline index identities agree with the original getTimelinePage, including ancestor visibility",
    "Timeline activity/conversation/item/attachment filters and bounded history/deferred-work pagination match exact upstream payloads and cursors",
    "Cancelled queued inputs, rolled-back ancestry, superseded interrupts, notification starts, persistent tool output and latest todo replacements preserve native selection",
    "Unanchored windows retain the presented run outside their item window, excluding held queues and silent compaction from its selection",
    "Same-transaction windows see new items and nested rollback restores metadata plus payloads",
  );
  await withServices("redb", source, ({ store }) =>
    Effect.gen(function* () {
      NodeAssert.deepEqual(
        normalized(yield* store.getThreadSnapshotWindow("thread:main", { rowLimit: 1 })),
        expected.committed,
      );
    }),
  );
  checks.push("Reopened native windows preserve committed state");
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
      "apps/server/src/orchestration-v2/ProjectionHistoryRows.ts",
      "apps/server/src/orchestration-v2/SqliteProjectionShellRows.ts",
      "apps/server/src/orchestration-v2/NativeProjectionRecordRows.ts",
      "apps/server/src/orchestration-v2/NativeProjectionStore.ts",
      "apps/server/src/persistence/redb_projection_records.rs",
      "apps/server/src/persistence/redb_projection_shell.rs",
      "apps/server/src/persistence/redb_projection_window.rs",
      "apps/server/src/persistence/redb_projection_window_index.rs",
      "tools/check-redb-projection-windows.mjs",
    ],
  });
} finally {
  await fixture.close();
}
