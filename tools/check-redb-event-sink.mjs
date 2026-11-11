#!/usr/bin/env node
import * as Assert from "node:assert/strict";
import * as FS from "node:fs/promises";
import * as Path from "node:path";
import * as Contracts from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as PubSub from "effect/PubSub";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import * as SecureRandom from "effect/crypto/SecureRandom";
import * as SqlClient from "effect/sql/SqlClient";
import * as SqlError from "effect/sql/SqlError";

import * as CommandReceiptStore from "../apps/server/src/orchestration-v2/CommandReceiptStore.ts";
import * as EffectOutbox from "../apps/server/src/orchestration-v2/EffectOutbox.ts";
import * as EventSink from "../apps/server/src/orchestration-v2/EventSink.ts";
import * as EventStore from "../apps/server/src/orchestration-v2/EventStore.ts";
import * as NativeEventSink from "../apps/server/src/orchestration-v2/NativeEventSink.ts";
import * as NativeDatabase from "../apps/server/src/orchestration-v2/NativeDatabase.ts";
import * as ProjectStore from "../apps/server/src/orchestration-v2/ProjectStore.ts";
import * as ProjectionStore from "../apps/server/src/orchestration-v2/ProjectionStore.ts";
import * as ProviderCommitBatch from "../apps/server/src/orchestration-v2/ProviderCommitBatch.ts";
import * as TurnItemPositionStore from "../apps/server/src/orchestration-v2/TurnItemPositionStore.ts";
import * as PersistenceTransactions from "../apps/server/src/persistence/PersistenceTransactions.ts";
import { replayAndBufferProjectedLiveEvents } from "../apps/server/src/orchestration-v2/LiveStreamBudget.ts";
import { projectStoredEventForWire } from "../apps/server/src/orchestration-v2/WireProjection.ts";
import { makeRunOutcomeRecorder } from "../apps/server/src/orchestration-v2/RunMetrics.ts";
import { createFixture } from "./database-fixture.mjs";
import { exportSqliteState } from "./export-sqlite-state.mjs";
import { at } from "./check-outbox-runtime.mjs";
import { upstreamSource } from "./upstream-source.mjs";
import { checkProjectionCodecs } from "./redb-projection-codec-cases.mjs";
import {
  createMigratedSqlite,
  finishProof,
  hash,
  loadUpstream,
  loadUpstreamModule,
  nativeRequest,
  normalized,
  withNative,
  withSqlite,
} from "./redb-domain-fixture.mjs";
import {
  fixedTime,
  fixedDateTime,
  payloads,
  jsonPayloads,
  sampleEvents,
  pullRequestLinkUpdate,
  makeEvent,
} from "./redb-event-sink-fixtures.mjs";

const [executable, proofPath] = process.argv.slice(2);
Assert.ok(executable && Path.isAbsolute(executable), "Pass an absolute native database executable");
await FS.access(executable, FS.constants.X_OK);
const projectionOracle = await loadUpstreamModule(
  "apps/server/src/orchestration-v2/ProjectionStore.ts",
);
const oracle = loadUpstream(
  "apps/server/src/orchestration-v2/EventSink.ts",
  {
    ...Contracts,
    Context,
    DateTime,
    Effect,
    Layer,
    Option,
    PubSub,
    Semaphore,
    Schema,
    Stream,
    SqlClient,
    SqlError,
    ProviderCommitBatch,
    CommandReceiptStore,
    EffectOutbox,
    EventStore,
    ProjectionStore: projectionOracle.module,
    ProjectStore,
    TurnItemPositionStore,
    replayAndBufferProjectedLiveEvents,
    projectStoredEventForWire,
    makeRunOutcomeRecorder,
  },
  ["EventSinkV2", "layerFromStores"],
);
const fixture = await createFixture();
const checks = [];
const collect = (stream) => stream.pipe(Stream.runCollect, Effect.map(Array.from));
const tables = [
  "orchestration_events",
  "orchestration_command_receipts",
  "projection_projects",
  "orchestration_v2_effect_outbox",
  "orchestration_v2_turn_item_positions",
  ...[
    "threads",
    "runs",
    "run_attempts",
    "nodes",
    "subagents",
    "provider_sessions",
    "provider_session_bindings",
    "provider_threads",
    "provider_turns",
    "runtime_requests",
    "messages",
    "plans",
    "turn_items",
    "checkpoint_scopes",
    "checkpoints",
    "context_handoffs",
    "context_transfers",
    "metadata",
  ].map((name) => `orchestration_v2_projection_${name}`),
];
const ordered = (rows) =>
  rows
    .map((row) => Object.fromEntries(Object.entries(row).sort(([a], [b]) => a.localeCompare(b))))
    .sort((a, b) => JSON.stringify(a).localeCompare(JSON.stringify(b)));
const detached = (id, threadId = "thread:main") =>
  makeEvent(
    "provider-session.detached",
    {
      providerSessionId: `session:${id}`,
      detachedAt: fixedTime,
    },
    { id, threadId },
  );
const pending = (id, request = { type: "terminal.cleanup" }) => ({
  id,
  commandId: `command:${id}`,
  threadId: "thread:main",
  request,
});
const command = (id, events, effects = []) => ({
  commandId: `command:${id}`,
  threadId: "thread:main",
  commandType: "fixture.command",
  acceptedAt: fixedDateTime,
  events,
  effects,
});
const projectEvent = (id, projectId = "project:main") => ({
  eventId: id,
  aggregateKind: "project",
  aggregateId: projectId,
  type: "project.created",
  occurredAt: fixedTime,
  commandId: `command:${id}`,
  causationEventId: null,
  correlationId: `command:${id}`,
  metadata: {},
  payload: {
    projectId,
    title: "Native project",
    workspaceRoot: `/fixture/${projectId}`,
    defaultModelSelection: null,
    scripts: [],
    createdAt: fixedTime,
    updatedAt: fixedTime,
  },
});
const projectCommand = (id, projectId = "project:main") => ({
  commandId: `command:${id}`,
  projectId,
  commandType: "project.create",
  acceptedAt: fixedDateTime,
  event: projectEvent(id, projectId),
});
const expectFailure = (effect) =>
  effect.pipe(
    Effect.exit,
    Effect.tap((exit) =>
      Effect.sync(() => Assert.ok(Exit.isFailure(exit), "Expected a failed operation")),
    ),
  );

function faultTransport(database, faults) {
  return NativeDatabase.NativeDatabase.of({
    request: database.request,
    withSession: (use) =>
      database.withSession((session) =>
        use({
          discard: session.discard,
          request: Effect.fnUntraced(function* (input, response) {
            faults.frames.push(structuredClone(input));
            if (input.operation === "application_begin" && faults.busyBegins > 0) {
              faults.busyBegins -= 1;
              return yield* new NativeDatabase.NativeDatabaseRejected({ kind: "writer_busy" });
            }
            const result = yield* session.request(input, response);
            if (input.operation === faults.drop) {
              faults.drop = undefined;
              yield* session.discard;
              return yield* new NativeDatabase.NativeDatabaseOutcomeUnknown({ stage: "response" });
            }
            return result;
          }),
        }),
      ),
  });
}

async function withServices(kind, filename, use, target = fixture, faults) {
  const run = (client, sql) =>
    Effect.gen(function* () {
      const stores =
        kind === "redb"
          ? undefined
          : Layer.mergeAll(
              CommandReceiptStore.layer,
              EffectOutbox.layer,
              EventStore.layer,
              kind === "upstream" ? projectionOracle.module.layer : ProjectionStore.layer,
              ProjectStore.layer,
              TurnItemPositionStore.layer,
            );
      const serviceLayer =
        kind === "redb"
          ? NativeEventSink.layer
          : (kind === "upstream" ? oracle.module.layerFromStores : EventSink.layerFromStores).pipe(
              Layer.provideMerge(stores),
            );
      const services = yield* Layer.build(serviceLayer);
      const sink = Context.get(
        services,
        kind === "upstream" ? oracle.module.EventSinkV2 : EventSink.EventSinkV2,
      );
      const events = Context.get(services, EventStore.EventStoreV2);
      const outbox = Context.get(services, EffectOutbox.EffectOutboxV2);
      const receipts = Context.get(services, CommandReceiptStore.CommandReceiptStoreV2);
      const projects = Context.get(services, ProjectStore.ProjectStoreV2);
      const positions = Context.get(services, TurnItemPositionStore.TurnItemPositionStoreV2);
      const transaction = (body) =>
        client ? client.withTransaction(body) : sql.withTransaction(body);
      const inTransaction = client
        ? client.isInTransaction
        : Effect.serviceOption(sql.transactionService).pipe(Effect.map(Option.isSome));
      const snapshot = Effect.gen(function* () {
        const result = {};
        for (const table of tables) {
          if (sql) result[table] = ordered(yield* sql.unsafe(`SELECT * FROM ${table}`));
          else {
            const rows = [];
            let after = null;
            do {
              const page = yield* client.scan(table, { after, limit: 256 });
              for (const { row } of page.rows)
                rows.push(
                  Object.fromEntries(
                    Object.entries(row).map(([key, cell]) => [
                      key,
                      cell.type === "null"
                        ? null
                        : cell.type === "integer"
                          ? Number(cell.value)
                          : cell.value,
                    ]),
                  ),
                );
              after = page.next;
            } while (after !== null);
            result[table] = ordered(rows);
          }
        }
        return result;
      });
      const publications = [];
      const originalPublish = events.publishCommitted;
      events.publishCommitted = (records) =>
        Effect.gen(function* () {
          Assert.equal(yield* inTransaction, false, "Publication must follow the root commit");
          for (const record of records) {
            const id = record.event?.id ?? record.eventId;
            const durable = client
              ? (yield* client.get("orchestration_events", [
                  { type: "integer", value: String(record.sequence) },
                ]))?.event_id.value
              : (yield* sql`SELECT event_id FROM orchestration_events WHERE sequence = ${record.sequence}`)[0]
                  ?.event_id;
            Assert.equal(durable, id, "Published event must already be durable");
            publications.push({ sequence: record.sequence, id });
          }
          yield* originalPublish(records);
        });
      return yield* use({
        sink,
        events,
        outbox,
        receipts,
        projects,
        positions,
        transaction,
        client,
        sql,
        snapshot,
        publications,
        inTransaction,
      }).pipe(Effect.timeout("30 seconds"));
    });
  return kind === "redb"
    ? withNative(
        executable,
        target,
        (client) => at(fixedTime, run(client).pipe(Effect.provide(SecureRandom.layerWebCrypto))),
        faults ? (database) => faultTransport(database, faults) : undefined,
      )
    : withSqlite(filename, (sql) => at(fixedTime, run(undefined, sql)));
}

function exercise({ sink, events, outbox, receipts, projects, snapshot, publications }) {
  return Effect.gen(function* () {
    const result = {};
    result.project = yield* sink.commitProjectCommand(projectCommand("project:create"));
    Assert.equal(result.project.committed, true);
    result.created = yield* sink.commitCommand(
      command("create", sampleEvents, [pending("initial-effect")]),
    );
    Assert.equal(result.created.committed, true);
    Assert.equal(result.created.storedEvents.length, sampleEvents.length);
    result.replay = yield* sink.commitCommand(
      command("create", [detached("must-not-replay")], [pending("must-not-replay")]),
    );
    Assert.equal(result.replay.committed, false);
    Assert.deepEqual(result.replay.storedEvents, result.created.storedEvents);
    Assert.ok(Option.isNone(yield* outbox.get("must-not-replay")));
    result.projectReplay = yield* sink.commitProjectCommand(
      projectCommand("project:create", "ignored-project"),
    );
    Assert.equal(result.projectReplay.committed, false);
    yield* expectFailure(
      sink.commitProjectCommand({ ...projectCommand("collision"), commandId: "command:create" }),
    );
    yield* expectFailure(
      sink.commitCommand({
        ...command("collision", [detached("must-not-collide")]),
        commandId: "command:project:create",
      }),
    );
    result.rejected = yield* sink.commitRejectedCommand({
      commandId: "command:rejected",
      threadId: "thread:main",
      commandType: "fixture.rejected",
      rejectedAt: fixedDateTime,
      error: "controlled rejection",
    });
    result.rejectedAgain = yield* sink.commitRejectedCommand({
      commandId: "command:rejected",
      threadId: "thread:other",
      commandType: "changed",
      rejectedAt: fixedDateTime,
      error: "changed",
    });
    Assert.deepEqual(result.rejectedAgain, result.rejected);
    result.projectRejected = yield* sink.commitRejectedProjectCommand({
      commandId: "command:project-rejected",
      projectId: "project:main",
      commandType: "project.rejected",
      rejectedAt: fixedDateTime,
      error: "controlled rejection",
    });
    result.projectRejectedAgain = yield* sink.commitRejectedProjectCommand({
      commandId: "command:project-rejected",
      projectId: "ignored-project",
      commandType: "changed",
      rejectedAt: fixedDateTime,
      error: "changed",
    });
    Assert.deepEqual(result.projectRejectedAgain, result.projectRejected);

    const runGuard = {
      threadId: "thread:main",
      runId: "run:main",
      activeAttemptId: "attempt:main",
      expectedStatus: "running",
    };
    result.runStale = yield* sink.writeIfRunCurrent({
      ...runGuard,
      activeAttemptId: "attempt:stale",
      events: [detached("stale-run")],
    });
    Assert.equal(result.runStale.committed, false);
    result.runCurrent = yield* sink.writeIfRunCurrent({
      ...runGuard,
      events: [detached("current-run")],
      effects: [pending("current-effect")],
    });
    Assert.equal(result.runCurrent.committed, true);
    const owner = {
      providerThreadId: "provider:main",
      runId: "run:main",
      activeAttemptId: "attempt:main",
      expectedLastRunOrdinal: 1,
    };
    result.ownerStale = yield* sink.writeIfProviderThreadOwner({
      ...owner,
      expectedLastRunOrdinal: 2,
      events: [detached("stale-owner")],
    });
    Assert.equal(result.ownerStale.committed, false);
    result.ownerCurrent = yield* sink.writeIfProviderThreadOwner({
      ...owner,
      events: [detached("current-owner")],
    });
    Assert.equal(result.ownerCurrent.committed, true);
    result.rowStale = yield* sink.writeIfProviderThreadUnchanged({
      expected: { ...payloads.providerThread, lastRunOrdinal: 2 },
      events: [detached("stale-row")],
    });
    Assert.equal(result.rowStale.committed, false);
    result.rowCurrent = yield* sink.writeIfProviderThreadUnchanged({
      expected: payloads.providerThread,
      events: [detached("current-row")],
    });
    Assert.equal(result.rowCurrent.committed, true);
    result.nativeOwned = yield* sink.write({
      unlessNativeThreadOwned: { ...payloads.providerThread, appThreadId: "thread:other" },
      events: [detached("owned-native")],
    });
    Assert.deepEqual(result.nativeOwned, []);
    result.nativeSameOwner = yield* sink.write({
      unlessNativeThreadOwned: payloads.providerThread,
      events: [detached("same-native-owner")],
    });
    Assert.equal(result.nativeSameOwner.length, 1);

    result.pullRequest = yield* sink.write({ events: [pullRequestLinkUpdate] });
    result.position = yield* sink.write({
      events: [
        makeEvent(
          "turn-item.updated",
          { ...jsonPayloads.turnItem, ordinal: 999999 },
          { id: "position:repeat" },
        ),
      ],
    });
    Assert.equal(
      result.position[0].event.payload.ordinal,
      result.created.storedEvents.find((record) => record.event.type === "turn-item.updated").event
        .payload.ordinal,
    );
    const beforeConstraint = yield* snapshot;
    yield* expectFailure(
      sink.commitCommand(
        command(
          "duplicate-run",
          [
            makeEvent(
              "run.created",
              { ...jsonPayloads.run, id: "run:duplicate" },
              { id: "duplicate-run:event" },
            ),
          ],
          [pending("duplicate-run:effect")],
        ),
      ),
    );
    Assert.deepEqual(
      yield* snapshot,
      beforeConstraint,
      "A projection uniqueness failure must undo receipt and event allocation",
    );

    const cohort = { disposition: "open", nextGeneration: 1, delivery: null };
    yield* sink.write({
      events: [
        makeEvent(
          "run.updated",
          {
            ...jsonPayloads.run,
            delegatedCompletion: cohort,
            restartCancelledBackgroundWork: [{ kind: "shell", label: "Fixture shell" }],
          },
          { id: "sparse:run" },
        ),
        makeEvent(
          "subagent.updated",
          {
            ...jsonPayloads.subagent,
            completionDelivery: { state: "pending", observedByRunId: null },
          },
          { id: "sparse:subagent" },
        ),
        makeEvent(
          "provider-turn.updated",
          { ...jsonPayloads.providerTurn, tokenUsage: { usedTokens: 3, updatedAt: fixedTime } },
          { id: "sparse:turn" },
        ),
      ],
    });
    result.sparse = yield* sink.write({
      events: [
        makeEvent("run.updated", jsonPayloads.run, { id: "sparse:run:omitted" }),
        makeEvent("subagent.updated", jsonPayloads.subagent, { id: "sparse:subagent:omitted" }),
        makeEvent("provider-turn.updated", jsonPayloads.providerTurn, {
          id: "sparse:turn:omitted",
        }),
      ],
    });
    result.delegatedOnly = yield* sink.writeWithEffects({
      updateDelegatedCompletionOnly: true,
      events: [
        makeEvent(
          "run.updated",
          {
            ...jsonPayloads.run,
            status: "failed",
            delegatedCompletion: { ...cohort, nextGeneration: 2 },
          },
          { id: "delegated-only:event" },
        ),
      ],
      effects: [],
    });
    Assert.equal(
      result.delegatedOnly[0].event.payload.status,
      "running",
      "Delegated-only merge must retain the stored lifecycle",
    );
    Assert.equal(
      result.delegatedOnly[0].event.payload.restartCancelledBackgroundWork[0].kind,
      "shell",
    );
    result.background = yield* sink.write({
      events: [
        makeEvent(
          "run.background-work-cancelled",
          {
            runId: "run:main",
            restartCancelledBackgroundWork: [
              { kind: "task", label: "Fixture task", awaited: true },
            ],
          },
          { id: "background:cancelled" },
        ),
      ],
    });
    yield* sink.write({
      events: [
        makeEvent(
          "runtime-request.updated",
          {
            ...jsonPayloads.request,
            status: "resolved",
            resolvedAt: fixedTime,
          },
          { id: "request:resolved" },
        ),
      ],
    });
    result.staleCancellation = yield* sink.write({
      guardPendingUserInputCancellations: true,
      events: [
        makeEvent(
          "runtime-request.updated",
          { ...jsonPayloads.request, status: "cancelled", resolvedAt: fixedTime },
          { id: "request:stale-cancel" },
        ),
        makeEvent(
          "node.updated",
          { ...jsonPayloads.node, status: "cancelled" },
          { id: "node:stale-cancel" },
        ),
        detached("cancellation:retained"),
      ],
    });
    Assert.deepEqual(
      result.staleCancellation.map((row) => row.event.id),
      ["cancellation:retained"],
    );
    const beforeRollback = yield* snapshot;
    const publishedBefore = publications.length;
    yield* expectFailure(
      sink.withProviderBatch(
        Effect.gen(function* () {
          yield* sink.commitProjectCommand(
            projectCommand("rollback:project", "project:rolled-back"),
          );
          yield* sink.commitCommand(
            command(
              "rollback:command",
              [
                makeEvent(
                  "turn-item.updated",
                  { ...jsonPayloads.turnItem, id: "item:rolled-back" },
                  { id: "rollback:position" },
                ),
                detached("rollback:event"),
              ],
              [pending("rollback:effect")],
            ),
          );
          return yield* Effect.fail("controlled whole-boundary rollback");
        }),
      ),
    );
    Assert.deepEqual(yield* snapshot, beforeRollback);
    Assert.equal(publications.length, publishedBefore);
    Assert.ok(Option.isNone(yield* receipts.getByCommandId("command:rollback:command")));
    Assert.ok(Option.isNone(yield* projects.get("project:rolled-back")));

    result.batch = yield* sink.withProviderBatch(
      Effect.gen(function* () {
        const first = yield* sink.write({ events: [detached("batch:first")] });
        Assert.equal(publications.length, publishedBefore);
        const second = yield* sink.withProviderBatch(
          sink.writeWithEffects({
            events: [detached("batch:second")],
            effects: [pending("batch:effect")],
          }),
        );
        Assert.equal(publications.length, publishedBefore);
        return [...first, ...second];
      }),
    );
    Assert.deepEqual(
      publications.slice(publishedBefore).map((row) => row.id),
      ["batch:first", "batch:second"],
    );
    const waiting = yield* Effect.forkScoped(outbox.awaitCancellation("initial-effect"));
    result.cancelled = yield* sink.commitCommand({
      ...command("cancel", [detached("cancel:event")]),
      cancelUnsettledEffects: {
        effectTypes: ["terminal.cleanup"],
        reason: "controlled cancellation",
      },
    });
    Assert.ok(result.cancelled.cancelledEffectCount >= 1);
    yield* Fiber.join(waiting);
    Assert.equal((yield* outbox.get("initial-effect")).value.status, "cancelled");

    // Subscribe before the write; a real replay/live subscription must see its commit exactly once.
    const ready = yield* Deferred.make();
    const originalLatest = events.latestSequence;
    const afterSequence = yield* sink.latestSequence();
    events.latestSequence = (input) =>
      originalLatest(input).pipe(Effect.tap(() => Deferred.succeed(ready, undefined)));
    const live = yield* Effect.forkScoped(
      collect(
        sink
          .stream({
            afterSequence,
            threadId: "thread:main",
            eventType: "provider-session.detached",
          })
          .pipe(Stream.take(1)),
      ),
    );
    yield* Deferred.await(ready);
    events.latestSequence = originalLatest;
    result.liveWrite = yield* sink.write({ events: [detached("live:event")] });
    result.live = yield* Fiber.join(live);
    Assert.deepEqual(result.live, result.liveWrite);
    const providerSource = Stream.fromArray(
      Array.from({ length: 12 }, (_, index) => ({ type: "message.updated", index })),
    );
    yield* sink.consumeProviderEvents(
      providerSource,
      (raw) => sink.write({ events: [detached(`provider:${raw.index}`)] }).pipe(Effect.asVoid),
      Effect.succeed(false),
      Effect.succeed(true),
    );
    result.byCommand = yield* collect(sink.readByCommandId({ commandId: "command:create" }));
    Assert.deepEqual(result.byCommand, result.created.storedEvents);
    result.replayAll = yield* collect(events.read());
    result.latest = yield* sink.latestSequence();
    result.latestThread = yield* sink.latestSequence({ threadId: "thread:main" });
    result.filtered = yield* collect(
      sink
        .stream({ threadId: "thread:main", eventType: "provider-session.detached", bounded: true })
        .pipe(
          Stream.take(
            result.replayAll.filter((row) => row.event.type === "provider-session.detached").length,
          ),
        ),
    );
    result.snapshot = yield* snapshot;
    result.publications = publications;
    Assert.deepEqual(
      publications.map((row) => row.sequence),
      [...publications.map((row) => row.sequence)].sort((a, b) => a - b),
    );
    return normalized(result);
  });
}

function nativeOwnerCases({ sink, client, snapshot, publications }) {
  return Effect.gen(function* () {
    const initial = yield* snapshot;
    yield* expectFailure(
      client.withTransaction(
        Effect.gen(function* () {
          yield* sink.commitCommand(
            command(
              "ambient:rollback",
              [detached("ambient:rollback")],
              [pending("ambient:rollback")],
            ),
          );
          Assert.equal(publications.length, 0);
          return yield* Effect.fail("ambient owner rolled back");
        }),
      ),
    );
    Assert.equal(publications.length, 0);
    Assert.deepEqual(yield* snapshot, initial);
    yield* client.withTransaction(
      Effect.gen(function* () {
        yield* sink.write({ events: [detached("ambient:first")] });
        yield* expectFailure(
          client.withTransaction(
            Effect.gen(function* () {
              yield* sink.writeWithEffects({
                events: [detached("ambient:discarded")],
                effects: [pending("ambient:discarded")],
              });
              return yield* Effect.fail("nested owner rolled back");
            }),
          ),
        );
        yield* sink.withProviderBatch(sink.write({ events: [detached("ambient:last")] }));
        Assert.equal(
          publications.length,
          0,
          "No released savepoint may publish before its root owner commits",
        );
      }),
    );
    Assert.deepEqual(
      publications.map((row) => row.id),
      ["ambient:first", "ambient:last"],
    );
    const prior = publications.length;
    yield* sink.withProviderBatch(
      Effect.gen(function* () {
        yield* expectFailure(
          client.withTransaction(
            sink
              .write({ events: [detached("batch:savepoint:discarded")] })
              .pipe(Effect.andThen(Effect.fail("snapshot rolled back"))),
          ),
        );
        yield* sink.write({ events: [detached("batch:savepoint:retained")] });
      }),
    );
    Assert.deepEqual(
      publications.slice(prior).map((row) => row.id),
      ["batch:savepoint:retained"],
    );
  });
}

function nativeFaultCases(faults) {
  return ({ sink, client, snapshot, publications, receipts, projects, outbox }) =>
    Effect.gen(function* () {
      let callbackCount = 0;
      faults.busyBegins = 2;
      yield* sink.withProviderBatch(
        Effect.sync(() => {
          callbackCount += 1;
        }).pipe(Effect.andThen(sink.write({ events: [detached("busy:event")] }))),
      );
      Assert.equal(callbackCount, 1, "Writer acquisition retry must never rerun normalization");
      Assert.equal(faults.busyBegins, 0);
      const beforeUnknown = publications.length;
      faults.frames.length = 0;
      faults.drop = "application_commit";
      yield* expectFailure(
        sink.withProviderBatch(
          Effect.gen(function* () {
            yield* sink.commitProjectCommand(projectCommand("unknown:project", "project:unknown"));
            yield* sink.commitCommand(
              command(
                "unknown:command",
                [
                  makeEvent(
                    "turn-item.updated",
                    { ...jsonPayloads.turnItem, id: "item:unknown" },
                    { id: "unknown:position" },
                  ),
                  detached("unknown:event"),
                ],
                [pending("unknown:effect")],
              ),
            );
          }),
        ),
      );
      Assert.equal(
        faults.frames.filter((frame) => frame.operation === "application_commit").length,
        1,
      );
      Assert.equal(
        publications.length,
        beforeUnknown,
        "Unknown durability must not publish success",
      );
      Assert.ok(Option.isSome(yield* receipts.getByCommandId("command:unknown:command")));
      Assert.ok(Option.isSome(yield* projects.get("project:unknown")));
      Assert.ok(Option.isSome(yield* outbox.get("unknown:effect")));
      const replay = yield* sink.commitCommand(
        command("unknown:command", [detached("unknown:must-not-retry")]),
      );
      Assert.equal(replay.committed, false);
      Assert.deepEqual(
        replay.storedEvents.map((row) => row.event.id),
        ["unknown:position", "unknown:event"],
      );
      Assert.equal(publications.length, beforeUnknown);
      for (const operation of [
        "application_projections_apply",
        "application_release",
        "application_rollback_to",
      ]) {
        const before = yield* snapshot;
        const published = publications.length;
        faults.frames.length = 0;
        faults.drop = operation;
        const write = sink.commitCommand(
          command(
            `fault:${operation}`,
            [detached(`fault:${operation}`)],
            [pending(`fault:${operation}`)],
          ),
        );
        const body =
          operation === "application_projections_apply"
            ? sink.withProviderBatch(write)
            : client.withTransaction(
                (operation === "application_release"
                  ? write
                  : client.withTransaction(
                      write.pipe(Effect.andThen(Effect.fail("rollback this scope"))),
                    )
                ).pipe(Effect.catch(() => Effect.void)),
              );
        yield* expectFailure(body);
        Assert.equal(
          faults.drop,
          undefined,
          `The real ${operation} response must have been discarded`,
        );
        Assert.equal(publications.length, published);
        Assert.deepEqual(
          yield* snapshot,
          before,
          `Uncertain ${operation} must discard the complete transaction`,
        );
        Assert.equal(
          faults.frames.filter((frame) => frame.operation === "application_events_append").length,
          1,
          "An uncertain write must not be replayed through either backend",
        );
      }
    });
}

try {
  const source = Path.join(fixture.root, "source.sqlite");
  const retainedPath = Path.join(fixture.root, "retained.sqlite");
  const upstreamPath = Path.join(fixture.root, "upstream.sqlite");
  const exported = Path.join(fixture.root, "source.ndjson");
  await createMigratedSqlite(source);
  const sourceHash = hash(await FS.readFile(source));
  await FS.copyFile(source, retainedPath);
  await FS.copyFile(source, upstreamPath);
  exportSqliteState({ source, destination: exported });
  nativeRequest(executable, fixture, "initialize", {
    operation: "import_sqlite_application",
    path: exported,
  });
  const expected = await withServices("upstream", upstreamPath, exercise);
  const retained = await withServices("sqlite", retainedPath, exercise);
  Assert.deepEqual(
    retained,
    expected,
    "The shared EventSink algorithm must retain immutable upstream SQLite behavior",
  );
  const actual = await withServices("redb", source, exercise);
  Assert.deepEqual(
    actual,
    expected,
    "The real native EventSink must commit the same events, receipts, projections, positions, projects and outbox rows",
  );
  checks.push(
    "Every EventSink public member runs through the immutable upstream, retained SQLite and real native compositions",
    "All 16 projection entity families, provider-session bindings and projection cursor agree as complete stored rows",
    "Global receipts reject cross-aggregate command collisions and retain accepted/rejected command replay without another mutation",
    "Run, provider-thread owner, exact-payload and native-session ownership guards execute inside the same reserved writer",
    "Turn-position normalization and shared pull-request normalization agree with upstream projections",
    "A failed provider batch rolls back events, command receipts, projects, positions, projections and outbox together",
    "Nested provider batches publish in sequence only after durable root commit, including outbox cancellation signals",
    "The actual provider snapshot coordinator, live stream, filtered replay, command replay and high-water reads retain ordering",
  );
  await withServices("redb", source, ({ snapshot, sink }) =>
    Effect.gen(function* () {
      Assert.deepEqual(yield* snapshot, expected.snapshot);
      Assert.equal(yield* sink.latestSequence(), expected.latest);
    }),
  );
  checks.push(
    "Reopening the complete candidate service preserves every committed application row and all managed indexes",
  );
  checks.push(
    ...(await checkProjectionCodecs({ executable, seedSource: retainedPath, withServices })),
  );
  await withServices("redb", source, nativeOwnerCases);
  checks.push(
    "Native ambient transactions defer publications to the root owner and discard only callbacks belonging to rolled-back savepoints",
  );
  const faults = { frames: [], drop: undefined, busyBegins: 0 };
  await withServices("redb", source, nativeFaultCases(faults), fixture, faults);
  checks.push(
    "Known writer-busy responses retry before normalization; lost real commit, apply, savepoint-release and rollback acknowledgements never replay or publish uncertain work",
  );
  Assert.equal(hash(await FS.readFile(source)), sourceHash);
  await finishProof({
    executable,
    fixture,
    proofPath,
    checks,
    sourceHash,
    oracles: [oracle.source, projectionOracle.source],
    sourcePaths: [
      "apps/server/src/orchestration-v2/EventSink.ts",
      "apps/server/src/orchestration-v2/NativeEventSink.ts",
      "apps/server/src/orchestration-v2/ProjectionCommitStore.ts",
      "apps/server/src/persistence/SqliteProjectionCommitStore.ts",
      "apps/server/src/persistence/NativeProjectionCommitStore.ts",
      "apps/server/src/persistence/PersistenceTransactions.ts",
      "apps/server/src/persistence/RedbClient.ts",
      "apps/server/src/persistence/redb_projection_commit.rs",
      "tools/check-redb-event-sink.mjs",
      "tools/redb-event-sink-fixtures.mjs",
      "tools/redb-domain-fixture.mjs",
      "tools/redb-projection-codec-cases.mjs",
    ],
  });
} finally {
  await fixture.close();
}
