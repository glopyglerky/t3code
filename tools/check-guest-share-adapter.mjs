#!/usr/bin/env node
import * as NodeAssert from "node:assert/strict";
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";
import * as PlatformCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  CommandId,
  EventId,
  GuestMessageId,
  GuestShareId,
  MessageId,
  OrchestrationV2DomainEventJson,
  OrchestrationV2ThreadProjectionJson,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ThreadId,
  TurnItemId,
} from "@t3tools/contracts";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Cause from "effect/Cause";
import * as Clock from "effect/Clock";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as References from "effect/References";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/sql/SqlClient";
import * as ServerSecretStore from "../apps/server/src/auth/ServerSecretStore.ts";
import { CloudManagedEndpointRuntime } from "../apps/server/src/cloud/ManagedEndpointRuntime.ts";
import * as ServerConfig from "../apps/server/src/config.ts";
import * as GuestShareService from "../apps/server/src/guest/GuestShareService.ts";
import { CodexProviderCapabilitiesV2 } from "../apps/server/src/orchestration-v2/Adapters/CodexAdapterV2.ts";
import * as CommandOutputHub from "../apps/server/src/orchestration-v2/CommandOutputHub.ts";
import * as EffectOutbox from "../apps/server/src/orchestration-v2/EffectOutbox.ts";
import * as EffectWorker from "../apps/server/src/orchestration-v2/EffectWorker.ts";
import * as EventSink from "../apps/server/src/orchestration-v2/EventSink.ts";
import * as EventStore from "../apps/server/src/orchestration-v2/EventStore.ts";
import * as GuestShareDecider from "./oracles/GuestShareDecider.ts";
import * as NativeGuestShare from "../apps/server/src/orchestration-v2/NativeGuestShare.ts";
import * as Orchestrator from "../apps/server/src/orchestration-v2/Orchestrator.ts";
import * as ProjectionStore from "../apps/server/src/orchestration-v2/ProjectionStore.ts";
import * as ProviderAdapterRegistry from "../apps/server/src/orchestration-v2/ProviderAdapterRegistry.ts";
import * as ResourceCleanupService from "../apps/server/src/orchestration-v2/ResourceCleanupService.ts";
import * as ProjectionReplay from "../apps/server/src/orchestration-v2/testkit/ProjectionReplay.ts";
import * as ProviderReplayHarness from "../apps/server/src/orchestration-v2/testkit/ProviderReplayHarness.ts";
import * as SqlitePersistence from "../apps/server/src/persistence/Sqlite.ts";
import * as PreviewManager from "../apps/server/src/preview/Manager.ts";
import * as ProcessRunner from "../apps/server/src/processRunner.ts";
import * as ProviderRegistry from "../apps/server/src/provider/ProviderRegistry.ts";
import * as ScheduledTaskService from "../apps/server/src/scheduledTasks/ScheduledTaskService.ts";
import * as ServerSettings from "../apps/server/src/serverSettings.ts";
import * as AnalyticsService from "../apps/server/src/telemetry/AnalyticsService.ts";
import * as TerminalManager from "../apps/server/src/terminal/Manager.ts";

NodeAssert.ok(process.argv[2], "Pass the native guest-share executable");
const executable = NodePath.resolve(process.argv[2]);
const proofPath = process.argv[3];
const baselineOnly = process.argv.includes("--baseline-only");
const root = NodeURL.fileURLToPath(new URL("../", import.meta.url));
const temporary = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "t3ls-guest-adapter-"));
const scenarios = [];
const nowMillis = Date.parse("2026-11-08T12:00:00.123Z");
const now = DateTime.makeUnsafe(nowMillis);
const instanceId = ProviderInstanceId.make("codex");
const threadId = ThreadId.make("thread:guest-native");
const shareId = GuestShareId.make("share:guest-native");
const encodeEvent = Schema.encodeSync(OrchestrationV2DomainEventJson);
const encodeProjection = Schema.encodeSync(OrchestrationV2ThreadProjectionJson);
const PNG = Uint8Array.of(0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a, 0, 0, 0, 13);
const MIB = 1024 * 1024;
const unicodeUploadName = "a".repeat(254) + "🦀";
const storedUnicodeName = "a".repeat(254) + "\ud83e";
const unicodeUploadMime = "application/x-\ud800";
const utf16Units = (value) =>
  Array.from({ length: value.length }, (_, index) => value.charCodeAt(index));
const adapter = {
  instanceId,
  driver: ProviderDriverKind.make("codex"),
  getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
  planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" }),
  openSession: () => Effect.die("This guest-share fixture must never open a provider session"),
};

function serviceLayer(name, filename, copyCalls) {
  const database =
    filename === undefined
      ? SqlitePersistence.layerMemory
      : SqlitePersistence.layerFromPath(filename);
  const stores = Layer.mergeAll(EventStore.layer, ProjectionStore.layer, EffectOutbox.layer).pipe(
    Layer.provideMerge(database),
  );
  const config = ServerConfig.layerTest(temporary, NodePath.join(temporary, name));
  const cleanup = ResourceCleanupService.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        Layer.mock(TerminalManager.TerminalManager)({ close: () => Effect.void }),
        Layer.mock(PreviewManager.PreviewManager)({ close: () => Effect.void }),
        Layer.mock(ScheduledTaskService.ScheduledTaskService)({
          deleteForThread: () => Effect.void,
        }),
      ),
    ),
    Layer.provide(config),
    Layer.provide(NodeServices.layer),
  );
  const orchestration = Layer.mergeAll(
    stores,
    ProjectionReplay.layer.pipe(Layer.provide(stores)),
    ProviderReplayHarness.layerWithRegistry(
      { name: `native-guest-${name}` },
      ProviderAdapterRegistry.layerFromAdapters([adapter]),
      { databaseLayer: database, runEffectWorker: false },
    ).pipe(Layer.provide(cleanup)),
  );
  const guestService =
    copyCalls === undefined
      ? GuestShareService.layer
      : GuestShareService.layer.pipe(
          Layer.provide(
            Layer.effect(
              FileSystem.FileSystem,
              Effect.gen(function* () {
                const actual = yield* FileSystem.FileSystem;
                return {
                  ...actual,
                  copyFile: (...args) => {
                    copyCalls.push(args);
                    return actual.copyFile(...args);
                  },
                };
              }),
            ),
          ),
        );
  return guestService.pipe(
    Layer.provideMerge(CommandOutputHub.layer.pipe(Layer.provideMerge(orchestration))),
    Layer.provide(
      Layer.mock(ProviderRegistry.ProviderRegistry)({ getProviders: Effect.succeed([]) }),
    ),
    Layer.provideMerge(ServerSettings.layerTest({ guestSharingEnabled: true })),
    Layer.provide(
      Layer.mock(CloudManagedEndpointRuntime)({
        inspectConnector: Effect.succeed({
          pid: 1,
          readyConnections: 1,
          hostname: "env.t3.test",
          protocol: "http2",
        }),
      }),
    ),
    Layer.provide(
      Layer.mock(AnalyticsService.AnalyticsService)({
        record: () => Effect.void,
        flush: Effect.void,
        recordOwners: () => Effect.void,
      }),
    ),
    Layer.provide(ServerSecretStore.layer),
    Layer.provideMerge(config),
    Layer.provideMerge(Layer.mergeAll(NodeServices.layer, PlatformCrypto.layer)),
  );
}

async function run(effect, selection, name, filename, direct = false, copyCalls) {
  const clock = Effect.runSync(Clock.Clock);
  return Effect.runPromise(
    effect.pipe(
      Effect.scoped,
      Effect.provide(direct ? NodeServices.layer : serviceLayer(name, filename, copyCalls)),
      Effect.provideService(
        HostProcess.Environment,
        selection === undefined
          ? { T3_CODE_LS_IMPLEMENTATION: "upstream" }
          : { T3_CODE_LS_GUEST_SHARE_EXECUTABLE: selection },
      ),
      Effect.provideService(HostProcess.WorkingDirectory, temporary),
      Effect.provideService(
        Clock.Clock,
        Object.assign(Object.create(clock), {
          currentTimeMillis: Effect.succeed(nowMillis),
          currentTimeMillisUnsafe: () => nowMillis,
        }),
      ),
      Effect.provideService(References.MinimumLogLevel, "Warn"),
    ),
  );
}

const createThread = Effect.fn("checkGuestShareAdapter.createThread")(function* (id = threadId) {
  const orchestrator = yield* Orchestrator.OrchestratorV2;
  yield* orchestrator.dispatch({
    type: "thread.create",
    commandId: CommandId.make(`create:${id}`),
    threadId: id,
    projectId: ProjectId.make("project:guest-native"),
    title: "Native guest fixture",
    modelSelection: { instanceId, model: "gpt-5.1-codex" },
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    createdBy: "user",
    creationSource: "web",
  });
  return orchestrator;
});

const join = Effect.fn("checkGuestShareAdapter.join")(function* (id = threadId) {
  const orchestrator = yield* createThread(id);
  const guestShares = yield* GuestShareService.GuestShareService;
  const { url } = yield* guestShares.create({ threadId: id });
  NodeAssert.ok(url.startsWith("https://env.t3.test/guest#"));
  const credential = url.slice(url.indexOf("#") + 1);
  const session = yield* guestShares.redeem({ credential, name: "Sam 💬" });
  return { orchestrator, guestShares, credential, ...session };
});

const projection = Effect.fn("checkGuestShareAdapter.projection")(function* (id = threadId) {
  return yield* (yield* ProjectionStore.ProjectionStoreV2).getThreadProjection(id);
});

const snapshot = Effect.fn("checkGuestShareAdapter.snapshot")(function* (ids = [threadId]) {
  const sql = yield* SqlClient.SqlClient;
  const store = yield* EventStore.EventStoreV2;
  return {
    events: (yield* store.read().pipe(Stream.runCollect)).map((stored) => ({
      sequence: stored.sequence,
      event: encodeEvent(stored.event),
    })),
    projections: yield* Effect.forEach(ids, (id) =>
      projection(id).pipe(Effect.map(encodeProjection)),
    ),
    receipts: yield* sql`SELECT * FROM orchestration_command_receipts ORDER BY command_id`,
    outbox: yield* sql`SELECT * FROM orchestration_v2_effect_outbox ORDER BY effect_id`,
  };
});

function assertRejected(before, after, commandId) {
  NodeAssert.deepEqual(after.events, before.events, "rejected native decision wrote domain events");
  NodeAssert.deepEqual(
    after.projections,
    before.projections,
    "rejected decision changed projections",
  );
  NodeAssert.deepEqual(after.outbox, before.outbox, "rejected decision enqueued effects");
  const previous = new Set(before.receipts.map((row) => row.command_id));
  NodeAssert.deepEqual(
    after.receipts.filter((row) => previous.has(row.command_id)),
    before.receipts,
  );
  const added = after.receipts.filter((row) => !previous.has(row.command_id));
  NodeAssert.equal(added.length, 1, "one rejected command receipt must persist");
  NodeAssert.equal(added[0].status, "rejected");
  if (commandId !== undefined) NodeAssert.equal(added[0].command_id, commandId);
  return added[0];
}

const rejectedDispatch = Effect.fn("checkGuestShareAdapter.rejectedDispatch")(function* (command) {
  const orchestrator = yield* Orchestrator.OrchestratorV2;
  const before = yield* snapshot();
  const error = yield* Effect.flip(orchestrator.dispatch(command));
  const after = yield* snapshot();
  assertRejected(before, after, command.commandId);
  const replay = yield* Effect.flip(orchestrator.dispatch(command));
  NodeAssert.equal(replay._tag, "OrchestratorCommandPreviouslyRejectedError");
  NodeAssert.deepEqual(
    yield* snapshot(),
    after,
    "rejected receipt must replay without side effects",
  );
  return error;
});

const uploads = Effect.fn("checkGuestShareAdapter.uploads")(function* (
  name,
  size = PNG.length,
  contentType = "image/png",
  header = PNG,
) {
  const fs = yield* FileSystem.FileSystem;
  const directory = yield* fs.makeTempDirectoryScoped({ prefix: "native-guest-upload-" });
  const bytes = new Uint8Array(size);
  bytes.set(header.subarray(0, size));
  const path = NodePath.join(directory, "upload");
  yield* fs.writeFile(path, bytes);
  return [{ path, name, contentType }];
});

const storedFiles = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;
  const { attachmentsDir } = yield* ServerConfig.ServerConfig;
  return (yield* fs.readDirectory(attachmentsDir)).sort();
});

function ownerSend(id, guestMessage, dispatchMode = "defer_start") {
  return {
    type: "message.dispatch",
    commandId: CommandId.make(id),
    threadId,
    messageId: MessageId.make(id),
    ...(guestMessage === undefined ? {} : { guestMessageId: guestMessage.id }),
    text: "Owner text must not replace a guest's text",
    attachments: guestMessage?.attachments ?? [],
    dispatchMode: { type: dispatchMode },
    createdBy: "user",
    creationSource: "web",
  };
}

const persistQueuedItem = Effect.fn("checkGuestShareAdapter.persistQueuedItem")(function* (run) {
  const before = yield* projection();
  const original = before.turnItems.find((item) => item.type === "user_message");
  const message = before.messages.find((message) => message.id === run.userMessageId);
  NodeAssert.ok(original);
  NodeAssert.ok(message);
  NodeAssert.equal(
    before.turnItems.find(
      (item) => item.type === "user_message" && item.messageId === run.userMessageId,
    ),
    undefined,
  );
  yield* (yield* EventSink.EventSinkV2).write({
    events: [
      {
        id: EventId.make(`event:legacy:${run.userMessageId}`),
        type: "turn-item.updated",
        threadId,
        runId: run.id,
        occurredAt: now,
        payload: {
          ...original,
          id: TurnItemId.make(`item:legacy:${run.userMessageId}`),
          runId: run.id,
          nodeId: null,
          providerThreadId: null,
          ordinal: original.ordinal + 1000,
          messageId: message.id,
          text: message.text,
          context: message.context,
          attachments: message.attachments,
        },
      },
    ],
  });
});

async function fixture(name, body) {
  const path = NodePath.join(temporary, name);
  await NodeFSP.writeFile(path, `#!${process.execPath}\n${body}\n`, { mode: 0o700 });
  return path;
}

async function lifecycle(selection, label) {
  const filename = NodePath.join(temporary, `${label}-lifecycle.sqlite`);
  const name = `${label}-lifecycle`;
  const first = await run(
    Effect.gen(function* () {
      const orchestrator = yield* createThread();
      const guestShares = yield* GuestShareService.GuestShareService;
      const { url } = yield* guestShares.create({ threadId });
      const credential = url.slice(url.indexOf("#") + 1);
      const redeemed = yield* Effect.all(
        ["Sam 💬", "Sam 💬"].map((name) =>
          guestShares.redeem({ credential, name }).pipe(Effect.exit),
        ),
        { concurrency: "unbounded" },
      );
      NodeAssert.equal(redeemed.filter(Exit.isSuccess).length, 1);
      NodeAssert.equal(redeemed.filter(Exit.isFailure).length, 1);
      const session = redeemed.find(Exit.isSuccess).value;
      yield* guestShares.check(session.token);
      NodeAssert.equal(
        (yield* Effect.flip(guestShares.redeem({ credential, name: "Eve" }))).reason,
        "used",
      );
      yield* orchestrator.dispatch({
        type: "thread.settle",
        commandId: CommandId.make("settle-before-guest"),
        threadId,
      });
      yield* guestShares.submit(
        session.token,
        { shareId: session.shareId, text: "/model 💬 guest request" },
        [
          ...(yield* uploads(unicodeUploadName)),
          ...(yield* uploads("mime-surrogate.txt", 1, unicodeUploadMime)),
        ],
      );
      const submitted = yield* projection();
      NodeAssert.equal(submitted.thread.settledOverride, null);
      NodeAssert.equal(submitted.thread.settledAt, null);
      NodeAssert.equal(DateTime.toEpochMillis(submitted.thread.unsettledAt), nowMillis);
      const message = submitted.thread.guestShare.messages[0];
      NodeAssert.equal(message.status, "pending");
      NodeAssert.equal(message.attachments[0].name, storedUnicodeName);
      NodeAssert.equal(message.attachments[0].name.length, 255);
      NodeAssert.equal(message.attachments[0].name.charCodeAt(254), 0xd83e);
      NodeAssert.equal(message.attachments[1].mimeType, unicodeUploadMime);
      yield* guestShares.attachment(session.token, message.attachments[0].id);
      const command = ownerSend("owner-send", message);
      yield* orchestrator.dispatch(command);
      const sent = yield* projection();
      NodeAssert.equal(sent.thread.guestShare.messages[0].status, "sent");
      NodeAssert.equal(sent.runs.length, 1);
      NodeAssert.equal(
        sent.messages[0].text,
        "Message from guest Sam 💬, sent by the owner:\n\n/model 💬 guest request",
      );
      NodeAssert.deepEqual(sent.messages[0].context.guest, {
        guestMessageId: message.id,
        name: "Sam 💬",
        text: message.text,
        shareId: session.shareId,
      });
      NodeAssert.deepEqual(sent.messages[0].attachments, message.attachments);
      const accepted = yield* snapshot();
      yield* orchestrator.dispatch(command);
      NodeAssert.deepEqual(
        yield* snapshot(),
        accepted,
        "accepted receipt must prevent a second send",
      );
      const duplicate = yield* rejectedDispatch(ownerSend("owner-send-again", message));
      NodeAssert.equal(duplicate.cause, `Guest message ${message.id} is not pending.`);
      NodeAssert.equal((yield* projection()).runs.length, 1);
      const events = yield* guestShares.stream(session.token);
      const beforeFiles = yield* storedFiles;
      yield* orchestrator.dispatch({
        type: "thread.guest-share.close",
        commandId: CommandId.make("close-lifecycle"),
        threadId,
      });
      const frames = Array.from(yield* Stream.runCollect(events));
      NodeAssert.equal(frames.at(-1).type, "ended");
      yield* (yield* EffectWorker.OrchestrationEffectWorkerV2).drain();
      NodeAssert.deepEqual(
        yield* storedFiles,
        beforeFiles,
        "sent attachments survive share closure",
      );
      NodeAssert.equal((yield* Effect.flip(guestShares.check(session.token))).reason, "inactive");
      return {
        state: yield* snapshot(),
        token: session.token,
        summary: {
          statuses: sent.thread.guestShare.messages.map((item) => item.status),
          text: sent.messages[0].text,
          attachmentNames: sent.messages[0].attachments.map((attachment) => attachment.name),
          attachmentMimeTypes: sent.messages[0].attachments.map((attachment) => attachment.mimeType),
          runs: sent.runs.length,
          events: (yield* snapshot()).events.map((row) => row.event.type),
          finalFrame: frames.at(-1).type,
        },
      };
    }),
    selection,
    name,
    filename,
  );
  await run(
    Effect.gen(function* () {
      NodeAssert.deepEqual(yield* snapshot(), first.state);
      NodeAssert.equal((yield* (yield* ProjectionReplay.ProjectionReplay).rebuild).valid, true);
      NodeAssert.deepEqual(
        yield* snapshot(),
        first.state,
        "replay must reproduce guest state exactly",
      );
      NodeAssert.equal(
        (yield* Effect.flip((yield* GuestShareService.GuestShareService).check(first.token)))
          .reason,
        "inactive",
      );
    }),
    selection,
    name,
    filename,
  );
  scenarios.push(
    `${label}: service lifecycle, concurrent redemption, once-only send, lone-surrogate attachment metadata, reopen and replay`,
  );
  return first.summary;
}

const limits = Effect.gen(function* () {
  const { guestShares, token, shareId } = yield* join();
  for (let index = 0; index < 2; index++) {
    yield* guestShares.submit(
      token,
      { shareId, text: `image ${index}` },
      yield* uploads(`${index}.png`, 9 * MIB),
    );
  }
  const before = yield* snapshot();
  const files = yield* storedFiles;
  const refused = yield* Effect.flip(
    guestShares.submit(
      token,
      { shareId, text: "too many bytes" },
      yield* uploads("excess.png", 9 * MIB),
    ),
  );
  NodeAssert.equal(refused.reason, "files_busy");
  assertRejected(before, yield* snapshot());
  NodeAssert.deepEqual(
    yield* storedFiles,
    files,
    "rejected native submit must remove its copied upload",
  );
  yield* guestShares.submit(token, { shareId, text: "text still fits" }, []);
  const full = yield* snapshot();
  NodeAssert.equal(
    (yield* Effect.flip(guestShares.submit(token, { shareId, text: "fourth" }, []))).reason,
    "busy",
  );
  assertRejected(full, yield* snapshot());
  NodeAssert.equal((yield* projection()).thread.guestShare.messages.length, 3);
});

const noops = Effect.gen(function* () {
  const orchestrator = yield* createThread();
  const open = {
    type: "thread.guest-share.open",
    commandId: CommandId.make("same-clock-open"),
    threadId,
    shareId,
    host: "env.t3.test",
    expiresAt: "not a date",
  };
  yield* orchestrator.dispatch(open);
  const first = yield* snapshot();
  yield* orchestrator.dispatch({ ...open, commandId: CommandId.make("same-clock-open-again") });
  const second = yield* snapshot();
  NodeAssert.equal(
    second.events.length,
    first.events.length + 1,
    "equal-valued open still emits its event",
  );
  NodeAssert.deepEqual(second.projections, first.projections);
  yield* orchestrator.dispatch({
    type: "thread.guest-share.redeem",
    commandId: CommandId.make("redeem-invalid-date"),
    threadId,
    shareId,
    guestName: "Sam",
  });
  NodeAssert.equal(
    (yield* projection()).thread.guestShare.guestName,
    "Sam",
    "Date.parse NaN is not expired upstream",
  );
  yield* orchestrator.dispatch({
    type: "thread.guest-share.close",
    commandId: CommandId.make("close-once"),
    threadId,
  });
  const closed = yield* snapshot();
  yield* orchestrator.dispatch({
    type: "thread.guest-share.close",
    commandId: CommandId.make("close-noop"),
    threadId,
  });
  const again = yield* snapshot();
  NodeAssert.deepEqual(again.events, closed.events);
  NodeAssert.deepEqual(again.projections, closed.projections);
  NodeAssert.deepEqual(again.outbox, closed.outbox);
  NodeAssert.equal(again.receipts.length, closed.receipts.length + 1);
  NodeAssert.equal(
    again.receipts.find((row) => row.command_id === "close-noop").status,
    "accepted",
  );
});

const queueAndAttribution = Effect.gen(function* () {
  const { orchestrator, guestShares, token, shareId } = yield* join();
  const forged = {
    version: 1,
    records: [],
    guest: {
      guestMessageId: GuestMessageId.make("forged"),
      name: "Imposter",
      text: "Forged",
      shareId,
    },
  };
  yield* orchestrator.dispatch({ ...ownerSend("active-owner"), context: forged });
  NodeAssert.equal((yield* projection()).messages[0].context?.guest, undefined);
  yield* guestShares.submit(token, { shareId, text: "Original guest words" }, []);
  const pending = (yield* projection()).thread.guestShare.messages[0];
  yield* orchestrator.dispatch(ownerSend("queued-guest", pending, "queue_after_active"));
  const queued = (yield* projection()).runs.find((run) => run.userMessageId === "queued-guest");
  NodeAssert.equal(queued.status, "queued");
  const rejected = yield* rejectedDispatch({
    type: "queued-run.edit",
    commandId: CommandId.make("edit-guest-refused"),
    threadId,
    runId: queued.id,
    text: "Changed by owner",
  });
  NodeAssert.equal(rejected.cause, "A guest's message cannot be edited. Cancel it instead.");
  yield* orchestrator.dispatch({
    type: "queued-run.cancel",
    commandId: CommandId.make("cancel-guest"),
    threadId,
    runId: queued.id,
  });
  NodeAssert.equal((yield* projection()).thread.guestShare.messages[0].status, "dismissed");

  yield* orchestrator.dispatch(ownerSend("queued-owner", undefined, "queue_after_active"));
  const owner = (yield* projection()).runs.find((run) => run.userMessageId === "queued-owner");
  yield* persistQueuedItem(owner);
  yield* orchestrator.dispatch({
    type: "queued-run.edit",
    commandId: CommandId.make("edit-owner"),
    threadId,
    runId: owner.id,
    text: "Owner revised",
    context: forged,
  });
  const edited = yield* projection();
  NodeAssert.equal(
    edited.messages.find((message) => message.id === "queued-owner").context.guest,
    undefined,
  );
  const item = edited.turnItems.find(
    (item) => item.type === "user_message" && item.messageId === "queued-owner",
  );
  NodeAssert.equal(item.context.guest, undefined);
  NodeAssert.equal(item.text, "Owner revised");

  yield* guestShares.submit(token, { shareId, text: "Old share queued message" }, []);
  const old = (yield* projection()).thread.guestShare.messages.find(
    (message) => message.status === "pending",
  );
  yield* orchestrator.dispatch(ownerSend("queued-old-share", old, "queue_after_active"));
  const oldRun = (yield* projection()).runs.find((run) => run.userMessageId === "queued-old-share");
  yield* guestShares.create({ threadId });
  const replaced = yield* snapshot();
  NodeAssert.equal((yield* Effect.flip(guestShares.check(token))).reason, "inactive");
  yield* orchestrator.dispatch({
    type: "queued-run.cancel",
    commandId: CommandId.make("cancel-replaced-guest"),
    threadId,
    runId: oldRun.id,
  });
  const cancelled = yield* snapshot();
  NodeAssert.deepEqual(
    cancelled.projections[0].thread.guestShare,
    replaced.projections[0].thread.guestShare,
  );
  NodeAssert.equal(
    cancelled.events
      .slice(replaced.events.length)
      .filter((row) => row.event.type === "thread.guest-share-updated").length,
    0,
  );
});

const cleanup = Effect.gen(function* () {
  const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
  for (const action of ["dismiss", "close", "replace", "archive", "delete"]) {
    const id = ThreadId.make(`thread:cleanup:${action}`);
    const { orchestrator, guestShares, token, shareId } = yield* join(id);
    yield* guestShares.submit(token, { shareId, text: action }, yield* uploads(unicodeUploadName));
    const attachment = (yield* projection(id)).thread.guestShare.messages[0].attachments[0];
    NodeAssert.equal(attachment.name, storedUnicodeName);
    const before = yield* storedFiles;
    NodeAssert.ok(before.some((entry) => entry.startsWith(attachment.id)));
    if (action === "replace") yield* guestShares.create({ threadId: id });
    else
      yield* orchestrator.dispatch({
        type:
          action === "dismiss"
            ? "thread.guest-message.dismiss"
            : action === "close"
              ? "thread.guest-share.close"
              : `thread.${action}`,
        commandId: CommandId.make(`cleanup:${action}`),
        threadId: id,
        ...(action === "dismiss"
          ? { guestMessageId: (yield* projection(id)).thread.guestShare.messages[0].id }
          : {}),
      });
    NodeAssert.deepEqual(
      yield* storedFiles,
      before,
      "files may be removed only after commit by the effect worker",
    );
    const sql = yield* SqlClient.SqlClient;
    const effects =
      yield* sql`SELECT status FROM orchestration_v2_effect_outbox WHERE thread_id = ${id} AND effect_type = 'attachment.cleanup'`;
    NodeAssert.equal(effects.length, 1);
    NodeAssert.equal(effects[0].status, "pending");
    yield* worker.drain();
    NodeAssert.ok(!(yield* storedFiles).some((entry) => entry.startsWith(attachment.id)));
    NodeAssert.ok(Exit.isFailure(yield* Effect.exit(guestShares.attachment(token, attachment.id))));
  }
});

function directThread() {
  const future = {
    nested: { id: "opaque-id", updatedAt: "not a timestamp", now: 19 },
    values: [null, false, "💬 é العربية"],
  };
  return {
    id: threadId,
    createdBy: "user",
    creationSource: "web",
    projectId: ProjectId.make("project:guest-native"),
    title: "Native guest fixture",
    providerInstanceId: instanceId,
    modelSelection: { instanceId, model: "gpt-5.1-codex" },
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    activeProviderThreadId: null,
    lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
    forkedFrom: null,
    createdAt: now,
    updatedAt: now,
    archivedAt: null,
    settledOverride: "settled",
    settledAt: now,
    lastVisitedAt: null,
    deletedAt: null,
    future,
    guestShare: {
      id: shareId,
      host: "env.t3.test",
      createdAt: DateTime.formatIso(now),
      expiresAt: "2026-11-09T12:00:00.123Z",
      guestName: "Sam 💬",
      future,
      messages: [
        {
          id: GuestMessageId.make("message:direct-guest"),
          text: "/model 💬 guest text",
          status: "pending",
          submittedAt: DateTime.formatIso(now),
          future,
          attachments: [
            {
              type: "image",
              id: "attachment_direct",
              name: "a.png",
              mimeType: "image/png",
              sizeBytes: 12,
              future,
            },
          ],
        },
      ],
    },
  };
}

const directRoundTrip = Effect.gen(function* () {
  const native = yield* NativeGuestShare.make();
  const thread = directThread();
  const original = JSON.stringify(thread);
  const input = {
    thread,
    command: {
      type: "thread.guest-message.submit",
      commandId: CommandId.make("direct:submit"),
      threadId,
      shareId,
      guestMessageId: GuestMessageId.make("message:direct-next"),
      text: "Follow-up",
      attachments: thread.guestShare.messages[0].attachments,
    },
    now,
  };
  const submitted = yield* native.decideGuestShareCommand(input);
  NodeAssert.deepEqual(submitted, GuestShareDecider.decideGuestShareCommand(input));
  NodeAssert.equal(DateTime.toEpochMillis(submitted.success.unsettledAt), nowMillis);
  NodeAssert.deepEqual(submitted.success.future, thread.future);
  NodeAssert.deepEqual(submitted.success.guestShare.future, thread.guestShare.future);
  NodeAssert.deepEqual(
    submitted.success.guestShare.messages[0].future,
    thread.guestShare.messages[0].future,
  );
  NodeAssert.deepEqual(
    submitted.success.guestShare.messages[1].attachments,
    thread.guestShare.messages[0].attachments,
  );
  const sendInput = { thread, guestMessageId: thread.guestShare.messages[0].id, now };
  const sent = yield* native.decideGuestMessageSend(sendInput);
  NodeAssert.deepEqual(sent, GuestShareDecider.decideGuestMessageSend(sendInput));
  const cancelled = yield* native.guestShareAfterCancelledSend(
    sent.success.thread,
    sent.success.guest,
    now,
  );
  NodeAssert.deepEqual(
    cancelled,
    GuestShareDecider.guestShareAfterCancelledSend(sent.success.thread, sent.success.guest, now),
  );
  NodeAssert.deepEqual(
    yield* native.releasedGuestAttachmentIds(thread.guestShare, cancelled.guestShare),
    GuestShareDecider.releasedGuestAttachmentIds(thread.guestShare, cancelled.guestShare),
  );
  const duplicateFiles = {
    ...thread.guestShare,
    messages: [thread.guestShare.messages[0], thread.guestShare.messages[0]],
  };
  NodeAssert.deepEqual(yield* native.releasedGuestAttachmentIds(duplicateFiles, null), [
    "attachment_direct",
    "attachment_direct",
  ]);

  const history = {
    ...thread,
    guestShare: {
      ...thread.guestShare,
      messages: [
        thread.guestShare.messages[0],
        ...Array.from({ length: 21 }, (_, index) => ({
          ...thread.guestShare.messages[0],
          id: GuestMessageId.make(`history:${index}`),
          status: "sent",
        })).flatMap((message, index) =>
          index === 5
            ? [
                message,
                {
                  ...thread.guestShare.messages[0],
                  id: GuestMessageId.make("history:pending"),
                },
              ]
            : [message],
        ),
      ],
    },
  };
  const historyInput = { ...input, thread: history };
  NodeAssert.deepEqual(
    yield* native.decideGuestShareCommand(historyInput),
    GuestShareDecider.decideGuestShareCommand(historyInput),
    "valid submit trims only the oldest resolved history",
  );
  const dismissHistory = {
    ...historyInput,
    command: {
      type: "thread.guest-message.dismiss",
      commandId: CommandId.make("direct:history-dismiss"),
      threadId,
      guestMessageId: thread.guestShare.messages[0].id,
    },
  };
  NodeAssert.deepEqual(
    yield* native.decideGuestShareCommand(dismissHistory),
    GuestShareDecider.decideGuestShareCommand(dismissHistory),
    "a resolved target may itself leave the retained history",
  );
  const historySend = { ...sendInput, thread: history };
  NodeAssert.deepEqual(
    yield* native.decideGuestMessageSend(historySend),
    GuestShareDecider.decideGuestMessageSend(historySend),
  );
  const cancelHistory = { shareId, guestMessageId: GuestMessageId.make("history:0") };
  NodeAssert.deepEqual(
    yield* native.guestShareAfterCancelledSend(history, cancelHistory, now),
    GuestShareDecider.guestShareAfterCancelledSend(history, cancelHistory, now),
    "cancellation retains history without trimming",
  );

  const command = {
    commandId: "opaque-command",
    text: "owner",
    createdAt: now,
    opaque: { callback: () => "retained on host" },
    context: { version: 1, records: [], future: thread.future, guest: sent.success.guest },
  };
  const stripped = yield* native.withoutGuestAttribution(command);
  NodeAssert.deepEqual(stripped, GuestShareDecider.withoutGuestAttribution(command));
  NodeAssert.equal(stripped.opaque, command.opaque);
  NodeAssert.equal(stripped.createdAt, now);
  NodeAssert.equal(yield* native.withoutGuestAttribution(stripped), stripped);
  NodeAssert.equal(
    yield* native
      .withoutGuestAttribution({ ...command, context: undefined })
      .pipe(Effect.map((value) => value.context)),
    undefined,
  );
  NodeAssert.equal(
    JSON.stringify(thread),
    original,
    "native execution must not mutate caller state",
  );
});

const directNoopsAndDates = Effect.gen(function* () {
  const native = yield* NativeGuestShare.make();
  const thread = directThread();
  const { guestShare: _share, ...absent } = thread;
  const close = {
    type: "thread.guest-share.close",
    commandId: CommandId.make("direct:close"),
    threadId,
  };
  for (const empty of [absent, { ...thread, guestShare: null }]) {
    NodeAssert.equal(
      (yield* native.decideGuestShareCommand({ thread: empty, command: close, now })).success,
      empty,
    );
    NodeAssert.equal(
      yield* native.guestShareAfterCancelledSend(
        empty,
        { guestMessageId: GuestMessageId.make("missing"), shareId },
        now,
      ),
      empty,
    );
  }
  const open = {
    type: "thread.guest-share.open",
    commandId: CommandId.make("direct:open"),
    threadId,
    shareId,
    host: "env.t3.test",
    expiresAt: thread.guestShare.expiresAt,
  };
  const opened = GuestShareDecider.decideGuestShareCommand({ thread, command: open, now }).success;
  const reopened = (yield* native.decideGuestShareCommand({ thread: opened, command: open, now }))
    .success;
  NodeAssert.deepEqual(reopened, opened);
  NodeAssert.notEqual(reopened, opened, "same-clock open is still a change");
  const decideAt = Effect.fn("checkGuestShareAdapter.decideAt")(
    function* (current, command, offset) {
      const input = { thread: current, command, now: DateTime.makeUnsafe(nowMillis + offset) };
      const actual = yield* native.decideGuestShareCommand(input);
      NodeAssert.deepEqual(actual, GuestShareDecider.decideGuestShareCommand(input));
      return actual;
    },
  );
  const openedLater = (yield* decideAt(absent, open, 1_000)).success;
  const submit = {
    type: "thread.guest-message.submit",
    commandId: CommandId.make("direct:advancing-submit"),
    threadId,
    shareId,
    guestMessageId: GuestMessageId.make("direct:advancing-guest"),
    text: "/model guest request with a later timestamp",
    attachments: thread.guestShare.messages[0].attachments,
  };
  NodeAssert.equal(
    (yield* decideAt(openedLater, submit, 2_000)).failure,
    GuestShareDecider.GUEST_FAILURE.inactive,
    "an invitation must be redeemed before submitting",
  );
  const redeemedLater = (yield* decideAt(
    openedLater,
    {
      type: "thread.guest-share.redeem",
      commandId: CommandId.make("direct:advancing-redeem"),
      threadId,
      shareId,
      guestName: "Zoë 🦀",
    },
    3_000,
  )).success;
  const submittedLater = (yield* decideAt(redeemedLater, submit, 4_000)).success;
  NodeAssert.equal(DateTime.toEpochMillis(submittedLater.updatedAt), nowMillis + 4_000);
  NodeAssert.equal(DateTime.toEpochMillis(submittedLater.unsettledAt), nowMillis + 4_000);
  const laterSend = {
    thread: submittedLater,
    guestMessageId: submit.guestMessageId,
    now: DateTime.makeUnsafe(nowMillis + 5_000),
  };
  const sentLater = yield* native.decideGuestMessageSend(laterSend);
  NodeAssert.deepEqual(sentLater, GuestShareDecider.decideGuestMessageSend(laterSend));
  const cancelAt = DateTime.makeUnsafe(nowMillis + 6_000);
  const cancelledLater = yield* native.guestShareAfterCancelledSend(
    sentLater.success.thread,
    sentLater.success.guest,
    cancelAt,
  );
  NodeAssert.deepEqual(
    cancelledLater,
    GuestShareDecider.guestShareAfterCancelledSend(
      sentLater.success.thread,
      sentLater.success.guest,
      cancelAt,
    ),
  );
  yield* decideAt(cancelledLater, close, 7_000);
  for (const expiresAt of [
    "not a date",
    "Sun, 08 Nov 2026 12:00:01 GMT",
    "2026-11-08T13:00:00.124+01:00",
    "2026-11-08T12:00:00.123Z",
    "2026-11-08T12:00:00.122Z",
  ]) {
    const input = {
      thread: { ...opened, guestShare: { ...opened.guestShare, expiresAt } },
      command: {
        type: "thread.guest-share.redeem",
        commandId: CommandId.make("direct:redeem"),
        threadId,
        shareId,
        guestName: "Sam",
      },
      now,
    };
    NodeAssert.deepEqual(
      yield* native.decideGuestShareCommand(input),
      GuestShareDecider.decideGuestShareCommand(input),
    );
  }
  const expired = {
    ...thread,
    guestShare: { ...thread.guestShare, expiresAt: "1970-01-01T00:00:00Z" },
  };
  const input = { thread: expired, guestMessageId: expired.guestShare.messages[0].id, now };
  NodeAssert.deepEqual(
    yield* native.decideGuestMessageSend(input),
    GuestShareDecider.decideGuestMessageSend(input),
    "owner may send an existing pending message after expiry",
  );
});

async function controlledForwarder() {
  const modePath = NodePath.join(temporary, "native-mode.json");
  const countPath = NodePath.join(temporary, "native-call-count");
  const callsPath = NodePath.join(temporary, "native-calls.jsonl");
  const readyPath = NodePath.join(temporary, "native-ready.json");
  const path = await fixture(
    "controlled-guest-native",
    `
import * as fs from "node:fs";
import * as child from "node:child_process";
const input = fs.readFileSync(0);
if (Object.keys(process.env).length !== 0 || process.argv.length !== 2) {
  process.stderr.write("Guest planner inherited ambient environment or arguments\\n");
  process.exit(78);
}
const request = JSON.parse(input.toString("utf8"));
fs.appendFileSync(${JSON.stringify(callsPath)}, JSON.stringify({
  operation: request.operation,
  commandType: request.command?.type,
  uploads: request.uploads,
}) + "\\n");
const mode = JSON.parse(fs.readFileSync(${JSON.stringify(modePath)}, "utf8"));
let target = mode.operation === request.operation && (mode.commandType === undefined || mode.commandType === request.command?.type);
if (target && mode.failOnCall !== undefined) {
  const count = Number(fs.readFileSync(${JSON.stringify(countPath)}, "utf8")) + 1;
  fs.writeFileSync(${JSON.stringify(countPath)}, String(count));
  target = count === mode.failOnCall;
}
if (target && mode.kind === "invalid-json") {
  process.stdout.write("{invalid\\n");
} else if (target && mode.kind === "exit") {
  process.stderr.write("synthetic guest planner failure\\n");
  process.exitCode = 7;
} else if (target && mode.kind === "hang") {
  fs.watch(${JSON.stringify(temporary)}, () => {});
  fs.writeFileSync(${JSON.stringify(readyPath + ".tmp")}, String(process.pid));
  fs.renameSync(${JSON.stringify(readyPath + ".tmp")}, ${JSON.stringify(readyPath)});
} else {
  const forwarded = target && mode.kind === "wrong-share-success"
    ? JSON.stringify({ ...request, command: { ...request.command, shareId: request.thread.guestShare.id } }) + "\\n"
    : input;
  const native = child.spawnSync(${JSON.stringify(executable)}, [], { input: forwarded, env: {}, maxBuffer: 128 * 1024 * 1024 });
  if (native.error) throw native.error;
  if (native.status !== 0 || native.stderr.length !== 0) throw new Error("Actual Lust planner failed before fault injection");
  const response = JSON.parse(native.stdout.toString("utf8"));
  if (target) {
    if (mode.kind === "noop-open") { response.ok = request.thread; response.changed = false; }
    if (mode.kind === "foreign-thread") response.ok.id = "thread:foreign";
    if (mode.kind === "foreign-association") response.ok.projectId = "project:foreign";
    if (mode.kind === "unknown-field") response.ok.future.nested.id = "changed opaque ID";
    if (mode.kind === "wrong-send-text") response.ok.text = "forged owner instruction";
    if (mode.kind === "drop-pending") {
      const retained = request.thread.guestShare.messages.find((message) => message.status === "pending");
      response.ok.guestShare.messages = response.ok.guestShare.messages.filter((message) => message.id !== retained.id);
    }
    if (mode.kind === "reorder-pending") {
      const messages = response.ok.guestShare.messages;
      [messages[0], messages[1]] = [messages[1], messages[0]];
    }
    if (mode.kind === "foreign-release") response.ok.attachmentIds.push(mode.attachmentId);
    if (mode.kind === "keep-attribution") response.ok.context = request.context;
    if (mode.kind === "attachment-name-units") response.ok.guestShare.messages.at(-1).attachments[0].name = [65536];
    if (mode.kind === "attachment-mime-units") response.ok.guestShare.messages.at(-1).attachments[0].mimeType = [1.25];
    if (mode.kind === "upload-count") response.ok.attachments.pop();
    if (mode.kind === "upload-order") response.ok.attachments.reverse();
    if (mode.kind === "upload-size") response.ok.attachments[0].sizeBytes += 1;
    if (mode.kind === "upload-type") response.ok.attachments[0].type = "video";
    if (mode.kind === "upload-units") response.ok.attachments[0].nameUnits = [65536];
    if (mode.kind === "upload-reason") response.ok = { ok: false, reason: "forged" };
    if (mode.kind === "upload-changed") response.changed = false;
    if (mode.kind === "cancel-replaced") {
      response.ok = { ...request.thread, updatedAt: request.now, guestShare: { ...request.thread.guestShare, messages: request.thread.guestShare.messages.map((message) => message.id === request.guest.guestMessageId ? { ...message, status: "dismissed" } : message) } };
      response.changed = true;
    }
  }
  process.stdout.write(JSON.stringify(response) + "\\n");
}
`,
  );
  const setMode = Effect.fn("checkGuestShareAdapter.setMode")(function* (mode = {}) {
    yield* Effect.promise(() => NodeFSP.writeFile(modePath, JSON.stringify(mode)));
    yield* Effect.promise(() => NodeFSP.writeFile(countPath, "0"));
    yield* Effect.promise(() => NodeFSP.writeFile(callsPath, ""));
  });
  await Effect.runPromise(setMode());
  const calls = Effect.promise(async () =>
    (await NodeFSP.readFile(callsPath, "utf8"))
      .split("\n")
      .filter(Boolean)
      .map((line) => JSON.parse(line)),
  );
  return { path, setMode, readyPath, calls };
}

function nativeError(error, stage = "response") {
  NodeAssert.equal(error._tag, "NativeGuestShareError");
  NodeAssert.equal(error.stage, stage);
}

const directUploadRoundTrip = Effect.gen(function* () {
  const native = yield* NativeGuestShare.make();
  NodeAssert.deepEqual(yield* native.classifyGuestUploads([]), { ok: true, attachments: [] });
  const inspected = [
    {
      name: " \u202ereport/one.png ",
      contentType: " IMAGE/PNG; charset=ignored",
      sizeBytes: PNG.length,
      header: PNG,
    },
    {
      name: "a".repeat(254) + "🦀",
      contentType: "text/plain",
      sizeBytes: 0.5,
      header: new Uint8Array(),
    },
    {
      name: "\ud800raw\udfff",
      contentType: "APPLICATION/X-İ-ΟΣ",
      sizeBytes: 1,
      header: new Uint8Array(),
    },
    { name: "\ufeff ", contentType: "", sizeBytes: 2, header: new Uint8Array() },
  ];
  const before = inspected.map((entry) => ({ ...entry, header: Uint8Array.from(entry.header) }));
  NodeAssert.deepEqual(yield* native.classifyGuestUploads(inspected), {
    ok: true,
    attachments: [
      { type: "image", name: "_report_one.png", mimeType: "image/png", sizeBytes: PNG.length },
      { type: "file", name: "a".repeat(254) + "\ud83e", mimeType: "text/plain", sizeBytes: 0.5 },
      { type: "file", name: "\ud800raw\udfff", mimeType: "application/x-i\u0307-ος", sizeBytes: 1 },
      { type: "file", name: "file", mimeType: "application/octet-stream", sizeBytes: 2 },
    ],
  });
  NodeAssert.deepEqual(inspected, before, "native classification must not mutate inspected uploads");
});

async function uploadClassificationChecks(control) {
  const copies = [];
  await run(
    Effect.gen(function* () {
      yield* control.setMode();
      const { guestShares, token, shareId } = yield* join();
      const image = yield* uploads(" ../invoice\u202e.png ", PNG.length, " IMAGE/PNG; charset=ignored");
      const file = yield* uploads(" report.txt ", PNG.length + 1, "TEXT/PLAIN; charset=utf-8");
      yield* control.setMode();
      yield* guestShares.submit(token, { shareId, text: "Classify both files" }, [...image, ...file]);
      const calls = yield* control.calls;
      const classifyIndex = calls.findIndex((call) => call.operation === "classify-uploads");
      NodeAssert.notEqual(classifyIndex, -1, "real guest upload must invoke the native classifier");
      NodeAssert.ok(
        calls.findIndex((call) => call.commandType === "thread.guest-message.submit") > classifyIndex,
      );
      NodeAssert.equal(calls.filter((call) => call.operation === "classify-uploads").length, 1);
      NodeAssert.deepEqual(
        calls[classifyIndex].uploads,
        [image[0], file[0]].map((entry, index) => ({
          nameUnits: utf16Units(entry.name),
          contentTypeLowercaseUnits: utf16Units(entry.contentType.toLowerCase()),
          sizeBytes: PNG.length + index,
          // Inspection reads 12 bytes, so the text file's final zero is excluded.
          header: Array.from(PNG),
        })),
        "native request contains inspected bytes and metadata, without host file paths",
      );
      const attachments = (yield* projection()).thread.guestShare.messages[0].attachments;
      NodeAssert.deepEqual(attachments.map(({ id: _id, ...draft }) => draft), [
        { type: "image", name: ".._invoice_.png", mimeType: "image/png", sizeBytes: PNG.length },
        { type: "file", name: "report.txt", mimeType: "text/plain", sizeBytes: PNG.length + 1 },
      ]);
      NodeAssert.equal(copies.length, 2, "both classified files must use real upload copies");
      scenarios.push(
        "guest service invokes native upload classification before dispatch and stores ordered image/file metadata",
      );

      const expectNoWrite = Effect.fn("checkGuestShareAdapter.classifierRejectsBeforeCopy")(
        function* (label, supplied, reason, kind) {
          const before = yield* snapshot();
          const files = yield* storedFiles;
          const copyCount = copies.length;
          yield* control.setMode(kind === undefined ? {} : { operation: "classify-uploads", kind });
          const error = yield* Effect.flip(
            guestShares.submit(token, { shareId, text: label }, supplied),
          );
          NodeAssert.equal(error.reason, reason, label);
          NodeAssert.equal(copies.length, copyCount, `${label}: classifier must fail before copying`);
          NodeAssert.deepEqual(yield* storedFiles, files, `${label}: retained attachments changed`);
          NodeAssert.deepEqual(yield* snapshot(), before, `${label}: classifier committed durable changes`);
          const calls = yield* control.calls;
          NodeAssert.equal(
            calls.filter((call) => call.operation === "classify-uploads").length, 1, label,
          );
          NodeAssert.equal(
            calls.some((call) => call.commandType === "thread.guest-message.submit"), false, label,
          );
        },
      );
      yield* expectNoWrite("too many files", Array(5).fill(image[0]), "too_many_files");
      yield* expectNoWrite("too many bytes", yield* uploads("large.png", 10 * MIB + 1), "too_large");
      yield* expectNoWrite(
        "unsupported image", [{ ...image[0], contentType: "image/svg+xml" }], "unsupported_file",
      );
      scenarios.push(
        "native count, byte and MIME rejections copy nothing and leave all SQLite records and prior files unchanged",
      );
      for (const kind of [
        "upload-count", "upload-order", "upload-size", "upload-type", "upload-units",
        "upload-reason", "upload-changed", "invalid-json", "exit",
      ]) {
        yield* expectNoWrite(`classifier ${kind}`, [...image, ...file], "rejected", kind);
        scenarios.push(`${kind}: classifier failure rejects before copy or dispatch without upstream fallback`);
      }
      yield* control.setMode();
      yield* guestShares.submit(token, { shareId, text: "Text after classifier failures" }, []);
      NodeAssert.equal(
        (yield* control.calls).some((call) => call.operation === "classify-uploads"), false,
      );
      NodeAssert.equal(copies.length, 2, "text-only submission does not inspect or copy uploads");
    }),
    control.path,
    "upload-classification",
    undefined,
    false,
    copies,
  );
}

async function nativeFailureChecks() {
  const control = await controlledForwarder();
  await uploadClassificationChecks(control);
  for (const kind of ["drop-pending", "reorder-pending"]) {
    await run(
      Effect.gen(function* () {
        yield* control.setMode();
        const { guestShares, token, shareId } = yield* join();
        yield* guestShares.submit(
          token,
          { shareId, text: "Retained A" },
          yield* uploads("retained-a.png"),
        );
        yield* guestShares.submit(token, { shareId, text: "Retained B" }, []);
        const before = yield* snapshot();
        const files = yield* storedFiles;
        const retained = (yield* projection()).thread.guestShare.messages[0];
        yield* control.setMode({
          operation: "decide",
          commandType: "thread.guest-message.submit",
          kind,
        });
        const outcome = yield* guestShares
          .submit(token, { shareId, text: "New C" }, [])
          .pipe(Effect.exit);
        const after = yield* snapshot();
        if (Exit.isSuccess(outcome)) {
          yield* (yield* EffectWorker.OrchestrationEffectWorkerV2).drain();
          const stored = yield* storedFiles;
          NodeAssert.fail(
            `${kind} native reply committed: messages=${after.projections[0].thread.guestShare.messages.map((message) => message.text).join(",")}; retained attachment exists after worker=${stored.some((file) => file.startsWith(retained.attachments[0].id))}`,
          );
        }
        const error = yield* Effect.failCause(outcome.cause).pipe(Effect.flip);
        NodeAssert.equal(error.reason, "rejected");
        assertRejected(before, after);
        yield* (yield* EffectWorker.OrchestrationEffectWorkerV2).drain();
        NodeAssert.deepEqual(
          yield* storedFiles,
          files,
          "rejected reply must retain earlier pending files",
        );
        yield* control.setMode();
      }),
      control.path,
      `retained-messages-${kind}`,
    );
    scenarios.push(
      `${kind}: native reply cannot delete or reorder earlier pending messages or their files`,
    );
  }
  const marker = "T3LS_GUEST_AMBIENT_PROOF";
  const previous = process.env[marker];
  try {
    process.env[marker] = "synthetic-guest-environment-marker";
    const inherited = await run(
      Effect.gen(function* () {
        return yield* (yield* ProcessRunner.make()).run({
          command: control.path,
          args: [],
          env: {},
          stdin: "{}\n",
        });
      }),
      undefined,
      "environment-witness",
      undefined,
      true,
    );
    NodeAssert.equal(inherited.code, 78, "default environment merging must fail the witness");
    await run(directRoundTrip, control.path, "isolated-native", undefined, true);
    scenarios.push(
      "native success has empty environment and no payload arguments; inherited-env witness fails",
    );
  } finally {
    if (previous === undefined) delete process.env[marker];
    else process.env[marker] = previous;
  }

  for (const selection of ["", "relative-planner", "/missing/native-guest-planner"]) {
    await run(
      Effect.gen(function* () {
        const native = yield* NativeGuestShare.make();
        const error = yield* Effect.flip(native.withoutGuestAttribution({ text: "owner" }));
        nativeError(error, selection.startsWith("/") ? "process" : "configuration");
      }),
      selection,
      "invalid-selector",
      undefined,
      true,
    );
  }
  scenarios.push(
    "configured empty, relative and missing executables fail without upstream fallback",
  );

  for (const kind of ["noop-open", "foreign-thread", "foreign-association"]) {
    await run(
      Effect.gen(function* () {
        const orchestrator = yield* createThread();
        yield* control.setMode({
          operation: "decide",
          commandType: "thread.guest-share.open",
          kind,
        });
        const command = {
          type: "thread.guest-share.open",
          commandId: CommandId.make(`fault:${kind}`),
          threadId,
          shareId,
          host: "env.t3.test",
          expiresAt: "2026-11-09T12:00:00Z",
        };
        const error = yield* rejectedDispatch(command);
        nativeError(error.cause);
        yield* control.setMode();
        const before = yield* snapshot();
        NodeAssert.equal(
          (yield* Effect.flip(orchestrator.dispatch(command)))._tag,
          "OrchestratorCommandPreviouslyRejectedError",
        );
        NodeAssert.deepEqual(yield* snapshot(), before);
        yield* orchestrator.dispatch({ ...command, commandId: CommandId.make(`retry:${kind}`) });
        NodeAssert.equal((yield* projection()).thread.guestShare.id, shareId);
      }),
      control.path,
      `fault-${kind}`,
    );
    scenarios.push(
      `${kind}: invalid native open persists only rejected receipt; new command succeeds`,
    );
  }

  await run(
    Effect.gen(function* () {
      const native = yield* NativeGuestShare.make();
      const thread = directThread();
      yield* control.setMode({ operation: "decide", kind: "unknown-field" });
      nativeError(
        yield* Effect.flip(
          native.decideGuestShareCommand({
            thread,
            command: {
              type: "thread.guest-message.dismiss",
              commandId: CommandId.make("fault:extension"),
              threadId,
              guestMessageId: thread.guestShare.messages[0].id,
            },
            now,
          }),
        ),
      );
      const replacement = {
        ...thread,
        guestShare: {
          ...thread.guestShare,
          id: GuestShareId.make("replacement-share"),
          messages: thread.guestShare.messages.map((message) => ({ ...message, status: "sent" })),
        },
      };
      yield* control.setMode({ operation: "cancel", kind: "cancel-replaced" });
      nativeError(
        yield* Effect.flip(
          native.guestShareAfterCancelledSend(
            replacement,
            { shareId, guestMessageId: thread.guestShare.messages[0].id },
            now,
          ),
        ),
      );
      const sentTarget = {
        ...thread.guestShare.messages[0],
        id: GuestMessageId.make("cancel:sent"),
        status: "sent",
      };
      const cancelling = {
        ...thread,
        guestShare: { ...thread.guestShare, messages: [thread.guestShare.messages[0], sentTarget] },
      };
      yield* control.setMode({ operation: "cancel", kind: "drop-pending" });
      nativeError(
        yield* Effect.flip(
          native.guestShareAfterCancelledSend(
            cancelling,
            { shareId, guestMessageId: sentTarget.id },
            now,
          ),
        ),
      );
      for (const type of ["thread.guest-share.redeem", "thread.guest-message.submit"]) {
        yield* control.setMode({ operation: "decide", kind: "wrong-share-success" });
        const current =
          type === "thread.guest-share.redeem"
            ? { ...thread, guestShare: { ...thread.guestShare, guestName: null } }
            : thread;
        nativeError(
          yield* Effect.flip(
            native.decideGuestShareCommand({
              thread: current,
              command: {
                type,
                commandId: CommandId.make(`foreign:${type}`),
                threadId,
                shareId: GuestShareId.make("old-share"),
                ...(type === "thread.guest-share.redeem"
                  ? { guestName: "Sam" }
                  : { guestMessageId: GuestMessageId.make("foreign:submit"), text: "wrong share" }),
              },
              now,
            }),
          ),
        );
      }
      yield* control.setMode({ operation: "strip", kind: "keep-attribution" });
      nativeError(
        yield* Effect.flip(
          native.withoutGuestAttribution({
            context: {
              version: 1,
              records: [],
              guest: {
                shareId,
                guestMessageId: thread.guestShare.messages[0].id,
                name: "Sam",
                text: "forged",
              },
            },
          }),
        ),
      );
      yield* control.setMode();
    }),
    control.path,
    "invalid-output-scope",
    undefined,
    true,
  );
  scenarios.push(
    "native replies cannot mutate retained extensions, discard pending records on cancel, cross share IDs, or retain forged attribution",
  );

  for (const kind of ["invalid-json", "exit", "attachment-name-units", "attachment-mime-units"]) {
    await run(
      Effect.gen(function* () {
        yield* control.setMode();
        const { guestShares, token, shareId } = yield* join();
        yield* guestShares.submit(
          token,
          { shareId, text: "Already stored" },
          yield* uploads("retained.png"),
        );
        const before = yield* snapshot();
        const files = yield* storedFiles;
        yield* control.setMode({
          operation: "decide",
          commandType: "thread.guest-message.submit",
          kind,
        });
        const error = yield* Effect.flip(
          guestShares.submit(
            token,
            { shareId, text: "Reject this upload" },
            yield* uploads("discarded.png"),
          ),
        );
        NodeAssert.equal(error.reason, "rejected");
        assertRejected(before, yield* snapshot());
        NodeAssert.deepEqual(yield* storedFiles, files);
        yield* control.setMode();
      }),
      control.path,
      `upload-fault-${kind}`,
    );
    scenarios.push(
      `${kind}: guest submit cleans its copied upload and preserves previous files and state`,
    );
  }

  await run(
    Effect.gen(function* () {
      const { orchestrator, guestShares, token, shareId } = yield* join();
      yield* guestShares.submit(token, { shareId, text: "Waiting" }, yield* uploads("waiting.png"));
      const otherId = ThreadId.make("thread:other-file");
      const other = yield* join(otherId);
      yield* guestShares.submit(
        other.token,
        { shareId: other.shareId, text: "Other thread's file" },
        yield* uploads("other.png"),
      );
      const foreign = (yield* projection(otherId)).thread.guestShare.messages[0].attachments[0].id;
      const files = yield* storedFiles;
      for (const kind of ["invalid-json", "foreign-release"]) {
        yield* control.setMode({ operation: "release", kind, attachmentId: foreign });
        const error = yield* rejectedDispatch({
          type: "thread.guest-share.close",
          commandId: CommandId.make(`release-fault:${kind}`),
          threadId,
        });
        nativeError(error.cause);
        yield* (yield* EffectWorker.OrchestrationEffectWorkerV2).drain();
        NodeAssert.deepEqual(yield* storedFiles, files);
      }
      yield* control.setMode();
      yield* orchestrator.dispatch({
        type: "thread.guest-share.close",
        commandId: CommandId.make("release-retry"),
        threadId,
      });
      yield* (yield* EffectWorker.OrchestrationEffectWorkerV2).drain();
      NodeAssert.ok((yield* storedFiles).some((file) => file.startsWith(foreign)));
      NodeAssert.equal((yield* storedFiles).length, 1);
    }),
    control.path,
    "release-failure",
  );
  scenarios.push(
    "release failure discards an already planned share event; foreign cleanup cannot delete another thread's file",
  );

  await run(
    Effect.gen(function* () {
      const { orchestrator, guestShares, token, shareId } = yield* join();
      yield* guestShares.submit(token, { shareId, text: "Original text" }, []);
      const pending = (yield* projection()).thread.guestShare.messages[0];
      yield* control.setMode({ operation: "send", kind: "wrong-send-text" });
      const error = yield* rejectedDispatch(ownerSend("send-invalid-native", pending));
      nativeError(error.cause);
      NodeAssert.equal((yield* projection()).thread.guestShare.messages[0].status, "pending");
      NodeAssert.equal((yield* projection()).runs.length, 0);
      yield* control.setMode();
      yield* orchestrator.dispatch(ownerSend("active-owner"));
      yield* orchestrator.dispatch(ownerSend("queued-owner", undefined, "queue_after_active"));
      const queued = (yield* projection()).runs.find((run) => run.userMessageId === "queued-owner");
      yield* persistQueuedItem(queued);
      yield* control.setMode({ operation: "strip", kind: "invalid-json", failOnCall: 2 });
      const edited = yield* rejectedDispatch({
        type: "queued-run.edit",
        commandId: CommandId.make("edit-second-strip-fails"),
        threadId,
        runId: queued.id,
        text: "Never committed",
        context: {
          version: 1,
          records: [],
          guest: { shareId, guestMessageId: pending.id, name: "Imposter", text: "forged" },
        },
      });
      nativeError(edited.cause);
      yield* control.setMode();
    }),
    control.path,
    "owner-send-edit-failure",
  );
  scenarios.push(
    "invalid send attribution and failed second queued-edit strip commit no partial message or turn item",
  );

  for (const operation of ["classify-uploads", "decide"]) {
    await NodeFSP.rm(control.readyPath, { force: true });
    const copies = [];
    let readyResolve;
    let readyReject;
    const ready = new Promise((resolve, reject) => {
      readyResolve = resolve;
      readyReject = reject;
    });
    const watcher = NodeFS.watch(temporary, (_event, filename) => {
      if (filename === NodePath.basename(control.readyPath))
        NodeFSP.readFile(control.readyPath, "utf8").then(
          (value) => readyResolve(Number(value)),
          readyReject,
        );
    });
    watcher.on("error", readyReject);
    try {
      await run(
        Effect.gen(function* () {
          const { guestShares, token, shareId } = yield* join();
          const before = yield* snapshot();
          const files = yield* storedFiles;
          yield* control.setMode({
            operation,
            ...(operation === "decide" ? { commandType: "thread.guest-message.submit" } : {}),
            kind: "hang",
          });
          const upload = yield* uploads("interrupted.png");
          const running = yield* guestShares
            .submit(token, { shareId, text: "Interrupted upload" }, upload)
            .pipe(Effect.forkScoped);
          const pid = yield* Effect.promise(() => ready);
          NodeAssert.ok(Number.isSafeInteger(pid) && pid > 0);
          const expectedCopies = operation === "decide" ? 1 : 0;
          NodeAssert.equal(copies.length, expectedCopies, `${operation}: incorrect upload copy boundary`);
          NodeAssert.equal((yield* storedFiles).length, files.length + expectedCopies);
          yield* Fiber.interrupt(running);
          const outcome = yield* Fiber.await(running);
          NodeAssert.ok(Exit.isFailure(outcome) && Cause.hasInterruptsOnly(outcome.cause));
          NodeAssert.throws(
            () => process.kill(pid, 0),
            { code: "ESRCH" },
            "interruption must reap the owned planner child",
          );
          NodeAssert.deepEqual(
            yield* snapshot(),
            before,
            "interruption must not commit a decision or a rejected receipt",
          );
          NodeAssert.deepEqual(yield* storedFiles, files, "interrupted submit must retain no new files");
          NodeAssert.equal(copies.length, expectedCopies, "interruption must not retry through upstream");
          if (operation === "classify-uploads") {
            NodeAssert.equal(
              (yield* control.calls).some((call) => call.commandType === "thread.guest-message.submit"),
              false,
              "interrupted classification must never dispatch the submission",
            );
          }
          yield* control.setMode();
          yield* guestShares.submit(token, { shareId, text: "Retry after interruption" }, []);
        }),
        control.path,
        `interruption-${operation}`,
        undefined,
        false,
        copies,
      );
      scenarios.push(
        `${operation}: interruption reaps its child, preserves files and durable state, and releases the thread lock`,
      );
    } finally {
      watcher.close();
    }
  }
}

try {
  const baseline = await lifecycle(undefined, "upstream");
  for (const [name, program] of [
    ["limits and rejected-upload cleanup", limits],
    ["same-clock changes and no-op receipts", noops],
    ["queued cancellation, replacement and attribution", queueAndAttribution],
    ["attachment cleanup only after commit", cleanup],
  ]) {
    await run(program, undefined, `upstream-${scenarios.length}`);
    scenarios.push(`upstream: ${name}`);
  }
  if (!baselineOnly) {
    NodeAssert.deepEqual(await lifecycle(executable, "native"), baseline);
    for (const [name, program] of [
      ["limits and rejected-upload cleanup", limits],
      ["same-clock changes and no-op receipts", noops],
      ["queued cancellation, replacement and attribution", queueAndAttribution],
      ["attachment cleanup only after commit", cleanup],
    ]) {
      await run(program, executable, `native-${scenarios.length}`);
      scenarios.push(`native: ${name}`);
    }
    await run(directRoundTrip, executable, "native-round-trip", undefined, true);
    scenarios.push(
      "five actual native adapter methods preserve unknown nested payloads, DateTime values and generic command fields",
    );
    await run(directUploadRoundTrip, executable, "native-upload-round-trip", undefined, true);
    scenarios.push(
      "native upload adapter preserves empty batches, MIME case folding, attachment order and lone UTF-16 surrogates",
    );
    await run(directNoopsAndDates, executable, "native-noops-dates", undefined, true);
    scenarios.push(
      "native adapter preserves no-op identity, equal-valued changes and JavaScript expiry parsing",
    );
    await nativeFailureChecks();
  }
  const sources = [
    "apps/server/src/orchestration-v2/NativeGuestShare.ts",
    "apps/server/src/orchestration-v2/GuestShareDecider.ts",
    "apps/server/src/orchestration-v2/Orchestrator.ts",
    "apps/server/src/orchestration-v2/GuestShare.test.ts",
    "apps/server/src/guest/GuestShareService.ts",
    "apps/server/src/guest/GuestShareService.test.ts",
    "apps/server/src/guest/guestAttachments.ts",
    "apps/server/src/guest/guest_uploads.rs",
    "apps/server/src/guest/guestView.ts",
    "apps/server/src/auth/PayloadSigner.ts",
    "apps/server/src/auth/ServerSecretStore.ts",
    "apps/server/src/auth/utils.ts",
    "apps/server/src/attachmentStore.ts",
    "apps/server/src/attachmentPaths.ts",
    "apps/server/src/config.ts",
    "apps/server/src/serverSettings.ts",
    "apps/server/src/processRunner.ts",
    "apps/server/src/orchestration-v2/CommandOutputHub.ts",
    "apps/server/src/orchestration-v2/CommandReceiptStore.ts",
    "apps/server/src/orchestration-v2/EventSink.ts",
    "apps/server/src/orchestration-v2/EventStore.ts",
    "apps/server/src/orchestration-v2/ProjectionStore.ts",
    "apps/server/src/orchestration-v2/EffectOutbox.ts",
    "apps/server/src/orchestration-v2/EffectWorker.ts",
    "apps/server/src/orchestration-v2/ResourceCleanupService.ts",
    "apps/server/src/orchestration-v2/ProviderAdapterRegistry.ts",
    "apps/server/src/orchestration-v2/ThreadCommandExecutor.ts",
    "apps/server/src/orchestration-v2/TurnItemPositionStore.ts",
    "apps/server/src/orchestration-v2/testkit/ProjectionReplay.ts",
    "apps/server/src/orchestration-v2/testkit/ProviderReplayHarness.ts",
    "apps/server/src/persistence/Sqlite.ts",
    "apps/server/src/persistence/OrchestrationEventStore.ts",
    "apps/server/src/persistence/OrchestrationCommandReceipts.ts",
    "apps/server/src/persistence/Migrations.ts",
    "packages/shared/src/nodeSqliteClient.ts",
    "packages/shared/src/HostProcess.ts",
    "packages/shared/src/shell.ts",
    "packages/provider-core/src/server/IdAllocator.ts",
    "packages/provider-core/src/server/collectStreamText.ts",
    "packages/contracts/src/orchestrationV2.ts",
    "packages/contracts/src/guestShare.ts",
    "packages/contracts/src/chatAttachment.ts",
    "packages/contracts/src/composerContext.ts",
    "packages/contracts/src/baseSchemas.ts",
    "crates/core/src/guest_share.rs",
    "crates/core/src/guest_share_wire.rs",
    "crates/core/src/bin/t3-code-ls-guest-share.rs",
    "crates/core/src/json.rs",
    "tools/check-guest-share-adapter.mjs",
    "tools/guest-share-tsconfig.json",
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
    ...(
      await NodeFSP.readdir(NodePath.join(root, "apps/server/src/persistence/Migrations"), {
        recursive: true,
      })
    )
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
  const platform = Effect.runSync(HostProcess.Platform);
  const architecture = Effect.runSync(HostProcess.Architecture);
  const proof = {
    runtime: process.version,
    platform: `${platform}-${architecture}`,
    executable_sha256: baselineOnly
      ? null
      : NodeCrypto.createHash("sha256")
          .update(await NodeFSP.readFile(executable))
          .digest("hex"),
    source_sha256: sourceSha256,
    scenarios,
    count: scenarios.length,
    actual_sqlite: true,
    actual_guest_share_service: true,
    actual_orchestrator: true,
    actual_resource_cleanup: true,
    actual_signing_and_owned_files: true,
    actual_native_guest_decisions: !baselineOnly,
    actual_native_guest_upload_classification: !baselineOnly,
    note: "Guest decision and upload classification integration only. Auth, service orchestration and clients remain upstream TypeScript; this does not establish a complete application, mobile or Iroh port.",
  };
  if (proofPath !== undefined && proofPath !== "--baseline-only")
    await NodeFSP.writeFile(proofPath, JSON.stringify(proof, null, 2) + "\n");
  console.log(
    `Guest-share adapter: ${scenarios.length} real service/persistence scenarios passed${baselineOnly ? " (upstream baseline only)" : ""}.`,
  );
} finally {
  await NodeFSP.rm(temporary, { recursive: true, force: true });
}
