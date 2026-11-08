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
import { CommandId, EventId, ProjectId, ProviderInstanceId, ThreadId } from "@t3tools/contracts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Cause from "effect/Cause";
import * as Clock from "effect/Clock";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as References from "effect/References";
import * as Result from "effect/Result";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/sql/SqlClient";
import * as ServerConfig from "../../../apps/server/src/config.ts";
import * as EventSink from "../../../apps/server/src/orchestration-v2/EventSink.ts";
import * as EventStore from "../../../apps/server/src/orchestration-v2/EventStore.ts";
import * as NativeProjectCommands from "../../../apps/server/src/orchestration-v2/NativeProjectCommands.ts";
import * as ProjectCommands from "../../../apps/server/src/orchestration-v2/ProjectCommands.ts";
import * as ProjectStore from "../../../apps/server/src/orchestration-v2/ProjectStore.ts";
import * as ProjectionStore from "../../../apps/server/src/orchestration-v2/ProjectionStore.ts";
import * as RuntimeLayer from "../../../apps/server/src/orchestration-v2/runtimeLayer.ts";
import * as ThreadCommandExecutor from "../../../apps/server/src/orchestration-v2/ThreadCommandExecutor.ts";
import * as LegacyV1ThreadImporter from "../../../apps/server/src/orchestration-v2/legacy/LegacyV1ThreadImporter.ts";
import * as ProjectionReplay from "../../../apps/server/src/orchestration-v2/testkit/ProjectionReplay.ts";
import * as ApplicationEvents from "../../../apps/server/src/persistence/OrchestrationEventStore.ts";
import * as SqlitePersistence from "../../../apps/server/src/persistence/Sqlite.ts";
import * as ProjectEnrichmentService from "../../../apps/server/src/project/ProjectEnrichmentService.ts";
import * as ProjectFaviconResolver from "../../../apps/server/src/project/ProjectFaviconResolver.ts";
import * as ProjectService from "../../../apps/server/src/project/ProjectService.ts";
import * as RepositoryIdentityResolver from "../../../apps/server/src/project/RepositoryIdentityResolver.ts";
import * as ServerSettings from "../../../apps/server/src/serverSettings.ts";
import * as WorkspacePaths from "../../../apps/server/src/workspace/WorkspacePaths.ts";

NodeAssert.ok(process.argv[2], "Pass the Lust-built project executable");
const executable = NodePath.resolve(process.argv[2]);
const proofPath = process.argv[3];
const baselineOnly = process.argv.includes("--baseline-only");
const root = NodeURL.fileURLToPath(new URL("../../../", import.meta.url));
const temporary = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "t3ls-project-adapter-"));
const scenarios = [];
const nowMillis = Date.parse("2026-11-08T12:00:00.123Z");
const now = DateTime.makeUnsafe(nowMillis);
const instanceId = ProviderInstanceId.make("codex");

function fixedClock(milliseconds) {
  const clock = Effect.runSync(Clock.Clock);
  return Object.assign(Object.create(clock), {
    currentTimeMillis: Effect.succeed(milliseconds),
    currentTimeMillisUnsafe: () => milliseconds,
  });
}

const at = (offset, effect) =>
  effect.pipe(Effect.provideService(Clock.Clock, fixedClock(nowMillis + offset)));
const iso = (offset = 0) => new Date(nowMillis + offset).toISOString();
const workspace = (name) => NodePath.join(temporary, "workspaces", name);
const projectId = (name) => ProjectId.make(`project:${name}`);
const commandId = (name) => CommandId.make(`command:${name}`);
const createInput = (name, location = name) => ({
  commandId: commandId(`create:${name}`),
  projectId: projectId(name),
  title: `Project ${name} 💬`,
  workspaceRoot: `${workspace(location)}/`,
  createWorkspaceRootIfMissing: true,
});
const script = (id, overrides = {}) => ({
  id,
  name: "Verify é العربية",
  command: "printf 'a\\nb 💬'",
  icon: "test",
  runOnWorktreeCreate: true,
  runOnSettle: false,
  stopOnSettle: true,
  async: true,
  previewUrl: "http://127.0.0.1:3456/path?x=a&y=b",
  autoOpenPreview: false,
  ...overrides,
});

function serviceLayer(name, filename) {
  const database =
    filename === undefined
      ? SqlitePersistence.layerMemory
      : SqlitePersistence.layerFromPath(filename);
  const stores = Layer.mergeAll(
    ApplicationEvents.layer,
    EventStore.layer,
    ProjectionStore.layer,
    ProjectStore.layer,
    IdAllocator.layer,
    ThreadCommandExecutor.layer,
  ).pipe(Layer.provideMerge(database));
  const sink = EventSink.layer.pipe(Layer.provideMerge(stores));
  const enrichment = ProjectEnrichmentService.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        Layer.succeed(RepositoryIdentityResolver.RepositoryIdentityResolver, {
          resolve: () => Effect.succeed(null),
        }),
        Layer.succeed(ProjectFaviconResolver.ProjectFaviconResolver, {
          invalidate: () => Effect.void,
          resolvePath: () => Effect.succeed(null),
        }),
      ),
    ),
  );
  const dependencies = Layer.mergeAll(
    sink,
    LegacyV1ThreadImporter.layer.pipe(Layer.provide(sink)),
    ProjectionReplay.layer.pipe(Layer.provide(sink)),
    enrichment,
    WorkspacePaths.layer,
    ServerSettings.layerTest(),
  );
  return RuntimeLayer.layerProjectService.pipe(
    Layer.provideMerge(dependencies),
    Layer.provideMerge(ServerConfig.layerTest(temporary, NodePath.join(temporary, name))),
    Layer.provideMerge(Layer.mergeAll(NodeServices.layer, PlatformCrypto.layer)),
  );
}

async function run(effect, selection, name, filename, direct = false) {
  return Effect.runPromise(
    effect.pipe(
      Effect.scoped,
      Effect.provide(direct ? NodeServices.layer : serviceLayer(name, filename)),
      Effect.provideService(
        HostProcess.Environment,
        selection === undefined ? {} : { T3_CODE_LS_PROJECT_EXECUTABLE: selection },
      ),
      Effect.provideService(HostProcess.WorkingDirectory, temporary),
      Effect.provideService(HostProcess.HomeDirectory, temporary),
      Effect.provideService(Clock.Clock, fixedClock(nowMillis)),
      Effect.provideService(References.MinimumLogLevel, "Warn"),
    ),
  );
}

const snapshot = Effect.fn("checkProjectAdapter.snapshot")(function* () {
  const sql = yield* SqlClient.SqlClient;
  return {
    events: yield* sql`SELECT * FROM orchestration_events ORDER BY sequence`,
    projects: yield* sql`SELECT * FROM projection_projects ORDER BY project_id`,
    receipts: yield* sql`SELECT * FROM orchestration_command_receipts ORDER BY command_id`,
    threads: yield* sql`SELECT * FROM orchestration_v2_projection_threads ORDER BY thread_id`,
    outbox: yield* sql`SELECT * FROM orchestration_v2_effect_outbox ORDER BY effect_id`,
  };
});

function comparable(state) {
  return {
    ...state,
    events: state.events.map(({ event_id: _eventId, ...event }) => event),
  };
}

const replayProjects = Effect.fn("checkProjectAdapter.replayProjects")(function* () {
  const sql = yield* SqlClient.SqlClient;
  const store = yield* ProjectStore.ProjectStoreV2;
  const events = yield* ApplicationEvents.OrchestrationEventStore;
  const before = yield* store.list({ includeDeleted: true });
  const throughSequence = yield* events.latestApplicationSequence;
  const history = yield* events
    .readApplicationEvents({ afterSequence: 0, throughSequence })
    .pipe(Stream.runCollect);
  yield* sql.withTransaction(
    Effect.gen(function* () {
      yield* sql`DELETE FROM projection_projects`;
      for (const event of history) {
        if (event.aggregateKind === "project") yield* store.apply(event);
      }
    }),
  );
  NodeAssert.deepEqual(yield* store.list({ includeDeleted: true }), before);
});

function errorView(error) {
  const { _tag, projectId, workspaceRoot, conflictingProjectId, detail, operation } = error;
  return { _tag, projectId, workspaceRoot, conflictingProjectId, detail, operation };
}

function nativeError(error, stage = "response") {
  let current = error;
  for (let depth = 0; depth < 8 && current?._tag !== "NativeProjectCommandsError"; depth++) {
    current = current?.cause;
  }
  NodeAssert.equal(current?._tag, "NativeProjectCommandsError");
  NodeAssert.equal(current.stage, stage);
}

const lifecycle = Effect.fn("checkProjectAdapter.lifecycle")(function* () {
  const service = yield* ProjectService.ProjectService;
  const input = { ...createInput("lifecycle"), scripts: [script("legacy.INVALID")] };
  const created = yield* service.create(input);
  NodeAssert.equal(created.workspaceRoot, workspace("lifecycle"));
  NodeAssert.equal(created.defaultModelSelection, null);
  NodeAssert.deepEqual(created.scripts, input.scripts);
  const first = yield* snapshot();
  NodeAssert.deepEqual(yield* service.create(input), created);
  NodeAssert.deepEqual(yield* snapshot(), first, "accepted create receipt must replay once");
  const bootstrap = yield* service.bootstrap({
    ...input,
    projectId: projectId("unused"),
    commandId: commandId("unused"),
  });
  NodeAssert.equal(bootstrap.created, false);
  NodeAssert.deepEqual(bootstrap.project, created);
  const update = {
    projectId: input.projectId,
    commandId: commandId("lifecycle:update"),
    title: "Renamed é العربية 💬",
    defaultModelSelection: {
      instanceId,
      model: "gpt-5.4",
      options: [
        { id: "reasoning", value: "high" },
        { id: "fast", value: false },
      ],
    },
    defaultThreadEnvMode: "worktree",
    autoPull: true,
    faviconPath: workspace("custom.svg"),
    projectIcon: { kind: "monogram", text: "ÁB", color: "violet" },
    scripts: [
      script("legacy.INVALID", { command: "edited persisted legacy id" }),
      script("verify-2"),
    ],
    pinned: true,
    archived: true,
  };
  const updated = yield* at(1_000, service.update(update));
  for (const key of [
    "title",
    "defaultModelSelection",
    "defaultThreadEnvMode",
    "autoPull",
    "faviconPath",
    "projectIcon",
    "scripts",
  ]) {
    NodeAssert.deepEqual(updated[key], update[key]);
  }
  NodeAssert.equal(updated.createdAt, iso());
  NodeAssert.equal(updated.updatedAt, iso(1_000));
  NodeAssert.equal(updated.pinnedAt, iso(1_000));
  NodeAssert.equal(updated.archivedAt, iso(1_000));
  const afterUpdate = yield* snapshot();
  NodeAssert.deepEqual(yield* at(2_000, service.update(update)), updated);
  NodeAssert.deepEqual(
    yield* snapshot(),
    afterUpdate,
    "accepted update replay must not mutate timestamps",
  );
  const repeated = yield* at(
    3_000,
    service.update({
      projectId: input.projectId,
      commandId: commandId("lifecycle:repeat-pin"),
      pinned: true,
      archived: true,
    }),
  );
  NodeAssert.equal(repeated.pinnedAt, updated.pinnedAt);
  NodeAssert.equal(repeated.archivedAt, updated.archivedAt);
  NodeAssert.deepEqual(repeated.scripts, update.scripts);
  const cleared = yield* at(
    4_000,
    service.update({
      projectId: input.projectId,
      commandId: commandId("lifecycle:clear"),
      defaultModelSelection: null,
      defaultThreadEnvMode: null,
      autoPull: false,
      projectIcon: null,
      faviconPath: null,
      scripts: [],
      pinned: false,
      archived: false,
    }),
  );
  for (const key of [
    "defaultModelSelection",
    "defaultThreadEnvMode",
    "projectIcon",
    "faviconPath",
    "pinnedAt",
    "archivedAt",
  ]) {
    NodeAssert.equal(cleared[key], null);
  }
  NodeAssert.equal(cleared.autoPull, false);
  NodeAssert.deepEqual(cleared.scripts, []);
  NodeAssert.equal(
    Option.getOrThrow(yield* service.getByWorkspaceRoot(`${workspace("lifecycle")}/`)).id,
    input.projectId,
  );
  NodeAssert.equal(Option.getOrThrow(yield* service.getShell(input.projectId)).title, update.title);
  NodeAssert.deepEqual(
    (yield* service.listShells()).map((row) => row.id),
    [input.projectId],
  );
  const deletion = { commandId: commandId("lifecycle:delete"), projectId: input.projectId };
  const deleted = yield* at(5_000, service.delete(deletion));
  NodeAssert.equal(deleted.deletedAt, iso(5_000));
  const afterDelete = yield* snapshot();
  NodeAssert.deepEqual(yield* at(6_000, service.delete(deletion)), deleted);
  NodeAssert.deepEqual(yield* snapshot(), afterDelete);
  NodeAssert.ok(Option.isNone(yield* service.getById(input.projectId)));
  NodeAssert.deepEqual((yield* service.snapshot).projects, []);
  yield* replayProjects();
  NodeAssert.deepEqual(yield* snapshot(), afterDelete);
  return afterDelete;
});

async function lifecycleWithReopen(selection, name) {
  const filename = NodePath.join(temporary, `${name}.sqlite`);
  const committed = await run(lifecycle(), selection, name, filename);
  await run(
    Effect.gen(function* () {
      const service = yield* ProjectService.ProjectService;
      NodeAssert.deepEqual(yield* snapshot(), committed);
      NodeAssert.equal(
        Option.getOrThrow(yield* service.getById(projectId("lifecycle"), { includeDeleted: true }))
          .deletedAt,
        iso(5_000),
      );
      yield* replayProjects();
      NodeAssert.deepEqual(yield* snapshot(), committed);
      const missing = yield* service
        .update({
          projectId: projectId("lifecycle"),
          commandId: commandId("deleted:update"),
          title: "must fail",
        })
        .pipe(Effect.flip);
      NodeAssert.equal(missing._tag, "ProjectNotFoundError");
      NodeAssert.deepEqual(yield* snapshot(), committed);
    }),
    selection,
    `${name}-reopened`,
    filename,
  );
  scenarios.push(
    `${name}: real service create/update/pin/archive/delete, receipt replay and SQLite reopen/project-event replay`,
  );
  return comparable(committed);
}

const rejectionPrecedence = Effect.fn("checkProjectAdapter.rejectionPrecedence")(function* () {
  const service = yield* ProjectService.ProjectService;
  const input = createInput("rejections");
  yield* service.create(input);
  const owner = createInput("owner");
  yield* service.create(owner);
  const errors = [];
  const invalid = {
    projectId: input.projectId,
    commandId: commandId("reject:monogram"),
    projectIcon: { kind: "monogram", text: "ABC", color: "blue" },
    scripts: [script("INVALID")],
    workspaceRoot: owner.workspaceRoot,
  };
  const first = yield* service.update(invalid).pipe(Effect.flip);
  NodeAssert.equal(first._tag, "ProjectInvalidError");
  NodeAssert.equal(first.detail, "Project monograms must contain at most two characters.");
  errors.push(errorView(first));
  const invalidScript = { ...invalid, projectIcon: null, commandId: commandId("reject:script") };
  const second = yield* service.update(invalidScript).pipe(Effect.flip);
  NodeAssert.equal(second._tag, "ProjectInvalidError");
  NodeAssert.match(second.detail, /Script IDs must be/);
  errors.push(errorView(second));
  const conflict = { ...invalidScript, scripts: [], commandId: commandId("reject:workspace") };
  const third = yield* service.update(conflict).pipe(Effect.flip);
  NodeAssert.equal(third._tag, "ProjectConflictError");
  NodeAssert.equal(third.conflictingProjectId, owner.projectId);
  errors.push(errorView(third));
  const duplicate = yield* service
    .create({
      ...input,
      commandId: commandId("reject:duplicate"),
      workspaceRoot: owner.workspaceRoot,
    })
    .pipe(Effect.flip);
  NodeAssert.equal(duplicate._tag, "ProjectInvalidError");
  NodeAssert.match(duplicate.detail, /already exists/);
  errors.push(errorView(duplicate));
  const beforeRetry = yield* snapshot();
  NodeAssert.deepEqual(
    errorView(
      yield* service
        .update({ ...invalid, projectIcon: null, scripts: [], workspaceRoot: input.workspaceRoot })
        .pipe(Effect.flip),
    ),
    errorView(first),
  );
  NodeAssert.deepEqual(
    yield* snapshot(),
    beforeRetry,
    "changed retry payload must not replace a rejected receipt",
  );
  yield* service.delete({ projectId: owner.projectId, commandId: commandId("release:owner") });
  const freed = yield* snapshot();
  NodeAssert.deepEqual(
    errorView(yield* service.update(conflict).pipe(Effect.flip)),
    errorView(third),
  );
  NodeAssert.deepEqual(
    yield* snapshot(),
    freed,
    "free workspace must not erase an earlier rejected outcome",
  );
  NodeAssert.equal(
    Option.getOrThrow(yield* service.getById(input.projectId)).workspaceRoot,
    workspace("rejections"),
  );
  const state = yield* snapshot();
  NodeAssert.equal(state.receipts.filter((receipt) => receipt.status === "rejected").length, 4);
  return { errors, state: comparable(state) };
});

const workspaceCollision = Effect.fn("checkProjectAdapter.workspaceCollision")(function* () {
  const sink = yield* EventSink.EventSinkV2;
  const paths = yield* WorkspacePaths.WorkspacePaths;
  const firstReached = yield* Deferred.make();
  const releaseFirst = yield* Deferred.make();
  const secondNormalized = yield* Deferred.make();
  let normalizations = 0;
  const service = yield* ProjectService.make.pipe(
    Effect.provideService(EventSink.EventSinkV2, {
      ...sink,
      commitProjectCommand: Effect.fn("checkProjectAdapter.gatedCommit")(function* (input) {
        if (input.projectId === projectId("race:first")) {
          yield* Deferred.succeed(firstReached, undefined);
          yield* Deferred.await(releaseFirst);
        }
        return yield* sink.commitProjectCommand(input);
      }),
    }),
    Effect.provideService(WorkspacePaths.WorkspacePaths, {
      ...paths,
      normalizeWorkspaceRoot: Effect.fn("checkProjectAdapter.observeNormalization")(function* (
        ...args
      ) {
        const normalized = yield* paths.normalizeWorkspaceRoot(...args);
        normalizations++;
        if (normalizations === 2) yield* Deferred.succeed(secondNormalized, undefined);
        return normalized;
      }),
    }),
  );
  const first = yield* service.create(createInput("race:first", "race")).pipe(Effect.forkScoped);
  yield* Deferred.await(firstReached);
  const second = yield* service
    .create(createInput("race:second", "race"))
    .pipe(Effect.flip, Effect.forkScoped);
  yield* Deferred.await(secondNormalized);
  yield* Deferred.succeed(releaseFirst, undefined);
  NodeAssert.equal((yield* Fiber.join(first)).id, projectId("race:first"));
  const conflict = yield* Fiber.join(second);
  NodeAssert.equal(conflict._tag, "ProjectConflictError");
  const state = yield* snapshot();
  NodeAssert.equal(state.projects.length, 1);
  NodeAssert.equal(state.events.length, 1);
  NodeAssert.deepEqual(
    state.receipts.map((receipt) => receipt.status),
    ["accepted", "rejected"],
  );
  return comparable(state);
});

const atomicFailures = Effect.fn("checkProjectAdapter.atomicFailures")(function* () {
  const service = yield* ProjectService.ProjectService;
  const sql = yield* SqlClient.SqlClient;
  for (const target of ["event", "row", "receipt-insert", "receipt-update"]) {
    const input = createInput(`atomic:${target}`);
    const before = yield* snapshot();
    if (target === "event") {
      yield* sql`CREATE TRIGGER fail_native_project BEFORE INSERT ON orchestration_events BEGIN SELECT RAISE(ABORT, 'injected event failure'); END`;
    } else if (target === "row") {
      yield* sql`CREATE TRIGGER fail_native_project BEFORE INSERT ON projection_projects BEGIN SELECT RAISE(ABORT, 'injected project row failure'); END`;
    } else if (target === "receipt-insert") {
      yield* sql`CREATE TRIGGER fail_native_project BEFORE INSERT ON orchestration_command_receipts BEGIN SELECT RAISE(ABORT, 'injected receipt insert failure'); END`;
    } else {
      yield* sql`CREATE TRIGGER fail_native_project BEFORE UPDATE ON orchestration_command_receipts BEGIN SELECT RAISE(ABORT, 'injected final receipt failure'); END`;
    }
    const failure = yield* service.create(input).pipe(Effect.flip);
    NodeAssert.equal(failure._tag, "ProjectOperationError");
    NodeAssert.deepEqual(
      yield* snapshot(),
      before,
      `${target}: events, rows and receipts must roll back together`,
    );
    yield* sql`DROP TRIGGER fail_native_project`;
    NodeAssert.equal((yield* service.create(input)).id, input.projectId);
  }
  return comparable(yield* snapshot());
});

function threadCreated(project, id) {
  return {
    id: EventId.make(`seed:${id}`),
    type: "thread.created",
    threadId: id,
    providerInstanceId: instanceId,
    occurredAt: now,
    payload: {
      id,
      projectId: project,
      title: id,
      createdBy: "user",
      creationSource: "web",
      providerInstanceId: instanceId,
      modelSelection: { instanceId, model: "gpt-5.4" },
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: null,
      worktreePath: null,
      activeProviderThreadId: null,
      lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: id },
      forkedFrom: null,
      createdAt: now,
      updatedAt: now,
      archivedAt: null,
      settledOverride: null,
      settledAt: null,
      lastVisitedAt: null,
      deletedAt: null,
    },
  };
}

const childCascade = Effect.fn("checkProjectAdapter.childCascade")(function* (control) {
  const service = yield* ProjectService.ProjectService;
  const sink = yield* EventSink.EventSinkV2;
  const projections = yield* ProjectionStore.ProjectionStoreV2;
  const threads = [ThreadId.make("thread:project:a"), ThreadId.make("thread:project:b")];
  const input = createInput("cascade");
  yield* service.create(input);
  yield* sink.write({ events: threads.map((id) => threadCreated(input.projectId, id)) });
  const deletion = {
    projectId: input.projectId,
    commandId: commandId("cascade:delete"),
    force: true,
  };
  const before = yield* snapshot();
  const notEmpty = yield* service.delete({ ...deletion, force: false }).pipe(Effect.flip);
  NodeAssert.equal(notEmpty._tag, "ProjectNotEmptyError");
  NodeAssert.deepEqual(yield* snapshot(), before);
  if (control !== undefined) {
    yield* control.setMode({ commandType: "project.delete", kind: "exit" });
    const failure = yield* service.delete(deletion).pipe(Effect.flip);
    nativeError(failure);
    const partial = yield* snapshot();
    NodeAssert.ok(Option.isSome(yield* service.getById(input.projectId)));
    NodeAssert.equal(
      partial.events.filter((event) => event.event_type === "thread.deleted").length,
      2,
    );
    NodeAssert.equal(
      partial.events.filter((event) => event.event_type === "project.deleted").length,
      0,
    );
    NodeAssert.equal(
      partial.receipts.filter((receipt) => receipt.command_id === deletion.commandId).length,
      0,
    );
    NodeAssert.equal(partial.outbox.length, 6);
    for (const id of threads)
      NodeAssert.ok((yield* projections.getThreadProjection(id)).thread.deletedAt !== null);
    yield* control.setMode();
    yield* service.delete(deletion);
    const retried = yield* snapshot();
    NodeAssert.deepEqual(
      retried.events.filter((event) => event.aggregate_kind === "thread"),
      partial.events.filter((event) => event.aggregate_kind === "thread"),
    );
    NodeAssert.deepEqual(
      retried.outbox,
      partial.outbox,
      "project retry must not repeat already durable child cleanup",
    );
  } else {
    yield* service.delete(deletion);
  }
  const finished = yield* snapshot();
  NodeAssert.equal(
    finished.events.filter((event) => event.event_type === "project.deleted").length,
    1,
  );
  NodeAssert.equal(
    finished.events.filter((event) => event.event_type === "thread.deleted").length,
    2,
  );
  NodeAssert.equal(finished.outbox.length, 6);
  yield* service.delete(deletion);
  NodeAssert.deepEqual(yield* snapshot(), finished);
  const replay = yield* ProjectionReplay.ProjectionReplay;
  NodeAssert.equal((yield* replay.rebuild).valid, true);
  yield* replayProjects();
  NodeAssert.deepEqual(yield* snapshot(), finished);
  return comparable(finished);
});

const directPayloads = Effect.fn("checkProjectAdapter.directPayloads")(function* () {
  const native = yield* NativeProjectCommands.make();
  const nested = { id: "opaque 💬", value: [null, false, 1.25, { updatedAt: "not a date" }] };
  const row = {
    projectId: projectId("direct"),
    title: "Retained",
    workspaceRoot: workspace("direct"),
    defaultModelSelection: null,
    defaultThreadEnvMode: null,
    autoPull: false,
    faviconPath: null,
    projectIcon: null,
    scripts: [script("legacy.INVALID", { future: nested })],
    createdAt: iso(),
    updatedAt: iso(),
    pinnedAt: iso(-1_000),
    archivedAt: iso(-2_000),
    deletedAt: null,
    future: nested,
  };
  const commands = [
    {
      type: "project.create",
      projectId: row.projectId,
      commandId: commandId("direct:create"),
      title: "Full script payload",
      workspaceRoot: row.workspaceRoot,
      scripts: row.scripts,
    },
    {
      type: "project.meta.update",
      projectId: row.projectId,
      commandId: commandId("direct:update"),
      scripts: [script("legacy.INVALID", { future: nested })],
      defaultModelSelection: {
        instanceId,
        model: "gpt-5.4",
        options: [{ id: "mode", value: "high", future: nested }],
        future: nested,
      },
      projectIcon: { kind: "monogram", text: "ÁB", color: "blue", future: nested },
      pinned: true,
      archived: true,
    },
    {
      type: "project.meta.update",
      projectId: row.projectId,
      commandId: commandId("direct:omitted"),
    },
    {
      type: "project.meta.update",
      projectId: row.projectId,
      commandId: commandId("direct:null"),
      defaultModelSelection: null,
      defaultThreadEnvMode: null,
      projectIcon: null,
      faviconPath: null,
    },
    { type: "project.delete", projectId: row.projectId, commandId: commandId("direct:delete") },
  ];
  for (const command of commands) {
    const input = {
      command,
      state: {
        project: command.type === "project.create" ? undefined : row,
        workspaceOwner: undefined,
      },
      eventId: EventId.make(`event:${command.commandId}`),
      now,
    };
    NodeAssert.deepEqual(
      yield* native.planProjectCommand(input),
      ProjectCommands.planProjectCommand(input),
    );
  }
  const missing = {
    command: commands[1],
    state: { project: undefined, workspaceOwner: row },
    eventId: EventId.make("event:missing"),
    now,
  };
  const result = yield* native.planProjectCommand(missing);
  NodeAssert.ok(Result.isFailure(result));
  NodeAssert.equal(result.failure._tag, "ProjectCommandMissingProjectError");
  NodeAssert.deepEqual(result, ProjectCommands.planProjectCommand(missing));
});

async function fixture(name, source) {
  const path = NodePath.join(temporary, name);
  await NodeFSP.writeFile(path, `#!${process.execPath}\n${source}`, { mode: 0o700 });
  return path;
}

async function controlledForwarder() {
  const modePath = NodePath.join(temporary, "project-native-mode.json");
  const readyPath = NodePath.join(temporary, "project-native-ready.json");
  const path = await fixture(
    "controlled-project-native",
    `
import * as fs from "node:fs";
import * as child from "node:child_process";
const input = fs.readFileSync(0);
if (Object.keys(process.env).length !== 0 || process.argv.length !== 2) {
  process.stderr.write("Project planner inherited ambient environment or arguments\\n");
  process.exit(78);
}
const request = JSON.parse(input.toString("utf8"));
const mode = JSON.parse(fs.readFileSync(${JSON.stringify(modePath)}, "utf8"));
const target = mode.commandType === request.command.type;
if (target && mode.kind === "invalid-json") {
  process.stdout.write("{invalid\\n");
} else if (target && mode.kind === "exit") {
  process.stderr.write("synthetic project planner failure\\n"); process.exitCode = 7;
} else if (target && mode.kind === "hang") {
  fs.watch(${JSON.stringify(temporary)}, () => {});
  fs.writeFileSync(${JSON.stringify(readyPath + ".tmp")}, String(process.pid));
  fs.renameSync(${JSON.stringify(readyPath + ".tmp")}, ${JSON.stringify(readyPath)});
} else {
  const forwarded = target && mode.kind === "success-existing-create"
    ? { ...request, state: { ...request.state, project: null } }
    : target && mode.kind === "success-deleted"
      ? { ...request, state: { ...request.state, project: { ...request.state.project, deletedAt: null } } }
      : undefined;
  const native = child.spawnSync(${JSON.stringify(executable)}, [], { input: forwarded === undefined ? input : JSON.stringify(forwarded) + "\\n", env: {}, maxBuffer: 128 * 1024 * 1024 });
  if (native.error) throw native.error;
  if (native.status !== 0 || native.stderr.length !== 0) throw new Error("Actual Lust planner failed before fault injection");
  const response = JSON.parse(native.stdout.toString("utf8"));
  if (target) {
    if (mode.kind === "event-id") response.ok.eventId = "event:foreign";
    if (mode.kind === "aggregate") response.ok.aggregateId = "project:foreign";
    if (mode.kind === "payload-project") response.ok.payload.projectId = "project:foreign";
    if (mode.kind === "command") response.ok.commandId = "command:foreign";
    if (mode.kind === "correlation") response.ok.correlationId = "command:foreign";
    if (mode.kind === "timestamp") response.ok.occurredAt = "2000-01-01T00:00:00.000Z";
    if (mode.kind === "title") response.ok.payload.title = "forged project title";
    if (mode.kind === "omitted-null") response.ok.payload.faviconPath = null;
    if (mode.kind === "drop-script") response.ok.payload.scripts.pop();
    if (mode.kind === "model-option") response.ok.payload.defaultModelSelection.options[0].value = "forged";
    if (mode.kind === "rejection-scope") response.error.projectId = "project:foreign";
    if (mode.kind === "rejection-detail") response.error.detail = "forged rejection";
    if (mode.kind === "missing-active") {
      delete response.ok;
      response.error = { _tag: "ProjectCommandMissingProjectError", commandType: request.command.type, projectId: request.command.projectId };
    }
    if (mode.kind === "duplicate-absent") {
      delete response.ok;
      response.error = { _tag: "ProjectCommandInvariantError", commandType: request.command.type, detail: "Project '" + request.command.projectId + "' already exists and cannot be created twice." };
    }
    if (mode.kind === "extra") response.unexpected = true;
  }
  process.stdout.write(JSON.stringify(response) + "\\n");
  if (target && mode.kind === "trailing") process.stdout.write("{}\\n");
}
`,
  );
  const setMode = Effect.fn("checkProjectAdapter.setMode")(function* (mode = {}) {
    yield* Effect.promise(() => NodeFSP.writeFile(modePath, JSON.stringify(mode)));
  });
  await Effect.runPromise(setMode());
  return { path, readyPath, setMode };
}

async function nativeFailureChecks() {
  const control = await controlledForwarder();
  const ambientName = "T3_CODE_LS_PROJECT_TEST_AMBIENT";
  const previousAmbient = process.env[ambientName];
  process.env[ambientName] = "must-not-reach-the-planner";
  try {
    await run(
      Effect.gen(function* () {
        const service = yield* ProjectService.ProjectService;
        NodeAssert.equal(
          (yield* service.create(createInput("isolated"))).id,
          projectId("isolated"),
        );
      }),
      control.path,
      "isolated",
    );
  } finally {
    if (previousAmbient === undefined) delete process.env[ambientName];
    else process.env[ambientName] = previousAmbient;
  }
  scenarios.push(
    "native executable receives empty environment and no command or payload arguments",
  );

  for (const [selection, stage] of [
    ["", "configuration"],
    ["relative-project-program", "configuration"],
    [NodePath.join(temporary, "missing-project-program"), "process"],
  ]) {
    await run(
      Effect.gen(function* () {
        const service = yield* ProjectService.ProjectService;
        const before = yield* snapshot();
        const failure = yield* service.create(createInput("invalid-config")).pipe(Effect.flip);
        nativeError(failure, stage);
        NodeAssert.deepEqual(yield* snapshot(), before);
      }),
      selection,
      `invalid-config-${scenarios.length}`,
    );
  }
  scenarios.push(
    "configured empty, relative and missing executables fail closed without durable receipts or fallback",
  );

  for (const [kind, stage] of [
    ["invalid-json", "response"],
    ["trailing", "response"],
    ["exit", "response"],
    ["event-id", "response"],
    ["aggregate", "response"],
    ["payload-project", "response"],
    ["command", "response"],
    ["correlation", "response"],
    ["timestamp", "response"],
    ["title", "response"],
    ["extra", "response"],
    ["duplicate-absent", "response"],
  ]) {
    await run(
      Effect.gen(function* () {
        const service = yield* ProjectService.ProjectService;
        const before = yield* snapshot();
        yield* control.setMode({ commandType: "project.create", kind });
        const failure = yield* service.create(createInput(`fault:${kind}`)).pipe(Effect.flip);
        nativeError(failure, stage);
        NodeAssert.deepEqual(yield* snapshot(), before);
        yield* control.setMode();
        NodeAssert.equal(
          (yield* service.create(createInput(`fault:${kind}`))).id,
          projectId(`fault:${kind}`),
        );
      }),
      control.path,
      `fault-${kind}`,
    );
  }
  scenarios.push(
    "malformed output, failure, wrong event/scope/correlation/timestamp/title and extra envelope fields commit no state",
  );

  for (const kind of [
    "omitted-null",
    "drop-script",
    "model-option",
    "rejection-detail",
    "missing-active",
  ]) {
    await run(
      Effect.gen(function* () {
        const service = yield* ProjectService.ProjectService;
        const input = createInput(`update-fault:${kind}`);
        yield* service.create(input);
        const before = yield* snapshot();
        const change =
          kind === "drop-script"
            ? { scripts: [script("one"), script("two")] }
            : kind === "model-option"
              ? {
                  defaultModelSelection: {
                    instanceId,
                    model: "gpt-5.4",
                    options: [{ id: "reasoning", value: "high" }],
                  },
                }
              : kind === "rejection-detail"
                ? { scripts: [script("INVALID")] }
                : {};
        yield* control.setMode({ commandType: "project.meta.update", kind });
        const failure = yield* service
          .update({
            projectId: input.projectId,
            commandId: commandId(`update-fault:${kind}`),
            ...change,
          })
          .pipe(Effect.flip);
        nativeError(failure);
        NodeAssert.deepEqual(yield* snapshot(), before);
        yield* control.setMode();
      }),
      control.path,
      `update-fault-${kind}`,
    );
  }
  scenarios.push(
    "native response cannot invent null fields, discard scripts, change model options or forge impossible domain rejections",
  );

  await run(
    Effect.gen(function* () {
      const service = yield* ProjectService.ProjectService;
      const input = createInput("missing-association");
      yield* service.create(input);
      const existing = yield* snapshot();
      yield* control.setMode({ commandType: "project.create", kind: "success-existing-create" });
      const duplicate = yield* service
        .create({ ...input, commandId: commandId("existing:forged-create") })
        .pipe(Effect.flip);
      nativeError(duplicate);
      NodeAssert.deepEqual(yield* snapshot(), existing);
      yield* control.setMode();
      yield* service.delete({
        projectId: input.projectId,
        commandId: commandId("missing-association:delete"),
      });
      const before = yield* snapshot();
      for (const kind of ["rejection-scope", "success-deleted"]) {
        yield* control.setMode({ commandType: "project.delete", kind });
        const failure = yield* service
          .delete({
            projectId: input.projectId,
            commandId: commandId(`missing-association:${kind}`),
          })
          .pipe(Effect.flip);
        nativeError(failure);
        NodeAssert.deepEqual(yield* snapshot(), before);
      }
      yield* control.setMode();
    }),
    control.path,
    "rejection-association",
  );
  scenarios.push(
    "native successes must match existing/deleted project state and rejection must retain its project association",
  );

  await run(childCascade(control), control.path, "native-cascade-failure");
  scenarios.push(
    "failed native project deletion preserves committed child deletions and retries without duplicate events or cleanup",
  );

  let readyResolve;
  let readyReject;
  const ready = new Promise((resolve, reject) => {
    readyResolve = resolve;
    readyReject = reject;
  });
  const watcher = NodeFS.watch(temporary, (_event, filename) => {
    if (filename === NodePath.basename(control.readyPath)) {
      NodeFSP.readFile(control.readyPath, "utf8").then(
        (value) => readyResolve(Number(value)),
        readyReject,
      );
    }
  });
  watcher.on("error", readyReject);
  try {
    await run(
      Effect.gen(function* () {
        const service = yield* ProjectService.ProjectService;
        const input = createInput("interrupted");
        const before = yield* snapshot();
        yield* control.setMode({ commandType: "project.create", kind: "hang" });
        const running = yield* service.create(input).pipe(Effect.forkScoped);
        const pid = yield* Effect.promise(() => ready);
        NodeAssert.ok(Number.isSafeInteger(pid) && pid > 0);
        yield* Fiber.interrupt(running);
        const outcome = yield* Fiber.await(running);
        NodeAssert.ok(Exit.isFailure(outcome) && Cause.hasInterruptsOnly(outcome.cause));
        NodeAssert.throws(() => process.kill(pid, 0), { code: "ESRCH" });
        NodeAssert.deepEqual(yield* snapshot(), before);
        yield* control.setMode();
        NodeAssert.equal((yield* service.create(input)).id, input.projectId);
      }),
      control.path,
      "interrupted",
    );
    scenarios.push(
      "interrupted native process is reaped without durable writes and releases project and workspace locks",
    );
  } finally {
    watcher.close();
  }
}

try {
  const baseline = await lifecycleWithReopen(undefined, "upstream");
  const programs = [
    ["domain rejection precedence and durable rejection replay", rejectionPrecedence],
    ["serialized concurrent workspace claims", workspaceCollision],
    ["atomic event/row/receipt failure rollback", atomicFailures],
    ["durable child cascade and complete project/thread replay", childCascade],
  ];
  const expected = [];
  for (const [name, program] of programs) {
    expected.push(await run(program(), undefined, `upstream-${scenarios.length}`));
    scenarios.push(`upstream: ${name}`);
  }
  if (!baselineOnly) {
    NodeAssert.deepEqual(await lifecycleWithReopen(executable, "native"), baseline);
    for (let index = 0; index < programs.length; index++) {
      const [name, program] = programs[index];
      NodeAssert.deepEqual(
        await run(program(), executable, `native-${scenarios.length}`),
        expected[index],
      );
      scenarios.push(`native: ${name}`);
    }
    await run(directPayloads(), executable, "direct-payloads", undefined, true);
    scenarios.push(
      "actual adapter preserves full nested script/model/icon payloads, omitted/null fields and domain errors against upstream",
    );
    await nativeFailureChecks();
  }
  const sources = [
    "apps/server/src/orchestration-v2/NativeProjectCommands.ts",
    "apps/server/src/orchestration-v2/ProjectCommands.ts",
    "apps/server/src/orchestration-v2/ProjectCommands.test.ts",
    "apps/server/src/project/ProjectService.ts",
    "apps/server/src/project/ProjectService.test.ts",
    "apps/server/src/project/ProjectService.deletion.test.ts",
    "apps/server/src/project/ProjectEnrichmentService.ts",
    "apps/server/src/workspace/WorkspacePaths.ts",
    "apps/server/src/orchestration-v2/runtimeLayer.ts",
    "apps/server/src/orchestration-v2/EventSink.ts",
    "apps/server/src/orchestration-v2/EventStore.ts",
    "apps/server/src/orchestration-v2/ProjectStore.ts",
    "apps/server/src/orchestration-v2/ProjectionStore.ts",
    "apps/server/src/orchestration-v2/CommandReceiptStore.ts",
    "apps/server/src/orchestration-v2/EffectOutbox.ts",
    "apps/server/src/orchestration-v2/ThreadDeletion.ts",
    "apps/server/src/orchestration-v2/ThreadCommandExecutor.ts",
    "apps/server/src/orchestration-v2/legacy/LegacyV1ThreadImporter.ts",
    "apps/server/src/orchestration-v2/testkit/ProjectionReplay.ts",
    "apps/server/src/persistence/Sqlite.ts",
    "apps/server/src/persistence/OrchestrationEventStore.ts",
    "apps/server/src/persistence/OrchestrationCommandReceipts.ts",
    "apps/server/src/persistence/Migrations.ts",
    "apps/server/src/config.ts",
    "apps/server/src/serverSettings.ts",
    "apps/server/src/processRunner.ts",
    "packages/provider-core/src/server/IdAllocator.ts",
    "packages/provider-core/src/server/collectStreamText.ts",
    "packages/shared/src/nodeSqliteClient.ts",
    "packages/shared/src/HostProcess.ts",
    "packages/shared/src/KeyedLock.ts",
    "packages/contracts/src/project.ts",
    "packages/contracts/src/applicationEvent.ts",
    "packages/contracts/src/keybindings.ts",
    "packages/contracts/src/model.ts",
    "packages/contracts/src/modelSelection.ts",
    "packages/contracts/src/baseSchemas.ts",
    "native/t3-code-ls/crates/core/src/json.rs",
    "native/t3-code-ls/crates/core/src/project_commands.rs",
    "native/t3-code-ls/crates/core/src/project_commands_wire.rs",
    "native/t3-code-ls/crates/core/src/bin/t3-code-ls-project.rs",
    "native/t3-code-ls/tools/check-project-adapter.mjs",
    "native/t3-code-ls/tools/project-tsconfig.json",
    "native/t3-code-ls/tools/install-server-tests.mjs",
    "native/t3-code-ls/tools/server-test-runtime/package.json",
    "native/t3-code-ls/tools/server-test-runtime/package-lock.json",
    "native/t3-code-ls/upstream.lock.json",
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
    actual_project_service: true,
    actual_runtime_layer: true,
    actual_sqlite: true,
    actual_workspace_paths: true,
    actual_project_event_replay: true,
    actual_thread_cascade: true,
    actual_native_project_planner: !baselineOnly,
    note: "Project command planning integration only. The existing TypeScript service owns persistence, enrichment and child deletion; project deletion intentionally preserves already committed child progress on a later failure. This does not establish a complete application, mobile or Iroh port.",
  };
  if (proofPath !== undefined && proofPath !== "--baseline-only")
    await NodeFSP.writeFile(proofPath, JSON.stringify(proof, null, 2) + "\n");
  console.log(
    `Project adapter: ${scenarios.length} real service/persistence scenarios passed${baselineOnly ? " (upstream baseline only)" : ""}.`,
  );
} finally {
  await NodeFSP.rm(temporary, { recursive: true, force: true });
}
