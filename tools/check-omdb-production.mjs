#!/usr/bin/env node
import * as NodeAssert from "node:assert/strict";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeChildProcess from "node:child_process";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Logger from "effect/Logger";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { AuthStandardClientScopes } from "@t3tools/contracts";
import { createFixture } from "./database-fixture.mjs";
import { finishProof } from "./redb-domain-fixture.mjs";
import {
  sampleEvents,
  fixedDateTime,
  fixedTime,
  jsonPayloads,
  makeEvent,
} from "./redb-event-sink-fixtures.mjs";
import { inspectNativeReplay } from "../apps/web/scripts/hybrid-client-database.ts";
import { verifyNativeSchemaUpgrade } from "./omdb-schema-upgrade-fixture.mjs";

const [executable, proofPath] = process.argv.slice(2);
NodeAssert.ok(executable && NodePath.isAbsolute(executable));
NodeChildProcess.execFileSync(process.execPath, ["tools/generate-omdb-schema.mjs", "--check"], {
  stdio: "inherit",
});
// Select exactly as production does. Other incremental helpers are unrelated
// to this database acceptance and keep their retained upstream implementation.
process.env.T3_CODE_LS_DB_EXECUTABLE = executable;
process.env.T3_CODE_LS_IMPLEMENTATION = "upstream";
const Selected = await import("../apps/server/src/persistence/SelectedPersistence.ts");
const Runtime = await import("../apps/server/src/orchestration-v2/selectedRuntimeLayer.ts");
const Config = await import("../apps/server/src/config.ts");
const Projection = await import("../apps/server/src/orchestration-v2/ProjectionStore.ts");
const Sink = await import("../apps/server/src/orchestration-v2/EventSink.ts");
const Search = await import("../apps/server/src/orchestration-v2/ThreadSearch.ts");
const Sessions = await import("../apps/server/src/persistence/AuthSessions.ts");
const decodeCreateAuthSessionInput = Schema.decodeUnknownSync(Sessions.CreateAuthSessionInput);
const EnvironmentAuth = await import("../apps/server/src/auth/EnvironmentAuth.ts");
const ServerEnvironment = await import("../apps/server/src/environment/ServerEnvironment.ts");
const ServerSecretStore = await import("../apps/server/src/auth/ServerSecretStore.ts");
const ManagedPolicy = await import("../apps/server/src/policy/ManagedPolicy.ts");
const Transactions = await import("../apps/server/src/persistence/PersistenceTransactions.ts");
const Sqlite = await import("../apps/server/src/persistence/Sqlite.ts");
NodeAssert.equal(Selected.selection.kind, "omdb");

const fixture = await createFixture();
const checks = [];
const completedAt = "2026-11-10T12:00:01.000Z";
const completionEvents = [
  ["run.updated", { ...jsonPayloads.run, status: "completed", completedAt }],
  // ACP's authoritative inverse link remains present while its optional reverse link is null.
  [
    "run-attempt.updated",
    { ...jsonPayloads.attempt, status: "completed", completedAt, providerTurnId: null },
  ],
  ["provider-turn.updated", { ...jsonPayloads.providerTurn, status: "completed", completedAt }],
  [
    "message.updated",
    {
      ...jsonPayloads.message,
      text: "This turn is a replay.",
      streaming: false,
      updatedAt: completedAt,
    },
  ],
].map(([type, payload]) =>
  makeEvent(type, payload, { id: `completed:${type}`, occurredAt: completedAt }),
);
let replayExpectation;
const configLayer = Config.layerTest(fixture.root, fixture.root);
const graph = Layer.mergeAll(
  Runtime.layerEventSink,
  Selected.projections,
  Selected.projects,
  Selected.search,
  Selected.authSessions,
  Selected.authPairing,
  Selected.viewedFiles,
  Selected.reviewGuides,
  Selected.layerConfig,
  EnvironmentAuth.layerSelected,
).pipe(
  Layer.provideMerge(ManagedPolicy.layerTest()),
  Layer.provideMerge(ServerSecretStore.layer),
  Layer.provideMerge(ServerEnvironment.layerIdentity),
  Layer.provideMerge(configLayer),
  Layer.provideMerge(NodeCrypto.layer),
  Layer.provideMerge(NodeServices.layer),
);
const run = (use) =>
  Effect.runPromise(
    Effect.gen(function* () {
      const context = yield* Layer.build(graph);
      NodeAssert.ok(Option.isNone(Context.getOption(context, SqlClient.SqlClient)));
      return yield* use(context).pipe(Effect.provideContext(context));
    }).pipe(Effect.scoped, Effect.provide(Logger.layer([]))),
  );

try {
  await run((context) =>
    Effect.gen(function* () {
      const config = Context.get(context, Config.ServerConfig);
      NodeAssert.equal(config.dbPath, NodePath.join(fixture.root, "userdata", "statev2.omdb"));
      const sink = Context.get(context, Sink.EventSinkV2);
      yield* sink.commitProjectCommand({
        commandId: "production:project",
        projectId: "project:main",
        commandType: "project.create",
        acceptedAt: fixedDateTime,
        event: {
          eventId: "production:project-event",
          aggregateKind: "project",
          aggregateId: "project:main",
          type: "project.created",
          occurredAt: fixedTime,
          commandId: "production:project",
          causationEventId: null,
          correlationId: "production:project",
          metadata: {},
          payload: {
            projectId: "project:main",
            title: "Omdb production",
            workspaceRoot: fixture.root,
            defaultModelSelection: null,
            scripts: [],
            createdAt: fixedTime,
            updatedAt: fixedTime,
          },
        },
      });
      const committed = yield* sink.commitCommand({
        commandId: "production:thread",
        threadId: "thread:main",
        commandType: "fixture.command",
        acceptedAt: fixedDateTime,
        events: sampleEvents,
        effects: [],
      });
      NodeAssert.equal(committed.committed, true);
      yield* sink.write({ events: completionEvents });
      const projections = Context.get(context, Projection.ProjectionStoreV2);
      const tx = Context.get(context, Transactions.PersistenceTransactions);
      yield* tx.withReadTransaction(
        Effect.gen(function* () {
          NodeAssert.equal((yield* projections.getThread("thread:main")).id, "thread:main");
          const shell = yield* projections.getShellSnapshot();
          NodeAssert.ok(shell.threads.some((thread) => thread.id === "thread:main"));
        }),
      );
      const snapshot = yield* projections.getThreadSnapshot("thread:main");
      replayExpectation = {
        projectId: "project:main",
        threadId: "thread:main",
        runId: "run:main",
        attemptId: "attempt:main",
        providerTurnId: "turn:main",
        assistantMessageId: "message:main",
        snapshotSequence: snapshot.snapshotSequence,
        runCompletedAt: completedAt,
        attemptCompletedAt: completedAt,
        providerTurnCompletedAt: completedAt,
      };
      NodeAssert.equal(
        inspectNativeReplay({
          executable,
          stateDirectory: fixture.root,
          expected: replayExpectation,
        }).engine,
        "omdb",
      );
      const auth = Context.get(context, EnvironmentAuth.EnvironmentAuth);
      const issued = yield* auth.issueSession({
        scopes: AuthStandardClientScopes,
        label: "native production",
      });
      NodeAssert.ok(
        (yield* auth.listSessions()).some((session) => session.sessionId === issued.sessionId),
      );
      yield* Context.get(context, Sessions.AuthSessionRepository).create(
        decodeCreateAuthSessionInput({
          sessionId: "production:persisted-auth",
          subject: "owner",
          scopes: ["orchestration:read"],
          method: "bearer-access-token",
          client: {
            label: "native",
            ipAddress: "127.0.0.1",
            userAgent: "gate",
            deviceType: "desktop",
            os: null,
            browser: null,
          },
          issuedAt: fixedTime,
          expiresAt: "2099-01-01T00:00:00.000Z",
        }),
      );
      // The full search service reads live projection writes through the selected graph.
      yield* Context.get(context, Search.ThreadSearch).search({ query: "Native" });
    }),
  );
  checks.push(
    "production-selected event sink, complete projections, search and authentication share native persistence without SqlClient",
  );
  NodeAssert.equal(
    inspectNativeReplay({
      executable,
      stateDirectory: fixture.root,
      expected: replayExpectation,
      closed: true,
    }).closedDatabaseUnchanged,
    true,
  );
  checks.push(
    "client acceptance inspector reads six real canonical rows and shared event heads both live and closed without SQLite or file mutation",
  );
  await run((context) =>
    Effect.gen(function* () {
      const projections = Context.get(context, Projection.ProjectionStoreV2);
      NodeAssert.equal((yield* projections.getThread("thread:main")).id, "thread:main");
      NodeAssert.ok(
        Option.isSome(
          yield* Context.get(context, Sessions.AuthSessionRepository).getById({
            sessionId: "production:persisted-auth",
          }),
        ),
      );
      const sink = Context.get(context, Sink.EventSinkV2);
      const replay = yield* sink.commitCommand({
        commandId: "production:thread",
        threadId: "thread:main",
        commandType: "fixture.command",
        acceptedAt: fixedDateTime,
        events: sampleEvents,
        effects: [],
      });
      NodeAssert.equal(replay.committed, false);
      NodeAssert.equal(
        inspectNativeReplay({
          executable,
          stateDirectory: fixture.root,
          expected: replayExpectation,
        }).engine,
        "omdb",
      );
    }),
  );
  checks.push(
    "restart preserves committed threads and authentication; duplicate command replay does not write again",
  );
  const files = await NodeFSP.readdir(NodePath.join(fixture.root, "userdata"));
  NodeAssert.ok(files.includes("statev2.omdb"));
  NodeAssert.ok(!files.some((file) => /^state(?:v2)?\.sqlite(?:-(?:wal|shm))?$/.test(file)));
  NodeAssert.equal(
    await NodeFSP.readFile(NodePath.join(fixture.root, "userdata", "server-owner.sqlite"), "utf8"),
    "t3code-omdb-owner-v1\n",
  );
  checks.push(
    "fresh production graph creates omdb only and records the older-server activation fence",
  );
  const refusal = await Effect.runPromise(
    Effect.exit(
      Layer.build(
        Sqlite.layerConfig.pipe(Layer.provide(configLayer), Layer.provide(NodeServices.layer)),
      ),
    ).pipe(Effect.scoped),
  );
  NodeAssert.equal(refusal._tag, "Failure");
  NodeAssert.ok(JSON.stringify(refusal).includes("NativeDatabaseAuthoritative"));
  checks.push(
    "retained SQLite configuration refuses an authoritative omdb home instead of forking state",
  );
  checks.push(await verifyNativeSchemaUpgrade(executable));
  await finishProof({
    executable,
    fixture,
    proofPath,
    checks,
    oracles: [],
    sourceHash: null,
    sourcePaths: [
      "apps/server/src/server.ts",
      "apps/server/src/config.ts",
      "apps/server/src/persistence/Omdb.ts",
      "apps/server/src/persistence/redb_schema_upgrade.rs",
      "apps/server/src/persistence/redb_import.rs",
      "apps/server/src/persistence/migrateSqliteToOmdb.ts",
      "apps/server/src/persistence/redb_bootstrap.rs",
      "apps/server/src/persistence/omdb-schema.json",
      "apps/server/src/persistence/omdb-schema-80.json",
      "apps/server/src/persistence/omdb-schema-88.json",
      "tools/omdb-schema-upgrade-fixture.mjs",
      "apps/server/src/persistence/SelectedPersistence.ts",
      "apps/server/src/persistence/Sqlite.ts",
      "apps/server/src/auth/EnvironmentAuth.ts",
      "apps/server/src/auth/SessionStore.ts",
      "apps/server/src/auth/PairingGrantStore.ts",
      "apps/server/src/orchestration-v2/runtimeLayer.ts",
      "apps/server/src/orchestration-v2/nativeRuntimeLayer.ts",
      "apps/server/src/orchestration-v2/selectedRuntimeLayer.ts",
      "packages/shared/src/applicationDatabase.ts",
      "apps/web/scripts/hybrid-client-database.ts",
      "apps/web/scripts/verify-hybrid-client.ts",
    ],
  });
} finally {
  await fixture.close();
}
