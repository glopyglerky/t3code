#!/usr/bin/env node
import * as NodeAssert from "node:assert/strict";
import * as NodeChildProcess from "node:child_process";
import * as NodeCrypto from "node:crypto";
import * as NodeEvents from "node:events";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeReadline from "node:readline";
import * as NodeSqlite from "node:sqlite";
import * as NodeURL from "node:url";

import { createFixture } from "./database-fixture.mjs";

const [database, planner, proofPath] = process.argv.slice(2);
for (const [name, executable] of Object.entries({ database, planner })) {
  NodeAssert.ok(
    executable && NodePath.isAbsolute(executable),
    `Pass an absolute ${name} executable path`,
  );
  await NodeFSP.access(executable, NodeFSP.constants.X_OK);
}
const fixture = await createFixture();
const { root } = fixture;
const checks = [];
const clients = new Set();
const hash = (bytes) => NodeCrypto.createHash("sha256").update(bytes).digest("hex");
const checked = (description) => checks.push(description);

function client(mode = "connect", selectedPlanner = planner) {
  const child = NodeChildProcess.spawn(database, fixture.cliArgs(mode), {
    env: { T3_CODE_LS_PROJECT_STORE_EXECUTABLE: selectedPlanner },
    stdio: ["pipe", "pipe", "pipe"],
  });
  const pending = [];
  let errors = "";
  let exited = false;
  child.stderr.on("data", (data) => {
    errors = (errors + data).slice(-8192);
  });
  const fail = (error) => {
    for (const request of pending.splice(0)) request.reject(error);
  };
  child.on("error", fail);
  const closed = NodeEvents.once(child, "close").then(([code, signal]) => {
    exited = true;
    fail(new Error(`Database client closed (${code ?? signal}): ${errors}`));
    return { code, signal, errors };
  });
  NodeReadline.createInterface({ input: child.stdout }).on("line", (line) => {
    const request = pending.shift();
    if (!request) {
      fail(new Error("Unsolicited database response"));
      child.kill();
      return;
    }
    try {
      request.resolve(JSON.parse(line));
    } catch (error) {
      request.reject(error);
    }
  });
  const api = {
    send(value) {
      return api.raw(JSON.stringify(value) + "\n");
    },
    raw(frame) {
      if (exited) return Promise.reject(new Error("Database client has exited"));
      return new Promise((resolve, reject) => {
        const timeout = setTimeout(() => {
          child.kill();
          reject(new Error("Database request deadline exceeded"));
        }, 45_000);
        pending.push({
          resolve: (value) => {
            clearTimeout(timeout);
            resolve(value);
          },
          reject: (error) => {
            clearTimeout(timeout);
            reject(error);
          },
        });
        child.stdin.write(frame, (error) => {
          if (error) fail(error);
        });
      });
    },
    async close() {
      if (!exited) child.stdin.end();
      const result = await closed;
      clients.delete(api);
      NodeAssert.equal(result.code, 0, result.errors);
      NodeAssert.equal(result.errors, "");
    },
    async kill() {
      if (!exited) child.kill("SIGKILL");
      await closed;
      clients.delete(api);
    },
  };
  clients.add(api);
  return api;
}

function successful(response) {
  NodeAssert.equal(response.ok, true, JSON.stringify(response));
  return response.result;
}
const snapshot = async (connection) => successful(await connection.send({ operation: "snapshot" }));
const events = async (connection) =>
  successful(await connection.send({ operation: "events", after_sequence: 0, limit: 100 }));
const project = (command, eventId, now = 1794139200123) => ({
  operation: "project",
  request: { command, event_id: eventId, now },
});
const create = (commandId, projectId, workspaceRoot) => ({
  type: "project.create",
  commandId,
  projectId,
  workspaceRoot,
  title: "Rust project",
});

try {
  for (const args of [
    ["local", fixture.databasePath],
    ["initialize-loopback", "ws://127.0.0.1:8000"],
    ["loopback", "ws://127.0.0.1:8000"],
    ["remote", "wss://example.invalid"],
  ]) {
    const rejected = NodeChildProcess.spawnSync(database, args, {
      cwd: root,
      input: '{"operation":"snapshot"}\n',
      encoding: "utf8",
      env: {},
      timeout: 10_000,
    });
    NodeAssert.equal(rejected.status, 1, rejected.stderr);
    NodeAssert.equal(rejected.stdout, "");
    NodeAssert.notEqual(rejected.stderr, "");
    NodeAssert.deepEqual(await NodeFSP.readdir(root), []);
  }
  checked("ordinary open requires an existing store and removed network modes cannot create one");
  let left = client("initialize");
  NodeAssert.deepEqual(await snapshot(left), { revision: 0, last_sequence: 0, projections: [] });
  let right = client();
  checked("explicit schema initialization and empty durable store");

  const createCommand = create("create-one", "one", "/fixture/one");
  const created = successful(await left.send(project(createCommand, "created-one")));
  NodeAssert.equal(created.replayed, false);
  NodeAssert.equal(created.receipt.result.ok.project.title, "Rust project");
  NodeAssert.equal(created.receipt.result.ok.project.defaultModelSelection, null);
  NodeAssert.equal(created.receipt.first_sequence, 1);
  NodeAssert.deepEqual(
    (await snapshot(right)).projections[0].payload,
    created.receipt.result.ok.project,
  );
  checked(
    "Lust project creation atomically writes event, projection, and receipt visible to another process",
  );

  const replay = successful(
    await right.send(project(createCommand, "unused-retry-event", 1794139999999)),
  );
  NodeAssert.equal(replay.replayed, true);
  NodeAssert.deepEqual(replay.receipt, created.receipt);
  NodeAssert.equal((await events(left)).length, 1);
  checked("same command replays the original timestamp and event without duplicates");
  const collision = await left.send(
    project({ ...createCommand, title: "Different request" }, "collision"),
  );
  NodeAssert.equal(collision.error.kind, "command_conflict");
  checked("command ID reuse with different intent is rejected");

  const model = {
    instanceId: "codex",
    model: "gpt-5.4",
    options: [
      { id: "effort", value: "high" },
      { id: "flag", value: true },
    ],
  };
  const script = {
    id: "build",
    name: "Build",
    command: "printf build",
    icon: "configure",
    runOnWorktreeCreate: false,
    runOnSettle: true,
  };
  const updated = successful(
    await right.send(
      project(
        {
          type: "project.meta.update",
          commandId: "update-one",
          projectId: "one",
          title: "Renamed",
          pinned: true,
          archived: true,
          defaultModelSelection: model,
          defaultThreadEnvMode: "worktree",
          scripts: [script],
          projectIcon: { kind: "monogram", text: "a\u0301", color: "blue" },
        },
        "updated-one",
        1794139201123,
      ),
    ),
  );
  const row = updated.receipt.result.ok.project;
  NodeAssert.deepEqual(row.defaultModelSelection, model);
  NodeAssert.deepEqual(row.scripts, [script]);
  NodeAssert.equal(row.projectIcon.text, "a\u0301");
  NodeAssert.equal(row.pinnedAt, "2026-11-08T12:00:01.123Z");
  NodeAssert.equal(row.archivedAt, row.pinnedAt);
  checked(
    "native validation, Unicode graphemes, nested model/scripts/icon data and timestamp projection",
  );

  const nulled = successful(
    await left.send(
      project(
        {
          type: "project.meta.update",
          commandId: "null-one",
          projectId: "one",
          defaultModelSelection: null,
          pinned: true,
          archived: true,
        },
        "nulled-one",
        1794139202123,
      ),
    ),
  );
  const nulledRow = nulled.receipt.result.ok.project;
  NodeAssert.equal(nulledRow.defaultModelSelection, null);
  NodeAssert.equal(nulledRow.defaultThreadEnvMode, "worktree");
  NodeAssert.deepEqual(nulledRow.projectIcon, row.projectIcon);
  NodeAssert.deepEqual(nulledRow.scripts, row.scripts);
  NodeAssert.equal(nulledRow.pinnedAt, row.pinnedAt);
  NodeAssert.equal(nulledRow.archivedAt, row.archivedAt);
  checked(
    "explicit null clears one field while omitted fields and original pin/archive timestamps survive",
  );

  const beforeRejected = await snapshot(left);
  const rejected = successful(
    await right.send(
      project(create("workspace-conflict", "two", "/fixture/one"), "unused-conflict"),
    ),
  );
  NodeAssert.ok(rejected.receipt.result.error);
  const afterRejected = await snapshot(left);
  NodeAssert.equal(afterRejected.revision, beforeRejected.revision + 1);
  NodeAssert.equal(afterRejected.last_sequence, beforeRejected.last_sequence);
  NodeAssert.deepEqual(afterRejected.projections, beforeRejected.projections);
  const replayedRejection = successful(
    await left.send(
      project(create("workspace-conflict", "two", "/fixture/one"), "unused-conflict-again"),
    ),
  );
  NodeAssert.deepEqual(replayedRejection.receipt, rejected.receipt);
  checked(
    "domain rejection has a durable replayable receipt without an event or projection mutation",
  );

  const beforeInvalid = await snapshot(left);
  for (const command of [
    { type: "project.delete", commandId: "unported-delete", projectId: "one", force: true },
    {
      type: "project.meta.update",
      commandId: "bad-icon",
      projectId: "one",
      projectIcon: { kind: "monogram", text: "🙂", color: "blue" },
    },
    { ...create("extra-field", "extra", "/fixture/extra"), unexpected: true },
  ])
    NodeAssert.equal(
      (await left.send(project(command, command.commandId + "-event"))).error.kind,
      "invalid_request",
    );
  for (const frame of [
    '{"operation":"snapshot","operation":"snapshot"}\n',
    '{"operation":"snapshot","extra":true}\n',
    '{"operation":"snapshot"}\r\n',
  ]) {
    const malformed = await left.raw(frame);
    NodeAssert.equal(
      malformed.error?.kind,
      "invalid_request",
      JSON.stringify({ frame, malformed }),
    );
  }
  NodeAssert.deepEqual(await snapshot(left), beforeInvalid);
  checked(
    "unported deletion, malformed contracts, duplicate keys, and invalid framing fail without persistence",
  );

  const fakePlanner = NodePath.join(root, "fake-planner");
  await NodeFSP.writeFile(
    fakePlanner,
    '#!/bin/sh\nprintf \'{"version":1,"ok":{"event":{},"project":{}}}\\n\'\n',
    { mode: 0o700 },
  );
  const forgedClient = client("connect", fakePlanner);
  NodeAssert.equal(
    (
      await forgedClient.send(
        project(create("forged", "forged", "/fixture/forged"), "forged-event"),
      )
    ).error.kind,
    "invalid_request",
  );
  await forgedClient.close();
  NodeAssert.deepEqual(await snapshot(left), beforeInvalid);
  checked("a forged selected planner response cannot write the database");

  const competingCommands = [
    create("claim-left", "claim-left", "/fixture/shared"),
    create("claim-right", "claim-right", "/fixture/shared"),
  ];
  const competing = await Promise.all([
    left.send(project(competingCommands[0], "claim-event-left")),
    right.send(project(competingCommands[1], "claim-event-right")),
  ]);
  const isAccepted = (response) => response.ok && response.result.receipt.result.ok !== undefined;
  NodeAssert.equal(competing.filter(isAccepted).length, 1);
  const rejectedIndex = competing.findIndex((response) => !isAccepted(response));
  if (!competing[rejectedIndex].ok) {
    NodeAssert.ok(
      ["stale_revision", "transaction_conflict"].includes(competing[rejectedIndex].error.kind),
    );
  }
  const ownershipRejection = successful(
    await left.send(project(competingCommands[rejectedIndex], "claim-retry-unused")),
  );
  NodeAssert.ok(ownershipRejection.receipt.result.error);
  const sharedOwners = (await snapshot(left)).projections.filter(
    (projection) => projection.payload.workspaceRoot === "/fixture/shared",
  );
  NodeAssert.equal(sharedOwners.length, 1);
  checked("concurrent native project commands and conflict retry preserve one workspace owner");

  const revision = (await snapshot(left)).revision;
  const makeBatch = (name) => ({
    command_id: name,
    request_digest: hash(name),
    expected_revision: revision,
    events: [{ id: "event-" + name, payload: { writer: name, optional: null } }],
    projections: [{ operation: "put", key: "concurrent/" + name, payload: { writer: name } }],
    effects: [{ id: "effect-" + name, payload: { type: "cleanup", writer: name } }],
    result: { writer: name },
  });
  const batches = [makeBatch("writer-left"), makeBatch("writer-right")];
  const outcomes = await Promise.all([
    left.send({ operation: "commit", batch: batches[0] }),
    right.send({ operation: "commit", batch: batches[1] }),
  ]);
  NodeAssert.equal(outcomes.filter((result) => result.ok).length, 1);
  const loser = outcomes.findIndex((result) => !result.ok);
  NodeAssert.ok(["stale_revision", "transaction_conflict"].includes(outcomes[loser].error.kind));
  NodeAssert.equal((await snapshot(left)).revision, revision + 1);
  NodeAssert.equal(
    successful(await left.send({ operation: "pending_effects", limit: 100 })).length,
    1,
  );
  checked("independent application processes cannot both commit from the same revision");
  batches[loser].expected_revision = revision + 1;
  successful(await right.send({ operation: "commit", batch: batches[loser] }));
  NodeAssert.equal(
    successful(await left.send({ operation: "pending_effects", limit: 100 })).length,
    2,
  );
  const replayedBatch = successful(await left.send({ operation: "commit", batch: batches[loser] }));
  NodeAssert.equal(replayedBatch.replayed, true);
  checked("explicit replan succeeds and retry cannot duplicate outbox work");

  const beforeRollback = await snapshot(left);
  const broken = makeBatch("rollback");
  broken.expected_revision = beforeRollback.revision;
  broken.effects[0].id = "effect-writer-left";
  NodeAssert.equal(
    (await left.send({ operation: "commit", batch: broken })).error.kind,
    "duplicate_record",
  );
  NodeAssert.deepEqual(await snapshot(right), beforeRollback);
  NodeAssert.equal(
    successful(await left.send({ operation: "receipt", command_id: "rollback" })),
    null,
  );
  checked("a late outbox collision rolls back the event, projection, receipt, and counters");

  const sourcePath = NodePath.join(root, "source.sqlite");
  const exportPath = NodePath.join(root, "source.ndjson");
  const source = new NodeSqlite.DatabaseSync(sourcePath);
  source.exec(
    "PRAGMA user_version=7; CREATE TABLE records(id INTEGER PRIMARY KEY AUTOINCREMENT, counter INTEGER, body BLOB, note TEXT);",
  );
  source
    .prepare("INSERT INTO records(counter,body,note) VALUES (?,?,?)")
    .run(9223372036854775807n, Buffer.from([0, 255, 128]), "Unicode 🦀\u0000 preserved");
  source.close();
  const sourceHash = hash(await NodeFSP.readFile(sourcePath));
  const exporter = NodeURL.fileURLToPath(new URL("./export-sqlite-state.mjs", import.meta.url));
  const exportResult = NodeChildProcess.spawnSync(
    process.execPath,
    ["--disable-warning=ExperimentalWarning", exporter, sourcePath, exportPath],
    { encoding: "utf8", timeout: 30_000 },
  );
  NodeAssert.equal(exportResult.status, 0, exportResult.stderr);
  const archived = successful(await left.send({ operation: "archive_sqlite", path: exportPath }));
  NodeAssert.equal(archived.replayed, false);
  NodeAssert.equal(archived.summary.table_count, 1);
  NodeAssert.equal(archived.summary.row_count, 1);
  NodeAssert.equal(archived.summary.file_sha256, hash(await NodeFSP.readFile(exportPath)));
  const archiveReplay = successful(
    await right.send({ operation: "archive_sqlite", path: exportPath }),
  );
  NodeAssert.equal(archiveReplay.replayed, true);
  NodeAssert.equal(archiveReplay.archive_id, archived.archive_id);
  NodeAssert.equal(hash(await NodeFSP.readFile(sourcePath)), sourceHash);
  NodeAssert.deepEqual(await snapshot(right), beforeRollback);
  checked(
    "read-only SQLite export is verified and archived idempotently without changing live projections",
  );

  const recoveredPath = NodePath.join(root, "recovered.ndjson");
  successful(
    await right.send({
      operation: "restore_sqlite_export",
      archive_id: archived.archive_id,
      destination: recoveredPath,
    }),
  );
  NodeAssert.deepEqual(await NodeFSP.readFile(recoveredPath), await NodeFSP.readFile(exportPath));
  NodeAssert.equal(
    (
      await right.send({
        operation: "restore_sqlite_export",
        archive_id: archived.archive_id,
        destination: recoveredPath,
      })
    ).ok,
    false,
  );
  checked("archive recovery reconstructs identical verified bytes and refuses to overwrite a file");
  const malformedPath = NodePath.join(root, "tampered.ndjson");
  const original = await NodeFSP.readFile(exportPath, "utf8");
  await NodeFSP.writeFile(malformedPath, original.replace("Unicode", "Damaged"));
  NodeAssert.equal(
    (await right.send({ operation: "archive_sqlite", path: malformedPath })).ok,
    false,
  );
  NodeAssert.deepEqual(await snapshot(left), beforeRollback);
  checked("checksum failure prevents publishing a complete archive");

  const outboxNow = "2026-11-08T12:00:00.000Z";
  const workerCommit = {
    operation: "commit_outbox",
    batch: {
      command_id: "worker-fixture",
      request_digest: hash("worker-fixture"),
      expected_revision: (await snapshot(left)).revision,
      events: [{ id: "worker-event", payload: { type: "worker-fixture" } }],
      projections: [{ operation: "put", key: "worker/fixture", payload: { durable: true } }],
      effects: [],
      result: { accepted: true },
    },
    outbox: {
      enqueue_at: outboxNow,
      enqueue: [
        {
          id: "worker-cleanup",
          command_id: "worker-fixture",
          thread_id: "worker-thread",
          request: { type: "terminal.cleanup" },
        },
      ],
      cancel: [],
    },
  };
  const workerCommitted = successful(await left.send(workerCommit));
  NodeAssert.equal(workerCommitted.enqueued, 1);
  const claims = await Promise.all(
    [left, right].map((connection, index) =>
      connection.send({
        operation: "outbox_claim",
        worker_id: `worker-${index}`,
        now: outboxNow,
        exclude_restart_continuations: false,
      }),
    ),
  );
  const wonClaims = claims.filter((response) => response.ok && response.result !== null);
  NodeAssert.equal(wonClaims.length, 1);
  for (const response of claims) {
    if (!response.ok) NodeAssert.equal(response.error.kind, "transaction_conflict");
  }
  const claimedCleanup = wonClaims[0].result;
  NodeAssert.equal(claimedCleanup.id, "worker-cleanup");
  NodeAssert.equal(claimedCleanup.attempt_count, 1);
  checked(
    "event, projection, receipt and worker enqueue commit together; independent processes cannot both claim an effect",
  );
  successful(
    await right.send({
      operation: "outbox_enqueue",
      now: outboxNow,
      effects: [
        {
          id: "process-bound",
          command_id: "worker-process",
          thread_id: "other-worker-thread",
          request: { type: "provider-turn.start", runId: "fixture-run" },
        },
      ],
    }),
  );
  const claimedProvider = successful(
    await right.send({
      operation: "outbox_claim",
      worker_id: "provider-worker",
      now: outboxNow,
      exclude_restart_continuations: false,
    }),
  );
  NodeAssert.equal(claimedProvider.id, "process-bound");
  const workerReceipt = successful(
    await right.send({ operation: "outbox_receipt", command_id: "worker-fixture" }),
  );
  NodeAssert.deepEqual(workerReceipt.commit.receipt, workerCommitted.commit.receipt);

  const durableState = await snapshot(left);
  const durableEvents = await events(left);
  const durableEffects = successful(await left.send({ operation: "pending_effects", limit: 100 }));
  await Promise.all([left.kill(), right.kill()]);
  left = client();
  right = client();
  NodeAssert.deepEqual(await snapshot(left), durableState);
  NodeAssert.deepEqual(await events(right), durableEvents);
  NodeAssert.deepEqual(
    successful(await right.send({ operation: "pending_effects", limit: 100 })),
    durableEffects,
  );
  NodeAssert.deepEqual(
    successful(await left.send({ operation: "outbox_get", effect_id: "worker-cleanup" })),
    claimedCleanup,
  );
  NodeAssert.deepEqual(
    successful(await right.send({ operation: "outbox_get", effect_id: "process-bound" })),
    claimedProvider,
  );
  NodeAssert.deepEqual(
    successful(await left.send({ operation: "outbox_receipt", command_id: "worker-fixture" })),
    workerReceipt,
  );
  NodeAssert.deepEqual(
    successful(
      await right.send({ operation: "outbox_reconcile", now: "2026-11-08T12:00:01.000Z" }),
    ),
    { requeued: 1, cancelled: 1 },
  );
  NodeAssert.equal(
    successful(
      await left.send({
        operation: "outbox_succeed",
        effect_id: "worker-cleanup",
        worker_id: claimedCleanup.lease_owner,
        now: "2026-11-08T12:00:01.000Z",
      }),
    ),
    false,
  );
  const recoveredCleanup = successful(
    await left.send({
      operation: "outbox_claim",
      worker_id: "recovery-worker",
      now: "2026-11-08T12:00:01.000Z",
      exclude_restart_continuations: false,
    }),
  );
  NodeAssert.equal(recoveredCleanup.id, "worker-cleanup");
  NodeAssert.equal(recoveredCleanup.attempt_count, 2);
  const recoveredProvider = successful(
    await right.send({ operation: "outbox_get", effect_id: "process-bound" }),
  );
  NodeAssert.equal(recoveredProvider.status, "cancelled");
  NodeAssert.equal(recoveredProvider.lease_owner, null);
  const workerReplay = successful(await right.send(workerCommit));
  NodeAssert.equal(workerReplay.commit.replayed, true);
  NodeAssert.equal(workerReplay.enqueued, 1);
  NodeAssert.deepEqual(
    successful(await left.send({ operation: "outbox_get", effect_id: "worker-cleanup" })),
    recoveredCleanup,
  );
  checked(
    "SIGKILL recovery requeues cleanup, cancels process-bound work, rejects the old lease and preserves the original commit receipt",
  );
  NodeAssert.deepEqual(
    successful(await left.send(project(createCommand, "never-recommitted"))).receipt,
    created.receipt,
  );
  const recoveredArchive = successful(
    await right.send({ operation: "sqlite_archive", archive_id: archived.archive_id }),
  );
  NodeAssert.equal(recoveredArchive.summary.file_sha256, archived.summary.file_sha256);
  const crashRecoveredPath = NodePath.join(root, "recovered-after-crash.ndjson");
  successful(
    await right.send({
      operation: "restore_sqlite_export",
      archive_id: archived.archive_id,
      destination: crashRecoveredPath,
    }),
  );
  NodeAssert.deepEqual(
    await NodeFSP.readFile(crashRecoveredPath),
    await NodeFSP.readFile(exportPath),
  );
  checked(
    "acknowledged state, receipts, events, outbox and archive bytes survive SIGKILL of every process holding the redb file",
  );

  const corruptRow = {
    ...created.receipt.result.ok.project,
    projectId: "corrupt",
    workspaceRoot: "/fixture/corrupt",
  };
  delete corruptRow.deletedAt;
  successful(
    await left.send({
      operation: "commit",
      batch: {
        command_id: "inject-corrupt-row",
        request_digest: hash("inject-corrupt-row"),
        expected_revision: durableState.revision,
        events: [],
        effects: [],
        projections: [{ operation: "put", key: "project/corrupt", payload: corruptRow }],
        result: null,
      },
    }),
  );
  const corruptState = await snapshot(right);
  NodeAssert.equal(
    (
      await left.send(
        project(create("cannot-bypass-owner", "new-owner", "/fixture/corrupt"), "never-committed"),
      )
    ).error.kind,
    "invalid_request",
  );
  NodeAssert.deepEqual(await snapshot(left), corruptState);
  checked("malformed stored project rows cannot bypass workspace ownership checks");
  await Promise.all([left.close(), right.close()]);

  const proof = {
    status: "passed",
    engine: fixture.engineLabel,
    sourceCommit: process.env.GITHUB_SHA ?? null,
    databaseSha256: hash(await NodeFSP.readFile(database)),
    plannerSha256: hash(await NodeFSP.readFile(planner)),
    passed: checks.length,
    checks,
    boundaries: [
      "one local database file; serialized write transactions; no distributed HA",
      "Lust planner and projector; Cargo-built embedded database host",
      "standalone native store; existing application remains SQLite",
      "archive recovery restores NDJSON, not a live application database",
      "no throughput or power-failure durability claim",
    ],
  };
  if (proofPath) {
    await NodeFSP.mkdir(NodePath.dirname(proofPath), { recursive: true });
    await NodeFSP.writeFile(proofPath, JSON.stringify(proof, null, 2) + "\n");
  }
  console.log(JSON.stringify(proof));
} finally {
  await Promise.allSettled([...clients].map((connection) => connection.kill()));
  await fixture.close();
}
