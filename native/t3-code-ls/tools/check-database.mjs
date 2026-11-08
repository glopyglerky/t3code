#!/usr/bin/env node
import assert from "node:assert/strict";
import { spawn, spawnSync } from "node:child_process";
import { createHash, randomBytes } from "node:crypto";
import { once } from "node:events";
import * as fs from "node:fs/promises";
import * as net from "node:net";
import * as os from "node:os";
import * as path from "node:path";
import * as readline from "node:readline";
import { DatabaseSync } from "node:sqlite";
import { fileURLToPath } from "node:url";

const [database, planner, engine, proofPath] = process.argv.slice(2);
for (const [name, executable] of Object.entries({ database, planner, engine })) {
  assert.ok(executable && path.isAbsolute(executable), `Pass an absolute ${name} executable path`);
  await fs.access(executable, fs.constants.X_OK);
}
const version = spawnSync(engine, ["version"], { encoding: "utf8", env: {}, timeout: 10_000 });
assert.equal(version.status, 0, version.stderr);
assert.match(version.stdout, /^4\.0\.0(?:\s|\+|$)/);
const root = await fs.mkdtemp(
  path.join(process.env.T3_CODE_LS_TEST_TMPDIR ?? os.tmpdir(), "t3-database-gate-"),
);
const checks = [];
const clients = new Set();
const password = randomBytes(32).toString("hex");
const listener = net.createServer();
listener.listen(0, "127.0.0.1");
await once(listener, "listening");
const address = `127.0.0.1:${listener.address().port}`;
await new Promise((resolve, reject) =>
  listener.close((error) => (error ? reject(error) : resolve())),
);
const endpoint = `ws://${address}`;
let daemon;
let daemonClosed;
let token;
const hash = (bytes) => createHash("sha256").update(bytes).digest("hex");
const checked = (description) => checks.push(description);

async function startDaemon() {
  daemon = spawn(
    engine,
    [
      "start",
      "--bind",
      address,
      "--deny-net",
      "--deny-scripting",
      "--log",
      "info",
      "--log-format",
      "json",
      `surrealkv://${path.join(root, "state")}?sync=every`,
    ],
    {
      env: {
        SURREAL_USER: "database_gate",
        SURREAL_PASS: password,
        SURREAL_SURREALKV_BLOCK_CACHE_CAPACITY: "33554432",
        SURREAL_SURREALKV_MAX_MEMTABLE_SIZE: "16777216",
      },
      stdio: ["ignore", "pipe", "pipe"],
    },
  );
  daemonClosed = once(daemon, "close");
  daemon.stderr.resume();
  const lines = readline.createInterface({ input: daemon.stdout });
  await new Promise((resolve, reject) => {
    const timeout = setTimeout(
      () => reject(new Error("Database listener readiness timed out")),
      20_000,
    );
    let observed = 0;
    const fail = () => {
      clearTimeout(timeout);
      reject(new Error("Database exited before listener readiness"));
    };
    daemon.once("exit", fail);
    daemon.once("error", reject);
    lines.on("line", (line) => {
      observed += line.length;
      if (observed > 1_048_576) {
        clearTimeout(timeout);
        reject(new Error("Excessive database startup output"));
        return;
      }
      let log;
      try {
        log = JSON.parse(line);
      } catch {
        return;
      }
      if (log.fields?.message === `Started web server on ${address}`) {
        clearTimeout(timeout);
        daemon.removeListener("exit", fail);
        resolve();
      }
    });
  });
  const response = await fetch(`http://${address}/signin`, {
    method: "POST",
    headers: { "Content-Type": "application/json", Accept: "application/json" },
    body: JSON.stringify({ user: "database_gate", pass: password }),
    signal: AbortSignal.timeout(10_000),
  });
  assert.equal(response.status, 200);
  token = (await response.json()).token;
  assert.equal(typeof token, "string");
}

function client(mode = "loopback", selectedPlanner = planner) {
  const child = spawn(database, [mode, endpoint], {
    env: { T3_CODE_LS_DATABASE_TOKEN: token, T3_CODE_LS_PROJECT_STORE_EXECUTABLE: selectedPlanner },
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
  const closed = once(child, "close").then(([code, signal]) => {
    exited = true;
    fail(new Error(`Database client closed (${code ?? signal}): ${errors}`));
    return { code, signal, errors };
  });
  readline.createInterface({ input: child.stdout }).on("line", (line) => {
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
      assert.equal(result.code, 0, result.errors);
      assert.equal(result.errors, "");
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
  assert.equal(response.ok, true, JSON.stringify(response));
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
  await startDaemon();
  let left = client("initialize-loopback");
  assert.deepEqual(await snapshot(left), { revision: 0, last_sequence: 0, projections: [] });
  let right = client();
  checked("explicit schema initialization and empty durable store");

  const createCommand = create("create-one", "one", "/fixture/one");
  const created = successful(await left.send(project(createCommand, "created-one")));
  assert.equal(created.replayed, false);
  assert.equal(created.receipt.result.ok.project.title, "Rust project");
  assert.equal(created.receipt.result.ok.project.defaultModelSelection, null);
  assert.equal(created.receipt.first_sequence, 1);
  assert.deepEqual(
    (await snapshot(right)).projections[0].payload,
    created.receipt.result.ok.project,
  );
  checked(
    "Lust project creation atomically writes event, projection, and receipt visible to another process",
  );

  const replay = successful(
    await right.send(project(createCommand, "unused-retry-event", 1794139999999)),
  );
  assert.equal(replay.replayed, true);
  assert.deepEqual(replay.receipt, created.receipt);
  assert.equal((await events(left)).length, 1);
  checked("same command replays the original timestamp and event without duplicates");
  const collision = await left.send(
    project({ ...createCommand, title: "Different request" }, "collision"),
  );
  assert.equal(collision.error.kind, "command_conflict");
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
  assert.deepEqual(row.defaultModelSelection, model);
  assert.deepEqual(row.scripts, [script]);
  assert.equal(row.projectIcon.text, "a\u0301");
  assert.equal(row.pinnedAt, "2026-11-08T12:00:01.123Z");
  assert.equal(row.archivedAt, row.pinnedAt);
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
  assert.equal(nulledRow.defaultModelSelection, null);
  assert.equal(nulledRow.defaultThreadEnvMode, "worktree");
  assert.deepEqual(nulledRow.projectIcon, row.projectIcon);
  assert.deepEqual(nulledRow.scripts, row.scripts);
  assert.equal(nulledRow.pinnedAt, row.pinnedAt);
  assert.equal(nulledRow.archivedAt, row.archivedAt);
  checked(
    "explicit null clears one field while omitted fields and original pin/archive timestamps survive",
  );

  const beforeRejected = await snapshot(left);
  const rejected = successful(
    await right.send(
      project(create("workspace-conflict", "two", "/fixture/one"), "unused-conflict"),
    ),
  );
  assert.ok(rejected.receipt.result.error);
  const afterRejected = await snapshot(left);
  assert.equal(afterRejected.revision, beforeRejected.revision + 1);
  assert.equal(afterRejected.last_sequence, beforeRejected.last_sequence);
  assert.deepEqual(afterRejected.projections, beforeRejected.projections);
  const replayedRejection = successful(
    await left.send(
      project(create("workspace-conflict", "two", "/fixture/one"), "unused-conflict-again"),
    ),
  );
  assert.deepEqual(replayedRejection.receipt, rejected.receipt);
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
    assert.equal(
      (await left.send(project(command, command.commandId + "-event"))).error.kind,
      "invalid_request",
    );
  for (const frame of [
    '{"operation":"snapshot","operation":"snapshot"}\n',
    '{"operation":"snapshot","extra":true}\n',
    '{"operation":"snapshot"}\r\n',
  ]) {
    const malformed = await left.raw(frame);
    assert.equal(malformed.error?.kind, "invalid_request", JSON.stringify({ frame, malformed }));
  }
  assert.deepEqual(await snapshot(left), beforeInvalid);
  checked(
    "unported deletion, malformed contracts, duplicate keys, and invalid framing fail without persistence",
  );

  const fakePlanner = path.join(root, "fake-planner");
  await fs.writeFile(
    fakePlanner,
    '#!/bin/sh\nprintf \'{"version":1,"ok":{"event":{},"project":{}}}\\n\'\n',
    { mode: 0o700 },
  );
  const forgedClient = client("loopback", fakePlanner);
  assert.equal(
    (
      await forgedClient.send(
        project(create("forged", "forged", "/fixture/forged"), "forged-event"),
      )
    ).error.kind,
    "invalid_request",
  );
  await forgedClient.close();
  assert.deepEqual(await snapshot(left), beforeInvalid);
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
  assert.equal(competing.filter(isAccepted).length, 1);
  const rejectedIndex = competing.findIndex((response) => !isAccepted(response));
  if (!competing[rejectedIndex].ok) {
    assert.ok(
      ["stale_revision", "transaction_conflict"].includes(competing[rejectedIndex].error.kind),
    );
  }
  const ownershipRejection = successful(
    await left.send(project(competingCommands[rejectedIndex], "claim-retry-unused")),
  );
  assert.ok(ownershipRejection.receipt.result.error);
  const sharedOwners = (await snapshot(left)).projections.filter(
    (projection) => projection.payload.workspaceRoot === "/fixture/shared",
  );
  assert.equal(sharedOwners.length, 1);
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
  assert.equal(outcomes.filter((result) => result.ok).length, 1);
  const loser = outcomes.findIndex((result) => !result.ok);
  assert.ok(["stale_revision", "transaction_conflict"].includes(outcomes[loser].error.kind));
  assert.equal((await snapshot(left)).revision, revision + 1);
  assert.equal(successful(await left.send({ operation: "pending_effects", limit: 100 })).length, 1);
  checked("independent application processes cannot both commit from the same revision");
  batches[loser].expected_revision = revision + 1;
  successful(await right.send({ operation: "commit", batch: batches[loser] }));
  assert.equal(successful(await left.send({ operation: "pending_effects", limit: 100 })).length, 2);
  const replayedBatch = successful(await left.send({ operation: "commit", batch: batches[loser] }));
  assert.equal(replayedBatch.replayed, true);
  checked("explicit replan succeeds and retry cannot duplicate outbox work");

  const beforeRollback = await snapshot(left);
  const broken = makeBatch("rollback");
  broken.expected_revision = beforeRollback.revision;
  broken.effects[0].id = "effect-writer-left";
  assert.equal(
    (await left.send({ operation: "commit", batch: broken })).error.kind,
    "duplicate_record",
  );
  assert.deepEqual(await snapshot(right), beforeRollback);
  assert.equal(successful(await left.send({ operation: "receipt", command_id: "rollback" })), null);
  checked("a late outbox collision rolls back the event, projection, receipt, and counters");

  const sourcePath = path.join(root, "source.sqlite");
  const exportPath = path.join(root, "source.ndjson");
  const source = new DatabaseSync(sourcePath);
  source.exec(
    "PRAGMA user_version=7; CREATE TABLE records(id INTEGER PRIMARY KEY AUTOINCREMENT, counter INTEGER, body BLOB, note TEXT);",
  );
  source
    .prepare("INSERT INTO records(counter,body,note) VALUES (?,?,?)")
    .run(9223372036854775807n, Buffer.from([0, 255, 128]), "Unicode 🦀\u0000 preserved");
  source.close();
  const sourceHash = hash(await fs.readFile(sourcePath));
  const exporter = fileURLToPath(new URL("./export-sqlite-state.mjs", import.meta.url));
  const exportResult = spawnSync(
    process.execPath,
    ["--disable-warning=ExperimentalWarning", exporter, sourcePath, exportPath],
    { encoding: "utf8", timeout: 30_000 },
  );
  assert.equal(exportResult.status, 0, exportResult.stderr);
  const archived = successful(await left.send({ operation: "archive_sqlite", path: exportPath }));
  assert.equal(archived.replayed, false);
  assert.equal(archived.summary.table_count, 1);
  assert.equal(archived.summary.row_count, 1);
  assert.equal(archived.summary.file_sha256, hash(await fs.readFile(exportPath)));
  const archiveReplay = successful(
    await right.send({ operation: "archive_sqlite", path: exportPath }),
  );
  assert.equal(archiveReplay.replayed, true);
  assert.equal(archiveReplay.archive_id, archived.archive_id);
  assert.equal(hash(await fs.readFile(sourcePath)), sourceHash);
  assert.deepEqual(await snapshot(right), beforeRollback);
  checked(
    "read-only SQLite export is verified and archived idempotently without changing live projections",
  );

  const recoveredPath = path.join(root, "recovered.ndjson");
  successful(
    await right.send({
      operation: "restore_sqlite_export",
      archive_id: archived.archive_id,
      destination: recoveredPath,
    }),
  );
  assert.deepEqual(await fs.readFile(recoveredPath), await fs.readFile(exportPath));
  assert.equal(
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
  const malformedPath = path.join(root, "tampered.ndjson");
  const original = await fs.readFile(exportPath, "utf8");
  await fs.writeFile(malformedPath, original.replace("Unicode", "Damaged"));
  assert.equal((await right.send({ operation: "archive_sqlite", path: malformedPath })).ok, false);
  assert.deepEqual(await snapshot(left), beforeRollback);
  checked("checksum failure prevents publishing a complete archive");

  const durableState = await snapshot(left);
  const durableEvents = await events(left);
  await Promise.all([left.kill(), right.kill()]);
  daemon.kill("SIGKILL");
  await daemonClosed;
  await startDaemon();
  left = client();
  right = client();
  assert.deepEqual(await snapshot(left), durableState);
  assert.deepEqual(await events(right), durableEvents);
  assert.deepEqual(
    successful(await left.send(project(createCommand, "never-recommitted"))).receipt,
    created.receipt,
  );
  const recoveredArchive = successful(
    await right.send({ operation: "sqlite_archive", archive_id: archived.archive_id }),
  );
  assert.equal(recoveredArchive.summary.file_sha256, archived.summary.file_sha256);
  checked(
    "acknowledged state, receipts, events and archives survive SIGKILL of both clients and database",
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
  assert.equal(
    (
      await left.send(
        project(create("cannot-bypass-owner", "new-owner", "/fixture/corrupt"), "never-committed"),
      )
    ).error.kind,
    "invalid_request",
  );
  assert.deepEqual(await snapshot(left), corruptState);
  checked("malformed stored project rows cannot bypass workspace ownership checks");
  await Promise.all([left.close(), right.close()]);

  const proof = {
    status: "passed",
    engine: "SurrealDB 4.0.0 / SurrealKV sync=every",
    sourceCommit: process.env.GITHUB_SHA ?? null,
    databaseSha256: hash(await fs.readFile(database)),
    plannerSha256: hash(await fs.readFile(planner)),
    passed: checks.length,
    checks,
    boundaries: [
      "single database node; no HA qualification",
      "Lust planner and projector; Cargo-built database service/client",
      "standalone native store; existing application remains SQLite",
      "archive recovery restores NDJSON, not a live application database",
      "no throughput or power-failure durability claim",
    ],
  };
  if (proofPath) {
    await fs.mkdir(path.dirname(proofPath), { recursive: true });
    await fs.writeFile(proofPath, JSON.stringify(proof, null, 2) + "\n");
  }
  console.log(JSON.stringify(proof));
} finally {
  await Promise.allSettled([...clients].map((connection) => connection.kill()));
  if (daemon) {
    daemon.kill("SIGKILL");
    await daemonClosed.catch(() => {});
  }
  await fs.rm(root, { recursive: true, force: true });
}
