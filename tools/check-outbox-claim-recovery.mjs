#!/usr/bin/env node
import * as NodeAssert from "node:assert/strict";
import * as NodeChildProcess from "node:child_process";
import * as NodeCrypto from "node:crypto";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeReadline from "node:readline";

import { createFixture } from "./database-fixture.mjs";

const [database, proofPath] = process.argv.slice(2);
NodeAssert.ok(
  database && NodePath.isAbsolute(database),
  "Pass an absolute database executable path",
);
await NodeFSP.access(database, NodeFSP.constants.X_OK);
const now = "2026-11-08T12:00:00.123Z";
const later = "2026-11-08T12:01:00.123Z";
const future = "2026-11-08T12:02:00.123Z";
const checks = [];
const metrics = { discardedResponses: 0, conflictRetries: 0, processesStarted: 0 };

function client(fixture, clients, initialize = false) {
  const child = NodeChildProcess.spawn(
    database,
    fixture.cliArgs(initialize ? "initialize" : "connect"),
    {
      env: {},
      stdio: ["pipe", "pipe", "pipe"],
    },
  );
  metrics.processesStarted += 1;
  const pending = [];
  let errors = "";
  let exited = false;
  let fatal;
  child.stderr.on("data", (data) => {
    errors = (errors + data).slice(-8192);
  });
  const fail = (error) => {
    fatal ??= error;
    for (const request of pending.splice(0)) request.reject(fatal);
  };
  child.on("error", fail);
  const closed = new Promise((resolve) => {
    child.once("close", (code, signal) => {
      exited = true;
      if (pending.length > 0)
        fail(new Error(`Database client closed (${code ?? signal}): ${errors}`));
      resolve({ code, signal });
    });
  });
  NodeReadline.createInterface({ input: child.stdout }).on("line", (line) => {
    const request = pending.shift();
    if (!request) {
      fail(new Error("Unsolicited database response"));
      return;
    }
    if (request.discard) {
      metrics.discardedResponses += 1;
      request.resolve(undefined);
      return;
    }
    try {
      request.resolve(JSON.parse(line));
    } catch (error) {
      request.reject(error);
      fail(error);
    }
  });
  function write(value, discard) {
    if (fatal) return Promise.reject(fatal);
    if (exited) return Promise.reject(new Error("Database client has exited"));
    return new Promise((resolve, reject) => {
      const deadline = setTimeout(() => {
        child.kill("SIGKILL");
        fail(new Error("Database request deadline exceeded"));
      }, 45_000);
      pending.push({
        discard,
        resolve: (result) => {
          clearTimeout(deadline);
          resolve(result);
        },
        reject: (error) => {
          clearTimeout(deadline);
          reject(error);
        },
      });
      child.stdin.write(`${JSON.stringify(value)}\n`, (error) => {
        if (error) fail(error);
      });
    });
  }
  const api = {
    send: (value) => write(value, false),
    // The response is discarded before decoding; another process proves what committed.
    discard: (value) => write(value, true),
    async close(abrupt = false) {
      if (!exited) {
        if (abrupt || fatal) child.kill("SIGKILL");
        else child.stdin.end();
      }
      const status = await closed;
      clients.delete(api);
      if (!abrupt) {
        if (fatal) throw fatal;
        NodeAssert.equal(status.code, 0, errors);
        NodeAssert.equal(errors, "");
      }
    },
  };
  clients.add(api);
  return api;
}

function successful(response) {
  NodeAssert.equal(response.ok, true, JSON.stringify(response));
  return response.result;
}
const send = async (connection, request) => successful(await connection.send(request));
const get = (connection, id) => send(connection, { operation: "outbox_get", effect_id: id });
const effect = (id, request = { type: "terminal.cleanup" }) => ({
  id,
  command_id: `command:${id}`,
  thread_id: `thread:${id}`,
  request,
});
const enqueue = (connection, effects, at = now) =>
  send(connection, { operation: "outbox_enqueue", effects, now: at });
const claim = (stream, sequence = 1, at = now) => ({
  operation: "outbox_claim_once",
  stream_id: stream,
  sequence,
  worker_id: "fixture-worker",
  now: at,
  exclude_restart_continuations: false,
});
const settle = (operation, id, at = later) => ({
  operation,
  effect_id: id,
  worker_id: "fixture-worker",
  now: at,
});
function outcome(value, state, replayed, id) {
  NodeAssert.equal(value.state, state);
  NodeAssert.equal(value.replayed, replayed);
  if (id === undefined) NodeAssert.equal(value.effect, null);
  else {
    NodeAssert.equal(value.effect.id, id);
    NodeAssert.equal(value.effect.status, "running");
    NodeAssert.equal(value.effect.lease_owner, "fixture-worker");
  }
}
async function invalid(connection, request) {
  const response = await connection.send(request);
  NodeAssert.equal(response.ok, false, JSON.stringify(response));
  NodeAssert.equal(response.error.kind, "invalid_request", JSON.stringify(response));
}
async function claimAfterConflict(connection, request) {
  for (let attempt = 0; attempt < 8; attempt += 1) {
    const response = await connection.send(request);
    if (!response.ok && response.error.kind === "transaction_conflict") {
      metrics.conflictRetries += 1;
      continue;
    }
    return successful(response);
  }
  NodeAssert.fail("Eight explicitly rejected transactions prevented the claim from completing");
}
async function scenario(description, run) {
  const fixture = await createFixture();
  const clients = new Set();
  const connect = () => client(fixture, clients);
  const control = client(fixture, clients, true);
  try {
    NodeAssert.deepEqual(await send(control, { operation: "snapshot" }), {
      revision: 0,
      last_sequence: 0,
      projections: [],
    });
    await run({ control, connect });
    await Promise.all([...clients].map((connection) => connection.close()));
    checks.push(description);
  } finally {
    await Promise.allSettled([...clients].map((connection) => connection.close(true)));
    await fixture.close();
  }
}

await scenario(
  "lost claim response survives SIGKILL of every holder and replays one committed attempt",
  async ({ control, connect }) => {
    NodeAssert.equal(await enqueue(control, [effect("lost"), effect("untouched")]), 2);
    const worker = connect();
    const request = claim("lost-stream");
    await worker.discard(request);
    const observed = await get(control, "lost");
    NodeAssert.equal(observed.status, "running");
    NodeAssert.equal(observed.attempt_count, 1);
    NodeAssert.equal(observed.lease_owner, request.worker_id);
    NodeAssert.equal((await get(control, "untouched")).status, "pending");
    await worker.close(true);
    await control.close(true);
    const restarted = connect();
    const recovered = await send(restarted, request);
    outcome(recovered, "claimed", true, "lost");
    NodeAssert.deepEqual(recovered.effect, observed);
    NodeAssert.deepEqual(await get(restarted, "lost"), observed);
    NodeAssert.equal((await get(restarted, "untouched")).attempt_count, 0);
  },
);

await scenario(
  "four independent processes retrying one stream commit only one claim",
  async ({ control, connect }) => {
    await enqueue(control, [effect("race"), effect("untouched")]);
    const request = claim("race-stream");
    const results = await Promise.all(
      Array.from({ length: 4 }, () => claimAfterConflict(connect(), request)),
    );
    NodeAssert.equal(results.filter((result) => !result.replayed).length, 1);
    for (const result of results) {
      outcome(result, "claimed", result.replayed, "race");
      NodeAssert.deepEqual(result.effect, results[0].effect);
    }
    NodeAssert.equal((await get(control, "race")).attempt_count, 1);
    NodeAssert.equal((await get(control, "untouched")).status, "pending");
  },
);

await scenario(
  "lost empty response remains empty after enqueue until the stream advances",
  async ({ control, connect }) => {
    const worker = connect();
    const request = claim("empty-stream");
    await worker.discard(request);
    await worker.close(true);
    await enqueue(control, [effect("arrived-later")]);
    outcome(await send(control, request), "empty", true);
    NodeAssert.equal((await get(control, "arrived-later")).status, "pending");
    const next = await send(control, claim("empty-stream", 2, later));
    outcome(next, "claimed", false, "arrived-later");
    NodeAssert.equal(next.effect.attempt_count, 1);
  },
);

for (const transition of ["cancel", "succeed", "fail"]) {
  await scenario(
    `${transition} retires the old claim without selecting another effect`,
    async ({ control, connect }) => {
      const id = `retire-${transition}`;
      await enqueue(control, [effect(id), effect("untouched")]);
      const request = claim(`${transition}-stream`);
      outcome(await send(control, request), "claimed", false, id);
      const other = connect();
      if (transition === "cancel") {
        NodeAssert.deepEqual(
          await send(other, {
            operation: "outbox_cancel",
            thread_id: `thread:${id}`,
            effect_types: ["terminal.cleanup"],
            reason: "fixture cancellation",
            now: later,
          }),
          [id],
        );
      } else {
        NodeAssert.equal(
          await send(other, {
            ...settle(`outbox_${transition}`, id),
            ...(transition === "fail" ? { error: "fixture failure" } : {}),
          }),
          true,
        );
      }
      const before = await get(other, id);
      outcome(await send(control, request), "retired", true);
      NodeAssert.deepEqual(await get(other, id), before);
      NodeAssert.equal((await get(other, "untouched")).attempt_count, 0);
    },
  );
}

await scenario(
  "requeued work cannot revive an old claim even when the same owner claims it again",
  async ({ control, connect }) => {
    await enqueue(control, [effect("retry")]);
    const request = claim("retry-stream");
    outcome(await send(control, request), "claimed", false, "retry");
    const other = connect();
    NodeAssert.equal(
      await send(other, {
        ...settle("outbox_retry", "retry"),
        available_at: future,
        error: "fixture retry",
      }),
      true,
    );
    outcome(await send(control, request), "retired", true);
    const next = await send(other, claim("retry-new-stream", 1, future));
    outcome(next, "claimed", false, "retry");
    NodeAssert.equal(next.effect.attempt_count, 2);
    outcome(await send(control, request), "retired", true);
    NodeAssert.deepEqual(await get(control, "retry"), next.effect);
  },
);

await scenario(
  "process-loss reconciliation retires safe and process-bound claims with their upstream recovery distinction",
  async ({ control, connect }) => {
    await enqueue(control, [
      effect("a-safe"),
      effect("b-bound", { type: "provider-turn.start", runId: "fixture-run" }),
    ]);
    const safe = claim("reconcile-safe-stream");
    const bound = claim("reconcile-bound-stream");
    const worker = connect();
    outcome(await send(worker, safe), "claimed", false, "a-safe");
    outcome(await send(worker, bound), "claimed", false, "b-bound");
    await worker.close(true);
    NodeAssert.deepEqual(await send(control, { operation: "outbox_reconcile", now: later }), {
      requeued: 1,
      cancelled: 1,
    });
    outcome(await send(control, safe), "retired", true);
    outcome(await send(control, bound), "retired", true);
    NodeAssert.equal((await get(control, "a-safe")).status, "pending");
    NodeAssert.equal((await get(control, "b-bound")).status, "cancelled");
    const recovered = await send(control, claim("reconciled-new-stream", 1, later));
    outcome(recovered, "claimed", false, "a-safe");
    NodeAssert.equal(recovered.effect.attempt_count, 2);
    outcome(await send(control, safe), "retired", true);
  },
);

await scenario(
  "pruning and reinserting an ID cannot make an old claim executable again",
  async ({ control, connect }) => {
    await enqueue(control, [effect("reinserted")]);
    const request = claim("reinserted-old-stream");
    const original = await send(control, request);
    outcome(original, "claimed", false, "reinserted");
    const other = connect();
    NodeAssert.equal(await send(other, settle("outbox_succeed", "reinserted")), true);
    NodeAssert.equal(
      await send(other, { operation: "outbox_prune", before: future, limit: 500 }),
      1,
    );
    NodeAssert.equal(await get(control, "reinserted"), null);
    outcome(await send(control, request), "retired", true);
    NodeAssert.equal(await enqueue(other, [effect("reinserted")]), 1);
    const replacement = await send(other, claim("reinserted-new-stream"));
    outcome(replacement, "claimed", false, "reinserted");
    NodeAssert.equal(replacement.effect.attempt_count, original.effect.attempt_count);
    NodeAssert.equal(replacement.effect.lease_owner, original.effect.lease_owner);
    NodeAssert.ok(replacement.effect.insertion_ordinal > original.effect.insertion_ordinal);
    outcome(await send(control, request), "retired", true);
    NodeAssert.deepEqual(await get(control, "reinserted"), replacement.effect);
  },
);

await scenario(
  "stream owner, sequence and exact claim inputs reject invalid reuse without mutations",
  async ({ control, connect }) => {
    await enqueue(control, [effect("a-first"), effect("b-second")]);
    const request = claim("ordered-stream");
    for (const sequence of [0, 2, -1, 1.5, 9_007_199_254_740_992]) {
      await invalid(control, { ...request, sequence });
    }
    outcome(await send(control, request), "claimed", false, "a-first");
    const other = connect();
    for (const replacement of [
      { now: later },
      { exclude_restart_continuations: true },
      { worker_id: "different-owner" },
      { sequence: 3 },
      { sequence: 2, worker_id: "different-owner" },
    ]) {
      await invalid(other, { ...request, ...replacement });
    }
    NodeAssert.equal((await get(other, "a-first")).attempt_count, 1);
    NodeAssert.equal((await get(other, "b-second")).status, "pending");
    NodeAssert.equal(await send(other, settle("outbox_succeed", "a-first")), true);
    outcome(await send(control, claim("ordered-stream", 2, later)), "claimed", false, "b-second");
    await invalid(other, request);
    await invalid(other, claim("ordered-stream", 4, future));
    NodeAssert.equal((await get(control, "b-second")).attempt_count, 1);
  },
);

await scenario(
  "four concurrent streams may share one worker ID and each claim distinct work once",
  async ({ control, connect }) => {
    const ids = Array.from({ length: 4 }, (_, index) => `parallel-${index}`);
    await enqueue(
      control,
      ids.map((id) => effect(id)),
    );
    const requests = ids.map((id) => claim(`stream:${id}`));
    const peers = ids.map(() => connect());
    const results = await Promise.all(
      peers.map((peer, index) => claimAfterConflict(peer, requests[index])),
    );
    NodeAssert.deepEqual(results.map((result) => result.effect.id).sort(), ids);
    for (const [index, result] of results.entries()) {
      outcome(result, "claimed", false, result.effect.id);
      NodeAssert.equal(result.effect.attempt_count, 1);
      const repeated = await send(control, requests[index]);
      outcome(repeated, "claimed", true, result.effect.id);
      NodeAssert.deepEqual(repeated.effect, result.effect);
    }
    for (const id of ids) NodeAssert.equal((await get(control, id)).attempt_count, 1);
  },
);

const proof = {
  status: "passed",
  engine: "redb 4.5.0 / MultiWriter / Immediate",
  sourceCommit: process.env.GITHUB_SHA ?? null,
  databaseSha256: NodeCrypto.createHash("sha256")
    .update(await NodeFSP.readFile(database))
    .digest("hex"),
  passed: checks.length,
  ...metrics,
  checks,
  boundaries: [
    "claim response recovery only; no exactly-once external effect execution guarantee",
    "local independent processes sharing one file; no distributed fencing or HA",
    "whole-server reconciliation runs only after the simulated worker process stops",
    "SIGKILL recovery does not simulate power loss",
    "isolated fixtures; no production state, provider processes or paid turns",
  ],
};
if (proofPath) {
  await NodeFSP.mkdir(NodePath.dirname(proofPath), { recursive: true });
  await NodeFSP.writeFile(proofPath, JSON.stringify(proof, null, 2) + "\n");
}
console.log(JSON.stringify(proof));
