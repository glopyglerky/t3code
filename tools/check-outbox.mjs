#!/usr/bin/env node
import * as NodeAssert from "node:assert/strict";
import * as NodeChildProcess from "node:child_process";
import * as NodeCrypto from "node:crypto";
import * as NodeEvents from "node:events";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeReadline from "node:readline";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";

import {
  invokeOracle,
  oracleSource,
  runOracleScenario,
  snapshot,
  upstream,
} from "./check-outbox-runtime.mjs";

const args = process.argv.slice(2);
const policyOnly = args[1] === "--policy-only";
const oracleOnly = args[0] === "--oracle-only";
const database = policyOnly || oracleOnly ? undefined : args[0];
const policy = oracleOnly ? undefined : policyOnly ? args[0] : args[1];
const proofPath = oracleOnly ? args[1] : args[2];
for (const [name, executable] of Object.entries({ database, policy })) {
  if (executable === undefined && (name === "database" || oracleOnly)) continue;
  NodeAssert.ok(
    executable && NodePath.isAbsolute(executable),
    `Pass an absolute ${name} executable path`,
  );
  await NodeFSP.access(executable, NodeFSP.constants.X_OK);
}
const temporary = await NodeFSP.mkdtemp(
  NodePath.join(process.env.T3_CODE_LS_TEST_TMPDIR ?? NodeOS.tmpdir(), "t3-outbox-gate-"),
);
const nowMillis = Date.parse("2026-11-08T12:00:00.123Z");
const day = 86_400_000;
const iso = (offset = 0) => new Date(nowMillis + offset).toISOString();
const checks = [];
const coverage = new Map();
let comparisons = 0;
let policyComparisons = 0;
let nativeComparisons = 0;

function client(filename, initialize) {
  const child = NodeChildProcess.spawn(
    database,
    [initialize ? "initialize-local" : "local", filename],
    {
      env: {},
      stdio: ["pipe", "pipe", "pipe"],
    },
  );
  const pending = [];
  let errors = "";
  let exited = false;
  let fatal;
  child.stderr.on("data", (bytes) => {
    errors = (errors + bytes).slice(-8192);
  });
  const rejectPending = (error) => {
    for (const request of pending.splice(0)) request.reject(error);
  };
  const fail = (error) => {
    fatal ??= error;
    rejectPending(fatal);
  };
  child.on("error", fail);
  const closed = NodeEvents.once(child, "close").then(([code, signal]) => {
    exited = true;
    if (pending.length > 0)
      fail(new Error(`Outbox database process exited (${code ?? signal}): ${errors}`));
    return { code, signal };
  });
  NodeReadline.createInterface({ input: child.stdout }).on("line", (line) => {
    const request = pending.shift();
    if (!request) return fail(new Error("Unsolicited database response"));
    try {
      request.resolve(JSON.parse(line));
    } catch (error) {
      request.reject(error);
      fail(error);
    }
  });
  return {
    send(input) {
      if (fatal) return Promise.reject(fatal);
      NodeAssert.equal(exited, false);
      return new Promise((resolve, rejectRequest) => {
        const deadline = setTimeout(() => {
          child.kill();
          fail(new Error("Outbox database request deadline exceeded"));
        }, 45_000);
        pending.push({
          resolve: (response) => {
            clearTimeout(deadline);
            resolve(response);
          },
          reject: (error) => {
            clearTimeout(deadline);
            rejectRequest(error);
          },
        });
        child.stdin.write(`${JSON.stringify(input)}\n`, (error) => {
          if (error) fail(error);
        });
      });
    },
    async close(abrupt = false) {
      if (!exited) {
        if (abrupt || fatal) child.kill("SIGKILL");
        else child.stdin.end();
      }
      const status = await closed;
      if (fatal) throw fatal;
      if (!abrupt) {
        NodeAssert.equal(status.code, 0, errors);
        NodeAssert.equal(errors, "");
      }
    },
  };
}

function successful(response, label) {
  NodeAssert.equal(response.ok, true, `${label}: ${JSON.stringify(response)}`);
  return response.result;
}

function withoutOrdinal(row) {
  if (row === null) return null;
  const { insertion_ordinal: _ordinal, ...rest } = row;
  return rest;
}

function orderedRows(rows) {
  const ordered = rows.toSorted((left, right) => left.insertion_ordinal - right.insertion_ordinal);
  let previous = 0;
  const ids = new Set();
  for (const row of ordered) {
    NodeAssert.ok(Number.isSafeInteger(row.insertion_ordinal) && row.insertion_ordinal > previous);
    NodeAssert.equal(ids.has(row.id), false);
    previous = row.insertion_ordinal;
    ids.add(row.id);
  }
  // SQLite can reuse a deleted maximum rowid. Only relative insertion order is
  // observable; the native durable allocator may keep a higher monotonic value.
  return ordered.map(withoutOrdinal);
}

function policyRow(row) {
  return {
    id: row.id,
    threadId: row.thread_id,
    effectType: row.request.type,
    status: row.status,
    insertionOrdinal: row.insertion_ordinal,
    attemptCount: row.attempt_count,
    availableAt: row.available_at,
    leaseOwner: row.lease_owner,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
    completedAt: row.completed_at,
    lastError: row.last_error,
  };
}

function invokePolicy(input) {
  const execution = NodeChildProcess.spawnSync(policy, [], {
    input: `${JSON.stringify(input)}\n`,
    encoding: "utf8",
    env: {},
    maxBuffer: 32 * 1024 * 1024,
    timeout: 30_000,
  });
  NodeAssert.ifError(execution.error);
  NodeAssert.equal(execution.status, 0, execution.stderr);
  NodeAssert.equal(execution.stderr, "");
  const output = JSON.parse(execution.stdout);
  NodeAssert.equal(output.version, 1);
  NodeAssert.ok(Object.hasOwn(output, "ok"), JSON.stringify(output));
  policyComparisons++;
  return output.ok;
}

function comparePolicy(input, before, after, result, label) {
  if (!policy) return;
  const rows = before.map(policyRow);
  const base = { version: 1, rows };
  const changed = () =>
    before.flatMap((row, index) => {
      const next = after.find(({ id }) => id === row.id);
      return JSON.stringify(next) === JSON.stringify(row) ? [] : [{ index, row: policyRow(next) }];
    });
  switch (input.operation) {
    case "outbox_claim":
      NodeAssert.deepEqual(
        invokePolicy({
          ...base,
          operation: "claim",
          now: input.now,
          workerId: input.worker_id,
          excludeRestartContinuations: input.exclude_restart_continuations,
        }),
        result === null
          ? { index: null, row: null }
          : { index: before.findIndex(({ id }) => id === result.id), row: policyRow(result) },
        label,
      );
      break;
    case "outbox_next_claimable_at":
      NodeAssert.deepEqual(
        invokePolicy({ ...base, operation: "next" }),
        { availableAt: result },
        label,
      );
      break;
    case "outbox_succeed":
    case "outbox_retry":
    case "outbox_fail": {
      const action = input.operation.slice("outbox_".length);
      const expected = result ? changed()[0] : { index: null, row: null };
      NodeAssert.deepEqual(
        invokePolicy({
          ...base,
          operation: "transition",
          action,
          now: input.now,
          effectId: input.effect_id,
          workerId: input.worker_id,
          ...(action === "succeed" ? {} : { error: input.error }),
          ...(action === "retry" ? { availableAt: input.available_at } : {}),
        }),
        expected,
        label,
      );
      break;
    }
    case "outbox_cancel":
      NodeAssert.deepEqual(
        invokePolicy({
          ...base,
          operation: "cancel",
          now: input.now,
          threadId: input.thread_id,
          effectTypes: input.effect_types,
          reason: input.reason,
        }),
        { changes: changed() },
        label,
      );
      break;
    case "outbox_reconcile":
      NodeAssert.deepEqual(
        invokePolicy({ ...base, operation: "reconcile", now: input.now }),
        { changes: changed(), ...result },
        label,
      );
      break;
    case "outbox_prune": {
      let retained = rows;
      let removed = 0;
      while (true) {
        const { indices } = invokePolicy({
          version: 1,
          operation: "prune",
          rows: retained,
          cutoff: input.before,
          limit: input.limit,
        });
        NodeAssert.ok(indices.length <= input.limit);
        NodeAssert.equal(new Set(indices).size, indices.length);
        for (const index of indices)
          NodeAssert.ok(Number.isSafeInteger(index) && index >= 0 && index < retained.length);
        removed += indices.length;
        retained = retained.filter((_, index) => !indices.includes(index));
        if (indices.length < input.limit) break;
      }
      NodeAssert.equal(removed, result, label);
      NodeAssert.deepEqual(retained, after.map(policyRow), label);
      break;
    }
  }
}

const effect = (id, thread, request, availableAt, command = "command:fixture") => ({
  id,
  command_id: command,
  thread_id: thread,
  request,
  ...(availableAt === undefined ? {} : { available_at: availableAt }),
});
const enqueue = (effects, now = iso()) => ({ operation: "outbox_enqueue", effects, now });
const claim = (worker = "worker:one", now = iso(), exclude = false) => ({
  operation: "outbox_claim",
  worker_id: worker,
  now,
  exclude_restart_continuations: exclude,
});
const settle = (
  action,
  id,
  worker = "worker:one",
  now = iso(),
  error = "provider failed é\n💬",
) => ({
  operation: `outbox_${action}`,
  effect_id: id,
  worker_id: worker,
  now,
  ...(action === "succeed" ? {} : { error }),
});
const retry = (id, availableAt, worker = "worker:one", now = iso()) => ({
  ...settle("retry", id, worker, now),
  available_at: availableAt,
});
const cancel = (thread, effectTypes, now = iso(), reason = "cancelled é\n💬") => ({
  operation: "outbox_cancel",
  thread_id: thread,
  effect_types: effectTypes,
  reason,
  now,
});
const next = () => ({ operation: "outbox_next_claimable_at" });
const reconcile = (now = iso()) => ({ operation: "outbox_reconcile", now });
const prune = (before = iso(), limit = 500) => ({ operation: "outbox_prune", before, limit });
const expect = (input, expected) => ({ input, expected });
const request = (type, fields = {}) => ({ type, ...fields });
const cleanup = request("terminal.cleanup");
const title = request("thread-title.generate", { kind: { type: "regenerate" } });
const start = request("provider-turn.start", { runId: "run:start" });
const continuation = request("provider-runtime.continue", { sourceRunId: "run:source" });
const rollback = request("provider-thread.rollback", {
  providerThreadId: "provider-thread:one",
  checkpointId: "checkpoint:one",
  scopeId: "scope:one",
  restoreFiles: false,
});
const requests = [
  continuation,
  request("provider-session.detach", {
    providerSessionId: "session:one",
    detail: "detach é\n💬",
    revokeMcpCredential: true,
  }),
  start,
  request("provider-turn.interrupt", {
    providerSessionId: "session:one",
    providerThreadId: "provider-thread:one",
    providerTurnId: "turn:one",
    subagent: { id: "node:child", nativeTaskId: "native:child" },
  }),
  request("provider-turn.steer", {
    providerSessionId: "session:one",
    providerThreadId: "provider-thread:one",
    providerTurnId: "turn:one",
    messageId: "message:one",
  }),
  request("provider-turn.restart", {
    providerSessionId: "session:one",
    providerThreadId: "provider-thread:one",
    providerTurnId: "turn:one",
    interruptedAttemptId: "attempt:one",
    runId: "run:restart",
    sessionTransition: { type: "replace", replacementProviderSessionId: "session:replacement" },
  }),
  request("runtime-request.respond", {
    providerSessionId: "session:one",
    requestId: "request:one",
    feedback: "review é\n💬",
    answers: { choice: ["one", "two"], accepted: true, optional: null },
  }),
  rollback,
  request("checkpoint.capture", { runId: "run:one", scopeId: "scope:one" }),
  request("scheduled-tasks.cleanup"),
  cleanup,
  request("preview.cleanup"),
  request("attachment.cleanup", { attachmentIds: ["attachment:one", "attachment:é💬"] }),
  title,
  request("delegated-tasks.stop", { reason: "parent stopped é\n💬" }),
];
NodeAssert.equal(requests.length, 15);
NodeAssert.deepEqual(
  new Set(requests.map(({ type }) => type)),
  new Set([
    ...upstream.PROCESS_BOUND_EFFECT_TYPES,
    ...upstream.REPLAY_SAFE_EFFECT_TYPES_AFTER_PROCESS_LOSS,
  ]),
);

async function runScenario(name, steps) {
  const index = checks.length;
  const databasePath = NodePath.join(temporary, `${index}.redb`);
  let connection = database ? client(databasePath, true) : undefined;
  const commands = new Set();
  try {
    await runOracleScenario(NodePath.join(temporary, `${index}.sqlite`), (outbox) =>
      Effect.gen(function* () {
        for (let stepIndex = 0; stepIndex < steps.length; stepIndex++) {
          const step = steps[stepIndex];
          const input = step.input ?? step;
          const label = `${name} / ${stepIndex}: ${input.operation}`;
          if (input.operation === "reopen") {
            if (connection) {
              yield* Effect.promise(() => connection.close(input.abrupt));
              connection = client(databasePath, false);
            }
            continue;
          }
          const before = yield* snapshot;
          // A rejected enqueue can name a new command. Probe it before returning so a
          // partial write cannot hide outside namespaces from earlier successful requests.
          if (input.operation === "outbox_enqueue") {
            for (const row of input.effects) commands.add(row.command_id);
          }
          if (step.rejected) {
            const failure = yield* Effect.exit(invokeOracle(outbox, input));
            NodeAssert.ok(Exit.isFailure(failure), `${label}: immutable upstream must reject`);
            NodeAssert.deepEqual(
              yield* snapshot,
              before,
              `${label}: rejected request must not persist`,
            );
            if (connection) {
              const actual = yield* Effect.promise(() => connection.send(input));
              NodeAssert.equal(actual.ok, false, `${label}: redb must reject`);
              NodeAssert.equal(actual.error.kind, "invalid_request", JSON.stringify(actual));
              const stored = (yield* Effect.promise(() =>
                Promise.all(
                  [...commands].map(async (command_id) =>
                    successful(
                      await connection.send({ operation: "outbox_list", command_id }),
                      label,
                    ),
                  ),
                ),
              )).flat();
              NodeAssert.deepEqual(
                orderedRows(stored),
                orderedRows(before),
                `${label}: rejected native request must not persist`,
              );
              if (input.operation === "outbox_enqueue") {
                for (const id of new Set(input.effects.map(({ id }) => id))) {
                  const actualRow = successful(
                    yield* Effect.promise(() =>
                      connection.send({ operation: "outbox_get", effect_id: id }),
                    ),
                    label,
                  );
                  NodeAssert.deepEqual(
                    withoutOrdinal(actualRow),
                    withoutOrdinal(before.find((row) => row.id === id) ?? null),
                    `${label}: rejected effect ${id} must remain unchanged`,
                  );
                }
              }
              nativeComparisons++;
            }
            comparisons++;
            continue;
          }
          const { result, rows } = yield* invokeOracle(outbox, input);
          if (Object.hasOwn(step, "expected")) {
            NodeAssert.deepEqual(
              result?.id ?? result,
              step.expected,
              `${label}: independent expected result`,
            );
          }
          comparePolicy(input, before, rows, result, label);
          for (const row of rows) {
            const statuses = coverage.get(row.request.type) ?? new Set();
            statuses.add(row.status);
            coverage.set(row.request.type, statuses);
          }
          if (connection) {
            let actual;
            if (input.operation === "outbox_prune") {
              actual = 0;
              while (true) {
                const count = successful(
                  yield* Effect.promise(() => connection.send(input)),
                  label,
                );
                NodeAssert.ok(
                  Number.isSafeInteger(count) && count >= 0 && count <= input.limit,
                  label,
                );
                actual += count;
                if (count < input.limit) break;
              }
            } else {
              actual = successful(yield* Effect.promise(() => connection.send(input)), label);
            }
            const comparableResult = (value) => {
              if (input.operation === "outbox_claim" || input.operation === "outbox_get")
                return withoutOrdinal(value);
              if (input.operation === "outbox_list") return value.map(withoutOrdinal);
              return value;
            };
            NodeAssert.deepEqual(
              comparableResult(actual),
              comparableResult(result),
              `${label}: result`,
            );
            const stored = (yield* Effect.promise(() =>
              Promise.all(
                [...commands].map(async (command_id) =>
                  successful(
                    await connection.send({ operation: "outbox_list", command_id }),
                    label,
                  ),
                ),
              ),
            )).flat();
            NodeAssert.deepEqual(
              orderedRows(stored),
              orderedRows(rows),
              `${label}: complete persisted rows and FIFO order`,
            );
            nativeComparisons++;
          }
          comparisons++;
        }
      }),
    );
    checks.push(name);
  } finally {
    if (connection) await connection.close();
  }
}

try {
  await runScenario("empty queue and missing or unowned transitions", [
    expect({ operation: "outbox_get", effect_id: "missing" }, null),
    expect({ operation: "outbox_list", command_id: "missing" }, []),
    expect(next(), null),
    expect(claim(), null),
    expect(settle("succeed", "missing"), false),
    expect(retry("missing", iso(1000)), false),
    expect(settle("fail", "missing"), false),
    expect(cancel("missing", []), []),
    expect(reconcile(), { requeued: 0, cancelled: 0 }),
    expect(prune(), 0),
  ]);

  const all = requests.map((value, index) =>
    effect(`request:${String(index).padStart(2, "0")}:é💬`, `thread:${index}`, value, iso(1000)),
  );
  await runScenario("all effect payloads roundtrip and duplicate enqueue preserves originals", [
    expect(enqueue(all), 15),
    ...all.map(({ id }) => ({ operation: "outbox_get", effect_id: id })),
    { operation: "outbox_list", command_id: "command:fixture" },
    expect(
      enqueue(
        all.map((row) => ({
          ...row,
          command_id: "command:ignored",
          thread_id: "thread:ignored",
          request: cleanup,
          available_at: iso(8000),
        })),
        iso(5000),
      ),
      0,
    ),
    { operation: "reopen", abrupt: true },
    ...all.map(({ id }) => ({ operation: "outbox_get", effect_id: id })),
    expect(next(), iso(1000)),
  ]);

  const boundaryRequests = [
    {
      ...cleanup,
      unknown: { nested: [null, true, "discard"] },
      detail: null,
      subagent: null,
      sessionTransition: false,
      answers: "discard",
    },
    request("provider-session.detach", {
      providerSessionId: "session:one",
      unknown: { nested: true },
    }),
    request("provider-session.detach", {
      providerSessionId: "session:one",
      revokeMcpCredential: false,
    }),
    request("provider-turn.start", { runId: "\uFEFF run:trimmed\u00A0", irrelevant: null }),
    request("provider-turn.start", { runId: "\u0085run:preserved\u0085" }),
    {
      ...requests[3],
      subagent: {
        id: "\uFEFF node:trimmed\u00A0",
        nativeTaskId: "  native:preserved\uFEFF",
        extra: null,
      },
    },
    {
      ...requests[5],
      sessionTransition: {
        type: "replace",
        replacementProviderSessionId: "\uFEFF session:trimmed\u00A0",
        extra: { arbitrary: true },
      },
    },
    {
      ...requests[5],
      sessionTransition: { type: "detach", replacementProviderSessionId: null, extra: "discard" },
    },
    request("runtime-request.respond", {
      providerSessionId: "session:one",
      requestId: "request:one",
      answers: { free: { unknown: [null, true, 1, "\uFEFF preserve\u00A0"] }, empty: {} },
    }),
    ...["accept", "acceptForSession", "acceptAlways", "decline", "cancel"].map((decision) =>
      request("runtime-request.respond", {
        providerSessionId: "session:one",
        requestId: "request:one",
        decision,
      }),
    ),
    request("thread-title.generate", {
      kind: { type: "initial", messageId: "\uFEFF message:trimmed\u00A0", extra: null },
      extra: [],
    }),
    request("thread-title.generate", {
      kind: { type: "regenerate", messageId: null, extra: { nested: null } },
    }),
    request("attachment.cleanup", {
      attachmentIds: ["", "  attachment:preserved\uFEFF", "\u0085"],
    }),
    request("delegated-tasks.stop"),
  ];
  const boundaryEffects = boundaryRequests.map((value, index) =>
    effect(`boundary:${index}`, `thread:boundary:${index}`, value, iso(day)),
  );
  await runScenario(
    "request schemas strip unknown fields, distinguish null and preserve arbitrary answer values",
    [
      expect(enqueue(boundaryEffects), boundaryEffects.length),
      ...boundaryEffects.map(({ id }) => ({ operation: "outbox_get", effect_id: id })),
      { operation: "reopen", abrupt: true },
      { operation: "outbox_list", command_id: "command:fixture" },
      ...[
        request("provider-turn.start", { runId: null }),
        request("provider-turn.start", { runId: "\uFEFF \u00A0" }),
        request("provider-session.detach", { providerSessionId: "session:one", detail: null }),
        request("provider-session.detach", {
          providerSessionId: "session:one",
          revokeMcpCredential: null,
        }),
        { ...requests[3], subagent: null },
        { ...requests[3], subagent: { id: "node:one", nativeTaskId: null } },
        { ...requests[5], sessionTransition: null },
        {
          ...requests[5],
          sessionTransition: { type: "replace", replacementProviderSessionId: null },
        },
        request("runtime-request.respond", {
          providerSessionId: "session:one",
          requestId: "request:one",
          answers: null,
        }),
        request("runtime-request.respond", {
          providerSessionId: "session:one",
          requestId: "request:one",
          decision: null,
        }),
        request("thread-title.generate", { kind: { type: "initial", messageId: null } }),
        request("attachment.cleanup", { attachmentIds: [null] }),
      ].map((value, index) => ({
        input: enqueue([
          effect(
            `invalid:${index}`,
            "thread:invalid",
            value,
            undefined,
            `command:invalid:${index}`,
          ),
        ]),
        rejected: true,
      })),
      { operation: "outbox_list", command_id: "command:fixture" },
    ],
  );

  await runScenario("thread FIFO, title lane and cross-thread claims", [
    enqueue([
      effect("a-rollback", "thread:a", rollback, iso(1000)),
      effect("b-start", "thread:a", start),
      effect("c-cleanup", "thread:b", cleanup),
      effect("title-1", "thread:a", title),
      effect("title-2", "thread:a", title),
    ]),
    expect(claim(), "c-cleanup"),
    settle("succeed", "c-cleanup"),
    expect(claim(), "title-1"),
    expect(next(), iso(1000)),
    expect(claim(), null),
    expect(claim("worker:two", iso(1000)), "a-rollback"),
    expect(claim("worker:three", iso(1000)), null),
    settle("succeed", "a-rollback", "worker:two", iso(1000)),
    settle("fail", "title-1", "worker:one", iso(1000)),
    expect(claim("worker:three", iso(1000)), "b-start"),
    expect(cancel("thread:a", [start.type, title.type], iso(1001)), ["b-start", "title-2"]),
    expect(settle("succeed", "b-start", "worker:three", iso(1002)), false),
  ]);

  await runScenario("claim order is availability, creation time, then binary identifier", [
    enqueue([effect("z-first", "thread:z", cleanup, iso())]),
    enqueue([effect("a-created-later", "thread:a", cleanup, iso())], iso(1)),
    enqueue([effect("m-earliest-available", "thread:m", cleanup, iso(-1))], iso(2)),
    expect(claim("worker:one", iso(2)), "m-earliest-available"),
    expect(claim("worker:two", iso(2)), "z-first"),
    expect(claim("worker:three", iso(2)), "a-created-later"),
    enqueue([
      effect("é", "thread:é", cleanup),
      effect("z", "thread:az", cleanup),
      effect("a", "thread:aa", cleanup),
    ]),
    expect(claim(), "a"),
    expect(claim(), "z"),
    expect(claim(), "é"),
  ]);

  await runScenario("insertion FIFO survives backward clock movement and duplicate IDs", [
    expect(enqueue([effect("z-inserted-first", "thread:a", cleanup, iso())], iso(100)), 1),
    expect(
      enqueue([
        effect("a-inserted-second", "thread:a", cleanup),
        effect("a-inserted-second", "thread:ignored", title),
      ]),
      1,
    ),
    expect(claim("worker:one", iso(100)), "z-inserted-first"),
    settle("succeed", "z-inserted-first", "worker:one", iso(100)),
    expect(claim("worker:two", iso(100)), "a-inserted-second"),
  ]);

  await runScenario("retry backoff blocks later thread work and lease changes guard completion", [
    enqueue([
      effect("a-rollback", "thread:a", rollback),
      effect("b-start", "thread:a", start),
      effect("c-other", "thread:b", cleanup),
    ]),
    expect(claim(), "a-rollback"),
    expect(retry("a-rollback", iso(1000)), true),
    expect(claim(), "c-other"),
    settle("succeed", "c-other"),
    expect(next(), iso(1000)),
    expect(claim("worker:two", iso(999)), null),
    { operation: "reopen", abrupt: true },
    expect(claim("worker:two", iso(1000)), "a-rollback"),
    expect(retry("a-rollback", iso(2000), "worker:wrong", iso(1000)), false),
    expect(settle("succeed", "a-rollback", "worker:wrong", iso(1000)), false),
    expect(settle("fail", "a-rollback", "worker:wrong", iso(1000)), false),
    expect(retry("a-rollback", iso(1000), "worker:two", iso(1000)), true),
    expect(claim("worker:three", iso(1000)), "a-rollback"),
    expect(settle("succeed", "a-rollback", "worker:two", iso(1000)), false),
    expect(settle("fail", "a-rollback", "worker:three", iso(1000)), true),
    expect(claim("worker:four", iso(1000)), "b-start"),
    cancel("thread:a", [start.type], iso(1001)),
    expect(retry("b-start", iso(2000), "worker:four", iso(1001)), false),
    expect(settle("fail", "b-start", "worker:four", iso(1001)), false),
  ]);

  await runScenario("excluded pending continuations do not block, but running continuations do", [
    enqueue([
      effect("a-continue", "thread:a", continuation, iso(1000)),
      effect("b-start", "thread:a", start),
    ]),
    expect(claim(), null),
    expect(next(), iso(1000)),
    expect(claim("worker:one", iso(), true), "b-start"),
    expect(next(), null),
    settle("succeed", "b-start"),
    expect(claim("worker:two", iso(1000)), "a-continue"),
    enqueue([effect("c-cleanup", "thread:a", cleanup)], iso(1001)),
    expect(claim("worker:three", iso(1001), true), null),
    enqueue([effect("title", "thread:a", title)], iso(1001)),
    expect(claim("worker:three", iso(1001), true), "title"),
    cancel("thread:a", [continuation.type], iso(1002)),
    expect(claim("worker:four", iso(1002), true), "c-cleanup"),
    expect(settle("succeed", "a-continue", "worker:two", iso(1003)), false),
  ]);

  await runScenario("cancellation filters by thread, type and unsettled status", [
    enqueue([
      effect("a-terminal", "thread:a", cleanup),
      effect("b-preview", "thread:a", request("preview.cleanup")),
      effect("c-title", "thread:a", title),
      effect("d-other-thread", "thread:b", cleanup),
    ]),
    expect(claim(), "a-terminal"),
    expect(cancel("thread:a", []), []),
    expect(cancel("thread:a", [cleanup.type, cleanup.type]), ["a-terminal"]),
    expect(cancel("thread:a", [cleanup.type]), []),
    expect(claim(), "b-preview"),
    settle("fail", "b-preview"),
    expect(cancel("thread:a", ["preview.cleanup", title.type]), ["c-title"]),
    expect(claim(), "d-other-thread"),
    settle("succeed", "d-other-thread"),
    expect(cancel("thread:b", [cleanup.type]), []),
  ]);

  const matrix = [];
  const matrixRows = [];
  for (const status of ["running", "succeeded", "failed", "cancelled", "pending"]) {
    for (let index = 0; index < requests.length; index++) {
      const prefix = { running: "a", succeeded: "b", failed: "c", cancelled: "d", pending: "e" }[
        status
      ];
      const id = `${prefix}-${status}-${String(index).padStart(2, "0")}`;
      matrixRows.push(
        effect(id, `thread:${id}`, requests[index], status === "pending" ? iso(day) : iso()),
      );
    }
  }
  matrix.push(enqueue(matrixRows));
  for (const status of ["running", "succeeded", "failed"]) {
    for (let index = 0; index < requests.length; index++) {
      const prefix = { running: "a", succeeded: "b", failed: "c" }[status];
      const id = `${prefix}-${status}-${String(index).padStart(2, "0")}`;
      matrix.push(expect(claim("worker:original"), id));
      if (status !== "running")
        matrix.push(settle(status === "succeeded" ? "succeed" : "fail", id, "worker:original"));
    }
  }
  for (let index = 0; index < requests.length; index++) {
    const id = `d-cancelled-${String(index).padStart(2, "0")}`;
    matrix.push(cancel(`thread:${id}`, [requests[index].type]));
  }
  matrix.push(
    { operation: "reopen", abrupt: true },
    expect(reconcile(iso(1000)), { requeued: 10, cancelled: 10 }),
    expect(reconcile(iso(1001)), { requeued: 0, cancelled: 0 }),
    expect(settle("succeed", "a-running-00", "worker:original", iso(1001)), false),
    expect(claim("worker:recovered", iso(1001)), "a-running-00"),
    expect(settle("succeed", "a-running-00", "worker:original", iso(1002)), false),
    expect(settle("succeed", "a-running-00", "worker:recovered", iso(1002)), true),
  );
  await runScenario(
    "all fifteen effect types across five statuses and process-loss recovery",
    matrix,
  );

  const retention = [];
  for (const [id, action, time] of [
    ["old-success", "succeed", -8 * day],
    ["old-cancel", "cancel", -8 * day],
    ["old-failed", "fail", -8 * day],
    ["boundary-success", "succeed", -7 * day],
    ["boundary-cancel", "cancel", -7 * day],
    ["recent-success", "succeed", 0],
  ]) {
    retention.push(enqueue([effect(id, `thread:${id}`, cleanup)], iso(time)));
    if (action === "cancel") retention.push(cancel(`thread:${id}`, [cleanup.type], iso(time)));
    else
      retention.push(
        expect(claim("worker:one", iso(time)), id),
        settle(action, id, "worker:one", iso(time)),
      );
  }
  retention.push(
    enqueue([effect("old-running", "thread:running", cleanup)], iso(-8 * day)),
    expect(claim("worker:one", iso(-8 * day)), "old-running"),
    enqueue([effect("old-pending", "thread:pending", cleanup)], iso(-8 * day)),
    expect(prune(iso(-7 * day)), 2),
    expect(prune(iso(-7 * day + 1), 1), 2),
    expect(prune(iso(day)), 1),
    { operation: "outbox_get", effect_id: "old-failed" },
    { operation: "outbox_get", effect_id: "old-running" },
    { operation: "outbox_get", effect_id: "old-pending" },
    expect(enqueue([effect("old-success", "thread:reused", title)], iso(day)), 1),
    { operation: "reopen", abrupt: false },
    { operation: "outbox_get", effect_id: "old-success" },
  );
  await runScenario("strict retention cutoff keeps failed and unsettled rows", retention);

  const bulk = Array.from({ length: 503 }, (_, index) =>
    effect(`bulk-${String(index).padStart(4, "0")}`, "thread:bulk", cleanup),
  );
  await runScenario("bounded prune spans batches and empty-table reinsertion preserves FIFO", [
    expect(enqueue(bulk), 503),
    cancel("thread:bulk", [cleanup.type]),
    expect(prune(iso(1)), 503),
    expect(
      enqueue([
        effect("new-first", "thread:new", cleanup),
        effect("new-second", "thread:new", start),
      ]),
      2,
    ),
    expect(claim(), "new-first"),
    settle("succeed", "new-first"),
    expect(claim(), "new-second"),
  ]);

  for (const { type } of requests) {
    NodeAssert.deepEqual(
      coverage.get(type),
      new Set(["pending", "running", "succeeded", "failed", "cancelled"]),
      `complete status coverage: ${type}`,
    );
  }
  const proof = {
    schema_version: 1,
    kind: "immutable-upstream-sqlite-outbox-comparison",
    mode: oracleOnly ? "oracle-only" : policyOnly ? "lust-policy" : "redb-and-lust-policy",
    oracle: oracleSource,
    database_sha256: database
      ? NodeCrypto.createHash("sha256")
          .update(await NodeFSP.readFile(database))
          .digest("hex")
      : null,
    policy_sha256: policy
      ? NodeCrypto.createHash("sha256")
          .update(await NodeFSP.readFile(policy))
          .digest("hex")
      : null,
    comparisons,
    native_comparisons: nativeComparisons,
    policy_comparisons: policyComparisons,
    effect_types: 15,
    statuses_per_type: 5,
    checks,
    boundaries: [
      "Production persistence remains SQLite until the complete repository cutover is qualified.",
      "Effect dispatch and in-process notifications are outside the durable outbox comparison.",
      "Insertion ordinals are compared by FIFO order because SQLite may reuse deleted rowids.",
      "Retry timestamps are computed by the host; the policy receives the resulting canonical timestamp.",
      "Invalid request payloads compare rejection and unchanged persistence; backend-specific error messages are not asserted equal.",
    ],
  };
  if (proofPath) await NodeFSP.writeFile(proofPath, `${JSON.stringify(proof, null, 2)}\n`);
  console.log(
    `Outbox comparisons passed: ${comparisons} upstream operations, ${nativeComparisons} complete redb comparisons, ${policyComparisons} policy comparisons; 15 effect types × 5 statuses.`,
  );
} finally {
  await NodeFSP.rm(temporary, { recursive: true, force: true });
}
