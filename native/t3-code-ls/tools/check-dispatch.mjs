import * as assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { stripTypeScriptTypes } from "node:module";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { dispatchFacts, dispatchResult } from "../../../apps/server/src/orchestration-v2/NativeDispatchFacts.ts";
import { repoRoot, upstreamSource } from "./upstream-source.mjs";

const source = upstreamSource("apps/server/src/orchestration-v2/CommandPolicy.ts");
const start = source.indexOf("export function isNativeMaintenanceCommand(");
const end = source.indexOf("interface CapabilityCheckInput", start);
assert.ok(start >= 0 && end > start, "Review the changed upstream dispatch boundary");
assert.equal(source.indexOf("export function isNativeMaintenanceCommand(", start + 1), -1);
const upstream = new Function(`${stripTypeScriptTypes(source.slice(start, end), { mode: "strip" }).replaceAll("export function", "function")}\nreturn { resolveMessageDispatchIntent, goalCommandStopsActiveRun };`)();

function empty() {
  return { runs: [], messages: [], providerThreads: [], providerSessions: [], providerTurns: [] };
}
function addRun(projection, id, status, mask, turn = "running", text = "message", attachments = []) {
  const index = projection.runs.length;
  const providerThreadId = `provider-thread-${index}`;
  const providerSessionId = `provider-session-${index}`;
  const activeAttemptId = `attempt-${index}`;
  const userMessageId = `message-${index}`;
  projection.runs.push({ id, status, providerThreadId, activeAttemptId, userMessageId });
  projection.messages.push({ id: userMessageId, text, attachments });
  if (mask !== undefined) {
    projection.providerThreads.push({ id: providerThreadId, providerSessionId });
    projection.providerSessions.push({ id: providerSessionId, capabilities: { turns: {
      supportsQueuedMessages: Boolean(mask & 1), supportsActiveSteering: Boolean(mask & 2),
      supportsInterrupt: Boolean(mask & 4), supportsSteeringByInterruptRestart: Boolean(mask & 8),
    } } });
  }
  if (turn !== "absent") projection.providerTurns.push({
    runAttemptId: turn === "foreign" ? "another-attempt" : activeAttemptId,
    status: turn === "foreign" ? "running" : turn,
  });
}
const requestedModes = [
  { type: "start_immediately" }, { type: "queue_after_active" },
  { type: "steer_active", targetRunId: "requested" },
  { type: "restart_active", targetRunId: "requested" },
];
const intents = [undefined, "auto", "steer", "restart"];
const masks = [undefined, ...Array.from({ length: 16 }, (_, index) => index)];
const messages = [
  ["ordinary message", []], ["/compact", []], ["/logout", []],
  ["\ufeff/COMPACT\u2029", []], ["/compact", [{ id: "attachment" }]],
];
function dispatchCase(projection, requested, intent) {
  const facts = dispatchFacts(projection, requested, intent);
  assert.equal(facts.args.length, 12);
  const frame = facts.args.join("\t");
  assert.ok(frame.length <= 256 && /^[\t\x20-\x7e]+$/.test(frame));
  return {
    method: "resolveMessageDispatchIntent", args: [projection, requested, intent], frame,
    expected: upstream.resolveMessageDispatchIntent(projection, requested, intent),
    decode: (value) => dispatchResult(value, requested, facts.activeRunId),
  };
}
function* cases() {
  for (const state of [undefined, "preparing", "starting", "running", "waiting", "completed"]) {
    for (const mask of masks) {
      for (const turn of ["running", "completed", "foreign", "absent"]) {
        for (const [text, attachments] of messages) {
          const projection = empty();
          addRun(projection, "requested", "completed", 1, "completed");
          if (state !== undefined) addRun(projection, "active", state, mask, turn, text, attachments);
          addRun(projection, "trailing-inactive", "completed", 15, "completed");
          for (const requested of requestedModes) for (const intent of intents) {
            yield dispatchCase(projection, requested, intent);
          }
        }
      }
    }
  }
  for (const state of [undefined, "queued", "preparing", "starting", "running", "waiting", "completed", "failed", "cancelled", "rolled_back"]) {
    for (const mask of masks) for (const [text, attachments] of messages) {
      const projection = empty();
      if (state !== undefined) addRun(projection, "requested", state, mask, "running", text, attachments);
      addRun(projection, "latest-active", "running", 15);
      for (const requested of requestedModes) for (const intent of intents) {
        yield dispatchCase(projection, requested, intent);
      }
    }
  }
  // Reference lookups use array order and distinguish absent, null, and missing links.
  for (const variant of ["duplicate-run", "duplicate-thread", "duplicate-session", "missing-message", "missing-thread", "missing-session", "null-session", "missing-turn", "old-attempt", "empty-id"]) {
    const projection = empty();
    addRun(projection, "requested", "running", 1, "running", "/logout");
    addRun(projection, "latest-active", "waiting", 15);
    if (variant === "duplicate-run") projection.runs[0].id = "latest-active";
    if (variant === "duplicate-thread") projection.providerThreads[0].id = projection.providerThreads[1].id;
    if (variant === "duplicate-session") projection.providerSessions[0].id = projection.providerSessions[1].id;
    if (variant === "missing-message") projection.messages = [];
    if (variant === "missing-thread") projection.providerThreads = [];
    if (variant === "missing-session") projection.providerSessions = [];
    if (variant === "null-session") projection.providerThreads[1].providerSessionId = null;
    if (variant === "missing-turn") projection.providerTurns = [];
    if (variant === "old-attempt") projection.providerTurns[1].runAttemptId = "old-attempt";
    if (variant === "empty-id") projection.runs[1].id = "";
    for (const requested of requestedModes) for (const intent of intents) {
      yield dispatchCase(projection, requested, intent);
    }
  }
  for (const type of ["set", "resume", "clear", "pause", "show"]) {
    for (const status of [null, "active", "paused", "completed", "failed", "future-status"]) {
      yield {
        method: "goalCommandStopsActiveRun", args: [{ type }, status],
        frame: `t3ls-goal-v1\t${type}\t${status === "active" ? 1 : 0}`,
        expected: upstream.goalCommandStopsActiveRun({ type }, status),
        decode(value) {
          assert.ok(value === "true" || value === "false");
          return value === "true";
        },
      };
    }
  }
}

let executable = process.argv[2];
if (!executable) {
  const build = spawnSync("cargo", ["build", "--locked", "--quiet", "--manifest-path",
    `${repoRoot}/native/t3-code-ls/Cargo.toml`, "-p", "t3-code-ls-core", "--bin", "t3-code-ls-policy"],
    { encoding: "utf8", timeout: 120_000 });
  assert.ifError(build.error);
  assert.equal(build.status, 0, build.stderr);
  executable = `${repoRoot}/native/t3-code-ls/target/debug/t3-code-ls-policy${process.platform === "win32" ? ".exe" : ""}`;
}
function execute(input) {
  const result = spawnSync(executable, [], { input, encoding: "utf8", timeout: 30_000, maxBuffer: 4 * 1024 * 1024 });
  assert.ifError(result.error);
  assert.equal(result.signal, null);
  return result;
}
let checked = 0;
let batch = [];
const representatives = [];
function checkBatch() {
  const result = execute(batch.map((item) => item.frame).join("\n") + "\n");
  assert.equal(result.status, 0, result.stderr);
  const lines = result.stdout.split("\n");
  assert.equal(lines.pop(), "");
  assert.equal(lines.length, batch.length);
  for (let index = 0; index < batch.length; index++) {
    const item = batch[index];
    assert.match(lines[index], /^ok\t[a-z_]+$/);
    const value = lines[index].slice(3);
    assert.deepEqual(item.decode(value), item.expected, `Dispatch mismatch: ${item.frame}`);
    if (value === "requested") assert.equal(item.decode(value), item.args[1]);
  }
  checked += batch.length;
  batch = [];
}
let ordinal = 0;
for (const item of cases()) {
  if (ordinal++ % 97 === 0 || item.method === "goalCommandStopsActiveRun") representatives.push(item);
  batch.push(item);
  if (batch.length === 1_024) checkBatch();
}
if (batch.length) checkBatch();
assert.equal(checked, 46_430);
const good = "t3ls-goal-v1\tshow\t0\n";
for (const bad of ["t3ls-goal-v2\tshow\t0\n", "t3ls-goal-v1\tshow\ttrue\n", "t3ls-goal-v1\tunknown\t0\n",
  "t3ls-dispatch-v1\tother\tauto\trunning\t16\t1\t-\t-\t0\t-\t-\t0\n",
  "t3ls-dispatch-v1\tother\tauto\trunning\t0\t1\t-\t-\t0\t-\t-\t0\textra\n"]) {
  const result = execute(good + bad + good);
  assert.equal(result.status, 2);
  assert.ok(result.stdout.startsWith("ok\tfalse\ninvalid\t"));
  assert.equal(result.stdout.split("ok\tfalse\n").length, 2);
}
console.log(`${checked} message-dispatch and goal decisions match the immutable upstream functions through the production fact adapter.`);

if (process.argv[3] === "--service") {
  const Effect = await import("effect/Effect");
  const { CommandPolicyV2, layer } = await import("../../../apps/server/src/orchestration-v2/CommandPolicy.ts");
  async function select(path) {
    const previous = process.env.T3_CODE_LS_POLICY_EXECUTABLE;
    try {
      if (path === undefined) delete process.env.T3_CODE_LS_POLICY_EXECUTABLE;
      else process.env.T3_CODE_LS_POLICY_EXECUTABLE = path;
      return await Effect.runPromise(Effect.provide(CommandPolicyV2, layer));
    } finally {
      if (previous === undefined) delete process.env.T3_CODE_LS_POLICY_EXECUTABLE;
      else process.env.T3_CODE_LS_POLICY_EXECUTABLE = previous;
    }
  }
  const native = await select(executable);
  const reference = await select(undefined);
  let assertions = 0;
  for (const item of representatives) {
    assert.deepEqual(await Effect.runPromise(native[item.method](...item.args)), item.expected);
    assert.deepEqual(await Effect.runPromise(reference[item.method](...item.args)), item.expected);
    assertions += 2;
  }
  const directory = await mkdtemp(join(tmpdir(), "t3ls-dispatch-"));
  const projection = empty();
  const mode = { type: "start_immediately" };
  try {
    const missing = await select(join(directory, "missing"));
    await assert.rejects(Effect.runPromise(missing.resolveMessageDispatchIntent(projection, mode, "auto")));
    await assert.rejects(Effect.runPromise(missing.goalCommandStopsActiveRun({ type: "set" }, null)));
    assertions += 2;
    const witness = join(directory, "selection-witness");
    await writeFile(witness, `#!${process.execPath}\nconst protocol = process.argv[2];\nif (protocol === "t3ls-dispatch-v1") process.stdout.write("ok\\tqueue_after_active\\n");\nelse if (protocol === "t3ls-goal-v1") process.stdout.write("ok\\tfalse\\n");\nelse process.exitCode = 2;\n`, { mode: 0o700 });
    const selected = await select(witness);
    assert.deepEqual(await Effect.runPromise(selected.resolveMessageDispatchIntent(projection, mode, "auto")), { type: "queue_after_active" });
    assert.equal(await Effect.runPromise(selected.goalCommandStopsActiveRun({ type: "set" }, null)), false);
    assertions += 2;
    for (const reply of ["", "ok\t-\n", "ok\ttrue\n", "ok\tsteer_active\n", "ok\tunknown\n",
      "ok\tqueue_after_active\n\n", "ok\tqueue_after_active\r\n", "unsupported\tqueued_messages\tno\n"]) {
      const path = join(directory, "invalid-response");
      await writeFile(path, `#!${process.execPath}\nprocess.stdout.write(${JSON.stringify(reply)});\n`, { mode: 0o700 });
      const broken = await select(path);
      await assert.rejects(Effect.runPromise(broken.resolveMessageDispatchIntent(projection, mode, "auto")));
      assertions++;
    }
    const controller = new AbortController();
    controller.abort();
    await assert.rejects(Effect.runPromise(native.resolveMessageDispatchIntent(projection, mode, "auto"), { signal: controller.signal }));
    assertions++;
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
  const orchestrator = await readFile(`${repoRoot}/apps/server/src/orchestration-v2/Orchestrator.ts`, "utf8");
  assert.equal(orchestrator.split("yield* commandPolicy.resolveMessageDispatchIntent(").length - 1, 1);
  assert.equal(orchestrator.split("yield* commandPolicy.goalCommandStopsActiveRun(").length - 1, 2);
  assert.equal(orchestrator.includes("\n  resolveMessageDispatchIntent,"), false);
  assert.equal(orchestrator.includes("\n  goalCommandStopsActiveRun,"), false);
  stripTypeScriptTypes(orchestrator, { mode: "strip" });
  assertions += 5;
  const proof = { status: "passed", differential_cases: checked, assertions,
    binary_sha256: createHash("sha256").update(await readFile(executable)).digest("hex"),
    scope: "Message-dispatch and goal service selection, protocol errors, and orchestrator call-site wiring; not whole-application qualification" };
  if (process.argv[4]) await writeFile(process.argv[4], JSON.stringify(proof, null, 2) + "\n");
  console.log(`${assertions} actual message-dispatch/goal service and call-site checks passed with the Lust executable.`);
}
