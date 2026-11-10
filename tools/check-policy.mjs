import * as assert from "node:assert/strict";
import { spawn, spawnSync } from "node:child_process";
import { once } from "node:events";
import { stripTypeScriptTypes } from "node:module";
import { repoRoot, upstreamSource } from "./upstream-source.mjs";

const source = upstreamSource("apps/server/src/orchestration-v2/CommandPolicy.ts");
const start = source.indexOf("function unsupported(");
const end = source.indexOf("export const layer:", start);
assert.ok(start >= 0 && end > start, "Review the changed upstream policy boundary");
assert.equal(source.indexOf("function unsupported(", start + 1), -1);
assert.equal(source.indexOf("export const layer:", end + 1), -1);

// Only the Effect result container is substituted. Every decision branch and
// failure message comes unchanged from the immutable upstream implementation.
class Outcome {
  constructor(ok, value) {
    this.ok = ok;
    this.value = value;
  }
  pipe(operation) {
    return operation(this);
  }
}
const Effect = {
  void: new Outcome(true, undefined),
  succeed: (value) => new Outcome(true, value),
  fail: (value) => new Outcome(false, value),
  as: (value) => (outcome) => outcome.ok ? new Outcome(true, value) : outcome,
};
class Unsupported {
  constructor(fields) {
    Object.assign(this, fields);
  }
}
const upstream = new Function(
  "Effect",
  "CommandPolicyCapabilityUnsupportedError",
  `${stripTypeScriptTypes(source.slice(start, end), { mode: "strip" })}\nreturn {
    ensureQueuedMessages, decideSteeringExecution, ensureInterrupt, ensureNativeFork,
    decideForkExecution, ensureRollback, ensureContextHandoff
  };`,
)(Effect, Unsupported);

function capabilities(bits) {
  const bit = (index) => (bits & (1 << index)) !== 0;
  return {
    turns: {
      supportsQueuedMessages: bit(0), supportsActiveSteering: bit(1),
      supportsInterrupt: bit(2), supportsSteeringByInterruptRestart: bit(3),
    },
    threads: {
      canForkThread: bit(4), canForkFromTurn: bit(5), canRollbackThread: bit(7),
    },
    identity: { nativeThreadIds: bit(6) ? "strong" : "weak" },
    checkpointing: {
      providerCanRollbackConversation: bit(8), providerRollbackReturnsSnapshot: bit(9),
    },
    context: {
      canConsumeHandoffSummaries: bit(10), acceptsSyntheticUserContext: bit(11),
      supportsDeltaHandoff: bit(12), supportsFullThreadHandoff: bit(13),
    },
  };
}

const protocol = "t3ls-policy-v1";
function row(operation, mask, args, outcome) {
  assert.ok(outcome instanceof Outcome);
  const output = outcome.ok
    ? `ok\t${outcome.value ?? "-"}`
    : `unsupported\t${outcome.value.capability}\t${outcome.value.detail}`;
  return { input: [protocol, operation, mask, ...args].join("\t"), output };
}

function* cases() {
  for (let mask = 0; mask < 1 << 14; mask++) {
    const input = { commandId: "c", threadId: "t", providerInstanceId: "p", capabilities: capabilities(mask) };
    yield row("queued", mask, [], upstream.ensureQueuedMessages(input));
    yield row("interrupt", mask, [], upstream.ensureInterrupt(input));
    yield row("rollback", mask, [], upstream.ensureRollback(input));
    for (const flag of [false, true]) {
      yield row("steering", mask, [Number(flag)], upstream.decideSteeringExecution({ ...input, forceRestart: flag }));
      yield row("native-fork", mask, [Number(flag)], upstream.ensureNativeFork({ ...input, fromSpecificTurn: flag }));
    }
    for (const strategy of ["fork_delta_context", "delta_context", "full_thread_summary"]) {
      yield row("handoff", mask, [strategy], upstream.ensureContextHandoff({ ...input, strategy }));
    }
    for (let flags = 0; flags < 32; flags++) {
      const values = [0, 1, 2, 3, 4].map((bit) => Number((flags & (1 << bit)) !== 0));
      yield row("fork", mask, values, upstream.decideForkExecution({
        ...input,
        sameProvider: Boolean(values[0]),
        hasStrongNativeSource: Boolean(values[1]),
        sourceRunStatus: values[2] ? "completed" : "failed",
        fromSpecificTurn: Boolean(values[3]),
        sourceBusy: Boolean(values[4]),
      }));
    }
  }
}

let executable = process.argv[2];
if (!executable) {
  const build = spawnSync("cargo", ["build", "--locked", "--quiet", "--manifest-path",
    `${repoRoot}/Cargo.toml`, "-p", "t3-code-ls-core", "--bin", "t3-code-ls-policy"],
    { encoding: "utf8", timeout: 120_000 });
  assert.ifError(build.error);
  assert.equal(build.status, 0, build.stderr);
  executable = `${repoRoot}/target/debug/t3-code-ls-policy${process.platform === "win32" ? ".exe" : ""}`;
}
function execute(input, args = []) {
  const result = spawnSync(executable, args, {
    input, encoding: "utf8", maxBuffer: 4 * 1024 * 1024, timeout: 30_000,
  });
  assert.ifError(result.error);
  assert.equal(result.signal, null, "policy process terminated by signal");
  return result;
}

let checked = 0;
let batch = [];
function checkBatch() {
  const result = execute(batch.map((item) => item.input).join("\n") + "\n");
  assert.equal(result.status, 0, result.stderr);
  const actual = result.stdout.replaceAll("\r\n", "\n").split("\n");
  assert.equal(actual.pop(), "", "policy responses must end in a newline");
  assert.equal(actual.length, batch.length, "one response is required per request");
  for (let index = 0; index < batch.length; index++) {
    assert.equal(actual[index], batch[index].output, `Upstream policy mismatch: ${batch[index].input}`);
  }
  checked += batch.length;
  batch = [];
}
for (const item of cases()) {
  batch.push(item);
  if (batch.length === 1_024) checkBatch();
}
if (batch.length > 0) checkBatch();
assert.equal(checked, 688_128);

// EOF, invalid frames, and version drift are operational failures, not denials
// attributed to a provider. A failed frame must not execute the next request.
const good = `${protocol}\tqueued\t1\n`;
const single = execute("", [protocol, "steering", "14", "1"]);
assert.equal(single.status, 0, single.stderr);
assert.equal(single.stdout, "ok\tinterrupt_restart\n");
for (const bad of [
  "t3ls-policy-v2\tqueued\t1\n", `${protocol}\tqueued\t16384\n`,
  `${protocol}\tqueued\t+1\n`, `${protocol}\tsteering\t14\ttrue\n`,
  `${protocol}\tqueued\t1\textra\n`, `${protocol}\tunknown\t0\n`,
  `${protocol}\tfork\t112\t1\t1\t1\t0\n`,
]) {
  const result = execute(good + bad + good);
  assert.equal(result.status, 2);
  assert.ok(result.stdout.startsWith("ok\t-\ninvalid\t"));
  assert.equal(result.stdout.split("ok\t-\n").length, 2, "later request ran after a protocol failure");
}
for (const bad of [good.trimEnd(), "x".repeat(258), Buffer.from([255, 10])]) {
  const result = execute(bad);
  assert.equal(result.status, 2);
  assert.equal(result.stdout, "");
}
const disconnected = spawn(executable, [], { stdio: ["pipe", "pipe", "pipe"], timeout: 30_000 });
const exited = once(disconnected, "close");
let writeFailure = "";
disconnected.stderr.setEncoding("utf8");
disconnected.stderr.on("data", (chunk) => { writeFailure += chunk; });
const outputClosed = once(disconnected.stdout, "close");
disconnected.stdout.destroy();
await outputClosed;
disconnected.stdin.end(good);
const [code, signal] = await exited;
assert.equal(code, 2, "losing the response consumer must fail the policy process");
assert.equal(signal, null, "a broken response pipe must be reported without a signal");
assert.match(writeFailure, /cannot (write|flush) policy response/);
console.log(`${checked} provider-policy decisions match the pinned upstream implementation; malformed input, single-request execution, fail-stop framing, and broken-pipe failure passed.`);
