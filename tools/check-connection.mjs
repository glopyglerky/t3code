import * as NodeAssert from "node:assert/strict";
import * as NodeChildProcess from "node:child_process";
import * as NodeModule from "node:module";
import { repoRoot, upstreamSource } from "./upstream-source.mjs";

const supervisor = upstreamSource("packages/client-runtime/src/connection/supervisor.ts");
const wakeups = upstreamSource("packages/client-runtime/src/connection/wakeups.ts");

function section(source, start, end) {
  const from = source.indexOf(start);
  NodeAssert.ok(from >= 0, `Upstream connection source no longer contains ${start}`);
  NodeAssert.equal(
    source.indexOf(start, from + start.length),
    -1,
    `Upstream anchor is ambiguous: ${start}`,
  );
  const to = source.indexOf(end, from + start.length);
  NodeAssert.ok(to > from, `Upstream connection section ${start} no longer ends at ${end}`);
  return source.slice(from, to);
}

const constantNames = [
  "RETRY_BASE_DELAY_MS",
  "RETRY_MAX_DELAY_MS",
  "SESSION_LOSS_RETRY_DELAY_MS",
  "APPROVAL_PENDING_RETRY_DELAY_MS",
  "APPROVAL_PENDING_FAST_RETRIES",
  "BACKOFF_RESET_AFTER_MS",
];
const constants = constantNames
  .map((name) => {
    const match = supervisor.match(new RegExp(`^const ${name} = [0-9_]+;`, "m"));
    NodeAssert.ok(match, `Review changed upstream constant: ${name}`);
    return match[0];
  })
  .join("\n");

// Execute the actual pinned state initialization/reset, retry functions and post-attempt decision
// block. Only the Effect boundary is supplied here: deterministic Random.next,
// the intent ref, a state sink, and an inactive wakeup. The decision branches,
// ordering, arithmetic, and resets remain the upstream source, unchanged.
const retryFunction = section(supervisor, "function retryDelayMs(", "interface SilentRoute");
const connectingFunction = section(
  supervisor,
  "function connectingState(",
  "function failureFromExit",
);
const decisionBlock = section(
  supervisor,
  "      if (outcome.established) {",
  "      pendingRetry = Option.map",
);
const retryState = section(supervisor, "    let failureCount = 0;", "\n    for (;;) {");
const wakeupFunctions = section(
  wakeups,
  "function isApplicationActiveWakeup(",
  "export class ConnectionWakeups",
);
const original = `
${constants}
${retryFunction}
${connectingFunction}
${wakeupFunctions}
function makeRetry() {
  const Option = { none: () => null };
${retryState}
  const snapshot = () => ({ failureCount, backoffFailures, approvalWaits });
  function evaluate(outcome, failedProbe, random) {
    const nextGeneration = generation + 1;
    const attempt = failureCount + 1;
    let state = null;
    const value = (result) => ({ *[Symbol.iterator]() { return result; } });
    const Random = { next: value(random) };
    const Ref = { get: (result) => value(result) };
    const intent = { desired: true, network: "online" };
    const setState = (next) => { state = next; return value(undefined); };
    const waitForSignal = value(false);
    function* run() {
      for (let iteration = 0; ; iteration++) {
        if (iteration > 0) return { action: state?.phase === "blocked" ? "signal" : "now", delayMs: 0, ...snapshot() };
${decisionBlock}
        return { action: "after", delayMs, ...snapshot() };
      }
    }
    const result = run().next();
    if (!result.done) throw new Error("Unexpected Effect suspension in retry decision block");
    return result.value;
  }
  return { evaluate, reset: resetRetryLadder, snapshot };
}
`;
const javascript = NodeModule.stripTypeScriptTypes(original, { mode: "strip" }).replaceAll(
  "export function",
  "function",
);
const upstream = new Function(
  `${javascript}\nreturn { makeRetry, retryDelayMs, resetsRetryBackoff, shouldResubscribeAfterWakeup, shouldRelistAccountAfterWakeup, reachesConnectionSupervisor, BACKOFF_RESET_AFTER_MS };`,
)();

function outcome(end, established, stable) {
  return end < 2 || end === 4
    ? {
        _tag: "Failure",
        established,
        stable,
        failure: {
          attemptSpan: null,
          error: {
            _tag: end === 1 ? "ConnectionBlockedError" : "ConnectionTransientError",
            reason: end === 1 ? "authentication" : end === 4 ? "approval-pending" : "network",
            message: "fixture failure",
          },
        },
      }
    : { _tag: "Interrupted", established, stable, resetRetry: end === 3 };
}
function seeded(failures, approvalWaits = 0) {
  const retry = upstream.makeRetry();
  for (let index = 0; index < failures; index++) {
    retry.evaluate(outcome(0, false, false), false, 0);
  }
  for (let index = 0; index < approvalWaits; index++) {
    retry.evaluate(outcome(4, false, false), false, 0);
  }
  return retry;
}
const expected = [`stable\t${upstream.BACKOFF_RESET_AFTER_MS}`];
let decisions = 0;
function addDecision(result) {
  expected.push(
    `${result.action}\t${result.delayMs}\t${result.failureCount}\t${result.failureCount + 1}\t${result.backoffFailures}\t${result.approvalWaits}`,
  );
  decisions++;
}
for (let failures = 0; failures <= 24; failures++) {
  for (let sample = 0; sample <= 2_000; sample++) {
    expected.push(`delay\t${upstream.retryDelayMs(failures, sample / 2_000)}`);
  }
}
for (let sample = 0; sample <= 2_000; sample++) {
  const result = seeded(0).evaluate(outcome(0, true, false), false, sample / 2_000);
  expected.push(`loss\t${result.delayMs}`);
}
// Include both sides of the approval budget and histories where earlier network
// failures must survive approval polling without being multiplied by it.
const histories = [
  [0, 0],
  [1, 0],
  [2, 0],
  [7, 0],
  [8, 0],
  [25, 0],
  [0, 1],
  [0, 149],
  [0, 150],
  [0, 151],
  [8, 149],
  [8, 150],
  [8, 151],
];
for (const [failures, approvalWaits] of histories) {
  for (const established of [false, true]) {
    for (const stable of [false, true]) {
      for (const probeFailed of [false, true]) {
        for (let end = 0; end < 5; end++) {
          for (let sample = 0; sample <= 16; sample++) {
            addDecision(
              seeded(failures, approvalWaits).evaluate(
                outcome(end, established, stable),
                probeFailed,
                sample / 16,
              ),
            );
          }
        }
      }
    }
  }
}
let wakeupCases = 0;
for (const [failures, approvalWaits] of histories) {
  for (const reason of [
    "application-active",
    "application-active-probe",
    "application-active-long",
    "application-focused",
    "credentials-changed",
    "network-changed",
  ]) {
    const reset = upstream.resetsRetryBackoff(reason);
    const retry = seeded(failures, approvalWaits);
    if (reset) retry.reset();
    const snapshot = retry.snapshot();
    expected.push(
      `wakeup\t${reset}\t${upstream.shouldResubscribeAfterWakeup(reason)}\t${upstream.shouldRelistAccountAfterWakeup(reason)}\t${upstream.reachesConnectionSupervisor(reason)}\t${snapshot.failureCount}\t${snapshot.backoffFailures}\t${snapshot.approvalWaits}`,
    );
    wakeupCases++;
  }
}
const retry = seeded(0);
for (let index = 0; index < 360; index++) {
  const end =
    index < 160
      ? 4
      : index % 11 === 0
        ? 1
        : index % 5 === 0
          ? 2
          : index % 17 === 0
            ? 3
            : index % 3 === 0
              ? 4
              : 0;
  const result = retry.evaluate(
    outcome(end, index >= 160 && index % 3 === 0, index >= 160 && index % 53 === 0),
    index >= 160 && index % 19 === 0,
    (index % 13) / 13,
  );
  addDecision(result);
  for (const [divisor, reason] of [
    [23, "network-changed"],
    [29, "credentials-changed"],
    [31, "application-focused"],
  ]) {
    if (index >= 160 && index % divisor === 0 && upstream.resetsRetryBackoff(reason)) retry.reset();
  }
}
const executable = process.argv[2];
const result = executable
  ? NodeChildProcess.spawnSync(executable, [], { encoding: "utf8", maxBuffer: 4 * 1024 * 1024 })
  : NodeChildProcess.spawnSync(
      "cargo",
      [
        "run",
        "--locked",
        "--quiet",
        "--manifest-path",
        `${repoRoot}/Cargo.toml`,
        "-p",
        "t3-code-ls-core",
        "--example",
        "connection_matrix",
      ],
      { encoding: "utf8", maxBuffer: 4 * 1024 * 1024 },
    );
NodeAssert.ifError(result.error);
NodeAssert.equal(result.status, 0, `Connection oracle failed:\n${result.stderr}`);
const actual = result.stdout.replaceAll("\r\n", "\n").trimEnd().split("\n");
NodeAssert.equal(
  actual.length,
  expected.length,
  "Connection oracle emitted a different number of cases",
);
for (let row = 0; row < expected.length; row++) {
  NodeAssert.equal(
    actual[row],
    expected[row],
    `Connection decision differs from upstream TypeScript at row ${row + 1}`,
  );
}
console.log(
  `${25 * 2_001} exponential delay cases, ${2_001} session-loss delays, ${decisions} retry transitions, and ${wakeupCases} wakeup cases match the pinned upstream TypeScript.`,
);
