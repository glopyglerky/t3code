import * as assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import * as Effect from "effect/Effect";
import {
  CommandPolicyV2,
  CommandPolicyCapabilityUnsupportedError,
  layer,
} from "../../../apps/server/src/orchestration-v2/CommandPolicy.ts";

const executable = process.argv[2];
assert.ok(executable, "Pass the Lust-built policy executable");
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
function input(mask, strength = "weak") {
  const bit = (index) => Boolean(mask & (1 << index));
  return {
    commandId: "command-1", threadId: "thread-1", providerInstanceId: "provider-1",
    capabilities: {
      turns: { supportsQueuedMessages: bit(0), supportsActiveSteering: bit(1),
        supportsInterrupt: bit(2), supportsSteeringByInterruptRestart: bit(3) },
      threads: { canForkThread: bit(4), canForkFromTurn: bit(5), canRollbackThread: bit(7) },
      identity: { nativeThreadIds: bit(6) ? "strong" : strength },
      checkpointing: { providerCanRollbackConversation: bit(8), providerRollbackReturnsSnapshot: bit(9) },
      context: { canConsumeHandoffSummaries: bit(10), acceptsSyntheticUserContext: bit(11),
        supportsDeltaHandoff: bit(12), supportsFullThreadHandoff: bit(13) },
    },
  };
}

let assertions = 0;
function outcome(effect) {
  return Effect.runPromise(effect.pipe(Effect.match({
    onSuccess: (value) => ({ ok: true, value }),
    onFailure: (error) => {
      assert.ok(error instanceof CommandPolicyCapabilityUnsupportedError);
      return { ok: false, tag: error._tag, commandId: error.commandId, threadId: error.threadId,
        providerInstanceId: error.providerInstanceId, capability: error.capability,
        detail: error.detail, message: error.message };
    },
  })));
}
async function compare(method, value) {
  assert.deepEqual(await outcome(native[method](value)), await outcome(reference[method](value)),
    `${method} differs for ${JSON.stringify(value)}`);
  assertions++;
}
const all = (1 << 14) - 1;
const masks = [0, all];
for (let bit = 0; bit < 14; bit++) masks.push(1 << bit, all ^ (1 << bit));
for (const mask of masks) {
  const base = input(mask);
  for (const method of ["ensureQueuedMessages", "ensureInterrupt", "ensureRollback"]) {
    await compare(method, base);
  }
  for (const flag of [false, true]) {
    await compare("decideSteeringExecution", { ...base, forceRestart: flag });
    await compare("ensureNativeFork", { ...base, fromSpecificTurn: flag });
  }
  await compare("decideSteeringExecution", base);
  for (const strategy of ["fork_delta_context", "delta_context", "full_thread_summary"]) {
    await compare("ensureContextHandoff", { ...base, strategy });
  }
  for (let flags = 0; flags < 32; flags++) {
    await compare("decideForkExecution", { ...base,
      sameProvider: Boolean(flags & 1), hasStrongNativeSource: Boolean(flags & 2),
      sourceRunStatus: flags & 4 ? "completed" : "failed",
      fromSpecificTurn: Boolean(flags & 8), sourceBusy: Boolean(flags & 16),
    });
  }
}
for (const strength of ["none", "weak", "strong"]) {
  for (const sourceRunStatus of ["completed", "waiting", "queued", "preparing", "starting", "running", "failed", "cancelled", "rolled_back"]) {
    const base = input(all ^ (1 << 6), strength);
    await compare("decideForkExecution", { ...base, sameProvider: true, hasStrongNativeSource: true,
      sourceRunStatus, fromSpecificTurn: true, sourceBusy: false });
  }
}
await Promise.all(masks.slice(0, 16).map((mask) => compare("ensureQueuedMessages", input(mask))));
for (const path of ["relative-policy", "", "/invalid\0path"]) {
  await assert.rejects(select(path));
  assertions++;
}
const directory = await mkdtemp(join(tmpdir(), "t3ls-policy-adapter-"));
try {
  const missing = await select(join(directory, "does-not-exist"));
  // Infrastructure defects must not become provider refusals or fall back to upstream.
  await assert.rejects(Effect.runPromise(Effect.flip(missing.ensureQueuedMessages(input(all)))));
  assertions++;
  const bodies = [
    ...["", "ok\t-", "ok\t-\nok\t-\n", "ok\tunknown\n", "ok\t-\r\n",
      "ok\t-\n\n", "unsupported\tbogus\tdetail\n", "unsupported\tqueued_messages\t\n",
      "unsupported\tinterrupt\twrong operation\n", "unsupported\tqueued_messages\tbad\0text\n",
    ].map((value) => `process.stdout.write(${JSON.stringify(value)});`),
    'process.stdout.write("ok\\t-\\n"); process.exitCode = 2;',
    'process.stdout.write("ok\\t-\\n"); process.stderr.write("unexpected stderr");',
    'process.stdout.write("x".repeat(8192));',
  ];
  for (const body of bodies) {
    const fixture = join(directory, "invalid-response");
    await writeFile(fixture, `#!${process.execPath}\n${body}\n`, { mode: 0o700 });
    const broken = await select(fixture);
    await assert.rejects(Effect.runPromise(Effect.flip(broken.ensureQueuedMessages(input(all)))));
    assertions++;
  }
  const fixture = join(directory, "private-environment");
  await writeFile(fixture, `#!${process.execPath}\nprocess.stdout.write(process.env.T3_LS_TEST_SECRET === undefined ? "ok\\t-\\n" : "invalid\\tcredential leaked\\n");\n`, { mode: 0o700 });
  const previous = process.env.T3_LS_TEST_SECRET;
  try {
    process.env.T3_LS_TEST_SECRET = "synthetic-test-value";
    const isolated = await select(fixture);
    assert.equal(await Effect.runPromise(isolated.ensureQueuedMessages(input(all))), undefined);
    assertions++;
  } finally {
    if (previous === undefined) delete process.env.T3_LS_TEST_SECRET;
    else process.env.T3_LS_TEST_SECRET = previous;
  }
  const controller = new AbortController();
  controller.abort();
  await assert.rejects(Effect.runPromise(native.ensureQueuedMessages(input(all)), { signal: controller.signal }));
  assertions++;
} finally {
  await rm(directory, { recursive: true, force: true });
}
const proof = {
  status: "passed", assertions,
  binary_sha256: createHash("sha256").update(await readFile(executable)).digest("hex"),
  scope: "Actual CommandPolicy service and Effect runtime using Lust-built executable; not full application or UI parity",
};
if (process.argv[3]) await writeFile(process.argv[3], JSON.stringify(proof, null, 2) + "\n");
console.log(`${assertions} actual server-policy service checks passed with the Lust executable, including fail-closed selection and process protocol failures.`);
