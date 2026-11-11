import * as NodeAssert from "node:assert/strict";
import * as NodeChildProcess from "node:child_process";

/** An argument-only rejection may close stdin before spawnSync finishes writing it. */
export function assertArgumentRejection(result, args, expectedStderr) {
  NodeAssert.ok(args.length > 0, "Argument rejection requires an unexpected argument");
  if (result.error?.code !== "EPIPE") NodeAssert.ifError(result.error);
  NodeAssert.equal(result.status, 2, "Unexpected arguments must exit with status 2");
  NodeAssert.equal(result.signal, null, "Argument rejection must not terminate by signal");
  NodeAssert.equal(result.stdout, "", "Argument rejection must not expose a partial response");
  NodeAssert.equal(result.stderr, expectedStderr, "Argument rejection must report the exact error");
}

export function checkArgumentRejection(executable, args, input, expectedStderr) {
  NodeAssert.ok(args.length > 0, "Argument rejection requires an unexpected argument");
  const result = NodeChildProcess.spawnSync(executable, args, {
    input,
    env: {},
    encoding: "utf8",
    maxBuffer: 64 * 1024,
    timeout: 30_000,
  });
  assertArgumentRejection(result, args, expectedStderr);
  return result;
}
