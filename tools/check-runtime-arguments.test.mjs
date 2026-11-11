import * as NodeAssert from "node:assert/strict";
import * as NodeTest from "node:test";
import { assertArgumentRejection } from "./check-runtime-arguments.mjs";

const args = ["unexpected"];
const expectedStderr = "t3-code-ls-fixture: invalid request\n";
const result = (fields = {}) => ({
  status: 2,
  signal: null,
  stdout: "",
  stderr: expectedStderr,
  ...fields,
});
const error = (code) => Object.assign(new Error(`spawnSync fixture ${code}`), { code });

NodeTest.test("argument rejection accepts a normal status-2 exit", () => {
  assertArgumentRejection(result(), args, expectedStderr);
});

NodeTest.test(
  "argument rejection permits only an early stdin EPIPE with the exact exit contract",
  () => {
    assertArgumentRejection(result({ error: error("EPIPE") }), args, expectedStderr);
  },
);

for (const code of ["ENOENT", "EACCES", "ETIMEDOUT", "ENOBUFS", "ECONNRESET", undefined]) {
  NodeTest.test(`argument rejection does not hide a ${code ?? "code-less"} spawn error`, () => {
    NodeAssert.throws(() =>
      assertArgumentRejection(result({ error: error(code) }), args, expectedStderr),
    );
  });
}

for (const fields of [
  { status: 0 },
  { status: 1 },
  { status: null },
  { signal: "SIGPIPE" },
  { stdout: "partial response\n" },
  { stderr: "t3-code-ls-fixture: wrong error\n" },
  { stderr: `${expectedStderr}extra output\n` },
]) {
  NodeTest.test(`EPIPE does not hide a rejected exit contract: ${JSON.stringify(fields)}`, () => {
    NodeAssert.throws(() =>
      assertArgumentRejection(result({ error: error("EPIPE"), ...fields }), args, expectedStderr),
    );
  });
}

NodeTest.test("successful execution cannot be accepted as argument rejection", () => {
  NodeAssert.throws(() => assertArgumentRejection(result({ status: 0 }), args, expectedStderr));
});

NodeTest.test("the EPIPE exception is unavailable without unexpected arguments", () => {
  NodeAssert.throws(() =>
    assertArgumentRejection(result({ error: error("EPIPE") }), [], expectedStderr),
  );
});
