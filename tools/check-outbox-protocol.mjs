#!/usr/bin/env node
import * as NodeAssert from "node:assert/strict";
import * as NodeChildProcess from "node:child_process";
import * as NodePath from "node:path";

const executable = process.argv[2];
NodeAssert.ok(
  executable && NodePath.isAbsolute(executable),
  "Pass an absolute outbox executable path",
);
const now = "2026-11-09T01:00:00.000Z";
const row = {
  id: "effect:first",
  threadId: "thread:first",
  effectType: "provider-turn.start",
  status: "pending",
  insertionOrdinal: 1,
  attemptCount: 0,
  availableAt: "2026-11-09T00:00:00.000Z",
  leaseOwner: null,
  createdAt: "2026-11-09T00:00:00.000Z",
  updatedAt: "2026-11-09T00:00:00.000Z",
  completedAt: null,
  lastError: null,
};
const request = {
  version: 1,
  operation: "claim",
  rows: [row],
  now,
  workerId: "worker:first",
  excludeRestartContinuations: false,
};
const frame = (value) => `${JSON.stringify(value)}\n`;
let checks = 0;
function execute(input, args = []) {
  const result = NodeChildProcess.spawnSync(executable, args, {
    input,
    env: {},
    encoding: "utf8",
    maxBuffer: 20 * 1024 * 1024,
    timeout: 15_000,
  });
  NodeAssert.ifError(result.error);
  NodeAssert.equal(result.signal, null);
  return result;
}
function rejected(label, input, args = []) {
  const result = execute(input, args);
  NodeAssert.equal(result.status, 2, label);
  NodeAssert.equal(result.stdout, "", `${label}: partial response`);
  NodeAssert.match(result.stderr, /^t3-code-ls-outbox: [^\n]+\n$/, label);
  checks++;
}

const successful = execute(frame(request));
NodeAssert.equal(successful.status, 0);
NodeAssert.equal(successful.stderr, "");
NodeAssert.deepEqual(JSON.parse(successful.stdout), {
  version: 1,
  ok: {
    index: 0,
    row: {
      ...row,
      status: "running",
      attemptCount: 1,
      leaseOwner: request.workerId,
      updatedAt: now,
    },
  },
});
checks++;

for (const [label, input, args] of [
  ["empty", ""],
  ["missing final LF", JSON.stringify(request)],
  ["extra LF", `${frame(request)}\n`],
  ["two requests", frame(request) + frame(request)],
  ["CRLF", `${JSON.stringify(request)}\r\n`],
  ["unexpected args", frame(request), ["extra"]],
  ["invalid UTF-8", Buffer.from([0xff, 0x0a])],
  ["duplicate root key", frame(request).replace('"version":1', '"version":1,"version":1')],
  [
    "duplicate row key",
    frame(request).replace('"attemptCount":0', '"attemptCount":0,"attemptCount":0'),
  ],
  ["excessive nesting", `${"[".repeat(17)}0${"]".repeat(17)}\n`],
]) {
  rejected(label, input, args);
}

for (const [label, changed] of [
  ["wrong version", { version: 2 }],
  ["unknown operation", { operation: "expire-leases" }],
  ["unknown root field", { extra: true }],
  ["wrong rows type", { rows: {} }],
  ["wrong exclusion type", { excludeRestartContinuations: "false" }],
  ["empty worker", { workerId: "" }],
  ["NUL worker", { workerId: "a\0b" }],
  ["noncanonical date", { now: "2026-11-09T01:00:00Z" }],
  ["wrong offset", { now: "2026-11-09T01:00:00.000+00:00" }],
  ["invalid leap day", { now: "2026-02-29T01:00:00.000Z" }],
  ["invalid month day", { now: "2026-04-31T01:00:00.000Z" }],
  ["invalid time", { now: "2026-11-09T25:00:00.000Z" }],
  ["duplicate effect ID", { rows: [row, { ...row, insertionOrdinal: 2 }] }],
  ["duplicate insertion ordinal", { rows: [row, { ...row, id: "effect:other" }] }],
]) {
  rejected(label, frame({ ...request, ...changed }));
}

for (const [label, changed] of [
  ["unknown effect", { effectType: "future.effect" }],
  ["unknown status", { status: "expired" }],
  ["missing status", { status: undefined }],
  ["unexpected row field", { extra: true }],
  ["zero ordinal", { insertionOrdinal: 0 }],
  ["negative attempt", { attemptCount: -1 }],
  ["fractional attempt", { attemptCount: 0.5 }],
  ["unsafe attempt", { attemptCount: Number.MAX_SAFE_INTEGER + 1 }],
  ["attempt overflow", { attemptCount: Number.MAX_SAFE_INTEGER }],
  ["invalid optional date", { completedAt: "yesterday" }],
  ["invalid optional error", { lastError: false }],
  ["text limit", { lastError: "a".repeat(65_537) }],
]) {
  rejected(label, frame({ ...request, rows: [{ ...row, ...changed }] }));
}

rejected("input byte limit", `${" ".repeat(16 * 1024 * 1024)}\n`);
rejected(
  "zero prune limit",
  frame({ version: 1, operation: "prune", rows: [], cutoff: now, limit: 0 }),
);
rejected(
  "retry deadline precedes now",
  frame({
    version: 1,
    operation: "transition",
    rows: [row],
    action: "retry",
    now,
    effectId: row.id,
    workerId: "worker:first",
    error: "retry",
    availableAt: row.availableAt,
  }),
);
rejected(
  "output amplification limit",
  frame({
    version: 1,
    operation: "cancel",
    rows: Array.from({ length: 300 }, (_, index) => ({
      ...row,
      id: `effect:${index}`,
      insertionOrdinal: index + 1,
    })),
    now,
    threadId: row.threadId,
    effectTypes: [row.effectType],
    reason: "a".repeat(65_536),
  }),
);

console.log(`Outbox executable protocol: ${checks} checks passed, including input/output bounds.`);
