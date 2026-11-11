// @effect-diagnostics nodeBuiltinImport:off - offline acceptance assertion regressions.
import * as Assert from "node:assert/strict";
import * as FS from "node:fs";
import * as OS from "node:os";
import * as Path from "node:path";
import { test } from "node:test";
import {
  assertNativeReplayResponses,
  assertNoApplicationSqlite,
  nativeReplayRequests,
  type PersistedReplayExpectation,
} from "./hybrid-client-database.ts";

const at = "2026-11-10T12:00:00.000Z";
const expected: PersistedReplayExpectation = {
  projectId: "project",
  threadId: "thread",
  runId: "run",
  attemptId: "attempt",
  providerTurnId: "turn",
  assistantMessageId: "message",
  snapshotSequence: 10,
  runCompletedAt: at,
  attemptCompletedAt: at,
  providerTurnCompletedAt: at,
};
const cell = (value: string) => ({ type: "text", value });
const row = (idColumn: string, id: string, payload: Record<string, unknown>, extra = {}) => ({
  [idColumn]: cell(id),
  thread_id: cell(expected.threadId),
  status: cell(String(payload.status ?? "")),
  payload_json: cell(JSON.stringify(payload)),
  ...extra,
});

function responses(
  options: {
    runStatus?: string;
    turnAttempt?: string;
    messageText?: string;
    reverseLink?: string | null;
  } = {},
): unknown[] {
  const rows = [
    { project_id: cell(expected.projectId) },
    row(
      "thread_id",
      expected.threadId,
      { id: expected.threadId },
      { project_id: cell(expected.projectId) },
    ),
    row("run_id", expected.runId, {
      id: expected.runId,
      status: options.runStatus ?? "completed",
      completedAt: at,
      providerThreadId: "provider",
      rootNodeId: "node",
    }),
    row("attempt_id", expected.attemptId, {
      id: expected.attemptId,
      status: "completed",
      completedAt: at,
      runId: expected.runId,
      providerThreadId: "provider",
      rootNodeId: "node",
      providerTurnId: options.reverseLink ?? null,
    }),
    row("provider_turn_id", expected.providerTurnId, {
      id: expected.providerTurnId,
      status: "completed",
      completedAt: at,
      runAttemptId: options.turnAttempt ?? expected.attemptId,
      providerThreadId: "provider",
      nodeId: "node",
    }),
    row(
      "message_id",
      expected.assistantMessageId,
      {
        id: expected.assistantMessageId,
        runId: expected.runId,
        streaming: false,
        text: options.messageText ?? "This turn is a replay.",
      },
      { role: cell("assistant") },
    ),
  ];
  return [
    {
      version: 1,
      tables: [
        "projection_projects",
        "orchestration_v2_projection_threads",
        "orchestration_v2_projection_runs",
        "orchestration_v2_projection_run_attempts",
        "orchestration_v2_projection_provider_turns",
        "orchestration_v2_projection_messages",
      ].map((name) => ({ name })),
    },
    { transaction_id: "hybrid-client-read-proof" },
    ...rows,
    12,
    11,
    { rolled_back: true },
  ].map((result) => ({ ok: true, result }));
}

test("native persistence inspection uses one read transaction, exact row keys, and rollback only", () => {
  const requests = nativeReplayRequests(expected);
  Assert.equal(requests.length, 11);
  Assert.deepEqual(requests[1], {
    operation: "application_begin",
    transaction_id: "hybrid-client-read-proof",
    mode: "read",
  });
  Assert.deepEqual(
    requests.slice(2, 8).map((request) => ("key" in request ? request.key : undefined)),
    ["project", "thread", "run", "attempt", "turn", "message"].map((value) => [
      { type: "text", value },
    ]),
  );
  Assert.deepEqual(
    requests.map((request) => request.operation),
    [
      "application_schema",
      "application_begin",
      ...Array.from({ length: 6 }, () => "application_get"),
      "application_events_latest",
      "application_events_latest",
      "application_rollback",
    ],
  );
});

test("canonical rows preserve the inverse replay link when the optional reverse link is null", () => {
  const proof = assertNativeReplayResponses(responses(), expected);
  Assert.equal(proof.threadId, expected.threadId);
  Assert.equal(proof.latestThreadSequence, 11);
  Assert.equal(proof.latestApplicationSequence, 12);
  Assert.doesNotThrow(() =>
    assertNativeReplayResponses(responses({ reverseLink: expected.providerTurnId }), expected),
  );
});

test("native persistence proof rejects missing rows, incomplete turns, wrong links, stale heads and helper failures", () => {
  for (const values of [
    responses({ runStatus: "running" }),
    responses({ turnAttempt: "wrong" }),
    responses({ reverseLink: "wrong" }),
    responses({ messageText: "different turn" }),
  ])
    Assert.throws(() => assertNativeReplayResponses(values, expected));
  const missing = responses();
  missing[3] = { ok: true, result: null };
  Assert.throws(() => assertNativeReplayResponses(missing, expected));
  const stale = responses();
  stale[8] = { ok: true, result: 9 };
  Assert.throws(() => assertNativeReplayResponses(stale, expected));
  const failed = responses();
  failed[2] = { ok: false, error: { kind: "incompatible_store" } };
  Assert.throws(() => assertNativeReplayResponses(failed, expected));
  Assert.throws(() => assertNativeReplayResponses(responses().slice(1), expected));
});

test("native state rejects SQLite databases and sidecars but permits the native ownership marker", () => {
  const directory = FS.mkdtempSync(Path.join(OS.tmpdir(), "t3-client-persistence-test-"));
  try {
    const state = Path.join(directory, "userdata");
    FS.mkdirSync(state);
    FS.writeFileSync(Path.join(state, "server-owner.sqlite"), "t3code-omdb-owner-v1\n");
    Assert.doesNotThrow(() => assertNoApplicationSqlite(directory));
    for (const name of [
      "state.sqlite",
      "statev2.sqlite",
      "statev2.sqlite-wal",
      "state.sqlite-shm",
    ]) {
      const target = Path.join(state, name);
      FS.writeFileSync(target, "fixture");
      Assert.throws(() => assertNoApplicationSqlite(directory));
      FS.unlinkSync(target);
    }
  } finally {
    FS.rmSync(directory, { recursive: true, force: true });
  }
});
