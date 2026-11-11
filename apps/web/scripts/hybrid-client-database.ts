// @effect-diagnostics nodeBuiltinImport:off - isolated Node acceptance assertions, never application runtime.
import * as Assert from "node:assert/strict";
import * as ChildProcess from "node:child_process";
import * as Crypto from "node:crypto";
import * as FS from "node:fs";
import * as Path from "node:path";
import type { ReplayTurnIdentity } from "./hybrid-client-turn.ts";

export interface PersistedReplayExpectation extends ReplayTurnIdentity {
  readonly projectId: string;
  readonly threadId: string;
  readonly assistantMessageId: string;
  readonly snapshotSequence: number;
  readonly runCompletedAt: string;
  readonly attemptCompletedAt: string;
  readonly providerTurnCompletedAt: string;
}

const tables = [
  "projection_projects",
  "orchestration_v2_projection_threads",
  "orchestration_v2_projection_runs",
  "orchestration_v2_projection_run_attempts",
  "orchestration_v2_projection_provider_turns",
  "orchestration_v2_projection_messages",
] as const;
const transactionId = "hybrid-client-read-proof";

function object(value: unknown, label: string): Record<string, unknown> {
  Assert.ok(typeof value === "object" && value !== null && !Array.isArray(value), label);
  return value as Record<string, unknown>;
}

function text(row: Record<string, unknown>, column: string): string {
  const cell = object(row[column], `Missing persisted cell ${column}`);
  Assert.equal(cell.type, "text", `Persisted ${column} must be text`);
  Assert.equal(typeof cell.value, "string");
  return cell.value as string;
}

function payload(row: Record<string, unknown>): Record<string, unknown> {
  return object(JSON.parse(text(row, "payload_json")), "Persisted payload must be an object");
}

export function nativeReplayRequests(expected: PersistedReplayExpectation) {
  const ids = [
    expected.projectId,
    expected.threadId,
    expected.runId,
    expected.attemptId,
    expected.providerTurnId,
    expected.assistantMessageId,
  ];
  return [
    { operation: "application_schema" },
    { operation: "application_begin", transaction_id: transactionId, mode: "read" },
    ...tables.map((table, index) => ({
      operation: "application_get",
      transaction_id: transactionId,
      table,
      key: [{ type: "text", value: ids[index] }],
    })),
    { operation: "application_events_latest", transaction_id: transactionId, kind: "application" },
    {
      operation: "application_events_latest",
      transaction_id: transactionId,
      kind: "agent",
      thread_id: expected.threadId,
    },
    { operation: "application_rollback", transaction_id: transactionId },
  ];
}

/** Assert durable canonical rows, not browser caches or a reconstructed response. */
export function assertNativeReplayResponses(
  responses: readonly unknown[],
  expected: PersistedReplayExpectation,
) {
  Assert.equal(
    responses.length,
    nativeReplayRequests(expected).length,
    "Every read-only request must reply",
  );
  const results = responses.map((response, index) => {
    const envelope = object(response, `Missing native response ${index}`);
    Assert.equal(
      envelope.ok,
      true,
      `Native read-only inspection rejected request ${index}: ${JSON.stringify(envelope.error)}`,
    );
    return envelope.result;
  });
  const schema = object(results[0], "Native application schema is missing");
  Assert.equal(schema.version, 1);
  Assert.ok(Array.isArray(schema.tables));
  const tableNames = schema.tables.map((entry) => object(entry, "Invalid table schema").name);
  for (const table of tables)
    Assert.ok(tableNames.includes(table), `Missing native table ${table}`);
  const [project, thread, run, attempt, turn, message] = results
    .slice(2, 8)
    .map((row) => object(row, "Durable fixture row is missing"));
  Assert.ok(project && thread && run && attempt && turn && message);
  Assert.equal(text(project, "project_id"), expected.projectId);
  Assert.equal(text(thread, "thread_id"), expected.threadId);
  Assert.equal(text(thread, "project_id"), expected.projectId);
  Assert.equal(payload(thread).id, expected.threadId);
  for (const [row, idColumn, id, completedAt] of [
    [run, "run_id", expected.runId, expected.runCompletedAt],
    [attempt, "attempt_id", expected.attemptId, expected.attemptCompletedAt],
    [turn, "provider_turn_id", expected.providerTurnId, expected.providerTurnCompletedAt],
  ] as const) {
    Assert.equal(text(row, idColumn), id);
    Assert.equal(text(row, "thread_id"), expected.threadId);
    Assert.equal(text(row, "status"), "completed");
    Assert.equal(payload(row).id, id);
    Assert.equal(payload(row).status, "completed");
    Assert.equal(payload(row).completedAt, completedAt);
  }
  const persistedRun = payload(run),
    persistedAttempt = payload(attempt),
    persistedTurn = payload(turn);
  Assert.equal(persistedAttempt.runId, expected.runId);
  Assert.equal(persistedAttempt.providerThreadId, persistedRun.providerThreadId);
  Assert.equal(persistedAttempt.rootNodeId, persistedRun.rootNodeId);
  Assert.equal(persistedTurn.runAttemptId, expected.attemptId);
  Assert.equal(persistedTurn.providerThreadId, persistedRun.providerThreadId);
  Assert.equal(persistedTurn.nodeId, persistedRun.rootNodeId);
  if (persistedAttempt.providerTurnId !== null)
    Assert.equal(persistedAttempt.providerTurnId, expected.providerTurnId);
  const persistedMessage = payload(message);
  Assert.equal(text(message, "message_id"), expected.assistantMessageId);
  Assert.equal(text(message, "thread_id"), expected.threadId);
  Assert.equal(text(message, "role"), "assistant");
  Assert.equal(persistedMessage.id, expected.assistantMessageId);
  Assert.equal(persistedMessage.runId, expected.runId);
  Assert.equal(persistedMessage.streaming, false);
  Assert.equal(typeof persistedMessage.text, "string");
  Assert.ok((persistedMessage.text as string).includes("This turn is a replay."));
  const latestApplicationSequence = results[8],
    latestThreadSequence = results[9];
  Assert.ok(
    typeof latestApplicationSequence === "number" &&
      Number.isSafeInteger(latestApplicationSequence),
  );
  Assert.ok(typeof latestThreadSequence === "number" && Number.isSafeInteger(latestThreadSequence));
  Assert.ok(
    latestApplicationSequence >= expected.snapshotSequence,
    "Stored event head must include the acknowledged browser snapshot",
  );
  Assert.ok(latestThreadSequence > 0 && latestThreadSequence <= latestApplicationSequence);
  return {
    latestApplicationSequence,
    latestThreadSequence,
    projectId: expected.projectId,
    threadId: expected.threadId,
    runId: expected.runId,
    attemptId: expected.attemptId,
    providerTurnId: expected.providerTurnId,
    assistantMessageId: expected.assistantMessageId,
  };
}

export function assertNoApplicationSqlite(stateDirectory: string): void {
  const pending = [stateDirectory];
  while (pending.length > 0) {
    const directory = pending.pop()!;
    for (const entry of FS.readdirSync(directory, { withFileTypes: true })) {
      Assert.ok(
        !/^state(?:v2)?\.sqlite(?:-(?:wal|shm|journal))?$/u.test(entry.name),
        `Native acceptance created an application SQLite file: ${Path.join(directory, entry.name)}`,
      );
      // Never follow symlinks outside the owned fixture state.
      if (entry.isDirectory()) pending.push(Path.join(directory, entry.name));
    }
  }
}

export function inspectNativeReplay(input: {
  readonly executable: string;
  readonly stateDirectory: string;
  readonly expected: PersistedReplayExpectation;
  readonly closed?: boolean;
}) {
  Assert.ok(Path.isAbsolute(input.executable));
  assertNoApplicationSqlite(input.stateDirectory);
  const databasePath = Path.join(input.stateDirectory, "userdata", "statev2.omdb");
  Assert.ok(
    FS.lstatSync(databasePath).isFile(),
    "Native application database must be a regular file",
  );
  const hash = () =>
    Crypto.createHash("sha256").update(FS.readFileSync(databasePath)).digest("hex");
  const before = input.closed ? hash() : null;
  const requests = nativeReplayRequests(input.expected);
  const result = ChildProcess.spawnSync(input.executable, ["read-only-local", databasePath], {
    input: requests.map((request) => JSON.stringify(request)).join("\n") + "\n",
    encoding: "utf8",
    env: {},
    timeout: 30_000,
    maxBuffer: 32 * 1024 * 1024,
  });
  Assert.ifError(result.error);
  Assert.equal(result.status, 0, `Read-only native inspection failed: ${result.stderr}`);
  const persisted = assertNativeReplayResponses(
    result.stdout
      .trim()
      .split("\n")
      .map((line) => JSON.parse(line)),
    input.expected,
  );
  if (before !== null)
    Assert.equal(hash(), before, "Read-only inspection must not mutate the closed database");
  assertNoApplicationSqlite(input.stateDirectory);
  return {
    engine: "omdb" as const,
    inspector: "read-only-local" as const,
    databasePath,
    databaseBytes: FS.statSync(databasePath).size,
    closedDatabaseUnchanged: before === null ? null : true,
    ...persisted,
  };
}
