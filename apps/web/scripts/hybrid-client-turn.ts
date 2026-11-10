// @effect-diagnostics nodeBuiltinImport:off - assertions shared by the application acceptance runner and its offline regression.
import * as Assert from "node:assert/strict";
import {
  latestProviderTurnForAttempt,
  type OrchestrationV2ProviderTurn,
  type OrchestrationV2RunAttempt,
  type OrchestrationV2ThreadDetailSnapshot,
} from "@t3tools/contracts";

export type ReplayTurnIdentity = { runId: string; attemptId: string; providerTurnId: string };

export function assertReplayTurnLinks(
  attempt: OrchestrationV2RunAttempt,
  providerTurn: OrchestrationV2ProviderTurn,
): void {
  Assert.equal(providerTurn.runAttemptId, attempt.id);
  Assert.equal(providerTurn.providerThreadId, attempt.providerThreadId);
  Assert.equal(providerTurn.nodeId, attempt.rootNodeId);
  // ACP publishes the authoritative inverse link on the provider turn. The attempt's
  // optional reverse link may remain null while running and after completion.
  if (attempt.providerTurnId !== null) Assert.equal(attempt.providerTurnId, providerTurn.id);
}

export function replayTurnIdentity(
  snapshot: OrchestrationV2ThreadDetailSnapshot,
): ReplayTurnIdentity {
  Assert.equal(
    snapshot.projection.runs.length,
    1,
    "The controlled fixture must have exactly one run",
  );
  const run = snapshot.projection.runs[0]!;
  Assert.equal(run.providerInstanceId, "devReplay");
  Assert.equal(run.modelSelection.model, "showcase");
  Assert.ok(run.activeAttemptId, "The replay approval must belong to an active attempt");
  const attempt = snapshot.projection.attempts.find((item) => item.id === run.activeAttemptId);
  Assert.ok(attempt);
  Assert.equal(attempt.runId, run.id);
  Assert.equal(attempt.providerInstanceId, "devReplay");
  Assert.equal(attempt.providerThreadId, run.providerThreadId);
  Assert.equal(attempt.rootNodeId, run.rootNodeId);
  const providerTurn = latestProviderTurnForAttempt(snapshot.projection.providerTurns, attempt.id);
  Assert.ok(providerTurn, "The replay approval must have a provider turn linked to its attempt");
  assertReplayTurnLinks(attempt, providerTurn);
  return { runId: run.id, attemptId: attempt.id, providerTurnId: providerTurn.id };
}
