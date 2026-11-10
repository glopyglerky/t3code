// @effect-diagnostics nodeBuiltinImport:off - offline regression for the acceptance runner's contract assertions.
import * as Assert from "node:assert/strict";
import { test } from "node:test";
import {
  MessageId,
  NodeId,
  ProviderInstanceId,
  ProviderThreadId,
  ProviderTurnId,
  RunAttemptId,
  RunId,
  type OrchestrationV2ThreadDetailSnapshot,
} from "@t3tools/contracts";
import {
  v2Now,
  v2Projection,
} from "../../../packages/client-runtime/src/state/orchestrationV2TestFixtures.ts";
import { assertReplayTurnLinks, replayTurnIdentity } from "./hybrid-client-turn.ts";

const runId = RunId.make("replay-run");
const attemptId = RunAttemptId.make("replay-attempt");
const providerTurnId = ProviderTurnId.make("replay-turn");
const providerThreadId = ProviderThreadId.make("replay-provider-thread");
const rootNodeId = NodeId.make("replay-root");
const providerInstanceId = ProviderInstanceId.make("devReplay");
const modelSelection = { instanceId: providerInstanceId, model: "showcase" };
const expected = { runId, attemptId, providerTurnId };

function fixture(): OrchestrationV2ThreadDetailSnapshot {
  return {
    snapshotSequence: 1,
    projection: {
      ...v2Projection,
      thread: { ...v2Projection.thread, providerInstanceId, modelSelection },
      runs: [
        {
          id: runId,
          threadId: v2Projection.thread.id,
          ordinal: 1,
          providerInstanceId,
          modelSelection,
          providerThreadId,
          userMessageId: MessageId.make("replay-message"),
          rootNodeId,
          activeAttemptId: attemptId,
          status: "running",
          requestedAt: v2Now,
          startedAt: v2Now,
          completedAt: null,
          checkpointId: null,
          contextHandoffId: null,
        },
      ],
      attempts: [
        {
          id: attemptId,
          runId,
          attemptOrdinal: 1,
          rootNodeId,
          providerInstanceId,
          providerThreadId,
          providerTurnId: null,
          reason: "initial",
          status: "running",
          startedAt: v2Now,
          completedAt: null,
        },
      ],
      providerTurns: [
        {
          id: providerTurnId,
          providerThreadId,
          nodeId: rootNodeId,
          runAttemptId: attemptId,
          nativeTurnRef: null,
          ordinal: 1,
          status: "running",
          startedAt: v2Now,
          completedAt: null,
        },
      ],
    },
  };
}

test("finds the ACP replay turn through its inverse link when the attempt's reverse link is null", () => {
  Assert.deepEqual(replayTurnIdentity(fixture()), expected);
});

test("keeps the same identity when completion leaves the attempt's reverse link null", () => {
  const snapshot = fixture();
  const completed: OrchestrationV2ThreadDetailSnapshot = {
    ...snapshot,
    projection: {
      ...snapshot.projection,
      runs: snapshot.projection.runs.map((run) => ({
        ...run,
        status: "completed",
        completedAt: v2Now,
      })),
      attempts: snapshot.projection.attempts.map((attempt) => ({
        ...attempt,
        status: "completed",
        completedAt: v2Now,
      })),
      providerTurns: snapshot.projection.providerTurns.map((turn) => ({
        ...turn,
        status: "completed",
        completedAt: v2Now,
      })),
    },
  };
  Assert.deepEqual(replayTurnIdentity(completed), expected);
  Assert.doesNotThrow(() =>
    assertReplayTurnLinks(
      completed.projection.attempts[0]!,
      completed.projection.providerTurns[0]!,
    ),
  );
});

test("selects the latest turn for this attempt and ignores a newer turn for another attempt", () => {
  const snapshot = fixture();
  const turn = snapshot.projection.providerTurns[0]!;
  const latestId = ProviderTurnId.make("replay-latest-turn");
  const projection = {
    ...snapshot.projection,
    providerTurns: [
      {
        ...turn,
        id: ProviderTurnId.make("unrelated-turn"),
        runAttemptId: RunAttemptId.make("another-attempt"),
        ordinal: 99,
      },
      { ...turn, id: latestId, ordinal: 2 },
      turn,
    ],
  };
  Assert.deepEqual(replayTurnIdentity({ ...snapshot, projection }), {
    ...expected,
    providerTurnId: latestId,
  });
});

test("rejects a reverse link without the authoritative attempt association", () => {
  const snapshot = fixture();
  const projection = {
    ...snapshot.projection,
    attempts: snapshot.projection.attempts.map((attempt) => ({ ...attempt, providerTurnId })),
    providerTurns: snapshot.projection.providerTurns.map((turn) => ({
      ...turn,
      runAttemptId: null,
    })),
  };
  Assert.throws(() => replayTurnIdentity({ ...snapshot, projection }), /linked to its attempt/);
});

test("rejects an inconsistent reverse link when one is present", () => {
  const snapshot = fixture();
  const projection = {
    ...snapshot.projection,
    attempts: snapshot.projection.attempts.map((attempt) => ({
      ...attempt,
      providerTurnId: ProviderTurnId.make("wrong-turn"),
    })),
  };
  Assert.throws(() => replayTurnIdentity({ ...snapshot, projection }));
});

test("rejects provider turns attached to a different provider thread or root node", () => {
  const snapshot = fixture();
  const turn = snapshot.projection.providerTurns[0]!;
  for (const wrongTurn of [
    { ...turn, providerThreadId: ProviderThreadId.make("wrong-thread") },
    { ...turn, nodeId: NodeId.make("wrong-node") },
  ]) {
    const projection = { ...snapshot.projection, providerTurns: [wrongTurn] };
    Assert.throws(() => replayTurnIdentity({ ...snapshot, projection }));
  }
});
