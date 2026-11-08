import type { OrchestrationV2Run, OrchestrationV2ThreadProjection, RunId } from "@t3tools/contracts";
import type { resolveMessageDispatchIntent } from "./CommandPolicy.ts";

type Arguments = Parameters<typeof resolveMessageDispatchIntent>;
type Mode = ReturnType<typeof resolveMessageDispatchIntent>;

function isActive(run: OrchestrationV2Run): boolean {
  return ["preparing", "starting", "running", "waiting"].includes(run.status);
}

function status(run: OrchestrationV2Run | undefined): string {
  return run === undefined ? "-" : isActive(run) ? run.status : "inactive";
}

function capabilities(projection: OrchestrationV2ThreadProjection, run: OrchestrationV2Run | undefined): string {
  if (run === undefined) return "-";
  const thread = projection.providerThreads.find((candidate) => candidate.id === run.providerThreadId);
  const session = thread?.providerSessionId == null ? undefined :
    projection.providerSessions.find((candidate) => candidate.id === thread.providerSessionId);
  const turns = session?.capabilities.turns;
  if (turns === undefined) return "-";
  return String([
    turns.supportsQueuedMessages,
    turns.supportsActiveSteering,
    turns.supportsInterrupt,
    turns.supportsSteeringByInterruptRestart,
  ].reduce((mask, enabled, bit) => mask | (enabled ? 1 << bit : 0), 0));
}

function maintenance(projection: OrchestrationV2ThreadProjection, run: OrchestrationV2Run | undefined): string {
  const message = projection.messages.find((candidate) => candidate.id === run?.userMessageId);
  return message !== undefined && message.attachments.length === 0 &&
    ["/compact", "/logout"].includes(message.text.trim().toLowerCase()) ? "1" : "0";
}

export function dispatchFacts(projection: Arguments[0], requested: Arguments[1], intent: Arguments[2]) {
  const active = projection.runs.findLast(isActive);
  const requestedTarget = requested.type === "steer_active"
    ? projection.runs.find((run) => run.id === requested.targetRunId) : undefined;
  // Upstream normalizes against find(), even though automatic targeting uses findLast().
  const activeTarget = active === undefined ? undefined : projection.runs.find((run) => run.id === active.id);
  return {
    activeRunId: active?.id,
    args: [
      "t3ls-dispatch-v1", requested.type === "steer_active" ? "steer_active" : "other", intent ?? "-",
      status(active), capabilities(projection, active),
      active !== undefined && projection.providerTurns.some((turn) =>
        turn.runAttemptId === active.activeAttemptId && turn.status === "running") ? "1" : "0",
      status(requestedTarget), capabilities(projection, requestedTarget), maintenance(projection, requestedTarget),
      status(activeTarget), capabilities(projection, activeTarget), maintenance(projection, activeTarget),
    ],
  };
}

export function dispatchResult(value: string, requested: Mode, activeRunId: RunId | undefined): Mode {
  if (value === "requested") return requested;
  if (value === "start_immediately" || value === "queue_after_active") return { type: value };
  if ((value === "steer_active" || value === "restart_active") && activeRunId !== undefined) {
    return { type: value, targetRunId: activeRunId };
  }
  throw new Error("Native dispatch policy returned an invalid target or decision");
}
