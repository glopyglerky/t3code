import * as Contracts from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Schema from "effect/Schema";

export const fixedTime = "2026-11-10T12:00:00.000Z";
export const fixedDateTime = DateTime.makeUnsafe(fixedTime);
export const fixtureIds = Object.freeze({
  thread: "thread:main", run: "run:main", attempt: "attempt:main", providerThread: "provider:main",
  providerSession: "session:main", providerInstance: "codex", providerTurn: "turn:main",
  node: "node:main", request: "request:main", project: "project:main", message: "message:main",
  turnItem: "item:main", subagent: "subagent:main", plan: "plan:main", checkpointScope: "scope:main",
  checkpoint: "checkpoint:main", contextHandoff: "handoff:main", contextTransfer: "transfer:main",
});
const id = fixtureIds;
const modelSelection = { instanceId: id.providerInstance, model: "fixture-model" };
const nativeThreadRef = { driver: "codex", nativeId: "native:main", strength: "strong" };
const capabilities = {
  sessions: {
    supportsMultipleProviderThreadsPerSession: false, supportsModelSwitchInSession: false,
    supportsProviderSwitchingViaHandoff: false, supportsRuntimeModeSwitchInSession: false,
    pendingRequestsSurviveRestart: false,
  },
  threads: {
    canCreateEmptyThread: false, canReadThreadSnapshot: false, canRollbackThread: false,
    canForkThread: false, canForkFromTurn: false, canForkFromSubagentThread: false,
    exposesNativeThreadId: true,
  },
  turns: {
    exposesNativeTurnId: true, emitsTurnStarted: true, emitsTurnCompleted: true,
    supportsInterrupt: true, supportsActiveSteering: false, supportsSteeringByInterruptRestart: false,
    supportsQueuedMessages: false, terminalStatusQuality: "strong",
  },
  streaming: {
    streamsAssistantText: true, streamsReasoning: true, streamsToolOutput: false,
    streamsPlanText: false, emitsMessageCompleted: true,
  },
  tools: {
    exposesToolItemIds: false, emitsToolStarted: false, emitsToolCompleted: false,
    emitsToolOutput: false, supportsMcpTools: false, supportsDynamicToolCallbacks: false,
  },
  approvals: {
    supportsCommandApproval: false, supportsFileReadApproval: false, supportsFileChangeApproval: false,
    supportsApplyPatchApproval: false, approvalsHaveNativeRequestIds: false,
    approvalCallbacksAreLiveOnly: false, approvalsCanOriginateFromSubagents: false,
  },
  planning: {
    emitsPlanUpdated: true, emitsTodoList: true, emitsProposedPlan: true,
    supportsStructuredQuestions: true, planDeltasHaveItemIds: false,
  },
  subagents: {
    supportsSubagents: true, exposesSubagentThreadIds: false, emitsSubagentLifecycle: true,
    canWaitForSubagents: false, canCloseSubagents: false, canForkSubagentThread: false,
  },
  context: {
    acceptsSystemContext: true, acceptsDeveloperContext: false, acceptsSyntheticUserContext: false,
    canGenerateSummaries: false, canConsumeHandoffSummaries: false, supportsDeltaHandoff: false,
    supportsFullThreadHandoff: false, maxRecommendedHandoffChars: null,
  },
  checkpointing: {
    appCanCheckpointFilesystem: true, supportsNestedCheckpointScopes: false,
    providerCanRollbackConversation: false, providerRollbackReturnsSnapshot: false,
    providerCanReadConversationSnapshot: false,
  },
  identity: { nativeThreadIds: "strong", nativeTurnIds: "strong", nativeItemIds: "none", nativeRequestIds: "none" },
  runtimePolicy: { enforcement: "client-boundary" },
};

const inputs = {
  thread: {
    id: id.thread, projectId: id.project, title: "Native EventSink fixture", createdBy: "user", creationSource: "web",
    providerInstanceId: id.providerInstance, modelSelection, runtimeMode: "full-access", interactionMode: "default",
    branch: null, worktreePath: null, linkedPullRequest: null, pullRequests: [], branchPullRequest: null,
    activeProviderThreadId: null, lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: id.thread },
    forkedFrom: null, createdAt: fixedTime, updatedAt: fixedTime, archivedAt: null, settledAt: null,
    lastVisitedAt: null, deletedAt: null,
  },
  run: {
    id: id.run, threadId: id.thread, ordinal: 1, providerInstanceId: id.providerInstance, modelSelection,
    providerThreadId: id.providerThread, userMessageId: id.message, rootNodeId: id.node,
    activeAttemptId: id.attempt, status: "running", requestedAt: fixedTime, startedAt: fixedTime,
    completedAt: null, checkpointId: null, contextHandoffId: null,
  },
  attempt: {
    id: id.attempt, runId: id.run, attemptOrdinal: 1, rootNodeId: id.node,
    providerInstanceId: id.providerInstance, providerThreadId: id.providerThread, providerTurnId: id.providerTurn,
    reason: "initial", status: "running", startedAt: fixedTime, completedAt: null,
  },
  node: {
    id: id.node, threadId: id.thread, runId: id.run, parentNodeId: null, rootNodeId: id.node,
    kind: "root_turn", status: "running", countsForRun: true, providerThreadId: id.providerThread,
    providerTurnId: id.providerTurn, nativeItemRef: null, runtimeRequestId: null,
    checkpointScopeId: id.checkpointScope, startedAt: fixedTime, completedAt: null,
  },
  subagent: {
    id: id.subagent, threadId: id.thread, runId: id.run, parentNodeId: id.node,
    origin: "provider_native", createdBy: "agent", driver: "codex", providerInstanceId: id.providerInstance,
    providerThreadId: id.providerThread, childThreadId: null, nativeTaskRef: null,
    prompt: "Inspect the fixture 🦊", title: "Fixture subagent", model: null, status: "running",
    progress: "Inspecting", result: null, startedAt: fixedTime, completedAt: null, updatedAt: fixedTime,
  },
  providerSession: {
    id: id.providerSession, driver: "codex", providerInstanceId: id.providerInstance, status: "ready",
    cwd: "/fixture/workspace", model: "fixture-model", capabilities, createdAt: fixedTime,
    updatedAt: fixedTime, lastError: null,
  },
  providerThread: {
    id: id.providerThread, driver: "codex", providerInstanceId: id.providerInstance,
    providerSessionId: id.providerSession, appThreadId: id.thread, ownerNodeId: id.node,
    nativeThreadRef, nativeConversationHeadRef: null, status: "active", firstRunOrdinal: 1,
    lastRunOrdinal: 1, handoffIds: [], forkedFrom: null, createdAt: fixedTime, updatedAt: fixedTime,
  },
  providerTurn: {
    id: id.providerTurn, providerThreadId: id.providerThread, nodeId: id.node, runAttemptId: id.attempt,
    nativeTurnRef: { driver: "codex", nativeId: "native-turn:main", strength: "strong" },
    ordinal: 1, status: "running", startedAt: fixedTime, completedAt: null,
  },
  request: {
    id: id.request, nodeId: id.node, providerTurnId: id.providerTurn, nativeRequestRef: null,
    kind: "user_input", status: "pending", responseCapability: { type: "live", providerSessionId: id.providerSession },
    createdAt: fixedTime, resolvedAt: null,
  },
  message: {
    id: id.message, threadId: id.thread, runId: id.run, nodeId: id.node, role: "assistant",
    createdBy: "agent", creationSource: "provider", text: "Fixture message 🦊", attachments: [],
    streaming: true, createdAt: fixedTime, updatedAt: fixedTime,
  },
  turnItem: {
    id: id.turnItem, type: "reasoning", threadId: id.thread, runId: id.run, nodeId: id.node,
    providerThreadId: id.providerThread, providerTurnId: id.providerTurn, nativeItemRef: null,
    parentItemId: null, ordinal: 1_000_001, status: "running", title: null, startedAt: fixedTime,
    completedAt: null, updatedAt: fixedTime, text: "Fixture reasoning", streaming: true,
  },
  plan: {
    id: id.plan, threadId: id.thread, runId: id.run, nodeId: id.node, status: "draft",
    kind: "proposed_plan", markdown: "# Fixture plan\n\nVerify atomic persistence.",
  },
  checkpointScope: {
    id: id.checkpointScope, threadId: id.thread, runId: id.run, nodeId: id.node, parentScopeId: null,
    providerThreadId: id.providerThread, kind: "root_run", ordinalWithinParent: 0,
    advancesAppRunCount: true, cwd: "/fixture/workspace", createdAt: fixedTime,
  },
  checkpoint: {
    id: id.checkpoint, threadId: id.thread, scopeId: id.checkpointScope, runId: id.run, nodeId: id.node,
    parentCheckpointId: null, ordinalWithinScope: 1, appRunOrdinal: 1, ref: "checkpoint-ref:main", status: "ready",
    files: [{ path: "fixture.txt", kind: "modified", additions: 1, deletions: 0 }], capturedAt: fixedTime,
  },
  contextHandoff: {
    id: id.contextHandoff, threadId: id.thread, targetRunId: id.run,
    fromProviderThreadIds: ["provider:source"], toProviderThreadId: id.providerThread,
    coveredRunOrdinals: { from: 1, to: 1 }, strategy: "manual_context", status: "ready",
    summaryMessageId: null, summaryText: "Fixture handoff", createdByProviderInstanceId: null,
    createdAt: fixedTime, updatedAt: fixedTime,
  },
  contextTransfer: {
    id: id.contextTransfer, type: "provider_handoff", sourceThreadId: id.thread, targetThreadId: id.thread,
    sourcePoint: { threadId: id.thread, runId: id.run }, basePoint: null,
    sourceProviderInstanceId: id.providerInstance, targetProviderInstanceId: id.providerInstance,
    targetRunId: id.run, status: "resolved_portable",
    resolution: { strategy: "portable_context", contextHandoffId: id.contextHandoff },
    createdBy: "user", error: null, createdAt: fixedTime, updatedAt: fixedTime, consumedAt: null,
  },
};

const families = [
  ["thread", "OrchestrationV2AppThreadJson", "thread.created"],
  ["run", "OrchestrationV2RunJson", "run.created"],
  ["attempt", "OrchestrationV2RunAttemptJson", "run-attempt.created"],
  ["node", "OrchestrationV2ExecutionNodeJson", "node.updated"],
  ["subagent", "OrchestrationV2SubagentJson", "subagent.updated"],
  ["providerSession", "OrchestrationV2ProviderSessionJson", "provider-session.attached"],
  ["providerThread", "OrchestrationV2ProviderThreadJson", "provider-thread.updated"],
  ["providerTurn", "OrchestrationV2ProviderTurnJson", "provider-turn.updated"],
  ["request", "OrchestrationV2RuntimeRequestJson", "runtime-request.updated"],
  ["message", "OrchestrationV2ConversationMessageJson", "message.updated"],
  ["turnItem", "OrchestrationV2TurnItemJson", "turn-item.updated"],
  ["plan", "OrchestrationV2PlanArtifact", "plan.updated"],
  ["checkpointScope", "OrchestrationV2CheckpointScopeJson", "checkpoint-scope.created"],
  ["checkpoint", "OrchestrationV2CheckpointJson", "checkpoint.captured"],
  ["contextHandoff", "OrchestrationV2ContextHandoffJson", "context-handoff.updated"],
  ["contextTransfer", "OrchestrationV2ContextTransferJson", "context-transfer.created"],
];

/** Real contract decoding supplies defaults and validates every fixture at module load. */
export const payloads = Object.fromEntries(families.map(([family, contract]) => [
  family, Schema.decodeUnknownSync(Contracts[contract])(inputs[family]),
]));
export const jsonPayloads = Object.fromEntries(families.map(([family, contract]) => [
  family, Schema.encodeSync(Contracts[contract])(payloads[family]),
]));

/** Accept JSON payloads and JSON envelope overrides; return a decoded domain event. */
export function makeEvent(type, payload, overrides = {}) {
  return Schema.decodeUnknownSync(Contracts.OrchestrationV2DomainEventJson)({
    id: `event:${type}:${payload.id ?? "main"}`, threadId: id.thread, occurredAt: fixedTime,
    type, payload, ...overrides,
  });
}

export const sampleEvents = families.map(([family, , type]) => makeEvent(type, jsonPayloads[family]));
export const sampleJsonEvents = sampleEvents.map(Schema.encodeSync(Contracts.OrchestrationV2DomainEventJson));
export const pullRequestLinkUpdate = makeEvent("thread.pull-request-link-updated", {
  host: "github.com", repository: "fixture/repository", number: 1,
  url: "https://github.com/fixture/repository/pull/1", source: "manual", linkedAt: fixedTime,
  snapshot: { state: "open", title: "Fixture pull request", headBranch: "fixture", baseBranch: "main", isDraft: false, updatedAt: fixedTime, syncedAt: fixedTime },
  stack: null,
});
