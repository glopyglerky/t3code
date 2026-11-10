import type {
  OrchestrationV2AppThread,
  OrchestrationV2DomainEvent,
  OrchestrationV2ProviderThread,
  OrchestrationV2Run,
  OrchestrationV2RuntimeRequest,
  ProviderThreadId,
  RunAttemptId,
  RunId,
  RuntimeRequestId,
  ThreadId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import type * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import { applyThreadPullRequestLinkUpdate } from "../sourceControl/threadPullRequests.ts";

export class ProjectionCommitStoreError extends Schema.TaggedError<ProjectionCommitStoreError>()(
  "ProjectionCommitStoreError",
  { operation: Schema.String, cause: Schema.Defect() },
) {}

const isProjectionCommitStoreError = Schema.is(ProjectionCommitStoreError);
export const toProjectionCommitStoreError = (operation: string) => (cause: unknown) =>
  isProjectionCommitStoreError(cause)
    ? cause
    : new ProjectionCommitStoreError({ operation, cause });

/** Projection mutations and the reads that must share EventSink's transaction. */
export interface ProjectionCommitStoreShape {
  readonly apply: (
    event: OrchestrationV2DomainEvent,
  ) => Effect.Effect<void, ProjectionCommitStoreError>;
  readonly getThread: (
    threadId: ThreadId,
  ) => Effect.Effect<OrchestrationV2AppThread | undefined, ProjectionCommitStoreError>;
  readonly getRuntimeRequest: (
    threadId: ThreadId,
    requestId: RuntimeRequestId,
  ) => Effect.Effect<OrchestrationV2RuntimeRequest | undefined, ProjectionCommitStoreError>;
  readonly getRun: (
    threadId: ThreadId,
    runId: RunId,
  ) => Effect.Effect<OrchestrationV2Run | undefined, ProjectionCommitStoreError>;
  readonly markApplied: (
    sequence: number,
    updatedAt: DateTime.Utc,
  ) => Effect.Effect<void, ProjectionCommitStoreError>;
  readonly nativeThreadOwned: (
    input: Pick<
      OrchestrationV2ProviderThread,
      "driver" | "providerInstanceId" | "appThreadId" | "nativeThreadRef"
    >,
  ) => Effect.Effect<boolean, ProjectionCommitStoreError>;
  readonly runCurrent: (input: {
    readonly threadId: ThreadId;
    readonly runId: RunId;
    readonly activeAttemptId: RunAttemptId;
    readonly expectedStatus: OrchestrationV2Run["status"];
  }) => Effect.Effect<boolean, ProjectionCommitStoreError>;
  readonly providerThreadOwner: (input: {
    readonly providerThreadId: ProviderThreadId;
    readonly runId: RunId;
    readonly activeAttemptId: RunAttemptId;
    readonly expectedLastRunOrdinal: number;
  }) => Effect.Effect<boolean, ProjectionCommitStoreError>;
  readonly providerThreadUnchanged: (
    providerThreadId: ProviderThreadId,
    expectedJson: string,
  ) => Effect.Effect<boolean, ProjectionCommitStoreError>;
}

export class ProjectionCommitStore extends Context.Service<
  ProjectionCommitStore,
  ProjectionCommitStoreShape
>()("t3/orchestration-v2/ProjectionCommitStore") {}

/**
 * Host identity normalization remains in the existing source-control domain.
 * The supplied read and write both join the selected persistence transaction.
 */
export const applyWithThreadLinkNormalization = (
  store: Pick<ProjectionCommitStoreShape, "getThread" | "apply">,
): ProjectionCommitStoreShape["apply"] => Effect.fnUntraced(function* (event) {
  if (event.type !== "thread.pull-request-link-updated") return yield* store.apply(event);
  const current = yield* store.getThread(event.threadId);
  if (current === undefined) {
    return yield* new ProjectionCommitStoreError({
      operation: "apply",
      cause: `No orchestration projection exists for thread ${event.threadId}.`,
    });
  }
  yield* store.apply({
    ...event,
    type: "thread.pull-request-synced",
    payload: applyThreadPullRequestLinkUpdate(current, event.payload),
  });
});
