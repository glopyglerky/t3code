import {
  OrchestrationV2TurnItem,
  OrchestrationV2TurnItemStatus,
  OrchestrationV2UserMessageInputIntent,
  type ThreadId,
  type RunId,
  type TurnItemId,
} from "@t3tools/contracts";
import type * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import type { ProjectionRecordRowsError, ProjectionRecordWindow } from "./ProjectionRecordRows.ts";
import type { PersistenceTransactionsShape } from "../persistence/PersistenceTransactions.ts";

export const ProjectionTimelineRow = Schema.Struct({
  turn_item_id: Schema.String,
  run_id: Schema.NullOr(Schema.String),
  node_id: Schema.NullOr(Schema.String),
  parent_item_id: Schema.NullOr(Schema.String),
  type: Schema.Union(OrchestrationV2TurnItem.members.map((member) => member.fields.type)),
  status: OrchestrationV2TurnItemStatus,
  ordinal: Schema.Number,
  input_intent: Schema.NullOr(OrchestrationV2UserMessageInputIntent),
  created_by: Schema.NullOr(Schema.Literals(["user", "agent", "system"])),
  message_id: Schema.NullOr(Schema.String),
  persistent_output: Schema.Number,
  attempt_id: Schema.NullOr(Schema.String),
  attempt_status: Schema.NullOr(Schema.String),
});
export type ProjectionTimelineRow = typeof ProjectionTimelineRow.Type;

export interface ProjectionHistoryRows {
  readonly timeline: (
    threadId: ThreadId,
    includePersistentOutput: boolean,
  ) => Effect.Effect<ReadonlyArray<ProjectionTimelineRow>, ProjectionRecordRowsError>;
  readonly forkBoundary: (
    threadId: ThreadId,
    runId: RunId,
    anchorItemId?: TurnItemId,
  ) => Effect.Effect<string | null, ProjectionRecordRowsError>;
  readonly inheritedCount: (
    threadId: ThreadId,
    runId: RunId,
    window: ProjectionRecordWindow,
  ) => Effect.Effect<number | null, ProjectionRecordRowsError>;
}

export interface ProjectionSnapshotRows {
  readonly withReadTransaction: PersistenceTransactionsShape["withReadTransaction"];
  readonly anchorThread: (
    itemId: TurnItemId,
  ) => Effect.Effect<ThreadId | null, ProjectionRecordRowsError>;
  readonly pendingCards: (
    threadId: ThreadId,
  ) => Effect.Effect<ReadonlyArray<string>, ProjectionRecordRowsError>;
  readonly snapshotSequence: (
    threadId: ThreadId,
  ) => Effect.Effect<number, ProjectionRecordRowsError>;
}

export const ProjectionMaterializedRow = Schema.Struct({
  thread_id: Schema.String,
  turn_item_id: Schema.String,
  payload_json: Schema.String,
});
export type ProjectionMaterializedRow = typeof ProjectionMaterializedRow.Type;
export const ProjectionAttachmentOwner = Schema.Struct({
  thread_id: Schema.String,
  turn_item_id: Schema.String,
});
export type ProjectionAttachmentOwner = typeof ProjectionAttachmentOwner.Type;

export interface ProjectionTimelineRows {
  readonly withReadTransaction: PersistenceTransactionsShape["withReadTransaction"];
  readonly snapshotSequence: ProjectionSnapshotRows["snapshotSequence"];
  readonly materialize: (
    sources: string,
  ) => Effect.Effect<ReadonlyArray<ProjectionMaterializedRow>, ProjectionRecordRowsError>;
  readonly attachmentOwners: (
    threadIds: ReadonlyArray<ThreadId>,
    attachmentId: string,
  ) => Effect.Effect<ReadonlyArray<ProjectionAttachmentOwner>, ProjectionRecordRowsError>;
}
