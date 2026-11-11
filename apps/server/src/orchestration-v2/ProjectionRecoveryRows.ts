import type * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import type { ProjectionStoreV2Shape, ProjectionRecoveryKind } from "./ProjectionStore.ts";

export type ProjectionAuditTable =
  | "threads"
  | "runs"
  | "run_attempts"
  | "nodes"
  | "subagents"
  | "provider_sessions"
  | "provider_threads"
  | "provider_turns"
  | "runtime_requests"
  | "messages"
  | "plans"
  | "turn_items"
  | "checkpoint_scopes"
  | "checkpoints"
  | "context_handoffs"
  | "context_transfers";

export const LimitRecoveryRow = Schema.Struct({
  payload_json: Schema.String,
  run_id: Schema.String,
  completed_at: Schema.NullOr(Schema.String),
  failure_payload_json: Schema.String,
  last_error: Schema.NullOr(Schema.String),
});
export const RecoveryWakeRow = Schema.Struct({
  reset_ms: Schema.NullOr(Schema.Number),
  snooze_ms: Schema.NullOr(Schema.Number),
});
export const RecoveryThreadRow = Schema.Struct({ thread_id: Schema.String });
export const ProjectionAuditRow = Schema.Struct({
  row_id: Schema.String,
  entity_id: Schema.String,
  thread_id: Schema.NullOr(Schema.String),
  target_thread_id: Schema.NullOr(Schema.String),
  payload_json: Schema.String,
});
export const ProjectionAuditPage = Schema.Struct({
  rows: Schema.Array(ProjectionAuditRow),
  next: Schema.NullOr(Schema.String),
});

export class ProjectionRecoveryRowsError extends Schema.TaggedError<ProjectionRecoveryRowsError>()(
  "ProjectionRecoveryRowsError",
  { cause: Schema.Defect() },
) {}

export interface ProjectionRecoveryRows {
  readonly limitCandidates: (
    options: Parameters<ProjectionStoreV2Shape["getLimitRecoveryCandidates"]>[0],
  ) => Effect.Effect<ReadonlyArray<typeof LimitRecoveryRow.Type>, ProjectionRecoveryRowsError>;
  readonly wakeAt: (
    now: string,
  ) => Effect.Effect<ReadonlyArray<typeof RecoveryWakeRow.Type>, ProjectionRecoveryRowsError>;
  readonly recoveryThreads: (
    kind: ProjectionRecoveryKind,
    projects?: {
      readonly include?: ReadonlyArray<string>;
      readonly exclude?: ReadonlyArray<string>;
    },
  ) => Effect.Effect<ReadonlyArray<typeof RecoveryThreadRow.Type>, ProjectionRecoveryRowsError>;
  readonly unreadablePage: (
    table: ProjectionAuditTable,
    id: string,
    after: string | null,
    limit: number,
  ) => Effect.Effect<typeof ProjectionAuditPage.Type, ProjectionRecoveryRowsError>;
  readonly bindings: (
    id: string,
  ) => Effect.Effect<ReadonlyArray<typeof RecoveryThreadRow.Type>, ProjectionRecoveryRowsError>;
  readonly providerThreadOwners: (
    id: string,
  ) => Effect.Effect<ReadonlyArray<typeof RecoveryThreadRow.Type>, ProjectionRecoveryRowsError>;
}
