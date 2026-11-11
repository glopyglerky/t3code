import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { RunAttemptId } from "@t3tools/contracts";
import type { PersistenceTransactionsShape } from "../persistence/PersistenceTransactions.ts";

export type ProjectionContextKind =
  | "turn_runs"
  | "turn_attempts"
  | "turn_nodes"
  | "turn_subagents"
  | "turn_provider_threads"
  | "turn_provider_turns"
  | "turn_scopes"
  | "turn_handoffs"
  | "turn_transfers"
  | "turn_items"
  | "turn_sessions"
  | "turn_messages"
  | "turn_conversation"
  | "thread_exists"
  | "checkpoint_runs"
  | "checkpoint_scopes"
  | "checkpoint_records"
  | "side_chat"
  | "run"
  | "node"
  | "scope"
  | "provider"
  | "ready_ordinals"
  | "checkpoint"
  | "rollback_target_attempt"
  | "rollback_runs"
  | "bound_session"
  | "rolledback_attempts"
  | "stale_checkpoints"
  | "rollback_nodes"
  | "rollback_turns"
  | "attachment_messages"
  | "attachment_renders"
  | "attachment_answers"
  | "attachment_guests"
  | "thread"
  | "runtime_recovery";
/** Fixed application selections; no SQL text crosses the native boundary. */
export interface ProjectionContextQuery {
  readonly kind: ProjectionContextKind;
  readonly thread_id: string;
  readonly run_id?: string;
  readonly record_id?: string | null;
  readonly scope_id?: string;
  readonly ordinal?: number;
  readonly ids?: ReadonlyArray<string | null>;
  readonly provider_thread_id?: string;
  readonly provider_turn_id?: string | null;
  readonly attempt_id?: string | null;
}
export const PayloadRows = Schema.Array(Schema.Struct({ payload_json: Schema.String }));
export const SectionPayloadRows = Schema.Array(
  Schema.Struct({ section: Schema.String, payload_json: Schema.String }),
);
export const ThreadRows = Schema.Array(Schema.Struct({ thread_id: Schema.String }));
export const ConversationRows = Schema.Array(Schema.Struct({ present: Schema.Number }));
export const SideChatRows = Schema.Array(Schema.Struct({ side_chat: Schema.Json }));
export const OrdinalRows = Schema.Array(Schema.Struct({ ordinal: Schema.Number }));
export const AttemptIdRows = Schema.Array(Schema.Struct({ id: RunAttemptId }));
export const AttachmentRows = Schema.Array(Schema.Struct({ id: Schema.Json }));
export const RawRows = Schema.Array(Schema.JsonObject);
export class ProjectionContextRowsError extends Schema.TaggedError<ProjectionContextRowsError>()(
  "ProjectionContextRowsError",
  { cause: Schema.Defect() },
) {}
export interface ProjectionContextRows {
  readonly read: <A>(
    query: ProjectionContextQuery,
    response: Schema.Decoder<A>,
  ) => Effect.Effect<A, ProjectionContextRowsError>;
  readonly withReadTransaction: PersistenceTransactionsShape["withReadTransaction"];
}
