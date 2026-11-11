import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import type { PersistenceTransactionsShape } from "../persistence/PersistenceTransactions.ts";

/** Row selections for provider control. Domain decoding and control flow stay shared. */
export type ProjectionControlQuery =
  | {
      readonly kind: "pending_user_requests";
      readonly thread_id: string;
      readonly provider_turn_id: string;
    }
  | {
      readonly kind: "pending_user_input_details";
      readonly thread_id: string;
      readonly provider_turn_id: string;
      readonly node_ids: ReadonlyArray<string>;
    }
  | {
      readonly kind: "provider_control";
      readonly thread_id: string;
      readonly provider_thread_id: string;
      readonly provider_turn_id: string;
      readonly attempt_id?: string;
      readonly message_id?: string;
    }
  | {
      readonly kind: "provider_control_run";
      readonly thread_id: string;
      readonly active_attempt_id: string | null;
    }
  | { readonly kind: "running_run"; readonly thread_id: string }
  | {
      readonly kind: "running_turn_details";
      readonly provider_thread_id: string | null;
      readonly run_attempt_id: string | null;
      readonly node_id: string | null;
    }
  | {
      readonly kind: "thread_providers";
      readonly thread_id: string;
      readonly active_provider_thread_id: string | null;
      readonly target_instance_id?: string;
    }
  | {
      readonly kind: "runtime_response";
      readonly thread_id: string;
      readonly request_id: string;
      readonly node_id: string;
      readonly session_id?: string;
    };

export const ProjectionControlResult = Schema.Struct({
  requests: Schema.Array(Schema.String),
  nodes: Schema.Array(Schema.String),
  items: Schema.Array(Schema.String),
  providerThreads: Schema.Array(Schema.String),
  providerTurns: Schema.Array(Schema.String),
  attempts: Schema.Array(Schema.String),
  messages: Schema.Array(Schema.String),
  runs: Schema.Array(Schema.String),
  sessions: Schema.Array(Schema.String),
});
export type ProjectionControlResult = typeof ProjectionControlResult.Type;

export class ProjectionControlRowsError extends Schema.TaggedError<ProjectionControlRowsError>()(
  "ProjectionControlRowsError",
  { cause: Schema.Defect() },
) {}

export interface ProjectionControlRows {
  readonly query: (
    query: ProjectionControlQuery,
  ) => Effect.Effect<ProjectionControlResult, ProjectionControlRowsError>;
  readonly withReadTransaction: PersistenceTransactionsShape["withReadTransaction"];
}
