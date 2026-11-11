import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

/** Point queries shared by the retained SQL service and native projection reader. */
export type ProjectionLookup =
  | { readonly kind: "thread"; readonly thread_id: string }
  | { readonly kind: "thread_exists"; readonly thread_id: string }
  | { readonly kind: "plan"; readonly thread_id: string; readonly record_id: string }
  | { readonly kind: "runtime_request"; readonly thread_id: string; readonly record_id: string }
  | { readonly kind: "turn_item"; readonly thread_id: string; readonly record_id: string }
  | { readonly kind: "node"; readonly record_id: string }
  | {
      readonly kind: "messages";
      readonly thread_id: string;
      readonly record_ids: ReadonlyArray<string>;
    }
  | { readonly kind: "run_message"; readonly thread_id: string; readonly run_id: string }
  | { readonly kind: "run"; readonly thread_id: string; readonly run_id: string }
  | { readonly kind: "can_start_queued_run"; readonly thread_id: string }
  | { readonly kind: "held_queued_runs"; readonly thread_id: string }
  | { readonly kind: "message_count"; readonly thread_id: string }
  | { readonly kind: "subagent_count"; readonly thread_id: string }
  | { readonly kind: "app_owned_child_threads"; readonly thread_id: string }
  | { readonly kind: "next_turn_item_ordinal"; readonly thread_id: string }
  | {
      readonly kind: "turn_start_history";
      readonly thread_id: string;
      readonly run_ids?: ReadonlyArray<string>;
    }
  | { readonly kind: "thread_metadata"; readonly thread_ids: ReadonlyArray<string> }
  | {
      readonly kind: "unpaired_interrupt";
      readonly thread_id: string;
      readonly request_id: string;
      readonly result_id: string;
    };

export const ProjectionLookupResult = Schema.Struct({
  owner_exists: Schema.Boolean,
  payloads: Schema.Array(Schema.String),
  metadata: Schema.Array(
    Schema.Struct({ thread_id: Schema.String, project_id: Schema.String, title: Schema.String }),
  ),
  workspaces: Schema.Array(
    Schema.Struct({ project_id: Schema.String, worktree_path: Schema.String }),
  ),
  ids: Schema.Array(Schema.String),
  count: Schema.Number,
  found: Schema.Boolean,
});
export type ProjectionLookupResult = typeof ProjectionLookupResult.Type;

export class ProjectionLookupRowsError extends Schema.TaggedError<ProjectionLookupRowsError>()(
  "ProjectionLookupRowsError",
  { cause: Schema.Defect() },
) {}

export interface ProjectionLookupRows {
  readonly query: (
    query: ProjectionLookup,
  ) => Effect.Effect<ProjectionLookupResult, ProjectionLookupRowsError>;
}
