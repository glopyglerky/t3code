import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

export const TaskRow = Schema.Struct({
  task_id: Schema.NullOr(Schema.String),
  title: Schema.String,
  prompt: Schema.String,
  enabled: Schema.Int,
  schedule_json: Schema.String,
  project_id: Schema.String,
  thread_id: Schema.NullOr(Schema.String),
  thread_mode: Schema.String,
  workspace_strategy_json: Schema.String,
  model_selection_json: Schema.String,
  runtime_mode: Schema.String,
  interaction_mode: Schema.String,
  created_by: Schema.String,
  creation_source: Schema.String,
  created_at: Schema.String,
  updated_at: Schema.String,
  next_run_at: Schema.NullOr(Schema.String),
  last_run_at: Schema.NullOr(Schema.String),
  last_run_status: Schema.String,
  last_run_error: Schema.NullOr(Schema.String),
  run_count: Schema.Int,
  webhook_token: Schema.NullOr(Schema.String),
  webhook_secret: Schema.NullOr(Schema.String),
  last_run_webhook_delivery_id: Schema.NullOr(Schema.String),
});
export const TaskRows = Schema.Array(TaskRow);
export const TaskIds = Schema.Array(Schema.Struct({ task_id: Schema.String }));
export const DeliveryIds = Schema.Array(Schema.Struct({ delivery_id: Schema.String }));
export const RelayIds = Schema.Array(Schema.Struct({ relay_delivery_id: Schema.String }));
export const AnyRows = Schema.Array(Schema.Record(Schema.String, Schema.Unknown));
export const DeliveryRow = Schema.Struct({
  delivery_id: Schema.String,
  task_id: Schema.String,
  received_at: Schema.String,
  method: Schema.String,
  query: Schema.String,
  headers_json: Schema.String,
  body: Schema.String,
  body_bytes: Schema.Int,
  body_truncated: Schema.Int,
  outcome: Schema.String,
  signature_verified: Schema.Int,
  missing_fields_json: Schema.String,
  rendered_prompt: Schema.NullOr(Schema.String),
  pending_window: Schema.Int,
  error: Schema.NullOr(Schema.String),
});
export const DeliveryRows = Schema.Array(DeliveryRow);
export const PendingRows = Schema.Array(
  Schema.Struct({
    delivery_id: Schema.String,
    task_id: Schema.String,
    coalesce_key: Schema.String,
    due_at: Schema.Int,
    state: Schema.String,
    task_json: Schema.String,
    request_json: Schema.String,
    delivery_ids_json: Schema.String,
  }),
);
export const NextDueRows = Schema.Array(
  Schema.Struct({ next_run_at: Schema.NullOr(Schema.String) }),
);
export const NextPendingRows = Schema.Array(Schema.Struct({ due_at: Schema.NullOr(Schema.Int) }));
export const CountRows = Schema.Array(Schema.Struct({ count: Schema.Int }));
export type Row = Readonly<Record<string, string | number | null>>;

export type ScheduledRead =
  | {
      readonly kind: "tasks";
      readonly task_id?: string;
      readonly thread_id?: string;
      readonly due_before?: string;
    }
  | { readonly kind: "next_due" }
  | { readonly kind: "deliveries"; readonly task_id: string; readonly delivery_id?: string }
  | {
      readonly kind: "pending";
      readonly delivery_id?: string;
      readonly task_id?: string;
      readonly states?: ReadonlyArray<string>;
      readonly coalesce_key?: string;
      readonly due_after?: number;
      readonly first: boolean;
    }
  | { readonly kind: "pending_count"; readonly task_id: string }
  | { readonly kind: "next_pending" }
  | { readonly kind: "active_pending_tasks" }
  | {
      readonly kind: "has_replay";
      readonly task_id: string;
      readonly replay_key: string;
      readonly now: number;
    };

export type ScheduledWrite =
  | {
      readonly kind: "save_task";
      readonly row: Row;
      readonly require_existing: boolean;
      readonly kept_schedule_json: string | null;
      readonly secret_changed: boolean;
    }
  | {
      readonly kind: "patch_task";
      readonly task_id?: string;
      readonly thread_ids?: ReadonlyArray<string>;
      readonly expected: Row;
      readonly different: Row;
      readonly patch: Row;
      readonly increment_run_count: boolean;
    }
  | { readonly kind: "delete_tasks"; readonly task_id?: string; readonly thread_id?: string }
  | {
      readonly kind: "recover_task";
      readonly task_id: string | null;
      readonly patch: Row;
      readonly pending_delivery_id?: string;
      readonly match_pending_task: boolean;
    }
  | { readonly kind: "record_delivery"; readonly row: Row }
  | { readonly kind: "patch_delivery"; readonly delivery_id: string; readonly patch: Row }
  | {
      readonly kind: "prune_deliveries";
      readonly task_id: string;
      readonly bucket: "main" | "filtered" | "dedupe";
      readonly limit: number;
    }
  | {
      readonly kind: "claim_relay";
      readonly relay_delivery_id: string;
      readonly task_id: string;
      readonly seen_at: string;
    }
  | { readonly kind: "delete_relay"; readonly relay_delivery_id: string }
  | { readonly kind: "prune_relays"; readonly before: string }
  | {
      readonly kind: "claim_replay";
      readonly task_id: string;
      readonly replay_key: string;
      readonly delivery_id: string;
      readonly now: number;
      readonly expires_at: number;
    }
  | { readonly kind: "release_replay"; readonly delivery_id: string }
  | { readonly kind: "insert_pending"; readonly row: Row }
  | {
      readonly kind: "patch_pending";
      readonly delivery_id: string;
      readonly states?: ReadonlyArray<string>;
      readonly not_states?: ReadonlyArray<string>;
      readonly patch: Row;
      readonly append_delivery_id?: string;
    }
  | { readonly kind: "delete_pending"; readonly delivery_id: string }
  | {
      readonly kind: "expire_pending";
      readonly now: number;
      readonly task_id?: string;
      readonly coalesce_key?: string;
    }
  | { readonly kind: "release_failed_relays"; readonly delivery_id: string };

export class ScheduledRowsError extends Schema.TaggedError<ScheduledRowsError>()(
  "ScheduledRowsError",
  {
    operation: Schema.Literals(["read", "write"]),
    cause: Schema.Defect(),
  },
) {}
export const toRowsError = (operation: "read" | "write") => (cause: unknown) =>
  new ScheduledRowsError({ operation, cause });

export interface ScheduledRowsShape {
  readonly read: <A>(
    query: ScheduledRead,
    response: Schema.Decoder<A>,
  ) => Effect.Effect<A, ScheduledRowsError>;
  readonly write: <A>(
    mutation: ScheduledWrite,
    response: Schema.Decoder<A>,
  ) => Effect.Effect<A, ScheduledRowsError>;
}
export class ScheduledRows extends Context.Service<ScheduledRows, ScheduledRowsShape>()(
  "t3/scheduledTasks/ScheduledRows",
) {}
