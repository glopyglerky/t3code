import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

export type MaintenanceRead =
  | { readonly kind: "metadata" }
  | { readonly kind: "bounds" }
  | {
      readonly kind: "prompt_references";
      readonly after_sequence: number;
      readonly through_sequence: number;
    }
  | { readonly kind: "head_references"; readonly through_sequence: number }
  | {
      readonly kind: "candidates";
      readonly after_sequence: number;
      readonly through_sequence: number;
      readonly limit: number;
    }
  | { readonly kind: "appended"; readonly after_sequence: number }
  | {
      readonly kind: "legacy_events";
      readonly after_sequence: number;
      readonly through_sequence: number;
      readonly limit: number;
    }
  | {
      readonly kind: "legacy_receipts";
      readonly after_row_id: number;
      readonly through_row_id: number;
      readonly limit: number;
    }
  | { readonly kind: "reclaimable" };
export type MaintenanceWrite =
  | {
      readonly kind: "metadata";
      readonly name: string;
      readonly sequence: number;
      readonly schema_version: number;
      readonly updated_at: string;
    }
  | { readonly kind: "delete_events"; readonly sequences: ReadonlyArray<number> }
  | { readonly kind: "delete_receipts"; readonly command_ids: ReadonlyArray<string> };

export const MetadataRows = Schema.Array(
  Schema.Struct({
    projection_name: Schema.String,
    last_sequence: Schema.Number,
    updated_at: Schema.String,
  }),
);
export const BoundRows = Schema.Array(
  Schema.Struct({ sequence: Schema.Number, receipt_row_id: Schema.Number }),
);
export const ReferenceRows = Schema.Array(Schema.Struct({ prompt_sequence: Schema.Number }));
export const CandidateRows = Schema.Array(
  Schema.Struct({ sequence: Schema.Number, prunable: Schema.Number }),
);
export const AppendedRows = Schema.Array(
  Schema.Struct({ sequence: Schema.Number, prompt_sequence: Schema.NullOr(Schema.Number) }),
);
export const LegacyEventRows = Schema.Array(
  Schema.Struct({ sequence: Schema.Number, imported: Schema.Number }),
);
export const LegacyReceiptRows = Schema.Array(
  Schema.Struct({ row_id: Schema.Number, command_id: Schema.String, imported: Schema.Number }),
);
export const ReclaimableRows = Schema.Array(Schema.Struct({ reclaimable_bytes: Schema.Number }));
export const EmptyRows = Schema.Array(Schema.JsonObject);

export class MaintenanceRowsError extends Schema.TaggedError<MaintenanceRowsError>()(
  "MaintenanceRowsError",
  { operation: Schema.String, cause: Schema.Defect() },
) {}
export const toRowsError = (operation: string) => (cause: unknown) =>
  new MaintenanceRowsError({ operation, cause });
export interface MaintenanceRowsShape {
  readonly read: <A>(
    query: MaintenanceRead,
    response: Schema.Decoder<A>,
  ) => Effect.Effect<A, MaintenanceRowsError>;
  readonly write: <A>(
    mutation: MaintenanceWrite,
    response: Schema.Decoder<A>,
  ) => Effect.Effect<A, MaintenanceRowsError>;
}
export class MaintenanceRows extends Context.Service<MaintenanceRows, MaintenanceRowsShape>()(
  "t3/orchestration-v2/MaintenanceRows",
) {}
