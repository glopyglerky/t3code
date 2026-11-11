import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

export const PeerLinkRow = Schema.Struct({
  environment_id: Schema.String,
  secret_name: Schema.String,
  label: Schema.String,
  urls_json: Schema.String,
  access: Schema.String,
  linked_at: Schema.String,
  expires_at: Schema.String,
  last_reached_at: Schema.NullOr(Schema.String),
  last_error: Schema.NullOr(Schema.String),
  fenced: Schema.Int,
});
export type PeerLinkRow = typeof PeerLinkRow.Type;

export class PeerLinkRowsError extends Schema.TaggedError<PeerLinkRowsError>()(
  "PeerLinkRowsError",
  { operation: Schema.String, cause: Schema.Defect() },
) {}

export const toPeerLinkRowsError = (operation: string) => (cause: unknown) =>
  new PeerLinkRowsError({ operation, cause });

/** Storage only. Credential, network and expiry behavior stays in PeerLinks. */
export class PeerLinkRows extends Context.Service<
  PeerLinkRows,
  {
    readonly list: Effect.Effect<ReadonlyArray<PeerLinkRow>, PeerLinkRowsError>;
    readonly get: (environmentId: string) => Effect.Effect<PeerLinkRow | null, PeerLinkRowsError>;
    readonly upsert: (row: PeerLinkRow) => Effect.Effect<void, PeerLinkRowsError>;
    readonly relabel: (
      environmentId: string,
      label: string,
    ) => Effect.Effect<void, PeerLinkRowsError>;
    readonly remove: (environmentId: string) => Effect.Effect<string | null, PeerLinkRowsError>;
    readonly recordOutcome: (
      environmentId: string,
      error: string | null,
      reachedAt: string,
    ) => Effect.Effect<void, PeerLinkRowsError>;
  }
>()("t3/peer/PeerLinkRows") {}
