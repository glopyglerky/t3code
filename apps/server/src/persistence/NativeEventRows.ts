import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";

import { EventRows, toEventRowsError, type EventRowsShape } from "./EventRows.ts";
import { RedbClient } from "./RedbClient.ts";

const Counter = Schema.Number.check(
  Schema.isInt(),
  Schema.isBetween({ minimum: 0, maximum: Number.MAX_SAFE_INTEGER }),
);
const StoredRow = Schema.Struct({
  sequence: Counter,
  event_id: Schema.String,
  command_id: Schema.NullOr(Schema.String),
  aggregate_kind: Schema.Literals(["project", "thread"]),
  stream_id: Schema.String,
  event_type: Schema.String,
  occurred_at: Schema.String,
  payload_json: Schema.String,
  metadata_json: Schema.String,
  application_event_version: Counter,
  causation_event_id: Schema.NullOr(Schema.String),
  correlation_id: Schema.NullOr(Schema.String),
});
const Appended = Schema.Struct({ sequence: Counter, stores_full_prompt: Schema.Boolean });
const Stats = Schema.Struct({
  event_count: Counter,
  raw_payload_bytes: Counter,
  has_create_event: Schema.Boolean,
});

/** Prepare imported indexes before opening sessions; every row operation then uses RedbClient. */
export const make = Effect.gen(function* () {
  const client = yield* RedbClient;
  while (!(yield* client.prepareEvents).prepared) yield* Effect.yieldNow;
  return EventRows.of({
    append: (input) => client.request("application_events_append", { ...input }, Appended).pipe(
      Effect.mapError(toEventRowsError("append")),
    ),
    read: (input) => client.request("application_events_read", { ...input }, Schema.Array(StoredRow)).pipe(
      Effect.mapError(toEventRowsError("read")),
    ),
    stats: (input) => client.request("application_events_stats", { ...input }, Stats).pipe(
      Effect.mapError(toEventRowsError("stats")),
    ),
    latest: (input) => client.request("application_events_latest", { ...input }, Counter).pipe(
      Effect.mapError(toEventRowsError("latest")),
    ),
    prunedThrough: client.request("application_events_pruned_through", {}, Counter).pipe(
      Effect.mapError(toEventRowsError("pruned-through")),
    ),
  } satisfies EventRowsShape);
});

export const layer = Layer.effect(EventRows, make);
