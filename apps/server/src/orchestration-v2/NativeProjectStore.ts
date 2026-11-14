import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";

import { RedbClient } from "../persistence/RedbClient.ts";
import * as NativeProjectRows from "./NativeProjectRows.ts";
import { ProjectRow, ProjectStoreV2, makeWithRowsAndReducer, reduceEvent } from "./ProjectStore.ts";

const fields = ProjectRow.fields;
// This is the transport contract for fields apply actually consumes. Reusing
// the row's type schemas preserves its looser title/date rules and discards
// ignored extensions before bounded JSON serialization, without normalizing.
const EventInput = Schema.Union([
  Schema.Struct({
    type: Schema.Literal("project.created"),
    payload: Schema.Struct({
      projectId: fields.projectId,
      title: fields.title,
      workspaceRoot: fields.workspaceRoot,
      defaultModelSelection: fields.defaultModelSelection,
      defaultThreadEnvMode: Schema.optional(fields.defaultThreadEnvMode),
      faviconPath: Schema.optional(fields.faviconPath),
      projectIcon: Schema.optional(fields.projectIcon),
      scripts: fields.scripts,
      createdAt: fields.createdAt,
      updatedAt: fields.updatedAt,
    }),
  }),
  Schema.Struct({
    type: Schema.Literal("project.meta-updated"),
    payload: Schema.Struct({
      projectId: fields.projectId,
      title: Schema.optional(fields.title),
      workspaceRoot: Schema.optional(fields.workspaceRoot),
      defaultModelSelection: Schema.optional(fields.defaultModelSelection),
      defaultThreadEnvMode: Schema.optional(fields.defaultThreadEnvMode),
      autoPull: Schema.optional(fields.autoPull),
      faviconPath: Schema.optional(fields.faviconPath),
      projectIcon: Schema.optional(fields.projectIcon),
      scripts: Schema.optional(fields.scripts),
      pinnedAt: Schema.optional(fields.pinnedAt),
      archivedAt: Schema.optional(fields.archivedAt),
      updatedAt: fields.updatedAt,
    }),
  }),
  Schema.Struct({
    type: Schema.Literal("project.deleted"),
    payload: Schema.Struct({ projectId: fields.projectId, deletedAt: fields.updatedAt }),
  }),
]);
const encodeExisting = Schema.encodeEffect(Schema.fromJsonString(Schema.toType(ProjectRow)));
const encodeEvent = Schema.encodeEffect(Schema.fromJsonString(Schema.toType(EventInput)));
const decodeRow = Schema.decodeUnknownEffect(Schema.toType(ProjectRow));
const Reduction = Schema.Union([
  Schema.Struct({ status: Schema.Literal("reduced"), row: Schema.JsonObject }),
  Schema.Struct({ status: Schema.Literal("invalid_event") }),
]);

class InvalidProjectEvent extends Schema.TaggedError<InvalidProjectEvent>()(
  "InvalidProjectEvent",
  {},
) {}

const MAX_REDUCTION_BYTES = 16 * 1024 * 1024;
// The real session ID is at most "application:9007199254740991". This also
// covers the operation/envelope/newline and the reducer's added creation fields.
const REDUCTION_HEADROOM = 1_024;
const MAX_REDUCTION_VALUES = 262_144;
const MAX_REDUCTION_DEPTH = 64;
const utf8 = new TextEncoder();

function canReduceNatively(existingJson: string | null, eventJson: string): boolean {
  const inputs = existingJson === null ? [eventJson] : [existingJson, eventJson];
  const limit = MAX_REDUCTION_BYTES - REDUCTION_HEADROOM;
  // This cheap lower bound avoids allocating another large encoded frame for
  // an old/replacement pair that already cannot fit the new pure request.
  if (inputs.reduce((sum, input) => sum + input.length, 0) > limit) return false;
  if (inputs.reduce((sum, input) => sum + utf8.encode(input).length, 0) > limit) return false;
  if (
    utf8.encode(JSON.stringify({ existing_json: existingJson, event_json: eventJson })).length >
    limit
  )
    return false;

  function* children(value: object): Generator<{ key?: string; value: unknown }> {
    if (Array.isArray(value)) {
      for (const child of value) yield { value: child };
    } else {
      for (const key in value) {
        if (Object.hasOwn(value, key)) yield { key, value: Reflect.get(value, key) };
      }
    }
  }
  // JSON.parse accepts escaped lone UTF-16 surrogates; Rust strings do not.
  // Inspect only the codec-selected JSON, with stack space bounded by depth.
  // Count each container/scalar as the Rust parser does, excluding object keys.
  // The combined budget is conservative for its two separate input parses.
  let values = 0;
  for (const input of inputs) {
    const pending: Array<{
      iterator: Iterator<{ key?: string; value: unknown }>;
      depth: number;
    }> = [{ iterator: [{ value: JSON.parse(input) as unknown }].values(), depth: 0 }];
    while (pending.length > 0) {
      const frame = pending.at(-1);
      if (frame === undefined) return false;
      const entry = frame.iterator.next();
      if (entry.done) {
        pending.pop();
        continue;
      }
      if (++values > MAX_REDUCTION_VALUES - 32) return false;
      if (entry.value.key !== undefined && !entry.value.key.isWellFormed()) return false;
      const value = entry.value.value;
      if (typeof value === "string" && !value.isWellFormed()) return false;
      if (typeof value !== "object" || value === null) continue;
      if (frame.depth >= MAX_REDUCTION_DEPTH - 1) return false;
      pending.push({ iterator: children(value), depth: frame.depth + 1 });
    }
  }
  return true;
}

/** The reducer joins the same session as get/upsert and never owns a commit. */
export const makeWithRows = Effect.gen(function* () {
  const client = yield* RedbClient;
  return yield* makeWithRowsAndReducer(
    Effect.fn("NativeProjectStore.reduceEvent")(function* (event, existing) {
      const existingJson = existing === null ? null : yield* encodeExisting(existing);
      // apply does not inspect the envelope or validate the public mutation
      // contract. Row codecs retain its direct-event acceptance boundary.
      const eventJson = yield* encodeEvent(event);
      if (!canReduceNatively(existingJson, eventJson)) return yield* reduceEvent(event, existing);
      const result = yield* client.request(
        "application_projects_reduce_event",
        {
          existing_json: existingJson,
          event_json: eventJson,
        },
        Reduction.check(
          Schema.makeFilter(
            (result) =>
              result.status === "invalid_event" || result.row.projectId === event.payload.projectId,
            { expected: "a reduced row for the requested project" },
          ),
        ),
      );
      if (result.status === "invalid_event") return yield* new InvalidProjectEvent();
      // Row validation belongs outside request: like the original row encoder,
      // a caught domain failure must not poison an existing database transaction.
      // The unchanged ProjectDbRow encoder performs normalization before upsert.
      return yield* decodeRow(result.row);
    }),
  );
});

export const make = makeWithRows.pipe(Effect.provide(NativeProjectRows.layer));
export const layer = Layer.effect(ProjectStoreV2, make);
