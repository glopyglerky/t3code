import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import type * as Schema from "effect/Schema";
import { RedbClient } from "../persistence/RedbClient.ts";
import {
  ScheduledRows,
  type ScheduledRead,
  type ScheduledWrite,
  toRowsError,
} from "./ScheduledRows.ts";

export const make = Effect.gen(function* () {
  const client = yield* RedbClient;
  while (!(yield* client.prepareScheduled).prepared) yield* Effect.yieldNow;
  return ScheduledRows.of({
    read: Effect.fn("ScheduledRows.read")(<A>(query: ScheduledRead, response: Schema.Decoder<A>) =>
      client
        .request("application_scheduled_read", { query }, response)
        .pipe(Effect.mapError(toRowsError("read"))),
    ),
    write: Effect.fn("ScheduledRows.write")(
      <A>(mutation: ScheduledWrite, response: Schema.Decoder<A>) =>
        client
          .request("application_scheduled_write", { mutation }, response)
          .pipe(Effect.mapError(toRowsError("write"))),
    ),
  });
});
export const layer = Layer.effect(ScheduledRows, make);
