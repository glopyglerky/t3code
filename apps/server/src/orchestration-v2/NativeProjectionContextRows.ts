import * as Effect from "effect/Effect";
import type * as Schema from "effect/Schema";
import { RedbClient } from "../persistence/RedbClient.ts";
import {
  ProjectionContextRowsError,
  type ProjectionContextRows,
  type ProjectionContextQuery,
} from "./ProjectionContextRows.ts";

export const make = Effect.gen(function* () {
  const client = yield* RedbClient;
  while (!(yield* client.prepareProjections).prepared) yield* Effect.yieldNow;
  while (!(yield* client.prepareProjectionReads).prepared) yield* Effect.yieldNow;
  return {
    read: Effect.fn("ProjectionContextRows.read")(
      <A>(query: ProjectionContextQuery, response: Schema.Decoder<A>) =>
        client
          .request("application_projections_context", { query }, response)
          .pipe(Effect.mapError((cause) => new ProjectionContextRowsError({ cause }))),
    ),
    withReadTransaction: client.withReadTransaction,
  } satisfies ProjectionContextRows;
});
