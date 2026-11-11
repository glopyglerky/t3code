import * as Effect from "effect/Effect";

import { RedbClient } from "../persistence/RedbClient.ts";
import {
  ProjectionLookupResult,
  ProjectionLookupRowsError,
  type ProjectionLookupRows,
} from "./ProjectionLookupRows.ts";
import { makeProjectionLookups } from "./ProjectionStore.ts";

export const makeRows = Effect.gen(function* () {
  const client = yield* RedbClient;
  while (!(yield* client.prepareProjectionReads).prepared) yield* Effect.yieldNow;
  return {
    query: (query) =>
      client
        .request("application_projections_lookup", { query }, ProjectionLookupResult)
        .pipe(Effect.mapError((cause) => new ProjectionLookupRowsError({ cause }))),
  } satisfies ProjectionLookupRows;
});

/** Only the lookup family, not a replacement for the full ProjectionStore service. */
export const make = makeRows.pipe(Effect.map(makeProjectionLookups));
