import * as Effect from "effect/Effect";

import { RedbClient } from "../persistence/RedbClient.ts";
import {
  ProjectionControlResult,
  ProjectionControlRowsError,
  type ProjectionControlRows,
} from "./ProjectionControlRows.ts";
import * as NativeProjectionLookupRows from "./NativeProjectionLookupRows.ts";
import { makeProjectionControls, makeProjectionLookups } from "./ProjectionStore.ts";

export const makeRows = Effect.gen(function* () {
  const client = yield* RedbClient;
  while (!(yield* client.prepareProjectionReads).prepared) yield* Effect.yieldNow;
  return {
    withReadTransaction: client.withReadTransaction,
    query: (query) =>
      client
        .request("application_projections_control", { query }, ProjectionControlResult)
        .pipe(Effect.mapError((cause) => new ProjectionControlRowsError({ cause }))),
  } satisfies ProjectionControlRows;
});

export const make = Effect.gen(function* () {
  return makeProjectionControls(
    yield* makeRows,
    makeProjectionLookups(yield* NativeProjectionLookupRows.makeRows),
  );
});
