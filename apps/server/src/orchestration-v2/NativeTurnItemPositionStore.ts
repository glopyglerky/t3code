import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";

import { RedbClient } from "../persistence/RedbClient.ts";
import {
  TurnItemPositionStoreError,
  TurnItemPositionStoreV2,
  makeWithAllocate,
} from "./TurnItemPositionStore.ts";

const Ordinal = Schema.Number.check(
  Schema.isInt(),
  Schema.isBetween({ minimum: Number.MIN_SAFE_INTEGER, maximum: Number.MAX_SAFE_INTEGER }),
);

export const make = Effect.gen(function* () {
  const client = yield* RedbClient;
  while (!(yield* client.preparePositions).prepared) yield* Effect.yieldNow;
  return makeWithAllocate(({ threadId, turnItemId, runId, runOrdinal, placement }) =>
    client
      .request(
        "application_positions_allocate",
        {
          thread_id: threadId,
          turn_item_id: turnItemId,
          run_id: runId,
          ...(placement === undefined ? {} : { placement }),
          ...(runOrdinal === undefined ? {} : { run_ordinal: runOrdinal }),
        },
        Ordinal,
      )
      .pipe(
        Effect.mapError((cause) => new TurnItemPositionStoreError({ threadId, turnItemId, cause })),
      ),
  );
});

export const layer = Layer.effect(TurnItemPositionStoreV2, make);
