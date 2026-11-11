import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";

import { RedbClient } from "../persistence/RedbClient.ts";
import { CodexUsageHistory, historyFromRows } from "./CodexUsageHistory.ts";

const Row = Schema.Struct({
  native_thread_id: Schema.Unknown,
  native_turn_id: Schema.String,
  selection_json: Schema.Unknown,
});

export const make = Effect.gen(function* () {
  const client = yield* RedbClient;
  while (!(yield* client.prepareProjectionReads).prepared) yield* Effect.yieldNow;
  while (!(yield* client.prepareEvents).prepared) yield* Effect.yieldNow;
  return historyFromRows((native_turn_ids) =>
    client.request(
      "application_projections_history",
      {
        query: { kind: "codex_usage", native_turn_ids },
      },
      Schema.Array(Row),
    ),
  );
});

export const layer = Layer.effect(CodexUsageHistory, make);
