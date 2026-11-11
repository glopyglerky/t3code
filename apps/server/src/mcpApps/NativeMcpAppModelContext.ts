import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";

import { RedbClient } from "../persistence/RedbClient.ts";
import { McpAppModelContext, modelContextFromRows } from "./McpAppModelContext.ts";

const Written = Schema.Struct({ written: Schema.Literal(true) });
const Entry = Schema.Struct({
  itemId: Schema.String,
  server: Schema.String,
  tool: Schema.String,
  text: Schema.String,
});

export const make = Effect.gen(function* () {
  const client = yield* RedbClient;
  while (!(yield* client.prepareRuntimeState).prepared) yield* Effect.yieldNow;
  return modelContextFromRows({
    remove: ({ threadId, itemId }) =>
      client
        .request(
          "application_runtime_state_write",
          {
            request: { kind: "context_delete", thread_id: threadId, item_id: itemId },
          },
          Written,
        )
        .pipe(Effect.asVoid),
    upsert: ({ threadId, itemId, server, tool, text, updatedAt }) =>
      client
        .request(
          "application_runtime_state_write",
          {
            request: {
              kind: "context_upsert",
              context: { threadId, itemId, server, tool, text, updatedAt },
            },
          },
          Written,
        )
        .pipe(Effect.asVoid),
    forThread: (threadId) =>
      client.request(
        "application_runtime_state_read",
        {
          request: { kind: "context_for_thread", thread_id: threadId },
        },
        Schema.Array(Entry),
      ),
  });
});

export const layer = Layer.effect(McpAppModelContext, make);
