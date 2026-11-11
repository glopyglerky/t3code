import { OrchestrationV2TurnItem, ThreadId, TurnItemId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import { RedbClient } from "../persistence/RedbClient.ts";
import { makeThreadFindWithRows, type ReadThreadFindIndex } from "./ThreadFind.ts";
import type { ThreadFindRows } from "./ThreadFindRows.ts";

const ActiveThreads = Schema.Array(Schema.Struct({ cwd: Schema.NullOr(Schema.String) }));
const Sources = Schema.Array(Schema.Struct({ threadId: ThreadId, id: TurnItemId }));
const ThreadIds = Schema.Array(ThreadId);
const encodeSources = Schema.encodeEffect(Sources);
const encodeThreadIds = Schema.encodeEffect(ThreadIds);
const Payloads = Schema.Array(
  Schema.Struct({
    thread_id: Schema.String,
    turn_item_id: Schema.String,
    payload_json: Schema.String,
    entry_id: Schema.String,
    item_type: Schema.Union(OrchestrationV2TurnItem.members.map((item) => item.fields.type)),
  }),
);

const makeRows = Effect.gen(function* () {
  const client = yield* RedbClient;
  while (!(yield* client.prepareProjectionReads).prepared) yield* Effect.yieldNow;
  while (!(yield* client.prepareEvents).prepared) yield* Effect.yieldNow;
  return {
    activeThread: (threadId) =>
      client.request(
        "application_thread_find_read",
        { query: { kind: "active_thread", thread_id: threadId } },
        ActiveThreads,
      ),
    payloads: (sources) =>
      encodeSources(sources).pipe(
        Effect.flatMap((encoded) =>
          client.request(
            "application_thread_find_read",
            {
              query: {
                kind: "payloads",
                sources: encoded.map(({ threadId, id }) => ({ thread_id: threadId, item_id: id })),
              },
            },
            Payloads,
          ),
        ),
      ),
    revision: (threadIds) =>
      encodeThreadIds(threadIds).pipe(
        Effect.flatMap((ids) =>
          client.request(
            "application_thread_find_read",
            { query: { kind: "revision", thread_ids: ids } },
            Schema.Number,
          ),
        ),
      ),
    withReadTransaction: client.withReadTransaction,
  } satisfies ThreadFindRows;
});

/** The caller supplies the native projected index so all find reads share its snapshot. */
export const make = Effect.fn("NativeThreadFind.make")(function* (readIndex: ReadThreadFindIndex) {
  return yield* makeThreadFindWithRows(readIndex, yield* makeRows);
});
