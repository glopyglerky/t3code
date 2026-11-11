import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";

import { RedbClient } from "../persistence/RedbClient.ts";
import { LinkedPullRequestThreads, listFromRows } from "./linkedThreads.ts";

const Row = Schema.Struct({
  id: Schema.String,
  projectId: Schema.String,
  title: Schema.String,
  archivedAt: Schema.NullOr(Schema.String),
  settled: Schema.Number,
  host: Schema.String,
  repository: Schema.String,
  number: Schema.Number,
  url: Schema.String,
});

export const make = Effect.gen(function* () {
  const client = yield* RedbClient;
  while (!(yield* client.prepareProjectionReads).prepared) yield* Effect.yieldNow;
  return LinkedPullRequestThreads.of({
    list: listFromRows((number) =>
      client.request(
        "application_projections_history",
        { query: { kind: "linked_pull_requests", number } },
        Schema.Array(Row),
      ),
    ),
  });
});

export const layer = Layer.effect(LinkedPullRequestThreads, make);
