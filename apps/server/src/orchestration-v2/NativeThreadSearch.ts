import { ProjectId, ThreadId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";

import { RedbClient } from "../persistence/RedbClient.ts";
import * as Search from "./ThreadSearch.ts";
import type { ThreadSearchRows } from "./ThreadSearchRows.ts";

const TokenList = Schema.fromJsonString(Schema.Array(Schema.String));
const OpenRequest = Schema.Struct({ threadIds: TokenList });
const TitleRequest = Schema.Struct({ projectId: Schema.NullOr(ProjectId) });
const OlderTitleRequest = Schema.Struct({
  before: Schema.String,
  projectIds: Schema.NullOr(TokenList),
});
const OlderIdsRequest = Schema.Struct({
  before: Schema.String,
  threadIds: TokenList,
  projectIds: Schema.NullOr(TokenList),
});

export const makeRows = Effect.gen(function* () {
  const client = yield* RedbClient;
  while (!(yield* client.prepareProjectionReads).prepared) yield* Effect.yieldNow;
  while (!(yield* client.prepareMessageRowids).prepared) yield* Effect.yieldNow;
  while (!(yield* client.prepareSearch).prepared) yield* Effect.yieldNow;
  const query = <A>(query: Readonly<Record<string, unknown>>, schema: Schema.Decoder<A>) =>
    client
      .request("application_thread_search_read", { query }, Schema.Unknown)
      .pipe(Effect.flatMap(Schema.decodeUnknownEffect(schema)));
  return {
    hasIndex: query({ kind: "has_index" }, Schema.Boolean),
    searchRows: (input) =>
      Schema.encodeEffect(Search.SearchRequest)(input).pipe(
        Effect.flatMap((request) =>
          query(
            {
              kind: "message_hits",
              match: request.match,
              substrings: request.substrings,
              short_spellings: request.shortSpellings,
              project_id: request.projectId,
              excluded_thread_ids: request.excludedThreadIds,
            },
            Schema.Array(Search.SearchRow),
          ),
        ),
      ),
    threadOpens: (input) =>
      Schema.encodeEffect(OpenRequest)(input).pipe(
        Effect.flatMap(({ threadIds }) =>
          query(
            { kind: "thread_opens", thread_ids: threadIds },
            Schema.Array(Search.ThreadOpensRow),
          ),
        ),
      ),
    titleCandidates: (input) =>
      Schema.encodeEffect(TitleRequest)(input).pipe(
        Effect.flatMap(({ projectId }) =>
          query(
            { kind: "title_candidates", project_id: projectId },
            Schema.Array(Search.TitleCandidateRow),
          ),
        ),
      ),
    olderSettledTitleCandidates: (input) =>
      Schema.encodeEffect(OlderTitleRequest)(input).pipe(
        Effect.flatMap(({ before, projectIds }) =>
          query(
            { kind: "older_settled_titles", before, project_ids: projectIds },
            Schema.Array(Search.OlderSettledTitleRow),
          ),
        ),
      ),
    olderSettledIds: (input) =>
      Schema.encodeEffect(OlderIdsRequest)(input).pipe(
        Effect.flatMap(({ before, threadIds, projectIds }) =>
          query(
            { kind: "older_settled_ids", before, thread_ids: threadIds, project_ids: projectIds },
            Schema.Array(Schema.Struct({ id: ThreadId })),
          ),
        ),
      ),
    withReadTransaction: client.withReadTransaction,
  } satisfies ThreadSearchRows;
});

export const make = makeRows.pipe(Effect.flatMap(Search.makeWithRows));
export const layer = Layer.effect(Search.ThreadSearch, make);
