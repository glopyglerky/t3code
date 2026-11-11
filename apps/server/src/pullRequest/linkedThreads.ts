import {
  normalizeThreadPullRequestKey,
  threadPullRequestKeysEqual,
} from "../sourceControl/threadPullRequests.ts";
import {
  PullRequestLinkedThreadsResult,
  PullRequestOperationError,
  type ThreadPullRequestKey,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Context from "effect/Context";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";

const decodeLinkedThreads = Schema.decodeUnknownEffect(PullRequestLinkedThreadsResult);

export interface LinkedPullRequestThreadRow {
  readonly id: string;
  readonly projectId: string;
  readonly title: string;
  readonly archivedAt: string | null;
  readonly settled: number;
  readonly host: string;
  readonly repository: string;
  readonly number: number;
  readonly url: string;
}

export class LinkedPullRequestThreads extends Context.Service<
  LinkedPullRequestThreads,
  {
    readonly list: (
      input: ThreadPullRequestKey,
    ) => Effect.Effect<PullRequestLinkedThreadsResult, PullRequestOperationError>;
  }
>()("t3/pullRequest/linkedThreads/LinkedPullRequestThreads") {}

/** Canonical identity comparison remains shared with source-control link handling. */
export function listFromRows<E>(
  readRows: (number: number) => Effect.Effect<readonly LinkedPullRequestThreadRow[], E>,
): LinkedPullRequestThreads["Service"]["list"] {
  return Effect.fn("listLinkedPullRequestThreads")(
    function* (input: ThreadPullRequestKey) {
      const key = normalizeThreadPullRequestKey(input);
      const rows = yield* readRows(key.number);
      const threads = rows
        .filter((row) => threadPullRequestKeysEqual(row, key))
        .map(({ id, projectId, title, archivedAt, settled }) => ({
          id,
          projectId,
          title,
          archivedAt,
          settled: settled === 1,
        }));
      return yield* decodeLinkedThreads({ threads });
    },
    Effect.mapError(
      (cause) =>
        new PullRequestOperationError({
          operation: "linkedThreads",
          detail: "Could not load linked threads.",
          cause,
        }),
    ),
  );
}

export const makeSqlite = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  return LinkedPullRequestThreads.of({
    list: listFromRows((number) =>
      sql<LinkedPullRequestThreadRow>`
      SELECT t.thread_id AS id, t.project_id AS "projectId", t.title,
        t.archived_at AS "archivedAt",
        (json_extract(t.payload_json, '$.settledAt') IS NOT NULL
          OR json_extract(t.payload_json, '$.settledOverride') IS 'settled') AS settled,
        json_extract(link.value, '$.host') AS host,
        json_extract(link.value, '$.repository') AS repository,
        json_extract(link.value, '$.number') AS number,
        json_extract(link.value, '$.url') AS url,
        t.updated_at AS "updatedAt"
      FROM orchestration_v2_projection_threads AS t
      JOIN json_each(t.payload_json, '$.pullRequests') AS link
      WHERE json_extract(link.value, '$.number') = ${number}
        AND json_extract(link.value, '$.source') != 'stack-dismissed'
        AND t.deleted_at IS NULL
        AND json_extract(t.payload_json, '$.lineage.sideChat') IS NOT 1
      ORDER BY "updatedAt" DESC, id ASC
    `.pipe(NodeSqliteClient.readOnly),
    ),
  });
});

export const layerSqlite = Layer.effect(LinkedPullRequestThreads, makeSqlite);

/** Retained SQL entry point for existing migration fixtures and callers. */
export const listLinkedPullRequestThreads = Effect.fn("listLinkedPullRequestThreads")(function* (
  input: ThreadPullRequestKey,
) {
  const service = yield* makeSqlite;
  return yield* service.list(input);
});
