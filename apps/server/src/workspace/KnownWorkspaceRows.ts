import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";

export class KnownWorkspaceRowsError extends Schema.TaggedError<KnownWorkspaceRowsError>()(
  "KnownWorkspaceRowsError",
  { cause: Schema.Defect() },
) {}

export class KnownWorkspaceRows extends Context.Service<
  KnownWorkspaceRows,
  {
    readonly threadWorktrees: Effect.Effect<
      ReadonlyArray<{
        readonly worktreePath: string;
        readonly projectId: string;
      }>,
      KnownWorkspaceRowsError
    >;
  }
>()("t3/workspace/KnownWorkspaceRows") {}

export const layerSqlite = Layer.effect(
  KnownWorkspaceRows,
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    return KnownWorkspaceRows.of({
      threadWorktrees: sql<{ readonly worktreePath: string; readonly projectId: string }>`
      SELECT DISTINCT
        json_extract(payload_json, '$.worktreePath') AS "worktreePath",
        project_id AS "projectId"
      FROM orchestration_v2_projection_threads
      WHERE deleted_at IS NULL AND json_extract(payload_json, '$.worktreePath') IS NOT NULL
    `.pipe(
        NodeSqliteClient.readOnly,
        Effect.mapError((cause) => new KnownWorkspaceRowsError({ cause })),
      ),
    });
  }),
);
