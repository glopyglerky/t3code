import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";
import * as SqlSchema from "effect/sql/SqlSchema";

import { ProjectRecord, ProjectRows, ProjectSelection, toProjectRowsError } from "./ProjectRows.ts";

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const selectRows = SqlSchema.findAll({
    Request: ProjectSelection,
    Result: ProjectRecord,
    execute: (request) => sql`
      SELECT
        project_id AS "projectId",
        title,
        has_custom_title AS "hasCustomTitle",
        workspace_root AS "workspaceRoot",
        default_model_selection_json AS "defaultModelSelection",
        default_thread_env_mode AS "defaultThreadEnvMode",
        auto_pull AS "autoPull",
        favicon_path AS "faviconPath",
        project_icon_json AS "projectIcon",
        scripts_json AS "scripts",
        pinned_at AS "pinnedAt",
        archived_at AS "archivedAt",
        created_at AS "createdAt",
        updated_at AS "updatedAt",
        deleted_at AS "deletedAt"
      FROM projection_projects
      WHERE ${sql.and([
        ...(request.includeDeleted ? [] : [sql`deleted_at IS NULL`]),
        ...(request.projectId === undefined ? [] : [sql`project_id = ${request.projectId}`]),
        ...(request.projectIds === undefined ? [] : [sql.in("project_id", request.projectIds)]),
        ...(request.workspaceRoot === undefined
          ? []
          : [sql`workspace_root = ${request.workspaceRoot}`]),
      ])}
      ORDER BY created_at ASC, project_id ASC
    `,
  });

  return ProjectRows.of({
    select: Effect.fn("ProjectRows.select")((input) =>
      selectRows(input).pipe(
        NodeSqliteClient.readOnly,
        Effect.mapError(toProjectRowsError("select")),
      ),
    ),
    upsert: Effect.fn("ProjectRows.upsert")((encoded) =>
      sql`
      INSERT INTO projection_projects (
        project_id, title, has_custom_title, workspace_root, default_model_selection_json, default_thread_env_mode,
        auto_pull, favicon_path, project_icon_json, scripts_json, pinned_at, archived_at,
        created_at, updated_at, deleted_at
      )
      VALUES (
        ${encoded.projectId}, ${encoded.title}, ${encoded.hasCustomTitle}, ${encoded.workspaceRoot}, ${encoded.defaultModelSelection},
        ${encoded.defaultThreadEnvMode}, ${encoded.autoPull}, ${encoded.faviconPath}, ${encoded.projectIcon},
        ${encoded.scripts}, ${encoded.pinnedAt}, ${encoded.archivedAt}, ${encoded.createdAt},
        ${encoded.updatedAt}, ${encoded.deletedAt}
      )
      ON CONFLICT (project_id)
      DO UPDATE SET
        title = CASE WHEN projection_projects.has_custom_title = 1 AND excluded.has_custom_title = 0
          THEN projection_projects.title ELSE excluded.title END,
        has_custom_title = max(projection_projects.has_custom_title, excluded.has_custom_title),
        workspace_root = excluded.workspace_root,
        default_model_selection_json = excluded.default_model_selection_json,
        default_thread_env_mode = excluded.default_thread_env_mode,
        auto_pull = excluded.auto_pull,
        favicon_path = excluded.favicon_path,
        project_icon_json = excluded.project_icon_json,
        scripts_json = excluded.scripts_json,
        pinned_at = excluded.pinned_at,
        archived_at = excluded.archived_at,
        created_at = excluded.created_at,
        updated_at = excluded.updated_at,
        deleted_at = excluded.deleted_at
    `.pipe(Effect.asVoid, Effect.mapError(toProjectRowsError("upsert"))),
    ),
  });
});

export const layer = Layer.effect(ProjectRows, make);
