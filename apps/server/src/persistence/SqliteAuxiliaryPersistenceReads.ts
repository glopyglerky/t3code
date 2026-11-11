import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";

import {
  AuxiliaryPersistenceReads,
  toAuxiliaryReadError,
  type ProviderHistoryRow,
  type LegacyProjectSettingsRow,
  type ThreadWorktreeRow,
  type DeletedThreadWorktreeRow,
  type PendingCleanupRow,
  type ProviderSessionRow,
} from "./AuxiliaryPersistenceReads.ts";

export const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  return AuxiliaryPersistenceReads.of({
    providerHistory: sql<typeof ProviderHistoryRow.Type>`
      SELECT DISTINCT provider_name AS "providerName", provider_instance_id AS "providerInstanceId"
      FROM projection_thread_sessions WHERE provider_name IN ('cursor', 'grok', 'opencode')
      UNION
      SELECT DISTINCT provider_name AS "providerName", provider_instance_id AS "providerInstanceId"
      FROM provider_session_runtime WHERE provider_name IN ('cursor', 'grok', 'opencode')
    `.pipe(NodeSqliteClient.readOnly, Effect.mapError(toAuxiliaryReadError("providerHistory"))),
    legacyProjectSettings: sql<typeof LegacyProjectSettingsRow.Type>`
      SELECT project_id AS "projectId", default_model_selection_json AS "defaultModelSelection",
        default_thread_env_mode AS "defaultThreadEnvMode", auto_pull AS "autoPull", scripts_json AS "scripts"
      FROM projection_projects WHERE deleted_at IS NULL
    `.pipe(
      NodeSqliteClient.readOnly,
      Effect.mapError(toAuxiliaryReadError("legacyProjectSettings")),
    ),
    allThreadWorktrees: sql<typeof ThreadWorktreeRow.Type>`
      SELECT DISTINCT json_extract(payload_json, '$.worktreePath') AS "worktreePath"
      FROM orchestration_v2_projection_threads
      WHERE json_extract(payload_json, '$.worktreePath') IS NOT NULL
    `.pipe(NodeSqliteClient.readOnly, Effect.mapError(toAuxiliaryReadError("allThreadWorktrees"))),
    deletedThreadWorktrees: sql<typeof DeletedThreadWorktreeRow.Type>`
      SELECT t.payload_json, p.workspace_root AS "workspaceRoot"
      FROM orchestration_v2_projection_threads t JOIN projection_projects p ON p.project_id = t.project_id
      WHERE t.deleted_at IS NOT NULL
    `.pipe(
      NodeSqliteClient.readOnly,
      Effect.mapError(toAuxiliaryReadError("deletedThreadWorktrees")),
    ),
    pendingThreadCleanup: Effect.fn("AuxiliaryPersistenceReads.pendingThreadCleanup")((threadId) =>
      sql<typeof PendingCleanupRow.Type>`SELECT status FROM orchestration_v2_effect_outbox
        WHERE thread_id = ${threadId} AND status NOT IN ('succeeded', 'cancelled')
      `.pipe(
        NodeSqliteClient.readOnly,
        Effect.mapError(toAuxiliaryReadError("pendingThreadCleanup")),
      ),
    ),
    nonStoppedProviderSessions: sql<typeof ProviderSessionRow.Type>`
      SELECT payload_json FROM orchestration_v2_projection_provider_sessions WHERE status != 'stopped'
    `.pipe(
      NodeSqliteClient.readOnly,
      Effect.mapError(toAuxiliaryReadError("nonStoppedProviderSessions")),
    ),
  });
});

export const layer = Layer.effect(AuxiliaryPersistenceReads, make);
