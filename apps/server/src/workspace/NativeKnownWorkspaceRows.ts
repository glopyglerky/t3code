import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

import { RedbClient } from "../persistence/RedbClient.ts";
import { ProjectionLookupResult } from "../orchestration-v2/ProjectionLookupRows.ts";
import { KnownWorkspaceRows, KnownWorkspaceRowsError } from "./KnownWorkspaceRows.ts";

export const make = Effect.gen(function* () {
  const client = yield* RedbClient;
  while (!(yield* client.prepareProjectionReads).prepared) yield* Effect.yieldNow;
  return KnownWorkspaceRows.of({
    threadWorktrees: client
      .request(
        "application_projections_lookup",
        {
          query: { kind: "workspace_paths" },
        },
        ProjectionLookupResult,
      )
      .pipe(
        Effect.map(({ workspaces }) =>
          workspaces.map((row) => ({
            worktreePath: row.worktree_path,
            projectId: row.project_id,
          })),
        ),
        Effect.mapError((cause) => new KnownWorkspaceRowsError({ cause })),
      ),
  });
});

export const layer = Layer.effect(KnownWorkspaceRows, make);
