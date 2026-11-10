import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";

import { RedbClient } from "../persistence/RedbClient.ts";
import { ProjectRecord, ProjectRows, toProjectRowsError } from "./ProjectRows.ts";

const Written = Schema.Struct({ written: Schema.Literal(true) });

export const make = Effect.gen(function* () {
  const client = yield* RedbClient;
  while (!(yield* client.prepareProjects).prepared) yield* Effect.yieldNow;
  return ProjectRows.of({
    select: Effect.fn("ProjectRows.select")((input) => client.request("application_projects_select", {
      ...(input.projectId === undefined ? {} : { project_id: input.projectId }),
      ...(input.projectIds === undefined ? {} : { project_ids: input.projectIds }),
      ...(input.workspaceRoot === undefined ? {} : { workspace_root: input.workspaceRoot }),
      include_deleted: input.includeDeleted,
    }, Schema.Array(ProjectRecord)).pipe(Effect.mapError(toProjectRowsError("select")))),
    upsert: Effect.fn("ProjectRows.upsert")((row) => client.request("application_projects_upsert", { row }, Written).pipe(
      Effect.asVoid,
      Effect.mapError(toProjectRowsError("upsert")),
    )),
  });
});

export const layer = Layer.effect(ProjectRows, make);
