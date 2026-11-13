import { ProjectId } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

/** Encoded database values. ProjectStore owns JSON decoding and application defaults. */
export const ProjectRecord = Schema.Struct({
  projectId: Schema.String,
  title: Schema.String,
  hasCustomTitle: Schema.Literals([0, 1]),
  workspaceRoot: Schema.String,
  defaultModelSelection: Schema.NullOr(Schema.String),
  defaultThreadEnvMode: Schema.NullOr(Schema.String),
  autoPull: Schema.Literals([0, 1]),
  faviconPath: Schema.NullOr(Schema.String),
  projectIcon: Schema.NullOr(Schema.String),
  scripts: Schema.String,
  pinnedAt: Schema.NullOr(Schema.String),
  archivedAt: Schema.NullOr(Schema.String),
  createdAt: Schema.String,
  updatedAt: Schema.String,
  deletedAt: Schema.NullOr(Schema.String),
});
export type ProjectRecord = typeof ProjectRecord.Type;

export const ProjectSelection = Schema.Struct({
  projectId: Schema.optional(ProjectId),
  projectIds: Schema.optional(Schema.Array(ProjectId)),
  workspaceRoot: Schema.optional(Schema.String),
  includeDeleted: Schema.Boolean,
});
export type ProjectSelection = typeof ProjectSelection.Type;

export class ProjectRowsError extends Schema.TaggedError<ProjectRowsError>()("ProjectRowsError", {
  operation: Schema.Literals(["select", "upsert"]),
  cause: Schema.Defect(),
}) {}

export const toProjectRowsError = (operation: "select" | "upsert") => (cause: unknown) =>
  new ProjectRowsError({ operation, cause });

export interface ProjectRowsShape {
  readonly select: (input: ProjectSelection) => Effect.Effect<ReadonlyArray<ProjectRecord>, ProjectRowsError>;
  readonly upsert: (row: ProjectRecord) => Effect.Effect<void, ProjectRowsError>;
}

export class ProjectRows extends Context.Service<ProjectRows, ProjectRowsShape>()(
  "t3/orchestration-v2/ProjectRows",
) {}
