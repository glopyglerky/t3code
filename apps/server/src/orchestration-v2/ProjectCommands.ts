import {
  type CommandId,
  type EventId,
  type ModelSelection,
  type ProjectIconOverride,
  ProjectId,
  type ProjectScript,
  type ThreadEnvMode,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Schema from "effect/Schema";

import type { ProjectRow } from "./ProjectStore.ts";

export interface ProjectCreateCommand {
  readonly type: "project.create";
  readonly commandId: CommandId;
  readonly projectId: ProjectId;
  readonly title: string;
  readonly workspaceRoot: string;
  readonly scripts?: ReadonlyArray<ProjectScript>;
}

export interface ProjectMetaUpdateCommand {
  readonly type: "project.meta.update";
  readonly commandId: CommandId;
  readonly projectId: ProjectId;
  readonly title?: string;
  readonly workspaceRoot?: string;
  readonly defaultModelSelection?: ModelSelection | null;
  readonly defaultThreadEnvMode?: ThreadEnvMode | null;
  readonly autoPull?: boolean;
  readonly faviconPath?: string | null;
  readonly projectIcon?: ProjectIconOverride | null;
  readonly scripts?: ReadonlyArray<ProjectScript>;
  readonly pinned?: boolean;
  readonly archived?: boolean;
}

export interface ProjectDeleteCommand {
  readonly type: "project.delete";
  readonly commandId: CommandId;
  readonly projectId: ProjectId;
}

export type ProjectCommand = ProjectCreateCommand | ProjectMetaUpdateCommand | ProjectDeleteCommand;

export class ProjectCommandInvariantError extends Schema.TaggedError<ProjectCommandInvariantError>()(
  "ProjectCommandInvariantError",
  {
    commandType: Schema.String,
    detail: Schema.String,
  },
) {
  override get message(): string {
    return `Project command invariant failed (${this.commandType}): ${this.detail}`;
  }
}

export class ProjectCommandMissingProjectError extends Schema.TaggedError<ProjectCommandMissingProjectError>()(
  "ProjectCommandMissingProjectError",
  {
    commandType: Schema.String,
    projectId: ProjectId,
  },
) {
  override get message(): string {
    return `Project '${this.projectId}' does not exist for command '${this.commandType}'.`;
  }
}

export class ProjectWorkspaceConflictError extends Schema.TaggedError<ProjectWorkspaceConflictError>()(
  "ProjectWorkspaceConflictError",
  {
    workspaceRoot: Schema.String,
    conflictingProjectId: ProjectId,
  },
) {
  override get message(): string {
    return `Active project '${this.conflictingProjectId}' already exists for workspace root '${this.workspaceRoot}'.`;
  }
}

export const ProjectCommandRejection = Schema.Union([
  ProjectCommandInvariantError,
  ProjectCommandMissingProjectError,
  ProjectWorkspaceConflictError,
]);
export type ProjectCommandRejection = typeof ProjectCommandRejection.Type;

const ProjectCommandRejectionJson = Schema.fromJsonString(ProjectCommandRejection);
export const encodeProjectCommandRejection = Schema.encodeSync(ProjectCommandRejectionJson);
export const decodeProjectCommandRejection = Schema.decodeUnknownOption(
  ProjectCommandRejectionJson,
);

export interface ProjectCommandState {
  readonly project: ProjectRow | undefined;
  readonly workspaceOwner: ProjectRow | undefined;
}

export interface ProjectPlanInput {
  readonly command: ProjectCommand;
  readonly state: ProjectCommandState;
  readonly eventId: EventId;
  readonly now: DateTime.Utc;
}
