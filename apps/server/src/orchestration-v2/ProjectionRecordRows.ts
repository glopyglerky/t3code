import type * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import type {
  ProjectionRecordField,
  ProjectionRecordFilter,
  ProjectionWindow,
} from "./ProjectionStore.ts";

export const projectionRecordFields = [
  "runs",
  "attempts",
  "nodes",
  "subagents",
  "providerSessions",
  "providerThreads",
  "providerTurns",
  "runtimeRequests",
  "messages",
  "plans",
  "turnItems",
  "checkpointScopes",
  "checkpoints",
  "contextHandoffs",
  "contextTransfers",
] as const satisfies ReadonlyArray<ProjectionRecordField>;

export type ProjectionRecordWindow = Pick<
  ProjectionWindow,
  "rowLimit" | "userTurnLimit" | "deferWork" | "anchorItemId" | "requiredRunId" | "forkCreatedAt"
>;

export interface ProjectionRecordQuery {
  readonly thread_id: string;
  readonly fields: ReadonlyArray<ProjectionRecordField>;
  readonly filter?: ProjectionRecordFilter | undefined;
  readonly window?: ProjectionRecordWindow | undefined;
}

export interface ProjectionRecordResult {
  readonly thread: string | null;
  readonly rows: Readonly<Record<ProjectionRecordField, ReadonlyArray<string>>>;
  readonly messageOrdinals: ReadonlyArray<{ readonly messageId: string; readonly ordinal: number }>;
  readonly deferredWork: ReadonlyArray<{
    readonly rowIndex: number;
    readonly itemId: string;
    readonly lastItemId: string;
    readonly lastOrdinal: number;
    readonly itemCount: number;
  }>;
}

export class ProjectionRecordRowsError extends Schema.TaggedError<ProjectionRecordRowsError>()(
  "ProjectionRecordRowsError",
  { cause: Schema.Defect() },
) {}

/** Raw selected records from one snapshot, before domain codecs and message ordering. */
export interface ProjectionRecordRows {
  readonly read: (
    query: ProjectionRecordQuery,
  ) => Effect.Effect<ProjectionRecordResult, ProjectionRecordRowsError>;
}

export function emptyProjectionRecordRows(): Record<ProjectionRecordField, ReadonlyArray<string>> {
  return {
    runs: [],
    attempts: [],
    nodes: [],
    subagents: [],
    providerSessions: [],
    providerThreads: [],
    providerTurns: [],
    runtimeRequests: [],
    messages: [],
    plans: [],
    turnItems: [],
    checkpointScopes: [],
    checkpoints: [],
    contextHandoffs: [],
    contextTransfers: [],
  };
}
