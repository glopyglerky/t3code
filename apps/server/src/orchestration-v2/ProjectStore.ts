import {
  type ApplicationProjectEvent,
  IsoDateTime,
  ModelSelection,
  type OrchestrationProjectShell,
  StoredProjectIcon,
  ProjectId,
  ProjectScript,
  ThreadEnvMode,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import { ProjectRows, type ProjectSelection } from "./ProjectRows.ts";
import * as SqliteProjectRows from "./SqliteProjectRows.ts";

export class ProjectStoreV2Error extends Schema.TaggedError<ProjectStoreV2Error>()(
  "ProjectStoreV2Error",
  {
    operation: Schema.String,
    cause: Schema.Defect(),
  },
) {
  override get message(): string {
    return `Project store operation '${this.operation}' failed.`;
  }
}

export const ProjectRow = Schema.Struct({
  projectId: ProjectId,
  title: Schema.String,
  hasCustomTitle: Schema.Boolean,
  workspaceRoot: Schema.String,
  defaultModelSelection: Schema.NullOr(ModelSelection),
  defaultThreadEnvMode: Schema.NullOr(ThreadEnvMode),
  autoPull: Schema.Boolean,
  faviconPath: Schema.NullOr(Schema.String),
  projectIcon: Schema.NullOr(StoredProjectIcon),
  scripts: Schema.Array(ProjectScript),
  pinnedAt: Schema.NullOr(IsoDateTime),
  archivedAt: Schema.NullOr(IsoDateTime),
  createdAt: IsoDateTime,
  updatedAt: IsoDateTime,
  deletedAt: Schema.NullOr(IsoDateTime),
});
export type ProjectRow = typeof ProjectRow.Type;

const ProjectDbRow = Schema.Struct({
  ...ProjectRow.fields,
  defaultModelSelection: Schema.NullOr(Schema.fromJsonString(ModelSelection)),
  hasCustomTitle: Schema.BooleanFromBit,
  autoPull: Schema.BooleanFromBit,
  projectIcon: Schema.NullOr(Schema.fromJsonString(StoredProjectIcon)),
  scripts: Schema.fromJsonString(Schema.Array(ProjectScript)),
});

function toShell(row: ProjectRow): OrchestrationProjectShell {
  return {
    id: row.projectId,
    title: row.title,
    hasCustomTitle: row.hasCustomTitle,
    workspaceRoot: row.workspaceRoot,
    repositoryIdentity: null,
    defaultModelSelection: row.defaultModelSelection,
    defaultThreadEnvMode: row.defaultThreadEnvMode,
    autoPull: row.autoPull,
    faviconPath: row.faviconPath,
    projectIcon: row.projectIcon,
    scripts: row.scripts,
    pinnedAt: row.pinnedAt,
    archivedAt: row.archivedAt,
    createdAt: row.createdAt,
    updatedAt: row.updatedAt,
  };
}

export class ProjectStoreV2 extends Context.Service<
  ProjectStoreV2,
  {
    /** Call inside the commit transaction. */
    readonly apply: (event: ApplicationProjectEvent) => Effect.Effect<void, ProjectStoreV2Error>;
    readonly get: (
      projectId: ProjectId,
      options?: { readonly includeDeleted?: boolean },
    ) => Effect.Effect<Option.Option<ProjectRow>, ProjectStoreV2Error>;
    readonly list: (options?: {
      readonly projectIds?: ReadonlyArray<ProjectId>;
      readonly includeDeleted?: boolean;
    }) => Effect.Effect<ReadonlyArray<ProjectRow>, ProjectStoreV2Error>;
    /** Workspace roots match by exact string; callers normalize before asking. */
    readonly findActiveByWorkspaceRoot: (
      workspaceRoot: string,
    ) => Effect.Effect<Option.Option<ProjectRow>, ProjectStoreV2Error>;
    readonly getShell: (
      projectId: ProjectId,
    ) => Effect.Effect<Option.Option<OrchestrationProjectShell>, ProjectStoreV2Error>;
    readonly listShells: (options?: {
      readonly projectIds?: ReadonlyArray<ProjectId>;
    }) => Effect.Effect<ReadonlyArray<OrchestrationProjectShell>, ProjectStoreV2Error>;
  }
>()("t3/orchestration-v2/ProjectStore/ProjectStoreV2") {}

/** Native routes may select the retained reducer before sending an unsupported wire value. */
type ProjectEventReducer<E = never> = (
  event: ApplicationProjectEvent,
  existing: ProjectRow | null,
) => Effect.Effect<ProjectRow | null, E>;

class ProjectReducerMissingRow extends Schema.TaggedError<ProjectReducerMissingRow>()(
  "ProjectReducerMissingRow",
  {},
) {
  override get message(): string {
    return "Project reducer returned no row for an applicable event.";
  }
}

export const reduceEvent: ProjectEventReducer = Effect.fn("ProjectStoreV2.reduceEvent")(
  (event, row) =>
    Effect.sync(() => {
      if (event.type === "project.created") {
        const payload = event.payload;
        return {
          projectId: payload.projectId,
          title: payload.title,
          hasCustomTitle: false,
          workspaceRoot: payload.workspaceRoot,
          defaultModelSelection: payload.defaultModelSelection,
          defaultThreadEnvMode: payload.defaultThreadEnvMode ?? null,
          autoPull: false,
          faviconPath: payload.faviconPath ?? null,
          projectIcon: payload.projectIcon ?? null,
          scripts: payload.scripts,
          pinnedAt: null,
          archivedAt: null,
          createdAt: payload.createdAt,
          updatedAt: payload.updatedAt,
          deletedAt: null,
        };
      }
      if (row === null) return null;
      if (event.type === "project.deleted") {
        return {
          ...row,
          deletedAt: event.payload.deletedAt,
          updatedAt: event.payload.deletedAt,
        };
      }
      const payload = event.payload;
      return {
        ...row,
        ...(payload.title === undefined ? {} : { title: payload.title, hasCustomTitle: true }),
        ...(payload.workspaceRoot === undefined ? {} : { workspaceRoot: payload.workspaceRoot }),
        ...(payload.defaultModelSelection === undefined
          ? {}
          : { defaultModelSelection: payload.defaultModelSelection }),
        ...(payload.defaultThreadEnvMode === undefined
          ? {}
          : { defaultThreadEnvMode: payload.defaultThreadEnvMode }),
        ...(payload.autoPull === undefined ? {} : { autoPull: payload.autoPull }),
        ...(payload.faviconPath === undefined ? {} : { faviconPath: payload.faviconPath }),
        ...(payload.projectIcon === undefined ? {} : { projectIcon: payload.projectIcon }),
        ...(payload.scripts === undefined ? {} : { scripts: payload.scripts }),
        ...(payload.pinnedAt === undefined ? {} : { pinnedAt: payload.pinnedAt }),
        ...(payload.archivedAt === undefined ? {} : { archivedAt: payload.archivedAt }),
        updatedAt: payload.updatedAt,
      };
    }),
);

/** Shared reads, codecs, shell mapping and upsert ownership for both reducers. */
export const makeWithRowsAndReducer = Effect.fn("ProjectStoreV2.makeWithRowsAndReducer")(function* <
  E,
>(reduce: ProjectEventReducer<E>) {
  const rows = yield* ProjectRows;
  const encodeRow = Schema.encodeEffect(ProjectDbRow);
  const decodeRows = Schema.decodeUnknownEffect(Schema.Array(ProjectDbRow));
  const selectRows = (input: ProjectSelection) =>
    rows.select(input).pipe(Effect.flatMap(decodeRows));
  const upsertRow = (row: ProjectRow) => encodeRow(row).pipe(Effect.flatMap(rows.upsert));

  const mapError =
    (operation: string) =>
    <A, E, R>(effect: Effect.Effect<A, E, R>) =>
      effect.pipe(Effect.mapError((cause) => new ProjectStoreV2Error({ operation, cause })));

  const get: ProjectStoreV2["Service"]["get"] = (projectId, options) =>
    selectRows({ projectId, includeDeleted: options?.includeDeleted === true }).pipe(
      Effect.map((rows) => Option.fromUndefinedOr(rows[0])),
      mapError("get"),
    );

  const list: ProjectStoreV2["Service"]["list"] = (options) =>
    selectRows({
      ...(options?.projectIds === undefined ? {} : { projectIds: options.projectIds }),
      includeDeleted: options?.includeDeleted === true,
    }).pipe(mapError("list"));

  const findActiveByWorkspaceRoot: ProjectStoreV2["Service"]["findActiveByWorkspaceRoot"] = (
    workspaceRoot,
  ) =>
    selectRows({ workspaceRoot, includeDeleted: false }).pipe(
      Effect.map((rows) => Option.fromUndefinedOr(rows[0])),
      mapError("findActiveByWorkspaceRoot"),
    );

  const applyReduced = Effect.fnUntraced(function* (
    event: ApplicationProjectEvent,
    existing: ProjectRow | null,
  ) {
    const row = yield* reduce(event, existing);
    if (row === null) return yield* new ProjectReducerMissingRow();
    yield* upsertRow(row);
  }, mapError("apply"));

  const apply: ProjectStoreV2["Service"]["apply"] = Effect.fn("ProjectStoreV2.apply")(
    function* (event) {
      // Creation deliberately does not decode the old row. The existing upsert
      // owns sticky custom titles and can replace otherwise unused malformed JSON.
      if (event.type === "project.created") return yield* applyReduced(event, null);
      const existing = yield* get(event.payload.projectId, { includeDeleted: true });
      if (Option.isNone(existing)) return;
      yield* applyReduced(event, existing.value);
    },
  );

  return ProjectStoreV2.of({
    apply,
    get,
    list,
    findActiveByWorkspaceRoot,
    getShell: Effect.fn("ProjectStoreV2.getShell")(function (projectId) {
      return get(projectId).pipe(Effect.map(Option.map(toShell)));
    }),
    listShells: Effect.fn("ProjectStoreV2.listShells")(function (options) {
      return list(
        options?.projectIds === undefined ? undefined : { projectIds: options.projectIds },
      ).pipe(Effect.map((rows) => rows.map(toShell)));
    }),
  });
});

/** @public Shared row repository with the one retained TypeScript reducer. */
export const makeWithRows = makeWithRowsAndReducer(reduceEvent);

/** @public The existing construction path retains the SQLite repository. */
export const make = makeWithRows.pipe(Effect.provide(SqliteProjectRows.layer));

export const layer = Layer.effect(ProjectStoreV2, make);
