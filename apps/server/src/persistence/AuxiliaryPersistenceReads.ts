import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

export const ProviderHistoryRow = Schema.Struct({
  providerName: Schema.String,
  providerInstanceId: Schema.NullOr(Schema.String),
});
export const LegacyProjectSettingsRow = Schema.Struct({
  projectId: Schema.String,
  defaultModelSelection: Schema.NullOr(Schema.String),
  defaultThreadEnvMode: Schema.NullOr(Schema.String),
  autoPull: Schema.Number,
  scripts: Schema.String,
});
export const ThreadWorktreeRow = Schema.Struct({ worktreePath: Schema.String });
export const DeletedThreadWorktreeRow = Schema.Struct({
  payload_json: Schema.String,
  workspaceRoot: Schema.String,
});
export const PendingCleanupRow = Schema.Struct({ status: Schema.String });
export const ProviderSessionRow = Schema.Struct({
  payload_json: Schema.String,
});

export class AuxiliaryPersistenceReadError extends Schema.TaggedError<AuxiliaryPersistenceReadError>()(
  "AuxiliaryPersistenceReadError",
  { operation: Schema.String, cause: Schema.Defect() },
) {}
export const toAuxiliaryReadError = (operation: string) => (cause: unknown) =>
  new AuxiliaryPersistenceReadError({ operation, cause });

export class AuxiliaryPersistenceReads extends Context.Service<
  AuxiliaryPersistenceReads,
  {
    readonly providerHistory: Effect.Effect<
      ReadonlyArray<typeof ProviderHistoryRow.Type>,
      AuxiliaryPersistenceReadError
    >;
    readonly legacyProjectSettings: Effect.Effect<
      ReadonlyArray<typeof LegacyProjectSettingsRow.Type>,
      AuxiliaryPersistenceReadError
    >;
    readonly allThreadWorktrees: Effect.Effect<
      ReadonlyArray<typeof ThreadWorktreeRow.Type>,
      AuxiliaryPersistenceReadError
    >;
    readonly deletedThreadWorktrees: Effect.Effect<
      ReadonlyArray<typeof DeletedThreadWorktreeRow.Type>,
      AuxiliaryPersistenceReadError
    >;
    readonly pendingThreadCleanup: (
      threadId: string,
    ) => Effect.Effect<ReadonlyArray<typeof PendingCleanupRow.Type>, AuxiliaryPersistenceReadError>;
    readonly nonStoppedProviderSessions: Effect.Effect<
      ReadonlyArray<typeof ProviderSessionRow.Type>,
      AuxiliaryPersistenceReadError
    >;
  }
>()("t3/persistence/AuxiliaryPersistenceReads") {}
