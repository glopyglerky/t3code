import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

export class DatabaseStorageBackendError extends Schema.TaggedError<DatabaseStorageBackendError>()(
  "DatabaseStorageBackendError",
  { cause: Schema.Defect() },
) {}
export interface StorageAccounting {
  readonly databaseBytes: number;
  readonly reclaimableBytes: number;
  readonly incrementalVacuum: boolean;
  readonly autoVacuum: number;
}
export class DatabaseStorageBackend extends Context.Service<
  DatabaseStorageBackend,
  {
    readonly inspect: Effect.Effect<StorageAccounting, DatabaseStorageBackendError>;
    readonly reclaim: Effect.Effect<void, DatabaseStorageBackendError>;
    readonly spaceShortfall: (
      databasePath: string,
      liveBytes: number,
    ) => Effect.Effect<
      {
        readonly neededBytes: number;
        readonly freeBytes: number;
      } | null,
      DatabaseStorageBackendError
    >;
  }
>()("t3/persistence/DatabaseStorageBackend") {}
