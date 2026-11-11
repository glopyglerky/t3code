import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as SecureRandom from "effect/crypto/SecureRandom";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";
import * as NativeDatabase from "../orchestration-v2/NativeDatabase.ts";
import * as NativeProjectionMaintenance from "../orchestration-v2/NativeProjectionMaintenance.ts";
import * as ProjectionMaintenance from "../orchestration-v2/ProjectionMaintenance.ts";
import * as Maintenance from "./DatabaseMaintenance.ts";
import * as RedbClient from "./RedbClient.ts";
import { StorageStatistics, nativeSpaceShortfall } from "./NativeDatabaseStorageBackend.ts";

const Backup = Schema.Struct({
  rows: Schema.Number,
  bytes: Schema.Number,
  checksum: Schema.String,
});
const Compaction = Schema.Struct({
  before_bytes: Schema.Number,
  after_bytes: Schema.Number,
});
const busy = (cause: unknown) =>
  Schema.is(NativeDatabase.NativeDatabaseRejected)(cause) && cause.kind === "writer_busy";

export const make = Effect.fn("NativeDatabaseMaintenance.make")(function* (executable: string) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const random = yield* SecureRandom.SecureRandom;
  const locate = (databasePath: string) =>
    fs
      .exists(databasePath)
      .pipe(
        Effect.flatMap((exists) =>
          exists
            ? Effect.void
            : Effect.fail(new Maintenance.DatabaseMissingError({ path: databasePath })),
        ),
      );
  const inspect = Effect.fn("NativeDatabaseMaintenance.inspect")(
    function* (databasePath: string) {
      yield* locate(databasePath);
      const database = yield* NativeDatabase.make({ executable, databasePath });
      const stats = yield* database.request({ operation: "storage_statistics" }, StorageStatistics);
      return {
        databaseBytes: stats.database_bytes,
        reclaimableBytes: stats.reclaimable_bytes,
        incrementalVacuumEnabled: false,
        legacyDatabaseBytes: null,
      };
    },
    (effect, databasePath) =>
      effect.pipe(
        Effect.scoped,
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
        Effect.mapError((cause) =>
          Schema.is(Maintenance.DatabaseMissingError)(cause)
            ? cause
            : new Maintenance.DatabaseInspectError({
                path: databasePath,
                cause,
              }),
        ),
      ),
  );
  const compact = Effect.fn("NativeDatabaseMaintenance.compact")(
    function* (databasePath: string) {
      yield* locate(databasePath);
      const database = yield* NativeDatabase.make({
        executable,
        databasePath,
        mode: "exclusive-local",
        requestTimeout: "30 minutes",
      });
      const before = yield* database.request(
        { operation: "storage_statistics" },
        StorageStatistics,
      );
      const shortfall = yield* nativeSpaceShortfall(databasePath);
      if (shortfall !== null)
        return yield* new Maintenance.DatabaseCompactError({
          path: databasePath,
          cause: new Error("insufficient backup and compaction space"),
        });
      const id = yield* random.uuidV4();
      const destination = path.join(
        path.dirname(databasePath),
        `${path.basename(databasePath)}.precompact-${id}.omdb`,
      );
      // The verified create-new backup precedes index preparation and history deletion.
      yield* database.request({ operation: "storage_backup", destination }, Backup);
      const client = yield* RedbClient.make.pipe(
        Effect.provideService(NativeDatabase.NativeDatabase, database),
      );
      const maintenance = yield* ProjectionMaintenance.ProjectionMaintenanceV2.pipe(
        Effect.provide(NativeProjectionMaintenance.layer),
        Effect.provideService(RedbClient.RedbClient, client),
      );
      const pruned = yield* maintenance.pruneEventStore();
      const compacted = yield* database.request({ operation: "storage_compact" }, Compaction);
      return {
        beforeBytes: before.database_bytes,
        afterBytes: compacted.after_bytes,
        deletedEventCount: pruned.deletedEventCount,
        deletedReceiptCount: pruned.deletedReceiptCount,
      };
    },
    (effect, databasePath) =>
      effect.pipe(
        Effect.scoped,
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
        Effect.mapError((cause) =>
          Schema.is(Maintenance.DatabaseMissingError)(cause)
            ? cause
            : busy(cause)
              ? new Maintenance.DatabaseInUseError({
                  path: databasePath,
                  cause,
                })
              : new Maintenance.DatabaseCompactError({
                  path: databasePath,
                  cause,
                }),
        ),
      ),
  );
  return Maintenance.DatabaseMaintenance.of({
    inspect,
    compact,
    // Migration sources and rollback copies are not pre-0.1.0 garbage. Native
    // maintenance must never delete them through the old database-removal API.
    removeLegacyDatabase: Effect.fn("NativeDatabaseMaintenance.removeLegacyDatabase")(
      (databasePath) =>
        Effect.fail(
          new Maintenance.LegacyDatabaseRemoveError({
            path: databasePath,
            cause: new Error("SQLite migration source and rollback backups are retained"),
          }),
        ),
    ),
  });
});
export const layer = (executable: string) =>
  Layer.effect(Maintenance.DatabaseMaintenance, make(executable));
