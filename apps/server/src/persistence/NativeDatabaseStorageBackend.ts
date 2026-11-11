// @effect-diagnostics nodeBuiltinImport:off -- Effect exposes no free-space query.
import * as NodeFS from "node:fs/promises";
import * as NodePath from "node:path";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as NativeDatabase from "../orchestration-v2/NativeDatabase.ts";
import { DatabaseStorageBackend, DatabaseStorageBackendError } from "./DatabaseStorageBackend.ts";

export const StorageStatistics = Schema.Struct({
  database_bytes: Schema.Number,
  reclaimable_bytes: Schema.Number,
  reusable_bytes: Schema.Number,
  fragmented_bytes: Schema.Number,
  allocated_bytes: Schema.Number,
});
export const nativeSpaceShortfall = (databasePath: string) =>
  Effect.tryPromise({
    try: async () => {
      const [file, disk] = await Promise.all([
        NodeFS.stat(databasePath),
        NodeFS.statfs(NodePath.dirname(databasePath)),
      ]);
      // Keep a verified backup and allow a complete rewrite on the same filesystem.
      const neededBytes = Math.ceil(file.size * 2.2);
      const freeBytes = disk.bavail * disk.bsize;
      return freeBytes < neededBytes ? { neededBytes, freeBytes } : null;
    },
    catch: (cause) => new DatabaseStorageBackendError({ cause }),
  });

export const make = Effect.gen(function* () {
  const database = yield* NativeDatabase.NativeDatabase;
  return DatabaseStorageBackend.of({
    inspect: database.request({ operation: "storage_statistics" }, StorageStatistics).pipe(
      Effect.map((stats) => ({
        databaseBytes: stats.database_bytes,
        reclaimableBytes: stats.reclaimable_bytes,
        incrementalVacuum: false,
        autoVacuum: 0,
      })),
      Effect.mapError((cause) => new DatabaseStorageBackendError({ cause })),
    ),
    // omdb reuses free pages online; shrinking requires the exclusive offline path.
    reclaim: Effect.void,
    spaceShortfall: nativeSpaceShortfall,
  });
});
export const layer = Layer.effect(DatabaseStorageBackend, make);
