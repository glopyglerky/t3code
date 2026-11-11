import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as SqlClient from "effect/sql/SqlClient";
import * as DatabaseMaintenance from "./DatabaseMaintenance.ts";
import { DatabaseStorageBackend, DatabaseStorageBackendError } from "./DatabaseStorageBackend.ts";

export const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  let layout: { pageSize: number; autoVacuum: number; incrementalVacuum: boolean } | undefined;
  const readLayout = Effect.suspend(() =>
    layout === undefined
      ? Effect.gen(function* () {
          const size = yield* sql<{
            readonly page_size: number;
          }>`PRAGMA page_size`;
          const mode = yield* sql<{
            readonly auto_vacuum: number;
          }>`PRAGMA auto_vacuum`;
          const autoVacuum = mode[0]?.auto_vacuum ?? 0;
          layout = {
            pageSize: size[0]?.page_size ?? 0,
            autoVacuum,
            incrementalVacuum: autoVacuum === 2,
          };
          return layout;
        })
      : Effect.succeed(layout),
  );
  const error = (cause: unknown) => new DatabaseStorageBackendError({ cause });
  return DatabaseStorageBackend.of({
    inspect: Effect.gen(function* () {
      const current = yield* readLayout;
      const pages = yield* sql<{
        readonly page_count: number;
      }>`PRAGMA page_count`;
      const free = yield* sql<{
        readonly freelist_count: number;
      }>`PRAGMA freelist_count`;
      return {
        databaseBytes: (pages[0]?.page_count ?? 0) * current.pageSize,
        reclaimableBytes: (free[0]?.freelist_count ?? 0) * current.pageSize,
        incrementalVacuum: current.incrementalVacuum,
        autoVacuum: current.autoVacuum,
      };
    }).pipe(Effect.mapError(error)),
    reclaim: Effect.gen(function* () {
      if (!(yield* readLayout).incrementalVacuum) return;
      // Preserve upstream's bounded one-page statements and yielding cadence.
      for (let page = 0; page < 1024; page++) {
        const free = yield* sql<{
          readonly freelist_count: number;
        }>`PRAGMA freelist_count`;
        if ((free[0]?.freelist_count ?? 0) === 0) break;
        yield* sql`PRAGMA incremental_vacuum(1)`;
        yield* Effect.yieldNow;
      }
    }).pipe(Effect.mapError(error)),
    spaceShortfall: Effect.fn("SqliteDatabaseStorageBackend.spaceShortfall")(
      (databasePath, liveBytes) =>
        DatabaseMaintenance.reclaimSpaceShortfall(databasePath, liveBytes).pipe(
          Effect.provideService(FileSystem.FileSystem, fs),
          Effect.provideService(Path.Path, path),
          Effect.mapError(error),
        ),
    ),
  });
});
export const layer = Layer.effect(DatabaseStorageBackend, make);
