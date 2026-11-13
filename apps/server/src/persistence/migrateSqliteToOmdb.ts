// @effect-diagnostics nodeBuiltinImport:off -- Isolated, offline SQLite migration adapter.
import * as NodeSqlite from "node:sqlite";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Schema from "effect/Schema";
import * as Path from "effect/Path";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import { exportSqliteState } from "./sqliteExport.mjs";
import { runMigrations } from "./Migrations.ts";

export class SqliteNativeMigrationError extends Schema.TaggedError<SqliteNativeMigrationError>()(
  "SqliteNativeMigrationError",
  { path: Schema.String, cause: Schema.Defect() },
) {}

/** All SQL migrations run on a private backup. Never mutate the retained source. */
export const snapshotForNativeImport = Effect.fn("snapshotForNativeImport")(function* (
  source: string,
  options: { readonly migrate?: boolean } = {},
) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const directory = yield* fs.makeTempDirectoryScoped({
    directory: path.dirname(source),
    prefix: ".omdb-import-",
  });
  const copied = path.join(directory, "migrated.sqlite");
  const exported = path.join(directory, "native.ndjson");
  yield* Effect.tryPromise({
    try: async () => {
      const database = new NodeSqlite.DatabaseSync(source, {
        readOnly: true,
        allowExtension: false,
      });
      try {
        await NodeSqlite.backup(database, copied);
      } finally {
        database.close();
      }
    },
    catch: (cause) => new SqliteNativeMigrationError({ path: source, cause }),
  });
  if (options.migrate !== false) {
    yield* runMigrations().pipe(Effect.provide(NodeSqliteClient.layer({ filename: copied })));
  }
  const summary = yield* Effect.try({
    try: () => exportSqliteState({ source: copied, destination: exported }),
    catch: (cause) => new SqliteNativeMigrationError({ path: source, cause }),
  });
  return { path: exported, sha256: summary.sha256 };
});
