// @effect-diagnostics nodeBuiltinImport:off - Effect's realPath keeps the requested letter case.
import * as NodeFSP from "node:fs/promises";

import * as Effect from "effect/Effect";
import * as Path from "effect/Path";

import * as ServerConfig from "../config.ts";
import { isWithinRoot } from "../workspace/KnownWorkspaces.ts";

/** Builds a check for the server's own state files, which client file reads do not serve. */
export const makeIsServerInternalPath = Effect.gen(function* () {
  const config = yield* ServerConfig.ServerConfig;
  const path = yield* Path.Path;
  // Native realpath returns the on-disk letter case.
  const realPath = (value: string) =>
    Effect.promise(() => NodeFSP.realpath(value).catch(() => path.resolve(value)));
  const configuredDatabase = path.resolve(config.dbPath);
  return Effect.fn("ServerInternalPaths.isServerInternalPath")(function* (filePath: string) {
    const [file, stateDir, secretsDir, database, configuredDatabaseDirectory] = yield* Effect.all(
      [
        realPath(filePath),
        realPath(config.stateDir),
        realPath(config.secretsDir),
        realPath(config.dbPath),
        realPath(path.dirname(configuredDatabase)),
      ],
      { concurrency: "unbounded" },
    );
    const filename = path.basename(file);
    // Maintenance names backups beside the supplied path, which may be a symlink.
    const isDatabaseBackup = (databasePath: string) =>
      path.dirname(file) === path.dirname(databasePath) &&
      filename.startsWith(`${path.basename(databasePath)}.precompact-`) &&
      filename.endsWith(".omdb");
    return (
      file === database ||
      isDatabaseBackup(database) ||
      isDatabaseBackup(path.join(configuredDatabaseDirectory, path.basename(configuredDatabase))) ||
      isWithinRoot(path, file, secretsDir) ||
      (path.dirname(file) === stateDir &&
        (filename.includes(".sqlite") || filename.includes(".omdb"))) ||
      // macOS does not resolve `/dev/fd/N` to the open file's path.
      isWithinRoot(path, file, "/dev/fd")
    );
  });
});
