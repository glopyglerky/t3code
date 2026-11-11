import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Exit from "effect/Exit";
import * as HostProcess from "@t3tools/shared/HostProcess";
import { nativeExecutable } from "@t3tools/shared/nativeExecutable";
import * as NativeDatabase from "../orchestration-v2/NativeDatabase.ts";
import * as ServerConfig from "../config.ts";
import * as ProcessRunner from "../processRunner.ts";
import * as ServerOwnership from "../serverOwnership.ts";
import * as NativeServerOwnershipLock from "./NativeServerOwnershipLock.ts";
import * as RedbClient from "./RedbClient.ts";

export class OmdbStartupError extends Schema.TaggedError<OmdbStartupError>()("OmdbStartupError", {
  path: Schema.String,
  cause: Schema.Defect(),
}) {
  override get message() {
    return `Could not start the native database at ${this.path}. No fallback to SQLite was attempted.`;
  }
}

/** A running server already owns the state directory; CLI bootstrap acquires its own lease. */
export const BootstrapOwnership = Context.Reference<
  { readonly activate: Effect.Effect<void, OmdbStartupError> } | undefined
>("t3/persistence/Omdb/BootstrapOwnership", { defaultValue: () => undefined });
export const Bootstrap = Schema.Struct({
  initialized: Schema.Boolean,
  migrations: Schema.Array(Schema.Struct({ id: Schema.String, name: Schema.String })),
  schema_version: Schema.Number,
});
const Imported = Schema.Struct({
  generation: Schema.String,
  replayed: Schema.Boolean,
  summary: Schema.Struct({
    export_sha256: Schema.String,
    file_sha256: Schema.String,
    byte_count: Schema.Number,
    table_count: Schema.Number,
    row_count: Schema.Number,
  }),
  excluded_derived_tables: Schema.Array(Schema.String),
});
const ACTIVE_MARKER = "t3code-omdb-owner-v1\n";

export const make = Effect.fn("Omdb.make")(
  function* (options: { readonly executable: string; readonly databasePath: string }) {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const directory = path.dirname(options.databasePath);
    yield* fs.makeDirectory(directory, { recursive: true });
    const suppliedOwnership = yield* BootstrapOwnership;
    const markerPath = path.join(directory, "server-owner.sqlite");
    const marker = yield* fs.readFileString(markerPath).pipe(
      Effect.catchIf(
        (error) => error.reason._tag === "NotFound",
        () => Effect.succeed(""),
      ),
    );
    const leaseScope = yield* Effect.acquireRelease(Scope.make(), (scope) =>
      Scope.close(scope, Exit.void),
    );
    let activate = suppliedOwnership?.activate;
    let awaitLost: Effect.Effect<never, ServerOwnership.ServerOwnershipError> | undefined;
    if (activate === undefined && marker !== ACTIVE_MARKER) {
      // CLI cutover must use the same pre-lock PID and old boot-service checks as
      // server startup. A byte-range lock alone cannot exclude older releases
      // which never acquired that lock and can still write the SQLite source.
      const ownership = yield* ServerOwnership.acquireServerOwnershipWithLock(
        path.join(directory, "server-runtime.json"),
      ).pipe(
        Effect.provide(
          Layer.mergeAll(NativeServerOwnershipLock.layer(options.executable), ProcessRunner.layer),
        ),
        Effect.provideService(Scope.Scope, leaseScope),
      );
      if (ownership.activateNative === undefined)
        return yield* new OmdbStartupError({
          path: options.databasePath,
          cause: new Error("Could not establish migration ownership"),
        });
      activate = ownership.activateNative.pipe(
        Effect.mapError((cause) => new OmdbStartupError({ path: options.databasePath, cause })),
      );
      awaitLost = ownership.awaitLost;
    }
    const bootstrap = Effect.gen(function* () {
      const exists = yield* fs.exists(options.databasePath);
      if (!exists && marker === ACTIVE_MARKER)
        return yield* new OmdbStartupError({
          path: options.databasePath,
          cause: new Error(
            "The authoritative native database is missing; refusing to initialize an empty replacement",
          ),
        });
      let source: string | undefined;
      if (marker !== ACTIVE_MARKER) {
        for (const name of ["statev2.sqlite", "state.sqlite"]) {
          const candidate = path.join(directory, name);
          if (yield* fs.exists(candidate)) {
            source = candidate;
            break;
          }
        }
      }
      const loadMigrationAdapter = Effect.tryPromise({
        try: () => import("./migrateSqliteToOmdb.ts"),
        catch: (cause) => new OmdbStartupError({ path: options.databasePath, cause }),
      });
      if (exists && marker !== ACTIVE_MARKER) {
        // A failed first attempt may have imported a catalog without activating
        // the older-server fence. SQLite could then receive newer writes before
        // this retry. Inspect without opening a writable native handle first.
        const prior = yield* Effect.scoped(
          Effect.gen(function* () {
            const inspection = yield* NativeDatabase.make({
              ...options,
              mode: "read-only-local",
              requestTimeout: "15 minutes",
            });
            const schema = yield* inspection.request(
              { operation: "application_schema" },
              Schema.NullOr(RedbClient.ApplicationSchema),
            );
            const receipt = yield* inspection.request(
              { operation: "application_import" },
              Schema.NullOr(Imported),
            );
            return { schema, receipt };
          }),
        );
        if (prior.schema !== null && (prior.receipt !== null || source !== undefined)) {
          if (prior.receipt === null || source === undefined) {
            return yield* new OmdbStartupError({
              path: options.databasePath,
              cause: new Error(
                "Unactivated native candidate has no verifiable SQLite import source; both stores were retained without activation",
              ),
            });
          }
          const adapter = yield* loadMigrationAdapter;
          const currentDigest = yield* adapter.snapshotForNativeImport(source).pipe(
            Effect.map((snapshot) => snapshot.sha256),
            Effect.scoped,
          );
          if (currentDigest !== prior.receipt.summary.export_sha256) {
            return yield* new OmdbStartupError({
              path: options.databasePath,
              cause: new Error(
                "SQLite no longer matches the unactivated native candidate; both stores were retained without activation. Pending historical migrations can also generate new timestamps and require explicit recovery",
              ),
            });
          }
        }
      }
      const database = yield* NativeDatabase.make({
        ...options,
        mode: exists ? "local" : "initialize-local",
        requestTimeout: "15 minutes",
      });
      const schema = yield* database.request(
        { operation: "application_schema" },
        Schema.NullOr(RedbClient.ApplicationSchema),
      );
      if (schema === null && marker === ACTIVE_MARKER)
        return yield* new OmdbStartupError({
          path: options.databasePath,
          cause: new Error("The authoritative native database has no application catalog"),
        });
      if (schema === null) {
        if (source !== undefined) {
          const adapter = yield* loadMigrationAdapter;
          yield* Effect.scoped(
            Effect.gen(function* () {
              const exported = yield* adapter.snapshotForNativeImport(source);
              yield* database.request(
                { operation: "import_sqlite_application", path: exported.path },
                Imported,
              );
            }),
          );
        }
      }
      yield* database.request({ operation: "application_bootstrap" }, Bootstrap);
      const client = yield* RedbClient.make.pipe(
        Effect.provideService(NativeDatabase.NativeDatabase, database),
      );
      // Finish every resumable index preparation before making the cutover durable.
      // A failed preparation leaves the preserved SQLite source and the retryable
      // native candidate intact, without activating the older-server fence.
      for (const prepare of [
        client.prepareEvents,
        client.prepareReceipts,
        client.preparePositions,
        client.prepareProjects,
        client.prepareProjections,
        client.prepareOutbox,
        client.prepareViewedFiles,
        client.prepareAuth,
        client.preparePeers,
        client.prepareRuntimeState,
        client.prepareScheduled,
        client.prepareProjectionReads,
        client.prepareAuxiliary,
        client.prepareMessageRowids,
        client.prepareSearch,
        client.prepareLegacyV1,
        client.prepareMaintenance,
      ]) {
        while (!(yield* prepare).prepared) yield* Effect.yieldNow;
      }
      if (activate !== undefined) yield* activate;
      return { database, client };
    });
    const runtime = yield* awaitLost === undefined
      ? bootstrap
      : Effect.raceFirst(bootstrap, awaitLost);
    // CLI bootstrap must not keep a server ownership lease for its whole command
    // lifetime. A supplied running-server lease belongs to its outer scope.
    yield* Scope.close(leaseScope, Exit.void);
    return runtime;
  },
  (effect, options) =>
    effect.pipe(
      Effect.mapError((cause) => new OmdbStartupError({ path: options.databasePath, cause })),
    ),
);

export const layerFromPath = (databasePath: string, executable: string) =>
  Layer.unwrap(
    Effect.gen(function* () {
      const { database, client } = yield* make({ databasePath, executable });
      return Layer.merge(
        Layer.succeed(NativeDatabase.NativeDatabase, database),
        Layer.succeed(RedbClient.RedbClient, client),
      );
    }),
  );

export const layerConfig = Layer.unwrap(
  Effect.gen(function* () {
    const config = yield* ServerConfig.ServerConfig;
    const environment = yield* HostProcess.Environment;
    const executable = yield* Effect.try({
      try: () => nativeExecutable("db", environment.T3_CODE_LS_DB_EXECUTABLE),
      catch: (cause) => new OmdbStartupError({ path: config.dbPath, cause }),
    });
    return layerFromPath(config.dbPath, executable);
  }),
);
