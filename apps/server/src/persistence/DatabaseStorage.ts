import { DatabaseReclaimError, type DatabaseStorageStatus } from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Equal from "effect/Equal";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Schedule from "effect/Schedule";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";

import { resolveHostCliCommand } from "../cli/invocation.ts";
import * as ServerConfig from "../config.ts";
import * as ProjectionMaintenance from "../orchestration-v2/ProjectionMaintenance.ts";
import * as DatabaseMaintenance from "./DatabaseMaintenance.ts";
import { forkParked } from "../serverActivation.ts";
import { DatabaseStorageBackend } from "./DatabaseStorageBackend.ts";
import * as SqliteDatabaseStorageBackend from "./SqliteDatabaseStorageBackend.ts";

const COMPACTION_INTERVAL = Duration.hours(6);

export class DatabaseStorage extends Context.Service<
  DatabaseStorage,
  {
    /** Return free pages to the filesystem on databases with incremental auto_vacuum. */
    readonly reclaim: Effect.Effect<void>;
    /** Current file accounting, re-published after each maintenance pass. Null until first read. */
    readonly status: Stream.Stream<DatabaseStorageStatus | null>;
    /** Checks free space, then stops the server; its shutdown runs the offline repair. */
    readonly requestReclaim: Effect.Effect<void, DatabaseReclaimError>;
  }
>()("t3/persistence/DatabaseStorage") {}

/** Stops this server so its desktop app or background service starts it again; null when run by hand. */
export const ServerRestart = Context.Reference<Effect.Effect<void> | null>(
  "t3/persistence/DatabaseStorage/ServerRestart",
  { defaultValue: () => null },
);

const formatGiB = (bytes: number) => `${(bytes / 1024 ** 3).toFixed(1)} GiB`;

export const makeWithBackend = Effect.gen(function* () {
  const storage = yield* DatabaseStorageBackend;
  const maintenance = yield* ProjectionMaintenance.ProjectionMaintenanceV2;
  const config = yield* ServerConfig.ServerConfig;
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const statusRef = yield* SubscriptionRef.make<DatabaseStorageStatus | null>(null);
  const restart = yield* ServerRestart;
  const reclaimFiles = DatabaseMaintenance.reclaimFiles(path, config.dbPath);
  let reclaimFailure = Option.getOrUndefined(
    yield* fs.readFileString(reclaimFiles.failure).pipe(Effect.option),
  );

  // The notice shows host-shell quoting verbatim; desktop needs its launcher because t3 is absent from PATH.
  const { command: compactCommand } = yield* resolveHostCliCommand("doctor", [
    "--base-dir",
    config.baseDir,
  ]);
  // doctor --base-dir targets userdata, not implicit dev homes; suppress status to avoid touching the live install.
  const doctorTargetsThisDatabase =
    path.resolve(config.dbPath) ===
    path.resolve(config.baseDir, "userdata", path.basename(config.dbPath));
  const readLayout = storage.inspect;
  const readPages = storage.inspect;
  const publishStatus = Effect.gen(function* () {
    if (!doctorTargetsThisDatabase) return;
    const { databaseBytes, reclaimableBytes, incrementalVacuum } = yield* readPages;
    const next: DatabaseStorageStatus = {
      databaseBytes,
      reclaimableBytes,
      incrementalVacuumEnabled: incrementalVacuum,
      compactCommand,
      canReclaim: restart !== null,
      ...(reclaimFailure === undefined ? {} : { reclaimFailure }),
    };
    // Every set reaches each open Storage page; most passes change nothing.
    if (!Equal.equals(yield* SubscriptionRef.get(statusRef), next)) {
      yield* SubscriptionRef.set(statusRef, next);
    }
  }).pipe(
    Effect.catch((cause) => Effect.logWarning("Database storage inspection failed", { cause })),
  );

  const reclaim = storage.reclaim.pipe(
    Effect.catch((cause) => Effect.logWarning("Database space reclamation failed", { cause })),
  );

  // Even a pass that deletes nothing scans the event store. Persist completion
  // so restarting does not trigger another scan before the six-hour interval.
  const compactedAtPath = path.join(path.dirname(config.dbPath), "compacted-at");
  const compact = Effect.gen(function* () {
    let sliceStarted = yield* Clock.monotonicTimeNanos;
    const betweenPages = Effect.gen(function* () {
      const elapsed = (yield* Clock.monotonicTimeNanos) - sliceStarted;
      if (elapsed < 25_000_000n) return;
      // Keep background maintenance at 10% duty, including pages that exceed the budget.
      yield* Effect.sleep((Number(elapsed) / 1_000_000) * 9);
      sliceStarted = yield* Clock.monotonicTimeNanos;
    });
    const result = yield* maintenance.pruneEventStore(betweenPages);
    const completedAt = yield* Clock.currentTimeMillis;
    yield* fs.writeFileString(compactedAtPath, String(completedAt)).pipe(Effect.ignore);
    if (result.deletedEventCount > 0 || result.deletedReceiptCount > 0) {
      yield* Effect.logInfo("Compacted orchestration history", result);
    }
  }).pipe(
    Effect.catch((cause) =>
      Effect.logWarning("Orchestration history compaction failed", { cause }),
    ),
  );
  const lastCompactedAt = yield* fs.readFileString(compactedAtPath).pipe(
    Effect.map(Number),
    Effect.orElseSucceed(() => Number.NaN),
  );
  const intervalMs = Duration.toMillis(COMPACTION_INTERVAL);
  const untilDueMs = Number.isFinite(lastCompactedAt)
    ? lastCompactedAt + intervalMs - (yield* Clock.currentTimeMillis)
    : 0;
  const firstCompactionDelay = Duration.millis(
    Math.min(intervalMs, Math.max(Duration.toMillis(Duration.minutes(1)), untilDueMs)),
  );

  yield* forkParked(
    Effect.sleep(firstCompactionDelay).pipe(
      Effect.andThen(
        compact.pipe(
          Effect.andThen(publishStatus),
          Effect.repeat(Schedule.spaced(COMPACTION_INTERVAL)),
        ),
      ),
    ),
  );
  yield* forkParked(
    publishStatus.pipe(
      Effect.andThen(Effect.sleep("1 minute")),
      Effect.andThen(
        reclaim.pipe(Effect.andThen(publishStatus), Effect.repeat(Schedule.spaced("1 minute"))),
      ),
    ),
  );

  const requestReclaim = Effect.gen(function* () {
    if (restart === null || !doctorTargetsThisDatabase) {
      return yield* new DatabaseReclaimError({
        reason: `Stop T3 Code on this machine, then run: ${compactCommand}`,
      });
    }
    // Only databases without automatic shrinking need it; a stale dialog can ask after another client's repair.
    if ((yield* readLayout).autoVacuum !== 0) {
      return yield* new DatabaseReclaimError({
        reason: "This database already shrinks on its own. Nothing to reclaim.",
      });
    }
    const { databaseBytes, reclaimableBytes } = yield* readPages;
    const shortfall = yield* storage
      .spaceShortfall(config.dbPath, databaseBytes - reclaimableBytes)
      .pipe(
        Effect.provideService(FileSystem.FileSystem, fs),
        Effect.provideService(Path.Path, path),
      );
    if (shortfall !== null) {
      return yield* new DatabaseReclaimError({
        reason: `Reclaiming needs ${formatGiB(shortfall.neededBytes)} of free disk space on this machine. ${formatGiB(shortfall.freeBytes)} is free.`,
      });
    }
    yield* fs.remove(reclaimFiles.failure, { force: true });
    reclaimFailure = undefined;
    yield* publishStatus;
    const requestedAt = yield* Clock.currentTimeMillis;
    // One step: a request interrupted between them would repair on some unrelated later exit.
    yield* Effect.uninterruptible(
      fs.writeFileString(reclaimFiles.request, String(requestedAt)).pipe(
        // Reply before stopping: the restart closes this connection.
        Effect.andThen(restart.pipe(Effect.delay("500 millis"), Effect.forkDetach)),
      ),
    );
    yield* Effect.logInfo("Restarting to reclaim database space");
  }).pipe(
    Effect.mapError((cause) =>
      cause._tag === "DatabaseReclaimError"
        ? cause
        : new DatabaseReclaimError({
            reason: "Could not prepare the repair. Try again.",
          }),
    ),
    Effect.withSpan("DatabaseStorage.requestReclaim"),
  );

  return DatabaseStorage.of({
    reclaim,
    status: SubscriptionRef.changes(statusRef),
    requestReclaim,
  });
});

export const layerWithBackend = Layer.effect(DatabaseStorage, makeWithBackend);
export const layer = layerWithBackend.pipe(Layer.provide(SqliteDatabaseStorageBackend.layer));
