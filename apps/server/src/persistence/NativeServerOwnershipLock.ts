import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";
import * as NativeDatabase from "../orchestration-v2/NativeDatabase.ts";
import { OwnershipLockError, ServerOwnershipLock } from "./ServerOwnershipLock.ts";

const Acquired = Schema.Struct({
  owned: Schema.Boolean,
  replaced_during_acquire: Schema.Boolean,
});
const Owned = Schema.Struct({ owned: Schema.Boolean });
const Activated = Schema.Struct({ activated: Schema.Literal(true) });
const error = (cause: NativeDatabase.NativeDatabaseError) =>
  new OwnershipLockError({
    cause,
    busy: cause._tag === "NativeDatabaseRejected" && cause.kind === "writer_busy",
  });
export const make = Effect.fn("NativeServerOwnershipLock.make")(function* (executable: string) {
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  return ServerOwnershipLock.of({
    acquire: Effect.fn("NativeServerOwnershipLock.acquire")(function* (lockPath) {
      const database = yield* NativeDatabase.make({
        executable,
        databasePath: lockPath,
        mode: "owner-lock",
        reconnect: false,
      }).pipe(Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner));
      const acquired = yield* database.request({ operation: "owner_acquire" }, Acquired);
      return {
        replacedDuringAcquire: !acquired.owned || acquired.replaced_during_acquire,
        activateNative: database
          .request({ operation: "owner_activate" }, Activated)
          .pipe(Effect.asVoid, Effect.mapError(error)),
        // A failed dedicated helper has lost its lock. Do not reconnect or keep
        // serving under a stale ownership assumption.
        verify: database.request({ operation: "owner_check" }, Owned).pipe(
          Effect.map((result) => result.owned),
          Effect.orElseSucceed(() => false),
        ),
      };
    }, Effect.mapError(error)),
  });
});
export const layer = (executable: string) => Layer.effect(ServerOwnershipLock, make(executable));
