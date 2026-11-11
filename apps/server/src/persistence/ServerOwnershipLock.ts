import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import type * as Scope from "effect/Scope";

export class OwnershipLockError extends Schema.TaggedError<OwnershipLockError>()(
  "OwnershipLockError",
  { busy: Schema.Boolean, cause: Schema.Defect() },
) {}
export class ServerOwnershipLock extends Context.Service<
  ServerOwnershipLock,
  {
    readonly acquire: (path: string) => Effect.Effect<
      {
        readonly replacedDuringAcquire: boolean;
        readonly verify: Effect.Effect<boolean, OwnershipLockError>;
        readonly activateNative?: Effect.Effect<void, OwnershipLockError>;
      },
      OwnershipLockError,
      Scope.Scope
    >;
  }
>()("t3/persistence/ServerOwnershipLock") {}
