// @effect-diagnostics nodeBuiltinImport:off -- Retained SQLite compatibility ownership handle.
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import * as NodeSqlite from "node:sqlite";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import { OwnershipLockError, ServerOwnershipLock } from "./ServerOwnershipLock.ts";

const error = (cause: unknown) =>
  new OwnershipLockError({
    cause,
    busy:
      cause instanceof Error &&
      (("errcode" in cause && cause.errcode === 5) ||
        ("code" in cause && cause.code === "SQLITE_BUSY")),
  });
export const layer = Layer.succeed(
  ServerOwnershipLock,
  ServerOwnershipLock.of({
    acquire: Effect.fn("SqliteServerOwnershipLock.acquire")(function* (lockPath) {
      const lock = yield* Effect.acquireRelease(
        Effect.try({
          try: () => {
            NodeFS.mkdirSync(NodePath.dirname(lockPath), { recursive: true });
            let before = NodeFS.statSync(lockPath, {
              bigint: true,
              throwIfNoEntry: false,
            });
            if (before === undefined) {
              NodeFS.closeSync(NodeFS.openSync(lockPath, "a", 0o644));
              before = NodeFS.statSync(lockPath, { bigint: true });
            }
            const db = new NodeSqlite.DatabaseSync(lockPath);
            try {
              db.exec("PRAGMA busy_timeout = 0; BEGIN EXCLUSIVE;");
              const identity = NodeFS.statSync(lockPath, { bigint: true });
              return {
                db,
                identity,
                replacedDuringAcquire: before.dev !== identity.dev || before.ino !== identity.ino,
              };
            } catch (cause) {
              db.close();
              throw cause;
            }
          },
          catch: error,
        }),
        ({ db }) => Effect.sync(() => db.close()),
      );
      return {
        replacedDuringAcquire: lock.replacedDuringAcquire,
        verify: Effect.try({
          try: () => {
            const current = NodeFS.statSync(lockPath, {
              bigint: true,
              throwIfNoEntry: false,
            });
            return (
              current !== undefined &&
              current.dev === lock.identity.dev &&
              current.ino === lock.identity.ino
            );
          },
          catch: error,
        }),
      };
    }),
  }),
);
