import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { RedbClient } from "../persistence/RedbClient.ts";
import {
  LimitRecoveryRow,
  ProjectionAuditPage,
  ProjectionRecoveryRowsError,
  RecoveryThreadRow,
  RecoveryWakeRow,
  type ProjectionRecoveryRows,
} from "./ProjectionRecoveryRows.ts";

/** The raw unreadability audit is available before preparing projection indexes. */
export const make = Effect.gen(function* () {
  const client = yield* RedbClient;
  const read = <A>(query: Readonly<Record<string, unknown>>, schema: Schema.Decoder<A>) =>
    client
      .request("application_projections_recovery", { query }, schema)
      .pipe(Effect.mapError((cause) => new ProjectionRecoveryRowsError({ cause })));
  const reader: ProjectionRecoveryRows = {
    limitCandidates: ({ now, autoResume, snooze }) =>
      read(
        { kind: "candidates", now: DateTime.formatIso(now), auto_resume: autoResume, snooze },
        Schema.Array(LimitRecoveryRow),
      ),
    wakeAt: (now) => read({ kind: "wake_at", now }, Schema.Array(RecoveryWakeRow)),
    recoveryThreads: (recovery_kind, projects) =>
      read(
        {
          kind: "threads",
          recovery_kind,
          ...(projects?.include === undefined ? {} : { include: projects.include }),
          ...(projects?.exclude === undefined ? {} : { exclude: projects.exclude }),
        },
        Schema.Array(RecoveryThreadRow),
      ),
    unreadablePage: (table, _id, after, limit) =>
      read({ kind: "unreadable_page", table, after, limit }, ProjectionAuditPage),
    bindings: (record_id) => read({ kind: "bindings", record_id }, Schema.Array(RecoveryThreadRow)),
    providerThreadOwners: (record_id) =>
      read({ kind: "provider_thread_owners", record_id }, Schema.Array(RecoveryThreadRow)),
  };
  return reader;
});
