import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";

import { RedbClient } from "./RedbClient.ts";
import {
  AuxiliaryPersistenceReads,
  toAuxiliaryReadError,
  ProviderHistoryRow,
  LegacyProjectSettingsRow,
  ThreadWorktreeRow,
  DeletedThreadWorktreeRow,
  PendingCleanupRow,
  ProviderSessionRow,
} from "./AuxiliaryPersistenceReads.ts";

export const make = Effect.gen(function* () {
  const client = yield* RedbClient;
  while (!(yield* client.prepareProjects).prepared) yield* Effect.yieldNow;
  while (!(yield* client.prepareRuntimeState).prepared) yield* Effect.yieldNow;
  while (!(yield* client.prepareProjectionReads).prepared) yield* Effect.yieldNow;
  while (!(yield* client.prepareOutbox).prepared) yield* Effect.yieldNow;
  while (!(yield* client.prepareAuxiliary).prepared) yield* Effect.yieldNow;
  return AuxiliaryPersistenceReads.of({
    providerHistory: client
      .request(
        "application_auxiliary_read",
        { query: { kind: "provider_history" } },
        Schema.Array(ProviderHistoryRow),
      )
      .pipe(Effect.mapError(toAuxiliaryReadError("providerHistory"))),
    legacyProjectSettings: client
      .request(
        "application_auxiliary_read",
        { query: { kind: "legacy_project_settings" } },
        Schema.Array(LegacyProjectSettingsRow),
      )
      .pipe(Effect.mapError(toAuxiliaryReadError("legacyProjectSettings"))),
    allThreadWorktrees: client
      .request(
        "application_auxiliary_read",
        { query: { kind: "all_thread_worktrees" } },
        Schema.Array(ThreadWorktreeRow),
      )
      .pipe(Effect.mapError(toAuxiliaryReadError("allThreadWorktrees"))),
    deletedThreadWorktrees: client
      .request(
        "application_auxiliary_read",
        { query: { kind: "deleted_thread_worktrees" } },
        Schema.Array(DeletedThreadWorktreeRow),
      )
      .pipe(Effect.mapError(toAuxiliaryReadError("deletedThreadWorktrees"))),
    pendingThreadCleanup: Effect.fn("AuxiliaryPersistenceReads.pendingThreadCleanup")((thread_id) =>
      client
        .request(
          "application_auxiliary_read",
          { query: { kind: "pending_thread_cleanup", thread_id } },
          Schema.Array(PendingCleanupRow),
        )
        .pipe(Effect.mapError(toAuxiliaryReadError("pendingThreadCleanup"))),
    ),
    nonStoppedProviderSessions: client
      .request(
        "application_auxiliary_read",
        { query: { kind: "non_stopped_provider_sessions" } },
        Schema.Array(ProviderSessionRow),
      )
      .pipe(Effect.mapError(toAuxiliaryReadError("nonStoppedProviderSessions"))),
  });
});

export const layer = Layer.effect(AuxiliaryPersistenceReads, make);
