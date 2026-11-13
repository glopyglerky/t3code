import {
  OrchestrationV2AppThreadJson,
  OrchestrationV2DomainEventJson,
  OrchestrationV2ProviderTurnJson,
  OrchestrationV2RunJson,
  OrchestrationV2RuntimeRequestJson,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";

import {
  ProjectionCommitStore,
  ProjectionCommitStoreError,
  applyWithThreadLinkNormalization,
  toProjectionCommitStoreError,
  type ProjectionCommitStoreShape,
} from "../orchestration-v2/ProjectionCommitStore.ts";
import { RedbClient } from "./RedbClient.ts";

const encodeEvent = Schema.encodeEffect(OrchestrationV2DomainEventJson);
const encodeJson = Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown));
const encodeTime = Schema.encodeEffect(Schema.DateTimeUtcFromString);
const decodeThread = Schema.decodeUnknownEffect(
  Schema.fromJsonString(OrchestrationV2AppThreadJson),
);
const decodeRun = Schema.decodeUnknownEffect(Schema.fromJsonString(OrchestrationV2RunJson));
const decodeRequest = Schema.decodeUnknownEffect(
  Schema.fromJsonString(OrchestrationV2RuntimeRequestJson),
);
const ThreadPayload = Schema.fromJsonString(OrchestrationV2AppThreadJson);
const TurnPayload = Schema.fromJsonString(OrchestrationV2ProviderTurnJson);
const Normalization = Schema.NullOr(
  Schema.Struct({
    kind: Schema.Literals(["thread", "provider_turn"]),
    payload_json: Schema.String,
  }),
);

export const make = Effect.gen(function* () {
  const client = yield* RedbClient;
  while (!(yield* client.prepareProjections).prepared) yield* Effect.yieldNow;
  const store: ProjectionCommitStoreShape = {
    apply: Effect.fnUntraced(
      function* (event) {
        const encoded = yield* encodeEvent(event);
        const input = {
          event_type: encoded.type,
          thread_id: encoded.threadId,
          occurred_at: encoded.occurredAt,
          payload_json: yield* encodeJson(encoded.payload),
        };
        if (event.type !== "provider-thread.updated" && event.type !== "provider-turn.updated") {
          return yield* client
            .request("application_projections_apply", { event: input }, Schema.Null)
            .pipe(Effect.asVoid);
        }
        yield* client.withTransaction(
          Effect.gen(function* () {
            const context = yield* client.request(
              "application_projections_normalization",
              { event: input },
              Normalization,
            );
            if (context === null) {
              return yield* client
                .request("application_projections_apply", { event: input }, Schema.Null)
                .pipe(Effect.asVoid);
            }
            // This is the same codec round trip performed by the SQLite writer.
            // Rust chooses the old row and owns every merge and column mutation.
            const payloadJson =
              context.kind === "thread"
                ? yield* Schema.decodeEffect(ThreadPayload)(context.payload_json).pipe(
                    Effect.flatMap(Schema.encodeEffect(ThreadPayload)),
                  )
                : yield* Schema.decodeEffect(TurnPayload)(context.payload_json).pipe(
                    Effect.flatMap(Schema.encodeEffect(TurnPayload)),
                  );
            yield* client.request(
              "application_projections_apply",
              {
                event: {
                  ...input,
                  normalization: {
                    expected_payload_json: context.payload_json,
                    payload_json: payloadJson,
                  },
                },
              },
              Schema.Null,
            );
          }),
        );
      },
      Effect.mapError(toProjectionCommitStoreError("apply")),
    ),
    getThread: Effect.fnUntraced(
      function* (threadId) {
        const payload = yield* client.request(
          "application_projections_read",
          {
            read: { kind: "thread", thread_id: threadId },
          },
          Schema.NullOr(Schema.String),
        );
        return payload === null ? undefined : yield* decodeThread(payload);
      },
      Effect.mapError(toProjectionCommitStoreError("get-thread")),
    ),
    getRun: Effect.fnUntraced(
      function* (threadId, runId) {
        return yield* client.withTransaction(
          Effect.gen(function* () {
            // getThreadRecords decodes its owner even when the selected run is absent.
            const owner = yield* client.request(
              "application_projections_read",
              {
                read: { kind: "thread", thread_id: threadId },
              },
              Schema.NullOr(Schema.String),
            );
            if (owner === null) {
              return yield* new ProjectionCommitStoreError({
                operation: "get-run",
                cause: `No orchestration projection exists for thread ${threadId}.`,
              });
            }
            yield* decodeThread(owner);
            const payload = yield* client.request(
              "application_projections_read",
              {
                read: { kind: "run", thread_id: threadId, record_id: runId },
              },
              Schema.NullOr(Schema.String),
            );
            return payload === null ? undefined : yield* decodeRun(payload);
          }),
        );
      },
      Effect.mapError(toProjectionCommitStoreError("get-run")),
    ),
    getRuntimeRequest: Effect.fnUntraced(
      function* (threadId, requestId) {
        const payload = yield* client.request(
          "application_projections_read",
          {
            read: { kind: "runtime_request", thread_id: threadId, record_id: requestId },
          },
          Schema.NullOr(Schema.String),
        );
        return payload === null ? undefined : yield* decodeRequest(payload);
      },
      Effect.mapError(toProjectionCommitStoreError("get-runtime-request")),
    ),
    markApplied: Effect.fnUntraced(
      function* (sequence, updatedAt) {
        yield* client.request(
          "application_projections_mark_applied",
          {
            sequence,
            updated_at: yield* encodeTime(updatedAt),
          },
          Schema.Null,
        );
      },
      Effect.mapError(toProjectionCommitStoreError("mark-applied")),
    ),
    nativeThreadOwned: (input) =>
      input.nativeThreadRef === null
        ? Effect.succeed(false)
        : client
            .request(
              "application_projections_guard",
              {
                guard: {
                  kind: "native_thread_owned",
                  driver: input.driver,
                  provider_instance_id: input.providerInstanceId,
                  native_id: input.nativeThreadRef.nativeId,
                  app_thread_id: input.appThreadId,
                },
              },
              Schema.Boolean,
            )
            .pipe(Effect.mapError(toProjectionCommitStoreError("native-thread-owned"))),
    runCurrent: (input) =>
      client
        .request(
          "application_projections_guard",
          {
            guard: {
              kind: "run_current",
              thread_id: input.threadId,
              run_id: input.runId,
              active_attempt_id: input.activeAttemptId,
              status: input.expectedStatus,
            },
          },
          Schema.Boolean,
        )
        .pipe(Effect.mapError(toProjectionCommitStoreError("run-current"))),
    providerThreadOwner: (input) =>
      client
        .request(
          "application_projections_guard",
          {
            guard: {
              kind: "provider_thread_owner",
              provider_thread_id: input.providerThreadId,
              run_id: input.runId,
              active_attempt_id: input.activeAttemptId,
              expected_last_run_ordinal: input.expectedLastRunOrdinal,
            },
          },
          Schema.Boolean,
        )
        .pipe(Effect.mapError(toProjectionCommitStoreError("provider-thread-owner"))),
    providerThreadUnchanged: (providerThreadId, expectedJson) =>
      client
        .request(
          "application_projections_guard",
          {
            guard: {
              kind: "provider_thread_unchanged",
              provider_thread_id: providerThreadId,
              expected_json: expectedJson,
            },
          },
          Schema.Boolean,
        )
        .pipe(Effect.mapError(toProjectionCommitStoreError("provider-thread-unchanged"))),
    turnItemUnchanged: (turnItemId, expectedJson) =>
      client
        .request(
          "application_projections_guard",
          {
            guard: {
              kind: "turn_item_unchanged",
              turn_item_id: turnItemId,
              expected_json: expectedJson,
            },
          },
          Schema.Boolean,
        )
        .pipe(Effect.mapError(toProjectionCommitStoreError("turn-item-unchanged"))),
    interruptPending: (input) =>
      client
        .request(
          "application_projections_guard",
          {
            guard: {
              kind: "interrupt_pending",
              thread_id: input.threadId,
              request_id: input.requestId,
              result_id: input.resultId,
            },
          },
          Schema.Boolean,
        )
        .pipe(Effect.mapError(toProjectionCommitStoreError("interrupt-pending"))),
  };
  const normalizedApply = applyWithThreadLinkNormalization(store);
  return ProjectionCommitStore.of({
    ...store,
    // This normalization reads before it writes. A standalone apply needs the
    // same writer reservation that EventSink already owns for its full commit.
    apply: Effect.fnUntraced(function* (event) {
      if (event.type !== "thread.pull-request-link-updated") return yield* store.apply(event);
      yield* client
        .withTransaction(normalizedApply(event))
        .pipe(Effect.mapError(toProjectionCommitStoreError("apply")));
    }),
  });
});

export const layer = Layer.effect(ProjectionCommitStore, make);
