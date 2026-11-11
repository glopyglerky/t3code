import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";

import {
  DeleteProviderSessionRuntimeInput,
  GetProviderSessionRuntimeInput,
  ProviderSessionRuntimeDbRowSchema,
  ProviderSessionRuntimeRawDbRowSchema,
  ProviderSessionRuntimeRepository,
  RecordImportedTranscriptRequestSchema,
  repositoryFromRows,
} from "./ProviderSessionRuntime.ts";
import { RedbClient } from "./RedbClient.ts";

const Written = Schema.Struct({ written: Schema.Literal(true) });

export const make = Effect.gen(function* () {
  const client = yield* RedbClient;
  while (!(yield* client.prepareRuntimeState).prepared) yield* Effect.yieldNow;
  const upsert = (ignore: boolean) => (input: typeof ProviderSessionRuntimeDbRowSchema.Type) =>
    Schema.encodeEffect(ProviderSessionRuntimeDbRowSchema)(input).pipe(
      Effect.flatMap((runtime) =>
        client.request(
          "application_runtime_state_write",
          {
            request: { kind: "runtime_upsert", runtime, ignore },
          },
          Written,
        ),
      ),
      Effect.asVoid,
    );
  return repositoryFromRows({
    upsertRuntimeRow: upsert(false),
    insertRuntimeRow: upsert(true),
    recordImportedTranscriptRow: (input) =>
      Schema.encodeEffect(RecordImportedTranscriptRequestSchema)(input).pipe(
        Effect.flatMap(({ threadId, source }) =>
          client.request(
            "application_runtime_state_write",
            {
              request: { kind: "runtime_import", thread_id: threadId, source },
            },
            Written,
          ),
        ),
        Effect.asVoid,
      ),
    getRuntimeRowByThreadId: (input) =>
      Schema.encodeEffect(GetProviderSessionRuntimeInput)(input).pipe(
        Effect.flatMap(({ threadId }) =>
          client.request(
            "application_runtime_state_read",
            {
              request: { kind: "runtime_get", thread_id: threadId },
            },
            Schema.NullOr(ProviderSessionRuntimeRawDbRowSchema),
          ),
        ),
        Effect.map((row) => (row === null ? Option.none() : Option.some(row))),
      ),
    listRuntimeRows: ({ excludeStopped }) =>
      client.request(
        "application_runtime_state_read",
        {
          request: { kind: "runtime_list", exclude_stopped: excludeStopped },
        },
        Schema.Array(ProviderSessionRuntimeRawDbRowSchema),
      ),
    deleteRuntimeByThreadId: (input) =>
      Schema.encodeEffect(DeleteProviderSessionRuntimeInput)(input).pipe(
        Effect.flatMap(({ threadId }) =>
          client.request(
            "application_runtime_state_write",
            {
              request: { kind: "runtime_delete", thread_id: threadId },
            },
            Written,
          ),
        ),
        Effect.asVoid,
      ),
  });
});

export const layer = Layer.effect(ProviderSessionRuntimeRepository, make);
