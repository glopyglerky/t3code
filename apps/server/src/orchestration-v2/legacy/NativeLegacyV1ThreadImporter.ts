import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";

import * as RedbClient from "../../persistence/RedbClient.ts";
import {
  LegacyV1ThreadImporter,
  makeFromRows,
  type LegacyV1Rows,
} from "./LegacyV1ThreadImporter.ts";

const NullableString = Schema.NullOr(Schema.String);
const ThreadRow = Schema.Struct({
  thread_id: Schema.String,
  project_id: Schema.String,
  title: Schema.String,
  model_selection_json: NullableString,
  runtime_mode: Schema.String,
  interaction_mode: Schema.String,
  branch: NullableString,
  worktree_path: NullableString,
  created_at: Schema.String,
  updated_at: Schema.String,
  archived_at: NullableString,
  settled_override: NullableString,
  settled_at: NullableString,
  unsettled_at: NullableString,
  snoozed_until: NullableString,
  snoozed_at: NullableString,
  pinned_at: NullableString,
  auto_settle_disabled_at: NullableString,
  pin_order_key: NullableString,
  pull_requests_json: Schema.String,
  linked_pull_request_json: NullableString,
  branch_pull_request_json: NullableString,
  active_order_key: NullableString,
  deleted_at: NullableString,
});
const RepairRow = Schema.Struct({ ...ThreadRow.fields, payload_json: Schema.String });
const ImportRow = Schema.Struct({
  thread_id: Schema.String,
  transcript_imported_at: NullableString,
});
const MessageRow = Schema.Struct({
  message_id: Schema.String,
  thread_id: Schema.String,
  role: Schema.Literals(["user", "assistant"]),
  text: Schema.String,
  attachments_json: NullableString,
  context_json: NullableString,
  is_streaming: Schema.Number,
  created_at: Schema.String,
  updated_at: Schema.String,
  ordinal: Schema.Number,
});

const make = Effect.gen(function* () {
  const client = yield* RedbClient.RedbClient;
  while (!(yield* client.prepareEvents).prepared) yield* Effect.yieldNow;
  while (!(yield* client.preparePositions).prepared) yield* Effect.yieldNow;
  while (!(yield* client.prepareLegacyV1).prepared) yield* Effect.yieldNow;
  const rows: LegacyV1Rows<RedbClient.RedbClientError> = {
    messages: (thread_id) =>
      client.request(
        "application_legacy_v1_read",
        { query: { kind: "messages", thread_id, mode: "all" } },
        Schema.Array(MessageRow),
      ),
    latestMessage: (thread_id, userOnly) =>
      client.request(
        "application_legacy_v1_read",
        { query: { kind: "messages", thread_id, mode: userOnly ? "latest_user" : "latest" } },
        Schema.Array(MessageRow),
      ),
    repairs: client.request(
      "application_legacy_v1_read",
      { query: { kind: "repairs" } },
      Schema.Array(RepairRow),
    ),
    pendingThreads: client.request(
      "application_legacy_v1_read",
      { query: { kind: "pending_threads" } },
      Schema.Array(ThreadRow),
    ),
    pendingThreadCount: client.request(
      "application_legacy_v1_read",
      { query: { kind: "pending_count" } },
      Schema.Number,
    ),
    getImport: (thread_id) =>
      client.request(
        "application_legacy_v1_read",
        { query: { kind: "get_import", thread_id } },
        Schema.Array(ImportRow),
      ),
    importedEvents: (thread_id) =>
      client.request(
        "application_legacy_v1_read",
        { query: { kind: "imported_events", thread_id } },
        Schema.Array(Schema.Struct({ event_id: Schema.String })),
      ),
    pendingImports: client.request(
      "application_legacy_v1_read",
      { query: { kind: "pending_imports" } },
      Schema.Array(ImportRow),
    ),
    insertPosition: ({ threadId, turnItemId, ordinal }) =>
      client.request(
        "application_legacy_v1_write",
        {
          mutation: {
            kind: "insert_position",
            thread_id: threadId,
            turn_item_id: turnItemId,
            ordinal,
          },
        },
        Schema.Unknown,
      ),
    insertShell: ({ threadId, sourceUpdatedAt, shellImportedAt, importedMessageCount }) =>
      client.request(
        "application_legacy_v1_write",
        {
          mutation: {
            kind: "insert_shell",
            thread_id: threadId,
            source_updated_at: sourceUpdatedAt,
            shell_imported_at: shellImportedAt,
            imported_message_count: importedMessageCount,
          },
        },
        Schema.Unknown,
      ),
    complete: ({ threadId, completedAt, messageCount }) =>
      client.request(
        "application_legacy_v1_write",
        {
          mutation: {
            kind: "complete",
            thread_id: threadId,
            completed_at: completedAt,
            message_count: messageCount,
          },
        },
        Schema.Unknown,
      ),
    failure: (thread_id) =>
      client.request(
        "application_legacy_v1_write",
        { mutation: { kind: "failure", thread_id } },
        Schema.Unknown,
      ),
    withTransaction: client.withTransaction,
  };
  return yield* makeFromRows(rows);
});

export const layer = Layer.effect(LegacyV1ThreadImporter, make);
