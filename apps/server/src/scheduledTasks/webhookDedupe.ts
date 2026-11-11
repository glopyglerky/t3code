// @effect-diagnostics-next-line nodeBuiltinImport:off -- Keys need a deterministic digest, not random bytes.
import * as NodeCrypto from "node:crypto";
import { ScheduledTask, ScheduledTaskError } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { ScheduledRows, AnyRows, PendingRows } from "./ScheduledRows.ts";
import { renderWebhookPrompt, type WebhookRequest } from "./webhookTemplate.ts";

const encodeDeliveryIds = Schema.encodeEffect(Schema.fromJsonString(Schema.Array(Schema.String)));

function key(template: string | undefined, request: WebhookRequest): string {
  if (template === undefined) return "";
  const rendered = renderWebhookPrompt(template, request, "", true);
  return rendered.missing.length > 0 || rendered.prompt === ""
    ? ""
    : NodeCrypto.createHash("sha256").update(rendered.prompt).digest("hex");
}

export const releaseWebhookReplay = Effect.fn("ScheduledTaskService.releaseWebhookReplay")(
  function* (deliveryId: string) {
    const rows = yield* ScheduledRows;
    yield* rows.write({ kind: "release_replay", delivery_id: deliveryId }, AnyRows);
  },
);

const hasReplayKey = Effect.fn(function* (taskId: string, replayKey: string, now: number) {
  const rows = yield* ScheduledRows;
  return (
    replayKey !== "" &&
    (yield* rows.read({ kind: "has_replay", task_id: taskId, replay_key: replayKey, now }, AnyRows))
      .length > 0
  );
});
const claimReplayKey = Effect.fn(function* (
  taskId: string,
  replayKey: string,
  deliveryId: string,
  now: number,
) {
  if (replayKey === "") return;
  const rows = yield* ScheduledRows;
  yield* rows.write(
    {
      kind: "claim_replay",
      task_id: taskId,
      replay_key: replayKey,
      delivery_id: deliveryId,
      now,
      expires_at: now + 7 * 24 * 60 * 60 * 1000,
    },
    AnyRows,
  );
});

export const acceptWebhookDedupe = Effect.fn("ScheduledTaskService.acceptWebhookDedupe")(function* (
  task: ScheduledTask,
  request: WebhookRequest,
  deliveryId: string,
  now: number,
  reserve: Effect.Effect<void, "first_rejected" | "rejected">,
  reserveCoalescedLog: Effect.Effect<boolean>,
  checkCapacity: Effect.Effect<void, "queue_full" | ScheduledTaskError>,
  replaceDeliveryRequest: (ownerId: string) => Effect.Effect<void, ScheduledTaskError>,
) {
  const rows = yield* ScheduledRows;
  const dedupe = task.schedule.type === "webhook" ? task.schedule.dedupe : undefined;
  if (dedupe === undefined) return "run" as const;
  const replayKey = key(dedupe.replayKey, request);
  if (yield* hasReplayKey(task.id, replayKey, now)) return "duplicate" as const;
  const coalesceKey = key(dedupe.coalesceKey, request);
  if (coalesceKey === "") return "run" as const;
  const pending = yield* rows.read(
    {
      kind: "pending",
      task_id: task.id,
      coalesce_key: coalesceKey,
      states: ["pending"],
      due_after: now,
      first: false,
    },
    PendingRows,
  );
  const logCoalesced = pending.length > 0 ? yield* reserveCoalescedLog : true;
  if (pending.length === 0) {
    yield* checkCapacity;
    yield* reserve;
  }
  // Coalesced requests retain their validation slot, bounding writes without dropping accepted payloads.
  yield* claimReplayKey(task.id, replayKey, pending[0]?.delivery_id ?? deliveryId, now);
  const requestJson = yield* encodePendingRequest({
    method: request.method,
    path: request.path,
    query: request.query,
    headers: request.headers,
    bodyText: request.bodyText,
  });
  if (pending.length > 0) {
    yield* rows.write(
      {
        kind: "patch_pending",
        delivery_id: pending[0]!.delivery_id,
        patch: { request_json: requestJson },
        append_delivery_id: deliveryId,
      },
      AnyRows,
    );
    yield* replaceDeliveryRequest(pending[0]!.delivery_id);
    return logCoalesced ? ("coalesced" as const) : ("coalesced_unlogged" as const);
  }
  // Expired windows are immutable, even if the scheduler has not dispatched them yet.
  yield* rows.write(
    { kind: "expire_pending", task_id: task.id, coalesce_key: coalesceKey, now },
    AnyRows,
  );
  yield* rows.write(
    {
      kind: "insert_pending",
      row: {
        delivery_id: deliveryId,
        task_id: task.id,
        coalesce_key: coalesceKey,
        due_at: now + (dedupe.coalesceWindowSeconds ?? 120) * 1000,
        task_json: yield* encodePendingTask(task),
        request_json: requestJson,
        delivery_ids_json: yield* encodeDeliveryIds([deliveryId]),
      },
    },
    AnyRows,
  );
  return "pending" as const;
});

export const acceptImmediateWebhookDedupe = Effect.fn(
  "ScheduledTaskService.acceptImmediateWebhookDedupe",
)(function* (
  task: ScheduledTask,
  request: WebhookRequest,
  deliveryId: string,
  now: number,
  reserve: Effect.Effect<void, "first_rejected" | "rejected" | "queue_full" | ScheduledTaskError>,
) {
  const replayKey = key(
    task.schedule.type === "webhook" ? task.schedule.dedupe?.replayKey : undefined,
    request,
  );
  if (yield* hasReplayKey(task.id, replayKey, now)) return "duplicate" as const;
  yield* reserve;
  yield* claimReplayKey(task.id, replayKey, deliveryId, now);
  return "run" as const;
});

const PendingTaskJson = Schema.fromJsonString(ScheduledTask);
export const encodePendingTask = Schema.encodeEffect(PendingTaskJson);
export const decodePendingTask = Schema.decodeUnknownEffect(PendingTaskJson);
const PendingRequestJson = Schema.fromJsonString(
  Schema.Struct({
    method: Schema.String,
    path: Schema.String,
    query: Schema.String,
    headers: Schema.Record(Schema.String, Schema.String),
    bodyText: Schema.String,
  }),
);
const encodePendingRequest = Schema.encodeEffect(PendingRequestJson);
export const decodePendingRequest = Schema.decodeUnknownEffect(PendingRequestJson);
export const dedupeError = (cause: unknown) =>
  new ScheduledTaskError({ message: "Could not persist webhook dedupe state.", cause });
