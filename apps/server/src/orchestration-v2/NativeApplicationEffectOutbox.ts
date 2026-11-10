import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";

import { RedbClient } from "../persistence/RedbClient.ts";
import * as EffectOutbox from "./EffectOutbox.ts";
import * as NativeDatabase from "./NativeDatabase.ts";
import { makeWithTransport } from "./NativeEffectOutbox.ts";

const Operation = Schema.Literals([
  "outbox_enqueue", "outbox_get", "outbox_list", "outbox_cancel", "outbox_reconcile",
  "outbox_claim_once", "outbox_claim", "outbox_next_claimable_at", "outbox_succeed", "outbox_retry",
  "outbox_fail", "outbox_prune",
]);
const operations = {
  outbox_enqueue: "application_outbox_enqueue",
  outbox_get: "application_outbox_get",
  outbox_list: "application_outbox_list",
  outbox_cancel: "application_outbox_cancel",
  outbox_reconcile: "application_outbox_reconcile",
  outbox_claim_once: "application_outbox_claim_once",
  outbox_claim: "application_outbox_claim",
  outbox_next_claimable_at: "application_outbox_next_claimable_at",
  outbox_succeed: "application_outbox_succeed",
  outbox_retry: "application_outbox_retry",
  outbox_fail: "application_outbox_fail",
  outbox_prune: "application_outbox_prune",
} as const;
const decodeInput = Schema.decodeUnknownEffect(Schema.JsonObject);
const decodeOperation = Schema.decodeUnknownEffect(Operation);

/** Authoritative application rows share the caller's reserved redb transaction. */
export const layer = Layer.effect(EffectOutbox.EffectOutboxV2, Effect.gen(function* () {
  const client = yield* RedbClient;
  while (!(yield* client.prepareOutbox).prepared) yield* Effect.yieldNow;
  return yield* makeWithTransport({
    retainLeaseExpiry: true,
    transactionalClaims: true,
    canRecover: client.isInTransaction.pipe(Effect.map((inside) => !inside)),
    publish: Effect.fnUntraced(function* (effect: Effect.Effect<void>) {
      if (yield* client.isInTransaction) {
        yield* client.afterCommit(effect).pipe(Effect.orDie);
      } else {
        yield* effect;
      }
    }),
    request: Effect.fnUntraced(function* <A>(input: unknown, response: Schema.Decoder<A>) {
      const record = yield* decodeInput(input).pipe(Effect.mapError(() =>
        new NativeDatabase.NativeDatabaseInputError({ stage: "request" }),
      ));
      const operation = yield* decodeOperation(record.operation).pipe(Effect.mapError(() =>
        new NativeDatabase.NativeDatabaseInputError({ stage: "request" }),
      ));
      const { operation: _operation, ...parameters } = record;
      return yield* client.request(operations[operation], parameters, response);
    }),
  });
}));
