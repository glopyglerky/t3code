import { CommandId, IsoDateTime, NonNegativeInt, ProjectId, ThreadId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";

import { toPersistenceSqlError } from "./Errors.ts";
import {
  OrchestrationCommandReceiptRepository,
  type OrchestrationCommandReceipt,
} from "./OrchestrationCommandReceipts.ts";
import { RedbClient } from "./RedbClient.ts";

const ReceiptRow = Schema.Struct({
  command_id: CommandId,
  aggregate_kind: Schema.Literals(["project", "thread"]),
  aggregate_id: Schema.Union([ProjectId, ThreadId]),
  command_type: Schema.String,
  accepted_at: IsoDateTime,
  result_sequence: NonNegativeInt,
  status: Schema.Literals(["accepted", "rejected"]),
  error: Schema.NullOr(Schema.String),
});
const Inserted = Schema.Struct({ inserted: Schema.Boolean });
const Written = Schema.Struct({ written: Schema.Literal(true) });

function toRow(receipt: OrchestrationCommandReceipt): typeof ReceiptRow.Type {
  return {
    command_id: receipt.commandId,
    aggregate_kind: receipt.aggregateKind,
    aggregate_id: receipt.aggregateId,
    command_type: receipt.commandType,
    accepted_at: receipt.acceptedAt,
    result_sequence: receipt.resultSequence,
    status: receipt.status,
    error: receipt.error,
  };
}

function fromRow(row: typeof ReceiptRow.Type): OrchestrationCommandReceipt {
  return {
    commandId: row.command_id,
    aggregateKind: row.aggregate_kind,
    aggregateId: row.aggregate_id,
    commandType: row.command_type,
    acceptedAt: row.accepted_at,
    resultSequence: row.result_sequence,
    status: row.status,
    error: row.error,
  };
}

/** The existing repository contract, backed by the current RedbClient transaction. */
export const make = Effect.gen(function* () {
  const client = yield* RedbClient;
  while (!(yield* client.prepareReceipts).prepared) yield* Effect.yieldNow;
  return OrchestrationCommandReceiptRepository.of({
    insertIfAbsent: Effect.fn("OrchestrationCommandReceiptRepository.insertIfAbsent")((receipt) =>
      client.request("application_receipts_insert", { receipt: toRow(receipt) }, Inserted).pipe(
        Effect.map((result) => result.inserted),
        Effect.mapError(toPersistenceSqlError("OrchestrationCommandReceiptRepository.insertIfAbsent:query")),
      )),
    upsert: Effect.fn("OrchestrationCommandReceiptRepository.upsert")((receipt) =>
      client.request("application_receipts_upsert", { receipt: toRow(receipt) }, Written).pipe(
        Effect.asVoid,
        Effect.mapError(toPersistenceSqlError("OrchestrationCommandReceiptRepository.upsert:query")),
      )),
    getByCommandId: Effect.fn("OrchestrationCommandReceiptRepository.getByCommandId")(({ commandId }) =>
      client.request("application_receipts_get", { command_id: commandId }, Schema.NullOr(ReceiptRow)).pipe(
        Effect.map((row) => row === null ? Option.none() : Option.some(fromRow(row))),
        Effect.mapError(toPersistenceSqlError("OrchestrationCommandReceiptRepository.getByCommandId:query")),
      )),
  });
});

export const layer = Layer.effect(OrchestrationCommandReceiptRepository, make);
