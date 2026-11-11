import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import type * as Schema from "effect/Schema";
import { RedbClient } from "../persistence/RedbClient.ts";
import {
  MaintenanceRows,
  type MaintenanceRead,
  type MaintenanceWrite,
  toRowsError,
} from "./MaintenanceRows.ts";

export const make = Effect.gen(function* () {
  const client = yield* RedbClient;
  while (!(yield* client.prepareEvents).prepared) yield* Effect.yieldNow;
  while (!(yield* client.prepareReceipts).prepared) yield* Effect.yieldNow;
  while (!(yield* client.prepareMaintenance).prepared) yield* Effect.yieldNow;
  return MaintenanceRows.of({
    read: Effect.fn("MaintenanceRows.read")(
      <A>(query: MaintenanceRead, response: Schema.Decoder<A>) =>
        client
          .request("application_maintenance_read", { query }, response)
          .pipe(Effect.mapError(toRowsError("read"))),
    ),
    write: Effect.fn("MaintenanceRows.write")(
      <A>(mutation: MaintenanceWrite, response: Schema.Decoder<A>) =>
        client
          .request("application_maintenance_write", { mutation }, response)
          .pipe(Effect.mapError(toRowsError("write"))),
    ),
  });
});
export const layer = Layer.effect(MaintenanceRows, make);
