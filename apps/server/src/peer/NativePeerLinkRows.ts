import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";

import { RedbClient } from "../persistence/RedbClient.ts";
import { PeerLinkRow, PeerLinkRows, toPeerLinkRowsError } from "./PeerLinkRows.ts";

const Written = Schema.Struct({ written: Schema.Literal(true) });

export const make = Effect.gen(function* () {
  const client = yield* RedbClient;
  while (!(yield* client.preparePeers).prepared) yield* Effect.yieldNow;
  return PeerLinkRows.of({
    list: client
      .request("application_peers_read", { request: { kind: "list" } }, Schema.Array(PeerLinkRow))
      .pipe(Effect.mapError(toPeerLinkRowsError("list"))),
    get: Effect.fn("PeerLinkRows.get")((environment_id) =>
      client
        .request(
          "application_peers_read",
          {
            request: { kind: "get", environment_id },
          },
          Schema.NullOr(PeerLinkRow),
        )
        .pipe(Effect.mapError(toPeerLinkRowsError("get"))),
    ),
    upsert: Effect.fn("PeerLinkRows.upsert")((row) =>
      client
        .request(
          "application_peers_write",
          {
            request: { kind: "upsert", row },
          },
          Written,
        )
        .pipe(Effect.asVoid, Effect.mapError(toPeerLinkRowsError("upsert"))),
    ),
    relabel: Effect.fn("PeerLinkRows.relabel")((environment_id, label) =>
      client
        .request(
          "application_peers_write",
          {
            request: { kind: "relabel", environment_id, label },
          },
          Written,
        )
        .pipe(Effect.asVoid, Effect.mapError(toPeerLinkRowsError("relabel"))),
    ),
    remove: Effect.fn("PeerLinkRows.remove")((environment_id) =>
      client
        .request(
          "application_peers_write",
          {
            request: { kind: "delete", environment_id },
          },
          Schema.NullOr(Schema.String),
        )
        .pipe(Effect.mapError(toPeerLinkRowsError("remove"))),
    ),
    recordOutcome: Effect.fn("PeerLinkRows.recordOutcome")((environment_id, error, reached_at) =>
      client
        .request(
          "application_peers_write",
          {
            request: { kind: "record_outcome", environment_id, error, reached_at },
          },
          Written,
        )
        .pipe(Effect.asVoid, Effect.mapError(toPeerLinkRowsError("recordOutcome"))),
    ),
  });
});

export const layer = Layer.effect(PeerLinkRows, make);
