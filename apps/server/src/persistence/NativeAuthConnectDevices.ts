import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";

import * as Devices from "./AuthConnectDevices.ts";
import { PersistenceSqlError } from "./Errors.ts";
import { RedbClient } from "./RedbClient.ts";

const Written = Schema.Struct({ written: Schema.Literal(true) });
const encodeTrustInput = Schema.encodeEffect(Devices.TrustInput);
const encodeProofKeyThumbprint = Schema.encodeEffect(Schema.String);
const encodeOptionalProofKeyThumbprint = Schema.encodeEffect(Schema.NullOr(Schema.String));

/** Device trust shares the same native transaction as pairing and session state. */
export const make = Effect.gen(function* () {
  const client = yield* RedbClient;
  while (!(yield* client.prepareAuth).prepared) yield* Effect.yieldNow;

  const request = <A>(
    read: boolean,
    input: Readonly<Record<string, unknown>>,
    response: Schema.Decoder<A>,
  ) =>
    client.request(
      read ? "application_auth_read" : "application_auth_write",
      { request: input },
      response,
    );
  const failure = (method: string) => (cause: unknown) =>
    new PersistenceSqlError({ operation: `AuthConnectDeviceRepository.${method}`, cause });

  return Devices.AuthConnectDeviceRepository.of({
    trust: Effect.fn("AuthConnectDeviceRepository.trust")((input) =>
      encodeTrustInput(input).pipe(
        Effect.flatMap(({ proofKeyThumbprint, client, approvedAt }) =>
          request(
            false,
            {
              kind: "device_trust",
              proof_key_thumbprint: proofKeyThumbprint,
              client,
              approved_at: approvedAt,
            },
            Written,
          ),
        ),
        Effect.asVoid,
        Effect.mapError(failure("trust")),
      ),
    ),
    isTrusted: Effect.fn("AuthConnectDeviceRepository.isTrusted")((proofKeyThumbprint) =>
      encodeProofKeyThumbprint(proofKeyThumbprint).pipe(
        Effect.flatMap((proof) =>
          request(true, { kind: "device_is_trusted", proof_key_thumbprint: proof }, Schema.Boolean),
        ),
        Effect.mapError(failure("isTrusted")),
      ),
    ),
    list: Effect.fn("AuthConnectDeviceRepository.list")(() =>
      request(true, { kind: "device_list" }, Schema.Array(Devices.AuthConnectDeviceRecord)).pipe(
        Effect.mapError(failure("list")),
      ),
    ),
    remove: Effect.fn("AuthConnectDeviceRepository.remove")((proofKeyThumbprint) =>
      encodeProofKeyThumbprint(proofKeyThumbprint).pipe(
        Effect.flatMap((proof) =>
          request(false, { kind: "device_remove", proof_key_thumbprint: proof }, Schema.Boolean),
        ),
        Effect.mapError(failure("remove")),
      ),
    ),
    removeAllExcept: Effect.fn("AuthConnectDeviceRepository.removeAllExcept")(
      (keepProofKeyThumbprint) =>
        encodeOptionalProofKeyThumbprint(keepProofKeyThumbprint).pipe(
          Effect.flatMap((proof) =>
            request(
              false,
              { kind: "device_remove_all_except", keep_proof_key_thumbprint: proof },
              Schema.Number,
            ),
          ),
          Effect.mapError(failure("removeAllExcept")),
        ),
    ),
  });
});

export const layer = Layer.effect(Devices.AuthConnectDeviceRepository, make);
