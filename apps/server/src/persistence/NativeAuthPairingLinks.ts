import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";

import * as Links from "./AuthPairingLinks.ts";
import { PersistenceDecodeError, type PersistenceErrorCorrelation } from "./Errors.ts";
import { RedbClient } from "./RedbClient.ts";

const Written = Schema.Struct({ written: Schema.Literal(true) });
const requestPhases = {
  create: "encodeRequest",
  consumeAvailable: "decodeRow",
  listActive: "decodeRows",
  revoke: "decodeRows",
  getByCredential: "decodeRow",
} as const;

export const make = Effect.gen(function* () {
  const client = yield* RedbClient;
  while (!(yield* client.prepareAuth).prepared) yield* Effect.yieldNow;

  const request = <A>(
    read: boolean,
    input: Readonly<Record<string, unknown>>,
    response: Schema.Decoder<A>,
    method: keyof typeof requestPhases,
    correlation?: PersistenceErrorCorrelation,
  ) =>
    client
      .request(
        read ? "application_auth_read" : "application_auth_write",
        { request: input },
        response,
      )
      .pipe(
        Effect.mapError(
          Links.toPersistenceSqlOrDecodeError(
            `AuthPairingLinkRepository.${method}:query`,
            `AuthPairingLinkRepository.${method}:${requestPhases[method]}`,
            correlation,
          ),
        ),
      );

  const decodeOption = (method: string, row: typeof Links.AuthPairingLinkRawDbRow.Type | null) =>
    row === null
      ? Effect.succeedNone
      : Links.decodeAuthPairingLinkDbRow(row).pipe(
          Effect.mapError((cause) =>
            PersistenceDecodeError.fromSchemaError(
              `AuthPairingLinkRepository.${method}:decodeRow`,
              cause,
              { pairingLinkId: row.id },
            ),
          ),
          Effect.asSome,
        );

  return Links.AuthPairingLinkRepository.of({
    create: Effect.fn("AuthPairingLinkRepository.create")((input) =>
      Schema.encodeEffect(Links.CreateAuthPairingLinkInput)(input).pipe(
        Effect.mapError((cause) =>
          PersistenceDecodeError.fromSchemaError(
            "AuthPairingLinkRepository.create:encodeRequest",
            cause,
            { pairingLinkId: input.id },
          ),
        ),
        Effect.flatMap((link) =>
          request(false, { kind: "pairing_create", link }, Written, "create", {
            pairingLinkId: input.id,
          }),
        ),
        Effect.asVoid,
      ),
    ),
    consumeAvailable: Effect.fn("AuthPairingLinkRepository.consumeAvailable")((input) =>
      Schema.encodeEffect(Links.ConsumeAuthPairingLinkInput)(input).pipe(
        Effect.mapError((cause) =>
          PersistenceDecodeError.fromSchemaError(
            "AuthPairingLinkRepository.consumeAvailable:decodeRow",
            cause,
          ),
        ),
        Effect.flatMap((encoded) =>
          request(
            false,
            {
              kind: "pairing_consume",
              credential: encoded.credential,
              proof_key_thumbprint: encoded.proofKeyThumbprint,
              requested_scopes: encoded.requestedScopes ?? null,
              required_scopes: encoded.requiredScopes ?? [],
              mcp_approval_subject: encoded.mcpApprovalSubject ?? null,
              consumed_at: encoded.consumedAt,
              now: encoded.now,
            },
            Schema.NullOr(Links.AuthPairingLinkRawDbRow),
            "consumeAvailable",
          ),
        ),
        Effect.flatMap((row) => decodeOption("consumeAvailable", row)),
      ),
    ),
    listActive: Effect.fn("AuthPairingLinkRepository.listActive")((input) =>
      Schema.encodeEffect(Links.ListActiveAuthPairingLinksInput)(input).pipe(
        Effect.mapError((cause) =>
          PersistenceDecodeError.fromSchemaError(
            "AuthPairingLinkRepository.listActive:decodeRows",
            cause,
          ),
        ),
        Effect.flatMap(({ now }) =>
          request(
            true,
            { kind: "pairing_list_active", now },
            Schema.Array(Links.AuthPairingLinkRawDbRow),
            "listActive",
          ),
        ),
        Effect.flatMap((rows) =>
          Effect.forEach(rows, (row) =>
            Links.decodeAuthPairingLinkDbRow(row).pipe(
              Effect.mapError((cause) =>
                PersistenceDecodeError.fromSchemaError(
                  "AuthPairingLinkRepository.listActive:decodeRows",
                  cause,
                  { pairingLinkId: row.id },
                ),
              ),
            ),
          ),
        ),
      ),
    ),
    revoke: Effect.fn("AuthPairingLinkRepository.revoke")((input) =>
      Schema.encodeEffect(Links.RevokeAuthPairingLinkInput)(input).pipe(
        Effect.mapError((cause) =>
          PersistenceDecodeError.fromSchemaError(
            "AuthPairingLinkRepository.revoke:decodeRows",
            cause,
            { pairingLinkId: input.id },
          ),
        ),
        Effect.flatMap(({ id, revokedAt }) =>
          request(
            false,
            { kind: "pairing_revoke", id, revoked_at: revokedAt },
            Schema.Boolean,
            "revoke",
            { pairingLinkId: id },
          ),
        ),
      ),
    ),
    getByCredential: Effect.fn("AuthPairingLinkRepository.getByCredential")((input) =>
      Schema.encodeEffect(Links.GetAuthPairingLinkByCredentialInput)(input).pipe(
        Effect.mapError((cause) =>
          PersistenceDecodeError.fromSchemaError(
            "AuthPairingLinkRepository.getByCredential:decodeRow",
            cause,
          ),
        ),
        Effect.flatMap(({ credential }) =>
          request(
            true,
            { kind: "pairing_get", credential },
            Schema.NullOr(Links.AuthPairingLinkRawDbRow),
            "getByCredential",
          ),
        ),
        Effect.flatMap((row) => decodeOption("getByCredential", row)),
      ),
    ),
  });
});

export const layer = Layer.effect(Links.AuthPairingLinkRepository, make);
