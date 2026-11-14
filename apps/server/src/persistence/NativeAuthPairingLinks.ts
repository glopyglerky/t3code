import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";

import * as Links from "./AuthPairingLinks.ts";
import { PersistenceDecodeError, type PersistenceErrorCorrelation } from "./Errors.ts";
import { RedbClient } from "./RedbClient.ts";

const Written = Schema.Struct({ written: Schema.Literal(true) });
const encodeCreateAuthPairingLinkInput = Schema.encodeEffect(Links.CreateAuthPairingLinkInput);
const encodeConsumeAuthPairingLinkInput = Schema.encodeEffect(Links.ConsumeAuthPairingLinkInput);
const encodeListActiveAuthPairingLinksInput = Schema.encodeEffect(
  Links.ListActiveAuthPairingLinksInput,
);
const encodeRevokeAuthPairingLinkInput = Schema.encodeEffect(Links.RevokeAuthPairingLinkInput);
const encodeGetAuthPairingLinkByCredentialInput = Schema.encodeEffect(
  Links.GetAuthPairingLinkByCredentialInput,
);
const encodeGetActiveAuthPairingLinkByIdInput = Schema.encodeEffect(
  Links.GetActiveAuthPairingLinkByIdInput,
);
const encodeFindPendingConnectPairingLinkInput = Schema.encodeEffect(
  Links.FindPendingConnectPairingLinkInput,
);
const encodeRecordConnectRequestClientInput = Schema.encodeEffect(
  Links.RecordConnectRequestClientInput,
);
const encodeRevokeUnspentByProofKeyInput = Schema.encodeEffect(Links.RevokeUnspentByProofKeyInput);
const requestPhases = {
  create: "encodeRequest",
  consumeAvailable: "decodeRow",
  listActive: "decodeRows",
  revoke: "decodeRows",
  getByCredential: "decodeRow",
  getActiveById: "decodeRow",
  findPendingConnect: "decodeRow",
  recordConnectRequestClient: "encodeRequest",
  revokeUnspentByProofKey: "decodeRows",
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
      encodeCreateAuthPairingLinkInput(input).pipe(
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
      encodeConsumeAuthPairingLinkInput(input).pipe(
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
      encodeListActiveAuthPairingLinksInput(input).pipe(
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
      encodeRevokeAuthPairingLinkInput(input).pipe(
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
      encodeGetAuthPairingLinkByCredentialInput(input).pipe(
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
    getActiveById: Effect.fn("AuthPairingLinkRepository.getActiveById")((input) =>
      encodeGetActiveAuthPairingLinkByIdInput(input).pipe(
        Effect.mapError((cause) =>
          PersistenceDecodeError.fromSchemaError(
            "AuthPairingLinkRepository.getActiveById:decodeRow",
            cause,
            { pairingLinkId: input.id },
          ),
        ),
        Effect.flatMap(({ id, now }) =>
          request(
            true,
            { kind: "pairing_get_active", id, now },
            Schema.NullOr(Links.AuthPairingLinkRawDbRow),
            "getActiveById",
            { pairingLinkId: id },
          ),
        ),
        Effect.flatMap((row) => decodeOption("getActiveById", row)),
      ),
    ),
    findPendingConnect: Effect.fn("AuthPairingLinkRepository.findPendingConnect")((input) =>
      encodeFindPendingConnectPairingLinkInput(input).pipe(
        Effect.mapError((cause) =>
          PersistenceDecodeError.fromSchemaError(
            "AuthPairingLinkRepository.findPendingConnect:decodeRow",
            cause,
          ),
        ),
        Effect.flatMap(({ proofKeyThumbprint, now }) =>
          request(
            true,
            { kind: "pairing_find_pending_connect", proof_key_thumbprint: proofKeyThumbprint, now },
            Schema.NullOr(Links.AuthPairingLinkRawDbRow),
            "findPendingConnect",
          ),
        ),
        Effect.flatMap((row) => decodeOption("findPendingConnect", row)),
      ),
    ),
    recordConnectRequestClient: Effect.fn("AuthPairingLinkRepository.recordConnectRequestClient")(
      (input) =>
        encodeRecordConnectRequestClientInput(input).pipe(
          Effect.mapError((cause) =>
            PersistenceDecodeError.fromSchemaError(
              "AuthPairingLinkRepository.recordConnectRequestClient:encodeRequest",
              cause,
              { pairingLinkId: input.id },
            ),
          ),
          Effect.flatMap(({ id, client }) =>
            request(
              false,
              { kind: "pairing_record_connect_client", id, client },
              Schema.Boolean,
              "recordConnectRequestClient",
              { pairingLinkId: id },
            ),
          ),
        ),
    ),
    revokeUnspentByProofKey: Effect.fn("AuthPairingLinkRepository.revokeUnspentByProofKey")(
      (input) =>
        encodeRevokeUnspentByProofKeyInput(input).pipe(
          Effect.mapError((cause) =>
            PersistenceDecodeError.fromSchemaError(
              "AuthPairingLinkRepository.revokeUnspentByProofKey:decodeRows",
              cause,
            ),
          ),
          Effect.flatMap(({ proofKeyThumbprint, keepProofKeyThumbprint, revokedAt }) =>
            request(
              false,
              {
                kind: "pairing_revoke_unspent_by_proof_key",
                proof_key_thumbprint: proofKeyThumbprint,
                keep_proof_key_thumbprint: keepProofKeyThumbprint,
                revoked_at: revokedAt,
              },
              Schema.Array(Schema.String),
              "revokeUnspentByProofKey",
            ),
          ),
        ),
    ),
  });
});

export const layer = Layer.effect(Links.AuthPairingLinkRepository, make);
