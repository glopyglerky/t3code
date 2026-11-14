import { AuthSessionId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";

import * as Sessions from "./AuthSessions.ts";
import { PersistenceDecodeError, type PersistenceErrorCorrelation } from "./Errors.ts";
import { RedbClient } from "./RedbClient.ts";

const Written = Schema.Struct({ written: Schema.Literal(true) });
const encodeCreateAuthSessionInput = Schema.encodeEffect(Sessions.CreateAuthSessionInput);
const encodeCreateReplacingActiveAuthSessionInput = Schema.encodeEffect(
  Sessions.CreateReplacingActiveAuthSessionInput,
);
const encodeGetAuthSessionByIdInput = Schema.encodeEffect(Sessions.GetAuthSessionByIdInput);
const encodeListActiveAuthSessionsInput = Schema.encodeEffect(Sessions.ListActiveAuthSessionsInput);
const encodeRevokeAuthSessionInput = Schema.encodeEffect(Sessions.RevokeAuthSessionInput);
const encodeRevokeOtherAuthSessionsInput = Schema.encodeEffect(
  Sessions.RevokeOtherAuthSessionsInput,
);
const encodeSetAuthSessionLastConnectedAtInput = Schema.encodeEffect(
  Sessions.SetAuthSessionLastConnectedAtInput,
);
const encodeRevokeAuthSessionsByProofKeyInput = Schema.encodeEffect(
  Sessions.RevokeAuthSessionsByProofKeyInput,
);
const requestPhases = {
  create: "encodeRequest",
  createIfAbsent: "encodeRequest",
  createReplacingActive: "encodeRequest",
  getById: "decodeRow",
  listActive: "decodeRows",
  revoke: "decodeRows",
  revokeAllExcept: "decodeRows",
  setLastConnectedAt: "encodeRequest",
  getProofKeyThumbprint: "decodeRows",
  revokeByProofKey: "decodeRows",
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
    transactional = false,
  ) => {
    const operation = client.request(
      read ? "application_auth_read" : "application_auth_write",
      { request: input },
      response,
    );
    return (transactional ? client.withTransaction(operation) : operation).pipe(
      Effect.mapError(
        Sessions.toPersistenceSqlOrDecodeError(
          `AuthSessionRepository.${method}:query`,
          `AuthSessionRepository.${method}:${requestPhases[method]}`,
          correlation,
        ),
      ),
    );
  };

  const create = (method: "create" | "createIfAbsent", input: Sessions.CreateAuthSessionInput) =>
    encodeCreateAuthSessionInput(input).pipe(
      Effect.mapError((cause) =>
        PersistenceDecodeError.fromSchemaError(
          `AuthSessionRepository.${method}:encodeRequest`,
          cause,
          { sessionId: input.sessionId },
        ),
      ),
      Effect.flatMap((session) =>
        request(
          false,
          {
            kind: "session_create",
            session,
            if_absent: method === "createIfAbsent",
          },
          Written,
          method,
          { sessionId: input.sessionId },
        ),
      ),
      Effect.asVoid,
    );

  return Sessions.AuthSessionRepository.of({
    create: Effect.fn("AuthSessionRepository.create")((input) => create("create", input)),
    createIfAbsent: Effect.fn("AuthSessionRepository.createIfAbsent")((input) =>
      create("createIfAbsent", input),
    ),
    createReplacingActive: Effect.fn("AuthSessionRepository.createReplacingActive")((input) =>
      encodeCreateReplacingActiveAuthSessionInput(input).pipe(
        Effect.mapError((cause) =>
          PersistenceDecodeError.fromSchemaError(
            "AuthSessionRepository.createReplacingActive:encodeRequest",
            cause,
            { sessionId: input.session.sessionId },
          ),
        ),
        Effect.flatMap(({ session, revokedAt, replaceSessionId }) =>
          request(
            false,
            {
              kind: "session_create_replacing",
              session,
              revoked_at: revokedAt,
              replace_session_id: replaceSessionId ?? null,
            },
            Schema.Array(AuthSessionId),
            "createReplacingActive",
            { sessionId: input.session.sessionId },
            true,
          ),
        ),
      ),
    ),
    getById: Effect.fn("AuthSessionRepository.getById")((input) =>
      encodeGetAuthSessionByIdInput(input).pipe(
        Effect.mapError((cause) =>
          PersistenceDecodeError.fromSchemaError("AuthSessionRepository.getById:decodeRow", cause, {
            sessionId: input.sessionId,
          }),
        ),
        Effect.flatMap(({ sessionId }) =>
          request(
            true,
            { kind: "session_get", session_id: sessionId },
            Schema.NullOr(Sessions.AuthSessionRawDbRow),
            "getById",
            { sessionId },
          ),
        ),
        Effect.flatMap((row) =>
          row === null
            ? Effect.succeedNone
            : Sessions.decodeAuthSessionDbRow(row).pipe(
                Effect.mapError((cause) =>
                  PersistenceDecodeError.fromSchemaError(
                    "AuthSessionRepository.getById:decodeRow",
                    cause,
                    { sessionId: input.sessionId },
                  ),
                ),
                Effect.map((decoded) => Option.some(Sessions.toAuthSessionRecord(decoded))),
              ),
        ),
      ),
    ),
    listActive: Effect.fn("AuthSessionRepository.listActive")((input) =>
      encodeListActiveAuthSessionsInput(input).pipe(
        Effect.mapError((cause) =>
          PersistenceDecodeError.fromSchemaError(
            "AuthSessionRepository.listActive:decodeRows",
            cause,
          ),
        ),
        Effect.flatMap(({ now, connectedSessionIds }) =>
          request(
            true,
            {
              kind: "session_list_active",
              now,
              connected_session_ids: connectedSessionIds ?? [],
            },
            Schema.Array(Sessions.AuthSessionRawDbRow),
            "listActive",
          ),
        ),
        Effect.flatMap((rows) =>
          Effect.forEach(rows, (row) =>
            Sessions.decodeAuthSessionDbRow(row).pipe(
              Effect.mapError((cause) =>
                PersistenceDecodeError.fromSchemaError(
                  "AuthSessionRepository.listActive:decodeRows",
                  cause,
                  { sessionId: row.sessionId },
                ),
              ),
              Effect.map(Sessions.toAuthSessionRecord),
            ),
          ),
        ),
      ),
    ),
    revoke: Effect.fn("AuthSessionRepository.revoke")((input) =>
      encodeRevokeAuthSessionInput(input).pipe(
        Effect.mapError((cause) =>
          PersistenceDecodeError.fromSchemaError("AuthSessionRepository.revoke:decodeRows", cause, {
            sessionId: input.sessionId,
          }),
        ),
        Effect.flatMap(({ sessionId, revokedAt }) =>
          request(
            false,
            {
              kind: "session_revoke",
              session_id: sessionId,
              revoked_at: revokedAt,
            },
            Schema.Boolean,
            "revoke",
            { sessionId },
          ),
        ),
      ),
    ),
    revokeAllExcept: Effect.fn("AuthSessionRepository.revokeAllExcept")((input) =>
      encodeRevokeOtherAuthSessionsInput(input).pipe(
        Effect.mapError((cause) =>
          PersistenceDecodeError.fromSchemaError(
            "AuthSessionRepository.revokeAllExcept:decodeRows",
            cause,
            { currentSessionId: input.currentSessionId },
          ),
        ),
        Effect.flatMap(({ currentSessionId, revokedAt }) =>
          request(
            false,
            {
              kind: "session_revoke_all_except",
              current_session_id: currentSessionId,
              revoked_at: revokedAt,
            },
            Schema.Array(AuthSessionId),
            "revokeAllExcept",
            { currentSessionId },
          ),
        ),
      ),
    ),
    setLastConnectedAt: Effect.fn("AuthSessionRepository.setLastConnectedAt")((input) =>
      encodeSetAuthSessionLastConnectedAtInput(input).pipe(
        Effect.mapError((cause) =>
          PersistenceDecodeError.fromSchemaError(
            "AuthSessionRepository.setLastConnectedAt:encodeRequest",
            cause,
            { sessionId: input.sessionId },
          ),
        ),
        Effect.flatMap(({ sessionId, lastConnectedAt }) =>
          request(
            false,
            {
              kind: "session_set_last_connected",
              session_id: sessionId,
              last_connected_at: lastConnectedAt,
            },
            Written,
            "setLastConnectedAt",
            { sessionId },
          ),
        ),
        Effect.asVoid,
      ),
    ),
    getProofKeyThumbprint: Effect.fn("AuthSessionRepository.getProofKeyThumbprint")((input) =>
      encodeGetAuthSessionByIdInput(input).pipe(
        Effect.mapError((cause) =>
          PersistenceDecodeError.fromSchemaError(
            "AuthSessionRepository.getProofKeyThumbprint:decodeRows",
            cause,
            { sessionId: input.sessionId },
          ),
        ),
        Effect.flatMap(({ sessionId }) =>
          request(
            true,
            { kind: "session_get_proof_key", session_id: sessionId },
            Schema.NullOr(Schema.String),
            "getProofKeyThumbprint",
            { sessionId },
          ),
        ),
        Effect.map(Option.fromNullishOr),
      ),
    ),
    revokeByProofKey: Effect.fn("AuthSessionRepository.revokeByProofKey")((input) =>
      encodeRevokeAuthSessionsByProofKeyInput(input).pipe(
        Effect.mapError((cause) =>
          PersistenceDecodeError.fromSchemaError(
            "AuthSessionRepository.revokeByProofKey:decodeRows",
            cause,
          ),
        ),
        Effect.flatMap(({ proofKeyThumbprint, revokedAt }) =>
          request(
            false,
            {
              kind: "session_revoke_by_proof_key",
              proof_key_thumbprint: proofKeyThumbprint,
              revoked_at: revokedAt,
            },
            Schema.Array(AuthSessionId),
            "revokeByProofKey",
          ),
        ),
      ),
    ),
  });
});

export const layer = Layer.effect(Sessions.AuthSessionRepository, make);
