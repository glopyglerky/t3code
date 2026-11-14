#!/usr/bin/env node
import * as NodeAssert from "node:assert/strict";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeSqlite from "node:sqlite";
import * as Contracts from "@t3tools/contracts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as SqlSchema from "effect/sql/SqlSchema";

import * as Sessions from "../apps/server/src/persistence/AuthSessions.ts";
import * as Links from "../apps/server/src/persistence/AuthPairingLinks.ts";
import * as NativeSessions from "../apps/server/src/persistence/NativeAuthSessions.ts";
import * as NativeLinks from "../apps/server/src/persistence/NativeAuthPairingLinks.ts";
import * as Devices from "../apps/server/src/persistence/AuthConnectDevices.ts";
import * as NativeDevices from "../apps/server/src/persistence/NativeAuthConnectDevices.ts";
import * as Errors from "../apps/server/src/persistence/Errors.ts";
import { createFixture } from "./database-fixture.mjs";
import { exportSqliteState } from "./export-sqlite-state.mjs";
import {
  createMigratedSqlite,
  finishProof,
  hash,
  loadUpstream,
  nativeRequest,
  normalized,
  withNative,
  withSqlite,
} from "./redb-domain-fixture.mjs";

const [executable, proofPath] = process.argv.slice(2);
NodeAssert.ok(
  executable && NodePath.isAbsolute(executable),
  "Pass an absolute native database executable",
);
await NodeFSP.access(executable, NodeFSP.constants.X_OK);
const at = "2026-11-10T12:00:00.000Z";
const future = "2027-01-01T00:00:00.000Z";
const now = DateTime.makeUnsafe(at);
const imports = {
  ...Contracts,
  ...Errors,
  Context,
  Effect,
  Layer,
  Option,
  Schema,
  SqlClient,
  SqlSchema,
  NodeSqliteClient,
};
const sessionOracle = loadUpstream("apps/server/src/persistence/AuthSessions.ts", imports, [
  "make",
]);
const linkOracle = loadUpstream("apps/server/src/persistence/AuthPairingLinks.ts", imports, [
  "make",
]);
const deviceOracle = loadUpstream("apps/server/src/persistence/AuthConnectDevices.ts", imports, [
  "make",
]);
const decodeSessionInput = Schema.decodeUnknownSync(Sessions.CreateAuthSessionInput);
const decodeLinkInput = Schema.decodeUnknownSync(Links.CreateAuthPairingLinkInput);
const decodeClientMetadata = Schema.decodeUnknownSync(Contracts.AuthClientMetadata);
const fixture = await createFixture();
const checks = [];
const expectFailure = (effect) =>
  Effect.exit(effect).pipe(
    Effect.tap((exit) => Effect.sync(() => NodeAssert.ok(Exit.isFailure(exit)))),
  );
const session = (id, fields = {}) =>
  decodeSessionInput({
    sessionId: id,
    subject: "owner",
    scopes: ["orchestration:read"],
    method: "bearer-access-token",
    client: {
      label: "Auth fixture",
      ipAddress: "127.0.0.1",
      userAgent: "fixture",
      deviceType: "desktop",
      os: null,
      browser: null,
    },
    issuedAt: at,
    expiresAt: future,
    ...fields,
  });
const link = (id, fields = {}) =>
  decodeLinkInput({
    id,
    credential: `code:${id}`,
    method: "one-time-token",
    scopes: ["orchestration:read", "access:read"],
    subject: "owner",
    label: "",
    proofKeyThumbprint: "proof",
    createdAt: at,
    expiresAt: future,
    ...fields,
  });
const consume = (id, fields = {}) => ({
  credential: `code:${id}`,
  proofKeyThumbprint: "proof",
  requestedScopes: ["access:read"],
  requiredScopes: ["orchestration:read"],
  consumedAt: now,
  now,
  ...fields,
});

async function withServices(kind, filename, use, target = fixture) {
  const construct = Effect.fn("AuthFixture.construct")(function* (transaction) {
    const sessions = yield* kind === "omdb"
      ? NativeSessions.make
      : kind === "upstream"
        ? sessionOracle.module.make
        : Sessions.make;
    const links = yield* kind === "omdb"
      ? NativeLinks.make
      : kind === "upstream"
        ? linkOracle.module.make
        : Links.make;
    const devices = yield* kind === "omdb"
      ? NativeDevices.make
      : kind === "upstream"
        ? deviceOracle.module.make
        : Devices.make;
    return yield* use({ sessions, links, devices, transaction });
  });
  return kind === "omdb"
    ? withNative(executable, target, (client) => construct((body) => client.withTransaction(body)))
    : withSqlite(filename, (sql) => construct((body) => sql.withTransaction(body)));
}

const exercise = Effect.fn("AuthFixture.exercise")(function* ({
  sessions,
  links,
  devices,
  transaction,
}) {
  const result = {};
  result.initialSessions = yield* sessions.listActive({ now });
  NodeAssert.equal(result.initialSessions.length, 130);
  result.initialLinks = yield* links.listActive({ now });
  NodeAssert.equal(result.initialLinks.length, 130);
  result.initialDevices = yield* devices.list();
  NodeAssert.equal(result.initialDevices.length, 130);
  NodeAssert.equal(yield* devices.isTrusted("missing"), false);
  NodeAssert.equal(yield* devices.remove("missing"), false);
  NodeAssert.ok(Option.isNone(yield* sessions.getById({ sessionId: "missing" })));
  NodeAssert.ok(Option.isNone(yield* links.getByCredential({ credential: "missing" })));
  result.connectedExpired = yield* sessions.listActive({
    now,
    connectedSessionIds: ["imported:expired", "imported:expired", "imported:revoked", "missing"],
  });
  NodeAssert.equal(result.connectedExpired.length, 131);

  yield* sessions.create(session("written"));
  yield* expectFailure(sessions.create(session("written")));
  yield* sessions.createIfAbsent(session("written", { subject: "must-not-replace" }));
  result.written = yield* sessions.getById({ sessionId: "written" });
  NodeAssert.equal(result.written.value.subject, "owner");
  yield* sessions.setLastConnectedAt({ sessionId: "written", lastConnectedAt: now });
  result.connected = yield* sessions.getById({ sessionId: "written" });
  NodeAssert.equal(DateTime.formatIso(result.connected.value.lastConnectedAt), at);
  NodeAssert.equal(yield* sessions.revoke({ sessionId: "written", revokedAt: now }), true);
  NodeAssert.equal(yield* sessions.revoke({ sessionId: "written", revokedAt: now }), false);
  yield* sessions.setLastConnectedAt({
    sessionId: "written",
    lastConnectedAt: DateTime.makeUnsafe(future),
  });
  result.revoked = yield* sessions.getById({ sessionId: "written" });
  NodeAssert.equal(DateTime.formatIso(result.revoked.value.lastConnectedAt), at);

  yield* sessions.create(session("replace:a", { subject: "replace" }));
  yield* sessions.create(session("replace:b", { subject: "replace" }));
  yield* expectFailure(
    sessions.createReplacingActive({
      session: session("replace:b", { subject: "replace" }),
      revokedAt: now,
    }),
  );
  NodeAssert.equal(
    Option.getOrThrow(yield* sessions.getById({ sessionId: "replace:a" })).revokedAt,
    null,
  );
  result.replacement = [
    ...(yield* sessions.createReplacingActive({
      session: session("replace:c", { subject: "replace" }),
      revokedAt: now,
    })),
  ].sort();
  NodeAssert.deepEqual(result.replacement, ["replace:a", "replace:b"]);
  yield* transaction(
    Effect.gen(function* () {
      yield* expectFailure(
        sessions.createReplacingActive({
          session: session("replace:c", { subject: "replace" }),
          revokedAt: now,
        }),
      );
      NodeAssert.equal(
        Option.getOrThrow(yield* sessions.getById({ sessionId: "replace:c" })).revokedAt,
        null,
      );
      yield* sessions.create(session("after-rejected-replacement", { subject: "outer-survives" }));
    }),
  );
  const requestedClient = decodeClientMetadata({
    label: "Auth fixture",
    ipAddress: "127.0.0.1",
    userAgent: "fixture",
    deviceType: "desktop",
  });
  const connectLink = (id, proof = "connect-proof") =>
    link(id, {
      requiresConnectApproval: true,
      proofKeyThumbprint: proof,
    });
  const connectConsume = (id, proof = "connect-proof") =>
    consume(id, {
      proofKeyThumbprint: proof,
    });
  yield* links.create(connectLink("connect-waiting"));
  NodeAssert.ok(Option.isNone(yield* links.consumeAvailable(connectConsume("connect-waiting"))));
  NodeAssert.ok(Option.isNone(yield* links.getActiveById({ id: "missing", now })));
  NodeAssert.ok(
    Option.isNone(
      yield* links.getActiveById({ id: "connect-waiting", now: DateTime.makeUnsafe(future) }),
    ),
  );
  result.waiting = yield* links.getActiveById({ id: "connect-waiting", now });
  NodeAssert.equal(result.waiting.value.connectApproved, 0);
  NodeAssert.equal(
    yield* links.recordConnectRequestClient({ id: "connect-waiting", client: requestedClient }),
    true,
  );
  NodeAssert.equal(
    yield* links.recordConnectRequestClient({
      id: "connect-waiting",
      client: { ...requestedClient, label: "changed" },
    }),
    false,
  );
  NodeAssert.equal(
    yield* links.recordConnectRequestClient({ id: "missing", client: requestedClient }),
    false,
  );
  result.pending = yield* links.findPendingConnect({ proofKeyThumbprint: "connect-proof", now });
  NodeAssert.equal(result.pending.value.requestedClient.label, requestedClient.label);
  yield* devices.trust({ proofKeyThumbprint: "connect-proof", client: null, approvedAt: now });
  yield* devices.trust({
    proofKeyThumbprint: "connect-proof",
    client: requestedClient,
    approvedAt: DateTime.makeUnsafe(future),
  });
  yield* devices.trust({
    proofKeyThumbprint: "connect-proof",
    client: null,
    approvedAt: DateTime.makeUnsafe(future),
  });
  NodeAssert.equal(yield* devices.isTrusted("connect-proof"), true);
  result.trusted = (yield* devices.list()).find(
    (device) => device.proofKeyThumbprint === "connect-proof",
  );
  NodeAssert.equal(DateTime.formatIso(result.trusted.approvedAt), at);
  NodeAssert.equal(result.trusted.client.label, requestedClient.label);
  result.connectConsumed = yield* links.consumeAvailable(connectConsume("connect-waiting"));
  NodeAssert.equal(result.connectConsumed.value.connectApproved, 1);
  NodeAssert.ok(
    Option.isNone(yield* links.findPendingConnect({ proofKeyThumbprint: "connect-proof", now })),
  );

  yield* links.create(connectLink("denied:a", "denied-proof"));
  yield* links.create(connectLink("denied:b", "denied-proof"));
  NodeAssert.equal(yield* links.revoke({ id: "denied:b", revokedAt: now }), true);
  result.denied = yield* links.findPendingConnect({ proofKeyThumbprint: "denied-proof", now });
  NodeAssert.equal(result.denied.value.id, "denied:b");
  NodeAssert.ok(
    Option.isNone(yield* links.consumeAvailable(connectConsume("denied:b", "denied-proof"))),
  );
  result.revokeWaiting = [
    ...(yield* links.revokeUnspentByProofKey({
      proofKeyThumbprint: "denied-proof",
      keepProofKeyThumbprint: null,
      revokedAt: now,
    })),
  ].sort();
  NodeAssert.deepEqual(result.revokeWaiting, ["denied:a"]);
  NodeAssert.equal(
    (yield* links.findPendingConnect({ proofKeyThumbprint: "denied-proof", now })).value.id,
    "denied:b",
  );
  yield* links.create(connectLink("device-revoked", "revoked-proof"));
  NodeAssert.deepEqual(
    yield* links.revokeUnspentByProofKey({
      proofKeyThumbprint: "revoked-proof",
      keepProofKeyThumbprint: "revoked-proof",
      revokedAt: now,
    }),
    [],
  );
  NodeAssert.deepEqual(
    yield* links.revokeUnspentByProofKey({
      proofKeyThumbprint: "revoked-proof",
      keepProofKeyThumbprint: null,
      revokedAt: now,
    }),
    ["device-revoked"],
  );
  NodeAssert.ok(
    Option.isNone(yield* links.findPendingConnect({ proofKeyThumbprint: "revoked-proof", now })),
  );

  yield* sessions.create(
    session("proof-expired", {
      proofKeyThumbprint: "connect-proof",
      expiresAt: "2020-01-01T00:00:00.000Z",
    }),
  );
  yield* sessions.create(session("proof-live", { proofKeyThumbprint: "connect-proof" }));
  yield* sessions.create(session("proof-other", { proofKeyThumbprint: "other-proof" }));
  NodeAssert.equal(
    Option.getOrThrow(yield* sessions.getProofKeyThumbprint({ sessionId: "proof-expired" })),
    "connect-proof",
  );
  NodeAssert.ok(Option.isNone(yield* sessions.getProofKeyThumbprint({ sessionId: "missing" })));
  NodeAssert.ok(Option.isNone(yield* sessions.getProofKeyThumbprint({ sessionId: "written" })));
  result.revokedByProof = [
    ...(yield* sessions.revokeByProofKey({
      proofKeyThumbprint: "connect-proof",
      revokedAt: now,
    })),
  ].sort();
  NodeAssert.deepEqual(result.revokedByProof, ["proof-expired", "proof-live"]);
  NodeAssert.equal(
    Option.getOrThrow(yield* sessions.getProofKeyThumbprint({ sessionId: "proof-expired" })),
    "connect-proof",
  );
  NodeAssert.equal(
    Option.getOrThrow(yield* sessions.getById({ sessionId: "proof-other" })).revokedAt,
    null,
  );

  yield* links.create(connectLink("rolled-back", "rolled-back-proof"));
  yield* expectFailure(
    transaction(
      Effect.gen(function* () {
        yield* devices.trust({
          proofKeyThumbprint: "rolled-back-proof",
          client: requestedClient,
          approvedAt: now,
        });
        NodeAssert.ok(
          Option.isSome(
            yield* links.consumeAvailable(connectConsume("rolled-back", "rolled-back-proof")),
          ),
        );
        return yield* Effect.fail("roll back device approval and pairing consumption together");
      }),
    ),
  );
  NodeAssert.equal(yield* devices.isTrusted("rolled-back-proof"), false);
  NodeAssert.equal(
    Option.getOrThrow(yield* links.getByCredential({ credential: "code:rolled-back" })).consumedAt,
    null,
  );
  yield* transaction(
    Effect.gen(function* () {
      yield* expectFailure(
        transaction(
          Effect.gen(function* () {
            NodeAssert.equal(yield* devices.remove("connect-proof"), true);
            return yield* Effect.fail("roll back device removal savepoint");
          }),
        ),
      );
      NodeAssert.equal(yield* devices.isTrusted("connect-proof"), true);
    }),
  );
  result.removedDevices = yield* devices.removeAllExcept("connect-proof");
  NodeAssert.equal(result.removedDevices, 130);
  result.keptDevices = yield* devices.list();
  NodeAssert.deepEqual(
    result.keptDevices.map((device) => device.proofKeyThumbprint),
    ["connect-proof"],
  );
  NodeAssert.equal(yield* devices.remove("connect-proof"), true);
  NodeAssert.equal(yield* devices.remove("connect-proof"), false);
  yield* devices.trust({ proofKeyThumbprint: "remove-all", client: null, approvedAt: now });
  NodeAssert.equal(yield* devices.removeAllExcept(null), 1);
  NodeAssert.deepEqual(yield* devices.list(), []);
  yield* sessions.create(session("replace:other", { subject: "other" }));
  result.targeted = yield* sessions.createReplacingActive({
    session: session("replace:target", { subject: "new-subject" }),
    replaceSessionId: "replace:other",
    revokedAt: now,
  });
  NodeAssert.deepEqual(result.targeted, ["replace:other"]);

  yield* links.create(link("written"));
  yield* expectFailure(links.create(link("other-id", { credential: "code:written" })));
  for (const rejected of [
    { proofKeyThumbprint: null },
    { proofKeyThumbprint: "wrong" },
    { requestedScopes: [] },
    { requestedScopes: ["access:write"] },
    { requiredScopes: ["access:write"] },
    { now: DateTime.makeUnsafe(future) },
  ])
    NodeAssert.ok(Option.isNone(yield* links.consumeAvailable(consume("written", rejected))));
  result.beforeConsume = yield* links.getByCredential({ credential: "code:written" });
  NodeAssert.equal(result.beforeConsume.value.consumedAt, null);
  result.consumed = yield* links.consumeAvailable(consume("written"));
  NodeAssert.ok(Option.isSome(result.consumed));
  NodeAssert.ok(Option.isNone(yield* links.consumeAvailable(consume("written"))));
  NodeAssert.equal(yield* links.revoke({ id: "written", revokedAt: now }), false);
  yield* links.create(link("revoked", { proofKeyThumbprint: null }));
  NodeAssert.equal(yield* links.revoke({ id: "revoked", revokedAt: now }), true);
  NodeAssert.equal(yield* links.revoke({ id: "revoked", revokedAt: now }), false);
  NodeAssert.ok(Option.isNone(yield* links.consumeAvailable(consume("revoked"))));
  yield* links.create(link("approval", { subject: "MCP-APPROVAL:read" }));
  NodeAssert.ok(Option.isNone(yield* links.consumeAvailable(consume("approval"))));
  NodeAssert.ok(
    Option.isNone(
      yield* links.consumeAvailable(
        consume("approval", { mcpApprovalSubject: "mcp-approval:read" }),
      ),
    ),
  );
  result.approval = yield* links.consumeAvailable(
    consume("approval", { mcpApprovalSubject: "MCP-APPROVAL:read" }),
  );
  NodeAssert.ok(Option.isSome(result.approval));

  yield* links.create(link("transaction"));
  yield* expectFailure(
    transaction(
      Effect.gen(function* () {
        NodeAssert.ok(Option.isSome(yield* links.consumeAvailable(consume("transaction"))));
        yield* sessions.create(session("transaction"));
        return yield* Effect.fail("roll back pairing and session together");
      }),
    ),
  );
  NodeAssert.ok(Option.isNone(yield* sessions.getById({ sessionId: "transaction" })));
  NodeAssert.equal(
    Option.getOrThrow(yield* links.getByCredential({ credential: "code:transaction" })).consumedAt,
    null,
  );
  yield* transaction(
    Effect.gen(function* () {
      NodeAssert.ok(Option.isSome(yield* links.consumeAvailable(consume("transaction"))));
      yield* sessions.create(session("transaction"));
      yield* expectFailure(
        transaction(
          Effect.gen(function* () {
            yield* sessions.revoke({ sessionId: "transaction", revokedAt: now });
            return yield* Effect.fail("savepoint rollback");
          }),
        ),
      );
      NodeAssert.equal(
        Option.getOrThrow(yield* sessions.getById({ sessionId: "transaction" })).revokedAt,
        null,
      );
    }),
  );
  result.revokeOthers = [
    ...(yield* sessions.revokeAllExcept({ currentSessionId: "transaction", revokedAt: now })),
  ].sort();
  result.finalSessions = yield* sessions.listActive({ now });
  NodeAssert.deepEqual(
    result.finalSessions.map((row) => row.sessionId),
    ["transaction"],
  );
  result.finalLinks = yield* links.listActive({ now });
  return normalized(result);
});

try {
  const source = NodePath.join(fixture.root, "source.sqlite");
  const sqlitePath = NodePath.join(fixture.root, "sqlite.sqlite");
  const upstreamPath = NodePath.join(fixture.root, "upstream.sqlite");
  const exported = NodePath.join(fixture.root, "source.ndjson");
  await createMigratedSqlite(source);
  const database = new NodeSqlite.DatabaseSync(source);
  try {
    const sessionInsert = database.prepare(`INSERT INTO auth_sessions
      (session_id,subject,scopes,method,client_device_type,issued_at,expires_at,revoked_at,client_surface,client_app_version)
      VALUES (?, 'imported', '["orchestration:read"]', 'bearer-access-token', 'unknown', ?, ?, ?, 'web', 'fixture-version')`);
    const linkInsert = database.prepare(`INSERT INTO auth_pairing_links
      (id,credential,method,scopes,subject,created_at,expires_at)
      VALUES (?, ?, 'one-time-token', '["orchestration:read"]', 'imported', ?, ?)`);
    const deviceInsert = database.prepare(`INSERT INTO auth_connect_devices
      (proof_key_thumbprint,client_json,approved_at) VALUES (?,NULL,?)`);
    for (let i = 0; i < 130; i++) {
      const id = `imported:${String(i).padStart(4, "0")}`;
      sessionInsert.run(id, at, future, null);
      linkInsert.run(id, `code:${id}`, at, future);
      deviceInsert.run(`proof:${id}`, at);
    }
    sessionInsert.run("imported:expired", at, "2020-01-01T00:00:00.000Z", null);
    sessionInsert.run("imported:revoked", at, future, at);
  } finally {
    database.close();
  }
  const sourceHash = hash(await NodeFSP.readFile(source));
  await NodeFSP.copyFile(source, sqlitePath);
  await NodeFSP.copyFile(source, upstreamPath);
  exportSqliteState({ source, destination: exported });
  nativeRequest(executable, fixture, "initialize", {
    operation: "import_sqlite_application",
    path: exported,
  });
  const expected = await withServices("upstream", upstreamPath, exercise);
  NodeAssert.deepEqual(await withServices("sqlite", sqlitePath, exercise), expected);
  NodeAssert.deepEqual(await withServices("omdb", source, exercise), expected);
  checks.push(
    "All 10 auth-session, 9 pairing-link and 5 Connect-device repository members match immutable upstream SQLite and retained SQLite",
    "Imported active indexes span multiple pages and retain raw rows, legacy scopes and extra client-connection columns",
    "Expiry, connected expired sessions, revocation, create-if-absent and targeted/subject replacements preserve upstream behavior",
    "Wrong proof keys, requested/required scope failures and reserved approval subjects cannot consume pairing codes",
    "Shared and nested application transactions commit or roll back pairing consumption and session issuance together",
    "Pending approvals require a trusted key, denied requests survive retries, and device revocation does not create a denial",
    "Connect trust preserves the initial approval time and non-null client metadata; session proof revocation includes expired bound sessions",
    "Native device preparation spans multiple pages and transaction/savepoint rollback preserves cross-repository trust and pairing state",
  );
  await withServices("omdb", source, ({ sessions, links }) =>
    Effect.gen(function* () {
      NodeAssert.deepEqual(normalized(yield* sessions.listActive({ now })), expected.finalSessions);
      NodeAssert.deepEqual(normalized(yield* links.listActive({ now })), expected.finalLinks);
    }),
  );
  checks.push(
    "A reopened native helper retains committed authentication state without re-preparing live indexes",
  );
  NodeAssert.equal(hash(await NodeFSP.readFile(source)), sourceHash);
  await finishProof({
    executable,
    fixture,
    proofPath,
    checks,
    sourceHash,
    oracles: [sessionOracle.source, linkOracle.source, deviceOracle.source],
    sourcePaths: [
      "apps/server/src/persistence/AuthSessions.ts",
      "apps/server/src/persistence/AuthPairingLinks.ts",
      "apps/server/src/persistence/NativeAuthSessions.ts",
      "apps/server/src/persistence/NativeAuthPairingLinks.ts",
      "apps/server/src/persistence/AuthConnectDevices.ts",
      "apps/server/src/persistence/NativeAuthConnectDevices.ts",
      "apps/server/src/persistence/redb_auth_rows.rs",
      "tools/check-redb-auth.mjs",
      "tools/redb-domain-fixture.mjs",
    ],
  });
} finally {
  await fixture.close();
}
