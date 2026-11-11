#!/usr/bin/env node
import * as Assert from "node:assert/strict";
import * as FS from "node:fs/promises";
import * as Path from "node:path";
import * as Sqlite from "node:sqlite";
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
Assert.ok(executable && Path.isAbsolute(executable), "Pass an absolute native database executable");
await FS.access(executable, FS.constants.X_OK);
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
const fixture = await createFixture();
const checks = [];
const expectFailure = (effect) =>
  Effect.exit(effect).pipe(
    Effect.tap((exit) => Effect.sync(() => Assert.ok(Exit.isFailure(exit)))),
  );
const session = (id, fields = {}) =>
  Schema.decodeUnknownSync(Sessions.CreateAuthSessionInput)({
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
  Schema.decodeUnknownSync(Links.CreateAuthPairingLinkInput)({
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
  const construct = (transaction) =>
    Effect.gen(function* () {
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
      return yield* use({ sessions, links, transaction });
    });
  return kind === "omdb"
    ? withNative(executable, target, (client) => construct((body) => client.withTransaction(body)))
    : withSqlite(filename, (sql) => construct((body) => sql.withTransaction(body)));
}

function exercise({ sessions, links, transaction }) {
  return Effect.gen(function* () {
    const result = {};
    result.initialSessions = yield* sessions.listActive({ now });
    Assert.equal(result.initialSessions.length, 130);
    result.initialLinks = yield* links.listActive({ now });
    Assert.equal(result.initialLinks.length, 130);
    Assert.ok(Option.isNone(yield* sessions.getById({ sessionId: "missing" })));
    Assert.ok(Option.isNone(yield* links.getByCredential({ credential: "missing" })));
    result.connectedExpired = yield* sessions.listActive({
      now,
      connectedSessionIds: ["imported:expired", "imported:expired", "imported:revoked", "missing"],
    });
    Assert.equal(result.connectedExpired.length, 131);

    yield* sessions.create(session("written"));
    yield* expectFailure(sessions.create(session("written")));
    yield* sessions.createIfAbsent(session("written", { subject: "must-not-replace" }));
    result.written = yield* sessions.getById({ sessionId: "written" });
    Assert.equal(result.written.value.subject, "owner");
    yield* sessions.setLastConnectedAt({ sessionId: "written", lastConnectedAt: now });
    result.connected = yield* sessions.getById({ sessionId: "written" });
    Assert.equal(DateTime.formatIso(result.connected.value.lastConnectedAt), at);
    Assert.equal(yield* sessions.revoke({ sessionId: "written", revokedAt: now }), true);
    Assert.equal(yield* sessions.revoke({ sessionId: "written", revokedAt: now }), false);
    yield* sessions.setLastConnectedAt({
      sessionId: "written",
      lastConnectedAt: DateTime.makeUnsafe(future),
    });
    result.revoked = yield* sessions.getById({ sessionId: "written" });
    Assert.equal(DateTime.formatIso(result.revoked.value.lastConnectedAt), at);

    yield* sessions.create(session("replace:a", { subject: "replace" }));
    yield* sessions.create(session("replace:b", { subject: "replace" }));
    yield* expectFailure(
      sessions.createReplacingActive({
        session: session("replace:b", { subject: "replace" }),
        revokedAt: now,
      }),
    );
    Assert.equal(
      Option.getOrThrow(yield* sessions.getById({ sessionId: "replace:a" })).revokedAt,
      null,
    );
    result.replacement = [
      ...(yield* sessions.createReplacingActive({
        session: session("replace:c", { subject: "replace" }),
        revokedAt: now,
      })),
    ].sort();
    Assert.deepEqual(result.replacement, ["replace:a", "replace:b"]);
    yield* transaction(
      Effect.gen(function* () {
        yield* expectFailure(
          sessions.createReplacingActive({
            session: session("replace:c", { subject: "replace" }),
            revokedAt: now,
          }),
        );
        Assert.equal(
          Option.getOrThrow(yield* sessions.getById({ sessionId: "replace:c" })).revokedAt,
          null,
        );
        yield* sessions.create(
          session("after-rejected-replacement", { subject: "outer-survives" }),
        );
      }),
    );
    yield* sessions.create(session("replace:other", { subject: "other" }));
    result.targeted = yield* sessions.createReplacingActive({
      session: session("replace:target", { subject: "new-subject" }),
      replaceSessionId: "replace:other",
      revokedAt: now,
    });
    Assert.deepEqual(result.targeted, ["replace:other"]);

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
      Assert.ok(Option.isNone(yield* links.consumeAvailable(consume("written", rejected))));
    result.beforeConsume = yield* links.getByCredential({ credential: "code:written" });
    Assert.equal(result.beforeConsume.value.consumedAt, null);
    result.consumed = yield* links.consumeAvailable(consume("written"));
    Assert.ok(Option.isSome(result.consumed));
    Assert.ok(Option.isNone(yield* links.consumeAvailable(consume("written"))));
    Assert.equal(yield* links.revoke({ id: "written", revokedAt: now }), false);
    yield* links.create(link("revoked", { proofKeyThumbprint: null }));
    Assert.equal(yield* links.revoke({ id: "revoked", revokedAt: now }), true);
    Assert.equal(yield* links.revoke({ id: "revoked", revokedAt: now }), false);
    Assert.ok(Option.isNone(yield* links.consumeAvailable(consume("revoked"))));
    yield* links.create(link("approval", { subject: "MCP-APPROVAL:read" }));
    Assert.ok(Option.isNone(yield* links.consumeAvailable(consume("approval"))));
    Assert.ok(
      Option.isNone(
        yield* links.consumeAvailable(
          consume("approval", { mcpApprovalSubject: "mcp-approval:read" }),
        ),
      ),
    );
    result.approval = yield* links.consumeAvailable(
      consume("approval", { mcpApprovalSubject: "MCP-APPROVAL:read" }),
    );
    Assert.ok(Option.isSome(result.approval));

    yield* links.create(link("transaction"));
    yield* expectFailure(
      transaction(
        Effect.gen(function* () {
          Assert.ok(Option.isSome(yield* links.consumeAvailable(consume("transaction"))));
          yield* sessions.create(session("transaction"));
          return yield* Effect.fail("roll back pairing and session together");
        }),
      ),
    );
    Assert.ok(Option.isNone(yield* sessions.getById({ sessionId: "transaction" })));
    Assert.equal(
      Option.getOrThrow(yield* links.getByCredential({ credential: "code:transaction" }))
        .consumedAt,
      null,
    );
    yield* transaction(
      Effect.gen(function* () {
        Assert.ok(Option.isSome(yield* links.consumeAvailable(consume("transaction"))));
        yield* sessions.create(session("transaction"));
        yield* expectFailure(
          transaction(
            Effect.gen(function* () {
              yield* sessions.revoke({ sessionId: "transaction", revokedAt: now });
              return yield* Effect.fail("savepoint rollback");
            }),
          ),
        );
        Assert.equal(
          Option.getOrThrow(yield* sessions.getById({ sessionId: "transaction" })).revokedAt,
          null,
        );
      }),
    );
    result.revokeOthers = [
      ...(yield* sessions.revokeAllExcept({ currentSessionId: "transaction", revokedAt: now })),
    ].sort();
    result.finalSessions = yield* sessions.listActive({ now });
    Assert.deepEqual(
      result.finalSessions.map((row) => row.sessionId),
      ["transaction"],
    );
    result.finalLinks = yield* links.listActive({ now });
    return normalized(result);
  });
}

try {
  const source = Path.join(fixture.root, "source.sqlite");
  const sqlitePath = Path.join(fixture.root, "sqlite.sqlite");
  const upstreamPath = Path.join(fixture.root, "upstream.sqlite");
  const exported = Path.join(fixture.root, "source.ndjson");
  await createMigratedSqlite(source);
  const database = new Sqlite.DatabaseSync(source);
  try {
    const sessionInsert = database.prepare(`INSERT INTO auth_sessions
      (session_id,subject,scopes,method,client_device_type,issued_at,expires_at,revoked_at,client_surface,client_app_version)
      VALUES (?, 'imported', '["orchestration:read"]', 'bearer-access-token', 'unknown', ?, ?, ?, 'web', 'fixture-version')`);
    const linkInsert = database.prepare(`INSERT INTO auth_pairing_links
      (id,credential,method,scopes,subject,created_at,expires_at)
      VALUES (?, ?, 'one-time-token', '["orchestration:read"]', 'imported', ?, ?)`);
    for (let i = 0; i < 130; i++) {
      const id = `imported:${String(i).padStart(4, "0")}`;
      sessionInsert.run(id, at, future, null);
      linkInsert.run(id, `code:${id}`, at, future);
    }
    sessionInsert.run("imported:expired", at, "2020-01-01T00:00:00.000Z", null);
    sessionInsert.run("imported:revoked", at, future, at);
  } finally {
    database.close();
  }
  const sourceHash = hash(await FS.readFile(source));
  await FS.copyFile(source, sqlitePath);
  await FS.copyFile(source, upstreamPath);
  exportSqliteState({ source, destination: exported });
  nativeRequest(executable, fixture, "initialize", {
    operation: "import_sqlite_application",
    path: exported,
  });
  const expected = await withServices("upstream", upstreamPath, exercise);
  Assert.deepEqual(await withServices("sqlite", sqlitePath, exercise), expected);
  Assert.deepEqual(await withServices("omdb", source, exercise), expected);
  checks.push(
    "All 8 auth-session and 5 pairing-link repository members match immutable upstream SQLite and retained SQLite",
    "Imported active indexes span multiple pages and retain raw rows, legacy scopes and extra client-connection columns",
    "Expiry, connected expired sessions, revocation, create-if-absent and targeted/subject replacements preserve upstream behavior",
    "Wrong proof keys, requested/required scope failures and reserved approval subjects cannot consume pairing codes",
    "Shared and nested application transactions commit or roll back pairing consumption and session issuance together",
  );
  await withServices("omdb", source, ({ sessions, links }) =>
    Effect.gen(function* () {
      Assert.deepEqual(normalized(yield* sessions.listActive({ now })), expected.finalSessions);
      Assert.deepEqual(normalized(yield* links.listActive({ now })), expected.finalLinks);
    }),
  );
  checks.push(
    "A reopened native helper retains committed authentication state without re-preparing live indexes",
  );
  Assert.equal(hash(await FS.readFile(source)), sourceHash);
  await finishProof({
    executable,
    fixture,
    proofPath,
    checks,
    sourceHash,
    oracles: [sessionOracle.source, linkOracle.source],
    sourcePaths: [
      "apps/server/src/persistence/AuthSessions.ts",
      "apps/server/src/persistence/AuthPairingLinks.ts",
      "apps/server/src/persistence/NativeAuthSessions.ts",
      "apps/server/src/persistence/NativeAuthPairingLinks.ts",
      "apps/server/src/persistence/redb_auth_rows.rs",
      "tools/check-redb-auth.mjs",
      "tools/redb-domain-fixture.mjs",
    ],
  });
} finally {
  await fixture.close();
}
