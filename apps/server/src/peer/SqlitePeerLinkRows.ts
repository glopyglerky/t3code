import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";

import { PeerLinkRow, PeerLinkRows, toPeerLinkRowsError } from "./PeerLinkRows.ts";

export const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  return PeerLinkRows.of({
    list: sql<PeerLinkRow>`SELECT * FROM peer_environment_links ORDER BY label, environment_id`.pipe(
      NodeSqliteClient.readOnly,
      Effect.mapError(toPeerLinkRowsError("list")),
    ),
    get: Effect.fn("PeerLinkRows.get")((environmentId) =>
      sql<PeerLinkRow>`
      SELECT * FROM peer_environment_links WHERE environment_id = ${environmentId}
    `.pipe(
        NodeSqliteClient.readOnly,
        Effect.map((rows) => rows[0] ?? null),
        Effect.mapError(toPeerLinkRowsError("get")),
      ),
    ),
    upsert: Effect.fn("PeerLinkRows.upsert")((row) =>
      sql`
      INSERT INTO peer_environment_links (
        environment_id, secret_name, label, urls_json, access, linked_at, expires_at,
        last_reached_at, last_error, fenced
      ) VALUES (
        ${row.environment_id}, ${row.secret_name}, ${row.label}, ${row.urls_json}, ${row.access},
        ${row.linked_at}, ${row.expires_at}, ${row.last_reached_at}, ${row.last_error}, ${row.fenced}
      ) ON CONFLICT (environment_id) DO UPDATE SET
        secret_name = excluded.secret_name, fenced = excluded.fenced, label = excluded.label,
        urls_json = excluded.urls_json, access = excluded.access, linked_at = excluded.linked_at,
        expires_at = excluded.expires_at, last_reached_at = excluded.last_reached_at,
        last_error = excluded.last_error
    `.pipe(Effect.asVoid, Effect.mapError(toPeerLinkRowsError("upsert"))),
    ),
    relabel: Effect.fn("PeerLinkRows.relabel")((environmentId, label) =>
      sql`
      UPDATE peer_environment_links SET label = ${label} WHERE environment_id = ${environmentId}
    `.pipe(Effect.asVoid, Effect.mapError(toPeerLinkRowsError("relabel"))),
    ),
    remove: Effect.fn("PeerLinkRows.remove")((environmentId) =>
      sql<{ readonly secret_name: string }>`
      DELETE FROM peer_environment_links WHERE environment_id = ${environmentId} RETURNING secret_name
    `.pipe(
        Effect.map((rows) => rows[0]?.secret_name ?? null),
        Effect.mapError(toPeerLinkRowsError("remove")),
      ),
    ),
    recordOutcome: Effect.fn("PeerLinkRows.recordOutcome")((environmentId, error, reachedAt) =>
      (error === null
        ? sql`UPDATE peer_environment_links SET last_reached_at = ${reachedAt}, last_error = NULL WHERE environment_id = ${environmentId}`
        : sql`UPDATE peer_environment_links SET last_error = ${error} WHERE environment_id = ${environmentId}`
      ).pipe(Effect.asVoid, Effect.mapError(toPeerLinkRowsError("recordOutcome"))),
    ),
  });
});

export const layer = Layer.effect(PeerLinkRows, make);
