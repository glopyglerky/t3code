import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import {
  MaintenanceRows,
  type MaintenanceRead,
  type MaintenanceWrite,
  toRowsError,
} from "./MaintenanceRows.ts";

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const read = (query: MaintenanceRead) => {
    switch (query.kind) {
      case "metadata":
        return sql`SELECT projection_name,last_sequence,updated_at FROM orchestration_v2_projection_metadata WHERE projection_name IN ('thread-projections','event-log-prune-mark','event-log-pruned-through')`;
      case "bounds":
        return sql`SELECT COALESCE((SELECT MAX(sequence) FROM orchestration_events),0) AS sequence,COALESCE((SELECT MAX(rowid) FROM orchestration_command_receipts),0) AS receipt_row_id`;
      case "prompt_references":
        return sql`SELECT json_extract(payload_json,'$.promptSequence') AS prompt_sequence FROM orchestration_events WHERE sequence>${query.after_sequence} AND sequence<=${query.through_sequence} AND event_type IN ('subagent.updated','turn-item.updated') AND json_type(payload_json,'$.promptSequence')='integer'`;
      case "head_references":
        return sql`SELECT json_extract(event.payload_json,'$.promptSequence') AS prompt_sequence FROM (SELECT MAX(sequence) AS sequence FROM orchestration_events GROUP BY aggregate_kind,stream_id) AS heads JOIN orchestration_events AS event ON event.sequence=heads.sequence WHERE heads.sequence<=${query.through_sequence} AND event.event_type IN ('subagent.updated','turn-item.updated') AND json_type(event.payload_json,'$.promptSequence')='integer'`;
      case "candidates":
        return sql`SELECT event.sequence,CASE WHEN event.application_event_version=1 THEN 0 WHEN event.event_type='thread.created' THEN 0 WHEN event.event_type='run.updated' AND json_extract(event.payload_json,'$.status')='running' THEN 0 WHEN event.sequence=(SELECT MAX(newer.sequence) FROM orchestration_events AS newer WHERE newer.aggregate_kind=event.aggregate_kind AND newer.stream_id=event.stream_id) THEN 0 ELSE 1 END AS prunable FROM orchestration_events AS event WHERE event.sequence>${query.after_sequence} AND event.sequence<=${query.through_sequence} ORDER BY event.sequence ASC LIMIT ${query.limit}`;
      case "appended":
        return sql`SELECT sequence,CASE WHEN event_type IN ('subagent.updated','turn-item.updated') AND json_type(payload_json,'$.promptSequence')='integer' THEN json_extract(payload_json,'$.promptSequence') END AS prompt_sequence FROM orchestration_events WHERE sequence>${query.after_sequence}`;
      case "legacy_events":
        return sql`SELECT event.sequence,event.aggregate_kind='thread' AND EXISTS (SELECT 1 FROM orchestration_v2_legacy_imports AS legacy_import WHERE legacy_import.thread_id=event.stream_id AND legacy_import.transcript_imported_at IS NOT NULL) AS imported FROM orchestration_events AS event INDEXED BY idx_orchestration_events_application_sequence WHERE event.application_event_version=1 AND event.sequence>${query.after_sequence} AND event.sequence<=${query.through_sequence} ORDER BY event.sequence ASC LIMIT ${query.limit}`;
      case "legacy_receipts":
        return sql`SELECT receipt.rowid AS row_id,receipt.command_id,receipt.command_type='legacy' AND receipt.aggregate_kind='thread' AND EXISTS (SELECT 1 FROM orchestration_v2_legacy_imports AS legacy_import WHERE legacy_import.thread_id=receipt.aggregate_id AND legacy_import.transcript_imported_at IS NOT NULL) AS imported FROM orchestration_command_receipts AS receipt WHERE receipt.rowid>${query.after_row_id} AND receipt.rowid<=${query.through_row_id} ORDER BY receipt.rowid ASC LIMIT ${query.limit}`;
      case "reclaimable":
        return Effect.gen(function* () {
          const free = yield* sql<{ readonly freelist_count: number }>`PRAGMA freelist_count`;
          const page = yield* sql<{ readonly page_size: number }>`PRAGMA page_size`;
          return [
            { reclaimable_bytes: (free[0]?.freelist_count ?? 0) * (page[0]?.page_size ?? 0) },
          ];
        });
    }
  };
  const write = (mutation: MaintenanceWrite) => {
    switch (mutation.kind) {
      case "metadata":
        return sql`INSERT INTO orchestration_v2_projection_metadata (projection_name,schema_version,last_sequence,updated_at) VALUES (${mutation.name},${mutation.schema_version},${mutation.sequence},${mutation.updated_at}) ON CONFLICT(projection_name) DO UPDATE SET last_sequence=excluded.last_sequence,updated_at=excluded.updated_at`;
      case "delete_events":
        return sql`DELETE FROM orchestration_events WHERE sequence IN ${sql.in(mutation.sequences)}`;
      case "delete_receipts":
        return sql`DELETE FROM orchestration_command_receipts WHERE command_id IN ${sql.in(mutation.command_ids)}`;
    }
  };
  return MaintenanceRows.of({
    read: Effect.fn("MaintenanceRows.read")(
      <A>(query: MaintenanceRead, response: Schema.Decoder<A>) =>
        read(query).pipe(
          NodeSqliteClient.readOnly,
          Effect.flatMap(Schema.decodeUnknownEffect(response)),
          Effect.mapError(toRowsError("read")),
        ),
    ),
    write: Effect.fn("MaintenanceRows.write")(
      <A>(mutation: MaintenanceWrite, response: Schema.Decoder<A>) =>
        write(mutation).pipe(
          Effect.flatMap(Schema.decodeUnknownEffect(response)),
          Effect.mapError(toRowsError("write")),
        ),
    ),
  });
});
export const layer = Layer.effect(MaintenanceRows, make);
