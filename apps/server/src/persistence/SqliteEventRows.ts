import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";

import {
  EventRows,
  EventRowsError,
  toEventRowsError,
  type ApplicationEventRow,
  type EventRowsShape,
} from "./EventRows.ts";

export const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const restoredPayloadJson = sql`CASE
    WHEN orchestration_events.event_type IN ('subagent.updated', 'turn-item.updated')
      AND json_type(orchestration_events.payload_json, '$.promptSequence') = 'integer'
    THEN json_set(
      json_remove(orchestration_events.payload_json, '$.promptSequence'),
      '$.prompt',
      (SELECT json_extract(anchor.payload_json, '$.prompt')
       FROM orchestration_events AS anchor
       WHERE anchor.sequence = json_extract(orchestration_events.payload_json, '$.promptSequence'))
    )
    ELSE orchestration_events.payload_json
  END`;

  return EventRows.of({
    append: Effect.fnUntraced(function* ({ row, prompt_hint }) {
      const storedPayload =
        prompt_hint === undefined
          ? sql`${row.payload_json}`
          : sql`CASE WHEN EXISTS (
              SELECT 1 FROM orchestration_events
              WHERE sequence = ${prompt_hint}
                AND json_type(payload_json, '$.promptSequence') IS NULL
                AND json_extract(payload_json, '$.prompt') = json_extract(${row.payload_json}, '$.prompt')
            ) THEN json_set(${row.payload_json}, '$.prompt', '', '$.promptSequence', CAST(${prompt_hint} AS INTEGER))
            ELSE ${row.payload_json} END`;
      const rows = yield* sql<{
        readonly sequence: number;
        readonly stores_full_prompt: number;
      }>`
        INSERT INTO orchestration_events (
          event_id, aggregate_kind, stream_id, stream_version, event_type, occurred_at,
          command_id, causation_event_id, correlation_id, actor_kind,
          payload_json, metadata_json, application_event_version
        )
        VALUES (
          ${row.event_id}, ${row.aggregate_kind}, ${row.stream_id},
          COALESCE(
            (SELECT MAX(stream_version) + 1 FROM orchestration_events
             WHERE aggregate_kind = ${row.aggregate_kind} AND stream_id = ${row.stream_id}),
            0
          ),
          ${row.event_type}, ${row.occurred_at}, ${row.command_id},
          ${row.causation_event_id}, ${row.correlation_id}, ${row.actor_kind},
          ${storedPayload}, ${row.metadata_json}, ${row.application_event_version}
        )
        RETURNING sequence, json_type(payload_json, '$.promptSequence') IS NULL AS stores_full_prompt
      `;
      const inserted = rows[0];
      if (inserted === undefined) {
        return yield* new EventRowsError({
          operation: "append",
          cause: "Event insert returned no row.",
        });
      }
      return { sequence: inserted.sequence, stores_full_prompt: inserted.stores_full_prompt === 1 };
    }, Effect.mapError(toEventRowsError("append"))),
    read: (input) =>
      sql<ApplicationEventRow>`
        SELECT
          sequence, event_id, command_id, aggregate_kind, stream_id, event_type, occurred_at,
          ${restoredPayloadJson} AS payload_json, metadata_json, application_event_version,
          causation_event_id, correlation_id
        FROM orchestration_events
        ${
          input.command_id !== undefined
            ? sql`INDEXED BY idx_orch_events_command_id`
            : input.only_agent_events === true
              ? sql``
              : sql`INDEXED BY idx_orchestration_events_application_high_water`
        }
        WHERE sequence > ${input.after_sequence}
          AND sequence <= ${input.through_sequence ?? Number.MAX_SAFE_INTEGER}
          AND (
            ${
              input.only_agent_events === true
                ? sql`application_event_version = 2 AND aggregate_kind = 'thread'`
                : sql`aggregate_kind = 'project'
                      OR (application_event_version = 2 AND aggregate_kind = 'thread')`
            }
          )
          AND ${sql.and([
            ...(input.thread_id === undefined ? [] : [sql`stream_id = ${input.thread_id}`]),
            ...(input.command_id === undefined ? [] : [sql`command_id = ${input.command_id}`]),
            ...(input.event_type === undefined ? [] : [sql`event_type = ${input.event_type}`]),
          ])}
        ORDER BY sequence ASC
        LIMIT ${input.limit}
      `.pipe(NodeSqliteClient.readOnly, Effect.mapError(toEventRowsError("read"))),
    stats: (input) =>
      (input.kind === "agent"
        ? sql<{
            readonly event_count: number;
            readonly raw_payload_bytes: number;
            readonly has_create_event: number;
          }>`
            SELECT COUNT(*) AS event_count,
              COALESCE(SUM(octet_length(payload_json)), 0) AS raw_payload_bytes,
              COALESCE(MAX(event_type = 'thread.created'), 0) AS has_create_event
            FROM (
              SELECT ${restoredPayloadJson} AS payload_json, event_type
              FROM orchestration_events
              WHERE aggregate_kind = 'thread' AND stream_id = ${input.thread_id}
                AND application_event_version = 2
                AND sequence > ${input.after_sequence} AND sequence <= ${input.through_sequence}
              ORDER BY sequence ASC
              LIMIT ${Math.max(0, Math.floor(input.max_events)) + 1}
            )
          `
        : sql<{
            readonly event_count: number;
            readonly raw_payload_bytes: number;
            readonly has_create_event: number;
          }>`
            SELECT COUNT(*) AS event_count,
              COALESCE(SUM(octet_length(${restoredPayloadJson})), 0) AS raw_payload_bytes,
              0 AS has_create_event
            FROM orchestration_events INDEXED BY idx_orchestration_events_application_high_water
            WHERE sequence > ${input.after_sequence} AND sequence <= ${input.through_sequence}
              AND (aggregate_kind = 'project'
                OR (application_event_version = 2 AND aggregate_kind = 'thread'))
          `).pipe(
        NodeSqliteClient.readOnly,
        Effect.map((rows) => ({
          event_count: rows[0]?.event_count ?? 0,
          raw_payload_bytes: rows[0]?.raw_payload_bytes ?? 0,
          has_create_event: (rows[0]?.has_create_event ?? 0) !== 0,
        })),
        Effect.mapError(toEventRowsError("stats")),
      ),
    latest: (input) =>
      (input.kind === "agent"
        ? sql<{ readonly sequence: number | null }>`
            SELECT MAX(sequence) AS sequence FROM orchestration_events
            ${
              input.thread_id === undefined
                ? sql`INDEXED BY idx_orchestration_events_application_sequence`
                : sql`INDEXED BY idx_orchestration_events_agent_stream_sequence`
            }
            WHERE application_event_version = 2 AND aggregate_kind = 'thread'
              ${input.thread_id === undefined ? sql`` : sql`AND stream_id = ${input.thread_id}`}
          `
        : sql<{ readonly sequence: number | null }>`
            SELECT MAX(sequence) AS sequence
            FROM orchestration_events INDEXED BY idx_orchestration_events_application_high_water
            WHERE aggregate_kind = 'project'
              OR (application_event_version = 2 AND aggregate_kind = 'thread')
          `).pipe(
        NodeSqliteClient.readOnly,
        Effect.map((rows) => rows[0]?.sequence ?? 0),
        Effect.mapError(toEventRowsError("latest")),
      ),
    prunedThrough: sql<{ readonly last_sequence: number }>`
      SELECT last_sequence FROM orchestration_v2_projection_metadata
      WHERE projection_name = 'event-log-pruned-through'
    `.pipe(
      NodeSqliteClient.readOnly,
      Effect.map((rows) => rows[0]?.last_sequence ?? 0),
      Effect.mapError(toEventRowsError("pruned-through")),
    ),
  } satisfies EventRowsShape);
});

export const layer = Layer.effect(EventRows, make);
