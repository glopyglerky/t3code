import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

export class EventRowsError extends Schema.TaggedError<EventRowsError>()("EventRowsError", {
  operation: Schema.String,
  cause: Schema.Defect(),
}) {}

const isEventRowsError = Schema.is(EventRowsError);
export const toEventRowsError = (operation: string) => (cause: unknown): EventRowsError =>
  isEventRowsError(cause) ? cause : new EventRowsError({ operation, cause });

/** Encoded application rows shared by the SQLite and native event repositories. */
export interface NewApplicationEventRow {
  readonly event_id: string;
  readonly aggregate_kind: "project" | "thread";
  readonly stream_id: string;
  readonly event_type: string;
  readonly occurred_at: string;
  readonly command_id: string | null;
  readonly causation_event_id: string | null;
  readonly correlation_id: string | null;
  readonly actor_kind: "client" | "server" | "provider";
  readonly payload_json: string;
  readonly metadata_json: string;
  readonly application_event_version: 2;
}

export interface ApplicationEventRow {
  readonly sequence: number;
  readonly event_id: string;
  readonly command_id: string | null;
  readonly aggregate_kind: "project" | "thread";
  readonly stream_id: string;
  readonly event_type: string;
  readonly occurred_at: string;
  readonly payload_json: string;
  readonly metadata_json: string;
  readonly application_event_version: number;
  readonly causation_event_id: string | null;
  readonly correlation_id: string | null;
}

export interface ReadEventRows {
  readonly after_sequence: number;
  readonly through_sequence?: number;
  readonly thread_id?: string;
  readonly command_id?: string;
  readonly event_type?: string;
  readonly only_agent_events?: boolean;
  readonly limit: number;
}

export type EventReplayStatsInput =
  | {
      readonly kind: "agent";
      readonly after_sequence: number;
      readonly through_sequence: number;
      readonly thread_id: string;
      readonly max_events: number;
    }
  | {
      readonly kind: "application";
      readonly after_sequence: number;
      readonly through_sequence: number;
    };

export interface EventReplayStats {
  readonly event_count: number;
  readonly raw_payload_bytes: number;
  readonly has_create_event: boolean;
}

/** Each operation joins the caller's transaction; publication stays in the event service. */
export interface EventRowsShape {
  readonly append: (input: {
    readonly row: NewApplicationEventRow;
    readonly prompt_hint?: number;
  }) => Effect.Effect<
    { readonly sequence: number; readonly stores_full_prompt: boolean },
    EventRowsError
  >;
  readonly read: (
    input: ReadEventRows,
  ) => Effect.Effect<ReadonlyArray<ApplicationEventRow>, EventRowsError>;
  readonly stats: (input: EventReplayStatsInput) => Effect.Effect<EventReplayStats, EventRowsError>;
  readonly latest: (input: {
    readonly kind: "agent" | "application";
    readonly thread_id?: string;
  }) => Effect.Effect<number, EventRowsError>;
  readonly prunedThrough: Effect.Effect<number, EventRowsError>;
}

export class EventRows extends Context.Service<EventRows, EventRowsShape>()(
  "t3/persistence/EventRows",
) {}
