import {
  ApplicationEventMetadata,
  ApplicationProjectEvent,
  type ApplicationStoredEvent,
  CommandId,
  EventId,
  IsoDateTime,
  isKnownOrchestrationV2EventType,
  OrchestrationV2DomainEventJson,
  OrchestrationV2StoredEvent,
  ProjectId,
  StoredProjectIcon,
  ThreadId,
  type OrchestrationV2DomainEvent,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as PubSub from "effect/PubSub";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";

import { replayAndBufferProjectedLiveEvents } from "../orchestration-v2/LiveStreamBudget.ts";
import { EventRows, type ApplicationEventRow, type NewApplicationEventRow } from "./EventRows.ts";
import * as SqliteEventRows from "./SqliteEventRows.ts";

import {
  toPersistenceDecodeError,
  toPersistenceSqlError,
  type OrchestrationEventStoreError,
} from "./Errors.ts";

/** Projection metadata rows: events at or below the pruned-through sequence may be gone. */
export const EVENT_LOG_PRUNED_THROUGH = "event-log-pruned-through";
export const EVENT_LOG_PRUNE_MARK = "event-log-prune-mark";

export type UnsequencedProjectEvent = ApplicationProjectEvent extends infer Event
  ? Event extends ApplicationProjectEvent
    ? Omit<Event, "sequence">
    : never
  : never;

export class OrchestrationEventStore extends Context.Service<
  OrchestrationEventStore,
  {
    readonly appendProjectEvent: (
      event: UnsequencedProjectEvent,
    ) => Effect.Effect<ApplicationProjectEvent, OrchestrationEventStoreError>;

    readonly appendAgentEvents: (input: {
      readonly commandId?: CommandId;
      readonly events: ReadonlyArray<OrchestrationV2DomainEvent>;
    }) => Effect.Effect<ReadonlyArray<OrchestrationV2StoredEvent>, OrchestrationEventStoreError>;

    /** Unknown event types fail unless skipUnknownEventTypes logs and drops them; only client replay may opt in. */
    readonly readAgentEvents: (input?: {
      readonly afterSequence?: number;
      readonly throughSequence?: number;
      readonly threadId?: ThreadId;
      readonly commandId?: CommandId;
      readonly eventType?: OrchestrationV2DomainEvent["type"];
      readonly limit?: number;
      readonly skipUnknownEventTypes?: boolean;
    }) => Stream.Stream<OrchestrationV2StoredEvent, OrchestrationEventStoreError>;

    readonly getAgentReplayStats: (input: {
      readonly threadId: ThreadId;
      readonly afterSequence: number;
      readonly throughSequence: number;
      readonly maxEvents: number;
    }) => Effect.Effect<
      {
        readonly eventCount: number;
        readonly rawPayloadBytes: number;
        readonly hasCreateEvent: boolean;
      },
      OrchestrationEventStoreError
    >;

    readonly getReplayStats: (input: {
      readonly afterSequence: number;
      readonly throughSequence: number;
    }) => Effect.Effect<
      {
        readonly eventCount: number;
        readonly rawPayloadBytes: number;
      },
      OrchestrationEventStoreError
    >;

    readonly latestAgentSequence: (
      threadId?: ThreadId,
    ) => Effect.Effect<number, OrchestrationEventStoreError>;

    readonly latestApplicationSequence: Effect.Effect<number, OrchestrationEventStoreError>;

    /** Replay from a cursor below this misses pruned events; resync from a snapshot instead. */
    readonly prunedThroughSequence: Effect.Effect<number, OrchestrationEventStoreError>;

    /** Unknown event types fail unless skipUnknownEventTypes logs and drops them; only client replay may opt in. */
    readonly readApplicationEvents: (input: {
      readonly afterSequence: number;
      readonly throughSequence: number;
      readonly skipUnknownEventTypes?: boolean;
    }) => Stream.Stream<ApplicationStoredEvent, OrchestrationEventStoreError>;

    /** Publish only after the surrounding event/projection transaction commits. */
    readonly publishCommitted: (
      events: ReadonlyArray<ApplicationStoredEvent>,
    ) => Effect.Effect<void>;

    readonly streamApplicationEvents: (input?: {
      readonly afterSequence?: number;
    }) => Stream.Stream<ApplicationStoredEvent, OrchestrationEventStoreError>;
    readonly streamProjectedApplicationEvents: <A extends { readonly sequence: number }>(input: {
      readonly afterSequence?: number;
      readonly project: (event: ApplicationStoredEvent) => A;
      readonly filter?: (event: ApplicationStoredEvent) => boolean;
    }) => Stream.Stream<A, OrchestrationEventStoreError>;
  }
>()("t3/persistence/OrchestrationEventStore") {}

const encodeProjectIcon = Schema.encodeSync(StoredProjectIcon);
const decodeProjectEvent = Schema.decodeUnknownEffect(ApplicationProjectEvent);
const UnknownFromJsonString = Schema.fromJsonString(Schema.Unknown);
const EventMetadataFromJsonString = Schema.fromJsonString(ApplicationEventMetadata);
const ProjectEventType = Schema.Literals([
  "project.created",
  "project.meta-updated",
  "project.deleted",
]);
const ActorKind = Schema.Literals(["client", "server", "provider"]);

const AppendProjectEventRequestSchema = Schema.Struct({
  eventId: EventId,
  streamId: ProjectId,
  type: ProjectEventType,
  causationEventId: Schema.NullOr(EventId),
  correlationId: Schema.NullOr(CommandId),
  actorKind: ActorKind,
  occurredAt: IsoDateTime,
  commandId: Schema.NullOr(CommandId),
  payloadJson: UnknownFromJsonString,
  metadataJson: EventMetadataFromJsonString,
});

const encodeProjectRequest = Schema.encodeEffect(AppendProjectEventRequestSchema);

const READ_PAGE_SIZE = 500;

const decodeV2EventJson = Schema.decodeUnknownEffect(OrchestrationV2DomainEventJson);
const encodeV2EventJson = Schema.encodeEffect(OrchestrationV2DomainEventJson);
const decodeV2StoredEvent = Schema.decodeUnknownEffect(OrchestrationV2StoredEvent);
const decodeJson = Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.Unknown));
const encodeJson = Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown));

function metadataForV2Event(event: OrchestrationV2DomainEvent): Record<string, unknown> {
  return {
    ...(event.runId === undefined ? {} : { runId: event.runId }),
    ...(event.nodeId === undefined ? {} : { nodeId: event.nodeId }),
    ...(event.driver === undefined ? {} : { driver: event.driver }),
    ...(event.providerInstanceId === undefined
      ? {}
      : { providerInstanceId: event.providerInstanceId }),
    ...(event.rawEventId === undefined ? {} : { rawEventId: event.rawEventId }),
  };
}

const rowToV2StoredEvent = Effect.fn("OrchestrationEventStore.rowToV2StoredEvent")(function* (
  row: ApplicationEventRow,
) {
  const payload = yield* decodeJson(row.payload_json);
  const metadata = yield* decodeJson(row.metadata_json);
  const values =
    typeof metadata === "object" && metadata !== null ? (metadata as Record<string, unknown>) : {};
  const event = yield* decodeV2EventJson({
    id: row.event_id,
    threadId: row.stream_id,
    type: row.event_type,
    occurredAt: row.occurred_at,
    payload,
    ...(values.runId === undefined ? {} : { runId: values.runId }),
    ...(values.nodeId === undefined ? {} : { nodeId: values.nodeId }),
    ...(values.driver === undefined ? {} : { driver: values.driver }),
    ...(values.providerInstanceId === undefined
      ? {}
      : { providerInstanceId: values.providerInstanceId }),
    ...(values.rawEventId === undefined ? {} : { rawEventId: values.rawEventId }),
  });
  return yield* decodeV2StoredEvent({
    sequence: row.sequence,
    commandId: row.command_id,
    event,
  });
});

const rowToProjectEvent = Effect.fn("OrchestrationEventStore.rowToProjectEvent")(function* (
  row: ApplicationEventRow,
) {
  return yield* decodeProjectEvent({
    sequence: row.sequence,
    eventId: row.event_id,
    type: row.event_type,
    aggregateKind: row.aggregate_kind,
    aggregateId: row.stream_id,
    occurredAt: row.occurred_at,
    commandId: row.command_id,
    causationEventId: row.causation_event_id,
    correlationId: row.correlation_id,
    payload: yield* decodeJson(row.payload_json),
    metadata: yield* decodeJson(row.metadata_json),
  });
});

function rowToApplicationStoredEvent(
  row: ApplicationEventRow,
): Effect.Effect<ApplicationStoredEvent, Schema.SchemaError> {
  return row.aggregate_kind === "project" ? rowToProjectEvent(row) : rowToV2StoredEvent(row);
}

const isKnownProjectEventType = Schema.is(ProjectEventType);

const keepReplayRow = (row: ApplicationEventRow, skipUnknownEventTypes?: boolean) =>
  skipUnknownEventTypes !== true ||
  (row.aggregate_kind === "project"
    ? isKnownProjectEventType(row.event_type)
    : isKnownOrchestrationV2EventType(row.event_type))
    ? Effect.succeed(true)
    : Effect.logWarning("Skipping an application event row with an unknown event type", {
        aggregateKind: row.aggregate_kind,
        sequence: row.sequence,
        eventTypeLength: row.event_type.length,
      }).pipe(Effect.as(false));

function inferActorKind(event: UnsequencedProjectEvent): typeof ActorKind.Type {
  if (event.commandId !== null && event.commandId.startsWith("provider:")) {
    return "provider";
  }
  if (event.commandId !== null && event.commandId.startsWith("server:")) {
    return "server";
  }
  if (
    event.metadata.providerTurnId !== undefined ||
    event.metadata.providerItemId !== undefined ||
    event.metadata.adapterKey !== undefined
  ) {
    return "provider";
  }
  if (event.commandId === null) {
    return "server";
  }
  return "client";
}

function toPersistenceSqlOrDecodeError(sqlOperation: string, decodeOperation: string) {
  return (cause: unknown): OrchestrationEventStoreError =>
    Schema.isSchemaError(cause)
      ? toPersistenceDecodeError(decodeOperation)(cause)
      : toPersistenceSqlError(sqlOperation)(cause);
}

const makeEventStore = Effect.gen(function* () {
  const rows = yield* EventRows;
  const committedEvents = yield* PubSub.unbounded<ApplicationStoredEvent>();
  const promptHints = new Map<string, number>();

  const appendProjectEvent: OrchestrationEventStore["Service"]["appendProjectEvent"] = Effect.fn(
    "OrchestrationEventStore.appendProjectEvent",
  )(
    function* (event) {
      const request = yield* encodeProjectRequest({
        eventId: event.eventId,
        streamId: event.aggregateId,
        type: event.type,
        causationEventId: event.causationEventId,
        correlationId: event.correlationId,
        actorKind: inferActorKind(event),
        occurredAt: event.occurredAt,
        commandId: event.commandId,
        payloadJson:
          event.type === "project.deleted" || !event.payload.projectIcon
            ? event.payload
            : { ...event.payload, projectIcon: encodeProjectIcon(event.payload.projectIcon) },
        metadataJson: event.metadata,
      });
      const row: NewApplicationEventRow = {
        event_id: request.eventId,
        aggregate_kind: "project",
        stream_id: request.streamId,
        event_type: request.type,
        occurred_at: request.occurredAt,
        command_id: request.commandId,
        causation_event_id: request.causationEventId,
        correlation_id: request.correlationId,
        actor_kind: request.actorKind,
        payload_json: request.payloadJson,
        metadata_json: request.metadataJson,
        application_event_version: 2,
      };
      const inserted = yield* rows.append({ row });
      return yield* rowToProjectEvent({ ...row, sequence: inserted.sequence });
    },
    Effect.mapError(
      toPersistenceSqlOrDecodeError(
        "OrchestrationEventStore.appendProjectEvent:insert",
        "OrchestrationEventStore.appendProjectEvent:decode",
      ),
    ),
  );

  const readApplicationRows = (input: {
    readonly afterSequence: number;
    readonly throughSequence?: number;
    readonly threadId?: ThreadId;
    readonly commandId?: CommandId;
    readonly eventType?: OrchestrationV2DomainEvent["type"];
    readonly onlyAgentEvents?: boolean;
    readonly limit: number;
  }) =>
    rows.read({
      after_sequence: input.afterSequence,
      ...(input.throughSequence === undefined ? {} : { through_sequence: input.throughSequence }),
      ...(input.threadId === undefined ? {} : { thread_id: input.threadId }),
      ...(input.commandId === undefined ? {} : { command_id: input.commandId }),
      ...(input.eventType === undefined ? {} : { event_type: input.eventType }),
      ...(input.onlyAgentEvents === undefined ? {} : { only_agent_events: input.onlyAgentEvents }),
      limit: input.limit,
    });

  const appendAgentEvents: OrchestrationEventStore["Service"]["appendAgentEvents"] = (input) =>
    Effect.forEach(
      input.events,
      (event) =>
        Effect.gen(function* () {
          const encoded = yield* encodeV2EventJson(event);
          const subagent =
            encoded.type === "subagent.updated"
              ? encoded.payload
              : encoded.type === "turn-item.updated" && encoded.payload.type === "subagent"
                ? encoded.payload
                : undefined;
          const promptKey = subagent === undefined ? undefined : `${event.type}:${subagent.id}`;
          const hint = promptKey === undefined ? undefined : promptHints.get(promptKey);
          const payloadJson = yield* encodeJson(encoded.payload);
          // The repository validates an anchor before storing a prompt reference.
          const inserted = yield* rows.append({
            row: {
              event_id: event.id,
              aggregate_kind: "thread",
              stream_id: event.threadId,
              event_type: event.type,
              occurred_at: encoded.occurredAt,
              command_id: input.commandId ?? null,
              causation_event_id: null,
              correlation_id: input.commandId ?? null,
              actor_kind: event.rawEventId === undefined ? "server" : "provider",
              payload_json: payloadJson,
              metadata_json: yield* encodeJson(metadataForV2Event(event)),
              application_event_version: 2,
            },
            ...(hint === undefined ? {} : { prompt_hint: hint }),
          });
          if (promptKey !== undefined) {
            promptHints.delete(promptKey);
            promptHints.set(
              promptKey,
              hint === undefined || inserted.stores_full_prompt ? inserted.sequence : hint,
            );
            if (promptHints.size > 4096) {
              const oldest = promptHints.keys().next().value;
              if (oldest !== undefined) promptHints.delete(oldest);
            }
          }
          return yield* decodeV2StoredEvent({
            sequence: inserted.sequence,
            commandId: input.commandId ?? null,
            event,
          });
        }),
      { concurrency: 1 },
    ).pipe(
      Effect.mapError(
        toPersistenceSqlOrDecodeError(
          "OrchestrationEventStore.appendAgentEvents:insert",
          "OrchestrationEventStore.appendAgentEvents:decode",
        ),
      ),
    );

  const readAgentEvents: OrchestrationEventStore["Service"]["readAgentEvents"] = (input) => {
    const totalLimit =
      input?.limit === undefined ? Number.MAX_SAFE_INTEGER : Math.max(0, Math.floor(input.limit));
    if (totalLimit === 0) {
      return Stream.empty;
    }
    return Stream.paginate(
      { cursor: input?.afterSequence ?? 0, remaining: totalLimit },
      ({ cursor, remaining }) => {
        const pageLimit = Math.min(remaining, READ_PAGE_SIZE);
        return readApplicationRows({
          afterSequence: cursor,
          ...(input?.throughSequence === undefined
            ? {}
            : { throughSequence: input.throughSequence }),
          ...(input?.threadId === undefined ? {} : { threadId: input.threadId }),
          ...(input?.commandId === undefined ? {} : { commandId: input.commandId }),
          ...(input?.eventType === undefined ? {} : { eventType: input.eventType }),
          onlyAgentEvents: true,
          limit: pageLimit,
        }).pipe(
          Effect.mapError(toPersistenceSqlError("OrchestrationEventStore.readAgentEvents:query")),
          Effect.map((rows) => {
            const last = rows.at(-1);
            const nextRemaining = remaining - rows.length;
            return [
              rows,
              last === undefined ||
              rows.length < pageLimit ||
              nextRemaining <= 0 ||
              (input?.throughSequence !== undefined && last.sequence >= input.throughSequence)
                ? Option.none()
                : Option.some({ cursor: last.sequence, remaining: nextRemaining }),
            ] as const;
          }),
        );
      },
    ).pipe(
      Stream.filterEffect((row) => keepReplayRow(row, input?.skipUnknownEventTypes)),
      Stream.mapEffect((row) =>
        rowToV2StoredEvent(row).pipe(
          Effect.mapError(
            toPersistenceDecodeError("OrchestrationEventStore.readAgentEvents:decode"),
          ),
        ),
      ),
    );
  };

  const getAgentReplayStats: OrchestrationEventStore["Service"]["getAgentReplayStats"] = (input) =>
    rows.stats({
      kind: "agent",
      after_sequence: input.afterSequence,
      through_sequence: input.throughSequence,
      thread_id: input.threadId,
      max_events: Math.max(0, Math.floor(input.maxEvents)),
    }).pipe(
      Effect.mapError(toPersistenceSqlError("OrchestrationEventStore.getAgentReplayStats:query")),
      Effect.map((stats) => ({
        eventCount: stats.event_count,
        rawPayloadBytes: stats.raw_payload_bytes,
        hasCreateEvent: stats.has_create_event,
      })),
    );

  const getReplayStats: OrchestrationEventStore["Service"]["getReplayStats"] = (input) =>
    rows.stats({
      kind: "application",
      after_sequence: input.afterSequence,
      through_sequence: input.throughSequence,
    }).pipe(
      Effect.mapError(toPersistenceSqlError("OrchestrationEventStore.getReplayStats:query")),
      Effect.map((stats) => ({
        eventCount: stats.event_count,
        rawPayloadBytes: stats.raw_payload_bytes,
      })),
    );

  const latestAgentSequence: OrchestrationEventStore["Service"]["latestAgentSequence"] = (threadId) =>
    rows.latest({
      kind: "agent",
      ...(threadId === undefined ? {} : { thread_id: threadId }),
    }).pipe(
      Effect.mapError(toPersistenceSqlError("OrchestrationEventStore.latestAgentSequence:query")),
    );

  const latestApplicationSequence = rows.latest({ kind: "application" }).pipe(
    Effect.mapError(
      toPersistenceSqlError("OrchestrationEventStore.latestApplicationSequence:query"),
    ),
  );

  const prunedThroughSequence = rows.prunedThrough.pipe(
    Effect.mapError(toPersistenceSqlError("OrchestrationEventStore.prunedThroughSequence:query")),
  );

  // Advance over raw rows so even a full page of unknown types cannot end replay early.
  const catchUpApplicationEvents = (input: {
    readonly afterSequence: number;
    readonly throughSequence: number;
    readonly skipUnknownEventTypes?: boolean;
  }): Stream.Stream<ApplicationStoredEvent, OrchestrationEventStoreError> =>
    Stream.paginate(input.afterSequence, (afterSequence) =>
      readApplicationRows({
        afterSequence,
        throughSequence: input.throughSequence,
        limit: READ_PAGE_SIZE,
      }).pipe(
        Effect.mapError(
          toPersistenceSqlError("OrchestrationEventStore.readApplicationEvents:query"),
        ),
        Effect.map((rows) => {
          const last = rows.at(-1);
          return [
            rows,
            last === undefined ||
            rows.length < READ_PAGE_SIZE ||
            last.sequence >= input.throughSequence
              ? Option.none()
              : Option.some(last.sequence),
          ] as const;
        }),
      ),
    ).pipe(
      Stream.filterEffect((row) => keepReplayRow(row, input.skipUnknownEventTypes)),
      Stream.mapEffect((row) =>
        rowToApplicationStoredEvent(row).pipe(
          Effect.mapError(
            toPersistenceDecodeError("OrchestrationEventStore.readApplicationEvents:decode"),
          ),
        ),
      ),
    );

  const streamProjectedApplicationEvents: OrchestrationEventStore["Service"]["streamProjectedApplicationEvents"] =
    (input) =>
      replayAndBufferProjectedLiveEvents<
        ApplicationStoredEvent,
        ReturnType<typeof input.project>,
        OrchestrationEventStoreError,
        never
      >({
        subscribe: PubSub.subscribe(committedEvents),
        latestSequence: latestApplicationSequence,
        afterSequence: input.afterSequence ?? 0,
        project: input.project,
        ...(input.filter === undefined ? {} : { filter: input.filter }),
        replay: (throughSequence) =>
          catchUpApplicationEvents({
            afterSequence: input?.afterSequence ?? 0,
            throughSequence,
          }),
      }).pipe(
        Stream.catchTag("LiveStreamBufferError", (cause) =>
          Stream.fail(
            toPersistenceSqlError("OrchestrationEventStore.streamApplicationEvents:buffer")(cause),
          ),
        ),
      );

  const streamApplicationEvents: OrchestrationEventStore["Service"]["streamApplicationEvents"] = (
    input,
  ) => streamProjectedApplicationEvents({ ...input, project: (event) => event });

  return {
    appendProjectEvent,
    appendAgentEvents,
    readAgentEvents,
    getAgentReplayStats,
    getReplayStats,
    latestAgentSequence,
    latestApplicationSequence,
    prunedThroughSequence,
    readApplicationEvents: catchUpApplicationEvents,
    publishCommitted: (events) => PubSub.publishAll(committedEvents, events).pipe(Effect.asVoid),
    streamApplicationEvents,
    streamProjectedApplicationEvents,
  } satisfies OrchestrationEventStore["Service"];
});

export const layerFromEventRows = Layer.effect(OrchestrationEventStore, makeEventStore);

export const layer = layerFromEventRows.pipe(Layer.provide(SqliteEventRows.layer));
