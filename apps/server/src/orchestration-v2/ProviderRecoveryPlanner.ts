// @effect-diagnostics nodeBuiltinImport:off -- This adapter validates a native executable path on the host OS.
import * as NodePath from "node:path";
import {
  EventId,
  OrchestrationV2DomainEventJson,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2ThreadProjection,
} from "@t3tools/contracts";
import * as UpstreamRecovery from "@t3tools/provider-core/server/subagentProjection";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as ProcessRunner from "../processRunner.ts";

export class NativeRecoveryPlannerError extends Schema.TaggedError<NativeRecoveryPlannerError>()(
  "NativeRecoveryPlannerError",
  {
    stage: Schema.Literals(["configuration", "request", "process", "response"]),
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    return this.stage === "configuration"
      ? "T3_CODE_LS_RECOVERY_EXECUTABLE must name an absolute executable path."
      : `Lust recovery planning failed during ${this.stage}.`;
  }
}

type OrphanInput<E> = Parameters<typeof UpstreamRecovery.endOrphanedNativeSubagent<E>>[0];
type RootsInput<E> = Parameters<typeof UpstreamRecovery.endRunlessRootTurns<E>>[0];
type ReconcileInput<E> = Omit<RootsInput<E>, "projection"> & {
  readonly projection: Pick<
    OrchestrationV2ThreadProjection,
    "nodes" | "subagents" | "turnItems" | "providerTurns"
  >;
};
type Input<E> = OrphanInput<E> | RootsInput<E> | ReconcileInput<E>;
type Operation = "orphan" | "roots" | "reconcile";
type RecoveryEvent = Extract<
  OrchestrationV2DomainEvent,
  {
    readonly type:
      | "subagent.updated"
      | "node.updated"
      | "turn-item.updated"
      | "provider-turn.updated";
  }
>;

const MAX_BYTES = 128 * 1024 * 1024;
const MAX_EVENTS = 65_536;
const Response = Schema.Struct({
  version: Schema.Literal(1),
  events: Schema.Array(Schema.Unknown),
});
const encodeRequest = Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown));
const decodeResponse = Schema.decodeUnknownEffect(Schema.fromJsonString(Response), {
  onExcessProperty: "error",
});
const decodeEvent = Schema.decodeUnknownEffect(OrchestrationV2DomainEventJson);
const draftFields = new Set([
  "type",
  "threadId",
  "runId",
  "nodeId",
  "providerInstanceId",
  "driver",
  "occurredAt",
  "payload",
]);

function isRecoveryEvent(event: OrchestrationV2DomainEvent): event is RecoveryEvent {
  return (
    event.type === "subagent.updated" ||
    event.type === "node.updated" ||
    event.type === "turn-item.updated" ||
    event.type === "provider-turn.updated"
  );
}

function plainRecord(value: unknown): value is Record<string, unknown> {
  if (value === null || typeof value !== "object") return false;
  const prototype: unknown = Object.getPrototypeOf(value);
  return prototype === Object.prototype || prototype === null;
}

// Schema restores known DateTime values but strips excess keys. Keep JSON record
// extensions, including nested fields, while decoded values retain their types.
function preserveJsonFields<A>(wire: unknown, decoded: A): A {
  if (Array.isArray(wire) && Array.isArray(decoded)) {
    return decoded.map((value, index) => preserveJsonFields(wire[index], value)) as A;
  }
  if (plainRecord(wire) && plainRecord(decoded)) {
    return Object.fromEntries([
      ...Object.entries(wire),
      ...Object.entries(decoded).map(([key, value]) => [key, preserveJsonFields(wire[key], value)]),
    ]) as A;
  }
  return decoded;
}

const recoveryFields = {
  "subagent.updated": new Set(["status", "completedAt", "updatedAt", "result"]),
  "node.updated": new Set(["status", "completedAt"]),
  "turn-item.updated": new Set(["status", "completedAt", "updatedAt"]),
  "provider-turn.updated": new Set(["status", "completedAt"]),
} as const;

function sourceKey(type: RecoveryEvent["type"], payload: Record<string, unknown>): string {
  // Recovery may patch only these fields. Compare every other field, including
  // nested associations and JSON extensions, independently of object key order.
  const retained = Object.fromEntries(
    Object.entries(payload).filter(([key]) => {
      if (recoveryFields[type].has(key)) return false;
      if (type !== "turn-item.updated") return true;
      if (key === "result" && payload.type === "subagent") return false;
      return !(
        key === "streaming" &&
        (payload.type === "assistant_message" || payload.type === "reasoning")
      );
    }),
  );
  return JSON.stringify(retained, (_key, value: unknown) =>
    plainRecord(value)
      ? Object.fromEntries(
          Object.keys(value)
            .sort()
            .map((key) => [key, value[key]]),
        )
      : value,
  );
}

function responseScope<E>(operation: Operation, input: Input<E>) {
  const projection = input.projection;
  const subagents = "subagents" in projection ? (projection.subagents ?? []) : [];
  const turns = "providerTurns" in projection ? (projection.providerTurns ?? []) : [];
  const items = projection.turnItems ?? [];
  const sources = {
    "subagent.updated": new Set(subagents.map((value) => sourceKey("subagent.updated", value))),
    "node.updated": new Set(projection.nodes.map((value) => sourceKey("node.updated", value))),
    "turn-item.updated": new Set(items.map((value) => sourceKey("turn-item.updated", value))),
    "provider-turn.updated": new Set(
      turns.map((value) => sourceKey("provider-turn.updated", value)),
    ),
  };
  const firstSubagents = new Map<string, (typeof subagents)[number]>();
  for (const subagent of subagents) {
    if (!firstSubagents.has(subagent.id)) firstSubagents.set(subagent.id, subagent);
  }
  const firstItems = new Map<string, (typeof items)[number]>();
  for (const item of items) {
    if (item.type === "subagent" && !firstItems.has(item.subagentId))
      firstItems.set(item.subagentId, item);
  }
  const roots = new Set(
    projection.nodes
      .filter((node) => node.kind === "root_turn" && node.runId === null)
      .map((node) => node.id),
  );
  const now = DateTime.toEpochMillis(input.now);
  // Limit updates to records and scopes supplied by the caller. The native
  // planner owns eligibility, event ordering, and terminal-state decisions.
  return (event: RecoveryEvent): boolean => {
    if (
      !sources[event.type].has(sourceKey(event.type, event.payload)) ||
      DateTime.toEpochMillis(event.occurredAt) !== now ||
      event.nodeId === undefined
    )
      return false;
    const parent = firstSubagents.get(event.nodeId);
    if (
      operation !== "roots" &&
      parent?.origin === "provider_native" &&
      (!("subagentId" in input) || input.subagentId === parent.id) &&
      event.threadId === parent.threadId &&
      event.providerInstanceId === parent.providerInstanceId &&
      (event.runId ?? null) === parent.runId
    ) {
      if (event.type === "subagent.updated")
        return event.payload.id === parent.id && event.driver === parent.driver;
      if (event.driver !== undefined) return false;
      if (event.type === "node.updated" && event.payload.id === parent.id) return true;
      if (event.type === "turn-item.updated" && event.payload.id === firstItems.get(parent.id)?.id)
        return true;
    }
    if (
      operation === "orphan" ||
      !("threadId" in input) ||
      !roots.has(event.nodeId) ||
      event.threadId !== input.threadId ||
      event.providerInstanceId !== input.providerInstanceId ||
      event.runId !== undefined ||
      event.driver !== undefined
    )
      return false;
    switch (event.type) {
      case "node.updated":
        return (
          event.payload.id === event.nodeId &&
          event.payload.kind === "root_turn" &&
          event.payload.runId === null
        );
      case "turn-item.updated":
        return event.payload.nodeId === event.nodeId && event.payload.runId === null;
      case "provider-turn.updated":
        return event.payload.nodeId === event.nodeId && event.payload.runAttemptId === null;
      case "subagent.updated":
        return false;
    }
  };
}

export const make = Effect.fn("ProviderRecoveryPlanner.make")(function* () {
  const executable = (yield* HostProcess.Environment).T3_CODE_LS_RECOVERY_EXECUTABLE;
  const runner = yield* ProcessRunner.make();

  const native = Effect.fn("ProviderRecoveryPlanner.native")(function* <E>(
    operation: Operation,
    input: Input<E>,
  ): Effect.fn.Return<Array<OrchestrationV2DomainEvent>, E | NativeRecoveryPlannerError> {
    if (
      executable === undefined ||
      executable.length === 0 ||
      executable.includes("\0") ||
      !NodePath.isAbsolute(executable)
    ) {
      return yield* new NativeRecoveryPlannerError({ stage: "configuration" });
    }
    const projection = input.projection;
    const encoded =
      (yield* encodeRequest({
        version: 1,
        operation,
        projection: {
          nodes: projection.nodes,
          turnItems: projection.turnItems ?? [],
          ...(operation === "roots"
            ? {}
            : { subagents: "subagents" in projection ? (projection.subagents ?? []) : [] }),
          ...(operation === "orphan"
            ? {}
            : {
                providerTurns:
                  "providerTurns" in projection ? (projection.providerTurns ?? []) : [],
              }),
        },
        emitted: input.emitted
          .filter(isRecoveryEvent)
          .map((event) => ({ type: event.type, payload: { id: event.payload.id } })),
        status: input.status,
        now: input.now,
        ...(input.completedAt === undefined ? {} : { completedAt: input.completedAt }),
        ...("subagentId" in input
          ? {
              subagentId: input.subagentId,
              ...(input.result === undefined ? {} : { result: input.result }),
            }
          : { threadId: input.threadId, providerInstanceId: input.providerInstanceId }),
      }).pipe(
        Effect.mapError((cause) => new NativeRecoveryPlannerError({ stage: "request", cause })),
      )) + "\n";
    if (Buffer.byteLength(encoded, "utf8") > MAX_BYTES)
      return yield* new NativeRecoveryPlannerError({ stage: "request" });
    const result = yield* runner
      .run({
        command: executable,
        args: [],
        env: {},
        extendEnv: false,
        stdin: encoded,
        timeout: "30 seconds",
        maxOutputBytes: MAX_BYTES,
        outputMode: "error",
      })
      .pipe(
        Effect.mapError((cause) => new NativeRecoveryPlannerError({ stage: "process", cause })),
      );
    if (
      result.code !== 0 ||
      result.timedOut ||
      result.stdoutTruncated ||
      result.stderrTruncated ||
      result.stdoutInvalidUtf8 ||
      result.stderrInvalidUtf8 ||
      result.stderr !== "" ||
      !result.stdout.endsWith("\n") ||
      result.stdout.slice(0, -1).includes("\n")
    ) {
      return yield* new NativeRecoveryPlannerError({ stage: "response" });
    }
    const response = yield* decodeResponse(result.stdout).pipe(
      Effect.mapError((cause) => new NativeRecoveryPlannerError({ stage: "response", cause })),
    );
    if (response.events.length > MAX_EVENTS)
      return yield* new NativeRecoveryPlannerError({ stage: "response" });
    const permitted = responseScope(operation, input);
    const drafts: Array<RecoveryEvent> = [];
    for (const draft of response.events) {
      if (!plainRecord(draft) || Object.keys(draft).some((key) => !draftFields.has(key))) {
        return yield* new NativeRecoveryPlannerError({ stage: "response" });
      }
      const event = yield* decodeEvent({
        ...draft,
        id: EventId.make("native-recovery-unallocated"),
      }).pipe(
        Effect.mapError((cause) => new NativeRecoveryPlannerError({ stage: "response", cause })),
      );
      if (!isRecoveryEvent(event))
        return yield* new NativeRecoveryPlannerError({ stage: "response" });
      const preserved = preserveJsonFields(draft, event);
      if (!permitted(preserved))
        return yield* new NativeRecoveryPlannerError({ stage: "response" });
      drafts.push(preserved);
    }
    // Native event IDs are never accepted. All drafts are checked before the
    // first allocation, and the caller's existing event transaction owns commit.
    const events: Array<OrchestrationV2DomainEvent> = [];
    for (const draft of drafts) events.push({ ...draft, id: yield* input.allocateEventId() });
    return events;
  });

  return {
    endOrphanedNativeSubagent: Effect.fn("ProviderRecoveryPlanner.endOrphanedNativeSubagent")(
      function* <E>(input: OrphanInput<E>) {
        return yield* executable === undefined
          ? UpstreamRecovery.endOrphanedNativeSubagent(input)
          : native("orphan", input);
      },
    ),
    endRunlessRootTurns: Effect.fn("ProviderRecoveryPlanner.endRunlessRootTurns")(function* <E>(
      input: RootsInput<E>,
    ) {
      return yield* executable === undefined
        ? UpstreamRecovery.endRunlessRootTurns(input)
        : native("roots", input);
    }),
    reconcile: Effect.fn("ProviderRecoveryPlanner.reconcile")(function* <E>(
      input: ReconcileInput<E>,
    ) {
      if (executable !== undefined) return yield* native("reconcile", input);
      const events: Array<OrchestrationV2DomainEvent> = [];
      for (const subagent of input.projection.subagents ?? []) {
        events.push(
          ...(yield* UpstreamRecovery.endOrphanedNativeSubagent({
            projection: input.projection,
            subagentId: subagent.id,
            status: input.status,
            now: input.now,
            ...(input.completedAt === undefined ? {} : { completedAt: input.completedAt }),
            emitted: [...input.emitted, ...events],
            allocateEventId: input.allocateEventId,
          })),
        );
      }
      events.push(
        ...(yield* UpstreamRecovery.endRunlessRootTurns({
          ...input,
          emitted: [...input.emitted, ...events],
        })),
      );
      return events;
    }),
  };
});
