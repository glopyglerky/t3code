import { selectNativeExecutable } from "@t3tools/shared/nativeExecutable";
// @effect-diagnostics nodeBuiltinImport:off -- This adapter validates a native executable path on the host OS.
import * as NodePath from "node:path";
import {
  ApplicationProjectCreatedEvent,
  ApplicationProjectDeletedEvent,
  ApplicationProjectMetaUpdatedEvent,
  MAX_SCRIPT_ID_LENGTH,
} from "@t3tools/contracts";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";

import * as ProcessRunner from "../processRunner.ts";
import type { UnsequencedProjectEvent } from "../persistence/OrchestrationEventStore.ts";
import * as Upstream from "./ProjectCommands.ts";

export class NativeProjectCommandsError extends Schema.TaggedError<NativeProjectCommandsError>()(
  "NativeProjectCommandsError",
  {
    stage: Schema.Literals(["configuration", "request", "process", "response"]),
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    return this.stage === "configuration"
      ? "T3_CODE_LS_PROJECT_EXECUTABLE must name an absolute executable path."
      : `Lust project planning failed during ${this.stage}.`;
  }
}

type PlanInput = Parameters<typeof Upstream.planProjectCommand>[0];

const MAX_BYTES = 128 * 1024 * 1024;
const MAX_DEPTH = 128;
const MAX_VALUES = 1_048_576;
const monogramSegmenter = new Intl.Segmenter(undefined, { granularity: "grapheme" });
const encodeRequest = Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown));
const decodeResponse = Schema.decodeUnknownEffect(
  Schema.fromJsonString(
    Schema.Union([
      Schema.Struct({ version: Schema.Literal(1), ok: Schema.Unknown }),
      Schema.Struct({ version: Schema.Literal(1), error: Schema.Unknown }),
    ]),
  ),
  { onExcessProperty: "error" },
);
const decodeEvent = Schema.decodeUnknownEffect(
  Schema.toType(
    Schema.Union([
      ApplicationProjectCreatedEvent.mapFields(({ sequence: _sequence, ...fields }) => fields),
      ApplicationProjectMetaUpdatedEvent.mapFields(({ sequence: _sequence, ...fields }) => fields),
      ApplicationProjectDeletedEvent.mapFields(({ sequence: _sequence, ...fields }) => fields),
    ]),
  ),
);
const decodeRejection = Schema.decodeUnknownEffect(Upstream.ProjectCommandRejection, {
  onExcessProperty: "error",
});

function plainRecord(value: unknown): value is Record<string, unknown> {
  if (value === null || typeof value !== "object") return false;
  const prototype: unknown = Object.getPrototypeOf(value);
  return prototype === Object.prototype || prototype === null;
}

function canonical(value: unknown): string | undefined {
  return JSON.stringify(value, (_key, entry: unknown) =>
    plainRecord(entry)
      ? Object.fromEntries(
          Object.keys(entry)
            .sort()
            .map((key) => [key, entry[key]]),
        )
      : entry,
  );
}

// Contract decoding validates known fields; native payload extensions must also survive.
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

function scalarString(value: string): boolean {
  for (const character of value) {
    const point = character.codePointAt(0);
    if (point !== undefined && point >= 0xd800 && point <= 0xdfff) return false;
  }
  return true;
}

function boundedJson(value: unknown): boolean {
  const pending = [{ value, depth: 0 }];
  let count = 1;
  while (pending.length > 0) {
    const next = pending.pop();
    if (next === undefined || next.depth > MAX_DEPTH) return false;
    if (typeof next.value === "string" && !scalarString(next.value)) return false;
    if (typeof next.value === "number" && !Number.isFinite(next.value)) return false;
    if (Array.isArray(next.value)) {
      for (const value of next.value) {
        if (++count > MAX_VALUES) return false;
        pending.push({ value, depth: next.depth + 1 });
      }
    } else if (plainRecord(next.value)) {
      for (const [key, value] of Object.entries(next.value)) {
        if (!scalarString(key) || ++count > MAX_VALUES) return false;
        pending.push({ value, depth: next.depth + 1 });
      }
    }
  }
  return true;
}

function copiedPayload(input: PlanInput): UnsequencedProjectEvent["payload"] {
  const { command, state } = input;
  const occurredAt = DateTime.formatIso(input.now);
  if (command.type === "project.create") {
    return {
      projectId: command.projectId,
      title: command.title,
      workspaceRoot: command.workspaceRoot,
      defaultModelSelection: null,
      faviconPath: null,
      projectIcon: null,
      scripts: command.scripts ?? [],
      createdAt: occurredAt,
      updatedAt: occurredAt,
    };
  }
  if (command.type === "project.delete") {
    return { projectId: command.projectId, deletedAt: occurredAt };
  }
  const fields = [
    "title",
    "workspaceRoot",
    "defaultModelSelection",
    "defaultThreadEnvMode",
    "autoPull",
    "faviconPath",
    "projectIcon",
    "scripts",
  ] as const;
  return {
    projectId: command.projectId,
    ...Object.fromEntries(
      fields.flatMap((key) => (command[key] === undefined ? [] : [[key, command[key]]])),
    ),
    ...(command.pinned === undefined
      ? {}
      : { pinnedAt: command.pinned ? (state.project?.pinnedAt ?? occurredAt) : null }),
    ...(command.archived === undefined
      ? {}
      : { archivedAt: command.archived ? (state.project?.archivedAt ?? occurredAt) : null }),
    updatedAt: occurredAt,
  };
}

function matchesEvent(input: PlanInput, value: unknown): boolean {
  const { command } = input;
  if (
    command.type === "project.create"
      ? input.state.project !== undefined
      : input.state.project?.deletedAt !== null
  )
    return false;
  const eventType = {
    "project.create": "project.created",
    "project.meta.update": "project.meta-updated",
    "project.delete": "project.deleted",
  } as const;
  return (
    canonical(value) ===
    canonical({
      eventId: input.eventId,
      aggregateKind: "project",
      aggregateId: command.projectId,
      occurredAt: DateTime.formatIso(input.now),
      commandId: command.commandId,
      causationEventId: null,
      correlationId: command.commandId,
      metadata: {},
      type: eventType[command.type],
      payload: copiedPayload(input),
    })
  );
}

function matchesRejection(
  input: PlanInput,
  error: Upstream.ProjectCommandRejection,
  monogramGraphemeCount: number | null,
): boolean {
  const { command, state } = input;
  switch (error._tag) {
    case "ProjectCommandMissingProjectError":
      return (
        command.type !== "project.create" &&
        state.project?.deletedAt !== null &&
        error.commandType === command.type &&
        error.projectId === command.projectId
      );
    case "ProjectWorkspaceConflictError":
      return (
        command.type !== "project.delete" &&
        (command.type === "project.create"
          ? state.project === undefined
          : state.project?.deletedAt === null) &&
        command.workspaceRoot !== undefined &&
        error.workspaceRoot === command.workspaceRoot &&
        error.conflictingProjectId === state.workspaceOwner?.projectId &&
        error.conflictingProjectId !== command.projectId
      );
    case "ProjectCommandInvariantError":
      if (error.commandType !== command.type) return false;
      if (command.type === "project.create")
        return (
          state.project !== undefined &&
          error.detail ===
            `Project '${command.projectId}' already exists and cannot be created twice.`
        );
      if (command.type !== "project.meta.update" || state.project?.deletedAt !== null) return false;
      return (
        (command.projectIcon?.kind === "monogram" &&
          monogramGraphemeCount !== null &&
          monogramGraphemeCount > 2 &&
          error.detail === "Project monograms must contain at most two characters.") ||
        command.scripts?.some(
          (script) =>
            error.detail ===
            `Script IDs must be 1-${MAX_SCRIPT_ID_LENGTH} lowercase letters, digits or hyphens, starting with a letter or digit (got ${script.id.length} characters).`,
        ) === true
      );
  }
}

export const make = Effect.fn("NativeProjectCommands.make")(function* () {
  const executable = selectNativeExecutable(
    "project",
    (yield* HostProcess.Environment).T3_CODE_LS_PROJECT_EXECUTABLE,
    yield* HostProcess.Platform,
    yield* HostProcess.Architecture,
    (yield* HostProcess.Environment).T3_CODE_LS_IMPLEMENTATION,
  );
  if (executable === undefined)
    return {
      planProjectCommand: Effect.fn("NativeProjectCommands.planProjectCommand")(
        (input: PlanInput) => Effect.sync(() => Upstream.planProjectCommand(input)),
      ),
    };
  const runner = yield* ProcessRunner.make();

  const planProjectCommand = Effect.fn("NativeProjectCommands.planProjectCommand")(function* (
    input: PlanInput,
  ) {
    if (executable.length === 0 || executable.includes("\0") || !NodePath.isAbsolute(executable))
      return yield* new NativeProjectCommandsError({ stage: "configuration" });
    const count = yield* Effect.try({
      try: () =>
        input.command.type === "project.meta.update" &&
        input.command.projectIcon?.kind === "monogram"
          ? Array.from(monogramSegmenter.segment(input.command.projectIcon.text)).length
          : null,
      catch: (cause) => new NativeProjectCommandsError({ stage: "request", cause }),
    });
    const encoded =
      (yield* encodeRequest({
        version: 1,
        operation: "plan",
        command: input.command,
        state: {
          project: input.state.project ?? null,
          workspaceOwner: input.state.workspaceOwner ?? null,
        },
        eventId: input.eventId,
        now: DateTime.toEpochMillis(input.now),
        monogramGraphemeCount: count,
      }).pipe(
        Effect.mapError((cause) => new NativeProjectCommandsError({ stage: "request", cause })),
      )) + "\n";
    if (Buffer.byteLength(encoded, "utf8") > MAX_BYTES)
      return yield* new NativeProjectCommandsError({ stage: "request" });
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
        Effect.mapError((cause) => new NativeProjectCommandsError({ stage: "process", cause })),
      );
    const body = result.stdout.slice(0, -1);
    if (
      result.code !== 0 ||
      result.timedOut ||
      result.stdoutTruncated ||
      result.stderrTruncated ||
      result.stdoutInvalidUtf8 ||
      result.stderrInvalidUtf8 ||
      result.stderr !== "" ||
      !result.stdout.endsWith("\n") ||
      body.includes("\n") ||
      body.includes("\r")
    )
      return yield* new NativeProjectCommandsError({ stage: "response" });
    const response = yield* decodeResponse(body).pipe(
      Effect.mapError((cause) => new NativeProjectCommandsError({ stage: "response", cause })),
    );
    if (!boundedJson(response)) return yield* new NativeProjectCommandsError({ stage: "response" });
    if ("error" in response) {
      const rejection = yield* decodeRejection(response.error).pipe(
        Effect.mapError((cause) => new NativeProjectCommandsError({ stage: "response", cause })),
      );
      if (!matchesRejection(input, rejection, count))
        return yield* new NativeProjectCommandsError({ stage: "response" });
      return Result.fail(rejection);
    }
    const event = yield* decodeEvent(response.ok).pipe(
      Effect.mapError((cause) => new NativeProjectCommandsError({ stage: "response", cause })),
    );
    if (!matchesEvent(input, response.ok))
      return yield* new NativeProjectCommandsError({ stage: "response" });
    return Result.succeed(preserveJsonFields(response.ok, event));
  });

  return { planProjectCommand };
});
