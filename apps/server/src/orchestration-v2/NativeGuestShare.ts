import { selectNativeExecutable } from "@t3tools/shared/nativeExecutable";
// @effect-diagnostics nodeBuiltinImport:off -- This adapter validates a native executable path on the host OS.
import * as NodePath from "node:path";
import {
  GUEST_MESSAGE_MAX_ATTACHMENT_BYTES,
  GUEST_MESSAGE_MAX_ATTACHMENTS,
  GUEST_RESOLVED_MESSAGE_LIMIT,
  GuestMessageAttribution,
  OrchestrationV2AppThreadJson,
  type OrchestrationMessageContext,
  type OrchestrationV2AppThread,
  type ThreadGuestMessage,
  type ThreadGuestShare,
} from "@t3tools/contracts";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import * as GuestAttachments from "../guest/guestAttachments.ts";
import * as ProcessRunner from "../processRunner.ts";
import * as Upstream from "./GuestShareDecider.ts";

export class NativeGuestShareError extends Schema.TaggedError<NativeGuestShareError>()(
  "NativeGuestShareError",
  {
    stage: Schema.Literals(["configuration", "request", "process", "response"]),
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    return this.stage === "configuration"
      ? "T3_CODE_LS_GUEST_SHARE_EXECUTABLE must name an absolute executable path."
      : `Lust guest-share planning failed during ${this.stage}.`;
  }
}

type DecideInput = Parameters<typeof Upstream.decideGuestShareCommand>[0];
type SendInput = Parameters<typeof Upstream.decideGuestMessageSend>[0];
type CancelGuest = Parameters<typeof Upstream.guestShareAfterCancelledSend>[1];
type ContextCommand = { readonly context?: OrchestrationMessageContext | undefined };
type Transition =
  | Upstream.GuestShareCommand
  | { readonly type: "send"; readonly messageId: string }
  | { readonly type: "cancel"; readonly messageId: string; readonly shareId?: string | undefined };

const MAX_BYTES = 128 * 1024 * 1024;
const encodeRequest = Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown));
const decodeResponse = Schema.decodeUnknownEffect(
  Schema.fromJsonString(
    Schema.Union([
      Schema.Struct({ version: Schema.Literal(1), error: Schema.String }),
      Schema.Struct({
        version: Schema.Literal(1),
        ok: Schema.Unknown,
        changed: Schema.optional(Schema.Boolean),
      }),
    ]),
  ),
  { onExcessProperty: "error" },
);
const decodeThread = Schema.decodeUnknownEffect(
  OrchestrationV2AppThreadJson.mapFields((fields) => ({
    ...fields,
    archivedAt: Schema.NullOr(Schema.DateTimeUtcFromMillis),
    settledAt: Schema.NullOr(Schema.DateTimeUtcFromMillis),
    unsettledAt: Schema.optional(Schema.NullOr(Schema.DateTimeUtcFromMillis)),
    updatedAt: Schema.DateTimeUtcFromMillis,
  })),
);
const decodeSent = Schema.decodeUnknownEffect(
  Schema.Struct({ thread: Schema.Unknown, text: Schema.String, guest: GuestMessageAttribution }),
  { onExcessProperty: "error" },
);
const decodeReleased = Schema.decodeUnknownEffect(
  Schema.Struct({ attachmentIds: Schema.Array(Schema.String) }),
  { onExcessProperty: "error" },
);
const decodeStripped = Schema.decodeUnknownEffect(
  Schema.Struct({ context: Schema.optional(Schema.Unknown) }),
  { onExcessProperty: "error" },
);
const Utf16Unit = Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: 0xffff }));
const AttachmentNameUnits = Schema.Array(Utf16Unit).check(
  Schema.isMinLength(1),
  Schema.isMaxLength(255),
);
const AttachmentMimeUnits = Schema.Array(Utf16Unit).check(
  Schema.isMinLength(1),
  Schema.isMaxLength(100),
);
const decodeAttachmentText = Schema.decodeUnknownEffect(
  Schema.Struct({ name: AttachmentNameUnits, mimeType: AttachmentMimeUnits }),
  { onExcessProperty: "error" },
);
const decodeClassifiedUploads = Schema.decodeUnknownEffect(
  Schema.Union([
    Schema.Struct({
      ok: Schema.Literal(false),
      reason: Schema.Literals(["too_many_files", "too_large", "unsupported_file"]),
    }),
    Schema.Struct({
      ok: Schema.Literal(true),
      attachments: Schema.Array(
        Schema.Struct({
          type: Schema.Literals(["image", "file"]),
          nameUnits: AttachmentNameUnits,
          mimeTypeUnits: AttachmentMimeUnits,
          sizeBytes: Schema.Number.check(Schema.isFinite()),
        }),
      ).check(Schema.isMaxLength(GUEST_MESSAGE_MAX_ATTACHMENTS)),
    }),
  ]),
  { onExcessProperty: "error" },
);

function utf16Units(value: string): Array<number> {
  return Array.from({ length: value.length }, (_, index) => value.charCodeAt(index));
}

function plainRecord(value: unknown): value is Record<string, unknown> {
  if (value === null || typeof value !== "object") return false;
  const prototype: unknown = Object.getPrototypeOf(value);
  return prototype === Object.prototype || prototype === null;
}

// Keep JSON extensions while the real contract restores known DateTime fields.
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

function withoutFields(value: object, fields: ReadonlyArray<string>): Record<string, unknown> {
  return Object.fromEntries(Object.entries(value).filter(([key]) => !fields.includes(key)));
}

function sameRetained(before: object, after: object, fields: ReadonlyArray<string>): boolean {
  return canonical(withoutFields(before, fields)) === canonical(withoutFields(after, fields));
}

function wireAttachments(attachments: NonNullable<ThreadGuestMessage["attachments"]>) {
  // Decisions inspect only IDs and sizes. Keep attachment text lossless through
  // the same helper when JavaScript's filename slice leaves a lone surrogate.
  return attachments.map((attachment) => ({
    ...attachment,
    name: utf16Units(attachment.name),
    mimeType: utf16Units(attachment.mimeType),
  }));
}

function wireGuestShare(share: ThreadGuestShare | null | undefined) {
  if (share == null) return share;
  return {
    ...share,
    messages: share.messages.map((message) => ({
      ...message,
      ...(message.attachments === undefined
        ? {}
        : { attachments: wireAttachments(message.attachments) }),
    })),
  };
}

function wireThread(thread: OrchestrationV2AppThread) {
  return {
    ...thread,
    ...(thread.guestShare === undefined ? {} : { guestShare: wireGuestShare(thread.guestShare) }),
    archivedAt: thread.archivedAt === null ? null : DateTime.toEpochMillis(thread.archivedAt),
    settledAt: thread.settledAt === null ? null : DateTime.toEpochMillis(thread.settledAt),
    ...(thread.unsettledAt === undefined
      ? {}
      : {
          unsettledAt:
            thread.unsettledAt === null ? null : DateTime.toEpochMillis(thread.unsettledAt),
        }),
    updatedAt: DateTime.toEpochMillis(thread.updatedAt),
  };
}

const restoreThreadAttachmentText = Effect.fn("NativeGuestShare.restoreThreadAttachmentText")(
  function* (raw: unknown) {
    if (!plainRecord(raw)) return yield* new NativeGuestShareError({ stage: "response" });
    const share = raw.guestShare;
    if (share === undefined || share === null) return raw;
    if (!plainRecord(share) || !Array.isArray(share.messages))
      return yield* new NativeGuestShareError({ stage: "response" });
    const messages = yield* Effect.forEach(share.messages, (message: unknown) =>
      Effect.gen(function* () {
        if (!plainRecord(message))
          return yield* new NativeGuestShareError({ stage: "response" });
        if (message.attachments === undefined) return message;
        if (!Array.isArray(message.attachments))
          return yield* new NativeGuestShareError({ stage: "response" });
        const attachments = yield* Effect.forEach(message.attachments, (attachment: unknown) =>
          Effect.gen(function* () {
            if (!plainRecord(attachment))
              return yield* new NativeGuestShareError({ stage: "response" });
            const text = yield* decodeAttachmentText({
              name: attachment.name,
              mimeType: attachment.mimeType,
            }).pipe(
              Effect.mapError((cause) => new NativeGuestShareError({ stage: "response", cause })),
            );
            return {
              ...attachment,
              name: String.fromCharCode(...text.name),
              mimeType: String.fromCharCode(...text.mimeType),
            };
          }),
        );
        return { ...message, attachments };
      }),
    );
    return { ...raw, guestShare: { ...share, messages } };
  },
);

function parsedDates(shares: ReadonlyArray<ThreadGuestShare | null | undefined>, expiry?: string) {
  const values = new Set(shares.flatMap((share) => (share == null ? [] : [share.expiresAt])));
  if (expiry !== undefined) values.add(expiry);
  return Object.fromEntries(
    [...values].map((value) => {
      // Upstream expiry uses Date.parse, including NaN for invalid strings.
      const parsed = Date.parse(value);
      return [value, Number.isNaN(parsed) ? null : parsed];
    }),
  );
}

function retainedMessages(
  before: ReadonlyArray<ThreadGuestMessage>,
  after: ReadonlyArray<ThreadGuestMessage>,
  transition: {
    readonly target?: { readonly id: string; readonly status: "sent" | "dismissed" };
    readonly inserted?: ThreadGuestMessage;
    readonly trimResolved: boolean;
  },
): boolean {
  let position = after.length - 1;
  if (transition.inserted !== undefined) {
    if (canonical(after[position]) !== canonical(transition.inserted)) return false;
    position -= 1;
  }
  let resolvedBudget = GUEST_RESOLVED_MESSAGE_LIMIT;
  // Only the oldest resolved history may disappear; every other record keeps its position.
  for (let index = before.length - 1; index >= 0; index -= 1) {
    const source = before[index];
    if (source === undefined) return false;
    const expected =
      source.id === transition.target?.id
        ? { ...source, status: transition.target.status }
        : source;
    if (transition.trimResolved && expected.status !== "pending") {
      if (resolvedBudget === 0) continue;
      resolvedBudget -= 1;
    }
    if (canonical(after[position]) !== canonical(expected)) return false;
    position -= 1;
  }
  return position === -1;
}

function shareTransition(
  before: OrchestrationV2AppThread,
  after: OrchestrationV2AppThread,
  now: DateTime.Utc,
  operation: Transition,
): boolean {
  const threadFields =
    operation.type === "thread.guest-message.submit"
      ? ["guestShare", "updatedAt", "settledOverride", "settledAt", "unsettledAt"]
      : ["guestShare", "updatedAt"];
  if (
    !sameRetained(before, after, threadFields) ||
    DateTime.toEpochMillis(after.updatedAt) !== DateTime.toEpochMillis(now)
  )
    return false;
  if (operation.type === "thread.guest-share.close") return after.guestShare === null;
  const next = after.guestShare;
  if (next == null) return false;
  if (operation.type === "thread.guest-share.open") {
    return (
      canonical(next) ===
      canonical({
        id: operation.shareId,
        host: operation.host,
        createdAt: DateTime.formatIso(now),
        expiresAt: operation.expiresAt,
        guestName: null,
        messages: [],
      })
    );
  }
  const previous = before.guestShare;
  if (previous == null) return false;
  if (
    (operation.type === "cancel" ||
      operation.type === "thread.guest-share.redeem" ||
      operation.type === "thread.guest-message.submit") &&
    previous.id !== operation.shareId
  )
    return false;
  if (operation.type === "thread.guest-share.redeem") {
    return next.guestName === operation.guestName && sameRetained(previous, next, ["guestName"]);
  }
  if (!sameRetained(previous, next, ["messages"])) return false;
  if (operation.type === "thread.guest-message.submit") {
    const expectedSettlement =
      before.settledOverride === "settled"
        ? { settledOverride: null, settledAt: null, unsettledAt: now }
        : {
            settledOverride: before.settledOverride,
            settledAt: before.settledAt,
            ...(before.unsettledAt === undefined ? {} : { unsettledAt: before.unsettledAt }),
          };
    if (
      !sameRetained(
        expectedSettlement,
        {
          settledOverride: after.settledOverride,
          settledAt: after.settledAt,
          ...(after.unsettledAt === undefined ? {} : { unsettledAt: after.unsettledAt }),
        },
        [],
      )
    )
      return false;
    return retainedMessages(previous.messages, next.messages, {
      inserted: {
        id: operation.guestMessageId,
        text: operation.text,
        status: "pending",
        submittedAt: DateTime.formatIso(now),
        ...((operation.attachments?.length ?? 0) === 0
          ? {}
          : { attachments: operation.attachments }),
      },
      trimResolved: true,
    });
  }
  return retainedMessages(previous.messages, next.messages, {
    target: {
      id:
        operation.type === "thread.guest-message.dismiss"
          ? operation.guestMessageId
          : operation.messageId,
      status: operation.type === "send" ? "sent" : "dismissed",
    },
    trimResolved: operation.type !== "cancel",
  });
}

function unchangedTransition(thread: OrchestrationV2AppThread, operation: Transition): boolean {
  if (operation.type === "thread.guest-share.close") return thread.guestShare == null;
  if (operation.type !== "cancel") return false;
  const share = thread.guestShare;
  return (
    share == null ||
    share.id !== operation.shareId ||
    share.messages.find((message) => message.id === operation.messageId)?.status !== "sent"
  );
}

function allowedRejection(
  command: Upstream.GuestShareCommand | { readonly type: "send"; readonly messageId: string },
  error: string,
): boolean {
  switch (command.type) {
    case "thread.guest-share.open":
      return error === "Archived threads cannot be shared.";
    case "thread.guest-share.close":
      return false;
    case "thread.guest-share.redeem":
      return error === Upstream.GUEST_FAILURE.inactive || error === Upstream.GUEST_FAILURE.used;
    case "thread.guest-message.submit":
      return [
        Upstream.GUEST_FAILURE.inactive,
        Upstream.GUEST_FAILURE.busy,
        Upstream.GUEST_FAILURE.filesBusy,
      ].some((value) => value === error);
    case "thread.guest-message.dismiss":
    case "send":
      return (
        error === "This thread has no guest link." ||
        error ===
          `Guest message ${command.type === "send" ? command.messageId : command.guestMessageId} is not pending.`
      );
  }
}

export const make = Effect.fn("NativeGuestShare.make")(function* () {
  const executable = selectNativeExecutable(
    "guest-share",
    (yield* HostProcess.Environment).T3_CODE_LS_GUEST_SHARE_EXECUTABLE,
    yield* HostProcess.Platform,
    yield* HostProcess.Architecture,
    (yield* HostProcess.Environment).T3_CODE_LS_IMPLEMENTATION,
  );
  const runner = yield* ProcessRunner.make();

  const call = Effect.fn("NativeGuestShare.call")(function* (request: object) {
    if (
      executable === undefined ||
      executable.length === 0 ||
      executable.includes("\0") ||
      !NodePath.isAbsolute(executable)
    ) {
      return yield* new NativeGuestShareError({ stage: "configuration" });
    }
    const encoded =
      (yield* encodeRequest({ version: 1, ...request }).pipe(
        Effect.mapError((cause) => new NativeGuestShareError({ stage: "request", cause })),
      )) + "\n";
    if (Buffer.byteLength(encoded, "utf8") > MAX_BYTES)
      return yield* new NativeGuestShareError({ stage: "request" });
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
      .pipe(Effect.mapError((cause) => new NativeGuestShareError({ stage: "process", cause })));
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
      return yield* new NativeGuestShareError({ stage: "response" });
    }
    return yield* decodeResponse(result.stdout).pipe(
      Effect.mapError((cause) => new NativeGuestShareError({ stage: "response", cause })),
    );
  });

  const restoreThread = Effect.fn("NativeGuestShare.restoreThread")(function* (
    raw: unknown,
    changed: boolean | undefined,
    before: OrchestrationV2AppThread,
    now: DateTime.Utc,
    operation: Parameters<typeof shareTransition>[3],
  ) {
    const restored = yield* restoreThreadAttachmentText(raw);
    const decoded = yield* decodeThread(restored).pipe(
      Effect.mapError((cause) => new NativeGuestShareError({ stage: "response", cause })),
    );
    const after = preserveJsonFields(restored, decoded);
    const expectedChanged = !unchangedTransition(before, operation);
    if (
      changed !== expectedChanged ||
      (changed
        ? !shareTransition(before, after, now, operation)
        : canonical(before) !== canonical(after))
    ) {
      return yield* new NativeGuestShareError({ stage: "response" });
    }
    return changed ? after : before;
  });

  return {
    classifyGuestUploads: Effect.fn("NativeGuestShare.classifyGuestUploads")(function* (
      uploads: ReadonlyArray<GuestAttachments.InspectedGuestUpload>,
    ) {
      if (executable === undefined) return GuestAttachments.classifyGuestUploads(uploads);
      if (uploads.some((upload) => !Number.isFinite(upload.sizeBytes)))
        return yield* new NativeGuestShareError({ stage: "request" });
      const response = yield* call({
        operation: "classify-uploads",
        uploads: uploads.map((upload) => ({
          nameUnits: utf16Units(upload.name),
          // Keep JavaScript's Unicode case tables at the host boundary. Rust owns
          // MIME selection, trimming, validation and all filename transformations.
          contentTypeLowercaseUnits: utf16Units(upload.contentType.toLowerCase()),
          sizeBytes: upload.sizeBytes,
          header: Array.from(upload.header),
        })),
      });
      if ("error" in response || response.changed !== undefined)
        return yield* new NativeGuestShareError({ stage: "response" });
      const classified = yield* decodeClassifiedUploads(response.ok).pipe(
        Effect.mapError((cause) => new NativeGuestShareError({ stage: "response", cause })),
      );
      if (!classified.ok) return classified;
      if (
        classified.attachments.length !== uploads.length ||
        uploads.reduce((sum, upload) => sum + upload.sizeBytes, 0) >
          GUEST_MESSAGE_MAX_ATTACHMENT_BYTES ||
        classified.attachments.some((draft, index) => draft.sizeBytes !== uploads[index]?.sizeBytes)
      ) {
        return yield* new NativeGuestShareError({ stage: "response" });
      }
      return {
        ok: true as const,
        attachments: classified.attachments.map((draft) => ({
          type: draft.type,
          name: String.fromCharCode(...draft.nameUnits),
          mimeType: String.fromCharCode(...draft.mimeTypeUnits),
          sizeBytes: draft.sizeBytes,
        })),
      };
    }),
    decideGuestShareCommand: Effect.fn("NativeGuestShare.decideGuestShareCommand")(function* (
      input: DecideInput,
    ) {
      if (executable === undefined) return Upstream.decideGuestShareCommand(input);
      const response = yield* call({
        operation: "decide",
        thread: wireThread(input.thread),
        command:
          input.command.type === "thread.guest-message.submit" &&
          input.command.attachments !== undefined
            ? { ...input.command, attachments: wireAttachments(input.command.attachments) }
            : input.command,
        now: DateTime.toEpochMillis(input.now),
        parsedDates: parsedDates(
          [input.thread.guestShare],
          input.command.type === "thread.guest-share.open" ? input.command.expiresAt : undefined,
        ),
      });
      if ("error" in response) {
        if (!allowedRejection(input.command, response.error))
          return yield* new NativeGuestShareError({ stage: "response" });
        return Result.fail(response.error);
      }
      return Result.succeed(
        yield* restoreThread(response.ok, response.changed, input.thread, input.now, input.command),
      );
    }),
    decideGuestMessageSend: Effect.fn("NativeGuestShare.decideGuestMessageSend")(function* (
      input: SendInput,
    ) {
      if (executable === undefined) return Upstream.decideGuestMessageSend(input);
      const operation = { type: "send", messageId: input.guestMessageId } as const;
      const response = yield* call({
        operation: "send",
        thread: wireThread(input.thread),
        messageId: input.guestMessageId,
        now: DateTime.toEpochMillis(input.now),
        parsedDates: parsedDates([input.thread.guestShare]),
      });
      if ("error" in response) {
        if (!allowedRejection(operation, response.error))
          return yield* new NativeGuestShareError({ stage: "response" });
        return Result.fail(response.error);
      }
      if (response.changed !== true) return yield* new NativeGuestShareError({ stage: "response" });
      const sent = yield* decodeSent(response.ok).pipe(
        Effect.mapError((cause) => new NativeGuestShareError({ stage: "response", cause })),
      );
      const thread = yield* restoreThread(
        sent.thread,
        response.changed,
        input.thread,
        input.now,
        operation,
      );
      const share = input.thread.guestShare;
      const message = share?.messages.find((candidate) => candidate.id === input.guestMessageId);
      const name = share?.guestName ?? "Guest";
      if (
        share == null ||
        message === undefined ||
        sent.text !== `Message from guest ${name}, sent by the owner:\n\n${message.text}` ||
        canonical(sent.guest) !==
          canonical({
            guestMessageId: input.guestMessageId,
            name,
            text: message.text,
            shareId: share.id,
          })
      ) {
        return yield* new NativeGuestShareError({ stage: "response" });
      }
      return Result.succeed({ thread, text: sent.text, guest: sent.guest });
    }),
    guestShareAfterCancelledSend: Effect.fn("NativeGuestShare.guestShareAfterCancelledSend")(
      function* (thread: OrchestrationV2AppThread, guest: CancelGuest, now: DateTime.Utc) {
        if (executable === undefined)
          return Upstream.guestShareAfterCancelledSend(thread, guest, now);
        const response = yield* call({
          operation: "cancel",
          thread: wireThread(thread),
          guest,
          now: DateTime.toEpochMillis(now),
          parsedDates: parsedDates([thread.guestShare]),
        });
        if ("error" in response) return yield* new NativeGuestShareError({ stage: "response" });
        return yield* restoreThread(response.ok, response.changed, thread, now, {
          type: "cancel",
          messageId: guest.guestMessageId,
          shareId: guest.shareId,
        });
      },
    ),
    releasedGuestAttachmentIds: Effect.fn("NativeGuestShare.releasedGuestAttachmentIds")(function* (
      before: ThreadGuestShare | null,
      after: ThreadGuestShare | null,
    ) {
      if (executable === undefined) return Upstream.releasedGuestAttachmentIds(before, after);
      const response = yield* call({
        operation: "release",
        before: wireGuestShare(before),
        after: wireGuestShare(after),
        parsedDates: parsedDates([before, after]),
      });
      if ("error" in response || response.changed !== undefined)
        return yield* new NativeGuestShareError({ stage: "response" });
      const released = yield* decodeReleased(response.ok).pipe(
        Effect.mapError((cause) => new NativeGuestShareError({ stage: "response", cause })),
      );
      const kept = new Set(
        after !== null && after.id === before?.id
          ? after.messages.flatMap((message) =>
              message.status === "dismissed"
                ? []
                : (message.attachments ?? []).map((file) => file.id),
            )
          : [],
      );
      const allowed = new Map<string, number>();
      for (const message of before?.messages ?? []) {
        if (message.status !== "pending") continue;
        for (const file of message.attachments ?? [])
          if (!kept.has(file.id)) allowed.set(file.id, (allowed.get(file.id) ?? 0) + 1);
      }
      for (const id of released.attachmentIds) {
        const remaining = allowed.get(id) ?? 0;
        if (remaining === 0) return yield* new NativeGuestShareError({ stage: "response" });
        allowed.set(id, remaining - 1);
      }
      return released.attachmentIds;
    }),
    withoutGuestAttribution: Effect.fn("NativeGuestShare.withoutGuestAttribution")(function* <
      C extends ContextCommand,
    >(command: C) {
      if (executable === undefined) return Upstream.withoutGuestAttribution(command);
      const response = yield* call({
        operation: "strip",
        ...(command.context === undefined ? {} : { context: command.context }),
      });
      if ("error" in response || response.changed === undefined)
        return yield* new NativeGuestShareError({ stage: "response" });
      const stripped = yield* decodeStripped(response.ok).pipe(
        Effect.mapError((cause) => new NativeGuestShareError({ stage: "response", cause })),
      );
      const { guest: _guest, ...retainedContext } = command.context ?? {};
      const expected = command.context === undefined ? undefined : retainedContext;
      if (
        canonical(stripped.context) !== canonical(expected) ||
        response.changed !== (command.context?.guest !== undefined)
      )
        return yield* new NativeGuestShareError({ stage: "response" });
      if (!response.changed) return command;
      // Preserve generic command fields and context values that never crossed JSON.
      return { ...command, context: expected } as C;
    }),
  };
});
