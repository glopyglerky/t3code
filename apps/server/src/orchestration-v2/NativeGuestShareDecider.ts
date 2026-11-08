// @effect-diagnostics nodeBuiltinImport:off -- The selected Lust executable runs on the server host.
// The guest-share decisions execute in Lust. This file is a temporary contract/date adapter.
import * as NodeChildProcess from "node:child_process";
import * as NodePath from "node:path";
import type {
  GuestMessageAttribution,
  GuestMessageId,
  OrchestrationV2AppThread,
  ThreadGuestShare,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Result from "effect/Result";
import type { GuestShareCommand } from "./GuestShareDecider.ts";

const MAX_FRAME_BYTES = 16 * 1024 * 1024;
const TIMEOUT_MS = 30_000;

type Input = {
  readonly thread: OrchestrationV2AppThread;
  readonly now: DateTime.Utc;
};
type GuestSend = {
  readonly thread: OrchestrationV2AppThread;
  readonly text: string;
  readonly guest: GuestMessageAttribution;
};

function record(value: unknown): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new Error("Lust guest-share returned a non-object");
  }
  return value as Record<string, unknown>;
}

function dateMillis(value: DateTime.Utc | null | undefined): number | null | undefined {
  return value == null ? value : DateTime.toEpochMillis(value);
}

function wireThread(thread: OrchestrationV2AppThread) {
  return {
    ...thread,
    archivedAt: dateMillis(thread.archivedAt),
    settledAt: dateMillis(thread.settledAt),
    unsettledAt: dateMillis(thread.unsettledAt),
    updatedAt: DateTime.toEpochMillis(thread.updatedAt),
  };
}

function parsedDates(thread: OrchestrationV2AppThread, command?: GuestShareCommand) {
  const result: Record<string, number | null> = {};
  for (const value of [
    thread.guestShare?.expiresAt,
    command?.type === "thread.guest-share.open" ? command.expiresAt : undefined,
  ]) {
    if (value === undefined) continue;
    const parsed = Date.parse(value);
    result[value] = Number.isFinite(parsed) ? parsed : null;
  }
  return result;
}

function restoreDate(
  value: unknown,
  before: DateTime.Utc | null | undefined,
  now: DateTime.Utc,
): DateTime.Utc | null | undefined {
  if (value == null) return value;
  if (typeof value !== "number" || !Number.isSafeInteger(value)) {
    throw new Error("Lust guest-share returned an invalid timestamp");
  }
  if (before != null && DateTime.toEpochMillis(before) === value) return before;
  if (DateTime.toEpochMillis(now) === value) return now;
  return DateTime.makeUnsafe(value);
}

/** Retain the real application's untouched fields and identity of unchanged date values.
 * The native decider owns only guest-share state, settlement transitions, and update time.
 */
function restoreThread(
  before: OrchestrationV2AppThread,
  raw: unknown,
  now: DateTime.Utc,
): OrchestrationV2AppThread {
  const value = record(raw);
  if (
    !Object.hasOwn(value, "guestShare") ||
    !Object.hasOwn(value, "updatedAt") ||
    !Object.hasOwn(value, "settledAt") ||
    !Object.hasOwn(value, "settledOverride")
  ) {
    throw new Error("Lust guest-share returned incomplete thread state");
  }
  if (value.guestShare !== null && (typeof value.guestShare !== "object" || Array.isArray(value.guestShare))) {
    throw new Error("Lust guest-share returned an invalid share");
  }
  if (value.settledOverride !== null &&
      value.settledOverride !== "active" &&
      value.settledOverride !== "settled") {
    throw new Error("Lust guest-share returned an invalid settlement state");
  }
  return {
    ...before,
    guestShare: value.guestShare as ThreadGuestShare | null,
    settledOverride: value.settledOverride as OrchestrationV2AppThread["settledOverride"],
    settledAt: restoreDate(value.settledAt, before.settledAt, now) as DateTime.Utc | null,
    ...(Object.hasOwn(value, "unsettledAt")
      ? {
          unsettledAt: restoreDate(
            value.unsettledAt,
            before.unsettledAt,
            now,
          ) as DateTime.Utc | null,
        }
      : {}),
    updatedAt: restoreDate(value.updatedAt, before.updatedAt, now) as DateTime.Utc,
  };
}

function execute(executable: string, input: unknown, signal: AbortSignal): Promise<unknown> {
  return new Promise((resolve, reject) => {
    if (signal.aborted) {
      reject(new Error("Lust guest-share execution interrupted"));
      return;
    }
    const json = JSON.stringify(input);
    if (Buffer.byteLength(json, "utf8") + 1 > MAX_FRAME_BYTES) {
      reject(new Error("Lust guest-share request exceeds the bounded frame size"));
      return;
    }
    const child = NodeChildProcess.execFile(
      executable,
      [],
      {
        encoding: "utf8",
        shell: false,
        windowsHide: true,
        timeout: TIMEOUT_MS,
        maxBuffer: MAX_FRAME_BYTES,
        killSignal: "SIGKILL",
        env: {},
        signal,
      },
      (error, stdout, stderr) => {
        if (error || stderr !== "") {
          reject(new Error("Lust guest-share process failed", { cause: error }));
          return;
        }
        if (Buffer.byteLength(stdout, "utf8") > MAX_FRAME_BYTES ||
            !stdout.endsWith("\n") || stdout.slice(0, -1).includes("\n")) {
          reject(new Error("Lust guest-share returned an invalid response frame"));
          return;
        }
        try {
          resolve(JSON.parse(stdout.slice(0, -1)));
        } catch (cause) {
          reject(new Error("Lust guest-share returned invalid JSON", { cause }));
        }
      },
    );
    if (!child.stdin) {
      reject(new Error("Lust guest-share has no input stream"));
      return;
    }
    child.stdin.on("error", reject);
    child.stdin.end(json + "\n");
  });
}

function decision(raw: unknown): Result.Result<unknown, string> {
  const value = record(raw);
  if (typeof value.error === "string" && !Object.hasOwn(value, "ok")) {
    return Result.fail(value.error);
  }
  if (!Object.hasOwn(value, "ok") || Object.hasOwn(value, "error")) {
    throw new Error("Lust guest-share returned an invalid decision");
  }
  if (!Array.isArray(value.released) ||
      !value.released.every((item) => typeof item === "string") ||
      typeof value.pending !== "number" || !Number.isSafeInteger(value.pending) ||
      value.pending < 0) {
    throw new Error("Lust guest-share returned invalid cleanup metadata");
  }
  return Result.succeed(value.ok);
}

export function makeNativeGuestShareDecider(executable: string) {
  if (!NodePath.isAbsolute(executable) || executable.includes("\0")) {
    throw new Error("T3_CODE_LS_GUEST_SHARE_EXECUTABLE must be an absolute executable path");
  }
  const evaluate = (input: unknown) => Effect.promise((signal) => execute(executable, input, signal))
    .pipe(Effect.map(decision));

  return {
    decideGuestShareCommand: (input: Input & { readonly command: GuestShareCommand }) =>
      evaluate({
        operation: "decide", ...input, thread: wireThread(input.thread),
        now: DateTime.toEpochMillis(input.now),
        parsedDates: parsedDates(input.thread, input.command),
      }).pipe(Effect.map((result): Result.Result<OrchestrationV2AppThread, string> =>
        Result.map(result, (native) =>
          // Closing an absent share is a no-op, including object identity.
          input.command.type === "thread.guest-share.close" && input.thread.guestShare == null
            ? input.thread
            : restoreThread(input.thread, native, input.now)),
      )),
    decideGuestMessageSend: (input: Input & { readonly guestMessageId: GuestMessageId }) =>
      evaluate({
        operation: "send", thread: wireThread(input.thread), messageId: input.guestMessageId,
        now: DateTime.toEpochMillis(input.now), parsedDates: parsedDates(input.thread),
      }).pipe(Effect.map((result): Result.Result<GuestSend, string> =>
        Result.map(result, (native) => {
          const value = record(native);
          if (typeof value.text !== "string" || !value.guest || !value.thread) {
            throw new Error("Lust guest-share returned an invalid sent message");
          }
          const guest = record(value.guest);
          if (typeof guest.guestMessageId !== "string" ||
              typeof guest.name !== "string" || typeof guest.text !== "string" ||
              typeof guest.shareId !== "string") {
            throw new Error("Lust guest-share returned invalid guest attribution");
          }
          return {
            thread: restoreThread(input.thread, value.thread, input.now),
            text: value.text,
            guest: guest as unknown as GuestMessageAttribution,
          };
        }),
      )),
    afterCancelledSend: (thread: OrchestrationV2AppThread, guest: Pick<GuestMessageAttribution, "guestMessageId" | "shareId">, now: DateTime.Utc) =>
      evaluate({
        operation: "cancel", thread: wireThread(thread), guest,
        now: DateTime.toEpochMillis(now), parsedDates: parsedDates(thread),
      }).pipe(Effect.map((result) =>
        Result.match(result, {
          onFailure: (error) => { throw new Error("Lust guest-share cancellation failed: " + error); },
          onSuccess: (native) => {
            const value = record(native);
            if (value.updatedAt === DateTime.toEpochMillis(thread.updatedAt) &&
                JSON.stringify(value.guestShare) === JSON.stringify(thread.guestShare)) {
              return thread;
            }
            return restoreThread(thread, native, now);
          },
        }),
      )),
  };
}
