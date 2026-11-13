// @effect-diagnostics nodeBuiltinImport:off -- This existing policy adapter starts a bounded native executable on the host OS.
import * as NodeChildProcess from "node:child_process";
import * as NodePath from "node:path";
import { once } from "node:events";
import * as DateTime from "effect/DateTime";
import type { OrchestrationV2Run } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import { makeNativeDispatchPolicy } from "./NativeDispatchPolicy.ts";

import type {
  CommandPolicyCapability,
  CommandPolicyCapabilityUnsupportedError,
  CommandPolicyV2Shape,
} from "./CommandPolicy.ts";

type Input = Parameters<CommandPolicyV2Shape["ensureQueuedMessages"]>[0];
type Rejection = (
  input: Input,
  capability: CommandPolicyCapability,
  detail: string,
) => CommandPolicyCapabilityUnsupportedError;

function capabilityMask(input: Input): number {
  const caps = input.capabilities;
  const bits = [
    caps.turns.supportsQueuedMessages,
    caps.turns.supportsActiveSteering,
    caps.turns.supportsInterrupt,
    caps.turns.supportsSteeringByInterruptRestart,
    caps.threads.canForkThread,
    caps.threads.canForkFromTurn,
    caps.identity.nativeThreadIds === "strong",
    caps.threads.canRollbackThread,
    caps.checkpointing.providerCanRollbackConversation,
    caps.checkpointing.providerRollbackReturnsSnapshot,
    caps.context.canConsumeHandoffSummaries,
    caps.context.acceptsSyntheticUserContext,
    caps.context.supportsDeltaHandoff,
    caps.context.supportsFullThreadHandoff,
  ];
  return bits.reduce((mask, enabled, bit) => mask | (enabled ? 1 << bit : 0), 0);
}

function execute(executable: string, args: ReadonlyArray<string>, signal: AbortSignal) {
  return new Promise<string>((resolve, reject) => {
    if (signal.aborted) {
      reject(new Error("Native provider policy was interrupted before execution"));
      return;
    }
    NodeChildProcess.execFile(
      executable,
      args,
      {
        encoding: "utf8",
        shell: false,
        windowsHide: true,
        timeout: 5_000,
        maxBuffer: 4_096,
        killSignal: "SIGKILL",
        signal,
        env: {},
      },
      (error, stdout, stderr) => {
        if (error || stderr !== "") {
          reject(new Error("Native provider policy process failed", { cause: error }));
          return;
        }
        resolve(stdout);
      },
    );
  });
}

function selectFollowUpOwner(
  executable: string,
  runs: ReadonlyArray<OrchestrationV2Run>,
  signal: AbortSignal,
) {
  return new Promise<OrchestrationV2Run | null>((resolve, reject) => {
    const child = NodeChildProcess.execFile(
      executable,
      ["--follow-up-owner"],
      {
        encoding: "utf8",
        shell: false,
        windowsHide: true,
        timeout: 5_000,
        maxBuffer: 4_096,
        killSignal: "SIGKILL",
        signal,
        env: {},
      },
      (error, stdout, stderr) => {
        if (error || stderr !== "") {
          reject(new Error("Native follow-up policy process failed", { cause: error }));
          return;
        }
        const match = /^ok\t(-|0|[1-9][0-9]*)\n$/.exec(stdout);
        if (match === null || match[0] !== stdout) {
          reject(new Error("Native follow-up policy returned an invalid response"));
          return;
        }
        if (match[1] === "-") {
          resolve(null);
          return;
        }
        const index = Number(match[1]);
        const run = runs[index];
        if (!Number.isSafeInteger(index) || run === undefined) {
          reject(new Error("Native follow-up policy returned an invalid run index"));
          return;
        }
        resolve(run);
      },
    );
    const input = child.stdin;
    if (input === null) {
      reject(new Error("Native follow-up policy has no input pipe"));
      return;
    }
    input.on("error", reject);
    void (async () => {
      for (const run of runs) {
        const completed =
          run.completedAt === null ? "-" : String(DateTime.toEpochMillis(run.completedAt));
        const frame =
          [
            "t3ls-followup-v1",
            run.status,
            run.ordinal,
            run.startedAt === null ? "0" : "1",
            completed,
          ].join("\t") + "\n";
        if (!input.write(frame)) await once(input, "drain", { signal });
      }
      input.end();
    })().catch((error: unknown) => {
      child.kill("SIGKILL");
      reject(error);
    });
  });
}

export function makeNativeCommandPolicy(
  executable: string,
  unsupported: Rejection,
): CommandPolicyV2Shape {
  if (!NodePath.isAbsolute(executable) || executable.includes("\0")) {
    throw new Error("T3_CODE_LS_POLICY_EXECUTABLE must be an absolute executable path");
  }

  function decide<T extends string>(
    input: Input,
    operation: string,
    arguments_: ReadonlyArray<string>,
    allowed: ReadonlyArray<T>,
    refusals: ReadonlyArray<CommandPolicyCapability>,
  ): Effect.Effect<T, CommandPolicyCapabilityUnsupportedError> {
    const args = ["t3ls-policy-v1", operation, String(capabilityMask(input)), ...arguments_];
    return Effect.promise((signal) => execute(executable, args, signal)).pipe(
      Effect.flatMap((stdout) => {
        if (!stdout.endsWith("\n") || /[^\t\x20-\x7e]/.test(stdout.slice(0, -1))) {
          return Effect.die(new Error("Native provider policy returned an invalid response frame"));
        }
        const fields = stdout.slice(0, -1).split("\t");
        if (fields[0] === "ok" && fields.length === 2) {
          for (const value of allowed) {
            if (fields[1] === value) return Effect.succeed(value);
          }
        }
        const detail = fields[2];
        if (
          fields[0] === "unsupported" &&
          fields.length === 3 &&
          typeof detail === "string" &&
          detail !== ""
        ) {
          for (const capability of refusals) {
            if (fields[1] === capability) {
              return Effect.fail(unsupported(input, capability, detail));
            }
          }
        }
        return Effect.die(new Error("Native provider policy returned an unknown decision"));
      }),
    );
  }

  return {
    followUpOwnerRun: Effect.fn("NativeCommandPolicy.followUpOwnerRun")((runs) =>
      Effect.promise((signal) => selectFollowUpOwner(executable, runs, signal)),
    ),
    ...makeNativeDispatchPolicy((args, signal) => execute(executable, args, signal)),
    ensureQueuedMessages: Effect.fn("NativeCommandPolicy.ensureQueuedMessages")((input: Input) =>
      decide(input, "queued", [], ["-"], ["queued_messages"]).pipe(Effect.asVoid),
    ),
    decideSteeringExecution: Effect.fn("NativeCommandPolicy.decideSteeringExecution")(
      (input: Parameters<CommandPolicyV2Shape["decideSteeringExecution"]>[0]) =>
        decide(
          input,
          "steering",
          [input.forceRestart ? "1" : "0"],
          ["active_steering", "interrupt_restart"] as const,
          ["active_steering", "interrupt_restart_steering"],
        ),
    ),
    ensureInterrupt: Effect.fn("NativeCommandPolicy.ensureInterrupt")((input: Input) =>
      decide(input, "interrupt", [], ["-"], ["interrupt"]).pipe(Effect.asVoid),
    ),
    ensureNativeFork: Effect.fn("NativeCommandPolicy.ensureNativeFork")(
      (input: Parameters<CommandPolicyV2Shape["ensureNativeFork"]>[0]) =>
        decide(
          input,
          "native-fork",
          [input.fromSpecificTurn ? "1" : "0"],
          ["-"],
          ["native_fork", "fork_from_turn"],
        ).pipe(Effect.asVoid),
    ),
    decideForkExecution: Effect.fn("NativeCommandPolicy.decideForkExecution")(
      (input: Parameters<CommandPolicyV2Shape["decideForkExecution"]>[0]) =>
        decide(
          input,
          "fork",
          [
            input.sameProvider ? "1" : "0",
            input.hasStrongNativeSource ? "1" : "0",
            input.sourceRunStatus === "completed" || input.sourceRunStatus === "waiting"
              ? "1"
              : "0",
            input.fromSpecificTurn ? "1" : "0",
            input.sourceBusy ? "1" : "0",
          ],
          ["native_fork", "portable_context"] as const,
          ["context_handoff"],
        ),
    ),
    ensureRollback: Effect.fn("NativeCommandPolicy.ensureRollback")((input: Input) =>
      decide(input, "rollback", [], ["-"], ["rollback", "rollback_snapshot"]).pipe(Effect.asVoid),
    ),
    ensureContextHandoff: Effect.fn("NativeCommandPolicy.ensureContextHandoff")(
      (input: Parameters<CommandPolicyV2Shape["ensureContextHandoff"]>[0]) =>
        decide(input, "handoff", [input.strategy], ["-"], ["context_handoff"]).pipe(Effect.asVoid),
    ),
  };
}
