import { nativeExecutable } from "@t3tools/shared/nativeExecutable";
import {
  type ChatAttachment,
  CommandId,
  type OrchestrationV2Command,
  OrchestrationV2ProviderCapabilities,
  type OrchestrationV2GoalCommand,
  type OrchestrationV2ProviderGoalStatus,
  type OrchestrationV2Run,
  OrchestrationV2ThreadProjection,
  ProviderInstanceId,
  ThreadId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import { makeNativeCommandPolicy } from "./NativeCommandPolicy.ts";

export const SteeringExecutionPolicyV2 = Schema.Literals(["active_steering", "interrupt_restart"]);
export type SteeringExecutionPolicyV2 = typeof SteeringExecutionPolicyV2.Type;

export const ForkExecutionPolicyV2 = Schema.Literals(["native_fork", "portable_context"]);
export type ForkExecutionPolicyV2 = typeof ForkExecutionPolicyV2.Type;

export const CommandPolicyCapability = Schema.Literals([
  "queued_messages",
  "active_steering",
  "interrupt",
  "interrupt_restart_steering",
  "native_fork",
  "fork_from_turn",
  "rollback",
  "rollback_snapshot",
  "context_handoff",
  "strong_terminal_status",
]);
export type CommandPolicyCapability = typeof CommandPolicyCapability.Type;

export class CommandPolicyCapabilityUnsupportedError extends Schema.TaggedError<CommandPolicyCapabilityUnsupportedError>()(
  "CommandPolicyCapabilityUnsupportedError",
  {
    commandId: CommandId,
    threadId: ThreadId,
    providerInstanceId: ProviderInstanceId,
    capability: CommandPolicyCapability,
    detail: Schema.String,
  },
) {
  override get message(): string {
    return `${this.providerInstanceId} cannot satisfy ${this.capability} for command ${this.commandId}: ${this.detail}`;
  }
}

export const CommandPolicyV2Error = Schema.Union([CommandPolicyCapabilityUnsupportedError]);
export type CommandPolicyV2Error = typeof CommandPolicyV2Error.Type;

type MessageDispatchMode = Extract<
  OrchestrationV2Command,
  { readonly type: "message.dispatch" }
>["dispatchMode"];

export function isNativeMaintenanceCommand(message: {
  readonly text: string;
  readonly attachments: ReadonlyArray<ChatAttachment>;
  readonly context?: import("@t3tools/contracts").OrchestrationMessageContext | undefined;
}): boolean {
  return (
    message.attachments.length === 0 &&
    ["/compact", "/logout"].includes(message.text.trim().toLowerCase())
  );
}

/**
 * A goal command sent into active work stops that work when it starts or
 * replaces goal work, or ends a goal that is driving the run.
 */
export type GoalCommandStopsActiveRun = (
  command: OrchestrationV2GoalCommand,
  goalStatus: OrchestrationV2ProviderGoalStatus | null,
) => boolean;

export type ResolveMessageDispatchIntent = (
  projection: OrchestrationV2ThreadProjection,
  requestedMode: MessageDispatchMode,
  deliveryIntent?: "auto" | "steer" | "restart",
) => MessageDispatchMode;

/** A run that still owns the thread's provider turn. */
export function isActiveRun(run: OrchestrationV2Run): boolean {
  return (
    run.status === "preparing" ||
    run.status === "starting" ||
    run.status === "running" ||
    run.status === "waiting"
  );
}

interface CapabilityCheckInput {
  readonly commandId: CommandId;
  readonly threadId: ThreadId;
  readonly providerInstanceId: ProviderInstanceId;
  readonly capabilities: OrchestrationV2ProviderCapabilities;
}

export interface CommandPolicyV2Shape {
  readonly resolveMessageDispatchIntent: (
    ...args: Parameters<ResolveMessageDispatchIntent>
  ) => Effect.Effect<ReturnType<ResolveMessageDispatchIntent>>;
  readonly goalCommandStopsActiveRun: (
    ...args: Parameters<GoalCommandStopsActiveRun>
  ) => Effect.Effect<boolean>;
  readonly ensureQueuedMessages: (
    input: CapabilityCheckInput,
  ) => Effect.Effect<void, CommandPolicyV2Error>;
  readonly decideSteeringExecution: (
    input: CapabilityCheckInput & {
      readonly forceRestart?: boolean;
    },
  ) => Effect.Effect<SteeringExecutionPolicyV2, CommandPolicyV2Error>;
  readonly ensureInterrupt: (
    input: CapabilityCheckInput,
  ) => Effect.Effect<void, CommandPolicyV2Error>;
  readonly ensureNativeFork: (
    input: CapabilityCheckInput & {
      readonly fromSpecificTurn: boolean;
    },
  ) => Effect.Effect<void, CommandPolicyV2Error>;
  readonly decideForkExecution: (
    input: CapabilityCheckInput & {
      readonly sameProvider: boolean;
      readonly hasStrongNativeSource: boolean;
      readonly sourceRunStatus: OrchestrationV2Run["status"];
      readonly fromSpecificTurn: boolean;
      /** Later source runs are still active; providers cannot fork a thread mid-turn. */
      readonly sourceBusy: boolean;
    },
  ) => Effect.Effect<ForkExecutionPolicyV2, CommandPolicyV2Error>;
  readonly ensureRollback: (
    input: CapabilityCheckInput,
  ) => Effect.Effect<void, CommandPolicyV2Error>;
  readonly ensureContextHandoff: (
    input: CapabilityCheckInput & {
      readonly strategy: "fork_delta_context" | "delta_context" | "full_thread_summary";
    },
  ) => Effect.Effect<void, CommandPolicyV2Error>;
}

export class CommandPolicyV2 extends Context.Service<CommandPolicyV2, CommandPolicyV2Shape>()(
  "t3/orchestration-v2/CommandPolicy/CommandPolicyV2",
) {}

function unsupported(
  input: CapabilityCheckInput,
  capability: CommandPolicyCapability,
  detail: string,
) {
  return new CommandPolicyCapabilityUnsupportedError({
    commandId: input.commandId,
    threadId: input.threadId,
    providerInstanceId: input.providerInstanceId,
    capability,
    detail,
  });
}

export const layer: Layer.Layer<CommandPolicyV2> = Layer.sync(CommandPolicyV2, () =>
  makeNativeCommandPolicy(nativeExecutable("policy", process.env.T3_CODE_LS_POLICY_EXECUTABLE), unsupported),
);
