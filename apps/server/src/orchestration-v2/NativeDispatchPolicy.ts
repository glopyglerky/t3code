import * as Effect from "effect/Effect";
import type { CommandPolicyV2Shape } from "./CommandPolicy.ts";
import { dispatchFacts, dispatchResult } from "./NativeDispatchFacts.ts";

type Execute = (args: ReadonlyArray<string>, signal: AbortSignal) => Promise<string>;
type DispatchMethods = Pick<CommandPolicyV2Shape, "resolveMessageDispatchIntent" | "goalCommandStopsActiveRun">;

export function makeNativeDispatchPolicy(execute: Execute): DispatchMethods {
  const value = (args: ReadonlyArray<string>) => Effect.promise((signal) => execute(args, signal)).pipe(
    Effect.map((stdout) => {
      const match = /^ok\t([a-z_]+)\n$/.exec(stdout);
      if (match?.[1] === undefined || match[0] !== stdout) {
        throw new Error("Native dispatch policy returned an invalid response frame");
      }
      return match[1];
    }),
  );
  return {
    resolveMessageDispatchIntent: Effect.fn("NativeCommandPolicy.resolveMessageDispatchIntent")(
      function* (...args: Parameters<DispatchMethods["resolveMessageDispatchIntent"]>) {
        const facts = dispatchFacts(...args);
        const decision = yield* value(facts.args);
        return dispatchResult(decision, args[1], facts.activeRunId);
      },
    ),
    goalCommandStopsActiveRun: Effect.fn("NativeCommandPolicy.goalCommandStopsActiveRun")(
      function* (...args: Parameters<DispatchMethods["goalCommandStopsActiveRun"]>) {
        const decision = yield* value(["t3ls-goal-v1", args[0].type, args[1] === "active" ? "1" : "0"]);
        if (decision !== "true" && decision !== "false") {
          throw new Error("Native goal policy returned a non-boolean decision");
        }
        return decision === "true";
      },
    ),
  };
}
