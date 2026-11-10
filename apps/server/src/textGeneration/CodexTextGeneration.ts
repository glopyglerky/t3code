import * as Effect from "effect/Effect";
import * as HostProcess from "@t3tools/shared/HostProcess";
import { nativeExecutable } from "@t3tools/shared/nativeExecutable";
import * as Native from "./NativeCodexTextGeneration.ts";
import * as Windows from "./WindowsCodexTextGeneration.ts";

export const makeCodexTextGeneration: typeof Windows.makeCodexTextGeneration = Effect.fn("makeCodexTextGeneration")(function* (config, environment, getModels, resolveRuntime) {
  const host = yield* HostProcess.Environment;
  if ((yield* HostProcess.Platform) === "win32" && host.T3_CODE_LS_GENERATION_EXECUTABLE === undefined)
    return yield* Windows.makeCodexTextGeneration(config, environment, getModels, resolveRuntime);
  return yield* Native.make({ executable: nativeExecutable("generate", host.T3_CODE_LS_GENERATION_EXECUTABLE), config, environment: environment ?? process.env, getModels: getModels ?? Effect.succeed([]), resolveRuntime });
});
