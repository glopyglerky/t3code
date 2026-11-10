import { selectNativeExecutable } from "@t3tools/shared/nativeExecutable";
// @effect-diagnostics nodeBuiltinImport:off - The native process launcher resolves paths using its host OS.
import * as NodePath from "node:path";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as HostProcess from "@t3tools/shared/HostProcess";

export class CodexAppServerProcessConfigurationError extends Schema.TaggedError<CodexAppServerProcessConfigurationError>()(
  "CodexAppServerProcessConfigurationError",
  { reason: Schema.Literals(["invalid-executable", "unsupported-platform"]) },
) {
  override get message(): string {
    return this.reason === "invalid-executable"
      ? "T3_CODE_LS_PROVIDER_EXECUTABLE must be an absolute executable path without NUL characters."
      : "The selected Lust provider runtime requires a Unix host.";
  }
}

export const selectCodexAppServerExecutable = Effect.fn("selectCodexAppServerExecutable")(
  function* () {
    const environment = yield* HostProcess.Environment;
    return selectNativeExecutable(
      "provider",
      environment.T3_CODE_LS_PROVIDER_EXECUTABLE,
      yield* HostProcess.Platform,
      yield* HostProcess.Architecture,
      environment.T3_CODE_LS_IMPLEMENTATION,
    );
  },
);

export const selectCodexAppServerCommand = Effect.fn("selectCodexAppServerCommand")(
  function* (input: {
    readonly command: string;
    readonly args: ReadonlyArray<string>;
    readonly cwd?: string | undefined;
  }) {
    const executable = yield* selectCodexAppServerExecutable();
    if (executable === undefined) return { command: input.command, args: input.args };
    if (!NodePath.isAbsolute(executable) || executable.includes("\0")) {
      return yield* new CodexAppServerProcessConfigurationError({ reason: "invalid-executable" });
    }
    if ((yield* HostProcess.Platform) === "win32") {
      return yield* new CodexAppServerProcessConfigurationError({ reason: "unsupported-platform" });
    }

    const cwd = NodePath.resolve(yield* HostProcess.WorkingDirectory, input.cwd ?? ".");
    return {
      command: executable,
      args: [
        "--inherit-env",
        "--inherit-stderr",
        "--parent-protocol",
        "--request-timeout-ms",
        "0",
        "--program",
        input.command,
        "--cwd",
        cwd,
        "--",
        ...input.args,
      ],
    };
  },
);
