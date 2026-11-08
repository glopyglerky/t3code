import * as NodePath from "node:path";
import type { ResolvedSpawnCommand } from "@t3tools/shared/shell";

export const NATIVE_CODEX_PROVIDER_EXECUTABLE = "T3_CODE_LS_PROVIDER_EXECUTABLE";

/** Pass credentials via the child environment, never secret-bearing argv. */
export function selectNativeCodexProcess(
  executable: string | undefined,
  codex: ResolvedSpawnCommand,
  cwd: string,
  home: string,
  platform: NodeJS.Platform,
): ResolvedSpawnCommand {
  if (executable === undefined) return codex;
  if (platform === "win32") {
    throw new Error("The Lust provider subprocess requires Unix socket support");
  }
  if (
    !NodePath.isAbsolute(executable) ||
    executable.includes("\0") ||
    !NodePath.isAbsolute(codex.command) ||
    codex.shell ||
    !NodePath.isAbsolute(cwd) ||
    !NodePath.isAbsolute(home)
  ) {
    throw new Error("Native provider, Codex executable, cwd and home must be absolute, without shell execution");
  }
  return {
    command: executable,
    args: [
      "--program", codex.command,
      "--cwd", cwd,
      "--home", home,
      "--inherit-env", "--",
      ...codex.args,
    ],
    shell: false,
  };
}
