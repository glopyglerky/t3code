/* oxlint-disable t3code/no-global-process-runtime -- Synchronous executable paths and standalone build scripts have no Effect runtime. */
import * as NodeOS from "node:os";
// @effect-diagnostics nodeBuiltinImport:off -- Executable selection uses host paths in source and bundled distributions.
import * as NodePath from "node:path";
import * as NodeURL from "node:url";

export function nativeExecutable(component: string, override?: string): string {
  if (override !== undefined) {
    if (!NodePath.isAbsolute(override) || override.trim() !== override)
      throw new Error("Native executable override must be an absolute path");
    return override;
  }
  const directory = NodePath.dirname(NodeURL.fileURLToPath(import.meta.url)).replace(
    /\.asar([\\/])/,
    ".asar.unpacked$1",
  );
  const source = directory.endsWith(NodePath.join("packages", "shared", "src"));
  return NodePath.resolve(
    directory,
    source ? "../../../bin" : "native-bin",
    `t3-code-ls-${component}${NodeOS.platform() === "win32" ? ".exe" : ""}`,
  );
}
