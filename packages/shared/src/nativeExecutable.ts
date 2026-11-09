// @effect-diagnostics nodeBuiltinImport:off -- Executable selection uses host paths in source and bundled distributions.
import * as NodePath from "node:path";
import { fileURLToPath } from "node:url";

export function nativeExecutable(component: string, override?: string): string {
  if (override !== undefined) return override;
  const directory = NodePath.dirname(fileURLToPath(import.meta.url));
  const source = directory.endsWith(NodePath.join("packages", "shared", "src"));
  return NodePath.resolve(directory, source ? "../../../bin" : "native-bin", `t3-code-ls-${component}${process.platform === "win32" ? ".exe" : ""}`);
}
