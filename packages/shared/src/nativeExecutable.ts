/* oxlint-disable t3code/no-global-process-runtime -- Synchronous executable paths and standalone build scripts have no Effect runtime. */
import * as NodeOS from "node:os";
import * as NodeFS from "node:fs";
// @effect-diagnostics nodeBuiltinImport:off -- Executable selection uses host paths in source and bundled distributions.
import * as NodePath from "node:path";
import * as NodeURL from "node:url";

const hostGlibcVersion = (() => {
  try {
    const report = process.report.getReport();
    if (!("header" in report) || typeof report.header !== "object" || report.header === null)
      return undefined;
    if (!("glibcVersionRuntime" in report.header)) return undefined;
    const version = report.header.glibcVersionRuntime;
    return typeof version === "string" ? version : undefined;
  } catch {
    return undefined;
  }
})();

export function isQualifiedNativeHost(
  platform: NodeJS.Platform = NodeOS.platform(),
  architecture: string = NodeOS.arch(),
  glibcVersion: string | undefined = hostGlibcVersion,
): boolean {
  if (platform !== "linux" || architecture !== "x64" || glibcVersion === undefined) return false;
  const match = /^(\d+)\.(\d+)(?:\.\d+)?$/.exec(glibcVersion);
  return (
    match !== null && (Number(match[1]) > 2 || (Number(match[1]) === 2 && Number(match[2]) >= 39))
  );
}

function packageUsesUpstream(
  component: string,
  platform: NodeJS.Platform,
  architecture: string,
): boolean {
  const manifestPath = NodePath.join(
    NodePath.dirname(nativeExecutable(component)),
    "manifest.json",
  );
  if (!NodeFS.existsSync(manifestPath)) return false;
  const manifest: unknown = JSON.parse(NodeFS.readFileSync(manifestPath, "utf8"));
  if (typeof manifest !== "object" || manifest === null)
    throw new Error("Invalid native implementation manifest");
  if (!("implementation" in manifest) || manifest.implementation === "native") {
    return false;
  }
  if (
    manifest.implementation !== "upstream" ||
    !("target" in manifest) ||
    manifest.target !== `${platform}-${architecture}` ||
    !("selection" in manifest) ||
    !["explicit-upstream", "unqualified-platform", "incompatible-host-libc"].includes(
      String(manifest.selection),
    ) ||
    !("upstreamCommit" in manifest) ||
    typeof manifest.upstreamCommit !== "string" ||
    !/^[a-f0-9]{40}$/.test(manifest.upstreamCommit) ||
    !("files" in manifest) ||
    typeof manifest.files !== "object" ||
    manifest.files === null ||
    Array.isArray(manifest.files) ||
    Object.keys(manifest.files).length !== 0
  )
    throw new Error("Invalid upstream implementation manifest");
  return true;
}

export function selectNativeExecutable(
  component: string,
  override?: string,
  platform: NodeJS.Platform = NodeOS.platform(),
  architecture: string = NodeOS.arch(),
  implementation: string | undefined = process.env.T3_CODE_LS_IMPLEMENTATION,
): string | undefined {
  if (implementation !== undefined && implementation !== "upstream" && implementation !== "native")
    throw new Error("T3_CODE_LS_IMPLEMENTATION must be upstream or native");
  // The adapter validates an explicit selection through its own error contract.
  if (override !== undefined) return override;
  if (implementation === "upstream") return undefined;
  if (!isQualifiedNativeHost(platform, architecture)) {
    if (implementation === "native")
      throw new Error("The native implementation is not qualified for this host");
    return undefined;
  }
  if (implementation !== "native" && packageUsesUpstream(component, platform, architecture))
    return undefined;
  return nativeExecutable(component);
}

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
