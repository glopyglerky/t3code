/* oxlint-disable t3code/no-global-process-runtime -- Synchronous executable paths and standalone build scripts have no Effect runtime. */
import * as NodeOS from "node:os";
import * as NodeFS from "node:fs";
import * as NodeCrypto from "node:crypto";
// @effect-diagnostics nodeBuiltinImport:off -- Executable selection uses host paths in source and bundled distributions.
import * as NodePath from "node:path";
import * as NodeURL from "node:url";

export const supportedNativeTargets: Readonly<Record<string, string>> = {
  "linux-x64": "x86_64-unknown-linux-gnu",
  "linux-arm64": "aarch64-unknown-linux-gnu",
  "darwin-x64": "x86_64-apple-darwin",
  "darwin-arm64": "aarch64-apple-darwin",
  "win32-x64": "x86_64-pc-windows-msvc",
  "win32-arm64": "aarch64-pc-windows-msvc",
};

export const databaseAcceptanceChecks = [
  "storage-tests",
  "startup",
  "production",
  "transactions",
  "restart-and-ownership",
] as const;

export const databasePackageChecks = ["source-package", "cli-sidecar", "desktop-asar"] as const;

const record = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

/** A database-only bundle never selects unrelated native service replacements. */
export function databaseArtifactFiles(
  manifest: unknown,
  directory: string,
  target: string,
  options: { readonly lifecycleOnly?: boolean } = {},
): [string, string] {
  if (
    !Object.hasOwn(supportedNativeTargets, target) ||
    !record(manifest) ||
    manifest.schema !== 2 ||
    manifest.implementation !== "database-only" ||
    manifest.target !== target ||
    manifest.rustTarget !== supportedNativeTargets[target]
  )
    throw new Error(`Database components do not match ${target}`);
  const accepted = manifest.databaseAcceptance;
  const checks = record(accepted) ? accepted.checks : undefined;
  const file = `t3-code-ls-db${target.startsWith("win32-") ? ".exe" : ""}`;
  if (
    !record(accepted) ||
    accepted.status !== "passed" ||
    accepted.target !== target ||
    typeof accepted.sourceCommit !== "string" ||
    !/^[a-f0-9]{40}$/.test(accepted.sourceCommit) ||
    accepted.sourceCommit !== manifest.sourceCommit ||
    typeof accepted.upstreamCommit !== "string" ||
    !/^[a-f0-9]{40}$/.test(accepted.upstreamCommit) ||
    accepted.upstreamCommit !== manifest.upstreamCommit ||
    !Array.isArray(checks) ||
    !databaseAcceptanceChecks.every((check) => checks.includes(check)) ||
    (target.startsWith("linux-") &&
      (typeof accepted.glibcMinimum !== "string" || !/^\d+\.\d+$/.test(accepted.glibcMinimum))) ||
    !record(manifest.files) ||
    Object.keys(manifest.files).length !== 1 ||
    typeof manifest.files[file] !== "string" ||
    !/^[a-f0-9]{64}$/.test(manifest.files[file]) ||
    accepted.executableSha256 !== manifest.files[file] ||
    !record(manifest.compiler) ||
    manifest.compiler.route !== "rustc" ||
    typeof manifest.compiler.toolchain !== "string" ||
    !/^\d+\.\d+\.\d+$/.test(manifest.compiler.toolchain) ||
    typeof manifest.compiler.version !== "string" ||
    typeof manifest.compiler.cargoVersion !== "string" ||
    typeof manifest.compiler.executableSha256 !== "string" ||
    !/^[a-f0-9]{64}$/.test(manifest.compiler.executableSha256)
  )
    throw new Error(`Database bundle lacks complete target-specific acceptance for ${target}`);
  // Only the owned package probe may stage a lifecycle-qualified candidate.
  // Ordinary package staging and runtime selection require both receipts.
  if (!options.lifecycleOnly) {
    const packaged = manifest.packageAcceptance;
    const packageChecks = record(packaged) ? packaged.checks : undefined;
    if (
      !record(packaged) ||
      packaged.status !== "passed" ||
      packaged.target !== target ||
      packaged.sourceCommit !== manifest.sourceCommit ||
      packaged.upstreamCommit !== manifest.upstreamCommit ||
      packaged.executableSha256 !== manifest.files[file] ||
      !Array.isArray(packageChecks) ||
      !databasePackageChecks.every((check) => packageChecks.includes(check))
    )
      throw new Error(
        `Database bundle lacks complete target-specific package acceptance for ${target}`,
      );
  }
  const input = NodePath.join(directory, file);
  if (
    !NodeFS.lstatSync(input).isFile() ||
    NodeCrypto.createHash("sha256").update(NodeFS.readFileSync(input)).digest("hex") !==
      manifest.files[file]
  )
    throw new Error(`Database component hash mismatch: ${file}`);
  NodeFS.accessSync(
    input,
    target.startsWith("win32-") ? NodeFS.constants.R_OK : NodeFS.constants.X_OK,
  );
  return [file, "manifest.json"];
}

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
  if (component === "db" && manifest.selection !== "explicit-upstream")
    throw new Error(
      "Rebuild the omdb package; platform qualification is no longer an implicit SQLite selection",
    );
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
  const directory = NodePath.dirname(nativeExecutable(component));
  const manifestPath = NodePath.join(directory, "manifest.json");
  if (NodeFS.existsSync(manifestPath)) {
    const manifest: unknown = JSON.parse(NodeFS.readFileSync(manifestPath, "utf8"));
    if (record(manifest) && manifest.implementation === "database-only") {
      const target = `${platform}-${architecture}`;
      const [file] = databaseArtifactFiles(manifest, directory, target);
      if (platform === "linux") {
        const acceptance = manifest.databaseAcceptance;
        if (
          !record(acceptance) ||
          typeof acceptance.glibcMinimum !== "string" ||
          !satisfiesGlibc(hostGlibcVersion, acceptance.glibcMinimum)
        )
          throw new Error("The accepted database bundle requires a newer GNU runtime");
      }
      return component === "db" ? NodePath.join(directory, file) : undefined;
    }
  }
  if (
    component === "db" &&
    implementation !== "native" &&
    packageUsesUpstream(component, platform, architecture)
  )
    return undefined;
  if (!isQualifiedNativeHost(platform, architecture)) {
    if (component === "db" && Object.hasOwn(supportedNativeTargets, `${platform}-${architecture}`))
      throw new Error(
        "The omdb database bundle is missing or unqualified. Run node tools/build-native.mjs or install a target-tested package; use T3_CODE_LS_IMPLEMENTATION=upstream only for an intentional SQLite baseline.",
      );
    if (implementation === "native")
      throw new Error("The native implementation is not qualified for this host");
    return undefined;
  }
  if (
    component !== "db" &&
    implementation !== "native" &&
    packageUsesUpstream(component, platform, architecture)
  )
    return undefined;
  return nativeExecutable(component);
}

function satisfiesGlibc(actual: string | undefined, required: string): boolean {
  if (!actual || !/^\d+\.\d+$/.test(required) || !/^\d+\.\d+(?:\.\d+)?$/.test(actual)) return false;
  const [major = 0, minor = 0] = actual.split(".").map(Number);
  const [requiredMajor = 0, requiredMinor = 0] = required.split(".").map(Number);
  return major > requiredMajor || (major === requiredMajor && minor >= requiredMinor);
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
