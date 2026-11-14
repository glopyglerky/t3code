/* oxlint-disable t3code/no-global-process-runtime -- Synchronous executable paths and standalone build scripts have no Effect runtime. */
import * as NodeOS from "node:os";
import * as NodeChildProcess from "node:child_process";
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";
import {
  nativeComponents,
  nativeFile,
  nativeHash,
  nativeTargets,
  nativeTargetQualified,
  isQualifiedNativeHost,
  stageNativeArtifacts,
  stageUpstreamManifest,
} from "./native-artifacts.mjs";
import { flagsForSysroot } from "./install-toolchain.mjs";

const root = NodePath.resolve(NodePath.dirname(NodeURL.fileURLToPath(import.meta.url)), "..");
function sourceIdentity() {
  const git = (...args) =>
    NodeChildProcess.execFileSync("git", ["--no-replace-objects", ...args], {
      cwd: root,
      encoding: "utf8",
      stdio: ["ignore", "pipe", "pipe"],
    }).trim();
  try {
    return {
      source_commit: git("rev-parse", "HEAD"),
      source_tree: git("rev-parse", "HEAD^{tree}"),
      source_tree_dirty: git("status", "--porcelain", "--untracked-files=normal").length !== 0,
    };
  } catch {
    // Source archives can build helpers, but cannot issue exact-commit CI evidence.
    return { source_commit: null, source_tree: null, source_tree_dirty: true };
  }
}

const destination = NodePath.resolve(root, process.argv[2] ?? "bin");
const targetKey = process.env.T3_CODE_LS_NATIVE_TARGET ?? `${NodeOS.platform()}-${NodeOS.arch()}`;
const rustTarget = nativeTargets[targetKey];
if (!rustTarget) throw new Error(`Unsupported native component target: ${targetKey}`);
const requestedImplementation = process.env.T3_CODE_LS_IMPLEMENTATION;
if (
  requestedImplementation !== undefined &&
  !["native", "upstream"].includes(requestedImplementation)
)
  throw new Error("T3_CODE_LS_IMPLEMENTATION must be native or upstream");
const incompatibleHost =
  targetKey === `${NodeOS.platform()}-${NodeOS.arch()}` && !isQualifiedNativeHost();
const upstreamSelection =
  requestedImplementation === "upstream"
    ? "explicit-upstream"
    : !nativeTargetQualified(targetKey)
      ? "unqualified-platform"
      : incompatibleHost
        ? "incompatible-host-libc"
        : undefined;
const explicitHelper = Object.keys(process.env).find(
  (name) => /^T3_CODE_LS_.*_EXECUTABLE$/.test(name) && process.env[name] !== undefined,
);
if (
  upstreamSelection &&
  (process.env.T3_CODE_LS_TOOLCHAIN !== undefined ||
    process.env.T3_CODE_LS_SYSROOT !== undefined ||
    explicitHelper ||
    requestedImplementation === "native")
) {
  throw new Error(
    requestedImplementation === "upstream"
      ? "Explicit upstream service selection conflicts with native compiler/helper settings"
      : `Native service components are not qualified for ${targetKey}; remove explicit native compiler/helper selection to retain the upstream implementations`,
  );
}
if (process.env.T3_CODE_LS_NATIVE_DIR) {
  const selected = JSON.parse(
    NodeFS.readFileSync(NodePath.join(process.env.T3_CODE_LS_NATIVE_DIR, "manifest.json"), "utf8"),
  );
  if (
    (requestedImplementation === "upstream" && selected.implementation !== "upstream") ||
    (requestedImplementation === "native" && selected.implementation === "upstream") ||
    (incompatibleHost && selected.implementation !== "upstream")
  )
    throw new Error(
      "Selected component bundle conflicts with the requested implementation or host qualification",
    );
  stageNativeArtifacts(NodePath.resolve(process.env.T3_CODE_LS_NATIVE_DIR), destination, targetKey);
} else if (upstreamSelection) {
  stageUpstreamManifest(destination, targetKey, upstreamSelection);
} else {
  const toolchain = process.env.T3_CODE_LS_TOOLCHAIN;
  if (!toolchain)
    throw new Error(
      "Set T3_CODE_LS_TOOLCHAIN to the qualified Lustr/BlueFe toolchain directory, or T3_CODE_LS_NATIVE_DIR to matching produced components",
    );
  if (process.env.CARGO_BUILD_TARGET && process.env.CARGO_BUILD_TARGET !== rustTarget) {
    throw new Error(`CARGO_BUILD_TARGET must match ${rustTarget}`);
  }
  const sysroot = NodePath.resolve(process.env.T3_CODE_LS_SYSROOT ?? toolchain);
  if (!NodeFS.existsSync(NodePath.resolve(sysroot, "lib/rustlib", rustTarget, "lib"))) {
    throw new Error(`Missing produced target sysroot for ${rustTarget}: set T3_CODE_LS_SYSROOT`);
  }
  const requestedTarget = process.env.CARGO_TARGET_DIR;
  if (!requestedTarget || !NodePath.isAbsolute(requestedTarget))
    throw new Error(
      "Set CARGO_TARGET_DIR to an absolute generated-output directory outside the checkout",
    );
  NodeFS.mkdirSync(requestedTarget, { recursive: true });
  const target = NodeFS.realpathSync(requestedTarget);
  const canonicalRoot = NodeFS.realpathSync(root);
  if (target === canonicalRoot || target.startsWith(`${canonicalRoot}${NodePath.sep}`))
    throw new Error("CARGO_TARGET_DIR must be outside the source checkout");
  const flags =
    process.env.CARGO_ENCODED_RUSTFLAGS?.split("\x1f") ??
    process.env.RUSTFLAGS?.match(/(?:[^\s"']+|"[^"]*"|'[^']*')+/g)?.map((flag) =>
      flag.replace(/^(["'])(.*)\1$/, "$2"),
    ) ??
    [];
  const env = {
    ...process.env,
    CARGO: NodePath.resolve(toolchain, "bin/cargo"),
    RUSTC: NodePath.resolve(toolchain, "bin/rustc"),
    RUSTDOC: NodePath.resolve(toolchain, "bin/rustdoc"),
    CARGO_ENCODED_RUSTFLAGS: flagsForSysroot(sysroot, flags.join("\x1f")),
  };
  NodeChildProcess.execFileSync(
    NodePath.resolve(toolchain, "bin/lustr"),
    [
      "--bluefe-root",
      toolchain,
      "build",
      "--locked",
      "--release",
      "--target",
      rustTarget,
      "--target-dir",
      target,
      "-p",
      "t3-code-ls-core",
      "-p",
      "t3-code-ls-storage",
      "--bins",
    ],
    { cwd: root, env, stdio: "inherit" },
  );
  NodeFS.mkdirSync(destination, { recursive: true });
  const files = {};
  for (const component of nativeComponents) {
    const file = nativeFile(component, targetKey);
    NodeFS.copyFileSync(
      NodePath.resolve(target, rustTarget, "release", file),
      NodePath.resolve(destination, file),
    );
    files[file] = nativeHash(NodePath.resolve(destination, file));
  }
  NodeFS.writeFileSync(
    NodePath.resolve(destination, "manifest.json"),
    JSON.stringify(
      {
        target: targetKey,
        rustTarget,
        files,
        ...sourceIdentity(),
        compiler_route: "bluefe-rustc-via-lustr",
        build_profile: "release",
        toolchain_lock_sha256: nativeHash(NodePath.join(root, "toolchain.lock.json")),
        ci_run_id: process.env.GITHUB_RUN_ID ?? null,
        ci_run_attempt: process.env.GITHUB_RUN_ATTEMPT ?? null,
      },
      null,
      2,
    ) + "\n",
  );
}
