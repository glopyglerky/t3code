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
  stageNativeArtifacts,
} from "./native-artifacts.mjs";

const root = NodePath.resolve(NodePath.dirname(NodeURL.fileURLToPath(import.meta.url)), "..");
const destination = NodePath.resolve(root, process.argv[2] ?? "bin");
const targetKey = process.env.T3_CODE_LS_NATIVE_TARGET ?? `${NodeOS.platform()}-${NodeOS.arch()}`;
const rustTarget = nativeTargets[targetKey];
if (!rustTarget) throw new Error(`Unsupported native component target: ${targetKey}`);
if (process.env.T3_CODE_LS_NATIVE_DIR) {
  stageNativeArtifacts(NodePath.resolve(process.env.T3_CODE_LS_NATIVE_DIR), destination, targetKey);
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
  const target = NodePath.resolve(process.env.CARGO_TARGET_DIR ?? NodePath.resolve(root, "target"));
  const flags =
    process.env.CARGO_ENCODED_RUSTFLAGS?.split("\x1f") ??
    process.env.RUSTFLAGS?.match(/(?:[^\s"']+|"[^"]*"|'[^']*')+/g)?.map((flag) =>
      flag.replace(/^(["'])(.*)\1$/, "$2"),
    ) ??
    [];
  if (flags.some((flag) => flag === "--sysroot" || flag.startsWith("--sysroot=")))
    throw new Error("Select sysroot with T3_CODE_LS_SYSROOT instead of Rust flags");
  const env = {
    ...process.env,
    RUSTC: NodePath.resolve(toolchain, "bin/rustc"),
    RUSTDOC: NodePath.resolve(toolchain, "bin/rustdoc"),
    CARGO_ENCODED_RUSTFLAGS: [...flags, "--sysroot", sysroot].join("\x1f"),
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
    JSON.stringify({ target: targetKey, rustTarget, files }, null, 2) + "\n",
  );
}
