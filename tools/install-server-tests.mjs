#!/usr/bin/env node
/* oxlint-disable t3code/no-global-process-runtime -- The standalone dependency installer runs before the application's Effect runtime exists and must select host-specific process and link behavior. */
import * as NodeChildProcess from "node:child_process";
import * as NodeFSP from "node:fs/promises";
import * as NodeModule from "node:module";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";
import { alignRuntimeEffect } from "./server-test-runtime-identity.mjs";

const root = NodeURL.fileURLToPath(new URL("../", import.meta.url));
const manifests = NodeURL.fileURLToPath(new URL("./server-test-runtime/", import.meta.url));
const runtime = process.env.T3_CODE_LS_TEST_RUNTIME ?? manifests;
if (runtime !== manifests) {
  await NodeFSP.mkdir(runtime, { recursive: true });
  for (const name of ["package.json", "package-lock.json"])
    await NodeFSP.copyFile(NodePath.join(manifests, name), NodePath.join(runtime, name));
}
const npmArguments = ["ci", "--ignore-scripts", "--no-audit", "--no-fund"];
NodeChildProcess.execFileSync(
  process.platform === "win32" ? "cmd.exe" : "npm",
  process.platform === "win32" ? ["/d", "/s", "/c", "npm", ...npmArguments] : npmArguments,
  {
    cwd: runtime,
    stdio: "inherit",
  },
);

// Full workspace installs and the scoped npm runtime must share Effect's class
// and schema identity, even when they contain the exact same package version.
const requireRuntime = NodeModule.createRequire(
  NodePath.join(NodePath.resolve(runtime), "package.json"),
);
const { parse } = requireRuntime("yaml");
const workspaceConfig = parse(
  await NodeFSP.readFile(NodePath.join(root, "pnpm-workspace.yaml"), "utf8"),
);
const effect = await alignRuntimeEffect({
  repository: root,
  runtime,
  workspacePatterns: workspaceConfig.packages,
});
console.log(
  `Effect ${effect.version}: ${effect.mode === "workspace" ? "reusing the workspace singleton" : "using the isolated runtime singleton"}.`,
);

// Production adapters and recovery imports reach Markdown and provider SDKs.
// Apply the same pinned patches as the workspace installation before importing.
const patches = [
  ["mdast-util-math", "3.0.0"],
  ["micromark-extension-llm-math", "3.1.0", "micromark-extension-math"],
  ["micromark-extension-gfm-table", "2.1.1"],
  ["micromark", "4.0.2"],
  ["mdast-util-to-hast", "13.2.1"],
  ["@muse-code/sdk", "1.4.6"],
];
for (const [name, version, installedName = name] of patches) {
  const directory = NodePath.join(runtime, "node_modules", installedName);
  const manifest = JSON.parse(
    await NodeFSP.readFile(NodePath.join(directory, "package.json"), "utf8"),
  );
  if (manifest.version !== version) throw new Error(`Unexpected ${name} test dependency version`);
  NodeChildProcess.execFileSync(
    "patch",
    [
      "--batch",
      "--forward",
      "-p1",
      "-i",
      NodePath.join(root, "patches", `${name.replace("/", "__")}@${version}.patch`),
    ],
    {
      cwd: directory,
      stdio: "inherit",
    },
  );
}

async function linkIfMissing(name, target) {
  const link = NodePath.join(root, "node_modules", name);
  try {
    await NodeFSP.lstat(link);
    return;
  } catch (error) {
    if (error.code !== "ENOENT") throw error;
  }
  await NodeFSP.mkdir(NodePath.join(link, ".."), { recursive: true });
  await NodeFSP.symlink(target, link, process.platform === "win32" ? "junction" : "dir");
}

const manifest = JSON.parse(await NodeFSP.readFile(NodePath.join(runtime, "package.json"), "utf8"));
for (const name of Object.keys(manifest.dependencies)) {
  await linkIfMissing(name, NodePath.join(runtime, "node_modules", name));
}

// The targeted checker follows real workspace types, including settings schemas
// for the other providers. No replacement declarations or mock packages are used.
for (const entry of await NodeFSP.readdir(NodePath.join(root, "packages"), {
  withFileTypes: true,
})) {
  if (!entry.isDirectory()) continue;
  const directory = NodePath.join(root, "packages", entry.name);
  let workspace;
  try {
    workspace = JSON.parse(
      await NodeFSP.readFile(NodePath.join(directory, "package.json"), "utf8"),
    );
  } catch (error) {
    if (error.code === "ENOENT") continue;
    throw error;
  }
  await linkIfMissing(workspace.name, directory);
}
console.log(
  "Installed locked server integration test dependencies and linked actual workspace sources.",
);
