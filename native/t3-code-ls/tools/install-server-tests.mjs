#!/usr/bin/env node
import * as NodeChildProcess from "node:child_process";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";

const root = NodeURL.fileURLToPath(new URL("../../../", import.meta.url));
const runtime = NodeURL.fileURLToPath(new URL("./server-test-runtime/", import.meta.url));
NodeChildProcess.execFileSync("npm", ["ci", "--ignore-scripts", "--no-audit", "--no-fund"], {
  cwd: runtime,
  stdio: "inherit",
});

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
  await NodeFSP.symlink(target, link, "dir");
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
