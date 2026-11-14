#!/usr/bin/env node
import * as NodeAssert from "node:assert/strict";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";

const root = NodeURL.fileURLToPath(new URL("../", import.meta.url));
const externalPackages = ["effect", "@effect/platform-node", "yaml"];
const workspacePackages = [
  ["@t3tools/contracts", "packages/contracts"],
  ["@t3tools/shared", "packages/shared"],
];
const within = (parent, child) => {
  const relative = NodePath.relative(parent, child);
  return (
    relative !== "" &&
    !relative.startsWith(`..${NodePath.sep}`) &&
    relative !== ".." &&
    !NodePath.isAbsolute(relative)
  );
};

/** Make the comparison harness use the application's installed dependency graph. */
export async function linkPerformanceRuntime(directory) {
  const checkout = await NodeFSP.realpath(directory);
  const manifest = JSON.parse(
    await NodeFSP.readFile(NodePath.join(checkout, "package.json"), "utf8"),
  );
  NodeAssert.equal(manifest.packageManager, "pnpm@12.14.0");
  const modules = await NodeFSP.realpath(NodePath.join(checkout, "node_modules"));
  const virtualStore = await NodeFSP.realpath(NodePath.join(modules, ".pnpm"));
  NodeAssert.ok(
    within(checkout, modules) && within(modules, virtualStore),
    "Use the frozen workspace installation inside this checkout",
  );
  await NodeFSP.access(NodePath.join(modules, ".modules.yaml"));
  await NodeFSP.access(NodePath.join(virtualStore, "lock.yaml"));

  const links = [];
  for (const name of externalPackages) {
    const target = await NodeFSP.realpath(
      NodePath.join(checkout, "apps/server/node_modules", name),
    );
    NodeAssert.ok(within(virtualStore, target), `${name} is outside the frozen pnpm virtual store`);
    const installed = JSON.parse(
      await NodeFSP.readFile(NodePath.join(target, "package.json"), "utf8"),
    );
    NodeAssert.equal(installed.name, name);
    links.push({ name, target });
  }
  for (const [name, relative] of workspacePackages) {
    const target = await NodeFSP.realpath(NodePath.join(checkout, relative));
    NodeAssert.ok(within(checkout, target), `${name} is outside this checkout`);
    const workspace = JSON.parse(
      await NodeFSP.readFile(NodePath.join(target, "package.json"), "utf8"),
    );
    NodeAssert.equal(workspace.name, name);
    links.push({ name, target });
  }

  // Validate every existing destination before creating any links. Never replace
  // another runtime or hide a conflicting root dependency behind a new symlink.
  for (const item of links) {
    item.destination = NodePath.join(modules, item.name);
    const parent = NodePath.dirname(item.destination);
    try {
      const resolvedParent = await NodeFSP.realpath(parent);
      NodeAssert.ok(
        resolvedParent === modules || within(modules, resolvedParent),
        `Dependency scope for ${item.name} is outside node_modules`,
      );
    } catch (error) {
      if (error.code !== "ENOENT") throw error;
    }
    let existing;
    try {
      existing = await NodeFSP.lstat(item.destination);
    } catch (error) {
      if (error.code !== "ENOENT") throw error;
    }
    item.exists = existing !== undefined;
    if (!item.exists) continue;
    NodeAssert.ok(existing.isSymbolicLink(), `Refusing to replace ${item.name}`);
    NodeAssert.equal(
      await NodeFSP.realpath(item.destination),
      item.target,
      `Conflicting installed ${item.name}`,
    );
  }
  for (const item of links) {
    if (item.exists) continue;
    await NodeFSP.mkdir(NodePath.dirname(item.destination), { recursive: true });
    await NodeFSP.symlink(item.target, item.destination, "dir");
  }
  return links.map(({ name, target }) => ({ name, target }));
}

if (
  process.argv[1] &&
  NodeURL.pathToFileURL(NodePath.resolve(process.argv[1])).href === import.meta.url
) {
  const links = await linkPerformanceRuntime(root);
  console.log(
    `Linked ${links.length} comparison imports from the frozen pnpm application installation.`,
  );
}
