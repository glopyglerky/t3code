/* oxlint-disable t3code/no-global-process-runtime -- The dependency installer runs before the application's Effect runtime exists and must select host-specific directory link behavior. */
import * as NodeFSP from "node:fs/promises";
import * as NodeModule from "node:module";
import * as NodePath from "node:path";

const inside = (directory, candidate) => {
  const relative = NodePath.relative(directory, candidate);
  return (
    relative === "" ||
    (!relative.startsWith(`..${NodePath.sep}`) &&
      relative !== ".." &&
      !NodePath.isAbsolute(relative))
  );
};
const readJson = async (file) => JSON.parse(await NodeFSP.readFile(file, "utf8"));

/** Reuse the workspace singleton by changing only this freshly installed npm runtime. */
export async function alignRuntimeEffect({ repository, runtime, workspacePatterns }) {
  const repositoryRoot = await NodeFSP.realpath(repository);
  const runtimeRoot = await NodeFSP.realpath(runtime);
  const modules = NodePath.join(runtimeRoot, "node_modules");
  if ((await NodeFSP.realpath(modules)) !== modules)
    throw new Error("The scoped runtime must own its node_modules directory");
  const installed = NodePath.join(modules, "effect");
  const installedDirectory = await NodeFSP.realpath(installed);
  const runtimeManifest = await readJson(NodePath.join(runtimeRoot, "package.json"));
  const expectedVersion = runtimeManifest.dependencies?.effect;
  if (typeof expectedVersion !== "string")
    throw new Error("The scoped runtime must pin Effect exactly");
  const checkVersion = async (directory) => {
    const manifest = await readJson(NodePath.join(directory, "package.json"));
    if (manifest.name !== "effect" || manifest.version !== expectedVersion)
      throw new Error(
        `Effect version mismatch: expected ${expectedVersion}, found ${manifest.version} at ${directory}; install the frozen workspace dependencies`,
      );
  };
  await checkVersion(installedDirectory);
  if (
    !Array.isArray(workspacePatterns) ||
    workspacePatterns.some(
      (pattern) =>
        typeof pattern !== "string" ||
        NodePath.isAbsolute(pattern.replace(/^!/, "")) ||
        pattern.split(/[\\/]/).includes(".."),
    )
  )
    throw new Error("Workspace package patterns must stay within the repository");

  const patterns = workspacePatterns
    .filter((pattern) => !pattern.startsWith("!"))
    .map((pattern) => `${pattern.replace(/\/$/, "")}/package.json`);
  const exclude = workspacePatterns
    .filter((pattern) => pattern.startsWith("!"))
    .map((pattern) => `${pattern.slice(1).replace(/\/$/, "")}/package.json`);
  const consumers = [];
  for await (const manifest of NodeFSP.glob(patterns, { cwd: repositoryRoot, exclude }))
    consumers.push(NodePath.join(repositoryRoot, NodePath.dirname(manifest)));
  // Inspect source consumers before the root alias, which may already point at
  // this scoped runtime from an earlier integration-only installation.
  consumers.sort();
  consumers.push(repositoryRoot);
  const existing = new Map();
  for (const consumer of consumers) {
    let resolved;
    try {
      resolved = NodeModule.createRequire(NodePath.join(consumer, "package.json")).resolve(
        "effect/package.json",
      );
    } catch (error) {
      if (error.code === "MODULE_NOT_FOUND") continue;
      throw error;
    }
    const directory = NodePath.dirname(await NodeFSP.realpath(resolved));
    await checkVersion(directory);
    if (!inside(modules, directory)) existing.set(directory, true);
  }
  if (!inside(modules, installedDirectory)) existing.set(installedDirectory, true);
  if (existing.size > 1)
    throw new Error(
      `The workspace has multiple existing Effect identities: ${[...existing.keys()].join(", ")}; restore a consistent frozen workspace install before adding the scoped runtime`,
    );
  const [workspace] = existing.keys();
  if (!workspace)
    return { mode: "isolated", version: expectedVersion, directory: installedDirectory };
  if (workspace === installedDirectory)
    return { mode: "workspace", version: expectedVersion, directory: workspace };
  const info = await NodeFSP.lstat(installed);
  if (!info.isDirectory() || info.isSymbolicLink() || installedDirectory !== installed)
    throw new Error("Only the freshly installed scoped runtime Effect directory may be replaced");

  const rollbackDirectory = await NodeFSP.mkdtemp(NodePath.join(modules, ".t3-effect-rollback-"));
  const rollback = NodePath.join(rollbackDirectory, "effect");
  await NodeFSP.rename(installed, rollback);
  try {
    await NodeFSP.symlink(workspace, installed, process.platform === "win32" ? "junction" : "dir");
  } catch (error) {
    try {
      await NodeFSP.lstat(installed);
    } catch (missing) {
      if (missing.code === "ENOENT") await NodeFSP.rename(rollback, installed);
      else throw missing;
    }
    throw error;
  }
  if ((await NodeFSP.realpath(installed)) !== workspace)
    throw new Error(
      `Scoped Effect link did not resolve to the workspace; original retained at ${rollback}`,
    );
  return { mode: "workspace", version: expectedVersion, directory: workspace, rollback };
}
