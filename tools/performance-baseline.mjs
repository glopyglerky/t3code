import * as NodeAssert from "node:assert/strict";
import * as NodeChildProcess from "node:child_process";
import * as NodeCrypto from "node:crypto";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import {
  inspectInstalledRuntime,
  parseLockDocuments,
  withinDirectory,
} from "./performance-runtime.mjs";

const digest = (bytes) => NodeCrypto.createHash("sha256").update(bytes).digest("hex");

/** Export the pinned tree and preserve each production workspace's dependency resolution. */
export async function createPinnedBaseline(
  root,
  commit,
  tree,
  directory,
  { qualify = false } = {},
) {
  NodeAssert.match(commit, /^[a-f0-9]{40}$/);
  NodeAssert.match(tree, /^[a-f0-9]{40}$/);
  const git = (...args) =>
    NodeChildProcess.execFileSync("git", ["--no-replace-objects", "-C", root, ...args], {
      encoding: "utf8",
      timeout: 60_000,
    });
  NodeAssert.equal(git("rev-parse", `${commit}^{tree}`).trim(), tree);
  await NodeFSP.mkdir(directory);
  const archive = `${directory}.tar`;
  git("archive", "--format=tar", "--output", archive, commit);
  const archiveSha256 = digest(await NodeFSP.readFile(archive));
  NodeChildProcess.execFileSync("tar", ["-xf", archive, "-C", directory], { timeout: 60_000 });
  await NodeFSP.unlink(archive);

  const lock = await NodeFSP.readFile(NodePath.join(directory, "pnpm-lock.yaml"));
  const forkLock = await NodeFSP.readFile(NodePath.join(root, "pnpm-lock.yaml"));
  const upstreamDocuments = parseLockDocuments(lock);
  const forkDocuments = parseLockDocuments(forkLock);
  const upstreamModel = upstreamDocuments.find((document) => document.importers?.["apps/server"]);
  const forkModel = forkDocuments.find((document) => document.importers?.["apps/server"]);
  NodeAssert.ok(upstreamModel && forkModel);
  const installedRuntime = await inspectInstalledRuntime(root, forkModel);
  const runtime = installedRuntime.provenance;
  let harnessImporter = false;
  if (!("tools/oracles" in upstreamModel.importers) && "tools/oracles" in forkModel.importers) {
    NodeAssert.deepEqual(
      forkModel.importers["tools/oracles"],
      {
        devDependencies: {
          "@t3tools/contracts": {
            specifier: "workspace:*",
            version: "link:../../packages/contracts",
          },
          "@t3tools/shared": { specifier: "workspace:*", version: "link:../../packages/shared" },
          effect: { specifier: "4.1.3", version: "4.1.3" },
        },
      },
      "Only the existing oracle harness importer may differ",
    );
    delete forkModel.importers["tools/oracles"];
    harnessImporter = true;
  }
  NodeAssert.deepEqual(
    forkDocuments,
    upstreamDocuments,
    "Pinned upstream and fork runtime dependency resolutions differ; resolve the baseline environment explicitly before comparing",
  );

  const workspaces = new Map();
  for (const group of ["packages", "apps"]) {
    for (const entry of await NodeFSP.readdir(NodePath.join(directory, group), {
      withFileTypes: true,
    })) {
      if (!entry.isDirectory()) continue;
      const importer = `${group}/${entry.name}`;
      try {
        const manifest = JSON.parse(
          await NodeFSP.readFile(NodePath.join(directory, importer, "package.json"), "utf8"),
        );
        if (manifest.name) workspaces.set(manifest.name, importer);
      } catch (error) {
        if (error.code !== "ENOENT") throw error;
      }
    }
  }
  const dependencies = [];
  const escaped = [];
  const rootModules = NodePath.join(directory, "node_modules");
  const link = async (modules, name, target) => {
    const destination = NodePath.join(modules, name);
    await NodeFSP.mkdir(NodePath.dirname(destination), { recursive: true });
    await NodeFSP.symlink(target, destination, "dir");
  };
  const linkInstalled = async (importer) => {
    const sourceModules = NodePath.join(root, importer, "node_modules");
    const modules = NodePath.join(directory, importer, "node_modules");
    await NodeFSP.mkdir(modules, { recursive: true });
    const linked = new Set();
    let entries;
    try {
      entries = await NodeFSP.readdir(sourceModules);
    } catch (error) {
      if (error.code !== "ENOENT") throw error;
      if ([".", "apps/server", "packages/shared", "packages/contracts"].includes(importer))
        runtime.issues.push(`Missing installed production workspace modules: ${importer}`);
      return linked;
    }
    for (const entry of entries) {
      if (entry.startsWith(".")) continue;
      const names = entry.startsWith("@")
        ? (await NodeFSP.readdir(NodePath.join(sourceModules, entry))).map(
            (name) => `${entry}/${name}`,
          )
        : [entry];
      for (const name of names) {
        const target = await NodeFSP.realpath(NodePath.join(sourceModules, name));
        if (workspaces.has(name)) {
          const workspace = workspaces.get(name);
          if (target !== (await NodeFSP.realpath(NodePath.join(root, workspace))))
            runtime.issues.push(
              `Workspace alias ${importer}:${name} does not resolve to its production source.`,
            );
          await link(modules, name, NodePath.join(directory, workspace));
          linked.add(name);
          continue;
        }
        const relative = NodePath.relative(root, target);
        if (
          relative.startsWith(`packages${NodePath.sep}`) ||
          relative.startsWith(`apps${NodePath.sep}`)
        )
          continue;
        if (
          installedRuntime.virtualStore === null ||
          !withinDirectory(installedRuntime.virtualStore, target)
        )
          escaped.push(`${importer}:${name}`);
        await link(modules, name, target);
        linked.add(name);
        const packageBytes = await NodeFSP.readFile(NodePath.join(target, "package.json"));
        const manifest = JSON.parse(packageBytes);
        dependencies.push({
          importer,
          name,
          version: manifest.version ?? null,
          package_json_sha256: digest(packageBytes),
          installed_path: relative.split(NodePath.sep).join("/"),
        });
      }
    }
    return linked;
  };
  const linkedRoot = await linkInstalled(".");
  for (const [name, importer] of workspaces)
    if (!linkedRoot.has(name)) await link(rootModules, name, NodePath.join(directory, importer));
  for (const importer of [...workspaces.values()].sort()) await linkInstalled(importer);

  const resolvedRuntime = [];
  for (const importer of ["apps/server", "packages/shared", "packages/contracts"]) {
    for (const [name, dependency] of Object.entries(
      upstreamModel.importers[importer].dependencies ?? {},
    )) {
      if (dependency.version.startsWith("link:")) continue;
      const installed =
        dependencies.find((entry) => entry.importer === importer && entry.name === name) ??
        dependencies.find((entry) => entry.importer === "." && entry.name === name);
      if (installed === undefined) {
        runtime.issues.push(`Missing resolved production dependency ${importer}:${name}`);
        continue;
      }
      const expectedVersion = dependency.version.split("(")[0];
      NodeAssert.equal(
        installed.version,
        expectedVersion,
        `${importer} resolves a different ${name} version from stock upstream`,
      );
      resolvedRuntime.push({
        importer,
        name,
        expected_version: expectedVersion,
        actual_version: installed.version,
        package_json_sha256: installed.package_json_sha256,
        installed_path: installed.installed_path,
      });
    }
  }
  for (const name of ["effect", "@effect/platform-node"])
    NodeAssert.ok(
      resolvedRuntime.some((entry) => entry.name === name),
      `Missing critical runtime dependency ${name}`,
    );
  dependencies.sort(
    (left, right) =>
      left.importer.localeCompare(right.importer) || left.name.localeCompare(right.name),
  );
  runtime.external_packages_within_virtual_store =
    installedRuntime.virtualStore !== null && escaped.length === 0;
  if (escaped.length > 0)
    runtime.issues.push(
      `External dependency targets escape the installed pnpm virtual store: ${escaped.sort().join(", ")}`,
    );
  runtime.external_resolution_sha256 = digest(
    JSON.stringify(
      dependencies.map(({ importer, name, version, package_json_sha256, installed_path }) => [
        importer,
        name,
        version,
        package_json_sha256,
        installed_path,
      ]),
    ),
  );
  runtime.per_workspace_resolution = true;
  runtime.issues = [...new Set(runtime.issues)].sort();
  runtime.status = runtime.issues.length === 0 ? "verified" : "diagnostic-unverified";
  if (qualify)
    NodeAssert.equal(
      runtime.status,
      "verified",
      `Unverified production runtime: ${runtime.issues.join("; ")}`,
    );

  return {
    root: directory,
    commit,
    tree,
    archive_sha256: archiveSha256,
    dependency_lock_sha256: digest(lock),
    fork_dependency_lock_sha256: digest(forkLock),
    dependency_comparison: harnessImporter
      ? "All settings, patches, package/snapshot resolutions and shared importers match. Only the exact existing tools/oracles importer with workspace links and Effect 4.1.3 is additional."
      : "Complete lock structures match.",
    workspace_packages: [...workspaces.keys()].sort(),
    external_dependencies: dependencies,
    resolved_runtime: resolvedRuntime,
    runtime_provenance: runtime,
    sqlite_wrapper_sha256: digest(
      await NodeFSP.readFile(NodePath.join(directory, "packages/shared/src/nodeSqliteClient.ts")),
    ),
    resolution:
      "Every relative and workspace import resolves inside the pinned upstream export. Each workspace retains its actual installed external dependency and peer-instance targets; qualification requires those targets inside the source-matched frozen pnpm virtual store.",
  };
}
