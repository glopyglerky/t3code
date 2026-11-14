import * as NodeAssert from "node:assert/strict";
import * as NodeChildProcess from "node:child_process";
import * as NodeCrypto from "node:crypto";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as Yaml from "yaml";

const digest = (bytes) => NodeCrypto.createHash("sha256").update(bytes).digest("hex");

/** Export the whole pinned tree so stock code cannot resolve fork workspace modules. */
export async function createPinnedBaseline(root, commit, tree, directory) {
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

  const workspaces = new Map();
  for (const group of ["packages", "apps"]) {
    for (const entry of await NodeFSP.readdir(NodePath.join(directory, group), {
      withFileTypes: true,
    })) {
      if (!entry.isDirectory()) continue;
      const path = NodePath.join(directory, group, entry.name);
      try {
        const manifest = JSON.parse(
          await NodeFSP.readFile(NodePath.join(path, "package.json"), "utf8"),
        );
        if (manifest.name) workspaces.set(manifest.name, path);
      } catch (error) {
        if (error.code !== "ENOENT") throw error;
      }
    }
  }
  const modules = NodePath.join(directory, "node_modules");
  await NodeFSP.mkdir(modules);
  const sharedDependencies = [];
  const link = async (name, target) => {
    const destination = NodePath.join(modules, name);
    await NodeFSP.mkdir(NodePath.dirname(destination), { recursive: true });
    await NodeFSP.symlink(target, destination, "dir");
  };
  const sourceModules = NodePath.join(root, "node_modules");
  for (const entry of await NodeFSP.readdir(sourceModules)) {
    if (entry.startsWith(".")) continue;
    const names = entry.startsWith("@")
      ? (await NodeFSP.readdir(NodePath.join(sourceModules, entry))).map(
          (name) => `${entry}/${name}`,
        )
      : [entry];
    for (const name of names) {
      if (workspaces.has(name)) continue;
      const target = await NodeFSP.realpath(NodePath.join(sourceModules, name));
      const relative = NodePath.relative(root, target);
      if (
        relative.startsWith(`packages${NodePath.sep}`) ||
        relative.startsWith(`apps${NodePath.sep}`)
      )
        continue;
      await link(name, target);
      try {
        const packageBytes = await NodeFSP.readFile(NodePath.join(target, "package.json"));
        const manifest = JSON.parse(packageBytes);
        sharedDependencies.push({
          name,
          version: manifest.version ?? null,
          package_json_sha256: digest(packageBytes),
        });
      } catch (error) {
        if (error.code !== "ENOENT") throw error;
      }
    }
  }
  for (const [name, target] of workspaces) await link(name, target);
  // The application lock must describe the same third-party versions and patches.
  const lock = await NodeFSP.readFile(NodePath.join(directory, "pnpm-lock.yaml"));
  const forkLock = await NodeFSP.readFile(NodePath.join(root, "pnpm-lock.yaml"));
  const parseLock = (bytes) =>
    Yaml.parseAllDocuments(bytes.toString("utf8")).map((document) => {
      NodeAssert.deepEqual(document.errors, []);
      return document.toJS();
    });
  const upstreamDocuments = parseLock(lock);
  const forkDocuments = parseLock(forkLock);
  const upstreamModel = upstreamDocuments.find((document) => document.importers?.["apps/server"]);
  const forkModel = forkDocuments.find((document) => document.importers?.["apps/server"]);
  NodeAssert.ok(upstreamModel && forkModel);
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
  const resolvedRuntime = [];
  for (const importer of ["apps/server", "packages/shared"]) {
    for (const [name, dependency] of Object.entries(
      upstreamModel.importers[importer].dependencies ?? {},
    )) {
      if (dependency.version.startsWith("link:")) continue;
      const installed = sharedDependencies.find((entry) => entry.name === name);
      if (installed === undefined) continue;
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
      });
    }
  }
  for (const name of ["effect", "@effect/platform-node"])
    NodeAssert.ok(
      resolvedRuntime.some((entry) => entry.name === name),
      `Missing critical runtime dependency ${name}`,
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
    external_dependencies: sharedDependencies.sort((left, right) =>
      left.name.localeCompare(right.name),
    ),
    resolved_runtime: resolvedRuntime,
    sqlite_wrapper_sha256: digest(
      await NodeFSP.readFile(NodePath.join(directory, "packages/shared/src/nodeSqliteClient.ts")),
    ),
    resolution:
      "Every relative and workspace import resolves inside the exported pinned upstream tree. Only identical locked external dependencies are shared with the fork.",
  };
}
