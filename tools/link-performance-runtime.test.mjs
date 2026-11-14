import * as NodeAssert from "node:assert/strict";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeTest from "node:test";
import { linkPerformanceRuntime } from "./link-performance-runtime.mjs";

async function fixture(test) {
  const root = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "t3-performance-runtime-"));
  test.after(() => NodeFSP.rm(root, { recursive: true, force: true }));
  await NodeFSP.mkdir(NodePath.join(root, "node_modules/.pnpm"), { recursive: true });
  await NodeFSP.writeFile(
    NodePath.join(root, "package.json"),
    JSON.stringify({ packageManager: "pnpm@12.14.0" }),
  );
  await NodeFSP.writeFile(
    NodePath.join(root, "node_modules/.modules.yaml"),
    "packageManager: pnpm@12.14.0\n",
  );
  await NodeFSP.writeFile(
    NodePath.join(root, "node_modules/.pnpm/lock.yaml"),
    "lockfileVersion: '9.0'\n",
  );
  for (const name of ["effect", "@effect/platform-node", "yaml"]) {
    const target = NodePath.join(
      root,
      "node_modules/.pnpm",
      `${name.replaceAll("/", "+")}@locked`,
      "node_modules",
      name,
    );
    const destination = NodePath.join(root, "apps/server/node_modules", name);
    await NodeFSP.mkdir(target, { recursive: true });
    await NodeFSP.writeFile(
      NodePath.join(target, "package.json"),
      JSON.stringify({ name, version: "1.0.0" }),
    );
    await NodeFSP.mkdir(NodePath.dirname(destination), { recursive: true });
    await NodeFSP.symlink(target, destination, "dir");
  }
  for (const name of ["contracts", "shared"]) {
    const directory = NodePath.join(root, "packages", name);
    await NodeFSP.mkdir(directory, { recursive: true });
    await NodeFSP.writeFile(
      NodePath.join(directory, "package.json"),
      JSON.stringify({ name: `@t3tools/${name}` }),
    );
  }
  return root;
}

NodeTest.test(
  "comparison imports reuse the application's exact package instances and can be linked twice",
  async (test) => {
    const root = await fixture(test);
    const links = await linkPerformanceRuntime(root);
    NodeAssert.equal(links.length, 5);
    NodeAssert.deepEqual(await linkPerformanceRuntime(root), links);
    for (const { name, target } of links) {
      NodeAssert.equal(await NodeFSP.realpath(NodePath.join(root, "node_modules", name)), target);
    }
    NodeAssert.equal(
      await NodeFSP.realpath(NodePath.join(root, "node_modules/effect")),
      await NodeFSP.realpath(NodePath.join(root, "apps/server/node_modules/effect")),
    );
  },
);

NodeTest.test(
  "external runtimes and conflicting root packages are rejected without replacing or partially linking dependencies",
  async (test) => {
    const root = await fixture(test);
    const outside = NodePath.join(root, "unqualified-runtime");
    await NodeFSP.mkdir(outside);
    const source = NodePath.join(root, "apps/server/node_modules/yaml");
    const original = await NodeFSP.readlink(source);
    await NodeFSP.unlink(source);
    await NodeFSP.symlink(outside, source, "dir");
    await NodeAssert.rejects(linkPerformanceRuntime(root), /outside the frozen pnpm virtual store/);
    await NodeAssert.rejects(NodeFSP.lstat(NodePath.join(root, "node_modules/effect")), {
      code: "ENOENT",
    });
    await NodeFSP.unlink(source);
    await NodeFSP.symlink(original, source, "dir");
    const conflict = NodePath.join(root, "node_modules/yaml");
    await NodeFSP.symlink(outside, conflict, "dir");
    await NodeAssert.rejects(linkPerformanceRuntime(root), /Conflicting installed yaml/);
    NodeAssert.equal(await NodeFSP.realpath(conflict), outside);
    await NodeAssert.rejects(NodeFSP.lstat(NodePath.join(root, "node_modules/effect")), {
      code: "ENOENT",
    });
    await NodeFSP.unlink(conflict);
    await NodeFSP.mkdir(conflict);
    await NodeAssert.rejects(linkPerformanceRuntime(root), /Refusing to replace yaml/);
    NodeAssert.ok((await NodeFSP.stat(conflict)).isDirectory());
  },
);

NodeTest.test(
  "scoped link creation cannot escape the installed node_modules directory",
  async (test) => {
    const root = await fixture(test);
    const outside = NodePath.join(root, "other-runtime");
    await NodeFSP.mkdir(outside);
    await NodeFSP.symlink(outside, NodePath.join(root, "node_modules/@effect"), "dir");
    await NodeAssert.rejects(linkPerformanceRuntime(root), /outside node_modules/);
    NodeAssert.deepEqual(await NodeFSP.readdir(outside), []);
    await NodeAssert.rejects(NodeFSP.lstat(NodePath.join(root, "node_modules/effect")), {
      code: "ENOENT",
    });
  },
);
