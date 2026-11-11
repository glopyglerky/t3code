/* oxlint-disable t3code/no-global-process-runtime -- Standalone installer fixtures select host-specific links before any application Effect runtime exists. */
import * as NodeAssert from "node:assert/strict";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeTest from "node:test";
import { alignRuntimeEffect } from "./server-test-runtime-identity.mjs";

async function fixture() {
  const directory = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "t3-runtime-identity-"));
  const repository = NodePath.join(directory, "repository");
  const runtime = NodePath.join(directory, "runtime");
  for (const root of [repository, runtime]) await NodeFSP.mkdir(root, { recursive: true });
  await NodeFSP.writeFile(
    NodePath.join(repository, "package.json"),
    JSON.stringify({ private: true }),
  );
  await NodeFSP.writeFile(
    NodePath.join(runtime, "package.json"),
    JSON.stringify({ dependencies: { effect: "4.1.2" } }),
  );
  const packageAt = async (directory, version = "4.1.2", marker = "workspace") => {
    await NodeFSP.mkdir(directory, { recursive: true });
    await NodeFSP.writeFile(
      NodePath.join(directory, "package.json"),
      JSON.stringify({ name: "effect", version }),
    );
    await NodeFSP.writeFile(NodePath.join(directory, "identity.txt"), marker);
    return directory;
  };
  const installed = await packageAt(
    NodePath.join(runtime, "node_modules/effect"),
    "4.1.2",
    "owned runtime",
  );
  const link = async (target, path) => {
    await NodeFSP.mkdir(NodePath.dirname(path), { recursive: true });
    await NodeFSP.symlink(target, path, process.platform === "win32" ? "junction" : "dir");
  };
  const consumer = async (relative, effect) => {
    const directory = NodePath.join(repository, relative);
    await NodeFSP.mkdir(directory, { recursive: true });
    await NodeFSP.writeFile(
      NodePath.join(directory, "package.json"),
      JSON.stringify({ private: true }),
    );
    if (effect) await link(effect, NodePath.join(directory, "node_modules/effect"));
    return directory;
  };
  const align = () =>
    alignRuntimeEffect({ repository, runtime, workspacePatterns: ["packages/*", "apps/*"] });
  return {
    directory,
    repository,
    runtime,
    installed,
    packageAt,
    link,
    consumer,
    align,
    close: () => NodeFSP.rm(directory, { recursive: true, force: true }),
  };
}

NodeTest.test(
  "owned runtime reuses the existing exact workspace Effect without changing pnpm links",
  async () => {
    const f = await fixture();
    try {
      const workspace = await f.packageAt(
        NodePath.join(f.repository, "node_modules/.pnpm/effect@4.1.2/node_modules/effect"),
      );
      const contracts = await f.consumer("packages/contracts", workspace);
      await f.consumer("apps/server", workspace);
      const rootAlias = NodePath.join(f.repository, "node_modules/effect");
      await f.link(f.installed, rootAlias);
      const rootLinkBefore = await NodeFSP.readlink(rootAlias);
      const contractsLinkBefore = await NodeFSP.readlink(
        NodePath.join(contracts, "node_modules/effect"),
      );
      const result = await f.align();
      NodeAssert.equal(result.mode, "workspace");
      NodeAssert.equal(result.directory, await NodeFSP.realpath(workspace));
      NodeAssert.equal(await NodeFSP.realpath(rootAlias), result.directory);
      NodeAssert.equal(await NodeFSP.realpath(f.installed), result.directory);
      NodeAssert.equal(await NodeFSP.readlink(rootAlias), rootLinkBefore);
      NodeAssert.equal(
        await NodeFSP.readlink(NodePath.join(contracts, "node_modules/effect")),
        contractsLinkBefore,
      );
      NodeAssert.equal(
        await NodeFSP.readFile(NodePath.join(result.rollback, "identity.txt"), "utf8"),
        "owned runtime",
      );
      NodeAssert.equal(
        await NodeFSP.readFile(NodePath.join(workspace, "identity.txt"), "utf8"),
        "workspace",
      );
      NodeAssert.equal((await f.align()).rollback, undefined);
    } finally {
      await f.close();
    }
  },
);

NodeTest.test("fresh isolated CI retains the newly installed Effect package", async () => {
  const f = await fixture();
  try {
    await f.consumer("packages/contracts");
    await f.consumer("apps/server");
    const result = await f.align();
    NodeAssert.equal(result.mode, "isolated");
    NodeAssert.equal(result.directory, await NodeFSP.realpath(f.installed));
    NodeAssert.equal(result.rollback, undefined);
    NodeAssert.equal((await NodeFSP.lstat(f.installed)).isSymbolicLink(), false);
  } finally {
    await f.close();
  }
});

NodeTest.test(
  "a workspace Effect version mismatch fails without altering either package",
  async () => {
    const f = await fixture();
    try {
      const workspace = await f.packageAt(
        NodePath.join(f.repository, "node_modules/.pnpm/effect@4.1.3/node_modules/effect"),
        "4.1.3",
      );
      await f.consumer("packages/contracts", workspace);
      await NodeAssert.rejects(f.align(), /Effect version mismatch/);
      NodeAssert.equal((await NodeFSP.lstat(f.installed)).isSymbolicLink(), false);
      NodeAssert.equal(
        await NodeFSP.readFile(NodePath.join(f.installed, "identity.txt"), "utf8"),
        "owned runtime",
      );
      NodeAssert.equal(
        await NodeFSP.readFile(NodePath.join(workspace, "identity.txt"), "utf8"),
        "workspace",
      );
    } finally {
      await f.close();
    }
  },
);

NodeTest.test(
  "multiple existing workspace identities are reported rather than rewriting the workspace",
  async () => {
    const f = await fixture();
    try {
      const first = await f.packageAt(NodePath.join(f.repository, "first-effect"));
      const second = await f.packageAt(NodePath.join(f.repository, "second-effect"));
      await f.consumer("packages/contracts", first);
      await f.consumer("apps/server", second);
      await NodeAssert.rejects(f.align(), /multiple existing Effect identities/);
      NodeAssert.equal((await NodeFSP.lstat(f.installed)).isSymbolicLink(), false);
      NodeAssert.equal(
        await NodeFSP.readFile(NodePath.join(first, "identity.txt"), "utf8"),
        "workspace",
      );
      NodeAssert.equal(
        await NodeFSP.readFile(NodePath.join(second, "identity.txt"), "utf8"),
        "workspace",
      );
    } finally {
      await f.close();
    }
  },
);
