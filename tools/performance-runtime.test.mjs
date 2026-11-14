import * as NodeAssert from "node:assert/strict";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeProcess from "node:process";
import * as NodeTest from "node:test";
import {
  inspectInstalledRuntime,
  parseLockDocuments,
  withinDirectory,
} from "./performance-runtime.mjs";

const application = {
  lockfileVersion: "9.0",
  importers: { "apps/server": { dependencies: { effect: { version: "4.1.3" } } } },
  packages: { "transitive@1.0.0": { resolution: { integrity: "locked-input" } } },
};

async function fixture(use, installed = application) {
  const root = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "t3-performance-runtime-"));
  try {
    await NodeFSP.mkdir(NodePath.join(root, "node_modules/.pnpm"), { recursive: true });
    await NodeFSP.writeFile(
      NodePath.join(root, "node_modules/.pnpm/lock.yaml"),
      JSON.stringify(installed),
    );
    await NodeFSP.writeFile(
      NodePath.join(root, "node_modules/.modules.yaml"),
      JSON.stringify({
        packageManager: "pnpm@12.14.0",
        nodeLinker: "isolated",
        virtualStoreDir: ".pnpm",
      }),
    );
    await NodeFSP.writeFile(
      NodePath.join(root, "package.json"),
      JSON.stringify({
        packageManager: "pnpm@12.14.0",
        engines: { node: NodeProcess.version.slice(1) },
      }),
    );
    await use(root);
  } finally {
    await NodeFSP.rm(root, { recursive: true, force: true });
  }
}

NodeTest.test(
  "installed application lock attests the transitive snapshot, not only direct versions",
  async () => {
    await fixture(async (root) => {
      const { provenance, virtualStore } = await inspectInstalledRuntime(root, application);
      NodeAssert.deepEqual(provenance.issues, []);
      NodeAssert.equal(provenance.installed_lock_matches_application_lock, true);
      NodeAssert.equal(provenance.package_manager, "pnpm@12.14.0");
      NodeAssert.match(provenance.node_executable_sha256, /^[a-f0-9]{64}$/);
      NodeAssert.equal(virtualStore, NodePath.join(root, "node_modules/.pnpm"));
    });
    await fixture(
      async (root) => {
        const { provenance } = await inspectInstalledRuntime(root, application);
        NodeAssert.equal(provenance.installed_lock_matches_application_lock, false);
        NodeAssert.ok(provenance.issues.some((issue) => issue.includes("transitive resolutions")));
      },
      {
        ...application,
        packages: { "transitive@2.0.0": { resolution: { integrity: "different-input" } } },
      },
    );
  },
);

NodeTest.test("missing installed metadata remains unverified", async () => {
  await fixture(async (root) => {
    await NodeFSP.unlink(NodePath.join(root, "node_modules/.modules.yaml"));
    const { provenance } = await inspectInstalledRuntime(root, application);
    NodeAssert.equal(provenance.status, "diagnostic-unverified");
    NodeAssert.ok(provenance.issues.some((issue) => issue.includes("unavailable")));
  });
});

NodeTest.test("multi-document pnpm locks and external path boundaries stay explicit", () => {
  const docs = parseLockDocuments(
    Buffer.from(
      `---\n${JSON.stringify({ toolchain: true })}\n---\n${JSON.stringify(application)}\n`,
    ),
  );
  NodeAssert.equal(docs.length, 2);
  NodeAssert.deepEqual(docs[1], application);
  NodeAssert.equal(
    withinDirectory(
      "/repo/node_modules/.pnpm",
      "/repo/node_modules/.pnpm/effect/node_modules/effect",
    ),
    true,
  );
  NodeAssert.equal(
    withinDirectory("/repo/node_modules/.pnpm", "/repo/node_modules/.pnpm-other/effect"),
    false,
  );
  NodeAssert.equal(
    withinDirectory("/repo/node_modules/.pnpm", "/test-runtime/node_modules/effect"),
    false,
  );
});
