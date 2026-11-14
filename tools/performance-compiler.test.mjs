import * as NodeAssert from "node:assert/strict";
import * as NodeTest from "node:test";
import { compilerProvenanceMarkdown, resolveCompilerProvenance } from "./performance-compiler.mjs";

const expectedCompilerIdentity = {
  compiler_revision: "a".repeat(40),
  coordinator_revision: "b".repeat(40),
};
const current = {
  executable_sha256: "c".repeat(64),
  source_commit: "d".repeat(40),
  source_tree: "e".repeat(40),
  source_tree_dirty: false,
  toolchain_lock_sha256: "f".repeat(64),
  ci_run_id: "12345",
  ci_run_attempt: "2",
};
const manifest = {
  files: { "t3-code-ls-db": current.executable_sha256 },
  source_commit: current.source_commit,
  source_tree: current.source_tree,
  source_tree_dirty: false,
  toolchain_lock_sha256: current.toolchain_lock_sha256,
  ci_run_id: current.ci_run_id,
  ci_run_attempt: "1",
  compiler_route: "bluefe-rustc-via-lustr",
  build_profile: "release",
};
const input = {
  expectedCompilerIdentity,
  manifest,
  executableName: "t3-code-ls-db",
  current,
};

NodeTest.test("a diagnostic lock pin never becomes observed compiler provenance", () => {
  const result = resolveCompilerProvenance({ expectedCompilerIdentity });
  NodeAssert.equal(result.compiler_provenance, "unverified");
  NodeAssert.equal(result.compiler_route, null);
  NodeAssert.equal(result.compiler_identity, null);
  NodeAssert.equal(result.build_profile, "unverified-diagnostic");
  NodeAssert.deepEqual(result.expected_compiler_identity, expectedCompilerIdentity);
  const markdown = compilerProvenanceMarkdown(result).join("\n");
  NodeAssert.match(markdown, /Manifest-reported route: not established/);
  NodeAssert.match(markdown, /Expected compiler revision:/);
  NodeAssert.match(markdown, /No executable-bound build manifest was supplied/);
});

NodeTest.test(
  "a hash-bound diagnostic manifest reports claims without source qualification",
  () => {
    const result = resolveCompilerProvenance({
      ...input,
      manifest: {
        ...manifest,
        compiler_route: "direct-diagnostic-driver",
        build_profile: "debug",
        source_commit: "0".repeat(40),
        ci_run_id: "other-run",
      },
    });
    NodeAssert.equal(result.compiler_provenance, "manifest-reported");
    NodeAssert.equal(result.compiler_route, "direct-diagnostic-driver");
    NodeAssert.equal(result.build_profile, "debug");
    NodeAssert.equal(result.compiler_identity, null);
    NodeAssert.match(compilerProvenanceMarkdown(result).join("\n"), /not qualification/);
  },
);

NodeTest.test(
  "an executable hash mismatch cannot produce compiler provenance in either mode",
  () => {
    for (const mode of ["diagnostic", "qualify"])
      NodeAssert.throws(() =>
        resolveCompilerProvenance({
          ...input,
          mode,
          manifest: { ...manifest, files: { "t3-code-ls-db": "0".repeat(64) } },
        }),
      );
  },
);

NodeTest.test("qualification requires a supplied build manifest", () => {
  NodeAssert.throws(() => resolveCompilerProvenance({ expectedCompilerIdentity, mode: "qualify" }));
});

NodeTest.test(
  "a verified same-run release manifest does not invent an unreported driver identity",
  () => {
    const result = resolveCompilerProvenance({ ...input, mode: "qualify" });
    NodeAssert.equal(result.compiler_provenance, "verified-ci-manifest");
    NodeAssert.equal(result.compiler_route, "bluefe-rustc-via-lustr");
    NodeAssert.equal(result.compiler_identity, null);
    NodeAssert.equal(result.build_profile, "release");
    NodeAssert.deepEqual(result.expected_compiler_identity, expectedCompilerIdentity);
    NodeAssert.notEqual(manifest.ci_run_attempt, current.ci_run_attempt);
    NodeAssert.match(
      compilerProvenanceMarkdown(result).join("\n"),
      /not independently identify a compiler/,
    );
  },
);

for (const [name, changes] of [
  ["source commit", { source_commit: "0".repeat(40) }],
  ["source tree", { source_tree: "0".repeat(40) }],
  ["dirty build source", { source_tree_dirty: true }],
  ["debug profile", { build_profile: "debug" }],
  ["different compiler route", { compiler_route: "direct-diagnostic-driver" }],
  ["different toolchain lock", { toolchain_lock_sha256: "0".repeat(64) }],
  ["different CI run", { ci_run_id: "other-run" }],
  ["missing build attempt", { ci_run_attempt: undefined }],
  ["future build attempt", { ci_run_attempt: "3" }],
  ["nonpositive build attempt", { ci_run_attempt: "0" }],
])
  NodeTest.test(`qualification rejects a manifest with ${name}`, () => {
    NodeAssert.throws(() =>
      resolveCompilerProvenance({
        ...input,
        mode: "qualify",
        manifest: { ...manifest, ...changes },
      }),
    );
  });

for (const [name, changes] of [
  ["dirty measured source", { source_tree_dirty: true }],
  ["missing CI run", { ci_run_id: null }],
  ["missing measurement attempt", { ci_run_attempt: null }],
  ["nondecimal CI run", { ci_run_id: "not-a-run" }],
  ["nonpositive measurement attempt", { ci_run_attempt: "0" }],
])
  NodeTest.test(`qualification rejects ${name}`, () => {
    NodeAssert.throws(() =>
      resolveCompilerProvenance({ ...input, mode: "qualify", current: { ...current, ...changes } }),
    );
  });

NodeTest.test(
  "an explicitly reported identity remains separate and must match the qualification pin",
  () => {
    const reported = { compiler_revision: "0".repeat(40) };
    const diagnostic = resolveCompilerProvenance({
      ...input,
      manifest: { ...manifest, compiler_identity: reported },
    });
    NodeAssert.deepEqual(diagnostic.compiler_identity, reported);
    NodeAssert.deepEqual(diagnostic.expected_compiler_identity, expectedCompilerIdentity);
    NodeAssert.throws(() =>
      resolveCompilerProvenance({
        ...input,
        mode: "qualify",
        manifest: { ...manifest, compiler_identity: reported },
      }),
    );
    const qualified = resolveCompilerProvenance({
      ...input,
      mode: "qualify",
      manifest: { ...manifest, compiler_identity: expectedCompilerIdentity },
    });
    NodeAssert.deepEqual(qualified.compiler_identity, expectedCompilerIdentity);
  },
);
