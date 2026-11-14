import * as NodeAssert from "node:assert/strict";

const expectedRoute = "bluefe-rustc-via-lustr";

function reportedText(value, field) {
  if (value === undefined || value === null) return null;
  NodeAssert.ok(
    typeof value === "string" && value.length > 0,
    `${field} must be a nonempty string`,
  );
  return value;
}

/** Keep configured compiler pins distinct from an executable-bound build report. */
export function resolveCompilerProvenance({
  expectedCompilerIdentity,
  mode = "diagnostic",
  manifest = null,
  executableName,
  current,
}) {
  NodeAssert.ok(mode === "diagnostic" || mode === "qualify");
  NodeAssert.ok(
    expectedCompilerIdentity !== null &&
      typeof expectedCompilerIdentity === "object" &&
      !Array.isArray(expectedCompilerIdentity),
    "The expected compiler identity must come from the pinned lock",
  );
  const result = {
    expected_compiler_route: expectedRoute,
    expected_compiler_identity: expectedCompilerIdentity,
    compiler_route: null,
    compiler_identity: null,
    compiler_provenance: "unverified",
    build_profile: "unverified-diagnostic",
  };
  if (manifest === null) {
    NodeAssert.equal(mode, "diagnostic", "Qualification requires a native build manifest");
    return result;
  }

  NodeAssert.equal(
    manifest.files?.[executableName],
    current.executable_sha256,
    "The native build manifest must bind the measured executable hash",
  );
  NodeAssert.match(current.executable_sha256, /^[a-f0-9]{64}$/);
  result.compiler_route = reportedText(manifest.compiler_route, "Manifest compiler_route");
  result.build_profile =
    reportedText(manifest.build_profile, "Manifest build_profile") ?? "unverified-diagnostic";
  if (manifest.compiler_identity !== undefined && manifest.compiler_identity !== null) {
    NodeAssert.ok(
      typeof manifest.compiler_identity === "object" && !Array.isArray(manifest.compiler_identity),
      "Manifest compiler_identity must be an explicitly reported object",
    );
    result.compiler_identity = manifest.compiler_identity;
  }
  result.compiler_provenance = "manifest-reported";
  if (mode === "diagnostic") return result;

  NodeAssert.match(current.source_commit, /^[a-f0-9]{40}$/);
  NodeAssert.match(current.source_tree, /^[a-f0-9]{40}$/);
  NodeAssert.match(current.toolchain_lock_sha256, /^[a-f0-9]{64}$/);
  NodeAssert.equal(current.source_tree_dirty, false, "Qualification requires a clean source tree");
  NodeAssert.equal(manifest.source_commit, current.source_commit);
  NodeAssert.equal(manifest.source_tree, current.source_tree);
  NodeAssert.equal(manifest.source_tree_dirty, false);
  NodeAssert.equal(manifest.build_profile, "release");
  NodeAssert.equal(manifest.compiler_route, expectedRoute);
  NodeAssert.equal(manifest.toolchain_lock_sha256, current.toolchain_lock_sha256);
  NodeAssert.match(current.ci_run_id, /^[1-9][0-9]*$/);
  NodeAssert.match(current.ci_run_attempt, /^[1-9][0-9]*$/);
  NodeAssert.match(manifest.ci_run_attempt, /^[1-9][0-9]*$/);
  NodeAssert.equal(manifest.ci_run_id, current.ci_run_id);
  NodeAssert.ok(
    BigInt(manifest.ci_run_attempt) <= BigInt(current.ci_run_attempt),
    "The native build attempt cannot be newer than the measurement attempt",
  );
  if (result.compiler_identity !== null)
    NodeAssert.deepEqual(result.compiler_identity, expectedCompilerIdentity);
  // The manifest currently reports route/profile and a lock hash, not driver
  // identity. Matching that hash must never synthesize an observed identity.
  result.compiler_provenance = "verified-ci-manifest";
  return result;
}

export function compilerProvenanceMarkdown(provenance) {
  const expected = provenance.expected_compiler_identity;
  const reported = provenance.compiler_identity;
  return [
    `Compiler provenance: **${provenance.compiler_provenance}**. Manifest-reported route: ${provenance.compiler_route ?? "not established"}. Build profile: ${provenance.build_profile}.`,
    `Expected route: ${provenance.expected_compiler_route}. Expected compiler revision: \`${expected.compiler_revision ?? "not specified"}\`. Expected coordinator: \`${expected.coordinator_revision ?? "not specified"}\`.`,
    reported === null
      ? "Manifest-reported compiler identity: not supplied. The expected lock identity does not establish which drivers built this executable."
      : `Manifest-reported compiler revision: \`${reported.compiler_revision ?? "not supplied"}\`. Manifest-reported coordinator: \`${reported.coordinator_revision ?? "not supplied"}\`.`,
    provenance.compiler_provenance === "verified-ci-manifest"
      ? "The compiler provenance label verifies the exact-source, same-run release manifest binding; it does not independently identify a compiler from the executable."
      : provenance.compiler_provenance === "manifest-reported"
        ? "Diagnostic mode binds only the supplied manifest's executable hash; its source, compiler route and build identity are reported claims, not qualification."
        : "No executable-bound build manifest was supplied; compiler provenance is unverified.",
  ];
}
