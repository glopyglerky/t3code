#!/usr/bin/env node
/* oxlint-disable t3code/no-global-process-runtime -- Standalone compiler qualification runs before an Effect runtime exists. */
import * as NodeAssert from "node:assert/strict";
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeChildProcess from "node:child_process";
import * as NodeURL from "node:url";
import { nativeComponents, nativeFile, nativeHash, nativeTargets } from "./native-artifacts.mjs";
import { applicationChecks, parseHybridArguments, planAdapterChecks } from "./hybrid-checks.mjs";

const root = NodeURL.fileURLToPath(new URL("../", import.meta.url));
const { mode, proofDirectory, selectedPorts } = parseHybridArguments(process.argv.slice(2));
const adapterChecks = mode === "--adapters" ? planAdapterChecks(selectedPorts) : [];

const bin = NodePath.join(root, "bin");
const nativeManifest = JSON.parse(NodeFS.readFileSync(NodePath.join(bin, "manifest.json"), "utf8"));
const target = `${NodeOS.platform()}-${NodeOS.arch()}`;
NodeAssert.ok(
  Object.hasOwn(nativeTargets, target),
  `Unsupported native artifact target: ${target}`,
);
NodeAssert.equal(nativeManifest.target, target, "Native artifact host does not match this runner");
NodeAssert.equal(
  nativeManifest.rustTarget,
  nativeTargets[target],
  "Native artifact target mismatch",
);
for (const component of nativeComponents) {
  const name = nativeFile(component, target);
  const file = NodePath.join(bin, name);
  NodeAssert.ok(NodeFS.lstatSync(file).isFile(), `Expected a regular native executable: ${name}`);
  NodeAssert.equal(nativeHash(file), nativeManifest.files[name], `Native hash mismatch: ${name}`);
  // Actions artifact downloads preserve content but reset executable permissions.
  if (!target.startsWith("win32-")) NodeFS.chmodSync(file, 0o755);
  NodeFS.accessSync(
    file,
    target.startsWith("win32-") ? NodeFS.constants.R_OK : NodeFS.constants.X_OK,
  );
}

if (mode === "--verify-binaries") {
  console.log(`Verified ${nativeComponents.length} native executables for ${target}`);
} else {
  const requested = proofDirectory ?? process.env.T3_CODE_LS_HYBRID_PROOF_DIR;
  NodeAssert.ok(
    requested && NodePath.isAbsolute(requested),
    "Set an absolute proof directory outside the checkout",
  );
  NodeFS.mkdirSync(requested, { recursive: true });
  const proof = NodeFS.realpathSync(requested);
  const canonicalRoot = NodeFS.realpathSync(root);
  NodeAssert.ok(
    proof !== canonicalRoot && !proof.startsWith(`${canonicalRoot}${NodePath.sep}`),
    "Qualification output must be outside the source checkout",
  );
  const digest = (file) =>
    NodeCrypto.createHash("sha256").update(NodeFS.readFileSync(file)).digest("hex");
  const report = {
    schema_version: 1,
    source_commit: NodeChildProcess.execFileSync("git", ["rev-parse", "HEAD"], {
      cwd: root,
      encoding: "utf8",
    }).trim(),
    source_tree_dirty:
      NodeChildProcess.execFileSync("git", ["status", "--porcelain", "--untracked-files=normal"], {
        cwd: root,
        encoding: "utf8",
      }).trim().length !== 0,
    upstream_commit: JSON.parse(
      NodeFS.readFileSync(NodePath.join(root, "upstream.lock.json"), "utf8"),
    ).upstream.commit,
    target,
    compiler_route: nativeManifest.compiler?.route ?? "unrecorded",
    compiler: nativeManifest.compiler ?? null,
    component_compilers: nativeManifest.componentCompilers ?? null,
    baseline_selection:
      mode === "--adapters"
        ? "Trusted upstream behavior is used as the differential oracle for the selected Rust ports"
        : "Native service bundle through the retained application build",
    toolchain_lock_sha256: digest(NodePath.join(root, "toolchain.lock.json")),
    native_manifest_sha256: digest(NodePath.join(bin, "manifest.json")),
    mode,
    selected_ports: selectedPorts,
    status: "running",
    checks: [],
    scope:
      mode === "--adapters"
        ? "Selected fork adapter typechecks, Rust parity and failure paths with controlled provider fixtures and the upstream SQLite event sink"
        : "Fork native-executable selection tests and the client/server integration builds; pristine upstream tests are trusted",
    remaining_acceptance: [
      "A complete interactive client project/thread/provider/approval/reconnect flow",
      "Native desktop and mobile lifecycle and distribution",
      "macOS and Windows execution",
      "omdb lifecycle and release acceptance on unqualified platforms; Iroh production cutover",
      "Native Lust compilation of this source revision",
    ],
  };
  const reportPath = NodePath.join(
    proof,
    mode === "--adapters" ? "adapters.json" : "application.json",
  );
  const save = () => NodeFS.writeFileSync(reportPath, JSON.stringify(report, null, 2) + "\n");
  const env = {
    ...process.env,
    T3CODE_DEV_SEED: "0",
    VP_REMOTE_CACHE: "off",
    VP_REMOTE_CACHE_URL: "",
    T3_CODE_LS_NATIVE_DIR: bin,
    T3_CODE_LS_TEST_TMPDIR: proof,
  };
  function run(name, program, args) {
    console.log(`\n${name}`);
    const check = { name, program: NodePath.basename(program), arguments: args, status: "running" };
    report.checks.push(check);
    save();
    const result = NodeChildProcess.spawnSync(program, args, { cwd: root, env, stdio: "inherit" });
    check.status = result.status === 0 && !result.error ? "passed" : "failed";
    check.exit_code = result.status;
    if (result.signal) check.signal = result.signal;
    save();
    if (result.error) throw result.error;
    NodeAssert.equal(result.status, 0, `${name} failed`);
  }
  const node = (name, script, args = []) =>
    run(name, process.execPath, ["--disable-warning=ExperimentalWarning", script, ...args]);
  try {
    save();
    if (mode === "--adapters") {
      const typechecker = NodePath.resolve(
        process.env.T3_CODE_LS_TEST_RUNTIME ?? NodePath.join(root, "tools", "server-test-runtime"),
        "node_modules",
        ".bin",
        "effect-tsgo",
      );
      for (const check of adapterChecks.filter((check) => check.typecheckProject)) {
        run(`Typecheck ${check.port} adapter`, typechecker, [
          "tsrs",
          "--noEmit",
          "--project",
          check.typecheckProject,
        ]);
      }
      for (const check of adapterChecks) {
        node(check.name, check.script, [
          NodePath.join(bin, nativeFile(check.component, target)),
          ...check.extraArguments,
          NodePath.join(proof, check.proofFile),
        ]);
      }
    } else {
      for (const check of applicationChecks()) {
        run(check.name, check.program, check.arguments);
      }
    }
    report.status = "passed";
    save();
    console.log(`Hybrid qualification evidence: ${reportPath}`);
  } catch (error) {
    report.status = "failed";
    report.failure = String(error);
    save();
    throw error;
  }
}
