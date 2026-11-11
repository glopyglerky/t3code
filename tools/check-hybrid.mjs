#!/usr/bin/env node
import * as Assert from "node:assert/strict";
import * as Crypto from "node:crypto";
import * as FS from "node:fs";
import * as OS from "node:os";
import * as Path from "node:path";
import * as ChildProcess from "node:child_process";
import * as NodeURL from "node:url";
import { nativeComponents, nativeFile, nativeHash, nativeTargets } from "./native-artifacts.mjs";
import { applicationChecks, parseHybridArguments, planAdapterChecks } from "./hybrid-checks.mjs";

const root = NodeURL.fileURLToPath(new URL("../", import.meta.url));
const { mode, proofDirectory, selectedPorts } = parseHybridArguments(process.argv.slice(2));
const adapterChecks = mode === "--adapters" ? planAdapterChecks(selectedPorts) : [];

const bin = Path.join(root, "bin");
const nativeManifest = JSON.parse(FS.readFileSync(Path.join(bin, "manifest.json"), "utf8"));
const target = `${OS.platform()}-${OS.arch()}`;
Assert.ok(Object.hasOwn(nativeTargets, target), `Unsupported native artifact target: ${target}`);
Assert.equal(nativeManifest.target, target, "Native artifact host does not match this runner");
Assert.equal(nativeManifest.rustTarget, nativeTargets[target], "Native artifact target mismatch");
for (const component of nativeComponents) {
  const name = nativeFile(component, target);
  const file = Path.join(bin, name);
  Assert.ok(FS.lstatSync(file).isFile(), `Expected a regular native executable: ${name}`);
  Assert.equal(nativeHash(file), nativeManifest.files[name], `Native hash mismatch: ${name}`);
  // Actions artifact downloads preserve content but reset executable permissions.
  if (!target.startsWith("win32-")) FS.chmodSync(file, 0o755);
  FS.accessSync(file, target.startsWith("win32-") ? FS.constants.R_OK : FS.constants.X_OK);
}

if (mode === "--verify-binaries") {
  console.log(`Verified ${nativeComponents.length} native executables for ${target}`);
} else {
  const requested = proofDirectory ?? process.env.T3_CODE_LS_HYBRID_PROOF_DIR;
  Assert.ok(
    requested && Path.isAbsolute(requested),
    "Set an absolute proof directory outside the checkout",
  );
  FS.mkdirSync(requested, { recursive: true });
  const proof = FS.realpathSync(requested);
  const canonicalRoot = FS.realpathSync(root);
  Assert.ok(
    proof !== canonicalRoot && !proof.startsWith(`${canonicalRoot}${Path.sep}`),
    "Qualification output must be outside the source checkout",
  );
  const digest = (file) => Crypto.createHash("sha256").update(FS.readFileSync(file)).digest("hex");
  const report = {
    schema_version: 1,
    source_commit: ChildProcess.execFileSync("git", ["rev-parse", "HEAD"], {
      cwd: root,
      encoding: "utf8",
    }).trim(),
    source_tree_dirty:
      ChildProcess.execFileSync("git", ["status", "--porcelain", "--untracked-files=normal"], {
        cwd: root,
        encoding: "utf8",
      }).trim().length !== 0,
    upstream_commit: JSON.parse(FS.readFileSync(Path.join(root, "upstream.lock.json"), "utf8"))
      .upstream.commit,
    target,
    compiler_route: "bluefe-rustc-via-lustr",
    baseline_selection:
      mode === "--adapters"
        ? "Trusted upstream behavior is used as the differential oracle for the selected Rust ports"
        : "Native service bundle through the retained application build",
    toolchain_lock_sha256: digest(Path.join(root, "toolchain.lock.json")),
    native_manifest_sha256: digest(Path.join(bin, "manifest.json")),
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
  const reportPath = Path.join(proof, mode === "--adapters" ? "adapters.json" : "application.json");
  const save = () => FS.writeFileSync(reportPath, JSON.stringify(report, null, 2) + "\n");
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
    const check = { name, program: Path.basename(program), arguments: args, status: "running" };
    report.checks.push(check);
    save();
    const result = ChildProcess.spawnSync(program, args, { cwd: root, env, stdio: "inherit" });
    check.status = result.status === 0 && !result.error ? "passed" : "failed";
    check.exit_code = result.status;
    if (result.signal) check.signal = result.signal;
    save();
    if (result.error) throw result.error;
    Assert.equal(result.status, 0, `${name} failed`);
  }
  const node = (name, script, args = []) =>
    run(name, process.execPath, ["--disable-warning=ExperimentalWarning", script, ...args]);
  try {
    save();
    if (mode === "--adapters") {
      const typechecker = Path.resolve(
        process.env.T3_CODE_LS_TEST_RUNTIME ?? Path.join(root, "tools", "server-test-runtime"),
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
          Path.join(bin, nativeFile(check.component, target)),
          ...check.extraArguments,
          Path.join(proof, check.proofFile),
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
