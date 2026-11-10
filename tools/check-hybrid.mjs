#!/usr/bin/env node
import * as Assert from "node:assert/strict";
import * as Crypto from "node:crypto";
import * as FS from "node:fs";
import * as OS from "node:os";
import * as Path from "node:path";
import * as ChildProcess from "node:child_process";
import * as NodeURL from "node:url";
import { nativeComponents, nativeFile, nativeHash, nativeTargets } from "./native-artifacts.mjs";

const root = NodeURL.fileURLToPath(new URL("../", import.meta.url));
const arguments_ = process.argv.slice(2);
const mode = arguments_[0];
Assert.ok(
  ["--verify-binaries", "--adapters", "--application"].includes(mode),
  "Usage: node tools/check-hybrid.mjs <--verify-binaries|--adapters|--application> [absolute-proof-directory]",
);
Assert.ok(arguments_.length <= 2, "Unexpected qualification arguments");

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
  const requested = arguments_[1] ?? process.env.T3_CODE_LS_HYBRID_PROOF_DIR;
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
    baseline_selection: "T3_CODE_LS_IMPLEMENTATION=upstream before execution",
    toolchain_lock_sha256: digest(Path.join(root, "toolchain.lock.json")),
    native_manifest_sha256: digest(Path.join(bin, "manifest.json")),
    mode,
    status: "running",
    checks: [],
    scope:
      mode === "--adapters"
        ? "Production services with controlled provider fixtures and the upstream SQLite event sink"
        : "Contracts, shared client logic, WebSocket/replay/recovery tests, server/web bundles and browser boot",
    remaining_acceptance: [
      "A complete interactive client project/thread/provider/approval/reconnect flow",
      "Native desktop and mobile lifecycle and distribution",
      "macOS and Windows execution",
      "Production redb and Iroh cutovers",
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
      for (const [component, script] of [
        ["policy", "check-policy-adapter.mjs"],
        ["project", "check-project-adapter.mjs"],
        ["recovery", "check-recovery-adapter.mjs"],
        ["guest-share", "check-guest-share-adapter.mjs"],
        ["provider", "check-provider-adapter.mjs"],
        ["maintenance", "check-maintenance-adapter.mjs"],
        ["generate", "check-generation-adapter.mjs"],
      ]) {
        node(`${component} production adapter`, `tools/${script}`, [
          Path.join(bin, nativeFile(component, target)),
          Path.join(proof, `${component}-adapter.json`),
        ]);
      }
      node("Production dispatch and goal policy", "tools/check-dispatch.mjs", [
        Path.join(bin, nativeFile("policy", target)),
        "--service",
        Path.join(proof, "dispatch.json"),
      ]);
    } else {
      for (const directory of [
        "packages/contracts",
        "packages/client-runtime",
        "apps/server",
        "apps/web",
      ]) {
        run(`Typecheck ${directory}`, "pnpm", [
          "exec",
          "effect-tsgo",
          "tsrs",
          "--noEmit",
          "--project",
          `${directory}/tsconfig.json`,
        ]);
      }
      run("WebSocket, replay, cancellation and durable recovery", "pnpm", [
        "exec",
        "vp",
        "test",
        "run",
        "--maxWorkers",
        "2",
        "apps/server/src/ws.test.ts",
        "apps/server/src/orchestration-v2/runtimeLayer.test.ts",
        "apps/server/src/orchestration-v2/FoundationPersistence.test.ts",
        "apps/server/src/orchestration-v2/RestartContinuation.test.ts",
        "apps/server/src/orchestration-v2/ProjectCommands.test.ts",
        "apps/server/src/orchestration-v2/SubagentProjection.test.ts",
        "apps/server/src/orchestration-v2/CommandPolicy.test.ts",
        "apps/server/src/orchestration-v2/BackgroundWorkStop.integration.test.ts",
        "apps/server/src/orchestration-v2/testkit/CodexReplayFixtures.integration.test.ts",
        "apps/server/src/orchestration-v2/testkit/OrchestratorReplayRecovery.integration.test.ts",
        "packages/client-runtime/src/operations/projects.test.ts",
        "packages/client-runtime/src/operations/commands.test.ts",
        "packages/client-runtime/src/state/threadWorkflows.test.ts",
        "packages/client-runtime/src/state/threads-sync.test.ts",
        "packages/client-runtime/src/connection/supervisor.test.ts",
        "packages/shared/src/nativeExecutable.test.ts",
      ]);
      run("Build the original web client", "pnpm", ["--dir", "apps/web", "run", "build"]);
      run("Build the original server with native adapters", "pnpm", [
        "--dir",
        "apps/server",
        "run",
        "build:bundle",
      ]);
      node("Boot the built web client in Chromium", "apps/web/scripts/verify-built-boot.ts");
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
