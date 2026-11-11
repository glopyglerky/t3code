#!/usr/bin/env node
/* oxlint-disable t3code/no-global-process-runtime -- Standalone platform acceptance verifies the actual host and launches controlled fixture processes outside the application's Effect runtime. */
import * as NodeAssert from "node:assert/strict";
import * as NodeChildProcess from "node:child_process";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";
import { nativeFile, nativeHash, nativeTargets, nativeArtifactFiles } from "./native-artifacts.mjs";
import { databaseAcceptanceChecks } from "../packages/shared/src/nativeExecutable.ts";

const root = NodeURL.fileURLToPath(new URL("../", import.meta.url));
const [directoryArgument, proofArgument] = process.argv.slice(2);
NodeAssert.ok(
  directoryArgument && NodePath.isAbsolute(directoryArgument),
  "Pass an absolute candidate bundle directory",
);
NodeAssert.ok(
  proofArgument && NodePath.isAbsolute(proofArgument),
  "Pass an absolute evidence directory",
);
const directory = NodeFS.realpathSync(directoryArgument);
NodeFS.mkdirSync(proofArgument, { recursive: true });
const proof = NodeFS.realpathSync(proofArgument);
process.env.T3_CODE_LS_TEST_TMPDIR = proof;
for (const path of [directory, proof]) {
  const relative = NodePath.relative(NodeFS.realpathSync(root), path);
  NodeAssert.ok(
    relative.startsWith(`..${NodePath.sep}`) || NodePath.isAbsolute(relative),
    "Candidate output must stay outside source",
  );
}
const target = `${NodeOS.platform()}-${NodeOS.arch()}`;
const manifestPath = NodePath.join(directory, "manifest.json");
const manifest = JSON.parse(NodeFS.readFileSync(manifestPath, "utf8"));
delete manifest.packageAcceptance;
NodeAssert.equal(manifest.implementation, "database-only");
NodeAssert.equal(manifest.target, target, "Acceptance must execute on the actual target host");
NodeAssert.equal(manifest.rustTarget, nativeTargets[target]);
NodeAssert.equal(manifest.compiler.route, "rustc");
const executable = NodePath.join(directory, nativeFile("db", target));
const digest = nativeHash(executable);
NodeAssert.equal(manifest.files[NodePath.basename(executable)], digest);
// Every source-level adapter import in this isolated acceptance process binds
// the candidate. Installed-package selection is exercised separately with no override.
process.env.T3_CODE_LS_DB_EXECUTABLE = executable;
const git = (...args) =>
  NodeChildProcess.execFileSync("git", args, { cwd: root, encoding: "utf8" }).trim();
const sourceCommit = git("rev-parse", "HEAD");
NodeAssert.equal(
  manifest.sourceCommit,
  sourceCommit,
  "Candidate was built from another application revision",
);
const initialChanges = git("status", "--porcelain", "--untracked-files=normal");
NodeAssert.equal(
  initialChanges,
  "",
  "Platform acceptance requires committed, clean application source",
);
const upstreamCommit = JSON.parse(
  NodeFS.readFileSync(NodePath.join(root, "upstream.lock.json"), "utf8"),
).upstream.commit;
NodeAssert.equal(manifest.upstreamCommit, upstreamCommit);
const report = {
  schema: 1,
  status: "running",
  target,
  sourceCommit,
  upstreamCommit,
  executableSha256: digest,
  compiler: manifest.compiler,
  platform: {
    system: NodeOS.platform(),
    architecture: NodeOS.arch(),
    release: NodeOS.release(),
    node: process.version,
  },
  checks: [],
};
const reportPath = NodePath.join(proof, "database-platform.json");
const save = () => NodeFS.writeFileSync(reportPath, JSON.stringify(report, null, 2) + "\n");
function run(name, program, args) {
  console.log(`Checking ${name} on ${target}`);
  const check = { name, status: "running", command: [NodePath.basename(program), ...args] };
  report.checks.push(check);
  save();
  const result = NodeChildProcess.spawnSync(program, args, {
    cwd: root,
    env: process.env,
    stdio: "inherit",
    timeout: 1_200_000,
  });
  check.status = result.status === 0 && !result.error ? "passed" : "failed";
  check.exitCode = result.status;
  save();
  if (result.error) throw result.error;
  NodeAssert.equal(result.status, 0, `${name} failed on ${target}`);
}
try {
  const channel = NodeFS.readFileSync(NodePath.join(root, "rust-toolchain.toml"), "utf8").match(
    /^channel\s*=\s*"(\d+\.\d+\.\d+)"\s*$/m,
  )?.[1];
  NodeAssert.equal(
    manifest.compiler.toolchain,
    channel,
    "Candidate compiler differs from the application pin",
  );
  const compilerPath = NodeChildProcess.execFileSync(
    "rustup",
    ["which", "--toolchain", channel, "rustc"],
    { cwd: root, encoding: "utf8" },
  ).trim();
  NodeAssert.equal(
    nativeHash(compilerPath),
    manifest.compiler.executableSha256,
    "Acceptance compiler differs from the producing compiler",
  );
  run("storage-tests", "rustup", [
    "run",
    manifest.compiler.toolchain,
    "cargo",
    "test",
    "--locked",
    "--target",
    manifest.rustTarget,
    "-p",
    "t3-code-ls-storage",
    "--all-targets",
  ]);
  for (const [name, script] of [
    ["startup", "check-omdb-startup.mjs"],
    ["production", "check-omdb-production.mjs"],
    ["transactions", "check-redb-transactions.mjs"],
    ["restart-and-ownership", "check-redb-cli.mjs"],
  ]) {
    run(name, process.execPath, [
      NodePath.join(root, "tools", script),
      executable,
      NodePath.join(proof, `${name}.json`),
    ]);
  }
  NodeAssert.deepEqual(
    report.checks.map((check) => check.name),
    databaseAcceptanceChecks,
  );
  NodeAssert.equal(git("rev-parse", "HEAD"), sourceCommit);
  NodeAssert.equal(git("status", "--porcelain", "--untracked-files=normal"), initialChanges);
  NodeAssert.equal(nativeHash(executable), digest, "Candidate binary changed during acceptance");
  manifest.databaseAcceptance = {
    status: "passed",
    target,
    sourceCommit,
    upstreamCommit,
    executableSha256: digest,
    checks: report.checks.map((check) => check.name),
    ...(NodeOS.platform() === "linux"
      ? { glibcMinimum: process.report.getReport().header.glibcVersionRuntime }
      : {}),
  };
  NodeFS.writeFileSync(manifestPath, JSON.stringify(manifest, null, 2) + "\n");
  nativeArtifactFiles(directory, target, { lifecycleOnly: true });
  report.status = "passed";
  report.manifestSha256 = nativeHash(manifestPath);
  save();
  console.log(`Accepted database-only bundle for ${target}; unrelated helpers remain upstream.`);
} catch (error) {
  manifest.databaseAcceptance = { status: "candidate" };
  NodeFS.writeFileSync(manifestPath, JSON.stringify(manifest, null, 2) + "\n");
  report.status = "failed";
  report.failure = String(error);
  save();
  throw error;
}
