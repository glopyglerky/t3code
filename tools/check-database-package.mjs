#!/usr/bin/env node
/* oxlint-disable t3code/no-global-process-runtime -- Standalone package acceptance inspects the actual host and starts owned fixture processes outside the application's Effect runtime. */
import * as NodeAssert from "node:assert/strict";
import * as NodeChildProcess from "node:child_process";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";
import { nativeArtifactFiles, nativeHash, stageNativeArtifacts } from "./native-artifacts.mjs";
import {
  databaseArtifactFiles,
  databasePackageChecks,
} from "../packages/shared/src/nativeExecutable.ts";

const repository = NodeURL.fileURLToPath(new URL("../", import.meta.url));
const git = (...args) =>
  NodeChildProcess.execFileSync("git", args, { cwd: repository, encoding: "utf8" }).trim();
const [sourceArgument, proofArgument] = process.argv.slice(2);
NodeAssert.ok(
  sourceArgument && NodePath.isAbsolute(sourceArgument),
  "Pass the accepted bundle directory",
);
NodeAssert.ok(
  proofArgument && NodePath.isAbsolute(proofArgument),
  "Pass a package execution proof path",
);
NodeAssert.equal(
  process.env.T3_CODE_LS_IMPLEMENTATION,
  undefined,
  "Package acceptance tests the default selection",
);
const source = NodeFS.realpathSync(sourceArgument);
const target = `${NodeOS.platform()}-${NodeOS.arch()}`;
nativeArtifactFiles(source, target, { lifecycleOnly: true });
const manifestPath = NodePath.join(source, "manifest.json");
const manifest = JSON.parse(NodeFS.readFileSync(manifestPath, "utf8"));
NodeAssert.equal(manifest.implementation, "database-only");
const sourceCommit = git("rev-parse", "HEAD");
NodeAssert.equal(manifest.sourceCommit, sourceCommit);
NodeAssert.equal(
  git("status", "--porcelain", "--untracked-files=normal"),
  "",
  "Package acceptance requires clean committed application source",
);
const migrations = JSON.parse(
  NodeFS.readFileSync(
    new URL("../apps/server/src/persistence/omdb-schema.json", import.meta.url),
    "utf8",
  ),
).migrations;
const directory = NodeFS.realpathSync(
  NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-omdb-package-")),
);
const report = {
  schema: 1,
  status: "running",
  sourceCommit,
  upstreamCommit: manifest.upstreamCommit,
  target,
  bundleSha256: nativeHash(NodePath.join(source, "manifest.json")),
  checks: [],
};
const save = () => {
  NodeFS.mkdirSync(NodePath.dirname(proofArgument), { recursive: true });
  NodeFS.writeFileSync(proofArgument, JSON.stringify(report, null, 2) + "\n");
};
const saveManifest = () =>
  NodeFS.writeFileSync(manifestPath, JSON.stringify(manifest, null, 2) + "\n");
function bootstrap(executable, database, mode, initialized) {
  const result = NodeChildProcess.spawnSync(executable, [mode, database], {
    input: JSON.stringify({ operation: "application_bootstrap" }) + "\n",
    encoding: "utf8",
    timeout: 30_000,
  });
  if (result.error) throw result.error;
  NodeAssert.equal(result.status, 0, result.stderr);
  const response = JSON.parse(result.stdout.trim());
  NodeAssert.equal(response.ok, true, result.stdout);
  NodeAssert.equal(response.result.initialized, initialized);
  NodeAssert.deepEqual(response.result.migrations, migrations);
}
try {
  const probes = [];
  for (const [name, selectorRelative, bundleRelative] of [
    ["source-package", "source/packages/shared/src/nativeExecutable.ts", "source/bin"],
    ["cli-sidecar", "cli/bin.mjs.ts", "cli/native-bin"],
    [
      "desktop-asar",
      "desktop/app.asar/apps/server/dist/bin.mjs.ts",
      "desktop/app.asar.unpacked/apps/server/dist/native-bin",
    ],
  ]) {
    const selector = NodePath.join(directory, selectorRelative);
    const bundle = NodePath.join(directory, bundleRelative);
    NodeFS.mkdirSync(NodePath.dirname(selector), { recursive: true });
    NodeFS.copyFileSync(
      new URL("../packages/shared/src/nativeExecutable.ts", import.meta.url),
      selector,
    );
    // The owned probe may resolve a lifecycle-qualified candidate directly.
    // Normal selection below remains closed until these executions succeed.
    stageNativeArtifacts(source, bundle, target, { lifecycleOnly: true });
    const { nativeExecutable, selectNativeExecutable } = await import(
      NodeURL.pathToFileURL(selector).href
    );
    const executable = nativeExecutable("db");
    NodeAssert.equal(NodePath.dirname(executable), bundle);
    const database = NodePath.join(directory, `${name}.omdb`);
    for (const [mode, initialized] of [
      ["initialize-local", true],
      ["local", false],
    ]) {
      bootstrap(executable, database, mode, initialized);
    }
    probes.push({ name, bundle, database, selectNativeExecutable });
  }
  NodeAssert.deepEqual(
    probes.map((probe) => probe.name),
    databasePackageChecks,
  );
  const databaseFile = `t3-code-ls-db${target.startsWith("win32-") ? ".exe" : ""}`;
  const packageAcceptance = {
    status: "passed",
    target,
    sourceCommit,
    upstreamCommit: manifest.upstreamCommit,
    executableSha256: manifest.files[databaseFile],
    checks: [...databasePackageChecks],
  };
  // Exercise the unmodified default selector and reopen through its result.
  // Provisional receipts stay in owned probe copies, never the exportable source.
  for (const { name, bundle, database, selectNativeExecutable } of probes) {
    NodeFS.writeFileSync(
      NodePath.join(bundle, "manifest.json"),
      JSON.stringify({ ...manifest, packageAcceptance }, null, 2) + "\n",
    );
    nativeArtifactFiles(bundle, target);
    const executable = selectNativeExecutable("db");
    NodeAssert.equal(NodePath.dirname(executable), bundle);
    NodeAssert.equal(selectNativeExecutable("provider"), undefined);
    NodeAssert.equal(selectNativeExecutable("generate"), undefined);
    bootstrap(executable, database, "local", false);
    report.checks.push({ name, status: "passed", executableSha256: nativeHash(executable) });
    save();
  }
  NodeAssert.equal(
    NodeFS.readdirSync(directory).filter((name) => name.endsWith(".sqlite")).length,
    0,
  );
  NodeAssert.equal(git("rev-parse", "HEAD"), sourceCommit);
  NodeAssert.equal(git("status", "--porcelain", "--untracked-files=normal"), "");
  // A killed probe cannot leave the supplied bundle with an unearned receipt.
  manifest.packageAcceptance = packageAcceptance;
  databaseArtifactFiles(manifest, source, target);
  saveManifest();
  report.bundleSha256 = nativeHash(manifestPath);
  report.status = "passed";
  save();
  console.log(`Executed the default omdb package in source, CLI and desktop layouts on ${target}.`);
} catch (error) {
  delete manifest.packageAcceptance;
  saveManifest();
  report.status = "failed";
  report.failure = String(error);
  save();
  throw error;
} finally {
  // Only this script's newly allocated fixture tree is removed after all children exit.
  NodeFS.rmSync(directory, { recursive: true, force: true });
}
