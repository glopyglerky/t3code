import * as Assert from "node:assert/strict";
import * as ChildProcess from "node:child_process";
import * as FS from "node:fs";
import * as Path from "node:path";
import * as Test from "node:test";
import { fileURLToPath } from "node:url";
import {
  acceptanceChecks,
  parseAcceptanceArguments,
  planAcceptanceBuilds,
  planAcceptanceChecks,
  selectAcceptanceTests,
  validateAcceptanceManifest,
} from "./check-acceptance.mjs";
import { adapterPortIds, planAdapterChecks } from "./hybrid-checks.mjs";

const root = fileURLToPath(new URL("../", import.meta.url));
const targetDirectory = Path.resolve("/tmp/acceptance-target");
const proofDirectory = Path.resolve("/tmp/acceptance-proof");
const toolchainDirectory = Path.resolve("/tmp/qualified-bluefe");

Test.test("every declared acceptance test has exactly one execution route", () => {
  const mapping = JSON.parse(FS.readFileSync(Path.join(root, "port-map.json"), "utf8"));
  const declared = new Set(mapping.ports.flatMap((port) => port.acceptance_tests));
  const adapters = new Set(planAdapterChecks(adapterPortIds).map((check) => check.script));
  const components = new Set(Object.keys(acceptanceChecks));
  for (const script of declared) {
    Assert.ok(FS.existsSync(Path.join(root, script)), `Missing acceptance source: ${script}`);
    if (script.endsWith(".rs")) {
      Assert.match(script, /^crates\/[a-z-]+\/tests\/[a-z_]+\.rs$/,
        "Rust acceptance must be a Cargo integration test included by --all-targets");
      Assert.equal(adapters.has(script) || components.has(script), false);
    } else {
      Assert.equal(Number(adapters.has(script)) + Number(components.has(script)), 1,
        `Acceptance script must have one explicit execution route: ${script}`);
    }
  }
  for (const script of [...adapters, ...components]) {
    Assert.ok(declared.has(script), `Execution mapping has no declared acceptance owner: ${script}`);
  }
});

Test.test("manifest executables belong to existing Cargo package targets", () => {
  const packages = new Map();
  for (const entry of FS.readdirSync(Path.join(root, "crates"), { withFileTypes: true })) {
    if (!entry.isDirectory()) continue;
    const directory = Path.join(root, "crates", entry.name);
    const source = FS.readFileSync(Path.join(directory, "Cargo.toml"), "utf8");
    const name = source.match(/^name\s*=\s*"([^"]+)"/m)?.[1];
    if (name) packages.set(name, directory);
  }
  for (const [script, check] of Object.entries(acceptanceChecks)) {
    Assert.ok(packages.has(check.package), `Unknown manifest Cargo package: ${script}`);
    if (check.executable === null) continue;
    const source = check.executable.startsWith("examples/")
      ? `${check.executable}.rs`
      : `src/bin/${check.executable}.rs`;
    Assert.ok(FS.existsSync(Path.join(packages.get(check.package), source)),
      `Wrong executable owner or target for ${script}: ${source}`);
  }
});

Test.test("selection is explicit, stable, nonempty and rejects unknown or duplicate scripts", () => {
  Assert.deepEqual(selectAcceptanceTests([
    "tools/check-policy.mjs", "tools/check-connection.mjs",
  ]), ["tools/check-connection.mjs", "tools/check-policy.mjs"]);
  for (const requested of [undefined, null, "tools/check-policy.mjs", [], [null],
    ["tools/check-policy.mjs", "tools/check-policy.mjs"], ["../check-policy.mjs"],
    ["tools/check-policy-adapter.mjs"], ["tools/check-dispatch.mjs"], ["tools/unknown.mjs"]]) {
    Assert.throws(() => selectAcceptanceTests(requested));
  }
});

Test.test("command plans preserve executable, single-frame and optional proof positions", () => {
  const checks = planAcceptanceChecks([
    "tools/check-generation.mjs", "tools/check-native-database.mjs",
    "tools/check-project-commands.mjs", "tools/check-project-persistence.mjs",
  ], { targetDirectory, proofDirectory, platform: "linux" });
  const argumentsFor = (script) => checks.find((check) => check.script === script).arguments.slice(2);
  Assert.deepEqual(argumentsFor("tools/check-generation.mjs"), [
    Path.join(targetDirectory, "debug/t3-code-ls-generate"),
    Path.join(proofDirectory, "check-generation.json"),
  ]);
  Assert.deepEqual(argumentsFor("tools/check-native-database.mjs"), [
    Path.join(proofDirectory, "check-native-database.json"),
  ]);
  Assert.deepEqual(argumentsFor("tools/check-project-commands.mjs"), [
    Path.join(targetDirectory, "debug/examples/project_commands_matrix"),
  ]);
  Assert.deepEqual(argumentsFor("tools/check-project-persistence.mjs"), [
    Path.join(targetDirectory, "debug/t3-code-ls-project-store"), "--single",
  ]);
  Assert.equal(checks.find((check) => check.script === "tools/check-project-persistence.mjs").proof, null);
});

Test.test("selected builds use only pinned Lustr and deduplicate exact package targets", () => {
  const builds = planAcceptanceBuilds([
    "tools/check-maintenance.mjs", "tools/check-maintenance-runtime.mjs",
    "tools/check-connection.mjs", "tools/check-native-outbox-worker.mjs",
    "tools/check-outbox-claim-recovery.mjs", "tools/check-native-database.mjs",
  ], toolchainDirectory);
  Assert.deepEqual(builds, [
    {
      package: "t3-code-ls-core",
      program: Path.join(toolchainDirectory, "bin/lustr"),
      arguments: ["--bluefe-root", toolchainDirectory, "build", "--locked", "-p", "t3-code-ls-core",
        "--example", "connection_matrix", "--bin", "t3-code-ls-maintenance"],
    },
    {
      package: "t3-code-ls-storage",
      program: Path.join(toolchainDirectory, "bin/lustr"),
      arguments: ["--bluefe-root", toolchainDirectory, "build", "--locked", "-p", "t3-code-ls-storage",
        "--bin", "t3-code-ls-db"],
    },
  ]);
  Assert.throws(() => planAcceptanceBuilds(["tools/check-policy.mjs"], undefined), /TOOLCHAIN/);
  Assert.throws(() => planAcceptanceChecks(["tools/check-policy.mjs"], { proofDirectory }), /TARGET_DIR/);
});

Test.test("fixture-only acceptance does not require or start a compiler", () => {
  const tests = ["tools/check-native-database.mjs"];
  Assert.deepEqual(planAcceptanceBuilds(tests, undefined), []);
  const [check] = planAcceptanceChecks(tests, { proofDirectory });
  Assert.equal(check.executable, null);
  Assert.deepEqual(check.arguments, [
    "--disable-warning=ExperimentalWarning", tests[0],
    Path.join(proofDirectory, "check-native-database.json"),
  ]);
});

Test.test("manifest rejects paths and executables outside its fixed command schema", () => {
  const valid = acceptanceChecks["tools/check-policy.mjs"];
  for (const entry of [
    { ...valid, executable: "../unexpected" },
    { ...valid, executable: "/absolute/unexpected" },
    { ...valid, executable: "debug/t3-code-ls-policy" },
    { ...valid, package: "--workspace" },
    { ...valid, extra_arguments: "--single" },
    { ...valid, proof_argument: undefined },
    { ...valid, command: "cargo" },
  ]) Assert.throws(() => validateAcceptanceManifest({ "tools/check-policy.mjs": entry }));
  Assert.throws(() => validateAcceptanceManifest({ "../check-policy.mjs": valid }));
});

Test.test("CLI requires an explicit JSON selection and external absolute proof argument", () => {
  const selection = JSON.stringify(["tools/check-policy.mjs"]);
  Assert.deepEqual(parseAcceptanceArguments([
    "--tests", selection, "--proof-directory", proofDirectory,
  ]), { tests: ["tools/check-policy.mjs"], proofDirectory });
  for (const args of [
    [], ["--tests"], ["--tests", selection],
    ["--tests", "{}", "--proof-directory", proofDirectory],
    ["--tests", selection, "--proof-directory", "relative"],
    ["--tests", selection, "--tests", selection, "--proof-directory", proofDirectory],
    ["--tests", selection, "--proof-directory", proofDirectory, "--all"],
  ]) Assert.throws(() => parseAcceptanceArguments(args));
});

Test.test("invalid CLI selections fail before environment checks or compiler execution", () => {
  for (const tests of [["tools/unknown.mjs"], ["tools/check-policy.mjs", "tools/check-policy.mjs"]]) {
    const result = ChildProcess.spawnSync(process.execPath, [
      Path.join(root, "tools/check-acceptance.mjs"),
      "--tests", JSON.stringify(tests), "--proof-directory", proofDirectory,
    ], { encoding: "utf8" });
    Assert.notEqual(result.status, 0);
    Assert.match(result.stderr, /Unknown component acceptance script|must not repeat/);
    Assert.doesNotMatch(result.stderr, /ENOENT|CARGO_TARGET_DIR|T3_CODE_LS_TOOLCHAIN/);
  }
});
