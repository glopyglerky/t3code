#!/usr/bin/env node
import * as Assert from "node:assert/strict";
import * as ChildProcess from "node:child_process";
import * as Crypto from "node:crypto";
import * as FS from "node:fs";
import * as Path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const root = fileURLToPath(new URL("../", import.meta.url));
const manifestPath = fileURLToPath(new URL("./acceptance-checks.json", import.meta.url));
export const acceptanceChecks = JSON.parse(FS.readFileSync(manifestPath, "utf8"));

export function validateAcceptanceManifest(manifest) {
  Assert.ok(manifest && typeof manifest === "object" && !Array.isArray(manifest),
    "Acceptance manifest must map script paths to execution details");
  for (const [script, check] of Object.entries(manifest)) {
    Assert.match(script, /^tools\/check-[a-z0-9-]+\.mjs$/, "Invalid acceptance script path");
    Assert.ok(check && typeof check === "object" && !Array.isArray(check),
      `Invalid acceptance entry: ${script}`);
    Assert.deepEqual(Object.keys(check).sort(),
      ["executable", "extra_arguments", "package", "proof_argument"],
      `Unexpected acceptance fields: ${script}`);
    Assert.match(check.package, /^[a-z][a-z0-9-]+$/, `Invalid Cargo package: ${script}`);
    Assert.ok(check.executable === null ||
      (typeof check.executable === "string" && /^(?:examples\/)?[a-zA-Z0-9_-]+$/.test(check.executable)),
    `Executable must be a relative debug binary or example: ${script}`);
    Assert.ok(Array.isArray(check.extra_arguments) &&
      check.extra_arguments.every((argument) => typeof argument === "string"),
    `Invalid extra arguments: ${script}`);
    Assert.equal(typeof check.proof_argument, "boolean", `Invalid proof argument: ${script}`);
  }
  return manifest;
}

validateAcceptanceManifest(acceptanceChecks);

export function selectAcceptanceTests(requested, manifest = acceptanceChecks) {
  Assert.ok(Array.isArray(requested) && requested.length > 0,
    "--tests must be a nonempty JSON array of acceptance script paths");
  Assert.equal(new Set(requested).size, requested.length, "--tests must not repeat a script path");
  for (const script of requested) {
    Assert.ok(typeof script === "string" && Object.hasOwn(manifest, script),
      `Unknown component acceptance script: ${String(script)}`);
  }
  return [...requested].sort();
}

export function parseAcceptanceArguments(args) {
  let requested;
  let proofDirectory;
  for (let index = 0; index < args.length; index++) {
    const option = args[index];
    Assert.ok(option === "--tests" || option === "--proof-directory",
      `Unexpected acceptance argument: ${option}`);
    const value = args[++index];
    Assert.ok(value !== undefined && !value.startsWith("--"), `${option} requires a value`);
    if (option === "--tests") {
      Assert.equal(requested, undefined, "Specify --tests once");
      requested = JSON.parse(value);
    } else {
      Assert.equal(proofDirectory, undefined, "Specify --proof-directory once");
      proofDirectory = value;
    }
  }
  const tests = selectAcceptanceTests(requested);
  Assert.ok(proofDirectory && Path.isAbsolute(proofDirectory),
    "--proof-directory must be an absolute directory outside the checkout");
  return { tests, proofDirectory };
}

export function planAcceptanceChecks(tests, {
  targetDirectory,
  proofDirectory,
  platform = process.platform,
  manifest = acceptanceChecks,
}) {
  const selected = selectAcceptanceTests(tests, manifest);
  Assert.ok(proofDirectory && Path.isAbsolute(proofDirectory), "Proof directory must be absolute");
  return selected.map((script) => {
    const check = manifest[script];
    if (check.executable !== null) {
      Assert.ok(targetDirectory && Path.isAbsolute(targetDirectory),
        "Set an absolute CARGO_TARGET_DIR for component acceptance executables");
    }
    const executable = check.executable === null ? null : Path.join(
      targetDirectory, "debug", `${check.executable}${platform === "win32" ? ".exe" : ""}`,
    );
    const proof = check.proof_argument
      ? Path.join(proofDirectory, `${Path.basename(script, ".mjs")}.json`)
      : null;
    return {
      script,
      package: check.package,
      executable,
      proof,
      arguments: [
        "--disable-warning=ExperimentalWarning", script,
        ...(executable === null ? [] : [executable]),
        ...check.extra_arguments,
        ...(proof === null ? [] : [proof]),
      ],
    };
  });
}

export function planAcceptanceBuilds(tests, toolchainDirectory, manifest = acceptanceChecks) {
  const packages = new Map();
  for (const script of selectAcceptanceTests(tests, manifest)) {
    const check = manifest[script];
    if (check.executable === null) continue;
    const targets = packages.get(check.package) ?? new Set();
    targets.add(check.executable);
    packages.set(check.package, targets);
  }
  if (packages.size > 0) {
    Assert.ok(toolchainDirectory && Path.isAbsolute(toolchainDirectory),
      "Set an absolute T3_CODE_LS_TOOLCHAIN for the verified Lustr/BlueFe compiler");
  }
  return [...packages].sort(([left], [right]) => left.localeCompare(right)).map(([packageName, targets]) => ({
    package: packageName,
    program: Path.join(toolchainDirectory, "bin", "lustr"),
    arguments: [
      "--bluefe-root", toolchainDirectory, "build", "--locked", "-p", packageName,
      ...[...targets].sort().flatMap((target) => target.startsWith("examples/")
        ? ["--example", target.slice("examples/".length)]
        : ["--bin", target]),
    ],
  }));
}

function outsideCheckout(directory) {
  const relative = Path.relative(FS.realpathSync(root), directory);
  Assert.ok(relative === ".." || relative.startsWith(`..${Path.sep}`) || Path.isAbsolute(relative),
    "Acceptance output directories must be outside the source checkout");
}

function sha256(file) {
  return Crypto.createHash("sha256").update(FS.readFileSync(file)).digest("hex");
}

function main() {
  // Reject bad selectors before creating output or starting a compiler/test process.
  const { tests, proofDirectory } = parseAcceptanceArguments(process.argv.slice(2));
  outsideCheckout(Path.resolve(proofDirectory));
  FS.mkdirSync(proofDirectory, { recursive: true });
  const proof = FS.realpathSync(proofDirectory);
  outsideCheckout(proof);
  const report = {
    schema_version: 1,
    source_commit: ChildProcess.execFileSync("git", ["rev-parse", "HEAD"], {
      cwd: root, encoding: "utf8",
    }).trim(),
    selected_tests: tests,
    manifest_sha256: sha256(manifestPath),
    compiler_route: "bluefe-rustc-via-lustr",
    scope: "Selected mapped Rust component parity, runtime and candidate-boundary acceptance; upstream behavior is the trusted oracle",
    status: "running",
    builds: [],
    checks: [],
  };
  const reportPath = Path.join(proof, "acceptance.json");
  const save = () => FS.writeFileSync(reportPath, JSON.stringify(report, null, 2) + "\n");
  function run(records, details, program, args, env) {
    const check = { ...details, program: Path.basename(program), arguments: args, status: "running" };
    records.push(check);
    save();
    console.log(`\n${details.script ?? `Build ${details.package} acceptance executables`}`);
    const result = ChildProcess.spawnSync(program, args, { cwd: root, env, stdio: "inherit" });
    check.status = result.status === 0 && !result.error ? "passed" : "failed";
    check.exit_code = result.status;
    if (result.signal) check.signal = result.signal;
    if (result.error) check.error = String(result.error);
    save();
    if (result.error) throw result.error;
    Assert.equal(result.status, 0, `${details.script ?? details.package} acceptance failed`);
  }
  try {
    save();
    const checks = planAcceptanceChecks(tests, {
      targetDirectory: process.env.CARGO_TARGET_DIR,
      proofDirectory: proof,
    });
    const builds = planAcceptanceBuilds(tests, process.env.T3_CODE_LS_TOOLCHAIN);
    if (builds.length > 0) {
      Assert.ok(!process.env.CARGO_BUILD_TARGET,
        "Component acceptance requires host debug executables; leave CARGO_BUILD_TARGET unset");
      outsideCheckout(Path.resolve(process.env.CARGO_TARGET_DIR));
      FS.mkdirSync(process.env.CARGO_TARGET_DIR, { recursive: true });
      outsideCheckout(FS.realpathSync(process.env.CARGO_TARGET_DIR));
      FS.accessSync(builds[0].program, FS.constants.X_OK);
    }
    const temporary = Path.join(proof, "fixtures");
    FS.mkdirSync(temporary, { recursive: true });
    const env = {
      ...process.env,
      T3CODE_DEV_SEED: "0",
      T3_CODE_LS_TEST_TMPDIR: temporary,
      TMPDIR: temporary,
      TMP: temporary,
      TEMP: temporary,
    };
    for (const build of builds) {
      run(report.builds, { package: build.package }, build.program, build.arguments, env);
    }
    // Always supply the selected executable: legacy scripts must never enter their Cargo fallback.
    for (const check of checks) {
      if (check.executable !== null) {
        Assert.ok(FS.lstatSync(check.executable).isFile(),
          `Expected a built acceptance executable: ${check.executable}`);
        FS.accessSync(check.executable, FS.constants.X_OK);
      }
    }
    for (const check of checks) {
      run(report.checks, {
        script: check.script,
        package: check.package,
        script_sha256: sha256(Path.join(root, check.script)),
        executable: check.executable,
        executable_sha256: check.executable === null ? null : sha256(check.executable),
        proof_file: check.proof,
      }, process.execPath, check.arguments, env);
    }
    report.status = "passed";
    save();
    console.log(`Component acceptance evidence: ${reportPath}`);
  } catch (error) {
    report.status = "failed";
    report.failure = String(error);
    save();
    throw error;
  }
}

if (process.argv[1] && pathToFileURL(Path.resolve(process.argv[1])).href === import.meta.url) main();
