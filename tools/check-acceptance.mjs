#!/usr/bin/env node
/* oxlint-disable t3code/no-global-process-runtime -- Standalone compiler qualification runs before an Effect runtime exists. */
import * as NodeAssert from "node:assert/strict";
import * as NodeChildProcess from "node:child_process";
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";
import { stockRustEnvironment } from "./install-toolchain.mjs";

const root = NodeURL.fileURLToPath(new URL("../", import.meta.url));
const manifestPath = NodeURL.fileURLToPath(new URL("./acceptance-checks.json", import.meta.url));
export const acceptanceChecks = JSON.parse(NodeFS.readFileSync(manifestPath, "utf8"));

export function acceptanceExecutables(check) {
  return [
    ...(check.executable === null
      ? []
      : [{ package: check.package, executable: check.executable }]),
    ...(check.additional_executables ?? []),
  ];
}

export function validateAcceptanceManifest(manifest) {
  NodeAssert.ok(
    manifest && typeof manifest === "object" && !Array.isArray(manifest),
    "Acceptance manifest must map script paths to execution details",
  );
  for (const [script, check] of Object.entries(manifest)) {
    NodeAssert.match(script, /^tools\/check-[a-z0-9-]+\.mjs$/, "Invalid acceptance script path");
    NodeAssert.ok(
      check && typeof check === "object" && !Array.isArray(check),
      `Invalid acceptance entry: ${script}`,
    );
    NodeAssert.deepEqual(
      Object.keys(check).sort(),
      [
        "executable",
        "extra_arguments",
        "package",
        "proof_argument",
        ...(Object.hasOwn(check, "additional_executables") ? ["additional_executables"] : []),
        ...(Object.hasOwn(check, "typecheck_project") ? ["typecheck_project"] : []),
      ].sort(),
      `Unexpected acceptance fields: ${script}`,
    );
    NodeAssert.match(check.package, /^[a-z][a-z0-9-]+$/, `Invalid Cargo package: ${script}`);
    NodeAssert.ok(
      check.executable === null ||
        (typeof check.executable === "string" &&
          /^(?:examples\/)?[a-zA-Z0-9_-]+$/.test(check.executable)),
      `Executable must be a relative debug binary or example: ${script}`,
    );
    NodeAssert.ok(
      Array.isArray(check.extra_arguments) &&
        check.extra_arguments.every((argument) => typeof argument === "string"),
      `Invalid extra arguments: ${script}`,
    );
    NodeAssert.equal(typeof check.proof_argument, "boolean", `Invalid proof argument: ${script}`);
    if (Object.hasOwn(check, "typecheck_project")) {
      NodeAssert.ok(
        typeof check.typecheck_project === "string" &&
          /^tools\/[a-z][a-z0-9-]*-tsconfig\.json$/.test(check.typecheck_project),
        `Invalid typecheck project: ${script}`,
      );
    }
    if (Object.hasOwn(check, "additional_executables")) {
      NodeAssert.ok(
        check.executable !== null &&
          Array.isArray(check.additional_executables) &&
          check.additional_executables.length > 0,
        `Additional executables require a primary executable and nonempty target list: ${script}`,
      );
      for (const target of check.additional_executables) {
        NodeAssert.ok(
          target && typeof target === "object" && !Array.isArray(target),
          `Invalid additional executable: ${script}`,
        );
        NodeAssert.deepEqual(
          Object.keys(target).sort(),
          ["executable", "package"],
          `Unexpected additional executable fields: ${script}`,
        );
        NodeAssert.match(target.package, /^[a-z][a-z0-9-]+$/, `Invalid Cargo package: ${script}`);
        NodeAssert.ok(
          typeof target.executable === "string" &&
            /^(?:examples\/)?[a-zA-Z0-9_-]+$/.test(target.executable),
          `Additional executable must be a relative debug binary or example: ${script}`,
        );
      }
      const targets = acceptanceExecutables(check).map(
        (target) => `${target.package}/${target.executable}`,
      );
      NodeAssert.equal(
        new Set(targets).size,
        targets.length,
        `Duplicate executable target: ${script}`,
      );
    }
  }
  return manifest;
}

validateAcceptanceManifest(acceptanceChecks);

export function selectAcceptanceTests(requested, manifest = acceptanceChecks) {
  NodeAssert.ok(
    Array.isArray(requested) && requested.length > 0,
    "--tests must be a nonempty JSON array of acceptance script paths",
  );
  NodeAssert.equal(
    new Set(requested).size,
    requested.length,
    "--tests must not repeat a script path",
  );
  for (const script of requested) {
    NodeAssert.ok(
      typeof script === "string" && Object.hasOwn(manifest, script),
      `Unknown component acceptance script: ${String(script)}`,
    );
  }
  return [...requested].sort();
}

export function parseAcceptanceArguments(args) {
  let requested;
  let proofDirectory;
  for (let index = 0; index < args.length; index++) {
    const option = args[index];
    NodeAssert.ok(
      option === "--tests" || option === "--proof-directory",
      `Unexpected acceptance argument: ${option}`,
    );
    const value = args[++index];
    NodeAssert.ok(value !== undefined && !value.startsWith("--"), `${option} requires a value`);
    if (option === "--tests") {
      NodeAssert.equal(requested, undefined, "Specify --tests once");
      requested = JSON.parse(value);
    } else {
      NodeAssert.equal(proofDirectory, undefined, "Specify --proof-directory once");
      proofDirectory = value;
    }
  }
  const tests = selectAcceptanceTests(requested);
  NodeAssert.ok(
    proofDirectory && NodePath.isAbsolute(proofDirectory),
    "--proof-directory must be an absolute directory outside the checkout",
  );
  return { tests, proofDirectory };
}

export function planAcceptanceChecks(
  tests,
  { targetDirectory, proofDirectory, platform = process.platform, manifest = acceptanceChecks },
) {
  const selected = selectAcceptanceTests(tests, manifest);
  NodeAssert.ok(
    proofDirectory && NodePath.isAbsolute(proofDirectory),
    "Proof directory must be absolute",
  );
  return selected.map((script) => {
    const check = manifest[script];
    if (check.executable !== null) {
      NodeAssert.ok(
        targetDirectory && NodePath.isAbsolute(targetDirectory),
        "Set an absolute CARGO_TARGET_DIR for component acceptance executables",
      );
    }
    const executables = acceptanceExecutables(check).map((target) => ({
      package: target.package,
      path: NodePath.join(
        targetDirectory,
        "debug",
        `${target.executable}${platform === "win32" ? ".exe" : ""}`,
      ),
    }));
    const executable = executables[0]?.path ?? null;
    const proof = check.proof_argument
      ? NodePath.join(proofDirectory, `${NodePath.basename(script, ".mjs")}.json`)
      : null;
    return {
      script,
      package: check.package,
      executable,
      executables,
      proof,
      arguments: [
        "--disable-warning=ExperimentalWarning",
        script,
        ...executables.map((target) => target.path),
        ...check.extra_arguments,
        ...(proof === null ? [] : [proof]),
      ],
    };
  });
}

export function planAcceptanceBuilds(
  tests,
  toolchainDirectory,
  manifest = acceptanceChecks,
  compiler = { route: "lustr" },
) {
  NodeAssert.ok(
    compiler.route === "lustr" || compiler.route === "rustc",
    "Unknown acceptance compiler route",
  );
  if (compiler.route === "rustc") {
    NodeAssert.match(
      compiler.release ?? "",
      /^\d+\.\d+\.\d+$/,
      "Stock Rust must use a pinned release",
    );
  }
  const packages = new Map();
  for (const script of selectAcceptanceTests(tests, manifest)) {
    const check = manifest[script];
    for (const target of acceptanceExecutables(check)) {
      const targets = packages.get(target.package) ?? new Set();
      targets.add(target.executable);
      packages.set(target.package, targets);
    }
  }
  if (packages.size > 0 && compiler.route === "lustr") {
    NodeAssert.ok(
      toolchainDirectory && NodePath.isAbsolute(toolchainDirectory),
      "Set an absolute T3_CODE_LS_TOOLCHAIN for the verified Lustr/BlueFe compiler",
    );
  }
  return [...packages]
    .sort(([left], [right]) => left.localeCompare(right))
    .map(([packageName, targets]) => ({
      package: packageName,
      program:
        compiler.route === "rustc" ? "rustup" : NodePath.join(toolchainDirectory, "bin", "lustr"),
      arguments: [
        ...(compiler.route === "rustc"
          ? ["run", compiler.release, "cargo"]
          : ["--bluefe-root", toolchainDirectory]),
        "build",
        "--locked",
        "-p",
        packageName,
        ...[...targets]
          .sort()
          .flatMap((target) =>
            target.startsWith("examples/")
              ? ["--example", target.slice("examples/".length)]
              : ["--bin", target],
          ),
      ],
    }));
}

export function planAcceptanceTypechecks(tests, runtimeDirectory, manifest = acceptanceChecks) {
  const projects = new Set();
  for (const script of selectAcceptanceTests(tests, manifest)) {
    const project = manifest[script].typecheck_project;
    if (project !== undefined) projects.add(project);
  }
  return [...projects].sort().map((project) => ({
    project,
    program: NodePath.resolve(
      runtimeDirectory ?? NodePath.join(root, "tools/server-test-runtime"),
      "node_modules",
      ".bin",
      "effect-tsgo",
    ),
    arguments: ["tsrs", "--noEmit", "--project", project],
  }));
}

function outsideCheckout(directory) {
  const relative = NodePath.relative(NodeFS.realpathSync(root), directory);
  NodeAssert.ok(
    relative === ".." || relative.startsWith(`..${NodePath.sep}`) || NodePath.isAbsolute(relative),
    "Acceptance output directories must be outside the source checkout",
  );
}

function sha256(file) {
  return NodeCrypto.createHash("sha256").update(NodeFS.readFileSync(file)).digest("hex");
}

function main() {
  // Reject bad selectors before creating output or starting a compiler/test process.
  const { tests, proofDirectory } = parseAcceptanceArguments(process.argv.slice(2));
  outsideCheckout(NodePath.resolve(proofDirectory));
  NodeFS.mkdirSync(proofDirectory, { recursive: true });
  const proof = NodeFS.realpathSync(proofDirectory);
  outsideCheckout(proof);
  const compilerRoute = process.env.T3_CODE_LS_COMPILER ?? "rustc";
  const compiler = {
    route: compilerRoute,
    ...(compilerRoute === "rustc"
      ? {
          release: NodeFS.readFileSync(NodePath.join(root, "rust-toolchain.toml"), "utf8").match(
            /^channel\s*=\s*"(\d+\.\d+\.\d+)"/m,
          )?.[1],
        }
      : {}),
  };
  const report = {
    schema_version: 1,
    source_commit: NodeChildProcess.execFileSync("git", ["rev-parse", "HEAD"], {
      cwd: root,
      encoding: "utf8",
    }).trim(),
    selected_tests: tests,
    manifest_sha256: sha256(manifestPath),
    compiler_route: compilerRoute === "rustc" ? "pinned-stock-rustc" : "bluefe-rustc-via-lustr",
    compiler,
    scope:
      "Selected mapped Rust component parity, runtime and candidate-boundary acceptance; upstream behavior is the trusted oracle",
    status: "running",
    builds: [],
    typechecks: [],
    checks: [],
  };
  const reportPath = NodePath.join(proof, "acceptance.json");
  const save = () => NodeFS.writeFileSync(reportPath, JSON.stringify(report, null, 2) + "\n");
  function run(records, details, program, args, env) {
    const check = {
      ...details,
      program: NodePath.basename(program),
      arguments: args,
      status: "running",
    };
    records.push(check);
    save();
    console.log(`\n${details.script ?? `Build ${details.package} acceptance executables`}`);
    const result = NodeChildProcess.spawnSync(program, args, { cwd: root, env, stdio: "inherit" });
    check.status = result.status === 0 && !result.error ? "passed" : "failed";
    check.exit_code = result.status;
    if (result.signal) check.signal = result.signal;
    if (result.error) check.error = String(result.error);
    save();
    if (result.error) throw result.error;
    NodeAssert.equal(result.status, 0, `${details.script ?? details.package} acceptance failed`);
  }
  try {
    save();
    const checks = planAcceptanceChecks(tests, {
      targetDirectory: process.env.CARGO_TARGET_DIR,
      proofDirectory: proof,
    });
    const builds = planAcceptanceBuilds(
      tests,
      process.env.T3_CODE_LS_TOOLCHAIN,
      acceptanceChecks,
      compiler,
    );
    const typechecks = planAcceptanceTypechecks(tests, process.env.T3_CODE_LS_TEST_RUNTIME);
    if (builds.length > 0) {
      NodeAssert.ok(
        !process.env.CARGO_BUILD_TARGET,
        "Component acceptance requires host debug executables; leave CARGO_BUILD_TARGET unset",
      );
      outsideCheckout(NodePath.resolve(process.env.CARGO_TARGET_DIR));
      NodeFS.mkdirSync(process.env.CARGO_TARGET_DIR, { recursive: true });
      outsideCheckout(NodeFS.realpathSync(process.env.CARGO_TARGET_DIR));
      if (compiler.route === "lustr") NodeFS.accessSync(builds[0].program, NodeFS.constants.X_OK);
    }
    const temporary = NodePath.join(proof, "fixtures");
    NodeFS.mkdirSync(temporary, { recursive: true });
    const env = {
      ...process.env,
      T3CODE_DEV_SEED: "0",
      T3_CODE_LS_TEST_TMPDIR: temporary,
      TMPDIR: temporary,
      TMP: temporary,
      TEMP: temporary,
    };
    if (builds.length > 0 && compiler.route === "rustc") {
      const rustup = (args) =>
        NodeChildProcess.execFileSync("rustup", args, {
          cwd: root,
          env,
          encoding: "utf8",
        }).trim();
      const tools = Object.fromEntries(
        ["CARGO", "RUSTC", "RUSTDOC"].map((name) => [
          name,
          rustup(["which", "--toolchain", compiler.release, name.toLowerCase()]),
        ]),
      );
      const identity = NodeChildProcess.execFileSync(tools.RUSTC, ["-vV"], {
        cwd: root,
        env,
        encoding: "utf8",
      })
        .replace(/\r\n/g, "\n")
        .trim();
      Object.assign(
        env,
        stockRustEnvironment(env, {
          tools,
          release: compiler.release,
          target: identity.match(/^host: (.+)$/m)?.[1],
          sysroot: NodeChildProcess.execFileSync(tools.RUSTC, ["--print", "sysroot"], {
            cwd: root,
            env,
            encoding: "utf8",
          }).trim(),
        }),
      );
      NodeAssert.equal(
        identity.match(/^release: (.+)$/m)?.[1],
        compiler.release,
        "Acceptance compiler does not match rust-toolchain.toml",
      );
      report.compiler = { ...compiler, identity, sha256: sha256(env.RUSTC) };
      save();
    }
    for (const build of builds) {
      run(report.builds, { package: build.package }, build.program, build.arguments, env);
    }
    for (const check of typechecks) {
      run(report.typechecks, { script: check.project }, check.program, check.arguments, env);
    }
    // Always supply the selected executable: legacy scripts must never enter their Cargo fallback.
    for (const check of checks) {
      for (const target of check.executables) {
        NodeAssert.ok(
          NodeFS.lstatSync(target.path).isFile(),
          `Expected a built acceptance executable: ${target.path}`,
        );
        NodeFS.accessSync(target.path, NodeFS.constants.X_OK);
      }
    }
    for (const check of checks) {
      run(
        report.checks,
        {
          script: check.script,
          package: check.package,
          script_sha256: sha256(NodePath.join(root, check.script)),
          executable: check.executable,
          executable_sha256: check.executable === null ? null : sha256(check.executable),
          executables: check.executables.map((target) => ({
            package: target.package,
            path: target.path,
            sha256: sha256(target.path),
          })),
          proof_file: check.proof,
        },
        process.execPath,
        check.arguments,
        env,
      );
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

if (
  process.argv[1] &&
  NodeURL.pathToFileURL(NodePath.resolve(process.argv[1])).href === import.meta.url
)
  main();
