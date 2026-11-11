import * as NodeAssert from "node:assert/strict";
import * as NodeChildProcess from "node:child_process";
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import * as NodeTest from "node:test";
import * as NodeURL from "node:url";
import {
  acceptanceChecks,
  acceptanceExecutables,
  parseAcceptanceArguments,
  planAcceptanceBuilds,
  planAcceptanceChecks,
  planAcceptanceTypechecks,
  selectAcceptanceTests,
  validateAcceptanceManifest,
} from "./check-acceptance.mjs";
import { adapterPortIds, planAdapterChecks } from "./hybrid-checks.mjs";

const root = NodeURL.fileURLToPath(new URL("../", import.meta.url));
const targetDirectory = NodePath.resolve("/tmp/acceptance-target");
const proofDirectory = NodePath.resolve("/tmp/acceptance-proof");
const toolchainDirectory = NodePath.resolve("/tmp/qualified-bluefe");

NodeTest.test("every declared acceptance test has exactly one execution route", () => {
  const mapping = JSON.parse(NodeFS.readFileSync(NodePath.join(root, "port-map.json"), "utf8"));
  const declared = new Set(mapping.ports.flatMap((port) => port.acceptance_tests));
  const adapters = new Set(planAdapterChecks(adapterPortIds).map((check) => check.script));
  const components = new Set(Object.keys(acceptanceChecks));
  for (const script of declared) {
    NodeAssert.ok(
      NodeFS.existsSync(NodePath.join(root, script)),
      `Missing acceptance source: ${script}`,
    );
    if (script.endsWith(".rs")) {
      NodeAssert.match(
        script,
        /^crates\/[a-z-]+\/tests\/[a-z_]+\.rs$/,
        "Rust acceptance must be a Cargo integration test included by --all-targets",
      );
      NodeAssert.equal(adapters.has(script) || components.has(script), false);
    } else {
      NodeAssert.equal(
        Number(adapters.has(script)) + Number(components.has(script)),
        1,
        `Acceptance script must have one explicit execution route: ${script}`,
      );
    }
  }
  for (const script of [...adapters, ...components]) {
    NodeAssert.ok(
      declared.has(script),
      `Execution mapping has no declared acceptance owner: ${script}`,
    );
  }
});

NodeTest.test("manifest executables belong to existing Cargo package targets", () => {
  const packages = new Map();
  for (const entry of NodeFS.readdirSync(NodePath.join(root, "crates"), { withFileTypes: true })) {
    if (!entry.isDirectory()) continue;
    const directory = NodePath.join(root, "crates", entry.name);
    const source = NodeFS.readFileSync(NodePath.join(directory, "Cargo.toml"), "utf8");
    const name = source.match(/^name\s*=\s*"([^"]+)"/m)?.[1];
    if (name) packages.set(name, directory);
  }
  for (const [script, check] of Object.entries(acceptanceChecks)) {
    NodeAssert.ok(packages.has(check.package), `Unknown manifest Cargo package: ${script}`);
    for (const target of acceptanceExecutables(check)) {
      NodeAssert.ok(packages.has(target.package), `Unknown additional Cargo package: ${script}`);
      const source = target.executable.startsWith("examples/")
        ? `${target.executable}.rs`
        : `src/bin/${target.executable}.rs`;
      NodeAssert.ok(
        NodeFS.existsSync(NodePath.join(packages.get(target.package), source)),
        `Wrong executable owner or target for ${script}: ${source}`,
      );
    }
  }
});

NodeTest.test(
  "selection is explicit, stable, nonempty and rejects unknown or duplicate scripts",
  () => {
    NodeAssert.deepEqual(
      selectAcceptanceTests(["tools/check-policy.mjs", "tools/check-connection.mjs"]),
      ["tools/check-connection.mjs", "tools/check-policy.mjs"],
    );
    for (const requested of [
      undefined,
      null,
      "tools/check-policy.mjs",
      [],
      [null],
      ["tools/check-policy.mjs", "tools/check-policy.mjs"],
      ["../check-policy.mjs"],
      ["tools/check-policy-adapter.mjs"],
      ["tools/check-dispatch.mjs"],
      ["tools/unknown.mjs"],
    ]) {
      NodeAssert.throws(() => selectAcceptanceTests(requested));
    }
  },
);

NodeTest.test(
  "command plans preserve executable, single-frame and optional proof positions",
  () => {
    const checks = planAcceptanceChecks(
      [
        "tools/check-generation.mjs",
        "tools/check-native-database.mjs",
        "tools/check-project-commands.mjs",
        "tools/check-project-persistence.mjs",
      ],
      { targetDirectory, proofDirectory, platform: "linux" },
    );
    const argumentsFor = (script) =>
      checks.find((check) => check.script === script).arguments.slice(2);
    NodeAssert.deepEqual(argumentsFor("tools/check-generation.mjs"), [
      NodePath.join(targetDirectory, "debug/t3-code-ls-generate"),
      NodePath.join(proofDirectory, "check-generation.json"),
    ]);
    NodeAssert.deepEqual(argumentsFor("tools/check-native-database.mjs"), [
      NodePath.join(proofDirectory, "check-native-database.json"),
    ]);
    NodeAssert.deepEqual(argumentsFor("tools/check-project-commands.mjs"), [
      NodePath.join(targetDirectory, "debug/examples/project_commands_matrix"),
    ]);
    NodeAssert.deepEqual(argumentsFor("tools/check-project-persistence.mjs"), [
      NodePath.join(targetDirectory, "debug/t3-code-ls-project-store"),
      "--single",
    ]);
    NodeAssert.equal(
      checks.find((check) => check.script === "tools/check-project-persistence.mjs").proof,
      null,
    );
  },
);

NodeTest.test("selected builds use only pinned Lustr and deduplicate exact package targets", () => {
  const builds = planAcceptanceBuilds(
    [
      "tools/check-maintenance.mjs",
      "tools/check-maintenance-runtime.mjs",
      "tools/check-connection.mjs",
      "tools/check-native-outbox-worker.mjs",
      "tools/check-outbox-claim-recovery.mjs",
      "tools/check-native-database.mjs",
    ],
    toolchainDirectory,
  );
  NodeAssert.deepEqual(builds, [
    {
      package: "t3-code-ls-core",
      program: NodePath.join(toolchainDirectory, "bin/lustr"),
      arguments: [
        "--bluefe-root",
        toolchainDirectory,
        "build",
        "--locked",
        "-p",
        "t3-code-ls-core",
        "--example",
        "connection_matrix",
        "--bin",
        "t3-code-ls-maintenance",
      ],
    },
    {
      package: "t3-code-ls-storage",
      program: NodePath.join(toolchainDirectory, "bin/lustr"),
      arguments: [
        "--bluefe-root",
        toolchainDirectory,
        "build",
        "--locked",
        "-p",
        "t3-code-ls-storage",
        "--bin",
        "t3-code-ls-db",
      ],
    },
  ]);
  NodeAssert.throws(() => planAcceptanceBuilds(["tools/check-policy.mjs"], undefined), /TOOLCHAIN/);
  NodeAssert.throws(
    () => planAcceptanceChecks(["tools/check-policy.mjs"], { proofDirectory }),
    /TARGET_DIR/,
  );
});

NodeTest.test("stock Rust acceptance is explicitly pinned and does not need Lustr", () => {
  const builds = planAcceptanceBuilds(
    ["tools/check-native-outbox-worker.mjs", "tools/check-outbox-claim-recovery.mjs"],
    undefined,
    acceptanceChecks,
    { route: "rustc", release: "1.99.1" },
  );
  NodeAssert.deepEqual(builds, [
    {
      package: "t3-code-ls-storage",
      program: "rustup",
      arguments: [
        "run",
        "1.99.1",
        "cargo",
        "build",
        "--locked",
        "-p",
        "t3-code-ls-storage",
        "--bin",
        "t3-code-ls-db",
      ],
    },
  ]);
  for (const compiler of [
    { route: "rustc", release: "stable" },
    { route: "rustc" },
    { route: "unknown" },
  ])
    NodeAssert.throws(() =>
      planAcceptanceBuilds(
        ["tools/check-native-outbox-worker.mjs"],
        undefined,
        acceptanceChecks,
        compiler,
      ),
    );
});

NodeTest.test(
  "database gates receive every helper in order and compile each owning package once",
  () => {
    const tests = [
      "tools/check-database.mjs",
      "tools/check-outbox.mjs",
      "tools/check-native-outbox-worker.mjs",
    ];
    for (const platform of ["linux", "win32"]) {
      const checks = planAcceptanceChecks(tests, { targetDirectory, proofDirectory, platform });
      const suffix = platform === "win32" ? ".exe" : "";
      for (const [script, helper] of [
        ["tools/check-database.mjs", "t3-code-ls-project-store"],
        ["tools/check-outbox.mjs", "t3-code-ls-outbox"],
      ]) {
        const check = checks.find((entry) => entry.script === script);
        NodeAssert.deepEqual(check.arguments.slice(2), [
          NodePath.join(targetDirectory, `debug/t3-code-ls-db${suffix}`),
          NodePath.join(targetDirectory, `debug/${helper}${suffix}`),
          NodePath.join(proofDirectory, `${NodePath.basename(script, ".mjs")}.json`),
        ]);
        NodeAssert.deepEqual(
          check.executables.map((target) => target.package),
          ["t3-code-ls-storage", "t3-code-ls-core"],
        );
      }
    }
    NodeAssert.deepEqual(planAcceptanceBuilds(tests, toolchainDirectory), [
      {
        package: "t3-code-ls-core",
        program: NodePath.join(toolchainDirectory, "bin/lustr"),
        arguments: [
          "--bluefe-root",
          toolchainDirectory,
          "build",
          "--locked",
          "-p",
          "t3-code-ls-core",
          "--bin",
          "t3-code-ls-outbox",
          "--bin",
          "t3-code-ls-project-store",
        ],
      },
      {
        package: "t3-code-ls-storage",
        program: NodePath.join(toolchainDirectory, "bin/lustr"),
        arguments: [
          "--bluefe-root",
          toolchainDirectory,
          "build",
          "--locked",
          "-p",
          "t3-code-ls-storage",
          "--bin",
          "t3-code-ls-db",
        ],
      },
    ]);
  },
);

NodeTest.test("candidate database adapters use the locked runtime's actual typechecker", () => {
  const runtimeDirectory = NodePath.resolve("/tmp/acceptance-runtime");
  const tests = ["tools/check-redb-transactions.mjs", "tools/check-native-outbox-worker.mjs"];
  NodeAssert.deepEqual(
    planAcceptanceTypechecks(tests, runtimeDirectory),
    ["tools/outbox-tsconfig.json", "tools/redb-tsconfig.json"].map((project) => ({
      project,
      program: NodePath.join(runtimeDirectory, "node_modules/.bin/effect-tsgo"),
      arguments: ["tsrs", "--noEmit", "--project", project],
    })),
  );
  NodeAssert.deepEqual(
    planAcceptanceTypechecks(["tools/check-native-database.mjs"], undefined),
    [],
  );
});

NodeTest.test("fixture-only acceptance does not require or start a compiler", () => {
  const tests = ["tools/check-native-database.mjs"];
  NodeAssert.deepEqual(planAcceptanceBuilds(tests, undefined), []);
  const [check] = planAcceptanceChecks(tests, { proofDirectory });
  NodeAssert.equal(check.executable, null);
  NodeAssert.deepEqual(check.arguments, [
    "--disable-warning=ExperimentalWarning",
    tests[0],
    NodePath.join(proofDirectory, "check-native-database.json"),
  ]);
});

NodeTest.test("manifest rejects paths and executables outside its fixed command schema", () => {
  const valid = acceptanceChecks["tools/check-policy.mjs"];
  for (const entry of [
    { ...valid, executable: "../unexpected" },
    { ...valid, executable: "/absolute/unexpected" },
    { ...valid, executable: "debug/t3-code-ls-policy" },
    { ...valid, package: "--workspace" },
    { ...valid, extra_arguments: "--single" },
    { ...valid, proof_argument: undefined },
    { ...valid, command: "cargo" },
    { ...valid, additional_executables: [] },
    { ...valid, additional_executables: null },
    { ...valid, additional_executables: [{ package: "t3-code-ls-core", executable: "../wrong" }] },
    { ...valid, additional_executables: [{ package: "--workspace", executable: "wrong" }] },
    {
      ...valid,
      additional_executables: [{ package: valid.package, executable: valid.executable }],
    },
    {
      ...valid,
      additional_executables: [{ package: valid.package, executable: "valid", extra: true }],
    },
    {
      ...valid,
      executable: null,
      additional_executables: [{ package: valid.package, executable: "valid" }],
    },
    { ...valid, typecheck_project: "../other-tsconfig.json" },
    { ...valid, typecheck_project: "tools/../other-tsconfig.json" },
    { ...valid, typecheck_project: null },
  ])
    NodeAssert.throws(() => validateAcceptanceManifest({ "tools/check-policy.mjs": entry }));
  NodeAssert.throws(() => validateAcceptanceManifest({ "../check-policy.mjs": valid }));
});

NodeTest.test(
  "CLI requires an explicit JSON selection and external absolute proof argument",
  () => {
    const selection = JSON.stringify(["tools/check-policy.mjs"]);
    NodeAssert.deepEqual(
      parseAcceptanceArguments(["--tests", selection, "--proof-directory", proofDirectory]),
      { tests: ["tools/check-policy.mjs"], proofDirectory },
    );
    for (const args of [
      [],
      ["--tests"],
      ["--tests", selection],
      ["--tests", "{}", "--proof-directory", proofDirectory],
      ["--tests", selection, "--proof-directory", "relative"],
      ["--tests", selection, "--tests", selection, "--proof-directory", proofDirectory],
      ["--tests", selection, "--proof-directory", proofDirectory, "--all"],
    ])
      NodeAssert.throws(() => parseAcceptanceArguments(args));
  },
);

NodeTest.test("invalid CLI selections fail before environment checks or compiler execution", () => {
  for (const tests of [
    ["tools/unknown.mjs"],
    ["tools/check-policy.mjs", "tools/check-policy.mjs"],
  ]) {
    const result = NodeChildProcess.spawnSync(
      process.execPath,
      [
        NodePath.join(root, "tools/check-acceptance.mjs"),
        "--tests",
        JSON.stringify(tests),
        "--proof-directory",
        proofDirectory,
      ],
      { encoding: "utf8" },
    );
    NodeAssert.notEqual(result.status, 0);
    NodeAssert.match(result.stderr, /Unknown component acceptance script|must not repeat/);
    NodeAssert.doesNotMatch(result.stderr, /ENOENT|CARGO_TARGET_DIR|T3_CODE_LS_TOOLCHAIN/);
  }
});
