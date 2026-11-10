import * as Assert from "node:assert/strict";
import * as ChildProcess from "node:child_process";
import * as FS from "node:fs";
import * as Test from "node:test";
import { fileURLToPath } from "node:url";
import {
  adapterPortIds,
  applicationChecks,
  parseHybridArguments,
  planAdapterChecks,
  selectAdapterPorts,
} from "./hybrid-checks.mjs";

Test.test("default adapter selection covers every mapped production Rust port", () => {
  const map = JSON.parse(FS.readFileSync(new URL("../port-map.json", import.meta.url), "utf8"));
  const productionPorts = map.ports.filter((port) => port.state !== "candidate" &&
    port.platform_routes.some((route) => route.implementation === "rust"),
  ).map((port) => port.id).sort();
  Assert.deepEqual([...adapterPortIds].sort(), productionPorts);
  Assert.deepEqual(parseHybridArguments(["--adapters", "/tmp/proof"]).selectedPorts, adapterPortIds);
});

Test.test("a selected port runs only its production checks in stable order", () => {
  const selected = selectAdapterPorts("codex-process-runtime,project-commands");
  Assert.deepEqual(selected, ["project-commands", "codex-process-runtime"]);
  const checks = planAdapterChecks(selected);
  Assert.deepEqual(checks.map(({ port, script }) => ({ port, script })), [
    { port: "project-commands", script: "tools/check-project-adapter.mjs" },
    { port: "codex-process-runtime", script: "tools/check-provider-adapter.mjs" },
  ]);
  Assert.equal(checks.some((check) => check.script === "tools/check-dispatch.mjs"), false);
});

Test.test("dispatch policy always accompanies its selected command-policy owner", () => {
  const checks = planAdapterChecks(selectAdapterPorts("command-and-dispatch-policy"));
  Assert.deepEqual(checks.map((check) => check.script), [
    "tools/check-policy-adapter.mjs", "tools/check-dispatch.mjs",
  ]);
  Assert.deepEqual(checks[1].extraArguments, ["--service"]);
  Assert.equal(checks[1].component, "policy");
  Assert.equal(checks[1].proofFile, "dispatch.json");
});

Test.test("each selected adapter has one existing scoped typecheck project", () => {
  const selected = selectAdapterPorts("command-and-dispatch-policy,codex-structured-generation");
  const checks = planAdapterChecks(selected);
  const typechecks = checks.filter((check) => check.typecheckProject);
  Assert.deepEqual(typechecks.map((check) => check.typecheckProject), [
    "tools/policy-tsconfig.json", "tools/generation-tsconfig.json",
  ]);
  Assert.equal(typechecks.length, selected.length, "dispatch must not duplicate policy typechecking");
  for (const check of planAdapterChecks(selectAdapterPorts()).filter((check) => check.typecheckProject)) {
    Assert.ok(FS.existsSync(new URL(`../${check.typecheckProject}`, import.meta.url)));
    Assert.match(check.typecheckProject, /^tools\/[a-z-]+-tsconfig\.json$/);
  }
});

Test.test("empty, duplicate, unknown and candidate-only port selections fail", () => {
  for (const value of ["", " ", ",", "project-commands,", "project-commands,project-commands",
    "redb-persistence", "../check-other.mjs", "--all", "project-commands,unknown"]) {
    Assert.throws(() => selectAdapterPorts(value));
  }
  Assert.throws(() => planAdapterChecks([]), /empty selection/);
});

Test.test("port arguments are restricted to adapter qualification", () => {
  Assert.deepEqual(parseHybridArguments(["--adapters", "--ports", "project-commands"]), {
    mode: "--adapters", proofDirectory: undefined, selectedPorts: ["project-commands"],
  });
  Assert.deepEqual(parseHybridArguments(["--application", "/tmp/proof"]), {
    mode: "--application", proofDirectory: "/tmp/proof", selectedPorts: [],
  });
  for (const args of [
    [], ["--unknown"], ["--adapters", "--ports"],
    ["--application", "/tmp/proof", "--ports", "project-commands"],
    ["--verify-binaries", "--ports", "project-commands"],
    ["--adapters", "/tmp/one", "/tmp/two"],
    ["--adapters", "--ports", "project-commands", "--ports", "provider-recovery"],
  ]) Assert.throws(() => parseHybridArguments(args));
});

Test.test("invalid CLI selections fail before reading a binary bundle or spawning tests", () => {
  const script = fileURLToPath(new URL("./check-hybrid.mjs", import.meta.url));
  for (const selection of ["unknown", ""]) {
    const result = ChildProcess.spawnSync(process.execPath, [
      script, "--adapters", "/tmp/unused-proof", "--ports", selection,
    ], { encoding: "utf8" });
    Assert.notEqual(result.status, 0);
    Assert.match(result.stderr, /Unknown production adapter port|empty selection/);
    Assert.doesNotMatch(result.stderr, /ENOENT|native hash mismatch/i);
  }
});

Test.test("application qualification keeps fork selection and integration builds", () => {
  const checks = applicationChecks();
  Assert.equal(checks.length, 3);
  Assert.deepEqual(checks[0].arguments.filter((argument) => argument.endsWith(".test.ts")), [
    "packages/shared/src/nativeExecutable.test.ts",
  ]);
  Assert.deepEqual(checks.slice(1).map((check) => check.arguments), [
    ["--dir", "apps/web", "run", "build"],
    ["--dir", "apps/server", "run", "build:bundle"],
  ]);
  Assert.equal(checks.some((check) => check.arguments.includes("effect-tsgo")), false);
  Assert.equal(checks.some((check) => check.arguments.includes("apps/web/scripts/verify-built-boot.ts")), false);
});
