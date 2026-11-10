#!/usr/bin/env node
import * as NodeAssert from "node:assert/strict";
import * as NodeCrypto from "node:crypto";
import * as NodeFSP from "node:fs/promises";
import * as NodeModule from "node:module";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { ProviderDriverKind, ProviderInstanceId } from "@t3tools/contracts";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Context from "effect/Context";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as References from "effect/References";
import * as Scheduler from "effect/Scheduler";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import * as ChildProcess from "effect/process/ChildProcess";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";
import * as HttpClient from "effect/http/HttpClient";
import * as Maintenance from "../packages/provider-core/src/server/maintenanceResolver.ts";
import * as NativeMaintenancePolicy from "../packages/provider-core/src/server/NativeMaintenancePolicy.ts";
import * as ProviderLatestVersions from "../packages/provider-core/src/server/ProviderLatestVersions.ts";
import * as ProviderMaintenanceRunner from "../apps/server/src/provider/providerMaintenanceRunner.ts";
import * as ProviderRegistry from "../apps/server/src/provider/ProviderRegistry.ts";
import * as ModelManifest from "../apps/server/src/provider/ModelManifest.ts";
import { upstreamSource } from "./upstream-source.mjs";

NodeAssert.ok(process.argv[2], "Pass the native maintenance executable");
const executable = NodePath.resolve(process.argv[2]);
const proofPath = process.argv[3];
const root = NodeURL.fileURLToPath(new URL("../", import.meta.url));
const temporary = await NodeFSP.mkdtemp(
  NodePath.join(NodeOS.tmpdir(), "t3ls-maintenance-adapter-"),
);
const scenarios = [];
const driver = ProviderDriverKind.make("codex");
const instanceId = ProviderInstanceId.make("codex");
const definition = {
  provider: driver,
  npmPackageName: "@openai/codex",
  nativeUpdate: null,
  mise: { tool: "codex" },
};
const owner = { provider: driver, packageName: "@openai/codex" };
const policyExecutables = new Set([executable]);
let totalNativeProcesses = 0;
let totalProbeCommands = 0;
let totalUpdateCommands = 0;
let totalUpdateActionComparisons = 0;
let totalVerificationComparisons = 0;

const oracleSource = upstreamSource("packages/provider-core/src/server/maintenanceResolver.ts");
const oracleStart = "export function makeTargetedProviderUpdateAction(";
const oracleEnd = "export function makeManualOnlyProviderMaintenanceCapabilities(";
const oracleStartIndex = oracleSource.indexOf(oracleStart);
const oracleEndIndex = oracleSource.indexOf(oracleEnd);
NodeAssert.ok(oracleStartIndex >= 0 && oracleEndIndex > oracleStartIndex);
NodeAssert.equal(oracleSource.indexOf(oracleStart, oracleStartIndex + 1), -1);
NodeAssert.equal(oracleSource.indexOf(oracleEnd, oracleEndIndex + 1), -1);
const updateActionOracle = new Function(
  NodeModule.stripTypeScriptTypes(oracleSource.slice(oracleStartIndex, oracleEndIndex))
    .replaceAll("export function ", "function ") +
    "\nreturn { makeTargetedProviderUpdateAction, resolveLatestProviderUpdateAction };",
)();

const verificationOracleSource = upstreamSource("apps/server/src/provider/providerMaintenanceRunner.ts");
function verificationSourceBetween(start, end) {
  const first = verificationOracleSource.indexOf(start);
  const last = verificationOracleSource.indexOf(end, first + start.length);
  NodeAssert.ok(first >= 0 && last > first, `Missing upstream verification markers: ${start}`);
  NodeAssert.equal(verificationOracleSource.indexOf(start, first + 1), -1);
  NodeAssert.equal(verificationOracleSource.indexOf(end, last + 1), -1);
  return verificationOracleSource.slice(first, last);
}
const preUpdateOracle = new Function("prior", "precheck",
  NodeModule.stripTypeScriptTypes(verificationSourceBetween("const alreadyCurrent =", "if (alreadyCurrent)")) +
  "\nreturn alreadyCurrent;",
);
const postUpdateOracle = new Function("verifiedProviders", "targetVersion",
  NodeModule.stripTypeScriptTypes(
    verificationSourceBetween("function isOutdatedProvider(", "function makeUpdateState(") +
    verificationSourceBetween("const couldNotVerify =", "return yield* finish(\n              makeUpdateState({\n                status: couldNotVerify"),
  ) + "\nreturn {" +
  verificationSourceBetween("status: couldNotVerify || stillOutdated", "                startedAt,\n                finishedAt,\n                message: couldNotVerify") +
  verificationSourceBetween("message: couldNotVerify", "                output: commandOutput(result),\n              }),\n            );\n          },") +
  "};",
);

function originalVerification(input) {
  const verifiedProviders = input.verifiedProviders.map(({ authStatus, versionAdvisoryStatus, ...provider }) => ({
    ...provider,
    auth: { status: authStatus },
    ...(versionAdvisoryStatus === null ? {} : { versionAdvisory: { status: versionAdvisoryStatus } }),
  }));
  return input.mode === "pre-update"
    ? { kind: "verified", mode: input.mode, alreadyCurrent: preUpdateOracle(
      input.priorCheckedAt === null ? undefined : { checkedAt: input.priorCheckedAt },
      { advisoryEnriched: input.advisoryEnriched, verifiedProviders },
    ) }
    : { kind: "verified", mode: input.mode, ...postUpdateOracle(verifiedProviders, input.targetVersion ?? undefined) };
}

async function compareVerifications(name, inputs) {
  const f = await fixture(`verification-${name}`);
  await run(Effect.gen(function* () {
    for (const [index, input] of inputs.entries()) {
      const before = structuredClone(input);
      const actual = yield* NativeMaintenancePolicy.verifyUpdate(executable, input);
      NodeAssert.deepEqual(actual, originalVerification(input), `${name} case ${index}: ${JSON.stringify(input)}`);
      NodeAssert.deepEqual(input, before, "verification must not mutate captured inputs");
      totalVerificationComparisons++;
    }
  }), f);
  NodeAssert.equal(f.natives.length, inputs.length);
  NodeAssert.equal(f.calls.length, 0);
  NodeAssert.equal(f.updates.length, 0);
}

function originalUpdateAction(input) {
  const capabilities = { provider: driver, packageName: input.packageName, update: input.update };
  const action = input.mode === "target"
    ? updateActionOracle.makeTargetedProviderUpdateAction(capabilities, input.versionValue)
    : updateActionOracle.resolveLatestProviderUpdateAction(capabilities, input.versionValue);
  return action === null ? null : { command: action.command, args: action.args };
}

async function compareUpdateActions(name, inputs) {
  const f = await fixture(`selection-${name}`);
  await run(Effect.gen(function* () {
    for (const [index, input] of inputs.entries()) {
      const before = structuredClone(input);
      const actual = yield* NativeMaintenancePolicy.selectUpdateAction(executable, input);
      NodeAssert.deepEqual(actual, originalUpdateAction(input), `${name} case ${index}: ${JSON.stringify(input)}`);
      NodeAssert.deepEqual(input, before, "selection must not mutate captured inputs");
      totalUpdateActionComparisons++;
    }
  }), f);
  NodeAssert.equal(f.natives.length, inputs.length);
  NodeAssert.equal(f.calls.length, 0);
  NodeAssert.equal(f.updates.length, 0);
}

async function writeExecutable(filename, source = "#!/bin/sh\nexit 93\n") {
  await NodeFSP.mkdir(NodePath.dirname(filename), { recursive: true });
  await NodeFSP.writeFile(filename, source, { mode: 0o755 });
  return filename;
}

async function fixture(name, relative = "native/codex") {
  const base = NodePath.join(temporary, name);
  const command = await writeExecutable(NodePath.join(base, relative));
  return {
    base,
    command,
    env: {
      PATH: NodePath.join(base, "bin"),
      PRIVATE_INSTALL_TOKEN: "fixture-secret-do-not-publish",
    },
    calls: [],
    natives: [],
    updates: [],
    commands: [],
    requestedVersion: "latest",
    active: true,
    which: command,
    latest: "2.0.0",
    context: {
      binaryPath: command,
      resolvedCommandPath: command,
      realCommandPath: command,
      env: {},
      platform: "linux",
    },
  };
}

async function miseFixture(name) {
  const f = await fixture(name, "mise/installs/codex/1.0.0/bin/codex");
  f.mise = await writeExecutable(NodePath.join(f.base, "bin", "mise"));
  f.install = NodePath.dirname(NodePath.dirname(f.command));
  f.shim = NodePath.join(f.base, "mise", "shims", "codex");
  await NodeFSP.mkdir(NodePath.dirname(f.shim), { recursive: true });
  await NodeFSP.symlink(f.mise, f.shim);
  f.context = { ...f.context, resolvedCommandPath: f.shim, realCommandPath: f.mise, env: f.env };
  return f;
}

function answer(f, command, args) {
  const call = [command, ...args];
  f.calls.push(call);
  totalProbeCommands++;
  if (f.respond) return f.respond(command, args);
  if (args[0] === "which") return { stdout: `${f.which}\n` };
  if (args[0] === "ls" && args.includes("--installed"))
    return {
      stdout: JSON.stringify([
        { install_path: f.install, requested_version: f.requestedVersion, active: f.active },
      ]),
    };
  if (args[0] === "latest") return { stdout: `${f.latest}\n` };
  if (args[0] === "--version" && NodePath.basename(command) === "pnpm")
    return { stdout: "10.4.0\n" };
  if (args[0] === "outdated")
    return { stdout: JSON.stringify({ "@openai/codex": { latest: f.latest } }), code: 1 };
  if (args[0] === "view") return { stdout: JSON.stringify(f.latest) };
  if (args[0] === "--prefix") return { stdout: `${f.base}\n` };
  if (args[0] === "info")
    return {
      stdout: JSON.stringify({
        casks: [{ version: f.latest }],
        formulae: [{ versions: { stable: f.latest } }],
      }),
    };
  if (args[0] === "search") return { stdout: `claude-code|${f.latest}\n` };
  if (args[0] === "upgrade" && command === f.mise && args[1] === "codex" && f.allowUpdate) {
    totalUpdateCommands++;
    f.updates.push(call);
    return { stdout: "Controlled update completed\n" };
  }
  NodeAssert.fail(`Unexpected controlled command ${JSON.stringify(call)}`);
}

function handle(result) {
  return ChildProcessSpawner.makeHandle({
    pid: ChildProcessSpawner.ProcessId(1),
    exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(result.code ?? 0)),
    isRunning: Effect.succeed(false),
    kill: () => Effect.void,
    unref: Effect.succeed(Effect.void),
    stdin: Sink.drain,
    stdout: Stream.encodeText(Stream.make(result.stdout ?? "")),
    stderr: Stream.encodeText(Stream.make(result.stderr ?? "")),
    all: Stream.empty,
    getInputFd: () => Sink.drain,
    getOutputFd: () => Stream.empty,
  });
}

async function run(effect, f, selected = executable) {
  const environment = {
    ...f.env,
    ...(f.useDefaultNativeSelection ? {} : selected === null
      ? { T3_CODE_LS_IMPLEMENTATION: "upstream" }
      : selected === undefined ? {} : { T3_CODE_LS_MAINTENANCE_EXECUTABLE: selected }),
  };
  return Effect.runPromise(
    Effect.gen(function* () {
      const actual = yield* ChildProcessSpawner.ChildProcessSpawner;
      const actualFs = yield* FileSystem.FileSystem;
      const controlled = ChildProcessSpawner.make(
        Effect.fn("checkMaintenanceAdapter.spawn")(function* (command) {
          if (policyExecutables.has(command.command)) {
            NodeAssert.deepEqual(command.args, []);
            NodeAssert.deepEqual(command.options.env, {});
            NodeAssert.equal(command.options.extendEnv, false);
            NodeAssert.equal(command.options.shell, false);
            totalNativeProcesses++;
            const replacement = f.policyOverrides?.get(f.natives.length + 1);
            const child = yield* actual.spawn(replacement === undefined
              ? command
              : ChildProcess.make(replacement, command.args, command.options));
            f.natives.push(Number(child.pid));
            if (f.onNativeSpawn) yield* f.onNativeSpawn(child);
            return child;
          }
          f.commands.push({ command: command.command, args: command.args, options: command.options });
          return handle(answer(f, command.command, command.args));
        }),
      );
      const operation =
        f.wrapFileSystem === undefined
          ? effect
          : effect.pipe(Effect.provideService(FileSystem.FileSystem, f.wrapFileSystem(actualFs)));
      return yield* operation.pipe(
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, controlled),
      );
    }).pipe(
      Effect.scoped,
      Effect.provide(NodeServices.layer),
      Effect.provideService(HostProcess.Environment, environment),
      Effect.provideService(HostProcess.Platform, f.platform ?? "linux"),
      Effect.provideService(HostProcess.HomeDirectory, f.base),
      Effect.provideService(HostProcess.WorkingDirectory, f.base),
      Effect.provideService(References.MinimumLogLevel, "None"),
    ),
  );
}

const direct = (f, spec = definition) =>
  Maintenance.resolvePackageManagedProviderMaintenance(spec, { ...f.context, env: f.env });
const full = (f, spec = definition) =>
  Maintenance.resolveProviderMaintenanceCapabilitiesEffect(
    Maintenance.makePackageManagedProviderMaintenanceResolver(spec),
    { binaryPath: f.shim ?? f.command, env: f.env },
  );

async function scenario(name, check) {
  await check();
  scenarios.push(name);
}

async function fakePolicy(name, source) {
  const file = await writeExecutable(
    NodePath.join(temporary, `${name}.cjs`),
    `#!${process.execPath}\n${source}\n`,
  );
  policyExecutables.add(file);
  return file;
}

function policyReply(expression, tail = "") {
  return `process.stdin.once('data', b => { const q=JSON.parse(b); process.stdout.write(JSON.stringify({version:1,ok:${expression}})+'\\n',()=>{${tail}process.exit(0)}); });`;
}

function runnerProvider(overrides = {}) {
  return {
    instanceId, driver, enabled: true, installed: true, version: "1.0.0", status: "ready",
    auth: { status: "authenticated" }, checkedAt: "2026-11-08T12:00:00.000Z",
    models: [], slashCommands: [], skills: [], ...overrides,
  };
}

async function updateRunnerFixture(name, mode = "latest", options = {}) {
  const f = await fixture(name);
  const manager = await writeExecutable(NodePath.join(f.base, "bin", mode === "latest" ? "pnpm" : "npm"));
  const args = mode === "latest"
    ? ["add", "--global", "@openai/codex@latest"]
    : ["install", "-g", "@openai/codex@latest"];
  const updateEnvironment = { ...f.env, CAPTURED_UPDATE_ENV: "retained-by-host" };
  const capabilities = Maintenance.makeProviderMaintenanceCapabilities({
    provider: driver,
    packageName: owner.packageName,
    updateExecutable: manager,
    updateArgs: args,
    updateLockKey: mode === "latest" ? "pnpm-global" : `npm-global:${f.base}`,
    env: updateEnvironment,
    latestVersion: "2.0.0",
  });
  const expectedArgs = args.map((arg) => arg === "@openai/codex@latest" ? "@openai/codex@2.0.0" : arg);
  const states = [];
  let providers = options.initialProviders ?? [runnerProvider()];
  const refreshes = [];
  const manifest = {
    version: 1,
    currentModels: {},
    compatibility: [{
      driver, t3CodeRange: ">=0.0.0", recommendedVersion: "2.0.0",
      ranges: [{ range: ">=0.0.0", status: "supported" }],
    }],
  };
  const registry = {
    getProviders: Effect.sync(() => providers),
    refresh: () => Effect.sync(() => providers),
    refreshInstance: () => Effect.sync(() => {
      refreshes.push("pre-update");
      providers = options.preProviders ?? providers;
      return providers;
    }),
    reloadSecretsAndRefresh: () => Effect.sync(() => providers),
    refreshAfterUpdate: () => Effect.sync(() => {
      refreshes.push("post-update");
      providers = options.postProviders ?? providers.map((provider) => ({ ...provider, version: "2.0.0" }));
      return providers;
    }),
    refreshWorkspaceSnapshot: () => Effect.sync(() => providers),
    invalidateModelCaches: () => Effect.void,
    getProviderMaintenanceCapabilitiesForInstance: () => Effect.succeed(capabilities),
    setProviderMaintenanceActionState: (input) => Effect.sync(() => {
      if (input.state) states.push(input.state);
      return providers;
    }),
    streamChanges: Stream.empty,
  };
  f.respond = (command, selectedArgs) => {
    NodeAssert.equal(command, manager, "selection must retain the captured executable");
    NodeAssert.deepEqual(selectedArgs, expectedArgs);
    const spawned = f.commands.at(-1);
    NodeAssert.deepEqual(spawned.options.env, updateEnvironment);
    NodeAssert.equal(spawned.options.extendEnv, true);
    NodeAssert.equal(spawned.options.shell, false);
    f.updates.push([command, ...selectedArgs]);
    totalUpdateCommands++;
    return { stdout: "Controlled update completed\n" };
  };
  const operation = Effect.gen(function* () {
    const runner = yield* ProviderMaintenanceRunner.make().pipe(
      Effect.provideService(ProviderRegistry.ProviderRegistry, registry),
      Effect.provideService(ModelManifest.ModelManifest, {
        current: Effect.succeed(manifest), refresh: Effect.succeed(manifest),
        forceRefresh: Effect.succeed(manifest), refreshInBackground: Effect.void,
      }),
      Effect.provideService(HttpClient.HttpClient,
        HttpClient.make(() => Effect.die("No external version request is allowed"))),
      Effect.provide(ProviderLatestVersions.layer),
    );
    yield* runner.updateProvider({
      provider: driver, instanceId, ...(mode === "target" ? { targetVersion: "2.0.0" } : {}),
    });
  });
  return { f, capabilities, operation, states, manager, expectedArgs, refreshes };
}

try {
  const selectionBase = {
    mode: "target",
    packageName: "@openai/codex",
    update: {
      command: "pnpm add --global @openai/codex@latest",
      args: ["add", "--global", "@openai/codex@latest"],
      lockKey: "pnpm-global",
    },
    versionValue: "2.0.0",
  };
  const versions = [
    "0.0.0", "2.0.0", "01.002.0003", `${"9".repeat(128)}.2.3`,
    "", "latest", "v2.0.0", "2.0", "2.0.0.0", "2.0.0-beta.1", "2.0.0+build",
    " 2.0.0", "2.0.0 ", "2.0.0\t", "2.0.0\n", "2.0.0\r", "2.0.0\r\n",
    "2.0.0\u2028", "2.0.0\u2029", "\n2.0.0", "2.0.0\0", "2.\n0.0",
    "٢.٠.٠", "２.０.０", "𝟚.𝟘.𝟘", "-2.0.0", "+2.0.0", "2e0.0.0",
  ];
  await scenario("Rust update version validation matches the immutable upstream oracle", () =>
    compareUpdateActions("versions", versions.map((versionValue) => ({ ...selectionBase, versionValue }))));

  const lockKeys = [
    "npm-global:", "npm-global:/opt/node", "npm-global:\nowner", "npm-global:opaque\0suffix",
    "bun-global", "pnpm-global", "vite-plus-global", "yarn-global", "volta",
    "", "npm-global", "npm-globalx:prefix", "xnpm-global:prefix", "NPM-global:prefix",
    "bun-global:prefix", "pnpm-global:prefix", "vite-plus-global:prefix", "yarn-global:prefix",
    "volta:prefix", "xbun-global", "mise:codex", "homebrew:codex", "codex-native",
    ...["bun-global", "pnpm-global", "vite-plus-global", "yarn-global", "volta"].flatMap(
      (key) => ["\n", "\r", "\r\n", "\u2028", "\u2029"].map((suffix) => `${key}${suffix}`)),
  ];
  await scenario("Rust target eligibility matches upstream installer lock semantics", () =>
    compareUpdateActions("locks", lockKeys.map((lockKey) => ({
      ...selectionBase, update: { ...selectionBase.update, lockKey },
    }))));

  const packageVariants = [
    { packageName: null },
    { packageName: "" },
    { update: null },
    { update: { ...selectionBase.update, args: [] } },
    { update: { ...selectionBase.update, args: ["@openai/codex@next"] } },
    { update: { ...selectionBase.update, args: ["@openai/codex@1.0.0"] } },
    { update: { ...selectionBase.update, args: ["--package=@openai/codex@latest"] } },
    { update: { ...selectionBase.update, args: ["@openai/codex", "@openai/codex@latest", "@openai/codex"] } },
    { update: { ...selectionBase.update, args: ["@openai/codex@latest", "@openai/codex", "@openai/codex@latest"] } },
    { update: { ...selectionBase.update, command: "@openai/codex@latest before @openai/codex@latest after" } },
    { update: { ...selectionBase.update, command: "@openai/codex before @openai/codex@latest", args: ["@openai/codex"] } },
    { update: { ...selectionBase.update, command: "an unrelated display command" } },
    { update: { ...selectionBase.update, command: "" } },
    { packageName: "@scope/工具🚀", update: {
      command: "📦 add @scope/工具🚀@latest and @scope/工具🚀@latest",
      args: ["add", "@scope/工具🚀@latest"], lockKey: "volta",
    } },
  ];
  await scenario("Rust selection preserves first-argument and last-display-occurrence behavior", () =>
    compareUpdateActions("packages", packageVariants.map((variant) => ({ ...selectionBase, ...variant }))));

  await scenario("Rust latest selection preserves pnpm pinning and original-action fallbacks", () =>
    compareUpdateActions("latest", [
      ...[null, ...versions].map((versionValue) => ({ ...selectionBase, mode: "latest", versionValue })),
      ...lockKeys.slice(0, 23).map((lockKey) => ({
        ...selectionBase, mode: "latest", update: { ...selectionBase.update, lockKey },
      })),
      ...packageVariants.map((variant) => ({ ...selectionBase, ...variant, mode: "latest" })),
    ]));

  const verificationProvider = {
    checkedAt: "2026-11-08T12:00:01.000Z", installed: true, status: "ready",
    authStatus: "authenticated", version: "2.0.0", versionAdvisoryStatus: "current",
  };
  const preVariants = [
    [], [verificationProvider], [verificationProvider, verificationProvider],
    ...[
      { checkedAt: "2026-11-08T12:00:00.000Z" }, { installed: false },
      { status: "error" }, { versionAdvisoryStatus: null }, { versionAdvisoryStatus: "behind_latest" },
      { versionAdvisoryStatus: "unknown" }, { status: "future" }, { authStatus: "unauthenticated" },
      { version: null }, { installedVersion: null },
    ].flatMap((change) => [[{ ...verificationProvider, ...change }],
      [verificationProvider, { ...verificationProvider, ...change }],
      [{ ...verificationProvider, ...change }, verificationProvider]]),
  ];
  await scenario("Rust pre-update verdicts match immutable upstream freshness and all-provider rules", () =>
    compareVerifications("pre-update", [true, false].flatMap((advisoryEnriched) =>
      [null, "2026-11-08T12:00:00.000Z", "2026-11-08T12:00:01.000Z"].flatMap((priorCheckedAt) =>
        preVariants.map((verifiedProviders) => ({
          mode: "pre-update", advisoryEnriched, priorCheckedAt, verifiedProviders,
        }))))));

  const postVariants = [
    [], [verificationProvider], [verificationProvider, verificationProvider],
    ...[
      { installed: false }, { status: "error" }, { versionAdvisoryStatus: "behind_latest" },
      { versionAdvisoryStatus: null }, { status: "future", authStatus: "unknown" },
      { installed: false, versionAdvisoryStatus: "behind_latest" },
      ...[null, "", "1.0.0", "2.0.0", "v2.0.0", "vv2.0.0", "V2.0.0", " 2.0.0", "2.0.0\n", "🚀", "v🚀"]
        .flatMap((version) => [{ version }, { installedVersion: version }]),
      ...[null, "", "1.0.0", "2.0.0"].flatMap((version) => ["authenticated", "unauthenticated", "unknown"]
        .map((authStatus) => ({ status: "error", authStatus, version, installedVersion: "2.0.0" }))),
    ].flatMap((change) => [[{ ...verificationProvider, ...change }],
      [verificationProvider, { ...verificationProvider, ...change }]]),
  ];
  await scenario("Rust post-update verdicts match immutable upstream null, auth, prefix and priority rules", () =>
    compareVerifications("post-update", [null, "2.0.0", "", "v2.0.0", "🚀"].flatMap((targetVersion) =>
      postVariants.map((verifiedProviders) => ({ mode: "post-update", targetVersion, verifiedProviders })))));

  const freshCurrent = runnerProvider({ version: "2.0.0", checkedAt: "2026-11-08T12:00:01.000Z" });
  for (const [name, options, skips] of [
    ["fresh current version", { preProviders: [freshCurrent] }, true],
    ["unchanged checkedAt", { preProviders: [runnerProvider({ version: "2.0.0" })] }, false],
    ["missing prior provider", { initialProviders: [], preProviders: [freshCurrent] }, false],
    ["empty refresh", { preProviders: [], postProviders: [freshCurrent] }, false],
    ["another provider instance", { preProviders: [{ ...freshCurrent, instanceId: "other" }], postProviders: [freshCurrent] }, false],
    ["one stale duplicate", { preProviders: [freshCurrent, runnerProvider({ version: "2.0.0" })] }, false],
  ]) {
    await scenario(`the actual runner ${skips ? "skips" : "runs"} the update after ${name}`, async () => {
      const { f, operation, states, refreshes } = await updateRunnerFixture(`precheck-${scenarios.length}`, "latest", options);
      await run(operation, f);
      NodeAssert.equal(states.at(-1).status, "succeeded");
      NodeAssert.equal(states.at(-1).message, skips ? "Already up to date." : "Provider updated.");
      NodeAssert.equal(f.updates.length, skips ? 0 : 1);
      NodeAssert.equal(f.natives.length, skips ? 2 : 3);
      NodeAssert.deepEqual(refreshes, skips ? ["pre-update"] : ["pre-update", "post-update"]);
    });
  }

  const unverifiableMessage = "Update command completed, but T3 Code could not verify the provider version.";
  const outdatedMessage = "Update command completed, but T3 Code still detects an outdated provider version.";
  for (const [name, mode, postProviders, status, message] of [
    ["empty refreshed set", "latest", [], "unchanged", unverifiableMessage],
    ["removed installation", "latest", [runnerProvider({ installed: false })], "unchanged", unverifiableMessage],
    ["still outdated", "latest", [runnerProvider()], "unchanged", outdatedMessage],
    ["unverifiable before outdated", "latest", [runnerProvider(), runnerProvider({ installed: false })], "unchanged", unverifiableMessage],
    ["transient missing version", "latest", [runnerProvider({ version: null })], "succeeded", "Provider updated."],
    ["auth error with known version", "latest", [runnerProvider({ version: "2.0.0", status: "error", auth: { status: "unauthenticated" } })], "succeeded", "Provider updated."],
    ["auth error without reported version", "target", [runnerProvider({ version: null, installedVersion: "2.0.0", status: "error", auth: { status: "unauthenticated" } })], "unchanged", unverifiableMessage],
    ["explicitly unknown installed version", "target", [runnerProvider({ version: "2.0.0", installedVersion: null })], "unchanged", unverifiableMessage],
    ["legacy installed version fallback", "target", [runnerProvider({ version: "2.0.0" })], "succeeded", "Provider updated."],
    ["one lowercase prefix", "target", [runnerProvider({ installedVersion: "v2.0.0" })], "succeeded", "Provider updated."],
    ["two lowercase prefixes", "target", [runnerProvider({ installedVersion: "vv2.0.0" })], "unchanged", unverifiableMessage],
    ["uppercase prefix", "target", [runnerProvider({ installedVersion: "V2.0.0" })], "unchanged", unverifiableMessage],
  ]) {
    await scenario(`the actual runner preserves ${name} after exactly one controlled update`, async () => {
      const { f, operation, states, refreshes } = await updateRunnerFixture(`postcheck-${scenarios.length}`, mode, { postProviders });
      await run(operation, f);
      NodeAssert.equal(states.at(-1).status, status);
      NodeAssert.equal(states.at(-1).message, message);
      NodeAssert.equal(states.at(-1).output, "Controlled update completed");
      NodeAssert.equal(f.updates.length, 1);
      NodeAssert.equal(f.natives.length, mode === "latest" ? 3 : 2);
      NodeAssert.deepEqual(refreshes, mode === "latest" ? ["pre-update", "post-update"] : ["post-update"]);
    });
  }

  await scenario("the service retains the captured executable, environment and lock after Rust selection", async () => {
    const { f, capabilities } = await updateRunnerFixture("selection-host-fields");
    const before = structuredClone(capabilities);
    const selected = await run(Maintenance.selectProviderUpdateAction(capabilities, undefined, "2.0.0"), f);
    NodeAssert.equal(selected.executable, capabilities.update.executable);
    NodeAssert.equal(selected.lockKey, capabilities.update.lockKey);
    NodeAssert.strictEqual(selected.env, capabilities.update.env);
    NodeAssert.deepEqual(selected.args, ["add", "--global", "@openai/codex@2.0.0"]);
    NodeAssert.deepEqual(capabilities, before);
    NodeAssert.equal(f.calls.length, 0);
    NodeAssert.equal(f.natives.length, 1);
  });

  for (const mode of ["latest", "target"]) {
    await scenario(`the actual runner executes and verifies the Rust-selected ${mode} version`, async () => {
      const { f, operation, states, manager, expectedArgs } = await updateRunnerFixture(`selection-runner-${mode}`, mode);
      await run(operation, f);
      NodeAssert.equal(states.at(-1).status, "succeeded");
      NodeAssert.deepEqual(f.updates, [[manager, ...expectedArgs]]);
      NodeAssert.equal(f.natives.length, mode === "latest" ? 3 : 2);
    });
  }

  const selectionFaults = [
    ["unknown selected field", policyReply("{kind:'selected',action:null,extra:true}")],
    ["missing action", policyReply("{kind:'selected'}")],
    ["nonstrings in selected arguments", policyReply("{kind:'selected',action:{command:'bad',args:[1]}}")],
    ["forged executable", policyReply("{kind:'selected',action:{command:'bad',args:[],executable:'/unowned/updater'}}")],
    ["forged environment", policyReply("{kind:'selected',action:{command:'bad',args:[],env:{PRIVATE:'forged'}}}")],
    ["forged lock", policyReply("{kind:'selected',action:{command:'bad',args:[],lockKey:'unowned'}}")],
    ["resolution instead of selection", policyReply("{kind:'resolved',capabilities:null}")],
    ["selection requesting an observation", policyReply("{kind:'observe',request:{kind:'pnpmVersion'}}")],
    ["nonzero exit after valid selection", policyReply("{kind:'selected',action:{command:'bad',args:[]}}", "process.exit(7);")],
    ["trailing data after valid selection", policyReply("{kind:'selected',action:{command:'bad',args:[]}}", "process.stdout.write('trailing data\\n');")],
  ];
  for (const [name, source] of selectionFaults) {
    await scenario(`${name} stops the actual runner before any update or fallback`, async () => {
      const { f, operation, states } = await updateRunnerFixture(`selection-fault-${scenarios.length}`);
      const policy = await fakePolicy(`selection-policy-${scenarios.length}`, source);
      await run(operation, f, policy);
      NodeAssert.equal(states.at(-1).status, "failed");
      NodeAssert.equal(f.natives.length, 1);
      NodeAssert.equal(f.updates.length, 0);
      NodeAssert.equal(f.calls.length, 0);
    });
  }

  await scenario("interrupting selection closes the helper without an update or fallback", async () => {
    const { f, operation } = await updateRunnerFixture("selection-interruption");
    const policy = await fakePolicy("selection-hang", "process.stdin.resume();setInterval(()=>{},1000);");
    await run(Effect.gen(function* () {
      const started = yield* Deferred.make();
      f.onNativeSpawn = (child) => Deferred.succeed(started, Number(child.pid));
      const fiber = yield* Effect.forkChild(operation);
      const pid = yield* Deferred.await(started);
      yield* Fiber.interrupt(fiber);
      const exit = yield* Fiber.await(fiber);
      NodeAssert.equal(Exit.isFailure(exit), true);
      NodeAssert.throws(() => process.kill(pid, 0), { code: "ESRCH" });
    }), f, policy);
    NodeAssert.equal(f.natives.length, 1);
    NodeAssert.equal(f.updates.length, 0);
    NodeAssert.equal(f.calls.length, 0);
  });

  for (const mode of ["pre-update", "post-update"]) {
    const input = mode === "pre-update"
      ? { mode, priorCheckedAt: null, advisoryEnriched: true, verifiedProviders: [verificationProvider] }
      : { mode, targetVersion: null, verifiedProviders: [verificationProvider] };
    const expected = originalVerification(input);
    await scenario(`a valid ${mode} response reaches the adapter's terminal response check`, async () => {
      const f = await fixture(`verified-control-${mode}`);
      const policy = await fakePolicy(`verified-control-${mode}`, policyReply(JSON.stringify(expected)));
      NodeAssert.deepEqual(await run(NativeMaintenancePolicy.verifyUpdate(policy, input), f, policy), expected);
      NodeAssert.equal(f.natives.length, 1);
      NodeAssert.equal(f.calls.length, 0);
    });
    const faults = [
      ["wrong operation response", "{kind:'selected',action:null}", "", "response"],
      ["observation instead of verification", "{kind:'observe',request:{kind:'pnpmVersion'}}", "", "response"],
      ["wrong verification mode", JSON.stringify(mode === "pre-update"
        ? { kind: "verified", mode: "post-update", status: "succeeded", message: "Provider updated." }
        : { kind: "verified", mode: "pre-update", alreadyCurrent: true }), "", "response"],
      ["unknown verification field", JSON.stringify({ ...expected, extra: true }), "", "response"],
      ["missing verification verdict", JSON.stringify({ kind: "verified", mode }), "", "response"],
      ["nonzero exit", JSON.stringify(expected), "process.exit(7);", "process"],
      ["stderr after verification", JSON.stringify(expected), "process.stderr.write('unexpected');", "response"],
      ["trailing verification", JSON.stringify(expected), "process.stdout.write('trailing\\n');", "response"],
      ...(mode === "pre-update"
        ? [["nonboolean already-current", JSON.stringify({ ...expected, alreadyCurrent: "true" }), "", "response"]]
        : [
          ["forged verification message", JSON.stringify({ ...expected, message: "Forged result" }), "", "response"],
          ["inconsistent verification status", JSON.stringify({ ...expected, message: unverifiableMessage }), "", "response"],
          ["invalid verification status", JSON.stringify({ ...expected, status: "running" }), "", "response"],
        ]),
    ];
    for (const [name, expression, tail, stage] of faults) {
      await scenario(`${mode} rejects ${name} without a host observation`, async () => {
        const f = await fixture(`verified-fault-${scenarios.length}`);
        const policy = await fakePolicy(`verified-fault-${scenarios.length}`, policyReply(expression, tail));
        await NodeAssert.rejects(() => run(NativeMaintenancePolicy.verifyUpdate(policy, input), f, policy),
          new RegExp(`Lust maintenance resolution failed during ${stage}`));
        NodeAssert.equal(f.natives.length, 1);
        NodeAssert.equal(f.calls.length, 0);
        NodeAssert.equal(f.updates.length, 0);
      });
    }
  }

  for (const [phase, mode, nativeCall, updateCount] of [
    ["pre-update", "latest", 2, 0],
    ["post-update", "latest", 3, 1],
    ["post-update", "target", 2, 1],
  ]) {
    for (const [name, source, stage] of [
      ["failed helper", "process.stdin.once('data',()=>process.exit(9));", "process"],
      ["invalid helper verdict", policyReply("{kind:'verified',mode:'post-update',status:'running',message:'invalid'}"), "response"],
    ]) {
      await scenario(`a ${name} at ${mode} ${phase} records failure without retrying the updater`, async () => {
        const { f, operation, states } = await updateRunnerFixture(`runner-verification-fault-${scenarios.length}`, mode);
        const policy = await fakePolicy(`runner-verification-fault-${scenarios.length}`, source);
        f.policyOverrides = new Map([[nativeCall, policy]]);
        await run(operation, f);
        NodeAssert.equal(states.at(-1).status, "failed");
        NodeAssert.match(states.at(-1).message, new RegExp(`Lust maintenance resolution failed during ${stage}`));
        NodeAssert.equal(states.at(-1).output, null);
        NodeAssert.equal(f.natives.length, nativeCall);
        NodeAssert.equal(f.updates.length, updateCount);
        for (const pid of f.natives) NodeAssert.throws(() => process.kill(pid, 0), { code: "ESRCH" });
      });
    }
    await scenario(`interrupting ${mode} ${phase} closes the helper without retrying the updater`, async () => {
      const { f, operation } = await updateRunnerFixture(`runner-verification-interrupt-${scenarios.length}`, mode);
      const policy = await fakePolicy(`runner-verification-interrupt-${scenarios.length}`, "process.stdin.resume();setInterval(()=>{},1000);");
      f.policyOverrides = new Map([[nativeCall, policy]]);
      await run(Effect.gen(function* () {
        const started = yield* Deferred.make();
        f.onNativeSpawn = (child) => f.natives.length === nativeCall
          ? Deferred.succeed(started, Number(child.pid)) : Effect.void;
        const fiber = yield* Effect.forkChild(operation);
        const pid = yield* Deferred.await(started);
        yield* Fiber.interrupt(fiber);
        NodeAssert.equal(Exit.isFailure(yield* Fiber.await(fiber)), true);
        NodeAssert.throws(() => process.kill(pid, 0), { code: "ESRCH" });
      }), f);
      NodeAssert.equal(f.natives.length, nativeCall);
      NodeAssert.equal(f.updates.length, updateCount);
    });
  }

  await scenario("explicit upstream execution retains pre-update and post-update behavior without a helper", async () => {
    const pre = await updateRunnerFixture("upstream-pre-verification", "latest", { preProviders: [freshCurrent] });
    await run(pre.operation, pre.f, null);
    NodeAssert.equal(pre.states.at(-1).message, "Already up to date.");
    NodeAssert.equal(pre.f.natives.length, 0);
    NodeAssert.equal(pre.f.updates.length, 0);
    const post = await updateRunnerFixture("upstream-post-verification", "target", {
      postProviders: [runnerProvider({ version: "2.0.0", installedVersion: null })],
    });
    await run(post.operation, post.f, null);
    NodeAssert.equal(post.states.at(-1).status, "unchanged");
    NodeAssert.equal(post.states.at(-1).message, unverifiableMessage);
    NodeAssert.equal(post.f.natives.length, 0);
    NodeAssert.equal(post.f.updates.length, 1);
  });

  for (const [name, implementation, platform, message] of [
    ["invalid implementation setting", "invalid", "linux", /T3_CODE_LS_IMPLEMENTATION must be upstream or native/],
    ["forced native on an unqualified host", "native", "darwin", /native implementation is not qualified for this host/],
  ]) {
    await scenario(`cached capabilities with ${name} publish a failed state before starting any child`, async () => {
      const { f, operation, states, refreshes } = await updateRunnerFixture(`runner-configuration-${scenarios.length}`);
      f.env.T3_CODE_LS_IMPLEMENTATION = implementation;
      f.platform = platform;
      f.useDefaultNativeSelection = true;
      await run(operation, f);
      const activeStates = states.filter((state) => state.status !== "queued");
      NodeAssert.deepEqual(activeStates.map((state) => state.status), ["running", "failed"]);
      NodeAssert.equal(activeStates[0].message, "Checking for the latest version");
      const failed = states.at(-1);
      NodeAssert.match(failed.message, message);
      NodeAssert.equal(failed.startedAt, activeStates[0].startedAt);
      NodeAssert.equal(typeof failed.startedAt, "string");
      NodeAssert.equal(typeof failed.finishedAt, "string");
      NodeAssert.ok(Number.isFinite(Date.parse(failed.startedAt)));
      NodeAssert.ok(Number.isFinite(Date.parse(failed.finishedAt)));
      NodeAssert.equal(failed.output, null);
      NodeAssert.equal(f.natives.length, 0);
      NodeAssert.equal(f.updates.length, 0);
      NodeAssert.equal(f.calls.length, 0);
      NodeAssert.deepEqual(refreshes, []);
    });
  }

  await scenario(
    "the selected executable resolves the actual shared service through one isolated process",
    async () => {
      const f = await fixture("npm", "node/lib/node_modules/@openai/codex/bin/codex.js");
      const cap = await run(full(f, { ...definition, mise: undefined }), f);
      NodeAssert.equal(
        cap.update.lockKey,
        `npm-global:${Maintenance.normalizeCommandPath(NodePath.join(f.base, "node"))}`,
      );
      NodeAssert.deepEqual(cap.update.args, [
        "install",
        "-g",
        "--prefix",
        NodePath.join(f.base, "node"),
        "--allow-scripts=@openai/codex",
        "@openai/codex@latest",
      ]);
      NodeAssert.deepEqual(cap.update.env, f.env);
      NodeAssert.equal(f.natives.length, 1);
      NodeAssert.equal(f.calls.length, 0);
    },
  );

  await scenario(
    "native command-path callbacks and updater-only environment survive the adapter",
    async () => {
      const f = await fixture("native", "owned/packages/standalone/bin/codex");
      const cap = await run(
        full(f, {
          ...definition,
          mise: undefined,
          nativeUpdate: {
            args: ["update"],
            requiresCommandPath: true,
            isCommandPath: (p) => p.includes("/packages/standalone/"),
            env: () => ({ CODEX_HOME: NodePath.join(f.base, "owned") }),
          },
        }),
        f,
      );
      NodeAssert.equal(cap.update.executable, f.command);
      NodeAssert.deepEqual(cap.update.env, { CODEX_HOME: NodePath.join(f.base, "owned") });
      NodeAssert.equal(cap.update.lockKey, "codex-native");
    },
  );

  await scenario(
    "unproven installations remain manual instead of running a native updater",
    async () => {
      const f = await fixture("manual", "project/node_modules/@unrelated/tool/bin/codex");
      const cap = await run(
        full(f, { ...definition, mise: undefined, nativeUpdate: { args: ["update"] } }),
        f,
      );
      NodeAssert.equal(cap.update, null);
      NodeAssert.equal(cap.installPath, f.command);
      NodeAssert.equal(f.calls.length, 0);
    },
  );

  await scenario(
    "pnpm script approval and deferred manager-specific release lookup are retained",
    async () => {
      const f = await fixture("pnpm", ".local/share/pnpm/codex");
      await writeExecutable(NodePath.join(f.base, "bin", "pnpm"));
      await run(
        Effect.gen(function* () {
          const cap = yield* full(f, { ...definition, mise: undefined });
          NodeAssert.deepEqual(cap.update.args, [
            "add",
            "-g",
            "--allow-build=@openai/codex",
            "@openai/codex@latest",
          ]);
          NodeAssert.equal(f.calls.length, 1);
          NodeAssert.equal(yield* cap.lookupLatestVersion, "2.0.0");
          NodeAssert.deepEqual(f.calls[1].slice(1), [
            "outdated",
            "-g",
            "--reporter=silent",
            "--format",
            "json",
            "@openai/codex",
          ]);
        }),
        f,
      );
    },
  );

  await scenario(
    "an active latest mise shim upgrades its verified owner and defers latest lookup",
    async () => {
      const f = await miseFixture("mise-shim");
      await run(
        Effect.gen(function* () {
          const cap = yield* full(f);
          NodeAssert.deepEqual(cap.update.args, ["upgrade", "codex"]);
          NodeAssert.equal(cap.update.executable, f.mise);
          NodeAssert.deepEqual(cap.update.env, f.env);
          NodeAssert.equal(cap.latestVersion, null);
          NodeAssert.equal(f.calls.length, 2);
          NodeAssert.equal(cap.latestVersionCacheKey.includes(f.env.PRIVATE_INSTALL_TOKEN), false);
          NodeAssert.equal(yield* cap.lookupLatestVersion, "2.0.0");
          NodeAssert.deepEqual(
            f.calls.map((call) => call.slice(1)),
            [
              ["which", "codex"],
              ["ls", "--json", "--installed", "codex"],
              ["latest", "codex"],
            ],
          );
          NodeAssert.equal(f.natives.length, 1);
        }),
        f,
      );
    },
  );

  await scenario(
    "a fixed mise path stays manual even when its installed version is active latest",
    async () => {
      const f = await miseFixture("mise-fixed");
      delete f.shim;
      f.context = { ...f.context, resolvedCommandPath: f.command, realCommandPath: f.command };
      const cap = await run(full(f), f);
      NodeAssert.equal(cap.update, null);
      NodeAssert.equal(cap.installPath, f.command);
      NodeAssert.equal(f.calls.length, 2);
    },
  );

  await scenario(
    "a moving mise latest link remains eligible after canonical path resolution",
    async () => {
      const f = await miseFixture("mise-link");
      const link = NodePath.join(NodePath.dirname(f.install), "latest");
      await NodeFSP.symlink(f.install, link);
      f.shim = NodePath.join(link, "bin", "codex");
      const cap = await run(full(f), f);
      NodeAssert.equal(cap.update.lockKey, "mise");
      NodeAssert.equal(cap.update.executable, f.mise);
    },
  );

  await scenario("pinned and explicitly non-upgradeable mise installs remain manual", async () => {
    for (const pin of [true, false]) {
      const f = await miseFixture(`mise-pinned-${pin}`);
      f.requestedVersion = pin ? "1.0.0" : "latest";
      const cap = await run(full(f, { ...definition, mise: { tool: "codex", upgrade: pin } }), f);
      NodeAssert.equal(cap.update, null);
      NodeAssert.equal(cap.installPath, f.command);
    }
  });

  await scenario(
    "the direct mise helper used by Grok and Muse selects the native policy",
    async () => {
      const f = await miseFixture("direct-mise");
      const cap = await run(
        Maintenance.resolveMiseProviderMaintenance(owner, { tool: "codex" }, f.context),
        f,
      );
      NodeAssert.equal(cap.update.lockKey, "mise");
      NodeAssert.equal(f.natives.length, 1);
    },
  );

  await scenario("verified Homebrew ownership reaches its own lazy version channel", async () => {
    const f = await fixture("brew", "Caskroom/codex/1.0.0/codex");
    const brew = await writeExecutable(NodePath.join(f.base, "bin", "brew"));
    await run(
      Effect.gen(function* () {
        const cap = yield* Maintenance.resolveHomebrewProviderMaintenance(
          owner,
          { kind: "cask", name: "codex", prefix: f.base },
          { ...f.context, env: f.env },
        );
        NodeAssert.equal(cap.update.executable, brew);
        NodeAssert.equal(cap.update.command, "brew upgrade --cask codex");
        NodeAssert.equal(cap.update.env, undefined);
        NodeAssert.equal(f.calls.length, 1);
        NodeAssert.equal(yield* cap.lookupLatestVersion, "2.0.0");
      }),
      f,
    );
  });

  await scenario("a mismatched Homebrew prefix cannot authorize an update", async () => {
    const f = await fixture("brew-mismatch", "Caskroom/codex/1.0.0/codex");
    await writeExecutable(NodePath.join(f.base, "bin", "brew"));
    f.respond = () => ({ stdout: "/different/prefix\n" });
    const cap = await run(full(f, { ...definition, mise: undefined }), f);
    NodeAssert.equal(cap.update, null);
    NodeAssert.equal(f.calls.length, 1);
  });

  await scenario(
    "cached resolution and explicit refresh preserve the production cache boundary",
    async () => {
      const f = await miseFixture("cache");
      await run(
        Effect.gen(function* () {
          const resolve = yield* Maintenance.makeCachedProviderMaintenanceResolution(full(f));
          const initial = yield* resolve();
          f.requestedVersion = "1.0.0";
          NodeAssert.strictEqual(yield* resolve(), initial);
          NodeAssert.equal(f.natives.length, 1);
          NodeAssert.equal((yield* resolve({ fresh: true })).update, null);
          NodeAssert.equal(f.natives.length, 2);
        }),
        f,
      );
    },
  );

  for (const changed of [false, true]) {
    await scenario(
      changed
        ? "the actual runner refuses ownership that changes after the advisory"
        : "the actual runner executes and verifies the native-selected owner with a controlled update child",
      async () => {
        const f = await miseFixture(`runner-${changed}`);
        f.allowUpdate = true;
        const states = [];
        let providers = [
          {
            instanceId,
            driver,
            enabled: true,
            installed: true,
            version: "1.0.0",
            status: "ready",
            auth: { status: "authenticated" },
            checkedAt: "2026-11-08T12:00:00.000Z",
            models: [],
            slashCommands: [],
            skills: [],
          },
        ];
        const manifest = {
          version: 1,
          currentModels: {},
          compatibility: [
            { driver, t3CodeRange: ">=0.0.0", ranges: [{ range: ">=0.0.0", status: "supported" }] },
          ],
        };
        await run(
          Effect.gen(function* () {
            const resolve = yield* Maintenance.makeCachedProviderMaintenanceResolution(full(f));
            yield* resolve();
            if (changed) f.requestedVersion = "1.0.0";
            const registry = {
              getProviders: Effect.sync(() => providers),
              refresh: () => Effect.sync(() => providers),
              refreshInstance: () => Effect.sync(() => providers),
              reloadSecretsAndRefresh: () => Effect.sync(() => providers),
              refreshAfterUpdate: () =>
                Effect.sync(() => {
                  providers = providers.map((p) => ({ ...p, version: "2.0.0" }));
                  return providers;
                }),
              refreshWorkspaceSnapshot: () => Effect.sync(() => providers),
              invalidateModelCaches: () => Effect.void,
              getProviderMaintenanceCapabilitiesForInstance: (_id, _driver, options) =>
                resolve(options),
              setProviderMaintenanceActionState: (input) =>
                Effect.sync(() => {
                  if (input.state) states.push(input.state);
                  return providers;
                }),
              streamChanges: Stream.empty,
            };
            const runner = yield* ProviderMaintenanceRunner.make().pipe(
              Effect.provideService(ProviderRegistry.ProviderRegistry, registry),
              Effect.provideService(ModelManifest.ModelManifest, {
                current: Effect.succeed(manifest),
                refresh: Effect.succeed(manifest),
                forceRefresh: Effect.succeed(manifest),
                refreshInBackground: Effect.void,
              }),
              Effect.provideService(
                HttpClient.HttpClient,
                HttpClient.make(() => Effect.die("No external version request is allowed")),
              ),
              Effect.provide(ProviderLatestVersions.layer),
            );
            yield* runner.updateProvider({ provider: driver, instanceId });
          }),
          f,
        );
        NodeAssert.equal(states.at(-1).status, changed ? "failed" : "succeeded");
        NodeAssert.equal(f.updates.length, changed ? 0 : 1);
        if (changed)
          NodeAssert.equal(
            states.at(-1).message,
            "Provider installation changed. Refresh and try again.",
          );
        else NodeAssert.deepEqual(f.updates[0], [f.mise, "upgrade", "codex"]);
        NodeAssert.equal(f.natives.length, changed ? 2 : 6);
      },
    );
  }

  await scenario(
    "invalid native configuration fails before any package-manager command",
    async () => {
      const f = await fixture("bad-config");
      for (const path of ["", "relative-policy", `${executable}\0tail`]) {
        await NodeAssert.rejects(
          () => run(direct(f), f, path),
          /Lust maintenance resolution failed during configuration/,
        );
      }
      NodeAssert.equal(f.calls.length, 0);
      NodeAssert.equal(f.natives.length, 0);
    },
  );

  await scenario(
    "a native failure during the runner's fresh check cannot fall back to TypeScript",
    async () => {
      const f = await miseFixture("runner-failure");
      const bad = await fakePolicy(
        "bad-refresh",
        "process.stdin.once('data',()=>{process.stderr.write('fixture failure');process.exit(2)});",
      );
      const states = [];
      await run(
        Effect.gen(function* () {
          const cached = yield* full(f);
          const registry = {
            getProviders: Effect.succeed([]),
            refreshInstance: () => Effect.succeed([]),
            getProviderMaintenanceCapabilitiesForInstance: (_id, _driver, options) =>
              options?.fresh
                ? full(f).pipe(
                    Effect.provideService(HostProcess.Environment, {
                      ...f.env,
                      T3_CODE_LS_MAINTENANCE_EXECUTABLE: bad,
                    }),
                  )
                : Effect.succeed(cached),
            setProviderMaintenanceActionState: ({ state }) =>
              Effect.sync(() => {
                if (state) states.push(state);
                return [];
              }),
          };
          const runner = yield* ProviderMaintenanceRunner.make().pipe(
            Effect.provideService(ProviderRegistry.ProviderRegistry, registry),
            Effect.provideService(ModelManifest.ModelManifest, {
              current: Effect.succeed({ version: 1, currentModels: {}, compatibility: [] }),
            }),
            Effect.provideService(
              HttpClient.HttpClient,
              HttpClient.make(() => Effect.die("No external request")),
            ),
            Effect.provide(ProviderLatestVersions.layer),
          );
          yield* runner.updateProvider({ provider: driver, instanceId });
        }),
        f,
      );
      NodeAssert.equal(states.at(-1).status, "failed");
      NodeAssert.match(states.at(-1).message, /Lust maintenance resolution failed/);
      NodeAssert.equal(f.updates.length, 0);
    },
  );

  const faults = [
    [
      "wrong owner",
      policyReply(
        "{kind:'resolved',capabilities:{provider:'forged',packageName:q.owner.packageName,update:null}}",
      ),
    ],
    [
      "unknown response field",
      policyReply(
        "{kind:'resolved',capabilities:{provider:q.owner.provider,packageName:q.owner.packageName,update:null,extra:true}}",
      ),
    ],
    [
      "unbound observation path",
      policyReply("{kind:'observe',request:{kind:'homebrewExecutable',prefix:'/unrelated'}}"),
    ],
    [
      "forged update executable",
      policyReply(
        "{kind:'resolved',capabilities:{provider:q.owner.provider,packageName:q.owner.packageName,update:{command:'/unrelated update',executable:'/unrelated',args:['update'],lockKey:'codex-native',env:'none'}}}",
      ),
    ],
    [
      "forged updater arguments",
      policyReply(
        "{kind:'resolved',capabilities:{provider:q.owner.provider,packageName:q.owner.packageName,update:{command:q.context.resolvedCommandPath+' --unsafe',executable:q.context.resolvedCommandPath,args:['--unsafe'],lockKey:'codex-native',env:'none'}}}",
      ),
    ],
    [
      "forged updater environment",
      policyReply(
        "{kind:'resolved',capabilities:{provider:q.owner.provider,packageName:q.owner.packageName,update:{command:q.context.resolvedCommandPath+' update',executable:q.context.resolvedCommandPath,args:['update'],lockKey:'codex-native',env:'context'}}}",
      ),
    ],
    [
      "forged lazy lookup",
      policyReply(
        "{kind:'resolved',capabilities:{provider:q.owner.provider,packageName:q.owner.packageName,update:null,latestVersion:null,latestLookup:{kind:'mise',executable:'/unrelated',tool:'codex'}}}",
      ),
    ],
    [
      "oversized policy output",
      "process.stdin.once('data',()=>process.stdout.write(Buffer.alloc(1048577,32),()=>process.exit(0)));",
    ],
    [
      "stderr after response",
      policyReply(
        "{kind:'resolved',capabilities:{provider:q.owner.provider,packageName:q.owner.packageName,update:null}}",
        "process.stderr.write('extra');",
      ),
    ],
    [
      "trailing response",
      "process.stdin.once('data',b=>{const q=JSON.parse(b);const r=JSON.stringify({version:1,ok:{kind:'resolved',capabilities:{provider:q.owner.provider,packageName:q.owner.packageName,update:null}}});process.stdout.write(r+'\\n'+r+'\\n',()=>process.exit(0));});",
    ],
    [
      "invalid UTF-8",
      "process.stdin.once('data',()=>process.stdout.write(Buffer.from([255,10]),()=>process.exit(0)));",
    ],
  ];
  for (const [name, source] of faults) {
    await scenario(`${name} cannot authorize maintenance or execute a host probe`, async () => {
      const f = await fixture(`fault-${scenarios.length}`);
      const policy = await fakePolicy(`policy-${scenarios.length}`, source);
      await NodeAssert.rejects(
        () =>
          run(
            direct(f, { ...definition, mise: undefined, nativeUpdate: { args: ["update"] } }),
            f,
            policy,
          ),
        /Lust maintenance resolution failed/,
      );
      NodeAssert.equal(f.calls.length, 0);
      NodeAssert.equal(f.updates.length, 0);
    });
  }

  await scenario(
    "an oversized resolution request fails before sending a host observation",
    async () => {
      const f = await fixture("oversized-input");
      f.context.realCommandPath = `/${"x".repeat(1_048_576)}`;
      await NodeAssert.rejects(
        () => run(direct(f), f),
        /Lust maintenance resolution failed during request/,
      );
      NodeAssert.equal(f.calls.length, 0);
      NodeAssert.equal(f.updates.length, 0);
    },
  );

  await scenario(
    "a wrapper that grows past its stat bound fails without permitting a native updater",
    async () => {
      const f = await fixture("growing-wrapper");
      await NodeFSP.writeFile(f.command, `#!/bin/sh\nmise x codex\n#${"x".repeat(17 * 1024)}`);
      f.wrapFileSystem = (actual) => ({
        ...actual,
        stat: (target) =>
          actual
            .stat(target)
            .pipe(Effect.map((info) => (target === f.command ? { ...info, size: 1n } : info))),
      });
      await NodeAssert.rejects(
        () =>
          run(direct(f, { ...definition, mise: undefined, nativeUpdate: { args: ["update"] } }), f),
        /Lust maintenance resolution failed during observation/,
      );
      NodeAssert.equal(f.calls.length, 0);
      NodeAssert.equal(f.updates.length, 0);
    },
  );

  await scenario("interruption closes the owned policy process before returning", async () => {
    const f = await fixture("interrupt");
    const policy = await fakePolicy("hang", "process.stdin.resume();setInterval(()=>{},1000);");
    await run(
      Effect.gen(function* () {
        const started = yield* Deferred.make();
        f.onNativeSpawn = (child) => Deferred.succeed(started, Number(child.pid));
        const fiber = yield* Effect.forkChild(direct(f));
        const pid = yield* Deferred.await(started);
        yield* Fiber.interrupt(fiber);
        const exit = yield* Fiber.await(fiber);
        NodeAssert.equal(Exit.isFailure(exit), true);
        NodeAssert.throws(() => process.kill(pid, 0), { code: "ESRCH" });
      }),
      f,
      policy,
    );
    NodeAssert.equal(f.calls.length, 0);
  });

  for (const framing of ["complete", "missing", "partial"]) {
    await scenario(
      `zero exit before gated ${framing} stdout preserves response framing`,
      async () => {
        const tasks = [];
        const flush = () => {
          while (tasks.length > 0) tasks.shift()();
        };
        const scheduler = {
          executionMode: "sync",
          shouldYield: () => false,
          makeDispatcher: () => ({ scheduleTask: (task) => tasks.push(task), flush }),
        };
        const requestWritten = Deferred.makeUnsafe();
        const releaseOutput = Deferred.makeUnsafe();
        let exitObserved = false;
        const capabilities = { ...owner, update: null };
        const line = JSON.stringify({ version: 1, ok: { kind: "resolved", capabilities } });
        const output = framing === "complete" ? `${line}\n` : framing === "partial" ? line : "";
        const spawner = ChildProcessSpawner.make(() =>
          Effect.succeed(
            ChildProcessSpawner.makeHandle({
              pid: ChildProcessSpawner.ProcessId(1),
              exitCode: Deferred.await(requestWritten).pipe(
                Effect.andThen(
                  Effect.sync(() => {
                    exitObserved = true;
                  }),
                ),
                Effect.as(ChildProcessSpawner.ExitCode(0)),
              ),
              isRunning: Effect.succeed(false),
              kill: () => Effect.void,
              unref: Effect.succeed(Effect.void),
              stdin: Sink.forEach(() => Deferred.succeed(requestWritten, undefined)),
              stdout: Stream.unwrap(
                Deferred.await(releaseOutput).pipe(
                  Effect.as(output === "" ? Stream.empty : Stream.encodeText(Stream.make(output))),
                ),
              ),
              stderr: Stream.empty,
              all: Stream.empty,
              getInputFd: () => Sink.drain,
              getOutputFd: () => Stream.empty,
            }),
          ),
        );
        const operation = NativeMaintenancePolicy.resolve(
          executable,
          {
            mode: "package",
            owner,
            context: null,
            definition: { mise: null, native: null },
            homebrew: null,
          },
          () => Effect.die("A resolved policy must not request observations"),
        ).pipe(
          Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
          Effect.scoped,
        );
        const fiber = Effect.runForkWith(Context.make(Scheduler.Scheduler, scheduler))(operation);
        // Drain runnable fibers completely while stdout remains gated, with no timing waits.
        flush();
        NodeAssert.equal(exitObserved, true);
        NodeAssert.equal(fiber.pollUnsafe(), undefined);
        Effect.runSync(Deferred.succeed(releaseOutput, undefined));
        flush();
        const result = fiber.pollUnsafe();
        NodeAssert.notEqual(result, undefined);
        if (framing === "complete") {
          NodeAssert.equal(Exit.isSuccess(result), true);
          NodeAssert.deepEqual(result.value.capabilities, capabilities);
        } else {
          NodeAssert.equal(Exit.isFailure(result), true);
          NodeAssert.equal(result.cause.reasons[0].error.stage, "process");
        }
      },
    );
  }

  const sourceFiles = [
    "packages/provider-core/src/server/NativeMaintenancePolicy.ts",
    "packages/provider-core/src/server/maintenanceResolver.ts",
    "apps/server/src/provider/providerMaintenanceRunner.ts",
    "tools/check-maintenance-adapter.mjs",
  ];
  const hashes = Object.fromEntries(
    await Promise.all(
      sourceFiles.map(async (file) => [
        file,
        NodeCrypto.createHash("sha256")
          .update(await NodeFSP.readFile(NodePath.join(root, file)))
          .digest("hex"),
      ]),
    ),
  );
  const proof = {
    schema_version: 1,
    status: "passed",
    sourceCommit: process.env.SOURCE_REVISION ?? null,
    executableSha256: NodeCrypto.createHash("sha256")
      .update(await NodeFSP.readFile(executable))
      .digest("hex"),
    sourceHashes: hashes,
    upstreamOracleSha256: NodeCrypto.createHash("sha256").update(oracleSource).digest("hex"),
    verificationOracleSha256: NodeCrypto.createHash("sha256").update(verificationOracleSource).digest("hex"),
    passed: scenarios.length,
    updateActionComparisons: totalUpdateActionComparisons,
    verificationComparisons: totalVerificationComparisons,
    nativeProcesses: totalNativeProcesses,
    probeCommands: totalProbeCommands,
    controlledUpdateCommands: totalUpdateCommands,
    checks: scenarios,
    boundaries: [
      "actual shared maintenance resolver and production update runner with a selected native policy",
      "isolated filesystem fixtures and controlled package-manager outputs; no real install, upgrade, provider turn or external request",
      "host owns filesystem/process access, callback predicates, semver facts, lazy lookups and cache hashing",
      "native ownership, update-action and verification decisions do not port the remaining provider drivers, server, storage or clients",
    ],
  };
  if (proofPath) {
    await NodeFSP.mkdir(NodePath.dirname(NodePath.resolve(proofPath)), { recursive: true });
    await NodeFSP.writeFile(proofPath, `${JSON.stringify(proof, null, 2)}\n`);
  }
  process.stdout.write(`${JSON.stringify(proof)}\n`);
} finally {
  await NodeFSP.rm(temporary, { recursive: true, force: true });
}
