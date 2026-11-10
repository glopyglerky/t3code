#!/usr/bin/env node
import * as NodeAssert from "node:assert/strict";
import * as NodeCrypto from "node:crypto";
import * as NodeFSP from "node:fs/promises";
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
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";
import * as HttpClient from "effect/http/HttpClient";
import * as Maintenance from "../packages/provider-core/src/server/maintenanceResolver.ts";
import * as NativeMaintenancePolicy from "../packages/provider-core/src/server/NativeMaintenancePolicy.ts";
import * as ProviderLatestVersions from "../packages/provider-core/src/server/ProviderLatestVersions.ts";
import * as ProviderMaintenanceRunner from "../apps/server/src/provider/providerMaintenanceRunner.ts";
import * as ProviderRegistry from "../apps/server/src/provider/ProviderRegistry.ts";
import * as ModelManifest from "../apps/server/src/provider/ModelManifest.ts";

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
    ...(selected === undefined ? {} : { T3_CODE_LS_MAINTENANCE_EXECUTABLE: selected }),
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
            const child = yield* actual.spawn(command);
            f.natives.push(Number(child.pid));
            if (f.onNativeSpawn) yield* f.onNativeSpawn(child);
            return child;
          }
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

try {
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
        NodeAssert.equal(f.natives.length, changed ? 2 : 3);
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
    passed: scenarios.length,
    nativeProcesses: totalNativeProcesses,
    probeCommands: totalProbeCommands,
    controlledUpdateCommands: totalUpdateCommands,
    checks: scenarios,
    boundaries: [
      "actual shared maintenance resolver and production update runner with a selected native policy",
      "isolated filesystem fixtures and controlled package-manager outputs; no real install, upgrade, provider turn or external request",
      "host owns filesystem/process access, callback predicates, semver facts, lazy lookups and cache hashing",
      "native ownership decisions do not port the remaining provider drivers, server, storage or clients",
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
