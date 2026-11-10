import * as NodeAssert from "node:assert/strict";
import * as NodeBuffer from "node:buffer";
import * as NodeChildProcess from "node:child_process";
import * as NodeCrypto from "node:crypto";
import * as NodeFSP from "node:fs/promises";
import * as NodeModule from "node:module";
import * as NodePath from "node:path";
import * as NodeReadline from "node:readline";
import * as NodeURL from "node:url";
import * as Cache from "effect/Cache";
import * as Config from "effect/Config";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpClientRequest from "effect/http/HttpClientRequest";
import * as HttpClientResponse from "effect/http/HttpClientResponse";
import * as ChildProcess from "effect/process/ChildProcess";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";
import { upstreamSource } from "./upstream-source.mjs";

function immutableModule(path, imports, names) {
  const source = NodeModule.stripTypeScriptTypes(upstreamSource(path), { mode: "strip" })
    .replace(/^import\s[\s\S]*?;\s*$/gm, "")
    .replace(/^export (?=(?:function|const|class) )/gm, "");
  return new Function(...Object.keys(imports), `${source}\nreturn {${names.join(",")}};`)(
    ...Object.values(imports),
  );
}

const semver = immutableModule("packages/shared/src/semver.ts", {}, [
  "compareSemverVersions",
  "satisfiesSemverRange",
]);
const { collectUint8StreamText } = immutableModule(
  "packages/provider-core/src/server/collectStreamText.ts",
  { Effect, Stream, NodeBuffer },
  ["collectUint8StreamText"],
);
const copy = (value) => structuredClone(value);
const clean = (value) => JSON.parse(JSON.stringify(value));
const packageName = "@openai/codex";
const env = { PATH: "/fixture/bin", HOME: "/fixture/home", PRIVATE_VALUE: "host-only-fixture" };

function base(label, changes = {}) {
  return {
    label,
    mode: "package",
    owner: { provider: "codex", packageName },
    context: {
      binaryPath: "codex",
      resolvedCommandPath: "/fixture/bin/codex",
      realCommandPath: "/fixture/unknown/codex",
      env: { ...env },
      platform: "linux",
    },
    hostPlatform: "linux",
    definition: { mise: undefined, nativeUpdate: null },
    homebrew: null,
    lookups: {},
    files: {},
    exists: {},
    realPaths: {},
    probes: {},
    ...changes,
  };
}

export const maintenanceCases = [];
function add(fixture) {
  maintenanceCases.push(fixture);
  return fixture;
}
function commandPath(fixture, resolved, real = resolved) {
  fixture.context.resolvedCommandPath = resolved;
  fixture.context.realCommandPath = real;
  return fixture;
}
function probe(fixture, command, args, stdout, exitCode = 0) {
  fixture.probes[JSON.stringify([command, args])] = { stdout, exitCode };
  return fixture;
}
function nativeDefinition(fixture, changes = {}) {
  fixture.definition.nativeUpdate = {
    args: ["update"],
    matches: [],
    requiresCommandPath: false,
    ...changes,
  };
  return fixture;
}

function world(fixture) {
  const trace = [];
  const path = fixture.hostPlatform === "win32" ? NodePath.win32 : NodePath.posix;
  const host = {
    Platform: Context.Reference(`maintenance-platform:${fixture.label}`, {
      defaultValue: () => fixture.commandPlatform ?? fixture.hostPlatform,
    }),
    Environment: Context.Reference(`maintenance-env:${fixture.label}`, {
      defaultValue: () => fixture.context?.env ?? {},
    }),
  };
  const lookup = (name, options) => {
    trace.push(["lookup", name, clean(options.env)]);
    return fixture.lookups[name] ?? null;
  };
  const exists = (name) => {
    trace.push(["exists", name]);
    return fixture.exists[name] === true;
  };
  const realPath = (name) => {
    trace.push(["realPath", name]);
    return fixture.realPaths[name] ?? name;
  };
  const stat = (name) => {
    trace.push(["stat", name]);
    const file = fixture.files[name];
    return file === undefined ? null : (file.size ?? Buffer.byteLength(file.text ?? ""));
  };
  const read = (name) => {
    trace.push(["read", name]);
    return fixture.files[name]?.text ?? "";
  };
  const run = (command, args, commandEnv) => {
    trace.push(["probe", command, [...args], clean(commandEnv)]);
    return fixture.probes[JSON.stringify([command, args])] ?? { stdout: "", exitCode: 1 };
  };
  const fs = FileSystem.makeNoop({
    stat: (name) => {
      const size = stat(name);
      return size === null
        ? Effect.fail(new Error("fixture missing file"))
        : Effect.succeed({ size });
    },
    readFileString: (name) => Effect.succeed(read(name)),
    realPath: (name) => Effect.succeed(realPath(name)),
    exists: (name) => Effect.succeed(exists(name)),
  });
  const spawner = ChildProcessSpawner.make((command) => {
    const answer = run(command.command, command.args, command.options.env);
    return Effect.succeed(
      ChildProcessSpawner.makeHandle({
        pid: ChildProcessSpawner.ProcessId(1),
        exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(answer.exitCode)),
        isRunning: Effect.succeed(false),
        kill: () => Effect.void,
        unref: Effect.succeed(Effect.void),
        stdin: Sink.drain,
        stdout: Stream.encodeText(Stream.make(answer.stdout)),
        stderr: Stream.empty,
        all: Stream.empty,
        getInputFd: () => Sink.drain,
        getOutputFd: () => Stream.empty,
      }),
    );
  });
  const module = immutableModule(
    "packages/provider-core/src/server/maintenanceResolver.ts",
    {
      NodeCrypto,
      ProviderDriverKind: Schema.String,
      ...semver,
      HostProcess: host,
      causeErrorTag: () => "FixtureError",
      resolveCommandPath: (name, options) => Effect.succeed(lookup(name, options)),
      resolveSpawnCommand: (command, args) => Effect.succeed({ command, args, shell: false }),
      Cache,
      Config,
      DateTime,
      Duration,
      Effect,
      FileSystem,
      Option,
      Path,
      Schema,
      Stream,
      HttpClient,
      HttpClientRequest,
      HttpClientResponse,
      ChildProcess,
      ChildProcessSpawner,
      collectUint8StreamText,
      ProviderLatestVersions: {},
    },
    [
      "resolvePackageManagedProviderMaintenance",
      "resolveMiseProviderMaintenance",
      "resolveHomebrewProviderMaintenance",
      "resolvePnpmGlobalLatestVersion",
      "parseChocolateyLatestVersion",
      "parseHomebrewLatestVersion",
      "runProbe",
    ],
  );
  const provide = (effect) =>
    effect.pipe(
      Effect.provideService(FileSystem.FileSystem, fs),
      Effect.provideService(Path.Path, Path.make(path)),
      Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
      Effect.provideService(host.Platform, fixture.hostPlatform),
    );
  const execute = (effect) => Effect.runPromise(provide(effect));
  const boundedProbe = (command, args, ok = [0]) => {
    const answer = run(command, args, fixture.context.env);
    return ok.includes(answer.exitCode) && Buffer.byteLength(answer.stdout) <= 256 * 1024
      ? answer.stdout
      : null;
  };
  return { trace, path, lookup, exists, realPath, stat, read, boundedProbe, module, execute };
}

function definition(fixture) {
  const value = fixture.definition.nativeUpdate;
  return {
    provider: fixture.owner.provider,
    npmPackageName: fixture.owner.packageName,
    mise: fixture.definition.mise,
    nativeUpdate: value
      ? {
          args: value.args,
          requiresCommandPath: value.requiresCommandPath,
          ...(value.matches === null
            ? {}
            : { isCommandPath: (path) => value.matches.includes(path) }),
          ...(value.env === undefined ? {} : { env: () => value.env }),
        }
      : null,
  };
}

function request(fixture) {
  const native = fixture.definition.nativeUpdate;
  return {
    version: 1,
    operation: "resolve",
    mode: fixture.mode,
    owner: fixture.owner,
    context: fixture.context
      ? {
          resolvedCommandPath: fixture.context.resolvedCommandPath,
          realCommandPath: fixture.context.realCommandPath,
          platform: fixture.context.platform,
          hostPlatform: fixture.hostPlatform,
          commandPlatform: fixture.commandPlatform ?? fixture.hostPlatform,
          chocolateyInstall: fixture.context.env.ChocolateyInstall ?? null,
        }
      : null,
    definition: {
      mise: fixture.definition.mise
        ? { tool: fixture.definition.mise.tool, upgrade: fixture.definition.mise.upgrade !== false }
        : null,
      native: native
        ? {
            args: native.args,
            matchesCommandPath:
              fixture.context !== null &&
              (native.matches ?? []).some((value) =>
                [fixture.context.resolvedCommandPath, fixture.context.realCommandPath].includes(
                  value,
                ),
              ),
            requiresCommandPath: native.requiresCommandPath,
            hasEnv: native.env !== undefined,
          }
        : null,
    },
    homebrew: fixture.homebrew,
    observations: [],
  };
}

function observe(fixture, state, input, observation) {
  const context = fixture.context;
  const observed = (kind) =>
    input.observations.find((entry) => entry.request.kind === kind)?.result;
  const mise = () => observed("resolveMise") ?? context.realCommandPath;
  const nonEmpty = (text) => (text?.trim() ? text.trim() : null);
  switch (observation.kind) {
    case "wrapper": {
      const size = state.stat(context.realCommandPath);
      return size === null || size > 16 * 1024 ? null : state.read(context.realCommandPath);
    }
    case "resolveMise":
      return state.lookup("mise", { env: context.env });
    case "miseWhich": {
      const which = nonEmpty(state.boundedProbe(mise(), ["which", fixture.definition.mise.tool]));
      return which ? state.realPath(which) : null;
    }
    case "miseListing":
      return state.boundedProbe(mise(), [
        "ls",
        "--json",
        "--installed",
        fixture.definition.mise.tool,
      ]);
    case "miseInstallPaths":
      return observation.paths.map(state.realPath);
    case "miseToolPath":
      return state.realPath(observation.path);
    case "pnpmVersion": {
      const version = state.boundedProbe("pnpm", ["--version"]);
      return version !== null && semver.satisfiesSemverRange(version, ">=10.4.0");
    }
    case "voltaPackage": {
      const home = state.path.dirname(state.path.dirname(context.resolvedCommandPath));
      return state.exists(
        state.path.join(
          home,
          "tools",
          "image",
          "packages",
          ...fixture.owner.packageName.split("/"),
        ),
      );
    }
    case "npmShim": {
      const directory = state.path.dirname(context.resolvedCommandPath);
      return {
        directory,
        exists: state.exists(
          state.path.join(
            directory,
            "node_modules",
            ...fixture.owner.packageName.split("/"),
            "package.json",
          ),
        ),
      };
    }
    case "homebrewExecutable": {
      const executable = state.path.join(observation.prefix, "bin", "brew");
      return state.exists(executable) ? executable : state.lookup("brew", { env: context.env });
    }
    case "homebrewPrefix": {
      const value = nonEmpty(state.boundedProbe(observed("homebrewExecutable"), ["--prefix"]));
      return value ? state.realPath(value) : null;
    }
    default:
      throw new Error(`Unknown native observation: ${JSON.stringify(observation)}`);
  }
}

function hydrated(fixture, capabilities) {
  if (capabilities === null) return null;
  const { latestLookup, ...result } = copy(capabilities);
  if (result.update !== null) {
    const selector = result.update.env;
    delete result.update.env;
    NodeAssert.ok(["none", "context", "native"].includes(selector));
    if (selector === "context") result.update.env = fixture.context.env;
    if (selector === "native") result.update.env = fixture.definition.nativeUpdate.env;
  }
  if (latestLookup?.kind === "mise") {
    const hash = NodeCrypto.createHash("sha256");
    for (const [name, value] of Object.entries(fixture.context.env).toSorted(([a], [b]) =>
      a < b ? -1 : 1,
    )) {
      hash.update(`${name}\0${value ?? ""}\0`);
    }
    result.latestVersionCacheKey = [
      "mise",
      latestLookup.executable,
      latestLookup.tool,
      hash.digest("hex"),
    ].join("\0");
  }
  return { ...clean(result), hasLatestLookup: latestLookup !== undefined };
}

function projected(capabilities) {
  if (capabilities === null) return null;
  const { lookupLatestVersion, ...result } = capabilities;
  return { ...clean(result), hasLatestLookup: lookupLatestVersion !== undefined };
}

async function lookupLatest(fixture, state, lookup) {
  if (!lookup) return undefined;
  const module = state.module;
  switch (lookup.kind) {
    case "mise": {
      const output = await state.execute(
        module.runProbe(lookup.executable, ["latest", lookup.tool], fixture.context.env),
      );
      const version = output?.trim();
      return version && /^\S+$/.test(version) ? version : null;
    }
    case "homebrew": {
      const info = await state.execute(
        module.runProbe(lookup.executable, ["info", "--json=v2", lookup.name], fixture.context.env),
      );
      return info
        ? module.parseHomebrewLatestVersion(info, { kind: lookup.ownershipKind, name: lookup.name })
        : null;
    }
    case "pnpm":
      return state.execute(
        module.resolvePnpmGlobalLatestVersion(lookup.packageName, fixture.context.env),
      );
    case "chocolatey": {
      const output = await state.execute(
        module.runProbe(
          "choco",
          ["search", "claude-code", "--exact", "--limit-output"],
          fixture.context.env,
        ),
      );
      return output ? module.parseChocolateyLatestVersion(output, "claude-code") : null;
    }
    default:
      throw new Error(`Unknown lookup descriptor: ${JSON.stringify(lookup)}`);
  }
}

async function nativeResolve(executable, fixture, state) {
  const child = NodeChildProcess.spawn(executable, [], {
    env: {},
    stdio: ["pipe", "pipe", "pipe"],
  });
  let stderr = "";
  child.stderr.setEncoding("utf8");
  child.stderr.on("data", (chunk) => {
    stderr += chunk;
  });
  const closed = new Promise((resolve, reject) => {
    child.once("error", reject);
    child.once("close", (code, signal) => resolve({ code, signal }));
  });
  const lines = NodeReadline.createInterface({ input: child.stdout, crlfDelay: Infinity });
  const output = lines[Symbol.asyncIterator]();
  const input = request(fixture);
  let capabilities;
  try {
    for (let step = 0; step < 32; step++) {
      child.stdin.write(`${JSON.stringify(input)}\n`);
      const line = await output.next();
      NodeAssert.equal(line.done, false, `${fixture.label}: native exited early: ${stderr}`);
      const response = JSON.parse(line.value);
      NodeAssert.equal(response.version, 1, fixture.label);
      if (response.ok?.kind === "resolved") {
        capabilities = response.ok.capabilities;
        break;
      }
      NodeAssert.equal(response.ok?.kind, "observe", `${fixture.label}: ${line.value}`);
      const observation = response.ok.request;
      input.observations.push({
        request: observation,
        result: observe(fixture, state, input, observation),
      });
    }
    NodeAssert.notEqual(
      capabilities,
      undefined,
      `${fixture.label}: native resolution did not finish`,
    );
    child.stdin.end();
    NodeAssert.equal((await output.next()).done, true, `${fixture.label}: extra response`);
    NodeAssert.deepEqual(await closed, { code: 0, signal: null }, `${fixture.label}: ${stderr}`);
    NodeAssert.equal(stderr, "", fixture.label);
    return { capabilities, observations: input.observations.length };
  } finally {
    if (child.exitCode === null && child.signalCode === null) child.kill("SIGKILL");
    lines.close();
    await closed;
  }
}

export async function compareMaintenance(executable, fixtures = maintenanceCases) {
  let observations = 0;
  let lazyLookups = 0;
  for (const fixture of fixtures) {
    const upstream = world(fixture);
    const native = world(fixture);
    const effect =
      fixture.mode === "package"
        ? upstream.module.resolvePackageManagedProviderMaintenance(
            definition(fixture),
            fixture.context,
          )
        : fixture.mode === "mise"
          ? upstream.module.resolveMiseProviderMaintenance(
              fixture.owner,
              fixture.definition.mise,
              fixture.context,
            )
          : upstream.module.resolveHomebrewProviderMaintenance(
              fixture.owner,
              fixture.homebrew,
              fixture.context,
            );
    const expected = await upstream.execute(effect);
    const actual = await nativeResolve(executable, fixture, native);
    NodeAssert.deepEqual(
      hydrated(fixture, actual.capabilities),
      projected(expected),
      fixture.label,
    );
    NodeAssert.deepEqual(native.trace, upstream.trace, `${fixture.label}: resolution probe order`);
    observations += actual.observations;
    if (expected?.lookupLatestVersion) {
      const expectedVersion = await upstream.execute(expected.lookupLatestVersion);
      const actualVersion = await lookupLatest(fixture, native, actual.capabilities.latestLookup);
      NodeAssert.deepEqual(actualVersion, expectedVersion, `${fixture.label}: lazy latest version`);
      NodeAssert.deepEqual(native.trace, upstream.trace, `${fixture.label}: lazy probe order`);
      lazyLookups++;
    }
  }
  return { cases: fixtures.length, observations, lazyLookups };
}

add(base("missing binary has manual capabilities", { context: null }));
add(base("unknown installation stays manual"));
add(nativeDefinition(base("self updater is fallback"), { matches: null }));
add(
  nativeDefinition(base("path-required self updater cannot use fallback"), {
    requiresCommandPath: true,
  }),
);
add(
  nativeDefinition(base("native path preserves its own env"), {
    matches: ["/fixture/unknown/codex"],
    env: { CODEX_HOME: "/home/fixture's account" },
  }),
);

const installerPaths = [
  ["Vite home", "/home/a/.vite-plus/bin/codex"],
  ["Vite XDG", "/home/a/.local/share/vite-plus/bin/codex"],
  ["Vite Windows", "C:\\Users\\a\\AppData\\Local\\vite-plus\\bin\\codex.cmd"],
  ["Bun", "/home/a/.bun/bin/codex"],
  ["pnpm Linux", "/home/a/.local/share/pnpm/codex"],
  ["pnpm macOS", "/Users/a/Library/pnpm/codex"],
  ["pnpm alternate", "/opt/local/share/pnpm/codex"],
  ["pnpm Windows", "C:\\Users\\a\\AppData\\Local\\pnpm\\codex.cmd"],
  ["pnpm global", "/tools/pnpm/global/5/node_modules/codex/bin/codex"],
  ["Yarn", "/usr/share/yarn/global/node_modules/codex/bin/codex"],
  ["Yarn data", "/usr/share/yarn/data/global/node_modules/codex/bin/codex"],
  ["npm", "/usr/lib/node_modules/@openai/codex/bin/codex.js"],
  ["npm root", "/lib/node_modules/@openai/codex/bin/codex.js"],
  [
    "Homebrew Node npm",
    "/opt/homebrew/Cellar/node/25.0/lib/node_modules/@openai/codex/bin/codex.js",
  ],
  ["mise Node npm", "/home/a/mise/installs/node/25.0/lib/node_modules/@openai/codex/bin/codex.js"],
  ["npm expanding lowercase", "/home/İ/versions/😀/lib/node_modules/@openai/codex/bin/codex"],
  ["npm contextual lowercase", "/home/ΣΟΣ/lib/node_modules/@openai/codex/bin/codex"],
];
for (const [name, install] of installerPaths) {
  for (const original of [true, false]) {
    const fixture = base(`${name} ownership through ${original ? "configured" : "real"} path`);
    commandPath(fixture, original ? install : "/fixture/bin/codex", install);
    probe(fixture, "pnpm", ["--version"], "10.4.0\n");
    add(fixture);
  }
}
for (const [index, left] of installerPaths.entries()) {
  if (index >= 10) break;
  const right = installerPaths[(index + 4) % installerPaths.length];
  const fixture = nativeDefinition(base(`overlapping ownership ${left[0]} and ${right[0]}`), {
    matches: null,
  });
  commandPath(fixture, left[1], right[1]);
  probe(fixture, "pnpm", ["--version"], "10.4.0");
  add(fixture);
}
for (const version of [
  "9.9.9",
  "10.3.99",
  "10.4.0",
  "10.4.0-rc.1",
  "10.4.1-beta",
  "11.0.0",
  "v10.4",
  "10",
  "invalid",
  "",
  " 10.4.0\r\n",
]) {
  const fixture = commandPath(
    base(`pnpm build approval ${JSON.stringify(version)}`),
    "/home/a/.local/share/pnpm/codex",
  );
  probe(fixture, "pnpm", ["--version"], version);
  fixture.lookups.pnpm = "/fixture/bin/pnpm";
  probe(
    fixture,
    "/fixture/bin/pnpm",
    ["outdated", "-g", "--reporter=silent", "--format", "json", packageName],
    JSON.stringify({ [packageName]: { latest: "2.1.0" } }),
    1,
  );
  add(fixture);
}
for (const [label, outdated, list] of [
  [
    "empty outdated uses installed",
    "{}",
    [{ dependencies: { [packageName]: { version: "1.2.3" } } }],
  ],
  ["malformed outdated does not list", "not-json", []],
  ["wrong outdated shape", "[]", []],
  ["empty installed version", "{}", [{ dependencies: { [packageName]: { version: " " } } }]],
]) {
  const fixture = commandPath(base(`pnpm lazy ${label}`), "/home/a/.local/share/pnpm/codex");
  probe(fixture, "pnpm", ["--version"], "10.4.0");
  fixture.lookups.pnpm = "/fixture/bin/pnpm";
  probe(
    fixture,
    "/fixture/bin/pnpm",
    ["outdated", "-g", "--reporter=silent", "--format", "json", packageName],
    outdated,
  );
  probe(
    fixture,
    "/fixture/bin/pnpm",
    ["ls", "-g", "--reporter=silent", "--depth", "0", "--json", packageName],
    JSON.stringify(list),
  );
  add(fixture);
}

for (const real of [
  "/project/node_modules/@openai/codex/bin/codex.js",
  "/project/node_modules/other/lib/node_modules/@openai/codex/bin/codex.js",
  "/usr/lib/node_modules/other/bin/codex.js",
  "/usr/lib/node_modules/@openai/codex-else/bin/codex.js",
  "/home/a/mise/installs/codex/1.0/lib/node_modules/@openai/codex/bin/codex.js",
]) {
  add(
    commandPath(
      nativeDefinition(base(`unproven npm path ${real}`), { matches: null }),
      "/fixture/bin/codex",
      real,
    ),
  );
}
for (const platform of ["linux", "win32"]) {
  for (const present of [true, false]) {
    const fixture = base(`npm adjacent manifest ${platform}/${present}`, {
      hostPlatform: platform,
    });
    fixture.context.platform = platform;
    const resolved =
      platform === "win32"
        ? "C:\\Users\\a\\AppData\\Roaming\\npm\\codex.cmd"
        : "/project/bin/codex";
    commandPath(fixture, resolved);
    const path = platform === "win32" ? NodePath.win32 : NodePath.posix;
    fixture.exists[
      path.join(path.dirname(resolved), "node_modules", "@openai", "codex", "package.json")
    ] = present;
    add(fixture);
  }
}
for (const commandPlatform of ["linux", "win32"]) {
  const fixture = commandPath(
    base(`injected Windows filesystem with ${commandPlatform} default command platform`, {
      hostPlatform: "win32",
      commandPlatform,
    }),
    "C:\\Users\\a'b\\npm\\codex.cmd",
  );
  fixture.context.platform = "win32";
  fixture.exists["C:\\Users\\a'b\\npm\\node_modules\\@openai\\codex\\package.json"] = true;
  add(fixture);
}
for (const present of [true, false]) {
  const fixture = commandPath(
    base(`Volta image proof ${present}`),
    "/home/a/.volta/bin/codex",
    "/home/a/.volta/bin/volta-shim",
  );
  fixture.exists["/home/a/.volta/tools/image/packages/@openai/codex"] = present;
  add(fixture);
}

function brewFixture(label, changes = {}) {
  const fixture = commandPath(
    base(label),
    "/opt/homebrew/bin/codex",
    "/opt/homebrew/Caskroom/codex/1.0/codex",
  );
  fixture.exists["/opt/homebrew/bin/brew"] = true;
  probe(fixture, "/opt/homebrew/bin/brew", ["--prefix"], "/opt/homebrew\n");
  probe(
    fixture,
    "/opt/homebrew/bin/brew",
    ["info", "--json=v2", "codex"],
    JSON.stringify({ casks: [{ version: "2.0,build-3" }] }),
  );
  Object.assign(fixture, changes);
  return fixture;
}
add(brewFixture("Homebrew cask proves its prefix"));
add(
  brewFixture("Homebrew direct helper", {
    mode: "homebrew",
    homebrew: { kind: "cask", name: "codex", prefix: "/opt/homebrew" },
  }),
);
for (const prefix of ["/another/homebrew", "", " /opt/homebrew ", "/opt/homebrew/"]) {
  add(
    probe(
      brewFixture(`Homebrew prefix ${JSON.stringify(prefix)}`),
      "/opt/homebrew/bin/brew",
      ["--prefix"],
      prefix,
    ),
  );
}
for (const [name, real] of [
  ["formula", "/opt/homebrew/Cellar/codex/1.0/bin/codex"],
  ["case", "/OPT/HomeBrew/CASKROOM/CoDeX/1.0/codex"],
]) {
  const fixture = commandPath(brewFixture(`Homebrew ${name}`), "/fixture/bin/codex", real);
  const prefix = name === "case" ? "/OPT/HomeBrew" : "/opt/homebrew";
  const tool = `${prefix}/bin/brew`;
  fixture.exists[tool] = true;
  probe(fixture, tool, ["--prefix"], prefix);
  probe(
    fixture,
    tool,
    ["info", "--json=v2", name === "case" ? "CoDeX" : "codex"],
    JSON.stringify({
      formulae: [{ versions: { stable: "3.0.0" } }],
      casks: [{ version: "latest" }],
    }),
  );
  add(fixture);
}
for (const available of [true, false]) {
  const fixture = brewFixture(`Homebrew PATH fallback ${available}`);
  fixture.exists["/opt/homebrew/bin/brew"] = false;
  fixture.lookups.brew = available ? "/fixture/brew" : null;
  probe(fixture, "/fixture/brew", ["--prefix"], "/alias/homebrew");
  fixture.realPaths["/alias/homebrew"] = "/opt/homebrew";
  probe(fixture, "/fixture/brew", ["info", "--json=v2", "codex"], "{}");
  add(fixture);
}
for (const real of [
  "/opt\n/Cellar/tool/1/bin/tool",
  "/opt/Cellar/to\nol/1/bin/tool",
  "/opt/Cellar/tool/1\n2/bin/tool",
]) {
  const fixture = commandPath(
    base(`Homebrew newline classification ${JSON.stringify(real)}`),
    "/fixture/bin/tool",
    real,
  );
  fixture.lookups.brew = "/fixture/bin/brew";
  probe(fixture, "/fixture/bin/brew", ["--prefix"], "/opt");
  add(fixture);
}

for (const install of [
  "C:\\Users\\alice\\AppData\\Local\\Microsoft\\WinGet\\Links\\claude.exe",
  "C:\\Program Files\\WinGet\\Packages\\Anthropic.ClaudeCode_123\\claude.exe",
  "C:\\Program Files (x86)\\WinGet\\Links\\claude.exe",
  "C:\\ProgramData\\chocolatey\\bin\\claude.exe",
  "C:\\ProgramData\\chocolatey\\lib\\claude-code\\tools\\claude.exe",
  "D:\\Managed Tools\\bin\\claude.exe",
  "D:\\Managed Tools\\lib\\claude-code\\tools\\claude.exe",
  "a😀/winget/links/claude.exe",
]) {
  for (const provider of ["claudeAgent", "codex"]) {
    const fixture = nativeDefinition(
      commandPath(
        base(`${provider} Windows owner ${install}`, {
          owner: { provider, packageName: "@anthropic-ai/claude-code" },
          hostPlatform: "win32",
        }),
        install,
      ),
      { matches: null },
    );
    fixture.context.platform = "win32";
    fixture.context.env.ChocolateyInstall = " D:\\Managed Tools\\ ";
    probe(
      fixture,
      "choco",
      ["search", "claude-code", "--exact", "--limit-output"],
      "other|9.0\r\nCLAUDE-CODE| 2.1.5 \r\n",
    );
    add(fixture);
  }
}

function miseFixture(label, changes = {}) {
  const install = "/home/a/mise/installs/codex/1.2.3";
  const binary = `${install}/bin/codex`;
  const fixture = commandPath(base(label), "/home/a/mise/installs/codex/latest/bin/codex", binary);
  fixture.definition.mise = { tool: "codex" };
  fixture.lookups.mise = "/fixture/bin/mise";
  probe(fixture, "/fixture/bin/mise", ["which", "codex"], `${binary}\n`);
  probe(
    fixture,
    "/fixture/bin/mise",
    ["ls", "--json", "--installed", "codex"],
    JSON.stringify([{ install_path: install, active: true, requested_version: "latest" }]),
  );
  probe(fixture, "/fixture/bin/mise", ["latest", "codex"], "2.0.0\n");
  Object.assign(fixture, changes);
  return fixture;
}
add(miseFixture("mise active latest"));
add(
  miseFixture("mise direct helper", {
    mode: "mise",
    owner: { provider: "grok", packageName: null },
  }),
);
add(base("mise direct non-owner returns null", { mode: "mise" }));
for (const mapping of [undefined, { tool: "codex", upgrade: false }]) {
  const fixture = miseFixture(`mise unsupported mapping ${JSON.stringify(mapping)}`);
  fixture.definition.mise = mapping;
  add(fixture);
}
for (const [active, version] of [
  [false, "latest"],
  [true, "1.2.3"],
  [true, "stable"],
  [true, null],
  [true, undefined],
  [undefined, "latest"],
]) {
  const fixture = miseFixture(`mise install eligibility ${active}/${version}`);
  probe(
    fixture,
    "/fixture/bin/mise",
    ["ls", "--json", "--installed", "codex"],
    JSON.stringify([
      { install_path: "/home/a/mise/installs/codex/1.2.3", active, requested_version: version },
    ]),
  );
  add(fixture);
}
for (const [label, listing] of [
  [
    "object format",
    {
      codex: [
        {
          install_path: "/home/a/mise/installs/codex/1.2.3",
          active: true,
          requested_version: "latest",
        },
      ],
    },
  ],
  [
    "other tool object",
    {
      codex: [
        {
          install_path: "/home/a/mise/installs/codex/1.2.3",
          active: true,
          requested_version: "latest",
        },
      ],
      node: [],
    },
  ],
  ["empty", []],
  ["wrong shape", { install_path: "/home/a/mise/installs/codex/1.2.3" }],
  [
    "wrong active type",
    [
      {
        install_path: "/home/a/mise/installs/codex/1.2.3",
        active: "true",
        requested_version: "latest",
      },
    ],
  ],
  ["null install", [null]],
  [
    "mixed invalid rows",
    [
      {
        install_path: "/home/a/mise/installs/codex/1.2.3",
        active: true,
        requested_version: "latest",
      },
      { install_path: 7 },
    ],
  ],
  [
    "path prefix collision",
    [
      {
        install_path: "/home/a/mise/installs/codex/1.2",
        active: true,
        requested_version: "latest",
      },
    ],
  ],
  [
    "unknown fields survive validation",
    [
      {
        install_path: "/home/a/mise/installs/codex/1.2.3/",
        active: true,
        requested_version: "latest",
        extra: { nested: true },
      },
    ],
  ],
  [
    "inactive then active",
    [
      {
        install_path: "/home/a/mise/installs/codex/1.2.3",
        active: false,
        requested_version: "latest",
      },
      {
        install_path: "/home/a/mise/installs/codex/1.2.3",
        active: true,
        requested_version: "latest",
      },
    ],
  ],
]) {
  add(
    probe(
      miseFixture(`mise listing ${label}`),
      "/fixture/bin/mise",
      ["ls", "--json", "--installed", "codex"],
      JSON.stringify(listing),
    ),
  );
}
for (const text of ["not-json", "", "null", "[", " ", "x".repeat(256 * 1024 + 1)]) {
  add(
    probe(
      miseFixture(`mise invalid listing length ${text.length}`),
      "/fixture/bin/mise",
      ["ls", "--json", "--installed", "codex"],
      text,
    ),
  );
}
for (const which of ["", "/different/active/codex", "/alias/codex"]) {
  const fixture = probe(
    miseFixture(`mise which ${JSON.stringify(which)}`),
    "/fixture/bin/mise",
    ["which", "codex"],
    which,
  );
  fixture.realPaths["/alias/codex"] = fixture.context.realCommandPath;
  add(fixture);
}
for (const resolved of [
  "/home/a/mise/installs/codex/1.2.3/bin/codex",
  "/unrelated/latest/bin/codex",
  "/home/a/mise/installs/codex/latest-ish/bin/codex",
]) {
  const fixture = miseFixture(`mise refuses fixed launcher ${resolved}`);
  fixture.context.resolvedCommandPath = resolved;
  add(fixture);
}
for (const name of [
  "codex",
  "codex.exe",
  "codex.cmd",
  "codex.bat",
  "codex.ps1",
  "other",
  "codex.not-exe",
]) {
  for (const found of [true, false]) {
    const fixture = miseFixture(`mise shim ${name}/${found}`);
    commandPath(fixture, `/home/a/mise/shims/${name}`, "/custom/bin/mise");
    if (!found) {
      fixture.lookups.mise = null;
      for (const args of [
        ["which", "codex"],
        ["ls", "--json", "--installed", "codex"],
        ["latest", "codex"],
      ]) {
        fixture.probes[JSON.stringify(["/custom/bin/mise", args])] =
          fixture.probes[JSON.stringify(["/fixture/bin/mise", args])];
      }
    }
    add(fixture);
  }
}
for (const source of [
  "#!/bin/sh\nexec mise x -- codex\n",
  "#!/bin/sh\nmise exec -- codex\n",
  "#!/bin/sh\nnotmise exec -- codex\n",
  "#!/bin/sh\nmise execution -- codex\n",
  "#!/bin/sh\nmise\nexec codex\n",
  "echo mise exec codex\n",
  "#!/bin/sh\n_mise exec codex\n",
  "#!/bin/sh\né mise exec codex\n",
]) {
  const fixture = base(`wrapper ${JSON.stringify(source)}`);
  fixture.files[fixture.context.realCommandPath] = { text: source };
  add(fixture);
}
for (const size of [16 * 1024, 16 * 1024 + 1]) {
  const fixture = base(`wrapper stat limit ${size}`);
  fixture.files[fixture.context.realCommandPath] = {
    text: "#!/bin/sh\nmise exec -- codex\n",
    size,
  };
  add(fixture);
}
for (const latest of [
  "",
  " \n",
  "2.1.0",
  "2.1.0\nnoise",
  "latest channel",
  "v2.0.0",
  "\u00a02.0.0\u00a0",
]) {
  add(
    probe(
      miseFixture(`mise lazy latest ${JSON.stringify(latest)}`),
      "/fixture/bin/mise",
      ["latest", "codex"],
      latest,
    ),
  );
}
{
  const fixture = miseFixture("mise custom data directory and realpath tool parent");
  commandPath(fixture, "/aliased/codex/latest/bin/codex", "/private/tools/codex/1.2.3/bin/codex");
  probe(fixture, "/fixture/bin/mise", ["which", "codex"], fixture.context.realCommandPath);
  probe(
    fixture,
    "/fixture/bin/mise",
    ["ls", "--json", "--installed", "codex"],
    JSON.stringify([{ install_path: "/alias/install", active: true, requested_version: "latest" }]),
  );
  fixture.realPaths["/alias/install"] = "/private/tools/codex/1.2.3";
  fixture.realPaths["/alias"] = "/aliased/codex";
  add(fixture);
}

for (const platform of ["linux", "win32"]) {
  for (const executable of [
    "/safe/bin/codex",
    "/space path/codex",
    "/home/a'b/codex",
    "/home/é/codex",
    "C:\\Program Files\\codex.exe",
    "C:\\safe\\codex.exe",
    "/home/‘curly’/codex",
    "",
    "/bin/$(do-not-run)",
  ]) {
    const fixture = nativeDefinition(
      commandPath(
        base(`command quoting ${platform}/${JSON.stringify(executable)}`, {
          hostPlatform: platform,
        }),
        executable,
      ),
      {
        matches: null,
        args: [
          "update",
          "",
          "safe:@=-/._",
          "space word",
          "can't",
          "‘curly’",
          "é",
          "a\\b",
          "$(never)",
        ],
      },
    );
    fixture.context.platform = platform;
    add(fixture);
  }
}

if (
  process.argv[1] &&
  NodePath.resolve(process.argv[1]) === NodeURL.fileURLToPath(import.meta.url)
) {
  if (!process.argv[2])
    throw new Error("Usage: check-maintenance.mjs /absolute/executable [proof.json]");
  const executable = NodePath.resolve(process.argv[2]);
  const result = await compareMaintenance(executable);
  const proof = {
    version: 1,
    status: "passed",
    ...result,
    executableSha256: NodeCrypto.createHash("sha256")
      .update(await NodeFSP.readFile(executable))
      .digest("hex"),
    checks: [
      "complete immutable upstream resolver outputs and ordered read-only observations",
      "installer precedence, native fallback, npm prefix proof and Windows adjacent manifests",
      "mise active latest ownership, shims, aliases, pinned paths and malformed listings",
      "Homebrew prefix ownership, Chocolatey and WinGet manual-only policy",
      "lazy installer version lookup values and probe order",
      "POSIX and PowerShell display quoting and retained environment selection",
    ],
    boundaries: [
      "controlled filesystem and process services; no actual provider installation or upgrade",
      "host path operations, native driver predicates, semver facts and lazy parsers remain TypeScript",
      "cross-platform decision fixtures do not qualify Windows or mobile native execution",
    ],
  };
  if (process.argv[3])
    await NodeFSP.writeFile(process.argv[3], `${JSON.stringify(proof, null, 2)}\n`);
  console.log(JSON.stringify(proof));
}
