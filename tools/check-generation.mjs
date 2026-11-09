#!/usr/bin/env node
import * as NodeAssert from "node:assert/strict";
import * as NodeChildProcess from "node:child_process";
import * as NodeCrypto from "node:crypto";
import * as NodeFSP from "node:fs/promises";
import * as NodeModule from "node:module";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";
import * as Contracts from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as References from "effect/References";
import * as Schema from "effect/Schema";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
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

const model = immutableModule(
  "packages/shared/src/model.ts",
  { ...Contracts, Option, Schema, copySorted: (values, compare) => [...values].sort(compare) },
  [
    "codexModelFamily",
    "readCustomModelEntries",
    "getModelSelectionStringOptionValue",
    "getModelSelectionBooleanOptionValue",
  ],
);
const launch = immutableModule(
  "apps/server/src/provider/codexLaunchArgs.ts",
  immutableModule("packages/shared/src/cliArgs.ts", {}, ["tokenizeCliArgs"]),
  ["codexExecLaunchArgs", "resolveCodexLaunchArgs"],
);
const tier = immutableModule("apps/server/src/codexModelOptions.ts", model, [
  "getCodexServiceTierOptionValue",
]);
const utils = immutableModule(
  "packages/provider-core/src/server/textGenerationUtils.ts",
  { TextGenerationError: Contracts.TextGenerationError, Option, Schema },
  ["normalizeCliError", "toJsonSchemaObject"],
);
const { expandHomePath } = immutableModule(
  "packages/provider-core/src/server/pathExpansion.ts",
  { NodePath },
  ["expandHomePath"],
);
const outputSchema = Schema.Struct({ value: Schema.String });
const host = {
  HomeDirectory: Context.Reference("generation-oracle-home", {
    defaultValue: () => "/fixture-home",
  }),
};
const serverConfig = {
  ServerConfig: Context.Service()("generation-oracle-server-config"),
};
const observedEnvironment = (env) =>
  Object.fromEntries(
    ["HOME", "CODEX_HOME", "PRIVATE_FIXTURE_TOKEN", "T3CODE_CODEX_LAUNCH_ARGS"].flatMap((key) =>
      env[key] === undefined ? [] : [[key, env[key]]],
    ),
  );

function normalizedArgs(args) {
  return args.map((arg, index) =>
    args[index - 1] === "--output-schema"
      ? "<schema>"
      : args[index - 1] === "--output-last-message"
        ? "<output>"
        : arg,
  );
}

async function upstream(fixture) {
  const files = new Map();
  const trace = [];
  let sequence = 0;
  const fs = FileSystem.makeNoop({
    makeTempFile: () => Effect.sync(() => `/oracle-temp-${++sequence}/file`),
    writeFileString: (path, text) => Effect.sync(() => files.set(path, text)),
    readFileString: (path) =>
      files.has(path)
        ? Effect.succeed(new TextDecoder().decode(new TextEncoder().encode(files.get(path))))
        : Effect.fail(new Error("missing file")),
    stat: () => Effect.succeed({ type: "File" }),
    remove: (directory) =>
      Effect.sync(() => {
        for (const path of files.keys()) if (path.startsWith(`${directory}/`)) files.delete(path);
      }),
  });
  const spawner = ChildProcessSpawner.make((command) =>
    Effect.gen(function* () {
      const args = command.args;
      const phase = args[0] === "exec" ? "exec" : "mcp";
      const prompt = command.options.stdin?.stream
        ? yield* command.options.stdin.stream.pipe(Stream.decodeText(), Stream.mkString)
        : "";
      const schemaPath = args[args.indexOf("--output-schema") + 1];
      trace.push({
        phase,
        args: normalizedArgs(args),
        cwd: command.options.cwd,
        env: observedEnvironment(command.options.env),
        prompt,
        ...(phase === "exec" ? { schema: JSON.parse(files.get(schemaPath)) } : {}),
      });
      if (phase === "exec") {
        files.set(args[args.indexOf("--output-last-message") + 1], fixture.output);
      }
      const stdout = phase === "mcp" ? fixture.mcpRaw : fixture.stdout;
      const stderr = phase === "mcp" ? fixture.mcpStderr : fixture.stderr;
      const exitCode = phase === "mcp" ? fixture.mcpExit : fixture.exitCode;
      const signal = phase === "mcp" ? fixture.mcpSignal : fixture.execSignal;
      return ChildProcessSpawner.makeHandle({
        pid: ChildProcessSpawner.ProcessId(1),
        exitCode: signal
          ? Effect.fail(new Error("fixture child terminated by signal"))
          : Effect.succeed(ChildProcessSpawner.ExitCode(exitCode)),
        isRunning: Effect.succeed(false),
        kill: () => Effect.void,
        unref: Effect.succeed(Effect.void),
        stdin: Sink.drain,
        stdout: Stream.encodeText(Stream.make(stdout)),
        stderr:
          fixture.stderrBytes && phase === "exec"
            ? Stream.make(Uint8Array.from(fixture.stderrBytes))
            : Stream.encodeText(Stream.make(stderr)),
        all: Stream.empty,
        getInputFd: () => Sink.drain,
        getOutputFd: () => Stream.empty,
      });
    }),
  );
  const { makeCodexTextGeneration } = immutableModule(
    "apps/server/src/textGeneration/CodexTextGeneration.ts",
    {
      Effect,
      FileSystem,
      Option,
      Path,
      Schema,
      Stream,
      ChildProcess,
      ChildProcessSpawner,
      TextGenerationError: Contracts.TextGenerationError,
      DEFAULT_TEXT_GENERATION_REASONING_EFFORT: Contracts.DEFAULT_TEXT_GENERATION_REASONING_EFFORT,
      resolveSpawnCommand: (command, args) => Effect.succeed({ command, args, shell: false }),
      resolveAttachmentPath: ({ attachment }) => attachment.fixturePath,
      ServerConfig: serverConfig,
      HostProcess: host,
      expandHomePath,
      ...launch,
      ...model,
      ...tier,
      ...utils,
      TextGenerationOperations: { fromRunner: (_name, runner) => runner },
    },
    ["makeCodexTextGeneration"],
  );
  const program = Effect.gen(function* () {
    const runner = yield* makeCodexTextGeneration(
      fixture.config,
      fixture.env,
      Effect.succeed(fixture.models),
      fixture.managed
        ? Effect.succeed({
            config: fixture.effectiveConfig,
            environment: fixture.env,
            revision: "fixture",
          })
        : undefined,
    );
    return yield* runner({
      operation: "generateThreadTitle",
      cwd: fixture.cwd,
      prompt: fixture.prompt,
      outputSchema,
      modelSelection: fixture.modelSelection,
      attachments: fixture.imagePaths.map((fixturePath) => ({ type: "image", fixturePath })),
    });
  }).pipe(
    Effect.provideService(FileSystem.FileSystem, fs),
    Effect.provideService(Path.Path, Path.make(NodePath.posix)),
    Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
    Effect.provideService(serverConfig.ServerConfig, { attachmentsDir: "/fixture-attachments" }),
    Effect.provideService(References.MinimumLogLevel, "None"),
  );
  const result = await Effect.runPromiseExit(program);
  NodeAssert.equal(files.size, 0, `${fixture.label}: upstream cleanup`);
  if (Exit.isFailure(result) && Cause.squash(result.cause)?.detail === undefined) {
    throw Cause.squash(result.cause);
  }
  return {
    trace,
    result: Exit.isSuccess(result)
      ? { ok: result.value }
      : { error: Cause.squash(result.cause).detail },
  };
}

const fixtures = [];
function add(label, changes = {}) {
  const fixture = {
    label,
    launchArgs: "",
    env: { PRIVATE_FIXTURE_TOKEN: "fixture-only-token" },
    models: [],
    customModels: [],
    modelSelection: { model: "gpt-5.4-mini", options: [] },
    managed: false,
    imagePaths: [],
    prompt: "A structured generation fixture.\nPreserve Unicode: 雪 🌲",
    mcpRaw: "[]",
    mcpExit: 0,
    mcpStderr: "",
    output: '{"value":"fixture result"}',
    stdout: "",
    stderr: "",
    exitCode: 0,
    ...changes,
  };
  fixtures.push(fixture);
  return fixture;
}
const candidate = (slug, changes = {}) => ({
  slug,
  isCustom: false,
  isDefault: false,
  isLegacy: false,
  ...changes,
});
const selected = (model, options = []) => ({ model, options });
const options = [
  { id: "reasoningEffort", value: "high" },
  { id: "serviceTier", value: "priority" },
];

add("empty model catalog");
add("exact model precedes default", {
  models: [candidate("default", { isDefault: true }), candidate("requested")],
  modelSelection: selected("requested", options),
});
add("exact custom catalog entry", {
  models: [candidate("default", { isDefault: true }), candidate("custom", { isCustom: true })],
  modelSelection: selected("custom", options),
});
add("configured custom omitted from catalog", {
  customModels: [" \ufeffcustom-model\u00a0 "],
  models: [candidate("default", { isDefault: true })],
  modelSelection: selected("custom-model", options),
});
add("configured object custom entry", {
  customModels: [null, false, 2, {}, { slug: 42 }, { slug: " custom-model ", name: "Named" }],
  models: [candidate("default", { isDefault: true })],
  modelSelection: selected("custom-model", options),
});
add("managed keeps original configured custom", {
  managed: true,
  customModels: ["custom-model"],
  runtimeCustomModels: [],
  models: [candidate("default", { isDefault: true })],
  modelSelection: selected("custom-model", options),
});
add("managed does not adopt runtime custom entries", {
  managed: true,
  runtimeCustomModels: ["custom-model"],
  models: [candidate("default", { isDefault: true })],
  modelSelection: selected("custom-model", options),
});
for (const [requested, available] of [
  ["openai.gpt-5.4-mini", "gpt-5.4-mini"],
  ["gpt-5.4-mini", "openai.gpt-5.4-mini"],
  ["openai.other", "other"],
  ["OpenAI.gpt-5.4-mini", "gpt-5.4-mini"],
]) {
  add(`family ${requested} to ${available}`, {
    models: [candidate("default", { isDefault: true }), candidate(available)],
    modelSelection: selected(requested, options),
  });
}
for (const [label, models] of [
  ["default", [candidate("legacy", { isLegacy: true }), candidate("default", { isDefault: true })]],
  ["nonlegacy", [candidate("legacy", { isLegacy: true }), candidate("ordinary")]],
  [
    "first legacy",
    [candidate("first", { isLegacy: true }), candidate("second", { isLegacy: true })],
  ],
  ["custom excluded", [candidate("custom", { isCustom: true, isDefault: true })]],
  ["empty preserves options", []],
]) {
  add(`fallback ${label}`, { models, modelSelection: selected("unavailable", options) });
}
for (const managed of [false, true]) {
  for (const value of ["minimal", "low", "medium", "high", "xhigh", "", "space value", true]) {
    add(`reasoning ${managed}:${String(value)}`, {
      managed,
      modelSelection: selected("requested", [{ id: "reasoningEffort", value }]),
    });
  }
  for (const optionSet of [
    [{ id: "fastMode", value: true }],
    [{ id: "fastMode", value: false }],
    [{ id: "fastMode", value: "true" }],
    [
      { id: "serviceTier", value: "fast" },
      { id: "fastMode", value: true },
    ],
    [
      { id: "serviceTier", value: "" },
      { id: "fastMode", value: true },
    ],
    [
      { id: "serviceTier", value: true },
      { id: "fastMode", value: true },
    ],
    [
      { id: "reasoningEffort", value: true },
      { id: "reasoningEffort", value: "high" },
    ],
    [
      { id: "serviceTier", value: false },
      { id: "serviceTier", value: "priority" },
    ],
  ]) {
    add(`option precedence ${managed}:${JSON.stringify(optionSet)}`, {
      managed,
      modelSelection: selected("requested", optionSet),
    });
  }
}
for (const launchArgs of [
  "--listen 127.0.0.1 --strict-config --config x=1 --enable foo --disable bar",
  "-c x=1 --config=y=2 -c=z=3 --enable=foo --disable=bar",
  "--config --strict-config --enable --disable -c -x --config",
  "--config '' --enable \"\" --disable '' -c \"\"",
  '--config \'a="quoted value"\' --config "b=\\"double\\""',
  "--config x=escaped\\ value --config x=escaped\\'quote --config x=trailing\\",
  '--config "unterminated --strict-config',
  "--config 'unterminated --strict-config",
  '--config "x=back\\slash\\$dollar\\`tick"',
  "--config\ufeffx=1\u00a0--strict-config\u2028--enable\u3000雪",
  "--config x=1\u0085--strict-config",
  "-- --config after=separator --strict-config",
  "--enable-analytics --disable-whatever --configurable x --strict-config=true",
  "--config=features.apps=true --config 'features.plugins=true'",
  "--config a='single'\"double\"plain --config 雪=🌲",
]) {
  add(`launch ${launchArgs}`, { launchArgs });
}
for (const override of ["", "  \ufeff\u00a0 ", " --config env=1 --listen ignored ", "\u0085"]) {
  add(`environment launch override ${JSON.stringify(override)}`, {
    launchArgs: "--config setting=1 --strict-config",
    env: { T3CODE_CODEX_LAUNCH_ARGS: override },
  });
}
for (const mcpRaw of [
  JSON.stringify([
    { name: "enabled", enabled: true },
    { name: "disabled", enabled: false },
  ]),
  JSON.stringify(
    ["dot.name", 'quote"name', "slash\\name", "", "雪🌲", "line\nname"].map((name) => ({
      name,
      enabled: true,
    })),
  ),
  JSON.stringify([
    { name: "repeat", enabled: true },
    { name: "repeat", enabled: true },
  ]),
  JSON.stringify([{ name: "extra", enabled: true, metadata: { ignored: [1, null] } }]),
  JSON.stringify([{ name: "wrong", enabled: "true" }]),
  JSON.stringify([{ enabled: true }]),
  JSON.stringify({ name: "object", enabled: true }),
  "not JSON",
  "[] trailing",
  "",
  '[{"name":"first","name":"second","enabled":false,"enabled":true}]',
  '[{"name":"first","name":42,"enabled":true}]',
]) {
  add(`MCP ${mcpRaw}`, { mcpRaw, launchArgs: "--strict-config --config features.plugins=true" });
}
add("MCP nonzero proceeds", { mcpExit: 9, mcpStderr: "fixture probe failure" });
add("MCP UTF-8 BOM", { mcpRaw: '\ufeff[{"name":"enabled","enabled":true}]' });
add("MCP duplicate UTF-8 BOM", { mcpRaw: '\ufeff\ufeff[{"name":"enabled","enabled":true}]' });
add("stderr wins on nonzero", { exitCode: 7, stderr: "  fixture failure \n", stdout: "ignore" });
add("stdout fallback on nonzero", { exitCode: 8, stdout: " fixture stdout failure \n" });
add("exit-only error", { exitCode: 9 });
add("MCP signal exit proceeds", { mcpSignal: "SIGTERM" });
add("execution signal exit", { execSignal: "SIGTERM" });
add("execution interrupt signal", { execSignal: "SIGINT" });
add("Unicode error trimming", { exitCode: 10, stderr: "\ufeff\u00a0fixture error\u3000" });
add("invalid diagnostic bytes use replacement", { exitCode: 11, stderrBytes: [255, 10] });
add("invalid output JSON", { output: "not JSON" });
add("invalid structured output", { output: '{"value":42}' });
add("extra output properties", { output: '{"value":"kept","unknown":"removed"}' });
add("empty successful output", { output: "" });
add("output UTF-8 BOM", { output: '\ufeff{"value":"BOM output"}' });
add("output duplicate UTF-8 BOM", { output: '\ufeff\ufeff{"value":"BOM output"}' });
add("output escaped high surrogate", {
  output: JSON.stringify({ value: String.fromCharCode(0xd800) }),
});
add("output escaped low surrogate", {
  output: JSON.stringify({ value: String.fromCharCode(0xdc00) }),
});
add("home expansion", {
  homePath: "~/codex home",
  env: { HOME: "/environment-home", CODEX_HOME: "/old" },
});
add("literal home", { homePath: "/direct/codex home", env: { CODEX_HOME: "/old" } });
add("absent HOME remains absent");
add("images", { imagePaths: ["/fixture image.png", "/fixture/雪.png"] });
add("managed launch configuration", {
  managed: true,
  launchArgs: "--config original=1",
  runtimeLaunchArgs: "--config managed=1",
});

const fakeSource = `#!${process.execPath}
import * as fs from 'node:fs';
const data = JSON.parse(fs.readFileSync(process.env.GENERATION_FIXTURE, 'utf8'));
const args = process.argv.slice(2);
const phase = args[0] === 'exec' ? 'exec' : 'mcp';
let prompt = '';
if (phase === 'exec') { for await (const chunk of process.stdin) prompt += chunk; }
const normalized = args.map((arg, index) => args[index - 1] === '--output-schema' ? '<schema>' : args[index - 1] === '--output-last-message' ? '<output>' : arg);
const env = Object.fromEntries(['HOME','CODEX_HOME','PRIVATE_FIXTURE_TOKEN','T3CODE_CODEX_LAUNCH_ARGS'].flatMap(key => process.env[key] === undefined ? [] : [[key,process.env[key]]]));
const trace = {phase,args:normalized,cwd:process.cwd(),env,prompt};
if (phase === 'exec') trace.schema = JSON.parse(fs.readFileSync(args[args.indexOf('--output-schema')+1], 'utf8'));
fs.appendFileSync(process.env.GENERATION_TRACE, JSON.stringify(trace)+'\\n');
if (phase === 'exec') fs.writeFileSync(args[args.indexOf('--output-last-message')+1], data.output);
process.stdout.write(phase === 'mcp' ? data.mcpRaw : data.stdout);
process.stderr.write(phase === 'mcp' ? data.mcpStderr : data.stderrBytes ? Buffer.from(data.stderrBytes) : data.stderr);
const signal = phase === 'mcp' ? data.mcpSignal : data.execSignal;
if (signal) process.kill(process.pid, signal);
process.exitCode = phase === 'mcp' ? data.mcpExit : data.exitCode;
`;

async function native(executable, fixture, directory) {
  const config = fixture.effectiveConfig;
  const tracePath = NodePath.join(directory, "trace.jsonl");
  const planPath = NodePath.join(directory, "fixture.json");
  const tempPath = NodePath.join(directory, "temporary");
  await NodeFSP.mkdir(tempPath);
  await NodeFSP.writeFile(planPath, JSON.stringify(fixture));
  const env = {
    PATH: process.env.PATH,
    ...fixture.env,
    ...(config.homePath ? { CODEX_HOME: expandHomePath(config.homePath, "/fixture-home") } : {}),
    GENERATION_FIXTURE: planPath,
    GENERATION_TRACE: tracePath,
    TMPDIR: tempPath,
  };
  const request = {
    version: 1,
    operation: "run",
    binary: config.binaryPath,
    cwd: fixture.cwd,
    prompt: fixture.prompt,
    outputSchema: utils.toJsonSchemaObject(outputSchema),
    launchArgs: config.launchArgs ?? "",
    models: fixture.models,
    customModels: fixture.customModels,
    modelSelection: fixture.modelSelection,
    managed: fixture.managed,
    imagePaths: fixture.imagePaths,
    timeoutMs: 180_000,
  };
  const run = NodeChildProcess.spawnSync(executable, [], {
    input: `${JSON.stringify(request)}\n`,
    encoding: "utf8",
    env,
    timeout: 30_000,
    maxBuffer: 32 * 1024 * 1024,
  });
  NodeAssert.equal(run.error, undefined, fixture.label);
  NodeAssert.equal(run.status, 0, `${fixture.label}: ${run.stderr}`);
  const response = JSON.parse(run.stdout);
  NodeAssert.equal(response.version, 1);
  let result;
  if (response.error) result = { error: response.error.detail };
  else {
    NodeAssert.equal(
      response.ok.output,
      new TextDecoder().decode(new TextEncoder().encode(fixture.output)),
      `${fixture.label}: decoded output file`,
    );
    const decoded = await Effect.runPromiseExit(
      Schema.decodeEffect(Schema.fromJsonString(outputSchema))(response.ok.output),
    );
    result = Exit.isSuccess(decoded)
      ? { ok: decoded.value }
      : { error: "Codex returned invalid structured output." };
  }
  const trace = (await NodeFSP.readFile(tracePath, "utf8")).trim().split("\n").map(JSON.parse);
  NodeAssert.deepEqual(await NodeFSP.readdir(tempPath), [], `${fixture.label}: native cleanup`);
  return { trace, result };
}

async function main() {
  const executable =
    process.argv[2] && process.argv[2] !== "--oracle-only"
      ? NodePath.resolve(process.argv[2])
      : null;
  const proofPath = process.argv[3];
  NodeAssert.ok(executable || process.argv.includes("--oracle-only"), "Pass the native executable");
  const temporary = await NodeFSP.mkdtemp(
    NodePath.join(NodeOS.tmpdir(), "t3ls-generation-oracle-"),
  );
  let observations = 0;
  try {
    const binary = NodePath.join(temporary, "controlled codex.mjs");
    await NodeFSP.writeFile(binary, fakeSource, { mode: 0o755 });
    for (const [index, fixture] of fixtures.entries()) {
      const directory = NodePath.join(temporary, String(index));
      await NodeFSP.mkdir(directory);
      fixture.cwd = directory;
      fixture.config = {
        binaryPath: binary,
        launchArgs: fixture.launchArgs,
        homePath: fixture.homePath,
        customModels: fixture.customModels,
      };
      fixture.effectiveConfig = fixture.managed
        ? {
            ...fixture.config,
            launchArgs: fixture.runtimeLaunchArgs ?? fixture.launchArgs,
            customModels: fixture.runtimeCustomModels ?? fixture.customModels,
          }
        : fixture.config;
      const expected = await upstream(fixture);
      NodeAssert.equal(expected.trace.length, 2, `${fixture.label}: both upstream phases`);
      if (executable) {
        const actual = await native(executable, fixture, directory);
        NodeAssert.deepEqual(actual, expected, fixture.label);
      }
      observations += expected.trace.length;
    }
    const receipt = {
      version: 1,
      status: executable ? "passed" : "oracle-only",
      upstreamCommit: JSON.parse(
        await NodeFSP.readFile(new URL("../upstream.lock.json", import.meta.url), "utf8"),
      ).upstream.commit,
      cases: fixtures.length,
      observations,
      executableSha256: executable
        ? NodeCrypto.createHash("sha256")
            .update(await NodeFSP.readFile(executable))
            .digest("hex")
        : null,
      checks: [
        "complete immutable upstream runner results and ordered MCP/exec invocations",
        "exact model, configured custom, family, default and legacy fallback with option precedence",
        "raw launch argument tokenization, filtering and environment override",
        "MCP disable quoting, invalid-list tolerance and trailing plugin/app overrides",
        "prompt, output schema, authorized image paths, selected environment and working directory",
        "complete nonzero-exit errors, structured output decoding and temporary-file cleanup",
      ],
      boundaries: [
        "controlled provider executable; no actual provider generation, credentials or paid service",
        "upstream filesystem and subprocess services are controlled; native subprocesses execute",
        "runtime/credential selection, attachment authorization and final schemas remain host responsibilities",
      ],
    };
    if (proofPath) await NodeFSP.writeFile(proofPath, `${JSON.stringify(receipt, null, 2)}\n`);
    process.stdout.write(`${JSON.stringify(receipt)}\n`);
  } finally {
    await NodeFSP.rm(temporary, { recursive: true, force: true });
  }
}

if (
  process.argv[1] &&
  NodePath.resolve(process.argv[1]) === NodeURL.fileURLToPath(import.meta.url)
) {
  await main();
}
