function effectiveDefault() { return NodeURL.fileURLToPath(new URL("../bin/t3-code-ls-generate", import.meta.url)); }
import * as NodeAssert from "node:assert/strict";
import * as NodeCrypto from "node:crypto";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { CodexSettings, ProviderDriverKind, ProviderInstanceId } from "@t3tools/contracts";
import * as HostProcess from "@t3tools/shared/HostProcess";
import { createModelSelection } from "@t3tools/shared/model";
import * as Cause from "effect/Cause";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";
import * as ServerConfig from "../apps/server/src/config.ts";
import * as ServerSettings from "../apps/server/src/serverSettings.ts";
import * as ProviderInstanceRegistry from "../apps/server/src/provider/ProviderInstanceRegistry.ts";
import * as SourceControlProviderRegistry from "../apps/server/src/sourceControl/SourceControlProviderRegistry.ts";
import * as TextGeneration from "../apps/server/src/textGeneration/TextGeneration.ts";
import * as TextGenerators from "../apps/server/src/textGeneration/TextGenerators.ts";
import { makeCodexTextGeneration } from "../apps/server/src/textGeneration/CodexTextGeneration.ts";
import { makeProcessProbe } from "../apps/server/src/testUtils/processProbe.ts";

NodeAssert.ok(process.argv[2], "Pass the native generation executable");
const executable = NodePath.resolve(process.argv[2]);
const proofPath = process.argv[3];
const root = NodeURL.fileURLToPath(new URL("../", import.meta.url));
const temporary = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "t3ls-generation-adapter-"));
const instanceId = ProviderInstanceId.make("codex");
const defaultSelection = createModelSelection(instanceId, "gpt-5.4-mini");
const decodeCodexSettings = Schema.decodeSync(CodexSettings);
const hostPlatform = Effect.runSync(HostProcess.Platform);
const credential = "synthetic-generation-credential-never-an-argument";
const scenarios = [];
let totalNativeProcesses = 0;
let totalProtocolFixtureProcesses = 0;
let totalCliProcesses = 0;
let nextFixture = 0;

const peerSource = `#!${process.execPath}
import * as fs from "node:fs";
import * as net from "node:net";
const input = JSON.parse(fs.readFileSync(process.env.T3LS_CASE, "utf8"));
const args = process.argv.slice(2);
const mcp = args.includes("mcp") && args.includes("list");
const stage = mcp ? "mcp" : "exec";
const after = (name) => args[args.indexOf(name) + 1];
const schemaPath = mcp ? null : after("--output-schema");
const outputPath = mcp ? null : after("--output-last-message");
const audit = {
  pid: process.pid, ppid: process.ppid, stage, args, cwd: process.cwd(),
  home: process.env.HOME ?? null, codexHome: process.env.CODEX_HOME ?? null,
  credential: process.env.T3LS_CREDENTIAL ?? null,
  schemaPath, outputPath,
  schema: schemaPath ? JSON.parse(fs.readFileSync(schemaPath, "utf8")) : null,
  schemaMode: schemaPath ? fs.statSync(schemaPath).mode & 0o777 : null,
  outputMode: outputPath ? fs.statSync(outputPath).mode & 0o777 : null,
};
fs.writeFileSync(process.env.T3LS_AUDIT + "-" + stage + ".json", JSON.stringify(audit));
if (input.hold === stage) {
  process.on("SIGTERM", () => {});
  const probe = net.createConnection(Number(process.env.T3LS_PROBE_PORT), "127.0.0.1", () => {
    probe.write(process.pid + "\\n");
  });
  await new Promise(() => {});
}
if (mcp) {
  if (input.mcpExit) process.exit(input.mcpExit);
  process.stdout.write(input.mcpRaw ?? JSON.stringify(input.servers ?? []));
} else {
  const chunks = [];
  for await (const chunk of process.stdin) chunks.push(chunk);
  audit.prompt = Buffer.concat(chunks).toString("utf8");
  fs.writeFileSync(process.env.T3LS_AUDIT + "-exec.json", JSON.stringify(audit));
  if (input.removeOutput) fs.unlinkSync(outputPath);
  else fs.writeFileSync(outputPath, input.output ?? JSON.stringify({ title: "Fixture title" }));
  if (input.stderr) process.stderr.write(input.stderr);
  if (input.stdout) process.stdout.write(input.stdout);
  process.exitCode = input.exitCode ?? 0;
}
`;

async function fixture(options = {}) {
  const directory = NodePath.join(temporary, `case-${++nextFixture}`);
  const f = {
    directory,
    workspace: NodePath.join(directory, "workspace"),
    home: NodePath.join(directory, "home"),
    tmp: NodePath.join(directory, "tmp"),
    bin: NodePath.join(directory, "bin"),
    audit: NodePath.join(directory, "audit"),
    caseFile: NodePath.join(directory, "case.json"),
    commands: [],
    handles: [],
    frames: [],
    runtimeClosed: 0,
    options,
  };
  await Promise.all(
    [f.workspace, f.home, f.tmp, f.bin].map((path) => NodeFSP.mkdir(path, { recursive: true })),
  );
  f.peer = NodePath.join(f.bin, "codex");
  await NodeFSP.writeFile(f.peer, peerSource, { mode: 0o700 });
  await NodeFSP.writeFile(f.caseFile, JSON.stringify(options));
  f.env = {
    HOME: f.home,
    PATH: f.bin,
    TMPDIR: f.tmp,
    T3LS_CASE: f.caseFile,
    T3LS_AUDIT: f.audit,
    T3LS_CREDENTIAL: credential,
    ...options.environment,
  };
  return f;
}

async function check(name, body) {
  await body();
  scenarios.push(name);
  console.log(`Passed: ${name}`);
}

function assertDead(pid) {
  NodeAssert.throws(
    () => process.kill(pid, 0),
    (error) => error.code === "ESRCH",
  );
}

async function audits(f) {
  const rows = [];
  for (const stage of ["mcp", "exec"]) {
    try {
      rows.push(JSON.parse(await NodeFSP.readFile(`${f.audit}-${stage}.json`, "utf8")));
    } catch (error) {
      if (error.code !== "ENOENT") throw error;
    }
  }
  return rows;
}

async function closed(f) {
  for (const handle of f.handles) assertDead(Number(handle.pid));
  const rows = await audits(f);
  for (const row of rows) {
    assertDead(row.pid);
    totalCliProcesses++;
    for (const path of [row.schemaPath, row.outputPath].filter(Boolean)) {
      await NodeAssert.rejects(NodeFSP.stat(path), (error) => error.code === "ENOENT");
    }
  }
  NodeAssert.deepEqual(await NodeFSP.readdir(f.tmp), []);
  return rows;
}

function value(exit) {
  if (Exit.isFailure(exit)) throw Cause.squash(exit.cause);
  return exit.value;
}

function failure(exit, operation = "generateThreadTitle") {
  NodeAssert.ok(Exit.isFailure(exit));
  const error = Cause.squash(exit.cause);
  NodeAssert.equal(error._tag, "TextGenerationError");
  NodeAssert.equal(error.operation, operation);
  return error;
}

async function run(f, body, options = {}) {
  const selected = options.unselected ? undefined : (options.selected ?? executable);
  const effective = selected ?? effectiveDefault();
  let program = Effect.gen(function* () {
    const actual = yield* ChildProcessSpawner.ChildProcessSpawner;
    const observed = ChildProcessSpawner.make((command) => {
      NodeAssert.ok(
        command.command === effective,
      );
      f.commands.push(command);
      {
        NodeAssert.deepEqual(command.args, []);
        NodeAssert.equal(command.options.extendEnv, false);
        NodeAssert.equal(command.options.shell, false);
        NodeAssert.equal(command.options.env.T3LS_CREDENTIAL, credential);
        const stream = command.options.stdin.stream.pipe(
          Stream.tap((chunk) => Effect.sync(() => f.frames.push(new TextDecoder().decode(chunk)))),
        );
        command = {
          ...command,
          options: { ...command.options, stdin: { ...command.options.stdin, stream } },
        };
      }
      return actual.spawn(command).pipe(
        Effect.tap((child) =>
          Effect.sync(() => {
            f.handles.push(child);
            if (command.command === executable) totalNativeProcesses++;
            else totalProtocolFixtureProcesses++;
          }),
        ),
      );
    });
    if (f.options.hold) {
      f.probe = yield* makeProcessProbe;
      f.env.T3LS_PROBE_PORT = String(f.probe.port);
    }
    const config = decodeCodexSettings({
      binaryPath: f.options.pathLookup ? "codex" : f.peer,
      ...f.options.config,
    });
    const runtime =
      f.runtime ??
      (f.options.managed
        ? Effect.gen(function* () {
            yield* Effect.addFinalizer(() =>
              Effect.sync(() => {
                f.runtimeClosed++;
              }),
            );
            return {
              config: { ...config, customModels: [], homePath: "~/managed-home" },
              environment: {
                ...f.env,
                T3_CODE_LS_GENERATION_EXECUTABLE: "/untrusted-provider-overlay",
              },
              revision: "fixture",
            };
          })
        : undefined);
    const models =
      f.models ??
      Effect.succeed(
        (f.options.models ?? []).map((model) => ({
          slug: typeof model === "string" ? model : model.slug,
          name: typeof model === "string" ? model : model.slug,
          isCustom: false,
          capabilities: null,
          ...(typeof model === "string" ? {} : model),
        })),
      );
    return yield* Effect.gen(function* () {
      const service = yield* makeCodexTextGeneration(config, f.env, models, runtime);
      return yield* body(service, f);
    }).pipe(Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, observed));
  }).pipe(
    Effect.scoped,
    Effect.provide(
      ServerConfig.ServerConfig.layerTest(f.workspace, NodePath.join(f.directory, "server")).pipe(
        Layer.provideMerge(NodeServices.layer),
      ),
    ),
    Effect.provideService(
      HostProcess.Environment,
      selected === undefined ? {} : { T3_CODE_LS_GENERATION_EXECUTABLE: selected },
    ),
    Effect.provideService(HostProcess.Platform, options.platform ?? hostPlatform),
    Effect.provideService(HostProcess.WorkingDirectory, f.workspace),
    Effect.provideService(HostProcess.HomeDirectory, f.home),
  );
  if (options.clock || f.options.hold) program = program.pipe(Effect.provide(TestClock.layer()));
  return Effect.runPromiseExit(program);
}

function title(service, f, extra = {}) {
  return service.generateThreadTitle({
    cwd: f.workspace,
    message: "Describe fixture changes",
    modelSelection: defaultSelection,
    ...extra,
  });
}

function argument(args, flag) {
  return args[args.lastIndexOf(flag) + 1];
}

try {
  const cases = [
    [
      "generateCommitMessage",
      { subject: " Update fixture.\nIgnored", body: " detail " },
      { subject: "Update fixture", body: "detail" },
      {
        branch: "main",
        stagedSummary: "M fixture.ts",
        stagedPatch: "+fixture",
        includeBranch: false,
      },
    ],
    [
      "generatePrContent",
      { title: " Fixture PR\nIgnored", body: " details " },
      { title: "Fixture PR", body: "details" },
      {
        baseBranch: "main",
        headBranch: "fixture",
        commitSummary: "Fixture commit",
        diffSummary: "fixture.ts",
        diffPatch: "+fixture",
      },
    ],
    [
      "generateBranchName",
      { branch: "feat/fixture-generation" },
      { branch: "feat/fixture-generation" },
      { message: "Fixture generation" },
    ],
    [
      "generateThreadTitle",
      { title: " `Fixture   title` ", needsRefinement: true },
      { title: "Fixture title", needsRefinement: true },
      { message: "Fixture generation" },
    ],
    [
      "generateTranscriptionPostProcessing",
      { transcription: "Fixture transcription" },
      { transcription: "Fixture transcription" },
      { prompt: "Return the corrected fixture transcription" },
    ],
    [
      "generateReviewGuide",
      { stops: [] },
      { stops: [] },
      {
        number: 123,
        title: "Fixture PR",
        description: "Fixture changes",
        baseBranch: "main",
        baseCommit: "0123456789abcdef",
        settledReview: "Already checked",
      },
    ],
  ];
  for (const [operation, output, expected, input] of cases) {
    await check(
      `${operation} uses the selected executable and preserves its production result`,
      async () => {
        const f = await fixture({ output: JSON.stringify(output) });
        NodeAssert.deepEqual(
          value(
            await run(f, (service) =>
              service[operation]({ ...input, cwd: f.workspace, modelSelection: defaultSelection }),
            ),
          ),
          expected,
        );
        const rows = await closed(f);
        NodeAssert.deepEqual(
          rows.map((row) => row.stage),
          ["mcp", "exec"],
        );
        const exec = rows[1];
        NodeAssert.equal(exec.credential, credential);
        NodeAssert.equal(exec.cwd, f.workspace);
        NodeAssert.equal(exec.schemaMode, 0o600);
        NodeAssert.equal(exec.outputMode, 0o600);
        NodeAssert.equal(exec.schema.type, "object");
        NodeAssert.ok(exec.prompt.length > 0);
        NodeAssert.equal(argument(exec.args, "-s"), "read-only");
        NodeAssert.equal(argument(exec.args, "--model"), defaultSelection.model);
        NodeAssert.ok(
          exec.args.includes("--ephemeral") && exec.args.includes("--skip-git-repo-check"),
        );
        const frame = JSON.parse(f.frames.join(""));
        NodeAssert.equal(frame.timeoutMs, operation === "generateReviewGuide" ? 900_000 : 180_000);
        NodeAssert.ok(!f.frames.join("").includes(credential));
      },
    );
  }

  await check(
    "managed runtime keeps credentials and original custom models while suppressing service tier",
    async () => {
      const f = await fixture({
        managed: true,
        config: { customModels: ["private-preview"] },
        models: [{ slug: "gpt-6-astra", isDefault: true }],
      });
      const selection = createModelSelection(instanceId, "private-preview", [
        { id: "reasoningEffort", value: "high" },
        { id: "serviceTier", value: "priority" },
      ]);
      value(await run(f, (service) => title(service, f, { modelSelection: selection })));
      const exec = (await closed(f))[1];
      NodeAssert.equal(argument(exec.args, "--model"), "private-preview");
      NodeAssert.ok(exec.args.includes('model_reasoning_effort="high"'));
      NodeAssert.ok(!exec.args.some((arg) => arg.startsWith("service_tier=")));
      NodeAssert.equal(exec.codexHome, NodePath.join(f.home, "managed-home"));
      NodeAssert.equal(f.runtimeClosed, 1);
      NodeAssert.equal(f.commands[0].command, executable);
    },
  );

  await check("model fallback drops incompatible options and uses the listed default", async () => {
    const f = await fixture({ models: ["gpt-5.4-mini", { slug: "gpt-6-astra", isDefault: true }] });
    value(
      await run(f, (service) =>
        title(service, f, {
          modelSelection: createModelSelection(instanceId, "unavailable", [
            { id: "reasoningEffort", value: "xhigh" },
            { id: "serviceTier", value: "priority" },
          ]),
        }),
      ),
    );
    const exec = (await closed(f))[1];
    NodeAssert.equal(argument(exec.args, "--model"), "gpt-6-astra");
    NodeAssert.ok(exec.args.includes('model_reasoning_effort="low"'));
    NodeAssert.ok(!exec.args.some((arg) => arg.startsWith("service_tier=")));
  });

  await check(
    "launch overrides preserve quoted arguments and disable plugins, apps and named MCP servers",
    async () => {
      const f = await fixture({
        pathLookup: true,
        config: { launchArgs: "--config 'developer_instructions=\"ignored\"'" },
        environment: {
          T3CODE_CODEX_LAUNCH_ARGS:
            "--strict-config --config 'developer_instructions=\"space argument\"' --config features.plugins=true --config features.apps=true",
        },
        servers: [
          { name: "my.server", enabled: true },
          { name: 'quoted"server', enabled: true },
          { name: "off", enabled: false },
        ],
      });
      value(
        await run(f, (service) =>
          title(service, f, {
            modelSelection: createModelSelection(instanceId, defaultSelection.model, [
              { id: "serviceTier", value: "priority" },
            ]),
          }),
        ),
      );
      const [mcp, exec] = await closed(f);
      NodeAssert.ok(
        exec.args.includes('developer_instructions="space argument"') &&
          !exec.args.includes('developer_instructions="ignored"'),
      );
      NodeAssert.ok(!mcp.args.includes("--strict-config"));
      NodeAssert.ok(exec.args.includes("--strict-config"));
      NodeAssert.ok(
        exec.args.lastIndexOf("features.plugins=false") >
          exec.args.indexOf("features.plugins=true"),
      );
      NodeAssert.ok(
        exec.args.lastIndexOf("features.apps=false") > exec.args.indexOf("features.apps=true"),
      );
      NodeAssert.ok(
        exec.args.includes(
          'mcp_servers={"my.server"={enabled=false},"quoted\\"server"={enabled=false}}',
        ),
      );
      NodeAssert.ok(exec.args.includes('service_tier="priority"'));
    },
  );

  for (const [name, options] of [
    ["unsuccessful", { mcpExit: 7 }],
    ["malformed", { mcpRaw: "invalid JSON" }],
  ]) {
    await check(`${name} MCP discovery still permits the isolated generation command`, async () => {
      const f = await fixture(options);
      value(await run(f, (service) => title(service, f)));
      const exec = (await closed(f))[1];
      NodeAssert.ok(!exec.args.some((arg) => arg.startsWith("mcp_servers=")));
      NodeAssert.ok(
        exec.args.includes("features.plugins=false") && exec.args.includes("features.apps=false"),
      );
    });
  }

  await check("only authorized existing image files reach the native command", async () => {
    const f = await fixture();
    let imagePath;
    value(
      await run(f, (service) =>
        Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem;
          const config = yield* ServerConfig.ServerConfig;
          imagePath = NodePath.join(config.attachmentsDir, "fixture-image.png");
          yield* fs.makeDirectory(config.attachmentsDir, { recursive: true });
          yield* fs.writeFile(imagePath, new Uint8Array([1, 2, 3]));
          yield* fs.makeDirectory(NodePath.join(config.attachmentsDir, "directory.png"));
          return yield* title(service, f, {
            attachments: ["fixture-image", "missing", "directory", "../../outside"].map((id) => ({
              type: "image",
              id,
              name: "fixture.png",
              mimeType: "image/png",
              sizeBytes: 3,
            })),
          });
        }),
      ),
    );
    const exec = (await closed(f))[1];
    const images = exec.args.flatMap((arg, index) =>
      arg === "--image" ? [exec.args[index + 1]] : [],
    );
    NodeAssert.deepEqual(images, [imagePath]);
  });

  await check(
    "a closed ambient scope does not reap native generation files or managed credentials",
    async () => {
      const f = await fixture({
        managed: true,
        output: JSON.stringify({ branch: "feat/background" }),
      });
      NodeAssert.deepEqual(
        value(
          await run(f, (service) =>
            Effect.gen(function* () {
              const closedScope = yield* Scope.make();
              yield* Scope.close(closedScope, Exit.void);
              return yield* service
                .generateBranchName({
                  cwd: f.workspace,
                  message: "Background generation",
                  modelSelection: defaultSelection,
                })
                .pipe(Effect.provideService(Scope.Scope, closedScope));
            }),
          ),
        ),
        { branch: "feat/background" },
      );
      await closed(f);
      NodeAssert.equal(f.runtimeClosed, 1);
    },
  );

  for (const [name, options, expected] of [
    [
      "invalid structured output",
      { output: JSON.stringify({ title: 7 }) },
      "Codex returned invalid structured output.",
    ],
    ["missing output file", { removeOutput: true }, "Failed to read Codex output file."],
    [
      "CLI failure stderr",
      { exitCode: 7, stderr: " fixture failure \n", stdout: "ignored stdout" },
      "Codex CLI command failed: fixture failure",
    ],
  ]) {
    await check(`${name} preserves the production error and cleans up`, async () => {
      const f = await fixture(options);
      const error = failure(await run(f, (service) => title(service, f)));
      NodeAssert.equal(error.detail, expected);
      await closed(f);
    });
  }

  for (const stage of ["mcp", "exec"]) {
    await check(`interruption reaps the native runner and its stalled ${stage} child`, async () => {
      const f = await fixture({ hold: stage });
      const exit = await run(f, (service) =>
        Effect.gen(function* () {
          const fiber = yield* title(service, f).pipe(Effect.forkScoped);
          const pid = yield* f.probe.started;
          yield* Fiber.interrupt(fiber);
          yield* f.probe.exited;
          assertDead(pid);
          const result = yield* Fiber.await(fiber);
          NodeAssert.ok(Exit.isFailure(result) && Cause.hasInterruptsOnly(result.cause));
        }),
      );
      value(exit);
      await closed(f);
    });
  }

  for (const phase of ["runtime", "models", "process"]) {
    await check(
      `the outer request deadline covers ${phase} and releases its resources`,
      async () => {
        const f = await fixture(phase === "process" ? { hold: "exec" } : {});
        const started = Deferred.makeUnsafe();
        const blocked = Deferred.succeed(started, undefined).pipe(Effect.andThen(Effect.never));
        if (phase === "runtime") f.runtime = blocked;
        if (phase === "models") f.models = blocked;
        value(
          await run(
            f,
            (service) =>
              Effect.gen(function* () {
                const fiber = yield* title(service, f).pipe(Effect.forkScoped);
                if (phase === "process") yield* f.probe.started;
                else yield* Deferred.await(started);
                yield* TestClock.adjust(180_000);
                const error = failure(yield* Fiber.await(fiber));
                NodeAssert.equal(error.detail, "Codex CLI request timed out.");
                if (phase === "process") yield* f.probe.exited;
              }),
            { clock: true },
          ),
        );
        await closed(f);
        NodeAssert.equal(f.commands.length, phase === "process" ? 1 : 0);
      },
    );
  }

  for (const [name, selected, platform] of [
    ["relative executable", "relative/generate"],
    ["empty executable", ""],
    ["unsupported host", executable, "win32"],
  ]) {
    await check(`${name} fails before launching a provider`, async () => {
      const f = await fixture();
      const error = failure(await run(f, (service) => title(service, f), { selected, platform }));
      NodeAssert.equal(error.cause._tag, "NativeCodexTextGenerationError");
      NodeAssert.equal(error.cause.stage, "configuration");
      NodeAssert.equal(f.commands.length, 0);
      await closed(f);
    });
  }

  const badResponses = [
    ["missing response", ""],
    ["truncated response", '{"version":1,"ok":{"output":"{}"}}'],
    ["extra response", '{"version":1,"ok":{"output":"{}"}}\n{}\n'],
    ["unknown version", '{"version":2,"ok":{"output":"{}"}}\n'],
    ["unknown fields", '{"version":1,"ok":{"output":"{}","extra":true}}\n'],
    ["wrong output type", '{"version":1,"ok":{"output":{}}}\n'],
  ];
  for (const [name, output] of badResponses) {
    await check(`${name} cannot fall back to the TypeScript CLI runner`, async () => {
      const f = await fixture();
      const invalid = NodePath.join(f.directory, "invalid-native");
      await NodeFSP.writeFile(
        invalid,
        `#!${process.execPath}\nfor await (const chunk of process.stdin) {}\nprocess.stdout.write(${JSON.stringify(output)});\n`,
        { mode: 0o700 },
      );
      const error = failure(await run(f, (service) => title(service, f), { selected: invalid }));
      NodeAssert.equal(error.cause._tag, "NativeCodexTextGenerationError");
      NodeAssert.equal(error.cause.stage, "response");
      NodeAssert.equal(f.commands.length, 1);
      NodeAssert.deepEqual(await closed(f), []);
    });
  }

  for (const [name, source, stage] of [
    ["invalid UTF-8", "process.stdout.write(Buffer.from([255, 10]));", "response"],
    [
      "oversized response",
      "process.stdout.write(Buffer.alloc(24 * 1024 * 1024 + 1025, 120));",
      "response",
    ],
    [
      "oversized output value",
      'process.stdout.write(JSON.stringify({ version: 1, ok: { output: "x".repeat(4 * 1024 * 1024 + 1) } }) + "\\n");',
      "response",
    ],
    ["native process failure", "process.exitCode = 7;", "process"],
  ]) {
    await check(`${name} is a bounded typed failure without a provider fallback`, async () => {
      const f = await fixture();
      const invalid = NodePath.join(f.directory, "invalid-native");
      await NodeFSP.writeFile(
        invalid,
        `#!${process.execPath}\nfor await (const chunk of process.stdin) {}\n${source}\n`,
        { mode: 0o700 },
      );
      const error = failure(await run(f, (service) => title(service, f), { selected: invalid }));
      NodeAssert.equal(error.cause._tag, "NativeCodexTextGenerationError");
      NodeAssert.equal(error.cause.stage, stage);
      NodeAssert.equal(f.commands.length, 1);
      NodeAssert.deepEqual(await closed(f), []);
    });
  }

  await check("an oversized request is rejected before spawning the native runner", async () => {
    const f = await fixture();
    const error = failure(
      await run(f, (service) =>
        service.generateTranscriptionPostProcessing({
          cwd: f.workspace,
          prompt: "x".repeat(8 * 1024 * 1024),
          modelSelection: defaultSelection,
        }),
      ),
      "generateTranscriptionPostProcessing",
    );
    NodeAssert.equal(error.cause.stage, "request");
    NodeAssert.equal(f.commands.length, 0);
    await closed(f);
  });

  await check("a missing selected executable cannot use the original CLI path", async () => {
    const f = await fixture();
    const error = failure(
      await run(f, (service) => title(service, f), {
        selected: NodePath.join(f.directory, "absent-native"),
      }),
    );
    NodeAssert.equal(error.cause.stage, "process");
    NodeAssert.equal(f.commands.length, 1);
    NodeAssert.deepEqual(await closed(f), []);
  });

  await check("an unset selector uses the default native generation executable", async () => {
    const f = await fixture({ environment: { T3_CODE_LS_GENERATION_EXECUTABLE: executable } });
    NodeAssert.deepEqual(
      value(await run(f, (service) => title(service, f), { unselected: true })),
      { title: "Fixture title" },
    );
    NodeAssert.equal(f.commands.length, 1);
    NodeAssert.equal(f.commands[0].command, effectiveDefault());
    await closed(f);
  });

  await check(
    "the application TextGeneration service routes to the selected native implementation",
    async () => {
      const f = await fixture({ output: JSON.stringify({ transcription: "Routed fixture" }) });
      let textGeneratorLookups = 0;
      NodeAssert.deepEqual(
        value(
          await run(f, (provider) =>
            Effect.gen(function* () {
              const instance = {
                instanceId,
                textGeneration: provider,
                snapshot: {
                  getSnapshot: Effect.succeed({
                    instanceId,
                    driver: ProviderDriverKind.make("codex"),
                    enabled: true,
                    installed: true,
                    status: "ready",
                    auth: { status: "authenticated" },
                  }),
                },
              };
              const service = yield* TextGeneration.make.pipe(
                Effect.provide(
                  Layer.mergeAll(
                    Layer.mock(ProviderInstanceRegistry.ProviderInstanceRegistry)({
                      listInstances: Effect.succeed([instance]),
                      getInstance: () => Effect.succeed(instance),
                    }),
                    Layer.mock(SourceControlProviderRegistry.SourceControlProviderRegistry)({
                      resolveLink: () => Effect.die("Unexpected source-control lookup"),
                    }),
                    Layer.mock(TextGenerators.TextGenerators)({
                      runner: (selectedInstanceId) => {
                        NodeAssert.equal(selectedInstanceId, instanceId);
                        textGeneratorLookups++;
                        return Effect.succeed(undefined);
                      },
                      listModels: () => Effect.die("Unexpected text-generator model lookup"),
                    }),
                    ServerSettings.layerTest({}),
                  ),
                ),
              );
              return yield* service.generateTranscriptionPostProcessing({
                cwd: f.workspace,
                prompt: "Routed fixture",
                modelSelection: defaultSelection,
              });
            }),
          ),
        ),
        { transcription: "Routed fixture" },
      );
      NodeAssert.equal(textGeneratorLookups, 1);
      await closed(f);
    },
  );

  const sources = [
    "apps/server/src/textGeneration/NativeCodexTextGeneration.ts",
    "apps/server/src/textGeneration/CodexTextGeneration.ts",
    "apps/server/src/textGeneration/TextGeneration.ts",
    "apps/server/src/textGeneration/TextGenerators.ts",
    "tools/check-generation-adapter.mjs",
  ];
  const sourceHashes = Object.fromEntries(
    await Promise.all(
      sources.map(async (source) => [
        source,
        NodeCrypto.createHash("sha256")
          .update(await NodeFSP.readFile(NodePath.join(root, source)))
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
    sourceHashes,
    passed: scenarios.length,
    nativeProcesses: totalNativeProcesses,
    protocolFixtureProcesses: totalProtocolFixtureProcesses,
    controlledCliProcesses: totalCliProcesses,
    checks: scenarios,
    boundaries: [
      "real production services and native processes execute with an isolated controlled Codex CLI",
      "no real provider request, credential, user data or installation is used",
      "managed credential resolution, attachment authorization, prompt construction and final schema validation remain host responsibilities",
      "this gate executes on Unix and does not qualify Windows or mobile execution",
    ],
  };
  if (proofPath) await NodeFSP.writeFile(proofPath, `${JSON.stringify(proof, null, 2)}\n`);
  console.log(JSON.stringify(proof));
} finally {
  await NodeFSP.rm(temporary, { recursive: true, force: true });
}
