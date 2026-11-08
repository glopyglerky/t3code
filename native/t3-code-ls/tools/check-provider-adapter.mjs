#!/usr/bin/env node
import * as NodeAssert from "node:assert/strict";
import * as NodeCrypto from "node:crypto";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";
import { CodexSettings } from "@t3tools/contracts";
import { AgentScope, agentScopeCommand } from "@t3tools/shared/AgentScope";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as ProviderEventLoggers from "@t3tools/provider-core/server/ProviderEventLoggers";
import * as CodexProvider from "../../../apps/server/src/provider/CodexProvider.ts";
import * as CodexAdapter from "../../../apps/server/src/orchestration-v2/Adapters/CodexAdapterV2.ts";
import * as PortScanner from "../../../apps/server/src/preview/PortScanner.ts";
import {
  CodexAppServerProcessConfigurationError,
  selectCodexAppServerCommand,
} from "../../../apps/server/src/provider/CodexAppServerProcess.ts";

NodeAssert.ok(process.argv[2], "Pass the Lust-built provider executable");
const executable = NodePath.resolve(process.argv[2]);
const hostPlatform = Effect.runSync(HostProcess.Platform);
const selectionOnly = process.argv.includes("--selection-only");
const proofPath = process.argv[3] === "--selection-only" ? undefined : process.argv[3];
const upstreamPeer = NodeURL.fileURLToPath(
  new URL(
    "../../../packages/effect-codex-app-server/test/fixtures/codex-app-server-mock-peer.ts",
    import.meta.url,
  ),
);
const temporary = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "t3ls-provider-adapter-"));
const home = NodePath.join(temporary, "home");
const codexHome = NodePath.join(temporary, "separate-codex-home");
const workspace = NodePath.join(temporary, "workspace");
await Promise.all([home, codexHome, workspace].map((directory) => NodeFSP.mkdir(directory)));
const peer = NodePath.join(temporary, "codex-fixture");
const credential = "synthetic-provider-credential-never-an-argument";
const defaultSettings = Schema.decodeSync(CodexSettings)({});
const scenarios = [];
let nextAudit = 0;

// The peer records only synthetic test values. Protocol success uses upstream's
// unchanged fixture and the real generated Effect client, not a replacement client.
await NodeFSP.writeFile(
  peer,
  `#!${process.execPath}
import { writeFileSync } from "node:fs";
import { pathToFileURL } from "node:url";
writeFileSync(process.env.T3LS_ADAPTER_AUDIT, JSON.stringify({
  pid: process.pid, ppid: process.ppid, cwd: process.cwd(), argv: process.argv.slice(2),
  home: process.env.HOME ?? null, codexHome: process.env.CODEX_HOME ?? null,
  credential: process.env.T3LS_ADAPTER_CREDENTIAL,
  selector: process.env.T3_CODE_LS_PROVIDER_EXECUTABLE ?? null,
  originator: process.env.CODEX_INTERNAL_ORIGINATOR_OVERRIDE,
}));
if (process.env.T3LS_ADAPTER_MODE === "exit") {
  process.stderr.write("Error: synthetic Codex startup failure\\n", () => process.exit(7));
} else if (["status", "hold"].includes(process.env.T3LS_ADAPTER_MODE)) {
  let pending = "";
  process.stdin.setEncoding("utf8");
  process.stdin.on("data", (chunk) => {
    pending += chunk;
    const lines = pending.split("\\n");
    pending = lines.pop();
    for (const line of lines) {
      if (!line.trim()) continue;
      const message = JSON.parse(line);
      if (message.method === "fixture/hold") {
        process.stdout.write(JSON.stringify({ method: "fixture/received", params: { index: message.params.index } }) + "\\n");
        continue;
      }
      const result = message.method === "initialize"
        ? { userAgent: "fixture/0.159.0", codexHome: process.env.CODEX_HOME, platformFamily: "unix", platformOs: process.platform }
        : message.method === "account/read" ? { account: null, requiresOpenaiAuth: true } : undefined;
      if (result) process.stdout.write(JSON.stringify({ id: message.id, result }) + "\\n");
    }
  });
  process.stdin.on("end", () => process.exit(0));
} else {
  await import(pathToFileURL(${JSON.stringify(upstreamPeer)}).href);
}
`,
  { mode: 0o700 },
);

async function check(name, body) {
  await body();
  scenarios.push(name);
  console.log(`Passed: ${name}`);
}

function environment(audit, extra = {}) {
  return {
    HOME: home,
    CODEX_HOME: codexHome,
    T3LS_ADAPTER_AUDIT: audit,
    T3LS_ADAPTER_CREDENTIAL: credential,
    ...extra,
  };
}

function settings(binaryPath = peer) {
  return {
    ...defaultSettings,
    enabled: true,
    binaryPath,
    launchArgs: "--fixture 'space argument'",
  };
}

function observe(options = {}) {
  return {
    selected: options.selected,
    host: options.host ?? hostPlatform,
    cwd: options.cwd ?? process.cwd(),
    handles: [],
    commands: [],
    scopes: [],
    running: 0,
  };
}

// Only external process/OS services are observed. Both production launch paths,
// their client factory, Effect's Node spawner, and generated codecs execute here.
function run(operation, observation, options) {
  const program = Effect.gen(function* () {
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const originalScope = yield* AgentScope;
    const observedSpawner = ChildProcessSpawner.make((command) =>
      spawner.spawn(command).pipe(
        Effect.tap((handle) =>
          Effect.sync(() => {
            observation.commands.push(command);
            observation.handles.push(handle);
          }),
        ),
      ),
    );
    return yield* operation.pipe(
      Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, observedSpawner),
      Effect.provideService(AgentScope, {
        ...originalScope,
        wrap: (input) =>
          Effect.sync(() => {
            observation.scopes.push(input);
            return agentScopeCommand(input);
          }),
      }),
      Effect.provideService(
        ProviderEventLoggers.ProviderEventLoggers,
        ProviderEventLoggers.NoOpProviderEventLoggers,
      ),
      Effect.provideService(PortScanner.PortDiscovery, {
        trackAgent: () => {
          observation.running++;
          return () => {
            observation.running--;
          };
        },
      }),
    );
  }).pipe(
    Effect.scoped,
    Effect.provide(NodeServices.layer),
    Effect.provideService(
      HostProcess.Environment,
      observation.selected === undefined
        ? {}
        : { T3_CODE_LS_PROVIDER_EXECUTABLE: observation.selected },
    ),
    Effect.provideService(HostProcess.Platform, observation.host),
    Effect.provideService(HostProcess.WorkingDirectory, observation.cwd),
    Effect.provideService(HostProcess.HomeDirectory, home),
  );
  return Effect.runPromiseExit(program, options);
}

function value(exit) {
  if (Exit.isFailure(exit)) throw Cause.squash(exit.cause);
  return exit.value;
}

function failure(exit) {
  NodeAssert.ok(Exit.isFailure(exit), "Expected a typed failure");
  return Cause.squash(exit.cause);
}

function assertDead(pid) {
  NodeAssert.throws(
    () => process.kill(pid, 0),
    (error) => error.code === "ESRCH",
    `Owned process ${pid} survived its Effect scope`,
  );
}

function assertClosed(observation, audit) {
  NodeAssert.equal(observation.running, 0, "Agent discovery retained a closed session");
  for (const handle of observation.handles) assertDead(handle.pid);
  if (audit) assertDead(audit.pid);
}

function open(kind, input, observation) {
  if (kind === "probe")
    return CodexProvider.withCodexAppServerClient({
      binaryPath: input.binaryPath ?? peer,
      cwd: input.cwd ?? workspace,
      launchArgs: settings().launchArgs,
      environment: input.environment,
      ...(input.homePath === undefined ? {} : { homePath: input.homePath }),
    }).pipe(Effect.map(({ client }) => client));
  return Effect.gen(function* () {
    const context = yield* Layer.build(CodexAdapter.layerAppServerClientFactory);
    const factory = Context.get(context, CodexAdapter.CodexAppServerClientFactory);
    const client = yield* factory.open({
      instanceId: CodexAdapter.CODEX_DEFAULT_INSTANCE_ID,
      threadId: "thread-provider-integration",
      providerSessionId: "session-provider-integration",
      runtimePolicy: { runtimeMode: "full-access", interactionMode: "default", cwd: workspace },
      settings: {
        ...settings(input.binaryPath),
        ...(input.homePath === undefined ? {} : { homePath: input.homePath }),
      },
      environment: input.environment,
    });
    NodeAssert.equal(observation.running, 1);
    return client;
  });
}

const conversation = Effect.fn("checkProviderAdapter.conversation")(
  function* (kind, input, observation, after) {
    const client = yield* open(kind, input, observation);
    const delta = yield* Deferred.make();
    let approvals = 0;
    yield* client.handleServerRequest("item/tool/requestUserInput", (request) =>
      Effect.sync(() => {
        NodeAssert.equal(request.questions[0].id, "approved");
        approvals++;
        return { answers: { approved: { answers: ["yes"] } } };
      }),
    );
    yield* client.handleServerNotification("item/agentMessage/delta", (notification) =>
      Deferred.succeed(delta, notification.delta),
    );
    if (kind === "session") {
      const initialized = yield* client.request(
        "initialize",
        CodexProvider.buildCodexInitializeParams(),
      );
      NodeAssert.equal(initialized.userAgent, "mock-codex-app-server");
    }
    yield* client.notify("initialized", undefined);
    NodeAssert.equal(yield* Deferred.await(delta), "Mock server is ready.");
    const account = yield* client.request("account/read", {});
    NodeAssert.equal(account.account.email, "mock@example.com");
    const skills = yield* client.request("skills/list", { cwds: [workspace] });
    NodeAssert.equal(skills.data.length, 1);
    NodeAssert.equal(approvals, 1);
    // The approval exchange must leave the shared connection usable.
    NodeAssert.deepEqual(yield* client.request("account/read", {}), account);
    if (after) return yield* after(client);
    return { account, skills };
  },
);

try {
  await check("unset selection preserves original command and arguments", async () => {
    const input = { command: "codex", args: ["app-server", "argument with spaces"] };
    const result = value(await run(selectCodexAppServerCommand(input), observe()));
    NodeAssert.equal(result.command, input.command);
    NodeAssert.deepEqual(result.args, input.args);
  });
  await check(
    "selection resolves cwd without passing environment values as arguments",
    async () => {
      const result = value(
        await run(
          selectCodexAppServerCommand({
            command: "./codex",
            args: ["app-server"],
            cwd: "workspace",
          }),
          observe({ selected: executable, cwd: temporary }),
        ),
      );
      NodeAssert.equal(result.command, executable);
      NodeAssert.equal(result.args[result.args.indexOf("--program") + 1], "./codex");
      NodeAssert.equal(result.args[result.args.indexOf("--cwd") + 1], workspace);
      for (const flag of ["--inherit-env", "--inherit-stderr", "--parent-protocol"])
        NodeAssert.ok(result.args.includes(flag));
      NodeAssert.equal(result.args[result.args.indexOf("--request-timeout-ms") + 1], "0");
      NodeAssert.deepEqual(result.args.slice(result.args.indexOf("--") + 1), ["app-server"]);
    },
  );
  for (const invalid of ["", "relative-provider", "/invalid\0provider"]) {
    await check(
      `invalid configured selector fails before spawning: ${JSON.stringify(invalid)}`,
      async () => {
        for (const kind of ["probe", "session"]) {
          const observation = observe({ selected: invalid });
          const error = failure(
            await run(
              open(
                kind,
                { environment: environment(NodePath.join(temporary, "never-started")) },
                observation,
              ),
              observation,
            ),
          );
          const configError = kind === "session" ? error.cause : error;
          NodeAssert.ok(configError instanceof CodexAppServerProcessConfigurationError);
          NodeAssert.equal(configError.reason, "invalid-executable");
          NodeAssert.equal(observation.handles.length, 0);
          NodeAssert.equal(observation.scopes.length, 0);
        }
      },
    );
  }
  await check("configured runtime rejects unsupported host before launching", async () => {
    const error = failure(
      await run(
        selectCodexAppServerCommand({ command: peer, args: [] }),
        observe({ selected: executable, host: "win32" }),
      ),
    );
    NodeAssert.ok(error instanceof CodexAppServerProcessConfigurationError);
    NodeAssert.equal(error.reason, "unsupported-platform");
  });
  await check(
    "status preserves native configuration failure without running original version probe",
    async () => {
      const observation = observe({ selected: "relative-provider" });
      const result = value(
        await run(
          CodexProvider.checkCodexProviderStatus(
            settings(),
            undefined,
            environment(NodePath.join(temporary, "status-never-started")),
          ),
          observation,
        ),
      );
      NodeAssert.equal(result.status, "error");
      NodeAssert.match(result.message, /T3_CODE_LS_PROVIDER_EXECUTABLE/);
      NodeAssert.equal(observation.handles.length, 0);
    },
  );

  if (!selectionOnly) {
    for (const kind of ["probe", "session"]) {
      let expected;
      for (const selected of [undefined, executable]) {
        await check(
          `${kind} ${selected ? "native" : "direct"} runs generated RPC, approval, notification, and cleanup`,
          async () => {
            const auditPath = NodePath.join(temporary, `audit-${nextAudit++}`);
            const observation = observe({ selected });
            const result = value(
              await run(
                conversation(
                  kind,
                  {
                    environment: environment(auditPath, {
                      T3_CODE_LS_PROVIDER_EXECUTABLE: "/must-not-select-provider-override",
                    }),
                  },
                  observation,
                ),
                observation,
              ),
            );
            if (expected === undefined) expected = result;
            else NodeAssert.deepEqual(result, expected);
            const audit = JSON.parse(await NodeFSP.readFile(auditPath, "utf8"));
            NodeAssert.deepEqual(audit.argv, ["app-server", "--fixture", "space argument"]);
            NodeAssert.equal(audit.home, home);
            NodeAssert.equal(audit.codexHome, codexHome);
            NodeAssert.equal(audit.credential, credential);
            NodeAssert.equal(audit.selector, "/must-not-select-provider-override");
            NodeAssert.equal(audit.originator, "T3 Code");
            NodeAssert.equal(audit.cwd, kind === "probe" ? workspace : process.cwd());
            NodeAssert.equal(observation.handles.length, 1);
            NodeAssert.ok(!observation.commands[0].args.join("\0").includes(credential));
            if (selected) NodeAssert.equal(audit.ppid, observation.handles[0].pid);
            if (kind === "session") {
              NodeAssert.equal(observation.scopes.length, 1);
              NodeAssert.equal(observation.scopes[0].command, selected ?? peer);
              NodeAssert.equal(observation.scopes[0].shared, true);
              NodeAssert.equal(observation.scopes[0].threadId, "thread-provider-integration");
              NodeAssert.ok(!observation.scopes[0].args.join("\0").includes(credential));
            }
            assertClosed(observation, audit);
          },
        );
      }
      await check(
        `${kind} native cancellation reaps wrapper and provider after live traffic`,
        async () => {
          const controller = new AbortController();
          const auditPath = NodePath.join(temporary, `audit-${nextAudit++}`);
          const observation = observe({ selected: executable });
          let markReady;
          const ready = new Promise((resolveReady) => {
            markReady = resolveReady;
          });
          const pending = run(
            conversation(kind, { environment: environment(auditPath) }, observation, () =>
              Effect.sync(markReady).pipe(Effect.andThen(Effect.never)),
            ),
            observation,
            { signal: controller.signal },
          );
          try {
            await Promise.race([
              ready,
              pending.then((exit) => {
                throw failure(exit);
              }),
            ]);
            controller.abort();
            const exit = await pending;
            NodeAssert.ok(Exit.isFailure(exit));
            NodeAssert.ok(Cause.hasInterrupts(exit.cause));
            assertClosed(observation, JSON.parse(await NodeFSP.readFile(auditPath, "utf8")));
          } finally {
            controller.abort();
            await pending;
          }
        },
      );
      let directError;
      for (const selected of [undefined, executable]) {
        await check(
          `${kind} ${selected ? "native" : "direct"} keeps provider exit code and stderr diagnosis`,
          async () => {
            const auditPath = NodePath.join(temporary, `audit-${nextAudit++}`);
            const observation = observe({ selected });
            const operation = Effect.gen(function* () {
              const client = yield* open(
                kind,
                { environment: environment(auditPath, { T3LS_ADAPTER_MODE: "exit" }) },
                observation,
              );
              return yield* client.request(
                "initialize",
                CodexProvider.buildCodexInitializeParams(),
              );
            });
            const error = failure(await run(operation, observation));
            const diagnosis = { tag: error._tag, code: error.code, stderr: error.stderr };
            NodeAssert.deepEqual(diagnosis, {
              tag: "CodexAppServerProcessExitedError",
              code: 7,
              stderr: "Error: synthetic Codex startup failure",
            });
            if (directError === undefined) directError = diagnosis;
            else NodeAssert.deepEqual(diagnosis, directError);
            assertClosed(observation, JSON.parse(await NodeFSP.readFile(auditPath, "utf8")));
          },
        );
      }
    }
    await check(
      "status uses native app-server and preserves authentication diagnosis",
      async () => {
        const auditPath = NodePath.join(temporary, `audit-${nextAudit++}`);
        const observation = observe({ selected: executable });
        const result = value(
          await run(
            CodexProvider.checkCodexProviderStatus(
              settings(),
              undefined,
              environment(auditPath, { T3LS_ADAPTER_MODE: "status" }),
            ),
            observation,
          ),
        );
        NodeAssert.equal(result.auth.status, "unauthenticated");
        NodeAssert.equal(result.version, "0.159.0");
        NodeAssert.equal(observation.handles.length, 1);
        const audit = JSON.parse(await NodeFSP.readFile(auditPath, "utf8"));
        NodeAssert.equal(audit.ppid, observation.handles[0].pid);
        assertClosed(observation, audit);
      },
    );
    await check(
      "configured launcher exit never falls back to original version command",
      async () => {
        const broken = NodePath.join(temporary, "broken-launcher");
        await NodeFSP.writeFile(broken, `#!${process.execPath}\nprocess.exit(7);\n`, {
          mode: 0o700,
        });
        const auditPath = NodePath.join(temporary, `audit-${nextAudit++}`);
        const observation = observe({ selected: broken });
        const result = value(
          await run(
            CodexProvider.checkCodexProviderStatus(settings(), undefined, environment(auditPath)),
            observation,
          ),
        );
        NodeAssert.equal(result.status, "error");
        NodeAssert.equal(observation.handles.length, 1);
        await NodeAssert.rejects(NodeFSP.readFile(auditPath), (error) => error.code === "ENOENT");
        assertClosed(observation);
      },
    );
    await check("missing selected launcher fails through both production paths", async () => {
      for (const kind of ["probe", "session"]) {
        const observation = observe({ selected: NodePath.join(temporary, "missing-launcher") });
        const auditPath = NodePath.join(temporary, `audit-${nextAudit++}`);
        const error = failure(
          await run(
            open(kind, { environment: environment(auditPath) }, observation).pipe(
              Effect.flatMap((client) => client.awaitTermination),
            ),
            observation,
          ),
        );
        if (kind === "probe") NodeAssert.equal(error._tag, "CodexAppServerSpawnError");
        else
          NodeAssert.ok(
            ["CodexAppServerProcessExitedError", "CodexAppServerTransportError"].includes(
              error._tag,
            ),
          );
        // AgentScope successfully spawns its shell, whose exec then reports 127.
        if (kind === "session")
          NodeAssert.equal(await Effect.runPromise(observation.handles[0].exitCode), 127);
        NodeAssert.equal(observation.handles.length, kind === "probe" ? 0 : 1);
        await NodeAssert.rejects(NodeFSP.readFile(auditPath), (error) => error.code === "ENOENT");
        assertClosed(observation);
      }
    });
    for (const binaryPath of [NodePath.basename(peer), "../codex-fixture"]) {
      await check(
        `native probe resolves ${binaryPath} and keeps configured Codex home separate`,
        async () => {
          const auditPath = NodePath.join(temporary, `audit-${nextAudit++}`);
          const observation = observe({ selected: executable });
          value(
            await run(
              conversation(
                "probe",
                {
                  binaryPath,
                  homePath: "~/specific-codex",
                  environment: environment(auditPath, { PATH: temporary }),
                },
                observation,
              ),
              observation,
            ),
          );
          const audit = JSON.parse(await NodeFSP.readFile(auditPath, "utf8"));
          NodeAssert.equal(audit.home, home);
          NodeAssert.equal(audit.codexHome, NodePath.join(home, "specific-codex"));
          assertClosed(observation, audit);
        },
      );
    }
    await check("native probe preserves an absent HOME and independent CODEX_HOME", async () => {
      const auditPath = NodePath.join(temporary, `audit-${nextAudit++}`);
      const observation = observe({ selected: executable });
      value(
        await run(
          conversation(
            "probe",
            {
              environment: environment(auditPath, { HOME: undefined }),
            },
            observation,
          ),
          observation,
        ),
      );
      const audit = JSON.parse(await NodeFSP.readFile(auditPath, "utf8"));
      NodeAssert.equal(audit.home, null);
      NodeAssert.equal(audit.codexHome, codexHome);
      assertClosed(observation, audit);
    });
    for (const selected of [undefined, executable]) {
      await check(
        `${selected ? "native" : "direct"} session survives 129 interrupted unanswered requests`,
        async () => {
          const auditPath = NodePath.join(temporary, `audit-${nextAudit++}`);
          const observation = observe({ selected });
          value(
            await run(
              Effect.gen(function* () {
                const client = yield* open(
                  "session",
                  {
                    environment: environment(auditPath, { T3LS_ADAPTER_MODE: "hold" }),
                  },
                  observation,
                );
                yield* client.request("initialize", CodexProvider.buildCodexInitializeParams());
                const received = new Map();
                yield* client.handleUnknownServerNotification((method, params) => {
                  NodeAssert.equal(method, "fixture/received");
                  const milestone = received.get(params.index);
                  NodeAssert.ok(milestone);
                  return Deferred.succeed(milestone, undefined);
                });
                for (let index = 0; index < 129; index++) {
                  const milestone = yield* Deferred.make();
                  received.set(index, milestone);
                  const pending = yield* client.raw
                    .request("fixture/hold", { index })
                    .pipe(Effect.forkScoped);
                  yield* Effect.raceFirst(Deferred.await(milestone), client.awaitTermination);
                  yield* Fiber.interrupt(pending);
                  const exit = yield* Fiber.await(pending);
                  NodeAssert.ok(Exit.isFailure(exit) && Cause.hasInterruptsOnly(exit.cause));
                  received.delete(index);
                }
                NodeAssert.equal(
                  (yield* client.request("account/read", {})).requiresOpenaiAuth,
                  true,
                );
              }),
              observation,
            ),
          );
          assertClosed(observation, JSON.parse(await NodeFSP.readFile(auditPath, "utf8")));
        },
      );
    }
  }
  const sourcePaths = [
    "apps/server/src/provider/CodexAppServerProcess.ts",
    "apps/server/src/provider/CodexProvider.ts",
    "apps/server/src/orchestration-v2/Adapters/CodexAdapterV2.ts",
    "apps/server/src/provider/codexLaunchArgs.ts",
    "apps/server/src/preview/PortScanner.ts",
    "apps/server/package.json",
    "packages/contracts/package.json",
    "packages/contracts/src/index.ts",
    "packages/contracts/src/settings.ts",
    "packages/shared/package.json",
    "packages/shared/src/AgentScope.ts",
    "packages/shared/src/HostProcess.ts",
    "packages/shared/src/shell.ts",
    "packages/shared/src/cliArgs.ts",
    "packages/provider-core/package.json",
    "packages/provider-core/src/server/ProviderEventLoggers.ts",
    "packages/provider-core/src/server/snapshotProbe.ts",
    "packages/effect-codex-app-server/package.json",
    "packages/effect-codex-app-server/src/client.ts",
    "packages/effect-codex-app-server/src/protocol.ts",
    "packages/effect-codex-app-server/src/errors.ts",
    "packages/effect-codex-app-server/src/schema.ts",
    "packages/effect-codex-app-server/src/_generated/meta.gen.ts",
    "packages/effect-codex-app-server/src/_generated/namespaces.gen.ts",
    "packages/effect-codex-app-server/src/_generated/schema.gen.ts",
    "packages/effect-codex-app-server/src/_internal/shared.ts",
    "packages/effect-codex-app-server/src/_internal/stdio.ts",
    "packages/effect-codex-app-server/test/fixtures/codex-app-server-mock-peer.ts",
    "native/t3-code-ls/tools/check-provider-adapter.mjs",
    "native/t3-code-ls/tools/provider-tsconfig.json",
    "native/t3-code-ls/tools/install-server-tests.mjs",
    "native/t3-code-ls/tools/server-test-runtime/package.json",
    "native/t3-code-ls/tools/server-test-runtime/package-lock.json",
    "native/t3-code-ls/upstream.lock.json",
    "tsconfig.base.json",
    "tsconfig.effect.json",
    "patches/mdast-util-math@3.0.0.patch",
    "patches/micromark-extension-llm-math@3.1.0.patch",
    "patches/micromark-extension-gfm-table@2.1.1.patch",
    "patches/micromark@4.0.2.patch",
    "patches/mdast-util-to-hast@13.2.1.patch",
  ];
  const repositoryRoot = new URL("../../../", import.meta.url);
  const sourceHashes = Object.fromEntries(
    await Promise.all(
      sourcePaths.map(async (path) => [
        path,
        NodeCrypto.createHash("sha256")
          .update(await NodeFSP.readFile(new URL(path, repositoryRoot)))
          .digest("hex"),
      ]),
    ),
  );
  const proof = {
    status: "passed",
    scenarios,
    source_sha256: sourceHashes,
    ...(selectionOnly
      ? {}
      : {
          binary_sha256: NodeCrypto.createHash("sha256")
            .update(await NodeFSP.readFile(executable))
            .digest("hex"),
        }),
    scope:
      "Actual production Codex probe and session client factory, generated schemas, Effect Node process lifecycle, and native provider transport. Not full application or other provider parity.",
  };
  if (proofPath) await NodeFSP.writeFile(proofPath, JSON.stringify(proof, null, 2) + "\n");
  console.log(
    `${scenarios.length} actual provider integration scenarios passed${selectionOnly ? " (selection only)" : ""}.`,
  );
} finally {
  await NodeFSP.rm(temporary, { recursive: true, force: true });
}
