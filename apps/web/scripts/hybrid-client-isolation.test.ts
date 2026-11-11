// @effect-diagnostics nodeBuiltinImport:off - offline acceptance harness isolation regressions.
import * as NodeAssert from "node:assert/strict";
import * as NodeChildProcess from "node:child_process";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeTest from "node:test";
import {
  applicationLaunch,
  browserIsolationArgs,
  createChildIsolation,
  enabledAvailableProviders,
  loopbackGuard,
  rejectDotenvFiles,
  replayConfiguration,
} from "./hybrid-client-isolation.ts";

function temporary(t: { after(callback: () => void): void }): string {
  const directory = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "hybrid-isolation-test-"));
  t.after(() => NodeFS.rmSync(directory, { recursive: true, force: true }));
  return directory;
}

NodeTest.test(
  "provider selection matches omitted availability and excludes disabled or blocked drivers",
  () => {
    const replay = { instanceId: "devReplay", driver: "acpRegistry", enabled: true };
    NodeAssert.deepEqual(
      enabledAvailableProviders([
        replay,
        { instanceId: "codex", driver: "codex", enabled: true, availability: "unavailable" },
        { instanceId: "disabled", driver: "acpRegistry", enabled: false },
      ]),
      [{ instanceId: "devReplay", driver: "acpRegistry" }],
    );
    NodeAssert.deepEqual(enabledAvailableProviders([{ ...replay, availability: "available" }]), [
      { instanceId: "devReplay", driver: "acpRegistry" },
    ]);
    NodeAssert.equal(
      enabledAvailableProviders([
        replay,
        { instanceId: "unexpected", driver: "codex", enabled: true },
      ]).length,
      2,
      "An unexpectedly available real provider remains visible to the exact assertion",
    );
  },
);

NodeTest.test("a credential-bearing parent cannot populate the owned child environment", (t) => {
  const root = NodePath.join(temporary(t), "owned");
  const source = new URL("./hybrid-client-isolation.ts", import.meta.url).href;
  // This is a pure Node helper invocation, not the application or a provider. Its
  // fabricated parent environment proves that even inherited injection knobs,
  // homes, CLI paths, and secret names are not copied into the returned launch env.
  const output = NodeChildProcess.execFileSync(
    process.execPath,
    [
      "--input-type=module",
      "-e",
      `
    import { createChildIsolation } from ${JSON.stringify(source)};
    const owned = createChildIsolation(${JSON.stringify(root)}, process.execPath, "/usr/bin/git");
    process.stdout.write(JSON.stringify(owned.env));
  `,
    ],
    {
      env: {
        HOME: "/unowned-test-home",
        USERPROFILE: "/unowned-test-home",
        CODEX_HOME: "/unowned-codex",
        CLAUDE_CONFIG_DIR: "/unowned-claude",
        XDG_CONFIG_HOME: "/unowned-xdg",
        PATH: "/unowned-provider-cli-bin",
        OPENAI_API_KEY: "fixture-secret",
        ANTHROPIC_API_KEY: "fixture-secret",
        GITHUB_TOKEN: "fixture-secret",
        SSH_AUTH_SOCK: "/unowned-agent",
        HTTPS_PROXY: "https://fixture-secret@external.invalid",
        NODE_OPTIONS: "--no-warnings",
        NODE_EXTRA_CA_CERTS: "/unowned-cert",
        NPM_CONFIG_USERCONFIG: "/unowned-npmrc",
        T3_CODE_LS_DB_EXECUTABLE: "/unowned-helper",
        T3CODE_PORT: "1",
      },
      encoding: "utf8",
      timeout: 30_000,
    },
  );
  const env: NodeJS.ProcessEnv = JSON.parse(output);
  NodeAssert.equal(output.includes("fixture-secret"), false);
  NodeAssert.equal(output.includes("unowned"), false);
  NodeAssert.equal(env.HOME, NodePath.join(root, "home"));
  NodeAssert.equal(env.USERPROFILE, env.HOME);
  NodeAssert.equal(env.PATH, NodePath.join(root, "bin"));
  NodeAssert.deepEqual(NodeFS.readdirSync(env.PATH!).sort(), ["git", "node", "sh"]);
  for (const name of [
    "OPENAI_API_KEY",
    "ANTHROPIC_API_KEY",
    "GITHUB_TOKEN",
    "SSH_AUTH_SOCK",
    "HTTPS_PROXY",
    "NODE_OPTIONS",
    "NODE_EXTRA_CA_CERTS",
    "LD_PRELOAD",
    "NPM_CONFIG_USERCONFIG",
    "T3_CODE_LS_DB_EXECUTABLE",
    "T3CODE_PORT",
  ])
    NodeAssert.equal(env[name], undefined, name);
  const childHome = NodeChildProcess.execFileSync(
    process.execPath,
    ["--input-type=module", "-e", "import os from 'node:os'; process.stdout.write(os.homedir())"],
    { env, encoding: "utf8", timeout: 10_000 },
  );
  NodeAssert.equal(
    childHome,
    env.HOME,
    "HostProcess.HomeDirectory resolves to the actual owned HOME",
  );
  NodeAssert.deepEqual(NodeFS.readdirSync(env.CODEX_HOME!), []);
  NodeAssert.deepEqual(NodeFS.readdirSync(env.CLAUDE_CONFIG_DIR!), []);
  NodeAssert.equal(env.GIT_CONFIG_NOSYSTEM, "1");
  NodeAssert.equal(env.GIT_TERMINAL_PROMPT, "0");
  NodeAssert.equal(NodeFS.readFileSync(env.GIT_CONFIG_GLOBAL!, "utf8"), "");
  NodeAssert.equal(env.GIT_CONFIG_KEY_0, "core.hooksPath");
  NodeAssert.deepEqual(NodeFS.readdirSync(env.GIT_CONFIG_VALUE_0!), []);
  NodeAssert.equal(env.GIT_CONFIG_KEY_1, "core.fsmonitor");
  NodeAssert.equal(env.GIT_CONFIG_VALUE_1, "false");
  NodeAssert.equal(env.GIT_CONFIG_KEY_2, "init.templateDir");
  NodeAssert.deepEqual(NodeFS.readdirSync(env.GIT_CONFIG_VALUE_2!), []);
  NodeAssert.equal(env.GIT_CONFIG_KEY_3, "credential.helper");
  NodeAssert.equal(env.GIT_CONFIG_VALUE_3, "");
});

NodeTest.test(
  "only the source CLI and Vite entrypoints receive replay-only supported configuration",
  (t) => {
    const directory = temporary(t);
    const isolation = createChildIsolation(
      NodePath.join(directory, "owned"),
      process.execPath,
      "/usr/bin/git",
    );
    const repoRoot = NodePath.join(directory, "source");
    const stateDirectory = NodePath.join(directory, "state");
    const { settings, policy } = replayConfiguration(repoRoot, process.execPath);
    NodeAssert.equal(settings.enableProviderUpdateChecks, false);
    NodeAssert.deepEqual(Object.keys(settings.providerInstances), ["devReplay"]);
    NodeAssert.deepEqual(settings.providerInstances.devReplay.config, {
      source: "local",
      commandPath: process.execPath,
      commandArgs: [NodePath.join(repoRoot, "apps/server/scripts/dev-replay-agent.ts")],
    });
    NodeAssert.deepEqual(policy.limits.providers.allowedDrivers, ["acpRegistry"]);
    NodeAssert.equal(policy.limits.telemetry, false);
    NodeAssert.equal(policy.limits.connect, false);
    NodeAssert.equal(policy.settings.enableProviderUpdateChecks.locked, true);
    const launch = applicationLaunch({
      repoRoot,
      stateDirectory,
      serverPort: 42341,
      webPort: 42342,
      policyPath: NodePath.join(directory, "policy.json"),
      implementation: "native",
      isolation,
    });
    NodeAssert.equal(launch.serverArgs[0], NodePath.join(repoRoot, "apps/server/src/bin.ts"));
    NodeAssert.equal(launch.webArgs[0], NodePath.join(repoRoot, "node_modules/vite-plus/bin/vp"));
    NodeAssert.deepEqual(launch.serverArgs.slice(1, 3), ["--mode", "web"]);
    NodeAssert.deepEqual(launch.serverArgs.slice(-2), ["--bootstrap-fd", "3"]);
    NodeAssert.ok(launch.serverArgs.includes("--no-auto-bootstrap-project-from-cwd"));
    NodeAssert.ok(launch.serverArgs.includes("--no-browser"));
    NodeAssert.equal(launch.bootstrap.shellEnvironmentPrepared, true);
    NodeAssert.equal(launch.bootstrap.desktopBootstrapToken, "");
    NodeAssert.equal(launch.bootstrap.desktopBootstrapSecret, undefined);
    NodeAssert.equal(launch.bootstrap.tailscaleServeEnabled, false);
    NodeAssert.equal(launch.env.T3CODE_HOST, "127.0.0.1");
    NodeAssert.equal(launch.env.T3CODE_SINGLE_ORIGIN_DEV, "1");
    NodeAssert.equal(launch.env.VITE_DEV_SERVER_URL, "http://127.0.0.1:42342");
    NodeAssert.equal(launch.env.T3CODE_TELEMETRY_ENABLED, "false");
    NodeAssert.equal(launch.env.PATH, isolation.bin);
    NodeAssert.ok(browserIsolationArgs.includes("--disable-background-networking"));
    NodeAssert.ok(browserIsolationArgs.includes("--disable-component-update"));
  },
);

for (const directory of ["", "apps/web", "apps/server"]) {
  for (const name of [".env", ".env.local", ".env.development", ".env.development.local"]) {
    NodeTest.test(`refuses ${directory || "root"}/${name} without loading its contents`, (t) => {
      const repo = temporary(t);
      const target = NodePath.join(repo, directory);
      NodeFS.mkdirSync(target, { recursive: true });
      rejectDotenvFiles(repo);
      // A dangling symlink is sufficient evidence: reading a file is unnecessary.
      NodeFS.symlinkSync(NodePath.join(repo, "must-not-be-read"), NodePath.join(target, name));
      NodeAssert.throws(() => rejectDotenvFiles(repo), /refuses.*dotenv/);
    });
  }
}

function httpFixture(url: string, status = 200, headers: Record<string, string> = {}) {
  const calls: string[] = [];
  return {
    calls,
    route: {
      url: () => url,
      fetch: async (options: { maxRedirects: 0 }) => {
        NodeAssert.deepEqual(options, { maxRedirects: 0 });
        calls.push("fetch");
        return { status: () => status, headers: () => headers };
      },
      fulfill: async () => {
        calls.push("fulfill");
      },
      abort: async () => {
        calls.push("abort");
      },
    },
  };
}

for (const url of [
  "https://external.invalid/?secret=do-not-log",
  "http://127.0.0.1:42343/",
  "http://localhost:42342/",
  "http://name:secret@127.0.0.1:42342/",
  "file:///owned/local",
  "https://127.0.0.1:42342/",
  "not a URL",
]) {
  NodeTest.test(`blocks disallowed HTTP origin case ${url.split(":")[0]}`, async () => {
    const guard = loopbackGuard([42341, 42342]);
    const fixture = httpFixture(url);
    const failed = NodeAssert.rejects(guard.failure, (error: Error) => {
      NodeAssert.equal(error.message.includes("secret"), false);
      NodeAssert.equal(error.message.includes("do-not-log"), false);
      return /qualification blocked/.test(error.message);
    });
    await guard.http(fixture.route);
    await failed;
    NodeAssert.deepEqual(fixture.calls, ["abort"], "No off-origin fetch is performed");
  });
}

NodeTest.test(
  "same-origin HTTP is fetched without automatic redirects before fulfillment",
  async () => {
    const guard = loopbackGuard([42341, 42342]);
    const fixture = httpFixture("http://127.0.0.1:42342/api/config");
    await guard.http(fixture.route);
    NodeAssert.deepEqual(fixture.calls, ["fetch", "fulfill"]);
    NodeAssert.equal(guard.violated(), false);
  },
);

for (const failure of ["fetch", "fulfill", "abort"] as const) {
  NodeTest.test(
    `local ${failure} cancellation aborts without inventing a policy breach`,
    async () => {
      const guard = loopbackGuard([42342]);
      const calls: string[] = [];
      await guard.http({
        url: () => "http://127.0.0.1:42342/api/config",
        fetch: async (options) => {
          NodeAssert.deepEqual(options, { maxRedirects: 0 });
          calls.push("fetch");
          if (failure !== "fulfill") throw new Error("Owned connection refused during restart");
          return { status: () => 200, headers: () => ({}) };
        },
        fulfill: async () => {
          calls.push("fulfill");
          throw new Error("Owned context closed during teardown");
        },
        abort: async () => {
          calls.push("abort");
          if (failure === "abort") throw new Error("Owned request already closed");
        },
      });
      NodeAssert.deepEqual(
        calls,
        failure === "fulfill" ? ["fetch", "fulfill", "abort"] : ["fetch", "abort"],
      );
      NodeAssert.equal(guard.violated(), false);
      guard.assertUrl("http://127.0.0.1:42342/");
    },
  );
}

NodeTest.test("an abort cancellation cannot erase a real off-origin policy violation", async () => {
  const guard = loopbackGuard([42342]);
  const fixture = httpFixture("https://external.invalid/?secret=do-not-log");
  const failed = NodeAssert.rejects(guard.failure, /qualification blocked/);
  await guard.http({
    ...fixture.route,
    abort: async () => {
      fixture.calls.push("abort");
      throw new Error("Owned context already closed");
    },
  });
  await failed;
  NodeAssert.deepEqual(fixture.calls, ["abort"]);
  NodeAssert.equal(guard.violated(), true);
});

for (const status of [301, 302, 303, 307, 308]) {
  NodeTest.test(
    `refuses redirect ${status}, including a local first hop of an external chain`,
    async () => {
      const guard = loopbackGuard([42341, 42342]);
      // The next local URL could itself redirect externally. It must never be
      // fetched or handed to Chromium, whose redirect requests bypass routing.
      const fixture = httpFixture("http://127.0.0.1:42342/first", status, {
        location: "http://127.0.0.1:42342/second?secret=do-not-log",
      });
      const failed = NodeAssert.rejects(guard.failure, /blocked an HTTP redirect/);
      await guard.http(fixture.route);
      await failed;
      NodeAssert.deepEqual(fixture.calls, ["fetch", "abort"]);
    },
  );
}

NodeTest.test("blocks Refresh redirects before fulfillment", async () => {
  const guard = loopbackGuard([42342]);
  const fixture = httpFixture("http://127.0.0.1:42342/", 200, {
    refresh: "0;url=https://external.invalid",
  });
  const failed = NodeAssert.rejects(guard.failure, /HTTP refresh/);
  await guard.http(fixture.route);
  await failed;
  NodeAssert.deepEqual(fixture.calls, ["fetch", "abort"]);
});

for (const url of ["ws://127.0.0.1:42342/rpc", "ws://external.invalid/"]) {
  NodeTest.test(
    "socket cancellation preserves the distinction between local failure and URL denial",
    async () => {
      const guard = loopbackGuard([42342]);
      const calls: string[] = [];
      const denied = url.includes("external");
      const failed = denied
        ? NodeAssert.rejects(guard.failure, /qualification blocked/)
        : undefined;
      await guard.socket({
        url: () => url,
        connect: () => {
          calls.push("connect");
          throw new Error("Owned connection closed");
        },
        close: async () => {
          calls.push("close");
          throw new Error("Owned context closed");
        },
      });
      await failed;
      NodeAssert.deepEqual(calls, denied ? ["close"] : ["connect", "close"]);
      NodeAssert.equal(guard.violated(), denied);
    },
  );
}

for (const url of [
  "ws://external.invalid/",
  "ws://127.0.0.1:42343/",
  "wss://127.0.0.1:42342/",
  "ws://name:secret@127.0.0.1:42342/",
]) {
  NodeTest.test(
    "blocks an external, wrong-port, encrypted, or credential-bearing WebSocket",
    async () => {
      const guard = loopbackGuard([42342]);
      const calls: string[] = [];
      const failed = NodeAssert.rejects(guard.failure, /qualification blocked/);
      await guard.socket({
        url: () => url,
        connect: () => {
          calls.push("connect");
        },
        close: async () => {
          calls.push("close");
        },
      });
      await failed;
      NodeAssert.deepEqual(calls, ["close"]);
    },
  );
}

NodeTest.test(
  "connects only the allocated loopback WebSocket and latches later violations",
  async () => {
    const guard = loopbackGuard([42342]);
    const calls: string[] = [];
    await guard.socket({
      url: () => "ws://127.0.0.1:42342/rpc",
      connect: () => {
        calls.push("connect");
      },
      close: async () => {
        calls.push("close");
      },
    });
    NodeAssert.deepEqual(calls, ["connect"]);
    const failed = NodeAssert.rejects(guard.failure, /qualification blocked/);
    NodeAssert.throws(() => guard.assertUrl("http://external.invalid/"), /qualification blocked/);
    await failed;
    NodeAssert.throws(() => guard.assertUrl("http://127.0.0.1:42342/"), /qualification blocked/);
  },
);
