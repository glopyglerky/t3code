// @effect-diagnostics nodeBuiltinImport:off - isolated Node acceptance harness, never application runtime.
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import { DesktopBackendBootstrap, ManagedPolicyDocument, ServerSettings } from "@t3tools/contracts";
import * as Schema from "effect/Schema";

const decodeBootstrap = Schema.decodeUnknownSync(DesktopBackendBootstrap);
const decodePolicy = Schema.decodeUnknownSync(ManagedPolicyDocument, { onExcessProperty: "error" });
const decodeSettings = Schema.decodeUnknownSync(ServerSettings);

export function rejectDotenvFiles(repoRoot: string): void {
  // Vite's config also calls loadRepoEnv(). Do not inspect, load, copy, or override an
  // operator's dotenv files: this qualification requires a clean source checkout.
  for (const directory of [
    repoRoot,
    NodePath.join(repoRoot, "apps/web"),
    NodePath.join(repoRoot, "apps/server"),
  ]) {
    for (const name of [".env", ".env.local", ".env.development", ".env.development.local"]) {
      try {
        NodeFS.lstatSync(NodePath.join(directory, name));
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code === "ENOENT") continue;
        throw error;
      }
      throw new Error("Client qualification refuses a checkout containing dotenv configuration");
    }
  }
}

export function createChildIsolation(root: string, nodeExecutable: string, gitExecutable: string) {
  NodeFS.mkdirSync(root, { mode: 0o700 });
  const home = NodePath.join(root, "home");
  const bin = NodePath.join(root, "bin");
  const empty = NodePath.join(root, "empty");
  const temporary = NodePath.join(root, "tmp");
  const directories = {
    HOME: home,
    USERPROFILE: home,
    CODEX_HOME: NodePath.join(home, ".codex"),
    CLAUDE_CONFIG_DIR: NodePath.join(home, ".claude"),
    XDG_CONFIG_HOME: NodePath.join(home, ".config"),
    XDG_DATA_HOME: NodePath.join(home, ".local/share"),
    XDG_CACHE_HOME: NodePath.join(home, ".cache"),
    XDG_STATE_HOME: NodePath.join(home, ".local/state"),
    XDG_RUNTIME_DIR: NodePath.join(root, "runtime"),
    APPDATA: NodePath.join(home, "AppData/Roaming"),
    LOCALAPPDATA: NodePath.join(home, "AppData/Local"),
    TMPDIR: temporary,
    TMP: temporary,
    TEMP: temporary,
  };
  for (const directory of [bin, empty, ...new Set(Object.values(directories))]) {
    NodeFS.mkdirSync(directory, { recursive: true, mode: 0o700 });
  }
  for (const [name, executable] of Object.entries({
    node: nodeExecutable,
    git: gitExecutable,
    sh: "/bin/sh",
  })) {
    if (!NodePath.isAbsolute(executable) || !NodeFS.statSync(executable).isFile()) {
      throw new Error(`Client qualification requires an absolute ${name} executable`);
    }
    NodeFS.symlinkSync(NodeFS.realpathSync(executable), NodePath.join(bin, name));
  }
  const gitConfig = NodePath.join(root, "gitconfig");
  NodeFS.writeFileSync(gitConfig, "", { flag: "wx", mode: 0o600 });
  // Construct from an allowlist, never from process.env. This also applies to Git
  // and Chromium; a telemetry opt-out alone does not prevent identity discovery.
  const env: NodeJS.ProcessEnv = {
    ...directories,
    PATH: bin,
    SHELL: NodePath.join(bin, "sh"),
    LANG: "C.UTF-8",
    LC_ALL: "C.UTF-8",
    TZ: "UTC",
    CI: "1",
    NO_COLOR: "1",
    GIT_CONFIG_NOSYSTEM: "1",
    GIT_CONFIG_GLOBAL: gitConfig,
    GIT_TERMINAL_PROMPT: "0",
    GIT_CONFIG_COUNT: "4",
    GIT_CONFIG_KEY_0: "core.hooksPath",
    GIT_CONFIG_VALUE_0: empty,
    GIT_CONFIG_KEY_1: "core.fsmonitor",
    GIT_CONFIG_VALUE_1: "false",
    GIT_CONFIG_KEY_2: "init.templateDir",
    GIT_CONFIG_VALUE_2: empty,
    GIT_CONFIG_KEY_3: "credential.helper",
    GIT_CONFIG_VALUE_3: "",
  };
  return { root, home, bin, env };
}

export function replayConfiguration(repoRoot: string, nodeExecutable: string) {
  const settings = {
    enableProviderUpdateChecks: false,
    providerInstances: {
      devReplay: {
        driver: "acpRegistry",
        displayName: "Replay (dev)",
        enabled: true,
        config: {
          source: "local",
          commandPath: nodeExecutable,
          commandArgs: [NodePath.join(repoRoot, "apps/server/scripts/dev-replay-agent.ts")],
        },
      },
    },
  };
  const policy = {
    schemaVersion: 1,
    organization: "Hybrid acceptance",
    settings: { enableProviderUpdateChecks: { value: false, locked: true } },
    limits: {
      clients: ["local-browser"],
      connect: false,
      telemetry: false,
      webhookTasks: false,
      browserCookieImport: false,
      providers: { allowedDrivers: ["acpRegistry"] },
      updates: { mode: "off" },
    },
  };
  decodeSettings(settings);
  decodePolicy(policy);
  return { settings, policy };
}

export function enabledAvailableProviders(
  providers: readonly {
    instanceId: string;
    driver: string;
    enabled: boolean;
    availability?: "available" | "unavailable" | undefined;
  }[],
) {
  // Match ServerProvider/isProviderAvailable and the real client: older available
  // snapshots omit availability; only an explicit "unavailable" blocks them.
  return providers
    .filter((provider) => provider.enabled && provider.availability !== "unavailable")
    .map(({ instanceId, driver }) => ({ instanceId, driver }));
}

export function applicationLaunch(options: {
  repoRoot: string;
  stateDirectory: string;
  serverPort: number;
  webPort: number;
  policyPath: string;
  implementation: "upstream" | "native";
  isolation: ReturnType<typeof createChildIsolation>;
}) {
  const { repoRoot, stateDirectory, serverPort, webPort, policyPath, implementation, isolation } =
    options;
  const webOrigin = `http://127.0.0.1:${webPort}`;
  const env: NodeJS.ProcessEnv = {
    ...isolation.env,
    T3_CODE_LS_IMPLEMENTATION: implementation,
    T3CODE_HOME: stateDirectory,
    T3CODE_MODE: "web",
    T3CODE_PORT: String(serverPort),
    T3CODE_HOST: "127.0.0.1",
    T3CODE_NO_BROWSER: "1",
    T3CODE_AUTO_BOOTSTRAP_PROJECT_FROM_CWD: "0",
    T3CODE_DEV_SEED: "0",
    T3CODE_TELEMETRY_ENABLED: "false",
    T3CODE_DEV_MANAGED_POLICY_PATH: policyPath,
    T3CODE_SINGLE_ORIGIN_DEV: "1",
    VITE_DEV_SERVER_URL: webOrigin,
    PORT: String(webPort),
  };
  // These are the same source entrypoints used by the dev-runner's server and web
  // scripts, without its task runner, watch processes, or ambient bin expansion.
  const serverArgs = [
    NodePath.join(repoRoot, "apps/server/src/bin.ts"),
    "--mode",
    "web",
    "--base-dir",
    stateDirectory,
    "--dev-url",
    webOrigin,
    "--port",
    String(serverPort),
    "--host",
    "127.0.0.1",
    "--no-browser",
    "--no-auto-bootstrap-project-from-cwd",
    "--bootstrap-fd",
    "3",
  ];
  const webArgs = [
    NodePath.join(repoRoot, "node_modules/vite-plus/bin/vp"),
    "dev",
    "--host",
    "127.0.0.1",
    "--port",
    String(webPort),
    "--strictPort",
  ];
  // The existing bootstrap descriptor is the supported way to mark a prepared
  // environment. Explicit --mode web wins; an empty token grants no desktop auth.
  // Keep normal browser pairing and initial setup mandatory.
  const bootstrap = decodeBootstrap({
    mode: "desktop",
    noBrowser: true,
    port: serverPort,
    t3Home: stateDirectory,
    host: "127.0.0.1",
    desktopBootstrapToken: "",
    shellEnvironmentPrepared: true,
    tailscaleServeEnabled: false,
    tailscaleServePort: 443,
  });
  return { env, serverArgs, webArgs, bootstrap, webOrigin };
}

type HttpResponse = { status(): number; headers(): Record<string, string> };
type HttpRoute<Response extends HttpResponse> = {
  url(): string;
  fetch(options: { maxRedirects: 0 }): Promise<Response>;
  fulfill(response: Response): Promise<void>;
  abort(): Promise<void>;
};
type SocketRoute = {
  url(): string;
  connect(): void;
  close(): Promise<void>;
};

export function loopbackGuard(ports: readonly number[]) {
  const origins = new Set(ports.map((port) => `http://127.0.0.1:${port}`));
  let rejectFailure: (error: Error) => void = () => {};
  let violation: Error | undefined;
  const failure = new Promise<never>((_, reject) => {
    rejectFailure = reject;
  });
  // The owner races this promise with the entire UI flow. Prevent a transient
  // unhandled rejection while a fresh context is being installed after restart.
  void failure.catch(() => {});
  function deny(reason: string): never {
    violation ??= new Error(`Client qualification blocked ${reason}`);
    rejectFailure(violation);
    throw violation;
  }
  function assertUrl(value: string, socket = false): void {
    if (violation) throw violation;
    let url: URL;
    try {
      url = new URL(value);
    } catch {
      deny("an invalid URL");
    }
    if (url.username || url.password) deny("URL credentials");
    const protocol = socket ? "ws:" : "http:";
    if (url.protocol !== protocol || !origins.has(`http://${url.host}`)) {
      // Never put userinfo, path, query, headers, or request bodies in diagnostics.
      deny("a request outside the owned loopback origins");
    }
  }
  async function http<Response extends HttpResponse>(route: HttpRoute<Response>): Promise<void> {
    try {
      assertUrl(route.url());
      const response = await route.fetch({ maxRedirects: 0 });
      // Playwright automatically continues subsequent browser redirects without
      // rerunning route handlers. Never hand a redirect to Chromium: even an
      // initially local redirect can have an external second hop.
      if (response.status() >= 300 && response.status() < 400 && response.status() !== 304) {
        deny("an HTTP redirect");
      }
      if (Object.keys(response.headers()).some((name) => name.toLowerCase() === "refresh")) {
        deny("an HTTP refresh redirect");
      }
      await route.fulfill(response);
    } catch {
      // Owned restart/teardown can cancel fetch, fulfill, and abort. Never continue
      // the failed request or label local transport cancellation a policy breach.
      // URL/redirect denials are already latched before cleanup and remain fatal.
      await route.abort().catch(() => {});
    }
  }
  async function socket(route: SocketRoute): Promise<void> {
    try {
      assertUrl(route.url(), true);
      route.connect();
    } catch {
      // As with HTTP, a local connection/teardown cancellation is not a policy
      // violation. A URL denial remains latched even if close itself is cancelled.
      await route.close().catch(() => {});
    }
  }
  return { failure, assertUrl, http, socket, violated: () => violation !== undefined };
}

export const browserIsolationArgs = [
  "--disable-background-networking",
  "--disable-component-update",
  "--disable-default-apps",
  "--disable-domain-reliability",
  "--disable-sync",
  "--no-first-run",
];
