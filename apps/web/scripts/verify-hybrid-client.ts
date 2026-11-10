// @effect-diagnostics nodeBuiltinImport:off - isolated, plain Node application acceptance runner.
// Run from a clean CI checkout after installing Playwright Chromium and building the native helpers:
// node apps/web/scripts/verify-hybrid-client.ts /absolute/path/to/proof [upstream|native|both]
// The fixture and application state stay outside the checkout. They are retained for investigation.
import * as Assert from "node:assert/strict";
import * as ChildProcess from "node:child_process";
import * as NodeFS from "node:fs";
import * as NodeNet from "node:net";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";
import { stripVTControlCharacters } from "node:util";
import { chromium, type Browser, type Page } from "playwright";

type Implementation = "upstream" | "native";
type DevReplayWindow = Window & {
  t3dev?: { replayScenarios: () => string[]; replay: (scenario: string) => Promise<string> };
};
type OwnedApplication = { pid: number; pairingUrl: string; stop: () => Promise<void> };

const repoRoot = NodeFS.realpathSync(NodeURL.fileURLToPath(new URL("../../../", import.meta.url)));
const timeoutMs = 90_000;
const startupTimeoutMs = 180_000;
const replayMarker = "This turn is a replay.";
const mode = process.argv[3] ?? "both";
const proofArgument = process.argv[2];

if (
  !proofArgument ||
  !NodePath.isAbsolute(proofArgument) ||
  !["upstream", "native", "both"].includes(mode)
) {
  throw new Error(
    "Usage: node apps/web/scripts/verify-hybrid-client.ts /absolute/proof-dir [upstream|native|both]",
  );
}
if (process.platform !== "linux") {
  throw new Error(
    "This acceptance runner qualifies Linux only; other platforms require their own gate.",
  );
}

function outsideCheckout(path: string): void {
  const relative = NodePath.relative(repoRoot, path);
  if (
    relative === "" ||
    (!relative.startsWith(`..${NodePath.sep}`) && !NodePath.isAbsolute(relative))
  ) {
    throw new Error(`Acceptance artifacts and state must be outside the checkout: ${path}`);
  }
}

outsideCheckout(NodePath.resolve(proofArgument));
NodeFS.mkdirSync(proofArgument, { recursive: true });
const proofRoot = NodeFS.realpathSync(proofArgument);
outsideCheckout(proofRoot);
const runRoot = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3code-hybrid-client-"));
outsideCheckout(NodeFS.realpathSync(runRoot));

function redact(text: string): string {
  return stripVTControlCharacters(text).replace(/([#?&]token=)[^\s&"'<>]+/giu, "$1[REDACTED]");
}

function writeJson(path: string, value: unknown): void {
  NodeFS.writeFileSync(
    path,
    `${JSON.stringify(value, (_key, item: unknown) => (typeof item === "string" ? redact(item) : item), 2)}\n`,
  );
}

function git(args: string[], cwd = repoRoot): string {
  return ChildProcess.execFileSync("git", args, {
    cwd,
    encoding: "utf8",
    stdio: ["ignore", "pipe", "pipe"],
  }).trim();
}

function readJsonIfPresent(path: string): unknown {
  return NodeFS.existsSync(path) ? JSON.parse(NodeFS.readFileSync(path, "utf8")) : null;
}

const source = {
  commit: git(["rev-parse", "HEAD"]),
  dirty: git(["status", "--porcelain"]).length > 0,
  upstream: readJsonIfPresent(NodePath.join(repoRoot, "upstream.lock.json")),
  toolchain: readJsonIfPresent(NodePath.join(repoRoot, "toolchain.lock.json")),
  nativeManifest: readJsonIfPresent(NodePath.join(repoRoot, "bin", "manifest.json")),
};

async function availablePorts(): Promise<number[]> {
  const servers = [NodeNet.createServer(), NodeNet.createServer()];
  try {
    // Reserve both at once so the kernel cannot return the same port twice.
    return await Promise.all(
      servers.map(
        (server) =>
          new Promise<number>((resolve, reject) => {
            server.once("error", reject);
            server.listen(0, "127.0.0.1", () => {
              const address = server.address();
              if (!address || typeof address === "string")
                return reject(new Error("No TCP port allocated"));
              resolve(address.port);
            });
          }),
      ),
    );
  } finally {
    await Promise.all(
      servers.map((server) => new Promise<void>((resolve) => server.close(() => resolve()))),
    );
  }
}

let activeApplication: OwnedApplication | undefined;
let browser: Browser | undefined;

async function startApplication(
  implementation: Implementation,
  stateDirectory: string,
  serverPort: number,
  webPort: number,
  logPath: string,
): Promise<OwnedApplication> {
  const env = { ...process.env };
  // Component overrides would invalidate an explicit upstream/native comparison.
  for (const name of Object.keys(env)) {
    if (/^T3_CODE_LS_.*_EXECUTABLE$/.test(name)) delete env[name];
  }
  Object.assign(env, {
    PATH: `${NodePath.join(repoRoot, "node_modules", ".bin")}${NodePath.delimiter}${env.PATH ?? ""}`,
    T3_CODE_LS_IMPLEMENTATION: implementation,
    T3CODE_HOME: stateDirectory,
    T3CODE_DEV_SEED: "0",
    T3CODE_TELEMETRY_ENABLED: "false",
    T3CODE_NO_BROWSER: "1",
    T3CODE_PORT_OFFSET: String(webPort - 5733),
    FORCE_COLOR: "0",
    NO_COLOR: "1",
  });
  NodeFS.writeFileSync(logPath, "");
  const child = ChildProcess.spawn(
    process.execPath,
    [
      "scripts/dev-runner.ts",
      "dev",
      "--home-dir",
      stateDirectory,
      "--no-seed",
      "--no-auto-bootstrap-project-from-cwd",
      "--port",
      String(serverPort),
    ],
    { cwd: repoRoot, env, detached: true, stdio: ["ignore", "pipe", "pipe"] },
  );
  const pid = child.pid;
  if (pid === undefined) {
    await new Promise<never>((_, reject) => child.once("error", reject));
    throw new Error("The development runner did not receive a process ID");
  }

  let stopping = false;
  let pairingUrl: string | undefined;
  let viteReady = false;
  let readyResolve: () => void;
  let readyReject: (error: Error) => void;
  const ready = new Promise<void>((resolve, reject) => {
    readyResolve = resolve;
    readyReject = reject;
  });
  const timer = setTimeout(
    () => readyReject(new Error(`Application startup timed out; see ${logPath}`)),
    startupTimeoutMs,
  );
  const exited = new Promise<void>((resolve) => child.once("close", () => resolve()));
  child.once("error", (error) => readyReject(error));
  child.once("exit", (code, signal) => {
    if (!stopping)
      readyReject(new Error(`Development runner exited (${code ?? signal}); see ${logPath}`));
  });

  function consumeLine(raw: string): void {
    const line = stripVTControlCharacters(raw);
    NodeFS.appendFileSync(logPath, `${redact(line)}\n`);
    const candidate = line.match(/https?:\/\/[^\s"'<>]+\/pair#token=[^\s"'<>]+/u)?.[0];
    if (candidate) {
      const url = new URL(candidate);
      if (url.origin !== `http://localhost:${webPort}`) {
        readyReject(new Error(`Pairing URL has an unexpected origin: ${url.origin}`));
      } else {
        pairingUrl = candidate;
      }
    }
    if (/\bready in\b/iu.test(line)) viteReady = true;
    if (pairingUrl && viteReady) readyResolve();
  }

  for (const stream of [child.stdout, child.stderr]) {
    let pending = "";
    stream.setEncoding("utf8");
    stream.on("data", (chunk: string) => {
      pending += chunk;
      const lines = pending.split(/\r?\n/u);
      pending = lines.pop() ?? "";
      for (const line of lines) consumeLine(line);
    });
    stream.once("end", () => {
      if (pending) consumeLine(pending);
    });
  }

  const stop = async () => {
    if (stopping) return;
    stopping = true;
    const signal = (value: NodeJS.Signals) => {
      try {
        process.kill(-pid, value);
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code !== "ESRCH") throw error;
      }
    };
    signal("SIGTERM");
    const killTimer = setTimeout(() => signal("SIGKILL"), 10_000);
    let deadlineTimer: ReturnType<typeof setTimeout>;
    const deadline = new Promise<never>((_, reject) => {
      deadlineTimer = setTimeout(
        () => reject(new Error(`Owned process group ${pid} did not close within 15 seconds`)),
        15_000,
      );
    });
    try {
      await Promise.race([exited, deadline]);
    } finally {
      clearTimeout(killTimer);
      clearTimeout(deadlineTimer!);
    }
  };
  const application: OwnedApplication = { pid, pairingUrl: "", stop };
  activeApplication = application;
  try {
    await ready;
    Assert.ok(pairingUrl);
    application.pairingUrl = pairingUrl;
    return application;
  } catch (error) {
    await stop();
    throw error;
  } finally {
    clearTimeout(timer);
  }
}

async function capture(page: Page, directory: string, name: string): Promise<void> {
  await page.screenshot({
    path: NodePath.join(directory, `${name}.png`),
    fullPage: true,
    timeout: 15_000,
  });
  NodeFS.writeFileSync(
    NodePath.join(directory, `${name}.aria.txt`),
    redact(await page.locator("body").ariaSnapshot()),
  );
}

async function assertCompleted(page: Page, threadId: string): Promise<void> {
  await page.waitForURL((url) => url.pathname.endsWith(`/${threadId}`), { timeout: timeoutMs });
  await page
    .getByText(replayMarker, { exact: false })
    .last()
    .waitFor({ state: "visible", timeout: timeoutMs });
  await page
    .getByRole("button", { name: "Stop generation", exact: true })
    .waitFor({ state: "hidden", timeout: timeoutMs });
}

async function exercise(implementation: Implementation): Promise<void> {
  const directory = NodePath.join(proofRoot, implementation);
  NodeFS.mkdirSync(directory, { recursive: true });
  const workspace = NodePath.join(runRoot, implementation, "hybrid-fixture");
  const stateDirectory = NodePath.join(runRoot, implementation, "state");
  NodeFS.mkdirSync(NodePath.join(workspace, "src"), { recursive: true });
  NodeFS.writeFileSync(
    NodePath.join(workspace, "package.json"),
    '{"name":"hybrid-fixture","private":true,"scripts":{"test":"node --test"}}\n',
  );
  NodeFS.writeFileSync(
    NodePath.join(workspace, "src", "math.ts"),
    "export const sum = (a: number, b: number) => a + b;\n",
  );
  git(["init", "--initial-branch=main"], workspace);
  git(["add", "package.json", "src/math.ts"], workspace);
  git(
    [
      "-c",
      "user.name=Hybrid acceptance",
      "-c",
      "user.email=hybrid-acceptance@localhost",
      "-c",
      "commit.gpgSign=false",
      "commit",
      "-m",
      "Add controlled replay fixture",
    ],
    workspace,
  );
  const [serverPort, webPort] = await availablePorts();
  Assert.ok(serverPort && webPort);
  Assert.notEqual(serverPort, webPort, "Server and client need distinct ports");
  Assert.ok(webPort > 5733, "The allocated web port must support the dev-runner offset");
  const proof = {
    schemaVersion: 1,
    status: "running",
    implementation,
    scope:
      "Linux original web client; local project; free ACP showcase approval; reload; same-state server restart",
    startedAt: new Date().toISOString(),
    finishedAt: null as string | null,
    source,
    platform: process.platform,
    architecture: process.arch,
    node: process.version,
    browser: browser!.version(),
    stateDirectory,
    fixtureDirectory: workspace,
    serverPort,
    webPort,
    processGroups: [] as number[],
    completedSteps: [] as string[],
    threadId: null as string | null,
    threadUrl: null as string | null,
    browserErrors: [] as string[],
    error: null as string | null,
  };
  const saveProof = () => writeJson(NodePath.join(directory, "proof.json"), proof);
  const completed = (step: string) => {
    proof.completedSteps.push(step);
    saveProof();
    console.log(`[${implementation}] ${step}`);
  };
  saveProof();
  const contextOptions = {
    viewport: { width: 1440, height: 1000 },
    reducedMotion: "reduce" as const,
  };
  let context = await browser!.newContext(contextOptions);
  let page = await context.newPage();
  function observePage(current: Page): void {
    current.setDefaultTimeout(timeoutMs);
    current.setDefaultNavigationTimeout(timeoutMs);
    current.on("pageerror", (error) =>
      proof.browserErrors.push(redact(error.stack ?? error.message)),
    );
    current.on("console", (message) => {
      if (message.type() === "error") proof.browserErrors.push(redact(message.text()));
    });
  }
  observePage(page);

  try {
    let application = await startApplication(
      implementation,
      stateDirectory,
      serverPort,
      webPort,
      NodePath.join(directory, "startup.log"),
    );
    proof.processGroups.push(application.pid);
    await page.goto(application.pairingUrl, { waitUntil: "domcontentloaded" });
    await page.getByRole("button", { name: "Add project", exact: true }).first().click();
    const dialog = page.getByRole("dialog");
    await dialog.getByText("Local folder", { exact: true }).click();
    await dialog.getByRole("combobox").fill(workspace);
    await dialog.getByRole("button", { name: /^Add \(/u }).click();
    await dialog.waitFor({ state: "hidden" });
    await page.getByText("hybrid-fixture", { exact: true }).first().waitFor({ state: "visible" });
    completed("Created local project through the normal client");

    await page.waitForFunction(
      () => (window as DevReplayWindow).t3dev?.replayScenarios().includes("showcase"),
      undefined,
      { timeout: timeoutMs },
    );
    const threadId = await page.evaluate(async () => {
      const replay = (window as DevReplayWindow).t3dev;
      if (!replay) throw new Error("The original development replay host is unavailable");
      return await replay.replay("showcase");
    });
    proof.threadId = threadId;
    const approval = page.getByRole("button", { name: /^(Yes|Approve)$/u }).first();
    await approval.waitFor({ state: "visible" });
    await capture(page, directory, "approval");
    await approval.click();
    completed("Approved the controlled replay provider request in the normal client");
    await assertCompleted(page, threadId);
    proof.threadUrl = page.url();
    completed("Received the completed replay turn on the original thread route");

    await page.reload({ waitUntil: "domcontentloaded" });
    await assertCompleted(page, threadId);
    Assert.equal(page.url(), proof.threadUrl);
    completed("Reconnected after a client reload with the same completed thread");

    await application.stop();
    application = await startApplication(
      implementation,
      stateDirectory,
      serverPort,
      webPort,
      NodePath.join(directory, "restart.log"),
    );
    proof.processGroups.push(application.pid);
    // Keep only the authenticated session cookies. A fresh client cannot satisfy recovery
    // from localStorage or IndexedDB, and must retrieve the old thread from the restarted server.
    // Do not consume the restart's new pairing token or persist the session cookies in proof.
    const cookies = await context.cookies();
    await context.close();
    context = await browser!.newContext({
      ...contextOptions,
      storageState: { cookies, origins: [] },
    });
    page = await context.newPage();
    observePage(page);
    await page.goto(proof.threadUrl, { waitUntil: "domcontentloaded" });
    await assertCompleted(page, threadId);
    Assert.equal(page.url(), proof.threadUrl);
    completed(
      "Recovered the same durable thread after server restart in a fresh client with the existing session",
    );
    await capture(page, directory, "completed");
    proof.status = "passed";
  } catch (error) {
    proof.status = "failed";
    proof.error = redact(error instanceof Error ? (error.stack ?? error.message) : String(error));
    try {
      await capture(page, directory, "failure");
    } catch (captureError) {
      proof.browserErrors.push(redact(`Could not capture failure: ${captureError}`));
    }
    throw error;
  } finally {
    proof.finishedAt = new Date().toISOString();
    saveProof();
    await activeApplication?.stop();
    activeApplication = undefined;
    await context.close();
  }
}

for (const signal of ["SIGINT", "SIGTERM"] as const) {
  process.once(signal, () => {
    void (async () => {
      await activeApplication?.stop();
      await browser?.close();
      process.exit(signal === "SIGINT" ? 130 : 143);
    })();
  });
}

const requested: Implementation[] =
  mode === "both" ? ["upstream", "native"] : [mode as Implementation];
const run = {
  status: "running",
  requested,
  passed: [] as Implementation[],
  source,
  runRoot,
  error: null as string | null,
};
try {
  const executablePath = process.env.T3_CODE_LS_CHROMIUM_EXECUTABLE;
  browser = await chromium.launch(executablePath ? { executablePath } : {});
  console.log(`Original client acceptance: ${requested.join(", ")}; Chromium ${browser.version()}`);
  for (const implementation of requested) {
    await exercise(implementation);
    run.passed.push(implementation);
  }
  run.status = "passed";
  console.log(`Client acceptance passed. Evidence: ${proofRoot}`);
} catch (error) {
  run.status = "failed";
  run.error = redact(error instanceof Error ? (error.stack ?? error.message) : String(error));
  console.error(run.error);
  process.exitCode = 1;
} finally {
  writeJson(NodePath.join(proofRoot, "summary.json"), run);
  await activeApplication?.stop();
  await browser?.close();
}
