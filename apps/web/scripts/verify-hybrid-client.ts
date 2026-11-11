// @effect-diagnostics nodeBuiltinImport:off - isolated, plain Node application acceptance runner.
// Run from a clean CI checkout after installing Playwright Chromium and building the native helpers:
// node apps/web/scripts/verify-hybrid-client.ts /absolute/path/to/proof [upstream|native|both]
// The fixture and application state stay outside the checkout. They are retained for investigation.
import * as NodeAssert from "node:assert/strict";
import * as NodeChildProcess from "node:child_process";
import * as NodeFS from "node:fs";
import * as NodeNet from "node:net";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";
import * as NodeUtil from "node:util";
import {
  ORCHESTRATION_PROTOCOL_HEADER,
  ORCHESTRATION_PROTOCOL_VERSION_TEXT,
  OrchestrationV2ThreadDetailSnapshot,
  type ServerProvider,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Schema from "effect/Schema";
import { chromium, type Browser, type BrowserContext, type Page } from "playwright";
import { nativeExecutable } from "@t3tools/shared/nativeExecutable";
import { inspectNativeReplay } from "./hybrid-client-database.ts";
import {
  applicationLaunch,
  browserIsolationArgs,
  createChildIsolation,
  enabledAvailableProviders,
  loopbackGuard,
  rejectDotenvFiles,
  replayConfiguration,
} from "./hybrid-client-isolation.ts";
import {
  assertReplayTurnLinks,
  replayTurnIdentity,
  type ReplayTurnIdentity,
} from "./hybrid-client-turn.ts";

type Implementation = "upstream" | "native";
type DevReplayWindow = Window & {
  t3dev?: { replayScenarios: () => string[]; replay: (scenario: string) => Promise<string> };
};
type OwnedApplication = { pids: number[]; pairingUrl: string; stop: () => Promise<void> };

const repoRoot = NodeFS.realpathSync(NodeURL.fileURLToPath(new URL("../../../", import.meta.url)));
const timeoutMs = 90_000;
const startupTimeoutMs = 180_000;
const replayMarker = "This turn is a replay.";
const decodeSnapshot = Schema.decodeUnknownSync(
  Schema.toCodecJson(OrchestrationV2ThreadDetailSnapshot),
);
const mode = process.argv[3] ?? "both";
const proofArgument = process.argv[2];
// oxlint-disable-next-line t3code/no-global-process-runtime -- standalone acceptance records the actual runner, not injected application state.
const runnerPlatform = process.platform;
// oxlint-disable-next-line t3code/no-global-process-runtime -- standalone acceptance must identify the binary's actual host architecture.
const runnerArchitecture = process.arch;

if (
  !proofArgument ||
  !NodePath.isAbsolute(proofArgument) ||
  !["upstream", "native", "both"].includes(mode)
) {
  throw new Error(
    "Usage: node apps/web/scripts/verify-hybrid-client.ts /absolute/proof-dir [upstream|native|both]",
  );
}
if (runnerPlatform !== "linux") {
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
rejectDotenvFiles(repoRoot);
const nodeExecutable = NodeFS.realpathSync(process.execPath);
const gitExecutable = NodeFS.realpathSync("/usr/bin/git");
const controllerIsolation = createChildIsolation(
  NodePath.join(runRoot, "controller"),
  nodeExecutable,
  gitExecutable,
);

function redact(text: string): string {
  return NodeUtil.stripVTControlCharacters(text).replace(
    /([#?&]token=)[^\s&"'<>]+/giu,
    "$1[REDACTED]",
  );
}

function writeJson(path: string, value: unknown): void {
  NodeFS.writeFileSync(
    path,
    `${JSON.stringify(value, (_key, item: unknown) => (typeof item === "string" ? redact(item) : item), 2)}\n`,
  );
}

function git(args: string[], cwd = repoRoot): string {
  return NodeChildProcess.execFileSync(gitExecutable, args, {
    cwd,
    env: controllerIsolation.env,
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
const pageGuards = new WeakMap<Page, ReturnType<typeof loopbackGuard>>();

async function guardContext(
  context: BrowserContext,
  guard: ReturnType<typeof loopbackGuard>,
): Promise<void> {
  await context.route("**/*", (route) =>
    guard.http({
      url: () => route.request().url(),
      fetch: (options) => route.fetch(options),
      fulfill: (response) => route.fulfill({ response }),
      abort: () => route.abort("blockedbyclient"),
    }),
  );
  await context.routeWebSocket("**/*", (route) =>
    guard.socket({
      url: () => route.url(),
      connect: () => {
        route.connectToServer();
      },
      close: () =>
        route.close({ code: 1008, reason: "Qualification permits owned loopback origins only" }),
    }),
  );
}

async function startApplication(
  implementation: Implementation,
  stateDirectory: string,
  serverPort: number,
  webPort: number,
  logPath: string,
  isolation: ReturnType<typeof createChildIsolation>,
  policyPath: string,
): Promise<OwnedApplication> {
  rejectDotenvFiles(repoRoot);
  const launch = applicationLaunch({
    repoRoot,
    stateDirectory,
    serverPort,
    webPort,
    policyPath,
    implementation,
    isolation,
  });
  NodeFS.writeFileSync(logPath, "");
  const children: Array<{ pid: number; exited: Promise<void> }> = [];
  let stopping = false;
  let pairingUrl: string | undefined;
  let viteReady = false;
  let readyResolve: () => void;
  let readyReject: (error: Error) => void;
  const ready = new Promise<void>((resolve, reject) => {
    readyResolve = resolve;
    readyReject = reject;
  });
  void ready.catch(() => {});
  const timer = setTimeout(
    () => readyReject(new Error(`Application startup timed out; see ${logPath}`)),
    startupTimeoutMs,
  );

  function consumeLine(raw: string): void {
    const line = NodeUtil.stripVTControlCharacters(raw);
    NodeFS.appendFileSync(logPath, `${redact(line)}\n`);
    const candidate = line.match(/https?:\/\/[^\s"'<>]+\/pair#token=[^\s"'<>]+/u)?.[0];
    if (candidate) {
      const url = new URL(candidate);
      if (url.origin !== launch.webOrigin) {
        readyReject(new Error(`Pairing URL has an unexpected origin: ${url.origin}`));
      } else {
        pairingUrl = candidate;
      }
    }
    if (/\bready in\b/iu.test(line)) viteReady = true;
    if (pairingUrl && viteReady) readyResolve();
  }

  function ownChild(child: NodeChildProcess.ChildProcess, label: string): void {
    const exited = new Promise<void>((resolve) => child.once("close", () => resolve()));
    child.once("error", (error) => readyReject(error));
    child.once("exit", (code, signal) => {
      if (!stopping) readyReject(new Error(`${label} exited (${code ?? signal}); see ${logPath}`));
    });
    if (child.pid === undefined) {
      readyReject(new Error(`${label} did not receive a process ID`));
    } else {
      children.push({ pid: child.pid, exited });
    }
    for (const stream of [child.stdout, child.stderr]) {
      if (!stream) continue;
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
  }

  const stop = async () => {
    if (stopping) return;
    stopping = true;
    readyReject(new Error("Owned application stopped before startup completed"));
    const signal = (value: NodeJS.Signals) => {
      for (const { pid } of children) {
        try {
          process.kill(-pid, value);
        } catch (error) {
          if ((error as NodeJS.ErrnoException).code !== "ESRCH") throw error;
        }
      }
    };
    signal("SIGTERM");
    const killTimer = setTimeout(() => signal("SIGKILL"), 10_000);
    let deadlineTimer: ReturnType<typeof setTimeout>;
    const deadline = new Promise<never>((_, reject) => {
      deadlineTimer = setTimeout(
        () => reject(new Error("Owned application process groups did not close within 15 seconds")),
        15_000,
      );
    });
    try {
      await Promise.race([Promise.all(children.map((child) => child.exited)), deadline]);
    } finally {
      clearTimeout(killTimer);
      clearTimeout(deadlineTimer!);
    }
  };
  const application: OwnedApplication = { pids: [], pairingUrl: "", stop };
  activeApplication = application;
  try {
    const bootstrapPath = NodePath.join(isolation.root, "bootstrap.json");
    NodeFS.writeFileSync(bootstrapPath, `${JSON.stringify(launch.bootstrap)}\n`, { mode: 0o600 });
    const bootstrapFd = NodeFS.openSync(bootstrapPath, "r");
    try {
      ownChild(
        NodeChildProcess.spawn(nodeExecutable, launch.serverArgs, {
          cwd: NodePath.join(repoRoot, "apps/server"),
          env: launch.env,
          detached: true,
          stdio: ["ignore", "pipe", "pipe", bootstrapFd],
        }),
        "Source server",
      );
    } finally {
      NodeFS.closeSync(bootstrapFd);
    }
    ownChild(
      NodeChildProcess.spawn(nodeExecutable, launch.webArgs, {
        cwd: NodePath.join(repoRoot, "apps/web"),
        env: launch.env,
        detached: true,
        stdio: ["ignore", "pipe", "pipe"],
      }),
      "Source web client",
    );
    application.pids = children.map(({ pid }) => pid);
    await ready;
    NodeAssert.ok(pairingUrl);
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
  NodeFS.writeFileSync(
    NodePath.join(directory, `${name}.aria.txt`),
    redact(await page.locator("body").ariaSnapshot({ timeout: 15_000 })),
  );
  await page.screenshot({
    path: NodePath.join(directory, `${name}.png`),
    fullPage: true,
    timeout: 15_000,
  });
}

async function completeInitialSetup(page: Page): Promise<void> {
  // The integrated nightly opens a welcome page after pairing. Complete its normal
  // persisted setup before creating the fixture project through the sidebar.
  const heading = page.getByRole("heading", { name: "Welcome to T3 Code", exact: true });
  await heading.waitFor({ state: "visible" });
  const welcome = page.getByRole("main").filter({ has: heading });
  // Source development mode supplies the free replay agent. Requiring its row and
  // the enabled Start button also checks provider discovery without a real sign-in.
  await welcome.getByText("Replay (dev)", { exact: true }).waitFor({ state: "visible" });
  const providers = await page.evaluate(async () => {
    // Read the real client state without introducing a production-only test hook.
    const stateModule = "/src/state/server.ts";
    const registryModule = "/src/rpc/atomRegistry.ts";
    const [{ primaryServerProvidersAtom }, { appAtomRegistry }] = await Promise.all([
      import(stateModule),
      import(registryModule),
    ]);
    const snapshots = appAtomRegistry.get(primaryServerProvidersAtom) as readonly ServerProvider[];
    return snapshots.map(({ instanceId, driver, availability, enabled }) => ({
      instanceId,
      driver,
      availability,
      enabled,
    }));
  });
  NodeAssert.deepEqual(
    enabledAvailableProviders(providers),
    [{ instanceId: "devReplay", driver: "acpRegistry" }],
    "Only the local replay provider may be available during qualification",
  );
  await welcome
    .getByText(/^Looking for projects in your .* history…$/u)
    .waitFor({ state: "hidden" });
  const projectImport = welcome.getByRole("checkbox", {
    name: /^Import (?:\d+ )?projects? and their threads\b/u,
  });
  if (await projectImport.isVisible()) await projectImport.uncheck();
  await welcome.getByRole("button", { name: "Start", exact: true }).click();
  await heading.waitFor({ state: "hidden" });
  await page.waitForURL((url) => url.pathname === "/");
}

async function readThreadSnapshot(
  page: Page,
  threadId: string,
): Promise<OrchestrationV2ThreadDetailSnapshot> {
  // Read the existing authenticated application contract with this browser session's cookies.
  const snapshotUrl = new URL(
    `/api/orchestration/threads/${encodeURIComponent(threadId)}`,
    page.url(),
  ).href;
  const guard = pageGuards.get(page);
  NodeAssert.ok(guard, "Every browser page must have the loopback guard installed");
  guard.assertUrl(snapshotUrl);
  const response = await page.request.get(snapshotUrl, {
    headers: { [ORCHESTRATION_PROTOCOL_HEADER]: ORCHESTRATION_PROTOCOL_VERSION_TEXT },
    timeout: timeoutMs,
    maxRedirects: 0,
  });
  NodeAssert.ok(response.ok(), `Thread snapshot failed with HTTP ${response.status()}`);
  const snapshot = decodeSnapshot(await response.json());
  snapshotMetadata.set(page, summarizeSnapshot(snapshot));
  NodeAssert.equal(snapshot.projection.thread.id, threadId);
  return snapshot;
}

function summarizeSnapshot(snapshot: OrchestrationV2ThreadDetailSnapshot) {
  const projection = snapshot.projection;
  return {
    snapshotSequence: snapshot.snapshotSequence,
    threadId: projection.thread.id,
    runs: projection.runs.map(({ id, status, activeAttemptId, rootNodeId, providerThreadId }) => ({
      id,
      status,
      activeAttemptId,
      rootNodeId,
      providerThreadId,
    })),
    attempts: projection.attempts.map(
      ({ id, status, runId, rootNodeId, providerThreadId, providerTurnId }) => ({
        id,
        status,
        runId,
        rootNodeId,
        providerThreadId,
        providerTurnId,
      }),
    ),
    providerTurns: projection.providerTurns.map(
      ({ id, status, runAttemptId, providerThreadId, nodeId, ordinal }) => ({
        id,
        status,
        runAttemptId,
        providerThreadId,
        nodeId,
        ordinal,
      }),
    ),
    runtimeRequests: projection.runtimeRequests.map(({ id, status, nodeId, providerTurnId }) => ({
      id,
      status,
      nodeId,
      providerTurnId,
    })),
  };
}
const snapshotMetadata = new WeakMap<Page, ReturnType<typeof summarizeSnapshot>>();

async function assertCompleted(page: Page, threadId: string, identity: ReplayTurnIdentity) {
  await page.waitForURL((url) => url.pathname.endsWith(`/${threadId}`), { timeout: timeoutMs });
  await page
    .getByText(replayMarker, { exact: false })
    .last()
    .waitFor({ state: "visible", timeout: timeoutMs });
  await page
    .getByRole("button", { name: "Stop generation", exact: true })
    .waitFor({ state: "hidden", timeout: timeoutMs });
  const snapshot = await readThreadSnapshot(page, threadId);
  NodeAssert.equal(
    snapshot.projection.runs.length,
    1,
    "Recovery must not create a second fixture run",
  );
  const run = snapshot.projection.runs.find((item) => item.id === identity.runId);
  const attempt = snapshot.projection.attempts.find((item) => item.id === identity.attemptId);
  const providerTurn = snapshot.projection.providerTurns.find(
    (item) => item.id === identity.providerTurnId,
  );
  NodeAssert.ok(run, "The original replay run must still exist");
  NodeAssert.ok(attempt, "The original replay attempt must still exist");
  NodeAssert.ok(providerTurn, "The original replay provider turn must still exist");
  NodeAssert.equal(run.status, "completed", `Replay run ${run.id} did not complete`);
  NodeAssert.equal(attempt.status, "completed", `Replay attempt ${attempt.id} did not complete`);
  NodeAssert.equal(
    providerTurn.status,
    "completed",
    `Replay provider turn ${providerTurn.id} did not complete`,
  );
  NodeAssert.equal(attempt.runId, run.id);
  NodeAssert.equal(attempt.providerThreadId, run.providerThreadId);
  NodeAssert.equal(attempt.rootNodeId, run.rootNodeId);
  assertReplayTurnLinks(attempt, providerTurn);
  NodeAssert.ok(run.completedAt);
  NodeAssert.ok(attempt.completedAt);
  NodeAssert.ok(providerTurn.completedAt);
  const message = snapshot.projection.messages.find(
    (item) =>
      item.role === "assistant" && item.runId === run.id && item.text.includes(replayMarker),
  );
  NodeAssert.ok(message, "The completed replay text must belong to the recorded run");
  NodeAssert.equal(message.streaming, false);
  return {
    ...identity,
    projectId: snapshot.projection.thread.projectId,
    threadId,
    snapshotSequence: snapshot.snapshotSequence,
    runStatus: run.status,
    attemptStatus: attempt.status,
    providerTurnStatus: providerTurn.status,
    runCompletedAt: DateTime.formatIso(run.completedAt),
    attemptCompletedAt: DateTime.formatIso(attempt.completedAt),
    providerTurnCompletedAt: DateTime.formatIso(providerTurn.completedAt),
    assistantMessageId: message.id,
  };
}

async function exercise(implementation: Implementation): Promise<void> {
  const directory = NodePath.join(proofRoot, implementation);
  NodeFS.mkdirSync(directory, { recursive: true });
  const workspace = NodePath.join(runRoot, implementation, "hybrid-fixture");
  const stateDirectory = NodePath.join(runRoot, implementation, "state");
  NodeFS.mkdirSync(NodePath.join(workspace, "src"), { recursive: true });
  const isolation = createChildIsolation(
    NodePath.join(runRoot, implementation, "isolated"),
    nodeExecutable,
    gitExecutable,
  );
  const configuration = replayConfiguration(repoRoot, nodeExecutable);
  const policyPath = NodePath.join(isolation.root, "policy.json");
  NodeFS.mkdirSync(NodePath.join(stateDirectory, "userdata"), { recursive: true, mode: 0o700 });
  NodeFS.writeFileSync(
    NodePath.join(stateDirectory, "userdata/settings.json"),
    `${JSON.stringify(configuration.settings)}\n`,
    { flag: "wx", mode: 0o600 },
  );
  NodeFS.writeFileSync(policyPath, `${JSON.stringify(configuration.policy)}\n`, {
    flag: "wx",
    mode: 0o600,
  });
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
  NodeAssert.ok(serverPort && webPort);
  NodeAssert.notEqual(serverPort, webPort, "Server and client need distinct ports");
  const proof = {
    schemaVersion: 1,
    status: "running",
    implementation,
    scope:
      "Linux original web client; local project; free ACP showcase approval; reload; same-state server restart",
    startedAt: new Date().toISOString(),
    finishedAt: null as string | null,
    source,
    platform: runnerPlatform,
    architecture: runnerArchitecture,
    node: process.version,
    browser: browser!.version(),
    stateDirectory,
    fixtureDirectory: workspace,
    serverPort,
    webPort,
    processGroups: [] as number[],
    currentStep: "application startup",
    completedSteps: [] as string[],
    threadId: null as string | null,
    threadUrl: null as string | null,
    turnIdentity: null as ReplayTurnIdentity | null,
    completedSnapshots: [] as Array<
      Awaited<ReturnType<typeof assertCompleted>> & { checkpoint: string }
    >,
    persistence: [] as Array<ReturnType<typeof inspectNativeReplay> & { checkpoint: string }>,
    browserErrors: [] as string[],
    error: null as string | null,
  };
  const saveProof = () => writeJson(NodePath.join(directory, "proof.json"), proof);
  const starting = (step: string) => {
    proof.currentStep = step;
    saveProof();
    console.log(`[${implementation}] Starting ${step}`);
  };
  const completed = (step: string) => {
    proof.completedSteps.push(step);
    saveProof();
    console.log(`[${implementation}] ${step}`);
  };
  const inspectPersistence = (checkpoint: string, closed = false) => {
    if (implementation !== "native") return;
    const expected = proof.completedSnapshots.at(-1);
    NodeAssert.ok(expected, "A completed client snapshot must precede native row verification");
    proof.persistence.push({
      checkpoint,
      ...inspectNativeReplay({
        executable: nativeExecutable("db"),
        stateDirectory,
        expected,
        closed,
      }),
    });
    saveProof();
  };
  saveProof();
  const contextOptions = {
    viewport: { width: 1440, height: 1000 },
    reducedMotion: "reduce" as const,
    serviceWorkers: "block" as const,
  };
  const guard = loopbackGuard([serverPort, webPort]);
  let context = await browser!.newContext(contextOptions);
  await guardContext(context, guard);
  let page = await context.newPage();
  function observePage(current: Page): void {
    pageGuards.set(current, guard);
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

  let flowPromise: Promise<void> | undefined;
  try {
    const flow = async () => {
      let application = await startApplication(
        implementation,
        stateDirectory,
        serverPort,
        webPort,
        NodePath.join(directory, "startup.log"),
        isolation,
        policyPath,
      );
      proof.processGroups.push(...application.pids);
      starting("initial setup");
      await page.goto(application.pairingUrl, { waitUntil: "domcontentloaded" });
      await completeInitialSetup(page);
      completed(
        "Completed initial setup through the normal UI without agent sign-in or project import",
      );
      starting("local project creation");
      await page.getByRole("button", { name: "Add project", exact: true }).first().click();
      const dialog = page.getByRole("dialog");
      await dialog.getByText("Local folder", { exact: true }).click();
      await dialog.getByRole("combobox").fill(workspace);
      await dialog.getByRole("button", { name: /^Add \(/u }).click();
      await dialog.waitFor({ state: "hidden" });
      await page.getByText("hybrid-fixture", { exact: true }).first().waitFor({ state: "visible" });
      completed("Created local project through the normal client");

      starting("controlled replay and approval");
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
      const turnIdentity = replayTurnIdentity(await readThreadSnapshot(page, threadId));
      proof.turnIdentity = turnIdentity;
      await capture(page, directory, "approval");
      await approval.click();
      completed("Approved the controlled replay provider request in the normal client");
      starting("replay completion and durable snapshot");
      proof.completedSnapshots.push({
        checkpoint: "initial",
        ...(await assertCompleted(page, threadId, turnIdentity)),
      });
      proof.threadUrl = page.url();
      inspectPersistence("initial");
      completed("Received the completed replay turn on the original thread route");

      starting("client reload");
      await page.reload({ waitUntil: "domcontentloaded" });
      proof.completedSnapshots.push({
        checkpoint: "client-reload",
        ...(await assertCompleted(page, threadId, turnIdentity)),
      });
      NodeAssert.equal(page.url(), proof.threadUrl);
      inspectPersistence("client-reload");
      completed("Reconnected after a client reload with the same completed thread");

      starting("owned server restart");
      await application.stop();
      inspectPersistence("closed-before-restart", true);
      guard.assertUrl(`http://127.0.0.1:${webPort}`);
      application = await startApplication(
        implementation,
        stateDirectory,
        serverPort,
        webPort,
        NodePath.join(directory, "restart.log"),
        isolation,
        policyPath,
      );
      proof.processGroups.push(...application.pids);
      starting("durable recovery in a fresh client");
      // Keep only the authenticated session cookies. A fresh client cannot satisfy recovery
      // from localStorage or IndexedDB, and must retrieve the old thread from the restarted server.
      // Do not consume the restart's new pairing token or persist the session cookies in proof.
      const cookies = await context.cookies();
      await context.close();
      context = await browser!.newContext({
        ...contextOptions,
        storageState: { cookies, origins: [] },
      });
      await guardContext(context, guard);
      page = await context.newPage();
      observePage(page);
      await page.goto(proof.threadUrl, { waitUntil: "domcontentloaded" });
      proof.completedSnapshots.push({
        checkpoint: "server-restart",
        ...(await assertCompleted(page, threadId, turnIdentity)),
      });
      NodeAssert.equal(page.url(), proof.threadUrl);
      inspectPersistence("server-restart");
      completed(
        "Recovered the same durable thread after server restart in a fresh client with the existing session",
      );
      await capture(page, directory, "completed");
      if (implementation === "native") {
        NodeAssert.equal(proof.persistence.length, 4);
        completed(
          "Verified canonical omdb rows and event heads through read-only native inspection, with no application SQLite files",
        );
      }
      proof.status = "passed";
    };
    flowPromise = flow();
    await Promise.race([flowPromise, guard.failure]);
  } catch (error) {
    if (guard.violated()) {
      await context.close();
      await activeApplication?.stop();
      await flowPromise?.catch(() => {});
    }
    proof.status = "failed";
    proof.error = redact(error instanceof Error ? (error.stack ?? error.message) : String(error));
    try {
      await capture(page, directory, "failure");
    } catch (captureError) {
      proof.browserErrors.push(redact(`Could not capture failure: ${captureError}`));
    }
    console.error(
      `[${implementation}] Failure progress: ${JSON.stringify({
        step: proof.currentStep,
        url: redact(page.url()),
        completedSteps: proof.completedSteps,
        threadId: proof.threadId,
        turnIdentity: proof.turnIdentity,
        browserErrors: proof.browserErrors.slice(-5),
      })}`,
    );
    const metadata = snapshotMetadata.get(page);
    if (metadata) {
      writeJson(NodePath.join(directory, "failure-snapshot-metadata.json"), metadata);
      console.error(`[${implementation}] Last snapshot links: ${JSON.stringify(metadata)}`);
    }
    const snapshotPath = NodePath.join(directory, "failure.aria.txt");
    if (NodeFS.existsSync(snapshotPath)) {
      const snapshot = NodeFS.readFileSync(snapshotPath, "utf8");
      console.error(
        `[${implementation}] Failure accessibility snapshot (first 8,000 characters):\n${snapshot.slice(0, 8_000)}`,
      );
    }
    throw error;
  } finally {
    await activeApplication?.stop();
    activeApplication = undefined;
    await context.close();
    proof.finishedAt = new Date().toISOString();
    // A blocked late request during teardown must invalidate an otherwise
    // completed UI flow; Promise.race alone cannot provide that guarantee.
    if (guard.violated()) {
      proof.status = "failed";
      proof.error ??= "Browser attempted a request outside the qualification boundary";
      saveProof();
      await guard.failure;
    }
    saveProof();
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
  if (executablePath !== undefined) {
    NodeAssert.ok(
      NodePath.isAbsolute(executablePath) && NodeFS.statSync(executablePath).isFile(),
      "A Chromium override must be an explicit installed executable",
    );
  }
  const browserIsolation = createChildIsolation(
    NodePath.join(runRoot, "browser"),
    nodeExecutable,
    gitExecutable,
  );
  browser = await chromium.launch({
    ...(executablePath ? { executablePath } : {}),
    headless: true,
    env: browserIsolation.env,
    args: browserIsolationArgs,
  });
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
