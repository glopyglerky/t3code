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
import {
  ORCHESTRATION_PROTOCOL_HEADER,
  ORCHESTRATION_PROTOCOL_VERSION_TEXT,
  OrchestrationV2ThreadDetailSnapshot,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Schema from "effect/Schema";
import { chromium, type Browser, type Page } from "playwright";
import { nativeExecutable } from "@t3tools/shared/nativeExecutable";
import { inspectNativeReplay } from "./hybrid-client-database.ts";
import {
  assertReplayTurnLinks,
  replayTurnIdentity,
  type ReplayTurnIdentity,
} from "./hybrid-client-turn.ts";

type Implementation = "upstream" | "native";
type DevReplayWindow = Window & {
  t3dev?: { replayScenarios: () => string[]; replay: (scenario: string) => Promise<string> };
};
type OwnedApplication = { pid: number; pairingUrl: string; stop: () => Promise<void> };

const repoRoot = NodeFS.realpathSync(NodeURL.fileURLToPath(new URL("../../../", import.meta.url)));
const timeoutMs = 90_000;
const startupTimeoutMs = 180_000;
const replayMarker = "This turn is a replay.";
const decodeSnapshot = Schema.decodeUnknownSync(
  Schema.toCodecJson(OrchestrationV2ThreadDetailSnapshot),
);
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
  const response = await page.request.get(
    new URL(`/api/orchestration/threads/${encodeURIComponent(threadId)}`, page.url()).href,
    {
      headers: { [ORCHESTRATION_PROTOCOL_HEADER]: ORCHESTRATION_PROTOCOL_VERSION_TEXT },
      timeout: timeoutMs,
    },
  );
  Assert.ok(response.ok(), `Thread snapshot failed with HTTP ${response.status()}`);
  const snapshot = decodeSnapshot(await response.json());
  snapshotMetadata.set(page, summarizeSnapshot(snapshot));
  Assert.equal(snapshot.projection.thread.id, threadId);
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
  Assert.equal(snapshot.projection.runs.length, 1, "Recovery must not create a second fixture run");
  const run = snapshot.projection.runs.find((item) => item.id === identity.runId);
  const attempt = snapshot.projection.attempts.find((item) => item.id === identity.attemptId);
  const providerTurn = snapshot.projection.providerTurns.find(
    (item) => item.id === identity.providerTurnId,
  );
  Assert.ok(run, "The original replay run must still exist");
  Assert.ok(attempt, "The original replay attempt must still exist");
  Assert.ok(providerTurn, "The original replay provider turn must still exist");
  Assert.equal(run.status, "completed", `Replay run ${run.id} did not complete`);
  Assert.equal(attempt.status, "completed", `Replay attempt ${attempt.id} did not complete`);
  Assert.equal(
    providerTurn.status,
    "completed",
    `Replay provider turn ${providerTurn.id} did not complete`,
  );
  Assert.equal(attempt.runId, run.id);
  Assert.equal(attempt.providerThreadId, run.providerThreadId);
  Assert.equal(attempt.rootNodeId, run.rootNodeId);
  assertReplayTurnLinks(attempt, providerTurn);
  Assert.ok(run.completedAt);
  Assert.ok(attempt.completedAt);
  Assert.ok(providerTurn.completedAt);
  const message = snapshot.projection.messages.find(
    (item) =>
      item.role === "assistant" && item.runId === run.id && item.text.includes(replayMarker),
  );
  Assert.ok(message, "The completed replay text must belong to the recorded run");
  Assert.equal(message.streaming, false);
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
    Assert.ok(expected, "A completed client snapshot must precede native row verification");
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
    Assert.equal(page.url(), proof.threadUrl);
    inspectPersistence("client-reload");
    completed("Reconnected after a client reload with the same completed thread");

    starting("owned server restart");
    await application.stop();
    inspectPersistence("closed-before-restart", true);
    application = await startApplication(
      implementation,
      stateDirectory,
      serverPort,
      webPort,
      NodePath.join(directory, "restart.log"),
    );
    proof.processGroups.push(application.pid);
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
    page = await context.newPage();
    observePage(page);
    await page.goto(proof.threadUrl, { waitUntil: "domcontentloaded" });
    proof.completedSnapshots.push({
      checkpoint: "server-restart",
      ...(await assertCompleted(page, threadId, turnIdentity)),
    });
    Assert.equal(page.url(), proof.threadUrl);
    inspectPersistence("server-restart");
    completed(
      "Recovered the same durable thread after server restart in a fresh client with the existing session",
    );
    await capture(page, directory, "completed");
    if (implementation === "native") {
      Assert.equal(proof.persistence.length, 4);
      completed(
        "Verified canonical omdb rows and event heads through read-only native inspection, with no application SQLite files",
      );
    }
    proof.status = "passed";
  } catch (error) {
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
