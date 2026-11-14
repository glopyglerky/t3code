#!/usr/bin/env node
import * as NodeAssert from "node:assert/strict";
import * as NodeChildProcess from "node:child_process";
import * as NodeCrypto from "node:crypto";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeProcess from "node:process";
import * as NodeURL from "node:url";
import * as NodeUtil from "node:util";
import * as Effect from "effect/Effect";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as PlatformCrypto from "@effect/platform-node/NodeCrypto";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Logger from "effect/Logger";
import * as SqlClient from "effect/sql/SqlClient";
import * as Omdb from "../apps/server/src/persistence/Omdb.ts";
import { createFixture } from "./database-fixture.mjs";
import { exportSqliteState } from "./export-sqlite-state.mjs";
import { hash } from "./redb-domain-fixture.mjs";
import { fixedTime, jsonPayloads, makeEvent } from "./redb-event-sink-fixtures.mjs";
import {
  PERFORMANCE_POLICY,
  balancedOrders,
  comparePairs,
  quantile,
  summarize,
} from "./performance-statistics.mjs";
import contract from "./performance-contract.json" with { type: "json" };
import { createPinnedBaseline } from "./performance-baseline.mjs";
import { processTreeRss } from "./performance-memory.mjs";

const root = NodeURL.fileURLToPath(new URL("../", import.meta.url));
const workerPath = NodeURL.fileURLToPath(new URL("./performance-worker.mjs", import.meta.url));
const allowed = new Set([
  "database",
  "output",
  "pairs",
  "warmups",
  "samples",
  "mode",
  "manifest",
  "plan",
  "phase",
]);
const options = {};
for (let index = 2; index < NodeProcess.argv.length; index += 2) {
  const flag = NodeProcess.argv[index];
  const value = NodeProcess.argv[index + 1];
  NodeAssert.ok(
    flag.startsWith("--") && allowed.has(flag.slice(2)) && value !== undefined,
    "Expected --database PATH --output DIRECTORY [--mode diagnostic|qualify] [--manifest PATH --plan PATH]",
  );
  NodeAssert.ok(!(flag.slice(2) in options), `Repeated argument ${flag}`);
  options[flag.slice(2)] = value;
}
const mode = options.mode ?? "qualify";
NodeAssert.ok(["diagnostic", "qualify"].includes(mode));
const phase = options.phase ?? "all";
NodeAssert.ok(["all", "validation"].includes(phase));
const configuration = {
  pairs: Number(options.pairs ?? PERFORMANCE_POLICY.minimum_pairs),
  warmups: Number(options.warmups ?? PERFORMANCE_POLICY.warmups),
  samples: Number(options.samples ?? PERFORMANCE_POLICY.samples),
  seed: PERFORMANCE_POLICY.seed,
  bootstrap_resamples: PERFORMANCE_POLICY.bootstrap_resamples,
  confidence: PERFORMANCE_POLICY.confidence,
  comparison_ratio: 1,
  fixture: contract.fixture,
  phase,
  worker_timeout_ms: 15 * 60 * 1000,
  worker_output_limit_bytes: 16 * 1024 * 1024,
  worker_shutdown_grace_ms: 2000,
  rss_sampling_interval_ms: 25,
};
NodeAssert.ok(
  Number.isInteger(configuration.pairs) &&
    configuration.pairs >= 2 &&
    configuration.pairs % 2 === 0,
);
NodeAssert.ok(Number.isInteger(configuration.warmups) && configuration.warmups >= 1);
NodeAssert.ok(Number.isInteger(configuration.samples) && configuration.samples >= 3);
if (mode === "qualify") {
  NodeAssert.equal(phase, "all");
  NodeAssert.equal(
    configuration.pairs,
    PERFORMANCE_POLICY.minimum_pairs,
    "Qualification uses the preregistered eight pairs",
  );
  NodeAssert.equal(configuration.warmups, PERFORMANCE_POLICY.warmups);
  NodeAssert.equal(configuration.samples, PERFORMANCE_POLICY.samples);
}
NodeAssert.ok(options.database && NodePath.isAbsolute(options.database));
NodeAssert.ok(options.output && NodePath.isAbsolute(options.output));
await NodeFSP.access(options.database, NodeFSP.constants.X_OK);
await NodeFSP.mkdir(options.output, { recursive: true });
const output = await NodeFSP.realpath(options.output);
NodeAssert.ok(
  !output.startsWith(`${await NodeFSP.realpath(root)}${NodePath.sep}`) &&
    output !== (await NodeFSP.realpath(root)),
  "Performance output must be outside the source checkout",
);
NodeAssert.equal(
  (await NodeFSP.readdir(output)).length,
  0,
  "Use a fresh output directory for every attempt; failed or inconclusive attempts must not be overwritten",
);
const git = (...args) =>
  NodeChildProcess.execFileSync("git", ["--no-replace-objects", "-C", root, ...args], {
    encoding: "utf8",
  }).trim();
const lockBytes = await NodeFSP.readFile(NodePath.join(root, "upstream.lock.json"));
const upstream = JSON.parse(lockBytes).upstream;
const toolchainBytes = await NodeFSP.readFile(NodePath.join(root, "toolchain.lock.json"));
const toolchain = JSON.parse(toolchainBytes);
const report = {
  schema_version: 1,
  suite: "persistence-api",
  status: "running",
  mode,
  attempt_id: `${new Date().toISOString().replaceAll(":", "-")}-${NodeCrypto.randomBytes(4).toString("hex")}`,
  source_commit: git("rev-parse", "HEAD"),
  source_tree: git("rev-parse", "HEAD^{tree}"),
  source_tree_dirty: git("status", "--porcelain", "--untracked-files=normal").length !== 0,
  policy_digest: null,
  upstream_commit: upstream.commit,
  upstream_tree: upstream.root_tree,
  compiler_route: "bluefe-rustc-via-lustr",
  compiler_identity: toolchain.compatibility,
  build_profile: "unverified-diagnostic",
  toolchain_lock_sha256: hash(toolchainBytes),
  native_manifest_sha256: null,
  executable_sha256: hash(await NodeFSP.readFile(options.database)),
  ci_run_id: NodeProcess.env.GITHUB_RUN_ID ?? null,
  ci_run_attempt: NodeProcess.env.GITHUB_RUN_ATTEMPT ?? null,
  command: [NodeProcess.execPath, ...NodeProcess.argv.slice(1)],
  configuration,
  host: {
    platform: Effect.runSync(HostProcess.Platform),
    architecture: Effect.runSync(HostProcess.Architecture),
    release: NodeOS.release(),
    cpu_model: NodeOS.cpus()[0]?.model ?? null,
    cpus: NodeOS.availableParallelism(),
    total_memory_bytes: NodeOS.totalmem(),
    load_before: NodeOS.loadavg(),
    node: NodeProcess.version,
  },
  correctness: { status: "pending", checks: [] },
  durability: {
    upstream: {
      journal_mode: "wal",
      synchronous: "NORMAL",
      synchronous_numeric: 1,
      reader_worker: true,
      writer_worker: true,
    },
    native: { engine: "omdb", durability: "Immediate" },
    comparison:
      "Actual selected production defaults; stronger native commit durability is preserved and any cost is included in visit-commit.",
  },
  scenarios: null,
  runs: [],
  sources: {},
  decisions: [
    "The workloads are fixed before measurements: sidebar-shell, history-window, message-search, accent-search, and visit-commit transaction/readback.",
    "Every scenario gates independently; a faster scenario cannot offset a slower one.",
    "Eight balanced, seeded process pairs and all requests are retained. No slow sample is dropped or retried.",
    "Confidence intervals compare native/upstream latency against exactly 1.0. A 10% interval width is only a noise diagnostic, never a regression allowance.",
    "Migration, fixture seeding and native index preparation occur before timed workers; process-cold service acquisition and warm API calls are reported separately.",
    "The write comparison uses actual production settings: stock SQLite WAL/NORMAL and native Immediate. Native commits retain stronger power-loss durability; any associated slowdown remains visible and gates qualification.",
    "Rollback and committed reopen are required correctness checks; they do not prove power-failure durability.",
  ],
  limitations: [
    "This measures the production persistence/API boundary, including native IPC and serialization, not complete UI, network, provider or whole-application performance.",
    "Cold means a fresh Node process and service open. The shared OS page cache is not flushed or claimed cold; backend order is balanced.",
    "Warm request timings exclude result fingerprinting; native IPC/serialization and upstream worker messaging remain inside the timed operation.",
    "RSS is a 25 ms sample of the Node process plus its native descendants. It includes shared pages more than once and can miss short peaks; it is advisory and never a heap or memory-safety claim.",
    "If /proc does not expose a process and its descendants, whole-process RSS is explicitly unavailable (null), never zero or a Node-only substitute.",
    "V8 GC is observed without forced collections. Node peak RSS excludes child helpers and is separately labeled; the process-tree sample includes them.",
    "Shared CI load, filesystem caching, CPU frequency and small-sample tail uncertainty remain. Inconclusive evidence cannot establish equivalence or qualify this commit.",
  ],
};
const artifacts = {
  json: NodePath.join(output, "performance.json"),
  markdown: NodePath.join(output, "performance.md"),
};
const format = (value) => (Number.isFinite(value) ? value.toFixed(3) : "unavailable");
function markdown() {
  const lines = [
    "# Persistence performance comparison",
    "",
    `Status: **${report.status}**. Mode: ${mode}. Attempt: ${report.attempt_id}.`,
    "",
    `Fork commit: \`${report.source_commit}\`. Tree: \`${report.source_tree}\`. Dirty: ${report.source_tree_dirty}.`,
    `Pinned upstream: \`${report.upstream_commit}\`. Tree: \`${report.upstream_tree}\`.`,
    `Compiler route: ${report.compiler_route}. Build profile: ${report.build_profile}.`,
    `Compiler revision: \`${toolchain.compatibility.compiler_revision}\`. Coordinator: \`${toolchain.compatibility.coordinator_revision}\`.`,
    `Executable SHA-256: \`${report.executable_sha256}\`. Toolchain lock: \`${report.toolchain_lock_sha256}\`.`,
    `Manifest SHA-256: \`${report.native_manifest_sha256 ?? "not supplied"}\`. Policy: \`${report.policy_digest ?? "not supplied"}\`.`,
    "",
    "## Results",
    "",
    "| Scenario | Phase | Upstream median / p95 / worst ms | Native median / p95 / worst ms | Paired native/upstream ratio | Latency change | 95% interval | Decision |",
    "|---|---|---:|---:|---:|---:|---|---|",
  ];
  for (const row of report.scenarios ?? [])
    lines.push(
      `| ${row.id} | ${row.phase} | ${format(row.upstream.median_ms)} / ${format(row.upstream.p95_ms)} / ${format(row.upstream.max_ms)} | ${format(row.native.median_ms)} / ${format(row.native.p95_ms)} / ${format(row.native.max_ms)} | ${format(row.ratio)} | ${row.latency_change_percent >= 0 ? "+" : ""}${format(row.latency_change_percent)}% | ${row.confidence_interval.map(format).join("–")} | ${row.status} |`,
    );
  if (report.scenarios === null)
    lines.push(
      "",
      "No performance verdict is available until all correctness and source checks pass.",
    );
  lines.push(
    "",
    "Negative latency change means faster; positive means slower for the named workload and statistic only.",
    "",
    "## Reasons and divergences",
    "",
  );
  for (const row of report.scenarios ?? [])
    lines.push(
      `- ${row.id}: ${row.reason}${row.precision_warning ? " The relative interval width exceeds 10%; investigate runner noise." : ""}`,
    );
  if (report.error) lines.push(`- Failure: ${report.error}`);
  lines.push("", `Correctness: ${report.correctness.status}.`, "");
  for (const check of report.correctness.checks) lines.push(`- ${check}`);
  lines.push("", "## Protocol and decisions", "");
  for (const decision of report.decisions) lines.push(`- ${decision}`);
  lines.push("", "## Measurement limits", "");
  for (const limit of report.limitations) lines.push(`- ${limit}`);
  lines.push(
    "",
    "## Reproduction",
    "",
    "```json",
    JSON.stringify(
      {
        command: report.command,
        configuration,
        host: report.host,
        fixture: report.fixture,
        baseline: report.baseline,
        durability: report.durability,
        sources: report.sources,
      },
      null,
      2,
    ),
    "```",
    "",
    "Raw process order, every request duration, GC observations and sampled process-tree RSS are in performance.json and the worker JSON files. No result row is omitted.",
    "",
  );
  return lines.join("\n");
}
async function save() {
  await NodeFSP.writeFile(artifacts.json, `${JSON.stringify(report, null, 2)}\n`);
  await NodeFSP.writeFile(artifacts.markdown, markdown());
}

let active;
const cancellation = new AbortController();
const cancel = () => cancellation.abort();
NodeProcess.default.once("SIGTERM", cancel);
NodeProcess.default.once("SIGINT", cancel);

async function runWorker(name, config) {
  cancellation.signal.throwIfAborted();
  const configPath = NodePath.join(output, `${name}.config.json`);
  await NodeFSP.writeFile(configPath, JSON.stringify(config));
  const child = NodeChildProcess.spawn(NodeProcess.execPath, [workerPath, configPath], {
    cwd: root,
    env: { ...NodeProcess.env, T3CODE_DEV_SEED: "0" },
    stdio: ["ignore", "pipe", "pipe"],
    detached: NodeProcess.platform !== "win32",
  });
  active = child;
  let stdout = "",
    stderr = "",
    peak = 0,
    rssSamples = 0,
    rssAttempts = 0,
    sampling = false,
    failure,
    escalation,
    outputBytes = 0;
  const rssUnavailableReasons = new Set();
  const signalGroup = (signal) => {
    if (child.pid === undefined) return;
    try {
      if (NodeProcess.platform === "win32") child.kill(signal);
      else NodeProcess.kill(-child.pid, signal);
    } catch (error) {
      if (error.code !== "ESRCH") throw error;
    }
  };
  const stop = () => {
    signalGroup("SIGTERM");
    escalation ??= setTimeout(() => signalGroup("SIGKILL"), configuration.worker_shutdown_grace_ms);
  };
  cancellation.signal.addEventListener("abort", stop, { once: true });
  const timer = setTimeout(() => {
    failure = "Worker timeout";
    stop();
  }, configuration.worker_timeout_ms);
  const sampler = setInterval(async () => {
    if (sampling || NodeProcess.platform !== "linux") return;
    sampling = true;
    try {
      rssAttempts++;
      const sample = await processTreeRss(child.pid);
      if (sample.rss_kib === null) rssUnavailableReasons.add(sample.reason);
      else {
        peak = Math.max(peak, sample.rss_kib);
        rssSamples++;
      }
    } catch (error) {
      failure = `RSS sampler: ${error.message}`;
      stop();
    } finally {
      sampling = false;
    }
  }, configuration.rss_sampling_interval_ms);
  child.stdout.setEncoding("utf8").on("data", (chunk) => {
    outputBytes += Buffer.byteLength(chunk);
    if (outputBytes <= configuration.worker_output_limit_bytes) stdout += chunk;
    else {
      failure = "Worker output limit exceeded";
      stop();
    }
  });
  child.stderr.setEncoding("utf8").on("data", (chunk) => {
    outputBytes += Buffer.byteLength(chunk);
    if (outputBytes <= configuration.worker_output_limit_bytes) stderr += chunk;
    else {
      failure = "Worker output limit exceeded";
      stop();
    }
  });
  const exit = await new Promise((resolve, reject) => {
    child.once("error", (error) => {
      failure = `Worker spawn failed: ${error.message}`;
      reject(error);
    });
    child.once("close", (code, signal) => resolve({ code, signal }));
  }).finally(() => {
    if (failure !== undefined || cancellation.signal.aborted) signalGroup("SIGKILL");
    clearTimeout(timer);
    clearTimeout(escalation);
    clearInterval(sampler);
    cancellation.signal.removeEventListener("abort", stop);
    active = undefined;
  });
  await NodeFSP.writeFile(NodePath.join(output, `${name}.stderr.log`), stderr);
  await NodeFSP.writeFile(NodePath.join(output, `${name}.stdout.log`), stdout);
  NodeAssert.equal(exit.code, 0, `${name}: ${failure ?? (stderr.slice(-6000) || exit.signal)}`);
  NodeAssert.equal(failure, undefined, `${name}: ${failure}`);
  const data = JSON.parse(stdout);
  data.process_tree_peak_rss_kib = NodeProcess.platform === "linux" && rssSamples > 0 ? peak : null;
  data.process_tree_rss_status = rssSamples > 0 ? "sampled" : "unavailable";
  data.process_tree_rss_samples = rssSamples;
  data.process_tree_rss_attempts = rssAttempts;
  data.process_tree_rss_unavailable_reasons = [...rssUnavailableReasons].sort();
  data.pair = config.pair ?? null;
  data.order = config.order ?? null;
  await NodeFSP.writeFile(
    NodePath.join(output, `${name}.json`),
    `${JSON.stringify(data, null, 2)}\n`,
  );
  report.runs.push(data);
  await save();
  return data;
}

const seed = (projection, project) =>
  Effect.fn("Performance.seed")(function* (sql) {
    const projects = yield* project.ProjectStoreV2.pipe(Effect.provide(project.layer));
    const store = yield* projection.ProjectionStoreV2.pipe(Effect.provide(projection.layer));
    yield* sql.withTransaction(
      Effect.gen(function* () {
        yield* projects.apply({
          sequence: 0,
          eventId: "event:performance:project",
          aggregateKind: "project",
          aggregateId: "project:main",
          type: "project.created",
          occurredAt: fixedTime,
          commandId: null,
          causationEventId: null,
          correlationId: null,
          metadata: {},
          payload: {
            projectId: "project:main",
            title: "Performance fixture",
            workspaceRoot: "/fixture/performance",
            defaultModelSelection: null,
            scripts: [],
            createdAt: fixedTime,
            updatedAt: fixedTime,
          },
        });
        for (let index = 0; index < configuration.fixture.threads; index++) {
          const id = index === 0 ? "thread:performance:long" : `thread:performance:${index}`;
          yield* store.apply(
            makeEvent(
              "thread.created",
              {
                ...jsonPayloads.thread,
                id,
                title: `Benchmark thread ${index}`,
                lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: id },
                archivedAt: index % 8 === 7 ? fixedTime : null,
              },
              { threadId: id },
            ),
          );
        }
        yield* store.apply(
          makeEvent(
            "thread.created",
            {
              ...jsonPayloads.thread,
              id: "thread:performance:write",
              title: "Isolated commit fixture",
              lineage: {
                parentThreadId: null,
                relationshipToParent: null,
                rootThreadId: "thread:performance:write",
              },
              archivedAt: fixedTime,
            },
            { threadId: "thread:performance:write" },
          ),
        );
        const threadId = "thread:performance:long",
          runId = "run:performance",
          nodeId = "node:performance",
          attemptId = "attempt:performance",
          providerThreadId = "provider:performance",
          providerTurnId = "turn:performance",
          providerSessionId = "session:performance";
        const event = (type, payload) => makeEvent(type, payload, { threadId });
        yield* store.apply(
          event("provider-session.attached", {
            ...jsonPayloads.providerSession,
            id: providerSessionId,
          }),
        );
        yield* store.apply(
          event("provider-thread.updated", {
            ...jsonPayloads.providerThread,
            id: providerThreadId,
            providerSessionId,
            appThreadId: threadId,
            ownerNodeId: nodeId,
          }),
        );
        yield* store.apply(
          event("run.created", {
            ...jsonPayloads.run,
            id: runId,
            threadId,
            rootNodeId: nodeId,
            activeAttemptId: attemptId,
            userMessageId: "message:performance:input",
            providerThreadId,
            status: "completed",
            completedAt: fixedTime,
          }),
        );
        yield* store.apply(
          event("run-attempt.created", {
            ...jsonPayloads.attempt,
            id: attemptId,
            runId,
            rootNodeId: nodeId,
            providerThreadId,
            providerTurnId,
            status: "completed",
            completedAt: fixedTime,
          }),
        );
        yield* store.apply(
          event("node.updated", {
            ...jsonPayloads.node,
            id: nodeId,
            threadId,
            runId,
            rootNodeId: nodeId,
            providerThreadId,
            providerTurnId,
            checkpointScopeId: null,
            status: "completed",
            completedAt: fixedTime,
          }),
        );
        yield* store.apply(
          event("provider-turn.updated", {
            ...jsonPayloads.providerTurn,
            id: providerTurnId,
            providerThreadId,
            nodeId,
            runAttemptId: attemptId,
            status: "completed",
            completedAt: fixedTime,
          }),
        );
        yield* store.apply(
          event("message.updated", {
            ...jsonPayloads.message,
            id: "message:performance:input",
            threadId,
            runId,
            nodeId,
            role: "user",
            createdBy: "user",
            creationSource: "web",
            text: "Inspect benchmark fixture",
            streaming: false,
          }),
        );
        for (let index = 0; index < configuration.fixture.history_items; index++) {
          yield* store.apply(
            event("turn-item.updated", {
              ...jsonPayloads.turnItem,
              id: `item:performance:${index}`,
              threadId,
              runId,
              nodeId,
              providerThreadId,
              providerTurnId,
              ordinal: index + 1,
              status: "completed",
              completedAt: fixedTime,
              streaming: false,
              text: `Tool result ${index}: ${"x".repeat(configuration.fixture.payload_bytes)}`,
              ...(index === 0
                ? {
                    type: "user_message",
                    messageId: "message:performance:input",
                    createdBy: "user",
                    creationSource: "web",
                    inputIntent: "turn_start",
                    text: "Inspect benchmark fixture",
                    attachments: [],
                  }
                : {}),
            }),
          );
        }
        for (let index = 0; index < configuration.fixture.messages; index++) {
          const owner = index % configuration.fixture.threads;
          const id = owner === 0 ? threadId : `thread:performance:${owner}`;
          const text =
            index % 37 === 0
              ? `benchmark needle result ${index}`
              : index % 31 === 0
                ? `école fermée result ${index}`
                : `ordinary benchmark material ${index}`;
          yield* store.apply(
            makeEvent(
              "message.updated",
              {
                ...jsonPayloads.message,
                id: `message:performance:${index}`,
                threadId: id,
                runId: null,
                nodeId: null,
                role: "user",
                createdBy: "user",
                creationSource: "web",
                text,
                streaming: false,
              },
              { threadId: id },
            ),
          );
        }
      }),
    );
  });

const canonical = (value) =>
  Array.isArray(value)
    ? value.map(canonical)
    : value !== null && typeof value === "object"
      ? Object.fromEntries(
          Object.keys(value)
            .sort()
            .map((key) => [key, canonical(value[key])]),
        )
      : value;
let fixture;
try {
  await save();
  if (options.manifest) {
    const bytes = await NodeFSP.readFile(options.manifest);
    const manifest = JSON.parse(bytes);
    report.native_manifest_sha256 = hash(bytes);
    report.native_build_run_attempt = manifest.ci_run_attempt ?? null;
    report.build_profile = manifest.build_profile ?? "unverified-diagnostic";
    NodeAssert.equal(manifest.files[NodePath.basename(options.database)], report.executable_sha256);
    if (mode === "qualify") {
      NodeAssert.equal(manifest.source_commit, report.source_commit);
      NodeAssert.equal(manifest.source_tree, report.source_tree);
      NodeAssert.equal(manifest.source_tree_dirty, false);
      NodeAssert.equal(manifest.build_profile, "release");
      NodeAssert.equal(manifest.compiler_route, report.compiler_route);
      NodeAssert.equal(manifest.toolchain_lock_sha256, report.toolchain_lock_sha256);
      NodeAssert.equal(manifest.ci_run_id, report.ci_run_id);
      NodeAssert.ok(report.ci_run_id !== null);
      NodeAssert.ok(report.ci_run_attempt !== null);
    }
  }
  if (options.plan) {
    const plan = JSON.parse(await NodeFSP.readFile(options.plan, "utf8"));
    NodeAssert.equal(plan.head, report.source_commit);
    NodeAssert.equal(plan.source_tree, report.source_tree);
    NodeAssert.match(plan.policy_digest, /^[a-f0-9]{64}$/);
    report.policy_digest = plan.policy_digest;
  }
  if (mode === "qualify") {
    NodeAssert.ok(
      options.manifest && options.plan,
      "Qualification requires exact-run release manifest and CI plan",
    );
    NodeAssert.equal(report.source_tree_dirty, false, "Qualification requires a clean source tree");
  }
  const sourcePaths = [
    "tools/performance-persistence.mjs",
    "tools/performance-worker.mjs",
    "tools/performance-statistics.mjs",
    "tools/performance-contract.json",
    "tools/performance-baseline.mjs",
    "tools/performance-memory.mjs",
    "tools/performance-runtime.mjs",
    "apps/server/src/persistence/Omdb.ts",
    "apps/server/src/orchestration-v2/NativeProjectionStore.ts",
    "apps/server/src/orchestration-v2/NativeThreadSearch.ts",
    "apps/server/src/persistence/omdb-schema.json",
    "crates/storage/src/backend.rs",
  ];
  for (const path of sourcePaths)
    report.sources[path] = hash(await NodeFSP.readFile(NodePath.join(root, path)));
  fixture = await createFixture();
  report.baseline = await createPinnedBaseline(
    root,
    upstream.commit,
    upstream.root_tree,
    NodePath.join(fixture.root, "upstream"),
    { qualify: mode === "qualify" },
  );
  const baselineModule = async (path) => ({
    module: await import(NodeURL.pathToFileURL(NodePath.join(report.baseline.root, path)).href),
    source: {
      path,
      sha256: hash(await NodeFSP.readFile(NodePath.join(report.baseline.root, path))),
    },
  });
  const projection = await baselineModule("apps/server/src/orchestration-v2/ProjectionStore.ts");
  const project = await baselineModule("apps/server/src/orchestration-v2/ProjectStore.ts");
  const sqlite = await baselineModule("apps/server/src/persistence/Sqlite.ts");
  const source = NodePath.join(fixture.root, "statev2.sqlite");
  await Effect.runPromise(
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* seed(projection.module, project.module)(sql);
      yield* sql`PRAGMA wal_checkpoint(TRUNCATE)`;
    }).pipe(
      Effect.provide(sqlite.module.layerFromPath(source)),
      Effect.scoped,
      Effect.provide(NodeServices.layer),
      Effect.provide(Logger.layer([])),
    ),
  );
  const sourceHash = hash(await NodeFSP.readFile(source));
  const exported = NodePath.join(fixture.root, "source.ndjson");
  exportSqliteState({ source, destination: exported });
  await Effect.runPromise(
    Omdb.make({ executable: options.database, databasePath: fixture.databasePath }).pipe(
      Effect.provideService(HostProcess.HomeDirectory, fixture.root),
      Effect.provideService(HostProcess.Environment, {}),
      Effect.scoped,
      Effect.provide(NodeServices.layer),
      Effect.provide(PlatformCrypto.layer),
      Effect.provide(Logger.layer([])),
    ),
  );
  report.fixture = {
    sqlite_sha256: sourceHash,
    sqlite_file_bytes: (await NodeFSP.stat(source)).size,
    export_sha256: hash(await NodeFSP.readFile(exported)),
    native_template_sha256: hash(await NodeFSP.readFile(fixture.databasePath)),
    native_file_bytes: (await NodeFSP.stat(fixture.databasePath)).size,
    schema_sha256: report.sources["apps/server/src/persistence/omdb-schema.json"],
    definitions: configuration.fixture,
    seed_sources: [projection.source, project.source],
  };
  await save();
  const prepare = async (backend, name) => {
    const directory = NodePath.join(fixture.root, name);
    await NodeFSP.mkdir(directory);
    const databasePath = NodePath.join(
      directory,
      backend === "native" ? "state.omdb" : "statev2.sqlite",
    );
    await NodeFSP.copyFile(backend === "native" ? fixture.databasePath : source, databasePath);
    if (backend === "native")
      await NodeFSP.copyFile(
        NodePath.join(fixture.root, "server-owner.sqlite"),
        NodePath.join(directory, "server-owner.sqlite"),
      );
    return databasePath;
  };
  const validation = {};
  for (const backend of ["upstream", "native"]) {
    validation[backend] = await runWorker(`validation-${backend}`, {
      backend,
      purpose: "validation",
      baselineRoot: report.baseline.root,
      databasePath: await prepare(backend, `validation-${backend}`),
      executable: options.database,
    });
  }
  NodeAssert.deepEqual(
    validation.native.result,
    validation.upstream.result,
    "Backend result data/order, rollback or committed reopen diverged",
  );
  const expected = Object.fromEntries(
    Object.entries(validation.upstream.result.initial).map(([id, value]) => [
      id,
      hash(JSON.stringify(canonical(value))),
    ]),
  );
  report.correctness = {
    status: "passed",
    checks: [
      "Every initial API result is deeply equal to pinned upstream, including array order.",
      "Same-transaction visit readback sees the write; rollback restores the prior row exactly.",
      "Committed visit survives closing and reopening each production persistence service.",
      "Every warmup and measured response matches the validated immutable result digest.",
      "SQLite production WAL/NORMAL settings are asserted; native Immediate commits use the unchanged selected production backend.",
    ],
    result_digests: expected,
  };
  const orders = phase === "validation" ? [] : balancedOrders(configuration.pairs);
  for (const [pair, order] of orders.entries()) {
    for (const backend of order) {
      const name = `pair-${String(pair).padStart(2, "0")}-${backend}`;
      await runWorker(name, {
        backend,
        purpose: "measurement",
        baselineRoot: report.baseline.root,
        pair,
        order,
        databasePath: await prepare(backend, name),
        executable: options.database,
        expected,
        expected_values: { "visit-commit": validation.upstream.result.initial["visit-commit"] },
        warmups: configuration.warmups,
        samples: configuration.samples,
      });
      console.log(`Completed pair ${pair + 1}/${configuration.pairs}: ${backend}`);
    }
  }
  NodeAssert.equal(
    hash(await NodeFSP.readFile(source)),
    sourceHash,
    "Immutable source fixture was mutated",
  );
  NodeAssert.equal(
    hash(await NodeFSP.readFile(options.database)),
    report.executable_sha256,
    "Native helper changed during measurement",
  );
  if (options.manifest)
    NodeAssert.equal(
      hash(await NodeFSP.readFile(options.manifest)),
      report.native_manifest_sha256,
      "Native release manifest changed during measurement",
    );
  NodeAssert.equal(
    git("rev-parse", "HEAD"),
    report.source_commit,
    "Source commit changed during measurement",
  );
  for (const [path, digest] of Object.entries(report.sources))
    NodeAssert.equal(
      hash(await NodeFSP.readFile(NodePath.join(root, path))),
      digest,
      `Benchmark source changed: ${path}`,
    );
  report.source_tree_dirty_after =
    git("status", "--porcelain", "--untracked-files=normal").length !== 0;
  if (mode === "qualify")
    NodeAssert.equal(
      report.source_tree_dirty_after,
      false,
      "Source tree changed during measurement",
    );
  const measurements = report.runs.filter((run) => run.purpose === "measurement");
  report.scenarios =
    orders.length === 0
      ? null
      : contract.scenarios.map(({ id, phase, metric }, index) => {
          const operation = metric === "p95" ? id.slice(0, -4) : id;
          const values =
            phase === "cold"
              ? (run) => [run.result.cold_open_ms]
              : (run) => run.result.samples[operation];
          const percentile = metric === "p95" ? 0.95 : 0.5;
          const all = (backend) =>
            measurements.filter((run) => run.backend === backend).flatMap(values);
          const pairs = orders.map((order, pair) => {
            const find = (backend) =>
              measurements.find((run) => run.pair === pair && run.backend === backend);
            const upstream_ms = quantile(values(find("upstream")), percentile),
              native_ms = quantile(values(find("native")), percentile);
            return { pair, order, upstream_ms, native_ms, ratio: native_ms / upstream_ms };
          });
          const comparison = comparePairs(pairs, configuration.seed + index);
          return {
            id,
            phase,
            metric,
            ...comparison,
            latency_change_percent: (comparison.ratio - 1) * 100,
            upstream: summarize(all("upstream")),
            native: summarize(all("native")),
            pairs,
          };
        });
  report.status =
    report.scenarios === null
      ? "inconclusive"
      : report.scenarios.some((row) => row.status === "regressed")
        ? "regressed"
        : report.scenarios.every((row) => row.status === "passed")
          ? "passed"
          : "inconclusive";
  report.qualifies = mode === "qualify" && report.status === "passed";
  if (phase === "validation")
    report.decisions.push(
      "This attempt ran fixture and correctness validation only. No speed conclusion is available.",
    );
  report.host.load_after = NodeOS.loadavg();
} catch (error) {
  report.status = "failed";
  report.correctness.status = "failed";
  report.scenarios = null;
  report.qualifies = false;
  report.error = NodeUtil.inspect(error, {
    depth: 8,
    maxArrayLength: 20,
    maxStringLength: 10_000,
    breakLength: 120,
  });
} finally {
  NodeAssert.equal(active, undefined, "Owned worker must finish before fixture cleanup");
  if (fixture) await fixture.close();
  NodeProcess.default.removeListener("SIGTERM", cancel);
  NodeProcess.default.removeListener("SIGINT", cancel);
  await save();
}
console.log(
  JSON.stringify({
    status: report.status,
    qualifies: report.qualifies,
    attempt_id: report.attempt_id,
    report: artifacts.json,
    scenarios: report.scenarios?.map(({ id, ratio, confidence_interval, status }) => ({
      id,
      ratio,
      confidence_interval,
      status,
    })),
  }),
);
NodeProcess.default.exitCode =
  report.status === "failed"
    ? 1
    : mode === "diagnostic" || report.status === "passed"
      ? 0
      : report.status === "regressed"
        ? 1
        : 2;
