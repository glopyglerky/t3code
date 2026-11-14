import * as NodeAssert from "node:assert/strict";
import * as NodeCrypto from "node:crypto";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";
import * as NodeProcess from "node:process";
import * as NodePerfHooks from "node:perf_hooks";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as PlatformCrypto from "@effect/platform-node/NodeCrypto";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Effect from "effect/Effect";
import * as Clock from "effect/Clock";
import * as Cause from "effect/Cause";
import * as DateTime from "effect/DateTime";
import * as Exit from "effect/Exit";
import * as Logger from "effect/Logger";
import * as SqlClient from "effect/sql/SqlClient";
import * as Omdb from "../apps/server/src/persistence/Omdb.ts";
import { RedbClient } from "../apps/server/src/persistence/RedbClient.ts";
import * as NativeProjectionStore from "../apps/server/src/orchestration-v2/NativeProjectionStore.ts";
import * as NativeThreadSearch from "../apps/server/src/orchestration-v2/NativeThreadSearch.ts";
import * as ThreadSearch from "../apps/server/src/orchestration-v2/ThreadSearch.ts";
import { normalized } from "./redb-domain-fixture.mjs";
import { fixedTime, makeEvent } from "./redb-event-sink-fixtures.mjs";

const config = JSON.parse(await NodeFSP.readFile(NodeProcess.argv[2], "utf8"));
const loadBaseline = async (path) => ({
  module: await import(NodeURL.pathToFileURL(NodePath.join(config.baselineRoot, path)).href),
  source: {
    path,
    sha256: NodeCrypto.createHash("sha256")
      .update(await NodeFSP.readFile(NodePath.join(config.baselineRoot, path)))
      .digest("hex"),
  },
});
const projectionOracle = await loadBaseline("apps/server/src/orchestration-v2/ProjectionStore.ts");
const searchOracle = await loadBaseline("apps/server/src/orchestration-v2/ThreadSearch.ts");
const sqliteOracle = await loadBaseline("apps/server/src/persistence/Sqlite.ts");
const started = NodePerfHooks.performance.now();
const clock = Effect.runSync(Clock.Clock);
const fixedClock = Object.assign(Object.create(clock), {
  currentTimeMillis: Effect.succeed(Date.parse("2026-11-14T00:00:00.000Z")),
  currentTimeMillisUnsafe: () => Date.parse("2026-11-14T00:00:00.000Z"),
});
const gc = { count: 0, duration_ms: 0 };
const observer = new NodePerfHooks.PerformanceObserver((list) => {
  for (const entry of list.getEntries()) {
    gc.count++;
    gc.duration_ms += entry.duration;
  }
});
observer.observe({ entryTypes: ["gc"] });

function stable(value) {
  if (Array.isArray(value)) return value.map(stable);
  if (value !== null && typeof value === "object") {
    return Object.fromEntries(
      Object.keys(value)
        .sort()
        .map((key) => [key, stable(value[key])]),
    );
  }
  return value;
}
const fingerprint = (value) =>
  NodeCrypto.createHash("sha256")
    .update(JSON.stringify(stable(normalized(value))))
    .digest("hex");
const isolation = (body) =>
  body.pipe(
    Effect.provideService(HostProcess.HomeDirectory, NodePath.dirname(config.databasePath)),
    Effect.provideService(HostProcess.Environment, {}),
    Effect.provideService(Clock.Clock, fixedClock),
    Effect.scoped,
    Effect.provide(NodeServices.layer),
    Effect.provide(PlatformCrypto.layer),
    Effect.provide(Logger.layer([])),
  );

async function withServices(use) {
  const program =
    config.backend === "native"
      ? Effect.gen(function* () {
          const runtime = yield* Omdb.make({
            executable: config.executable,
            databasePath: config.databasePath,
          });
          return yield* Effect.gen(function* () {
            const store = yield* NativeProjectionStore.make;
            const search = yield* ThreadSearch.ThreadSearch.pipe(
              Effect.provide(NativeThreadSearch.layer),
            );
            return yield* use({ store, search, transaction: runtime.client.withTransaction });
          }).pipe(Effect.provideService(RedbClient, runtime.client));
        })
      : Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          const store = yield* projectionOracle.module.ProjectionStoreV2.pipe(
            Effect.provide(projectionOracle.module.layer),
          );
          const search = yield* searchOracle.module.ThreadSearch.pipe(
            Effect.provide(searchOracle.module.layer),
          );
          const journal = yield* sql`PRAGMA journal_mode`;
          const synchronous = yield* sql`PRAGMA synchronous`;
          NodeAssert.equal(journal[0].journal_mode, "wal");
          NodeAssert.equal(synchronous[0].synchronous, 1);
          return yield* use({ store, search, transaction: (body) => sql.withTransaction(body) });
        }).pipe(Effect.provide(sqliteOracle.module.layerFromPath(config.databasePath)));
  return Effect.runPromise(isolation(program));
}

let writeCount = 0;
let writeTimestamp;
const commitVisit = Effect.fn("Performance.commitVisit")(function* ({ store, transaction }) {
  writeTimestamp = new Date(Date.parse(fixedTime) + ++writeCount * 1000).toISOString();
  return yield* transaction(
    Effect.gen(function* () {
      yield* store.apply(
        makeEvent(
          "thread.visit-recorded",
          { lastVisitedAt: writeTimestamp },
          {
            id: `event:performance:timed-visit:${writeCount}`,
            threadId: "thread:performance:write",
            occurredAt: writeTimestamp,
          },
        ),
      );
      return yield* store.getThread("thread:performance:write");
    }),
  );
});
const operations = [
  [
    "sidebar-shell",
    "Active navigation over all seeded threads",
    ({ store }) => store.getShellSnapshot({ location: "active" }),
  ],
  [
    "history-window",
    "Bounded v2 history from a completed long turn",
    ({ store }) =>
      store.getThreadSnapshotWindow("thread:performance:long", {
        rowLimit: 75,
        userTurnLimit: 10,
        deferWork: true,
      }),
  ],
  [
    "message-search",
    "Indexed message search over every seeded message",
    ({ search }) => search.search({ query: "benchmark needle", limit: 20 }),
  ],
  [
    "accent-search",
    "Upstream Latin accent substring/GLOB search",
    ({ search }) => search.search({ query: "ecole fer", limit: 20 }),
  ],
  ["visit-commit", "Committed partial visit mutation and transactional readback", commitVisit],
];

function verify(id, value) {
  if (id === "visit-commit") {
    NodeAssert.deepEqual(
      normalized(value),
      {
        ...config.expected_values[id],
        lastVisitedAt: normalized(DateTime.makeUnsafe(writeTimestamp)),
      },
      "Committed visit must match the exact expected row for this timestamp",
    );
  } else NodeAssert.equal(fingerprint(value), config.expected[id], `${id}: result differs`);
  return fingerprint(value);
}

const collect = Effect.fn("Performance.collect")(function* (services) {
  const results = {};
  for (const [id, , operation] of operations) results[id] = normalized(yield* operation(services));
  return results;
});

function cardinality(results) {
  const shell = results["sidebar-shell"];
  NodeAssert.equal(shell.threads.length, 224);
  NodeAssert.equal(shell.archivedThreads.length, 0);
  const history = results["history-window"].projection;
  NodeAssert.equal(history.thread.id, "thread:performance:long");
  NodeAssert.ok(history.turnItems.length > 0 && history.turnItems.length <= 75);
  NodeAssert.ok(history.runs.length > 0 && history.nodes.length > 0);
  NodeAssert.ok(Buffer.byteLength(JSON.stringify(history)) <= 1_100_000);
  for (const id of ["message-search", "accent-search"]) {
    NodeAssert.ok(
      results[id].matches.length > 0 && results[id].matches.length <= 20,
      `${id} must return bounded nonempty results`,
    );
  }
  NodeAssert.equal(results["visit-commit"].id, "thread:performance:write");
  return {
    active_threads: shell.threads.length,
    history_items: history.turnItems.length,
    history_runs: history.runs.length,
    history_nodes: history.nodes.length,
    history_json_bytes: Buffer.byteLength(JSON.stringify(history)),
    message_matches: results["message-search"].matches.length,
    accent_matches: results["accent-search"].matches.length,
  };
}

const visit = (at) =>
  makeEvent(
    "thread.visit-recorded",
    { lastVisitedAt: at },
    {
      id: `event:performance:visit:${at}`,
      threadId: "thread:performance:long",
      occurredAt: at,
    },
  );
const committedAt = "2026-11-11T12:00:00.000Z";
let result;
if (config.purpose === "validation") {
  result = await withServices(
    Effect.fn("Performance.validate")(function* (services) {
      const initial = yield* collect(services);
      const cardinalityWitness = cardinality(initial);
      const original = normalized(yield* services.store.getThread("thread:performance:long"));
      const rollback = yield* Effect.exit(
        services.transaction(
          Effect.gen(function* () {
            yield* services.store.apply(visit("2026-11-12T12:00:00.000Z"));
            const inside = yield* services.store.getThread("thread:performance:long");
            NodeAssert.equal(DateTime.formatIso(inside.lastVisitedAt), "2026-11-12T12:00:00.000Z");
            return yield* Effect.fail("deliberate benchmark rollback");
          }),
        ),
      );
      NodeAssert.ok(Exit.isFailure(rollback));
      NodeAssert.equal(
        Cause.squash(rollback.cause),
        "deliberate benchmark rollback",
        "Unexpected write/readback failures cannot count as successful rollback evidence",
      );
      NodeAssert.deepEqual(
        normalized(yield* services.store.getThread("thread:performance:long")),
        original,
      );
      yield* services.transaction(services.store.apply(visit(committedAt)));
      const saved = yield* services.store.getThread("thread:performance:long");
      NodeAssert.equal(DateTime.formatIso(saved.lastVisitedAt), committedAt);
      NodeAssert.equal(DateTime.formatIso(saved.updatedAt), fixedTime);
      const committed = normalized(saved);
      return { initial, committed, rollback: "passed", cardinality: cardinalityWitness };
    }),
  );
  result.reopened = await withServices(
    Effect.fn("Performance.verifyReopen")(function* ({ store }) {
      const reopened = normalized(yield* store.getThread("thread:performance:long"));
      NodeAssert.deepEqual(reopened, result.committed);
      return reopened;
    }),
  );
} else {
  NodeAssert.equal(config.purpose, "measurement");
  const opening = NodePerfHooks.performance.now();
  result = await withServices(
    Effect.fn("Performance.measure")(function* (services) {
      const serviceOpenMs = NodePerfHooks.performance.now() - opening;
      const samples = Object.fromEntries(operations.map(([id]) => [id, []]));
      const digests = {};
      const cold = {};
      for (const [id, , operation] of operations) {
        const before = NodePerfHooks.performance.now();
        const value = yield* operation(services);
        cold[id] = NodePerfHooks.performance.now() - before;
        digests[id] = verify(id, value);
        NodeAssert.equal(digests[id], config.expected[id], `${id}: first result differs`);
      }
      const coldOpenMs =
        serviceOpenMs + Object.values(cold).reduce((sum, duration) => sum + duration, 0);
      for (let index = 0; index < config.warmups; index++) {
        for (const [id, , operation] of operations) {
          verify(id, yield* operation(services));
        }
      }
      for (let index = 0; index < config.samples; index++) {
        // Rotate workloads, while keeping the exact same order for both backends.
        for (let offset = 0; offset < operations.length; offset++) {
          const [id, , operation] = operations[(index + offset) % operations.length];
          const before = NodePerfHooks.performance.now();
          const value = yield* operation(services);
          samples[id].push(NodePerfHooks.performance.now() - before);
          verify(id, value);
        }
      }
      return {
        cold_open_ms: coldOpenMs,
        service_open_ms: serviceOpenMs,
        cold_first_operations_ms: cold,
        samples,
        digests,
        committed_writes: writeCount,
      };
    }),
  );
}
await new Promise((resolve) => setImmediate(resolve));
observer.disconnect();
console.log(
  JSON.stringify({
    backend: config.backend,
    purpose: config.purpose,
    result,
    worker_elapsed_ms: NodePerfHooks.performance.now() - started,
    node_peak_rss_kib: NodeProcess.resourceUsage().maxRSS,
    gc,
    oracles: [projectionOracle.source, searchOracle.source, sqliteOracle.source],
    timing_scope:
      "Service acquisition and production API calls; native IPC/serialization included; common result fingerprinting excluded from each API sample.",
  }),
);
