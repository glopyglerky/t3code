#!/usr/bin/env node
import * as NodeAssert from "node:assert/strict";
import * as NodeChildProcess from "node:child_process";
import * as NodeCrypto from "node:crypto";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeProcess from "node:process";
import * as NodeURL from "node:url";
import { createFixture } from "./database-fixture.mjs";

// This is a one-time compatibility proof with a retained, identified old
// executable. It is not an optional branch of the ordinary acceptance gate.
const args = NodeProcess.argv.slice(2);
NodeAssert.equal(args.length, 2, "Usage: check-redb-count-writer-compat.mjs NEW_HELPER OLD_HELPER");
const [newExecutable, oldExecutable] = args;
for (const executable of args) {
  NodeAssert.ok(NodePath.isAbsolute(executable), "Both helper paths must be absolute");
  await NodeFSP.access(executable, NodeFSP.constants.X_OK);
}
NodeAssert.notEqual(await NodeFSP.realpath(newExecutable), await NodeFSP.realpath(oldExecutable));

const root = NodeURL.fileURLToPath(new URL("../", import.meta.url));
const digest = (value) => NodeCrypto.createHash("sha256").update(value).digest("hex");
const git = (...gitArgs) =>
  NodeChildProcess.execFileSync("git", ["--no-replace-objects", "-C", root, ...gitArgs], {
    maxBuffer: 16 * 1024 * 1024,
    timeout: 30_000,
    killSignal: "SIGKILL",
  });
const gitText = (...gitArgs) =>
  git(...gitArgs)
    .toString("utf8")
    .trim();
const oldIdentity = {
  source_commit: "d661dbb2bf693b88c8969fcda9a0f5b98e3a2cca",
  source_tree: "549bff502c033739693b562a854437c0292a5019",
  executable_sha256: "1d04aae57b316d02eba6452c04b81bc616ab6d554f9686c76e973aa58068b60c",
  upstream_commit: "913d7fc45ec5814fff25c819cfe4de193ea42d63",
  expected_compiler_route: "bluefe-rustc-via-lustr",
  compiler_route: null,
  compiler_provenance: "unverified",
  build_profile: "release",
};
NodeAssert.equal(
  gitText("rev-parse", `${oldIdentity.source_commit}^{tree}`),
  oldIdentity.source_tree,
);
const oldHash = digest(await NodeFSP.readFile(oldExecutable));
const newHash = digest(await NodeFSP.readFile(newExecutable));
NodeAssert.equal(
  oldHash,
  oldIdentity.executable_sha256,
  "Use the exact preserved d661 release helper",
);
NodeAssert.notEqual(newHash, oldHash, "The new helper must be a distinct build");

const fenceSources = [
  "Cargo.toml",
  "Cargo.lock",
  "toolchain.lock.json",
  "upstream.lock.json",
  "crates/storage/Cargo.toml",
  "crates/storage/src/lib.rs",
  "crates/storage/src/backend.rs",
  "crates/storage/src/bin/t3-code-ls-db.rs",
  "apps/server/src/persistence/omdb-schema.json",
  "apps/server/src/persistence/redb_bootstrap.rs",
  "apps/server/src/persistence/redb_schema_upgrade.rs",
  "apps/server/src/persistence/redb_transactions.rs",
  "apps/server/src/persistence/redb_application_rows.rs",
  "apps/server/src/persistence/redb_projection_commit.rs",
  "apps/server/src/persistence/redb_projection_read_index.rs",
  "apps/server/src/persistence/redb_projection_shell.rs",
  "apps/server/src/persistence/redb_turn_item_counts.rs",
];
const currentSources = [
  ...fenceSources,
  "tools/database-fixture.mjs",
  "tools/check-redb-count-writer-compat.mjs",
];
async function sourceHashes(paths) {
  return Object.fromEntries(
    await Promise.all(
      paths.map(async (path) => [path, digest(await NodeFSP.readFile(NodePath.join(root, path)))]),
    ),
  );
}
const beforeSources = await sourceHashes(currentSources);
const oldSources = Object.fromEntries(
  fenceSources.map((path) => [
    path,
    digest(git("cat-file", "blob", `${oldIdentity.source_commit}:${path}`)),
  ]),
);
const upstream = JSON.parse(
  await NodeFSP.readFile(NodePath.join(root, "upstream.lock.json"), "utf8"),
).upstream;
const proof = {
  schema: 1,
  suite: "count-owner-old-writer-compatibility",
  scope:
    "One-time protocol compatibility proof against the exact retained d661 binary; no application or performance qualification",
  status: "running",
  qualifies: false,
  started_at: new Date().toISOString(),
  command: [NodeProcess.execPath, ...NodeProcess.argv.slice(1)],
  old: { ...oldIdentity, executable: oldExecutable, sources: oldSources },
  new: {
    executable: newExecutable,
    executable_sha256: newHash,
    reference_source_commit: gitText("rev-parse", "HEAD"),
    reference_source_tree: gitText("rev-parse", "HEAD^{tree}"),
    source_tree_dirty: gitText("status", "--porcelain", "--untracked-files=normal").length !== 0,
    upstream_commit: upstream.commit,
    sources: beforeSources,
    source_scope:
      "Fence implementation, schema, helper entrypoint, transaction/backend and lock files. These hashes identify the inspected source; this proof alone is not a binary build-origin attestation.",
  },
  limits: {
    total_timeout_ms: 180_000,
    helper_timeout_ms: 30_000,
    helper_output_bytes: 4 * 1024 * 1024,
    preparation_batches: 128,
  },
  checks: [],
  calls: [],
  states: [],
};

const fixture = await createFixture();
const deadline = Date.now() + proof.limits.total_timeout_ms;
const THREADS = "orchestration_v2_projection_threads";
const ITEMS = "orchestration_v2_projection_turn_items";
const COUNTS = "orchestration_v2_projection_turn_item_counts";
const threadId = "thread:count-writer-compat";
const initialItemId = "item:count-writer-compat:initial";
const blockedItemId = "item:count-writer-compat:blocked";
const newItemId = "item:count-writer-compat:new";
const blockedRunId = "run:count-writer-compat:blocked";
const fixedTime = "2026-11-14T00:00:00.000Z";
const text = (value) => ({ type: "text", value });
const integer = (value) => ({ type: "integer", value: String(value) });
const countRow = (count) => ({
  thread_id: text(threadId),
  run_key: text(""),
  item_count: integer(count),
});
const thread = {
  id: threadId,
  projectId: "project:count-writer-compat",
  title: "Count ownership compatibility fixture",
  createdBy: "user",
  creationSource: "web",
  providerInstanceId: "codex",
  modelSelection: { instanceId: "codex", model: "fixture-model" },
  runtimeMode: "full-access",
  interactionMode: "default",
  branch: null,
  worktreePath: null,
  linkedPullRequest: null,
  pullRequests: [],
  branchPullRequest: null,
  activeProviderThreadId: null,
  lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
  forkedFrom: null,
  createdAt: fixedTime,
  updatedAt: fixedTime,
  archivedAt: null,
  settledAt: null,
  lastVisitedAt: null,
  deletedAt: null,
};
const item = (id, runId = null) => ({
  id,
  threadId,
  runId,
  nodeId: null,
  providerThreadId: null,
  providerTurnId: null,
  nativeItemRef: null,
  parentItemId: null,
  ordinal: id === initialItemId ? 1 : 2,
  type: "assistant_message",
  status: "completed",
  title: null,
  startedAt: fixedTime,
  completedAt: fixedTime,
  updatedAt: fixedTime,
  text: "Synthetic compatibility fixture",
  streaming: false,
});
const event = (type, payload) => ({
  event_type: type,
  thread_id: threadId,
  occurred_at: fixedTime,
  payload_json: JSON.stringify(payload),
});
proof.fixture_sha256 = digest(
  JSON.stringify({
    thread,
    initial: item(initialItemId),
    blocked: item(blockedItemId),
    moved: item(initialItemId, blockedRunId),
    next: item(newItemId),
  }),
);

function invoke(implementation, phase, requests, expected = [], mode = "connect") {
  const timeout = Math.min(proof.limits.helper_timeout_ms, deadline - Date.now());
  NodeAssert.ok(timeout > 0, "Compatibility proof exceeded its total deadline");
  const executable = implementation === "old" ? oldExecutable : newExecutable;
  const frames = [...requests, { operation: "shutdown" }];
  const result = NodeChildProcess.spawnSync(executable, fixture.cliArgs(mode), {
    input: `${frames.map((frame) => JSON.stringify(frame)).join("\n")}\n`,
    env: {},
    encoding: "utf8",
    timeout,
    killSignal: "SIGKILL",
    maxBuffer: proof.limits.helper_output_bytes,
  });
  NodeAssert.equal(result.error, undefined, `${phase}: helper failed or exceeded its bound`);
  NodeAssert.equal(result.signal, null, `${phase}: helper received a signal`);
  NodeAssert.equal(result.status, 0, `${phase}: helper exited unsuccessfully`);
  NodeAssert.equal(result.stderr, "", `${phase}: helper emitted stderr`);
  const responses = result.stdout
    .trim()
    .split("\n")
    .map((line) => JSON.parse(line));
  NodeAssert.equal(responses.length, frames.length, `${phase}: response count differs`);
  proof.calls.push({
    phase,
    implementation,
    responses: responses.map((response, index) => ({
      operation: frames[index].operation,
      ok: response.ok,
      error_kind: response.ok ? null : response.error?.kind,
    })),
  });
  for (const [index, response] of responses.entries()) {
    const kind = expected[index];
    NodeAssert.equal(response.ok, kind === undefined, `${phase}: unexpected response at ${index}`);
    if (kind !== undefined)
      NodeAssert.equal(response.error?.kind, kind, `${phase}: wrong rejection at ${index}`);
  }
  NodeAssert.equal(responses.at(-1).result, null, `${phase}: shutdown must be acknowledged`);
  return responses.slice(0, -1).map((response) => response.result);
}

function prepare(implementation, operation, phase) {
  for (let batch = 0; batch < proof.limits.preparation_batches; batch++) {
    const [result] = invoke(implementation, `${phase}:${batch}`, [{ operation }]);
    NodeAssert.equal(typeof result.prepared, "boolean");
    NodeAssert.ok(Number.isSafeInteger(result.indexed_rows) && result.indexed_rows >= 0);
    if (result.prepared) return result;
  }
  NodeAssert.fail(`${phase}: preparation exceeded the bounded batch count`);
}

function apply(implementation, phase, events) {
  const transaction_id = "compat-write";
  const requests = [
    { operation: "application_begin", transaction_id, mode: "write" },
    ...events.map((value) => ({
      operation: "application_projections_apply",
      transaction_id,
      event: value,
    })),
    { operation: "application_commit", transaction_id },
  ];
  const result = invoke(implementation, phase, requests);
  NodeAssert.deepEqual(result.at(-1), { committed: true });
}

function snapshot(implementation, phase, includeIndex) {
  const transaction_id = "compat-read";
  const requests = [
    { operation: "application_begin", transaction_id, mode: "read" },
    { operation: "application_get", transaction_id, table: THREADS, key: [text(threadId)] },
    ...[initialItemId, blockedItemId, newItemId].map((id) => ({
      operation: "application_get",
      transaction_id,
      table: ITEMS,
      key: [text(id)],
    })),
    ...["", blockedRunId].map((run) => ({
      operation: "application_get",
      transaction_id,
      table: COUNTS,
      key: [text(threadId), text(run)],
    })),
    ...(includeIndex
      ? [
          {
            operation: "application_projections_shell",
            transaction_id,
            query: { kind: "item_counts", thread_ids: [threadId] },
          },
          {
            operation: "application_projections_shell",
            transaction_id,
            query: { kind: "run_item_counts", thread_ids: [threadId] },
          },
        ]
      : []),
    { operation: "application_rollback", transaction_id },
  ];
  const result = invoke(implementation, phase, requests);
  NodeAssert.deepEqual(result.at(-1), { rolled_back: true });
  const canonical = result.slice(1, 7);
  const index = includeIndex ? result.slice(7, 9) : null;
  proof.states.push({
    phase,
    implementation,
    canonical_sha256: digest(JSON.stringify(canonical)),
    index_sha256: index === null ? null : digest(JSON.stringify(index)),
  });
  return { canonical, index };
}

function assertState(state, count, includeNew) {
  NodeAssert.equal(state.canonical[0].thread_id.value, threadId);
  NodeAssert.equal(state.canonical[1].turn_item_id.value, initialItemId);
  NodeAssert.equal(state.canonical[2], null, "Rejected old insert must stay absent");
  if (includeNew) NodeAssert.equal(state.canonical[3].turn_item_id.value, newItemId);
  else NodeAssert.equal(state.canonical[3], null);
  NodeAssert.deepEqual(state.canonical[4], countRow(count));
  NodeAssert.equal(state.canonical[5], null, "Rejected old move must not create a count group");
  if (state.index !== null)
    NodeAssert.deepEqual(state.index, [
      [{ thread_id: threadId, item_count: count, runless_item_count: count }],
      [],
    ]);
}

function rejectOld(phase, mutation) {
  const transaction_id = "compat-rejected";
  const result = invoke(
    "old",
    phase,
    [
      { operation: "application_begin", transaction_id, mode: "write" },
      { operation: "application_projections_apply", transaction_id, event: mutation },
      { operation: "application_commit", transaction_id },
    ],
    [undefined, "incompatible_store", "invalid_request"],
  );
  NodeAssert.deepEqual(result[0], { transaction_id });
}

try {
  const [bootstrapped] = invoke(
    "old",
    "old-bootstrap",
    [{ operation: "application_bootstrap" }],
    [],
    "initialize",
  );
  NodeAssert.equal(bootstrapped.initialized, true);
  NodeAssert.equal(bootstrapped.migrations.length, 91);
  NodeAssert.equal(bootstrapped.migrations.at(-1).id, "91");
  prepare("old", "application_projections_prepare", "old-projection-prepare");
  prepare("old", "application_projection_reads_prepare", "old-read-prepare");
  apply("old", "old-seed", [
    event("thread.created", thread),
    event("turn-item.updated", item(initialItemId)),
  ]);
  const original = snapshot("old", "old-original", false);
  assertState(original, 1, false);
  proof.checks.push(
    "The exact old schema91 helper initializes, prepares, commits a real turn item, and reads canonical count1 before the transition.",
  );

  const [reopened] = invoke("new", "new-bootstrap", [{ operation: "application_bootstrap" }]);
  NodeAssert.equal(reopened.initialized, false);
  NodeAssert.deepEqual(reopened.migrations, bootstrapped.migrations);
  prepare("new", "application_projections_prepare", "new-owner-transition");
  // Fence rejection must already hold before the first count backfill page.
  invoke(
    "old",
    "old-prepare-before-count-backfill",
    [{ operation: "application_projections_prepare" }],
    ["incompatible_store"],
  );
  rejectOld("old-insert-before-count-backfill", event("turn-item.updated", item(blockedItemId)));
  const fenced = snapshot("new", "new-before-count-backfill", false);
  NodeAssert.deepEqual(fenced, original);
  proof.checks.push(
    "A fresh old preparation and real item insert reject incompatible_store immediately after the owner transition, before count backfill; commit cannot succeed and canonical rows remain exact.",
  );

  prepare("new", "application_projection_reads_prepare", "new-count-backfill");
  const prepared = snapshot("new", "new-prepared", true);
  assertState(prepared, 1, false);
  NodeAssert.deepEqual(prepared.canonical, original.canonical);
  invoke(
    "old",
    "old-prepare-after-count-backfill",
    [{ operation: "application_projections_prepare" }],
    ["incompatible_store"],
  );
  rejectOld("old-insert-after-count-backfill", event("turn-item.updated", item(blockedItemId)));
  NodeAssert.deepEqual(snapshot("new", "after-rejected-insert", true), prepared);
  rejectOld(
    "old-move-after-count-backfill",
    event("turn-item.updated", item(initialItemId, blockedRunId)),
  );
  NodeAssert.deepEqual(snapshot("new", "after-rejected-move", true), prepared);
  proof.checks.push(
    "After count-index readiness, fresh old preparation, insertion and cross-group update all fail; both canonical rows/counts and the new total/per-run index results remain unchanged.",
  );

  apply("new", "new-write-after-rejections", [event("turn-item.updated", item(newItemId))]);
  const committed = snapshot("new", "new-reopened-after-write", true);
  assertState(committed, 2, true);
  const repeated = snapshot("new", "new-second-reopen", true);
  NodeAssert.deepEqual(repeated, committed);
  proof.checks.push(
    "The new helper remains writable after every rejection; a distinct committed item produces canonical/index count2 and survives two fresh helper reopens.",
  );

  NodeAssert.equal(digest(await NodeFSP.readFile(oldExecutable)), oldHash);
  NodeAssert.equal(digest(await NodeFSP.readFile(newExecutable)), newHash);
  NodeAssert.deepEqual(await sourceHashes(currentSources), beforeSources);
  proof.checks.push(
    "Both executable hashes and all recorded source hashes are unchanged at completion; no whole-file database hash is used as a mutation assertion.",
  );
  proof.status = "passed";
} catch (error) {
  proof.status = "failed";
  proof.failure = { name: error.name, message: error.message };
  NodeProcess.default.exitCode = 1;
} finally {
  await fixture.close();
  proof.fixture_removed = true;
  proof.finished_at = new Date().toISOString();
  console.log(JSON.stringify(proof, null, 2));
}
