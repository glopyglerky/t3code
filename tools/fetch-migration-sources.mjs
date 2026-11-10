#!/usr/bin/env node
/* oxlint-disable t3code/no-global-process-runtime -- This standalone tool fetches immutable source objects for hosted qualification. */
import * as NodeChildProcess from "node:child_process";
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";

const initialUpstream = "521ac7c218b3faa74281fb72fc36ce74a0951fb3";
const recoveryCommit = "d29a94f3a175eac1f26c548023fedd8695cedfaf";
const upstreamRemote = "https://github.com/pingdotgg/t3code.git";

function assert(condition, message) {
  if (!condition) throw new Error(message);
}

export function planMigrationFetches(map, lock) {
  assert(map?.schema_version === 1 && lock?.schema_version === 1, "Unsupported migration source schema");
  assert(map.recovery_commit === recoveryCommit, "Migration source recovery commit must remain immutable");
  assert(lock.upstream?.repository === "pingdotgg/t3code", "Unexpected upstream source repository");
  const history = map.source_history;
  assert(Array.isArray(history) && history.length > 0, "Migration source history must be nonempty");
  const seen = new Set();
  for (const commit of history) {
    assert(typeof commit === "string" && commit.length === 40 && /^[0-9a-f]{40}$/.test(commit),
      "Migration history requires complete lowercase commit SHAs");
    assert(!seen.has(commit), "Migration source history contains duplicate commits");
    seen.add(commit);
  }
  assert(history[0] === initialUpstream, "Migration source history must start at the initial upstream baseline");
  const integrated = history.at(-1);
  assert(integrated === map.integrated_upstream_commit && integrated === lock.upstream.commit,
    "Migration source history must end at the locked integrated upstream commit");

  return [
    ...history.map((commit) => ({ remote: upstreamRemote, commit })),
    { remote: "origin", commit: recoveryCommit },
  ];
}

if (process.argv[1] && NodeURL.pathToFileURL(NodePath.resolve(process.argv[1])).href === import.meta.url) {
  try {
    const repository = NodePath.resolve(NodePath.dirname(NodeURL.fileURLToPath(import.meta.url)), "..");
    const map = JSON.parse(NodeFS.readFileSync(NodePath.join(repository, "port-map.json"), "utf8"));
    const lock = JSON.parse(NodeFS.readFileSync(NodePath.join(repository, "upstream.lock.json"), "utf8"));
    const plan = planMigrationFetches(map, lock);
    for (const { remote, commit } of plan) {
      NodeChildProcess.execFileSync("git", [
        "fetch", "--no-tags", "--no-write-fetch-head", "--depth=1", remote, commit,
      ], { cwd: repository, stdio: "inherit", env: { ...process.env, GIT_TERMINAL_PROMPT: "0" } });
    }
    console.log(`Fetched ${plan.length - 1} immutable upstream revisions and the recovery source.`);
  } catch (error) {
    console.error(`Migration source fetch failed: ${error.message}`);
    process.exitCode = 1;
  }
}
