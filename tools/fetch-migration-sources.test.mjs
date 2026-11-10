import * as NodeTest from "node:test";
import * as NodeAssert from "node:assert/strict";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeChildProcess from "node:child_process";
import { planMigrationFetches } from "./fetch-migration-sources.mjs";

const initial = "521ac7c218b3faa74281fb72fc36ce74a0951fb3";
const recovery = "d29a94f3a175eac1f26c548023fedd8695cedfaf";
const integrated = "b".repeat(40);
const detected = "c".repeat(40);

function fixture(history = [initial, integrated]) {
  return {
    map: {
      schema_version: 1, recovery_commit: recovery,
      source_history: history, integrated_upstream_commit: integrated,
    },
    lock: {
      schema_version: 1,
      upstream: { repository: "pingdotgg/t3code", commit: integrated },
      detection: { commit: detected },
    },
  };
}

NodeTest.test("fetch plan contains all integrated history and recovery, never the detected nightly", () => {
  const { map, lock } = fixture();
  const before = JSON.stringify({ map, lock });
  NodeAssert.deepEqual(planMigrationFetches(map, lock), [
    { remote: "https://github.com/pingdotgg/t3code.git", commit: initial },
    { remote: "https://github.com/pingdotgg/t3code.git", commit: integrated },
    { remote: "origin", commit: recovery },
  ]);
  NodeAssert.equal(JSON.stringify({ map, lock }), before);
});

NodeTest.test("the initial single-revision history remains valid", () => {
  const { map, lock } = fixture([initial]);
  map.integrated_upstream_commit = initial;
  lock.upstream.commit = initial;
  NodeAssert.equal(planMigrationFetches(map, lock).length, 2);
});

NodeTest.test("malformed, moving, duplicate and incomplete source histories fail", () => {
  for (const history of [
    null, {}, initial, [], [initial, "main"], [initial, "b".repeat(39)],
    [initial, "B".repeat(40)], [initial, integrated + "\n"], [initial, null], [initial, 123],
    [initial, integrated, integrated], [integrated], [initial, "--all"],
  ]) {
    const { map, lock } = fixture(history);
    NodeAssert.throws(() => planMigrationFetches(map, lock), /Migration/);
  }
});

NodeTest.test("final history revision must match both integration records", () => {
  for (const mismatch of ["map", "lock"]) {
    const { map, lock } = fixture();
    if (mismatch === "map") map.integrated_upstream_commit = detected;
    else lock.upstream.commit = detected;
    NodeAssert.throws(() => planMigrationFetches(map, lock), /end at the locked integrated/);
  }
});

NodeTest.test("recovery identity, source repository and schemas cannot be redirected", () => {
  for (const modify of [
    (map) => { map.recovery_commit = integrated; },
    (map) => { map.schema_version = 2; },
    (_map, lock) => { lock.schema_version = 2; },
    (_map, lock) => { lock.upstream.repository = "another/repo"; },
  ]) {
    const { map, lock } = fixture();
    modify(map, lock);
    NodeAssert.throws(() => planMigrationFetches(map, lock));
  }
});

NodeTest.test("the CLI validates the entire plan before issuing immutable git fetches", () => {
  const directory = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-migration-fetch-"));
  try {
    const toolDirectory = NodePath.join(directory, "tools");
    const bin = NodePath.join(directory, "bin");
    NodeFS.mkdirSync(toolDirectory);
    NodeFS.mkdirSync(bin);
    const script = NodePath.join(toolDirectory, "fetch-migration-sources.mjs");
    NodeFS.copyFileSync(new URL("./fetch-migration-sources.mjs", import.meta.url), script);
    const log = NodePath.join(directory, "fetches.jsonl");
    NodeFS.writeFileSync(NodePath.join(bin, "git"),
      `#!${process.execPath}\nrequire("node:fs").appendFileSync(process.env.T3_FETCH_TEST_LOG, JSON.stringify(process.argv.slice(2)) + "\\n");\n`,
      { mode: 0o755 });
    const { map, lock } = fixture();
    const mapPath = NodePath.join(directory, "port-map.json");
    NodeFS.writeFileSync(NodePath.join(directory, "upstream.lock.json"), JSON.stringify(lock));
    NodeFS.writeFileSync(mapPath, JSON.stringify({ ...map, source_history: [initial, integrated, "main"] }));
    const run = () => NodeChildProcess.spawnSync(process.execPath, [script], {
      encoding: "utf8",
      env: { ...process.env, PATH: bin + NodePath.delimiter + process.env.PATH, T3_FETCH_TEST_LOG: log },
    });
    const invalid = run();
    NodeAssert.notEqual(invalid.status, 0);
    NodeAssert.match(invalid.stderr, /complete lowercase commit SHAs/);
    NodeAssert.equal(NodeFS.existsSync(log), false, "an invalid later entry must prevent every fetch");

    NodeFS.writeFileSync(mapPath, JSON.stringify(map));
    const valid = run();
    NodeAssert.equal(valid.status, 0, valid.stderr);
    const calls = NodeFS.readFileSync(log, "utf8").trim().split("\n").map((line) => JSON.parse(line));
    NodeAssert.deepEqual(calls, [
      ["fetch", "--no-tags", "--no-write-fetch-head", "--depth=1", "https://github.com/pingdotgg/t3code.git", initial],
      ["fetch", "--no-tags", "--no-write-fetch-head", "--depth=1", "https://github.com/pingdotgg/t3code.git", integrated],
      ["fetch", "--no-tags", "--no-write-fetch-head", "--depth=1", "origin", recovery],
    ]);
  } finally {
    NodeFS.rmSync(directory, { recursive: true, force: true });
  }
});
