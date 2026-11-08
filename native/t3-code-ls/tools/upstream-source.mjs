import * as NodeAssert from "node:assert/strict";
import * as NodeChildProcess from "node:child_process";
import * as NodeFS from "node:fs";
import * as NodeURL from "node:url";

export const repoRoot = NodeURL.fileURLToPath(new URL("../../../", import.meta.url));

export function upstreamSource(path) {
  NodeAssert.match(path, /^[a-zA-Z0-9_./-]+\.tsx?$/);
  NodeAssert.ok(!path.startsWith("/") && !path.split("/").includes(".."));
  const lock = JSON.parse(
    NodeFS.readFileSync(`${repoRoot}/native/t3-code-ls/upstream.lock.json`, "utf8"),
  );
  NodeAssert.equal(lock.upstream.repository, "pingdotgg/t3code");
  const { commit, root_tree: tree } = lock.upstream;
  NodeAssert.match(commit, /^[a-f0-9]{40}$/);
  NodeAssert.match(tree, /^[a-f0-9]{40}$/);
  const git = (...args) =>
    NodeChildProcess.execFileSync("git", ["--no-replace-objects", "-C", repoRoot, ...args], {
      encoding: "utf8",
      maxBuffer: 16 * 1024 * 1024,
      timeout: 30_000,
    });
  NodeAssert.equal(git("cat-file", "-t", commit).trim(), "commit");
  NodeAssert.equal(git("rev-parse", `${commit}^{tree}`).trim(), tree);
  return git("cat-file", "blob", `${commit}:${path}`);
}
