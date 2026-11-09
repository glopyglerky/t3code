import * as NodeChildProcess from "node:child_process";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import { referenceRepos } from "./lib/reference-repos.ts";
import { applyReferenceRepoSync, planReferenceRepoSync } from "./sync-reference-repos.ts";

it.live("restores a verified cache without staging source or replacing an existing cache", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const root = yield* fs.makeTempDirectoryScoped({ prefix: "reference-cache-test-" });
    const source = path.join(root, "upstream");
    const workspace = path.join(root, "workspace");
    const git = (cwd: string, ...args: string[]) =>
      NodeChildProcess.execFileSync("git", args, { cwd, encoding: "utf8" }).trim();
    for (const directory of [source, workspace]) {
      yield* fs.makeDirectory(directory);
      git(directory, "init", "--quiet");
      yield* fs.writeFileString(path.join(directory, ".gitignore"), ".repos/\n");
      yield* fs.writeFileString(path.join(directory, "LICENSE"), "fixture license");
      git(directory, "add", ".");
      git(
        directory,
        "-c",
        "user.name=Fixture",
        "-c",
        "user.email=fixture@example.invalid",
        "commit",
        "--quiet",
        "-m",
        "fixture",
      );
    }
    const repo = {
      ...referenceRepos[0]!,
      repository: source,
      commit: git(source, "rev-parse", "HEAD"),
      tree: git(source, "rev-parse", "HEAD^{tree}"),
    };
    const plan = yield* planReferenceRepoSync(repo, workspace, false);
    yield* applyReferenceRepoSync(workspace, plan);
    assert.equal(git(workspace, "status", "--porcelain"), "");
    assert.equal(git(path.join(workspace, repo.prefix), "rev-parse", "HEAD"), repo.commit);
    assert.equal(
      yield* fs.readFileString(path.join(workspace, repo.prefix, "LICENSE")),
      "fixture license",
    );
    assert.isTrue(
      (yield* Effect.result(applyReferenceRepoSync(workspace, plan)))._tag === "Failure",
    );
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);
it.effect("restores immutable commits instead of deriving mutable tags from dependencies", () =>
  Effect.gen(function* () {
    for (const repo of referenceRepos) {
      const plan = yield* planReferenceRepoSync(repo, "/unused", false);
      assert.equal(plan.ref, repo.commit);
      assert.include(plan.cloneArgs, "--no-checkout");
      assert.notInclude(plan.cloneArgs, `--branch=${repo.pinnedRef}`);
      assert.equal((yield* planReferenceRepoSync(repo, "/unused", true)).ref, repo.latestRef);
    }
  }),
);
