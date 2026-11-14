import * as NodeTest from "node:test";
import * as NodeAssert from "node:assert/strict";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";
import * as NodeChildProcess from "node:child_process";
import * as NodeNet from "node:net";

NodeTest.test(
  "bundled and SEA modules resolve executable sidecars, including unpacked ASARs",
  async () => {
    const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-native-path-"));
    try {
      for (const relative of [
        "dist/bin.mjs",
        "sea/t3",
        "app.asar/apps/server/dist/bin.mjs",
        "server.asar/apps/server/dist/bin.mjs",
      ]) {
        const modulePath = NodePath.join(root, relative);
        NodeFS.mkdirSync(NodePath.dirname(modulePath), { recursive: true });
        NodeFS.copyFileSync(
          new URL("../packages/shared/src/nativeExecutable.ts", import.meta.url),
          modulePath + ".ts",
        );
        const { nativeExecutable } = await import(NodeURL.pathToFileURL(modulePath + ".ts").href);
        for (const component of ["policy", "db"]) {
          const expected = NodePath.join(
            NodePath.dirname(modulePath).replace(/\.asar(?=\/)/, ".asar.unpacked"),
            "native-bin",
            `t3-code-ls-${component}`,
          );
          NodeFS.mkdirSync(NodePath.dirname(expected), { recursive: true });
          NodeFS.writeFileSync(
            expected,
            `#!/usr/bin/env node\nconsole.log("native-${component}-fixture")\n`,
            { mode: 0o755 },
          );
          NodeAssert.equal(nativeExecutable(component), expected);
          const child = NodeChildProcess.spawnSync(nativeExecutable(component), [], {
            encoding: "utf8",
          });
          NodeAssert.equal(child.status, 0, child.stderr);
          NodeAssert.equal(child.stdout.trim(), `native-${component}-fixture`);
        }
      }
    } finally {
      NodeFS.rmSync(root, { recursive: true, force: true });
    }
  },
);

NodeTest.test(
  "build helper stages the selected Cargo target and preserves encoded flags; archives reject mismatched or damaged components",
  async () => {
    const { nativeComponents, nativeFile, nativeHash, stageNativeArtifacts } =
      await import("./native-artifacts.mjs");
    const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-native-stage-"));
    try {
      const toolchain = NodePath.join(root, "compiler");
      const target = NodePath.join(root, "cargo");
      const output = NodePath.join(root, "native-bin");
      NodeFS.mkdirSync(NodePath.join(toolchain, "lib/rustlib/x86_64-unknown-linux-gnu/lib"), {
        recursive: true,
      });
      NodeFS.mkdirSync(NodePath.join(toolchain, "bin"));
      NodeFS.writeFileSync(
        NodePath.join(toolchain, "bin/lustr"),
        `#!${process.execPath}
const NodeFS = require('node:fs'), p = require('node:path');
const args = process.argv.slice(2);
const packages = args.flatMap((arg, index) => arg === '-p' ? [args[index + 1]] : []);
const dir = p.join(args[args.indexOf('--target-dir') + 1], args[args.indexOf('--target') + 1], 'release');
NodeFS.mkdirSync(dir, {recursive:true});
for (const name of ${JSON.stringify(nativeComponents)}) {
  if (!packages.includes(name === 'db' ? 't3-code-ls-storage' : 't3-code-ls-core')) continue;
  NodeFS.writeFileSync(p.join(dir, 't3-code-ls-' + name), '#!/usr/bin/env node\\nconsole.log("fixture-component")\\n', {mode:0o755});
}
NodeFS.writeFileSync(p.join(dir, 'flags.json'), JSON.stringify(process.env.CARGO_ENCODED_RUSTFLAGS));
NodeFS.writeFileSync(p.join(dir, 'packages.json'), JSON.stringify(packages));
`,
        { mode: 0o755 },
      );
      const env = {
        ...process.env,
        T3_CODE_LS_TOOLCHAIN: toolchain,
        T3_CODE_LS_NATIVE_TARGET: "linux-x64",
        CARGO_BUILD_TARGET: "x86_64-unknown-linux-gnu",
        CARGO_TARGET_DIR: target,
        CARGO_ENCODED_RUSTFLAGS: "-C\x1fdebuginfo=0",
        GITHUB_RUN_ID: "101",
        GITHUB_RUN_ATTEMPT: "3",
      };
      delete env.T3_CODE_LS_NATIVE_DIR;
      const run = NodeChildProcess.spawnSync(
        process.execPath,
        [new URL("./build-native.mjs", import.meta.url).pathname, output],
        { env, encoding: "utf8" },
      );
      NodeAssert.equal(run.status, 0, run.stderr);
      NodeAssert.deepEqual(
        JSON.parse(
          NodeFS.readFileSync(
            NodePath.join(target, "x86_64-unknown-linux-gnu/release/packages.json"),
          ),
        ),
        ["t3-code-ls-core", "t3-code-ls-storage"],
      );
      const manifest = JSON.parse(NodeFS.readFileSync(NodePath.join(output, "manifest.json")));
      NodeAssert.ok(Object.hasOwn(manifest.files, "t3-code-ls-db"));
      const checkout = NodeURL.fileURLToPath(new URL("..", import.meta.url));
      const git = (...args) =>
        NodeChildProcess.execFileSync("git", args, {
          cwd: checkout,
          encoding: "utf8",
        }).trim();
      NodeAssert.equal(manifest.source_commit, git("rev-parse", "HEAD"));
      NodeAssert.equal(manifest.source_tree, git("rev-parse", "HEAD^{tree}"));
      NodeAssert.equal(
        manifest.source_tree_dirty,
        git("status", "--porcelain", "--untracked-files=normal").length !== 0,
      );
      NodeAssert.equal(manifest.compiler_route, "bluefe-rustc-via-lustr");
      NodeAssert.equal(manifest.build_profile, "release");
      NodeAssert.equal(
        manifest.toolchain_lock_sha256,
        nativeHash(NodePath.join(checkout, "toolchain.lock.json")),
      );
      NodeAssert.equal(manifest.ci_run_id, "101");
      NodeAssert.equal(manifest.ci_run_attempt, "3");
      NodeAssert.equal(
        JSON.parse(
          NodeFS.readFileSync(NodePath.join(target, "x86_64-unknown-linux-gnu/release/flags.json")),
        ),
        `-C\x1fdebuginfo=0\x1f--sysroot\x1f${toolchain}`,
      );
      const inheritedFlags = `-C\x1flink-arg=a b\x1f--sysroot\x1f${toolchain}`;
      const inherited = NodeChildProcess.spawnSync(
        process.execPath,
        [new URL("./build-native.mjs", import.meta.url).pathname, output],
        { env: { ...env, CARGO_ENCODED_RUSTFLAGS: inheritedFlags }, encoding: "utf8" },
      );
      NodeAssert.equal(inherited.status, 0, inherited.stderr);
      NodeAssert.equal(
        JSON.parse(
          NodeFS.readFileSync(NodePath.join(target, "x86_64-unknown-linux-gnu/release/flags.json")),
        ),
        inheritedFlags,
      );
      const conflicting = NodeChildProcess.spawnSync(
        process.execPath,
        [new URL("./build-native.mjs", import.meta.url).pathname, output],
        { env: { ...env, CARGO_ENCODED_RUSTFLAGS: "--sysroot\x1f/another" }, encoding: "utf8" },
      );
      NodeAssert.notEqual(conflicting.status, 0);
      NodeAssert.match(conflicting.stderr, /Conflicting.*sysroot/);
      const stage = NodePath.join(root, "archive/native-bin");
      stageNativeArtifacts(output, stage, "linux-x64");
      const archive = NodePath.join(root, "fixture.tar");
      NodeAssert.equal(
        NodeChildProcess.spawnSync("tar", [
          "-cf",
          archive,
          "-C",
          NodePath.join(root, "archive"),
          ".",
        ]).status,
        0,
      );
      const extracted = NodePath.join(root, "extracted");
      NodeFS.mkdirSync(extracted);
      NodeAssert.equal(
        NodeChildProcess.spawnSync("tar", ["-xf", archive, "-C", extracted]).status,
        0,
      );
      NodeAssert.equal(
        NodeChildProcess.spawnSync(
          NodePath.join(extracted, "native-bin", nativeFile("db", "linux-x64")),
          [],
          { encoding: "utf8" },
        ).stdout.trim(),
        "fixture-component",
      );
      NodeAssert.throws(() => stageNativeArtifacts(output, stage, "win32-x64"), /do not match/);
      const databaseHelper = NodePath.join(output, nativeFile("db", "linux-x64"));
      const databaseBytes = NodeFS.readFileSync(databaseHelper);
      NodeFS.rmSync(databaseHelper);
      NodeAssert.throws(() => stageNativeArtifacts(output, stage, "linux-x64"), /ENOENT/);
      NodeFS.writeFileSync(databaseHelper, databaseBytes, { mode: 0o755 });
      NodeFS.appendFileSync(databaseHelper, "damage");
      NodeAssert.throws(() => stageNativeArtifacts(output, stage, "linux-x64"), /hash mismatch/);
      const cross = NodeChildProcess.spawnSync(
        process.execPath,
        [new URL("./build-native.mjs", import.meta.url).pathname, output],
        {
          env: { ...env, CARGO_BUILD_TARGET: "", T3_CODE_LS_NATIVE_TARGET: "win32-x64" },
          encoding: "utf8",
        },
      );
      NodeAssert.notEqual(cross.status, 0);
      NodeAssert.match(cross.stderr, /not qualified/);
    } finally {
      NodeFS.rmSync(root, { recursive: true, force: true });
    }
  },
);

NodeTest.test(
  "unqualified platforms package retained service implementations and reject native overrides",
  async () => {
    const {
      nativeTargets,
      nativeTargetQualified,
      isQualifiedNativeHost,
      stageNativeArtifacts,
      nativeArtifactFiles,
    } = await import("./native-artifacts.mjs");
    const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-upstream-platform-"));
    try {
      NodeAssert.equal(isQualifiedNativeHost("linux", "x64", "2.38"), false);
      NodeAssert.equal(isQualifiedNativeHost("linux", "x64", ""), false);
      NodeAssert.equal(isQualifiedNativeHost("linux", "x64", "2.39"), true);
      for (const target of Object.keys(nativeTargets)) {
        const output = NodePath.join(root, target);
        const env = { ...process.env, T3_CODE_LS_NATIVE_TARGET: target };
        for (const name of Object.keys(env)) {
          if (
            /^T3_CODE_LS_.*_EXECUTABLE$/.test(name) ||
            ["T3_CODE_LS_TOOLCHAIN", "T3_CODE_LS_SYSROOT", "T3_CODE_LS_NATIVE_DIR"].includes(name)
          )
            delete env[name];
        }
        env.T3_CODE_LS_IMPLEMENTATION = nativeTargetQualified(target) ? "upstream" : undefined;
        const run = (extra = {}) =>
          NodeChildProcess.spawnSync(
            process.execPath,
            [new URL("./build-native.mjs", import.meta.url).pathname, output],
            { env: { ...env, ...extra }, encoding: "utf8" },
          );
        const baseline = run();
        NodeAssert.equal(baseline.status, 0, baseline.stderr);
        const manifest = JSON.parse(
          NodeFS.readFileSync(NodePath.join(output, "manifest.json"), "utf8"),
        );
        NodeAssert.equal(manifest.implementation, "upstream");
        NodeAssert.deepEqual(manifest.files, {});
        NodeAssert.deepEqual(NodeFS.readdirSync(output), ["manifest.json"]);
        stageNativeArtifacts(output, NodePath.join(root, "archive", target), target);
        if (target === "linux-x64" && isQualifiedNativeHost()) {
          const staged = NodePath.join(root, "desktop-staged");
          const stagedEnv = { ...env, T3_CODE_LS_NATIVE_DIR: output };
          delete stagedEnv.T3_CODE_LS_IMPLEMENTATION;
          const bundle = NodeChildProcess.spawnSync(
            process.execPath,
            [new URL("./build-native.mjs", import.meta.url).pathname, staged],
            { env: stagedEnv, encoding: "utf8" },
          );
          NodeAssert.equal(bundle.status, 0, bundle.stderr);
          const readiness = nativeArtifactFiles(staged, target);
          NodeAssert.deepEqual(readiness, ["manifest.json"]);
          const { waitForResources } =
            await import("../apps/desktop/scripts/wait-for-resources.mjs");
          const listener = NodeNet.createServer((socket) => socket.destroy());
          await new Promise((resolve, reject) => {
            listener.once("error", reject);
            listener.listen(0, "127.0.0.1", resolve);
          });
          try {
            await waitForResources({
              baseDir: staged,
              files: readiness,
              tcpHost: "127.0.0.1",
              tcpPort: listener.address().port,
              timeoutMs: 1000,
            });
          } finally {
            await new Promise((resolve, reject) =>
              listener.close((error) => (error ? reject(error) : resolve())),
            );
          }
          const manifestPath = NodePath.join(staged, "manifest.json");
          const wrongPin = JSON.parse(NodeFS.readFileSync(manifestPath, "utf8"));
          wrongPin.upstreamCommit = "0".repeat(40);
          NodeFS.writeFileSync(manifestPath, JSON.stringify(wrongPin));
          NodeAssert.throws(() => nativeArtifactFiles(staged, target), /Invalid upstream-only/);
          const installation = NodePath.join(root, "installed-original");
          const module = NodePath.join(installation, "packages/shared/src/nativeExecutable.ts");
          NodeFS.mkdirSync(NodePath.dirname(module), { recursive: true });
          NodeFS.copyFileSync(
            new URL("../packages/shared/src/nativeExecutable.ts", import.meta.url),
            module,
          );
          stageNativeArtifacts(output, NodePath.join(installation, "bin"), target);
          const installedEnv = { ...env };
          delete installedEnv.T3_CODE_LS_IMPLEMENTATION;
          const probe = NodeChildProcess.spawnSync(
            process.execPath,
            [
              "--input-type=module",
              "-e",
              `import assert from 'node:assert/strict'; import { selectNativeExecutable } from ${JSON.stringify(NodeURL.pathToFileURL(module).href)}; assert.equal(selectNativeExecutable('policy'), undefined); assert.equal(selectNativeExecutable('policy', '/explicit/helper'), '/explicit/helper');`,
            ],
            { env: installedEnv, encoding: "utf8" },
          );
          NodeAssert.equal(probe.status, 0, probe.stderr);
        }
        for (const extra of [
          { T3_CODE_LS_TOOLCHAIN: "/missing" },
          { T3_CODE_LS_POLICY_EXECUTABLE: "/missing" },
          { T3_CODE_LS_NATIVE_DIR: "/missing" },
        ]) {
          NodeAssert.notEqual(run(extra).status, 0);
        }
        if (target !== "linux-x64")
          NodeAssert.throws(
            () => stageNativeArtifacts(output, NodePath.join(root, "wrong"), "linux-x64"),
            /do not match/,
          );
        NodeFS.writeFileSync(NodePath.join(output, "t3-code-ls-policy"), "stale binary");
        NodeAssert.throws(
          () => stageNativeArtifacts(output, NodePath.join(root, "stale"), target),
          /native component/,
        );
      }
    } finally {
      NodeFS.rmSync(root, { recursive: true, force: true });
    }
  },
);
