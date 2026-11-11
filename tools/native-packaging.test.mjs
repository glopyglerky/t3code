import * as NodeTest from "node:test";
import * as NodeAssert from "node:assert/strict";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";
import * as NodeChildProcess from "node:child_process";
import * as NodeNet from "node:net";

NodeTest.test(
  "database-only packages require target-specific acceptance and retain other upstream helpers",
  async () => {
    const { nativeHash, stageNativeArtifacts, nativeArtifactFiles } =
      await import("./native-artifacts.mjs");
    const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-database-package-"));
    try {
      const source = NodePath.join(root, "source");
      NodeFS.mkdirSync(source);
      NodeFS.writeFileSync(NodePath.join(source, "t3-code-ls-db"), "database fixture", {
        mode: 0o755,
      });
      const digest = nativeHash(NodePath.join(source, "t3-code-ls-db"));
      const upstream = JSON.parse(
        NodeFS.readFileSync(new URL("../upstream.lock.json", import.meta.url), "utf8"),
      ).upstream.commit;
      const sourceCommit = NodeChildProcess.execFileSync("git", ["rev-parse", "HEAD"], {
        encoding: "utf8",
      }).trim();
      const manifest = {
        schema: 2,
        target: "darwin-arm64",
        rustTarget: "aarch64-apple-darwin",
        implementation: "database-only",
        sourceCommit,
        upstreamCommit: upstream,
        files: { "t3-code-ls-db": digest },
        compiler: {
          route: "rustc",
          toolchain: "1.99.1",
          version: "rustc fixture",
          cargoVersion: "cargo fixture",
          executableSha256: "2".repeat(64),
        },
        databaseAcceptance: { status: "candidate" },
      };
      const save = () =>
        NodeFS.writeFileSync(NodePath.join(source, "manifest.json"), JSON.stringify(manifest));
      save();
      NodeAssert.throws(() => nativeArtifactFiles(source, "darwin-arm64"), /acceptance/);
      manifest.databaseAcceptance = {
        status: "passed",
        target: "darwin-arm64",
        sourceCommit,
        upstreamCommit: upstream,
        executableSha256: digest,
        checks: ["storage-tests", "startup", "production", "transactions", "restart-and-ownership"],
      };
      save();
      NodeAssert.throws(() => nativeArtifactFiles(source, "darwin-arm64"), /package acceptance/);
      const installed = NodePath.join(root, "installed");
      const selector = NodePath.join(installed, "packages/shared/src/nativeExecutable.ts");
      NodeFS.mkdirSync(NodePath.dirname(selector), { recursive: true });
      NodeFS.copyFileSync(
        new URL("../packages/shared/src/nativeExecutable.ts", import.meta.url),
        selector,
      );
      stageNativeArtifacts(source, NodePath.join(installed, "bin"), "darwin-arm64", {
        lifecycleOnly: true,
      });
      const { selectNativeExecutable } = await import(NodeURL.pathToFileURL(selector).href);
      NodeAssert.throws(
        () => selectNativeExecutable("db", undefined, "darwin", "arm64"),
        /package acceptance/,
      );
      manifest.packageAcceptance = {
        status: "passed",
        target: "darwin-arm64",
        sourceCommit,
        upstreamCommit: upstream,
        executableSha256: digest,
        checks: ["source-package", "cli-sidecar", "desktop-asar"],
      };
      for (const invalid of [
        { status: "candidate" },
        { target: "darwin-x64" },
        { sourceCommit: "0".repeat(40) },
        { upstreamCommit: "0".repeat(40) },
        { executableSha256: "0".repeat(64) },
        { checks: ["source-package", "cli-sidecar"] },
      ]) {
        const accepted = manifest.packageAcceptance;
        manifest.packageAcceptance = { ...accepted, ...invalid };
        save();
        NodeAssert.throws(() => nativeArtifactFiles(source, "darwin-arm64"), /package acceptance/);
        manifest.packageAcceptance = accepted;
      }
      save();
      stageNativeArtifacts(source, NodePath.join(installed, "bin"), "darwin-arm64");
      NodeAssert.equal(
        selectNativeExecutable("db", undefined, "darwin", "arm64"),
        NodePath.join(installed, "bin", "t3-code-ls-db"),
      );
      NodeAssert.equal(selectNativeExecutable("provider", undefined, "darwin", "arm64"), undefined);
      NodeAssert.equal(selectNativeExecutable("generate", undefined, "darwin", "arm64"), undefined);
      NodeAssert.equal(
        selectNativeExecutable("db", undefined, "darwin", "arm64", "upstream"),
        undefined,
      );
      const supplied = {
        ...process.env,
        T3_CODE_LS_NATIVE_DIR: source,
        T3_CODE_LS_NATIVE_TARGET: "darwin-arm64",
      };
      for (const name of [
        "T3_CODE_LS_IMPLEMENTATION",
        "T3_CODE_LS_NATIVE_COMPONENTS",
        "T3_CODE_LS_TOOLCHAIN",
        "T3_CODE_LS_SYSROOT",
      ])
        delete supplied[name];
      const prequalified = NodeChildProcess.spawnSync(
        process.execPath,
        [
          NodeURL.fileURLToPath(new URL("./build-native.mjs", import.meta.url)),
          NodePath.join(root, "prequalified"),
        ],
        { env: supplied, encoding: "utf8" },
      );
      NodeAssert.equal(prequalified.status, 0, prequalified.stderr);
      NodeAssert.throws(() => nativeArtifactFiles(source, "win32-arm64"), /do not match/);
      const stale = NodePath.join(root, "stale");
      NodeFS.mkdirSync(stale);
      NodeFS.writeFileSync(NodePath.join(stale, "t3-code-ls-provider"), "unrelated old helper");
      NodeAssert.throws(
        () => stageNativeArtifacts(source, stale, "darwin-arm64"),
        /clean.*directory/,
      );
      manifest.sourceCommit = "3".repeat(40);
      save();
      NodeAssert.throws(() => nativeArtifactFiles(source, "darwin-arm64"), /acceptance/);
      manifest.sourceCommit = sourceCommit;
      manifest.databaseAcceptance.checks.pop();
      save();
      NodeAssert.throws(() => nativeArtifactFiles(source, "darwin-arm64"), /acceptance/);
      manifest.databaseAcceptance.checks.push("restart-and-ownership");
      save();
      NodeFS.appendFileSync(NodePath.join(source, "t3-code-ls-db"), "changed after acceptance");
      NodeAssert.throws(() => nativeArtifactFiles(source, "darwin-arm64"), /hash/);
    } finally {
      NodeFS.rmSync(root, { recursive: true, force: true });
    }
  },
);

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
    const { nativeComponents, nativeFile, stageNativeArtifacts } =
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
        T3_CODE_LS_COMPILER: "lustr",
        T3_CODE_LS_TOOLCHAIN: toolchain,
        T3_CODE_LS_NATIVE_TARGET: "linux-x64",
        CARGO_BUILD_TARGET: "x86_64-unknown-linux-gnu",
        CARGO_TARGET_DIR: target,
        CARGO_ENCODED_RUSTFLAGS: "-C\x1fdebuginfo=0",
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
      NodeAssert.match(cross.stderr, /Cannot qualify.*this host/);
    } finally {
      NodeFS.rmSync(root, { recursive: true, force: true });
    }
  },
);

NodeTest.test(
  "stock Rust build helper preserves target flags and rejects mismatched sysroots",
  async () => {
    const { nativeComponents, nativeTargets } = await import("./native-artifacts.mjs");
    const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-stock-flags-"));
    try {
      const compiler = NodePath.join(root, "compiler");
      const bin = NodePath.join(compiler, "bin");
      const target = NodePath.join(root, "cargo");
      const output = NodePath.join(root, "native-bin");
      const capture = NodePath.join(root, "build-environment.json");
      const rustTarget = "x86_64-unknown-linux-gnu";
      // oxlint-disable-next-line t3code/no-global-process-runtime -- The fixture compiler must report the actual subprocess host; no Effect context crosses this boundary.
      const compilerHost = nativeTargets[`${NodeOS.platform()}-${NodeOS.arch()}`];
      const flagKey = "CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUSTFLAGS";
      const library = NodePath.join(compiler, "lib/rustlib", rustTarget, "lib");
      NodeFS.mkdirSync(bin, { recursive: true });
      NodeFS.mkdirSync(library, { recursive: true });
      const script = (name, body) =>
        NodeFS.writeFileSync(NodePath.join(bin, name), `#!${process.execPath}\n${body}\n`, {
          mode: 0o755,
        });
      script(
        "rustup",
        `if (process.argv[2] !== 'which') process.exit(31);
process.stdout.write(require('node:path').join(${JSON.stringify(bin)}, process.argv.at(-1)));`,
      );
      script(
        "rustc",
        `const args = process.argv.slice(2);
if (args[0] === '-vV') process.stdout.write(${JSON.stringify(`rustc 1.99.1\nrelease: 1.99.1\nhost: ${compilerHost}`)});
else if (args[1] === 'sysroot') process.stdout.write(${JSON.stringify(compiler)});
else if (args[1] === 'target-libdir') process.stdout.write(${JSON.stringify(library)});
else process.exit(32);`,
      );
      script("rustdoc", "process.exit(0);");
      script(
        "cargo",
        `const fs = require('node:fs'), path = require('node:path');
const args = process.argv.slice(2);
if (args[0] === '-V') { process.stdout.write('cargo 1.99.1'); process.exit(0); }
if (args[0] !== 'build') process.exit(33);
const dir = path.join(args[args.indexOf('--target-dir') + 1], args[args.indexOf('--target') + 1], 'release');
fs.mkdirSync(dir, { recursive: true });
for (const name of ${JSON.stringify(nativeComponents)}) fs.writeFileSync(path.join(dir, 't3-code-ls-' + name), '#!/bin/sh\\nexit 0\\n', { mode: 0o755 });
fs.writeFileSync(${JSON.stringify(capture)}, JSON.stringify({
  encoded: process.env.CARGO_ENCODED_RUSTFLAGS,
  plain: process.env.RUSTFLAGS,
  targetFlags: process.env.${flagKey},
  target: process.env.CARGO_BUILD_TARGET,
  arguments: args
}));`,
      );
      const env = { ...process.env };
      for (const name of Object.keys(env)) {
        if (
          name.startsWith("T3_CODE_LS_") ||
          /^CARGO_TARGET_.*_RUSTFLAGS$/.test(name) ||
          [
            "CARGO",
            "RUSTC",
            "RUSTDOC",
            "RUSTFLAGS",
            "CARGO_ENCODED_RUSTFLAGS",
            "CARGO_BUILD_RUSTFLAGS",
            "RUSTC_WRAPPER",
            "RUSTC_WORKSPACE_WRAPPER",
            "CARGO_BUILD_RUSTC_WRAPPER",
            "CARGO_BUILD_RUSTC_WORKSPACE_WRAPPER",
          ].includes(name)
        )
          delete env[name];
      }
      Object.assign(env, {
        PATH: `${bin}${NodePath.delimiter}${process.env.PATH}`,
        T3_CODE_LS_COMPILER: "rustc",
        T3_CODE_LS_NATIVE_TARGET: "linux-x64",
        CARGO_BUILD_TARGET: rustTarget,
        CARGO_TARGET_DIR: target,
        [flagKey]: "  -C opt-level=1  ",
      });
      const run = (extra = {}) =>
        NodeChildProcess.spawnSync(
          process.execPath,
          [NodeURL.fileURLToPath(new URL("./build-native.mjs", import.meta.url)), output],
          { env: { ...env, ...extra }, encoding: "utf8" },
        );
      const read = () => JSON.parse(NodeFS.readFileSync(capture, "utf8"));
      const baseline = run();
      NodeAssert.equal(baseline.status, 0, baseline.stderr);
      NodeAssert.equal(Object.hasOwn(read(), "encoded"), false);
      NodeAssert.equal(Object.hasOwn(read(), "plain"), false);
      NodeAssert.equal(read().targetFlags, env[flagKey]);
      NodeAssert.equal(read().target, rustTarget);
      NodeAssert.equal(read().arguments[read().arguments.indexOf("--target") + 1], rustTarget);
      const selectedFlags = `--sysroot=${compiler}  -C opt-level=1`;
      const matching = run({ [flagKey]: selectedFlags });
      NodeAssert.equal(matching.status, 0, matching.stderr);
      NodeAssert.equal(read().targetFlags, selectedFlags);
      for (const globals of [
        { RUSTFLAGS: " -C opt-level=2 " },
        { RUSTFLAGS: "" },
        { CARGO_ENCODED_RUSTFLAGS: "", RUSTFLAGS: "--sysroot=/other" },
      ]) {
        const result = run({ ...globals, [flagKey]: "--sysroot=/other" });
        NodeAssert.equal(result.status, 0, result.stderr);
        NodeAssert.equal(read().encoded, globals.CARGO_ENCODED_RUSTFLAGS);
        NodeAssert.equal(read().plain, globals.RUSTFLAGS);
      }
      const before = NodeFS.readFileSync(capture, "utf8");
      for (const flags of [
        "--sysroot=/other -C opt-level=1",
        "--sysroot",
        `--sysroot=${compiler} --sysroot ${compiler}`,
      ]) {
        const result = run({ [flagKey]: flags });
        NodeAssert.notEqual(result.status, 0);
        NodeAssert.match(result.stderr, /sysroot/);
        NodeAssert.equal(NodeFS.readFileSync(capture, "utf8"), before);
      }
    } finally {
      NodeFS.rmSync(root, { recursive: true, force: true });
    }
  },
);

NodeTest.test(
  "explicit upstream packages retain service implementations and reject native overrides",
  async () => {
    const { nativeTargets, isQualifiedNativeHost, stageNativeArtifacts, nativeArtifactFiles } =
      await import("./native-artifacts.mjs");
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
        env.T3_CODE_LS_IMPLEMENTATION = "upstream";
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
