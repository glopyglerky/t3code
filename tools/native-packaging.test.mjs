import * as NodeTest from "node:test";
import * as NodeAssert from "node:assert/strict";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";
import * as NodeChildProcess from "node:child_process";

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
        const expected = NodePath.join(
          NodePath.dirname(modulePath).replace(/\.asar(?=\/)/, ".asar.unpacked"),
          "native-bin",
          "t3-code-ls-policy",
        );
        NodeFS.mkdirSync(NodePath.dirname(expected), { recursive: true });
        NodeFS.writeFileSync(
          expected,
          '#!/usr/bin/env node\nconsole.log("native-policy-fixture")\n',
          { mode: 0o755 },
        );
        NodeAssert.equal(nativeExecutable("policy"), expected);
        const child = NodeChildProcess.spawnSync(nativeExecutable("policy"), [], {
          encoding: "utf8",
        });
        NodeAssert.equal(child.status, 0, child.stderr);
        NodeAssert.equal(child.stdout.trim(), "native-policy-fixture");
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
        `#!${process.execPath}\nconst NodeFS = require('node:fs'), p = require('node:path');\nconst args = process.argv.slice(2);\nconst dir = p.join(args[args.indexOf('--target-dir') + 1], args[args.indexOf('--target') + 1], 'release');\nNodeFS.mkdirSync(dir, {recursive:true});\nfor (const name of ${JSON.stringify(nativeComponents)}) NodeFS.writeFileSync(p.join(dir, 't3-code-ls-' + name), '#!/usr/bin/env node\\nconsole.log("fixture-component")\\n', {mode:0o755});\nNodeFS.writeFileSync(p.join(dir, 'flags.json'), JSON.stringify(process.env.CARGO_ENCODED_RUSTFLAGS));\n`,
        { mode: 0o755 },
      );
      const env = {
        ...process.env,
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
      NodeAssert.equal(
        JSON.parse(
          NodeFS.readFileSync(NodePath.join(target, "x86_64-unknown-linux-gnu/release/flags.json")),
        ),
        `-C\x1fdebuginfo=0\x1f--sysroot\x1f${toolchain}`,
      );
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
          NodePath.join(extracted, "native-bin", nativeFile("policy", "linux-x64")),
          [],
          { encoding: "utf8" },
        ).stdout.trim(),
        "fixture-component",
      );
      NodeAssert.throws(() => stageNativeArtifacts(output, stage, "win32-x64"), /do not match/);
      NodeFS.appendFileSync(NodePath.join(output, nativeFile("policy", "linux-x64")), "damage");
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
      NodeAssert.match(cross.stderr, /Missing produced target sysroot/);
    } finally {
      NodeFS.rmSync(root, { recursive: true, force: true });
    }
  },
);
