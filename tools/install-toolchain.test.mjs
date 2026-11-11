import * as NodeTest from "node:test";
import * as NodeAssert from "node:assert/strict";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeChildProcess from "node:child_process";
import * as NodeCrypto from "node:crypto";
import * as NodeZlib from "node:zlib";
import {
  archiveInventory,
  flagsForSysroot,
  hostLauncher,
  stockRustEnvironment,
  verifyVersion,
} from "./install-toolchain.mjs";

function header(name, kind = "0", size = 0) {
  const result = Buffer.alloc(512);
  result.write(name, 0, 100);
  result.write("0000755\0", 100);
  result.write("0000000\0", 108);
  result.write("0000000\0", 116);
  result.write(size.toString(8).padStart(11, "0") + "\0", 124);
  result.write("00000000000\0", 136);
  result.fill(32, 148, 156);
  result.write(kind, 156);
  result.write("ustar\0", 257);
  result.write("00", 263);
  const checksum = result.reduce((sum, byte) => sum + byte, 0);
  result.write(checksum.toString(8).padStart(6, "0") + "\0 ", 148);
  return result;
}

NodeTest.test(
  "archive admission rejects links, traversal, duplicates and truncation before extraction",
  async () => {
    const directory = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-toolchain-archive-"));
    try {
      const body = Buffer.from("application fixture\n".repeat(10_000));
      const entries = [
        header("bluefe-lustr/", "5"),
        header("bluefe-lustr/compiler", "0", body.length),
        body,
        Buffer.alloc((512 - (body.length % 512)) % 512),
      ];
      const valid = Buffer.concat([...entries, Buffer.alloc(1024)]);
      const file = NodePath.join(directory, "archive.tar.zst");
      NodeFS.writeFileSync(file, NodeZlib.zstdCompressSync(valid));
      const result = await archiveInventory(file, "bluefe-lustr");
      NodeAssert.equal(
        result.get("bluefe-lustr/compiler").sha256,
        NodeCrypto.createHash("sha256").update(body).digest("hex"),
      );
      const invalid = [
        Buffer.concat([...entries, header("bluefe-lustr/link", "2"), Buffer.alloc(1024)]),
        Buffer.concat([...entries, header("bluefe-lustr/../escape"), Buffer.alloc(1024)]),
        Buffer.concat([...entries, header("bluefe-lustr/compiler"), Buffer.alloc(1024)]),
        Buffer.concat([...entries, header("/bluefe-lustr/absolute"), Buffer.alloc(1024)]),
        valid.subarray(0, 1200),
      ];
      for (const contents of invalid) {
        NodeFS.writeFileSync(file, NodeZlib.zstdCompressSync(contents));
        await NodeAssert.rejects(archiveInventory(file, "bluefe-lustr"));
      }
    } finally {
      NodeFS.rmSync(directory, { recursive: true, force: true });
    }
  },
);

NodeTest.test(
  "host launchers preserve caller arguments, explicit sysroots and bundled linker flavors",
  () => {
    const directory = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-toolchain launch '"));
    try {
      const producer = NodePath.join(directory, "producer");
      NodeFS.writeFileSync(
        producer,
        `#!${process.execPath}\nif (process.env["CARGO_BIN_EXE_test-app"] !== "preserved") process.exit(44);\nprocess.stdout.write(JSON.stringify(process.argv.slice(2)));\n`,
        { mode: 0o755 },
      );
      const loader = NodePath.join(directory, "loader");
      NodeFS.writeFileSync(
        loader,
        '#!/bin/bash\n[ "$1" = --argv0 ] || exit 33\nshift 2\nexec "$@"\n',
        {
          mode: 0o755,
        },
      );
      const wrapper = NodePath.join(directory, "wrapper");
      const root = NodePath.join(directory, "sysroot with spaces");
      NodeFS.writeFileSync(wrapper, hostLauncher({ producer, loader, root, compiler: true }), {
        mode: 0o755,
      });
      const run = (args) => {
        const child = NodeChildProcess.spawnSync(wrapper, args, {
          encoding: "utf8",
          env: { ...process.env, "CARGO_BIN_EXE_test-app": "preserved" },
        });
        NodeAssert.equal(child.status, 0, child.stderr);
        return JSON.parse(child.stdout);
      };
      const args = ["--crate-name", "example", "-C", "link-arg=a b", "file's name.rs"];
      NodeAssert.deepEqual(run(args), ["--sysroot", root, ...args]);
      NodeAssert.deepEqual(run(["--sysroot", "/explicit", ...args]), [
        "--sysroot",
        "/explicit",
        ...args,
      ]);
      NodeAssert.deepEqual(run(["--sysroot=/explicit", ...args]), ["--sysroot=/explicit", ...args]);
      for (const linkerFlavor of ["gnu", "darwin", "link", "wasm"]) {
        NodeFS.writeFileSync(
          wrapper,
          hostLauncher({ producer, root, linkerFlavor, linker: producer }),
          {
            mode: 0o755,
          },
        );
        NodeAssert.deepEqual(run(["-o", "target with spaces", "input.o"]), [
          "-flavor",
          linkerFlavor,
          "-o",
          "target with spaces",
          "input.o",
        ]);
      }
    } finally {
      NodeFS.rmSync(directory, { recursive: true, force: true });
    }
  },
);

NodeTest.test(
  "compiler identity and sysroot selection reject drift without changing caller flags",
  () => {
    const expected = { compiler_revision: "a".repeat(40), host: "x86_64-unknown-linux-gnu" };
    verifyVersion(
      `rustc\ncommit-hash: ${expected.compiler_revision}\nhost: ${expected.host}`,
      expected,
    );
    NodeAssert.throws(() =>
      verifyVersion("rustc\ncommit-hash: other\nhost: x86_64-unknown-linux-gnu", expected),
    );
    const encoded = "-C\x1flink-arg=a b";
    const selected = flagsForSysroot("/selected", encoded);
    NodeAssert.equal(selected, `${encoded}\x1f--sysroot\x1f/selected`);
    NodeAssert.equal(flagsForSysroot("/selected", selected), selected);
    NodeAssert.throws(() => flagsForSysroot("/selected", "--sysroot\x1f/another"));
    NodeAssert.throws(() =>
      flagsForSysroot("/selected", "--sysroot\x1f/selected\x1f--sysroot=/selected"),
    );
  },
);

NodeTest.test(
  "stock Rust validation preserves flags and targets before selecting matched tools",
  () => {
    const tools = {
      CARGO: "/pinned/bin/cargo",
      RUSTC: "/pinned/bin/rustc",
      RUSTDOC: "/pinned/bin/rustdoc",
    };
    const selection = {
      tools,
      sysroot: "/pinned",
      release: "1.99.1",
      target: "aarch64-apple-darwin",
    };
    const env = {
      CARGO_ENCODED_RUSTFLAGS: "--sysroot\x1f/pinned\x1f-C\x1flink-arg=a b\x1f",
      RUSTFLAGS: "ignored because encoded flags win",
      CARGO_BUILD_TARGET: "aarch64-apple-darwin",
      RUSTC: tools.RUSTC,
      RUSTC_WRAPPER: "",
    };
    const before = { ...env };
    NodeAssert.deepEqual(stockRustEnvironment(env, selection), {
      ...before,
      ...tools,
      RUSTUP_TOOLCHAIN: "1.99.1",
    });
    NodeAssert.deepEqual(env, before);
    for (const RUSTFLAGS of ["-C opt-level=1", "--sysroot=/pinned -C opt-level=1"])
      NodeAssert.equal(stockRustEnvironment({ RUSTFLAGS }, selection).RUSTFLAGS, RUSTFLAGS);
    NodeAssert.equal(
      Object.hasOwn(stockRustEnvironment({}, selection), "CARGO_ENCODED_RUSTFLAGS"),
      false,
    );
    for (const name of [
      ...Object.keys(tools),
      "RUSTC_WRAPPER",
      "RUSTC_WORKSPACE_WRAPPER",
      "CARGO_BUILD_RUSTC_WRAPPER",
      "CARGO_BUILD_RUSTC_WORKSPACE_WRAPPER",
      "T3_CODE_LS_TOOLCHAIN",
      "T3_CODE_LS_SYSROOT",
      "T3_CODE_LS_COMPILER",
    ]) {
      const invalid = { [name]: "/other" };
      NodeAssert.throws(() => stockRustEnvironment(invalid, selection), /conflicts/);
      NodeAssert.deepEqual(invalid, { [name]: "/other" });
    }
    NodeAssert.throws(() => stockRustEnvironment({}, { ...selection, release: "stable" }));
  },
);

NodeTest.test("stock Rust sysroot validation follows Cargo encoded flag precedence", () => {
  const selection = {
    tools: {},
    sysroot: "/pinned",
    release: "1.99.1",
    target: "x86_64-unknown-linux-gnu",
  };
  for (const encoded of [
    "--sysroot",
    "--sysroot=",
    "--sysroot\x1f/other",
    "--sysroot=/pinned\x1f--sysroot\x1f/pinned",
  ])
    NodeAssert.throws(
      () =>
        stockRustEnvironment(
          {
            CARGO_ENCODED_RUSTFLAGS: encoded,
            RUSTFLAGS: "--sysroot=/pinned",
          },
          selection,
        ),
      /sysroot/,
    );
  NodeAssert.equal(
    stockRustEnvironment(
      {
        CARGO_ENCODED_RUSTFLAGS: "",
        RUSTFLAGS: "--sysroot=/other",
      },
      selection,
    ).CARGO_ENCODED_RUSTFLAGS,
    "",
  );
  NodeAssert.equal(
    flagsForSysroot("/pinned", "-C\x1flink-arg=a b\x1f"),
    "-C\x1flink-arg=a b\x1f\x1f--sysroot\x1f/pinned",
  );
});

NodeTest.test("stock Rust validates only the effective selected-target flags", () => {
  const selection = {
    tools: {},
    sysroot: "/pinned",
    release: "1.99.1",
    target: "x86_64-unknown-linux-gnu",
  };
  const key = "CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUSTFLAGS";
  const benign = { [key]: "  --sysroot=/pinned  -C opt-level=1  " };
  NodeAssert.deepEqual(stockRustEnvironment(benign, selection), {
    ...benign,
    RUSTUP_TOOLCHAIN: "1.99.1",
  });
  for (const flags of [
    "--sysroot=/other -C opt-level=1",
    "--sysroot",
    "--sysroot=/pinned --sysroot /pinned",
  ])
    NodeAssert.throws(() => stockRustEnvironment({ [key]: flags }, selection), /sysroot/);
  for (const globals of [
    { CARGO_ENCODED_RUSTFLAGS: "" },
    { RUSTFLAGS: "" },
    { CARGO_ENCODED_RUSTFLAGS: "-C\x1flink-arg=a b", RUSTFLAGS: "--sysroot=/other" },
  ]) {
    const env = { [key]: "--sysroot=/other", ...globals };
    NodeAssert.deepEqual(stockRustEnvironment(env, selection), {
      ...env,
      RUSTUP_TOOLCHAIN: "1.99.1",
    });
  }
  NodeAssert.throws(
    () => stockRustEnvironment({ CARGO_BUILD_RUSTFLAGS: "--sysroot=/other" }, selection),
    /sysroot/,
  );
  const unused = { CARGO_TARGET_AARCH64_APPLE_DARWIN_RUSTFLAGS: "--sysroot=/other" };
  NodeAssert.deepEqual(stockRustEnvironment(unused, selection), {
    ...unused,
    RUSTUP_TOOLCHAIN: "1.99.1",
  });
  NodeAssert.throws(
    () => stockRustEnvironment(unused, { ...selection, target: "aarch64-apple-darwin" }),
    /sysroot/,
  );
});
