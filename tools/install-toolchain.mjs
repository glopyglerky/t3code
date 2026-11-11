#!/usr/bin/env node
/* oxlint-disable t3code/no-global-process-runtime -- This standalone tool provisions a hosted build environment without an Effect runtime. */
import * as NodeCrypto from "node:crypto";
import * as NodeChildProcess from "node:child_process";
import * as NodeFS from "node:fs";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";
import * as NodeStream from "node:stream";
import * as NodeStreamPromises from "node:stream/promises";
import * as NodeZlib from "node:zlib";
// Install the pinned compatibility package outside source. Native Lust is separate.

const repository = NodePath.resolve(NodePath.dirname(NodeURL.fileURLToPath(import.meta.url)), "..");
const pin = JSON.parse(
  await NodeFSP.readFile(NodePath.join(repository, "toolchain.lock.json"), "utf8"),
);

function assert(condition, message) {
  if (!condition) throw new Error(message);
}

function command(program, args) {
  return NodeChildProcess.execFileSync(program, args, {
    encoding: "utf8",
    maxBuffer: 8 * 1024 * 1024,
  }).trim();
}

async function sha256(file) {
  const digest = NodeCrypto.createHash("sha256");
  for await (const chunk of NodeFS.createReadStream(file)) digest.update(chunk);
  return digest.digest("hex");
}

async function verifyAsset(file, asset) {
  const info = await NodeFSP.lstat(file);
  assert(info.isFile() && !info.isSymbolicLink(), `Expected a regular asset: ${asset.name}`);
  assert(info.size === asset.size, `Asset size mismatch: ${asset.name}`);
  assert((await sha256(file)) === asset.sha256, `Asset checksum mismatch: ${asset.name}`);
}

async function downloadAsset(directory, asset, toolchain) {
  const file = NodePath.join(directory, asset.name);
  if (NodeFS.existsSync(file)) {
    await verifyAsset(file, asset);
    return;
  }
  console.error(`Downloading ${asset.name}`);
  const temporary = `${file}.partial-${process.pid}`;
  try {
    const response = await fetch(
      `${toolchain.repository}/releases/download/${toolchain.release}/${asset.name}`,
      {
        signal: AbortSignal.timeout(300_000),
      },
    );
    assert(
      response.ok && response.body,
      `Download failed for ${asset.name}: HTTP ${response.status}`,
    );
    let received = 0;
    const limit = new NodeStream.Transform({
      transform(chunk, _encoding, callback) {
        received += chunk.length;
        callback(
          received > asset.size ? new Error(`Asset exceeds pinned size: ${asset.name}`) : null,
          chunk,
        );
      },
    });
    await NodeStreamPromises.pipeline(
      NodeStream.Readable.fromWeb(response.body),
      limit,
      NodeFS.createWriteStream(temporary, { flags: "wx", mode: 0o600 }),
    );
    await verifyAsset(temporary, asset);
    await NodeFSP.rename(temporary, file);
  } finally {
    await NodeFSP.rm(temporary, { force: true });
  }
}

export function validateMember(name, kind, expectedRoot) {
  const relative = name.replace(/\/$/, "");
  assert(!/[\x00-\x1f\x7f\\]/.test(name), `Unsafe archive member: ${JSON.stringify(name)}`);
  assert(
    !NodePath.posix.isAbsolute(name) &&
      relative.split("/").every((part) => part && part !== "." && part !== ".."),
    `Unsafe archive path: ${name}`,
  );
  assert(
    relative === expectedRoot || relative.startsWith(`${expectedRoot}/`),
    `Unexpected archive root: ${name}`,
  );
  assert(
    kind === "5" || kind === "0" || kind === "\0",
    `Links and special archive entries are forbidden: ${name}`,
  );
  assert(kind === "5" || !name.endsWith("/"), `Archive entry type mismatch: ${name}`);
  return relative;
}

function octal(buffer) {
  const value = buffer.toString("ascii").replace(/\0.*$/, "").trim();
  assert(/^[0-7]+$/.test(value), "Unsupported archive numeric field");
  const result = Number.parseInt(value, 8);
  assert(Number.isSafeInteger(result), "Archive numeric field overflow");
  return result;
}

// The pinned producer uses USTAR without links, PAX, sparse files or special entries.
// Parse that bounded format before extraction and hash every file while streaming.
export async function archiveInventory(archive, expectedRoot) {
  const members = new Map();
  let pending = Buffer.alloc(0);
  let current = null;
  let remaining = 0;
  let padding = 0;
  let ended = false;
  let zeroBlocks = 0;
  let expandedBytes = 0;
  await NodeStreamPromises.pipeline(
    NodeFS.createReadStream(archive),
    NodeZlib.createZstdDecompress(),
    async (chunks) => {
      for await (const chunk of chunks) {
        pending = Buffer.concat([pending, chunk]);
        while (pending.length) {
          if (remaining) {
            const count = Math.min(remaining, pending.length);
            current.digest.update(pending.subarray(0, count));
            remaining -= count;
            pending = pending.subarray(count);
            if (!remaining) current.sha256 = current.digest.digest("hex");
            continue;
          }
          if (padding) {
            const count = Math.min(padding, pending.length);
            assert(
              pending.subarray(0, count).every((byte) => byte === 0),
              "Nonzero archive padding",
            );
            padding -= count;
            pending = pending.subarray(count);
            continue;
          }
          if (pending.length < 512) break;
          const header = pending.subarray(0, 512);
          pending = pending.subarray(512);
          if (header.every((byte) => byte === 0)) {
            ended = true;
            zeroBlocks++;
            continue;
          }
          assert(!ended, "Archive data follows its terminator");
          assert(
            header.subarray(257, 263).toString("ascii") === "ustar\0",
            "Only the pinned USTAR format is supported",
          );
          const expectedChecksum = octal(header.subarray(148, 156));
          let checksum = 0;
          for (let index = 0; index < header.length; index++)
            checksum += index >= 148 && index < 156 ? 32 : header[index];
          assert(checksum === expectedChecksum, "Archive header checksum mismatch");
          const text = (start, stop) =>
            header.subarray(start, stop).toString("utf8").replace(/\0.*$/s, "");
          const prefix = text(345, 500);
          const name = `${prefix ? `${prefix}/` : ""}${text(0, 100)}`;
          const kind = String.fromCharCode(header[156]);
          const relative = validateMember(name, kind, expectedRoot);
          assert(!members.has(relative), `Duplicate archive member: ${relative}`);
          assert(!text(157, 257), `Unexpected archive link target: ${relative}`);
          const size = octal(header.subarray(124, 136));
          const mode = octal(header.subarray(100, 108));
          assert((mode & ~0o777) === 0, `Special permission bits are forbidden: ${relative}`);
          assert(kind !== "5" || size === 0, `Directory has file content: ${relative}`);
          expandedBytes += size;
          assert(
            expandedBytes <= 1024 * 1024 * 1024 && members.size < 10_000,
            "Archive exceeds pinned package bounds",
          );
          current = {
            kind: kind === "5" ? "directory" : "file",
            size,
            mode,
            digest: NodeCrypto.createHash("sha256"),
          };
          if (size === 0) current.sha256 = current.digest.digest("hex");
          members.set(relative, current);
          remaining = size;
          padding = (512 - (size % 512)) % 512;
        }
      }
    },
  );
  assert(
    !remaining && !padding && pending.length === 0 && zeroBlocks >= 2 && members.size > 0,
    "Truncated archive",
  );
  assert(members.get(expectedRoot)?.kind === "directory", "Archive root must be a directory");
  for (const [name, member] of members) {
    delete member.digest;
    let parent = NodePath.posix.dirname(name);
    while (parent !== ".") {
      assert(
        members.get(parent)?.kind === "directory",
        `Missing or non-directory archive parent: ${parent}`,
      );
      parent = NodePath.posix.dirname(parent);
    }
  }
  return members;
}

async function verifyExtracted(directory, members, rootName) {
  const seen = new Set();
  async function walk(relative) {
    const absolute = NodePath.join(directory, relative);
    const info = await NodeFSP.lstat(absolute);
    assert(!info.isSymbolicLink(), `Extracted symlink is forbidden: ${relative}`);
    const kind = info.isDirectory() ? "directory" : info.isFile() ? "file" : "unsupported";
    const expected = members.get(relative);
    assert(
      expected?.kind === kind && expected.mode === (info.mode & 0o777),
      `Unexpected extracted member or permissions: ${relative}`,
    );
    seen.add(relative);
    if (kind === "directory") {
      for (const entry of await NodeFSP.readdir(absolute)) await walk(`${relative}/${entry}`);
    } else {
      assert(
        info.size === expected.size && (await sha256(absolute)) === expected.sha256,
        `Extracted file checksum mismatch: ${relative}`,
      );
    }
  }
  await walk(rootName);
  assert(seen.size === members.size, "Extracted package has missing members");
}

export function verifyVersion(version, expected) {
  const fields = Object.fromEntries(
    version
      .split("\n")
      .slice(1)
      .map((line) => {
        const separator = line.indexOf(":");
        return [line.slice(0, separator), line.slice(separator + 1).trim()];
      }),
  );
  assert(
    fields["commit-hash"] === expected.compiler_revision,
    "Produced compiler source identity mismatch",
  );
  assert(fields.host === expected.host, "Produced compiler host mismatch");
}

export function flagsForSysroot(
  root,
  encoded = process.env.CARGO_ENCODED_RUSTFLAGS,
  plain = process.env.RUSTFLAGS,
) {
  const flags =
    encoded !== undefined
      ? encoded === ""
        ? []
        : encoded.split("\x1f")
      : (plain ?? "").split(/\s+/).filter(Boolean);
  let selected = false;
  for (let index = 0; index < flags.length; index++) {
    const flag = flags[index];
    if (flag === "--sysroot" || flag.startsWith("--sysroot=")) {
      const value = flag === "--sysroot" ? flags[++index] : flag.slice("--sysroot=".length);
      assert(
        !selected && value && samePath(value, root),
        "Conflicting or repeated explicit sysroot selection",
      );
      selected = true;
    }
  }
  return (selected ? flags : [...flags, "--sysroot", root]).join("\x1f");
}

function samePath(left, right) {
  const canonical = (value) =>
    NodeFS.existsSync(value) ? NodeFS.realpathSync(value) : NodePath.resolve(value);
  return canonical(left) === canonical(right);
}

export function stockRustEnvironment(env, { tools, sysroot, release, target }) {
  assert(/^\d+\.\d+\.\d+$/.test(release), "Stock Rust requires an exact pinned release");
  assert(
    (env.T3_CODE_LS_COMPILER ?? "rustc") === "rustc",
    "Stock Rust conflicts with the explicit compiler route",
  );
  for (const name of ["T3_CODE_LS_TOOLCHAIN", "T3_CODE_LS_SYSROOT"])
    assert(env[name] === undefined, `${name} conflicts with the pinned stock Rust route`);
  for (const name of [
    "RUSTC_WRAPPER",
    "RUSTC_WORKSPACE_WRAPPER",
    "CARGO_BUILD_RUSTC_WRAPPER",
    "CARGO_BUILD_RUSTC_WORKSPACE_WRAPPER",
  ])
    assert(!env[name], `${name} conflicts with the verified compiler route`);
  for (const [name, selected] of Object.entries(tools))
    assert(
      !env[name] || samePath(env[name], selected),
      `${name} conflicts with the pinned stock Rust route`,
    );
  assert(sysroot, "The pinned compiler did not report its sysroot");
  assert(
    typeof target === "string" && target.length > 0,
    "The selected Rust target or compiler host is required",
  );
  // Validate explicit flags without synthesizing an encoded value that masks
  // Cargo's per-target flags. The compiler supplies its own matched default.
  const targetName = target.endsWith(".json") ? NodePath.basename(target, ".json") : target;
  const targetFlags = `CARGO_TARGET_${targetName.toUpperCase().replaceAll("-", "_")}_RUSTFLAGS`;
  const plain = env.RUSTFLAGS ?? env[targetFlags] ?? env.CARGO_BUILD_RUSTFLAGS ?? "";
  const encoded = env.CARGO_ENCODED_RUSTFLAGS ?? plain.split(/\s+/).filter(Boolean).join("\x1f");
  flagsForSysroot(sysroot, encoded, "");
  return { ...env, ...tools, RUSTUP_TOOLCHAIN: release };
}

function shellQuote(value) {
  return `'${value.replaceAll("'", `'"'"'`)}'`;
}

export function hostLauncher({ producer, loader, root, compiler, linkerFlavor, linker }) {
  const executable = loader
    ? `${shellQuote(loader)} --argv0 ${shellQuote(producer)} ${shellQuote(producer)}`
    : shellQuote(producer);
  const selection = linkerFlavor
    ? `exec ${shellQuote(linker)} -flavor ${linkerFlavor} "$@"\n`
    : compiler
      ? `for argument do\n  case "$argument" in\n    --sysroot|--sysroot=*) exec ${executable} "$@" ;;\n  esac\ndone\nexec ${executable} --sysroot ${shellQuote(root)} "$@"\n`
      : `exec ${executable} "$@"\n`;
  // dash discards inherited environment keys containing '-', including Cargo's
  // CARGO_BIN_EXE_<package-name> variables. Bash preserves those compiler inputs.
  return `#!/bin/bash\n# Generated host loader adapter. The verified package is unchanged.\n${selection}`;
}

// Some producer binaries request /usr/lib/ld-linux-x86-64.so.2. Debian and Ubuntu
// expose the same GNU loader at /lib64 instead. Keep all producer bytes intact;
// an explicitly recorded local tool directory adapts only process launching.
async function hostTools(destination, payload, members, platform) {
  const missing = new Map();
  for (const object of platform.objects) {
    for (const interpreter of object.interpreters) {
      if (!NodeFS.existsSync(interpreter)) {
        assert(
          interpreter === "/usr/lib/ld-linux-x86-64.so.2",
          `Unsupported missing ELF interpreter: ${interpreter}`,
        );
        assert(
          members.get(`${pin.compatibility.root}/${object.path}`)?.kind === "file",
          `Unknown runtime object: ${object.path}`,
        );
        missing.set(object.path, interpreter);
      }
    }
  }
  if (missing.size === 0) return { root: payload, adapter: null };
  const loader = await NodeFSP.realpath("/lib64/ld-linux-x86-64.so.2");
  const root = NodePath.join(destination, "host-tools");
  const expected = new Map();
  const wrappers = [];
  for (const [name, member] of members) {
    const relative =
      name === pin.compatibility.root ? "" : name.slice(pin.compatibility.root.length + 1);
    const file = NodePath.join(root, relative);
    const original = NodePath.join(payload, relative);
    if (member.kind === "directory") {
      expected.set(relative, { kind: "directory" });
      await NodeFSP.mkdir(file, { recursive: true });
      const directory = await NodeFSP.lstat(file);
      assert(
        directory.isDirectory() && !directory.isSymbolicLink(),
        `Unexpected adapter directory: ${relative}`,
      );
      continue;
    }
    let script;
    const compiler = ["bin/rustc", "bin/rustdoc", "bin/clippy-driver"].includes(relative);
    const linkerPrefix = `lib/rustlib/${pin.compatibility.host}/bin/gcc-ld/`;
    const linkerFlavor = relative.startsWith(linkerPrefix)
      ? { "ld.lld": "gnu", "ld64.lld": "darwin", "lld-link": "link", "wasm-ld": "wasm" }[
          relative.slice(linkerPrefix.length)
        ]
      : undefined;
    if (missing.has(relative) || compiler) {
      // lld-wrapper reads current_exe, which identifies the explicit loader.
      // Preserve its exact pinned dispatch using the SAME bundled rust-lld:
      // blueFe 1b74388, src/tools/lld-wrapper/src/main.rs, blob 299f0329.
      script = hostLauncher({
        producer: original,
        loader: missing.has(relative) ? loader : undefined,
        root,
        compiler,
        linkerFlavor,
        linker: NodePath.join(payload, `lib/rustlib/${pin.compatibility.host}/bin/rust-lld`),
      });
      expected.set(relative, { kind: "wrapper", content: script });
      wrappers.push({
        path: relative,
        sha256: NodeCrypto.createHash("sha256").update(script).digest("hex"),
        producer: original,
        route: linkerFlavor ? `bundled rust-lld -flavor ${linkerFlavor}` : "host GNU loader",
      });
      if (!NodeFS.existsSync(file)) {
        await NodeFSP.writeFile(file, script, { flag: "wx", mode: 0o755 });
        await NodeFSP.chmod(file, 0o755);
      }
    } else {
      expected.set(relative, { kind: "symlink", target: original });
      if (!NodeFS.existsSync(file)) await NodeFSP.symlink(original, file);
    }
  }
  const seen = new Set();
  async function verify(relative) {
    const file = NodePath.join(root, relative);
    const info = await NodeFSP.lstat(file);
    const entry = expected.get(relative);
    assert(entry, `Unrecorded host adapter member: ${relative}`);
    seen.add(relative);
    if (entry.kind === "directory") {
      assert(
        info.isDirectory() && !info.isSymbolicLink(),
        `Unexpected adapter directory: ${relative}`,
      );
      for (const child of await NodeFSP.readdir(file))
        await verify(relative ? `${relative}/${child}` : child);
    } else if (entry.kind === "symlink") {
      assert(
        info.isSymbolicLink() && (await NodeFSP.readlink(file)) === entry.target,
        `Host adapter link mismatch: ${relative}`,
      );
    } else {
      assert(
        info.isFile() &&
          !info.isSymbolicLink() &&
          (info.mode & 0o777) === 0o755 &&
          (await NodeFSP.readFile(file, "utf8")) === entry.content,
        `Host adapter wrapper mismatch: ${relative}`,
      );
    }
  }
  await verify("");
  assert(seen.size === expected.size, "Host adapter is incomplete");
  console.error(
    `Using an explicit host loader adapter for ${missing.size} producer executables; the package's interpreter path is absent.`,
  );
  return {
    root,
    adapter: {
      loader,
      loader_sha256: await sha256(loader),
      missing_interpreters: [...new Set(missing.values())],
      wrappers,
      scope:
        "Local process-launch adaptation. Producer binaries and sysroot bytes remain unchanged; this is not evidence that the unadapted package runs on this host.",
    },
  };
}

async function install(destination) {
  const toolchain = pin.compatibility;
  assert(
    pin.schema === 1 && toolchain.host === "x86_64-unknown-linux-gnu",
    "Unsupported toolchain lock schema or host",
  );
  const native = JSON.parse(
    await NodeFSP.readFile(NodePath.join(repository, pin.native_lust.metadata), "utf8"),
  );
  assert(
    native.revision === pin.native_lust.revision,
    "Consolidated native Lust metadata drifted from its retained pin",
  );
  assert(
    process.platform === "linux" && process.arch === "x64",
    "The pinned package is qualified only for Linux x86_64 GNU",
  );
  const glibc = process.report.getReport().header.glibcVersionRuntime;
  const [major, minor] = (glibc ?? "0.0").split(".").map(Number);
  const [requiredMajor, requiredMinor] = toolchain.glibc_minimum.split(".").map(Number);
  assert(
    major > requiredMajor || (major === requiredMajor && minor >= requiredMinor),
    `The pinned package requires GLIBC ${toolchain.glibc_minimum}`,
  );
  assert(
    NodePath.isAbsolute(destination) && !/[\r\n]/.test(destination),
    "--destination must be an absolute path without newlines",
  );
  await NodeFSP.mkdir(destination, { recursive: true });
  destination = await NodeFSP.realpath(destination);
  const source = await NodeFSP.realpath(repository);
  assert(
    destination !== source && !destination.startsWith(`${source}${NodePath.sep}`),
    "Toolchains must be outside the source checkout",
  );
  assert(
    /^[\w.-]+$/.test(toolchain.root) && /^[\w.-]+$/.test(toolchain.release),
    "Unsafe toolchain package identity",
  );
  assert(
    toolchain.repository === "https://github.com/glopyglerky/blueFe",
    "Unreviewed compiler repository",
  );
  const names = new Set();
  for (const asset of toolchain.assets) {
    assert(
      /^[\w.-]+$/.test(asset.name) &&
        /^[a-f0-9]{64}$/.test(asset.sha256) &&
        Number.isSafeInteger(asset.size) &&
        asset.size > 0,
      "Invalid asset identity",
    );
    assert(!names.has(asset.name), `Duplicate asset pin: ${asset.name}`);
    names.add(asset.name);
    await downloadAsset(destination, asset, toolchain);
  }
  assert(names.has(toolchain.archive), "Archive is not independently pinned");
  const archive = NodePath.join(destination, toolchain.archive);
  const members = await archiveInventory(archive, toolchain.root);
  const payload = NodePath.join(destination, toolchain.root);
  if (!NodeFS.existsSync(payload)) {
    const staging = await NodeFSP.mkdtemp(NodePath.join(destination, ".extract-"));
    try {
      command("tar", [
        "--zstd",
        "--extract",
        "--file",
        archive,
        "--directory",
        staging,
        "--no-same-owner",
        "--same-permissions",
        "--delay-directory-restore",
        "--keep-old-files",
      ]);
      await verifyExtracted(staging, members, toolchain.root);
      await NodeFSP.rename(NodePath.join(staging, toolchain.root), payload);
    } finally {
      await NodeFSP.rm(staging, { recursive: true, force: true });
    }
  }
  // Recheck complete content before execution, including cached installations.
  await verifyExtracted(destination, members, toolchain.root);
  const sources = JSON.parse(
    await NodeFSP.readFile(NodePath.join(destination, "source-components.json"), "utf8"),
  );
  assert(
    sources.version === toolchain.release &&
      sources.bluefe.commit === toolchain.compiler_revision &&
      sources.lustr.commit === toolchain.coordinator_revision,
    "Package source identities differ from application pin",
  );
  const platform = JSON.parse(
    await NodeFSP.readFile(NodePath.join(destination, "platform.json"), "utf8"),
  );
  const { root, adapter } = await hostTools(destination, payload, members, platform);
  const rustc = NodePath.join(root, "bin/rustc");
  const rustdoc = NodePath.join(root, "bin/rustdoc");
  const version = command(rustc, ["-vV"]);
  verifyVersion(version, toolchain);
  verifyVersion(command(rustdoc, ["-vV"]), toolchain);
  assert(
    (await NodeFSP.realpath(command(rustc, ["--print", "sysroot"]))) ===
      (await NodeFSP.realpath(root)),
    "Relocated compiler selected another sysroot",
  );
  assert(
    (await NodeFSP.stat(NodePath.join(root, "lib/rustlib", toolchain.host, "lib"))).isDirectory(),
    "Missing matching native sysroot",
  );
  const doctor = command(NodePath.join(root, "bin/lustr"), ["--bluefe-root", root, "doctor"]);
  const environment = {
    T3_CODE_LS_TOOLCHAIN: root,
    CARGO: NodePath.join(root, "bin/cargo"),
    RUSTC: rustc,
    RUSTDOC: rustdoc,
    CARGO_ENCODED_RUSTFLAGS: flagsForSysroot(root),
  };
  await NodeFSP.writeFile(
    NodePath.join(destination, "installation-proof.json"),
    `${JSON.stringify(
      {
        schema: 1,
        release: toolchain.release,
        compiler_revision: toolchain.compiler_revision,
        coordinator_revision: toolchain.coordinator_revision,
        payload,
        root,
        adapter,
        version,
        doctor,
        archive_sha256: toolchain.assets.find((asset) => asset.name === toolchain.archive).sha256,
        checked_members: members.size,
        scope: toolchain.scope,
      },
      null,
      2,
    )}\n`,
  );
  if (process.env.GITHUB_ENV)
    await NodeFSP.appendFile(
      process.env.GITHUB_ENV,
      Object.entries(environment)
        .map(([name, value]) => {
          const delimiter = `T3_CODE_LS_${NodeCrypto.randomUUID()}`;
          return `${name}<<${delimiter}\n${value}\n${delimiter}\n`;
        })
        .join(""),
    );
  if (process.env.GITHUB_OUTPUT)
    await NodeFSP.appendFile(process.env.GITHUB_OUTPUT, `root=${root}\n`);
  console.error(
    `Verified ${toolchain.release}; full Rust uses the compatibility compiler. Native Lust remains separate.`,
  );
  console.log(root);
}

if (
  process.argv[1] &&
  NodePath.resolve(process.argv[1]) === NodeURL.fileURLToPath(import.meta.url)
) {
  try {
    const args = process.argv.slice(2);
    assert(
      args.length === 2 && args[0] === "--destination",
      "Usage: node tools/install-toolchain.mjs --destination /absolute/versioned/directory",
    );
    await install(args[1]);
  } catch (error) {
    console.error(error.message);
    process.exitCode = 1;
  }
}
