/* oxlint-disable t3code/no-global-process-runtime -- Synchronous executable paths and standalone build scripts have no Effect runtime. */
import * as NodeOS from "node:os";
import * as NodeChildProcess from "node:child_process";
import * as NodeFS from "node:fs";
import * as NodeCrypto from "node:crypto";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";
import {
  nativeComponents,
  nativeFile,
  nativeHash,
  nativeTargets,
  nativeTargetQualified,
  nativeArtifactFiles,
  isQualifiedNativeHost,
  stageNativeArtifacts,
  stageUpstreamManifest,
} from "./native-artifacts.mjs";
import { flagsForSysroot, stockRustEnvironment } from "./install-toolchain.mjs";

const root = NodePath.resolve(NodePath.dirname(NodeURL.fileURLToPath(import.meta.url)), "..");
const destination = NodePath.resolve(root, process.argv[2] ?? "bin");
const targetKey = process.env.T3_CODE_LS_NATIVE_TARGET ?? `${NodeOS.platform()}-${NodeOS.arch()}`;
const rustTarget = nativeTargets[targetKey];
if (!rustTarget) throw new Error(`Unsupported native component target: ${targetKey}`);
const candidateOnly = process.env.T3_CODE_LS_NATIVE_COMPONENTS === "db";
if (process.env.T3_CODE_LS_NATIVE_COMPONENTS !== undefined && !candidateOnly)
  throw new Error("T3_CODE_LS_NATIVE_COMPONENTS supports only db or the complete default bundle");
const compilerRoute =
  process.env.T3_CODE_LS_COMPILER ?? (process.env.T3_CODE_LS_TOOLCHAIN ? "lustr" : "rustc");
if (!["rustc", "lustr"].includes(compilerRoute))
  throw new Error("T3_CODE_LS_COMPILER must be rustc or lustr");
const requestedImplementation = process.env.T3_CODE_LS_IMPLEMENTATION;
if (
  requestedImplementation !== undefined &&
  !["native", "upstream"].includes(requestedImplementation)
)
  throw new Error("T3_CODE_LS_IMPLEMENTATION must be native or upstream");
const incompatibleHost =
  targetKey === `${NodeOS.platform()}-${NodeOS.arch()}` && !isQualifiedNativeHost();
if (requestedImplementation === "upstream" && candidateOnly)
  throw new Error("Explicit upstream selection conflicts with a database candidate build");
// Service-helper qualification and database qualification are independent. A
// supported host must not silently return to SQLite because another helper is Unix-only.
const automaticDatabase =
  !candidateOnly &&
  requestedImplementation !== "upstream" &&
  (!nativeTargetQualified(targetKey) || incompatibleHost);
const databaseOnly = candidateOnly || automaticDatabase;
const upstreamSelection = requestedImplementation === "upstream" ? "explicit-upstream" : undefined;
const explicitHelper = Object.keys(process.env).find(
  (name) => /^T3_CODE_LS_.*_EXECUTABLE$/.test(name) && process.env[name] !== undefined,
);
if (
  upstreamSelection &&
  (process.env.T3_CODE_LS_TOOLCHAIN !== undefined ||
    process.env.T3_CODE_LS_SYSROOT !== undefined ||
    explicitHelper ||
    requestedImplementation === "native")
) {
  throw new Error(
    "Explicit upstream service selection conflicts with native compiler/helper settings",
  );
}
if (process.env.T3_CODE_LS_NATIVE_DIR) {
  const selected = JSON.parse(
    NodeFS.readFileSync(NodePath.join(process.env.T3_CODE_LS_NATIVE_DIR, "manifest.json"), "utf8"),
  );
  if (
    (requestedImplementation === "upstream" &&
      !databaseOnly &&
      selected.implementation !== "upstream") ||
    (requestedImplementation === "native" && selected.implementation === "upstream") ||
    (incompatibleHost &&
      !databaseOnly &&
      selected.implementation !== "upstream" &&
      selected.implementation !== "database-only")
  )
    throw new Error(
      "Selected component bundle conflicts with the requested implementation or host qualification",
    );
  if (selected.implementation === "database-only") {
    const head = NodeChildProcess.execFileSync("git", ["rev-parse", "HEAD"], {
      cwd: root,
      encoding: "utf8",
    }).trim();
    if (selected.sourceCommit !== head)
      throw new Error("The accepted database bundle belongs to another application revision");
  }
  stageNativeArtifacts(NodePath.resolve(process.env.T3_CODE_LS_NATIVE_DIR), destination, targetKey);
} else if (upstreamSelection) {
  stageUpstreamManifest(destination, targetKey, upstreamSelection);
} else {
  const git = (...args) =>
    NodeChildProcess.execFileSync("git", args, { cwd: root, encoding: "utf8" }).trim();
  let sourceCommit;
  let cacheDirectory;
  let automaticRoot;
  let buildDestination = destination;
  if (databaseOnly) {
    if (targetKey !== `${NodeOS.platform()}-${NodeOS.arch()}`)
      throw new Error(
        `Cannot qualify ${targetKey} on this host; supply a target-tested bundle with T3_CODE_LS_NATIVE_DIR`,
      );
    sourceCommit = git("rev-parse", "HEAD");
    if (git("status", "--porcelain", "--untracked-files=normal") !== "")
      throw new Error(
        "Database acceptance requires clean committed source; commit the candidate or supply a same-revision accepted bundle with T3_CODE_LS_NATIVE_DIR",
      );
  }
  if (automaticDatabase) {
    automaticRoot = NodePath.join(
      NodeOS.tmpdir(),
      "t3code-ls-native",
      NodeCrypto.createHash("sha256").update(root).digest("hex").slice(0, 16),
    );
    NodeFS.mkdirSync(automaticRoot, { recursive: true });
    cacheDirectory = NodePath.join(automaticRoot, "accepted", targetKey, sourceCommit);
    if (NodeFS.existsSync(NodePath.join(cacheDirectory, "manifest.json"))) {
      const cached = JSON.parse(
        NodeFS.readFileSync(NodePath.join(cacheDirectory, "manifest.json"), "utf8"),
      );
      if (cached.sourceCommit !== sourceCommit)
        throw new Error("The cached database bundle belongs to another application revision");
      nativeArtifactFiles(cacheDirectory, targetKey);
      stageNativeArtifacts(cacheDirectory, destination, targetKey);
      console.log(
        `Staged the accepted ${targetKey} database; other helpers retain upstream behavior.`,
      );
      process.exit(0);
    }
    buildDestination = NodeFS.mkdtempSync(NodePath.join(automaticRoot, "candidate-"));
    process.env.CARGO_TARGET_DIR ??= NodePath.join(automaticRoot, "cargo");
  }
  const toolchain = process.env.T3_CODE_LS_TOOLCHAIN;
  if (compilerRoute === "lustr" && !toolchain)
    throw new Error(
      "Set T3_CODE_LS_TOOLCHAIN to the qualified Lustr/BlueFe toolchain directory, or T3_CODE_LS_NATIVE_DIR to matching produced components",
    );
  if (process.env.CARGO_BUILD_TARGET && process.env.CARGO_BUILD_TARGET !== rustTarget) {
    throw new Error(`CARGO_BUILD_TARGET must match ${rustTarget}`);
  }
  const requestedTarget =
    process.env.CARGO_TARGET_DIR ??
    NodePath.join(
      NodeOS.tmpdir(),
      "t3code-ls-native",
      NodeCrypto.createHash("sha256").update(root).digest("hex").slice(0, 16),
      "cargo",
    );
  process.env.CARGO_TARGET_DIR = requestedTarget;
  if (!requestedTarget || !NodePath.isAbsolute(requestedTarget))
    throw new Error(
      "Set CARGO_TARGET_DIR to an absolute generated-output directory outside the checkout",
    );
  NodeFS.mkdirSync(requestedTarget, { recursive: true });
  const target = NodeFS.realpathSync(requestedTarget);
  const canonicalRoot = NodeFS.realpathSync(root);
  if (target === canonicalRoot || target.startsWith(`${canonicalRoot}${NodePath.sep}`))
    throw new Error("CARGO_TARGET_DIR must be outside the source checkout");
  const flags =
    process.env.CARGO_ENCODED_RUSTFLAGS?.split("\x1f") ??
    process.env.RUSTFLAGS?.match(/(?:[^\s"']+|"[^"]*"|'[^']*')+/g)?.map((flag) =>
      flag.replace(/^(["'])(.*)\1$/, "$2"),
    ) ??
    [];
  const subsetDirectory = process.env.T3_CODE_LS_LUST_DIR;
  const subsetReceiptPath = process.env.T3_CODE_LS_LUST_RECEIPT;
  let subset;
  if (!databaseOnly && (subsetDirectory || subsetReceiptPath)) {
    if (!subsetDirectory || !subsetReceiptPath || targetKey !== "linux-x64")
      throw new Error("Native Lust components require a Linux x64 directory and matching receipt");
    const receipt = JSON.parse(NodeFS.readFileSync(subsetReceiptPath, "utf8"));
    const pin = JSON.parse(NodeFS.readFileSync(NodePath.join(root, "lust-toolchain.json"), "utf8"));
    const head = NodeChildProcess.execFileSync("git", ["rev-parse", "HEAD"], {
      cwd: root,
      encoding: "utf8",
    }).trim();
    if (
      receipt.status !== "passed" ||
      receipt.workspace_git_revision !== head ||
      receipt.compiler?.revision !== pin.revision ||
      !receipt.inputs ||
      !Array.isArray(receipt.artifacts)
    )
      throw new Error("Native Lust receipt does not qualify this application revision");
    for (const [path, expected] of Object.entries(receipt.inputs)) {
      const content = path.startsWith("git:")
        ? NodeChildProcess.execFileSync(
            "git",
            ["--no-replace-objects", "cat-file", "blob", path.slice(4)],
            { cwd: root },
          )
        : NodeFS.readFileSync(NodePath.join(root, path));
      if (NodeCrypto.createHash("sha256").update(content).digest("hex") !== expected)
        throw new Error(`Native Lust acceptance input changed: ${path}`);
    }
    subset = { directory: subsetDirectory, receipt };
    for (const component of nativeComponents.filter((component) => component !== "db")) {
      const file = nativeFile(component, targetKey);
      const accepted = receipt.artifacts.find((artifact) => artifact.file === file);
      if (!accepted || nativeHash(NodePath.join(subsetDirectory, file)) !== accepted.sha256)
        throw new Error(`Native Lust artifact hash mismatch: ${file}`);
    }
  }
  const env = { ...process.env };
  let program;
  let prefix;
  let compiler;
  const output = (program, args) =>
    NodeChildProcess.execFileSync(program, args, {
      cwd: root,
      env,
      encoding: "utf8",
      timeout: 30_000,
    }).trim();
  if (compilerRoute === "lustr") {
    const sysroot = NodePath.resolve(process.env.T3_CODE_LS_SYSROOT ?? toolchain);
    if (!NodeFS.existsSync(NodePath.resolve(sysroot, "lib/rustlib", rustTarget, "lib")))
      throw new Error(`Missing produced target sysroot for ${rustTarget}: set T3_CODE_LS_SYSROOT`);
    Object.assign(env, {
      CARGO: NodePath.resolve(toolchain, "bin/cargo"),
      RUSTC: NodePath.resolve(toolchain, "bin/rustc"),
      RUSTDOC: NodePath.resolve(toolchain, "bin/rustdoc"),
      CARGO_ENCODED_RUSTFLAGS: flagsForSysroot(sysroot, flags.join("\x1f")),
    });
    program = NodePath.resolve(toolchain, "bin/lustr");
    prefix = ["--bluefe-root", toolchain];
    compiler = {
      route: "bluefe-rustc-via-lustr",
      toolchainLockSha256: nativeHash(NodePath.join(root, "toolchain.lock.json")),
    };
  } else {
    if (toolchain || process.env.T3_CODE_LS_SYSROOT)
      throw new Error("Stock rustc selection conflicts with a Lustr compiler or sysroot override");
    const channel = NodeFS.readFileSync(NodePath.join(root, "rust-toolchain.toml"), "utf8").match(
      /^channel\s*=\s*"(\d+\.\d+\.\d+)"\s*$/m,
    )?.[1];
    if (!channel) throw new Error("Pin a complete Rust release in rust-toolchain.toml");
    const rustc = output("rustup", ["which", "--toolchain", channel, "rustc"]);
    const cargo = output("rustup", ["which", "--toolchain", channel, "cargo"]);
    const rustdoc = output("rustup", ["which", "--toolchain", channel, "rustdoc"]);
    Object.assign(
      env,
      stockRustEnvironment(env, {
        tools: { RUSTC: rustc, CARGO: cargo, RUSTDOC: rustdoc },
        sysroot: output(rustc, ["--print", "sysroot"]),
        release: channel,
        target: rustTarget,
      }),
    );
    const version = output(rustc, ["-vV"]).replace(/\r\n/g, "\n");
    if (!version.split("\n").includes(`release: ${channel}`))
      throw new Error("Installed rustc does not match rust-toolchain.toml");
    const host = version.match(/^host: (.+)$/m)?.[1];
    if (host !== nativeTargets[`${NodeOS.platform()}-${NodeOS.arch()}`])
      throw new Error("Installed rustc does not match the actual build host");
    const targetLibrary = output(rustc, ["--print", "target-libdir", "--target", rustTarget]);
    if (!NodeFS.existsSync(targetLibrary))
      throw new Error(`Install the pinned ${rustTarget} Rust target`);
    program = cargo;
    prefix = [];
    compiler = {
      route: "rustc",
      toolchain: channel,
      version,
      cargoVersion: output(cargo, ["-V"]),
      executableSha256: nativeHash(rustc),
    };
  }
  NodeChildProcess.execFileSync(
    program,
    [
      ...prefix,
      "build",
      "--locked",
      "--release",
      "--target",
      rustTarget,
      "--target-dir",
      target,
      ...(!databaseOnly && !subset ? ["-p", "t3-code-ls-core"] : []),
      "-p",
      "t3-code-ls-storage",
      ...(databaseOnly || subset ? ["--bin", "t3-code-ls-db"] : ["--bins"]),
    ],
    { cwd: root, env, stdio: "inherit" },
  );
  if (
    databaseOnly &&
    (git("rev-parse", "HEAD") !== sourceCommit ||
      git("status", "--porcelain", "--untracked-files=normal") !== "")
  )
    throw new Error("Application source changed while building the database candidate");
  NodeFS.mkdirSync(buildDestination, { recursive: true });
  const files = {};
  const componentCompilers = {};
  for (const component of databaseOnly ? ["db"] : nativeComponents) {
    const file = nativeFile(component, targetKey);
    const input =
      subset && component !== "db"
        ? NodePath.join(subset.directory, file)
        : NodePath.resolve(target, rustTarget, "release", file);
    NodeFS.copyFileSync(input, NodePath.resolve(buildDestination, file));
    files[file] = nativeHash(NodePath.resolve(buildDestination, file));
    componentCompilers[component] =
      subset && component !== "db"
        ? {
            route: "lust-native",
            revision: subset.receipt.compiler.revision,
            compilerSha256: subset.receipt.compiler_binary_sha256,
            coreSha256: subset.receipt.compiler_core_sha256,
            receiptSha256: nativeHash(subsetReceiptPath),
          }
        : compiler;
  }
  NodeFS.writeFileSync(
    NodePath.resolve(buildDestination, "manifest.json"),
    JSON.stringify(
      {
        target: targetKey,
        rustTarget,
        files,
        compiler,
        componentCompilers,
        ...(databaseOnly
          ? {
              schema: 2,
              implementation: "database-only",
              sourceCommit,
              upstreamCommit: JSON.parse(
                NodeFS.readFileSync(NodePath.join(root, "upstream.lock.json"), "utf8"),
              ).upstream.commit,
              databaseAcceptance: { status: "candidate" },
            }
          : {}),
      },
      null,
      2,
    ) + "\n",
  );
  if (automaticDatabase) {
    const runtime =
      process.env.T3_CODE_LS_TEST_RUNTIME ?? NodePath.join(automaticRoot, "test-runtime");
    const proof = NodePath.join(buildDestination, "acceptance-proof");
    const acceptanceEnv = {
      ...env,
      T3_CODE_LS_TEST_RUNTIME: runtime,
      T3_CODE_LS_TEST_TMPDIR: proof,
    };
    const upstream = JSON.parse(
      NodeFS.readFileSync(NodePath.join(root, "upstream.lock.json"), "utf8"),
    ).upstream.commit;
    if (
      NodeChildProcess.spawnSync("git", ["cat-file", "-e", `${upstream}^{commit}`], {
        cwd: root,
        stdio: "ignore",
      }).status !== 0
    )
      NodeChildProcess.execFileSync(
        "git",
        ["fetch", "--no-tags", "--depth=1", "https://github.com/pingdotgg/t3code.git", upstream],
        { cwd: root, stdio: "inherit", env: { ...env, GIT_TERMINAL_PROMPT: "0" } },
      );
    console.log(
      `Qualifying the ${targetKey} database before selecting it as the application default.`,
    );
    NodeChildProcess.execFileSync(
      process.execPath,
      [NodePath.join(root, "tools/install-server-tests.mjs")],
      { cwd: root, env: acceptanceEnv, stdio: "inherit" },
    );
    NodeChildProcess.execFileSync(
      process.execPath,
      [NodePath.join(root, "tools/check-database-platform.mjs"), buildDestination, proof],
      { cwd: root, env: acceptanceEnv, stdio: "inherit" },
    );
    const packageEnv = { ...acceptanceEnv };
    delete packageEnv.T3_CODE_LS_IMPLEMENTATION;
    NodeChildProcess.execFileSync(
      process.execPath,
      [
        NodePath.join(root, "tools/check-database-package.mjs"),
        buildDestination,
        NodePath.join(proof, "package.json"),
      ],
      { cwd: root, env: packageEnv, stdio: "inherit" },
    );
    stageNativeArtifacts(buildDestination, cacheDirectory, targetKey);
    stageNativeArtifacts(cacheDirectory, destination, targetKey);
    console.log(
      `Staged the accepted ${targetKey} database; other helpers retain upstream behavior.`,
    );
  }
}
