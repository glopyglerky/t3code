#!/usr/bin/env node
import * as NodeAssert from "node:assert/strict";
import * as NodeChildProcess from "node:child_process";
import * as NodeCrypto from "node:crypto";
import * as NodeEvents from "node:events";
import * as NodeFS from "node:fs";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";

const source = NodeURL.fileURLToPath(import.meta.url);
const maximumRequest = 8 * 1024 * 1024;
const maximumPrompt = 4 * 1024 * 1024;
const maximumSchema = 1024 * 1024;
const maximumOutput = 4 * 1024 * 1024;
const maximumResponse = 24 * 1024 * 1024 + 1024;
const frame = (value) => `${JSON.stringify(value)}\n`;

async function notifyFixture(root, value) {
  const pending = NodePath.join(root, `${value.phase}.pending`);
  await NodeFSP.writeFile(pending, frame(value));
  await NodeFSP.rename(pending, NodePath.join(root, `${value.phase}.event.json`));
}

function awaitRelease(root) {
  return new Promise((resolve) => {
    const watcher = NodeFS.watch(root, (_event, name) => {
      if (name === "release") {
        watcher.close();
        resolve();
      }
    });
  });
}

async function fixture() {
  const mode = process.env.T3LS_GENERATION_FIXTURE;
  const phase = process.argv.includes("exec") ? "exec" : "mcp";
  const root = process.env.T3LS_GENERATION_ROOT;
  const args = process.argv.slice(2);
  const option = (name) => args[args.indexOf(name) + 1];
  const schemaPath = phase === "exec" ? option("--output-schema") : undefined;
  const outputPath = phase === "exec" ? option("--output-last-message") : undefined;
  const record = { phase, pid: process.pid, parentPid: process.ppid, args, schemaPath, outputPath };
  if (phase === "exec") {
    record.directoryMode = (await NodeFSP.stat(NodePath.dirname(schemaPath))).mode & 0o777;
    record.schemaMode = (await NodeFSP.stat(schemaPath)).mode & 0o777;
    record.outputMode = (await NodeFSP.stat(outputPath)).mode & 0o777;
    record.schemaBytes = (await NodeFSP.stat(schemaPath)).size;
  }
  await NodeFSP.writeFile(NodePath.join(root, `${phase}.json`), frame(record));
  const signalReady = (name) => notifyFixture(root, { ...record, phase: name });
  await signalReady(`observed-${phase}`);
  if (phase === "mcp") {
    if (mode === "mcp-exit") {
      process.stderr.write("controlled MCP failure\n");
      process.exitCode = 7;
    } else if (mode === "mcp-invalid-json") {
      process.stdout.write("not JSON");
    } else if (mode === "mcp-invalid-utf8") {
      process.stdout.write(Buffer.from([0xff, 0xfe]));
    } else if (mode === "mcp-limit") {
      process.stdout.write(" ".repeat(1024 * 1024 + 1));
    } else if (mode === "mcp-stderr-limit") {
      process.stderr.write("e".repeat(1024 * 1024 + 1));
    } else if (mode === "mcp-depth-limit") {
      process.stdout.write(
        `[{"name":"fixture.server","enabled":true,"extra":${"[".repeat(129)}0${"]".repeat(129)}}]`,
      );
    } else if (mode === "mcp-surrogate") {
      process.stdout.write('[{"name":"fixture.server","enabled":true,"extra":"\\ud800"}]');
    } else if (mode === "mcp-wait" || mode === "mcp-timeout") {
      const released = awaitRelease(root);
      await signalReady(mode);
      await released;
    } else {
      process.stdout.write('[{"name":"fixture.server","enabled":true}]');
    }
    return;
  }
  if (mode === "exec-wait" || mode === "exec-blocked-input") {
    if (mode === "exec-wait") process.stdin.resume();
    const released = awaitRelease(root);
    await signalReady(mode);
    await released;
    return;
  }
  const prompt = [];
  for await (const part of process.stdin) prompt.push(part);
  await NodeFSP.writeFile(
    NodePath.join(root, "prompt.json"),
    frame({ bytes: Buffer.concat(prompt).length }),
  );
  if (mode === "exec-descendant") {
    const child = NodeChildProcess.spawn(process.execPath, [source, "--descendant"], {
      env: { ...process.env, T3LS_GENERATION_FIXTURE: "descendant" },
      stdio: ["ignore", "inherit", "inherit"],
    });
    child.unref();
    await NodeFSP.writeFile(outputPath, '{"answer":"descendant"}');
    process.exit(0);
  }
  if (mode === "exec-exit") {
    process.stdout.write("controlled stdout failure");
    process.stderr.write("controlled stderr failure");
    process.exitCode = 9;
  } else if (mode === "exec-signal") {
    process.kill(process.pid, "SIGTERM");
  } else if (mode === "exec-stdout-limit") {
    process.stdout.write("x".repeat(maximumOutput + 1));
  } else if (mode === "exec-stderr-limit") {
    process.stderr.write("x".repeat(1024 * 1024 + 1));
  } else if (mode === "output-limit") {
    await NodeFSP.writeFile(outputPath, "x".repeat(maximumOutput + 1));
  } else if (mode === "output-missing") {
    await NodeFSP.unlink(outputPath);
  } else if (mode === "output-directory") {
    await NodeFSP.unlink(outputPath);
    await NodeFSP.mkdir(outputPath);
  } else if (mode === "output-symlink") {
    const target = NodePath.join(root, "controlled-target.json");
    await NodeFSP.writeFile(target, '{"answer":"outside the output file"}');
    await NodeFSP.unlink(outputPath);
    await NodeFSP.symlink(target, outputPath);
  } else if (mode === "output-fifo") {
    await NodeFSP.unlink(outputPath);
    NodeChildProcess.execFileSync("/usr/bin/mkfifo", [outputPath]);
  } else if (mode === "output-invalid-utf8") {
    await NodeFSP.writeFile(outputPath, Buffer.from([0xff, 0xfe]));
  } else if (mode === "output-exact-limit") {
    await NodeFSP.writeFile(outputPath, "x".repeat(maximumOutput));
  } else if (mode === "output-escaped-limit") {
    await NodeFSP.writeFile(outputPath, "\u0001".repeat(maximumOutput));
  } else {
    await NodeFSP.writeFile(outputPath, process.env.T3LS_GENERATION_OUTPUT ?? '{"answer":"ok"}\n');
  }
}

async function descendant() {
  const root = process.env.T3LS_GENERATION_ROOT;
  const released = awaitRelease(root);
  await notifyFixture(root, { phase: "descendant", pid: process.pid });
  await released;
  await notifyFixture(root, { phase: "descendant-released", pid: process.pid });
}

async function verify(executable, proofPath) {
  NodeAssert.ok(
    executable && NodePath.isAbsolute(executable),
    "Pass an absolute generation executable path",
  );
  const temporary = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "t3ls-generation-check-"));
  const fixturePath = NodePath.join(temporary, "codex-fixture.mjs");
  await NodeFSP.writeFile(
    fixturePath,
    `#!${process.execPath}\nimport ${JSON.stringify(NodeURL.pathToFileURL(source).href)};\n`,
    { mode: 0o700 },
  );
  await NodeFSP.symlink(fixturePath, NodePath.join(temporary, "codex"));
  const messages = [];
  const waiters = [];
  const nativeProcesses = new Set();
  const fixturePids = new Set();
  const watchers = new Set();
  const checks = [];
  const nextMessage = (phase) => {
    const index = messages.findIndex((message) => message.phase === phase);
    return index < 0
      ? new Promise((resolve, reject) => waiters.push({ phase, resolve, reject }))
      : Promise.resolve(messages.splice(index, 1)[0]);
  };
  let watchdogExpired = false;
  const watchdog = setTimeout(() => {
    watchdogExpired = true;
    for (const child of nativeProcesses) child.kill("SIGKILL");
    for (const pid of fixturePids) {
      try {
        process.kill(pid, "SIGKILL");
      } catch {}
    }
    for (const waiter of waiters.splice(0))
      waiter.reject(new Error("generation verification watchdog expired"));
    for (const watcher of watchers) watcher.close();
  }, 90_000);
  let serial = 0;
  async function scenario(mode = "normal", changes = {}, environment = {}) {
    const root = NodePath.join(temporary, `case-${++serial}`);
    const temps = NodePath.join(root, "temps");
    const cwd = NodePath.join(root, "workspace");
    await NodeFSP.mkdir(temps, { recursive: true });
    await NodeFSP.mkdir(cwd);
    const received = new Set();
    const watcher = NodeFS.watch(root, (_event, filename) => {
      if (!filename?.endsWith(".event.json") || received.has(filename)) return;
      received.add(filename);
      NodeFSP.readFile(NodePath.join(root, filename), "utf8")
        .then((data) => {
          const message = JSON.parse(data);
          fixturePids.add(message.pid);
          const index = waiters.findIndex((waiter) => waiter.phase === message.phase);
          if (index < 0) messages.push(message);
          else waiters.splice(index, 1)[0].resolve(message);
        })
        .catch((error) => {
          if (error.code !== "ENOENT") throw error;
        });
    });
    watchers.add(watcher);
    const request = {
      version: 1,
      operation: "run",
      binary: fixturePath,
      cwd,
      prompt: "Generate a concise answer. 💬",
      outputSchema: { type: "object", properties: { answer: { type: "string" } } },
      launchArgs: "",
      models: [],
      customModels: [],
      modelSelection: { model: "gpt-fixture", options: [] },
      managed: false,
      imagePaths: [],
      timeoutMs: 30_000,
      ...changes,
    };
    return {
      root,
      temps,
      watcher,
      request,
      env: {
        PATH: `${temporary}:${NodePath.dirname(process.execPath)}`,
        HOME: root,
        TMPDIR: temps,
        T3LS_GENERATION_FIXTURE: mode,
        T3LS_GENERATION_ROOT: root,
        ...environment,
      },
    };
  }
  function start(test, bytes = frame(test.request), args = []) {
    const child = NodeChildProcess.spawn(executable, args, {
      env: test.env,
      stdio: ["pipe", "pipe", "pipe"],
    });
    nativeProcesses.add(child);
    const stdout = [];
    const stderr = [];
    child.stdout.on("data", (data) => stdout.push(data));
    child.stderr.on("data", (data) => stderr.push(data));
    child.stdin.on("error", (error) => {
      if (error.code !== "EPIPE") throw error;
    });
    const finished = NodeEvents.once(child, "close").then(([status, signal]) => {
      nativeProcesses.delete(child);
      return { status, signal, stdout: Buffer.concat(stdout), stderr: Buffer.concat(stderr) };
    });
    child.stdin.end(bytes);
    return { child, finished };
  }
  const entered = (phase, running) =>
    Promise.race([
      nextMessage(phase),
      running.finished.then((result) => {
        throw new Error(`native process exited before ${phase}: ${result.status} ${result.stderr}`);
      }),
    ]);
  async function noRunningProcess(pid) {
    try {
      process.kill(pid, 0);
    } catch (error) {
      if (error.code === "ESRCH") return;
      throw error;
    }
    NodeAssert.fail(`owned fixture ${pid} has not been reaped`);
  }
  async function cleanup(test, { spawned = true } = {}) {
    test.watcher.close();
    watchers.delete(test.watcher);
    NodeAssert.deepEqual(await NodeFSP.readdir(test.temps), [], "native private files are removed");
    for (const phase of ["mcp", "exec"]) {
      try {
        const record = JSON.parse(
          await NodeFSP.readFile(NodePath.join(test.root, `${phase}.json`), "utf8"),
        );
        NodeAssert.ok(spawned, "invalid requests never spawn the provider");
        await noRunningProcess(record.pid);
        fixturePids.delete(record.pid);
        if (phase === "exec") {
          NodeAssert.equal(record.directoryMode, 0o700);
          NodeAssert.equal(record.schemaMode, 0o600);
          NodeAssert.equal(record.outputMode, 0o600);
        }
      } catch (error) {
        if (error.code !== "ENOENT") throw error;
      }
    }
  }
  function response(result) {
    NodeAssert.equal(result.status, 0, result.stderr.toString());
    NodeAssert.equal(result.signal, null);
    NodeAssert.equal(result.stderr.length, 0);
    NodeAssert.ok(result.stdout.length <= maximumResponse);
    const text = new TextDecoder("utf-8", { fatal: true }).decode(result.stdout);
    NodeAssert.ok(text.endsWith("\n"));
    NodeAssert.equal(text.indexOf("\n"), text.length - 1, "one complete response frame");
    const value = JSON.parse(text);
    NodeAssert.equal(value.version, 1);
    NodeAssert.deepEqual(
      Object.keys(value).sort(),
      value.ok === undefined ? ["error", "version"] : ["ok", "version"],
    );
    if (value.ok === undefined) {
      NodeAssert.deepEqual(Object.keys(value.error).sort(), ["detail", "kind"]);
      NodeAssert.ok(
        ["spawn", "io", "timeout", "limit", "exit", "output"].includes(value.error.kind),
      );
      NodeAssert.equal(typeof value.error.detail, "string");
    } else {
      NodeAssert.deepEqual(Object.keys(value.ok), ["output"]);
      NodeAssert.equal(typeof value.ok.output, "string");
    }
    return value;
  }
  async function rejected(label, mutate, args = []) {
    const test = await scenario();
    const bytes = mutate(test.request);
    const result = await start(test, bytes, args).finished;
    NodeAssert.equal(result.status, 2, `${label}: ${result.stderr}`);
    NodeAssert.equal(result.signal, null);
    NodeAssert.equal(result.stdout.length, 0, label);
    NodeAssert.match(result.stderr.toString(), /^t3-code-ls-generate: [^\n]+\n$/, label);
    await cleanup(test, { spawned: false });
    checks.push(label);
  }
  async function completed(label, mode = "normal", changes = {}, environment = {}) {
    const test = await scenario(mode, changes, environment);
    const result = response(await start(test).finished);
    await cleanup(test);
    checks.push(label);
    return { test, result };
  }
  try {
    const initial = await completed(
      "one complete request returns the raw structured output and removes private files",
    );
    NodeAssert.equal(initial.result.ok.output, '{"answer":"ok"}\n');
    for (const [label, mutate] of [
      ["empty input", () => ""],
      ["missing LF", (request) => JSON.stringify(request)],
      ["blank line", () => "\n"],
      ["second delimiter", (request) => frame(request) + "\n"],
      ["second request", (request) => frame(request) + frame(request)],
      ["extra trailing bytes", (request) => frame(request) + "x"],
      ["multiline request", (request) => JSON.stringify(request, null, 2) + "\n"],
      ["non-object request", () => "[]\n"],
      ["null request", () => "null\n"],
      ["byte order mark", (request) => "\uFEFF" + frame(request)],
      ["invalid UTF-8", () => Buffer.from([0xff, 0xfe, 10])],
      ["unpaired high surrogate", (request) => frame(request).replace("gpt-fixture", "\\ud800")],
      ["unpaired low surrogate", (request) => frame(request).replace("gpt-fixture", "\\udc00")],
      [
        "duplicate envelope field",
        (request) => frame(request).replace('"version":1', '"version":1,"version":1'),
      ],
      [
        "escaped duplicate envelope field",
        (request) => frame(request).replace('"version":1', '"version":1,"\\u0076ersion":1'),
      ],
      [
        "duplicate nested field",
        (request) =>
          frame(request).replace(
            '"model":"gpt-fixture"',
            '"model":"gpt-fixture","model":"gpt-fixture"',
          ),
      ],
      ["request frame over limit", (request) => " ".repeat(maximumRequest) + frame(request)],
    ])
      await rejected(label, mutate);
    await rejected("unexpected argument", frame, ["--help"]);
    const base = (await scenario()).request;
    for (const key of Object.keys(base))
      await rejected(`missing ${key}`, (request) => {
        delete request[key];
        return frame(request);
      });
    for (const [key, value] of [
      ["version", 2],
      ["version", "1"],
      ["operation", "upgrade"],
      ["binary", false],
      ["binary", "bad\0path"],
      ["cwd", "relative/path"],
      ["cwd", "bad\0path"],
      ["prompt", false],
      ["launchArgs", []],
      ["models", {}],
      ["models", [{ slug: "gpt-fixture", isCustom: false, isDefault: false }]],
      [
        "models",
        [{ slug: "gpt-fixture", isCustom: false, isDefault: false, isLegacy: false, extra: true }],
      ],
      ["modelSelection", { model: "gpt-fixture" }],
      ["modelSelection", { model: "gpt-fixture", options: [{ id: "fastMode", value: 1 }] }],
      [
        "modelSelection",
        { model: "gpt-fixture", options: [{ id: "fastMode", value: true, extra: false }] },
      ],
      ["managed", "false"],
      ["imagePaths", ["relative/image.png"]],
      ["imagePaths", ["/bad\0path"]],
      ["timeoutMs", 0],
      ["timeoutMs", -1],
      ["timeoutMs", 1.5],
      ["timeoutMs", 900001],
      ["timeoutMs", "1000"],
      ["extra", true],
      ["launchArgs", "x".repeat(65_537)],
      ["imagePaths", Array.from({ length: 4097 }, () => "/image.png")],
      [
        "models",
        Array.from({ length: 4097 }, () => ({
          slug: "model",
          isCustom: false,
          isDefault: false,
          isLegacy: false,
        })),
      ],
      [
        "modelSelection",
        {
          model: "model",
          options: Array.from({ length: 4097 }, () => ({ id: "reasoningEffort", value: "low" })),
        },
      ],
      ["customModels", Array(262_145).fill(0)],
      ["prompt", "x".repeat(maximumPrompt + 1)],
      ["outputSchema", { description: "x".repeat(maximumSchema) }],
    ])
      await rejected(`invalid ${key} case ${checks.length}`, (request) =>
        frame({ ...request, [key]: value }),
      );
    await rejected("JSON nesting limit", (request) => {
      let nested = null;
      for (let depth = 0; depth < 65; depth++) nested = [nested];
      return frame({ ...request, outputSchema: nested });
    });
    for (const whitespace of [" \t", "\r"]) {
      const test = await scenario();
      const bytes =
        whitespace === "\r"
          ? JSON.stringify(test.request) + "\r\n"
          : " \t" + JSON.stringify(test.request) + " \t\n";
      NodeAssert.equal(response(await start(test, bytes).finished).ok.output, '{"answer":"ok"}\n');
      await cleanup(test);
      checks.push("JSON whitespace and CRLF are accepted within one frame");
    }
    const exactRequest = await scenario();
    const shortFrame = frame(exactRequest.request);
    NodeAssert.equal(
      response(
        await start(
          exactRequest,
          " ".repeat(maximumRequest - Buffer.byteLength(shortFrame)) + shortFrame,
        ).finished,
      ).ok.output,
      '{"answer":"ok"}\n',
    );
    await cleanup(exactRequest);
    checks.push("exact maximum request frame is accepted");
    const exactPrompt = await completed("exact prompt limit is transmitted completely", "normal", {
      prompt: "x".repeat(maximumPrompt),
    });
    NodeAssert.equal(
      JSON.parse(
        await NodeFSP.readFile(NodePath.join(exactPrompt.test.root, "prompt.json"), "utf8"),
      ).bytes,
      maximumPrompt,
    );
    const exactSchema = {
      description: "x".repeat(
        maximumSchema - Buffer.byteLength(JSON.stringify({ description: "" })),
      ),
    };
    const schema = await completed("exact canonical schema limit is written completely", "normal", {
      outputSchema: exactSchema,
    });
    NodeAssert.equal(
      JSON.parse(await NodeFSP.readFile(NodePath.join(schema.test.root, "exec.json"), "utf8"))
        .schemaBytes,
      maximumSchema,
    );
    for (const mode of ["mcp-exit", "mcp-invalid-json"]) {
      const { test, result } = await completed(`${mode} preserves generation fallback`, mode);
      NodeAssert.equal(result.ok.output, '{"answer":"ok"}\n');
      const exec = JSON.parse(
        await NodeFSP.readFile(NodePath.join(test.root, "exec.json"), "utf8"),
      );
      NodeAssert.ok(!exec.args.some((arg) => arg.startsWith("mcp_servers=")));
    }
    for (const [mode, kind] of [
      ["mcp-invalid-utf8", "output"],
      ["mcp-limit", "limit"],
      ["mcp-stderr-limit", "limit"],
      ["mcp-depth-limit", "limit"],
      ["mcp-surrogate", "limit"],
      ["exec-exit", "exit"],
      ["exec-signal", "io"],
      ["exec-stdout-limit", "limit"],
      ["exec-stderr-limit", "limit"],
      ["output-limit", "limit"],
      ["output-missing", "output"],
      ["output-directory", "output"],
      ["output-symlink", "output"],
      ["output-fifo", "output"],
      ["output-invalid-utf8", "output"],
    ]) {
      const { test, result } = await completed(`${mode} fails and cleans private files`, mode);
      NodeAssert.equal(result.error.kind, kind);
      if (mode.startsWith("mcp-"))
        await NodeAssert.rejects(NodeFSP.access(NodePath.join(test.root, "exec.json")), {
          code: "ENOENT",
        });
    }
    NodeAssert.equal(
      (await completed("exact output file limit is returned", "output-exact-limit")).result.ok
        .output.length,
      maximumOutput,
    );
    NodeAssert.equal(
      (await completed("empty executable uses the Codex PATH default", "normal", { binary: "" }))
        .result.ok.output,
      '{"answer":"ok"}\n',
    );
    NodeAssert.equal(
      (
        await completed(
          "escaped output amplification fits bounded response",
          "output-escaped-limit",
        )
      ).result.ok.output,
      "\u0001".repeat(maximumOutput),
    );
    NodeAssert.equal(
      (
        await completed("missing executable fails without leaking files", "normal", {
          binary: NodePath.join(temporary, "missing"),
        })
      ).result.error.kind,
      "spawn",
    );
    NodeAssert.equal(
      (
        await completed(
          "inherited launch arguments have a byte bound",
          "normal",
          {},
          {
            T3CODE_CODEX_LAUNCH_ARGS: "x".repeat(65_537),
          },
        )
      ).result.error.kind,
      "limit",
    );
    const failedTemporary = await completed(
      "temporary directory creation failure leaves no provider process",
      "normal",
      {},
      { TMPDIR: fixturePath },
    );
    NodeAssert.equal(failedTemporary.result.error.kind, "io");
    await cleanup(failedTemporary.test, { spawned: false });
    for (const [mode, signal] of [
      ["mcp-wait", "SIGTERM"],
      ["exec-wait", "SIGINT"],
      ["exec-blocked-input", "SIGTERM"],
    ]) {
      const test = await scenario(
        mode,
        mode === "exec-blocked-input" ? { prompt: "x".repeat(maximumPrompt) } : {},
      );
      const running = start(test);
      const ready = await entered(mode, running);
      NodeAssert.ok(ready.pid > 0 && ready.pid !== process.pid);
      running.child.kill(signal);
      const result = await running.finished;
      NodeAssert.equal(result.signal, null);
      NodeAssert.equal(result.status, signal === "SIGINT" ? 130 : 143);
      NodeAssert.equal(result.stdout.length, 0);
      await noRunningProcess(ready.pid);
      await cleanup(test);
      checks.push(
        `${signal} during ${mode} kills and reaps the owned child and removes private files`,
      );
    }
    const timed = await scenario("exec-wait", { timeoutMs: 5000 });
    const timedRun = start(timed);
    const timedChild = await entered("exec-wait", timedRun);
    NodeAssert.equal(response(await timedRun.finished).error.kind, "timeout");
    await noRunningProcess(timedChild.pid);
    await cleanup(timed);
    checks.push("request deadline kills the active provider and removes private files");
    const preflight = await scenario("mcp-timeout");
    const preflightRun = start(preflight);
    const preflightChild = await entered("mcp-timeout", preflightRun);
    NodeAssert.equal(response(await preflightRun.finished).ok.output, '{"answer":"ok"}\n');
    await noRunningProcess(preflightChild.pid);
    await cleanup(preflight);
    checks.push("MCP timeout reaps preflight and proceeds to generation");
    const brokenOutput = await scenario("exec-wait");
    const brokenRun = start(brokenOutput);
    await entered("exec-wait", brokenRun);
    brokenRun.child.stdout.destroy();
    await NodeFSP.writeFile(NodePath.join(brokenOutput.root, "release"), "finish\n");
    const brokenResult = await brokenRun.finished;
    NodeAssert.equal(brokenResult.status, 2);
    NodeAssert.equal(brokenResult.signal, null);
    NodeAssert.match(
      brokenResult.stderr.toString(),
      /^t3-code-ls-generate: generation input or output failed\n$/,
    );
    await cleanup(brokenOutput);
    checks.push("a closed response pipe fails after provider and private-file cleanup");
    const inherited = await scenario("exec-descendant");
    const inheritedRun = start(inherited);
    const holder = await entered("descendant", inheritedRun);
    NodeAssert.equal(response(await inheritedRun.finished).ok.output, '{"answer":"descendant"}');
    await NodeFSP.writeFile(NodePath.join(inherited.root, "release"), "exit\n");
    await nextMessage("descendant-released");
    await cleanup(inherited);
    fixturePids.delete(holder.pid);
    checks.push(
      "a descendant holding stdout and stderr cannot hang completion after the owned child exits",
    );
    NodeAssert.equal(watchdogExpired, false);
    const proof = {
      version: 1,
      status: "passed",
      executableSha256: NodeCrypto.createHash("sha256")
        .update(await NodeFSP.readFile(executable))
        .digest("hex"),
      checks: checks.length,
      coverage: checks,
      boundaries: [
        "controlled Node fixtures only; no provider account or model request",
        "Linux process cleanup checks cover owned direct children; unrelated descendants are not claimed as reaped",
        "native output is returned as text; production Effect schemas validate the structured result",
      ],
    };
    if (proofPath) await NodeFSP.writeFile(proofPath, frame(proof));
    process.stdout.write(frame(proof));
  } finally {
    clearTimeout(watchdog);
    for (const child of nativeProcesses) child.kill("SIGKILL");
    for (const pid of fixturePids) {
      try {
        process.kill(pid, "SIGKILL");
      } catch {}
    }
    for (const watcher of watchers) watcher.close();
    await NodeFSP.rm(temporary, { recursive: true, force: true });
  }
}

if (process.env.T3LS_GENERATION_FIXTURE === "descendant") await descendant();
else if (process.env.T3LS_GENERATION_FIXTURE) await fixture();
else await verify(process.argv[2], process.argv[3]);
