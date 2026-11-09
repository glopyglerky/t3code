#!/usr/bin/env node
import * as NodeAssert from "node:assert/strict";
import * as NodeChildProcess from "node:child_process";
import * as NodeEvents from "node:events";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";
import { upstreamSource } from "./upstream-source.mjs";

const script = NodeURL.fileURLToPath(import.meta.url);

function fixture() {
  const mode = process.env.T3LS_FIXTURE;
  if (process.env.T3LS_EXPECT_ENV) {
    const actual = { ...process.env };
    delete actual.T3LS_EXPECT_ENV;
    NodeAssert.deepEqual(actual, JSON.parse(process.env.T3LS_EXPECT_ENV));
    NodeAssert.equal(process.cwd(), process.env.T3LS_EXPECT_CWD);
  } else {
    NodeAssert.equal(process.env.HOME, process.env.T3LS_EXPECT_HOME);
    NodeAssert.equal(process.env.CODEX_HOME, NodePath.join(process.env.HOME, ".codex"));
    NodeAssert.equal(process.env.T3LS_PARENT_SECRET, undefined);
  }
  const write = (message) => process.stdout.write(`${JSON.stringify(message)}\n`);
  const respond = (id, result) => write({ id, result });
  let held;
  let count = 0;
  let pendingApproval;
  const diagnostic =
    mode === "inherit-stderr"
      ? "upstream-provider-fixture-diagnostic\n"
      : "sensitive-stderr-sentinel".repeat(12000);
  process.stderr.write(diagnostic, () => {
    write({ method: "fixture/ready", params: { pid: process.pid } });
  });
  if (mode === "upstream") {
    import(NodeURL.pathToFileURL(process.env.T3LS_UPSTREAM_PEER).href).catch((error) => {
      process.stderr.write(String(error));
      process.exitCode = 1;
    });
    return;
  }
  if (mode === "stubborn" || mode === "blocked-write") {
    setInterval(() => {}, 1000);
    return;
  }
  let buffer = "";
  process.stdin.setEncoding("utf8");
  process.stdin.on("data", (chunk) => {
    buffer += chunk;
    for (;;) {
      const newline = buffer.indexOf("\n");
      if (newline < 0) break;
      const line = buffer.slice(0, newline);
      buffer = buffer.slice(newline + 1);
      if (!line.trim()) continue;
      const message = JSON.parse(line);
      if (mode === "malformed" || mode === "truncated" || mode === "exit") {
        if (++count < 2) continue;
        if (mode === "malformed") process.stdout.write("{not json\n");
        if (mode === "truncated") process.stdout.write('{"id":');
        if (mode !== "malformed") process.stdout.end(() => process.exit(mode === "exit" ? 7 : 0));
        continue;
      }
      if (mode === "oversized") {
        process.stdout.write("x".repeat(257));
        continue;
      }
      if (mode === "invalid-utf8") {
        process.stdout.write(Buffer.from([255, 10]));
        continue;
      }
      if (mode === "bad-blank") {
        process.stdout.write("\u0085\n");
        continue;
      }
      if (mode === "bom-blank") {
        process.stdout.write("\ufeff\n");
        respond(message.id, { ok: true });
        continue;
      }
      if (mode === "final-line") {
        process.stdout.end(JSON.stringify({ id: message.id, result: { complete: true } }), () =>
          process.exit(0),
        );
        continue;
      }
      if (message.method === "initialize") {
        NodeAssert.equal(message.params.clientInfo.name, "t3-code-ls-fixture");
        respond(message.id, {
          userAgent: "deterministic-codex-app-server",
          platformFamily: "unix",
        });
      } else if (message.method === "initialized") {
        process.stdout.write(" \r\n");
        write({
          method: "item/agentMessage/delta",
          params: { delta: "ready ✓", threadId: "thread-1" },
        });
      } else if (message.method === "fixture/hold" || message.method === "fixture/first") {
        held = message.id;
      } else if (message.method === "fixture/second") {
        respond(message.id, { order: 1 });
        respond(held, { order: 2 });
      } else if (message.method === "fixture/release") {
        respond(held, { late: true });
      } else if (message.method === "skills/list") {
        pendingApproval = message.id;
        write({ id: 10000, method: "item/tool/requestUserInput", params: { questions: [] } });
      } else if (message.method === "fixture/resolved") {
        write({ id: 10001, method: "item/tool/requestUserInput", params: { questions: [] } });
        write({ method: "serverRequest/resolved", params: { requestId: 10001 } });
        respond(message.id, {});
      } else if (message.method === "fixture/duplicate-server-approval") {
        // JS String(id) identifies numeric and string approval IDs as the same request.
        write({ id: 10010, method: "item/tool/requestUserInput", params: { questions: [] } });
        write({ id: "10010", method: "item/tool/requestUserInput", params: { questions: [] } });
        respond(message.id, {});
      } else if (message.method === "fixture/resolved-string-id") {
        write({ id: 10011, method: "item/tool/requestUserInput", params: { questions: [] } });
        write({ method: "serverRequest/resolved", params: { requestId: "10011" } });
        write({ id: 10012, method: "item/tool/requestUserInput", params: { questions: [] } });
        respond(message.id, {});
      } else if (message.method === "fixture/approval-burst") {
        for (let index = 0; index < 8; index += 1)
          write({
            id: 10000 + index,
            method: "item/tool/requestUserInput",
            params: { questions: [] },
          });
        respond(message.id, { count: 8 });
      } else if (message.method === "fixture/unknown-response") {
        respond("unknown", { ignored: true });
        respond(message.id, { ok: true });
      } else if (message.method === "fixture/burst") {
        const lines = [];
        for (let index = 0; index < 1000; index += 1)
          lines.push(JSON.stringify({ method: "fixture/delta", params: { index } }));
        lines.push(JSON.stringify({ id: message.id, result: { count: 1000 } }));
        process.stdout.write(`${lines.join("\n")}\n`);
      } else if (message.method === "fixture/big-output") {
        write({ method: "fixture/large", params: "x".repeat(3 * 1024 * 1024) });
      } else if (message.method === "fixture/exit") {
        process.stdout.end(() => process.exit(message.params.code));
      } else if (message.method === "fixture/signal") {
        process.kill(process.pid, message.params.signal);
      } else if (message.method === "fixture/error") {
        write({
          id: message.id,
          error: { code: -32601, message: "unsupported method", data: { detail: "kept" } },
        });
      } else if (message.method === "fixture/missing-result") {
        write({ id: message.id });
      } else if (message.method === "account/read" || message.method === "fixture/echo") {
        respond(message.id, { account: null, requiresOpenaiAuth: false });
      } else if (message.method === "fixture/numeric-response") {
        respond(Number(message.id), { converted: true });
      } else if (message.id === 10000 && !("method" in message)) {
        NodeAssert.deepEqual(message.result, { answers: {} });
        respond(pendingApproval, { data: [{ cwd: process.cwd(), skills: [], errors: [] }] });
        write({ method: "serverRequest/resolved", params: { requestId: 10000 } });
      } else if (message.id === 10001 && !("method" in message)) {
        // An approval can be answered after the server has already resolved it.
      } else {
        NodeAssert.fail(`Unexpected fixture method: ${message.method}`);
      }
    }
  });
  process.stdin.on("end", () => process.stdout.end(() => process.exit(0)));
}

async function verify(binary) {
  const temporary = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "t3ls-provider-"));
  const home = NodePath.join(temporary, "home");
  await NodeFSP.mkdir(home);
  const managedHome = NodePath.join(temporary, "managed codex home");
  const alternateHome = NodePath.join(temporary, "alternate home");
  const workspace = NodePath.join(temporary, "selected workspace");
  for (const directory of [managedHome, alternateHome, workspace]) await NodeFSP.mkdir(directory);
  await NodeFSP.symlink(process.execPath, NodePath.join(workspace, "provider-fixture"));
  const upstreamPeer = NodePath.join(temporary, "upstream-peer.ts");
  await NodeFSP.writeFile(
    upstreamPeer,
    upstreamSource("packages/effect-codex-app-server/test/fixtures/codex-app-server-mock-peer.ts"),
  );
  const processes = new Set();
  const providers = new Set();
  let expired = false;
  const watchdog = setTimeout(() => {
    expired = true;
    for (const child of processes) child.kill("SIGKILL");
    for (const pid of providers) {
      try {
        process.kill(pid, "SIGKILL");
      } catch {}
    }
  }, 30000);
  let assertions = 0;

  async function start(mode, extra = [], options = {}) {
    const cwd = options.cwd ?? temporary;
    const environment = options.inheritedEnvironment
      ? {
          ...options.inheritedEnvironment,
          T3LS_FIXTURE: mode,
          T3LS_EXPECT_CWD: cwd,
          T3LS_UPSTREAM_PEER: upstreamPeer,
        }
      : { ...process.env, T3LS_PARENT_SECRET: "must-not-reach-child" };
    if (options.inheritedEnvironment) {
      environment.T3LS_EXPECT_ENV = JSON.stringify({
        ...environment,
        ...(options.homeOverride ? { HOME: options.homeOverride } : {}),
      });
    }
    const environmentArgs = options.inheritedEnvironment
      ? ["--inherit-env", ...(options.homeOverride ? ["--home", options.homeOverride] : [])]
      : [
          "--home",
          home,
          "--env",
          `T3LS_FIXTURE=${mode}`,
          "--env",
          `T3LS_EXPECT_HOME=${home}`,
          "--env",
          `T3LS_UPSTREAM_PEER=${upstreamPeer}`,
        ];
    const child = NodeChildProcess.spawn(
      binary,
      [
        "--program",
        options.program ?? process.execPath,
        "--cwd",
        cwd,
        ...environmentArgs,
        ...extra,
        "--",
        script,
        "--fixture",
      ],
      { env: environment, stdio: ["pipe", "pipe", "pipe"] },
    );
    processes.add(child);
    const exited = NodeEvents.once(child, "exit");
    let stderr = "";
    child.stderr.setEncoding("utf8");
    child.stderr.on("data", (chunk) => {
      stderr += chunk;
      NodeAssert.ok(stderr.length < 65536);
    });
    child.stdin.on("error", () => {});
    let tail = "";
    const messages = [];
    const waiters = [];
    let ended = false;
    let exit;
    const completion = new Promise((resolve, reject) => {
      child.once("error", reject);
      child.once("close", (code, signal) => {
        ended = true;
        exit = { code, signal, stderr };
        processes.delete(child);
        for (const waiter of waiters.splice(0))
          waiter.reject(new Error(`Provider closed: ${JSON.stringify(exit)}`));
        resolve(exit);
      });
    });
    child.stdout.setEncoding("utf8");
    child.stdout.on("data", (chunk) => {
      tail += chunk;
      for (;;) {
        const newline = tail.indexOf("\n");
        if (newline < 0) break;
        const frame = tail.slice(0, newline);
        tail = tail.slice(newline + 1);
        const message = JSON.parse(frame);
        messages.push(message);
        const index = waiters.findIndex((waiter) => waiter.predicate(message));
        if (index >= 0) waiters.splice(index, 1)[0].resolve(message);
      }
    });
    const wait = (predicate) => {
      const found = messages.find(predicate);
      if (found) return Promise.resolve(found);
      if (ended)
        return Promise.reject(new Error(`Provider already closed: ${JSON.stringify(exit)}`));
      return new Promise((resolve, reject) => waiters.push({ predicate, resolve, reject }));
    };
    const ready = await wait((message) => message.method === "fixture/ready");
    const providerPid = ready.params.pid;
    providers.add(providerPid);
    return {
      child,
      messages,
      wait,
      completion,
      exited,
      providerPid,
      send: (message) => child.stdin.write(`${JSON.stringify(message)}\n`),
      response: (id) =>
        wait(
          (message) =>
            "id" in message && String(message.id) === String(id) && !("method" in message),
        ),
      async finish(expectedCode = 0) {
        child.stdin.end();
        const result = await completion;
        NodeAssert.equal(result.signal, null, JSON.stringify(result));
        NodeAssert.equal(result.code, expectedCode, JSON.stringify(result));
        NodeAssert.ok(!result.stderr.includes("sensitive-stderr-sentinel"));
        NodeAssert.throws(() => process.kill(providerPid, 0), { code: "ESRCH" });
        providers.delete(providerPid);
        assertions += 3;
        return result;
      },
    };
  }

  try {
    const selectedEnvironment = {
      PATH: workspace,
      HOME: home,
      CODEX_HOME: managedHome,
      XDG_CONFIG_HOME: NodePath.join(temporary, "selected configuration"),
      T3LS_SYNTHETIC_SECRET: "synthetic-only-provider-secret-with-newline\nsecond-line",
    };
    for (const options of [
      { inheritedEnvironment: selectedEnvironment, program: "provider-fixture", cwd: workspace },
      {
        inheritedEnvironment: { PATH: workspace, CODEX_HOME: managedHome },
        program: "./provider-fixture",
        cwd: workspace,
      },
      { inheritedEnvironment: selectedEnvironment, homeOverride: alternateHome },
    ]) {
      const peer = await start("inherited-environment", [], options);
      NodeAssert.ok(
        !peer.child.spawnargs.some((argument) =>
          argument.includes(selectedEnvironment.T3LS_SYNTHETIC_SECRET),
        ),
      );
      peer.send({ id: 1, method: "fixture/echo" });
      NodeAssert.equal((await peer.response(1)).result.requiresOpenaiAuth, false);
      assertions += 2;
      await peer.finish();
    }

    const stderr = await start("inherit-stderr", ["--inherit-stderr"]);
    stderr.send({ id: 1, method: "fixture/echo" });
    await stderr.response(1);
    NodeAssert.match((await stderr.finish()).stderr, /upstream-provider-fixture-diagnostic/);
    assertions += 1;

    const parentDeadline = await start("normal", ["--request-timeout-ms", "0"]);
    parentDeadline.send({ id: 1, method: "fixture/hold" });
    parentDeadline.send({ id: 2, method: "fixture/burst" });
    await parentDeadline.response(2);
    parentDeadline.send({ method: "fixture/release" });
    NodeAssert.equal((await parentDeadline.response(1)).result.late, true);
    assertions += 1;
    await parentDeadline.finish();

    const parentProtocol = await start(
      "inherit-stderr",
      ["--inherit-stderr", "--parent-protocol", "--request-timeout-ms", "0"],
      { inheritedEnvironment: { PATH: workspace, CODEX_HOME: managedHome } },
    );
    parentProtocol.send({ id: 1, method: "fixture/error" });
    NodeAssert.deepEqual((await parentProtocol.response(1)).error, {
      code: -32601,
      message: "unsupported method",
      data: { detail: "kept" },
    });
    parentProtocol.send({ id: 2, method: "fixture/hold" });
    parentProtocol.send({ id: 3, method: "fixture/exit", params: { code: 42 } });
    await parentProtocol.completion;
    NodeAssert.deepEqual(
      parentProtocol.messages.filter((message) => message.id === 2 || message.id === 3),
      [],
    );
    NodeAssert.equal(
      (await parentProtocol.finish(42)).stderr,
      "upstream-provider-fixture-diagnostic\n",
    );
    assertions += 3;

    const parentCleanExit = await start("normal", ["--parent-protocol"]);
    parentCleanExit.send({ id: 4, method: "fixture/hold" });
    parentCleanExit.send({ id: 5, method: "fixture/exit", params: { code: 0 } });
    await parentCleanExit.completion;
    NodeAssert.deepEqual(
      parentCleanExit.messages.filter((message) => "id" in message),
      [],
    );
    await parentCleanExit.finish();
    assertions += 1;

    const parentFinalFrame = await start("final-line", ["--parent-protocol"]);
    parentFinalFrame.send({ id: 6, method: "fixture/echo" });
    await parentFinalFrame.completion;
    NodeAssert.deepEqual(parentFinalFrame.messages.find((message) => message.id === 6)?.result, {
      complete: true,
    });
    await parentFinalFrame.finish();
    assertions += 1;

    await NodeAssert.rejects(
      start("normal", ["--parent-protocol", "--request-timeout-ms", "100"]),
      /cannot use a local request deadline/,
    );
    assertions += 1;

    const parentCorrelation = await start("normal", ["--parent-protocol", "--max-pending", "2"]);
    for (let id = 80; id < 88; id += 1) parentCorrelation.send({ id, method: "fixture/hold" });
    parentCorrelation.send({ id: 90, method: "fixture/unknown-response" });
    NodeAssert.deepEqual((await parentCorrelation.response("unknown")).result, { ignored: true });
    NodeAssert.deepEqual((await parentCorrelation.response(90)).result, { ok: true });
    parentCorrelation.send({ id: 91, method: "fixture/approval-burst" });
    NodeAssert.deepEqual((await parentCorrelation.response(91)).result, { count: 8 });
    NodeAssert.equal(
      parentCorrelation.messages.filter(
        (message) => message.method === "item/tool/requestUserInput",
      ).length,
      8,
    );
    parentCorrelation.send({ id: 92, method: "fixture/echo" });
    NodeAssert.equal((await parentCorrelation.response(92)).result.requiresOpenaiAuth, false);
    await parentCorrelation.finish();
    assertions += 5;

    const upstream = await start("upstream");
    upstream.send({
      id: 1,
      method: "initialize",
      params: {
        clientInfo: {
          name: "t3-code-ls-upstream-fixture",
          title: "Lust upstream fixture",
          version: "0.1.0",
        },
        capabilities: { experimentalApi: true, optOutNotificationMethods: null },
      },
    });
    NodeAssert.equal((await upstream.response(1)).result.userAgent, "mock-codex-app-server");
    upstream.send({ method: "initialized" });
    NodeAssert.equal(
      (await upstream.wait((message) => message.method === "item/agentMessage/delta")).params.delta,
      "Mock server is ready.",
    );
    upstream.send({ id: 2, method: "account/read", params: {} });
    NodeAssert.equal((await upstream.response(2)).result.account.type, "chatgpt");
    upstream.send({ id: 3, method: "skills/list", params: { cwds: [temporary] } });
    const approval = await upstream.wait(
      (message) => message.method === "item/tool/requestUserInput",
    );
    NodeAssert.equal(approval.params.questions[0].id, "approved");
    upstream.send({ id: approval.id, result: { answers: { approved: { answers: ["yes"] } } } });
    NodeAssert.deepEqual((await upstream.response(3)).result.data[0].skills, []);
    assertions += 5;
    await upstream.finish();

    const normal = await start("normal");
    normal.child.stdin.write("\ufeff\n");
    normal.send({
      id: 1,
      method: "initialize",
      params: {
        clientInfo: { name: "t3-code-ls-fixture", title: "Lust fixture", version: "0.1.0" },
        capabilities: { experimentalApi: true, optOutNotificationMethods: null },
      },
    });
    NodeAssert.equal((await normal.response(1)).result.userAgent, "deterministic-codex-app-server");
    normal.send({ method: "initialized" });
    NodeAssert.equal(
      (await normal.wait((message) => message.method === "item/agentMessage/delta")).params.delta,
      "ready ✓",
    );
    normal.send({ id: 2, method: "fixture/first" });
    normal.send({ id: 3, method: "fixture/second" });
    await normal.response(2);
    NodeAssert.deepEqual(
      normal.messages
        .filter((message) => message.id === 2 || message.id === 3)
        .map((message) => message.id),
      [3, 2],
    );
    normal.send({ id: 4, method: "skills/list", params: { cwds: [temporary] } });
    await normal.wait(
      (message) => message.id === 10000 && message.method === "item/tool/requestUserInput",
    );
    normal.send({ id: 10000, result: { answers: {} } });
    NodeAssert.deepEqual((await normal.response(4)).result.data[0].skills, []);
    normal.send({ id: 5, method: "fixture/error" });
    NodeAssert.deepEqual((await normal.response(5)).error, {
      code: -32601,
      message: "unsupported method",
      data: { detail: "kept" },
    });
    normal.send({ id: 6, method: "fixture/missing-result" });
    NodeAssert.deepEqual(await normal.response(6), { id: 6 });
    normal.send({ id: "7", method: "fixture/numeric-response" });
    NodeAssert.equal((await normal.response(7)).result.converted, true);
    normal.send({ id: 8, method: "fixture/unknown-response" });
    await normal.response(8);
    NodeAssert.ok(!normal.messages.some((message) => message.id === "unknown"));
    for (const id of [1.5, 9007199254740992, 1e21, 1e-7, "identifier".repeat(200)]) {
      normal.send({ id, method: "fixture/echo" });
      NodeAssert.equal((await normal.response(id)).result.requiresOpenaiAuth, false);
    }
    normal.send({ id: 9, method: "fixture/burst" });
    NodeAssert.equal((await normal.response(9)).result.count, 1000);
    NodeAssert.deepEqual(
      normal.messages
        .filter((message) => message.method === "fixture/delta")
        .map((message) => message.params.index),
      Array.from({ length: 1000 }, (_, index) => index),
    );
    assertions += 15;
    await normal.finish();

    for (const parentProtocol of [false, true]) {
      for (const mode of ["malformed", "truncated", "exit"]) {
        const peer = await start(mode, parentProtocol ? ["--parent-protocol"] : []);
        peer.send({ id: 10, method: "fixture/hold" });
        peer.send({ id: 11, method: "fixture/hold" });
        if (parentProtocol) {
          await peer.completion;
          NodeAssert.deepEqual(
            peer.messages.filter((message) => "id" in message),
            [],
          );
          assertions += 1;
        } else {
          const first = await peer.response(10);
          const second = await peer.response(11);
          NodeAssert.equal(first.error.code, -32000);
          NodeAssert.equal(second.error.code, -32000);
          assertions += 2;
        }
        await peer.finish(mode === "exit" ? 7 : 2);
      }
    }

    for (const mode of ["oversized", "invalid-utf8", "bad-blank"]) {
      const peer = await start(mode, ["--max-frame-bytes", "256"]);
      peer.send({ id: 12, method: "fixture/echo" });
      NodeAssert.equal((await peer.response(12)).error.code, -32000);
      assertions += 1;
      await peer.finish(2);
    }

    for (const mode of ["bom-blank", "final-line"]) {
      const peer = await start(mode);
      peer.send({ id: 13, method: "fixture/echo" });
      NodeAssert.ok((await peer.response(13)).result);
      assertions += 1;
      await peer.finish();
    }

    const timeout = await start("normal", ["--request-timeout-ms", "100"]);
    timeout.send({ id: 20, method: "fixture/hold" });
    NodeAssert.equal((await timeout.response(20)).error.message, "provider request timed out");
    timeout.send({ method: "fixture/release" });
    timeout.send({ id: 21, method: "fixture/echo" });
    await timeout.response(21);
    NodeAssert.equal(timeout.messages.filter((message) => message.id === 20).length, 1);
    assertions += 2;
    await timeout.finish(2);

    const duplicate = await start("normal");
    duplicate.send({ id: 30, method: "fixture/hold" });
    duplicate.send({ id: "30", method: "fixture/hold" });
    NodeAssert.match((await duplicate.finish(2)).stderr, /request ID is already active/);
    assertions += 1;

    const resolved = await start("normal");
    resolved.send({ id: 40, method: "fixture/resolved" });
    await resolved.wait(
      (message) => message.id === 10001 && message.method === "item/tool/requestUserInput",
    );
    resolved.send({ id: 10001, result: {} });
    await resolved.response(40);
    resolved.send({ id: 41, method: "fixture/echo" });
    NodeAssert.equal((await resolved.response(41)).result.requiresOpenaiAuth, false);
    await resolved.finish();
    assertions += 1;

    const crossTypeResolved = await start("normal", ["--max-pending", "1"]);
    crossTypeResolved.send({ id: 42, method: "fixture/resolved-string-id" });
    await crossTypeResolved.wait(
      (message) => message.id === 10012 && message.method === "item/tool/requestUserInput",
    );
    await crossTypeResolved.response(42);
    await crossTypeResolved.finish();
    assertions += 1;

    const duplicateServerApproval = await start("normal");
    duplicateServerApproval.send({ id: 43, method: "fixture/duplicate-server-approval" });
    await duplicateServerApproval.wait(
      (message) => message.id === 10010 && message.method === "item/tool/requestUserInput",
    );
    const duplicateServerExit = await duplicateServerApproval.completion;
    NodeAssert.equal(duplicateServerExit.code, 2);
    NodeAssert.match((await duplicateServerApproval.finish(2)).stderr, /request ID is already active/);
    assertions += 2;

    const nonfinite = await start("normal");
    nonfinite.child.stdin.write('{"id":1e9999,"method":"fixture/echo"}\n');
    NodeAssert.match((await nonfinite.finish(2)).stderr, /invalid JSON-RPC/);
    assertions += 1;

    const invalid = await start("normal");
    invalid.child.stdin.write('{"id":null,"method":"fixture/echo"}\n');
    NodeAssert.match((await invalid.finish(2)).stderr, /invalid JSON-RPC/);
    assertions += 1;

    for (const mode of ["stubborn", "blocked-write"]) {
      const peer = await start(mode, ["--max-frame-bytes", "1048576"]);
      if (mode === "blocked-write")
        peer.send({ id: 50, method: "fixture/hold", params: "x".repeat(900000) });
      NodeAssert.match((await peer.finish(2)).stderr, /did not exit after its input closed/);
      assertions += 1;
    }

    for (const [signal, expected] of [
      ["SIGINT", 130],
      ["SIGTERM", 143],
    ]) {
      const peer = await start("blocked-write", ["--max-frame-bytes", "1048576"]);
      peer.send({ id: 60, method: "fixture/hold", params: "x".repeat(900000) });
      peer.child.kill(signal);
      await peer.finish(expected);
      assertions += 1;
    }
    const failedChild = await start("normal");
    failedChild.send({ id: 70, method: "fixture/exit", params: { code: 42 } });
    NodeAssert.equal((await failedChild.response(70)).error.code, -32000);
    await failedChild.finish(42);
    assertions += 1;

    for (const parentProtocol of [false, true]) {
      const signalledChild = await start("normal", parentProtocol ? ["--parent-protocol"] : []);
      signalledChild.send({ id: 71, method: "fixture/signal", params: { signal: "SIGTERM" } });
      if (parentProtocol) {
        await signalledChild.completion;
        NodeAssert.deepEqual(
          signalledChild.messages.filter((message) => "id" in message),
          [],
        );
      } else {
        NodeAssert.equal((await signalledChild.response(71)).error.code, -32000);
      }
      await signalledChild.finish(143);
      assertions += 1;
    }
    const blockedOutput = await start("normal");
    blockedOutput.child.stdout.removeAllListeners("data");
    blockedOutput.child.stdout.pause();
    const readable = new Promise((resolve) => {
      const ready = () => {
        if (blockedOutput.child.stdout.readableLength > 0) {
          resolve();
        }
      };
      blockedOutput.child.stdout.on("readable", ready);
      ready();
    });
    blockedOutput.send({ id: 61, method: "fixture/big-output" });
    await Promise.race([
      readable,
      blockedOutput.completion.then(() => {
        throw new Error("Provider exited before output backpressure");
      }),
    ]);
    NodeAssert.ok(blockedOutput.child.stdout.readableLength > 0);
    blockedOutput.child.kill("SIGTERM");
    await blockedOutput.exited;
    blockedOutput.child.stdout.removeAllListeners("readable");
    blockedOutput.child.stdout.resume();
    await blockedOutput.finish(143);
    assertions += 1;
    NodeAssert.equal(expired, false, "Provider fixture process deadline exceeded");
    console.log(
      `Provider transport: ${assertions} assertions passed; pinned upstream Codex fixture, real child stdio, concurrent RPC, approvals, limits, failures, EOF and signal shutdown verified.`,
    );
  } finally {
    clearTimeout(watchdog);
    for (const child of processes) {
      child.stdin.destroy();
      child.kill("SIGTERM");
      const force = setTimeout(() => child.kill("SIGKILL"), 1000);
      try {
        await NodeEvents.once(child, "close");
      } catch {
      } finally {
        clearTimeout(force);
      }
    }
    for (const pid of providers) {
      try {
        process.kill(pid, "SIGKILL");
      } catch {}
    }
    await NodeFSP.rm(temporary, { recursive: true, force: true });
  }
}

if (process.argv[2] === "--fixture") {
  fixture();
} else {
  const binary = process.argv[2];
  if (!binary)
    throw new Error("Usage: node tools/check-provider.mjs /absolute/t3-code-ls-provider");
  await verify(NodePath.resolve(binary));
}
