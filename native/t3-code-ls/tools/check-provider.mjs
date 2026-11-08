#!/usr/bin/env node
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { mkdtemp, mkdir, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { upstreamSource } from "./upstream-source.mjs";

const script = fileURLToPath(import.meta.url);

function fixture() {
  const mode = process.env.T3LS_FIXTURE;
  assert.equal(process.env.HOME, process.env.T3LS_EXPECT_HOME);
  assert.equal(process.env.CODEX_HOME, path.join(process.env.HOME, ".codex"));
  if (mode === "inherited-env") {
    assert.equal(process.env.T3LS_PARENT_SECRET, "must-not-reach-child");
  } else {
    assert.equal(process.env.T3LS_PARENT_SECRET, undefined);
  }
  const write = (message) => process.stdout.write(`${JSON.stringify(message)}\n`);
  const respond = (id, result) => write({ id, result });
  let held;
  let count = 0;
  let pendingApproval;
  process.stderr.write("sensitive-stderr-sentinel".repeat(12000), () => {
    write({ method: "fixture/ready", params: { pid: process.pid } });
  });
  if (mode === "upstream") {
    import(pathToFileURL(process.env.T3LS_UPSTREAM_PEER).href).catch((error) => {
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
      if (mode === "oversized") { process.stdout.write("x".repeat(257)); continue; }
      if (mode === "invalid-utf8") { process.stdout.write(Buffer.from([255, 10])); continue; }
      if (mode === "bad-blank") { process.stdout.write("\u0085\n"); continue; }
      if (mode === "bom-blank") { process.stdout.write("\ufeff\n"); respond(message.id, { ok: true }); continue; }
      if (mode === "final-line") {
        process.stdout.end(JSON.stringify({ id: message.id, result: { complete: true } }), () => process.exit(0));
        continue;
      }
      if (message.method === "initialize") {
        assert.equal(message.params.clientInfo.name, "t3-code-ls-fixture");
        respond(message.id, { userAgent: "deterministic-codex-app-server", platformFamily: "unix" });
      } else if (message.method === "initialized") {
        process.stdout.write(" \r\n");
        write({ method: "item/agentMessage/delta", params: { delta: "ready ✓", threadId: "thread-1" } });
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
      } else if (message.method === "fixture/unknown-response") {
        respond("unknown", { ignored: true });
        respond(message.id, { ok: true });
      } else if (message.method === "fixture/burst") {
        const lines = [];
        for (let index = 0; index < 1000; index += 1) lines.push(JSON.stringify({ method: "fixture/delta", params: { index } }));
        lines.push(JSON.stringify({ id: message.id, result: { count: 1000 } }));
        process.stdout.write(`${lines.join("\n")}\n`);
      } else if (message.method === "fixture/big-output") {
        write({ method: "fixture/large", params: "x".repeat(3 * 1024 * 1024) });
      } else if (message.method === "fixture/error") {
        write({ id: message.id, error: { code: -32601, message: "unsupported method", data: { detail: "kept" } } });
      } else if (message.method === "fixture/missing-result") {
        write({ id: message.id });
      } else if (message.method === "account/read" || message.method === "fixture/echo") {
        respond(message.id, { account: null, requiresOpenaiAuth: false });
      } else if (message.method === "fixture/numeric-response") {
        respond(Number(message.id), { converted: true });
      } else if (message.id === 10000 && !("method" in message)) {
        assert.deepEqual(message.result, { answers: {} });
        respond(pendingApproval, { data: [{ cwd: process.cwd(), skills: [], errors: [] }] });
        write({ method: "serverRequest/resolved", params: { requestId: 10000 } });
      } else if (message.id === 10001 && !("method" in message)) {
        // An approval can be answered after the server has already resolved it.
      } else {
        assert.fail(`Unexpected fixture method: ${message.method}`);
      }
    }
  });
  process.stdin.on("end", () => process.stdout.end(() => process.exit(0)));
}

async function verify(binary) {
  const temporary = await mkdtemp(path.join(os.tmpdir(), "t3ls-provider-"));
  const home = path.join(temporary, "home");
  await mkdir(home);
  const upstreamPeer = path.join(temporary, "upstream-peer.ts");
  await writeFile(upstreamPeer, upstreamSource("packages/effect-codex-app-server/test/fixtures/codex-app-server-mock-peer.ts"));
  const processes = new Set();
  const providers = new Set();
  let expired = false;
  const watchdog = setTimeout(() => {
    expired = true;
    for (const child of processes) child.kill("SIGKILL");
    for (const pid of providers) { try { process.kill(pid, "SIGKILL"); } catch {} }
  }, 30000);
  let assertions = 0;

  async function start(mode, extra = []) {
    const child = spawn(binary, [
      "--program", process.execPath, "--cwd", temporary, "--home", home,
      "--env", `T3LS_FIXTURE=${mode}`, "--env", `T3LS_EXPECT_HOME=${home}`,
      "--env", `T3LS_UPSTREAM_PEER=${upstreamPeer}`,
      ...extra, "--", script, "--fixture",
    ], { env: { ...process.env, T3LS_PARENT_SECRET: "must-not-reach-child" }, stdio: ["pipe", "pipe", "pipe"] });
    processes.add(child);
    const exited = once(child, "exit");
    let stderr = "";
    child.stderr.setEncoding("utf8");
    child.stderr.on("data", (chunk) => { stderr += chunk; assert.ok(stderr.length < 65536); });
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
        for (const waiter of waiters.splice(0)) waiter.reject(new Error(`Provider closed: ${JSON.stringify(exit)}`));
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
      if (ended) return Promise.reject(new Error(`Provider already closed: ${JSON.stringify(exit)}`));
      return new Promise((resolve, reject) => waiters.push({ predicate, resolve, reject }));
    };
    const ready = await wait((message) => message.method === "fixture/ready");
    const providerPid = ready.params.pid;
    providers.add(providerPid);
    return {
      child, messages, wait, completion, exited, providerPid,
      send: (message) => child.stdin.write(`${JSON.stringify(message)}\n`),
      response: (id) => wait((message) => "id" in message && String(message.id) === String(id) && !("method" in message)),
      async finish(expectedCode = 0) {
        child.stdin.end();
        const result = await completion;
        assert.equal(result.signal, null, JSON.stringify(result));
        assert.equal(result.code, expectedCode, JSON.stringify(result));
        assert.ok(!result.stderr.includes("sensitive-stderr-sentinel"));
        assert.throws(() => process.kill(providerPid, 0), { code: "ESRCH" });
        providers.delete(providerPid);
        assertions += 3;
        return result;
      },
    };
  }

  try {
    const upstream = await start("upstream");
    upstream.send({ id: 1, method: "initialize", params: {
      clientInfo: { name: "t3-code-ls-upstream-fixture", title: "Lust upstream fixture", version: "0.1.0" },
      capabilities: { experimentalApi: true, optOutNotificationMethods: null },
    } });
    assert.equal((await upstream.response(1)).result.userAgent, "mock-codex-app-server");
    upstream.send({ method: "initialized" });
    assert.equal((await upstream.wait((message) => message.method === "item/agentMessage/delta")).params.delta, "Mock server is ready.");
    upstream.send({ id: 2, method: "account/read", params: {} });
    assert.equal((await upstream.response(2)).result.account.type, "chatgpt");
    upstream.send({ id: 3, method: "skills/list", params: { cwds: [temporary] } });
    const approval = await upstream.wait((message) => message.method === "item/tool/requestUserInput");
    assert.equal(approval.params.questions[0].id, "approved");
    upstream.send({ id: approval.id, result: { answers: { approved: { answers: ["yes"] } } } });
    assert.deepEqual((await upstream.response(3)).result.data[0].skills, []);
    assertions += 5;
    await upstream.finish();

    const normal = await start("normal");
    normal.child.stdin.write("\ufeff\n");
    normal.send({ id: 1, method: "initialize", params: {
      clientInfo: { name: "t3-code-ls-fixture", title: "Lust fixture", version: "0.1.0" },
      capabilities: { experimentalApi: true, optOutNotificationMethods: null },
    } });
    assert.equal((await normal.response(1)).result.userAgent, "deterministic-codex-app-server");
    normal.send({ method: "initialized" });
    assert.equal((await normal.wait((message) => message.method === "item/agentMessage/delta")).params.delta, "ready ✓");
    normal.send({ id: 2, method: "fixture/first" });
    normal.send({ id: 3, method: "fixture/second" });
    await normal.response(2);
    assert.deepEqual(normal.messages.filter((message) => message.id === 2 || message.id === 3).map((message) => message.id), [3, 2]);
    normal.send({ id: 4, method: "skills/list", params: { cwds: [temporary] } });
    await normal.wait((message) => message.id === 10000 && message.method === "item/tool/requestUserInput");
    normal.send({ id: 10000, result: { answers: {} } });
    assert.deepEqual((await normal.response(4)).result.data[0].skills, []);
    normal.send({ id: 5, method: "fixture/error" });
    assert.deepEqual((await normal.response(5)).error, { code: -32601, message: "unsupported method", data: { detail: "kept" } });
    normal.send({ id: 6, method: "fixture/missing-result" });
    assert.deepEqual(await normal.response(6), { id: 6 });
    normal.send({ id: "7", method: "fixture/numeric-response" });
    assert.equal((await normal.response(7)).result.converted, true);
    normal.send({ id: 8, method: "fixture/unknown-response" });
    await normal.response(8);
    assert.ok(!normal.messages.some((message) => message.id === "unknown"));
    for (const id of [1.5, 9007199254740992, 1e21, 1e-7, "identifier".repeat(200)]) {
      normal.send({ id, method: "fixture/echo" });
      assert.equal((await normal.response(id)).result.requiresOpenaiAuth, false);
    }
    normal.send({ id: 9, method: "fixture/burst" });
    assert.equal((await normal.response(9)).result.count, 1000);
    assert.deepEqual(normal.messages.filter((message) => message.method === "fixture/delta").map((message) => message.params.index), Array.from({ length: 1000 }, (_, index) => index));
    assertions += 15;
    await normal.finish();

    // Credentials cross the native boundary by environment, never by argv.
    const inherited = await start("inherited-env", ["--inherit-env"]);
    assert.ok(inherited.child.spawnargs.every((arg) => !arg.includes("must-not-reach-child")));
    inherited.send({ id: 901, method: "initialize", params: {
      clientInfo: { name: "t3-code-ls-fixture", title: "Inherited env", version: "0.1.0" },
      capabilities: { experimentalApi: true, optOutNotificationMethods: null },
    } });
    assert.equal((await inherited.response(901)).result.userAgent, "deterministic-codex-app-server");
    assertions += 2;
    await inherited.finish();

    for (const mode of ["malformed", "truncated", "exit"]) {
      const peer = await start(mode);
      peer.send({ id: 10, method: "fixture/hold" });
      peer.send({ id: 11, method: "fixture/hold" });
      const first = await peer.response(10);
      const second = await peer.response(11);
      assert.equal(first.error.code, -32000);
      assert.equal(second.error.code, -32000);
      assertions += 2;
      await peer.finish(2);
    }

    for (const mode of ["oversized", "invalid-utf8", "bad-blank"]) {
      const peer = await start(mode, ["--max-frame-bytes", "256"]);
      peer.send({ id: 12, method: "fixture/echo" });
      assert.equal((await peer.response(12)).error.code, -32000);
      assertions += 1;
      await peer.finish(2);
    }

    for (const mode of ["bom-blank", "final-line"]) {
      const peer = await start(mode);
      peer.send({ id: 13, method: "fixture/echo" });
      assert.ok((await peer.response(13)).result);
      assertions += 1;
      await peer.finish();
    }

    const timeout = await start("normal", ["--request-timeout-ms", "100"]);
    timeout.send({ id: 20, method: "fixture/hold" });
    assert.equal((await timeout.response(20)).error.message, "provider request timed out");
    timeout.send({ method: "fixture/release" });
    timeout.send({ id: 21, method: "fixture/echo" });
    await timeout.response(21);
    assert.equal(timeout.messages.filter((message) => message.id === 20).length, 1);
    assertions += 2;
    await timeout.finish(2);

    const duplicate = await start("normal");
    duplicate.send({ id: 30, method: "fixture/hold" });
    duplicate.send({ id: "30", method: "fixture/hold" });
    assert.match((await duplicate.finish(2)).stderr, /request ID is already active/);
    assertions += 1;

    const resolved = await start("normal");
    resolved.send({ id: 40, method: "fixture/resolved" });
    await resolved.wait((message) => message.id === 10001 && message.method === "item/tool/requestUserInput");
    resolved.send({ id: 10001, result: {} });
    await resolved.response(40);
    resolved.send({ id: 41, method: "fixture/echo" });
    assert.equal((await resolved.response(41)).result.requiresOpenaiAuth, false);
    await resolved.finish();
    assertions += 1;

    const nonfinite = await start("normal");
    nonfinite.child.stdin.write('{"id":1e9999,"method":"fixture/echo"}\n');
    assert.match((await nonfinite.finish(2)).stderr, /invalid JSON-RPC/);
    assertions += 1;

    const invalid = await start("normal");
    invalid.child.stdin.write('{"id":null,"method":"fixture/echo"}\n');
    assert.match((await invalid.finish(2)).stderr, /invalid JSON-RPC/);
    assertions += 1;

    for (const mode of ["stubborn", "blocked-write"]) {
      const peer = await start(mode, ["--max-frame-bytes", "1048576"]);
      if (mode === "blocked-write") peer.send({ id: 50, method: "fixture/hold", params: "x".repeat(900000) });
      assert.match((await peer.finish(2)).stderr, /did not exit after its input closed/);
      assertions += 1;
    }

    for (const [signal, expected] of [["SIGINT", 130], ["SIGTERM", 143]]) {
      const peer = await start("blocked-write", ["--max-frame-bytes", "1048576"]);
      peer.send({ id: 60, method: "fixture/hold", params: "x".repeat(900000) });
      peer.child.kill(signal);
      await peer.finish(expected);
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
    await Promise.race([readable, blockedOutput.completion.then(() => { throw new Error("Provider exited before output backpressure"); })]);
    assert.ok(blockedOutput.child.stdout.readableLength > 0);
    blockedOutput.child.kill("SIGTERM");
    await blockedOutput.exited;
    blockedOutput.child.stdout.removeAllListeners("readable");
    blockedOutput.child.stdout.resume();
    await blockedOutput.finish(143);
    assertions += 1;
    assert.equal(expired, false, "Provider fixture process deadline exceeded");
    console.log(`Provider transport: ${assertions} assertions passed; pinned upstream Codex fixture, real child stdio, concurrent RPC, approvals, limits, failures, EOF and signal shutdown verified.`);
  } finally {
    clearTimeout(watchdog);
    for (const child of processes) {
      child.stdin.destroy();
      child.kill("SIGTERM");
      const force = setTimeout(() => child.kill("SIGKILL"), 1000);
      try { await once(child, "close"); } catch {} finally { clearTimeout(force); }
    }
    for (const pid of providers) { try { process.kill(pid, "SIGKILL"); } catch {} }
    await rm(temporary, { recursive: true, force: true });
  }
}

if (process.argv[2] === "--fixture") {
  fixture();
} else {
  const binary = process.argv[2];
  if (!binary) throw new Error("Usage: node tools/check-provider.mjs /absolute/t3-code-ls-provider");
  await verify(path.resolve(binary));
}
