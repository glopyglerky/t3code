#!/usr/bin/env node
import * as Assert from "node:assert/strict";
import * as FS from "node:fs/promises";
import * as Path from "node:path";
import * as ChildProcess from "node:child_process";
import { fileURLToPath } from "node:url";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import { createFixture } from "./database-fixture.mjs";
import { finishProof, withNative } from "./redb-domain-fixture.mjs";

const [mode, ...args] = process.argv.slice(2);
if (mode === "--child") {
  const [family, ...commandArgs] = args;
  const Command = await import("effect/cli/Command");
  const Net = await import("@t3tools/shared/Net");
  const command =
    family === "project"
      ? (await import("../apps/server/src/cli/project.ts")).projectCommand
      : family === "environment"
        ? (await import("../apps/server/src/cli/environment.ts")).environmentCommand
        : (await import("../apps/server/src/cli/doctor.ts")).doctorCommand;
  try {
    await Effect.runPromise(
      Command.runWith(command, { version: "0.0.0" })(commandArgs).pipe(
        Effect.scoped,
        Effect.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer, Net.layer)),
      ),
    );
  } catch (error) {
    console.error(error);
    process.exitCode = 1;
  }
} else {
  const executable = mode,
    [proofPath] = args;
  Assert.ok(
    executable && Path.isAbsolute(executable),
    "Pass an absolute native database executable",
  );
  await FS.access(executable, FS.constants.X_OK);
  const fixture = await createFixture();
  const baseDir = Path.join(fixture.root, "home");
  const workspace = Path.join(fixture.root, "workspace");
  const stateDir = Path.join(baseDir, "userdata");
  const databasePath = Path.join(stateDir, "statev2.omdb");
  const checks = [];
  await FS.mkdir(workspace);
  const child = (family, commandArgs, expectedSuccess = true) =>
    new Promise((resolve, reject) => {
      const processChild = ChildProcess.spawn(
        process.execPath,
        [fileURLToPath(import.meta.url), "--child", family, ...commandArgs, "--base-dir", baseDir],
        {
          cwd: workspace,
          env: {
            PATH: process.env.PATH ?? "/usr/bin:/bin",
            LANG: "C.UTF-8",
            T3_CODE_LS_IMPLEMENTATION: "upstream",
            T3_CODE_LS_DB_EXECUTABLE: executable,
          },
          stdio: ["ignore", "pipe", "pipe"],
        },
      );
      let stdout = "",
        stderr = "";
      processChild.stdout.setEncoding("utf8").on("data", (data) => {
        stdout += data;
      });
      processChild.stderr.setEncoding("utf8").on("data", (data) => {
        stderr += data;
      });
      const timer = setTimeout(() => {
        processChild.kill("SIGTERM");
      }, 60_000);
      processChild.once("error", (error) => {
        clearTimeout(timer);
        reject(error);
      });
      processChild.once("exit", (code, signal) => {
        clearTimeout(timer);
        try {
          if (expectedSuccess)
            Assert.equal(code, 0, `${family} ${commandArgs.join(" ")}: ${stderr}\n${stdout}`);
          else Assert.notEqual(code, 0, "owned state refuses a concurrent offline mutation");
          Assert.equal(signal, null, "CLI completed before its owned timeout");
          resolve({ stdout, stderr, code });
        } catch (error) {
          reject(error);
        }
      });
    });
  const projects = async () => {
    const NativeProjectRows =
      await import("../apps/server/src/orchestration-v2/NativeProjectRows.ts");
    return withNative(executable, { ...fixture, databasePath }, () =>
      Effect.gen(function* () {
        const rows = yield* NativeProjectRows.make;
        return yield* rows.select({ includeDeleted: false });
      }),
    );
  };
  try {
    await child("project", ["add", workspace, "--title", "Alpha"]);
    const [added] = await projects();
    Assert.equal(added.title, "Alpha");
    Assert.equal(added.workspaceRoot, workspace);
    await FS.access(databasePath);
    for (const path of ["state.sqlite", "statev2.sqlite"]) {
      await Assert.rejects(FS.access(Path.join(stateDir, path)));
    }
    Assert.equal(
      await FS.readFile(Path.join(stateDir, "server-owner.sqlite"), "utf8"),
      "t3code-omdb-owner-v1\n",
    );
    checks.push(
      "Fresh offline project CLI bootstraps omdb and ownership marker without creating a SQLite state file",
    );

    await child("project", ["rename", workspace, "Beta"]);
    Assert.equal((await projects())[0].title, "Beta");
    const NativeOwnership =
      await import("../apps/server/src/persistence/NativeServerOwnershipLock.ts");
    await Effect.runPromise(
      Effect.gen(function* () {
        const locks = yield* NativeOwnership.make(executable);
        yield* locks.acquire(Path.join(stateDir, "server-owner.sqlite"));
        const refused = yield* Effect.promise(() =>
          child("project", ["rename", workspace, "Must not persist"], false),
        );
        Assert.match(refused.stderr, /server already owns|ServerAlreadyRunningError/);
      }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
    );
    Assert.equal((await projects())[0].title, "Beta");
    checks.push(
      "Separate CLI invocations persist rename and refuse offline mutation while another native owner holds the lock",
    );

    const links = await child("environment", ["list", "--json"]);
    Assert.deepEqual(JSON.parse(links.stdout), []);
    const doctor = await child("doctor", ["--dry-run"]);
    Assert.match(doctor.stdout, /Database:/);
    Assert.ok(!doctor.stdout.includes("automatic shrinking"));
    checks.push(
      "Environment list and doctor select native persistence without SQL fallback or an automatic shrinking claim",
    );

    await child("project", ["remove", added.projectId]);
    Assert.deepEqual(await projects(), []);
    checks.push(
      "Offline project removal commits through the real selected ProjectService after helper restarts",
    );

    const { sqliteShowcaseEnvironment, assertSqliteShowcaseHome } =
      await import("../scripts/mobile-showcase-environment.ts");
    const { selectNativeExecutable } = await import("../packages/shared/src/nativeExecutable.ts");
    const ambient = {
      T3_CODE_LS_IMPLEMENTATION: "native",
      T3_CODE_LS_DB_EXECUTABLE: executable,
      PATH: "fixture",
    };
    const showcase = sqliteShowcaseEnvironment(ambient);
    Assert.equal(
      selectNativeExecutable(
        "db",
        showcase.T3_CODE_LS_DB_EXECUTABLE,
        "linux",
        "x64",
        showcase.T3_CODE_LS_IMPLEMENTATION,
      ),
      undefined,
    );
    Assert.equal(
      ambient.T3_CODE_LS_DB_EXECUTABLE,
      executable,
      "Fixture selection does not alter its caller's environment",
    );
    Assert.equal(showcase.PATH, "fixture");
    await Assert.rejects(assertSqliteShowcaseHome(baseDir), /authoritative omdb/);
    const showcaseHome = Path.join(fixture.root, "showcase");
    await assertSqliteShowcaseHome(showcaseHome);
    await FS.mkdir(Path.join(showcaseHome, "userdata"), { recursive: true });
    await FS.writeFile(
      Path.join(showcaseHome, "userdata", "server-owner.sqlite"),
      "t3code-omdb-owner-v1\n",
    );
    await Assert.rejects(assertSqliteShowcaseHome(showcaseHome), /authoritative omdb/);
    checks.push(
      "Retained mobile screenshot fixtures explicitly select SQLite before launch and cannot seed native or marker-only homes",
    );

    const { runDevRunnerWithInput } = await import("../scripts/dev-runner.ts");
    const HostProcess = await import("@t3tools/shared/HostProcess");
    const ConfigProvider = await import("effect/ConfigProvider");
    const Sink = await import("effect/Sink"),
      Stream = await import("effect/Stream");
    const { ChildProcessSpawner } = await import("effect/process");
    const Net = await import("@t3tools/shared/Net");
    const worktree = Path.join(fixture.root, "dev-worktree"),
      fakeHome = Path.join(fixture.root, "dev-home");
    await FS.mkdir(worktree);
    await FS.writeFile(Path.join(worktree, ".git"), "gitdir: /fixture/.git/worktrees/demo\n");
    await FS.mkdir(Path.join(fakeHome, ".t3", "userdata"), { recursive: true });
    await FS.writeFile(Path.join(fakeHome, ".t3", "userdata", "statev2.sqlite"), "");
    const runDev = async () => {
      const spawned = [];
      const spawn = ChildProcessSpawner.make((command) => {
        spawned.push(command.args);
        return Effect.succeed(
          ChildProcessSpawner.makeHandle({
            pid: ChildProcessSpawner.ProcessId(1),
            exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(0)),
            isRunning: Effect.succeed(false),
            kill: () => Effect.void,
            unref: Effect.succeed(Effect.void),
            stdin: Sink.drain,
            stdout: Stream.empty,
            stderr: Stream.empty,
            all: Stream.empty,
            getInputFd: () => Sink.drain,
            getOutputFd: () => Stream.empty,
          }),
        );
      });
      await Effect.runPromise(
        runDevRunnerWithInput({
          mode: "dev:server",
          t3Home: undefined,
          browser: undefined,
          autoBootstrapProjectFromCwd: undefined,
          logWebSocketEvents: undefined,
          host: undefined,
          port: 13773,
          devUrl: undefined,
          dryRun: false,
          share: false,
          built: false,
          runArgs: [],
          seed: true,
        }).pipe(
          Effect.scoped,
          Effect.provide(
            Layer.mergeAll(
              NodeServices.layer,
              ConfigProvider.layer(ConfigProvider.fromEnv({ env: {} })),
              Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, spawn),
              Layer.succeed(Net.NetService, {
                canListenOnHost: () => Effect.succeed(true),
                isPortAvailableOnLoopback: () => Effect.succeed(true),
                hasListenerOnHost: () => Effect.succeed(false),
                reserveLoopbackPort: () => Effect.succeed(49152),
                findAvailablePort: (port) => Effect.succeed(port),
              }),
            ),
          ),
          Effect.provideService(HostProcess.Platform, "linux"),
          Effect.provideService(HostProcess.WorkingDirectory, worktree),
          Effect.provideService(HostProcess.HomeDirectory, fakeHome),
          Effect.provideService(HostProcess.Environment, {}),
        ),
      );
      return spawned.filter((args) => args.some((arg) => arg.endsWith("migrate-dev-db.ts")));
    };
    Assert.equal((await runDev()).length, 1, "An actually empty worktree still seeds");
    for (const existing of ["statev2.omdb", "server-owner.sqlite"]) {
      const file = Path.join(worktree, ".t3", "userdata", existing);
      await FS.mkdir(Path.dirname(file), { recursive: true });
      await FS.writeFile(file, "");
      Assert.deepEqual(await runDev(), [], `Do not reseed ${existing}`);
      await FS.unlink(file);
    }
    checks.push(
      "The actual dev runner does not reseed an existing native-only or marker-only home; subprocesses and homes are fixture-owned",
    );
    await finishProof({
      executable,
      fixture,
      proofPath,
      checks,
      oracles: [],
      sourcePaths: [
        "apps/server/src/cli/environment.ts",
        "apps/server/src/cli/project.ts",
        "apps/server/src/cli/doctor.ts",
        "apps/server/src/cli/server.ts",
        "apps/server/src/persistence/SelectedPersistence.ts",
        "apps/server/src/persistence/Omdb.ts",
        "apps/server/src/orchestration-v2/selectedRuntimeLayer.ts",
        "tools/check-redb-cli.mjs",
        "scripts/mobile-showcase.ts",
        "scripts/mobile-showcase-environment.ts",
        "scripts/dev-runner.ts",
        "apps/server/src/triage/triagePrompt.ts",
      ],
    });
  } finally {
    await fixture.close();
  }
}
