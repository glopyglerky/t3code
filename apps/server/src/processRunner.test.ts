import { isWindowsCommandNotFound } from "@t3tools/provider-core/server/snapshotProbe";
import { describe, expect, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as PlatformError from "effect/PlatformError";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import { ChildProcessSpawner } from "effect/process";
import * as HostProcess from "@t3tools/shared/HostProcess";
import { SpawnExecutableResolution } from "@t3tools/shared/shell";

import * as ProcessRunner from "./processRunner.ts";

type ChildProcessCommand = {
  readonly command: string;
  readonly args: ReadonlyArray<string>;
  readonly options: {
    readonly shell?: boolean | string;
    readonly env?: NodeJS.ProcessEnv | undefined;
    readonly extendEnv?: boolean | undefined;
  };
};

function asChildProcessCommand(command: unknown): ChildProcessCommand {
  return command as ChildProcessCommand;
}

function makeHandle(input: {
  readonly stdout?: string | Stream.Stream<Uint8Array>;
  readonly stderr?: string | Stream.Stream<Uint8Array>;
  readonly code?: number;
  readonly stdin?: ChildProcessSpawner.ChildProcessHandle["stdin"];
  readonly exitCode?: Effect.Effect<ChildProcessSpawner.ExitCode>;
}) {
  return ChildProcessSpawner.makeHandle({
    pid: ChildProcessSpawner.ProcessId(1),
    exitCode: input.exitCode ?? Effect.succeed(ChildProcessSpawner.ExitCode(input.code ?? 0)),
    isRunning: Effect.succeed(false),
    kill: () => Effect.void,
    unref: Effect.succeed(Effect.void),
    stdin: input.stdin ?? Sink.drain,
    stdout:
      typeof input.stdout === "string"
        ? Stream.encodeText(Stream.make(input.stdout))
        : (input.stdout ?? Stream.empty),
    stderr:
      typeof input.stderr === "string"
        ? Stream.encodeText(Stream.make(input.stderr))
        : (input.stderr ?? Stream.empty),
    all: Stream.empty,
    getInputFd: () => Sink.drain,
    getOutputFd: () => Stream.empty,
  });
}

function makeSpawner(
  f: (
    command: ChildProcessCommand,
  ) => Effect.Effect<ChildProcessSpawner.ChildProcessHandle, PlatformError.PlatformError>,
) {
  return ChildProcessSpawner.make((command) => f(asChildProcessCommand(command)));
}

const runWith =
  (spawner: ChildProcessSpawner.ChildProcessSpawner["Service"]) =>
  (input: ProcessRunner.ProcessRunInput) =>
    Effect.service(ProcessRunner.ProcessRunner).pipe(
      Effect.flatMap((runner) =>
        runner.run({
          ...input,
        }),
      ),
      Effect.provide(
        ProcessRunner.layer.pipe(
          Layer.provide(Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, spawner)),
        ),
      ),
    );

describe("runProcess", () => {
  it.effect("resolves and escapes Windows command shims using the selected environment", () =>
    Effect.gen(function* () {
      const hostEnvironment = {
        PATH: "C:\\Users\\tester\\AppData\\Roaming\\npm",
        PATHEXT: ".COM;.EXE;.BAT;.CMD",
        PARENT_ONLY: "must-not-reach-isolated-child",
      };
      const args = ["repos", "pr", "list", "--source-branch", "feature & release"];
      for (const input of [
        {
          env: { AZURE_CONFIG_DIR: "C:\\Users\\tester\\.azure" },
          extendEnv: undefined,
          executable: "C:\\Users\\tester\\AppData\\Roaming\\npm\\az.cmd",
        },
        { env: {}, extendEnv: false, executable: undefined },
        {
          env: { PATH: "C:\\isolated\\bin", AZURE_CONFIG_DIR: "C:\\isolated\\.azure" },
          extendEnv: false,
          executable: "C:\\isolated\\bin\\az.cmd",
        },
      ]) {
        const spawner = makeSpawner((command) =>
          Effect.sync(() => {
            expect(command.command).toBe(
              input.executable === undefined
                ? "az"
                : [
                    `^"${input.executable}^"`,
                    '^"repos^"',
                    '^"pr^"',
                    '^"list^"',
                    '^"--source-branch^"',
                    '^"feature^ ^&^ release^"',
                  ].join(" "),
            );
            expect(command.args).toEqual(input.executable === undefined ? args : []);
            expect(command.options.shell).toBe(input.executable !== undefined);
            expect(command.options.env).toEqual(input.env);
            expect(command.options.extendEnv).toBe(input.extendEnv ?? true);
            return makeHandle({ stdout: "[]" });
          }),
        );
        const result = yield* runWith(spawner)({
          command: "az",
          args,
          env: input.env,
          extendEnv: input.extendEnv,
        }).pipe(
          Effect.provideService(HostProcess.Platform, "win32"),
          Effect.provideService(HostProcess.Environment, hostEnvironment),
          Effect.provideService(SpawnExecutableResolution, (_command, _platform, env) => {
            expect(env).toEqual(
              input.extendEnv === false ? input.env : { ...hostEnvironment, ...input.env },
            );
            return input.executable;
          }),
        );
        expect(result.stdout).toBe("[]");
      }
    }),
  );

  it.effect("types spawn failures that the platform spawner throws synchronously", () =>
    Effect.gen(function* () {
      // File-valued PATH entries make spawn throw ENOTDIR synchronously, arriving through Effect.callback as a defect.
      const defect = Object.assign(new Error("spawn jj ENOTDIR"), { code: "ENOTDIR" });
      const spawner = makeSpawner(() =>
        Effect.callback<never, never>(() => {
          throw defect;
        }),
      );

      const error = yield* runWith(spawner)({
        command: "jj",
        args: ["--version"],
        cwd: "/repo",
      }).pipe(Effect.flip);

      expect(error._tag).toBe("ProcessSpawnError");
      if (error._tag !== "ProcessSpawnError") {
        return expect.fail("Expected ProcessSpawnError");
      }
      expect(error).toMatchObject({ command: "jj", argumentCount: 1, cwd: "/repo" });
      expect(error.cause).toBe(defect);
    }),
  );

  it.effect("fails when output exceeds max buffer in default mode", () =>
    Effect.gen(function* () {
      const spawner = makeSpawner(() => Effect.succeed(makeHandle({ stdout: "x".repeat(2048) })));

      const error = yield* runWith(spawner)({
        command: "fake",
        args: ["stdout-bytes", "2048"],
        maxOutputBytes: 128,
      }).pipe(Effect.flip);

      expect(error._tag).toBe("ProcessOutputLimitError");
      if (error._tag !== "ProcessOutputLimitError") {
        return expect.fail("Expected ProcessOutputLimitError");
      }
      expect(error).toMatchObject({
        stream: "stdout",
        maxBytes: 128,
        observedBytes: 2048,
      });
      expect(error.message).toBe(
        "Process 'fake' stdout produced 2048 bytes, exceeding the 128 byte limit",
      );
    }),
  );

  it.effect("accepts output at the byte limit followed by an empty chunk", () =>
    Effect.gen(function* () {
      const output = new TextEncoder().encode("exactly");
      const spawner = makeSpawner(() =>
        Effect.succeed(
          makeHandle({
            stdout: Stream.make(output, new Uint8Array()),
          }),
        ),
      );

      const result = yield* runWith(spawner)({
        command: "fake",
        args: ["exact-limit"],
        maxOutputBytes: output.byteLength,
      });

      expect(result.stdout).toBe("exactly");
    }),
  );

  it.effect("fails fast on output limit before timeout for long-running output", () =>
    Effect.gen(function* () {
      const textChunk = "x".repeat(64);
      const spawner = makeSpawner(() =>
        Effect.succeed(
          makeHandle({
            stdout: Stream.fromIterable(Array.from({ length: 10 }, () => textChunk)).pipe(
              Stream.encodeText,
            ),
            exitCode: Effect.never,
          }),
        ),
      );

      const error = yield* runWith(spawner)({
        command: "fake",
        args: ["spam-stdout"],
        maxOutputBytes: 128,
        timeout: "2 seconds",
      }).pipe(Effect.flip);

      expect(error).toBeInstanceOf(ProcessRunner.ProcessOutputLimitError);
    }),
  );

  it.effect("truncates output when outputMode is truncate", () =>
    Effect.gen(function* () {
      const spawner = makeSpawner(() => Effect.succeed(makeHandle({ stdout: "x".repeat(2048) })));

      const result = yield* runWith(spawner)({
        command: "fake",
        args: ["stdout-bytes", "2048"],
        maxOutputBytes: 128,
        outputMode: "truncate",
      });

      expect(result.code).toBe(0);
      expect(result.stdout.length).toBeLessThanOrEqual(128);
      expect(result.stdoutTruncated).toBe(true);
      expect(result.stderrTruncated).toBe(false);
    }),
  );

  it.effect("writes stdin before waiting for exit", () =>
    Effect.gen(function* () {
      const stdinWritten = yield* Deferred.make<void>();
      const decoder = new TextDecoder();
      const spawner = makeSpawner(() =>
        Effect.succeed(
          makeHandle({
            stdout: "stdin payload",
            stdin: Sink.forEach((chunk: Uint8Array) => {
              const text = decoder.decode(chunk, { stream: true });
              return text.includes("stdin payload")
                ? Deferred.succeed(stdinWritten, undefined)
                : Effect.void;
            }),
            exitCode: Deferred.await(stdinWritten).pipe(Effect.as(ChildProcessSpawner.ExitCode(0))),
          }),
        ),
      );

      const result = yield* runWith(spawner)({
        command: "fake",
        args: ["stdin-echo"],
        stdin: "stdin payload",
      });

      expect(result.stdout).toBe("stdin payload");
      expect(result.code).toBe(0);
    }),
  );
});

describe("isWindowsCommandNotFound", () => {
  it.effect("matches the localized German cmd.exe error text", () =>
    Effect.gen(function* () {
      const isCommandNotFound = yield* isWindowsCommandNotFound(
        1,
        "wird nicht als interner oder externer Befehl, betriebsfahiges Programm oder Batch-Datei erkannt",
      ).pipe(Effect.provideService(HostProcess.Platform, "win32"));
      expect(isCommandNotFound).toBe(true);
    }),
  );
});
