// @effect-diagnostics nodeBuiltinImport:off -- Native process selection validates a host executable path.
import * as NodePath from "node:path";
import {
  type CodexSettings,
  type ProviderSetupError,
  type ServerProviderModel,
  TextGenerationError,
} from "@t3tools/contracts";
import * as HostProcess from "@t3tools/shared/HostProcess";
import { expandHomePath } from "@t3tools/provider-core/server/pathExpansion";
import * as TextGenerationOperations from "@t3tools/provider-core/server/textGenerationOperations";
import { toJsonSchemaObject } from "@t3tools/provider-core/server/textGenerationUtils";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import type * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import { ChildProcess, ChildProcessSpawner } from "effect/process";

import { resolveAttachmentPath } from "../attachmentStore.ts";
import * as ServerConfig from "../config.ts";
import type { CodexEffectiveRuntime } from "../provider/CodexManagedRuntime.ts";

const MAX_REQUEST_BYTES = 8 * 1024 * 1024;
const MAX_RESPONSE_BYTES = 24 * 1024 * 1024 + 1024;
const MAX_OUTPUT_BYTES = 4 * 1024 * 1024;

class NativeCodexTextGenerationError extends Schema.TaggedError<NativeCodexTextGenerationError>()(
  "NativeCodexTextGenerationError",
  {
    stage: Schema.Literals(["configuration", "request", "process", "response"]),
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    return `Lust Codex text generation failed during ${this.stage}.`;
  }
}

class NativeGenerationFailure extends Schema.TaggedError<NativeGenerationFailure>()(
  "NativeGenerationFailure",
  {
    kind: Schema.Literals(["spawn", "io", "timeout", "limit", "exit", "output"]),
    detail: Schema.String,
  },
) {}

const encodeRequest = Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown));
const decodeResponse = Schema.decodeUnknownEffect(
  Schema.fromJsonString(
    Schema.Union([
      Schema.Struct({ version: Schema.Literal(1), ok: Schema.Struct({ output: Schema.String }) }),
      Schema.Struct({
        version: Schema.Literal(1),
        error: NativeGenerationFailure.mapFields(({ _tag, ...fields }) => fields),
      }),
    ]),
  ),
  { onExcessProperty: "error" },
);

const readBounded = Effect.fn("NativeCodexTextGeneration.readBounded")(function* <E>(
  stream: Stream.Stream<Uint8Array, E>,
  limit: number,
) {
  let bytes = 0;
  const chunks: Array<Uint8Array> = [];
  yield* stream.pipe(
    Stream.mapError((cause) => new NativeCodexTextGenerationError({ stage: "response", cause })),
    Stream.runForEach((chunk) => {
      bytes += chunk.byteLength;
      if (bytes > limit)
        return Effect.fail(new NativeCodexTextGenerationError({ stage: "response" }));
      chunks.push(chunk);
      return Effect.void;
    }),
  );
  const data = new Uint8Array(bytes);
  let offset = 0;
  for (const chunk of chunks) {
    data.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return yield* Effect.try({
    try: () => new TextDecoder("utf-8", { fatal: true }).decode(data),
    catch: (cause) => new NativeCodexTextGenerationError({ stage: "response", cause }),
  });
});

export const make = Effect.fn("NativeCodexTextGeneration.make")(function* (input: {
  readonly executable: string;
  readonly config: CodexSettings;
  readonly environment: NodeJS.ProcessEnv;
  readonly getModels: Effect.Effect<ReadonlyArray<ServerProviderModel>>;
  readonly resolveRuntime?:
    | Effect.Effect<CodexEffectiveRuntime, ProviderSetupError, Scope.Scope>
    | undefined;
}) {
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const serverConfig = yield* ServerConfig.ServerConfig;

  const materializeImages = Effect.fn("NativeCodexTextGeneration.materializeImages")(function* (
    attachments: TextGenerationOperations.Request<Schema.Top>["attachments"],
  ) {
    const paths: Array<string> = [];
    for (const attachment of attachments ?? []) {
      if (attachment.type !== "image") continue;
      const resolvedPath = resolveAttachmentPath({
        attachmentsDir: serverConfig.attachmentsDir,
        attachment,
      });
      if (!resolvedPath || !path.isAbsolute(resolvedPath)) continue;
      const info = yield* fileSystem.stat(resolvedPath).pipe(Effect.orElseSucceed(() => null));
      if (info?.type === "File") paths.push(resolvedPath);
    }
    return paths;
  });

  const run = Effect.fn("NativeCodexTextGeneration.run")(function* <S extends Schema.Top>(
    request: TextGenerationOperations.Request<S>,
  ): Effect.fn.Return<S["Type"], TextGenerationError, S["DecodingServices"]> {
    const { operation } = request;
    if (
      !NodePath.isAbsolute(input.executable) ||
      input.executable.includes("\0") ||
      (yield* HostProcess.Platform) === "win32"
    )
      return yield* new TextGenerationError({
        operation,
        detail: "The selected Lust Codex generator requires an absolute executable path on Unix.",
        cause: new NativeCodexTextGenerationError({ stage: "configuration" }),
      });

    const imagePaths = yield* materializeImages(request.attachments);
    const outputSchema = yield* Effect.try({
      try: () => toJsonSchemaObject(request.outputSchema),
      catch: (cause) =>
        new TextGenerationError({
          operation,
          detail: "Failed to encode structured output schema.",
          cause,
        }),
    });
    const timeoutMs = request.investigate?.timeoutMs ?? 180_000;
    const output = yield* Effect.gen(function* () {
      const resolved = input.resolveRuntime
        ? yield* input.resolveRuntime.pipe(
            Effect.mapError(
              (cause) => new TextGenerationError({ operation, detail: cause.detail, cause }),
            ),
          )
        : undefined;
      const config = resolved?.config ?? input.config;
      const environment = resolved?.environment ?? input.environment;
      const models = yield* input.getModels;
      const env = {
        ...environment,
        ...(config.homePath
          ? { CODEX_HOME: expandHomePath(config.homePath, yield* HostProcess.HomeDirectory) }
          : {}),
      };
      const encoded = yield* encodeRequest({
        version: 1,
        operation: "run",
        binary: config.binaryPath || "codex",
        cwd: NodePath.resolve(yield* HostProcess.WorkingDirectory, request.cwd),
        prompt: request.prompt,
        outputSchema,
        launchArgs: config.launchArgs,
        models: models.map((model) => ({
          slug: model.slug,
          isCustom: model.isCustom,
          isDefault: model.isDefault === true,
          isLegacy: model.isLegacy === true,
        })),
        customModels: input.config.customModels,
        modelSelection: {
          model: request.modelSelection.model,
          options: request.modelSelection.options ?? [],
        },
        managed: resolved !== undefined,
        imagePaths,
        timeoutMs,
      }).pipe(
        Effect.mapError((cause) => new NativeCodexTextGenerationError({ stage: "request", cause })),
      );
      const frame = `${encoded}\n`;
      if (new TextEncoder().encode(frame).byteLength > MAX_REQUEST_BYTES)
        return yield* new NativeCodexTextGenerationError({ stage: "request" });
      const child = yield* spawner
        .spawn(
          ChildProcess.make(input.executable, [], {
            env,
            extendEnv: false,
            shell: false,
            stdin: { stream: Stream.encodeText(Stream.make(frame)) },
            stdout: "pipe",
            stderr: "pipe",
            forceKillAfter: "5 seconds",
          }),
        )
        .pipe(
          Effect.mapError(
            (cause) => new NativeCodexTextGenerationError({ stage: "process", cause }),
          ),
        );
      const [stdout, , exitCode] = yield* Effect.all(
        [
          readBounded(child.stdout, MAX_RESPONSE_BYTES),
          readBounded(child.stderr, 64 * 1024),
          child.exitCode.pipe(
            Effect.mapError(
              (cause) => new NativeCodexTextGenerationError({ stage: "process", cause }),
            ),
          ),
        ],
        { concurrency: "unbounded" },
      );
      if (exitCode !== 0)
        return yield* new NativeCodexTextGenerationError({
          stage: "process",
          cause: { exitCode },
        });
      if (!stdout.endsWith("\n") || stdout.indexOf("\n") !== stdout.length - 1)
        return yield* new NativeCodexTextGenerationError({ stage: "response" });
      const response = yield* decodeResponse(stdout).pipe(
        Effect.mapError(
          (cause) => new NativeCodexTextGenerationError({ stage: "response", cause }),
        ),
      );
      if ("error" in response)
        return yield* new TextGenerationError({
          operation,
          detail: response.error.detail,
          cause: new NativeGenerationFailure(response.error),
        });
      if (new TextEncoder().encode(response.ok.output).byteLength > MAX_OUTPUT_BYTES)
        return yield* new NativeCodexTextGenerationError({ stage: "response" });
      return response.ok.output;
    }).pipe(
      Effect.scoped,
      Effect.timeoutOption(timeoutMs),
      Effect.flatMap(
        Option.match({
          onNone: () =>
            Effect.fail(
              new TextGenerationError({ operation, detail: "Codex CLI request timed out." }),
            ),
          onSome: Effect.succeed,
        }),
      ),
      Effect.catchTags({
        NativeCodexTextGenerationError: (cause) =>
          Effect.fail(new TextGenerationError({ operation, detail: cause.message, cause })),
      }),
    );
    const decodeOutput = Schema.decodeEffect(Schema.fromJsonString(request.outputSchema));
    return yield* decodeOutput(output).pipe(
      Effect.mapError(
        (cause) =>
          new TextGenerationError({
            operation,
            detail: "Codex returned invalid structured output.",
            cause,
          }),
      ),
    );
  });

  return TextGenerationOperations.fromRunner("CodexTextGeneration", run);
});
