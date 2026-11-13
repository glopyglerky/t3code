// @effect-diagnostics nodeBuiltinImport:off -- Native policy configuration names a host executable path.
import * as NodePath from "node:path";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as ChildProcess from "effect/process/ChildProcess";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";

const MAX_BYTES = 1_048_576;
const MAX_OBSERVATIONS = 4_096;

export class NativeMaintenancePolicyError extends Schema.TaggedError<NativeMaintenancePolicyError>()(
  "NativeMaintenancePolicyError",
  {
    stage: Schema.Literals([
      "configuration",
      "request",
      "process",
      "response",
      "observation",
      "timeout",
    ]),
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    return `Lust maintenance resolution failed during ${this.stage}.`;
  }
}

const Observation = Schema.Union([
  Schema.Struct({ kind: Schema.Literal("wrapper") }),
  Schema.Struct({ kind: Schema.Literal("resolveMise") }),
  Schema.Struct({ kind: Schema.Literal("miseWhich") }),
  Schema.Struct({ kind: Schema.Literal("miseListing") }),
  Schema.Struct({ kind: Schema.Literal("miseInstallPaths"), paths: Schema.Array(Schema.String) }),
  Schema.Struct({ kind: Schema.Literal("miseToolPath"), path: Schema.String }),
  Schema.Struct({ kind: Schema.Literal("pnpmVersion") }),
  Schema.Struct({ kind: Schema.Literal("voltaPackage") }),
  Schema.Struct({ kind: Schema.Literal("npmShim") }),
  Schema.Struct({ kind: Schema.Literal("homebrewExecutable"), prefix: Schema.String }),
  Schema.Struct({ kind: Schema.Literal("homebrewPrefix") }),
]);

const Lookup = Schema.Union([
  Schema.Struct({
    kind: Schema.Literal("mise"),
    executable: Schema.String,
    tool: Schema.String,
  }),
  Schema.Struct({
    kind: Schema.Literal("homebrew"),
    executable: Schema.String,
    name: Schema.String,
    ownershipKind: Schema.Literals(["formula", "cask"]),
  }),
  Schema.Struct({
    kind: Schema.Literals(["pnpm", "vite-plus", "bun"]),
    packageName: Schema.String,
  }),
  Schema.Struct({ kind: Schema.Literal("chocolatey") }),
]);

const Capabilities = Schema.Struct({
  provider: Schema.String,
  packageName: Schema.NullOr(Schema.String),
  update: Schema.NullOr(
    Schema.Struct({
      command: Schema.String,
      executable: Schema.String,
      args: Schema.Array(Schema.String),
      lockKey: Schema.String,
      env: Schema.Literals(["context", "native", "none"]),
    }),
  ),
  installPath: Schema.optional(Schema.String),
  manualCommand: Schema.optional(Schema.String),
  latestVersion: Schema.optional(Schema.Null),
  latestLookup: Schema.optional(Lookup),
});

const Response = Schema.Struct({
  version: Schema.Literal(1),
  ok: Schema.Union([
    Schema.Struct({ kind: Schema.Literal("observe"), request: Observation }),
    Schema.Struct({ kind: Schema.Literal("resolved"), capabilities: Schema.NullOr(Capabilities) }),
    Schema.Struct({
      kind: Schema.Literal("selected"),
      action: Schema.NullOr(
        Schema.Struct({ command: Schema.String, args: Schema.Array(Schema.String) }),
      ),
    }),
    Schema.Struct({
      kind: Schema.Literal("verified"),
      mode: Schema.Literal("pre-update"),
      alreadyCurrent: Schema.Boolean,
    }),
    ...(
      [
        [
          "unchanged",
          "Update command completed, but T3 Code could not verify the provider version.",
        ],
        [
          "unchanged",
          "Update command completed, but T3 Code still detects an outdated provider version.",
        ],
        ["succeeded", "Provider updated."],
        [
          "succeeded",
          "OpenCode CLI updated. Restart the configured local OpenCode server to use the new version.",
        ],
      ] as const
    ).map(([status, message]) =>
      Schema.Struct({
        kind: Schema.Literal("verified"),
        mode: Schema.Literal("post-update"),
        status: Schema.Literal(status),
        message: Schema.Literal(message),
      }),
    ),
  ]),
});
const decodeResponse = Schema.decodeUnknownEffect(Schema.fromJsonString(Response), {
  onExcessProperty: "error",
});
const encodeRequest = Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown));

export type Observation = typeof Observation.Type;
export type Capabilities = typeof Capabilities.Type;
export interface Evidence {
  readonly request: Observation;
  readonly result: unknown;
}
export interface Request {
  readonly mode: "package" | "mise" | "homebrew";
  readonly owner: { readonly provider: string; readonly packageName: string | null };
  readonly context: {
    readonly resolvedCommandPath: string;
    readonly realCommandPath: string;
    readonly platform: string;
    readonly hostPlatform: string;
    readonly commandPlatform: string;
    readonly chocolateyInstall: string | null;
  } | null;
  readonly definition: {
    readonly mise: { readonly tool: string; readonly upgrade: boolean } | null;
    readonly native: {
      readonly args: ReadonlyArray<string>;
      readonly matchesCommandPath: boolean;
      readonly requiresCommandPath: boolean;
      readonly hasEnv: boolean;
    } | null;
  };
  readonly homebrew: {
    readonly kind: "formula" | "cask";
    readonly name: string;
    readonly prefix: string;
  } | null;
}

export interface UpdateActionRequest {
  readonly mode: "target" | "latest";
  readonly packageName: string | null;
  readonly update: {
    readonly command: string;
    readonly args: ReadonlyArray<string>;
    readonly lockKey: string;
  } | null;
  readonly versionValue: string | null;
}

export interface VerificationSnapshot {
  readonly driver?: string;
  readonly checkedAt: string;
  readonly installed: boolean;
  readonly status: string;
  readonly authStatus: string;
  readonly version: string | null;
  readonly installedVersion?: string | null;
  readonly versionAdvisoryStatus: string | null;
}

export type UpdateVerificationRequest =
  | {
      readonly mode: "pre-update";
      readonly priorCheckedAt: string | null;
      readonly advisoryEnriched: boolean;
      readonly verifiedProviders: ReadonlyArray<VerificationSnapshot>;
    }
  | {
      readonly mode: "post-update";
      readonly targetVersion: string | null;
      readonly verifiedProviders: ReadonlyArray<VerificationSnapshot>;
    };

function boundedJson(value: unknown): boolean {
  const pending = [{ value, depth: 0 }];
  let count = 0;
  while (pending.length > 0) {
    const entry = pending.pop();
    if (entry === undefined || entry.depth > 32 || ++count > 131_072) return false;
    if (typeof entry.value === "number" && !Number.isFinite(entry.value)) return false;
    if (typeof entry.value === "string") {
      for (const character of entry.value) {
        const point = character.codePointAt(0);
        if (point !== undefined && point >= 0xd800 && point <= 0xdfff) return false;
      }
    }
    if (Array.isArray(entry.value)) {
      for (const value of entry.value) pending.push({ value, depth: entry.depth + 1 });
    } else if (typeof entry.value === "object" && entry.value !== null) {
      for (const value of Object.values(entry.value))
        pending.push({ value, depth: entry.depth + 1 });
    }
  }
  return true;
}

const exchange = Effect.fn("NativeMaintenancePolicy.exchange")(function* <R>(
  executable: string,
  input: Request | UpdateActionRequest | UpdateVerificationRequest,
  observe: (
    request: Observation,
    evidence: ReadonlyArray<Evidence>,
  ) => Effect.Effect<unknown, NativeMaintenancePolicyError, R>,
) {
  const selection = input.mode === "target" || input.mode === "latest";
  const verification = input.mode === "pre-update" || input.mode === "post-update";
  const terminalKind = selection ? "selected" : verification ? "verified" : "resolved";
  if (!NodePath.isAbsolute(executable) || executable.includes("\0"))
    return yield* new NativeMaintenancePolicyError({ stage: "configuration" });
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  return yield* Effect.gen(function* () {
    const scope = yield* Scope.Scope;
    const child = yield* spawner
      .spawn(
        ChildProcess.make(executable, [], {
          env: {},
          extendEnv: false,
          shell: false,
          stdin: { stream: "pipe", endOnDone: false },
          stdout: "pipe",
          stderr: "pipe",
          forceKillAfter: "1 second",
        }),
      )
      .pipe(
        Effect.mapError((cause) => new NativeMaintenancePolicyError({ stage: "process", cause })),
        Effect.catchDefect((cause) =>
          Effect.fail(new NativeMaintenancePolicyError({ stage: "process", cause })),
        ),
      );
    let pending: Deferred.Deferred<string, NativeMaintenancePolicyError> | undefined;
    let failure: NativeMaintenancePolicyError | undefined;
    let received = false;
    let ended = false;
    let parts: Array<Uint8Array> = [];
    let bytes = 0;
    // These callbacks handle chunks inside the enclosing resolution span.
    const poison = Effect.fnUntraced(function* (error: NativeMaintenancePolicyError) {
      failure ??= error;
      if (pending !== undefined) yield* Deferred.fail(pending, failure);
    });
    const output = yield* child.stdout.pipe(
      Stream.runForEach(
        Effect.fnUntraced(function* (chunk) {
          if (chunk.byteLength === 0) return;
          const newline = chunk.indexOf(10);
          if (
            failure !== undefined ||
            pending === undefined ||
            received ||
            chunk.includes(13) ||
            (newline !== -1 && newline !== chunk.byteLength - 1) ||
            bytes + chunk.byteLength > MAX_BYTES
          )
            return yield* new NativeMaintenancePolicyError({ stage: "response" });
          parts.push(chunk);
          bytes += chunk.byteLength;
          if (newline === -1) return;
          const body = yield* Effect.try({
            try: () =>
              new TextDecoder("utf-8", { fatal: true, ignoreBOM: true }).decode(
                Buffer.concat(parts, bytes).subarray(0, bytes - 1),
              ),
            catch: (cause) => new NativeMaintenancePolicyError({ stage: "response", cause }),
          });
          parts = [];
          bytes = 0;
          received = true;
          yield* Deferred.succeed(pending, body);
        }),
      ),
      Effect.mapError((cause) =>
        cause._tag === "NativeMaintenancePolicyError"
          ? cause
          : new NativeMaintenancePolicyError({ stage: "response", cause }),
      ),
      Effect.andThen(
        Effect.suspend(() => {
          ended = true;
          return received
            ? Effect.void
            : Effect.fail(new NativeMaintenancePolicyError({ stage: "process" }));
        }),
      ),
      Effect.catch(poison),
      Effect.forkIn(scope),
    );
    const stderr = yield* child.stderr.pipe(
      Stream.runForEach((chunk) =>
        chunk.byteLength === 0
          ? Effect.void
          : Effect.fail(new NativeMaintenancePolicyError({ stage: "response" })),
      ),
      Effect.mapError((cause) =>
        cause._tag === "NativeMaintenancePolicyError"
          ? cause
          : new NativeMaintenancePolicyError({ stage: "response", cause }),
      ),
      Effect.catch(poison),
      Effect.forkIn(scope),
    );
    yield* child.exitCode.pipe(
      // Process exit may precede stdout delivery; EOF validates a clean exit's framing.
      Effect.flatMap((code) =>
        Number(code) === 0
          ? Effect.void
          : poison(new NativeMaintenancePolicyError({ stage: "process" })),
      ),
      Effect.catch((cause) =>
        poison(new NativeMaintenancePolicyError({ stage: "process", cause })),
      ),
      Effect.forkIn(scope),
    );
    const evidence: Array<Evidence> = [];
    let aggregateBytes = 0;
    for (let step = 0; step <= MAX_OBSERVATIONS; step++) {
      const request = selection
        ? { version: 1, operation: "select-update-action", ...input }
        : verification
          ? { version: 1, operation: "verify-update", ...input }
          : { version: 1, operation: "resolve", ...input, observations: evidence };
      if (!boundedJson(request))
        return yield* new NativeMaintenancePolicyError({ stage: "request" });
      const frame = `${yield* encodeRequest(request).pipe(
        Effect.mapError((cause) => new NativeMaintenancePolicyError({ stage: "request", cause })),
      )}\n`;
      const frameBytes = Buffer.byteLength(frame, "utf8");
      aggregateBytes += frameBytes;
      if (frameBytes > MAX_BYTES || aggregateBytes > 16 * MAX_BYTES)
        return yield* new NativeMaintenancePolicyError({ stage: "request" });
      if (failure !== undefined) return yield* failure;
      pending = yield* Deferred.make<string, NativeMaintenancePolicyError>();
      received = false;
      yield* Stream.run(Stream.encodeText(Stream.make(frame)), child.stdin).pipe(
        Effect.mapError((cause) => new NativeMaintenancePolicyError({ stage: "process", cause })),
      );
      const body = yield* Deferred.await(pending);
      pending = undefined;
      const response = yield* decodeResponse(body).pipe(
        Effect.mapError((cause) => new NativeMaintenancePolicyError({ stage: "response", cause })),
      );
      if (failure !== undefined || !boundedJson(response))
        return yield* failure ?? new NativeMaintenancePolicyError({ stage: "response" });
      if (response.ok.kind !== "observe") {
        if (response.ok.kind !== terminalKind)
          return yield* new NativeMaintenancePolicyError({ stage: "response" });
        const code = yield* child.exitCode.pipe(
          Effect.mapError((cause) => new NativeMaintenancePolicyError({ stage: "process", cause })),
        );
        yield* Fiber.join(output);
        yield* Fiber.join(stderr);
        if (Number(code) !== 0 || failure !== undefined)
          return yield* failure ?? new NativeMaintenancePolicyError({ stage: "process" });
        return { result: response.ok, evidence };
      }
      if (selection || verification)
        return yield* new NativeMaintenancePolicyError({ stage: "response" });
      if (ended) return yield* new NativeMaintenancePolicyError({ stage: "process" });
      const observation = response.ok.request;
      if (
        step === MAX_OBSERVATIONS ||
        evidence.some((entry) => JSON.stringify(entry.request) === JSON.stringify(observation))
      )
        return yield* new NativeMaintenancePolicyError({ stage: "observation" });
      const result = yield* observe(observation, evidence);
      evidence.push({ request: observation, result });
    }
    return yield* new NativeMaintenancePolicyError({ stage: "observation" });
  }).pipe(
    Effect.scoped,
    Effect.timeoutOption("60 seconds"),
    Effect.flatMap((result) =>
      Effect.fromOption(result, () => new NativeMaintenancePolicyError({ stage: "timeout" })),
    ),
  );
});

export const resolve = Effect.fn("NativeMaintenancePolicy.resolve")(function* <R>(
  executable: string,
  input: Request,
  observe: (
    request: Observation,
    evidence: ReadonlyArray<Evidence>,
  ) => Effect.Effect<unknown, NativeMaintenancePolicyError, R>,
) {
  const { result, evidence } = yield* exchange(executable, input, observe);
  if (result.kind !== "resolved")
    return yield* new NativeMaintenancePolicyError({ stage: "response" });
  return { capabilities: result.capabilities, evidence };
});

export const selectUpdateAction = Effect.fn("NativeMaintenancePolicy.selectUpdateAction")(
  function* (executable: string, input: UpdateActionRequest) {
    const { result } = yield* exchange(executable, input, () =>
      Effect.fail(new NativeMaintenancePolicyError({ stage: "response" })),
    );
    if (result.kind !== "selected")
      return yield* new NativeMaintenancePolicyError({ stage: "response" });
    return result.action;
  },
);

export const verifyUpdate = Effect.fn("NativeMaintenancePolicy.verifyUpdate")(function* (
  executable: string,
  input: UpdateVerificationRequest,
) {
  const { result } = yield* exchange(executable, input, () =>
    Effect.fail(new NativeMaintenancePolicyError({ stage: "response" })),
  );
  if (result.kind !== "verified" || result.mode !== input.mode)
    return yield* new NativeMaintenancePolicyError({ stage: "response" });
  return result;
});
