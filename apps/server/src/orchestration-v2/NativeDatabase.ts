// @effect-diagnostics nodeBuiltinImport:off -- Native database configuration names host executable and database paths.
import * as NodePath from "node:path";
import * as Context from "effect/Context";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import * as ChildProcess from "effect/process/ChildProcess";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";

const MAX_REQUEST_BYTES = 16 * 1024 * 1024;
const MAX_RESPONSE_BYTES = MAX_REQUEST_BYTES + 1_024;
const MAX_JSON_VALUES = MAX_RESPONSE_BYTES;
// Native receipts wrap caller values with metadata beyond the request parser's 64 levels.
const MAX_JSON_DEPTH = 128;

export class NativeDatabaseInputError extends Schema.TaggedError<NativeDatabaseInputError>()(
  "NativeDatabaseInputError",
  { stage: Schema.Literals(["configuration", "request"]) },
) {
  override get message(): string {
    return `Native database ${this.stage} is invalid; no request was sent.`;
  }
}

export class NativeDatabaseUnavailable extends Schema.TaggedError<NativeDatabaseUnavailable>()(
  "NativeDatabaseUnavailable",
  { stage: Schema.Literals(["spawn", "closed"]) },
) {
  override get message(): string {
    return "Native database is unavailable; no request was sent.";
  }
}

const RejectionKind = Schema.Literals([
  "invalid_request",
  "incompatible_store",
  "stale_revision",
  "command_conflict",
  "duplicate_record",
  "transaction_conflict",
  "database_error",
]);

export class NativeDatabaseRejected extends Schema.TaggedError<NativeDatabaseRejected>()(
  "NativeDatabaseRejected",
  { kind: RejectionKind },
) {
  override get message(): string {
    return `Native database rejected the request with ${this.kind}.`;
  }
}

export class NativeDatabaseOutcomeUnknown extends Schema.TaggedError<NativeDatabaseOutcomeUnknown>()(
  "NativeDatabaseOutcomeUnknown",
  {
    stage: Schema.Literals(["write", "response", "timeout", "exit", "cancelled", "database"]),
  },
) {
  override get message(): string {
    return "Native database outcome is unknown; resolve the original operation before retrying.";
  }
}

export type NativeDatabaseError =
  | NativeDatabaseInputError
  | NativeDatabaseUnavailable
  | NativeDatabaseRejected
  | NativeDatabaseOutcomeUnknown;

export class NativeDatabase extends Context.Service<
  NativeDatabase,
  {
    readonly request: <A>(
      input: unknown,
      response: Schema.Decoder<A>,
    ) => Effect.Effect<A, NativeDatabaseError>;
  }
>()("t3/orchestration-v2/NativeDatabase") {}

const decodeEnvelope = Schema.decodeUnknownEffect(
  Schema.fromJsonString(
    Schema.Union([
      Schema.Struct({ ok: Schema.Literal(true), result: Schema.Unknown }),
      Schema.Struct({
        ok: Schema.Literal(false),
        error: Schema.Struct({
          kind: Schema.Union([RejectionKind, Schema.Literal("unknown_commit")]),
          message: Schema.String,
        }),
      }),
    ]),
  ),
  { onExcessProperty: "error" },
);
const encodeRequest = Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown));

function boundedJson(value: unknown): boolean {
  function* children(object: object): Generator<unknown> {
    for (const key in object) {
      if (Object.hasOwn(object, key)) yield Reflect.get(object, key);
    }
  }
  // Each value needs at least one frame byte; iterators bound extra memory by nesting depth.
  const pending: Array<{ iterator: Iterator<unknown>; depth: number }> = [
    { iterator: [value].values(), depth: 0 },
  ];
  let count = 0;
  while (pending.length > 0) {
    const frame = pending.at(-1);
    if (frame === undefined) return false;
    const entry = frame.iterator.next();
    if (entry.done) {
      pending.pop();
      continue;
    }
    if (frame.depth > MAX_JSON_DEPTH || ++count > MAX_JSON_VALUES) return false;
    if (typeof entry.value === "number" && !Number.isFinite(entry.value)) return false;
    if (typeof entry.value !== "object" || entry.value === null) continue;
    pending.push({
      iterator: Array.isArray(entry.value) ? entry.value.values() : children(entry.value),
      depth: frame.depth + 1,
    });
  }
  return true;
}

interface Session {
  readonly scope: Scope.Closeable;
  readonly child: ChildProcessSpawner.ChildProcessHandle;
  pending: Deferred.Deferred<string, NativeDatabaseOutcomeUnknown> | undefined;
  received: boolean;
  failure: NativeDatabaseOutcomeUnknown | undefined;
}

export const make = Effect.fn("NativeDatabase.make")(function* (config: {
  readonly executable: string;
  readonly databasePath: string;
  readonly requestTimeout?: Duration.Input;
}) {
  const { executable, databasePath } = config;
  if (
    [executable, databasePath].some(
      (value) => value.length === 0 || value.includes("\0") || !NodePath.isAbsolute(value),
    )
  )
    return yield* new NativeDatabaseInputError({ stage: "configuration" });
  const timeout = yield* Effect.try({
    try: () => Duration.fromInputUnsafe(config.requestTimeout ?? "30 seconds"),
    catch: () => new NativeDatabaseInputError({ stage: "configuration" }),
  });
  if (!Number.isFinite(Duration.toMillis(timeout)) || Duration.toMillis(timeout) <= 0)
    return yield* new NativeDatabaseInputError({ stage: "configuration" });

  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const owner = yield* Scope.Scope;
  const gate = yield* Semaphore.make(1);
  let current: Session | undefined;
  let closed = false;

  // Stream callbacks share the enclosing request span; they never record payloads or host paths.
  const poison = Effect.fnUntraced(function* (
    session: Session,
    error: NativeDatabaseOutcomeUnknown,
  ) {
    session.failure ??= error;
    if (session.pending !== undefined) yield* Deferred.fail(session.pending, session.failure);
  });
  yield* Effect.addFinalizer(() => {
    closed = true;
    return current === undefined
      ? Effect.void
      : poison(current, new NativeDatabaseOutcomeUnknown({ stage: "cancelled" }));
  });

  const connect = Effect.fn("NativeDatabase.connect")(function* () {
    if (closed) return yield* new NativeDatabaseUnavailable({ stage: "closed" });
    if (current !== undefined && current.failure === undefined) return current;
    if (current !== undefined) yield* Scope.close(current.scope, Exit.void);
    current = undefined;
    const scope = yield* Scope.fork(owner);
    const child = yield* spawner
      .spawn(
        ChildProcess.make(executable, ["local", databasePath], {
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
        Scope.provide(scope),
        Effect.mapError(() => new NativeDatabaseUnavailable({ stage: "spawn" })),
        Effect.catchDefect(() => Effect.fail(new NativeDatabaseUnavailable({ stage: "spawn" }))),
        Effect.timeoutOption(timeout),
        Effect.flatMap((result) =>
          Effect.fromOption(result, () => new NativeDatabaseUnavailable({ stage: "spawn" })),
        ),
        Effect.onError(() => Scope.close(scope, Exit.void)),
      );
    const session: Session = {
      scope,
      child,
      pending: undefined,
      received: false,
      failure: undefined,
    };
    current = session;
    let parts: Array<Uint8Array> = [];
    let bytes = 0;
    const readOutput = child.stdout.pipe(
      Stream.runForEach(
        // Frame handling is per chunk inside the request's transport span.
        Effect.fnUntraced(function* (chunk) {
          if (chunk.byteLength === 0) return;
          const newline = chunk.indexOf(10);
          if (
            session.failure !== undefined ||
            session.pending === undefined ||
            session.received ||
            chunk.includes(13) ||
            (newline !== -1 && newline !== chunk.byteLength - 1) ||
            bytes + chunk.byteLength > MAX_RESPONSE_BYTES
          ) {
            return yield* new NativeDatabaseOutcomeUnknown({ stage: "response" });
          }
          bytes += chunk.byteLength;
          parts.push(chunk);
          if (newline === -1) return;
          const body = yield* Effect.try({
            try: () =>
              new TextDecoder("utf-8", { fatal: true, ignoreBOM: true }).decode(
                Buffer.concat(parts, bytes).subarray(0, bytes - 1),
              ),
            catch: () => new NativeDatabaseOutcomeUnknown({ stage: "response" }),
          });
          parts = [];
          bytes = 0;
          session.received = true;
          yield* Deferred.succeed(session.pending, body);
        }),
      ),
      Effect.mapError(() => new NativeDatabaseOutcomeUnknown({ stage: "response" })),
      Effect.andThen(Effect.fail(new NativeDatabaseOutcomeUnknown({ stage: "exit" }))),
      Effect.catch((error) => poison(session, error)),
    );
    yield* Effect.forkIn(readOutput, scope);
    yield* child.stderr.pipe(
      Stream.runForEach((chunk) =>
        chunk.byteLength === 0
          ? Effect.void
          : Effect.fail(new NativeDatabaseOutcomeUnknown({ stage: "response" })),
      ),
      Effect.mapError(() => new NativeDatabaseOutcomeUnknown({ stage: "response" })),
      Effect.catch((error) => poison(session, error)),
      Effect.forkIn(scope),
    );
    yield* child.exitCode.pipe(
      Effect.ignore,
      Effect.andThen(poison(session, new NativeDatabaseOutcomeUnknown({ stage: "exit" }))),
      Effect.forkIn(scope),
    );
    return session;
  });

  const request = Effect.fn("NativeDatabase.request")(function* <A>(
    input: unknown,
    response: Schema.Decoder<A>,
  ) {
    const encoded = `${yield* encodeRequest(input).pipe(
      Effect.mapError(() => new NativeDatabaseInputError({ stage: "request" })),
    )}\n`;
    if (Buffer.byteLength(encoded, "utf8") > MAX_REQUEST_BYTES)
      return yield* new NativeDatabaseInputError({ stage: "request" });
    return yield* gate.withPermits(1)(
      Effect.gen(function* () {
        const session = yield* connect();
        const pending = yield* Deferred.make<string, NativeDatabaseOutcomeUnknown>();
        session.pending = pending;
        session.received = false;
        return yield* Stream.run(Stream.encodeText(Stream.make(encoded)), session.child.stdin).pipe(
          Effect.mapError(() => new NativeDatabaseOutcomeUnknown({ stage: "write" })),
          Effect.andThen(Deferred.await(pending)),
          Effect.flatMap((body) =>
            decodeEnvelope(body).pipe(
              Effect.mapError(() => new NativeDatabaseOutcomeUnknown({ stage: "response" })),
            ),
          ),
          Effect.flatMap(
            (envelope): Effect.Effect<A, NativeDatabaseOutcomeUnknown | NativeDatabaseRejected> => {
              if (session.failure !== undefined) return Effect.fail(session.failure);
              if (!envelope.ok)
                return envelope.error.kind === "unknown_commit"
                  ? Effect.fail(new NativeDatabaseOutcomeUnknown({ stage: "database" }))
                  : Effect.fail(new NativeDatabaseRejected({ kind: envelope.error.kind }));
              if (!boundedJson(envelope.result))
                return Effect.fail(new NativeDatabaseOutcomeUnknown({ stage: "response" }));
              return Schema.decodeUnknownEffect(response, { onExcessProperty: "error" })(
                envelope.result,
              ).pipe(
                Effect.mapError(() => new NativeDatabaseOutcomeUnknown({ stage: "response" })),
              );
            },
          ),
          Effect.timeoutOption(timeout),
          Effect.flatMap((result) =>
            Effect.fromOption(result, () => new NativeDatabaseOutcomeUnknown({ stage: "timeout" })),
          ),
          Effect.onExit((exit) => {
            session.pending = undefined;
            if (
              Exit.isSuccess(exit) ||
              exit.cause.reasons.every(
                (reason) =>
                  reason._tag === "Fail" && reason.error._tag === "NativeDatabaseRejected",
              )
            )
              return Effect.void;
            session.failure ??= new NativeDatabaseOutcomeUnknown({ stage: "cancelled" });
            return Scope.close(session.scope, Exit.void);
          }),
        );
      }),
    );
  });
  return NativeDatabase.of({ request });
});
