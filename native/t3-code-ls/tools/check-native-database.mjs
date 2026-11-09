import * as NodeAssert from "node:assert/strict";
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Schema from "effect/Schema";

import * as NativeDatabase from "../../../apps/server/src/orchestration-v2/NativeDatabase.ts";

const maximumBytes = 16 * 1024 * 1024;
const proofPath = process.argv[2];
const directory = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "t3-native-db-client-"));
const executable = NodePath.join(directory, "database-fixture");
const databasePath = NodePath.join(directory, "state.redb");
let checks = 0;

const fixture =
  `#!${process.execPath}\n` +
  String.raw`
import * as FS from "node:fs";
import * as Readline from "node:readline";
let count = 0;
for await (const line of Readline.createInterface({ input: process.stdin })) {
  const request = JSON.parse(line);
  count++;
  switch (request.action) {
    case "rejected":
      process.stdout.write(JSON.stringify({ ok: false, error: {
        kind: request.kind, message: "/private/fixture/database.redb"
      } }) + "\n");
      break;
    case "double":
      process.stdout.write('{"ok":true,"result":1}\n{"ok":true,"result":2}\n');
      break;
    case "invalid-utf8":
      process.stdout.write(Buffer.from([0xff, 10]));
      break;
    case "bom":
      process.stdout.write('\ufeff{"ok":true,"result":1}\n');
      break;
    case "crlf":
      process.stdout.write('{"ok":true,"result":1}\r\n');
      break;
    case "malformed":
      process.stdout.write('{"ok":true,"result":}\n');
      break;
    case "extra-field":
      process.stdout.write('{"ok":true,"result":1,"extra":true}\n');
      break;
    case "deep":
      process.stdout.write('{"ok":true,"result":' + '['.repeat(129) + '0' + ']'.repeat(129) + '}\n');
      break;
    case "deep-metadata":
      process.stdout.write('{"ok":true,"result":' + '['.repeat(65) + '0' + ']'.repeat(65) + '}\n');
      break;
    case "oversized":
      process.stdout.write(JSON.stringify({ ok: true, result: "x".repeat(16 * 1024 * 1024 + 1024) }) + "\n");
      break;
    case "maximum":
      process.stdout.write(JSON.stringify({ ok: true, result: "x".repeat(16 * 1024 * 1024 - 2) }) + "\n");
      break;
    case "many-values":
      process.stdout.write(JSON.stringify({ ok: true, result: Array(262_145).fill(0) }) + "\n");
      break;
    case "stderr":
      process.stderr.write("/private/fixture/database.redb");
      break;
    case "partial":
      process.stdout.write('{"ok":true');
      process.exit(0);
      break;
    case "cancel":
      FS.writeFileSync(request.marker, "received");
      break;
    case "timeout":
      break;
    default:
      process.stdout.write(JSON.stringify({ ok: true, result: {
        pid: process.pid, count, environment: Object.keys(process.env), args: process.argv.slice(2)
      } }) + "\n");
  }
}
`;

const Echo = Schema.Struct({
  pid: Schema.Number,
  count: Schema.Number,
  environment: Schema.Array(Schema.String),
  args: Schema.Array(Schema.String),
});
const failureOf = (effect, tag) =>
  effect.pipe(
    Effect.catch(Effect.succeed),
    Effect.map((result) => {
      NodeAssert.equal(result?._tag, tag);
      NodeAssert.ok(!JSON.stringify(result).includes("/private/"));
      checks++;
      return result;
    }),
  );

try {
  await NodeFSP.writeFile(executable, fixture, { mode: 0o700 });
  let escaped;
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const database = yield* NativeDatabase.make({ executable, databasePath });
        escaped = database;
        const first = yield* database.request({ action: "echo" }, Echo);
        NodeAssert.equal(first.count, 1);
        NodeAssert.deepEqual(first.environment, []);
        NodeAssert.deepEqual(first.args, ["local", databasePath]);
        checks++;

        const concurrent = yield* Effect.all(
          [database.request({ action: "echo" }, Echo), database.request({ action: "echo" }, Echo)],
          { concurrency: "unbounded" },
        );
        NodeAssert.deepEqual(concurrent.map((result) => result.count).sort(), [2, 3]);
        NodeAssert.ok(concurrent.every((result) => result.pid === first.pid));
        checks++;

        for (const kind of [
          "invalid_request",
          "incompatible_store",
          "stale_revision",
          "command_conflict",
          "duplicate_record",
          "transaction_conflict",
          "database_error",
        ]) {
          const error = yield* failureOf(
            database.request({ action: "rejected", kind }, Schema.Unknown),
            "NativeDatabaseRejected",
          );
          NodeAssert.equal(error.kind, kind);
        }
        NodeAssert.equal((yield* database.request({ action: "echo" }, Echo)).pid, first.pid);
        checks++;

        const uncertain = yield* failureOf(
          database.request({ action: "rejected", kind: "unknown_commit" }, Schema.Unknown),
          "NativeDatabaseOutcomeUnknown",
        );
        NodeAssert.equal(uncertain.stage, "database");
        NodeAssert.equal((yield* database.request({ action: "echo" }, Echo)).count, 1);
        checks++;

        for (const action of [
          "double",
          "invalid-utf8",
          "bom",
          "crlf",
          "malformed",
          "extra-field",
          "deep",
          "oversized",
          "stderr",
          "partial",
        ]) {
          yield* failureOf(
            database.request({ action }, Schema.Unknown),
            "NativeDatabaseOutcomeUnknown",
          );
          NodeAssert.equal((yield* database.request({ action: "echo" }, Echo)).count, 1, action);
          checks++;
        }

        yield* failureOf(
          database.request({ action: "echo" }, Schema.String),
          "NativeDatabaseOutcomeUnknown",
        );
        const healthy = yield* database.request({ action: "echo" }, Echo);
        NodeAssert.equal(healthy.count, 1);
        const maximum = yield* database.request({ action: "maximum" }, Schema.String);
        NodeAssert.equal(maximum.length, maximumBytes - 2);
        checks++;
        const manyValues = yield* database.request(
          { action: "many-values" },
          Schema.Array(Schema.Number),
        );
        NodeAssert.equal(manyValues.length, 262_145);
        checks++;
        let nested = yield* database.request({ action: "deep-metadata" }, Schema.Unknown);
        for (let depth = 0; depth < 65; depth++) {
          NodeAssert.equal(nested.length, 1);
          nested = nested[0];
        }
        NodeAssert.equal(nested, 0);
        checks++;

        const template = { action: "echo", padding: "" };
        const exact = {
          ...template,
          padding: "x".repeat(maximumBytes - 1 - Buffer.byteLength(JSON.stringify(template))),
        };
        NodeAssert.equal((yield* database.request(exact, Echo)).pid, healthy.pid);
        yield* failureOf(
          database.request({ ...exact, padding: exact.padding + "x" }, Echo),
          "NativeDatabaseInputError",
        );
        yield* failureOf(
          database.request({ value: 1n }, Schema.Unknown),
          "NativeDatabaseInputError",
        );
        const circular = {};
        circular.self = circular;
        yield* failureOf(database.request(circular, Schema.Unknown), "NativeDatabaseInputError");
        NodeAssert.equal((yield* database.request({ action: "echo" }, Echo)).pid, healthy.pid);
        checks++;

        const marker = NodePath.join(directory, "cancel-received");
        const notification = Promise.withResolvers();
        const watcher = NodeFS.watch(directory, (_event, name) => {
          if (name === "cancel-received") notification.resolve();
        });
        watcher.on("error", notification.reject);
        try {
          const pending = yield* database
            .request({ action: "cancel", marker }, Schema.Unknown)
            .pipe(Effect.forkChild);
          yield* Effect.promise(() => notification.promise);
          yield* Fiber.interrupt(pending);
        } finally {
          watcher.close();
        }
        const afterCancellation = yield* database.request({ action: "echo" }, Echo);
        NodeAssert.equal(afterCancellation.count, 1);
        NodeAssert.notEqual(afterCancellation.pid, healthy.pid);
        checks++;

        const withDeadline = yield* NativeDatabase.make({
          executable,
          databasePath,
          requestTimeout: "1 second",
        });
        yield* withDeadline.request({ action: "echo" }, Echo);
        const timeout = yield* failureOf(
          withDeadline.request({ action: "timeout" }, Schema.Unknown),
          "NativeDatabaseOutcomeUnknown",
        );
        NodeAssert.equal(timeout.stage, "timeout");
        NodeAssert.equal((yield* withDeadline.request({ action: "echo" }, Echo)).count, 1);
        checks++;

        yield* failureOf(
          NativeDatabase.make({ executable: "relative", databasePath }),
          "NativeDatabaseInputError",
        );
        const missing = yield* NativeDatabase.make({
          executable: NodePath.join(directory, "missing"),
          databasePath,
        });
        yield* failureOf(missing.request({ action: "echo" }, Echo), "NativeDatabaseUnavailable");
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  );
  const closed = await Effect.runPromise(
    failureOf(escaped.request({ action: "echo" }, Echo), "NativeDatabaseUnavailable"),
  );
  NodeAssert.equal(closed.stage, "closed");
  const sources = {};
  for (const [path, url] of [
    [
      "apps/server/src/orchestration-v2/NativeDatabase.ts",
      new URL("../../../apps/server/src/orchestration-v2/NativeDatabase.ts", import.meta.url),
    ],
    ["native/t3-code-ls/tools/check-native-database.mjs", new URL(import.meta.url)],
  ]) {
    sources[path] = NodeCrypto.createHash("sha256")
      .update(await NodeFSP.readFile(url))
      .digest("hex");
  }
  const report = {
    version: 1,
    success: true,
    checks,
    sources,
    coverage: [
      "persistent process, serialized requests, empty environment and direct argv",
      "known rejection and uncertain outcomes have distinct typed errors",
      "bounded JSONL/UTF-8/schema validation and exact request limit including newline",
      "transport failure, deadline and interruption close the old process before explicit reconnect",
      "response envelope allowance preserves maximum-sized result payloads",
      "response value count allows native metadata beyond the request parser limit",
      "response depth allows receipt wrappers around maximum-depth request values",
    ],
  };
  if (proofPath !== undefined) {
    await NodeFSP.mkdir(NodePath.dirname(NodePath.resolve(proofPath)), { recursive: true });
    await NodeFSP.writeFile(proofPath, JSON.stringify(report, null, 2) + "\n");
  }
  console.log(JSON.stringify({ success: true, checks, coverage: report.coverage }));
} finally {
  await NodeFSP.rm(directory, { recursive: true, force: true });
}
