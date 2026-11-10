#!/usr/bin/env node
import * as Assert from "node:assert/strict";
import * as ChildProcess from "node:child_process";
import * as Crypto from "node:crypto";
import * as FS from "node:fs/promises";
import * as Path from "node:path";
import * as Readline from "node:readline";
import * as Sqlite from "node:sqlite";
import * as NodeURL from "node:url";

import { createFixture } from "./database-fixture.mjs";
import { exportSqliteState } from "./export-sqlite-state.mjs";

const [executable, proofPath] = process.argv.slice(2);
Assert.ok(executable && Path.isAbsolute(executable), "Pass the absolute database executable");
await FS.access(executable, FS.constants.X_OK);
const checks = [];
const hash = (bytes) => Crypto.createHash("sha256").update(bytes).digest("hex");

function client(fixture, clients, initialize = false) {
  const child = ChildProcess.spawn(executable, fixture.cliArgs(initialize ? "initialize" : "connect"), {
    env: {}, stdio: ["pipe", "pipe", "pipe"],
  });
  const pending = [];
  let errors = "";
  let exited = false;
  let failure;
  const fail = (error) => {
    failure ??= error;
    for (const request of pending.splice(0)) request.reject(failure);
  };
  child.on("error", fail);
  child.stderr.on("data", (bytes) => { errors = (errors + bytes).slice(-8192); });
  const closed = new Promise((resolve) => child.once("close", (code, signal) => {
    exited = true;
    if (pending.length > 0) fail(new Error(`Database exited: ${code ?? signal}: ${errors}`));
    resolve({ code, signal });
  }));
  Readline.createInterface({ input: child.stdout }).on("line", (line) => {
    const request = pending.shift();
    if (!request) return fail(new Error("Unexpected database response"));
    try { request.resolve(request.discard ? undefined : JSON.parse(line)); }
    catch (error) { request.reject(error); fail(error); }
  });
  const api = {
    send(value, discard = false) {
      if (failure || exited) return Promise.reject(failure ?? new Error("Closed database client"));
      return new Promise((resolve, reject) => {
        const timeout = setTimeout(() => {
          child.kill("SIGKILL");
          fail(new Error("Database request exceeded 45 seconds"));
        }, 45_000);
        pending.push({
          discard,
          resolve: (value) => { clearTimeout(timeout); resolve(value); },
          reject: (error) => { clearTimeout(timeout); reject(error); },
        });
        child.stdin.write(`${JSON.stringify(value)}\n`, (error) => { if (error) fail(error); });
      });
    },
    async close(abrupt = false) {
      if (!exited) {
        if (abrupt || failure) child.kill("SIGKILL");
        else child.stdin.end();
      }
      const status = await closed;
      clients.delete(api);
      if (!abrupt) {
        if (failure) throw failure;
        Assert.equal(status.code, 0, errors);
        Assert.equal(errors, "");
      }
    },
  };
  clients.add(api);
  return api;
}

function ok(response) {
  Assert.equal(response.ok, true, JSON.stringify(response));
  return response.result;
}
const send = async (client, request) => ok(await client.send(request));
const rejected = async (client, request) => {
  const response = await client.send(request);
  Assert.equal(response.ok, false, JSON.stringify(response));
  Assert.equal(response.error.kind, "invalid_request", JSON.stringify(response));
};

async function scenario(name, run) {
  const fixture = await createFixture();
  const clients = new Set();
  const control = client(fixture, clients, true);
  try {
    await send(control, { operation: "snapshot" });
    await run({ fixture, control, connect: () => client(fixture, clients) });
    checks.push(name);
    console.log(`PASS ${name}`);
  } finally {
    for (const connection of clients) await connection.close(true);
    await fixture.close();
  }
}

function exportRows(records) {
  const tables = new Map();
  let table;
  for (const record of records) {
    if (record.type === "table") {
      table = { definition: record, rows: [] };
      tables.set(record.name, table);
    } else if (record.type === "row") {
      const row = Object.fromEntries(table.definition.columns.map((column, index) => [column.name, record.values[index]]));
      const key = table.definition.columns.filter((column) => column.primaryKeyOrder > 0)
        .sort((a, b) => a.primaryKeyOrder - b.primaryKeyOrder).map((column) => row[column.name]);
      table.rows.push({ key, row });
    }
  }
  return tables;
}

async function exportSource(fixture, source) {
  const destination = Path.join(fixture.root, "source.ndjson");
  const summary = exportSqliteState({ source, destination });
  const bytes = await FS.readFile(destination);
  const records = bytes.toString("utf8").trimEnd().split("\n").map((line) => JSON.parse(line));
  return { destination, summary, records, bytes, tables: exportRows(records) };
}

async function compareRows(control, tables) {
  const schema = await send(control, { operation: "application_schema" });
  Assert.equal(schema.tables.length, tables.size);
  for (const [name, table] of tables) {
    await send(control, { operation: "application_begin", transaction_id: "inspect" });
    const actual = [];
    let after = null;
    do {
      const page = await send(control, { operation: "application_scan", transaction_id: "inspect", table: name, after, limit: 16 });
      actual.push(...page.rows);
      after = page.next;
    } while (after !== null);
    await send(control, { operation: "application_rollback", transaction_id: "inspect" });
    const ordered = (rows) => [...rows].sort((left, right) =>
      JSON.stringify(left.key).localeCompare(JSON.stringify(right.key)),
    );
    Assert.deepEqual(ordered(actual), ordered(table.rows), name);
  }
}

function createTypedSource(source) {
  const database = new Sqlite.DatabaseSync(source);
  try {
    database.exec(`
      CREATE TABLE accounts (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, raw BLOB, ratio REAL, nullable TEXT, json TEXT);
      CREATE TABLE grants (account TEXT, permission TEXT, expiry TEXT, PRIMARY KEY (account, permission));
      CREATE TABLE effect_sql_migrations (migration_id INTEGER PRIMARY KEY, name TEXT NOT NULL);
      INSERT INTO effect_sql_migrations VALUES (80, 'ScheduledTaskThreadMode');
      INSERT INTO grants VALUES ('alice', 'read', NULL), ('alice', 'write', '2026-11-10T12:00:00.000Z');
    `);
    database.prepare("INSERT INTO accounts VALUES (?, ?, ?, ?, ?, ?)").run(
      9007199254740993n, "Unicode 😀 and NUL\0", Buffer.from([0, 128, 255]), -1.25, null, '{"enabled":true,"missing":null}',
    );
    database.exec("INSERT INTO accounts (name, raw, ratio, nullable, json) VALUES (CAST(X'80ff00' AS TEXT), X'', 1e999, '', '{}')");
  } finally { database.close(); }
}

await scenario("typed cells, composite keys, sequences and migration metadata survive active hydration", async ({ fixture, control, connect }) => {
  const source = Path.join(fixture.root, "source.sqlite");
  createTypedSource(source);
  const originalHash = hash(await FS.readFile(source));
  const exported = await exportSource(fixture, source);
  Assert.equal(await send(control, { operation: "application_schema" }), null);
  const receipt = await send(control, { operation: "import_sqlite_application", path: exported.destination });
  Assert.equal(receipt.replayed, false);
  Assert.equal(receipt.summary.export_sha256, exported.summary.sha256);
  Assert.equal(receipt.summary.row_count, exported.summary.rowCount);
  await compareRows(control, exported.tables);
  Assert.equal(hash(await FS.readFile(source)), originalHash);
  // Independent-process visibility proves that these are durable table rows.
  const other = connect();
  await compareRows(other, exported.tables);
  const restored = Path.join(fixture.root, "roundtrip.ndjson");
  await send(other, { operation: "restore_sqlite_export", archive_id: receipt.generation, destination: restored });
  Assert.equal(hash(await FS.readFile(restored)), hash(exported.bytes));
  const first = exported.tables.get("accounts").rows[0];
  await send(other, { operation: "application_begin", transaction_id: "update" });
  await send(other, { operation: "application_put", transaction_id: "update", table: "accounts", key: first.key,
    row: { ...first.row, name: { type: "text", value: "after cutover" } } });
  await send(other, { operation: "application_commit", transaction_id: "update" });
  await other.close(true);
  const reopened = connect();
  const replay = await send(reopened, { operation: "import_sqlite_application", path: exported.destination });
  Assert.equal(replay.replayed, true);
  await send(reopened, { operation: "application_begin", transaction_id: "after-replay" });
  const updated = await send(reopened, { operation: "application_get", transaction_id: "after-replay", table: "accounts", key: first.key });
  Assert.equal(updated.name.value, "after cutover");
  await send(reopened, { operation: "application_rollback", transaction_id: "after-replay" });
  Assert.deepEqual(await send(reopened, { operation: "snapshot" }), { revision: 0, last_sequence: 0, projections: [] });
});

await scenario("the checked-in application SQLite fixture is queryable as native rows", async ({ fixture, control }) => {
  const source = NodeURL.fileURLToPath(new URL("../scripts/fixtures/desktop-launch/statev2.sqlite", import.meta.url));
  const originalHash = hash(await FS.readFile(source));
  const exported = await exportSource(fixture, source);
  const receipt = await send(control, { operation: "import_sqlite_application", path: exported.destination });
  Assert.ok(receipt.summary.table_count > 30);
  Assert.ok(receipt.excluded_derived_tables.includes("orchestration_v2_message_search"));
  await compareRows(control, exported.tables);
  Assert.equal(hash(await FS.readFile(source)), originalHash);
});

await scenario("a lost import acknowledgement resolves from the durable receipt after restart", async ({ fixture, control, connect }) => {
  const source = Path.join(fixture.root, "source.sqlite");
  createTypedSource(source);
  const exported = await exportSource(fixture, source);
  await control.send({ operation: "import_sqlite_application", path: exported.destination }, true);
  await control.close(true);
  const recovered = connect();
  const receipt = await send(recovered, { operation: "application_import" });
  Assert.equal(receipt.generation, exported.summary.sha256);
  Assert.equal(receipt.replayed, true);
  await compareRows(recovered, exported.tables);
});

for (const kind of ["no primary key", "null primary key", "corrupt manifest"]) {
  await scenario(`${kind} cannot publish a partial application catalog`, async ({ fixture, control }) => {
    const source = Path.join(fixture.root, "source.sqlite");
    createTypedSource(source);
    if (kind !== "corrupt manifest") {
      const database = new Sqlite.DatabaseSync(source);
      try {
        database.exec(kind === "no primary key"
          ? "CREATE TABLE z_unsupported (value TEXT); INSERT INTO z_unsupported VALUES ('must remain preserved')"
          : "CREATE TABLE z_unsupported (id TEXT PRIMARY KEY, value TEXT); INSERT INTO z_unsupported VALUES (NULL, 'preserved')");
      } finally { database.close(); }
    }
    const original = hash(await FS.readFile(source));
    const exported = await exportSource(fixture, source);
    if (kind === "corrupt manifest") {
      const corrupted = exported.bytes.toString("utf8").replace('"rowCount":2', '"rowCount":3');
      Assert.notEqual(corrupted, exported.bytes.toString("utf8"));
      await FS.writeFile(exported.destination, corrupted);
    }
    await rejected(control, { operation: "import_sqlite_application", path: exported.destination });
    Assert.equal(await send(control, { operation: "application_schema" }), null);
    Assert.equal(await send(control, { operation: "application_import" }), null);
    Assert.equal(hash(await FS.readFile(source)), original);
  });
}

await scenario("existing native tables and a different active import are never overwritten", async ({ fixture, control }) => {
  const source = Path.join(fixture.root, "source.sqlite");
  createTypedSource(source);
  const exported = await exportSource(fixture, source);
  const schema = { version: 1, tables: [{ name: "kept", columns: ["id", "value"], primary_key: ["id"] }] };
  await send(control, { operation: "application_init", schema });
  await rejected(control, { operation: "import_sqlite_application", path: exported.destination });
  Assert.deepEqual(await send(control, { operation: "application_schema" }), schema);
});

await scenario("competing importers publish one complete generation", async ({ fixture, control, connect }) => {
  const source = Path.join(fixture.root, "source.sqlite");
  createTypedSource(source);
  const exported = await exportSource(fixture, source);
  // Archive first to isolate the actual activation race from archive publication.
  await send(control, { operation: "archive_sqlite", path: exported.destination });
  const other = connect();
  const importOnce = async (connection) => {
    for (let attempt = 0; attempt < 6; attempt++) {
      const result = await connection.send({ operation: "import_sqlite_application", path: exported.destination });
      if (!result.ok && result.error.kind === "transaction_conflict") continue;
      return ok(result);
    }
    Assert.fail("Repeated known import conflicts");
  };
  const receipts = await Promise.all([importOnce(control), importOnce(other)]);
  Assert.ok(receipts.every((receipt) => receipt.generation === exported.summary.sha256));
  await compareRows(control, exported.tables);
  // A new source generation cannot replace data after this one became visible.
  const database = new Sqlite.DatabaseSync(source);
  database.exec("UPDATE accounts SET name='new source'");
  database.close();
  const different = Path.join(fixture.root, "different.ndjson");
  exportSqliteState({ source, destination: different });
  await rejected(control, { operation: "import_sqlite_application", path: different });
  await compareRows(control, exported.tables);
});

const proof = {
  schema: 1,
  executable_sha256: hash(await FS.readFile(executable)),
  engine: "redb 4.5.0 / MultiWriter / Immediate",
  checks,
  scope: "Native typed-row hydration and atomic catalog activation. Production SQL repository cutover and derived search indexes remain unimplemented.",
};
if (proofPath) {
  await FS.mkdir(Path.dirname(Path.resolve(proofPath)), { recursive: true });
  await FS.writeFile(proofPath, `${JSON.stringify(proof, null, 2)}\n`);
}
console.log(`Passed ${checks.length} native application import scenarios.`);
