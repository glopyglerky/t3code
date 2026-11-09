import * as Assert from "node:assert/strict";
import * as Crypto from "node:crypto";
import * as FS from "node:fs";
import * as OS from "node:os";
import * as Path from "node:path";
import { spawnSync } from "node:child_process";
import { DatabaseSync } from "node:sqlite";
import { test } from "node:test";
import { fileURLToPath } from "node:url";

import { exportSqliteState, sqliteSnapshotLines } from "./export-sqlite-state.mjs";

const tool = fileURLToPath(new URL("./export-sqlite-state.mjs", import.meta.url));
const sha256 = (value) => Crypto.createHash("sha256").update(value).digest("hex");

function fixture(run) {
  const directory = FS.mkdtempSync(Path.join(OS.tmpdir(), "t3ls-sqlite-export-"));
  const source = Path.join(directory, "source.sqlite");
  const destination = Path.join(directory, "export.ndjson");
  const database = new DatabaseSync(source);
  try {
    database.exec(`
      PRAGMA journal_mode = WAL;
      PRAGMA wal_autocheckpoint = 0;
      PRAGMA user_version = 17;
      PRAGMA application_id = 42;
      CREATE TABLE records (id INTEGER PRIMARY KEY AUTOINCREMENT, payload TEXT, amount INTEGER, fraction REAL, bytes BLOB, absent);
      CREATE TABLE empty_table (id TEXT PRIMARY KEY) WITHOUT ROWID;
      CREATE TABLE effect_sql_migrations (migration_id INTEGER PRIMARY KEY, name TEXT NOT NULL, created_at TEXT NOT NULL);
      INSERT INTO effect_sql_migrations VALUES (80, 'ScheduledTaskThreadMode', '2026-11-08T12:00:00.000Z');
      CREATE TABLE compound (left_key TEXT, right_key INTEGER, payload TEXT, PRIMARY KEY(left_key, right_key)) WITHOUT ROWID;
      INSERT INTO compound VALUES ('z', 1, 'last'), ('a', 2, 'middle'), ('a', 1, 'first');
      CREATE INDEX records_payload ON records(payload);
      CREATE VIEW visible_records AS SELECT id, payload FROM records;
      CREATE TABLE orchestration_v2_projection_messages (message_id TEXT PRIMARY KEY, payload_json TEXT NOT NULL);
      INSERT INTO orchestration_v2_projection_messages VALUES ('message:1', '{"text":"retained message text"}');
      CREATE VIRTUAL TABLE orchestration_v2_message_search USING fts5(text, content = '', contentless_delete = 1, tokenize = 'trigram remove_diacritics 1');
      INSERT INTO orchestration_v2_message_search(rowid,text) VALUES (1,'retained message text');
      CREATE TABLE orchestration_v2_message_search_other (payload TEXT);
      INSERT INTO orchestration_v2_message_search_other VALUES ('authoritative despite prefix');
    `);
    const insert = database.prepare(
      "INSERT INTO records (payload,amount,fraction,bytes,absent) VALUES (?,?,?,?,?)",
    );
    insert.run(
      'a\u0000 💬 "line\n',
      9_223_372_036_854_775_807n,
      1.25,
      Buffer.from([0, 128, 255]),
      null,
    );
    insert.run("minimum", -9_223_372_036_854_775_808n, Infinity, Buffer.alloc(0), null);
    database.exec(
      "INSERT INTO records(payload) VALUES (CAST(x'80FF' AS TEXT)); INSERT INTO records(payload) VALUES (CAST(x'EFBBBF6162' AS TEXT));",
    );
    database.exec(
      "INSERT INTO records(id,payload) VALUES (100,'deleted'); DELETE FROM records WHERE id = 100;",
    );
    run({ database, source, destination, directory });
  } finally {
    database.close();
    FS.rmSync(directory, { recursive: true, force: true });
  }
}

function parseSnapshot(contents) {
  Assert.ok(contents.endsWith("\n"));
  const lines = contents
    .trimEnd()
    .split("\n")
    .map((line) => `${line}\n`);
  const records = lines.map(JSON.parse);
  const manifest = records.at(-1);
  Assert.equal(manifest.type, "manifest");
  Assert.equal(manifest.sha256, sha256(lines.slice(0, -1).join("")));
  for (const table of manifest.tables) {
    const rows = lines.filter((line) => {
      const record = JSON.parse(line);
      return record.type === "row" && record.table === table.name;
    });
    Assert.equal(table.rowCount, rows.length);
    Assert.equal(table.sha256, sha256(rows.join("")));
  }
  Assert.equal(manifest.tableCount, records.filter((record) => record.type === "table").length);
  Assert.equal(manifest.rowCount, records.filter((record) => record.type === "row").length);
  return records;
}

test("exports all authoritative rows and exact SQLite storage classes with canonical checksums", () => {
  fixture(({ source, destination, directory }) => {
    const before = [source, `${source}-wal`].map((path) => sha256(FS.readFileSync(path)));
    const result = exportSqliteState({ source, destination });
    const contents = FS.readFileSync(destination, "utf8");
    const records = parseSnapshot(contents);
    const header = records[0];
    Assert.equal(header.format, "t3-code-ls/sqlite-export");
    Assert.equal(header.version, 1);
    Assert.equal(header.userVersion, 17);
    Assert.equal(header.applicationId, 42);
    Assert.ok(header.schemaVersion > 0);
    Assert.deepEqual(header.migrations, [{ id: "80", name: "ScheduledTaskThreadMode" }]);
    Assert.deepEqual(header.sequences, [{ name: "records", sequence: "100" }]);
    Assert.ok(header.excludedDerivedTables.includes("orchestration_v2_message_search"));
    Assert.ok(header.excludedDerivedTables.includes("orchestration_v2_message_search_data"));
    Assert.ok(!header.excludedDerivedTables.includes("orchestration_v2_message_search_other"));
    Assert.equal(
      records.find(
        (record) =>
          record.type === "row" && record.table === "orchestration_v2_projection_messages",
      ).values[1].value,
      '{"text":"retained message text"}',
    );
    Assert.ok(
      header.schema.some((entry) => entry.name === "visible_records" && entry.type === "view"),
    );
    Assert.ok(
      header.schema.some((entry) => entry.name === "records_payload" && entry.type === "index"),
    );
    const tableRows = records.filter(
      (record) => record.type === "row" && record.table === "records",
    );
    Assert.deepEqual(tableRows[0].values, [
      { type: "integer", value: "1" },
      { type: "text", value: 'a\u0000 💬 "line\n' },
      { type: "integer", value: "9223372036854775807" },
      { type: "real", value: "3ff4000000000000" },
      { type: "blob", value: "AID/" },
      { type: "null" },
    ]);
    Assert.deepEqual(tableRows[1].values.slice(2, 5), [
      { type: "integer", value: "-9223372036854775808" },
      { type: "real", value: "7ff0000000000000" },
      { type: "blob", value: "" },
    ]);
    Assert.deepEqual(tableRows[2].values[1], { type: "text", encoding: "base64", value: "gP8=" });
    Assert.deepEqual(tableRows[3].values[1], { type: "text", value: "\uFEFFab" });
    Assert.deepEqual(
      tableRows.map((row) => row.ordinal),
      [0, 1, 2, 3],
    );
    Assert.deepEqual(
      records
        .filter((record) => record.type === "row" && record.table === "compound")
        .map((row) => row.values[2].value),
      ["first", "middle", "last"],
    );
    Assert.equal(result.bytes, Buffer.byteLength(contents));
    Assert.deepEqual(
      [source, `${source}-wal`].map((path) => sha256(FS.readFileSync(path))),
      before,
    );
    const second = Path.join(directory, "second.ndjson");
    exportSqliteState({ source, destination: second });
    Assert.equal(FS.readFileSync(second, "utf8"), contents);
    if (process.platform !== "win32") Assert.equal(FS.statSync(destination).mode & 0o777, 0o600);
  });
});

test("pins one WAL snapshot while another writer commits or rolls back", () => {
  fixture(({ database, source }) => {
    const snapshot = sqliteSnapshotLines({ source });
    const header = snapshot.next();
    Assert.equal(JSON.parse(header.value).type, "header");
    database.exec(
      "BEGIN IMMEDIATE; UPDATE records SET payload = 'committed during export' WHERE id = 1; COMMIT;",
    );
    database.exec("BEGIN IMMEDIATE; INSERT INTO records(payload) VALUES ('uncommitted');");
    const exported = parseSnapshot(header.value + [...snapshot].join(""));
    database.exec("ROLLBACK");
    const rows = exported.filter((record) => record.type === "row" && record.table === "records");
    Assert.equal(rows.length, 4);
    Assert.equal(rows[0].values[1].value, 'a\u0000 💬 "line\n');
    const next = parseSnapshot([...sqliteSnapshotLines({ source })].join(""));
    Assert.equal(
      next.find((record) => record.type === "row" && record.table === "records").values[1].value,
      "committed during export",
    );
    const abandoned = sqliteSnapshotLines({ source });
    abandoned.next();
    abandoned.return();
    Assert.equal(database.prepare("PRAGMA wal_checkpoint(TRUNCATE)").get().busy, 0);
  });
});

test("failed and cancelled exports leave no partial destination and preserve existing files", () => {
  fixture(({ source, destination, directory }) => {
    const good = [...sqliteSnapshotLines({ source })].join("");
    for (const limits of [
      { maxBytes: Buffer.byteLength(good) - 1 },
      { maxRows: 1 },
      { maxRowBytes: 1 },
      { maxTables: 1 },
      { maxColumns: 1 },
      { maxRows: 0 },
    ]) {
      Assert.throws(() => exportSqliteState({ source, destination, limits }), /max|limit/);
      Assert.equal(FS.existsSync(destination), false);
      Assert.deepEqual(
        FS.readdirSync(directory).filter((name) => name.includes(".partial")),
        [],
      );
    }
    FS.writeFileSync(destination, "preserve me");
    Assert.throws(() => exportSqliteState({ source, destination }), /already exists/);
    Assert.equal(FS.readFileSync(destination, "utf8"), "preserve me");
    Assert.throws(() => exportSqliteState({ source, destination: source }), /already exists/);
  });
});

test("quotes identifiers and preserves rowid-shadowing tables without interpreting their names", () => {
  fixture(({ database, source }) => {
    database.exec(
      `CREATE TABLE "quoted""table" ("strange""column" TEXT, rowid, _rowid_, oid); INSERT INTO "quoted""table" VALUES ('safe', 1, 2, 3);`,
    );
    const records = parseSnapshot([...sqliteSnapshotLines({ source })].join(""));
    const row = records.find((record) => record.type === "row" && record.table === 'quoted"table');
    Assert.equal(row.values[0].value, "safe");
    Assert.deepEqual(
      row.values.slice(1).map((value) => value.value),
      ["1", "2", "3"],
    );
  });
});

test("CLI fails closed on existing output and prints a verifiable completion manifest", () => {
  fixture(({ source, destination, directory }) => {
    const result = spawnSync(process.execPath, [tool, source, destination], { encoding: "utf8" });
    Assert.equal(result.status, 0, result.stderr);
    const completion = JSON.parse(result.stdout);
    const records = parseSnapshot(FS.readFileSync(destination, "utf8"));
    Assert.equal(completion.sha256, records.at(-1).sha256);
    const repeated = spawnSync(process.execPath, [tool, source, destination], { encoding: "utf8" });
    Assert.equal(repeated.status, 1);
    Assert.match(repeated.stderr, /already exists/);
    const missing = Path.join(directory, "missing.sqlite");
    Assert.throws(() =>
      exportSqliteState({ source: missing, destination: Path.join(directory, "missing.ndjson") }),
    );
    Assert.equal(FS.existsSync(missing), false);
    Assert.deepEqual(
      FS.readdirSync(directory).filter((name) => name.includes(".partial")),
      [],
    );
  });
});

test("refuses standalone or unknown virtual FTS tables without discarding their authoritative text", () => {
  fixture(({ database, source, destination, directory }) => {
    database.exec(
      "CREATE VIRTUAL TABLE notes USING fts5(text); INSERT INTO notes(text) VALUES ('the only copy');",
    );
    Assert.throws(() => exportSqliteState({ source, destination }), /Unsupported virtual table/);
    Assert.equal(database.prepare("SELECT text FROM notes").get().text, "the only copy");
    Assert.equal(FS.existsSync(destination), false);
    Assert.deepEqual(
      FS.readdirSync(directory).filter((name) => name.includes(".partial")),
      [],
    );
    database.exec(
      "DROP TABLE notes; DROP TABLE orchestration_v2_message_search; CREATE VIRTUAL TABLE orchestration_v2_message_search USING fts5(text); INSERT INTO orchestration_v2_message_search(text) VALUES ('standalone despite known name');",
    );
    Assert.throws(() => exportSqliteState({ source, destination }), /Unsupported virtual table/);
    Assert.equal(FS.existsSync(destination), false);
    database.exec(
      "DROP TABLE orchestration_v2_message_search; CREATE VIRTUAL TABLE orchestration_v2_message_search USING fts5(text, content = '', contentless_delete = 1, tokenize = 'trigram remove_diacritics 1'); DROP TABLE orchestration_v2_projection_messages;",
    );
    Assert.throws(() => exportSqliteState({ source, destination }), /Unsupported virtual table/);
    Assert.equal(FS.existsSync(destination), false);
  });
});
