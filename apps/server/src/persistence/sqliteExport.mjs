import * as Crypto from "node:crypto";
import * as FS from "node:fs";
import * as OS from "node:os";
import * as Path from "node:path";
import { DatabaseSync } from "node:sqlite";

const FORMAT = "t3-code-ls/sqlite-export";
const DEFAULT_LIMITS = Object.freeze({
  maxBytes: 512 * 1024 * 1024,
  maxRows: 10_000_000,
  maxRowBytes: 16 * 1024 * 1024,
  maxTables: 4096,
  maxColumns: 2048,
});
const utf8 = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true });
const quote = (name) => `"${name.replaceAll('"', '""')}"`;
const messageSearch = "orchestration_v2_message_search";
const messageSearchSchema =
  /^CREATE\s+VIRTUAL\s+TABLE\s+orchestration_v2_message_search\s+USING\s+fts5\s*\(\s*text\s*,\s*content\s*=\s*''\s*,\s*contentless_delete\s*=\s*1\s*,\s*tokenize\s*=\s*'trigram remove_diacritics 1'\s*\)\s*;?\s*$/i;
const searchDefinition = `CREATE VIRTUAL TABLE ${messageSearch} USING fts5(text, content = '', contentless_delete = 1, tokenize = 'trigram remove_diacritics 1')`;
const searchShadows = Object.freeze({
  data: ["id", "block"],
  idx: ["segid", "term", "pgno"],
  docsize: ["id", "sz", "origin"],
  config: ["k", "v"],
});

function limitsFor(options) {
  const limits = { ...DEFAULT_LIMITS, ...options };
  for (const [key, value] of Object.entries(limits)) {
    if (!(key in DEFAULT_LIMITS) || !Number.isSafeInteger(value) || value < 1)
      throw new Error(`Invalid export limit: ${key}`);
  }
  return limits;
}

function canonical(value) {
  if (Array.isArray(value)) return `[${value.map(canonical).join(",")}]`;
  if (value !== null && typeof value === "object")
    return `{${Object.keys(value)
      .sort()
      .map((key) => `${JSON.stringify(key)}:${canonical(value[key])}`)
      .join(",")}}`;
  return JSON.stringify(value);
}

function statement(database, sql) {
  const result = database.prepare(sql);
  result.setReadBigInts(true);
  result.setReturnArrays(true);
  return result;
}

function cell(type, value) {
  switch (type) {
    case "null":
      return { type: "null" };
    case "integer":
      return { type, value: value.toString() };
    case "real": {
      const bytes = Buffer.alloc(8);
      bytes.writeDoubleBE(value);
      return { type, value: bytes.toString("hex") };
    }
    case "text": {
      // Reading bytes first prevents SQLite's invalid UTF-8 from silently becoming U+FFFD.
      try {
        return { type, value: utf8.decode(value) };
      } catch {
        return { type, encoding: "base64", value: Buffer.from(value).toString("base64") };
      }
    }
    case "blob":
      return { type, value: Buffer.from(value).toString("base64") };
    default:
      throw new Error(`Unsupported SQLite storage class: ${type}`);
  }
}

function readColumns(database, name, limits) {
  const columns = database.prepare(`PRAGMA main.table_xinfo(${quote(name)})`).all();
  if (columns.length > limits.maxColumns) throw new Error(`Table exceeds maxColumns: ${name}`);
  return columns.map((column) => ({
    name: column.name,
    declaredType: column.type,
    notNull: column.notnull === 1,
    defaultSql: column.dflt_value,
    primaryKeyOrder: column.pk,
    hidden: column.hidden,
  }));
}

function createSearchVocabulary(database) {
  database.exec(
    `CREATE VIRTUAL TABLE temp.t3_export_search_terms USING fts5vocab(main, ${messageSearch}, instance)`,
  );
}

function* searchPostings(database, limits, type = "search_posting") {
  let count = 0;
  for (const [term, rowid, column, offset] of statement(
    database,
    "SELECT CAST(term AS BLOB), doc, col, offset FROM temp.t3_export_search_terms ORDER BY term COLLATE BINARY, doc, col, offset",
  ).iterate()) {
    if (++count > limits.maxRows) throw new Error("Search postings exceed maxRows");
    if (column !== "text" || offset < 0n || offset > 2_147_483_647n)
      throw new Error("Unsupported search posting");
    yield { type, term: utf8.decode(term), rowid: rowid.toString(), offset: Number(offset) };
  }
}

/**
 * Copy only the exact supported FTS shadow schema from the pinned read-only
 * snapshot. Closing and reopening discards SQLite's per-connection FTS caches.
 * The clone's original vocabulary must match before it can provide evidence.
 */
function* searchDeleteWitness(source, limits, expected) {
  const directory = FS.mkdtempSync(Path.join(OS.tmpdir(), "t3-omdb-search-witness-"));
  const owned = FS.lstatSync(directory);
  const filename = Path.join(directory, "witness.sqlite");
  let clone;
  let transaction = false;
  try {
    FS.closeSync(FS.openSync(filename, "wx", 0o600));
    // Only this newly owned clone permits shadow-table writes. The source
    // remains a separate readOnly handle throughout the snapshot.
    clone = new DatabaseSync(filename, { allowExtension: false, defensive: false });
    clone.exec(`PRAGMA journal_mode = DELETE; PRAGMA trusted_schema = OFF; ${searchDefinition}`);
    clone.exec("BEGIN");
    transaction = true;
    let copiedBytes = 0;
    let copiedRows = 0;
    for (const [suffix, columns] of Object.entries(searchShadows)) {
      const name = `${messageSearch}_${suffix}`;
      const actual = readColumns(source, name, limits).map((column) => column.name);
      if (canonical(actual) !== canonical(columns))
        throw new Error(`Unsupported search shadow schema: ${name}`);
      const largest = statement(
        source,
        `SELECT COALESCE(MAX(${columns.map((column) => `COALESCE(length(CAST(${quote(column)} AS BLOB)),0)`).join("+")}),0) FROM ${quote(name)}`,
      ).get()[0];
      if (largest > BigInt(limits.maxRowBytes))
        throw new Error("Search shadow row exceeds maxRowBytes");
      clone.exec(`DELETE FROM ${quote(name)}`);
      // run() also returns lastInsertRowid; FTS shadow IDs can exceed safe JS integers.
      const insert = statement(
        clone,
        `INSERT INTO ${quote(name)} (${columns.map(quote).join(",")}) VALUES (${columns.map(() => "?").join(",")})`,
      );
      for (const row of statement(
        source,
        `SELECT ${columns.map(quote).join(",")} FROM ${quote(name)}`,
      ).iterate()) {
        if (++copiedRows > limits.maxRows) throw new Error("Search shadow copy exceeds maxRows");
        for (const value of row) {
          copiedBytes +=
            value === null
              ? 0
              : typeof value === "string"
                ? Buffer.byteLength(value)
                : value instanceof Uint8Array
                  ? value.byteLength
                  : 8;
        }
        if (copiedBytes > limits.maxBytes) throw new Error("Search shadow copy exceeds maxBytes");
        insert.run(...row);
      }
    }
    clone.exec("COMMIT");
    transaction = false;
    clone.close();
    clone = undefined;
    clone = new DatabaseSync(filename, { allowExtension: false });
    clone.exec("PRAGMA trusted_schema = OFF");
    createSearchVocabulary(clone);
    const originalHash = Crypto.createHash("sha256");
    let originalCount = 0;
    for (const record of searchPostings(clone, limits)) {
      originalHash.update(`${canonical(record)}\n`);
      originalCount += 1;
    }
    if (originalCount !== expected.count || originalHash.digest("hex") !== expected.sha256)
      throw new Error("Owned search clone does not match the source vocabulary");
    clone.exec("BEGIN");
    transaction = true;
    const selectFirst = statement(
      clone,
      `SELECT id FROM ${messageSearch}_docsize ORDER BY id LIMIT 128`,
    );
    const selectAfter = statement(
      clone,
      `SELECT id FROM ${messageSearch}_docsize WHERE id > ? ORDER BY id LIMIT 128`,
    );
    const remove = statement(clone, `DELETE FROM ${messageSearch} WHERE rowid = ?`);
    let after;
    let documents = 0;
    while (true) {
      const page = after === undefined ? selectFirst.all() : selectAfter.all(after);
      for (const [rowid] of page) {
        if (++documents > limits.maxRows) throw new Error("Search documents exceed maxRows");
        // Include even zero-token current rows: these are not absent documents.
        yield { type: "search_current_document", rowid: rowid.toString() };
        remove.run(rowid);
        after = rowid;
      }
      if (page.length < 128) break;
    }
    // A broad DELETE/delete-all would erase older generations. Explicit rowid
    // deletes above preserve the exact old-only terms contentless FTS retains.
    yield* searchPostings(clone, limits, "search_survivor_posting");
    clone.exec("ROLLBACK");
    transaction = false;
  } finally {
    try {
      if (clone !== undefined) {
        try {
          if (transaction) clone.exec("ROLLBACK");
        } finally {
          clone.close();
        }
      }
    } finally {
      try {
        const found = FS.lstatSync(directory);
        if (found.dev === owned.dev && found.ino === owned.ino)
          FS.rmSync(directory, { recursive: true });
      } catch (error) {
        if (error.code !== "ENOENT") throw error;
      }
    }
  }
}

function* recordsFromDatabase(database, limits) {
  const schema = database
    .prepare(
      "SELECT type, name, tbl_name AS tableName, sql FROM sqlite_schema ORDER BY name COLLATE BINARY",
    )
    .all();
  const tableList = database.prepare("PRAGMA main.table_list").all();
  const derived = new Set();
  for (const entry of schema) {
    if (entry.type !== "table" || !/^CREATE\s+VIRTUAL\s+TABLE\b/i.test(entry.sql ?? "")) continue;
    const source = tableList.find((table) => table.name === "orchestration_v2_projection_messages");
    if (
      entry.name !== messageSearch ||
      !messageSearchSchema.test(entry.sql) ||
      source?.type !== "table" ||
      !readColumns(database, source.name, limits).some((column) => column.name === "payload_json")
    )
      throw new Error(`Unsupported virtual table requires an explicit migration: ${entry.name}`);
    derived.add(entry.name);
  }
  for (const table of tableList.filter((table) => table.type === "shadow")) {
    if (
      !derived.has(messageSearch) ||
      !["data", "idx", "docsize", "config"].some(
        (suffix) => table.name === `${messageSearch}_${suffix}`,
      )
    )
      throw new Error(`Unsupported shadow table requires an explicit migration: ${table.name}`);
    derived.add(table.name);
  }
  const tables = schema.filter(
    (entry) =>
      entry.type === "table" && !entry.name.startsWith("sqlite_") && !derived.has(entry.name),
  );
  if (tables.length > limits.maxTables) throw new Error("Database exceeds maxTables");
  const tableInfo = tables.map((table) => {
    const columns = readColumns(database, table.name, limits);
    const count = statement(database, `SELECT COUNT(*) FROM ${quote(table.name)}`).get()[0];
    if (count > BigInt(limits.maxRows)) throw new Error(`Table exceeds maxRows: ${table.name}`);
    const rowCount = Number(count);
    const largest =
      columns.length === 0
        ? 0n
        : statement(
            database,
            `SELECT COALESCE(MAX(${columns.map(({ name }) => `COALESCE(length(CAST(${quote(name)} AS BLOB)),0)`).join("+")}),0) FROM ${quote(table.name)}`,
          ).get()[0];
    if (largest > BigInt(limits.maxRowBytes))
      throw new Error(`Table contains a row exceeding maxRowBytes: ${table.name}`);
    const names = new Set(columns.map(({ name }) => name.toLowerCase()));
    const withoutRowid = tableList.find((entry) => entry.name === table.name)?.wr === 1;
    const rowidColumn = withoutRowid
      ? null
      : (["rowid", "_rowid_", "oid"].find((name) => !names.has(name)) ?? null);
    return { type: "table", name: table.name, sql: table.sql, columns, rowCount, rowidColumn };
  });
  const rowCount = tableInfo.reduce((total, table) => total + table.rowCount, 0);
  if (!Number.isSafeInteger(rowCount) || rowCount > limits.maxRows)
    throw new Error("Database exceeds maxRows");
  const sequences = schema.some((entry) => entry.name === "sqlite_sequence")
    ? [
        ...statement(
          database,
          "SELECT name, seq FROM sqlite_sequence ORDER BY name COLLATE BINARY",
        ).iterate(),
      ].map(([name, sequence]) => ({ name, sequence: sequence.toString() }))
    : [];
  const migrations = tables.some((table) => table.name === "effect_sql_migrations")
    ? [
        ...statement(
          database,
          "SELECT migration_id, name FROM effect_sql_migrations ORDER BY migration_id",
        ).iterate(),
      ].map(([id, name]) => ({ id: id.toString(), name }))
    : [];
  yield {
    type: "header",
    format: FORMAT,
    version: 2,
    searchIndex: derived.has(messageSearch),
    searchDeleteWitness: true,
    sqliteVersion: database.prepare("SELECT sqlite_version() AS version").get().version,
    userVersion: database.prepare("PRAGMA user_version").get().user_version,
    schemaVersion: database.prepare("PRAGMA schema_version").get().schema_version,
    applicationId: database.prepare("PRAGMA application_id").get().application_id,
    tableCount: tables.length,
    excludedDerivedTables: [...derived].sort(),
    sequences,
    migrations,
    schema: schema.filter(
      (entry) =>
        !entry.name.startsWith("sqlite_") &&
        !derived.has(entry.name) &&
        !derived.has(entry.tableName),
    ),
  };
  for (const table of tableInfo) {
    yield table;
    const rowid = table.rowidColumn;
    const primaryKey = table.columns
      .filter((column) => column.primaryKeyOrder > 0)
      .sort((left, right) => left.primaryKeyOrder - right.primaryKeyOrder);
    const order =
      rowid === null
        ? (primaryKey.length > 0 ? primaryKey : table.columns)
            .map(({ name }) => quote(name))
            .join(",")
        : quote(rowid);
    const selected = table.columns.flatMap(({ name }) => [
      `typeof(${quote(name)})`,
      `CASE WHEN typeof(${quote(name)}) = 'text' THEN CAST(${quote(name)} AS BLOB) ELSE ${quote(name)} END`,
    ]);
    if (rowid !== null) selected.push(quote(rowid));
    let ordinal = 0;
    for (const values of statement(
      database,
      `SELECT ${selected.join(",")} FROM ${quote(table.name)} ORDER BY ${order}`,
    ).iterate()) {
      const encoded = table.columns.map((_, index) =>
        cell(values[index * 2], values[index * 2 + 1]),
      );
      yield {
        type: "row",
        table: table.name,
        ordinal,
        values: encoded,
        ...(rowid === null ? {} : { rowid: values.at(-1).toString() }),
      };
      ordinal += 1;
    }
    if (ordinal !== table.rowCount) throw new Error(`Snapshot row count changed: ${table.name}`);
  }
  if (derived.has(messageSearch)) {
    // The source is opened read-only. A TEMP vocabulary is the supported FTS5
    // interface for reading its exact postings, including retained legacy hits.
    // Rebuilding from current messages would alter the newest-candidate cutoff.
    database.exec("PRAGMA query_only = OFF");
    try {
      createSearchVocabulary(database);
    } finally {
      database.exec("PRAGMA query_only = ON");
    }
    let count = 0;
    const originalHash = Crypto.createHash("sha256");
    for (const record of searchPostings(database, limits)) {
      originalHash.update(`${canonical(record)}\n`);
      count += 1;
      yield record;
    }
    yield* searchDeleteWitness(database, limits, { count, sha256: originalHash.digest("hex") });
  }
}

/** A read transaction stays open until iteration finishes or the caller closes the generator. */
export function* sqliteSnapshotLines({ source, limits: requestedLimits }) {
  const limits = limitsFor(requestedLimits);
  if (!FS.statSync(source).isFile()) throw new Error("SQLite source must be a regular file");
  const database = new DatabaseSync(source, { readOnly: true, allowExtension: false });
  const hash = Crypto.createHash("sha256");
  const tables = [];
  let current;
  let bytes = 0;
  let rows = 0;
  let searchPostings = 0;
  const searchHash = Crypto.createHash("sha256");
  let searchDocuments = 0;
  let searchSurvivors = 0;
  const witnessHash = Crypto.createHash("sha256");
  let transaction = false;
  const line = (record) => {
    const encoded = `${canonical(record)}\n`;
    const length = Buffer.byteLength(encoded);
    if (length > limits.maxRowBytes) throw new Error("Export record exceeds maxRowBytes");
    if (bytes + length > limits.maxBytes) throw new Error("Export exceeds maxBytes");
    bytes += length;
    return encoded;
  };
  const finishTable = () => {
    if (current === undefined) return;
    tables.push({
      name: current.name,
      rowCount: current.rowCount,
      sha256: current.hash.digest("hex"),
    });
  };
  try {
    // Do not use immutable=1: it ignores a live source's uncheckpointed WAL.
    database.exec(
      "PRAGMA query_only = ON; PRAGMA trusted_schema = OFF; PRAGMA busy_timeout = 5000; BEGIN;",
    );
    transaction = true;
    for (const record of recordsFromDatabase(database, limits)) {
      const encoded = line(record);
      hash.update(encoded);
      if (record.type === "table") {
        finishTable();
        current = { name: record.name, rowCount: 0, hash: Crypto.createHash("sha256") };
      } else if (record.type === "row") {
        current.hash.update(encoded);
        current.rowCount += 1;
        rows += 1;
      } else if (record.type === "search_posting") {
        searchHash.update(encoded);
        searchPostings += 1;
      } else if (
        record.type === "search_current_document" ||
        record.type === "search_survivor_posting"
      ) {
        witnessHash.update(encoded);
        if (record.type === "search_current_document") searchDocuments += 1;
        else searchSurvivors += 1;
      }
      yield encoded;
    }
    finishTable();
    const manifest = {
      type: "manifest",
      tableCount: tables.length,
      rowCount: rows,
      tables,
      sha256: hash.digest("hex"),
      searchIndex: { postingCount: searchPostings, sha256: searchHash.digest("hex") },
      searchDeleteWitness: {
        documentCount: searchDocuments,
        survivorPostingCount: searchSurvivors,
        sha256: witnessHash.digest("hex"),
      },
    };
    database.exec("ROLLBACK");
    transaction = false;
    yield line(manifest);
  } finally {
    try {
      if (transaction) database.exec("ROLLBACK");
    } finally {
      database.close();
    }
  }
}

function removeOwned(path, identity) {
  try {
    const found = FS.lstatSync(path);
    if (found.dev === identity.dev && found.ino === identity.ino) FS.unlinkSync(path);
  } catch (error) {
    if (error.code !== "ENOENT") throw error;
  }
}

/** The destination appears only after a complete, fsynced export, and is never overwritten. */
export function exportSqliteState({ source, destination, limits }) {
  limitsFor(limits);
  try {
    FS.lstatSync(destination);
    throw new Error("Export destination already exists");
  } catch (error) {
    if (error.code !== "ENOENT") throw error;
  }
  const directory = Path.dirname(Path.resolve(destination));
  const temporary = Path.join(directory, `.t3-sqlite-export-${Crypto.randomUUID()}.partial`);
  let fd = FS.openSync(temporary, "wx", 0o600);
  const identity = FS.fstatSync(fd);
  let published = false;
  let bytes = 0;
  let last;
  try {
    for (const line of sqliteSnapshotLines({ source, limits })) {
      const buffer = Buffer.from(line);
      for (let written = 0; written < buffer.length;) {
        const count = FS.writeSync(fd, buffer, written, buffer.length - written);
        if (count === 0) throw new Error("Export write made no progress");
        written += count;
      }
      bytes += buffer.length;
      last = line;
    }
    FS.fsyncSync(fd);
    FS.closeSync(fd);
    fd = undefined;
    FS.linkSync(temporary, destination);
    published = true;
    if (process.platform !== "win32") {
      const dir = FS.openSync(directory, "r");
      try {
        FS.fsyncSync(dir);
      } finally {
        FS.closeSync(dir);
      }
    }
    return { ...JSON.parse(last), bytes };
  } catch (error) {
    if (published) removeOwned(destination, identity);
    throw error;
  } finally {
    try {
      if (fd !== undefined) FS.closeSync(fd);
    } finally {
      removeOwned(temporary, identity);
    }
  }
}
