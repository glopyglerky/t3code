// @effect-diagnostics nodeBuiltinImport:off - read synchronously by the preflight subprocess and at startup.
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import { spawnSync } from "node:child_process";
import * as NodeSqlite from "node:sqlite";
import { applicationDatabase } from "@t3tools/shared/applicationDatabase";

import { migrationManifest } from "./Migrations.ts";

export const BUILD_MIGRATION_HEAD = Math.max(...migrationManifest.map(([id]) => id));

type RecordedMigrations = ReadonlyArray<{ readonly id: number; readonly name: string }>;

function recordedNativeMigrations(databasePath: string): RecordedMigrations | null {
  try {
    const selection = applicationDatabase();
    if (selection.kind !== "omdb" || !NodePath.isAbsolute(selection.executable)) return null;
    const result = spawnSync(
      selection.executable,
      ["read-only-local", NodePath.resolve(databasePath)],
      {
        input: `${JSON.stringify({ operation: "application_migrations" })}\n`,
        encoding: "utf8",
        env: {},
        shell: false,
        timeout: 5_000,
        maxBuffer: 1_048_576,
        killSignal: "SIGKILL",
      },
    );
    if (result.error !== undefined || result.status !== 0 || result.signal !== null) return null;
    const frames = result.stdout.trim().split("\n");
    if (frames.length !== 1) return null;
    const response: unknown = JSON.parse(frames[0]!);
    if (
      typeof response !== "object" ||
      response === null ||
      !("ok" in response) ||
      response.ok !== true ||
      !("result" in response) ||
      !Array.isArray(response.result) ||
      response.result.length > 4096
    )
      return null;
    const records: Array<{ id: number; name: string }> = [];
    let previous = -1;
    for (const migration of response.result) {
      if (
        typeof migration !== "object" ||
        migration === null ||
        typeof migration.id !== "string" ||
        !/^(?:0|[1-9][0-9]*)$/.test(migration.id) ||
        typeof migration.name !== "string"
      )
        return null;
      const id = Number(migration.id);
      if (!Number.isSafeInteger(id) || id <= previous) return null;
      records.push({ id, name: migration.name });
      previous = id;
    }
    return records;
  } catch {
    return null;
  }
}

/** Inspect the header before choosing a reader; never pass an omdb file to SQLite. */
function format(databasePath: string): "missing" | "sqlite" | "native" | "unreadable" {
  let descriptor: number | undefined;
  try {
    descriptor = NodeFS.openSync(databasePath, "r");
    if (NodePath.extname(databasePath).toLowerCase() === ".omdb") return "native";
    const header = Buffer.alloc(16);
    const count = NodeFS.readSync(descriptor, header, 0, header.length, 0);
    // SQLite's empty files are valid uninitialized databases.
    return count === 0 || header.toString("binary") === "SQLite format 3\0" ? "sqlite" : "native";
  } catch (error) {
    return typeof error === "object" && error !== null && "code" in error && error.code === "ENOENT"
      ? "missing"
      : "unreadable";
  } finally {
    if (descriptor !== undefined) {
      try {
        NodeFS.closeSync(descriptor);
      } catch {
        /* No writable handle or flush. */
      }
    }
  }
}

/** Migrations the database records, read-only. A missing or new database records none; one that
 * exists but cannot be read is null, since its history is unknown. */
export function recordedMigrations(databasePath: string): RecordedMigrations | null {
  const source = format(databasePath);
  if (source === "missing") return [];
  if (source === "unreadable") return null;
  if (source === "native") return recordedNativeMigrations(databasePath);
  let db: NodeSqlite.DatabaseSync;
  try {
    db = new NodeSqlite.DatabaseSync(databasePath, { readOnly: true });
  } catch {
    return NodeFS.existsSync(databasePath) ? null : [];
  }
  try {
    const hasTable = db
      .prepare(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'effect_sql_migrations'",
      )
      .get();
    if (hasTable === undefined) return [];
    return db
      .prepare("SELECT migration_id, name FROM effect_sql_migrations ORDER BY migration_id")
      .all()
      .map((row) => ({ id: Number(row.migration_id), name: String(row.name) }));
  } catch {
    return null;
  } finally {
    try {
      db.close();
    } catch {
      // A read-only handle has nothing to flush; the result above already stands.
    }
  }
}

/** Highest migration the database holds once this build's startup migrations have run. A newer
 * build may have recorded more than this build knows. Undefined when the database is unreadable. */
export function databaseMigrationHead(databasePath: string): number | undefined {
  const recorded = recordedMigrations(databasePath);
  return recorded === null
    ? undefined
    : Math.max(BUILD_MIGRATION_HEAD, ...recorded.map(({ id }) => id));
}
