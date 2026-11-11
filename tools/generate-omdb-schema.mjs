#!/usr/bin/env node
// Development-only schema generator. Production boot never opens SQLite for a
// new store. --check verifies this artifact against the actual retained migrations.
import * as Assert from "node:assert/strict";
import * as Crypto from "node:crypto";
import * as FS from "node:fs/promises";
import * as OS from "node:os";
import * as Path from "node:path";
import { fileURLToPath } from "node:url";
import { createMigratedSqlite } from "./redb-domain-fixture.mjs";
import { sqliteSnapshotLines } from "./export-sqlite-state.mjs";

const root = fileURLToPath(new URL("../", import.meta.url));
const artifact = Path.join(root, "apps/server/src/persistence/omdb-schema.json");
const directory = await FS.mkdtemp(Path.join(OS.tmpdir(), "t3-omdb-schema-"));
try {
  const source = Path.join(directory, "fresh.sqlite");
  await createMigratedSqlite(source);
  const records = [...sqliteSnapshotLines({ source })].map(JSON.parse);
  const header = records[0];
  const tables = records
    .filter((record) => record.type === "table")
    .map((record) => ({
      name: record.name,
      columns: record.columns.map((column) => column.name),
      primary_key: record.columns
        .filter((column) => column.primaryKeyOrder > 0)
        .sort((a, b) => a.primaryKeyOrder - b.primaryKeyOrder)
        .map((column) => column.name),
    }));
  Assert.ok(tables.every((table) => table.primary_key.length > 0));
  const paths = [
    "apps/server/src/persistence/Migrations.ts",
    "apps/server/src/persistence/reconcileV2PreviewMigration.ts",
  ];
  async function visit(directory) {
    for (const entry of await FS.readdir(Path.join(root, directory), { withFileTypes: true })) {
      const path = Path.posix.join(directory, entry.name);
      if (entry.isDirectory()) await visit(path);
      else if (path.endsWith(".ts") && !path.endsWith(".test.ts")) paths.push(path);
    }
  }
  await visit("apps/server/src/persistence/Migrations");
  const sources = {};
  for (const path of paths.sort())
    sources[path] = Crypto.createHash("sha256")
      .update(await FS.readFile(Path.join(root, path)))
      .digest("hex");
  const generated = {
    format: 1,
    migrations: header.migrations,
    schema: { version: 1, tables },
    sources,
  };
  if (process.argv.includes("--check")) {
    Assert.deepEqual(
      JSON.parse(await FS.readFile(artifact, "utf8")),
      generated,
      "Native schema differs from retained migrations; regenerate and port changes before activation",
    );
    console.log(
      `Native schema matches ${header.migrations.length} migrations and ${tables.length} tables.`,
    );
  } else process.stdout.write(JSON.stringify(generated, null, 2) + "\n");
} finally {
  await FS.rm(directory, { recursive: true, force: true });
}
