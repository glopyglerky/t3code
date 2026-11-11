#!/usr/bin/env node
import * as Path from "node:path";
import { pathToFileURL } from "node:url";
import { exportSqliteState } from "../apps/server/src/persistence/sqliteExport.mjs";
export {
  exportSqliteState,
  sqliteSnapshotLines,
} from "../apps/server/src/persistence/sqliteExport.mjs";

function main(args) {
  if (args.includes("--help")) {
    process.stdout.write(
      "Usage: node export-sqlite-state.mjs SOURCE.sqlite DESTINATION.ndjson [--max-bytes N] [--max-rows N] [--max-row-bytes N] [--max-tables N] [--max-columns N]\n",
    );
    return;
  }
  const [source, destination, ...flags] = args;
  if (!source || !destination || flags.length % 2 !== 0)
    throw new Error("Expected source, fresh destination, and optional limit pairs; use --help");
  const keys = {
    "--max-bytes": "maxBytes",
    "--max-rows": "maxRows",
    "--max-row-bytes": "maxRowBytes",
    "--max-tables": "maxTables",
    "--max-columns": "maxColumns",
  };
  const limits = {};
  for (let index = 0; index < flags.length; index += 2) {
    const key = keys[flags[index]];
    if (key === undefined || key in limits || !/^[0-9]+$/.test(flags[index + 1]))
      throw new Error(`Invalid export option: ${flags[index]}`);
    limits[key] = Number(flags[index + 1]);
  }
  process.stdout.write(`${JSON.stringify(exportSqliteState({ source, destination, limits }))}\n`);
}

if (process.argv[1] && import.meta.url === pathToFileURL(Path.resolve(process.argv[1])).href) {
  try {
    main(process.argv.slice(2));
  } catch (error) {
    process.stderr.write(`SQLite export failed: ${error.message}\n`);
    process.exitCode = 1;
  }
}
