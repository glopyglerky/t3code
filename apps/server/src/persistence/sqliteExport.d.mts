export interface ExportLimits {
  maxBytes?: number;
  maxRows?: number;
  maxRowBytes?: number;
  maxTables?: number;
  maxColumns?: number;
}
export function sqliteSnapshotLines(options: {
  source: string;
  limits?: ExportLimits;
}): Generator<string>;
export function exportSqliteState(options: {
  source: string;
  destination: string;
  limits?: ExportLimits;
}): { bytes: number; sha256: string };
