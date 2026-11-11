import { selectNativeExecutable } from "./nativeExecutable.ts";

/** Choose once at startup, never in response to a failed database operation. */
export const applicationDatabase = () => {
  const executable = selectNativeExecutable("db", process.env.T3_CODE_LS_DB_EXECUTABLE);
  return executable === undefined
    ? { kind: "sqlite" as const }
    : { kind: "omdb" as const, executable };
};
