import type * as Layer from "effect/Layer";
import type * as SqlClient from "effect/sql/SqlClient";
import type * as NativeRuntime from "../apps/server/src/orchestration-v2/nativeRuntimeLayer.ts";
import type * as SqliteRuntime from "../apps/server/src/orchestration-v2/runtimeLayer.ts";

type Assert<Condition extends true> = Condition;

/** A broad unknown service channel would also fail this assertion. */
export type NativeRuntimeDoesNotRequireSqlite = Assert<
  SqlClient.SqlClient extends Layer.Services<typeof NativeRuntime.layerProduction> ? false : true
>;
export type RetainedSqliteRuntimeStillRequiresSqlite = Assert<
  SqlClient.SqlClient extends Layer.Services<typeof SqliteRuntime.layerProduction> ? true : false
>;
export type NativeAndSqliteRuntimeExposeSameServices = Assert<
  [Layer.Success<typeof NativeRuntime.layerProduction>] extends [
    Layer.Success<typeof SqliteRuntime.layerProduction>,
  ]
    ? [Layer.Success<typeof SqliteRuntime.layerProduction>] extends [
        Layer.Success<typeof NativeRuntime.layerProduction>,
      ]
      ? true
      : false
    : false
>;
