import * as Layer from "effect/Layer";
import { select } from "../persistence/SelectedPersistence.ts";
import * as Omdb from "../persistence/Omdb.ts";
import * as Sqlite from "../persistence/Sqlite.ts";
import * as NativeRuntime from "./nativeRuntimeLayer.ts";
import * as SqliteRuntime from "./runtimeLayer.ts";

const native = <A, E, R>(layer: Layer.Layer<A, E, R>) =>
  layer.pipe(Layer.provide(Omdb.layerConfig));
const sqlite = <A, E, R>(layer: Layer.Layer<A, E, R>) =>
  layer.pipe(Layer.provide(Sqlite.layerConfig));
export const layerEventInfrastructure = select(
  native(NativeRuntime.layerEventInfrastructure),
  sqlite(SqliteRuntime.layerEventInfrastructure),
);
export const layerEventSink = select(
  native(NativeRuntime.layerEventSink),
  sqlite(SqliteRuntime.layerEventSink),
);
export const layerProjectService = select(
  native(NativeRuntime.layerProjectService),
  sqlite(SqliteRuntime.layerProjectService),
);
export const layerProjectSetupScriptRunner = select(
  native(NativeRuntime.layerProjectSetupScriptRunner),
  sqlite(SqliteRuntime.layerProjectSetupScriptRunner),
);
export const layer = select(native(NativeRuntime.layer), sqlite(SqliteRuntime.layer));
export const layerProduction = select(
  native(NativeRuntime.layerProduction),
  sqlite(SqliteRuntime.layerProduction),
);
