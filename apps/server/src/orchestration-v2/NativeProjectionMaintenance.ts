import * as Layer from "effect/Layer";
import { redbLayer } from "../persistence/PersistenceTransactions.ts";
import { layerWithRows } from "./ProjectionMaintenance.ts";
import * as NativeMaintenanceRows from "./NativeMaintenanceRows.ts";

export const layer = layerWithRows.pipe(
  Layer.provide(NativeMaintenanceRows.layer),
  Layer.provide(redbLayer),
);
