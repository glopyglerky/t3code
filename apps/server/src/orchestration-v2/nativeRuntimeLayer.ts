import * as Layer from "effect/Layer";

import * as NativeMcpAppModelContext from "../mcpApps/NativeMcpAppModelContext.ts";
import * as NativeCommandReceipts from "../persistence/NativeCommandReceipts.ts";
import * as NativeProviderSessionRuntime from "../persistence/NativeProviderSessionRuntime.ts";
import * as PersistenceTransactions from "../persistence/PersistenceTransactions.ts";
import * as NativeScheduledRows from "../scheduledTasks/NativeScheduledRows.ts";
import * as ScheduledTaskService from "../scheduledTasks/ScheduledTaskService.ts";
import * as NativeApplicationEffectOutbox from "./NativeApplicationEffectOutbox.ts";
import * as NativeEventSink from "./NativeEventSink.ts";
import * as NativeProjectionMaintenance from "./NativeProjectionMaintenance.ts";
import * as NativeProjectionStore from "./NativeProjectionStore.ts";
import * as NativeTurnItemPositionStore from "./NativeTurnItemPositionStore.ts";
import * as NativeLegacyV1ThreadImporter from "./legacy/NativeLegacyV1ThreadImporter.ts";
import { makeRuntimeLayers } from "./runtimeLayer.ts";

/** Only storage implementations differ. All services, workers and queues use the shared graph. */
const nativeRuntime = makeRuntimeLayers({
  eventInfrastructure: Layer.mergeAll(
    NativeEventSink.applicationEvents,
    NativeCommandReceipts.layer,
  ),
  projections: NativeProjectionStore.layer,
  projects: NativeEventSink.projects,
  outbox: NativeApplicationEffectOutbox.layer,
  positions: NativeTurnItemPositionStore.layer,
  eventSink: NativeEventSink.layerFromStores,
  maintenance: NativeProjectionMaintenance.layer,
  legacy: NativeLegacyV1ThreadImporter.layer,
  providerRuntime: NativeProviderSessionRuntime.layer,
  mcpContext: NativeMcpAppModelContext.layer,
  scheduledTasks: ScheduledTaskService.layerWithRows.pipe(
    Layer.provide(NativeScheduledRows.layer),
    Layer.provide(PersistenceTransactions.redbLayer),
  ),
});

export const {
  layerEventInfrastructure,
  layerEventSink,
  layerProjectService,
  layerProjectSetupScriptRunner,
  layer,
  layerProduction,
} = nativeRuntime;
