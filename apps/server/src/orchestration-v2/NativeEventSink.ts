import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

import * as NativeCommandReceipts from "../persistence/NativeCommandReceipts.ts";
import * as NativeEventRows from "../persistence/NativeEventRows.ts";
import * as NativeProjectionCommitStore from "../persistence/NativeProjectionCommitStore.ts";
import * as OrchestrationEventStore from "../persistence/OrchestrationEventStore.ts";
import * as PersistenceTransactions from "../persistence/PersistenceTransactions.ts";
import { RedbClient } from "../persistence/RedbClient.ts";
import * as CommandReceiptStore from "./CommandReceiptStore.ts";
import * as EventSink from "./EventSink.ts";
import * as EventStore from "./EventStore.ts";
import * as NativeApplicationEffectOutbox from "./NativeApplicationEffectOutbox.ts";
import * as NativeProjectRows from "./NativeProjectRows.ts";
import * as NativeTurnItemPositionStore from "./NativeTurnItemPositionStore.ts";
import * as ProjectStore from "./ProjectStore.ts";

const eventRows = Layer.unwrap(
  Effect.gen(function* () {
    const client = yield* RedbClient;
    while (!(yield* client.prepareEvents).prepared) yield* Effect.yieldNow;
    return NativeEventRows.layer;
  }),
);

export const applicationEvents = OrchestrationEventStore.layerFromEventRows.pipe(
  Layer.provide(eventRows),
);
const events = EventStore.layerFromOrchestrationEventStore.pipe(
  Layer.provideMerge(applicationEvents),
);
const receipts = CommandReceiptStore.layerFromApplicationReceipts.pipe(
  Layer.provide(NativeCommandReceipts.layer),
);
export const projects = ProjectStore.layerWithRows.pipe(Layer.provide(NativeProjectRows.layer));

/** Every repository joins one RedbClient. The returned outbox also owns worker wakeups. */
export const repositories = Layer.mergeAll(
  events,
  receipts,
  projects,
  NativeTurnItemPositionStore.layer,
  NativeProjectionCommitStore.layer,
  NativeApplicationEffectOutbox.layer,
  PersistenceTransactions.redbLayer,
);

/** Use the same commit writer and transaction owner when assembling the full runtime graph. */
export const layerFromStores = EventSink.layerFromPersistence.pipe(
  Layer.provide(
    Layer.mergeAll(NativeProjectionCommitStore.layer, PersistenceTransactions.redbLayer),
  ),
);

/** Standalone native composition shares these exact repository layer references. */
export const layer = layerFromStores.pipe(Layer.provideMerge(repositories));
