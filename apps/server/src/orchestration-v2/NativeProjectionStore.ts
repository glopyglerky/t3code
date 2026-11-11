import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

import { RedbClient } from "../persistence/RedbClient.ts";
import * as NativeProjectionCommitStore from "../persistence/NativeProjectionCommitStore.ts";
import * as NativeProjectionLookupRows from "./NativeProjectionLookupRows.ts";
import * as NativeProjectionControlRows from "./NativeProjectionControlRows.ts";
import * as NativeProjectionContextRows from "./NativeProjectionContextRows.ts";
import * as NativeProjectionRecordRows from "./NativeProjectionRecordRows.ts";
import * as NativeProjectionRecoveryRows from "./NativeProjectionRecoveryRows.ts";
import * as NativeProjectionShellRows from "./NativeProjectionShellRows.ts";
import * as NativeThreadFind from "./NativeThreadFind.ts";
import {
  ProjectionStoreV2,
  ProjectionStoreApplyEventError,
  ProjectionStoreSetupError,
  makeProjectionLookups,
  makeProjectionControls,
  makeProjectionContexts,
  makeProjectionRecovery,
  type ProjectionStoreV2Shape,
} from "./ProjectionStore.ts";

/** One complete application projection boundary; every operation uses the same native owner. */
export const make = Effect.gen(function* () {
  const client = yield* RedbClient;
  while (!(yield* client.prepareOutbox).prepared) yield* Effect.yieldNow;
  while (!(yield* client.prepareAuxiliary).prepared) yield* Effect.yieldNow;
  const writer = yield* NativeProjectionCommitStore.make;
  const lookups = makeProjectionLookups(yield* NativeProjectionLookupRows.makeRows);
  const { requireThread: _requireThread, ...publicLookups } = lookups;
  const readTimeline = yield* NativeProjectionRecordRows.makeTimelineIndex;
  const find = yield* NativeThreadFind.make((threadId) =>
    readTimeline(threadId, new Set()).pipe(Effect.map((index) => index.visible)),
  );
  return {
    ...publicLookups,
    ...makeProjectionControls(yield* NativeProjectionControlRows.makeRows, lookups),
    ...makeProjectionContexts(yield* NativeProjectionContextRows.make, lookups),
    ...makeProjectionRecovery(yield* NativeProjectionRecoveryRows.make),
    ...(yield* NativeProjectionShellRows.make),
    ...(yield* NativeProjectionRecordRows.makeSnapshots),
    ...(yield* NativeProjectionRecordRows.makeTimelinePages),
    ...find,
    getThreadRecords: yield* NativeProjectionRecordRows.make,
    apply: (event) =>
      writer
        .apply(event)
        .pipe(
          Effect.mapError(
            (cause) => new ProjectionStoreApplyEventError({ eventType: event.type, cause }),
          ),
        ),
  } satisfies ProjectionStoreV2Shape;
}).pipe(Effect.mapError((cause) => new ProjectionStoreSetupError({ cause })));

export const layer = Layer.effect(ProjectionStoreV2, make);
