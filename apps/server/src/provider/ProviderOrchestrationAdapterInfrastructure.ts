import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as AuxiliaryPersistenceReads from "../persistence/AuxiliaryPersistenceReads.ts";
import * as SqliteAuxiliaryPersistenceReads from "../persistence/SqliteAuxiliaryPersistenceReads.ts";

import * as ClaudeAdapterV2 from "../orchestration-v2/Adapters/ClaudeAdapterV2.ts";
import * as CodexAdapterV2 from "../orchestration-v2/Adapters/CodexAdapterV2.ts";
import * as CursorAgentSdk from "@t3tools/provider-cursor/server/CursorAgentSdk";
import * as CursorSdk from "@t3tools/provider-cursor/server/CursorSdk";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as ProviderContinuationRequests from "@t3tools/provider-core/server/ProviderContinuationRequests";

export type ProviderOrchestrationAdapterInfrastructure =
  | ClaudeAdapterV2.ClaudeAgentSdkQueryRunner
  | CodexAdapterV2.CodexAppServerClientFactory
  | CursorAgentSdk.CursorAgentSdkRunner
  | CursorSdk.CursorSdk
  | IdAllocator.IdAllocatorV2;

// Subagent threads created before short ids keep their stored id when an adapter resumes them.
export const layerIdAllocatorWithPersistenceReads = Layer.unwrap(
  Effect.gen(function* () {
    const persistence = yield* AuxiliaryPersistenceReads.AuxiliaryPersistenceReads;
    const threadIds = yield* persistence.legacySubagentThreadIds.pipe(
      Effect.catch((cause) =>
        Effect.logWarning("orchestration-v2.legacy-subagent-thread-ids-unavailable", {
          cause,
        }).pipe(Effect.as([])),
      ),
    );
    return IdAllocator.layerWithLegacySubagentThreads(new Set(threadIds));
  }),
);

export const layerIdAllocator = layerIdAllocatorWithPersistenceReads.pipe(
  Layer.provide(SqliteAuxiliaryPersistenceReads.layer),
);

/** Reuse the continuation worker's exact layer reference so memoization shares one queue. */
export const layerWithPersistenceReads = Layer.mergeAll(
  ClaudeAdapterV2.layerQueryRunner,
  CodexAdapterV2.layerAppServerClientFactory,
  CursorAgentSdk.layer.pipe(Layer.provideMerge(CursorSdk.layer)),
  layerIdAllocatorWithPersistenceReads,
  ProviderContinuationRequests.layer,
);

export const layer = layerWithPersistenceReads.pipe(
  Layer.provide(SqliteAuxiliaryPersistenceReads.layer),
);
