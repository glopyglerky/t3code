import * as WorktreeBranchSync from "./WorktreeBranchSync.ts";
import * as GuestShareService from "../guest/GuestShareService.ts";
import * as UsageLimitRecoveryWorker from "./UsageLimitRecoveryWorker.ts";
import * as Scheduler from "../scheduling/Scheduler.ts";
import * as Layer from "effect/Layer";
import * as GitVcsDriver from "../vcs/GitVcsDriver.ts";
import * as OrchestrationCommandReceipts from "../persistence/OrchestrationCommandReceipts.ts";
import * as OrchestrationEventStore from "../persistence/OrchestrationEventStore.ts";
import * as McpAppModelContext from "../mcpApps/McpAppModelContext.ts";
import * as PreviewAgentActivity from "../preview/PreviewAgentActivity.ts";
import * as McpAppRequests from "../mcpApps/McpAppRequests.ts";
import * as ProviderSessionRuntime from "../persistence/ProviderSessionRuntime.ts";
import * as TextGeneration from "../textGeneration/TextGeneration.ts";
import * as ProviderAuthService from "../provider/ProviderAuthService.ts";
import * as AgentSessionImporter from "../project/AgentSessionImporter.ts";
import * as AgentSessionScanner from "../project/AgentSessionScanner.ts";
import * as ProjectService from "../project/ProjectService.ts";
import * as ProjectSetupScriptRunner from "../project/ProjectSetupScriptRunner.ts";
import * as ManagedProjectFolders from "../project/ManagedProjectFolders.ts";
import * as ScratchThreadPromotion from "../project/ScratchThreadPromotion.ts";
import * as CheckpointCaptureService from "./CheckpointCaptureService.ts";
import * as CheckpointService from "./CheckpointService.ts";
import * as CheckpointRollbackService from "./CheckpointRollbackService.ts";
import * as CommandPolicy from "./CommandPolicy.ts";
import * as CommandReceiptStore from "./CommandReceiptStore.ts";
import * as ContextHandoffService from "./ContextHandoffService.ts";
import * as EffectOutbox from "./EffectOutbox.ts";
import * as EffectWorker from "./EffectWorker.ts";
import * as EventSink from "./EventSink.ts";
import * as EventStore from "./EventStore.ts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as LegacyV1ThreadImporter from "./legacy/LegacyV1ThreadImporter.ts";
import * as Orchestrator from "./Orchestrator.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as ProjectionMaintenance from "./ProjectionMaintenance.ts";
import * as ProjectStore from "./ProjectStore.ts";
import * as ProviderAdapterRegistry from "./ProviderAdapterRegistry.ts";
import * as ProviderContinuationRequests from "@t3tools/provider-core/server/ProviderContinuationRequests";
import * as ProviderContinuationService from "./ProviderContinuationService.ts";
import * as ThreadTitleRegenerationService from "./ThreadTitleRegenerationService.ts";
import * as ProviderEventIngestor from "./ProviderEventIngestor.ts";
import * as ThreadCommandExecutor from "./ThreadCommandExecutor.ts";
import * as ProviderSessionManager from "./ProviderSessionManager.ts";
import * as CommandOutputHub from "./CommandOutputHub.ts";
import * as ProviderRuntimeRecoveryService from "./ProviderRuntimeRecoveryService.ts";
import * as ProviderSwitchService from "./ProviderSwitchService.ts";
import * as ProviderTurnControlService from "./ProviderTurnControlService.ts";
import * as ProviderTurnStartService from "./ProviderTurnStartService.ts";
import * as RunExecutionService from "./RunExecutionService.ts";
import * as RunFinalizationService from "./RunFinalizationService.ts";
import * as ResourceCleanupService from "./ResourceCleanupService.ts";
import * as RuntimePolicy from "./RuntimePolicy.ts";
import * as RuntimeRequestService from "./RuntimeRequestService.ts";
import * as ThreadManagementService from "./ThreadManagementService.ts";
import * as ThreadLaunchService from "./ThreadLaunchService.ts";
import * as ThreadForkService from "./ThreadForkService.ts";
import * as TurnItemPositionStore from "./TurnItemPositionStore.ts";
import * as ScheduledTaskService from "../scheduledTasks/ScheduledTaskService.ts";
import * as SecretRequests from "../secrets/SecretRequests.ts";

/** Persistence is chosen once; the application graph and shared worker instances stay unchanged. */
interface RuntimePersistenceServices {
  readonly eventInfrastructure:
    | OrchestrationEventStore.OrchestrationEventStore
    | OrchestrationCommandReceipts.OrchestrationCommandReceiptRepository;
  readonly projections: ProjectionStore.ProjectionStoreV2;
  readonly projects: ProjectStore.ProjectStoreV2;
  readonly outbox: EffectOutbox.EffectOutboxV2;
  readonly positions: TurnItemPositionStore.TurnItemPositionStoreV2;
  readonly eventSink: EventSink.EventSinkV2;
  readonly maintenance: ProjectionMaintenance.ProjectionMaintenanceV2;
  readonly legacy: LegacyV1ThreadImporter.LegacyV1ThreadImporter;
  readonly providerRuntime: ProviderSessionRuntime.ProviderSessionRuntimeRepository;
  readonly mcpContext: McpAppModelContext.McpAppModelContext;
  readonly scheduledTasks: ScheduledTaskService.ScheduledTaskService;
}

type RuntimePersistenceChannels = { readonly [Key in keyof RuntimePersistenceServices]: unknown };

export type RuntimePersistenceLayers<
  Errors extends RuntimePersistenceChannels,
  Requirements extends RuntimePersistenceChannels,
> = {
  readonly [Key in keyof RuntimePersistenceServices]: Layer.Layer<
    RuntimePersistenceServices[Key],
    Errors[Key],
    Requirements[Key]
  >;
};

export const makeRuntimeLayers = <
  EventError,
  EventServices,
  ProjectionError,
  ProjectionServices,
  ProjectError,
  ProjectServices,
  OutboxError,
  OutboxServices,
  PositionError,
  PositionServices,
  SinkError,
  SinkServices,
  MaintenanceError,
  MaintenanceServices,
  LegacyError,
  LegacyServices,
  ProviderError,
  ProviderServices,
  McpError,
  McpServices,
  ScheduledError,
  ScheduledServices,
>(
  persistence: RuntimePersistenceLayers<
    {
      eventInfrastructure: EventError;
      projections: ProjectionError;
      projects: ProjectError;
      outbox: OutboxError;
      positions: PositionError;
      eventSink: SinkError;
      maintenance: MaintenanceError;
      legacy: LegacyError;
      providerRuntime: ProviderError;
      mcpContext: McpError;
      scheduledTasks: ScheduledError;
    },
    {
      eventInfrastructure: EventServices;
      projections: ProjectionServices;
      projects: ProjectServices;
      outbox: OutboxServices;
      positions: PositionServices;
      eventSink: SinkServices;
      maintenance: MaintenanceServices;
      legacy: LegacyServices;
      providerRuntime: ProviderServices;
      mcpContext: McpServices;
      scheduledTasks: ScheduledServices;
    }
  >,
) => {
  const layerEventInfrastructure = persistence.eventInfrastructure;

  const layerRuntimePolicyProvided = RuntimePolicy.layerFromProjectStore.pipe(
    Layer.provide(persistence.projects),
  );

  const layerEventStoreProvided = EventStore.layerFromOrchestrationEventStore.pipe(
    Layer.provide(layerEventInfrastructure),
  );
  const layerCommandReceiptStoreProvided = CommandReceiptStore.layerFromApplicationReceipts.pipe(
    Layer.provide(layerEventInfrastructure),
  );

  const layerStores = Layer.mergeAll(
    layerEventInfrastructure,
    layerEventStoreProvided,
    persistence.projections,
    persistence.projects,
    layerCommandReceiptStoreProvided,
    persistence.outbox,
    persistence.positions,
  );

  const layerEventSink = persistence.eventSink.pipe(Layer.provide(layerStores));
  const layerProjectionMaintenanceProvided = persistence.maintenance.pipe(
    Layer.provide(layerStores),
  );
  const layerLegacyV1ThreadImporterProvided = persistence.legacy.pipe(
    Layer.provide(layerEventSink),
  );

  const layerProjectService = ProjectService.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        persistence.projects,
        persistence.projections,
        layerEventSink,
        IdAllocator.layer,
        layerLegacyV1ThreadImporterProvided,
      ),
    ),
  );

  const layerProviderEventIngestorProvided = ProviderEventIngestor.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        layerEventSink,
        IdAllocator.layer,
        persistence.projections,
        ThreadCommandExecutor.layer,
        GitVcsDriver.layer,
      ),
    ),
  );

  const layerCheckpointServiceProvided = CheckpointService.layer.pipe(
    Layer.provide(IdAllocator.layer),
  );
  const layerContextHandoffServiceProvided = ContextHandoffService.layer.pipe(
    Layer.provide(IdAllocator.layer),
  );

  const layerProviderSwitchServiceProvided = ProviderSwitchService.layer.pipe(
    Layer.provide(ProviderAdapterRegistry.layerFromProviderInstanceRegistry),
  );

  const layerCommandOutputHubProvided = CommandOutputHub.layer.pipe(
    Layer.provide(Layer.merge(layerEventSink, persistence.projections)),
  );

  const layerProviderSessionManagerProvided = ProviderSessionManager.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        layerCommandOutputHubProvided,
        ProviderAdapterRegistry.layerFromProviderInstanceRegistry,
        layerEventSink,
        IdAllocator.layer,
        layerProviderEventIngestorProvided,
        persistence.projections,
      ),
    ),
  );

  const layerProviderAuthServiceProvided = ProviderAuthService.layer.pipe(
    Layer.provide(Layer.merge(persistence.projections, layerProviderSessionManagerProvided)),
  );

  const layerRunExecutionServiceProvided = RunExecutionService.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        persistence.mcpContext,
        PreviewAgentActivity.layer,
        layerCheckpointServiceProvided,
        layerEventSink,
        IdAllocator.layer,
        layerProviderEventIngestorProvided,
        persistence.positions,
      ),
    ),
  );

  const layerProviderTurnStartServiceProvided = ProviderTurnStartService.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        layerContextHandoffServiceProvided,
        layerEventSink,
        IdAllocator.layer,
        persistence.projections,
        layerProviderSessionManagerProvided,
        layerProviderAuthServiceProvided,
        layerRunExecutionServiceProvided,
        layerRuntimePolicyProvided,
      ),
    ),
  );

  const layerProviderTurnControlServiceProvided = ProviderTurnControlService.layer.pipe(
    Layer.provide(Layer.merge(persistence.projections, layerProviderSessionManagerProvided)),
  );
  const layerRuntimeRequestServiceProvided = RuntimeRequestService.layer.pipe(
    Layer.provide(Layer.merge(persistence.projections, layerProviderSessionManagerProvided)),
  );
  const layerCheckpointRollbackServiceProvided = CheckpointRollbackService.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        persistence.projects,
        layerCheckpointServiceProvided,
        layerEventSink,
        IdAllocator.layer,
        persistence.projections,
        layerProviderSessionManagerProvided,
        ThreadCommandExecutor.layer,
        layerRuntimePolicyProvided,
      ),
    ),
  );
  const layerCheckpointCaptureServiceProvided = CheckpointCaptureService.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        layerCheckpointServiceProvided,
        layerEventSink,
        IdAllocator.layer,
        persistence.projections,
      ),
    ),
  );
  const layerRunFinalizationServiceProvided = RunFinalizationService.layer.pipe(
    Layer.provide(Layer.merge(layerCheckpointCaptureServiceProvided, persistence.projections)),
  );

  const layerOrchestratorProvided = Orchestrator.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        layerCheckpointServiceProvided,
        CommandPolicy.layer,
        layerStores,
        layerEventSink,
        layerCommandReceiptStoreProvided,
        layerContextHandoffServiceProvided,
        IdAllocator.layer,
        persistence.projects,
        ProviderAdapterRegistry.layerFromProviderInstanceRegistry,
        // Reuse the continuation worker's layer reference so memoization shares one request queue.
        ProviderContinuationRequests.layer,
        layerProviderEventIngestorProvided,
        layerRuntimePolicyProvided,
        layerProviderSessionManagerProvided,
        layerProviderSwitchServiceProvided,
        layerRunExecutionServiceProvided,
        ThreadForkService.layer,
      ),
    ),
  );

  const layerAgentSessionImporterProvided = AgentSessionImporter.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        AgentSessionScanner.layer,
        layerProjectService,
        layerOrchestratorProvided,
        layerEventSink,
        IdAllocator.layer,
        persistence.providerRuntime,
      ),
    ),
  );

  const layerThreadManagementProvided = ThreadManagementService.layerWithLegacyImporter.pipe(
    Layer.provide(
      Layer.mergeAll(
        layerOrchestratorProvided,
        layerLegacyV1ThreadImporterProvided,
        layerCommandReceiptStoreProvided,
        layerProviderSessionManagerProvided,
      ),
    ),
  );
  const layerProjectSetupScriptRunner = ProjectSetupScriptRunner.layer.pipe(
    Layer.provide(layerProjectService),
    // Whether a thread came from a linked environment, which skips setup.
    Layer.provide(persistence.projections),
  );
  const layerManagedProjectFoldersProvided = ManagedProjectFolders.layer.pipe(
    Layer.provide(layerProjectService),
  );
  const layerThreadLaunchProvided = ThreadLaunchService.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        layerProjectService,
        layerProjectSetupScriptRunner,
        layerManagedProjectFoldersProvided,
        layerThreadManagementProvided,
        layerCommandReceiptStoreProvided,
        IdAllocator.layer,
      ),
    ),
  );
  const layerSecretRequestsProvided = SecretRequests.layer.pipe(
    Layer.provide(layerThreadManagementProvided),
  );
  const layerScheduledTaskProvided = persistence.scheduledTasks.pipe(
    Layer.provide(
      Layer.mergeAll(
        layerThreadLaunchProvided,
        layerThreadManagementProvided,
        layerManagedProjectFoldersProvided,
        layerSecretRequestsProvided,
      ),
    ),
  );
  const layerScratchThreadPromotionProvided = ScratchThreadPromotion.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        layerProjectService,
        layerManagedProjectFoldersProvided,
        layerThreadManagementProvided,
        layerScheduledTaskProvided,
      ),
    ),
  );
  const layerProviderContinuationWorkerProvided = ProviderContinuationService.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        ProviderContinuationRequests.layer,
        layerThreadManagementProvided,
        IdAllocator.layer,
      ),
    ),
  );
  const layerThreadTitleRegenerationProvided = ThreadTitleRegenerationService.layer.pipe(
    Layer.provide(
      Layer.mergeAll(layerThreadManagementProvided, persistence.projects, TextGeneration.layer),
    ),
  );
  const layerEffectExecutorProvided = EffectWorker.layerExecutor.pipe(
    Layer.provide(
      Layer.mergeAll(
        layerRunFinalizationServiceProvided,
        layerCheckpointRollbackServiceProvided,
        layerProviderSessionManagerProvided,
        layerProviderTurnControlServiceProvided,
        layerProviderTurnStartServiceProvided,
        layerRuntimeRequestServiceProvided,
        layerThreadTitleRegenerationProvided,
        layerThreadManagementProvided,
      ),
    ),
  );
  const layerEffectWorkerProvided = EffectWorker.layer.pipe(
    Layer.provide(Layer.merge(layerStores, layerEffectExecutorProvided)),
  );
  const layerProviderRuntimeRecoveryProvided = ProviderRuntimeRecoveryService.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        layerEffectWorkerProvided,
        layerStores,
        layerEventSink,
        IdAllocator.layer,
        persistence.projections,
      ),
    ),
  );

  const layerMcpAppRequestsProvided = McpAppRequests.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        persistence.mcpContext,
        layerOrchestratorProvided,
        layerThreadManagementProvided,
        layerProviderSessionManagerProvided,
      ),
    ),
  );

  const layer = Layer.mergeAll(
    layerCommandOutputHubProvided,
    layerOrchestratorProvided,
    layerMcpAppRequestsProvided,
    layerThreadManagementProvided,
    layerEffectWorkerProvided,
    layerProviderSessionManagerProvided,
    layerProviderAuthServiceProvided,
    layerProviderRuntimeRecoveryProvided,
    layerProjectionMaintenanceProvided,
    layerLegacyV1ThreadImporterProvided,
  );

  const layerProduction = Layer.mergeAll(
    WorktreeBranchSync.layer.pipe(
      Layer.provide(Layer.merge(layerThreadManagementProvided, persistence.projections)),
    ),
    layer.pipe(Layer.provide(layerProjectService)),
    layerProjectService,
    layerManagedProjectFoldersProvided,
    layerThreadLaunchProvided,
    layerScheduledTaskProvided,
    layerScratchThreadPromotionProvided,
    layerSecretRequestsProvided,
    GuestShareService.layer.pipe(
      Layer.provide(Layer.merge(layerOrchestratorProvided, layerCommandOutputHubProvided)),
    ),
    UsageLimitRecoveryWorker.layer.pipe(
      Layer.provide(
        Layer.mergeAll(
          persistence.projections,
          layerThreadManagementProvided,
          layerOrchestratorProvided,
        ),
      ),
    ),
    layerProviderContinuationWorkerProvided,
    layerAgentSessionImporterProvided,
    EffectOutbox.layerPruneWorker.pipe(Layer.provide(persistence.outbox)),
  ).pipe(
    Layer.provide(ResourceCleanupService.layer.pipe(Layer.provide(layerScheduledTaskProvided))),
    Layer.provide(Scheduler.layer),
    Layer.provideMerge(layerEventInfrastructure),
  );

  return {
    layerEventInfrastructure,
    layerEventSink,
    layerProjectService,
    layerProjectSetupScriptRunner,
    layer,
    layerProduction,
  };
};

const sqliteRuntime = makeRuntimeLayers({
  eventInfrastructure: Layer.mergeAll(
    OrchestrationEventStore.layer,
    OrchestrationCommandReceipts.layer,
  ),
  projections: ProjectionStore.layer,
  projects: ProjectStore.layer,
  outbox: EffectOutbox.layer,
  positions: TurnItemPositionStore.layer,
  eventSink: EventSink.layerFromStores,
  maintenance: ProjectionMaintenance.layer,
  legacy: LegacyV1ThreadImporter.layer,
  providerRuntime: ProviderSessionRuntime.layer,
  mcpContext: McpAppModelContext.layer,
  scheduledTasks: ScheduledTaskService.layer,
});

// Retained SQLite composition remains available to upstream tests and migration oracles.
export const {
  layerEventInfrastructure,
  layerEventSink,
  layerProjectService,
  layerProjectSetupScriptRunner,
  layer,
  layerProduction,
} = sqliteRuntime;
