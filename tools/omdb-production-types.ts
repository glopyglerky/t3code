import type * as Effect from "effect/Effect";
import type * as Layer from "effect/Layer";
import type * as SqlClient from "effect/sql/SqlClient";
import type * as Selected from "../apps/server/src/persistence/SelectedPersistence.ts";
import type * as Runtime from "../apps/server/src/orchestration-v2/selectedRuntimeLayer.ts";
import type * as Server from "../apps/server/src/server.ts";
import type * as ProviderHydration from "../apps/server/src/provider/ProviderInstanceRegistryHydration.ts";

type Assert<T extends true> = T;
export type ServerHasNoUnresolvedSqlClient = Assert<
  SqlClient.SqlClient extends Effect.Services<typeof Server.runServer> ? false : true
>;
export type SelectedRuntimeHasNoUnresolvedSqlClient = Assert<
  SqlClient.SqlClient extends Layer.Services<typeof Runtime.layerProduction> ? false : true
>;
export type SharedProviderHydrationHasNoUnresolvedSqlClient = Assert<
  SqlClient.SqlClient extends Layer.Services<typeof ProviderHydration.layerWithPersistenceReads>
    ? false
    : true
>;
export type EverySelectedRepositoryClosesItsDatabase = Assert<
  SqlClient.SqlClient extends Layer.Services<
    | typeof Selected.auxiliaryReads
    | typeof Selected.projections
    | typeof Selected.projects
    | typeof Selected.search
    | typeof Selected.settings
    | typeof Selected.cleanup
    | typeof Selected.storage
    | typeof Selected.viewedFiles
    | typeof Selected.reviewGuides
    | typeof Selected.peers
    | typeof Selected.knownWorkspaces
    | typeof Selected.usage
    | typeof Selected.authSessions
    | typeof Selected.authPairing
    | typeof Selected.layerConfig
  >
    ? false
    : true
>;
