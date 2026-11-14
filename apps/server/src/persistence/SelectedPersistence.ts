import * as Layer from "effect/Layer";
import { applicationDatabase } from "@t3tools/shared/applicationDatabase";
import * as Omdb from "./Omdb.ts";
import * as Sqlite from "./Sqlite.ts";
import * as PersistenceTransactions from "./PersistenceTransactions.ts";
import * as NativeLinkedThreads from "../pullRequest/NativeLinkedThreads.ts";
import * as LinkedPullRequestThreads from "../pullRequest/linkedThreads.ts";
import * as NativeProjectionStore from "../orchestration-v2/NativeProjectionStore.ts";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import * as NativeProjectRows from "../orchestration-v2/NativeProjectRows.ts";
import * as ProjectStore from "../orchestration-v2/ProjectStore.ts";
import * as NativeThreadSearch from "../orchestration-v2/NativeThreadSearch.ts";
import * as ThreadSearch from "../orchestration-v2/ThreadSearch.ts";
import * as NativeAuxiliaryPersistenceReads from "./NativeAuxiliaryPersistenceReads.ts";
import * as SqliteAuxiliaryPersistenceReads from "./SqliteAuxiliaryPersistenceReads.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as StorageCleanup from "../storageCleanup.ts";
import * as DatabaseStorage from "./DatabaseStorage.ts";
import * as NativeDatabaseStorageBackend from "./NativeDatabaseStorageBackend.ts";
import * as DatabaseMaintenance from "./DatabaseMaintenance.ts";
import * as NativeDatabaseMaintenance from "./NativeDatabaseMaintenance.ts";
import * as NativePullRequestRows from "./NativePullRequestRows.ts";
import * as PullRequestFilesViewed from "./PullRequestFilesViewed.ts";
import * as PullRequestReviewGuides from "./PullRequestReviewGuides.ts";
import * as NativePeerLinkRows from "../peer/NativePeerLinkRows.ts";
import * as PeerLinks from "../peer/PeerLinks.ts";
import * as NativeKnownWorkspaceRows from "../workspace/NativeKnownWorkspaceRows.ts";
import * as KnownWorkspaces from "../workspace/KnownWorkspaces.ts";
import * as NativeCodexUsageHistory from "../usage/NativeCodexUsageHistory.ts";
import * as CodexUsageHistory from "../usage/CodexUsageHistory.ts";
import * as NativeAuthSessions from "./NativeAuthSessions.ts";
import * as AuthSessions from "./AuthSessions.ts";
import * as NativeAuthPairingLinks from "./NativeAuthPairingLinks.ts";
import * as AuthPairingLinks from "./AuthPairingLinks.ts";
import * as NativeAuthConnectDevices from "./NativeAuthConnectDevices.ts";
import * as AuthConnectDevices from "./AuthConnectDevices.ts";
import * as NativeProjectionMaintenance from "../orchestration-v2/NativeProjectionMaintenance.ts";
import * as ProjectionMaintenance from "../orchestration-v2/ProjectionMaintenance.ts";

export const selection = applicationDatabase();
/** Branches expose the same public service. Their private database requirements
 * are closed before selection, so no caller can accidentally supply SQLite to
 * one native component (or create a second database owner). Layer memoization
 * shares the exact Omdb.layerConfig instance throughout the application graph. */
export function select<A, EN, RN, ES, RS>(
  nativeLayer: Layer.Layer<A, EN, RN>,
  sqliteLayer: Layer.Layer<A, ES, RS>,
): Layer.Layer<A, EN | ES, RN | RS> {
  return selection.kind === "omdb" ? nativeLayer : sqliteLayer;
}
const native = <A, E, R>(layer: Layer.Layer<A, E, R>) =>
  layer.pipe(Layer.provide(Omdb.layerConfig));
const sqlite = <A, E, R>(layer: Layer.Layer<A, E, R>) =>
  layer.pipe(Layer.provide(Sqlite.layerConfig));
export const layerConfig = select(
  native(Layer.mergeAll(PersistenceTransactions.redbLayer, NativeLinkedThreads.layer)),
  sqlite(Layer.mergeAll(PersistenceTransactions.sqliteLayer, LinkedPullRequestThreads.layerSqlite)),
);
export const auxiliaryReads = select(
  native(NativeAuxiliaryPersistenceReads.layer),
  sqlite(SqliteAuxiliaryPersistenceReads.layer),
);
export const projections = select(
  native(NativeProjectionStore.layer),
  sqlite(ProjectionStore.layer),
);
export const projects = select(
  native(ProjectStore.layerWithRows.pipe(Layer.provide(NativeProjectRows.layer))),
  sqlite(ProjectStore.layer),
);
export const search = select(native(NativeThreadSearch.layer), sqlite(ThreadSearch.layer));
export const settings = select(
  native(
    ServerSettings.layerWithPersistenceReads.pipe(
      Layer.provide(NativeAuxiliaryPersistenceReads.layer),
    ),
  ),
  sqlite(ServerSettings.layer),
);
export const cleanup = select(
  native(
    StorageCleanup.layerWithPersistenceReads.pipe(
      Layer.provide(NativeAuxiliaryPersistenceReads.layer),
    ),
  ),
  sqlite(StorageCleanup.layer),
);
export const storage = select(
  native(DatabaseStorage.layerWithBackend.pipe(Layer.provide(NativeDatabaseStorageBackend.layer))),
  sqlite(DatabaseStorage.layer),
);
export const projectionMaintenance = select(
  native(NativeProjectionMaintenance.layer),
  sqlite(ProjectionMaintenance.layer),
);
export const maintenance = select(
  NativeDatabaseMaintenance.layer(selection.kind === "omdb" ? selection.executable : ""),
  DatabaseMaintenance.layer,
);
export const viewedFiles = select(
  native(NativePullRequestRows.layerViewedFiles),
  sqlite(PullRequestFilesViewed.layer),
);
export const reviewGuides = select(
  native(NativePullRequestRows.layerGuide),
  sqlite(PullRequestReviewGuides.layer),
);
export const peers = select(
  native(PeerLinks.layerWithRows.pipe(Layer.provide(NativePeerLinkRows.layer))),
  sqlite(PeerLinks.layer),
);
export const knownWorkspaces = select(
  native(KnownWorkspaces.layerWithRows.pipe(Layer.provide(NativeKnownWorkspaceRows.layer))),
  sqlite(KnownWorkspaces.layer),
);
export const usage = select(native(NativeCodexUsageHistory.layer), sqlite(CodexUsageHistory.layer));
export const authSessions = select(native(NativeAuthSessions.layer), sqlite(AuthSessions.layer));
export const authPairing = select(
  native(NativeAuthPairingLinks.layer),
  sqlite(AuthPairingLinks.layer),
);
export const authConnectDevices = select(
  native(NativeAuthConnectDevices.layer),
  sqlite(AuthConnectDevices.layer),
);

// CLI readers may share a current server's schema, but pending migrations need
// the same exclusive ownership check as upstream's CLI persistence layer.
const sqliteCli = <A, E, R>(layer: Layer.Layer<A, E, R>) =>
  layer.pipe(Layer.provide(Sqlite.layerCliConfig));
export const cliLayerConfig = select(
  native(Layer.mergeAll(PersistenceTransactions.redbLayer, NativeLinkedThreads.layer)),
  sqliteCli(
    Layer.mergeAll(PersistenceTransactions.sqliteLayer, LinkedPullRequestThreads.layerSqlite),
  ),
);
export const cliPeers = select(
  native(PeerLinks.layerWithRows.pipe(Layer.provide(NativePeerLinkRows.layer))),
  sqliteCli(PeerLinks.layer),
);
export const cliSettings = select(
  native(
    ServerSettings.layerWithPersistenceReads.pipe(
      Layer.provide(NativeAuxiliaryPersistenceReads.layer),
    ),
  ),
  sqliteCli(ServerSettings.layer),
);
export const cliAuthSessions = select(
  native(NativeAuthSessions.layer),
  sqliteCli(AuthSessions.layer),
);
export const cliAuthPairing = select(
  native(NativeAuthPairingLinks.layer),
  sqliteCli(AuthPairingLinks.layer),
);
export const cliAuthConnectDevices = select(
  native(NativeAuthConnectDevices.layer),
  sqliteCli(AuthConnectDevices.layer),
);
