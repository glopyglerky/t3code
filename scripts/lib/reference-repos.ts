export interface ReferenceRepo {
  readonly id: string;
  readonly prefix: string;
  readonly repository: string;
  readonly latestRef: string;
  readonly pinnedRef: string;
  readonly commit: string;
  readonly tree: string;
  readonly snapshotCommit: string;
  readonly snapshotTree: string;
  readonly snapshotGitlinks?: Readonly<Record<string, string>>;
}

export const referenceRepos: ReadonlyArray<ReferenceRepo> = [
  {
    id: "effect",
    prefix: ".repos/effect",
    repository: "https://github.com/Effect-TS/effect.git",
    latestRef: "main",
    pinnedRef: "effect@4.1.2",
    commit: "506eef143bcc1750ce58920aac1e45da7265d103",
    tree: "63d0d4c2e4ae8cb312819a34d46d6a43dbba4c7a",
    snapshotCommit: "00de749d71ebcf1cf38ccbc82cdd499fdeb20da7",
    snapshotTree: "db60e8ab965ed9e979b9f2efc19c31b1817a1b0c",
  },
  {
    id: "alchemy-effect",
    prefix: ".repos/alchemy-effect",
    repository: "https://github.com/alchemy-run/alchemy-effect.git",
    latestRef: "main",
    pinnedRef: "v2.0.0-beta.85",
    commit: "d0b4a3326718309793459160e94b35abee517b23",
    tree: "2ada27634e972a6a49f137ae3c134a6a52754166",
    snapshotCommit: "00de749d71ebcf1cf38ccbc82cdd499fdeb20da7",
    snapshotTree: "edae632610129cfe1589991982843dc99a34a303",
    snapshotGitlinks: { "submodules/distilled": "e02e9d405b8848909f4f746b61b12a663f631c7c" },
  },
];
