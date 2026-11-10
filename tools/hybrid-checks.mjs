import * as Assert from "node:assert/strict";

const adapters = [
  ["command-and-dispatch-policy", "policy", "check-policy-adapter.mjs", "policy-tsconfig.json"],
  ["project-commands", "project", "check-project-adapter.mjs", "project-tsconfig.json"],
  ["provider-recovery", "recovery", "check-recovery-adapter.mjs", "recovery-tsconfig.json"],
  ["guest-share-decisions", "guest-share", "check-guest-share-adapter.mjs", "guest-share-tsconfig.json"],
  ["codex-process-runtime", "provider", "check-provider-adapter.mjs", "provider-tsconfig.json"],
  ["provider-maintenance", "maintenance", "check-maintenance-adapter.mjs", "maintenance-tsconfig.json"],
  ["codex-structured-generation", "generate", "check-generation-adapter.mjs", "generation-tsconfig.json"],
];

export const adapterPortIds = Object.freeze(adapters.map(([port]) => port));

export function selectAdapterPorts(requested) {
  if (requested === undefined) return [...adapterPortIds];
  Assert.equal(typeof requested, "string", "--ports requires comma-separated port IDs");
  const ids = requested.split(",").map((id) => id.trim());
  Assert.ok(ids.length > 0 && ids.every(Boolean), "--ports must not contain an empty selection");
  Assert.equal(new Set(ids).size, ids.length, "--ports must not repeat a port ID");
  for (const id of ids)
    Assert.ok(adapterPortIds.includes(id), `Unknown production adapter port: ${id}`);
  return adapterPortIds.filter((id) => ids.includes(id));
}

export function parseHybridArguments(args) {
  const [mode, ...rest] = args;
  Assert.ok(
    ["--verify-binaries", "--adapters", "--application"].includes(mode),
    "Usage: node tools/check-hybrid.mjs <--verify-binaries|--adapters|--application> [absolute-proof-directory] [--ports comma-separated-port-IDs]",
  );
  if (mode === "--verify-binaries") {
    Assert.equal(rest.length, 0, "Binary verification takes no qualification arguments");
    return { mode, proofDirectory: undefined, selectedPorts: [] };
  }
  let proofDirectory;
  let requested;
  for (let index = 0; index < rest.length; index++) {
    const value = rest[index];
    if (value === "--ports") {
      Assert.equal(mode, "--adapters", "--ports applies only to adapter qualification");
      Assert.equal(requested, undefined, "Specify --ports once");
      requested = rest[++index];
      Assert.notEqual(requested, undefined, "--ports requires comma-separated port IDs");
    } else {
      Assert.ok(!value.startsWith("--") && proofDirectory === undefined,
        `Unexpected qualification argument: ${value}`);
      proofDirectory = value;
    }
  }
  return {
    mode,
    proofDirectory,
    selectedPorts: mode === "--adapters" ? selectAdapterPorts(requested) : [],
  };
}

export function planAdapterChecks(selectedPorts) {
  // Revalidate callers as well as CLI selections before creating a command plan.
  const selected = selectAdapterPorts(selectedPorts.join(","));
  const checks = adapters.filter(([port]) => selected.includes(port)).map(([port, component, script, project]) => ({
    port,
    component,
    name: `${component} production adapter`,
    script: `tools/${script}`,
    proofFile: `${component}-adapter.json`,
    typecheckProject: `tools/${project}`,
    extraArguments: [],
  }));
  if (selected.includes("command-and-dispatch-policy")) checks.push({
    port: "command-and-dispatch-policy",
    component: "policy",
    name: "Production dispatch and goal policy",
    script: "tools/check-dispatch.mjs",
    proofFile: "dispatch.json",
    extraArguments: ["--service"],
  });
  return checks;
}

export function applicationChecks() {
  return [
    {
      name: "Fork native-executable selection contract",
      program: "pnpm",
      arguments: ["exec", "vp", "test", "run", "--maxWorkers", "2", "packages/shared/src/nativeExecutable.test.ts"],
    },
    {
      name: "Build the client for Rust integration",
      program: "pnpm",
      arguments: ["--dir", "apps/web", "run", "build"],
    },
    {
      name: "Build the server with native adapters",
      program: "pnpm",
      arguments: ["--dir", "apps/server", "run", "build:bundle"],
    },
  ];
}
