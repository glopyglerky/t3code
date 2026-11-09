import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

/** A real redb file shared by independently spawned database clients. */
export async function createFixture({
  tmpRoot = process.env.T3_CODE_LS_TEST_TMPDIR ?? NodeOS.tmpdir(),
} = {}) {
  const root = await NodeFSP.mkdtemp(NodePath.join(NodePath.resolve(tmpRoot), "t3-redb-gate-"));
  const databasePath = NodePath.join(root, "state.redb");
  return {
    root,
    databasePath,
    engineLabel: "redb 4.5.0 / MultiWriter / Immediate",
    cliArgs(mode = "connect") {
      if (mode === "initialize") return ["initialize-local", databasePath];
      if (mode === "connect") return ["local", databasePath];
      throw new Error(`Unknown database fixture mode: ${mode}`);
    },
    async close() {
      // Call only after every client has exited and released its file handles.
      await NodeFSP.rm(root, { recursive: true, force: true });
    },
  };
}
