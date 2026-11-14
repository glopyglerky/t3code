import * as NodeFSP from "node:fs/promises";
import * as NodeProcess from "node:process";

// Linux include/uapi/linux/magic.h. Other filesystem IDs remain explicit and unnamed.
const linuxTypes = new Map([
  [0x01021994n, "tmpfs"],
  [0x794c7630n, "overlayfs"],
]);

/** One bounded statfs call; it does not inspect devices or establish persistence guarantees. */
export async function observeFilesystem(path, statfs = NodeFSP.statfs) {
  const observation = { path, observed_at: new Date().toISOString() };
  try {
    const stats = await statfs(path, { bigint: true });
    return {
      ...observation,
      status: "observed",
      type_id_hex: `0x${stats.type.toString(16)}`,
      type_name: NodeProcess.platform === "linux" ? (linuxTypes.get(stats.type) ?? null) : null,
      block_size_bytes: stats.bsize.toString(),
      fragment_size_bytes: stats.frsize?.toString() ?? null,
      total_blocks: stats.blocks.toString(),
      free_blocks: stats.bfree.toString(),
      available_blocks: stats.bavail.toString(),
      total_file_nodes: stats.files.toString(),
      free_file_nodes: stats.ffree.toString(),
      units: "Exact decimal strings from Node statfs; block counts are filesystem blocks.",
    };
  } catch (error) {
    return {
      ...observation,
      status: "unavailable",
      reason: error.code ?? "Filesystem statistics unavailable",
    };
  }
}
