import * as NodeAssert from "node:assert/strict";
import * as NodeProcess from "node:process";
import * as NodeTest from "node:test";
import { observeFilesystem } from "./performance-filesystem.mjs";

const stats = {
  type: 0x794c7630n,
  bsize: 4096n,
  frsize: 4096n,
  blocks: 9007199254740993n,
  bfree: 7000000000000001n,
  bavail: 6000000000000001n,
  files: 1234n,
  ffree: 1200n,
};

NodeTest.test("filesystem counts remain exact beyond JavaScript's safe integer range", async () => {
  let calls = 0;
  const observed = await observeFilesystem("/fixture", async (path, options) => {
    calls++;
    NodeAssert.equal(path, "/fixture");
    NodeAssert.deepEqual(options, { bigint: true });
    return stats;
  });
  NodeAssert.equal(calls, 1);
  NodeAssert.equal(observed.status, "observed");
  NodeAssert.equal(observed.total_blocks, "9007199254740993");
  NodeAssert.equal(observed.available_blocks, "6000000000000001");
  NodeAssert.equal(observed.block_size_bytes, "4096");
  NodeAssert.equal(observed.fragment_size_bytes, "4096");
  NodeAssert.equal(observed.type_id_hex, "0x794c7630");
  NodeAssert.equal(observed.type_name, NodeProcess.platform === "linux" ? "overlayfs" : null);
});

NodeTest.test(
  "an unknown filesystem keeps its type ID without inventing a device classification",
  async () => {
    const observed = await observeFilesystem("/fixture", async () => ({
      ...stats,
      type: 0x12345678n,
      frsize: undefined,
    }));
    NodeAssert.equal(observed.type_id_hex, "0x12345678");
    NodeAssert.equal(observed.type_name, null);
    NodeAssert.equal(observed.fragment_size_bytes, null);
  },
);

NodeTest.test(
  "missing filesystem observations remain unavailable instead of zero capacity",
  async () => {
    const observed = await observeFilesystem("/fixture", async () => {
      throw Object.assign(new Error("not exposed"), { code: "ENOSYS" });
    });
    NodeAssert.equal(observed.status, "unavailable");
    NodeAssert.equal(observed.reason, "ENOSYS");
    NodeAssert.equal(observed.available_blocks, undefined);
    NodeAssert.equal(observed.type_id_hex, undefined);
  },
);
