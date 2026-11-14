import * as NodeAssert from "node:assert/strict";
import * as NodeTest from "node:test";
import { processTreeRss } from "./performance-memory.mjs";

const files = (values) => async (path) => {
  if (!(path in values)) throw Object.assign(new Error("not available"), { code: "ENOENT" });
  return values[path];
};

NodeTest.test("RSS includes the worker and every visible descendant exactly once", async () => {
  const sample = await processTreeRss(
    10,
    files({
      "/proc/10/status": "VmRSS:\t 1200 kB\n",
      "/proc/10/task/10/children": "11 12 ",
      "/proc/11/status": "VmRSS:\t 800 kB\n",
      "/proc/11/task/11/children": "12 ",
      "/proc/12/status": "VmRSS:\t 400 kB\n",
      "/proc/12/task/12/children": "",
    }),
  );
  NodeAssert.deepEqual(sample, { rss_kib: 2400, status: "sampled" });
});

NodeTest.test(
  "missing child enumeration cannot become zero or Node-only process-tree RSS",
  async () => {
    const sample = await processTreeRss(10, files({ "/proc/10/status": "VmRSS:\t 1200 kB\n" }));
    NodeAssert.equal(sample.rss_kib, null);
    NodeAssert.equal(sample.status, "unavailable");
    NodeAssert.match(sample.reason, /enumeration unavailable/);
  },
);

NodeTest.test("a missing child or RSS field makes that sample explicitly unavailable", async () => {
  for (const content of [
    { "/proc/10/status": "Name:\tnode\n", "/proc/10/task/10/children": "" },
    { "/proc/10/status": "VmRSS:\t 1200 kB\n", "/proc/10/task/10/children": "11 " },
  ]) {
    const sample = await processTreeRss(10, files(content));
    NodeAssert.equal(sample.rss_kib, null);
    NodeAssert.equal(sample.status, "unavailable");
  }
});
