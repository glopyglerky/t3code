import * as NodeFSP from "node:fs/promises";

const readTextFile = (path) => NodeFSP.readFile(path, "utf8");

/** A missing descendant list makes a whole-process measurement unavailable. */
export async function processTreeRss(pid, readText = readTextFile) {
  const seen = new Set();
  const unavailable = (reason) => ({ rss_kib: null, status: "unavailable", reason });
  const read = async (id) => {
    if (seen.has(id)) return { rss_kib: 0, status: "sampled" };
    seen.add(id);
    let status, children;
    try {
      status = await readText(`/proc/${id}/status`);
      children = await readText(`/proc/${id}/task/${id}/children`);
    } catch (error) {
      if (["ENOENT", "ESRCH", "EACCES", "EPERM"].includes(error.code))
        return unavailable(`Process or descendant enumeration unavailable (${error.code})`);
      throw error;
    }
    const match = status.match(/^VmRSS:\s+(\d+)\s+kB\s*$/m);
    if (match === null) return unavailable("Process RSS is not exposed");
    let total = Number(match[1]);
    if (!Number.isSafeInteger(total)) return unavailable("Process RSS is not a safe integer");
    for (const child of children.trim().split(/\s+/).filter(Boolean)) {
      if (!/^[1-9][0-9]*$/.test(child)) return unavailable("Malformed descendant process list");
      const sample = await read(Number(child));
      if (sample.rss_kib === null) return sample;
      total += sample.rss_kib;
    }
    return { rss_kib: total, status: "sampled" };
  };
  return read(pid);
}
