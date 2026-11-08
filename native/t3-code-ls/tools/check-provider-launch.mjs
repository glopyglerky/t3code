import * as assert from "node:assert/strict";
import { selectNativeCodexProcess } from "../../../apps/server/src/provider/nativeCodexProcess.ts";

const upstream = { command: "/usr/local/bin/codex", args: ["app-server", "--config", "x=y"], shell: false };
assert.equal(selectNativeCodexProcess(undefined, upstream, "/tmp/project", "/tmp/home", "linux"), upstream);
assert.deepEqual(selectNativeCodexProcess("/usr/local/bin/t3-code-ls-provider", upstream, "/tmp/project", "/tmp/home", "linux"), {
  command: "/usr/local/bin/t3-code-ls-provider",
  args: ["--program", "/usr/local/bin/codex", "--cwd", "/tmp/project",
    "--home", "/tmp/home", "--inherit-env", "--", "app-server", "--config", "x=y"],
  shell: false,
});
for (const [binary, codex, cwd, home, platform] of [
  ["", upstream, "/tmp/project", "/tmp/home", "linux"],
  ["relative", upstream, "/tmp/project", "/tmp/home", "linux"],
  ["/usr/local/bin/native", { ...upstream, command: "codex" }, "/tmp/project", "/tmp/home", "linux"],
  ["/usr/local/bin/native", { ...upstream, shell: true }, "/tmp/project", "/tmp/home", "linux"],
  ["/usr/local/bin/native", upstream, "relative", "/tmp/home", "linux"],
  ["/usr/local/bin/native", upstream, "/tmp/project", "relative", "linux"],
  ["/usr/local/bin/native", upstream, "/tmp/project", "/tmp/home", "win32"],
]) {
  assert.throws(() => selectNativeCodexProcess(binary, codex, cwd, home, platform));
}
console.log("9 Codex launch-selection cases passed.");
