import { execFileSync } from "node:child_process";
import { mkdirSync, copyFileSync } from "node:fs";
import { resolve, dirname } from "node:path";
import { fileURLToPath } from "node:url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const toolchain = process.env.T3_CODE_LS_TOOLCHAIN;
if (!toolchain) throw new Error("Set T3_CODE_LS_TOOLCHAIN to the qualified Lustr/BlueFe toolchain directory");
const target = resolve(process.env.CARGO_TARGET_DIR ?? resolve(root, "target"));
const env = { ...process.env, RUSTC: resolve(toolchain, "bin/rustc"), RUSTDOC: resolve(toolchain, "bin/rustdoc"), CARGO_ENCODED_RUSTFLAGS: ["--sysroot", toolchain].join("\x1f") };
execFileSync(resolve(toolchain, "bin/lustr"), ["--bluefe-root", toolchain, "build", "--locked", "--release", "--target-dir", target, "-p", "t3-code-ls-core", "--bins"], { cwd: root, env, stdio: "inherit" });
const destination = resolve(root, process.argv[2] ?? "bin");
mkdirSync(destination, { recursive: true });
for (const name of ["policy", "project", "guest-share", "recovery", "provider", "generate", "maintenance", "outbox", "project-store"]) {
  const file = `t3-code-ls-${name}${process.platform === "win32" ? ".exe" : ""}`;
  copyFileSync(resolve(target, "release", file), resolve(destination, file));
}
