import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import * as NodeCrypto from "node:crypto";

export const nativeComponents = [
  "policy",
  "project",
  "guest-share",
  "recovery",
  "provider",
  "generate",
  "maintenance",
  "outbox",
  "project-store",
];
export const nativeTargets = {
  "linux-x64": "x86_64-unknown-linux-gnu",
  "linux-arm64": "aarch64-unknown-linux-gnu",
  "darwin-x64": "x86_64-apple-darwin",
  "darwin-arm64": "aarch64-apple-darwin",
  "win32-x64": "x86_64-pc-windows-msvc",
  "win32-arm64": "aarch64-pc-windows-msvc",
};
export const nativeFile = (component, target) =>
  `t3-code-ls-${component}${target.startsWith("win32-") ? ".exe" : ""}`;
export const nativeHash = (file) =>
  NodeCrypto.createHash("sha256").update(NodeFS.readFileSync(file)).digest("hex");

export function stageNativeArtifacts(source, destination, target) {
  const manifest = JSON.parse(
    NodeFS.readFileSync(NodePath.resolve(source, "manifest.json"), "utf8"),
  );
  if (
    !nativeTargets[target] ||
    manifest.target !== target ||
    manifest.rustTarget !== nativeTargets[target]
  ) {
    throw new Error(
      `Native components do not match ${target}; build matching components before packaging`,
    );
  }
  for (const component of nativeComponents) {
    const file = nativeFile(component, target);
    const input = NodePath.resolve(source, file);
    if (manifest.files[file] !== nativeHash(input))
      throw new Error(`Native component hash mismatch: ${file}`);
    NodeFS.accessSync(
      input,
      target.startsWith("win32-") ? NodeFS.constants.R_OK : NodeFS.constants.X_OK,
    );
  }
  NodeFS.mkdirSync(destination, { recursive: true });
  for (const file of [
    ...nativeComponents.map((component) => nativeFile(component, target)),
    "manifest.json",
  ]) {
    if (NodePath.resolve(source, file) !== NodePath.resolve(destination, file))
      NodeFS.copyFileSync(NodePath.resolve(source, file), NodePath.resolve(destination, file));
  }
}
