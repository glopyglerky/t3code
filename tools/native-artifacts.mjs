import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import * as NodeCrypto from "node:crypto";
import {
  isQualifiedNativeHost,
  supportedNativeTargets,
  databaseArtifactFiles,
} from "../packages/shared/src/nativeExecutable.ts";

export { isQualifiedNativeHost };

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
  "db",
];
export const nativeTargets = supportedNativeTargets;
// Qualified service-helper scope. Other product targets keep their upstream implementations.
export const nativeQualifiedTargets = ["linux-x64"];
export const nativeTargetQualified = (target) => nativeQualifiedTargets.includes(target);
const upstreamCommit = () =>
  JSON.parse(NodeFS.readFileSync(new URL("../upstream.lock.json", import.meta.url), "utf8"))
    .upstream.commit;
export const nativeFile = (component, target) =>
  `t3-code-ls-${component}${target.startsWith("win32-") ? ".exe" : ""}`;
export const nativeHash = (file) =>
  NodeCrypto.createHash("sha256").update(NodeFS.readFileSync(file)).digest("hex");

function validateUpstreamSelection(target, selection) {
  if (
    !nativeTargets[target] ||
    !["unqualified-platform", "explicit-upstream", "incompatible-host-libc"].includes(selection) ||
    (nativeTargetQualified(target) && selection === "unqualified-platform")
  )
    throw new Error(`Upstream-only service packaging is not selected for ${target}`);
}

export function stageUpstreamManifest(destination, target, selection = "unqualified-platform") {
  validateUpstreamSelection(target, selection);
  const priorPath = NodePath.join(destination, "manifest.json");
  const prior = NodeFS.existsSync(priorPath)
    ? JSON.parse(NodeFS.readFileSync(priorPath, "utf8"))
    : undefined;
  const obsolete = [];
  for (const component of nativeComponents) {
    for (const file of [nativeFile(component, "linux-x64"), nativeFile(component, "win32-x64")]) {
      const existing = NodePath.join(destination, file);
      if (NodeFS.existsSync(existing)) {
        if (!prior?.files || prior.files[file] !== nativeHash(existing))
          throw new Error(
            `Use a clean upstream-only output directory; unverified native component: ${file}`,
          );
        obsolete.push(existing);
      }
    }
  }
  NodeFS.mkdirSync(destination, { recursive: true });
  for (const file of obsolete) NodeFS.unlinkSync(file);
  NodeFS.writeFileSync(
    NodePath.join(destination, "manifest.json"),
    JSON.stringify(
      {
        target,
        rustTarget: nativeTargets[target],
        implementation: "upstream",
        selection,
        upstreamCommit: upstreamCommit(),
        scope:
          "Retained upstream service implementations selected before execution; native helpers are not included.",
        files: {},
      },
      null,
      2,
    ) + "\n",
  );
}

export function nativeArtifactFiles(source, target, options = {}) {
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
  if (manifest.implementation === "upstream") {
    validateUpstreamSelection(target, manifest.selection);
    if (
      manifest.upstreamCommit !== upstreamCommit() ||
      !manifest.files ||
      typeof manifest.files !== "object" ||
      Array.isArray(manifest.files) ||
      Object.keys(manifest.files).length !== 0
    )
      throw new Error(`Invalid upstream-only service manifest for ${target}`);
    for (const component of nativeComponents) {
      for (const file of [nativeFile(component, "linux-x64"), nativeFile(component, "win32-x64")]) {
        if (NodeFS.existsSync(NodePath.join(source, file)))
          throw new Error(`Upstream-only manifest contains a native component: ${file}`);
      }
    }
    return ["manifest.json"];
  }
  if (manifest.implementation === "database-only") {
    if (manifest.upstreamCommit !== upstreamCommit())
      throw new Error("Database acceptance belongs to another upstream revision");
    const files = databaseArtifactFiles(manifest, source, target, options);
    for (const component of nativeComponents) {
      for (const file of [nativeFile(component, "linux-x64"), nativeFile(component, "win32-x64")]) {
        if (!files.includes(file) && NodeFS.existsSync(NodePath.join(source, file)))
          throw new Error(`Database-only bundle contains an unrelated native component: ${file}`);
      }
    }
    return files;
  }
  if (!nativeTargetQualified(target))
    throw new Error(
      `Native service components are not qualified for ${target}; retain upstream implementations`,
    );
  if (manifest.implementation !== undefined && manifest.implementation !== "native")
    throw new Error(`Unknown service implementation: ${manifest.implementation}`);
  if (
    !manifest.files ||
    typeof manifest.files !== "object" ||
    Array.isArray(manifest.files) ||
    Object.keys(manifest.files).length !== nativeComponents.length
  )
    throw new Error("Native component manifest must identify every selected component");
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
  return [...nativeComponents.map((component) => nativeFile(component, target)), "manifest.json"];
}

export function stageNativeArtifacts(source, destination, target, options = {}) {
  const files = nativeArtifactFiles(source, target, options);
  if (files.length === 1) {
    const manifest = JSON.parse(
      NodeFS.readFileSync(NodePath.join(source, "manifest.json"), "utf8"),
    );
    stageUpstreamManifest(destination, target, manifest.selection);
    return;
  }
  for (const component of nativeComponents) {
    for (const file of [nativeFile(component, "linux-x64"), nativeFile(component, "win32-x64")]) {
      if (!files.includes(file) && NodeFS.existsSync(NodePath.join(destination, file)))
        throw new Error(`Use a clean native output directory; unrelated component: ${file}`);
    }
  }
  NodeFS.mkdirSync(destination, { recursive: true });
  for (const file of files) {
    if (NodePath.resolve(source, file) !== NodePath.resolve(destination, file))
      NodeFS.copyFileSync(NodePath.resolve(source, file), NodePath.resolve(destination, file));
  }
}
