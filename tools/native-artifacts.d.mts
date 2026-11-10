export const nativeComponents: readonly string[];
export const nativeTargets: Readonly<Record<string, string>>;
export const nativeQualifiedTargets: readonly string[];
export function nativeTargetQualified(target: string): boolean;
export function isQualifiedNativeHost(
  platform?: string,
  architecture?: string,
  glibcVersion?: string,
): boolean;
export function nativeFile(component: string, target: string): string;
export function nativeHash(file: string): string;
export function nativeArtifactFiles(source: string, target: string): string[];
export function stageNativeArtifacts(source: string, destination: string, target: string): void;
export function stageUpstreamManifest(
  destination: string,
  target: string,
  selection?: string,
): void;
