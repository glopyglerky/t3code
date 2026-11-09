export const nativeComponents: readonly string[];
export const nativeTargets: Readonly<Record<string, string>>;
export function nativeFile(component: string, target: string): string;
export function nativeHash(file: string): string;
export function stageNativeArtifacts(source: string, destination: string, target: string): void;
