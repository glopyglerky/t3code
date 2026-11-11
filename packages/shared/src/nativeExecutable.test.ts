import { describe, expect, it } from "vite-plus/test";
import {
  isQualifiedNativeHost,
  nativeExecutable,
  selectNativeExecutable,
} from "./nativeExecutable.ts";

describe("native implementation selection", () => {
  it("uses the qualified native default only on Linux x64", () => {
    expect(selectNativeExecutable("project", undefined, "linux", "x64")).toBe(
      isQualifiedNativeHost("linux", "x64") ? nativeExecutable("project") : undefined,
    );
    for (const [platform, architecture] of [
      ["win32", "x64"],
      ["win32", "arm64"],
      ["darwin", "x64"],
      ["darwin", "arm64"],
      ["linux", "arm64"],
      ["freebsd", "x64"],
    ] as const) {
      expect(selectNativeExecutable("project", undefined, platform, architecture)).toBeUndefined();
    }
  });

  it("keeps upstream on Linux hosts outside the qualified GNU runtime", () => {
    expect(isQualifiedNativeHost("linux", "x64", "2.39")).toBe(true);
    expect(isQualifiedNativeHost("linux", "x64", "2.41")).toBe(true);
    expect(isQualifiedNativeHost("linux", "x64", "2.38")).toBe(false);
    expect(isQualifiedNativeHost("linux", "x64", "musl")).toBe(false);
    expect(isQualifiedNativeHost("linux", "arm64", "2.41")).toBe(false);
  });

  it("requires an accepted database package instead of silently selecting SQLite", () => {
    for (const [platform, architecture] of [
      ["win32", "x64"],
      ["win32", "arm64"],
      ["darwin", "x64"],
      ["darwin", "arm64"],
      ["linux", "arm64"],
    ] as const) {
      expect(() => selectNativeExecutable("db", undefined, platform, architecture)).toThrow(
        "omdb database bundle",
      );
      expect(
        selectNativeExecutable("db", undefined, platform, architecture, "upstream"),
      ).toBeUndefined();
    }
  });

  it("selects an upstream baseline explicitly and rejects unknown modes", () => {
    expect(
      selectNativeExecutable("project", undefined, "linux", "x64", "upstream"),
    ).toBeUndefined();
    expect(selectNativeExecutable("project", "/explicit/project", "linux", "x64", "upstream")).toBe(
      "/explicit/project",
    );
    expect(() => selectNativeExecutable("project", undefined, "linux", "x64", "typo")).toThrow(
      "T3_CODE_LS_IMPLEMENTATION",
    );
    expect(() => selectNativeExecutable("project", undefined, "darwin", "arm64", "native")).toThrow(
      "not qualified",
    );
  });

  it("preserves an explicit backend selection for adapter validation", () => {
    for (const override of ["/qualified/project", "", "relative-project", "bad\0path"]) {
      expect(selectNativeExecutable("project", override, "win32", "x64")).toBe(override);
      expect(selectNativeExecutable("project", override, "linux", "x64")).toBe(override);
    }
  });
});
