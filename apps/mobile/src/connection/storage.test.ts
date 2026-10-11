import { describe, expect, it } from "@effect/vitest";
import {
  BearerConnectionCredential,
  BearerConnectionProfile,
  BearerConnectionRegistration,
  BearerConnectionTarget,
} from "@t3tools/client-runtime/connection";
import {
  ConnectionCatalogDocument,
  EMPTY_CONNECTION_CATALOG_DOCUMENT,
  registerConnectionInCatalog,
} from "@t3tools/client-runtime/platform";
import { EnvironmentId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { vi } from "vite-plus/test";

vi.mock("react-native", () => ({
  Platform: { OS: "ios" },
}));

vi.mock("expo-secure-store", () => ({
  deleteItemAsync: vi.fn(),
  getItemAsync: vi.fn(),
  setItemAsync: vi.fn(),
}));

import {
  CONNECTION_CATALOG_BACKUP_KEY,
  CONNECTION_CATALOG_KEY,
  LEGACY_CONNECTIONS_KEY,
  make,
} from "./catalog-store";
import * as MobileSecureStorage from "../persistence/mobile-secure-storage";

function makeStorage(initial: Readonly<Record<string, string>>) {
  const values = new Map(Object.entries(initial));
  const deleted: Array<string> = [];
  const failures: Array<{ operation: "read" | "write" | "delete"; key: string }> = [];
  const attempts: Array<{ operation: "read" | "write" | "delete"; key: string }> = [];
  function run<A>(operation: "read" | "write" | "delete", key: string, action: () => A) {
    return Effect.suspend(() => {
      attempts.push({ operation, key });
      const index = failures.findIndex(
        (failure) => failure.operation === operation && failure.key === key,
      );
      if (index !== -1) {
        failures.splice(index, 1);
        return Effect.fail(
          new MobileSecureStorage.MobileSecureStorageError({
            operation,
            key,
            cause: new Error("injected failure"),
          }),
        );
      }
      return Effect.sync(action);
    });
  }
  const storage = MobileSecureStorage.MobileSecureStorage.of({
    getItem: (key) => run("read", key, () => values.get(key) ?? null),
    setItem: (key, value) =>
      run("write", key, () => {
        values.set(key, value);
      }),
    removeItem: (key) =>
      run("delete", key, () => {
        deleted.push(key);
        values.delete(key);
      }),
  });
  const failNext = (operation: "read" | "write" | "delete", key: string) => {
    failures.push({ operation, key });
  };
  return { deleted, storage, values, attempts, failNext };
}

describe("mobile connection catalog storage", () => {
  const encodedCatalog = Schema.encodeEffect(Schema.fromJsonString(ConnectionCatalogDocument));
  const fixtureCatalog = registerConnectionInCatalog(
    EMPTY_CONNECTION_CATALOG_DOCUMENT,
    new BearerConnectionRegistration({
      target: new BearerConnectionTarget({
        environmentId: EnvironmentId.make("saved-environment"),
        label: "Saved environment",
        connectionId: "bearer:saved-environment",
      }),
      profile: new BearerConnectionProfile({
        connectionId: "bearer:saved-environment",
        environmentId: EnvironmentId.make("saved-environment"),
        label: "Saved environment",
        httpBaseUrl: "https://saved.example.test",
        wsBaseUrl: "wss://saved.example.test",
      }),
      credential: new BearerConnectionCredential({ token: "saved-token" }),
    }),
  );

  const legacyRaw = JSON.stringify({
    connections: [
      {
        environmentId: "legacy-environment",
        environmentLabel: "Legacy",
        pairingUrl: "https://legacy.example.test/pair",
        displayUrl: "https://legacy.example.test",
        httpBaseUrl: "https://legacy.example.test",
        wsBaseUrl: "wss://legacy.example.test",
        bearerToken: "legacy-token",
        authenticationMethod: "bearer",
      },
    ],
  });

  it.effect("seeds a backup when an existing primary catalog is first read", () =>
    Effect.gen(function* () {
      const encoded = yield* encodedCatalog(fixtureCatalog);
      const memory = makeStorage({ [CONNECTION_CATALOG_KEY]: encoded });
      const catalog = yield* make().pipe(
        Effect.provideService(MobileSecureStorage.MobileSecureStorage, memory.storage),
      );

      expect(yield* catalog.read).toEqual(fixtureCatalog);
      expect(memory.values.get(CONNECTION_CATALOG_BACKUP_KEY)).toBe(encoded);
      memory.values.set(CONNECTION_CATALOG_KEY, "{not-json");
      const restarted = yield* make().pipe(
        Effect.provideService(MobileSecureStorage.MobileSecureStorage, memory.storage),
      );
      expect(yield* restarted.read).toEqual(fixtureCatalog);
      expect(memory.deleted).toEqual([]);
    }),
  );

  it.effect(
    "migrates valid legacy data when the primary is missing and the backup is malformed",
    () =>
      Effect.gen(function* () {
        const memory = makeStorage({
          [CONNECTION_CATALOG_BACKUP_KEY]: "{not-json",
          [LEGACY_CONNECTIONS_KEY]: legacyRaw,
        });
        const catalog = yield* make().pipe(
          Effect.provideService(MobileSecureStorage.MobileSecureStorage, memory.storage),
        );

        const recovered = yield* catalog.read;
        expect(recovered.targets.map((target) => target.environmentId)).toEqual([
          "legacy-environment",
        ]);
        expect(recovered.credentials[0]?.credential).toMatchObject({ token: "legacy-token" });
        const encoded = yield* encodedCatalog(recovered);
        expect(memory.values.get(CONNECTION_CATALOG_KEY)).toBe(encoded);
        expect(memory.values.get(CONNECTION_CATALOG_BACKUP_KEY)).toBe(encoded);
        expect(memory.deleted).toEqual([LEGACY_CONNECTIONS_KEY]);
      }),
  );

  it.effect("recovers a corrupt current catalog from its durable backup", () =>
    Effect.gen(function* () {
      const encoded = yield* encodedCatalog(fixtureCatalog);
      const memory = makeStorage({
        [CONNECTION_CATALOG_KEY]: "{not-json",
        [CONNECTION_CATALOG_BACKUP_KEY]: encoded,
      });
      const catalog = yield* make().pipe(
        Effect.provideService(MobileSecureStorage.MobileSecureStorage, memory.storage),
      );

      expect((yield* catalog.read).targets).toHaveLength(1);
      expect(memory.values.get(CONNECTION_CATALOG_KEY)).toBe(encoded);
      expect(memory.deleted).toEqual([]);
    }),
  );

  it.effect("fails closed without deleting a corrupt catalog when no backup exists", () =>
    Effect.gen(function* () {
      const memory = makeStorage({
        [CONNECTION_CATALOG_KEY]: "{not-json",
      });
      const catalog = yield* make().pipe(
        Effect.provideService(MobileSecureStorage.MobileSecureStorage, memory.storage),
      );

      const error = yield* Effect.flip(catalog.read);
      expect(error.detail).toContain("No valid connection catalog backup is available");
      expect(memory.values.get(CONNECTION_CATALOG_KEY)).toBe("{not-json");
      expect(memory.deleted).toEqual([]);
    }),
  );

  it.effect("replaces and removes a corrupt legacy catalog", () =>
    Effect.gen(function* () {
      const memory = makeStorage({
        [LEGACY_CONNECTIONS_KEY]: JSON.stringify({ connections: [{ invalid: true }] }),
      });
      const catalog = yield* make().pipe(
        Effect.provideService(MobileSecureStorage.MobileSecureStorage, memory.storage),
      );

      expect((yield* catalog.read).targets).toEqual([]);
      expect(memory.deleted).toEqual([LEGACY_CONNECTIONS_KEY]);
      expect(memory.values.has(CONNECTION_CATALOG_KEY)).toBe(true);
      expect(memory.values.has(CONNECTION_CATALOG_BACKUP_KEY)).toBe(true);
    }),
  );

  it.effect("restores a missing current catalog from backup before consulting legacy data", () =>
    Effect.gen(function* () {
      const encoded = yield* encodedCatalog(fixtureCatalog);
      const memory = makeStorage({
        [CONNECTION_CATALOG_BACKUP_KEY]: encoded,
        [LEGACY_CONNECTIONS_KEY]: legacyRaw,
      });
      const catalog = yield* make().pipe(
        Effect.provideService(MobileSecureStorage.MobileSecureStorage, memory.storage),
      );

      expect((yield* catalog.read).targets).toHaveLength(1);
      expect(memory.values.get(CONNECTION_CATALOG_KEY)).toBe(encoded);
      expect(memory.deleted).toEqual([]);

      yield* catalog.update((document) => document);
      expect(memory.values.has(CONNECTION_CATALOG_KEY)).toBe(true);
      expect(memory.values.has(CONNECTION_CATALOG_BACKUP_KEY)).toBe(true);
      expect(memory.values.has(LEGACY_CONNECTIONS_KEY)).toBe(true);
    }),
  );

  it.effect(
    "retries backup seeding without recovering an older backup or caching a failed load",
    () =>
      Effect.gen(function* () {
        const encoded = yield* encodedCatalog(fixtureCatalog);
        const older = yield* encodedCatalog(EMPTY_CONNECTION_CATALOG_DOCUMENT);
        const memory = makeStorage({
          [CONNECTION_CATALOG_KEY]: encoded,
          [CONNECTION_CATALOG_BACKUP_KEY]: older,
        });
        memory.failNext("write", CONNECTION_CATALOG_BACKUP_KEY);
        const catalog = yield* make().pipe(
          Effect.provideService(MobileSecureStorage.MobileSecureStorage, memory.storage),
        );

        expect((yield* Effect.flip(catalog.read)).detail).toContain("Could not save");
        expect(memory.values.get(CONNECTION_CATALOG_KEY)).toBe(encoded);
        expect(memory.values.get(CONNECTION_CATALOG_BACKUP_KEY)).toBe(older);
        expect(yield* catalog.read).toEqual(fixtureCatalog);
        expect(memory.values.get(CONNECTION_CATALOG_BACKUP_KEY)).toBe(encoded);
        const attempts = memory.attempts.length;
        expect(yield* catalog.read).toEqual(fixtureCatalog);
        expect(memory.attempts).toHaveLength(attempts);
        expect(memory.deleted).toEqual([]);
      }),
  );

  it.effect.each([undefined, "{not-json"])(
    "preserves backup and legacy data if restoring primary %s fails",
    (primary) =>
      Effect.gen(function* () {
        const encoded = yield* encodedCatalog(fixtureCatalog);
        const memory = makeStorage({
          ...(primary === undefined ? {} : { [CONNECTION_CATALOG_KEY]: primary }),
          [CONNECTION_CATALOG_BACKUP_KEY]: encoded,
          [LEGACY_CONNECTIONS_KEY]: legacyRaw,
        });
        memory.failNext("write", CONNECTION_CATALOG_KEY);
        const catalog = yield* make().pipe(
          Effect.provideService(MobileSecureStorage.MobileSecureStorage, memory.storage),
        );

        expect((yield* Effect.flip(catalog.read)).detail).toContain("Could not save");
        expect(memory.values.get(CONNECTION_CATALOG_KEY)).toBe(primary);
        expect(memory.values.get(CONNECTION_CATALOG_BACKUP_KEY)).toBe(encoded);
        expect(memory.values.get(LEGACY_CONNECTIONS_KEY)).toBe(legacyRaw);
        expect(memory.deleted).toEqual([]);
        expect(yield* catalog.read).toEqual(fixtureCatalog);
        expect(memory.values.get(CONNECTION_CATALOG_KEY)).toBe(encoded);
        expect(memory.values.get(LEGACY_CONNECTIONS_KEY)).toBe(legacyRaw);
      }),
  );

  it.effect.each([CONNECTION_CATALOG_KEY, CONNECTION_CATALOG_BACKUP_KEY])(
    "does not migrate legacy data on a storage read failure for %s",
    (key) =>
      Effect.gen(function* () {
        const encoded = yield* encodedCatalog(fixtureCatalog);
        const memory = makeStorage({
          [CONNECTION_CATALOG_BACKUP_KEY]: encoded,
          [LEGACY_CONNECTIONS_KEY]: legacyRaw,
        });
        memory.failNext("read", key);
        const catalog = yield* make().pipe(
          Effect.provideService(MobileSecureStorage.MobileSecureStorage, memory.storage),
        );

        expect((yield* Effect.flip(catalog.read)).detail).toContain("Could not load");
        expect(memory.values.has(CONNECTION_CATALOG_KEY)).toBe(false);
        expect(memory.values.get(CONNECTION_CATALOG_BACKUP_KEY)).toBe(encoded);
        expect(memory.values.get(LEGACY_CONNECTIONS_KEY)).toBe(legacyRaw);
        expect(memory.deleted).toEqual([]);
        expect(yield* catalog.read).toEqual(fixtureCatalog);
      }),
  );

  it.effect.each([undefined, " ", "{not-json", '{"connections":[{"invalid":true}]}'])(
    "preserves a malformed backup when legacy data cannot recover it: %s",
    (legacy) =>
      Effect.gen(function* () {
        const initial = {
          [CONNECTION_CATALOG_BACKUP_KEY]: "{invalid-backup",
          ...(legacy === undefined ? {} : { [LEGACY_CONNECTIONS_KEY]: legacy }),
        };
        const memory = makeStorage(initial);
        const catalog = yield* make().pipe(
          Effect.provideService(MobileSecureStorage.MobileSecureStorage, memory.storage),
        );

        yield* Effect.flip(catalog.read);
        expect(Object.fromEntries(memory.values)).toEqual(initial);
        expect(memory.deleted).toEqual([]);
        memory.values.set(LEGACY_CONNECTIONS_KEY, legacyRaw);
        expect((yield* catalog.read).targets[0]?.environmentId).toBe("legacy-environment");
      }),
  );

  it.effect("fails closed when both catalogs are corrupt without consulting legacy data", () =>
    Effect.gen(function* () {
      const initial = {
        [CONNECTION_CATALOG_KEY]: "{invalid-primary",
        [CONNECTION_CATALOG_BACKUP_KEY]: "{invalid-backup",
        [LEGACY_CONNECTIONS_KEY]: legacyRaw,
      };
      const memory = makeStorage(initial);
      const catalog = yield* make().pipe(
        Effect.provideService(MobileSecureStorage.MobileSecureStorage, memory.storage),
      );

      yield* Effect.flip(catalog.read);
      expect(Object.fromEntries(memory.values)).toEqual(initial);
      expect(memory.attempts.some((attempt) => attempt.key === LEGACY_CONNECTIONS_KEY)).toBe(false);
      expect(memory.deleted).toEqual([]);
    }),
  );

  it.effect.each([CONNECTION_CATALOG_BACKUP_KEY, CONNECTION_CATALOG_KEY])(
    "preserves the primary and cached catalog when an update cannot write %s",
    (key) =>
      Effect.gen(function* () {
        const encoded = yield* encodedCatalog(fixtureCatalog);
        const next = {
          ...fixtureCatalog,
          disabledEnvironmentIds: [EnvironmentId.make("saved-environment")],
        };
        const nextEncoded = yield* encodedCatalog(next);
        const memory = makeStorage({ [CONNECTION_CATALOG_KEY]: encoded });
        const catalog = yield* make().pipe(
          Effect.provideService(MobileSecureStorage.MobileSecureStorage, memory.storage),
        );
        yield* catalog.read;
        memory.attempts.length = 0;
        memory.failNext("write", key);

        expect((yield* Effect.flip(catalog.update(() => next))).detail).toContain("Could not save");
        expect(yield* catalog.read).toEqual(fixtureCatalog);
        expect(memory.values.get(CONNECTION_CATALOG_KEY)).toBe(encoded);
        expect(memory.values.get(CONNECTION_CATALOG_BACKUP_KEY)).toBe(
          key === CONNECTION_CATALOG_KEY ? nextEncoded : encoded,
        );
        expect(
          memory.attempts
            .filter((attempt) => attempt.operation === "write")
            .map((attempt) => attempt.key),
        ).toEqual(
          key === CONNECTION_CATALOG_KEY
            ? [CONNECTION_CATALOG_BACKUP_KEY, CONNECTION_CATALOG_KEY]
            : [CONNECTION_CATALOG_BACKUP_KEY],
        );

        const restarted = yield* make().pipe(
          Effect.provideService(MobileSecureStorage.MobileSecureStorage, memory.storage),
        );
        expect(yield* restarted.read).toEqual(fixtureCatalog);
        yield* restarted.update(() => next);
        expect(yield* restarted.read).toEqual(next);
        expect(memory.values.get(CONNECTION_CATALOG_KEY)).toBe(nextEncoded);
        expect(memory.values.get(CONNECTION_CATALOG_BACKUP_KEY)).toBe(nextEncoded);
        expect(memory.deleted).toEqual([]);
      }),
  );

  it.effect.each([
    { operation: "write", key: CONNECTION_CATALOG_BACKUP_KEY },
    { operation: "write", key: CONNECTION_CATALOG_KEY },
    { operation: "delete", key: LEGACY_CONNECTIONS_KEY },
  ] as const)(
    "keeps legacy data recoverable if migration $operation fails for $key",
    ({ operation, key }) =>
      Effect.gen(function* () {
        const memory = makeStorage({
          [CONNECTION_CATALOG_BACKUP_KEY]: "{invalid-backup",
          [LEGACY_CONNECTIONS_KEY]: legacyRaw,
        });
        memory.failNext(operation, key);
        const catalog = yield* make().pipe(
          Effect.provideService(MobileSecureStorage.MobileSecureStorage, memory.storage),
        );

        yield* Effect.flip(catalog.read);
        expect(memory.values.get(LEGACY_CONNECTIONS_KEY)).toBe(legacyRaw);
        expect(memory.deleted).toEqual([]);
        expect(memory.values.has(CONNECTION_CATALOG_KEY)).toBe(operation === "delete");
        if (key === CONNECTION_CATALOG_BACKUP_KEY) {
          expect(memory.values.get(CONNECTION_CATALOG_BACKUP_KEY)).toBe("{invalid-backup");
        }
        const recovered = yield* catalog.read;
        expect(recovered.targets[0]?.environmentId).toBe("legacy-environment");
        expect(recovered.credentials[0]?.credential).toMatchObject({ token: "legacy-token" });
        const encoded = yield* encodedCatalog(recovered);
        expect(memory.values.get(CONNECTION_CATALOG_KEY)).toBe(encoded);
        expect(memory.values.get(CONNECTION_CATALOG_BACKUP_KEY)).toBe(encoded);
      }),
  );
});
