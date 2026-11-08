import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { stripTypeScriptTypes } from "node:module";
import { fileURLToPath } from "node:url";

const root = fileURLToPath(new URL("../../../", import.meta.url));
const source = readFileSync(`${root}/packages/contracts/src/auth.ts`, "utf8");

function section(start, end) {
  const from = source.indexOf(start);
  assert(from >= 0, `Upstream auth contract no longer contains ${start}`);
  const to = source.indexOf(end, from + start.length);
  assert(to > from, `Upstream auth contract no longer ends ${start} at ${end}`);
  return source.slice(from, to);
}

const constants = [...source.matchAll(/^(?:export )?const (Auth\w+Scope) = "([^"]+)" as const;/gm)];
assert.equal(constants.length, 17, "Review new upstream permission vocabulary before porting");
const declarations = constants.map((match) => `const ${match[1]} = ${JSON.stringify(match[2])};`).join("\n");
const original = [
  declarations,
  section("const legacyScopes =", "export function authScopeResponse"),
  section("const legacyParents:", "/** Keep permission denials"),
  section("export function authScopeResponse", "const authScopeResponseFields"),
  section("export function authScopeRequiredResponse", "// Shell and full-access"),
].join("\n");
const javascript = stripTypeScriptTypes(original, { mode: "strip" }).replaceAll("export function", "function");
const upstream = new Function(`${javascript}\nreturn { authScopeResponse, authScopeRequiredResponse, sessionGrantsScope, sessionHasLegacyPermissions };`)();
const all = [
  "orchestration:read", "orchestration:operate", "settings:write", "providers:manage",
  "environment:maintain", "preview:operate", "diagnostics:read", "terminal:read",
  "terminal:operate", "filesystem:read", "filesystem:write", "review:write",
  "source-control:write", "access:read", "access:write", "relay:read", "relay:write",
];
assert.deepEqual(new Set(all), new Set(constants.map((match) => match[2])));

function selection(index) {
  if (index === 0) return [];
  if (index <= all.length) return [all[index - 1]];
  if (index === all.length + 1) return all;
  return ["orchestration:read", "orchestration:operate", "terminal:operate", "orchestration:operate"];
}

const expected = [];
for (const scope of all) {
  expected.push(`scope\t${scope}\t${upstream.authScopeRequiredResponse(scope).requiredScope}`);
}
for (const scope of upstream.authScopeResponse(all).scopes) expected.push(`legacy\t${scope}`);
let decisions = 0;
for (const authenticated of [false, true]) {
  for (const knowsGranular of [false, true]) {
    for (let scopes = 0; scopes < all.length + 3; scopes++) {
      for (let permissions = 0; permissions < all.length + 4; permissions++) {
        const session = {
          authenticated,
          scopes: selection(scopes),
          ...(permissions === 0 ? {} : { permissions: selection(permissions - 1) }),
          auth: knowsGranular ? { serverUpdateScope: "environment:maintain" } : {},
        };
        const grants = all.map((permission) => upstream.sessionGrantsScope(session, permission) ? "1" : "0").join("");
        expected.push(`${grants}\t${upstream.sessionHasLegacyPermissions(session)}`);
        decisions += all.length;
      }
    }
  }
}
const executable = process.argv[2];
const result = executable
  ? spawnSync(executable, [], { encoding: "utf8", maxBuffer: 4 * 1024 * 1024 })
  : spawnSync("cargo", ["run", "--locked", "--quiet", "--manifest-path", `${root}/native/t3-code-ls/Cargo.toml`, "-p", "t3-code-ls-core", "--example", "permission_matrix"], { encoding: "utf8", maxBuffer: 4 * 1024 * 1024 });
assert.ifError(result.error);
assert.equal(result.status, 0, `Permission oracle failed:\n${result.stderr}`);
assert.equal(result.stdout.replaceAll("\r\n", "\n"), `${expected.join("\n")}\n`, "Port diverges from the checked-out upstream authorization functions");
console.log(`${decisions} permission decisions match the upstream TypeScript functions; legacy projections and permission-presence semantics match.`);
