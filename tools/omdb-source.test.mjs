import * as Assert from "node:assert/strict";
import * as FS from "node:fs";
import * as Test from "node:test";

const root = new URL("../", import.meta.url);
const pin = JSON.parse(FS.readFileSync(new URL("database.lock.json", root), "utf8"));
const manifest = FS.readFileSync(new URL("crates/storage/Cargo.toml", root), "utf8");
const cargoLock = FS.readFileSync(new URL("Cargo.lock", root), "utf8");

Test.test("native storage resolves the immutable omdb fork, never the crates.io redb", () => {
  const revision = pin.sourceCommit;
  const repository = "https://github.com/glopyglerky/omdb";

  Assert.equal(pin.product, "omdb");
  Assert.match(revision, /^[a-f0-9]{40}$/);
  Assert.equal(pin.sourceUrl, `${repository}/tree/${revision}`);
  Assert.equal(pin.crate.name, "redb");
  Assert.equal(pin.crate.version, pin.version);
  Assert.equal(pin.crate.git, repository);
  Assert.equal(pin.crate.rev, revision);

  const dependency = manifest.match(/^redb\s*=\s*\{([^\n]+)\}/m);
  Assert.ok(dependency, "the native storage package must declare its omdb source");
  Assert.equal(dependency[1].match(/version\s*=\s*"=([^"]+)"/)?.[1], pin.version);
  Assert.ok(dependency[1].includes(`git = "${repository}"`));
  Assert.ok(dependency[1].includes(`rev = "${revision}"`));

  const entries = [...cargoLock.matchAll(/^\[\[package\]\]\nname = "redb"\nversion = "([^"]+)"\nsource = "([^"]+)"/gm)];
  Assert.equal(entries.length, 1, "one and only one redb engine must be linked");
  Assert.equal(entries[0][1], pin.version);
  Assert.equal(entries[0][2], `git+${repository}?rev=${revision}#${revision}`);
});
