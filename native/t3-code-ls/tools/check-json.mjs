import * as assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";

const workspace = fileURLToPath(new URL("../", import.meta.url));
let executable = process.argv[2];
if (!executable) {
  const build = spawnSync("cargo", ["build", "--locked", "--quiet", "--manifest-path",
    workspace + "Cargo.toml", "-p", "t3-code-ls-core", "--example", "json_round_trip"],
    { encoding: "utf8", timeout: 120_000 });
  assert.ifError(build.error);
  assert.equal(build.status, 0, build.stderr);
  executable = workspace + "target/debug/examples/json_round_trip" + (process.platform === "win32" ? ".exe" : "");
}

const defaults = { bytes: 128 * 1024 * 1024, depth: 128, values: 1_048_576 };
const cases = [];
function scalarStrings(value) {
  if (typeof value === "string") return value.isWellFormed();
  if (value === null || typeof value !== "object") return true;
  return Object.entries(value).every(([key, item]) => key.isWellFormed() && scalarStrings(item));
}
function add(input, label, options = {}) {
  let accepted = true;
  let expected;
  try {
    expected = JSON.parse(input);
  } catch {
    accepted = false;
  }
  if (accepted && !scalarStrings(expected) && !options.reject) {
    options = { ...options, reject: /unpaired Unicode surrogate/ };
  }
  if (options.reject) {
    assert.equal(accepted, true, label + ": an explicit restriction must have valid JSON syntax");
    accepted = false;
  }
  cases.push({ input, label, accepted, expected, mode: "value", limits: defaults, ...options });
}

function numberLexemes(input) {
  const lexemes = [];
  JSON.parse(input, (_key, value, context) => {
    if (typeof value === "number") {
      assert.equal(typeof context?.source, "string", "Node must expose the original primitive token");
      lexemes.push(context.source);
    }
    return value;
  });
  return lexemes;
}

let scalarCount = 0;
for (let first = 0; first < 0x110000; first += 0x1000) {
  const points = [];
  for (let point = first; point < first + 0x1000; point++) {
    if (point < 0xd800 || point > 0xdfff) points.push(point);
  }
  scalarCount += points.length;
  const value = String.fromCodePoint(...points);
  let escaped = '"';
  for (let index = 0; index < value.length; index++) {
    escaped += "\\u" + value.charCodeAt(index).toString(16).padStart(4, "0");
  }
  escaped += '"';
  for (const [encoding, input] of [["UTF-8", JSON.stringify(value)], ["escape", escaped]]) {
    add(input, encoding + " scalar block " + first, {
      limits: { bytes: Buffer.byteLength(input), depth: 0, values: 1 },
    });
  }
}
assert.equal(scalarCount, 1_112_064);

// Rust strings require Unicode scalar values. Rejecting lone UTF-16 surrogates
// is deliberately stricter than Node's JSON parser, not a parity claim.
for (let point = 0xd800; point <= 0xdfff; point++) {
  add('"\\u' + point.toString(16) + '"', "lone surrogate " + point, {
    reject: /unpaired Unicode surrogate/,
  });
}
for (const input of ['"\\ud800\\ud800"', '"\\udc00\\ud800"', '"\\ud800\\u0041"']) {
  add(input, "unpaired surrogate sequence", { reject: /unpaired Unicode surrogate/ });
}

let grammarCases = 0;
for (const sign of ["", "-", "+", "--"]) {
  for (const integer of ["", "0", "00", "01", "1", "10", "9", "12345678901234567890", "０", "١"]) {
    for (const fraction of ["", ".", ".0", ".12", "..0", ".e0"]) {
      for (const exponent of ["", "e", "e0", "E-1", "e+9", "E+", "e-", "e--1", "e1.0", "e٠"]) {
        const token = sign + integer + fraction + exponent;
        add(token, "number grammar " + grammarCases++);
      }
    }
  }
}
for (const token of [
  "9007199254740991", "9007199254740992", "9007199254740993",
  "18446744073709551615", "18446744073709551616", "-9223372036854775808",
  "-9223372036854775809", "1e9999", "-1e9999", "1e-9999", "-0", "-0.0e+00",
  "1234567890".repeat(100), "0." + "0".repeat(1_000) + "1",
]) {
  add(token, "lexical number preservation", { mode: "number" });
  add('{"id":' + token + ',"nested":[' + token + "]}",
    "nested number preservation");
}

for (const input of [
  "null", "true", "false", "[]", "{}", '""', " \t\r\n0 \t\r\n",
  '{"probe":1,"probe":2}', '{"probe":1,"pr\\u006fbe":{"id":9007199254740993}}',
  '{"__proto__":{"polluted":true},"constructor":0,"probe":null}',
  '{"a\\/b":"slash","\\u0000":"nul","\\u2028":"separator"}',
  '"\\\\\\"\\/\\b\\f\\n\\r\\t"', '"\\uD83D\\uDE80"', '"🚀 héllo 日本語"',
  "undefined", "NaN", "Infinity", "-Infinity", "True", "NULL",
  "true false", "nullx", "[1,]", '{"a":0,}', '{"a" 0}', "{a:0}",
  "[,]", "[1 2]", '{"a":1 "b":2}', "'text'", '"\\x41"', '"\\u0x00"',
  '"\\u123"', '"\\u"', '"\\', '"unterminated', "[", "{",
  "\uFEFF0", "\u00A00", "\u20280", "0// comment", "/* comment */0",
]) {
  add(input, "literal, string, or structural syntax");
}
for (let code = 0; code < 0x20; code++) {
  add('"a' + String.fromCharCode(code) + 'b"', "raw control " + code);
  add(String.fromCharCode(code) + "0", "whitespace " + code);
}
for (const input of [
  '{"probe":1,"pr\\u006fbe":9007199254740993}', '{"probe":null}',
  '{"other":1}', "null", '["probe"]',
]) {
  add(input, "last matching object member", { mode: "get" });
}

for (const source of [
  '{"id":42,"method":"turn/start","params":{"text":"hello","stream":true}}',
  '{"unicode":"\\ud83d\\ude80","escaped":"\\n\\t\\\\\\"","number":-12.5e+10}',
  '[null,false,{"x":[0,1,2]},{},"text"]',
]) {
  for (let index = 0; index < source.length; index++) {
    add(source.slice(0, index) + source.slice(index + 1), "deleted wire byte " + index);
    add(source.slice(0, index) + "?" + source.slice(index + 1), "changed wire byte " + index);
  }
}

function shape(value) {
  if (value === null || typeof value !== "object") return { depth: 0, values: 1 };
  const children = Object.values(value).map(shape);
  return {
    depth: 1 + Math.max(0, ...children.map((child) => child.depth)),
    values: 1 + children.reduce((total, child) => total + child.values, 0),
  };
}
for (const input of [
  '"🚀"', " \r\nnull\t", "[]", "{}", "[[0]]", '{"a":[true,null,{}],"b":"x"}',
  "[" + "0,".repeat(4_999) + "0]", "[".repeat(128) + "0" + "]".repeat(128),
]) {
  const { depth, values } = shape(JSON.parse(input));
  const exact = { bytes: Buffer.byteLength(input), depth, values };
  add(input, "exact resource limits", { limits: exact });
  add(input, "one byte beyond limit", {
    limits: { ...exact, bytes: exact.bytes - 1 }, reject: /byte limit/,
  });
  add(input, "one value beyond limit", {
    limits: { ...exact, values: values - 1 }, reject: /value limit/,
  });
  if (depth > 0) add(input, "one level beyond limit", {
    limits: { ...exact, depth: depth - 1 }, reject: /nesting limit/,
  });
}
add("[".repeat(129) + "0" + "]".repeat(129), "default nesting limit", {
  reject: /nesting limit/,
});

function frame(test) {
  const body = Buffer.from(test.input, "utf8");
  const { bytes, depth, values } = test.limits;
  return Buffer.concat([
    Buffer.from([test.mode, body.length, bytes, depth, values].join("\t") + "\n"),
    body,
  ]);
}
function execute(tests) {
  const result = spawnSync(executable, [], {
    input: Buffer.concat(tests.map(frame)), encoding: "utf8",
    maxBuffer: 16 * 1024 * 1024, timeout: 30_000,
  });
  assert.ifError(result.error);
  assert.equal(result.signal, null, "JSON probe must not terminate by signal");
  return result;
}

for (let index = 0; index < cases.length; index += 64) {
  const batch = cases.slice(index, index + 64);
  const result = execute(batch);
  assert.equal(result.status, 0, result.stderr);
  const outputs = result.stdout.split("\n");
  assert.equal(outputs.pop(), "", "each JSON result must end in a newline");
  assert.equal(outputs.length, batch.length, "each input needs one result");
  for (let offset = 0; offset < batch.length; offset++) {
    const test = batch[offset];
    const line = outputs[offset];
    const context = test.label + " (case " + (index + offset) + ")";
    if (!test.accepted) {
      assert.ok(line.startsWith("error\t"), context + ": parser accepted rejected input");
      const [_, position, message] = line.split("\t");
      assert.ok(Number.isInteger(Number(position)) && Number(position) >= 0 &&
        Number(position) <= Buffer.byteLength(test.input), context + ": invalid error offset");
      if (test.reject) assert.match(message, test.reject, context);
      continue;
    }
    if (test.mode === "get" &&
        (test.expected === null || typeof test.expected !== "object" ||
         !Object.hasOwn(test.expected, "probe"))) {
      assert.equal(line, "missing", context);
      continue;
    }
    assert.ok(line.startsWith("ok\t"), context + ": parser rejected valid JSON: " + line);
    const encoded = line.slice(3);
    const expected = test.mode === "get" ? test.expected.probe : test.expected;
    assert.deepEqual(JSON.parse(encoded), expected, context + ": changed JSON meaning");
    if (test.mode !== "get") {
      assert.deepEqual(numberLexemes(encoded), numberLexemes(test.input),
        context + ": changed a numeric token or rounded an identifier");
    }
  }
}

// Constructed Number values must contain only a number token: embedded framing
// whitespace is not a numeric lexeme, even though JSON.parse accepts it.
for (const input of ["1\n", "\n1", " 1", "1 ", "1\t", "0\r\n", "01", "NaN", "1,2", "null"]) {
  const result = execute([{ input, mode: "number", limits: defaults }]);
  assert.notEqual(result.status, 0, "accepted an invalid constructed Number: " + JSON.stringify(input));
  assert.match(result.stderr, /invalid JSON number/);
  assert.equal(result.stdout, "");
}
console.log(cases.length + " JSON cases passed against Node parsing; all " + scalarCount +
  " Unicode scalar values checked in UTF-8 and escaped form, numeric lexemes and resource limits preserved. " +
  "Lone UTF-16 surrogates are explicitly rejected; constructed numbers reject framing whitespace.");
