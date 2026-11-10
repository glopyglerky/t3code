#!/usr/bin/env node
import * as NodeAssert from "node:assert/strict";
import * as NodeChildProcess from "node:child_process";
import * as NodePath from "node:path";

const executable = process.argv[2];
NodeAssert.ok(
  executable && NodePath.isAbsolute(executable),
  "Pass an absolute maintenance executable path",
);
const maximumBytes = 1_048_576;
const maximumValues = 131_072;
const frame = (value) => `${JSON.stringify(value)}\n`;
const copy = (value) => structuredClone(value);
let checks = 0;

function execute(input, args = []) {
  const result = NodeChildProcess.spawnSync(executable, args, {
    input,
    env: {},
    encoding: "utf8",
    maxBuffer: 16 * maximumBytes + 1024,
  });
  NodeAssert.ifError(result.error);
  NodeAssert.equal(result.signal, null);
  return result;
}

function responses(output) {
  NodeAssert.ok(output.endsWith("\n"), "each response has an LF delimiter");
  NodeAssert.ok(!output.includes("\r"), "responses use LF framing");
  return output
    .slice(0, -1)
    .split("\n")
    .map((line) => {
      NodeAssert.ok(Buffer.byteLength(line) + 1 <= maximumBytes, "response frame bound");
      const response = JSON.parse(line);
      NodeAssert.deepEqual(Object.keys(response).sort(), ["ok", "version"]);
      NodeAssert.equal(response.version, 1);
      NodeAssert.ok(["observe", "resolved"].includes(response.ok.kind));
      return response;
    });
}

function succeeded(input) {
  const result = execute(input);
  NodeAssert.equal(result.status, 0, result.stderr);
  NodeAssert.equal(result.stderr, "");
  const values = responses(result.stdout);
  NodeAssert.equal(values.at(-1).ok.kind, "resolved");
  NodeAssert.ok(values.slice(0, -1).every((value) => value.ok.kind === "observe"));
  checks++;
  return values;
}

function rejected(label, input, { previous = [], category, args = [] } = {}) {
  const result = execute(input, args);
  NodeAssert.equal(result.status, 2, `${label}: ${result.stderr}`);
  NodeAssert.equal(
    result.stdout,
    previous.map(frame).join(""),
    `${label}: no response to the invalid frame`,
  );
  NodeAssert.match(result.stderr, /^t3-code-ls-maintenance: [^\n]+\n$/, label);
  if (category !== undefined)
    NodeAssert.equal(result.stderr, `t3-code-ls-maintenance: ${category}\n`, label);
  checks++;
}

const request = {
  version: 1,
  operation: "resolve",
  mode: "package",
  owner: { provider: "codex", packageName: "@openai/codex" },
  context: null,
  definition: { mise: null, native: null },
  homebrew: null,
  observations: [],
};
const terminal = {
  version: 1,
  ok: { kind: "resolved", capabilities: { ...request.owner, update: null } },
};
const valid = frame(request);
NodeAssert.deepEqual(succeeded(valid), [terminal]);
NodeAssert.deepEqual(succeeded(` \t${valid.slice(0, -1)} \t\n`), [terminal]);
NodeAssert.deepEqual(succeeded(`${valid.slice(0, -1)}\r\n`), [terminal]);

for (const [label, input] of [
  ["empty input", ""],
  ["missing LF", valid.slice(0, -1)],
  ["blank line", "\n"],
  ["buffered extra delimiter", valid + "\n"],
  ["buffered second resolution", valid + valid],
  ["buffered malformed second resolution", valid + "{\n"],
  ["formatted multiline request", JSON.stringify(request, null, 2) + "\n"],
  ["trailing token in frame", valid.slice(0, -1) + " false\n"],
  ["non-object request", "[]\n"],
  ["null request", "null\n"],
  ["byte order mark", "\uFEFF" + valid],
  ["invalid UTF-8", Buffer.from([0xff, 0xfe, 0x0a])],
  ["unpaired high surrogate", valid.replace("@openai/codex", "\\ud800")],
  ["unpaired low surrogate", valid.replace("@openai/codex", "\\udc00")],
  ["surrogate followed by nonsurrogate", valid.replace("@openai/codex", "\\ud800\\u0041")],
])
  rejected(label, input);
rejected("unexpected argument", valid, { args: ["--help"] });

const unicode = { ...request, owner: { ...request.owner, packageName: "@fixture/💬" } };
NodeAssert.deepEqual(succeeded(frame(unicode).replace("💬", "\\ud83d\\udcac")), [
  { ...terminal, ok: { ...terminal.ok, capabilities: { ...unicode.owner, update: null } } },
]);

for (const key of Object.keys(request)) {
  const missing = copy(request);
  delete missing[key];
  rejected(`missing ${key}`, frame(missing));
}
for (const [key, value] of [
  ["version", 2],
  ["version", "1"],
  ["operation", "upgrade"],
  ["mode", "future"],
  ["owner", null],
  ["owner", { provider: "codex" }],
  ["owner", { provider: "", packageName: null }],
  ["owner", { provider: "code\0x", packageName: null }],
  ["owner", { provider: "codex", packageName: false }],
  ["owner", { provider: "codex", packageName: null, extra: true }],
  ["context", {}],
  ["context", []],
  ["definition", null],
  ["definition", { mise: null }],
  ["definition", { mise: false, native: null }],
  ["definition", { mise: { tool: "codex", upgrade: "true" }, native: null }],
  ["definition", { mise: { tool: "codex" }, native: null }],
  ["homebrew", { kind: "unknown", name: "codex", prefix: "/brew" }],
  ["observations", null],
  ["observations", {}],
  ["extra", true],
])
  rejected(`invalid ${key} ${JSON.stringify(value)}`, frame({ ...request, [key]: value }));

for (const [label, input] of [
  ["duplicate envelope key", valid.replace('"version":1', '"version":1,"version":1')],
  ["escaped duplicate envelope key", valid.replace('"version":1', '"version":1,"\\u0076ersion":1')],
  [
    "duplicate owner key",
    valid.replace('"provider":"codex"', '"provider":"codex","provider":"codex"'),
  ],
  ["duplicate definition key", valid.replace('"mise":null', '"mise":null,"mise":null')],
])
  rejected(label, input, { category: "duplicate object key" });

const context = {
  resolvedCommandPath: "/fixture/codex",
  realCommandPath: "/fixture/codex",
  platform: "linux",
  hostPlatform: "linux",
  commandPlatform: "linux",
  chocolateyInstall: null,
};
const pending = { ...request, context };
const wrapper = { request: { kind: "wrapper" }, result: null };
const observedWrapper = { version: 1, ok: { kind: "observe", request: wrapper.request } };
const completed = { ...pending, observations: [wrapper] };
const completedResponse = {
  ...terminal,
  ok: {
    ...terminal.ok,
    capabilities: { ...terminal.ok.capabilities, installPath: context.realCommandPath },
  },
};
NodeAssert.deepEqual(succeeded(frame(pending) + frame(completed)), [
  observedWrapper,
  completedResponse,
]);
NodeAssert.deepEqual(succeeded(frame(completed)), [completedResponse]);
rejected("EOF with unresolved observation", frame(pending), { previous: [observedWrapper] });
rejected("invalid second frame", frame(pending) + "{\n", { previous: [observedWrapper] });
rejected("repeated request without answering", frame(pending) + frame(pending), {
  previous: [observedWrapper],
});

for (const [key, value] of [
  ["resolvedCommandPath", null],
  ["realCommandPath", "bad\0path"],
  ["platform", false],
  ["hostPlatform", ""],
  ["commandPlatform", false],
  ["chocolateyInstall", {}],
  ["extra", "path"],
])
  rejected(`invalid context ${key}`, frame({ ...pending, context: { ...context, [key]: value } }));
for (const value of [false, 1, [], {}])
  rejected(
    `invalid wrapper observation result ${JSON.stringify(value)}`,
    frame({ ...completed, observations: [{ ...wrapper, result: value }] }),
  );
for (const observation of [
  { request: { kind: "wrapper" } },
  { request: { kind: "wrapper", path: "/unrequested" }, result: null },
  { request: { kind: "future" }, result: null },
  { request: { kind: "wrapper" }, result: null, extra: true },
  { request: { kind: "resolveMise" }, result: null },
])
  rejected(
    "invalid or unrequested observation",
    frame({ ...completed, observations: [observation] }),
  );
rejected("unused initial observation", frame({ ...request, observations: [wrapper] }));
rejected("surplus initial observation", frame({ ...completed, observations: [wrapper, wrapper] }));
rejected(
  "two observations appended at once",
  frame(pending) + frame({ ...completed, observations: [wrapper, wrapper] }),
  { previous: [observedWrapper] },
);
rejected(
  "duplicate observation key",
  frame(completed).replace('"result":null', '"result":null,"result":null'),
  { category: "duplicate object key" },
);
rejected(
  "duplicate observation request key",
  frame(completed).replace('"kind":"wrapper"', '"kind":"wrapper","kind":"wrapper"'),
  { category: "duplicate object key" },
);

for (const [key, value] of [
  ["mode", "mise"],
  ["owner", { provider: "claudeAgent", packageName: "@openai/codex" }],
  ["context", { ...context, realCommandPath: "/other/codex" }],
  ["definition", { mise: { tool: "codex", upgrade: true }, native: null }],
  ["homebrew", { kind: "formula", name: "codex", prefix: "/brew" }],
])
  rejected(`changed resolution ${key}`, frame(pending) + frame({ ...completed, [key]: value }), {
    previous: [observedWrapper],
  });

const install = "/fixture/.local/share/mise/installs/codex/1.0.0";
const toolPath = "/fixture/.local/share/mise/installs/codex";
const binary = `${install}/bin/codex`;
const otherInstall = `${toolPath}/0.9.0`;
const listingValue = {
  codex: [
    { install_path: otherInstall, active: false, requested_version: "0.9.0" },
    { install_path: install, active: true, requested_version: "latest" },
  ],
};
const mise = {
  ...request,
  mode: "mise",
  context: {
    ...context,
    resolvedCommandPath: `${toolPath}/latest/bin/codex`,
    realCommandPath: binary,
  },
  definition: { mise: { tool: "codex", upgrade: true }, native: null },
};
const evidence = [
  { request: { kind: "resolveMise" }, result: "/fixture/bin/mise" },
  { request: { kind: "miseWhich" }, result: binary },
  { request: { kind: "miseListing" }, result: JSON.stringify(listingValue) },
  {
    request: { kind: "miseInstallPaths", paths: [otherInstall, install] },
    result: [otherInstall, install],
  },
  { request: { kind: "miseToolPath", path: toolPath }, result: toolPath },
];
const requests = Array.from({ length: evidence.length + 1 }, (_, count) => ({
  ...mise,
  observations: evidence.slice(0, count),
}));
const exchange = succeeded(requests.map(frame).join(""));
NodeAssert.deepEqual(
  exchange.slice(0, -1),
  evidence.map(({ request }) => ({ version: 1, ok: { kind: "observe", request } })),
);
NodeAssert.equal(exchange.at(-1).ok.capabilities.update.executable, "/fixture/bin/mise");
NodeAssert.deepEqual(succeeded(frame(requests.at(-1))), [exchange.at(-1)]);

const prefix = requests.slice(0, 2).map(frame).join("");
const previous = exchange.slice(0, 2);
for (const [label, observations] of [
  ["changed prior result", [{ ...evidence[0], result: "/different/mise" }, evidence[1]]],
  ["reordered evidence", [evidence[1], evidence[0]]],
  ["removed prior evidence", [evidence[1]]],
  ["repeated prior evidence", [evidence[0], evidence[0]]],
  ["skipped required observation", [evidence[0], evidence[2]]],
])
  rejected(label, prefix + frame({ ...mise, observations }), { previous });
rejected(
  "changed prior listing text",
  requests.slice(0, 4).map(frame).join("") +
    frame({
      ...mise,
      observations: [
        evidence[0],
        evidence[1],
        { ...evidence[2], result: JSON.stringify(listingValue, null, 1) },
        evidence[3],
      ],
    }),
  { previous: exchange.slice(0, 4) },
);
rejected(
  "reordered requested paths",
  requests.slice(0, 4).map(frame).join("") +
    frame({
      ...mise,
      observations: [
        ...evidence.slice(0, 3),
        { ...evidence[3], request: { kind: "miseInstallPaths", paths: [install, otherInstall] } },
      ],
    }),
  { previous: exchange.slice(0, 4) },
);
rejected(
  "wrong path result length",
  frame({
    ...mise,
    observations: [...evidence.slice(0, 3), { ...evidence[3], result: [install] }],
  }),
);

const maximumWrapper = {
  ...completed,
  observations: [{ ...wrapper, result: "x".repeat(16 * 1024) }],
};
NodeAssert.deepEqual(succeeded(frame(maximumWrapper)), [completedResponse]);
rejected(
  "wrapper byte limit",
  frame({ ...completed, observations: [{ ...wrapper, result: "x".repeat(16 * 1024 + 1) }] }),
  { category: "input limit exceeded" },
);
NodeAssert.deepEqual(
  succeeded(frame({ ...completed, observations: [{ ...wrapper, result: "\0" }] })),
  [completedResponse],
);
const maximumListing = " ".repeat(256 * 1024 - 2) + "{}";
const listingRequest = (result) => ({
  ...mise,
  observations: [...evidence.slice(0, 2), { request: { kind: "miseListing" }, result }],
});
NodeAssert.equal(succeeded(frame(listingRequest(maximumListing)))[0].ok.capabilities.update, null);
rejected("listing byte limit", frame(listingRequest(maximumListing + " ")), {
  category: "input limit exceeded",
});
const deepListing = `[{"install_path":"${install}","metadata":${"[".repeat(129)}0${"]".repeat(129)}}]`;
rejected("listing nesting cannot become non-ownership", frame(listingRequest(deepListing)), {
  category: "input limit exceeded",
});
for (const [label, escaped] of [
  ["high", "\\ud800"],
  ["low", "\\udc00"],
]) {
  const customInstall = "/custom/tools/codex/1.0.0";
  const customBinary = `${customInstall}/bin/codex`;
  const unsupportedListing = `{"codex":[{"install_path":"${customInstall}","active":true,"requested_version":"latest","unused":"${escaped}"}]}`;
  rejected(
    `unsupported ${label} surrogate cannot erase installer ownership`,
    frame({
      ...mise,
      mode: "package",
      context: {
        ...context,
        resolvedCommandPath: "/custom/tools/codex/latest/bin/codex",
        realCommandPath: customBinary,
      },
      definition: {
        ...mise.definition,
        native: {
          args: ["update"],
          matchesCommandPath: true,
          requiresCommandPath: false,
          hasEnv: false,
        },
      },
      observations: [
        wrapper,
        evidence[0],
        { request: { kind: "miseWhich" }, result: customBinary },
        { request: { kind: "miseListing" }, result: unsupportedListing },
      ],
    }),
    { category: "invalid request" },
  );
}
rejected(
  "observation count limit",
  frame({ ...pending, observations: Array.from({ length: 4097 }, () => wrapper) }),
  { category: "input limit exceeded" },
);
rejected("request nesting limit", `${"[".repeat(33)}0${"]".repeat(33)}\n`, {
  category: "input limit exceeded",
});

function valueCount(value) {
  return value !== null && typeof value === "object"
    ? 1 + Object.values(value).reduce((total, entry) => total + valueCount(entry), 0)
    : 1;
}
const manyValues = {
  ...request,
  definition: {
    mise: null,
    native: {
      args: [],
      matchesCommandPath: false,
      requiresCommandPath: false,
      hasEnv: false,
    },
  },
};
manyValues.definition.native.args = Array.from(
  { length: maximumValues - valueCount(manyValues) },
  () => "x",
);
NodeAssert.ok(Buffer.byteLength(frame(manyValues)) < maximumBytes);
NodeAssert.deepEqual(succeeded(frame(manyValues)), [terminal]);
manyValues.definition.native.args.push("x");
rejected("request value count limit", frame(manyValues), { category: "input limit exceeded" });

const body = valid.slice(0, -1);
const atLimit = body + " ".repeat(maximumBytes - Buffer.byteLength(body) - 1) + "\n";
NodeAssert.deepEqual(succeeded(atLimit), [terminal]);
rejected("one byte beyond frame including LF", atLimit.slice(0, -1) + " \n", {
  category: "input limit exceeded",
});
const amplified = {
  ...pending,
  definition: {
    mise: null,
    native: {
      args: Array.from({ length: 9 }, () => "x".repeat(65_536)),
      matchesCommandPath: true,
      requiresCommandPath: false,
      hasEnv: false,
    },
  },
  observations: [wrapper],
};
NodeAssert.ok(Buffer.byteLength(frame(amplified)) < maximumBytes);
rejected("output command and args amplification", frame(amplified), {
  category: "output limit exceeded",
});

process.stdout.write(
  JSON.stringify({
    version: 1,
    status: "passed",
    checks,
    coverage: [
      "single resolution, ordered observation frames, and a final bounded response",
      "strict framing, UTF-8, schemas, duplicate keys, and complete transcripts",
      "base request and prior observation immutability across a process session",
      "exact frame and value limits, bounded wrapper/listing inputs, and output amplification",
    ],
    boundaries: [
      "no filesystem or provider commands are executed by the policy process",
      "input is LF-delimited JSON and permits a preceding CR as JSON whitespace",
      "buffered trailing input is rejected; future input after process exit is not validated",
      "aggregate input guard is not independently exercised beyond valid resolution lengths",
    ],
  }) + "\n",
);
