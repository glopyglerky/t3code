import * as NodeAssert from "node:assert/strict";
import * as NodeChildProcess from "node:child_process";
import * as NodeModule from "node:module";
import * as DateTime from "effect/DateTime";
import * as Result from "effect/Result";
import { upstreamSource } from "./upstream-source.mjs";

const executable = process.argv[2];
NodeAssert.ok(executable, "Pass the Lust-built guest-share executable");
const maximumBytes = 128 * 1024 * 1024;
const source = upstreamSource("apps/server/src/orchestration-v2/GuestShareDecider.ts");
const contracts = upstreamSource("packages/contracts/src/guestShare.ts");
const constants = [
  "GUEST_PENDING_MESSAGE_LIMIT",
  "GUEST_RESOLVED_MESSAGE_LIMIT",
  "GUEST_PENDING_ATTACHMENT_BYTES_LIMIT",
]
  .map((name) => {
    const declaration = contracts.match(new RegExp(`^export const ${name} = [^;]+;`, "m"));
    NodeAssert.ok(declaration, `Review changed upstream constant: ${name}`);
    return declaration[0];
  })
  .join("\n");
const javascript = NodeModule.stripTypeScriptTypes(`${constants}\n${source}`, { mode: "strip" })
  .replace(/^import\s[\s\S]*?;\s*$/gm, "")
  .replace(/^export (?=(?:function|const) )/gm, "");
const upstream = new Function(
  "DateTime",
  "Result",
  `${javascript}\nreturn {
  decideGuestShareCommand, decideGuestMessageSend, guestShareAfterCancelledSend,
  releasedGuestAttachmentIds, withoutGuestAttribution,
};`,
)(DateTime, Result);

const copy = (value) => JSON.parse(JSON.stringify(value));
let checks = 0;
let comparisons = 0;
function execute(input, args = []) {
  return NodeChildProcess.spawnSync(executable, args, {
    input,
    encoding: "utf8",
    env: {},
    maxBuffer: maximumBytes + 1024,
  });
}
function succeeded(input) {
  const result = execute(input);
  NodeAssert.ifError(result.error);
  NodeAssert.equal(result.status, 0, result.stderr);
  NodeAssert.equal(result.stderr, "");
  NodeAssert.ok(result.stdout.endsWith("\n"));
  NodeAssert.equal(result.stdout.indexOf("\n"), result.stdout.length - 1);
  NodeAssert.ok(Buffer.byteLength(result.stdout) <= maximumBytes);
  checks++;
  return JSON.parse(result.stdout);
}
function rejected(label, input, category = "invalid request", args = []) {
  const result = execute(input, args);
  NodeAssert.ifError(result.error);
  NodeAssert.equal(result.status, 2, label);
  NodeAssert.equal(result.stdout, "", `${label}: no partial response may escape`);
  NodeAssert.equal(result.stderr, `t3-code-ls-guest-share: ${category}\n`, label);
  checks++;
}
function request(operation, fields = {}) {
  const value = { version: 1, operation, ...copy(fields) };
  if (operation !== "strip") {
    const dates = [
      value.thread?.guestShare?.expiresAt,
      value.before?.expiresAt,
      value.after?.expiresAt,
      value.command?.type === "thread.guest-share.open" ? value.command.expiresAt : undefined,
    ];
    value.parsedDates = Object.fromEntries(
      dates
        .filter((value) => value !== undefined)
        .map((value) => {
          const parsed = Date.parse(value);
          return [value, Number.isNaN(parsed) ? null : parsed];
        }),
    );
  }
  return value;
}
function wireTimes(value) {
  if (DateTime.isDateTime(value)) return DateTime.toEpochMillis(value);
  if (Array.isArray(value)) return value.map(wireTimes);
  if (value !== null && typeof value === "object") {
    return Object.fromEntries(Object.entries(value).map(([key, value]) => [key, wireTimes(value)]));
  }
  return value;
}
function expected(fixture) {
  const input = copy(fixture);
  if (input.now !== undefined) input.now = DateTime.makeUnsafe(input.now);
  for (const key of ["updatedAt", "archivedAt", "settledAt", "unsettledAt"]) {
    if (typeof input.thread?.[key] === "number")
      input.thread[key] = DateTime.makeUnsafe(input.thread[key]);
  }
  if (input.operation === "strip") {
    const before = "context" in input ? { context: input.context } : {};
    const after = upstream.withoutGuestAttribution(before);
    return { version: 1, ok: after, changed: after !== before };
  }
  if (input.operation === "release")
    return {
      version: 1,
      ok: { attachmentIds: upstream.releasedGuestAttachmentIds(input.before, input.after) },
    };
  const result =
    input.operation === "decide"
      ? upstream.decideGuestShareCommand(input)
      : input.operation === "send"
        ? upstream.decideGuestMessageSend({ ...input, guestMessageId: input.messageId })
        : Result.succeed(
            upstream.guestShareAfterCancelledSend(input.thread, input.guest, input.now),
          );
  NodeAssert.deepEqual(wireTimes(input), fixture, "Upstream mutated the supplied records");
  if (Result.isFailure(result)) return { version: 1, error: result.failure };
  const thread = input.operation === "send" ? result.success.thread : result.success;
  return { version: 1, ok: wireTimes(result.success), changed: thread !== input.thread };
}
function compare(label, input) {
  const actual = succeeded(JSON.stringify(input) + "\n");
  NodeAssert.deepEqual(actual, expected(input), label);
  comparisons++;
  return actual;
}

const now = Date.parse("2026-11-08T12:00:00.123Z");
const expiresAt = "2026-11-09T12:00:00.123Z";
const initial = () => ({
  id: "thread:guest",
  providerInstanceId: "fixture-provider",
  updatedAt: now - 1,
  archivedAt: null,
  settledOverride: "settled",
  settledAt: now - 2,
  untouched: {
    expiresAt: "provider content",
    now: 17,
    nested: [null, false, 1.25, "💬 é العربية"],
  },
});
const open = {
  type: "thread.guest-share.open",
  shareId: "share:one",
  host: "fixture.test",
  expiresAt,
};
const redeem = { type: "thread.guest-share.redeem", shareId: "share:one", guestName: "Sam 🦀" };
const file = (id, sizeBytes) => ({
  id,
  sizeBytes,
  type: "image",
  mimeType: "image/png",
  name: id,
  extension: { expiresAt: "unparsed", guest: ["opaque"] },
});
const submit = (id, attachments) => ({
  type: "thread.guest-message.submit",
  shareId: "share:one",
  guestMessageId: id,
  text: `/do-not-run ${id}\n💬`,
  ...(attachments === undefined ? {} : { attachments }),
});
const decide = (label, thread, command, at = now) =>
  compare(label, request("decide", { thread, command, now: at }));

let thread = initial();
for (const at of [-8_640_000_000_000_000, -1, 0, 8_640_000_000_000_000]) {
  decide("DateTime wire fields preserve the full UTC range", thread, open, at);
}
decide("closing absent share preserves identity", thread, { type: "thread.guest-share.close" });
decide("archived share cannot open", { ...thread, archivedAt: now }, open);
thread = decide("open share", thread, open).ok;
const identicalOpen = decide("opening equal data still changes identity", thread, open);
NodeAssert.deepEqual(identicalOpen.ok, thread);
NodeAssert.equal(identicalOpen.changed, true);
decide("unredeemed share rejects submission", thread, submit("early"));
thread = decide("redeem share", thread, redeem).ok;
decide("second redemption fails with exact public reason", thread, redeem);
thread.guestShare.extension = { list: [null, "retained", { expiresAt: "opaque" }] };
thread = decide(
  "attachment budget and settled reopening",
  thread,
  submit("first", [file("large", 20 * 1024 * 1024)]),
).ok;
thread.guestShare.messages[0].extension = { title: "retain original message" };
decide(
  "one extra byte exceeds pending attachment limit",
  thread,
  submit("overflow", [file("extra", 1)]),
);
thread = decide("text submission is allowed at attachment limit", thread, submit("second")).ok;
thread = decide("empty attachments stay absent", thread, submit("third", [])).ok;
decide("fourth pending message exceeds count limit", thread, submit("fourth"));
const sent = compare(
  "send preserves attribution, metadata, and files",
  request("send", { thread, messageId: "first", now }),
).ok;
compare(
  "sent file remains owned by the thread",
  request("release", { before: thread.guestShare, after: sent.thread.guestShare }),
);
thread = sent.thread;
compare("a second send is refused", request("send", { thread, messageId: "first", now }));
compare(
  "legacy attribution does not cancel a current share",
  request("cancel", { thread, guest: { guestMessageId: "first" }, now }),
);
compare(
  "stale attribution does not affect a replacement share",
  request("cancel", { thread, guest: { ...sent.guest, shareId: "old" }, now }),
);
thread = compare(
  "cancel queued guest send",
  request("cancel", { thread, guest: sent.guest, now }),
).ok;
compare(
  "repeated cancellation preserves identity",
  request("cancel", { thread, guest: sent.guest, now }),
);
thread = decide("dismiss waiting message", thread, {
  type: "thread.guest-message.dismiss",
  guestMessageId: "second",
}).ok;
thread = decide(
  "released count permits another pending message",
  thread,
  submit("fourth", [file("pending", 1)]),
).ok;
const replacement = decide("replacement drops only the old share", thread, {
  ...open,
  shareId: "share:two",
}).ok;
compare(
  "replacement releases only pending files",
  request("release", { before: thread.guestShare, after: replacement.guestShare }),
);
compare(
  "close releases waiting files",
  request("release", { before: thread.guestShare, after: null }),
);
const closed = decide("close current share", thread, { type: "thread.guest-share.close" }).ok;
decide(
  "closing twice preserves timestamp and identity",
  closed,
  { type: "thread.guest-share.close" },
  now + 1,
);

for (const expiration of [
  expiresAt,
  new Date(now).toISOString(),
  "2026-11-08T17:30:00.123+05:30",
  "not a date",
  "__proto__",
]) {
  const state = {
    ...thread,
    guestShare: { ...thread.guestShare, expiresAt: expiration, guestName: null },
  };
  decide("expiration preserves host Date.parse semantics", state, redeem);
}
const duplicate = copy(thread);
duplicate.guestShare.messages = [
  {
    ...duplicate.guestShare.messages[0],
    id: "duplicate",
    status: "pending",
    extension: { source: 1 },
  },
  {
    ...duplicate.guestShare.messages[0],
    id: "duplicate",
    status: "pending",
    extension: { source: 2 },
  },
];
compare(
  "duplicate IDs keep separate retained payloads",
  request("send", { thread: duplicate, messageId: "duplicate", now }),
);
const releaseDuplicate = copy(duplicate.guestShare);
releaseDuplicate.messages[0].attachments = [file("a", 1), file("a", 1), file("b", 1)];
releaseDuplicate.messages[1].attachments = [];
compare(
  "attachment release preserves duplicates and order",
  request("release", { before: releaseDuplicate, after: null }),
);
for (const context of [
  undefined,
  {},
  { version: 1, records: [] },
  { guest: null, nested: { guest: "retain" } },
  {
    version: 1,
    guest: sent.guest,
    records: [{ guest: "unrelated", extras: [1, null, "💬"] }],
  },
])
  compare(
    "strip retains context presence and nested data",
    request("strip", context === undefined ? {} : { context }),
  );

const empty = request("strip");
const valid = JSON.stringify(empty);
for (const [label, input] of [
  ["empty input", ""],
  ["missing LF", valid],
  ["multiple lines", valid + "\n\n"],
  ["CRLF", valid + "\r\n"],
  ["second request", `${valid}\n${valid}\n`],
  ["trailing data", valid + "x\n"],
  ["non-object", "[]\n"],
  ["invalid UTF-8", Buffer.from([0xff, 10])],
])
  rejected(label, input);
for (const fields of [
  { version: 2 },
  { version: "1" },
  { operation: "timestamp" },
  { now: 0 },
  { context: null },
  { context: [] },
  { extra: "PRIVATE TEXT MUST NOT ENTER STDERR" },
]) {
  rejected("invalid envelope or scope", JSON.stringify({ ...empty, ...fields }) + "\n");
}
rejected("command arguments", valid + "\n", "invalid request", ["unexpected"]);
rejected("duplicate version", valid.slice(0, -1) + ',"version":1}\n', "duplicate object key");
rejected(
  "escaped duplicate version",
  valid.slice(0, -1) + ',"ver\\u0073ion":1}\n',
  "duplicate object key",
);
rejected(
  "nested duplicate context",
  valid.slice(0, -1) + ',"context":{"a":1,"a":2}}\n',
  "duplicate object key",
);
for (const value of ["\ud800", "\udfff"])
  rejected("unpaired surrogate", JSON.stringify(request("strip", { context: { value } })) + "\n");
const dated = request("send", { thread, messageId: "third", now });
for (const fields of [
  { parsedDates: {} },
  { parsedDates: { [expiresAt]: null, extra: 0 } },
  { parsedDates: { [expiresAt]: 8_640_000_000_000_001 } },
  { now: null },
  { now: 1.5 },
]) {
  rejected("invalid date boundary", JSON.stringify({ ...dated, ...fields }) + "\n");
}
const lateInvalid = copy(dated);
lateInvalid.thread.guestShare.messages.push({ id: "late-invalid" });
rejected("late invalid record exposes no partial state", JSON.stringify(lateInvalid) + "\n");
let deep = "leaf";
for (let depth = 0; depth < 128; depth++) deep = [deep];
rejected("depth limit", JSON.stringify(request("strip", { context: { deep } })) + "\n");
rejected(
  "value limit",
  JSON.stringify(request("strip", { context: { values: Array(1_048_576).fill(null) } })) + "\n",
);
{
  const frame = valid + " ".repeat(maximumBytes - Buffer.byteLength(valid) - 1) + "\n";
  NodeAssert.deepEqual(succeeded(frame), { version: 1, ok: {}, changed: false });
}
rejected("input byte limit", Buffer.alloc(maximumBytes + 1, 0x20), "input limit exceeded");
{
  const expanded = copy(dated);
  expanded.messageId = "large";
  expanded.thread.guestShare.messages = [
    {
      id: "large",
      text: '\u0000🚀"\\\n'.repeat(3 * 1024 * 1024),
      status: "pending",
      submittedAt: expiresAt,
    },
  ];
  rejected(
    "attributed text and retained message exceed output limit",
    JSON.stringify(expanded) + "\n",
    "output limit exceeded",
  );
}

console.log(
  `${checks} guest-share executable checks passed, including ${comparisons} complete comparisons with immutable upstream functions and actual Effect, retained payloads, explicit identity, strict framing, duplicate keys, Unicode, and input/output bounds. Failures emitted no partial stdout.`,
);
