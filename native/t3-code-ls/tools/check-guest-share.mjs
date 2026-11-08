import * as NodeAssert from "node:assert/strict";
import * as NodeChildProcess from "node:child_process";
import * as NodeModule from "node:module";
import { repoRoot, upstreamSource } from "./upstream-source.mjs";

const source = upstreamSource("apps/server/src/orchestration-v2/GuestShareDecider.ts");
const contracts = upstreamSource("packages/contracts/src/guestShare.ts");
const names = [
  "GUEST_PENDING_MESSAGE_LIMIT",
  "GUEST_RESOLVED_MESSAGE_LIMIT",
  "GUEST_PENDING_ATTACHMENT_BYTES_LIMIT",
];
const constants = names.map((name) => {
  const match = contracts.match(new RegExp(`^export const ${name} = [^;]+;`, "m"));
  NodeAssert.ok(match, `Review changed upstream constant: ${name}`);
  return match[0].replace("export ", "");
}).join("\n");
const pendingStart = contracts.indexOf("export const pendingGuestMessageCount =");
const pendingEnd = contracts.indexOf("export const GuestShareCreateInput", pendingStart);
NodeAssert.ok(pendingStart >= 0 && pendingEnd > pendingStart);

// Only the Effect boundaries are substituted: UTC milliseconds and Result.
// The complete pinned decider functions, including Date.parse, run unchanged.
const javascript = NodeModule.stripTypeScriptTypes(
  `${constants}\n${source}\n${contracts.slice(pendingStart, pendingEnd)}`,
  { mode: "strip" },
).replace(/^import\s[\s\S]*?;\s*$/gm, "").replace(/^export (?=(?:function|const) )/gm, "");
const upstream = new Function("DateTime", "Result", `${javascript}\nreturn {
  decideGuestShareCommand, decideGuestMessageSend, guestShareAfterCancelledSend,
  releasedGuestAttachmentIds, withoutGuestAttribution, pendingGuestMessageCount,
  GUEST_PENDING_MESSAGE_LIMIT, GUEST_RESOLVED_MESSAGE_LIMIT,
  GUEST_PENDING_ATTACHMENT_BYTES_LIMIT,
};`)(
  { toEpochMillis: (value) => value, formatIso: (value) => new Date(value).toISOString() },
  {
    succeed: (ok) => ({ ok }),
    fail: (error) => ({ error }),
    map: (result, transform) => "error" in result ? result : { ok: transform(result.ok) },
  },
);

const cases = [];
const copy = (value) => JSON.parse(JSON.stringify(value));
function expected(input) {
  if (input.operation === "timestamp") return { iso: new Date(input.now).toISOString() };
  if (input.operation === "strip") {
    return { command: upstream.withoutGuestAttribution(input.command) };
  }
  if (input.operation === "release") {
    return { released: upstream.releasedGuestAttachmentIds(input.before, input.after) };
  }
  let result;
  if (input.operation === "decide") {
    result = upstream.decideGuestShareCommand(input);
  } else if (input.operation === "send") {
    result = upstream.decideGuestMessageSend({
      thread: input.thread, guestMessageId: input.messageId, now: input.now,
    });
  } else if (input.operation === "cancel") {
    result = { ok: upstream.guestShareAfterCancelledSend(input.thread, input.guest, input.now) };
  } else {
    throw new Error(`Unknown operation ${input.operation}`);
  }
  if ("error" in result) return result;
  const after = input.operation === "send" ? result.ok.thread : result.ok;
  return {
    ...result,
    released: upstream.releasedGuestAttachmentIds(input.thread.guestShare ?? null, after.guestShare ?? null),
    pending: upstream.pendingGuestMessageCount(after.guestShare),
  };
}

function add(label, fixture) {
  const input = copy(fixture);
  const dates = {};
  const collectDates = (value) => {
    if (value === null || typeof value !== "object") return;
    for (const [key, child] of Object.entries(value)) {
      if (key === "expiresAt") {
        const millis = Date.parse(child);
        dates[child] = Number.isNaN(millis) ? null : millis;
      } else {
        collectDates(child);
      }
    }
  };
  collectDates(input);
  input.parsedDates = dates;
  const result = copy(expected(input));
  cases.push({ label, input, result });
  return result;
}

const now = Date.parse("2026-11-08T12:00:00.000Z");
const expiry = "2026-11-09T12:00:00.000Z";
const initial = () => ({
  id: "thread:guest", title: "Preserve the existing thread", projectId: "project:one",
  createdAt: now - 1000, updatedAt: now - 1, archivedAt: null,
  settledOverride: "settled", settledAt: now - 100,
  modelSelection: { instanceId: "codex", model: "fixture" },
  unrelated: { array: [null, false, 2, "💬"], object: { nested: "untouched" } },
});
const file = (id, bytes, extra = {}) => ({
  type: "image", id, name: `File ${id} 🖼️`, mimeType: "image/png", sizeBytes: bytes,
  source: { kind: "snap-shot", appName: "Fixture", capturedAt: expiry }, ...extra,
});
const message = (id, status = "pending", attachments) => ({
  id, status, text: `Text ${id}\n💬 e\u0301`, submittedAt: new Date(now - 500).toISOString(),
  ...(attachments === undefined ? {} : { attachments }),
});
const share = (messages = [], overrides = {}) => ({
  id: "share:one", host: "fixture.t3.test", createdAt: new Date(now - 1000).toISOString(),
  expiresAt: expiry, guestName: "Sam 🦀", messages, ...overrides,
});
const withShare = (value = share(), overrides = {}) => ({ ...initial(), guestShare: value, ...overrides });
const submit = (id, attachments, overrides = {}) => ({
  type: "thread.guest-message.submit", shareId: "share:one", guestMessageId: id,
  text: `/command ${id}\nfrom the guest\u0000`,
  ...(attachments === undefined ? {} : { attachments }), ...overrides,
});
const open = (overrides = {}) => ({
  type: "thread.guest-share.open", shareId: "share:one", host: "fixture.t3.test", expiresAt: expiry,
  ...overrides,
});
const decide = (label, thread, command, at = now) => add(label, { operation: "decide", thread, command, now: at });
const send = (label, thread, messageId, at = now) => add(label, { operation: "send", thread, messageId, now: at });

for (const start of [initial(), { ...initial(), guestShare: null }]) {
  decide("close absent share preserves optional properties and timestamp", start, { type: "thread.guest-share.close" });
  decide("dismiss without a share", start, { type: "thread.guest-message.dismiss", guestMessageId: "missing" });
  send("send without a share", start, "missing");
}
decide("archived threads cannot open a share", { ...initial(), archivedAt: now }, open());
decide("reopening replaces guest state and releases waiting attachments", withShare(share([
  message("waiting", "pending", [file("waiting-file", 1)]),
  message("sent", "sent", [file("sent-file", 1)]),
])), open({ shareId: "replacement" }));
let thread = decide("open share", initial(), open()).ok;
decide("submit before redeem", thread, submit("too-early"));
decide("redeem wrong share", thread, { type: "thread.guest-share.redeem", shareId: "old-share", guestName: "Sam" });
thread = decide("redeem once", thread, {
  type: "thread.guest-share.redeem", shareId: "share:one", guestName: "Zoë 🦀 e\u0301 العربية",
}, now + 1).ok;
decide("second device cannot redeem again", thread, {
  type: "thread.guest-share.redeem", shareId: "share:one", guestName: "Other",
}, now + 2);
thread = decide("submit attached message and unsettle thread", thread, submit("first", [file("image", 4)]), now + 3).ok;
thread = decide("submit text-only message", thread, submit("second"), now + 4).ok;
thread = decide("empty attachment list stays omitted", thread, submit("third", []), now + 5).ok;
decide("fourth waiting message rejected", thread, submit("fourth"), now + 6);
const sent = send("owner sends attributed message", thread, "first", now + 7).ok;
thread = sent.thread;
send("second device cannot send the same message", thread, "first", now + 8);
thread = decide("dismiss a waiting message", thread, { type: "thread.guest-message.dismiss", guestMessageId: "second" }, now + 9).ok;
decide("resolved messages cannot be dismissed again", thread, { type: "thread.guest-message.dismiss", guestMessageId: "second" });
thread = add("cancel a queued guest send", { operation: "cancel", thread, guest: sent.guest, now: now + 10 }).ok;
add("duplicate cancellation is unchanged", { operation: "cancel", thread, guest: sent.guest, now: now + 11 });
add("old attribution without shareId does not cancel", {
  operation: "cancel", thread: sent.thread,
  guest: { guestMessageId: "first", name: "Sam", text: "legacy" }, now,
});
add("old link cannot cancel a replacement share", {
  operation: "cancel", thread: sent.thread, guest: { ...sent.guest, shareId: "old-link" }, now,
});
thread = decide("replace link drops waiting files and earlier guest", thread, open({ shareId: "share:two" }), now + 12).ok;
decide("old link cannot submit into new share", thread, submit("stale"));
thread = decide("close current share", thread, { type: "thread.guest-share.close" }, now + 13).ok;
decide("closing twice does not stamp a new update", thread, { type: "thread.guest-share.close" }, now + 14);

for (const settledOverride of [null, "active", "settled"]) {
  for (const unsettledAt of [undefined, null, now - 200]) {
    decide("only explicitly settled threads reopen", withShare(share(), {
      settledOverride, ...(unsettledAt === undefined ? {} : { unsettledAt }),
    }), submit("settlement"));
  }
}

const expirationDates = [
  new Date(now - 1).toISOString(), new Date(now).toISOString(), new Date(now + 1).toISOString(),
  "2026-11-08T17:30:00+05:30", "2026-11-08T05:00:00-07:00", "2026-11-08",
  "2000-02-29T23:59:59.999Z", "2024-02-30T00:00:00.000Z", "not a date", "",
  "-000001-01-01T00:00:00.000Z", "+010000-01-01T00:00:00.000Z",
  "-271821-04-20T00:00:00.000Z", "+275760-09-13T00:00:00.000Z",
];
for (const expiresAt of expirationDates) {
  for (const at of [now - 1, now, now + 1]) {
    decide("date parsing stays at the date adapter boundary", withShare(share([], { expiresAt, guestName: null })), {
      type: "thread.guest-share.redeem", shareId: "share:one", guestName: "Date guest",
    }, at);
    decide("expiry rejects new submissions at the exact instant", withShare(share([], { expiresAt })), submit("expiry"), at);
    send("owner may send an already waiting message after expiry", withShare(share([message("waiting")], { expiresAt })), "waiting", at);
    decide("owner may dismiss after expiry", withShare(share([message("waiting")], { expiresAt })), {
      type: "thread.guest-message.dismiss", guestMessageId: "waiting",
    }, at);
  }
}

const byteLimit = upstream.GUEST_PENDING_ATTACHMENT_BYTES_LIMIT;
for (const count of [0, 1, 2, 3, 4]) {
  for (const pendingBytes of [0, 1, byteLimit - 1, byteLimit, byteLimit + 1, 2 ** 53]) {
    for (const incoming of [undefined, [], [file("next", 0)], [file("next", 1)], [file("next", byteLimit)]]) {
      const waiting = Array.from({ length: count }, (_, index) => message(
        `waiting:${index}`, "pending", index === 0 ? [file("existing", pendingBytes)] : [],
      ));
      const existing = [message("sent", "sent", [file("sent-files", 2 ** 53)]), ...waiting,
        message("dismissed", "dismissed", [file("discarded", 2 ** 53)])];
      decide("pending count precedes bytes; resolved files do not consume budget", withShare(share(existing)), submit("next", incoming));
    }
  }
}

for (const count of [0, 1, 19, 20, 21, 80]) {
  for (const position of [0, Math.floor(count / 2), count]) {
    const messages = Array.from({ length: count }, (_, index) => message(`old:${index}`, index % 2 ? "sent" : "dismissed"));
    messages.splice(position, 0, message("waiting", "pending", [file("waiting-file", 1)]));
    const state = withShare(share(messages));
    decide("trim old resolved messages while retaining every pending message", state, submit("new"));
    send("sending also trims resolved history", state, "waiting");
    decide("dismissing also trims resolved history", state, { type: "thread.guest-message.dismiss", guestMessageId: "waiting" });
  }
}

thread = withShare(share([message("keep-waiting")]));
for (let index = 0; index < 35; index++) {
  thread = decide("long lifecycle accepts the next message", thread, submit(`long:${index}`, [file(`file:${index}`, index + 1)])).ok;
  if (index % 3 === 0) {
    thread = decide("long lifecycle owner dismisses", thread, { type: "thread.guest-message.dismiss", guestMessageId: `long:${index}` }).ok;
  } else {
    const result = send("long lifecycle owner sends", thread, `long:${index}`).ok;
    thread = result.thread;
    if (index % 5 === 0) {
      thread = add("long lifecycle cancels a queued send", { operation: "cancel", thread, guest: result.guest, now }).ok;
    }
  }
}
send("unredeemed legacy message gets Guest attribution", withShare(share([message("legacy")], { guestName: null })), "legacy");
send("empty legacy name keeps null-only fallback semantics", withShare(share([message("legacy")], { guestName: "" })), "legacy");

for (const beforeStatus of ["pending", "sent", "dismissed"]) {
  for (const afterStatus of ["pending", "sent", "dismissed"]) {
    for (const sameShare of [false, true]) {
      for (const sameFile of [false, true]) {
        add("attachment release retains only same-share sent or waiting files", {
          operation: "release", before: share([message("before", beforeStatus, [file("a", 1), file("a", 1), file("b", 1)])]),
          after: share([message("after", afterStatus, [file(sameFile ? "a" : "other", 1)])], { id: sameShare ? "share:one" : "replacement" }),
        });
      }
    }
    const duplicate = withShare(share([
      message("duplicate", beforeStatus, [file("a", 1)]),
      message("duplicate", afterStatus, [file("b", 1)]),
    ]));
    send("first matching duplicate determines eligibility; updates affect every match", duplicate, "duplicate");
    decide("duplicate dismissal matches upstream array behavior", duplicate, { type: "thread.guest-message.dismiss", guestMessageId: "duplicate" });
    add("duplicate cancellation matches upstream array behavior", {
      operation: "cancel", thread: duplicate, guest: { guestMessageId: "duplicate", shareId: "share:one" }, now,
    });
  }
}
add("no prior share has no attachments to release", { operation: "release", before: null, after: share() });
add("closing a share releases waiting files only", {
  operation: "release", before: share([
    message("waiting", "pending", [file("waiting-file", 1)]),
    message("sent", "sent", [file("sent-file", 1)]),
  ]), after: null,
});

for (const context of [undefined, { version: 1, records: [] }, {
  version: 1, records: [{ contextId: "keep", nested: { guest: "unrelated data" } }],
  guest: { guestMessageId: "forged", name: "Guest", text: "forged" },
}, {
  version: 1, records: [], guest: { guestMessageId: "forged", name: "Guest", text: "forged", shareId: "share:one" },
}]) {
  add("client-supplied attribution is stripped without touching other context", {
    operation: "strip", command: { type: "message.dispatch", text: "Owner's message", other: [1, false], ...(context === undefined ? {} : { context }) },
  });
}

for (const date of [
  "-271821-04-20T00:00:00.000Z", "-000400-02-29T12:34:56.789Z",
  "-000001-12-31T23:59:59.999Z", "0000-01-01T00:00:00.000Z", "0000-02-29T00:00:00.000Z",
  "0001-01-01T00:00:00.000Z", "1600-02-29T00:00:00.000Z", "1700-03-01T00:00:00.000Z",
  "1900-03-01T00:00:00.000Z", "1969-12-31T23:59:59.999Z", "1970-01-01T00:00:00.000Z",
  "2000-02-29T00:00:00.000Z", "2100-03-01T00:00:00.000Z", "2400-02-29T00:00:00.000Z",
  "9999-12-31T23:59:59.999Z", "+010000-01-01T00:00:00.000Z", "+275760-09-13T00:00:00.000Z",
]) {
  for (const delta of [-1, 0, 1]) {
    const millis = Date.parse(date) + delta;
    if (Math.abs(millis) <= 8_640_000_000_000_000) add("UTC calendar boundary", { operation: "timestamp", now: millis });
  }
}
let random = 0x513af2d;
for (let index = 0; index < 1000; index++) {
  random = (Math.imul(random, 1664525) + 1013904223) >>> 0;
  const millis = Math.trunc((random / 0xffffffff * 2 - 1) * 8_640_000_000_000_000);
  add("UTC formatting across the entire JavaScript Date range", { operation: "timestamp", now: millis });
}

const executable = process.argv[2];
const result = executable
  ? NodeChildProcess.spawnSync(executable, [], {
      input: cases.map(({ input }) => JSON.stringify(input)).join("\n") + "\n",
      encoding: "utf8", maxBuffer: 32 * 1024 * 1024,
    })
  : NodeChildProcess.spawnSync("cargo", [
      "run", "--locked", "--quiet", "--manifest-path", `${repoRoot}/native/t3-code-ls/Cargo.toml`,
      "-p", "t3-code-ls-core", "--example", "guest_share_matrix",
    ], {
      input: cases.map(({ input }) => JSON.stringify(input)).join("\n") + "\n",
      encoding: "utf8", maxBuffer: 32 * 1024 * 1024,
    });
NodeAssert.ifError(result.error);
NodeAssert.equal(result.status, 0, `Guest-share oracle failed:\n${result.stderr}`);
const actual = result.stdout.replaceAll("\r\n", "\n").trimEnd().split("\n");
NodeAssert.equal(actual.length, cases.length, "Guest-share oracle emitted a different number of cases");
for (let index = 0; index < cases.length; index++) {
  NodeAssert.deepEqual(JSON.parse(actual[index]), cases[index].result,
    `Guest-share case ${index + 1}: ${cases[index].label}`);
}
const calendarCases = cases.filter(({ input }) => input.operation === "timestamp").length;
console.log(`${cases.length - calendarCases} guest-share lifecycle, limits, cleanup, and attribution cases and ${calendarCases} UTC calendar cases match the pinned upstream TypeScript. Date.parse results are supplied by the date adapter.`);
