#!/usr/bin/env node
import * as Assert from "node:assert/strict";
import * as NodePath from "node:path";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Result from "effect/Result";
import {
  decideGuestShareCommand,
  decideGuestMessageSend,
  guestShareAfterCancelledSend,
} from "../../../apps/server/src/orchestration-v2/GuestShareDecider.ts";
import { makeNativeGuestShareDecider } from "../../../apps/server/src/orchestration-v2/NativeGuestShareDecider.ts";

const binary = process.argv[2];
Assert.ok(binary && NodePath.isAbsolute(binary), "Expected an absolute Lust or Cargo guest executable");
const native = makeNativeGuestShareDecider(binary);
const time = (millis) => DateTime.unsafeMake(millis);
const start = time(1_000);
const thread = {
  id: "guest:test", title: "Retain untouched state",
  projectId: "project:test", createdAt: start, updatedAt: start,
  archivedAt: null, settledAt: start, settledOverride: "settled",
  unsettledAt: null, guestShare: null,
  extensions: { nested: [{ preserved: true }], count: 7 },
};
const expiry = time(100_000);
const commands = {
  open: {
    type: "thread.guest-share.open", shareId: "share:test", host: "host.test",
    expiresAt: DateTime.formatIso(expiry),
  },
  redeem: { type: "thread.guest-share.redeem", shareId: "share:test", guestName: "Zoë 🦀" },
  submit: {
    type: "thread.guest-message.submit", shareId: "share:test",
    guestMessageId: "guest:1", text: "/pretend owner\\nThis came from a guest",
    attachments: [{ id: "file:1", sizeBytes: 200, type: "image", metadata: { x: 1 } }],
  },
  dismiss: { type: "thread.guest-message.dismiss", guestMessageId: "guest:1" },
  close: { type: "thread.guest-share.close" },
};
async function decide(state, command, now) {
  const input = { thread: state, command, now };
  const wanted = decideGuestShareCommand(input);
  const actual = await Effect.runPromise(native.decideGuestShareCommand(input));
  Assert.deepStrictEqual(actual, wanted, `Lust guest decision: ${command.type}`);
  return Result.isSuccess(actual) ? actual.success : state;
}
const empty = await decide(thread, commands.close, time(1_100));
Assert.strictEqual(empty, thread, "Closing an absent guest share must retain object identity");
const opened = await decide(thread, commands.open, time(2_000));
const redeemed = await decide(opened, commands.redeem, time(3_000));
await decide(redeemed, commands.redeem, time(4_000));
const submitted = await decide(redeemed, commands.submit, time(5_000));
Assert.deepStrictEqual(submitted.extensions, thread.extensions, "Nested thread state must survive the native boundary");
Assert.equal(submitted.settledOverride, null, "Guest submission reopens a settled thread");
Assert.deepStrictEqual(submitted.guestShare.messages[0].attachments[0].metadata, { x: 1 });
const message = { thread: submitted, guestMessageId: "guest:1", now: time(6_000) };
const wantedSend = decideGuestMessageSend(message);
const nativeSend = await Effect.runPromise(native.decideGuestMessageSend(message));
Assert.deepStrictEqual(nativeSend, wantedSend, "Guest send content and attribution must match upstream");
Assert.equal(nativeSend.success.guest.name, "Zoë 🦀");
const sentThread = nativeSend.success.thread;
const cancelledAt = time(7_000);
const expectedCancelled = guestShareAfterCancelledSend(sentThread, nativeSend.success.guest, cancelledAt);
const actualCancelled = await Effect.runPromise(native.afterCancelledSend(sentThread, nativeSend.success.guest, cancelledAt));
Assert.deepStrictEqual(actualCancelled, expectedCancelled, "Cancel must dismiss sent guest messages");
const stale = await Effect.runPromise(native.afterCancelledSend(
  actualCancelled, { guestMessageId: "guest:1", shareId: "stale" }, time(8_000),
));
Assert.strictEqual(stale, actualCancelled, "Stale cancellation must retain object identity");
const dismissed = await decide(submitted, commands.dismiss, time(9_000));
Assert.equal(dismissed.guestShare.messages[0].status, "dismissed");
await decide(submitted, commands.close, time(10_000));
await decide(opened, commands.submit, time(11_000));
console.log("Guest-share Lust subprocess adapter matches the live Effect DateTime contract for open, close, redeem, submit, send, dismiss, cancellation, stale state, and nested data.");
