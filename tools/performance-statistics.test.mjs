import * as NodeAssert from "node:assert/strict";
import * as NodeTest from "node:test";
import { balancedOrders, comparePairs, summarize } from "./performance-statistics.mjs";

const pairs = (ratios) => ratios.map((ratio) => ({ upstream_ms: 10, native_ms: 10 * ratio }));

NodeTest.test(
  "balanced order is deterministic and gives each backend equal first positions",
  () => {
    const orders = balancedOrders(8);
    NodeAssert.deepEqual(orders, balancedOrders(8));
    NodeAssert.equal(orders.filter(([first]) => first === "native").length, 4);
    NodeAssert.throws(() => balancedOrders(7));
  },
);

NodeTest.test("a clear 1% slowdown cannot use a hidden 10% allowance", () => {
  NodeAssert.equal(comparePairs(pairs(Array(8).fill(1.01))).status, "regressed");
});

NodeTest.test("clear speedups pass and overlapping evidence stays inconclusive", () => {
  NodeAssert.equal(comparePairs(pairs(Array(8).fill(0.8))).status, "passed");
  NodeAssert.equal(
    comparePairs(pairs([0.8, 1.2, 0.9, 1.1, 0.8, 1.2, 0.9, 1.1])).status,
    "inconclusive",
  );
  NodeAssert.equal(comparePairs(pairs([0.5, 0.5])).status, "inconclusive");
});

NodeTest.test("all slow samples remain in tails and paired confidence is repeatable", () => {
  const sample = pairs([0.8, 0.8, 0.8, 0.8, 0.8, 0.8, 0.8, 10]);
  NodeAssert.deepEqual(comparePairs(sample), comparePairs(sample));
  NodeAssert.equal(summarize([1, 1, 1, 100]).max_ms, 100);
  NodeAssert.ok(summarize([1, 1, 1, 100]).p95_ms > 80);
  NodeAssert.throws(() => comparePairs([{ upstream_ms: 0, native_ms: 1 }]));
});
