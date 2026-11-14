import * as NodeAssert from "node:assert/strict";
import contract from "./performance-contract.json" with { type: "json" };

export const PERFORMANCE_POLICY = Object.freeze({
  minimum_pairs: contract.pairs,
  warmups: contract.warmups,
  samples: contract.samples,
  bootstrap_resamples: contract.bootstrap_resamples,
  confidence: contract.confidence,
  comparison_ratio: contract.comparison_ratio,
  seed: contract.seed,
});

export function seededRandom(seed) {
  let state = seed >>> 0;
  return () => {
    state = (Math.imul(state, 1664525) + 1013904223) >>> 0;
    return state / 0x1_0000_0000;
  };
}

export function balancedOrders(pairs, seed = PERFORMANCE_POLICY.seed) {
  NodeAssert.ok(Number.isInteger(pairs) && pairs >= 2 && pairs % 2 === 0);
  const first = Array.from({ length: pairs }, (_, index) => index < pairs / 2);
  const random = seededRandom(seed);
  for (let index = first.length - 1; index > 0; index--) {
    const selected = Math.floor(random() * (index + 1));
    [first[index], first[selected]] = [first[selected], first[index]];
  }
  return first.map((nativeFirst) =>
    nativeFirst ? ["native", "upstream"] : ["upstream", "native"],
  );
}

export function quantile(values, probability) {
  NodeAssert.ok(values.length > 0 && values.every(Number.isFinite));
  const ordered = [...values].sort((left, right) => left - right);
  const index = (ordered.length - 1) * probability;
  const lower = Math.floor(index);
  return ordered[lower] + (ordered[Math.ceil(index)] - ordered[lower]) * (index - lower);
}

export function summarize(values) {
  NodeAssert.ok(values.every((value) => Number.isFinite(value) && value > 0));
  return {
    samples: values.length,
    median_ms: quantile(values, 0.5),
    p95_ms: quantile(values, 0.95),
    max_ms: Math.max(...values),
    sequential_requests_per_second: (1000 * values.length) / values.reduce((a, b) => a + b, 0),
  };
}

/** Resample whole process pairs, never individual requests within one process. */
export function comparePairs(pairs, seed = PERFORMANCE_POLICY.seed) {
  NodeAssert.ok(pairs.length > 0);
  const logs = pairs.map(({ native_ms, upstream_ms }) => {
    NodeAssert.ok(Number.isFinite(native_ms) && native_ms > 0);
    NodeAssert.ok(Number.isFinite(upstream_ms) && upstream_ms > 0);
    return Math.log(native_ms / upstream_ms);
  });
  const mean = (values) => values.reduce((a, b) => a + b, 0) / values.length;
  const random = seededRandom(seed);
  const bootstrap = Array.from({ length: PERFORMANCE_POLICY.bootstrap_resamples }, () =>
    Math.exp(mean(logs.map(() => logs[Math.floor(random() * logs.length)]))),
  );
  const interval = [quantile(bootstrap, 0.025), quantile(bootstrap, 0.975)];
  const enoughPairs = pairs.length >= PERFORMANCE_POLICY.minimum_pairs;
  const status = !enoughPairs
    ? "inconclusive"
    : interval[0] > PERFORMANCE_POLICY.comparison_ratio
      ? "regressed"
      : interval[1] <= PERFORMANCE_POLICY.comparison_ratio
        ? "passed"
        : "inconclusive";
  return {
    status,
    ratio: Math.exp(mean(logs)),
    confidence_interval: interval,
    relative_interval_width: (interval[1] - interval[0]) / Math.exp(mean(logs)),
    precision_warning: (interval[1] - interval[0]) / Math.exp(mean(logs)) > 0.1,
    reason: !enoughPairs
      ? `Only ${pairs.length} process pairs; qualification requires at least ${PERFORMANCE_POLICY.minimum_pairs}.`
      : status === "regressed"
        ? "The whole 95% paired ratio interval is above 1.0: a measured slowdown blocks qualification."
        : status === "passed"
          ? "The whole 95% paired ratio interval is at or below 1.0 for this workload."
          : "The 95% paired ratio interval overlaps 1.0; the result cannot establish nonregression.",
  };
}
