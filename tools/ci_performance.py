"""Validate paired workload evidence before it can qualify a source revision."""

from __future__ import annotations

import hashlib
import json
import math
from pathlib import Path
import re


SHA = re.compile(r"[0-9a-f]{40}\Z")
DIGEST = re.compile(r"[0-9a-f]{64}\Z")
RUN_ID = re.compile(r"[1-9][0-9]{0,19}\Z")
MAX_REPORT_BYTES = 32 * 1024 * 1024
CONTRACT = json.loads(Path(__file__).with_name("performance-contract.json").read_text())
IDENTITY_FIELDS = ("source_commit", "source_tree", "policy_digest", "upstream_commit",
                   "upstream_tree", "ci_run_id", "ci_run_attempt", "native_build_run_attempt",
                   "executable_sha256", "native_manifest_sha256",
                   "toolchain_lock_sha256")


def require(condition: bool, message: str):
    if not condition:
        raise ValueError(f"Performance evidence rejected: {message}")


def positive(value: object) -> bool:
    return type(value) in (float, int) and 0 < value <= 1.7976931348623157e308



def random_values(seed: int):
    state = seed & 0xffffffff
    while True:
        state = (state * 1664525 + 1013904223) & 0xffffffff
        yield state / 2**32


def expected_orders(pairs: int, seed: int) -> list:
    first = [index < pairs // 2 for index in range(pairs)]
    random = random_values(seed)
    for index in range(pairs - 1, 0, -1):
        selected = int(next(random) * (index + 1))
        first[index], first[selected] = first[selected], first[index]
    return [["native", "upstream"] if value else ["upstream", "native"] for value in first]


def quantile(values: list, probability: float) -> float:
    ordered = sorted(values)
    index = (len(ordered) - 1) * probability
    lower, upper = math.floor(index), math.ceil(index)
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (index - lower)


def paired_comparison(ratios: list, seed: int) -> tuple:
    logs = [math.log(value) for value in ratios]
    random = random_values(seed)
    bootstrap = [math.exp(sum(logs[int(next(random) * len(logs))] for _ in logs) / len(logs))
                 for _ in range(CONTRACT["bootstrap_resamples"])]
    tail = (1 - CONTRACT["confidence"]) / 2
    return math.exp(sum(logs) / len(logs)), [quantile(bootstrap, tail), quantile(bootstrap, 1 - tail)]



def canonical(value: object) -> str:
    """Match the worker's sorted JSON, including ECMAScript number spelling."""
    if isinstance(value, dict):
        indexes = sorted((key for key in value if key.isascii() and key.isdecimal()
                          and str(int(key)) == key and int(key) < 2**32 - 1), key=int)
        others = sorted(set(value) - set(indexes), key=lambda key: key.encode("utf-16-be", "surrogatepass"))
        return "{" + ",".join(canonical(key) + ":" + canonical(value[key]) for key in indexes + others) + "}"
    if isinstance(value, list):
        return "[" + ",".join(canonical(item) for item in value) + "]"
    if type(value) is float:
        require(math.isfinite(value), "non-finite validation value")
        if value == 0:
            return "0"
        text = repr(value)
        if "e" in text:
            mantissa, exponent = text.split("e")
            exponent = int(exponent)
            if -6 <= exponent < 21:
                sign = "-" if mantissa.startswith("-") else ""
                digits = mantissa.removeprefix("-").replace(".", "")
                point = exponent + 1
                return sign + ("0." + "0" * -point + digits if point <= 0 else
                               digits[:point] + "." + digits[point:] if point < len(digits) else
                               digits + "0" * (point - len(digits)))
            return mantissa.removesuffix(".0") + "e" + ("+" if exponent >= 0 else "-") + str(abs(exponent))
        return text.removesuffix(".0")
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), allow_nan=False)


def validation_cardinality(initial: dict) -> dict:
    shell, window = initial.get("sidebar-shell"), initial.get("history-window")
    require(isinstance(shell, dict) and isinstance(shell.get("threads"), list)
            and len(shell["threads"]) == CONTRACT["fixture"]["threads"] * 7 // 8
            and shell.get("archivedThreads") == [], "validation shell has the wrong active thread set")
    history = window.get("projection") if isinstance(window, dict) else None
    require(isinstance(history, dict) and isinstance(history.get("thread"), dict)
            and history["thread"].get("id") == "thread:performance:long"
            and isinstance(history.get("turnItems"), list) and 0 < len(history["turnItems"]) <= 75
            and all(isinstance(history.get(key), list) and bool(history[key]) for key in ("runs", "nodes")),
            "validation history must retain the requested thread and bounded nonempty work")
    history_bytes = len(canonical(history).encode("utf-8"))
    require(history_bytes <= 1_100_000, "validation history exceeds its payload bound")
    for operation in ("message-search", "accent-search"):
        value = initial.get(operation)
        require(isinstance(value, dict) and isinstance(value.get("matches"), list)
                and 0 < len(value["matches"]) <= 20, "validation search has no bounded matches")
    visit = initial.get("visit-commit")
    require(isinstance(visit, dict) and visit.get("id") == "thread:performance:write",
            "validation committed visit did not return the seeded write thread")
    return {"active_threads": len(shell["threads"]), "history_items": len(history["turnItems"]),
            "history_runs": len(history["runs"]), "history_nodes": len(history["nodes"]),
            "history_json_bytes": history_bytes,
            "message_matches": len(initial["message-search"]["matches"]),
            "accent_matches": len(initial["accent-search"]["matches"])}


def validate_runs(report: dict, orders: list) -> dict:
    operations = [row["id"] for row in CONTRACT["scenarios"] if row["metric"] == "median"]
    expected = [("validation", "upstream", None, None), ("validation", "native", None, None)]
    expected += [("measurement", backend, pair, order)
                 for pair, order in enumerate(orders) for backend in order]
    runs = report.get("runs")
    require(isinstance(runs, list) and len(runs) == len(expected),
            "both validation runs and every ordered measurement run must be retained")
    digests = report["correctness"].get("result_digests")
    require(isinstance(digests, dict) and set(digests) == set(operations)
            and all(isinstance(value, str) and DIGEST.fullmatch(value) for value in digests.values()),
            "all validated operation results need digests")
    measurements = {}
    for run, (purpose, backend, pair, order) in zip(runs, expected):
        require(isinstance(run, dict)
                and (run.get("purpose"), run.get("backend"), run.get("pair"), run.get("order"))
                == (purpose, backend, pair, order), "missing, repeated or reordered worker run")
        require((pair is None and run.get("pair") is None) or type(run.get("pair")) is int,
                "worker pair IDs must be integers")
        result = run.get("result")
        require(isinstance(result, dict), "worker result is missing")
        if purpose == "validation":
            require(isinstance(result.get("initial"), dict) and set(result["initial"]) == set(operations)
                    and result.get("rollback") == "passed" and isinstance(result.get("committed"), dict)
                    and bool(result["committed"]) and canonical(result["committed"]) == canonical(result.get("reopened")),
                    "validation is missing initial results, rollback or committed reopen")
            counts = validation_cardinality(result["initial"])
            require(canonical(counts) == canonical(result.get("cardinality")),
                    "validation cardinalities contradict the retained result values")
            require(all(hashlib.sha256(canonical(value).encode("utf-8")).hexdigest() == digests[name]
                        for name, value in result["initial"].items()),
                    "validation digests contradict the retained result values")
            continue
        samples, cold = result.get("samples"), result.get("cold_first_operations_ms")
        require(isinstance(samples, dict) and set(samples) == set(operations)
                and all(isinstance(values, list) and len(values) == CONTRACT["samples"]
                        and all(positive(value) for value in values) for values in samples.values()),
                "every worker must retain the exact request count for every warm operation")
        require(isinstance(cold, dict) and set(cold) == set(operations)
                and all(positive(value) for value in cold.values())
                and positive(result.get("cold_open_ms")) and positive(result.get("service_open_ms"))
                and math.isclose(result["cold_open_ms"], result["service_open_ms"] + sum(cold.values()), rel_tol=1e-12),
                "cold service timing must include the recorded first operation cycle")
        require(result.get("digests") == digests
                and result.get("committed_writes") == 1 + CONTRACT["warmups"] + CONTRACT["samples"],
                "worker lost a validated result or a committed write")
        measurements[(pair, backend)] = result
    require(canonical(runs[0]["result"]) == canonical(runs[1]["result"]),
            "stock and native validation results differ")
    return measurements


def validate_baseline(report: dict):
    baseline = report.get("baseline")
    require(isinstance(baseline, dict)
            and baseline.get("commit") == report["upstream_commit"]
            and baseline.get("tree") == report["upstream_tree"],
            "stock export identity differs from the immutable comparison")
    for field in ("archive_sha256", "dependency_lock_sha256", "fork_dependency_lock_sha256",
                  "sqlite_wrapper_sha256"):
        require(isinstance(baseline.get(field), str) and bool(DIGEST.fullmatch(baseline[field])),
                f"stock export omits {field}")
    provenance = baseline.get("runtime_provenance")
    require(isinstance(provenance, dict) and provenance.get("status") == "verified"
            and provenance.get("issues") == [] and provenance.get("package_manager") == "pnpm@12.14.0"
            and provenance.get("installed_lock_matches_application_lock") is True
            and provenance.get("external_packages_within_virtual_store") is True
            and provenance.get("per_workspace_resolution") is True,
            "comparison dependencies were not verified against the frozen production pnpm installation")
    for field in ("installed_lock_sha256", "modules_metadata_sha256", "node_executable_sha256", "external_resolution_sha256"):
        require(isinstance(provenance.get(field), str) and bool(DIGEST.fullmatch(provenance[field])),
                f"comparison runtime omits {field}")
    dependencies = baseline.get("external_dependencies")
    fields = ("importer", "name", "version", "package_json_sha256", "installed_path")
    require(isinstance(dependencies, list) and bool(dependencies)
            and all(isinstance(item, dict)
                    and all(isinstance(item.get(field), str) and bool(item[field]) for field in fields)
                    and DIGEST.fullmatch(item["package_json_sha256"])
                    and item["installed_path"].startswith("node_modules/.pnpm/")
                    and not any(part in (".", "..", "") for part in item["installed_path"].split("/"))
                    and "\\" not in item["installed_path"] for item in dependencies),
            "comparison omits installed external dependency locations or points outside the frozen store")
    installed = {(item["importer"], item["name"]): item for item in dependencies}
    require(len(installed) == len(dependencies), "comparison repeats an installed dependency resolution")
    retained = [[item[field] for field in fields] for item in dependencies]
    require(hashlib.sha256(json.dumps(retained, ensure_ascii=False, separators=(",", ":")).encode()).hexdigest()
            == provenance["external_resolution_sha256"],
            "installed dependency resolution digest contradicts the retained package evidence")
    runtime = baseline.get("resolved_runtime")
    require(isinstance(runtime, list) and bool(runtime)
            and all(isinstance(item, dict) and isinstance(item.get("name"), str)
                    and isinstance(item.get("importer"), str)
                    and isinstance(item.get("expected_version"), str) and bool(item["expected_version"])
                    and item.get("actual_version") == item["expected_version"]
                    and isinstance(item.get("package_json_sha256"), str)
                    and DIGEST.fullmatch(item["package_json_sha256"]) for item in runtime)
            and {"effect", "@effect/platform-node"} <= {item["name"] for item in runtime},
            "stock and native critical dependency versions are not bound to the same lock")
    for item in runtime:
        descriptor = installed.get((item["importer"], item["name"]), installed.get((".", item["name"])))
        require(descriptor is not None
                and descriptor["version"] == item["actual_version"]
                and descriptor["package_json_sha256"] == item["package_json_sha256"]
                and descriptor["installed_path"] == item.get("installed_path"),
                "resolved production dependency contradicts its retained installed package")


def valid_result(row: object) -> bool:
    if not isinstance(row, dict):
        return False
    interval = row.get("confidence_interval")
    return (isinstance(row.get("id"), str) and bool(row["id"])
            and row.get("phase") in ("cold", "warm") and row.get("status") == "passed"
            and positive(row.get("ratio")) and row["ratio"] <= 1
            and isinstance(interval, list) and len(interval) == 2
            and all(positive(value) for value in interval)
            and interval[0] <= interval[1] <= 1)


def valid_summary(value: object, commit: str, tree: str, policy: str) -> bool:
    if not isinstance(value, dict):
        return False
    if (value.get("status") != "passed" or value.get("suite") != "persistence-api"
            or value.get("source_commit") != commit or value.get("source_tree") != tree
            or value.get("policy_digest") != policy):
        return False
    for field in ("source_commit", "source_tree", "upstream_commit", "upstream_tree"):
        if not isinstance(value.get(field), str) or not SHA.fullmatch(value[field]):
            return False
    for field in ("policy_digest", "executable_sha256", "native_manifest_sha256",
                  "toolchain_lock_sha256", "report_sha256"):
        if not isinstance(value.get(field), str) or not DIGEST.fullmatch(value[field]):
            return False
    for field in ("ci_run_id", "ci_run_attempt", "native_build_run_attempt"):
        if not isinstance(value.get(field), str) or not RUN_ID.fullmatch(value[field]):
            return False
    if int(value["native_build_run_attempt"]) > int(value["ci_run_attempt"]):
        return False
    rows = value.get("scenario_results")
    return (isinstance(rows, list) and bool(rows) and all(valid_result(row) for row in rows)
            and [(row["id"], row["phase"], row.get("metric")) for row in rows]
            == [(row["id"], row["phase"], row["metric"]) for row in CONTRACT["scenarios"]])


def validate_report(report: object, raw: bytes, plan: dict, run_id: str,
                    expected_digest: str, run_attempt: str = "1") -> dict:
    require(len(raw) <= MAX_REPORT_BYTES, "report is too large")
    require(isinstance(expected_digest, str) and bool(DIGEST.fullmatch(expected_digest)),
            "the current performance job did not identify its report")
    digest = hashlib.sha256(raw).hexdigest()
    require(digest == expected_digest, "downloaded report differs from the current job output")
    require(isinstance(report, dict), "report must be an object")
    require(type(report.get("schema_version")) is int and report["schema_version"] == 1,
            "unsupported report schema")
    require(report.get("status") == "passed" and report.get("mode") == "qualify",
            "failed, regressed, diagnostic or inconclusive measurements cannot qualify")
    require(report.get("suite") == "persistence-api", "unexpected workload suite")
    for field, expected in (("source_commit", plan["head"]),
                            ("source_tree", plan["source_tree"]),
                            ("policy_digest", plan["policy_digest"]),
                            ("upstream_commit", plan["upstream_commit"]),
                            ("upstream_tree", plan["upstream_tree"]),
                            ("ci_run_id", run_id), ("ci_run_attempt", run_attempt)):
        require(report.get(field) == expected, f"stale or mismatched {field}")
    require(report.get("source_tree_dirty") is False and report.get("source_tree_dirty_after") is False,
            "source must remain clean throughout measurement")
    require(report.get("qualifies") is True, "the comparison did not qualify")
    require(report.get("compiler_provenance") == "verified-ci-manifest"
            and report.get("compiler_route") == "bluefe-rustc-via-lustr"
            and report.get("build_profile") == "release", "unverified compilation route/profile")
    validate_baseline(report)
    configuration = report.get("configuration")
    require(isinstance(configuration, dict), "missing fixed workload configuration")
    for field in ("pairs", "warmups", "samples", "seed", "bootstrap_resamples",
                  "confidence", "comparison_ratio", "fixture"):
        require(json.dumps(configuration.get(field), sort_keys=True) == json.dumps(CONTRACT[field], sort_keys=True),
                f"{field} differs from the preregistered workload contract")
    require(configuration.get("phase") == "all", "validation-only attempts do not measure speed")
    pairs = CONTRACT["pairs"]
    correctness = report.get("correctness")
    require(isinstance(correctness, dict) and correctness.get("status") == "passed"
            and isinstance(correctness.get("checks"), list) and bool(correctness["checks"]),
            "correctness, rollback and reopen evidence is required before timing")
    rows = report.get("scenarios")
    require(isinstance(rows, list) and bool(rows) and all(valid_result(row) for row in rows),
            "every measured workload must independently pass; no aggregate can hide a regression")
    require([(row["id"], row["phase"], row.get("metric")) for row in rows]
            == [(row["id"], row["phase"], row["metric"]) for row in CONTRACT["scenarios"]],
            "missing, additional, reordered or misclassified workload/metric rows")
    orders = expected_orders(pairs, CONTRACT["seed"])
    measurements = validate_runs(report, orders)
    for index, row in enumerate(rows):
        raw_pairs = row.get("pairs")
        require(isinstance(raw_pairs, list) and len(raw_pairs) == pairs,
                f"{row['id']} is missing raw paired samples")
        require(all(isinstance(pair, dict) and type(pair.get("pair")) is int
                    and positive(pair.get("upstream_ms")) and positive(pair.get("native_ms"))
                    and positive(pair.get("ratio")) for pair in raw_pairs),
                f"{row['id']} contains malformed raw samples")
        require([pair["pair"] for pair in raw_pairs] == list(range(pairs)),
                f"{row['id']} has missing, repeated or reordered pair IDs")
        require([pair.get("order") for pair in raw_pairs] == orders,
                f"{row['id']} does not use the preregistered balanced order")
        operation = row["id"].removesuffix(".p95")
        probability = .95 if row["metric"] == "p95" else .5
        def values(pair, backend):
            run = measurements[(pair, backend)]
            return [run["cold_open_ms"]] if row["phase"] == "cold" else run["samples"][operation]
        require(all(math.isclose(pair[backend + "_ms"], quantile(values(pair["pair"], backend), probability), rel_tol=1e-12)
                    for pair in raw_pairs for backend in ("upstream", "native")),
                f"{row['id']} paired summaries contradict the raw worker timings")
        ratios = [pair["native_ms"] / pair["upstream_ms"] for pair in raw_pairs]
        require(all(math.isclose(pair["ratio"], ratio, rel_tol=1e-12) for pair, ratio in zip(raw_pairs, ratios)),
                f"{row['id']} pair ratios differ from their raw timings")
        expected_ratio, expected_interval = paired_comparison(ratios, CONTRACT["seed"] + index)
        require(math.isclose(row["ratio"], expected_ratio, rel_tol=1e-12)
                and all(math.isclose(actual, expected, rel_tol=1e-12)
                        for actual, expected in zip(row["confidence_interval"], expected_interval)),
                f"{row['id']} summary differs from the preregistered paired comparison")
        for backend in ("upstream", "native"):
            metrics = row.get(backend)
            require(isinstance(metrics, dict)
                    and all(positive(metrics.get(key)) for key in ("median_ms", "p95_ms", "max_ms")),
                    f"{row['id']} omits {backend} timing statistics")
            all_values = [value for pair in range(pairs) for value in values(pair, backend)]
            require(type(metrics.get("samples")) is int and metrics["samples"] == len(all_values)
                    and positive(metrics.get("sequential_requests_per_second"))
                    and math.isclose(metrics["sequential_requests_per_second"],
                                     1000 * len(all_values) / sum(all_values), rel_tol=1e-12),
                    f"{row['id']} {backend} sample count or throughput contradicts raw worker timings")
            require(all(math.isclose(metrics[key], expected, rel_tol=1e-12)
                        for key, expected in (("median_ms", quantile(all_values, .5)),
                                              ("p95_ms", quantile(all_values, .95)),
                                              ("max_ms", max(all_values)))),
                    f"{row['id']} {backend} statistics contradict raw worker timings")
    result = {field: report.get(field) for field in IDENTITY_FIELDS}
    result.update(status="passed", suite=report["suite"], report_sha256=digest,
                  scenario_results=[{key: row[key] for key in
                                     ("id", "phase", "metric", "status", "ratio", "confidence_interval")}
                                    for row in rows])
    require(valid_summary(result, plan["head"], plan["source_tree"], plan["policy_digest"]),
            "invalid compact result or missing cold/warm workload")
    return result
