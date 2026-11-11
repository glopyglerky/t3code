"""Fail-closed checks for the compiler proof receipt; these do not claim Lust parity."""
import importlib.util
import json
from pathlib import Path
import tempfile
import sys
import unittest
from unittest.mock import patch
from pinned_rust import pinned_stock_environment, stock_rust_environment

sys.dont_write_bytecode = True

SPEC = importlib.util.spec_from_file_location("check_lust", Path(__file__).with_name("check-lust.py"))
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class ProofGateTests(unittest.TestCase):
    def test_stock_selection_validates_effective_target_flags_with_global_precedence(self):
        target = "x86_64-unknown-linux-gnu"
        key = "CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUSTFLAGS"
        benign = {key: "  --sysroot=/pinned  -C opt-level=1  "}
        self.assertEqual(stock_rust_environment(benign, {}, "/pinned", "1.99.1", target),
                         benign | {"RUSTUP_TOOLCHAIN": "1.99.1"})
        for flags in ("--sysroot=/other -C opt-level=1", "--sysroot",
                      "--sysroot=/pinned --sysroot /pinned"):
            with self.subTest(flags=flags), self.assertRaisesRegex(RuntimeError, "sysroot"):
                stock_rust_environment({key: flags}, {}, "/pinned", "1.99.1", target)
        for globals_ in ({"CARGO_ENCODED_RUSTFLAGS": ""}, {"RUSTFLAGS": ""},
                         {"CARGO_ENCODED_RUSTFLAGS": "-C\x1flink-arg=a b", "RUSTFLAGS": "--sysroot=/other"}):
            env = {key: "--sysroot=/other"} | globals_
            self.assertEqual(stock_rust_environment(env, {}, "/pinned", "1.99.1", target),
                             env | {"RUSTUP_TOOLCHAIN": "1.99.1"})
        with self.assertRaisesRegex(RuntimeError, "sysroot"):
            stock_rust_environment({"CARGO_BUILD_RUSTFLAGS": "--sysroot=/other"}, {}, "/pinned", "1.99.1", target)
        unused = {"CARGO_TARGET_AARCH64_APPLE_DARWIN_RUSTFLAGS": "--sysroot=/other"}
        self.assertEqual(stock_rust_environment(unused, {}, "/pinned", "1.99.1", target),
                         unused | {"RUSTUP_TOOLCHAIN": "1.99.1"})
        with self.assertRaisesRegex(RuntimeError, "sysroot"):
            stock_rust_environment(unused, {}, "/pinned", "1.99.1", "aarch64-apple-darwin")

    def test_pinned_stock_caller_uses_cargo_target_or_verified_compiler_host(self):
        def output(command, **_):
            if command[:2] == ["rustup", "which"]:
                return f"/pinned/bin/{command[-1]}"
            if command[-1] == "-vV":
                return "rustc 1.99.1\nrelease: 1.99.1\nhost: x86_64-unknown-linux-gnu"
            if command[-2:] == ["--print", "sysroot"]:
                return "/pinned"
            self.fail(f"Unexpected compiler invocation: {command}")
        env = {"CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUSTFLAGS": "--sysroot=/other"}
        with patch("pinned_rust.subprocess.check_output", side_effect=output):
            with self.assertRaisesRegex(RuntimeError, "sysroot"):
                pinned_stock_environment(MODULE.ROOT, env)
            selected, _, _ = pinned_stock_environment(MODULE.ROOT, env | {
                "CARGO_BUILD_TARGET": "aarch64-apple-darwin",
                "CARGO_TARGET_AARCH64_APPLE_DARWIN_RUSTFLAGS": "-C opt-level=1",
            })
            self.assertEqual(selected["CARGO_BUILD_TARGET"], "aarch64-apple-darwin")
            self.assertNotIn("CARGO_ENCODED_RUSTFLAGS", selected)

    def test_stock_selection_preserves_matching_flags_target_and_input_environment(self):
        tools = {"CARGO": "/pinned/bin/cargo", "RUSTC": "/pinned/bin/rustc",
                 "RUSTDOC": "/pinned/bin/rustdoc"}
        env = dict(CARGO_ENCODED_RUSTFLAGS="--sysroot\x1f/pinned\x1f-C\x1flink-arg=a b\x1f",
                   RUSTFLAGS="ignored because encoded flags win", CARGO_BUILD_TARGET="aarch64-apple-darwin",
                   RUSTC=tools["RUSTC"], RUSTC_WRAPPER="")
        before = env.copy()
        selected = stock_rust_environment(env, tools, "/pinned", "1.99.1", "x86_64-unknown-linux-gnu")
        self.assertEqual(selected, before | tools | {"RUSTUP_TOOLCHAIN": "1.99.1"})
        self.assertEqual(env, before)
        for plain in ("-C opt-level=1", "--sysroot=/pinned -C opt-level=1"):
            self.assertEqual(stock_rust_environment({"RUSTFLAGS": plain}, tools, "/pinned", "1.99.1", "x86_64-unknown-linux-gnu")["RUSTFLAGS"], plain)
        selected = stock_rust_environment({}, tools, "/pinned", "1.99.1", "x86_64-unknown-linux-gnu")
        self.assertNotIn("RUSTFLAGS", selected)
        self.assertNotIn("CARGO_ENCODED_RUSTFLAGS", selected)

    def test_stock_selection_rejects_conflicting_sysroots_with_cargo_precedence(self):
        for flags in ("--sysroot", "--sysroot=", "--sysroot\x1f/other",
                      "--sysroot=/pinned\x1f--sysroot\x1f/pinned"):
            with self.subTest(flags=flags), self.assertRaises(RuntimeError):
                stock_rust_environment({"CARGO_ENCODED_RUSTFLAGS": flags,
                                        "RUSTFLAGS": "--sysroot=/pinned"}, {}, "/pinned", "1.99.1", "x86_64-unknown-linux-gnu")
        selected = stock_rust_environment({"CARGO_ENCODED_RUSTFLAGS": "",
                                           "RUSTFLAGS": "--sysroot=/other"}, {}, "/pinned", "1.99.1", "x86_64-unknown-linux-gnu")
        self.assertEqual(selected["CARGO_ENCODED_RUSTFLAGS"], "")

    def test_stock_selection_rejects_tools_wrappers_and_routes_without_overwriting(self):
        tools = {"CARGO": "/pinned/bin/cargo", "RUSTC": "/pinned/bin/rustc",
                 "RUSTDOC": "/pinned/bin/rustdoc"}
        for name in (*tools, "RUSTC_WRAPPER", "RUSTC_WORKSPACE_WRAPPER",
                     "CARGO_BUILD_RUSTC_WRAPPER", "CARGO_BUILD_RUSTC_WORKSPACE_WRAPPER",
                     "T3_CODE_LS_TOOLCHAIN", "T3_CODE_LS_SYSROOT", "T3_CODE_LS_COMPILER"):
            env = {name: "/other"}
            with self.subTest(name=name), self.assertRaises(RuntimeError):
                stock_rust_environment(env, tools, "/pinned", "1.99.1", "x86_64-unknown-linux-gnu")
            self.assertEqual(env, {name: "/other"})
        with self.assertRaises(RuntimeError):
            stock_rust_environment({}, tools, "/pinned", "stable", "x86_64-unknown-linux-gnu")

    def test_stock_fallback_cannot_run_after_environment_validation_failure(self):
        with patch.object(MODULE, "pinned_stock_environment", side_effect=RuntimeError("conflicting sysroot")), \
             patch.object(MODULE, "run") as run:
            with self.assertRaisesRegex(RuntimeError, "conflicting sysroot"):
                MODULE.verify_stock_fallback({}, {"steps": []}, [])
            run.assert_not_called()

    def test_only_structured_subset_compile_failures_can_select_stock_rust(self):
        diagnostic = dict(schema=1, kind="diagnostic", severity="error", code="L0104",
                          message="no method `replace_range` on type `String`")
        step = dict(name="check core with Lust", returncode=1,
                    stdout=json.dumps(diagnostic), stderr="")
        self.assertEqual(MODULE.unsupported_core_diagnostics(step), [diagnostic])
        for changes in [dict(name="verify the Lust-built policy executable"),
                        dict(name="build pinned Lust compiler"), dict(returncode=-11),
                        dict(returncode=0), dict(stderr="compiler panicked"),
                        dict(stdout=""), dict(stdout="unstructured error"),
                        dict(stdout=json.dumps(diagnostic | {"code": "unknown"})),
                        dict(stdout=json.dumps(diagnostic | {"severity": "warning"}))]:
            with self.subTest(changes=changes):
                self.assertIsNone(MODULE.unsupported_core_diagnostics(step | changes))

    def test_only_complete_nonempty_passing_run_counts_as_proof(self):
        valid = dict(kind="summary", build="ok", total=2, passed=2,
                     failed=0, timed_out=0, ignored=0, filtered_out=0)
        self.assertEqual(MODULE.test_summary(json.dumps(valid)), valid)
        changes = [dict(total=0, passed=0), dict(passed=1, failed=1),
                   dict(passed=1, timed_out=1), dict(passed=1, ignored=1),
                   dict(filtered_out=1), dict(build="failed")]
        for change in changes:
            with self.subTest(change=change), self.assertRaises(RuntimeError):
                MODULE.test_summary(json.dumps(valid | change))
        with self.assertRaises(RuntimeError):
            MODULE.test_summary("")
        with self.assertRaises(RuntimeError):
            MODULE.test_summary(json.dumps(valid) + "\n" + json.dumps(valid))

    def test_mismatched_revision_and_dirty_source_are_rejected_before_build(self):
        with patch.object(MODULE, "git", return_value="other"):
            with self.assertRaisesRegex(RuntimeError, "revision mismatch"):
                MODULE.verify_source(Path("/compiler"), "pinned")
        with patch.object(MODULE, "git", side_effect=["pinned", " M src/main.rs"]):
            with self.assertRaisesRegex(RuntimeError, "local changes"):
                MODULE.verify_source(Path("/compiler"), "pinned")

    def test_failed_receipt_replaces_stale_success(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "receipt.json"
            MODULE.write_receipt(path, dict(status="passed"))
            MODULE.write_receipt(path, dict(status="failed", error="compiler exited 1"))
            self.assertEqual(json.loads(path.read_text())["status"], "failed")


if __name__ == "__main__":
    unittest.main()
