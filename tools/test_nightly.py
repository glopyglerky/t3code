"""Offline contract tests for nightly selection and immutable source comparison."""

import copy
import importlib.util
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location("nightly", Path(__file__).with_name("nightly.py"))
nightly = importlib.util.module_from_spec(spec)
spec.loader.exec_module(nightly)


def release(tag="v0.1.8-nightly.20261108.4367", published="2026-11-08T12:24:26Z", identifier=1, **extra):
    return {"tag_name": tag, "published_at": published, "id": identifier,
            "draft": False, "prerelease": True, **extra}


def api(fixtures):
    calls = []

    def get(url):
        calls.append(url)
        return fixtures[url]

    return get, calls


class NightlyMetadata(unittest.TestCase):
    def test_selects_published_nightly_across_every_page(self):
        first = f"{nightly.API}/releases?per_page=100"
        second = f"{nightly.API}/releases?per_page=100&page=2"
        newer = release(identifier=9)
        get, calls = api({
            first: ([release(tag="v0.1.8-nightly.20261107.4340", published="2026-11-07T00:00:00Z"),
                     release(tag="v99.0.0", identifier=20), release(identifier=30, draft=True)],
                    {"Link": f'<{second}>; rel="next", <{second}>; rel="last"'}),
            second: ([newer], {}),
        })
        self.assertEqual(nightly.latest_nightly(nightly.all_releases(get))["release_id"], 9)
        self.assertEqual(calls, [first, second])

    def test_empty_page_with_continuation_is_not_end(self):
        first = f"{nightly.API}/releases?per_page=100"
        second = f"{nightly.API}/releases?page=2"
        get, _ = api({first: ([], {"link": f'<{second}>; rel="next"'}), second: ([release()], {})})
        self.assertEqual(len(nightly.all_releases(get)), 1)

    def test_never_reports_latest_after_partial_or_cyclic_pagination(self):
        first = f"{nightly.API}/releases?per_page=100"
        get, _ = api({first: ([release()], {"Link": f'<{first}>; rel="next"'})})
        with self.assertRaisesRegex(nightly.TrackingError, "repeated"):
            nightly.all_releases(get)
        with self.assertRaisesRegex(nightly.TrackingError, "limit"):
            nightly.all_releases(get, max_pages=0)

    def test_legacy_nightly_prefix_does_not_break_full_history(self):
        legacy = release("nightly-v0.0.21-nightly.20260417.58", "2026-04-18T00:03:28Z", 310602486)
        self.assertEqual(nightly.latest_nightly([legacy, release()])["tag"], release()["tag_name"])

    def test_publication_time_beats_tag_and_api_order(self):
        older_tag_published_later = release("v0.1.7-nightly.20261101.4", "2026-11-09T00:00:00Z", 5)
        self.assertEqual(nightly.latest_nightly([older_tag_published_later, release()])["release_id"], 5)

    def test_rejects_ambiguous_or_missing_nightly_metadata(self):
        bad = [release(id=True), release(published_at="tomorrow"), release(published_at="2026-11-08T01:00:00"),
               release(tag_name="v0.1.8-nightly.bad"), release(draft="false"), {"id": 4}]
        for item in bad:
            with self.subTest(item=item), self.assertRaises(nightly.TrackingError):
                nightly.latest_nightly([item])
        with self.assertRaises(nightly.TrackingError):
            nightly.latest_nightly([release(), release(identifier=2)])
        with self.assertRaises(nightly.TrackingError):
            nightly.latest_nightly([release(draft=True), release(tag="v0.1.8")])

    def test_pagination_never_forwards_token_to_another_origin_or_repo(self):
        for url in ["http://api.github.com/repos/pingdotgg/t3code/releases?page=2",
                    "https://evil.test/releases", "https://api.github.com/repos/another/repo/releases",
                    "https://api.github.com@evil.test/repos/pingdotgg/t3code/releases"]:
            with self.subTest(url=url), self.assertRaises(nightly.TrackingError):
                nightly.next_page({"Link": f'<{url}>; rel="next"'})
        with self.assertRaises(nightly.TrackingError):
            nightly.next_page({"Link": "not a valid header"})
        canonical = f"https://api.github.com/repositories/{nightly.REPOSITORY_ID}/releases?page=2"
        self.assertEqual(nightly.next_page({"Link": f'<{canonical}>; rel="next"'}), canonical)

    def test_resolves_lightweight_and_nested_annotated_tags(self):
        tag = release()["tag_name"]
        ref = f"{nightly.API}/git/ref/tags/{tag}"
        commit = "c" * 40
        for annotation_count in (0, 1, 2):
            tags = [str(i + 1) * 40 for i in range(annotation_count)]
            chain = [{"type": "tag", "sha": sha} for sha in tags] + [{"type": "commit", "sha": commit}]
            fixtures = {ref: ({"ref": f"refs/tags/{tag}", "object": chain[0]}, {})}
            for index, sha in enumerate(tags):
                fixtures[f"{nightly.API}/git/tags/{sha}"] = ({"sha": sha, "object": chain[index + 1]}, {})
            get, calls = api(fixtures)
            self.assertEqual(nightly.resolve_tag(get, tag), commit)
            self.assertEqual(len(calls), annotation_count + 1)

    def test_rejects_tag_cycles_wrong_refs_and_noncommits(self):
        tag = release()["tag_name"]
        ref = f"{nightly.API}/git/ref/tags/{tag}"
        sha = "a" * 40
        get, _ = api({ref: ({"ref": f"refs/tags/{tag}", "object": {"type": "tag", "sha": sha}}, {}),
                      f"{nightly.API}/git/tags/{sha}": ({"sha": sha, "object": {"type": "tag", "sha": sha}}, {})})
        with self.assertRaisesRegex(nightly.TrackingError, "cycle"):
            nightly.resolve_tag(get, tag)
        for data in [{"ref": "refs/tags/other", "object": {"type": "commit", "sha": sha}},
                     {"ref": f"refs/tags/{tag}", "object": {"type": "blob", "sha": sha}},
                     {"ref": f"refs/tags/{tag}", "object": {"type": "commit", "sha": "short"}}]:
            get, _ = api({ref: (data, {})})
            with self.assertRaises(nightly.TrackingError):
                nightly.resolve_tag(get, tag)


class ImmutableSnapshots(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.run_git("init", "--quiet")
        self.run_git("config", "user.email", "test@example.invalid")
        self.run_git("config", "user.name", "Offline test")
        for prefix in ["apps/server/", "apps/web/", "apps/desktop/", "apps/mobile/", "packages/contracts/",
                       "packages/client-runtime/", "infra/relay/", "packages/provider-core/", "packages/shared/"]:
            file = self.root / prefix / "source.txt"
            file.parent.mkdir(parents=True, exist_ok=True)
            file.write_text("baseline\n")
        (self.root / "apps/server/retained.txt").write_text("keep server present")
        self.base = self.commit("baseline")
        self.metadata = nightly.release_metadata(release())
        self.lock = nightly.manifest(self.root, self.metadata, self.base)

    def run_git(self, *args):
        return subprocess.check_output(["git", "-C", str(self.root), *args], stderr=subprocess.DEVNULL).decode().strip()

    def commit(self, message):
        self.run_git("add", "--all")
        self.run_git("commit", "--quiet", "-m", message)
        return self.run_git("rev-parse", "HEAD")

    def write_port_map(self, *ports):
        mapping = {"schema_version": 1, "integrated_upstream_commit": self.base, "ports": list(ports)}
        (self.root / "port-map.json").write_text(json.dumps(mapping))
        return mapping

    def port(self, identifier, source):
        return {
            "id": identifier, "state": "integrated", "domain_owner": "apps/server",
            "reviewed_upstream_commit": self.base,
            "upstream_sources": [{"path": source, "blob": self.run_git("rev-parse", f"{self.base}:{source}")}],
            "retired_paths": [], "rust_paths": [f"crates/core/src/{identifier}.rs"],
            "rust_crates": ["crates/core"], "adapter_paths": [f"apps/server/{identifier}.ts"],
            "platform_routes": [{"targets": ["linux-x64"], "implementation": "rust", "state": "integrated"}],
            "acceptance_tests": [f"tools/check-{identifier}.mjs"],
        }

    def test_mapped_original_change_still_alerts_after_fork_source_retirement(self):
        source = "apps/server/source.txt"
        (self.root / source).write_text("changed upstream behavior\n")
        head = self.commit("upstream changes translated source")
        candidate = nightly.manifest(self.root, self.metadata, head)
        port = self.port("policy", source)
        port["state"] = "retired"
        port["retired_paths"] = [{"path": source, "source_blob": port["upstream_sources"][0]["blob"]}]
        self.write_port_map(port)
        (self.root / source).unlink()
        status = self.run_git("status", "--porcelain")
        result = nightly.report(self.root, self.lock, candidate)
        affected = result["port_impact"]["affected_ports"]
        self.assertEqual(len(affected), 1)
        self.assertEqual(affected[0]["id"], "policy")
        self.assertEqual(affected[0]["changed_upstream_paths"], [source])
        self.assertEqual(affected[0]["rust_paths"], port["rust_paths"])
        self.assertEqual(affected[0]["acceptance_tests"], port["acceptance_tests"])
        self.assertFalse(affected[0]["requires_shared_contract_review"])
        self.assertEqual(self.run_git("status", "--porcelain"), status)
        self.assertFalse((self.root / source).exists())

    def test_shared_contract_change_reviews_all_ports_and_preserves_unmapped_additions(self):
        contract = "packages/contracts/source.txt"
        (self.root / contract).write_text("new public response\n")
        addition = "apps/mobile/new-feature.txt"
        (self.root / addition).write_text("retain this upstream implementation\n")
        head = self.commit("contract and new client functionality")
        candidate = nightly.manifest(self.root, self.metadata, head)
        self.write_port_map(self.port("policy", "apps/server/source.txt"),
                            self.port("generation", "packages/shared/source.txt"))
        self.lock["qualification"] = {"status": "qualified", "last_qualified_commit": self.base,
                                      "evidence": ["prior acceptance"]}
        before = copy.deepcopy(self.lock)
        result = nightly.report(self.root, self.lock, candidate)
        impact = result["port_impact"]
        self.assertEqual(impact["shared_contract_paths"], [contract])
        self.assertEqual([port["id"] for port in impact["affected_ports"]], ["policy", "generation"])
        self.assertTrue(all(port["requires_shared_contract_review"] for port in impact["affected_ports"]))
        self.assertIn("mobile", impact["contract_consumer_review"])
        self.assertIn("providers", impact["contract_consumer_review"])
        self.assertIn({"path": addition, "status": "added"}, impact["unmapped_changes"])
        self.assertEqual(result["revisions"], {"detected": head, "integrated": self.base, "qualified": self.base})
        self.assertEqual(result["qualification"], before["qualification"])
        self.assertEqual(self.lock, before)
        self.assertEqual(candidate["qualification"]["status"], "unqualified")

    def test_stale_map_or_port_review_cannot_report_no_translation_impact(self):
        for changed in ("map", "port"):
            mapping = self.write_port_map(self.port("policy", "apps/server/source.txt"))
            if changed == "map":
                mapping["integrated_upstream_commit"] = "a" * 40
            else:
                mapping["ports"][0]["reviewed_upstream_commit"] = "a" * 40
            (self.root / "port-map.json").write_text(json.dumps(mapping))
            with self.subTest(changed=changed), self.assertRaisesRegex(nightly.TrackingError, "baseline"):
                nightly.report(self.root, self.lock, self.lock)

    def test_missing_map_is_reported_as_unavailable_not_no_affected_ports(self):
        (self.root / "apps/web/source.txt").write_text("new upstream UI\n")
        head = self.commit("upstream client change")
        candidate = nightly.manifest(self.root, self.metadata, head)
        result = nightly.report(self.root, self.lock, candidate)
        self.assertEqual(result["port_impact"]["status"], "unavailable")
        self.assertIn("manual", result["port_impact"]["reason"])
        self.assertEqual(result["port_impact"]["unmapped_changes"],
                         [{"path": "apps/web/source.txt", "status": "modified"}])

    def test_reports_more_than_300_files_and_preserves_worktree(self):
        for index in range(341):
            (self.root / "apps/mobile" / f"file-{index}.txt").write_text(str(index))
        (self.root / "apps/server/source.txt").unlink()
        head = self.commit("new nightly")
        candidate = nightly.manifest(self.root, {**self.metadata, "release_id": 2}, head)
        (self.root / "apps/mobile/local-fork-change.txt").write_text("do not overwrite")
        status = self.run_git("status", "--porcelain")
        result = nightly.report(self.root, self.lock, candidate)
        self.assertEqual(result["changed_file_count"], 342)
        self.assertEqual(result["changed_surface_counts"]["mobile"], 341)
        self.assertEqual(self.run_git("status", "--porcelain"), status)
        self.assertEqual(self.run_git("rev-parse", "HEAD"), head)
        self.assertEqual(result["qualification"], self.lock["qualification"])
        self.assertEqual(candidate["qualification"]["status"], "unqualified")

    def test_object_snapshots_ignore_uncommitted_edits_and_are_idempotent(self):
        (self.root / "apps/server/source.txt").write_text("local change")
        new = nightly.manifest(self.root, self.metadata, self.base)
        self.assertEqual(new, self.lock)
        result = nightly.report(self.root, self.lock, new)
        self.assertEqual(result["state"], "current")
        self.assertEqual(result["changed_file_count"], 0)
        output = self.root / "result.json"
        self.assertTrue(nightly.write_json(output, result))
        timestamp = output.stat().st_mtime_ns
        self.assertFalse(nightly.write_json(output, result))
        self.assertEqual(output.stat().st_mtime_ns, timestamp)

    def test_changed_mode_and_unscoped_paths_are_in_complete_report(self):
        self.run_git("update-index", "--chmod=+x", "apps/server/source.txt")
        (self.root / "package.json").write_text("{}")
        self.run_git("add", "package.json")
        self.run_git("commit", "--quiet", "-m", "mode and build")
        head = self.run_git("rev-parse", "HEAD")
        candidate = nightly.manifest(self.root, self.metadata, head)
        result = nightly.report(self.root, self.lock, candidate)
        self.assertEqual([change["path"] for change in result["changes"]], ["apps/server/source.txt", "package.json"])
        self.assertEqual(result["changes"][1]["surfaces"], [])

    def test_tampered_inventory_and_root_tree_fail(self):
        for field in ("source_surfaces", "root_tree"):
            changed = copy.deepcopy(self.lock)
            if field == "root_tree":
                changed["upstream"]["root_tree"] = "a" * 40
            else:
                changed["source_surfaces"]["mobile"]["entries"] += 1
            with self.subTest(field=field), self.assertRaises(nightly.TrackingError):
                nightly.report(self.root, changed, self.lock)

    def test_invalid_or_missing_object_is_never_treated_as_current(self):
        for sha in ("a" * 40, "HEAD", "--help"):
            with self.subTest(sha=sha), self.assertRaises(nightly.TrackingError):
                nightly.ensure_commit(self.root, sha)
        blob = self.run_git("rev-parse", f"{self.base}:apps/server/source.txt")
        with self.assertRaisesRegex(nightly.TrackingError, "not a commit"):
            nightly.ensure_commit(self.root, blob)

    def test_missing_surface_and_invalid_lock_fail(self):
        (self.root / "apps/mobile/source.txt").unlink()
        head = self.commit("unexpected mobile removal")
        with self.assertRaisesRegex(nightly.TrackingError, "absent: mobile"):
            nightly.manifest(self.root, self.metadata, head)
        output = self.root / "lock.json"
        output.write_text(json.dumps({**self.lock, "schema_version": 99}))
        with self.assertRaises(nightly.TrackingError):
            nightly.read_lock(output)

    def test_removed_nightly_cannot_silently_roll_back_the_pin(self):
        output = self.root / "lock.json"
        report_path = self.root / "report.json"
        nightly.write_json(output, self.lock)
        older = release("v0.1.8-nightly.20261101.4000", "2026-11-01T00:00:00Z", 1)
        with patch.object(nightly, "all_releases", return_value=[older]), patch("sys.stderr"):
            status = nightly.main(["check", "--repo-root", str(self.root), "--lock", str(output),
                                   "--output", str(report_path)])
        self.assertEqual(status, 1)
        self.assertFalse(report_path.exists())

    def test_moved_pinned_tag_fails_before_any_output(self):
        output = self.root / "lock.json"
        report_path = self.root / "report.json"
        nightly.write_json(output, self.lock)
        pinned_tag = self.lock["upstream"]["tag"]
        newer = release("v0.1.8-nightly.20261109.4400", "2026-11-09T00:00:00Z", 2)

        def resolve(_get, tag):
            return "a" * 40 if tag == pinned_tag else self.base

        for releases in ([release()], [release(), newer]):
            with self.subTest(newest=releases[-1]["tag_name"]), \
                    patch.object(nightly, "all_releases", return_value=releases), \
                    patch.object(nightly, "resolve_tag", side_effect=resolve), patch("sys.stderr"):
                status = nightly.main(["check", "--repo-root", str(self.root), "--lock", str(output),
                                       "--output", str(report_path)])
                self.assertEqual(status, 1)
                self.assertFalse(report_path.exists())
                self.assertEqual(json.loads(output.read_text()), self.lock)

    def test_check_refuses_to_overwrite_lock(self):
        output = self.root / "lock.json"
        nightly.write_json(output, self.lock)
        before = output.read_bytes()
        with patch("sys.stderr"):
            status = nightly.main(["check", "--repo-root", str(self.root), "--lock", str(output), "--output", str(output)])
        self.assertEqual(status, 1)
        self.assertEqual(output.read_bytes(), before)


if __name__ == "__main__":
    unittest.main()
