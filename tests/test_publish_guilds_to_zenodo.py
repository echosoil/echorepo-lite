"""Offline and mocked checks for staged guild release safety."""

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from tools import publish_guilds_to_zenodo as p
from tools import prepare_guild_publication as prep


class ReleaseTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.bundle = self.root / "bundle"
        self.bundle.mkdir()
        report = {"candidate_manifests": {}}
        for kind in p.KINDS:
            folder = self.bundle / kind
            folder.mkdir()
            for name in p.FILES[kind] - {"provenance.json"}:
                (folder / name).write_text("{}", encoding="utf-8")
            prep.write_json(folder / "file.json", {"tables": []})
            stats = {"samples": 1}
            prep.write_json(folder / "provenance.json", {
                "publication_schema_version": "2.0", "proportional_unit": "percent",
                "statistics": stats, "files": {x.name: prep.file_meta(x) for x in folder.iterdir()}})
            report[kind] = stats
            report["candidate_manifests"][kind] = prep.file_meta(folder / "provenance.json")
        prep.write_json(self.bundle / "preparation-report.json", report)

    def approved(self, kind):
        config = p.review_template(kind)
        config["metadata"]["license"] = "cc-by-4.0"
        for item in config["review"].values():
            item.update(approved=True, evidence="fixture")
        for name in config["public_provenance"]:
            config["public_provenance"][name] = "fixture"
        return config

    def test_offline_plan_blocked_and_no_remote(self):
        with patch.object(p.zen, "create_new_deposition") as create:
            plan = p.plan_release(self.bundle, "faprotax", "sandbox",
                                  p.review_template("faprotax"), self.root / "plan")
            create.assert_not_called()
        self.assertTrue(plan["blockers"])
        with self.assertRaises(ValueError):
            p.verify_plan(self.root / "plan", plan["plan_sha256"])

    def test_approved_plan_and_tamper_detection(self):
        folder = self.root / "plan"
        plan = p.plan_release(self.bundle, "funguild", "sandbox",
                              self.approved("funguild"), folder)
        self.assertFalse(plan["blockers"])
        self.assertEqual(p.verify_plan(folder, plan["plan_sha256"]), plan)
        with self.assertRaises(ValueError):
            p.verify_plan(folder, "wrong")
        (folder / "files" / "guild_profiles.csv").write_text("changed")
        with self.assertRaises(ValueError):
            p.verify_plan(folder, plan["plan_sha256"])

    def test_candidate_hash_mismatch(self):
        (self.bundle / "faprotax" / "functional_profiles.csv").write_text("changed")
        with self.assertRaises(ValueError):
            p.plan_release(self.bundle, "faprotax", "sandbox",
                           p.review_template("faprotax"), self.root / "plan")

    def test_remote_checksum_verification(self):
        expected = {"a.csv": {"md5": "abc", "bytes": 7}}
        with patch.object(p.zen, "list_deposition_files",
                          return_value=[{"filename": "a.csv", "checksum": "md5:abc", "filesize": 7}]):
            p.verify_remote_files("base", "token", "1", expected)
        with patch.object(p.zen, "list_deposition_files",
                          return_value=[{"filename": "a.csv", "checksum": "md5:bad", "filesize": 7}]):
            with self.assertRaises(ValueError):
                p.verify_remote_files("base", "token", "1", expected)

    def test_stage_does_not_publish_and_state_separate(self):
        folder = self.root / "plan"
        plan = p.plan_release(self.bundle, "faprotax", "sandbox",
                              self.approved("faprotax"), folder)
        state = self.root / "state"
        draft = {"id": 123, "submitted": False,
                 "links": {"bucket": "https://sandbox.zenodo.org/api/files/abc"}}
        record = {"metadata": plan["metadata"], "submitted": False}
        with (
            patch.object(p.zen, "env_or_config", return_value="token"),
            patch.object(p.zen, "create_new_deposition", return_value=draft),
            patch.object(p.zen, "update_metadata"),
            patch.object(p.zen, "upload_file_to_bucket"),
            patch.object(p, "verify_remote_files"),
            patch.object(p, "get_record", return_value=record),
            patch.object(p.zen, "publish_deposition") as publish,
        ):
            result = p.run_remote("stage", folder, plan["plan_sha256"], state, self.root / "env")
            publish.assert_not_called()
            self.assertEqual(result["status"], "staged")
            with self.assertRaises(ValueError):
                p.run_remote("stage", folder, plan["plan_sha256"], state, self.root / "env")
        self.assertTrue((state / "faprotax-sandbox.json").exists())
        self.assertFalse((state / "funguild-sandbox.json").exists())

    def test_publish_records_doi_and_skips_identical_release(self):
        folder = self.root / "plan"
        plan = p.plan_release(self.bundle, "funguild", "sandbox",
                              self.approved("funguild"), folder)
        state = self.root / "state"
        state.mkdir()
        content = p.digest({"files": plan["files"], "metadata": plan["metadata"]})
        p.atomic_state(state / "funguild-sandbox.json", {
            "dataset": "funguild", "target": "sandbox", "published": None,
            "pending": {"status": "staged", "id": "123",
                        "plan_sha256": plan["plan_sha256"], "content_sha256": content},
            "history": [],
        })
        draft = {"metadata": plan["metadata"], "submitted": False}
        published = {"metadata": plan["metadata"], "submitted": True, "doi": "10.5281/zenodo.123",
                     "conceptrecid": "122", "conceptdoi": "10.5281/zenodo.122",
                     "links": {"html": "https://sandbox.zenodo.org/records/123"}}
        with (
            patch.object(p.zen, "env_or_config", return_value="token"),
            patch.object(p, "get_record", side_effect=[draft, published]),
            patch.object(p, "verify_remote_files"),
            patch.object(p.zen, "publish_deposition") as publish,
            patch.object(p.zen, "create_new_deposition") as create,
        ):
            result = p.run_remote("publish", folder, plan["plan_sha256"], state, self.root / "env")
            publish.assert_called_once()
            self.assertEqual(result["doi"], "10.5281/zenodo.123")
            result = p.run_remote("stage", folder, plan["plan_sha256"], state, self.root / "env")
            self.assertEqual(result["status"], "unchanged")
            create.assert_not_called()
        saved = prep.read_json(state / "funguild-sandbox.json")
        self.assertIsNone(saved["pending"])
        self.assertEqual(len(saved["history"]), 1)

    def test_version_self_link_requires_explicit_confirmation(self):
        base = "https://sandbox.zenodo.org/api/deposit/depositions"
        prior = {"links": {"latest_draft": f"{base}/123"}}
        with self.assertRaisesRegex(ValueError, "confirm-no-existing-draft"):
            p.check_version_links(prior, base, "123")
        p.check_version_links(prior, base, "123", True)
        p.check_version_links({"links": {}}, base, "123")
        for url in (f"{base}/456", "https://example.org/api/deposit/depositions/123"):
            with self.assertRaisesRegex(ValueError, "different draft link"):
                p.check_version_links({"links": {"latest_draft": url}}, base, "123", True)

    def test_version_stage_from_published_self_link(self):
        folder = self.root / "plan"
        config = self.approved("faprotax")
        config["metadata"]["version"] = "1.1"
        plan = p.plan_release(self.bundle, "faprotax", "sandbox", config, folder)
        state = self.root / "state"
        state.mkdir()
        previous = {"id": "123", "conceptrecid": "122", "content_sha256": "old"}
        state_path = state / "faprotax-sandbox.json"
        p.atomic_state(state_path, {
            "dataset": "faprotax", "target": "sandbox", "published": previous,
            "pending": None, "history": [previous],
        })
        base = "https://sandbox.zenodo.org/api/deposit/depositions"
        prior = {"id": 123, "submitted": True, "conceptrecid": "122",
                 "links": {"latest_draft": f"{base}/123"}}
        draft = {"id": 456, "submitted": False, "conceptrecid": "122",
                 "links": {"bucket": "https://sandbox.zenodo.org/api/files/abc"}}
        with (
            patch.object(p.zen, "env_or_config", return_value="token"),
            patch.object(p, "get_record", return_value=prior),
            patch.object(p.zen, "create_new_version_draft", return_value=draft) as version,
        ):
            with self.assertRaisesRegex(ValueError, "confirm-no-existing-draft"):
                p.run_remote("stage", folder, plan["plan_sha256"], state, self.root / "env")
            version.assert_not_called()
            self.assertIsNone(prep.read_json(state_path)["pending"])
        with (
            patch.object(p.zen, "env_or_config", return_value="token"),
            patch.object(p, "get_record", side_effect=[prior, {"metadata": plan["metadata"]}]),
            patch.object(p.zen, "create_new_version_draft", return_value=draft) as version,
            patch.object(p.zen, "clear_deposition_files") as clear,
            patch.object(p.zen, "update_metadata"),
            patch.object(p.zen, "upload_file_to_bucket"),
            patch.object(p, "verify_remote_files"),
            patch.object(p.zen, "publish_deposition") as publish,
        ):
            result = p.run_remote("stage", folder, plan["plan_sha256"], state,
                                  self.root / "env", confirm_no_existing_draft=True)
            version.assert_called_once_with(base, "token", "123")
            clear.assert_called_once_with(base, "token", "456")
            publish.assert_not_called()
        self.assertEqual(result["id"], "456")
        self.assertEqual(result["status"], "staged")
        self.assertEqual(prep.read_json(state_path)["published"], previous)

    def test_both_funding_awards_required_and_verified(self):
        config = self.approved("faprotax")
        self.assertEqual(len(config["metadata"]["grants"]), 2)
        remote = {**config["metadata"], "grants": config["metadata"]["grants"][:1]}
        with self.assertRaisesRegex(ValueError, "funding"):
            p.verify_metadata({"metadata": remote}, config["metadata"])
        config["metadata"]["grants"] = remote["grants"]
        self.assertIn("Expected both ECHO funding awards", p.review_blockers("faprotax", config))

    def test_restage_reuses_unpublished_draft(self):
        folder = self.root / "plan"
        plan = p.plan_release(self.bundle, "faprotax", "sandbox",
                              self.approved("faprotax"), folder)
        state = self.root / "state"
        state.mkdir()
        p.atomic_state(state / "faprotax-sandbox.json", {
            "dataset": "faprotax", "target": "sandbox", "published": None,
            "pending": {"status": "staged", "id": "123", "plan_sha256": "old"},
            "history": [],
        })
        draft = {"id": 123, "submitted": False, "metadata": plan["metadata"],
                 "links": {"bucket": "https://sandbox.zenodo.org/api/files/abc"}}
        with (
            patch.object(p.zen, "env_or_config", return_value="token"),
            patch.object(p, "get_record", return_value=draft),
            patch.object(p.zen, "list_deposition_files",
                         return_value=[{"filename": name} for name in plan["files"]]),
            patch.object(p.zen, "update_metadata"),
            patch.object(p.zen, "upload_file_to_bucket"),
            patch.object(p, "verify_remote_files"),
            patch.object(p.zen, "create_new_deposition") as create,
            patch.object(p.zen, "publish_deposition") as publish,
        ):
            with self.assertRaises(ValueError):
                p.run_remote("restage", folder, plan["plan_sha256"], state, self.root / "env", "wrong")
            result = p.run_remote("restage", folder, plan["plan_sha256"], state, self.root / "env", "old")
            self.assertEqual(result["id"], "123")
            self.assertEqual(result["status"], "staged")
            create.assert_not_called()
            publish.assert_not_called()

    def test_data_curator_is_supported_and_verified(self):
        config = self.approved("faprotax")
        config["metadata"]["contributors"] = [
            {"name": "Example, Curator", "type": "DataCurator"}
        ]
        self.assertFalse(p.review_blockers("faprotax", config))
        p.verify_metadata({"metadata": config["metadata"]}, config["metadata"])
        remote = {**config["metadata"], "contributors": []}
        with self.assertRaisesRegex(ValueError, "contributor"):
            p.verify_metadata({"metadata": remote}, config["metadata"])
        config["metadata"]["contributors"][0].pop("type")
        self.assertIn("Invalid contributor metadata", p.review_blockers("faprotax", config))


if __name__ == "__main__":
    unittest.main()
