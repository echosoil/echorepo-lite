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
