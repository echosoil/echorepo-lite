"""Offline checks for candidate numerical tables and CSVW metadata."""

import tempfile
import unittest
from pathlib import Path

from tools import prepare_guild_publication as p


class GuildPreparationTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.out = self.root / "out"
        self.out.mkdir()

    def test_fap_preserves_overlap_zeros_and_read_coverage(self):
        (self.root / p.FAP8).write_text(",f1,f2\nA,0.7,0.8\nB,0,0\n")
        (self.root / p.FAP9).write_text("Sample,Annotated_frac,Annotated_pct\nB,0,0\nA,0.42,42\n")
        stats = p.fap_tables(self.root, self.out)
        self.assertEqual(stats["profile_rows"], 4)
        self.assertEqual(stats["overlapping_profile_rows"], 1)
        self.assertEqual(stats["zero_profiles"], 1)
        self.assertIn("A,f2,80.0", (self.out / "functional_profiles.csv").read_text())

        self.assertEqual((self.out / "annotation_coverage.csv").read_text().splitlines(),
                         ["sample_id,annotated_read_perc", "A,42", "B,0"])

    def test_fungal_percentage_conversion(self):
        manifest = self.fungal_fixture()
        p.fungal_tables(self.out, manifest)
        p.convert_fungal_percentages(self.out)
        profiles = p.records(self.out / "guild_profiles.csv",
                             ("sample_id", "guild", "read_count", "guild_perc"))
        self.assertEqual(p.number(profiles[0]["guild_perc"]), p.number("0.5"))
        coverage = p.records(self.out / "annotation_coverage.csv",
                             (*p.FUN_HEADERS[1][:-1], "assigned_perc"))
        self.assertEqual(coverage[1]["assigned_perc"], "")
        self.assertEqual(coverage[2]["assigned_perc"], "0")
        self.assertEqual(coverage[0]["assigned_reads"], "1")

    def test_fap_rejects_misalignment(self):
        (self.root / p.FAP8).write_text(",f1\nA,0.7\n")
        (self.root / p.FAP9).write_text("Sample,Annotated_frac,Annotated_pct\nB,0,0\n")
        with self.assertRaises(ValueError):
            p.fap_tables(self.root, self.out)

    def fungal_fixture(self):
        (self.out / "guild_definitions.csv").write_text("guild,primary_guild\nG,P\n")
        (self.out / "guild_profiles.csv").write_text("sample_id,guild,read_count,fraction\nA,G,1,0.005\n")
        (self.out / "annotation_coverage.csv").write_text(
            "sample_id,status,source_kind,total_reads,assigned_reads,unassigned_reads,assigned_fraction\n"
            "A,observed,legacy,200,1,199,0.005\nB,not_reconstructed,current_archive,,,,\n"
            "C,observed,legacy,10,0,10,0\n")
        return {"expected_samples": 3, "observed_samples": 2, "profile_rows": 1,
                "distinct_observed_guilds": 1, "status_counts": {"observed": 2, "not_reconstructed": 1}}

    def test_fungal_rare_guild_and_missing_coverage(self):
        stats = p.fungal_tables(self.out, self.fungal_fixture())
        self.assertEqual(stats["annotation_percent"]["max"], 0.5)
        self.assertEqual(stats["zero_annotation_samples"], 1)

    def test_fungal_rejects_inconsistent_count(self):
        manifest = self.fungal_fixture()
        (self.out / "guild_profiles.csv").write_text("sample_id,guild,read_count,fraction\nA,G,2,0.005\n")
        with self.assertRaises(ValueError):
            p.fungal_tables(self.out, manifest)

    def test_hash_mismatch(self):
        src = self.root / "input"
        src.write_text("original")
        meta = p.file_meta(src)
        src.write_text("changed")
        with self.assertRaises(ValueError):
            p.checked_copy(src, self.out / "input", meta)

    def test_csvw_keys(self):
        doc = p.metadata("faprotax")
        self.assertEqual(doc["tables"][0]["tableSchema"]["primaryKey"], ["sample_id", "function_code"])
        self.assertEqual(len(doc["tables"][0]["tableSchema"]["foreignKeys"]), 2)
        self.assertEqual(doc["tables"][0]["tableSchema"]["columns"][-1]["datatype"]["maximum"], 100)
        self.assertEqual(doc["tables"][0]["tableSchema"]["columns"][-1]["name"], "otu_perc")

    def test_complete_bundle_excludes_private_inventory(self):
        fap = self.root / "fap"
        fap.mkdir()
        (fap / p.FAP8).write_text(",f1\nA,0.7\n")
        (fap / p.FAP9).write_text("Sample,Annotated_frac,Annotated_pct\nA,0.42,42\n")
        audit = {"phase": "guild-publication-A", "report_commit": "audit",
                 "files": {p.FAP8: p.file_meta(fap / p.FAP8),
                           p.FAP9: p.file_meta(fap / p.FAP9), "FUNGuild_db.json": {"sha256": "ref"}},
                 "observations": {p.FAP8: {"rows": 1, "function_columns": 1}, p.FAP9: {"rows": 1}},
                 "faprotax_label_alignment": {"sets_match": True}, "input_inventory": {}}
        p.write_json(self.root / "audit.json", audit)
        manifest = self.fungal_fixture()
        (self.out / "source_inventory.local.json").write_text('{"private":true}')
        manifest.update(schema_version="0.1", marker="ITS", sample_filter_applied=False,
                        display_cutoff_applied=False, tracked_worktree_dirty=False,
                        reference_sha256="ref", reference_version=None, code_commit="analysis",
                        started_at_utc="start", generated_at_utc="end", confidence=["Probable"],
                        tie_rule="first", limitations=[], files={x.name: p.file_meta(x) for x in self.out.iterdir()})
        p.write_json(self.out / "provenance.local.json", manifest)
        dest = self.root / "bundle"
        report = p.prepare(fap, self.root / "audit.json", self.out, dest, "prepare")
        self.assertFalse(report["publication_ready"])
        self.assertFalse(list(dest.rglob("*.local.json")))
        self.assertTrue((dest / "faprotax" / "file.json").is_file())
        self.assertTrue((dest / "funguild" / "file.json").is_file())
        with self.assertRaises(FileExistsError):
            p.prepare(fap, self.root / "audit.json", self.out, dest, "prepare")


if __name__ == "__main__":
    unittest.main()
