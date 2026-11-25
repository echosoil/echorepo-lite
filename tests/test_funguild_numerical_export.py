"""Offline numerical export checks and plot equivalence."""

import csv
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from tools import export_funguild_profiles as exporter
from tools import generate_biodiversity_piecharts as guild


class FungalExportTests(unittest.TestCase):
    def setUp(self):
        self.frame = pd.DataFrame([
            {"sample_id": "ABCD-1234", "count": 80, "taxa": {"Genus": "FungusA"}},
            {"sample_id": "ABCD-1234", "count": 1, "taxa": {"Genus": "FungusB"}},
            {"sample_id": "ABCD-1234", "count": 119, "taxa": {}},
            {"sample_id": "UNKN-0001", "count": 10, "taxa": {}},
            {"sample_id": "ZERO-0001", "count": 0, "taxa": {}},
        ])
        self.annotations = {"FungusA": {"macro": "Plant pathogens"},
                            "FungusB": {"macro": "Endophytes"}}
        self.inventory = {ident: {"source_kind": "legacy", "source_status": "reconstructed"}
                          for ident in self.frame["sample_id"].unique()}
        self.inventory["MISS-0001"] = {"source_kind": "current_archive",
                                        "source_status": "not_reconstructed"}

    def test_rare_guild_and_plot_equivalence(self):
        profiles, coverage, _ = exporter.build_tables(
            self.frame, self.annotations, self.inventory)
        self.assertEqual(len(profiles), 2)
        by_guild = {r["guild"]: r for r in profiles}
        self.assertAlmostEqual(by_guild["Endophytes"]["fraction"], .005)
        by_id = {r["sample_id"]: r for r in coverage}
        row = by_id["ABCD-1234"]
        self.assertEqual(row["total_reads"], 200)
        self.assertEqual(row["assigned_reads"], 81)
        self.assertEqual(row["unassigned_reads"], 119)
        self.assertAlmostEqual(row["assigned_fraction"], .405)
        displayed, pct = guild.summarize_fungal_guilds(
            self.frame[self.frame["sample_id"] == "ABCD-1234"], self.annotations)
        self.assertEqual(list(displayed["macro"]), ["Plant pathogens"])
        self.assertAlmostEqual(pct, row["assigned_fraction"] * 100)
        self.assertEqual(by_id["UNKN-0001"]["assigned_fraction"], 0)
        self.assertIsNone(by_id["ZERO-0001"]["assigned_fraction"])
        self.assertIsNone(by_id["MISS-0001"]["total_reads"])

    def test_missing_current_not_replaced_by_legacy(self):
        legacy = self.frame.iloc[[0, 3]].copy()
        with (
            patch.object(guild, "fetch_current_fungal_data_from_archives",
                         return_value=(legacy.iloc[0:0], {"ABCD-1234"}, set())),
            patch.object(guild, "fetch_otu_data", return_value=legacy),
        ):
            inventory = {"ABCD-1234": {"source_kind": "current_archive",
                                        "source_status": "not_reconstructed"}}
            frame = guild.fetch_fungal_guild_source_data(None, source_inventory=inventory)
        self.assertEqual(set(frame["sample_id"]), {"UNKN-0001"})
        self.assertEqual(inventory["ABCD-1234"]["source_status"], "not_reconstructed")

    def test_bundle_and_hashes(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "bundle"
            summary = exporter.export(out, self.frame, self.annotations, self.inventory, {})
            self.assertEqual(summary["observed_samples"], 2)
            self.assertFalse(summary["publication_ready"])
            for name, meta in summary["files"].items():
                self.assertEqual(exporter.sha256(out / name), meta["sha256"])
            with (out / "annotation_coverage.csv").open(newline="") as stream:
                self.assertEqual(len(list(csv.DictReader(stream))), 4)
            self.assertEqual(json.loads((out / "provenance.local.json").read_text()), summary)
            with self.assertRaises(FileExistsError):
                exporter.export(out, self.frame, self.annotations, self.inventory, {})

    def test_invalid_count_is_rejected(self):
        self.frame.loc[0, "count"] = -1
        with self.assertRaises(ValueError):
            exporter.build_tables(self.frame, self.annotations, self.inventory)

    def test_requested_missing_sample_is_reported(self):
        empty = self.frame.iloc[0:0]
        _, rows, _ = exporter.build_tables(empty, self.annotations, {}, {"NONE-0001"})
        self.assertEqual(rows[0]["status"], "not_found")
        self.assertIsNone(rows[0]["total_reads"])


if __name__ == "__main__":
    unittest.main()
