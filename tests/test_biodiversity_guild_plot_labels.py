"""Regression tests for biodiversity guild-plot percentages and coverage labels."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import matplotlib
matplotlib.use("Agg")
import pandas as pd
from matplotlib.figure import Figure

from tools import generate_biodiversity_piecharts as guild


class GuildPlotCoverageTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.folder = Path(self.temp.name)

    def _write_coverage(self, text):
        path = self.folder / "9_faprotax_annotated_fraction.csv"
        path.write_text(text, encoding="utf-8")
        return path

    def test_faprotax_read_coverage_from_file_9(self):
        path = self._write_coverage(
            "Sample,Annotated_frac,Annotated_pct\n"
            "ABCD-1234,0.425,42.5\n"
            "WXYZ-0001,0.0,0\n"
        )
        with patch.object(guild, "FAPROTAX_COVERAGE_CSV", str(path)):
            result = guild.load_faprotax_read_coverage(
                {"ABCD-1234", "WXYZ-0001"}
            )
        self.assertEqual(result, {"ABCD-1234": 42.5, "WXYZ-0001": 0.0})

    def test_faprotax_coverage_missing_is_optional(self):
        with patch.object(
            guild, "FAPROTAX_COVERAGE_CSV", str(self.folder / "missing.csv")
        ):
            self.assertEqual(
                guild.load_faprotax_read_coverage({"ABCD-1234"}), {}
            )

    def test_faprotax_coverage_rejects_stale_sample_ids_and_invalid_values(self):
        path = self._write_coverage(
            "Sample,Annotated_frac,Annotated_pct\nABCD-1234,0.2,20\n"
        )
        with patch.object(guild, "FAPROTAX_COVERAGE_CSV", str(path)):
            with self.assertRaisesRegex(RuntimeError, "does not match current"):
                guild.load_faprotax_read_coverage({"ABCD-1234", "WXYZ-0001"})

        self._write_coverage(
            "Sample,Annotated_frac,Annotated_pct\nABCD-1234,1.1,110\n"
        )
        with patch.object(guild, "FAPROTAX_COVERAGE_CSV", str(path)):
            with self.assertRaisesRegex(ValueError, "0..100"):
                guild.load_faprotax_read_coverage({"ABCD-1234"})

    def test_fungal_read_coverage_is_before_display_threshold(self):
        sample = pd.DataFrame(
            [
                {"count": 80, "taxa": {"Genus": "FungusA"}},
                {"count": 1, "taxa": {"Genus": "FungusB"}},
                {"count": 119, "taxa": {"Genus": ""}},
            ]
        )
        annotations = {
            "FungusA": {"macro": "Plant pathogens"},
            "FungusB": {"macro": "Endophytes"},
        }
        displayed, coverage = guild.summarize_fungal_guilds(
            sample, annotations
        )
        # 81/200 reads have accepted guilds, including 0.5% hidden group.
        self.assertAlmostEqual(coverage, 40.5)
        self.assertEqual(set(displayed["macro"]), {"Plant pathogens"})
        self.assertAlmostEqual(float(displayed.iloc[0]["percent"]), 40.0)

    def test_bacterial_chart_labels_separate_otu_fraction_from_read_coverage(self):
        texts = []
        original = Figure.text

        def capture_text(fig, x, y, text, *args, **kwargs):
            texts.append(str(text))
            return original(fig, x, y, text, *args, **kwargs)

        output = self.folder / "bacterial.png"
        with patch.object(Figure, "text", capture_text):
            ok = guild.make_bacterial_guildplot_for_sample(
                sample_id="ABCD-1234",
                func_row=pd.Series(
                    {
                        "aerobic_chemoheterotrophy": 0.267,
                        "anaerobic_chemoheterotrophy": 0.204,
                    }
                ),
                out_path=output,
                read_coverage_pct=61.2,
            )

        self.assertTrue(ok)
        self.assertGreater(output.stat().st_size, 0)
        self.assertIn("61.2%", "\n".join(texts))
        self.assertIn("Bars sum % of present OTUs", "\n".join(texts))

    def test_fungal_chart_labels_accepted_annotation_coverage(self):
        texts = []
        original = Figure.text

        def capture_text(fig, x, y, text, *args, **kwargs):
            texts.append(str(text))
            return original(fig, x, y, text, *args, **kwargs)

        output = self.folder / "fungal.png"
        sample = pd.DataFrame(
            [
                {"count": 30, "taxa": {"Genus": "FungusA"}},
                {"count": 70, "taxa": {"Genus": "Unknown"}},
            ]
        )
        annotations = {"FungusA": {"macro": "Plant pathogens"}}
        with patch.object(Figure, "text", capture_text):
            ok = guild.make_fungal_guildplot_for_sample(
                sample_df=sample,
                sample_id="ABCD-1234",
                funguild_by_genus=annotations,
                out_path=output,
            )

        self.assertTrue(ok)
        self.assertGreater(output.stat().st_size, 0)
        self.assertIn("FUNGuild coverage: 30.0%", "\n".join(texts))
        self.assertIn("70.0% remain unassigned", "\n".join(texts))


if __name__ == "__main__":
    unittest.main()
