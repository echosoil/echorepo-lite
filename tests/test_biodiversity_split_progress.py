"""Progress reporting for long, streaming sample-column biodiversity splits."""
from __future__ import annotations

import contextlib
import io
import tempfile
import unittest
from pathlib import Path

from tools.prepare_split_biodiversity_for_echorepo_premin10 import convert


class BiodiversitySplitProgressTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        root = Path(self.tmp.name)
        self.table = root / "abundance.tsv"
        self.taxonomy = root / "taxonomy.txt"
        self.output = root / "parts.csv"
        sample_columns = ["ABCD-1234-16S", "EFGH-5678-16S"]
        with self.table.open("w", encoding="utf-8") as table:
            table.write("# Constructed from biom file\n")
            table.write("#OTU ID\t" + "\t".join(sample_columns) + "\n")
            for index in range(192):
                table.write(f"otu{index}\t12\t0\n")

        with self.taxonomy.open("w", encoding="utf-8") as tax:
            for index in range(192):
                tax.write(
                    f"otu{index}\tZ1|Bacteria;Acidobacteriota;C;O;F;G;S\n"
                )

    def convert(self, *, list_parts=False, progress_seconds=0.000001):
        return convert(
            self.table,
            self.taxonomy,
            self.output,
            "16S",
            True,
            500000,
            10,
            compression="gzip",
            list_parts=list_parts,
            progress_seconds=progress_seconds,
        )

    def test_progress_emitted_for_taxonomy_scanning_and_writing(self):
        output = io.StringIO()
        with contextlib.redirect_stderr(output):
            result = self.convert()
        log = output.getvalue()
        self.assertIn("Taxonomy: started", log)
        self.assertIn("Taxonomy: 100.0% complete", log)
        self.assertIn("Pass 1/2 (scan/filter): started", log)
        self.assertIn("Pass 1/2 (scan/filter): 100.0% complete", log)
        self.assertIn("Pass 2/2 (write parts): started", log)
        self.assertIn("Pass 2/2 (write parts): 100.0% complete", log)
        self.assertIn("Split plan:", log)
        self.assertIn("ETA~", log)
        self.assertIn("rows=192", log)
        self.assertIn("GiB", log)
        self.assertEqual(result["retained_otu_rows"], 192)
        self.assertTrue(Path(result["output_parts"][0]["path"]).exists())

    def test_list_parts_skips_write_and_progress_can_be_disabled(self):
        output = io.StringIO()
        with contextlib.redirect_stderr(output):
            result = self.convert(list_parts=True)
        self.assertIn("mode=plan only", output.getvalue())
        self.assertNotIn("Pass 2/2", output.getvalue())
        self.assertFalse(list(self.output.parent.glob("*_part*.csv.gz")))
        self.assertTrue(all(not part["written"] for part in result["output_parts"]))

        output = io.StringIO()
        with contextlib.redirect_stderr(output):
            self.convert(list_parts=True, progress_seconds=0)
        self.assertEqual(output.getvalue(), "")

    def test_negative_progress_interval_rejected(self):
        with self.assertRaisesRegex(ValueError, "progress-seconds"):
            self.convert(list_parts=True, progress_seconds=-1)


if __name__ == "__main__":
    unittest.main()
