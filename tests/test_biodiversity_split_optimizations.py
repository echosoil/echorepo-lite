"""Regression tests for optimized sparse 16S/ITS TSV conversion."""
from __future__ import annotations

import csv
import gzip
import io
import tempfile
import unittest
from pathlib import Path

from tools.prepare_split_biodiversity_for_echorepo_premin10 import (
    _csv_field_size,
    _parse_abundance,
    _validated_abundance_field_size,
    convert,
)


class AbundanceFastPathTests(unittest.TestCase):
    def test_csv_field_sizes_match_legacy_csv_writer(self):
        values = [
            "", "0.0", "123456789.0", "12", "1e+6", " 0.0 ",
            'a,b', 'a"b', "two\nlines", "carriage\rreturn",
            "Crème brûlée", "汉字", "simple taxonomy",
        ]
        for value in values:
            with self.subTest(value=value):
                out = io.StringIO(newline="")
                csv.writer(out, lineterminator="\n").writerow([value])
                legacy = len(out.getvalue().encode("utf-8")) - 1
                self.assertEqual(_csv_field_size(value), legacy)

    def test_integer_and_fallback_abundances(self):
        examples = [
            ("0.0", 0), ("0", 0), ("000012.0", 12),
            ("-1.0", -1), ("+12.0", 12), ("9.0", 9),
            ("1.25", 1.25), ("1e2", 100.0),
            ("9007199254740993.0", 9007199254740993),
        ]
        for raw, expected in examples:
            with self.subTest(value=raw):
                self.assertEqual(_parse_abundance(raw), expected)
        with self.assertRaises(ValueError):
            _parse_abundance("not-a-number")

    def test_validated_abundance_sizing_preserves_empty_field(self):
        # Empty fields were previously counted as two bytes by the
        # one-field CSV writer. Preserve grouping decisions.
        self.assertEqual(_validated_abundance_field_size(""), 2)
        self.assertEqual(_validated_abundance_field_size("0.0"), 3)
        self.assertEqual(_validated_abundance_field_size("00123.0"), 7)


class SplitterEquivalenceTests(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        self.root = Path(self.tempdir.name)
        self.table = self.root / "table.tsv"
        self.taxonomy = self.root / "taxa.txt"
        self.samples = [f"ABCD-{i:04d}-16S" for i in range(1000, 1006)]
        with self.table.open("w", encoding="utf-8") as handle:
            handle.write("# Constructed from biom file\n")
            handle.write(
                "#OTU ID\t" + "\t".join(self.samples)
                + "\tNegative-control-16S\tTaxonomy\n"
            )
            for i in range(35):
                if i % 6 == 0:
                    counts = ["0.0"] * 6
                else:
                    counts = ["0.0"] * 6
                    counts[i % 6] = "11.0"
                    counts[(i + 2) % 6] = "2.0"
                if i == 3:
                    counts[0] = "1.25"  # generic floating-point fallback
                if i == 4:
                    counts[1] = "1e2"  # scientific notation fallback
                handle.write(
                    f"otu{i}\t" + "\t".join(counts) + "\tignored\t\n"
                )
        self.taxonomy.write_text(
            "".join(
                f"otu{i}\tZ1|Bacteria;Acidobacteriota;Class;Order;"
                "Family;Genus, complex;Espèce\n"
                for i in range(35)
            ),
            encoding="utf-8",
        )

    def _convert(self, output: Path, *, fast: bool, compression: str):
        return convert(
            table_path=self.table,
            taxonomy_path=self.taxonomy,
            output_path=output,
            marker="16S",
            fail_on_missing_taxonomy=True,
            max_output_bytes=1800,
            min_total_reads=10,
            compression=compression,
            fast_tsv=fast,
        )

    def test_standard_and_fast_tsv_byte_for_byte_equivalent(self):
        for compression in ("none", "gzip"):
            with self.subTest(compression=compression):
                regular = self._convert(
                    self.root / f"regular_{compression}.csv",
                    fast=False, compression=compression,
                )
                fast = self._convert(
                    self.root / f"fast_{compression}.csv",
                    fast=True, compression=compression,
                )
                self.assertEqual(
                    regular["retained_otu_rows"], fast["retained_otu_rows"]
                )
                self.assertEqual(
                    regular["excluded_below_min_total"],
                    fast["excluded_below_min_total"],
                )
                self.assertEqual(len(regular["output_parts"]), len(fast["output_parts"]))
                for original, optimized in zip(
                    regular["output_parts"], fast["output_parts"]
                ):
                    for key in (
                        "sample_count", "first_sample", "last_sample",
                        "uncompressed_size_bytes", "otu_rows", "size_bytes",
                    ):
                        self.assertEqual(original[key], optimized[key])
                    original_bytes = Path(original["path"]).read_bytes()
                    optimized_bytes = Path(optimized["path"]).read_bytes()
                    self.assertEqual(original_bytes, optimized_bytes)
                    if compression == "gzip":
                        self.assertEqual(
                            gzip.decompress(original_bytes),
                            gzip.decompress(optimized_bytes),
                        )

    def test_strict_fast_tsv_rejects_quoted_fields_safely(self):
        self.table.write_text(
            "#OTU ID\tABCD-1000-16S\n"
            'otu0\t"12.0"\n',
            encoding="utf-8",
        )
        normal = self._convert(
            self.root / "quoted_standard.csv", fast=False, compression="none"
        )
        self.assertEqual(normal["retained_otu_rows"], 1)

        with self.assertRaisesRegex(ValueError, "unquoted TSV"):
            self._convert(
                self.root / "quoted_fast.csv", fast=True, compression="none"
            )
        self.assertFalse(list(self.root.glob("quoted_fast_part*.csv")))


if __name__ == "__main__":
    unittest.main()
