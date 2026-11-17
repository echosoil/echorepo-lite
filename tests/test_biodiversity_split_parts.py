"""Test sample-column splitting for sequential archive-free imports."""
import csv
import gzip
import tempfile
import unittest
from pathlib import Path

from tools.prepare_split_biodiversity_for_echorepo_premin10 import convert


class SamplePartTests(unittest.TestCase):
    def test_list_and_one_part_are_deterministic_and_non_overlapping(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            table = root / "input.tsv"
            taxonomy = root / "tax.txt"
            output = root / "joined.csv"
            samples = [f"AAAA-{i:04d}-16S" for i in range(6)]
            with table.open("w", encoding="utf-8") as f:
                f.write("# Constructed from biom file\n")
                f.write("#OTU ID\t" + "\t".join(samples) + "\n")
                for i in range(12):
                    # Every OTU totals >= 10 across the complete matrix.
                    f.write(f"otu{i}\t" + "\t".join(["2"] * 6) + "\n")
                # Excluded OTU totals 6: one read in each sample.
                f.write("low\t" + "\t".join(["1"] * 6) + "\n")
            taxonomy.write_text(
                "".join(
                    f"otu{i}\tZ1|Bacteria;Acidobacteriota;Class;Order;Family;Genus;Species\n"
                    for i in range(12)
                )
                + "low\tZ1|Bacteria;Acidobacteriota\n",
                encoding="utf-8",
            )
            kwargs = dict(
                table_path=table,
                taxonomy_path=taxonomy,
                output_path=output,
                marker="16S",
                fail_on_missing_taxonomy=True,
                max_output_bytes=950,
                min_total_reads=10,
            )
            plan = convert(**kwargs, list_parts=True)
            parts = plan["output_parts"]
            self.assertGreater(len(parts), 1)
            self.assertEqual(list(root.glob("*part*.csv")), [])
            self.assertEqual(plan["excluded_below_min_total"], 1)

            all_headers = []
            for number in range(1, len(parts) + 1):
                result = convert(**kwargs, only_part=number)
                present = [p for p in result["output_parts"] if p["written"]]
                self.assertEqual([p["part"] for p in present], [number])
                path = Path(present[0]["path"])
                self.assertLessEqual(path.stat().st_size, 950)
                with path.open(newline="") as stream:
                    reader = csv.reader(stream)
                    header = next(reader)
                    self.assertEqual(
                        header[-7:],
                        ["Kingdom", "Phylum", "Class", "Order", "Family",
                         "Genus", "Species"],
                    )
                    got = list(reader)
                    self.assertEqual(len(got), 12)
                    self.assertEqual([row[0] for row in got],
                                     [f"otu{i}" for i in range(12)])
                all_headers.extend(header[1:-7])
            self.assertEqual(sorted(all_headers), sorted(samples))
            self.assertEqual(len(set(all_headers)), len(samples))
            self.assertEqual(
                len(list(root.glob("joined_part*.csv"))), len(parts)
            )

    def test_compressed_part_matches_plain_csv_and_is_reproducible(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            table = root / "input.tsv"
            taxonomy = root / "taxa.txt"
            output = root / "joined.csv"
            table.write_text(
                "# Constructed from biom file\n"
                "#OTU ID\tABCD-1234-16S\tEFGH-5678-16S\n"
                "otu1\t11\t5\notu2\t0\t8\n",
                encoding="utf-8",
            )
            taxonomy.write_text(
                "otu1\tZ1|Bacteria;Acidobacteriota\n"
                "otu2\tZ1|Bacteria;Bacillota\n",
                encoding="utf-8",
            )
            kwargs = dict(
                table_path=table, taxonomy_path=taxonomy,
                output_path=output, marker="16S",
                fail_on_missing_taxonomy=True,
                max_output_bytes=10000, min_total_reads=10,
            )
            plan = convert(**kwargs, compression="gzip", list_parts=True)
            self.assertEqual(plan["compression"], "gzip")
            self.assertFalse(list(root.glob("*.csv.gz")))

            plain = convert(**kwargs, only_part=1)
            plain_bytes = Path(plain["output_parts"][0]["path"]).read_bytes()
            compressed = convert(**kwargs, only_part=1, compression="gzip")
            part = Path(compressed["output_parts"][0]["path"])
            self.assertTrue(part.name.endswith("_part001.csv.gz"))
            self.assertTrue(compressed["output_parts"][0]["written"])
            self.assertEqual(compressed["output_parts"][0]["compression"], "gzip")
            with gzip.open(part, "rb") as stream:
                self.assertEqual(stream.read(), plain_bytes)
            first_bytes = part.read_bytes()
            convert(**kwargs, only_part=1, compression="gzip")
            self.assertEqual(part.read_bytes(), first_bytes)
            self.assertEqual(list(root.glob("*.tmp")), [])

    def test_invalid_part_does_not_delete_existing_part(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            table = root / "x.tsv"
            taxonomy = root / "tax.txt"
            output = root / "joined.csv"
            table.write_text(
                "#OTU ID\tABCD-1234-16S\n"
                "otu1\t20\n", encoding="utf-8"
            )
            taxonomy.write_text(
                "otu1\tZ1|Bacteria;Acidobacteriota\n", encoding="utf-8"
            )
            args = dict(
                table_path=table, taxonomy_path=taxonomy, output_path=output,
                marker="16S", fail_on_missing_taxonomy=True,
                max_output_bytes=300, min_total_reads=10
            )
            result = convert(**args, only_part=1)
            part_path = Path(result["output_parts"][0]["path"])
            before = part_path.read_bytes()
            with self.assertRaisesRegex(ValueError, "out of range"):
                convert(**args, only_part=2)
            self.assertEqual(part_path.read_bytes(), before)


if __name__ == "__main__":
    unittest.main()
