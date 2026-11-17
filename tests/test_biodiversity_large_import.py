"""Unit tests for the large biodiversity importer; no PostgreSQL needed."""
import hashlib
import tempfile
import unittest
from pathlib import Path

from echorepo.services.biodiversity_import import (
    _is_biodiversity_header,
    _open_biodiversity_rows,
)
from tools.import_biodiversity_large import (
    JoinedTsvSource,
    file_digest,
    joined_identity,
)


class LargeBiodiversityImportTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        root = Path(self.temp.name)
        self.table = root / "table_16S_dn_97.tsv"
        self.taxonomy = root / "taxa_16S_97.txt"
        self.table.write_text(
            "# QIIME2 comments\n"
            "#OTU ID\tABCD-1234-16S\tBPCR1-16S\tEFGH-5678-16S\tTaxonomy\n"
            "otu1\t6\t1000\t5\t\n"
            "otu2\t3\t999\t6\t\n"
            "otu3\t0\t3\t12\t\n",
            encoding="utf-8",
        )
        self.taxonomy.write_text(
            "otu1\tZ1|Bacteria;Acidobacteriota;C;O;F;G;S\n"
            "otu2\tZ2|Bacteria;Proteobacteria;C;O;F;G;S\n"
            "otu3\tZ3|Bacteria;Actinomycetota;C;O;F;G;S\n",
            encoding="utf-8",
        )
        self.scratch = root

    def _source(self, **kwargs):
        return JoinedTsvSource(
            table=self.table,
            taxonomy=self.taxonomy,
            marker="16S",
            min_total_reads=10,
            fail_on_missing_taxonomy=False,
            scratch_dir=self.scratch,
            **kwargs,
        )

    def test_streamed_join_filters_against_canonical_samples(self):
        source = self._source()
        header, rows, close, description = source.open_biodiversity_rows()
        try:
            self.assertTrue(_is_biodiversity_header(header))
            self.assertIn("streamed TSV", description)
            self.assertEqual(
                header[:3],
                ["OTU ID", "ABCD-1234-16S", "EFGH-5678-16S"],
            )
            data = list(rows)
            self.assertEqual([r[0] for r in data], ["otu1", "otu3"])
            self.assertEqual([r.source_row_number for r in data], [3, 5])
            self.assertEqual(data[0][1:3], ["6", "5"])
            self.assertEqual(data[0][4], "Acidobacteriota")
            self.assertEqual(
                source.source_sample_column_numbers["EFGH-5678-16S"], 4
            )
        finally:
            close()
        self.assertFalse(list(self.scratch.glob("echorepo-biodiv-tax-*")))
        source.assert_unchanged()

    def test_negative_reads_fail(self):
        self.table.write_text(
            "#OTU ID\tABCD-1234-16S\n"
            "otu1\t-1\n",
            encoding="utf-8",
        )
        src = self._source()
        _header, rows, close, _desc = src.open_biodiversity_rows()
        try:
            with self.assertRaisesRegex(ValueError, "Invalid abundance"):
                list(rows)
        finally:
            close()

    def test_strict_missing_taxonomy_fails(self):
        self.taxonomy.write_text(
            "other\tZ1|Bacteria;Acidobacteriota\n",
            encoding="utf-8",
        )
        src = JoinedTsvSource(
            self.table, self.taxonomy, "16S", 10, True, self.scratch
        )
        _header, rows, close, _desc = src.open_biodiversity_rows()
        try:
            with self.assertRaisesRegex(ValueError, "No taxonomy found"):
                list(rows)
        finally:
            close()

    def test_conflicting_duplicate_taxonomy_fails(self):
        self.taxonomy.write_text(
            "otu1\tZ1|Bacteria;Acidobacteriota\n"
            "otu1\tZ2|Bacteria;Proteobacteria\n",
            encoding="utf-8",
        )
        with self.assertRaisesRegex(ValueError, "Conflicting taxonomy"):
            self._source().open_biodiversity_rows()

    def test_direct_file_opens_without_loading_whole_source(self):
        joined = self.scratch / "joined.tsv"
        joined.write_text(
            "OTU ID\tABCD-1234-16S\tKingdom\tPhylum\n"
            "otu1\t12\tBacteria\tAcidobacteriota\n",
            encoding="utf-8",
        )
        header, rows, close, _desc = _open_biodiversity_rows(
            joined, joined.name
        )
        try:
            self.assertTrue(_is_biodiversity_header(header))
            self.assertEqual(next(rows)[1], "12")
        finally:
            close()
        self.assertEqual(
            file_digest(joined), hashlib.sha256(joined.read_bytes()).digest()
        )

    def test_composite_identity_changes_with_taxonomy_and_settings(self):
        a = joined_identity(self.table, self.taxonomy, "16S", 10)
        self.assertEqual(a, joined_identity(
            self.table, self.taxonomy, "16S", 10
        ))
        self.assertNotEqual(
            a, joined_identity(self.table, self.taxonomy, "16S", 0)
        )
        self.taxonomy.write_text(
            self.taxonomy.read_text() + "otu4\tZ4|Bacteria;Bacillota\n"
        )
        self.assertNotEqual(
            a, joined_identity(self.table, self.taxonomy, "16S", 10)
        )


if __name__ == "__main__":
    unittest.main()
