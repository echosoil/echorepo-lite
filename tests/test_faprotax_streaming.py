"""Regression tests for memory-bounded FAPROTAX OTU count CSV generation."""
from __future__ import annotations

import csv
import tempfile
import tracemalloc
import unittest
from pathlib import Path

import pandas as pd

from tools.faprotax_streaming import write_dense_otu_csv


class FaprotaxStreamingTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.output = Path(self.temp.name) / "6_otu_clean_counts_no_blanks.csv"

    def test_matches_original_pandas_pivot_byte_for_byte(self):
        samples = ["ABCD-0001", "EFGH-0002", "WXYZ-0003"]
        # SQL joins can contain more than one contribution per OTU/sample.
        # The SELECT GROUP BY must combine them before writing.
        counts_df = pd.DataFrame(
            [
                ("ABCD-0001", "otu10", 2.0),
                ("EFGH-0002", "otu10", 5.0),
                ("ABCD-0001", "otu2", 3.0),
                ("WXYZ-0003", "otu2", 8.0),
                ("WXYZ-0003", "otu3", 4.0),
            ],
            columns=["sample_id", "otu_id", "count"],
        )
        expected = (
            counts_df.pivot_table(
                index="otu_id",
                columns="sample_id",
                values="count",
                aggfunc="sum",
                fill_value=0,
            )
            .sort_index()
            .reindex(columns=samples, fill_value=0)
            .to_csv()
        )

        sparse = sorted(
            (
                row.otu_id,
                row.sample_id,
                row.count,
            )
            for row in counts_df.itertuples(index=False)
        )
        stats = write_dense_otu_csv(
            self.output,
            sample_ids=samples,
            eligible_otu_ids=["otu10", "otu2", "otu3"],
            sorted_counts=iter(sparse),
        )

        self.assertEqual(self.output.read_text(encoding="utf-8"), expected)
        self.assertEqual(stats.otu_rows, 3)
        self.assertEqual(stats.nonzero_pairs, 5)
        self.assertEqual(stats.zero_samples, [])

    def test_zero_only_columns_and_csv_escaping(self):
        samples = ["ABCD-1234", "EFGH-5678", "IJKL-9012"]
        stats = write_dense_otu_csv(
            self.output,
            sample_ids=samples,
            eligible_otu_ids=['otu"two', "otu,one"],
            sorted_counts=[
                ('otu"two', "ABCD-1234", 0.25),
                ("otu,one", "ABCD-1234", 7.0),
            ],
        )
        with self.output.open(newline="", encoding="utf-8") as handle:
            rows = list(csv.reader(handle))
        self.assertEqual(
            rows,
            [
                ["otu_id", *samples],
                ['otu"two', "0.25", "0.0", "0.0"],
                ["otu,one", "7.0", "0.0", "0.0"],
            ],
        )
        self.assertEqual(stats.zero_samples, ["EFGH-5678", "IJKL-9012"])

    def test_invalid_input_is_rejected(self):
        samples = ["ABCD-1234", "EFGH-5678"]
        for records, eligible, message in [
            ([("otu1", "NOPE-0000", 1)], ["otu1"], "Unknown"),
            ([("otu2", "ABCD-1234", 1)], ["otu1"], "not aligned"),
            ([("otu1", "ABCD-1234", 1)], ["otu1", "otu2"], "Missing OTU"),
            ([
                ("otu1", "EFGH-5678", 1),
                ("otu1", "ABCD-1234", 2),
             ], ["otu1"], "Unsorted"),
            ([("otu1", "ABCD-1234", -1)], ["otu1"], "Invalid nonzero"),
        ]:
            with self.subTest(records=records):
                with self.assertRaisesRegex(ValueError, message):
                    write_dense_otu_csv(
                        self.output,
                        sample_ids=samples,
                        eligible_otu_ids=eligible,
                        sorted_counts=records,
                    )

    def test_iterates_large_sparse_rows_without_dense_matrix(self):
        # Generator cannot be rewound; rows are produced lazily. The writer
        # only holds one OTU row and the sample lookup at a time.
        samples = [f"ABCD-{i:04d}" for i in range(200)]
        eligible = [f"otu{i:05d}" for i in range(1200)]

        def sparse_rows():
            for i, otu_id in enumerate(eligible):
                yield otu_id, samples[i % len(samples)], float(i + 1)

        tracemalloc.start()
        try:
            result = write_dense_otu_csv(
                self.output,
                sample_ids=samples,
                eligible_otu_ids=eligible,
                sorted_counts=sparse_rows(),
            )
            _, peak = tracemalloc.get_traced_memory()
        finally:
            tracemalloc.stop()

        self.assertEqual(result.otu_rows, len(eligible))
        self.assertEqual(result.nonzero_pairs, len(eligible))
        self.assertLess(peak, 25 * 1024 * 1024)
        self.assertGreater(self.output.stat().st_size, 500_000)


if __name__ == "__main__":
    unittest.main()
