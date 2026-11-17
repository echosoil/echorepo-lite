#!/usr/bin/env python3
"""Reproducible CPU-oriented benchmark for sample-column TSV split planning.

Example (from repository root):
    python -m tools.benchmark_biodiversity_split --rows 2000 --samples 1000

The synthetic matrix is sparse and contains integral XYZ.0 abundance values.
This only benchmarks the taxonomy + first scan/plan, not compression or disk
throughput for a real 25 GiB input; use the progress logs for that comparison.
"""
from __future__ import annotations

import argparse
import tempfile
import time
from pathlib import Path

from tools.prepare_split_biodiversity_for_echorepo_premin10 import convert


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=2000)
    parser.add_argument("--samples", type=int, default=1000)
    args = parser.parse_args()
    if args.rows < 1 or not 1 <= args.samples <= 100000:
        parser.error("rows must be positive and samples must be 1..100000")

    with tempfile.TemporaryDirectory() as folder:
        root = Path(folder)
        table, taxa, output = (
            root / "table.tsv", root / "taxa.txt", root / "out.csv"
        )
        labels = [f"ABCD-{i:04d}-16S" for i in range(args.samples)]
        with table.open("w", encoding="utf-8", newline="") as handle:
            handle.write("#OTU ID\t" + "\t".join(labels) + "\n")
            for row in range(args.rows):
                values = [
                    "1.0" if (row * 17 + col * 13) % 97 == 0 else "0.0"
                    for col in range(args.samples)
                ]
                handle.write(f"otu{row}\t" + "\t".join(values) + "\n")
        with taxa.open("w", encoding="utf-8") as handle:
            for row in range(args.rows):
                handle.write(f"otu{row}\tX|Bacteria;Firmicutes\n")

        size_mib = table.stat().st_size / 1024**2
        print(f"Input: {args.rows:,} rows x {args.samples:,} samples "
              f"({size_mib:.2f} MiB)")

        for label, fast_tsv in (("csv.reader", False), ("--fast-tsv", True)):
            start = time.perf_counter()
            report = convert(
                table_path=table,
                taxonomy_path=taxa,
                output_path=output,
                marker="16S",
                fail_on_missing_taxonomy=True,
                max_output_bytes=1_000_000_000,
                min_total_reads=10,
                list_parts=True,
                progress_seconds=0,
                fast_tsv=fast_tsv,
            )
            elapsed = time.perf_counter() - start
            print(
                f"{label:11s}  {elapsed:7.3f}s  "
                f"{size_mib / elapsed:8.1f} MiB/s  "
                f"{args.rows / elapsed:10,.0f} rows/s  "
                f"kept={report['retained_otu_rows']:,}  "
                f"excluded={report['excluded_below_min_total']:,}"
            )


if __name__ == "__main__":
    main()
