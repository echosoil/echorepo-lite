"""Bounded-memory writer for FAPROTAX's dense OTU-by-sample CSV format.

A complete CSV is still required by the existing R/microeco workflow, but
we never materialize its OTU x sample array in Python. PostgreSQL supplies
only non-zero (OTU, sample, sum-of-reads) rows, sorted by OTU and sample.
"""
from __future__ import annotations

import csv
import itertools
import math
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class DenseCountsStats:
    otu_rows: int
    nonzero_pairs: int
    zero_samples: list[str]


def write_dense_otu_csv(
    path: Path,
    *,
    sample_ids: Sequence[str],
    eligible_otu_ids: Sequence[str],
    sorted_counts: Iterable[tuple[str, str, float]],
    on_progress: Callable[[int], None] | None = None,
) -> DenseCountsStats:
    """Write file 6 with O(samples + eligible OTU IDs) Python memory.

    The SQL cursor must yield (otu_id, sample_id, read_count) sorted in
    Python-compatible lexical order (PostgreSQL COLLATE "C"). Counts are
    already aggregated per OTU/sample. The eligible OTUs and sample columns
    come from the same database snapshot and are checked for exact alignment.

    Float rendering intentionally matches the old pandas float64 matrix,
    including the ".0" suffix on integral read counts.
    """
    samples = list(sample_ids)
    eligible = list(eligible_otu_ids)
    if not samples or not eligible:
        raise ValueError("FAPROTAX requires nonempty sample and OTU lists")
    if samples != sorted(set(samples)):
        raise ValueError("FAPROTAX sample columns must be unique and sorted")
    if eligible != sorted(set(eligible)):
        raise ValueError("FAPROTAX OTU IDs must be unique and sorted")

    sample_positions = {sample_id: i for i, sample_id in enumerate(samples)}
    sample_totals = [0.0] * len(samples)
    written = 0
    nonzero_pairs = 0
    expected = iter(eligible)

    with path.open("w", encoding="utf-8", newline="") as output:
        writer = csv.writer(output, lineterminator="\n")
        writer.writerow(["otu_id", *samples])

        for otu_id, abundance_rows in itertools.groupby(
            sorted_counts, key=lambda record: record[0]
        ):
            wanted = next(expected, None)
            if wanted != otu_id:
                raise ValueError(
                    f"OTU stream not aligned with eligible set: expected "
                    f"{wanted!r}, received {otu_id!r}"
                )

            counts = [0.0] * len(samples)
            last_position = -1

            for _otu_id, sample_id, raw_count in abundance_rows:
                position = sample_positions.get(sample_id)
                if position is None:
                    raise ValueError(f"Unknown FAPROTAX sample: {sample_id!r}")
                if position <= last_position:
                    raise ValueError(
                        f"Unsorted or duplicate sample {sample_id!r} "
                        f"for OTU {otu_id!r}"
                    )
                last_position = position

                count = float(raw_count)
                if not math.isfinite(count) or count <= 0:
                    raise ValueError(
                        f"Invalid nonzero count {raw_count!r} "
                        f"for {otu_id!r}/{sample_id!r}"
                    )
                counts[position] = count
                sample_totals[position] += count
                nonzero_pairs += 1

            writer.writerow([otu_id, *counts])
            written += 1

            if on_progress is not None and written % 5000 == 0:
                on_progress(written)

        remaining = next(expected, None)
        if remaining is not None:
            raise ValueError(
                f"Missing OTU(s) in streamed abundance data, first: {remaining!r}"
            )

    return DenseCountsStats(
        otu_rows=written,
        nonzero_pairs=nonzero_pairs,
        zero_samples=[
            sample_id for sample_id, total in zip(samples, sample_totals)
            if total <= 0
        ],
    )
