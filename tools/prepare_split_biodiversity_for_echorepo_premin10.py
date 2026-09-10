#!/usr/bin/env python3
"""
Convert the new split biodiversity format into an ECHOREPO importer-ready CSV.

Input:
  1) OTU abundance table, e.g. table_ITS_dn_97.tsv
  2) Taxonomy assignment file, e.g. taxa_ITS_97.txt

Output:
  OTU ID,<ECHOREPO sample columns>,Kingdom,Phylum,Class,Order,Family,Genus,Species

The script intentionally does NOT reproduce ECHOREPO's scientific cleaning
rules (taxonomy trash filtering, chloroplast/mitochondria filtering, total
reads < 10). Those remain in biodiversity_import.py so there is one source of
truth for data-cleaning policy.
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import re
import sys
from collections import Counter
from pathlib import Path
from typing import Iterable


SAMPLE_RE = re.compile(
    r"^[A-Za-z0-9]{4}-[A-Za-z0-9]{4,}-(16S|ITS)$",
    re.IGNORECASE,
)

RANK_PREFIXES = {
    "k": "Kingdom",
    "p": "Phylum",
    "c": "Class",
    "o": "Order",
    "f": "Family",
    "g": "Genus",
    "s": "Species",
}

OUTPUT_TAXONOMY_COLUMNS = [
    "Kingdom",
    "Phylum",
    "Class",
    "Order",
    "Family",
    "Genus",
    "Species",
]




def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Join a split OTU abundance table and taxonomy assignment file "
            "into an ECHOREPO biodiversity import CSV."
        )
    )
    parser.add_argument(
        "--table",
        required=True,
        help="OTU abundance TSV, e.g. table_ITS_dn_97.tsv",
    )
    parser.add_argument(
        "--taxonomy",
        required=True,
        help="Taxonomy assignment TXT/TSV, e.g. taxa_ITS_97.txt",
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Output CSV accepted by the ECHOREPO biodiversity importer",
    )
    parser.add_argument(
        "--marker",
        choices=("16S", "ITS"),
        help=(
            "Expected marker. If omitted, infer it from canonical ECHOREPO "
            "sample column names."
        ),
    )
    parser.add_argument(
        "--max-output-mb",
        type=float,
        default=150.0,
        help=(
            "Maximum size of each output CSV in decimal MB. Default: 150. "
            "Files are split by sample columns, while every OTU row is kept "
            "in every part."
        ),
    )
    parser.add_argument(
        "--min-total-reads",
        type=float,
        default=10.0,
        help=(
            "Remove an OTU before splitting when its total abundance across all "
            "canonical ECHOREPO sample columns is below this value. Default: 10. "
            "Use 0 to disable this preprocessing filter."
        ),
    )
    parser.add_argument(
        "--report-json",
        help="Optional path for a machine-readable conversion report",
    )
    parser.add_argument(
        "--fail-on-missing-taxonomy",
        action="store_true",
        help=(
            "Fail if an OTU in the abundance table has no taxonomy entry. "
            "By default it is written with blank taxonomy and ECHOREPO's "
            "importer can subsequently reject it."
        ),
    )
    return parser.parse_args()


def iter_nonempty_lines(path: Path) -> Iterable[tuple[int, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        for line_number, raw in enumerate(handle, start=1):
            line = raw.rstrip("\r\n")
            if line.strip():
                yield line_number, line


def clean_taxon_value(value: str) -> str:
    return value.strip()


def parse_taxonomy_annotation(annotation: str) -> dict[str, str]:
    """
    Parse strings such as:

      MT762711|k__Fungi;p__Ascomycota;c__Sordariomycetes;...|SH1063900.10FU

    Only the k__/p__/c__/o__/f__/g__/s__ part is used.
    Other fields in taxa_*.txt are intentionally ignored.
    """
    result = {column: "" for column in OUTPUT_TAXONOMY_COLUMNS}

    annotation = annotation.strip()
    if not annotation:
        return result

    # Taxonomy is normally the pipe-delimited chunk containing rank prefixes.
    # Parsing every semicolon token across all chunks also makes this tolerant
    # of small layout variations.
    for pipe_chunk in annotation.split("|"):
        for token in pipe_chunk.split(";"):
            token = token.strip()
            match = re.match(r"^([kpcofgs])__(.*)$", token, re.IGNORECASE)
            if not match:
                continue

            prefix = match.group(1).lower()
            value = clean_taxon_value(match.group(2))
            column = RANK_PREFIXES[prefix]

            if result[column] and result[column] != value:
                raise ValueError(
                    f"Conflicting {column} values inside taxonomy annotation: "
                    f"{result[column]!r} vs {value!r} in {annotation!r}"
                )
            result[column] = value

    return result


def load_taxonomy_file(
    path: Path,
) -> tuple[dict[str, dict[str, str]], dict[str, int]]:
    """
    taxa_*.txt is expected to be tab-separated and headerless.

    Column 1: OTU ID
    Column 2: hit/annotation containing taxonomy
    Remaining columns: ignored by this structural converter.
    """
    taxonomy_by_otu: dict[str, dict[str, str]] = {}
    duplicate_identical = 0
    no_rank_information = 0
    rows = 0

    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.reader(handle, delimiter="\t")

        for line_number, row in enumerate(reader, start=1):
            if not row or all(not value.strip() for value in row):
                continue
            if row[0].lstrip().startswith("#"):
                continue
            if len(row) < 2:
                raise ValueError(
                    f"{path}: line {line_number} has {len(row)} fields; "
                    "expected at least OTU ID and taxonomy/hit annotation."
                )

            rows += 1
            otu_id = row[0].strip()
            if not otu_id:
                raise ValueError(f"{path}: line {line_number} has an empty OTU ID")

            taxonomy = parse_taxonomy_annotation(row[1])
            if not any(taxonomy.values()):
                no_rank_information += 1

            previous = taxonomy_by_otu.get(otu_id)
            if previous is not None:
                if previous != taxonomy:
                    raise ValueError(
                        f"{path}: OTU {otu_id!r} occurs more than once with "
                        "different taxonomy assignments."
                    )
                duplicate_identical += 1
                continue

            taxonomy_by_otu[otu_id] = taxonomy

    if not taxonomy_by_otu:
        raise ValueError(f"{path}: no taxonomy rows were found")

    stats = {
        "taxonomy_rows_read": rows,
        "taxonomy_unique_otus": len(taxonomy_by_otu),
        "taxonomy_duplicate_identical_rows": duplicate_identical,
        "taxonomy_rows_without_rank_information": no_rank_information,
    }
    return taxonomy_by_otu, stats


def find_table_header(
    path: Path,
) -> tuple[int, list[str]]:
    """
    Locate the #OTU ID / OTU ID header while ignoring leading BIOM comments.
    """
    for line_number, line in iter_nonempty_lines(path):
        row = next(csv.reader([line], delimiter="\t"))
        if not row:
            continue
        first = row[0].strip().lstrip("#").strip().lower()
        if first in {"otu id", "otu_id", "feature id", "feature_id"}:
            header = [value.strip() for value in row]
            header[0] = "OTU ID"
            return line_number, header

    raise ValueError(
        f"{path}: could not find a TSV header beginning with #OTU ID / OTU ID"
    )


def canonical_sample_columns(
    header: list[str],
    marker: str | None,
) -> tuple[list[tuple[int, str]], list[str], str]:
    """
    Select only canonical ECHOREPO sample labels.

    Control columns such as BPCR10-ITS and Negative-control-ITS do not match the
    canonical ECHOREPO sample-ID pattern and are therefore reported and dropped.
    """
    canonical: list[tuple[int, str]] = []
    noncanonical: list[str] = []
    markers: Counter[str] = Counter()

    for index, column in enumerate(header[1:], start=1):
        if column.strip().lower() == "taxonomy":
            # New BIOM table has a trailing taxonomy header, but the actual
            # taxonomy values now live in taxa_*.txt.
            continue

        match = SAMPLE_RE.fullmatch(column)
        if match:
            found_marker = match.group(1).upper()
            canonical.append((index, column))
            markers[found_marker] += 1
        else:
            noncanonical.append(column)

    if not canonical:
        raise ValueError("No canonical ECHOREPO sample columns were found")

    if marker:
        expected_marker = marker.upper()
        wrong = [
            column
            for _, column in canonical
            if SAMPLE_RE.fullmatch(column).group(1).upper() != expected_marker
        ]
        if wrong:
            raise ValueError(
                f"Found sample columns for a marker other than {expected_marker}: "
                + ", ".join(wrong[:10])
            )
        inferred_marker = expected_marker
    else:
        if len(markers) != 1:
            raise ValueError(
                "Cannot infer a single marker from sample columns: "
                + ", ".join(f"{key}={value}" for key, value in sorted(markers.items()))
            )
        inferred_marker = next(iter(markers))

    return canonical, noncanonical, inferred_marker


def _csv_field_size(value: str) -> int:
    """Exact UTF-8 byte size of one CSV-encoded field, excluding newline."""
    buffer = io.StringIO(newline="")
    writer = csv.writer(buffer, lineterminator="\n")
    writer.writerow([value])
    encoded = buffer.getvalue().encode("utf-8")
    return len(encoded) - 1  # remove final newline


def _csv_row_bytes(values: list[str]) -> bytes:
    buffer = io.StringIO(newline="")
    writer = csv.writer(buffer, lineterminator="\n")
    writer.writerow(values)
    return buffer.getvalue().encode("utf-8")


def _compact_number(value: float) -> str:
    if value.is_integer():
        return str(int(value))
    return format(value, ".15g")


def _part_path(base_output: Path, part_number: int) -> Path:
    suffix = base_output.suffix or ".csv"
    stem = base_output.stem if base_output.suffix else base_output.name
    return base_output.with_name(f"{stem}_part{part_number:03d}{suffix}")


def _remove_old_parts(base_output: Path) -> None:
    suffix = base_output.suffix or ".csv"
    stem = base_output.stem if base_output.suffix else base_output.name
    pattern = f"{stem}_part[0-9][0-9][0-9]{suffix}"
    for path in base_output.parent.glob(pattern):
        path.unlink()


def _row_taxonomy(
    otu_id: str,
    taxonomy_by_otu: dict[str, dict[str, str]],
    fail_on_missing_taxonomy: bool,
    taxonomy_path: Path,
) -> tuple[dict[str, str], bool]:
    taxonomy = taxonomy_by_otu.get(otu_id)
    if taxonomy is not None:
        return taxonomy, False

    if fail_on_missing_taxonomy:
        raise ValueError(
            f"OTU {otu_id!r} has no entry in {taxonomy_path.name}"
        )

    return (
        {column: "" for column in OUTPUT_TAXONOMY_COLUMNS},
        True,
    )


def convert(
    table_path: Path,
    taxonomy_path: Path,
    output_path: Path,
    marker: str | None,
    fail_on_missing_taxonomy: bool,
    max_output_bytes: int,
    min_total_reads: float,
) -> dict[str, object]:
    """
    Convert the split laboratory format to ECHOREPO CSV files.

    Processing order:

      1. join OTU counts with taxonomy;
      2. calculate each OTU's total across ALL canonical ECHOREPO sample
         columns in the original unsplit matrix;
      3. remove OTUs whose total is strictly below ``min_total_reads``;
      4. split the RETAINED matrix by sample columns so every output CSV is
         <= ``max_output_bytes``.

    Splitting by sample columns is intentional: each ECHOREPO sample belongs
    to one output part, while every retained OTU is repeated across all parts.
    This matches ECHOREPO's current-source model during later export.

    No ECHOREPO-specific helper column is written to the output.
    """
    if max_output_bytes <= 0:
        raise ValueError("--max-output-mb must be greater than zero")
    if min_total_reads < 0:
        raise ValueError("--min-total-reads must be >= 0")

    taxonomy_by_otu, taxonomy_stats = load_taxonomy_file(taxonomy_path)
    header_line, header = find_table_header(table_path)

    samples, dropped_columns, inferred_marker = canonical_sample_columns(
        header,
        marker,
    )

    taxonomy_header_index = next(
        (
            index
            for index, name in enumerate(header)
            if name.strip().lower() == "taxonomy"
        ),
        None,
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    _remove_old_parts(output_path)

    # ------------------------------------------------------------------
    # PASS 1
    #
    # Validate the source, apply the full-matrix <10 filter conceptually,
    # and calculate exact byte contributions for retained rows only.
    # ------------------------------------------------------------------
    sample_field_bytes = [0] * len(samples)
    fixed_data_field_bytes = 0

    input_otu_rows = 0
    retained_otu_rows = 0
    excluded_below_min_total = 0
    missing_taxonomy = 0
    retained_missing_taxonomy = 0
    taxonomy_otus_used: set[str] = set()

    with table_path.open("r", encoding="utf-8-sig", newline="") as source:
        reader = csv.reader(source, delimiter="\t")
        for _ in range(header_line):
            next(reader)

        for line_number, row in enumerate(reader, start=header_line + 1):
            if not row or all(not value.strip() for value in row):
                continue
            if row[0].lstrip().startswith("#"):
                continue

            input_otu_rows += 1

            expected_without_taxonomy = (
                len(header) - 1
                if taxonomy_header_index is not None
                else len(header)
            )
            valid_lengths = {expected_without_taxonomy}
            if taxonomy_header_index is not None:
                valid_lengths.add(len(header))

            if len(row) not in valid_lengths:
                raise ValueError(
                    f"{table_path}: line {line_number} has {len(row)} fields; "
                    f"expected one of {sorted(valid_lengths)}."
                )

            otu_id = row[0].strip()
            if not otu_id:
                raise ValueError(
                    f"{table_path}: line {line_number} has an empty OTU ID"
                )

            taxonomy, missing = _row_taxonomy(
                otu_id,
                taxonomy_by_otu,
                fail_on_missing_taxonomy,
                taxonomy_path,
            )
            if missing:
                missing_taxonomy += 1
            else:
                taxonomy_otus_used.add(otu_id)

            full_total = 0.0
            sample_values: list[str] = []

            for index, column in samples:
                if index >= len(row):
                    raise ValueError(
                        f"{table_path}: line {line_number} is missing value "
                        f"for sample column {column!r}"
                    )

                value = row[index].strip()
                if value:
                    try:
                        numeric = float(value)
                    except ValueError as exc:
                        raise ValueError(
                            f"{table_path}: line {line_number}, sample "
                            f"{column!r} has non-numeric value {value!r}"
                        ) from exc

                    if numeric < 0:
                        raise ValueError(
                            f"{table_path}: line {line_number}, sample "
                            f"{column!r} has negative value {value!r}"
                        )

                    full_total += numeric

                sample_values.append(value)

            # IMPORTANT:
            # this decision is made before the sample columns are split.
            if full_total < min_total_reads:
                excluded_below_min_total += 1
                continue

            retained_otu_rows += 1
            if missing:
                retained_missing_taxonomy += 1

            for sample_pos, value in enumerate(sample_values):
                sample_field_bytes[sample_pos] += _csv_field_size(value)

            fixed_values = [
                otu_id,
                *[taxonomy[column] for column in OUTPUT_TAXONOMY_COLUMNS],
            ]
            fixed_data_field_bytes += sum(
                _csv_field_size(value) for value in fixed_values
            )

    if input_otu_rows == 0:
        raise ValueError(f"{table_path}: no OTU rows were found")

    if retained_otu_rows == 0:
        raise ValueError(
            "No OTUs remain after the preprocessing abundance filter "
            f"(minimum total reads = {min_total_reads:g})."
        )

    # ------------------------------------------------------------------
    # Choose sample-column groups that fit the byte limit.
    #
    # Output fields per row:
    #   OTU ID + k sample columns + 7 taxonomy columns
    #
    # Fixed field count = 8.
    # ------------------------------------------------------------------
    FIXED_FIELD_COUNT = 1 + len(OUTPUT_TAXONOMY_COLUMNS)

    def estimated_part_size(sample_positions: list[int]) -> int:
        sample_names = [samples[pos][1] for pos in sample_positions]

        header_values = [
            "OTU ID",
            *sample_names,
            *OUTPUT_TAXONOMY_COLUMNS,
        ]
        header_size = len(_csv_row_bytes(header_values))

        k = len(sample_positions)
        data_size = fixed_data_field_bytes
        data_size += sum(
            sample_field_bytes[pos] for pos in sample_positions
        )

        # For every data row:
        #   fields-1 commas + one newline == number_of_fields bytes.
        data_size += retained_otu_rows * (FIXED_FIELD_COUNT + k)

        return header_size + data_size

    groups: list[list[int]] = []
    current: list[int] = []

    for sample_pos in range(len(samples)):
        candidate = [*current, sample_pos]

        if current and estimated_part_size(candidate) > max_output_bytes:
            groups.append(current)
            current = [sample_pos]
        else:
            current = candidate

        if estimated_part_size(current) > max_output_bytes:
            sample_name = samples[sample_pos][1]
            raise ValueError(
                f"Even a one-sample output containing {sample_name!r} and "
                "the complete retained OTU universe exceeds the requested "
                f"{max_output_bytes}-byte limit."
            )

    if current:
        groups.append(current)

    # ------------------------------------------------------------------
    # PASS 2
    #
    # Re-read the source, repeat the same full-matrix threshold decision,
    # and write only retained OTUs. Every part has all retained OTUs but a
    # disjoint subset of sample columns.
    # ------------------------------------------------------------------
    part_handles: list[tuple[Path, object, list[int]]] = []

    try:
        for part_number, group in enumerate(groups, start=1):
            path = _part_path(output_path, part_number)
            handle = path.open("wb")
            part_handles.append((path, handle, group))

            header_values = [
                "OTU ID",
                *[samples[pos][1] for pos in group],
                *OUTPUT_TAXONOMY_COLUMNS,
            ]
            handle.write(_csv_row_bytes(header_values))

        with table_path.open("r", encoding="utf-8-sig", newline="") as source:
            reader = csv.reader(source, delimiter="\t")
            for _ in range(header_line):
                next(reader)

            for line_number, row in enumerate(
                reader,
                start=header_line + 1,
            ):
                if not row or all(not value.strip() for value in row):
                    continue
                if row[0].lstrip().startswith("#"):
                    continue

                otu_id = row[0].strip()

                taxonomy, _missing = _row_taxonomy(
                    otu_id,
                    taxonomy_by_otu,
                    fail_on_missing_taxonomy,
                    taxonomy_path,
                )

                full_total = 0.0
                all_sample_values: list[str] = []

                for index, column in samples:
                    value = row[index].strip()

                    if value:
                        try:
                            numeric = float(value)
                        except ValueError as exc:
                            raise ValueError(
                                f"{table_path}: line {line_number}, sample "
                                f"{column!r} has non-numeric value {value!r}"
                            ) from exc

                        if numeric < 0:
                            raise ValueError(
                                f"{table_path}: line {line_number}, sample "
                                f"{column!r} has negative value {value!r}"
                            )

                        full_total += numeric

                    all_sample_values.append(value)

                if full_total < min_total_reads:
                    continue

                for _path, handle, group in part_handles:
                    output_row = [
                        otu_id,
                        *[
                            all_sample_values[pos]
                            for pos in group
                        ],
                        *[
                            taxonomy[column]
                            for column in OUTPUT_TAXONOMY_COLUMNS
                        ],
                    ]
                    handle.write(_csv_row_bytes(output_row))

    finally:
        for _path, handle, _group in part_handles:
            handle.close()

    output_parts: list[dict[str, object]] = []

    for part_number, (path, _handle, group) in enumerate(
        part_handles,
        start=1,
    ):
        size_bytes = path.stat().st_size

        if size_bytes > max_output_bytes:
            raise RuntimeError(
                f"{path} is {size_bytes} bytes, above the requested "
                f"{max_output_bytes}-byte limit"
            )

        output_parts.append(
            {
                "part": part_number,
                "path": str(path),
                "size_bytes": size_bytes,
                "size_mb": round(size_bytes / 1_000_000, 3),
                "sample_count": len(group),
                "first_sample": samples[group[0]][1],
                "last_sample": samples[group[-1]][1],
                "otu_rows": retained_otu_rows,
            }
        )

    taxonomy_not_in_table = len(
        set(taxonomy_by_otu) - taxonomy_otus_used
    )

    return {
        "table": str(table_path),
        "taxonomy": str(taxonomy_path),
        "output_base": str(output_path),
        "output_parts": output_parts,
        "max_output_bytes": max_output_bytes,
        "marker": inferred_marker,
        "min_total_reads": min_total_reads,
        "input_table_columns": len(header),
        "canonical_sample_columns_kept": len(samples),
        "noncanonical_columns_dropped": dropped_columns,
        "input_otu_rows": input_otu_rows,
        "retained_otu_rows": retained_otu_rows,
        "excluded_below_min_total": excluded_below_min_total,
        "table_otus_missing_taxonomy": missing_taxonomy,
        "retained_otus_missing_taxonomy": retained_missing_taxonomy,
        "taxonomy_otus_not_present_in_table": taxonomy_not_in_table,
        **taxonomy_stats,
    }

def main() -> int:
    args = parse_args()

    try:
        report = convert(
            Path(args.table),
            Path(args.taxonomy),
            Path(args.output),
            args.marker,
            args.fail_on_missing_taxonomy,
            int(args.max_output_mb * 1_000_000),
            args.min_total_reads,
        )
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    print("Conversion complete.")
    print(f"  Marker:                    {report['marker']}")
    print(
        "  Canonical samples kept:    "
        f"{report['canonical_sample_columns_kept']}"
    )
    print(
        "  Input OTU rows:             "
        f"{report['input_otu_rows']}"
    )
    print(
        "  Removed total < threshold:  "
        f"{report['excluded_below_min_total']}"
    )
    print(
        "  Retained OTU rows:          "
        f"{report['retained_otu_rows']}"
    )
    print(
        "  OTUs missing taxonomy:     "
        f"{report['table_otus_missing_taxonomy']}"
    )
    print(
        "  Taxonomy-only OTUs:        "
        f"{report['taxonomy_otus_not_present_in_table']}"
    )

    dropped = report["noncanonical_columns_dropped"]
    if dropped:
        print(
            "  Non-canonical columns dropped: "
            + ", ".join(str(value) for value in dropped)
        )

    print(
        "  Output parts:               "
        f"{len(report['output_parts'])}"
    )
    for part in report["output_parts"]:
        print(
            f"    {part['path']}  "
            f"({part['size_mb']:.3f} MB, "
            f"{part['sample_count']} samples, "
            f"{part['otu_rows']} OTUs)"
        )

    if args.report_json:
        report_path = Path(args.report_json)
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(
            json.dumps(report, indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
        print(f"  Report:                     {report_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
