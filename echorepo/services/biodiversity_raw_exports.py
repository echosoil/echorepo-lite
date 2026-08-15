from __future__ import annotations

import csv
import io
import json
import math
import zipfile
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from psycopg2.extras import RealDictCursor

from .db import get_pg_conn


# Public scientific bundle. PostgreSQL remains normalized/sparse internally,
# but the export reconstructs familiar wide OTU x sample matrices.
BIODIVERSITY_RAW_FILES = (
    "biodiversity_16S.csv",
    "biodiversity_ITS.csv",
    "biodiversity_taxonomy.csv",
    "biodiversity_metadata.json",
)

TAXONOMY_RANKS = (
    "kingdom",
    "phylum",
    "class",
    "order",
    "family",
    "genus",
    "species",
)

TAXONOMY_COLUMNS = [
    "taxonomy_id",
    *TAXONOMY_RANKS,
]

MARKERS = ("16S", "ITS")


@dataclass(frozen=True)
class BiodiversityRawBundle:
    zip_bytes: bytes
    row_counts: dict[str, int]


def _utc_iso(value: datetime | None = None) -> str:
    if value is None:
        value = datetime.now(timezone.utc)
    elif value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)

    return (
        value.astimezone(timezone.utc)
        .isoformat()
        .replace("+00:00", "Z")
    )


def _text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _count_value(value: Any) -> int | float | str:
    """Write counts compactly: 552.0 -> 552, while preserving 12.5."""
    if value is None:
        return ""

    number = float(value)
    if not math.isfinite(number):
        raise ValueError(f"Non-finite biodiversity abundance value: {value!r}")

    if number.is_integer():
        return int(number)

    return format(number, ".15g")


def _fetch_rows(
    cur,
    sql: str,
    params: tuple[Any, ...] | list[Any] | None = None,
) -> list[dict[str, Any]]:
    cur.execute(sql, params or ())
    return [dict(row) for row in cur.fetchall()]


def _fetch_current_samples(cur) -> list[dict[str, Any]]:
    """
    Resolve the current source upload for every sample/marker.

    sample_taxon_abundance is used as the current-source pointer; the join to
    biodiversity_raw_samples recovers the original sample column/index.
    """
    return _fetch_rows(
        cur,
        """
        WITH current_sample_sources AS (
            SELECT DISTINCT ON (
                UPPER(sta.sample_id),
                UPPER(sta.marker)
            )
                UPPER(sta.sample_id) AS sample_id,
                UPPER(sta.marker) AS marker,
                sta.source_upload_id AS upload_id,
                bu.uploaded_at
            FROM sample_taxon_abundance AS sta
            JOIN biodiversity_uploads AS bu
              ON bu.upload_id = sta.source_upload_id
            WHERE sta.source_upload_id IS NOT NULL
              AND UPPER(sta.marker) IN ('16S', 'ITS')
            ORDER BY
                UPPER(sta.sample_id),
                UPPER(sta.marker),
                bu.uploaded_at DESC,
                sta.source_upload_id DESC
        )
        SELECT
            css.upload_id AS source_id,
            rs.sample_index,
            rs.source_sample_label,
            css.sample_id,
            css.marker
        FROM current_sample_sources AS css
        JOIN biodiversity_raw_samples AS rs
          ON rs.upload_id = css.upload_id
         AND UPPER(rs.sample_id) = css.sample_id
         AND UPPER(rs.marker) = css.marker
        ORDER BY
            css.marker,
            css.sample_id,
            css.upload_id,
            rs.sample_index
        """,
    )


def _taxonomy_tuple(row: dict[str, Any]) -> tuple[str, ...]:
    return tuple(_text(row.get(rank)) for rank in TAXONOMY_RANKS)


def _load_marker_data(
    cur,
    *,
    marker: str,
    samples: list[dict[str, Any]],
) -> dict[str, Any]:
    marker = marker.upper()

    marker_samples = [
        row
        for row in samples
        if _text(row.get("marker")).upper() == marker
    ]

    seen_sample_ids: set[str] = set()
    seen_column_labels: set[str] = set()
    normalized_samples: list[dict[str, Any]] = []

    for row in marker_samples:
        source_id = _text(row["source_id"])
        sample_index = int(row["sample_index"])
        sample_id = _text(row["sample_id"]).upper()

        source_label = _text(row.get("source_sample_label"))
        if not source_label:
            source_label = f"{sample_id}-{marker}"

        if sample_id in seen_sample_ids:
            raise RuntimeError(
                f"Sample {sample_id!r} occurs more than once in the current "
                f"{marker} raw representation"
            )

        if source_label in seen_column_labels:
            raise RuntimeError(
                f"Duplicate sample column label {source_label!r} in current "
                f"{marker} raw representation"
            )

        normalized_samples.append(
            {
                "source_id": source_id,
                "sample_index": sample_index,
                "sample_id": sample_id,
                "column_label": source_label,
            }
        )
        seen_sample_ids.add(sample_id)
        seen_column_labels.add(source_label)

    normalized_samples.sort(key=lambda row: row["sample_id"])

    column_index = {
        (row["source_id"], row["sample_index"]): idx
        for idx, row in enumerate(normalized_samples)
    }

    source_ids = sorted({row["source_id"] for row in normalized_samples})

    if not source_ids:
        return {
            "marker": marker,
            "samples": [],
            "source_ids": [],
            "feature_taxonomy": {},
            "feature_sources": {},
            "feature_index_to_id": {},
            "counts": {},
        }

    features = _fetch_rows(
        cur,
        """
        SELECT
            f.upload_id AS source_id,
            f.feature_index,
            f.source_feature_id,
            f.kingdom,
            f.phylum,
            f.class_name AS class,
            f.order_name AS "order",
            f.family,
            f.genus,
            f.species
        FROM biodiversity_raw_features AS f
        WHERE f.upload_id = ANY(%s)
        ORDER BY
            f.upload_id,
            f.feature_index
        """,
        (source_ids,),
    )

    feature_taxonomy: dict[str, tuple[str, ...]] = {}
    feature_sources: dict[str, set[str]] = defaultdict(set)
    feature_index_to_id: dict[tuple[str, int], str] = {}
    feature_seen_in_source: set[tuple[str, str]] = set()

    for row in features:
        source_id = _text(row["source_id"])
        feature_index = int(row["feature_index"])
        feature_id = _text(row["source_feature_id"])

        if not feature_id:
            raise RuntimeError(
                f"Empty source_feature_id in {source_id}, "
                f"feature_index={feature_index}"
            )

        taxonomy = _taxonomy_tuple(row)

        existing_taxonomy = feature_taxonomy.get(feature_id)
        if existing_taxonomy is None:
            feature_taxonomy[feature_id] = taxonomy
        elif existing_taxonomy != taxonomy:
            raise RuntimeError(
                "The same OTU/feature ID maps to different taxonomies across "
                f"current {marker} source files: {feature_id!r}. "
                f"First={existing_taxonomy!r}, later={taxonomy!r}"
            )

        source_feature_key = (source_id, feature_id)
        if source_feature_key in feature_seen_in_source:
            raise RuntimeError(
                "Duplicate OTU/feature ID inside one source file cannot be "
                f"losslessly merged: source={source_id}, feature={feature_id!r}"
            )
        feature_seen_in_source.add(source_feature_key)

        feature_index_key = (source_id, feature_index)
        if feature_index_key in feature_index_to_id:
            raise RuntimeError(
                f"Duplicate feature index in source={source_id}: {feature_index}"
            )

        feature_index_to_id[feature_index_key] = feature_id
        feature_sources[feature_id].add(source_id)

    # Sparse memory representation: feature_id -> {sample-column-index: count}
    counts: dict[str, dict[int, float]] = defaultdict(dict)

    cur.execute(
        """
        SELECT
            a.upload_id AS source_id,
            a.feature_index,
            a.sample_index,
            a.read_count
        FROM biodiversity_raw_abundance AS a
        WHERE a.upload_id = ANY(%s)
        ORDER BY
            a.upload_id,
            a.feature_index,
            a.sample_index
        """,
        (source_ids,),
    )

    while True:
        batch = cur.fetchmany(20_000)
        if not batch:
            break

        for raw_row in batch:
            row = dict(raw_row)
            source_id = _text(row["source_id"])
            sample_index = int(row["sample_index"])

            sample_col = column_index.get((source_id, sample_index))
            if sample_col is None:
                # Same raw source may contain sample columns that are no longer
                # the current representation. Do not re-export stale samples.
                continue

            feature_id = feature_index_to_id.get(
                (source_id, int(row["feature_index"]))
            )
            if feature_id is None:
                raise RuntimeError(
                    "Abundance row references a missing raw feature: "
                    f"source={source_id}, feature_index={row['feature_index']}"
                )

            value = float(row["read_count"])
            if value < 0:
                raise RuntimeError(
                    "Negative biodiversity abundance encountered during export: "
                    f"source={source_id}, feature={feature_id}, value={value}"
                )

            existing = counts[feature_id].get(sample_col)
            if existing is not None:
                value += existing

            counts[feature_id][sample_col] = value

    return {
        "marker": marker,
        "samples": normalized_samples,
        "source_ids": source_ids,
        "feature_taxonomy": feature_taxonomy,
        "feature_sources": dict(feature_sources),
        "feature_index_to_id": feature_index_to_id,
        "counts": dict(counts),
    }


def _build_taxonomy_dictionary(
    marker_data: dict[str, dict[str, Any]],
) -> tuple[list[tuple[str, ...]], dict[tuple[str, ...], int]]:
    """Create one bundle-local taxonomy dictionary shared by 16S and ITS."""
    lineages = {
        taxonomy
        for data in marker_data.values()
        for taxonomy in data["feature_taxonomy"].values()
    }

    ordered = sorted(lineages)
    taxonomy_id_by_lineage = {
        lineage: index
        for index, lineage in enumerate(ordered, start=1)
    }

    return ordered, taxonomy_id_by_lineage


def _taxonomy_csv_bytes(lineages: list[tuple[str, ...]]) -> bytes:
    buffer = io.StringIO(newline="")
    writer = csv.writer(buffer, lineterminator="\n")
    writer.writerow(TAXONOMY_COLUMNS)

    for taxonomy_id, lineage in enumerate(lineages, start=1):
        writer.writerow([taxonomy_id, *lineage])

    return buffer.getvalue().encode("utf-8")


def _marker_matrix_csv_bytes(
    data: dict[str, Any],
    taxonomy_id_by_lineage: dict[tuple[str, ...], int],
) -> tuple[bytes, int]:
    """
    Reconstruct one wide OTU x sample matrix.

    Cell semantics:
      non-zero value -> stored count
      0              -> feature existed in that source matrix, count was zero
      blank          -> feature did not exist as a row in that sample's source
                        file, so no artificial zero is invented
    """
    samples: list[dict[str, Any]] = data["samples"]
    feature_taxonomy: dict[str, tuple[str, ...]] = data["feature_taxonomy"]
    feature_sources: dict[str, set[str]] = data["feature_sources"]
    counts: dict[str, dict[int, float]] = data["counts"]

    buffer = io.StringIO(newline="")
    writer = csv.writer(buffer, lineterminator="\n")

    writer.writerow(
        [
            "OTU ID",
            "taxonomy_id",
            *[row["column_label"] for row in samples],
        ]
    )

    feature_ids = sorted(feature_taxonomy)

    for feature_id in feature_ids:
        taxonomy = feature_taxonomy[feature_id]
        taxonomy_id = taxonomy_id_by_lineage[taxonomy]
        present_sources = feature_sources.get(feature_id, set())
        feature_counts = counts.get(feature_id, {})

        values: list[int | float | str] = []

        for sample_col, sample in enumerate(samples):
            if sample["source_id"] not in present_sources:
                values.append("")
                continue

            values.append(
                _count_value(feature_counts.get(sample_col, 0.0))
            )

        writer.writerow([feature_id, taxonomy_id, *values])

    return buffer.getvalue().encode("utf-8"), len(feature_ids)


def _metadata_bytes(
    *,
    marker_data: dict[str, dict[str, Any]],
    taxonomy_count: int,
    generated_at: str,
) -> bytes:
    metadata = {
        "format_version": "2.0",
        "generated_at_utc": generated_at,
        "description": (
            "ECHOrepo biodiversity OTU/feature count matrices reconstructed "
            "from the current structured raw PostgreSQL representation."
        ),
        "files": {
            "biodiversity_16S.csv": {
                "marker": "16S",
                "rows": "OTU/feature IDs",
                "columns": (
                    "OTU ID, taxonomy_id, followed by original 16S sample "
                    "column labels"
                ),
                "values": "sequencing read counts",
                "feature_count": len(marker_data["16S"]["feature_taxonomy"]),
                "sample_count": len(marker_data["16S"]["samples"]),
            },
            "biodiversity_ITS.csv": {
                "marker": "ITS",
                "rows": "OTU/feature IDs",
                "columns": (
                    "OTU ID, taxonomy_id, followed by original ITS sample "
                    "column labels"
                ),
                "values": "sequencing read counts",
                "feature_count": len(marker_data["ITS"]["feature_taxonomy"]),
                "sample_count": len(marker_data["ITS"]["samples"]),
            },
            "biodiversity_taxonomy.csv": {
                "description": (
                    "Bundle-local taxonomy lookup used by both marker matrices."
                ),
                "taxonomy_count": taxonomy_count,
                "columns": TAXONOMY_COLUMNS,
            },
        },
        "encoding": {
            "taxonomy_id": (
                "Bundle-local integer code for one complete taxonomic lineage. "
                "Resolve it through biodiversity_taxonomy.csv."
            ),
            "zero": (
                "0 means the OTU/feature existed in the corresponding source "
                "matrix and its abundance for that sample was zero."
            ),
            "blank": (
                "A blank abundance cell means that OTU/feature was not present "
                "as a feature row in the source file supplying that sample. "
                "Blank is intentionally different from zero."
            ),
            "sample_headers": (
                "Original source sample labels are retained, including the "
                "-16S or -ITS marker suffix when present in the input."
            ),
        },
        "provenance_note": (
            "Database/source bookkeeping columns are intentionally not repeated "
            "in the public scientific matrices. The normalized raw tables and "
            "original archived source uploads remain the internal provenance "
            "representation."
        ),
    }

    return (
        json.dumps(
            metadata,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")


def build_biodiversity_raw_bundle() -> BiodiversityRawBundle:
    """
    Build a compact, lossless scientific biodiversity ZIP.

    Output:
      - biodiversity_16S.csv
      - biodiversity_ITS.csv
      - biodiversity_taxonomy.csv
      - biodiversity_metadata.json

    The previous long-form biodiversity_abundance.csv is deliberately not
    generated. Quantitative information is reconstructed into wide OTU x sample
    matrices instead.

    Only the raw source currently associated with each sample/marker is used,
    so historical re-imports are not double-counted.
    """
    generated_at = _utc_iso()

    with get_pg_conn() as conn, conn.cursor(
        cursor_factory=RealDictCursor
    ) as cur:
        current_samples = _fetch_current_samples(cur)

        marker_data = {
            marker: _load_marker_data(
                cur,
                marker=marker,
                samples=current_samples,
            )
            for marker in MARKERS
        }

    lineages, taxonomy_id_by_lineage = _build_taxonomy_dictionary(marker_data)

    resources: dict[str, bytes] = {}
    row_counts: dict[str, int] = {}

    for marker in MARKERS:
        filename = f"biodiversity_{marker}.csv"
        csv_bytes, feature_count = _marker_matrix_csv_bytes(
            marker_data[marker],
            taxonomy_id_by_lineage,
        )
        resources[filename] = csv_bytes
        row_counts[filename] = feature_count

    resources["biodiversity_taxonomy.csv"] = _taxonomy_csv_bytes(lineages)
    row_counts["biodiversity_taxonomy.csv"] = len(lineages)

    resources["biodiversity_metadata.json"] = _metadata_bytes(
        marker_data=marker_data,
        taxonomy_count=len(lineages),
        generated_at=generated_at,
    )
    row_counts["biodiversity_metadata.json"] = 1

    output = io.BytesIO()
    with zipfile.ZipFile(
        output,
        mode="w",
        compression=zipfile.ZIP_DEFLATED,
        compresslevel=6,
    ) as archive:
        for filename in BIODIVERSITY_RAW_FILES:
            archive.writestr(filename, resources[filename])

    return BiodiversityRawBundle(
        zip_bytes=output.getvalue(),
        row_counts=row_counts,
    )
