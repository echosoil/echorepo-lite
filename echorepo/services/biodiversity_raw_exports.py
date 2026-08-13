from __future__ import annotations

import csv
import io
import json
import os
import zipfile
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from psycopg2.extras import RealDictCursor

from .db import get_pg_conn


BIODIVERSITY_RAW_FILES = (
    "biodiversity_sources.csv",
    "biodiversity_samples.csv",
    "biodiversity_features.csv",
    "biodiversity_abundance.csv",
)

SOURCE_COLUMNS = [
    "source_id",
    "source_filename",
    "source_row_count",
    "nonzero_value_count",
    "sample_count",
    "marker_count",
    "ingested_datetime_utc",
    "licence",
]

SAMPLE_COLUMNS = [
    "source_id",
    "sample_index",
    "source_column_number",
    "source_sample_label",
    "sample_id",
    "marker",
]

FEATURE_COLUMNS = [
    "source_id",
    "feature_index",
    "source_row_number",
    "source_feature_id",
    "taxonomy_raw",
    "kingdom",
    "phylum",
    "class",
    "order",
    "family",
    "genus",
    "species",
    "taxonomy_source_json",
]

ABUNDANCE_COLUMNS = [
    "source_id",
    "feature_index",
    "sample_index",
    "read_count",
]


@dataclass(frozen=True)
class BiodiversityRawBundle:
    zip_bytes: bytes
    row_counts: dict[str, int]


def _csv_value(value: Any) -> Any:
    if value is None:
        return ""

    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        value = value.astimezone(timezone.utc)
        return value.isoformat().replace("+00:00", "Z")

    if isinstance(value, (dict, list)):
        return json.dumps(
            value,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )

    return value


def _csv_bytes(
    columns: list[str],
    rows: list[dict[str, Any]],
) -> bytes:
    buffer = io.StringIO(newline="")
    writer = csv.DictWriter(
        buffer,
        fieldnames=columns,
        extrasaction="ignore",
        lineterminator="\n",
    )
    writer.writeheader()

    for row in rows:
        writer.writerow(
            {
                column: _csv_value(row.get(column))
                for column in columns
            }
        )

    return buffer.getvalue().encode("utf-8")


def _fetch_rows(
    cur,
    sql: str,
) -> list[dict[str, Any]]:
    cur.execute(sql)
    return [dict(row) for row in cur.fetchall()]


def build_biodiversity_raw_bundle() -> BiodiversityRawBundle:
    """
    Build the public raw-biodiversity ZIP from PostgreSQL.

    Only uploads that already have a structured raw feature representation are
    exported. This prevents older aggregate-only biodiversity_uploads records
    from appearing as incomplete raw sources.
    """
    licence = (
        os.getenv("BIODIV_PUBLIC_LICENCE")
        or "CC-BY-4.0"
    ).strip()

    with get_pg_conn() as conn, conn.cursor(
        cursor_factory=RealDictCursor
    ) as cur:
        sources = _fetch_rows(
            cur,
            """
            SELECT
                u.upload_id AS source_id,
                u.original_filename AS source_filename,
                u.source_row_count,
                u.nonzero_value_count,
                u.sample_count,
                u.marker_count,
                u.uploaded_at AS ingested_datetime_utc
            FROM biodiversity_uploads AS u
            WHERE EXISTS (
                SELECT 1
                FROM biodiversity_raw_features AS f
                WHERE f.upload_id = u.upload_id
            )
            ORDER BY
                u.uploaded_at,
                u.upload_id
            """,
        )

        source_ids = {
            row["source_id"]
            for row in sources
        }

        for row in sources:
            row["licence"] = licence

        samples = _fetch_rows(
            cur,
            """
            SELECT
                s.upload_id AS source_id,
                s.sample_index,
                s.source_column_number,
                s.source_sample_label,
                s.sample_id,
                s.marker
            FROM biodiversity_raw_samples AS s
            WHERE EXISTS (
                SELECT 1
                FROM biodiversity_raw_features AS f
                WHERE f.upload_id = s.upload_id
            )
            ORDER BY
                s.upload_id,
                s.sample_index
            """,
        )

        features = _fetch_rows(
            cur,
            """
            SELECT
                f.upload_id AS source_id,
                f.feature_index,
                f.source_row_number,
                f.source_feature_id,
                f.taxonomy_raw,
                f.kingdom,
                f.phylum,
                f.class_name AS class,
                f.order_name AS "order",
                f.family,
                f.genus,
                f.species,
                f.taxonomy_source AS taxonomy_source_json
            FROM biodiversity_raw_features AS f
            ORDER BY
                f.upload_id,
                f.feature_index
            """,
        )

        abundances = _fetch_rows(
            cur,
            """
            SELECT
                a.upload_id AS source_id,
                a.feature_index,
                a.sample_index,
                a.read_count
            FROM biodiversity_raw_abundance AS a
            ORDER BY
                a.upload_id,
                a.feature_index,
                a.sample_index
            """,
        )

    samples = [
        row for row in samples
        if row["source_id"] in source_ids
    ]
    features = [
        row for row in features
        if row["source_id"] in source_ids
    ]
    abundances = [
        row for row in abundances
        if row["source_id"] in source_ids
    ]

    resources = {
        "biodiversity_sources.csv": _csv_bytes(
            SOURCE_COLUMNS,
            sources,
        ),
        "biodiversity_samples.csv": _csv_bytes(
            SAMPLE_COLUMNS,
            samples,
        ),
        "biodiversity_features.csv": _csv_bytes(
            FEATURE_COLUMNS,
            features,
        ),
        "biodiversity_abundance.csv": _csv_bytes(
            ABUNDANCE_COLUMNS,
            abundances,
        ),
    }

    output = io.BytesIO()
    with zipfile.ZipFile(
        output,
        mode="w",
        compression=zipfile.ZIP_DEFLATED,
        compresslevel=6,
    ) as archive:
        for filename in BIODIVERSITY_RAW_FILES:
            archive.writestr(
                filename,
                resources[filename],
            )

    return BiodiversityRawBundle(
        zip_bytes=output.getvalue(),
        row_counts={
            "biodiversity_sources.csv": len(sources),
            "biodiversity_samples.csv": len(samples),
            "biodiversity_features.csv": len(features),
            "biodiversity_abundance.csv": len(abundances),
        },
    )
