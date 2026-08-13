#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
import zipfile
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path
from typing import Any, Iterator

from psycopg2.extras import Json, RealDictCursor, execute_values

# Allow: python3 tools/backfill_biodiversity_raw.py
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from echorepo.services.biodiversity_import import (  # noqa: E402
    BIODIVERSITY_SAMPLE_RE,
    _is_biodiversity_header,
    _open_biodiversity_rows,
)
from echorepo.services.db import get_pg_conn  # noqa: E402
from echorepo.services.storage.minio import (  # noqa: E402
    bucket_name,
    get_client,
)


SUPPORTED_SOURCE_SUFFIXES = {".csv", ".tsv", ".txt", ".xlsx"}
FEATURE_BATCH_SIZE = 5_000
ABUNDANCE_BATCH_SIZE = 10_000


@dataclass
class PreparedSource:
    rows_iter: Any
    close_source: Any
    source_description: str
    sample_cols: list[dict[str, Any]]
    taxonomy_cols: list[tuple[int, str]]
    taxonomy_rank_indices: dict[str, int | None]


@dataclass
class ScanStats:
    sample_count: int
    marker_count: int
    feature_count: int
    nonzero_value_count: int
    markers: tuple[str, ...]


def normalize_header(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value or "").strip().lower())


def split_sample_marker(value: str) -> tuple[str, str]:
    parts = str(value or "").strip().rsplit("-", 1)
    if len(parts) != 2:
        return "", ""
    sample_id = parts[0].strip().upper()
    marker = parts[1].strip().upper()
    if marker not in {"16S", "ITS"}:
        return "", ""
    return sample_id, marker


def to_float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    text = str(value).strip().replace(",", ".")
    if not text or text.lower() in {"nan", "none", "null", "na", "n/a"}:
        return None
    try:
        return float(text)
    except (TypeError, ValueError):
        return None


def clean_taxonomy_rank(value: Any, expected_prefix: str) -> str | None:
    if value is None:
        return None
    raw = str(value).strip()
    if not raw or raw.lower() in {
        "nan", "none", "null", "na", "n/a", "unassigned", "unknown"
    }:
        return None

    wanted = expected_prefix.lower() + "__"
    for token in re.split(r"[;|]", raw):
        token = token.strip()
        if token.lower().startswith(wanted):
            raw = token
            break

    raw = re.sub(r"^[A-Za-z]__", "", raw, flags=re.IGNORECASE).strip()
    return raw or None


def taxonomy_for_row(
    row: Any,
    taxonomy_rank_indices: dict[str, int | None],
) -> dict[str, str | None]:
    prefix_by_rank = {
        "kingdom": "k",
        "phylum": "p",
        "class_name": "c",
        "order_name": "o",
        "family": "f",
        "genus": "g",
        "species": "s",
    }
    result: dict[str, str | None] = {}
    for rank, idx in taxonomy_rank_indices.items():
        if idx is None or idx >= len(row):
            result[rank] = None
            continue
        result[rank] = clean_taxonomy_rank(row[idx], prefix_by_rank[rank])
    return result


def taxonomy_source_for_row(
    row: Any,
    taxonomy_cols: list[tuple[int, str]],
) -> dict[str, str | None]:
    result: dict[str, str | None] = {}
    for idx, column_name in taxonomy_cols:
        value = row[idx] if idx < len(row) else None
        result[str(column_name)] = None if value is None else str(value)
    return result


def taxonomy_raw_for_row(
    row: Any,
    taxonomy_cols: list[tuple[int, str]],
) -> str | None:
    values: list[str] = []
    for idx, _column_name in taxonomy_cols:
        if idx >= len(row):
            continue
        value = row[idx]
        if value is None:
            continue
        text = str(value).strip()
        if text:
            values.append(text)
    return ";".join(values) or None


def prepare_source(file_bytes: bytes, filename: str) -> PreparedSource:
    raw_header, rows_iter, close_source, source_description = _open_biodiversity_rows(
        file_bytes, filename
    )

    header = ["" if value is None else str(value).strip() for value in raw_header]
    if not any(header):
        close_source()
        raise ValueError("Biodiversity file header is empty")
    if not _is_biodiversity_header(header):
        close_source()
        raise ValueError(
            "The archived file does not look like biodiversity data. "
            "Expected OTU ID, sample-marker columns and Phylum/taxonomy."
        )

    # Keep exactly the same convention as the current importer.
    otu_col_idx = 0
    sample_cols: list[dict[str, Any]] = []

    for idx, column_name in enumerate(header):
        if idx == otu_col_idx:
            continue
        if not BIODIVERSITY_SAMPLE_RE.fullmatch(str(column_name).strip()):
            continue
        sample_id, marker = split_sample_marker(column_name)
        if not sample_id or not marker:
            continue
        sample_cols.append(
            {
                "sample_index": len(sample_cols) + 1,
                "idx": idx,
                "source_column_number": idx + 1,
                "column_name": column_name,
                "source_sample_label": column_name,
                "sample_id": sample_id,
                "marker": marker,
            }
        )

    if not sample_cols:
        close_source()
        raise ValueError("No sample columns found in archived biodiversity source.")

    sample_col_indices = {item["idx"] for item in sample_cols}
    taxonomy_cols = [
        (idx, column_name)
        for idx, column_name in enumerate(header)
        if idx != otu_col_idx and idx not in sample_col_indices
    ]

    normalized_taxonomy_headers = {
        normalize_header(column_name): idx for idx, column_name in taxonomy_cols
    }
    legacy_taxonomy_layout = (
        "taxonomy" in normalized_taxonomy_headers
        and "a" in normalized_taxonomy_headers
    )

    if legacy_taxonomy_layout:
        taxonomy_rank_indices = {
            "kingdom": normalized_taxonomy_headers.get("taxonomy"),
            "phylum": normalized_taxonomy_headers.get("a"),
            "class_name": normalized_taxonomy_headers.get("b"),
            "order_name": normalized_taxonomy_headers.get("c"),
            "family": normalized_taxonomy_headers.get("d"),
            "genus": normalized_taxonomy_headers.get("e"),
            "species": normalized_taxonomy_headers.get("f"),
        }
    else:
        taxonomy_rank_indices = {
            "kingdom": normalized_taxonomy_headers.get("kingdom"),
            "phylum": (
                normalized_taxonomy_headers.get("phylum")
                or normalized_taxonomy_headers.get("philum")
            ),
            "class_name": normalized_taxonomy_headers.get("class"),
            "order_name": normalized_taxonomy_headers.get("order"),
            "family": normalized_taxonomy_headers.get("family"),
            "genus": normalized_taxonomy_headers.get("genus"),
            "species": normalized_taxonomy_headers.get("species"),
        }

    return PreparedSource(
        rows_iter=rows_iter,
        close_source=close_source,
        source_description=source_description,
        sample_cols=sample_cols,
        taxonomy_cols=taxonomy_cols,
        taxonomy_rank_indices=taxonomy_rank_indices,
    )


def iter_parsed_features(
    prepared: PreparedSource,
) -> Iterator[
    tuple[
        int,
        int,
        str,
        str | None,
        dict[str, str | None],
        dict[str, str | None],
        list[tuple[int, float]],
    ]
]:
    feature_index = 0
    for source_row_number, row in enumerate(prepared.rows_iter, start=2):
        otu_id = "" if not row or row[0] is None else str(row[0]).strip()
        if not otu_id:
            continue

        feature_index += 1
        taxonomy = taxonomy_for_row(row, prepared.taxonomy_rank_indices)
        taxonomy_source = taxonomy_source_for_row(row, prepared.taxonomy_cols)
        taxonomy_raw = taxonomy_raw_for_row(row, prepared.taxonomy_cols)
        abundances: list[tuple[int, float]] = []

        for sample_info in prepared.sample_cols:
            idx = sample_info["idx"]
            value = row[idx] if idx < len(row) else None
            count = to_float_or_none(value)
            if count is None or count == 0:
                continue
            if count < 0:
                raise ValueError(
                    "Negative abundance at source row "
                    f"{source_row_number}, column {sample_info['column_name']}: {count}"
                )
            abundances.append((sample_info["sample_index"], float(count)))

        yield (
            feature_index,
            source_row_number,
            otu_id,
            taxonomy_raw,
            taxonomy,
            taxonomy_source,
            abundances,
        )


def current_pairs_for_upload(upload_id: str) -> set[tuple[str, str]]:
    with get_pg_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT UPPER(sample_id), UPPER(marker)
            FROM sample_taxon_abundance
            WHERE source_upload_id = %s
            """,
            (upload_id,),
        )
        return {
            (str(row[0]).strip(), str(row[1]).strip())
            for row in cur.fetchall()
            if row and row[0] and row[1]
        }


def raw_pairs_for_upload(upload_id: str) -> set[tuple[str, str]]:
    with get_pg_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT UPPER(sample_id), UPPER(marker)
            FROM biodiversity_raw_samples
            WHERE upload_id = %s
            """,
            (upload_id,),
        )
        return {
            (str(row[0]).strip(), str(row[1]).strip())
            for row in cur.fetchall()
            if row and row[0] and row[1]
        }


def existing_raw_counts(upload_id: str) -> dict[str, int]:
    with get_pg_conn() as conn, conn.cursor() as cur:
        result = {}
        for key, table in (
            ("samples", "biodiversity_raw_samples"),
            ("features", "biodiversity_raw_features"),
            ("abundance", "biodiversity_raw_abundance"),
        ):
            cur.execute(f"SELECT COUNT(*) FROM {table} WHERE upload_id = %s", (upload_id,))
            result[key] = int(cur.fetchone()[0])
        return result


def fetch_candidate_uploads(
    *,
    upload_id: str | None,
    limit: int | None,
    force: bool,
) -> list[dict[str, Any]]:
    sql = """
        WITH current_pairs AS (
            SELECT DISTINCT
                source_upload_id AS upload_id,
                UPPER(sample_id) AS sample_id,
                UPPER(marker) AS marker
            FROM sample_taxon_abundance
            WHERE source_upload_id IS NOT NULL
        ),
        pair_stats AS (
            SELECT
                cp.upload_id,
                COUNT(*) AS current_pair_count,
                COUNT(*) FILTER (
                    WHERE NOT EXISTS (
                        SELECT 1
                        FROM biodiversity_raw_samples AS rs
                        WHERE rs.upload_id = cp.upload_id
                          AND UPPER(rs.sample_id) = cp.sample_id
                          AND UPPER(rs.marker) = cp.marker
                    )
                ) AS missing_pair_count
            FROM current_pairs AS cp
            GROUP BY cp.upload_id
        )
        SELECT
            bu.upload_id,
            bu.original_filename,
            bu.archive_object_name,
            bu.sha256,
            bu.aggregation_level,
            bu.sample_count,
            bu.marker_count,
            bu.source_row_count,
            bu.nonzero_value_count,
            bu.uploaded_at,
            bu.uploaded_by,
            ps.current_pair_count,
            ps.missing_pair_count
        FROM biodiversity_uploads AS bu
        JOIN pair_stats AS ps ON ps.upload_id = bu.upload_id
    """

    where = []
    params: list[Any] = []
    if upload_id:
        where.append("bu.upload_id = %s")
        params.append(upload_id)
    if not force:
        where.append("ps.missing_pair_count > 0")
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY bu.uploaded_at, bu.upload_id"
    if limit is not None:
        sql += " LIMIT %s"
        params.append(limit)

    with get_pg_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, params)
        return [dict(row) for row in cur.fetchall()]


def download_archive_bytes(object_name: str) -> bytes:
    client = get_client(required=True)
    response = None
    try:
        response = client.get_object(bucket_name(), object_name)
        return response.read()
    finally:
        if response is not None:
            response.close()
            response.release_conn()


def extract_original_source(
    archive_bytes: bytes,
    *,
    database_filename: str,
) -> tuple[str, bytes, dict[str, Any]]:
    with zipfile.ZipFile(BytesIO(archive_bytes)) as zf:
        try:
            manifest = json.loads(zf.read("manifest.json").decode("utf-8"))
        except KeyError:
            manifest = {}
        except Exception as exc:
            raise ValueError(f"Cannot parse archive manifest.json: {exc}") from exc

        supported_members = [
            info
            for info in zf.infolist()
            if not info.is_dir()
            and Path(info.filename).suffix.lower() in SUPPORTED_SOURCE_SUFFIXES
        ]
        if not supported_members:
            raise ValueError("Archive contains no CSV/TSV/TXT/XLSX biodiversity source.")

        preferred_names = [
            str(manifest.get("original_filename") or "").strip(),
            str(database_filename or "").strip(),
        ]
        preferred_names = [Path(name).name for name in preferred_names if name]

        selected = None
        for preferred in preferred_names:
            for member in supported_members:
                if Path(member.filename).name == preferred:
                    selected = member
                    break
            if selected is not None:
                break

        if selected is None and len(supported_members) == 1:
            selected = supported_members[0]
        if selected is None:
            available = ", ".join(info.filename for info in supported_members)
            raise ValueError(
                "Could not unambiguously identify original source file. "
                f"Candidates: {available}"
            )

        return Path(selected.filename).name, zf.read(selected), manifest


def verify_source_hash(
    *,
    upload: dict[str, Any],
    source_bytes: bytes,
    manifest: dict[str, Any],
) -> str:
    actual = hashlib.sha256(source_bytes).hexdigest()
    expected_values: list[tuple[str, str]] = []

    db_sha = str(upload.get("sha256") or "").strip().lower()
    if db_sha:
        expected_values.append(("biodiversity_uploads.sha256", db_sha))

    upload_id = str(upload.get("upload_id") or "").strip().lower()
    if re.fullmatch(r"[0-9a-f]{64}", upload_id):
        expected_values.append(("upload_id", upload_id))

    manifest_sha = str(manifest.get("sha256") or "").strip().lower()
    if manifest_sha:
        expected_values.append(("manifest.sha256", manifest_sha))

    for label, expected in expected_values:
        if expected != actual:
            raise ValueError(
                f"SHA-256 mismatch: {label}={expected}, actual={actual}"
            )
    return actual


def parsed_sample_pairs(prepared: PreparedSource) -> set[tuple[str, str]]:
    return {
        (str(item["sample_id"]).strip().upper(), str(item["marker"]).strip().upper())
        for item in prepared.sample_cols
    }


def _stats_from_prepared(
    prepared: PreparedSource,
    feature_count: int,
    nonzero_value_count: int,
) -> ScanStats:
    markers = tuple(
        sorted({str(item["marker"]).strip().upper() for item in prepared.sample_cols})
    )
    return ScanStats(
        sample_count=len(
            {str(item["sample_id"]).strip().upper() for item in prepared.sample_cols}
        ),
        marker_count=len(markers),
        feature_count=feature_count,
        nonzero_value_count=nonzero_value_count,
        markers=markers,
    )


def scan_prepared_source(prepared: PreparedSource) -> ScanStats:
    feature_count = 0
    nonzero_value_count = 0
    try:
        for *_, abundances in iter_parsed_features(prepared):
            feature_count += 1
            nonzero_value_count += len(abundances)
    finally:
        prepared.close_source()
    return _stats_from_prepared(prepared, feature_count, nonzero_value_count)


def execute_backfill(*, upload_id: str, prepared: PreparedSource) -> ScanStats:
    raw_sample_rows = [
        (
            upload_id,
            item["sample_index"],
            item["source_column_number"],
            item["source_sample_label"],
            item["sample_id"],
            item["marker"],
        )
        for item in prepared.sample_cols
    ]

    feature_batch: list[tuple[Any, ...]] = []
    abundance_batch: list[tuple[Any, ...]] = []
    feature_count = 0
    nonzero_value_count = 0
    conn = get_pg_conn()

    try:
        with conn.cursor() as cur:
            # Rebuild ONLY the structured raw children.
            cur.execute(
                "DELETE FROM biodiversity_raw_abundance WHERE upload_id = %s",
                (upload_id,),
            )
            cur.execute(
                "DELETE FROM biodiversity_raw_features WHERE upload_id = %s",
                (upload_id,),
            )
            cur.execute(
                "DELETE FROM biodiversity_raw_samples WHERE upload_id = %s",
                (upload_id,),
            )

            execute_values(
                cur,
                """
                INSERT INTO biodiversity_raw_samples (
                    upload_id, sample_index, source_column_number,
                    source_sample_label, sample_id, marker
                ) VALUES %s
                """,
                raw_sample_rows,
                page_size=1_000,
            )

            def flush_features() -> None:
                if not feature_batch:
                    return
                execute_values(
                    cur,
                    """
                    INSERT INTO biodiversity_raw_features (
                        upload_id, feature_index, source_row_number,
                        source_feature_id, taxonomy_raw, kingdom, phylum,
                        class_name, order_name, family, genus, species,
                        taxonomy_source
                    ) VALUES %s
                    """,
                    feature_batch,
                    page_size=FEATURE_BATCH_SIZE,
                )
                feature_batch.clear()

            def flush_abundances() -> None:
                if not abundance_batch:
                    return
                # Abundance rows reference raw features.
                flush_features()
                execute_values(
                    cur,
                    """
                    INSERT INTO biodiversity_raw_abundance (
                        upload_id, feature_index, sample_index, read_count
                    ) VALUES %s
                    """,
                    abundance_batch,
                    page_size=ABUNDANCE_BATCH_SIZE,
                )
                abundance_batch.clear()

            for (
                feature_index,
                source_row_number,
                otu_id,
                taxonomy_raw,
                taxonomy,
                taxonomy_source,
                abundances,
            ) in iter_parsed_features(prepared):
                feature_count += 1
                feature_batch.append(
                    (
                        upload_id,
                        feature_index,
                        source_row_number,
                        otu_id,
                        taxonomy_raw,
                        taxonomy.get("kingdom"),
                        taxonomy.get("phylum"),
                        taxonomy.get("class_name"),
                        taxonomy.get("order_name"),
                        taxonomy.get("family"),
                        taxonomy.get("genus"),
                        taxonomy.get("species"),
                        Json(taxonomy_source),
                    )
                )
                if len(feature_batch) >= FEATURE_BATCH_SIZE:
                    flush_features()

                for sample_index, count in abundances:
                    abundance_batch.append(
                        (upload_id, feature_index, sample_index, count)
                    )
                    nonzero_value_count += 1
                    if len(abundance_batch) >= ABUNDANCE_BATCH_SIZE:
                        flush_abundances()

            flush_features()
            flush_abundances()

            if feature_count == 0:
                raise ValueError("No OTU/feature rows were found in source.")
            if nonzero_value_count == 0:
                raise ValueError("No non-zero abundance values were found in source.")

        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        prepared.close_source()
        conn.close()

    return _stats_from_prepared(prepared, feature_count, nonzero_value_count)


def global_coverage() -> dict[str, tuple[int, int, int]]:
    result: dict[str, tuple[int, int, int]] = {}
    with get_pg_conn() as conn, conn.cursor() as cur:
        for marker in ("16S", "ITS"):
            cur.execute(
                """
                SELECT COUNT(DISTINCT sample_id)
                FROM sample_taxon_abundance
                WHERE UPPER(marker) = %s
                """,
                (marker,),
            )
            canonical = int(cur.fetchone()[0])

            cur.execute(
                """
                SELECT COUNT(DISTINCT sample_id)
                FROM biodiversity_raw_samples
                WHERE UPPER(marker) = %s
                """,
                (marker,),
            )
            raw = int(cur.fetchone()[0])

            cur.execute(
                """
                SELECT COUNT(*)
                FROM (
                    SELECT DISTINCT UPPER(sta.sample_id) AS sample_id
                    FROM sample_taxon_abundance AS sta
                    WHERE UPPER(sta.marker) = %s
                      AND NOT EXISTS (
                          SELECT 1
                          FROM biodiversity_raw_samples AS rs
                          WHERE rs.upload_id = sta.source_upload_id
                            AND UPPER(rs.sample_id) = UPPER(sta.sample_id)
                            AND UPPER(rs.marker) = UPPER(sta.marker)
                      )
                ) AS missing
                """,
                (marker,),
            )
            missing = int(cur.fetchone()[0])
            result[marker] = (canonical, raw, missing)
    return result


def print_coverage(title: str) -> None:
    print()
    print(title)
    print("-" * len(title))
    for marker, (canonical, raw, missing) in global_coverage().items():
        print(
            f"{marker}: canonical={canonical}, "
            f"structured_raw={raw}, missing_current={missing}"
        )


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Reconstruct biodiversity_raw_* rows from existing historical "
            "MinIO archives without modifying compact canonical biodiversity "
            "data or biodiversity_uploads provenance."
        )
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Actually write biodiversity_raw_* rows. Default is dry-run.",
    )
    parser.add_argument(
        "--upload-id",
        help="Inspect/backfill only this biodiversity upload ID.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Process at most this many candidate uploads.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help=(
            "Include/rebuild uploads whose current sample-marker pairs "
            "already have structured raw rows."
        ),
    )
    args = parser.parse_args()

    if args.limit is not None and args.limit <= 0:
        parser.error("--limit must be greater than zero")

    mode = "EXECUTE" if args.execute else "DRY RUN"
    print(f"Mode: {mode}")
    print("Protected tables: biodiversity_uploads, sample_taxon_abundance")
    print(
        "Writable tables: biodiversity_raw_samples, "
        "biodiversity_raw_features, biodiversity_raw_abundance"
    )
    print_coverage("Coverage before")

    uploads = fetch_candidate_uploads(
        upload_id=args.upload_id,
        limit=args.limit,
        force=args.force,
    )
    if not uploads:
        print("\nNo candidate uploads found.")
        if args.upload_id and not args.force:
            print(
                "If this upload is already complete and you intentionally "
                "want to rebuild it, add --force."
            )
        return 0

    print(f"\nCandidate uploads: {len(uploads)}\n")
    processed = 0
    failed = 0
    total_samples = 0
    total_features = 0
    total_nonzero = 0

    for index, upload in enumerate(uploads, start=1):
        upload_id = str(upload["upload_id"])
        filename = str(upload["original_filename"] or "")
        object_name = str(upload["archive_object_name"] or "")

        print(f"[{index}/{len(uploads)}] {filename or upload_id}")
        print(f"  upload_id: {upload_id}")
        print(f"  archive:   {object_name}")
        print(f"  current sample-marker pairs: {upload.get('current_pair_count')}")
        print(f"  missing current pairs:       {upload.get('missing_pair_count')}")

        existing = existing_raw_counts(upload_id)
        print(
            "  existing raw rows: "
            f"samples={existing['samples']}, features={existing['features']}, "
            f"abundance={existing['abundance']}"
        )

        try:
            if not object_name:
                raise ValueError("biodiversity_uploads.archive_object_name is empty")

            archive_bytes = download_archive_bytes(object_name)
            extracted_filename, source_bytes, manifest = extract_original_source(
                archive_bytes,
                database_filename=filename,
            )
            source_sha = verify_source_hash(
                upload=upload,
                source_bytes=source_bytes,
                manifest=manifest,
            )
            print(
                f"  extracted: {extracted_filename} ({len(source_bytes):,} bytes)"
            )
            print(f"  SHA-256:   OK ({source_sha[:16]}...)")

            prepared = prepare_source(source_bytes, extracted_filename)
            parsed_pairs = parsed_sample_pairs(prepared)
            current_pairs = current_pairs_for_upload(upload_id)
            missing_from_archive = current_pairs - parsed_pairs
            extra_in_archive = parsed_pairs - current_pairs

            print(f"  parsed sample-marker pairs: {len(parsed_pairs)}")
            if missing_from_archive:
                preview = ", ".join(
                    f"{sample}/{marker}"
                    for sample, marker in sorted(missing_from_archive)[:10]
                )
                prepared.close_source()
                raise ValueError(
                    "Current database pairs are absent from archived source: "
                    f"{preview}"
                    + (" ..." if len(missing_from_archive) > 10 else "")
                )

            if extra_in_archive:
                print(
                    "  note: archive contains "
                    f"{len(extra_in_archive)} additional sample-marker pairs not "
                    "currently pointing to this upload; they will still be restored "
                    "as historical raw source rows."
                )

            if args.execute:
                stats = execute_backfill(upload_id=upload_id, prepared=prepared)
                action = "BACKFILLED"
            else:
                stats = scan_prepared_source(prepared)
                action = "WOULD BACKFILL"

            expected_features = upload.get("source_row_count")
            expected_nonzero = upload.get("nonzero_value_count")
            if (
                expected_features is not None
                and int(expected_features) != stats.feature_count
            ):
                print(
                    "  WARNING: source_row_count differs: "
                    f"DB={expected_features}, parsed={stats.feature_count}"
                )
            if (
                expected_nonzero is not None
                and int(expected_nonzero) != stats.nonzero_value_count
            ):
                print(
                    "  WARNING: nonzero_value_count differs: "
                    f"DB={expected_nonzero}, parsed={stats.nonzero_value_count}"
                )

            print(
                f"  {action}: samples={stats.sample_count}, "
                f"markers={','.join(stats.markers)}, "
                f"features={stats.feature_count:,}, "
                f"nonzero={stats.nonzero_value_count:,}"
            )

            if args.execute:
                now_raw_pairs = raw_pairs_for_upload(upload_id)
                still_missing = current_pairs - now_raw_pairs
                if still_missing:
                    raise RuntimeError(
                        "Backfill committed but some current pairs are still missing: "
                        f"{sorted(still_missing)[:10]}"
                    )
                print("  verification: current pairs restored")

            processed += 1
            total_samples += stats.sample_count
            total_features += stats.feature_count
            total_nonzero += stats.nonzero_value_count

        except Exception as exc:
            failed += 1
            print(f"  ERROR: {type(exc).__name__}: {exc}")

        print()

    print("Summary")
    print("-------")
    print(f"Uploads processed successfully: {processed}")
    print(f"Uploads failed:                 {failed}")
    print(f"Parsed samples (per source):    {total_samples}")
    print(f"Parsed features:                {total_features:,}")
    print(f"Parsed non-zero abundances:     {total_nonzero:,}")

    if not args.execute:
        print("\nDRY RUN — no database changes made.")
        print("If the report is correct, rerun with --execute.")
    else:
        print_coverage("Coverage after")

    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
