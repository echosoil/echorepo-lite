#!/usr/bin/env python3
"""
ECHOrepo biodiversity image generator
======================================

Run from the repository root, where the project .env file is located.

NORMAL IMAGE GENERATION
-----------------------

If the FAPROTAX sample-by-function CSV is already up to date, generate every
image family with one command:

    python3 tools/generate_biodiversity_piecharts.py --all-images

This generates any missing:

  - 16S Phylum taxonomic pie charts
  - ITS Phylum taxonomic pie charts
  - bacterial ecological guild plots (from FAPROTAX_FUNCTION_CSV)
  - fungal ecological guild plots

Existing MinIO images are skipped by default. Add --force to recreate them all:

    python3 tools/generate_biodiversity_piecharts.py --all-images --force

Use --dry-run to preview what would be generated.

FAPROTAX PREPARATION
--------------------

Bacterial guild images depend on an external FAPROTAX analysis. This script can
prepare the FAPROTAX input files, but it does NOT run FAPROTAX itself.

After new 16S data are imported:

    python3 tools/generate_biodiversity_piecharts.py \
      --marker 16S \
      --build-faprotax-inputs

This creates:

    data/biodiversity/faprotax_work/6_otu_clean_counts_no_blanks.csv
    data/biodiversity/faprotax_work/7_taxonomy_clean.csv

Run the external FAPROTAX workflow, then place its sample-by-function result at
FAPROTAX_FUNCTION_CSV (default:
`data/biodiversity/8_faprotax_samples_x_functions.csv`). Only then run
`--all-images`.

Do not combine --build-faprotax-inputs with --all-images: an external FAPROTAX
step must happen between those operations.

INDIVIDUAL IMAGE FAMILIES
-------------------------

Taxonomic charts only:

    python3 tools/generate_biodiversity_piecharts.py --marker 16S --level Phylum
    python3 tools/generate_biodiversity_piecharts.py --marker ITS --level Phylum

Taxonomic chart + matching guild family:

    python3 tools/generate_biodiversity_piecharts.py --marker 16S --bacterial-guilds
    python3 tools/generate_biodiversity_piecharts.py --marker ITS --fungal-guilds

Useful options:

    --sample-id CLMW-8393
    --sample-id CLMW-8393,AACW-5934
    --dry-run
    --force
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import math
import os
import re
import shutil
import sys
import tempfile
import zipfile
from pathlib import Path
import threading
import time

import matplotlib as mpl
import matplotlib.pyplot as plt
import pandas as pd
from dotenv import load_dotenv
from openpyxl import load_workbook

# ---------------------------------------------------------------------------
# Load .env exactly like pull_and_enrich_samples.py
# ---------------------------------------------------------------------------
env_path = Path.cwd() / ".env"
load_dotenv(dotenv_path=env_path)
print(f"[INFO] Loaded environment from {env_path}")

# ---------------------------------------------------------------------------
# Make sure project root is importable
# ---------------------------------------------------------------------------
THIS_DIR = Path(__file__).resolve().parent
DEFAULT_ROOT = THIS_DIR.parent
PROJECT_ROOT = Path(os.getenv("PROJECT_ROOT", str(DEFAULT_ROOT)))
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
print(f"[INFO] Using PROJECT_ROOT={PROJECT_ROOT}")

# ---------------------------------------------------------------------------
# MinIO config: same style as pull_and_enrich_samples.py
# ---------------------------------------------------------------------------
try:
    from minio import Minio
    from minio.error import S3Error
except ImportError:
    Minio = None

    class S3Error(Exception):
        pass


MINIO_ENDPOINT = (
    os.getenv("MINIO_ENDPOINT_INSIDE")
    or os.getenv("MINIO_ENDPOINT_OUTSIDE")
    or os.getenv("MINIO_ENDPOINT")
    or "localhost:9000"
)
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY") or os.getenv("MINIO_ROOT_USER") or ""
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY") or os.getenv("MINIO_ROOT_PASSWORD") or ""
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "echorepo-uploads")
PUBLIC_STORAGE_BASE = os.getenv("PUBLIC_STORAGE_BASE", "/storage")
FUNGUILD_DB_JSON = os.getenv(
    "FUNGUILD_DB_JSON",
    str(PROJECT_ROOT / "data" / "biodiversity" / "FUNGuild_db.json"),
)

GENERATE_FUNGAL_GUILDS = os.getenv("GENERATE_FUNGAL_GUILDS", "0") == "1"
BUILD_FAPROTAX_INPUTS = os.getenv("BUILD_FAPROTAX_INPUTS", "0") == "1"

GENERATE_BACTERIAL_GUILDS = os.getenv("GENERATE_BACTERIAL_GUILDS", "0") == "1"

FAPROTAX_FUNCTION_CSV = os.getenv(
    "FAPROTAX_FUNCTION_CSV",
    str(PROJECT_ROOT / "data" / "biodiversity" / "8_faprotax_samples_x_functions.csv"),
)

# ---------------------------------------------------------------------------
# Plot styling
# ---------------------------------------------------------------------------
PIE_BG = "#FFFFFF"
PIE_TEXT = "#000000"
PIE_EDGE = "#FFFFFF"
PIE_GRID = "#e0e0e0"

PIE_COLORS = [
    "#f0746a",  # salmon
    "#df9600",  # orange
    "#a6a800",  # olive
    "#41c400",  # green
    "#12bf80",  # teal-green
    "#1db7be",  # cyan-teal
    "#20a7df",  # blue
    "#8a83e6",  # lavender
    "#cc62dc",  # magenta-violet
    "#eb5bb3",  # pink
    "#999999",  # grey fallback for "Other"
]

mpl.rcParams["font.family"] = "DejaVu Sans"
mpl.rcParams["text.color"] = PIE_TEXT
mpl.rcParams["axes.labelcolor"] = PIE_TEXT
mpl.rcParams["xtick.color"] = PIE_TEXT
mpl.rcParams["ytick.color"] = PIE_TEXT

# ----------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------
def _normalise_tax_value(value):
    if pd.isna(value):
        return ""

    value = str(value).strip()

    if value.lower() in {
        "",
        "nan",
        "none",
        "null",
        "na",
        "n/a",
    }:
        return ""

    return value

FAPROTAX_TAX_COLS = [
    "kingdom",
    "phylum",
    "class_name",
    "order_name",
    "family",
    "genus",
    "species",
]


def reconcile_faprotax_taxonomy(
    taxonomy_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Produce exactly one canonical taxonomy lineage per OTU.

    Rules:
      1. Normalize blanks / NA-like values.
      2. Repair the known Excel MM-YY -> date coercion only when it can be
         corroborated by another current source for the same OTU.
      3. If an Excel-looking date cannot be corroborated, treat the Order as
         missing rather than pretending the calendar date is taxonomy.
      4. Merge compatible incomplete lineages.
      5. Stop on genuinely conflicting non-empty taxonomy values.
    """
    df = taxonomy_df.copy()

    if df.empty:
        raise RuntimeError(
            "No taxonomy rows were returned for eligible FAPROTAX OTUs"
        )

    df["otu_id"] = (
        df["otu_id"]
        .astype(str)
        .str.strip()
    )

    for col in FAPROTAX_TAX_COLS:
        df[col] = df[col].map(_normalise_tax_value)

    # --------------------------------------------------------------
    # Repair the known Excel date coercion.
    #
    # Example:
    #
    #   original taxonomy label: 11-24
    #   XLSX/openpyxl value:      2024-11-01 00:00:00
    #
    # We only restore 11-24 when that exact value also occurs for
    # the same OTU in another current source.
    #
    # If no corroborating source exists, the date-shaped value is
    # discarded as unknown taxonomy instead of inventing a label.
    # --------------------------------------------------------------
    excel_date_re = re.compile(
        r"^[12]\d{3}-(0[1-9]|1[0-2])-01"
        r"(?:[ T]00:00:00)?$"
    )

    order_values_by_otu = {
        otu_id: {
            value
            for value in group["order_name"]
            if value
        }
        for otu_id, group in df.groupby(
            "otu_id",
            sort=False,
        )
    }

    repaired_rows = 0
    repaired_otus: set[str] = set()

    discarded_rows = 0
    discarded_otus: set[str] = set()

    for idx in df.index:
        value = df.at[idx, "order_name"]

        if not value or not excel_date_re.fullmatch(value):
            continue

        parsed = pd.to_datetime(
            value,
            errors="coerce",
        )

        if pd.isna(parsed):
            continue

        candidate = parsed.strftime("%m-%y")
        otu_id = df.at[idx, "otu_id"]

        known_values = order_values_by_otu.get(
            otu_id,
            set(),
        )

        if candidate in known_values:
            df.at[idx, "order_name"] = candidate
            repaired_rows += 1
            repaired_otus.add(otu_id)

        else:
            # Calendar dates are not valid taxonomy Order labels.
            # Without corroboration, missing data is safer than an
            # invented conversion.
            df.at[idx, "order_name"] = ""
            discarded_rows += 1
            discarded_otus.add(otu_id)

    if repaired_rows:
        print(
            "[INFO] Repaired corroborated Excel taxonomy-date coercion: "
            f"{repaired_rows:,} source rows covering "
            f"{len(repaired_otus):,} OTUs",
            flush=True,
        )

    if discarded_rows:
        print(
            "[WARN] Removed uncorroborated date-shaped taxonomy Orders: "
            f"{discarded_rows:,} source rows covering "
            f"{len(discarded_otus):,} OTUs",
            flush=True,
        )

    # Drop duplicate copies after normalization/repair.
    tax_unique = (
        df[
            [
                "otu_id",
                *FAPROTAX_TAX_COLS,
            ]
        ]
        .drop_duplicates()
    )

    resolved_rows = []
    true_conflicts = []

    for otu_id, variants in tax_unique.groupby(
        "otu_id",
        sort=False,
    ):
        resolved = {
            "otu_id": otu_id,
        }

        conflict_found = False

        for col in FAPROTAX_TAX_COLS:
            values = sorted(
                {
                    value
                    for value in variants[col]
                    if value
                }
            )

            if len(values) > 1:
                true_conflicts.append(
                    (
                        otu_id,
                        col,
                        values,
                    )
                )
                conflict_found = True
                break

            resolved[col] = (
                values[0]
                if values
                else ""
            )

        if not conflict_found:
            resolved_rows.append(resolved)

    if true_conflicts:
        examples = "; ".join(
            f"{otu_id}: {rank}={values}"
            for otu_id, rank, values
            in true_conflicts[:20]
        )

        raise RuntimeError(
            "Cannot safely merge raw source files for FAPROTAX: "
            f"{len(true_conflicts)} OTUs have genuinely conflicting "
            f"non-empty taxonomy values after normalization. "
            f"Examples: {examples}"
        )

    resolved_df = pd.DataFrame(
        resolved_rows,
        columns=[
            "otu_id",
            *FAPROTAX_TAX_COLS,
        ],
    )

    if resolved_df["otu_id"].duplicated().any():
        raise RuntimeError(
            "Internal error: duplicate OTU IDs remain after "
            "taxonomy reconciliation"
        )

    print(
        "[OK] Canonical FAPROTAX taxonomy: "
        f"{len(resolved_df):,} OTUs",
        flush=True,
    )

    return resolved_df

# ---------------------------------------------------------------------------
# Postgres config
# ---------------------------------------------------------------------------
try:
    import psycopg2
except ImportError:
    psycopg2 = None


def get_pg_conn():
    if psycopg2 is None:
        raise RuntimeError("psycopg2 is not installed")

    host = (
        os.getenv("DB_HOST_OUTSIDE")
        or os.getenv("DB_HOST_INSIDE")
        or os.getenv("DB_HOST")
        or "localhost"
    )
    port = int(
        os.getenv("DB_PORT_OUTSIDE")
        or os.getenv("DB_PORT_INSIDE")
        or os.getenv("DB_PORT")
        or "5432"
    )

    return psycopg2.connect(
        host=host,
        port=port,
        dbname=os.getenv("DB_NAME", "echorepo"),
        user=os.getenv("DB_USER", "echorepo"),
        password=os.getenv("DB_PASSWORD", "echorepo-pass"),
    )


def init_minio():
    if Minio is None:
        print("[INFO] python-minio not installed; skipping MinIO upload.")
        return None

    secure = False
    endpoint = MINIO_ENDPOINT
    if endpoint.startswith("https://"):
        secure = True
        endpoint = endpoint[len("https://") :]
    elif endpoint.startswith("http://"):
        secure = False
        endpoint = endpoint[len("http://") :]

    if not MINIO_ACCESS_KEY or not MINIO_SECRET_KEY:
        print("[WARN] MinIO credentials not set; skipping chart upload.")
        return None

    client = Minio(
        endpoint,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=secure,
    )

    try:
        found = client.bucket_exists(MINIO_BUCKET)
        if not found:
            client.make_bucket(MINIO_BUCKET)
            print(f"[INFO] Created MinIO bucket {MINIO_BUCKET}")
    except Exception as e:
        print(f"[WARN] Could not ensure MinIO bucket: {e}")
        return None

    print(f"[INFO] MinIO ready at {MINIO_ENDPOINT}, bucket={MINIO_BUCKET}")
    return client


def upload_file_to_minio(
    mclient, local_path: Path, object_name: str, content_type: str = "image/png"
):
    if mclient is None:
        return None

    try:
        size = local_path.stat().st_size
        with local_path.open("rb") as f:
            mclient.put_object(
                MINIO_BUCKET,
                object_name,
                data=f,
                length=size,
                content_type=content_type,
            )
        print(f"[OK] uploaded to MinIO: {object_name}")
        return f"{PUBLIC_STORAGE_BASE}/{object_name}"
    except Exception as e:
        print(f"[WARN] could not upload {local_path} to MinIO as {object_name}: {e}")
        return None


def list_existing_minio_objects(mclient, prefix: str) -> set[str]:
    """
    Return all existing object names below a MinIO prefix.

    The generator calls this once per chart family, rather than issuing one
    stat/HEAD request per sample. If MinIO is configured but listing fails,
    abort instead of accidentally regenerating the complete dataset.
    """
    if mclient is None:
        raise RuntimeError(
            "MinIO is unavailable, so existing charts cannot be checked safely. "
            "Configure MinIO or run with --force to regenerate local files intentionally."
        )

    try:
        existing = {
            obj.object_name
            for obj in mclient.list_objects(
                MINIO_BUCKET,
                prefix=prefix,
                recursive=True,
            )
        }
    except Exception as e:
        raise RuntimeError(
            f"Could not list existing MinIO objects under {prefix!r}: {e}"
        ) from e

    print(f"[INFO] Found {len(existing)} existing MinIO objects under {prefix}")
    return existing


def normalize_sample_filter(values: list[str] | None) -> set[str] | None:
    """Normalize repeated/comma-separated --sample-id arguments."""
    if not values:
        return None

    result: set[str] = set()
    for value in values:
        for token in re.split(r"[,;\s]+", str(value or "").strip()):
            if token:
                result.add(token.upper())

    return result or None

def normalize_taxonomic_level(level: str) -> str:
    """
    Normalize user/env level names.

    Accepts:
      Philum, phylum, Phylum, p, p__, p__Ascomycota

    Returns one of:
      Kingdom, Phylum, Class, Order, Family, Genus, Species
    """
    if level is None:
        return "Phylum"

    s = str(level).strip()

    if not s:
        return "Phylum"

    s_lower = s.lower().strip()

    aliases = {
        "kingdom": "Kingdom",
        "taxonomy": "Kingdom",
        "k": "Kingdom",
        "k__": "Kingdom",
        "d": "Kingdom",
        "d__": "Kingdom",

        "phylum": "Phylum",
        "philum": "Phylum",   # common typo
        "p": "Phylum",
        "p__": "Phylum",

        "class": "Class",
        "c": "Class",
        "c__": "Class",

        "order": "Order",
        "o": "Order",
        "o__": "Order",

        "family": "Family",
        "f": "Family",
        "f__": "Family",

        "genus": "Genus",
        "g": "Genus",
        "g__": "Genus",

        "species": "Species",
        "s": "Species",
        "s__": "Species",
    }

    if s_lower in aliases:
        return aliases[s_lower]

    # If someone passes something like p__Ascomycota,
    # infer the level from the prefix.
    m = re.match(r"^([dkpcofgs])__", s_lower)
    if m:
        return aliases.get(m.group(1), "Phylum")

    # If already correctly capitalized
    for valid in ("Kingdom", "Phylum", "Class", "Order", "Family", "Genus", "Species"):
        if s_lower == valid.lower():
            return valid

    print(f"[WARN] Unknown BIODIV_LEVEL={level!r}; falling back to Phylum")
    return "Phylum"

def sanitize_filename(s: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", str(s).strip())


def fetch_otu_data(marker: str = "16S") -> pd.DataFrame:
    """
    Fetch legacy OTU-level data.

    This is retained only as a legacy fallback for older fungal/FUNGuild data.
    FAPROTAX inputs are built from the structured biodiversity_raw_* tables.
    """
    sql = """
        SELECT sample_id, otu_id, count, taxa
        FROM sample_otu_counts
        WHERE marker = %s
    """
    with get_pg_conn() as conn:
        df = pd.read_sql(sql, conn, params=[marker])
    return df



BIODIVERSITY_SAMPLE_COLUMN_RE = re.compile(
    r"^(?P<sample>[A-Za-z0-9]{4}-[A-Za-z0-9]{4,})-(?P<marker>16S|ITS)$",
    re.IGNORECASE,
)


def _normalize_biodiversity_header(value) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value or "").strip().lower())


def fetch_current_biodiversity_archives(
    marker: str,
    sample_ids: set[str] | None = None,
) -> pd.DataFrame:
    """
    Return the raw MinIO archive currently associated with each sample.

    sample_taxon_abundance is the source of truth for which upload is current.
    All Phylum rows for one sample/marker should point to the same upload ID.
    """
    sql = """
        SELECT DISTINCT
            UPPER(sta.sample_id) AS sample_id,
            sta.source_upload_id,
            bu.archive_object_name,
            bu.original_filename,
            bu.uploaded_at
        FROM sample_taxon_abundance AS sta
        JOIN biodiversity_uploads AS bu
          ON bu.upload_id = sta.source_upload_id
        WHERE UPPER(sta.marker) = UPPER(%s)
          AND sta.source_upload_id IS NOT NULL
    """
    params: list[object] = [marker]

    if sample_ids:
        sql += " AND UPPER(sta.sample_id) = ANY(%s)"
        params.append(sorted(sample_ids))

    sql += " ORDER BY sample_id, bu.uploaded_at DESC"

    with get_pg_conn() as conn:
        df = pd.read_sql(sql, conn, params=params)

    if df.empty:
        return df

    # Defensive: if inconsistent rows exist, retain the newest mapping.
    return (
        df.sort_values("uploaded_at", ascending=False)
        .drop_duplicates(subset=["sample_id"], keep="first")
        .reset_index(drop=True)
    )


def _download_minio_object(mclient, object_name: str, destination: Path) -> None:
    if mclient is None:
        raise RuntimeError("MinIO is required to read raw biodiversity archives")

    response = None
    try:
        response = mclient.get_object(MINIO_BUCKET, object_name)
        with destination.open("wb") as out:
            shutil.copyfileobj(response, out, length=1024 * 1024)
    finally:
        if response is not None:
            response.close()
            response.release_conn()


def _extract_biodiversity_source(zip_path: Path, output_dir: Path) -> Path:
    """Extract the original CSV/TSV/XLSX from one raw-upload ZIP."""
    supported = {".csv", ".tsv", ".txt", ".xlsx"}

    with zipfile.ZipFile(zip_path) as zf:
        members = [
            info
            for info in zf.infolist()
            if not info.is_dir()
            and Path(info.filename).suffix.lower() in supported
        ]

        if not members:
            raise ValueError(f"No CSV/TSV/XLSX source found in {zip_path.name}")

        preferred_name = None
        try:
            manifest = json.loads(zf.read("manifest.json").decode("utf-8"))
            preferred_name = Path(
                str(manifest.get("original_filename") or "")
            ).name
        except Exception:
            preferred_name = None

        selected = members[0]
        if preferred_name:
            for member in members:
                if Path(member.filename).name == preferred_name:
                    selected = member
                    break

        safe_name = Path(selected.filename).name
        destination = output_dir / safe_name
        with zf.open(selected) as source, destination.open("wb") as target:
            shutil.copyfileobj(source, target, length=1024 * 1024)

    return destination


def _aggregate_fungal_rows(
    header,
    rows,
    target_samples: set[str],
) -> pd.DataFrame:
    """
    Reconstruct the information needed by FUNGuild from a raw OTU table.

    Rows are aggregated by sample and genus to keep memory use small. Counts
    without a usable genus are retained so chart percentages still use the
    full fungal-community read total as their denominator.
    """
    cleaned_header = [str(value or "").strip() for value in header]
    normalized = [_normalize_biodiversity_header(value) for value in cleaned_header]

    otu_idx = 0
    for idx, value in enumerate(normalized):
        if value in {"otuid", "otu"}:
            otu_idx = idx
            break

    sample_columns: list[tuple[int, str]] = []
    for idx, value in enumerate(cleaned_header):
        match = BIODIVERSITY_SAMPLE_COLUMN_RE.fullmatch(value)
        if not match or match.group("marker").upper() != "ITS":
            continue
        sample_id = match.group("sample").upper()
        if sample_id in target_samples:
            sample_columns.append((idx, sample_id))

    if not sample_columns:
        return pd.DataFrame(columns=["sample_id", "otu_id", "count", "taxa"])

    by_name = {value: idx for idx, value in enumerate(normalized) if value}
    genus_idx = by_name.get("genus")
    taxonomy_idx = by_name.get("taxonomy")

    # Legacy layout: Taxonomy, A, B, C, D, E, F where E is Genus.
    if genus_idx is None and taxonomy_idx is not None:
        genus_idx = by_name.get("e")

    aggregates: dict[str, dict[str, float]] = {}

    for row in rows:
        if otu_idx >= len(row) or not str(row[otu_idx] or "").strip():
            continue

        positive_counts: list[tuple[str, float]] = []
        for idx, sample_id in sample_columns:
            raw_value = row[idx] if idx < len(row) else None
            try:
                value = float(raw_value)
            except (TypeError, ValueError):
                continue
            if not math.isfinite(value) or value <= 0:
                continue
            positive_counts.append((sample_id, value))

        if not positive_counts:
            continue

        genus = ""
        if genus_idx is not None and genus_idx < len(row):
            genus = clean_taxon_value(row[genus_idx])

        if not genus and taxonomy_idx is not None and taxonomy_idx < len(row):
            genus = parse_taxonomy_string(row[taxonomy_idx]).get("Genus", "")
            genus = clean_taxon_value(genus)

        for sample_id, value in positive_counts:
            sample_counts = aggregates.setdefault(sample_id, {})
            sample_counts[genus] = sample_counts.get(genus, 0.0) + value

    output_rows = []
    for sample_id, genus_counts in aggregates.items():
        for genus, count in genus_counts.items():
            output_rows.append(
                {
                    "sample_id": sample_id,
                    "otu_id": f"raw-genus:{genus or 'unclassified'}",
                    "count": count,
                    "taxa": {"Genus": genus} if genus else {},
                }
            )

    return pd.DataFrame(output_rows, columns=["sample_id", "otu_id", "count", "taxa"])


def _read_fungal_rows_from_source(
    source_path: Path,
    target_samples: set[str],
) -> pd.DataFrame:
    suffix = source_path.suffix.lower()

    if suffix == ".xlsx":
        wb = load_workbook(source_path, read_only=True, data_only=True)
        try:
            sheet_name = next(
                (
                    name
                    for name in wb.sheetnames
                    if str(name).strip().lower() == "clean_phylum"
                ),
                None,
            )

            if sheet_name is None:
                for candidate in wb.sheetnames:
                    ws_candidate = wb[candidate]
                    first_row = next(
                        ws_candidate.iter_rows(
                            min_row=1,
                            max_row=1,
                            values_only=True,
                        ),
                        None,
                    )
                    if first_row and any(
                        BIODIVERSITY_SAMPLE_COLUMN_RE.fullmatch(str(v or "").strip())
                        for v in first_row
                    ):
                        sheet_name = candidate
                        break

            if sheet_name is None:
                raise ValueError(f"No biodiversity worksheet found in {source_path.name}")

            rows = wb[sheet_name].iter_rows(values_only=True)
            header = next(rows, None)
            if header is None:
                raise ValueError(f"Empty biodiversity worksheet in {source_path.name}")

            return _aggregate_fungal_rows(header, rows, target_samples)
        finally:
            wb.close()

    if suffix not in {".csv", ".tsv", ".txt"}:
        raise ValueError(f"Unsupported raw biodiversity file: {source_path.name}")

    with source_path.open("r", encoding="utf-8-sig", newline="") as stream:
        probe = stream.read(65536)
        stream.seek(0)

        if suffix == ".tsv":
            dialect = csv.excel_tab
        else:
            try:
                dialect = csv.Sniffer().sniff(probe, delimiters=",\t;")
            except csv.Error:
                dialect = csv.excel

        reader = csv.reader(stream, dialect=dialect)
        header = next(reader, None)
        if header is None:
            raise ValueError(f"Empty biodiversity file: {source_path.name}")

        return _aggregate_fungal_rows(header, reader, target_samples)


def fetch_current_fungal_data_from_archives(
    mclient,
    sample_ids: set[str] | None = None,
) -> tuple[pd.DataFrame, set[str], set[str]]:
    """
    Reconstruct current ITS genus/count data from raw MinIO archives.

    Returns: dataframe, all current sample IDs, successfully reconstructed IDs.
    """
    mappings = fetch_current_biodiversity_archives("ITS", sample_ids)
    if mappings.empty:
        return (
            pd.DataFrame(columns=["sample_id", "otu_id", "count", "taxa"]),
            set(),
            set(),
        )

    current_ids = set(mappings["sample_id"].astype(str).str.upper())
    frames: list[pd.DataFrame] = []
    reconstructed_ids: set[str] = set()

    grouped = mappings.groupby("archive_object_name", dropna=False)

    with tempfile.TemporaryDirectory(prefix="echorepo-funguild-") as tmp:
        tmp_dir = Path(tmp)

        for archive_number, (object_name, group) in enumerate(grouped, start=1):
            object_name = str(object_name or "").strip()
            archive_samples = set(group["sample_id"].astype(str).str.upper())

            if not object_name:
                print(
                    "[WARN] Current ITS samples have no archive object: "
                    f"{sorted(archive_samples)}"
                )
                continue

            archive_path = tmp_dir / f"archive-{archive_number}.zip"
            extract_dir = tmp_dir / f"archive-{archive_number}"
            extract_dir.mkdir(parents=True, exist_ok=True)

            try:
                print(
                    f"[INFO] Reading raw ITS archive {object_name} "
                    f"for {len(archive_samples)} samples"
                )
                _download_minio_object(mclient, object_name, archive_path)
                source_path = _extract_biodiversity_source(archive_path, extract_dir)
                frame = _read_fungal_rows_from_source(source_path, archive_samples)
            except Exception as exc:
                print(f"[WARN] Could not reconstruct {object_name}: {exc}")
                continue

            if frame.empty:
                print(f"[WARN] No positive ITS rows reconstructed from {object_name}")
                continue

            frame["sample_id"] = frame["sample_id"].astype(str).str.upper()
            found = set(frame["sample_id"].unique())
            reconstructed_ids.update(found)
            frames.append(frame)

            missing = archive_samples - found
            if missing:
                print(
                    f"[WARN] Archive {object_name} did not yield ITS data for "
                    f"{len(missing)} mapped samples: {sorted(missing)[:10]}"
                )

    combined = (
        pd.concat(frames, ignore_index=True)
        if frames
        else pd.DataFrame(columns=["sample_id", "otu_id", "count", "taxa"])
    )
    return combined, current_ids, reconstructed_ids


def fetch_fungal_guild_source_data(
    mclient,
    sample_ids: set[str] | None = None,
) -> pd.DataFrame:
    """
    Build the effective fungal source dataset.

    Current raw archives take precedence. The legacy sample_otu_counts table is
    retained only for samples that have no current sample_taxon_abundance
    provenance, so old data cannot override a newer upload.
    """
    archive_df, current_ids, reconstructed_ids = (
        fetch_current_fungal_data_from_archives(mclient, sample_ids)
    )

    legacy_df = fetch_otu_data(marker="ITS")
    if not legacy_df.empty:
        legacy_df["sample_id"] = legacy_df["sample_id"].astype(str).str.upper()
        if sample_ids:
            legacy_df = legacy_df[legacy_df["sample_id"].isin(sample_ids)].copy()
        legacy_df = legacy_df[~legacy_df["sample_id"].isin(current_ids)].copy()

    failed_current = current_ids - reconstructed_ids
    print(f"[INFO] Current ITS samples linked to raw archives: {len(current_ids)}")
    print(f"[INFO] Current ITS samples reconstructed: {len(reconstructed_ids)}")
    print(
        "[INFO] Legacy-only ITS samples retained: "
        f"{legacy_df['sample_id'].nunique() if not legacy_df.empty else 0}"
    )
    if failed_current:
        print(
            "[WARN] Current ITS samples not reconstructed and not replaced by "
            f"stale legacy rows: {len(failed_current)}"
        )

    frames = [df for df in (archive_df, legacy_df) if not df.empty]
    if not frames:
        return pd.DataFrame(columns=["sample_id", "otu_id", "count", "taxa"])

    combined = pd.concat(frames, ignore_index=True)
    print(f"[INFO] Effective ITS guild samples: {combined['sample_id'].nunique()}")
    return combined


def fetch_taxon_abundance(
    marker: str = "16S",
    level: str = "Phylum",
) -> pd.DataFrame:
    """
    Fetch compact taxonomic statistics produced by the current biodiversity
    importer.
    """
    sql = """
        SELECT
            sample_id,
            taxon,
            read_count AS count,
            relative_abundance_pct
        FROM sample_taxon_abundance
        WHERE marker = %s
          AND level = %s
        ORDER BY sample_id, read_count DESC, taxon
    """
    with get_pg_conn() as conn:
        return pd.read_sql(
            sql,
            conn,
            params=[marker.upper(), level],
        )


TAX_PREFIX_TO_RANK = {
    "k": "Kingdom",
    "p": "Phylum",
    "c": "Class",
    "o": "Order",
    "f": "Family",
    "g": "Genus",
    "s": "Species",
}


def clean_taxon_value(value: str) -> str:
    """
    Clean values like:
      g__Fusarium
      f__Nectriaceae
      Capnodiales_fam_Incertae_sedis

    Returns a display-friendly value, or "" if it is not useful.
    """
    if value is None:
        return ""

    s = str(value).strip()
    if not s:
        return ""

    # Remove rank prefix if still present
    s = re.sub(r"^[a-zA-Z]__", "", s)

    # Convert underscores to spaces for display / matching
    s = s.replace("_", " ").strip()

    # Drop low-information labels
    if re.search(r"incertae|unclassified|uncultured|unknown", s, flags=re.I):
        return ""

    # Drop generic species placeholders such as "Capnodiales sp"
    if re.search(r"\bsp\.?$", s, flags=re.I):
        return ""

    return s


def parse_taxonomy_string(raw: str) -> dict:
    """
    Parse a single taxonomy string like:
      k__Fungi;p__Ascomycota;c__Dothideomycetes;o__Capnodiales;f__...;g__...;s__...

    Returns:
      {
        "Kingdom": "Fungi",
        "Phylum": "Ascomycota",
        "Class": "Dothideomycetes",
        "Order": "Capnodiales",
        "Family": "...",
        "Genus": "...",
        "Species": "..."
      }
    """
    out = {}

    if raw is None:
        return out

    s = str(raw).strip()
    if not s:
        return out

    # Accept semicolon, pipe, or comma-separated taxonomy strings
    parts = re.split(r"\s*[;|]\s*", s)

    for part in parts:
        part = part.strip()
        if not part:
            continue

        m = re.match(r"^([kpcofgs])__?(.*)$", part, flags=re.I)
        if not m:
            continue

        prefix = m.group(1).lower()
        value = m.group(2).strip()
        rank = TAX_PREFIX_TO_RANK.get(prefix)
        if not rank:
            continue

        cleaned = clean_taxon_value(value)
        if cleaned:
            out[rank] = cleaned

    return out


def taxa_to_normalized_dict(taxa) -> dict:
    """
    Normalize taxa from Postgres sample_otu_counts.taxa.

    Supports:
      - dict with Taxonomy raw string
      - dict with A/B/C/D/E/F columns
      - dict with named ranks
      - JSON string
      - raw taxonomy string
    """
    d = _taxa_to_dict(taxa)

    # Case 1: taxa is a raw taxonomy string, not JSON
    if not d and isinstance(taxa, str):
        parsed = parse_taxonomy_string(taxa)
        if parsed:
            return parsed

    out = {}

    # Case 2: raw taxonomy column inside JSON/dict
    raw_tax = d.get("Taxonomy") or d.get("taxonomy") or d.get("taxon") or d.get("Taxon") or ""
    if raw_tax:
        out.update(parse_taxonomy_string(raw_tax))

    # Case 3: named rank columns already present
    for rank in ("Kingdom", "Phylum", "Class", "Order", "Family", "Genus", "Species"):
        val = d.get(rank) or d.get(rank.lower())
        cleaned = clean_taxon_value(val)
        if cleaned:
            out[rank] = cleaned

    # Case 4: old A/B/C/D/E/F style.
    # Based on your Excel: Taxonomy = kingdom, A=phylum, B=class,
    # C=order, D=family, E=genus, F=species.
    letter_map = {
        "A": "Phylum",
        "B": "Class",
        "C": "Order",
        "D": "Family",
        "E": "Genus",
        "F": "Species",
    }
    for key, rank in letter_map.items():
        cleaned = clean_taxon_value(d.get(key))
        if cleaned and rank not in out:
            out[rank] = cleaned

    return out


def extract_taxon_label(row: pd.Series, level: str) -> str:
    level = normalize_taxonomic_level(level)
    taxa = taxa_to_normalized_dict(row.get("taxa"))

    val = taxa.get(level)
    cleaned = clean_taxon_value(val)

    if cleaned:
        return cleaned

    return "Unclassified"


def run_with_heartbeat(label, func, interval=30):
    """
    Run a blocking operation while printing a heartbeat periodically.

    Useful for long PostgreSQL queries where no row-level progress is available.
    """
    started = time.monotonic()
    stop_event = threading.Event()

    print(f"[INFO] {label}...", flush=True)

    def heartbeat():
        while not stop_event.wait(interval):
            elapsed = time.monotonic() - started
            print(
                f"[INFO] {label}: still running "
                f"({elapsed / 60:.1f} min elapsed)",
                flush=True,
            )

    thread = threading.Thread(
        target=heartbeat,
        daemon=True,
    )
    thread.start()

    try:
        result = func()
    except Exception:
        elapsed = time.monotonic() - started
        print(
            f"[ERROR] {label} failed after {elapsed:.1f} s",
            flush=True,
        )
        raise
    finally:
        stop_event.set()
        thread.join(timeout=1)

    elapsed = time.monotonic() - started
    print(
        f"[OK] {label} finished in {elapsed:.1f} s",
        flush=True,
    )

    return result
    

def make_piechart_for_sample(
    sample_df: pd.DataFrame, sample_id: str, marker: str, level: str, out_path: Path
):
    plot_df = sample_df.copy()

    # Current importer: taxon/read_count are already aggregated in
    # sample_taxon_abundance. Legacy OTU rows still need taxonomy extraction.
    if "taxon" in plot_df.columns:
        plot_df["taxon"] = (
            plot_df["taxon"]
            .fillna("Unclassified")
            .astype(str)
            .str.strip()
            .replace("", "Unclassified")
        )
    else:
        plot_df["taxon"] = plot_df.apply(
            lambda r: extract_taxon_label(r, level),
            axis=1,
        )

    plot_df["count"] = pd.to_numeric(
        plot_df["count"],
        errors="coerce",
    ).fillna(0)

    grouped = (
        plot_df.groupby("taxon", dropna=False)["count"]
        .sum()
        .reset_index()
        .sort_values("count", ascending=False)
    )

    grouped = grouped[grouped["count"] > 0].copy()
    if grouped.empty:
        return False

    # ---------- Collapse low-percentage taxa into Other ----------
    # First calculate percentages against the full sample total.
    total = grouped["count"].sum()
    grouped["pct"] = grouped["count"] / total * 100.0

    min_pct = float(os.getenv("BIODIV_MIN_TAXON_PCT", "1.0"))
    other_label = os.getenv("BIODIV_OTHER_LABEL", "Other")

    small = grouped[grouped["pct"] < min_pct].copy()
    large = grouped[grouped["pct"] >= min_pct].copy()

    other_count = small["count"].sum()

    if other_count > 0:
        other_row = pd.DataFrame(
            [
                {
                    "taxon": other_label,
                    "count": other_count,
                    "pct": other_count / total * 100.0,
                }
            ]
        )
        grouped = pd.concat([large, other_row], ignore_index=True)
    else:
        grouped = large

    # Sort again after adding Other.
    grouped = grouped.sort_values("count", ascending=False).reset_index(drop=True)

    # Optional: still limit the chart to top N visible labels.
    # If there are more than top_n categories above 1%, collapse the rest into Other too.
    top_n = int(os.getenv("BIODIV_TOP_N", "10"))

    if len(grouped) > top_n:
        existing_other = grouped[grouped["taxon"] == other_label].copy()
        main = grouped[grouped["taxon"] != other_label].copy()

        top = main.iloc[:top_n].copy()
        rest_count = main.iloc[top_n:]["count"].sum()

        if not existing_other.empty:
            rest_count += existing_other["count"].sum()

        if rest_count > 0:
            top = pd.concat(
                [
                    top,
                    pd.DataFrame(
                        [
                            {
                                "taxon": other_label,
                                "count": rest_count,
                                "pct": rest_count / total * 100.0,
                            }
                        ]
                    ),
                ],
                ignore_index=True,
            )

        grouped = top

    # Recalculate final percentages so labels always sum correctly.
    grouped["pct"] = grouped["count"] / total * 100.0

    # ---------- Figure ----------
    fig, ax = plt.subplots(figsize=(14, 10), facecolor=PIE_BG)
    ax.set_facecolor(PIE_BG)

    colors = PIE_COLORS[: len(grouped)]
    if len(colors) < len(grouped):
        # fallback if ever needed
        extra = ["#777777"] * (len(grouped) - len(colors))
        colors = colors + extra

    wedges, _ = ax.pie(
        grouped["pct"],
        startangle=90,
        colors=colors,
        labels=None,  # no labels directly on the pie
        counterclock=True,
        wedgeprops={
            "edgecolor": PIE_EDGE,
            "linewidth": 2.0,
        },
        radius=1.0,
    )

    ax.axis("equal")

    # ---------- Title ----------
    fig.suptitle(
        f"Top {min(top_n, len(grouped))} {level} — {sample_id} ({marker})",
        fontsize=22,
        fontweight="bold",
        color=PIE_TEXT,
        y=0.96,
    )

    # ---------- Legend ----------
    legend_labels = [
        f"{taxon} ({pct:.1f}%)" for taxon, pct in zip(grouped["taxon"], grouped["pct"])
    ]

    leg = ax.legend(
        wedges,
        legend_labels,
        loc="center left",
        bbox_to_anchor=(1.02, 0.5),
        frameon=False,
        fontsize=14,
        labelcolor=PIE_TEXT,
        handlelength=1.6,
        handleheight=1.6,
        borderaxespad=0.0,
    )

    # Some matplotlib versions ignore labelcolor above, so force it:
    for txt in leg.get_texts():
        txt.set_color(PIE_TEXT)

    # Remove axes junk
    ax.set_xticks([])
    ax.set_yticks([])

    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.tight_layout(rect=[0.02, 0.02, 0.82, 0.93])
    fig.savefig(
        out_path,
        dpi=300,
        bbox_inches="tight",
        facecolor=fig.get_facecolor(),
    )
    plt.close(fig)
    return True


def _current_raw_sample_sources(marker: str) -> pd.DataFrame:
    """
    Return the current structured-raw source and sample_index for every sample.

    sample_taxon_abundance.source_upload_id is the canonical pointer to the
    upload currently represented by the compact taxonomic data. If inconsistent
    historical rows ever exist, the newest biodiversity_uploads.uploaded_at wins.
    """
    sql = """
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
            WHERE UPPER(sta.marker) = UPPER(%s)
              AND sta.source_upload_id IS NOT NULL
            ORDER BY
                UPPER(sta.sample_id),
                UPPER(sta.marker),
                bu.uploaded_at DESC,
                sta.source_upload_id DESC
        )
        SELECT
            css.sample_id,
            css.marker,
            css.upload_id,
            rs.sample_index
        FROM current_sample_sources AS css
        LEFT JOIN biodiversity_raw_samples AS rs
          ON rs.upload_id = css.upload_id
         AND UPPER(rs.sample_id) = css.sample_id
         AND UPPER(rs.marker) = css.marker
        ORDER BY css.sample_id
    """

    with get_pg_conn() as conn:
        return pd.read_sql(sql, conn, params=[marker])


def fetch_current_raw_faprotax_data(
    marker: str = "16S",
    min_prev: int = 2,
    min_total: int = 50,
) -> tuple[pd.DataFrame, pd.DataFrame, list[str], int]:
    """
    Fetch the current sparse abundance matrix and taxonomy needed by FAPROTAX.

    Returns:
        counts_df:
            sample_id, otu_id, count

        taxonomy_df:
            one or more taxonomy variants per OTU, including source provenance

        current_sample_ids:
            complete current sample set

        otu_before:
            number of distinct OTUs before FAPROTAX filtering

    The expensive filtering is performed once in PostgreSQL. Taxonomy is fetched
    separately so it is not repeated for every non-zero abundance value.
    """
    marker = marker.upper()

    sources = run_with_heartbeat(
        "[1/7] Resolving current sample sources",
        lambda: _current_raw_sample_sources(marker),
        interval=15,
    )

    if sources.empty:
        raise RuntimeError(
            f"No current sample_taxon_abundance sources found for marker={marker}"
        )

    print(
        f"[INFO] Current {marker} samples resolved: "
        f"{sources['sample_id'].nunique()}",
        flush=True,
    )

    missing_raw = sources[sources["sample_index"].isna()].copy()

    if not missing_raw.empty:
        missing_ids = sorted(
            missing_raw["sample_id"].astype(str).unique()
        )
        preview = ", ".join(missing_ids[:20])

        raise RuntimeError(
            f"Structured raw data are incomplete for marker={marker}: "
            f"{len(missing_ids)} current samples are missing raw rows. "
            f"Examples: {preview}. Run the biodiversity raw backfill first."
        )

    current_sample_ids = sorted(
        sources["sample_id"]
        .astype(str)
        .str.strip()
        .str.upper()
        .unique()
        .tolist()
    )

    with get_pg_conn() as conn:
        with conn.cursor() as cur:

            # ----------------------------------------------------------
            # Current sample -> raw upload/sample-column mapping
            # ----------------------------------------------------------
            print(
                "[INFO] [2/7] Building current-sample working table...",
                flush=True,
            )

            cur.execute(
                """
                CREATE TEMP TABLE _fap_current_samples
                ON COMMIT DROP
                AS
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
                    WHERE UPPER(sta.marker) = UPPER(%s)
                      AND sta.source_upload_id IS NOT NULL
                    ORDER BY
                        UPPER(sta.sample_id),
                        UPPER(sta.marker),
                        bu.uploaded_at DESC,
                        sta.source_upload_id DESC
                )
                SELECT
                    css.sample_id,
                    css.upload_id,
                    rs.sample_index
                FROM current_sample_sources AS css
                JOIN biodiversity_raw_samples AS rs
                  ON rs.upload_id = css.upload_id
                 AND UPPER(rs.sample_id) = css.sample_id
                 AND UPPER(rs.marker) = UPPER(%s)
                """,
                (marker, marker),
            )

            cur.execute(
                """
                CREATE UNIQUE INDEX
                ON _fap_current_samples (
                    sample_id,
                    upload_id,
                    sample_index
                )
                """
            )

            cur.execute(
                """
                CREATE INDEX
                ON _fap_current_samples (
                    upload_id,
                    sample_index
                )
                """
            )

            cur.execute("ANALYZE _fap_current_samples")

            cur.execute(
                """
                SELECT
                    sample_id,
                    COUNT(*) AS n
                FROM _fap_current_samples
                GROUP BY sample_id
                HAVING COUNT(*) <> 1
                ORDER BY sample_id
                LIMIT 20
                """
            )

            ambiguous_samples = cur.fetchall()

            if ambiguous_samples:
                raise RuntimeError(
                    "Some current 16S samples resolve to more than one raw "
                    "sample column. Examples: "
                    + ", ".join(
                        f"{sample_id} ({n} rows)"
                        for sample_id, n in ambiguous_samples
                    )
                )

            cur.execute(
                "SELECT COUNT(*) FROM _fap_current_samples"
            )
            resolved_count = int(cur.fetchone()[0])

            if resolved_count != len(current_sample_ids):
                raise RuntimeError(
                    "Current raw sample resolution mismatch: "
                    f"expected={len(current_sample_ids)}, "
                    f"resolved={resolved_count}"
                )

            print(
                f"[OK] Current raw sample columns validated: "
                f"{resolved_count:,}",
                flush=True,
            )
            # ----------------------------------------------------------
            # Count distinct OTUs before the FAPROTAX-specific filter
            # ----------------------------------------------------------
            started = time.monotonic()

            print(
                "[INFO] [3/7] Counting current raw OTUs...",
                flush=True,
            )

            cur.execute(
                """
                SELECT COUNT(DISTINCT f.source_feature_id)
                FROM _fap_current_samples AS cs
                JOIN biodiversity_raw_abundance AS a
                  ON a.upload_id = cs.upload_id
                 AND a.sample_index = cs.sample_index
                JOIN biodiversity_raw_features AS f
                  ON f.upload_id = a.upload_id
                 AND f.feature_index = a.feature_index
                WHERE a.read_count > 0
                  AND NULLIF(BTRIM(f.source_feature_id), '') IS NOT NULL
                """
            )

            otu_before = int(cur.fetchone()[0] or 0)

            print(
                f"[OK] Raw OTUs before FAPROTAX filtering: "
                f"{otu_before:,} "
                f"({time.monotonic() - started:.1f} s)",
                flush=True,
            )

            # ----------------------------------------------------------
            # Calculate eligible OTUs ONCE.
            # ----------------------------------------------------------
            started = time.monotonic()

            print(
                "[INFO] [4/7] Identifying eligible FAPROTAX OTUs "
                f"(min_prev={min_prev}, min_total={min_total})...",
                flush=True,
            )

            cur.execute(
                """
                CREATE TEMP TABLE _fap_eligible_otus
                ON COMMIT DROP
                AS
                SELECT
                    f.source_feature_id AS otu_id
                FROM _fap_current_samples AS cs
                JOIN biodiversity_raw_abundance AS a
                  ON a.upload_id = cs.upload_id
                 AND a.sample_index = cs.sample_index
                JOIN biodiversity_raw_features AS f
                  ON f.upload_id = a.upload_id
                 AND f.feature_index = a.feature_index
                WHERE a.read_count > 0
                  AND NULLIF(BTRIM(f.source_feature_id), '') IS NOT NULL
                GROUP BY f.source_feature_id
                HAVING COUNT(DISTINCT cs.sample_id) >= %s
                   AND SUM(a.read_count) >= %s
                """,
                (
                    int(min_prev),
                    int(min_total),
                ),
            )

            cur.execute(
                """
                CREATE UNIQUE INDEX
                ON _fap_eligible_otus (otu_id)
                """
            )

            cur.execute("ANALYZE _fap_eligible_otus")

            cur.execute(
                "SELECT COUNT(*) FROM _fap_eligible_otus"
            )

            eligible_count = int(cur.fetchone()[0])

            print(
                f"[OK] Eligible OTUs: {eligible_count:,} "
                f"({time.monotonic() - started:.1f} s)",
                flush=True,
            )

        # --------------------------------------------------------------
        # Sparse abundance matrix.
        #
        # IMPORTANT:
        #   - no taxonomy repeated millions of times
        #   - no unnecessary ORDER BY
        #   - aggregate by sample + biological OTU
        # --------------------------------------------------------------
        counts_sql = """
            SELECT
                cs.sample_id,
                f.source_feature_id AS otu_id,
                SUM(a.read_count)::double precision AS count
            FROM _fap_current_samples AS cs
            JOIN biodiversity_raw_abundance AS a
              ON a.upload_id = cs.upload_id
             AND a.sample_index = cs.sample_index
            JOIN biodiversity_raw_features AS f
              ON f.upload_id = a.upload_id
             AND f.feature_index = a.feature_index
            JOIN _fap_eligible_otus AS e
              ON e.otu_id = f.source_feature_id
            WHERE a.read_count > 0
            GROUP BY
                cs.sample_id,
                f.source_feature_id
        """

        counts_df = run_with_heartbeat(
            "[5/7] Loading sparse eligible OTU abundances",
            lambda: pd.read_sql(
                counts_sql,
                conn,
            ),
            interval=30,
        )

        # --------------------------------------------------------------
        # Taxonomy fetched separately.
        #
        # Keep upload/file provenance here because it is useful when
        # diagnosing cross-source taxonomy disagreements.
        # --------------------------------------------------------------
        taxonomy_sql = """
            SELECT DISTINCT
                f.source_feature_id AS otu_id,
                f.upload_id,
                bu.original_filename,
                f.kingdom,
                f.phylum,
                f.class_name,
                f.order_name,
                f.family,
                f.genus,
                f.species
            FROM biodiversity_raw_features AS f
            JOIN (
                SELECT DISTINCT upload_id
                FROM _fap_current_samples
            ) AS current_uploads
              ON current_uploads.upload_id = f.upload_id
            JOIN _fap_eligible_otus AS e
              ON e.otu_id = f.source_feature_id
            JOIN biodiversity_uploads AS bu
              ON bu.upload_id = f.upload_id
            WHERE NULLIF(BTRIM(f.source_feature_id), '') IS NOT NULL
        """

        taxonomy_df = run_with_heartbeat(
            "[6/7] Loading taxonomy for eligible OTUs",
            lambda: pd.read_sql(
                taxonomy_sql,
                conn,
            ),
            interval=30,
        )

    if counts_df.empty:
        raise RuntimeError(
            f"No current raw OTUs survive FAPROTAX filtering "
            f"for marker={marker} "
            f"(min_prev={min_prev}, min_total={min_total})"
        )

    counts_df["sample_id"] = (
        counts_df["sample_id"]
        .astype(str)
        .str.strip()
        .str.upper()
    )

    counts_df["otu_id"] = (
        counts_df["otu_id"]
        .astype(str)
        .str.strip()
    )

    counts_df["count"] = pd.to_numeric(
        counts_df["count"],
        errors="coerce",
    ).fillna(0)

    taxonomy_df["otu_id"] = (
        taxonomy_df["otu_id"]
        .astype(str)
        .str.strip()
    )

    print(
        f"[OK] Sparse FAPROTAX data: "
        f"{len(counts_df):,} non-zero sample/OTU rows; "
        f"{counts_df['otu_id'].nunique():,} OTUs",
        flush=True,
    )

    print(
        f"[OK] Taxonomy source rows: {len(taxonomy_df):,}",
        flush=True,
    )

    return (
        counts_df,
        taxonomy_df,
        current_sample_ids,
        otu_before,
    )


def build_clean_otu_and_taxonomy_files(
    marker: str = "16S",
    out_dir: Path | None = None,
    min_prev: int = 2,
    min_total: int = 50,
) -> tuple[Path, Path]:
    """
    Build R-compatible FAPROTAX inputs from the current structured raw tables.

    Produces:
      - 6_otu_clean_counts_no_blanks.csv
      - 7_taxonomy_clean.csv

    Only the source_upload_id currently referenced by sample_taxon_abundance is
    used for each sample. Historical/replaced uploads are therefore excluded.

    Filtering is equivalent to the previous implementation:
      - keep OTUs present in at least min_prev samples
      - keep OTUs with at least min_total total reads
    """
    marker = marker.upper()
    if marker != "16S":
        raise ValueError("FAPROTAX input generation is intended for marker=16S")

    if out_dir is None:
        out_dir = PROJECT_ROOT / "data" / "biodiversity" / "faprotax_work"

    out_dir.mkdir(parents=True, exist_ok=True)

    print(
        f"[INFO] Building FAPROTAX inputs from current structured raw marker={marker} "
        f"(min_prev={min_prev}, min_total={min_total})"
    )

    (
        counts_df,
        taxonomy_raw_df,
        current_sample_ids,
        otu_before,
    ) = fetch_current_raw_faprotax_data(
        marker=marker,
        min_prev=min_prev,
        min_total=min_total,
    )

    print(
        "[INFO] [7/7] Normalising and reconciling taxonomy...",
        flush=True,
    )

    taxonomy_df = reconcile_faprotax_taxonomy(
        taxonomy_raw_df
    )

    print(
        f"[INFO] Current structured raw samples: "
        f"{len(current_sample_ids)}"
    )

    print(
        f"[INFO] OTUs before filtering: {otu_before:,}"
    )

    print(
        f"[INFO] OTUs after filtering: "
        f"{counts_df['otu_id'].nunique():,}"
    )

    print(
        f"[INFO] Non-zero values after filtering: "
        f"{len(counts_df):,}"
    )

    # ------------------------------------------------------------------
    # 1) OTU count matrix: rows = OTU IDs, columns = current sample IDs
    # ------------------------------------------------------------------
    step_started = time.monotonic()

    print(
        "[INFO] [5/6] Building dense OTU x sample matrix...",
        flush=True,
    )

    otu_clean = counts_df.pivot_table(
        index="otu_id",
        columns="sample_id",
        values="count",
        aggfunc="sum",
        fill_value=0,
    ).sort_index()

    # Keep the complete current sample set in a stable order. Normally every
    # sample has at least one retained OTU; zero-only samples are reported.
    otu_clean = otu_clean.reindex(
        columns=current_sample_ids,
        fill_value=0,
    )

    print(
        f"[OK] Matrix built: "
        f"{otu_clean.shape[0]:,} OTUs x "
        f"{otu_clean.shape[1]:,} samples "
        f"in {time.monotonic() - step_started:.1f} s",
        flush=True,
    )

    zero_samples = [
        str(sample_id)
        for sample_id, total in otu_clean.sum(axis=0).items()
        if float(total) <= 0
    ]
    if zero_samples:
        print(
            "[WARN] Current samples with no reads after OTU filtering: "
            f"{len(zero_samples)}; examples={zero_samples[:20]}"
        )

    # ------------------------------------------------------------------
    # 2) Taxonomy table for the retained OTUs
    # ------------------------------------------------------------------
    tax_cols = [
        "kingdom",
        "phylum",
        "class_name",
        "order_name",
        "family",
        "genus",
        "species",
    ]

    count_otus = set(
        otu_clean.index.astype(str)
    )

    taxonomy_otus = set(
        taxonomy_df["otu_id"].astype(str)
    )

    missing_taxonomy = count_otus - taxonomy_otus

    if missing_taxonomy:
        raise RuntimeError(
            "Some retained OTUs have no reconciled taxonomy: "
            f"{len(missing_taxonomy)} missing. "
            f"Examples: {sorted(missing_taxonomy)[:20]}"
        )

    tax_source = (
        taxonomy_df
        .set_index("otu_id")
        .reindex(otu_clean.index)
    )

    tax_df = tax_source.rename(
        columns={
            "kingdom": "Kingdom",
            "phylum": "Phylum",
            "class_name": "Class",
            "order_name": "Order",
            "family": "Family",
            "genus": "Genus",
            "species": "Species",
        }
    )[
        [
            "Kingdom",
            "Phylum",
            "Class",
            "Order",
            "Family",
            "Genus",
            "Species",
        ]
    ].fillna("")

    tax_df.index.name = "OTU_ID"

    # ------------------------------------------------------------------
    # 3) Write files expected by the R/FAPROTAX workflow
    # ------------------------------------------------------------------
    otu_path = out_dir / "6_otu_clean_counts_no_blanks.csv"
    tax_path = out_dir / "7_taxonomy_clean.csv"

    print(
        "[INFO] [6/6] Writing FAPROTAX input CSV files...",
        flush=True,
    )

    write_started = time.monotonic()

    otu_tmp = otu_path.with_name(otu_path.name + ".tmp")
    tax_tmp = tax_path.with_name(tax_path.name + ".tmp")

    try:
        otu_clean.to_csv(otu_tmp)
        tax_df.to_csv(tax_tmp, sep=";")

        # Basic integrity checks before replacing the official files.
        if not otu_tmp.exists() or otu_tmp.stat().st_size == 0:
            raise RuntimeError("Temporary OTU file was not written correctly")

        if not tax_tmp.exists() or tax_tmp.stat().st_size == 0:
            raise RuntimeError("Temporary taxonomy file was not written correctly")

        os.replace(otu_tmp, otu_path)
        os.replace(tax_tmp, tax_path)

    finally:
        otu_tmp.unlink(missing_ok=True)
        tax_tmp.unlink(missing_ok=True)

    print(
        f"[OK] CSV files written in "
        f"{time.monotonic() - write_started:.1f} s",
        flush=True,
    )
    
    print(f"[OK] Wrote {otu_path}")
    print(f"[OK] Wrote {tax_path}")
    print(
        f"[OK] FAPROTAX input matrix: {otu_clean.shape[0]} OTUs x "
        f"{otu_clean.shape[1]} samples"
    )

    return otu_path, tax_path


# ---------------------------------------------------------------------------
# Fungal ecological guild plots, based on FUNGuild genus-level assignments
# ---------------------------------------------------------------------------

FUNGUILD_KEEP_CONFIDENCE = {
    "Probable",
    "Highly Probable",
    "Higly Probable",  # typo present in some FUNGuild outputs
}

FUNGUILD_CONF_SCORE = {
    "Highly Probable": 3,
    "Higly Probable": 3,
    "Probable": 2,
    "Possible": 1,
}

FUNGAL_GUILD_MACRO_MAP = {
    "Ectomycorrhizal": "Ectomycorrhizal fungi",
    "Arbuscular Mycorrhizal": "Arbuscular mycorrhizal fungi",
    "Ericoid Mycorrhizal": "Mycorrhizal fungi",
    "Orchid Mycorrhizal": "Mycorrhizal fungi",
    "Wood Saprotroph": "Wood decomposers",
    "Litter Saprotroph": "Litter decomposers",
    "Plant Saprotroph": "Plant litter decomposers",
    "Dung Saprotroph": "Dung decomposers",
    "Undefined Saprotroph": "Decomposers (unspecified)",
    "Plant Pathogen": "Plant pathogens",
    "Animal Pathogen": "Animal pathogens",
    "Animal Parasite": "Animal pathogens",
    "Endophyte": "Endophytes",
    "Fungal Parasite": "Fungal parasites",
    "Lichen Parasite": "Lichen parasites",
    "Lichenized": "Lichenized fungi",
    "Nematophagous": "Nematophagous fungi",
    "Algal Parasite": "Algal parasites",
    "Insect Pathogen": "Insect pathogens",
    "Epiphyte": "Endophytes",
    "Pollen Saprotroph": "Decomposers (unspecified)",
}

FUNGAL_GUILD_ORDER = [
    "Ectomycorrhizal fungi",
    "Arbuscular mycorrhizal fungi",
    "Mycorrhizal fungi",
    "Wood decomposers",
    "Litter decomposers",
    "Plant litter decomposers",
    "Dung decomposers",
    "Decomposers (unspecified)",
    "Plant pathogens",
    "Animal pathogens",
    "Endophytes",
    "Fungal parasites",
    "Lichen parasites",
    "Lichenized fungi",
    "Nematophagous fungi",
    "Algal parasites",
    "Insect pathogens",
]

FUNGAL_GUILD_COLORS = {
    "Ectomycorrhizal fungi": "#264653",
    "Arbuscular mycorrhizal fungi": "#2A9D8F",
    "Mycorrhizal fungi": "#457B9D",
    "Wood decomposers": "#8B5E3C",
    "Litter decomposers": "#C9A96E",
    "Plant litter decomposers": "#E9C46A",
    "Dung decomposers": "#A8DADC",
    "Decomposers (unspecified)": "#BDB2A7",
    "Plant pathogens": "#E76F51",
    "Animal pathogens": "#F4A261",
    "Endophytes": "#6A994E",
    "Fungal parasites": "#BC6C25",
    "Lichen parasites": "#8D99AE",
    "Lichenized fungi": "#CDB4DB",
    "Nematophagous fungi": "#FFAFCC",
    "Algal parasites": "#D4E09B",
    "Insect pathogens": "#F08080",
}

# ---------------------------------------------------------------------------
# Bacterial ecological guild plots from FAPROTAX output
# ---------------------------------------------------------------------------

BACTERIAL_SOIL_CORE = [
    # Nitrogen
    "nitrogen_fixation",
    "nitrification",
    "aerobic_ammonia_oxidation",
    "nitrate_reduction",
    "nitrate_respiration",
    "nitrite_respiration",
    "nitrogen_respiration",
    "ureolysis",
    # Sulfur
    "sulfate_respiration",
    "sulfur_respiration",
    "sulfite_respiration",
    "respiration_of_sulfur_compounds",
    "dark_sulfide_oxidation",
    "dark_oxidation_of_sulfur_compounds",
    # Methane / C1
    "methanotrophy",
    "methanol_oxidation",
    "methylotrophy",
    "methanogenesis",
    "hydrogenotrophic_methanogenesis",
    "methanogenesis_by_reduction_of_methyl_compounds_with_H2",
    # Carbon degradation
    "cellulolysis",
    "xylanolysis",
    "aromatic_compound_degradation",
    "aromatic_hydrocarbon_degradation",
    "hydrocarbon_degradation",
    "aliphatic_non_methane_hydrocarbon_degradation",
    # Heterotrophy
    "aerobic_chemoheterotrophy",
    "anaerobic_chemoheterotrophy",
    "fermentation",
    # Mineral cycling
    "iron_respiration",
    "dark_iron_oxidation",
    "manganese_oxidation",
    # Pathogens / parasites / predation
    "plant_pathogen",
    "animal_parasite_or_symbiont",
    "predatory_or_exoparasitic",
    "chitinolysis",
    "nitrous_oxide_denitrification",
    "ligninolysis",
    "dark_hydrogen_oxidation",
    "phototrophy",
    "photoautotrophy",
    "cyanobacteria",
]

BACTERIAL_MACRO_MAP = {
    "chitinolysis": "Chitinolytic bacteria",
    "nitrogen_fixation": "Nitrogen fixers",
    "nitrification": "Nitrifiers",
    "aerobic_ammonia_oxidation": "Nitrifiers",
    "nitrate_reduction": "Denitrifiers",
    "nitrate_respiration": "Denitrifiers",
    "nitrite_respiration": "Denitrifiers",
    "nitrogen_respiration": "Denitrifiers",
    "nitrous_oxide_denitrification": "Denitrifiers",
    "ureolysis": "Ureolytic bacteria",
    "aromatic_compound_degradation": "Hydrocarbon degraders",
    "aromatic_hydrocarbon_degradation": "Hydrocarbon degraders",
    "hydrocarbon_degradation": "Hydrocarbon degraders",
    "aliphatic_non_methane_hydrocarbon_degradation": "Hydrocarbon degraders",
    "methanotrophy": "Methanotrophs",
    "methanol_oxidation": "Methanotrophs",
    "methylotrophy": "Methanotrophs",
    "methanogenesis": "Methanogens",
    "hydrogenotrophic_methanogenesis": "Methanogens",
    "methanogenesis_by_reduction_of_methyl_compounds_with_H2": "Methanogens",
    "dark_sulfide_oxidation": "Sulfur oxidizers",
    "dark_oxidation_of_sulfur_compounds": "Sulfur oxidizers",
    "sulfate_respiration": "Sulfate reducers",
    "sulfur_respiration": "Sulfate reducers",
    "sulfite_respiration": "Sulfate reducers",
    "respiration_of_sulfur_compounds": "Sulfate reducers",
    "iron_respiration": "Iron & Manganese cyclers",
    "dark_iron_oxidation": "Iron & Manganese cyclers",
    "manganese_oxidation": "Iron & Manganese cyclers",
    "fermentation": "Anaerobic heterotrophs",
    "aerobic_chemoheterotrophy": "Aerobic heterotrophs",
    "anaerobic_chemoheterotrophy": "Anaerobic heterotrophs",
    "plant_pathogen": "Plant pathogens",
    "animal_parasite_or_symbiont": "Animal parasites",
    "predatory_or_exoparasitic": "Predatory bacteria",
    "ligninolysis": "Lignocellulose degraders",
    "cellulolysis": "Lignocellulose degraders",
    "xylanolysis": "Lignocellulose degraders",
    "dark_hydrogen_oxidation": "Hydrogen oxidizers",
    "phototrophy": "Phototrophs",
    "photoautotrophy": "Phototrophs",
    "cyanobacteria": "Phototrophs",
}

BACTERIAL_GUILD_ORDER = [
    "Aerobic heterotrophs",
    "Anaerobic heterotrophs",
    "Nitrogen fixers",
    "Nitrifiers",
    "Denitrifiers",
    "Ureolytic bacteria",
    "Hydrocarbon degraders",
    "Methanotrophs",
    "Methanogens",
    "Sulfur oxidizers",
    "Sulfate reducers",
    "Iron & Manganese cyclers",
    "Plant pathogens",
    "Animal parasites",
    "Predatory bacteria",
    "Chitinolytic bacteria",
    "Lignocellulose degraders",
    "Hydrogen oxidizers",
    "Phototrophs",
]

BACTERIAL_GUILD_COLORS = {
    "Nitrogen fixers": "#264653",
    "Nitrifiers": "#2A9D8F",
    "Denitrifiers": "#457B9D",
    "Ureolytic bacteria": "#A8DADC",
    "Hydrocarbon degraders": "#C9A96E",
    "Methanotrophs": "#6A994E",
    "Methanogens": "#386641",
    "Sulfur oxidizers": "#FBF259",
    "Sulfate reducers": "#E9C46A",
    "Iron & Manganese cyclers": "#8D99AE",
    "Aerobic heterotrophs": "#E76F51",
    "Anaerobic heterotrophs": "#F4A261",
    "Plant pathogens": "#D62828",
    "Animal parasites": "#F08080",
    "Predatory bacteria": "#BC6C25",
    "Chitinolytic bacteria": "#CDB4DB",
    "Lignocellulose degraders": "#8B5E3C",
    "Hydrogen oxidizers": "#577590",
    "Phototrophs": "#D4E09B",
}


def _taxa_to_dict(taxa) -> dict:
    """
    sample_otu_counts.taxa may arrive as dict, JSON string, or None.
    """
    if isinstance(taxa, dict):
        return taxa
    if taxa is None:
        return {}
    if isinstance(taxa, str):
        s = taxa.strip()
        if not s:
            return {}
        try:
            obj = json.loads(s)
            return obj if isinstance(obj, dict) else {}
        except Exception:
            return {}
    return {}


def _strip_tax_prefix(value: str) -> str:
    """
    Convert g__Fusarium -> Fusarium, f__Nectriaceae -> Nectriaceae.
    Also replaces underscores with spaces.
    """
    if value is None:
        return ""
    s = str(value).strip()
    s = re.sub(r"^[a-zA-Z]__", "", s)
    s = s.replace("_", " ").strip()
    if not s:
        return ""
    if re.search(r"incertae|unclassified|uncultured", s, flags=re.I):
        return ""
    return s


def extract_fungal_genus(row: pd.Series) -> str:
    """
    Extract genus for FUNGuild matching.

    Supports both:
      - raw taxonomy string: k__;p__;c__;o__;f__;g__;s__
      - split fields: A/B/C/D/E/F
      - named rank fields: Genus
    """
    taxa = taxa_to_normalized_dict(row.get("taxa"))
    return taxa.get("Genus", "").strip()


def extract_primary_guild(guild_name: str) -> str:
    """
    FUNGuild guild names may contain a primary guild between pipes:
      Something-|Plant Pathogen|-Something
    If there are no pipes, fall back to the raw value or parts split by '-'.
    """
    if guild_name is None:
        return ""

    s = str(guild_name).strip()
    if not s or s.upper() == "NULL":
        return ""

    m = re.search(r"\|([^|]+)\|", s)
    if m:
        return m.group(1).strip()

    # fallback: try direct match first
    if s in FUNGAL_GUILD_MACRO_MAP:
        return s

    # fallback: split compound guilds
    for part in re.split(r"\s*-\s*", s):
        part = part.strip().replace("|", "")
        if part in FUNGAL_GUILD_MACRO_MAP:
            return part

    return s.replace("|", "").strip()


def load_funguild_best_by_genus() -> dict[str, dict]:
    """
    Load local FUNGuild_db.json and keep one best assignment per genus.

    Returns:
      {
        "Fusarium": {
          "guild": "...",
          "primary_guild": "...",
          "macro": "Plant pathogens",
          ...
        }
      }
    """
    path = Path(FUNGUILD_DB_JSON)
    if not path.exists():
        raise FileNotFoundError(
            f"FUNGuild DB JSON not found: {path}. Set FUNGUILD_DB_JSON or place the file there."
        )

    with path.open("r", encoding="utf-8") as f:
        raw = json.load(f)

    if isinstance(raw, dict):
        # Some JSON exports are dict-like. Try values.
        records = list(raw.values())
    elif isinstance(raw, list):
        records = raw
    else:
        raise ValueError(f"Unsupported FUNGuild JSON structure in {path}")

    best: dict[str, dict] = {}

    for rec in records:
        if not isinstance(rec, dict):
            continue

        taxon = str(rec.get("taxon") or rec.get("queried_taxon") or "").strip()
        if not taxon:
            continue

        confidence = str(rec.get("confidenceRanking") or "").strip()
        if confidence not in FUNGUILD_KEEP_CONFIDENCE:
            continue

        trophic = str(rec.get("trophicMode") or "").strip()
        guild = str(rec.get("guild") or "").strip()

        if not trophic or trophic.upper() == "NULL":
            continue
        if not guild or guild.upper() == "NULL":
            continue

        primary = extract_primary_guild(guild)
        macro = FUNGAL_GUILD_MACRO_MAP.get(primary)

        # Only keep guilds that map to citizen-friendly categories.
        if not macro:
            continue

        score = FUNGUILD_CONF_SCORE.get(confidence, 0)

        prev = best.get(taxon)
        if prev is None or score > prev["score"]:
            best[taxon] = {
                "taxon": taxon,
                "confidence": confidence,
                "score": score,
                "trophicMode": trophic,
                "guild": guild,
                "primary_guild": primary,
                "macro": macro,
            }

    print(f"[INFO] Loaded FUNGuild best assignments for {len(best)} genera")
    return best


def make_bacterial_guildplot_for_sample(
    sample_id: str,
    func_row: pd.Series,
    out_path: Path,
) -> bool:
    """
    Create one citizen-friendly bacterial ecological guild plot from one
    FAPROTAX sample row.

    Input values are expected to be FAPROTAX fractions, as in the R script.
    If values look like percentages already, the function handles that too.
    """
    values = {}

    for func_name, raw_val in func_row.items():
        if func_name not in BACTERIAL_SOIL_CORE:
            continue

        guild = BACTERIAL_MACRO_MAP.get(func_name)
        if not guild:
            continue

        try:
            v = float(raw_val)
        except Exception:
            continue

        if not math.isfinite(v) or v <= 0:
            continue

        values[guild] = values.get(guild, 0.0) + v

    if not values:
        return False

    df = pd.DataFrame([{"guild": k, "value": v} for k, v in values.items()])

    # File 8 contains FAPROTAX fractions.
    # Match the R workflow: Percent = 100 * sum(Value).
    df["percent"] = df["value"] * 100.0
    df = df[df["percent"] >= 1.0].copy()
    if df.empty:
        return False

    order_index = {name: i for i, name in enumerate(BACTERIAL_GUILD_ORDER)}
    df["order"] = df["guild"].map(lambda x: order_index.get(x, 999))
    df = df.sort_values(["order", "percent"], ascending=[True, False])

    # barh draws bottom-to-top, so reverse for top-to-bottom display.
    df = df.iloc[::-1].copy()

    labels = df["guild"].tolist()
    values_pct = df["percent"].tolist()
    colors = [BACTERIAL_GUILD_COLORS.get(label, "#999999") for label in labels]

    out_path.parent.mkdir(parents=True, exist_ok=True)

    fig_height = max(6.5, 0.55 * len(df) + 2.2)
    fig, ax = plt.subplots(figsize=(10, fig_height))

    # White background + black text style
    fig.patch.set_facecolor("white")
    ax.set_facecolor("white")

    y_pos = list(range(len(labels)))

    ax.barh(
        y_pos,
        values_pct,
        color=colors,
        height=0.75,
        edgecolor="none",
    )

    ax.set_yticks(y_pos)
    ax.set_yticklabels(labels, fontsize=11, color="#333333")

    x_max = max(max(values_pct) * 1.12, 20)
    ax.set_xlim(0, x_max)

    if x_max <= 20:
        ticks = [0, 5, 10, 15, 20]
    else:
        step = 5
        ticks = list(range(0, int(x_max + step), step))

    ax.set_xticks(ticks)
    ax.set_xticklabels([f"{t}%" for t in ticks], fontsize=10, color="#333333")

    ax.set_xlabel(
        "% of bacterial community",
        fontsize=12,
        color="#111111",
        labelpad=8,
    )

    ax.xaxis.grid(True, color="#dddddd", linewidth=1)
    ax.yaxis.grid(False)
    ax.set_axisbelow(True)

    for spine in ax.spines.values():
        spine.set_visible(False)

    ax.tick_params(axis="both", length=0)

    for y, pct in zip(y_pos, values_pct, strict=False):
        ax.text(
            pct + x_max * 0.006,
            y,
            f"{pct:.1f}%",
            va="center",
            ha="left",
            fontsize=11,
            color="#222222",
        )

    ax.set_title(
        "Soil bacterial ecological guilds\n",
        loc="left",
        fontsize=15,
        fontweight="bold",
        color="#111111",
        pad=8,
    )

    ax.text(
        0,
        1.02,
        "Guild-level functional categories",
        transform=ax.transAxes,
        ha="left",
        va="bottom",
        fontsize=11,
        color="#555555",
    )

    fig.suptitle(
        f"Your soil bacteria at a glance — Sample: {sample_id}",
        x=0.02,
        y=0.98,
        ha="left",
        va="top",
        fontsize=16,
        fontweight="bold",
        color="#111111",
    )

    caption = (
        "Values represent summed FAPROTAX-predicted functional abundance relative "
        "to the bacterial community.\n"
        "Functional assignments may overlap, so categories are not compositional "
        "and do not necessarily sum to 100%."
    )

    fig.text(
        0.02,
        0.025,
        caption,
        ha="left",
        va="bottom",
        fontsize=8.5,
        color="#666666",
    )

    fig.tight_layout(rect=[0.02, 0.09, 0.98, 0.92])
    fig.savefig(out_path, dpi=300, bbox_inches="tight", facecolor=fig.get_facecolor())
    plt.close(fig)

    return True


def make_fungal_guildplot_for_sample(
    sample_df: pd.DataFrame,
    sample_id: str,
    funguild_by_genus: dict[str, dict],
    out_path: Path,
) -> bool:
    """
    Create one citizen-friendly fungal guild horizontal bar plot.

    Percentages are relative to the full fungal community for the sample,
    matching the R script interpretation.

    Output style mirrors the R/ggplot example:
      - horizontal bars
      - fixed macro-category colours
      - title + subtitle
      - percentage labels at bar ends
      - explanatory caption
    """
    plot_df = sample_df.copy()
    plot_df["count"] = pd.to_numeric(plot_df["count"], errors="coerce").fillna(0)
    plot_df = plot_df[plot_df["count"] > 0].copy()

    if plot_df.empty:
        return False

    total = plot_df["count"].sum()
    if total <= 0:
        return False

    # Extract genus and map to citizen-friendly guild macro-category
    plot_df["genus"] = plot_df.apply(extract_fungal_genus, axis=1)
    plot_df["macro"] = plot_df["genus"].map(lambda g: funguild_by_genus.get(g, {}).get("macro", ""))

    annotated = plot_df[plot_df["macro"].astype(str).str.strip() != ""].copy()
    if annotated.empty:
        return False

    grouped = annotated.groupby("macro", dropna=False)["count"].sum().reset_index()
    grouped["percent"] = grouped["count"] / total * 100.0

    # Same communication threshold as the R script
    grouped = grouped[grouped["percent"] >= 1.0].copy()
    if grouped.empty:
        return False

    # Keep the same category order as the R script.
    order_index = {name: i for i, name in enumerate(FUNGAL_GUILD_ORDER)}
    grouped["order"] = grouped["macro"].map(lambda x: order_index.get(x, 999))
    grouped = grouped.sort_values(["order", "percent"], ascending=[True, False])

    # Matplotlib barh draws bottom-to-top, so reverse for top-to-bottom display.
    grouped = grouped.iloc[::-1].copy()

    labels = grouped["macro"].tolist()
    values = grouped["percent"].tolist()
    colors = [FUNGAL_GUILD_COLORS.get(label, "#999999") for label in labels]

    out_path.parent.mkdir(parents=True, exist_ok=True)

    # Similar aspect to the R output: wide, communication-oriented.
    fig_height = max(5.5, 0.55 * len(grouped) + 2.2)
    fig, ax = plt.subplots(figsize=(10, fig_height))

    # Warm, clean background like ggplot/theme_minimal
    fig.patch.set_facecolor("#f7f7f5")
    ax.set_facecolor("#f7f7f5")

    y_pos = list(range(len(labels)))

    ax.barh(
        y_pos,
        values,
        color=colors,
        height=0.75,
        edgecolor="none",
    )

    ax.set_yticks(y_pos)
    ax.set_yticklabels(labels, fontsize=11, color="#4d4d4d")

    # Axis max: at least 20%, otherwise 12% extra headroom
    x_max = max(max(values) * 1.12, 20)
    ax.set_xlim(0, x_max)

    # Use 0/5/10/15/20 style ticks where possible
    if x_max <= 20:
        ticks = [0, 5, 10, 15, 20]
    else:
        step = 5
        ticks = list(range(0, int(x_max + step), step))

    ax.set_xticks(ticks)
    ax.set_xticklabels([f"{t}%" for t in ticks], fontsize=10, color="#555555")

    ax.set_xlabel(
        "% of fungal community",
        fontsize=12,
        color="#111111",
        labelpad=8,
    )

    # Subtle vertical gridlines
    ax.xaxis.grid(True, color=PIE_GRID, linewidth=1)
    ax.yaxis.grid(False)
    ax.set_axisbelow(True)

    # Remove plot frame for ggplot-like look
    for spine in ax.spines.values():
        spine.set_visible(False)

    ax.tick_params(axis="both", length=0)

    # Percentage labels at bar ends
    for y, pct in zip(y_pos, values, strict=False):
        ax.text(
            pct + x_max * 0.006,
            y,
            f"{pct:.1f}%",
            va="center",
            ha="left",
            fontsize=11,
            color="#333333",
        )

    # Main chart title and subtitle
    ax.set_title(
        "Fungal ecological guilds\n",
        loc="left",
        fontsize=15,
        fontweight="bold",
        color="#111111",
        pad=8,
    )

    ax.text(
        0,
        1.02,
        "Guild macro-categories",
        transform=ax.transAxes,
        ha="left",
        va="bottom",
        fontsize=11,
        color="#666666",
    )

    # Figure-level top title, like the R patchwork annotation
    fig.suptitle(
        f"Your soil fungi at a glance — Sample: {sample_id}",
        x=0.02,
        y=0.98,
        ha="left",
        va="top",
        fontsize=16,
        fontweight="bold",
        color="#111111",
    )

    # Footer caption
    caption = (
        "Values indicate the estimated percentage of the fungal community associated with each ecological guild.\n"
        "Only guilds exceeding 1% are shown; absent categories may reflect low detection rather than true absence."
    )
    fig.text(
        0.02,
        0.025,
        caption,
        ha="left",
        va="bottom",
        fontsize=8.5,
        color="#777777",
    )

    # Leave room for suptitle and footer
    fig.tight_layout(rect=[0.02, 0.08, 0.98, 0.92])

    fig.savefig(out_path, dpi=300, bbox_inches="tight", facecolor=fig.get_facecolor())
    plt.close(fig)
    return True


def generate_bacterial_guildplots_from_faprotax(
    mclient,
    *,
    force: bool = False,
    sample_ids: set[str] | None = None,
    dry_run: bool = False,
) -> tuple[int, int]:
    """
    Generate bacterial ecological guild plots from a FAPROTAX sample x function CSV.

    Existing MinIO objects are skipped unless force=True.

    Expected input:
      rows    = sample IDs
      columns = FAPROTAX function names
      values  = fractions or percentages

    Uploads to:
      biodiversity/guildplots/bacteria/<sample_id>.png
    """
    path = Path(FAPROTAX_FUNCTION_CSV)
    if not path.exists():
        raise FileNotFoundError(
            f"FAPROTAX function CSV not found: {path}. "
            "Set FAPROTAX_FUNCTION_CSV or place the file there."
        )

    print(f"[INFO] Loading FAPROTAX functions from {path}")

    func_sxf = pd.read_csv(path, index_col=0)

    if func_sxf.empty:
        print("[INFO] FAPROTAX function matrix is empty; skipping bacterial guild plots.")
        return 0, 0

    # Validate file 8 against the current database before generating bacterial images.
    func_sxf.index = (
        func_sxf.index.astype(str)
        .str.strip()
        .str.upper()
    )

    if func_sxf.index.duplicated().any():
        dup = func_sxf.index[func_sxf.index.duplicated()].unique()
        raise RuntimeError(
            f"Duplicate sample IDs in FAPROTAX output: {list(dup[:20])}"
        )

    current = _current_raw_sample_sources("16S")
    expected = set(
        current["sample_id"]
        .astype(str)
        .str.strip()
        .str.upper()
    )

    actual = set(func_sxf.index)

    missing = expected - actual
    extra = actual - expected

    if missing or extra:
        raise RuntimeError(
            "FAPROTAX output does not match current 16S dataset: "
            f"current={len(expected)}, output={len(actual)}, "
            f"missing={len(missing)}, extra={len(extra)}. "
            f"Missing examples={sorted(missing)[:10]}; "
            f"extra examples={sorted(extra)[:10]}"
        )

    print(f"[INFO] FAPROTAX matrix: {func_sxf.shape[0]} samples x {func_sxf.shape[1]} functions")

    available = set(func_sxf.columns)
    selected = [c for c in BACTERIAL_SOIL_CORE if c in available]

    print(f"[INFO] Relevant FAPROTAX functions present: {len(selected)}")
    if selected:
        print("[INFO] First relevant functions:", ", ".join(selected[:20]))

    if not selected:
        print("[WARN] No expected FAPROTAX soil functions found in the CSV.")
        return 0, 0

    prefix = "biodiversity/guildplots/bacteria/"
    existing_objects = set() if force else list_existing_minio_objects(mclient, prefix)

    out_dir = PROJECT_ROOT / "data" / "bacterial_guildplots"
    out_dir.mkdir(parents=True, exist_ok=True)

    generated = 0
    uploaded = 0
    skipped_existing = 0
    selected_samples = 0

    for sample_id, row in func_sxf.iterrows():
        sample_id = str(sample_id).strip()
        if not sample_id:
            continue

        if sample_ids and sample_id.upper() not in sample_ids:
            continue

        selected_samples += 1
        safe_id = sanitize_filename(sample_id)
        object_name = f"{prefix}{safe_id}.png"

        if not force and object_name in existing_objects:
            skipped_existing += 1
            continue

        if dry_run:
            print(f"[NEW] would generate {object_name}")
            continue

        local_png = out_dir / f"{safe_id}.png"

        ok = make_bacterial_guildplot_for_sample(
            sample_id=sample_id,
            func_row=row[selected],
            out_path=local_png,
        )
        if not ok:
            continue

        generated += 1

        uploaded_url = upload_file_to_minio(
            mclient,
            local_png,
            object_name,
            content_type="image/png",
        )
        if uploaded_url:
            uploaded += 1

    print(f"[OK] Considered {selected_samples} bacterial guild samples")
    print(f"[OK] Skipped {skipped_existing} existing bacterial guild plots")
    print(f"[OK] Generated {generated} bacterial guild plots")
    print(f"[OK] Uploaded {uploaded} bacterial guild plots to MinIO")
    return generated, uploaded


def generate_fungal_guildplots(
    mclient,
    *,
    force: bool = False,
    sample_ids: set[str] | None = None,
    dry_run: bool = False,
) -> tuple[int, int]:
    """
    Generate fungal ecological guild plots from current raw ITS archives,
    with legacy OTU rows used only for samples that have no current upload.

    Existing MinIO objects are skipped unless force=True.

    Uploads to:
      biodiversity/guildplots/fungi/<sample_id>.png
    """
    marker = "ITS"
    print("[INFO] Generating fungal ecological guild plots from ITS data")

    df = fetch_fungal_guild_source_data(
        mclient,
        sample_ids=sample_ids,
    )
    if df.empty:
        print("[INFO] No ITS data available for fungal guild plots.")
        return 0, 0

    funguild_by_genus = load_funguild_best_by_genus()

    all_genera = df.apply(extract_fungal_genus, axis=1)
    nonempty = all_genera[all_genera.astype(str).str.strip() != ""]
    matched = nonempty.map(lambda g: g in funguild_by_genus)

    print(f"[DEBUG] Extracted non-empty genera: {len(nonempty)}")
    print(f"[DEBUG] Unique extracted genera: {nonempty.nunique()}")
    print(f"[DEBUG] FUNGuild genus matches: {matched.sum()} / {len(nonempty)}")

    prefix = "biodiversity/guildplots/fungi/"
    existing_objects = set() if force else list_existing_minio_objects(mclient, prefix)

    out_dir = PROJECT_ROOT / "data" / "biodiversity_guildplots" / "fungi"
    out_dir.mkdir(parents=True, exist_ok=True)

    generated = 0
    uploaded = 0
    skipped_existing = 0

    for sample_id, sample_df in df.groupby("sample_id"):
        sample_id = str(sample_id).strip()
        if not sample_id:
            continue

        safe_id = sanitize_filename(sample_id)
        object_name = f"{prefix}{safe_id}.png"

        if not force and object_name in existing_objects:
            skipped_existing += 1
            continue

        if dry_run:
            print(f"[NEW] would generate {object_name}")
            continue

        local_png = out_dir / f"{safe_id}.png"

        ok = make_fungal_guildplot_for_sample(
            sample_df=sample_df,
            sample_id=sample_id,
            funguild_by_genus=funguild_by_genus,
            out_path=local_png,
        )
        if not ok:
            continue

        generated += 1

        uploaded_url = upload_file_to_minio(
            mclient,
            local_png,
            object_name,
            content_type="image/png",
        )
        if uploaded_url:
            uploaded += 1

        if not uploaded_url:
            raise RuntimeError(f"Upload failed: {object_name}")

        local_png.unlink(missing_ok=True)

    print(f"[OK] Skipped {skipped_existing} existing fungal guild plots")
    print(f"[OK] Generated {generated} fungal guild plots")
    print(f"[OK] Uploaded {uploaded} fungal guild plots to MinIO")
    return generated, uploaded


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Generate biodiversity charts that are missing from MinIO. "
            "Existing objects are skipped by default."
        )
    )
    parser.add_argument(
        "--marker",
        default=os.getenv("BIODIV_MARKER", "16S"),
        choices=("16S", "ITS", "16s", "its"),
        help="Marker for taxonomic pie charts (default: BIODIV_MARKER or 16S).",
    )
    parser.add_argument(
        "--level",
        default=os.getenv("BIODIV_LEVEL", "Phylum"),
        help="Taxonomic level (current compact importer stores Phylum).",
    )
    parser.add_argument(
        "--sample-id",
        action="append",
        help=(
            "Generate only selected sample IDs. May be repeated or contain "
            "comma-separated IDs."
        ),
    )
    parser.add_argument(
        "--force",
        action="store_true",
        default=os.getenv("BIODIV_FORCE_REGENERATE", "0") == "1",
        help="Regenerate and overwrite charts even when they already exist.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show missing chart objects without creating or uploading them.",
    )
    parser.add_argument(
        "--all-images",
        action="store_true",
        help=(
            "Generate all image families: 16S and ITS taxonomic charts, "
            "bacterial guild plots, and fungal guild plots. Requires an "
            "up-to-date FAPROTAX_FUNCTION_CSV for bacterial guild plots."
        ),
    )
    parser.add_argument(
        "--fungal-guilds",
        action="store_true",
        default=GENERATE_FUNGAL_GUILDS,
        help=(
            "Generate ITS fungal ecological guild charts. "
            "Can also be enabled with GENERATE_FUNGAL_GUILDS=1."
        ),
    )
    parser.add_argument(
        "--bacterial-guilds",
        action="store_true",
        default=GENERATE_BACTERIAL_GUILDS,
        help=(
            "Generate 16S bacterial ecological guild charts from the "
            "configured FAPROTAX sample-by-function CSV. Can also be enabled "
            "with GENERATE_BACTERIAL_GUILDS=1."
        ),
    )
    parser.add_argument(
        "--build-faprotax-inputs",
        action="store_true",
        default=BUILD_FAPROTAX_INPUTS,
        help=(
            "Build FAPROTAX OTU-count and taxonomy input files. "
            "Can also be enabled with BUILD_FAPROTAX_INPUTS=1."
        ),
    )
    return parser.parse_args()



def generate_taxonomic_charts(
    *,
    marker: str,
    level: str,
    mclient,
    force: bool,
    sample_ids: set[str] | None,
    dry_run: bool,
) -> tuple[int, int]:
    """Generate one marker/level taxonomic chart family."""
    marker = marker.upper()
    level = normalize_taxonomic_level(level)

    out_dir = PROJECT_ROOT / "data" / "biodiversity_piecharts" / marker / level
    out_dir.mkdir(parents=True, exist_ok=True)

    df = fetch_taxon_abundance(marker=marker, level=level)

    if sample_ids and not df.empty:
        df = df[
            df["sample_id"]
            .fillna("")
            .astype(str)
            .str.upper()
            .isin(sample_ids)
        ].copy()

    generated = 0
    uploaded = 0
    skipped_existing = 0
    missing = 0

    prefix = f"biodiversity/piecharts/{marker}/{level}/"
    existing_objects = set() if force else list_existing_minio_objects(
        mclient,
        prefix,
    )

    if df.empty:
        print(
            "[INFO] No compact taxonomic rows found for "
            f"marker={marker}, level={level}."
        )
    else:
        for sample_id, sample_df in df.groupby("sample_id"):
            sample_id = str(sample_id).strip()
            if not sample_id:
                continue

            safe_id = sanitize_filename(sample_id)
            object_name = f"{prefix}{safe_id}.png"

            if not force and object_name in existing_objects:
                skipped_existing += 1
                continue

            missing += 1

            if dry_run:
                print(f"[NEW] would generate {object_name}")
                continue

            local_png = out_dir / f"{safe_id}.png"
            ok = make_piechart_for_sample(
                sample_df,
                sample_id,
                marker,
                level,
                local_png,
            )
            if not ok:
                print(f"[WARN] No positive chart data for {sample_id}")
                continue

            generated += 1

            uploaded_url = upload_file_to_minio(
                mclient,
                local_png,
                object_name,
                content_type="image/png",
            )
            if uploaded_url:
                uploaded += 1
            
            if not uploaded_url:
                raise RuntimeError(f"Upload failed: {object_name}")

            local_png.unlink(missing_ok=True)

    print(f"[OK] {marker}/{level}: missing taxonomic charts: {missing}")
    print(
        f"[OK] {marker}/{level}: skipped existing taxonomic charts: "
        f"{skipped_existing}"
    )
    print(f"[OK] {marker}/{level}: generated {generated} taxonomic charts")
    print(f"[OK] {marker}/{level}: uploaded {uploaded} taxonomic charts to MinIO")
    return generated, uploaded


def main():
    args = parse_args()

    marker = args.marker.upper()
    level = normalize_taxonomic_level(args.level)
    sample_ids = normalize_sample_filter(args.sample_id)

    print(
        f"[INFO] marker={marker} level={level} "
        f"all_images={args.all_images} force={args.force} dry_run={args.dry_run}"
    )
    if sample_ids:
        print(f"[INFO] sample filter: {sorted(sample_ids)}")

    # FAPROTAX input preparation is intentionally a separate phase because an
    # external FAPROTAX run must happen before bacterial guild images can use
    # the resulting sample-by-function CSV.
    if args.build_faprotax_inputs:
        if args.all_images or args.fungal_guilds or args.bacterial_guilds:
            raise SystemExit(
                "ERROR: --build-faprotax-inputs is a standalone phase. "
                "Build files 6/7, run FAPROTAX, then generate images."
            )

        build_clean_otu_and_taxonomy_files(
            marker=marker,
            out_dir=Path(
                os.getenv(
                    "FAPROTAX_WORK_DIR",
                    str(PROJECT_ROOT / "data" / "biodiversity" / "faprotax_work"),
                )
            ),
            min_prev=int(os.getenv("FAPROTAX_MIN_PREV", "2")),
            min_total=int(os.getenv("FAPROTAX_MIN_TOTAL", "50")),
        )

        if not args.fungal_guilds and not args.bacterial_guilds:
            return

    mclient = init_minio()

    if args.all_images:
        # Generate both taxonomic marker families.
        generate_taxonomic_charts(
            marker="16S",
            level=level,
            mclient=mclient,
            force=args.force,
            sample_ids=sample_ids,
            dry_run=args.dry_run,
        )
        generate_taxonomic_charts(
            marker="ITS",
            level=level,
            mclient=mclient,
            force=args.force,
            sample_ids=sample_ids,
            dry_run=args.dry_run,
        )

        # Then both ecological guild families.
        generate_bacterial_guildplots_from_faprotax(
            mclient,
            force=args.force,
            sample_ids=sample_ids,
            dry_run=args.dry_run,
        )
        generate_fungal_guildplots(
            mclient,
            force=args.force,
            sample_ids=sample_ids,
            dry_run=args.dry_run,
        )
        return

    # Original single-marker behavior.
    generate_taxonomic_charts(
        marker=marker,
        level=level,
        mclient=mclient,
        force=args.force,
        sample_ids=sample_ids,
        dry_run=args.dry_run,
    )

    if args.fungal_guilds:
        generate_fungal_guildplots(
            mclient,
            force=args.force,
            sample_ids=sample_ids,
            dry_run=args.dry_run,
        )

    if args.bacterial_guilds:
        generate_bacterial_guildplots_from_faprotax(
            mclient,
            force=args.force,
            sample_ids=sample_ids,
            dry_run=args.dry_run,
        )


if __name__ == "__main__":
    main()
