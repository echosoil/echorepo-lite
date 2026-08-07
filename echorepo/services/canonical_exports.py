from __future__ import annotations

import io
import os
import re
import zipfile
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from urllib.parse import urlsplit

import pandas as pd
from psycopg2.extras import RealDictCursor

from .db import get_pg_conn


ZENODO_DOI = "10.5281/zenodo.19722513"

SAMPLE_COLUMNS = [
    "sample_id",
    "timestamp_utc",
    "lat",
    "lon",
    "country_code",
    "location_accuracy_m",
    "ph",
    "organic_carbon_pct",
    "earthworms_count",
    "contamination_debris",
    "contamination_plastic",
    "contamination_other_orig",
    "contamination_other_en",
    "pollutants_count",
    "soil_structure_orig",
    "soil_structure_en",
    "soil_texture_orig",
    "soil_texture_en",
    "observations_orig",
    "observations_en",
    "metals_info_orig",
    "metals_info_en",
    "collected_by",
    "data_source",
    "qa_status",
    "licence",
]

# The PostgreSQL schema remains unchanged. These maps define only the
# names exposed by the canonical/public CSV resources.
SAMPLE_PUBLIC_COLUMN_MAP = {
    "timestamp_utc": "sampling_datetime_utc",
    "ph": "soil_ph_field",
    "organic_carbon_pct": "soil_organic_matter_estimate_pct",
    "location_accuracy_m": "coordinate_obfuscation_radius_m",
}

IMAGE_COLUMNS = [
    "sample_id",
    "country_code",
    "image_id",
    "image_url",
    "image_description_orig",
    "image_description_en",
    "collected_by",
    "timestamp_utc",
    "licence",
]

IMAGE_PUBLIC_COLUMN_MAP = {
    "timestamp_utc": "image_datetime_utc",
}

PARAMETER_COLUMNS = [
    "sample_id",
    "country_code",
    "parameter_code",
    "parameter_name",
    "value",
    "uom",
    "analysis_method",
    "analysis_date",
    "lab_id",
    "created_by",
    "licence",
    "parameter_uri",
]

PARAMETER_PUBLIC_COLUMN_MAP = {
    "value": "result_value",
    "uom": "unit",
    "analysis_date": "analysis_datetime_utc",
}

BIODIVERSITY_COLUMNS = [
    "sample_id",
    "country_code",
    "marker",
    "taxonomic_level",
    "taxon",
    "read_count",
    "relative_abundance_pct",
    "uploaded_at",
    "source_file",
    "licence",
]

BIODIVERSITY_PUBLIC_COLUMN_MAP = {
    "taxonomic_level": "taxon_rank",
    "taxon": "scientific_name",
    # The source column is sample_taxon_abundance.uploaded_at. It is not
    # necessarily the laboratory analysis time, so do not call it analysis_date.
    "uploaded_at": "ingested_datetime_utc",
}

ELEMENT_PARAMETER_NAMES = {
    "As": "Total arsenic concentration in soil",
    "Ca": "Total calcium concentration in soil",
    "Cd": "Total cadmium concentration in soil",
    "Co": "Total cobalt concentration in soil",
    "Cr": "Total chromium concentration in soil",
    "Cu": "Total copper concentration in soil",
    "Fe": "Total iron concentration in soil",
    "K": "Total potassium concentration in soil",
    "Mg": "Total magnesium concentration in soil",
    "Mn": "Total manganese concentration in soil",
    "Mo": "Total molybdenum concentration in soil",
    "Ni": "Total nickel concentration in soil",
    "P": "Total phosphorus concentration in soil",
    "Pb": "Total lead concentration in soil",
    "S": "Total sulfur concentration in soil",
    "Zn": "Total zinc concentration in soil",
}

DEFAULT_ELEMENT_ANALYSIS_METHOD = (
    "Micro-X-ray fluorescence (µXRF) after oven drying at 105 °C "
    "to constant weight"
)


@dataclass(frozen=True)
class ExportSpec:
    filename: str
    description: str
    notes: tuple[str, ...] = ()


@dataclass(frozen=True)
class CanonicalBundle:
    csv_contents: dict[str, str]
    zip_bytes: bytes


EXPORT_SPECS = {
    "samples.csv": ExportSpec(
        filename="samples.csv",
        description=(
            "Canonical sample-level data including location, field-estimated "
            "soil pH, soil organic matter, earthworms, visible contamination, "
            "soil structure, soil texture and participant observations."
        ),
    ),
    "sample_images.csv": ExportSpec(
        filename="sample_images.csv",
        description=(
            "Canonical image metadata linked to samples "
            "(IDs, URLs and descriptions)."
        ),
    ),
    "sample_parameters.csv": ExportSpec(
        filename="sample_parameters.csv",
        description=(
            "Canonical laboratory elemental concentration measurements "
            "associated with ECHOREPO soil samples."
        ),
        notes=(
            "Rows with numeric values below the configured canonical export "
            "threshold (default 0.01) are omitted.",
            "Oxide-form parameter rows are excluded from the canonical export "
            "by default.",
        ),
    ),
    "sample_biodiversity.csv": ExportSpec(
        filename="sample_biodiversity.csv",
        description=(
            "Microbial taxonomic abundance statistics by reported taxonomic rank "
            "per sample and molecular marker (for example 16S or ITS)."
        ),
        notes=(
            "Raw OTU-level source data are not included "
            "in this canonical export.",
        ),
    ),
}


def _utc_now_iso() -> str:
    return (
        datetime.now(timezone.utc)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z")
    )


def _normalise_sample_ids(
    sample_ids: Sequence[str] | None,
) -> list[str] | None:
    if sample_ids is None:
        return None

    cleaned = {
        str(sample_id).strip().upper()
        for sample_id in sample_ids
        if sample_id is not None
        and str(sample_id).strip()
    }

    return sorted(cleaned)


def _fetch_dataframe(
    sql: str,
    params: tuple,
    columns: list[str],
) -> pd.DataFrame:
    with get_pg_conn() as conn, conn.cursor(
        cursor_factory=RealDictCursor
    ) as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()

    if not rows:
        return pd.DataFrame(columns=columns)

    return pd.DataFrame(rows, columns=columns)


def looks_like_oxide(label: str) -> bool:
    """
    Return True for formulas such as SiO2, Al2O3, FeO, K2O or P2O5.
    """
    if not label:
        return False

    text = re.sub(
        r"\(.*?\)",
        "",
        str(label).strip(),
    )

    tokens = re.findall(
        r"[A-Z][a-z]?\d*",
        text,
    )

    return (
        len(tokens) >= 2
        and any(token.startswith("O") for token in tokens)
    )


def drop_parameter_values_below(
    df: pd.DataFrame,
    threshold: float = 0.01,
    value_col: str = "value",
) -> pd.DataFrame:
    """
    Remove rows whose numeric value is strictly below the threshold.

    Non-numeric values and values equal to the threshold are retained.
    """
    if (
        df is None
        or df.empty
        or value_col not in df.columns
    ):
        return df

    numeric_values = pd.to_numeric(
        df[value_col],
        errors="coerce",
    )

    keep = (
        numeric_values.isna()
        | (numeric_values >= threshold)
    )

    return df.loc[keep].copy()


def drop_oxide_rows(
    df: pd.DataFrame,
    code_col: str = "parameter_code",
    name_col: str = "parameter_name",
) -> pd.DataFrame:
    if df is None or df.empty:
        return df

    oxide_mask = pd.Series(
        False,
        index=df.index,
    )

    if code_col in df.columns:
        oxide_mask |= (
            df[code_col]
            .fillna("")
            .map(looks_like_oxide)
        )

    if name_col in df.columns:
        oxide_mask |= (
            df[name_col]
            .fillna("")
            .map(looks_like_oxide)
        )

    return df.loc[~oxide_mask].copy()

def _standardise_timestamp_columns(
    df: pd.DataFrame,
    timestamp_cols: list[str] | None = None,
) -> pd.DataFrame:
    """
    Convert timestamp columns to ISO 8601 UTC strings.

    Invalid values become empty in the CSV rather than being emitted in a
    non-standard timestamp format.
    """
    if df is None or df.empty:
        return df

    for col in timestamp_cols or []:
        if col not in df.columns:
            continue

        df[col] = pd.to_datetime(
            df[col],
            utc=True,
            errors="coerce",
        ).dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

    return df


def _standardise_nullable_integer_columns(
    df: pd.DataFrame,
    int_cols: list[str] | None = None,
) -> pd.DataFrame:
    """
    Convert integer-like columns to pandas nullable integers.

    A column is left unchanged if it contains a non-numeric value or a genuine
    fractional value. This avoids silently destroying source information in
    fields whose exact semantics are still being reviewed.
    """
    if df is None or df.empty:
        return df

    for col in int_cols or []:
        if col not in df.columns:
            continue

        original = df[col]
        non_empty = original.notna() & original.astype(str).str.strip().ne("")
        numeric = pd.to_numeric(original, errors="coerce")

        if numeric[non_empty].isna().any():
            continue

        non_null_numeric = numeric.dropna()
        if not non_null_numeric.empty and (
            (non_null_numeric % 1).abs() > 1e-12
        ).any():
            continue

        df[col] = numeric.astype("Int64")

    return df


def _enrich_parameter_metadata(df: pd.DataFrame) -> pd.DataFrame:
    """
    Improve public labels for elemental measurements without changing the DB.

    Existing non-empty human-readable names and methods are preserved.
    The ECHO field protocol specifies µXRF after oven drying at 105 °C to
    constant weight for the canonical heavy-metal/nutrient measurements.
    """
    if df is None or df.empty or "parameter_code" not in df.columns:
        return df

    code_series = df["parameter_code"].fillna("").astype(str).str.strip()
    code_folded = code_series.str.casefold()

    if "parameter_name" in df.columns:
        name_series = df["parameter_name"].fillna("").astype(str).str.strip()
        for code, label in ELEMENT_PARAMETER_NAMES.items():
            mask = code_folded.eq(code.casefold())
            replace_name = name_series.eq("") | name_series.str.casefold().eq(
                code.casefold()
            )
            df.loc[mask & replace_name, "parameter_name"] = label

    if "analysis_method" in df.columns:
        method_series = df["analysis_method"].fillna("").astype(str).str.strip()
        known_codes = {code.casefold() for code in ELEMENT_PARAMETER_NAMES}
        mask = code_folded.isin(known_codes) & method_series.eq("")
        df.loc[mask, "analysis_method"] = DEFAULT_ELEMENT_ANALYSIS_METHOD

    return df


def _rewrite_local_image_urls(df: pd.DataFrame) -> pd.DataFrame:
    """
    Rewrite localhost image URLs when ECHOREPO_PUBLIC_BASE_URL is configured.

    Example:
      ECHOREPO_PUBLIC_BASE_URL=https://echorepo.quanta-labs.com

    Non-local URLs are left unchanged.
    """
    if df is None or df.empty or "image_url" not in df.columns:
        return df

    public_base = os.getenv("ECHOREPO_PUBLIC_BASE_URL", "").strip().rstrip("/")
    if not public_base:
        return df

    def rewrite(value: object) -> object:
        if value is None or pd.isna(value):
            return value

        text = str(value).strip()
        if not text:
            return text

        try:
            parsed = urlsplit(text)
        except ValueError:
            return text

        if parsed.hostname not in {"localhost", "127.0.0.1", "::1"}:
            return text

        suffix = parsed.path or "/"
        if parsed.query:
            suffix += f"?{parsed.query}"
        if parsed.fragment:
            suffix += f"#{parsed.fragment}"
        return f"{public_base}{suffix}"

    df["image_url"] = df["image_url"].map(rewrite)
    return df


def get_samples_df(
    sample_ids: Sequence[str] | None = None,
) -> pd.DataFrame:
    ids = _normalise_sample_ids(sample_ids)

    if ids == []:
        return pd.DataFrame(columns=SAMPLE_COLUMNS).rename(
            columns=SAMPLE_PUBLIC_COLUMN_MAP
        )

    where_sql = ""
    params: tuple = ()

    if ids is not None:
        where_sql = "WHERE sample_id = ANY(%s)"
        params = (ids,)

    sql = f"""
        SELECT {", ".join(SAMPLE_COLUMNS)}
        FROM samples
        {where_sql}
        ORDER BY timestamp_utc DESC, sample_id
    """

    df = _fetch_dataframe(
        sql,
        params,
        SAMPLE_COLUMNS,
    ).rename(columns=SAMPLE_PUBLIC_COLUMN_MAP)

    df = _standardise_timestamp_columns(
        df,
        timestamp_cols=["sampling_datetime_utc"],
    )
    df = _standardise_nullable_integer_columns(
        df,
        int_cols=[
            "earthworms_count",
            "contamination_debris",
            "contamination_plastic",
            "pollutants_count",
        ],
    )
    return df


def get_images_df(
    sample_ids: Sequence[str] | None = None,
) -> pd.DataFrame:
    ids = _normalise_sample_ids(sample_ids)

    if ids == []:
        return pd.DataFrame(columns=IMAGE_COLUMNS).rename(
            columns=IMAGE_PUBLIC_COLUMN_MAP
        )

    where_sql = ""
    params: tuple = ()

    if ids is not None:
        where_sql = "WHERE sample_id = ANY(%s)"
        params = (ids,)

    sql = f"""
        SELECT {", ".join(IMAGE_COLUMNS)}
        FROM sample_images
        {where_sql}
        ORDER BY sample_id, image_id
    """

    df = _fetch_dataframe(
        sql,
        params,
        IMAGE_COLUMNS,
    ).rename(columns=IMAGE_PUBLIC_COLUMN_MAP)

    df = _standardise_timestamp_columns(
        df,
        timestamp_cols=["image_datetime_utc"],
    )
    return _rewrite_local_image_urls(df)


def get_parameters_df(
    sample_ids: Sequence[str] | None = None,
    *,
    minimum_value: float = 0.01,
    exclude_oxides: bool = True,
) -> pd.DataFrame:
    ids = _normalise_sample_ids(sample_ids)

    if ids == []:
        return pd.DataFrame(columns=PARAMETER_COLUMNS).rename(
            columns=PARAMETER_PUBLIC_COLUMN_MAP
        )

    where_sql = ""
    params: tuple = ()

    if ids is not None:
        where_sql = "WHERE sample_id = ANY(%s)"
        params = (ids,)

    sql = f"""
        SELECT {", ".join(PARAMETER_COLUMNS)}
        FROM sample_parameters
        {where_sql}
        ORDER BY sample_id, parameter_code
    """

    df = _fetch_dataframe(
        sql,
        params,
        PARAMETER_COLUMNS,
    )

    if exclude_oxides:
        df = drop_oxide_rows(df)

    df = drop_parameter_values_below(
        df,
        threshold=minimum_value,
    )
    df = _enrich_parameter_metadata(df)
    df = df.rename(columns=PARAMETER_PUBLIC_COLUMN_MAP)
    return _standardise_timestamp_columns(
        df,
        timestamp_cols=["analysis_datetime_utc"],
    )


def get_biodiversity_df(
    sample_ids: Sequence[str] | None = None,
) -> pd.DataFrame:
    ids = _normalise_sample_ids(sample_ids)

    if ids == []:
        return pd.DataFrame(
            columns=BIODIVERSITY_COLUMNS
        ).rename(columns=BIODIVERSITY_PUBLIC_COLUMN_MAP)

    where_sql = ""
    params: tuple = ()

    if ids is not None:
        where_sql = "WHERE sta.sample_id = ANY(%s)"
        params = (ids,)

    sql = f"""
        SELECT
            sta.sample_id,
            s.country_code,
            sta.marker,
            sta.level AS taxonomic_level,
            sta.taxon,
            sta.read_count,
            sta.relative_abundance_pct,
            sta.uploaded_at,
            sta.source_file,
            COALESCE(
                NULLIF(s.licence, ''),
                'CC-BY-4.0'
            ) AS licence
        FROM sample_taxon_abundance AS sta
        LEFT JOIN samples AS s
          ON s.sample_id = sta.sample_id
        {where_sql}
        ORDER BY
            sta.sample_id,
            sta.marker,
            sta.level,
            sta.read_count DESC,
            sta.taxon
    """

    df = _fetch_dataframe(
        sql,
        params,
        BIODIVERSITY_COLUMNS,
    ).rename(columns=BIODIVERSITY_PUBLIC_COLUMN_MAP)

    df = _standardise_nullable_integer_columns(
        df,
        int_cols=["read_count"],
    )
    return _standardise_timestamp_columns(
        df,
        timestamp_cols=["ingested_datetime_utc"],
    )


def get_export_df(
    filename: str,
    sample_ids: Sequence[str] | None = None,
) -> pd.DataFrame:
    if filename == "samples.csv":
        return get_samples_df(sample_ids)

    if filename == "sample_images.csv":
        return get_images_df(sample_ids)

    if filename == "sample_parameters.csv":
        return get_parameters_df(sample_ids)

    if filename == "sample_biodiversity.csv":
        return get_biodiversity_df(sample_ids)

    raise ValueError(
        f"Unsupported canonical export: {filename}"
    )


def dataframe_to_csv_body(
    df: pd.DataFrame,
) -> str:
    buffer = io.StringIO()
    df.to_csv(buffer, index=False)
    return buffer.getvalue()


def build_live_csv(
    *,
    filename: str,
    base_url: str,
    snapshot_date: str | None,
) -> tuple[pd.DataFrame, str]:
    spec = EXPORT_SPECS[filename]
    df = get_export_df(filename)

    header = [
        "# ECHOrepo Canonical Dataset",
        f"# File: {filename}",
        f"# Generated at: {_utc_now_iso()}",
        (
            "# Downloaded from: "
            f"{base_url}/download/canonical/{filename}"
        ),
        f"# Description: {spec.description}",
    ]

    header.extend(
        f"# Note: {note}"
        for note in spec.notes
    )

    header.append(
        "# Note: This is a live export. For a fixed, "
        "citable snapshot, use the full canonical ZIP export."
    )

    if snapshot_date:
        header.extend(
            [
                (
                    "# DOI for latest citable snapshot: "
                    f"{ZENODO_DOI}"
                ),
                (
                    "# Latest citable snapshot (all.zip): "
                    f"{base_url}/download/canonical/"
                    f"{snapshot_date}/all.zip"
                ),
                (
                    f"# Latest citable snapshot ({filename}): "
                    f"{base_url}/download/canonical/"
                    f"{snapshot_date}/{filename}"
                ),
            ]
        )
    else:
        header.append(
            "# Latest citable snapshot: "
            "(not available yet on this instance)"
        )

    header.append("")

    return (
        df,
        "\n".join(header)
        + dataframe_to_csv_body(df),
    )


def build_snapshot_csv(
    *,
    filename: str,
    df: pd.DataFrame,
    base_url: str,
    version_date: str,
) -> str:
    spec = EXPORT_SPECS[filename]

    header = [
        "# ECHOrepo Canonical Dataset",
        f"# File: {filename}",
        f"# Version date: {version_date}",
        (
            "# Version URL: "
            f"{base_url}/download/canonical/"
            f"{version_date}/{filename}"
        ),
        (
            "# Latest canonical: "
            f"{base_url}/download/canonical/{filename}"
        ),
        f"# Description: {spec.description}",
    ]

    header.extend(
        f"# Note: {note}"
        for note in spec.notes
    )

    header.append("")

    return (
        "\n".join(header)
        + dataframe_to_csv_body(df)
    )


def build_snapshot_bundle(
    *,
    base_url: str,
    version_date: str,
) -> CanonicalBundle:
    csv_contents: dict[str, str] = {}

    for filename in EXPORT_SPECS:
        df = get_export_df(filename)

        csv_contents[filename] = build_snapshot_csv(
            filename=filename,
            df=df,
            base_url=base_url,
            version_date=version_date,
        )

    zip_buffer = io.BytesIO()

    with zipfile.ZipFile(
        zip_buffer,
        mode="w",
        compression=zipfile.ZIP_DEFLATED,
    ) as archive:
        for filename, csv_text in csv_contents.items():
            archive.writestr(
                filename,
                csv_text,
            )

    return CanonicalBundle(
        csv_contents=csv_contents,
        zip_bytes=zip_buffer.getvalue(),
    )

FILTERED_EXPORT_FILENAMES = {
    "samples.csv": "samples_filtered.csv",
    "sample_images.csv": "sample_images_filtered.csv",
    "sample_parameters.csv": "sample_parameters_filtered.csv",
    "sample_biodiversity.csv": "sample_biodiversity_filtered.csv",
}

FILTERED_SOURCE_TABLES = {
    "samples.csv": "samples",
    "sample_images.csv": "sample_images",
    "sample_parameters.csv": "sample_parameters",
    "sample_biodiversity.csv": "sample_taxon_abundance",
}


def build_filtered_csv(
    *,
    filename: str,
    df: pd.DataFrame,
    snapshot_url: str,
    query_string: str,
    generated_at: str | None = None,
) -> str:
    """Build one non-citable filtered CSV export."""
    spec = EXPORT_SPECS[filename]
    filtered_filename = FILTERED_EXPORT_FILENAMES[filename]
    source_table = FILTERED_SOURCE_TABLES[filename]

    header = [
        "# ECHOrepo Filtered Dataset",
        f"# File: {filtered_filename}",
        f"# Source table: {source_table} (filtered subset)",
        f"# Download full dataset snapshot: {snapshot_url}",
        f"# Generated at: {generated_at or _utc_now_iso()}",
        f"# Query: {query_string}",
        f"# Description: {spec.description}",
    ]

    header.extend(
        f"# Note: {note}"
        for note in spec.notes
    )

    header.extend(
        [
            (
                "# Note: This is a filtered export for user inspection. "
                "It is not a stable or citable dataset."
            ),
            f"# DOI for latest citable snapshot: {ZENODO_DOI}",
            "",
        ]
    )

    return (
        "\n".join(header)
        + dataframe_to_csv_body(df)
    )


def build_filtered_bundle(
    *,
    sample_ids: Sequence[str],
    snapshot_url: str,
    query_string: str,
) -> CanonicalBundle:
    """Build the four-file ZIP returned by the filtered search export."""
    csv_contents: dict[str, str] = {}
    generated_at = _utc_now_iso()

    for filename in EXPORT_SPECS:
        df = get_export_df(
            filename,
            sample_ids=sample_ids,
        )
        filtered_filename = FILTERED_EXPORT_FILENAMES[filename]
        csv_contents[filtered_filename] = build_filtered_csv(
            filename=filename,
            df=df,
            snapshot_url=snapshot_url,
            query_string=query_string,
            generated_at=generated_at,
        )

    zip_buffer = io.BytesIO()

    with zipfile.ZipFile(
        zip_buffer,
        mode="w",
        compression=zipfile.ZIP_DEFLATED,
    ) as archive:
        for filename, csv_text in csv_contents.items():
            archive.writestr(filename, csv_text)

    return CanonicalBundle(
        csv_contents=csv_contents,
        zip_bytes=zip_buffer.getvalue(),
    )


def build_machine_bundle(
    *,
    sample_ids: Sequence[str] | None = None,
) -> CanonicalBundle:
    """
    Build a machine-readable canonical ZIP.

    Unlike the human-facing snapshot download, these CSV files contain
    no leading comment lines. Their first line is the CSV column header.

    Used by /api/v1/canonical/all.zip and the Zenodo publication script.
    """
    csv_contents: dict[str, str] = {}

    for filename in EXPORT_SPECS:
        df = get_export_df(
            filename,
            sample_ids=sample_ids,
        )

        csv_contents[filename] = (
            dataframe_to_csv_body(df)
        )

    zip_buffer = io.BytesIO()

    with zipfile.ZipFile(
        zip_buffer,
        mode="w",
        compression=zipfile.ZIP_DEFLATED,
    ) as archive:
        for filename, csv_text in csv_contents.items():
            archive.writestr(
                filename,
                csv_text,
            )

    return CanonicalBundle(
        csv_contents=csv_contents,
        zip_bytes=zip_buffer.getvalue(),
    )