#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import fnmatch
import hashlib
import io
import json
import os
import shutil
import sys
import tempfile
import zipfile
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import quote

import requests


REPO_ROOT = Path(__file__).resolve().parents[1]

DEFAULT_API_PATH = "/canonical/zenodo_bundle.zip"
DEFAULT_METADATA_CONFIG = "metadata/soilwise/echorepo_columns.json"
DEFAULT_SOILVOC_API = "https://api.soilwise-he.containers.wur.nl/vocab/api/v1"

CANONICAL_RESOURCE_SCHEMAS: dict[str, list[str]] = {
    "samples.csv": [
        "sample_id",
        "timestamp_utc",
        "lat",
        "lon",
        "country_code",
        "location_accuracy_m",
        "ph",
        "soil_organic_matter_estimate_pct",
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
    ],
    "sample_images.csv": [
        "sample_id",
        "country_code",
        "image_id",
        "image_url",
        "image_description_orig",
        "image_description_en",
        "collected_by",
        "timestamp_utc",
        "licence",
    ],
    "sample_parameters.csv": [
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
    ],
    "sample_biodiversity.csv": [
        "sample_id",
        "country_code",
        "marker",
        "taxonomic_level",
        "taxon",
        "read_count",
        "relative_abundance_pct",
        "analysis_date",
        "source_file",
        "licence",
    ],
}

DEFAULT_CSV_PATTERNS = tuple(CANONICAL_RESOURCE_SCHEMAS)

LEGACY_BIODIVERSITY_HEADERS = [
    "sample_id",
    "marker",
    "otu_id",
    "count",
    "taxa",
    "uploaded_at",
    "uploaded_by",
    "source_file",
]


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _resolve_input_path(path: str | Path) -> Path:
    candidate = Path(path).expanduser()
    if candidate.is_absolute() or candidate.exists():
        return candidate

    repo_candidate = REPO_ROOT / candidate
    return repo_candidate if repo_candidate.exists() else candidate


def load_simple_env_file(path: str) -> dict[str, str]:
    env: dict[str, str] = {}
    p = _resolve_input_path(path)
    if not p.exists():
        return env

    for raw_line in p.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        env[key.strip()] = value.strip().strip("'").strip('"')
    return env


def env_or_config(
    name: str,
    cli_value: str | None,
    file_env: dict[str, str],
    default: str | None = None,
) -> str | None:
    if cli_value not in (None, ""):
        return cli_value
    environment_value = os.getenv(name)
    if environment_value not in (None, ""):
        return environment_value
    if name in file_env:
        return file_env[name]
    return default


def bool_from_envish(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "on", "y", "t"}


def build_echorepo_headers(
    api_key: str | None,
    bearer_token: str | None,
) -> dict[str, str]:
    if api_key:
        return {"X-API-Key": api_key}
    if bearer_token:
        return {"Authorization": f"Bearer {bearer_token}"}
    raise ValueError("Need either ECHOREPO API key or bearer token")


def request_ok(
    response: requests.Response,
    expected: set[int],
    context: str,
) -> None:
    if response.status_code not in expected:
        raise RuntimeError(
            f"{context} failed with HTTP {response.status_code}: "
            f"{response.text[:3000]}"
        )


def build_filter_params(args: argparse.Namespace) -> dict[str, str]:
    params: dict[str, str] = {}
    if args.from_date:
        params["from"] = args.from_date
    if args.to_date:
        params["to"] = args.to_date
    if args.country:
        params["country"] = args.country
    if args.country_code:
        params["country_code"] = args.country_code
    if args.bbox:
        params["bbox"] = args.bbox
    if args.within:
        params["within"] = args.within

    for item in args.extra_param or []:
        if "=" not in item:
            raise ValueError(
                f"Invalid --extra-param value {item!r}; expected key=value"
            )
        key, value = item.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key:
            raise ValueError(
                f"Invalid --extra-param value {item!r}; key is empty"
            )
        params[key] = value

    return params


def normalize_endpoint_path(api_path: str) -> str:
    api_path = api_path.strip()
    if not api_path:
        raise ValueError("API path must not be empty")
    if not api_path.startswith("/"):
        api_path = "/" + api_path
    return api_path


def infer_download_name_from_path(
    api_path: str,
    fallback: str = "downloaded_file",
) -> str:
    name = api_path.rstrip("/").split("/")[-1]
    return name or fallback


def normalize_grant_id(raw: str) -> str:
    value = raw.strip()
    if not value:
        raise ValueError("Grant ID must not be empty")
    if "::" in value:
        return value
    return f"10.13039/501100000780::{value}"


def parse_subject(raw: str) -> dict[str, str]:
    parts = [part.strip() for part in raw.split("|")]
    if len(parts) < 2 or not parts[0] or not parts[1]:
        raise ValueError(
            f"Invalid subject specification {raw!r}; expected "
            "'term|identifier|scheme'"
        )

    subject = {"term": parts[0], "identifier": parts[1]}
    if len(parts) >= 3 and parts[2]:
        subject["scheme"] = parts[2]
    return subject


def parse_creator(raw: str) -> dict[str, str]:
    parts = [part.strip() for part in raw.split("|")]
    if not parts or not parts[0]:
        raise ValueError(f"Invalid creator specification: {raw!r}")

    creator: dict[str, str] = {"name": parts[0]}
    if len(parts) >= 2 and parts[1]:
        creator["affiliation"] = parts[1]
    if len(parts) >= 3 and parts[2]:
        creator["orcid"] = parts[2]
    return creator


def parse_keywords(values: list[str] | None) -> list[str]:
    output: list[str] = []
    for raw in values or []:
        for part in raw.split(","):
            keyword = part.strip()
            if keyword and keyword not in output:
                output.append(keyword)
    return output


def build_zenodo_metadata(args: argparse.Namespace) -> dict[str, Any]:
    metadata: dict[str, Any] = {
        "title": args.title,
        "upload_type": "dataset",
        "description": args.description,
        "creators": [parse_creator(value) for value in args.creator],
        "access_right": args.access_right,
        "license": args.license,
        "prereserve_doi": True,
    }

    keywords = parse_keywords(args.keyword)
    if keywords:
        metadata["keywords"] = keywords
    if args.version:
        metadata["version"] = args.version
    if args.communities:
        metadata["communities"] = [
            {"identifier": identifier} for identifier in args.communities
        ]
    if args.grant:
        metadata["grants"] = [
            {"id": normalize_grant_id(grant)} for grant in args.grant
        ]
    if args.subject:
        metadata["subjects"] = [parse_subject(value) for value in args.subject]
    if args.copyright:
        metadata["notes"] = f"Copyright: {args.copyright}"

    return metadata


def download_api_file(
    api_base: str,
    api_path: str,
    headers: dict[str, str],
    filters: dict[str, str],
    output_path: Path,
    timeout: int = 300,
) -> dict[str, Any]:
    url = f"{api_base.rstrip('/')}{normalize_endpoint_path(api_path)}"
    request_headers = {
        **headers,
        "Accept": "application/zip, text/csv;q=0.9, */*;q=0.1",
        "User-Agent": "ECHOREPO-Zenodo-Publisher/2",
    }
    partial_path = output_path.with_name(output_path.name + ".part")
    partial_path.unlink(missing_ok=True)

    try:
        with requests.get(
            url,
            headers=request_headers,
            params=filters,
            timeout=timeout,
            stream=True,
            allow_redirects=True,
        ) as response:
            request_ok(response, {200}, "API file download")

            content_type = response.headers.get("Content-Type", "")
            if content_type.lower().startswith("text/html"):
                raise RuntimeError(
                    "API returned HTML instead of a canonical ZIP/CSV resource"
                )

            size_bytes = 0
            with partial_path.open("wb") as file_handle:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    if not chunk:
                        continue
                    file_handle.write(chunk)
                    size_bytes += len(chunk)

            download_url = response.url
            etag = response.headers.get("ETag", "")
            last_modified = response.headers.get("Last-Modified", "")

        if size_bytes == 0:
            raise RuntimeError("API returned an empty file")

        partial_path.replace(output_path)

        return {
            "download_url": download_url,
            "content_type": content_type,
            "size_bytes": size_bytes,
            "etag": etag,
            "last_modified": last_modified,
        }
    except Exception:
        partial_path.unlink(missing_ok=True)
        raise


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file_handle:
        for chunk in iter(lambda: file_handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _safe_zip_member_basename(info: zipfile.ZipInfo) -> str:
    member = Path(info.filename)
    if info.is_dir():
        return ""
    if member.is_absolute() or ".." in member.parts:
        raise ValueError(f"Unsafe path inside ZIP: {info.filename!r}")
    return member.name


def _matches_any_pattern(filename: str, patterns: Iterable[str]) -> bool:
    return any(fnmatch.fnmatchcase(filename, pattern) for pattern in patterns)


def extract_csv_resources(
    source_path: Path,
    output_dir: Path,
    patterns: list[str],
) -> list[Path]:
    output_dir.mkdir(parents=True, exist_ok=True)

    if zipfile.is_zipfile(source_path):
        selected: dict[str, zipfile.ZipInfo] = {}
        selected_casefold: dict[str, str] = {}

        with zipfile.ZipFile(source_path) as archive:
            bad_member = archive.testzip()
            if bad_member is not None:
                raise RuntimeError(
                    f"Downloaded ZIP failed its CRC check at {bad_member!r}"
                )

            for info in archive.infolist():
                basename = _safe_zip_member_basename(info)
                if not basename or not basename.lower().endswith(".csv"):
                    continue
                if not _matches_any_pattern(basename, patterns):
                    continue
                if info.flag_bits & 0x1:
                    raise ValueError(
                        f"Encrypted ZIP member is not supported: {info.filename!r}"
                    )

                folded = basename.casefold()
                if folded in selected_casefold:
                    raise ValueError(
                        "ZIP contains duplicate selected CSV basenames: "
                        f"{selected_casefold[folded]!r} and {basename!r}"
                    )

                selected[basename] = info
                selected_casefold[folded] = basename

            if not selected:
                raise RuntimeError(
                    "No CSV files in the downloaded ZIP matched: "
                    + ", ".join(patterns)
                )

            extracted: list[Path] = []
            for basename in sorted(selected):
                destination = output_dir / basename
                with archive.open(selected[basename]) as source, destination.open(
                    "wb"
                ) as target:
                    shutil.copyfileobj(source, target)
                if destination.stat().st_size == 0:
                    raise RuntimeError(f"Extracted CSV is empty: {basename}")
                extracted.append(destination)
            return extracted

    if source_path.suffix.lower() == ".csv":
        destination = output_dir / source_path.name
        shutil.copy2(source_path, destination)
        if destination.stat().st_size == 0:
            raise RuntimeError(f"CSV is empty: {source_path}")
        return [destination]

    raise ValueError(
        f"Downloaded source is neither a ZIP archive nor a CSV file: {source_path}"
    )


def decode_csv_bytes(raw: bytes) -> tuple[str, str]:
    for encoding in ("utf-8-sig", "utf-8", "cp1252"):
        try:
            return raw.decode(encoding), encoding
        except UnicodeDecodeError:
            continue
    return raw.decode("utf-8", errors="replace"), "utf-8-replacement"


def detect_csv_dialect(text: str) -> csv.Dialect:
    sample = text[:65536]
    try:
        return csv.Sniffer().sniff(sample, delimiters=",;\t|")
    except csv.Error:
        return csv.get_dialect("excel")


def _is_integer(value: str) -> bool:
    try:
        int(value)
        return True
    except ValueError:
        return False


def _is_number(value: str) -> bool:
    try:
        float(value.replace(",", "."))
        return True
    except ValueError:
        return False


def _is_iso_date(value: str) -> bool:
    try:
        date.fromisoformat(value)
        return True
    except ValueError:
        return False


def _is_iso_datetime(value: str) -> bool:
    candidate = value.strip().replace("Z", "+00:00")
    try:
        datetime.fromisoformat(candidate)
        return "T" in value or ":" in value
    except ValueError:
        return False


def infer_csvw_datatype(column_name: str, values: list[str]) -> str:
    name = column_name.lower()
    nonempty = [value.strip() for value in values if value.strip()]

    if name.endswith("_uri") or name.endswith("_url"):
        if not nonempty or all(
            value.startswith(("http://", "https://")) for value in nonempty
        ):
            return "anyURI"

    if not nonempty:
        return "string"

    lowered = {value.lower() for value in nonempty}
    if lowered <= {"true", "false", "yes", "no"}:
        return "boolean"
    if all(_is_integer(value) for value in nonempty):
        return "integer"
    if all(_is_number(value) for value in nonempty):
        return "number"
    if all(_is_iso_datetime(value) for value in nonempty):
        return "datetime"
    if all(_is_iso_date(value) for value in nonempty):
        return "date"
    return "string"


def _open_csv_reader(
    text: str,
    filename: str,
) -> tuple[csv._reader, str]:
    """Return a reader and delimiter for one CSV resource.

    ECHOREPO canonical exports have a fixed RFC-4180-style dialect: comma
    delimiter, double-quote quote character, and doubled quotes inside quoted
    fields. Do not run ``csv.Sniffer`` on these files: free-text image
    descriptions may contain many apostrophes, causing Sniffer to incorrectly
    choose a single quote as the quote character and then split quoted commas
    into extra cells.

    Non-canonical CSVs retain dialect detection for backwards compatibility.
    """
    stream = io.StringIO(text, newline="")

    if filename in CANONICAL_RESOURCE_SCHEMAS:
        return (
            csv.reader(
                stream,
                delimiter=",",
                quotechar='"',
                doublequote=True,
                escapechar=None,
                skipinitialspace=False,
                strict=True,
            ),
            ",",
        )

    dialect = detect_csv_dialect(text)
    return csv.reader(stream, dialect=dialect, strict=True), dialect.delimiter


def analyse_csv(path: Path, sample_rows: int = 200) -> dict[str, Any]:
    raw = path.read_bytes()
    if not raw:
        raise ValueError(f"CSV is empty: {path.name}")

    text, encoding = decode_csv_bytes(raw)
    if "\x00" in text:
        raise ValueError(f"CSV contains NUL bytes: {path.name}")

    reader, delimiter = _open_csv_reader(text, path.name)

    try:
        headers = next(reader)
    except StopIteration as exc:
        raise ValueError(f"CSV has no header row: {path.name}") from exc
    except csv.Error as exc:
        raise ValueError(
            f"Cannot parse the header of {path.name}: {exc}"
        ) from exc

    headers = [header.lstrip("\ufeff").strip() for header in headers]
    if not headers or any(not header for header in headers):
        raise ValueError(f"CSV contains an empty column header: {path.name}")
    if len(headers) != len(set(headers)):
        raise ValueError(f"CSV contains duplicate column headers: {path.name}")

    values_by_column: dict[str, list[str]] = {header: [] for header in headers}
    row_count = 0

    try:
        for row in reader:
            if not row or all(not cell.strip() for cell in row):
                continue

            row_count += 1
            if len(row) != len(headers):
                preview = " | ".join(
                    cell.replace("\r", "\\r").replace("\n", "\\n")[:120]
                    for cell in row[: min(len(row), 6)]
                )
                raise ValueError(
                    f"CSV record {row_count + 1} in {path.name} has "
                    f"{len(row)} cells; expected {len(headers)}. "
                    f"The record ends near physical line {reader.line_num}. "
                    f"Parsed preview: {preview!r}"
                )

            if row_count <= sample_rows:
                for header, value in zip(headers, row):
                    values_by_column[header].append(value)

    except csv.Error as exc:
        raise ValueError(
            f"Malformed CSV in {path.name} near physical line "
            f"{reader.line_num}: {exc}"
        ) from exc

    inferred = {
        header: infer_csvw_datatype(header, values_by_column[header])
        for header in headers
    }

    return {
        "path": path,
        "filename": path.name,
        "headers": headers,
        "row_count": row_count,
        "encoding": "utf-8" if encoding.startswith("utf-8") else encoding,
        "delimiter": delimiter,
        "inferred_datatypes": inferred,
        "size_bytes": path.stat().st_size,
        "sha256": sha256_file(path),
    }


def canonical_schema_summary(
    analyses: list[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    return {
        analysis["filename"]: {
            "headers": analysis["headers"],
            "row_count": analysis["row_count"],
            "size_bytes": analysis["size_bytes"],
            "sha256": analysis["sha256"],
        }
        for analysis in sorted(analyses, key=lambda item: item["filename"])
    }


def validate_echorepo_resource_schemas(
    analyses: list[dict[str, Any]],
) -> None:
    by_filename = {
        analysis["filename"]: analysis
        for analysis in analyses
    }
    expected_files = set(CANONICAL_RESOURCE_SCHEMAS)
    actual_files = set(by_filename)

    missing = sorted(expected_files - actual_files)
    unexpected = sorted(actual_files - expected_files)
    if missing or unexpected:
        parts = []
        if missing:
            parts.append(f"missing canonical resources: {missing}")
        if unexpected:
            parts.append(f"unexpected CSV resources: {unexpected}")
        raise RuntimeError("Invalid canonical bundle: " + "; ".join(parts))

    for filename, expected_headers in CANONICAL_RESOURCE_SCHEMAS.items():
        analysis = by_filename[filename]
        actual_headers = analysis["headers"]

        if (
            filename == "sample_biodiversity.csv"
            and actual_headers == LEGACY_BIODIVERSITY_HEADERS
        ):
            raise RuntimeError(
                "The ECHOREPO API returned the obsolete raw-OTU biodiversity "
                "schema. Refusing to prepare or publish this bundle."
            )

        if actual_headers != expected_headers:
            raise RuntimeError(
                f"Unexpected header in {filename}. "
                f"Expected {expected_headers}, got {actual_headers}"
            )

        if analysis["encoding"] != "utf-8":
            raise RuntimeError(
                f"Canonical resource {filename} is not UTF-8: "
                f"detected {analysis['encoding']}"
            )

        if analysis["delimiter"] != ",":
            raise RuntimeError(
                f"Canonical resource {filename} uses delimiter "
                f"{analysis['delimiter']!r}; expected ','"
            )


def validate_metadata_config_against_analyses(
    config: dict[str, Any],
    analyses: list[dict[str, Any]],
) -> list[str]:
    warnings: list[str] = []
    tables = config.get("tables")
    if not isinstance(tables, dict):
        return ["Metadata configuration has no 'tables' object."]

    actual_by_filename = {
        analysis["filename"]: set(analysis["headers"])
        for analysis in analyses
    }

    for filename, actual_headers in actual_by_filename.items():
        table_config = tables.get(filename)
        if not isinstance(table_config, dict):
            warnings.append(
                f"{filename} has no table-level metadata configuration."
            )
            continue

        configured_columns = table_config.get("columns")
        if isinstance(configured_columns, dict):
            stale_columns = sorted(set(configured_columns) - actual_headers)
            if stale_columns:
                warnings.append(
                    f"{filename} metadata contains obsolete columns: "
                    f"{stale_columns}."
                )

    unknown_tables = sorted(set(tables) - set(actual_by_filename))
    if unknown_tables:
        warnings.append(
            "Metadata configuration contains tables not present in this bundle: "
            f"{unknown_tables}."
        )

    return warnings


def load_metadata_config(path: str | None) -> tuple[dict[str, Any], list[str]]:
    if not path:
        return {}, ["No metadata configuration file was specified."]

    config_path = _resolve_input_path(path)
    if not config_path.exists():
        return {}, [
            f"Metadata configuration file does not exist: {config_path}. "
            "Generic descriptions will be generated."
        ]

    try:
        value = json.loads(config_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(
            f"Cannot read metadata configuration {config_path}: {exc}"
        ) from exc

    if not isinstance(value, dict):
        raise ValueError("Metadata configuration must be a JSON object")
    return value, []


def _merge_dicts(*objects: Any) -> dict[str, Any]:
    merged: dict[str, Any] = {}
    for obj in objects:
        if isinstance(obj, dict):
            merged.update(obj)
    return merged


def humanize_column_name(name: str) -> str:
    return " ".join(part for part in name.replace("-", "_").split("_") if part).capitalize()


def normalize_property_urls(value: Any) -> list[str]:
    if value in (None, "", []):
        return []
    if isinstance(value, str):
        values = [value]
    elif isinstance(value, list):
        values = value
    else:
        raise ValueError(f"propertyUrl must be a string or list, got {type(value).__name__}")

    output: list[str] = []
    for item in values:
        uri = str(item).strip()
        if not uri:
            continue
        if not uri.startswith(("http://", "https://")):
            raise ValueError(f"Invalid propertyUrl URI: {uri!r}")
        if uri not in output:
            output.append(uri)
    return output


def table_config_for(config: dict[str, Any], filename: str) -> dict[str, Any]:
    tables = config.get("tables") if isinstance(config.get("tables"), dict) else {}
    return tables.get(filename, {}) if isinstance(tables.get(filename), dict) else {}


def column_config_for(
    config: dict[str, Any],
    filename: str,
    column_name: str,
) -> dict[str, Any]:
    table_config = table_config_for(config, filename)
    table_columns = (
        table_config.get("columns")
        if isinstance(table_config.get("columns"), dict)
        else {}
    )
    global_columns = (
        config.get("global_columns")
        if isinstance(config.get("global_columns"), dict)
        else {}
    )
    return _merge_dicts(
        global_columns.get(column_name),
        table_columns.get(column_name),
    )


def build_column_description(
    analysis: dict[str, Any],
    column_name: str,
    config: dict[str, Any],
    warnings: list[str],
) -> dict[str, Any]:
    filename = analysis["filename"]
    metadata = column_config_for(config, filename, column_name)
    status = str(metadata.get("status", "unresolved")).strip().lower()

    title = str(metadata.get("title") or metadata.get("element") or "").strip()
    description = str(metadata.get("description") or "").strip()
    datatype = str(
        metadata.get("datatype")
        or analysis["inferred_datatypes"][column_name]
        or "string"
    ).strip()

    if not title:
        title = humanize_column_name(column_name)
    if not description:
        description = (
            f"Value of the '{column_name}' column in the ECHOREPO "
            f"resource {filename}."
        )
        warnings.append(
            f"{filename}:{column_name} has no reviewed description; "
            "a generic description was generated."
        )
    if status in {"unresolved", "needs-review", "provisional"}:
        warnings.append(f"{filename}:{column_name} metadata status is {status}.")

    column: dict[str, Any] = {
        "name": column_name,
        "titles": [title],
        "datatype": datatype,
        "description": description,
    }

    property_urls = normalize_property_urls(
        metadata.get("propertyUrl", metadata.get("element_uri"))
    )
    if property_urls:
        # SoilWise's Tabular Data Annotator exports propertyUrl as a list.
        column["propertyUrl"] = property_urls

    for key in ("unit", "method", "separator", "format", "valueUrl", "null"):
        value = metadata.get(key)
        if value not in (None, "", []):
            column[key] = value

    if bool(metadata.get("required", False)):
        column["required"] = True

    return column


def build_csvw_document(
    analyses: list[dict[str, Any]],
    config: dict[str, Any],
    args: argparse.Namespace,
    source_url: str,
    reserved_doi: str | None,
) -> tuple[dict[str, Any], list[str]]:
    warnings: list[str] = []
    filenames = {analysis["filename"] for analysis in analyses}
    tables: list[dict[str, Any]] = []

    for analysis in sorted(analyses, key=lambda item: item["filename"]):
        filename = analysis["filename"]
        table_config = table_config_for(config, filename)
        table_title = str(
            table_config.get("title") or f"ECHOREPO {filename}"
        ).strip()
        table_description = str(
            table_config.get("description")
            or f"Tabular ECHOREPO data resource stored in {filename}."
        ).strip()

        columns = [
            build_column_description(
                analysis,
                column_name,
                config,
                warnings,
            )
            for column_name in analysis["headers"]
        ]

        table_schema: dict[str, Any] = {"columns": columns}
        primary_key = table_config.get("primaryKey")
        if primary_key:
            keys = [primary_key] if isinstance(primary_key, str) else list(primary_key)
            missing_keys = [key for key in keys if key not in analysis["headers"]]
            if missing_keys:
                raise ValueError(
                    f"Configured primary key for {filename} references missing "
                    f"columns: {missing_keys}"
                )
            table_schema["primaryKey"] = primary_key

        if (
            filename != "samples.csv"
            and "samples.csv" in filenames
            and "sample_id" in analysis["headers"]
        ):
            table_schema["foreignKeys"] = [
                {
                    "columnReference": "sample_id",
                    "reference": {
                        "resource": "samples.csv",
                        "columnReference": "sample_id",
                    },
                }
            ]

        table: dict[str, Any] = {
            "url": filename,
            "dc:title": table_title,
            "dc:description": table_description,
            "schema:numberOfItems": analysis["row_count"],
            "schema:contentSize": analysis["size_bytes"],
            "schema:sha256": analysis["sha256"],
            "dialect": {
                "encoding": analysis["encoding"],
                "delimiter": analysis["delimiter"],
                "header": True,
            },
            "tableSchema": table_schema,
        }
        tables.append(table)

    creators = [parse_creator(value)["name"] for value in args.creator]
    document: dict[str, Any] = {
        "@context": [
            "http://www.w3.org/ns/csvw",
            {
                "@language": "en",
                "dc": "http://purl.org/dc/terms/",
                "schema": "https://schema.org/",
                "sosa": "http://www.w3.org/ns/sosa/",
                "qudt": "http://qudt.org/schema/qudt/",
            },
        ],
        "dc:title": args.title,
        "dc:description": args.description,
        "dc:creator": creators,
        "dc:license": license_url(args.license),
        "dc:source": source_url,
        "dc:modified": utc_now_iso(),
        "tables": tables,
    }
    if reserved_doi:
        document["dc:identifier"] = f"https://doi.org/{reserved_doi}"
    if args.version:
        document["schema:version"] = args.version
    if args.copyright:
        document["dc:rights"] = args.copyright

    return document, deduplicate_strings(warnings)


def license_url(license_id: str) -> str:
    normalized = license_id.strip().lower()
    known = {
        "cc-by-4.0": "https://creativecommons.org/licenses/by/4.0/",
        "cc-by-sa-4.0": "https://creativecommons.org/licenses/by-sa/4.0/",
        "cc0-1.0": "https://creativecommons.org/publicdomain/zero/1.0/",
    }
    return known.get(normalized, license_id)


def deduplicate_strings(values: Iterable[str]) -> list[str]:
    output: list[str] = []
    for value in values:
        if value not in output:
            output.append(value)
    return output


def validate_csvw_document(
    document: dict[str, Any],
    analyses: list[dict[str, Any]],
) -> None:
    tables = document.get("tables")
    if not isinstance(tables, list) or not tables:
        raise ValueError("file.json must contain a non-empty tables array")

    expected_headers = {
        analysis["filename"]: analysis["headers"] for analysis in analyses
    }
    described_files: set[str] = set()

    for table in tables:
        filename = table.get("url")
        if filename not in expected_headers:
            raise ValueError(f"file.json describes an unexpected table: {filename!r}")
        described_files.add(filename)

        columns = table.get("tableSchema", {}).get("columns")
        if not isinstance(columns, list):
            raise ValueError(f"file.json has no columns array for {filename}")
        described_headers = [column.get("name") for column in columns]
        if described_headers != expected_headers[filename]:
            raise ValueError(
                f"file.json column order for {filename} does not match its CSV header"
            )

    if described_files != set(expected_headers):
        missing = sorted(set(expected_headers) - described_files)
        raise ValueError(f"file.json does not describe CSV files: {missing}")


def collect_soilvoc_uris(document: dict[str, Any]) -> list[str]:
    values: list[str] = []
    for table in document.get("tables", []):
        for column in table.get("tableSchema", {}).get("columns", []):
            urls = column.get("propertyUrl", [])
            if isinstance(urls, str):
                urls = [urls]
            for uri in urls:
                if str(uri).startswith("https://w3id.org/eusoilvoc#"):
                    values.append(str(uri))
    return deduplicate_strings(values)


def validate_soilvoc_uris(
    uris: list[str],
    api_base: str,
    timeout: int = 30,
) -> list[str]:
    warnings: list[str] = []
    for uri in uris:
        fragment = uri.rsplit("#", 1)[-1]
        endpoint = f"{api_base.rstrip('/')}/concepts/{quote(fragment, safe='')}"
        try:
            response = requests.get(endpoint, timeout=timeout)
        except requests.RequestException as exc:
            warnings.append(f"Could not validate SoilVoc URI {uri}: {exc}")
            continue

        if response.status_code != 200:
            warnings.append(
                f"SoilVoc API returned HTTP {response.status_code} for {uri}"
            )
            continue

        try:
            payload = response.json()
        except ValueError:
            warnings.append(f"SoilVoc API returned invalid JSON for {uri}")
            continue

        returned_uri = str(payload.get("uri", ""))
        if returned_uri != uri:
            warnings.append(
                f"SoilVoc API returned {returned_uri!r} while validating {uri!r}"
            )
    return warnings


def build_zenodo_base_url(use_sandbox: bool) -> str:
    return (
        "https://sandbox.zenodo.org/api/deposit/depositions"
        if use_sandbox
        else "https://zenodo.org/api/deposit/depositions"
    )


def zenodo_auth_headers(
    access_token: str,
    json_body: bool = False,
) -> dict[str, str]:
    headers = {"Authorization": f"Bearer {access_token}"}
    if json_body:
        headers["Content-Type"] = "application/json"
    return headers


def create_new_deposition(
    zenodo_url: str,
    access_token: str,
) -> dict[str, Any]:
    response = requests.post(
        zenodo_url,
        headers=zenodo_auth_headers(access_token, json_body=True),
        json={"metadata": {"prereserve_doi": True}},
        timeout=120,
    )
    request_ok(response, {201}, "Zenodo create deposition")
    return response.json()


def create_new_version_draft(
    zenodo_url: str,
    access_token: str,
    existing_deposition_id: str,
) -> dict[str, Any]:
    response = requests.post(
        f"{zenodo_url}/{existing_deposition_id}/actions/newversion",
        headers=zenodo_auth_headers(access_token),
        timeout=120,
    )
    request_ok(response, {201}, "Zenodo new version action")

    latest_draft_url = response.json().get("links", {}).get("latest_draft")
    if not latest_draft_url:
        raise RuntimeError("Zenodo did not return links.latest_draft")

    draft_response = requests.get(
        latest_draft_url,
        headers=zenodo_auth_headers(access_token),
        timeout=120,
    )
    request_ok(draft_response, {200}, "Zenodo fetch latest draft")
    return draft_response.json()


def update_metadata(
    zenodo_url: str,
    access_token: str,
    deposition_id: str,
    metadata: dict[str, Any],
) -> dict[str, Any]:
    response = requests.put(
        f"{zenodo_url}/{deposition_id}",
        headers=zenodo_auth_headers(access_token, json_body=True),
        json={"metadata": metadata},
        timeout=120,
    )
    request_ok(response, {200}, "Zenodo metadata update")
    return response.json()


def list_deposition_files(
    zenodo_url: str,
    access_token: str,
    deposition_id: str,
) -> list[dict[str, Any]]:
    response = requests.get(
        f"{zenodo_url}/{deposition_id}/files",
        headers=zenodo_auth_headers(access_token),
        timeout=120,
    )
    request_ok(response, {200}, "Zenodo list draft files")
    payload = response.json()
    if not isinstance(payload, list):
        raise RuntimeError("Zenodo file-list response is not an array")
    return payload


def clear_deposition_files(
    zenodo_url: str,
    access_token: str,
    deposition_id: str,
) -> list[str]:
    removed: list[str] = []
    for item in list_deposition_files(zenodo_url, access_token, deposition_id):
        file_id = item.get("id")
        filename = item.get("filename") or item.get("name") or str(file_id)
        if not file_id:
            raise RuntimeError(f"Zenodo returned a file without an ID: {item}")

        response = requests.delete(
            f"{zenodo_url}/{deposition_id}/files/{file_id}",
            headers=zenodo_auth_headers(access_token),
            timeout=120,
        )
        request_ok(response, {204}, f"Zenodo delete inherited file {filename}")
        removed.append(str(filename))
    return removed


def upload_file_to_bucket(
    bucket_url: str,
    access_token: str,
    file_path: Path,
) -> dict[str, Any]:
    encoded_name = quote(file_path.name, safe="")
    with file_path.open("rb") as file_handle:
        response = requests.put(
            f"{bucket_url.rstrip('/')}/{encoded_name}",
            headers={"Authorization": f"Bearer {access_token}"},
            data=file_handle,
            timeout=1800,
        )
    request_ok(response, {200, 201}, f"Zenodo upload file {file_path.name}")
    try:
        return response.json()
    except ValueError:
        return {"filename": file_path.name, "size": file_path.stat().st_size}


def publish_deposition(
    zenodo_url: str,
    access_token: str,
    deposition_id: str,
) -> dict[str, Any]:
    response = requests.post(
        f"{zenodo_url}/{deposition_id}/actions/publish",
        headers=zenodo_auth_headers(access_token),
        timeout=120,
    )
    request_ok(response, {202}, "Zenodo publish")
    return response.json()


def derive_concept_doi(published: dict[str, Any]) -> str | None:
    if published.get("conceptdoi"):
        return str(published["conceptdoi"])

    version_doi = published.get("doi")
    concept_record_id = published.get("conceptrecid")
    if version_doi and concept_record_id:
        parts = str(version_doi).rsplit(".", 1)
        if len(parts) == 2:
            return f"{parts[0]}.{concept_record_id}"
    return None


def reserved_doi_from_draft(draft: dict[str, Any]) -> str | None:
    value = draft.get("metadata", {}).get("prereserve_doi", {})
    if isinstance(value, dict) and value.get("doi"):
        return str(value["doi"])
    if draft.get("doi"):
        return str(draft["doi"])
    return None


def append_log_row(log_file: Path, row: dict[str, Any]) -> None:
    fieldnames = [
        "run_at_utc",
        "status",
        "message",
        "api_base",
        "api_path",
        "download_url",
        "filters_json",
        "existing_deposition_id",
        "deposition_id",
        "record_id",
        "conceptrecid",
        "version_doi",
        "concept_doi",
        "prereserved_doi",
        "zenodo_html",
        "latest_draft_html",
        "bucket_url",
        "downloaded_filename",
        "downloaded_size_bytes",
        "source_content_type",
        "source_sha256",
        "source_etag",
        "source_last_modified",
        "canonical_schema_validated",
        "resource_schemas_json",
        "upload_filename",
        "upload_size_bytes",
        "wrapped_in_zip",
        "zip_member_name",
        "uploaded_files_json",
        "uploaded_file_count",
        "file_json_filename",
        "source_archive_uploaded",
        "removed_draft_files_json",
        "metadata_warnings_json",
        "sandbox",
        "title",
    ]

    log_file.parent.mkdir(parents=True, exist_ok=True)
    existing_rows: list[dict[str, str]] = []
    existing_header: list[str] = []

    if log_file.exists() and log_file.stat().st_size > 0:
        with log_file.open("r", encoding="utf-8", newline="") as file_handle:
            reader = csv.DictReader(file_handle)
            existing_header = reader.fieldnames or []
            existing_rows = list(reader)

    if existing_header and existing_header != fieldnames:
        with log_file.open("w", encoding="utf-8", newline="") as file_handle:
            writer = csv.DictWriter(file_handle, fieldnames=fieldnames)
            writer.writeheader()
            for existing in existing_rows:
                writer.writerow({key: existing.get(key, "") for key in fieldnames})

    file_exists = log_file.exists() and log_file.stat().st_size > 0
    with log_file.open("a", encoding="utf-8", newline="") as file_handle:
        writer = csv.DictWriter(file_handle, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        writer.writerow({key: row.get(key, "") for key in fieldnames})


def copy_prepared_files(files: list[Path], destination: Path) -> None:
    destination.mkdir(parents=True, exist_ok=True)
    for path in files:
        shutil.copy2(path, destination / path.name)


def ensure_unique_filenames(paths: list[Path]) -> None:
    names = [path.name for path in paths]
    duplicates = sorted({name for name in names if names.count(name) > 1})
    if duplicates:
        raise ValueError(f"Duplicate upload filenames: {duplicates}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Download an ECHOREPO CSV bundle, generate SoilWise-style CSVW "
            "file.json, and publish all resources under one Zenodo record DOI"
        )
    )
    parser.add_argument("--env-file", default=".env_zenodo")
    parser.add_argument("--sandbox", action="store_true")
    parser.add_argument("--api-base")
    parser.add_argument(
        "--api-path",
        default=DEFAULT_API_PATH,
        help=(
            "API path relative to --api-base; default: "
            f"{DEFAULT_API_PATH}"
        ),
    )
    parser.add_argument(
        "--source-file",
        help="Use a local ZIP/CSV instead of downloading from the API",
    )
    parser.add_argument("--echorepo-api-key")
    parser.add_argument("--echorepo-bearer-token")
    parser.add_argument("--zenodo-access-token")
    parser.add_argument("--existing-deposition-id")

    parser.add_argument("--title", required=True)
    parser.add_argument("--description", required=True)
    parser.add_argument("--creator", action="append", required=True)
    parser.add_argument("--keyword", action="append")
    parser.add_argument("--communities", nargs="*")
    parser.add_argument("--license", default="CC-BY-4.0")
    parser.add_argument("--access-right", default="open")
    parser.add_argument("--version")
    parser.add_argument("--grant", action="append")
    parser.add_argument("--subject", action="append")
    parser.add_argument("--copyright")

    parser.add_argument("--from-date")
    parser.add_argument("--to-date")
    parser.add_argument("--country")
    parser.add_argument("--country-code")
    parser.add_argument("--bbox")
    parser.add_argument("--within")
    parser.add_argument("--extra-param", action="append")

    parser.add_argument(
        "--csv-pattern",
        action="append",
        help=(
            "ZIP member basename pattern to publish; repeatable. Defaults to "
            "the four exact canonical resource filenames."
        ),
    )
    parser.add_argument(
        "--validate-canonical-schema",
        action=argparse.BooleanOptionalAction,
        default=True,
        help=(
            "Require the exact ECHOREPO canonical filenames and headers. "
            "Enabled by default; use --no-validate-canonical-schema only "
            "for deliberately non-canonical source files."
        ),
    )
    parser.add_argument(
        "--metadata-config",
        default=DEFAULT_METADATA_CONFIG,
        help="Curated column metadata JSON used to build file.json",
    )
    parser.add_argument("--file-json-name", default="file.json")
    parser.add_argument("--download-name")
    parser.add_argument(
        "--keep-source-archive",
        action="store_true",
        help="Also upload the downloaded ZIP as an optional convenience file",
    )
    parser.add_argument(
        "--source-archive-name",
        help="Rename the source ZIP when it is also uploaded",
    )
    parser.add_argument(
        "--extra-file",
        action="append",
        help="Additional local file to upload, e.g. README.md or LICENSE",
    )
    parser.add_argument(
        "--replace-draft-files",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Remove inherited/existing files from the new-version draft",
    )
    parser.add_argument(
        "--validate-soilvoc",
        action="store_true",
        help="Validate eusoilvoc propertyUrl values against the SoilVoc REST API",
    )
    parser.add_argument(
        "--require-soilvoc-validation",
        action="store_true",
        help="Fail if any live SoilVoc validation warning occurs",
    )
    parser.add_argument(
        "--soilvoc-api-base",
        default=DEFAULT_SOILVOC_API,
    )
    parser.add_argument(
        "--require-complete-metadata",
        action="store_true",
        help="Fail when metadata is provisional, unresolved, or auto-generated",
    )
    parser.add_argument(
        "--save-prepared-dir",
        help="Copy extracted CSVs and generated file.json to this directory",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Prepare and validate resources without creating a Zenodo draft",
    )
    parser.add_argument("--log-file", default="data/zenodo_sync_log.csv")
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    file_env = load_simple_env_file(args.env_file)

    sandbox = args.sandbox or bool_from_envish(
        env_or_config("ZENODO_SANDBOX", None, file_env),
        default=False,
    )
    zenodo_access_token = env_or_config(
        "ACCESS_TOKEN",
        args.zenodo_access_token,
        file_env,
    )
    echorepo_api_key = env_or_config(
        "ECHOREPO_API_KEY",
        args.echorepo_api_key,
        file_env,
    )
    echorepo_bearer = env_or_config(
        "ECHOREPO_BEARER_TOKEN",
        args.echorepo_bearer_token,
        file_env,
    )
    api_base = env_or_config("ZENODO_API_BASE", args.api_base, file_env)

    if not args.source_file:
        if not api_base:
            print("ERROR: missing --api-base / ZENODO_API_BASE", file=sys.stderr)
            return 2
        if not echorepo_api_key and not echorepo_bearer:
            print("ERROR: missing ECHOREPO API credentials", file=sys.stderr)
            return 2
    if not args.dry_run and not zenodo_access_token:
        print("ERROR: missing Zenodo access token", file=sys.stderr)
        return 2

    metadata = build_zenodo_metadata(args)
    filters = build_filter_params(args)
    patterns = args.csv_pattern or list(DEFAULT_CSV_PATTERNS)
    log_file = Path(args.log_file).expanduser()

    log_row: dict[str, Any] = {
        "run_at_utc": utc_now_iso(),
        "status": "started",
        "message": "",
        "api_base": api_base or "",
        "api_path": normalize_endpoint_path(args.api_path),
        "download_url": "",
        "filters_json": json.dumps(filters, ensure_ascii=False, sort_keys=True),
        "existing_deposition_id": args.existing_deposition_id or "",
        "deposition_id": "",
        "record_id": "",
        "conceptrecid": "",
        "version_doi": "",
        "concept_doi": "",
        "prereserved_doi": "",
        "zenodo_html": "",
        "latest_draft_html": "",
        "bucket_url": "",
        "downloaded_filename": "",
        "downloaded_size_bytes": "",
        "source_content_type": "",
        "source_sha256": "",
        "source_etag": "",
        "source_last_modified": "",
        "canonical_schema_validated": "0",
        "resource_schemas_json": "{}",
        "upload_filename": "",
        "upload_size_bytes": "",
        "wrapped_in_zip": "0",
        "zip_member_name": "",
        "uploaded_files_json": "[]",
        "uploaded_file_count": "0",
        "file_json_filename": args.file_json_name,
        "source_archive_uploaded": "1" if args.keep_source_archive else "0",
        "removed_draft_files_json": "[]",
        "metadata_warnings_json": "[]",
        "sandbox": "1" if sandbox else "0",
        "title": args.title,
    }

    try:
        with tempfile.TemporaryDirectory(prefix="echorepo_zenodo_") as temp_dir:
            temp_path = Path(temp_dir)
            resources_dir = temp_path / "resources"

            download_info: dict[str, Any] = {}

            if args.source_file:
                source_path = _resolve_input_path(args.source_file).resolve()
                if not source_path.exists():
                    raise FileNotFoundError(f"Source file does not exist: {source_path}")
                download_url = source_path.as_uri()
            else:
                download_name = args.download_name or infer_download_name_from_path(
                    args.api_path
                )
                source_path = temp_path / download_name
                headers = build_echorepo_headers(
                    echorepo_api_key,
                    echorepo_bearer,
                )
                download_info = download_api_file(
                    str(api_base),
                    args.api_path,
                    headers,
                    filters,
                    source_path,
                )
                download_url = str(download_info["download_url"])

            if source_path.stat().st_size == 0:
                raise RuntimeError("Downloaded source file is empty")

            source_content_type = str(download_info.get("content_type") or "")
            if not source_content_type:
                source_content_type = (
                    "application/zip"
                    if zipfile.is_zipfile(source_path)
                    else "text/csv"
                )

            log_row["download_url"] = download_url
            log_row["downloaded_filename"] = source_path.name
            log_row["downloaded_size_bytes"] = str(source_path.stat().st_size)
            log_row["source_content_type"] = source_content_type
            log_row["source_sha256"] = sha256_file(source_path)
            log_row["source_etag"] = str(download_info.get("etag") or "")
            log_row["source_last_modified"] = str(
                download_info.get("last_modified") or ""
            )

            csv_paths = extract_csv_resources(source_path, resources_dir, patterns)
            analyses = [analyse_csv(path) for path in csv_paths]

            if args.validate_canonical_schema:
                validate_echorepo_resource_schemas(analyses)
                log_row["canonical_schema_validated"] = "1"

            schema_summary = canonical_schema_summary(analyses)
            log_row["resource_schemas_json"] = json.dumps(
                schema_summary,
                ensure_ascii=False,
                sort_keys=True,
            )

            config, config_warnings = load_metadata_config(args.metadata_config)
            config_warnings = deduplicate_strings(
                [
                    *config_warnings,
                    *validate_metadata_config_against_analyses(
                        config,
                        analyses,
                    ),
                ]
            )

            # First pass validates the structure before creating a Zenodo draft.
            csvw_document, metadata_warnings = build_csvw_document(
                analyses,
                config,
                args,
                download_url,
                reserved_doi=None,
            )
            metadata_warnings = deduplicate_strings(
                [*config_warnings, *metadata_warnings]
            )
            validate_csvw_document(csvw_document, analyses)

            if args.validate_soilvoc or args.require_soilvoc_validation:
                soilvoc_warnings = validate_soilvoc_uris(
                    collect_soilvoc_uris(csvw_document),
                    args.soilvoc_api_base,
                )
                metadata_warnings = deduplicate_strings(
                    [*metadata_warnings, *soilvoc_warnings]
                )
                if args.require_soilvoc_validation and soilvoc_warnings:
                    raise RuntimeError(
                        "SoilVoc validation failed: " + "; ".join(soilvoc_warnings)
                    )

            if args.require_complete_metadata and metadata_warnings:
                raise RuntimeError(
                    "Metadata is not complete: " + "; ".join(metadata_warnings)
                )

            draft: dict[str, Any] | None = None
            deposition_id = ""
            bucket_url = ""
            reserved_doi: str | None = None
            removed_files: list[str] = []

            if not args.dry_run:
                zenodo_url = build_zenodo_base_url(sandbox)
                if args.existing_deposition_id:
                    draft = create_new_version_draft(
                        zenodo_url,
                        str(zenodo_access_token),
                        args.existing_deposition_id,
                    )
                else:
                    draft = create_new_deposition(
                        zenodo_url,
                        str(zenodo_access_token),
                    )

                deposition_id = str(draft["id"])
                bucket_url = str(draft["links"]["bucket"])
                reserved_doi = reserved_doi_from_draft(draft)
                log_row["deposition_id"] = deposition_id
                log_row["bucket_url"] = bucket_url
                log_row["latest_draft_html"] = str(
                    draft.get("links", {}).get("latest_draft_html", "")
                )
                log_row["prereserved_doi"] = reserved_doi or ""

                if args.replace_draft_files:
                    removed_files = clear_deposition_files(
                        zenodo_url,
                        str(zenodo_access_token),
                        deposition_id,
                    )

            # Rebuild file.json so it contains the reserved version DOI.
            csvw_document, second_pass_warnings = build_csvw_document(
                analyses,
                config,
                args,
                download_url,
                reserved_doi=reserved_doi,
            )
            metadata_warnings = deduplicate_strings(
                [*metadata_warnings, *second_pass_warnings]
            )
            validate_csvw_document(csvw_document, analyses)

            file_json_path = resources_dir / args.file_json_name
            file_json_path.write_text(
                json.dumps(csvw_document, ensure_ascii=False, indent=2) + "\n",
                encoding="utf-8",
            )

            upload_paths: list[Path] = [*csv_paths, file_json_path]

            for extra in args.extra_file or []:
                extra_path = _resolve_input_path(extra).resolve()
                if not extra_path.is_file():
                    raise FileNotFoundError(f"Extra upload file not found: {extra_path}")
                copied = resources_dir / extra_path.name
                shutil.copy2(extra_path, copied)
                upload_paths.append(copied)

            if args.keep_source_archive:
                if not zipfile.is_zipfile(source_path):
                    raise ValueError(
                        "--keep-source-archive was requested, but the source is not a ZIP"
                    )
                archive_name = args.source_archive_name or source_path.name
                copied_archive = resources_dir / archive_name
                if source_path.resolve() != copied_archive.resolve():
                    shutil.copy2(source_path, copied_archive)
                upload_paths.append(copied_archive)

            ensure_unique_filenames(upload_paths)
            upload_paths = sorted(upload_paths, key=lambda path: path.name)

            if args.save_prepared_dir:
                copy_prepared_files(upload_paths, Path(args.save_prepared_dir))

            uploaded_names = [path.name for path in upload_paths]
            total_upload_size = sum(path.stat().st_size for path in upload_paths)
            log_row["upload_filename"] = ",".join(uploaded_names)
            log_row["upload_size_bytes"] = str(total_upload_size)
            log_row["uploaded_files_json"] = json.dumps(
                uploaded_names,
                ensure_ascii=False,
            )
            log_row["uploaded_file_count"] = str(len(upload_paths))
            log_row["removed_draft_files_json"] = json.dumps(
                removed_files,
                ensure_ascii=False,
            )
            log_row["metadata_warnings_json"] = json.dumps(
                metadata_warnings,
                ensure_ascii=False,
            )

            if args.dry_run:
                log_row["status"] = "dry-run"
                log_row["message"] = "resources prepared; Zenodo was not called"
                append_log_row(log_file, log_row)
                print(
                    json.dumps(
                        {
                            "ok": True,
                            "dry_run": True,
                            "source": download_url,
                            "source_sha256": log_row["source_sha256"],
                            "canonical_schema_validated": (
                                log_row["canonical_schema_validated"] == "1"
                            ),
                            "resource_schemas": schema_summary,
                            "csv_files": [path.name for path in csv_paths],
                            "file_json": args.file_json_name,
                            "prepared_files": uploaded_names,
                            "save_prepared_dir": args.save_prepared_dir,
                            "metadata_warnings": metadata_warnings,
                            "log_file": str(log_file),
                        },
                        ensure_ascii=False,
                        indent=2,
                    )
                )
                return 0

            assert draft is not None
            zenodo_url = build_zenodo_base_url(sandbox)
            updated = update_metadata(
                zenodo_url,
                str(zenodo_access_token),
                deposition_id,
                metadata,
            )

            uploaded_results: list[dict[str, Any]] = []
            for path in upload_paths:
                uploaded_results.append(
                    upload_file_to_bucket(
                        bucket_url,
                        str(zenodo_access_token),
                        path,
                    )
                )

            remote_files = list_deposition_files(
                zenodo_url,
                str(zenodo_access_token),
                deposition_id,
            )
            remote_names = sorted(
                str(item.get("filename") or item.get("name") or "")
                for item in remote_files
            )
            if remote_names != sorted(uploaded_names):
                raise RuntimeError(
                    "Zenodo draft file list does not match the prepared upload list. "
                    f"Expected {sorted(uploaded_names)}, got {remote_names}"
                )

            published = publish_deposition(
                zenodo_url,
                str(zenodo_access_token),
                deposition_id,
            )

            log_row["status"] = "ok"
            log_row["message"] = "published"
            log_row["record_id"] = str(published.get("record_id", ""))
            log_row["conceptrecid"] = str(published.get("conceptrecid", ""))
            log_row["version_doi"] = str(published.get("doi", ""))
            log_row["concept_doi"] = derive_concept_doi(published) or ""
            log_row["zenodo_html"] = str(
                published.get("links", {}).get("html", "")
            )
            log_row["latest_draft_html"] = str(
                updated.get("links", {}).get(
                    "latest_draft_html",
                    log_row["latest_draft_html"],
                )
            )
            append_log_row(log_file, log_row)

            print(
                json.dumps(
                    {
                        "ok": True,
                        "sandbox": sandbox,
                        "api_download_url": download_url,
                        "api_path": log_row["api_path"],
                        "source_sha256": log_row["source_sha256"],
                        "canonical_schema_validated": (
                            log_row["canonical_schema_validated"] == "1"
                        ),
                        "resource_schemas": schema_summary,
                        "deposition_id": deposition_id,
                        "record_id": log_row["record_id"],
                        "conceptrecid": log_row["conceptrecid"],
                        "version_doi": log_row["version_doi"],
                        "concept_doi": log_row["concept_doi"],
                        "prereserved_doi": log_row["prereserved_doi"],
                        "zenodo_html": log_row["zenodo_html"],
                        "uploaded_files": uploaded_names,
                        "uploaded_file_count": len(upload_paths),
                        "removed_draft_files": removed_files,
                        "metadata_warnings": metadata_warnings,
                        "file_json": args.file_json_name,
                        "log_file": str(log_file),
                        "filters": filters,
                        "uploaded_results": uploaded_results,
                    },
                    ensure_ascii=False,
                    indent=2,
                )
            )
            return 0

    except Exception as exc:
        log_row["status"] = "error"
        log_row["message"] = str(exc)
        append_log_row(log_file, log_row)
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
