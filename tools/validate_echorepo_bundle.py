#!/usr/bin/env python3
"""
Validate the public ECHOREPO machine-readable canonical bundle.

Usage:
    python validate_echorepo_bundle.py canonical_all.zip
    python validate_echorepo_bundle.py /path/to/unpacked_bundle

The validator treats samples.csv as the parent table and, when the other
canonical CSV files are present, also checks the documented cross-table keys.
"""

from __future__ import annotations

import csv
import io
import re
import sys
import zipfile
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Iterable

EXPECTED_SCHEMAS = {
    "samples.csv": [
        "sample_id",
        "sampling_datetime_utc",
        "lat",
        "lon",
        "country_code",
        "coordinate_obfuscation_radius_m",
        "soil_ph_field",
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
        "image_datetime_utc",
        "licence",
    ],
    "sample_parameters.csv": [
        "sample_id",
        "country_code",
        "parameter_code",
        "result_value",
        "unit",
        "method_code",
        "analysis_datetime_utc",
        "lab_id",
        "licence",
    ],
    "sample_biodiversity.csv": [
        "sample_id",
        "country_code",
        "marker",
        "taxon_rank",
        "scientific_name",
        "read_count",
        "relative_abundance_pct",
        "ingested_datetime_utc",
        "source_file",
        "licence",
    ],
    "parameter_definitions.csv": [
        "parameter_code",
        "parameter_name",
        "parameter_uri",
        "default_unit",
        "method_code",
        "parameter_description",
    ],
    "analysis_methods.csv": [
        "method_code",
        "method_name",
        "method_description",
        "procedure_uri",
    ],
}

TEXTURE_VALUES = {
    "sandy",
    "sandy loam",
    "sandy clay",
    "silty",
    "silty clay",
    "clayey",
    "clay loam",
    "other",
    "no data",
}

STRUCTURE_VALUES = {
    "friable",
    "intact",
    "firm",
    "compact",
    "high compact",
    "no data",
}

INTEGER_COLUMNS = {
    "earthworms_count",
    "pollutants_count",
    "read_count",
}

NUMERIC_COLUMNS = {
    "lat",
    "lon",
    "coordinate_obfuscation_radius_m",
    "soil_ph_field",
    "soil_organic_matter_estimate_pct",
    "earthworms_count",
    "contamination_debris",
    "contamination_plastic",
    "pollutants_count",
    "result_value",
    "read_count",
    "relative_abundance_pct",
}

TIMESTAMP_COLUMNS = {
    "sampling_datetime_utc",
    "image_datetime_utc",
    "analysis_datetime_utc",
    "ingested_datetime_utc",
}

COUNTRY_RE = re.compile(r"^[A-Z]{2}$")


class Validation:
    def __init__(self) -> None:
        self.errors: list[str] = []
        self.warnings: list[str] = []

    def error(self, msg: str) -> None:
        self.errors.append(msg)

    def warn(self, msg: str) -> None:
        self.warnings.append(msg)


def is_blank(value: str | None) -> bool:
    return value is None or str(value).strip() == ""


def parse_number(value: str) -> float:
    return float(value)


def parse_integer(value: str) -> int:
    number = float(value)
    if not number.is_integer():
        raise ValueError("not an integer")
    return int(number)


def parse_utc_timestamp(value: str) -> datetime:
    text = value.strip()
    if not text.endswith("Z"):
        raise ValueError("timestamp is not UTC/Z")
    return datetime.fromisoformat(text[:-1] + "+00:00")


def load_csvs(path: Path, v: Validation) -> dict[str, list[dict[str, str]]]:
    tables: dict[str, list[dict[str, str]]] = {}

    if path.is_dir():
        names = {p.name: p for p in path.iterdir() if p.is_file()}
        for filename in EXPECTED_SCHEMAS:
            p = names.get(filename)
            if p is None:
                continue
            with p.open("r", encoding="utf-8-sig", newline="") as fh:
                reader = csv.DictReader(fh)
                check_header(filename, reader.fieldnames, v)
                tables[filename] = list(reader)
        return tables

    if not zipfile.is_zipfile(path):
        v.error(f"{path} is neither a directory nor a ZIP archive")
        return tables

    with zipfile.ZipFile(path) as zf:
        members = set(zf.namelist())
        for filename in EXPECTED_SCHEMAS:
            if filename not in members:
                continue
            with zf.open(filename) as raw:
                text = io.TextIOWrapper(raw, encoding="utf-8-sig", newline="")
                reader = csv.DictReader(text)
                check_header(filename, reader.fieldnames, v)
                tables[filename] = list(reader)

    return tables


def check_header(filename: str, fieldnames: list[str] | None, v: Validation) -> None:
    actual = fieldnames or []
    expected = EXPECTED_SCHEMAS[filename]
    if actual != expected:
        v.error(
            f"{filename}: header mismatch\n"
            f"  expected: {expected}\n"
            f"  actual:   {actual}"
        )


def validate_generic_types(filename: str, rows: Iterable[dict[str, str]], v: Validation) -> None:
    for line_no, row in enumerate(rows, start=2):
        for col in NUMERIC_COLUMNS & row.keys():
            value = row.get(col)
            if is_blank(value):
                continue
            try:
                parse_number(str(value))
            except ValueError:
                v.error(f"{filename}:{line_no}: {col} is not numeric: {value!r}")

        for col in INTEGER_COLUMNS & row.keys():
            value = row.get(col)
            if is_blank(value):
                continue
            try:
                parse_integer(str(value))
            except ValueError:
                v.error(f"{filename}:{line_no}: {col} is not an integer: {value!r}")

        for col in TIMESTAMP_COLUMNS & row.keys():
            value = row.get(col)
            if is_blank(value):
                continue
            try:
                parse_utc_timestamp(str(value))
            except ValueError as exc:
                v.error(f"{filename}:{line_no}: invalid {col}: {value!r} ({exc})")


def validate_samples(rows: list[dict[str, str]], v: Validation) -> None:
    ids = [str(row.get("sample_id") or "").strip() for row in rows]

    for line_no, sample_id in enumerate(ids, start=2):
        if not sample_id:
            v.error(f"samples.csv:{line_no}: blank sample_id")

    duplicates = sorted(k for k, count in Counter(ids).items() if k and count > 1)
    if duplicates:
        v.error(f"samples.csv: duplicate sample_id values: {duplicates[:20]}")

    for line_no, row in enumerate(rows, start=2):
        def number(col: str) -> float | None:
            value = row.get(col)
            if is_blank(value):
                return None
            try:
                return float(str(value))
            except ValueError:
                return None

        lat = number("lat")
        lon = number("lon")
        radius = number("coordinate_obfuscation_radius_m")
        ph = number("soil_ph_field")
        som = number("soil_organic_matter_estimate_pct")

        if lat is not None and not (-90 <= lat <= 90):
            v.error(f"samples.csv:{line_no}: latitude outside [-90,90]: {lat}")
        if lon is not None and not (-180 <= lon <= 180):
            v.error(f"samples.csv:{line_no}: longitude outside [-180,180]: {lon}")
        if radius is not None and radius < 0:
            v.error(f"samples.csv:{line_no}: negative coordinate_obfuscation_radius_m: {radius}")

        cc = str(row.get("country_code") or "").strip()
        if cc and not COUNTRY_RE.fullmatch(cc):
            v.warn(f"samples.csv:{line_no}: country_code is not ISO alpha-2 uppercase: {cc!r}")

        # Domain sanity checks: useful for consumers, but not enforced by the export code.
        if ph is not None and not (0 <= ph <= 14):
            v.warn(f"samples.csv:{line_no}: soil_ph_field outside usual pH range 0..14: {ph}")
        if som is not None and not (0 <= som <= 100):
            v.warn(
                f"samples.csv:{line_no}: soil_organic_matter_estimate_pct outside 0..100: {som}"
            )

        texture = str(row.get("soil_texture_en") or "").strip()
        if texture and texture not in TEXTURE_VALUES:
            v.error(f"samples.csv:{line_no}: unexpected soil_texture_en: {texture!r}")

        structure = str(row.get("soil_structure_en") or "").strip()
        if structure and structure not in STRUCTURE_VALUES:
            v.error(f"samples.csv:{line_no}: unexpected soil_structure_en: {structure!r}")


def unique_key(
    filename: str,
    rows: list[dict[str, str]],
    columns: tuple[str, ...],
    v: Validation,
) -> set[tuple[str, ...]]:
    keys: list[tuple[str, ...]] = []
    for line_no, row in enumerate(rows, start=2):
        key = tuple(str(row.get(col) or "").strip() for col in columns)
        if any(not part for part in key):
            v.error(f"{filename}:{line_no}: blank primary-key component {columns}: {key}")
        keys.append(key)

    duplicates = sorted(k for k, count in Counter(keys).items() if count > 1)
    if duplicates:
        v.error(f"{filename}: duplicate key {columns}: {duplicates[:20]}")
    return set(keys)


def check_foreign_keys(tables: dict[str, list[dict[str, str]]], v: Validation) -> None:
    samples = tables.get("samples.csv")
    if samples is None:
        return

    sample_ids = {str(r.get("sample_id") or "").strip() for r in samples}

    for child in ("sample_images.csv", "sample_parameters.csv", "sample_biodiversity.csv"):
        rows = tables.get(child)
        if rows is None:
            continue
        orphans = sorted(
            {
                str(r.get("sample_id") or "").strip()
                for r in rows
                if str(r.get("sample_id") or "").strip() not in sample_ids
            }
        )
        if orphans:
            v.error(f"{child}: sample_id values absent from samples.csv: {orphans[:20]}")

    definitions = tables.get("parameter_definitions.csv")
    methods = tables.get("analysis_methods.csv")
    params = tables.get("sample_parameters.csv")

    definition_codes: set[str] = set()
    method_codes: set[str] = set()

    if definitions is not None:
        definition_codes = {
            key[0] for key in unique_key(
                "parameter_definitions.csv", definitions, ("parameter_code",), v
            )
        }

    if methods is not None:
        method_codes = {
            key[0] for key in unique_key(
                "analysis_methods.csv", methods, ("method_code",), v
            )
        }

    images = tables.get("sample_images.csv")
    if images is not None:
        unique_key("sample_images.csv", images, ("sample_id", "image_id"), v)

    if params is not None and definition_codes:
        missing = sorted(
            {
                str(r.get("parameter_code") or "").strip()
                for r in params
                if str(r.get("parameter_code") or "").strip() not in definition_codes
            }
        )
        if missing:
            v.error(f"sample_parameters.csv: unknown parameter_code values: {missing}")

    if params is not None and method_codes:
        missing = sorted(
            {
                str(r.get("method_code") or "").strip()
                for r in params
                if str(r.get("method_code") or "").strip() not in method_codes
            }
        )
        if missing:
            v.error(f"sample_parameters.csv: unknown method_code values: {missing}")

    if definitions is not None and method_codes:
        missing = sorted(
            {
                str(r.get("method_code") or "").strip()
                for r in definitions
                if str(r.get("method_code") or "").strip()
                and str(r.get("method_code") or "").strip() not in method_codes
            }
        )
        if missing:
            v.error(f"parameter_definitions.csv: unknown method_code values: {missing}")


def main() -> int:
    if len(sys.argv) != 2:
        print("Usage: python validate_echorepo_bundle.py <canonical_all.zip|directory>")
        return 2

    path = Path(sys.argv[1])
    v = Validation()

    if not path.exists():
        print(f"ERROR: path does not exist: {path}")
        return 2

    tables = load_csvs(path, v)

    if "samples.csv" not in tables:
        v.error("samples.csv is missing")

    for filename, rows in tables.items():
        validate_generic_types(filename, rows, v)

    if "samples.csv" in tables:
        validate_samples(tables["samples.csv"], v)

    check_foreign_keys(tables, v)

    print("ECHOREPO canonical bundle validation")
    print(f"Source: {path}")
    for filename in EXPECTED_SCHEMAS:
        if filename in tables:
            print(f"  {filename}: {len(tables[filename])} data rows")
        else:
            print(f"  {filename}: not present")

    if v.warnings:
        print("\nWARNINGS")
        for item in v.warnings:
            print(f"  - {item}")

    if v.errors:
        print("\nERRORS")
        for item in v.errors:
            print(f"  - {item}")
        print(f"\nFAILED: {len(v.errors)} error(s), {len(v.warnings)} warning(s)")
        return 1

    print(f"\nOK: 0 errors, {len(v.warnings)} warning(s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
