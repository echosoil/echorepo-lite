from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Any


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


VOCAB_PATH = (
    Path(__file__).resolve().parents[2]
    / "metadata"
    / "canonical"
    / "soil_categories.json"
)


def _normalize_lookup_key(value: Any) -> str:
    if value is None:
        return ""

    return " ".join(
        str(value)
        .strip()
        .casefold()
        .split()
    )


@lru_cache(maxsize=1)
def load_soil_category_mapping() -> dict[str, dict[str, str]]:
    with VOCAB_PATH.open(
        "r",
        encoding="utf-8",
    ) as handle:
        raw = json.load(handle)

    result: dict[str, dict[str, str]] = {}

    for category in (
        "soil_texture",
        "soil_structure",
    ):
        values = raw.get(category, {})

        result[category] = {
            _normalize_lookup_key(source): (
                str(target)
                .strip()
                .lower()
            )
            for source, target in values.items()
        }

    return result


def canonical_soil_value(
    value: Any,
    category: str,
) -> str | None:
    if category not in (
        "soil_texture",
        "soil_structure",
    ):
        raise ValueError(
            f"Unknown soil category: {category}"
        )

    key = _normalize_lookup_key(value)

    if not key:
        return None

    mapping = load_soil_category_mapping()

    return mapping[category].get(key)


def standardize_soil_columns(df):
    """
    Derive canonical English texture/structure values
    from the corresponding *_orig columns.

    *_orig values are preserved unchanged.
    *_en values become lowercase canonical English.
    """
    df = df.copy()

    if "soil_texture_orig" in df.columns:
        df["soil_texture_en"] = (
            df["soil_texture_orig"].map(
                lambda value: canonical_soil_value(
                    value,
                    "soil_texture",
                )
            )
        )

    if "soil_structure_orig" in df.columns:
        df["soil_structure_en"] = (
            df["soil_structure_orig"].map(
                lambda value: canonical_soil_value(
                    value,
                    "soil_structure",
                )
            )
        )

    return df