"""Shared canonical caption rules for flattened participant photo records."""

import re

import pandas as pd


def photo_text(value) -> str:
    if value is None or pd.isna(value):
        return ""
    text = str(value).strip()
    return "" if text.casefold() in {"nan", "none", "null", "<na>"} else text


def photo_description(option, comment) -> str:
    """Keep both distinct source strings, option first, without inventing text."""
    caption, note = photo_text(option), photo_text(comment)
    return "\n".join(dict.fromkeys(value for value in (caption, note) if value))


def photo_slots(columns) -> list[int]:
    """Discover all actual flattened photo slots, not a fixed maximum of 13."""
    slots = set()
    for column in columns:
        match = re.fullmatch(r"PHOTO_photos_([1-9][0-9]*)_path", str(column))
        if match:
            slots.add(int(match.group(1)))
    return sorted(slots)
