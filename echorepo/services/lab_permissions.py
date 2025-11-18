import csv
from pathlib import Path
from typing import Set

from flask import current_app


def _allowlist_path() -> Path:
    """
    Returns the path to the allowlist CSV.

    Can be overridden via LAB_UPLOAD_ALLOWLIST_PATH in Flask config,
    otherwise defaults to ../data/config/lab_upload_lab_allowlist.csv
    relative to the package root.
    """
    override = current_app.config.get("LAB_UPLOAD_ALLOWLIST_PATH")
    if override:
        return Path(override)
    return Path(current_app.root_path).parent.parent / "data" / "config" / "lab_upload_lab_allowlist.csv"


def _read_allowlist() -> Set[str]:
    path = _allowlist_path()
    if not path.exists():
        return set()

    allowed: Set[str] = set()
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            return set()

        # Prefer 'user_key' column, otherwise use first column
        key_field = "user_key" if "user_key" in reader.fieldnames else reader.fieldnames[0]

        for row in reader:
            value = (row.get(key_field) or "").strip()
            if value:
                allowed.add(value)
    return allowed


def can_upload_lab_data(user_key: str | None) -> bool:
    if not user_key:
        return False
    allowlist = _read_allowlist()
    return user_key in allowlist
