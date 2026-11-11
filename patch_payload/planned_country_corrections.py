"""Restricted, auditable correction of Country Planned in the shared workbook.

The web route validates permissions and the QA record before calling this function.
Only an unambiguous matching QR is allowed; all workbook edits are serialized.
"""

from __future__ import annotations

import fcntl
import os
import re
import shutil
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from openpyxl import load_workbook

from .planned import _split_planned


class PlannedCountryCorrectionError(ValueError):
    pass


def norm_qr(value):
    """Match ingestion's QR normalization (uppercase, remove literal spaces)."""
    return str(value or "").strip().replace(" ", "").upper()


def planned_workbook_path() -> Path:
    """Follow pull_and_enrich_samples.py rather than Config's older /data default."""
    value = os.getenv("PLANNED_XLSX", "/data/utils/planned.xlsx")
    path = Path(value).expanduser()
    if not path.is_absolute():
        return (Path(__file__).resolve().parents[2] / path).resolve()
    if path.exists():
        return path.resolve()
    # Local development, outside the container: /data/... -> repository/data/...
    if str(path).startswith("/data/"):
        return (Path(__file__).resolve().parents[2] / "data" / path.relative_to("/data")).resolve()
    return path


def _columns(ws):
    headers = {str(c.value).strip().casefold(): c.column for c in ws[1] if c.value is not None}
    if "qr code" not in headers or "country planned" not in headers:
        raise PlannedCountryCorrectionError(
            "Missing 'QR Code' or 'Country Planned' column in the first worksheet"
        )
    return headers["qr code"], headers["country planned"]


def _matches(ws, qr, qr_col):
    return [i for i in range(2, ws.max_row + 1) if norm_qr(ws.cell(i, qr_col).value) == qr]


def read_planned_country_matches(path: Path, qrs: set[str]) -> dict[str, set[str]]:
    """Read the workbook once for QA-page display; duplicates are not concealed."""
    if not path.is_file():
        return {}
    book = load_workbook(path, read_only=True, data_only=True)
    try:
        ws = book.worksheets[0]
        qr_col, planned_col = _columns(ws)
        out: dict[str, list[set[str]]] = {}
        for row in ws.iter_rows(min_row=2, values_only=True):
            qr = norm_qr(row[qr_col - 1] if len(row) >= qr_col else None)
            if qr in qrs:
                raw = row[planned_col - 1] if len(row) >= planned_col else None
                out.setdefault(qr, []).append(_split_planned(raw))
        # Duplicates are not safe for automatic editing, even if values agree.
        return {qr: values[0] for qr, values in out.items() if len(values) == 1}
    finally:
        book.close()


def update_planned_country(
    path: Path, qr: str, actual_country: str, *, expected_planned: str
) -> tuple[str, Path]:
    """Update exactly one QR, atomically, with backup and a file lock.

    expected_planned comes from the trusted QA CSV, not a user-supplied field.
    Old values must still match that QA snapshot; otherwise an enrichment refresh
    or manual review is required before changing the workbook.
    Returns (previous workbook value, backup location).
    """
    path = Path(path)
    qr = norm_qr(qr)
    country = str(actual_country or "").strip().upper()
    if not re.fullmatch(r"[A-Z]{4}-\d{4}", qr):
        raise PlannedCountryCorrectionError("Invalid QR code")
    if not re.fullmatch(r"[A-Z]{2}", country):
        raise PlannedCountryCorrectionError("Invalid actual country ISO2 code")
    if not path.is_file() or path.is_symlink():
        raise PlannedCountryCorrectionError(f"Planned workbook unavailable or unsafe: {path}")

    # A distinct lock file remains stable when the workbook is atomically replaced.
    lock_path = path.with_name(path.name + ".country-edit.lock")
    with lock_path.open("a+b") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        if not path.is_file() or path.is_symlink():
            raise PlannedCountryCorrectionError("Planned workbook changed during edit")
        book = load_workbook(path)
        try:
            ws = book.worksheets[0]
            qr_col, planned_col = _columns(ws)
            matches = _matches(ws, qr, qr_col)
            if len(matches) != 1:
                raise PlannedCountryCorrectionError(
                    f"QR {qr} has {len(matches)} rows in planned.xlsx; expected exactly one"
                )
            cell = ws.cell(matches[0], planned_col)
            old_value = str(cell.value or "").strip()
            old_countries = _split_planned(old_value)
            expected = {x.strip().upper() for x in expected_planned.split(",") if x.strip()}
            if old_countries != expected:
                raise PlannedCountryCorrectionError(
                    f"Planned value changed since QA snapshot: {old_value!r} != {expected_planned!r}"
                )
            if country in old_countries:
                raise PlannedCountryCorrectionError(
                    "The actual country already matches the current workbook; refresh QA instead"
                )
            cell.value = country
            timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            backup = path.with_name(f"{path.name}.bak.{timestamp}.{uuid4().hex[:8]}")
            shutil.copy2(path, backup)
            tmp_path = None
            try:
                fd, tmp_name = tempfile.mkstemp(
                    prefix=".planned-country-", suffix=".xlsx", dir=path.parent
                )
                tmp_path = Path(tmp_name)
                os.close(fd)
                book.save(tmp_path)
                shutil.copymode(path, tmp_path)
                try:
                    stat = path.stat()
                    os.chown(tmp_path, stat.st_uid, stat.st_gid)
                except PermissionError:
                    pass
                with tmp_path.open("rb") as fh:
                    os.fsync(fh.fileno())
                os.replace(tmp_path, path)
                tmp_path = None
                # Persist directory entry, if supported.
                try:
                    dir_fd = os.open(path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
                    try:
                        os.fsync(dir_fd)
                    finally:
                        os.close(dir_fd)
                except OSError:
                    pass
                return old_value, backup
            finally:
                if tmp_path is not None:
                    tmp_path.unlink(missing_ok=True)
        finally:
            book.close()
