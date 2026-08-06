from __future__ import annotations

import io
import json
import os
import sqlite3
from pathlib import Path
from typing import Any

import pandas as pd

from echorepo.config import settings
from echorepo.services.db import _ensure_lab_enrichment

# Transitional import. Later, move this helper from routes/data_api.py
# into a shared service module.
from echorepo.routes.data_api import _oxide_to_metal

from .exceptions import (
    InvalidLabUpload,
    LabImportError,
)


def _normalize_qr(raw: Any) -> str:
    if raw is None:
        return ""

    value = str(raw).strip()

    if not value:
        return ""

    if value.upper().startswith("ECHO-"):
        value = value[5:]

    if "-" not in value and len(value) >= 5:
        value = value[:4] + "-" + value[4:]

    return value.upper()


def _read_metals_dataframe(
    data: bytes,
    filename: str,
) -> pd.DataFrame:
    if not data:
        raise InvalidLabUpload(
            f"{filename}: uploaded file is empty"
        )

    suffix = Path(filename).suffix.lower()

    try:
        if suffix == ".xlsx":
            df = pd.read_excel(
                io.BytesIO(data),
            )

        elif suffix == ".csv":
            df = pd.read_csv(
                io.BytesIO(data),
                sep=",",
            )

        elif suffix == ".tsv":
            df = pd.read_csv(
                io.BytesIO(data),
                sep="\t",
            )

        elif suffix == ".txt":
            # Allow delimiter detection for generic text files.
            df = pd.read_csv(
                io.BytesIO(data),
                sep=None,
                engine="python",
            )

        else:
            raise InvalidLabUpload(
                f"Unsupported laboratory file type: "
                f"{suffix or '(no extension)'}. "
                "Expected XLSX, CSV, TSV, or TXT."
            )

    except InvalidLabUpload:
        raise

    except Exception as exc:
        raise InvalidLabUpload(
            f"Cannot read {filename}: {exc}"
        ) from exc

    if df.empty:
        raise InvalidLabUpload(
            f"{filename}: uploaded file has no rows"
        )

    return df


def import_metals_file(
    data: bytes,
    filename: str,
    uploader_id: str,
) -> int:
    """
    Import a metals/laboratory XLSX, CSV, TSV, or TXT file.

    Existing qr_code + param combinations are updated.
    Unrelated existing laboratory values are preserved.

    Returns the number of SQL upsert operations performed, including
    elemental values derived from oxide measurements.
    """
    filename = str(filename or "").strip()

    if not filename:
        raise InvalidLabUpload(
            "Uploaded file has no filename"
        )

    df = _read_metals_dataframe(
        data=data,
        filename=filename,
    )

    db_path = str(settings.SQLITE_PATH or "").strip()

    if not db_path:
        raise LabImportError(
            "SQLITE_PATH is not configured"
        )

    if not os.path.isfile(db_path):
        raise LabImportError(
            f"SQLite database not found at {db_path}"
        )

    fieldnames = list(df.columns)

    id_column = next(
        (
            column
            for column in fieldnames
            if str(column).strip().lower() == "id"
        ),
        None,
    )

    if id_column is None:
        raise InvalidLabUpload(
            f"{filename}: expected an ID column"
        )

    values_upserted = 0

    try:
        with sqlite3.connect(db_path) as conn:
            _ensure_lab_enrichment(conn)
            cur = conn.cursor()

            for _, row in df.iterrows():
                raw_dict = row.to_dict()

                qr = _normalize_qr(
                    raw_dict.get(id_column)
                )

                if not qr:
                    continue

                clean_raw = {
                    str(key): (
                        ""
                        if pd.isna(value)
                        else value
                    )
                    for key, value in raw_dict.items()
                }

                raw_json = json.dumps(
                    clean_raw,
                    ensure_ascii=False,
                    default=str,
                )

                for index, column in enumerate(fieldnames):
                    column_name = str(column).strip()

                    if column == id_column:
                        continue

                    if column_name.lower().startswith("unit"):
                        continue

                    value = row.get(column)

                    if pd.isna(value):
                        continue

                    if isinstance(value, str) and not value.strip():
                        continue

                    parameter = column_name

                    if not parameter:
                        continue

                    unit = ""

                    if index + 1 < len(fieldnames):
                        possible_unit_column = (
                            fieldnames[index + 1]
                        )

                        if (
                            str(possible_unit_column)
                            .strip()
                            .lower()
                            .startswith("unit")
                        ):
                            unit_value = row.get(
                                possible_unit_column
                            )

                            if not pd.isna(unit_value):
                                unit = str(
                                    unit_value
                                ).strip()

                    cur.execute(
                        """
                        INSERT INTO lab_enrichment (
                            qr_code,
                            param,
                            value,
                            unit,
                            user_id,
                            raw_row,
                            updated_at
                        )
                        VALUES (
                            ?, ?, ?, ?, ?, ?,
                            datetime('now')
                        )
                        ON CONFLICT (
                            qr_code,
                            param
                        )
                        DO UPDATE SET
                            value = excluded.value,
                            unit = excluded.unit,
                            user_id = excluded.user_id,
                            raw_row = excluded.raw_row,
                            updated_at = datetime('now')
                        """,
                        (
                            qr,
                            parameter,
                            str(value),
                            unit,
                            uploader_id,
                            raw_json,
                        ),
                    )

                    values_upserted += 1

                    converted = _oxide_to_metal(
                        parameter,
                        value,
                    )

                    if converted is None:
                        continue

                    metal_parameter, metal_value = converted

                    cur.execute(
                        """
                        INSERT INTO lab_enrichment (
                            qr_code,
                            param,
                            value,
                            unit,
                            user_id,
                            raw_row,
                            updated_at
                        )
                        VALUES (
                            ?, ?, ?, ?, ?, ?,
                            datetime('now')
                        )
                        ON CONFLICT (
                            qr_code,
                            param
                        )
                        DO UPDATE SET
                            value = excluded.value,
                            unit = excluded.unit,
                            user_id = excluded.user_id,
                            raw_row = excluded.raw_row,
                            updated_at = datetime('now')
                        """,
                        (
                            qr,
                            metal_parameter,
                            str(metal_value),
                            unit,
                            uploader_id,
                            raw_json,
                        ),
                    )

                    values_upserted += 1

            # The sqlite3 connection context manager commits here
            # when no exception occurred.

    except sqlite3.Error as exc:
        raise LabImportError(
            f"SQLite import failed for {filename}: {exc}"
        ) from exc

    except LabImportError:
        raise

    except Exception as exc:
        raise LabImportError(
            f"Laboratory import failed for {filename}: {exc}"
        ) from exc

    return values_upserted