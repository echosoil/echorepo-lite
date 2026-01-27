import os
import re
from functools import lru_cache

import pandas as pd

from ..config import settings
from ..utils.country import country_to_iso2


def _split_planned(s: str) -> set[str]:
    if not s or not str(s).strip():
        return set()
    parts = re.split(r"[;,]", str(s))
    out = set()
    for p in parts:
        iso2 = country_to_iso2(p.strip())
        if iso2:
            out.add(iso2)
    return out


@lru_cache(maxsize=1)
def load_qr_to_planned() -> dict[str, set[str]]:
    path = settings.PLANNED_XLSX  # e.g. /data/planned.xlsx or /data/planned.csv
    if not path or not os.path.exists(path):
        return {}
    if path.lower().endswith((".xlsx", ".xls")):
        df = pd.read_excel(path, engine="openpyxl")
    else:
        df = pd.read_csv(path, dtype=str, keep_default_na=False)

    col_qr = None
    col_countries = None
    for c in df.columns:
        cl = c.strip().lower()
        if cl in ("qr code", "qr_code", "qr"):
            col_qr = c
        if cl in ("country planned", "planned country", "countries planned"):
            col_countries = c
    if not col_qr or not col_countries:
        return {}

    df = df[[col_qr, col_countries]].dropna(how="all")
    df[col_qr] = df[col_qr].astype(str).str.strip()
    df[col_countries] = df[col_countries].astype(str)

    mapping: dict[str, set[str]] = {}
    for _, row in df.iterrows():
        q = row[col_qr].strip()
        if not q:
            continue
        s = _split_planned(row[col_countries])
        if not s:
            continue
        mapping.setdefault(q, set()).update(s)
    return mapping
