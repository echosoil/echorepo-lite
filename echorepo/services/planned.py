# echorepo/services/planned.py
from __future__ import annotations
import os
import re
from typing import Dict, Set
import pandas as pd
from ..config import settings

# Optional deps used by country normalisation:
#   pycountry, openpyxl  (add to requirements.txt)
import pycountry

# Common alias fixes.
# For stuff like "Scotland" we go straight to ISO2 "GB" so we don't depend
# on pycountry recognising the wording.
ALIASES = {
    # UK variants → GB
    "uk": "GB",
    "u.k.": "GB",
    "great britain": "GB",
    "gb": "GB",
    "england": "GB",
    "scotland": "GB",
    "wales": "GB",
    "northern ireland": "GB",

    # US variants
    "us": "United States",
    "u.s.": "United States",
    "usa": "United States",
    "u.s.a.": "United States",

    # other common ones
    "russia": "Russian Federation",
    "czech republic": "Czechia",
    "ivory coast": "Côte d’Ivoire",
    "south korea": "Korea, Republic of",
    "north korea": "Korea, Democratic People’s Republic of",
    "swaziland": "Eswatini",
    "cape verde": "Cabo Verde",
    "macedonia": "North Macedonia",
    "moldova": "Moldova, Republic of",
    "bolivia": "Bolivia, Plurinational State of",
    "tanzania": "Tanzania, United Republic of",
    "palestine": "Palestine, State of",
    "laos": "Lao People’s Democratic Republic",
    "vietnam": "Viet Nam",
    "syria": "Syrian Arab Republic",
    "iran": "Iran, Islamic Republic of",
    "venezuela": "Venezuela, Bolivarian Republic of",
}


def _country_to_iso2(name: str | None) -> str | None:
    if not name or not str(name).strip():
        return None

    s = str(name).strip()
    s_l = s.lower()

    # 1) our alias table first
    if s_l in ALIASES:
        ali = ALIASES[s_l]
        # if alias is already an ISO2 code, return it immediately
        if len(ali) == 2 and ali.isalpha():
            return ali.upper()
        # otherwise keep going with the aliased full name
        s = ali

    # 2) try pycountry directly
    try:
        c = pycountry.countries.lookup(s)
        return c.alpha_2
    except LookupError:
        # 3) tiny normalisation for curly apostrophes
        s2 = re.sub(r"[’`]", "'", s).strip()
        try:
            c = pycountry.countries.lookup(s2)
            return c.alpha_2
        except LookupError:
            return None


def _split_planned(s: str | None) -> Set[str]:
    """Split 'Denmark, Sweden; Scotland' -> {'DK','SE','GB'} (ISO2)."""
    if s is None or str(s).strip() == "":
        return set()
    parts = re.split(r"[;,]", str(s))
    out: Set[str] = set()
    for p in parts:
        iso2 = _country_to_iso2(p.strip())
        if iso2:
            out.add(iso2)
    return out


def _guess_columns(df: pd.DataFrame) -> tuple[str, str]:
    """
    Return (qr_col, planned_col) from an Excel with headers like:
      'QR code', 'QR_code', 'QR', ... and
      'Country Planned', 'Planned country', 'Countries planned', ...
    """
    qr_col = None
    planned_col = None
    for c in df.columns:
        cl = c.strip().lower()
        if cl in ("qr code", "qr_code", "qr"):
            qr_col = c
        if cl in ("country planned", "planned country", "countries planned"):
            planned_col = c
    if not qr_col or not planned_col:
        raise ValueError("Planned XLSX must have columns 'QR code' and 'Country Planned' (case-insensitive).")
    return qr_col, planned_col


def load_qr_to_planned(xlsx_path: str | None = None) -> Dict[str, Set[str]]:
    """
    Load planned countries per QR from an Excel file and return:
      { qr_code (str): {ISO2, ISO2, ...}, ... }

    - File path defaults to settings.PLANNED_XLSX (or PLANNED_XLSX env).
    - Duplicate QRs union their country sets.
    - Returns {} if path not set or file doesn’t exist.
    """
    path = (
        xlsx_path
        or getattr(settings, "PLANNED_XLSX", None)
        or os.getenv("PLANNED_XLSX")
    )
    if not path or not os.path.exists(path):
        print(f"[WARN][planned] planned countries XLSX not found at {path}, skipping")
        return {}
    else:
        print(f"[INFO][planned] loading planned countries from {path}")
    df = pd.read_excel(path, engine="openpyxl")
    qr_col, planned_col = _guess_columns(df)
    df = df[[qr_col, planned_col]].dropna(how="all")
    df[qr_col] = df[qr_col].astype(str).str.strip()
    df[planned_col] = df[planned_col].astype(str)

    mapping: Dict[str, Set[str]] = {}
    for _, row in df.iterrows():
        q = row[qr_col].strip()
        if not q:
            continue
        iso_set = _split_planned(row[planned_col])
        if not iso_set:
            continue
        if q in mapping:
            mapping[q] |= iso_set
        else:
            mapping[q] = set(iso_set)
    return mapping
