# echorepo/services/validation.py
import numpy as np
import pandas as pd
import reverse_geocoder as rg

from ..config import settings
from .planned import load_qr_to_planned

# --- helpers ---------------------------------------------------------------

def _clean_coords(df: pd.DataFrame, lat_col: str, lon_col: str):
    """Return (lat_f, lon_f, valid_mask) with floats and world-bounds mask."""
    lat_s = df[lat_col].astype(str).str.replace(",", ".", regex=False).str.strip()
    lon_s = df[lon_col].astype(str).str.replace(",", ".", regex=False).str.strip()
    lat_f = pd.to_numeric(lat_s, errors="coerce")
    lon_f = pd.to_numeric(lon_s, errors="coerce")
    mask = (
        np.isfinite(lat_f) & np.isfinite(lon_f) &
        (lat_f.between(-90.0, 90.0)) &
        (lon_f.between(-180.0, 180.0))
    )
    # Exclude your app's default/sentinel coords
    bad_default = (lat_f == settings.DEFAULT_COORD_LAT) & (lon_f == settings.DEFAULT_COORD_LON)
    mask = mask & (~bad_default)
    return lat_f, lon_f, mask

# --- public API ------------------------------------------------------------

def find_default_coord_rows(df: pd.DataFrame,
                            lat_col: str | None = None,
                            lon_col: str | None = None) -> pd.DataFrame:
    """
    Rows that have the sentinel default coordinates (e.g., 46.5, 11.35).
    """
    if df is None or df.empty:
        return df.iloc[0:0].copy()

    # 👇 prefer original columns if available
    orig_lat_col = getattr(settings, "ORIG_LAT_COL", None)
    print(f"[DEBUG] ORIG_LAT_COL={orig_lat_col}")
    print(f"[DEBUG] DF[ORIG_LAT_COL]={df[orig_lat_col] if orig_lat_col in df.columns else 'N/A'}")  
    lat_col = lat_col or getattr(settings, "ORIG_LAT_COL", None) or settings.LAT_COL
    lon_col = lon_col or getattr(settings, "ORIG_LON_COL", None) or settings.LON_COL

    lat_s = df[lat_col].astype(str).str.replace(",", ".", regex=False).str.strip()
    lon_s = df[lon_col].astype(str).str.replace(",", ".", regex=False).str.strip()
    lat_f = pd.to_numeric(lat_s, errors="coerce")
    lon_f = pd.to_numeric(lon_s, errors="coerce")

    mask_default = (lat_f == settings.DEFAULT_COORD_LAT) & (lon_f == settings.DEFAULT_COORD_LON)
    return df.loc[mask_default].copy()

def select_country_mismatches(df: pd.DataFrame,
                              qr_col: str | None = None,
                              lat_col: str | None = None,
                              lon_col: str | None = None) -> pd.DataFrame:
    """
    Return *only* rows that are true mismatches:
      - have valid coords (not sentinel defaults)
      - have an actual country code
      - have a planned set (non-empty)
      - actual_cc NOT in planned_iso2_set
    """
    ann = annotate_country_mismatches(df, qr_col=qr_col, lat_col=lat_col, lon_col=lon_col)
    if ann is None or ann.empty:
        return ann

    # planned present & actual present & NOT match
    has_planned = ann["planned_iso2_set"].apply(bool)
    has_actual  = ann["actual_cc"].notna()
    mism_mask   = has_planned & has_actual & (~ann["planned_match"])
    return ann.loc[mism_mask].copy()

def annotate_country_mismatches(df: pd.DataFrame,
                                qr_col: str | None = None,
                                lat_col: str | None = None,
                                lon_col: str | None = None) -> pd.DataFrame:
    """
    Add columns:
      - actual_cc: ISO2 from reverse geocoding (None if invalid/missing coords)
      - planned_iso2_set: set[str] of allowed countries per QR (never None; use empty set)
      - planned_iso2: pretty CSV version for display
      - planned_match: bool (actual_cc ∈ planned_iso2_set)
    Excludes rows with sentinel default coords from the match logic.
    """
    if df is None or df.empty:
        out = df.copy()
        out["actual_cc"] = None
        out["planned_iso2_set"] = [set()] * len(out)
        out["planned_iso2"] = ""
        out["planned_match"] = False
        return out

    qr_col  = qr_col  or ("QR_qrCode" if "QR_qrCode" in df.columns else settings.USER_KEY_COLUMN)
    # 👇 prefer original columns if available
    lat_col = lat_col or getattr(settings, "ORIG_LAT_COL", None) or settings.LAT_COL
    lon_col = lon_col or getattr(settings, "ORIG_LON_COL", None) or settings.LON_COL

    df2 = df.copy()

    # ---- Reverse geocode (skip invalid + sentinel defaults) ----
    lat_f, lon_f, mask_valid = _clean_coords(df2, lat_col, lon_col)
    actual_cc = [None] * len(df2)

    if mask_valid.any():
        coords = list(zip(lat_f[mask_valid].to_numpy(), lon_f[mask_valid].to_numpy()))
        idxs   = np.flatnonzero(mask_valid.values)
        # chunk to be safe on big inputs
        CHUNK = 5000
        for i in range(0, len(coords), CHUNK):
            chunk = coords[i:i+CHUNK]
            chunk_idx = idxs[i:i+CHUNK]
            try:
                res = rg.search(chunk, mode=1)
                for ridx, r in zip(chunk_idx, res):
                    actual_cc[ridx] = (r.get("cc") or None)
            except Exception:
                # Ultra defensive fallback
                for (lt, ln), ridx in zip(chunk, chunk_idx):
                    try:
                        r1 = rg.search([(lt, ln)], mode=1)
                        actual_cc[ridx] = (r1[0].get("cc") or None)
                    except Exception:
                        actual_cc[ridx] = None

    df2["actual_cc"] = actual_cc

    # ---- Planned countries by QR (always a set) ----
    qr_to_planned = load_qr_to_planned(settings.PLANNED_XLSX)  # {qr: set('DK','SE',...)}
    def _planned_set(q):
        if q is None:
            return set()
        s = str(q).strip()
        return qr_to_planned.get(s, set())

    planned_sets = df2[qr_col].map(_planned_set)
    # ensure dtype=object and each element is a real Python set
    planned_sets = planned_sets.apply(lambda v: v if isinstance(v, set) else set())

    df2["planned_iso2_set"] = planned_sets
    df2["planned_iso2"] = planned_sets.apply(lambda s: ",".join(sorted(s)) if s else "")

    # ---- planned_match (pure boolean Series) ----
    has_planned = df2["planned_iso2_set"].apply(lambda s: bool(s))
    has_actual  = df2["actual_cc"].notna()

    # Start false
    df2["planned_match"] = False
    # Evaluate only where both sides exist; use a row-wise boolean, but the result is a Series
    mask_eval = has_planned & has_actual
    df2.loc[mask_eval, "planned_match"] = df2.loc[mask_eval].apply(
        lambda r: r["actual_cc"] in r["planned_iso2_set"],
        axis=1
    ).astype(bool)    
    return df2
