# echorepo/services/validation.py

import numpy as np
import pandas as pd
import shapefile
from shapely.geometry import Point
from shapely.geometry import shape as shp_shape

from ..config import settings
from .planned import load_qr_to_planned

# --- helpers ---------------------------------------------------------------
_COUNTRY_SHAPES = None


def _load_country_shapes():
    global _COUNTRY_SHAPES
    if _COUNTRY_SHAPES is not None:
        return _COUNTRY_SHAPES

    shp_path = getattr(
        settings,
        "COUNTRY_SHP_PATH",
        "/data/ne_50m_admin_0_countries/ne_50m_admin_0_countries.shp",
    )

    r = shapefile.Reader(shp_path)
    field_names = [f[0] for f in r.fields[1:]]
    iso_idx = field_names.index("ISO_A2") if "ISO_A2" in field_names else None

    shapes = {}
    for sr in r.shapeRecords():
        geom = shp_shape(sr.shape.__geo_interface__)
        rec = sr.record
        iso2 = rec[iso_idx] if iso_idx is not None else ""
        if iso2 and iso2 != "-99":
            shapes[iso2] = geom

    _COUNTRY_SHAPES = shapes
    return _COUNTRY_SHAPES


def _point_country(lat: float, lon: float) -> str | None:
    shapes = _load_country_shapes()
    pt = Point(lon, lat)
    for iso2, geom in shapes.items():
        if geom.covers(pt):
            return iso2
    return None


def _km_to_deg_lat(km: float) -> float:
    return km / 111.32


def _within_planned_country_tolerance(
    lat: float, lon: float, planned_set: set[str], km: float
) -> bool:
    if not planned_set:
        return False

    shapes = _load_country_shapes()
    pt = Point(lon, lat)
    tol_deg = _km_to_deg_lat(km)

    for cc in planned_set:
        geom = shapes.get(cc)
        if geom is None:
            continue
        if geom.covers(pt):
            return True
        if geom.buffer(tol_deg).covers(pt):
            return True
    return False


def _clean_coords(df: pd.DataFrame, lat_col: str, lon_col: str):
    """Return (lat_f, lon_f, valid_mask) with floats and world-bounds mask."""
    lat_s = df[lat_col].astype(str).str.replace(",", ".", regex=False).str.strip()
    lon_s = df[lon_col].astype(str).str.replace(",", ".", regex=False).str.strip()
    lat_f = pd.to_numeric(lat_s, errors="coerce")
    lon_f = pd.to_numeric(lon_s, errors="coerce")
    mask = (
        np.isfinite(lat_f)
        & np.isfinite(lon_f)
        & (lat_f.between(-90.0, 90.0))
        & (lon_f.between(-180.0, 180.0))
    )
    # Exclude your app's default/sentinel coords
    bad_default = (lat_f == settings.DEFAULT_COORD_LAT) & (lon_f == settings.DEFAULT_COORD_LON)
    mask = mask & (~bad_default)
    return lat_f, lon_f, mask


# --- public API ------------------------------------------------------------


def find_default_coord_rows(
    df: pd.DataFrame, lat_col: str | None = None, lon_col: str | None = None
) -> pd.DataFrame:
    """
    Rows that have the sentinel default coordinates (e.g., 46.5, 11.35).
    """
    if df is None or df.empty:
        return df.iloc[0:0].copy()

    # 👇 prefer original columns if available
    lat_col = lat_col or getattr(settings, "ORIG_LAT_COL", None) or settings.LAT_COL
    lon_col = lon_col or getattr(settings, "ORIG_LON_COL", None) or settings.LON_COL

    lat_s = df[lat_col].astype(str).str.replace(",", ".", regex=False).str.strip()
    lon_s = df[lon_col].astype(str).str.replace(",", ".", regex=False).str.strip()
    lat_f = pd.to_numeric(lat_s, errors="coerce")
    lon_f = pd.to_numeric(lon_s, errors="coerce")

    mask_default = (lat_f == settings.DEFAULT_COORD_LAT) & (lon_f == settings.DEFAULT_COORD_LON)
    return df.loc[mask_default].copy()


def select_country_mismatches(
    df: pd.DataFrame,
    qr_col: str | None = None,
    lat_col: str | None = None,
    lon_col: str | None = None,
) -> pd.DataFrame:
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
    has_actual = ann["actual_cc"].notna()
    mism_mask = has_planned & has_actual & (~ann["planned_match"])
    return ann.loc[mism_mask].copy()


def annotate_country_mismatches(
    df: pd.DataFrame,
    qr_col: str | None = None,
    lat_col: str | None = None,
    lon_col: str | None = None,
) -> pd.DataFrame:
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

    qr_col = qr_col or ("QR_qrCode" if "QR_qrCode" in df.columns else settings.USER_KEY_COLUMN)
    # 👇 prefer original columns if available
    lat_col = lat_col or getattr(settings, "ORIG_LAT_COL", None) or settings.LAT_COL
    lon_col = lon_col or getattr(settings, "ORIG_LON_COL", None) or settings.LON_COL

    df2 = df.copy()

    # ---- Reverse geocode (skip invalid + sentinel defaults) ----
    lat_f, lon_f, mask_valid = _clean_coords(df2, lat_col, lon_col)
    actual_cc = [None] * len(df2)

    if mask_valid.any():
        idxs = np.flatnonzero(mask_valid.values)
        for ridx in idxs:
            lt = float(lat_f.iloc[ridx])
            ln = float(lon_f.iloc[ridx])
            actual_cc[ridx] = _point_country(lt, ln)

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
    has_actual = df2["actual_cc"].notna()

    # Start false
    df2["planned_match"] = False

    # Evaluate only where both sides exist; use a row-wise boolean, but the result is a Series
    mask_eval = has_planned & has_actual

    BORDER_TOLERANCE_KM = getattr(settings, "COUNTRY_BORDER_TOLERANCE_KM", 10.0)

    def _row_match(r):
        if not r["planned_iso2_set"]:
            return False
        if pd.isna(r["actual_cc"]):
            return False

        if r["actual_cc"] in r["planned_iso2_set"]:
            return True

        try:
            lt = float(str(r[lat_col]).replace(",", "."))
            ln = float(str(r[lon_col]).replace(",", "."))
        except Exception:
            return False

        return _within_planned_country_tolerance(
            lt,
            ln,
            r["planned_iso2_set"],
            BORDER_TOLERANCE_KM,
        )

    df2.loc[mask_eval, "planned_match"] = df2.loc[mask_eval].apply(_row_match, axis=1).astype(bool)
    return df2
