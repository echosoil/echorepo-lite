from flask import Blueprint, render_template, request, redirect, url_for, flash, session
from ..auth.decorators import login_required
from ..services.db import query_user_df, query_sample, update_coords_sqlite
from ..services.validation import find_default_coord_rows, annotate_country_mismatches, select_country_mismatches
from ..services.firebase import update_coords_by_user_sample
from ..utils.coords import parse_coord
from ..config import settings
import math, os
from functools import lru_cache


errors_bp = Blueprint("errors", __name__)


DEFAULT_LAT = float(os.getenv("DEFAULT_LAT", "46.5"))
DEFAULT_LON = float(os.getenv("DEFAULT_LON", "11.35"))
COUNTRY_SHP = os.getenv("COUNTRY_SHP", "/data/ne_50m_admin_0_countries/ne_50m_admin_0_countries.shp")  # e.g. /app/data/ne_110m_admin_0_countries.shp

def _to_float(x):
    try:
        if x is None: return None
        s = str(x).strip().replace(",", ".")
        return float(s)
    except Exception:
        return None

def _haversine_km(lat1, lon1, lat2, lon2):
    try:
        R = 6371.0088
        phi1, phi2 = math.radians(lat1), math.radians(lat2)
        dphi = math.radians(lat2 - lat1)
        dl = math.radians(lon2 - lon1)
        a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dl/2)**2
        return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    except Exception:
        return None

@lru_cache(maxsize=1)
def _country_index():
    """
    Lazy-load a simple country index from a Natural Earth shapefile.
    Optional; returns (reader, records) or (None, None) if unavailable.
    """
    if not COUNTRY_SHP or not os.path.exists(COUNTRY_SHP):
        return (None, None)
    try:
        import shapefile  # pyshp
        from shapely.geometry import shape as shp_shape, Point
        sf = shapefile.Reader(COUNTRY_SHP)
        shapes = sf.shapes()
        records = sf.records()
        # find field names safely
        fields = [f[0].lower() for f in sf.fields[1:]]
        try:
            iso2_idx = fields.index("iso_a2")
        except ValueError:
            iso2_idx = None
        try:
            name_idx = fields.index("name")
        except ValueError:
            name_idx = None

        items = []
        for shp, rec in zip(shapes, records):
            geom = shp_shape(shp.__geo_interface__)
            iso2 = (rec[iso2_idx] if iso2_idx is not None else "") or ""
            name = (rec[name_idx] if name_idx is not None else "") or ""
            items.append((geom, iso2, name))
        return ("ok", items)
    except Exception:
        return (None, None)

def _country_from_coords(lat, lon):
    """
    Returns dict with iso2 and name if shapefile is available, else None.
    """
    status, items = _country_index()
    if status != "ok":
        print("[ERROR][COORDS] country shapefile not available")
        return None
    try:
        from shapely.geometry import Point
        pt = Point(float(lon), float(lat))
        for geom, iso2, name in items:
            if geom.contains(pt):
                return {"iso2": iso2, "name": name}
        # try boundary-touch case
        for geom, iso2, name in items:
            if geom.touches(pt):
                return {"iso2": iso2, "name": name, "touches": True}
    except Exception as e:
        print("[ERROR][COORDS] country lookup error:", e)
    return None

@errors_bp.get("/issues")
@login_required
def issues():
    user_key = session.get("user")
    df = query_user_df(user_key)
    defaults = find_default_coord_rows(df)
    mism = select_country_mismatches(df)

    orig_lat_col = getattr(settings, "ORIG_LAT_COL", None) or settings.LAT_COL
    orig_lon_col = getattr(settings, "ORIG_LON_COL", None) or settings.LON_COL

    # minimal columns for the view
    cols = ["sampleId","userId","QR_qrCode", orig_lat_col, orig_lon_col]
    def pick(d):
        present = [c for c in cols if c in d.columns]
        return d[present].copy()

    return render_template(
        "issues.html",
        default_rows=pick(defaults),
        mismatch_rows=pick(mism),
        orig_lat_col=orig_lat_col,
        orig_lon_col=orig_lon_col,      
    )

@errors_bp.post("/issues/fix-coords")
@login_required
def fix_coords():
    """
    Expect form fields:
      sampleId, userId, lat_input, lon_input
    accepts decimal or DMS
    """
    sample_id = (request.form.get("sampleId") or "").strip()
    user_id   = (request.form.get("userId") or "").strip()
    lat_s     = (request.form.get("lat_input") or "").strip()
    lon_s     = (request.form.get("lon_input") or "").strip()

    lat = parse_coord(lat_s, is_lon=False)
    lon = parse_coord(lon_s, is_lon=True)
    if lat is None or lon is None:
        flash("Could not parse coordinates. Use decimal or DMS like 43°03'24.7\" N, 12°45'22.0\" E.", "danger")
        return redirect(url_for("errors.issues"))

    # 1) Firestore
    ok, info = update_coords_by_user_sample(user_id, sample_id, lat, lon)
    if not ok:
        flash(f"Firestore update failed: {info}", "danger")
        return redirect(url_for("errors.issues"))

    # 2) SQLite (so the page reflects the fix immediately)
    update_coords_sqlite(sample_id, lat, lon)

    flash("Coordinates updated.", "success")
    return redirect(url_for("errors.issues"))

@errors_bp.get("/issues/why")
@login_required
def why():
    """
    Diagnostics for *any* sampleId (not scoped to the logged-in user).
    Example: /errors/why?sampleId=ABC123
    Optional: DEFAULT_LAT/DEFAULT_LON/COUNTRY_SHP envs (see helpers above).
    """
    sample_id = (request.args.get("sampleId") or "").strip()
    if not sample_id:
        return {"error": "sampleId is required"}, 400

    # <-- GLOBAL lookup (not per-user)
    df = query_sample(sample_id)
    if df is None or df.empty:
        return {"error": f"sampleId '{sample_id}' not found"}, 404
    import pandas as pd
    pd.set_option("display.max_columns", None)   # show all columns
    pd.set_option("display.width", None)        # don't wrap to the next line

    # Work with the first row (there should normally be 1 per sampleId)
    r = df.iloc[0].to_dict()

    lat_raw = r.get(settings.LAT_COL)
    lon_raw = r.get(settings.LON_COL)
    lat = _to_float(lat_raw)
    lon = _to_float(lon_raw)

    # Re-run validators on JUST this row, to see how it’s classified
    one = df.iloc[[0]].copy()

    # Enriched (all rows, but it's a single-row df here)
    ann = annotate_country_mismatches(one)
    row = ann.iloc[0].to_dict()

    in_default  = not find_default_coord_rows(one).empty

    # True mismatch only when both sides exist AND not matching
    has_planned = bool(row.get("planned_iso2_set"))
    has_actual  = row.get("actual_cc") is not None
    in_mismatch = has_planned and has_actual and (not bool(row.get("planned_match")))

    coord_checks = {
        "lat_raw": lat_raw, 
        "lon_raw": lon_raw, 
        "lat_float": lat, 
        "lon_float": lon, 
        "missing": (lat is None or lon is None), 
        "non_numeric": (lat is None or lon is None) and (bool(lat_raw) or bool(lon_raw)), 
        "out_of_bounds": (lat is not None and lon is not None) and not (-90 <= lat <= 90 and -180 <= lon <= 180), 
        "equals_default_exact": ( lat is not None and lon is not None and lat == DEFAULT_LAT and lon == DEFAULT_LON ), 
        "distance_to_default_km": ( None if (lat is None or lon is None) else _haversine_km(lat, lon, DEFAULT_LAT, DEFAULT_LON) ), } 
        
    actual_country = None 
    if lat is not None and lon is not None: 
        actual_country = _country_from_coords(lat, lon) # optional if COUNTRY_SHP set declared = mism.iloc[0].get("planned_iso2") if not mism.empty else None
    declared = ann.iloc[0].get("planned_iso2") if not ann.empty else None

    return {
        "sampleId": sample_id,
        "userId": r.get("userId"),
        "qr": r.get("QR_qrCode"),
        "flags": {
            "listed_as_default_coord": bool(in_default),
            "listed_as_country_mismatch": bool(in_mismatch),
        },
        "coord_checks": coord_checks,
        "declared_country_field": row.get("planned_iso2"),
        "actual_country_from_coords": row.get("actual_cc"),
        "notes": "This endpoint inspects a single row. If it still appears in /issues, compare these checks.",
    }, 200
