import math, re, pandas as pd
from ..config import settings

LAT_CANDIDATES = ["lat","latitude","y","gps_lat","gps_latitude","geom_lat","geo_lat","gps_lat_deg","latitude_deg"]
LON_CANDIDATES = ["lon","lng","longitude","x","gps_lon","gps_longitude","geom_lon","geo_lon","long","gps_long","longitude_deg"]

def pick_lat_lon_cols(columns):
    cols_lower = {c.lower(): c for c in columns}
    if settings.LAT_COL.lower() in cols_lower and settings.LON_COL.lower() in cols_lower:
        return cols_lower[settings.LAT_COL.lower()], cols_lower[settings.LON_COL.lower()]
    lat = next((cols_lower[c] for c in cols_lower if c in LAT_CANDIDATES), None)
    lon = next((cols_lower[c] for c in cols_lower if c in LON_CANDIDATES), None)
    return lat, lon

def _parse_coord(value, kind: str):
    s = str(value).strip() if value is not None else ""
    if not s: return None
    s = s.replace("\u2212","-").replace(",", ".")
    m = re.match(r'^\s*([+-]?\d+(?:\.\d+)?)\s*([NnSsEeWw])?\s*$', s)
    if m:
        num = float(m.group(1)); hemi = (m.group(2) or "").upper()
        if hemi in ("S","W"): num = -abs(num)
        if hemi in ("N","E"): num =  abs(num)
        val = num
    else:
        try: val = float(s)
        except Exception: return None
    if kind == "lat" and not (-90 <= val <= 90): return None
    if kind == "lon" and not (-180 <= val <= 180): return None
    return val

def _hash_to_unit_floats(key: str, n: int = 2):
    import hashlib
    h = hashlib.sha256(key.encode("utf-8")).digest()
    vals = []
    for i in range(n):
        chunk = h[i*8:(i+1)*8]
        ui = int.from_bytes(chunk, "big", signed=False)
        vals.append((ui % (10**12)) / (10**12))
    return vals

def deterministic_jitter(lat: float, lon: float, key: str, max_dist_m: float = None):
    if max_dist_m is None: max_dist_m = settings.MAX_JITTER_METERS
    r1, r2 = _hash_to_unit_floats(f"{key}|{settings.JITTER_SALT}")
    theta = 2 * math.pi * r1
    d = max_dist_m * math.sqrt(r2)
    m_per_deg_lat = 111_000.0
    cos_lat = max(0.01, math.cos(math.radians(lat)))
    m_per_deg_lon = m_per_deg_lat * cos_lat
    d_lat = (d * math.cos(theta)) / m_per_deg_lat
    d_lon = (d * math.sin(theta)) / m_per_deg_lon
    j_lat, j_lon = lat + d_lat, lon + d_lon
    if j_lon > 180: j_lon -= 360
    if j_lon < -180: j_lon += 360
    j_lat = max(min(j_lat, 90), -90)
    return j_lat, j_lon

def df_to_geojson(df: pd.DataFrame):
    if df is None or df.empty:
        return {"type":"FeatureCollection","features":[]}
    lat_col, lon_col = pick_lat_lon_cols(df.columns)
    if not lat_col or not lon_col:
        return {"type":"FeatureCollection","features":[]}

    KEY_FIELDS = [
        "sampleId","collectedAt","QR_qrCode","PH_ph","SOIL_COLOR_color",
        "SOIL_TEXTURE_texture","SOIL_STRUCTURE_structure","SOIL_DIVER_earthworms",
        "SOIL_CONTAMINATION_plastic","SOIL_CONTAMINATION_debris","SOIL_CONTAMINATION_comments",
        "METALS_info",
    ]
    photo_cols = [c for c in df.columns if c.startswith("PHOTO_")]
    seen, fields = set(), []
    for c in KEY_FIELDS + photo_cols:
        if c not in seen:
            fields.append(c); seen.add(c)

    feats = []
    for _, row in df.iterrows():
        try:
            lat = _parse_coord(row.get(lat_col), "lat")
            lon = _parse_coord(row.get(lon_col), "lon")
            if lat is None or lon is None: continue
            if not (math.isfinite(lat) and math.isfinite(lon)): continue
        except Exception:
            continue
        props = {}
        for k in fields:
            if k in df.columns:
                props[k] = row.get(k)
        for k in ("email","userId"):
            if k in df.columns:
                props[k] = row.get(k)
        feats.append({"type":"Feature","geometry":{"type":"Point","coordinates":[lon,lat]},"properties":props})
    return {"type":"FeatureCollection","features":feats}
