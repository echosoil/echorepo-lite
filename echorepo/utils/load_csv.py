import os
import sqlite3
import math
import hashlib
import pandas as pd
import tempfile  # <-- added
import re 

# ---- Config (env) ----
CSV_PATH    = "/home/quanta/echorepo-lite/data/echorepo_samples_with_email.csv"
SQLITE_PATH = "/home/quanta/echorepo-lite/data/db/echo.db"
TABLE_NAME  = os.getenv("TABLE_NAME", "samples")

# Max jitter distance in meters (match the map legend/toggle)
MAX_JITTER_METERS = float(os.getenv("MAX_JITTER_METERS", "1000"))

# Secret salt for deterministic jitter (set a long random string in production)
JITTER_SALT = os.getenv("JITTER_SALT", "change-this-salt")

# Overwrite original GPS values. If you want to keep originals renamed, set KEEP_ORIGINALS=true
KEEP_ORIGINALS = os.getenv("KEEP_ORIGINALS", "false").lower() in ("1", "true", "yes")
print("KEEP_ORIGINALS=", KEEP_ORIGINALS)

# Prefer explicit column names, but also try candidates (case-insensitive)
PREFERRED_LAT = os.getenv("LAT_COL", "GPS_lat")
PREFERRED_LON = os.getenv("LON_COL", "GPS_long")

LAT_CANDIDATES = [
    "lat", "latitude", "y",
    "gps_lat", "gps_latitude", "geom_lat", "geo_lat",
    "gps_lat_deg", "latitude_deg"
]
LON_CANDIDATES = [
    "lon", "lng", "longitude", "x",
    "gps_lon", "gps_longitude", "geom_lon", "geo_lon",
    "long", "gps_long", "longitude_deg"
]

# Columns to try (in order) as a stable key for the jitter
STABLE_KEY_PREFS = ["sampleId", "QR_qrCode", "userId", "email"]

# Force rebuild even if nothing changed
FORCE_REBUILD = os.getenv("ECHO_FORCE_REBUILD", "false").lower() in ("1", "true", "yes")

# Where we store the last build signature
SIG_PATH = SQLITE_PATH + ".sig"

# Script/version signature (bump when you change logic)
SCRIPT_VERSION = "jittered-load-v1.0.0"


# ---- Helpers ----
def _parse_coord(value, kind: str):
    """
    Parse a latitude/longitude from messy strings:
    - handles unicode minus (−), decimal comma, whitespace
    - handles hemisphere suffixes N/S/E/W (W,S force negative)
    - clamps to valid ranges; returns None if invalid
    kind: "lat" or "lon"
    """
    if value is None:
        return None
    s = str(value).strip()
    if s == "":
        return None

    # normalize unicode minus and decimal comma
    s = s.replace("\u2212", "-").replace(",", ".")

    # try: number with optional hemisphere
    m = re.match(r'^\s*([+-]?\d+(?:\.\d+)?)\s*([NnSsEeWw])?\s*$', s)
    if m:
        num = float(m.group(1))
        hemi = (m.group(2) or "").upper()
        if hemi in ("S", "W"):
            num = -abs(num)
        elif hemi in ("N", "E"):
            num = abs(num)
        val = num
    else:
        # last resort: plain float()
        try:
            val = float(s)
        except Exception:
            return None

    # clamp
    if kind == "lat":
        return val if -90.0 <= val <= 90.0 else None
    if kind == "lon":
        return val if -180.0 <= val <= 180.0 else None
    return None

def _pick_lat_lon_cols(columns):
    """Find lat/lon columns in a case-insensitive way."""
    cols_lower = {c.lower(): c for c in columns}
    if PREFERRED_LAT.lower() in cols_lower and PREFERRED_LON.lower() in cols_lower:
        return cols_lower[PREFERRED_LAT.lower()], cols_lower[PREFERRED_LON.lower()]
    lat = next((cols_lower[c] for c in LAT_CANDIDATES if c in cols_lower), None)
    lon = next((cols_lower[c] for c in LON_CANDIDATES if c in cols_lower), None)
    return lat, lon

def _hash_file_sha256(path, chunk=1024 * 1024):
    """SHA-256 of a file (streamed)."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            b = f.read(chunk)
            if not b:
                break
            h.update(b)
    return h.hexdigest()

def _config_signature():
    """A small string capturing all settings that affect the DB output."""
    parts = [
        SCRIPT_VERSION,
        f"MAX_JITTER_METERS={MAX_JITTER_METERS}",
        f"KEEP_ORIGINALS={KEEP_ORIGINALS}",
        f"PREFERRED_LAT={PREFERRED_LAT}",
        f"PREFERRED_LON={PREFERRED_LON}",
        # include only a hash of the salt, not the salt itself
        f"JITTER_SALT_HASH={hashlib.sha256(JITTER_SALT.encode('utf-8')).hexdigest()}",
        f"TABLE_NAME={TABLE_NAME}",
    ]
    return "|".join(parts)

def _load_last_signature(path=SIG_PATH):
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return f.read().strip()
    except Exception:
        return None

def _save_signature(sig, path=SIG_PATH):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(sig)

def _compose_signature(csv_hash):
    """Combine CSV hash + config into one signature string/hash."""
    h = hashlib.sha256()
    h.update(csv_hash.encode("utf-8"))
    h.update(b"|")
    h.update(_config_signature().encode("utf-8"))
    return h.hexdigest()

def _table_exists(conn, table_name):
    cur = conn.execute(
        "SELECT count(*) FROM sqlite_master WHERE type='table' AND name=?;",
        (table_name,),
    )
    return cur.fetchone()[0] == 1

def _hash_to_unit_floats(key: str, n: int = 2):
    """Deterministically map a key -> n floats in [0,1)."""
    h = hashlib.sha256(key.encode("utf-8")).digest()
    vals = []
    for i in range(n):
        chunk = h[i*8:(i+1)*8]
        ui = int.from_bytes(chunk, "big", signed=False)
        vals.append((ui % (10**12)) / (10**12))
    return vals

def deterministic_jitter(lat: float, lon: float, key: str, max_dist_m: float = MAX_JITTER_METERS):
    """
    Stable random displacement within a circle of radius max_dist_m,
    based on key + secret salt. Returns (jittered_lat, jittered_lon).
    """
    r1, r2 = _hash_to_unit_floats(f"{key}|{JITTER_SALT}")
    theta = 2 * math.pi * r1
    d = max_dist_m * math.sqrt(r2)

    # meters -> degrees
    m_per_deg_lat = 111_000.0
    cos_lat = max(0.01, math.cos(math.radians(lat)))  # protect near poles
    m_per_deg_lon = m_per_deg_lat * cos_lat

    d_lat = (d * math.cos(theta)) / m_per_deg_lat
    d_lon = (d * math.sin(theta)) / m_per_deg_lon

    j_lat = lat + d_lat
    j_lon = lon + d_lon

    if j_lon > 180: j_lon -= 360
    if j_lon < -180: j_lon += 360
    j_lat = max(min(j_lat, 90), -90)
    return j_lat, j_lon

def _choose_stable_key(row: pd.Series) -> str:
    for k in STABLE_KEY_PREFS:
        v = row.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return str(row.name)


# ---- Main build ----
def ensure_sqlite():
    os.makedirs(os.path.dirname(SQLITE_PATH), exist_ok=True)
    if not os.path.exists(CSV_PATH):
        raise FileNotFoundError(f"CSV not found at {CSV_PATH}")

    # Compute the current signature
    csv_hash = _hash_file_sha256(CSV_PATH)
    current_sig = _compose_signature(csv_hash)
    last_sig = _load_last_signature()

    # Skip rebuild if signature matches and table exists (unless forced)
    if not FORCE_REBUILD and last_sig == current_sig and os.path.exists(SQLITE_PATH):
        try:
            with sqlite3.connect(SQLITE_PATH) as conn:
                if _table_exists(conn, TABLE_NAME):
                    print(f"[load_csv] No changes detected (signature match). Keeping existing {SQLITE_PATH}:{TABLE_NAME}.")
                    return
        except Exception:
            pass  # If any error checking table, we’ll rebuild below.

    # Load CSV as text to avoid undesired type coercion on IDs
    df = pd.read_csv(CSV_PATH, dtype=str, keep_default_na=False)

    # Identify LAT/LON columns
    lat_col, lon_col = _pick_lat_lon_cols(df.columns)
    if not lat_col or not lon_col:
        raise RuntimeError(
            f"Could not detect latitude/longitude columns. "
            f"Tried preferred ({PREFERRED_LAT}, {PREFERRED_LON}) and common candidates."
        )

    # Optionally keep originals by renaming them (create new columns with original values)
    if KEEP_ORIGINALS:
        orig_lat_col = f"{lat_col}_orig"
        orig_lon_col = f"{lon_col}_orig"
        if orig_lat_col not in df.columns:
            df[orig_lat_col] = df[lat_col]
        if orig_lon_col not in df.columns:
            df[orig_lon_col] = df[lon_col]

    # Apply deterministic jitter to every valid row; overwrite lat/lon
    def _jitter_row(row: pd.Series):
        try:
            lat_raw = row[lat_col]
            lon_raw = row[lon_col]
            lat = _parse_coord(lat_raw, "lat")
            lon = _parse_coord(lon_raw, "lon")
            if lat is None or lon is None:
                return row
        except Exception:
            return row  # leave untouched if cannot parse

        if not (math.isfinite(lat) and math.isfinite(lon)):
            return row

        key = _choose_stable_key(row)
        jlat, jlon = deterministic_jitter(lat, lon, key, MAX_JITTER_METERS)
        row[lat_col] = f"{jlat:.8f}"
        row[lon_col] = f"{jlon:.8f}"
        return row

        key = _choose_stable_key(row)
        jlat, jlon = deterministic_jitter(lat, lon, key, MAX_JITTER_METERS)
        row[lat_col] = f"{jlat:.8f}"
        row[lon_col] = f"{jlon:.8f}"
        return row

    df = df.apply(_jitter_row, axis=1)

    # --- Atomic write: build into a temp DB, then swap into place ---
    dirpath = os.path.dirname(SQLITE_PATH) or "."
    with tempfile.NamedTemporaryFile(prefix="echo_db_", suffix=".tmp", dir=dirpath, delete=False) as tmp:
        tmp_db = tmp.name

    try:
        conn = sqlite3.connect(tmp_db)
        # Faster bulk load
        conn.execute("PRAGMA journal_mode=OFF;")
        conn.execute("PRAGMA synchronous=OFF;")

        df.to_sql(TABLE_NAME, conn, if_exists="replace", index=False)

        # Helpful indexes for your app queries
        cur = conn.cursor()
        for idx_sql in [
            f"CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_email   ON {TABLE_NAME}(email);",
            f"CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_userId  ON {TABLE_NAME}(userId);",
            f"CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_qr      ON {TABLE_NAME}(QR_qrCode);",
            f"CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_sample  ON {TABLE_NAME}(sampleId);",
            f"CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_date    ON {TABLE_NAME}(collectedAt);",
        ]:
            try:
                cur.execute(idx_sql)
            except Exception:
                pass

        conn.commit()
        conn.close()

        # Atomic replace so readers never see a half-written DB
        os.replace(tmp_db, SQLITE_PATH)

        # Save signature for next time (only after successful swap)
        _save_signature(current_sig)

        print(f"[load_csv] Loaded {len(df)} jittered rows from {CSV_PATH} into {SQLITE_PATH}:{TABLE_NAME}")
        if KEEP_ORIGINALS:
            print(f"[load_csv] Original coordinates preserved in columns: {lat_col}_orig, {lon_col}_orig")
        else:
            print("[load_csv] Original coordinates overwritten (not preserved in DB).")

    except Exception:
        # Clean up temp file on failure
        try:
            os.remove(tmp_db)
        except Exception:
            pass
        raise


if __name__ == "__main__":
    ensure_sqlite()
