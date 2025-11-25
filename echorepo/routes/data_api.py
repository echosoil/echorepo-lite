# echorepo/routes/data_api.py
from __future__ import annotations

import csv, io, os, re, sqlite3, math, time, json
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple

import requests           # pip install requests
import jwt                # pip install PyJWT
from flask import Blueprint, current_app, request, jsonify, Response, abort, g

from psycopg2.extras import RealDictCursor          
from echorepo.services.db import get_pg_conn        
import zipfile 

data_api = Blueprint("data_api", __name__)

# -----------------------------------------------------------------------------  
# Canonical table schemas (Postgres)  
# -----------------------------------------------------------------------------  

CANONICAL_SAMPLE_COLS = [
    "sample_id",
    "timestamp_utc",
    "lat",
    "lon",
    "country_code",
    "location_accuracy_m",
    "ph",
    "organic_carbon_pct",
    "earthworms_count",
    "contamination_debris",
    "contamination_plastic",
    "contamination_other_orig",
    "contamination_other_en",
    "pollutants_count",
    "soil_structure_orig",
    "soil_structure_en",
    "soil_texture_orig",
    "soil_texture_en",
    "observations_orig",
    "observations_en",
    "metals_info_orig",
    "metals_info_en",
    "collected_by",
    "data_source",
    "qa_status",
    "licence",
]

CANONICAL_IMAGE_COLS = [
    "sample_id",
    "country_code",
    "image_id",
    "image_url",
    "image_description_orig",
    "image_description_en",
    "collected_by",
    "timestamp_utc",
    "licence",
]

CANONICAL_PARAM_COLS = [
    "sample_id",
    "country_code",
    "parameter_code",
    "parameter_name",
    "value",
    "uom",
    "analysis_method",
    "analysis_date",
    "lab_id",
    "created_by",
    "licence",
    "parameter_uri",
]


# -----------------------------------------------------------------------------
# Config helpers
# -----------------------------------------------------------------------------

def get_db_path() -> str:
    # Prefer Flask config, then ENV, then a sane default (your repo layout)
    return (
        current_app.config.get("SQLITE_PATH")
        or os.environ.get("SQLITE_PATH")
        or os.path.join(current_app.root_path, "..", "..", "data", "db", "data.db")
    )

def quote_ident(name: str) -> str:
    # Minimal identifier quoting for SQLite
    return '"' + name.replace('"', '""') + '"'

def get_sample_table(conn: sqlite3.Connection) -> str:
    """
    Find the samples table. Use SAMPLE_TABLE if provided; else pick common names
    or any table that has a 'sampleId' column as a heuristic.
    """
    explicit = current_app.config.get("SAMPLE_TABLE") or os.environ.get("SAMPLE_TABLE")
    if explicit:
        return explicit

    cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = [r[0] for r in cur.fetchall()]

    for candidate in ("samples", "sample", "data", "records"):
        if candidate in tables:
            return candidate

    for t in tables:
        try:
            c = conn.execute(f"PRAGMA table_info({quote_ident(t)})")
            cols = {row[1] for row in c.fetchall()}
            if "sampleId" in cols:
                return t
        except Exception:
            continue

    if tables:
        return tables[0]
    raise RuntimeError("No tables found in SQLite database")

# -----------------------------------------------------------------------------
# OIDC (Keycloak) helpers
# -----------------------------------------------------------------------------

def oidc_cfg():
    """Cache OIDC well-known + JWKS for 5 minutes."""
    cfg = getattr(g, "_oidc_cfg", None)
    if cfg and cfg["exp"] > time.time():
        return cfg

    issuer = current_app.config.get("OIDC_ISSUER_URL") or os.environ.get("OIDC_ISSUER_URL")
    if not issuer:
        g._oidc_cfg = {"exp": time.time() + 300, "enabled": False}
        return g._oidc_cfg

    well = requests.get(issuer.rstrip("/") + "/.well-known/openid-configuration", timeout=5).json()
    jwks = requests.get(well["jwks_uri"], timeout=5).json()
    g._oidc_cfg = {
        "enabled": True,
        "issuer": well["issuer"],
        "jwks": jwks,
        "aud": current_app.config.get("OIDC_AUDIENCE") or os.environ.get("OIDC_AUDIENCE"),
        "client_id": current_app.config.get("OIDC_CLIENT_ID") or os.environ.get("OIDC_CLIENT_ID"),
        "exp": time.time() + 300,
    }
    return g._oidc_cfg

def verify_bearer():
    """Return decoded claims if Authorization: Bearer <JWT> is valid; else None."""
    auth = request.headers.get("Authorization", "")
    if not auth.startswith("Bearer "):
        return None
    token = auth.split(" ", 1)[1].strip()
    cfg = oidc_cfg()
    if not cfg.get("enabled"):
        return None
    try:
        # pick key by kid
        keys = {k.get("kid"): jwt.algorithms.RSAAlgorithm.from_jwk(json.dumps(k))
                for k in cfg["jwks"]["keys"] if k.get("kid")}
        header = jwt.get_unverified_header(token)
        key = keys.get(header.get("kid"))
        if not key:
            return None

        aud = cfg.get("aud")
        opts = {"require": ["exp", "iat"], "verify_aud": bool(aud)}
        claims = jwt.decode(
            token,
            key=key,
            algorithms=["RS256", "PS256", "ES256"],
            audience=aud if aud else None,
            issuer=cfg.get("issuer"),
            options=opts,
        )

        # Secondary audience/client check
        client_id = cfg.get("client_id")
        if client_id:
            aud_claim = claims.get("aud")
            aud_set = set(aud_claim if isinstance(aud_claim, list) else [aud_claim] if aud_claim else [])
            if aud and aud not in aud_set and claims.get("azp") != client_id:
                return None
            if not aud and claims.get("azp") != client_id and client_id not in aud_set:
                return None

        return claims
    except Exception:
        return None

# -----------------------------------------------------------------------------
# DB connection per request
# -----------------------------------------------------------------------------

def get_conn() -> sqlite3.Connection:
    if "sqlite_conn" not in g:
        p = get_db_path()
        conn = sqlite3.connect(p, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        g.sqlite_conn = conn
    return g.sqlite_conn

@data_api.teardown_app_request
def _close_conn(exc):
    conn = g.pop("sqlite_conn", None)
    if conn is not None:
        conn.close()

# -----------------------------------------------------------------------------
# Parsing & helpers
# -----------------------------------------------------------------------------

def parse_iso8601(s: str) -> Optional[str]:
    if not s:
        return None
    s = s.strip()
    for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f"):
        try:
            dt = datetime.strptime(s, fmt)
            return dt.isoformat()
        except ValueError:
            continue
    return s  # fallback—SQLite can still compare ISO-like strings

def parse_bbox(s: str) -> Optional[Tuple[float,float,float,float]]:
    # west,south,east,north  (lon_min, lat_min, lon_max, lat_max)
    if not s:
        return None
    try:
        west, south, east, north = [float(x) for x in s.split(",")]
        return (west, south, east, north)
    except Exception:
        return None

def parse_within(s: str) -> Optional[Tuple[float,float,float]]:
    # lat,lon,r_km
    if not s:
        return None
    try:
        lat, lon, r_km = [float(x) for x in s.split(",")]
        return (lat, lon, r_km)
    except Exception:
        return None

def approx_deg_for_km_lat(km: float) -> float:
    return km / 111.32

def approx_deg_for_km_lon(km: float, at_lat: float) -> float:
    return km / (111.32 * max(0.1, math.cos(math.radians(at_lat))))

# -----------------------------------------------------------------------------
# Field policy (PII + *_state suppression)
# -----------------------------------------------------------------------------

PII_FIELDS = {"email", "userId"}         # always excluded
EXCLUDED_SUFFIXES = ("_state",)          # exclude any column ending with these

# oxide detectors (legacy endpoints only)
OXIDE_TOKEN_RE = re.compile(r"[A-Z][a-z]?\d*")


def _looks_like_oxide(label: str) -> bool:
    """
    Heuristic: a label like 'SiO2', 'Fe2O3', 'K2O', 'CaO'.
    We parse element tokens and require >=2 tokens with one being Oxygen.
    Also ignores anything in parentheses, like units.
    """
    if not label:
        return False
    s = re.sub(r"\(.*?\)", "", str(label))        # strip "(%)", "(mg/kg)", etc.
    tokens = OXIDE_TOKEN_RE.findall(s)            # e.g. ['Si', 'O2'] or ['Fe2', 'O3']
    if len(tokens) < 2:
        return False
    return any(t.startswith("O") for t in tokens) # one token is Oxygen

def _is_oxide_field(name: str) -> bool:
    """
    Consider the whole name and the last token after separators.
    This catches 'SiO2', 'lab_SiO2', 'value_Fe2O3_(%)', etc.
    """
    if not name:
        return False
    if _looks_like_oxide(name):
        return True
    last = re.split(r"[_\s/\-]+", str(name))[-1]
    return _looks_like_oxide(last)


def is_excluded_field(name: str) -> bool:
    if name in PII_FIELDS:
        return True
    if any(name.endswith(suf) for suf in EXCLUDED_SUFFIXES):
        return True
    return _is_oxide_field(name)

DEFAULT_FIELDS = [
    "sampleId", "collectedAt",
    "GPS_long", "GPS_lat",
    "PH_ph",
    "SOIL_TEXTURE_texture",
    "SOIL_STRUCTURE_structure",
    "SOIL_DIVER_earthworms",
    "SOIL_CONTAMINATION_plastic",
]

# -----------------------------------------------------------------------------
# Auth
# -----------------------------------------------------------------------------

def require_api_auth():
    """
    Allow one of:
      - X-API-Key / x-api-key / Authorization: ApiKey <key> / ?api_key=
      - Authorization: Bearer <JWT> (Keycloak)
      - Logged-in Flask session (flask-login)
    """
    required = (current_app.config.get("API_KEY") or os.environ.get("API_KEY") or "").strip()
    if required:
        authz = request.headers.get("Authorization", "")
        given = (
            request.headers.get("X-API-Key")
            or request.headers.get("X-Api-Key")
            or request.headers.get("x-api-key")
            or request.args.get("api_key")
            or (authz.startswith("ApiKey ") and authz[7:])
            or ""
        )
        if given.strip() == required:
            return

    if verify_bearer():
        return

    try:
        from flask_login import current_user
        if getattr(current_user, "is_authenticated", False):
            return
    except Exception:
        pass

    abort(401, description="Missing or invalid credentials")

# -----------------------------------------------------------------------------
# Responses
# -----------------------------------------------------------------------------

def to_geojson(rows: List[sqlite3.Row], lon_col: str, lat_col: str) -> Response:
    feats = []
    for r in rows:
        d = dict(r)
        try:
            lon = float(d.get(lon_col))
            lat = float(d.get(lat_col))
        except Exception:
            lon = lat = None
        if lon is None or lat is None:
            continue
        props = {k: v for k, v in d.items() if k not in (lon_col, lat_col)}
        feats.append({
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [lon, lat]},
            "properties": props,
        })
    return jsonify({"type": "FeatureCollection", "features": feats})

def stream_csv(rows_iter, fields: List[str]) -> Response:
    def generate():
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        yield output.getvalue(); output.seek(0); output.truncate(0)
        for r in rows_iter:
            writer.writerow(dict(r))
            yield output.getvalue(); output.seek(0); output.truncate(0)
    return Response(generate(), mimetype="text/csv")

# -----------------------------------------------------------------------------
# Canonical filters builder (Postgres) – reused by /canonical/* endpoints
# -----------------------------------------------------------------------------

def _canonical_where_from_request(alias: str = "") -> tuple[str, List[Any], Dict[str, Any]]:
    """
    Build a WHERE clause + params for canonical Postgres tables, based on
    query params in the current request.

    Supported filters:
      - from, to           (compared to timestamp_utc)
      - country, country_code
      - bbox               (west,south,east,north) on lon/lat
      - within             (lat,lon,r_km) using approximate degrees

    `alias` (e.g. "s") is used as table alias prefix, so we generate
    's.timestamp_utc' etc.  If alias == "", plain column names are used.
    """
    prefix = f"{alias}." if alias else ""

    where: List[str] = []
    params: List[Any] = []

    # --- time window ---
    raw_from = (request.args.get("from") or "").strip()
    raw_to   = (request.args.get("to") or "").strip()
    from_s   = parse_iso8601(raw_from) if raw_from else None
    to_s     = parse_iso8601(raw_to) if raw_to else None

    if from_s:
        where.append(f"{prefix}timestamp_utc >= %s")
        params.append(from_s)
    if to_s:
        where.append(f"{prefix}timestamp_utc <= %s")
        params.append(to_s)

    # --- country ---
    raw_country = (request.args.get("country") or request.args.get("country_code") or "").strip()
    country_norm = raw_country.upper() if raw_country else None
    if country_norm:
        where.append(f"{prefix}country_code = %s")
        params.append(country_norm)

    # --- bbox ---
    raw_bbox = (request.args.get("bbox") or "").strip()
    bbox = None
    if raw_bbox:
        bbox = parse_bbox(raw_bbox)
        if not bbox:
            abort(400, description=f"Invalid bbox='{raw_bbox}', expected west,south,east,north")

    if bbox:
        west, south, east, north = bbox
        where.append(f"({prefix}lon BETWEEN %s AND %s AND {prefix}lat BETWEEN %s AND %s)")
        params.extend([west, east, south, north])

    # --- within (lat,lon,r_km) ---
    raw_within = (request.args.get("within") or "").strip()
    within = None
    if raw_within:
        within = parse_within(raw_within) if raw_within else None
        if not within:
            abort(400, description=f"Invalid within='{raw_within}', expected lat[e.g. 42],lon[e.g. 2],radius[e.g. 20]")

    if within:
        lat0, lon0, r_km = within
        dlat = approx_deg_for_km_lat(r_km)
        dlon = approx_deg_for_km_lon(r_km, lat0)
        where.append(f"({prefix}lat BETWEEN %s AND %s AND {prefix}lon BETWEEN %s AND %s)")
        params.extend([lat0 - dlat, lat0 + dlat, lon0 - dlon, lon0 + dlon])

    where_sql = "WHERE " + " AND ".join(where) if where else ""
    filter_meta = {
        "from": from_s,
        "to": to_s,
        "country": country_norm,
        "bbox": raw_bbox or None,
        "within": raw_within or None,
    }
    return where_sql, params, filter_meta

# -----------------------------------------------------------------------------
# Endpoints
# -----------------------------------------------------------------------------

@data_api.get("/ping")
def ping():
    return jsonify({"ok": True})

@data_api.get("/samples")
def samples():
    """
    Query params:
      - from, to           (ISO-ish)
      - bbox               (west,south,east,north) — lon/lat
      - within             (lat,lon,r_km)
      - fields             (comma list or "*" — PII and *_state always stripped)
      - limit, offset      (pagination; default 100, max 1000)
      - order, dir         (column, asc|desc; default collectedAt desc)
      - format             (json|csv|geojson; default json)
      - api_key            (or X-API-Key header)
    """
    require_api_auth()

    fmt = (request.args.get("format") or "json").lower()
    limit = max(1, min(int(request.args.get("limit", 100)), 1000))
    offset = max(0, int(request.args.get("offset", 0)))

    from_s = parse_iso8601(request.args.get("from", ""))
    to_s   = parse_iso8601(request.args.get("to", ""))
    bbox   = parse_bbox(request.args.get("bbox", ""))
    within = parse_within(request.args.get("within", ""))

    # ---------- Fields (requested → sanitized → excluded stripped) ----------
    fields_param = request.args.get("fields", "")
    requested_fields = [f.strip() for f in fields_param.split(",") if f.strip()] if fields_param else DEFAULT_FIELDS[:]

    conn = get_conn()
    table = get_sample_table(conn)

    cur = conn.execute(f"PRAGMA table_info({quote_ident(table)})")
    cols = {row[1] for row in cur.fetchall()}

    if requested_fields == ["*"]:
        requested_fields = sorted(c for c in cols if not is_excluded_field(c))

    fields = [f for f in requested_fields if f in cols and not is_excluded_field(f)]
    if not fields:
        # fallback: defaults that exist and are safe
        fields = [f for f in DEFAULT_FIELDS if f in cols]
        if not fields:
            # ultimate fallback: any non-excluded columns
            fields = sorted(c for c in cols if not is_excluded_field(c))

    # ---------- Order (must not be excluded) ----------
    order = (request.args.get("order") or "collectedAt").strip()
    if order not in cols or is_excluded_field(order):
        order = "collectedAt" if "collectedAt" in cols and not is_excluded_field("collectedAt") \
            else (next((c for c in cols if not is_excluded_field(c)), "rowid"))
    direction = (request.args.get("dir") or "desc").lower()
    direction = "desc" if direction not in ("asc", "desc") else direction

    # ---------- WHERE ----------
    where = []
    params: List[Any] = []

    if from_s:
        where.append(f"{quote_ident('collectedAt')} >= ?")
        params.append(from_s)
    if to_s:
        where.append(f"{quote_ident('collectedAt')} <= ?")
        params.append(to_s)

    if bbox and "GPS_long" in cols and "GPS_lat" in cols:
        west, south, east, north = bbox
        where.append("(GPS_long BETWEEN ? AND ? AND GPS_lat BETWEEN ? AND ?)")
        params.extend([west, east, south, north])

    if within and "GPS_long" in cols and "GPS_lat" in cols:
        lat0, lon0, r_km = within
        dlat = approx_deg_for_km_lat(r_km)
        dlon = approx_deg_for_km_lon(r_km, lat0)
        where.append("(GPS_lat BETWEEN ? AND ? AND GPS_long BETWEEN ? AND ?)")
        params.extend([lat0 - dlat, lat0 + dlat, lon0 - dlon, lon0 + dlon])

    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    # ---------- Query ----------
    selected = ", ".join(quote_ident(f) for f in fields) if fields else "*"
    if selected == "*":
        safe_all = sorted(c for c in cols if not is_excluded_field(c))
        selected = ", ".join(quote_ident(f) for f in safe_all) if safe_all else "*"

    sql = f"SELECT {selected} FROM {quote_ident(table)} {where_sql} ORDER BY {quote_ident(order)} {direction} LIMIT ? OFFSET ?"
    rows = list(conn.execute(sql, params + [limit, offset]).fetchall())

    # count (best-effort)
    try:
        cnt = conn.execute(f"SELECT COUNT(*) FROM {quote_ident(table)} {where_sql}", params).fetchone()[0]
    except Exception:
        cnt = len(rows)

    if fmt == "csv":
        return stream_csv(iter(rows), fields)
    if fmt == "geojson":
        return to_geojson(rows, "GPS_long", "GPS_lat")
    return jsonify({
        "meta": {"count": cnt, "limit": limit, "offset": offset, "order": order, "dir": direction},
        "data": [dict(r) for r in rows],
    })

@data_api.get("/samples/count")
def samples_count():
    require_api_auth()
    conn = get_conn()
    table = get_sample_table(conn)

    from_s = parse_iso8601(request.args.get("from", ""))
    to_s   = parse_iso8601(request.args.get("to", ""))

    where = []
    params = []
    if from_s:
        where.append(f"{quote_ident('collectedAt')} >= ?")
        params.append(from_s)
    if to_s:
        where.append(f"{quote_ident('collectedAt')} <= ?")
        params.append(to_s)

    where_sql = ("WHERE " + " AND ".join(where)) if where else ""
    cnt = conn.execute(f"SELECT COUNT(*) FROM {quote_ident(table)} {where_sql}", params).fetchone()[0]
    return jsonify({"count": cnt})

# -------------------------------------------
# ---------- lab enrichment upload ----------
# -------------------------------------------

# ---------- Metal oxide → metal conversion ----------
OXIDE_TO_METAL: dict[str, tuple[str, float]] = {
    "MN2O3": ("Mn", 0.696),
    "AL2O3": ("Al", 0.529),
    "CAO":   ("Ca", 0.715),
    "FE2O3": ("Fe", 0.699),
    "MGO":   ("Mg", 0.603),
    "SIO2":  ("Si", 0.467),
    "P2O5":  ("P", 0.436),
    "TIO2":  ("Ti", 0.599),
    "K2O":   ("K", 0.83),
    "SO3":   ("S", 0.40),
}


def _oxide_to_metal(param: str, value: Any) -> Optional[tuple[str, float]]:
    """
    If param is an oxide (e.g. 'K2O') and value is numeric,
    return (metal_param, converted_value). Otherwise None.
    """
    if value is None or value == "":
        return None

    norm = param.strip().upper().replace(" ", "")
    meta = OXIDE_TO_METAL.get(norm)
    if not meta:
        return None

    metal_param, factor = meta

    # try to parse numeric value, allow "12,3" etc.
    try:
        v = float(str(value).replace(",", "."))
    except (TypeError, ValueError):
        return None

    return metal_param, v * factor


def _normalize_qr(raw: str) -> str:
    """
    Match the logic from web routes:
    - strip leading 'ECHO-'
    - inject dash after 4 chars if missing
    """
    if not raw:
        return ""
    raw = str(raw).strip()
    if raw.upper().startswith("ECHO-"):
        raw = raw[5:]
    if "-" not in raw and len(raw) >= 5:
        raw = raw[:4] + "-" + raw[4:]
    return raw

def _ensure_lab_enrichment(conn: sqlite3.Connection):
    # use the same schema as the web upload route
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS lab_enrichment (
          qr_code    TEXT NOT NULL,
          param      TEXT NOT NULL,
          value      TEXT,
          unit       TEXT,
          user_id    TEXT,
          raw_row    TEXT,
          updated_at TEXT DEFAULT (datetime('now')),
          PRIMARY KEY (qr_code, param)
        )
        """
    )

@data_api.post("/lab-enrichment")
def lab_enrichment_upload():
    """
    POST /api/v1/lab-enrichment

    Auth: same as /api/v1/samples (API key, bearer, or session)

    Accepted payloads:

    1) JSON:
       [
         {"qr_code": "ECHO-ABCD1234", "metal1": 12.3, "metal1_unit": "mg/kg"},
         ...
       ]
       or { "rows": [ ... ] }

    2) multipart/form-data:
       file=<csv|xlsx>

    3) Raw CSV/XLSX body (Content-Type: text/csv or application/vnd.openxmlformats-officedocument.spreadsheetml.sheet)

    Each row is turned into multiple lab_enrichment records:
    (qr_code, param=<column>, value=<cell>, unit=maybe_from_<column>_unit)
    """
    require_api_auth()

    conn = get_conn()
    _ensure_lab_enrichment(conn)

    rows = None

    # ---------- 1) multipart/form-data with file= ----------
    if "file" in request.files:
        f = request.files["file"]
        filename = (f.filename or "").lower()

        # we use pandas here because it already exists in the project
        try:
            import pandas as pd
        except ImportError:
            abort(400, description="pandas is required to read XLSX/CSV uploads")

        if filename.endswith(".xlsx") or (
            request.content_type or ""
        ).startswith("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"):
            df = pd.read_excel(f)
        else:
            # CSV — try comma, then tab
            try:
                df = pd.read_csv(f)
            except Exception:
                f.stream.seek(0)
                df = pd.read_csv(f, sep="\t")

        rows = df.to_dict(orient="records")

    else:
        # ---------- 2) check content-type ----------
        ct = (request.content_type or "").split(";", 1)[0].strip().lower()

        if ct in ("text/csv", "application/csv", "text/plain"):
            # raw CSV in body
            text = request.get_data(as_text=True)
            import csv as _csv
            reader = _csv.DictReader(text.splitlines())
            rows = list(reader)

        elif ct.startswith("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"):
            # raw XLSX in body
            try:
                import pandas as pd
            except ImportError:
                abort(400, description="pandas is required to read XLSX uploads")

            df = pd.read_excel(io.BytesIO(request.get_data()))
            rows = df.to_dict(orient="records")

        else:
            # ---------- 3) assume JSON ----------
            payload = request.get_json(silent=True)
            if not payload:
                abort(400, description="Expected JSON array/object, CSV, XLSX, or multipart/form-data with file=")

            if isinstance(payload, dict) and "rows" in payload:
                rows = payload["rows"]
            else:
                rows = payload
            
            g._analytics_extra = {
                "upload_type": "lab_enrichment_api",
                "payload_keys": list(payload.keys())[:10],  # only keys, not full data
            }

    if not isinstance(rows, list):
        abort(400, description="Parsed payload is not a list of rows")

    # identify uploader
    uploader_id = None
    claims = verify_bearer()
    if claims:
        uploader_id = claims.get("sub") or claims.get("preferred_username")
    if not uploader_id:
        uploader_id = request.headers.get("X-User-Id") or "api"

    inserted = 0
    updated = 0
    skipped = 0

    cur = conn.cursor()

    for raw_row in rows:
        if not isinstance(raw_row, dict):
            skipped += 1
            continue

        qr = (
            raw_row.get("qr_code")
            or raw_row.get("QR_qrCode")
            or raw_row.get("id")
            or raw_row.get("ID")
            or ""
        )
        qr = _normalize_qr(qr)
        if not qr:
            skipped += 1
            continue

        raw_json = json.dumps(raw_row, ensure_ascii=False)

        for key, val in raw_row.items():
            # skip id-like fields
            if key in ("qr_code", "QR_qrCode", "id", "ID"):
                continue
            if val is None or val == "":
                continue

            # skip obvious unit-only columns
            low = key.lower()
            if low.startswith("unit") or low.endswith("_unit"):
                continue

            param = str(key).strip()

            # find a unit (reuse for both oxide and metal)
            unit = ""
            unit_key = f"{key}_unit"
            if unit_key in raw_row and raw_row[unit_key]:
                unit = str(raw_row[unit_key]).strip()
            elif "unit" in raw_row and raw_row["unit"]:
                unit = str(raw_row["unit"]).strip()

            # 1) Store the raw value exactly as provided (e.g. 'K2O')
            cur.execute(
                """
                INSERT INTO lab_enrichment (qr_code, param, value, unit, user_id, raw_row, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, datetime('now'))
                ON CONFLICT(qr_code, param) DO UPDATE SET
                    value=excluded.value,
                    unit=excluded.unit,
                    user_id=excluded.user_id,
                    raw_row=excluded.raw_row,
                    updated_at=datetime('now')
                """,
                (qr, param, str(val), unit, uploader_id, raw_json),
            )
            inserted += 1

            # 2) If this is an oxide, also store the derived metal
            conv = _oxide_to_metal(param, val)
            if conv is not None:
                metal_param, metal_val = conv
                cur.execute(
                    """
                    INSERT INTO lab_enrichment (qr_code, param, value, unit, user_id, raw_row, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, datetime('now'))
                    ON CONFLICT(qr_code, param) DO UPDATE SET
                        value=excluded.value,
                        unit=excluded.unit,
                        user_id=excluded.user_id,
                        raw_row=excluded.raw_row,
                        updated_at=datetime('now')
                    """,
                    (qr, metal_param, str(metal_val), unit, uploader_id, raw_json),
                )
                inserted += 1

    conn.commit()

    return jsonify({
        "ok": True,
        "processed": inserted,
        "skipped": skipped,
    })

@data_api.get("/canonical/samples")
def canonical_samples():
    """
    Canonical API over Postgres "samples" table.

    Query params:
      - from, to        (ISO-ish; compared to timestamp_utc)
      - country        (or country_code)
      - bbox           (west,south,east,north) on lon/lat
      - within         (lat,lon,r_km)
      - fields         (comma list subset of canonical columns; default all)
      - limit, offset
      - order, dir     (whitelisted columns; default timestamp_utc desc)
      - format         (json|csv|geojson; default json)
      - api_key / Bearer / session as in require_api_auth()
    """
    require_api_auth()

    fmt = (request.args.get("format") or "json").lower()
    limit = max(1, min(int(request.args.get("limit", 100)), 1000))
    offset = max(0, int(request.args.get("offset", 0)))

    # ---------- fields ----------
    fields_param = (request.args.get("fields") or "").strip()
    if fields_param:
        requested = [f.strip() for f in fields_param.split(",") if f.strip()]
        fields = [f for f in requested if f in CANONICAL_SAMPLE_COLS]
    else:
        fields = CANONICAL_SAMPLE_COLS[:]

    if not fields:
        fields = CANONICAL_SAMPLE_COLS[:]

    # ---------- order ----------
    order = (request.args.get("order") or "timestamp_utc").strip()
    allowed_order = set(CANONICAL_SAMPLE_COLS)
    if order not in allowed_order:
        order = "timestamp_utc"

    direction = (request.args.get("dir") or "desc").lower()
    direction = "desc" if direction not in ("asc", "desc") else direction

    # ---------- WHERE (shared helper) ----------
    where_sql, params, filter_meta = _canonical_where_from_request(alias="")

    cols_sql = ", ".join(fields)

    # ---------- Query ----------
    with get_pg_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            f"""
            SELECT {cols_sql}
            FROM samples
            {where_sql}
            ORDER BY {order} {direction}
            LIMIT %s OFFSET %s
            """,
            params + [limit, offset],
        )
        rows = cur.fetchall()

        cur.execute(
            f"SELECT COUNT(*) AS c FROM samples {where_sql}",
            params,
        )
        total = cur.fetchone()["c"]

    # ---------- Analytics extras ----------
    meta = {
        "api_name": "canonical_samples",
        "format": fmt,
    }
    meta.update({k: v for k, v in filter_meta.items() if v is not None})
    g._analytics_extra = meta

    if fmt == "csv":
        return stream_csv(iter(rows), fields=fields)

    if fmt == "geojson":
        # canonical uses lon/lat columns
        return to_geojson(rows, "lon", "lat")

    return jsonify(
        {
            "meta": {
                "count": total,
                "limit": limit,
                "offset": offset,
                "order": order,
                "dir": direction,
                "fields": fields,
            },
            "data": rows,
        }
    )

@data_api.get("/canonical/samples/count")
def canonical_samples_count():
    """Count canonical samples in Postgres, with same filters as /canonical/samples."""
    require_api_auth()

    where_sql, params, filter_meta = _canonical_where_from_request(alias="")

    with get_pg_conn() as conn, conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM samples {where_sql}", params)
        count = cur.fetchone()[0]

    meta = {"api_name": "canonical_samples_count"}
    meta.update({k: v for k, v in filter_meta.items() if v is not None})
    g._analytics_extra = meta

    return jsonify({"count": count})

@data_api.get("/canonical/sample_images")
def canonical_sample_images():
    """
    Canonical images from Postgres "sample_images" table.

    Query params:
      - sample_id
      - country (or country_code)
      - from, to        (timestamp_utc)
      - fields          (subset of canonical image columns)
      - limit, offset
      - format          (json|csv; default json)
    """
    require_api_auth()

    fmt = (request.args.get("format") or "json").lower()
    limit = max(1, min(int(request.args.get("limit", 100)), 1000))
    offset = max(0, int(request.args.get("offset", 0)))

    fields_param = (request.args.get("fields") or "").strip()
    if fields_param:
        requested = [f.strip() for f in fields_param.split(",") if f.strip()]
        fields = [f for f in requested if f in CANONICAL_IMAGE_COLS]
    else:
        fields = CANONICAL_IMAGE_COLS[:]

    if not fields:
        fields = CANONICAL_IMAGE_COLS[:]

    where = []
    params: List[Any] = []

    sample_id = request.args.get("sample_id")
    if sample_id:
        where.append("sample_id = %s")
        params.append(sample_id.strip())

    country = request.args.get("country") or request.args.get("country_code")
    if country:
        where.append("country_code = %s")
        params.append(country.upper())

    from_s = parse_iso8601(request.args.get("from", ""))
    to_s   = parse_iso8601(request.args.get("to", ""))
    if from_s:
        where.append("timestamp_utc >= %s")
        params.append(from_s)
    if to_s:
        where.append("timestamp_utc <= %s")
        params.append(to_s)

    where_sql = "WHERE " + " AND ".join(where) if where else ""
    cols_sql = ", ".join(fields)

    with get_pg_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            f"""
            SELECT {cols_sql}
            FROM sample_images
            {where_sql}
            ORDER BY sample_id, image_id
            LIMIT %s OFFSET %s
            """,
            params + [limit, offset],
        )
        rows = cur.fetchall()

        cur.execute(
            f"SELECT COUNT(*) AS c FROM sample_images {where_sql}",
            params,
        )
        total = cur.fetchone()["c"]

    g._analytics_extra = {
        "api_name": "canonical_sample_images",
        "format": fmt,
    }

    if fmt == "csv":
        return stream_csv(iter(rows), fields=fields)

    return jsonify(
        {
            "meta": {
                "count": total,
                "limit": limit,
                "offset": offset,
                "fields": fields,
            },
            "data": rows,
        }
    )

@data_api.get("/canonical/sample_parameters")
def canonical_sample_parameters():
    """
    Canonical parameters from Postgres "sample_parameters" table.

    Query params:
      - sample_id
      - country (or country_code)
      - parameter_code
      - fields          (subset of canonical parameter columns)
      - limit, offset
      - format          (json|csv; default json)
    """
    require_api_auth()

    fmt = (request.args.get("format") or "json").lower()
    limit = max(1, min(int(request.args.get("limit", 100)), 1000))
    offset = max(0, int(request.args.get("offset", 0)))

    fields_param = (request.args.get("fields") or "").strip()
    if fields_param:
        requested = [f.strip() for f in fields_param.split(",") if f.strip()]
        fields = [f for f in requested if f in CANONICAL_PARAM_COLS]
    else:
        fields = CANONICAL_PARAM_COLS[:]

    if not fields:
        fields = CANONICAL_PARAM_COLS[:]

    where = []
    params: List[Any] = []

    sample_id = request.args.get("sample_id")
    if sample_id:
        where.append("sample_id = %s")
        params.append(sample_id.strip())

    country = request.args.get("country") or request.args.get("country_code")
    if country:
        where.append("country_code = %s")
        params.append(country.upper())

    param_code = request.args.get("parameter_code")
    if param_code:
        where.append("parameter_code = %s")
        params.append(param_code.strip())

    where_sql = "WHERE " + " AND ".join(where) if where else ""
    cols_sql = ", ".join(fields)

    with get_pg_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            f"""
            SELECT {cols_sql}
            FROM sample_parameters
            {where_sql}
            ORDER BY sample_id, parameter_code
            LIMIT %s OFFSET %s
            """,
            params + [limit, offset],
        )
        rows = cur.fetchall()

        cur.execute(
            f"SELECT COUNT(*) AS c FROM sample_parameters {where_sql}",
            params,
        )
        total = cur.fetchone()["c"]

    g._analytics_extra = {
        "api_name": "canonical_sample_parameters",
        "format": fmt,
    }

    if fmt == "csv":
        return stream_csv(iter(rows), fields=fields)

    return jsonify(
        {
            "meta": {
                "count": total,
                "limit": limit,
                "offset": offset,
                "fields": fields,
            },
            "data": rows,
        }
    )

@data_api.get("/canonical/all.zip")
def canonical_all_zip():
    """
    GET /api/v1/canonical/all.zip

    Returns a ZIP file with:
      - samples.csv
      - sample_images.csv
      - sample_parameters.csv

    Data is pulled from Postgres canonical tables and filtered with the same
    query params as /canonical/samples:

      - from, to           (timestamp_utc on samples)
      - country/country_code
      - bbox, within       (on samples.lon/lat)

    Images and parameters are restricted to the matching samples via JOIN.
    Auth: same as other /api/v1 endpoints (API key, bearer, or session).
    """
    require_api_auth()

    # WHERE over samples aliased as "s"
    where_sql, params, filter_meta = _canonical_where_from_request(alias="s")

    mem = io.BytesIO()

    def _write_query_to_zip(zip_name: str, cols: List[str], sql: str, sql_params: List[Any]):
        with get_pg_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, sql_params)
            rows = cur.fetchall()

        if cols:
            fields = cols[:]  # canonical order
        else:
            fields = list(rows[0].keys()) if rows else []

        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for r in rows:
            writer.writerow(r)
        zf.writestr(zip_name, buf.getvalue())

    with zipfile.ZipFile(mem, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        # 1) samples.csv (from samples s)
        sample_cols_sql = ", ".join(CANONICAL_SAMPLE_COLS)
        sql_samples = f"""
            SELECT {sample_cols_sql}
            FROM samples s
            {where_sql}
            ORDER BY s.timestamp_utc DESC, s.sample_id
        """
        _write_query_to_zip("samples.csv", CANONICAL_SAMPLE_COLS, sql_samples, params)

        # 2) sample_images.csv (images i joined to filtered samples s)
        img_cols_sql = ", ".join(f"i.{c}" for c in CANONICAL_IMAGE_COLS)
        sql_imgs = f"""
            SELECT {img_cols_sql}
            FROM sample_images i
            JOIN samples s ON s.sample_id = i.sample_id
            {where_sql}
            ORDER BY i.sample_id, i.image_id
        """
        _write_query_to_zip("sample_images.csv", CANONICAL_IMAGE_COLS, sql_imgs, params)

        # 3) sample_parameters.csv (parameters p joined to filtered samples s)
        param_cols_sql = ", ".join(f"p.{c}" for c in CANONICAL_PARAM_COLS)
        sql_params = f"""
            SELECT {param_cols_sql}
            FROM sample_parameters p
            JOIN samples s ON s.sample_id = p.sample_id
            {where_sql}
            ORDER BY p.sample_id, p.parameter_code
        """
        _write_query_to_zip("sample_parameters.csv", CANONICAL_PARAM_COLS, sql_params, params)

    mem.seek(0)

    # Analytics: mark dataset + filters + api endpoint
    meta = {
        "dataset": "canonical_all",
        "api_name": "canonical_all_zip",
    }
    meta.update({k: v for k, v in filter_meta.items() if v is not None})
    g._analytics_extra = meta

    return Response(
        mem.getvalue(),
        mimetype="application/zip",
        headers={
            "Content-Disposition": 'attachment; filename="canonical_all.zip"',
        },
    )
