# echorepo/routes/data_api.py
from __future__ import annotations

import csv
import hmac
import io
import json
import logging
import math
import os
import re
import sqlite3
import threading
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any, Iterable, Mapping

import jwt
import pandas as pd
import requests
from flask import (
    Blueprint,
    Response,
    abort,
    current_app,
    g,
    jsonify,
    request,
    session,
)
from psycopg2.extras import RealDictCursor

from echorepo.services.canonical_exports import (
    BIODIVERSITY_COLUMNS,
    IMAGE_COLUMNS,
    PARAMETER_COLUMNS,
    SAMPLE_COLUMNS,
    build_machine_bundle,
    get_parameters_df,
)
from echorepo.services.db import _ensure_lab_enrichment, get_pg_conn
from echorepo.services.storage.minio import upload_canonical_zip


log = logging.getLogger(__name__)
data_api = Blueprint("data_api", __name__)

# Keep the old public names for compatibility with imports elsewhere, while
# defining the canonical schemas in exactly one module.
CANONICAL_SAMPLE_COLS = SAMPLE_COLUMNS
CANONICAL_IMAGE_COLS = IMAGE_COLUMNS
CANONICAL_PARAM_COLS = PARAMETER_COLUMNS
CANONICAL_BIODIV_COLS = BIODIVERSITY_COLUMNS

MAX_PAGE_SIZE = 1_000
MAX_MAP_PAGE_SIZE = 10_000
DEFAULT_PAGE_SIZE = 100
DEFAULT_MAP_PAGE_SIZE = 2_000


# -----------------------------------------------------------------------------
# Generic configuration and validation helpers
# -----------------------------------------------------------------------------


def _env_bool(name: str, default: bool = False) -> bool:
    value = current_app.config.get(name)
    if value is None:
        value = os.getenv(name)
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "on", "y", "t"}


def _parse_int_arg(
    name: str,
    default: int,
    *,
    minimum: int = 0,
    maximum: int | None = None,
) -> int:
    raw = request.args.get(name)
    if raw in (None, ""):
        value = default
    else:
        try:
            value = int(raw)
        except (TypeError, ValueError):
            abort(400, description=f"{name} must be an integer")

    if value < minimum:
        abort(400, description=f"{name} must be at least {minimum}")
    if maximum is not None and value > maximum:
        value = maximum
    return value


def _parse_float_arg(name: str) -> float | None:
    raw = (request.args.get(name) or "").strip()
    if not raw:
        return None
    try:
        value = float(raw)
    except ValueError:
        abort(400, description=f"{name} must be numeric")
    if not math.isfinite(value):
        abort(400, description=f"{name} must be finite")
    return value


def _parse_format(*allowed: str, default: str = "json") -> str:
    value = (request.args.get("format") or default).strip().lower()
    if value not in allowed:
        abort(
            400,
            description=(
                f"Unsupported format {value!r}; expected one of: "
                + ", ".join(allowed)
            ),
        )
    return value


def _parse_fields(raw: str, allowed: list[str]) -> list[str]:
    requested = [part.strip() for part in raw.split(",") if part.strip()]
    if not requested:
        return allowed[:]
    selected = [field for field in requested if field in allowed]
    return selected or allowed[:]


def _parse_date_or_datetime(raw: str, *, end_bound: bool = False) -> tuple[str, str]:
    """
    Return (normalized value, SQL operator).

    A date-only upper bound is made inclusive by converting it to the start of
    the next day and using '<'. A datetime upper bound uses '<='.
    """
    value = str(raw or "").strip()
    if not value:
        raise ValueError("empty date")

    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        parsed_date = date.fromisoformat(value)
        if end_bound:
            return ((parsed_date + timedelta(days=1)).isoformat(), "<")
        return (parsed_date.isoformat(), ">=")

    candidate = value[:-1] + "+00:00" if value.endswith("Z") else value
    parsed = datetime.fromisoformat(candidate)
    return (parsed.isoformat(), "<=" if end_bound else ">=")


def parse_iso8601(value: str) -> str | None:
    """Backward-compatible public helper returning normalized ISO text."""
    if not value:
        return None
    normalized, _operator = _parse_date_or_datetime(value)
    return normalized


def _time_window_from_request(
    *,
    from_name: str = "from",
    to_name: str = "to",
) -> tuple[str | None, str | None, str]:
    raw_from = (request.args.get(from_name) or "").strip()
    raw_to = (request.args.get(to_name) or "").strip()

    from_value: str | None = None
    to_value: str | None = None
    to_operator = "<="

    try:
        if raw_from:
            from_value, _ = _parse_date_or_datetime(raw_from)
        if raw_to:
            to_value, to_operator = _parse_date_or_datetime(raw_to, end_bound=True)
    except ValueError:
        abort(
            400,
            description=(
                "Invalid date/time filter; use YYYY-MM-DD or an ISO-8601 datetime"
            ),
        )

    return from_value, to_value, to_operator


def parse_bbox(value: str) -> tuple[float, float, float, float] | None:
    if not value:
        return None
    try:
        parts = [float(part.strip()) for part in value.split(",")]
    except ValueError as exc:
        raise ValueError("bbox must contain four numbers") from exc
    if len(parts) != 4:
        raise ValueError("bbox must contain west,south,east,north")

    west, south, east, north = parts
    if not all(math.isfinite(part) for part in parts):
        raise ValueError("bbox values must be finite")
    if not (-180 <= west <= 180 and -180 <= east <= 180):
        raise ValueError("bbox longitudes must be between -180 and 180")
    if not (-90 <= south <= 90 and -90 <= north <= 90):
        raise ValueError("bbox latitudes must be between -90 and 90")
    if west > east or south > north:
        raise ValueError("bbox minimum values must not exceed maximum values")
    return west, south, east, north


def parse_within(value: str) -> tuple[float, float, float] | None:
    if not value:
        return None
    try:
        parts = [float(part.strip()) for part in value.split(",")]
    except ValueError as exc:
        raise ValueError("within must contain three numbers") from exc
    if len(parts) != 3:
        raise ValueError("within must contain lat,lon,radius_km")

    lat, lon, radius_km = parts
    if not all(math.isfinite(part) for part in parts):
        raise ValueError("within values must be finite")
    if not -90 <= lat <= 90 or not -180 <= lon <= 180:
        raise ValueError("within latitude/longitude is outside the valid range")
    if radius_km <= 0:
        raise ValueError("within radius must be greater than zero")
    return lat, lon, radius_km


def approx_deg_for_km_lat(km: float) -> float:
    return km / 111.32


def approx_deg_for_km_lon(km: float, at_lat: float) -> float:
    return km / (111.32 * max(0.1, math.cos(math.radians(at_lat))))


def _is_blank(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return not value.strip()
    try:
        return bool(pd.isna(value))
    except Exception:
        return False


def _dataframe_records(df: pd.DataFrame) -> list[dict[str, Any]]:
    if df.empty:
        return []
    cleaned = df.astype(object).where(pd.notna(df), None)
    return cleaned.to_dict(orient="records")


# -----------------------------------------------------------------------------
# Legacy SQLite access
# -----------------------------------------------------------------------------


def get_db_path() -> str:
    return (
        current_app.config.get("SQLITE_PATH")
        or os.environ.get("SQLITE_PATH")
        or os.path.join(current_app.root_path, "..", "..", "data", "db", "data.db")
    )


def quote_ident(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


def get_sample_table(conn: sqlite3.Connection) -> str:
    explicit = current_app.config.get("SAMPLE_TABLE") or os.environ.get("SAMPLE_TABLE")
    if explicit:
        return str(explicit)

    tables = [row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")]

    for candidate in ("samples", "sample", "data", "records"):
        if candidate in tables:
            return candidate

    for table in tables:
        try:
            columns = {
                row[1]
                for row in conn.execute(
                    f"PRAGMA table_info({quote_ident(table)})"
                ).fetchall()
            }
            if "sampleId" in columns:
                return table
        except sqlite3.Error:
            continue

    if tables:
        return tables[0]
    raise RuntimeError("No tables found in SQLite database")


def get_conn() -> sqlite3.Connection:
    if "sqlite_conn" not in g:
        db_path = get_db_path()
        if not os.path.isfile(db_path):
            abort(503, description=f"SQLite database not found: {db_path}")
        conn = sqlite3.connect(
            db_path,
            detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        g.sqlite_conn = conn
    return g.sqlite_conn


@data_api.teardown_app_request
def _close_conn(_exc):
    conn = g.pop("sqlite_conn", None)
    if conn is not None:
        conn.close()


# -----------------------------------------------------------------------------
# OIDC and API authentication
# -----------------------------------------------------------------------------


_OIDC_CACHE: dict[tuple[str, str, str], dict[str, Any]] = {}
_OIDC_CACHE_LOCK = threading.Lock()


def oidc_cfg(*, force_refresh: bool = False) -> dict[str, Any]:
    """Return cached OIDC discovery/JWKS configuration."""
    issuer_url = (
        current_app.config.get("OIDC_ISSUER_URL")
        or os.environ.get("OIDC_ISSUER_URL")
        or ""
    ).strip()
    audience = (
        current_app.config.get("OIDC_AUDIENCE")
        or os.environ.get("OIDC_AUDIENCE")
        or ""
    ).strip()
    client_id = (
        current_app.config.get("OIDC_CLIENT_ID")
        or os.environ.get("OIDC_CLIENT_ID")
        or ""
    ).strip()

    if not issuer_url:
        return {"enabled": False}

    cache_key = (issuer_url, audience, client_id)
    now = time.time()
    with _OIDC_CACHE_LOCK:
        cached = _OIDC_CACHE.get(cache_key)
        if not force_refresh and cached and cached["expires_at"] > now:
            return cached

    well_url = issuer_url.rstrip("/") + "/.well-known/openid-configuration"
    well_response = requests.get(well_url, timeout=5)
    well_response.raise_for_status()
    well = well_response.json()

    jwks_uri = str(well.get("jwks_uri") or "").strip()
    if not jwks_uri:
        raise RuntimeError("OIDC discovery document has no jwks_uri")

    jwks_response = requests.get(jwks_uri, timeout=5)
    jwks_response.raise_for_status()
    jwks = jwks_response.json()

    cfg = {
        "enabled": True,
        "issuer": well.get("issuer") or issuer_url.rstrip("/"),
        "jwks": jwks,
        "aud": audience or None,
        "client_id": client_id or None,
        "expires_at": now + 300,
    }
    with _OIDC_CACHE_LOCK:
        _OIDC_CACHE[cache_key] = cfg
    return cfg


def verify_bearer() -> dict[str, Any] | None:
    auth = request.headers.get("Authorization", "")
    if not auth.startswith("Bearer "):
        return None
    token = auth.split(" ", 1)[1].strip()
    if not token:
        return None

    try:
        cfg = oidc_cfg()
        if not cfg.get("enabled"):
            return None

        header = jwt.get_unverified_header(token)
        algorithm = str(header.get("alg") or "")
        if algorithm not in {"RS256", "PS256", "ES256"}:
            return None

        kid = header.get("kid")
        jwk_data = next(
            (
                item
                for item in cfg.get("jwks", {}).get("keys", [])
                if item.get("kid") == kid
            ),
            None,
        )

        if jwk_data is None:
            cfg = oidc_cfg(force_refresh=True)
            jwk_data = next(
                (
                    item
                    for item in cfg.get("jwks", {}).get("keys", [])
                    if item.get("kid") == kid
                ),
                None,
            )
        if jwk_data is None:
            return None

        key = jwt.PyJWK.from_dict(jwk_data).key
        audience = cfg.get("aud")
        claims = jwt.decode(
            token,
            key=key,
            algorithms=[algorithm],
            audience=audience,
            issuer=cfg.get("issuer"),
            options={
                "require": ["exp", "iat"],
                "verify_aud": bool(audience),
            },
        )

        client_id = cfg.get("client_id")
        if client_id:
            aud_claim = claims.get("aud")
            audiences = set(
                aud_claim
                if isinstance(aud_claim, list)
                else [aud_claim]
                if aud_claim
                else []
            )
            if claims.get("azp") != client_id and client_id not in audiences:
                return None

        return claims

    except requests.RequestException as exc:
        current_app.logger.warning("OIDC metadata/JWKS request failed: %s", exc)
        return None
    except (jwt.InvalidTokenError, ValueError, KeyError, RuntimeError) as exc:
        current_app.logger.debug("Bearer token rejected: %s", exc)
        return None


def require_api_auth() -> dict[str, Any] | None:
    """Require API key, Keycloak bearer token, or logged-in web session."""
    required_key = (
        current_app.config.get("API_KEY")
        or os.environ.get("API_KEY")
        or ""
    ).strip()

    if required_key:
        authz = request.headers.get("Authorization", "")
        supplied_key = (
            request.headers.get("X-API-Key")
            or request.headers.get("X-Api-Key")
            or request.headers.get("x-api-key")
            or request.args.get("api_key")
            or (authz[7:] if authz.startswith("ApiKey ") else "")
            or ""
        ).strip()
        if supplied_key and hmac.compare_digest(supplied_key, required_key):
            g.api_auth_method = "api_key"
            return None

    claims = verify_bearer()
    if claims:
        g.api_auth_method = "bearer"
        g.api_claims = claims
        return claims

    if session.get("user") or (session.get("kc") or {}).get("profile"):
        g.api_auth_method = "session"
        return None

    try:
        from flask_login import current_user

        if getattr(current_user, "is_authenticated", False):
            g.api_auth_method = "session"
            return None
    except Exception:
        pass

    response = jsonify({"ok": False, "error": "Missing or invalid credentials"})
    response.status_code = 401
    response.headers["WWW-Authenticate"] = "Bearer"
    abort(response)


def _canonical_public_enabled() -> bool:
    return _env_bool("CANONICAL_PUBLIC", False)


def _require_canonical_access(*, public_allowed: bool = False) -> None:
    if public_allowed and _canonical_public_enabled():
        return
    require_api_auth()


def _current_uploader_id() -> str:
    claims = getattr(g, "api_claims", None) or verify_bearer() or {}
    profile = (session.get("kc") or {}).get("profile") or {}
    return str(
        claims.get("sub")
        or claims.get("preferred_username")
        or profile.get("id")
        or profile.get("sub")
        or profile.get("email")
        or request.headers.get("X-User-Id")
        or session.get("user")
        or "api"
    )


# -----------------------------------------------------------------------------
# Response helpers
# -----------------------------------------------------------------------------


def to_geojson(rows: Iterable[Mapping[str, Any]], lon_col: str, lat_col: str) -> Response:
    features: list[dict[str, Any]] = []
    for row in rows:
        data = dict(row)
        try:
            lon = float(data.get(lon_col))
            lat = float(data.get(lat_col))
        except (TypeError, ValueError):
            continue
        if not math.isfinite(lon) or not math.isfinite(lat):
            continue
        properties = {
            key: value
            for key, value in data.items()
            if key not in (lon_col, lat_col)
        }
        features.append(
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [lon, lat]},
                "properties": properties,
            }
        )
    return jsonify({"type": "FeatureCollection", "features": features})


def stream_csv(
    rows_iter: Iterable[Mapping[str, Any]],
    fields: list[str],
    *,
    filename: str | None = None,
) -> Response:
    def generate():
        output = io.StringIO(newline="")
        writer = csv.DictWriter(
            output,
            fieldnames=fields,
            extrasaction="ignore",
            lineterminator="\n",
        )
        writer.writeheader()
        yield output.getvalue()
        output.seek(0)
        output.truncate(0)

        for row in rows_iter:
            writer.writerow(dict(row))
            yield output.getvalue()
            output.seek(0)
            output.truncate(0)

    headers = {}
    if filename:
        headers["Content-Disposition"] = f'attachment; filename="{filename}"'
    return Response(generate(), mimetype="text/csv", headers=headers)


def _zip_response(data: bytes, filename: str) -> Response:
    return Response(
        data,
        mimetype="application/zip",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


# -----------------------------------------------------------------------------
# Legacy field policy
# -----------------------------------------------------------------------------


PII_FIELDS = {
    "email",
    "userid",
    "user_id",
    "uid",
    "kc_user_id",
}
EXCLUDED_SUFFIXES = ("_state",)
OXIDE_TOKEN_RE = re.compile(r"[A-Z][a-z]?\d*")


def _looks_like_oxide(label: str) -> bool:
    if not label:
        return False
    text = re.sub(r"\(.*?\)", "", str(label))
    tokens = OXIDE_TOKEN_RE.findall(text)
    return len(tokens) >= 2 and any(token.startswith("O") for token in tokens)


def _is_oxide_field(name: str) -> bool:
    if not name:
        return False
    if _looks_like_oxide(name):
        return True
    last = re.split(r"[_\s/\-]+", str(name))[-1]
    return _looks_like_oxide(last)


def is_excluded_field(name: str) -> bool:
    normalized = str(name).casefold()
    return (
        normalized in PII_FIELDS
        or any(normalized.endswith(suffix) for suffix in EXCLUDED_SUFFIXES)
        or _is_oxide_field(str(name))
    )


DEFAULT_FIELDS = [
    "sampleId",
    "collectedAt",
    "GPS_long",
    "GPS_lat",
    "PH_ph",
    "SOIL_TEXTURE_texture",
    "SOIL_STRUCTURE_structure",
    "SOIL_DIVER_earthworms",
    "SOIL_CONTAMINATION_plastic",
]


# -----------------------------------------------------------------------------
# Canonical filtering helpers
# -----------------------------------------------------------------------------


def _canonical_where_from_request(
    alias: str = "",
) -> tuple[str, list[Any], dict[str, Any]]:
    prefix = f"{alias}." if alias else ""
    where: list[str] = []
    params: list[Any] = []

    from_value, to_value, to_operator = _time_window_from_request()
    if from_value:
        where.append(f"{prefix}timestamp_utc >= %s")
        params.append(from_value)
    if to_value:
        where.append(f"{prefix}timestamp_utc {to_operator} %s")
        params.append(to_value)

    raw_country = (
        request.args.get("country")
        or request.args.get("country_code")
        or ""
    ).strip()
    country = raw_country.upper() if raw_country else None
    if country:
        where.append(f"{prefix}country_code = %s")
        params.append(country)

    raw_bbox = (request.args.get("bbox") or "").strip()
    raw_within = (request.args.get("within") or "").strip()

    try:
        bbox = parse_bbox(raw_bbox)
        within = parse_within(raw_within)
    except ValueError as exc:
        abort(400, description=str(exc))

    if bbox:
        west, south, east, north = bbox
        where.append(
            f"({prefix}lon BETWEEN %s AND %s "
            f"AND {prefix}lat BETWEEN %s AND %s)"
        )
        params.extend([west, east, south, north])

    if within:
        lat0, lon0, radius_km = within
        dlat = approx_deg_for_km_lat(radius_km)
        dlon = approx_deg_for_km_lon(radius_km, lat0)
        where.append(
            f"({prefix}lat BETWEEN %s AND %s "
            f"AND {prefix}lon BETWEEN %s AND %s)"
        )
        params.extend([lat0 - dlat, lat0 + dlat, lon0 - dlon, lon0 + dlon])

    return (
        "WHERE " + " AND ".join(where) if where else "",
        params,
        {
            "from": from_value,
            "to": to_value,
            "country": country,
            "bbox": raw_bbox or None,
            "within": raw_within or None,
        },
    )


def _canonical_matching_sample_ids(
    where_sql: str,
    params: list[Any],
) -> list[str] | None:
    if not where_sql.strip():
        return None

    with get_pg_conn() as conn, conn.cursor(
        cursor_factory=RealDictCursor
    ) as cur:
        cur.execute(
            f"""
            SELECT s.sample_id
            FROM samples AS s
            {where_sql}
            ORDER BY s.timestamp_utc DESC, s.sample_id
            """,
            params,
        )
        return [
            str(row["sample_id"]).strip()
            for row in cur.fetchall()
            if row.get("sample_id")
        ]


def _sample_ids_for_resource_filters(
    *,
    sample_id: str | None,
    country: str | None,
) -> list[str] | None:
    sample_id = str(sample_id or "").strip()
    country = str(country or "").strip().upper()
    if not sample_id and not country:
        return None

    where: list[str] = []
    params: list[Any] = []
    if sample_id:
        where.append("UPPER(sample_id) = UPPER(%s)")
        params.append(sample_id)
    if country:
        where.append("country_code = %s")
        params.append(country)

    with get_pg_conn() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT sample_id FROM samples WHERE " + " AND ".join(where),
            params,
        )
        return [str(row[0]).strip() for row in cur.fetchall() if row and row[0]]


def _add_analytics(api_name: str, **extra: Any) -> None:
    g._analytics_extra = {
        "api_name": api_name,
        **{key: value for key, value in extra.items() if value is not None},
    }


# -----------------------------------------------------------------------------
# Basic and legacy endpoints
# -----------------------------------------------------------------------------


@data_api.get("/ping")
def ping():
    return jsonify({"ok": True})


@data_api.get("/samples")
def samples():
    """Legacy SQLite samples API retained for backward compatibility."""
    require_api_auth()
    fmt = _parse_format("json", "csv", "geojson")
    limit = _parse_int_arg("limit", DEFAULT_PAGE_SIZE, minimum=1, maximum=MAX_PAGE_SIZE)
    offset = _parse_int_arg("offset", 0, minimum=0)

    from_value, to_value, to_operator = _time_window_from_request()
    try:
        bbox = parse_bbox((request.args.get("bbox") or "").strip())
        within = parse_within((request.args.get("within") or "").strip())
    except ValueError as exc:
        abort(400, description=str(exc))

    conn = get_conn()
    table = get_sample_table(conn)
    columns = {
        row[1]
        for row in conn.execute(
            f"PRAGMA table_info({quote_ident(table)})"
        ).fetchall()
    }

    raw_fields = request.args.get("fields", "")
    requested = (
        [part.strip() for part in raw_fields.split(",") if part.strip()]
        if raw_fields
        else DEFAULT_FIELDS[:]
    )
    if requested == ["*"]:
        requested = sorted(column for column in columns if not is_excluded_field(column))

    fields = [
        field
        for field in requested
        if field in columns and not is_excluded_field(field)
    ]
    if not fields:
        fields = [field for field in DEFAULT_FIELDS if field in columns]
    if not fields:
        fields = sorted(column for column in columns if not is_excluded_field(column))

    order = (request.args.get("order") or "collectedAt").strip()
    if order not in columns or is_excluded_field(order):
        order = "collectedAt" if "collectedAt" in columns else fields[0]
    direction = (request.args.get("dir") or "desc").strip().lower()
    if direction not in {"asc", "desc"}:
        abort(400, description="dir must be asc or desc")

    where: list[str] = []
    params: list[Any] = []
    if from_value and "collectedAt" in columns:
        where.append(f"{quote_ident('collectedAt')} >= ?")
        params.append(from_value)
    if to_value and "collectedAt" in columns:
        where.append(f"{quote_ident('collectedAt')} {to_operator} ?")
        params.append(to_value)
    if bbox and {"GPS_long", "GPS_lat"} <= columns:
        west, south, east, north = bbox
        where.append("(GPS_long BETWEEN ? AND ? AND GPS_lat BETWEEN ? AND ?)")
        params.extend([west, east, south, north])
    if within and {"GPS_long", "GPS_lat"} <= columns:
        lat0, lon0, radius_km = within
        dlat = approx_deg_for_km_lat(radius_km)
        dlon = approx_deg_for_km_lon(radius_km, lat0)
        where.append("(GPS_lat BETWEEN ? AND ? AND GPS_long BETWEEN ? AND ?)")
        params.extend([lat0 - dlat, lat0 + dlat, lon0 - dlon, lon0 + dlon])

    where_sql = "WHERE " + " AND ".join(where) if where else ""
    selected = ", ".join(quote_ident(field) for field in fields)
    rows = list(
        conn.execute(
            f"""
            SELECT {selected}
            FROM {quote_ident(table)}
            {where_sql}
            ORDER BY {quote_ident(order)} {direction}
            LIMIT ? OFFSET ?
            """,
            params + [limit, offset],
        ).fetchall()
    )
    total = conn.execute(
        f"SELECT COUNT(*) FROM {quote_ident(table)} {where_sql}",
        params,
    ).fetchone()[0]

    _add_analytics("legacy_samples", format=fmt)
    if fmt == "csv":
        return stream_csv(rows, fields, filename="samples.csv")
    if fmt == "geojson":
        return to_geojson(rows, "GPS_long", "GPS_lat")
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
            "data": [dict(row) for row in rows],
        }
    )


@data_api.get("/samples/count")
def samples_count():
    require_api_auth()
    conn = get_conn()
    table = get_sample_table(conn)
    from_value, to_value, to_operator = _time_window_from_request()

    where: list[str] = []
    params: list[Any] = []
    if from_value:
        where.append(f"{quote_ident('collectedAt')} >= ?")
        params.append(from_value)
    if to_value:
        where.append(f"{quote_ident('collectedAt')} {to_operator} ?")
        params.append(to_value)

    where_sql = "WHERE " + " AND ".join(where) if where else ""
    count = conn.execute(
        f"SELECT COUNT(*) FROM {quote_ident(table)} {where_sql}",
        params,
    ).fetchone()[0]
    _add_analytics("legacy_samples_count")
    return jsonify({"count": count})


# -----------------------------------------------------------------------------
# Laboratory enrichment API
# -----------------------------------------------------------------------------


OXIDE_TO_METAL: dict[str, tuple[str, float]] = {
    "MN2O3": ("Mn", 0.696),
    "AL2O3": ("Al", 0.529),
    "CAO": ("Ca", 0.715),
    "FE2O3": ("Fe", 0.699),
    "MGO": ("Mg", 0.603),
    "SIO2": ("Si", 0.467),
    "P2O5": ("P", 0.436),
    "TIO2": ("Ti", 0.599),
    "K2O": ("K", 0.830),
    "SO3": ("S", 0.400),
}


def _oxide_to_metal(param: str, value: Any) -> tuple[str, float] | None:
    if _is_blank(value):
        return None
    metadata = OXIDE_TO_METAL.get(str(param).strip().upper().replace(" ", ""))
    if metadata is None:
        return None
    try:
        numeric_value = float(str(value).replace(",", "."))
    except (TypeError, ValueError):
        return None
    metal, factor = metadata
    return metal, numeric_value * factor


def _normalize_qr(raw: Any) -> str:
    if _is_blank(raw):
        return ""
    value = str(raw).strip()
    if value.upper().startswith("ECHO-"):
        value = value[5:]
    if "-" not in value and len(value) >= 5:
        value = value[:4] + "-" + value[4:]
    return value.upper()


def _csv_rows_from_bytes(data: bytes, *, delimiter: str | None = None) -> list[dict[str, Any]]:
    text = data.decode("utf-8-sig")
    if delimiter is None:
        try:
            dialect = csv.Sniffer().sniff(text[:65536], delimiters=",;\t|")
        except csv.Error:
            dialect = csv.get_dialect("excel")
    else:
        class ExplicitDialect(csv.excel):
            pass

        ExplicitDialect.delimiter = delimiter
        dialect = ExplicitDialect
    return list(csv.DictReader(io.StringIO(text), dialect=dialect))


def _parse_lab_upload_rows() -> tuple[list[dict[str, Any]], str | None]:
    if "file" in request.files:
        uploaded = request.files["file"]
        filename = str(uploaded.filename or "").strip()
        if not filename:
            abort(400, description="Uploaded file has no filename")
        data = uploaded.read()
        if not data:
            abort(400, description=f"{filename}: uploaded file is empty")

        suffix = os.path.splitext(filename)[1].lower()
        try:
            if suffix == ".xlsx":
                df = pd.read_excel(io.BytesIO(data))
                return _dataframe_records(df), filename
            if suffix == ".tsv":
                return _csv_rows_from_bytes(data, delimiter="\t"), filename
            if suffix in {".csv", ".txt"}:
                return _csv_rows_from_bytes(data), filename
        except (UnicodeDecodeError, csv.Error, ValueError) as exc:
            abort(400, description=f"Cannot parse {filename}: {exc}")

        abort(400, description="Expected an XLSX, CSV, TSV, or TXT file")

    content_type = (request.content_type or "").split(";", 1)[0].strip().lower()
    raw_data = request.get_data()

    if content_type in {"text/csv", "application/csv", "text/plain", "text/tab-separated-values"}:
        if not raw_data:
            abort(400, description="Request body is empty")
        delimiter = "\t" if content_type == "text/tab-separated-values" else None
        try:
            return _csv_rows_from_bytes(raw_data, delimiter=delimiter), None
        except (UnicodeDecodeError, csv.Error, ValueError) as exc:
            abort(400, description=f"Cannot parse tabular request body: {exc}")

    if content_type.startswith(
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    ):
        if not raw_data:
            abort(400, description="Request body is empty")
        try:
            return _dataframe_records(pd.read_excel(io.BytesIO(raw_data))), None
        except Exception as exc:
            abort(400, description=f"Cannot parse XLSX request body: {exc}")

    payload = request.get_json(silent=True)
    if payload is None:
        abort(
            400,
            description=(
                "Expected JSON rows, CSV/TSV/XLSX request body, or multipart file upload"
            ),
        )
    rows = payload.get("rows") if isinstance(payload, dict) and "rows" in payload else payload
    if not isinstance(rows, list):
        abort(400, description="JSON payload must be an array or an object containing rows")
    return rows, None


def _unit_for_parameter(row: Mapping[str, Any], key: str, ordered_keys: list[str]) -> str:
    direct_candidates = [f"{key}_unit", f"unit_{key}"]
    casefold_map = {str(column).casefold(): column for column in row}
    for candidate in direct_candidates:
        real_key = casefold_map.get(candidate.casefold())
        if real_key is not None and not _is_blank(row.get(real_key)):
            return str(row.get(real_key)).strip()

    try:
        position = ordered_keys.index(key)
    except ValueError:
        position = -1
    if position >= 0 and position + 1 < len(ordered_keys):
        next_key = ordered_keys[position + 1]
        if str(next_key).casefold().startswith("unit") and not _is_blank(row.get(next_key)):
            return str(row.get(next_key)).strip()

    generic_unit = casefold_map.get("unit")
    if generic_unit is not None and not _is_blank(row.get(generic_unit)):
        return str(row.get(generic_unit)).strip()
    return ""


def _upsert_lab_value(
    cursor: sqlite3.Cursor,
    *,
    qr: str,
    parameter: str,
    value: Any,
    unit: str,
    uploader_id: str,
    raw_json: str,
) -> None:
    cursor.execute(
        """
        INSERT INTO lab_enrichment (
            qr_code, param, value, unit, user_id, raw_row, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, datetime('now'))
        ON CONFLICT(qr_code, param) DO UPDATE SET
            value = excluded.value,
            unit = excluded.unit,
            user_id = excluded.user_id,
            raw_row = excluded.raw_row,
            updated_at = datetime('now')
        """,
        (qr, parameter, str(value), unit, uploader_id, raw_json),
    )


@data_api.post("/lab-enrichment")
def lab_enrichment_upload():
    """Upsert elementary-concentration rows without deleting unrelated data."""
    require_api_auth()
    rows, filename = _parse_lab_upload_rows()

    max_rows = int(current_app.config.get("LAB_UPLOAD_MAX_ROWS") or 50_000)
    if len(rows) > max_rows:
        abort(413, description=f"Upload contains more than {max_rows} rows")

    uploader_id = _current_uploader_id()
    inserted_values = 0
    processed_rows = 0
    skipped_rows = 0

    conn = get_conn()
    _ensure_lab_enrichment(conn)

    try:
        with conn:
            cursor = conn.cursor()
            for raw_row in rows:
                if not isinstance(raw_row, Mapping):
                    skipped_rows += 1
                    continue

                qr = _normalize_qr(
                    raw_row.get("qr_code")
                    or raw_row.get("QR_qrCode")
                    or raw_row.get("sample_id")
                    or raw_row.get("sampleId")
                    or raw_row.get("id")
                    or raw_row.get("ID")
                )
                if not qr:
                    skipped_rows += 1
                    continue

                ordered_keys = [str(key) for key in raw_row.keys()]
                raw_json = json.dumps(raw_row, ensure_ascii=False, default=str)
                row_values = 0

                for original_key, value in raw_row.items():
                    key = str(original_key).strip()
                    key_lower = key.casefold()
                    if key_lower in {
                        "qr_code",
                        "qr_qrcode",
                        "sample_id",
                        "sampleid",
                        "id",
                    }:
                        continue
                    if _is_blank(value):
                        continue
                    if key_lower.startswith("unit") or key_lower.endswith("_unit"):
                        continue
                    if not key:
                        continue

                    unit = _unit_for_parameter(raw_row, key, ordered_keys)
                    _upsert_lab_value(
                        cursor,
                        qr=qr,
                        parameter=key,
                        value=value,
                        unit=unit,
                        uploader_id=uploader_id,
                        raw_json=raw_json,
                    )
                    inserted_values += 1
                    row_values += 1

                    converted = _oxide_to_metal(key, value)
                    if converted is not None:
                        metal_parameter, metal_value = converted
                        _upsert_lab_value(
                            cursor,
                            qr=qr,
                            parameter=metal_parameter,
                            value=metal_value,
                            unit=unit,
                            uploader_id=uploader_id,
                            raw_json=raw_json,
                        )
                        inserted_values += 1
                        row_values += 1

                if row_values:
                    processed_rows += 1
                else:
                    skipped_rows += 1

    except sqlite3.Error as exc:
        current_app.logger.exception("Lab-enrichment API import failed")
        abort(500, description=f"Laboratory import failed: {exc}")

    g._analytics_extra = {
        "upload_type": "lab_enrichment_api",
        "file_name": filename,
        "rows_received": len(rows),
        "rows_processed": processed_rows,
        "rows_skipped": skipped_rows,
        "values_upserted": inserted_values,
    }
    return jsonify(
        {
            "ok": True,
            "rows_received": len(rows),
            "rows_processed": processed_rows,
            "rows_skipped": skipped_rows,
            "values_upserted": inserted_values,
        }
    )


# -----------------------------------------------------------------------------
# Canonical tabular endpoints
# -----------------------------------------------------------------------------


@data_api.get("/canonical/samples")
def canonical_samples():
    _require_canonical_access(public_allowed=True)
    fmt = _parse_format("json", "csv", "geojson")
    limit = None if fmt == "csv" else _parse_int_arg(
        "limit", DEFAULT_PAGE_SIZE, minimum=1, maximum=MAX_PAGE_SIZE
    )
    offset = 0 if fmt == "csv" else _parse_int_arg("offset", 0, minimum=0)

    fields = _parse_fields((request.args.get("fields") or "").strip(), CANONICAL_SAMPLE_COLS)
    if fmt == "geojson":
        for coordinate_field in ("lon", "lat"):
            if coordinate_field not in fields:
                fields.append(coordinate_field)
    order = (request.args.get("order") or "timestamp_utc").strip()
    if order not in CANONICAL_SAMPLE_COLS:
        abort(400, description=f"Unsupported order column: {order}")
    direction = (request.args.get("dir") or "desc").strip().lower()
    if direction not in {"asc", "desc"}:
        abort(400, description="dir must be asc or desc")

    where_sql, params, filter_meta = _canonical_where_from_request()
    columns_sql = ", ".join(fields)

    with get_pg_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        sql = f"""
            SELECT {columns_sql}
            FROM samples
            {where_sql}
            ORDER BY {order} {direction}
        """
        query_params = params[:]
        if limit is not None:
            sql += " LIMIT %s OFFSET %s"
            query_params.extend([limit, offset])
        cur.execute(sql, query_params)
        rows = cur.fetchall()
        cur.execute(f"SELECT COUNT(*) AS c FROM samples {where_sql}", params)
        total = cur.fetchone()["c"]

    _add_analytics("canonical_samples", format=fmt, **filter_meta)
    if fmt == "csv":
        return stream_csv(rows, fields, filename="samples.csv")
    if fmt == "geojson":
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
    _require_canonical_access(public_allowed=True)
    where_sql, params, filter_meta = _canonical_where_from_request()
    with get_pg_conn() as conn, conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM samples {where_sql}", params)
        count = cur.fetchone()[0]
    _add_analytics("canonical_samples_count", **filter_meta)
    return jsonify({"count": count})


@data_api.get("/canonical/sample_images")
def canonical_sample_images():
    _require_canonical_access(public_allowed=True)
    fmt = _parse_format("json", "csv")
    limit = None if fmt == "csv" else _parse_int_arg(
        "limit", DEFAULT_PAGE_SIZE, minimum=1, maximum=MAX_PAGE_SIZE
    )
    offset = 0 if fmt == "csv" else _parse_int_arg("offset", 0, minimum=0)
    fields = _parse_fields((request.args.get("fields") or "").strip(), CANONICAL_IMAGE_COLS)

    where: list[str] = []
    params: list[Any] = []
    sample_id = (request.args.get("sample_id") or "").strip()
    if sample_id:
        where.append("UPPER(sample_id) = UPPER(%s)")
        params.append(sample_id)
    country = (request.args.get("country") or request.args.get("country_code") or "").strip().upper()
    if country:
        where.append("country_code = %s")
        params.append(country)

    from_value, to_value, to_operator = _time_window_from_request()
    if from_value:
        where.append("timestamp_utc >= %s")
        params.append(from_value)
    if to_value:
        where.append(f"timestamp_utc {to_operator} %s")
        params.append(to_value)

    where_sql = "WHERE " + " AND ".join(where) if where else ""
    columns_sql = ", ".join(fields)
    with get_pg_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        sql = f"""
            SELECT {columns_sql}
            FROM sample_images
            {where_sql}
            ORDER BY sample_id, image_id
        """
        query_params = params[:]
        if limit is not None:
            sql += " LIMIT %s OFFSET %s"
            query_params.extend([limit, offset])
        cur.execute(sql, query_params)
        rows = cur.fetchall()
        cur.execute(f"SELECT COUNT(*) AS c FROM sample_images {where_sql}", params)
        total = cur.fetchone()["c"]

    _add_analytics("canonical_sample_images", format=fmt)
    if fmt == "csv":
        return stream_csv(rows, fields, filename="sample_images.csv")
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
    _require_canonical_access(public_allowed=True)
    fmt = _parse_format("json", "csv")
    limit = None if fmt == "csv" else _parse_int_arg(
        "limit", DEFAULT_PAGE_SIZE, minimum=1, maximum=MAX_PAGE_SIZE
    )
    offset = 0 if fmt == "csv" else _parse_int_arg("offset", 0, minimum=0)
    fields = _parse_fields((request.args.get("fields") or "").strip(), CANONICAL_PARAM_COLS)

    sample_id = request.args.get("sample_id")
    country = request.args.get("country") or request.args.get("country_code")
    sample_ids = _sample_ids_for_resource_filters(sample_id=sample_id, country=country)
    dataframe = get_parameters_df(sample_ids)

    parameter_code = (request.args.get("parameter_code") or "").strip()
    if parameter_code:
        dataframe = dataframe.loc[
            dataframe["parameter_code"].fillna("").astype(str).str.casefold()
            == parameter_code.casefold()
        ].copy()

    total = len(dataframe)
    if limit is not None:
        dataframe = dataframe.iloc[offset : offset + limit]
    rows = _dataframe_records(dataframe[fields])

    _add_analytics("canonical_sample_parameters", format=fmt)
    if fmt == "csv":
        return stream_csv(rows, fields, filename="sample_parameters.csv")
    return jsonify(
        {
            "meta": {
                "count": total,
                "limit": limit,
                "offset": offset,
                "fields": fields,
                "filters_applied": {
                    "minimum_value": 0.01,
                    "oxides_excluded": True,
                },
            },
            "data": rows,
        }
    )


@data_api.get("/canonical/sample_biodiversity")
def canonical_sample_biodiversity():
    _require_canonical_access(public_allowed=True)
    fmt = _parse_format("json", "csv")
    limit = None if fmt == "csv" else _parse_int_arg(
        "limit", DEFAULT_PAGE_SIZE, minimum=1, maximum=MAX_PAGE_SIZE
    )
    offset = 0 if fmt == "csv" else _parse_int_arg("offset", 0, minimum=0)
    fields = _parse_fields((request.args.get("fields") or "").strip(), BIODIVERSITY_COLUMNS)

    if request.args.get("otu_id"):
        abort(
            400,
            description=(
                "otu_id is no longer available; the canonical biodiversity API "
                "publishes compact taxonomic-abundance statistics"
            ),
        )

    field_sql = {
        "sample_id": "sta.sample_id",
        "country_code": "s.country_code",
        "marker": "sta.marker",
        "taxonomic_level": "sta.level",
        "taxon": "sta.taxon",
        "read_count": "sta.read_count",
        "relative_abundance_pct": "sta.relative_abundance_pct",
        "analysis_date": "sta.uploaded_at",
        "source_file": "sta.source_file",
        "licence": "COALESCE(NULLIF(s.licence, ''), 'CC-BY-4.0')",
    }
    selected_sql = ", ".join(
        f"{field_sql[field]} AS {field}" for field in fields
    )

    where: list[str] = []
    params: list[Any] = []
    sample_id = (request.args.get("sample_id") or "").strip()
    if sample_id:
        where.append("UPPER(sta.sample_id) = UPPER(%s)")
        params.append(sample_id)
    marker = (request.args.get("marker") or "").strip().upper()
    if marker:
        where.append("sta.marker = %s")
        params.append(marker)
    level = (request.args.get("taxonomic_level") or request.args.get("level") or "").strip()
    if level:
        where.append("LOWER(sta.level) = LOWER(%s)")
        params.append(level)
    taxon = (request.args.get("taxon") or "").strip()
    if taxon:
        where.append("sta.taxon ILIKE %s")
        params.append(f"%{taxon}%")
    country = (request.args.get("country") or request.args.get("country_code") or "").strip().upper()
    if country:
        where.append("s.country_code = %s")
        params.append(country)

    where_sql = "WHERE " + " AND ".join(where) if where else ""
    from_sql = """
        FROM sample_taxon_abundance AS sta
        LEFT JOIN samples AS s ON s.sample_id = sta.sample_id
    """

    with get_pg_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        sql = f"""
            SELECT {selected_sql}
            {from_sql}
            {where_sql}
            ORDER BY sta.sample_id, sta.marker, sta.level,
                     sta.read_count DESC, sta.taxon
        """
        query_params = params[:]
        if limit is not None:
            sql += " LIMIT %s OFFSET %s"
            query_params.extend([limit, offset])
        cur.execute(sql, query_params)
        rows = cur.fetchall()
        cur.execute(f"SELECT COUNT(*) AS c {from_sql} {where_sql}", params)
        total = cur.fetchone()["c"]

    _add_analytics("canonical_sample_biodiversity", format=fmt)
    if fmt == "csv":
        return stream_csv(rows, fields, filename="sample_biodiversity.csv")
    return jsonify(
        {
            "meta": {
                "count": total,
                "limit": limit,
                "offset": offset,
                "fields": fields,
                "aggregation": "compact taxonomic abundance",
            },
            "data": rows,
        }
    )


# -----------------------------------------------------------------------------
# Canonical map endpoints
# -----------------------------------------------------------------------------


def _usable_metals_value(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if text.casefold() in {"", "0", "0.0", "0,0", "nan", "none", "null", "-", "—"}:
        return ""
    return text


@data_api.get("/canonical/map.geojson")
def canonical_map_geojson():
    _require_canonical_access(public_allowed=True)
    limit = _parse_int_arg(
        "limit", DEFAULT_MAP_PAGE_SIZE, minimum=1, maximum=MAX_MAP_PAGE_SIZE
    )
    offset = _parse_int_arg("offset", 0, minimum=0)
    where_sql, params, filter_meta = _canonical_where_from_request(alias="s")

    sample_id = (request.args.get("sample_id") or request.args.get("sampleId") or "").strip()
    if sample_id:
        extra = "UPPER(s.sample_id) = UPPER(%s)"
        where_sql = f"{where_sql} AND {extra}" if where_sql else f"WHERE {extra}"
        params.append(sample_id)

    include_wrong = (request.args.get("include_wrong") or "").strip().lower() in {
        "1", "true", "yes", "on"
    }
    if not include_wrong:
        extra = "(s.qa_status IS NULL OR s.qa_status NOT LIKE %s)"
        where_sql = f"{where_sql} AND {extra}" if where_sql else f"WHERE {extra}"
        params.append("wrong_coordinates%")

    fields = [
        field
        for field in CANONICAL_SAMPLE_COLS
        if field != "collected_by"
    ]
    columns_sql = ", ".join(f"s.{field}" for field in fields)

    with get_pg_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            f"""
            WITH parameter_metals AS (
                SELECT
                    sample_id,
                    string_agg(
                        parameter_code || '=' || value::text ||
                        CASE WHEN COALESCE(uom, '') <> '' THEN ' ' || uom ELSE '' END,
                        '; ' ORDER BY parameter_code
                    ) AS metals_info_params
                FROM sample_parameters
                WHERE UPPER(REPLACE(parameter_code, ' ', '')) NOT IN
                    ('MN2O3','AL2O3','CAO','FE2O3','MGO','SIO2','P2O5','TIO2','K2O','SO3')
                  AND (
                      value IS NULL
                      OR value::text !~ '^[+-]?[0-9]+([.][0-9]+)?$'
                      OR value::text::double precision >= 0.01
                  )
                GROUP BY sample_id
            )
            SELECT {columns_sql}, pm.metals_info_params
            FROM samples AS s
            LEFT JOIN parameter_metals AS pm ON pm.sample_id = s.sample_id
            {where_sql}
            ORDER BY s.timestamp_utc DESC NULLS LAST, s.sample_id
            LIMIT %s OFFSET %s
            """,
            params + [limit, offset],
        )
        rows = cur.fetchall()
        cur.execute(f"SELECT COUNT(*) AS c FROM samples AS s {where_sql}", params)
        total = cur.fetchone()["c"]

    features: list[dict[str, Any]] = []
    for row in rows:
        try:
            lon = float(row.get("lon"))
            lat = float(row.get("lat"))
        except (TypeError, ValueError):
            continue
        if not math.isfinite(lon) or not math.isfinite(lat):
            continue

        properties = dict(row)
        properties.update(
            {
                "GPS_lat": lat,
                "GPS_long": lon,
                "sampleId": properties.get("sample_id"),
                "QR_qrCode": properties.get("sample_id"),
                "collectedAt": properties.get("timestamp_utc"),
                "PH_ph": properties.get("ph"),
                "SOIL_STRUCTURE_structure": (
                    properties.get("soil_structure_en")
                    or properties.get("soil_structure_orig")
                ),
                "SOIL_TEXTURE_texture": (
                    properties.get("soil_texture_en")
                    or properties.get("soil_texture_orig")
                ),
                "SOIL_CONTAMINATION_comments": (
                    properties.get("contamination_other_en")
                    or properties.get("contamination_other_orig")
                ),
                "SOIL_DIVER_earthworms": properties.get("earthworms_count"),
                "SOIL_CONTAMINATION_plastic": properties.get("contamination_plastic"),
                "SOIL_CONTAMINATION_debris": properties.get("contamination_debris"),
                "METALS_info": (
                    _usable_metals_value(properties.get("metals_info_params"))
                    or _usable_metals_value(properties.get("metals_info_en"))
                    or _usable_metals_value(properties.get("metals_info_orig"))
                ),
            }
        )
        qa_status = str(properties.get("qa_status") or "").strip().casefold()
        properties["wrong_coordinates"] = qa_status.startswith("wrong_coordinates")

        features.append(
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [lon, lat]},
                "properties": properties,
            }
        )

    _add_analytics("canonical_map_geojson", **filter_meta)
    return jsonify(
        {
            "type": "FeatureCollection",
            "features": features,
            "meta": {
                "count": total,
                "returned": len(features),
                "limit": limit,
                "offset": offset,
            },
        }
    )


@data_api.get("/canonical/map.count")
def canonical_map_count():
    _require_canonical_access(public_allowed=True)
    where: list[str] = []
    params: list[Any] = []

    country = (
        request.args.get("country_code")
        or request.args.get("country")
        or ""
    ).strip().upper()
    if country:
        where.append("country_code = %s")
        params.append(country)

    date_from = (request.args.get("from") or request.args.get("date_from") or "").strip()
    date_to = (request.args.get("to") or request.args.get("date_to") or "").strip()
    try:
        if date_from:
            from_value, _ = _parse_date_or_datetime(date_from)
            where.append("timestamp_utc >= %s")
            params.append(from_value)
        if date_to:
            to_value, to_operator = _parse_date_or_datetime(date_to, end_bound=True)
            where.append(f"timestamp_utc {to_operator} %s")
            params.append(to_value)
    except ValueError:
        abort(400, description="Invalid date filter")

    ph_min = _parse_float_arg("ph_min")
    ph_max = _parse_float_arg("ph_max")
    if ph_min is not None:
        where.append("ph >= %s")
        params.append(ph_min)
    if ph_max is not None:
        where.append("ph <= %s")
        params.append(ph_max)
    if ph_min is not None and ph_max is not None and ph_min > ph_max:
        abort(400, description="ph_min must not exceed ph_max")

    include_wrong = (request.args.get("include_wrong") or "").strip().lower() in {
        "1", "true", "yes", "on"
    }
    if not include_wrong:
        where.append("(qa_status IS NULL OR qa_status NOT LIKE %s)")
        params.append("wrong_coordinates%")

    where_sql = "WHERE " + " AND ".join(where) if where else ""
    with get_pg_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(f"SELECT COUNT(*) AS n FROM samples {where_sql}", params)
        count = int((cur.fetchone() or {"n": 0})["n"])

    _add_analytics("canonical_map_count")
    return jsonify(
        {
            "count": count,
            "filters": {
                "country_code": country or None,
                "from": date_from or None,
                "to": date_to or None,
                "ph_min": ph_min,
                "ph_max": ph_max,
                "include_wrong": include_wrong,
            },
        }
    )


# -----------------------------------------------------------------------------
# Canonical machine bundles and explicit snapshot creation
# -----------------------------------------------------------------------------


def build_canonical_all_zip_bytes(
    where_sql: str = "",
    params: list[Any] | None = None,
) -> bytes:
    """Backward-compatible wrapper around the shared machine-bundle builder."""
    sample_ids = _canonical_matching_sample_ids(where_sql, params or [])
    return build_machine_bundle(sample_ids=sample_ids).zip_bytes


def build_canonical_zenodo_bundle_zip_bytes(
    where_sql: str = "",
    params: list[Any] | None = None,
) -> bytes:
    """Deprecated compatibility alias for build_canonical_all_zip_bytes()."""
    return build_canonical_all_zip_bytes(where_sql, params)


def _canonical_machine_bundle_response(
    *,
    download_name: str,
    dataset: str,
    api_name: str,
) -> Response:
    require_api_auth()
    where_sql, params, filter_meta = _canonical_where_from_request(alias="s")
    sample_ids = _canonical_matching_sample_ids(where_sql, params)
    bundle = build_machine_bundle(sample_ids=sample_ids)
    g._analytics_extra = {
        "dataset": dataset,
        "api_name": api_name,
        **{key: value for key, value in filter_meta.items() if value is not None},
    }
    return _zip_response(bundle.zip_bytes, download_name)


@data_api.get("/canonical/zenodo_bundle.zip")
def canonical_zenodo_bundle_zip():
    return _canonical_machine_bundle_response(
        download_name="echorepo_bundle.zip",
        dataset="canonical_zenodo_bundle",
        api_name="canonical_zenodo_bundle_zip",
    )


@data_api.get("/canonical/all.zip")
def canonical_all_zip():
    return _canonical_machine_bundle_response(
        download_name="canonical_all.zip",
        dataset="canonical_all",
        api_name="canonical_all_zip",
    )


@data_api.get("/canonical/snapshot/all.zip")
def canonical_snapshot_all_zip():
    """Create and persist an explicit unfiltered canonical machine snapshot."""
    require_api_auth()
    bundle = build_machine_bundle(sample_ids=None)
    version_date = datetime.now(timezone.utc).date().isoformat()
    upload_canonical_zip(bundle.zip_bytes, version_date)

    g._analytics_extra = {
        "dataset": "canonical_all",
        "api_name": "canonical_snapshot_all_zip",
        "version_date": version_date,
    }
    return _zip_response(bundle.zip_bytes, f"canonical_{version_date}.zip")
