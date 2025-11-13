from flask import (
    Blueprint,
    render_template,
    request,
    send_file,
    abort,
    session,
    redirect,
    url_for,
    jsonify,
)
import pandas as pd
from io import BytesIO
import pathlib
from datetime import datetime
import sqlite3
import os
import json
import io
import zipfile  # kept in case you later want to build ZIPs locally
import logging
import csv
import psycopg2
from psycopg2.extras import RealDictCursor

from flask_babel import gettext as _, get_locale

from ..config import settings
from ..auth.decorators import login_required
from ..services.db import query_user_df, query_sample, _ensure_lab_enrichment
from ..services.validation import find_default_coord_rows, select_country_mismatches
from ..utils.table import make_table_html, strip_orig_cols
from echorepo.i18n import build_i18n_labels

# constants for privacy acceptance
PRIVACY_VERSION = "2025-11-echo"  # bump when text changes
PRIVACY_CSV_PATH = os.getenv("PRIVACY_CSV_PATH", "/data/privacy_acceptances.csv")

# blueprint
web_bp = Blueprint("web", __name__)

# --------------------------------------------------------------------------
# --- Postgres helpers -----------------------------------------------------
# --------------------------------------------------------------------------
def _pg_conn():
    return psycopg2.connect(
        host=os.getenv("DB_HOST_INSIDE", "echorepo-postgres"),
        port=int(os.getenv("DB_PORT_INSIDE", "5432")),
        dbname=os.getenv("DB_NAME", "echorepo"),
        user=os.getenv("DB_USER", "echorepo"),
        password=os.getenv("DB_PASSWORD", "echorepo-pass"),
    )


# --------------------------------------------------------------------------
# --- Privacy acceptance helpers -------------------------------------------
# --------------------------------------------------------------------------
def _env_true(name: str, default: bool = True) -> bool:
    val = os.getenv(name)
    if val is None:
        return default
    return str(val).strip().lower() in {"1", "true", "yes", "on", "y", "t"}

def _get_repo_user_id_from_db() -> str | None:
    """
    Return the userId we store in the samples table (the same we use for the
    survey r= param). Falls back to the display user_key if nothing else is found.
    """
    # this is the same way you find the "logical" user
    user_key = session.get("user") or session.get("kc", {}).get("profile", {}).get("email")
    if not user_key:
        return None

    df = query_user_df(user_key)
    if df is None or df.empty:
        return user_key  # last resort

    for col in ("userId", "user_id", "kc_user_id"):
        if col in df.columns:
            val = df[col].dropna().astype(str).iloc[0].strip()
            if val:
                return val

    return user_key

def _current_user_id() -> str | None:
    kc_profile = (session.get("kc") or {}).get("profile") or {}

    # prefer a stable internal KC id
    if kc_profile.get("id"):
        return kc_profile["id"]
    if kc_profile.get("sub"):
        return kc_profile["sub"]

    # then fall back to whatever you used before
    if session.get("user"):
        return session["user"]

    # last resort: email
    if kc_profile.get("email"):
        return kc_profile["email"]

    return None

def _has_accepted_privacy(user_id: str) -> bool:
    if not user_id:
        return False
    # fast path: we can also store in session
    if session.get("privacy_accepted_version") == PRIVACY_VERSION:
        return True

    if not os.path.exists(PRIVACY_CSV_PATH):
        return False

    try:
        with open(PRIVACY_CSV_PATH, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if (
                    row.get("user_id") == user_id
                    and row.get("version") == PRIVACY_VERSION
                ):
                    # cache in session
                    session["privacy_accepted_version"] = PRIVACY_VERSION
                    return True
    except Exception:
        return False

    return False


def _append_privacy_acceptance(user_id: str):
    dir_name = os.path.dirname(PRIVACY_CSV_PATH)
    if dir_name:
        os.makedirs(dir_name, exist_ok=True)

    file_exists = os.path.exists(PRIVACY_CSV_PATH)
    with open(PRIVACY_CSV_PATH, "a", newline="", encoding="utf-8") as f:
        fieldnames = ["user_id", "accepted_at", "version"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        writer.writerow(
            {
                "user_id": user_id,
                "accepted_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
                "version": PRIVACY_VERSION,
            }
        )
    session["privacy_accepted_version"] = PRIVACY_VERSION

# --------------------------------------------------------------------------
# SoSci survey helper
# --------------------------------------------------------------------------
def _build_sosci_url(user_id: str | None) -> str | None:
    if not user_id:
        return None

    sosci_map = {
        "de": "deu",
        "el": "gre",
        "en": "eng",
        "es": "spa",
        "fi": "fin",
        "it": "ita",
        "po": "pol",
        "pt": "por",
        "ro": "rum",
    }

    current = str(get_locale() or "en")
    current_base = current.split("_", 1)[0].split("-", 1)[0].lower()

    if current_base == "en":
        browser = request.accept_languages.best_match(
            ["de", "it", "fi", "el", "es", "po", "pt", "ro", "en"]
        )
        if browser:
            current_base = browser.split("-", 1)[0].lower()

    sosci_lang = sosci_map.get(current_base, "eng")

    base = getattr(
        settings,
        "SURVEY_BASE_URL",
        "https://www.soscisurvey.de/default",
    )

    sep = "&" if "?" in base else "?"
    return f"{base}{sep}l={sosci_lang}&r={user_id}"


# --------------------------------------------------------------------------
# labels for front-end
# --------------------------------------------------------------------------
def _js_base_labels() -> dict:
    return {
        "privacyRadius": _("Privacy radius (~±{km} km)"),
        "soilPh": _("Soil pH"),
        "acid": _("Acidic (≤5.5)"),
        "slightlyAcid": _("Slightly acidic (5.5–6.5)"),
        "neutral": _("Neutral (6.5–7.5)"),
        "slightlyAlkaline": _("Slightly alkaline (7.5–8.5)"),
        "alkaline": _("Alkaline (≥8.5)"),
        "yourSamples": _("Your samples"),
        "otherSamples": _("Other samples"),
        "export": _("Export"),
        "clear": _("Clear"),
        "exportFiltered": _("Export filtered ({n})"),
        "date": _("Date"),
        "qr": _("QR code"),
        "ph": _("pH"),
        "colour": _("Colour"),
        "texture": _("Texture"),
        "structure": _("Structure"),
        "earthworms": _("Earthworms"),
        "plastic": _("Plastic"),
        "debris": _("Debris"),
        "contamination": _("Contamination"),
        "metals": _("Metals"),
        "drawRectangle": _("Draw a rectangle"),
        "cancelDrawing": _("Cancel drawing"),
        "cancel": _("Cancel"),
        "deleteLastPoint": _("Delete last point"),
        "drawRectangleHint": _("Click and drag to draw a rectangle."),
        "releaseToFinish": _("Release mouse to finish drawing."),
    }


# --------------------------------------------------------------------------
# helpers for lab upload / QR
# --------------------------------------------------------------------------
def _normalize_qr(raw: str) -> str:
    if not raw:
        return ""
    raw = str(raw).strip()
    if raw.upper().startswith("ECHO-"):
        raw = raw[5:]
    if "-" not in raw and len(raw) >= 5:
        raw = raw[:4] + "-" + raw[4:]
    return raw


def _user_has_metals(df: pd.DataFrame) -> bool:
    if df is None or df.empty:
        return False

    candidate_cols = ("METALS_info", "lab_METALS_info", "METALS", "metals")
    for col in candidate_cols:
        if col not in df.columns:
            continue

        series = (
            df[col]
            .fillna("")
            .astype(str)
            .str.replace("<br>", ";", regex=False)
            .str.strip()
        )
        series = series.replace({"nan": "", "None": "", "0": "", "0.0": "", "NaN": ""})
        if series.str.contains("=", regex=False).any():
            return True

    return False


# --------------------------------------------------------------------------
# Postgres helpers
# -------------------------------------------------------------------------
def get_pg_conn():
    return psycopg2.connect(
        host=os.getenv("DB_HOST_INSIDE", "echorepo-postgres"),
        port=int(os.getenv("DB_PORT_INSIDE", "5432")),
        dbname=os.getenv("DB_NAME", "echorepo"),
        user=os.getenv("DB_USER", "echorepo"),
        password=os.getenv("DB_PASSWORD", "echorepo-pass"),
    )

# --------------------------------------------------------------------------
# MinIO helpers + canonical download routes (proxy through Flask)
# --------------------------------------------------------------------------
try:
    from minio import Minio
except ImportError:
    Minio = None


def _get_minio_client():
    """
    Build a MinIO client from env / app config.
    We use it server-side to fetch objects and stream them to the user.
    """
    if Minio is None:
        return None

    endpoint = os.getenv("MINIO_ENDPOINT", "minio:9000")
    access_key = os.getenv("MINIO_ACCESS_KEY") or os.getenv("MINIO_ROOT_USER") or ""
    secret_key = os.getenv("MINIO_SECRET_KEY") or os.getenv("MINIO_ROOT_PASSWORD") or ""
    secure = False
    if endpoint.startswith("https://"):
        secure = True
        endpoint = endpoint[len("https://") :]
    elif endpoint.startswith("http://"):
        secure = False
        endpoint = endpoint[len("http://") :]

    if not access_key or not secret_key:
        return None

    return Minio(
        endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=secure,
    )


def _stream_minio_canonical(obj_name: str):
    """
    Instead of redirecting the browser to MinIO (which you don't expose),
    we, the Flask app, download the object from MinIO and send it to the client.
    """
    client = _get_minio_client()
    bucket = os.getenv("MINIO_BUCKET", "echorepo-uploads")

    if client is None:
        abort(503, description="MinIO not configured on this instance")

    key = f"canonical/{obj_name}"
    try:
        resp = client.get_object(bucket, key)
    except Exception as e:
        abort(404, description=f"object not found in MinIO: {key}, error: {e}")

    # read into memory, then close the MinIO response
    data = resp.read()
    resp.close()
    resp.release_conn()

    mimetype = "application/zip" if obj_name.endswith(".zip") else "text/csv"
    return send_file(
        io.BytesIO(data),
        mimetype=mimetype,
        as_attachment=True,
        download_name=obj_name,
    )

@web_bp.post("/privacy/accept")
@login_required
def privacy_accept():
    if not _env_true("PRIVACY_GATE", True):  # short-circuit the accept route when off
        return redirect(url_for("web.home"))
    repo_user_id = _get_repo_user_id_from_db()
    if not repo_user_id:
        abort(400, description="Cannot determine userId from database")

    # make sure the dir exists (handle the case when path has no dir part)
    dir_name = os.path.dirname(PRIVACY_CSV_PATH)
    if dir_name:
      os.makedirs(dir_name, exist_ok=True)

    _append_privacy_acceptance(repo_user_id)
    return redirect(url_for("web.home"))

@web_bp.route("/download/canonical/samples.csv")
@login_required
def download_canonical_samples():
    return _stream_minio_canonical("samples.csv")


@web_bp.route("/download/canonical/sample_images.csv")
@login_required
def download_canonical_sample_images():
    return _stream_minio_canonical("sample_images.csv")


@web_bp.route("/download/canonical/sample_parameters.csv")
@login_required
def download_canonical_sample_parameters():
    return _stream_minio_canonical("sample_parameters.csv")


@web_bp.route("/download/canonical/all.zip")
@login_required
def download_canonical_zip():
    return _stream_minio_canonical("all.zip")


# --------------------------------------------------------------------------
# main UI
# --------------------------------------------------------------------------
@web_bp.get("/", endpoint="home")
@login_required
def home():
    # who is this
    user_key = session.get("user") or session.get("kc", {}).get("profile", {}).get("email")
    if not user_key:
        return redirect(url_for("auth.login"))

    # privacy gate
    privacy_user_id = _get_repo_user_id_from_db()
    privacy_gate_on = _env_true("PRIVACY_GATE", True)  # default ON unless overridden
    needs_privacy = privacy_gate_on and not _has_accepted_privacy(privacy_user_id or "")

    # user data
    df = query_user_df(user_key)
    i18n = {"labels": build_i18n_labels(_js_base_labels())}

    # try to get "real" internal user id from data
    kc_user_id = None
    if not df.empty:
        for col in ("userId", "user_id", "kc_user_id"):
            if col in df.columns:
                kc_user_id = df[col].dropna().astype(str).iloc[0].strip()
                break

    # survey url
    survey_user_id = kc_user_id or user_key
    survey_url = _build_sosci_url(survey_user_id)

    # only show survey if user actually has metals-like data
    has_metals = _user_has_metals(df)
    show_survey = bool(survey_url) and has_metals

    # EMPTY CASE ------------------------------------------------------------
    if df.empty:
        return render_template(
            "results.html",
            issue_count=0,
            user_key=user_key,
            kc_user_id=kc_user_id,
            columns=[],
            table_html="<p>No data available for this user.</p>",
            jitter_m=int(settings.MAX_JITTER_METERS),
            lat_col=settings.LAT_COL,
            lon_col=settings.LON_COL,
            I18N=i18n,
            survey_url=survey_url,
            show_survey=False,
            # privacy
            needs_privacy=needs_privacy,
            privacy_version=PRIVACY_VERSION,
        )

    # NON-EMPTY: data issues ------------------------------------------------
    defaults = find_default_coord_rows(df)
    mism = select_country_mismatches(df)
    issue_count = len(defaults) + len(mism)

    # HTML copy — prettify timestamp column
    df_html = df.copy()
    if "fs_createdAt" in df_html.columns:
        df_html["fs_createdAt"] = (
            df_html["fs_createdAt"]
            .fillna("")
            .astype(str)
            .str.split(".")
            .str[0]
            .str.replace("T", " ", regex=False)
            .str.replace("Z", "", regex=False)
        )
        cols = list(df_html.columns)
        if "fs_createdAt" in cols:
            if "sampleId" in cols:
                cols.insert(cols.index("sampleId") + 1, cols.pop(cols.index("fs_createdAt")))
            else:
                cols.insert(0, cols.pop(cols.index("fs_createdAt")))
            df_html = df_html[cols]

    table_html = make_table_html(df_html)

    return render_template(
        "results.html",
        issue_count=issue_count,
        user_key=user_key,
        kc_user_id=kc_user_id,
        columns=list(df.columns),
        table_html=table_html,
        jitter_m=int(settings.MAX_JITTER_METERS),
        lat_col=settings.LAT_COL,
        lon_col=settings.LON_COL,
        I18N=i18n,
        survey_url=survey_url,
        show_survey=show_survey,
        # privacy flags for the modal
        needs_privacy=needs_privacy,
        privacy_version=PRIVACY_VERSION,
    )


# --------------------------------------------------------------------------
# misc routes
# --------------------------------------------------------------------------
@web_bp.get("/i18n/labels")
@login_required
def i18n_labels():
    return jsonify({"labels": build_i18n_labels(_js_base_labels())})

@web_bp.get("/labels")
@login_required
def labels_json():
    loc_raw = request.args.get("locale") or str(get_locale() or "en")
    loc = _canon_locale(loc_raw)
    payload = {
        "labels": _make_labels(loc),               # key → text (already merges catalog + overrides)
        "by_msgid": get_overrides_msgid(loc) or {} # msgid → override text (only overrides)
    }
    return jsonify(payload)

@web_bp.post("/download/csv")
@login_required
def download_csv():
    user_key = (request.form.get("user_key") or "").strip()
    if not user_key:
        abort(400)
    df = query_user_df(user_key)
    if df.empty:
        abort(404)
    df = strip_orig_cols(df)
    buf = BytesIO()
    df.to_csv(buf, index=False)
    buf.seek(0)
    return send_file(
        buf,
        as_attachment=True,
        download_name=f"{user_key}_data.csv",
        mimetype="text/csv",
    )


@web_bp.post("/download/xlsx")
@login_required
def download_xlsx():
    user_key = (request.form.get("user_key") or "").strip()
    if not user_key:
        abort(400)
    df = query_user_df(user_key)
    if df.empty:
        abort(404)
    df = strip_orig_cols(df)
    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="xlsxwriter") as w:
        df.to_excel(w, index=False, sheet_name="data")
    buf.seek(0)
    return send_file(
        buf,
        as_attachment=True,
        download_name=f"{user_key}_data.xlsx",
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


@web_bp.get("/download/all_csv")
@login_required
def download_all_csv():
    p = pathlib.Path(settings.INPUT_CSV)
    if not p.exists() or not p.is_file():
        abort(404, description="Full CSV not found on server.")

    pii_cols = {
        "userId",
        "user_id",
        "email",
        getattr(settings, "USER_KEY_COLUMN", None),
    }
    pii_cols = {c.lower() for c in pii_cols if c}

    try:
        df_all = pd.read_csv(p, dtype=str, keep_default_na=False, low_memory=False)
        df_all = strip_orig_cols(df_all)
        drop_these = [col for col in df_all.columns if col.lower() in pii_cols]
        if drop_these:
            df_all = df_all.drop(columns=drop_these, errors="ignore")

        buf = BytesIO()
        df_all.to_csv(buf, index=False)
        buf.seek(0)
        return send_file(
            buf,
            as_attachment=True,
            download_name="echorepo_all_samples.csv",
            mimetype="text/csv",
        )
    except Exception:
        import csv

        buf = BytesIO()
        with p.open("r", encoding="utf-8", newline="") as f_in:
            reader = csv.reader(f_in)
            rows = list(reader)
            if not rows:
                abort(404, description="CSV is empty")

            header = rows[0]
            keep_idx = []
            for i, name in enumerate(header):
                if name.lower() in pii_cols:
                    continue
                keep_idx.append(i)

            writer = csv.writer(buf)
            writer.writerow([header[i] for i in keep_idx])
            for row in rows[1:]:
                writer.writerow([row[i] for i in keep_idx])

        buf.seek(0)
        return send_file(
            buf,
            as_attachment=True,
            download_name="echorepo_all_samples.csv",
            mimetype="text/csv",
        )


@web_bp.get("/download/sample_csv")
@login_required
def download_sample_csv():
    sample_id = (request.args.get("sampleId") or "").strip()
    if not sample_id:
        abort(400, description="sampleId is required")
    df = query_sample(sample_id)
    if df.empty:
        abort(404, description="Sample not found")

    user_key = session.get("user")
    is_owner = False
    try:
        if settings.USER_KEY_COLUMN in df.columns and (df[settings.USER_KEY_COLUMN] == user_key).any():
            is_owner = True
        if "userId" in df.columns and (df["userId"] == user_key).any():
            is_owner = True
    except Exception:
        pass

    if not is_owner:
        for pii in ("email", "userId"):
            if pii in df.columns:
                df = df.drop(columns=[pii])

    buf = BytesIO()
    df.to_csv(buf, index=False)
    buf.seek(0)
    return send_file(
        buf,
        as_attachment=True,
        download_name=f"sample_{sample_id}.csv",
        mimetype="text/csv",
    )


# --------------------------------------------------------------------------
# lab upload
# --------------------------------------------------------------------------
@web_bp.post("/lab-import")
@login_required
def lab_import():
    file = request.files.get("file")
    if not file:
        abort(400, description="No file uploaded")

    kc_profile = (session.get("kc") or {}).get("profile") or {}
    uploader_id = (
        kc_profile.get("id")
        or kc_profile.get("sub")
        or session.get("user")
        or "unknown"
    )

    filename = file.filename or ""
    try:
        if filename.lower().endswith(".xlsx"):
            df = pd.read_excel(file)
        else:
            try:
                df = pd.read_csv(file, sep="\t")
            except Exception:
                file.stream.seek(0)
                df = pd.read_csv(file)
    except Exception as e:
        abort(400, description=f"Cannot read file: {e}")

    if df.empty:
        abort(400, description="Uploaded file has no rows")

    db_path = settings.SQLITE_PATH
    if not os.path.exists(db_path):
        abort(500, description=f"SQLite database not found at {db_path}")

    conn = sqlite3.connect(db_path)
    _ensure_lab_enrichment(conn)
    cur = conn.cursor()
    cur.execute(
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

    fieldnames = list(df.columns)

    for _, row in df.iterrows():
        raw_dict = row.to_dict()
        qr = _normalize_qr(raw_dict.get("ID") or raw_dict.get("id") or "")
        if not qr:
            continue

        clean_raw = {k: ("" if pd.isna(v) else v) for k, v in raw_dict.items()}
        raw_json = json.dumps(clean_raw, ensure_ascii=False)

        for idx, col in enumerate(fieldnames):
            if col in ("ID", "id"):
                continue
            val = row.get(col)
            if pd.isna(val) or val == "":
                continue
            if str(col).lower().startswith("unit"):
                continue

            param = str(col).strip()
            unit = ""

            if idx + 1 < len(fieldnames):
                maybe_unit_col = fieldnames[idx + 1]
                if str(maybe_unit_col).lower().startswith("unit"):
                    uval = row.get(maybe_unit_col)
                    if not pd.isna(uval):
                        unit = str(uval).strip()

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

    conn.commit()
    conn.close()

    return redirect(url_for("web.home"))


@web_bp.get("/lab-upload")
@login_required
def lab_upload():
    return render_template("lab_upload.html")


@web_bp.post("/lab-upload")
@login_required
def lab_upload_post():
    file = request.files.get("file")
    if not file:
        abort(400, "No file")
    return redirect(url_for("web.home"))

@web_bp.route("/search", endpoint="search_samples", methods=["GET"])
@login_required
def search_samples():
    # ----- read filters from querystring -----
    criteria = {
        "sample_id": (request.args.get("sample_id") or "").strip(),
        "country_code": (request.args.get("country_code") or "").strip().upper(),
        "ph_min": (request.args.get("ph_min") or "").strip(),
        "ph_max": (request.args.get("ph_max") or "").strip(),
        "date_from": (request.args.get("date_from") or "").strip(),
        "date_to": (request.args.get("date_to") or "").strip(),
    }
    fmt = (request.args.get("format") or "").lower()

    # pagination (for HTML)
    page = max(int(request.args.get("page", 1)), 1)
    per_page = 50
    offset = (page - 1) * per_page

    # helper to build WHERE + params
    def _build_where(criteria):
        where = ["1=1"]
        params = []
        if criteria["sample_id"]:
            where.append("sample_id ILIKE %s")
            params.append(f"%{criteria['sample_id']}%")
        if criteria["country_code"]:
            where.append("country_code = %s")
            params.append(criteria["country_code"])
        if criteria["ph_min"]:
            where.append("ph >= %s")
            params.append(float(criteria["ph_min"]))
        if criteria["ph_max"]:
            where.append("ph <= %s")
            params.append(float(criteria["ph_max"]))
        if criteria["date_from"]:
            where.append("timestamp_utc >= %s")
            params.append(criteria["date_from"])
        if criteria["date_to"]:
            where.append("timestamp_utc <= %s")
            params.append(criteria["date_to"])
        return " AND ".join(where), params

    where_sql, params = _build_where(criteria)

    # ---- special case: export as ZIP ----
    if fmt == "zip":
        import csv, io, zipfile

        # we need *all* matching sample_ids (no limit)
        sql_all = f"""
            SELECT sample_id, timestamp_utc, country_code, ph, lat, lon, collected_by
            FROM samples
            WHERE {where_sql}
            ORDER BY timestamp_utc DESC
        """

        with get_pg_conn() as conn, conn.cursor() as cur:
            # 1) get all matching samples
            cur.execute(sql_all, params)
            samples = cur.fetchall()
            sample_ids = [row[0] for row in samples if row[0]]

            # to avoid "IN ()" when no results
            if not sample_ids:
                # return empty zip
                mem = io.BytesIO()
                with zipfile.ZipFile(mem, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
                    zf.writestr("samples_filtered.csv", "sample_id,timestamp_utc,country_code,ph,lat,lon,collected_by\n")
                    zf.writestr("sample_images_filtered.csv", "sample_id,country_code,image_id,image_url,image_description_orig,image_description_en,collected_by,timestamp_utc,licence\n")
                    zf.writestr("sample_parameters_filtered.csv", "sample_id,country_code,parameter_code,parameter_name,value,uom,analysis_method,analysis_date,lab_id,created_by,licence,parameter_uri\n")
                mem.seek(0)
                return send_file(
                    mem,
                    as_attachment=True,
                    download_name="search_export.zip",
                    mimetype="application/zip",
                )

            # 2) fetch images for those sample_ids
            sql_imgs = """
                SELECT sample_id, country_code, image_id, image_url,
                       image_description_orig, image_description_en,
                       collected_by, timestamp_utc, licence
                FROM sample_images
                WHERE sample_id = ANY(%s)
                ORDER BY sample_id, image_id
            """
            cur.execute(sql_imgs, (sample_ids,))
            images = cur.fetchall()

            # 3) fetch parameters for those sample_ids
            sql_params = """
                SELECT sample_id, country_code, parameter_code, parameter_name,
                       value, uom, analysis_method, analysis_date,
                       lab_id, created_by, licence, parameter_uri
                FROM sample_parameters
                WHERE sample_id = ANY(%s)
                ORDER BY sample_id, parameter_code
            """
            cur.execute(sql_params, (sample_ids,))
            params_rows = cur.fetchall()

        # build zip in-memory
        mem = io.BytesIO()
        with zipfile.ZipFile(mem, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
            # samples_filtered.csv
            out1 = io.StringIO()
            w1 = csv.writer(out1)
            w1.writerow(["sample_id", "timestamp_utc", "country_code", "ph", "lat", "lon", "collected_by"])
            for r in samples:
                w1.writerow(r)
            zf.writestr("samples_filtered.csv", out1.getvalue())

            # sample_images_filtered.csv
            out2 = io.StringIO()
            w2 = csv.writer(out2)
            w2.writerow(["sample_id", "country_code", "image_id", "image_url",
                         "image_description_orig", "image_description_en",
                         "collected_by", "timestamp_utc", "licence"])
            for r in images:
                w2.writerow(r)
            zf.writestr("sample_images_filtered.csv", out2.getvalue())

            # sample_parameters_filtered.csv
            out3 = io.StringIO()
            w3 = csv.writer(out3)
            w3.writerow(["sample_id", "country_code", "parameter_code", "parameter_name",
                         "value", "uom", "analysis_method", "analysis_date",
                         "lab_id", "created_by", "licence", "parameter_uri"])
            for r in params_rows:
                w3.writerow(r)
            zf.writestr("sample_parameters_filtered.csv", out3.getvalue())

        mem.seek(0)
        return send_file(
            mem,
            as_attachment=True,
            download_name="search_export.zip",
            mimetype="application/zip",
        )

    # ---- normal HTML search with pagination ----
    total_rows = 0
    rows = []

    with get_pg_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        # count
        cur.execute(f"SELECT COUNT(*) FROM samples WHERE {where_sql}", params)
        total_rows = cur.fetchone()["count"]

        # page
        cur.execute(
            f"""
            SELECT sample_id, timestamp_utc, country_code, ph, lat, lon, collected_by
            FROM samples
            WHERE {where_sql}
            ORDER BY timestamp_utc DESC
            LIMIT %s OFFSET %s
            """,
            params + [per_page, offset],
        )
        rows = cur.fetchall()

    total_pages = max((total_rows + per_page - 1) // per_page, 1)

    return render_template(
        "search.html",
        criteria=criteria,
        rows=rows,
        page=page,
        total_pages=total_pages,
        total_rows=total_rows,
    )
