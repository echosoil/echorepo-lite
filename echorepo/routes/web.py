import csv
import logging
import os
import re
import math
from datetime import date, datetime, timedelta
from io import BytesIO
from pathlib import Path
from urllib.parse import urlencode

import pandas as pd
from flask import (
    Blueprint,
    abort,
    g,
    jsonify,
    redirect,
    render_template,
    request,
    send_file,
    session,
    url_for,
)
from flask_babel import get_locale
from psycopg2.extras import RealDictCursor

from echorepo.services.i18n_labels import make_labels
from echorepo.services.i18n_overrides import (
    _canon_locale,
    get_overrides_msgid,
)

from ..auth.decorators import login_required
from ..config import settings
from ..services.db import get_pg_conn, query_user_df
from ..services.lab_permissions import can_upload_lab_data
from ..services.storage.minio import (
    StorageError,
    StorageNotConfigured,
    StorageObjectNotFound,
    get_canonical_object,
    latest_canonical_snapshot_date,
    object_exists,
    upload_canonical_csvs,
    upload_canonical_zip,
)
from ..services.validation import find_default_coord_rows, select_country_mismatches
from ..services.biodiversity_import import (
    _looks_like_biodiversity_file,
    _import_biodiversity_streaming,
    )

from ..utils.table import make_table_html, strip_orig_cols

from ..services.canonical_exports import (
    SAMPLE_COLUMNS as CANONICAL_SAMPLE_COLS,
    build_filtered_bundle,
    build_live_csv,
    build_snapshot_bundle,
    looks_like_oxide as _looks_like_oxide,
)
from ..services.lab_uploads import (
    InvalidLabUpload,
    LabImportError,
    import_metals_file,
)

# Zenodo sync log path (can also be set via env var)
ZENODO_LOG_DEFAULT = os.getenv("ZENODO_LOG_FILE", "/data/zenodo_sync_log.csv")

ZENODO_DATASET_URL = (
    os.getenv("ZENODO_DATASET_URL")
    or "https://zenodo.org/records/19723699/files/zenodo_bundle.zip?download=1"
)

# constants for privacy acceptance
PRIVACY_VERSION = "2025-11-echo"  # bump when text changes
PRIVACY_CSV_PATH = os.getenv("PRIVACY_CSV_PATH", "/data/privacy_acceptances.csv")

# blueprint
web_bp = Blueprint("web", __name__)

def _current_ui_i18n() -> dict:
    """
    Build the current UI translation payload.

    Precedence inside make_labels():
      1. compiled PO/MO catalogue
      2. msgid overrides
      3. JS key overrides
    """
    loc = _canon_locale(
        str(get_locale() or "en")
    )

    return {
        "labels": make_labels(loc),
        "by_msgid": (
            get_overrides_msgid(loc)
            or {}
        ),
        "locale": loc,
    }


def get_map_allowed_countries():
    raw = os.getenv("MAP_ALLOWED_COUNTRIES", "")

    return sorted({
        code.strip().upper()
        for code in raw.split(",")
        if code.strip()
    })


def _drop_oxide_columns_from_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove columns whose names look like oxides (directly or as a suffix).
    Works for things like 'SiO2', 'lab_SiO2', 'value_Fe2O3_(%)', etc.
    """
    if df is None or df.empty:
        return df
    to_drop = []
    for col in df.columns:
        if "photo" in col.lower():
            continue
        name = str(col)
        # check whole name
        if _looks_like_oxide(name):
            to_drop.append(col)
            continue
        # also check the last token after separators
        last = re.split(r"[_\s/\-]+", name)[-1]
        if _looks_like_oxide(last):
            to_drop.append(col)
    if to_drop:
        df = df.drop(columns=to_drop, errors="ignore")
    return df




# --------------------------------------------------------------------------
# --- Privacy acceptance helpers -------------------------------------------
# --------------------------------------------------------------------------
def _env_true(name: str, default: bool = True) -> bool:
    val = os.getenv(name)
    if val is None:
        return default
    return str(val).strip().lower() in {"1", "true", "yes", "on", "y", "t"}


def _repo_user_id_from_df(df: pd.DataFrame, fallback: str | None) -> str | None:
    """Return the first non-empty internal user ID, or the login identifier."""
    if df is not None and not df.empty:
        for col in ("userId", "user_id", "kc_user_id"):
            if col not in df.columns:
                continue
            values = df[col].dropna().astype(str).str.strip()
            values = values[values != ""]
            if not values.empty:
                return values.iloc[0]
    return fallback


def _current_authenticated_identifiers() -> set[str]:
    """Identifiers for the current user, for ownership-only display fields."""
    profile = ((session.get("kc") or {}).get("profile") or {})
    possible = {
        session.get("user"),
        profile.get("email"),
        profile.get("username"),
        profile.get("preferred_username"),
        profile.get("sub"),
        profile.get("id"),
    }
    return {
        str(value).strip().casefold()
        for value in possible
        if value is not None and str(value).strip()
    }


def _get_repo_user_id_from_db() -> str | None:
    """Resolve the repository user ID for the current authenticated user."""
    user_key = session.get("user") or (session.get("kc") or {}).get("profile", {}).get("email")
    if not user_key:
        return None
    return _repo_user_id_from_df(query_user_df(user_key), user_key)


def _has_accepted_privacy(user_id: str) -> bool:
    if not user_id:
        return False
    # fast path: we can also store in session
    if (
        session.get("privacy_accepted_version") == PRIVACY_VERSION
        and session.get("privacy_accepted_user_id") == user_id
    ):
        return True

    if not os.path.exists(PRIVACY_CSV_PATH):
        return False

    try:
        with open(PRIVACY_CSV_PATH, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get("user_id") == user_id and row.get("version") == PRIVACY_VERSION:
                    # cache in session
                    session["privacy_accepted_version"] = PRIVACY_VERSION
                    session["privacy_accepted_user_id"] = user_id
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
    session["privacy_accepted_user_id"] = user_id


# --------------------------------------------------------------------------
# SoSci survey helper
# --------------------------------------------------------------------------
def _user_has_lab_results(df_samples: pd.DataFrame) -> bool:
    """
    Return True if any of this user's samples have rows in sample_parameters.
    Works even if the samples DF doesn't contain any metals summary columns.
    """
    try:
        if df_samples is None or df_samples.empty:
            return False

        # Find a column that looks like the canonical sample id
        sid_col = None
        for cand in ("sampleId", "sample_id", "Sample", "sampleid"):
            if cand in df_samples.columns:
                sid_col = cand
                break
        if not sid_col:
            return False

        sample_ids = df_samples[sid_col].dropna().astype(str).unique().tolist()
        if not sample_ids:
            return False

        # Fast existence check in Postgres
        with get_pg_conn() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM sample_parameters WHERE sample_id = ANY(%s) LIMIT 1",
                (sample_ids,),
            )
            return cur.fetchone() is not None
    except Exception as e:
        logging.getLogger(__name__).warning("lab presence check failed: %s", e)
        return False


def _survey_timeout_reached(
    df: pd.DataFrame,
    months: int = 3,
) -> bool:
    """
    Return True when at least `months` calendar months have passed
    since the user's first sample.

    Prefer fs_createdAt because it represents when the sample entered
    ECHOREPO. Fall back to collectedAt / timestamp_utc for older data.
    """
    if df is None or df.empty:
        return False

    for col in (
        "fs_createdAt",
        "collectedAt",
        "timestamp_utc",
    ):
        if col not in df.columns:
            continue

        dates = pd.to_datetime(
            df[col],
            errors="coerce",
            utc=True,
        ).dropna()

        if dates.empty:
            continue

        first_sample_at = dates.min()

        survey_available_at = (
            first_sample_at
            + pd.DateOffset(months=months)
        )

        now = pd.Timestamp.now(tz="UTC")

        return now >= survey_available_at

    # We could not establish when the user's first sample appeared.
    return False


def _dataframe_has_metals(df: pd.DataFrame) -> bool:
    if df is None or df.empty:
        return False
    candidates = [
        "METALS_info",
        "lab_METALS_info",
        "METALS",
        "metals",
        "metals_info",
        "metals_info_en",
        "metals_info_orig",
        "elemental_concentrations",
        "elemental_concentrations_en",
        "elemental_concentrations_orig",
    ]
    for c in df.columns:
        cl = str(c).lower()
        if c in candidates or "metals" in cl or ("elemental" in cl and "concentration" in cl):
            s = (
                df[c]
                .fillna("")
                .astype(str)
                .str.replace("<br>", ";", regex=False)
                .str.strip()
                .replace({"nan": "", "None": "", "NaN": "", "0": "", "0.0": ""})
            )
            if s.str.contains("=", regex=False).any():
                return True
    return False


def _build_sosci_url(user_id: str | None) -> str | None:
    if not user_id:
        return None

    current = str(get_locale() or "en")
    current_base = current.split("_", 1)[0].split("-", 1)[0].lower()

    if current_base == "en":
        browser = request.accept_languages.best_match(
            ["de", "it", "fi", "el", "es", "po", "pt", "ro", "en"]
        )
        if browser:
            current_base = browser.split("-", 1)[0].lower()

    base = getattr(
        settings,
        "SURVEY_BASE_URL",
        "https://www.soscisurvey.de/default",
    )

    sep = "&" if "?" in base else "?"
    return f"{base}{sep}{urlencode({'r': user_id})}"




# --------- Biodiversity helpers



# --------- Search helpers
def _coordinate_approved_csv_path() -> Path:
    env_path = os.getenv("COORDINATE_APPROVED_CSV")
    if env_path:
        return Path(env_path)

    candidates = [
        Path("/data/coordinate_check_approved.csv"),
        Path("/app/data/coordinate_check_approved.csv"),
        Path.cwd() / "data" / "coordinate_check_approved.csv",
    ]

    for p in candidates:
        if p.exists():
            return p

    return Path("/data/coordinate_check_approved.csv")


def _load_coordinate_approvals() -> pd.DataFrame:
    path = _coordinate_approved_csv_path()

    if not path.exists():
        return pd.DataFrame(columns=["sample_id", "approved_by", "approved_at", "comment"])

    try:
        df = pd.read_csv(path, dtype=str, keep_default_na=False)
    except Exception:
        return pd.DataFrame(columns=["sample_id", "approved_by", "approved_at", "comment"])

    if "sample_id" not in df.columns:
        df["sample_id"] = ""

    for col in ["approved_by", "approved_at", "comment"]:
        if col not in df.columns:
            df[col] = ""

    df["sample_id"] = df["sample_id"].astype(str).str.strip().str.upper()
    return df


def _save_coordinate_approvals(df: pd.DataFrame) -> None:
    path = _coordinate_approved_csv_path()
    path.parent.mkdir(parents=True, exist_ok=True)

    keep_cols = ["sample_id", "approved_by", "approved_at", "comment"]
    for col in keep_cols:
        if col not in df.columns:
            df[col] = ""

    df = df[keep_cols].copy()
    df["sample_id"] = df["sample_id"].astype(str).str.strip().str.upper()
    df = df[df["sample_id"] != ""]
    df = df.drop_duplicates(subset=["sample_id"], keep="last")

    df.to_csv(path, index=False)


def _current_approver_id() -> str:
    kc_profile = (session.get("kc") or {}).get("profile") or {}
    return (
        kc_profile.get("email")
        or kc_profile.get("preferred_username")
        or kc_profile.get("id")
        or kc_profile.get("sub")
        or session.get("user")
        or "unknown"
    )

def _coordinate_wrong_csv_path() -> Path:
    """
    Resolve the CSV produced by pull_and_enrich_samples.py.

    In Docker this is usually /data/coordinate_check_wrong.csv.
    Locally it may be PROJECT_ROOT/data/coordinate_check_wrong.csv.
    """
    env_path = os.getenv("COORDINATE_WRONG_CSV")
    if env_path:
        return Path(env_path)

    candidates = [
        Path("/data/coordinate_check_wrong.csv"),
        Path("/app/data/coordinate_check_wrong.csv"),
        Path(settings.PROJECT_ROOT) / "data" / "coordinate_check_wrong.csv"
        if hasattr(settings, "PROJECT_ROOT")
        else None,
        Path.cwd() / "data" / "coordinate_check_wrong.csv",
    ]

    for p in candidates:
        if p and p.exists():
            return p

    # useful fallback for error display
    return Path("/data/coordinate_check_wrong.csv")


def _series_contains(series: pd.Series, text: str) -> pd.Series:
    if not text:
        return pd.Series(True, index=series.index)
    return series.fillna("").astype(str).str.contains(text, case=False, na=False, regex=False)

def _parse_sample_id_list(raw: str) -> list[str]:
    """
    Accept:
      - single: "AAAA-1111"
      - comma-separated: "AAAA-1111,BBBB-2222"
      - whitespace/newlines: "AAAA-1111  BBBB-2222"
    Returns cleaned non-empty tokens.
    """
    if not raw:
        return []
    # split on commas OR whitespace
    parts = re.split(r"[;,\s]+", raw.strip())
    return [p.strip() for p in parts if p.strip()]


# -------------- Endpoints for privacy acceptance ----------------


@web_bp.post("/privacy/accept")
@login_required
def privacy_accept():
    if not _env_true("PRIVACY_GATE", False):  # short-circuit the accept route when off
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


# --------------------------------------------------------------------------
# Canonical downloads (now built from Postgres on demand)
# --------------------------------------------------------------------------


# --------------------------------------------------------------------------
# Canonical downloads
# --------------------------------------------------------------------------

_CANONICAL_EXPORTS = {
    "samples.csv": {
        "dataset": "canonical_samples",
        "empty_message": "No canonical samples found in database",
    },
    "sample_images.csv": {
        "dataset": "canonical_sample_images",
        "empty_message": "No canonical sample_images found in database",
    },
    "sample_parameters.csv": {
        "dataset": "canonical_sample_parameters",
        "empty_message": "No canonical sample_parameters found in database",
    },
    "sample_biodiversity.csv": {
        "dataset": "canonical_sample_biodiversity",
        "empty_message": (
            "No compact biodiversity statistics found in database"
        ),
    },
}


def _send_live_canonical_csv(filename: str):
    config = _CANONICAL_EXPORTS[filename]

    g._analytics_extra = {
        "dataset": config["dataset"],
        "file_name": filename,
        "kind": "canonical_export",
    }

    df, csv_text = build_live_csv(
        filename=filename,
        base_url=request.url_root.rstrip("/"),
        snapshot_date=latest_canonical_snapshot_date(),
    )

    if df.empty:
        abort(
            404,
            description=config["empty_message"],
        )

    return send_file(
        BytesIO(csv_text.encode("utf-8")),
        as_attachment=True,
        download_name=filename,
        mimetype="text/csv",
    )


@web_bp.get("/download/canonical/samples.csv")
@login_required
def download_canonical_samples():
    return _send_live_canonical_csv("samples.csv")


@web_bp.get("/download/canonical/sample_images.csv")
@login_required
def download_canonical_sample_images():
    return _send_live_canonical_csv(
        "sample_images.csv"
    )


@web_bp.get("/download/canonical/sample_parameters.csv")
@login_required
def download_canonical_sample_parameters():
    return _send_live_canonical_csv(
        "sample_parameters.csv"
    )


@web_bp.get("/download/canonical/sample_biodiversity.csv")
@login_required
def download_canonical_sample_biodiversity():
    return _send_live_canonical_csv(
        "sample_biodiversity.csv"
    )


@web_bp.get("/download/canonical/all.zip")
@login_required
def download_canonical_zip():
    g._analytics_extra = {
        "dataset": "canonical_all",
        "file_name": "all_canonical.zip",
        "kind": "canonical_export",
    }

    version_date = (
        datetime.utcnow()
        .date()
        .isoformat()
    )

    bundle = build_snapshot_bundle(
        base_url=request.url_root.rstrip("/"),
        version_date=version_date,
    )

    upload_canonical_csvs(
        bundle.csv_contents,
        version_date,
    )

    upload_canonical_zip(
        bundle.zip_bytes,
        version_date,
    )

    return send_file(
        BytesIO(bundle.zip_bytes),
        as_attachment=True,
        download_name="all_canonical.zip",
        mimetype="application/zip",
    )


# --------------------------------------------------------------------------
# main UI
# --------------------------------------------------------------------------
@web_bp.get("/", endpoint="landing")
def landing():
    logged_in = bool(session.get("user") or (session.get("kc") or {}).get("profile"))
    if logged_in:
        return redirect(url_for("web.home"))  # now /my
    return redirect(url_for("web.explore"))


@web_bp.get("/my", endpoint="home")
@login_required
def home():
    # biodiversity marker and level from env, with fallback to legacy env names
    biodiv_marker = os.getenv(
        "BIODIV_MARKER",
        os.getenv(
            "BIODEV_MARKER",
            "16S,ITS",
        ),
    )

    biodiv_level = os.getenv(
        "BIODIV_LEVEL",
        os.getenv(
            "BIODEV_LEVEL",
            "Phylum",
        ),
    )

    # who is this
    user_key = session.get("user") or session.get("kc", {}).get("profile", {}).get("email")
    if not user_key:
        requested_url = request.full_path.rstrip("?")

        return redirect(url_for("auth.login", next=requested_url,))

    # lab upload permission flag
    can_upload = can_upload_lab_data(user_key)

    # Load once: used by the privacy gate, survey, and personal table.
    df = query_user_df(user_key)

    # privacy gate
    privacy_user_id = _repo_user_id_from_df(df, user_key)
    # 1) default from env (now default is OFF)
    privacy_gate_default = _env_true("PRIVACY_GATE", False)

    # 2) enable/disable URL override via .env
    privacy_override_enabled = _env_true("PRIVACY_DEBUG_OVERRIDE", False)

    if privacy_override_enabled:
        # optional per-request override: ?privacy=on / ?privacy=off
        override = (request.args.get("privacy") or "").strip().lower()
        if override in {"on", "1", "true", "yes", "y"}:
            privacy_gate_on = True
        elif override in {"off", "0", "false", "no", "n"}:
            privacy_gate_on = False
        else:
            privacy_gate_on = privacy_gate_default
    else:
        # ignore ?privacy=... when override is disabled
        privacy_gate_on = privacy_gate_default

    needs_privacy = privacy_gate_on and not _has_accepted_privacy(privacy_user_id or "")

    # Decide survey visibility from canonical lab rows and sample age.
    has_lab_results = _user_has_lab_results(df) or _dataframe_has_metals(df)

    # only for display, now drop oxide-like columns (harmless on samples DF)
    df = _drop_oxide_columns_from_df(df)

    i18n = _current_ui_i18n()

    # Resolve the stable internal identifier (also used in the SoSci URL).
    kc_user_id = _repo_user_id_from_df(df, None)

    # survey url
    survey_user_id = kc_user_id or user_key
    survey_url = _build_sosci_url(survey_user_id)

    # Final survey becomes available when either:
    #   1. laboratory / elemental concentration results are available, OR
    #   2. three months have passed since the user's first sample.
    try:
        survey_after_months = int(os.getenv("SURVEY_AFTER_MONTHS", "3"))
    except ValueError:
        survey_after_months = 3
    survey_after_months = max(1, min(survey_after_months, 120))
    survey_due_by_age = _survey_timeout_reached(
        df,
        months=survey_after_months,
    )

    show_survey = bool(survey_url) and (
        has_lab_results
        or survey_due_by_age
    )

    # --- survey override via URL (?survey=on/off), for testing ---
    survey_override_enabled = _env_true("SURVEY_DEBUG_OVERRIDE", False)

    if survey_override_enabled:
        survey_override = (request.args.get("survey") or "").strip().lower()
        if survey_override in {"on", "1", "true", "yes", "y"}:
            show_survey = bool(survey_url)  # ignore has_metals here
        elif survey_override in {"off", "0", "false", "no", "n"}:
            show_survey = False
    # else: ignore any ?survey=... and keep show_survey as computed

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
            show_survey=show_survey,
            # privacy
            needs_privacy=needs_privacy,
            privacy_version=PRIVACY_VERSION,
            # pass flag to template
            can_upload_lab_data=can_upload,
            current_locale=str(get_locale() or "en"),
            biodiv_marker=biodiv_marker,
            biodiv_level=biodiv_level,
        )

    # NON-EMPTY: data issues ------------------------------------------------
    # Use the new pipeline-generated effective coordinate flag when available.
    # This respects manually approved coordinate issues.
    if "wrong_coordinates_effective" in df.columns:
        issue_df = df[
            df["wrong_coordinates_effective"]
            .fillna("")
            .astype(str)
            .str.strip()
            .str.lower()
            .isin({"1", "true", "yes", "y", "on"})
        ].copy()
        issue_count = len(issue_df)
    else:
        # Fallback for old CSV/database versions.
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
        # NEW: pass flag to template
        can_upload_lab_data=can_upload,
        current_locale=str(get_locale() or "en"),
        biodiv_marker=biodiv_marker,
        biodiv_level=biodiv_level,
    )


# --------------------------------------------------------------------------
# misc routes
# --------------------------------------------------------------------------
@web_bp.get("/i18n/labels")
def i18n_labels():
    """
    Return current UI translations, including overrides.

    This must remain public because /explore also uses it.
    """
    payload = _current_ui_i18n()

    response = jsonify(payload)

    # Do not let browsers or reverse proxies retain an old
    # translation response after an override is changed.
    response.headers[
        "Cache-Control"
    ] = (
        "no-store, no-cache, "
        "must-revalidate, max-age=0"
    )

    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"

    return response


@web_bp.get("/labels")
@login_required
def labels_json():
    loc_raw = request.args.get("locale") or str(get_locale() or "en")
    loc = _canon_locale(loc_raw)
    payload = {
        "labels": make_labels(loc),
        "by_msgid": get_overrides_msgid(loc) or {},
    }
    return jsonify(payload)


@web_bp.post("/download/csv")
@login_required
def download_csv():
    g._analytics_extra = {
        "dataset": "user_data",
        "file_name": "user_data.csv",
        "kind": "user_export",
    }
    # Never trust a posted user_key for a personal export. Resolve it from
    # the authenticated session, exactly as /my does.
    user_key = session.get("user") or (session.get("kc") or {}).get("profile", {}).get("email")
    if not user_key:
        abort(401, description="No authenticated user identifier.")
    df = query_user_df(user_key)
    if df.empty:
        abort(404)
    df = strip_orig_cols(df)
    df = _drop_oxide_columns_from_df(df)

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
    g._analytics_extra = {
        "dataset": "user_data",
        "file_name": "user_data.xlsx",
        "kind": "user_export",
    }
    # Never trust a posted user_key for a personal export. Resolve it from
    # the authenticated session, exactly as /my does.
    user_key = session.get("user") or (session.get("kc") or {}).get("profile", {}).get("email")
    if not user_key:
        abort(401, description="No authenticated user identifier.")
    df = query_user_df(user_key)
    if df.empty:
        abort(404)
    df = strip_orig_cols(df)
    df = _drop_oxide_columns_from_df(df)

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
    """Export the canonical sample dataset, excluding direct user identifiers.

    Do not use settings.INPUT_CSV here: it is the flattened raw intake file
    and can contain exact GPS coordinates and other personal information.
    Verify that the canonical lat/lon columns contain the public jittered
    coordinates before enabling this export in production.
    """
    g._analytics_extra = {
        "dataset": "all_data",
        "file_name": "all_data.csv",
        "kind": "user_export",
    }

    df_all, _csv_text = build_live_csv(
        filename="samples.csv",
        base_url=request.url_root.rstrip("/"),
        snapshot_date=latest_canonical_snapshot_date(),
    )
    if df_all.empty:
        abort(404, description="No canonical samples found in database.")

    # A whitelist of the scientific canonical fields is preferable to
    # exporting the raw Firestore rows. As an additional guard, remove
    # commonly-identifying and original-coordinate columns if present.
    excluded = {
        "userid", "user_id", "uid", "email", "collected_by",
        "first_name", "last_name", "name", "phone", "telephone",
        "gps_lat", "gps_long", "gps_lon", "gps_lng",
        "latitude_orig", "longitude_orig", "lat_orig", "lon_orig",
    }
    user_key_column = getattr(settings, "USER_KEY_COLUMN", None)
    if user_key_column:
        excluded.add(user_key_column.lower())

    to_drop = [
        col for col in df_all.columns
        if str(col).lower() in excluded
        or str(col).lower().endswith("_orig")
    ]
    df_all = df_all.drop(columns=to_drop, errors="ignore")
    df_all = _drop_oxide_columns_from_df(df_all)

    buf = BytesIO()
    df_all.to_csv(buf, index=False)
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
    """
    Export one canonical sample from PostgreSQL.

    Supports both parameter spellings for backward compatibility:

        /download/sample_csv?sample_id=GLMA-3458
        /download/sample_csv?sampleId=GLMA-3458
    """
    sample_id = (
        request.args.get("sample_id")
        or request.args.get("sampleId")
        or ""
    ).strip()

    if not sample_id:
        abort(
            400,
            description="sample_id is required",
        )

    cols_sql = ", ".join(CANONICAL_SAMPLE_COLS)

    with (
        get_pg_conn() as conn,
        conn.cursor(
            cursor_factory=RealDictCursor
        ) as cur,
    ):
        cur.execute(
            f"""
            SELECT {cols_sql}
            FROM samples
            WHERE UPPER(sample_id) = UPPER(%s)
            LIMIT 1
            """,
            (sample_id,),
        )

        row = cur.fetchone()

    if not row:
        abort(
            404,
            description=(
                f"Canonical sample not found: "
                f"{sample_id}"
            ),
        )

    row = dict(row)

    canonical_sample_id = str(
        row.get("sample_id") or sample_id
    ).strip()

    # ----------------------------------------------------------
    # Determine whether the logged-in user owns this sample.
    # ----------------------------------------------------------
    possible_user_ids = _current_authenticated_identifiers()

    collected_by = str(
        row.get("collected_by") or ""
    ).strip().casefold()

    is_owner = bool(
        collected_by
        and collected_by in possible_user_ids
    )

    # Do not disclose the submitter identifier to another user.
    if not is_owner:
        row.pop("collected_by", None)

    df = pd.DataFrame([row])

    buf = BytesIO()

    df.to_csv(
        buf,
        index=False,
    )

    buf.seek(0)

    safe_filename_id = re.sub(
        r"[^A-Za-z0-9._-]+",
        "_",
        canonical_sample_id,
    )

    g._analytics_extra = {
        "dataset": "canonical_sample",
        "file_name":
            f"sample_{safe_filename_id}.csv",
        "kind": "sample_export",
        "sample_id": canonical_sample_id,
    }

    return send_file(
        buf,
        as_attachment=True,
        download_name=(
            f"sample_{safe_filename_id}.csv"
        ),
        mimetype="text/csv",
    )


# --------------------------------------------------------------------------
# lab upload
# --------------------------------------------------------------------------
@web_bp.get("/lab-upload")
@login_required
def lab_upload():
    user_key = session.get("user") or session.get("kc", {}).get("profile", {}).get("email")
    if not can_upload_lab_data(user_key):
        abort(403, description="Not authorised to upload lab data")
    return render_template("lab_upload.html")


@web_bp.route("/coordinate-issues", endpoint="coordinate_issues", methods=["GET"])
@login_required
def coordinate_issues():
    """
    Show all coordinate validation problems from coordinate_check_wrong.csv.

    This is an internal/login-only QA page because it can expose exact coordinates,
    QR codes and user IDs.
    """
    csv_path = _coordinate_wrong_csv_path()

    criteria = {
        "sample_id": (request.args.get("sample_id") or "").strip(),
        "user_id": (request.args.get("user_id") or "").strip(),
        "reason": (request.args.get("reason") or "").strip(),
        "actual_country": (request.args.get("actual_country") or "").strip().upper(),
        "planned_country": (request.args.get("planned_country") or "").strip().upper(),
    }

    fmt = (request.args.get("format") or "").strip().lower()

    try:
        page = max(int(request.args.get("page", 1) or 1), 1)
    except ValueError:
        page = 1

    per_page = 100

    if not csv_path.exists():
        return render_template(
            "coordinate_issues.html",
            rows=[],
            criteria=criteria,
            reasons=[],
            page=1,
            total_pages=1,
            total_rows=0,
            file_missing=True,
            csv_path=str(csv_path),
        )

    df = pd.read_csv(
        csv_path,
        dtype=str,
        keep_default_na=False,
    )

    # ----------------------------------------------------------
    # Tolerant display columns
    # ----------------------------------------------------------

    if "sample_display" not in df.columns:
        if "QR_qrCode" in df.columns:
            df["sample_display"] = df["QR_qrCode"]
        elif "sample_id" in df.columns:
            df["sample_display"] = df["sample_id"]
        elif "sampleId" in df.columns:
            df["sample_display"] = df["sampleId"]
        else:
            df["sample_display"] = ""

    if "uid_display" not in df.columns:
        if "userId" in df.columns:
            df["uid_display"] = df["userId"]
        elif "collected_by" in df.columns:
            df["uid_display"] = df["collected_by"]
        elif "user_id" in df.columns:
            df["uid_display"] = df["user_id"]
        else:
            df["uid_display"] = ""

    if "lat_display" not in df.columns:
        df["lat_display"] = (
            df["GPS_lat"]
            if "GPS_lat" in df.columns
            else ""
        )

    if "lon_display" not in df.columns:
        df["lon_display"] = (
            df["GPS_long"]
            if "GPS_long" in df.columns
            else ""
        )

    if "actual_country_display" not in df.columns:
        df["actual_country_display"] = (
            df["actual_cc"]
            if "actual_cc" in df.columns
            else ""
        )

    if "planned_country_display" not in df.columns:
        df["planned_country_display"] = (
            df["planned_iso2"]
            if "planned_iso2" in df.columns
            else ""
        )

    if "reason_display" not in df.columns:
        df["reason_display"] = (
            df["coordinate_check_reason"]
            if "coordinate_check_reason" in df.columns
            else ""
        )

    # Normalize display strings a little.
    for col in (
        "sample_display",
        "uid_display",
        "lat_display",
        "lon_display",
        "actual_country_display",
        "planned_country_display",
        "reason_display",
    ):
        df[col] = df[col].fillna("").astype(str).str.strip()

    # ----------------------------------------------------------
    # Issue types
    #
    # IMPORTANT:
    # This is built from the complete coordinate_check_wrong.csv
    # BEFORE applying the user's search filters.
    # ----------------------------------------------------------

    reasons = sorted(
        x
        for x in df["reason_display"].unique().tolist()
        if x
    )

    # ----------------------------------------------------------
    # Apply filters
    # ----------------------------------------------------------

    if criteria["sample_id"]:
        q = criteria["sample_id"]

        mask = _series_contains(
            df["sample_display"],
            q,
        )

        for extra_col in (
            "sampleId",
            "sample_id",
            "QR_qrCode",
        ):
            if extra_col in df.columns:
                mask = mask | _series_contains(
                    df[extra_col],
                    q,
                )

        df = df.loc[mask].copy()

    if criteria["user_id"]:
        df = df.loc[
            _series_contains(
                df["uid_display"],
                criteria["user_id"],
            )
        ].copy()

    if criteria["reason"]:
        df = df.loc[
            df["reason_display"] == criteria["reason"]
        ].copy()

    if criteria["actual_country"]:
        df = df.loc[
            df["actual_country_display"]
            .str.upper()
            == criteria["actual_country"]
        ].copy()

    if criteria["planned_country"]:
        df = df.loc[
            df["planned_country_display"]
            .str.upper()
            .str.contains(
                criteria["planned_country"],
                na=False,
                regex=False,
            )
        ].copy()

    # ----------------------------------------------------------
    # Export currently filtered result
    # ----------------------------------------------------------

    if fmt == "csv":
        g._analytics_extra = {
            "dataset": "coordinate_issues",
            "file_name": "coordinate_issues.csv",
            "kind": "qa_export",
        }

        preferred_cols = [
            "sample_display",
            "uid_display",
            "lat_display",
            "lon_display",
            "actual_country_display",
            "planned_country_display",
            "reason_display",
            "sampleId",
            "QR_qrCode",
            "userId",
            "GPS_lat",
            "GPS_long",
            "actual_cc",
            "planned_iso2",
            "wrong_coordinates",
            "coordinate_check_reason",
        ]

        cols = [
            c
            for c in preferred_cols
            if c in df.columns
        ]

        if not cols:
            cols = list(df.columns)

        buf = BytesIO()

        df[cols].to_csv(
            buf,
            index=False,
        )

        buf.seek(0)

        return send_file(
            buf,
            as_attachment=True,
            download_name="coordinate_issues.csv",
            mimetype="text/csv",
        )

    # ----------------------------------------------------------
    # Pagination
    # ----------------------------------------------------------

    total_rows = len(df)

    total_pages = max(
        (total_rows + per_page - 1) // per_page,
        1,
    )

    if page > total_pages:
        page = total_pages

    start = (page - 1) * per_page
    end = start + per_page

    rows = df.iloc[start:end].to_dict(
        orient="records"
    )

    return render_template(
        "coordinate_issues.html",
        rows=rows,
        criteria=criteria,
        reasons=reasons,
        page=page,
        total_pages=total_pages,
        total_rows=total_rows,
        file_missing=False,
        csv_path=str(csv_path),
    )

    
@web_bp.post("/coordinate-issues/approve", endpoint="coordinate_issues_approve")
@login_required
def coordinate_issues_approve():
    """
    Mark selected coordinate issues as manually accepted/OK.

    These approvals are stored separately from coordinate_check_wrong.csv so that
    they survive future pull_and_enrich_samples.py runs.
    """
    sample_ids = request.form.getlist("sample_id")
    comment = (request.form.get("comment") or "").strip()

    cleaned = []
    for sid in sample_ids:
        sid = str(sid or "").strip().upper()
        if sid:
            cleaned.append(sid)

    # Preserve current filters when returning to the page
    return_args = {
        "sample_id": request.form.get("filter_sample_id") or "",
        "user_id": request.form.get("filter_user_id") or "",
        "reason": request.form.get("filter_reason") or "",
        "actual_country": request.form.get("filter_actual_country") or "",
        "planned_country": request.form.get("filter_planned_country") or "",
        "show_approved": request.form.get("filter_show_approved") or "",
    }

    if not cleaned:
        return redirect(url_for("web.coordinate_issues", **return_args))

    approvals_df = _load_coordinate_approvals()

    now = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    approved_by = _current_approver_id()

    new_rows = pd.DataFrame(
        [
            {
                "sample_id": sid,
                "approved_by": approved_by,
                "approved_at": now,
                "comment": comment,
            }
            for sid in cleaned
        ]
    )

    approvals_df = pd.concat([approvals_df, new_rows], ignore_index=True)
    approvals_df = approvals_df.drop_duplicates(subset=["sample_id"], keep="last")

    _save_coordinate_approvals(approvals_df)

    return redirect(url_for("web.coordinate_issues", **return_args))


@web_bp.route("/search", endpoint="search_samples", methods=["GET", "POST"])
def search_samples():
    req = request.values
    q = request.args.get("q", "").strip()

    i18n = _current_ui_i18n()
    allowed_country_codes = get_map_allowed_countries()

    # ----- read filters from querystring -----
    criteria = {
        "sample_id": (
            req.get("sample_ids")
            or req.get("sample_id")
            or ""
        ).strip(),
        "sample_ids_exact": bool(req.get("sample_ids")),
        "country_code": (req.get("country_code") or req.get("country") or "").strip().upper(),
        "ph_min": (req.get("ph_min") or "").strip(),
        "ph_max": (req.get("ph_max") or "").strip(),
        "date_from": (req.get("date_from") or req.get("from") or "").strip(),
        "date_to": (req.get("date_to") or req.get("to") or "").strip(),
        "biogeo_region": (req.get("biogeo_region") or "").strip(),
    }
    fmt = (req.get("format") or "").lower()

    try:
        page = max(int(req.get("page", 1)), 1)
    except (TypeError, ValueError):
        abort(400, description="Invalid page number.")

    per_page = 50

    # helper to build WHERE + params
    def _build_where(criteria):
        where = ["1=1"]
        params = []

        # --------------------------------------------------
        # 1. Sample / QR filtering
        # --------------------------------------------------
        if criteria["sample_id"]:
            tokens = _parse_sample_id_list(criteria["sample_id"])

            if not tokens:
                abort(400, description="No valid sample IDs provided.")

            if criteria.get("sample_ids_exact"):
                # Map rectangle export:
                # match the selected sample IDs exactly.
                where.append("sample_id = ANY(%s)")
                params.append(tokens)

            else:
                # Normal search:
                # allow partial matching of sample IDs.
                if len(tokens) == 1:
                    where.append("sample_id ILIKE %s")
                    params.append(f"%{tokens[0]}%")

                else:
                    ors = []

                    for token in tokens:
                        ors.append("sample_id ILIKE %s")
                        params.append(f"%{token}%")

                    where.append("(" + " OR ".join(ors) + ")")

        # --------------------------------------------------
        # 2. Country filtering
        # --------------------------------------------------
        if criteria["country_code"]:
            where.append("country_code = %s")
            params.append(criteria["country_code"])

        # --------------------------------------------------
        # 3. pH filtering
        # --------------------------------------------------
        try:
            ph_min = (
                float(criteria["ph_min"])
                if criteria["ph_min"]
                else None
            )

            ph_max = (
                float(criteria["ph_max"])
                if criteria["ph_max"]
                else None
            )

        except (ValueError, TypeError):
            abort(400, description="Invalid pH value.")

        # Reject NaN and Infinity.
        if ph_min is not None and not math.isfinite(ph_min):
            abort(400, description="Invalid minimum pH.")

        if ph_max is not None and not math.isfinite(ph_max):
            abort(400, description="Invalid maximum pH.")

        # Check that the range makes sense.
        if (
            ph_min is not None
            and ph_max is not None
            and ph_min > ph_max
        ):
            abort(
                400,
                description="Minimum pH cannot exceed maximum pH."
            )

        if ph_min is not None:
            where.append("ph >= %s")
            params.append(ph_min)

        if ph_max is not None:
            where.append("ph <= %s")
            params.append(ph_max)

        # --------------------------------------------------
        # 4. Date filtering
        # --------------------------------------------------
        try:
            date_from = (
                date.fromisoformat(criteria["date_from"])
                if criteria["date_from"]
                else None
            )

            date_to = (
                date.fromisoformat(criteria["date_to"])
                if criteria["date_to"]
                else None
            )

        except (ValueError, TypeError):
            abort(400, description="Invalid date format.")

        # Ensure the requested date range is valid.
        if (
            date_from is not None
            and date_to is not None
            and date_from > date_to
        ):
            abort(
                400,
                description="Start date cannot exceed end date."
            )

        if date_from is not None:
            where.append("timestamp_utc >= %s")
            params.append(date_from.isoformat())

        if date_to is not None:
            # Include the entire final day.
            # Example: date_to=2026-10-22
            # means timestamp_utc < 2026-10-23.
            try:
                end_exclusive = date_to + timedelta(days=1)
            except OverflowError:
                abort(400, description="End date is outside the supported range.")

            where.append("timestamp_utc < %s")
            params.append(end_exclusive.isoformat())

        # --------------------------------------------------
        # 5. Return criteria based on Biogeo Region  
        # --------------------------------------------------

        if criteria["biogeo_region"]:
            where.append("""
                EXISTS (
                    SELECT 1
                    FROM sample_biogeo_regions AS br
                    WHERE br.sample_id = samples.sample_id
                    AND br.region_name = %s
                )
            """)
            params.append(criteria["biogeo_region"])

        # --------------------------------------------------
        # 6. Return SQL conditions and parameters
        # --------------------------------------------------
        return " AND ".join(where), params

    # Load available regions from existing canonical samples.
    # Used both for dropdown population and request validation.
    with get_pg_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT br.region_name
            FROM sample_biogeo_regions AS br
            JOIN samples AS s
            ON s.sample_id = br.sample_id
            WHERE br.region_name IS NOT NULL
            AND TRIM(br.region_name) <> ''
            ORDER BY br.region_name
        """)

        available_regions = [
            row[0]
            for row in cur.fetchall()
        ]

    # Reject unknown biogeographical regions.
    if (
        criteria["biogeo_region"]
        and criteria["biogeo_region"] not in available_regions
    ):
        abort(
            400,
            description="Invalid biogeographical region."
        )

    where_sql, params = _build_where(criteria)

    # Parameters shared by pagination and export links.
    link_filters = {
        "country_code": criteria["country_code"],
        "ph_min": criteria["ph_min"],
        "ph_max": criteria["ph_max"],
        "date_from": criteria["date_from"],
        "date_to": criteria["date_to"],
        "biogeo_region": criteria["biogeo_region"],
    }

    # Preserve the distinction between exact map selection
    # and normal partial sample-ID search.
    if criteria["sample_id"]:
        key = (
            "sample_ids"
            if criteria["sample_ids_exact"]
            else "sample_id"
        )
        link_filters[key] = criteria["sample_id"]

    def _is_logged_in() -> bool:
        return bool(session.get("user") or (session.get("kc") or {}).get("profile"))

    if fmt == "zip" and not _is_logged_in():
        # show search page as normal, but deny export
        abort(403, description="Please sign in to export ZIP files.")

    # ---- special case: export as ZIP ----
    if fmt == "zip":
        query_string = request.query_string.decode(
            "utf-8",
            errors="replace",
        )

        base_url = request.url_root.rstrip("/")
        snapshot_date = latest_canonical_snapshot_date()
        version_date = (
            snapshot_date
            or date.today().isoformat()
        )
        snapshot_url = (
            f"{base_url}/download/canonical/"
            f"{version_date}/all.zip"
        )

        # Determine only which samples match the search criteria.
        # The canonical export service fetches and formats all four datasets.
        sql_ids = f"""
            SELECT sample_id
            FROM samples
            WHERE {where_sql}
            ORDER BY timestamp_utc DESC NULLS LAST, sample_id
        """

        with get_pg_conn() as conn, conn.cursor() as cur:
            cur.execute(sql_ids, params)
            sample_ids = [
                row[0]
                for row in cur.fetchall()
                if row and row[0]
            ]

        bundle = build_filtered_bundle(
            sample_ids=sample_ids,
            snapshot_url=snapshot_url,
            query_string=query_string,
        )

        return send_file(
            BytesIO(bundle.zip_bytes),
            as_attachment=True,
            download_name="search_export.zip",
            mimetype="application/zip",
        )

    # ---- normal HTML search with pagination ----
    total_rows = 0
    rows = []

    with get_pg_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        # Countries actually represented in ECHOREPO,
        # restricted to the configured whitelist.
        cur.execute(
            """
            SELECT DISTINCT UPPER(TRIM(country_code)) AS code
            FROM samples
            WHERE country_code IS NOT NULL
            AND UPPER(TRIM(country_code)) = ANY(%s)
            ORDER BY code
            """,
            (list(allowed_country_codes),),
        )

        available_country_codes = [
            row["code"]
            for row in cur.fetchall()
        ]

        # count
        cur.execute(f"SELECT COUNT(*) FROM samples WHERE {where_sql}", params)
        total_rows = cur.fetchone()["count"]

        # Clamp out-of-range page numbers before applying LIMIT/OFFSET.
        total_pages = max((total_rows + per_page - 1) // per_page, 1)
        page = min(page, total_pages)
        offset = (page - 1) * per_page

        # page
        cur.execute(
            f"""
            SELECT sample_id, timestamp_utc, country_code, ph, lat, lon, collected_by
            FROM samples
            WHERE {where_sql}
            ORDER BY timestamp_utc DESC NULLS LAST, sample_id
            LIMIT %s OFFSET %s
            """,
            params + [per_page, offset],
        )
        rows = cur.fetchall()

    # /search is publicly accessible. Only an owner may see their
    # own collected_by identifier; don't disclose other submitter IDs.
    own_ids = _current_authenticated_identifiers()
    for row in rows:
        submitter = str(row.get("collected_by") or "").strip().casefold()
        if submitter not in own_ids:
            row["collected_by"] = None

    total_pages = max((total_rows + per_page - 1) // per_page, 1)

    # add analytics extras
    g._analytics_extra = {
        "search_query": q,
        # maybe:
        # "search_source": "web",
        # "filters": list(request.args.keys()),
    }

    return render_template(
        "search.html",
        criteria=criteria,
        rows=rows,
        page=page,
        total_pages=total_pages,
        total_rows=total_rows,
        I18N=i18n,
        available_country_codes=available_country_codes,
        available_regions=available_regions,
        link_filters=link_filters,
    )
    


@web_bp.route("/admin/usage")
@login_required  # TODO: restrict to admins if possible
def usage_dashboard():
    """
    Simple usage statistics dashboard backed by Postgres usage_events table.
    Shows:
      - summary counts for the period
      - per-day stats
      - downloads per dataset
      - events per type
    """

    # Period control via ?days=...
    try:
        days = int(request.args.get("days", 30))
    except ValueError:
        days = 30
    days = max(1, min(days, 365))  # clamp to [1, 365]

    end_ts = datetime.utcnow()
    start_ts = end_ts - timedelta(days=days)

    with get_pg_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        # 1) Summary for the period
        cur.execute(
            """
            SELECT
              COUNT(*) AS total_requests,
              COUNT(DISTINCT user_id) AS unique_users,
              COUNT(DISTINCT ip_hash) AS unique_visitors,
              COUNT(*) FILTER (WHERE event_type = 'download') AS total_downloads
            FROM usage_events
            WHERE ts >= %s AND ts < %s
            """,
            (start_ts, end_ts),
        )
        summary = cur.fetchone() or {
            "total_requests": 0,
            "unique_users": 0,
            "unique_visitors": 0,
            "total_downloads": 0,
        }

        # 2) Per-day stats
        cur.execute(
            """
            SELECT
              date_trunc('day', ts)::date AS day,
              COUNT(*) AS requests,
              COUNT(DISTINCT user_id) AS unique_users,
              COUNT(DISTINCT ip_hash) AS unique_visitors,
              COUNT(*) FILTER (WHERE event_type = 'download') AS downloads
            FROM usage_events
            WHERE ts >= %s AND ts < %s
            GROUP BY day
            ORDER BY day;
            """,
            (start_ts, end_ts),
        )
        daily = cur.fetchall() or []

        # 3) Downloads per dataset (for this period)
        cur.execute(
            """
            SELECT
              date_trunc('month', ts)::date AS month,
              COALESCE(
                extra->>'dataset',
                extra->>'file',
                path
              ) AS dataset,
              COUNT(*) AS downloads
            FROM usage_events
            WHERE event_type = 'download'
              AND ts >= %s AND ts < %s
            GROUP BY month, dataset
            ORDER BY month DESC, downloads DESC
            LIMIT 200;
            """,
            (start_ts, end_ts),
        )
        downloads_by_dataset = cur.fetchall() or []

        # 4) Events by type
        cur.execute(
            """
            SELECT
              event_type,
              COUNT(*) AS count
            FROM usage_events
            WHERE ts >= %s AND ts < %s
            GROUP BY event_type
            ORDER BY count DESC;
            """,
            (start_ts, end_ts),
        )
        by_type = cur.fetchall() or []

    return render_template(
        "admin/usage_stats.html",
        days=days,
        start_ts=start_ts,
        end_ts=end_ts,
        summary=summary,
        daily=daily,
        downloads_by_dataset=downloads_by_dataset,
        by_type=by_type,
    )


@web_bp.get("/download/canonical/<date>/<filename>")
@login_required
def download_canonical_version(date, filename):
    """
    Serve a specific dated canonical CSV from MinIO, e.g.
    /download/canonical/2025-12-02/samples.csv
    which maps to MinIO object canonical/2025-12-02/samples.csv
    """
    obj_name = f"{date}/{filename}"

    try:
        stored = get_canonical_object(obj_name)
    except StorageNotConfigured as exc:
        abort(503, description=str(exc))
    except StorageObjectNotFound as exc:
        abort(404, description=str(exc))
    except StorageError as exc:
        abort(502, description=str(exc))

    return send_file(
        BytesIO(stored.data),
        mimetype=stored.content_type,
        as_attachment=True,
        download_name=stored.download_name,
    )


@web_bp.get("/explore", endpoint="explore")
def explore():
    i18n = _current_ui_i18n()

    return render_template(
        "explore.html",
        jitter_m=int(settings.MAX_JITTER_METERS),
        lat_col=settings.LAT_COL,
        lon_col=settings.LON_COL,
        I18N=i18n,
        current_locale=str(get_locale() or "en"),
        public_mode=True,
        zenodo_dataset_url=ZENODO_DATASET_URL,
    )


@web_bp.get("/public/others_geojson")
def public_others_geojson():
    # Keep this very lightweight: only what the map needs.
    sql = """
    SELECT
        sample_id, lat, lon, ph, country_code, timestamp_utc,
        soil_texture_en, soil_texture_orig,
        soil_structure_en, soil_structure_orig,
        earthworms_count,
        contamination_plastic, contamination_debris,
        contamination_other_en, contamination_other_orig,
        observations_en, observations_orig,
        metals_info_en, metals_info_orig,
        location_accuracy_m,
        qa_status
    FROM samples
    WHERE lat IS NOT NULL AND lon IS NOT NULL
    ORDER BY timestamp_utc DESC
    LIMIT 20000
    """

    with get_pg_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql)
        rows = cur.fetchall()

    feats = []
    for r in rows:
        try:
            lat = float(r["lat"])
            lon = float(r["lon"])
        except Exception:
            continue

        feats.append(
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [lon, lat]},
                "properties": {
                    "sampleId": r["sample_id"],
                    "PH_ph": r.get("ph"),
                    "country_code": r.get("country_code"),
                    "timestamp_utc": (
                        r["timestamp_utc"].isoformat() if r.get("timestamp_utc") else None
                    ),
                    # soil descriptors (your map.js "pick(...)" already supports these)
                    "soil_texture_en": r.get("soil_texture_en"),
                    "soil_texture_orig": r.get("soil_texture_orig"),
                    "soil_structure_en": r.get("soil_structure_en"),
                    "soil_structure_orig": r.get("soil_structure_orig"),
                    # simple counts/flags
                    "earthworms_count": r.get("earthworms_count"),
                    "contamination_plastic": r.get("contamination_plastic"),
                    "contamination_debris": r.get("contamination_debris"),
                    "contamination_other_en": r.get("contamination_other_en"),
                    "contamination_other_orig": r.get("contamination_other_orig"),
                    "observations_en": r.get("observations_en"),
                    "observations_orig": r.get("observations_orig"),
                    # “metals blob” if you have it in samples
                    "metals_info_en": r.get("metals_info_en"),
                    "metals_info_orig": r.get("metals_info_orig"),
                    # optional
                    "location_accuracy_m": r.get("location_accuracy_m"),
                    "qa_status": r.get("qa_status"),
                    "wrong_coordinates": bool(
                        str(r.get("qa_status") or "").strip().lower().startswith("wrong_coordinates"))
                },
            }
        )

    return jsonify({"type": "FeatureCollection", "features": feats})


@web_bp.get("/public/sample_image/<sample_id>")
def public_sample_image(sample_id: str):
    # Return 1 thumbnail-ish image URL (or None) for a sample_id
    sql = """
      SELECT image_url, image_description_en, image_description_orig
      FROM sample_images
      WHERE sample_id = %s
        AND image_url IS NOT NULL AND image_url <> ''
      ORDER BY image_id
      LIMIT 1
    """
    with get_pg_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, (sample_id,))
        row = cur.fetchone()

    if not row:
        return jsonify({"ok": True, "image_url": None})

    desc = row.get("image_description_en") or row.get("image_description_orig") or ""
    return jsonify({"ok": True, "image_url": row.get("image_url"), "caption": desc})


def _process_lab_upload_file(
    uploaded_file,
    uploader_id: str,
) -> dict:
    """
    Process one uploaded laboratory file.

    Returns a small result dictionary for logging and analytics.
    """
    filename = (
        uploaded_file.filename
        or ""
    ).strip()

    if not filename:
        raise InvalidLabUpload(
            "Uploaded file has no filename"
        )

    data = uploaded_file.read()

    if not data:
        raise InvalidLabUpload(
            f"{filename}: uploaded file is empty"
        )

    if _looks_like_biodiversity_file(
        data,
        filename,
    ):
        inserted = _import_biodiversity_streaming(
            data,
            filename,
            uploader_id,
        )

        return {
            "filename": filename,
            "type": "biodiversity",
            "rows_inserted": inserted,
        }

    inserted = import_metals_file(
        data=data,
        filename=filename,
        uploader_id=uploader_id,
    )

    return {
        "filename": filename,
        "type": "metals",
        "rows_inserted": inserted,
    }


@web_bp.post("/lab-import-auto")
@login_required
def lab_import_auto():
    user_key = (
        session.get("user")
        or session.get("kc", {})
        .get("profile", {})
        .get("email")
    )

    if not can_upload_lab_data(user_key):
        abort(
            403,
            description="Not authorised to upload lab data",
        )

    uploaded_files = [
        uploaded_file
        for uploaded_file
        in request.files.getlist("file")
        if uploaded_file
        and (uploaded_file.filename or "").strip()
    ]

    if not uploaded_files:
        abort(
            400,
            description="No files uploaded",
        )

    kc_profile = (
        session.get("kc")
        or {}
    ).get("profile") or {}

    uploader_id = (
        kc_profile.get("id")
        or kc_profile.get("sub")
        or session.get("user")
        or "unknown"
    )

    results = []

    for uploaded_file in uploaded_files:
        filename = (
            uploaded_file.filename
            or ""
        ).strip()

        logging.getLogger(__name__).warning(
            "LABUPLOAD: processing file %s",
            filename,
        )

        try:
            result = _process_lab_upload_file(
                uploaded_file,
                uploader_id,
            )

        except InvalidLabUpload as exc:
            abort(
                400,
                description=str(exc),
            )
        except LabImportError as exc:
            logging.getLogger(__name__).exception(
                "Laboratory import failed for %s",
                filename,
            )

            abort(
                500,
                description=str(exc),
            )

        results.append(result)

        logging.getLogger(__name__).warning(
            "LABUPLOAD: completed file=%s type=%s rows=%s",
            result["filename"],
            result["type"],
            result["rows_inserted"],
        )

    g._analytics_extra = {
        "upload_type": (
            "multiple_lab_files"
            if len(results) > 1
            else "single_lab_file"
        ),

        "file_count": len(results),
        
        "biodiversity_file_count": sum(
            result["type"] == "biodiversity"
            for result in results
        ),
        
        "metals_file_count": sum(
            result["type"] == "metals"
            for result in results
        ),
        
        "metals_values_upserted": sum(
            result["rows_inserted"] or 0
            for result in results
            if result["type"] == "metals"
        ),
        
        "files": [
            result["filename"]
            for result in results
        ],
        
        "biodiversity_rows_inserted": sum(
            result["rows_inserted"] or 0
            for result in results
            if result["type"] == "biodiversity"
        ),
    }

    return redirect(
        url_for("web.home")
    )


@web_bp.get("/public/sample_piechart/<sample_id>")
def public_sample_piechart(sample_id: str):
    sample_id = str(sample_id or "").strip().upper()
    if not re.fullmatch(r"[A-Z0-9]{4}-[A-Z0-9]{4,}", sample_id):
        abort(400, description="Invalid sample ID")

    marker = (request.args.get("marker") or "16S").strip().upper()
    if marker not in {"16S", "ITS"}:
        abort(400, description="marker must be 16S or ITS")

    level_raw = (request.args.get("level") or "Phylum").strip().lower()
    level_aliases = {
        "kingdom": "Kingdom",
        "phylum": "Phylum",
        "philum": "Phylum",
        "class": "Class",
        "order": "Order",
        "family": "Family",
        "genus": "Genus",
        "species": "Species",
    }
    level = level_aliases.get(level_raw, "Phylum")

    # The taxonomic pie chart always depends on the imported data.
    object_name = (
        f"biodiversity/piecharts/"
        f"{marker}/{level}/{sample_id}.png"
    )

    try:
        chart_exists = object_exists(object_name)
    except StorageError:
        logging.getLogger(__name__).exception(
            "Could not inspect biodiversity chart %s",
            object_name,
        )
        chart_exists = False

    if not chart_exists:
        return jsonify({"ok": True, "image_url": None, "caption": ""})

    image_url = f"/storage/{object_name}"
    return jsonify(
        {
            "ok": True,
            "image_url": image_url,
            "caption": f"{marker} · {level}",
        }
    )


def _read_zenodo_publications(log_path: str) -> list[dict]:
    p = Path(log_path)
    if not p.exists():
        return []

    rows: list[dict] = []
    try:
        with p.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if (row.get("status") or "").strip().lower() != "ok":
                    continue
                if not (row.get("version_doi") or row.get("zenodo_html")):
                    continue
                rows.append(row)
    except (OSError, UnicodeError, csv.Error) as exc:
        logging.getLogger(__name__).warning(
            "Could not read Zenodo publication log %s: %s", p, exc
        )
        return []

    # newest first
    rows.sort(key=lambda r: r.get("run_at_utc", ""), reverse=True)
    return rows


@web_bp.get("/publications/zenodo", endpoint="zenodo_publications")
def zenodo_publications():
    publications = _read_zenodo_publications(ZENODO_LOG_DEFAULT)

    # If you want only production records on the public page:
    # publications = [p for p in publications if p.get("sandbox") != "1"]

    return render_template(
        "zenodo_publications.html",
        publications=publications,
        current_locale=str(get_locale() or "en"),
    )
