from flask import Blueprint, render_template, request, send_file, abort, session, redirect, url_for, jsonify, current_app
import pandas as pd
from io import BytesIO
import pathlib
from ..config import settings
from ..auth.decorators import login_required
from ..services.db import query_user_df, query_sample
from ..services.validation import find_default_coord_rows, annotate_country_mismatches
from ..utils.table import make_table_html, strip_orig_cols
from echorepo.i18n import build_i18n_labels
from flask_babel import gettext as _  
from datetime import datetime

web_bp = Blueprint("web", __name__)

# ---------- NEW: base UI labels used by map.js / UI ----------
def _js_base_labels() -> dict:
    # Keep keys in sync with your map.js calls
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
    }

@web_bp.get("/", endpoint="home")
@login_required
def home():
    # same as before: get the "logical" user key (email from KC / session)
    user_key = session.get("user") or session.get("kc", {}).get("profile", {}).get("email")
    if not user_key:
        return redirect(url_for("auth.login"))

    df = query_user_df(user_key)
    i18n = {"labels": build_i18n_labels(_js_base_labels())}

    # 👇 figure out the survey id
    SURVEY_BASE_URL = getattr(settings, "SURVEY_BASE_URL", "https://www.soscisurvey.de/default?r=")  # change to your real URL
    survey_user_id = None
    if not df.empty:
        # prefer real userId column if present
        if "userId" in df.columns:
            col = df["userId"].dropna().astype(str)
            if not col.empty:
                survey_user_id = col.iloc[0].strip()
        # optional fallback: some setups store it under a configured column
        elif getattr(settings, "USER_KEY_COLUMN", None) and settings.USER_KEY_COLUMN in df.columns:
            col = df[settings.USER_KEY_COLUMN].dropna().astype(str)
            if not col.empty:
                survey_user_id = col.iloc[0].strip()

    # final fallback to email if we didn't find anything
    if not survey_user_id:
        survey_user_id = user_key

    survey_url = f"{SURVEY_BASE_URL}{survey_user_id}"

    if df.empty:
        return render_template(
            "results.html",
            issue_count=0,
            user_key=user_key,
            columns=[],
            table_html="<p>No data available for this user.</p>",
            jitter_m=int(settings.MAX_JITTER_METERS),
            lat_col=settings.LAT_COL,
            lon_col=settings.LON_COL,
            I18N=i18n,
            # 👇 new
            survey_url=survey_url,
            show_survey=True,
        )

    defaults = find_default_coord_rows(df)
    mism = annotate_country_mismatches(df)
    issue_count = len(defaults) + len(mism)

    return render_template(
        "results.html",
        issue_count=issue_count,
        user_key=user_key,
        columns=list(df.columns),
        table_html=make_table_html(df),
        jitter_m=int(settings.MAX_JITTER_METERS),
        lat_col=settings.LAT_COL,
        lon_col=settings.LON_COL,
        I18N=i18n,
        # 👇 new
        survey_url=survey_url,
        show_survey=True,
    )

# (Optional) JSON endpoint if you prefer fetching labels via XHR
@web_bp.get("/i18n/labels")
@login_required
def i18n_labels():
    return jsonify({"labels": build_i18n_labels(_js_base_labels())})

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
        mimetype="text/csv"
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

    # columns we never want to expose
    pii_cols = {
        "userId",
        "user_id",
        "email",
        getattr(settings, "USER_KEY_COLUMN", None),
    }
    pii_cols = {c.lower() for c in pii_cols if c}  # drop Nones

    try:
        # 1) normal, fast, pandas path
        df_all = pd.read_csv(p, dtype=str, keep_default_na=False, low_memory=False)
        # existing anonymizer
        df_all = strip_orig_cols(df_all)
        # and belt-and-suspenders: drop common PII columns if still present
        cols_to_drop = [col for col in df_all.columns if col.lower() in pii_cols]
        df_all = df_all.drop(columns=[cols_to_drop], errors="ignore")

        buf = BytesIO()
        df_all.to_csv(buf, index=False)
        buf.seek(0)
        return send_file(
            buf,
            as_attachment=True,
            download_name="echorepo_all_samples.csv",
            mimetype="text/csv"
        )

    except Exception:
        # 2) fallback: stream-sanitize without pandas
        import csv
        buf = BytesIO()
        with p.open("r", encoding="utf-8", newline="") as f_in:
            reader = csv.reader(f_in)
            rows = list(reader)
            if not rows:
                abort(404, description="CSV is empty")

            header = rows[0]
            # figure out which columns to keep
            keep_idx = []
            for i, name in enumerate(header):
                if name in pii_cols:
                    continue
                keep_idx.append(i)

            writer = csv.writer(buf)
            # write filtered header
            writer.writerow([header[i] for i in keep_idx])
            # write filtered rows
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
        mimetype="text/csv"
    )
