from flask import Blueprint, render_template, request, send_file, abort, session, redirect, url_for
import pandas as pd
from io import BytesIO
import pathlib
from ..config import settings
from ..auth.decorators import login_required
from ..services.db import query_user_df, query_sample
from ..services.validation import find_default_coord_rows, annotate_country_mismatches
from ..utils.table import make_table_html, strip_orig_cols

web_bp = Blueprint("web", __name__)

@web_bp.get("/", endpoint="home")
@login_required
def home():
    user_key = session.get("user") or session.get("kc", {}).get("profile", {}).get("email")
    if not user_key:
        return redirect(url_for("auth.login"))

    df = query_user_df(user_key)
    if df.empty:
        return render_template(
            "results.html",
            user_key=user_key,
            columns=[],
            table_html="<p>No data available for this user.</p>",
            jitter_m=int(settings.MAX_JITTER_METERS),
            lat_col=settings.LAT_COL, lon_col=settings.LON_COL
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
        lat_col=settings.LAT_COL, lon_col=settings.LON_COL
    )

@web_bp.post("/download/csv")
@login_required
def download_csv():
    user_key = (request.form.get("user_key") or "").strip()
    if not user_key: abort(400)
    df = query_user_df(user_key)
    if df.empty: abort(404)
    df = strip_orig_cols(df)
    buf = BytesIO(); df.to_csv(buf, index=False); buf.seek(0)
    return send_file(buf, as_attachment=True, download_name=f"{user_key}_data.csv", mimetype="text/csv")

@web_bp.post("/download/xlsx")
@login_required
def download_xlsx():
    user_key = (request.form.get("user_key") or "").strip()
    if not user_key: abort(400)
    df = query_user_df(user_key)
    if df.empty: abort(404)
    df = strip_orig_cols(df)
    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="xlsxwriter") as w:
        df.to_excel(w, index=False, sheet_name="data")
    buf.seek(0)
    return send_file(
        buf, as_attachment=True,
        download_name=f"{user_key}_data.xlsx",
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

@web_bp.get("/download/all_csv")
@login_required
def download_all_csv():
    p = pathlib.Path(settings.INPUT_CSV)
    if not p.exists() or not p.is_file():
        abort(404, description="Full CSV not found on server.")
    try:
        df_all = pd.read_csv(p, dtype=str, keep_default_na=False)
        df_all = strip_orig_cols(df_all)
        buf = BytesIO(); df_all.to_csv(buf, index=False); buf.seek(0)
        return send_file(buf, as_attachment=True, download_name="echorepo_all_samples.csv", mimetype="text/csv")
    except Exception:
        return send_file(str(p), as_attachment=True, download_name="echorepo_all_samples.csv", mimetype="text/csv",
                         max_age=0, conditional=True)

@web_bp.get("/download/sample_csv")
@login_required
def download_sample_csv():
    sample_id = (request.args.get("sampleId") or "").strip()
    if not sample_id: abort(400, description="sampleId is required")
    df = query_sample(sample_id)
    if df.empty: abort(404, description="Sample not found")
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
        for pii in ("email","userId"):
            if pii in df.columns: df = df.drop(columns=[pii])
    buf = BytesIO(); df.to_csv(buf, index=False); buf.seek(0)
    return send_file(buf, as_attachment=True, download_name=f"sample_{sample_id}.csv", mimetype="text/csv")
