from flask import Blueprint, render_template, request, redirect, url_for, flash, session
from ..auth.decorators import login_required
from ..services.db import query_user_df, update_coords_sqlite
from ..services.validation import find_default_coord_rows, annotate_country_mismatches
from ..services.firebase import update_coords_by_user_sample
from ..utils.coords import parse_coord
from ..config import settings

errors_bp = Blueprint("errors", __name__)

@errors_bp.get("/issues")
@login_required
def issues():
    user_key = session.get("user")
    df = query_user_df(user_key)

    defaults = find_default_coord_rows(df)
    mism = annotate_country_mismatches(df)

    # minimal columns for the view
    cols = ["sampleId","userId","QR_qrCode", settings.LAT_COL, settings.LON_COL]
    def pick(d):
        present = [c for c in cols if c in d.columns]
        return d[present].copy()

    return render_template(
        "issues.html",
        default_rows=pick(defaults),
        mismatch_rows=pick(mism)
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
