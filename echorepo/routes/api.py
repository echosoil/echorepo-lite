# echorepo/routes/api.py
from flask import Blueprint, jsonify, abort, session, send_file, request
from io import BytesIO
import pandas as pd

from ..auth.decorators import login_required
from ..services.db import query_user_df, query_others_df, query_sample_df
from ..utils.geo import df_to_geojson, pick_lat_lon_cols
from ..config import settings

api_bp = Blueprint("api", __name__)


@api_bp.get("/user_geojson")
@login_required
def user_geojson():
    user_key = session.get("user")
    if not user_key: abort(401)
    df = query_user_df(user_key)
    return jsonify(df_to_geojson(df))

@api_bp.get("/user_geojson_debug")
@login_required
def user_geojson_debug():
    user_key = session.get("user")
    df = query_user_df(user_key)
    lat_col, lon_col = pick_lat_lon_cols(df.columns) if not df.empty else (None, None)
    info = {
        "session_user": user_key,
        "rows": 0 if df is None else len(df),
        "columns": [] if df is None else list(df.columns),
        "lat_col": lat_col, "lon_col": lon_col,
        "env_LAT_COL": settings.LAT_COL, "env_LON_COL": settings.LON_COL,
        "feature_count_if_converted": 0,
    }
    if df is not None and not df.empty and lat_col and lon_col:
        try:
            feats = sum(1 for idx, r in df.iterrows() if r.get(lat_col) and r.get(lon_col))
            info["feature_count_if_converted"] = feats
        except Exception:
            pass
    return jsonify(info)

@api_bp.get("/others_geojson")
@login_required
def others_geojson():
    user_key = session.get("user")
    if not user_key: abort(401)
    df = query_others_df(user_key)
    gj = df_to_geojson(df)
    for f in gj.get("features", []):
        if "properties" in f:
            f["properties"].pop("email", None)
            f["properties"].pop("userId", None)
    return jsonify(gj)

@api_bp.get("/download/sample_csv")
@login_required
def download_sample_csv():
    sample_id = (request.args.get("sampleId") or "").strip()
    if not sample_id:
        abort(400, description="sampleId is required")

    df = query_sample_df(sample_id)
    if df.empty:
        abort(404, description="Sample not found")

    user_key = session.get("user")
    is_owner = False
    try:
        if "email" in df.columns and (df["email"] == user_key).any():
            is_owner = True
        if "userId" in df.columns and (df["userId"] == user_key).any():
            is_owner = True
    except Exception:
        pass

    if not is_owner:
        df = df.drop(columns=["email", "userId"], errors="ignore")

    buf = BytesIO()
    df.to_csv(buf, index=False)
    buf.seek(0)
    return send_file(
        buf,
        as_attachment=True,
        download_name=f"sample_{sample_id}.csv",
        mimetype="text/csv",
    )