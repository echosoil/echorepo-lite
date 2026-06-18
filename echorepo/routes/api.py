# echorepo/routes/api.py
from io import BytesIO

from flask import Blueprint, abort, jsonify, request, send_file, session

from ..auth.decorators import login_required
from ..config import settings
from ..services.db import query_others_df, query_sample_df, query_user_df
from ..utils.geo import df_to_geojson, pick_lat_lon_cols

api_bp = Blueprint("api", __name__)


def _truthy_flag(v) -> bool:
    return str(v or "").strip().lower() in {"true", "1", "yes", "y", "t"}


def _add_coordinate_qa_columns(df):
    """
    Ensure legacy SQLite GeoJSON exposes wrong-coordinate information.

    df_to_geojson(df) will include these dataframe columns in feature.properties:
      - qa_status
      - wrong_coordinates
      - coordinate_check_reason
    """
    if df is None or df.empty:
        return df

    df = df.copy()

    if "wrong_coordinates" not in df.columns:
        df["wrong_coordinates"] = False

    if "coordinate_check_reason" not in df.columns:
        df["coordinate_check_reason"] = ""

    def build_qa(row):
        existing = ""
        if "qa_status" in df.columns:
            existing = str(row.get("qa_status") or "").strip()

        if existing:
            return existing

        if _truthy_flag(row.get("wrong_coordinates")):
            reason = str(row.get("coordinate_check_reason") or "").strip()
            return f"wrong_coordinates:{reason}" if reason else "wrong_coordinates"

        return None

    df["qa_status"] = df.apply(build_qa, axis=1)

    df["wrong_coordinates"] = df.apply(
        lambda row: (
            _truthy_flag(row.get("wrong_coordinates"))
            or str(row.get("qa_status") or "")
            .strip()
            .lower()
            .startswith("wrong_coordinates")
        ),
        axis=1,
    )

    return df


@api_bp.get("/user_geojson")
@login_required
def user_geojson():
    user_key = session.get("user")
    if not user_key:
        abort(401)

    df = query_user_df(user_key)
    df = _add_coordinate_qa_columns(df)

    return jsonify(df_to_geojson(df))


@api_bp.get("/user_geojson_debug")
@login_required
def user_geojson_debug():
    user_key = session.get("user")
    df = query_user_df(user_key)

    lat_col, lon_col = pick_lat_lon_cols(df.columns) if df is not None and not df.empty else (None, None)

    info = {
        "session_user": user_key,
        "rows": 0 if df is None else len(df),
        "columns": [] if df is None else list(df.columns),
        "lat_col": lat_col,
        "lon_col": lon_col,
        "env_LAT_COL": settings.LAT_COL,
        "env_LON_COL": settings.LON_COL,
        "feature_count_if_converted": 0,
    }

    if df is not None and not df.empty and lat_col and lon_col:
        try:
            feats = sum(
                1
                for _, r in df.iterrows()
                if r.get(lat_col) not in (None, "")
                and r.get(lon_col) not in (None, "")
            )
            info["feature_count_if_converted"] = feats
        except Exception:
            pass

    return jsonify(info)


@api_bp.get("/others_geojson")
@login_required
def others_geojson():
    user_key = session.get("user")
    if not user_key:
        abort(401)

    df = query_others_df(user_key)
    df = _add_coordinate_qa_columns(df)

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