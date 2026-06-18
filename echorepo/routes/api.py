# echorepo/routes/api.py
from io import BytesIO

from flask import Blueprint, abort, jsonify, request, send_file, session

from ..auth.decorators import login_required
from ..config import settings
from ..services.db import get_pg_conn, query_others_df, query_sample_df, query_user_df
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

def _feature_sample_id(props):
    """
    Return the canonical sample id used in Postgres.

    Important:
    In legacy SQLite data, sampleId can be the Firestore document id,
    while QR_qrCode is the real sample id like NMCG-9470.
    Therefore QR_qrCode must be preferred.
    """
    props = props or {}

    return (
        props.get("QR_qrCode")
        or props.get("qr_code")
        or props.get("qr")
        or props.get("sample_id")
        or props.get("Sample")
        or props.get("sampleId")
    )


def _inject_pg_qa_status(gj):
    """
    Inject qa_status from Postgres into legacy GeoJSON.

    /api/user_geojson and /api/others_geojson are built from SQLite,
    but Postgres currently has the reliable qa_status values.
    """
    if not gj or not isinstance(gj, dict):
        return gj

    features = gj.get("features") or []

    sample_ids = []
    for f in features:
        props = f.get("properties") or {}
        sid = _feature_sample_id(props)
        if sid:
            sample_ids.append(str(sid).strip())

    sample_ids = sorted({sid for sid in sample_ids if sid})
    if not sample_ids:
        return gj

    qa_by_id = {}

    try:
        with get_pg_conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT sample_id, qa_status
                FROM samples
                WHERE sample_id = ANY(%s)
                """,
                (sample_ids,),
            )

            for sample_id, qa_status in cur.fetchall():
                qa_by_id[str(sample_id)] = qa_status

    except Exception:
        # Do not break the map if this lookup fails.
        return gj

    for f in features:
        props = f.get("properties") or {}
        sid = _feature_sample_id(props)
        if not sid:
            continue

        qa_status = qa_by_id.get(str(sid).strip())

        if qa_status:
            props["qa_status"] = qa_status
            props["wrong_coordinates"] = (
                str(qa_status).strip().lower().startswith("wrong_coordinates")
            )
        else:
            existing_qa = str(props.get("qa_status") or "").strip().lower()
            props["wrong_coordinates"] = (
                _truthy_flag(props.get("wrong_coordinates"))
                or existing_qa.startswith("wrong_coordinates")
            )

    return gj


@api_bp.get("/user_geojson")
@login_required
def user_geojson():
    user_key = session.get("user")
    if not user_key:
        abort(401)

    df = query_user_df(user_key)
    df = _add_coordinate_qa_columns(df)

    gj = df_to_geojson(df)
    gj = _inject_pg_qa_status(gj)

    return jsonify(gj)


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
    gj = _inject_pg_qa_status(gj)

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