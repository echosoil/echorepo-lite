"""Read canonical translations only for the already-authorized personal rows."""

from contextlib import closing

import pandas as pd
from psycopg2.extras import RealDictCursor

from .db import get_pg_conn


def text_value(value):
    if value is None or pd.isna(value):
        return ""
    text = str(value).strip()
    return "" if text.lower() in {"nan", "none", "null", "<na>"} else text


def row_key(row):
    qr = text_value(row.get("QR_qrCode")).replace(" ", "").upper()
    return (qr or text_value(row.get("sampleId")), text_value(row.get("userId")))


def load_personal_translations(df):
    keys = {row_key(row) for _, row in df.iterrows()}
    keys = {(sid, uid) for sid, uid in keys if sid and uid}
    if not keys:
        return {}, {}
    ids = sorted({sid for sid, _ in keys})
    users = sorted({uid for _, uid in keys})
    with closing(get_pg_conn()) as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT sample_id, collected_by, contamination_other_orig, contamination_other_en,
                   observations_orig, observations_en, metals_info_orig, metals_info_en
            FROM samples WHERE sample_id = ANY(%s) AND collected_by = ANY(%s)
            """,
            (ids, users),
        )
        samples = {
            (row["sample_id"], row["collected_by"]): row
            for row in cur.fetchall()
            if (row["sample_id"], row["collected_by"]) in keys
        }
        cur.execute(
            """
            SELECT sample_id, collected_by, image_id, image_description_orig, image_description_en
            FROM sample_images WHERE sample_id = ANY(%s) AND collected_by = ANY(%s)
            """,
            (ids, users),
        )
        images = {
            (row["sample_id"], row["collected_by"], row["image_id"]): row
            for row in cur.fetchall()
            if (row["sample_id"], row["collected_by"]) in keys
        }
    return samples, images
