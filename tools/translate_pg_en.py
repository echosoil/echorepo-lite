#!/usr/bin/env python3
"""
Translate missing *_en fields in Postgres canonical tables using LibreTranslate.

- Works on:
    - samples.contamination_other_en / soil_structure_en / soil_texture_en /
      observations_en / metals_info_en
    - sample_images.image_description_en

- Only translates rows where:
    *_orig is non-empty AND *_en IS NULL/empty.
- Uses the same LT logic as echorepo.services.translate_en.
"""

import os
import sys
from pathlib import Path
from typing import Dict, Tuple

from dotenv import load_dotenv
import psycopg2

# ---------------------------------------------------------------------------
# Project root / imports
# ---------------------------------------------------------------------------
# Load .env as early as possible so PROJECT_ROOT from .env is picked up
DEFAULT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(DEFAULT_ROOT / ".env")

PROJECT_ROOT = Path(os.getenv("PROJECT_ROOT", DEFAULT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT))

from echorepo.services.translate_en import translate_many_to_en  # type: ignore


# ---------------------------------------------------------------------------
# PG connection
# ---------------------------------------------------------------------------
def _get_pg_conn():
    """
    Connect to Postgres.

    Priority:
      - DB_HOST_INSIDE / DB_PORT_INSIDE (inside container)
      - DB_HOST_OUTSIDE / DB_PORT_OUTSIDE (from host)
      - DB_HOST / DB_PORT (generic)
      - defaults: host='postgres', port=5432
    """
    host = (
        os.getenv("DB_HOST_INSIDE")
        or os.getenv("DB_HOST_OUTSIDE")
        or os.getenv("DB_HOST")
        or "postgres"
    )
    port = int(
        os.getenv("DB_PORT_INSIDE")
        or os.getenv("DB_PORT_OUTSIDE")
        or os.getenv("DB_PORT", "5432")
    )

    dbname = os.getenv("DB_NAME", "echorepo")
    user = os.getenv("DB_USER", "echorepo")
    password = os.getenv("DB_PASSWORD", "echorepo-pass")

    print(f"[PG] Connecting to {host}:{port}/{dbname} as {user}")
    return psycopg2.connect(
        host=host,
        port=port,
        dbname=dbname,
        user=user,
        password=password,
    )


def _norm_cc(cc: str | None) -> str:
    return (cc or "").strip().upper()


# ---------------------------------------------------------------------------
# samples translation
# ---------------------------------------------------------------------------
def translate_samples(conn) -> None:
    """
    Translate missing *_en text columns in samples table.
    """
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            sample_id,
            country_code,
            contamination_debris,  -- not used for translation, just FYI if needed
            contamination_plastic, -- idem
            contamination_other_orig,
            contamination_other_en,
            soil_structure_orig,
            soil_structure_en,
            soil_texture_orig,
            soil_texture_en,
            observations_orig,
            observations_en,
            metals_info_orig,
            metals_info_en
        FROM samples
        WHERE
            (
                contamination_other_orig IS NOT NULL
                AND contamination_other_orig <> ''
                AND (contamination_other_en IS NULL OR contamination_other_en = '')
            )
            OR (
                soil_structure_orig IS NOT NULL
                AND soil_structure_orig <> ''
                AND (soil_structure_en IS NULL OR soil_structure_en = '')
            )
            OR (
                soil_texture_orig IS NOT NULL
                AND soil_texture_orig <> ''
                AND (soil_texture_en IS NULL OR soil_texture_en = '')
            )
            OR (
                observations_orig IS NOT NULL
                AND observations_orig <> ''
                AND (observations_en IS NULL OR observations_en = '')
            )
            OR (
                metals_info_orig IS NOT NULL
                AND metals_info_orig <> ''
                AND (metals_info_en IS NULL OR metals_info_en = '')
            )
        ;
        """
    )
    rows = cur.fetchall()
    if not rows:
        print("[TR] samples: no rows with missing translations.")
        cur.close()
        return

    print(f"[TR] samples: {len(rows)} rows need translation")

    # collect unique (text, country_code) pairs
    pairs_set = set()
    # per-sample pending translations: sample_id -> {en_col: (text, CC)}
    pending: Dict[str, Dict[str, Tuple[str, str]]] = {}

    for (
        sample_id,
        country_code,
        _cont_debris,
        _cont_plastic,
        contamination_other_orig,
        contamination_other_en,
        soil_structure_orig,
        soil_structure_en,
        soil_texture_orig,
        soil_texture_en,
        observations_orig,
        observations_en,
        metals_info_orig,
        metals_info_en,
    ) in rows:
        sid = str(sample_id).strip()
        CC = _norm_cc(country_code)

        cols = [
            ("contamination_other_orig", "contamination_other_en", contamination_other_orig, contamination_other_en),
            ("soil_structure_orig", "soil_structure_en", soil_structure_orig, soil_structure_en),
            ("soil_texture_orig", "soil_texture_en", soil_texture_orig, soil_texture_en),
            ("observations_orig", "observations_en", observations_orig, observations_en),
            ("metals_info_orig", "metals_info_en", metals_info_orig, metals_info_en),
        ]

        for _orig_name, en_col, orig_val, en_val in cols:
            txt = (orig_val or "").strip()
            already = (en_val or "").strip() if en_val is not None else ""
            if not txt:
                continue
            if already:
                continue
            key = (txt, CC)
            pairs_set.add(key)
            pending.setdefault(sid, {})[en_col] = key

    if not pairs_set:
        print("[TR] samples: nothing to translate after filtering.")
        cur.close()
        return

    pairs_list = list(pairs_set)
    print(f"[TR] samples: unique (text,CC) pairs to translate: {len(pairs_list)}")
    # DEBUG: show a couple of examples
    print("[TR] samples: example pairs:", pairs_list[:5])

    en_map = translate_many_to_en(pairs_list)

    # apply updates
    updated_rows = 0
    for sample_id, cols in pending.items():
        set_parts = []
        values = []
        for en_col, pair in cols.items():
            translated = en_map.get(pair, pair[0])
            set_parts.append(f"{en_col} = %s")
            values.append(translated)
        if not set_parts:
            continue
        sql = f"UPDATE samples SET {', '.join(set_parts)} WHERE sample_id = %s"
        cur.execute(sql, values + [sample_id])
        updated_rows += 1

    conn.commit()
    cur.close()
    print(f"[TR] samples: updated {updated_rows} rows.")


# ---------------------------------------------------------------------------
# sample_images translation
# ---------------------------------------------------------------------------
def translate_sample_images(conn) -> None:
    """
    Translate missing image_description_en in sample_images.
    """
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            sample_id,
            image_id,
            country_code,
            image_description_orig,
            image_description_en
        FROM sample_images
        WHERE
            image_description_orig IS NOT NULL
            AND image_description_orig <> ''
            AND (image_description_en IS NULL OR image_description_en = '')
        ;
        """
    )
    rows = cur.fetchall()
    if not rows:
        print("[TR] sample_images: no rows with missing translations.")
        cur.close()
        return

    print(f"[TR] sample_images: {len(rows)} rows need translation")

    pairs_set = set()
    pending_imgs: Dict[Tuple[str, int], Tuple[str, str]] = {}

    for sample_id, image_id, country_code, orig, en_val in rows:
        sid = str(sample_id).strip()
        # robust int cast (in case we ever get Decimal or str)
        try:
            image_id_int = int(image_id)
        except Exception:
            print(f"[TR] WARN: unexpected image_id={image_id!r} for sample {sid}, skipping.")
            continue

        CC = _norm_cc(country_code)
        txt = (orig or "").strip()
        if not txt:
            continue
        already = (en_val or "").strip() if en_val is not None else ""
        if already:
            continue
        pair = (txt, CC)
        pairs_set.add(pair)
        pending_imgs[(sid, image_id_int)] = pair

    if not pairs_set:
        print("[TR] sample_images: nothing to translate after filtering.")
        cur.close()
        return

    pairs_list = list(pairs_set)
    print(f"[TR] sample_images: unique (text,CC) pairs to translate: {len(pairs_list)}")
    print("[TR] sample_images: example pairs:", pairs_list[:5])

    en_map = translate_many_to_en(pairs_list)

    updated_rows = 0
    for (sample_id, image_id), pair in pending_imgs.items():
        translated = en_map.get(pair, pair[0])
        cur.execute(
            """
            UPDATE sample_images
               SET image_description_en = %s
             WHERE sample_id = %s
               AND image_id  = %s
            """,
            (translated, sample_id, image_id),
        )
        updated_rows += 1

    conn.commit()
    cur.close()
    print(f"[TR] sample_images: updated {updated_rows} rows.")


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------
def main():
    env_path = PROJECT_ROOT / ".env"
    if env_path.exists():
        # we already loaded once above, but this keeps behaviour if PROJECT_ROOT differs
        load_dotenv(env_path)
        print(f"[TR] Loaded env from {env_path}")
    else:
        print("[TR] No .env at PROJECT_ROOT, relying on process env")

    conn = _get_pg_conn()
    try:
        translate_samples(conn)
        translate_sample_images(conn)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
