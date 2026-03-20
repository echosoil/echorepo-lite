#!/usr/bin/env python3
from __future__ import annotations

import io
import math
import os
import re
import sys
import tempfile
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Load .env exactly like pull_and_enrich_samples.py
# ---------------------------------------------------------------------------
env_path = Path.cwd() / ".env"
load_dotenv(dotenv_path=env_path)
print(f"[INFO] Loaded environment from {env_path}")

# ---------------------------------------------------------------------------
# Make sure project root is importable
# ---------------------------------------------------------------------------
THIS_DIR = Path(__file__).resolve().parent
DEFAULT_ROOT = THIS_DIR.parent
PROJECT_ROOT = Path(os.getenv("PROJECT_ROOT", str(DEFAULT_ROOT)))
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
print(f"[INFO] Using PROJECT_ROOT={PROJECT_ROOT}")

# ---------------------------------------------------------------------------
# MinIO config: same style as pull_and_enrich_samples.py
# ---------------------------------------------------------------------------
try:
    from minio import Minio
    from minio.error import S3Error
except ImportError:
    Minio = None

    class S3Error(Exception):
        pass


MINIO_ENDPOINT = (
    os.getenv("MINIO_ENDPOINT_INSIDE")
    or os.getenv("MINIO_ENDPOINT_OUTSIDE")
    or os.getenv("MINIO_ENDPOINT")
    or "localhost:9000"
)
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY") or os.getenv("MINIO_ROOT_USER") or ""
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY") or os.getenv("MINIO_ROOT_PASSWORD") or ""
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "echorepo-uploads")
PUBLIC_STORAGE_BASE = os.getenv("PUBLIC_STORAGE_BASE", "/storage")

# ---------------------------------------------------------------------------
# Postgres config
# ---------------------------------------------------------------------------
try:
    import psycopg2
except ImportError:
    psycopg2 = None


def get_pg_conn():
    if psycopg2 is None:
        raise RuntimeError("psycopg2 is not installed")

    host = (
        os.getenv("DB_HOST_OUTSIDE")
        or os.getenv("DB_HOST_INSIDE")
        or os.getenv("DB_HOST")
        or "localhost"
    )
    port = int(
        os.getenv("DB_PORT_OUTSIDE")
        or os.getenv("DB_PORT_INSIDE")
        or os.getenv("DB_PORT")
        or "5432"
    )

    return psycopg2.connect(
        host=host,
        port=port,
        dbname=os.getenv("DB_NAME", "echorepo"),
        user=os.getenv("DB_USER", "echorepo"),
        password=os.getenv("DB_PASSWORD", "echorepo-pass"),
    )


def init_minio():
    if Minio is None:
        print("[INFO] python-minio not installed; skipping MinIO upload.")
        return None

    secure = False
    endpoint = MINIO_ENDPOINT
    if endpoint.startswith("https://"):
        secure = True
        endpoint = endpoint[len("https://") :]
    elif endpoint.startswith("http://"):
        secure = False
        endpoint = endpoint[len("http://") :]

    if not MINIO_ACCESS_KEY or not MINIO_SECRET_KEY:
        print("[WARN] MinIO credentials not set; skipping chart upload.")
        return None

    client = Minio(
        endpoint,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=secure,
    )

    try:
        found = client.bucket_exists(MINIO_BUCKET)
        if not found:
            client.make_bucket(MINIO_BUCKET)
            print(f"[INFO] Created MinIO bucket {MINIO_BUCKET}")
    except Exception as e:
        print(f"[WARN] Could not ensure MinIO bucket: {e}")
        return None

    print(f"[INFO] MinIO ready at {MINIO_ENDPOINT}, bucket={MINIO_BUCKET}")
    return client


def upload_file_to_minio(
    mclient, local_path: Path, object_name: str, content_type: str = "image/png"
):
    if mclient is None:
        return None

    try:
        size = local_path.stat().st_size
        with local_path.open("rb") as f:
            mclient.put_object(
                MINIO_BUCKET,
                object_name,
                data=f,
                length=size,
                content_type=content_type,
            )
        print(f"[OK] uploaded to MinIO: {object_name}")
        return f"{PUBLIC_STORAGE_BASE}/{object_name}"
    except Exception as e:
        print(f"[WARN] could not upload {local_path} to MinIO as {object_name}: {e}")
        return None


def sanitize_filename(s: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", str(s).strip())


def fetch_otu_data(marker: str = "16S") -> pd.DataFrame:
    sql = """
        SELECT sample_id, otu_id, count, taxa
        FROM sample_otu_counts
        WHERE marker = %s
    """
    with get_pg_conn() as conn:
        df = pd.read_sql(sql, conn, params=[marker])
    return df


def extract_taxon_label(row: pd.Series, level: str) -> str:
    taxa = row.get("taxa")

    if isinstance(taxa, dict):
        if level in taxa and taxa[level]:
            return str(taxa[level]).strip()
        # fallback to common letters
        fallback_map = {
            "Kingdom": "A",
            "Phylum": "B",
            "Order": "C",
            "Family": "D",
            "Genus": "E",
            "Genus2": "F",
        }
        alt = fallback_map.get(level)
        if alt and taxa.get(alt):
            return str(taxa[alt]).strip()

    return "Unclassified"


def make_piechart_for_sample(
    sample_df: pd.DataFrame, sample_id: str, marker: str, level: str, out_path: Path
):
    plot_df = sample_df.copy()
    plot_df["taxon"] = plot_df.apply(lambda r: extract_taxon_label(r, level), axis=1)
    plot_df["count"] = pd.to_numeric(plot_df["count"], errors="coerce").fillna(0)

    grouped = (
        plot_df.groupby("taxon", dropna=False)["count"]
        .sum()
        .reset_index()
        .sort_values("count", ascending=False)
    )

    grouped = grouped[grouped["count"] > 0].copy()
    if grouped.empty:
        return False

    top_n = 10
    if len(grouped) > top_n:
        top = grouped.iloc[:top_n].copy()
        other_sum = grouped.iloc[top_n:]["count"].sum()
        if other_sum > 0:
            top = pd.concat(
                [top, pd.DataFrame([{"taxon": "Other", "count": other_sum}])],
                ignore_index=True,
            )
        grouped = top

    total = grouped["count"].sum()
    grouped["pct"] = grouped["count"] / total * 100.0

    fig, ax = plt.subplots(figsize=(10, 7))
    ax.pie(
        grouped["pct"],
        labels=grouped["taxon"],
        autopct="%1.1f%%",
        startangle=90,
    )
    ax.set_title(f"Top {min(top_n, len(grouped))} {level} — {sample_id} ({marker})")
    ax.axis("equal")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=300, bbox_inches="tight")
    plt.close(fig)
    return True


def main():
    marker = os.getenv("BIODIV_MARKER", "16S")
    level = os.getenv("BIODIV_LEVEL", "Order")
    out_dir = PROJECT_ROOT / "data" / "biodiversity_piecharts" / marker / level
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"[INFO] marker={marker} level={level}")
    df = fetch_otu_data(marker=marker)

    if df.empty:
        print("[INFO] No OTU rows found.")
        return

    mclient = init_minio()

    generated = 0
    uploaded = 0

    for sample_id, sample_df in df.groupby("sample_id"):
        sample_id = str(sample_id).strip()
        if not sample_id:
            continue

        local_png = out_dir / f"{sanitize_filename(sample_id)}.png"
        ok = make_piechart_for_sample(sample_df, sample_id, marker, level, local_png)
        if not ok:
            continue

        generated += 1

        object_name = f"biodiversity/piecharts/{marker}/{level}/{sanitize_filename(sample_id)}.png"
        uploaded_url = upload_file_to_minio(
            mclient,
            local_png,
            object_name,
            content_type="image/png",
        )
        if uploaded_url:
            uploaded += 1

    print(f"[OK] Generated {generated} charts")
    print(f"[OK] Uploaded {uploaded} charts to MinIO")


if __name__ == "__main__":
    main()
