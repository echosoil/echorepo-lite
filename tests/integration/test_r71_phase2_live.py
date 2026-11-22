"""R7-1 Phase 2: actual, read-only staging integration checks.

No fixture writes to application data. API GETs may still create usage log entries.
ALL network checks are opt-in and limited to explicitly allowed staging targets.
Never run with production credentials or production URLs.
"""
from __future__ import annotations

import csv
import io
import json
import os
import subprocess
import sys
import tempfile
import zipfile
from pathlib import Path
from urllib.parse import urlsplit

import psycopg2
import pytest
import requests
from minio import Minio

from tools.r71_phase2_guard import (
    approved_pg_dsn,
    approved_url,
    endpoint_url,
    require_opt_in,
)

ROOT = Path(__file__).resolve().parents[2]
TIMEOUT = (4, 12)


def required(name: str) -> str:
    require_opt_in()
    result = os.getenv(name, "").strip()
    if not result:
        pytest.skip(f"{name} is not configured for staging")
    return result


def api_base() -> str:
    return approved_url(required("R71_API_BASE_URL"))


def api_headers() -> dict[str, str]:
    key = os.getenv("R71_API_KEY", "").strip()
    bearer = os.getenv("R71_BEARER_TOKEN", "").strip()
    if key:
        return {"X-API-Key": key}
    if bearer:
        return {"Authorization": f"Bearer {bearer}"}
    pytest.skip("R71_API_KEY or R71_BEARER_TOKEN is required for authenticated API checks")


def request_get(url: str, *, headers: dict[str, str] | None = None, params: dict | None = None):
    # Explicitly block redirects, including to unrelated identity/provider domains.
    reply = requests.get(
        url,
        headers=headers,
        params=params,
        timeout=TIMEOUT,
        allow_redirects=False,
    )
    assert reply.status_code not in {301, 302, 303, 307, 308}, "Unexpected redirect from staging"
    return reply


def pg_connection():
    dsn = required("R71_PG_DSN")
    name = approved_pg_dsn(dsn)
    expected_name = required("R71_PG_EXPECT_DB")
    assert name == expected_name, "Staging PostgreSQL database name differs from approved value"
    conn = psycopg2.connect(dsn, connect_timeout=5)
    conn.set_session(readonly=True, autocommit=False)
    return conn


def local_bundle() -> Path:
    require_opt_in()
    path = required("R71_CANONICAL_BUNDLE")
    bundle = Path(path).expanduser().resolve(strict=True)
    assert bundle.is_file(), "R71_CANONICAL_BUNDLE must be a local file"
    assert bundle.stat().st_size <= 100 * 1024 * 1024, "Bundle exceeds safe 100 MiB staging cap"
    assert zipfile.is_zipfile(bundle), "Expected a canonical ZIP archive"
    with zipfile.ZipFile(bundle) as archive:
        assert len(archive.infolist()) <= 30, "Unexpected number of ZIP members"
        assert sum(x.file_size for x in archive.infolist()) <= 250 * 1024 * 1024, (
            "Uncompressed canonical resources exceed safety cap"
        )
    return bundle


def test_r71_i01_staging_api_health():
    url = endpoint_url(api_base(), "/api/v1/ping")
    reply = request_get(url)
    assert reply.status_code == 200
    assert reply.json() == {"ok": True}


def test_r71_i02_staging_private_bundle_rejects_anonymous():
    url = endpoint_url(api_base(), "/api/v1/canonical/all.zip")
    reply = request_get(url)  # Deliberately NO credentials and NO body download.
    assert reply.status_code == 401, "Canonical ZIP must require authentication"


def test_r71_i03_staging_authenticated_api_pagination():
    url = endpoint_url(api_base(), "/api/v1/canonical/samples")
    reply = request_get(
        url,
        headers=api_headers(),
        params={
            "format": "json",
            "fields": "sample_id,country_code,lat,lon",
            "limit": 3,
            "offset": 0,
            "order": "sample_id",
            "dir": "asc",
        },
    )
    assert reply.status_code == 200, f"Canonical sample request returned HTTP {reply.status_code}"
    result = reply.json()
    assert result["meta"]["limit"] == 3
    assert result["meta"]["offset"] == 0
    assert len(result["data"]) <= 3
    assert isinstance(result["meta"]["count"], int)
    assert result["meta"]["count"] >= len(result["data"])
    assert all(
        "collected_by" not in row
        and set(row).issubset({"sample_id", "country_code", "lat", "lon"})
        for row in result["data"]
    )


def test_r71_i04_staging_api_count_and_bounded_csv():
    base = api_base()
    headers = api_headers()
    count = request_get(endpoint_url(base, "/api/v1/canonical/samples/count"), headers=headers)
    csv_reply = request_get(
        endpoint_url(base, "/api/v1/canonical/samples"),
        headers=headers,
        params={"format": "csv", "fields": "sample_id,country_code", "limit": 3},
    )
    assert count.status_code == 200
    assert csv_reply.status_code == 200
    assert count.json()["count"] >= 0
    lines = list(csv.DictReader(io.StringIO(csv_reply.content.decode("utf-8-sig"))))
    assert len(lines) <= 3
    assert all(set(row) == {"sample_id", "country_code"} for row in lines)
    assert set(csv.DictReader(io.StringIO(csv_reply.text)).fieldnames or []) == {
        "sample_id", "country_code",
    }


def test_r71_i05_postgres_readonly_schema_and_count():
    with pg_connection() as connection:
        with connection.cursor() as cur:
            cur.execute("SHOW transaction_read_only")
            assert cur.fetchone()[0] == "on"
            cur.execute("SET LOCAL statement_timeout = '8000ms'")
            cur.execute(
                "SELECT to_regclass('public.samples'), "
                "to_regclass('public.sample_images'), "
                "to_regclass('public.sample_parameters')"
            )
            names = cur.fetchone()
            assert all(name is not None for name in names)
            cur.execute("SELECT COUNT(*) FROM public.samples")
            count = cur.fetchone()[0]
            assert isinstance(count, int) and count >= 0
        connection.rollback()


def test_r71_i06_postgres_matches_api_sample_count():
    base = api_base()
    headers = api_headers()
    with pg_connection() as connection:
        with connection.cursor() as cur:
            cur.execute("SET LOCAL statement_timeout = '8000ms'")
            cur.execute("SELECT COUNT(*) FROM public.samples")
            db_count = cur.fetchone()[0]
        connection.rollback()
    reply = request_get(endpoint_url(base, "/api/v1/canonical/samples/count"), headers=headers)
    assert reply.status_code == 200
    assert reply.json()["count"] == db_count, (
        "API and staging database counts differ (check routing, sync jobs, and concurrent imports)"
    )


def test_r71_i07_keycloak_discovery_and_jwks():
    issuer = approved_url(required("R71_KEYCLOAK_ISSUER"), require_https=True)
    discovery = request_get(issuer + "/.well-known/openid-configuration")
    assert discovery.status_code == 200
    metadata = discovery.json()
    assert metadata.get("issuer", "").rstrip("/") == issuer
    jwks_uri = approved_url(metadata["jwks_uri"], require_https=True)
    jwks = request_get(jwks_uri)
    assert jwks.status_code == 200
    keys = jwks.json().get("keys")
    assert isinstance(keys, list) and any(key.get("kid") and key.get("kty") for key in keys)


def test_r71_i08_minio_canonical_snapshot_exists():
    parsed = urlsplit(approved_url(required("R71_MINIO_ENDPOINT")))
    access_key = required("R71_MINIO_ACCESS_KEY")
    secret_key = required("R71_MINIO_SECRET_KEY")
    bucket = required("R71_MINIO_BUCKET")
    client = Minio(
        parsed.netloc,
        access_key=access_key,
        secret_key=secret_key,
        secure=parsed.scheme == "https",
    )
    assert client.bucket_exists(bucket), "Configured staging bucket is absent"
    stat = client.stat_object(bucket, "canonical/latest/all.zip")
    assert stat.size > 0, "Staging canonical/latest/all.zip must not be empty"


def test_r71_i09_local_canonical_bundle_validates():
    archive = local_bundle()
    command = [sys.executable, str(ROOT / "tools/validate_echorepo_bundle.py"), str(archive)]
    completed = subprocess.run(
        command, capture_output=True, text=True, timeout=120, cwd=ROOT, check=False
    )
    # Do not include validator text here: a failed bundle could contain sample identifiers.
    assert completed.returncode == 0, (
        "Local canonical ZIP failed schema/QA/foreign-key validation; run validator manually"
    )


def test_r71_i10_zenodo_csvw_dry_run_no_publication():
    """Call real publisher with --source-file and --dry-run; never contact Zenodo."""
    source = local_bundle()
    with tempfile.TemporaryDirectory(prefix="r71-staging-") as directory:
        temp = Path(directory)
        prepared = temp / "prepared"
        log_file = temp / "zenodo_dry_run_log.csv"
        command = [
            sys.executable, str(ROOT / "tools/publish_api_file_to_zenodo.py"),
            "--env-file", str(temp / "nonexistent_env_file"),
            "--source-file", str(source),
            "--dry-run",
            "--sandbox",
            "--validate-canonical-schema",
            "--title", "ECHOREPO R7-1 staging validation",
            "--description", "Local offline metadata validation only",
            "--creator", "ECHO Consortium",
            "--metadata-config", str(ROOT / "metadata/canonical/echorepo_columns.json"),
            "--log-file", str(log_file),
            "--save-prepared-dir", str(prepared),
        ]
        env = os.environ.copy()
        # Do not import operational publication secrets into the child process.
        for key in (
            "ACCESS_TOKEN", "ZENODO_ACCESS_TOKEN", "ECHOREPO_API_KEY",
            "ECHOREPO_BEARER_TOKEN", "ZENODO_API_BASE", "ZENODO_API_PATH",
            "ZENODO_DEPOSITION_ID", "SOILWISE_VALIDATE", "SOILWISE_REQUIRE_VALIDATION",
        ):
            env.pop(key, None)
        completed = subprocess.run(
            command, capture_output=True, text=True, timeout=120, cwd=ROOT,
            env=env, check=False,
        )
        assert completed.returncode == 0, (
            "Offline Zenodo CSVW preparation failed; inspect a local dry-run outside CI"
        )
        output = json.loads(completed.stdout)
        assert output["ok"] is True and output["dry_run"] is True
        assert output["canonical_schema_validated"] is True
        assert len(output["csv_files"]) == 6
        assert (prepared / "file.json").is_file()
        metadata = json.loads((prepared / "file.json").read_text(encoding="utf-8"))
        assert {table["url"] for table in metadata["tables"]} == set(output["csv_files"])
        with log_file.open(newline="", encoding="utf-8") as stream:
            records = list(csv.DictReader(stream))
        assert records and records[-1]["status"] == "dry-run"


if __name__ == "__main__":
    raise SystemExit("Use pytest, not a standalone script")
