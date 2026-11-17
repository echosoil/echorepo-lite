#!/usr/bin/env python3
"""Consume API-uploaded biodiversity files using a single durable DB queue.

Run inside Docker in a dedicated worker container. Uploads are committed to
PostgreSQL before their queued staging files are removed.
"""
from __future__ import annotations

import argparse
import hashlib
import logging
import os
import sys
import time
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))
load_dotenv(PROJECT_ROOT / ".env")

from echorepo.services.biodiversity_api_jobs import staging_path  # noqa: E402
from echorepo.services.biodiversity_import import import_biodiversity_file  # noqa: E402
from echorepo.services.db import get_pg_conn  # noqa: E402

LOG = logging.getLogger("biodiversity-import-worker")
ADVISORY_LOCK = 20261117001


def check_file(path: Path, expected_size: int, expected_sha: str):
    if not path.is_file():
        raise FileNotFoundError(f"Queued source file missing: {path}")
    if path.stat().st_size != expected_size:
        raise ValueError(f"Staged file size does not match receipt: {path}")
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    if digest.hexdigest() != expected_sha:
        raise ValueError(f"Staged file SHA-256 does not match receipt: {path}")


def claim_next(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            WITH job AS (
                SELECT job_id
                FROM biodiversity_import_jobs
                WHERE status = 'queued'
                ORDER BY queued_at, job_id
                FOR UPDATE SKIP LOCKED
                LIMIT 1
            )
            UPDATE biodiversity_import_jobs AS j
            SET status = 'running', started_at = now(), error_message = NULL
            FROM job
            WHERE j.job_id = job.job_id
            RETURNING
                j.job_id::text, j.original_filename, j.source_sha256,
                j.size_bytes, j.uploaded_by
            """
        )
        return cur.fetchone()


def finish(conn, job_id: str, *, rows: int | None, error: str | None):
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE biodiversity_import_jobs
            SET status = %s,
                completed_at = now(),
                inserted_rows = %s,
                error_message = %s
            WHERE job_id = %s::uuid AND status = 'running'
            """,
            (
                "failed" if error is not None else "succeeded",
                rows,
                error[:3000] if error else None,
                job_id,
            ),
        )


def run(*, staging_dir: Path, poll_seconds: int, once: bool) -> None:
    # A session-level PostgreSQL advisory lock prevents concurrent workers
    # from importing overlapping sample-column parts.
    with get_pg_conn() as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("SELECT pg_try_advisory_lock(%s)", (ADVISORY_LOCK,))
            if not cur.fetchone()[0]:
                raise RuntimeError("Another biodiversity-import worker is running")
        try:
            while True:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT to_regclass('public.biodiversity_import_jobs')"
                    )
                    exists = cur.fetchone()[0] is not None
                if exists:
                    break
                LOG.warning("Waiting for biodiversity_import_jobs migration...")
                if once:
                    raise RuntimeError("biodiversity_import_jobs migration missing")
                time.sleep(poll_seconds)

            # Only the worker owning the advisory lock can recover jobs that
            # were running before an abrupt worker/container shutdown.
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE biodiversity_import_jobs
                    SET status = 'queued', started_at = NULL
                    WHERE status = 'running'
                    """
                )

            LOG.warning("Biodiversity import worker ready; dir=%s", staging_dir)
            while True:
                job = claim_next(conn)
                if job is None:
                    if once:
                        return
                    time.sleep(poll_seconds)
                    continue

                job_id, filename, digest, size_bytes, uploader_id = job
                path = staging_path(staging_dir, job_id, filename)
                LOG.warning(
                    "Processing job %s: filename=%s size=%s",
                    job_id, filename, size_bytes
                )
                try:
                    check_file(path, int(size_bytes), digest)
                    rows = import_biodiversity_file(
                        path,
                        filename,
                        uploader_id,
                        archive_raw=False,
                        source_sha256=digest,
                        post_min10=False,
                    )
                    finish(conn, job_id, rows=rows, error=None)
                    LOG.warning("Job %s succeeded: %s Phylum rows", job_id, rows)
                except Exception as exc:
                    LOG.exception("Job %s failed", job_id)
                    finish(conn, job_id, rows=None, error=str(exc))
                finally:
                    try:
                        path.unlink(missing_ok=True)
                    except OSError:
                        LOG.exception("Could not remove staged input: %s", path)

                if once:
                    return
        finally:
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT pg_advisory_unlock(%s)", (ADVISORY_LOCK,))
            except Exception:
                LOG.exception("Could not release advisory lock")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--once", action="store_true", help="Process at most one job")
    parser.add_argument("--poll-seconds", type=int, default=5)
    args = parser.parse_args()
    if args.poll_seconds < 1:
        parser.error("--poll-seconds must be positive")
    stage_dir = Path(
        os.getenv("BIODIVERSITY_UPLOAD_STAGING_DIR", "/data/biodiversity/incoming")
    )
    run(
        staging_dir=stage_dir,
        poll_seconds=args.poll_seconds,
        once=args.once,
    )


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    main()
