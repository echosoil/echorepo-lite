"""PostgreSQL integration: two distinct QR-column imports retain both samples.

Enabled in CI via BIODIV_DB_TEST=1. Uses disposable PostgreSQL service.
"""
from __future__ import annotations

import gzip
import os
import tempfile
import unittest
from pathlib import Path

import psycopg2
from flask import Flask

from echorepo.routes.data_api import data_api
from tools.consume_biodiversity_jobs import run


@unittest.skipUnless(os.getenv("BIODIV_DB_TEST") == "1", "requires temporary PostgreSQL")
class BiodiversityQueueIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        root = Path(__file__).resolve().parents[1]
        cls.conn = psycopg2.connect(
            host=os.getenv("DB_HOST_INSIDE", "127.0.0.1"),
            port=int(os.getenv("DB_PORT_INSIDE", "5432")),
            dbname=os.getenv("DB_NAME", "echorepo"),
            user=os.getenv("DB_USER", "echorepo"),
            password=os.getenv("DB_PASSWORD", "echorepo-pass"),
        )
        with cls.conn.cursor() as cur:
            for name in (
                "00xx_compact_biodiversity.sql",
                "00xy_raw_biodiversity.sql",
                "20261117_001_biodiversity_import_jobs.sql",
            ):
                cur.execute(
                    (root / "migrations" / "postgres" / name).read_text()
                )
        cls.conn.commit()

    @classmethod
    def tearDownClass(cls):
        cls.conn.close()

    def test_sequential_qr_column_parts_preserve_both_samples(self):
        with tempfile.TemporaryDirectory() as tmp:
            app = Flask("biodiversity-queue-test")
            app.config.update(
                TESTING=True,
                API_KEY="test-key",
                BIODIVERSITY_UPLOAD_STAGING_DIR=tmp,
                BIODIVERSITY_API_MAX_UPLOAD_BYTES=1_000_000,
            )
            app.secret_key = "only-for-tests"
            app.register_blueprint(data_api, url_prefix="/api/v1")
            client = app.test_client()

            jobs = []
            for i, sample in enumerate(("ABCD-1234", "EFGH-5678"), 1):
                csv_data = (
                    f"OTU ID,{sample}-16S,Kingdom,Phylum,Class,Order,Family,Genus,Species\n"
                    f"otu1,{10 + i},Bacteria,Acidobacteriota,C,O,F,G,S\n"
                    f"otu2,{4 + i},Bacteria,Bacillota,C,O,F,G,S\n"
                ).encode()
                # Each QR column stays exclusive to one file, including gzip.
                compressed = i == 1
                payload = gzip.compress(csv_data, mtime=0) if compressed else csv_data
                response = client.post(
                    "/api/v1/biodiversity/uploads",
                    data=payload,
                    headers={
                        "X-API-Key": "test-key",
                        "X-Filename": (
                            f"biodiv_part{i:03d}.csv.gz"
                            if compressed else f"biodiv_part{i:03d}.csv"
                        ),
                        "Content-Type": (
                            "application/gzip" if compressed else "text/csv"
                        ),
                    },
                )
                self.assertEqual(response.status_code, 202, response.data)
                jobs.append(response.get_json()["job_id"])

            # Process only one per invocation, using the same queue code as
            # the deployed worker, to exercise restart/re-claim logic.
            for _ in jobs:
                run(staging_dir=Path(tmp), poll_seconds=1, once=True)

            self.assertEqual(list(Path(tmp).iterdir()), [])

            with self.conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT sample_id, taxon, read_count,
                           ROUND(relative_abundance_pct::numeric, 4)
                    FROM sample_taxon_abundance
                    WHERE sample_id IN ('ABCD-1234','EFGH-5678')
                    ORDER BY sample_id, taxon
                    """
                )
                rows = cur.fetchall()
                self.assertEqual(
                    [(r[0], r[1], int(r[2])) for r in rows],
                    [
                        ("ABCD-1234", "Acidobacteriota", 11),
                        ("ABCD-1234", "Bacillota", 5),
                        ("EFGH-5678", "Acidobacteriota", 12),
                        ("EFGH-5678", "Bacillota", 6),
                    ],
                )
                cur.execute(
                    """
                    SELECT COUNT(*), COUNT(DISTINCT source_upload_id)
                    FROM sample_taxon_abundance
                    WHERE sample_id IN ('ABCD-1234','EFGH-5678')
                    """
                )
                self.assertEqual(cur.fetchone(), (4, 2))
                cur.execute(
                    """
                    SELECT DISTINCT archive_object_name
                    FROM biodiversity_uploads
                    """
                )
                self.assertEqual(cur.fetchall(), [("",)])
                cur.execute(
                    """
                    SELECT status, inserted_rows
                    FROM biodiversity_import_jobs
                    WHERE job_id IN (%s::uuid, %s::uuid)
                    ORDER BY queued_at, job_id
                    """,
                    jobs,
                )
                self.assertEqual(
                    sorted(cur.fetchall()),
                    [("succeeded", 2), ("succeeded", 2)],
                )

            # Validate real PostgreSQL FAPROTAX preparation after uploading
            # disjoint sample-column parts. No huge pandas pivot is allowed.
            from tools.generate_biodiversity_piecharts import (
                build_clean_otu_and_taxonomy_files,
            )

            otu_path, tax_path = build_clean_otu_and_taxonomy_files(
                marker="16S",
                out_dir=Path(tmp) / "faprotax",
                min_prev=1,
                min_total=1,
            )
            import csv

            with otu_path.open(newline="", encoding="utf-8") as stream:
                otu_rows = list(csv.reader(stream))
            self.assertEqual(
                otu_rows,
                [
                    ["otu_id", "ABCD-1234", "EFGH-5678"],
                    ["otu1", "11.0", "12.0"],
                    ["otu2", "5.0", "6.0"],
                ],
            )
            with tax_path.open(newline="", encoding="utf-8") as stream:
                taxonomy_rows = list(csv.reader(stream, delimiter=";"))
            self.assertEqual(
                [row[0] for row in taxonomy_rows],
                ["OTU_ID", "otu1", "otu2"],
            )

            for job in jobs:
                result = client.get(
                    f"/api/v1/biodiversity/uploads/{job}",
                    headers={"X-API-Key": "test-key"},
                )
                self.assertEqual(result.status_code, 200)
                self.assertEqual(result.get_json()["status"], "succeeded")


if __name__ == "__main__":
    unittest.main()
