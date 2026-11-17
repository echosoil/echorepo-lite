"""Upload staging and API-key authorization tests (no PostgreSQL required)."""
from __future__ import annotations

import hashlib
import io
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from flask import Flask

from echorepo.services.biodiversity_api_jobs import (
    BiodiversityUploadTooLarge,
    InvalidBiodiversityUpload,
    stage_biodiversity_stream,
    staging_path,
    validate_filename,
)


class ReadSizeAssertingStream(io.BytesIO):
    def read(self, size=-1):
        assert 0 < size <= 4 * 1024 * 1024
        return super().read(size)


class BiodiversityUploadStagingTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)

    def test_staging_is_content_exact_and_bounded(self):
        source = b"OTU ID\tABCD-1234-16S\tKingdom\tPhylum\n" + (
            b"otu1\t1\tBacteria\tBacillota\n" * 10_000
        )
        receipt = stage_biodiversity_stream(
            ReadSizeAssertingStream(source),
            self.root,
            "biodiv_part001.tsv",
            max_bytes=len(source) + 1,
            content_length=len(source),
        )
        self.assertEqual(receipt.size_bytes, len(source))
        self.assertEqual(receipt.sha256, hashlib.sha256(source).hexdigest())
        self.assertEqual(receipt.path.read_bytes(), source)
        self.assertEqual(
            staging_path(self.root, receipt.job_id, receipt.filename),
            receipt.path,
        )
        self.assertEqual(list(self.root.glob("*.partial")), [])

    def test_rejects_over_limit_before_writing(self):
        with self.assertRaises(BiodiversityUploadTooLarge):
            stage_biodiversity_stream(
                io.BytesIO(b"123456"),
                self.root,
                "test.csv",
                max_bytes=5,
                content_length=6,
            )
        self.assertEqual(list(self.root.iterdir()), [])

    def test_incomplete_request_is_not_retained(self):
        with self.assertRaisesRegex(InvalidBiodiversityUpload, "incomplete"):
            stage_biodiversity_stream(
                io.BytesIO(b"small"),
                self.root,
                "test.tsv",
                max_bytes=100,
                content_length=9,
            )
        self.assertEqual(list(self.root.iterdir()), [])

    def test_rejects_paths_and_missing_length(self):
        for name in ("../test.csv", r"..\test.csv", "test.zip", ".csv"):
            with self.assertRaises(InvalidBiodiversityUpload):
                validate_filename(name)
        with self.assertRaisesRegex(InvalidBiodiversityUpload, "Content-Length"):
            stage_biodiversity_stream(
                io.BytesIO(b"x"), self.root, "ok.csv",
                max_bytes=100, content_length=None,
            )


class BiodiversityUploadAPITests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        # Import the app's existing API blueprint, not a mock auth adapter.
        from echorepo.routes.data_api import data_api

        self.app = Flask(__name__)
        self.app.secret_key = "local-test-only"
        self.app.config.update(
            TESTING=True,
            API_KEY="some-existing-read-key",
            BIODIVERSITY_UPLOAD_API_KEY="private-write-key",
            BIODIVERSITY_UPLOAD_STAGING_DIR=str(self.root),
            BIODIVERSITY_API_MAX_UPLOAD_BYTES=1_000,
        )
        self.app.register_blueprint(data_api, url_prefix="/api/v1")
        self.client = self.app.test_client()
        self.headers = {
            "X-API-Key": "private-write-key",
            "X-Filename": "biodiversity_16S_part001.csv",
            "Content-Type": "text/csv",
        }

    def test_wrong_key_cannot_upload_even_if_it_is_the_read_key(self):
        response = self.client.post(
            "/api/v1/biodiversity/uploads",
            headers={**self.headers, "X-API-Key": "some-existing-read-key"},
            data=b"some sample",
        )
        self.assertEqual(response.status_code, 401)
        self.assertFalse(list(self.root.iterdir()))

    def test_multipart_rejected_without_parsing_file(self):
        response = self.client.post(
            "/api/v1/biodiversity/uploads",
            headers={**self.headers, "Content-Type": "multipart/form-data"},
            data=b"anything",
        )
        self.assertEqual(response.status_code, 415)
        self.assertFalse(list(self.root.iterdir()))

    def test_api_upload_enqueues_file_and_exposes_status(self):
        data = (
            b"OTU ID,ABCD-1234-16S,Kingdom,Phylum\n"
            b"otu1,42,Bacteria,Bacillota\n"
        )
        with patch("echorepo.routes.data_api.get_pg_conn") as get_conn:
            conn = get_conn.return_value.__enter__.return_value
            cur = conn.cursor.return_value.__enter__.return_value
            response = self.client.post(
                "/api/v1/biodiversity/uploads",
                headers=self.headers,
                data=data,
            )
            self.assertEqual(response.status_code, 202)
            receipt = response.get_json()
            self.assertEqual(receipt["status"], "queued")
            cur.execute.assert_called_once()
            self.assertIn("INSERT INTO biodiversity_import_jobs",
                          cur.execute.call_args.args[0])
            self.assertEqual(
                staging_path(self.root, receipt["job_id"], receipt["filename"]).read_bytes(),
                data,
            )

            from datetime import datetime, timezone
            now = datetime.now(timezone.utc)
            cur.fetchone.return_value = (
                receipt["filename"], receipt["sha256"], len(data),
                "succeeded", now, now, now, 123, None,
            )
            status_response = self.client.get(
                receipt["status_url"],
                headers={"X-API-Key": "private-write-key"},
            )
            self.assertEqual(status_response.status_code, 200)
            self.assertEqual(status_response.get_json()["rows_inserted"], 123)

    def test_too_large_rejected(self):
        response = self.client.post(
            "/api/v1/biodiversity/uploads",
            headers=self.headers,
            data=b"x" * 1001,
        )
        self.assertEqual(response.status_code, 413)
        self.assertFalse(list(self.root.iterdir()))


if __name__ == "__main__":
    unittest.main()
