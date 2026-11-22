"""R7-1 acceptance checks for MinIO adapter, lab ingestion and operations.

All network and database interactions are mocked or rejected before use.
"""
from __future__ import annotations

import unittest
from unittest.mock import MagicMock, patch

from flask import Flask

from echorepo import analytics
from echorepo.routes import data_api as api
from echorepo.services.storage import minio as storage
from scripts import run_pg_migrations as migrations


class R71StorageTests(unittest.TestCase):
    def test_r71_storage_01_rejects_path_traversal(self):
        for name in ("", "../secret.csv", "safe/../../secret.csv"):
            with self.subTest(name=name), self.assertRaises(storage.StorageError):
                storage._validate_object_name(name)
        self.assertEqual(storage._validate_object_name("2026-09-30/all.zip"), "2026-09-30/all.zip")

    def test_r71_storage_02_archive_published_at_versioned_and_latest_paths(self):
        client = MagicMock()
        with patch.object(storage, "get_client", return_value=client):
            with patch.object(storage, "_ensure_bucket", return_value="echorepo-uploads"):
                storage.upload_canonical_zip(b"synthetic zip bytes", "2026-09-30")
        paths = [call.args[1] for call in client.put_object.call_args_list]
        self.assertEqual(paths, [
            "canonical/2026-09-30/all.zip",
            "canonical/latest/all.zip",
        ])
        for call in client.put_object.call_args_list:
            self.assertEqual(call.kwargs["content_type"], "application/zip")

    def test_r71_storage_03_read_object_closes_minio_response(self):
        client = MagicMock()
        response = client.get_object.return_value
        response.read.return_value = b"csv-bytes"
        with patch.object(storage, "get_client", return_value=client):
            resource = storage.get_canonical_object("latest/samples.csv")
        self.assertEqual(resource.data, b"csv-bytes")
        self.assertEqual(resource.content_type, "text/csv")
        self.assertEqual(resource.download_name, "samples.csv")
        response.close.assert_called_once()
        response.release_conn.assert_called_once()


class R71LabIngestionTests(unittest.TestCase):
    def setUp(self):
        self.app = Flask(__name__)
        self.app.config.update(
            TESTING=True,
            SECRET_KEY="local-lab-test",
            API_KEY="private-test-key",
            LAB_UPLOAD_MAX_BYTES=8,
        )
        self.app.register_blueprint(api.data_api, url_prefix="/api/v1")
        self.client = self.app.test_client()

    def test_r71_lab_01_qr_identifier_normalization(self):
        self.assertEqual(api._normalize_qr(" ECHO-abcd1234 "), "ABCD-1234")
        self.assertEqual(api._normalize_qr("abCd-1234"), "ABCD-1234")
        self.assertEqual(api._normalize_qr(None), "")

    def test_r71_lab_02_oversized_upload_rejected_before_db_access(self):
        with patch.object(api, "get_conn", side_effect=AssertionError("unexpected DB access")):
            response = self.client.post(
                "/api/v1/lab-enrichment",
                data=b"qr_code,ph\nABCD-1234,7\n",
                content_type="text/csv",
                headers={"X-API-Key": "private-test-key"},
            )
        self.assertEqual(response.status_code, 413)


class R71OperationsTests(unittest.TestCase):
    def test_r71_db_01_migration_ledger_is_parameterized(self):
        cursor = MagicMock()
        cursor.fetchone.return_value = (1,)
        self.assertTrue(migrations.migration_already_applied(cursor, "2026_initial.sql"))
        self.assertEqual(cursor.execute.call_args.args[1], ("2026_initial.sql",))
        migrations.mark_migration_applied(cursor, "2026_initial.sql")
        self.assertEqual(cursor.execute.call_args.args[1], ("2026_initial.sql",))

    def test_r71_analytics_01_ip_identifier_is_pseudonymized(self):
        digest = analytics.hash_ip("192.0.2.1")
        self.assertEqual(len(digest), 64)
        self.assertEqual(digest, analytics.hash_ip("192.0.2.1"))
        self.assertNotEqual(digest, analytics.hash_ip("192.0.2.2"))
        self.assertNotIn("192.0.2.1", digest)


if __name__ == "__main__":
    unittest.main()
