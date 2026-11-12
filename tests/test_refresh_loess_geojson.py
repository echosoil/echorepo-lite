"""Offline tests for the ECHOREPO public LOESS refresh job."""

import json
import tempfile
import unittest
from pathlib import Path

from tools import refresh_loess_geojson as job


def survey(identifier, status="ACCEPTED", image=True, lat=52.225, lon=17.273):
    return {
        "id": identifier,
        "category": "SEALED_SOILS",
        "status": status,
        "description": "A path",
        "solution": "Urbanization",
        "location": {"x": lat, "y": lon, "name": "Poznan", "countryCode": "PL"},
        "affectedArea": 7,
        "createdAt": "2025-01-17T00:00:00Z",
        "filePath": "test-image-survey" if image else None,
        "user": {"id": 4, "email": "must-not-be-exported@example.org"},
    }


class LoessRefreshTests(unittest.TestCase):
    def test_converts_coords_and_photos_without_private_fields(self):
        data = {"data": [survey(4), survey(5, status="REJECTED"), survey(6, image=False)]}
        output, report = job.make_geojson(data, min_features=1)
        self.assertEqual(len(output["features"]), 2)
        feature = output["features"][0]
        self.assertEqual(feature["geometry"]["coordinates"], [17.273, 52.225])
        props = feature["properties"]
        self.assertEqual(props["source_id"], "4")
        self.assertIn("surveyimages%2Ftest-image-survey", props["image_url"])
        self.assertIn("w=256&q=75", props["image_url"])
        self.assertNotIn("user", props)
        self.assertNotIn("filePath", props)
        self.assertEqual(report["excluded_by_status"], {"REJECTED": 1})
        self.assertNotIn("image_url", output["features"][1]["properties"])

    def test_atomic_publish_preserves_old_file_on_api_error(self):
        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / "geo.json"
            report_path = Path(td) / "report.json"
            old_bytes = b'{"type":"FeatureCollection","features":[]}\n'
            out.write_bytes(old_bytes)

            def broken_fetcher(timeout):
                raise OSError("upstream unavailable")

            with self.assertRaises(OSError):
                job.refresh(out=out, report_path=report_path, fetcher=broken_fetcher,
                            min_features=1)
            self.assertEqual(out.read_bytes(), old_bytes)
            self.assertFalse(report_path.exists())

    def test_suspicious_shrinkage_preserves_file(self):
        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / "geo.json"
            report_path = Path(td) / "report.json"
            current = {"data": [survey(i) for i in range(1, 11)]}
            job.refresh(out=out, report_path=report_path, fetcher=lambda timeout: current,
                        min_features=1)
            original = out.read_bytes()
            new = {"data": [survey(i) for i in range(1, 5)]}
            with self.assertRaisesRegex(ValueError, "shrinkage"):
                job.refresh(out=out, report_path=report_path, fetcher=lambda timeout: new,
                            min_features=1)
            self.assertEqual(out.read_bytes(), original)

    def test_unchanged_refresh_does_not_rewrite(self):
        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / "geo.json"
            report_path = Path(td) / "report.json"
            data = {"data": [survey(4)]}
            first = job.refresh(out=out, report_path=report_path, fetcher=lambda timeout: data,
                                min_features=1)
            original = out.read_bytes()
            second = job.refresh(out=out, report_path=report_path, fetcher=lambda timeout: data,
                                 min_features=1)
            self.assertTrue(first["changed"])
            self.assertFalse(second["changed"])
            self.assertEqual(out.read_bytes(), original)
            self.assertTrue(report_path.exists())
            parsed = json.loads(out.read_text(encoding="utf-8"))
            self.assertIn("image_url", parsed["features"][0]["properties"])

    def test_photo_loss_protection_and_dry_run(self):
        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / "geo.json"
            report_path = Path(td) / "report.json"
            original_data = {"data": [survey(i) for i in range(1, 6)]}
            job.refresh(out=out, report_path=report_path,
                        fetcher=lambda timeout: original_data, min_features=1)
            original = out.read_bytes()
            updated = {"data": [survey(i, image=False) for i in range(1, 6)]}
            with self.assertRaisesRegex(ValueError, "photo loss"):
                job.refresh(out=out, report_path=report_path,
                            fetcher=lambda timeout: updated, min_features=1)
            self.assertEqual(out.read_bytes(), original)
            changes = {"data": [survey(i) for i in range(1, 7)]}
            report = job.refresh(out=out, report_path=report_path,
                                 fetcher=lambda timeout: changes, min_features=1,
                                 dry_run=True)
            self.assertTrue(report["changed"])
            self.assertEqual(out.read_bytes(), original)


if __name__ == "__main__":
    unittest.main()
