import importlib.util
import unittest
from pathlib import Path

MODULE = Path(__file__).resolve().parents[1] / "tools/enrich_loess_photo_urls.py"
spec = importlib.util.spec_from_file_location("enrich_loess_photo_urls", MODULE)
enrich = importlib.util.module_from_spec(spec)
spec.loader.exec_module(enrich)


class LoessPhotoTests(unittest.TestCase):
    def setUp(self):
        self.raw = {"data": [
            {"id": 4, "status": "ACCEPTED", "filePath": "d4089cca-def5-4bdc-9f74-7f83e58b6038-survey", "user": {"email": "private@example.com"}},
            {"id": 5, "status": "ACCEPTED", "filePath": None},
            {"id": 6, "status": "REJECTED", "filePath": "rejected-survey"},
        ]}
        self.features = {"type": "FeatureCollection", "features": [
            {"type": "Feature", "id": "4", "geometry": {"type": "Point", "coordinates": [17.2, 52.2]},
             "properties": {"source_id": "4", "status": "ACCEPTED"}},
            {"type": "Feature", "id": "5", "geometry": {"type": "Point", "coordinates": [16.9, 52.4]},
             "properties": {"source_id": "5", "status": "ACCEPTED"}},
            {"type": "Feature", "id": "6", "geometry": {"type": "Point", "coordinates": [19.9, 50.0]},
             "properties": {"source_id": "6", "status": "REJECTED"}},
        ]}

    def test_adds_photo_to_accepted_with_file_path_only(self):
        stats = enrich.enrich(self.raw, self.features, "https://photos.example.org/v1/{filePath}?observation={id}")
        props = [f["properties"] for f in self.features["features"]]
        self.assertEqual(stats["with_photo_url"], 1)
        self.assertEqual(stats["without_photo"], 1)
        self.assertEqual(stats["not_accepted"], 1)
        self.assertEqual(props[0]["image_url"], "https://photos.example.org/v1/d4089cca-def5-4bdc-9f74-7f83e58b6038-survey?observation=4")
        self.assertNotIn("image_url", props[1])
        self.assertNotIn("image_url", props[2])
        self.assertNotIn("filePath", props[0])
        self.assertNotIn("user", props[0])

    def test_replaces_stale_url(self):
        self.features["features"][1]["properties"]["image_url"] = "https://old.example.com/old.png"
        enrich.enrich(self.raw, self.features, "https://photos.example.org/image/{filePath}")
        self.assertNotIn("image_url", self.features["features"][1]["properties"])

    def test_unsafe_template_rejected(self):
        for template in ["http://example.com/{filePath}",
                         "https://user:pass@example.org/{filePath}",
                         "https://example.org/no-placeholder",
                         "https://example.org/img/{filePath}?access_token=abc"]:
            with self.subTest(template=template), self.assertRaises(ValueError):
                enrich.validate_template(template)

    def test_id_only_template_still_requires_file_path(self):
        stats = enrich.enrich(self.raw, self.features, "https://photos.example.org/surveys/{id}/photo")
        self.assertEqual(stats["with_photo_url"], 1)


if __name__ == "__main__":
    unittest.main()
