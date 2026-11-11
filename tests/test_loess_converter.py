import importlib.util
import json
import tempfile
import unittest
from pathlib import Path

MOD = Path(__file__).resolve().parents[1] / "tools/build_loess_geojson.py"
spec = importlib.util.spec_from_file_location("loess_builder_v3", MOD)
builder = importlib.util.module_from_spec(spec)
spec.loader.exec_module(builder)


class ConverterTests(unittest.TestCase):
    def sample(self, id=4, status="ACCEPTED", category="SEALED_SOILS"):
        return {
            "id": id,
            "category": category,
            "status": status,
            "location": {
                "x": 52.22503986056946,
                "y": 17.273806276308914,
                "countryCode": "PL",
                "name": "Sportowa 3A",
            },
            "affectedArea": 7,
            "createdAt": "2025-01-17T14:26:51.555768Z",
            "description": "Sealed soil",
            "solution": "Urbanization",
            "user": {"email": "private@example.org", "permissions": ["ES"], "id": 77},
            "filePath": "private-filepath",
        }

    def test_verified_geojson_coordinate_convention(self):
        x = self.sample()
        f = builder.make_feature(x)
        self.assertEqual(f["geometry"]["coordinates"], [17.273806276308914, 52.22503986056946])
        self.assertEqual(f["properties"]["country_code"], "PL")
        self.assertEqual(f["properties"]["radius_m"], 7.0)
        self.assertEqual(f["properties"]["category"], "sealed")
        rendered = json.dumps(f)
        self.assertNotIn("private@example.org", rendered)
        self.assertNotIn("private-filepath", rendered)
        self.assertNotIn("permissions", rendered)

    def test_all_eight_categories(self):
        self.assertEqual(
            set(builder.CATEGORY_MAP.keys()),
            {
                "DRY_SOILS",
                "WET_SOILS",
                "EROSION",
                "SEALED_SOILS",
                "DEGRADATION",
                "LOSS_OF_ORGANIC_MATTER",
                "PH",
                "BIODIVERSITY",
            },
        )
        samples = [self.sample(id=i, category=c) for i, c in enumerate(builder.CATEGORY_MAP)]
        features, report = builder.build(samples)
        self.assertEqual(len(features), 8)
        self.assertEqual(report["unknown_categories"], [])

    def test_status_filter_default_and_optional(self):
        samples = [
            self.sample(id=4),
            self.sample(id=5, status="REJECTED"),
            self.sample(id=6, status="PENDING"),
        ]
        feats, report = builder.build(samples)
        self.assertEqual(len(feats), 1)
        self.assertEqual(report["statuses_in_source"], {"ACCEPTED": 1, "REJECTED": 1, "PENDING": 1})
        self.assertEqual(report["excluded_by_status"], {"REJECTED": 1, "PENDING": 1})
        feats, report = builder.build(samples, all_statuses=True)
        self.assertEqual(len(feats), 3)

    def test_existing_reversed_file_detection(self):
        record = self.sample()
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "loess.geojson"
            bad = builder.make_feature(record)
            bad["geometry"]["coordinates"] = [52.22503986056946, 17.273806276308914]
            path.write_text(json.dumps({"features": [bad]}), encoding="utf-8")
            comp = builder.compare_existing(path, [record])
            self.assertEqual(comp["matching_reversed"], 1)
            source = Path(tmp) / "raw.json"
            source.write_text(json.dumps({"data": [record]}), encoding="utf-8")
            report = Path(tmp) / "report.json"
            self.assertEqual(
                builder.main([str(source), "--out", str(path), "--report", str(report)]), 0
            )
            self.assertEqual(
                json.loads(path.read_text())["features"][0]["geometry"]["coordinates"],
                [17.273806276308914, 52.22503986056946],
            )
            self.assertEqual(
                json.loads(report.read_text())["previous_geojson"]["matching_reversed"], 1
            )

    def test_invalid_and_unknown_not_silently_dropped(self):
        invalid = self.sample(id=10, category="UNEXPECTED_NEW_CATEGORY")
        invalid["location"] = {"x": None, "y": 2}
        features, report = builder.build([invalid])
        self.assertEqual(len(features), 0)
        self.assertEqual(len(report["invalid_coordinates"]), 1)
        self.assertEqual(len(report["unknown_categories"]), 1)


if __name__ == "__main__":
    unittest.main()
