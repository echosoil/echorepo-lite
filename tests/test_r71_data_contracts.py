"""R7-1 data, geospatial, metadata and quality acceptance checks.

Synthetic fixtures are deliberately small and never access live databases or Zenodo.
"""
from __future__ import annotations

import csv
import io
import math
import unittest
import zipfile
from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd

from echorepo.services import canonical_exports as exports
from echorepo.services import soil_categories, validation
from echorepo.utils import geo
from tools import publish_api_file_to_zenodo as publisher
from tools import validate_echorepo_bundle as validator


class R71CanonicalContractTests(unittest.TestCase):
    def test_r71_export_01_contracts_match_publisher_and_validator(self):
        expected = publisher.CANONICAL_RESOURCE_SCHEMAS
        self.assertEqual(set(exports.EXPORT_SPECS), set(expected))
        self.assertEqual(expected, validator.EXPECTED_SCHEMAS)
        self.assertEqual(
            [exports.SAMPLE_PUBLIC_COLUMN_MAP.get(c, c) for c in exports.SAMPLE_COLUMNS],
            expected["samples.csv"],
        )
        self.assertEqual(
            [exports.IMAGE_PUBLIC_COLUMN_MAP.get(c, c) for c in exports.IMAGE_COLUMNS],
            expected["sample_images.csv"],
        )
        self.assertEqual(exports.PARAMETER_PUBLIC_COLUMNS, expected["sample_parameters.csv"])
        self.assertEqual(
            [exports.BIODIVERSITY_PUBLIC_COLUMN_MAP.get(c, c) for c in exports.BIODIVERSITY_COLUMNS],
            expected["sample_biodiversity.csv"],
        )
        self.assertEqual(exports.PARAMETER_DEFINITION_COLUMNS, expected["parameter_definitions.csv"])
        self.assertEqual(exports.ANALYSIS_METHOD_COLUMNS, expected["analysis_methods.csv"])

    def test_r71_export_02_machine_zip_has_six_parseable_resources(self):
        schemas = publisher.CANONICAL_RESOURCE_SCHEMAS

        def fake_export(filename, *, sample_ids=None):
            self.assertEqual(sample_ids, ["AAAA-1234"])
            return pd.DataFrame(columns=schemas[filename])

        with patch.object(exports, "get_export_df", side_effect=fake_export):
            bundle = exports.build_machine_bundle(sample_ids=["AAAA-1234"])
        with zipfile.ZipFile(io.BytesIO(bundle.zip_bytes)) as zf:
            self.assertEqual(set(zf.namelist()), set(schemas))
            for filename, columns in schemas.items():
                raw = zf.read(filename).decode("utf-8")
                self.assertFalse(raw.startswith("#"), filename)
                self.assertEqual(next(csv.reader(io.StringIO(raw))), columns, filename)

    def test_r71_export_03_sample_id_normalization_is_unique_and_stable(self):
        self.assertEqual(
            exports._normalise_sample_ids([" abcd-1234 ", "ABCD-1234", "", None, "bcde-2345"]),
            ["ABCD-1234", "BCDE-2345"],
        )

    def test_r71_export_04_parameter_threshold_preserves_boundary(self):
        frame = pd.DataFrame({"value": ["0.009", "0.010", "0.1", "unknown"]})
        kept = exports.drop_parameter_values_below(frame)
        self.assertEqual(kept["value"].tolist(), ["0.010", "0.1", "unknown"])


class R71HarmonisationTests(unittest.TestCase):
    def test_r71_harmon_01_multilingual_soil_texture_standardized(self):
        frame = pd.DataFrame({"soil_texture_orig": ["Arenoso", "Sandig", "not-mapped", None]})
        output = soil_categories.standardize_soil_columns(frame)
        self.assertEqual(output["soil_texture_en"].iloc[0], "sandy")
        self.assertEqual(output["soil_texture_en"].iloc[1], "sandy")
        self.assertIsNone(output["soil_texture_en"].iloc[2])
        self.assertEqual(output["soil_texture_orig"].iloc[0], "Arenoso")

    def test_r71_harmon_02_invalid_category_rejected(self):
        with self.assertRaises(ValueError):
            soil_categories.canonical_soil_value("Sandy", "unsupported-category")


class R71QualityAssuranceTests(unittest.TestCase):
    def test_r71_qa_01_sentinel_and_out_of_bounds_coordinates_excluded(self):
        frame = pd.DataFrame({
            "lat": ["41,4", "46.5", "91", "not-a-number"],
            "lon": ["2,1", "11.35", "3", "4"],
        })
        _lat, _lon, valid = validation._clean_coords(frame, "lat", "lon")
        self.assertEqual(valid.tolist(), [True, False, False, False])
        sentinels = validation.find_default_coord_rows(frame, lat_col="lat", lon_col="lon")
        self.assertEqual(len(sentinels), 1)

    def test_r71_qa_02_duplicate_samples_and_bad_categories_reported(self):
        rows = [
            {"sample_id": "ABCD-1234", "lat": "41.2", "lon": "2.2", "soil_texture_en": "sandy"},
            {"sample_id": "ABCD-1234", "lat": "100", "lon": "2.2", "soil_texture_en": "invalid"},
        ]
        report = validator.Validation()
        validator.validate_samples(rows, report)
        self.assertTrue(any("duplicate sample_id" in msg for msg in report.errors))
        self.assertTrue(any("latitude outside" in msg for msg in report.errors))
        self.assertTrue(any("unexpected soil_texture" in msg for msg in report.errors))

    def test_r71_qa_03_numeric_and_timestamp_validation(self):
        self.assertEqual(validator.parse_integer("12.0"), 12)
        with self.assertRaises(ValueError):
            validator.parse_integer("12.5")
        self.assertEqual(validator.parse_utc_timestamp("2026-09-30T12:00:00Z").year, 2026)
        with self.assertRaises(ValueError):
            validator.parse_utc_timestamp("2026-09-30T12:00:00+02:00")
        report = validator.Validation()
        validator.check_header("samples.csv", ["not_a_column"], report)
        self.assertTrue(any("header mismatch" in msg for msg in report.errors))


class R71GeospatialPrivacyTests(unittest.TestCase):
    def test_r71_geo_01_geojson_excludes_invalid_coordinates(self):
        frame = pd.DataFrame({
            "lat": ["41.4", "NaN", "200"],
            "lon": ["2.1", "2.2", "2.3"],
            "QR_qrCode": ["ABCD-1234", "BCDE-2345", "CDEF-3456"],
        })
        output = geo.df_to_geojson(frame)
        self.assertEqual(output["type"], "FeatureCollection")
        self.assertEqual(len(output["features"]), 1)
        self.assertEqual(output["features"][0]["geometry"]["coordinates"], [2.1, 41.4])

    def test_r71_privacy_01_jitter_is_stable_and_within_radius(self):
        lat, lon, limit = 41.4, 2.1, 500
        first = geo.deterministic_jitter(lat, lon, "ABCD-1234", max_dist_m=limit)
        self.assertEqual(first, geo.deterministic_jitter(lat, lon, "ABCD-1234", max_dist_m=limit))
        self.assertNotEqual(first, geo.deterministic_jitter(lat, lon, "EFGH-5678", max_dist_m=limit))
        displacement_m = math.hypot(
            (first[0] - lat) * 111_000,
            (first[1] - lon) * 111_000 * math.cos(math.radians(lat)),
        )
        self.assertLessEqual(displacement_m, limit + 1e-6)


class R71MetadataTests(unittest.TestCase):
    @staticmethod
    def analyses():
        return [{
            "filename": filename,
            "headers": headers,
            "inferred_datatypes": dict.fromkeys(headers, "string"),
            "row_count": 0,
            "size_bytes": len(",".join(headers)),
            "sha256": "0" * 64,
            "encoding": "utf-8",
            "delimiter": ",",
        } for filename, headers in publisher.CANONICAL_RESOURCE_SCHEMAS.items()]

    def test_r71_metadata_01_csvw_describes_every_public_resource(self):
        analyses = self.analyses()
        args = SimpleNamespace(
            title="ECHOREPO R7-1 synthetic fixture",
            description="Validation only",
            creator=["ECHO Consortium"],
            license="cc-by-4.0",
            version="test",
            copyright=None,
        )
        document, _warnings = publisher.build_csvw_document(
            analyses, {}, args, "https://example.org/dataset", None
        )
        publisher.validate_csvw_document(document, analyses)
        self.assertEqual(len(document["tables"]), 6)
        self.assertEqual(
            {t["url"] for t in document["tables"]},
            set(publisher.CANONICAL_RESOURCE_SCHEMAS),
        )
        self.assertEqual(document["dc:creator"], ["ECHO Consortium"])

    def test_r71_metadata_02_csvw_header_mismatch_rejected(self):
        analyses = self.analyses()
        first = analyses[0]
        document = {"tables": [{
            "url": first["filename"],
            "tableSchema": {"columns": [{"name": "wrong_column"}]},
        }]}
        with self.assertRaises(ValueError):
            publisher.validate_csvw_document(document, [first])


if __name__ == "__main__":
    unittest.main()
