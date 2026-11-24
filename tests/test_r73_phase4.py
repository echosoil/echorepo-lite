"""Synthetic fixtures only; no external network."""
import hashlib
import io
import json
import zipfile

import pytest

from tools.r73_interoperability import (
    BIO,
    bio_members,
    file_integrity,
    file_inventory,
    loess_evidence,
    table_evidence,
)


def test_record_and_checksum():
    raw = b"x\n1\n"
    file = {"key": "samples.csv", "size": len(raw),
            "checksum": "md5:" + hashlib.md5(raw, usedforsecurity=False).hexdigest()}
    doc = {"id": 23784504, "doi": "10.5281/zenodo.23784504",
           "metadata": {"access_right": "open", "license": {"id": "cc-by-4.0"}},
           "files": [file]}
    entries, info = file_inventory(doc, "23784504", ("samples.csv",))
    assert info["identity_matches"] and info["public_files"] and info["expected_inventory_matches"]
    assert file_integrity(raw, entries["samples.csv"])["published_checksum_matches"]
    assert not file_integrity(b"y\n1\n", file)["published_checksum_matches"]


def test_csvw_exact_bytes_and_columns():
    data = b"id,value\n1,2\n"
    table = {"url": "samples.csv", "schema:sha256": hashlib.sha256(data).hexdigest(),
             "schema:numberOfItems": 1, "schema:contentSize": len(data),
             "tableSchema": {"columns": [{"name": "id"}, {"name": "value", "propertyUrl": "https://example.org/value"}]}}
    meta = {"@context": ["http://www.w3.org/ns/csvw"], "tables": [table]}
    assert table_evidence(meta, {"samples.csv": data})[0]
    assert not table_evidence(meta, {"samples.csv": b"id,value\n1,3\n"})[0]
    table["tableSchema"]["columns"][1]["propertyUrl"] = ["https://example.org/value"]
    assert not table_evidence(meta, {"samples.csv": data})[0]


def test_bio_archive_no_extraction():
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as z:
        for name in BIO:
            z.writestr(name, "id,value\n1,2\n")
        z.writestr("biodiversity_metadata.json", "{}")
    assert set(bio_members(buffer.getvalue())[0]) == set(BIO)
    with zipfile.ZipFile(buffer, "a") as z:
        z.writestr("../unexpected", "bad")
    with pytest.raises(ValueError):
        bio_members(buffer.getvalue())


def test_loess_checks_without_row_values():
    feature = {"type": "Feature", "id": "fixture", "geometry": {"type": "Point", "coordinates": [2, 41]},
               "properties": {"image_url": "https://example.org/private-example"}}
    doc = {"type": "FeatureCollection", "features": [feature],
           "metadata": {"source": "https://www.loess-soil-map.eu/map", "generated_at": "2026-11-24T12:00:00Z"}}
    ok, evidence = loess_evidence(json.dumps(doc).encode())
    assert ok and evidence["features"] == 1
    assert "private-example" not in json.dumps(evidence)
    doc["features"].append(feature)
    assert not loess_evidence(json.dumps(doc).encode())[0]
