"""Offline tests of the R7-3 Phase 2 evidence tools."""
import csv
import io
import json
import zipfile

import pytest

from tools import r73_data_quality as dq
from tools import r73_performance as perf


def bundle(tmp_path, *, orphan=False, nonfinite=False, missing=False, duplicate=False):
    path = tmp_path / "bundle.zip"
    rows = {
        "samples.csv": [{"sample_id": "PRIVATE_SAMPLE", "lat": "NaN" if nonfinite else "41",
                         "lon": "2", "country_code": "ES", "licence": "CC-BY-4.0"}],
        "sample_images.csv": [{"sample_id": "PRIVATE_ORPHAN" if orphan else "PRIVATE_SAMPLE",
                               "image_id": "PRIVATE_IMAGE", "country_code": "ES", "licence": "CC-BY-4.0"}],
        "sample_parameters.csv": [{"sample_id": "PRIVATE_SAMPLE", "parameter_code": "p",
                                    "method_code": "m", "result_value": "2", "licence": "CC-BY-4.0"}],
        "sample_biodiversity.csv": [{"sample_id": "PRIVATE_SAMPLE", "read_count": "12",
                                      "relative_abundance_pct": "50", "licence": "CC-BY-4.0"}],
        "parameter_definitions.csv": [{"parameter_code": "p", "method_code": "m"}],
        "analysis_methods.csv": [{"method_code": "m"}],
    }
    if duplicate:
        rows["samples.csv"] *= 2
    with zipfile.ZipFile(path, "w") as archive:
        for name, schema in dq.EXPECTED_SCHEMAS.items():
            if missing and name == "analysis_methods.csv":
                continue
            output = io.StringIO()
            writer = csv.DictWriter(output, fieldnames=schema)
            writer.writeheader()
            writer.writerows(rows[name])
            archive.writestr(name, output.getvalue())
    return path


def test_aggregate_quality(tmp_path):
    r = dq.assess(bundle(tmp_path))
    assert r["status"] == "PASS_STRUCTURAL_CHECKS"
    assert r["distinct_samples"] == 1
    assert r["samples_with_linked_resources"]["sample_images.csv"] == 1
    assert r["csvw"]["status"] == "NOT_ASSESSED"
    assert "PRIVATE_" not in json.dumps(r)
    assert "PRIVATE_" not in dq.markdown(r)


@pytest.mark.parametrize("kwargs", [{"orphan": True}, {"nonfinite": True}, {"missing": True}, {"duplicate": True}])
def test_bad_bundle(tmp_path, kwargs):
    r = dq.assess(bundle(tmp_path, **kwargs))
    assert r["errors"] > 0
    assert "PRIVATE_" not in json.dumps(r)


def test_csvw_exact_snapshot(tmp_path):
    path = bundle(tmp_path)
    tables = dq.assess(path)["tables"]
    meta = {"tables": [
        {"url": name, "tableSchema": {"columns": [{"name": c} for c in dq.EXPECTED_SCHEMAS[name]]},
         "schema:sha256": t["sha256"], "schema:numberOfItems": t["rows"], "schema:contentSize": t["bytes"]}
        for name, t in tables.items()
    ]}
    m = tmp_path / "file.json"
    m.write_text(json.dumps(meta))
    assert dq.assess(path, m)["csvw"]["status"] == "MATCHED_SELECTED_CHECKS"
    meta["tables"][0]["schema:sha256"] = "incorrect"
    m.write_text(json.dumps(meta))
    assert dq.assess(path, m)["csvw"]["status"] == "MISMATCH_OR_MISSING_FIELDS"


def test_origin_guard():
    assert perf.approve_origin("https://echorepo.quanta-labs.com", "production-readonly", "yes-production-readonly") == "https://echorepo.quanta-labs.com"
    for url in ("https://evil.example", "http://echorepo.quanta-labs.com",
                "https://user:secret@echorepo.quanta-labs.com", "https://echorepo.quanta-labs.com/path"):
        with pytest.raises(ValueError):
            perf.approve_origin(url, "production-readonly", "yes-production-readonly")
    with pytest.raises(ValueError):
        perf.approve_origin("http://127.0.0.1:8001", "staging", "yes-staging-readonly")


def test_payload_and_summary():
    ops = {o["id"]: o for o in perf.operations("ES", False, False)}
    assert perf.valid_payload(ops["ping"], "application/json", b'{"ok":true}')
    assert not perf.valid_payload(ops["ping"], "text/html", b"login")
    assert perf.valid_payload(ops["sample_count"], "application/json", b'{"count":3}')
    assert not perf.valid_payload(ops["sample_count"], "application/json", b'{"count":true}')
    attempts = [
        {"result": "OK", "elapsed_ms": 100, "decoded_bytes_read": 50},
        {"result": "OK", "elapsed_ms": 400, "decoded_bytes_read": 50},
        {"result": "HTTP_STATUS", "elapsed_ms": 1, "decoded_bytes_read": 0},
    ]
    r = perf.summarize(ops["ping"], 4, attempts)
    assert r["median_ms"] == 250 and r["p95_ms"] == 400
    assert r["errors"] == 1 and r["not_run"] == 1
    assert r["target_result"] == "NOT_EVALUABLE"


def test_measure_does_not_follow_redirects_or_log_body():
    class Reply:
        status_code = 302
        headers = {"Location": "https://example.org/?token=SECRET"}
        def __enter__(self):
            return self
        def __exit__(self, *args):
            pass

    class Cookies:
        def clear(self):
            pass

    class Session:
        cookies = Cookies()
        def get(self, url, **kwargs):
            assert not kwargs["allow_redirects"]
            assert kwargs["headers"] == {}
            return Reply()

    op = perf.operations("ES", True, False)[0]
    result = perf.measure(Session(), "https://example.org", op, {"X-API-Key": "SECRET"})
    assert result["result"] == "HTTP_STATUS"
    assert "SECRET" not in json.dumps(result)


def test_stop_on_overload(monkeypatch):
    class Session:
        headers = {}
        def __enter__(self):
            return self
        def __exit__(self, *args):
            pass

    monkeypatch.setattr(perf.requests, "Session", Session)
    monkeypatch.setattr(perf.time, "sleep", lambda _: None)
    monkeypatch.setattr(perf, "measure", lambda *args: {
        "result": "HTTP_STATUS", "elapsed_ms": 50, "decoded_bytes_read": 0, "http_status": 429,
    })
    r = perf.run("https://example.org", "production-readonly", "ES", 3, 1, {}, True, False)
    assert r["stopped_early"]
    assert sum(x["attempted"] for x in r["operations"]) == 1
    assert not r["complete_success"]
