"""Offline synthetic RO4-1 inventory tests."""
import json

from tools.ro41_inventory import group_records, record_row, review_rows, search


def sample(ident, concept=None, doi=None, kind="dataset", grant=True):
    data = {"id": ident, "doi": f"10.5281/zenodo.{ident}",
            "metadata": {"title": "Synthetic data", "resource_type": {"type": kind},
                         "publication_date": "2026-01-01", "access_right": "open",
                         "license": {"id": "cc-by-4.0"},
                         "funding": [{"award": {"number": "101112869"}}] if grant else []},
            "files": [{"key": "file.json", "size": 10, "checksum": "md5:fixture"}]}
    if concept:
        data["conceptrecid"] = concept
    if doi:
        data["conceptdoi"] = doi
    return data


def test_version_grouping_and_metadata():
    a = record_row(sample(11, "10", "10.5281/zenodo.10"))
    b = record_row(sample(12, "10"))
    c = record_row(sample(13))
    assert a["echo_grant_confirmed"] and a["csvw_filename_present"]
    groups = group_records([a, b, c])
    assert len(groups) == 2
    known = next(g for g in groups if g["concept_resolved"])
    assert known["version_count_observed"] == 2
    assert known["representative_record_id"] == "12"
    assert all(r["eligibility"] == "REVIEW" for r in review_rows(groups))


def test_non_dataset_not_accepted():
    g = group_records([record_row(sample(11, "10", kind="publication"))])
    assert not list(review_rows(g))


def test_search_partial_on_inconsistent_total():
    class Reader:
        def get(self, url, cap):
            return json.dumps({"hits": {"total": 4, "hits": [sample(11)]}}).encode(), url
    rows, outcome = search(Reader(), "fixture", {})
    assert len(rows) == 1 and outcome["status"] == "PARTIAL"


def test_legacy_grant():
    doc = sample(11, grant=False)
    doc["metadata"]["grants"] = [{"id": "10.13039/501100000780::101112869"}]
    assert record_row(doc)["echo_grant_confirmed"]
