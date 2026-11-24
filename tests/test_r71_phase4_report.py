"""Offline R7-1 Phase 4 consolidation regression tests."""
from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from tools import r71_phase4_report as phase4


def fixtures():
    p1 = {"kpi": "R7-1", "baseline": "M40", "components": [
        {"id": "R71-C01", "name": "API", "criterion": "runs", "tests": ["test_unit"],
         "additional_evidence": "External signoff pending"},
        {"id": "R71-C18", "name": "External", "criterion": "externally deployed", "tests": [],
         "additional_evidence": "Firebase and EUSO evidence pending"},
    ]}
    p2 = {"checks": [{"id": "R71-I01", "component": "API", "criterion": "ping",
                       "name": "test_integration"}]}
    p3 = {"cases": [
        {"id": "R71-B01", "component": "Public browser", "criterion": "UI", "test": "test_browser",
         "required": True},
        {"id": "R71-B15", "component": "Private browser", "criterion": "auth", "test": "test_auth",
         "required": False},
    ]}
    links = {"components": [
        {"id": "R71-C01", "supporting_checks": ["R71-I01", "R71-B01"]},
        {"id": "R71-C18", "supporting_checks": ["R71-B15"]},
    ]}
    external = {"items": [{"id": "EXT-01", "assessment": "PUBLIC_RECORD_OBSERVED",
                           "subject": "Zenodo", "source_url": "https://zenodo.org/records/23784504"}]}
    return p1, p2, p3, links, external


def junit(path, cases):
    path.write_text(
        "<testsuite>" + "".join(
            '<testcase name="' + name + '">' +
            ("<skipped/>" if status == "SKIPPED" else "<failure/>" if status == "FAIL" else "") +
            "</testcase>"
            for name, status in cases
        ) + "</testsuite>",
        encoding="utf-8",
    )


class Phase4ConsolidationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.folder = Path(self.temp.name)
        self.p1, self.p2, self.p3, self.links, self.external = fixtures()
        self.xml1 = self.folder / "phase1.xml"
        self.xml2 = self.folder / "phase2.xml"
        self.xml3 = self.folder / "phase3.xml"
        self.snapshot = self.folder / "canonical.zip"

    def consolidate(self):
        return phase4.collect(
            phase1_xml=self.xml1, phase2_xml=self.xml2, phase3_xml=self.xml3,
            snapshot=self.snapshot, phase1_matrix=self.p1,
            phase2_matrix=self.p2, phase3_matrix=self.p3,
            external=self.external, links=self.links, commit="ref-123",
            phase2_environment="production observations with staging opt-in",
            phase3_environment="production-readonly",
        )

    def test_all_technical_checks_pass_but_overall_still_pending(self):
        junit(self.xml1, [("test_unit", "PASS")])
        junit(self.xml2, [("test_integration", "PASS")])
        junit(self.xml3, [("test_browser", "PASS"), ("test_auth", "PASS")])
        self.snapshot.write_bytes(b"fake-only-for-hashing")
        result = self.consolidate()
        self.assertTrue(result["technical_evidence_present_and_passing"])
        self.assertEqual(result["phase1"]["automated_passed"], 1)
        self.assertEqual(result["phase1"]["component_total"], 2)
        self.assertEqual(result["phase2"]["passed"], 1)
        self.assertEqual(result["phase3"]["passed_required"], 1)
        self.assertEqual(result["phase3"]["passed_optional"], 1)
        self.assertEqual(result["overall_kpi_status"], "NOT YET VERIFIED")
        self.assertEqual(result["accepted_components"], 0)
        self.assertTrue(all(not item["accepted_against_requirements"]
                            for item in result["components"]))
        self.assertEqual(result["artifacts"][3]["size_bytes"], len(b"fake-only-for-hashing"))
        text = phase4.markdown(result)
        self.assertIn("1/2 components", text)
        self.assertIn("M40", text)

    def test_missing_evidence_never_becomes_success(self):
        result = self.consolidate()
        self.assertFalse(result["technical_evidence_present_and_passing"])
        self.assertEqual(result["phase1"]["mapped_tests_seen"], 0)
        self.assertEqual(result["phase2"]["passed"], 0)
        self.assertEqual(result["phase3"]["passed_optional"], 0)
        self.assertFalse(result["artifacts"][0]["exists"])

    def test_skipped_optional_browser_case_prevents_full_technical_status(self):
        junit(self.xml1, [("test_unit", "PASS")])
        junit(self.xml2, [("test_integration", "PASS")])
        junit(self.xml3, [("test_browser", "PASS"), ("test_auth", "SKIPPED")])
        result = self.consolidate()
        self.assertFalse(result["technical_evidence_present_and_passing"])
        self.assertEqual(result["phase3"]["status"], "INCOMPLETE")

    def test_unknown_linked_case_refused(self):
        self.links["components"][0]["supporting_checks"] = ["NONEXISTENT"]
        with self.assertRaisesRegex(ValueError, "Unknown supporting tests"):
            self.consolidate()

    def test_real_traceability_files_are_consistent(self):
        root = Path(__file__).resolve().parents[1]
        matrix = phase4.read_json(root / "docs/R7_1_VALIDATION_MATRIX.json")
        m2 = phase4.read_json(root / "docs/R7_1_PHASE2_MATRIX.json")
        m3 = phase4.read_json(root / "docs/R7_1_PHASE3_MATRIX.json")
        links = phase4.read_json(root / "docs/R7_1_PHASE4_LINKS.json")
        self.assertEqual(len(matrix["components"]), 19)
        self.assertEqual(len(m2["checks"]), 10)
        self.assertEqual(len(m3["cases"]), 18)
        all_checks = {x["id"] for x in m2["checks"] + m3["cases"]}
        self.assertEqual(
            {x["id"] for x in links["components"]},
            {x["id"] for x in matrix["components"]},
        )
        for item in links["components"]:
            self.assertFalse(set(item["supporting_checks"]) - all_checks)


if __name__ == "__main__":
    unittest.main()
