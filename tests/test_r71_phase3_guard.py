"""Offline tests for Phase 3 safety gates and evidence accounting."""
from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from tools import r71_phase3_guard as guard
from tools import r71_phase3_report as report


class Phase3GuardTests(unittest.TestCase):
    def test_prod_requires_exact_ack(self):
        with self.assertRaises(guard.BrowserTargetError):
            guard.validate_target(
                "production-readonly", "http://127.0.0.1:8001",
                environ={"R71_PHASE3_PROD_ACK": ""},
            )
        self.assertEqual(
            guard.validate_target(
                "production-readonly", "http://127.0.0.1:8001",
                environ={"R71_PHASE3_PROD_ACK": guard.PROD_ACK},
            ),
            "http://127.0.0.1:8001",
        )

    def test_prod_known_local_port_cannot_be_mislabeled_staging(self):
        with self.assertRaises(guard.BrowserTargetError):
            guard.validate_target("staging", "http://127.0.0.1:8001", environ={})

    def test_production_domain_cannot_be_mislabeled_staging(self):
        with self.assertRaises(guard.BrowserTargetError):
            guard.validate_target(
                "staging", "https://echorepo.quanta-labs.com",
                environ={"R71_PHASE3_ALLOWED_HOSTS": "echorepo.quanta-labs.com"},
            )

    def test_remote_production_requires_https_and_allowlist(self):
        allowed = {
            "R71_PHASE3_PROD_ACK": guard.PROD_ACK,
            "R71_PHASE3_ALLOWED_HOSTS": "echorepo.quanta-labs.com",
        }
        self.assertEqual(
            guard.validate_target(
                "production-readonly", "https://echorepo.quanta-labs.com",
                environ=allowed,
            ),
            "https://echorepo.quanta-labs.com",
        )
        with self.assertRaises(guard.BrowserTargetError):
            guard.validate_target(
                "production-readonly", "http://echorepo.quanta-labs.com",
                environ=allowed,
            )
        with self.assertRaises(guard.BrowserTargetError):
            guard.validate_target(
                "production-readonly", "https://unknown.example",
                environ=allowed,
            )

    def test_staging_localhost_allowed_on_different_port(self):
        self.assertEqual(
            guard.validate_target("staging", "http://127.0.0.1:18080", environ={}),
            "http://127.0.0.1:18080",
        )

    def test_rejects_url_credentials_queries_and_paths(self):
        for url in (
            "http://user:secret@localhost:1234",
            "http://localhost:1234/?token=123",
            "http://localhost:1234/admin",
            "javascript:alert(1)",
            "http://localhost:1234#secret",
        ):
            with self.subTest(url=url), self.assertRaises(guard.BrowserTargetError):
                guard.validate_target("staging", url, environ={})

    def test_unknown_mode_never_approved(self):
        with self.assertRaises(guard.BrowserTargetError):
            guard.validate_target("live", "http://127.0.0.1:18080", environ={})


class Phase3ReportTests(unittest.TestCase):
    def test_missing_and_skipped_never_count_as_pass(self):
        matrix = {"kpi": "R7-1", "phase": "browser", "cases": [
            {"id": "A", "component": "Map", "criterion": "renders", "test": "good", "required": True},
            {"id": "B", "component": "Search", "criterion": "filters", "test": "skipped", "required": True},
            {"id": "C", "component": "Private", "criterion": "auth", "test": "not_seen", "required": False},
        ]}
        with tempfile.TemporaryDirectory() as path:
            file = Path(path) / "results.xml"
            file.write_text(
                '<testsuite><testcase name="good" />'
                '<testcase name="skipped"><skipped/></testcase></testsuite>',
                encoding="utf-8",
            )
            result = report.evaluate(
                matrix, report.load_junit(file),
                mode="production-readonly", commit="abc", origin="http://127.0.0.1:8001",
            )
            self.assertEqual(result["required_passed"], 1)
            self.assertEqual(result["optional_passed"], 0)
            self.assertFalse(result["required_suite_complete"])
            self.assertFalse(result["all_checks_complete"])
            self.assertEqual(
                [c["result"] for c in result["cases"]],
                ["PASS", "SKIPPED", "NOT_RUN"],
            )
            self.assertIn("production-readonly", report.render_markdown(result))

    def test_only_core_can_complete_when_optional_missing(self):
        matrix = {"kpi": "R7-1", "phase": "browser", "cases": [
            {"id": "A", "component": "Map", "criterion": "renders", "test": "good", "required": True},
            {"id": "B", "component": "Private", "criterion": "auth", "test": "optional", "required": False},
        ]}
        result = report.evaluate(matrix, {"good": ["PASS"]}, mode="staging", commit="abc")
        self.assertTrue(result["required_suite_complete"])
        self.assertFalse(result["all_checks_complete"])

    def test_duplicated_name_is_never_success(self):
        matrix = {"kpi": "R7-1", "phase": "browser", "cases": [
            {"id": "A", "component": "Map", "criterion": "renders", "test": "same", "required": True},
        ]}
        result = report.evaluate(matrix, {"same": ["PASS", "PASS"]}, mode="staging", commit="")
        self.assertEqual(result["cases"][0]["result"], "AMBIGUOUS")
        self.assertEqual(result["required_passed"], 0)


    def test_phase3_cli_strict_modes_and_report_files(self):
        """Exercise argparse and file writing, not only report.evaluate()."""
        matrix = {
            "kpi": "R7-1", "phase": "browser",
            "cases": [
                {"id": "B01", "component": "Map", "criterion": "loads",
                 "test": "test_ok", "required": True},
                {"id": "B15", "component": "Private", "criterion": "login",
                 "test": "test_optional", "required": False},
            ],
        }
        with tempfile.TemporaryDirectory() as directory:
            folder = Path(directory)
            xml = folder / "junit.xml"
            matrix_path = folder / "matrix.json"
            output_json = folder / "report.json"
            output_md = folder / "report.md"
            xml.write_text(
                '<testsuite tests="2"><testcase name="test_ok"/>'
                '<testcase name="test_optional"><skipped/></testcase></testsuite>',
                encoding="utf-8",
            )
            matrix_path.write_text(json.dumps(matrix), encoding="utf-8")
            common_args = [
                "r71_phase3_report.py",
                "--xml", str(xml),
                "--matrix", str(matrix_path),
                "--json", str(output_json),
                "--markdown", str(output_md),
                "--commit", "verified123",
            ]
            env = {
                "R71_PHASE3_RUN": "production-readonly",
                "R71_PHASE3_BASE_URL": "http://127.0.0.1:8001",
            }
            with patch.dict("os.environ", env):
                with patch("sys.argv", common_args + ["--require-core"]):
                    self.assertEqual(report.main(), 0)
                written = json.loads(output_json.read_text(encoding="utf-8"))
                self.assertEqual(written["required_passed"], 1)
                self.assertEqual(written["optional_passed"], 0)
                self.assertEqual(written["tested_commit"], "verified123")
                self.assertTrue(output_md.read_text(encoding="utf-8").startswith("# ECHOREPO R7-1"))
                with patch("sys.argv", common_args + ["--require-all"]):
                    self.assertEqual(report.main(), 1)


if __name__ == "__main__":
    unittest.main()
