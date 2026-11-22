"""Offline safety and report-generation tests for R7-1 Phase 2.

These tests must NEVER touch external services.
"""
from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from tools import r71_phase2_guard as guard
from tools import r71_phase2_report as report


class Phase2SafetyTests(unittest.TestCase):
    def test_requires_explicit_staging_opt_in(self):
        with patch.dict("os.environ", {"R71_PHASE2_RUN": ""}):
            with self.assertRaises(guard.UnsafeTarget):
                guard.approved_url("http://127.0.0.1:18080")

    def test_production_host_blocked_even_when_allowlisted(self):
        with patch.dict("os.environ", {
            "R71_PHASE2_RUN": "staging",
            "R71_STAGING_HOSTS": "echorepo.quanta-labs.com",
        }):
            with self.assertRaises(guard.UnsafeTarget):
                guard.approved_url("https://echorepo.quanta-labs.com")

    def test_unlisted_remote_host_denied(self):
        with patch.dict("os.environ", {"R71_PHASE2_RUN": "staging", "R71_STAGING_HOSTS": ""}):
            with self.assertRaises(guard.UnsafeTarget):
                guard.approved_url("https://unknown.example.net")

    def test_loopback_allowed_and_api_path_fixed(self):
        with patch.dict("os.environ", {"R71_PHASE2_RUN": "staging"}):
            self.assertEqual(
                guard.endpoint_url("http://127.0.0.1:18080/api/v1", "/api/v1/ping"),
                "http://127.0.0.1:18080/api/v1/ping",
            )
            with self.assertRaises(ValueError):
                guard.endpoint_url("http://127.0.0.1:18080", "/logout")

    def test_cleartext_remote_http_requires_extra_opt_in(self):
        env = {
            "R71_PHASE2_RUN": "staging",
            "R71_STAGING_HOSTS": "echorepo-lite",
            "R71_ALLOW_HTTP_STAGING": "",
        }
        with patch.dict("os.environ", env):
            with self.assertRaises(guard.UnsafeTarget):
                guard.approved_url("http://echorepo-lite:8000")
        env["R71_ALLOW_HTTP_STAGING"] = "yes"
        with patch.dict("os.environ", env):
            self.assertEqual(
                guard.approved_url("http://echorepo-lite:8000"),
                "http://echorepo-lite:8000",
            )

    def test_credential_bearing_urls_rejected(self):
        with patch.dict("os.environ", {"R71_PHASE2_RUN": "staging"}):
            for bad in ("http://user:pass@localhost", "http://localhost/?api_key=secret"):
                with self.subTest(url=bad), self.assertRaises(guard.UnsafeTarget):
                    guard.approved_url(bad)

    def test_postgres_dsn_requires_approved_host_and_db(self):
        with patch.dict("os.environ", {
            "R71_PHASE2_RUN": "staging", "R71_STAGING_HOSTS": "postgres"
        }):
            self.assertEqual(
                guard.approved_pg_dsn("host=postgres dbname=echorepo_dev user=test password=unused"),
                "echorepo_dev",
            )
            for bad in (
                "dbname=echorepo_dev user=test",
                "host=/var/run/postgresql dbname=echorepo_dev",
                "service=private-db dbname=echorepo_dev",
            ):
                with self.subTest(dsn=bad), self.assertRaises(guard.UnsafeTarget):
                    guard.approved_pg_dsn(bad)


class Phase2ReportTests(unittest.TestCase):
    def test_skip_and_missing_are_never_passed(self):
        checks = [
            {"id": "I01", "component": "API", "name": "test_good", "criterion": "good"},
            {"id": "I02", "component": "MinIO", "name": "test_skip", "criterion": "skip"},
            {"id": "I03", "component": "OIDC", "name": "test_missing", "criterion": "missing"},
        ]
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "junit.xml"
            path.write_text(
                '<testsuite tests="2"><testcase classname="t" name="test_good"/>'
                '<testcase classname="t" name="test_skip"><skipped/></testcase></testsuite>',
                encoding="utf-8",
            )
            cases = report.junit_cases(path)
            outcome = report.evaluate(checks, cases, "abcdef")
            self.assertEqual(outcome["passed"], 1)
            self.assertFalse(outcome["complete"])
            self.assertEqual(
                [item["result"] for item in outcome["checks"]],
                ["PASS", "SKIPPED", "NOT_RUN"],
            )
            self.assertNotIn("password", report.markdown(outcome).lower())

    def test_duplicate_test_results_are_ambiguous(self):
        checks = [{"id": "I01", "component": "API", "name": "same", "criterion": "single"}]
        result = report.evaluate(checks, {"same": ["PASS", "PASS"]}, "123")
        self.assertEqual(result["checks"][0]["result"], "AMBIGUOUS")
        self.assertFalse(result["complete"])


if __name__ == "__main__":
    unittest.main()
