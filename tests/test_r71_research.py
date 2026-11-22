"""R7-1 research client and Streamlit data-helper acceptance tests.

Only import the notebook API client and extract two pure Streamlit helpers:
importing the Streamlit UI at module level would start an interactive app.
"""
from __future__ import annotations

import ast
import importlib.util
import unittest
from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]


def load_client_class():
    path = ROOT / "notebooks/lib/echorepo.py"
    spec = importlib.util.spec_from_file_location("r71_notebook_echorepo", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.EchoRepo


def load_streamlit_helpers():
    path = ROOT / "streamlit/streamlit_app.py"
    syntax = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    wanted = {"countries_for", "csv_download"}
    functions = [
        node for node in syntax.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name in wanted
    ]
    if {node.name for node in functions} != wanted:
        raise AssertionError("Missing Streamlit data helpers")
    code = compile(ast.Module(body=functions, type_ignores=[]), str(path), "exec")
    namespace = {"pd": pd}
    exec(code, namespace)
    return namespace


class R71NotebookAPIClientTests(unittest.TestCase):
    def test_r71_research_01_csv_client_transmits_filters_and_parses(self):
        repo = load_client_class()(base_url="https://example.org/")
        response = MagicMock()
        response.headers = {"Content-Type": "text/csv; charset=utf-8"}
        response.content = b"sample_id,soil_ph_field\nABCD-1234,6.7\n"
        repo.session.get = MagicMock(return_value=response)
        result = repo.samples(country="ES")
        self.assertEqual(result["sample_id"].tolist(), ["ABCD-1234"])
        self.assertEqual(repo.session.get.call_args.args[0], "https://example.org/api/v1/canonical/samples")
        self.assertEqual(repo.session.get.call_args.kwargs["params"], {"format": "csv", "country": "ES"})
        self.assertEqual(repo.session.get.call_args.kwargs["timeout"], 300)

    def test_r71_research_02_unexpected_non_csv_response_rejected(self):
        repo = load_client_class()(base_url="https://example.org")
        response = MagicMock()
        response.headers = {"Content-Type": "text/html"}
        response.url = "https://example.org/api/v1/canonical/samples"
        response.text = "<html>login</html>"
        repo.session.get = MagicMock(return_value=response)
        with self.assertRaisesRegex(RuntimeError, "Expected CSV"):
            repo.samples()

    def test_r71_research_03_paginated_request_uses_json_contract(self):
        repo = load_client_class()(base_url="https://example.org")
        response = MagicMock()
        response.json.return_value = {"data": [{"sample_id": "ABCD-1234"}], "meta": {"count": 1}}
        repo.session.get = MagicMock(return_value=response)
        frame = repo.query_samples(limit=5, offset=3, country="ES")
        self.assertEqual(frame["sample_id"].tolist(), ["ABCD-1234"])
        self.assertEqual(repo.session.get.call_args.kwargs["params"], {
            "format": "json", "limit": 5, "offset": 3, "country": "ES",
        })


class R71StreamlitDataTests(unittest.TestCase):
    def test_r71_streamlit_01_country_list_omits_missing_and_sorts(self):
        helpers = load_streamlit_helpers()
        frame = pd.DataFrame({"country_code": ["ES", None, "DE", "ES"]})
        self.assertEqual(helpers["countries_for"](frame), ["DE", "ES"])
        self.assertEqual(helpers["countries_for"](pd.DataFrame()), [])

    def test_r71_streamlit_02_csv_export_is_utf8_and_preserves_headers(self):
        helpers = load_streamlit_helpers()
        frame = pd.DataFrame({"sample_id": ["ABCD-1234"], "country_code": ["ES"]})
        result = helpers["csv_download"](frame).decode("utf-8")
        self.assertEqual(result, "sample_id,country_code\nABCD-1234,ES\n")


if __name__ == "__main__":
    unittest.main()
