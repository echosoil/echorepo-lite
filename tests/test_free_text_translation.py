import csv
from unittest.mock import Mock

import pytest

from tools.translate_free_text_en import (
    FIELDS,
    LanguageChoice,
    Translator,
    available_sources,
    choose_source,
    detect_languages,
    spreadsheet_safe,
    translate_text,
    write_comparison,
)


def test_only_free_text_columns():
    assert {field for _, _, field in FIELDS} == {
        "contamination_other",
        "observations",
        "metals_info",
        "image_description",
    }


@pytest.mark.parametrize(
    ("candidate", "language", "expected"),
    [("soil", "es", "soil"), ("suelo", "es", None), ("suelo", "en", "suelo")],
)
def test_translate_text(candidate, language, expected):
    session = Mock()
    session.post.return_value.json.return_value = {
        "translatedText": candidate,
        "detectedLanguage": {"language": language},
    }
    assert translate_text(session, "http://localhost:5001", "suelo", 10) == expected


def test_empty_translation_fails():
    session = Mock()
    session.post.return_value.json.return_value = {"translatedText": ""}
    with pytest.raises(ValueError):
        translate_text(session, "http://localhost:5001", "suelo", 10)


SUPPORTED = frozenset({"en", "it", "es", "fr", "de", "nl", "nb"})


@pytest.mark.parametrize(
    ("text", "country", "candidates", "source", "reason"),
    [
        ("campione 3a", "IT", [("es", 91), ("it", 9)], "it", "country_prior"),
        ("The soil contains small stones", "IT", [("en", 91)], "en", "strong_detection"),
        ("sample 3", "IT", [("en", 98)], "en", "strong_detection"),
        ("sol humide", "BE", [("fr", 88)], "fr", "multilingual_detection"),
        ("terra humida amb pedres", "ES", [("ca", 99)], None, "unsupported_detected"),
        ("tekst", "HR", [("hr", 99)], None, "unsupported_detected"),
        ("unknown", None, [], "auto", "no_supported_prior"),
        ("jord", "NO", [("sv", 65)], "nb", "country_prior"),
        ("campione", "GB", [("it", 60)], "auto", "no_supported_prior"),
        ("sample", "IT", [("en", 96), ("it", 90)], "it", "country_prior"),
    ],
)
def test_hybrid_selection(text, country, candidates, source, reason):
    decision = choose_source(text, country, "hybrid", SUPPORTED, candidates)
    assert decision.source == source
    assert decision.reason == reason


def test_explicit_strategy():
    assert choose_source("text", "IT", "auto", SUPPORTED).source == "auto"
    assert choose_source("text", "IT", "country", SUPPORTED, [("en", 99)]).source == "it"
    assert choose_source("text", "HR", "country", SUPPORTED).source == "auto"


def test_detect_percentage_and_sort():
    session = Mock()
    session.post.return_value.json.return_value = [
        {"language": "es", "confidence": 5},
        {"language": "it", "confidence": 95},
    ]
    assert detect_languages(session, "http://localhost", "campione", 10) == [
        ("it", 95.0), ("es", 5.0)
    ]


def test_model_availability_requires_english_target():
    session = Mock()
    session.get.return_value.json.return_value = [
        {"code": "en", "targets": ["it"]},
        {"code": "it", "targets": ["en"]},
        {"code": "hr", "targets": ["de"]},
    ]
    assert available_sources(session, "http://localhost", 10) == {"en", "it"}


def test_cache_is_country_and_strategy_aware():
    session = Mock()

    def post(url, json, timeout):
        response = Mock()
        if url.endswith("/detect"):
            response.json.return_value = [{"language": "es", "confidence": 65}]
        else:
            response.json.return_value = {"translatedText": json["source"] + " result"}
        return response

    session.post.side_effect = post
    translator = Translator(session, "http://localhost", 10, SUPPORTED)
    assert translator.translate("terra", "IT", "hybrid")[0] == "it result"
    assert translator.translate("terra", "ES", "hybrid")[0] == "es result"
    assert translator.translate("terra", "IT", "hybrid")[0] == "it result"
    assert session.post.call_count == 3


def test_explicit_source_unchanged_is_not_success():
    session = Mock()
    session.post.return_value.json.return_value = {"translatedText": "campione"}
    assert translate_text(session, "http://localhost", "campione", 10, "it") is None


def test_spreadsheet_formula_is_escaped():
    assert spreadsheet_safe("=1+1") == "'=1+1"
    assert spreadsheet_safe("campione 3") == "campione 3"


def test_comparison_writes_review_csv_without_update(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "tools.translate_free_text_en.comparison_rows",
        lambda conn, per_country, countries: [("IT", "campione 3", "image_description")],
    )
    translator = Mock()
    translator.translate.side_effect = [
        ("sample 3", LanguageChoice("auto", "auto")),
        ("sample 3", LanguageChoice("it", "forced_country")),
        ("sample 3", LanguageChoice("it", "country_prior", "es", 60)),
    ]
    conn = Mock()
    path = tmp_path / "comparison.csv"
    assert write_comparison(conn, translator, path, 50, ["IT"]) == 0
    conn.cursor.assert_not_called()
    with path.open(encoding="utf-8", newline="") as handle:
        row = next(csv.DictReader(handle))
    assert row["hybrid_source"] == "it"
    assert row["hybrid_en"] == "sample 3"
    assert row["original"] == "campione 3"
    assert row["hybrid_status"] == "ok"
    with pytest.raises(FileExistsError):
        write_comparison(conn, translator, path, 50, ["IT"])


def test_country_mode_does_not_depend_on_detector():
    session = Mock()
    session.post.return_value.json.return_value = {"translatedText": "sample 3"}
    translator = Translator(session, "http://localhost", 10, SUPPORTED)
    assert translator.translate("campione 3", "IT", "country")[0] == "sample 3"
    assert session.post.call_count == 1
    assert session.post.call_args.kwargs["json"]["source"] == "it"
