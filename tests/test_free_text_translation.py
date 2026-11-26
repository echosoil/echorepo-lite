from unittest.mock import Mock

import pytest

from tools.translate_free_text_en import (
    FIELDS,
    Translator,
    available_sources,
    choose_source,
    detect_languages,
    spreadsheet_safe,
    translate_text,
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
