from unittest.mock import Mock

import pytest

from tools.translate_free_text_en import FIELDS, translate_text


def test_only_free_text_columns():
    assert {field for _, _, field in FIELDS} == {
        "contamination_other", "observations", "metals_info", "image_description"
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
