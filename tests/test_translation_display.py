import pandas as pd

from echorepo.services.personal_translations import row_key
from echorepo.utils.table import make_table_html, translation_html


def test_translation_escapes_both_strings():
    result = translation_html('<script>alert(1)</script>', '"<img onerror=alert(1)>')
    assert '<script>' not in result
    assert '<img' not in result
    assert '&quot;' in result
    assert 'lang="en"' in result
    assert '<details' in result


def test_identical_and_missing_translations_have_no_control():
    assert translation_html("soil", "soil") == "soil"
    assert translation_html("soil", None) == "soil"


def test_personal_key_preserves_identity():
    assert row_key({"QR_qrCode": " aa bb-1234 ", "userId": "u1"}) == ("AABB-1234", "u1")
    assert row_key({"sampleId": "hash", "userId": "u2"}) == ("hash", "u2")


def test_personal_table_discloses_only_matching_original_and_owner():
    df = pd.DataFrame(
        [
            {
                "sampleId": "hash",
                "QR_qrCode": "ABCD-1234",
                "userId": "owner",
                "SOIL_CONTAMINATION_comments": "piedras",
                "SOIL_DIVER_observations": "suelo",
            },
        ]
    )
    records = {
        ("ABCD-1234", "owner"): {
            "contamination_other_orig": "piedras",
            "contamination_other_en": "stones",
            "observations_orig": "old text",
            "observations_en": "stale translation",
        },
        ("ABCD-1234", "other"): {
            "observations_orig": "suelo",
            "observations_en": "not the owner's translation",
        },
    }
    rendered = make_table_html(df, translations=(records, {}))
    assert "piedras" in rendered and "stones" in rendered
    assert "suelo" in rendered
    assert "stale translation" not in rendered
    assert "not the owner" not in rendered
    assert rendered.count('class="echo-translation"') == 1
