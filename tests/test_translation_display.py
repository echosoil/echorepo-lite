from echorepo.services.personal_translations import row_key
from echorepo.utils.table import translation_html


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
