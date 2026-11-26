import ast
from pathlib import Path

import pandas as pd
import pytest

from echorepo.utils.photo_descriptions import photo_description, photo_slots, photo_text
from echorepo.utils.table import make_table_html

ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize(
    ("option", "comment", "expected"),
    [
        ("campione 3", None, "campione 3"),
        (float("nan"), "Note", "Note"),
        ("Caption", " Note ", "Caption\nNote"),
        ("Same", "Same", "Same"),
        (pd.NA, "nan", ""),
    ],
)
def test_photo_description(option, comment, expected):
    assert photo_description(option, comment) == expected


@pytest.mark.parametrize(
    "script", ["pull_and_enrich_samples.py", "pull_and_enrich_samples_noblur.py"]
)
def test_canonical_image_builder(script):
    # Exercise the actual builder without importing Firebase or initializing storage.
    source = (ROOT / "tools" / script).read_text(encoding="utf-8")
    tree = ast.parse(source)
    builder = next(
        node for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name == "build_sample_images_df"
    )
    namespace = {
        "pd": pd,
        "photo_slots": photo_slots,
        "photo_text": photo_text,
        "photo_description": photo_description,
        "_to_float_num": float,
        "norm_qr_for_id": lambda value: value,
        "DEFAULT_LICENCE": "CC-BY-4.0",
    }
    exec(compile(ast.Module(body=[builder], type_ignores=[]), script, "exec"), namespace)
    raw = pd.DataFrame([{
        "QR_qrCode": "TEST-1234",
        "sampleId": "hash",
        "GPS_lat": 45.0,
        "GPS_long": 9.0,
        "actual_cc": "IT",
        "PHOTO_photos_1_path": "https://example.com/1.jpg",
        "PHOTO_photos_1_option": "campione 3",
        "PHOTO_photos_1_comment": "",
        "PHOTO_photos_2_path": "https://example.com/2.jpg",
        "PHOTO_photos_2_option": "campione 3a",
        "PHOTO_photos_18_path": "https://example.com/18.jpg",
        "PHOTO_photos_18_comment": "note",
        "PHOTO_photos_4_path": float("nan"),
    }])
    result = namespace["build_sample_images_df"](raw)
    assert result["image_id"].tolist() == [1, 2, 18]
    assert result["image_description_orig"].tolist() == ["campione 3", "campione 3a", "note"]
    assert result["image_description_en"].tolist() == ["", "", ""]
    assert "sample_images.image_description_orig IS DISTINCT FROM EXCLUDED.image_description_orig" in source


def test_option_only_photo_caption_has_translation_in_personal_table():
    frame = pd.DataFrame([{
        "sampleId": "hash",
        "QR_qrCode": "TEST-1234",
        "userId": "owner",
        "PHOTO_photos_1_path": "https://example.com/photo.jpg",
        "PHOTO_photos_1_option": "campione 3",
        "PHOTO_photos_1_comment": "",
    }])
    images = {("TEST-1234", "owner", 1): {
        "image_description_orig": "campione 3",
        "image_description_en": "sample 3",
    }}
    rendered = make_table_html(frame, translations=({}, images))
    assert "campione 3" in rendered
    assert "sample 3" in rendered
    assert 'class="echo-translation"' in rendered
    assert rendered.count("<figcaption>") == 1
