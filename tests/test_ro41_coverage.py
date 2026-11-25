import io
import zipfile

import pytest

from tools.ro41_coverage import (
    ValidationFormatError, biodiversity, canonical, canonical_record_matches, validation,
)


def test_coverage_counts():
    r = canonical(b"sample_id,country_code\na,ES\nb,IT\nc,ES\nd,\ne,invalid\n")
    assert r["country_code_counts"] == {"ES": 2, "IT": 1}
    assert r["missing_country_code"] == r["malformed_country_code"] == 1


def test_marker_labels():
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("biodiversity_16S.csv", "OTU ID,taxonomy_id,a-16S,b-16S\n")
        z.writestr("biodiversity_ITS.csv", "OTU ID,taxonomy_id,b-ITS,c-ITS\n")
        z.writestr("biodiversity_taxonomy.csv", "id\n")
        z.writestr("biodiversity_metadata.json", "{}")
    r = biodiversity(buf.getvalue())
    assert r["shared_base_labels"] == 1 and r["union_base_labels"] == 3


def test_validation_pairs():
    r = validation(b"QR_code,mean_pH_lab,pH_CS\na,7,6\nb,,5\n")
    assert r["ph_paired_nonempty_or_numeric_rows"] == 1
    assert r["unique_nonempty_qr_labels"] == 2


def test_semicolon_validation_and_decimal_comma():
    result = validation(" QR_code ;mean_pH_lab;pH_CS;Lab_texture_reclass;CS_texture\n"
                        "A;7,2;6,5;L;L\nB;;5,0;S;\n".encode())
    assert result["csv_format"]["delimiter"] == "semicolon"
    assert result["ph_paired_nonempty_or_numeric_rows"] == 1
    assert result["texture_paired_nonempty_or_numeric_rows"] == 1


def test_tab_and_utf16():
    data = "QR_code\tmean_pH_lab\tpH_CS\nA\t7\t6\n".encode("utf-16")
    result = validation(data)
    assert result["csv_format"]["encoding"] == "utf-16"
    assert result["csv_format"]["delimiter"] == "tab"


def test_bad_width_and_unrecognized_header():
    for raw in (b"QR_code;mean_pH_lab;pH_CS\nPRIVATE;6\n", b"x;y\nPRIVATE;1\n"):
        with pytest.raises(ValidationFormatError) as exc:
            validation(raw)
        assert "PRIVATE" not in str(exc.value.diagnostics)


def test_duplicate_headers_not_accepted():
    with pytest.raises(ValidationFormatError):
        validation(b"QR_code;mean_pH_lab;pH_CS;pH_CS\nA;7;6;6\n")


def test_canonical_version_lineage():
    assert canonical_record_matches({"conceptrecid": "19722513"})
    assert canonical_record_matches({"conceptdoi": "10.5281/zenodo.19722513"})
    assert not canonical_record_matches({})
    assert not canonical_record_matches({"conceptrecid": "22014976"})
    assert not canonical_record_matches({
        "conceptrecid": "19722513", "conceptdoi": "10.5281/zenodo.22014976"
    })
