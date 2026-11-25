import io
import zipfile

from tools.ro41_coverage import biodiversity, canonical, validation


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
