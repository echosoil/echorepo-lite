import json

from tools.guild_publication_audit import FAP_FILES, REFERENCE, audit


def test_local_audit_alignment_and_overlapping_functions(tmp_path):
    (tmp_path / FAP_FILES[0]).write_text(
        ",function_a,function_b\nPRIVATE-A,0.8,0.9\nPRIVATE-B,0,0\n",
        encoding="utf-8",
    )
    (tmp_path / FAP_FILES[1]).write_text(
        "Sample,Annotated_frac,Annotated_pct\nprivate-a,0.4321,43.21\nPRIVATE-B,0,0\n",
        encoding="utf-8",
    )
    (tmp_path / REFERENCE).write_text(
        json.dumps([
            {"taxon": "Example", "confidenceRanking": "Probable",
             "guild": "Plant Pathogen", "trophicMode": "Pathotroph"},
            {"taxon": "Example", "confidenceRanking": "Possible",
             "guild": "Plant Pathogen", "trophicMode": "Pathotroph"},
        ]),
        encoding="utf-8",
    )
    result = audit(tmp_path)
    profile = result["observations"][FAP_FILES[0]]
    coverage = result["observations"][FAP_FILES[1]]
    assert profile["profile_sum_above_one_rows"] == 1
    assert profile["all_zero_profile_rows"] == 1
    assert profile["invalid_fraction_cells"] == 0
    assert coverage["fraction_percent_mismatch_rows"] == 0
    assert result["faprotax_label_alignment"]["sets_match"]
    assert result["files"][FAP_FILES[0]]["sha256"]
    assert not result["publication_ready"]
    assert "PRIVATE-A" not in json.dumps(result).upper()
    assert "PRIVATE-B" not in json.dumps(result).upper()


def test_bad_values_and_mismatched_labels(tmp_path):
    (tmp_path / FAP_FILES[0]).write_text(
        ",a\nSAMPLE-X,NaN\nSAMPLE-X,1.2\nSAMPLE-Y,0.4\n",
        encoding="utf-8",
    )
    (tmp_path / FAP_FILES[1]).write_text(
        "Sample,Annotated_frac,Annotated_pct\nSAMPLE-Y,0.2,80\nSAMPLE-Z,0.5,50\n",
        encoding="utf-8",
    )
    result = audit(tmp_path)
    profile = result["observations"][FAP_FILES[0]]
    coverage = result["observations"][FAP_FILES[1]]
    assert profile["duplicate_normalized_labels"] == 1
    assert profile["invalid_fraction_cells"] == 2
    assert coverage["fraction_percent_mismatch_rows"] == 1
    assert not result["faprotax_label_alignment"]["sets_match"]
    assert result["observations"][REFERENCE]["status"] == "MISSING"


def test_input_header_inventory(tmp_path):
    (tmp_path / FAP_FILES[0]).write_text(",a\nA,0.1\n", encoding="utf-8")
    work = tmp_path / "faprotax_work"
    work.mkdir()
    (work / "6_otu_clean_counts_no_blanks.csv").write_text(
        ",A,B\nfeature,1,2\n", encoding="utf-8"
    )
    result = audit(tmp_path, hash_inputs=True)
    assert result["input_inventory"]["otu_matrix_header"]["matrix_only_labels"] == 1
    assert result["input_inventory"]["faprotax_work/6_otu_clean_counts_no_blanks.csv"]["sha256"]
