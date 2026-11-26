import csv
import sys
from unittest.mock import MagicMock, Mock

import pytest

from tools import translate_free_text_en as cli
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


@pytest.mark.parametrize("preflight", [False, True])
def test_comparison_cli_preserves_output_path(tmp_path, monkeypatch, preflight):
    """Exercise CLI dispatch and the actual writer, not just the writer in isolation."""
    env = tmp_path / ".env"
    env.write_text("DB_PASSWORD=test-password\n", encoding="utf-8")
    output = tmp_path / "review.csv"
    argv = [
        "translate_free_text_en",
        "--env-file",
        str(env),
        "--db-port",
        "5432",
        "--compare-country",
        "IT",
        "--compare-per-country",
        "1",
        "--compare-output",
        str(output),
    ]
    if preflight:
        argv.append("--preflight")
    monkeypatch.setattr(sys, "argv", argv)
    monkeypatch.setenv("DB_PASSWORD", "test-password")
    conn = MagicMock()
    conn.cursor.return_value.__enter__.return_value.fetchone.return_value = (0,)
    monkeypatch.setattr(cli.psycopg2, "connect", Mock(return_value=conn))
    monkeypatch.setattr(cli.requests, "Session", Mock())
    models = Mock(return_value=SUPPORTED)
    monkeypatch.setattr(cli, "available_sources", models)
    monkeypatch.setattr(cli, "translate_text", Mock(return_value="Hello everyone"))
    monkeypatch.setattr(
        cli, "comparison_rows", Mock(return_value=[("IT", "campione 3", "image_description")])
    )
    translator = Mock()
    translator.translate.return_value = ("sample 3", LanguageChoice("it", "country_prior"))
    monkeypatch.setattr(cli, "Translator", Mock(return_value=translator))

    assert cli.main() == 0
    conn.set_session.assert_called_once_with(readonly=True)
    if preflight:
        assert not output.exists()
        models.assert_not_called()
        translator.translate.assert_not_called()
    else:
        with output.open(encoding="utf-8", newline="") as handle:
            row = next(csv.DictReader(handle))
        assert row["original"] == "campione 3"
        assert row["hybrid_en"] == "sample 3"
        assert translator.translate.call_count == 3
        cli.comparison_rows.assert_called_once_with(conn, 1, ["IT"])
        conn.cursor.assert_not_called()


def setup_retranslation_cli(tmp_path, monkeypatch, translator, extra=()):
    env = tmp_path / ".env"
    env.write_text("DB_PASSWORD=test-password\n", encoding="utf-8")
    backup = tmp_path / "backup.csv"
    argv = [
        "translate_free_text_en", "--env-file", str(env), "--db-port", "5432",
        "--apply", "--retranslate-country", "IT", "--backup-output", str(backup),
        *extra,
    ]
    monkeypatch.setattr(sys, "argv", argv)
    monkeypatch.setenv("DB_PASSWORD", "test-password")
    conn = MagicMock()
    cursor = conn.cursor.return_value.__enter__.return_value
    cursor.fetchone.return_value = (1,)
    cursor.fetchall.return_value = [("sample", "campione", "IT", "champion")]
    cursor.rowcount = 1
    monkeypatch.setattr(cli, "FIELDS", (("samples", ("sample_id",), "observations"),))
    monkeypatch.setattr(cli.psycopg2, "connect", Mock(return_value=conn))
    monkeypatch.setattr(cli.requests, "Session", Mock())
    monkeypatch.setattr(cli, "available_sources", Mock(return_value=SUPPORTED))
    monkeypatch.setattr(cli, "translate_text", Mock(return_value="Hello everyone"))
    monkeypatch.setattr(cli, "Translator", Mock(return_value=translator))
    return conn, cursor, backup


def test_retranslation_cli_journals_before_guarded_update(tmp_path, monkeypatch):
    translator = Mock()
    translator.translate.return_value = ("sample", LanguageChoice("it", "country_prior"))
    conn, cursor, backup = setup_retranslation_cli(tmp_path, monkeypatch, translator)

    def check_execute(query, params):
        if params and params[0] == "sample":
            assert "pending" in backup.read_text(encoding="utf-8")
            assert "champion" in backup.read_text(encoding="utf-8")
            assert params == ("sample", "sample", "campione", "IT", "champion")

    cursor.execute.side_effect = check_execute
    assert cli.main() == 0
    conn.set_session.assert_called_once_with(readonly=False)
    with backup.open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert [row["event"] for row in rows] == ["scope", "pending", "committed"]
    assert backup.stat().st_mode & 0o777 == 0o600


def test_failed_retranslation_retains_previous_value(tmp_path, monkeypatch):
    translator = Mock()
    translator.translate.return_value = (None, LanguageChoice(None, "unsupported_detected"))
    _, cursor, backup = setup_retranslation_cli(tmp_path, monkeypatch, translator)
    assert cli.main() == 0
    assert cursor.execute.call_count == 2  # count and select only
    assert "pending" not in backup.read_text(encoding="utf-8")


def test_retranslation_preflight_never_creates_backup(tmp_path, monkeypatch):
    translator = Mock()
    conn, cursor, backup = setup_retranslation_cli(
        tmp_path, monkeypatch, translator, extra=("--preflight",)
    )
    assert cli.main() == 0
    assert not backup.exists()
    conn.set_session.assert_called_once_with(readonly=True)
    translator.translate.assert_not_called()
    assert cursor.execute.call_count == 1


def test_retranslation_journal_resume_and_conflicts(tmp_path):
    from tools.retranslation_journal import RetranslationJournal

    backup = tmp_path / "journal.csv"
    scope = {"database": "test"}
    key = ("samples", "observations", ("sample",))
    journal = RetranslationJournal(backup, scope)
    journal.record("pending", *key, "campione", "IT", None, "sample")
    journal.close()
    journal = RetranslationJournal(backup, scope, resume=True)
    assert journal.resume_status(*key, "campione", "IT", None) == "retry"
    assert journal.resume_status(*key, "campione", "IT", "sample") == "done"
    assert journal.resume_status(*key, "campione", "IT", "sample") == "done"
    assert journal.resume_status(*key, "edited", "IT", "sample") == "conflict"
    assert journal.resume_status(*key, "campione", "IT", "manual edit") == "conflict"
    journal.close()
    with pytest.raises(ValueError, match="scope"):
        RetranslationJournal(backup, {"database": "other"}, resume=True)
