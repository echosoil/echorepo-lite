from unittest.mock import Mock

import pytest
from flask import Flask

from echorepo.routes import i18n_admin as admin


@pytest.fixture
def client(monkeypatch):
    app = Flask(__name__)
    app.secret_key = "test"
    app.register_blueprint(admin.bp)
    monkeypatch.setenv("I18N_ADMIN_EDITORS", "editor@example.org")
    with app.test_client() as client:
        with client.session_transaction() as state:
            state["kc"] = {"profile": {"email": "editor@example.org"}}
            state["_manual_en_csrf"] = "token"
        yield client


def test_blank_search_does_not_query_postgres(client, monkeypatch):
    search = Mock()
    monkeypatch.setattr(admin, "search_manual_en", search)
    monkeypatch.setattr(admin, "render_template", lambda *args, **kwargs: "search only")
    response = client.get("/i18n/admin?scope=manual_en")
    assert response.status_code == 200
    assert response.headers["Cache-Control"] == "private, no-store"
    search.assert_not_called()


def test_substring_search_is_forwarded(client, monkeypatch):
    search = Mock(return_value=([], False))
    monkeypatch.setattr(admin, "search_manual_en", search)
    monkeypatch.setattr(admin, "render_template", lambda *args, **kwargs: "results")
    assert client.get("/i18n/admin?scope=manual_en&q=Champ%25&page=2").status_code == 200
    search.assert_called_once_with("Champ%", 2)


def payload(**updates):
    return {
        "field": "image_description", "country": "IT", "orig": "campione 3",
        "value": "sample 3", "expected": None, "action": "save", "csrf_token": "token",
        **updates,
    }


def test_save_injects_exact_scope(client, monkeypatch):
    change = Mock(return_value={"matched": 2, "updated": 2, "override": "sample 3"})
    monkeypatch.setattr(admin, "change_manual_en", change)
    response = client.post("/i18n/admin/manual_set", json=payload())
    assert response.status_code == 200
    assert response.json["updated"] == 2
    change.assert_called_once_with(
        "image_description", "IT", "campione 3", "sample 3", None, "editor@example.org",
        clear=False,
    )


@pytest.mark.parametrize("updates", [
    {"csrf_token": "wrong"}, {"field": "soil_texture"}, {"value": ""},
    {"orig": ""}, {"action": "unknown"},
])
def test_invalid_mutation_is_rejected(client, monkeypatch, updates):
    change = Mock()
    monkeypatch.setattr(admin, "change_manual_en", change)
    assert client.post("/i18n/admin/manual_set", json=payload(**updates)).status_code == 400
    change.assert_not_called()


def test_signed_in_non_editor_denied(client, monkeypatch):
    monkeypatch.setenv("I18N_ADMIN_EDITORS", "someone-else@example.org")
    assert client.get("/i18n/admin?scope=manual_en&q=soil").status_code == 403
    assert client.post("/i18n/admin/manual_set", json=payload()).status_code == 403


def test_remove_protection_is_explicit(client, monkeypatch):
    change = Mock(return_value={"matched": 2, "updated": 0, "override": None})
    monkeypatch.setattr(admin, "change_manual_en", change)
    response = client.post(
        "/i18n/admin/manual_set", json=payload(action="clear", expected="sample 3", value="")
    )
    assert response.status_code == 200
    change.assert_called_once_with(
        "image_description", "IT", "campione 3", None, "sample 3", "editor@example.org",
        clear=True,
    )
