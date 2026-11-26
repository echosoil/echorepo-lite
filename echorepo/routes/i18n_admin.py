# echorepo/routes/i18n_admin.py
import hmac
import json
import os
import re
import secrets

from babel.support import Translations
from flask import Blueprint, Response, abort, current_app, jsonify, make_response, render_template, request, session
from flask_babel import get_locale

from echorepo.auth.decorators import login_required
from echorepo.i18n import BASE_LABEL_MSGIDS
from echorepo.services.i18n_labels import make_labels
from echorepo.services.manual_en import FIELDS, ManualConflict, change_manual_en, search_manual_en
from echorepo.services.i18n_overrides import (
    delete_override,
    delete_override_msgid,
    get_overrides,
    get_overrides_msgid,
    set_override,
    set_override_msgid,
)

bp = Blueprint("i18n_admin", __name__, url_prefix="/i18n")

# JS msgids — we hide these from the "Page texts" tab
JS_MSGIDS = set(BASE_LABEL_MSGIDS.values())


def _canon_locale(lang: str) -> str:
    if not lang:
        return "en"
    lang = lang.strip().lower().replace("-", "_")
    return lang.split("_", 1)[0]


def _get_catalog(loc: str) -> Translations | None:
    """Load compiled translations for a locale, or None."""
    trans_dir = os.path.join(current_app.root_path, "translations")
    try:
        # returns NullTranslations if not found; .gettext(msgid) => msgid
        return Translations.load(dirname=trans_dir, locales=[loc], domain="messages")
    except Exception:
        return None


def _catalog_gettext(loc: str, msgid: str) -> str:
    cat = _get_catalog(loc)
    if cat:
        try:
            return cat.gettext(msgid)
        except Exception:
            pass
    return msgid


def _load_pot_entries():
    """Read msgids + references from messages.pot if available."""
    pot = os.path.join(current_app.root_path, "translations", "messages.pot")
    try:
        import polib  # ensure polib is in requirements.txt

        if os.path.exists(pot):
            po = polib.pofile(pot)
            out = []
            for e in po:
                if not e.msgid:
                    continue
                refs = [f for (f, _lineno) in (e.occurrences or [])]
                out.append({"msgid": e.msgid, "refs": refs})
            return out
    except Exception:
        pass
    return []


# ---------- Manual EN overrides (for canonical *_en translations) ----------


def _manual_editor():
    profile = (session.get("kc") or {}).get("profile") or {}
    editors = {
        value.casefold()
        for value in re.split(
            r"[,;\s]+", os.getenv("I18N_ADMIN_EDITORS", os.getenv("COORDINATE_QA_EDITORS", ""))
        )
        if value
    }
    for value in (profile.get("email"), profile.get("preferred_username"), session.get("user")):
        identity = str(value or "").strip().casefold()
        if identity and identity in editors:
            return identity
    abort(403)


@bp.get("/labels.js")
def labels_js():
    loc_raw = request.args.get("locale") or str(get_locale() or "en")
    loc = _canon_locale(loc_raw)
    payload = {"labels": make_labels(loc)}
    js = "window.I18N = " + json.dumps(payload, ensure_ascii=False) + ";"
    resp = Response(js, mimetype="application/javascript; charset=utf-8")
    resp.headers["Cache-Control"] = "no-store"
    return resp


@bp.get("/admin")
@login_required
def admin_page():
    """
    scope:
      - 'page'      → template msgids
      - 'js'        → JS keys (BASE_LABEL_MSGIDS)
      - 'manual_en' → manual EN canonical overrides (text_to_en)
    """
    scope = request.args.get("scope", "page")
    if scope not in ("page", "js", "manual_en"):
        scope = "page"

    loc_raw = request.args.get("locale") or str(get_locale() or "en")
    loc = _canon_locale(loc_raw)
    q = (request.args.get("q") or "").strip().lower()
    file_filter = (request.args.get("file") or "").strip()

    if scope == "manual_en":
        _manual_editor()
        query = (request.args.get("q") or "").strip()
        if len(query) > 500:
            abort(400, "Search too long")
        page = request.args.get("page", 1, type=int)
        if not 1 <= page <= 1000:
            abort(400, "Invalid page")
        rows, has_next = [], False
        if query:
            rows, has_next = search_manual_en(query, page)
        if not session.get("_manual_en_csrf"):
            session["_manual_en_csrf"] = secrets.token_urlsafe(32)
        response = make_response(render_template(
            "i18n_admin.html",
            scope="manual_en",
            locale=loc,
            rows=rows,
            files_hint="",
            js_keys=False,
            page=page,
            has_next=has_next,
            query=query,
            csrf_token=session["_manual_en_csrf"],
        ))
        response.headers["Cache-Control"] = "private, no-store"
        return response

    # ----- Page texts (msgids) -----
    rows = []
    if scope == "page":
        entries = _load_pot_entries()
        seen = set()
        msg_over = get_overrides_msgid(loc)

        for e in entries:
            msgid = e["msgid"]

            # don't show strings that belong to the JS keys set
            if msgid in JS_MSGIDS:
                continue

            if msgid in seen:
                continue
            seen.add(msgid)

            refs = e.get("refs", [])
            if file_filter and not any(file_filter in r for r in refs):
                continue
            if q and q not in msgid.lower():
                continue

            catalog_txt = _catalog_gettext(loc, msgid)
            current_txt = msg_over.get(msgid, catalog_txt)

            rows.append(
                {
                    "msgid": msgid,
                    "current": current_txt,
                    "override": msg_over.get(msgid, ""),
                    "refs": refs,
                }
            )

        return render_template(
            "i18n_admin.html",
            scope="page",
            locale=loc,
            rows=rows,
            files_hint=file_filter,
            js_keys=False,
        )

    # ----- scope == 'js' (key-based) -----
    key_over = get_overrides(loc) or {}

    for k, msgid in BASE_LABEL_MSGIDS.items():
        catalog_txt = _catalog_gettext(loc, msgid)
        current_txt = key_over.get(k, catalog_txt)
        if q and not (q in k.lower() or q in str(current_txt).lower() or q in msgid.lower()):
            continue

        rows.append(
            {
                "key": k,
                "msgid": msgid,
                "current": current_txt,
                "override": key_over.get(k, ""),
            }
        )

    return render_template(
        "i18n_admin.html",
        scope="js",
        locale=loc,
        rows=rows,
        files_hint="",
        js_keys=True,
    )


@bp.post("/admin/set")
@login_required
def admin_set():
    data = request.get_json(silent=True) or request.form
    loc = _canon_locale(data.get("locale") or str(get_locale() or "en"))

    # key-based (JS labels)
    if (data.get("key") or "").strip():
        key = data["key"].strip()
        value = (data.get("value") or "").strip()
        if not key:
            abort(400, "key required")
        if value == "":
            delete_override(loc, key)
            return jsonify(ok=True, deleted=True)
        set_override(loc, key, value)
        return jsonify(ok=True)

    # msgid-based (page templates)
    if (data.get("msgid") or "").strip():
        msgid = data["msgid"].strip()
        value = (data.get("value") or "").strip()
        if not msgid:
            abort(400, "msgid required")
        if value == "":
            delete_override_msgid(loc, msgid)
            return jsonify(ok=True, deleted=True)
        set_override_msgid(loc, msgid, value)
        return jsonify(ok=True)

    abort(400, "key or msgid required")


@bp.post("/admin/manual_set")
@login_required
def manual_set():
    actor = _manual_editor()
    data = request.get_json(silent=True)
    if not isinstance(data, dict):
        return jsonify(ok=False, error="JSON required"), 400
    token = data.get("csrf_token")
    expected_token = session.get("_manual_en_csrf")
    if not isinstance(token, str) or not expected_token or not hmac.compare_digest(token, expected_token):
        return jsonify(ok=False, error="Invalid CSRF token"), 400
    field, country, original = (data.get(key) for key in ("field", "country", "orig"))
    value, expected = data.get("value"), data.get("expected")
    action = data.get("action")
    if (
        not isinstance(field, str) or field not in FIELDS
        or not isinstance(country, str) or len(country) > 16
        or not isinstance(original, str) or not original.strip() or len(original) > 100000
        or "expected" not in data or (expected is not None and not isinstance(expected, str))
        or action not in ("save", "clear")
        or (action == "save" and (not isinstance(value, str) or not value.strip() or len(value) > 100000))
    ):
        return jsonify(ok=False, error="Invalid correction"), 400
    try:
        result = change_manual_en(
            field, country, original, value.strip() if action == "save" else None,
            expected, actor, clear=action == "clear",
        )
    except ManualConflict as exc:
        return jsonify(ok=False, error=str(exc)), 409
    except Exception:
        current_app.logger.exception("Manual EN correction failed")
        return jsonify(ok=False, error="Could not save correction"), 500
    return jsonify(ok=True, **result)
