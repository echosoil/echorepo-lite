# echorepo/routes/i18n_admin.py
from flask_babel import get_locale, gettext as _
from echorepo.auth.decorators import login_required
from echorepo.i18n import BASE_LABEL_MSGIDS
from echorepo.services.i18n_overrides import (
    get_overrides, set_override, delete_override,
    get_overrides_msgid, set_override_msgid, delete_override_msgid,
)
import os, json
from babel.support import Translations

from flask import Blueprint, render_template, request, jsonify, abort, current_app, Response

bp = Blueprint("i18n_admin", __name__, url_prefix="/i18n")

def _canon_locale(lang: str) -> str:
    if not lang:
        return "en"
    lang = lang.strip().lower().replace("-", "_")
    return lang.split("_", 1)[0]

# cache loaded catalogs per request
def _get_catalog(loc: str) -> Translations | None:
    trans_dir = os.path.join(current_app.root_path, "translations")
    try:
        # returns NullTranslations if not found; good enough (.gettext returns msgid)
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

def _make_labels(locale_code: str) -> dict:
    loc = _canon_locale(locale_code)
    by_msgid = get_overrides_msgid(loc) or {}
    by_key   = get_overrides(loc) or {}

    labels = {}
    for key, msgid in BASE_LABEL_MSGIDS.items():
        # catalog for selected locale
        text = _catalog_gettext(loc, msgid)
        # then msgid-override
        text = by_msgid.get(msgid, text)
        # then key override (wins last for JS)
        text = by_key.get(key, text)
        labels[key] = text
    return labels

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

@bp.get("/labels.js")
def labels_js():
    loc_raw = request.args.get("locale") or str(get_locale() or "n")
    loc = _canon_locale(loc_raw)
    payload = {"labels": _make_labels(loc)}
    js = "window.I18N = " + json.dumps(payload, ensure_ascii=False) + ";"
    resp = Response(js, mimetype="application/javascript; charset=utf-8")
    resp.headers["Cache-Control"] = "no-store"
    return resp

@bp.get("/admin")
@login_required
def admin_page():
    # scope: 'page' (msgids) or 'js' (keys)
    scope = request.args.get("scope", "page")
    loc_raw = request.args.get("locale") or str(get_locale() or "en")
    loc = _canon_locale(loc_raw)
    q = (request.args.get("q") or "").strip().lower()
    file_filter = (request.args.get("file") or "").strip()

    rows = []

    if scope == "page":
        entries = _load_pot_entries()
        # Deduplicate just in case messages.pot has repeats
        seen = set()

        msg_over = get_overrides_msgid(loc)

        for e in entries:
            msgid = e["msgid"]
            if msgid in seen:
                continue
            seen.add(msgid)
            refs = e.get("refs", [])

            if file_filter and not any(file_filter in r for r in refs):
                continue
            if q and q not in msgid.lower():
                continue

            # IMPORTANT: translate using the SELECTED locale, not the request locale
            catalog_txt = _catalog_gettext(loc, msgid)
            current_txt = msg_over.get(msgid, catalog_txt)

            rows.append({
                "msgid": msgid,
                "current": current_txt,
                "override": msg_over.get(msgid, ""),
                "refs": refs,
            })

        return render_template(
            "i18n_admin.html",
            scope="page",
            locale=loc,
            rows=rows,
            files_hint=file_filter,
            js_keys=False,
        )

    # scope == 'js' (key-based)
    key_over = get_overrides(loc) or {}

    for k, msgid in BASE_LABEL_MSGIDS.items():
        catalog_txt = _catalog_gettext(loc, msgid)
        current_txt = key_over.get(k, catalog_txt)
        if q and not (q in k.lower() or q in str(current_txt).lower() or q in msgid.lower()):
            continue

        rows.append({
            "key": k,
            "msgid": msgid,
            "current": current_txt,
            "override": key_over.get(k, ""),
        })

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
