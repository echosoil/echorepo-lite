# echorepo/routes/i18n_admin.py
import json
import os
from pathlib import Path

from babel.support import Translations
from flask import Blueprint, Response, abort, current_app, jsonify, render_template, request
from flask_babel import get_locale

from echorepo.auth.decorators import login_required
from echorepo.i18n import BASE_LABEL_MSGIDS
from echorepo.services.i18n_labels import make_labels
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


def _manual_overrides_path() -> Path:
    """
    Single source of truth for manual_en overrides file.

    By default we keep it in /data/manual_overrides.json, which is
    backed by ./data on the host (see docker-compose).
    You can override via MANUAL_OVERRIDES_PATH if needed.
    """
    path = os.getenv("MANUAL_OVERRIDES_PATH", "/data/manual_overrides.json")
    return Path(path)


def _load_manual_overrides() -> dict:
    """
    Returns dict: { "text_to_en": {orig_text: en_text, ... } }
    """
    path = _manual_overrides_path()
    if not path.exists():
        return {"text_to_en": {}}

    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        current_app.logger.exception("Failed to read manual_overrides.json")
        return {"text_to_en": {}}

    if not isinstance(data, dict):
        data = {}
    if not isinstance(data.get("text_to_en"), dict):
        data["text_to_en"] = {}
    return data


def _save_manual_overrides(text_to_en: dict) -> None:
    path = _manual_overrides_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {"text_to_en": text_to_en}
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


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

    # ----- NEW: manual_en tab -----
    if scope == "manual_en":
        data = _load_manual_overrides()
        mapping = data.get("text_to_en", {})
        rows = []
        for orig, en_val in mapping.items():
            orig_str = str(orig)
            en_str = "" if en_val is None else str(en_val)
            if q and not (q in orig_str.lower() or q in en_str.lower()):
                continue
            rows.append(
                {
                    "orig": orig_str,
                    "override": en_str,
                }
            )
        rows.sort(key=lambda r: r["orig"].lower())

        return render_template(
            "i18n_admin.html",
            scope="manual_en",
            locale=loc,
            rows=rows,
            files_hint=file_filter,
            js_keys=False,
        )

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
    """
    Save / clear a single manual EN override.
    Body: { "orig": "...", "value": "..." }
      - if value == "" → delete entry
      - else           → set orig → value
    """
    data = request.get_json(silent=True) or request.form
    orig = (data.get("orig") or "").strip()
    if not orig:
        return jsonify({"ok": False, "error": "orig required"}), 400

    value = (data.get("value") or "").strip()

    data_obj = _load_manual_overrides()
    mapping = data_obj.get("text_to_en", {})
    if not isinstance(mapping, dict):
        mapping = {}

    if value == "":
        # delete
        mapping.pop(orig, None)
    else:
        mapping[orig] = value

    try:
        _save_manual_overrides(mapping)
    except Exception as e:
        current_app.logger.exception("Failed to save manual overrides")
        return jsonify({"ok": False, "error": str(e)}), 500

    return jsonify({"ok": True, "orig": orig, "value": value})
