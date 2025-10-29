# echorepo/routes/i18n_admin.py
from flask_babel import get_locale, gettext as _
from echorepo.auth.decorators import login_required
from echorepo.i18n import BASE_LABEL_MSGIDS, base_labels
from echorepo.services.i18n_overrides import (
    get_overrides, set_override, delete_override,
    get_overrides_msgid, set_override_msgid, delete_override_msgid,
)
import os

from flask import Blueprint, render_template, request, jsonify, abort, current_app, Response
import json

bp = Blueprint("i18n_admin", __name__, url_prefix="/i18n")

def _canon_locale(lang: str) -> str:
    if not lang:
        return "en"
    lang = lang.strip().lower().replace("-", "_")
    return lang.split("_", 1)[0]

def _make_labels(locale_code: str) -> dict:
    loc = _canon_locale(locale_code)
    by_msgid = get_overrides_msgid(loc) or {}
    by_key   = get_overrides(loc) or {}

    labels = {}
    for key, msgid in BASE_LABEL_MSGIDS.items():
        # gettext first, then msgid-override, then key-override (key wins last)
        text = _(msgid)
        text = by_msgid.get(msgid, text)
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
    # locale can come from querystring or current babel locale
    loc_raw = request.args.get("locale") or str(get_locale() or "en")
    labels = _make_labels(loc_raw)
    payload = {"labels": labels}

    js = "window.I18N = " + json.dumps(payload, ensure_ascii=False) + ";"
    resp = Response(js, mimetype="application/javascript; charset=utf-8")
    # No-cache so you see admin changes after reload (Shift+Reload in browser)
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
        # Template msgids from POT
        entries = _load_pot_entries()
        # Also include JS msgids (so you can fix map.js strings in the same tab)
        for key, msgid in BASE_LABEL_MSGIDS.items():
            entries.append({"msgid": msgid, "refs": ["static/js/map.js"]})

        msg_over = get_overrides_msgid(loc)

        for e in entries:
            msgid = e["msgid"]
            refs = e.get("refs", [])

            if file_filter and not any(file_filter in r for r in refs):
                continue
            if q and q not in msgid.lower():
                continue

            current_txt = _(msgid)  # current translation via Babel/Jinja `_`
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

    # scope == 'js' (key-based overrides)
    key_over = get_overrides(loc)     # { key: override }
    js_current = base_labels()        # { key: translated text via gettext }

    for k, cur in js_current.items():
        msgid = BASE_LABEL_MSGIDS.get(k, "")
        # basic filtering across key/current/msgid
        if q and not (q in k.lower() or q in str(cur).lower() or q in msgid.lower()):
            continue

        rows.append({
            "key": k,
            "msgid": msgid,
            "current": cur,
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
