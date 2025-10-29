# -*- coding: utf-8 -*-
from flask import Blueprint, render_template, request, jsonify, abort
from flask_babel import get_locale
from echorepo.services.i18n_overrides import get_overrides, set_override, delete_override
# Use your own auth decorators:
from echorepo.auth.decorators import login_required  # or require_auth
# If you have role-based admin, use admin_required instead of login_required.

bp = Blueprint("i18n_admin", __name__, url_prefix="/i18n")

@bp.get("/admin")
@login_required
def admin_page():
    # Available locales (adjust to your set)
    locales = ["en","cs","de","el","es","fi","fr","it","nl","pl","pt","ro","sk"]
    loc = request.args.get("locale") or str(get_locale() or "en")
    if loc not in locales:
        loc = "en"
    overrides = get_overrides(loc)
    q = (request.args.get("q") or "").strip().lower()

    # If you have a canonical list of UI keys, use it. Otherwise show only overrides.
    # Example keys we know from the map & UI:
    known_keys = [
        "privacyRadius","soilPh","acid","slightlyAcid","neutral","slightlyAlkaline","alkaline",
        "yourSamples","otherSamples","export","clear","exportFiltered","date","qr","ph",
        "colour","texture","structure","earthworms","plastic","debris","contamination","metals"
    ]
    # de-duplicate + ensure all overridden keys are present
    keys = sorted(set(known_keys) | set(overrides.keys()))
    if q:
        keys = [k for k in keys if q in k.lower() or q in (overrides.get(k,"").lower())]

    rows = [{"key": k, "value": overrides.get(k, "")} for k in keys]
    return render_template("i18n_admin.html", locale=loc, locales=locales, rows=rows, q=q)

@bp.post("/admin/set")
@login_required
def admin_set():
    data = request.get_json(silent=True) or request.form
    locale = (data.get("locale") or "").strip()
    key    = (data.get("key") or "").strip()
    value  = (data.get("value") or "").strip()
    if not locale or not key:
        abort(400, "locale and key are required")
    if value == "":
        delete_override(locale, key)
        return jsonify(ok=True, deleted=True)
    set_override(locale, key, value)
    return jsonify(ok=True)

