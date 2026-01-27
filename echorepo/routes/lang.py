from flask import Blueprint, redirect, request, url_for

bp = Blueprint("lang", __name__)


@bp.get("/set-lang/<lang_code>")
def set_language(lang_code):
    from echorepo.i18n import SUPPORTED_LOCALES

    if lang_code not in SUPPORTED_LOCALES:
        lang_code = "en"
    ref = request.headers.get("Referer") or url_for("root.index")
    resp = redirect(ref)
    # store for ~2 years
    resp.set_cookie(
        "locale",
        lang_code,
        max_age=60 * 60 * 24 * 730,
        samesite="Lax",
        path="/",
    )
    return resp
