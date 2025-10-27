# echorepo/i18n.py
from datetime import timedelta
from flask import Blueprint, redirect, request, session, url_for, current_app
from flask_babel import Babel, _

SUPPORTED_LOCALES = ["en","cs","nl","fi","fr","de","el","it","pl","pt","ro","sk","es"]

# map language -> country code for flag-icons
LOCALE_FLAGS = {
    "en": "gb", "cs": "cz", "nl": "nl", "fi": "fi", "fr": "fr",
    "de": "de", "el": "gr", "it": "it", "pl": "pl", "pt": "pt",
    "ro": "ro", "sk": "sk", "es": "es",
}

babel = Babel()

def get_locale():
    # 1) explicit ?lang=xx (no session write here; we only *read*)
    q = request.args.get("lang")
    if q in SUPPORTED_LOCALES:
        return q
    # 2) session
    s = session.get("lang")
    if s in SUPPORTED_LOCALES:
        return s
    # 3) browser
    return request.accept_languages.best_match(SUPPORTED_LOCALES) or "en"

def init_i18n(app):
    # default locale + both translation roots
    app.config.setdefault("BABEL_DEFAULT_LOCALE", "en")
    app.config.setdefault("BABEL_TRANSLATION_DIRECTORIES", "translations;echorepo/translations")
    app.config.setdefault("BABEL_DEFAULT_TIMEZONE", "UTC")

    # make session cookies persist (so language survives restarts)
    app.permanent_session_lifetime = timedelta(days=365)

    # init Babel (Flask-Babel 3.x API)
    babel.init_app(app, locale_selector=get_locale)

    @app.context_processor
    def inject_locale():
        # expose helpers to templates
        return {
            "current_locale": get_locale(),
            "SUPPORTED_LOCALES": SUPPORTED_LOCALES,
            "LOCALE_FLAGS": LOCALE_FLAGS,
            "_": _,
        }

lang_bp = Blueprint("lang", __name__, url_prefix="/lang")

@lang_bp.route("/<lang_code>")
def set_language(lang_code: str):
    if lang_code in SUPPORTED_LOCALES:
        session["lang"] = lang_code
        session.permanent = True  # persist for a year (see lifetime above)
    return redirect(request.referrer or url_for("home"))
