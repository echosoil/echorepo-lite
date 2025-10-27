# echorepo/i18n.py
from flask import Blueprint, redirect, request, session, url_for
from flask_babel import Babel, _

SUPPORTED_LOCALES = ["en","cs","nl","fi","fr","de","el","it","pl","pt","ro","sk","es"]

# map language -> country code used by flag-icons
LOCALE_FLAGS = {
    "en": "gb",  # or "us" if you prefer
    "cs": "cz",
    "nl": "nl",
    "fi": "fi",
    "fr": "fr",
    "de": "de",
    "el": "gr",
    "it": "it",
    "pl": "pl",
    "pt": "pt",  # use "pt-br" / "br" if you later add Brazilian Portuguese
    "ro": "ro",
    "sk": "sk",
    "es": "es",
}

babel = Babel()  # <-- create it here

def get_locale():
    q = request.args.get("lang")
    if q in SUPPORTED_LOCALES:
        return q
    s = session.get("lang")
    if s in SUPPORTED_LOCALES:
        return s
    return request.accept_languages.best_match(SUPPORTED_LOCALES) or "en"

def init_i18n(app):
    app.config.setdefault("BABEL_DEFAULT_LOCALE", "en")
    app.config.setdefault("BABEL_TRANSLATION_DIRECTORIES", "translations;echorepo/translations")
    # Flask-Babel 3+ API:
    babel.init_app(app, locale_selector=get_locale)

    @app.context_processor
    def inject_locale():
        return {
            "current_locale": get_locale(),
            "SUPPORTED_LOCALES": SUPPORTED_LOCALES,
            "LOCALE_FLAGS": LOCALE_FLAGS,
            "_": _,
        }

lang_bp = Blueprint("lang", __name__, url_prefix="/lang")

@lang_bp.route("/<lang_code>")
def set_language(lang_code):
    if lang_code in SUPPORTED_LOCALES:
        session["lang"] = lang_code
    return redirect(request.referrer or url_for("home"))
