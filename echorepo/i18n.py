# echorepo/i18n.py
from flask import Blueprint, redirect, request, session, url_for, current_app
from flask_babel import Babel, _
from werkzeug.user_agent import UserAgent
import os

SUPPORTED_LOCALES = ["en","cs","nl","fi","fr","de","el","it","pl","pt","ro","sk","es"]

# language -> flag-icons country code
LOCALE_FLAGS = {
    "en":"gb","cs":"cz","nl":"nl","fi":"fi","fr":"fr",
    "de":"de","el":"gr","it":"it","pl":"pl","pt":"pt",
    "ro":"ro","sk":"sk","es":"es",
}

babel = Babel()

def _select_locale():
    # 1) explicit URL param
    q = request.args.get("lang")
    if q in SUPPORTED_LOCALES:
        return q
    # 2) session (set by /lang/<code>)
    s = session.get("lang")
    if s in SUPPORTED_LOCALES:
        return s
    # 3) Accept-Language
    return request.accept_languages.best_match(SUPPORTED_LOCALES) or "en"

def init_i18n(app):
    # IMPORTANT: point to the directory that actually contains your *.mo
    # Using an absolute path avoids surprises with CWD:
    app.config.setdefault("BABEL_DEFAULT_LOCALE", "en")
    trans_dir = os.path.join(app.root_path, "translations")
    app.config["BABEL_TRANSLATION_DIRECTORIES"] = trans_dir

    # Flask-Babel 3.x+: locale_selector=...
    # Flask-Babel 2.x: use the @babel.localeselector decorator
    try:
        babel.init_app(app, locale_selector=_select_locale)
    except TypeError:
        babel.init_app(app)

        @babel.localeselector  # type: ignore[attr-defined]
        def _legacy_localeselector():
            return _select_locale()

    # Debug logs each request (helpful while testing)
    @app.before_request
    def _dbg():
        # avoid heavy work; just log what Babel will use
        loc = _select_locale()
        current_app.logger.debug(
            "i18n: locale=%s session.lang=%r args.lang=%r dirs=%s",
            loc, session.get("lang"), request.args.get("lang"),
            app.config.get("BABEL_TRANSLATION_DIRECTORIES"),
        )

    @app.context_processor
    def inject_locale():
        # we compute current locale the same way to keep things consistent
        return {
            "current_locale": _select_locale(),
            "SUPPORTED_LOCALES": SUPPORTED_LOCALES,
            "LOCALE_FLAGS": LOCALE_FLAGS,
            "_": _,
        }

# blueprint to set session language
lang_bp = Blueprint("lang", __name__, url_prefix="/lang")

@lang_bp.route("/<lang_code>")
def set_language(lang_code):
    if lang_code in SUPPORTED_LOCALES:
        session["lang"] = lang_code
    return redirect(request.referrer or url_for("home"))
