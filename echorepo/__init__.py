import os
from flask import Flask
from flask_babel import get_locale
from .config import settings
from .auth.routes import auth_bp, init_oauth
from .routes.web import web_bp
from .routes.api import api_bp
from .services.db import init_db_sanity
from .routes.errors import errors_bp
from .i18n import init_i18n, lang_bp, build_i18n_labels  # <- use centralized builder
from echorepo.routes.i18n_admin import bp as i18n_admin_bp

# Optional: base labels here or import from a helper if you split it
from flask_babel import gettext as _
def _base_labels() -> dict:
    return {
        "privacyRadius": _("Privacy radius (~±{km} km)"),
        "soilPh": _("Soil pH"),
        "acid": _("Acidic (≤5.5)"),
        "slightlyAcid": _("Slightly acidic (5.5–6.5)"),
        "neutral": _("Neutral (6.5–7.5)"),
        "slightlyAlkaline": _("Slightly alkaline (7.5–8.5)"),
        "alkaline": _("Alkaline (≥8.5)"),
        "yourSamples": _("Your samples"),
        "otherSamples": _("Other samples"),
        "export": _("Export"),
        "clear": _("Clear"),
        "exportFiltered": _("Export filtered ({n})"),
        "date": _("Date"),
        "qr": _("QR code"),
        "ph": _("pH"),
        "colour": _("Colour"),
        "texture": _("Texture"),
        "structure": _("Structure"),
        "earthworms": _("Earthworms"),
        "plastic": _("Plastic"),
        "debris": _("Debris"),
        "contamination": _("Contamination"),
        "metals": _("Metals"),
    }

def _default_flags(codes):
    base = {
        "en": "gb", "cs": "cz", "de": "de", "el": "gr", "es": "es", "fi": "fi",
        "fr": "fr", "it": "it", "nl": "nl", "pl": "pl", "pt": "pt", "ro": "ro", "sk": "sk",
    }
    out = dict(base)
    for c in codes:
        out.setdefault(c, "gb")
    return out

def create_app() -> Flask:
    pkg_dir = os.path.dirname(__file__)
    app = Flask(
        __name__,
        template_folder=os.path.join(pkg_dir, "templates"),
        static_folder=os.path.join(pkg_dir, "..", "static"),
        static_url_path="/static",
    )

    # Base config
    app.secret_key = settings.SECRET_KEY
    app.config.from_mapping(SECRET_KEY=os.environ.get("SECRET_KEY", "dev-secret"))

    app.config.update(
        SESSION_COOKIE_SAMESITE=settings.SESSION_COOKIE_SAMESITE,
        SESSION_COOKIE_SECURE=settings.SESSION_COOKIE_SECURE,
        # General settings
        LAT_COL=getattr(settings, "LAT_COL", "GPS_lat"),
        LON_COL=getattr(settings, "LON_COL", "GPS_long"),
        USER_KEY_COLUMN=getattr(settings, "USER_KEY_COLUMN", "email"),
        INPUT_CSV=getattr(settings, "INPUT_CSV", ""),
        SQLITE_PATH=getattr(settings, "SQLITE_PATH", ""),
        PLANNED_XLSX=getattr(settings, "PLANNED_XLSX", ""),
        ORIG_COL_SUFFIX=getattr(settings, "ORIG_COL_SUFFIX", "_orig"),
        HIDE_ORIG_COLS=getattr(settings, "HIDE_ORIG_COLS", True),
        MAX_JITTER_METERS=getattr(settings, "MAX_JITTER_METERS", 1000),
        # Firebase settings
        FIREBASE_PROJECT_ID=getattr(settings, "FIREBASE_PROJECT_ID", None),
        GOOGLE_APPLICATION_CREDENTIALS=getattr(settings, "GOOGLE_APPLICATION_CREDENTIALS", None),
    )

    # ---- i18n ----
    init_i18n(app)          # sets up Babel, locale selection, etc.
    app.register_blueprint(lang_bp)   # /set-lang/<code>

    # Supported locales & flags (from settings or defaults)
    SUPPORTED_LOCALES = getattr(
        settings, "SUPPORTED_LOCALES",
        ["en", "cs", "de", "el", "es", "fi", "fr", "it", "nl", "pl", "pt", "ro", "sk"]
    )
    LOCALE_FLAGS = getattr(settings, "LOCALE_FLAGS", _default_flags(SUPPORTED_LOCALES))

    # ---- Global template context: I18N + locale info ----
    @app.context_processor
    def inject_i18n_and_locale():
        try:
            labels = build_i18n_labels(_base_labels())  # merges DB overrides on top
        except Exception:
            labels = {}
        try:
            loc = str(get_locale() or "en")
        except Exception:
            loc = "en"
        return {
            "I18N": {"labels": labels},
            "current_locale": loc,
            "SUPPORTED_LOCALES": SUPPORTED_LOCALES,
            "LOCALE_FLAGS": LOCALE_FLAGS,
        }

    # ---- OAuth / Blueprints ----
    init_oauth(app)

    app.register_blueprint(auth_bp)
    app.register_blueprint(i18n_admin_bp)  # /i18n/admin
    app.register_blueprint(web_bp)
    app.register_blueprint(api_bp)
    app.register_blueprint(errors_bp)

    # ---- Back-compat endpoint aliases (so url_for('home') still works) ----
    alias_map = [
        # web
        ("home",               "web.home",               "/",                  ["GET"]),
        ("download_csv",       "web.download_csv",       "/download/csv",      ["POST"]),
        ("download_xlsx",      "web.download_xlsx",      "/download/xlsx",     ["POST"]),
        ("download_all_csv",   "web.download_all_csv",   "/download/all_csv",  ["GET"]),

        # api
        ("user_geojson",       "api.user_geojson",       "/api/user_geojson",       ["GET"]),
        ("user_geojson_debug", "api.user_geojson_debug", "/api/user_geojson_debug", ["GET"]),
        ("others_geojson",     "api.others_geojson",     "/api/others_geojson",     ["GET"]),
        ("download_sample_csv","api.download_sample_csv","/download/sample_csv",    ["GET"]),

        # auth
        ("login",              "auth.login",              "/login",        ["GET"]),
        ("sso_password_login", "auth.sso_password_login", "/login",        ["POST"]),
        ("logout",             "auth.logout",             "/logout",       ["GET"]),
        ("sso_callback",       "auth.sso_callback",       "/sso/callback", ["GET"]),
        # ("sso_login",          "auth.sso_login",          "/sso/login",    ["GET"]),
    ]
    for ep, target, rule, methods in alias_map:
        app.add_url_rule(
            rule,
            endpoint=ep,
            view_func=app.view_functions[target],
            methods=methods,
        )

    with app.app_context():
        init_db_sanity()

    return app
