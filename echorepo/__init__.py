# echorepo/__init__.py
import os
import json
from flask import Flask, jsonify, make_response
from flask_babel import get_locale, gettext as _real_gettext, ngettext as _real_ngettext

from .config import settings
from .auth.routes import auth_bp, init_oauth
from .routes.web import web_bp
from .routes.api import api_bp
from .routes.i18n_admin import bp as i18n_admin_bp
from .routes.errors import errors_bp
from .services.db import init_db_sanity
from .services.i18n_overrides import get_overrides, get_overrides_msgid
from .i18n import init_i18n, lang_bp, BASE_LABEL_MSGIDS


# ---------- helpers ----------
def _canon_locale(lang: str) -> str:
    if not lang:
        return "en"
    lang = lang.strip().lower().replace("-", "_")
    return lang.split("_", 1)[0]


def _default_flags(codes):
    base = {
        "en": "gb", "cs": "cz", "de": "de", "el": "gr", "es": "es", "fi": "fi",
        "fr": "fr", "it": "it", "nl": "nl", "pl": "pl", "pt": "pt", "ro": "ro", "sk": "sk",
    }
    out = dict(base)
    for c in codes:
        out.setdefault(c, "gb")
    return out


def _build_labels_for_locale(loc: str) -> dict:
    """
    Build the JS label dict for the current locale.
    Priority: key-override > msgid-override > gettext
    """
    loc = _canon_locale(loc or "en")
    by_msgid = get_overrides_msgid(loc) or {}
    by_key   = get_overrides(loc) or {}

    labels = {}
    for key, msgid in BASE_LABEL_MSGIDS.items():
        # prefer explicit key override for JS strings
        text = by_key.get(key)
        if text is None or text == "":
            text = by_msgid.get(msgid) or _real_gettext(msgid)
        labels[key] = text
    return labels


# ---------- create app ----------
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

        # Firebase / creds
        FIREBASE_PROJECT_ID=getattr(settings, "FIREBASE_PROJECT_ID", None),
        GOOGLE_APPLICATION_CREDENTIALS=getattr(settings, "GOOGLE_APPLICATION_CREDENTIALS", None),

        # Overrides storage path
        I18N_OVERRIDES_PATH=os.environ.get("I18N_OVERRIDES_PATH", "/data/i18n_overrides.json"),
    )

    # ---- i18n ----
    init_i18n(app)                 # set up Babel, locale selection, etc.
    app.register_blueprint(lang_bp)  # /set-lang/<code>

    # Supported locales & flags (from settings or defaults)
    SUPPORTED_LOCALES = getattr(
        settings, "SUPPORTED_LOCALES",
        ["en", "cs", "de", "el", "es", "fi", "fr", "it", "nl", "pl", "pt", "ro", "sk"]
    )
    LOCALE_FLAGS = getattr(settings, "LOCALE_FLAGS", _default_flags(SUPPORTED_LOCALES))

    # ---- Override-aware gettext for templates (incl. {% trans %}) ----
    def _gettext_with_overrides(msgid, **kwargs):
        loc = _canon_locale(str(get_locale() or "en"))
        ov = get_overrides_msgid(loc).get(msgid)
        text = ov if (ov is not None and ov != "") else _real_gettext(msgid)
        if kwargs:
            try:
                text = text % kwargs
            except Exception:
                pass
        return text

    def _ngettext_with_overrides(singular, plural, n, **kwargs):
        loc = _canon_locale(str(get_locale() or "en"))
        ov_s = get_overrides_msgid(loc).get(singular)
        ov_p = get_overrides_msgid(loc).get(plural)
        if n == 1 and ov_s not in (None, ""):
            text = ov_s
        elif n != 1 and ov_p not in (None, ""):
            text = ov_p
        else:
            text = _real_ngettext(singular, plural, n)
        if kwargs:
            try:
                text = text % kwargs
            except Exception:
                pass
        return text

    # Make Jinja use our override-aware functions everywhere
    app.jinja_env.globals.update(
        _=_gettext_with_overrides,
        gettext=_gettext_with_overrides,
        ngettext=_ngettext_with_overrides,
    )
    app.jinja_env.install_gettext_callables(
        _gettext_with_overrides, _ngettext_with_overrides, newstyle=True
    )

    # ---- Inject JS labels + locale into templates (for pages that need it) ----
    @app.context_processor
    def inject_i18n_and_locale():
        try:
            loc = _canon_locale(str(get_locale() or "en"))
        except Exception:
            loc = "en"

        # Build JS labels with both override layers
        labels = {}
        by_msgid = get_overrides_msgid(loc)  # {msgid: value}
        by_key   = get_overrides(loc)        # {key: value}
        for key, msgid in BASE_LABEL_MSGIDS.items():
            # default to catalog, then msgid override, then allow key to win for JS strings
            base = by_msgid.get(msgid) or _real_gettext(msgid)
            labels[key] = by_key.get(key, base)

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

    # ---- Back-compat endpoint aliases ----
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
    ]
    for ep, target, rule, methods in alias_map:
        app.add_url_rule(
            rule,
            endpoint=ep,
            view_func=app.view_functions[target],
            methods=methods,
        )

    # ---- DB sanity ----
    with app.app_context():
        init_db_sanity()

    # ---- i18n JSON/JS endpoints for the frontend ----
    @app.get("/i18n/labels.json")
    def i18n_labels_json():
        try:
            raw = str(get_locale() or "en")
        except Exception:
            raw = "en"
        labels = _build_labels_for_locale(raw)
        resp = jsonify({"labels": labels, "locale": _canon_locale(raw)})
        resp.headers["Cache-Control"] = "no-store"
        return resp

    @app.get("/i18n/labels.js")
    def i18n_labels_js():
        try:
            raw = str(get_locale() or "en")
        except Exception:
            raw = "en"
        labels = _build_labels_for_locale(raw)
        payload = "window.I18N = " + json.dumps({"labels": labels}, ensure_ascii=False) + ";"
        resp = make_response(payload, 200)
        resp.headers["Content-Type"] = "application/javascript; charset=utf-8"
        resp.headers["Cache-Control"] = "no-store"
        return resp

    # ---- Debugging endpoints ----
    @app.get("/i18n/debug")
    def i18n_debug():
        try:
            loc_raw = str(get_locale() or "en")
        except Exception:
            loc_raw = "en"
        loc = _canon_locale(loc_raw)
        labels = _build_labels_for_locale(loc)
        return jsonify({
            "locale_raw": loc_raw,
            "locale_canon": loc,
            "labels_count": len(labels),
            "labels_sample": {k: labels[k] for k in list(labels)[:10]},
            "has_privacyRadius": "privacyRadius" in labels,
            "privacyRadius": labels.get("privacyRadius"),
        })

    @app.get("/i18n/check-overrides")
    def i18n_check_overrides():
        loc = _canon_locale(str(get_locale() or "en"))
        return jsonify({
            "locale": loc,
            "by_key": get_overrides(loc),
            "by_msgid": get_overrides_msgid(loc),
        })

    # ---- No-cache for HTML ----
    @app.after_request
    def nocache_html(resp):
        if resp.mimetype == "text/html":
            resp.headers["Cache-Control"] = "no-store"
        return resp

    return app
