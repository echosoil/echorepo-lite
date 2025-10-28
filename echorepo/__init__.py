import os
from flask import Flask
from .config import settings
from .auth.routes import auth_bp, init_oauth
from .routes.web import web_bp
from .routes.api import api_bp
from .services.db import init_db_sanity
from .routes.errors import errors_bp  # add import
from .i18n import init_i18n, lang_bp

def create_app() -> Flask:
    pkg_dir = os.path.dirname(__file__)
    app = Flask(
        __name__,
        template_folder=os.path.join(pkg_dir, "templates"),
        static_folder=os.path.join(pkg_dir, "..", "static"),  # -> /app/static
        static_url_path="/static",
    )

    # Base config
    app.secret_key = settings.SECRET_KEY
    app.config.from_mapping(SECRET_KEY=os.environ.get("SECRET_KEY","dev-secret"))

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

    init_i18n(app)               # <-- enable Babel
    app.register_blueprint(lang_bp)  # <-- /set-lang/<code>

    # Extensions & OAuth
    init_oauth(app)

    # Blueprints
    app.register_blueprint(auth_bp)
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

        # auth aliases so templates like url_for('sso_password_login') work
        ("login",              "auth.login",              "/login",        ["GET"]),
        ("sso_password_login", "auth.sso_password_login", "/login",        ["POST"]),
        ("logout",             "auth.logout",             "/logout",       ["GET"]),
        ("sso_callback",       "auth.sso_callback",       "/sso/callback", ["GET"]),
        # If you also expose GET /sso/login:
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
