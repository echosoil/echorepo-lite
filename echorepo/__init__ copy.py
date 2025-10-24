import os
from flask import Flask
from .config import settings
from .auth.routes import auth_bp, init_oauth
from .routes.web import web_bp
from .routes.api import api_bp
from .services.db import init_db_sanity
from .routes.errors import errors_bp  # add import


def create_app() -> Flask:
    pkg_dir = os.path.dirname(__file__)
    app = Flask(
        __name__,
        template_folder=os.path.join(pkg_dir, "templates"),
        static_folder=os.path.join(pkg_dir, "..", "static"),  # <-- point to /app/static
        static_url_path="/static",                             # URL stays /static/...
    )
    app.secret_key = settings.SECRET_KEY
    app.config.update(
        SESSION_COOKIE_SAMESITE=settings.SESSION_COOKIE_SAMESITE,
        SESSION_COOKIE_SECURE=settings.SESSION_COOKIE_SECURE,
    )

    init_oauth(app)
    app.register_blueprint(auth_bp)
    app.register_blueprint(web_bp)
    app.register_blueprint(api_bp)
    app.register_blueprint(errors_bp)  # after auth/web/api

    # ---- Back-compat endpoint aliases (so url_for('home') still works) ----
    alias_map = [
        # web (you already have these)
        ("home",               "web.home",               "/",                  ["GET"]),
        ("download_csv",       "web.download_csv",       "/download/csv",      ["POST"]),
        ("download_xlsx",      "web.download_xlsx",      "/download/xlsx",     ["POST"]),
        ("download_all_csv",   "web.download_all_csv",   "/download/all_csv",  ["GET"]),

        # api (you already have these)
        ("user_geojson",       "api.user_geojson",       "/api/user_geojson",       ["GET"]),
        ("user_geojson_debug", "api.user_geojson_debug", "/api/user_geojson_debug", ["GET"]),
        ("others_geojson",     "api.others_geojson",     "/api/others_geojson",     ["GET"]),
        ("download_sample_csv","api.download_sample_csv","/download/sample_csv",    ["GET"]),

        # NEW: auth aliases so templates like url_for('sso_password_login') work
        ("login",              "auth.login",             "/login",             ["GET"]),
        ("sso_password_login", "auth.sso_password_login","/login",             ["POST"]),
        ("logout",             "auth.logout",            "/logout",            ["GET"]),
        ("sso_callback",       "auth.sso_callback",      "/sso/callback",      ["GET"]),
        # if you have a GET /sso/login route:
        # ("sso_login",          "auth.sso_login",         "/sso/login",         ["GET"]),
    ]
    for ep, target, rule, methods in alias_map:
        app.add_url_rule(rule, endpoint=ep,
                         view_func=app.view_functions[target],
                         methods=methods)
        
    with app.app_context():
        init_db_sanity()
    return app
