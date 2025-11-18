# echorepo/__init__.py
import os
import json
from flask import Flask, jsonify, make_response, request, render_template_string, current_app, g, session
from flask_babel import get_locale, gettext as _real_gettext, ngettext as _real_ngettext
import time 
import re 

from .config import settings
from .auth.routes import auth_bp, init_oauth
from .routes.web import web_bp
from .routes.api import api_bp
from .routes.i18n_admin import bp as i18n_admin_bp
from .routes.errors import errors_bp
from .routes import data_api

from .services.db import init_db_sanity
from .services.i18n_overrides import get_overrides, get_overrides_msgid
from .i18n import init_i18n, lang_bp, BASE_LABEL_MSGIDS
from .analytics import hash_ip, log_usage_event
import logging, time

# ---------- Logging ----------
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# only add the file handler once
if not any(isinstance(h, logging.FileHandler) for h in logger.handlers):
    os.makedirs("/tmp", exist_ok=True)
    fh = logging.FileHandler("/tmp/echorepo-slow.log")
    fh.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    fh.setFormatter(fmt)
    logger.addHandler(fh)

# ---------- helpers ----------
from flask import session

def _get_current_user_id():
    """
    Extract a stable user identifier from the session.

    For your setup, `session["user"]` is already the email ("oleg.osychenko@gmail.com"),
    so we use that first. As a fallback, we peek into `session["kc"]["profile"]`.
    """
    # 1) Primary source: session["user"] (string email)
    u = session.get("user")
    if isinstance(u, str) and u.strip():
        return u.strip()

    # 2) Fallback: look inside Keycloak info (session["kc"])
    kc = session.get("kc")
    if isinstance(kc, dict):
        profile = kc.get("profile")

        # If your auth stores it as a dict
        if isinstance(profile, dict):
            return (
                profile.get("email")
                or profile.get("preferred_username")
                or profile.get("name")
            )

        # If it's a stringified dict (as it looks in the snapshot),
        # try to regex out the email: "{'email': '...'}"
        if isinstance(profile, str):
            m = re.search(r"'email'\s*:\s*'([^']+)'", profile)
            if m:
                return m.group(1)

    # If nothing found, treat as anonymous
    return None

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

    # ---- Analytics configuration ----
    # Paths we consider "noise" (don't log them at all)
    ANALYTICS_EXCLUDED_PREFIXES = (
        "/static/",       # CSS, JS, images served as static files
        "/i18n/",         # labels.js / labels.json etc.
        "/debug/",        # debug helpers
        "/favicon.ico",
        "/robots.txt",
    )

    # Paths we consider *real* downloads (user-triggered data exports)
    ESSENTIAL_DOWNLOAD_PATHS = (
        "/download/canonical/all.zip",
        "/download/canonical/samples.csv",
        "/download/canonical/sample_images.csv",
        "/download/canonical/sample_parameters.csv",
        "/download/sample_csv",   # old alias
        "/download/all_csv",      # old alias
    )

    # ---- Request timing logging, see /tmp/echorepo-slow.log ----
    @app.before_request
    def _start_timer():
        request._t0 = time.time()

    @app.after_request
    def _log_time(response):
        t0 = getattr(request, "_t0", None)
        if t0 is not None:
            dt = (time.time() - t0) * 1000
            if dt > 100:  # only log slow ones
                current_app.logger.info("SLOW %s %s %.1f ms", request.method, request.path, dt)
                logger.warning("SLOW %s %s %.1f ms", request.method, request.path, dt)
        return response

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
        LOCAL_STORAGE_DIR=os.environ.get("LOCAL_STORAGE_DIR", "/data/storage"),
        
        # Expose allowlist path to current_app.config
        LAB_UPLOAD_ALLOWLIST_PATH=getattr(
            settings,
            "LAB_UPLOAD_ALLOWLIST_PATH",
            "/data/config/lab_upload_lab_allowlist.csv",
        ),
    )

    # ---- i18n ----
    init_i18n(app)                 # set up Babel, locale selection, etc.
    if 'jinja2.ext.i18n' not in app.jinja_env.extensions:
        app.jinja_env.add_extension('jinja2.ext.i18n')
    app.register_blueprint(lang_bp)  # /set-lang/<code>

    # Supported locales & flags (from settings or defaults)
    SUPPORTED_LOCALES = getattr(
        settings, "SUPPORTED_LOCALES",
        ["en", "cs", "de", "el", "es", "fi", "fr", "it", "nl", "pl", "pt", "ro", "sk"]
    )
    LOCALE_FLAGS = getattr(settings, "LOCALE_FLAGS", _default_flags(SUPPORTED_LOCALES))

    # ---- Override-aware gettext for templates (incl. {% trans %}) ----
    def _gettext_with_overrides(msgid, **kwargs):
        try:
            loc = _canon_locale(str(get_locale() or "en"))
        except Exception:
            loc = "en"
        ov = get_overrides_msgid(loc).get(msgid)
        if ov not in (None, ""):
            try:
                return ov % kwargs if kwargs else ov
            except Exception:
                return ov
        return _real_gettext(msgid, **kwargs)

    def _ngettext_with_overrides(singular, plural, n, **kwargs):
        try:
            loc = _canon_locale(str(get_locale() or "en"))
        except Exception:
            loc = "en"
        ov = get_overrides_msgid(loc).get(singular)
        if ov not in (None, ""):
            try:
                return ov % kwargs if kwargs else ov
            except Exception:
                return ov
        # Fall back to Babel plural handling
        return _real_ngettext(singular, plural, n, **kwargs)


    def _install_callables():
        # Install override-aware gettext/ngettext into the Jinja env
        app.jinja_env.install_gettext_callables(
            _gettext_with_overrides,
            _ngettext_with_overrides,
            newstyle=True,
        )
        # Also expose names that templates might use directly
        app.jinja_env.globals["_"] = _gettext_with_overrides
        app.jinja_env.globals["gettext"] = _gettext_with_overrides
        app.jinja_env.globals["ngettext"] = _ngettext_with_overrides

    # Install once now…
    _install_callables()

    # …and also re-install on every request in case something rebinds them later.
    @app.before_request
    def _force_i18n_install_every_time():
        _install_callables()

    # Make Jinja use our override-aware functions everywhere
    app.jinja_env.install_gettext_callables(
    _gettext_with_overrides,
    _ngettext_with_overrides,
    newstyle=True,   # enables %(name)s formatting
    )

    app.jinja_env.globals["_"] = _gettext_with_overrides

    # ✅ Ensure our binding persists *after* Babel hooks per-request
    @app.before_request
    def _rebind_gettext_per_request():
        app.jinja_env.install_gettext_callables(
            _gettext_with_overrides, _ngettext_with_overrides, newstyle=True
        )
        app.jinja_env.globals["_"] = _gettext_with_overrides
        
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
            base = by_msgid.get(msgid) or _real_gettext(msgid)
            labels[key] = by_key.get(key, base)

        # ⬇️ ADD THESE THREE KEYS to override Babel's context-level bindings
        return {
            "I18N": {"labels": labels},
            "current_locale": loc,
            "SUPPORTED_LOCALES": SUPPORTED_LOCALES,
            "LOCALE_FLAGS": LOCALE_FLAGS,
            "_": _gettext_with_overrides,          # <—
            "gettext": _gettext_with_overrides,    # <—
            "ngettext": _ngettext_with_overrides,  # <—
        }
    
    # ---- OAuth / Blueprints ----
    init_oauth(app)
    app.register_blueprint(auth_bp)
    app.register_blueprint(i18n_admin_bp)  # /i18n/admin
    app.register_blueprint(web_bp)
    app.register_blueprint(api_bp)
    app.register_blueprint(errors_bp)
    app.register_blueprint(data_api.data_api, url_prefix="/api/v1")  # or url_prefix="/api"
    from echorepo.routes.storage import storage_bp
    app.register_blueprint(storage_bp)               # ← add this

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
    
    @app.get("/i18n/probe-json")
    def i18n_probe_json():
        s = request.args.get("s", "About")
        loc = _canon_locale(str(get_locale() or "en"))
        return jsonify({
            "locale": loc,
            "override": get_overrides_msgid(loc).get(s),
            "gettext_with_overrides": _gettext_with_overrides(s),
            "babel_gettext": _real_gettext(s),
        })

    @app.get("/i18n/probe-tpl")
    def i18n_probe_tpl():
        s = request.args.get("s", "About")
        # Render via Jinja so we test what templates *actually* call.
        out = render_template_string("{{ _('"+s.replace('\"','\\\"')+"') }}")
        return out

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

    # ---- No-cache for HTML ----
    @app.after_request
    def nocache_html(resp):
        if resp.mimetype == "text/html":
            resp.headers["Cache-Control"] = "no-store"
        return resp

    @app.before_request
    def _start_timer():
        g._start_time = time.time()

    @app.after_request
    def _log_usage(response):
        try:
            start = getattr(g, "_start_time", None)
            duration_ms = int((time.time() - start) * 1000) if start is not None else None

            path = request.path

            # ---- 1) Skip noisy / internal endpoints completely ----
            if any(path.startswith(p) for p in ANALYTICS_EXCLUDED_PREFIXES):
                return response

            # If you also want to skip *all* /storage/* (images proxied from MinIO),
            # uncomment this:
            if path.startswith("/storage/"):
                return response
            # --------------------------------------------------------

            # ---- 2) Identify user ----
            user_id = _get_current_user_id()

            # ---- 3) Classify event type (only "essential" ones) ----
            if path in ESSENTIAL_DOWNLOAD_PATHS:
                event_type = "download"
            elif path.startswith("/api/"):
                event_type = "api_call"
            else:
                # Treat everything else as a "page view" (HTML pages)
                event_type = "page_view"

            # ---- 4) Collect other fields ----
            ip = request.headers.get("X-Forwarded-For", request.remote_addr)
            ip_h = hash_ip(ip)

            ua = request.headers.get("User-Agent", "")
            method = request.method
            status = response.status_code
            bytes_sent = response.calculate_content_length() or 0

            extra = getattr(g, "_analytics_extra", {}) or {}
            # ---- 1) classify event_type ----
            if path in ESSENTIAL_DOWNLOAD_PATHS:
                event_type = "download"
                extra.setdefault("file", path)

            elif path == "/search":
                event_type = "search"

            elif path.startswith("/lab-upload") or path.startswith("/lab-enrichment"):
                # web: /lab-upload
                # api: /api/v1/lab-enrichment
                event_type = "upload"

            elif path.startswith("/api/"):
                event_type = "api_call"

            else:
                event_type = "page_view"

            # ---- 2) add generic extras per type ----

            if event_type == "search":
                # try to capture search query safely
                q = request.args.get("q") or request.values.get("q")
                if q:
                    extra.setdefault("search_query", q[:200])  # avoid huge blobs

            elif event_type == "upload":
                # generic info; more detail can come from g._analytics_extra in the view
                extra.setdefault("upload_path", path)
                extra.setdefault("upload_method", method)

            elif event_type == "api_call":
                extra.setdefault("api_endpoint", path)
                extra.setdefault("api_method", method)
                # Optional: just log which query keys are used (not full values)
                if request.args:
                    extra.setdefault("api_query_keys", sorted(request.args.keys()))

            log_usage_event(
                user_id=user_id,
                event_type=event_type,
                path=path,
                method=method,
                status_code=status,
                bytes_sent=bytes_sent,
                duration_ms=duration_ms,
                ip_hash=ip_h,
                user_agent=ua,
                extra=extra,
            )
        except Exception as e:
            # never break the user request because analytics failed
            app.logger.warning("usage logging failed: %s", e)

        return response

    # ----- Debug routes --------------------------------------------    
    @app.get("/debug/whoami")
    def debug_whoami():
        # WARNING: remove or restrict this in production
        from flask import jsonify

        # snapshot of session (but shortened so we don't print full tokens)
        sess_snapshot = {}
        for k in list(session.keys()):
            v = session.get(k)
            if isinstance(v, (str, bytes)):
                s = v.decode("utf-8", errors="ignore") if isinstance(v, bytes) else v
                if len(s) > 120:
                    sess_snapshot[k] = s[:60] + "... (len=" + str(len(s)) + ")"
                else:
                    sess_snapshot[k] = s
            elif isinstance(v, dict):
                # show only first few keys, with repr of values
                small = {}
                for kk, vv in list(v.items())[:10]:
                    r = repr(vv)
                    small[kk] = r[:80] + ("..." if len(r) > 80 else "")
                sess_snapshot[k] = small
            else:
                # for lists/objects, just show repr
                r = repr(v)
                sess_snapshot[k] = r[:120] + ("..." if len(r) > 120 else "")

        return jsonify({
            "user_id_from_helper": _get_current_user_id(),
            "session_keys": list(session.keys()),
            "session_snapshot": sess_snapshot,
        })

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

    return app
