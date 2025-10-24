from flask import Blueprint, render_template, request, redirect, url_for, jsonify, session
import requests
from ..config import settings
from ..extensions import oauth
from .keycloak import KC_WELLKNOWN, KC_TOKEN, KC_LOGOUT, KC_ISSUER, KC_USERINFO
from .tokens import create_session_from_tokens, before_request_refresh

auth_bp = Blueprint("auth", __name__)

def init_oauth(app):
    if not settings.KC_CLIENT_SECRET:
        raise ValueError("KEYCLOAK_CLIENT_SECRET must be set")
    oauth.init_app(app)
    oauth.register(
        name="keycloak",
        server_metadata_url=KC_WELLKNOWN,
        client_id=settings.KC_CLIENT_ID,
        client_secret=settings.KC_CLIENT_SECRET,
        client_kwargs={"scope": "openid email profile"},
    )
    # register global before_request token refresh
    app.before_request(before_request_refresh)

@auth_bp.get("/diag/oidc")
def diag_oidc():
    return jsonify({
        "issuer": KC_ISSUER,
        "well_known": KC_WELLKNOWN,
        "token": KC_TOKEN,
        "userinfo": KC_USERINFO,
        "logout": KC_LOGOUT,
        "use_auth_prefix": settings.KC_USE_AUTH_PREFIX,
        "client_id": settings.KC_CLIENT_ID,
        "has_client_secret": bool(settings.KC_CLIENT_SECRET),
    })

@auth_bp.get("/login")
def login():
    # Demo shortcut
    if settings.DEMO_MODE and request.host == settings.DEMO_HOST:
        session["kc"] = {
            "access_token": "demo",
            "refresh_token": "demo",
            "exp": 9999999999,
            "refresh_exp": 9999999999,
            "profile": {"email": settings.DEMO_USER, "username": settings.DEMO_USER, "name": "Demo User", "sub":"demo"},
        }
        session["user"] = settings.DEMO_USER
        return redirect(url_for("web.home"))
    return render_template("login.html")

@auth_bp.post("/login")
def sso_password_login():
    username = request.form.get("username","").strip()
    password = request.form.get("password","")
    if not username or not password:
        return render_template("login.html", error="Please enter your email and password.")
    data = {
        "grant_type": "password",
        "client_id": settings.KC_CLIENT_ID,
        "client_secret": settings.KC_CLIENT_SECRET,
        "username": username,
        "password": password,
        "scope": "openid email profile",
    }
    try:
        r = requests.post(KC_TOKEN, data=data, timeout=15)
    except requests.RequestException as e:
        return render_template("login.html", error=f"Cannot reach identity provider: {e}")
    if r.status_code != 200:
        msg = r.json().get("error_description") if r.headers.get("content-type","").startswith("application/json") else "Invalid credentials."
        return render_template("login.html", error=msg)

    try:
        create_session_from_tokens(r.json())
    except Exception as e:
        return render_template("login.html", error=f"Login failed: {e}")
    return redirect(url_for("web.home"))

@auth_bp.get("/sso/login")
def sso_login():
    redirect_uri = url_for("auth.sso_callback", _external=True)
    return oauth.keycloak.authorize_redirect(redirect_uri)

@auth_bp.get("/sso/callback")
def sso_callback():
    token = oauth.keycloak.authorize_access_token()
    # prefer ID token; fallback to userinfo already happens in tokens.create_session_from_tokens
    create_session_from_tokens(token)
    return redirect(url_for("web.home"))

@auth_bp.get("/logout")
def logout():
    kc = session.get("kc")
    if kc:
        try:
            requests.post(
                KC_LOGOUT,
                data={"client_id": settings.KC_CLIENT_ID, "client_secret": settings.KC_CLIENT_SECRET, "refresh_token": kc.get("refresh_token","")},
                timeout=10
            )
        except Exception:
            pass
    session.clear()
    return redirect(url_for("auth.login"))
