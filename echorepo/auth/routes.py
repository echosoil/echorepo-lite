from urllib.parse import urlsplit

import requests
from flask import Blueprint, jsonify, redirect, render_template, request, session, url_for

from ..config import settings
from ..extensions import oauth
from ..services.firebase import send_password_reset_email
from .keycloak import KC_ISSUER, KC_LOGOUT, KC_TOKEN, KC_USERINFO, KC_WELLKNOWN
from .tokens import before_request_refresh, create_session_from_tokens


auth_bp = Blueprint("auth", __name__)


POST_LOGIN_REDIRECT_KEY = "post_login_redirect"


def safe_next_url(value: str | None) -> str | None:
    """
    Accept only an internal application path.

    Accepted examples:
        /my
        /my?lat=48.12886&lng=16.02856&z=13

    Rejected examples:
        https://evil.example/
        //evil.example/
        javascript:...
    """
    if not value:
        return None

    value = value.strip()

    if not value:
        return None

    parsed = urlsplit(value)

    # External URLs are not allowed.
    if parsed.scheme or parsed.netloc:
        return None

    # The destination must be an absolute path within this application.
    if not value.startswith("/") or value.startswith("//"):
        return None

    return value


def remember_next_url(value: str | None) -> str | None:
    """
    Validate and store the requested post-login destination.
    """
    next_url = safe_next_url(value)

    if next_url:
        session[POST_LOGIN_REDIRECT_KEY] = next_url

    return next_url


def current_next_url() -> str | None:
    """
    Read the stored destination without removing it.
    """
    return safe_next_url(
        session.get(POST_LOGIN_REDIRECT_KEY)
    )


def get_post_login_redirect() -> str:
    """
    Return and remove the stored post-login destination.
    """
    next_url = safe_next_url(
        session.pop(POST_LOGIN_REDIRECT_KEY, None)
    )

    return next_url or url_for("web.home")


def init_oauth(app):
    if not settings.KC_CLIENT_SECRET:
        raise ValueError(
            "KEYCLOAK_CLIENT_SECRET must be set"
        )

    oauth.init_app(app)

    oauth.register(
        name="keycloak",
        server_metadata_url=KC_WELLKNOWN,
        client_id=settings.KC_CLIENT_ID,
        client_secret=settings.KC_CLIENT_SECRET,
        client_kwargs={
            "scope": "openid email profile",
        },
    )

    # Register global before_request token refresh.
    app.before_request(before_request_refresh)


@auth_bp.get("/diag/oidc")
def diag_oidc():
    return jsonify(
        {
            "issuer": KC_ISSUER,
            "well_known": KC_WELLKNOWN,
            "token": KC_TOKEN,
            "userinfo": KC_USERINFO,
            "logout": KC_LOGOUT,
            "use_auth_prefix": settings.KC_USE_AUTH_PREFIX,
            "client_id": settings.KC_CLIENT_ID,
            "has_client_secret": bool(
                settings.KC_CLIENT_SECRET
            ),
        }
    )


@auth_bp.get("/login")
def login():
    """
    Display the login page and remember where the user
    should be sent after authentication.
    """
    next_url = remember_next_url(
        request.args.get("next")
    )

    if not next_url:
        next_url = current_next_url()

    # Demo shortcut.
    if (
        settings.DEMO_MODE
        and request.host == settings.DEMO_HOST
    ):
        session["kc"] = {
            "access_token": "demo",
            "refresh_token": "demo",
            "exp": 9999999999,
            "refresh_exp": 9999999999,
            "profile": {
                "email": settings.DEMO_USER,
                "username": settings.DEMO_USER,
                "name": "Demo User",
                "sub": "demo",
            },
        }

        session["user"] = settings.DEMO_USER

        return redirect(
            get_post_login_redirect()
        )

    return render_template(
        "login.html",
        next_url=next_url or "",
    )


@auth_bp.post("/login")
def sso_password_login():
    """
    Authenticate directly using username/password and
    redirect to the originally requested page.
    """
    # Preserve the hidden next value submitted by login.html.
    remember_next_url(
        request.form.get("next")
    )

    username = request.form.get(
        "username",
        "",
    ).strip()

    password = request.form.get(
        "password",
        "",
    )

    if not username or not password:
        return render_template(
            "login.html",
            error=(
                "Please enter your email and password."
            ),
            next_url=current_next_url() or "",
        )

    data = {
        "grant_type": "password",
        "client_id": settings.KC_CLIENT_ID,
        "client_secret": settings.KC_CLIENT_SECRET,
        "username": username,
        "password": password,
        "scope": "openid email profile",
    }

    try:
        response = requests.post(
            KC_TOKEN,
            data=data,
            timeout=15,
        )
    except requests.RequestException as exc:
        return render_template(
            "login.html",
            error=(
                "Cannot reach identity provider: "
                f"{exc}"
            ),
            next_url=current_next_url() or "",
        )

    if response.status_code != 200:
        message = "Invalid credentials."

        if response.headers.get(
            "content-type",
            "",
        ).startswith("application/json"):
            try:
                message = (
                    response.json().get(
                        "error_description"
                    )
                    or message
                )
            except ValueError:
                pass

        return render_template(
            "login.html",
            error=message,
            next_url=current_next_url() or "",
        )

    # Capture the destination before creating the authenticated
    # session, in case create_session_from_tokens modifies it.
    redirect_target = current_next_url()

    try:
        create_session_from_tokens(
            response.json()
        )
    except Exception as exc:
        return render_template(
            "login.html",
            error=f"Login failed: {exc}",
            next_url=redirect_target or "",
        )

    # Do not leave a stale redirect in the session.
    session.pop(
        POST_LOGIN_REDIRECT_KEY,
        None,
    )

    return redirect(
        redirect_target
        or url_for("web.home")
    )


@auth_bp.get("/sso/login")
def sso_login():
    """
    Start the browser-based Keycloak/OIDC login.
    """
    # This supports calling /sso/login?next=... directly.
    remember_next_url(
        request.args.get("next")
    )

    redirect_uri = url_for(
        "auth.sso_callback",
        _external=True,
    )

    return oauth.keycloak.authorize_redirect(
        redirect_uri
    )


@auth_bp.get("/sso/callback")
def sso_callback():
    """
    Complete browser-based Keycloak/OIDC authentication.
    """
    # Capture it before Authlib and the session creation code
    # potentially modify the Flask session.
    redirect_target = current_next_url()

    token = (
        oauth.keycloak.authorize_access_token()
    )

    # Prefer ID token; fallback to userinfo already happens
    # inside create_session_from_tokens().
    create_session_from_tokens(token)

    # Do not reuse this destination in a future login.
    session.pop(
        POST_LOGIN_REDIRECT_KEY,
        None,
    )

    return redirect(
        redirect_target
        or url_for("web.home")
    )


@auth_bp.get("/logout")
def logout():
    kc = session.get("kc")

    if kc:
        try:
            requests.post(
                KC_LOGOUT,
                data={
                    "client_id":
                        settings.KC_CLIENT_ID,

                    "client_secret":
                        settings.KC_CLIENT_SECRET,

                    "refresh_token":
                        kc.get(
                            "refresh_token",
                            "",
                        ),
                },
                timeout=10,
            )
        except Exception:
            pass

    session.clear()

    return redirect(
        url_for("auth.login")
    )


@auth_bp.post("/password-reset")
def password_reset():
    email = request.form.get(
        "username",
        "",
    ).strip()

    next_url = current_next_url() or ""

    if not email:
        return render_template(
            "login.html",
            error=(
                "Please enter your email address "
                "to reset your password."
            ),
            next_url=next_url,
        )

    ok, message = send_password_reset_email(
        email
    )

    if ok:
        return render_template(
            "login.html",
            info=message,
            next_url=next_url,
        )

    return render_template(
        "login.html",
        error=message,
        next_url=next_url,
    )