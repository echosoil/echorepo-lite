import base64
import json
import time

import requests
from flask import request, session

from ..config import settings
from .keycloak import KC_TOKEN, KC_USERINFO


def _jwt_payload_unverified(token: str) -> dict:
    try:
        parts = token.split(".")
        if len(parts) != 3:
            return {}
        payload_b64 = parts[1] + "=" * (-len(parts[1]) % 4)
        return json.loads(base64.urlsafe_b64decode(payload_b64).decode("utf-8"))
    except Exception:
        return {}


def create_session_from_tokens(tok_json: dict):
    access_token = tok_json.get("access_token")
    refresh_token = tok_json.get("refresh_token")
    if not access_token or not refresh_token:
        raise ValueError("Missing tokens from IdP")

    profile = None
    try:
        r = requests.get(
            KC_USERINFO, headers={"Authorization": f"Bearer {access_token}"}, timeout=10
        )
        profile = r.json() if r.status_code == 200 else None
    except Exception:
        profile = None
    if profile is None:
        p = _jwt_payload_unverified(access_token)
        profile = {
            "sub": p.get("sub"),
            "email": p.get("email"),
            "username": p.get("preferred_username") or p.get("email"),
            "name": p.get("name"),
        }

    now = int(time.time())
    session["kc"] = {
        "access_token": access_token,
        "refresh_token": refresh_token,
        "exp": now + int(tok_json.get("expires_in", 300)),
        "refresh_exp": now + int(tok_json.get("refresh_expires_in", 1800)),
        "profile": profile,
    }
    session["user"] = profile.get("email") or profile.get("username") or profile.get("sub")
    session.permanent = True


def refresh_tokens_if_needed():
    kc = session.get("kc")
    if not kc:
        return
    now = int(time.time())
    if now < kc.get("exp", 0) - 60:
        return
    data = {
        "grant_type": "refresh_token",
        "client_id": settings.KC_CLIENT_ID,
        "client_secret": settings.KC_CLIENT_SECRET,
        "refresh_token": kc.get("refresh_token"),
    }
    try:
        r = requests.post(KC_TOKEN, data=data, timeout=15)
        if r.status_code == 200:
            create_session_from_tokens(r.json())
        else:
            session.pop("kc", None)
    except Exception:
        session.pop("kc", None)


def before_request_refresh():
    if request.endpoint in ("static", "auth.sso_password_login", "auth.login", "auth.logout"):
        return
    refresh_tokens_if_needed()
