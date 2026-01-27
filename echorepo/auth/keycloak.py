from ..config import settings


def kc_url(path: str) -> str:
    """KC 12 uses /auth prefix; modern KC doesn't."""
    return (
        f"{settings.KC_BASE}/auth{path}"
        if settings.KC_USE_AUTH_PREFIX
        else f"{settings.KC_BASE}{path}"
    )


KC_ISSUER = kc_url(f"/realms/{settings.KC_REALM}")
KC_WELLKNOWN = kc_url(f"/realms/{settings.KC_REALM}/.well-known/openid-configuration")
KC_TOKEN = kc_url(f"/realms/{settings.KC_REALM}/protocol/openid-connect/token")
KC_USERINFO = kc_url(f"/realms/{settings.KC_REALM}/protocol/openid-connect/userinfo")
KC_LOGOUT = kc_url(f"/realms/{settings.KC_REALM}/protocol/openid-connect/logout")
KC_INTROSPECT = kc_url(f"/realms/{settings.KC_REALM}/protocol/openid-connect/token/introspect")
