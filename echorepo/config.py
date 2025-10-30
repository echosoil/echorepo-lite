# echorepo/config.py
import os
from typing import Optional

class Settings:
    # -------- Data locations --------
    INPUT_CSV: str       = os.getenv("INPUT_CSV", "/data/echorepo_samples.csv")
    SQLITE_PATH: str     = os.getenv("SQLITE_PATH", "/data/db/echo.db")
    TABLE_NAME: str      = os.getenv("TABLE_NAME", "samples")
    USERS_CSV: str       = os.getenv("USERS_CSV", "/data/users.csv")
    USER_KEY_COLUMN: str = os.getenv("USER_KEY_COLUMN", "email")
    API_KEY              = os.environ.get("API_KEY")  # if set, required for access
    SAMPLE_TABLE         = os.environ.get("SAMPLE_TABLE")  # optional override

    # Planned countries (xlsx with QR->countries)
    PLANNED_XLSX: str   = os.getenv("PLANNED_XLSX", "/data/planned.xlsx")

    # -------- App secret & cookies --------
    SECRET_KEY: str   = os.getenv("SECRET_KEY", "please-change-me")
    SESSION_COOKIE_NAME = "echorepo_session"
    SESSION_COOKIE_SAMESITE: str = os.getenv("SESSION_COOKIE_SAMESITE", "Lax")
    SESSION_COOKIE_SECURE: bool  = os.getenv("SESSION_COOKIE_SECURE", "true").lower() in ("1", "true", "yes")

    # -------- Jitter / privacy --------
    MAX_JITTER_METERS: float = float(os.getenv("MAX_JITTER_METERS", "1000"))
    JITTER_SALT: str         = os.getenv("JITTER_SALT", "change-this-salt")

    # -------- Column names (preferred) --------
    LAT_COL: str = os.getenv("LAT_COL", "GPS_lat")
    LON_COL: str = os.getenv("LON_COL", "GPS_long")

    # Sentinel default coordinates to flag as invalid (your app’s defaults)
    DEFAULT_COORD_LAT: float = float(os.getenv("DEFAULT_COORD_LAT", "46.5"))
    DEFAULT_COORD_LON: float = float(os.getenv("DEFAULT_COORD_LON", "11.35"))

    # -------- Demo options --------
    DEMO_MODE: bool = os.getenv("DEMO_MODE", "false").lower() in ("1", "true", "yes")
    DEMO_USER: str  = os.getenv("DEMO_USER", "echosoil@echosoil.eu")
    DEMO_HOST: str  = os.getenv("DEMO_HOST", "echorepo.quanta-labs.com")

    ORIG_COL_SUFFIX: str = os.getenv("ORIG_COL_SUFFIX", "_orig")
    HIDE_ORIG_COLS: bool = os.getenv("HIDE_ORIG_COLS", "true").lower() in ("1", "true", "yes")
    HIDE_ORIG_LIST: list[str] = [c.strip() for c in os.getenv("HIDE_ORIG_LIST", "").split(",") if c.strip()]

    # -------- Keycloak / OIDC --------
    KC_BASE: str   = os.getenv("KEYCLOAK_BASE_URL", "https://keycloak-dev.quanta-labs.com").rstrip("/")
    KC_REALM: str  = os.getenv("KEYCLOAK_REALM", "echo_realm")
    KC_USE_AUTH_PREFIX: bool = os.getenv("KEYCLOAK_USE_AUTH_PREFIX", "true").lower() in ("1", "true", "yes")

    KC_CLIENT_ID: str     = os.getenv("KEYCLOAK_CLIENT_ID", "echo_client")
    KC_CLIENT_SECRET: str = os.getenv("KEYCLOAK_CLIENT_SECRET", "")

    # -------- Firebase --------
    FIREBASE_PROJECT_ID: Optional[str] = os.getenv("FIREBASE_PROJECT_ID") or None
    GOOGLE_APPLICATION_CREDENTIALS: Optional[str] = os.getenv("GOOGLE_APPLICATION_CREDENTIALS") or None
    # -------- i18n / Babel --------
    BABEL_TRANSLATION_DIRECTORIES = "/app/translations" # compiled .mo files location

    # -------- Misc --------
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")


settings = Settings()
