import os

class Settings:
    # Data locations
    INPUT_CSV: str    = os.getenv("INPUT_CSV", "/data/echorepo_samples.csv")
    SQLITE_PATH: str  = os.getenv("SQLITE_PATH", "/data/db/echo.db")
    TABLE_NAME: str   = os.getenv("TABLE_NAME", "samples")
    USERS_CSV: str    = os.getenv("USERS_CSV", "/data/users.csv")
    USER_KEY_COLUMN: str = os.getenv("USER_KEY_COLUMN", "email")

    # App secret & cookies
    SECRET_KEY: str   = os.getenv("SECRET_KEY", "please-change-me")
    SESSION_COOKIE_SAMESITE: str = os.getenv("SESSION_COOKIE_SAMESITE", "Lax")
    SESSION_COOKIE_SECURE: bool  = os.getenv("SESSION_COOKIE_SECURE", "true").lower() in ("1","true","yes")

    # Jitter / privacy
    MAX_JITTER_METERS: float = float(os.getenv("MAX_JITTER_METERS", "1000"))
    JITTER_SALT: str         = os.getenv("JITTER_SALT", "change-this-salt")

    # Column names (preferred)
    LAT_COL: str = os.getenv("LAT_COL", "GPS_lat")
    LON_COL: str = os.getenv("LON_COL", "GPS_long")

    # Demo options
    DEMO_MODE: bool = os.getenv("DEMO_MODE", "false").lower() in ("1","true","yes")
    DEMO_USER: str  = os.getenv("DEMO_USER", "echosoil@echosoil.eu")
    DEMO_HOST: str  = os.getenv("DEMO_HOST", "echorepo.quanta-labs.com")

    ORIG_COL_SUFFIX: str = os.getenv("ORIG_COL_SUFFIX", "_orig")
    HIDE_ORIG_COLS: bool = os.getenv("HIDE_ORIG_COLS", "true").lower() in ("1","true","yes")
    HIDE_ORIG_LIST: list[str] = [c.strip() for c in os.getenv("HIDE_ORIG_LIST", "").split(",") if c.strip()]

    # Keycloak / OIDC
    KC_BASE: str   = os.getenv("KEYCLOAK_BASE_URL", "https://keycloak-dev.quanta-labs.com").rstrip("/")
    KC_REALM: str  = os.getenv("KEYCLOAK_REALM", "echo_realm")
    KC_USE_AUTH_PREFIX: bool = os.getenv("KEYCLOAK_USE_AUTH_PREFIX", "true").lower() in ("1","true","yes")

    KC_CLIENT_ID: str     = os.getenv("KEYCLOAK_CLIENT_ID", "echo_client")
    KC_CLIENT_SECRET: str = os.getenv("KEYCLOAK_CLIENT_SECRET", "")

    # Misc
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")

settings = Settings()
