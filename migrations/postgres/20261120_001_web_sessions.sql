-- Opaque SID is never persisted in plaintext; only SHA-256 of a 32-byte
-- random session identifier is stored. JSONB contains confidential Keycloak
-- tokens; restrict PostgreSQL access and backups accordingly.
CREATE TABLE IF NOT EXISTS echorepo_web_sessions (
    sid_hash CHAR(64) PRIMARY KEY,
    data JSONB NOT NULL,
    version BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS echorepo_web_sessions_expire_idx
    ON echorepo_web_sessions (expires_at);
