CREATE TABLE IF NOT EXISTS usage_events (
    id           BIGSERIAL PRIMARY KEY,
    ts           TIMESTAMPTZ NOT NULL DEFAULT now(),
    user_id      TEXT,
    event_type   TEXT NOT NULL,       -- 'page_view', 'download', 'api_call', ...
    path         TEXT NOT NULL,
    method       TEXT NOT NULL,
    status_code  INTEGER NOT NULL,
    bytes_sent   BIGINT,
    duration_ms  INTEGER,
    ip_hash      TEXT,
    user_agent   TEXT,
    extra        JSONB
);

CREATE INDEX IF NOT EXISTS idx_usage_events_ts ON usage_events (ts);
CREATE INDEX IF NOT EXISTS idx_usage_events_event_type_ts ON usage_events (event_type, ts);
CREATE INDEX IF NOT EXISTS idx_usage_events_user_ts ON usage_events (user_id, ts);