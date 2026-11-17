-- Durable receipt/status for API-key biodiversity uploads.
-- Original CSV/TSV bytes reside only in a temporary staging directory.
CREATE TABLE IF NOT EXISTS biodiversity_import_jobs (
    job_id UUID PRIMARY KEY,
    original_filename TEXT NOT NULL,
    source_sha256 TEXT NOT NULL CHECK (length(source_sha256) = 64),
    size_bytes BIGINT NOT NULL CHECK (size_bytes > 0),
    uploaded_by TEXT NOT NULL DEFAULT 'api_key',
    status TEXT NOT NULL DEFAULT 'queued'
        CHECK (status IN ('queued','running','succeeded','failed')),
    queued_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    inserted_rows BIGINT,
    error_message TEXT
);
CREATE INDEX IF NOT EXISTS biodiversity_import_jobs_next
    ON biodiversity_import_jobs (queued_at, job_id)
    WHERE status = 'queued';
