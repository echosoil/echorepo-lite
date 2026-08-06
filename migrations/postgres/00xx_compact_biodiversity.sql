CREATE TABLE IF NOT EXISTS biodiversity_uploads (
    upload_id TEXT PRIMARY KEY,
    original_filename TEXT NOT NULL,
    archive_object_name TEXT NOT NULL,
    sha256 TEXT NOT NULL UNIQUE,
    aggregation_level TEXT NOT NULL,
    sample_count INTEGER NOT NULL,
    marker_count INTEGER NOT NULL,
    source_row_count INTEGER NOT NULL,
    nonzero_value_count INTEGER NOT NULL,
    uploaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    uploaded_by TEXT
);

CREATE TABLE IF NOT EXISTS sample_taxon_abundance (
    sample_id TEXT NOT NULL,
    marker TEXT NOT NULL,
    level TEXT NOT NULL,
    taxon TEXT NOT NULL,
    read_count DOUBLE PRECISION NOT NULL,
    relative_abundance_pct DOUBLE PRECISION NOT NULL,
    source_upload_id TEXT,
    uploaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    uploaded_by TEXT,
    source_file TEXT,

    PRIMARY KEY (
        sample_id,
        marker,
        level,
        taxon
    ),

    FOREIGN KEY (source_upload_id)
        REFERENCES biodiversity_uploads(upload_id),

    CHECK (read_count >= 0),

    CHECK (
        relative_abundance_pct >= 0
        AND relative_abundance_pct <= 100.000001
    )
);

CREATE INDEX IF NOT EXISTS
    idx_sample_taxon_abundance_lookup
ON sample_taxon_abundance (
    sample_id,
    marker,
    level
);