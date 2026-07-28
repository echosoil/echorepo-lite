CREATE TABLE sample_taxon_abundance (
    sample_id TEXT NOT NULL,
    marker TEXT NOT NULL,
    level TEXT NOT NULL,
    taxon TEXT NOT NULL,

    read_count DOUBLE PRECISION NOT NULL,
    relative_abundance_pct DOUBLE PRECISION NOT NULL,

    source_upload_id UUID,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (sample_id, marker, level, taxon),

    CHECK (marker IN ('16S', 'ITS')),
    CHECK (read_count >= 0),
    CHECK (
        relative_abundance_pct >= 0
        AND relative_abundance_pct <= 100
    )
);

CREATE INDEX idx_sample_taxon_abundance_lookup
ON sample_taxon_abundance (sample_id, marker, level);