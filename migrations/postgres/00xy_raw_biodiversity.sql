CREATE TABLE IF NOT EXISTS biodiversity_raw_samples (
    upload_id TEXT NOT NULL
        REFERENCES biodiversity_uploads(upload_id)
        ON DELETE CASCADE,

    sample_index INTEGER NOT NULL
        CHECK (sample_index >= 1),

    source_column_number INTEGER NOT NULL
        CHECK (source_column_number >= 1),

    source_sample_label TEXT NOT NULL,

    -- Parsed ECHOREPO sample identifier, e.g.
    -- ABCD-1234-16S -> ABCD-1234.
    -- It may not currently exist in samples, therefore no FK here.
    sample_id TEXT,

    marker TEXT NOT NULL,

    PRIMARY KEY (
        upload_id,
        sample_index
    ),

    UNIQUE (
        upload_id,
        source_column_number
    )
);


CREATE TABLE IF NOT EXISTS biodiversity_raw_features (
    upload_id TEXT NOT NULL
        REFERENCES biodiversity_uploads(upload_id)
        ON DELETE CASCADE,

    feature_index INTEGER NOT NULL
        CHECK (feature_index >= 1),

    source_row_number INTEGER NOT NULL
        CHECK (source_row_number >= 2),

    -- OTU/feature ID exactly as present in the source.
    source_feature_id TEXT,

    -- Human-readable representation of the original taxonomy.
    taxonomy_raw TEXT,

    kingdom TEXT,
    phylum TEXT,
    class_name TEXT,
    order_name TEXT,
    family TEXT,
    genus TEXT,
    species TEXT,

    -- Preserve source taxonomy fields that do not fit the standard ranks.
    taxonomy_source JSONB NOT NULL DEFAULT '{}'::jsonb,

    PRIMARY KEY (
        upload_id,
        feature_index
    ),

    UNIQUE (
        upload_id,
        source_row_number
    )
);


CREATE TABLE IF NOT EXISTS biodiversity_raw_abundance (
    upload_id TEXT NOT NULL,

    feature_index INTEGER NOT NULL,

    sample_index INTEGER NOT NULL,

    -- Sparse representation: only non-zero values are stored.
    read_count DOUBLE PRECISION NOT NULL
        CHECK (read_count > 0),

    PRIMARY KEY (
        upload_id,
        feature_index,
        sample_index
    ),

    FOREIGN KEY (
        upload_id,
        feature_index
    )
    REFERENCES biodiversity_raw_features (
        upload_id,
        feature_index
    )
    ON DELETE CASCADE,

    FOREIGN KEY (
        upload_id,
        sample_index
    )
    REFERENCES biodiversity_raw_samples (
        upload_id,
        sample_index
    )
    ON DELETE CASCADE
);


CREATE INDEX IF NOT EXISTS
    idx_biodiversity_raw_samples_sample_id
ON biodiversity_raw_samples (
    sample_id,
    marker
);


CREATE INDEX IF NOT EXISTS
    idx_biodiversity_raw_abundance_sample
ON biodiversity_raw_abundance (
    upload_id,
    sample_index
);


CREATE INDEX IF NOT EXISTS
    idx_biodiversity_raw_abundance_feature
ON biodiversity_raw_abundance (
    upload_id,
    feature_index
);