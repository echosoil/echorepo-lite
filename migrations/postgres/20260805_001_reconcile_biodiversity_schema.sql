CREATE TABLE IF NOT EXISTS biodiversity_uploads (
    upload_id TEXT PRIMARY KEY,
    original_filename TEXT NOT NULL,
    archive_object_name TEXT NOT NULL,
    sha256 TEXT NOT NULL UNIQUE,
    aggregation_level TEXT NOT NULL DEFAULT 'Phylum',
    sample_count INTEGER NOT NULL,
    marker_count INTEGER NOT NULL,
    source_row_count INTEGER NOT NULL,
    nonzero_value_count INTEGER NOT NULL,
    uploaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    uploaded_by TEXT
);

-- Rename the old timestamp column when uploaded_at does not yet exist.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'sample_taxon_abundance'
          AND column_name = 'updated_at'
    )
    AND NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'sample_taxon_abundance'
          AND column_name = 'uploaded_at'
    )
    THEN
        ALTER TABLE sample_taxon_abundance
        RENAME COLUMN updated_at TO uploaded_at;
    END IF;
END
$$;

-- Ensure all columns required by the importer exist.
ALTER TABLE sample_taxon_abundance
    ADD COLUMN IF NOT EXISTS uploaded_at
        TIMESTAMPTZ NOT NULL DEFAULT now(),

    ADD COLUMN IF NOT EXISTS uploaded_by
        TEXT,

    ADD COLUMN IF NOT EXISTS source_file
        TEXT;

-- The new importer uses a SHA-256 string as source_upload_id,
-- so UUID must become TEXT.
DO $$
DECLARE
    current_type TEXT;
BEGIN
    SELECT data_type
    INTO current_type
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'sample_taxon_abundance'
      AND column_name = 'source_upload_id';

    IF current_type IS NOT NULL
       AND current_type <> 'text'
    THEN
        ALTER TABLE sample_taxon_abundance
        ALTER COLUMN source_upload_id
        TYPE TEXT
        USING source_upload_id::text;
    END IF;
END
$$;

-- Old UUID provenance values may not exist in biodiversity_uploads.
-- Clear only those unmatched legacy values before adding the FK.
UPDATE sample_taxon_abundance AS sta
SET source_upload_id = NULL
WHERE source_upload_id IS NOT NULL
  AND NOT EXISTS (
      SELECT 1
      FROM biodiversity_uploads AS bu
      WHERE bu.upload_id = sta.source_upload_id
  );

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid =
            'public.sample_taxon_abundance'::regclass
          AND conname =
            'sample_taxon_abundance_upload_fk'
    )
    THEN
        ALTER TABLE sample_taxon_abundance
        ADD CONSTRAINT sample_taxon_abundance_upload_fk
        FOREIGN KEY (source_upload_id)
        REFERENCES biodiversity_uploads(upload_id)
        ON DELETE SET NULL;
    END IF;
END
$$;

CREATE INDEX IF NOT EXISTS
    idx_sample_taxon_abundance_lookup
ON sample_taxon_abundance (
    sample_id,
    marker,
    level
);

CREATE INDEX IF NOT EXISTS
    idx_sample_taxon_abundance_upload
ON sample_taxon_abundance (
    source_upload_id
);