-- cleanup_biodiversity_unassigned.sql
--
-- One-time cleanup of already-imported biodiversity OTUs/features whose entire
-- taxonomy is blank / No_hit / No hits / unassigned / unknown / NA-like.
--
-- IMPORTANT:
--   * Original archived uploads in MinIO and biodiversity_uploads are preserved.
--   * biodiversity_raw_samples are preserved.
--   * Matching biodiversity_raw_abundance rows are deleted FIRST because they
--     reference biodiversity_raw_features.
--   * Current Phylum aggregates are rebuilt from the cleaned structured raw data,
--     so sample_taxon_abundance percentages remain correct.
--
-- Run only after installing the importer rule that skips completely unassigned
-- features on future imports.

BEGIN;

-- ---------------------------------------------------------------------------
-- 0. Capture which upload is currently authoritative for each sample/marker
--    BEFORE rebuilding sample_taxon_abundance.
-- ---------------------------------------------------------------------------

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM sample_taxon_abundance
        WHERE source_upload_id IS NOT NULL
        GROUP BY UPPER(sample_id), UPPER(marker)
        HAVING COUNT(DISTINCT source_upload_id) > 1
    ) THEN
        RAISE EXCEPTION
            'Some sample/marker pairs point to more than one source_upload_id; aborting cleanup.';
    END IF;
END
$$;

CREATE TEMP TABLE _bio_current_sources ON COMMIT DROP AS
SELECT
    UPPER(sample_id) AS sample_id,
    UPPER(marker) AS marker,
    MIN(source_upload_id) AS source_upload_id
FROM sample_taxon_abundance
WHERE source_upload_id IS NOT NULL
GROUP BY
    UPPER(sample_id),
    UPPER(marker);

CREATE UNIQUE INDEX ON _bio_current_sources (sample_id, marker);

-- ---------------------------------------------------------------------------
-- 1. Identify completely unassigned features.
--
-- Normalization removes punctuation/whitespace:
--   No_hit     -> nohit
--   No hit     -> nohit
--   No-hits    -> nohits
--   N/A        -> na
--
-- A feature is removed ONLY when every taxonomy rank is an unassigned token.
-- Partially classified features are retained.
-- ---------------------------------------------------------------------------

CREATE TEMP TABLE _bio_unassigned_features ON COMMIT DROP AS
SELECT
    upload_id,
    feature_index,
    source_feature_id
FROM biodiversity_raw_features
WHERE
    regexp_replace(LOWER(BTRIM(COALESCE(kingdom, ''))),   '[^a-z0-9]+', '', 'g')
        IN ('', 'nohit', 'nohits', 'unassigned', 'unknown', 'nan', 'none', 'null', 'na')
AND regexp_replace(LOWER(BTRIM(COALESCE(phylum, ''))),    '[^a-z0-9]+', '', 'g')
        IN ('', 'nohit', 'nohits', 'unassigned', 'unknown', 'nan', 'none', 'null', 'na')
AND regexp_replace(LOWER(BTRIM(COALESCE(class_name, ''))),'[^a-z0-9]+', '', 'g')
        IN ('', 'nohit', 'nohits', 'unassigned', 'unknown', 'nan', 'none', 'null', 'na')
AND regexp_replace(LOWER(BTRIM(COALESCE(order_name, ''))),'[^a-z0-9]+', '', 'g')
        IN ('', 'nohit', 'nohits', 'unassigned', 'unknown', 'nan', 'none', 'null', 'na')
AND regexp_replace(LOWER(BTRIM(COALESCE(family, ''))),    '[^a-z0-9]+', '', 'g')
        IN ('', 'nohit', 'nohits', 'unassigned', 'unknown', 'nan', 'none', 'null', 'na')
AND regexp_replace(LOWER(BTRIM(COALESCE(genus, ''))),     '[^a-z0-9]+', '', 'g')
        IN ('', 'nohit', 'nohits', 'unassigned', 'unknown', 'nan', 'none', 'null', 'na')
AND regexp_replace(LOWER(BTRIM(COALESCE(species, ''))),   '[^a-z0-9]+', '', 'g')
        IN ('', 'nohit', 'nohits', 'unassigned', 'unknown', 'nan', 'none', 'null', 'na');

CREATE UNIQUE INDEX ON _bio_unassigned_features (upload_id, feature_index);

-- Show exactly what will be removed.
SELECT
    COUNT(*) AS features_to_remove
FROM _bio_unassigned_features;

SELECT
    COUNT(*) AS abundance_rows_to_remove,
    COALESCE(SUM(a.read_count), 0) AS read_count_to_remove
FROM biodiversity_raw_abundance AS a
JOIN _bio_unassigned_features AS bad
  ON bad.upload_id = a.upload_id
 AND bad.feature_index = a.feature_index;

SELECT
    bu.original_filename,
    COUNT(*) AS features_to_remove
FROM _bio_unassigned_features AS bad
LEFT JOIN biodiversity_uploads AS bu
  ON bu.upload_id = bad.upload_id
GROUP BY bu.original_filename
ORDER BY features_to_remove DESC, bu.original_filename;

-- ---------------------------------------------------------------------------
-- 2. Delete dependent abundance rows first, then feature rows.
-- ---------------------------------------------------------------------------

DELETE FROM biodiversity_raw_abundance AS a
USING _bio_unassigned_features AS bad
WHERE a.upload_id = bad.upload_id
  AND a.feature_index = bad.feature_index;

DELETE FROM biodiversity_raw_features AS f
USING _bio_unassigned_features AS bad
WHERE f.upload_id = bad.upload_id
  AND f.feature_index = bad.feature_index;

-- Keep biodiversity_uploads.nonzero_value_count consistent with the cleaned
-- structured raw representation. source_row_count intentionally remains the
-- count of rows in the original archived source.
UPDATE biodiversity_uploads AS bu
SET nonzero_value_count = counts.nonzero_value_count
FROM (
    SELECT
        u.upload_id,
        COUNT(a.*)::integer AS nonzero_value_count
    FROM biodiversity_uploads AS u
    LEFT JOIN biodiversity_raw_abundance AS a
      ON a.upload_id = u.upload_id
    GROUP BY u.upload_id
) AS counts
WHERE counts.upload_id = bu.upload_id;

-- ---------------------------------------------------------------------------
-- 3. Rebuild CURRENT Phylum aggregates from the cleaned structured raw data.
--
-- The importer historically maps a retained feature with no usable Phylum
-- to "Unclassified". Completely unassigned features have already been removed.
-- ---------------------------------------------------------------------------

DELETE FROM sample_taxon_abundance AS sta
USING _bio_current_sources AS current
WHERE UPPER(sta.sample_id) = current.sample_id
  AND UPPER(sta.marker) = current.marker
  AND sta.source_upload_id = current.source_upload_id
  AND LOWER(sta.level) = 'phylum';

WITH phylum_counts AS (
    SELECT
        current.sample_id,
        current.marker,
        current.source_upload_id,
        COALESCE(
            NULLIF(BTRIM(f.phylum), ''),
            'Unclassified'
        ) AS taxon,
        SUM(a.read_count)::double precision AS read_count
    FROM _bio_current_sources AS current
    JOIN biodiversity_raw_samples AS rs
      ON rs.upload_id = current.source_upload_id
     AND UPPER(rs.sample_id) = current.sample_id
     AND UPPER(rs.marker) = current.marker
    JOIN biodiversity_raw_abundance AS a
      ON a.upload_id = rs.upload_id
     AND a.sample_index = rs.sample_index
    JOIN biodiversity_raw_features AS f
      ON f.upload_id = a.upload_id
     AND f.feature_index = a.feature_index
    GROUP BY
        current.sample_id,
        current.marker,
        current.source_upload_id,
        COALESCE(NULLIF(BTRIM(f.phylum), ''), 'Unclassified')
),
with_totals AS (
    SELECT
        pc.*,
        SUM(pc.read_count) OVER (
            PARTITION BY pc.sample_id, pc.marker
        ) AS total_count
    FROM phylum_counts AS pc
)
INSERT INTO sample_taxon_abundance (
    sample_id,
    marker,
    level,
    taxon,
    read_count,
    relative_abundance_pct,
    source_upload_id,
    uploaded_by,
    source_file
)
SELECT
    wt.sample_id,
    wt.marker,
    'Phylum',
    wt.taxon,
    wt.read_count,
    CASE
        WHEN wt.total_count > 0
            THEN wt.read_count / wt.total_count * 100.0
        ELSE 0.0
    END,
    wt.source_upload_id,
    bu.uploaded_by,
    bu.original_filename
FROM with_totals AS wt
JOIN biodiversity_uploads AS bu
  ON bu.upload_id = wt.source_upload_id
WHERE wt.read_count > 0
ON CONFLICT (
    sample_id,
    marker,
    level,
    taxon
)
DO UPDATE SET
    read_count = EXCLUDED.read_count,
    relative_abundance_pct = EXCLUDED.relative_abundance_pct,
    source_upload_id = EXCLUDED.source_upload_id,
    uploaded_at = now(),
    uploaded_by = EXCLUDED.uploaded_by,
    source_file = EXCLUDED.source_file;

-- ---------------------------------------------------------------------------
-- 4. Verification. All three checks should be sensible; final count should be 0.
-- ---------------------------------------------------------------------------

SELECT
    COUNT(*) AS remaining_completely_unassigned_features
FROM biodiversity_raw_features
WHERE
    regexp_replace(LOWER(BTRIM(COALESCE(kingdom, ''))),   '[^a-z0-9]+', '', 'g')
        IN ('', 'nohit', 'nohits', 'unassigned', 'unknown', 'nan', 'none', 'null', 'na')
AND regexp_replace(LOWER(BTRIM(COALESCE(phylum, ''))),    '[^a-z0-9]+', '', 'g')
        IN ('', 'nohit', 'nohits', 'unassigned', 'unknown', 'nan', 'none', 'null', 'na')
AND regexp_replace(LOWER(BTRIM(COALESCE(class_name, ''))),'[^a-z0-9]+', '', 'g')
        IN ('', 'nohit', 'nohits', 'unassigned', 'unknown', 'nan', 'none', 'null', 'na')
AND regexp_replace(LOWER(BTRIM(COALESCE(order_name, ''))),'[^a-z0-9]+', '', 'g')
        IN ('', 'nohit', 'nohits', 'unassigned', 'unknown', 'nan', 'none', 'null', 'na')
AND regexp_replace(LOWER(BTRIM(COALESCE(family, ''))),    '[^a-z0-9]+', '', 'g')
        IN ('', 'nohit', 'nohits', 'unassigned', 'unknown', 'nan', 'none', 'null', 'na')
AND regexp_replace(LOWER(BTRIM(COALESCE(genus, ''))),     '[^a-z0-9]+', '', 'g')
        IN ('', 'nohit', 'nohits', 'unassigned', 'unknown', 'nan', 'none', 'null', 'na')
AND regexp_replace(LOWER(BTRIM(COALESCE(species, ''))),   '[^a-z0-9]+', '', 'g')
        IN ('', 'nohit', 'nohits', 'unassigned', 'unknown', 'nan', 'none', 'null', 'na');

SELECT
    UPPER(marker) AS marker,
    COUNT(DISTINCT sample_id) AS current_samples
FROM sample_taxon_abundance
WHERE LOWER(level) = 'phylum'
GROUP BY UPPER(marker)
ORDER BY marker;

SELECT
    UPPER(marker) AS marker,
    MIN(sum_pct) AS min_sum_pct,
    MAX(sum_pct) AS max_sum_pct
FROM (
    SELECT
        sample_id,
        marker,
        SUM(relative_abundance_pct) AS sum_pct
    FROM sample_taxon_abundance
    WHERE LOWER(level) = 'phylum'
    GROUP BY sample_id, marker
) AS x
GROUP BY UPPER(marker)
ORDER BY marker;

COMMIT;
