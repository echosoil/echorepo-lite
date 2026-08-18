-- cleanup_biodiversity_global_1pct.sql
--
-- Remove from the CURRENT structured biodiversity dataset:
--   1) completely unassigned/no-hit OTUs; and
--   2) OTUs whose relative abundance is < 1.0% in EVERY current sample.
--
-- Relative abundance is calculated as:
--   OTU read_count / total retained reads in that sample
--
-- The denominator is calculated after removing completely unassigned/no-hit
-- OTUs, but before applying the 1% OTU filter.
--
-- An OTU is retained if it reaches >= 1.0% in at least one current sample.
-- Exactly 1.0% is retained.
--
-- Original archived uploads in MinIO are not touched.

BEGIN;

CREATE TEMP TABLE _bio_cleanup_config (
    min_relative_abundance_pct double precision NOT NULL
) ON COMMIT DROP;

INSERT INTO _bio_cleanup_config VALUES (1.0);

-- Current sample -> authoritative upload mapping.
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
GROUP BY UPPER(sample_id), UPPER(marker);

CREATE UNIQUE INDEX ON _bio_current_sources (sample_id, marker);

CREATE TEMP TABLE _bio_current_sample_columns ON COMMIT DROP AS
SELECT
    current.sample_id,
    current.marker,
    current.source_upload_id AS upload_id,
    rs.sample_index
FROM _bio_current_sources AS current
JOIN biodiversity_raw_samples AS rs
  ON rs.upload_id = current.source_upload_id
 AND UPPER(rs.sample_id) = current.sample_id
 AND UPPER(rs.marker) = current.marker;

CREATE UNIQUE INDEX ON _bio_current_sample_columns (sample_id, marker);

DO $$
DECLARE
    expected_count bigint;
    resolved_count bigint;
BEGIN
    SELECT COUNT(*) INTO expected_count FROM _bio_current_sources;
    SELECT COUNT(*) INTO resolved_count FROM _bio_current_sample_columns;

    IF expected_count <> resolved_count THEN
        RAISE EXCEPTION
            'Could not resolve every current sample/marker to exactly one raw sample column: expected %, resolved %',
            expected_count, resolved_count;
    END IF;
END
$$;

-- Completely unassigned/no-hit OTUs in current uploads.
CREATE TEMP TABLE _bio_unassigned_features ON COMMIT DROP AS
SELECT
    f.upload_id,
    f.feature_index,
    f.source_feature_id
FROM biodiversity_raw_features AS f
WHERE f.upload_id IN (
    SELECT DISTINCT upload_id FROM _bio_current_sample_columns
)
AND regexp_replace(LOWER(BTRIM(COALESCE(f.kingdom, ''))),   '[^a-z0-9]+', '', 'g')
        IN ('', 'nohit', 'nohits', 'unassigned', 'unknown', 'nan', 'none', 'null', 'na')
AND regexp_replace(LOWER(BTRIM(COALESCE(f.phylum, ''))),    '[^a-z0-9]+', '', 'g')
        IN ('', 'nohit', 'nohits', 'unassigned', 'unknown', 'nan', 'none', 'null', 'na')
AND regexp_replace(LOWER(BTRIM(COALESCE(f.class_name, ''))),'[^a-z0-9]+', '', 'g')
        IN ('', 'nohit', 'nohits', 'unassigned', 'unknown', 'nan', 'none', 'null', 'na')
AND regexp_replace(LOWER(BTRIM(COALESCE(f.order_name, ''))),'[^a-z0-9]+', '', 'g')
        IN ('', 'nohit', 'nohits', 'unassigned', 'unknown', 'nan', 'none', 'null', 'na')
AND regexp_replace(LOWER(BTRIM(COALESCE(f.family, ''))),    '[^a-z0-9]+', '', 'g')
        IN ('', 'nohit', 'nohits', 'unassigned', 'unknown', 'nan', 'none', 'null', 'na')
AND regexp_replace(LOWER(BTRIM(COALESCE(f.genus, ''))),     '[^a-z0-9]+', '', 'g')
        IN ('', 'nohit', 'nohits', 'unassigned', 'unknown', 'nan', 'none', 'null', 'na')
AND regexp_replace(LOWER(BTRIM(COALESCE(f.species, ''))),   '[^a-z0-9]+', '', 'g')
        IN ('', 'nohit', 'nohits', 'unassigned', 'unknown', 'nan', 'none', 'null', 'na');

CREATE UNIQUE INDEX ON _bio_unassigned_features (upload_id, feature_index);

-- Per-sample totals after excluding unassigned OTUs.
CREATE TEMP TABLE _bio_sample_totals ON COMMIT DROP AS
SELECT
    c.sample_id,
    c.marker,
    c.upload_id,
    c.sample_index,
    SUM(a.read_count)::double precision AS total_reads
FROM _bio_current_sample_columns AS c
JOIN biodiversity_raw_abundance AS a
  ON a.upload_id = c.upload_id
 AND a.sample_index = c.sample_index
LEFT JOIN _bio_unassigned_features AS bad
  ON bad.upload_id = a.upload_id
 AND bad.feature_index = a.feature_index
WHERE bad.feature_index IS NULL
GROUP BY c.sample_id, c.marker, c.upload_id, c.sample_index;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM _bio_current_sample_columns AS c
        LEFT JOIN _bio_sample_totals AS t
          ON t.sample_id = c.sample_id
         AND t.marker = c.marker
        WHERE COALESCE(t.total_reads, 0) <= 0
    ) THEN
        RAISE EXCEPTION
            'At least one current sample/marker has zero reads after removing unassigned OTUs; aborting.';
    END IF;
END
$$;

-- Maximum relative abundance of each OTU ID across all current samples.
CREATE TEMP TABLE _bio_feature_max_pct ON COMMIT DROP AS
SELECT
    f.source_feature_id,
    MAX(100.0 * a.read_count / t.total_reads)::double precision
        AS max_relative_abundance_pct
FROM _bio_current_sample_columns AS c
JOIN _bio_sample_totals AS t
  ON t.sample_id = c.sample_id
 AND t.marker = c.marker
 AND t.upload_id = c.upload_id
 AND t.sample_index = c.sample_index
JOIN biodiversity_raw_abundance AS a
  ON a.upload_id = c.upload_id
 AND a.sample_index = c.sample_index
JOIN biodiversity_raw_features AS f
  ON f.upload_id = a.upload_id
 AND f.feature_index = a.feature_index
LEFT JOIN _bio_unassigned_features AS bad
  ON bad.upload_id = f.upload_id
 AND bad.feature_index = f.feature_index
WHERE bad.feature_index IS NULL
GROUP BY f.source_feature_id;

CREATE UNIQUE INDEX ON _bio_feature_max_pct (source_feature_id);

-- OTUs that never reach 1% in any current sample.
CREATE TEMP TABLE _bio_low_features ON COMMIT DROP AS
SELECT
    f.upload_id,
    f.feature_index,
    f.source_feature_id,
    COALESCE(p.max_relative_abundance_pct, 0.0)
        AS max_relative_abundance_pct
FROM biodiversity_raw_features AS f
LEFT JOIN _bio_feature_max_pct AS p
  ON p.source_feature_id = f.source_feature_id
LEFT JOIN _bio_unassigned_features AS unassigned
  ON unassigned.upload_id = f.upload_id
 AND unassigned.feature_index = f.feature_index
CROSS JOIN _bio_cleanup_config AS cfg
WHERE f.upload_id IN (
    SELECT DISTINCT upload_id FROM _bio_current_sample_columns
)
AND unassigned.feature_index IS NULL
AND COALESCE(p.max_relative_abundance_pct, 0.0)
        < cfg.min_relative_abundance_pct;

CREATE UNIQUE INDEX ON _bio_low_features (upload_id, feature_index);

CREATE TEMP TABLE _bio_features_to_remove ON COMMIT DROP AS
SELECT upload_id, feature_index, source_feature_id, 'unassigned'::text AS reason
FROM _bio_unassigned_features
UNION ALL
SELECT upload_id, feature_index, source_feature_id,
       'below_1pct_everywhere'::text AS reason
FROM _bio_low_features;

CREATE UNIQUE INDEX ON _bio_features_to_remove (upload_id, feature_index);

-- Report before deletion.
SELECT reason, COUNT(*) AS feature_rows_to_remove
FROM _bio_features_to_remove
GROUP BY reason
ORDER BY reason;

SELECT COUNT(DISTINCT source_feature_id) AS distinct_otu_ids_to_remove
FROM _bio_features_to_remove;

SELECT
    COUNT(*) AS abundance_rows_to_remove,
    COALESCE(SUM(a.read_count), 0) AS read_count_to_remove
FROM biodiversity_raw_abundance AS a
JOIN _bio_features_to_remove AS bad
  ON bad.upload_id = a.upload_id
 AND bad.feature_index = a.feature_index;

SELECT
    source_feature_id,
    MAX(max_relative_abundance_pct) AS max_relative_abundance_pct
FROM _bio_low_features
GROUP BY source_feature_id
ORDER BY max_relative_abundance_pct DESC
LIMIT 30;

-- Safety: every current sample must retain some non-zero reads.
CREATE TEMP TABLE _bio_retained_sample_totals ON COMMIT DROP AS
SELECT
    c.sample_id,
    c.marker,
    SUM(a.read_count)::double precision AS retained_reads
FROM _bio_current_sample_columns AS c
JOIN biodiversity_raw_abundance AS a
  ON a.upload_id = c.upload_id
 AND a.sample_index = c.sample_index
LEFT JOIN _bio_features_to_remove AS bad
  ON bad.upload_id = a.upload_id
 AND bad.feature_index = a.feature_index
WHERE bad.feature_index IS NULL
GROUP BY c.sample_id, c.marker;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM _bio_current_sample_columns AS c
        LEFT JOIN _bio_retained_sample_totals AS r
          ON r.sample_id = c.sample_id
         AND r.marker = c.marker
        WHERE COALESCE(r.retained_reads, 0) <= 0
    ) THEN
        RAISE EXCEPTION
            'The 1%% filter would leave at least one current sample/marker with zero retained reads; aborting.';
    END IF;
END
$$;

-- Delete dependent abundance rows first, then feature rows.
DELETE FROM biodiversity_raw_abundance AS a
USING _bio_features_to_remove AS bad
WHERE a.upload_id = bad.upload_id
  AND a.feature_index = bad.feature_index;

DELETE FROM biodiversity_raw_features AS f
USING _bio_features_to_remove AS bad
WHERE f.upload_id = bad.upload_id
  AND f.feature_index = bad.feature_index;

-- Refresh current upload non-zero counts.
UPDATE biodiversity_uploads AS bu
SET nonzero_value_count = counts.nonzero_value_count
FROM (
    SELECT
        u.upload_id,
        COUNT(a.feature_index)::integer AS nonzero_value_count
    FROM (
        SELECT DISTINCT upload_id
        FROM _bio_current_sample_columns
    ) AS u
    LEFT JOIN biodiversity_raw_abundance AS a
      ON a.upload_id = u.upload_id
    GROUP BY u.upload_id
) AS counts
WHERE counts.upload_id = bu.upload_id;

-- Rebuild current Phylum aggregates from the cleaned structured raw data.
DELETE FROM sample_taxon_abundance AS sta
USING _bio_current_sources AS current
WHERE UPPER(sta.sample_id) = current.sample_id
  AND UPPER(sta.marker) = current.marker
  AND LOWER(sta.level) = 'phylum';

WITH classified_counts AS (
    SELECT
        current.sample_id,
        current.marker,
        current.source_upload_id,
        CASE
            WHEN regexp_replace(
                LOWER(BTRIM(COALESCE(f.phylum, ''))),
                '[^a-z0-9]+', '', 'g'
            ) IN (
                '', 'nohit', 'nohits', 'unassigned', 'unknown',
                'nan', 'none', 'null', 'na'
            )
            THEN 'Unclassified'
            ELSE BTRIM(f.phylum)
        END AS taxon,
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
        CASE
            WHEN regexp_replace(
                LOWER(BTRIM(COALESCE(f.phylum, ''))),
                '[^a-z0-9]+', '', 'g'
            ) IN (
                '', 'nohit', 'nohits', 'unassigned', 'unknown',
                'nan', 'none', 'null', 'na'
            )
            THEN 'Unclassified'
            ELSE BTRIM(f.phylum)
        END
),
with_totals AS (
    SELECT
        c.*,
        SUM(c.read_count) OVER (
            PARTITION BY c.sample_id, c.marker
        ) AS total_count
    FROM classified_counts AS c
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
    wt.read_count / wt.total_count * 100.0,
    wt.source_upload_id,
    bu.uploaded_by,
    bu.original_filename
FROM with_totals AS wt
JOIN biodiversity_uploads AS bu
  ON bu.upload_id = wt.source_upload_id
WHERE wt.read_count > 0
  AND wt.total_count > 0
ON CONFLICT (sample_id, marker, level, taxon)
DO UPDATE SET
    read_count = EXCLUDED.read_count,
    relative_abundance_pct = EXCLUDED.relative_abundance_pct,
    source_upload_id = EXCLUDED.source_upload_id,
    uploaded_at = now(),
    uploaded_by = EXCLUDED.uploaded_by,
    source_file = EXCLUDED.source_file;

-- Verification: aggregate percentages should sum to approximately 100.
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
