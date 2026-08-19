-- verify_biodiversity_after_reimport.sql
\set ON_ERROR_STOP on
\pset pager off

\echo '=== Raw sample columns imported ==='
SELECT
    UPPER(marker) AS marker,
    COUNT(*) AS raw_sample_columns,
    COUNT(DISTINCT UPPER(sample_id)) AS distinct_samples,
    COUNT(DISTINCT upload_id) AS source_uploads
FROM biodiversity_raw_samples
GROUP BY UPPER(marker)
ORDER BY marker;

\echo ''
\echo '=== Samples represented in current Phylum aggregates ==='
SELECT
    UPPER(marker) AS marker,
    COUNT(DISTINCT UPPER(sample_id)) AS aggregate_samples,
    COUNT(DISTINCT source_upload_id) AS source_uploads
FROM sample_taxon_abundance
WHERE LOWER(level) = 'phylum'
GROUP BY UPPER(marker)
ORDER BY marker;

\echo ''
\echo '=== Raw samples missing a current aggregate pointer ==='
SELECT
    UPPER(rs.marker) AS marker,
    COUNT(DISTINCT UPPER(rs.sample_id)) AS raw_samples_without_aggregate
FROM biodiversity_raw_samples AS rs
WHERE NOT EXISTS (
    SELECT 1
    FROM sample_taxon_abundance AS sta
    WHERE UPPER(sta.sample_id) = UPPER(rs.sample_id)
      AND UPPER(sta.marker) = UPPER(rs.marker)
      AND LOWER(sta.level) = 'phylum'
)
GROUP BY UPPER(rs.marker)
ORDER BY marker;

\echo ''
\echo '=== Stored OTU/features and non-zero abundance rows ==='
SELECT
    UPPER(rs.marker) AS marker,
    COUNT(DISTINCT (f.upload_id, f.feature_index)) AS stored_feature_rows,
    COUNT(a.feature_index) AS nonzero_abundance_rows
FROM biodiversity_raw_samples AS rs
JOIN biodiversity_raw_features AS f
  ON f.upload_id = rs.upload_id
LEFT JOIN biodiversity_raw_abundance AS a
  ON a.upload_id = f.upload_id
 AND a.feature_index = f.feature_index
GROUP BY UPPER(rs.marker)
ORDER BY marker;
