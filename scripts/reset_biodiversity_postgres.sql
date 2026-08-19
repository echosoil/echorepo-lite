-- reset_biodiversity_postgres.sql
--
-- COMPLETE RESET of the current PostgreSQL biodiversity import state.
-- Original MinIO source objects are NOT deleted.
--
-- No CASCADE is used intentionally. If another PostgreSQL table unexpectedly
-- depends on these tables, TRUNCATE will fail rather than deleting extra data.

\set ON_ERROR_STOP on
\timing on

BEGIN;

SELECT
    'before' AS stage,
    (SELECT COUNT(*) FROM sample_taxon_abundance) AS sample_taxon_abundance,
    (SELECT COUNT(*) FROM biodiversity_raw_abundance) AS raw_abundance,
    (SELECT COUNT(*) FROM biodiversity_raw_features) AS raw_features,
    (SELECT COUNT(*) FROM biodiversity_raw_samples) AS raw_samples,
    (SELECT COUNT(*) FROM biodiversity_uploads) AS uploads;

TRUNCATE TABLE
    sample_taxon_abundance,
    biodiversity_raw_abundance,
    biodiversity_raw_features,
    biodiversity_raw_samples,
    biodiversity_uploads
RESTART IDENTITY;

-- Older deployments may also contain the legacy OTU table used by historical
-- FAPROTAX/chart code. Clear it if it exists so no stale biodiversity dataset
-- survives the reset.
DO $$
BEGIN
    IF to_regclass('public.sample_otu_counts') IS NOT NULL THEN
        EXECUTE 'TRUNCATE TABLE public.sample_otu_counts RESTART IDENTITY';
        RAISE NOTICE 'Legacy table sample_otu_counts was also cleared.';
    END IF;
END
$$;

SELECT
    'after' AS stage,
    (SELECT COUNT(*) FROM sample_taxon_abundance) AS sample_taxon_abundance,
    (SELECT COUNT(*) FROM biodiversity_raw_abundance) AS raw_abundance,
    (SELECT COUNT(*) FROM biodiversity_raw_features) AS raw_features,
    (SELECT COUNT(*) FROM biodiversity_raw_samples) AS raw_samples,
    (SELECT COUNT(*) FROM biodiversity_uploads) AS uploads;

COMMIT;

\echo 'Biodiversity PostgreSQL reset completed.'
