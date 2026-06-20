-- migrations/postgres/20260620_001_map_performance_indexes.sql
-- Indexes for bbox-based dynamic map loading and canonical sample filtering.

CREATE INDEX IF NOT EXISTS idx_samples_lon_lat
ON samples (lon, lat);

CREATE INDEX IF NOT EXISTS idx_samples_lat_lon
ON samples (lat, lon);

CREATE INDEX IF NOT EXISTS idx_samples_country_code
ON samples (country_code);

CREATE INDEX IF NOT EXISTS idx_samples_timestamp_utc
ON samples (timestamp_utc);

CREATE INDEX IF NOT EXISTS idx_samples_qa_status
ON samples (qa_status);

CREATE INDEX IF NOT EXISTS idx_samples_sample_id
ON samples (sample_id);
