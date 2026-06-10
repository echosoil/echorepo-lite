CREATE INDEX IF NOT EXISTS idx_samples_lon_lat
ON samples (lon, lat);

CREATE INDEX IF NOT EXISTS idx_samples_lat_lon
ON samples (lat, lon);

CREATE INDEX IF NOT EXISTS idx_samples_country
ON samples (country_code);

CREATE INDEX IF NOT EXISTS idx_samples_timestamp
ON samples (timestamp_utc);

CREATE INDEX IF NOT EXISTS idx_samples_country_time
ON samples (country_code, timestamp_utc);
