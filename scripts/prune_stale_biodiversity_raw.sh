#!/usr/bin/env bash
set -euo pipefail

PG_CONTAINER="echorepo_prod-postgres-1"
PG_USER="echorepo"
PG_DB="echorepo"

echo "Finding fully stale biodiversity uploads..."

docker exec -i "$PG_CONTAINER" \
  psql -U "$PG_USER" -d "$PG_DB" -At -c "
    SELECT bu.upload_id
    FROM biodiversity_uploads bu
    WHERE NOT EXISTS (
        SELECT 1
        FROM sample_taxon_abundance sta
        WHERE sta.source_upload_id = bu.upload_id
    )
    ORDER BY bu.uploaded_at;
  " > /tmp/stale_biodiversity_uploads.txt

count=$(wc -l < /tmp/stale_biodiversity_uploads.txt)

echo "Found $count stale uploads."
echo

while IFS= read -r upload_id; do
    [ -z "$upload_id" ] && continue

    echo "=================================================="
    echo "Pruning structured raw upload: $upload_id"

    docker exec -i "$PG_CONTAINER" \
      psql \
        -v ON_ERROR_STOP=1 \
        -v upload_id="$upload_id" \
        -U "$PG_USER" \
        -d "$PG_DB" <<'SQL'

BEGIN;

DELETE FROM biodiversity_raw_abundance
WHERE upload_id = :'upload_id';

DELETE FROM biodiversity_raw_features
WHERE upload_id = :'upload_id';

DELETE FROM biodiversity_raw_samples
WHERE upload_id = :'upload_id';

COMMIT;

SQL

    echo "Finished: $upload_id"
done < /tmp/stale_biodiversity_uploads.txt

echo
echo "Running VACUUM/ANALYZE..."

docker exec -i "$PG_CONTAINER" \
  psql -U "$PG_USER" -d "$PG_DB" <<'SQL'

VACUUM (ANALYZE) biodiversity_raw_abundance;
VACUUM (ANALYZE) biodiversity_raw_features;
VACUUM (ANALYZE) biodiversity_raw_samples;

SQL

echo "Done."
