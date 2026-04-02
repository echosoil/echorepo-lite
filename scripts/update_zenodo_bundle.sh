#!/usr/bin/env bash
set -euo pipefail

set -a
source .env_zenodo
set +a

python3 tools/publish_api_file_to_zenodo.py \
  --sandbox \
  --api-base "${ZENODO_API_BASE:-https://echorepo.quanta-labs.com/api/v1}" \
  --api-path /canonical/zenodo_bundle.zip \
  --existing-deposition-id 483372 \
  --log-file data/zenodo_sync_log.csv \
  --title "${ZENODO_TITLE:-ECHOREPO Zenodo bundle update}" \
  --description "${ZENODO_DESCRIPTION:-ECHOREPO export bundle for Zenodo}" \
  --creator "${ZENODO_CREATOR:-Osychenko, Oleg|Quanta Systems, S.L.}"