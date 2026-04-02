#!/usr/bin/env bash
set -euo pipefail

python3 tools/publish_api_file_to_zenodo.py \
  --sandbox \
  --api-base "${ZENODO_API_BASE:-https://echorepo.quanta-labs.com/api/v1}" \
  --api-path /canonical/zenodo_bundle.zip \
  --existing-deposition-id 483318 \
  --log-file data/zenodo_sync_log.csv \
  --title "${ZENODO_TITLE:-ECHOREPO Zenodo bundle}" \
  --description "${ZENODO_DESCRIPTION:-ECHOREPO export bundle for Zenodo}" \
  --creator "${ZENODO_CREATOR:-Osychenko, Oleg|Quanta Systems, S.L.}"