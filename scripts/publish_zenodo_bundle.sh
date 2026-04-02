#!/usr/bin/env bash
set -euo pipefail

set -a
source .env_zenodo
set +a

python3 tools/publish_api_file_to_zenodo.py \
  --sandbox \
  --api-base "${ZENODO_API_BASE:-https://echorepo.quanta-labs.com/api/v1}" \
  --api-path "${ZENODO_API_PATH:-/canonical/zenodo_bundle.zip}" \
  --log-file "${ZENODO_LOG_FILE:-data/zenodo_sync_log.csv}" \
  --title "${ZENODO_TITLE:-ECHOREPO Zenodo bundle publication}" \
  --description "${ZENODO_DESCRIPTION:-ECHOREPO export bundle for Zenodo}" \
  --creator "${ZENODO_CREATOR:-Osychenko, Oleg|Quanta Systems, S.L.}"