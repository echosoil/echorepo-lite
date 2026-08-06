#!/usr/bin/env bash
set -euo pipefail

set -a
source .env_zenodo
set +a

sandbox_flag=()
if [[ "${ZENODO_SANDBOX:-false}" =~ ^(1|true|TRUE|yes|YES|on|ON)$ ]]; then
  sandbox_flag+=(--sandbox)
fi

python3 tools/publish_api_file_to_zenodo.py \
  "${sandbox_flag[@]}" \
  --api-base "${ZENODO_API_BASE:-https://echorepo.quanta-labs.com/api/v1}" \
  --api-path "${ZENODO_API_PATH:-/canonical/zenodo_bundle.zip}" \
  --existing-deposition-id "${ZENODO_DEPOSITION_ID:-19722514}" \
  --log-file "${ZENODO_LOG_FILE:-data/zenodo_sync_log.csv}" \
  --title "${ZENODO_TITLE:-ECHOREPO Zenodo bundle publication}" \
  --description "${ZENODO_DESCRIPTION:-ECHOREPO export bundle for Zenodo}" \
  --creator "${ZENODO_CREATOR:-Osychenko, Oleg|Quanta Systems, S.L.}" \
  --grant "${ZENODO_GRANT:-101112869}" \
  --copyright "${ZENODO_COPYRIGHT:-© 2026 ECHO Horizon Project}" \
  --keyword "${ZENODO_KEYWORD:-soil, biodiversity, citizen-science}" \
  --subject "${ZENODO_SUBJECT:-Soil science|http://id.loc.gov/authorities/subjects/sh85124022|url}"
