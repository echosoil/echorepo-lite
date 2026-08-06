#!/usr/bin/env bash
set -euo pipefail

set -a
source "${ZENODO_ENV_FILE:-.env_zenodo}"
set +a

extra_flags=()

if [[ "${SOILWISE_VALIDATE:-false}" =~ ^(1|true|yes|on)$ ]]; then
  extra_flags+=(--validate-soilvoc)
fi

if [[ "${SOILWISE_REQUIRE_VALIDATION:-false}" =~ ^(1|true|yes|on)$ ]]; then
  extra_flags+=(--require-soilvoc-validation)
fi

if [[ "${SOILWISE_REQUIRE_COMPLETE_METADATA:-false}" =~ ^(1|true|yes|on)$ ]]; then
  extra_flags+=(--require-complete-metadata)
fi

if [[ "${ZENODO_KEEP_SOURCE_ARCHIVE:-false}" =~ ^(1|true|yes|on)$ ]]; then
  extra_flags+=(--keep-source-archive)
fi

python3 tools/publish_api_file_to_zenodo.py \
  --env-file "${ZENODO_ENV_FILE:-.env_zenodo}" \
  --api-base "${ZENODO_API_BASE:-https://echorepo.quanta-labs.com/api/v1}" \
  --api-path "${ZENODO_API_PATH:-/canonical/all.zip}" \
  --metadata-config "${SOILWISE_METADATA_CONFIG:-metadata/soilwise/echorepo_columns.json}" \
  --file-json-name "${SOILWISE_FILE_JSON_NAME:-file.json}" \
  --log-file "${ZENODO_LOG_FILE:-data/zenodo_sync_log.csv}" \
  --title "${ZENODO_TITLE:-ECHOREPO Soil Dataset}" \
  --description "${ZENODO_DESCRIPTION:-Canonical ECHOREPO soil data resources and machine-readable CSVW metadata.}" \
  --creator "${ZENODO_CREATOR:-Osychenko, Oleg|Quanta Systems, S.L.}" \
  --grant "${ZENODO_GRANT:-101112869}" \
  --copyright "${ZENODO_COPYRIGHT:-© 2026 ECHO Horizon Project}" \
  --keyword "${ZENODO_KEYWORD:-soil,biodiversity,citizen-science}" \
  --subject "${ZENODO_SUBJECT:-Soil science|http://id.loc.gov/authorities/subjects/sh85124022|url}" \
  "${extra_flags[@]}"
