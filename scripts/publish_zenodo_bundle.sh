#!/usr/bin/env bash
set -euo pipefail

ZENODO_ENV_FILE="${ZENODO_ENV_FILE:-.env_zenodo}"

if [[ ! -f "$ZENODO_ENV_FILE" ]]; then
    echo "ERROR: Zenodo environment file not found: $ZENODO_ENV_FILE" >&2
    exit 2
fi

set -a
# shellcheck disable=SC1090
source "$ZENODO_ENV_FILE"
set +a

is_true() {
    [[ "${1:-}" =~ ^([1Tt][Rr][Uu][Ee]|[Yy][Ee][Ss]|[Oo][Nn])$ || "${1:-}" == "1" ]]
}

extra_flags=()

if is_true "${SOILWISE_VALIDATE:-false}"; then
    extra_flags+=(--validate-soilvoc)
fi

if is_true "${SOILWISE_REQUIRE_VALIDATION:-false}"; then
    extra_flags+=(--require-soilvoc-validation)
fi

if is_true "${SOILWISE_REQUIRE_COMPLETE_METADATA:-false}"; then
    extra_flags+=(--require-complete-metadata)
fi

if is_true "${ZENODO_KEEP_SOURCE_ARCHIVE:-false}"; then
    extra_flags+=(--keep-source-archive)
fi

python3 tools/publish_api_file_to_zenodo.py \
    --env-file "$ZENODO_ENV_FILE" \
    --api-base "${ZENODO_API_BASE:-https://echorepo.quanta-labs.com/api/v1}" \
    --api-path "${ZENODO_API_PATH:-/canonical/all.zip}" \
    --metadata-config "${SOILWISE_METADATA_CONFIG:-metadata/canonical/echorepo_columns.json}" \
    --file-json-name "${SOILWISE_FILE_JSON_NAME:-file.json}" \
    --log-file "${ZENODO_LOG_FILE:-data/zenodo_sync_log.csv}" \
    --title "${ZENODO_TITLE:-ECHOREPO Soil Dataset}" \
    --description "${ZENODO_DESCRIPTION:-Canonical ECHOREPO soil data resources, controlled reference tables, and machine-readable CSVW metadata.}" \
    --creator "${ZENODO_CREATOR:-Osychenko, Oleg|Quanta Systems, S.L.}" \
    --grant "${ZENODO_GRANT:-101112869}" \
    --copyright "${ZENODO_COPYRIGHT:-© 2026 ECHO Horizon Project}" \
    --keyword "${ZENODO_KEYWORD:-soil,biodiversity,citizen-science}" \
    --subject "${ZENODO_SUBJECT:-Soil science|http://id.loc.gov/authorities/subjects/sh85124022|url}" \
    "${extra_flags[@]}"
