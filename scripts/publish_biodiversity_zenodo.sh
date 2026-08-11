#!/usr/bin/env bash
set -euo pipefail

ZENODO_ENV_FILE="${ZENODO_ENV_FILE:-.env_zenodo_biodiversity}"

if [[ ! -f "$ZENODO_ENV_FILE" ]]; then
    echo "ERROR: Zenodo environment file not found: $ZENODO_ENV_FILE" >&2
    exit 2
fi

set -a
# shellcheck disable=SC1090
source "$ZENODO_ENV_FILE"
set +a

python3 tools/publish_biodiversity_to_zenodo.py \
    --env-file "$ZENODO_ENV_FILE" \
    --api-base "${BIODIVERSITY_ZENODO_API_BASE:-https://echorepo.quanta-labs.com/api/v1}" \
    --api-path "${BIODIVERSITY_ZENODO_API_PATH:-/biodiversity/raw/all.zip}" \
    --metadata-config "${BIODIVERSITY_METADATA_CONFIG:-metadata/biodiversity/echorepo_biodiversity_columns.json}" \
    --file-json-name "${BIODIVERSITY_FILE_JSON_NAME:-file.json}" \
    --log-file "${BIODIVERSITY_ZENODO_LOG_FILE:-data/zenodo_biodiversity_sync_log.csv}" \
    --title "${BIODIVERSITY_ZENODO_TITLE:-ECHOREPO Microbial Biodiversity Source Data}" \
    --description "${BIODIVERSITY_ZENODO_DESCRIPTION:-OTU/feature-level microbial biodiversity data associated with ECHOREPO soil samples, including taxonomy and sparse sequence-read abundances for 16S and ITS marker datasets.}" \
    --creator "${ZENODO_CREATOR:-Osychenko, Oleg|Quanta Systems, S.L.}" \
    --grant "${ZENODO_GRANT:-101112869}" \
    --copyright "${ZENODO_COPYRIGHT:-© 2026 ECHO Horizon Project}" \
    --keyword "soil,biodiversity,microbial-biodiversity,16S,ITS,OTU,citizen-science" \
    "$@"