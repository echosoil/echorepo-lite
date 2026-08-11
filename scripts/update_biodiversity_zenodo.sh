#!/usr/bin/env bash
set -euo pipefail

ZENODO_ENV_FILE="${ZENODO_ENV_FILE:-.env_zenodo_biodiversity}"
PUBLISHER="${BIODIVERSITY_ZENODO_PUBLISHER:-tools/publish_biodiversity_to_zenodo.py}"

if [[ ! -f "$ZENODO_ENV_FILE" ]]; then
    echo "ERROR: Zenodo environment file not found: $ZENODO_ENV_FILE" >&2
    exit 2
fi

if [[ ! -f "$PUBLISHER" ]]; then
    echo "ERROR: biodiversity Zenodo publisher not found: $PUBLISHER" >&2
    exit 2
fi

set -a
# shellcheck disable=SC1090
source "$ZENODO_ENV_FILE"
set +a

is_true() {
    [[ "${1:-}" =~ ^([1Tt][Rr][Uu][Ee]|[Yy][Ee][Ss]|[Oo][Nn])$ || "${1:-}" == "1" ]]
}

API_BASE="${BIODIVERSITY_ZENODO_API_BASE:-${ZENODO_API_BASE:-https://echorepo.quanta-labs.com/api/v1}}"
API_PATH="${BIODIVERSITY_ZENODO_API_PATH:-${ZENODO_API_PATH:-/biodiversity/raw/all.zip}}"
METADATA_CONFIG="${BIODIVERSITY_METADATA_CONFIG:-${SOILWISE_METADATA_CONFIG:-metadata/biodiversity/echorepo_biodiversity_columns.json}}"
FILE_JSON_NAME="${BIODIVERSITY_FILE_JSON_NAME:-${SOILWISE_FILE_JSON_NAME:-file.json}}"

ZENODO_LOG_FILE="${BIODIVERSITY_ZENODO_LOG_FILE:-${ZENODO_LOG_FILE:-data/zenodo_biodiversity_sync_log.csv}}"
ZENODO_DEPOSITION_STATE_FILE="${BIODIVERSITY_ZENODO_DEPOSITION_STATE_FILE:-${ZENODO_DEPOSITION_STATE_FILE:-data/zenodo_biodiversity_latest_deposition_id.txt}}"

TITLE="${BIODIVERSITY_ZENODO_TITLE:-${ZENODO_TITLE:-ECHOREPO Microbial Biodiversity Source Data}}"
DESCRIPTION="${BIODIVERSITY_ZENODO_DESCRIPTION:-${ZENODO_DESCRIPTION:-OTU/feature-level microbial biodiversity data associated with ECHOREPO soil samples, including taxonomy and sparse sequence-read abundances for 16S and ITS marker datasets.}}"
CREATOR="${BIODIVERSITY_ZENODO_CREATOR:-${ZENODO_CREATOR:-Osychenko, Oleg|Quanta Systems, S.L.}}"
GRANT="${BIODIVERSITY_ZENODO_GRANT:-${ZENODO_GRANT:-101112869}}"
COPYRIGHT="${BIODIVERSITY_ZENODO_COPYRIGHT:-${ZENODO_COPYRIGHT:-© 2026 ECHO Horizon Project}}"
KEYWORD="${BIODIVERSITY_ZENODO_KEYWORD:-${ZENODO_KEYWORD:-soil,biodiversity,microbial-biodiversity,16S,ITS,OTU,taxonomy,soil-biodiversity,citizen-science}}"
SUBJECT="${BIODIVERSITY_ZENODO_SUBJECT:-${ZENODO_SUBJECT:-}}"

RELATED_DOI="${BIODIVERSITY_RELATED_DOI:-10.5281/zenodo.19722513}"
RELATED_RELATION="${BIODIVERSITY_RELATED_RELATION:-isSupplementTo}"

if [[ -s "$ZENODO_DEPOSITION_STATE_FILE" ]]; then
    state_id="$(tr -d '[:space:]' < "$ZENODO_DEPOSITION_STATE_FILE")"
    if [[ ! "$state_id" =~ ^[0-9]+$ ]]; then
        echo "ERROR: invalid deposition ID in $ZENODO_DEPOSITION_STATE_FILE: $state_id" >&2
        exit 2
    fi
    ZENODO_DEPOSITION_ID="$state_id"
else
    bootstrap_id="${BIODIVERSITY_ZENODO_DEPOSITION_ID:-${ZENODO_DEPOSITION_ID:-}}"
    if [[ -z "$bootstrap_id" ]]; then
        echo "ERROR: no biodiversity deposition state exists yet." >&2
        echo "Publish the biodiversity dataset once first, then write its deposition ID to:" >&2
        echo "  $ZENODO_DEPOSITION_STATE_FILE" >&2
        exit 2
    fi
    if [[ ! "$bootstrap_id" =~ ^[0-9]+$ ]]; then
        echo "ERROR: invalid bootstrap biodiversity deposition ID: $bootstrap_id" >&2
        exit 2
    fi
    ZENODO_DEPOSITION_ID="$bootstrap_id"
fi

echo "Using biodiversity Zenodo deposition ID: $ZENODO_DEPOSITION_ID"
echo "API source: ${API_BASE}${API_PATH}"

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

if [[ -n "$SUBJECT" ]]; then
    extra_flags+=(--subject "$SUBJECT")
fi

if [[ -n "$RELATED_DOI" ]]; then
    if python3 "$PUBLISHER" --help 2>&1 | grep -q -- "--related-identifier"; then
        extra_flags+=(--related-identifier "${RELATED_DOI}|${RELATED_RELATION}")
    else
        echo "ERROR: $PUBLISHER does not support --related-identifier yet." >&2
        echo "Expected relation: ${RELATED_DOI} | ${RELATED_RELATION}" >&2
        exit 2
    fi
fi

python3 "$PUBLISHER" \
    --env-file "$ZENODO_ENV_FILE" \
    --api-base "$API_BASE" \
    --api-path "$API_PATH" \
    --existing-deposition-id "$ZENODO_DEPOSITION_ID" \
    --metadata-config "$METADATA_CONFIG" \
    --file-json-name "$FILE_JSON_NAME" \
    --log-file "$ZENODO_LOG_FILE" \
    --title "$TITLE" \
    --description "$DESCRIPTION" \
    --creator "$CREATOR" \
    --grant "$GRANT" \
    --copyright "$COPYRIGHT" \
    --keyword "$KEYWORD" \
    "${extra_flags[@]}" \
    "$@"

dry_run=false
for arg in "$@"; do
    if [[ "$arg" == "--dry-run" ]]; then
        dry_run=true
        break
    fi
done

if [[ "$dry_run" == "true" ]]; then
    echo "Dry run completed; biodiversity deposition state was not changed."
    exit 0
fi

if [[ ! -s "$ZENODO_LOG_FILE" ]]; then
    echo "ERROR: publication succeeded but Zenodo log is missing/empty: $ZENODO_LOG_FILE" >&2
    exit 3
fi

new_deposition_id="$(
    python3 - "$ZENODO_LOG_FILE" "$ZENODO_DEPOSITION_ID" <<'PY'
import csv
import sys
from pathlib import Path

log_path = Path(sys.argv[1])
previous_id = sys.argv[2]

with log_path.open("r", encoding="utf-8", newline="") as fh:
    rows = list(csv.DictReader(fh))

for row in reversed(rows):
    if (
        (row.get("status") or "").strip().lower() == "ok"
        and (row.get("deposition_id") or "").strip()
        and (row.get("existing_deposition_id") or "").strip() == previous_id
    ):
        value = row["deposition_id"].strip()
        if not value.isdigit():
            raise SystemExit(
                f"Invalid deposition_id in {log_path}: {value!r}"
            )
        print(value)
        break
else:
    raise SystemExit(
        f"No successful publication row found in {log_path} "
        f"for existing_deposition_id={previous_id}"
    )
PY
)"

mkdir -p "$(dirname "$ZENODO_DEPOSITION_STATE_FILE")"
tmp_state="${ZENODO_DEPOSITION_STATE_FILE}.tmp"
printf '%s\n' "$new_deposition_id" > "$tmp_state"
mv "$tmp_state" "$ZENODO_DEPOSITION_STATE_FILE"

echo "Saved latest biodiversity Zenodo deposition ID: $new_deposition_id"
echo "State file: $ZENODO_DEPOSITION_STATE_FILE"
