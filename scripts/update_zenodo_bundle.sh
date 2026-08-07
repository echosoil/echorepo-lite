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

ZENODO_LOG_FILE="${ZENODO_LOG_FILE:-data/zenodo_sync_log.csv}"
ZENODO_DEPOSITION_STATE_FILE="${ZENODO_DEPOSITION_STATE_FILE:-data/zenodo_latest_deposition_id.txt}"

# Prefer the state written by the previous successful publication.
# Fall back to .env_zenodo only for the first/bootstrap run.
if [[ -s "$ZENODO_DEPOSITION_STATE_FILE" ]]; then
    state_id="$(tr -d '[:space:]' < "$ZENODO_DEPOSITION_STATE_FILE")"
    if [[ ! "$state_id" =~ ^[0-9]+$ ]]; then
        echo "ERROR: invalid deposition ID in $ZENODO_DEPOSITION_STATE_FILE: $state_id" >&2
        exit 2
    fi
    ZENODO_DEPOSITION_ID="$state_id"
else
    : "${ZENODO_DEPOSITION_ID:?ZENODO_DEPOSITION_ID must identify the latest published Zenodo version for the first run}"
fi

echo "Using Zenodo deposition ID: $ZENODO_DEPOSITION_ID"

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
    --existing-deposition-id "$ZENODO_DEPOSITION_ID" \
    --metadata-config "${SOILWISE_METADATA_CONFIG:-metadata/canonical/echorepo_columns.json}" \
    --file-json-name "${SOILWISE_FILE_JSON_NAME:-file.json}" \
    --log-file "$ZENODO_LOG_FILE" \
    --title "${ZENODO_TITLE:-ECHOREPO Soil Dataset}" \
    --description "${ZENODO_DESCRIPTION:-Canonical ECHOREPO soil data resources, controlled reference tables, and machine-readable CSVW metadata.}" \
    --creator "${ZENODO_CREATOR:-Osychenko, Oleg|Quanta Systems, S.L.}" \
    --grant "${ZENODO_GRANT:-101112869}" \
    --copyright "${ZENODO_COPYRIGHT:-© 2026 ECHO Horizon Project}" \
    --keyword "${ZENODO_KEYWORD:-soil,biodiversity,citizen-science}" \
    --subject "${ZENODO_SUBJECT:-Soil science|http://id.loc.gov/authorities/subjects/sh85124022|url}" \
    "${extra_flags[@]}" \
    "$@"

# A dry run intentionally does not create/publish a new Zenodo version,
# so there is no new deposition ID to persist.
dry_run=false
for arg in "$@"; do
    if [[ "$arg" == "--dry-run" ]]; then
        dry_run=true
        break
    fi
done

if [[ "$dry_run" == "false" ]]; then
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
        row.get("status") == "ok"
        and row.get("deposition_id")
        and row.get("existing_deposition_id") == previous_id
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

    echo "Saved latest Zenodo deposition ID: $new_deposition_id"
    echo "State file: $ZENODO_DEPOSITION_STATE_FILE"
fi
