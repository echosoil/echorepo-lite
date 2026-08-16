#!/usr/bin/env bash
set -euo pipefail

ACTION="${1:-status}"
shift || true

ZENODO_ENV_FILE="${ZENODO_ENV_FILE:-.env_zenodo_biodiversity}"

# Keep all expensive intermediate results on ECHO-STORE.
WORK_DIR="${WORK_DIR:-/home/quanta/echorepo-lite-dev/biodiversity_zenodo_work}"

SOURCE_DIR="$WORK_DIR/01_source"
PREPARED_DIR="$WORK_DIR/02_prepared"
CHECKPOINT_DIR="$WORK_DIR/checkpoints"
LOG_DIR="$WORK_DIR/logs"

SOURCE_ZIP="$SOURCE_DIR/biodiversity_raw.zip"

# Change this if your actual filename is publish_biodiversity_zenodo.py
PUBLISHER="${BIODIVERSITY_PUBLISHER:-tools/publish_biodiversity_to_zenodo.py}"

METADATA_CONFIG="${BIODIVERSITY_METADATA_CONFIG:-metadata/biodiversity/echorepo_biodiversity_columns.json}"


# Resource limit for the expensive export job.
EXPORT_CPUS="${EXPORT_CPUS:-1.0}"
EXPORT_MEMORY="${EXPORT_MEMORY:-4g}"

# Production Docker network created by COMPOSE_PROJECT_NAME=echorepo_prod
DOCKER_NETWORK="${DOCKER_NETWORK:-echorepo_dev_default}"

mkdir -p \
    "$SOURCE_DIR" \
    "$PREPARED_DIR" \
    "$CHECKPOINT_DIR" \
    "$LOG_DIR"

if [[ ! -f "$ZENODO_ENV_FILE" ]]; then
    echo "ERROR: missing $ZENODO_ENV_FILE" >&2
    exit 2
fi

set -a
# shellcheck disable=SC1090
source "$ZENODO_ENV_FILE"
set +a


timestamp() {
    date '+%Y-%m-%d %H:%M:%S %z'
}


resources() {
    echo
    echo "===== $(timestamp) ====="

    echo "-- MEMORY --"
    free -h || true

    echo
    echo "-- DISK --"
    df -h /home/quanta/echorepo-lite-dev || true

    echo
    echo "-- DOCKER --"
    docker stats --no-stream \
        --format \
        'table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.MemPerc}}' \
        echorepo_dev-echorepo-lite-1 \
        echorepo_dev-postgres-1 \
        2>/dev/null || true

    echo
}


checkpoint() {
    local name="$1"

    date --iso-8601=seconds \
        > "$CHECKPOINT_DIR/$name.ok"

    echo
    echo "CHECKPOINT:"
    echo "  $CHECKPOINT_DIR/$name.ok"
}


require_checkpoint() {
    local name="$1"

    if [[ ! -f "$CHECKPOINT_DIR/$name.ok" ]]; then
        echo "ERROR: missing checkpoint $name" >&2
        exit 2
    fi
}


common_args=(
    --env-file "$ZENODO_ENV_FILE"

    # IMPORTANT:
    # Reuse the already-created bundle.
    # Do NOT ask the API to regenerate it.
    --source-file "$SOURCE_ZIP"

    --metadata-config "$METADATA_CONFIG"

    --file-json-name \
    "${BIODIVERSITY_FILE_JSON_NAME:-file.json}"

    --log-file \
    "${BIODIVERSITY_ZENODO_LOG_FILE:-
      data/zenodo_biodiversity_sync_log.csv}"

    --title \
    "${BIODIVERSITY_ZENODO_TITLE:-
      ECHOREPO Microbial Biodiversity Source Data}"

    --description \
    "${BIODIVERSITY_ZENODO_DESCRIPTION:-
      OTU/feature-level microbial biodiversity data associated with \
ECHOREPO soil samples, preserving OTU-by-sample read-count matrices \
for 16S and ITS marker datasets together with normalized taxonomy.}"

    --creator \
    "${ZENODO_CREATOR:-
      Osychenko, Oleg|Quanta Systems, S.L.}"

    --grant \
    "${ZENODO_GRANT:-101112869}"

    --copyright \
    "${ZENODO_COPYRIGHT:-© 2026 ECHO Horizon Project}"

    --keyword \
    "soil,biodiversity,microbial-biodiversity,16S,ITS,OTU,citizen-science"
)


case "$ACTION" in

    status)
        echo "WORK_DIR:"
        echo "  $WORK_DIR"

        echo
        echo "CHECKPOINTS:"
        ls -lh "$CHECKPOINT_DIR" || true

        echo
        echo "SOURCE:"
        if [[ -f "$SOURCE_ZIP" ]]; then
            ls -lh "$SOURCE_ZIP"
            sha256sum "$SOURCE_ZIP"
        else
            echo "  not generated"
        fi

        echo
        echo "PREPARED:"
        ls -lh "$PREPARED_DIR" || true

        resources
        ;;


    export)
        echo "=============================================="
        echo "PHASE 1: GENERATE BIODIVERSITY ZIP"
        echo "=============================================="
        echo
        echo "CPU limit:    $EXPORT_CPUS"
        echo "Memory limit: $EXPORT_MEMORY"
        echo "Output:       $SOURCE_ZIP"
        echo

        resources

        rm -f \
            "$SOURCE_ZIP" \
            "$SOURCE_ZIP.part" \
            "$CHECKPOINT_DIR/01_export.ok"

        # Verify expected production network first.
        docker network inspect "$DOCKER_NETWORK" \
            >/dev/null

        LOG="$LOG_DIR/01_export_$(date +%Y%m%d_%H%M%S).log"

        echo "Starting isolated export container..."
        echo

        docker run \
            --rm \
            --name echorepo-biodiversity-export \
            --cpus="$EXPORT_CPUS" \
            --memory="$EXPORT_MEMORY" \
            --memory-swap="$EXPORT_MEMORY" \
            --pids-limit=256 \
            --network "$DOCKER_NETWORK" \
            --env-file .env \
            -e DB_HOST=postgres \
            -e DB_PORT=5432 \
            -e DB_NAME=echorepo \
            -e DB_USER=echorepo \
            -e DB_PASSWORD="${POSTGRES_PASSWORD:-echorepo-pass}" \
            -v "$SOURCE_DIR:/out" \
            --entrypoint python3 \
            echorepo-lite:latest \
            -c '
from pathlib import Path
from echorepo.services.biodiversity_raw_exports import (
    build_biodiversity_raw_bundle,
)

print("[1/3] Querying PostgreSQL and constructing bundle...", flush=True)

bundle = build_biodiversity_raw_bundle()

print(
    "[2/3] Bundle generated:",
    f"{len(bundle.zip_bytes) / 1024 / 1024:.1f} MB",
    flush=True,
)

tmp = Path("/out/biodiversity_raw.zip.part")
final = Path("/out/biodiversity_raw.zip")

tmp.write_bytes(bundle.zip_bytes)
tmp.replace(final)

print("[3/3] Written:", final, flush=True)

print("Rows:", flush=True)
for filename, count in bundle.row_counts.items():
    print(f"  {filename}: {count}", flush=True)
' 2>&1 | tee "$LOG"

        echo
        echo "Verifying ZIP..."
        unzip -t "$SOURCE_ZIP"

        echo
        unzip -l "$SOURCE_ZIP"

        echo
        ls -lh "$SOURCE_ZIP"

        echo
        sha256sum "$SOURCE_ZIP" \
            | tee "$SOURCE_ZIP.sha256"

        checkpoint "01_export"

        resources

        echo
        echo "================ STOP POINT ================"
        echo
        echo "Nothing has been sent to Zenodo."
        echo
        echo "Inspect the ZIP, then run:"
        echo
        echo "  $0 prepare"
        echo
        ;;


    prepare)
        echo "=============================================="
        echo "PHASE 2: LOCAL VALIDATION / ZENODO PREPARATION"
        echo "=============================================="

        require_checkpoint "01_export"

        if [[ ! -s "$SOURCE_ZIP" ]]; then
            echo "ERROR: missing source ZIP: $SOURCE_ZIP" >&2
            exit 2
        fi

        echo
        echo "Using existing ZIP:"
        ls -lh "$SOURCE_ZIP"

        echo
        echo "NO request to /biodiversity/raw/all.zip will occur."
        echo

        rm -rf "$PREPARED_DIR"
        mkdir -p "$PREPARED_DIR"

        resources

        LOG="$LOG_DIR/02_prepare_$(date +%Y%m%d_%H%M%S).log"

        nice -n 10 \
        ionice -c 2 -n 7 \
        /usr/bin/time -v \
        python3 "$PUBLISHER" \
            "${common_args[@]}" \
            --dry-run \
            --save-prepared-dir "$PREPARED_DIR" \
            "$@" \
            2>&1 | tee "$LOG"

        echo
        echo "Prepared files:"
        ls -lh "$PREPARED_DIR"

        echo
        echo "Checksums:"
        sha256sum "$PREPARED_DIR"/* \
            | tee "$PREPARED_DIR/SHA256SUMS"

        checkpoint "02_prepare"

        resources

        echo
        echo "================ STOP POINT ================"
        echo
        echo "Zenodo has NOT been modified."
        echo
        echo "Inspect:"
        echo
        echo "  $PREPARED_DIR"
        echo
        echo "Then publish explicitly with:"
        echo
        echo "  CONFIRM_PUBLISH=YES $0 publish"
        echo
        ;;


    publish)
        echo "=============================================="
        echo "PHASE 3: PUBLISH TO ZENODO"
        echo "=============================================="

        require_checkpoint "01_export"
        require_checkpoint "02_prepare"

        if [[ "${CONFIRM_PUBLISH:-}" != "YES" ]]; then
            echo
            echo "REFUSING TO PUBLISH."
            echo
            echo "You must explicitly use:"
            echo
            echo "  CONFIRM_PUBLISH=YES $0 publish"
            echo
            exit 2
        fi

        echo
        echo "Publishing this exact source:"
        ls -lh "$SOURCE_ZIP"
        sha256sum "$SOURCE_ZIP"

        echo
        resources

        LOG="$LOG_DIR/03_publish_$(date +%Y%m%d_%H%M%S).log"

        nice -n 10 \
        ionice -c 2 -n 7 \
        /usr/bin/time -v \
        python3 "$PUBLISHER" \
            "${common_args[@]}" \
            "$@" \
            2>&1 | tee "$LOG"

        checkpoint "03_publish"

        resources
        ;;


    reset-prepare)
        rm -rf "$PREPARED_DIR"
        mkdir -p "$PREPARED_DIR"

        rm -f \
            "$CHECKPOINT_DIR/02_prepare.ok" \
            "$CHECKPOINT_DIR/03_publish.ok"

        echo "Source ZIP preserved:"
        ls -lh "$SOURCE_ZIP" 2>/dev/null || true
        ;;


    reset-all)
        echo "Removing generated work."
        rm -rf "$WORK_DIR"
        ;;


    *)
        echo "Usage:"
        echo
        echo "  $0 status"
        echo "  $0 export"
        echo "  $0 prepare"
        echo "  CONFIRM_PUBLISH=YES $0 publish"
        echo "  $0 reset-prepare"
        echo "  $0 reset-all"
        exit 2
        ;;
esac
