#!/usr/bin/env bash
set -Eeuo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON="/home/echo/ECHO-STORE/venv/bin/python3"

# Prevent accidentally running against production PostgreSQL from dev.
if [[ "$ROOT" != "/home/echo/ECHO-STORE/echorepo-lite" ]]; then
    echo "ERROR: Run this script only from the production checkout." >&2
    exit 1
fi

cd "$ROOT"

# Production PostgreSQL, exposed by Docker on localhost:5432.
export DB_HOST_OUTSIDE=127.0.0.1
export DB_PORT_OUTSIDE=5432

# Prevent overlapping runs.
exec 9>"$ROOT/data/soil_normalization.lock"
if ! flock -n 9; then
    echo "[$(date -Is)] Another normalization is running; skipping."
    exit 0
fi

echo "[$(date -Is)] Starting soil normalization"

"$PYTHON" tools/normalize_soil_classifications.py \
    --no-json \
    --unknown-report "$ROOT/data/soil_classification_unmatched.csv"

echo "[$(date -Is)] Soil normalization completed"
