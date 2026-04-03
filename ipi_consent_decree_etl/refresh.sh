#!/usr/bin/env bash
# IPI Dashboard ETL Refresh
# Downloads latest EPA bulk files, loads to BigQuery, and applies deadline corrections.
# Intended to run every two weeks via cron or scheduled task.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
VENV="$SCRIPT_DIR/venv/bin/activate"
LOG="$SCRIPT_DIR/refresh_$(date +%Y%m%d_%H%M%S).log"

export GOOGLE_APPLICATION_CREDENTIALS="$SCRIPT_DIR/service-account.json"

echo "=== IPI ETL Refresh started at $(date) ===" | tee "$LOG"

source "$VENV"

echo "[1/4] Running bulk ETL (download + BigQuery load)..." | tee -a "$LOG"
python "$SCRIPT_DIR/etl_bulk.py" 2>&1 | tee -a "$LOG"

echo "[2/4] Running seed data loader..." | tee -a "$LOG"
python "$SCRIPT_DIR/seed_data.py" 2>&1 | tee -a "$LOG"

echo "[3/5] Running deadline corrections..." | tee -a "$LOG"
python "$SCRIPT_DIR/deadline_lookup.py" 2>&1 | tee -a "$LOG"

echo "[4/5] Populating population data from Census..." | tee -a "$LOG"
python "$SCRIPT_DIR/populate_population.py" --update 2>&1 | tee -a "$LOG"

echo "[5/5] Running data validation (with auto-fix)..." | tee -a "$LOG"
python "$SCRIPT_DIR/validate_data.py" --fix 2>&1 | tee -a "$LOG" || true

echo "=== IPI ETL Refresh completed at $(date) ===" | tee -a "$LOG"
