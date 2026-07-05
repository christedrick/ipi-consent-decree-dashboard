#!/usr/bin/env bash
# IPI Dashboard ETL Refresh
# Downloads latest EPA bulk files, loads to BigQuery, and applies deadline corrections.
# Intended to run every two weeks via cron or scheduled task.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
VENV="$SCRIPT_DIR/venv/bin/activate"
LOG="$SCRIPT_DIR/refresh_$(date +%Y%m%d_%H%M%S).log"

export GOOGLE_APPLICATION_CREDENTIALS="$HOME/.config/ipi-etl/service-account.json"

echo "=== IPI ETL Refresh started at $(date) ===" | tee "$LOG"

source "$VENV"

echo "[1/4] Running bulk ETL (download + BigQuery load)..." | tee -a "$LOG"
python "$SCRIPT_DIR/etl_bulk.py" 2>&1 | tee -a "$LOG"

echo "[2/6] Running seed data loader..." | tee -a "$LOG"
python "$SCRIPT_DIR/seed_data.py" 2>&1 | tee -a "$LOG"

echo "[3/6] Running deadline corrections..." | tee -a "$LOG"
python "$SCRIPT_DIR/deadline_lookup.py" 2>&1 | tee -a "$LOG"

echo "[4/6] Populating population data from Census..." | tee -a "$LOG"
python "$SCRIPT_DIR/populate_population.py" --update 2>&1 | tee -a "$LOG"

# Runs after population so size tiers see final values; also classifies
# signal columns on rows added by seed_data (which carries NULL signals).
echo "[5/7] Backfilling derived columns (signals + size tiers)..." | tee -a "$LOG"
python "$SCRIPT_DIR/backfill_signals.py" 2>&1 | tee -a "$LOG"

echo "[6/8] Scanning news + NRC feed for infrastructure incidents..." | tee -a "$LOG"
python "$SCRIPT_DIR/incident_monitor.py" 2>&1 | tee -a "$LOG" || true
python "$SCRIPT_DIR/structured_incidents.py" 2>&1 | tee -a "$LOG" || true

echo "[7/8] Exporting qualified target list (Layer 3a)..." | tee -a "$LOG"
python "$SCRIPT_DIR/export_targets.py" 2>&1 | tee -a "$LOG"

echo "[8/8] Running data validation (with auto-fix)..." | tee -a "$LOG"
python "$SCRIPT_DIR/validate_data.py" --fix 2>&1 | tee -a "$LOG" || true

# Stamp the refresh so the dashboard's "Last refreshed" caption is accurate
# (previously only the dashboard's own refresh button wrote this).
date "+%Y-%m-%d %H:%M" > "$SCRIPT_DIR/.last_refresh"

echo "=== IPI ETL Refresh completed at $(date) ===" | tee -a "$LOG"
