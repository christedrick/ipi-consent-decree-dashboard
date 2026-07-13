#!/usr/bin/env bash
# Daily incident scan — news monitor + NRC structured feed + target re-export.
# Lightweight (~5 min): does NOT re-run the full EPA bulk ETL (that stays on
# the biweekly refresh.sh cadence). Scheduled via LaunchAgent:
#   ~/Library/LaunchAgents/com.ipi.incident-monitor.plist
# launchd runs missed jobs on wake, so a sleeping laptop catches up.
#
# IMPORTANT: launchd cannot execute scripts inside OneDrive/CloudStorage
# folders (macOS privacy protection denies background access — exit 126).
# This script must run from the runtime clone at ~/ipi-etl, which
# self-updates from GitHub below. Develop in the OneDrive copy, push to
# GitHub, and the runtime picks it up on its next run.

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LOG="$SCRIPT_DIR/logs/daily_incidents.log"
mkdir -p "$SCRIPT_DIR/logs"

# Best-effort: keep the runtime clone current with GitHub (never fatal)
git -C "$SCRIPT_DIR/.." pull --ff-only --quiet 2>/dev/null || true

export GOOGLE_APPLICATION_CREDENTIALS="$HOME/.config/ipi-etl/service-account.json"
PY="$SCRIPT_DIR/venv/bin/python"

{
  echo "=== Daily incident scan: $(date) ==="
  "$PY" "$SCRIPT_DIR/incident_monitor.py" || echo "news monitor failed ($?)"
  "$PY" "$SCRIPT_DIR/structured_incidents.py" || echo "NRC feed failed ($?)"
  "$PY" "$SCRIPT_DIR/export_targets.py" || echo "target export failed ($?)"
  echo "=== Done: $(date) ==="
} >> "$LOG" 2>&1

# Keep the log from growing unbounded (~last 5000 lines)
tail -n 5000 "$LOG" > "$LOG.tmp" && mv "$LOG.tmp" "$LOG"
