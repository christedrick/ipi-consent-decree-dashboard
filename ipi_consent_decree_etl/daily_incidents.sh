#!/usr/bin/env bash
# Daily incident scan — news monitor + NRC structured feed + target re-export.
# Lightweight (~5 min): does NOT re-run the full EPA bulk ETL (that stays on
# the biweekly refresh.sh cadence). Scheduled via LaunchAgent:
#   ~/Library/LaunchAgents/com.ipi.incident-monitor.plist
# launchd runs missed jobs on wake, so a sleeping laptop catches up.

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LOG="$SCRIPT_DIR/logs/daily_incidents.log"
mkdir -p "$SCRIPT_DIR/logs"

export GOOGLE_APPLICATION_CREDENTIALS="$SCRIPT_DIR/service-account.json"
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
