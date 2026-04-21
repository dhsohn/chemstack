#!/usr/bin/env bash
set -euo pipefail
ROOT="${1:-$HOME/orca_runs}"
echo "=== Preflight Check ==="

# 1. Running processes (exclude this script itself and its parent shell)
echo "[1/4] Checking running processes..."
RUNNING=$(pgrep -af 'chemstack\.orca\.cli|/orca ' 2>/dev/null | grep -v "preflight_check" | grep -v "$$" || true)
if [ -n "$RUNNING" ]; then
  echo "  FAIL: chemstack ORCA CLI or ORCA engine processes are running. Abort cutover."
  echo "$RUNNING"
  exit 1
fi
echo "  OK: No running processes."

# 2. Stale locks
echo "[2/4] Checking stale locks..."
locks=$(find "$ROOT" -name 'run.lock' 2>/dev/null || true)
if [ -n "$locks" ]; then
  echo "  WARN: Found lock files:"
  echo "$locks"
  echo "  Review and remove stale locks before proceeding."
else
  echo "  OK: No lock files."
fi

# 3. In-progress states
echo "[3/4] Checking in-progress states..."
in_progress=$(find "$ROOT" -name 'run_state.json' -exec grep -l '"status": "r' {} \; 2>/dev/null || true)
if [ -n "$in_progress" ]; then
  echo "  WARN: Found running/retrying states:"
  echo "$in_progress"
  echo "  Complete or stop these runs before cutover."
else
  echo "  OK: No running/retrying states."
fi

# 4. Disk space
echo "[4/4] Disk space check..."
df -h "$HOME"
echo "=== Preflight Complete ==="
