#!/usr/bin/env bash
# Cron script: organize completed simulations
# Schedule: every Saturday at midnight (0 0 * * 6)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
LOCK_FILE="$ROOT/.cron_organize.lock"
LOG_DIR="$ROOT/logs"
LOG_FILE="$LOG_DIR/cron_organize_$(date +%Y%m%d_%H%M%S).log"
CONFIG_PATH="${ORCA_AUTO_CONFIG:-$ROOT/config/orca_auto.yaml}"

# Load user env vars if available
[[ -f "$HOME/.orca_auto_env" ]] && source "$HOME/.orca_auto_env"

# Prevent concurrent execution via flock
exec 200>"$LOCK_FILE"
flock -n 200 || { echo "[cron_organize] Already running, exiting." >&2; exit 0; }

mkdir -p "$LOG_DIR"

PYTHON_BIN="$ROOT/.venv/bin/python3"
[[ -x "$PYTHON_BIN" ]] || PYTHON_BIN="$(command -v python3)"

ALLOWED_ROOT="$(
  PYTHONPATH="$ROOT:${PYTHONPATH:-}" "$PYTHON_BIN" - "$CONFIG_PATH" <<'PY'
import sys
from core.config import load_config

cfg = load_config(sys.argv[1])
print(cfg.runtime.allowed_root)
PY
)"

echo "[cron_organize] Started at $(date -Iseconds)" | tee -a "$LOG_FILE"
echo "[cron_organize] allowed_root=$ALLOWED_ROOT config=$CONFIG_PATH" | tee -a "$LOG_FILE"

PYTHONPATH="$ROOT:${PYTHONPATH:-}" "$PYTHON_BIN" -m core.cli \
  --config "$CONFIG_PATH" \
  organize --root "$ALLOWED_ROOT" --apply --json \
  2>&1 | tee -a "$LOG_FILE"

EXIT_CODE=${PIPESTATUS[0]}
echo "[cron_organize] Finished at $(date -Iseconds) with exit code $EXIT_CODE" | tee -a "$LOG_FILE"
exit $EXIT_CODE
